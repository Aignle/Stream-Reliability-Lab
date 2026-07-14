"""FastAPI composition root, WebSocket transports, and local overlay server."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Literal, cast
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ValidationError

from streamlab.analytics import AnalyticsService
from streamlab.models import (
    ConnectionEventKind,
    ConnectionEventRequest,
    ConnectionSide,
    ErrorCategory,
    OverlayEffect,
    RenderAckMessage,
    RunCompleteRequest,
    ScenarioConfig,
)
from streamlab.repository import Repository
from streamlab.service import IngestService

STATIC_DIRECTORY = Path(__file__).with_name("static")
logger = logging.getLogger(__name__)


class HealthResponse(BaseModel):
    """Response returned when the application is ready to serve requests."""

    status: Literal["healthy"] = "healthy"


@dataclass(slots=True)
class OverlayConnection:
    """One live browser connection with serialized WebSocket writes."""

    websocket: WebSocket
    run_id: UUID
    session_id: str
    send_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class OverlayHub:
    """In-memory live connection registry backed by persisted replay evidence."""

    def __init__(self, repository: Repository) -> None:
        self.repository = repository
        self._connections: dict[tuple[UUID, str], OverlayConnection] = {}
        self._guard = asyncio.Lock()

    async def add(self, connection: OverlayConnection) -> None:
        previous: OverlayConnection | None
        async with self._guard:
            key = (connection.run_id, connection.session_id)
            previous = self._connections.get(key)
            self._connections[key] = connection
        if previous is not None and previous is not connection:
            await previous.websocket.close(code=4001, reason="session reconnected")

    async def remove(self, connection: OverlayConnection) -> bool:
        async with self._guard:
            key = (connection.run_id, connection.session_id)
            if self._connections.get(key) is not connection:
                return False
            self._connections.pop(key)
            return True

    async def send_json(self, run_id: UUID, session_id: str, payload: object) -> bool:
        async with self._guard:
            connection = self._connections.get((run_id, session_id))
        if connection is None:
            return False
        try:
            async with connection.send_lock:
                await connection.websocket.send_json(payload)
            return True
        except (RuntimeError, WebSocketDisconnect):
            await self.remove(connection)
            return False

    async def send_effect(self, session_id: str, effect: OverlayEffect) -> bool:
        payload = {
            "kind": "effect",
            "event": effect.model_dump(mode="json"),
        }
        sent = await self.send_json(effect.run_id, session_id, payload)
        self.repository.record_dispatch(
            event_id=effect.event_id,
            session_id=session_id,
            outcome="sent" if sent else "failed",
            error_message=None if sent else "overlay WebSocket unavailable",
        )
        return sent

    async def broadcast(self, effect: OverlayEffect) -> int:
        async with self._guard:
            session_ids = [
                item.session_id
                for item in self._connections.values()
                if item.run_id == effect.run_id
            ]
        outcomes = await asyncio.gather(
            *(self.send_effect(session_id, effect) for session_id in session_ids)
        )
        return sum(outcomes)


def _repository(app: FastAPI) -> Repository:
    return cast(Repository, app.state.repository)


def _service(app: FastAPI) -> IngestService:
    return cast(IngestService, app.state.ingest_service)


def _hub(app: FastAPI) -> OverlayHub:
    return cast(OverlayHub, app.state.overlay_hub)


def _analytics(app: FastAPI) -> AnalyticsService:
    return cast(AnalyticsService, app.state.analytics_service)


def create_app(database_path: str | Path | None = None) -> FastAPI:
    """Create an isolated application instance for local runtime or tests."""
    resolved_database_path = str(
        database_path or os.environ.get("STREAMLAB_DB_PATH", "data/streamlab.duckdb")
    )

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        repository = Repository(resolved_database_path)
        service = IngestService(repository)
        application.state.repository = repository
        application.state.ingest_service = service
        application.state.analytics_service = AnalyticsService(repository)
        application.state.overlay_hub = OverlayHub(repository)
        service.recover_pending()
        try:
            yield
        finally:
            repository.close()

    application = FastAPI(
        title="Stream Reliability Lab",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.mount("/static", StaticFiles(directory=STATIC_DIRECTORY), name="static")

    @application.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Report application and database readiness."""
        if not _repository(application).ping():
            raise HTTPException(status_code=503, detail="database unavailable")
        return HealthResponse()

    @application.get("/overlay", include_in_schema=False)
    async def overlay() -> FileResponse:
        """Serve the dependency-free browser overlay."""
        return FileResponse(STATIC_DIRECTORY / "overlay.html")

    @application.post("/api/runs", status_code=201)
    async def create_run(config: ScenarioConfig) -> dict[str, object]:
        """Persist a complete generated-event manifest before delivery."""
        try:
            _repository(application).create_run(config)
        except Exception as error:
            if _repository(application).run_exists(config.run_id):
                raise HTTPException(
                    status_code=409,
                    detail="run_id already exists",
                ) from error
            raise
        return {
            "run_id": str(config.run_id),
            "status": "running",
            "generated": len(config.manifest),
            "overlay_url": f"/overlay?run_id={config.run_id}",
            "analytics_url": f"/api/runs/{config.run_id}/overview",
        }

    @application.get("/api/runs")
    async def list_runs(
        limit: int = Query(default=25, ge=1, le=200),
    ) -> dict[str, object]:
        rows = _repository(application).query(
            """
            SELECT run_id, scenario, status, seed, generated_count, started_at,
                   completed_at
            FROM runs ORDER BY started_at DESC LIMIT ?
            """,
            [limit],
        )
        return {"runs": rows}

    @application.post("/api/runs/{run_id}/complete")
    async def complete_run(
        run_id: UUID,
        request: RunCompleteRequest,
    ) -> dict[str, object]:
        if not _repository(application).run_exists(run_id):
            raise HTTPException(status_code=404, detail="run not found")
        try:
            _repository(application).complete_run(
                run_id,
                request.generated_count,
                request.client_acked_count,
                request.retry_count,
            )
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"run_id": str(run_id), "status": "completed"}

    @application.post("/api/runs/{run_id}/connection-events", status_code=201)
    async def record_connection_event(
        run_id: UUID,
        request: ConnectionEventRequest,
    ) -> dict[str, object]:
        if not _repository(application).run_exists(run_id):
            raise HTTPException(status_code=404, detail="run not found")
        try:
            _repository(application).record_submitted_connection_event(run_id, request)
        except ValueError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"recorded": True}

    @application.get("/api/runs/latest")
    async def latest_run() -> dict[str, object]:
        rows = _repository(application).query(
            """
            SELECT run_id, scenario, status, started_at
            FROM runs ORDER BY started_at DESC LIMIT 1
            """
        )
        if not rows:
            raise HTTPException(status_code=404, detail="no runs available")
        return rows[0]

    @application.get("/api/runs/{run_id}/overview")
    async def run_overview(run_id: UUID) -> dict[str, object]:
        try:
            return _analytics(application).overview(run_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="run not found") from error

    @application.get("/api/runs/{run_id}/events")
    async def run_events(
        run_id: UUID,
        search: str = Query(default="", max_length=120),
        limit: int = Query(default=200, ge=1, le=1_000),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, object]:
        try:
            return _analytics(application).event_table(
                run_id,
                search=search,
                limit=limit,
                offset=offset,
            )
        except KeyError as error:
            raise HTTPException(status_code=404, detail="run not found") from error

    @application.get("/api/runs/{run_id}/performance")
    async def run_performance(run_id: UUID) -> dict[str, object]:
        try:
            return _analytics(application).performance(run_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="run not found") from error

    @application.get("/api/runs/{run_id}/failures")
    async def run_failures(run_id: UUID) -> dict[str, object]:
        try:
            return _analytics(application).failures(run_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="run not found") from error

    @application.get("/api/runs/{run_id}/events/{event_id}")
    async def event_evidence(run_id: UUID, event_id: UUID) -> dict[str, object]:
        try:
            return _analytics(application).event_evidence(run_id, event_id)
        except KeyError as error:
            raise HTTPException(status_code=404, detail="event not found") from error

    @application.websocket("/ws/ingest")
    async def ingest_socket(
        websocket: WebSocket,
        run_id: UUID = Query(...),
        connection_id: str | None = Query(default=None, min_length=1, max_length=80),
    ) -> None:
        if not _repository(application).run_exists(run_id):
            await websocket.close(code=4404, reason="run not found")
            return
        await websocket.accept()
        active_connection_id = connection_id or str(uuid4())
        _repository(application).record_connection_event(
            run_id,
            ConnectionEventRequest(
                side=ConnectionSide.SIMULATOR,
                kind=ConnectionEventKind.CONNECTED,
                connection_id=active_connection_id,
                detail={"observer": "server"},
            ),
        )
        try:
            while True:
                try:
                    raw_payload = await websocket.receive_text()
                except WebSocketDisconnect:
                    break
                try:
                    result = _service(application).ingest_text(
                        raw_payload,
                        active_connection_id,
                        run_id,
                    )
                except Exception as error:
                    logger.exception("Unexpected ingestion failure")
                    try:
                        _repository(application).record_rejected_delivery(
                            raw_payload=raw_payload,
                            connection_id=active_connection_id,
                            error_category=ErrorCategory.INTERNAL_ERROR,
                            error_message=(
                                f"unexpected ingestion failure: {type(error).__name__}"
                            ),
                            run_id=run_id,
                        )
                    except Exception:
                        logger.exception("Failed to persist internal-error evidence")
                    # No terminal NACK is fabricated. Closing the transport makes
                    # the simulator's bounded retry policy decide what happens next.
                    try:
                        await websocket.close(
                            code=1011,
                            reason="unexpected ingestion failure",
                        )
                    except (RuntimeError, WebSocketDisconnect):
                        pass
                    break
                try:
                    await websocket.send_json(result.reply.model_dump(mode="json"))
                except (RuntimeError, WebSocketDisconnect):
                    break
                try:
                    effect = _service(application).record_reply_and_process(result)
                    if effect is not None:
                        await _hub(application).broadcast(effect)
                except Exception:
                    # The reply is already sent. Durable accepted work remains
                    # eligible for duplicate-triggered or startup recovery.
                    logger.exception("Post-ACK processing or dispatch failure")
                    continue
        finally:
            try:
                _repository(application).record_connection_event(
                    run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.DISCONNECTED,
                        connection_id=active_connection_id,
                        detail={"observer": "server"},
                    ),
                )
            except Exception:
                logger.exception("Failed to persist simulator disconnect evidence")

    @application.websocket("/ws/overlay")
    async def overlay_socket(
        websocket: WebSocket,
        run_id: UUID = Query(...),
        session_id: str = Query(min_length=1, max_length=80),
    ) -> None:
        if not _repository(application).run_exists(run_id):
            await websocket.close(code=4404, reason="run not found")
            return
        await websocket.accept()
        connection = OverlayConnection(
            websocket=websocket,
            run_id=run_id,
            session_id=session_id,
        )
        _repository(application).open_overlay_session(session_id, run_id)
        _repository(application).record_connection_event(
            run_id,
            ConnectionEventRequest(
                side=ConnectionSide.OVERLAY,
                kind=ConnectionEventKind.CONNECTED,
                connection_id=session_id,
            ),
        )
        await _hub(application).add(connection)
        await _hub(application).send_json(
            run_id,
            session_id,
            {"kind": "overlay_ready", "run_id": str(run_id), "session_id": session_id},
        )
        for effect in _repository(application).pending_effects_for_session(
            run_id,
            session_id,
        ):
            await _hub(application).send_effect(session_id, effect)
        try:
            while True:
                message = await websocket.receive_json()
                try:
                    render_ack = RenderAckMessage.model_validate(message)
                except ValidationError as error:
                    await _hub(application).send_json(
                        run_id,
                        session_id,
                        {
                            "kind": "render_nack",
                            "error": error.errors(include_url=False)[0]["msg"],
                        },
                    )
                    continue
                try:
                    created = _repository(application).record_render_ack(
                        event_id=render_ack.event_id,
                        session_id=session_id,
                        rendered_at=render_ack.rendered_at,
                    )
                except ValueError as error:
                    await _hub(application).send_json(
                        run_id,
                        session_id,
                        {"kind": "render_nack", "error": str(error)},
                    )
                    continue
                await _hub(application).send_json(
                    run_id,
                    session_id,
                    {
                        "kind": "render_acknowledged",
                        "event_id": str(render_ack.event_id),
                        "duplicate": not created,
                    },
                )
        except WebSocketDisconnect:
            pass
        finally:
            removed = await _hub(application).remove(connection)
            if removed:
                _repository(application).close_overlay_session(session_id)
            _repository(application).record_connection_event(
                run_id,
                ConnectionEventRequest(
                    side=ConnectionSide.OVERLAY,
                    kind=ConnectionEventKind.DISCONNECTED,
                    connection_id=session_id,
                ),
            )

    return application


app = create_app()
