"""Protocol-level ingestion and overlay WebSocket tests."""

import asyncio
import json
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from streamlab.main import OverlayConnection, OverlayHub, create_app
from streamlab.models import RenderAckMessage, ScenarioName
from streamlab.repository import Repository
from streamlab.service import IngestService
from streamlab.simulator import build_scenario

RUN_ID = UUID("55555555-5555-4555-8555-555555555555")


def _config(event_count: int = 1):
    return build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=55,
        event_count=event_count,
        event_rate=100,
        run_id=RUN_ID,
    )


def test_websocket_emits_one_reply_per_input_after_processing_failure(
    tmp_path,
    monkeypatch,
) -> None:
    application = create_app(tmp_path / "one-reply.duckdb")
    config = _config()
    with TestClient(application) as client:
        assert (
            client.post("/api/runs", json=config.model_dump(mode="json")).status_code
            == 201
        )
        repository = application.state.repository
        original_process = repository.process_event

        def fail_processing(_event_id):
            raise RuntimeError("injected processing failure")

        monkeypatch.setattr(repository, "process_event", fail_processing)
        with client.websocket_connect(
            f"/ws/ingest?connection_id=source-test&run_id={RUN_ID}"
        ) as socket:
            socket.send_text(config.manifest[0].canonical_json())
            accepted = socket.receive_json()
            assert accepted["kind"] == "ack"
            assert accepted["status"] == "accepted"

            socket.send_text("{")
            rejected = socket.receive_json()
            assert rejected["kind"] == "nack"
            assert rejected["error"]["category"] == "MALFORMED_JSON"

        evidence = client.get(
            f"/api/runs/{RUN_ID}/events/{config.manifest[0].event_id}"
        ).json()
        assert evidence["canonical"]["acknowledged_at"] is not None
        assert evidence["canonical"]["processed_at"] is None
        failures = client.get(f"/api/runs/{RUN_ID}/failures").json()
        assert len(failures["processing_failures"]) == 1
        assert (
            "injected processing failure"
            in failures["processing_failures"][0]["error_message"]
        )
        assert failures["processing_failures"][0]["effect_id"] is None

        monkeypatch.setattr(repository, "process_event", original_process)
        recovered = application.state.ingest_service.recover_pending()
        assert [item.event_id for item in recovered] == [config.manifest[0].event_id]
        attempts = repository.query(
            "SELECT effect_id, outcome FROM processing_attempts ORDER BY started_at"
        )
        assert [item["outcome"] for item in attempts] == ["failure", "success"]
        assert attempts[0]["effect_id"] is None
        assert attempts[1]["effect_id"] == str(config.manifest[0].event_id)


def test_unexpected_ingestion_failure_is_audited_and_retried_on_new_transport(
    tmp_path,
    monkeypatch,
) -> None:
    application = create_app(tmp_path / "internal-error-retry.duckdb")
    config = _config()
    event = config.manifest[0]
    with TestClient(application) as client:
        assert (
            client.post("/api/runs", json=config.model_dump(mode="json")).status_code
            == 201
        )
        service = application.state.ingest_service
        original_ingest = service.ingest_text
        calls = 0

        def fail_once(raw_payload, connection_id, run_id_hint=None):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("injected unexpected ingestion failure")
            return original_ingest(raw_payload, connection_id, run_id_hint)

        monkeypatch.setattr(service, "ingest_text", fail_once)
        with client.websocket_connect(
            f"/ws/ingest?connection_id=failed-source&run_id={RUN_ID}"
        ) as failed_socket:
            failed_socket.send_text(event.canonical_json())
            with pytest.raises(WebSocketDisconnect) as disconnect:
                failed_socket.receive_json()
            assert disconnect.value.code == 1011

        with client.websocket_connect(
            f"/ws/ingest?connection_id=retry-source&run_id={RUN_ID}"
        ) as retry_socket:
            retry_socket.send_text(event.canonical_json())
            reply = retry_socket.receive_json()
            assert reply["status"] == "accepted"

        attempts = application.state.repository.query(
            """
            SELECT connection_id, outcome, error_category, response_sent_at
            FROM delivery_attempts ORDER BY received_at
            """
        )
        assert [(row["connection_id"], row["outcome"]) for row in attempts] == [
            ("failed-source", "rejected"),
            ("retry-source", "accepted"),
        ]
        assert attempts[0]["error_category"] == "INTERNAL_ERROR"
        assert attempts[0]["response_sent_at"] is None
        assert attempts[1]["response_sent_at"] is not None
        assert (
            application.state.repository.query("SELECT COUNT(*) AS count FROM events")[
                0
            ]["count"]
            == 1
        )
        assert (
            application.state.repository.query(
                "SELECT COUNT(*) AS count FROM processing_attempts "
                "WHERE outcome = 'success'"
            )[0]["count"]
            == 1
        )

        completion = client.post(
            f"/api/runs/{RUN_ID}/complete",
            json={"generated_count": 1, "client_acked_count": 1, "retry_count": 1},
        )
        assert completion.status_code == 200
        overview = client.get(f"/api/runs/{RUN_ID}/overview").json()
        assert overview["payload_rejections"] == 0
        assert overview["operational_delivery_failures"] == 1
        assert overview["verdict"] != "pass"
        assert any(
            "operational ingestion failures" in failure
            for failure in overview["verdict_failures"]
        )


def test_submitted_connection_evidence_is_server_stamped_and_completion_locked(
    tmp_path,
) -> None:
    application = create_app(tmp_path / "connection-evidence-boundary.duckdb")
    config = _config()
    with TestClient(application) as client:
        assert (
            client.post("/api/runs", json=config.model_dump(mode="json")).status_code
            == 201
        )
        before = datetime.now(UTC)
        response = client.post(
            f"/api/runs/{RUN_ID}/connection-events",
            json={
                "side": "simulator",
                "kind": "delay_injected",
                "connection_id": "submitted-source",
                "occurred_at": "2000-01-01T00:00:00Z",
                "detail": {
                    "observer": "server",
                    "source_sequence": 1,
                    "configured_delay_ms": 25,
                },
            },
        )
        after = datetime.now(UTC)
        assert response.status_code == 201
        stored = application.state.repository.query(
            """
            SELECT occurred_at, detail_json FROM connection_events
            WHERE connection_id = 'submitted-source'
            """
        )[0]
        assert before <= stored["occurred_at"] <= after
        assert json.loads(stored["detail_json"])["observer"] == "simulator"

        count_before = application.state.repository.query(
            "SELECT COUNT(*) AS count FROM connection_events"
        )[0]["count"]
        forged_server_row = client.post(
            f"/api/runs/{RUN_ID}/connection-events",
            json={
                "side": "simulator",
                "kind": "connected",
                "connection_id": "never-a-websocket",
                "detail": {"observer": "server"},
            },
        )
        assert forged_server_row.status_code == 409
        assert "not allowed" in forged_server_row.json()["detail"]
        assert (
            application.state.repository.query(
                "SELECT COUNT(*) AS count FROM connection_events"
            )[0]["count"]
            == count_before
        )

        completion = client.post(
            f"/api/runs/{RUN_ID}/complete",
            json={"generated_count": 1, "client_acked_count": 0, "retry_count": 0},
        )
        assert completion.status_code == 200
        late = client.post(
            f"/api/runs/{RUN_ID}/connection-events",
            json={
                "side": "simulator",
                "kind": "delay_injected",
                "connection_id": "late-source",
                "detail": {"source_sequence": 1, "configured_delay_ms": 25},
            },
        )
        assert late.status_code == 409
        assert (
            application.state.repository.query(
                "SELECT COUNT(*) AS count FROM connection_events"
            )[0]["count"]
            == count_before
        )


def test_ingest_to_overlay_and_stored_render_ack_vertical_slice(tmp_path) -> None:
    application = create_app(tmp_path / "vertical.duckdb")
    config = _config()
    event = config.manifest[0]
    with TestClient(application) as client:
        assert (
            client.post("/api/runs", json=config.model_dump(mode="json")).status_code
            == 201
        )
        overlay_url = f"/ws/overlay?run_id={RUN_ID}&session_id=browser-test"
        with client.websocket_connect(overlay_url) as overlay:
            ready = overlay.receive_json()
            assert ready["kind"] == "overlay_ready"
            with client.websocket_connect(
                f"/ws/ingest?connection_id=source-test&run_id={RUN_ID}"
            ) as source:
                source.send_text(event.canonical_json())
                reply = source.receive_json()
                assert reply["kind"] == "ack"
                effect = overlay.receive_json()
                assert effect["kind"] == "effect"
                assert effect["event"]["event_id"] == str(event.event_id)
                overlay.send_json(
                    {
                        "kind": "render_ack",
                        "event_id": str(event.event_id),
                        "rendered_at": datetime.now(UTC).isoformat(),
                    }
                )
                render_reply = overlay.receive_json()
                assert render_reply == {
                    "kind": "render_acknowledged",
                    "event_id": str(event.event_id),
                    "duplicate": False,
                }

                source.send_text(event.canonical_json())
                duplicate = source.receive_json()
                assert duplicate["status"] == "duplicate"

        evidence = client.get(f"/api/runs/{RUN_ID}/events/{event.event_id}").json()
        assert len(evidence["delivery_attempts"]) == 2
        assert len(evidence["processing_attempts"]) == 1
        assert len(evidence["dispatches"]) == 1
        assert len(evidence["render_acknowledgments"]) == 1

        started_at = client.get("/api/runs").json()["runs"][0]["started_at"]
        assert datetime.fromisoformat(started_at).utcoffset().total_seconds() == 0

        malformed_before = client.get(f"/api/runs/{RUN_ID}/failures").json()
        assert malformed_before["payload_rejection_categories"] == []


def test_render_ack_waits_for_successful_dispatch_persistence(tmp_path) -> None:
    repository = Repository(tmp_path / "render-ordering.duckdb")
    try:
        config = _config()
        event = config.manifest[0]
        repository.create_run(config)
        result = IngestService(repository).ingest_text(
            event.canonical_json(),
            "render-order-source",
            RUN_ID,
        )
        effect = IngestService(repository).record_reply_and_process(result)
        assert effect is not None
        session_id = "render-order-browser"
        repository.open_overlay_session(session_id, RUN_ID)

        class PausedWebSocket:
            def __init__(self) -> None:
                self.effect_sent = asyncio.Event()
                self.release_send = asyncio.Event()

            async def send_json(self, _payload: object) -> None:
                self.effect_sent.set()
                await self.release_send.wait()

            async def close(
                self,
                code: int = 1000,
                reason: str | None = None,
            ) -> None:
                del code, reason

        async def prove_ordering() -> None:
            socket = PausedWebSocket()
            connection = OverlayConnection(
                websocket=socket,  # type: ignore[arg-type]
                run_id=RUN_ID,
                session_id=session_id,
            )
            hub = OverlayHub(repository)
            await hub.add(connection)
            send_task = asyncio.create_task(hub.send_effect(session_id, effect))
            await socket.effect_sent.wait()
            ack_task = asyncio.create_task(
                hub.record_render_ack(
                    connection,
                    RenderAckMessage(
                        kind="render_ack",
                        event_id=event.event_id,
                        rendered_at=datetime.now(UTC),
                    ),
                )
            )
            await asyncio.sleep(0)
            assert not ack_task.done()

            socket.release_send.set()
            assert await send_task is True
            assert await ack_task is True

        asyncio.run(prove_ordering())
        assert repository.query("SELECT outcome FROM overlay_dispatches") == [
            {"outcome": "sent"}
        ]
        assert (
            repository.query("SELECT COUNT(*) AS count FROM render_acknowledgments")[0][
                "count"
            ]
            == 1
        )
    finally:
        repository.close()


def test_completed_run_closes_ingest_sockets_without_mutating_evidence(
    tmp_path,
) -> None:
    application = create_app(tmp_path / "completed-sockets.duckdb")
    config = _config()
    event = config.manifest[0]
    with TestClient(application) as client:
        assert (
            client.post("/api/runs", json=config.model_dump(mode="json")).status_code
            == 201
        )
        overlay_url = f"/ws/overlay?run_id={RUN_ID}&session_id=completed-browser"
        source_url = f"/ws/ingest?connection_id=active-source&run_id={RUN_ID}"
        with client.websocket_connect(overlay_url) as overlay:
            assert overlay.receive_json()["kind"] == "overlay_ready"
            with client.websocket_connect(source_url) as source:
                source.send_text(event.canonical_json())
                assert source.receive_json()["status"] == "accepted"
                effect = overlay.receive_json()
                assert effect["event"]["event_id"] == str(event.event_id)
                overlay.send_json(
                    {
                        "kind": "render_ack",
                        "event_id": str(event.event_id),
                        "rendered_at": datetime.now(UTC).isoformat(),
                    }
                )
                assert overlay.receive_json()["kind"] == "render_acknowledged"
                completion = client.post(
                    f"/api/runs/{RUN_ID}/complete",
                    json={
                        "generated_count": 1,
                        "client_acked_count": 1,
                        "retry_count": 0,
                    },
                )
                assert completion.status_code == 200
                before = client.get(f"/api/runs/{RUN_ID}/overview").json()
                assert before["verdict"] == "pass"

                source.send_text(event.canonical_json())
                with pytest.raises(WebSocketDisconnect) as closed:
                    source.receive_json()
                assert closed.value.code == 4409

            with pytest.raises(WebSocketDisconnect) as refused:
                with client.websocket_connect(
                    f"/ws/ingest?connection_id=late-source&run_id={RUN_ID}"
                ):
                    pass
            assert refused.value.code == 4409

        after = client.get(f"/api/runs/{RUN_ID}/overview").json()
        for key in (
            "delivered",
            "unique_events",
            "acknowledged",
            "processed",
            "dispatched",
            "rendered",
            "duplicates",
            "payload_rejections",
            "conflicts",
            "verdict",
        ):
            assert after[key] == before[key]
        assert after["delivered"] == 1
        assert after["verdict"] == "pass"


def test_ingest_socket_rejects_event_bound_to_another_run(tmp_path) -> None:
    application = create_app(tmp_path / "run-binding.duckdb")
    first = _config()
    second_run_id = uuid4()
    second = build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=56,
        event_count=1,
        event_rate=100,
        run_id=second_run_id,
    )
    with TestClient(application) as client:
        for config in (first, second):
            response = client.post("/api/runs", json=config.model_dump(mode="json"))
            assert response.status_code == 201

        with client.websocket_connect(
            f"/ws/ingest?connection_id=bound-source&run_id={RUN_ID}"
        ) as source:
            source.send_text(second.manifest[0].canonical_json())
            reply = source.receive_json()

        assert reply["kind"] == "nack"
        assert reply["error"]["category"] == "RUN_ID_MISMATCH"
        repository = application.state.repository
        assert repository.query("SELECT COUNT(*) AS count FROM events")[0]["count"] == 0
        rejection = repository.query(
            "SELECT run_id, error_category FROM delivery_attempts "
            "WHERE connection_id = 'bound-source'"
        )[0]
        assert rejection == {
            "run_id": str(RUN_ID),
            "error_category": "RUN_ID_MISMATCH",
        }


def test_invalid_cross_run_payload_is_attributed_to_bound_run(tmp_path) -> None:
    application = create_app(tmp_path / "invalid-run-binding.duckdb")
    first = _config()
    second = build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=57,
        event_count=1,
        event_rate=100,
        run_id=uuid4(),
    )
    invalid_second_event = second.manifest[0].model_dump(mode="json")
    invalid_second_event.pop("actor_id")

    with TestClient(application) as client:
        for config in (first, second):
            response = client.post("/api/runs", json=config.model_dump(mode="json"))
            assert response.status_code == 201

        with client.websocket_connect(
            f"/ws/ingest?connection_id=bound-invalid&run_id={RUN_ID}"
        ) as source:
            source.send_json(invalid_second_event)
            reply = source.receive_json()

        assert reply["kind"] == "nack"
        assert reply["error"]["category"] == "RUN_ID_MISMATCH"
        attempt = application.state.repository.query(
            "SELECT run_id, event_id, error_category FROM delivery_attempts "
            "WHERE connection_id = 'bound-invalid'"
        )[0]
        assert attempt == {
            "run_id": str(RUN_ID),
            "event_id": str(second.manifest[0].event_id),
            "error_category": "RUN_ID_MISMATCH",
        }
        second_failures = client.get(f"/api/runs/{second.run_id}/failures").json()
        assert second_failures["payload_rejection_categories"] == []


def test_application_startup_recovers_and_replays_unprocessed_event(tmp_path) -> None:
    database_path = tmp_path / "application-recovery.duckdb"
    config = _config()
    first_application = create_app(database_path)
    with TestClient(first_application) as client:
        assert (
            client.post("/api/runs", json=config.model_dump(mode="json")).status_code
            == 201
        )
        persisted = first_application.state.ingest_service.ingest_text(
            config.manifest[0].canonical_json(),
            "crash-window",
            RUN_ID,
        )
        assert persisted.reply.kind == "ack"

    restarted_application = create_app(database_path)
    with TestClient(restarted_application) as client:
        overlay_url = f"/ws/overlay?run_id={RUN_ID}&session_id=recovery-browser"
        with client.websocket_connect(overlay_url) as overlay:
            assert overlay.receive_json()["kind"] == "overlay_ready"
            replayed = overlay.receive_json()
            assert replayed["kind"] == "effect"
            assert replayed["event"]["event_id"] == str(config.manifest[0].event_id)
            overlay.send_json(
                {
                    "kind": "render_ack",
                    "event_id": str(config.manifest[0].event_id),
                    "rendered_at": datetime.now(UTC).isoformat(),
                }
            )
            assert overlay.receive_json()["kind"] == "render_acknowledged"

        repository = restarted_application.state.repository
        canonical = repository.query(
            "SELECT acknowledged_at, processed_at FROM events WHERE event_id = ?",
            [str(config.manifest[0].event_id)],
        )[0]
        assert canonical["acknowledged_at"] is None
        assert canonical["processed_at"] is not None
        assert (
            repository.query(
                "SELECT COUNT(*) AS count FROM processing_attempts "
                "WHERE outcome = 'success'"
            )[0]["count"]
            == 1
        )
        assert (
            repository.query("SELECT COUNT(*) AS count FROM render_acknowledgments")[0][
                "count"
            ]
            == 1
        )
