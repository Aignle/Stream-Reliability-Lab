"""DuckDB persistence and evidence queries owned by the FastAPI service."""

from __future__ import annotations

import hashlib
import json
import logging
import threading
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import UUID, uuid4

import duckdb

from streamlab.models import (
    AttemptOutcome,
    ConnectionEventKind,
    ConnectionEventRequest,
    ConnectionSide,
    ErrorCategory,
    EventType,
    NormalizedEvent,
    OverlayEffect,
    ScenarioConfig,
    utc_now,
)

logger = logging.getLogger(__name__)

SUBMITTED_CONNECTION_EVENT_KINDS = {
    ConnectionEventKind.FORCED_DISCONNECT,
    ConnectionEventKind.RECONNECTED,
    ConnectionEventKind.RECOVERY_COMPLETE,
    ConnectionEventKind.DELAY_INJECTED,
}


@dataclass(frozen=True, slots=True)
class PersistedDelivery:
    """Result of atomically auditing and accepting one valid delivery."""

    attempt_id: UUID
    outcome: AttemptOutcome
    event_id: UUID
    run_id: UUID
    persisted: bool
    duplicate: bool
    error_category: ErrorCategory | None = None
    error_message: str | None = None


@dataclass(frozen=True, slots=True)
class RejectedDelivery:
    """Persisted evidence for a delivery that did not create an event."""

    attempt_id: UUID
    event_id: UUID | None
    run_id: UUID | None
    error_category: ErrorCategory
    error_message: str


class Repository:
    """Serialized DuckDB writer and read-query boundary for the application."""

    def __init__(self, database_path: str | Path) -> None:
        self.database_path = str(database_path)
        if self.database_path != ":memory:":
            Path(self.database_path).resolve().parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._connection = duckdb.connect(self.database_path)
        self._connection.execute("SET TimeZone='UTC'")
        self._initialize_schema()

    def close(self) -> None:
        """Close the application-owned database connection."""
        with self._lock:
            self._connection.close()

    def _initialize_schema(self) -> None:
        statements = [
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id VARCHAR PRIMARY KEY,
                scenario VARCHAR NOT NULL,
                seed BIGINT NOT NULL,
                requested_count INTEGER NOT NULL,
                event_rate DOUBLE NOT NULL,
                burst_start_sequence INTEGER,
                disconnect_sequence INTEGER,
                config_json VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ,
                generated_count INTEGER NOT NULL,
                client_acked_count INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS generated_events (
                event_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                source_sequence INTEGER NOT NULL,
                event_type VARCHAR NOT NULL,
                generated_at TIMESTAMPTZ NOT NULL,
                envelope_json VARCHAR NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS delivery_attempts (
                attempt_id VARCHAR PRIMARY KEY,
                run_id VARCHAR,
                event_id VARCHAR,
                connection_id VARCHAR NOT NULL,
                received_at TIMESTAMPTZ NOT NULL,
                raw_payload VARCHAR NOT NULL,
                outcome VARCHAR NOT NULL,
                error_category VARCHAR,
                error_message VARCHAR,
                duplicate BOOLEAN NOT NULL DEFAULT FALSE,
                conflict BOOLEAN NOT NULL DEFAULT FALSE,
                response_sent_at TIMESTAMPTZ
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                canonical_hash VARCHAR NOT NULL,
                schema_version VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                source_sequence INTEGER NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                actor_id VARCHAR NOT NULL,
                payload_json VARCHAR NOT NULL,
                validated_at TIMESTAMPTZ NOT NULL,
                persisted_at TIMESTAMPTZ NOT NULL,
                acknowledged_at TIMESTAMPTZ,
                processed_at TIMESTAMPTZ,
                first_dispatched_at TIMESTAMPTZ,
                rendered_at TIMESTAMPTZ,
                render_acknowledged_at TIMESTAMPTZ,
                arrival_index INTEGER NOT NULL,
                out_of_order BOOLEAN NOT NULL,
                sequence_gap INTEGER NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS processing_attempts (
                processing_attempt_id VARCHAR PRIMARY KEY,
                event_id VARCHAR NOT NULL,
                effect_id VARCHAR,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ NOT NULL,
                outcome VARCHAR NOT NULL,
                error_message VARCHAR
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS overlay_sessions (
                session_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                connected_at TIMESTAMPTZ NOT NULL,
                disconnected_at TIMESTAMPTZ,
                reconnect_count INTEGER NOT NULL DEFAULT 0
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS overlay_dispatches (
                dispatch_id VARCHAR PRIMARY KEY,
                event_id VARCHAR NOT NULL,
                session_id VARCHAR NOT NULL,
                attempt_no INTEGER NOT NULL,
                dispatched_at TIMESTAMPTZ NOT NULL,
                outcome VARCHAR NOT NULL,
                error_message VARCHAR
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS render_acknowledgments (
                event_id VARCHAR NOT NULL,
                session_id VARCHAR NOT NULL,
                rendered_at TIMESTAMPTZ NOT NULL,
                acknowledged_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(event_id, session_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS connection_events (
                connection_event_id VARCHAR PRIMARY KEY,
                run_id VARCHAR,
                side VARCHAR NOT NULL,
                kind VARCHAR NOT NULL,
                connection_id VARCHAR NOT NULL,
                occurred_at TIMESTAMPTZ NOT NULL,
                detail_json VARCHAR NOT NULL
            )
            """,
        ]
        with self._lock:
            for statement in statements:
                self._connection.execute(statement)
            self._connection.commit()

    @contextmanager
    def _transaction(self) -> Generator[None, None, None]:
        """Run a serialized transaction without awaiting network operations."""
        with self._lock:
            self._connection.execute("BEGIN TRANSACTION")
            try:
                yield
                self._commit()
            except Exception:
                self._connection.rollback()
                raise

    def _commit(self) -> None:
        """Commit hook kept narrow so failure behavior can be tested."""
        self._connection.commit()

    def query(
        self,
        sql: str,
        parameters: Sequence[object] = (),
    ) -> list[dict[str, object]]:
        """Return read-only query rows as dictionaries."""
        with self._lock:
            cursor = self._connection.execute(sql, parameters)
            columns = [item[0] for item in cursor.description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    def ping(self) -> bool:
        """Confirm the database connection can answer a simple query."""
        return bool(self.query("SELECT 1 AS ready")[0]["ready"])

    def create_run(self, config: ScenarioConfig) -> None:
        """Persist a run and complete generated-event manifest atomically."""
        started_at = utc_now()
        config_json = json.dumps(
            config.model_dump(mode="json", exclude={"manifest"}),
            sort_keys=True,
        )
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    str(config.run_id),
                    config.scenario.value,
                    config.seed,
                    config.event_count,
                    config.event_rate,
                    config.burst_start_sequence,
                    config.disconnect_sequence,
                    config_json,
                    "running",
                    started_at,
                    None,
                    len(config.manifest),
                    0,
                    0,
                ],
            )
            self._connection.executemany(
                "INSERT INTO generated_events VALUES (?, ?, ?, ?, ?, ?)",
                [
                    [
                        str(event.event_id),
                        str(event.run_id),
                        event.source_sequence,
                        event.event_type.value,
                        event.occurred_at,
                        event.canonical_json(),
                    ]
                    for event in config.manifest
                ],
            )

    def run_exists(self, run_id: UUID) -> bool:
        rows = self.query(
            "SELECT 1 AS present FROM runs WHERE run_id = ?",
            [str(run_id)],
        )
        return bool(rows)

    def generated_event_hash(self, event_id: UUID, run_id: UUID) -> str | None:
        """Return the manifest envelope hash for an event in a run."""
        rows = self.query(
            """
            SELECT envelope_json FROM generated_events
            WHERE event_id = ? AND run_id = ?
            """,
            [str(event_id), str(run_id)],
        )
        if not rows:
            return None
        return hashlib.sha256(str(rows[0]["envelope_json"]).encode("utf-8")).hexdigest()

    def complete_run(
        self,
        run_id: UUID,
        generated_count: int,
        client_acked_count: int,
        retry_count: int,
    ) -> None:
        """Complete a run while preserving the server-owned manifest count."""
        with self._transaction():
            manifest_row = self._connection.execute(
                "SELECT COUNT(*) FROM generated_events WHERE run_id = ?",
                [str(run_id)],
            ).fetchone()
            if manifest_row is None or int(manifest_row[0]) != generated_count:
                raise ValueError("generated_count does not match the stored manifest")
            run_row = self._connection.execute(
                """
                SELECT status, client_acked_count, retry_count
                FROM runs WHERE run_id = ?
                """,
                [str(run_id)],
            ).fetchone()
            if run_row is None:
                raise KeyError(f"run {run_id} does not exist")
            if client_acked_count > generated_count:
                raise ValueError("client_acked_count cannot exceed generated_count")
            if str(run_row[0]) == "completed":
                if (
                    int(run_row[1]) == client_acked_count
                    and int(run_row[2]) == retry_count
                ):
                    return
                raise ValueError("completed run counters cannot be rewritten")
            self._connection.execute(
                """
                UPDATE runs SET status = 'completed', completed_at = ?,
                    client_acked_count = ?, retry_count = ?
                WHERE run_id = ?
                """,
                [
                    utc_now(),
                    client_acked_count,
                    retry_count,
                    str(run_id),
                ],
            )

    def record_rejected_delivery(
        self,
        *,
        raw_payload: str,
        connection_id: str,
        error_category: ErrorCategory,
        error_message: str,
        event_id: UUID | None = None,
        run_id: UUID | None = None,
    ) -> RejectedDelivery:
        """Audit an invalid raw delivery without creating a canonical event."""
        attempt_id = uuid4()
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO delivery_attempts
                (attempt_id, run_id, event_id, connection_id, received_at,
                 raw_payload, outcome, error_category, error_message, duplicate,
                 conflict, response_sent_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, FALSE, NULL)
                """,
                [
                    str(attempt_id),
                    str(run_id) if run_id else None,
                    str(event_id) if event_id else None,
                    connection_id,
                    utc_now(),
                    raw_payload,
                    AttemptOutcome.REJECTED.value,
                    error_category.value,
                    error_message,
                ],
            )
        return RejectedDelivery(
            attempt_id=attempt_id,
            event_id=event_id,
            run_id=run_id,
            error_category=error_category,
            error_message=error_message,
        )

    def record_conflicted_delivery(
        self,
        *,
        raw_payload: str,
        connection_id: str,
        event_id: UUID,
        run_id: UUID,
        error_message: str,
    ) -> PersistedDelivery:
        """Audit an event-identity conflict without mutating canonical state."""
        attempt_id = uuid4()
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO delivery_attempts
                (attempt_id, run_id, event_id, connection_id, received_at,
                 raw_payload, outcome, error_category, error_message, duplicate,
                 conflict, response_sent_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, TRUE, NULL)
                """,
                [
                    str(attempt_id),
                    str(run_id),
                    str(event_id),
                    connection_id,
                    utc_now(),
                    raw_payload,
                    AttemptOutcome.CONFLICT.value,
                    ErrorCategory.EVENT_ID_CONFLICT.value,
                    error_message,
                ],
            )
        return PersistedDelivery(
            attempt_id=attempt_id,
            outcome=AttemptOutcome.CONFLICT,
            event_id=event_id,
            run_id=run_id,
            persisted=False,
            duplicate=False,
            error_category=ErrorCategory.EVENT_ID_CONFLICT,
            error_message=error_message,
        )

    def persist_valid_delivery(
        self,
        *,
        raw_payload: str,
        connection_id: str,
        event: NormalizedEvent,
    ) -> PersistedDelivery:
        """Audit a valid attempt and atomically insert at most one event."""
        attempt_id = uuid4()
        received_at = utc_now()
        event_id = str(event.event_id)
        run_id = str(event.run_id)
        canonical_hash = event.canonical_hash()
        with self._transaction():
            existing = self._connection.execute(
                "SELECT canonical_hash FROM events WHERE event_id = ?",
                [event_id],
            ).fetchone()
            if existing is not None:
                conflict = str(existing[0]) != canonical_hash
                outcome = (
                    AttemptOutcome.CONFLICT if conflict else AttemptOutcome.DUPLICATE
                )
                error_category = ErrorCategory.EVENT_ID_CONFLICT if conflict else None
                error_message = (
                    "event_id already exists with different canonical content"
                    if conflict
                    else None
                )
                self._connection.execute(
                    """
                    INSERT INTO delivery_attempts
                    (attempt_id, run_id, event_id, connection_id, received_at,
                     raw_payload, outcome, error_category, error_message,
                     duplicate, conflict, response_sent_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
                    """,
                    [
                        str(attempt_id),
                        run_id,
                        event_id,
                        connection_id,
                        received_at,
                        raw_payload,
                        outcome.value,
                        error_category.value if error_category else None,
                        error_message,
                        not conflict,
                        conflict,
                    ],
                )
                return PersistedDelivery(
                    attempt_id=attempt_id,
                    outcome=outcome,
                    event_id=event.event_id,
                    run_id=event.run_id,
                    persisted=not conflict,
                    duplicate=not conflict,
                    error_category=error_category,
                    error_message=error_message,
                )

            arrival = self._connection.execute(
                """
                SELECT COUNT(*) + 1, MAX(source_sequence)
                FROM events WHERE run_id = ?
                """,
                [run_id],
            ).fetchone()
            if arrival is None:
                raise RuntimeError("failed to calculate event arrival evidence")
            arrival_index = int(arrival[0])
            previous_max = int(arrival[1]) if arrival[1] is not None else None
            out_of_order = (
                previous_max is not None and event.source_sequence < previous_max
            )
            sequence_gap = (
                max(0, event.source_sequence - previous_max - 1)
                if previous_max is not None
                else max(0, event.source_sequence - 1)
            )
            self._connection.execute(
                """
                INSERT INTO delivery_attempts
                (attempt_id, run_id, event_id, connection_id, received_at,
                 raw_payload, outcome, error_category, error_message, duplicate,
                 conflict, response_sent_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, FALSE, FALSE, NULL)
                """,
                [
                    str(attempt_id),
                    run_id,
                    event_id,
                    connection_id,
                    received_at,
                    raw_payload,
                    AttemptOutcome.ACCEPTED.value,
                ],
            )
            self._connection.execute(
                """
                INSERT INTO events
                (event_id, run_id, canonical_hash, schema_version, source,
                 event_type, source_sequence, occurred_at, actor_id, payload_json,
                 validated_at, persisted_at, acknowledged_at, processed_at,
                 first_dispatched_at, rendered_at, render_acknowledged_at,
                 arrival_index, out_of_order, sequence_gap)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL,
                        NULL, NULL, ?, ?, ?)
                """,
                [
                    event_id,
                    run_id,
                    canonical_hash,
                    event.schema_version,
                    event.source,
                    event.event_type.value,
                    event.source_sequence,
                    event.occurred_at,
                    event.actor_id,
                    json.dumps(event.payload, sort_keys=True),
                    received_at,
                    utc_now(),
                    arrival_index,
                    out_of_order,
                    sequence_gap,
                ],
            )
        return PersistedDelivery(
            attempt_id=attempt_id,
            outcome=AttemptOutcome.ACCEPTED,
            event_id=event.event_id,
            run_id=event.run_id,
            persisted=True,
            duplicate=False,
        )

    def mark_delivery_reply_sent(
        self,
        attempt_id: UUID,
        event_id: UUID | None,
        *,
        accepted: bool,
    ) -> None:
        """Record a sent reply and event ACK evidence where applicable."""
        acknowledged_at = utc_now()
        with self._transaction():
            self._connection.execute(
                """
                UPDATE delivery_attempts SET response_sent_at = ?
                WHERE attempt_id = ?
                """,
                [acknowledged_at, str(attempt_id)],
            )
            if accepted and event_id is not None:
                self._connection.execute(
                    """
                    UPDATE events SET acknowledged_at = COALESCE(acknowledged_at, ?)
                    WHERE event_id = ?
                    """,
                    [acknowledged_at, str(event_id)],
                )

    def process_event(self, event_id: UUID) -> OverlayEffect | None:
        """Create the one successful effect for an accepted event."""
        processed_at = utc_now()
        with self._transaction():
            row = self._connection.execute(
                """
                SELECT event_id, run_id, event_type, actor_id, payload_json,
                       occurred_at, processed_at
                FROM events WHERE event_id = ?
                """,
                [str(event_id)],
            ).fetchone()
            if row is None:
                raise KeyError(f"event {event_id} does not exist")
            if row[6] is not None:
                return None
            effect_id = event_id
            self._connection.execute(
                """
                INSERT INTO processing_attempts VALUES (?, ?, ?, ?, ?, 'success', NULL)
                """,
                [
                    str(uuid4()),
                    str(event_id),
                    str(effect_id),
                    processed_at,
                    processed_at,
                ],
            )
            self._connection.execute(
                "UPDATE events SET processed_at = ? WHERE event_id = ?",
                [processed_at, str(event_id)],
            )
        return OverlayEffect(
            effect_id=effect_id,
            event_id=UUID(str(row[0])),
            run_id=UUID(str(row[1])),
            event_type=EventType(str(row[2])),
            actor_id=str(row[3]),
            payload=json.loads(str(row[4])),
            occurred_at=cast(datetime, row[5]),
        )

    def record_processing_failure(self, event_id: UUID, error: Exception) -> None:
        """Store a bounded failed processing attempt when DuckDB remains available."""
        occurred_at = utc_now()
        message = f"{type(error).__name__}: {error}"[:500]
        with self._transaction():
            self._connection.execute(
                """
                INSERT INTO processing_attempts
                VALUES (?, ?, ?, ?, ?, 'failure', ?)
                """,
                [
                    str(uuid4()),
                    str(event_id),
                    None,
                    occurred_at,
                    occurred_at,
                    message,
                ],
            )

    def recover_unprocessed(self) -> list[OverlayEffect]:
        """Process every persisted event interrupted before processing."""
        rows = self.query(
            """
            SELECT event_id FROM events
            WHERE processed_at IS NULL
            ORDER BY persisted_at
            """
        )
        recovered: list[OverlayEffect] = []
        for row in rows:
            event_id = UUID(str(row["event_id"]))
            try:
                effect = self.process_event(event_id)
            except Exception as error:
                logger.exception("Recovery processing failed for event %s", event_id)
                try:
                    self.record_processing_failure(event_id, error)
                except Exception:
                    logger.exception(
                        "Failed to store recovery failure evidence for event %s",
                        event_id,
                    )
                continue
            if effect is not None:
                recovered.append(effect)
        return recovered

    def pending_effects_for_session(
        self,
        run_id: UUID,
        session_id: str,
    ) -> list[OverlayEffect]:
        """Return processed effects not acknowledged by this browser session."""
        rows = self.query(
            """
            SELECT e.event_id, e.run_id, e.event_type, e.actor_id,
                   e.payload_json, e.occurred_at
            FROM events e
            WHERE e.run_id = ? AND e.processed_at IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM render_acknowledgments r
                  WHERE r.event_id = e.event_id AND r.session_id = ?
              )
            ORDER BY e.source_sequence
            """,
            [str(run_id), session_id],
        )
        return [
            OverlayEffect(
                effect_id=UUID(str(row["event_id"])),
                event_id=UUID(str(row["event_id"])),
                run_id=UUID(str(row["run_id"])),
                event_type=EventType(str(row["event_type"])),
                actor_id=str(row["actor_id"]),
                payload=json.loads(str(row["payload_json"])),
                occurred_at=cast(datetime, row["occurred_at"]),
            )
            for row in rows
        ]

    def open_overlay_session(self, session_id: str, run_id: UUID) -> None:
        """Create or reopen a stable browser overlay session."""
        connected_at = utc_now()
        with self._transaction():
            existing = self._connection.execute(
                """
                SELECT run_id, reconnect_count FROM overlay_sessions
                WHERE session_id = ?
                """,
                [session_id],
            ).fetchone()
            if existing is None:
                self._connection.execute(
                    "INSERT INTO overlay_sessions VALUES (?, ?, ?, NULL, 0)",
                    [session_id, str(run_id), connected_at],
                )
            else:
                if str(existing[0]) != str(run_id):
                    raise ValueError("overlay session belongs to another run")
                self._connection.execute(
                    """
                    UPDATE overlay_sessions
                    SET connected_at = ?, disconnected_at = NULL,
                        reconnect_count = reconnect_count + 1
                    WHERE session_id = ?
                    """,
                    [connected_at, session_id],
                )

    def close_overlay_session(self, session_id: str) -> None:
        with self._transaction():
            self._connection.execute(
                "UPDATE overlay_sessions SET disconnected_at = ? WHERE session_id = ?",
                [utc_now(), session_id],
            )

    def record_dispatch(
        self,
        *,
        event_id: UUID,
        session_id: str,
        outcome: str,
        error_message: str | None = None,
    ) -> None:
        """Audit every overlay send attempt and first successful dispatch."""
        dispatched_at = utc_now()
        with self._transaction():
            attempt_row = self._connection.execute(
                """
                SELECT COUNT(*) + 1 FROM overlay_dispatches
                WHERE event_id = ? AND session_id = ?
                """,
                [str(event_id), session_id],
            ).fetchone()
            if attempt_row is None:
                raise RuntimeError("failed to calculate overlay dispatch attempt")
            self._connection.execute(
                "INSERT INTO overlay_dispatches VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    str(uuid4()),
                    str(event_id),
                    session_id,
                    int(attempt_row[0]),
                    dispatched_at,
                    outcome,
                    error_message,
                ],
            )
            if outcome == "sent":
                self._connection.execute(
                    """
                    UPDATE events
                    SET first_dispatched_at = COALESCE(first_dispatched_at, ?)
                    WHERE event_id = ?
                    """,
                    [dispatched_at, str(event_id)],
                )

    def record_render_ack(
        self,
        *,
        event_id: UUID,
        session_id: str,
        rendered_at: datetime,
    ) -> bool:
        """Persist one browser render acknowledgment per event/session."""
        acknowledged_at = utc_now()
        with self._transaction():
            eligible = self._connection.execute(
                """
                SELECT 1
                FROM events e
                JOIN overlay_sessions s
                  ON s.session_id = ? AND s.run_id = e.run_id
                JOIN overlay_dispatches d
                  ON d.event_id = e.event_id AND d.session_id = s.session_id
                WHERE e.event_id = ? AND e.processed_at IS NOT NULL
                  AND d.outcome = 'sent'
                LIMIT 1
                """,
                [session_id, str(event_id)],
            ).fetchone()
            if eligible is None:
                raise ValueError("event was not dispatched to this overlay session")
            existing = self._connection.execute(
                """
                SELECT 1 FROM render_acknowledgments
                WHERE event_id = ? AND session_id = ?
                """,
                [str(event_id), session_id],
            ).fetchone()
            if existing is not None:
                return False
            self._connection.execute(
                "INSERT INTO render_acknowledgments VALUES (?, ?, ?, ?)",
                [str(event_id), session_id, rendered_at, acknowledged_at],
            )
            self._connection.execute(
                """
                UPDATE events
                SET rendered_at = COALESCE(rendered_at, ?),
                    render_acknowledged_at = COALESCE(render_acknowledged_at, ?)
                WHERE event_id = ?
                """,
                [rendered_at, acknowledged_at, str(event_id)],
            )
        return True

    def record_connection_event(
        self,
        run_id: UUID | None,
        event: ConnectionEventRequest,
    ) -> None:
        """Persist simulator or overlay connection/recovery evidence."""
        with self._transaction():
            self._connection.execute(
                "INSERT INTO connection_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    str(uuid4()),
                    str(run_id) if run_id else None,
                    event.side.value,
                    event.kind.value,
                    event.connection_id,
                    event.occurred_at,
                    json.dumps(event.detail, sort_keys=True),
                ],
            )

    def record_submitted_connection_event(
        self,
        run_id: UUID,
        event: ConnectionEventRequest,
    ) -> None:
        """Store bounded simulator evidence with server-owned time and provenance."""
        if event.side is not ConnectionSide.SIMULATOR:
            raise ValueError("submitted connection evidence must use simulator side")
        if event.kind not in SUBMITTED_CONNECTION_EVENT_KINDS:
            raise ValueError(f"submitted {event.kind.value} evidence is not allowed")
        detail = dict(event.detail)
        detail["observer"] = "simulator"
        with self._transaction():
            row = self._connection.execute(
                "SELECT status FROM runs WHERE run_id = ?",
                [str(run_id)],
            ).fetchone()
            if row is None:
                raise KeyError(f"run {run_id} does not exist")
            if str(row[0]) == "completed":
                raise ValueError("connection evidence cannot be added after completion")
            self._connection.execute(
                "INSERT INTO connection_events VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    str(uuid4()),
                    str(run_id),
                    event.side.value,
                    event.kind.value,
                    event.connection_id,
                    utc_now(),
                    json.dumps(detail, sort_keys=True),
                ],
            )
