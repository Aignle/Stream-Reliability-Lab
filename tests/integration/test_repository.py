"""DuckDB lifecycle, idempotency, recovery, and render evidence tests."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from threading import Barrier
from uuid import UUID, uuid4

import duckdb
import pytest

from streamlab.models import AttemptOutcome, ScenarioName
from streamlab.repository import Repository
from streamlab.service import IngestService
from streamlab.simulator import build_scenario

RUN_ID = UUID("44444444-4444-4444-8444-444444444444")


def _config(event_count: int = 2):
    return build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=44,
        event_count=event_count,
        event_rate=100,
        run_id=RUN_ID,
    )


def test_valid_delivery_is_committed_before_ack_result(tmp_path) -> None:
    database_path = tmp_path / "before-ack.duckdb"
    repository = Repository(database_path)
    try:
        config = _config(1)
        repository.create_run(config)
        service = IngestService(repository)
        result = service.ingest_text(config.manifest[0].canonical_json(), "source-1")

        assert result.reply.kind == "ack"
        with duckdb.connect(str(database_path)) as observer:
            event_count, attempt_count, acknowledged, processed = observer.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM events),
                  (SELECT COUNT(*) FROM delivery_attempts),
                  (SELECT COUNT(*) FROM events WHERE acknowledged_at IS NOT NULL),
                  (SELECT COUNT(*) FROM events WHERE processed_at IS NOT NULL)
                """
            ).fetchone()
        assert (event_count, attempt_count, acknowledged, processed) == (1, 1, 0, 0)

        effect = service.record_reply_and_process(result)
        assert effect is not None
        evidence = repository.query(
            """
            SELECT acknowledged_at, processed_at FROM events WHERE event_id = ?
            """,
            [str(config.manifest[0].event_id)],
        )[0]
        assert evidence["acknowledged_at"] is not None
        assert evidence["processed_at"] is not None
    finally:
        repository.close()


def test_commit_failure_cannot_produce_accepted_result(tmp_path, monkeypatch) -> None:
    repository = Repository(tmp_path / "commit-failure.duckdb")
    try:
        config = _config(1)
        repository.create_run(config)
        service = IngestService(repository)

        def fail_commit() -> None:
            raise RuntimeError("injected commit failure")

        monkeypatch.setattr(repository, "_commit", fail_commit)
        with pytest.raises(RuntimeError, match="injected commit failure"):
            service.ingest_text(config.manifest[0].canonical_json(), "source-1")

        assert repository.query("SELECT COUNT(*) AS count FROM events")[0]["count"] == 0
        assert (
            repository.query("SELECT COUNT(*) AS count FROM delivery_attempts")[0][
                "count"
            ]
            == 0
        )
    finally:
        repository.close()


def test_concurrent_duplicate_delivery_is_idempotent(tmp_path) -> None:
    repository = Repository(tmp_path / "duplicates.duckdb")
    try:
        config = _config(1)
        repository.create_run(config)
        service = IngestService(repository)
        raw = config.manifest[0].canonical_json()
        barrier = Barrier(2)

        def deliver(connection: str):
            barrier.wait()
            return service.ingest_text(raw, connection)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(deliver, ("source-a", "source-b")))

        assert {item.reply.status for item in results} == {
            AttemptOutcome.ACCEPTED,
            AttemptOutcome.DUPLICATE,
        }
        effects = [service.record_reply_and_process(item) for item in results]
        assert sum(effect is not None for effect in effects) == 1
        assert repository.query("SELECT COUNT(*) AS count FROM events")[0]["count"] == 1
        assert (
            repository.query("SELECT COUNT(*) AS count FROM delivery_attempts")[0][
                "count"
            ]
            == 2
        )
        assert (
            repository.query("SELECT COUNT(*) AS count FROM processing_attempts")[0][
                "count"
            ]
            == 1
        )
    finally:
        repository.close()


def test_changed_content_for_existing_event_is_audited_as_conflict(tmp_path) -> None:
    repository = Repository(tmp_path / "conflict.duckdb")
    try:
        config = _config(1)
        repository.create_run(config)
        service = IngestService(repository)
        accepted = service.ingest_text(
            config.manifest[0].canonical_json(),
            "source-a",
            RUN_ID,
        )
        assert service.record_reply_and_process(accepted) is not None

        changed = config.manifest[0].model_dump(mode="json")
        changed["actor_id"] = (
            "synthetic-actor-999"
            if changed["actor_id"] != "synthetic-actor-999"
            else "synthetic-actor-998"
        )
        conflict = service.ingest_text(
            json.dumps(changed),
            "source-b",
            RUN_ID,
        )
        assert conflict.reply.kind == "nack"
        assert conflict.reply.status is AttemptOutcome.CONFLICT
        assert conflict.reply.error is not None
        assert conflict.reply.error.category.value == "EVENT_ID_CONFLICT"
        assert service.record_reply_and_process(conflict) is None

        attempts = repository.query(
            "SELECT outcome FROM delivery_attempts ORDER BY received_at"
        )
        assert [row["outcome"] for row in attempts] == ["accepted", "conflict"]
        assert repository.query("SELECT COUNT(*) AS count FROM events")[0]["count"] == 1
        assert (
            repository.query(
                "SELECT COUNT(*) AS count FROM processing_attempts "
                "WHERE outcome = 'success'"
            )[0]["count"]
            == 1
        )
        stored_actor = repository.query("SELECT actor_id FROM events")[0]["actor_id"]
        assert stored_actor == config.manifest[0].actor_id
    finally:
        repository.close()


def test_recovery_closes_post_ack_evidence_crash_window(tmp_path) -> None:
    database_path = tmp_path / "recovery.duckdb"
    config = _config(1)
    repository = Repository(database_path)
    repository.create_run(config)
    service = IngestService(repository)
    persisted = service.ingest_text(
        config.manifest[0].canonical_json(),
        "source-1",
    )
    assert persisted.reply.kind == "ack"
    # Simulate a crash after the ACK frame was written but before reply evidence
    # or processing committed.
    repository.close()

    recovered_repository = Repository(database_path)
    try:
        recovered_service = IngestService(recovered_repository)
        recovered = recovered_service.recover_pending()
        assert [item.event_id for item in recovered] == [config.manifest[0].event_id]
        assert recovered_service.recover_pending() == []
        event = recovered_repository.query(
            "SELECT acknowledged_at, processed_at FROM events WHERE event_id = ?",
            [str(config.manifest[0].event_id)],
        )[0]
        assert event["acknowledged_at"] is None
        assert event["processed_at"] is not None

        retry = recovered_service.ingest_text(
            config.manifest[0].canonical_json(),
            "source-2",
        )
        assert retry.reply.status is AttemptOutcome.DUPLICATE
        assert recovered_service.record_reply_and_process(retry) is None
        assert (
            recovered_repository.query("SELECT COUNT(*) AS count FROM events")[0][
                "count"
            ]
            == 1
        )
        assert (
            recovered_repository.query(
                "SELECT COUNT(*) AS count FROM processing_attempts "
                "WHERE outcome = 'success'"
            )[0]["count"]
            == 1
        )
    finally:
        recovered_repository.close()


def test_render_ack_requires_matching_successful_dispatch(tmp_path) -> None:
    repository = Repository(tmp_path / "render.duckdb")
    try:
        config = _config(1)
        repository.create_run(config)
        service = IngestService(repository)
        result = service.ingest_text(config.manifest[0].canonical_json(), "source")
        effect = service.record_reply_and_process(result)
        assert effect is not None
        session_id = "overlay-test"
        repository.open_overlay_session(session_id, RUN_ID)

        with pytest.raises(ValueError, match="not dispatched"):
            repository.record_render_ack(
                event_id=effect.event_id,
                session_id=session_id,
                rendered_at=datetime.now(UTC),
            )
        with pytest.raises(ValueError, match="not dispatched"):
            repository.record_render_ack(
                event_id=uuid4(),
                session_id=session_id,
                rendered_at=datetime.now(UTC),
            )

        repository.record_dispatch(
            event_id=effect.event_id,
            session_id=session_id,
            outcome="sent",
        )
        assert repository.record_render_ack(
            event_id=effect.event_id,
            session_id=session_id,
            rendered_at=datetime.now(UTC),
        )
        assert not repository.record_render_ack(
            event_id=effect.event_id,
            session_id=session_id,
            rendered_at=datetime.now(UTC),
        )
        assert repository.pending_effects_for_session(RUN_ID, session_id) == []

        second_session = "overlay-second"
        repository.open_overlay_session(second_session, RUN_ID)
        assert [
            item.event_id
            for item in repository.pending_effects_for_session(RUN_ID, second_session)
        ] == [effect.event_id]
    finally:
        repository.close()


def test_completion_cannot_rewrite_manifest_evidence(tmp_path) -> None:
    repository = Repository(tmp_path / "completion.duckdb")
    try:
        config = _config(2)
        repository.create_run(config)
        with pytest.raises(ValueError, match="manifest"):
            repository.complete_run(RUN_ID, 999, 0, 0)
        with pytest.raises(ValueError, match="cannot exceed"):
            repository.complete_run(RUN_ID, 2, 3, 0)
        repository.complete_run(RUN_ID, 2, 2, 1)
        repository.complete_run(RUN_ID, 2, 2, 1)
        with pytest.raises(ValueError, match="cannot be rewritten"):
            repository.complete_run(RUN_ID, 2, 1, 0)

        run = repository.query(
            "SELECT generated_count, client_acked_count, retry_count FROM runs"
        )[0]
        assert run == {
            "generated_count": 2,
            "client_acked_count": 2,
            "retry_count": 1,
        }
    finally:
        repository.close()
