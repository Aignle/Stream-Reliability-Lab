"""Evidence-derived analytics tests."""

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from streamlab.analytics import AnalyticsService, percentile
from streamlab.models import (
    ConnectionEventKind,
    ConnectionEventRequest,
    ConnectionSide,
    NormalizedEvent,
    ScenarioName,
)
from streamlab.repository import Repository
from streamlab.service import IngestService
from streamlab.simulator import build_scenario

RUN_ID = UUID("66666666-6666-4666-8666-666666666666")


def _process_and_render(
    repository: Repository,
    service: IngestService,
    event: NormalizedEvent,
    *,
    connection_id: str,
    session_id: str,
) -> None:
    result = service.ingest_text(event.canonical_json(), connection_id)
    effect = service.record_reply_and_process(result)
    assert effect is not None
    repository.record_dispatch(
        event_id=event.event_id,
        session_id=session_id,
        outcome="sent",
    )
    assert repository.record_render_ack(
        event_id=event.event_id,
        session_id=session_id,
        rendered_at=datetime.now(UTC),
    )


def test_percentile_interpolates_and_discloses_empty_samples() -> None:
    assert percentile([], 0.95) is None
    assert percentile([10.0], 0.99) == 10.0
    assert percentile([0.0, 10.0, 20.0, 30.0], 0.50) == 15.0
    assert percentile([0.0, 10.0, 20.0, 30.0], 0.95) == 28.5


def test_overview_performance_failures_and_event_views_reconcile(tmp_path) -> None:
    repository = Repository(tmp_path / "analytics.duckdb")
    try:
        config = build_scenario(
            ScenarioName.HAPPY_PATH,
            seed=66,
            event_count=2,
            event_rate=100,
            run_id=RUN_ID,
        )
        repository.create_run(config)
        service = IngestService(repository)
        session_id = "analytics-overlay"
        repository.open_overlay_session(session_id, RUN_ID)

        for event in config.manifest:
            result = service.ingest_text(event.canonical_json(), "source")
            effect = service.record_reply_and_process(result)
            assert effect is not None
            repository.record_dispatch(
                event_id=event.event_id,
                session_id=session_id,
                outcome="sent",
            )
            assert repository.record_render_ack(
                event_id=event.event_id,
                session_id=session_id,
                rendered_at=datetime.now(UTC),
            )

        duplicate = service.ingest_text(config.manifest[0].canonical_json(), "source")
        assert service.record_reply_and_process(duplicate) is None
        repository.complete_run(RUN_ID, 2, 2, 0)

        analytics = AnalyticsService(repository)
        overview = analytics.overview(RUN_ID)
        assert overview["generated"] == 2
        assert overview["delivered"] == 3
        assert overview["unique_events"] == 2
        assert overview["processed"] == 2
        assert overview["rendered"] == 2
        assert overview["duplicates"] == 1
        assert overview["payload_rejections"] == 0
        assert overview["operational_delivery_failures"] == 0
        assert overview["payload_rejection_rate_percent"] == 0.0
        assert overview["payload_rejection_rate_definition"] == (
            "payload-rejected delivery attempts / all delivery attempts; "
            "identity conflicts and operational ingestion failures are separate"
        )
        assert overview["processing_attempts"] == 2
        assert overview["processing_attempt_successes"] == 2
        assert overview["processing_attempt_failures"] == 0
        assert overview["processing_attempt_success_percent"] == 100.0
        assert overview["processing_completion_percent"] == 100.0
        assert overview["render_completion_percent"] == 100.0
        assert overview["latency_sample_count"] == 2
        assert overview["scenario_checks"] == {
            "planned_duplicates": {
                "required": False,
                "passed": True,
                "expected_minimum": 0,
                "observed": 0,
                "missing_sequences": [],
            },
            "forced_reconnect": {
                "required": False,
                "passed": True,
                "forced_disconnects": 0,
                "reconnections": 0,
                "recovery_completions": 0,
                "retries": 0,
                "target_event_id": None,
                "target_accepted_attempts": 0,
                "target_duplicate_attempts": 0,
                "target_correlation": False,
                "transport_correlated": False,
                "attempt_path_correlated": False,
                "accepted_reply_sent_on_forced_transport": 0,
                "duplicate_reply_sent_on_reconnected_transport": 0,
            },
            "out_of_order_delivery": {
                "required": True,
                "planned_out_of_order": False,
                "passed": True,
                "observed_out_of_order_events": 0,
                "expected_canonical_count": 2,
                "observed_canonical_count": 2,
                "mismatch_count": 0,
                "first_mismatch": None,
            },
            "delayed_delivery": {
                "required": False,
                "passed": True,
                "configured_delay_ms": 0,
                "expected_minimum": 0,
                "observed": 0,
                "missing_sequences": [],
                "measured_delay_ms": {},
            },
        }
        assert overview["verdict"] == "pass"
        assert overview["verdict_failures"] == []

        performance = analytics.performance(RUN_ID)
        assert performance["sample_count"] == 2
        assert performance["percentiles_ms"]["p50"] is not None
        assert sum(item["count"] for item in performance["latency_distribution"]) == 2

        failures = analytics.failures(RUN_ID)
        assert len(failures["duplicate_deliveries"]) == 1
        assert failures["payload_rejection_categories"] == []
        assert failures["conflict_categories"] == []
        assert failures["operational_delivery_failures"] == []
        assert failures["unrendered_events"] == []
        assert failures["processing_failures"] == []

        selected_type = config.manifest[0].event_type.value
        events = analytics.event_table(RUN_ID, search=selected_type)
        assert events["total"] >= 1
        assert all(item["event_type"] == selected_type for item in events["events"])
        evidence = analytics.event_evidence(RUN_ID, config.manifest[0].event_id)
        assert len(evidence["delivery_attempts"]) == 2
        assert "render acknowledged" in {item["stage"] for item in evidence["timeline"]}
    finally:
        repository.close()


@pytest.mark.parametrize(
    ("scenario", "check_name", "failure_fragment"),
    [
        (
            ScenarioName.DUPLICATE_DELIVERY,
            "planned_duplicates",
            "Duplicate plan is missing stored duplicate attempts",
        ),
        (
            ScenarioName.FORCED_RECONNECT,
            "forced_reconnect",
            "Reconnect plan requires stored forced-disconnect",
        ),
        (
            ScenarioName.DELAYED_OUT_OF_ORDER,
            "out_of_order_delivery",
            "Stored canonical arrival order does not match the configured "
            "delivery order",
        ),
        (
            ScenarioName.DELAYED_OUT_OF_ORDER,
            "delayed_delivery",
            "Delay plan is missing stored delay evidence",
        ),
    ],
)
def test_overview_fails_without_planned_scenario_evidence(
    tmp_path,
    scenario: ScenarioName,
    check_name: str,
    failure_fragment: str,
) -> None:
    repository = Repository(tmp_path / f"missing-{scenario.value}.duckdb")
    try:
        config = build_scenario(
            scenario,
            seed=67,
            event_count=12,
            event_rate=100,
            run_id=uuid4(),
        )
        repository.create_run(config)
        service = IngestService(repository)
        session_id = f"missing-{scenario.value}"
        repository.open_overlay_session(session_id, config.run_id)

        # Deliberately satisfy the generic lifecycle while skipping the
        # scenario-specific duplicate, reconnect, or delivery-order plan.
        for event in config.manifest:
            result = service.ingest_text(event.canonical_json(), "source")
            effect = service.record_reply_and_process(result)
            assert effect is not None
            repository.record_dispatch(
                event_id=event.event_id,
                session_id=session_id,
                outcome="sent",
            )
            assert repository.record_render_ack(
                event_id=event.event_id,
                session_id=session_id,
                rendered_at=datetime.now(UTC),
            )

        repository.complete_run(
            config.run_id,
            config.event_count,
            config.event_count,
            0,
        )

        overview = AnalyticsService(repository).overview(config.run_id)
        scenario_check = overview["scenario_checks"][check_name]
        assert scenario_check["required"] is True
        assert scenario_check["passed"] is False
        assert overview["verdict"] == "fail"
        assert failure_fragment in overview["verdict_reason"]
        assert any(
            failure_fragment in reason for reason in overview["verdict_failures"]
        )
    finally:
        repository.close()


def test_overview_fails_when_client_ack_count_does_not_reconcile(tmp_path) -> None:
    repository = Repository(tmp_path / "missing-client-ack.duckdb")
    try:
        config = build_scenario(
            ScenarioName.HAPPY_PATH,
            seed=68,
            event_count=2,
            event_rate=100,
            run_id=uuid4(),
        )
        repository.create_run(config)
        service = IngestService(repository)
        session_id = "missing-client-ack"
        repository.open_overlay_session(session_id, config.run_id)

        for event in config.manifest:
            result = service.ingest_text(event.canonical_json(), "source")
            effect = service.record_reply_and_process(result)
            assert effect is not None
            repository.record_dispatch(
                event_id=event.event_id,
                session_id=session_id,
                outcome="sent",
            )
            assert repository.record_render_ack(
                event_id=event.event_id,
                session_id=session_id,
                rendered_at=datetime.now(UTC),
            )

        repository.complete_run(config.run_id, config.event_count, 1, 0)

        overview = AnalyticsService(repository).overview(config.run_id)
        failure = (
            "Expected the simulator client to acknowledge 2 unique events; "
            "it reported 1."
        )
        assert overview["client_acked_unique"] == 1
        assert overview["verdict"] == "fail"
        assert failure in overview["verdict_failures"]
        assert failure in overview["verdict_reason"]
    finally:
        repository.close()


def test_overview_fails_without_stored_ack_send_evidence(tmp_path) -> None:
    repository = Repository(tmp_path / "missing-stored-ack.duckdb")
    try:
        config = build_scenario(
            ScenarioName.HAPPY_PATH,
            seed=681,
            event_count=1,
            event_rate=100,
            run_id=uuid4(),
        )
        event = config.manifest[0]
        repository.create_run(config)
        repository.open_overlay_session("ack-proof-overlay", config.run_id)
        result = IngestService(repository).ingest_text(
            event.canonical_json(),
            "ack-proof-source",
        )

        # Satisfy downstream lifecycle stages without recording the reply send.
        effect = repository.process_event(event.event_id)
        assert effect is not None
        repository.record_dispatch(
            event_id=event.event_id,
            session_id="ack-proof-overlay",
            outcome="sent",
        )
        assert repository.record_render_ack(
            event_id=event.event_id,
            session_id="ack-proof-overlay",
            rendered_at=datetime.now(UTC),
        )
        repository.complete_run(config.run_id, 1, 1, 0)

        overview = AnalyticsService(repository).overview(config.run_id)
        failure = "Expected 1 unique events with stored ACK-send evidence; observed 0."
        assert result.reply.status.value == "accepted"
        assert overview["acknowledged"] == 0
        assert overview["processed"] == 1
        assert overview["rendered"] == 1
        assert overview["verdict"] == "fail"
        assert failure in overview["verdict_failures"]
    finally:
        repository.close()


def test_overview_fails_after_processing_failure_recovers(
    tmp_path,
    monkeypatch,
) -> None:
    repository = Repository(tmp_path / "recovered-processing-failure.duckdb")
    try:
        config = build_scenario(
            ScenarioName.HAPPY_PATH,
            seed=6811,
            event_count=1,
            event_rate=100,
            run_id=uuid4(),
        )
        event = config.manifest[0]
        repository.create_run(config)
        repository.open_overlay_session("recovery-overlay", config.run_id)
        service = IngestService(repository)
        result = service.ingest_text(event.canonical_json(), "recovery-source")
        original_process = repository.process_event

        def fail_processing(_event_id):
            raise RuntimeError("injected recoverable processing failure")

        monkeypatch.setattr(repository, "process_event", fail_processing)
        with pytest.raises(RuntimeError, match="recoverable processing failure"):
            service.record_reply_and_process(result)
        monkeypatch.setattr(repository, "process_event", original_process)

        recovered = service.recover_pending()
        assert [effect.event_id for effect in recovered] == [event.event_id]
        repository.record_dispatch(
            event_id=event.event_id,
            session_id="recovery-overlay",
            outcome="sent",
        )
        assert repository.record_render_ack(
            event_id=event.event_id,
            session_id="recovery-overlay",
            rendered_at=datetime.now(UTC),
        )
        repository.complete_run(config.run_id, 1, 1, 0)

        overview = AnalyticsService(repository).overview(config.run_id)
        failure = "Expected no failed processing attempts; observed 1."
        assert overview["processed"] == 1
        assert overview["rendered"] == 1
        assert overview["processing_attempts"] == 2
        assert overview["processing_attempt_successes"] == 1
        assert overview["processing_attempt_failures"] == 1
        assert overview["processing_attempt_success_percent"] == 50.0
        assert overview["verdict"] == "fail"
        assert failure in overview["verdict_failures"]
    finally:
        repository.close()


def test_overview_requires_exact_configured_delivery_order(tmp_path) -> None:
    repository = Repository(tmp_path / "wrong-delivery-order.duckdb")
    try:
        config = build_scenario(
            ScenarioName.HAPPY_PATH,
            seed=682,
            event_count=4,
            event_rate=100,
            run_id=uuid4(),
        )
        repository.create_run(config)
        service = IngestService(repository)
        repository.open_overlay_session("order-overlay", config.run_id)

        for index in (1, 0, 2, 3):
            _process_and_render(
                repository,
                service,
                config.manifest[index],
                connection_id="order-source",
                session_id="order-overlay",
            )
        repository.complete_run(config.run_id, 4, 4, 0)

        overview = AnalyticsService(repository).overview(config.run_id)
        check = overview["scenario_checks"]["out_of_order_delivery"]
        assert check["observed_out_of_order_events"] > 0
        assert check["mismatch_count"] == 2
        assert check["first_mismatch"] == {
            "arrival_index": 1,
            "expected_sequence": 1,
            "observed_sequence": 2,
        }
        assert check["passed"] is False
        assert overview["verdict"] == "fail"
    finally:
        repository.close()


def test_overview_and_performance_percentiles_use_stored_latency_samples(
    tmp_path,
) -> None:
    repository = Repository(tmp_path / "known-latencies.duckdb")
    try:
        config = build_scenario(
            ScenarioName.HAPPY_PATH,
            seed=683,
            event_count=4,
            event_rate=100,
            run_id=uuid4(),
        )
        repository.create_run(config)
        service = IngestService(repository)
        repository.open_overlay_session("latency-overlay", config.run_id)
        for event in config.manifest:
            _process_and_render(
                repository,
                service,
                event,
                connection_id="latency-source",
                session_id="latency-overlay",
            )

        baseline = datetime(2025, 1, 1, tzinfo=UTC)
        for event, latency_ms in zip(
            config.manifest,
            (0, 10, 20, 30),
            strict=True,
        ):
            rendered_at = baseline + timedelta(milliseconds=latency_ms)
            repository._connection.execute(
                """
                UPDATE events SET persisted_at = ?, rendered_at = ?,
                                  render_acknowledged_at = ?
                WHERE event_id = ?
                """,
                [baseline, rendered_at, rendered_at, str(event.event_id)],
            )
        repository.complete_run(config.run_id, 4, 4, 0)

        analytics = AnalyticsService(repository)
        overview = analytics.overview(config.run_id)
        performance = analytics.performance(config.run_id)
        assert overview["p50_latency_ms"] == 15.0
        assert overview["p95_latency_ms"] == 28.5
        assert overview["p99_latency_ms"] == 29.7
        assert performance["percentiles_ms"] == {
            "p50": 15.0,
            "p95": 28.5,
            "p99": 29.7,
        }
    finally:
        repository.close()


def test_reconnect_verdict_rejects_attempts_on_unrelated_transport(tmp_path) -> None:
    repository = Repository(tmp_path / "unrelated-reconnect-transport.duckdb")
    try:
        config = build_scenario(
            ScenarioName.FORCED_RECONNECT,
            seed=69,
            event_count=12,
            event_rate=100,
            run_id=uuid4(),
        )
        repository.create_run(config)
        service = IngestService(repository)
        session_id = "unrelated-transport-overlay"
        repository.open_overlay_session(session_id, config.run_id)
        assert config.disconnect_sequence is not None
        target = config.manifest[config.disconnect_sequence - 1]

        for event in config.manifest:
            if event.event_id == target.event_id:
                repository.record_connection_event(
                    config.run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.CONNECTED,
                        connection_id="forced-old-transport",
                        detail={"observer": "server"},
                    ),
                )
            _process_and_render(
                repository,
                service,
                event,
                connection_id="unrelated-delivery-socket",
                session_id=session_id,
            )
            if event.event_id == target.event_id:
                repository.record_connection_event(
                    config.run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.FORCED_DISCONNECT,
                        connection_id="forced-old-transport",
                        detail={"unacknowledged_event_id": str(target.event_id)},
                    ),
                )
                repository.record_connection_event(
                    config.run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.DISCONNECTED,
                        connection_id="forced-old-transport",
                        detail={"observer": "server"},
                    ),
                )
                repository.record_connection_event(
                    config.run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.CONNECTED,
                        connection_id="reconnected-new-transport",
                        detail={"observer": "server"},
                    ),
                )
                repository.record_connection_event(
                    config.run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.RECONNECTED,
                        connection_id="reconnected-new-transport",
                    ),
                )
                duplicate = service.ingest_text(
                    target.canonical_json(),
                    "unrelated-delivery-socket",
                )
                assert service.record_reply_and_process(duplicate) is None
                repository.record_connection_event(
                    config.run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.RECOVERY_COMPLETE,
                        connection_id="reconnected-new-transport",
                        detail={"retried_event_id": str(target.event_id)},
                    ),
                )

        repository.complete_run(
            config.run_id,
            config.event_count,
            config.event_count,
            1,
        )
        overview = AnalyticsService(repository).overview(config.run_id)
        check = overview["scenario_checks"]["forced_reconnect"]

        assert check["target_accepted_attempts"] == 1
        assert check["target_duplicate_attempts"] == 1
        assert check["target_correlation"] is True
        assert check["transport_correlated"] is True
        assert check["attempt_path_correlated"] is False
        assert check["passed"] is False
        assert overview["verdict"] == "fail"
    finally:
        repository.close()


def test_reconnect_verdict_rejects_server_disconnect_after_recovery(tmp_path) -> None:
    repository = Repository(tmp_path / "late-server-disconnect.duckdb")
    try:
        config = build_scenario(
            ScenarioName.FORCED_RECONNECT,
            seed=691,
            event_count=12,
            event_rate=100,
            run_id=uuid4(),
        )
        assert config.disconnect_sequence is not None
        target = config.manifest[config.disconnect_sequence - 1]
        repository.create_run(config)
        repository.open_overlay_session("late-disconnect-overlay", config.run_id)
        service = IngestService(repository)
        old_connection = "late-disconnect-old"
        new_connection = "late-disconnect-new"
        repository.record_connection_event(
            config.run_id,
            ConnectionEventRequest(
                side=ConnectionSide.SIMULATOR,
                kind=ConnectionEventKind.CONNECTED,
                connection_id=old_connection,
                detail={"observer": "server"},
            ),
        )

        for event in config.manifest:
            connection_id = (
                old_connection
                if event.source_sequence <= config.disconnect_sequence
                else new_connection
            )
            _process_and_render(
                repository,
                service,
                event,
                connection_id=connection_id,
                session_id="late-disconnect-overlay",
            )
            if event.event_id != target.event_id:
                continue

            repository.record_submitted_connection_event(
                config.run_id,
                ConnectionEventRequest(
                    side=ConnectionSide.SIMULATOR,
                    kind=ConnectionEventKind.FORCED_DISCONNECT,
                    connection_id=old_connection,
                    detail={"unacknowledged_event_id": str(target.event_id)},
                ),
            )
            repository.record_connection_event(
                config.run_id,
                ConnectionEventRequest(
                    side=ConnectionSide.SIMULATOR,
                    kind=ConnectionEventKind.CONNECTED,
                    connection_id=new_connection,
                    detail={"observer": "server"},
                ),
            )
            repository.record_submitted_connection_event(
                config.run_id,
                ConnectionEventRequest(
                    side=ConnectionSide.SIMULATOR,
                    kind=ConnectionEventKind.RECONNECTED,
                    connection_id=new_connection,
                ),
            )
            duplicate = service.ingest_text(
                target.canonical_json(),
                new_connection,
            )
            assert service.record_reply_and_process(duplicate) is None
            repository.record_submitted_connection_event(
                config.run_id,
                ConnectionEventRequest(
                    side=ConnectionSide.SIMULATOR,
                    kind=ConnectionEventKind.RECOVERY_COMPLETE,
                    connection_id=new_connection,
                    detail={"retried_event_id": str(target.event_id)},
                ),
            )
            # This server-observed disconnect is real provenance but too late to
            # prove the old transport closed before the reconnect and retry.
            repository.record_connection_event(
                config.run_id,
                ConnectionEventRequest(
                    side=ConnectionSide.SIMULATOR,
                    kind=ConnectionEventKind.DISCONNECTED,
                    connection_id=old_connection,
                    detail={"observer": "server"},
                ),
            )

        repository.complete_run(
            config.run_id,
            config.event_count,
            config.event_count,
            1,
        )
        overview = AnalyticsService(repository).overview(config.run_id)
        check = overview["scenario_checks"]["forced_reconnect"]

        assert check["target_accepted_attempts"] == 1
        assert check["target_duplicate_attempts"] == 1
        assert check["target_correlation"] is True
        assert check["transport_correlated"] is False
        assert check["attempt_path_correlated"] is False
        assert check["passed"] is False
        assert overview["verdict"] == "fail"
    finally:
        repository.close()


def test_delay_verdict_rejects_marker_recorded_after_delivery(tmp_path) -> None:
    repository = Repository(tmp_path / "late-delay-marker.duckdb")
    try:
        config = build_scenario(
            ScenarioName.DELAYED_OUT_OF_ORDER,
            seed=70,
            event_count=12,
            event_rate=100,
            run_id=uuid4(),
        )
        repository.create_run(config)
        service = IngestService(repository)
        session_id = "zero-delay-overlay"
        repository.open_overlay_session(session_id, config.run_id)
        by_sequence = {event.source_sequence: event for event in config.manifest}

        for sequence in config.delivery_order:
            event = by_sequence[sequence]
            _process_and_render(
                repository,
                service,
                event,
                connection_id="zero-delay-source",
                session_id=session_id,
            )
            if sequence in config.delayed_sequences:
                repository.record_submitted_connection_event(
                    config.run_id,
                    ConnectionEventRequest(
                        side=ConnectionSide.SIMULATOR,
                        kind=ConnectionEventKind.DELAY_INJECTED,
                        connection_id="zero-delay-source",
                        occurred_at=datetime(2000, 1, 1, tzinfo=UTC),
                        detail={
                            "source_sequence": sequence,
                            "configured_delay_ms": config.delay_ms,
                        },
                    ),
                )

        repository.complete_run(
            config.run_id,
            config.event_count,
            config.event_count,
            0,
        )
        overview = AnalyticsService(repository).overview(config.run_id)
        check = overview["scenario_checks"]["delayed_delivery"]

        assert overview["scenario_checks"]["out_of_order_delivery"]["passed"] is True
        assert check["configured_delay_ms"] == 25
        assert check["observed"] == 0
        assert check["missing_sequences"] == sorted(config.delayed_sequences)
        assert check["measured_delay_ms"] == {}
        assert check["passed"] is False
        assert overview["verdict"] == "fail"
    finally:
        repository.close()
