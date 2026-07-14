"""Deterministic scenario generation and retry tests."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest

from streamlab.models import (
    AttemptOutcome,
    DeliveryReply,
    ErrorCategory,
    EventType,
    ScenarioConfig,
    ScenarioName,
)
from streamlab.repository import Repository
from streamlab.service import IngestService
from streamlab.simulator import (
    INVALID_MUTATIONS,
    ScenarioRunner,
    build_scenario,
    invalid_delivery,
    scenario_client_reconciles,
)

RUN_ID = UUID("33333333-3333-4333-8333-333333333333")


def test_same_seed_and_run_produce_identical_semantic_manifest() -> None:
    first = build_scenario(
        ScenarioName.RECONNECT_BURST,
        seed=1234,
        event_count=40,
        event_rate=100,
        run_id=RUN_ID,
    )
    second = build_scenario(
        ScenarioName.RECONNECT_BURST,
        seed=1234,
        event_count=40,
        event_rate=100,
        run_id=RUN_ID,
    )
    changed = build_scenario(
        ScenarioName.RECONNECT_BURST,
        seed=1235,
        event_count=40,
        event_rate=100,
        run_id=RUN_ID,
    )

    assert [item.canonical_json() for item in first.manifest] == [
        item.canonical_json() for item in second.manifest
    ]
    assert [item.canonical_json() for item in first.manifest] != [
        item.canonical_json() for item in changed.manifest
    ]


@pytest.mark.parametrize(
    (
        "scenario",
        "has_duplicates",
        "has_invalid",
        "has_delays",
        "reordered",
        "reconnect",
    ),
    [
        (ScenarioName.HAPPY_PATH, False, False, False, False, False),
        (ScenarioName.DUPLICATE_DELIVERY, True, False, False, False, False),
        (ScenarioName.INVALID_PAYLOADS, False, True, False, False, False),
        (ScenarioName.DELAYED_OUT_OF_ORDER, False, False, True, True, False),
        (ScenarioName.FORCED_RECONNECT, False, False, False, False, True),
        (ScenarioName.RECONNECT_BURST, True, True, True, True, True),
    ],
)
def test_all_scenario_controls_are_configuration_driven(
    scenario: ScenarioName,
    has_duplicates: bool,
    has_invalid: bool,
    has_delays: bool,
    reordered: bool,
    reconnect: bool,
) -> None:
    config = build_scenario(
        scenario,
        seed=7,
        event_count=30,
        event_rate=100,
        run_id=RUN_ID,
    )

    assert bool(config.duplicate_sequences) is has_duplicates
    assert bool(config.invalid_sequences) is has_invalid
    assert bool(config.delayed_sequences) is has_delays
    assert (config.delay_ms > 0) is has_delays
    assert (config.delivery_order != list(range(1, 31))) is reordered
    assert (config.disconnect_sequence is not None) is reconnect
    assert (config.burst_event_rate is not None) is (
        scenario is ScenarioName.RECONNECT_BURST
    )


def test_invalid_mutations_have_structured_categories(tmp_path) -> None:
    config = build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=17,
        event_count=len(INVALID_MUTATIONS),
        event_rate=100,
        run_id=RUN_ID,
    )
    repository = Repository(tmp_path / "invalids.duckdb")
    try:
        repository.create_run(config)
        service = IngestService(repository)
        categories = []
        for ordinal, event in enumerate(config.manifest):
            result = service.ingest_text(
                invalid_delivery(event, ordinal),
                "invalid-test",
                RUN_ID,
            )
            assert result.reply.kind == "nack"
            assert result.reply.error is not None
            categories.append(result.reply.error.category)
            service.record_reply_and_process(result)

        assert set(categories) == {
            ErrorCategory.MISSING_FIELD,
            ErrorCategory.UNSUPPORTED_EVENT_TYPE,
            ErrorCategory.INVALID_TIMESTAMP,
            ErrorCategory.UNSUPPORTED_SCHEMA,
            ErrorCategory.INVALID_PAYLOAD,
            ErrorCategory.MALFORMED_JSON,
        }
        assert repository.query("SELECT COUNT(*) AS count FROM events")[0]["count"] == 0
    finally:
        repository.close()


def test_ingestion_rejects_numeric_string_coercion(tmp_path) -> None:
    config = build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=171,
        event_count=30,
        event_rate=100,
        run_id=RUN_ID,
    )
    repository = Repository(tmp_path / "strict-numbers.duckdb")
    try:
        repository.create_run(config)
        service = IngestService(repository)

        sequence_value = config.manifest[0].model_dump(mode="json")
        sequence_value["source_sequence"] = str(sequence_value["source_sequence"])
        sequence_result = service.ingest_text(
            json.dumps(sequence_value),
            "strict-number-test",
            RUN_ID,
        )

        numeric_event = next(
            event
            for event in config.manifest
            if event.event_type
            in {EventType.GIFT, EventType.LIKE, EventType.SUBSCRIPTION}
        )
        payload_value = numeric_event.model_dump(mode="json")
        numeric_key = {
            EventType.GIFT: "quantity",
            EventType.LIKE: "count",
            EventType.SUBSCRIPTION: "months",
        }[numeric_event.event_type]
        payload = payload_value["payload"]
        assert isinstance(payload, dict)
        payload[numeric_key] = str(payload[numeric_key])
        payload_result = service.ingest_text(
            json.dumps(payload_value),
            "strict-number-test",
            RUN_ID,
        )

        for result in (sequence_result, payload_result):
            assert result.reply.kind == "nack"
            assert result.reply.error is not None
            assert result.reply.error.category is ErrorCategory.INVALID_PAYLOAD
        assert repository.query("SELECT COUNT(*) AS count FROM events")[0]["count"] == 0
        assert (
            repository.query("SELECT COUNT(*) AS count FROM delivery_attempts")[0][
                "count"
            ]
            == 2
        )
    finally:
        repository.close()


class _FakeSocket:
    def __init__(self) -> None:
        self.close_count = 0

    async def close(self) -> None:
        self.close_count += 1


def test_retry_closes_timed_out_socket_before_reconnecting(monkeypatch) -> None:
    config = build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=18,
        event_count=1,
        event_rate=100,
        run_id=RUN_ID,
    )
    runner = ScenarioRunner(
        config,
        api_url="http://127.0.0.1:8000",
        ws_url="ws://127.0.0.1:8000",
    )
    original = _FakeSocket()
    replacement = _FakeSocket()
    calls = 0

    async def exchange(_socket, _raw_payload):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise TimeoutError
        return DeliveryReply(
            kind="ack",
            attempt_id=uuid4(),
            status=AttemptOutcome.ACCEPTED,
            event_id=config.manifest[0].event_id,
            run_id=RUN_ID,
            persisted=True,
        )

    async def reconnect(_client, _kind):
        return replacement, "source-b"

    async def record_connection(_client, _kind, _connection_id, _detail=None):
        return None

    monkeypatch.setattr(runner, "_exchange", exchange)
    monkeypatch.setattr(runner, "_connect", reconnect)
    monkeypatch.setattr(runner, "_record_connection", record_connection)

    socket, connection_id, reply = asyncio.run(
        runner._deliver_with_retry(object(), original, "source-a", "{}")
    )
    assert original.close_count == 1
    assert socket is replacement
    assert connection_id == "source-b"
    assert reply.status is AttemptOutcome.ACCEPTED
    assert runner.retry_count == 1


def test_retry_exhaustion_closes_every_socket_without_extra_reconnect(
    monkeypatch,
) -> None:
    config = build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=19,
        event_count=1,
        event_rate=100,
        run_id=RUN_ID,
    )
    runner = ScenarioRunner(
        config,
        api_url="http://127.0.0.1:8000",
        ws_url="ws://127.0.0.1:8000",
    )
    sockets = [_FakeSocket()]

    async def timeout(_socket, _raw_payload):
        raise TimeoutError

    async def reconnect(_client, _kind):
        socket = _FakeSocket()
        sockets.append(socket)
        return socket, f"source-{len(sockets)}"

    async def record_connection(_client, _kind, _connection_id, _detail=None):
        return None

    monkeypatch.setattr(runner, "_exchange", timeout)
    monkeypatch.setattr(runner, "_connect", reconnect)
    monkeypatch.setattr(runner, "_record_connection", record_connection)

    with pytest.raises(TimeoutError):
        asyncio.run(runner._deliver_with_retry(object(), sockets[0], "source-1", "{}"))
    assert len(sockets) == 5
    assert all(socket.close_count == 1 for socket in sockets)
    assert runner.retry_count == 5


def _successful_client_observations(config: ScenarioConfig) -> dict[str, object]:
    invalid_sequences = set(config.invalid_sequences)
    return {
        "acked_event_ids": {
            event.event_id
            for event in config.manifest
            if event.source_sequence not in invalid_sequences
        },
        "delivered_sequences": list(config.delivery_order),
        "confirmed_duplicate_sequences": set(config.duplicate_sequences),
        "rejected_replies": len(config.invalid_sequences),
        "conflict_replies": 0,
        "retry_count": 1 if config.disconnect_sequence is not None else 0,
        "forced_reconnects": 1 if config.disconnect_sequence is not None else 0,
    }


@pytest.mark.parametrize(
    ("scenario", "missing_evidence"),
    [
        (ScenarioName.DUPLICATE_DELIVERY, "duplicates"),
        (ScenarioName.FORCED_RECONNECT, "reconnect"),
        (ScenarioName.DELAYED_OUT_OF_ORDER, "delivery_order"),
    ],
)
def test_client_reconciliation_requires_planned_scenario_injections(
    scenario: ScenarioName,
    missing_evidence: str,
) -> None:
    config = build_scenario(
        scenario,
        seed=71,
        event_count=20,
        event_rate=100,
        run_id=RUN_ID,
    )
    observations = _successful_client_observations(config)
    assert scenario_client_reconciles(config, **observations)

    if missing_evidence == "duplicates":
        observations["confirmed_duplicate_sequences"] = set()
    elif missing_evidence == "reconnect":
        observations["retry_count"] = 0
        observations["forced_reconnects"] = 0
    else:
        observations["delivered_sequences"] = list(range(1, config.event_count + 1))

    assert not scenario_client_reconciles(config, **observations)
