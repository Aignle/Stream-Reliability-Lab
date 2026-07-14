"""Contract and protocol model tests."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from streamlab.models import NormalizedEvent, RenderAckMessage, ScenarioName
from streamlab.simulator import build_scenario

RUN_ID = UUID("11111111-1111-4111-8111-111111111111")


def _event_value() -> dict[str, object]:
    return {
        "schema_version": "1.0",
        "event_id": "22222222-2222-4222-8222-222222222222",
        "run_id": str(RUN_ID),
        "source": "simulator",
        "event_type": "comment",
        "source_sequence": 1,
        "occurred_at": "2025-01-01T00:00:00Z",
        "actor_id": "synthetic-actor-001",
        "payload": {"text": "Synthetic test"},
    }


@pytest.mark.parametrize("missing", ["schema_version", "source"])
def test_event_contract_requires_protocol_fields(missing: str) -> None:
    value = _event_value()
    value.pop(missing)

    with pytest.raises(ValidationError) as captured:
        NormalizedEvent.model_validate(value)

    assert captured.value.errors()[0]["type"] == "missing"


def test_render_ack_requires_discriminator() -> None:
    with pytest.raises(ValidationError) as captured:
        RenderAckMessage.model_validate(
            {
                "event_id": "22222222-2222-4222-8222-222222222222",
                "rendered_at": "2025-01-01T00:00:00Z",
            }
        )

    assert captured.value.errors()[0]["type"] == "missing"


def test_event_normalizes_aware_timestamp_and_rejects_naive() -> None:
    value = _event_value()
    event = NormalizedEvent.model_validate(value)
    assert event.occurred_at == datetime(2025, 1, 1, tzinfo=UTC)

    value["occurred_at"] = "2025-01-01T00:00:00"
    with pytest.raises(ValidationError):
        NormalizedEvent.model_validate(value)


def test_canonical_hash_is_order_independent_and_semantic() -> None:
    value = _event_value()
    first = NormalizedEvent.model_validate(value)
    reordered = dict(reversed(list(value.items())))
    second = NormalizedEvent.model_validate(reordered)
    changed = NormalizedEvent.model_validate(
        {**value, "payload": {"text": "Different synthetic text"}}
    )

    assert first.canonical_hash() == second.canonical_hash()
    assert first.canonical_hash() != changed.canonical_hash()


def test_scenario_manifest_invariants_are_explicit() -> None:
    config = build_scenario(
        ScenarioName.RECONNECT_BURST,
        seed=99,
        event_count=50,
        event_rate=200,
        run_id=RUN_ID,
    )

    assert len(config.manifest) == 50
    assert len({item.event_id for item in config.manifest}) == 50
    assert sorted(config.delivery_order) == list(range(1, 51))
    assert config.disconnect_sequence in range(1, 51)
    assert config.burst_start_sequence in range(1, 51)
    assert config.delayed_sequences
    assert config.delay_ms == 25
    assert not set(config.invalid_sequences) & {config.disconnect_sequence}
