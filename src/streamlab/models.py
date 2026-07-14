"""Versioned event, scenario, acknowledgment, and overlay contracts."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    TypeAdapter,
    field_validator,
    model_validator,
)

SCHEMA_VERSION: Literal["1.0"] = "1.0"


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""
    return datetime.now(UTC)


class StrictModel(BaseModel):
    """Base model for external contracts that reject unknown fields."""

    model_config = ConfigDict(extra="forbid")


class EventType(StrEnum):
    """Synthetic creator-platform event types supported by v0.1."""

    COMMENT = "comment"
    FOLLOW = "follow"
    GIFT = "gift"
    LIKE = "like"
    SUBSCRIPTION = "subscription"
    COMMAND = "command"


class ScenarioName(StrEnum):
    """Configuration-driven scenario catalog."""

    HAPPY_PATH = "happy_path"
    DUPLICATE_DELIVERY = "duplicate_delivery"
    INVALID_PAYLOADS = "invalid_payloads"
    DELAYED_OUT_OF_ORDER = "delayed_out_of_order"
    FORCED_RECONNECT = "forced_reconnect"
    RECONNECT_BURST = "reconnect_burst"


class CommentPayload(StrictModel):
    text: Annotated[str, Field(min_length=1, max_length=180)]


class FollowPayload(StrictModel):
    followed: Literal[True] = True


class GiftPayload(StrictModel):
    gift_name: Annotated[str, Field(min_length=1, max_length=40)]
    quantity: Annotated[int, Field(strict=True, ge=1, le=100)]


class LikePayload(StrictModel):
    count: Annotated[int, Field(strict=True, ge=1, le=10_000)]


class SubscriptionPayload(StrictModel):
    tier: Literal["tier_1", "tier_2", "tier_3"]
    months: Annotated[int, Field(strict=True, ge=1, le=120)]


class CommandPayload(StrictModel):
    name: Literal["blur", "save", "clear", "shield"]
    arguments: list[Annotated[str, Field(max_length=40)]] = Field(
        default_factory=list,
        max_length=8,
    )


PayloadModel = (
    CommentPayload
    | FollowPayload
    | GiftPayload
    | LikePayload
    | SubscriptionPayload
    | CommandPayload
)

PAYLOAD_ADAPTERS: dict[EventType, TypeAdapter[PayloadModel]] = {
    EventType.COMMENT: TypeAdapter(CommentPayload),
    EventType.FOLLOW: TypeAdapter(FollowPayload),
    EventType.GIFT: TypeAdapter(GiftPayload),
    EventType.LIKE: TypeAdapter(LikePayload),
    EventType.SUBSCRIPTION: TypeAdapter(SubscriptionPayload),
    EventType.COMMAND: TypeAdapter(CommandPayload),
}


class NormalizedEvent(StrictModel):
    """Canonical versioned event envelope accepted by the service."""

    schema_version: Literal["1.0"]
    event_id: UUID
    run_id: UUID
    source: Literal["simulator"]
    event_type: EventType
    source_sequence: Annotated[int, Field(strict=True, ge=1)]
    occurred_at: datetime
    actor_id: Annotated[str, Field(pattern=r"^synthetic-actor-[0-9]{3}$")]
    payload: dict[str, JsonValue]

    @field_validator("occurred_at")
    @classmethod
    def require_utc_timestamp(cls, value: datetime) -> datetime:
        """Reject naive timestamps and normalize aware values to UTC."""
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a UTC offset")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def validate_typed_payload(self) -> Self:
        """Validate payload fields against the selected event type."""
        validated = PAYLOAD_ADAPTERS[self.event_type].validate_python(self.payload)
        self.payload = validated.model_dump(mode="json")
        return self

    def canonical_json(self) -> str:
        """Serialize semantic content consistently for conflict detection."""
        return json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        )

    def canonical_hash(self) -> str:
        """Return a stable digest of the entire normalized envelope."""
        return hashlib.sha256(self.canonical_json().encode("utf-8")).hexdigest()


class ScenarioConfig(StrictModel):
    """Configuration persisted with a simulation run."""

    run_id: UUID
    scenario: ScenarioName
    seed: int
    event_count: Annotated[int, Field(ge=1, le=10_000)]
    event_rate: Annotated[float, Field(gt=0, le=5_000)] = 50.0
    burst_event_rate: Annotated[float, Field(gt=0, le=5_000)] | None = None
    burst_start_sequence: int | None = None
    disconnect_sequence: int | None = None
    duplicate_sequences: list[int] = Field(default_factory=list)
    invalid_sequences: list[int] = Field(default_factory=list)
    delayed_sequences: list[int] = Field(default_factory=list)
    delay_ms: Annotated[int, Field(strict=True, ge=0, le=5_000)] = 0
    delivery_order: list[int] = Field(default_factory=list)
    manifest: list[NormalizedEvent]

    @model_validator(mode="after")
    def validate_manifest(self) -> Self:
        """Ensure the generated manifest matches the declared run."""
        if len(self.manifest) != self.event_count:
            raise ValueError("manifest length must equal event_count")
        if any(event.run_id != self.run_id for event in self.manifest):
            raise ValueError("every manifest event must use run_id")
        sequences = [event.source_sequence for event in self.manifest]
        if sorted(sequences) != list(range(1, self.event_count + 1)):
            raise ValueError("manifest sequences must contain 1..event_count")
        if len({event.event_id for event in self.manifest}) != self.event_count:
            raise ValueError("manifest event_id values must be unique")
        expected = set(sequences)
        for name, configured in (
            ("duplicate_sequences", self.duplicate_sequences),
            ("invalid_sequences", self.invalid_sequences),
            ("delayed_sequences", self.delayed_sequences),
        ):
            if (
                len(configured) != len(set(configured))
                or not set(configured) <= expected
            ):
                raise ValueError(f"{name} must contain unique manifest sequences")
        if bool(self.delayed_sequences) != (self.delay_ms > 0):
            raise ValueError(
                "delayed_sequences and a positive delay_ms are required together"
            )
        if self.delivery_order:
            if len(self.delivery_order) != len(set(self.delivery_order)):
                raise ValueError("delivery_order cannot repeat sequences")
            if set(self.delivery_order) != expected:
                raise ValueError("delivery_order must contain every manifest sequence")
        else:
            self.delivery_order = sequences
        for name, sequence in (
            ("burst_start_sequence", self.burst_start_sequence),
            ("disconnect_sequence", self.disconnect_sequence),
        ):
            if sequence is not None and sequence not in expected:
                raise ValueError(f"{name} must reference a manifest sequence")
        return self


class RunCompleteRequest(StrictModel):
    """Simulator-reported completion evidence."""

    generated_count: Annotated[int, Field(ge=0)]
    client_acked_count: Annotated[int, Field(ge=0)]
    retry_count: Annotated[int, Field(ge=0)]


class ConnectionEventKind(StrEnum):
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    FORCED_DISCONNECT = "forced_disconnect"
    RECONNECTED = "reconnected"
    RECOVERY_COMPLETE = "recovery_complete"
    DELAY_INJECTED = "delay_injected"


class ConnectionSide(StrEnum):
    SIMULATOR = "simulator"
    OVERLAY = "overlay"


class ConnectionEventRequest(StrictModel):
    """Explicit connection/recovery evidence submitted through the API."""

    side: ConnectionSide
    kind: ConnectionEventKind
    connection_id: Annotated[str, Field(min_length=1, max_length=80)]
    occurred_at: datetime = Field(default_factory=utc_now)
    detail: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator("occurred_at")
    @classmethod
    def normalize_occurred_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a UTC offset")
        return value.astimezone(UTC)


class AttemptOutcome(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    REJECTED = "rejected"
    CONFLICT = "conflict"


class ErrorCategory(StrEnum):
    MALFORMED_JSON = "MALFORMED_JSON"
    MESSAGE_TOO_LARGE = "MESSAGE_TOO_LARGE"
    MISSING_FIELD = "MISSING_FIELD"
    UNSUPPORTED_SCHEMA = "UNSUPPORTED_SCHEMA"
    UNSUPPORTED_EVENT_TYPE = "UNSUPPORTED_EVENT_TYPE"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    RUN_ID_MISMATCH = "RUN_ID_MISMATCH"
    UNKNOWN_RUN = "UNKNOWN_RUN"
    UNREGISTERED_EVENT = "UNREGISTERED_EVENT"
    EVENT_ID_CONFLICT = "EVENT_ID_CONFLICT"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class ErrorDetail(StrictModel):
    category: ErrorCategory
    message: str


class DeliveryReply(StrictModel):
    """Structured WebSocket response correlated to one delivery attempt."""

    kind: Literal["ack", "nack"]
    attempt_id: UUID
    status: AttemptOutcome
    event_id: UUID | None = None
    run_id: UUID | None = None
    persisted: bool = False
    duplicate: bool = False
    error: ErrorDetail | None = None


class OverlayEffect(StrictModel):
    """Persisted canonical effect delivered to the browser overlay."""

    effect_id: UUID
    event_id: UUID
    run_id: UUID
    event_type: EventType
    actor_id: str
    payload: dict[str, JsonValue]
    occurred_at: datetime


class RenderAckMessage(StrictModel):
    """Browser-originated notice sent only after DOM insertion."""

    kind: Literal["render_ack"]
    event_id: UUID
    rendered_at: datetime

    @field_validator("rendered_at")
    @classmethod
    def normalize_rendered_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("rendered_at must include a UTC offset")
        return value.astimezone(UTC)
