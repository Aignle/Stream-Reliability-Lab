"""Application use cases for audited ingestion and idempotent processing."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from streamlab.models import (
    AttemptOutcome,
    DeliveryReply,
    ErrorCategory,
    ErrorDetail,
    NormalizedEvent,
    OverlayEffect,
)
from streamlab.repository import PersistedDelivery, RejectedDelivery, Repository

MAX_MESSAGE_BYTES = 64 * 1024
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestResult:
    """Transport-independent result returned before a network ACK is sent."""

    reply: DeliveryReply
    persisted_delivery: PersistedDelivery | None = None

    @property
    def may_process(self) -> bool:
        return (
            self.persisted_delivery is not None
            and self.persisted_delivery.outcome
            in {AttemptOutcome.ACCEPTED, AttemptOutcome.DUPLICATE}
        )


class IngestService:
    """Validate raw messages, persist evidence, and process accepted events."""

    def __init__(self, repository: Repository) -> None:
        self.repository = repository

    def ingest_text(
        self,
        raw_payload: str,
        connection_id: str,
        run_id_hint: UUID | None = None,
    ) -> IngestResult:
        """Audit one raw WebSocket message and prepare a structured reply."""
        encoded_size = len(raw_payload.encode("utf-8", errors="replace"))
        if encoded_size > MAX_MESSAGE_BYTES:
            retained = (
                raw_payload.encode("utf-8", errors="replace")[:MAX_MESSAGE_BYTES]
                .decode("utf-8", errors="replace")
                .rstrip("\ufffd")
                + "...[truncated]"
            )
            rejected = self.repository.record_rejected_delivery(
                raw_payload=retained,
                connection_id=connection_id,
                error_category=ErrorCategory.MESSAGE_TOO_LARGE,
                error_message=f"message exceeds {MAX_MESSAGE_BYTES} bytes",
                run_id=run_id_hint,
            )
            return self._rejected_result(rejected)

        try:
            decoded: Any = json.loads(raw_payload)
        except json.JSONDecodeError as error:
            rejected = self.repository.record_rejected_delivery(
                raw_payload=raw_payload,
                connection_id=connection_id,
                error_category=ErrorCategory.MALFORMED_JSON,
                error_message=f"invalid JSON at character {error.pos}",
                run_id=run_id_hint,
            )
            return self._rejected_result(rejected)

        event_id, run_id = self._extract_identifiers(decoded)
        if run_id_hint is not None and run_id is not None and run_id != run_id_hint:
            rejected = self.repository.record_rejected_delivery(
                raw_payload=raw_payload,
                connection_id=connection_id,
                error_category=ErrorCategory.RUN_ID_MISMATCH,
                error_message="event run_id does not match the WebSocket run_id",
                event_id=event_id,
                run_id=run_id_hint,
            )
            return self._rejected_result(rejected)
        try:
            event = NormalizedEvent.model_validate(decoded)
        except ValidationError as error:
            category = self._validation_category(error)
            rejected = self.repository.record_rejected_delivery(
                raw_payload=raw_payload,
                connection_id=connection_id,
                error_category=category,
                error_message=self._validation_message(error),
                event_id=event_id,
                run_id=run_id_hint or run_id,
            )
            return self._rejected_result(rejected)

        if run_id_hint is not None and event.run_id != run_id_hint:
            rejected = self.repository.record_rejected_delivery(
                raw_payload=raw_payload,
                connection_id=connection_id,
                error_category=ErrorCategory.RUN_ID_MISMATCH,
                error_message="event run_id does not match the WebSocket run_id",
                event_id=event.event_id,
                run_id=run_id_hint,
            )
            return self._rejected_result(rejected)

        if not self.repository.run_exists(event.run_id):
            rejected = self.repository.record_rejected_delivery(
                raw_payload=raw_payload,
                connection_id=connection_id,
                error_category=ErrorCategory.UNKNOWN_RUN,
                error_message="run_id is not registered",
                event_id=event.event_id,
                run_id=event.run_id,
            )
            return self._rejected_result(rejected)

        manifest_hash = self.repository.generated_event_hash(
            event.event_id,
            event.run_id,
        )
        if manifest_hash is None:
            rejected = self.repository.record_rejected_delivery(
                raw_payload=raw_payload,
                connection_id=connection_id,
                error_category=ErrorCategory.UNREGISTERED_EVENT,
                error_message="event_id is not present in the generated manifest",
                event_id=event.event_id,
                run_id=event.run_id,
            )
            return self._rejected_result(rejected)
        if manifest_hash != event.canonical_hash():
            conflicted = self.repository.record_conflicted_delivery(
                raw_payload=raw_payload,
                connection_id=connection_id,
                error_message="delivered event differs from its generated manifest",
                event_id=event.event_id,
                run_id=event.run_id,
            )
            return IngestResult(
                reply=DeliveryReply(
                    kind="nack",
                    attempt_id=conflicted.attempt_id,
                    status=AttemptOutcome.CONFLICT,
                    event_id=conflicted.event_id,
                    run_id=conflicted.run_id,
                    persisted=False,
                    error=ErrorDetail(
                        category=ErrorCategory.EVENT_ID_CONFLICT,
                        message=conflicted.error_message or "event_id conflict",
                    ),
                ),
                persisted_delivery=conflicted,
            )

        persisted = self.repository.persist_valid_delivery(
            raw_payload=raw_payload,
            connection_id=connection_id,
            event=event,
        )
        if persisted.outcome is AttemptOutcome.CONFLICT:
            return IngestResult(
                reply=DeliveryReply(
                    kind="nack",
                    attempt_id=persisted.attempt_id,
                    status=persisted.outcome,
                    event_id=persisted.event_id,
                    run_id=persisted.run_id,
                    persisted=False,
                    error=ErrorDetail(
                        category=ErrorCategory.EVENT_ID_CONFLICT,
                        message=persisted.error_message or "event_id conflict",
                    ),
                ),
                persisted_delivery=persisted,
            )
        return IngestResult(
            reply=DeliveryReply(
                kind="ack",
                attempt_id=persisted.attempt_id,
                status=persisted.outcome,
                event_id=persisted.event_id,
                run_id=persisted.run_id,
                persisted=True,
                duplicate=persisted.duplicate,
            ),
            persisted_delivery=persisted,
        )

    def record_reply_and_process(self, result: IngestResult) -> OverlayEffect | None:
        """Record a sent reply and process only accepted or duplicate events."""
        self.repository.mark_delivery_reply_sent(
            result.reply.attempt_id,
            result.reply.event_id,
            accepted=result.may_process,
        )
        if not result.may_process or result.persisted_delivery is None:
            return None
        delivery = result.persisted_delivery
        try:
            return self.repository.process_event(delivery.event_id)
        except Exception as error:
            try:
                self.repository.record_processing_failure(delivery.event_id, error)
            except Exception:
                logger.exception(
                    "Failed to store processing failure evidence for event %s",
                    delivery.event_id,
                )
            raise

    def recover_pending(self) -> list[OverlayEffect]:
        """Recover persisted events interrupted before processing."""
        return self.repository.recover_unprocessed()

    @staticmethod
    def _rejected_result(rejected: RejectedDelivery) -> IngestResult:
        return IngestResult(
            reply=DeliveryReply(
                kind="nack",
                attempt_id=rejected.attempt_id,
                status=AttemptOutcome.REJECTED,
                event_id=rejected.event_id,
                run_id=rejected.run_id,
                persisted=False,
                error=ErrorDetail(
                    category=rejected.error_category,
                    message=rejected.error_message,
                ),
            )
        )

    @staticmethod
    def _extract_identifiers(value: Any) -> tuple[UUID | None, UUID | None]:
        if not isinstance(value, dict):
            return None, None

        def parse(name: str) -> UUID | None:
            candidate = value.get(name)
            if not isinstance(candidate, str):
                return None
            try:
                return UUID(candidate)
            except ValueError:
                return None

        return parse("event_id"), parse("run_id")

    @staticmethod
    def _validation_category(error: ValidationError) -> ErrorCategory:
        details = error.errors()
        locations = {str(item["loc"][0]) for item in details if item["loc"]}
        if any(
            str(item["type"]).endswith("missing")
            and bool(item["loc"])
            and str(item["loc"][0]) in NormalizedEvent.model_fields
            for item in details
        ):
            return ErrorCategory.MISSING_FIELD
        if "payload" in locations or any(
            str(item["loc"][0]) not in NormalizedEvent.model_fields
            for item in details
            if item["loc"]
        ):
            return ErrorCategory.INVALID_PAYLOAD
        if "schema_version" in locations:
            return ErrorCategory.UNSUPPORTED_SCHEMA
        if "event_type" in locations:
            return ErrorCategory.UNSUPPORTED_EVENT_TYPE
        if "occurred_at" in locations:
            return ErrorCategory.INVALID_TIMESTAMP
        return ErrorCategory.INVALID_PAYLOAD

    @staticmethod
    def _validation_message(error: ValidationError) -> str:
        first = error.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "event"
        return f"{location}: {first['msg']}"
