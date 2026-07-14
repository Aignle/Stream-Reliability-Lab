"""Deterministic synthetic event generator and at-least-once WebSocket client."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
from contextlib import suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlencode, urlparse
from uuid import UUID, uuid4, uuid5

import httpx
from pydantic import JsonValue
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from streamlab.models import (
    SCHEMA_VERSION,
    AttemptOutcome,
    ConnectionEventKind,
    ConnectionSide,
    DeliveryReply,
    EventType,
    NormalizedEvent,
    ScenarioConfig,
    ScenarioName,
)

DEMO_SEED = 20250314
INVALID_MUTATIONS = (
    "missing_field",
    "unsupported_event_type",
    "invalid_timestamp",
    "unsupported_schema",
    "invalid_payload",
    "malformed_json",
)


@dataclass(frozen=True, slots=True)
class SimulationResult:
    """Client-observed outcome printed after a scenario finishes."""

    run_id: str
    scenario: str
    seed: int
    generated: int
    client_acked_unique: int
    accepted_replies: int
    duplicate_replies: int
    rejected_replies: int
    conflict_replies: int
    retries: int
    forced_reconnects: int
    completed: bool
    overlay_url: str
    analytics_url: str

    def as_json(self) -> str:
        return json.dumps(asdict(self), indent=2, sort_keys=True)


def _payload_for(
    event_type: EventType,
    rng: random.Random,
    sequence: int,
) -> dict[str, JsonValue]:
    if event_type is EventType.COMMENT:
        return {"text": f"Synthetic comment {sequence:04d}"}
    if event_type is EventType.FOLLOW:
        return {"followed": True}
    if event_type is EventType.GIFT:
        return {
            "gift_name": rng.choice(("spark", "signal", "pulse")),
            "quantity": rng.randint(1, 5),
        }
    if event_type is EventType.LIKE:
        return {"count": rng.randint(1, 250)}
    if event_type is EventType.SUBSCRIPTION:
        return {
            "tier": rng.choice(("tier_1", "tier_2", "tier_3")),
            "months": rng.randint(1, 24),
        }
    return {
        "name": rng.choice(("blur", "save", "clear", "shield")),
        "arguments": [f"synthetic-{sequence % 7}"],
    }


def _sample_sequences(
    event_count: int,
    amount: int,
    seed: int,
    *,
    excluded: set[int] | None = None,
) -> list[int]:
    candidates = [
        sequence
        for sequence in range(1, event_count + 1)
        if sequence not in (excluded or set())
    ]
    rng = random.Random(seed)
    return sorted(rng.sample(candidates, min(amount, len(candidates))))


def _reordered_sequences(event_count: int, *, primary: bool) -> list[int]:
    order = list(range(1, event_count + 1))
    if event_count < 3:
        return list(reversed(order))
    width = min(8 if primary else 5, event_count)
    start = max(0, min(event_count - width, event_count // 4))
    order[start : start + width] = reversed(order[start : start + width])
    if primary and event_count >= 20:
        second_width = min(6, event_count // 5)
        second_start = max(start + width, event_count // 2)
        second_end = min(event_count, second_start + second_width)
        order[second_start:second_end] = reversed(order[second_start:second_end])
    return order


def build_scenario(
    scenario: ScenarioName,
    *,
    seed: int,
    event_count: int,
    event_rate: float,
    burst_event_rate: float | None = None,
    run_id: UUID | None = None,
) -> ScenarioConfig:
    """Build a complete deterministic manifest and persisted injection plan."""
    active_run_id = run_id or uuid4()
    rng = random.Random(seed)
    base_time = datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=seed % 365)
    event_types = tuple(EventType)
    manifest: list[NormalizedEvent] = []
    for sequence in range(1, event_count + 1):
        event_type = rng.choice(event_types)
        manifest.append(
            NormalizedEvent(
                schema_version=SCHEMA_VERSION,
                event_id=uuid5(active_run_id, f"{seed}:{sequence}"),
                run_id=active_run_id,
                source="simulator",
                event_type=event_type,
                source_sequence=sequence,
                occurred_at=base_time + timedelta(milliseconds=sequence * 10),
                actor_id=f"synthetic-actor-{rng.randint(1, 80):03d}",
                payload=_payload_for(event_type, rng, sequence),
            )
        )

    duplicate_sequences: list[int] = []
    invalid_sequences: list[int] = []
    delayed_sequences: list[int] = []
    delay_ms = 0
    delivery_order = list(range(1, event_count + 1))
    disconnect_sequence: int | None = None
    burst_start_sequence: int | None = None
    configured_burst_rate: float | None = None

    if scenario is ScenarioName.DUPLICATE_DELIVERY:
        duplicate_sequences = _sample_sequences(
            event_count,
            max(1, event_count // 5),
            seed + 101,
        )
    elif scenario is ScenarioName.INVALID_PAYLOADS:
        invalid_sequences = _sample_sequences(
            event_count,
            min(event_count, max(4, event_count // 3)),
            seed + 202,
        )
    elif scenario is ScenarioName.DELAYED_OUT_OF_ORDER:
        delivery_order = _reordered_sequences(event_count, primary=False)
        delayed_sequences = _sample_sequences(
            event_count,
            max(1, round(event_count * 0.1)),
            seed + 505,
        )
        delay_ms = 25
    elif scenario is ScenarioName.FORCED_RECONNECT:
        disconnect_sequence = max(1, event_count // 2)
    elif scenario is ScenarioName.RECONNECT_BURST:
        disconnect_sequence = max(1, round(event_count * 0.4))
        burst_start_sequence = max(1, round(event_count * 0.7))
        invalid_sequences = _sample_sequences(
            event_count,
            max(5, round(event_count * 0.02)),
            seed + 303,
            excluded={disconnect_sequence},
        )
        duplicate_sequences = _sample_sequences(
            event_count,
            max(1, round(event_count * 0.05)),
            seed + 404,
            excluded=set(invalid_sequences) | {disconnect_sequence},
        )
        delayed_sequences = _sample_sequences(
            event_count,
            max(1, round(event_count * 0.01)),
            seed + 505,
            excluded=set(invalid_sequences) | {disconnect_sequence},
        )
        delay_ms = 25
        delivery_order = _reordered_sequences(event_count, primary=True)
        configured_burst_rate = burst_event_rate or min(5_000, event_rate * 8)

    return ScenarioConfig(
        run_id=active_run_id,
        scenario=scenario,
        seed=seed,
        event_count=event_count,
        event_rate=event_rate,
        burst_event_rate=configured_burst_rate,
        burst_start_sequence=burst_start_sequence,
        disconnect_sequence=disconnect_sequence,
        duplicate_sequences=duplicate_sequences,
        invalid_sequences=invalid_sequences,
        delayed_sequences=delayed_sequences,
        delay_ms=delay_ms,
        delivery_order=delivery_order,
        manifest=manifest,
    )


def invalid_delivery(event: NormalizedEvent, ordinal: int) -> str:
    """Create one deterministic malformed variant without changing its identity."""
    mutation = INVALID_MUTATIONS[ordinal % len(INVALID_MUTATIONS)]
    value = event.model_dump(mode="json")
    if mutation == "missing_field":
        value.pop("actor_id")
    elif mutation == "unsupported_event_type":
        value["event_type"] = "unsupported_synthetic_type"
    elif mutation == "invalid_timestamp":
        value["occurred_at"] = "2025-01-01T00:00:00"
    elif mutation == "unsupported_schema":
        value["schema_version"] = "9.9"
    elif mutation == "invalid_payload":
        value["payload"] = {"unexpected": True}
    else:
        return event.canonical_json()[:-1]
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def scenario_client_reconciles(
    config: ScenarioConfig,
    *,
    acked_event_ids: set[UUID],
    delivered_sequences: list[int],
    confirmed_duplicate_sequences: set[int],
    rejected_replies: int,
    conflict_replies: int,
    retry_count: int,
    forced_reconnects: int,
) -> bool:
    """Reconcile client observations with every configured scenario injection."""
    invalid_sequences = set(config.invalid_sequences)
    expected_acked_event_ids = {
        event.event_id
        for event in config.manifest
        if event.source_sequence not in invalid_sequences
    }
    duplicate_plan_completed = set(config.duplicate_sequences).issubset(
        confirmed_duplicate_sequences
    )
    reconnect_plan_completed = config.disconnect_sequence is None or (
        retry_count >= 1 and forced_reconnects >= 1
    )
    return (
        acked_event_ids == expected_acked_event_ids
        and rejected_replies == len(config.invalid_sequences)
        and conflict_replies == 0
        and delivered_sequences == config.delivery_order
        and duplicate_plan_completed
        and reconnect_plan_completed
    )


def _require_local_url(value: str, *, protocols: set[str]) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in protocols or parsed.hostname not in {
        "127.0.0.1",
        "localhost",
    }:
        raise ValueError(f"only local {sorted(protocols)} URLs are supported: {value}")
    return value.rstrip("/")


class ScenarioRunner:
    """Reliable WebSocket sender with explicit connection evidence and retries."""

    def __init__(
        self,
        config: ScenarioConfig,
        *,
        api_url: str,
        ws_url: str,
        overlay_wait: float = 0.0,
    ) -> None:
        self.config = config
        self.api_url = _require_local_url(api_url, protocols={"http", "https"})
        self.ws_url = _require_local_url(ws_url, protocols={"ws", "wss"})
        self.overlay_wait = overlay_wait
        self.accepted_replies = 0
        self.duplicate_replies = 0
        self.rejected_replies = 0
        self.conflict_replies = 0
        self.retry_count = 0
        self.forced_reconnects = 0
        self.acked_event_ids: set[UUID] = set()
        self.delivered_sequences: list[int] = []
        self.confirmed_duplicate_sequences: set[int] = set()

    async def _record_connection(
        self,
        client: httpx.AsyncClient,
        kind: ConnectionEventKind,
        connection_id: str,
        detail: dict[str, JsonValue] | None = None,
    ) -> None:
        response = await client.post(
            f"/api/runs/{self.config.run_id}/connection-events",
            json={
                "side": ConnectionSide.SIMULATOR.value,
                "kind": kind.value,
                "connection_id": connection_id,
                "detail": detail or {"observer": "simulator"},
            },
        )
        response.raise_for_status()

    async def _connect(
        self,
        client: httpx.AsyncClient,
        kind: ConnectionEventKind,
    ) -> tuple[ClientConnection, str]:
        connection_id = str(uuid4())
        query = urlencode(
            {
                "connection_id": connection_id,
                "run_id": str(self.config.run_id),
            }
        )
        socket_url = f"{self.ws_url}/ws/ingest?{query}"
        socket = await connect(socket_url, max_size=128 * 1024, open_timeout=10)
        if kind is ConnectionEventKind.RECONNECTED:
            await self._record_connection(client, kind, connection_id)
        return socket, connection_id

    @staticmethod
    async def _exchange(socket: ClientConnection, raw_payload: str) -> DeliveryReply:
        await socket.send(raw_payload)
        message = await asyncio.wait_for(socket.recv(), timeout=10)
        if isinstance(message, bytes):
            message = message.decode("utf-8")
        return DeliveryReply.model_validate_json(message)

    def _observe_reply(self, reply: DeliveryReply) -> None:
        if reply.status is AttemptOutcome.ACCEPTED:
            self.accepted_replies += 1
        elif reply.status is AttemptOutcome.DUPLICATE:
            self.duplicate_replies += 1
        elif reply.status is AttemptOutcome.CONFLICT:
            self.conflict_replies += 1
        else:
            self.rejected_replies += 1
        if reply.kind == "ack" and reply.event_id is not None:
            self.acked_event_ids.add(reply.event_id)

    async def _deliver_with_retry(
        self,
        client: httpx.AsyncClient,
        socket: ClientConnection,
        connection_id: str,
        raw_payload: str,
    ) -> tuple[ClientConnection, str, DeliveryReply]:
        for attempt in range(5):
            try:
                reply = await self._exchange(socket, raw_payload)
                self._observe_reply(reply)
                return socket, connection_id, reply
            except (ConnectionClosed, TimeoutError):
                self.retry_count += 1
                with suppress(Exception):
                    await socket.close()
                if attempt == 4:
                    raise
                socket, connection_id = await self._connect(
                    client,
                    ConnectionEventKind.RECONNECTED,
                )
        raise RuntimeError("delivery retry loop exhausted")

    async def _wait_for_overlay(self, client: httpx.AsyncClient) -> None:
        if self.overlay_wait <= 0:
            return
        deadline = asyncio.get_running_loop().time() + self.overlay_wait
        while asyncio.get_running_loop().time() < deadline:
            response = await client.get(f"/api/runs/{self.config.run_id}/overview")
            if response.is_success and response.json().get("overlay_sessions", 0) > 0:
                return
            await asyncio.sleep(0.1)
        raise TimeoutError(
            f"no overlay connected within {self.overlay_wait:.1f}s; "
            f"open {self.api_url}/overlay?run_id={self.config.run_id}"
        )

    async def _wait_for_sent_acceptance(
        self,
        client: httpx.AsyncClient,
        event_id: UUID,
        connection_id: str,
    ) -> None:
        """Wait until the old transport's accepted reply is recorded as sent."""
        deadline = asyncio.get_running_loop().time() + 10
        while asyncio.get_running_loop().time() < deadline:
            response = await client.get(
                f"/api/runs/{self.config.run_id}/events/{event_id}"
            )
            if response.is_success:
                attempts = response.json().get("delivery_attempts", [])
                if any(
                    attempt.get("outcome") == AttemptOutcome.ACCEPTED.value
                    and attempt.get("connection_id") == connection_id
                    and attempt.get("response_sent_at") is not None
                    for attempt in attempts
                ):
                    return
            await asyncio.sleep(0.01)
        raise TimeoutError(
            f"accepted reply for {event_id} was not sent on {connection_id}"
        )

    async def run(self) -> SimulationResult:
        """Create, deliver, reconcile, and complete one configured run."""
        manifest_by_sequence = {
            event.source_sequence: event for event in self.config.manifest
        }
        invalid_ordinals = {
            sequence: index
            for index, sequence in enumerate(self.config.invalid_sequences)
        }
        async with httpx.AsyncClient(base_url=self.api_url, timeout=30) as client:
            response = await client.post(
                "/api/runs",
                json=self.config.model_dump(mode="json"),
            )
            response.raise_for_status()
            if self.overlay_wait > 0:
                print(
                    "Run created. Open the overlay before delivery begins: "
                    f"{self.api_url}/overlay?run_id={self.config.run_id}"
                )
            await self._wait_for_overlay(client)
            socket, connection_id = await self._connect(
                client,
                ConnectionEventKind.CONNECTED,
            )
            forced_reconnect_done = False
            try:
                for sequence in self.config.delivery_order:
                    self.delivered_sequences.append(sequence)
                    event = manifest_by_sequence[sequence]
                    if sequence in self.config.delayed_sequences:
                        await self._record_connection(
                            client,
                            ConnectionEventKind.DELAY_INJECTED,
                            connection_id,
                            {
                                "observer": "simulator",
                                "source_sequence": sequence,
                                "configured_delay_ms": self.config.delay_ms,
                            },
                        )
                        await asyncio.sleep(self.config.delay_ms / 1_000)
                    raw_payload = (
                        invalid_delivery(event, invalid_ordinals[sequence])
                        if sequence in invalid_ordinals
                        else event.canonical_json()
                    )

                    if (
                        sequence == self.config.disconnect_sequence
                        and not forced_reconnect_done
                    ):
                        await socket.send(raw_payload)
                        await self._wait_for_sent_acceptance(
                            client,
                            event.event_id,
                            connection_id,
                        )
                        await self._record_connection(
                            client,
                            ConnectionEventKind.FORCED_DISCONNECT,
                            connection_id,
                            {
                                "observer": "simulator",
                                "unacknowledged_event_id": str(event.event_id),
                            },
                        )
                        await socket.close()
                        self.retry_count += 1
                        self.forced_reconnects += 1
                        socket, connection_id = await self._connect(
                            client,
                            ConnectionEventKind.RECONNECTED,
                        )
                        socket, connection_id, _ = await self._deliver_with_retry(
                            client,
                            socket,
                            connection_id,
                            raw_payload,
                        )
                        await self._record_connection(
                            client,
                            ConnectionEventKind.RECOVERY_COMPLETE,
                            connection_id,
                            {
                                "observer": "simulator",
                                "retried_event_id": str(event.event_id),
                            },
                        )
                        forced_reconnect_done = True
                    else:
                        socket, connection_id, _ = await self._deliver_with_retry(
                            client,
                            socket,
                            connection_id,
                            raw_payload,
                        )

                    if sequence in self.config.duplicate_sequences:
                        (
                            socket,
                            connection_id,
                            duplicate_reply,
                        ) = await self._deliver_with_retry(
                            client,
                            socket,
                            connection_id,
                            event.canonical_json(),
                        )
                        if duplicate_reply.status is AttemptOutcome.DUPLICATE:
                            self.confirmed_duplicate_sequences.add(sequence)

                    active_rate = self.config.event_rate
                    if (
                        self.config.burst_start_sequence is not None
                        and sequence >= self.config.burst_start_sequence
                        and self.config.burst_event_rate is not None
                    ):
                        active_rate = self.config.burst_event_rate
                    await asyncio.sleep(1 / active_rate)
            finally:
                await socket.close()

            completion = await client.post(
                f"/api/runs/{self.config.run_id}/complete",
                json={
                    "generated_count": self.config.event_count,
                    "client_acked_count": len(self.acked_event_ids),
                    "retry_count": self.retry_count,
                },
            )
            completion.raise_for_status()

        completed = scenario_client_reconciles(
            self.config,
            acked_event_ids=self.acked_event_ids,
            delivered_sequences=self.delivered_sequences,
            confirmed_duplicate_sequences=self.confirmed_duplicate_sequences,
            rejected_replies=self.rejected_replies,
            conflict_replies=self.conflict_replies,
            retry_count=self.retry_count,
            forced_reconnects=self.forced_reconnects,
        )
        return SimulationResult(
            run_id=str(self.config.run_id),
            scenario=self.config.scenario.value,
            seed=self.config.seed,
            generated=self.config.event_count,
            client_acked_unique=len(self.acked_event_ids),
            accepted_replies=self.accepted_replies,
            duplicate_replies=self.duplicate_replies,
            rejected_replies=self.rejected_replies,
            conflict_replies=self.conflict_replies,
            retries=self.retry_count,
            forced_reconnects=self.forced_reconnects,
            completed=completed,
            overlay_url=f"{self.api_url}/overlay?run_id={self.config.run_id}",
            analytics_url=f"{self.api_url}/api/runs/{self.config.run_id}/overview",
        )


async def run_scenario(
    config: ScenarioConfig,
    *,
    api_url: str = "http://127.0.0.1:8000",
    ws_url: str = "ws://127.0.0.1:8000",
    overlay_wait: float = 0.0,
) -> SimulationResult:
    """Programmatic entry point used by tests and the command-line interface."""
    return await ScenarioRunner(
        config,
        api_url=api_url,
        ws_url=ws_url,
        overlay_wait=overlay_wait,
    ).run()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a deterministic Stream Reliability Lab scenario.",
    )
    parser.add_argument(
        "--scenario",
        choices=[item.value for item in ScenarioName],
        default=ScenarioName.HAPPY_PATH.value,
    )
    parser.add_argument("--seed", type=int, default=DEMO_SEED)
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--burst-rate", type=float)
    parser.add_argument("--run-id", type=UUID)
    parser.add_argument(
        "--api-url",
        default=os.environ.get("STREAMLAB_API_URL", "http://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--ws-url",
        default=os.environ.get("STREAMLAB_WS_URL", "ws://127.0.0.1:8000"),
    )
    parser.add_argument(
        "--overlay-wait",
        type=float,
        default=0.0,
        help="Wait up to this many seconds for a browser overlay before sending.",
    )
    parser.add_argument("--result-file", type=Path)
    return parser


def main(argv: list[str] | None = None) -> None:
    """Run the CLI and return a nonzero status if client reconciliation fails."""
    args = _parser().parse_args(argv)
    config = build_scenario(
        ScenarioName(args.scenario),
        seed=args.seed,
        event_count=args.count,
        event_rate=args.rate,
        burst_event_rate=args.burst_rate,
        run_id=args.run_id,
    )
    result = asyncio.run(
        run_scenario(
            config,
            api_url=args.api_url,
            ws_url=args.ws_url,
            overlay_wait=args.overlay_wait,
        )
    )
    rendered = result.as_json()
    print(rendered)
    if args.result_file is not None:
        args.result_file.parent.mkdir(parents=True, exist_ok=True)
        args.result_file.write_text(rendered + "\n", encoding="utf-8")
    if not result.completed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
