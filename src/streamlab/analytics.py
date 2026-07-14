"""Evidence-derived run analytics exposed through the FastAPI read boundary."""

from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime
from itertools import zip_longest
from typing import cast
from uuid import UUID

from streamlab.repository import Repository


def percentile(values: list[float], quantile: float) -> float | None:
    """Return a linearly interpolated percentile without inventing samples."""
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * quantile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 3)


def _milliseconds(start: object, end: object) -> float | None:
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    return round((end - start).total_seconds() * 1_000, 3)


def _ratio(numerator: int, denominator: int) -> float | None:
    if denominator == 0:
        return None
    return round(numerator / denominator * 100, 2)


def _as_int(value: object) -> int:
    return int(cast(int | str, value))


def _as_float(value: object) -> float:
    return float(cast(float | int | str, value))


def _json_object(value: object) -> dict[str, object]:
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return cast(dict[str, object], decoded) if isinstance(decoded, dict) else {}


class AnalyticsService:
    """Calculate dashboard views strictly from persisted lifecycle evidence."""

    def __init__(self, repository: Repository) -> None:
        self.repository = repository

    def _run(self, run_id: UUID) -> dict[str, object]:
        rows = self.repository.query(
            "SELECT * FROM runs WHERE run_id = ?", [str(run_id)]
        )
        if not rows:
            raise KeyError(f"run {run_id} does not exist")
        return rows[0]

    @staticmethod
    def _config(run: dict[str, object]) -> dict[str, object]:
        value = json.loads(str(run["config_json"]))
        return cast(dict[str, object], value)

    def overview(self, run_id: UUID) -> dict[str, object]:
        run = self._run(run_id)
        config = self._config(run)
        outcome_rows = self.repository.query(
            """
            SELECT outcome, COUNT(*) AS count
            FROM delivery_attempts WHERE run_id = ? GROUP BY outcome
            """,
            [str(run_id)],
        )
        outcomes = {str(row["outcome"]): _as_int(row["count"]) for row in outcome_rows}
        counts = self.repository.query(
            """
            SELECT
              (SELECT COUNT(*) FROM generated_events WHERE run_id = ?) AS generated,
              (SELECT COUNT(*) FROM delivery_attempts WHERE run_id = ?) AS delivered,
              (SELECT COUNT(*) FROM events WHERE run_id = ?) AS unique_events,
              (SELECT COUNT(*) FROM events WHERE run_id = ?
                 AND acknowledged_at IS NOT NULL) AS acknowledged,
              (SELECT COUNT(*) FROM events WHERE run_id = ?
                 AND processed_at IS NOT NULL) AS processed,
              (SELECT COUNT(DISTINCT event_id) FROM overlay_dispatches
                 WHERE outcome = 'sent' AND event_id IN
                   (SELECT event_id FROM events WHERE run_id = ?)) AS dispatched,
              (SELECT COUNT(DISTINCT event_id) FROM render_acknowledgments
                 WHERE event_id IN
                   (SELECT event_id FROM events WHERE run_id = ?)) AS rendered,
              (SELECT COUNT(*) FROM processing_attempts
                 WHERE event_id IN
                   (SELECT event_id FROM events WHERE run_id = ?))
                 AS processing_attempts,
               (SELECT COUNT(*) FROM processing_attempts
                  WHERE outcome = 'success' AND event_id IN
                    (SELECT event_id FROM events WHERE run_id = ?))
                  AS processing_successes,
               (SELECT COUNT(*) FROM processing_attempts
                  WHERE outcome != 'success' AND event_id IN
                    (SELECT event_id FROM events WHERE run_id = ?))
                  AS processing_failures,
               (SELECT COUNT(*) FROM overlay_sessions WHERE run_id = ?)
                  AS overlay_sessions,
              (SELECT COUNT(*) FROM events WHERE run_id = ?
                 AND out_of_order = TRUE) AS out_of_order_events
            """,
            [str(run_id)] * 12,
        )[0]
        generated = _as_int(counts["generated"])
        delivered = _as_int(counts["delivered"])
        unique_events = _as_int(counts["unique_events"])
        acknowledged = _as_int(counts["acknowledged"])
        processed = _as_int(counts["processed"])
        dispatched = _as_int(counts["dispatched"])
        rendered = _as_int(counts["rendered"])
        processing_attempts = _as_int(counts["processing_attempts"])
        processing_successes = _as_int(counts["processing_successes"])
        processing_failures = _as_int(counts["processing_failures"])
        duplicate_count = outcomes.get("duplicate", 0)
        rejected_count = outcomes.get("rejected", 0)
        conflict_count = outcomes.get("conflict", 0)
        operational_delivery_failures = _as_int(
            self.repository.query(
                """
                SELECT COUNT(*) AS count FROM delivery_attempts
                WHERE run_id = ? AND outcome = 'rejected'
                  AND error_category = 'INTERNAL_ERROR'
                """,
                [str(run_id)],
            )[0]["count"]
        )
        payload_rejection_count = rejected_count - operational_delivery_failures
        valid_deliveries = outcomes.get("accepted", 0) + duplicate_count

        latency_rows = self.repository.query(
            """
            SELECT persisted_at, render_acknowledged_at
            FROM events WHERE run_id = ? AND render_acknowledged_at IS NOT NULL
            """,
            [str(run_id)],
        )
        latencies = [
            latency
            for row in latency_rows
            if (
                latency := _milliseconds(
                    row["persisted_at"],
                    row["render_acknowledged_at"],
                )
            )
            is not None
        ]

        expected_invalid = len(cast(list[object], config.get("invalid_sequences", [])))
        expected_unique = generated - expected_invalid
        delayed_sequences = [
            _as_int(item)
            for item in cast(list[object], config.get("delayed_sequences", []))
        ]
        configured_delay_ms = _as_int(config.get("delay_ms", 0))
        delay_rows = self.repository.query(
            """
            SELECT connection_id, occurred_at, detail_json FROM connection_events
            WHERE run_id = ? AND side = 'simulator' AND kind = 'delay_injected'
            ORDER BY occurred_at
            """,
            [str(run_id)],
        )
        first_attempt_rows = self.repository.query(
            """
            SELECT source_sequence, connection_id, received_at FROM (
              SELECT g.source_sequence, d.connection_id, d.received_at,
                     ROW_NUMBER() OVER (
                       PARTITION BY g.source_sequence ORDER BY d.received_at
                     ) AS attempt_number
              FROM generated_events g
              JOIN delivery_attempts d ON d.event_id = g.event_id
              WHERE g.run_id = ? AND d.run_id = ?
            ) WHERE attempt_number = 1
            """,
            [str(run_id), str(run_id)],
        )
        first_attempts = {
            _as_int(row["source_sequence"]): row for row in first_attempt_rows
        }
        observed_delayed_sequences: set[int] = set()
        measured_delay_ms: dict[str, float] = {}
        for row in delay_rows:
            detail = _json_object(row["detail_json"])
            source_sequence = detail.get("source_sequence")
            evidence_delay_ms = detail.get("configured_delay_ms")
            marker_at = row["occurred_at"]
            first_attempt = (
                first_attempts.get(source_sequence)
                if isinstance(source_sequence, int)
                and not isinstance(source_sequence, bool)
                else None
            )
            observed_delay_ms = (
                _milliseconds(marker_at, first_attempt["received_at"])
                if first_attempt is not None
                else None
            )
            if (
                isinstance(source_sequence, int)
                and not isinstance(source_sequence, bool)
                and source_sequence in delayed_sequences
                and isinstance(evidence_delay_ms, int)
                and not isinstance(evidence_delay_ms, bool)
                and evidence_delay_ms == configured_delay_ms
                and isinstance(marker_at, datetime)
                and first_attempt is not None
                and str(first_attempt["connection_id"]) == str(row["connection_id"])
                and isinstance(observed_delay_ms, (int, float))
                and observed_delay_ms >= configured_delay_ms
            ):
                observed_delayed_sequences.add(source_sequence)
                measured_delay_ms[str(source_sequence)] = observed_delay_ms
        missing_delayed_sequences = sorted(
            set(delayed_sequences) - observed_delayed_sequences
        )
        duplicate_sequences = [
            _as_int(item)
            for item in cast(list[object], config.get("duplicate_sequences", []))
        ]
        expected_duplicates = len(duplicate_sequences)
        duplicate_placeholders = ", ".join("?" for _ in duplicate_sequences)
        duplicate_sequence_rows = (
            self.repository.query(
                f"""
                SELECT g.source_sequence, COUNT(*) AS count
                FROM delivery_attempts d
                JOIN generated_events g ON g.event_id = d.event_id
                WHERE d.run_id = ? AND d.outcome = 'duplicate'
                  AND g.source_sequence IN ({duplicate_placeholders})
                GROUP BY g.source_sequence
                """,
                [str(run_id), *duplicate_sequences],
            )
            if duplicate_sequences
            else []
        )
        observed_duplicate_sequences = {
            _as_int(row["source_sequence"]) for row in duplicate_sequence_rows
        }
        missing_duplicate_sequences = sorted(
            set(duplicate_sequences) - observed_duplicate_sequences
        )
        raw_configured_order = cast(list[object], config.get("delivery_order", []))
        configured_order = (
            [_as_int(item) for item in raw_configured_order]
            if raw_configured_order
            else list(range(1, generated + 1))
        )
        expects_out_of_order = configured_order != list(range(1, generated + 1))
        invalid_sequences = {
            _as_int(item)
            for item in cast(list[object], config.get("invalid_sequences", []))
        }
        expected_canonical_order = [
            sequence
            for sequence in configured_order
            if sequence not in invalid_sequences
        ]
        observed_canonical_order = [
            _as_int(row["source_sequence"])
            for row in self.repository.query(
                """
                SELECT source_sequence FROM events
                WHERE run_id = ? ORDER BY arrival_index
                """,
                [str(run_id)],
            )
        ]
        order_mismatches = [
            (index, expected, observed)
            for index, (expected, observed) in enumerate(
                zip_longest(expected_canonical_order, observed_canonical_order),
                start=1,
            )
            if expected != observed
        ]
        delivery_order_matches = not order_mismatches
        first_order_mismatch = (
            {
                "arrival_index": order_mismatches[0][0],
                "expected_sequence": order_mismatches[0][1],
                "observed_sequence": order_mismatches[0][2],
            }
            if order_mismatches
            else None
        )
        disconnect_sequence = config.get("disconnect_sequence")
        expects_reconnect = disconnect_sequence is not None
        reconnect_target_id: str | None = None
        reconnect_target_accepted = 0
        reconnect_target_duplicates = 0
        if disconnect_sequence is not None:
            target_rows = self.repository.query(
                """
                SELECT g.event_id,
                  COUNT(*) FILTER (WHERE d.outcome = 'accepted') AS accepted,
                  COUNT(*) FILTER (WHERE d.outcome = 'duplicate') AS duplicates
                FROM generated_events g
                LEFT JOIN delivery_attempts d ON d.event_id = g.event_id
                WHERE g.run_id = ? AND g.source_sequence = ?
                GROUP BY g.event_id
                """,
                [str(run_id), _as_int(disconnect_sequence)],
            )
            if target_rows:
                reconnect_target_id = str(target_rows[0]["event_id"])
                reconnect_target_accepted = _as_int(target_rows[0]["accepted"])
                reconnect_target_duplicates = _as_int(target_rows[0]["duplicates"])
        reconnect = self._reconnect_summary(run_id, reconnect_target_id)
        client_acked_count = _as_int(run["client_acked_count"])
        retry_count = _as_int(run["retry_count"])
        out_of_order_count = _as_int(counts["out_of_order_events"])
        reconnect_passed = not expects_reconnect or (
            _as_int(reconnect["forced_disconnects"]) >= 1
            and _as_int(reconnect["reconnections"]) >= 1
            and _as_int(reconnect["recovery_completions"]) >= 1
            and retry_count >= 1
            and reconnect_target_accepted >= 1
            and reconnect_target_duplicates >= 1
            and reconnect["target_event_id"] == reconnect_target_id
            and reconnect["target_correlation"] is True
            and reconnect["transport_correlated"] is True
            and reconnect["attempt_path_correlated"] is True
        )
        scenario_checks = {
            "planned_duplicates": {
                "required": expected_duplicates > 0,
                "passed": not missing_duplicate_sequences,
                "expected_minimum": expected_duplicates,
                "observed": len(observed_duplicate_sequences),
                "missing_sequences": missing_duplicate_sequences,
            },
            "forced_reconnect": {
                "required": expects_reconnect,
                "passed": reconnect_passed,
                "forced_disconnects": reconnect["forced_disconnects"],
                "reconnections": reconnect["reconnections"],
                "recovery_completions": reconnect["recovery_completions"],
                "retries": retry_count,
                "target_event_id": reconnect_target_id,
                "target_accepted_attempts": reconnect_target_accepted,
                "target_duplicate_attempts": reconnect_target_duplicates,
                "target_correlation": reconnect["target_correlation"],
                "transport_correlated": reconnect["transport_correlated"],
                "attempt_path_correlated": reconnect["attempt_path_correlated"],
                "accepted_reply_sent_on_forced_transport": reconnect[
                    "accepted_reply_sent_on_forced_transport"
                ],
                "duplicate_reply_sent_on_reconnected_transport": reconnect[
                    "duplicate_reply_sent_on_reconnected_transport"
                ],
            },
            "out_of_order_delivery": {
                "required": True,
                "planned_out_of_order": expects_out_of_order,
                "passed": delivery_order_matches,
                "observed_out_of_order_events": out_of_order_count,
                "expected_canonical_count": len(expected_canonical_order),
                "observed_canonical_count": len(observed_canonical_order),
                "mismatch_count": len(order_mismatches),
                "first_mismatch": first_order_mismatch,
            },
            "delayed_delivery": {
                "required": bool(delayed_sequences),
                "passed": not missing_delayed_sequences,
                "configured_delay_ms": configured_delay_ms,
                "expected_minimum": len(delayed_sequences),
                "observed": len(observed_delayed_sequences),
                "missing_sequences": missing_delayed_sequences,
                "measured_delay_ms": measured_delay_ms,
            },
        }
        verdict_failures: list[str] = []
        if unique_events != expected_unique:
            verdict_failures.append(
                f"Expected {expected_unique} canonical events; "
                f"observed {unique_events}."
            )
        if client_acked_count != expected_unique:
            verdict_failures.append(
                f"Expected the simulator client to acknowledge {expected_unique} "
                f"unique events; it reported {client_acked_count}."
            )
        if acknowledged != expected_unique:
            verdict_failures.append(
                f"Expected {expected_unique} unique events with stored ACK-send "
                f"evidence; observed {acknowledged}."
            )
        if processed != unique_events:
            verdict_failures.append(
                f"Expected {unique_events} processed events; observed {processed}."
            )
        if processing_failures != 0:
            verdict_failures.append(
                "Expected no failed processing attempts; observed "
                f"{processing_failures}."
            )
        if rendered != processed:
            verdict_failures.append(
                f"Expected {processed} rendered events; observed {rendered}."
            )
        if conflict_count != 0:
            verdict_failures.append(
                f"Expected no event-identity conflicts; observed {conflict_count}."
            )
        if payload_rejection_count != expected_invalid:
            verdict_failures.append(
                f"Expected {expected_invalid} payload-rejected delivery attempts; "
                f"observed {payload_rejection_count}."
            )
        if operational_delivery_failures != 0:
            verdict_failures.append(
                "Expected no operational ingestion failures; observed "
                f"{operational_delivery_failures}."
            )
        if missing_duplicate_sequences:
            verdict_failures.append(
                "Duplicate plan is missing stored duplicate attempts for source "
                f"sequences {missing_duplicate_sequences}."
            )
        if not reconnect_passed:
            verdict_failures.append(
                "Reconnect plan requires stored forced-disconnect, reconnect, "
                "recovery-complete, and transport-bound retry evidence."
            )
        if not delivery_order_matches:
            verdict_failures.append(
                "Stored canonical arrival order does not match the configured "
                f"delivery order; observed {len(order_mismatches)} mismatch(es)."
            )
        if missing_delayed_sequences:
            verdict_failures.append(
                "Delay plan is missing stored delay evidence for source sequences "
                f"{missing_delayed_sequences}."
            )
        status = str(run["status"])
        if status != "completed":
            verdict = "running"
            verdict_reason = "The simulator has not completed the run."
        elif _as_int(counts["overlay_sessions"]) == 0:
            verdict = "incomplete"
            verdict_reason = "No browser overlay session produced render evidence."
        elif not verdict_failures:
            verdict = "pass"
            verdict_reason = "Stored lifecycle counts reconcile with the scenario plan."
        else:
            verdict = "fail"
            verdict_reason = " ".join(verdict_failures)

        return {
            "run_id": str(run_id),
            "scenario": run["scenario"],
            "status": status,
            "seed": _as_int(run["seed"]),
            "configured_event_rate": _as_float(run["event_rate"]),
            "burst_event_rate": config.get("burst_event_rate"),
            "started_at": run["started_at"],
            "completed_at": run["completed_at"],
            "generated": generated,
            "delivered": delivered,
            "valid_deliveries": valid_deliveries,
            "unique_events": unique_events,
            "acknowledged": acknowledged,
            "processed": processed,
            "dispatched": dispatched,
            "rendered": rendered,
            "duplicates": duplicate_count,
            "payload_rejections": payload_rejection_count,
            "conflicts": conflict_count,
            "operational_delivery_failures": operational_delivery_failures,
            "payload_rejection_rate_percent": _ratio(
                payload_rejection_count,
                delivered,
            ),
            "payload_rejection_rate_definition": (
                "payload-rejected delivery attempts / all delivery attempts; "
                "identity conflicts and operational ingestion failures are separate"
            ),
            "processing_attempts": processing_attempts,
            "processing_attempt_successes": processing_successes,
            "processing_attempt_failures": processing_failures,
            "processing_attempt_success_percent": _ratio(
                processing_successes,
                processing_attempts,
            ),
            "processing_completion_percent": _ratio(processed, unique_events),
            "render_completion_percent": _ratio(rendered, processed),
            "overlay_sessions": _as_int(counts["overlay_sessions"]),
            "client_acked_unique": client_acked_count,
            "retries": retry_count,
            "latency_definition": "persisted to browser render acknowledgment",
            "latency_sample_count": len(latencies),
            "p50_latency_ms": percentile(latencies, 0.50),
            "p95_latency_ms": percentile(latencies, 0.95),
            "p99_latency_ms": percentile(latencies, 0.99),
            "reconnection": reconnect,
            "scenario_checks": scenario_checks,
            "verdict": verdict,
            "verdict_reason": verdict_reason,
            "verdict_failures": verdict_failures,
        }

    def _reconnect_summary(
        self,
        run_id: UUID,
        target_event_id: str | None,
    ) -> dict[str, object]:
        rows = self.repository.query(
            """
            SELECT kind, connection_id, occurred_at, detail_json
            FROM connection_events
            WHERE run_id = ? AND side = 'simulator'
            ORDER BY occurred_at
            """,
            [str(run_id)],
        )
        forced_records: list[tuple[str, datetime, str | None]] = []
        reconnected_records: list[tuple[str, datetime]] = []
        recovery_records: list[tuple[str, datetime, str | None]] = []
        server_connected_at: dict[str, list[datetime]] = {}
        server_disconnected_at: dict[str, list[datetime]] = {}
        forced_target_ids: set[str] = set()
        recovered_target_ids: set[str] = set()
        for row in rows:
            kind = str(row["kind"])
            connection_id = str(row["connection_id"])
            occurred_at = row["occurred_at"]
            detail = _json_object(row["detail_json"])
            observer = detail.get("observer")
            if kind == "forced_disconnect" and isinstance(occurred_at, datetime):
                target = detail.get("unacknowledged_event_id")
                if isinstance(target, str):
                    forced_target_ids.add(target)
                forced_records.append(
                    (
                        connection_id,
                        occurred_at,
                        target if isinstance(target, str) else None,
                    )
                )
            elif kind == "reconnected" and isinstance(occurred_at, datetime):
                reconnected_records.append((connection_id, occurred_at))
            elif kind == "recovery_complete" and isinstance(occurred_at, datetime):
                target = detail.get("retried_event_id")
                if isinstance(target, str):
                    recovered_target_ids.add(target)
                recovery_records.append(
                    (
                        connection_id,
                        occurred_at,
                        target if isinstance(target, str) else None,
                    )
                )
            if (
                kind == "connected"
                and observer == "server"
                and isinstance(occurred_at, datetime)
            ):
                server_connected_at.setdefault(connection_id, []).append(occurred_at)
            elif (
                kind == "disconnected"
                and observer == "server"
                and isinstance(occurred_at, datetime)
            ):
                server_disconnected_at.setdefault(connection_id, []).append(occurred_at)

        matching_forced = [
            record for record in forced_records if record[2] == target_event_id
        ]
        matching_recovery = [
            record for record in recovery_records if record[2] == target_event_id
        ]
        forced_connection_id: str | None = None
        reconnected_connection_id: str | None = None
        forced_at: datetime | None = None
        reconnect_at: datetime | None = None
        recovery_at: datetime | None = None
        if len(matching_forced) == 1:
            forced_connection_id, forced_at, _ = matching_forced[0]
            later_recoveries = [
                record for record in matching_recovery if record[1] > forced_at
            ]
            if len(later_recoveries) == 1:
                recovery_connection_id, recovery_at, _ = later_recoveries[0]
                matching_reconnects = [
                    record
                    for record in reconnected_records
                    if record[0] == recovery_connection_id
                    and forced_at < record[1] < recovery_at
                ]
                if len(matching_reconnects) == 1:
                    reconnected_connection_id, reconnect_at = matching_reconnects[0]

        transport_correlated = False
        if (
            forced_connection_id is not None
            and reconnected_connection_id is not None
            and forced_connection_id != reconnected_connection_id
            and forced_at is not None
            and reconnect_at is not None
            and recovery_at is not None
        ):
            transport_correlated = any(
                old_connected
                <= forced_at
                <= old_disconnected
                <= new_connected
                <= reconnect_at
                < recovery_at
                for old_connected in server_connected_at.get(
                    forced_connection_id,
                    [],
                )
                for old_disconnected in server_disconnected_at.get(
                    forced_connection_id,
                    [],
                )
                for new_connected in server_connected_at.get(
                    reconnected_connection_id,
                    [],
                )
            )
        target_correlation = (
            target_event_id is not None
            and len(forced_target_ids) == 1
            and forced_target_ids == {target_event_id}
            and recovered_target_ids == {target_event_id}
        )
        accepted_reply_sent_on_forced_transport = 0
        duplicate_reply_sent_on_reconnected_transport = 0
        if (
            target_event_id is not None
            and forced_connection_id is not None
            and reconnected_connection_id is not None
            and forced_at is not None
            and reconnect_at is not None
            and recovery_at is not None
        ):
            attempt_rows = self.repository.query(
                """
                SELECT connection_id, outcome, received_at, response_sent_at
                FROM delivery_attempts
                WHERE run_id = ? AND event_id = ?
                ORDER BY received_at
                """,
                [str(run_id), target_event_id],
            )
            for row in attempt_rows:
                received_at = row["received_at"]
                response_sent_at = row["response_sent_at"]
                if not isinstance(received_at, datetime) or not isinstance(
                    response_sent_at, datetime
                ):
                    continue
                if (
                    str(row["connection_id"]) == forced_connection_id
                    and str(row["outcome"]) == "accepted"
                    and received_at <= response_sent_at <= forced_at
                ):
                    accepted_reply_sent_on_forced_transport += 1
                if (
                    str(row["connection_id"]) == reconnected_connection_id
                    and str(row["outcome"]) == "duplicate"
                    and reconnect_at <= received_at <= response_sent_at <= recovery_at
                ):
                    duplicate_reply_sent_on_reconnected_transport += 1
        attempt_path_correlated = (
            transport_correlated
            and accepted_reply_sent_on_forced_transport == 1
            and duplicate_reply_sent_on_reconnected_transport == 1
        )
        return {
            "forced_disconnects": sum(
                1 for row in rows if str(row["kind"]) == "forced_disconnect"
            ),
            "reconnections": sum(
                1 for row in rows if str(row["kind"]) == "reconnected"
            ),
            "recovery_completions": sum(
                1 for row in rows if str(row["kind"]) == "recovery_complete"
            ),
            "duration_ms": _milliseconds(forced_at, reconnect_at),
            "evidence_events": len(rows),
            "target_event_id": next(iter(forced_target_ids), None),
            "target_correlation": target_correlation,
            "transport_correlated": transport_correlated,
            "attempt_path_correlated": attempt_path_correlated,
            "accepted_reply_sent_on_forced_transport": (
                accepted_reply_sent_on_forced_transport
            ),
            "duplicate_reply_sent_on_reconnected_transport": (
                duplicate_reply_sent_on_reconnected_transport
            ),
        }

    def event_table(
        self,
        run_id: UUID,
        *,
        search: str = "",
        limit: int = 200,
        offset: int = 0,
    ) -> dict[str, object]:
        self._run(run_id)
        rows = self.repository.query(
            """
            SELECT g.event_id, g.source_sequence, g.event_type, g.generated_at,
                   e.actor_id, e.persisted_at, e.acknowledged_at, e.processed_at,
                   e.first_dispatched_at, e.render_acknowledged_at,
                   e.out_of_order, e.sequence_gap,
                   (SELECT COUNT(*) FROM delivery_attempts d
                     WHERE d.event_id = g.event_id) AS delivery_count,
                   (SELECT COUNT(*) FROM processing_attempts p
                     WHERE p.event_id = g.event_id) AS processing_count,
                   (SELECT COUNT(*) FROM render_acknowledgments r
                     WHERE r.event_id = g.event_id) AS render_count
            FROM generated_events g
            LEFT JOIN events e ON e.event_id = g.event_id
            WHERE g.run_id = ? ORDER BY g.source_sequence
            """,
            [str(run_id)],
        )
        term = search.strip().lower()
        if term:
            rows = [
                row
                for row in rows
                if term
                in " ".join(
                    str(row.get(field, "")).lower()
                    for field in (
                        "event_id",
                        "event_type",
                        "actor_id",
                        "source_sequence",
                    )
                )
            ]
        total = len(rows)
        selected = rows[offset : offset + limit]
        for row in selected:
            if _as_int(row["render_count"]) > 0:
                lifecycle = "render acknowledged"
            elif row["first_dispatched_at"] is not None:
                lifecycle = "sent to overlay"
            elif row["processed_at"] is not None:
                lifecycle = "processed"
            elif row["acknowledged_at"] is not None:
                lifecycle = "acknowledged"
            elif row["persisted_at"] is not None:
                lifecycle = "persisted"
            elif _as_int(row["delivery_count"]) > 0:
                lifecycle = "rejected"
            else:
                lifecycle = "generated"
            row["lifecycle_status"] = lifecycle
        return {"run_id": str(run_id), "total": total, "events": selected}

    def event_evidence(self, run_id: UUID, event_id: UUID) -> dict[str, object]:
        generated = self.repository.query(
            """
            SELECT * FROM generated_events WHERE run_id = ? AND event_id = ?
            """,
            [str(run_id), str(event_id)],
        )
        if not generated:
            raise KeyError(f"event {event_id} does not exist in run {run_id}")
        canonical = self.repository.query(
            "SELECT * FROM events WHERE run_id = ? AND event_id = ?",
            [str(run_id), str(event_id)],
        )
        deliveries = self.repository.query(
            """
            SELECT attempt_id, connection_id, received_at, outcome, error_category,
                   error_message, duplicate, conflict, response_sent_at
            FROM delivery_attempts WHERE event_id = ? ORDER BY received_at
            """,
            [str(event_id)],
        )
        processing = self.repository.query(
            """
            SELECT processing_attempt_id, started_at, completed_at, outcome,
                   error_message FROM processing_attempts
            WHERE event_id = ? ORDER BY started_at
            """,
            [str(event_id)],
        )
        dispatches = self.repository.query(
            """
            SELECT dispatch_id, session_id, attempt_no, dispatched_at, outcome,
                   error_message FROM overlay_dispatches
            WHERE event_id = ? ORDER BY dispatched_at
            """,
            [str(event_id)],
        )
        renders = self.repository.query(
            """
            SELECT session_id, rendered_at, acknowledged_at
            FROM render_acknowledgments WHERE event_id = ? ORDER BY acknowledged_at
            """,
            [str(event_id)],
        )
        timeline: list[dict[str, object]] = [
            {"stage": "generated", "at": generated[0]["generated_at"]}
        ]
        timeline.extend(
            {
                "stage": f"delivery {row['outcome']}",
                "at": row["received_at"],
                "evidence_id": row["attempt_id"],
            }
            for row in deliveries
        )
        if canonical:
            event = canonical[0]
            for stage, field in (
                ("persisted", "persisted_at"),
                ("acknowledged", "acknowledged_at"),
                ("processed", "processed_at"),
                ("sent to overlay", "first_dispatched_at"),
                ("render acknowledged", "render_acknowledged_at"),
            ):
                if event[field] is not None:
                    timeline.append({"stage": stage, "at": event[field]})
        timeline.sort(key=lambda item: cast(datetime, item["at"]))
        return {
            "run_id": str(run_id),
            "event_id": str(event_id),
            "generated": generated[0],
            "canonical": canonical[0] if canonical else None,
            "delivery_attempts": deliveries,
            "processing_attempts": processing,
            "dispatches": dispatches,
            "render_acknowledgments": renders,
            "timeline": timeline,
        }

    def performance(self, run_id: UUID) -> dict[str, object]:
        run = self._run(run_id)
        config = self._config(run)
        rows = self.repository.query(
            """
            SELECT source_sequence, persisted_at, render_acknowledged_at
            FROM events WHERE run_id = ? ORDER BY persisted_at
            """,
            [str(run_id)],
        )
        latency_points: list[dict[str, object]] = []
        throughput = Counter[str]()
        latencies: list[float] = []
        burst_latencies: list[float] = []
        normal_latencies: list[float] = []
        burst_start = config.get("burst_start_sequence")
        for row in rows:
            persisted_at = cast(datetime, row["persisted_at"])
            throughput[persisted_at.replace(microsecond=0).isoformat()] += 1
            latency = _milliseconds(persisted_at, row["render_acknowledged_at"])
            if latency is None:
                continue
            sequence = _as_int(row["source_sequence"])
            latencies.append(latency)
            latency_points.append(
                {
                    "source_sequence": sequence,
                    "persisted_at": persisted_at,
                    "latency_ms": latency,
                }
            )
            if isinstance(burst_start, int) and sequence >= burst_start:
                burst_latencies.append(latency)
            else:
                normal_latencies.append(latency)

        bins = [0, 10, 25, 50, 100, 250, 500, 1_000, math.inf]
        histogram: list[dict[str, object]] = []
        for lower, upper in zip(bins, bins[1:]):
            histogram.append(
                {
                    "range": f"{lower:g}-{upper:g}"
                    if math.isfinite(upper)
                    else "1000+",
                    "count": sum(lower <= item < upper for item in latencies),
                }
            )

        reconnect_rows = self.repository.query(
            """
            SELECT occurred_at FROM connection_events
            WHERE run_id = ? AND side = 'simulator' AND kind = 'reconnected'
            ORDER BY occurred_at LIMIT 1
            """,
            [str(run_id)],
        )
        reconnect_at = reconnect_rows[0]["occurred_at"] if reconnect_rows else None
        before: list[float] = []
        after: list[float] = []
        if isinstance(reconnect_at, datetime):
            for point in latency_points:
                target = (
                    before
                    if cast(datetime, point["persisted_at"]) < reconnect_at
                    else after
                )
                target.append(cast(float, point["latency_ms"]))

        return {
            "run_id": str(run_id),
            "latency_definition": "persisted to browser render acknowledgment",
            "sample_count": len(latencies),
            "percentiles_ms": {
                "p50": percentile(latencies, 0.50),
                "p95": percentile(latencies, 0.95),
                "p99": percentile(latencies, 0.99),
            },
            "throughput_per_second": [
                {"second": second, "events": count}
                for second, count in sorted(throughput.items())
            ],
            "latency_over_time": latency_points,
            "latency_distribution": histogram,
            "burst_comparison": {
                "normal_samples": len(normal_latencies),
                "normal_p95_ms": percentile(normal_latencies, 0.95),
                "burst_samples": len(burst_latencies),
                "burst_p95_ms": percentile(burst_latencies, 0.95),
            },
            "reconnection_comparison": {
                "before_samples": len(before),
                "before_p95_ms": percentile(before, 0.95),
                "after_samples": len(after),
                "after_p95_ms": percentile(after, 0.95),
            },
        }

    def failures(self, run_id: UUID) -> dict[str, object]:
        self._run(run_id)
        payload_rejection_categories = self.repository.query(
            """
            SELECT error_category, COUNT(*) AS count
            FROM delivery_attempts
            WHERE run_id = ? AND outcome = 'rejected'
              AND error_category != 'INTERNAL_ERROR'
            GROUP BY error_category ORDER BY count DESC, error_category
            """,
            [str(run_id)],
        )
        conflict_categories = self.repository.query(
            """
            SELECT error_category, COUNT(*) AS count
            FROM delivery_attempts
            WHERE run_id = ? AND outcome = 'conflict'
            GROUP BY error_category ORDER BY count DESC, error_category
            """,
            [str(run_id)],
        )
        operational_delivery_failures = self.repository.query(
            """
            SELECT attempt_id, connection_id, received_at, error_category,
                   error_message, response_sent_at
            FROM delivery_attempts
            WHERE run_id = ? AND outcome = 'rejected'
              AND error_category = 'INTERNAL_ERROR'
            ORDER BY received_at
            """,
            [str(run_id)],
        )
        duplicates = self.repository.query(
            """
            SELECT attempt_id, event_id, received_at, response_sent_at
            FROM delivery_attempts
            WHERE run_id = ? AND duplicate = TRUE ORDER BY received_at
            """,
            [str(run_id)],
        )
        processing_failures = self.repository.query(
            """
            SELECT * FROM processing_attempts
            WHERE outcome != 'success' AND event_id IN
              (SELECT event_id FROM events WHERE run_id = ?)
            ORDER BY started_at
            """,
            [str(run_id)],
        )
        unrendered = self.repository.query(
            """
            SELECT event_id, source_sequence, event_type, processed_at,
                   first_dispatched_at
            FROM events WHERE run_id = ? AND render_acknowledged_at IS NULL
            ORDER BY source_sequence
            """,
            [str(run_id)],
        )
        gaps = self.repository.query(
            """
            SELECT event_id, source_sequence, arrival_index, sequence_gap
            FROM events WHERE run_id = ? AND sequence_gap > 0
            ORDER BY arrival_index
            """,
            [str(run_id)],
        )
        out_of_order = self.repository.query(
            """
            SELECT event_id, source_sequence, arrival_index
            FROM events WHERE run_id = ? AND out_of_order = TRUE
            ORDER BY arrival_index
            """,
            [str(run_id)],
        )
        disconnection_evidence = self.repository.query(
            """
            SELECT side, kind, connection_id, occurred_at, detail_json
            FROM connection_events
            WHERE run_id = ? AND kind IN ('forced_disconnect', 'disconnected')
            ORDER BY occurred_at
            """,
            [str(run_id)],
        )
        return {
            "run_id": str(run_id),
            "payload_rejection_categories": payload_rejection_categories,
            "conflict_categories": conflict_categories,
            "operational_delivery_failures": operational_delivery_failures,
            "duplicate_deliveries": duplicates,
            "processing_failures": processing_failures,
            "unrendered_events": unrendered,
            "arrival_sequence_gaps": gaps,
            "arrival_sequence_gap_note": (
                "Arrival gaps show what was missing at that moment; "
                "later arrivals may close them."
            ),
            "out_of_order_events": out_of_order,
            "disconnection_evidence": disconnection_evidence,
        }
