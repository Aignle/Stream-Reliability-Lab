"""Streamlit dashboard consuming the FastAPI analytics boundary only."""

from __future__ import annotations

import os
from typing import Any, cast
from urllib.parse import urlparse

import httpx
import streamlit as st

DEFAULT_API_URL = "http://127.0.0.1:8000"


def api_base_url() -> str:
    """Return a local analytics API URL and reject accidental external access."""
    value = os.environ.get("STREAMLAB_API_URL", DEFAULT_API_URL).rstrip("/")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or parsed.hostname not in {
        "127.0.0.1",
        "localhost",
        "api",
    }:
        raise ValueError("STREAMLAB_API_URL must point to the local Stream Lab API")
    return value


def api_get(
    path: str,
    params: dict[str, str | int | float | bool | None] | None = None,
) -> dict[str, Any]:
    """Fetch one read-only analytics view with bounded local timeouts."""
    response = httpx.get(
        f"{api_base_url()}{path}",
        params=params,
        timeout=10,
    )
    response.raise_for_status()
    return cast(dict[str, Any], response.json())


def _format_number(value: object) -> str:
    if value is None:
        return "Not measured"
    if isinstance(value, float):
        return f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def _latency(value: object) -> str:
    return "Not measured" if value is None else f"{float(cast(float, value)):,.2f} ms"


def _metric_row(overview: dict[str, Any]) -> None:
    columns = st.columns(6)
    for column, (label, key) in zip(
        columns,
        (
            ("Generated", "generated"),
            ("Delivered", "delivered"),
            ("Valid", "valid_deliveries"),
            ("Unique", "unique_events"),
            ("Processed", "processed"),
            ("Rendered", "rendered"),
        ),
        strict=True,
    ):
        column.metric(label, _format_number(overview[key]))


def _overview_tab(overview: dict[str, Any]) -> None:
    verdict = str(overview["verdict"])
    st.markdown(
        f"<div class='verdict verdict-{verdict}'><span>Run verdict</span>"
        f"<strong>{verdict.upper()}</strong><p>{overview['verdict_reason']}</p></div>",
        unsafe_allow_html=True,
    )
    _metric_row(overview)

    left, right = st.columns((1.25, 1))
    with left:
        st.subheader("Lifecycle funnel")
        funnel = [
            {"stage": "Generated", "events": overview["generated"]},
            {"stage": "Unique", "events": overview["unique_events"]},
            {"stage": "Server ACK sent", "events": overview["acknowledged"]},
            {"stage": "Processed", "events": overview["processed"]},
            {"stage": "Dispatched", "events": overview["dispatched"]},
            {"stage": "Rendered", "events": overview["rendered"]},
        ]
        st.bar_chart(
            funnel,
            x="stage",
            y="events",
            horizontal=True,
            sort=False,
        )
    with right:
        st.subheader("Reliability summary")
        first, second = st.columns(2)
        first.metric("Duplicate attempts", _format_number(overview["duplicates"]))
        second.metric("Identity conflicts", _format_number(overview["conflicts"]))
        first.metric(
            "Payload rejections",
            _format_number(overview["payload_rejections"]),
        )
        second.metric(
            "Payload-rejection rate",
            f"{_format_number(overview['payload_rejection_rate_percent'])}%",
            help=str(overview["payload_rejection_rate_definition"]),
        )
        first.metric(
            "Operational ingestion failures",
            _format_number(overview["operational_delivery_failures"]),
        )
        second.metric(
            "Processing attempt success",
            f"{_format_number(overview['processing_attempt_success_percent'])}%",
        )
        first.metric(
            "Processing completion",
            f"{_format_number(overview['processing_completion_percent'])}%",
        )
        second.metric(
            "Render completion",
            f"{_format_number(overview['render_completion_percent'])}%",
        )
        st.caption(
            f"Latency uses {overview['latency_definition']} and "
            f"{overview['latency_sample_count']:,} stored samples."
        )

    st.subheader("End-to-end latency")
    p50, p95, p99, reconnect = st.columns(4)
    p50.metric("p50", _latency(overview["p50_latency_ms"]))
    p95.metric("p95", _latency(overview["p95_latency_ms"]))
    p99.metric("p99", _latency(overview["p99_latency_ms"]))
    reconnect.metric(
        "Reconnect duration",
        _latency(overview["reconnection"]["duration_ms"]),
    )

    with st.expander("Run configuration and recovery evidence"):
        st.json(
            {
                "scenario": overview["scenario"],
                "status": overview["status"],
                "seed": overview["seed"],
                "configured_event_rate": overview["configured_event_rate"],
                "burst_event_rate": overview["burst_event_rate"],
                "client_acked_unique": overview["client_acked_unique"],
                "retries": overview["retries"],
                "reconnection": overview["reconnection"],
                "scenario_checks": overview["scenario_checks"],
            }
        )


def _events_tab(run_id: str) -> None:
    search = st.text_input(
        "Search events",
        placeholder="Event ID, type, actor, or source sequence",
    )
    payload = api_get(
        f"/api/runs/{run_id}/events",
        {"search": search, "limit": 1_000},
    )
    events = cast(list[dict[str, Any]], payload["events"])
    if not events:
        st.info("No generated events match this search.")
        return

    st.caption(f"Showing {len(events):,} of {payload['total']:,} matching events.")
    st.dataframe(
        events,
        width="stretch",
        hide_index=True,
        column_order=(
            "source_sequence",
            "event_type",
            "event_id",
            "lifecycle_status",
            "delivery_count",
            "processing_count",
            "render_count",
            "out_of_order",
            "sequence_gap",
        ),
    )
    choices = {
        (
            f"#{item['source_sequence']} · {item['event_type']} · "
            f"{item['event_id'][:8]}"
        ): item["event_id"]
        for item in events
    }
    selected_label = st.selectbox("Inspect one event timeline", choices)
    selected_id = choices[selected_label]
    evidence = api_get(f"/api/runs/{run_id}/events/{selected_id}")
    st.dataframe(
        evidence["timeline"],
        width="stretch",
        hide_index=True,
        column_order=("at", "stage", "evidence_id"),
    )
    with st.expander("Raw stored evidence for this event"):
        st.json(evidence)


def _performance_tab(run_id: str) -> None:
    performance = api_get(f"/api/runs/{run_id}/performance")
    st.caption(
        f"{performance['sample_count']:,} samples · {performance['latency_definition']}"
    )
    p50, p95, p99 = st.columns(3)
    p50.metric("p50", _latency(performance["percentiles_ms"]["p50"]))
    p95.metric("p95", _latency(performance["percentiles_ms"]["p95"]))
    p99.metric("p99", _latency(performance["percentiles_ms"]["p99"]))

    throughput = performance["throughput_per_second"]
    latency_points = performance["latency_over_time"]
    left, right = st.columns(2)
    with left:
        st.subheader("Throughput over time")
        if throughput:
            st.line_chart(throughput, x="second", y="events")
        else:
            st.info("No persisted event throughput is available yet.")
    with right:
        st.subheader("Latency over time")
        if latency_points:
            st.line_chart(
                latency_points,
                x="source_sequence",
                y="latency_ms",
            )
        else:
            st.info("No browser render latency samples are available yet.")

    st.subheader("Latency distribution")
    st.bar_chart(
        performance["latency_distribution"],
        x="range",
        y="count",
    )
    comparison_a, comparison_b = st.columns(2)
    with comparison_a:
        st.markdown("**Normal traffic vs burst**")
        st.json(performance["burst_comparison"])
    with comparison_b:
        st.markdown("**Before vs after reconnect**")
        st.json(performance["reconnection_comparison"])


def _failure_section(title: str, items: list[dict[str, Any]], empty: str) -> None:
    st.subheader(title)
    if items:
        st.dataframe(items, width="stretch", hide_index=True)
    else:
        st.success(empty)


def _failures_tab(run_id: str) -> None:
    failures = api_get(f"/api/runs/{run_id}/failures")
    _failure_section(
        "Payload-rejection categories",
        failures["payload_rejection_categories"],
        "No payload-rejected deliveries were recorded.",
    )
    _failure_section(
        "Identity-conflict categories",
        failures["conflict_categories"],
        "No event-identity conflicts were recorded.",
    )
    _failure_section(
        "Operational ingestion failures",
        failures["operational_delivery_failures"],
        "No operational ingestion failures were recorded.",
    )
    _failure_section(
        "Duplicate deliveries",
        failures["duplicate_deliveries"],
        "No duplicate deliveries were recorded.",
    )
    _failure_section(
        "Processing failures",
        failures["processing_failures"],
        "No processing failures were recorded.",
    )
    _failure_section(
        "Unrendered events",
        failures["unrendered_events"],
        "Every processed event has stored browser render evidence.",
    )
    _failure_section(
        "Arrival sequence gaps",
        failures["arrival_sequence_gaps"],
        "No arrival-time sequence gaps were observed.",
    )
    st.caption(failures["arrival_sequence_gap_note"])
    _failure_section(
        "Out-of-order arrivals",
        failures["out_of_order_events"],
        "No out-of-order arrivals were observed.",
    )
    _failure_section(
        "Disconnection evidence",
        failures["disconnection_evidence"],
        "No disconnection evidence was recorded for this run.",
    )


def _style() -> None:
    st.markdown(
        """
        <style>
          .block-container {max-width: 1380px; padding-top: 2.2rem;}
          [data-testid="stMetric"] {background: #111722; border: 1px solid #293244;
            border-radius: 14px; padding: 14px 16px;}
          .verdict {display: grid; grid-template-columns: 1fr auto; gap: 4px 24px;
            margin: 0 0 20px; padding: 18px 20px; border: 1px solid #293244;
            border-radius: 16px; background: #111722;}
          .verdict span {color: #95a2b6; font-size: 12px; letter-spacing: .1em;
            text-transform: uppercase;}
          .verdict strong {grid-row: span 2; align-self: center; color: #f2bd68;
            font-size: 22px; letter-spacing: .06em;}
          .verdict p {margin: 0; color: #c8d0dc;}
          .verdict-pass strong {color: #72d99f;}
          .verdict-fail strong {color: #ff8f8f;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def main() -> None:
    st.set_page_config(
        page_title="Stream Reliability Lab",
        page_icon="SR",
        layout="wide",
    )
    _style()
    st.title("Stream Reliability Lab")
    st.caption(
        "Stored lifecycle evidence for deterministic, synthetic WebSocket scenarios."
    )
    try:
        runs_payload = api_get("/api/runs", {"limit": 200})
        runs = cast(list[dict[str, Any]], runs_payload["runs"])
    except (httpx.HTTPError, ValueError) as error:
        st.error(f"The local analytics API is unavailable: {error}")
        st.code("python -m uvicorn streamlab.main:app --host 127.0.0.1 --port 8000")
        st.stop()

    if not runs:
        st.info("No runs exist yet. Start a deterministic simulator scenario first.")
        st.code(
            "python -m streamlab.simulator --scenario happy_path --count 20 --seed 42"
        )
        st.stop()

    labels = {
        f"{item['scenario']} · {item['status']} · {str(item['run_id'])[:8]}": str(
            item["run_id"]
        )
        for item in runs
    }
    selected_label = st.selectbox("Run", labels)
    run_id = labels[selected_label]
    try:
        overview = api_get(f"/api/runs/{run_id}/overview")
        tabs = st.tabs(("Overview", "Event lifecycle", "Performance", "Failures"))
        with tabs[0]:
            _overview_tab(overview)
        with tabs[1]:
            _events_tab(run_id)
        with tabs[2]:
            _performance_tab(run_id)
        with tabs[3]:
            _failures_tab(run_id)
    except httpx.HTTPError as error:
        st.error(f"This run's analytics could not be loaded: {error}")


if __name__ == "__main__":
    main()
