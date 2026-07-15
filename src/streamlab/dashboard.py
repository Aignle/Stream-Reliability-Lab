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


def _percentage(value: object) -> str:
    if value is None:
        return "Not measured"
    numeric = float(cast(float, value))
    return f"{numeric:,.0f}%" if numeric.is_integer() else f"{numeric:,.2f}%"


def _latency(value: object) -> str:
    return "Not measured" if value is None else f"{float(cast(float, value)):,.2f} ms"


def _run_story(overview: dict[str, Any]) -> str:
    """Summarize the stored lifecycle as one readable progression."""
    stages = (
        ("generated", "generated"),
        ("delivered", "attempts"),
        ("payload_rejections", "rejected"),
        ("unique_events", "unique"),
        ("processed", "processed"),
        ("rendered", "rendered"),
    )
    return " → ".join(
        f"{_format_number(overview[key])} {label}" for key, label in stages
    )


def _reconnect_story(overview: dict[str, Any]) -> str | None:
    """Explain forced-reconnect evidence without inventing client reply counts."""
    checks = cast(dict[str, Any], overview.get("scenario_checks", {}))
    check = cast(dict[str, Any], checks.get("forced_reconnect", {}))
    if not check.get("required"):
        return None

    old_transport_ack = int(check.get("accepted_reply_sent_on_forced_transport") or 0)
    new_transport_duplicate = int(
        check.get("duplicate_reply_sent_on_reconnected_transport") or 0
    )
    correlated = bool(check.get("attempt_path_correlated"))
    if old_transport_ack != 1 or new_transport_duplicate != 1 or not correlated:
        return (
            "Reconnect proof is incomplete: the stored accepted-reply and "
            "duplicate-retry path does not fully correlate across transports."
        )

    unique_events = int(overview["unique_events"])
    client_acked_unique = int(overview["client_acked_unique"])
    rendered = int(overview["rendered"])
    story = (
        "The server sent the reconnect target's accepted reply on the old "
        "transport, but the simulator intentionally did not observe it. The "
        "retry on the new transport returned duplicate. "
        f"The client reconciled {client_acked_unique:,} of {unique_events:,} "
        "unique event IDs."
    )
    if client_acked_unique == unique_events and rendered == unique_events:
        return f"{story} All {rendered:,} reached stored browser render evidence."
    if rendered < unique_events:
        return (
            f"{story} Browser proof is still incomplete: {rendered:,} of "
            f"{unique_events:,} have stored render evidence."
        )
    return story


def _metric_row(overview: dict[str, Any]) -> None:
    columns = st.columns(4)
    for column, (label, key) in zip(
        columns,
        (
            ("Delivery attempts", "delivered"),
            ("Canonical events", "unique_events"),
            ("Processed", "processed"),
            ("DOM-acknowledged", "rendered"),
        ),
        strict=True,
    ):
        column.metric(label, _format_number(overview[key]))


def _overview_tab(overview: dict[str, Any]) -> None:
    verdict = str(overview["verdict"])
    verdict_text = (
        f"**{verdict.title()} · {overview['scenario']} · seed {overview['seed']}**\n\n"
        f"{overview['verdict_reason']}"
    )
    if verdict == "pass":
        st.success(verdict_text)
    elif verdict == "fail":
        st.error(verdict_text)
    else:
        st.warning(verdict_text)

    st.subheader("One run, end to end")
    st.markdown(
        f"<p class='run-story'>{_run_story(overview)}</p>",
        unsafe_allow_html=True,
    )
    st.caption("Each step comes from stored API evidence for the selected run.")
    _metric_row(overview)

    left, right = st.columns((1.1, 1))
    with left:
        st.subheader("Reliability evidence")
        operational_failures = _format_number(overview["operational_delivery_failures"])
        st.markdown(
            "\n".join(
                (
                    f"- **{_format_number(overview['duplicates'])}** duplicate "
                    "attempts — no second canonical event or processing effect",
                    f"- **{_format_number(overview['payload_rejections'])}** "
                    "payload-rejected attempts — "
                    f"{_percentage(overview['payload_rejection_rate_percent'])}",
                    f"- **{_format_number(overview['conflicts'])}** identity conflicts",
                    f"- **{operational_failures}** operational ingestion failures",
                    f"- **{_percentage(overview['processing_completion_percent'])}** "
                    "processing completion · "
                    f"**{_percentage(overview['render_completion_percent'])}** "
                    "render completion",
                    "- Browser replay deduplication is verified within one "
                    "browser document across an overlay reconnect in the "
                    "Playwright path.",
                )
            )
        )
    with right:
        st.subheader("Timing")
        if overview["p50_latency_ms"] is None:
            st.info(
                "Persist-to-DOM-ACK latency is not measured until a browser responds."
            )
        else:
            st.markdown(
                f"**Persist → DOM ACK:** {_latency(overview['p50_latency_ms'])} "
                f"median · {_latency(overview['p95_latency_ms'])} p95 · "
                f"{_latency(overview['p99_latency_ms'])} p99"
            )
            st.caption(
                f"{overview['latency_sample_count']:,} stored samples · "
                f"{overview['latency_definition']}"
            )
        reconnect_duration = overview["reconnection"]["duration_ms"]
        if reconnect_duration is not None:
            st.markdown(
                f"**Reconnect marker interval:** {_latency(reconnect_duration)}"
            )

    reconnect_story = _reconnect_story(overview)
    if reconnect_story is not None:
        st.subheader("Reconnect evidence")
        if reconnect_story.startswith("Reconnect proof is incomplete"):
            st.warning(reconnect_story)
        else:
            st.info(reconnect_story)

    with st.expander("Metric definitions"):
        st.markdown(
            "\n".join(
                (
                    "- **Payload-rejection rate:** "
                    f"{overview['payload_rejection_rate_definition']}",
                    "- **Processing attempt success:** "
                    f"{_percentage(overview['processing_attempt_success_percent'])}",
                    "- **Processing completion:** "
                    f"{_percentage(overview['processing_completion_percent'])}",
                    "- **Render completion:** "
                    f"{_percentage(overview['render_completion_percent'])}",
                    "- Configured event rates are simulator pacing settings, not "
                    "measured throughput.",
                )
            )
        )

    with st.expander("Technical run evidence"):
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
    st.caption(f"Selected event `{selected_id}`")
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
    with st.expander("Technical traffic comparisons"):
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
          .block-container {max-width: 1180px; padding-top: 1.8rem;}
          [data-testid="stMetric"] {background: transparent; border: 0;
            border-bottom: 1px solid rgba(128, 139, 158, .28);
            border-radius: 0; padding: .25rem 0 .8rem;}
          .run-story {font-size: clamp(1.08rem, 2.1vw, 1.5rem);
            font-weight: 650; line-height: 1.55; margin: -.2rem 0 .1rem;
            overflow-wrap: anywhere;}
          code {font-variant-ligatures: none;}
          :focus-visible {outline: 3px solid #5fa8ff; outline-offset: 2px;}
          @media (max-width: 700px) {
            .block-container {padding: 1rem 1rem 2rem;}
            .run-story {font-size: 1rem; line-height: 1.65;}
          }
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
    st.caption("What reached the browser after retries, rejection, and reconnects?")
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
    st.caption(f"Run ID `{run_id}`")
    try:
        overview = api_get(f"/api/runs/{run_id}/overview")
        tabs = st.tabs(("Run story", "Event trail", "Timing", "Exceptions"))
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
