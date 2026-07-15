"""Streamlit dashboard boundary and runtime smoke tests."""

import asyncio
from pathlib import Path
from uuid import uuid4

import httpx
from streamlit.testing.v1 import AppTest

from streamlab.dashboard import _percentage, _reconnect_story, _run_story
from streamlab.models import ScenarioName
from streamlab.simulator import build_scenario, run_scenario

DASHBOARD_PATH = Path(__file__).parents[2] / "src" / "streamlab" / "dashboard.py"


def test_dashboard_source_uses_api_boundary_only() -> None:
    source = DASHBOARD_PATH.read_text(encoding="utf-8").lower()

    assert "httpx" in source
    assert "duckdb" not in source
    assert "repository" not in source


def test_dashboard_has_honest_empty_state(live_server, monkeypatch) -> None:
    api_url, _ = live_server
    monkeypatch.setenv("STREAMLAB_API_URL", api_url)

    application = AppTest.from_file(str(DASHBOARD_PATH), default_timeout=20).run()

    assert not application.exception
    assert application.title[0].value == "Stream Reliability Lab"
    assert "No runs exist yet" in application.info[0].value


def test_dashboard_renders_stored_run_metrics(live_server, monkeypatch) -> None:
    api_url, _ = live_server
    monkeypatch.setenv("STREAMLAB_API_URL", api_url)
    config = build_scenario(
        ScenarioName.HAPPY_PATH,
        seed=77,
        event_count=3,
        event_rate=100,
        run_id=uuid4(),
    )
    response = httpx.post(
        f"{api_url}/api/runs",
        json=config.model_dump(mode="json"),
        timeout=10,
    )
    response.raise_for_status()

    application = AppTest.from_file(str(DASHBOARD_PATH), default_timeout=20).run()

    assert not application.exception
    assert application.selectbox[0].value.startswith("happy_path")
    metric_values = {item.label: item.value for item in application.metric}
    assert metric_values["Delivery attempts"] == "0"
    assert metric_values["Canonical events"] == "0"
    assert metric_values["Processed"] == "0"
    assert metric_values["DOM-acknowledged"] == "0"
    assert any(
        "3 generated → 0 attempts → 0 rejected → 0 unique → 0 processed → "
        "0 rendered" in item.value
        for item in application.markdown
    )
    assert [tab.label for tab in application.tabs] == [
        "Run story",
        "Event trail",
        "Timing",
        "Exceptions",
    ]
    expander_labels = {item.label for item in application.expander}
    assert {
        "Metric definitions",
        "Technical run evidence",
        "Raw stored evidence for this event",
        "Technical traffic comparisons",
    } <= expander_labels
    reliability_copy = " ".join(item.value for item in application.markdown)
    assert "payload-rejected attempts" in reliability_copy
    assert "within one browser document across an overlay reconnect" in reliability_copy


def test_run_story_uses_api_counts_in_lifecycle_order() -> None:
    overview = {
        "generated": 500,
        "delivered": 526,
        "payload_rejections": 10,
        "unique_events": 490,
        "processed": 490,
        "rendered": 490,
    }

    assert _run_story(overview) == (
        "500 generated → 526 attempts → 10 rejected → 490 unique → "
        "490 processed → 490 rendered"
    )


def test_dashboard_formats_whole_and_fractional_percentages_readably() -> None:
    assert _percentage(None) == "Not measured"
    assert _percentage(100.0) == "100%"
    assert _percentage(1.9) == "1.90%"


def test_reconnect_story_requires_correlated_stored_evidence() -> None:
    overview = {
        "unique_events": 4,
        "client_acked_unique": 4,
        "rendered": 4,
        "scenario_checks": {
            "forced_reconnect": {
                "required": True,
                "attempt_path_correlated": False,
                "accepted_reply_sent_on_forced_transport": 1,
                "duplicate_reply_sent_on_reconnected_transport": 1,
            }
        },
    }

    story = _reconnect_story(overview)

    assert story is not None
    assert story.startswith("Reconnect proof is incomplete")
    assert "All 4 reached" not in story


def test_dashboard_explains_real_forced_reconnect_without_overclaiming_browser(
    live_server,
    monkeypatch,
) -> None:
    api_url, ws_url = live_server
    monkeypatch.setenv("STREAMLAB_API_URL", api_url)
    config = build_scenario(
        ScenarioName.FORCED_RECONNECT,
        seed=78,
        event_count=4,
        event_rate=1_000,
        run_id=uuid4(),
    )

    result = asyncio.run(
        run_scenario(config, api_url=api_url, ws_url=ws_url, overlay_wait=0)
    )
    overview = httpx.get(
        f"{api_url}/api/runs/{config.run_id}/overview",
        timeout=10,
    ).json()

    assert result.completed is True
    assert overview["generated"] == 4
    assert overview["delivered"] == 5
    assert overview["unique_events"] == 4
    assert overview["processed"] == 4
    assert overview["rendered"] == 0
    assert overview["duplicates"] == 1
    assert overview["retries"] == 1
    reconnect_check = overview["scenario_checks"]["forced_reconnect"]
    assert reconnect_check["passed"] is True
    assert reconnect_check["attempt_path_correlated"] is True
    assert reconnect_check["accepted_reply_sent_on_forced_transport"] == 1
    assert reconnect_check["duplicate_reply_sent_on_reconnected_transport"] == 1

    application = AppTest.from_file(str(DASHBOARD_PATH), default_timeout=20).run()

    assert not application.exception
    reconnect_copy = " ".join(item.value for item in application.info)
    assert "simulator intentionally did not observe it" in reconnect_copy
    assert "retry on the new transport returned duplicate" in reconnect_copy
    assert "Browser proof is still incomplete: 0 of 4" in reconnect_copy
    assert "All 4 reached stored browser render evidence" not in reconnect_copy
