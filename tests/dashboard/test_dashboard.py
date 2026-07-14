"""Streamlit dashboard boundary and runtime smoke tests."""

from pathlib import Path
from uuid import uuid4

import httpx
from streamlit.testing.v1 import AppTest

from streamlab.models import ScenarioName
from streamlab.simulator import build_scenario

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
    metric_labels = {item.label for item in application.metric}
    assert {
        "Generated",
        "Delivered",
        "Unique",
        "Processed",
        "Rendered",
        "Duplicate attempts",
        "Identity conflicts",
        "Payload rejections",
        "Payload-rejection rate",
        "Operational ingestion failures",
        "Processing attempt success",
        "Processing completion",
        "Render completion",
    } <= metric_labels
    metric_values = {item.label: item.value for item in application.metric}
    assert metric_values["Generated"] == "3"
    assert metric_values["Delivered"] == "0"
    assert metric_values["Payload-rejection rate"] == "Not measured"
    assert metric_values["Processing attempt success"] == "Not measured"
    assert metric_values["Processing completion"] == "Not measured"
    assert metric_values["Render completion"] == "Not measured"
    assert application.tabs[0].label == "Overview"
    assert application.tabs[1].label == "Event lifecycle"
    assert application.tabs[2].label == "Performance"
    assert application.tabs[3].label == "Failures"
