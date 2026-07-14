"""Fixed-seed 500-event portfolio scenario reconciliation."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import httpx
import pytest
from playwright.sync_api import expect, sync_playwright

from streamlab.models import ScenarioName
from streamlab.simulator import DEMO_SEED, build_scenario, run_scenario


@pytest.mark.scenario
def test_fixed_seed_500_event_reconnect_burst_reconciles(live_server, tmp_path) -> None:
    api_url, ws_url = live_server
    config = build_scenario(
        ScenarioName.RECONNECT_BURST,
        seed=DEMO_SEED,
        event_count=500,
        event_rate=1_000,
        burst_event_rate=5_000,
        run_id=uuid4(),
    )
    expected_invalid = len(config.invalid_sequences)
    expected_unique = config.event_count - expected_invalid
    expected_planned_duplicates = len(config.duplicate_sequences)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            lambda: asyncio.run(
                run_scenario(
                    config,
                    api_url=api_url,
                    ws_url=ws_url,
                    overlay_wait=15,
                )
            )
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            response = httpx.get(
                f"{api_url}/api/runs/{config.run_id}/overview",
                timeout=1,
            )
            if response.status_code == 200:
                break
            time.sleep(0.05)
        else:
            pytest.fail("500-event run was not created")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1440, "height": 900})
            page.goto(f"{api_url}/overlay?run_id={config.run_id}")
            expect(page.locator("#connection-state strong")).to_have_text(
                "Connected",
                timeout=10_000,
            )
            result = future.result(timeout=120)
            assert result.completed
            assert result.generated == 500
            assert result.client_acked_unique == 490
            assert result.accepted_replies == 489
            assert result.duplicate_replies == 26
            assert result.rejected_replies == 10
            assert result.conflict_replies == 0
            assert result.retries == 1
            assert result.forced_reconnects == 1
            expect(page.locator("[data-event-id]")).to_have_count(
                expected_unique,
                timeout=30_000,
            )
            page.screenshot(path=str(tmp_path / "reconnect-burst-500.png"))
            browser.close()

    deadline = time.monotonic() + 20
    overview: dict[str, object] = {}
    while time.monotonic() < deadline:
        overview = httpx.get(
            f"{api_url}/api/runs/{config.run_id}/overview",
            timeout=2,
        ).json()
        if overview.get("rendered") == expected_unique:
            break
        time.sleep(0.05)

    assert overview["generated"] == 500
    assert overview["unique_events"] == expected_unique == 490
    assert overview["acknowledged"] == 490
    assert overview["processed"] == 490
    assert overview["dispatched"] == 490
    assert overview["rendered"] == 490
    assert overview["payload_rejections"] == expected_invalid == 10
    assert overview["operational_delivery_failures"] == 0
    assert overview["payload_rejection_rate_percent"] == 1.9
    assert overview["duplicates"] == expected_planned_duplicates + 1 == 26
    assert overview["delivered"] == 526
    assert overview["valid_deliveries"] == 516
    assert overview["conflicts"] == 0
    assert overview["retries"] == 1
    assert overview["reconnection"]["forced_disconnects"] == 1
    assert overview["reconnection"]["recovery_completions"] == 1
    assert (
        overview["scenario_checks"]["forced_reconnect"]["target_accepted_attempts"] == 1
    )
    assert (
        overview["scenario_checks"]["forced_reconnect"]["target_duplicate_attempts"]
        == 1
    )
    assert overview["scenario_checks"]["forced_reconnect"]["target_correlation"] is True
    assert (
        overview["scenario_checks"]["forced_reconnect"]["transport_correlated"] is True
    )
    assert (
        overview["scenario_checks"]["forced_reconnect"]["attempt_path_correlated"]
        is True
    )
    reconnect_check = overview["scenario_checks"]["forced_reconnect"]
    assert reconnect_check["accepted_reply_sent_on_forced_transport"] == 1
    assert reconnect_check["duplicate_reply_sent_on_reconnected_transport"] == 1
    order_check = overview["scenario_checks"]["out_of_order_delivery"]
    assert order_check["passed"] is True
    assert order_check["mismatch_count"] == 0
    assert order_check["observed_canonical_count"] == 490
    delay_check = overview["scenario_checks"]["delayed_delivery"]
    assert delay_check["required"] is True
    assert delay_check["passed"] is True
    assert delay_check["configured_delay_ms"] == 25
    assert delay_check["expected_minimum"] == 5
    assert delay_check["observed"] == 5
    assert delay_check["missing_sequences"] == []
    assert len(delay_check["measured_delay_ms"]) == 5
    assert all(value >= 25 for value in delay_check["measured_delay_ms"].values())
    assert overview["latency_sample_count"] == 490
    assert overview["p50_latency_ms"] is not None
    assert overview["p95_latency_ms"] is not None
    assert overview["p99_latency_ms"] is not None
    assert overview["verdict"] == "pass"

    assert config.disconnect_sequence is not None
    reconnect_target = config.manifest[config.disconnect_sequence - 1]
    target_evidence = httpx.get(
        f"{api_url}/api/runs/{config.run_id}/events/{reconnect_target.event_id}",
        timeout=10,
    ).json()
    target_attempts = target_evidence["delivery_attempts"]
    assert [attempt["outcome"] for attempt in target_attempts] == [
        "accepted",
        "duplicate",
    ]
    assert all(attempt["response_sent_at"] is not None for attempt in target_attempts)
    assert target_attempts[0]["connection_id"] != target_attempts[1]["connection_id"]

    performance = httpx.get(
        f"{api_url}/api/runs/{config.run_id}/performance",
        timeout=10,
    ).json()
    assert performance["sample_count"] == 490
    assert performance["burst_comparison"]["burst_samples"] > 0
    assert performance["reconnection_comparison"]["after_samples"] > 0

    failures = httpx.get(
        f"{api_url}/api/runs/{config.run_id}/failures",
        timeout=10,
    ).json()
    assert len(failures["duplicate_deliveries"]) >= 25
    assert len(failures["payload_rejection_categories"]) >= 4
    assert failures["conflict_categories"] == []
    assert failures["operational_delivery_failures"] == []
    assert failures["unrendered_events"] == []
    assert failures["out_of_order_events"]
