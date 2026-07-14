"""Playwright proof of the simulator-to-browser lifecycle."""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import httpx
import pytest
from playwright.sync_api import expect, sync_playwright

from streamlab.models import ScenarioName
from streamlab.simulator import build_scenario, run_scenario


@pytest.mark.e2e
def test_reconnect_retry_render_ack_and_dom_deduplication(
    live_server,
    tmp_path,
) -> None:
    api_url, ws_url = live_server
    config = build_scenario(
        ScenarioName.RECONNECT_BURST,
        seed=20250314,
        event_count=12,
        event_rate=20,
        burst_event_rate=40,
        run_id=uuid4(),
    )
    expected_valid = config.event_count - len(config.invalid_sequences)
    invalid_ids = {
        str(config.manifest[sequence - 1].event_id)
        for sequence in config.invalid_sequences
    }
    by_sequence = {event.source_sequence: event for event in config.manifest}
    first_delivered_valid = next(
        by_sequence[sequence]
        for sequence in config.delivery_order
        if str(by_sequence[sequence].event_id) not in invalid_ids
    )
    console_errors: list[str] = []

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            lambda: asyncio.run(
                run_scenario(
                    config,
                    api_url=api_url,
                    ws_url=ws_url,
                    overlay_wait=10,
                )
            )
        )
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            response = httpx.get(
                f"{api_url}/api/runs/{config.run_id}/overview",
                timeout=1,
            )
            if response.status_code == 200:
                break
            time.sleep(0.05)
        else:
            pytest.fail("simulator did not create its run")

        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1280, "height": 800})
            page.add_init_script(
                """
                const NativeWebSocket = window.WebSocket;
                window.__streamlabTestSockets = [];
                window.WebSocket = class TrackedWebSocket extends NativeWebSocket {
                  constructor(...args) {
                    if (window.__streamlabTestSockets.length > 0) {
                      const replayUrl = new URL(args[0]);
                      replayUrl.searchParams.set(
                        "session_id",
                        `${replayUrl.searchParams.get("session_id")}-replay`,
                      );
                      args[0] = replayUrl.toString();
                    }
                    super(...args);
                    window.__streamlabTestSocket = this;
                    window.__streamlabTestSockets.push(this);
                  }
                };
                """
            )
            page.on(
                "console",
                lambda message: (
                    console_errors.append(message.text)
                    if message.type == "error"
                    else None
                ),
            )
            page.goto(f"{api_url}/overlay?run_id={config.run_id}")
            expect(page.locator("#connection-state strong")).to_have_text(
                "Connected",
                timeout=10_000,
            )
            cards = page.locator("[data-event-id]")
            expect(
                page.locator(f'[data-event-id="{first_delivered_valid.event_id}"]')
            ).to_have_count(1, timeout=10_000)
            evidence_deadline = time.monotonic() + 5
            while time.monotonic() < evidence_deadline:
                before_reconnect_evidence = httpx.get(
                    f"{api_url}/api/runs/{config.run_id}/events/"
                    f"{first_delivered_valid.event_id}",
                    timeout=1,
                ).json()
                if before_reconnect_evidence["render_acknowledgments"]:
                    break
                time.sleep(0.02)
            else:
                pytest.fail("first browser render acknowledgment was not stored")
            cards_before_reconnect = cards.count()
            assert 0 < cards_before_reconnect < expected_valid
            page.evaluate(
                "window.__streamlabTestSocket.close(4000, 'playwright reconnect proof')"
            )
            page.wait_for_function(
                """
                window.__streamlabTestSockets.length >= 2 &&
                window.__streamlabTestSockets.at(-1).readyState === WebSocket.OPEN
                """,
                timeout=10_000,
            )
            expect(page.locator("#connection-state strong")).to_have_text(
                "Connected",
                timeout=10_000,
            )
            result = future.result(timeout=30)
            assert result.completed

            expect(cards).to_have_count(expected_valid, timeout=15_000)
            assert cards.count() > cards_before_reconnect
            for event in config.manifest:
                expected = 0 if str(event.event_id) in invalid_ids else 1
                expect(
                    page.locator(f'[data-event-id="{event.event_id}"]')
                ).to_have_count(expected)
            page.screenshot(path=str(tmp_path / "overlay-vertical.png"), full_page=True)
            browser.close()

    deadline = time.monotonic() + 10
    overview: dict[str, object] = {}
    while time.monotonic() < deadline:
        overview = httpx.get(
            f"{api_url}/api/runs/{config.run_id}/overview",
            timeout=1,
        ).json()
        if overview.get("rendered") == expected_valid:
            break
        time.sleep(0.05)

    assert overview["rendered"] == expected_valid
    assert overview["processed"] == expected_valid
    assert overview["payload_rejections"] == len(config.invalid_sequences)
    assert int(overview["duplicates"]) >= len(config.duplicate_sequences)
    assert overview["retries"] == 1
    assert (
        overview["scenario_checks"]["forced_reconnect"]["target_duplicate_attempts"]
        == 1
    )
    assert (
        overview["scenario_checks"]["forced_reconnect"]["transport_correlated"] is True
    )
    assert (
        overview["scenario_checks"]["forced_reconnect"]["attempt_path_correlated"]
        is True
    )
    assert overview["scenario_checks"]["delayed_delivery"]["passed"] is True
    assert overview["verdict"] == "pass"
    evidence = httpx.get(
        f"{api_url}/api/runs/{config.run_id}/events/{first_delivered_valid.event_id}",
        timeout=2,
    ).json()
    assert len(evidence["dispatches"]) == 2
    assert len(evidence["render_acknowledgments"]) == 2
    assert len({item["session_id"] for item in evidence["render_acknowledgments"]}) == 2
    assert console_errors == []
