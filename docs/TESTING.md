# Testing strategy

Tests use fixed UUIDs, fixed seeds, isolated temporary DuckDB files, ephemeral
local ports, and evidence polling. They do not use real platform data or fixed
sleep delays as success assertions.

## Commands

```bash
python -m pytest
python -m pytest -m e2e
python -m pytest -m scenario
python -m ruff check .
python -m ruff format --check .
python -m mypy src
python -m pip check
node --check src/streamlab/static/overlay.js
```

The default pytest command excludes the two explicit browser-heavy markers.
`make check` runs the fast core suite and static checks. `make verify` adds the
Playwright end-to-end test, fixed 500-event scenario, and Compose validation.

## Verification matrix

| Layer | Representative proof |
| --- | --- |
| Contract | Required envelope fields, UTC timestamps, typed payloads, canonical hash stability |
| Generator | Same fixed config produces identical manifest; six scenario plans expose their controls |
| Validation | Six invalid mutations map to six structured categories and zero events |
| Strict input | Numeric strings cannot normalize into canonical integer fields |
| Run isolation | Valid and invalid cross-run payloads remain attributed to the bound socket run |
| Persistence order | A second DuckDB connection sees event + attempt while ACK and processing timestamps are still null |
| Transaction failure | Injected commit failure rolls back attempt and event and cannot produce an accepted result |
| Idempotency | Barrier-concurrent duplicate service calls produce accepted + duplicate, one event, one processing effect |
| Recovery | A persisted event recovers exactly once after a crash between the ACK frame and post-send evidence |
| Application restart | A second app lifespan recovers and replays persisted unprocessed work |
| Render integrity | Unknown and undispatched IDs fail; per-session ACK is idempotent; one session does not suppress another |
| WebSocket protocol | A post-ACK processing failure cannot queue a contradictory NACK; unexpected ingestion failure is audited, closes with 1011, and retries on a new transport |
| Evidence provenance | Public fault markers are simulator-owned; caller JSON cannot create server-observed socket connect/disconnect evidence |
| Analytics | Stored ACK-send counts, payload-rejection rate, processing failures, exact configured order, exact percentile wiring, search, timeline, categorized failures, and neutral disconnection evidence derive from fixtures |
| Dashboard | Streamlit AppTest covers empty and populated API-backed states; source boundary excludes DuckDB |
| Playwright vertical | Real Uvicorn + Chromium prove source submission, overlay socket reconnect, later replay, stored render ACK, and DOM deduplication |
| 500-event scenario | Fixed seed reconciles 500 generated, fault attempts, 490 valid lifecycle completions, reconnect, and metrics |
| Delay evidence | Backdated and post-delivery claims fail; server-stamped pre-delay markers pass only when marker-to-attempt duration meets the plan |

## False-positive controls

- Persistence is inspected before the service records reply evidence, not only
  after a handler returns.
- Duplicate tests assert attempt, event, processing, dispatch, render, and DOM
  counts rather than only the ACK status.
- Browser tests wait for a second real WebSocket, use a new replay session to
  exercise in-document deduplication, assert one DOM card, and poll stored
  dispatch and per-session render evidence.
- Invalid event IDs are explicitly checked for zero DOM matches.
- Scenario success reconciles stored lifecycle, server ACK sends, client-observed
  replies, each planned duplicate, transport-bound reconnect/recovery,
  server-timed delay, and exact configured order; a zero exit code alone is not
  accepted.
- False-positive fixtures prove unrelated delivery sockets cannot satisfy a
  reconnect, a server disconnect after recovery cannot satisfy transport order,
  caller input cannot spoof server provenance, missing server ACK-send evidence
  cannot PASS, a recovered processing failure cannot PASS, and late or
  caller-backdated markers cannot satisfy a delay plan.
- Latency tests assert exact p50/p95/p99 values for known stored samples and do
  not invent a performance threshold.

## CI

GitHub Actions separates core quality, browser/scenario, and Compose jobs. The
browser job installs Chromium explicitly. The Compose job builds both services,
waits on health checks, runs a deterministic forced-reconnect lifecycle inside
the API image, proves dashboard-to-API networking, prints logs on failure, and
always tears down its disposable volume.
