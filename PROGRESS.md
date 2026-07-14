# Progress Log

## Current checkpoint

Checkpoint 8 complete - finalization review remediation and release verification.

## Completed checkpoints

### 1 - Foundation

- Python 3.12 package with bounded runtime and development dependencies.
- FastAPI health endpoint, Make targets, Ruff, strict mypy, pytest, and GitHub
  Actions foundations.
- Local-only environment defaults and ignored runtime evidence.

### 2 - First vertical slice

- Deterministic normalized event and run manifest.
- WebSocket ingestion, validation, atomic attempt + canonical persistence before
  ACK construction, idempotent processing, overlay delivery, DOM insertion,
  and stored browser render acknowledgment.
- Playwright proves the event ID in the DOM and in stored lifecycle evidence.

### 3 - Reliability behavior

- Every observed delivery is audited as accepted, duplicate, rejected, or
  conflict evidence.
- Duplicate events create no second canonical event, successful effect, or DOM
  element in a browser session.
- Invalid payload categories, delayed/out-of-order arrival, forced reconnect,
  ACK-based retry, burst traffic, and restart recovery are configuration driven.
- Startup recovers every persisted unprocessed event, including the crash
  window after an ACK frame but before post-send evidence commits.
- WebSocket run binding, retry socket cleanup, UTC DuckDB sessions, processing
  failure evidence, and render-ack eligibility have regression coverage.

### 4 - Analytics

- Stored-evidence overview, lifecycle, performance, failure, and per-event
  timeline APIs.
- Streamlit consumes only FastAPI APIs and shows ordered lifecycle, searchable
  events, throughput, latency, burst/reconnect comparisons, and failures.
- A pass verdict requires generic lifecycle reconciliation plus planned
  duplicate, transport-bound reconnect/recovery, measured delay, out-of-order,
  stored server ACK-send, and client-observed evidence. Payload-rejection count
  and rate disclose their attempt-level denominator; conflicts and operational
  ingestion failures remain separate.

### 5 - Product verification

- Fixed-seed 500-event `reconnect_burst` demonstration completed with one
  connected browser overlay and 490 DOM events.
- Final measured evidence: 500 generated, 526 delivery attempts, 516 valid
  deliveries, 490 unique/server-ACK-send/processed/dispatched/rendered, 26
  duplicates, 10 intentionally invalid payload deliveries, 0 conflicts, 0
  unrendered, a 1.90% payload-rejection rate, and verdict `pass`.
- The reconnect target has one accepted attempt on the old transport and one
  duplicate attempt on the distinct new transport in timestamp order. All 5
  planned 25 ms delay injections have stored measurements meeting the plan.
- Stored run duration 29.685 seconds; reconnect 13.314 ms; persist-to-render
  p50/p95/p99 28.930/39.398/50.043 ms from 490 samples.
- API and dashboard startup were exercised locally. Corrected dashboard and
  overlay screenshots are included under `docs/images/`.
- Docker Compose was rebuilt from a clean named volume. API and dashboard were
  healthy, a six-event containerized run rendered six DOM elements and passed,
  and an API-container restart retained six processing effects and six render
  acknowledgments. The host dashboard used port 8502 because Windows reserves
  port 8501; the container still used 8501.

### 6 - Review and stabilization

- Reliability architecture, skeptical correctness, test strategy, and
  simplicity reviews completed.
- Serious findings resolved: post-ACK recovery loss window, cross-run delivery,
  transport-unbound reconnect verdicts, zero-duration delay false positives,
  UTC timestamp materialization, retry socket leaks, processing failure
  evidence, and stale lifecycle chart ordering.
- The final Playwright path deliberately closes the overlay socket mid-run,
  observes reconnect, and proves later rendering without duplicate DOM effects.
- A later independent review found two proof-integrity P1 gaps in ACK and delay
  verdict evidence; checkpoint 7 resolves them and adds explicit false-positive
  regression tests.
- A clean container build exposed DuckDB's runtime need for `pytz`; the direct
  dependency was restored and the full Compose proof passed afterward.
- Remaining tradeoffs are documented in README under Honest limitations.

### 7 - Independent review remediation

- PASS now requires stored ACK-send evidence for every expected unique event;
  a client completion counter cannot substitute for missing server evidence.
- Delay proof uses a server-stamped pre-delay marker and first matching delivery
  receipt on the same connection. Submitted markers are locked after run
  completion, so backdated or late claims cannot fabricate a PASS.
- Unexpected ingestion exceptions are best-effort audited as operational
  failures, close the transport with 1011, and enter the existing bounded retry
  path instead of receiving an unpersisted terminal NACK.
- Reconnect proof requires reply-send timestamps on the old accepted attempt and
  new duplicate attempt. The 500-event test reconciles exactly 490 server
  acceptances, 489 client-observed accepted replies, 26 duplicates, 10 payload
  rejections, and 526 attempts with no lost unique event or visible effect.
- Analytics now compares the complete configured canonical order, separates
  payload rejections, conflicts, and operational failures, and labels canonical
  processing/render ratios as completion metrics.
- Playwright deliberately changes replay session after reconnect to exercise
  the browser's in-document deduplication branch against real API replay.
- Compose CI now runs a deterministic lifecycle inside the built API image and
  verifies dashboard-to-API service networking.

### 8 - Finalization review remediation

- A final skeptical review reproduced a false reconnect PASS when submitted
  connection JSON impersonated server provenance, and when an old-transport
  disconnect was stored only after retry recovery.
- The public evidence endpoint now accepts only simulator fault markers,
  overwrites their observer provenance, and cannot create server socket
  connect/disconnect rows. Reconnect proof requires internal server timestamps
  in old disconnect -> new connect -> reconnect -> retry/recovery order.
- A recovered event with a stored failed processing attempt can no longer PASS;
  processing failure evidence remains visible even after lifecycle completion.
- False-positive regressions cover forged server provenance, a late server
  disconnect, and failure -> recovery -> render evidence.

## Final verification (2026-07-14)

- `.venv\Scripts\python.exe -m pip install -e ".[dev]"` - passed.
- `.venv\Scripts\python.exe -m pytest tests\unit -q` - 21 passed in 1.55s.
- `.venv\Scripts\python.exe -m pytest tests\integration -q` - 21 passed in
  5.59s.
- `.venv\Scripts\python.exe -m pytest tests\integration\test_websockets.py -q`
  - 5 passed in 2.14s.
- `.venv\Scripts\python.exe -m pytest tests\dashboard -q` - 3 passed in 9.48s.
- `.venv\Scripts\python.exe -m pytest -q` - 45 passed, 2 deselected in 15.14s.
- `.venv\Scripts\python.exe -m pytest tests\e2e\test_overlay_vertical.py -m e2e -q`
  - 1 passed in 6.04s.
- `.venv\Scripts\python.exe -m pytest tests\scenarios\test_reconnect_burst.py -m scenario -q`
  - 1 passed in 17.66s.
- `.venv\Scripts\python.exe -m ruff check .` - passed.
- `.venv\Scripts\python.exe -m ruff format --check .` - 18 files formatted.
- `.venv\Scripts\python.exe -m mypy src` - passed for 8 source files.
- `node --check src/streamlab/static/overlay.js` - passed.
- `.venv\Scripts\python.exe -m pip check` - no broken requirements.
- `docker compose config --quiet` - passed.
- `$env:STREAMLAB_DASHBOARD_PORT='8502'; docker compose up --build --wait` -
  isolated API and dashboard healthy after both the clean build and final-source
  rebuild.
- Containerized six-event CLI + browser replay - 6 generated, 6 processed, 6
  rendered, verdict `pass`.
- `docker compose restart api` - persisted run remained `pass` with exactly 6
  successful processing effects and 6 rendered events.
- The pre-remediation ignored artifact under
  `artifacts/current-proof-fixed-20260714/` predates server-stamped pre-delay
  markers and is stale under the checkpoint 7 verifier. It is not current proof;
  the fresh isolated run documented below replaces its evidence claims.
- Final Compose logs contained no traceback or error; services were stopped
  with `docker compose down`, retaining the synthetic evidence volume.
- `git diff --check` - passed; Git emitted only Windows LF-to-CRLF notices for
  the two tracked Markdown files.

## Post-review verification (2026-07-14)

- `.venv\Scripts\python.exe -m pytest tests\integration\test_analytics.py tests\integration\test_websockets.py tests\unit\test_simulator.py tests\dashboard\test_dashboard.py -q`
  - 36 passed in 19.49s.
- `.venv\Scripts\python.exe -m pytest -q` - 50 passed, 2 deselected in
  14.91s.
- `.venv\Scripts\python.exe -m pytest -m e2e -q` - 1 passed, 51 deselected in
  6.79s.
- `.venv\Scripts\python.exe -m pytest -m scenario -q` - 1 passed, 51
  deselected in 17.63s.
- Final fixed-seed evidence: 500 generated, 526 attempts, 490 accepted, 26
  duplicate, 10 payload-rejected, 490 unique/server-ACK-send/processed/
  dispatched/rendered, 0 operational ingestion failures, and 0 identity
  conflicts. The client observed 489 accepted replies plus 26 duplicates and
  reconciled 490 unique IDs.
- The reconnect target has exactly one accepted reply-send on the old transport
  and one duplicate reply-send on a different transport. Exact canonical order
  matched; five server-timed delays measured 35.284-42.634 ms against 25 ms.
- Persist-to-render p50/p95/p99 measured 25.907/36.022/40.314 ms from 490
  samples; reconnect measured 19.359 ms. These are local observations, not
  capacity claims.
- `.venv\Scripts\python.exe -m ruff check .` - passed.
- `.venv\Scripts\python.exe -m ruff format --check .` - 18 files already
  formatted.
- `.venv\Scripts\python.exe -m mypy src` - passed for 8 source files.
- `.venv\Scripts\python.exe -m pip check` - no broken requirements.
- `node --check src\streamlab\static\overlay.js` - passed.
- `docker compose config --quiet` - passed.
- Isolated `docker compose up --build --wait` plus the exact CI container proof
  passed: 12 unique/ACK-sent/processed events, 13 attempts, 11 client-observed
  accepted replies, 1 duplicate retry, dashboard-to-API connectivity, and an
  honestly `incomplete` verdict without browser render evidence. The isolated
  containers, network, and volume were removed.
- `git diff --check` - passed with only LF-to-CRLF notices for the two tracked
  Markdown files.

## Finalization verification (2026-07-14)

- Directly affected analytics, WebSocket, and simulator tests - 35 passed in
  6.53s.
- `.venv\Scripts\python.exe -m pytest -q` - 52 passed, 2 deselected in
  14.88s.
- `.venv\Scripts\python.exe -m pytest -m e2e -q` - 1 passed, 53 deselected in
  6.85s.
- `.venv\Scripts\python.exe -m pytest -m scenario -q` - 1 passed, 53
  deselected in 18.25s.
- Fresh fixed-seed evidence: 500 generated, 526 attempts, 490 accepted, 26
  duplicate, 10 payload-rejected, 490 unique/server-ACK-send/processed/
  dispatched/rendered, 0 failed processing attempts, 0 operational ingestion
  failures, and 0 identity conflicts. The scenario asserted 489 client-observed
  accepted replies, 26 duplicate replies, and one retry while reconciling all
  490 unique IDs.
- The reconnect target had one accepted reply-send on the old transport and one
  duplicate reply-send on a distinct new transport. Internal server timestamps
  were correctly ordered; no submitted marker claimed server provenance. Exact
  canonical order matched, and five 25 ms delays measured 38.106-46.435 ms.
- Persist-to-render p50/p95/p99 measured 29.910/38.328/41.576 ms from 490
  samples; reconnect measured 12.628 ms and stored run duration was 13.057 s.
  These remain local observations, not capacity claims.
- Isolated Compose build and runtime proof passed: API and dashboard healthy, 12
  unique/ACK-sent/processed events from 13 attempts, 11 client-observed accepted
  replies plus one duplicate retry, ordered transport evidence, dashboard-to-API
  networking, and an honestly `incomplete` verdict without browser evidence.
  The scoped containers, network, volume, and image tags were removed.
- `.venv\Scripts\python.exe -m ruff check .` - passed.
- `.venv\Scripts\python.exe -m ruff format --check .` - 18 files already
  formatted.
- `.venv\Scripts\python.exe -m mypy src` - passed for 8 source files.
- `.venv\Scripts\python.exe -m pip check` - no broken requirements.
- `node --check src\streamlab\static\overlay.js` - passed.
- `docker compose config --quiet` - passed.
- `git diff --check` - passed with only LF-to-CRLF notices for the two tracked
  Markdown files.

## Known limitations

- One FastAPI process owns one serialized DuckDB writer; this is not a
  horizontal-scaling design.
- DuckDB work is synchronous and can briefly occupy the event loop.
- Browser acknowledgment proves DOM insertion, not OBS or physical display
  output.
- Forced disconnects are client-controlled, not packet-level network faults.
- Dependency ranges are bounded but there is no lock file yet.

## Blockers

- None.

## Repository state

- v0.1 verification is complete; cleanup work starts from a dedicated follow-up
  branch after the requested local finalization commit.
- No remote was configured and nothing was pushed, published, or deployed.
