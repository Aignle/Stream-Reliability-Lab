# Architecture and Product Decisions

## Initial constraints

- Local-first portfolio project using synthetic data only.
- At-least-once delivery with idempotent processing.
- FastAPI, Pydantic, DuckDB, Streamlit, Playwright, Docker Compose, and GitHub Actions.
- No real platform integration, broker, cloud infrastructure, authentication,
  React application, or AI feature in v0.1.

### 2026-07-13 - One application-owned DuckDB database

**Decision:** FastAPI is the only component that opens DuckDB. Writes are
serialized inside one repository lock. Streamlit uses analytics APIs.

**Reason:** This is the smallest boundary that prevents competing writers and
keeps every displayed metric tied to application evidence.

**Tradeoff:** It is not a horizontal-scaling design and synchronous database
work can briefly occupy the event loop.

### 2026-07-13 - Explicit lifecycle and terminal rejection

**Decision:** Store generated, delivery, persistence, reply, processing,
dispatch, render, and render-acknowledgment evidence separately. A rejection is
terminal attempt evidence and creates no event.

**Reason:** Counts can diverge honestly and interrupted stages remain visible.

**Tradeoff:** This requires several small evidence tables instead of one mutable
status field.

### 2026-07-13 - Append-only attempts and constrained canonical effects

**Decision:** Every observed raw message has a distinct attempt. `event_id` is
unique for canonical events. Equal content is a duplicate; changed content with
the same ID is a conflict. Processing is keyed by the event ID.

**Reason:** Retries remain auditable without duplicating business-visible work.

**Tradeoff:** Canonical hashing and conflict handling add code.

### 2026-07-13 - Persistence is the processing boundary

**Decision:** Commit a valid event before sending ACK, store reply evidence only
after the socket send, then process. Startup recovery processes every persisted
but unprocessed canonical event, whether or not post-send ACK evidence committed.

**Reason:** The server cannot know whether a successfully written ACK frame was
observed by the source. Gating recovery on later ACK evidence creates a crash
window that can strand durable accepted work.

**Tradeoff:** An event may be processed even when the source did not observe its
ACK. A retry is still safe because processing is idempotent by `event_id`.

### 2026-07-13 - Persisted replay with an in-process dispatcher

**Decision:** Processing and live overlay dispatch run inside FastAPI. Processed
effects without a per-session render acknowledgment replay when that session
connects.

**Reason:** Restart recovery and browser delivery need no broker for v0.1.

**Tradeoff:** Live sockets remain in memory; durable evidence and replay, not
connection state, provide recovery.

### 2026-07-13 - Browser report and API acknowledgment are distinct

**Decision:** The browser inserts one `data-event-id` element, reports
`rendered_at`, and waits for the API to persist that report. The API verifies
event, run, processing, session, and successful dispatch before accepting it.

**Reason:** Browser-originated evidence plus Playwright DOM proof is stronger
than a server-side send timestamp.

**Tradeoff:** This proves DOM insertion, not physical capture output.

### 2026-07-13 - Page-load sessions with in-document deduplication

**Decision:** Each document load creates a new overlay session. Reconnects reuse
that in-memory session and set of rendered IDs; a reload creates a fresh session
and receives a full replay.

**Reason:** Socket reconnects cannot duplicate the DOM, while a browser reload
does not produce an empty page because an old session already acknowledged the
run.

**Tradeoff:** Multiple open pages each receive and acknowledge their own effect
copy, as expected for separate browser sessions.

### 2026-07-13 - Deterministic plans and honest metrics

**Decision:** Persist the six exact scenario names, seed, anomaly sequences,
delivery order, reconnect point, burst point, and configured rates. Runtime
percentiles use stored persist-to-render timestamps and disclose sample count.

**Reason:** Repeatable semantics and explicit denominators make the demo
defensible.

**Tradeoff:** Measurements describe one local run and are not scalability
benchmarks.

### 2026-07-13 - Server-owned run completion evidence

**Decision:** The manifest count is never overwritten by client completion.
Client ACK counts cannot exceed it, repeated identical completion is idempotent,
and conflicting completion counters are rejected.

**Reason:** A simulator cannot fabricate a completed 500-event run over an empty
manifest.

**Tradeoff:** The client still reports its observed ACK and retry counters; the
final verdict reconciles those with server evidence instead of trusting them.

### 2026-07-13 - Bounded dependencies without a second UI framework

**Decision:** Use bounded dependency ranges, `httpx` for runtime HTTP and the
Starlette-required `httpx2` TestClient transport for tests. Keep the overlay
vanilla HTML/CSS/JavaScript.

**Reason:** The full proof needs no broker, React bundle, or additional
database.

**Tradeoff:** A future version should add a maintained lock file if
byte-for-byte dependency reproduction becomes necessary.

### 2026-07-14 - Ingestion sockets are bound to one run

**Decision:** Every ingestion WebSocket requires a registered `run_id` in its
query string. All connection and attempt evidence is attributed to that bound
run, and a valid payload naming another run is rejected before canonical
validation or persistence.

**Reason:** A malformed or hostile payload must not contaminate another run's
audit trail or cause the client to correlate a rejected attempt to the wrong
run.

**Tradeoff:** Producers must open a separate socket when they switch runs.

### 2026-07-14 - Strict integral event fields

**Decision:** Quantities, counts, subscription months, source sequences, and
scenario delay values reject numeric strings instead of relying on Pydantic
coercion.

**Reason:** The event contract is typed evidence. Accepting `"5"` where the
contract says integer would hide producer defects and weaken invalid-payload
tests.

**Tradeoff:** Producers must normalize values before sending them.

### 2026-07-14 - Corroborated fault evidence

**Decision:** Delayed scenarios store planned sequences plus configured and
measured simulator delay evidence, and only measured holds meeting the plan
count. Reconnect scenarios require the deliberately unacknowledged target to
have an accepted attempt on the old transport before disconnect and a duplicate
attempt on the distinct new transport after reconnect and before recovery
completion.

**Reason:** A reordered list or a socket reconnect alone does not prove that a
delay occurred or that the intended at-least-once retry crossed the reconnect.

**Tradeoff:** Scenario verdict queries and tests carry a small amount of extra
correlation logic.

### 2026-07-14 - Server-owned proof timestamps and completion lock

**Decision:** Timestamp simulator-submitted connection evidence at API receipt,
reject new submitted evidence after run completion, and measure a planned delay
from its pre-delay marker to the first matching delivery receipt on the same
connection.

**Reason:** Caller-authored durations and timestamps could otherwise fabricate
a delay after delivery or after the run had already completed.

**Tradeoff:** The duration is a lower-bound end-to-end hold measurement and
includes local HTTP/WebSocket overhead rather than only `asyncio.sleep` time.

### 2026-07-14 - Separate evidence viewpoints and metric categories

**Decision:** Require stored server ACK-send evidence for PASS, retain the
simulator's observed reply counts separately, and report payload rejections,
identity conflicts, and operational ingestion failures as distinct categories.
Canonical processing and render ratios are labeled as completion metrics.

**Reason:** In the forced reconnect, 490 unique events are accepted server-side
while only 489 accepted replies are observed client-side; the missing reply is
reconciled by a duplicate retry. Combining that distinction or intentionally
invalid payloads into a generic delivery-failure rate would misstate the stored
evidence.

**Tradeoff:** The overview contract has more explicit metric names and callers
must choose the server or client viewpoint they intend to present.

### 2026-07-14 - Server-owned transport provenance and failure-aware verdicts

**Decision:** Accept only simulator fault markers through the public connection
evidence endpoint, overwrite their observer as `simulator`, and keep
server-observed socket connect/disconnect evidence internal to FastAPI. Reconnect
proof requires those server rows in transport order. Any stored processing
failure prevents PASS even if later recovery completes the event.

**Reason:** Caller JSON must not fabricate server corroboration, a disconnect
recorded after retry recovery does not prove a transport transition, and eventual
completion must not hide an unplanned processing failure.

**Tradeoff:** The public endpoint is intentionally not a generic connection-log
API, and a recovered processing fault remains a failed lab run rather than a
PASS-with-warning result.

### 2026-07-14 - Run completion closes source ingestion

**Decision:** Recheck run status inside every attempt-writing transaction and
reject new or already-open ingestion sockets once a run is completed. Events
persisted while the run was open may still finish processing, dispatch, and
render acknowledgment.

**Reason:** A final verdict must not become PASS from late source work or flip
back to FAIL after additional post-completion attempts. The transactional check
orders a racing completion and delivery without relying on a stale route-level
status query.

**Tradeoff:** A source that keeps its socket open beyond completion receives a
4409 close and must create a new run rather than append more evidence.

### 2026-07-14 - Dispatch persistence orders render acknowledgments

**Decision:** Hold the overlay session's send lock through the successful
dispatch transaction, and validate render acknowledgments through the same
ordering gate.

**Reason:** A browser can receive and acknowledge an effect while the outbound
send coroutine is still yielding. Without a shared gate, valid DOM evidence can
be rejected just before its successful dispatch row commits.

**Tradeoff:** Sends and render acknowledgments are serialized per browser
session. Different sessions remain independent, which is sufficient for this
single-process local lab.

### 2026-07-14 - Keep bounded dependencies without adding a lock manager

**Decision:** Retain the current direct dependencies and bounded ranges, and do
not add a lockfile during cleanup.

**Reason:** The repository uses pip 24.2 and setuptools, and that installed pip
has no native lock command. Adding pip-tools, uv, or another manager solely to
generate a lock would expand the maintenance surface. `httpx2` is imported by
the installed Starlette TestClient, `httpx` is used at runtime, and `pytz` is a
verified DuckDB requirement in the clean container build.

**Tradeoff:** CI and Docker prove fresh resolution within the supported bounds,
but transitive versions are not byte-for-byte reproducible across time.
