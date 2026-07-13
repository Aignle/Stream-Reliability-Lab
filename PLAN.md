# Stream Reliability Lab — v0.1 Product Plan

## Product statement

Stream Reliability Lab is a local, reproducible reliability-testing and analytics platform for real-time event-driven applications.

It simulates creator-platform-style events, deliberately injects delivery and connection failures, traces each event through the system, verifies what appears in a browser overlay, and reports latency, duplicates, data-quality failures, event loss, and recovery behavior.

## Portfolio story

A reviewer should be able to understand that this project demonstrates:

- Python application engineering;
- WebSocket and API testing;
- event-driven reliability concepts;
- idempotency, retries, acknowledgments, and recovery;
- SQL and data-quality analysis;
- browser automation with Playwright;
- Docker and continuous integration;
- honest measurement and technical documentation.

## Primary v0.1 user experience

From a clean checkout, a reviewer can follow the README to:

1. install or start the project;
2. open a browser overlay;
3. run a deterministic event scenario;
4. watch simulated events appear;
5. open a dashboard for the completed run;
6. inspect generated, delivered, accepted, processed, and rendered counts;
7. inspect duplicate suppression, invalid events, latency percentiles, and reconnect behavior;
8. run automated tests that prove the golden path and important failure cases.

## Required event lifecycle

The system must preserve evidence for these stages:

```text
generated
  → delivery attempted
  → validated/rejected
  → accepted and persisted
  → processed
  → dispatched to overlay
  → rendered and acknowledged
```

Not every event reaches every stage. The analytics must make the difference visible.

## Event contract

Use a versioned normalized event with fields equivalent to:

- `schema_version`
- `event_id`
- `run_id`
- `source`
- `event_type`
- `source_sequence`
- `occurred_at`
- `actor_id`
- typed `payload`

Support a small useful set such as:

- comment
- follow
- gift
- like
- subscription
- command

All data is synthetic.

## Minimum components

### 1. Deterministic simulator

- Loads a scenario configuration.
- Generates synthetic events from a seed.
- Sends events over WebSocket.
- Tracks acknowledgments and retries unacknowledged events.
- Can inject duplicate, malformed, delayed, out-of-order, reconnect, and burst behavior.
- Produces a concise run result.

### 2. FastAPI application

- Health endpoint.
- WebSocket ingestion endpoint.
- Schema validation and structured acknowledgments.
- Persistence before successful acknowledgment.
- Idempotent event handling.
- Event processing and overlay dispatch.
- Overlay WebSocket endpoint.
- Render-acknowledgment endpoint or message flow.
- Read-only analytics endpoints for the dashboard and tests.

### 3. DuckDB persistence

The exact schema is an implementation decision, but it must distinguish at least:

- test runs;
- delivery attempts;
- unique canonical events;
- processing attempts/status;
- render acknowledgments;
- connection/recovery events.

Only the application service should own database writes. The dashboard should consume analytics through application endpoints rather than mutate the database.

### 4. Browser overlay

- Simple HTML/CSS/JavaScript served locally.
- Connects to the application over WebSocket.
- Renders testable DOM elements containing `data-event-id`.
- Sends a render acknowledgment only after the effect is inserted successfully.
- Clearly displays connection state.

### 5. Analytics dashboard

Provide useful views for a selected run:

- overview and final run verdict;
- lifecycle funnel/counts;
- event table and individual event trace;
- latency percentiles and time series;
- duplicate and invalid-delivery counts;
- errors by category;
- reconnect/recovery evidence.

### 6. Automated verification

Include:

- unit tests for models, deterministic generation, lifecycle rules, idempotency, and metrics;
- integration tests for WebSocket ingestion, persistence, acknowledgments, and analytics;
- Playwright tests for simulator-to-browser rendering and duplicate suppression;
- static checks and type checking;
- GitHub Actions using the same core commands documented locally.

## Required scenarios

### `happy_path`

Valid events at a steady rate. Every expected unique event should be accepted, processed, and rendered once while the overlay is connected.

### `duplicate_delivery`

Repeat selected `event_id` values. Duplicate attempts must be recorded, while processing and visible effects remain idempotent.

### `invalid_payloads`

Send missing fields, unsupported types, invalid timestamps, and unsupported schema versions. Reject them with structured, testable errors.

### `delayed_out_of_order`

Delay and reorder a bounded subset. Measure out-of-order arrivals and sequence gaps without silently corrupting canonical event identity.

### `forced_reconnect`

Interrupt the source connection. Reconnect, retry unacknowledged events, and record recovery evidence.

### `reconnect_burst`

Combine reconnect/retry with a later event burst. This is the primary portfolio demo scenario.

## Primary demo

Provide one documented command or short sequence that runs approximately 500 synthetic events using a fixed seed and the `reconnect_burst` scenario.

The exact measured result depends on the machine. The demo must produce a run identifier and make its analytics view easy to open.

## Acceptance criteria

The v0.1 goal is complete only when all of the following are true:

1. A clean checkout can be set up using README instructions.
2. The application and dashboard start using documented commands.
3. The simulator can run a fixed-seed scenario successfully.
4. Valid events are persisted before accepted acknowledgments are sent.
5. Every delivery attempt is traceable, including duplicates and invalid payloads.
6. A duplicate delivery cannot create a second canonical event, processing effect, or browser effect.
7. The browser records a render acknowledgment tied to the event and overlay session.
8. A Playwright test proves an event travels from submission to a visible DOM element and stored render acknowledgment.
9. A forced reconnect scenario retries unacknowledged work and records recovery evidence.
10. Analytics clearly distinguish lifecycle stages and calculate latency percentiles from stored timestamps.
11. Unit, integration, browser, lint, formatting, and type checks pass.
12. Docker Compose and GitHub Actions are present and documented.
13. The README contains architecture, quickstart, testing, limitations, and screenshots or generated visual evidence where practical.
14. No real platform credentials or private data are required.
15. No unmeasured performance claim appears in documentation.

## Quality priorities

In priority order:

1. Correct event identity and lifecycle evidence.
2. Reproducible end-to-end demonstration.
3. Tests that prove behavior rather than implementation details.
4. Clear setup and documentation.
5. Useful analytics.
6. Visual polish.
7. Additional features.

## Allowed pragmatic shortcuts

For v0.1, it is acceptable to:

- run the processor inside the FastAPI application;
- use one local DuckDB file;
- use one overlay session in the primary demo;
- use a simple polling or API-refresh approach in Streamlit;
- use synthetic data only;
- document non-production limitations clearly.

Record shortcuts and future improvements in `DECISIONS.md` or the README. Do not hide them behind production-ready language.

## Post-functionality cleanup

Once the primary demo works, perform a dedicated cleanup pass:

- remove dead code and abandoned approaches;
- consolidate duplicated lifecycle/metrics logic;
- reduce unnecessary dependencies;
- improve names and module boundaries where confusion is proven;
- strengthen weak assertions and failure diagnostics;
- make setup commands reproducible;
- reconcile README, diagrams, and actual behavior;
- preserve working behavior while simplifying.
