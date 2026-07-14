# Architecture

## Components and ownership

`streamlab.simulator` creates a complete scenario manifest before delivery. It
registers that manifest over HTTP, sends raw attempts over WebSocket, waits for
correlated ACK/NACK replies, and retries work for which it did not observe an
ACK.

`streamlab.main` is the composition root. It owns FastAPI routes, the in-memory
overlay connection hub, and application startup recovery. Route handlers call
the ingestion and analytics services.

`streamlab.service` categorizes malformed or invalid messages, verifies valid
events against the registered manifest, and sequences reply evidence and
idempotent processing.

`streamlab.repository` is the only DuckDB boundary. One connection and one
reentrant lock serialize transactions. No network await occurs while the lock
is held.

`streamlab.analytics` derives every dashboard view from stored tables. Stored
simulator-observed completion counters are labeled as client evidence and are
reconciled with server-owned manifests, attempts, connection rows, and browser
acknowledgments rather than trusted as standalone metrics.

Simulator-submitted connection markers receive a server timestamp at the HTTP
boundary, are forced to simulator provenance, and cannot be added after run
completion. The endpoint accepts only delay, forced-disconnect, reconnect, and
recovery markers; server-observed socket connect/disconnect rows are written
only by the WebSocket handler. Delay analytics therefore measure stored server
receipt time to the first matching delivery receipt instead of trusting a
client-reported duration.

`streamlab.dashboard` calls the read-only FastAPI analytics endpoints. It never
imports DuckDB or the repository.

The vanilla overlay is served by FastAPI. It keeps an in-document set of
rendered event IDs, inserts `data-event-id` before acknowledging a render, and
reconnects with bounded backoff.

## Evidence schema

| Table | Purpose |
| --- | --- |
| `runs` | Scenario config, seed, server-owned manifest count, completion counters |
| `generated_events` | Complete deterministic manifest and canonical envelope |
| `delivery_attempts` | Every observed raw attempt, outcome, category, and reply time |
| `events` | One canonical row per accepted `event_id` and first lifecycle timestamps |
| `processing_attempts` | At most one successful effect record per canonical event |
| `overlay_sessions` | Run-scoped browser connection and reconnect state |
| `overlay_dispatches` | Every attempted send to an overlay session |
| `render_acknowledgments` | One browser report per event and session |
| `connection_events` | Simulator- and server-observed connect, disconnect, and recovery evidence |

The canonical event row is intentionally compact. Append-only attempt and
dispatch tables preserve repeated work rather than overwriting it.

## Write path

1. The simulator registers the run and full manifest in one transaction.
2. A WebSocket bound to one registered run sends a text message to
   `IngestService`.
3. The service rejects cross-run identifiers before schema validation and
   audits malformed or invalid input against the socket's bound run.
4. For a valid manifest event, the repository inserts the delivery attempt and
   canonical event in one transaction. The transaction commits before the ACK
   object is sent.
5. After the socket send succeeds, the attempt reply time and event ACK time
   are stored. Processing then creates one successful effect. If ingestion
   instead raises unexpectedly, the API best-effort audits `INTERNAL_ERROR`,
   closes the socket with 1011, and leaves retry to the bounded source policy;
   it does not fabricate an unpersisted terminal NACK.
6. The overlay hub sends the effect to matching live sessions and stores each
   dispatch attempt.
7. The browser inserts the DOM element, sends `render_ack`, and waits for the
   API's persisted `render_acknowledged` response.

## Recovery and replay

At application startup, every persisted event without `processed_at` is
processed once. Post-send ACK evidence remains an observation, not a gate. If
the source retries because it did not observe the ACK, `event_id` idempotency
still prevents a second effect.

When an overlay connects, the repository selects processed events that lack a
render acknowledgment for that specific session. One session's render evidence
does not suppress replay to a different session.

## Concurrency boundary

DuckDB operations are synchronous and serialized. This makes transaction
invariants straightforward but can briefly block the single API event loop.
The v0.1 acceptance run covers 500 generated events; no broader concurrency or
throughput claim is made. A later design could add a dedicated DB executor or
durable outbox worker without changing the evidence contracts.
