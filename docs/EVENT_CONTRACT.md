# Event contract

## Normalized envelope

Every canonical event is strict and versioned:

```json
{
  "schema_version": "1.0",
  "event_id": "9d31fba7-1a0f-4bd3-a4c1-cb88b6b462f4",
  "run_id": "b6cbe3d2-9b62-4df3-9f3a-ddfbb13c8e79",
  "source": "simulator",
  "event_type": "comment",
  "source_sequence": 1,
  "occurred_at": "2025-01-01T00:00:00Z",
  "actor_id": "synthetic-actor-001",
  "payload": {"text": "Synthetic comment 0001"}
}
```

Unknown fields are rejected. `schema_version`, `source`, and every other
envelope field are required. Timestamps must include an offset and normalize to
UTC. Actor IDs match `synthetic-actor-NNN`; real account names are never used.

## Typed payloads

| `event_type` | Required payload |
| --- | --- |
| `comment` | `text` from 1 to 180 characters |
| `follow` | `followed: true` |
| `gift` | `gift_name`, integer `quantity` from 1 to 100 |
| `like` | integer `count` from 1 to 10,000 |
| `subscription` | `tier_1`, `tier_2`, or `tier_3`; months from 1 to 120 |
| `command` | one of `blur`, `save`, `clear`, `shield`; up to eight short arguments |

The service validates the payload selected by `event_type`; a payload valid for
another type is not accepted.

## Identity and canonical hashing

Canonical JSON sorts keys and removes insignificant whitespace before SHA-256
hashing. A repeated `event_id` with the same hash is a duplicate attempt. The
same ID with different semantic content is an `EVENT_ID_CONFLICT` and never
mutates the original event.

The generated manifest also stores canonical JSON. A delivered valid envelope
must match its manifest row. Scenario tests pass a fixed `run_id` to prove
byte-identical output for the same seed; ordinary CLI runs use a fresh run ID
while retaining the same payload and injection semantics.

## Delivery replies

Every ingestion WebSocket requires `run_id` in its query string. The server
attributes all attempt evidence to that registered bound run and rejects a
different payload `run_id` as `RUN_ID_MISMATCH` before full schema validation.
Once the run is completed, a new ingestion socket is closed with code `4409`.
An already-open source socket receives the same close if it submits more work;
the transactional persistence boundary creates no late attempt or event row.

Accepted first delivery:

```json
{
  "kind": "ack",
  "attempt_id": "...",
  "status": "accepted",
  "event_id": "...",
  "run_id": "...",
  "persisted": true,
  "duplicate": false,
  "error": null
}
```

Duplicate delivery uses `kind: ack`, `status: duplicate`, and
`duplicate: true`. An expected validation or identity rejection uses
`kind: nack`, `persisted: false`, and one structured category:

- `MALFORMED_JSON`
- `MESSAGE_TOO_LARGE`
- `MISSING_FIELD`
- `UNSUPPORTED_SCHEMA`
- `UNSUPPORTED_EVENT_TYPE`
- `INVALID_TIMESTAMP`
- `INVALID_PAYLOAD`
- `UNKNOWN_RUN`
- `UNREGISTERED_EVENT`
- `RUN_ID_MISMATCH`
- `EVENT_ID_CONFLICT`

Every stored attempt has its own `attempt_id`. `response_sent_at` is set only
after the WebSocket reply send succeeds.

An unexpected ingestion exception is different from an invalid payload. When
DuckDB remains writable, the API stores an `INTERNAL_ERROR` attempt with no
`response_sent_at`, closes the socket with code 1011, and sends no fabricated
terminal NACK. The simulator then uses its existing bounded reconnect-and-retry
policy.

## Overlay protocol

The server sends:

```json
{"kind": "effect", "event": {"event_id": "...", "effect_id": "..."}}
```

After DOM insertion, the browser sends:

```json
{
  "kind": "render_ack",
  "event_id": "...",
  "rendered_at": "2026-07-13T22:16:50.123Z"
}
```

The API accepts this only when the event belongs to the session's run, was
processed, and has a successful dispatch to that session. Unknown, cross-run,
or undispatched IDs receive `render_nack` and create no render evidence.
