# Reliability model

## Claim

The source protocol is at least once: an unacknowledged event may be delivered
again. Idempotency keys that repeated work by `event_id`. The lab does not claim
universal exactly-once delivery.

## Lifecycle

```text
generated
  -> delivery attempted
  -> validated or rejected
  -> persisted
  -> source reply send recorded
  -> processed
  -> dispatched to overlay
  -> inserted in DOM
  -> render acknowledgment persisted
```

Rejected attempts stop before a canonical event. Later stages are therefore
not expected to equal generated or delivered counts in fault scenarios.

## Invariants

1. A registered valid unique event and its attempt commit atomically before an
   accepted ACK is sent.
2. Every observed source message gets a distinct attempt row unless the
   database transaction itself is unavailable.
3. `events.event_id` is unique.
4. A duplicate creates another attempt row but no second canonical event.
5. One successful processing attempt exists per canonical event.
6. One browser DOM element exists per event in a document.
7. One render acknowledgment exists per event and overlay session.
8. Render evidence requires a matching processed event and successful dispatch.
9. Every source socket is bound to one run; untrusted payload identifiers cannot
   redirect rejected-attempt evidence to another run.
10. Client-submitted fault markers cannot claim server provenance or create
    server-observed socket connect/disconnect evidence.
11. Run completion closes source ingestion atomically; no later delivery
    outcome can change finalized event or attempt counts.
12. A render acknowledgment is validated only after the matching successful
    dispatch has committed for that overlay session.

## Failure boundaries

### Commit fails before ACK

The transaction rolls back both the attempt and event. No accepted reply is
constructed. The commit-failure integration test asserts both tables remain
empty.

### Connection closes after send but before client-observed ACK

The forced-reconnect simulator sends the target without reading its reply,
waits until DuckDB shows an accepted attempt and `response_sent_at` on the old
connection, closes that transport, reconnects with a new connection ID, and
resends the same envelope. The client observes 489 accepted replies even though
the server accepted 490 unique events; the missing accepted reply is replaced
by one observed duplicate reply. All 490 unique IDs still reconcile, and one
canonical effect is produced for each. A PASS additionally requires the
server-observed old disconnect before the server-observed new connection, and
the new connection before the retry/recovery path.

### Unexpected ingestion exception

The API best-effort stores an `INTERNAL_ERROR` attempt and closes the WebSocket
with code 1011 without sending an unpersisted terminal NACK. The simulator's
bounded policy retries on a new transport. Operational ingestion failures are
reported separately from intentionally invalid payload deliveries.

### ACK frame succeeds but reply evidence does not commit

The canonical event was already committed before the frame was sent. Startup
recovery processes every persisted, unprocessed event and does not use the
later `acknowledged_at` timestamp as an authorization boundary. Reply evidence
therefore remains an honest observation rather than a prerequisite for work.

### Reply succeeds but processing is interrupted

The server never sends a contradictory second NACK. The event remains durably
unprocessed. A duplicate delivery or application restart can process it once.

### No overlay is connected

Processing still completes. When a browser connects, the service replays
processed effects lacking an acknowledgment for that session. Analytics call
the run `incomplete` until browser evidence exists.

### Source sends after run completion

The API refuses new ingestion sockets for completed runs. If a source socket
was already open, every attempt-writing transaction rechecks the run status and
closes that socket without storing a late valid, invalid, duplicate, or
conflicting attempt. Events persisted before completion may still finish their
processing and render lifecycle.

### Overlay reconnects

The in-document `event_id` set suppresses duplicate DOM insertion during a
socket reconnect. A full page load creates a new run-scoped session and receives
its own replay, which makes the visible page complete rather than blank.
Effect send and successful-dispatch persistence share a per-session ordering
gate with render-ACK validation, preventing a fast browser response from being
rejected merely because dispatch evidence had not committed yet.

## Sequence evidence

Each canonical event stores arrival index, whether it arrived below the prior
maximum source sequence, and the gap visible at arrival. A scenario passes only
when the complete canonical arrival order exactly matches configured delivery
order after intentionally invalid sequences are removed; one out-of-order flag
is not sufficient proof. Delayed scenarios submit a marker before each hold.
The API replaces its caller timestamp with server receipt time and derives the
lower-bound duration to the first matching attempt on the same connection. A
delay satisfies the scenario only when that duration meets the configured
value. A later delayed event can close an earlier arrival gap, so the dashboard
labels gaps as arrival-time evidence rather than final event loss.

## Metrics

Counts and percentiles come only from DuckDB evidence. End-to-end runtime
latency is `persisted_at` to the API's stored browser render acknowledgment.
Deterministic logical `occurred_at` values are intentionally excluded. Every
percentile view includes its stored sample count.

Payload-rejection count includes intentionally invalid delivery attempts and
uses all stored delivery attempts as its rate denominator. Event-identity
conflicts and operational ingestion failures are reported separately. The lab
does not call the payload-rejection rate a generic delivery-failure rate, and
v0.1 does not calculate a transport delivery-failure rate.

Processing completion is processed canonical events divided by unique canonical
events; render completion is rendered events divided by processed events.
Processing-attempt success is separately available from successful and total
stored processing attempts. A PASS requires zero stored processing failures and
the expected number of stored server reply-send timestamps, not only eventual
recovery or the simulator's completion counter.

Reconnect duration is the interval from the simulator's forced-disconnect
evidence to its next reconnect evidence. Server-observed socket connect and
disconnect rows corroborate that a transport transition occurred. A reconnect
scenario passes only when the target's accepted attempt is on the old transport
with a stored reply-send timestamp before forced disconnect, and its duplicate
retry is on the new transport with a stored reply-send timestamp after reconnect
and before recovery completion. Internally written server socket timestamps must
also order the old disconnect before the new connection and reconnect marker.
