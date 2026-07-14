# Scenarios

All scenarios persist their complete manifest and injection plan before source
delivery. The same scenario, seed, count, rate, and fixed run ID produce the
same canonical envelopes and plan.

| Scenario | Injection behavior | Expected evidence |
| --- | --- | --- |
| `happy_path` | Monotonic valid delivery at the configured rate | Every generated event reaches render ACK with a connected overlay |
| `duplicate_delivery` | Deterministically samples about 20% of sequences for a second identical send | Extra duplicate attempts; one canonical event/effect/DOM element |
| `invalid_payloads` | Deterministically samples at least four events and cycles strict invalid mutations | Structured NACK categories; no canonical row for those attempts |
| `delayed_out_of_order` | Marks and reverses a bounded delivery window, then applies deterministic 25 ms holds | Server-timed holds, exact configured canonical order, arrival gaps, and lower-sequence later arrivals without identity changes |
| `forced_reconnect` | Sends the midpoint event, waits for server ACK-send evidence without reading the reply, closes, reconnects, and retries | Target accepted/reply-sent on the old transport, duplicated/reply-sent on the new transport, and one effect |
| `reconnect_burst` | Combines invalids, duplicates, deterministic holds, two reorder windows, a 40% reconnect point, and a 70% burst point | Reconciled mixed-fault run and measured recovery |

Invalid mutations cycle through:

1. missing envelope field;
2. unsupported event type;
3. naive timestamp;
4. unsupported schema version;
5. payload-field violation;
6. malformed JSON.

## Fixed portfolio demonstration

```bash
python -m streamlab.simulator --scenario reconnect_burst --seed 20250314 --count 500 --rate 1000 --burst-rate 5000 --overlay-wait 120
```

The persisted plan is:

- 500 generated manifest events;
- 10 invalid sequences (2% with a minimum of five);
- 25 planned duplicate sequences (5%);
- 5 delayed sequences (1%) with a configured 25 ms hold;
- forced source disconnect at sequence 200;
- burst period beginning at sequence 350;
- 1,000 events/s configured normal pacing;
- 5,000 events/s configured burst pacing;
- bounded reversed delivery windows for out-of-order evidence.

The disconnect event is excluded from invalid injection so it can participate
in ACK-based retry. Duplicate sequences are also excluded from invalid
sequences. The retry can add one more duplicate attempt when the server received
and committed the deliberately unread first send.

For the 500-event fixed-seed run, the server accepts and persists 490 unique
events. The simulator observes 489 accepted replies because it deliberately
does not read the reconnect target's old-transport reply. Retrying that target
produces the 26th duplicate reply (25 planned plus one reconnect retry). The
client still reconciles 490 unique IDs, and no unique event or visible effect is
lost.

Configured rates control simulator pacing; actual local throughput also includes
network, validation, transaction, browser, and acknowledgment work. The project
does not treat configured rates as achieved throughput.

A final `pass` requires the generic stored lifecycle and server ACK-send count
to reconcile, the client ACKed-ID count to equal valid unique events, and stored
evidence for every fault the selected scenario planned. Delay duration, exact
configured delivery order, and reconnect-attempt transport ordering are
verified explicitly. A completed manifest alone cannot produce a pass.
