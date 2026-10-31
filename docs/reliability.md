---
id: reliability
title: Reliability
sidebar_position: 4
---

How Eventarium decides what happens to a delivery, and what the words it
reports mean.

## The eleven statuses

Every transition of a delivery is one append-only row in ClickHouse
(`delivery_status`); a delivery's *current* status is the newest row for its
ID, the older rows stay as the history. "Newest" is a three-part key —
`(ts, attempt, lifecycle rank)` — not `ts` alone: transitions are produced in
batches (the dispatcher flushes at most every 50 ms), so `in_flight` and
`delivered` of one delivery routinely share a millisecond, and ordering by
timestamp alone breaks the tie by row order, which reported every delivery as
`in_flight`/`0` instead of `delivered`/`200`. The rank is the position of the
status in the lifecycle table below, so a same-millisecond transition always
loses to the one that follows it in the lifecycle. `attempt` exists to keep
the ordering total when two rows of the same status land on the same
millisecond (a retry).

The status summary counts **distinct deliveries**, not rows: `total` is the
number of distinct deliveries seen in the window and each status counts the
distinct deliveries that were *seen in* that status within the window. A
delivery that transitioned through `pending` → `in_flight` → `delivered` is
therefore counted under all three — the counts may sum to more than `total`.
What never happens is counting one delivery twice *within* one status: a
delivery with a dozen status rows still contributes at most 1 to each bucket,
which is exactly the mistake `total` exists to rule out (it was a plain row
count once, and reported 21 for a batch of 20).

These eleven values are the contract — defined once in `internal/status` and
accepted verbatim by the Status API `?status=` filter, by the replay
selector's `statuses` (except `filtered`, see below) and by the status-summary
counts:

| Status | Meaning | Written by | Terminal |
| --- | --- | --- | --- |
| `pending` | The router created it (in the same Kafka transaction as the delivery, reason `router`), or it is parked in the inbox — the `reason` says why: `circuit_open`, `draining`, `rate_limited`, `queue_full` (dispatcher), `no_connection` (SSE gateway), `replay` (replay worker) | router, dispatcher, SSE gateway, replay worker | – |
| `in_flight` | A webhook delivery attempt is running | dispatcher | – |
| `retrying` | The attempt failed, a later one is scheduled; carries `attempt`, `http_code`, `latency_ms` and a reason (`http_error`, `transport_error`, `http_429`) | dispatcher | – |
| `paused` | The circuit breaker is open (`circuit_open`); the delivery waits for the probe | dispatcher | – |
| `sent` | SSE only: written to the connection but not acknowledged yet | SSE gateway | – |
| `delivered` | Webhook: 2xx received. SSE: the client acknowledged the sequence | dispatcher, SSE gateway | ✓ |
| `dead_lettered` | Retries exhausted (`max_attempts`), or the receiver answered `410 Gone` (`gone`) | dispatcher | ✓ |
| `dropped` | Discarded on purpose: inbox overflow (`backlog_max`), full queue of a live-class delivery (`queue_full`), retries exhausted while live (`max_attempts`), live-class with no connection (`live_class`), unknown subscription (`unknown_subscription`), inbox write failed (`park_failed`) | dispatcher, SSE gateway | ✓ |
| `expired` | `expires_at` passed before the delivery went out | dispatcher, SSE gateway | ✓ |
| `failed` | Reserved by the spec ("failed *or* dead_lettered"); **no emitter writes it here** — exhausted retries become `dead_lettered`, so every filter accepts the value but it never appears in the data | – | ✓ |
| `filtered` | The router's verdict for a subscription that was a *candidate* for the event's type but whose filter rejected it: `reason` is `filter_mismatch` (filter said no) or `filter_error` (the CEL could not be evaluated, which counts as a non-match too). **No delivery exists** — the row carries the `delivery_id` that delivery *would* have had (`event_ref/subscription_id`), so it lists and counts like any other row. Extension beyond §9, which only describes the statuses of a delivery | router | ✓ |

`filtered` is the trace for a silent case: before it, a subscription whose
filter matched nothing left **no** record anywhere — no delivery, no status,
not even a log line — so "why did this never arrive" could only be answered by
re-running the filter by hand. Note the deliberate boundary: an event with **no
subscription at all** for its type gets no status (there is no subscription ID
to attach one to); instead the *publish* response carries a `warning` (see
[the API guide](api.md#publish-an-event)), because that is the only place the
mistake is visible at the source.

Because a `filtered` row is not a delivery, the replay selector rejects
`"statuses":["filtered"]` with a 400 (`status "filtered" cannot be replayed:
the filter rejected this event, so no delivery ever existed for it`) — a replay
re-applies the projection but **not** the filter, so replaying one would
deliver exactly what the filter said no to. Time-range and `event_refs`
selectors exclude these rows too, unconditionally, in `replay.Resolve`.

`filtered` rows *are* counted in `status-summary` (in their own bucket and in
`total`, which is the number of distinct delivery IDs seen in the window): a
subscription whose filter rejected every event reports `total` equal to the
number of rejected events, all of it under `counts.filtered` — which is the
point of the row.

The record also carries `reason`, `attempt`, `http_code`, `latency_ms`,
`connection_id`, the replay ID and the event's retention class — never a
payload. Statuses lag the real world by 1–5 s because `status-writer`
batch-inserts them. An unknown `?status=` value on the Status API is not
validated: it simply matches no rows (the replay selector rejects the same
mistake with a 400).

Replay *jobs* are a different, unrelated state machine in `replay_jobs`:
`pending → running → done`, plus `cancel_requested → cancelled` on a cancel
and `failed` on an error.

## Circuit breaker and dead letters

Two independent mechanisms that both deal with a failing receiver. The
**circuit breaker** decides *whether to attempt at all* — a gate on the
endpoint. The **dead letter** is the verdict *after* attempting and giving up
— a terminal status on one delivery. The breaker never dead-letters anything,
and a dead letter never trips the breaker.

### The breaker: a traffic light per subscription

State machine of `internal/dispatch/breaker.go`, in memory and per
subscription (it resets on a dispatcher restart: one bounded storm of a dead
endpoint per restart is the documented cost):

```text
CLOSED ──(5 consecutive failures)──► OPEN ──(probe interval elapsed)──► HALF-OPEN
   ▲                                  ▲                                  │
   └────(probe answers 2xx or 410)────┴──────(probe fails: re-open)──────┘
```

- **Failure**: 5xx, any other 4xx, connection refused, timeout, DNS error.
- **Success**: 2xx **and 410** — the receiver answered, so it is healthy.
- **Neither**: `429` pauses the subscription for `Retry-After`, consumes no
  attempt and counts no failure.
- Defaults: `cb_failure_threshold=5` **consecutive** failures (any success
  resets the counter), `probe_interval_s=300`, clamped to 5–15 minutes.
- A failed probe re-opens for the **full** interval, so an endpoint that stays
  down costs exactly **one request per probe interval** — the open/close loop
  is bounded by design, never a storm.

What each state actually does:

| State | New delivery from Kafka | Parked row (drain) | HTTP sent |
| --- | --- | --- | --- |
| `closed` | attempted: `in_flight` → `delivered` / `retrying` / `dead_lettered` | drained at `drain_rate_per_s` | yes |
| `half-open` | the first one becomes the probe, every other is refused and parks (live class retries in memory) | the first row is the probe, the rest wait | exactly 1 |
| `open` | refused: `paused (circuit_open)` → inbox → `pending (circuit_open)` | skipped — except at probe time, when the first row *is* the probe | none |

Two rules follow: *open means don't try* (protect the receiver from us) and
*park means don't lose* (the delivery waits in PostgreSQL, bounded by
`backlog_max`, beyond which `dropped` appears — the spec's
guaranteed-delivery-vs-storage trade-off). The drain runs through its own
token bucket and is gated by the breaker, and while a drain batch runs new
non-live deliveries park (`reason=draining`), so drain and live traffic never
overlap on one subscription.

**Live-class deliveries are the exception**: no inbox duty, so while the
breaker is open they retry in memory with backoff and end as `dropped
(max_attempts)` — never parked, never dead-lettered.

### The dead letter: a delivery that ran out of chances

`dead_lettered` is a terminal status, **not a queue**: there is no DLQ table
and no DLQ topic, only the ClickHouse row that says "finished,
unsuccessfully". Two ways in:

| Door | Trigger | Cost |
| --- | --- | --- |
| `gone` | the receiver answered **410 Gone** | one attempt — the receiver's own decision (see below) |
| `max_attempts` | all `DISPATCHER_MAX_ATTEMPTS` (10) attempts failed | ten attempts with exponential backoff (0.5 s → 60 s, jittered) |

After either door: no retry, no parking, no breaker involvement, ever. The
only way back is a **replay** (`{"statuses":["dead_lettered"]}`), which
re-emits `pending` for the same delivery ID and runs the whole lifecycle
again — that is Eventarium's DLQ.

**Why a receiver would answer 410**: HTTP's permanent "gone", used as an
opt-out — tenant deleted, endpoint decommissioned, subscription cancelled
downstream. It turns "keep hammering a receiver that told you to stop" into
one clean decision: no retry budget burned, no breaker noise (it counts as a
*success*), and a status the replay API selects on. It is an extension:
§9 only defines `failed or dead_lettered = retries exhausted`.

### How they interact (barely)

| Situation | The breaker does | The dead letter does |
| --- | --- | --- |
| Receiver totally down | opens after 5 failures, everything parks | almost nothing arrives here — attempt 6 is already refused |
| Receiver alive, one payload keeps failing | never opens (successes reset the counter) | `dead_lettered (max_attempts)` on attempt 10 |
| Receiver answers 410 | counts as success, stays closed | `dead_lettered (gone)` at once |
| Breaker open | emits `paused`, delivery parks | irrelevant — no attempt is made |
| Already `dead_lettered` | never sees it again | terminal until a replay |

The two answer different questions: the breaker detects *"the endpoint is
broken"* (stop trying, park, wait for recovery), the dead letter detects *"the
endpoint is fine but this delivery won't make it"* (give up, make it
replayable).

### Timeline: receiver hard down

```text
events 1-5    in_flight → retrying (transport_error), backoff 0.5s 1s 2s 4s
              5th consecutive failure → breaker OPEN
events 6-∞    paused (circuit_open) → pending (circuit_open)   no HTTP at all
t+300s        drain: row 1 is the probe → refused → re-OPEN     one request
              row 2: allow() is false → paused → row stays parked
...           one request per probe interval until the receiver returns
probe 200     breaker CLOSED (failures=0) → rows drain at drain_rate_per_s
              every parked delivery ends delivered
```

With an *intermittently* failing receiver the breaker never opens, and one
unlucky delivery instead reaches `dead_lettered (max_attempts)`.

### Watching it

```sh
grep "circuit breaker" dispatcher.log   # opened / closed after successful probe / re-opened after failed probe
docker compose exec -T clickhouse clickhouse-client -u pubsub --password pubsub -d pubsub \
  -q "SELECT status, count() FROM delivery_status GROUP BY status FORMAT PrettyCompact"
docker compose exec -T postgres psql -U pubsub -d pubsub \
  -c "SELECT subscription_id, reason, count(*) FROM inbox GROUP BY 1, 2;"
```

Breaker cycles add status rows every time (`paused`, `pending`, `retrying`) —
`delivery_status` is append-only — but they never produce new deliveries or
new events: the router writes one delivery record per event and subscription,
forever.

## At-least-once, and what that costs

Delivery is at-least-once by design. Two paths produce duplicates, and both
are replay of uncommitted work rather than re-production:

- **A crashed consumer.** Kafka hands its partitions to a survivor, which
  re-reads from the last committed offset — everything the dead member
  produced after its last commit is re-produced. Measured: duplicates fall on
  exactly the killed member's partitions and nowhere else.
- **An aborted transaction.** The router's per-batch transaction is rolled
  back and the batch is polled again. The abort log carries `elapsed`,
  `past_txn_timeout` and `txn_timeout`, so a timeout can be told from a
  revoke without a rerun.

Reading one duplicated `delivery_id` out of Kafka returns a single record in
both cases. Consumers must still be idempotent — that is the contract — but
the platform never *re-produces* a delivery that already committed.

The counterpart is that nothing is lost: with PostgreSQL stopped and the
receiver offline simultaneously, 200 published events produced 200 failed
attempts, 200 failed parks, and a commit point that stayed behind the records
until the database came back — then all 200 were parked, drained and
delivered. `expected=200 received=200 missing=0 unexpected=0 duplicates=0`.
