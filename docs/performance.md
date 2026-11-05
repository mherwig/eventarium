---
id: performance
title: Performance and resilience
sidebar_position: 5
---

Every number on this page comes from the harness's **independent oracle**: it
recomputes the expected deliveries locally — evaluating the CEL filters
itself, never asking the platform — and compares them against what receivers
actually recorded and what the Status API reports. It exits non-zero on any
missing or unexpected delivery.

## Throughput at 1,000 events/s

Measured on the local compose stack on one 8-core laptop: single-node
Kafka/PostgreSQL/ClickHouse and one instance of every service — the topology
as it then stood. All three stores have since gone to three nodes each, and
these figures predate that change.

Topology: 4 event types, 40 subscriptions — 20 webhook and 20 SSE — covering
unordered, per-shard ordered (attribute filter), key-ordered (data filter
`int(data.seq) % 2 == 0`) and rate-limited webhook subscriptions, plus 20
connected SSE clients. 1 KiB JSON payloads, batch mode (100 events per
request).

| | |
| --- | --- |
| Published | **30,000** events at a sustained **1,003 ev/s**, zero publish failures |
| Fanned out to | **145,918** deliveries |
| Webhook p50 / p99 | **34 ms / 59 ms** |
| SSE p50 / p99 | **56 ms / 90 ms** |
| Oracle | `expected=145918 received=145918 missing=0 unexpected=0 duplicates=0` |

The dispatcher bursts to ~2,900 webhook deliveries/s and drains to
`queue_depth=0` with `offsets_in_flight=0` and zero retries, parks, drops or
dead-letters. Rate-limited subscriptions (`rate_limit_per_s=50`) show minutes
of tail latency by design — bounded by their own limit, not by delivery
speed.

Latency from publish to receipt, four runs of the identical scenario — three
before publish-api went transactional, one after:

| Channel | p50 | p90 | p99 | max |
| --- | --- | --- | --- | --- |
| Webhook (unlimited), runs 1–3 (plain batch) | 39 / 38 / 37 ms | 48 / 46 / 45 ms | 94 / 70 / **52 ms** | 183 / 107 / 68 ms |
| Webhook (unlimited), run 4 (**transactional batch**) | 34 ms | 45 ms | **59 ms** | 88 ms |
| SSE, runs 1–3 (plain batch) | 61 / 59 / 58 ms | 84 / 75 / 74 ms | 253 / 99 / **92 ms** | 333 / 145 / 113 ms |
| SSE, run 4 (**transactional batch**) | 56 ms | 74 ms | **90 ms** | 144 ms |
| Webhook, rate-limited subs (by design) | 59 s | 108 s | 119 s | 120 s |

Read runs 1–2 as warm-up: the first run of a session pays for cold page
caches, empty ClickHouse partitions and unwarmed connection pools, and its p99
is 3× the steady figure. The transactional batch costs nothing measurable at
this rate — identical receipt counts (99,283 webhook + 46,635 SSE) at
webhook p99 59 ms against a 52–94 ms spread in the earlier runs.

:::note Cold start
A run started immediately after a full stack restart is measurably slower in
its **first ~10 s** and then settles: p99 1,101 ms for the first bucket
against 65 / 77 / 67 ms for the three that follow. A whole-run p99 in the high
hundreds of milliseconds says "cold start", not "steady state". Read the
per-bucket view, or let a warm-up publish finish first, before treating a
whole-run p99 above ~100 ms as a regression.
:::

Caveats: this validates the pipeline shape and per-stage latencies, not
cluster-scale HA. It was measured on **single-node Kafka, PostgreSQL and
ClickHouse** — the stack has since moved to three brokers at RF=3, three
CloudNativePG instances and three replicated ClickHouse nodes. Those figures
have **not** been re-taken in batch mode, so no row above covers HA for the
storage tier; what the three-node topology does measure, one event per
request, is in [Single-event publishing](#single-event-publishing-on-the-three-node-stack)
below — that is a different publish mode, so the two tables are not
comparable cell for cell. ClickHouse status write/query throughput is
the most pressured component at this rate. The
status table is cumulative, so re-running a load test for the same owner
counts earlier runs' deliveries too — the harness scopes Status API queries to
the run's publish window (`from`/`to`).

## Single-event publishing on the three-node stack

Batch mode amortises one HTTP round trip and one Kafka produce over 100
events. Publishing **one event per request** is the case that actually
matters, and it pays those costs per event — so it is measured separately,
on the topology above (three brokers at RF=3, three CloudNativePG instances,
three replicated ClickHouse nodes, nine consumer replicas).

Four event types, 40 subscriptions (17 unlimited webhook, 20 SSE, 3
rate-limited), 1 KiB payloads, `unordered` unless stated, **at 1,000 ev/s** —
the rate these measurements are taken at. The measured ceiling on this
hardware is ~1,500 ev/s, see [The throughput ceiling](#the-throughput-ceiling).

All of them are full-drain runs judged by the oracle; every one ended
`missing=0 unexpected=0 duplicates=0`. The three rate-limited subscriptions
are excluded from the latency columns — they take minutes by design.

| Mode | Channel | p50 | p90 | p99 |
| --- | --- | --- | --- | --- |
| `unordered` | webhook | **202–246 ms** | 0.29–0.41 s | 0.38–0.75 s |
| `unordered` | SSE | **229–291 ms** | 0.34–0.49 s | 0.43–0.88 s |
| `key_ordered` | webhook | **225 ms** | 0.37 s | 0.60 s |
| `key_ordered` | SSE | **263 ms** | 0.43 s | 0.68 s |

The `unordered` rows are ranges across **seven identical runs of the current
build**, 40 s of publish each, rather than single numbers: the host does not
hold still at that rate, and the spread is part of the result. Six of the
seven sit within **17 ms** of each other on p50 and **14 ms** on p90; the
seventh was taken immediately after a restart and paid for cold caches, which
is what widens the range. That 17 ms is the bar a change has to clear before
it counts as a change.

**p99 is only comparable within one publish duration.** A 60 s publish at the
same rate roughly doubles it — measured at **1.38 s** webhook and **1.46 s**
SSE against the 0.38–0.75 s above — because the three rate-limited
subscriptions accumulate for half again as long and their backlog reaches the
unlimited column through the shared dispatcher. The rows this table used to
carry (328–383 ms and 8.3–10.0 s) came from two 60 s publishes taken before
the dispatcher stopped parking on its poll loop; at the same 60 s duration
today that tail reads 1.38 s, so most of what those numbers recorded is gone.

The `key_ordered` rows are one run at the same rate and the same
`max_in_flight=8` as the `unordered` rows above. Strict mode now costs
**nothing measurable** against it — webhook p50 225 ms against 202–246 ms,
p90 372 ms against 294–407 ms — where this row used to read p90 **5.45 s**,
an order of magnitude worse, with its SSE half at 0.92 s. That figure was not
reproduced on this build.

Before reading anything into a mode-to-mode difference, read
[Choosing the delivery modes](#choosing-the-delivery-modes): the default seed
cycles `ordered,key_ordered,unordered`, and `ordered` pins `max_in_flight` to
1, so a run that leaves it in the mix reports that subscription's ceiling
rather than the platform's — and a row labelled `key_ordered` taken from such
a run describes `ordered` instead.

### Where those milliseconds go

End-to-end splits into three segments that sum to it. The check is that the
**per-delivery sum of the three segments** reproduces the harness's own
reported webhook p50 to within 1 ms across all seven runs — so the
decomposition is the same measurement cut three ways, not three medians
from three populations. It is computed by `scripts/decompose.py`, which
joins the generator's `pub.jsonl` to the delivery-status rows per event:

| Segment | 1,000 ev/s |
| --- | --- |
| publish → `pending` (events topic) | 121–150 ms |
| `pending` → `in_flight` (deliveries topic) | 76–92 ms |
| `in_flight` → `delivered` (the HTTP call) | **1–2 ms** |
| **per-delivery sum** | **202–246 ms** |

**The delivery itself is 1–2 ms.** Everything else is queueing, and it was
worth ruling the obvious suspects out properly:

- **publish-api is not the bulk of it.** A record's broker timestamp sits at
  p50 **−0.2 ms** from the generator's schedule time — measured with only
  publishing running. Under full load publish-api's whole handler is 41 ms
  p50 (against 1.7 ms idle) and the timestamp is stamped at the *start* of
  it, so under load the gap is at most ~41 ms, not the ~200 ms in question.
  That 41 ms is the ack wait, and a subscriber does not wait on it.
- **franz-go's fetcher is not blocking.** `poll_ms` is **0** — records are
  already buffered when the router polls. An earlier working theory that the
  fetcher held records for 60–110 ms is wrong and does not explain any of
  this.
- **Nothing is being dropped by the generator.** With the sender pool sized
  from `-rate`, every run reported `dropped=0`.

What is left is the router, which is a **serial poll → process → transaction
→ poll** loop. Measured per stage, each figure an average over 50 batches,
as a range across the same seven runs at 1,000 ev/s:

| Router stage | 1,000 ev/s |
| --- | --- |
| `kafka_wait` (produce → poll pickup) | 120–138 ms |
| `produce_sync` (deliveries + status) | 49–54 ms |
| `produce_end` (markers + offset commit) | 19–23 ms |
| `process` | 3–4 ms |
| `cycle` (poll → poll) | 74–81 ms |

Two numbers in that table do the explaining. `produce_sync` + `produce_end`
is **68–77 ms of a 74–81 ms cycle** — about nine tenths of the loop, a
ratio that has not moved:
the router can only commit as fast as Kafka will take a transaction across
RF=3, and consumption is gated by production. And `kafka_wait` (120–138 ms)
exceeds the whole cycle, which a steady-state consumer cannot do by fetching
alone — so records are queued rather than merely waiting for a poll.

They are. Sampled mid-publish with `kafka-consumer-groups`:

| Group | Lag at t+15 s → t+38 s | After drain |
| --- | --- | --- |
| `router` | **521 → 1** | 1 |
| `dispatcher` | 19,159 → 35,669 | 23 |
| `status-writer` | 52,908 → 120 | 0 |

Lag is reported as the **committed** offset, which advances only once per
~75 ms transaction, so a cycle's worth of already-processed records reads as
lag until the next commit — and the router's figure moves a lot within a
single run (521 during the initial ramp, 1 once settled; see the sweep
below). Read as an order of magnitude rather than a point value it agrees
with `kafka_wait`: 120–138 ms at 1,000 ev/s is ~120–140 records of standing
backlog across three routers, against 1–521 sampled.

What is stable in that table is the shape: the dispatcher's backlog is the
three rate-limited subscriptions accumulating *by design*, which is why the
unlimited subscriptions still see a 76–92 ms queue; and status-writer lag only
delays **observation**, never delivery, because `ts` is stamped when the
record is emitted rather than when it is written.

### Why 1,000 ev/s costs what it costs

Two things set the **median** at that rate, and neither is the delivery path.
The first is in the table above: the router commits only as fast as Kafka
accepts a transaction across RF=3. The second is the host — on this 8-core
laptop, sampled during the ceiling sweep above:

| | |
| --- | --- |
| Load average, stack idle | 1.33 |
| Load average mid-publish, 1,000 ev/s | **9.71 → 21.75** |
| Load average mid-publish, 2,500 ev/s | **21.14 → 34.65** |

Load average counts runnable *and* uninterruptible-sleeping tasks, so this
says the host is contended, not that it is purely CPU-bound. It is also not
all the stack: the harness's own generator, its 20 SSE clients and its
receiver are running on the same eight cores — a measurement client
competing with the system it measures is inherent to a load test on one
machine.

The practical consequence: **single-event latency here is dominated by
queueing rather than by work** — and as the next section shows, what breaks
first past the ceiling is the dispatcher's queue, not the router's. The
34/56 ms figures above were taken on single-node Kafka with
one instance of every service, and they do not transfer — not because
anything regressed, but because replication factor three on shared hardware
is a different machine.

### The throughput ceiling

The oracle will not find a ceiling, because a ceiling is not a correctness
problem — correctness held at **every** rate tested. Swept with
`scripts/ceiling.sh` (40 s of publish each, then a full drain):

| Rate (single, `unordered`) | Achieved | webhook p50 | webhook p90 | webhook p99 | SSE p50 | drain | `unrecorded` |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1,000 ev/s | 1,000.0 | 266 ms | 0.52 s | 0.83 s | 315 ms | 177 s | 0 |
| 1,500 ev/s | 1,500.0 | 289 ms | **0.53 s** | 10.98 s | 336 ms | 273 s | 0 |
| 2,000 ev/s | 2,000.0 | 366 ms | **33.58 s** | 75.06 s | 427 ms | 285 s | 0 |
| 2,500 ev/s | 2,500.0 | 486 ms | 47.44 s | 85.74 s | 511 ms | 465 s | 90,390¹ |

40 s of publish each, then a full drain, at exactly the requested rate.
`missing=0 unexpected=0 duplicates=0` at **every** rate — which is the
point: the oracle cannot find a ceiling.

**The knee is between 1,500 and 2,000 ev/s, and it appears in the tail
first: p90 goes from 0.53 s to 33.58 s — a 63× jump — while p50 moves only
289 → 366 ms.** At 1,500 the median is under a third of a second and only the
p99 has started to climb (10.98 s); at 2,000 the median is still sub-second
while p90 is half a minute, and only the rate at which you stop publishing
matters.

¹ `unrecorded=90,390` is the **oracle's window, not lost rows**, and the line
above already proves it: `expected = received = 486,422` with `missing=0`, so
every delivery arrived. What the oracle cannot see is the *status* row for the
ones that completed after the publish window — the drain ran **465 s** past a
40 s publish, so a large slice of `delivered` rows land after the query's `to`
timestamp. The one path that genuinely drops a status record, the dispatcher's
own `status_lost` counter, was **0** across this sweep.

So the ceiling for single-event publishing on this topology and this host is
**~1,500 ev/s**: below it latency runs from hundreds of milliseconds to low
seconds, above it latency is unbounded and only the rate at which you stop
publishing matters.

Three signals, one of which no longer discriminates. Drain time grows
177 → 465 s — the backlog still present when publishing stopped. `dropped`
cannot be the threshold, because **two sweeps disagree about it**: 0 / 884 /
0 / 1,762 in one and 0 / 0 / 0 / 366 in another, at identical configurations;
the whole 884 fell in two seconds (402 and 482), so it is a transient stall
overflowing the sender buffer rather than the edge itself. And **consumer lag
still does not locate the knee** — 748 → 459, 805 → 647, 919 → 1,080,
1,976 → 1,691 — falling at three of the four rates and rising only 18% at the
one where p90 is 34 s, so the tail is the only measurement left that finds
it.

#### What moved the ceiling

The first sweep of this topology put the knee at 1,000–1,500 ev/s, with the
unlimited subscriptions' `pending → in_flight` tail at p90 **7.61 s** at
1,500. That was the dispatcher parking *synchronously on its poll loop*:
`park()` ran a PostgreSQL `INSERT` on the one goroutine every subscription on
that instance shares, so the three rate-limited subscriptions' overflow
stalled everyone else's records. Measured directly, the instance owning them
ran a single batch at **402 ms** with a per-record high of **1,599 µs** while
the other two never exceeded 4 ms and 72 µs — the median hid it, which is why
the stage timings are read per instance rather than pooled.

Parks now go to `parkRetryLoop`, which already wrote the row and settled the
offset only after the write succeeded, so nothing about durability moved. The
stall is gone — that instance's `handle_ms` max falls 402 → 4, making it
indistinguishable from the others — and the knee moved with it: at 1,500 ev/s
the identical measurement went p90 **7.88 s → 0.85 s** and p99
**25.56 s → 3.37 s** — a pair taken before this host had any memory
headroom. The same rate in the sweep above now reads **0.53 s** p90 and
**10.98 s** p99, and the p99 gap is the knee showing through the tail rather
than anything the fix left behind.

#### Where the ceiling is

Not where the median lives. At 1,000 ev/s the median is dominated by the
router's segment (121–150 of the 202–246 ms), but the **tail is entirely the dispatcher's
queue** — same sweep, per-delivery p90 from `scripts/decompose.py`:

| Segment (p90) | 1,000 | 1,500 | 2,000 | 2,500 |
| --- | --- | --- | --- | --- |
| publish → `pending` (router) | 339 ms | 313 ms | 349 ms | 428 ms |
| `pending` → `in_flight` (dispatcher) | 198 ms | 200 ms | **33.33 s** | **47.18 s** |
| `in_flight` → `delivered` (HTTP) | 9 ms | 11 ms | 13 ms | 14 ms |

The router's segment grows **1.26×** across the whole sweep, the HTTP call
never leaves 9–14 ms, and the dispatcher's grows **238×** — all of it between
1,500 and 2,000. The knee is therefore still the **dispatcher's delivery
queue** saturating: the fix moved where that starts, not what it is.

The router is not the limiter either. Sampled twice inside each publish
window (the t+15 s sample catches the initial ramp, so t+38 s is the steadier
reading):

| Rate | lag t+15 s | lag t+38 s | Load average (8 cores) |
| --- | --- | --- | --- |
| 1,000 | 748 | 459 | 9.71 → 21.75 |
| 1,500 | 805 | 647 | 10.13 → 26.28 |
| 2,000 | 919 | 1,080 | 16.33 → 27.90 |
| 2,500 | 1,976 | 1,691 | 21.14 → 34.65 |

It falls at 1,000, 2,000 and 2,500 and is flat at 1,500 — so after the fix it
no longer distinguishes under from over the ceiling, which is why the knee
above is read from the tail instead. Load average comes from `/proc/loadavg`,
which counts uninterruptible sleepers as well as runnable tasks: it says the
host is contended, not that it is purely CPU-bound.

:::note Two harness traps, both of which produced wrong numbers first
A fixed 64-sender pool shed **500–953 events per run** at 1,000 ev/s — a
generator limit reporting itself as a shortfall in the platform, fixed by
sizing the pool from `-rate`. And a run must **fully drain** before it is
measured: judging at the end of the publish window hides the tail entirely,
which is why every figure above comes from a run that waited for both
receipt streams to stop growing first.
:::

## Resilience under injected faults

Same topology stepped up to **nine replicas** — three routers, three
dispatchers, three gateways — because killing one of one is not a failure
mode. Each scenario publishes at 200 ev/s for 60 s (12,000 events → 58,366
expected deliveries), injects one fault mid-publish, drains, then asks the
oracle:

| Scenario | Fault | missing | unexpected | duplicates |
| --- | --- | --- | --- | --- |
| `pods` | `kill -9` one dispatcher, one router, one gateway | **0** | 0 | 7,293 |
| `pg` | receiver offline **and** PostgreSQL down, 20 s overlap | **0** | 0 | 0 |
| `ch` | ClickHouse down 20 s | **0** | 0 | 0 |
| `kafka` | broker restart mid-run | **0** | 0 | 2,746 |
| `rebalance` | `kill -9` one dispatcher, handover measured against the session timeout | **0** | 0 | 533–16,554 |
| `gwfail` | `kill -9` one gateway, **left down for the whole run** | **0** | 0 | 0 |

**`missing=0` and `unexpected=0` in all six** — the two numbers the whole
exercise exists for.

`gwfail` is judged *while the gateway is still dead*, so its subscriptions have
to be routable somewhere else rather than on a node that comes back to serve
them. Its `duplicates=0` is the interesting one: nobody left a consumer group,
because gateway connections do not sit in one — the clients simply reconnected
and resumed from their cursors.

The duplicates track whether a *consumer left the group*, not whether something
broke. The `pods` run's duplicates fall on **partitions 8–11 only — exactly
the killed dispatcher's assignment** — and on no other partition (verified
twice against the topic's own sub→partition map). The two runs where only a
*store* died show **zero**, because no member left the group and the offsets
kept advancing throughout. Duplicates arrive when the group finally hands
over, and reading one duplicated `delivery_id` out of Kafka returns a single
record: it is replay of uncommitted work, not re-production. See
[Reliability](reliability.md#at-least-once-and-what-that-costs).

## Crash failover

A `kill -9`ed dispatcher's partitions belong to nobody until the group evicts
the dead member, then move to a survivor. Measured by polling the group's
assignment, on the three-dispatcher group:

| How the replica died | Until its partitions were owned again |
| --- | --- |
| `kill -9` | **13 s** at the shipped 15 s session timeout (46 s at franz-go's 45 s default) |
| `SIGTERM` | **~3 s** — but at t=1 s *all 12* were momentarily unowned, an eager rebalance stopping the world while one member leaves |

The two paths differ in kind, not only in speed: an ungraceful death costs a
third of the partitions a few seconds and leaves the rest untouched, a graceful
one costs everyone about two seconds. Kubernetes takes the graceful path for a
rolling update (`maxUnavailable: 0` surges first), so the first row is the
crash floor — OOMKill, node loss, a wedged process.

The `rebalance` scenario **asserts** this number rather than merely reporting
it: it fails beyond `KAFKA_SESSION_TIMEOUT + 15 s` of polling slack, so
tuning the value moves the bar with it and a library upgrade cannot quietly
change the behaviour. The values are a measured pair, not an inherited default
— see [Configuration](configuration.md#kafka-timeouts).

## What the load tests found

Two real bugs, worth remembering when touching these paths:

- The SSE delivery stage produced to `sse-live` with one synchronous Kafka
  round trip per record inside the serial batch loop, capping the fan-out at
  ~1,260/s (SSE p50 was 4.7 s). Now one produce per poll batch
  (`produceLiveBatch` in `internal/sse/delivery.go`); franz-go keeps
  per-subscription order because same-partition records are serialised in
  produce order.
- The status API's keyset cursor was bound as a `time.Time`, which the
  ClickHouse driver rendered as `toDateTime('...')` at second precision. With
  100+ status rows in one second — only under load — the page repeated forever
  and pagination stalled, hiding real deliveries from `verify`. Cursor and
  `from`/`to` are now bound as millisecond-precision strings inside
  `toDateTime64(?, 3)` (`internal/chquery/chquery.go`).

The `unexpected=5,185` of an early broker-restart run was publish-api's
partial batch: it returned 500 for a batch it had already written most of.
Batches are transactional now, so the same run reports 1,200 events failed and
*none* of them written.

## Verified end to end

Beyond the load and chaos runs, the following are covered by scenarios that
end `missing=0 unexpected=0 duplicates=0` client-side:

- All three delivery modes (unordered, per-shard ordered, key-ordered) with
  attribute filters, data filters and projections, at 50 ev/s and at
  1,000 ev/s. Latency for the first two of those modes is in the table above;
  the 50 ev/s run is asserted for correctness only, since its figures have not
  been re-taken.
- Dispatcher fault injection: circuit breaker with receiver outage, hard kill
  and restart, restart with a parked backlog.
- SSE gateway restart, graceful SIGTERM and hard SIGKILL with a live client.
- `Last-Event-ID` backlog fetch: a client reconnected by cursor after its
  gateway was SIGKILLed received all 1,200 events (100 through the cursor
  path), and a cursor reconnect after a client-side kill recovered a 119-row
  parked backlog instantly.
- The replay API end to end: 30 dead-lettered events replayed to 30
  delivered, a time-frame replay of 20, dry-run, idempotency,
  cancel-at-batch-boundary and the `not_replayable` paths.
- Stability hardening, scenario by scenario — no loss with the store down,
  `/readyz` behaving while `/healthz` does not, Status API 404s, soft delete,
  the SSE park path. Full detail in
  [Deviations and open points](deviations-and-open-points.md#stability-hardening-five-failure-modes-what-each-one-cost-and-gave-back).
- **Multi-node SSE gateway failover** (`gwfail`): a gateway `kill -9`ed and
  *left down* for the whole run, with the oracle judging while it is dead.
  6,199 deliveries sat on its subscriptions, all six of its clients reconnected
  to a survivor, routable subscriptions returned to 20 on the two survivors,
  and the run ended `missing=0 unexpected=0 duplicates=0 unrecorded=0`. The
  first run of this scenario found the real defect — see
  [Deviations and open points](deviations-and-open-points.md#open-points--still-open).

Not exercised yet: the soak test, restart-mid-job recovery of the replay
worker and the multi-day offline scenario.

## Reproducing it

Every figure above came from one command, which runs the whole sequence in
the only order that measures it — seed → receivers → publish → **full
drain** → settle the status store → oracle:

```sh
HARNESS_MODE=single ./scripts/loadtest.sh unordered 1000 40s load1
```

`scripts/decompose.py <owner> <seed.json> <pub.jsonl>` then splits the
result into the three segments, and `scripts/ceiling.sh 40s 1000 1500 2000
2500` sweeps rates. Doing it by hand is worth knowing too, because two steps
are easy to get wrong:

```sh
./bin/harness seed -owner load1 -prefix load1 -types 4 -subs 40 > seed.json
./bin/harness recv -addr :9090 -out recv.jsonl &
# one per seeded sse subscription (the ids file has no trailing newline:
# `while read` silently drops the last one — append one or use tail -1)
./bin/harness sse -sub <id> -owner load1 -addr http://localhost:8082 \
  -out sse-<id>.jsonl -duration 240s &
./bin/harness pub -owner load1 -rate 1000 -duration 30s \
  -types load1.type.0,load1.type.1,load1.type.2,load1.type.3 \
  -payload 1024 -out pub.jsonl
./bin/harness verify -owner load1 -api http://localhost:8083 \
  -status-api http://localhost:8081 -events pub.jsonl -received recv.jsonl \
  -sse "$(ls sse-*.jsonl | tr '\n' ',' | sed 's/,$//')"
```

`-sse` is optional but worth passing: without it the SSE half of the fan-out
is checked only against what the Status API *claims*, so a delivery written to
`sse-live` and never read by a client would not be caught. With it, `RECEIVED`
covers both channels — that is what makes `received` equal `expected` in the
results above rather than webhook-only.

**Wait for both receipt streams to stop growing before verifying.** Judging
right after the publish window ends measures only what happened to have
arrived and reports the rest as lost — or, with percentiles, hides a slow
tail that is still coming. Every latency figure on this page comes from a
run that waited for the counts to be flat across several consecutive polls.

### Choosing the delivery modes

`harness seed -modes` cycles the subscriptions over a comma-separated list.
The default is `ordered,key_ordered,unordered`, which is what the run above
used — and what `scripts/chaos.sh` requires: it assigns SSE clients to gateways
in groups of three on the assumption that three consecutive seeded
subscriptions are one of each mode, so a chaos run must keep the default.

For a **throughput** run, narrow it:

```sh
./bin/harness seed -owner load1 -prefix load1 -types 4 -subs 40 \
  -modes unordered,key_ordered > seed.json
```

`ordered` pins `max_in_flight` to 1, so one `ordered` subscription is a
per-subscription ceiling of one concurrent delivery. Left in the mix it becomes
the ceiling of the entire measurement, and the number you get back describes
that strict subscription rather than the platform. Keep the default for
correctness and chaos runs — those are the ones that need `ordered` exercised —
and narrow the list when the question is "how many events per second".

This changes *delivery* concurrency only. Which partition and broker an event
lands on is decided at publish by the partition key and is identical for every
mode ([Specification §7](spec.md#ordering-preserved-not-guaranteed)).

Chaos scenarios are a `make` target away:

```sh
make infra            # Kafka, PostgreSQL, ClickHouse plus topics and schemas
make stack            # five services, three replicas of each consumer
make chaos SCEN=pods  # pods | pg | kafka | ch | rebalance | gwfail
make chaos-test       # the driver's own tests, no stack needed
```
