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
rate-limited), 1 KiB payloads, `unordered` unless stated. Every figure below
is a full-drain run judged by the oracle; every one ended
`missing=0 unexpected=0 duplicates=0`. The three rate-limited subscriptions
are excluded from the latency columns — they take minutes by design.

| Mode | Rate | Channel | p50 | p90 | p99 |
| --- | --- | --- | --- | --- | --- |
| `unordered` | 100 ev/s | webhook | **79.8 ms** | 139 ms | 385 ms |
| `unordered` | 100 ev/s | SSE | **92.3 ms** | 160 ms | 512 ms |
| `unordered` | 300 ev/s | webhook | **154.2 ms** | 270 ms | 442 ms |
| `unordered` | 300 ev/s | SSE | **178.5 ms** | 321 ms | 528 ms |
| `unordered` | 1,000 ev/s | webhook | **328–383 ms** | 594–2097 ms | 8329–10026 ms |
| `unordered` | 1,000 ev/s | SSE | **399–454 ms** | 622–731 ms | 1004–1341 ms |
| `key_ordered` | 1,000 ev/s | webhook | **516.8 ms** | 5450 ms | 10956 ms |
| `key_ordered` | 1,000 ev/s | SSE | **542.0 ms** | 918 ms | 1212 ms |

The 1,000 ev/s rows are ranges across runs rather than single numbers: this
host does not hold still at that rate, and the spread is part of the result.
Worth noting what the strict mode costs at that rate: on one identical run,
`key_ordered` webhook sits at p90 **5450 ms** while its SSE half sits at
918 ms — the webhook side queues far deeper than the SSE side when per-key
ordering is imposed on a stack already at its ceiling, which is a further
reason not to run the strict mode at a rate the stack has no headroom for.

### Where those milliseconds go

End-to-end splits into three segments that sum to it. The check is that the
**per-delivery sum of the three segments** reproduces the harness's own
reported webhook p50 to within 0.6 ms across all three runs — so the
decomposition is the same measurement cut three ways, not three medians
from three populations. It is computed by `scripts/decompose.py`, which
joins the generator's `pub.jsonl` to the delivery-status rows per event:

| Segment | 100 ev/s | 300 ev/s | 1,000 ev/s |
| --- | --- | --- | --- |
| publish → `pending` (events topic) | 30.8 ms | 89.1 ms | 195–222 ms |
| `pending` → `in_flight` (deliveries topic) | 47.0 ms | 64.0 ms | 119–138 ms |
| `in_flight` → `delivered` (the HTTP call) | **0 ms** | **1 ms** | **2 ms** |
| **per-delivery sum** | **79.6 ms** | **153.9 ms** | **329–384 ms** |

**The delivery itself is 2 ms.** Everything else is queueing, and it was
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
→ poll** loop. Measured per stage, each figure an average over 50 batches
(one representative run per rate; the 1,000 ev/s column is the range across
two runs):

| Router stage | 100 ev/s | 300 ev/s | 1,000 ev/s |
| --- | --- | --- | --- |
| `kafka_wait` (produce → poll pickup) | 32 ms | 86 ms | 172–209 ms |
| `produce_sync` (deliveries + status) | 33 ms | 43 ms | 61–72 ms |
| `produce_end` (markers + offset commit) | 9 ms | 14 ms | 28–31 ms |
| `process` | 0 ms | 1 ms | 9 ms |
| `cycle` (poll → poll) | 44 ms | 60 ms | 97–119 ms |

Two numbers in that table do the explaining. `produce_sync` + `produce_end`
is **89–103 ms of a 97–119 ms cycle** — about nine tenths of the loop:
the router can only commit as fast as Kafka will take a transaction across
RF=3, and consumption is gated by production. And `kafka_wait` (172–209 ms)
exceeds the whole cycle, which a steady-state consumer cannot do by fetching
alone — so records are queued rather than merely waiting for a poll.

They are. Sampled mid-publish with `kafka-consumer-groups`:

| Group | Lag during publish | After drain |
| --- | --- | --- |
| `router` | **292–639 records** | 1 |
| `dispatcher` | 42,741 (27,817 of it in **one** partition) | 22 |
| `status-writer` | 14,268 | 0 |

The router's lag is the same order as the measured `kafka_wait`, which is
the point: a few hundred records at 1,000 ev/s is a few hundred milliseconds
of events sitting in Kafka rather than consumed. It slightly *overstates*
queueing too — the query reports the **committed** offset, which advances
only once per ~97 ms transaction, so a cycle's worth of already-processed
records still reads as lag until the next commit. The dispatcher's backlog is
the three rate-limited subscriptions accumulating *by design*, which is why
the unlimited subscriptions still see a 119 ms queue; and status-writer lag
only delays **observation**, never delivery, because `ts` is stamped when the
record is emitted rather than when it is written.

### Why 1,000 ev/s costs what it costs

Two things are at their limit at that rate, and neither is the delivery
path. The first is in the table above: the router commits only as fast as
Kafka accepts a transaction across RF=3. The second is the host — on this
8-core laptop, sampled during a `key_ordered` run at 1,000 ev/s:

| | |
| --- | --- |
| Load average, stack idle | 3.90 |
| Load average, mid-publish | **17.1 → 27.6** |

Load average counts runnable *and* uninterruptible-sleeping tasks, so this
says the host is contended, not that it is purely CPU-bound. It is also not
all the stack: the harness's own generator, its 20 SSE clients and its
receiver are running on the same eight cores — a measurement client
competing with the system it measures is inherent to a load test on one
machine.

The practical consequence: **at 1,000 ev/s single-event the stack runs at
its ceiling, and latency at a ceiling is queueing.** Lower the rate and it
falls away — 154 ms at 300 ev/s, 80 ms at 100 ev/s — with far less work
competing for the same eight cores. Load average was sampled only during the
1,000 ev/s runs, so no figure is claimed here for the other two rates. The
34/56 ms figures above were taken on single-node Kafka with one instance of
every service, and they do not transfer — not because anything regressed, but
because replication factor three on shared hardware is a different machine.

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
  attribute filters, data filters and projections, at 50 ev/s (p50 22 ms /
  p99 28 ms) and at 1,000 ev/s.
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
