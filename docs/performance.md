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
Kafka/PostgreSQL/ClickHouse and one instance of every service.

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
cluster-scale HA (single broker, single node of everything). ClickHouse status
write/query throughput is the most pressured component at this rate. The
status table is cumulative, so re-running a load test for the same owner
counts earlier runs' deliveries too — the harness scopes Status API queries to
the run's publish window (`from`/`to`).

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

**`missing=0` and `unexpected=0` in all five** — the two numbers the whole
exercise exists for.

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

Not exercised yet: the soak test, restart-mid-job recovery of the replay
worker, the multi-day offline scenario and multi-node SSE gateway failover.

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

Chaos scenarios are a `make` target away:

```sh
make infra            # Kafka, PostgreSQL, ClickHouse plus topics and schemas
make stack            # five services, three replicas of each consumer
make chaos SCEN=pods  # pods | pg | kafka | ch | rebalance
make chaos-test       # the driver's own tests, no stack needed
```
