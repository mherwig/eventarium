# Eventarium

**CloudEvents in, delivered with guarantees out.** Eventarium accepts
CloudEvents over REST, filters and reshapes them per subscription, and
delivers them over webhooks or SSE with every delivery tracked to a terminal
outcome. Built in Go on Kafka, PostgreSQL and ClickHouse.

> **Status: alpha.** It works end to end and is measured under load and under
> injected faults — but it is not production-ready. Every line was written by
> an AI agent from [the specification](docs/spec.md); nothing here has been
> reviewed, audited or hardened by a human engineer. Don't point it at
> production data yet.

## What it does

- **Per-subscription filtering and projection** — CEL filters over attributes
  and data, an allowlist projection, an optional ordering key. Two subscribers
  of the same event type receive two different payloads.
- **Two channels, one status model** — webhooks (HMAC-signed, SSRF-guarded,
  rate-limited) and SSE (heartbeats, explicit acks, cursor reconnect that
  replays what you missed), both writing into the same append-only status
  store.
- **Tracked, at-least-once delivery** — eleven statuses from `pending` to
  `delivered` or `dead_lettered`. Nothing vanishes: a delivery that cannot
  reach its receiver parks durably and drains when the receiver returns.
- **Receiver-friendly failure handling** — a per-subscription circuit breaker
  that costs a dead endpoint one request per probe interval, never a storm.
  `429` honours `Retry-After`; `410 Gone` is a clean opt-out.
- **Transactional where it counts** — publish batches and the router's
  consume-modify-produce are single Kafka transactions.
- **Horizontal scale-out** — any number of replicas of every service; the two
  pieces of work Kafka cannot partition are handled explicitly (an inbox
  lease, a groupless presence view).
- **Replay** — resend what a subscription matched, selected by status, time
  window or event reference, with dry-run and idempotency keys.

## What to expect

Measured on one 8-core laptop with single-node Kafka, PostgreSQL and
ClickHouse. Every figure comes from an independent oracle that recomputes
expected deliveries itself rather than asking the platform.

| | |
| --- | --- |
| Throughput | **1,003 ev/s** sustained — 30,000 events fanning out to 145,918 deliveries |
| Webhook latency | p50 **34 ms**, p99 **59 ms** |
| SSE latency | p50 **56 ms**, p99 **90 ms** |
| Correctness | `missing=0 unexpected=0 duplicates=0` across all 40 subscriptions |

Resilience, with one fault injected mid-publish on nine replicas
(three routers, three dispatchers, three gateways):

| Fault | Result |
| --- | --- |
| `kill -9` a dispatcher, a router **and** a gateway | **0 lost** |
| PostgreSQL down *and* receiver offline, overlapping | **0 lost**, 0 duplicates |
| ClickHouse down 20 s | **0 lost**, 0 duplicates |
| Kafka broker restart | **0 lost** |
| `kill -9` a dispatcher (failover asserted) | **0 lost**, partitions owned again in **13 s** |

Duplicates appear only when a consumer leaves the group — replay of
uncommitted work, never re-production, and confined to the dead member's
partitions.

These are single-machine numbers on a single-node topology, not cluster-scale HA
benchmarks. Read [the performance docs](https://mherwig.github.io/eventarium/docs/performance)
before quoting them.

## Quick start

```sh
docker compose up -d
go run ./cmd/admin all          # topics + migrations + ClickHouse table
go build -o bin/ ./cmd/...
```

Then follow [Getting started](https://mherwig.github.io/eventarium/docs/getting-started)
for one event end to end, or run the whole thing through the harness:

```sh
make infra && make stack        # nine replicas of the consumers
make chaos SCEN=pods            # break it on purpose, let the oracle judge
```

## Documentation

Full docs, searchable: **[mherwig.github.io/eventarium](https://mherwig.github.io/eventarium)**

| | |
| --- | --- |
| [Introduction](https://mherwig.github.io/eventarium/docs/intro) | What Eventarium is and does |
| [Getting started](https://mherwig.github.io/eventarium/docs/getting-started) | Build it, push one event through |
| [HTTP API](https://mherwig.github.io/eventarium/docs/api) | Register, subscribe, publish, receive, replay |
| [Reliability](https://mherwig.github.io/eventarium/docs/reliability) | Statuses, circuit breaker, dead letters |
| [Performance](https://mherwig.github.io/eventarium/docs/performance) | Load tests, chaos runs, failover |
| [Architecture](https://mherwig.github.io/eventarium/docs/architecture) | Services, stores, transactional boundaries |
| [Configuration](https://mherwig.github.io/eventarium/docs/configuration) | Every environment variable |
| [Specification](https://mherwig.github.io/eventarium/docs/spec) | The document this implements |

In the repository: [specification](docs/spec.md) ·
[service contracts](docs/contracts.md) ·
[deviations and open points](docs/deviations-and-open-points.md).

## Licence

MIT — see [LICENSE](LICENSE).
