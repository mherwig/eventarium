---
id: architecture
title: Architecture
sidebar_position: 6
---

Eight services on three data stores. The split that matters: **Kafka is the
data plane**, and neither database is on the delivery hot path.

```text
publish-api ─► Kafka events-<class> ─► router ─► Kafka deliveries ─┬─► dispatcher ─► webhook URLs
     ▲                                              │               └─► sse-gateway ─► SSE clients
     │ control plane (registry-api → PostgreSQL)    │
     └──────────────────────────────────────────────┴─► Kafka delivery-status ─► status-writer ─► ClickHouse ─► status-api
                                     overflow/offline ─► inbox (PostgreSQL) ─► drained back to the hot path
```

## Components

| Binary | Role | Listens |
| --- | --- | --- |
| `publish-api` | Accepts CloudEvents (binary, structured, batch), validates against the registry, stamps `expires_at` and the owner, writes to `events-<class>` | HTTP `:8080` |
| `registry-api` | CRUD for event types and subscriptions (the control plane) | HTTP `:8083` |
| `router` | Consumes all event topics; CEL attribute + data filters, ordering key, projection, fan-out. Writes the delivery and its pending status in one Kafka transaction | HTTP `:8085` (health only) |
| `dispatcher` | Webhook delivery: per-mode concurrency, retry with backoff + jitter, circuit breaker, token-bucket rate limit, HMAC signatures, SSRF guard, inbox parking and draining | HTTP `:8086` (health only) |
| `sse-gateway` | SSE connections with heartbeats and acks, presence topic, per-node live delivery, inbox parking and draining | HTTP `:8082` |
| `status-writer` | Consumes `delivery-status`, batch-inserts into ClickHouse (1 s / 10k rows) | HTTP `:8087` (health only) |
| `status-api` | The four delivery-status query endpoints, owner scoped | HTTP `:8081` |
| `replay-api` | Replay jobs: resolve a status/time-range selector against ClickHouse, re-read the original events from their `events-<class>` topic, re-enqueue deliveries marked with the replay ID | HTTP `:8084` |
| `admin` | Provisions Kafka topics, applies PostgreSQL migrations, creates the ClickHouse table | – |
| `minttoken` | Mints development JWTs (production uses an OIDC provider) | – |
| `harness` | Seed, open-loop publisher, simulated webhook receiver, simulated SSE consumer, independent oracle | – |

Kafka consumer groups: `router` (events-\*), `dispatcher` and `sse-delivery`
(both on `deliveries`, by channel), one `sse-live-<NODE_ID>` per gateway node,
`status-writer`. The presence view joins no group: every gateway node folds the
whole `sse-presence` topic itself, because `pick()` has to know the connections
of *all* nodes and a group would hand each node only its slice of them.

## Delivery modes

Three modes per subscription, per the spec's delivery policy:

| Mode | Guarantee |
| --- | --- |
| `unordered` | Fastest; no ordering across events |
| `ordered` | Per-shard ordering — events sharing a partition key arrive in publish order |
| `key_ordered` | Ordering per derived key (`ordering_key_def`): `subject`, `ext:<name>` or `json:<dotted.path>` |

All three are at-least-once. See [Reliability](reliability.md#at-least-once-and-what-that-costs)
for what that costs and where duplicates come from.

## Storage responsibilities

Neither database is on the delivery hot path: events and delivery payloads
live in Kafka, and both stores are read/written off that path.

| Store | Holds | Why there |
| --- | --- | --- |
| **Kafka** | `events-<class>` (the events themselves), `deliveries` (one record per subscription per event), `delivery-status`, `sse-live`, `sse-presence` | The data plane: buffering, fan-out, ordering per key, at-least-once replay from offsets |
| **PostgreSQL** | `event_types` + `subscriptions` (the registry), `inbox` (parked deliveries), `replay_jobs` (replay job metadata) | Small, transactional, mutated: CRUD with constraints, and the inbox needs ordered per-subscription scans, batched deletes and partition drop at expiry |
| **ClickHouse** | `delivery_status`: one append-only row per status transition (`pending → sent → delivered/failed/parked`) with attempt, HTTP code, latency, reason | High-volume append plus analytical reads (current status via the `(ts, attempt, rank)` latest key, distinct-delivery counts, keyset pagination), cheap expiry via partition TTL — the shape MergeTree is built for |

The status table grows with traffic × fan-out × attempts (~20k rows/s at the
1,000 ev/s rate), which is why it is not in PostgreSQL. The boundary is cheap
to revise: `status-writer` is the only writer, `status-api` the only reader.

Retention classes (`live`, `short`, `day`, `week`) map to separate
`events-<class>` topics, so TTL is a topic property rather than a query
predicate.

## More than one replica

Every component runs with any number of replicas. Work Kafka splits by
partition (consuming `events-*`, `deliveries`, `delivery-status`, the live
topic) needs no coordination — the group does it. Two pieces of work are *not*
partition-scoped, and both are handled:

- **The inbox duty of the dispatcher** (draining parked rows, sweeping expired
  ones) runs under a PostgreSQL advisory-lock lease, so exactly one dispatcher
  of the fleet does it while the others stay out of the inbox entirely. A
  standby takes over on its next 500 ms tick when the holder dies, and a
  rolling restart hands the lease over on shutdown. Without this, N replicas
  would each `SELECT ... LIMIT` the same parked rows and deliver the backlog N
  times, exactly when a receiver has just recovered. Everything else in the
  dispatcher — parking, counting, acking — is per record and runs on every node.
- **The presence view of the gateway** consumes groupless (above), so each node
  sees the connections of every node instead of its own slice.

Verified live with two dispatchers and two gateways: one lock row in
`pg_locks`, takeover in one tick, both gateways reporting the same `presence`
count while hosting different connections, and 40 parked rows drained to 40
receipts — not 80.

## Transactional boundaries

Two boundaries carry the correctness argument:

- **Publish batches** are written in one Kafka transaction, so a batch that
  fails half-way is not partially accepted. Before this, a failed batch had
  already written most of its events and still returned 500 — 5,185 phantom
  `unexpected` deliveries in one chaos run.
- **Each router poll batch** — the delivery records, their pending status
  records and the consumed offset commit — goes through one
  `kgo.GroupTransactSession`. A crash can neither lose a pending status nor
  reprocess a batch into duplicates from the consumer's point of view.

The router is the one client that is both a group member and a transactional
producer, which is why the session and transaction timeouts are read as a
pair: a transaction must not outlive the membership carrying it.

## Repository layout

```text
cmd/<service>/        one binary per service
internal/
  cloudevents/        CloudEvents 1.0 parse/validate/serialise (binary, structured, batch)
  registry/           event type + subscription models and the in-memory index
  store/              PostgreSQL: control data and the inbox
  filter/             CEL filters (attribute and data) with the equality index
  project/            field allowlist projection
  delivery/           the delivery message and the (partition, offset) sequence
  status/             delivery status records and the status producer
  kafkautil/          topic names, topic creation, record headers
  chstore/            ClickHouse connection and status table
  chquery/            status query SQL
  auth/               JWT validation, scopes, dev token minting
  httpx/              JSON responses, access log
  config/             environment access
  routing/, dispatch/, sse/, replay/, harness/   service-internal logic
migrations/           embedded PostgreSQL migrations
docker-compose.yaml   Kafka (KRaft), PostgreSQL 16, ClickHouse 24.8
Dockerfile            one distroless image containing every binary
charts/
  pubsub-infra/       Kafka + PostgreSQL + ClickHouse, one instance each
  pubsub/             the eight services (3 replicas), the admin hook, PDBs
website/              this documentation site
```
