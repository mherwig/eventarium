# Event Platform – Specification (Draft)

Oct 21, 2026 · @Mike H.

A self-operated, fully open-source platform that accepts CloudEvents over REST, filters and reshapes them per subscription, and delivers them with tracked, guaranteed delivery over webhooks or SSE streams. It is built in Go on Apache Kafka and PostgreSQL, with ClickHouse for delivery status.

## 1. Goals, scope and key decisions

**In scope:** publish API, subscription management, routing (filters, projection), webhook and SSE delivery, retries and circuit breaking, rate limiting, an inbox for offline consumers, delivery status and its query API, a replay API, and an end-to-end load-test setup.

**Out of scope for now:** the BFF and any UI or aggregated dashboards. The only status requirement is that the status of events can be queried through an API.

Key decisions so far:

- Custom implementation in Go on open-source infrastructure (Kafka, PostgreSQL, ClickHouse). No off-the-shelf webhook or realtime products (Convoy, Svix, Centrifugo): they are open-core, and their models do not fit the ordering, per-subscription policy and status requirements.
- PostgreSQL (the existing managed service) for the inbox and control data. Cassandra is not needed.
- Event format is CloudEvents. The data field may be arbitrary text; filters and projections apply only where the data supports them.
- Consumers choose strict ordering or parallel delivery per subscription.

## 2. Requirements

### 2.1 Functional

| ID | Requirement |
| --- | --- |
| F1 | Events are published through a REST API as CloudEvents (HTTP binary and structured mode, plus batch mode) |
| F2 | The CloudEvents data field may be any text (or binary); it does not have to be JSON |
| F3 | Subscriptions are registered dynamically through an API (channel, callback URL, filters, projection, delivery policy) |
| F4 | Delivery to a callback URL (webhook) |
| F5 | Delivery as an SSE stream that the consumer opens |
| F6 | Filters per subscription: on CloudEvents attributes for every event, on the data field only if it is JSON; conditions can be very complex |
| F7 | Projection: only selected fields of the data are sent, all others are removed (JSON data only) |
| F8 | Guaranteed delivery (at-least-once); ordering depends on the delivery mode |
| F9 | The consumer chooses the delivery mode per subscription: ordered (default), key\_ordered or unordered |
| F10 | Up to 3 concurrent SSE connections per subscription for consumer failover and parallelism |
| F11 | Retry with backoff and a circuit breaker |
| F12 | Configurable drain rate per subscription (redelivery after an outage) |
| F13 | Configurable rate limit per subscription for webhook delivery |
| F14 | Status of every individual delivery (for example pending, delivered, dropped), queryable through an API for up to 7 days |
| F15 | Replay through an API: redeliver events for a subscription, selected by status or by time range |
| F16 | Retention per event type through a few retention classes, maximum 7 days |
| F17 | SSE clients and webhook endpoints may be offline for about 4 days and then receive all missed events within retention |
| F18 | Thousands of event types with hundreds of subscriptions each |
| F19 | Publishing the same message again is allowed; every publish is a new event internally with its own event reference (no deduplication) |
| F20 | Events of unknown event types are rejected |
| F21 | Every API call carries a JWT; the owner reference comes from the token and scopes all access |

### 2.2 Non-functional

| ID | Requirement |
| --- | --- |
| N1 | Throughput: at least 1,000 published events/s; on average 10 matching subscribers per event, so about 10,000 deliveries/s |
| N2 | Channel mix (estimate): about 60 % SSE (6,000/s) and 40 % webhook (4,000/s) |
| N3 | Latency from publish to receipt below 1 s, target a few hundred ms (first delivery attempt; retries and rate-limited deliveries excluded) |
| N4 | Concurrent SSE connections: potentially thousands; how realistic this is remains unknown |
| N5 | Data minimisation: keep data only as long as needed (retention classes) |
| N6 | Licensing: 100 % open source, no freemium or open-core components, no components with a high rug-pull risk |
| N7 | Implementation: custom Go services on open-source infrastructure |
| N8 | Everything is tested end to end under load; building the load-test setup is part of the project |

## 3. Architecture overview

```
Publisher
   │ REST (CloudEvents)
   ▼
Publish API (validation, event type registry, expires_at)
   │
   ▼
Kafka: events-<class> (live, short, day, week)
   │
   ▼
Router (attribute and data filters, ordering key, projection)
   │ one message per matching subscription
   ▼
Kafka: deliveries (key = subscription ID)
   │
   ├─► Webhook dispatcher ──► callback URLs
   │        │ circuit breaker open / endpoint offline / rate-limit overflow
   │        ▼
   │      Inbox (PostgreSQL)
   │
   └─► SSE delivery ──► Kafka: sse-live ──► SSE gateways ──► clients
            │ no client connected
            ▼
          Inbox (PostgreSQL)

All stages ──► Kafka: delivery-status ──► ClickHouse ──► Status API
Replay API ──► Replay service ──► re-reads events from Kafka ──► deliveries
```

## 4. Components

| Component | Role | Technology | Build or use |
| --- | --- | --- | --- |
| Publish API | Accepts CloudEvents, validates, stamps expires\_at | Go | build |
| Registry and config | Event types, retention classes, subscriptions, policies | PostgreSQL, distributed through a compacted Kafka topic | build |
| Event log | Durable transport and buffering | Apache Kafka | use |
| Router | Filters, ordering key, projection, fan-out | Go, cel-go, franz-go | build |
| Webhook dispatcher | HTTP delivery, retry, circuit breaker, rate limit, drain | Go | build |
| SSE delivery and gateway | Connections, presence, connection assignment, acks | Go | build |
| Inbox | Parked deliveries for lagging or offline subscriptions | PostgreSQL (managed, partitioned) | use |
| Status pipeline | Append-only status events | Kafka, ClickHouse | use |
| Status API | Query delivery status | Go | build |
| Replay service | Executes replay jobs | Go | build |
| Load-test harness | Load generation, simulated consumers, verification, fault injection | Go plus open-source tools | build |

## 5. Event model (CloudEvents)

- **Format:** CloudEvents. Required attributes are id, source, specversion and type; optional ones include datacontenttype, subject, time, dataschema and extension attributes. The publish API accepts the HTTP binary and structured modes and a batch mode for throughput.
- **Event type:** the CloudEvents type attribute is the key into the registry (retention class, expected content type, ordering key definition).
- **Data:** JSON, plain text or binary. It is stored and delivered unchanged unless a projection applies.
- **Identity:** publishing the same message again (same source and id) is allowed. The platform treats every publish as a new event with its own unique event reference and does not deduplicate on (source, id). The delivery ID is the event reference plus the subscription ID. Delivery is at-least-once, so consumers deduplicate on the event reference or delivery ID from the transport metadata, not on (source, id).
- **Delivery metadata** (event reference, delivery ID, attempt, replay ID, sequence) travels in transport headers or SSE fields, not inside the CloudEvent.
- **Delivery formats:** webhooks use the CloudEvents HTTP binding in binary mode (default) or structured mode, set per subscription. SSE messages carry the CloudEvent in JSON format; binary data is base64-encoded.
- **Ordering key:** taken from an extension attribute (for example the partitioning extension's partitionkey), from subject, or from an expression on JSON data defined in the registry. The default is the Kafka partition key used at publish. Ordering guarantees hold between events of the same retention class (section 12).
- **Size limit:** to be defined; Kafka's default message limit is about 1 MB.

What works for which kind of data:

| Data kind | Attribute filters | Data filters | Field projection |
| --- | --- | --- | --- |
| JSON (application/json or +json) | yes | yes | yes |
| Text | yes | no | no |
| Binary | yes | no | no |

- The registry declares the expected content type per event type. The publish API rejects invalid JSON for JSON types.
- A subscription that defines data filters or projection is rejected for event types that are not JSON.
- Attribute filters work on every event.

## 6. Components in detail

### 6.1 Publish API and registry

- Validates the JWT, the required attributes and the size, then looks the event type up in the registry. Unknown event types are rejected with a client error (for example 422); there is no auto-registration.
- Stamps an immutable expires\_at (ingestion time plus retention class), an ingestion timestamp and the owner reference taken from the JWT, never from the event body. Later class changes affect only new events.
- The registry lives in memory and is refreshed from a compacted config topic, so a publish needs no database call.
- The Kafka partition key chosen at publish (for example the entity ID) preserves per-entity order within one retention class. Each event is written once, to the Kafka topic of its retention class (events-live, events-short, events-day, events-week). The registry supplies the class, and new event types default to short.
- The response contains the platform's event reference, a unique ID per publish, which is used for status queries. Publishing the same message again creates a new event with a new reference. Publishes are not deduplicated, so a publisher that retries after a timeout may create a duplicate.

### 6.2 Router

- Stateless and horizontally scalable. It consumes all per-class events topics. It holds an in-memory index of event type to precompiled filters, updated from the config topic.
- Filter language: CEL, sandboxed and precompiled, with cost limits and timeouts. Equality predicates (for example tenant) are indexed up front so that not every filter sees every event.
- The ordering key is computed before the projection, because the projection may remove the field, and is passed as a header.
- Projection: a field allowlist per subscription (fast and safe) or a transformation expression. JSON data only.
- Output: one delivery message per matching subscription, keyed by subscription ID. It carries the event's retention class, Kafka partition and offset, which the replay service needs.

### 6.3 Delivery topic and sequence

- Partitioned by subscription ID. Each subscription sits on exactly one partition with exactly one owner.
- The sequence number is (partition, offset). This requires a fixed partition count that is never changed later.
- Retention is an operational parameter (guideline 24 to 48 hours), because slow subscriptions move to the inbox.

### 6.4 Webhook dispatcher

- Consumes the delivery topic. Its state is the Kafka offset plus circuit-breaker and rate-limit state per subscription.
- Concurrency follows the delivery mode through max\_in\_flight: 1 for ordered, N for key\_ordered (serialised per key) or unordered.
- Retry with exponential backoff and jitter, plus a hard request timeout so that slow endpoints do not tie up workers.
- Circuit breaker: opens after N failures; the probe interval is capped (for example 5 to 15 minutes, never days).
- **Rate limit:** a token bucket per subscription (rate\_limit\_per\_s and burst), kept in memory by the partition owner, so no shared counter is needed. The effective rate is the lower of the limit and what max\_in\_flight allows.
- Rate-limit overflow: each subscription has a bounded in-memory queue. When it overflows under sustained load, the excess is parked in the inbox and delivered at drain\_rate\_per\_s, so one throttled subscription never blocks others on the same partition. The status shows pending with reason rate\_limited.
- An HTTP 429 response with Retry-After pauses the subscription for that time and does not count as a circuit-breaker failure.
- Limits across several subscriptions (per target host or tenant) are out of scope for the first version; they would need a shared counter.
- Security items planned from the start: SSRF protection (no delivery to internal addresses), HMAC signatures and secret rotation, redirect policy, payload limits, DNS and TLS behaviour, connection pooling.

### 6.5 SSE delivery and gateway

- A client connects with subscription ID, consumer ID and token. Up to 3 connections per subscription (max\_connections).
- **Presence:** gateways publish connect and disconnect events to a compacted topic. The delivery stage reads it and decides per subscription whether to deliver live or park in the inbox.
- **Live path:** the SSE delivery worker writes to the sse-live topic with a target connection. Every gateway node reads the whole topic (own consumer group per node, starting at latest) and drops messages without a local connection. At about 6,000 deliveries/s and 1 KB this is about 6 MB/s per node. A routing layer (for example Valkey or NATS) becomes necessary only when the node count grows a lot.
- **Connection hygiene:** heartbeats every 15 to 30 s, write timeouts, a maximum lifetime or end at token expiry, limits per subscription and per IP, reconnect jitter. No automatic disconnect on inactivity, because it would push events into the inbox and break the latency target.
- **Acks:** cumulative for ordered, per event (batched every few hundred ms) for key\_ordered and unordered. Unacknowledged events go to another connection after an ack timeout or connection loss.

### 6.6 Inbox (PostgreSQL)

- Managed PostgreSQL with partitions by the day of expires\_at, so an entire partition is dropped at expiry whatever the retention class (no mass deletes, no vacuum pressure). Check whether the provider offers pg\_partman; otherwise create partitions with a job.
- Rows hold subscription ID, sequence, the already projected payload, expires\_at and delivery state. Index on (subscription\_id, sequence). Writes use batch inserts or COPY.
- Acknowledged rows are not deleted one by one. A small per-subscription watermark table is updated in batches.
- Only deliveries of subscriptions that are offline or lagging land here. At 5 to 20 % offline share that is about 500 to 2,000 writes/s.
- Handover back to the hot path: the inbox is drained in order while new events are appended, and the subscription switches back only when it is empty. This runs in a transaction and is specified as a state machine.
- Use a connection pooler. Use only standard PostgreSQL features and no provider-specific extensions, to stay portable (N6).

### 6.7 Authentication and ownership

- Every API call (publish, subscriptions, SSE stream, status, replay) carries a JWT as bearer token. The signature is verified against the issuer's public keys (JWKS); issuer, audience and expiry are validated.
- The token carries the owner reference (claim name to be defined) and the permissions (for example publish, subscribe, status read, replay).
- The platform stamps the owner reference on the event at publish and stores it with deliveries, status records, subscriptions and replay jobs. Callers cannot set it through the event body or request parameters.
- Authorisation compares the owner reference in the token with the owner of the addressed resource. A caller sees and changes only data that belongs to its own owner reference.
- SSE connections use the same token. A connection ends when the token expires, and the client reconnects with a fresh token.
- Token issuance is outside the platform. Any standards-based OIDC identity provider works.
- Open: whether routing is restricted by owner (subscriptions receive only events of the same owner) or whether cross-owner subscriptions are allowed through explicit grants.

## 7. Delivery modes

The consumer chooses the mode per subscription; all connections of a subscription share it.

| Mode | Behaviour | Ordering | Parallelism |
| --- | --- | --- | --- |
| ordered (default) | One active connection, others on standby with failover | strict per subscription | no |
| key\_ordered | Spread over up to 3 connections; the same key always goes to the same connection | per ordering key | yes |
| unordered | Spread without ordering | none | yes, maximum |

- The delivery stage, as partition owner, assigns events to connections by hash (for example rendezvous hashing).
- On rebalancing, a key moves to another connection only after its in-flight events are acknowledged.
- Failover for ordered: a standby is promoted and redelivery starts from the last cumulative ack. The switch takes seconds.
- Changing the mode at runtime: stop delivery, wait for open acks, then switch.
- Webhooks use the same model through max\_in\_flight.

### Delivery policy per subscription

| Field | Meaning |
| --- | --- |
| delivery\_mode | ordered, key\_ordered or unordered |
| max\_connections | SSE connections, default 3 |
| max\_in\_flight | Concurrent deliveries (webhook) |
| rate\_limit\_per\_s and burst | Steady-state rate limit (webhook) |
| drain\_rate\_per\_s | Rate while draining the inbox after an outage |
| ack\_timeout\_ms and ack\_window | Acknowledgement deadline and window size |
| cb\_failure\_threshold and probe\_interval\_s | Circuit breaker |
| request\_timeout\_ms | Timeout per webhook request |
| backlog\_max | Upper bound of parked events per subscription |
| webhook\_format | binary or structured |

## 8. Failure handling

- Short outage: retry with backoff; order is preserved.
- Longer webhook outage: the circuit breaker opens, the subscription is paused, new events are parked in the inbox, and the Kafka partition is free again. A successful probe starts draining the inbox at drain\_rate\_per\_s.
- Longer SSE outage: without a connection, events are parked. After reconnecting, the client fetches the backlog while buffering live events, merges by sequence and drops duplicates by event reference.
- Internal component failure: Kafka offsets hold the state. A restart produces duplicates, which are handled idempotently by event reference plus subscription ID.
- After expires\_at, the delivery is marked expired.

## 9. Delivery status

- Every stage emits status events asynchronously to the Kafka topic delivery-status, off the hot path. The idempotency key is delivery ID, replay ID (if any), attempt and status.
- ClickHouse stores them append-only, partitioned by day and retention class, so whole partitions expire cheaply. A materialised view derives the current status; the history stays as an audit trail.
- Records hold IDs, timestamps, HTTP code, latency, connection ID and the event's retention class, Kafka partition and offset. They never hold payloads.
- The pending status is written in the same Kafka transaction as the delivery message, so a crash cannot lose it.
- Status data lags by about 1 to 5 seconds because of batch inserts.

| Status | Meaning |
| --- | --- |
| pending | The router created the delivery, or it is parked in the inbox (reason is recorded, for example rate\_limited) |
| in\_flight | A delivery attempt is running |
| retrying | The attempt failed; the next one is scheduled |
| delivered | Webhook: 2xx received. SSE: acknowledged by a connection |
| failed or dead\_lettered | Retries exhausted |
| paused | Circuit breaker open |
| expired | Retention ran out before delivery |
| dropped | Discarded, for example because backlog\_max was exceeded |

For SSE, delivered means more than written to the connection only if the client sends acks. Without acks the status should honestly be called sent.

## 10. Status API

The BFF and any UI are out of scope. This API is the one required read interface.

| Endpoint | Purpose |
| --- | --- |
| GET /deliveries/{delivery\_id} | One delivery with its full history (attempts, codes, latencies, connection) |
| GET /events/{event\_ref}/deliveries | All deliveries of one event across subscriptions |
| GET /subscriptions/{id}/deliveries | Deliveries of a subscription, filterable by status, event type and time range, with keyset pagination |
| GET /subscriptions/{id}/status-summary | Counts per status, backlog, mode, active connections (optional in the first version) |

- Access is scoped by the owner reference in the caller's JWT (section 6.7): a caller sees only data that belongs to its own owner reference.
- Data is available until expires\_at, at most 7 days.
- Results are eventually consistent (see section 9).

## 11. Replay API

Purpose: redeliver events to a subscription after a failure or a consumer-side problem.

| Endpoint | Purpose |
| --- | --- |
| POST /subscriptions/{id}/replays | Creates an asynchronous replay job (returns 202 and a replay ID). Supports dry\_run and an Idempotency-Key header |
| GET /replays/{replay\_id} | Progress: matched, enqueued, delivered, failed, not\_replayable |
| POST /replays/{replay\_id}/cancel | Stops a running job |
| GET /subscriptions/{id}/replays | Lists the jobs of a subscription |

- **Selector** (at least one): statuses (for example failed, dead\_lettered, dropped, delivered), a time range (by ingestion time), event types, explicit event references.
- **How it works:** the replay service resolves the selector against the status store, re-reads the original events from the events topic of their retention class by the stored class, partition and offset, applies the subscription's current projection and delivery policy, and writes new deliveries marked with the replay ID.
- **Retention limit:** only events within retention can be replayed. Events past expires\_at, or no longer in the events topic, are counted as not\_replayable and left untouched. dry\_run reports these counts up front.
- **Delivery path:** replayed deliveries use the normal path, including rate limit, drain rate, circuit breaker and delivery mode. Transport metadata flags them, and each produces new status records with the replay ID. The earlier history stays unchanged.
- **Ordering:** replayed events keep their original relative order. No ordering is guaranteed relative to live events. An exclusive mode that pauses live delivery during a replay is a possible later extension.
- **Consumers** see the same event reference again, flagged as a replay, and must be idempotent.
- **Safeguards:** one active job per subscription, a maximum number of events per job, authorisation and an audit trail.
- **First version:** replays existing deliveries only. A backfill for events that never matched the subscription (for example a newly created one) is an open decision.

## 12. Retention classes

| Class | Retention | Intended for |
| --- | --- | --- |
| live | none (hot path only, no inbox) | live events without redelivery duty |
| short | 1 hour | short-lived signals |
| day | 1 day | default for non-critical events |
| week | 7 days | events that must bridge a long weekend |

- expires\_at is stamped at publish. Inbox rows, status data and replay eligibility follow it.
- Replay needs events to stay readable in Kafka. Each class has its own events topic with that retention (events-live, events-short, events-day, events-week), so every event is written once and stored only as long as its class requires. Replay reads from the topic of the event's class. Each topic keeps at least an operational floor (for example a few hours) so the router can survive an outage, and its segment.ms must be small enough for deletion to follow the retention. Ordering holds only between events of the same class, because there is no order across topics. Event types that must stay ordered relative to each other must share a class; the registry should enforce this through an ordering group.
- The delivery topic keeps 24 to 48 hours.
- A class of 3 days would not cover the 4-day offline requirement. An intermediate class needs at least 5 days.
- The default class for new event types is short, so data is kept longer only when its owner explicitly asks for it.

## 13. Licence assessment (N6)

| Component | Assessment | Decision |
| --- | --- | --- |
| Apache Kafka | Apache 2.0, ASF governance | used |
| PostgreSQL (existing managed service) | Permissive, community-governed | used |
| ClickHouse | Apache 2.0, but a single vendor with cloud-only features; medium risk | used; fallback to a different store if the licence changes |
| Go libraries (cel-go, franz-go) | Permissive | used |
| Redpanda | BSL, source-available | not used |
| ScyllaDB | According to the vendor, 6.2 was the last AGPL release; later versions are source-available only | not used |
| Apache Cassandra | Apache 2.0, ASF | considered, not needed |
| Centrifugo | Open-core: OSS edition plus commercial PRO edition | not used; custom SSE gateway |
| Convoy, Svix | Open-core with premium or enterprise features | not used; custom dispatcher |
| Redis | Licence change in 2024 | not used; Valkey (BSD) if needed |

Only ScyllaDB and Centrifugo were re-checked recently. The other licence statements come from general knowledge and must be verified before the decision is final. Test tooling should meet the same standard.

## 14. Sizing (assumptions: 2 KB per event, 1 KB per delivery)

| Stage | Load or size |
| --- | --- |
| Kafka events | 1,000/s; about 1.2 TB if every event were in the 7-day class (before replication and compression); with short as the default class, far less |
| Kafka deliveries | 10,000/s; about 6 TB at 7 days, far less at 24 to 48 h retention |
| Router | Up to a few hundred filter evaluations per event; about 0.3 million/s without an index; a few cores with CEL |
| Status events | About 30,000 rows/s, about 2.6 billion per day; roughly tens to low hundreds of GB per day compressed |
| Inbox | 500 to 2,000 writes/s; about 170 GB to 1.2 TB depending on offline share and retention |
| SSE gateway | Thousands of connections; roughly 10 to 50 KB per connection |

All values are orders of magnitude and must be checked against real payload sizes and measurements.

## 15. Testing and load testing

### 15.1 Principles

- Every requirement (F1 to F18, N1 to N4) has at least one automated end-to-end test that runs under load.
- The harness is built first, together with the first spike, and grows with the system.
- An independent oracle computes the expected results. The system never verifies itself.

### 15.2 Harness components

| Component | Purpose |
| --- | --- |
| Publisher generator | Open-loop load at a configurable rate with a realistic mix of event types, JSON, text and binary data, payload sizes and ordering keys; embeds send timestamps and per-key sequence numbers |
| Subscription generator | Creates thousands of event types and hundreds of subscriptions per type with realistic filters, projections, modes and policies |
| Simulated webhook receivers | Configurable latency, error rates, 429 with Retry-After, timeouts and offline windows; record every received event |
| Simulated SSE clients | Up to 3 connections each, ack behaviour, slow consumers, reconnect patterns, long offline periods |
| Oracle and verifier | Computes the expected deliveries per subscription from the published events and the filters, then compares them with what receivers got and what the Status API reports |
| Fault injection | Kills and restarts of brokers, workers and gateways; network faults and delays; PostgreSQL failover; partition rebalances |
| Observability | Per-stage latency from embedded timestamps, lag, queue depths, resource usage |

### 15.3 Scenarios

- **Baseline:** 1,000 events/s with about 10 matches per event (about 10,000 deliveries/s), 60/40 SSE and webhook, mixed delivery modes.
- **Ramp and spike:** up to 2 to 3 times the baseline and back; check recovery and lag.
- **Soak:** at least 24 hours at baseline; check leaks, growth and database health.
- **Slow and dead endpoints:** a share of endpoints slow, failing or offline; verify that other subscriptions are not blocked.
- **Long offline periods:** consumers offline for days. Time-scaled retention for quick runs, plus a real multi-day run before release (4 days offline, then catch-up).
- **Rate limit, drain and circuit breaker** under load.
- **Failover and modes:** SSE connection loss, standby promotion, key rebalancing and mode switching under load.
- **Replay under load:** by status and by time range, including not\_replayable handling.
- **Retention expiry:** events reaching expires\_at are marked expired and storage is reclaimed.
- **Data kinds:** JSON, text and binary events; filters and projection only where defined.
- **Status API:** accuracy, lag and pagination correctness under load.
- **Infrastructure faults:** Kafka broker loss, PostgreSQL failover, ClickHouse outage (status events are buffered in Kafka).

### 15.4 Pass criteria

- No loss within retention.
- No order violations: strict per subscription for ordered, per key for key\_ordered.
- Duplicates only after injected failures, and always identifiable by event reference.
- p99 from publish to first delivery attempt below 1 s at baseline load, target a few hundred ms.
- Once the system is idle, status records match the oracle for 100 % of deliveries.
- No unbounded growth of memory, goroutines, lag or storage in the soak test.
- Failover and recovery times are measured and documented.

### 15.5 Execution

- The harness is written in Go. Fault injection uses open-source tools such as Toxiproxy (verify the licence) plus process kills. Kafka's own performance tools give broker-level baselines.
- Full-scale runs use a dedicated, production-like environment; a scaled-down variant runs in CI for each change.

## 16. Risks and open decisions

- The hot/cold handover and failover with ordering are the most error-prone parts. Specify them as state machines with invariants and test them with fault injection.
- Backlog limit per subscription: without one, storage grows without bound during outages; with one, dropped appears, which conflicts with guaranteed delivery.
- The offline share of subscriptions is unknown. It decides the inbox size and the load on the managed PostgreSQL (IOPS, WAL throughput, failover behaviour).
- Meaning of delivered for SSE (ack protocol).
- Ordering key: definition, default and behaviour for event types without a key.
- Fixed partition count for the delivery topic, because the sequence depends on it.
- Ordering across retention classes: events in different class topics have no relative order. Define the ordering scope (same class or ordering group), enforce it in the registry and document it for consumers.
- Replay: backfill scope, ordering relative to live events, whether to re-evaluate filters.
- Maximum event size, and whether publishers need an optional Idempotency-Key header for safe retries (publishes are not deduplicated by design).
- Filter language: CEL (proposed) or CloudEvents SQL for attribute filters.
- Webhook security (SSRF, signatures, redirects) is its own block of work.
- JWT claim names and permissions, the identity provider setup, and whether routing is restricted by owner (section 6.7).
- ClickHouse licence risk and the cost of operating one more component.
- Build effort: AI helps with boilerplate and tests; correctness under distribution, load testing and operations remain the team's work.
- Load and distribution figures are estimates and must be measured early.

## 17. Next steps

1. Build the load-test harness first, together with the spike.
2. Spike (2 to 3 weeks): publish, Kafka, router, SSE gateway, dispatcher and status in ClickHouse, with timestamps in events for per-stage latency.
3. Run the baseline scenario (1,000 events/s, about 10 matches per event) and compare against the pass criteria.
4. Specify the state machines for the hot/cold handover, failover, mode changes and replay, and verify them with the fault-injection tests.
5. Decide the open points from section 16, starting with offline share, backlog limit, ack semantics for SSE and replay scope.
6. Define the Status API and Replay API contracts and their authentication.
