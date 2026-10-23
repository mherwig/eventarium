# Spike contracts

This document fixes the interfaces between the services of the spike so that
they can be built independently. The full design is in `spec.md`; this file only
records what is already decided in code and what each service must implement.

## Repo layout

- Module name: `pubsub`. Go 1.26. Single module, one binary per service under
  `cmd/<name>`, shared code under `internal/<package>`.
- Services: `admin`, `minttoken`, `publish-api`, `registry-api`, `router`,
  `dispatcher`, `sse-gateway`, `status-writer`, `status-api`, `harness`.
- Logging: `log/slog` to stderr. HTTP helpers: `internal/httpx`.
- Do not add third-party dependencies without asking. The allowed set is:
  franz-go (Kafka), cel-go (`cel.dev/cel-go/cel`), pgx v5, clickhouse-go v2,
  golang-jwt v5, google/uuid.

## Environment variables

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | everything |
| `PG_DSN` | `postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable` | publish-api, registry-api, router, dispatcher, sse-gateway |
| `CH_DSN` | `clickhouse://pubsub:pubsub@localhost:9000/pubsub` | status-writer, status-api |
| `JWT_SECRET` | `dev-secret` | every API service, harness |
| `JWT_ISSUER` | `dev` | same |
| `JWT_AUDIENCE` | `pubsub` | same |
| `HTTP_ADDR` | `:8080` (publish-api, registry-api), `:8081` (status-api), `:8082` (sse-gateway) | the API services |
| `WEBHOOK_SIGNING_SECRET` | unset (no signature) | dispatcher |
| `MAX_EVENT_BYTES` | `1048576` | publish-api |
| `LOG_LEVEL` | `info` | everything |

Use `internal/config` for all reads. Never read the environment directly.

## Kafka topics and record shapes

Created by `bin/admin topics` (see `kafkautil.Specs()`). Retention classes:
`live`, `short`, `day`, `week`.

| Topic | Key | Value | Notes |
| --- | --- | --- | --- |
| `events-<class>` | partition key (string) | CloudEvent as structured JSON | one topic per retention class |
| `deliveries` | subscription ID | `delivery.Message` JSON | fixed 12 partitions; the consumer assigns the sequence |
| `delivery-status` | delivery ID | `status.Record` JSON | append-only, off the hot path |
| `sse-presence` | `<subscription_id>/<connection_id>` | presence JSON, tombstone on disconnect | compacted |
| `sse-live` | subscription ID | delivery JSON with target connection | written by the SSE delivery stage |

Platform metadata travels in Kafka headers, never inside the CloudEvent
(section 5). Header names are constants in `internal/kafkautil`:
`pubsub-ref` (event reference), `pubsub-owner`, `pubsub-class`,
`pubsub-expires-at` (unix ms), `pubsub-ingested-at` (unix ms),
`pubsub-partition-key`.

An event record on `events-<class>` carries all of the above, set by the
publish API. A delivery record additionally carries `pubsub-channel`.

## Shared packages (already implemented, do not change without saying so)

- `internal/cloudevents` – CloudEvents 1.0: `Read(r, maxBytes)` parses binary,
  structured and batch HTTP bodies; `Parse`/`MarshalJSON` for structured JSON;
  `SetHeaders` for the binary binding; `IsJSON`, `JSON`, `Extension`.
- `internal/delivery` – `Message` (the value of the `deliveries` topic and the
  inbox payload) and `Seq{Partition, Offset}`.
- `internal/status` – `Record` and `Producer` for the status topic. Status
  constants: pending, in_flight, retrying, delivered, sent, failed,
  dead_lettered, paused, expired, dropped.
- `internal/registry` – `EventType`, `Subscription`, `Entry` (subscription with
  compiled filters) and the in-memory `Registry` refreshed from PostgreSQL.
  Spike deviation: the registry polls PostgreSQL (default every 2 s) instead of
  reading a compacted config topic (section 6.1).
- `internal/store` – PostgreSQL access: control data and the inbox
  (`Park`, `FetchParked`, `AckParked`, `CountParked`, `DeleteExpiredParked`,
  `EnsureInboxPartitions`).
- `internal/filter` – CEL filters. `CompileAttribute` (variables: `attributes`,
  `id`, `source`, `type`, `subject`) and `CompileData` (variable: `data`).
  `Matches(attr, data, attrs, json)` evaluates both, with the equality index of
  section 6.2 applied first.
- `internal/project` – `Apply(data, allowlist)` keeps the listed dotted paths.
- `internal/auth` – `Validator` (HS256 in the spike, JWKS later), `Authorize(w,
  r, scope)` writes the 401/403 itself. Scopes: publish, subscribe,
  status:read, replay. Owner comes from the `owner` claim, falling back to
  `sub`.
- `internal/kafkautil` – topic names, `Specs()`, `NewClient`, `EnsureTopics`,
  header helpers `HSet`/`HGet`.
- `internal/chstore` – ClickHouse connection and the `delivery_status` table.
- `migrations` – embedded PostgreSQL migrations, applied by `bin/admin migrate`.

## Service contracts

### publish-api (port 8080)

- `POST /v1/events` – binary, structured or batch mode (scope `publish`).
- Validates the CloudEvent, looks the type up in the registry by
  `(token owner, type)`. Unknown type or invalid JSON for a JSON type: 422.
- Stamps `event_ref` (uuid), `ingested_at`, `expires_at` from the retention
  class, and the owner from the token. `live` means `expires_at = ingested_at`
  (never parked, no replay duty).
- Partition key: extension `partitionkey`, else `subject`, else `source/id`.
- Writes to `events-<class>` with the headers above; responds 201 with
  `{"event_ref": ..., "retention_class": ..., "expires_at": ...}` (a list for
  batches). Republishing is allowed and creates a new reference (F19).
- `GET /healthz` – no auth.

### registry-api (port 8080)

- `POST/GET/DELETE /v1/event-types` and `/v1/event-types/{name}` (scope
  `publish` for writes, owner scoped).
- `POST/GET/DELETE /v1/subscriptions` and `/v1/subscriptions/{id}` (scope
  `subscribe`).
- Rejects subscriptions with data filters or a projection for non-JSON event
  types (section 5), validates CEL and projection paths, applies the defaults
  of the delivery policy table (section 7).

### router

- Consumer group `router` over all four `events-*` topics.
- For each event: match subscriptions of the same owner and type, evaluate the
  attribute filter and the JSON data filter, compute the ordering key before
  projection, apply the projection, then produce one delivery record per
  matching subscription keyed by subscription ID, plus one pending
  `status.Record`.
- The delivery record and its pending status are written in one Kafka
  transaction together with the offset commit (section 9), using
  `kgo.NewGroupTransactSession`.
- Ordering: one source partition is processed serially; the key of the
  delivery record is the subscription ID, so per-key order survives because one
  router instance owns the source partition.

### dispatcher

- Consumer group `dispatcher` over `deliveries`, webhook subscriptions only.
- Delivers to the callback URL in the CloudEvents binary or structured binding
  (per subscription), with `X-Pubsub-Signature: sha256=<hex>` when
  `WEBHOOK_SIGNING_SECRET` is set, plus the delivery metadata headers
  `pubsub-delivery-id`, `pubsub-event-ref`, `pubsub-attempt`, `pubsub-seq`.
- Retry with exponential backoff and jitter, a circuit breaker, a token bucket
  rate limit, `max_in_flight` per delivery mode (section 7).
- On circuit-open, rate-limit overflow or queue overflow: park in the inbox and
  emit pending with the reason; live-class events are never parked.
- Drains the inbox at `drain_rate_per_s` when the subscription is healthy
  again; while draining, new events go to the inbox so that order holds.
- Emits in_flight, retrying, delivered, dead_lettered, paused, dropped.

### sse-gateway (port 8082)

- `GET /sse?subscription_id=..&consumer_id=..` (scope `subscribe`), heartbeats,
  at most `max_connections` per subscription.
- Publishes presence to `sse-presence` (tombstone on disconnect).
- Delivery stage: consumer group `sse-delivery` over `deliveries`, sse
  subscriptions only. With a live connection it writes to `sse-live` targeted
  at the owning node, otherwise it parks in the inbox.
- Live consumer: one consumer group per node, starts at latest, drops messages
  without a local connection.
- `POST /sse/ack` acknowledges deliveries (Last-Event-ID or body); only an ack
  turns `sent` into `delivered` (section 9).

### status-writer

- Consumer group `status-writer` over `delivery-status`, batch inserts into
  ClickHouse (`internal/chstore`), every 1 s or 10k rows.

### status-api (port 8081)

- The four read endpoints of section 10, owner scoped from the token (scope
  `status:read`), keyset pagination on the subscription list.
- Current status is `argMax(status, ts)` per delivery; the history stays
  queryable.

### harness

- `harness pub` – open-loop publisher at a configurable rate.
- `harness recv` – simulated webhook receiver with configurable latency, error
  rate and offline windows.
- `harness sse` – simulated SSE consumer.
- `harness verify` – the oracle: compares what receivers got with what the
  Status API reports and with the expected match set (section 15.1).

## Deviations from the spec, on purpose

- Registry distribution: PostgreSQL polling instead of a compacted Kafka topic.
- SSE multi-node routing: presence carries the node ID, so the targeted
  `sse-live` message reaches the node that holds the connection. No message is
  dropped because the connection lives elsewhere.
- Inbox deletion: batched deletes instead of a watermark table.
- Replay service: not part of the spike (section 17, step 4 comes later).
- Authentication: HS256 with a shared secret instead of JWKS.
