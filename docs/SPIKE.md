# Spike contracts

This document fixes the interfaces between the services of the spike so that
they can be built independently. The full design is in `spec.md`; this file only
records what is already decided in code and what each service must implement.

## Repo layout

- Module name: `pubsub`. Go 1.26. Single module, one binary per service under
  `cmd/<name>`, shared code under `internal/<package>`.
- Services: `admin`, `minttoken`, `publish-api`, `registry-api`, `router`,
  `dispatcher`, `sse-gateway`, `status-writer`, `status-api`, `replay-api`,
  `harness`.
- Logging: `log/slog` to stderr. HTTP helpers: `internal/httpx`.
- Do not add third-party dependencies without asking. The allowed set is:
  franz-go (Kafka), cel-go (`cel.dev/cel-go/cel`), pgx v5, clickhouse-go v2,
  golang-jwt v5, google/uuid.

## Environment variables

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | everything |
| `PG_DSN` | `postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable` | publish-api, registry-api, router, dispatcher, sse-gateway, replay-api |
| `CH_DSN` | `clickhouse://pubsub:pubsub@localhost:9000/pubsub` | status-writer, status-api, replay-api |
| `JWT_SECRET` | `dev-secret` | every API service, harness |
| `JWT_ISSUER` | `dev` | same |
| `JWT_AUDIENCE` | `pubsub` | same |
| `HTTP_ADDR` | `:8080` (publish-api, registry-api), `:8081` (status-api), `:8082` (sse-gateway), `:8084` (replay-api); `:8085` (router), `:8086` (dispatcher), `:8087` (status-writer), health only | every service |
| `WEBHOOK_SIGNING_SECRET` | unset (no signature) | dispatcher |
| `MAX_EVENT_BYTES` | `1048576` | publish-api |
| `REPLAY_MAX_EVENTS` | `10000` | replay-api (upper bound for `max_events`) |
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

### router (health port 8085)

- Consumer group `router` over all four `events-*` topics.
- For each event: match subscriptions of the same owner and type, evaluate the
  attribute filter and the JSON data filter, compute the ordering key before
  projection, apply the projection, then produce one delivery record per
  matching subscription keyed by subscription ID, plus one pending
  `status.Record`.
- The delivery record and its pending status are written in one Kafka
  transaction together with the offset commit (section 9), using
  `kgo.NewGroupTransactSession`.
- `End(TryCommit)` returns `(committed, err)` and both outcomes are handled:
  an `err != nil` is documented as non-retryable (failed/fenced transactional
  id, retry limit hit), so the router exits non-zero and Kubernetes restarts
  the pod with a fresh session — continuing would consume and commit nothing
  forever. `(false, nil)` means the transaction was aborted (offset commit
  rejected, group rebalanced): it is logged as a warning with the batch size,
  backed off, and re-consumed. franz-go rewinds the consumer to the last
  committed offsets in that case, so the aborted batch is polled again —
  observed live, where a transaction overlapping a rollout's rebalance aborted
  this way and the batch was re-routed without duplicates.
- Abort *reasons* come from franz-go's own logger, bridged to slog via
  `kgo.WithLogger(kgoLogger{})`: franz-go's Info (≈8 lines per transaction:
  begin, flush, `AddOffsetsToTxn`, end, response) is demoted to debug so the
  default log stays two lines, warnings and errors pass through, and
  `LOG_LEVEL=debug` turns on the full trace — where `transaction session
  ending` carries `was_failed` and `can_try_commit`, the two fields that say
  *why* an abort happened.
- Every consumer of a transactionally written topic reads with
  `read_committed`, set once in the frozen `kafkautil.NewClient`
  (`kgo.FetchIsolationLevel(kgo.ReadCommitted())`): Kafka's default is
  `read_uncommitted`, so a consumer would fetch records of an aborted
  transaction. Observed live: the router's first transaction aborted and the
  dispatcher still delivered the rolled-back record — 21 delivery records for
  20 published events, only 20 of them committed. That covers the dispatcher,
  status-writer, SSE gateway, backlog and replay worker; the two bare clients
  in `internal/replay/fetch.go` re-read `events-*`, which is never written
  transactionally, so isolation is irrelevant there (the option only affects
  fetches anyway).
- Ordering: one source partition is processed serially; the key of the
  delivery record is the subscription ID, so per-key order survives because one
  router instance owns the source partition.
- `GET /healthz` (liveness) and `GET /readyz` (readiness: PostgreSQL reachable
  and the initial registry load done) are the router's only endpoints; no auth.

### dispatcher (health port 8086)

- Consumer group `dispatcher` over `deliveries`, webhook subscriptions only.
- Inbox duty - draining and the expiry sweep - runs under a PostgreSQL
  advisory-lock lease (`internal/dispatch/lease.go`): the key is
  `fnv64a("pubsub:dispatcher:drain")`, pinned by a test, taken once on a
  pinned connection and re-proved against `pg_locks` on every 500 ms tick, so
  of N replicas exactly one drains. Park/count/ack are partition-scoped and run
  on every node; the standby takes over within one tick when the holder dies.
  The lock needs no schema and dies with its session, so a crashed replica
  cannot leave the inbox parked in nobody's hands.
- Delivers to the callback URL in the CloudEvents binary or structured binding
  (per subscription), with `X-Pubsub-Signature: sha256=<hex>` when
  `WEBHOOK_SIGNING_SECRET` is set, plus the delivery metadata headers
  `pubsub-delivery-id`, `pubsub-event-ref`, `pubsub-attempt`, `pubsub-seq`
  and `pubsub-replay-id` (only for replayed deliveries, section 11).
- Retry with exponential backoff and jitter, a circuit breaker, a token bucket
  rate limit, `max_in_flight` per delivery mode (section 7).
- On circuit-open, rate-limit overflow or queue overflow: park in the inbox and
  emit pending with the reason; live-class events are never parked.
- Drains the inbox at `drain_rate_per_s` when the subscription is healthy
  again; while draining, new events go to the inbox so that order holds.
- Emits in_flight, retrying, delivered, dead_lettered, paused, dropped.
- `GET /healthz` (liveness) and `GET /readyz` (readiness: PostgreSQL reachable,
  inbox partitions created, consume loop started) are the dispatcher's only
  endpoints; no auth.

### sse-gateway (port 8082)

- `GET /sse?subscription_id=..&consumer_id=..` (scope `subscribe`), heartbeats,
  at most `max_connections` per subscription.
- Publishes presence to `sse-presence` (tombstone on disconnect).
- Presence view: consumes that topic **without a consumer group** (a plain
  `ConsumeTopics` + `AtStart` client, nothing committed), so every node folds
  all partitions from the beginning and rebuilds the full connection map -
  including connections opened on other nodes while this one was down. A group
  would give each node only its slice, and `pick()` (section 7) would then park
  deliveries another node could have taken live.
- Delivery stage: consumer group `sse-delivery` over `deliveries`, sse
  subscriptions only. With a live connection it writes to `sse-live` targeted
  at the owning node, otherwise it parks in the inbox.
- Live consumer: one consumer group per node, starts at latest, drops messages
  without a local connection.
- `Last-Event-ID` on `GET /sse` (header, or `?last_event_id=`): the gateway
  replays the deliveries topic from cursor+1 to the high watermark with a
  throwaway groupless consumer (section 8), writes them before the event
  loop, then emits `sent` and deletes the inbox rows it served. Live-class
  records are skipped (section 12), expired ones get their status, a cursor
  behind retention answers a `: backlog gap:` comment, a malformed cursor is
  a 400 before the stream opens. A per-connection served set drops the
  overlap with the drainer. Fetches are best-effort (15 s bound); on failure
  the event loop continues and the client's next reconnect retries.
- `POST /sse/ack` acknowledges deliveries (Last-Event-ID or body); only an ack
  turns `sent` into `delivered` (section 9).

### status-writer (health port 8087)

- Consumer group `status-writer` over `delivery-status`, batch inserts into
  ClickHouse (`internal/chstore`), every 1 s or 10k rows.
- `GET /healthz` (liveness) and `GET /readyz` (readiness: ClickHouse reachable,
  table created) are the status-writer's only endpoints; no auth.

### status-api (port 8081)

- The four read endpoints of section 10, owner scoped from the token (scope
  `status:read`), keyset pagination on the subscription list.
- Current status is the newest row per delivery by the key
  `(ts, attempt, lifecycle rank)` (`chquery.LatestKeySQL` in
  `internal/chquery`): `ts` alone is not enough because transitions are
  flushed in batches and routinely share a millisecond, so `argMax` broke the
  tie by row order and reported `in_flight`/0 instead of `delivered`/200.
  The history stays queryable, ordered by the same key.
- The status summary counts **distinct deliveries**, not rows: `total` is the
  distinct deliveries in the window and each status counts the distinct
  deliveries whose current status is that value, so one delivery moving
  `pending → in_flight → delivered` counts once per status it *ends* in, not
  three times. (`COUNT(DISTINCT delivery_id)` per status; before this it was
  a row count and reported 21/63 for a batch of 20.)
- The status values of section 9 are defined once in `internal/status` and
  are the contract for `?status=` filtering, the replay selector and the
  status summary: `pending`, `in_flight`, `retrying`, `paused`, `sent` (SSE:
  written, not acknowledged), `delivered`, `dead_lettered`, `dropped`,
  `expired`, `failed`. Terminal: `delivered`, `dead_lettered`, `dropped`,
  `expired`, `failed`. `failed` is reserved by the spec ("failed or
  dead_lettered") but no emitter writes it — exhausted retries become
  `dead_lettered` — so it is accepted by every filter and never present in
  the data. The Status API does not validate `?status=` (an unknown value
  simply matches no rows); the replay selector rejects one with a 400.

### replay-api (port 8084)

- The four endpoints of section 11, all on scope `replay`, owner scoped
  (unknown and foreign answer the same 404):
  `POST /v1/subscriptions/{id}/replays` (202 + `replay_id`; `Idempotency-Key`,
  `dry_run`, at least one selector criterion), `GET /v1/replays/{id}`
  (progress: persisted matched/enqueued/not_replayable plus `delivered` and
  `failed` computed live from ClickHouse by replay ID), `POST
  /v1/replays/{id}/cancel`, `GET /v1/subscriptions/{id}/replays`.
- Selector: `statuses` (current status, by the same `(ts, attempt, rank)`
  latest-record key as the Status API), `from`/`to`
  (RFC3339, by ingestion time = first status record), `event_refs`. Order:
  original ingestion order (section 11). `event_types` is rejected with 400
  (section 9: status records carry no event type).
- Job store: `replay_jobs` in PostgreSQL (migration 002). Partial unique
  indexes enforce one active job per subscription (API maps a violation to
  409) and per-owner idempotency keys (a repeat returns the existing job).
- Job states (the CHECK of migration 002, unrelated to the delivery statuses
  above): `pending → running → done`, plus `cancel_requested → cancelled` on
  a cancel and `failed` on an error; the one-active-job index covers the
  first three.
- Worker (single goroutine): resolve in ClickHouse → re-read each event from
  `events-<class>` by stored `(class, partition, offset)` headers →
  `not_replayable` when the record is pruned or `expires_at` passed → apply
  the subscription's current projection (no filter re-evaluation, section 11)
  → produce delivery + pending status in one transaction per batch of 200,
  keyed by subscription ID, marked `Replay`/`ReplayID`. Cancellation is
  observed at batch boundaries. On startup `running → pending` (re-executes;
  duplicates acceptable, losses not), `cancel_requested → cancelled`.
- Transactional ID `replay-<host>-<pid>` so the replayer never fences the
  router's producer.

### harness

- `harness pub` – open-loop publisher at a configurable rate.
- `harness recv` – simulated webhook receiver with configurable latency, error
  rate, offline windows and `-gone410` (answers every delivery 410 Gone and
  records nothing, which turns each delivery into a deterministic
  dead-letter for replay tests). Receipts record `replay_id` when the
  `pubsub-replay-id` header is present.
- `harness sse` – simulated SSE consumer. Behaves like a browser EventSource:
  it reconnects automatically with `Last-Event-ID` of the last event received
  (initial cursor via `-last-event-id`), deduplicates by event reference
  across reconnects and acks every event.
- `harness verify` – the oracle: compares what receivers got with what the
  Status API reports and with the expected match set (section 15.1).

## Deviations from the spec, on purpose

- Registry distribution: PostgreSQL polling instead of a compacted Kafka topic.
- SSE multi-node routing: presence carries the node ID, so the targeted
  `sse-live` message reaches the node that holds the connection. No message is
  dropped because the connection lives elsewhere.
- Inbox deletion: batched deletes instead of a watermark table.
- Replay selector field `event_types`: rejected with 400 instead of
  implemented (section 9: status records carry no event type). `statuses`,
  the time range and `event_refs` work; the replay service itself is built
  (`replay-api`, section 11).
- Authentication: HS256 with a shared secret instead of JWKS.
