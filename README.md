# Event platform spike

A working end-to-end spike of the platform described in
[docs/spec.md](docs/spec.md): CloudEvents over REST in, filtered and
projected per subscription, delivered over webhooks and SSE with tracked,
guaranteed delivery, on Kafka, PostgreSQL and ClickHouse. The interfaces
between the services (topics, record shapes, endpoints) are fixed in
[docs/SPIKE.md](docs/SPIKE.md) — read it before changing anything that
crosses a service boundary. Deviations from the spec and the open points are
tracked in [docs/deviations-and-open-points.md](docs/deviations-and-open-points.md).

```
publish-api ─► Kafka events-<class> ─► router ─► Kafka deliveries ─┬─► dispatcher ─► webhook URLs
     ▲                                              │               └─► sse-gateway ─► SSE clients
     │ control plane (registry-api → PostgreSQL)    │
     └──────────────────────────────────────────────┴─► Kafka delivery-status ─► status-writer ─► ClickHouse ─► status-api
                                     overflow/offline ─► inbox (PostgreSQL) ─► drained back to the hot path
```

## Components

| Binary | Role | Listens | Spec |
| --- | --- | --- | --- |
| `publish-api` | Accepts CloudEvents (binary, structured, batch), validates against the registry, stamps `expires_at` and the owner, writes to `events-<class>` | HTTP `:8080` | 6.1 |
| `registry-api` | CRUD for event types and subscriptions (the control plane) | HTTP `:8083` | 5, 7 |
| `router` | Consumes all event topics; CEL attribute + data filters, ordering key, projection, fan-out. Writes the delivery and its pending status in one Kafka transaction | – | 6.2, 9 |
| `dispatcher` | Webhook delivery: per-mode concurrency, retry with backoff + jitter, circuit breaker, token-bucket rate limit, HMAC signatures, SSRF guard, inbox parking and draining | – | 6.4 |
| `sse-gateway` | SSE connections with heartbeats and acks, presence topic, per-node live delivery, inbox parking and draining | HTTP `:8082` | 6.5 |
| `status-writer` | Consumes `delivery-status`, batch-inserts into ClickHouse (1 s / 10k rows) | – | 9 |
| `status-api` | The four delivery-status query endpoints, owner scoped | HTTP `:8081` | 10 |
| `admin` | Provisions Kafka topics, applies PostgreSQL migrations, creates the ClickHouse table | – | 12, 6.6, 9 |
| `minttoken` | Mints development JWTs (production uses an OIDC provider) | – | 6.7 |
| `harness` | Seed, open-loop publisher, simulated webhook receiver, simulated SSE consumer, independent oracle | – | 15 |

Kafka consumer groups: `router` (events-\*), `dispatcher` and `sse-delivery`
(both on `deliveries`, by channel), `sse-presence-view`, one
`sse-live-<NODE_ID>` per gateway node, `status-writer`.

## Storage responsibilities

Neither database is on the delivery hot path: events and delivery payloads
live in Kafka, and both stores are read/written off that path.

| Store | Holds | Why there |
| --- | --- | --- |
| **Kafka** | `events-<class>` (the events themselves), `deliveries` (one record per subscription per event), `delivery-status`, `sse-live`, `sse-presence` | The data plane: buffering, fan-out, ordering per key, at-least-once replay from offsets |
| **PostgreSQL** | `event_types` + `subscriptions` (the registry, §5/§7), `inbox` (parked deliveries, §6.6) | Small, transactional, mutated: CRUD with constraints, and the inbox needs ordered per-subscription scans, batched deletes and partition drop at expiry |
| **ClickHouse** | `delivery_status`: one append-only row per status transition (`pending → sent → delivered/failed/parked`) with attempt, HTTP code, latency, reason (§9/§10) | High-volume append plus analytical reads (current status via `argMax`, counts, keyset pagination), cheap expiry via partition TTL — the shape MergeTree is built for |

The status table grows with traffic × fan-out × attempts (~20k rows/s at the
N1 rate), which is why it is not in PostgreSQL; see the load-test section for
the measured numbers. The boundary is cheap to revise: status-writer is the
only writer, status-api the only reader.

## Repository layout

```
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
  routing/, dispatch/, sse/, harness/   service-internal logic
migrations/           embedded PostgreSQL migrations
docker-compose.yaml   Kafka (KRaft), PostgreSQL 16, ClickHouse 24.8
```

## Prerequisites

Go 1.26+ and Docker with compose. (On this machine the toolchain lives in
`~/.local/go`, so `export PATH=$HOME/.local/go/bin:$PATH` first; `make` is not
installed here, use the `go` and `docker compose` commands directly.)

## Configuration

Every service reads its configuration from the environment
(`internal/config`); the defaults suit the local compose stack.

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | everything |
| `PG_DSN` | `postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable` | publish-api, registry-api, router, dispatcher, sse-gateway |
| `CH_DSN` | `clickhouse://pubsub:pubsub@localhost:9000/pubsub` | status-writer, status-api |
| `JWT_SECRET` / `JWT_ISSUER` / `JWT_AUDIENCE` | `dev-secret` / `dev` / `pubsub` | every API service, harness, minttoken |
| `HTTP_ADDR` | `:8080` (publish-api), `:8081` (status-api), `:8082` (sse-gateway), set `:8083` for registry-api | the API services |
| `MAX_EVENT_BYTES` | `1048576` | publish-api |
| `WEBHOOK_SIGNING_SECRET` | unset (no HMAC) | dispatcher |
| `ALLOW_PRIVATE_WEBHOOKS` | true in the spike; set `0` for real deployments | dispatcher |
| `DISPATCHER_MAX_ATTEMPTS` | `10` | dispatcher |
| `NODE_ID` | hostname | sse-gateway |
| `SSE_HEARTBEAT` / `SSE_MAX_CONNECTIONS` | `15s` / `3` | sse-gateway |
| `LOG_LEVEL` | `info` | everything |

## Running locally

### 1. Infrastructure

```sh
docker compose up -d
go run ./cmd/admin all        # topics + PostgreSQL migrations + ClickHouse table
```

`admin` is idempotent; run it again whenever you add a migration or a topic.
Ports: Kafka `9092`, PostgreSQL `5432`, ClickHouse `8123` (HTTP) / `9000`
(native). Both databases use user/password `pubsub`.

### 2. Build

```sh
go build -o bin/ ./cmd/...
```

### 3. Start the services

Each service is standalone; run them in separate terminals or backgrounded
(`setsid nohup ./bin/<name> > /tmp/<name>.log 2>&1 < /dev/null &`, otherwise
they die with your shell). In order:

```sh
./bin/publish-api                        # data plane, :8080
HTTP_ADDR=:8083 ./bin/registry-api       # control plane, :8083
./bin/router                             # needs PostgreSQL for the registry
./bin/dispatcher                         # needs PostgreSQL for the inbox
./bin/sse-gateway                        # :8082
./bin/status-writer                      # needs ClickHouse
./bin/status-api                         # :8081
```

All of them expose `/healthz` without authentication. A restart is safe: Kafka
offsets and the inbox hold the state, and consumers rejoin their groups.

### 4. Register, subscribe, publish, receive

The harness does all of this for you (§5); manually, one event end to end.
Every API call needs a bearer token from `minttoken` (scopes default to
`publish,subscribe,status:read`):

```sh
export JWT_SECRET=dev-secret
T=$(./bin/minttoken -owner team-a -scopes publish,subscribe,status:read)
```

**Register an event type** (control plane, `:8083`). `retention_class` is
`live`, `short`, `day` or `week` and selects the Kafka topic the events land
on. `ordering_key_def` (optional) derives the ordering key for `key_ordered`
subscriptions: `subject`, `ext:<name>`, or `json:<dotted.path>` in the data;
empty falls back to the publisher's partition key:

```sh
curl -s -X POST localhost:8083/v1/event-types -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders.created","retention_class":"short","content_type":"application/json"}'
```

**Create a webhook subscription** with a CEL attribute filter and a
projection (`:8083`):

```sh
curl -s -X POST localhost:8083/v1/subscriptions -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"orders.created","channel":"webhook",
       "callback_url":"http://localhost:9090/hook",
       "filter":"attributes[\"region\"] == \"eu\"",
       "projection":["order_id","total"],
       "delivery_mode":"key_ordered","rate_limit_per_s":100}'
```

`filter` is CEL over `attributes` and `data`; `projection` is the allowlist
of data fields the subscriber receives (§6.2). The callback may be any HTTP
endpoint; the simulated receiver of the harness listens on `:9090/hook`:

```sh
./bin/harness recv -addr :9090 -out recv.jsonl &
```

**Create an SSE subscription** the same way, with `channel: "sse"` (no
callback URL):

```sh
curl -s -X POST localhost:8083/v1/subscriptions -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"orders.created","channel":"sse"}'
```

**Publish an event** (data plane, `:8080`, binary CloudEvents binding —
`ce-*` headers carry the attributes, the body is the data member; use
`Content-Type: application/cloudevents+json` for structured mode, or
`harness pub` for load). `ce-region` becomes the extension attribute
`region` that the filter reads:

```sh
curl -s -X POST localhost:8080/v1/events -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -H 'ce-specversion: 1.0' -H 'ce-id: 42' -H 'ce-source: /orders' \
  -H 'ce-type: orders.created' -H 'ce-region: eu' \
  -d '{"order_id":"o-1","total":99.9,"internal":"secret"}'
```

The publish API caches the registry and refreshes it every 2 s (the SSE
gateway likewise), so wait a moment after creating an event type or
subscription before publishing to it.

**Receive on the webhook**: the dispatcher POSTs the event — binary binding
by default, with delivery metadata in `pubsub-*` headers (never inside the
event, §5) and an HMAC over the body when `WEBHOOK_SIGNING_SECRET` is set:

```
POST /hook
ce-specversion: 1.0
ce-id: 42
ce-source: /orders
ce-type: orders.created
ce-region: eu
pubsub-delivery-id: <event-ref>/<subscription-id>
pubsub-event-ref: 4f4e...            # the event's identity
pubsub-attempt: 1
pubsub-seq: 7:1904                   # position on the deliveries topic
X-Pubsub-Signature: sha256=...       # only with WEBHOOK_SIGNING_SECRET

{"order_id":"o-1","total":99.9}      # projection applied: "internal" is gone
```

**Receive on the SSE**: connect to `:8082` with a subscribe-scoped token:

```sh
curl -N -H "Authorization: Bearer $T" \
  "http://localhost:8082/sse?subscription_id=<id>&consumer_id=demo"
```

(The spike accepts the token only in the `Authorization` header;
browser `EventSource` cannot send headers, so a real deployment would add a
short-lived token in the query string or a cookie — like the usual
`?access_token=` dance.)

Each delivery is one frame — `id` is the sequence, `data` the delivery
envelope with the full CloudEvent inside:

```
id: 7:1904
event: delivery
data: {"delivery_id":"...","event_ref":"...","sequence":"7:1904",
       "event":{"specversion":"1.0","id":"42","source":"/orders",
                "type":"orders.created","datacontenttype":"application/json",
                "data":{"order_id":"o-1","total":99.9}}}
```

`sent` becomes `delivered` only when the client acknowledges the sequence —
either `POST /sse/ack` with `{"subscription_id":...,"sequence":"7:1904"}`, or
`Last-Event-ID` on reconnect to fetch the parked backlog (§6.5/§8). The
`harness sse` subcommand does both and logs what it saw:

```sh
./bin/harness sse -sub <id> -owner team-a -addr http://localhost:8082 \
  -out sse.jsonl -duration 60s
```

**Query the outcome** — the delivery's status history
(`status:read` scope, `:8081`):

```sh
T=$(./bin/minttoken -owner team-a -scopes status:read)
curl -s localhost:8081/v1/subscriptions/<id>/deliveries -H "Authorization: Bearer $T"
```

### 5. Load test with the harness

```sh
export API=http://localhost:8083        # seed/verify talk to the control plane
./bin/harness seed -owner team-a -types 6 -subs 12 > subs.json
./bin/harness recv  -addr :9090 -out recv.jsonl &
./bin/harness sse   -sub <sse-subscription-id> -owner team-a -out sse.jsonl &
./bin/harness pub   -owner team-a -rate 50 -duration 20s \
  -types harness.type.0,harness.type.1,harness.type.2,harness.type.3,harness.type.4,harness.type.5 \
  -out pub.jsonl
./bin/harness verify -owner team-a -api http://localhost:8083 \
  -events pub.jsonl -received recv.jsonl
```

Notes that cost me time:

- `harness seed`, `verify` and the type resolution of `harness pub` talk to the
  **control plane** (`-api http://localhost:8083`), while `harness pub` itself
  publishes to the **data plane** (`-api`, default `:8080`). Point `-api` at
  whichever the subcommand needs.
- Every subcommand takes its owner from `-owner`; the seeded event types belong
  to `team-a`, so `pub -owner team-a` must match.
- `verify` recomputes the expected deliveries locally — it evaluates the CEL
  filters itself and never asks the platform — and compares them with what the
  receivers recorded and what the status API reports. It exits non-zero on any
  missing or unexpected delivery.

## Inspecting state

```sh
# subscriptions and parked inbox rows
docker compose exec -T postgres psql -U pubsub -d pubsub \
  -c "SELECT id, event_type, channel, delivery_mode FROM subscriptions;"
docker compose exec -T postgres psql -U pubsub -d pubsub \
  -c "SELECT subscription_id, count(*) FROM inbox GROUP BY 1;"

# delivery status history
docker compose exec -T clickhouse clickhouse-client -u pubsub --password pubsub -d pubsub \
  -q "SELECT status, count() FROM delivery_status GROUP BY status FORMAT PrettyCompact"

# what the router produced, and per-stage latency from the headers
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic deliveries --max-messages 3 \
  --property print.headers=true
```

Reset everything Kafka/PostgreSQL/ClickHouse-specific without touching the
code: `docker compose down -v`, then `go run ./cmd/admin all`. To reset only
the data, `TRUNCATE inbox;` in PostgreSQL and `TRUNCATE TABLE delivery_status;`
in ClickHouse.

## Checks

```sh
go vet ./... && gofmt -l cmd internal migrations   # both should be silent
go test ./...                                       # unit tests for chquery and harness
```

## Where the spike stands

A full harness run verifies with zero missing, zero unexpected and zero
duplicate deliveries across both channels and all three delivery modes, with
attribute filters, data filters and projections — at 50 events/s (p50 22 ms /
p99 28 ms) and at the 1,000 events/s N1 target (see the next section). Not
exercised yet: fault injection, the soak test, replay (spec 17 step 4) and the
multi-day offline scenario.

## Load test at 1,000 events/s (spec 15.2, targets N1/N3)

Measured on the local compose stack on one 8-core laptop: single-node
Kafka/PostgreSQL/ClickHouse and one instance of every service.

Topology: `team-b`, 4 event types (`load.type.0..3`), 40 subscriptions — 20
webhook and 20 SSE — covering unordered, per-shard ordered (attribute filter),
key-ordered (data filter `int(data.seq) % 2 == 0`) and rate-limited webhook
subscriptions, plus 20 connected SSE clients. 1 KiB JSON payloads, batch mode
(100 events per request).

```sh
./bin/harness seed -owner team-b -prefix load -types 4 -subs 40 > seed-b.json
./bin/harness recv -addr :9090 -out recv.jsonl &
# one per seeded sse subscription
./bin/harness sse -sub <id> -owner team-b -addr http://localhost:8082 \
  -out sse-<id>.jsonl -duration 240s &
./bin/harness pub -owner team-b -rate 1000 -duration 30s \
  -types load.type.0,load.type.1,load.type.2,load.type.3 \
  -payload 1024 -out pub.jsonl
./bin/harness verify -owner team-b -api http://localhost:8083 \
  -status-api http://localhost:8081 -events pub.jsonl -received recv.jsonl
```

Result: 30,000 events published at a sustained 1,003 ev/s with zero publish
failures, fanning out to ~146,000 deliveries. The dispatcher burst to ~3,100
webhook deliveries/s and drained to `queue_depth=0` with zero retries, parks,
drops or dead-letters. Latency from publish to receipt:

| Channel | p50 | p90 | p99 | p99.9 | max |
| --- | --- | --- | --- | --- | --- |
| Webhook (unlimited subs) | 32 ms | 39 ms | **51 ms** | – | 109 ms |
| SSE | 51 ms | 65 ms | **88 ms** | 140 ms | 153 ms |

N3 (p99 < 1 s, target a few hundred ms) is met with margin on both channels.
Rate-limited webhook subscriptions (`rate_limit_per_s`) show minutes of tail
latency by design: they drain their backlog at the configured rate. The oracle
agrees: `expected=145918 missing=0 unexpected=0 duplicates=0` over all 40
subscriptions.

Caveats: this validates the pipeline shape and per-stage latencies, not
cluster-scale HA (single broker, single node of everything). ClickHouse status
write/query throughput is the most pressured component at this rate. The
status table is cumulative, so re-running a load test for the same owner
counts earlier runs' deliveries too — the harness scopes Status API queries to
the run's publish window (`from`/`to`), keep that in mind when probing the API
by hand.

The run found and fixed two real bugs, worth remembering when touching these
paths:

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

## Deviations from the spec, on purpose

Tracked, with reasons, in
[docs/deviations-and-open-points.md](docs/deviations-and-open-points.md)
together with the spec's open decisions and what this spike did or did not
answer. Short version: registry polls PostgreSQL instead of a compacted
config topic, inbox rows are deleted in batches instead of by watermark,
HS256 dev auth instead of JWKS, no replay service yet, and SSE multi-node
routing uses presence with a node ID rather than rendezvous hashing.
