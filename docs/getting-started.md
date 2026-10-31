---
id: getting-started
title: Getting started
sidebar_position: 2
---

Everything runs on one machine with Docker and Go. The compose stack brings
up the infrastructure; the services are plain binaries you run in the
foreground or background.

## Prerequisites

Go 1.26+ and Docker with compose.

## 1. Infrastructure

```sh
docker compose up -d
go run ./cmd/admin all        # topics + PostgreSQL migrations + ClickHouse table
```

`admin` is idempotent; run it again whenever you add a migration or a topic.
Ports: Kafka `9092`, PostgreSQL `5432`, ClickHouse `8123` (HTTP) / `9000`
(native). Both databases use user/password `pubsub`.

## 2. Build

```sh
go build -o bin/ ./cmd/...
```

## 3. Start the services

Each service is standalone; run them in separate terminals or backgrounded
(`setsid nohup ./bin/<name> > /tmp/<name>.log 2>&1 < /dev/null &`, otherwise
they die with your shell). In order:

```sh
./bin/publish-api                        # data plane, :8080
HTTP_ADDR=:8083 ./bin/registry-api       # control plane, :8083
./bin/router                             # health :8085, needs PostgreSQL for the registry
./bin/dispatcher                         # health :8086, needs PostgreSQL for the inbox
./bin/sse-gateway                        # :8082
./bin/status-writer                      # health :8087, needs ClickHouse
./bin/status-api                         # :8081
./bin/replay-api                         # :8084, needs PostgreSQL + ClickHouse + Kafka
```

All of them expose `/healthz` (liveness) and `/readyz` (readiness) without
authentication; the three workers serve only those two. Liveness never fails
because a database is down — restarting a consumer would gain nothing — while
readiness does: it also reports `starting` until the registry, the inbox
partitions and the Kafka client are set up, and afterwards it fails while
PostgreSQL (plus ClickHouse for status-api and replay-api) is unreachable or
the registry index has stopped moving (`REGISTRY_STALE_AFTER`, 30 s). A
restart is safe: Kafka offsets and the inbox hold the state, and consumers
rejoin their groups.

## 4. One event, end to end

Every API call needs a bearer token from `minttoken` (scopes default to
`publish,subscribe,status:read`):

```sh
export JWT_SECRET=dev-secret
T=$(./bin/minttoken -owner team-a -scopes publish,subscribe,status:read)
```

Register an event type, create a subscription, publish, receive — the full
walkthrough with every request and response is in
[the API guide](api.md). The short version:

```sh
curl -s -X POST localhost:8083/v1/event-types -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders.created","retention_class":"short","content_type":"application/json"}'

curl -s -X POST localhost:8083/v1/subscriptions -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"orders.created","channel":"webhook",
       "callback_url":"http://localhost:9090/hook"}'

./bin/harness recv -addr :9090 -out recv.jsonl &

curl -s -X POST localhost:8080/v1/events -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -H 'ce-specversion: 1.0' -H 'ce-id: 42' -H 'ce-source: /orders' \
  -H 'ce-type: orders.created' \
  -d '{"order_id":"o-1","total":99.9}'
```

`recv.jsonl` gains one line per delivered event. See
[Inspecting state](operations.md#inspecting-state) for the SQL and Kafka
console commands that show what happened along the way.

## 5. Run it all with the harness

The harness does the seeding, publishing, receiving and verifying for you,
including an oracle that recomputes expected deliveries independently:

```sh
export API=http://localhost:8083        # seed/verify talk to the control plane
./bin/harness seed -owner team-a -types 6 -subs 12 > subs.json
./bin/harness recv  -addr :9090 -out recv.jsonl &
./bin/harness sse   -sub <sse-subscription-id> -owner team-a -out sse.jsonl &
./bin/harness pub   -owner team-a -rate 50 -duration 20s \
  -types harness.type.0,harness.type.1,harness.type.2,harness.type.3,harness.type.4,harness.type.5 \
  -out pub.jsonl
./bin/harness verify -owner team-a -api http://localhost:8083 \
  -events pub.jsonl -received recv.jsonl -sse sse.jsonl
```

Notes that cost time to discover:

- `harness seed`, `verify` and the type resolution of `harness pub` talk to
  the **control plane** (`-api http://localhost:8083`), while `harness pub`
  itself publishes to the **data plane** (`-api`, default `:8080`). Point
  `-api` at whichever the subcommand needs.
- Every subcommand takes its owner from `-owner`; the seeded event types
  belong to `team-a`, so `pub -owner team-a` must match.
- `verify` recomputes the expected deliveries locally — it evaluates the CEL
  filters itself and never asks the platform — and compares them with what
  the receivers recorded and what the status API reports. It exits non-zero
  on any missing or unexpected delivery.

## 6. Reset

Reset everything Kafka/PostgreSQL/ClickHouse-specific without touching the
code: `docker compose down -v`, then `go run ./cmd/admin all`. To reset only
the data, `TRUNCATE inbox;` in PostgreSQL and `TRUNCATE TABLE delivery_status;`
in ClickHouse.

## Next steps

- [Configuration](configuration.md) — every environment variable.
- [Chaos scenarios](operations.md#chaos-scenarios) — break things on purpose
  and watch the oracle's verdict.
- [Kubernetes](operations.md#deploying-to-kubernetes) — the two Helm charts.
