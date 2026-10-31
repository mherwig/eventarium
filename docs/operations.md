---
id: operations
title: Operating Eventarium
sidebar_position: 8
---

## Deploying to Kubernetes

Two charts, installed in this order because the second one's hook provisions
the platform against the first one's endpoints:

```sh
docker build -t pubsub:dev .                  # or push it and set image.repository

helm upgrade --install pubsub-infra charts/pubsub-infra
kubectl rollout status statefulset/pubsub-infra-postgres   # wait for the stores

helm upgrade --install pubsub charts/pubsub
helm test pubsub                              # reaches /healthz and /readyz in-cluster
```

`pubsub-infra` is one Kafka, one PostgreSQL, one ClickHouse — deliberately
(see `charts/pubsub-infra/values.yaml`); growing them is failover tooling, a
different project from this one. `pubsub` runs **three replicas of every
service**, which the inbox lease and the groupless presence view make safe,
plus:

- `admin all` as a `pre-install`/`pre-upgrade` hook, so topics, migrations
  and the ClickHouse table exist before the new revision starts serving. Its
  environment is written inline because at `pre-install` the config secret
  does not exist yet.
- `maxUnavailable: 0` rollouts gated by `/readyz` on every component, and a
  `PodDisruptionBudget` per component (`replicas - 1`).
- Readiness is `/readyz` everywhere: PostgreSQL for the APIs and the
  gateway, ClickHouse additionally for status-api and replay-api, and a
  registry index that is not standing still for everything that routes on it.
  Liveness stays `/healthz` and never fails because a store is down.
- No service account token, no writable root filesystem, no capabilities,
  uid 65532 — nothing here talks to the Kubernetes API.

Configuration is one Secret (`pubsub-config`, or `existingSecret` if you
keep credentials elsewhere) that every pod loads with `envFrom`; keys are
the environment variables in [Configuration](configuration.md). `HTTP_ADDR` is
always set explicitly because the binaries disagree on their defaults.

:::caution Untested on a real cluster
Nothing in these charts has been installed on a real cluster yet — they are
`helm lint`-clean and validated against the Kubernetes 1.31 schemas with
`kubeconform -strict`, including the `existingSecret`, `admin.enabled=false`
and single-replica variants.
:::

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

To watch a breaker cycle or a drain:

```sh
grep "circuit breaker" dispatcher.log         # opened / closed after successful probe / re-opened after failed probe
docker compose exec -T postgres psql -U pubsub -d pubsub \
  -c "SELECT subscription_id, reason, count(*) FROM inbox GROUP BY 1, 2;"
```

## Chaos scenarios

Five scenarios, each "publish at 200 ev/s for 60 s, inject one fault in the
middle, let it drain, then ask the oracle". The driver is committed rather
than described — `scripts/chaos.sh`, against the topology `scripts/stack.sh`
starts:

```sh
make infra            # Kafka, PostgreSQL, ClickHouse plus topics and schemas
make stack            # five services, three replicas of each consumer
make chaos SCEN=pods  # pods | pg | kafka | ch | rebalance
make chaos-test       # the driver's own tests, no stack needed
```

| `SCEN` | What it breaks mid-publish |
| --- | --- |
| `pods` | `kill -9` one dispatcher, one router and one gateway |
| `pg` | receiver offline **and** PostgreSQL down, overlapping by 20 s |
| `ch` | ClickHouse down for 20 s |
| `kafka` | broker restart |
| `rebalance` | `kill -9` one dispatcher, **and** measures how long its partitions stay unowned against `KAFKA_SESSION_TIMEOUT` + 15 s of polling slack |

Results, including the full duplicate analysis, are in
[Performance](performance.md#resilience-under-injected-faults).

Three properties in the driver are load-bearing, and each one exists because
getting it wrong produced a result that looked fine:

- **Faults are asserted, not assumed.** An unquoted `$DOCKER` word-split once
  made every `docker compose stop` exit 127 into `/dev/null`, and the first
  `pg` run reported success with PostgreSQL never stopped.
- **The judge waits for the queues *and* the owner's inbox to reach 0.**
  Judging early reports work that is merely unfinished as lost. It also matters
  because `missing=0` only means *tracked*: `accounted()` counts `pending`, so
  the driver separately fails the run when `received` falls short of
  `expected` — parked-but-undelivered deliveries would otherwise pass.
- **Each run seeds its own owner**, so an earlier run's events cannot leak into
  the next run's status window as `unexpected`.

`rebalance` is the one that asserts a number rather than a verdict: it polls
the group's assignment until every partition is owned by a live member, and
fails if that takes longer than the configured session timeout allows. It is
what turns "failover takes 13 s" from an observation into something an upgrade
cannot silently change.

Setup also waits for two things it used to take on trust. The gateways must
already know the subscriptions just created — a subscription is only reachable
~50–60 ms after the API returns 201, and the harness's SSE client (like a
browser's `EventSource`) treats a 404 as final, so connecting a millisecond
early kills that client for the whole run. And its own receiver must be
listening: a leftover process on `:9090` used to make it fail to bind, with
the run dying eight minutes later as a shortfall that looked like the
platform's fault.

## Checks

```sh
go vet ./... && gofmt -l cmd internal migrations   # both should be silent
go test ./...                                       # unit tests for chquery and harness
./scripts/chaos_test.sh                             # the chaos driver's own tests
```

## Resetting

Reset everything Kafka/PostgreSQL/ClickHouse-specific without touching the
code: `docker compose down -v`, then `go run ./cmd/admin all`. To reset only
the data, `TRUNCATE inbox;` in PostgreSQL and `TRUNCATE TABLE delivery_status;`
in ClickHouse.
