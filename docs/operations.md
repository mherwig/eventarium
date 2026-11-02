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
service**, which the inbox lease and the mirrored presence view make safe,
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

# what the router produced, and per-stage latency from the headers.
# INTERNAL listener, not localhost: this runs inside the container, where
# localhost is that container only - a partition led by another broker would
# be unreachable on localhost:9092.
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:29092 --topic deliveries --max-messages 3 \
  --property print.headers=true
```

To watch a breaker cycle or a drain:

```sh
grep "circuit breaker" dispatcher.log         # opened / closed after successful probe / re-opened after failed probe
docker compose exec -T postgres psql -U pubsub -d pubsub \
  -c "SELECT subscription_id, reason, count(*) FROM inbox GROUP BY 1, 2;"
```

## Kafka topology

Three brokers in KRaft combined mode, `RF=3`, `min.insync.replicas=2`:

| service | container | CLIENT (host) | INTERNAL (Compose network) |
| --- | --- | --- | --- |
| `kafka` | `pubsub-kafka-1` | `localhost:9092` | `kafka:29092` |
| `kafka-2` | `pubsub-kafka-2` | `localhost:9094` | `kafka-2:29092` |
| `kafka-3` | `pubsub-kafka-3` | `localhost:9095` | `kafka-3:29092` |

**Which listener to use is decided by where the client runs, and getting it
wrong fails quietly.**

- Processes on the host — every Go service, `KAFKA_BROKERS` — use **CLIENT**.
  The CLIENT port is the same inside the container as on the host, so a
  broker's own healthcheck and its advertised address agree.
- Anything run with `docker exec` or `docker compose exec` must use
  **INTERNAL**. Inside a container `localhost` is *that* container, so CLIENT
  metadata hands back `localhost:9094` for broker 2, which nothing in broker
  3's container is listening on: `--list` and `--describe` then fail on every
  broker but their own, while the command still exits 0. Kafka matches the
  listener you connected on and returns that listener's addresses for the whole
  cluster, which is why `kafka:29092` resolves correctly from any container.

### Growing the cluster: do it before you have data

**A single-node KRaft cluster cannot be grown to three by editing
`KAFKA_CONTROLLER_QUORUM_VOTERS` and restarting.** Observed directly: the two
new brokers start fresh, form a quorum among themselves (2 of 3 is already a
majority) with an empty metadata log, and elect a leader. Broker 1 then joins
as a follower of that leader, truncates its own `__cluster_metadata` to match a
log that does not contain its topics, and dies with

```
Log doesn't have a last fetch epoch and there is a snapshot
(OffsetAndEpoch(offset=579547, epoch=28)). Expected the snapshot's end offset
to match the log's end
```

The topic segment files survive — only the metadata log is destroyed — but with
the metadata gone the cluster does not know the topics exist, so the data is
unreachable. Growing KRaft in place needs the dynamic voter-set procedure
(`kafka-metadata-quorum.sh`), not a config edit.

For this stack that means: **decide the broker count before you have data you
care about.** To start over, remove only the Kafka volumes —
`docker compose down -v` would also destroy PostgreSQL and ClickHouse.

### Replication factor does not upgrade

`kafkautil.EnsureTopics` tolerates `TOPIC_ALREADY_EXISTS`, and Kafka has no
"alter replication factor": an existing topic keeps whatever it was created
with. Changing `kafkautil.ReplicationFactor` does nothing to topics that
already exist. Verify rather than assume:

```sh
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 --describe \
  | awk '/^Topic:/{t=$2; for(i=1;i<=NF;i++) if($i=="ReplicationFactor:") print t, $(i+1)}'
```

## Chaos scenarios

Six scenarios, each "publish at 200 ev/s for 60 s, inject one fault in the
middle, let it drain, then ask the oracle". The driver is committed rather
than described — `scripts/chaos.sh`, against the topology `scripts/stack.sh`
starts:

```sh
make infra            # Kafka, PostgreSQL, ClickHouse plus topics and schemas
make stack            # five services, three replicas of each consumer
make chaos SCEN=pods  # pods | pg | kafka | ch | rebalance | gwfail
make chaos-test       # the driver's own tests, no stack needed
```

| `SCEN` | What it breaks mid-publish |
| --- | --- |
| `pods` | `kill -9` one dispatcher, one router and one gateway |
| `pg` | receiver offline **and** PostgreSQL down, overlapping by 20 s |
| `ch` | ClickHouse down for 20 s |
| `kafka` | broker restart |
| `rebalance` | `kill -9` one dispatcher, **and** measures how long its partitions stay unowned against `KAFKA_SESSION_TIMEOUT` + 15 s of polling slack |
| `gwfail` | `kill -9` one gateway and **leave it down**: its clients have to land on a survivor, and its subscriptions have to become routable again |

Results, including the full duplicate analysis, are in
[Performance](performance.md#resilience-under-injected-faults).

Five properties in the driver are load-bearing, and each one exists because
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
- **`ch` asserts the status store, not only the deliveries.** It settles
  `status-writer`'s lag on `delivery-status` to 0 *while the outage is still the
  subject*, then the oracle's `unrecorded=` count checks that every delivery a
  receiver got has a row in the store. Lag 0 is the *sufficient* half of that
  claim and it is sound: `status-writer` commits an offset only after the
  insert into ClickHouse succeeded, so while lag reads 0 nothing can be left in
  Kafka — including the rows written while it was down. Judging before that
  would read "still in flight" as "lost". The converse is **not** sound and the
  driver does not assert it: after a rebalance the commits fail with
  `UNKNOWN_MEMBER_ID` and the offsets are deliberately dropped
  (`maxCommitFailures`: the partitions were revoked, the commit can never
  succeed, the records may be re-consumed, the table is append-only), so from
  then on lag counts what may be *re-read* rather than what has *not been
  inserted*. It grew to 1.8 M across a suite run while `unrecorded=0` held
  throughout. So a non-zero lag only fails the run when `status-writer` has
  reported no commit trouble at all; otherwise the driver says so and hands the
  verdict to `unrecorded`, which reads rows rather than offsets. The producer's
  own counter (`status_lost`, a row that never reached Kafka at all, and so
  invisible to both lag and `unrecorded`) is diffed across the run as well — it
  is the one link neither of the other two can see.
  `unrecorded` judges only receipts inside the oracle's `[from,to]` window,
  because the Status API cannot see rows outside it and treating "not visible
  to this query" as "lost" produced 30,028 false alarms on the first `pg` run —
  a third of its receipts land minutes after publishing stops. On the run that
  matters the coverage is complete: **`ch` covers 33,703 of 33,703 receipts
  (100%)**, so the scenario whose status rows were actually at risk is judged
  in full. `pg` covers 21%, where it is belt-and-braces — its fault is
  PostgreSQL and the receiver, and ClickHouse is up throughout.
- **Membership is asserted separately, because lag cannot see it.** A
  `status-writer` that has lost its consumer-group membership keeps inserting
  from its stale assignment, so every row-level check stays green while the
  committed offset stops moving for the rest of the process's life. That is
  exactly the bug these runs found — and exactly what `status_settled` now
  declines to interpret lag as, so without this assertion the exemption
  written for it would have hidden the failure it describes. `judged()` asks
  for membership first: it is the cause of the other two, and it fails in 30 s
  instead of costing a settle timeout before the writer is named.

`rebalance` is the one that asserts a number rather than a verdict: it polls
the group's assignment until every partition is owned by a live member, and
fails if that takes longer than the configured session timeout allows. It is
what turns "failover takes 13 s" from an observation into something an upgrade
cannot silently change.

`gwfail` is the one that has to prove it did not test nothing. Killing a
gateway whose subscriptions carry no deliveries would move seven idle
connections and report success — which is exactly what its first run did, and
why it now sums the oracle's `expected` count for the subscriptions that were
on the victim and fails if that sum is 0. The subscription cycle in
`harness seed` and the client-to-gateway assignment used to line up so that
every `key_ordered` client — whose data filter rejects all 3,000 events —
landed on the same gateway; the assignment now walks the client list in groups
of three, which is one of each delivery mode. With that fixed the scenario
carried 6,199 deliveries on the dead gateway's subscriptions, and it found a
real loss: the delivery stage chose its target with the first entry of a Go map
range, so after a `kill -9` it kept stamping `sent` against the dead node's
connection while the client was already connected to a survivor and would never
reconnect to recover them. Six deliveries were lost that way, and `pick()` now
breaks the tie on the node's lease heartbeat instead.

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
