---
id: configuration
title: Configuration
sidebar_position: 7
---

Every service reads its configuration from the environment (`internal/config`);
the defaults suit the local compose stack. On Kubernetes it is one Secret
(`pubsub-config`, or `existingSecret`) that every pod loads with `envFrom`.

| Variable | Default | Used by |
| --- | --- | --- |
| `KAFKA_BROKERS` | `localhost:9092` | everything |
| `PG_DSN` | `postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable` | publish-api, registry-api, status-api, router, dispatcher, sse-gateway, replay-api |
| `CH_DSN` | `clickhouse://pubsub:pubsub@localhost:9000/pubsub` | status-writer, status-api, replay-api |
| `JWT_SECRET` / `JWT_ISSUER` / `JWT_AUDIENCE` | `dev-secret` / `dev` / `pubsub` | every API service, harness, minttoken |
| `HTTP_ADDR` | `:8080` (publish-api), `:8081` (status-api), `:8082` (sse-gateway), `:8084` (replay-api), set `:8083` for registry-api; `:8085` (router), `:8086` (dispatcher), `:8087` (status-writer) — health endpoints only | every service |
| `MAX_EVENT_BYTES` | `1048576` | publish-api |
| `REPLAY_MAX_EVENTS` | `10000` | replay-api |
| `REGISTRY_POLL` | `1s` (`2s` in publish-api) — safety net under the NOTIFY trigger of migration 003 | publish-api, router, dispatcher, replay-api, sse-gateway |
| `REGISTRY_STALE_AFTER` | `30s` — how old the registry index may be before `/readyz` fails | publish-api, replay-api, sse-gateway |
| `WEBHOOK_SIGNING_SECRET` | unset (no HMAC) | dispatcher |
| `ALLOW_PRIVATE_WEBHOOKS` | `1` in the local stack; set `0` for real deployments | dispatcher |
| `DISPATCHER_MAX_ATTEMPTS` | `10` | dispatcher |
| `NODE_ID` | hostname | sse-gateway |
| `SSE_HEARTBEAT` / `SSE_MAX_CONNECTIONS` | `15s` / `3` | sse-gateway |
| `LOG_LEVEL` | `info` | everything |

## Kafka timeouts

Two values that are only correct together, read as a pair by
`kafkautil.Timeouts()` and applied to every client.

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_SESSION_TIMEOUT` | `15s` | How long a group waits for a member before evicting it: the **crash floor**. After a `kill -9`, an OOMKill or a lost node, the dead member's partitions belong to nobody until it expires — measured 13–16 s to handover at this value, against 46 s at franz-go's 45 s default |
| `KAFKA_TRANSACTION_TIMEOUT` | `10s` | Must stay *below* the session timeout, so a consume-modify-produce transaction cannot outlive the member carrying it. `Timeouts()` enforces that by clamping (and logging), never by rejecting: refusing would take a service down over an environment variable |

:::note Why the pair was lowered
Both were franz-go's defaults (45 s / 40 s) until they were measured against
this platform. At 15 s / 10 s the crash handover drops from 46 s to 13 s with
an **identical oracle** (`missing=0 unexpected=0`) and an identical abort
count — one, the documented cold-start case, at 54 ms elapsed, so
`past_txn_timeout=false` even at 1,000 ev/s. A transaction that *does* outlive
its budget is re-consumed, so the failure mode is duplicates, never loss.

Both stay environment-tunable: the conservative pair is one variable away if a
deployment would rather keep the longer floor.
:::

Heartbeats run in franz-go's own background group goroutine rather than the
application's poll loop, so a slow publish or a long GC pause does not by
itself miss them. A process frozen or partitioned for longer than the session
timeout does — and evicting that one is the point.

The router's abort log carries `elapsed`, `past_txn_timeout` and `txn_timeout`,
so a timeout can be told from a revoke by reading one line rather than
re-running a scenario.

## Health and readiness

All eight services expose `/healthz` (liveness) and `/readyz` (readiness)
without authentication; the three workers serve only those two.

- **Liveness never fails because a database is down** — restarting a consumer
  would gain nothing.
- **Readiness does.** It reports `starting` until the registry, the inbox
  partitions and the Kafka client are set up, then fails while PostgreSQL
  (plus ClickHouse for status-api and replay-api) is unreachable, or while the
  registry index has stopped moving (`REGISTRY_STALE_AFTER`).

A restart is safe: Kafka offsets and the inbox hold the state, and consumers
rejoin their groups.
