---
id: deviations-and-open-points
title: Deviations and open points
sidebar_position: 95
---

Status of Eventarium against [spec.md](spec.md). The inter-service contracts
are in [contracts.md](contracts.md); the measured results are in
[Performance](performance.md).

## Deviations from the spec, on purpose — five

1. **Registry distribution (§6.1): services read PostgreSQL instead of a
   compacted Kafka config topic — polled every 1–2 s and, since migration
   003, woken by a `pg_notify` trigger on `event_types`/`subscriptions`.**
   Interface identical, only the source differs. This was the first cut when
   building in parallel — one less Kafka contract to get wrong between six
   components. The trigger turned the poll into a safety net: NOTIFY is
   transactional (delivered only after the writing transaction commits, so
   the index can never be refreshed ahead of the rows it describes) and
   reaches every listening instance, so a just-created subscription is
   matched within milliseconds instead of after the next tick. Notifications
   are not replayed, which is why the listener reloads once on every
   (re)connect and the poll stays; each registry instance holds one dedicated
   connection, `pubsub-registry-listen` in `pg_stat_activity`.
2. **Inbox deletion (§6.6): batched deletes instead of a per-subscription
   watermark.** Simpler; the watermark would save deletes at the cost of a
   new table and trickier invariants.
3. **Authentication (§6.7): HS256 with a shared secret instead of JWKS.**
   Dev-only; `minttoken` mints the tokens. JWKS/OIDC is a deployment concern
   that doesn't change the services' code (only `internal/auth`).
4. **Replay selector field `event_types` (§11/§17): rejected with 400 instead
   of implemented.** Status records deliberately carry no event metadata
   (§9), so the status store cannot filter on type; `statuses`, the time
   range and `event_refs` work. The other §11 scope questions (backfill for
   events that never matched) are open points below.
5. **SSE multi-node routing (§6.5/7): presence records carry the node ID
   instead of rendezvous hashing.** A `sse-live` message is targeted at the
   node that currently holds the connection; no message is dropped because
   the connection lives elsewhere. Rendezvous hashing would assign
   *connections* to nodes up front; we went with "where the connection
   actually is".

One small *extension* appeared during load testing: the status-summary
endpoint (§10, endpoint 4 — optional in the spec) gained an optional `since`
parameter, because the status table is cumulative across runs and the oracle
needed to scope counts to one run. The counts themselves are **distinct
deliveries**, not status rows: §10 says "counts per status" without saying
what is counted, and rows would count one delivery once per transition it
passed through (21 of a batch of 20, before the fix).

One smaller gap on the same page: §9's "`failed` *or* `dead_lettered` —
retries exhausted" is implemented as `dead_lettered` only. `failed` is part
of the contract (the Status API filter and the replay selector both accept
it) but the dispatcher never writes it, so it never appears in the data. All
eleven status values are documented in
[Reliability](reliability.md#the-eleven-statuses).

A second *extension* is the `filtered` status (it required a new constant in
the frozen `internal/status` — approved for this feature). §9 describes the
statuses of a **delivery**, but a candidate subscription whose filter rejects
an event gets no delivery at all, and before this the router's `continue` left
no trace of it anywhere: no row, no log line, no counter. Now the router
writes one `filtered` row (reason `filter_mismatch`, or `filter_error` when
the CEL itself failed to evaluate) carrying the `delivery_id` that delivery
*would* have had, so the case is queryable with the existing Status API,
appears as a zero-able bucket in the status summary, and `terminal` from the
start. Two boundaries are deliberate: an event with **no subscription at all**
for its type gets no status row (there is no subscription ID to hang one on —
publish-api reports that case as an advisory `warning` on the 201 instead),
and `filtered` is **not replayable** — the replay selector answers 400 and
`replay.Resolve` drops such rows from every selector, because a replay
re-applies the projection but never the filter and would otherwise deliver
exactly what the filter rejected. It is a per-event row, so its volume scales
with the number of candidate subscriptions per event type; the spec's own
figure of a few hundred filter evaluations per event bounds it.

A third *extension* is **`PUT /v1/subscriptions/{id}`** — mutable
subscriptions. §5/§7 say what a registry stores but never how a stored row is
changed (the spec only lists Status and Replay endpoints), so this fills the
gap with the conventional full replace: same body as `POST`, immutable `id`
and `owner`, everything else rewritten. Details and the consequences for
in-flight deliveries are in the
[mutability section](#mutable-subscriptions-put-v1subscriptionsid) below.

## Open points — answered

- *Load figures are estimates and must be measured early* (§16) — done:
  1,000 ev/s sustained, webhook p99 = 51 ms, SSE p99 = 88 ms, oracle clean.
  ClickHouse is the pressure point.
- *Filter language: CEL or CloudEvents SQL* — CEL works well, including the
  equality pre-index.
- *Ordering key: definition, default, no-key behavior* — implemented:
  `subject`, `ext:<name>`, `json:<path>`; default is the publisher's
  partition key; unresolvable keys hash to one partition, order not
  guaranteed across keys. Documented for consumers.
- *Fixed partition count for the deliveries topic* — 12, fixed; the sequence
  `(partition, offset)` depends on it.
- *Webhook security* (§16 calls it its own block of work) — Eventarium has a
  real first version: SSRF guard, no redirects, HMAC signatures, per-request
  timeout.
- *ClickHouse licence risk* — Apache 2.0; no issue.
- *Failover latency is franz-go's default, nobody chose it* (§16) — now an
  explicit pair, `KAFKA_SESSION_TIMEOUT` / `KAFKA_TRANSACTION_TIMEOUT`, read
  together in `kafkautil.Timeouts()` (which enforces franz-go's rule that the
  transaction timeout stays *below* the session, and clamps rather than
  rejects) and applied to every client including the router, which builds its
  client through `NewGroupTransactSession` and is the one place where both
  halves matter. Both were **lowered from 45 s / 40 s to 15 s / 10 s** on
  measurement: the `rebalance` scenario reports a 13–16 s handover against
  46–45 s at the inherited value, with an identical oracle
  (`missing=0 unexpected=0`) and an identical abort count — one per run, the
  documented cold-start case at 54 ms elapsed, `past_txn_timeout=false` at
  both, so 10 s holds even at 1,000 ev/s. A transaction that does outlive its
  budget is re-consumed, so the failure mode is duplicates, never loss. The
  conservative pair stays one environment variable away.

## Open points — still open

From §16, minus what Eventarium answered:

- **Hot/cold handover and failover with ordering** — the spec itself calls
  this the most error-prone part: specify as state machines, test with fault
  injection. The fault injection is done for the dispatcher (breaker
  recovery, kill/restart rejoin, parked backlog survival) and for a
  single-node SSE gateway restart with reconnecting clients (see the
  results below); the state-machine specification and failover **across
  nodes** — a second gateway node taking over a dead node's connections —
  are still open, §17 step 4-5 territory (the replay *service* is built —
  see below — but the formal state-machine specification of §17 step 4 is
  not written).
- **Backlog limit per subscription** — without one storage grows unbounded
  during outages; with one, `dropped` appears and conflicts with guaranteed
  delivery. Unresolved product decision.
- **franz-go's `ErrFirstReadEOF` is classified as a fatal producer-ID error**
  — upstream, and the reason publish-api replaces its transactional client when
  `BeginTransaction` fails (full write-up in the chaos section). The library's
  own comment says it means to treat "a transient network error … from a broker
  restart" as retryable, but the predicate for that is `isAnyDialErr`, which
  only matches a *dial*-phase `*net.OpError` — and this EOF happens after a
  successful dial, during ApiVersions. Until that is fixed upstream, the
  workaround is load-bearing and any upgrade should be checked against it: the
  latch is timing-dependent, so it looks like an unrelated flake when it fires,
  and it hits `NewGroupTransactSession` users too (the router escaped by luck
  in the run we measured).
- **A subscription is unreachable for ~50-60 ms after it is created** — found
  by the `rebalance` scenario, measured by polling `GET /sse/ack` on all three
  gateways (the NOTIFY fast path; the 1 s poll is not what makes it this
  small). Inside that window `GET /sse` answers `404: unknown subscription`.
  For a retrying client an inconvenience; for one that does not, permanent -
  the harness's SSE client exits on 404 (`elapsed=0.0s reconnects=0`) and so
  does a browser, because `EventSource` stops for good on a 4xx instead of
  reconnecting. A client dead at connect time never publishes presence, so its
  whole subscription's deliveries then park as `no_connection` with nothing
  left to drain them: 4 of 20 clients and 3,187 undelivered rows in one run.
  The window is inherent - another process's in-memory index cannot be atomic
  with your write - so the choice is only about how to live with it. The chaos
  driver waits for visibility before starting clients (test side, done);
  platform side, in increasing order of work: a **reload-on-miss** in the
  gateway's 404 path (one extra query on an error path, ~50 ms to ~5 ms, but
  it does not close the window), making it part of the API contract ("a
  subscription may not be immediately connectable"), or having clients retry -
  right for a fetch-based client, wrong for `EventSource`, so it only helps
  some callers.
- **Commit-watermark scan is O(backlog)** — found while re-measuring the load
  test, not observed as latency. `offsetgate.advance` walks the whole `pending`
  map to find the lowest offset, and `prune` walks `epochs`, so one `Settle`
  costs a scan of everything still in flight on that partition:
  `BenchmarkSettleUnderBacklog` in `internal/offsetgate` measures **4 µs at a
  256-deep backlog, 71 µs at 4,096 and 282 µs at 16,384**. Three 1,000 ev/s
  runs land on the previously documented latencies (p99 52 ms webhook / 92 ms
  SSE), so at this rate it is not on the critical path — but a partition whose
  store went away accumulates exactly that kind of depth, and then every
  delivery pays a linear scan while holding the gate lock (which `Track` and
  `Pending` also need). Cheap fix if it ever shows up: keep `pending` as an
  indexed min-heap instead of a map, or track the low-water mark incrementally
  and rescan only when the low-water offset itself settles. Not done, because
  nothing measured justifies it yet and the alternative (optimising ahead of
  data) is how the last "obvious" fix gets regretted.
- **The ClickHouse scenario never asserted status-row completeness** — its
  `missing=0` is evidence about *deliveries*: the receivers recorded all
  58,366 of them, so the oracle had no way to notice a status row lost while
  ClickHouse was down. The writer is *designed* not to drop (buffer kept on
  insert failure, backpressure at `maxBuffered`, Kafka offsets committed only
  after a successful insert), so this is an unverified claim rather than a
  suspected bug — but "designed not to" and "measured not to" are different
  statements, and §9 makes status the only record of what happened. Cheap to
  close: count `delivery_status` rows for the run's owner against the
  transitions that run must have produced, while the outage is still open
  rather than after the drain.
- **Offline share of subscriptions** — unknown; decides inbox sizing and the
  managed PostgreSQL load (IOPS, WAL, failover). The multi-day offline
  scenario hasn't been run.
- **Meaning of `delivered` for SSE** — Eventarium implements cumulative
  `Last-Event-ID` acks (only an ack turns `sent` into `delivered`), but
  whether that is *the* semantics is still open. Related gap found during
  testing: the gateway only accepts tokens in the `Authorization` header, so
  browser `EventSource` clients would need a query-string token or cookie —
  documented in the README, not built.
- **Ordering across retention classes** — events in different class topics
  have no relative order; the ordering scope (same class? ordering group?) is
  defined in the registry but not enforced or documented to consumers.
- **Replay** — the service is built and tested (results below). Still open:
  backfill for events that never matched the subscription (the spec itself
  defers this decision, §17/§11), and an exclusive mode that pauses live
  delivery during a replay (spec: possible later extension). Decided: a replay
  never re-evaluates filters (§11 "replays existing deliveries only"); it
  stamps the *current* projection and channel, so it follows a `PUT` like
  every other producer of delivery records.
- **Publisher idempotency** — publishes are not deduplicated by design;
  whether publishers get an optional `Idempotency-Key` is undecided. (Max
  event size *is* handled: `MAX_EVENT_BYTES`, default 1 MiB.) It is now also
  the only answer left to two ambiguities the platform cannot remove: a
  **client timeout** on a publish (the server may have finished after the
  client gave up) and a **lost commit acknowledgement**. Batches are
  transactional since the chaos run, so a 500 means nothing was written and
  partial batches are gone — but those two remain whole-request uncertainty
  that no server-side protocol removes for a caller who cannot retry blindly
  without risking duplicates.
- **AuthN/Z end state** — JWT claim names, IdP setup, owner-based routing
  restrictions (§6.7). HS256 stands in.
- **Per-target-host/tenant rate limits** — explicitly out of scope for v1
  (needs a shared counter), per §6.

## Mutable subscriptions: `PUT /v1/subscriptions/{id}`

Full replace, same body as `POST` (README has the call). What was decided and
what it costs:

- **Verdicts are frozen at routing time, configuration is not.** A filter
  change applies to *newly published* events only; a delivery that already
  exists keeps the filter it was routed with. Drain does not re-evaluate
  filters either — parked rows leave with the policy of the moment they are
  drained — and replay never re-evaluates them (§11).
- **The channel follows the record, wherever it is.** `registry.ChannelFor`
  (approved addition to the frozen package) answers "current channel if the ID
  is known, stamped channel otherwise" for both dispatcher and sse-gateway —
  one shared rule, because two rules could let *both* stages skip a record
  (lost) or *both* take it (duplicated). The stamp only decides for IDs
  nobody knows any more (deleted subscription), which keeps an orphaned
  webhook delivery dropping as `unknown_subscription` instead of disappearing
  between the stages.
- **Startup: load the registry before consuming.** The dispatcher and the
  sse-gateway used to start consuming with an empty index — the first reload
  runs inside `reg.Run`, a goroutine. In that window `ChannelFor` resolved
  every unknown ID to its stamp, so a record whose channel had been changed
  since routing was skipped by *both* stages: **lost**. Fixed by doing the
  synchronous `reg.Reload(ctx)` before the consumers start, exactly as
  publish-api, router and replay-api already did. This predates `PUT` (a
  dispatcher starting cold dropped everything as `unknown_subscription`),
  but `PUT` is what turned it into a silent loss instead of a loud one.
- **Observed live, once:** a consumer restarted *across* an update read the
  pre-update configuration while the other stage had already moved on;
  delivery `15caed88-…` stayed `pending` forever with zero attempts on either
  side. Same root cause as above, one reload wide.

### The reload-window race (open point)

Even with a loaded index, a `PUT` that flips the channel is not atomic with
respect to the two stages: each one reads the registry at its own instant, and
NOTIFY lands in each process separately. So for the handful of milliseconds
around the flip, two outcomes are possible for one in-flight record:

- **both stages see the new channel** → the old owner skips it and the new one
  takes it → correct;
- **one stage sees old, one sees new** → both skip → **lost**, or both take →
  **duplicate**.

Which one you get depends on which stage reloaded first, and neither can be
eliminated by reading more data: the two stages are separate processes with no
shared transaction. Today the code takes the *risk of a rare loss* in exchange
for never duplicating by construction (each record is owned by exactly one
stage in any single consistent snapshot).

**The alternative** is to decide ownership on `stamped OR current` instead of
`current` alone — a record is owned by whichever stage the stamp *or* the
current config claims, i.e. both stages act if the subscription moved. That
makes a loss impossible (nobody can skip a record the other also skips), at
the price of **systematically duplicating every in-flight record of the
switching subscription**: the old stage delivers it again while the new one
delivers it too. On an at-least-once platform that is a legal trade — receivers
are supposed to be idempotent on `pubsub-delivery-id` — but it turns a
milliseconds-wide, once-per-update race into a guaranteed duplicate burst
proportional to the in-flight window.

Not built; would be a ~10-line change in `registry.ChannelFor` plus the two
`ownsChannel` helpers. Recommendation if the platform ever needs "no loss, ever"
at this boundary: take the OR, and make the duplication visible (status rows
from both stages) rather than silent.

Also still open and untouched: an update does not rewrite `expires_at` or the
inbox TTL of already parked rows, so a webhook⇄SSE switch leaves rows waiting
for the other stage until they expire there.

## Stability hardening: five failure modes, what each one cost and gave back

Five questions about what happens when things break, the answer to each, and
the change it produced. Two of them were real data-loss paths.

1. **Data loss on a crash or a store outage — fixed, at the price of
   duplicates.** Both delivery stages used to commit their Kafka offsets as
   soon as a record was *owned in memory*: the dispatcher marked a record the
   moment it was queued, the sse-gateway committed the whole poll batch after
   processing it. A crash in between lost the delivery silently — the offset
   was committed, the work was not done. Now the commit point is a watermark
   (`internal/offsetgate`, an approved new package): a record's offset is
   released only when its delivery reached a durable outcome — delivered,
   dead-lettered, terminally dropped, on `sse-live`, or parked in the inbox.
   What is still in flight (queued, retrying, waiting for a park) holds the
   partition's commit point still, so a crash *repeats* the batch instead of
   skipping it; the duplicates are identified by the event reference (§8).
   A park that the store refused used to be dropped with the record still
   committed; it now goes to a bounded background retry queue (backpressure
   rather than memory), and its offset stays uncommitted until the row is in
   the inbox. The one deliberate exception is a payload that cannot be encoded
   for the inbox: no amount of retrying fixes that, so it is a terminal drop
   with a `dropped`/`park_failed` status — a loss with a record of it, not a
   silent one. Accepted and documented: the dispatcher's status emitter is
   asynchronous (50 ms / 512 records), so a crash can lose up to 50 ms of
   *status rows* while the offsets commit — observability only, never a
   delivery.
2. **Readiness — fixed.** The five APIs exposed only `/healthz`, so a rolling
   update declared a replica good two seconds after it started, before its
   registry was loaded and with a database it could not reach. Every service
   now also serves `/readyz` (`internal/httpx.Health`, `Register` onto the
   API mux, `RegisterReady` for the gateway, which keeps its own `/healthz`
   body): PostgreSQL for the APIs and the gateway, ClickHouse additionally for
   status-api and replay-api, and for everything that routes on the registry
   an index that is not standing still (`Registry.Check`). The chart's five
   `readyPath` values moved from `/healthz` to `/readyz`. Liveness is
   unchanged and still never fails because a store is down: restarting a
   consumer over a database outage gains nothing and only disturbs the
   consumer groups.
3. **Deleting a subscription was a hard `DELETE` — fixed, softly.** The row
   was the only subject a parked delivery, a status record or a replay could
   be attributed to, and a hard delete took it away: the status API answered
   404 for history that still existed in ClickHouse, and a delivery in flight
   became `unknown_subscription` noise instead of a traceable drop. Migration
   004 adds `deleted_at`; `DELETE` stamps it, and everything that answers
   "what is configured now" (registry index, control-plane GET/PUT/DELETE,
   list) filters it out, so the API contract is unchanged — 404, and the
   second delete is a 404 too. Only the attribution path
   (`store.SubscriptionExists`) counts deleted rows.
4. **The status API answered 200 with an empty list for IDs that never
   existed — fixed.** `/v1/subscriptions/{id}/deliveries` and
   `/v1/subscriptions/{id}/status-summary` asked ClickHouse, which holds
   delivery records and no subscription metadata (§9), so an arbitrary ID
   looked like a valid subscription with no deliveries. Both endpoints now
   look the ID up in PostgreSQL first, owner scoped, including soft-deleted
   rows; unknown and foreign answer the same 404 (§6.7), and a database error
   answers 500 — the check fails closed, because "no rows" and "we could not
   check" must not look alike.
5. **A registry that stopped reloading failed silently — fixed.** A reload
   failure was one warning per attempt and nothing else: the index kept
   serving whatever it last loaded, `/healthz` stayed green, and nothing said
   *how long* the snapshot had been standing still. The registry now counts
   consecutive failures, remembers the last error, and `Check(maxAge)` turns
   that into a readiness answer (`REGISTRY_STALE_AFTER`, default 30 s, so a
   single slow reload cannot flap a rollout). The reload log is one line per
   distinct error with the consecutive count and the index age, plus the
   recovery line — one line per attempt would bury the log exactly while the
   database is down.

## Chaos during load: crashed replicas and dead infrastructure (§17)

Four scenarios, each "publish at 200 ev/s for 60 s, inject one fault in the
middle, let it drain, then ask the oracle". Same topology as the load test:
4 types, 40 subscriptions (20 webhook, 20 SSE), 20 SSE clients spread over
three gateways, 12,000 events per run → `expected=58,366` deliveries. Unlike
the load test, this runs on **nine replicas** — three routers, three
dispatchers, three sse-gateways — because killing one of one is not a failure
mode. The dispatcher's inbox duty elects a leader over a PostgreSQL advisory
lock, the SSE drainer runs on `node-a` only, and each gateway owns its own
`sse-live-<NODE_ID>` group.

Every run seeds its own owner (`pods232146`, `pg231403`, …), which matters:
the status table is cumulative and the oracle scopes its query to the run's
publish window plus a minute of skew, so an earlier run whose `pub.jsonl` is
gone leaks into the next one as `unexpected`. That is not hypothetical — an
aborted scenario did exactly this and produced 18,432 phantom `unexpected`
before the harness was fixed.

The driver is committed rather than described — `scripts/chaos.sh`, against the
topology `scripts/stack.sh` starts:

```sh
make infra            # Kafka, PostgreSQL, ClickHouse plus topics and schemas
make stack            # five services, three replicas of each consumer
make chaos SCEN=pods  # pods | pg | kafka | ch
```

Its shape, spelled out for anyone who would rather read the four screens than
the script:

```sh
harness seed -api :8083 -owner $O -prefix $O -types 4 -subs 40 \
  -webhook-url http://localhost:9090/hook > seed.json   # own owner per run
harness recv -addr :9090 -out recv.jsonl &              # fresh file per run
harness sse -sub <id> -owner $O -addr http://localhost:8082 \
  -duration 1800s -out sse-<n>.jsonl &  # 20, over :8082/:8182/:8282
API=:8080 HARNESS_OWNER=$O harness pub -types "$O.type.0,..." \
  -rate 200 -duration 60s -payload 1024 -out pub.jsonl &
docker compose stop postgres          # | restart kafka | stop clickhouse
kill -9 <dispatcher pid>              # and/or router, gateway
# assert the fault actually landed, then wait for queue_depth=0 and for the
# owner's inbox rows to reach 0:
harness verify -owner $O -api :8083 -status-api :8081 \
  -events pub.jsonl -received recv.jsonl -sse "$(ls sse-*.jsonl | tr '\n' ,)"
```

Three properties in the driver are load-bearing, and each one exists because
getting it wrong produced a result that looked fine:

- **Faults are asserted, not assumed** (`assert_down` / `assert_up`). An
  unquoted `$DOCKER` word-split once made every `docker compose stop` exit 127
  into `/dev/null`, and the first `pg` run reported success with PostgreSQL
  never stopped.
- **The judge waits for the queues *and* the owner's inbox to reach 0.**
  Judging early reports work that is merely unfinished as lost. It also
  matters because `missing=0` only means *tracked*: `accounted()` counts
  `pending`, so the driver separately fails the run when `received` falls short
  of `expected` — parked-but-undelivered deliveries would otherwise pass.
- **Each run seeds its own owner**, so an earlier run's events cannot leak into
  the next run's status window as `unexpected`.

The two waits are not ceremony: judging before the queues drain reports work
that is merely unfinished as lost, and judging before the inbox drains reports
parked-but-durable deliveries as missing. The SSE clients get `-duration 1800s`
for the same reason — the waits alone can run 13 minutes, and a client that
exits mid-wait strands its parked rows with nothing left to drain to (which is
exactly what the first `ch` run of the committed script did: 81 rows and a
`received` 81 short of `expected`).

| Scenario | fault | missing | unexpected | duplicates |
| --- | --- | --- | --- | --- |
| `pods` | `kill -9` one dispatcher, one router, one gateway | **0** | 0 | 7,293 |
| `pg` | receiver offline **and** PostgreSQL down, 20 s overlap | **0** | 0 | 0 |
| `ch` | ClickHouse down 20 s | **0** | 0 | 0 |
| `kafka` | broker restart mid-run | **0** | 0 | 2,746 |
| `rebalance` | `kill -9` one dispatcher, handover measured against the session timeout | **0** | 0 | 533–16,554 (see below) |

**`missing=0` and `unexpected=0` in all five** — the two numbers this whole
exercise exists for.

The first four rows are from one pass against the current code (owners
`pods020953`, `pg021223`, `ch022133`, `kafka022326`), re-run after publish-api
went transactional so the table would not mix binaries; `rebalance` is
`rebalance101220` and four later runs at both timeout pairs, whose duplicate
counts are discussed below. The `kafka` row's
`expected` is lower than the others (52,536 vs 58,366) because 1,200 events
were *correctly* not written while the broker was restarting — see the
`unexpected` discussion below, which is where that number used to go wrong.

Earlier `kafka` rows in this document reported `unexpected=5,185`. That was
publish-api's partial batch, fixed in the same round of work; the run is kept
below because it is the measurement the fix was made against.

### What the duplicates are, precisely

Duplicate volume varies with the run — three runs of `pods` produced 7,698,
5,732 and 7,293 — because it tracks how far behind the killed partition's
watermark was, not how big the crash was. Where they *land* does not vary. On
two runs the sub→partition map was taken from the topic itself and the
duplicates sat on **partitions 8, 9, 10 and 11 and nowhere else**, which is
exactly the four partitions the killed dispatcher owned (checked against
`kafka-consumer-groups.sh --describe` before the kill); partitions owned by
the two survivors showed zero. The `pg` and `ch` runs — where only a *store*
died and no member left the group — show exactly zero; the `kafka` run, where
a broker restart forces a rejoin, shows the same replay effect.

The mechanism, confirmed end to end rather than assumed:

- **It is replay, not re-production.** Consuming `deliveries` and grepping for
  one duplicated `delivery_id` returns **1 record** while the receiver logged
  it twice: the same Kafka record was consumed twice.
- **The timing is franz-go's session timeout.** The first duplicate arrives
  *exactly 45 s* after the kill — `sessionTimeout: 45000 * time.Millisecond`
  is franz-go's default. `first_dup − 45 s` lands on the kill within the
  script's own sleep. Nothing happens *at* the kill; the group waits out the
  dead member, then the survivors resume from the last committed offset and
  repeat whatever was in flight.
- **The size is the commit watermark's unflushed tail.** Marks are flushed on
  an interval, so the kill loses up to one flush interval of committed
  progress — plus, on partition 11, everything the two `rate_limit_per_s=50`
  subscriptions had caused to sit below the watermark. That is why one
  partition carried ~2,200 duplicates while its neighbours carried ~225.

This is fix 1 from the previous section working as designed: the alternative
to these duplicates was dropping them. The webhook receiver is where replay
shows up at all — the SSE client deduplicates by event reference (§8) before
writing, so its file reports 0 by construction and proves nothing about what
the gateway sent.

### rebalance: the handover, measured

The scenario asserts a number rather than only a verdict: kill one dispatcher
mid-publish, poll the group's assignment until every partition is owned by a
live member, and fail beyond `KAFKA_SESSION_TIMEOUT + 15 s` of polling slack.
Every run below passed the oracle (`received == expected == 58,366`,
`missing=0`, `unexpected=0`):

| pair | run | handover | bound |
| --- | --- | --- | --- |
| 45 s / 40 s | 1 | 46 s | 60 s |
| 45 s / 40 s | 2 | 45 s | 60 s |
| 45 s / 40 s | 3 | 46 s | 60 s |
| **15 s / 10 s (shipped)** | 1 | 13 s | 30 s |
| **15 s / 10 s (shipped)** | 2 | 16 s | 30 s |

That agrees with the hand-rolled measurement above (43 s) within the polling
granularity, and it is now *enforced*: the bound is derived from
`KAFKA_SESSION_TIMEOUT`, so tuning the value moves the bar with it, and a
franz-go upgrade that changed the behaviour would fail the run instead of
quietly moving the number.

Duplicates across those runs were 825, 2,776, 16,554 at the old pair and 628,
533 at the new one — all webhook, none on SSE. The spread is not noise in the
platform, nor a property of the session timeout; it is the watermark rule with
a wider view of itself, and which subscriptions happen to hold their
partitions' commit points back. Three seeded subscriptions carry
`rate_limit_per_s=50`, so they are never finished inside a 60 s run and they
hold *their partitions'* commit points; everything delivered above a stalled
watermark is replayed by whoever takes over. The eight subscriptions that each
duplicated ~2,700 times in one run were those three plus their neighbours —
the same picture the `pods` runs showed one partition wide, wider there because
this run's clients were all alive.

Stranded clients had been hiding it: a client that dies at connect time makes
its whole subscription park durably, which *moves* the watermark and shrinks
the replay.

Two bugs the scenario itself caught, both in the driver rather than the
platform, and both fixed with a test that would have caught them:

- **The bound printed a stale session timeout.** `session_seconds()` fell back
  to a hardcoded 45 s when `KAFKA_SESSION_TIMEOUT` was unset — correct when it
  was written, wrong the moment the defaults were lowered. One run measured a
  right 16 s handover and printed it against a 60 s bound: four times too
  generous, which is worse than no bound at all. The fallback now matches the
  Go default, and `TestSessionSecondsFallbackMatchesDefault` in
  `internal/kafkautil` reads `scripts/chaos.sh` and fails if the two ever
  drift apart again.
- **A receiver that could not bind failed the run as a mystery shortfall.** A
  leftover delivery receiver on `:9090` (a previous run's, or one started by
  hand) made the scenario's own receiver exit with `bind: address already in
  use`; the log line went unread, `recv.jsonl` stayed empty, and eight minutes
  later the oracle reported `received=18653 of 58366` — a platform-shaped
  failure for a driver-shaped cause. `stop_receiver` now clears a leftover
  `harness recv` on the port (only that pattern; anything else keeps it and is
  named), and `wait_for_receiver` refuses to continue until the port is really
  listening, exiting 4 with the bind error if it is not.

### The subscription-visibility window (setup, not fault)

Running the scenario surfaced a race the platform owns. A subscription is
reachable **~50–60 ms after the API returns 201**, measured by polling
`GET /sse/ack` on all three gateways — that is the NOTIFY fast path, not the
1 s poll, so the machinery is working as designed. Inside that window
`GET /sse` answers `404: unknown subscription`.

For a client that retries, an inconvenience; for one that does not, permanent.
The harness's SSE client exits on a 404 (`elapsed=0.0s reconnects=0`), and so
does a browser — `EventSource` stops for good on a 4xx rather than
reconnecting. The consequence then compounds: a client dead at connect time
never publishes presence, so its whole subscription's deliveries park as
`no_connection` with nothing left to drain them.

Measured cost: seeding and connecting back to back lost **4 of 20 clients and
3,187 undelivered rows** — the oracle reported `received=55179 of 58366` and
failed the run, which is the correct verdict on a race the *setup* caused.
After the driver waits for visibility, the same run lost none.

The wait is the test-side fix; the platform-side options are an open point
below.

### publish-api batches are now transactional (was `unexpected=5,185`)

The only non-clean row of the first pass, and the fault was publish-api's, not
the pipeline's. The publisher reported `sent=12000 ok=10900 failed=1100
(5xx=1000 timeouts=100)` — yet 533 event references it recorded as failed
turned up at the receiver, accounting for the whole `unexpected=5,185`.

Mechanism: `ProduceSync` writes the whole request before the handler inspects
the results, and the handler returned 500 on the *first* record that errored
(after `cmd/publish-api/main.go:243`), so the other ~99 records of that request
were already in the log while the client — correctly, by HTTP semantics —
discarded all 100. 20 produce errors, 20 HTTP 500s, 1,000 events reported
failed.

Decision: make a batch all-or-nothing. `publishAPI.write` sends a single record
down the plain path — one record is atomic, there is nothing partial about it,
so the usual case pays for nothing — and a multi-record request inside one Kafka
transaction, with the handler's reply finally able to mean what it says.

The same scenario against the fix: **1,200 events reported failed, 12 HTTP 500s
(11 of them the latch described next), `unexpected=0`** — every failed request
wrote nothing — with `ok=10,800` against `10,900` before, i.e. the same number
of successes. The load test re-run says the same about cost: **1,003.0 ev/s,
30,000/30,000 ok, p99 59 ms webhook / 90 ms SSE** against 52/92 ms before,
inside the 52–94 ms spread already measured across three baseline runs.

Two things it deliberately does not fix, and one cost worth knowing:

- A **client timeout** and a **lost commit acknowledgement** are whole-batch
  ambiguity, not partial writes — they exist for a single event too and only
  publisher idempotency (open point) resolves them. Hence the reply wording:
  "no event of this request was written" when that is known, "the outcome could
  not be confirmed" when it is not.
- **Batches are serialised** through one mutex, because a producer has exactly
  one transaction in flight and `BeginTransaction` must not run concurrently
  with the client's other work. That caps batch throughput at 1/txn-duration;
  at 10 batches/s of 100 events it is nowhere near a constraint, but it is the
  number to revisit if the batch rate ever rises by orders of magnitude. The
  serialisation is also why single-record publishes get their own client: they
  are never queued behind a transaction.

### franz-go latches a restart into a permanent producer-ID failure

Found while verifying the transaction above, and it nearly made the fix worse
than the bug.

`BeginTransaction` runs `maybeRecoverProducerID`, which classifies a stored
producer-ID error as either retryable or fatal. An `ErrFirstReadEOF` — a
connection that dies *after* the dial, during ApiVersions, which is exactly
what a broker restart produces — unwraps to `io.EOF`, so it is neither a
`*net.OpError` with `Op=="dial"` (`isAnyDialErr`) nor a broker SQLSTATE
(`isRetryableBrokerErr`), and lands on `return true, false, err`: **fatal,
unrecoverable**. Nothing in the library clears it, and `BeginTransaction` then
fails in 0.5 ms, forever.

Measured: after one broker restart, **99 latched calls and 9,900 of 12,000
requests failed** for the rest of the run, while the plain single-record client
— which never enters this path — was unaffected. The library's own comment says
it intends to catch "a transient network error … from a broker restart", so
intent and predicate disagree; it is an upstream issue, not ours.

The workaround is load-bearing: on a failed `BeginTransaction`, publish-api
closes the client and opens a fresh one (only a new client re-runs
`InitProducerID`), then retries the begin so the request that *detects* the
latch usually succeeds instead of being sacrificed to it. A replacement that
cannot be opened leaves the client nil, and "no client" is retried rather than
becoming a permanent state — otherwise an outage would kill the batch path just
as effectively as the latch does. Re-run after the fix: **11 latched calls, all
inside the outage window, 1,200 failures total** instead of 9,900.

Pinning this down matters for anyone else reaching for franz-go transactions:
the `router` uses the same machinery through `kgo.NewGroupTransactSession` and
came through this run unscathed, so the latch is timing-dependent rather than
certain — which is the worst kind, because it looks like an unrelated flake
when it fires.

### Readiness, observed rather than asserted

- During the PostgreSQL outage, `dispatcher /readyz` → **503** while
  `/healthz` stayed **200**, on all three replicas; recovery confirmed back to
  200 afterwards.
- During the ClickHouse outage, `status-api /readyz` → **503** while its
  liveness stayed 200 *and the dispatcher stayed ready at 200* — the
  dependency boundary is where it should be, and deliveries continued
  untouched (`missing=0`, zero duplicates).
- A registry that stopped reloading says so with its age:
  `{"error":"registry index is 9s old (limit 5s)"}`.
- The outage also exercises the park path end to end: `offsets_in_flight=4,307`
  while both the receiver and PostgreSQL were down, `parking recovered,
  retries drained` once the store returned, then the inbox drained to 0 rows
  and the oracle read `expected=58366 received=58366 missing=0`.

### Bugs this round found (all fixed)

Two came out of the scenarios, one out of starting the topology for them, and
two more out of verifying the first fix — the last two have their own
subsections above (the transactional batch, and the franz-go latch that nearly
made it worse).

1. **Status records stamped the wrong source position** — `emitStatus` wrote
   `it.seq` (the delivery's position on the *deliveries* topic) into
   `SrcPartition`/`SrcOffset`, which mean the event's position on the *events*
   topic. One delivery's history disagreed with itself: `pending` said
   `events:4:4113`, `in_flight`/`delivered` said `deliveries:2:133500`. 33.1%
   of deliveries in every pre-fix run; 0% after. Replay only survived because
   `argMin(src_partition, ts)` happens to pick the router's `pending` row,
   which has the earliest ts. Regression tests in
   `internal/dispatch/status_test.go`.
2. **The oracle made a delivery *more* missing the further it got** —
   `{delivered, sent, pending}` was the accounted list, but `in_flight`,
   `retrying` and `paused` all come *after* `pending`. A delivery parked
   during the PostgreSQL outage read as `missing` while sitting in the inbox.
   That is exactly the run that had to prove the opposite. Now any status
   except `filtered` counts as accounted.
3. **A replica could exit on start-up over a schema that was already correct** —
   `EnsureInboxPartitions` creates each day's partition with
   `CREATE TABLE IF NOT EXISTS ... PARTITION OF`, and that `IF NOT EXISTS` is
   **not atomic for partitions**: six concurrent creators of one fresh
   partition produced five `CREATE TABLE` and one
   `ERROR: relation "…" already exists (SQLSTATE 42P07)`. The dispatcher
   treated the loser's error as fatal and exited — so a rolling update could
   shed a pod over a schema that, by then, was already correct. It bit during
   this session: `d1` died at start with `inbox_p20261108`, because that date
   was new (the previous day's run had only needed dates up to 11-07). The
   race is therefore a *first-deploy or day-rollover* event, which is exactly
   when every replica is starting at once. `42P07` now counts as success —
   it is the state `IF NOT EXISTS` asked for — and
   `TestEnsureInboxPartitionsIsSafeConcurrently` runs 16 creators over one
   fresh day: it fails on 4 of 16 without the fix, passes with it.

### What the harness itself got wrong (worth repeating)

Three harness faults, not platform faults — and they fail in *opposite*
directions, which is why each one is worth writing down: one made a scenario
pass with the fault never applied (the dangerous direction), the other two
made clean runs fail for reasons that had nothing to do with the platform
(equally wasteful, and the kind of thing that gets a real bug hunted for a
day):

- `$DOCKER` was unquoted in the scenario script and the path contains a
  space, so every `docker compose stop` exited 127 into `/dev/null`. The first
  `pg` run reported success with PostgreSQL never stopped. Fixed by quoting
  **and** by `assert_down`/`assert_up`, which fail the run unless the fault is
  observed to have landed (`probe` also has to fail on a non-200 — `curl` exits
  0 on a 503, so a naive probe makes the assertion vacuous).
- The receiver was killed at scenario end, so the inbox could never drain and
  the judge read parked deliveries as missing — a **false failure**: parked
  and durable, reported as lost. Fixed: `wait_inbox` waits for the owner's
  parked rows to reach 0 before the oracle is allowed to speak.
- Scenarios shared one owner across runs, so a previous run's events fell into
  the next run's status window — another **false failure** (18,432 phantom
  `unexpected`). Fixed: unique owner per invocation, plus a `trap … EXIT` so
  an aborted scenario cannot leave a publisher running.

## Fault injection: dispatcher, SSE gateway and cursor recovery (§17 test plan)

Run against the live stack — webhook subscription `73d5d2d2` (`fault.cb`),
SSE subscription `45b2bf24` (`fault.sse`), receiver = `harness recv` with its
fault flags, verdict = `harness verify` plus client-side event-reference
accounting. All seven scenarios passed: **zero missing deliveries** in every
one.

1. **Circuit breaker, receiver offline 60 s** (450 events at 5 ev/s).
   Breaker opened 5 s after the receiver went down (`failures=5`,
   `probe_interval=5m0s`); 410 deliveries parked into the PostgreSQL inbox
   (`queue_depth=0` — nothing even queued while open). The receiver returned
   60 s later, but the breaker correctly held it off until the probe fired
   **exactly 5 min** after opening (`closed after successful probe`); the
   backlog then drained and the oracle confirmed `450/450, missing=0,
   duplicates=0`.
2. **Dispatcher hard-killed (SIGKILL) mid-stream, then restarted** (600
   events at 20 ev/s). Zero loss; **75 duplicates** — the at-least-once
   contract made visible: franz-go's autocommit interval is 5 s, so
   deliveries accepted in the ~5 s before the kill were received but their
   offset marks were not yet committed, and the restarted consumer
   re-delivered them from the last committed offset. Rejoin took ~40 s
   (franz-go's default 45 s group session timeout bounds it: the dead
   member's session must expire before the group reassigns).
3. **SIGKILL while a parked backlog exists and the endpoint is still down**
   (30 parked rows). The inbox survived the restart (it is PostgreSQL, not
   memory); the restarted dispatcher re-attempted the rows, re-opened the
   breaker after 5 fresh failures, and drained everything after the next
   probe: `30/30, missing=0, duplicates=0`.
4. **SSE gateway graceful shutdown (SIGTERM) with a connected, acking
   client** (400 events at 10 ev/s). All four in-process roles stopped in
   order, disconnect tombstones published, clean exit; restarted in 3 s.
   51 events reached the client live before the shutdown; the remaining 349
   parked into the PostgreSQL inbox while no client was connected, and the
   reconnecting client drained them **instantly** (inbox 349 → 0): client-side
   accounting `400/400, missing=0, duplicates=0`, oracle
   `delivered=400, missing=0`.
5. **SSE gateway hard-killed (SIGKILL) mid-stream with a live client, then
   restarted** (1,200 events at 20 ev/s). The client saw `unexpected EOF`
   after 201 events; the gateway restarted 4 s later. The consumer groups
   (`sse-delivery`, the presence view, `sse-live-<node>`) re-joined only
   after the dead member's ~45 s session expired; during that window
   deliveries parked (999 rows, no client connected), and the reconnecting
   client drained all of them: client-side accounting `1200/1200,
   missing=0, duplicates=0`, oracle `delivered=1200, missing=0`. Notably
   **zero loss in the theoretically lossy window** — deliveries already
   written to the `sse-live` hop toward the dying connection: because the
   batch-level offset commit did not survive the kill, those deliveries
   replayed after rejoin, by which point the connection no longer existed,
   so they parked instead of being dropped. The window exists (a
   sub-millisecond race between live-write and commit would lose it) but did
   not materialise.
6. **Client killed mid-stream, reconnected by cursor** (150 events, SSE
   subscription `45b2bf24`). The first client was SIGKILLed after 30 events;
   100 more were published with no connection (inbox 0 → 119). A second
   client connected with `-last-event-id 0:19191`: the gateway replayed the
   gap from the deliveries topic in **19 ms** (`sent=120`), the inbox drained
   to 0, and the union of both client files was `150/150, missing=0,
   duplicates=0`, strictly increasing sequences.
7. **SIGKILL of the gateway, client auto-reconnect with cursor** (1,200
   events at 20 ev/s). The gateway was killed mid-stream and restarted 4 s
   later; the harness reconnected automatically with `Last-Event-ID`
   (`0:19833`) and was served **100 events through the cursor path** 0.99 s
   after the gateway came back (`elapsed=24ms`), then continued live after
   the ~40 s consumer-group rejoin: `1200/1200, missing=0, duplicates=0`,
   sequences strictly increasing across the whole run. This is the path that
   recovers the window test 5 could only cover by parking: deliveries whose
   connection died in the `sse-live` hop are re-read from the topic because
   the cursor fetch starts after the last id the client actually received.

Two behaviours worth knowing, both by design but visible here:

- **Breaker state is in-memory and resets on restart.** A restarted
  dispatcher briefly re-attempts a parked backlog even though the endpoint
  is still down (one round of ≤ threshold failures, then the breaker
  re-opens). Bounded and harmless in test 3, but a restart storms a dead
  endpoint once.
- **Duplicates, not losses, on restart** — receivers must be idempotent
  (spec §8's at-least-once). The duplicate window is bounded by the
  autocommit interval (5 s by default). The SSE tests measured *zero*
  duplicates: the killed gateway had not committed the batch it was
  delivering, and the parked-row path (inbox + drainer) hands each row over
  exactly once per connection.
- **SSE recovery is cursor-first since the `Last-Event-ID` feature.** On
  reconnect the gateway replays everything after the client's cursor from the
  `deliveries` topic (bounded by `backlog_max`, 15 s safety timeout, gap
  comment when retention moved past the cursor) and, as a cursor-aware drain,
  deletes the inbox rows it served so the drainer cannot double-send them; a
  per-connection served set drops any remaining overlap. Without a cursor the
  old drainer path still applies. Live-class records are excluded from the
  backlog (§12: hot path only). Two known residuals: the fetch is
  best-effort (on timeout or error the event loop simply continues and the
  client's next reconnect retries), and a residual race exists where the
  `sse-live` consumer restarts at the latest offset — records produced
  between its rejoin and the delivery stage's rejoin would be skipped (not
  observed in any test, timing-dependent).

Not exercised yet (§17 test plan): the soak test, the multi-day offline
scenario, PG/Kafka/CH outages beyond the receiver-level faults above, and the
replay worker's restart-mid-job recovery (a `running` job is deliberately put
back to `pending` on startup and re-executes from the start — duplicates
acceptable, losses not — but that path was not observed live). The SSE
gateway's multi-node behaviour (a second gateway node taking over a dead
node's connections) was also not exercised — single-node restarts only.

## Replay API: results (§11)

`replay-api` on `:8084`, all endpoints on the `replay` scope and
owner-scoped. Selector = `statuses` (current status) ± time range (by
ingestion time) ± `event_refs`, at least one required, delivered in original
ingestion order. Test runs against subscription `d3df8fd9` (webhook,
`replay.final` events, receiver `harness recv`):

1. **Status selector** — receiver answered 410 to 30 deliveries (deterministic
   `dead_lettered`; a 410 counts as breaker *success*, so no parking), replay
   job `matched=30, enqueued=30, not_replayable=0`, progress
   `delivered=30, failed=0`, and all 30 receipts carried the
   `pubsub-replay-id` header.
2. **Time-frame selector** — 20 `delivered` events inside an explicit
   `from`/`to` window: `matched=20`, receipts +20, none outside the window.
3. **dry_run** — same selector with `"dry_run":true`: `matched=20,
   enqueued=0`, zero receipts arrived.
4. **Idempotency-Key** — repeated POST returns the same `replay_id`, one row
   in `replay_jobs`.
5. **Guards** — `event_types` → 400 (honest: §9 records carry no type),
   empty selector → 400, unknown status → 400, `from` after `to` → 400,
   foreign subscription and foreign replay ID → 404, missing scope → 403,
   second job while one is active → 409 with the active ID, cancel of a
   finished job → 409.
6. **not_replayable, both branches** — a status row pointing at a pruned
   events-topic offset and a row whose `expires_at` had both passed:
   `matched=2, enqueued=0, not_replayable=2`.
7. **Cancel of a running job** — stopped exactly at a batch boundary:
   `matched=601, enqueued=400` (two batches of 200), 400 receipts.
8. **`filtered` is never replayed** — `statuses:["filtered"]` → 400 with a
   message that says why (`… the filter rejected this event, so no delivery
   ever existed for it`), and, more importantly, a time-range or
   `event_refs` selector (neither names a status) matched **0** for a
   subscription whose only rows were `filtered` — before `Resolve` excluded
   them it matched 2 and would have re-delivered both rejected events. A
   time-range selector over the same window on a subscription with real
   deliveries still matched 2, so the exclusion is surgical.

Behaviours worth knowing:

- **`src_partition`/`src_offset` are overloaded in `delivery_status`** —
  events-topic coordinates only on the router's `pending` record,
  deliveries-topic coordinates on every later record. The selector must take
  `argMin(ts)`, never `any()`: 1,965 deliveries in the dev database
  disagreed between the two before the fix. Any consumer needing the event
  position has to take the *earliest* record.
- **Millisecond ties in `argMax(status, ts)`** made the current status
  ambiguous when two transitions landed in the same millisecond (an instant
  410 answered before `in_flight` was written, for example) — observed live
  for 63 of the 90 deliveries in the test database, which the Status API
  reported as `in_flight`/`http_code 0` instead of `delivered`/`200`.
  **Fixed** by ordering on `(ts, attempt, lifecycle rank)` instead of `ts`
  alone: the rank is the position of the status in the lifecycle, so a
  same-millisecond transition always loses to the one that follows it. The
  key is defined once (`chquery.LatestKeySQL`) and used by the Status API,
  the replay selector and the replay progress counts alike, so all three
  judge "current status" identically; `-latency` on the simulated receiver
  is no longer what decides which status wins.
- **A replay keeps the original delivery ID** (`event_ref/subscription_id`)
  and carries the replay ID separately (message, status records,
  `pubsub-replay-id` header) — consumers must be idempotent by event
  reference (§11/§8).
- **Restart semantics**: a job left `running` by a crash goes back to
  `pending` on startup and re-executes from the start (duplicates possible,
  losses not); `cancel_requested` finishes as `cancelled`. Transactions use a
  `replay-<host>-<pid>` transactional ID so the replayer can never fence the
  router's producer.
