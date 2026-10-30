# Deviations from the spec and open points

Status of this spike against [spec.md](spec.md). The inter-service contracts
are in [SPIKE.md](SPIKE.md); the measured results are in the
[README](../README.md#load-test-at-1000-eventss-spec-152-targets-n1n3).

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
eleven status values are documented in the
[README](../README.md#delivery-statuses).

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

## Open points — answered by the spike

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
- *Webhook security* (§16 calls it its own block of work) — spike has a real
  first version: SSRF guard, no redirects, HMAC signatures, per-request
  timeout.
- *ClickHouse licence risk* — Apache 2.0; no issue.

## Open points — still open

From §16, minus what the spike answered:

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
- **Offline share of subscriptions** — unknown; decides inbox sizing and the
  managed PostgreSQL load (IOPS, WAL, failover). The multi-day offline
  scenario hasn't been run.
- **Meaning of `delivered` for SSE** — the spike implements cumulative
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
  event size *is* handled: `MAX_EVENT_BYTES`, default 1 MiB.)
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
