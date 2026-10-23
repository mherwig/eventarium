# Deviations from the spec and open points

Status of this spike against [spec.md](spec.md). The inter-service contracts
are in [SPIKE.md](SPIKE.md); the measured results are in the
[README](../README.md#load-test-at-1000-eventss-spec-152-targets-n1n3).

## Deviations from the spec, on purpose — five

1. **Registry distribution (§6.1): services poll PostgreSQL every ~1–2 s
   instead of reading a compacted Kafka config topic.** Interface identical,
   only the source differs. This was the first cut when building in parallel
   — one less Kafka contract to get wrong between six components.
2. **Inbox deletion (§6.6): batched deletes instead of a per-subscription
   watermark.** Simpler; the watermark would save deletes at the cost of a
   new table and trickier invariants.
3. **Authentication (§6.7): HS256 with a shared secret instead of JWKS.**
   Dev-only; `minttoken` mints the tokens. JWKS/OIDC is a deployment concern
   that doesn't change the services' code (only `internal/auth`).
4. **Replay service and the inbox state machine: not built** (§17 step 4).
   The `replay_id` field and headers exist end-to-end, but nothing produces
   replays yet.
5. **SSE multi-node routing (§6.5/7): presence records carry the node ID
   instead of rendezvous hashing.** A `sse-live` message is targeted at the
   node that currently holds the connection; no message is dropped because
   the connection lives elsewhere. Rendezvous hashing would assign
   *connections* to nodes up front; we went with "where the connection
   actually is".

One small *extension* appeared during load testing: the status-summary
endpoint (§10, endpoint 4 — optional in the spec) gained an optional `since`
parameter, because the status table is cumulative across runs and the oracle
needed to scope counts to one run.

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
  injection. The dispatcher side of the fault injection is done (see the
  results below: breaker recovery, kill/restart rejoin, parked backlog
  survival); the state-machine specification, the SSE gateway restart with
  reconnecting clients and failover across nodes are still open — §17 step
  4-5 territory alongside replay.
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
- **Replay** — backfill scope, ordering relative to live events, whether
  filters are re-evaluated. Service not built.
- **Publisher idempotency** — publishes are not deduplicated by design;
  whether publishers get an optional `Idempotency-Key` is undecided. (Max
  event size *is* handled: `MAX_EVENT_BYTES`, default 1 MiB.)
- **AuthN/Z end state** — JWT claim names, IdP setup, owner-based routing
  restrictions (§6.7). HS256 stands in.
- **Per-target-host/tenant rate limits** — explicitly out of scope for v1
  (needs a shared counter), per §6.

## Fault injection: three dispatcher scenarios (§17 test plan)

Run against the live stack on subscription `73d5d2d2` (webhook, unordered,
`fault.cb` events), receiver = `harness recv` with its fault flags, verdict =
`harness verify` (independent oracle). All three passed: **zero missing,
zero unexpected deliveries** in every scenario.

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

Two behaviours worth knowing, both by design but visible here:

- **Breaker state is in-memory and resets on restart.** A restarted
  dispatcher briefly re-attempts a parked backlog even though the endpoint
  is still down (one round of ≤ threshold failures, then the breaker
  re-opens). Bounded and harmless in test 3, but a restart storms a dead
  endpoint once.
- **Duplicates, not losses, on restart** — receivers must be idempotent
  (spec §8's at-least-once). The duplicate window is bounded by the
  autocommit interval (5 s by default).

Not exercised yet (§17 test plan): the soak test, the multi-day offline
scenario, SSE gateway restart with reconnecting clients (`Last-Event-ID`
backlog fetch), and PG/Kafka/CH outages beyond the receiver-level faults
above.
