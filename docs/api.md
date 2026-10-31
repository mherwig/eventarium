---
id: api
title: HTTP API walkthrough
sidebar_position: 3
---

One event, end to end: register an event type, create subscriptions, publish,
receive on both channels, query the outcome, replay what failed. Every call
needs a bearer token from `minttoken`:

```sh
export JWT_SECRET=dev-secret
T=$(./bin/minttoken -owner team-a -scopes publish,subscribe,status:read)
```

The control plane (`registry-api`) is on `:8083`, the data plane
(`publish-api`) on `:8080`, the status endpoints on `:8081`, SSE on `:8082`,
replay on `:8084`.

## Register an event type

`retention_class` is `live`, `short`, `day` or `week` and selects the Kafka
topic the events land on. `ordering_key_def` (optional) derives the ordering
key for `key_ordered` subscriptions: `subject`, `ext:<name>`, or
`json:<dotted.path>` in the data; empty falls back to the publisher's
partition key:

```sh
curl -s -X POST localhost:8083/v1/event-types -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders.created","retention_class":"short","content_type":"application/json"}'
```

## Create a webhook subscription

With a CEL attribute filter and a projection:

```sh
curl -s -X POST localhost:8083/v1/subscriptions -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"orders.created","channel":"webhook",
       "callback_url":"http://localhost:9090/hook",
       "filter":"attributes[\"region\"] == \"eu\"",
       "projection":["order_id","total"],
       "delivery_mode":"key_ordered","rate_limit_per_s":100}'
```

`filter` is CEL over `attributes` and `data`; `projection` is the allowlist of
data fields the subscriber receives. The callback may be any HTTP endpoint; the
simulated receiver of the harness listens on `:9090/hook`:

```sh
./bin/harness recv -addr :9090 -out recv.jsonl &
```

## Create an SSE subscription

The same request with `channel: "sse"` and no callback URL:

```sh
curl -s -X POST localhost:8083/v1/subscriptions -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"orders.created","channel":"sse"}'
```

## Update a subscription

`PUT` (`:8083`, same scope `subscribe`) — full replace with the same body as
`POST`, never a patch:

```sh
curl -s -X PUT localhost:8083/v1/subscriptions/<id> -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"orders.created","channel":"sse"}'
```

Everything the body carries is written; everything it omits goes back to the
defaults, so a `GET` → modify → `PUT` loop is lossless and `""` can only ever
mean "clear this filter". `id` and `owner` are the only immutable fields. An
unknown, malformed or another owner's ID answers `404` — checked *before* the
body, so an error never reveals whether an ID exists.

The change propagates through the same `NOTIFY` trigger as a create (in the
index within milliseconds), and the consequences depend on what changed:

- **Filter / data filter / projection** — applies to *newly published* events
  only. A verdict is frozen at routing time, so deliveries that already exist
  keep the configuration they were routed with.
- **Delivery policy** (`rate_limit_per_s`, `ack_timeout_ms`, breaker
  thresholds, …) — applies to what happens from now on, including rows the
  dispatcher drains out of the inbox.
- **Channel (`webhook` ⇄ `sse`)** — the whole record follows, wherever it
  already is:
  - *In-flight deliveries on the topic* follow the subscription's **current**
    channel, not the one stamped at routing time (the shared rule is
    `registry.ChannelFor`, so dispatcher and sse-gateway always agree on
    exactly one owner per record). Parked inbox rows drain with the
    configuration of the moment they are drained — never re-evaluated against
    the filter.
  - *Replay* also stamps the current channel: it re-sends existing deliveries,
    it does not re-run filters.
  - *Orphans*: a delivery whose subscription was deleted in between keeps its
    stamped channel, so an orphaned webhook delivery is still dropped as
    `unknown_subscription` instead of vanishing between the two stages.
  - Watch out for the switch itself: a webhook delivery that is parked while
    the subscription is an SSE one waits for an SSE connection (and vice
    versa); its expiry (`expires_at`, inbox TTL) is unchanged by the update.

## Publish an event

Data plane, `:8080`, binary CloudEvents binding — `ce-*` headers carry the
attributes, the body is the data member; use
`Content-Type: application/cloudevents+json` for structured mode, or
`harness pub` for load. `ce-region` becomes the extension attribute `region`
that the filter reads:

```sh
curl -s -X POST localhost:8080/v1/events -H "Authorization: Bearer $T" \
  -H 'Content-Type: application/json' \
  -H 'ce-specversion: 1.0' -H 'ce-id: 42' -H 'ce-source: /orders' \
  -H 'ce-type: orders.created' -H 'ce-region: eu' \
  -d '{"order_id":"o-1","total":99.9,"internal":"secret"}'
```

A `201` means *accepted into the event log*, not *will be delivered* — and an
event type nobody subscribes to would otherwise be accepted silently and
routed to nothing. So when the registry knows **zero** subscriptions for the
event's type, the response carries an extra advisory field (absent otherwise,
so the normal `201` is unchanged):

```json
{"event_ref":"94453d07-...","event_type":"track.empty","retention_class":"short",
 "expires_at":"2026-10-29T22:23:35Z",
 "warning":"no subscription is registered for event type \"track.empty\": the event is accepted and stored, but nothing will be delivered"}
```

It is a warning, never an error: a subscription can be created a second later,
and `422 unknown event type` already rejects the typo case. Note what it does
*not* report — the number of subscriptions. That count would be a **candidate**
count (publish does not evaluate CEL filters), so `"subscriptions": 5` could
still mean zero deliveries; it would read as a delivery forecast. The
per-event answer to "did the filter accept it?" lands in the status store a
moment later as [`filtered`](reliability.md#the-eleven-statuses).

The publish API caches the registry, and so do the router, dispatcher and SSE
gateway. Creating an event type or subscription fires a trigger (migration
003) on `event_types`/`subscriptions`, every service `LISTEN`s for it and
reloads as soon as the writing transaction commits — so once the
registry-api call has returned, the type or subscription is usable; no
settling time. `REGISTRY_POLL` (2 s for publish-api) is only the safety net
for notifications missed while a listener was down.

## Receive on the webhook

The dispatcher POSTs the event — binary binding by default, with delivery
metadata in `pubsub-*` headers (never inside the event) and an HMAC over the
body when `WEBHOOK_SIGNING_SECRET` is set:

```text
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

## Receive on SSE

Connect to `:8082` with a subscribe-scoped token:

```sh
curl -N -H "Authorization: Bearer $T" \
  "http://localhost:8082/sse?subscription_id=<id>&consumer_id=demo"
```

:::note
Eventarium accepts the token only in the `Authorization` header; browser
`EventSource` cannot send headers, so a real deployment would add a
short-lived token in the query string or a cookie — like the usual
`?access_token=` dance.
:::

Each delivery is one frame — `id` is the sequence, `data` the delivery
envelope with the full CloudEvent inside:

```text
id: 7:1904
event: delivery
data: {"delivery_id":"...","event_ref":"...","sequence":"7:1904",
       "event":{"specversion":"1.0","id":"42","source":"/orders",
                "type":"orders.created","datacontenttype":"application/json",
                "data":{"order_id":"o-1","total":99.9}}}
```

`sent` becomes `delivered` only when the client acknowledges the sequence —
either `POST /sse/ack` with `{"subscription_id":...,"sequence":"7:1904"}`, or
implicitly by receiving an event it then acks. On reconnect, echo the last
received `id` back as a `Last-Event-ID` header (or `?last_event_id=`): the
gateway then replays everything after that cursor straight from the
`deliveries` topic — covering parked rows *and* deliveries that died with the
old connection — merges it with the live stream, and deduplicates. The
`harness sse` subcommand does exactly that: it acks each event, reconnects
automatically with the cursor of the last event it received, deduplicates by
event reference, and logs what it saw:

```sh
./bin/harness sse -sub <id> -owner team-a -addr http://localhost:8082 \
  -out sse.jsonl -duration 60s
# resume a stored cursor explicitly on first connect:
./bin/harness sse -sub <id> -owner team-a -last-event-id 7:1904 ...
```

## Query the outcome

The delivery's status history and a per-status summary (`status:read` scope,
`:8081`):

```sh
T=$(./bin/minttoken -owner team-a -scopes status:read)

# one row per delivery with its current status (filter: ?status=&from=&to=,
# paging: ?limit=&cursor=)
curl -s localhost:8081/v1/subscriptions/<id>/deliveries -H "Authorization: Bearer $T"

# counts per status + total, over the last 24 h (narrow with ?since=RFC3339)
curl -s localhost:8081/v1/subscriptions/<id>/status-summary -H "Authorization: Bearer $T"
# → {"counts":{"delivered":40,...},"total":40}
```

The complete list of the eleven statuses, who writes them and which are
terminal is in [Delivery statuses](reliability.md#the-eleven-statuses).

## Replay deliveries

Resend what a subscription already matched, selected by current status, by
time frame, or both (`replay` scope, `:8084`):

```sh
T=$(./bin/minttoken -owner team-a -scopes replay)

# e.g. everything that dead-lettered, or everything delivered in a window
curl -s -X POST localhost:8084/v1/subscriptions/<id>/replays \
  -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: fix-2026-10-25-1' \
  -d '{"statuses":["dead_lettered"]}'

# → 202 {"replay_id":"55d3ce5d-...","status":"pending","dry_run":false}
curl -s localhost:8084/v1/replays/55d3ce5d-... -H "Authorization: Bearer $T"
# → matched/enqueued/not_replayable + live delivered/failed; "status":"done" when finished
curl -s -X POST localhost:8084/v1/replays/55d3ce5d-.../cancel -H "Authorization: Bearer $T"
curl -s localhost:8084/v1/subscriptions/<id>/replays -H "Authorization: Bearer $T"
```

The selector takes `statuses` (current status: `delivered`, `dead_lettered`,
`dropped`, `failed`, … — `filtered` is rejected with a 400, because a
filtered event never became a delivery), `from`/`to` (RFC3339, by ingestion
time) and `event_refs` — at least one. Time-range and `event_refs` selectors
never pick up `filtered` rows either; `replay.Resolve` excludes them
unconditionally, since a replay re-applies the projection but not the filter.
`"dry_run": true` reports `matched` and `not_replayable` without enqueuing
anything.

Replayed events keep their event reference, are flagged as a replay on the
wire (`pubsub-replay-id` header, `"replay":true` in the SSE payload), travel
the normal delivery path (rate limits, breaker, delivery mode) and must be
handled idempotently by the consumer. Only one job per subscription runs at a
time (a second POST answers `409`).
