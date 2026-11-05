---
id: monitoring
title: Monitoring
sidebar_position: 6
---

## Metrics

Every service serves OpenTelemetry metrics in Prometheus exposition format on
`GET /metrics` (`METRICS_ENABLED`, default on), so Prometheus, Grafana Agent
or Alloy can scrape all eight and Grafana can graph them. The instruments are
OpenTelemetry's and the exposition is Prometheus's, which means an OTLP
exporter can be added later behind the same instruments without touching a
call site.

### Seeing them

```sh
docker compose up -d prometheus grafana
```

Prometheus scrapes all eight services on **:9091** (it takes 9091 because
9090 is the harness's webhook receiver) with 30-day retention, so
`increase(...[1d])` and `increase(...[7d])` have samples to work with — the
two-hour default would answer "today" and nothing longer. Grafana is on
**:3000**, admin/admin, with the Prometheus data source provisioned at
startup rather than added by hand, so a fresh `docker compose up` has
somewhere to put the numbers.

Both are the only two containers in the file that are not part of the
platform: the eight services run on the host, which is why Prometheus
resolves `host.docker.internal` rather than scraping sibling containers.

### Keeping the series count predictable

Labels are chosen so cardinality stays bounded, because that is the decision
that is expensive to reverse:

- **Functionally dependent labels cost nothing.** Every subscription has
  exactly one owner and exactly one channel, so adding `owner` and `channel`
  to a per-subscription metric creates no extra series —
  `delivery.outcome{owner, subscription_id, channel}` has N combinations, not
  N × owners × channels. Only *independent* dimensions multiply.
- **Histograms carry no `outcome` label.** A label on a histogram multiplies
  the bucket count; the outcome breakdown is the counter's job.
- **Nothing traffic-driven is ever a label.** Values come from the registry,
  which already rejects unknown event types at publish, so `event_type` and
  `subscription_id` are enumerable rather than attacker-controlled.

At 1,000 subscriptions the delivery metrics are about N × (12 buckets + 6
outcomes) ≈ 18N series. If that ever needs to shrink, drop `subscription_id`
from the duration histogram first — the counters still say *who* is failing.

### What answers what

| Question | Metric |
| --- | --- |
| Is publishing failing, for which type? | `pubsub_publish_requests_total{event_type, outcome}` |
| How many events ingested, per second/day/week? | `pubsub_publish_events_total{event_type}` → `rate()` / `increase()` |
| Is publishing slow? | `pubsub_publish_duration_seconds{event_type}` (`batch` for a batch) |
| Payload size, summed | `pubsub_publish_bytes_total{event_type}` |
| Is delivery failing **for my subscription**? | `pubsub_delivery_outcome_total{owner, subscription_id, channel, outcome}` |
| Is delivery slow for it? | `pubsub_delivery_attempt_duration_seconds{owner, subscription_id, channel}` |
| **End-to-end latency**, p50…p99 | `pubsub_e2e_duration_seconds{channel}` — publish-api's ingest stamp to the consumer receiving it |
| How many delivered (last mile)? | `pubsub_delivery_outcome_total{outcome="delivered"}`, `pubsub_sse_sent_total` |
| Outgoing payload size | `pubsub_delivery_bytes_total{channel}` |
| Router processed per second? | `pubsub_router_records_total{direction}` → `rate()` |
| Is the router slow, and where? | `pubsub_router_stage_duration_seconds{stage}` — `kafka_wait`, `process`, `produce_sync`, `produce_end`, `poll`, `cycle` |
| Is there a backlog? | `pubsub_dispatch_queue_depth{class}` (split by rate limit), `pubsub_dispatch_offsets_in_flight`, `pubsub_dispatch_parked{subscription_id}` |
| Is a handover stalling? | `pubsub_rebalance_drain_duration_seconds`, `pubsub_rebalance_drain_abandoned_total` |
| Open SSE connections | `pubsub_sse_connections` (this node — sum across instances), `pubsub_sse_presence` (every node) |
| Is the registry going stale? | `pubsub_registry_reloads_total{result}`, `pubsub_registry_reload_duration_seconds` |
| Are delivery outcomes reaching the status store? | `pubsub_status_rows_written_total`, `pubsub_status_insert_failures_total`, `pubsub_status_write_duration_seconds` |
| Is ClickHouse keeping up with status writes? | `pubsub_status_buffer_depth` — a buffer that only grows is the earliest sign |
| Which replay jobs ran, and how did they end? | `pubsub_replay_jobs_total{status}`, `pubsub_replay_events_total{result}` |
| Which endpoint is slow? | `pubsub_http_server_duration_seconds{route, status_code}` |

Plus, on every service: `go_goroutine_count` (a leak shows as monotonic
growth), `go_memory_*`, `go_gc_*`, `go_processor_limit`,
`process_cpu_time_seconds_total{cpu}`, `process_memory_rss_bytes`, and
`target_info{service_name}`.

Two details worth knowing before building a panel:

- **`pubsub_e2e_duration_seconds` is server-side.** It starts at
  publish-api's ingest stamp, so it excludes the publisher's own network hop;
  the harness's `publish → receipt` measurement includes it and will read
  slightly higher. Both are correct, they answer different questions.
- **Counters only appear once they have recorded.** A service with no traffic
  shows its gauges but no series for its counters — that is "nothing happened
  yet", not "metrics are broken".
- **`route` is the registered pattern, never the URL.**
  `pubsub_http_server_*` labels with `GET /v1/subscriptions/{id}/deliveries`,
  not the path, so one subscription id cannot become one series. Health and
  metrics endpoints are excluded for the same reason: a probe and a scrape are
  not traffic.
- **`histogram_quantile` answers NaN if the window crosses a restart that
  changed the bucket boundaries.** The buckets live in code
  (`otelmetric.DurationBuckets`), so changing them gives Prometheus two
  incompatible layouts under the same `le` labels, and `sum by (le)` merges
  them into a non-monotonic series. Narrow the range until the window starts
  after the restart. It looks exactly like broken metrics and is not.

## Dashboard

`deploy/grafana/dashboards/eventarium.json` is provisioned at startup — 19
panels over ingest, delivery, end-to-end latency, backlog, the router, SSE and
the three business-metric services. It loads with the data source, needs no UI
step, and reloads itself 30 s after an edit, so the dashboard lives next to
the code that produces its numbers:

```sh
docker compose up -d prometheus grafana   # then open http://localhost:3000/d/eventarium-overview
```

The provider that mounts it is
`deploy/grafana/provisioning/dashboards/dashboards.yaml`. Adding a panel is
editing the JSON and waiting; no export, no import, nothing lost when the
`grafana-data` volume is replaced.
