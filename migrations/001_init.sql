-- Control data: event type registry and subscriptions (sections 6.1 and 7).

CREATE TABLE IF NOT EXISTS event_types (
  owner            text        NOT NULL,
  name             text        NOT NULL,
  retention_class  text        NOT NULL DEFAULT 'short',
  content_type     text        NOT NULL DEFAULT 'application/json',
  ordering_key_def text,
  ordering_group   text,
  created_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (owner, name)
);

CREATE TABLE IF NOT EXISTS subscriptions (
  id                   uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
  owner                text        NOT NULL,
  event_type           text        NOT NULL,
  channel              text        NOT NULL,
  callback_url         text,
  filter               text,
  data_filter          text,
  projection           jsonb,
  delivery_mode        text        NOT NULL DEFAULT 'ordered',
  max_connections      int         NOT NULL DEFAULT 3,
  max_in_flight        int         NOT NULL DEFAULT 1,
  rate_limit_per_s     int         NOT NULL DEFAULT 0,
  burst                int         NOT NULL DEFAULT 0,
  drain_rate_per_s     int         NOT NULL DEFAULT 200,
  ack_timeout_ms       int         NOT NULL DEFAULT 15000,
  ack_window           int         NOT NULL DEFAULT 100,
  cb_failure_threshold int         NOT NULL DEFAULT 5,
  probe_interval_s     int         NOT NULL DEFAULT 300,
  request_timeout_ms   int         NOT NULL DEFAULT 10000,
  backlog_max          int         NOT NULL DEFAULT 100000,
  webhook_format       text        NOT NULL DEFAULT 'binary',
  created_at           timestamptz NOT NULL DEFAULT now(),
  updated_at           timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT subscriptions_channel_check     CHECK (channel IN ('webhook', 'sse')),
  CONSTRAINT subscriptions_mode_check        CHECK (delivery_mode IN ('ordered', 'key_ordered', 'unordered')),
  CONSTRAINT subscriptions_webhook_check     CHECK (channel <> 'webhook' OR callback_url IS NOT NULL),
  CONSTRAINT subscriptions_format_check      CHECK (webhook_format IN ('binary', 'structured'))
);

CREATE INDEX IF NOT EXISTS subscriptions_type_idx  ON subscriptions (event_type);
CREATE INDEX IF NOT EXISTS subscriptions_owner_idx ON subscriptions (owner);

-- Inbox: parked deliveries, partitioned by the day of expires_at so that an
-- entire partition can be dropped at expiry (section 6.6).
CREATE TABLE IF NOT EXISTS inbox (
  subscription_id uuid        NOT NULL,
  part            int4        NOT NULL,
  off             int8        NOT NULL,
  payload         jsonb       NOT NULL,
  reason          text        NOT NULL,
  parked_at       timestamptz NOT NULL DEFAULT now(),
  expires_at      timestamptz NOT NULL,
  -- The primary key must include the partitioning column (section 6.6).
  -- part and off already identify the delivery uniquely; expires_at is
  -- deterministic per event because it comes from the record header.
  PRIMARY KEY (subscription_id, part, off, expires_at)
) PARTITION BY RANGE (expires_at);

-- Catches rows whose expires_at is already in the past or beyond the managed
-- horizon; the partition maintenance job creates daily partitions.
CREATE TABLE IF NOT EXISTS inbox_default PARTITION OF inbox DEFAULT;
