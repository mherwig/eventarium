-- SSE connection presence in PostgreSQL instead of the compacted sse-presence
-- topic (docs/deviations-and-open-points.md, deviation 6).
--
-- The gateway needs one question answered: which connection, on which node,
-- currently owns a subscription - and every node needs the answer for *all*
-- nodes, because the delivery stage runs wherever the deliveries partition is
-- assigned rather than where the connection lives. A compacted topic answered
-- it by folding a log, which has one property this design cannot live without:
-- a record is only ever replaced by a newer record *for the same key*, and
-- a key is `<subscription>/<connection>`. A node that dies without publishing
-- tombstones therefore leaves its last record behind permanently - a
-- reconnect mints a new key, so nothing overwrites it - and every later
-- pick() could route a delivery at a connection nobody owns.
--
-- PostgreSQL answers the same question with two tables and a clock:
--
--   sse_presence        one row per open connection; written on connect,
--                       deleted on disconnect. Only connect and disconnect
--                       change it, so only those notify (below).
--   sse_presence_nodes  one row per gateway, refreshed every couple of
--                       seconds. A presence row is *visible* only while its
--                       node's lease is fresh, so the worst a hard-killed
--                       node costs is the lease TTL of misrouted deliveries -
--                       not an entry that survives the cluster.
--
-- The lease table is separate from the presence rows on purpose: refreshing
-- one row per node is O(1) no matter how many connections it holds, whereas
-- touching every presence row on every heartbeat rewrites the table
-- continuously for no new information.
--
-- Freshness is evaluated by the reader (the visibility query joins on
-- last_seen), never stored: a reader that forgot to filter would be the bug
-- again. The reaper below is storage hygiene, not correctness.

CREATE TABLE IF NOT EXISTS sse_presence (
    subscription_id uuid        NOT NULL,
    connection_id   uuid        NOT NULL,
    node_id         text        NOT NULL,
    consumer_id     text        NOT NULL,
    connected_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (subscription_id, connection_id)
);

CREATE INDEX IF NOT EXISTS sse_presence_node_idx ON sse_presence (node_id);

CREATE TABLE IF NOT EXISTS sse_presence_nodes (
    node_id   text        PRIMARY KEY,
    last_seen timestamptz NOT NULL DEFAULT now()
);

-- Presence changes notify exactly like the registry does (migration 003),
-- for the same reason: the poll bounds staleness by an interval, this bounds
-- it by the notification round trip - milliseconds. A subscription that was
-- just created and connected must be routable before its first delivery, not
-- after the next poll.
--
-- FOR EACH STATEMENT rather than FOR EACH ROW: connect and disconnect are
-- low-rate, but a gateway restart re-registers every connection it holds and
-- one notification per statement keeps the channel quiet. The payload names
-- the table and the readers reload the whole view anyway.
--
-- No trigger on sse_presence_nodes: heartbeats are deliberate noise - one per
-- node every couple of seconds, forever. They change no *routing* answer
-- (visibility is computed from the clock at read time), so notifying on them
-- would cost a reload on every node, every tick, to deliver no new
-- information. The poll covers them, and a node that discovers its own lease
-- was reaped re-asserts its rows, which does notify.

CREATE OR REPLACE FUNCTION notify_sse_presence_changed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('sse_presence_changed', TG_TABLE_NAME);
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS sse_presence_notify ON sse_presence;
CREATE TRIGGER sse_presence_notify
  AFTER INSERT OR DELETE ON sse_presence
  FOR EACH STATEMENT EXECUTE FUNCTION notify_sse_presence_changed();
