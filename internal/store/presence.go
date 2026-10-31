package store

import (
	"context"
	"fmt"
	"time"
)

// PresenceRow is one open SSE connection as PostgreSQL knows it: which
// subscription it serves, which process holds it (node_id), and which client
// it is (consumer_id). The join with sse_presence_nodes - done by
// VisiblePresence, never by the caller - is what decides whether it counts,
// and it also hands over when that node last beat, so a caller choosing
// between two rows can tell a live connection from one whose node was killed
// but has not finished being reaped.
type PresenceRow struct {
	SubscriptionID string
	ConnectionID   string
	NodeID         string
	ConsumerID     string
	ConnectedAt    time.Time
	// NodeLastSeen is the owning node's lease heartbeat, carried by
	// VisiblePresence (which already joins the lease table to filter, and now
	// keeps the value instead of discarding it). It is what tells two otherwise
	// equal candidates apart: a node that stopped heartbeating keeps its rows
	// visible for the whole lease, so both a live and a dead connection can be
	// routable at once. UpsertPresence never writes it - the lease table has
	// its own heartbeat - so it is zero on rows built by a writer.
	NodeLastSeen time.Time
}

// UpsertPresence records an open connection. ON CONFLICT DO UPDATE, not
// DO NOTHING: the row is written when a connection opens, and re-writing it
// on a reconnect of the same ids must leave the freshest node_id rather than
// keep whichever process claimed it first.
func (s *Store) UpsertPresence(ctx context.Context, r PresenceRow) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO sse_presence (subscription_id, connection_id, node_id, consumer_id, connected_at)
		VALUES ($1, $2, $3, $4, COALESCE($5, now()))
		ON CONFLICT (subscription_id, connection_id) DO UPDATE
		SET node_id = EXCLUDED.node_id,
		    consumer_id = EXCLUDED.consumer_id,
		    connected_at = EXCLUDED.connected_at`,
		r.SubscriptionID, r.ConnectionID, r.NodeID, r.ConsumerID, r.ConnectedAt)
	if err != nil {
		return fmt.Errorf("store: recording presence: %w", err)
	}
	return nil
}

// DeletePresence forgets a connection. A disconnect that fails here leaves a
// row behind; the node lease is what makes it harmless (it stops being visible
// when this process stops heartbeating for the connection's node), and
// ReapPresence removes it for good.
func (s *Store) DeletePresence(ctx context.Context, subscriptionID, connectionID string) error {
	_, err := s.Pool.Exec(ctx,
		`DELETE FROM sse_presence WHERE subscription_id = $1 AND connection_id = $2`,
		subscriptionID, connectionID)
	if err != nil {
		return fmt.Errorf("store: removing presence: %w", err)
	}
	return nil
}

// VisiblePresence loads the connections a gateway may route to: every row
// whose node has heartbeated within leaseTTL.
//
// This is the whole correctness argument of the design in one query. The
// clock is evaluated here rather than stored, so a node that dies stops being
// routable without anything having to notice - the next reader to run the
// query already sees it as gone. A row whose node has no lease row at all is
// invisible too: an unknown node is treated as a dead one, which parks the
// delivery instead of aiming it at nobody.
func (s *Store) VisiblePresence(ctx context.Context, leaseTTL time.Duration) ([]PresenceRow, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT p.subscription_id::text, p.connection_id::text, p.node_id,
		       p.consumer_id, p.connected_at, n.last_seen
		FROM sse_presence p
		JOIN sse_presence_nodes n ON n.node_id = p.node_id
		WHERE n.last_seen > now() - make_interval(secs => $1)`,
		leaseTTL.Seconds())
	if err != nil {
		return nil, fmt.Errorf("store: loading presence: %w", err)
	}
	defer rows.Close()
	out := []PresenceRow{}
	for rows.Next() {
		var r PresenceRow
		if err := rows.Scan(&r.SubscriptionID, &r.ConnectionID, &r.NodeID,
			&r.ConsumerID, &r.ConnectedAt, &r.NodeLastSeen); err != nil {
			return nil, fmt.Errorf("store: scanning presence: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TouchNode refreshes this gateway's lease. It is called on a ticker, so it
// must be one cheap statement: a single row per process, whatever the process
// holds in connections.
//
// The gateway treats the first successful touch after a gap as a signal to
// re-assert its connection rows: a lease that expired (this process was
// frozen, or the store was unreachable for longer than the reap horizon) may
// already have been reaped, and rows for connections that are in fact still
// open would then be missing while the lease is fresh again.
func (s *Store) TouchNode(ctx context.Context, nodeID string) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO sse_presence_nodes (node_id, last_seen)
		VALUES ($1, now())
		ON CONFLICT (node_id) DO UPDATE SET last_seen = now()`, nodeID)
	if err != nil {
		return fmt.Errorf("store: refreshing presence lease: %w", err)
	}
	return nil
}

// ClearNodePresence removes every presence row claiming to belong to this
// node. It runs once at startup, before the gateway accepts a connection.
//
// This is the fast path for the problem the topic could not solve: a process
// restarted with the same NODE_ID leaves its previous life's rows behind, and
// they describe connections that closed when that process died. Deleting them
// up front means the new process starts from exactly what it actually holds -
// an empty set - instead of waiting for a lease it never wrote to expire.
//
// A node whose NODE_ID changes on every start (a fresh pod name) has no rows
// to clear here; those are the reaper's, once their old lease goes stale.
func (s *Store) ClearNodePresence(ctx context.Context, nodeID string) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM sse_presence WHERE node_id = $1`, nodeID)
	if err != nil {
		return fmt.Errorf("store: clearing presence of %s: %w", nodeID, err)
	}
	return nil
}

// ReapPresence deletes the rows nobody has been keeping alive: presence rows
// whose node lease is older than staleAfter, and the leases themselves. It
// returns what it removed.
//
// Pure hygiene - VisiblePresence already excludes these rows, so correctness
// never waits on the reaper. What it prevents is the slow accumulation of
// rows from gateways that came and went, each one a row a query has to skip.
func (s *Store) ReapPresence(ctx context.Context, staleAfter time.Duration) (presence, nodes int64, err error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("store: beginning presence reap: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Exec and RowsAffected, not QueryRow and Scan: a DELETE without
	// RETURNING produces no result set, so Scan would report "no rows" for a
	// statement that succeeded - the reaper failing on every call it makes.
	res, err := tx.Exec(ctx, `
		DELETE FROM sse_presence p
		WHERE NOT EXISTS (
			SELECT 1 FROM sse_presence_nodes n
			WHERE n.node_id = p.node_id
			  AND n.last_seen > now() - make_interval(secs => $1)
		)`, staleAfter.Seconds())
	if err != nil {
		return 0, 0, fmt.Errorf("store: reaping presence rows: %w", err)
	}
	presence = res.RowsAffected()
	res, err = tx.Exec(ctx, `
		DELETE FROM sse_presence_nodes
		WHERE last_seen <= now() - make_interval(secs => $1)`,
		staleAfter.Seconds())
	if err != nil {
		return 0, 0, fmt.Errorf("store: reaping presence leases: %w", err)
	}
	nodes = res.RowsAffected()
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, fmt.Errorf("store: committing presence reap: %w", err)
	}
	return presence, nodes, nil
}

// CountPresence is a diagnostic for the chaos assertions: how many rows
// currently claim a given node, without the visibility join.
func (s *Store) CountPresence(ctx context.Context, nodeID string) (int64, error) {
	var n int64
	err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM sse_presence WHERE node_id = $1`, nodeID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("store: counting presence: %w", err)
	}
	return n, nil
}
