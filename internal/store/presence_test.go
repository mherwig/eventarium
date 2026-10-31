package store_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"pubsub/internal/store"
	"pubsub/migrations"
)

// defaultTestDSN is the local compose database, skipped when nothing answers
// (the convention of internal/registry and internal/store's other PG tests).
const defaultTestDSN = "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"

// PresenceStore opens the database and applies every migration, or skips.
func presenceStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Skipf("no PostgreSQL at %q (set TEST_PG_DSN): %v", dsn, err)
	}
	t.Cleanup(st.Close)
	if err := migrations.Apply(ctx, st.Pool); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}
	return st
}

// TestVisiblePresenceRequiresAFreshLease is the whole correctness claim of the
// design in one test. A presence row is only routable while the node that owns
// it has been heartbeating recently - so a gateway that dies without deleting
// its rows (no tombstone possible: the process is gone) stops receiving
// deliveries within the lease TTL instead of holding them forever.
//
// It also pins the direction of the failure: an expired lease makes the row
// *invisible*, it does not delete it. Visibility is what correctness depends
// on and it is evaluated by the reader on every query; deletion is the
// reaper's separate, slower job.
func TestVisiblePresenceRequiresAFreshLease(t *testing.T) {
	st := presenceStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const node = "test-vis-node"
	cleanup(t, ctx, st, node)

	if err := st.TouchNode(ctx, node); err != nil {
		t.Fatalf("TouchNode: %v", err)
	}
	if err := st.UpsertPresence(ctx, store.PresenceRow{
		SubscriptionID: "00000000-0000-4000-8000-000000000001",
		ConnectionID:   "00000000-0000-4000-8000-000000000002",
		NodeID:         node,
		ConsumerID:     "consumer-1",
	}); err != nil {
		t.Fatalf("UpsertPresence: %v", err)
	}
	t.Cleanup(func() { cleanup(t, ctx, st, node) })

	if got := countVisible(t, ctx, st, node); got != 1 {
		t.Fatalf("visible rows for a freshly heartbeating node = %d, want 1", got)
	}

	// Expire the lease the way time does: backdate it, rather than sleeping
	// past the TTL. The lease is the only thing between the row and a
	// delivery aimed at a connection nobody owns.
	if _, err := st.Pool.Exec(ctx,
		`UPDATE sse_presence_nodes SET last_seen = now() - interval '1 hour' WHERE node_id = $1`,
		node); err != nil {
		t.Fatalf("backdating the lease: %v", err)
	}
	if got := countVisible(t, ctx, st, node); got != 0 {
		t.Errorf("visible rows after the lease expired = %d, want 0 - a dead node would still be routed to", got)
	}
	// ...but the row itself must still be there: only the reaper deletes, and
	// a node that was merely unreachable gets its rows back by heartbeating.
	n, err := st.CountPresence(ctx, node)
	if err != nil {
		t.Fatalf("CountPresence: %v", err)
	}
	if n != 1 {
		t.Errorf("presence rows after the lease expired = %d, want 1 (expiry hides, it must not delete)", n)
	}
}

// TestVisiblePresenceCarriesTheLeaseHeartbeat: pick() breaks its tie between
// two routable connections on the node that heartbeated most recently, so the
// heartbeat has to arrive with the row rather than only being used to filter.
//
// The distinction it exists for is the one gwfail ran into: a gateway that is
// `kill -9`ed keeps its rows routable for the rest of the lease, so a view can
// legitimately hold both the dead connection and the client's new one on a
// survivor. Filtering alone cannot separate them - both pass - and the old
// tie-break (the first entry of a map range) chose the dead one 7 times in 8
// and lost six deliveries for it.
//
// Both rows here stay *visible* on purpose: the lease is what decides
// visibility and it has not expired for either. What is asserted is that the
// reader can tell them apart, which is strictly more than the filter needed.
func TestVisiblePresenceCarriesTheLeaseHeartbeat(t *testing.T) {
	st := presenceStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const (
		freshNode = "test-heartbeat-fresh"
		staleNode = "test-heartbeat-stale"
	)
	cleanup(t, ctx, st, freshNode)
	cleanup(t, ctx, st, staleNode)
	t.Cleanup(func() {
		cleanup(t, ctx, st, freshNode)
		cleanup(t, ctx, st, staleNode)
	})

	for i, node := range []string{freshNode, staleNode} {
		if err := st.TouchNode(ctx, node); err != nil {
			t.Fatalf("TouchNode(%s): %v", node, err)
		}
		if err := st.UpsertPresence(ctx, store.PresenceRow{
			SubscriptionID: fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", 21+i),
			ConnectionID:   fmt.Sprintf("00000000-0000-4000-8000-0000000000%02d", 31+i),
			NodeID:         node,
			ConsumerID:     fmt.Sprintf("consumer-hb-%d", i),
		}); err != nil {
			t.Fatalf("UpsertPresence(%s): %v", node, err)
		}
	}
	// Age one lease, but only by less than the TTL: it must stay visible, and
	// it must be recognisably older than the other one. Backdating rather than
	// sleeping keeps the test instant and deterministic.
	if _, err := st.Pool.Exec(ctx,
		`UPDATE sse_presence_nodes SET last_seen = now() - interval '3 seconds' WHERE node_id = $1`,
		staleNode); err != nil {
		t.Fatalf("backdating the lease: %v", err)
	}

	rows, err := st.VisiblePresence(ctx, 6*time.Second)
	if err != nil {
		t.Fatalf("VisiblePresence: %v", err)
	}
	seen := map[string]time.Time{}
	for _, r := range rows {
		if r.NodeID == freshNode || r.NodeID == staleNode {
			seen[r.NodeID] = r.NodeLastSeen
		}
	}
	if len(seen) != 2 {
		t.Fatalf("VisiblePresence returned %d of the 2 seeded rows, want 2 (both inside the lease)", len(seen))
	}
	for node, ts := range seen {
		if ts.IsZero() {
			t.Errorf("%s: NodeLastSeen is zero - pick() would have nothing to break its tie on", node)
		}
	}
	if !seen[freshNode].After(seen[staleNode]) {
		t.Errorf("fresh node's last_seen %v is not after the aged one's %v - the reader cannot tell a live node from a stale one",
			seen[freshNode], seen[staleNode])
	}
}

// TestLeaseRefreshRestoresVisibility is the recovery half: a node that was
// unreachable but did not die resumes being routable by touching its lease
// again, with no row rewritten. This is what makes the reap horizon (60 s)
// safely longer than the visibility TTL (6 s).
func TestLeaseRefreshRestoresVisibility(t *testing.T) {
	st := presenceStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const node = "test-lease-node"
	cleanup(t, ctx, st, node)
	t.Cleanup(func() { cleanup(t, ctx, st, node) })

	if err := st.TouchNode(ctx, node); err != nil {
		t.Fatalf("TouchNode: %v", err)
	}
	if err := st.UpsertPresence(ctx, store.PresenceRow{
		SubscriptionID: "00000000-0000-4000-8000-000000000011",
		ConnectionID:   "00000000-0000-4000-8000-000000000012",
		NodeID:         node,
		ConsumerID:     "consumer-2",
	}); err != nil {
		t.Fatalf("UpsertPresence: %v", err)
	}
	if _, err := st.Pool.Exec(ctx,
		`UPDATE sse_presence_nodes SET last_seen = now() - interval '1 hour' WHERE node_id = $1`,
		node); err != nil {
		t.Fatalf("backdating the lease: %v", err)
	}
	if got := countVisible(t, ctx, st, node); got != 0 {
		t.Fatalf("visible rows before the refresh = %d, want 0", got)
	}

	if err := st.TouchNode(ctx, node); err != nil {
		t.Fatalf("TouchNode after the gap: %v", err)
	}
	if got := countVisible(t, ctx, st, node); got != 1 {
		t.Errorf("visible rows after the lease was refreshed = %d, want 1: a node that recovered should not need its rows rewritten", got)
	}
}

// TestReapPresenceTakesOnlyWhatTheLeaseGaveUpOn: the reaper is storage
// hygiene, so it must not race the visibility rule - a row whose lease is
// merely stale (the shape of a *paused* node, not a dead one) is outside the
// reap horizon and has to survive.
func TestReapPresenceTakesOnlyWhatTheLeaseGaveUpOn(t *testing.T) {
	st := presenceStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const stale, fresh = "test-reap-stale", "test-reap-fresh"
	for _, n := range []string{stale, fresh} {
		cleanup(t, ctx, st, n)
	}
	t.Cleanup(func() { cleanup(t, ctx, st, stale); cleanup(t, ctx, st, fresh) })

	const (
		staleSub = "00000000-0000-4000-8000-000000000021"
		staleCon = "00000000-0000-4000-8000-000000000022"
		freshSub = "00000000-0000-4000-8000-000000000023"
		freshCon = "00000000-0000-4000-8000-000000000024"
	)
	// The stale node's lease is *inside* the reap horizon: an hour old would
	// be reaped and the test would prove nothing about the horizon.
	for _, n := range []string{stale, fresh} {
		if err := st.TouchNode(ctx, n); err != nil {
			t.Fatalf("TouchNode(%s): %v", n, err)
		}
	}
	if err := st.UpsertPresence(ctx, store.PresenceRow{
		SubscriptionID: staleSub, ConnectionID: staleCon, NodeID: stale, ConsumerID: "c3",
	}); err != nil {
		t.Fatalf("UpsertPresence(stale): %v", err)
	}
	if err := st.UpsertPresence(ctx, store.PresenceRow{
		SubscriptionID: freshSub, ConnectionID: freshCon, NodeID: fresh, ConsumerID: "c4",
	}); err != nil {
		t.Fatalf("UpsertPresence(fresh): %v", err)
	}
	if _, err := st.Pool.Exec(ctx,
		`UPDATE sse_presence_nodes SET last_seen = now() - interval '2 hours' WHERE node_id = $1`,
		stale); err != nil {
		t.Fatalf("backdating the stale lease: %v", err)
	}

	removed, nodes, err := st.ReapPresence(ctx, time.Hour)
	if err != nil {
		t.Fatalf("ReapPresence: %v", err)
	}
	if removed < 1 || nodes < 1 {
		t.Errorf("ReapPresence removed %d rows / %d nodes, want at least one of each", removed, nodes)
	}
	if got := mustCount(t, ctx, st, stale); got != 0 {
		t.Errorf("rows of the two-hour-dead node = %d, want 0", got)
	}
	if got := mustCount(t, ctx, st, fresh); got != 1 {
		t.Errorf("rows of the live node = %d, want 1: the reaper must not touch a fresh lease", got)
	}
}

// TestClearNodePresenceDropsOnlyThisNodesRows is the startup path: a process
// restarted under the same NODE_ID must start from the connections it
// actually holds, which at that moment is none - but other nodes' rows are
// untouched, or one restart would blank the whole cluster's view.
func TestClearNodePresenceDropsOnlyThisNodesRows(t *testing.T) {
	st := presenceStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const mine, theirs = "test-clear-mine", "test-clear-theirs"
	cleanup(t, ctx, st, mine)
	cleanup(t, ctx, st, theirs)
	t.Cleanup(func() { cleanup(t, ctx, st, mine); cleanup(t, ctx, st, theirs) })

	for i, n := range []string{mine, theirs} {
		if err := st.TouchNode(ctx, n); err != nil {
			t.Fatalf("TouchNode(%s): %v", n, err)
		}
		if err := st.UpsertPresence(ctx, store.PresenceRow{
			SubscriptionID: "00000000-0000-4000-8000-00000000003" + string(rune('1'+i)),
			ConnectionID:   "00000000-0000-4000-8000-00000000004" + string(rune('1'+i)),
			NodeID:         n,
			ConsumerID:     "c5",
		}); err != nil {
			t.Fatalf("UpsertPresence(%s): %v", n, err)
		}
	}

	if err := st.ClearNodePresence(ctx, mine); err != nil {
		t.Fatalf("ClearNodePresence: %v", err)
	}
	if got := mustCount(t, ctx, st, mine); got != 0 {
		t.Errorf("this node's rows after the clear = %d, want 0", got)
	}
	if got := mustCount(t, ctx, st, theirs); got != 1 {
		t.Errorf("another node's rows after our clear = %d, want 1: a restart must not blank the cluster", got)
	}
}

// TestUpsertPresenceReplacesTheNode: the row is keyed by connection, so a
// reconnect carrying the same ids must move it to the process that holds it
// now - otherwise a stale node_id keeps routing deliveries to a node that no
// longer has the connection.
func TestUpsertPresenceReplacesTheNode(t *testing.T) {
	st := presenceStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const oldNode, newNode = "test-up-old", "test-up-new"
	cleanup(t, ctx, st, oldNode)
	cleanup(t, ctx, st, newNode)
	t.Cleanup(func() { cleanup(t, ctx, st, oldNode); cleanup(t, ctx, st, newNode) })

	for _, n := range []string{oldNode, newNode} {
		if err := st.TouchNode(ctx, n); err != nil {
			t.Fatalf("TouchNode(%s): %v", n, err)
		}
	}
	rec := store.PresenceRow{
		SubscriptionID: "00000000-0000-4000-8000-000000000051",
		ConnectionID:   "00000000-0000-4000-8000-000000000052",
		NodeID:         oldNode,
		ConsumerID:     "c6",
	}
	if err := st.UpsertPresence(ctx, rec); err != nil {
		t.Fatalf("UpsertPresence: %v", err)
	}
	rec.NodeID = newNode
	if err := st.UpsertPresence(ctx, rec); err != nil {
		t.Fatalf("UpsertPresence (second): %v", err)
	}
	if got := mustCount(t, ctx, st, oldNode); got != 0 {
		t.Errorf("rows still on the old node = %d, want 0", got)
	}
	if got := mustCount(t, ctx, st, newNode); got != 1 {
		t.Errorf("rows on the new node = %d, want 1", got)
	}
}

// countVisible answers the question the gateway asks: how many rows of this
// node may currently be routed to.
func countVisible(t *testing.T, ctx context.Context, st *store.Store, node string) int64 {
	t.Helper()
	rows, err := st.VisiblePresence(ctx, 6*time.Second)
	if err != nil {
		t.Fatalf("VisiblePresence: %v", err)
	}
	var n int64
	for _, r := range rows {
		if r.NodeID == node {
			n++
		}
	}
	return n
}

func mustCount(t *testing.T, ctx context.Context, st *store.Store, node string) int64 {
	t.Helper()
	n, err := st.CountPresence(ctx, node)
	if err != nil {
		t.Fatalf("CountPresence(%s): %v", node, err)
	}
	return n
}

// cleanup removes a test node's rows, so a re-run starts from nothing even
// when the previous run died halfway.
func cleanup(t *testing.T, ctx context.Context, st *store.Store, node string) {
	t.Helper()
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := st.Pool.Exec(c, `DELETE FROM sse_presence WHERE node_id = $1`, node); err != nil {
		t.Logf("cleaning presence rows of %s: %v", node, err)
	}
	if _, err := st.Pool.Exec(c, `DELETE FROM sse_presence_nodes WHERE node_id = $1`, node); err != nil {
		t.Logf("cleaning presence lease of %s: %v", node, err)
	}
}
