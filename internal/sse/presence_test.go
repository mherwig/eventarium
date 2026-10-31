package sse

import (
	"testing"
	"time"

	"pubsub/internal/store"
)

func row(subID, connID, node string) store.PresenceRow {
	return store.PresenceRow{
		SubscriptionID: subID,
		ConnectionID:   connID,
		NodeID:         node,
		ConsumerID:     "c-" + connID,
		ConnectedAt:    time.Now(),
	}
}

// beatRow is row() with a lease heartbeat, which is the only thing that
// distinguishes two otherwise identical candidates for one subscription.
func beatRow(subID, connID, node string, lastSeen time.Time) store.PresenceRow {
	r := row(subID, connID, node)
	r.NodeLastSeen = lastSeen
	return r
}

// TestViewPickPrefersTheNodeThatMostRecentlyHeartbeated is the fix for the
// gwfail loss. When a gateway is killed its presence rows stay routable for
// the whole lease, so the view offers both the dead connection and the
// client's new one on a survivor. The old tie-break was the first entry of a
// map range - arbitrary, and measurably biased: the run lost six deliveries
// because the dead node won 7 times out of 8.
//
// A killed node stops beating, so preferring the freshest lease picks the
// survivor. Two live nodes keep alternating, so neither starves.
func TestViewPickPrefersTheNodeThatMostRecentlyHeartbeated(t *testing.T) {
	now := time.Now()
	v := newPresenceView()
	v.replace([]store.PresenceRow{
		// node-b was killed: its lease stopped advancing with the kill.
		beatRow("sub1", "dead", "node-b", now.Add(-4*time.Second)),
		// the client failed over here and node-a is still beating.
		beatRow("sub1", "live", "node-a", now.Add(-300*time.Millisecond)),
	}, nil)

	rec, ok := v.pick("sub1", "node-c") // no local connection: the tie-break runs
	if !ok {
		t.Fatal("pick found no connection")
	}
	if rec.ConnectionID != "live" {
		t.Errorf("pick chose %s on %s, want the connection on the node that is still heartbeating",
			rec.ConnectionID, rec.NodeID)
	}
}

// TestViewPickIgnoresTheOrderOfTheMap: the rule has to be a decision, not a
// lucky hash layout. Both insertion orders must give the same answer, which is
// what a lease comparison gives and a bare map range never did.
func TestViewPickIgnoresTheOrderOfTheMap(t *testing.T) {
	now := time.Now()
	dead := beatRow("sub1", "dead", "node-b", now.Add(-4*time.Second))
	live := beatRow("sub1", "live", "node-a", now)

	for _, tc := range []struct {
		name string
		rows []store.PresenceRow
	}{
		{"dead first", []store.PresenceRow{dead, live}},
		{"live first", []store.PresenceRow{live, dead}},
	} {
		v := newPresenceView()
		v.replace(tc.rows, nil)
		rec, ok := v.pick("sub1", "node-c")
		if !ok {
			t.Fatalf("%s: pick found no connection", tc.name)
		}
		if rec.ConnectionID != "live" {
			t.Errorf("%s: pick chose %s, want live regardless of insertion order", tc.name, rec.ConnectionID)
		}
	}
}

// TestViewPickStillRoutesWhenNoLeaseIsKnown: a candidate with no heartbeat is
// not a reason to refuse a delivery - it is only a reason to prefer a
// competitor that has one. Callers that build records without a lease keep the
// previous behaviour instead of losing their only route.
func TestViewPickStillRoutesWhenNoLeaseIsKnown(t *testing.T) {
	v := newPresenceView()
	v.replace([]store.PresenceRow{row("sub1", "only", "node-b")}, nil)

	rec, ok := v.pick("sub1", "node-a")
	if !ok {
		t.Fatal("pick refused a connection because its lease was unknown")
	}
	if rec.ConnectionID != "only" {
		t.Errorf("pick returned %s, want only", rec.ConnectionID)
	}
}

// TestViewPickPrefersTheLocalConnection is the routing rule: a delivery whose
// connection sits on this node is served here and skips the sse-live hop
// entirely; only a remote connection costs the extra produce.
func TestViewPickPrefersTheLocalConnection(t *testing.T) {
	v := newPresenceView()
	v.replace([]store.PresenceRow{
		row("sub1", "remote", "node-b"),
		row("sub1", "here", "node-a"),
	}, nil)

	rec, ok := v.pick("sub1", "node-a")
	if !ok {
		t.Fatal("pick found no connection")
	}
	if rec.NodeID != "node-a" || rec.ConnectionID != "here" {
		t.Errorf("pick returned %s/%s, want the local node-a/here", rec.NodeID, rec.ConnectionID)
	}
}

// TestViewPickFallsBackToARemoteConnection: the delivery stage usually runs
// somewhere other than the node holding the connection, and refusing to route
// there would park deliveries that could have gone out.
func TestViewPickFallsBackToARemoteConnection(t *testing.T) {
	v := newPresenceView()
	v.replace([]store.PresenceRow{
		row("sub1", "remote", "node-b"),
		row("sub1", "also-remote", "node-c"),
	}, nil)

	rec, ok := v.pick("sub1", "node-a") // this node holds nothing
	if !ok {
		t.Fatal("pick found no connection although two exist on other nodes")
	}
	if rec.NodeID == "node-a" {
		t.Errorf("pick returned a local connection that does not exist: %+v", rec)
	}
}

// TestViewPickOfAnUnknownSubscription: no connection means park, not guess.
func TestViewPickOfAnUnknownSubscription(t *testing.T) {
	v := newPresenceView()
	v.replace([]store.PresenceRow{row("sub1", "c1", "node-a")}, nil)
	if rec, ok := v.pick("sub-other", "node-a"); ok {
		t.Errorf("pick returned %+v for a subscription with no connections", rec)
	}
}

// TestReplaceOverlaysLocalConnections closes the race a snapshot read cannot:
// the query returns rows as of its own start, so a connection opened after
// that - on this node - is missing from the snapshot. Without the overlay this
// node's own delivery stage would park deliveries for a connection it is
// holding in memory.
func TestReplaceOverlaysLocalConnections(t *testing.T) {
	v := newPresenceView()
	snapshot := []store.PresenceRow{row("subA", "snapshot", "node-b")}

	// The snapshot was read before this connect committed.
	local := []presenceRecord{
		{NodeID: "node-a", ConnectionID: "fresh", SubscriptionID: "subA", ConsumerID: "c1"},
	}
	v.replace(snapshot, local)

	if _, ok := v.pick("subA", "node-a"); !ok {
		t.Error("a connection opened after the snapshot was read is missing from the view")
	}
	// And the snapshot's own rows must survive the overlay.
	if _, ok := v.pick("subA", "node-b"); !ok {
		t.Error("the overlay replaced the snapshot instead of adding to it")
	}
}

// TestReplaceDropsWhatTheSnapshotNoLongerHas: a row deleted while this node
// was not looking must leave the view, or pick() keeps routing to a connection
// that closed - the exact failure the topic-based design could not fix.
func TestReplaceDropsWhatTheSnapshotNoLongerHas(t *testing.T) {
	v := newPresenceView()
	v.replace([]store.PresenceRow{row("subA", "gone", "node-b")}, nil)
	if _, ok := v.pick("subA", "node-b"); !ok {
		t.Fatal("setup: the connection should be visible")
	}

	v.replace(nil, nil)
	if _, ok := v.pick("subA", "node-b"); ok {
		t.Error("a connection absent from a fresh snapshot is still routable")
	}
}

// TestPutAndRemoveAreImmediate: the connect and disconnect paths must not wait
// for a notification to come back round - connect publishes presence before
// the first byte, and disconnect must stop routing before the handler returns.
func TestPutAndRemoveAreImmediate(t *testing.T) {
	v := newPresenceView()
	v.replace(nil, nil)

	v.put(presenceRecord{NodeID: "node-a", ConnectionID: "c1", SubscriptionID: "subA"})
	if _, ok := v.pick("subA", "node-a"); !ok {
		t.Fatal("put did not make the connection routable")
	}
	v.remove("subA", "c1")
	if _, ok := v.pick("subA", "node-a"); ok {
		t.Error("remove left the connection routable")
	}
	if len(v.subscriptions()) != 0 {
		t.Errorf("subscriptions() = %v, want it empty once the last connection is gone", v.subscriptions())
	}
}

// TestSubscriptionsListsEverySubscriptionWithAConnection: the drainer iterates
// this to decide which parked rows have somewhere to go, so a subscription
// whose only connection is remote must appear too.
func TestSubscriptionsListsEverySubscriptionWithAConnection(t *testing.T) {
	v := newPresenceView()
	v.replace([]store.PresenceRow{
		row("subA", "c1", "node-a"),
		row("subB", "c2", "node-b"),
	}, nil)

	got := map[string]bool{}
	for _, s := range v.subscriptions() {
		got[s] = true
	}
	if !got["subA"] || !got["subB"] || len(got) != 2 {
		t.Errorf("subscriptions() = %v, want subA and subB", v.subscriptions())
	}
}
