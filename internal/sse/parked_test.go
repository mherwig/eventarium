package sse

import (
	"testing"

	"pubsub/internal/store"
)

// The gateway caches the inbox count behind backlog_max (spec 16) instead of
// counting the inbox per delivery batch. What has to hold: one counter per
// subscription, parks and acks move it, the reconciliation count wins, and an
// id nobody asked about is never turned into a counter.
func TestParkedCounterIsCreatedOnce(t *testing.T) {
	g := &Gateway{parked: map[string]*store.ParkCounter{}}
	a, b := g.parkedCounter("sub"), g.parkedCounter("sub")
	if a != b {
		t.Error("the same subscription must get the same counter")
	}
	if a.Ready() {
		t.Error("a fresh counter must not be ready")
	}
}

func TestSeedParkedUsesZeroForMissingIDs(t *testing.T) {
	g := &Gateway{parked: map[string]*store.ParkCounter{}}
	full := g.parkedCounter("full")
	full.Seed(4)
	full.Add(6) // six parks since the last count
	g.parkedCounter("empty")

	g.seedParked([]string{"full", "empty", "unknown"}, map[string]int64{"empty": 3})

	if full.Total() != 0 {
		t.Errorf("full = %d, want 0 (a missing key means the rows are gone)", full.Total())
	}
	if empty := g.parkedCounter("empty"); empty.Total() != 3 || !empty.Ready() {
		t.Errorf("empty = %d (ready=%v), want 3 and ready", empty.Total(), empty.Ready())
	}
	if _, ok := g.parked["unknown"]; ok {
		t.Error("an unknown id must not create a counter")
	}
	if len(g.parked) != 2 {
		t.Errorf("counters = %d, want 2", len(g.parked))
	}
}

func TestAddParkedAndAckParked(t *testing.T) {
	g := &Gateway{parked: map[string]*store.ParkCounter{}}
	c := g.parkedCounter("sub")
	c.Seed(10)

	rows := []store.InboxRow{
		{SubscriptionID: "sub"},
		{SubscriptionID: "sub"},
		{SubscriptionID: "late"},
	}
	g.addParked(rows) // only runs after a successful insert
	if c.Total() != 12 {
		t.Errorf("after two parks Total() = %d, want 12", c.Total())
	}
	if l := g.parkedCounter("late"); l.Ready() {
		t.Error("a counter credited by a park is not seeded; Ensure does that")
	}

	g.ackParked(rows)
	if c.Total() != 10 {
		t.Errorf("after the acks Total() = %d, want 10", c.Total())
	}
	if l := g.parkedCounter("late"); l.Total() != 0 {
		t.Errorf("late = %d, want 0", l.Total())
	}
}
