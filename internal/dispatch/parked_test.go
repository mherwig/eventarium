package dispatch

import "testing"

// The counter replaces the count(*) the backlog cap used to run per parked
// delivery. What has to hold: it starts unknown (no cap until it knows), it
// follows local parks and acks, the authoritative count from PostgreSQL wins,
// and a restarted or pruned state re-seeds instead of forgetting how full the
// inbox already is.
func TestParkCounterStartsUnknown(t *testing.T) {
	var c parkCounter
	if c.ready() {
		t.Error("fresh counter must not be ready")
	}
	if c.total() != 0 {
		t.Errorf("total() = %d, want 0", c.total())
	}
	if c.overCap(100) {
		t.Error("an unseeded counter must not enforce a cap (degrades to no cap, as a failed count did)")
	}
}

func TestParkCounterCountsLocalDeltas(t *testing.T) {
	var c parkCounter
	c.seed(10)
	c.add(1)
	c.add(1)
	if c.total() != 12 {
		t.Errorf("after two parks total() = %d, want 12", c.total())
	}
	c.add(-1) // ackRow
	if c.total() != 11 {
		t.Errorf("after an ack total() = %d, want 11", c.total())
	}
	if !c.ready() {
		t.Error("local deltas must not clear the seeded base")
	}
}

func TestParkCounterCapBoundary(t *testing.T) {
	var c parkCounter
	c.seed(99)
	if c.overCap(100) {
		t.Error("one row below the cap must not be over")
	}
	c.add(1)
	if !c.overCap(100) {
		t.Error("at the cap must be over")
	}
	if c.overCap(0) {
		t.Error("backlog_max of 0 means uncapped, not capped at zero")
	}
}

func TestParkCounterSeedReplacesLocalDeltas(t *testing.T) {
	var c parkCounter
	c.seed(5)
	c.add(3)
	c.seed(7) // the drainer's count is authoritative
	if c.total() != 7 {
		t.Errorf("after seed(7) total() = %d, want 7 (local deltas belong to the count that replaced them)", c.total())
	}
}

// TestParkCounterSurvivesRestart pins the reason the cache is safe: a restart
// or a pruned state does not forget the rows that are already in the inbox,
// because the first park takes the one count ensure() needs.
func TestParkCounterSurvivesRestart(t *testing.T) {
	before := parkCounter{}
	before.seed(100000) // a full inbox before the restart
	if !before.overCap(100000) {
		t.Fatal("a full inbox must be over the cap")
	}

	after := parkCounter{} // same subscription, new process
	if after.overCap(100000) {
		t.Error("a fresh counter has not counted anything yet")
	}
	after.seed(100000) // the single count ensure() runs on first park
	if !after.overCap(100000) {
		t.Error("the inherited rows must be enforced again straight away")
	}
}

// TestSeedParked uses the drainer's count for every known state, treating a
// missing key as zero rows and never inventing a state for an unknown id.
func TestSeedParked(t *testing.T) {
	full := &subState{id: "full"}
	full.parked.seed(4)
	full.parked.add(6) // six parks since the last count
	empty := &subState{id: "empty"}
	d := &Dispatcher{subs: map[string]*subState{"full": full, "empty": empty}}

	d.seedParked([]string{"full", "empty", "gone"}, map[string]int64{"empty": 3})

	if full.parked.total() != 0 {
		t.Errorf("full = %d, want 0 (missing key means the rows are gone)", full.parked.total())
	}
	if empty.parked.total() != 3 || !empty.parked.ready() {
		t.Errorf("empty = %d (ready=%v), want 3 and ready", empty.parked.total(), empty.parked.ready())
	}
	if len(d.subs) != 2 {
		t.Errorf("subs = %d, want 2 (an unknown id must not create a state)", len(d.subs))
	}
}

// TestStateIDsSnapshot keeps the reconciliation passive: reading the states
// must not resurrect one that prune() removed.
func TestStateIDsSnapshot(t *testing.T) {
	d := &Dispatcher{subs: map[string]*subState{"a": {id: "a"}}}
	ids := d.stateIDs()
	if len(ids) != 1 || ids[0] != "a" {
		t.Errorf("stateIDs() = %v, want [a]", ids)
	}
	if len(d.subs) != 1 {
		t.Errorf("subs = %d, want 1", len(d.subs))
	}
}
