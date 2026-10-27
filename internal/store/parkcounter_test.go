package store

import "testing"

// ParkCounter replaces the count(*) the backlog cap used to run per parked
// delivery. What has to hold: it starts unknown (no cap until it knows), it
// follows local parks and acks, the authoritative count from PostgreSQL wins,
// and a restarted counter re-seeds instead of forgetting how full the inbox
// already is.
func TestParkCounterStartsUnknown(t *testing.T) {
	var c ParkCounter
	if c.Ready() {
		t.Error("fresh counter must not be ready")
	}
	if c.Total() != 0 {
		t.Errorf("Total() = %d, want 0", c.Total())
	}
	if c.OverCap(100) {
		t.Error("an unseeded counter must not enforce a cap (degrades to no cap, as a failed count did)")
	}
}

func TestParkCounterCountsLocalDeltas(t *testing.T) {
	var c ParkCounter
	c.Seed(10)
	c.Add(1)
	c.Add(1)
	if c.Total() != 12 {
		t.Errorf("after two parks Total() = %d, want 12", c.Total())
	}
	c.Add(-1) // ack
	if c.Total() != 11 {
		t.Errorf("after an ack Total() = %d, want 11", c.Total())
	}
	if !c.Ready() {
		t.Error("local deltas must not clear the seeded base")
	}
}

func TestParkCounterCapBoundary(t *testing.T) {
	var c ParkCounter
	c.Seed(99)
	if c.OverCap(100) {
		t.Error("one row below the cap must not be over")
	}
	c.Add(1)
	if !c.OverCap(100) {
		t.Error("at the cap must be over")
	}
	if c.OverCap(0) {
		t.Error("backlog_max of 0 means uncapped, not capped at zero")
	}
}

func TestParkCounterSeedReplacesLocalDeltas(t *testing.T) {
	var c ParkCounter
	c.Seed(5)
	c.Add(3)
	c.Seed(7) // the reconciliation count is authoritative
	if c.Total() != 7 {
		t.Errorf("after Seed(7) Total() = %d, want 7 (local deltas belong to the count that replaced them)", c.Total())
	}
}

// TestParkCounterSurvivesRestart pins the reason the cache is safe: a restart
// does not forget the rows that are already in the inbox, because the first
// use takes the one count Ensure needs.
func TestParkCounterSurvivesRestart(t *testing.T) {
	before := ParkCounter{}
	before.Seed(100000) // a full inbox before the restart
	if !before.OverCap(100000) {
		t.Fatal("a full inbox must be over the cap")
	}

	after := ParkCounter{} // same subscription, new process
	if after.OverCap(100000) {
		t.Error("a fresh counter has not counted anything yet")
	}
	after.Seed(100000) // the single count Ensure runs on first use
	if !after.OverCap(100000) {
		t.Error("the inherited rows must be enforced again straight away")
	}
}
