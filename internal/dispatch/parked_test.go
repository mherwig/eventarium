package dispatch

import "testing"

// TestSeedParked uses the drainer's count for every known state, treating a
// missing key as zero rows and never inventing a state for an unknown id.
func TestSeedParked(t *testing.T) {
	full := &subState{id: "full"}
	full.parked.Seed(4)
	full.parked.Add(6) // six parks since the last count
	empty := &subState{id: "empty"}
	d := &Dispatcher{subs: map[string]*subState{"full": full, "empty": empty}}

	d.seedParked([]string{"full", "empty", "gone"}, map[string]int64{"empty": 3})

	if full.parked.Total() != 0 {
		t.Errorf("full = %d, want 0 (missing key means the rows are gone)", full.parked.Total())
	}
	if empty.parked.Total() != 3 || !empty.parked.Ready() {
		t.Errorf("empty = %d (ready=%v), want 3 and ready", empty.parked.Total(), empty.parked.Ready())
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
