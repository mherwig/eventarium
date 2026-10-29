package chquery

import "testing"

// TestStatusesMatchesTheDocumentedContract pins the lifecycle list that seeds
// the zero buckets of the status summary and ranks the latest record of every
// query against the values the emitters actually write (README, "Delivery
// statuses"). A status that exists in the data but not here would show up
// without a zero bucket; one that exists only here would always read 0.
//
// Order matters too: it is the lifecycle rank of LatestKeySQL, so inserting
// in the middle would silently re-rank every existing status.
func TestStatusesMatchesTheDocumentedContract(t *testing.T) {
	want := []string{
		"pending",
		"in_flight",
		"retrying",
		"delivered",
		"sent",
		"failed",
		"dead_lettered",
		"paused",
		"expired",
		"dropped",
		// Appended, never inserted: the router's verdict for a candidate
		// subscription the filter rejected, which has no place in a
		// delivery's lifecycle because it never becomes one.
		"filtered",
	}
	if len(Statuses) != len(want) {
		t.Fatalf("Statuses has %d entries, want %d: %v", len(Statuses), len(want), Statuses)
	}
	for i := range want {
		if Statuses[i] != want[i] {
			t.Errorf("Statuses[%d] = %q, want %q", i, Statuses[i], want[i])
		}
	}
}
