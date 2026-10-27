package store

import (
	"context"
	"sync"
	"sync/atomic"
)

// ParkCounter is the cached inbox row count behind the backlog cap of spec 16.
//
// The cap used to be checked with a count(*) per parked delivery. That is
// cheap while an inbox is empty and expensive once it is not: measured on
// 100 000 rows, one count costs about 10 ms, and parking runs at ingestion
// speed for as long as an endpoint stays down - so during exactly the outage
// the cap exists for, the check itself became the heaviest query in the
// system (100 deliveries/s would have burned a full core).
//
// The counter is a cache, never a source of truth: base is the last count
// taken from PostgreSQL and delta what this process parked or acked since. It
// is refilled by whoever already runs a count - the dispatcher's drainer every
// 500 ms, the gateway's reconciler - and a counter that starts from scratch
// (restart, pruned state, rows another process parked) seeds itself once with
// a single count. Every failure mode is therefore bounded and self-correcting:
// an error keeps it unknown (no cap, as a failed count always behaved), a
// restart re-seeds on first use, and the expiry sweep is picked up on the next
// reconciliation.
//
// A counter is shared per process and keyed by subscription ID. Across
// processes it cannot be exact - two nodes would each hold their own delta -
// so the reconciliation interval bounds how far any one of them can drift.
type ParkCounter struct {
	base    atomic.Int64 // last count(*) from PostgreSQL
	delta   atomic.Int64 // parked - acked since that count
	hasBase atomic.Bool  // the cache has been seeded at least once
	initMu  sync.Mutex   // serialises the one-off seed of a fresh counter
}

// Ready reports whether the counter knows anything at all.
func (c *ParkCounter) Ready() bool { return c.hasBase.Load() }

// Total is the cached row count; only meaningful once Ready.
func (c *ParkCounter) Total() int64 { return c.base.Load() + c.delta.Load() }

// Add applies a local change: +1 for a successful park, -1 for an ack.
func (c *ParkCounter) Add(d int64) { c.delta.Add(d) }

// OverCap reports whether the subscription has reached backlog_max. While the
// counter is not ready there is no cap - the degradation a failed count had
// before, kept deliberately: better an unbounded inbox for one error window
// than dropping deliveries we never even tried to store. A max of 0 means no
// cap at all.
func (c *ParkCounter) OverCap(max int64) bool {
	return max > 0 && c.hasBase.Load() && c.Total() >= max
}

// Seed replaces the cache with an authoritative count from PostgreSQL. The
// local delta is dropped first: a row is inserted before its delta is added,
// so anything already inside n must not be counted twice. The remaining
// ambiguity is a park landing while the count runs - if its insert committed
// before the snapshot it is in n (correct), otherwise both n and the dropped
// delta miss it and the cache is low by those few rows until the next seed,
// which lets the inbox exceed backlog_max by that amount and no more. A seed
// applied out of order can equally regress base for one tick. Both errors are
// bounded by a single query window and corrected by the next reconciliation.
func (c *ParkCounter) Seed(n int64) {
	c.delta.Swap(0)
	c.base.Store(n)
	c.hasBase.Store(true)
}

// Ensure seeds the counter from PostgreSQL the first time it is read, for a
// counter that has no idea how many rows it inherited. One count per counter
// lifetime instead of one per park; the next reconciliation takes over from
// there. On an error the counter stays unknown and the caller retries later.
func (c *ParkCounter) Ensure(ctx context.Context, st *Store, id string) error {
	if c.Ready() {
		return nil
	}
	c.initMu.Lock()
	defer c.initMu.Unlock()
	if c.Ready() {
		return nil
	}
	counts, err := st.CountParked(ctx, []string{id})
	if err != nil {
		return err
	}
	c.Seed(counts[id])
	return nil
}
