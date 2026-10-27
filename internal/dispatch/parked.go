package dispatch

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

	"pubsub/internal/store"
)

// parkCounter is the cached inbox row count behind the backlog cap of spec 16.
//
// The cap used to be checked with a count(*) per parked delivery. That is
// cheap while an inbox is empty and expensive once it is not: measured on
// 100 000 rows, one count costs about 10 ms, and the hot path parks at
// ingestion speed for as long as an endpoint stays down - so during exactly
// the outage the cap exists for, the check itself became the heaviest query
// in the system (100 deliveries/s would have burned a full core).
//
// The counter is a cache, never a source of truth: base is the last count
// taken from PostgreSQL and delta what this process parked or acked since.
// It is refilled from a query the drainer already runs every 500 ms
// (drainOnce), and a state that starts from scratch - restart, pruned state,
// rows another process parked - seeds itself once with a single count. Every
// failure mode is therefore bounded and self-correcting: an error keeps the
// counter unknown (no cap, as a failed count always behaved), a restart or a
// prune re-seeds on first use, and the expiry sweep is picked up within half
// a second.
type parkCounter struct {
	base    atomic.Int64 // last count(*) from PostgreSQL
	delta   atomic.Int64 // parked - acked since that count
	hasBase atomic.Bool  // the cache has been seeded at least once
	initMu  sync.Mutex   // serialises the one-off seed of a fresh state
}

// ready reports whether the counter knows anything at all.
func (c *parkCounter) ready() bool { return c.hasBase.Load() }

// total is the cached row count; only meaningful once ready.
func (c *parkCounter) total() int64 { return c.base.Load() + c.delta.Load() }

// add applies a local change: +1 for a successful park, -1 for an ack.
func (c *parkCounter) add(d int64) { c.delta.Add(d) }

// overCap reports whether the subscription has reached backlog_max. While the
// counter is not ready there is no cap - the degradation a failed count had
// before, kept deliberately: better an unbounded inbox for one error window
// than dropping deliveries we never even tried to store.
func (c *parkCounter) overCap(max int64) bool {
	return max > 0 && c.hasBase.Load() && c.total() >= max
}

// seed replaces the cache with an authoritative count from PostgreSQL. The
// local delta is dropped first: a row is inserted before its delta is added,
// so anything already inside n must not be counted twice. The remaining
// ambiguity is a park landing while the count runs - if its insert committed
// before the snapshot it is in n (correct), otherwise both n and the dropped
// delta miss it and the cache is low by those few rows until the next seed,
// which lets the inbox exceed backlog_max by that amount and no more. A seed
// applied out of order can equally regress base for one tick. Both errors are
// bounded by a single query window and corrected 500 ms later.
func (c *parkCounter) seed(n int64) {
	c.delta.Swap(0)
	c.base.Store(n)
	c.hasBase.Store(true)
}

// ensure seeds the counter from PostgreSQL the first time it is read, for a
// state that has no idea how many rows it inherited. One count per state
// lifetime instead of one per park; the drainer's next tick takes over from
// there. On an error the counter stays unknown and the next park retries.
func (c *parkCounter) ensure(ctx context.Context, st *store.Store, id string) {
	if c.ready() {
		return
	}
	c.initMu.Lock()
	defer c.initMu.Unlock()
	if c.ready() {
		return
	}
	counts, err := st.CountParked(ctx, []string{id})
	if err != nil {
		slog.Warn("dispatcher: counting parked rows for the cap failed",
			"subscription", id, "err", err)
		return
	}
	c.seed(counts[id])
}
