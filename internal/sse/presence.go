package sse

import (
	"context"
	"sync"
	"time"

	"pubsub/internal/store"
)

// Presence lives in PostgreSQL (migration 005), not in a compacted topic: the
// question it answers - which connection, on which node, currently owns a
// subscription - is state, and state written by a process that may die without
// cleaning up needs a clock, not a log. A topic record is only replaced by a
// newer record *for the same key*, and a reconnect mints a new key, so a node
// that never published its tombstones left entries that no compaction would
// ever remove; pick() could then route deliveries at a connection nobody
// owns.
//
// The timings below are one mechanism split into three numbers, and they only
// make sense together:
//
//	presenceHeartbeat   2s   how often a live node refreshes its lease
//	presenceLeaseTTL    6s   how long after that a node's connections stay
//	                        routable - three missed beats
//	presenceReapStale  60s   how long after that the rows are deleted
//
// The gap between the TTL and the reap is deliberate. Visibility is what
// correctness depends on and it fails *fast* (a killed node stops receiving
// deliveries after six seconds); deletion is hygiene and fails *slowly*, so a
// node that was merely unreachable for a minute has its rows still present
// when it comes back and can re-assert them instead of finding them gone.
const (
	presenceHeartbeat   = 2 * time.Second
	presenceLeaseTTL    = 6 * time.Second
	presencePoll        = time.Second // safety net under NOTIFY
	presenceReapEvery   = time.Minute
	presenceReapStale   = time.Minute
	presenceWriteBounds = 5 * time.Second // one write on the connect path
)

// presenceRecord is the view's record of one connection: where it lives and
// who owns it. It is what pick() returns to the delivery stage, which needs
// the node ID to target the sse-live message and the connection ID to stamp
// the status record.
type presenceRecord struct {
	NodeID         string
	ConnectionID   string
	SubscriptionID string
	ConsumerID     string
	ConnectedAt    time.Time
}

// presenceView is the in-memory mirror of the presence tables. It answers the
// only question the delivery stage needs: which connection, on which node,
// currently owns a subscription (section 6.5).
//
// Every gateway holds the whole view, not its slice: the delivery stage runs
// wherever the deliveries partition is assigned, which is usually *not* where
// the connection lives, so a node-local view would park deliveries another
// node could have taken. That is why this is a mirror refreshed wholesale
// rather than work handed out by a consumer group.
type presenceView struct {
	mu    sync.RWMutex
	bySub map[string]map[string]presenceRecord // subscription ID -> connection ID -> record
}

func newPresenceView() *presenceView {
	return &presenceView{bySub: map[string]map[string]presenceRecord{}}
}

// replace swaps the whole view for a fresh snapshot of the tables, overlaid
// with the connections this process is holding right now.
//
// The overlay closes a race that snapshot reads cannot: a reload whose query
// started before a local connect committed would otherwise drop that
// connection from the view until the next reload. This process is the
// authority for its own connections - it has them in hand and it wrote their
// rows - so it re-applies them while it holds the lock, and no interleaving of
// a slow query with a fast connect can lose either one. The reverse (a row
// this process just deleted) is corrected by the notification its own delete
// queued.
func (v *presenceView) replace(snapshot []store.PresenceRow, local []presenceRecord) {
	next := make(map[string]map[string]presenceRecord, len(snapshot)+len(local))
	put := func(r presenceRecord) {
		if r.SubscriptionID == "" || r.ConnectionID == "" {
			return
		}
		m := next[r.SubscriptionID]
		if m == nil {
			m = map[string]presenceRecord{}
			next[r.SubscriptionID] = m
		}
		m[r.ConnectionID] = r
	}
	for _, row := range snapshot {
		put(presenceRecord{
			NodeID:         row.NodeID,
			ConnectionID:   row.ConnectionID,
			SubscriptionID: row.SubscriptionID,
			ConsumerID:     row.ConsumerID,
			ConnectedAt:    row.ConnectedAt,
		})
	}
	for _, r := range local {
		put(r)
	}
	v.mu.Lock()
	v.bySub = next
	v.mu.Unlock()
}

// put adds or replaces one connection immediately. The connect path calls it
// after the row is committed, so this node's own delivery stage sees the
// connection without waiting for the notification to come back around - which
// is what the connect path's "presence first" ordering is about: a delivery
// arriving before presence is visible parks for no reason.
func (v *presenceView) put(r presenceRecord) {
	if r.SubscriptionID == "" || r.ConnectionID == "" {
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	m := v.bySub[r.SubscriptionID]
	if m == nil {
		m = map[string]presenceRecord{}
		v.bySub[r.SubscriptionID] = m
	}
	m[r.ConnectionID] = r
}

// remove drops one connection. The disconnect path calls it after the row is
// deleted; the row's absence is also what the next reload will see, so this
// is only about not routing to a connection this process has already closed.
func (v *presenceView) remove(subID, connID string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if m := v.bySub[subID]; m != nil {
		delete(m, connID)
		if len(m) == 0 {
			delete(v.bySub, subID)
		}
	}
}

// pick chooses the connection that receives the next delivery of a
// subscription. A connection on this node is preferred to save the sse-live
// hop. Section 7's rendezvous hashing is not implemented: any remaining
// connection will do.
func (v *presenceView) pick(subID, nodeID string) (presenceRecord, bool) {
	v.mu.RLock()
	defer v.mu.RUnlock()
	m := v.bySub[subID]
	if len(m) == 0 {
		return presenceRecord{}, false
	}
	for _, rec := range m {
		if rec.NodeID == nodeID {
			return rec, true
		}
	}
	for _, rec := range m {
		return rec, true
	}
	return presenceRecord{}, false
}

// subscriptions lists the subscriptions that currently have a connection. The
// drainer uses it to know which parked rows have somewhere to go.
func (v *presenceView) subscriptions() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]string, 0, len(v.bySub))
	for subID := range v.bySub {
		out = append(out, subID)
	}
	return out
}

// local returns the connections this process holds, for the overlay of
// replace().
func (g *Gateway) local() []presenceRecord {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]presenceRecord, 0, len(g.conns))
	for _, c := range g.conns {
		out = append(out, presenceRecord{
			NodeID:         g.opts.NodeID,
			ConnectionID:   c.id,
			SubscriptionID: c.subID,
			ConsumerID:     c.consumerID,
			ConnectedAt:    c.openedAt,
		})
	}
	return out
}

// recordPresence writes an open connection and makes it visible locally. Both
// halves matter: the row is what the *other* gateways learn about through the
// notification, the local put is what this node's own delivery stage sees
// before the first byte reaches the client.
func (g *Gateway) recordPresence(ctx context.Context, c *connection) error {
	row := store.PresenceRow{
		SubscriptionID: c.subID,
		ConnectionID:   c.id,
		NodeID:         g.opts.NodeID,
		ConsumerID:     c.consumerID,
		ConnectedAt:    c.openedAt,
	}
	if err := g.store.UpsertPresence(ctx, row); err != nil {
		return err
	}
	g.view.put(presenceRecord{
		NodeID:         row.NodeID,
		ConnectionID:   row.ConnectionID,
		SubscriptionID: row.SubscriptionID,
		ConsumerID:     row.ConsumerID,
		ConnectedAt:    row.ConnectedAt,
	})
	return nil
}

// forgetPresence deletes a closed connection. It uses its own context because
// it runs from a defer, where the request context is usually already
// cancelled - and a disconnect that cannot be written must not hang the
// handler. The lease is the backstop: if this write is lost, the row stops
// being visible when this node's heartbeat stops, and the reaper collects it.
func (g *Gateway) forgetPresence(c *connection) {
	ctx, cancel := ctxTimeout(presenceWriteBounds)
	defer cancel()
	if err := g.store.DeletePresence(ctx, c.subID, c.id); err != nil {
		g.log.Warn("presence: removing a closed connection failed",
			"subscription", c.subID, "connection", c.id, "err", err)
	}
	g.view.remove(c.subID, c.id)
}

// reloadPresence replaces the view from the tables.
//
// A failed reload keeps the previous view - the same rule the registry
// follows. Stale presence degrades to routing at connections that may have
// closed (the sse-live consumer drops those and the client recovers through
// Last-Event-ID) or to parking deliveries another node could take; neither
// loses anything, and blanking the view on a database blip would be far worse.
func (g *Gateway) reloadPresence(ctx context.Context) error {
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()
	qctx, cancel := ctxTimeout(10 * time.Second)
	defer cancel()
	rows, err := g.store.VisiblePresence(qctx, presenceLeaseTTL)
	if err != nil {
		return err
	}
	g.view.replace(rows, g.local())
	return nil
}

// runPresence keeps the view fresh: a notification is the fast path, the
// ticker is the safety net for notifications PostgreSQL never delivered
// (they are only sent to listeners connected when the write commits), and the
// heartbeat and reaper are this node's own duties.
//
// The heartbeat's gap check is what makes the reap horizon safe to have at
// all. If this process could not write for longer than presenceReapStale, its
// rows may already be gone; the first successful touch after such a gap
// re-inserts the connections it is actually holding, so a node that was
// unreachable for a minute resumes serving instead of finding itself
// invisible while its lease says otherwise.
func (g *Gateway) runPresence(ctx context.Context) {
	// Fast path. It runs in its own goroutine because LISTEN blocks.
	go store.Listen(ctx, g.opts.PGDSN, store.PresenceNotifyChannel,
		store.PresenceListenAppName, func(lctx context.Context) {
			if err := g.reloadPresence(lctx); err != nil && lctx.Err() == nil {
				g.log.Warn("presence: reload after notification failed", "err", err)
			}
		})

	poll := time.NewTicker(presencePoll)
	beat := time.NewTicker(presenceHeartbeat)
	reap := time.NewTicker(presenceReapEvery)
	defer poll.Stop()
	defer beat.Stop()
	defer reap.Stop()

	// Seeded now so the first beat is an ordinary one: startup already
	// cleared this node's rows (ClearNodePresence), so there is nothing a
	// gap could have cost yet, and a re-assert on the first tick would only
	// log a gap measured from the zero time.
	lastTouch := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
			if err := g.reloadPresence(ctx); err != nil && ctx.Err() == nil {
				g.log.Warn("presence: reload failed", "err", err)
			}
		case <-beat.C:
			g.touchNode(ctx, &lastTouch)
		case <-reap.C:
			g.reapPresence(ctx)
		}
	}
}

// touchNode refreshes this node's lease, and after a gap long enough for the
// reaper to have acted, re-asserts every connection this process holds.
func (g *Gateway) touchNode(ctx context.Context, lastTouch *time.Time) {
	tctx, cancel := ctxTimeout(presenceWriteBounds)
	err := g.store.TouchNode(tctx, g.opts.NodeID)
	cancel()
	if err != nil {
		if ctx.Err() == nil {
			g.log.Warn("presence: refreshing this node's lease failed", "err", err)
		}
		return // lastTouch stays put, so the gap keeps growing
	}
	gap := time.Since(*lastTouch)
	*lastTouch = time.Now()
	if gap <= presenceReapStale {
		return // comfortably inside the reap horizon: nothing could have been deleted
	}
	g.reassertLocal(ctx, gap)
}

// reassertLocal re-inserts the rows of the connections this process holds.
// They are the rows a reap would have taken while this node was unable to
// heartbeat; every other node's view is corrected by the notifications these
// inserts queue.
func (g *Gateway) reassertLocal(ctx context.Context, gap time.Duration) {
	rows := g.local()
	if len(rows) == 0 {
		return
	}
	g.log.Info("presence: re-asserting connections after a lease gap",
		"connections", len(rows), "gap", gap.Round(time.Second).String())
	qctx, cancel := ctxTimeout(presenceWriteBounds)
	defer cancel()
	for _, r := range rows {
		if err := g.store.UpsertPresence(qctx, store.PresenceRow{
			SubscriptionID: r.SubscriptionID,
			ConnectionID:   r.ConnectionID,
			NodeID:         r.NodeID,
			ConsumerID:     r.ConsumerID,
			ConnectedAt:    r.ConnectedAt,
		}); err != nil {
			g.log.Warn("presence: re-asserting a connection failed",
				"subscription", r.SubscriptionID, "err", err)
			return
		}
	}
}

// reapPresence deletes rows nobody has been keeping alive. Every gateway runs
// it: the statements are keyed by the lease table, so they delete the same
// dead rows whoever issues them, and a fleet doing it redundantly is cheaper
// than electing a leader to do it once. Nothing reads these rows - visibility
// already excluded them - so a missed or delayed reap costs table space and
// nothing else.
func (g *Gateway) reapPresence(ctx context.Context) {
	qctx, cancel := ctxTimeout(presenceWriteBounds)
	defer cancel()
	presence, nodes, err := g.store.ReapPresence(qctx, presenceReapStale)
	if err != nil {
		if ctx.Err() == nil {
			g.log.Warn("presence: reaping stale rows failed", "err", err)
		}
		return
	}
	if presence > 0 || nodes > 0 {
		g.log.Info("presence: reaped stale rows",
			"connections", presence, "nodes", nodes, "stale_after", presenceReapStale.String())
	}
}
