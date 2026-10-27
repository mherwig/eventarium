package dispatch

import (
	"context"
	"log/slog"
	"sort"
	"time"

	"pubsub/internal/delivery"
	"pubsub/internal/store"
)

// drainLoop is the inbox handover of spec 6.6: every 500ms it looks for
// subscriptions that are healthy again (breaker closed or probe due, no queue,
// nothing in flight, not paused) and drains their parked rows in (partition,
// offset) order at drain_rate_per_s. Expired rows are swept every 5 minutes.
func (d *Dispatcher) drainLoop() {
	tick := time.NewTicker(drainInterval)
	defer tick.Stop()
	expiry := time.NewTicker(expiryInterval)
	defer expiry.Stop()
	refresh := time.NewTicker(knownRefresh)
	defer refresh.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-refresh.C:
			d.refreshKnown()
		case <-expiry.C:
			ctx, cancel := context.WithTimeout(context.WithoutCancel(d.ctx), 30*time.Second)
			if n, err := d.st.DeleteExpiredParked(ctx); err != nil {
				slog.Warn("dispatcher: deleting expired parked rows failed", "err", err)
			} else if n > 0 {
				slog.Info("dispatcher: expired parked rows deleted", "rows", n)
			}
			cancel()
		case <-tick.C:
			d.prune()
			d.drainOnce()
		}
	}
}

// refreshKnown snapshots the webhook subscription IDs from PostgreSQL. The
// shared registry only offers lookup by ID, so the drainer keeps its own set;
// it must know every subscription that can have parked rows, including ones
// with no live traffic since a restart.
func (d *Dispatcher) refreshKnown() {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(d.ctx), 15*time.Second)
	defer cancel()
	subs, err := d.st.Subscriptions(ctx)
	if err != nil {
		slog.Warn("dispatcher: loading subscription ids for the drainer failed", "err", err)
		return
	}
	known := make(map[string]struct{}, len(subs))
	for _, s := range subs {
		if s.Channel == delivery.ChannelWebhook {
			known[s.ID] = struct{}{}
		}
	}
	d.mu.Lock()
	d.known, d.knownAt = known, time.Now()
	d.mu.Unlock()
}

// candidateIDs returns the webhook subscriptions the drainer has to inspect.
func (d *Dispatcher) candidateIDs() []string {
	d.mu.Lock()
	ids := make(map[string]struct{}, len(d.known)+len(d.subs))
	for id := range d.known {
		ids[id] = struct{}{}
	}
	for id := range d.subs {
		ids[id] = struct{}{}
	}
	d.mu.Unlock()
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for id := range ids {
		out = append(out, id)
	}
	return out
}

// stateIDs returns the subscriptions that currently have a state object, i.e.
// the ones whose cached backlog cap can be refreshed. States are deliberately
// not created here: an idle subscription must stay prunable.
func (d *Dispatcher) stateIDs() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]string, 0, len(d.subs))
	for id := range d.subs {
		out = append(out, id)
	}
	return out
}

// seedParked refreshes the cached inbox counts of the given states from one
// count(*). A missing key means zero rows: the subscription was drained, its
// rows expired or they were never there.
func (d *Dispatcher) seedParked(ids []string, counts map[string]int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ids {
		if s, ok := d.subs[id]; ok {
			s.parked.Seed(counts[id])
		}
	}
}

// drainOnce drains one batch per eligible subscription.
func (d *Dispatcher) drainOnce() {
	d.refreshKnownIfStale()
	ids := d.candidateIDs()
	if len(ids) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(d.ctx), 15*time.Second)
	defer cancel()
	// Snapshot the states before counting: a state created after this point
	// seeds itself, while these are the ones whose cached cap this count may
	// replace.
	stateIDs := d.stateIDs()
	counts, err := d.st.CountParked(ctx, ids)
	if err != nil {
		slog.Warn("dispatcher: counting parked rows failed", "err", err)
		return
	}
	// This count runs anyway (it selects the subscriptions with rows) and is
	// also what keeps the cached backlog caps honest: it picks up rows parked
	// by an earlier process, rows acked since the last tick and rows the
	// expiry sweep deleted, without a single extra query.
	d.seedParked(stateIDs, counts)
	var withRows []string
	for _, id := range ids {
		if counts[id] == 0 {
			continue
		}
		// Gate before the read (section 6.6). FetchParked selects payloads,
		// so a subscription whose breaker is open, that is paused by a 429 or
		// that is busy with live work would otherwise cost up to drainBatch
		// rows read twice a second only to be thrown away below - exactly
		// while an endpoint is down, when the drainer should be quietest.
		if s, ok := d.drainState(id); !ok || !drainEligible(s) {
			continue
		}
		withRows = append(withRows, id)
	}
	if len(withRows) == 0 {
		return
	}
	rows, err := d.st.FetchParked(ctx, withRows, drainBatch)
	if err != nil {
		slog.Warn("dispatcher: fetching parked rows failed", "err", err)
		return
	}
	bySub := map[string][]store.InboxRow{}
	for _, r := range rows {
		bySub[r.SubscriptionID] = append(bySub[r.SubscriptionID], r)
	}
	for id, rs := range bySub {
		s, ok := d.drainState(id)
		if !ok {
			continue // deleted subscription; its rows expire
		}
		// Checked again here, right before the batch: the live workers change
		// the breaker, the pause and the queue depth while the inbox was read.
		if !drainEligible(s) {
			continue
		}
		if s.draining.Swap(true) {
			continue // already draining
		}
		d.drainSub(ctx, s, rs)
		s.draining.Store(false)
	}
}

// drainState resolves the state of a drain candidate. Only a webhook
// subscription that still exists in the registry may be drained, and its
// state carries the breaker, the 429 pause and the live-work counters the
// drainer has to respect.
func (d *Dispatcher) drainState(id string) (*subState, bool) {
	entry, ok := d.reg.Subscription(id)
	if !ok || entry.Sub == nil || entry.Sub.Channel != delivery.ChannelWebhook {
		return nil, false
	}
	return d.stateFor(entry.Sub), true
}

// drainEligible reports whether a subscription may start a drain round: the
// breaker is closed or its probe is due, no 429 pause is running, nothing is
// in flight and no delivery is queued (spec 6.6 - drain only what the hot
// path is not already doing). drainOnce calls it twice, before reading the
// inbox and again before the batch; both reads are needed because the second
// one sees the state the live workers produced meanwhile.
func drainEligible(s *subState) bool {
	if !s.breaker.canAttempt() || s.pauseFor() > 0 || s.inFlight.Load() > 0 {
		return false
	}
	for _, q := range s.qs {
		if len(q) > 0 {
			return false
		}
	}
	return true
}

func (d *Dispatcher) refreshKnownIfStale() {
	d.mu.Lock()
	stale := time.Since(d.knownAt) > knownRefresh
	d.mu.Unlock()
	if stale {
		d.refreshKnown()
	}
}

// drainSub delivers the rows of one subscription in (partition, offset) order
// through the drain token bucket. A terminal outcome acks the row; an aborted
// attempt (shutdown, breaker still refusing, 429 mid-drain) leaves the row and
// the rest of the batch for the next round, so order is never overtaken.
func (d *Dispatcher) drainSub(ctx context.Context, s *subState, rows []store.InboxRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Part != rows[j].Part {
			return rows[i].Part < rows[j].Part
		}
		return rows[i].Offset < rows[j].Offset
	})
	for _, row := range rows {
		if d.ctx.Err() != nil {
			return
		}
		msg, err := delivery.Decode(row.Payload)
		if err != nil {
			// A poison row would block the inbox forever; ack it away.
			slog.Warn("dispatcher: undecodable parked row dropped",
				"subscription", s.id, "part", row.Part, "offset", row.Offset, "err", err)
			d.ackRow(ctx, s, row)
			continue
		}
		ok, err := s.drainBucket.Wait(d.ctx, 0)
		if err != nil || !ok {
			return
		}
		it := &item{
			msg:       msg,
			seq:       delivery.Seq{Partition: row.Part, Offset: row.Offset},
			fromInbox: true,
		}
		out := s.attemptLoop(d.ctx, it, true)
		switch out {
		case outcomeAborted:
			return
		default:
			// delivered, dead-lettered, dropped or expired: the row has seen
			// its final outcome, remove it from the inbox.
			d.ackRow(ctx, s, row)
		}
	}
}

// ackRow removes a row whose outcome is final. The cached backlog cap is
// lowered with it, so the subscription can park again between two counts.
func (d *Dispatcher) ackRow(ctx context.Context, s *subState, row store.InboxRow) {
	actx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := d.st.AckParked(actx, []store.InboxRow{row}); err != nil {
		slog.Warn("dispatcher: acking parked row failed",
			"subscription", row.SubscriptionID, "part", row.Part, "offset", row.Offset, "err", err)
		return
	}
	s.parked.Add(-1)
}
