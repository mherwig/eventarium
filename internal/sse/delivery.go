package sse

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"pubsub/internal/delivery"
	"pubsub/internal/kafkautil"
	"pubsub/internal/status"
	"pubsub/internal/store"
)

// defaultDrainRate is the drain_rate_per_s used when the subscription carries
// no policy value (section 7; the registry API fills the defaults in the full
// platform).
const defaultDrainRate = 100

// drainTick is the drainer's polling interval.
const drainTick = 500 * time.Millisecond

// parkedReconcile is how often the cached inbox counts behind backlog_max are
// refreshed from PostgreSQL (spec 16). It bounds how far a counter can drift:
// rows the expiry sweep deleted, rows another node parked or acked - the
// drainer is allowed to run on a different node than the delivery stage - and
// any seed that raced a count.
const parkedReconcile = time.Second

// reasonParkFailed is the status reason used when a delivery could neither be
// written to sse-live nor encoded for the inbox: terminal, retried never.
const reasonParkFailed = "park_failed"

const (
	// parkRetryQueue bounds the hand-off to the park-retry worker. When it is
	// full the caller blocks, which is deliberate: the alternative is
	// committing an offset for a record that nobody holds any more.
	parkRetryQueue = 1024
	// parkRetryMax bounds what the retry worker keeps for itself, for the
	// same reason. A store that stays unreachable has to turn into
	// backpressure on the poll loop, not into memory - the offsets stay
	// uncommitted either way and Kafka keeps the records.
	parkRetryMax = 4096
	// parkRetryTick is how often a park that failed is attempted again.
	parkRetryTick = 2 * time.Second
	// parkLogEvery throttles the park-failure log: parking runs at delivery
	// speed and again every retry tick, so one line per attempt would drown
	// the log exactly while the store is down.
	parkLogEvery = 30 * time.Second
)

// runDeliveryStage consumes the deliveries topic. The consumer group is
// sse-delivery: across the group every partition of the topic is owned by
// exactly one node, and within a node one poll batch is processed serially, so
// per-subscription order is preserved (sections 6.3 and 8).
func (g *Gateway) runDeliveryStage(ctx context.Context) {
	for {
		fs := g.deliveryCl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				g.log.Warn("sse-delivery: poll", "err", e)
			}
			if !sleep(ctx, time.Second) {
				return
			}
		}
		recs := fs.Records()
		// Every polled record joins the commit gate before anything is done
		// with it: the gate may only decide about records it knows, and one it
		// never learned about would be invisible to the commit point, which
		// could then step over it.
		g.gate.Track(recs...)
		g.processDeliveries(ctx, recs)
		// The watermark, not the batch. Whatever in this batch is not durable
		// yet - a row waiting for the background park retry above all - keeps
		// its partition's commit point where it is, so a crash repeats it
		// instead of losing it; the duplicates are identified by the event
		// reference (section 8).
		g.commitHeads(ctx)
	}
}

// commitHeads commits the durable watermark of every partition the gate knows.
// Unlike the dispatcher, which marks and lets franz-go's autocommit flush, this
// consumer commits explicitly (DisableAutoCommit) after each batch and after a
// park the background retry got through.
func (g *Gateway) commitHeads(ctx context.Context) {
	heads := g.gate.CommitPoints()
	if len(heads) == 0 {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	g.deliveryCl.CommitOffsetsSync(cctx, heads,
		func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				// Not fatal: an uncommitted watermark only means the batch is
				// repeated after a restart, which is the direction we want.
				g.log.Warn("sse-delivery: committing offsets", "err", err)
			}
		})
}

// settle releases one record's offset: the delivery behind it reached a
// durable outcome, so its partition's commit point may move past it. The
// gateway consumes the deliveries topic alone, hence the topic name; a settle
// for something the gate does not know (an inbox row, a partition a rebalance
// took away) is a no-op.
func (g *Gateway) settle(partition int32, offset int64) {
	g.gate.Settle(kafkautil.TopicDeliveries, partition, offset)
}

// release settles a batch of records that reached a durable outcome.
func (g *Gateway) release(seqs []delivery.Seq) {
	for _, seq := range seqs {
		g.settle(seq.Partition, seq.Offset)
	}
}

// deliveryItem is one delivery record with its position on the deliveries
// topic; the position is the sequence number and therefore the ack handle.
type deliveryItem struct {
	seq delivery.Seq
	msg *delivery.Message
}

// ownsChannel reports whether the sse-delivery stage handles the record: the
// subscription's current channel, falling back to the stamp for an ID this
// instance does not know. registry.ChannelFor is the one implementation of
// that rule - the dispatcher applies it to the same data, which is what keeps
// exactly one of the two stages on each record (see the note in
// registry.ChannelFor for the reload window an update opens).
func (g *Gateway) ownsChannel(msg *delivery.Message) bool {
	return g.reg.ChannelFor(msg.SubscriptionID, msg.Channel) == delivery.ChannelSSE
}

func (g *Gateway) processDeliveries(ctx context.Context, recs []*kgo.Record) {
	bySub := map[string][]deliveryItem{}
	// durable holds the records whose outcome lives outside this process - an
	// inbox row or a record on sse-live - and statusOnly the ones whose only
	// trace is the status record below. Both are released after the status
	// emit, statusOnly only if it succeeded: a dropped delivery whose status
	// was lost would otherwise vanish without a trace, and replaying it is
	// what re-emits the status.
	var durable, statusOnly []delivery.Seq
	keep := func(seq delivery.Seq) { durable = append(durable, seq) }
	byStatus := func(seq delivery.Seq) { statusOnly = append(statusOnly, seq) }
	for _, rec := range recs {
		seq := delivery.Seq{Partition: rec.Partition, Offset: rec.Offset}
		msg, err := delivery.Decode(rec.Value)
		if err != nil {
			// Undecodable: nothing can ever act on it, and it carries no
			// status either, so it is as durable as it will ever be.
			g.log.Error("sse-delivery: decoding record", "err", err,
				"partition", rec.Partition, "offset", rec.Offset)
			keep(seq)
			continue
		}
		if !g.ownsChannel(msg) {
			keep(seq) // webhook deliveries belong to the dispatcher
			continue
		}
		it := deliveryItem{seq: seq, msg: msg}
		bySub[msg.SubscriptionID] = append(bySub[msg.SubscriptionID], it)
	}

	var (
		liveItems   []deliveryItem
		liveTargets []presenceRecord
		parkRows    []store.InboxRow
		pending     []deliveryItem
		statuses    []status.Record
	)
	for subID, items := range bySub {
		target, hasConn := g.view.pick(subID, g.opts.NodeID)
		backlogCap := g.backlogMax(subID) // backlog_max of section 7, 0 = no cap
		var parked int64
		if !hasConn && backlogCap > 0 {
			// The cached count instead of a count(*) per batch: parking runs
			// at ingestion speed while nobody is connected, and one count over
			// a full inbox costs about 10 ms. Until the counter knows the rows
			// it inherited there is no cap - the degradation a failed count
			// had (warn, then park rather than drop).
			c := g.parkedCounter(subID)
			if err := c.Ensure(ctx, g.store, subID); err != nil {
				g.log.Warn("sse-delivery: counting parked rows", "err", err, "subscription", subID)
				backlogCap = 0
			} else {
				parked = c.Total()
			}
		}
		for _, it := range items {
			switch {
			case hasConn:
				liveItems = append(liveItems, it)
				liveTargets = append(liveTargets, target)
			case it.msg.RetentionClass == kafkautil.ClassLive:
				// Live events have no redelivery duty, so they are never
				// parked (section 12); without a connection they are dropped.
				statuses = append(statuses, statusFor(it.msg, status.Dropped, "live_class", ""))
				byStatus(it.seq)
			case !it.msg.ExpiresAt.After(time.Now()):
				// Section 8: after expires_at a delivery is expired.
				statuses = append(statuses, statusFor(it.msg, status.Expired, "", ""))
				byStatus(it.seq)
			case backlogCap > 0 && parked >= backlogCap:
				// Section 9: dropped because backlog_max was exceeded.
				statuses = append(statuses, statusFor(it.msg, status.Dropped, "backlog_max", ""))
				byStatus(it.seq)
			default:
				row, err := parkRow(it, "no_connection")
				if err != nil {
					// The payload cannot be encoded for the inbox, so it never
					// will be - retrying cannot help and committing a record
					// nobody will ever see again would be silent loss. A
					// terminal drop with a status, exactly what the dispatcher
					// does with the same failure (reason park_failed).
					g.log.Error("sse-delivery: encoding parked payload", "err", err, "subscription", subID)
					statuses = append(statuses, statusFor(it.msg, status.Dropped, reasonParkFailed, ""))
					byStatus(it.seq)
					continue
				}
				parkRows = append(parkRows, row)
				pending = append(pending, it)
				parked++
			}
		}
	}

	// Park first: the inbox is the durable fallback (section 6.6).
	if len(parkRows) > 0 {
		if err := g.store.Park(ctx, parkRows); err != nil {
			g.logParkFailure(err)
			// Nothing is durable for these records: no inbox row and no
			// status. The background retry keeps trying until the store takes
			// them; until then their offsets stay uncommitted, so a crash
			// replays them instead of losing them.
			g.queueParkRetry(ctx, pending, parkRows, "no_connection")
		} else {
			g.addParked(parkRows)
			for _, it := range pending {
				statuses = append(statuses, statusFor(it.msg, status.Pending, "no_connection", ""))
				keep(it.seq) // the inbox row is the durability
			}
		}
	}

	// Then hand the live deliveries to the gateway node that owns the
	// connection. The records keep the batch order, and the topic is keyed by
	// subscription ID, so per-subscription order survives the hop. The whole
	// poll batch goes out in one produce request: a synchronous round trip per
	// record would cap the fan-out at the broker's round-trip latency.
	var (
		fallbackRows    []store.InboxRow
		fallbackPending []deliveryItem
	)
	errs := g.produceLiveBatch(ctx, liveItems, liveTargets)
	for i, it := range liveItems {
		if err := errs[i]; err != nil {
			g.log.Warn("sse-delivery: writing to sse-live failed, parking instead",
				"err", err, "subscription", it.msg.SubscriptionID)
			if row, perr := parkRow(it, "no_connection"); perr == nil {
				fallbackRows = append(fallbackRows, row)
				fallbackPending = append(fallbackPending, it)
				continue
			} else {
				// The payload cannot be encoded for the inbox either, so it
				// never will be - committing this record would be silent
				// loss. Terminal drop with a status, exactly what the
				// dispatcher does with the same failure.
				g.log.Error("sse-delivery: encoding parked payload", "err", perr,
					"subscription", it.msg.SubscriptionID)
				statuses = append(statuses, statusFor(it.msg, status.Dropped, reasonParkFailed, ""))
				byStatus(it.seq)
				continue
			}
		}
		// Written, not acknowledged: only an ack turns this into delivered
		// (sections 6.5 and 9).
		statuses = append(statuses, statusFor(it.msg, status.Sent, "", liveTargets[i].ConnectionID))
		keep(it.seq) // the record on sse-live is the durability
	}
	if len(fallbackRows) > 0 {
		if err := g.store.Park(ctx, fallbackRows); err != nil {
			g.logParkFailure(err)
			g.queueParkRetry(ctx, fallbackPending, fallbackRows, "no_connection")
		} else {
			g.addParked(fallbackRows)
			for _, it := range fallbackPending {
				statuses = append(statuses, statusFor(it.msg, status.Pending, "no_connection", ""))
				keep(it.seq)
			}
		}
	}

	if len(statuses) > 0 {
		if err := g.status.Produce(ctx, statuses...); err != nil {
			g.log.Warn("sse-delivery: emitting status", "err", err)
			// The status records of this batch are gone until the replay
			// after the restart re-emits them. Records with a durable outcome
			// may move anyway (an inbox row or an sse-live record is the
			// delivery); the ones whose status was their only trace may not -
			// they stay uncommitted and are repeated rather than vanishing.
			g.release(durable)
			return
		}
	}
	g.release(durable)
	g.release(statusOnly)
}

// parkJob is a delivery whose park failed: the store did not take the row, so
// the record's offset has to stay uncommitted and this job owns the delivery
// until the row is in the inbox (spec 6.6). Retrying instead of dropping is
// what keeps the partition's commit point from standing still for the
// lifetime of the process.
type parkJob struct {
	it     deliveryItem
	row    store.InboxRow
	reason string
	// stored marks a row that reached the inbox, so a later pass does not
	// credit the cached backlog counter for it a second time.
	stored bool
}

// queueParkRetry hands the deliveries whose park failed to the background
// worker. The send blocks when the channel is full, which is deliberate: the
// alternative is committing an offset for a record nobody holds any more.
// Shutdown escapes the block, and then the offsets simply stay uncommitted.
func (g *Gateway) queueParkRetry(ctx context.Context, items []deliveryItem, rows []store.InboxRow, reason string) {
	for i, row := range rows {
		select {
		case g.parkRetry <- parkJob{it: items[i], row: row, reason: reason}:
		case <-ctx.Done():
			return
		}
	}
}

// runParkRetry keeps trying the parks that failed. A store outage must not
// cost data and must not cost the process its memory either: the channel that
// feeds this loop and the jobs it holds itself are both bounded, so an
// unreachable PostgreSQL turns into backpressure on the poll loop, where the
// offsets are already staying put and Kafka keeps the records. A job leaves
// the loop only when its outcome is durable - the row is in the inbox and the
// status went out, or the delivery expired and the status for that did.
func (g *Gateway) runParkRetry(ctx context.Context) {
	tick := time.NewTicker(parkRetryTick)
	defer tick.Stop()
	var pending []parkJob
	var warned bool
	for {
		if len(pending) < parkRetryMax {
			select {
			case <-ctx.Done():
				return
			case j := <-g.parkRetry:
				pending = append(pending, j)
				continue
			case <-tick.C:
			}
		} else {
			// Full: stop accepting and let the channel block the callers.
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
		}
		if ctx.Err() != nil {
			return // the rows are not durable; their offsets stay uncommitted
		}
		kept, left := pending[:0], 0
		progressed := false
		for _, j := range pending {
			if !j.row.ExpiresAt.After(time.Now()) {
				// Section 8: after expires_at a delivery is expired - and
				// store.Park skips a row whose expiry has passed without
				// saying so, which would credit a durability that does not
				// exist. The status is the only trace of the drop, so the
				// offset may move only once it is out.
				if err := g.status.Produce(ctx, statusFor(j.it.msg, status.Expired, "", "")); err != nil {
					g.log.Warn("sse-delivery: emitting status", "err", err)
					kept = append(kept, j)
					left++
					continue
				}
				g.settle(j.it.seq.Partition, j.it.seq.Offset)
				progressed = true
				continue
			}
			if err := g.store.Park(ctx, []store.InboxRow{j.row}); err != nil {
				g.logParkFailure(err)
				kept = append(kept, j)
				left++
				continue
			}
			if !j.stored {
				g.addParked([]store.InboxRow{j.row})
				j.stored = true
			}
			if err := g.status.Produce(ctx, statusFor(j.it.msg, status.Pending, j.reason, "")); err != nil {
				// The row is durable but the status is not out: keep the job
				// and the offset, the row is re-parked harmlessly (ON
				// CONFLICT DO NOTHING) on the next pass.
				g.log.Warn("sse-delivery: emitting status", "err", err)
				kept = append(kept, j)
				left++
				continue
			}
			g.settle(j.it.seq.Partition, j.it.seq.Offset)
			progressed = true
		}
		pending = kept
		if progressed {
			g.commitHeads(ctx)
		}
		switch {
		case left > 0 && !warned:
			g.log.Warn("sse-delivery: parking is failing, retries queued",
				"waiting", left, "retry", parkRetryTick.String())
			warned = true
		case left == 0 && warned:
			g.log.Info("sse-delivery: parking recovered, retries drained")
			warned = false
		}
	}
}

// logParkFailure reports a failed park without drowning the log: parking runs
// at delivery speed and again every retry tick, so an unreachable store would
// otherwise put one line per delivery into it. One line per distinct error
// and never more often than parkLogEvery - the pattern the drainer's lease
// check uses for the same reason.
func (g *Gateway) logParkFailure(err error) {
	g.parkLogMu.Lock()
	defer g.parkLogMu.Unlock()
	text := err.Error()
	if text == g.parkLogErr && time.Since(g.parkLogAt) < parkLogEvery {
		return
	}
	g.parkLogErr, g.parkLogAt = text, time.Now()
	g.log.Warn("sse-delivery: parking failed", "err", err)
}

// runInboxExpiry deletes parked rows whose expiry has passed, every five
// minutes (sections 6.6 and 12). In the full platform whole partitions are
// dropped at expiry; the spike uses the batched delete on top.
func (g *Gateway) runInboxExpiry(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := g.store.DeleteExpiredParked(ctx)
			if err != nil {
				g.log.Warn("deleting expired parked rows", "err", err)
				continue
			}
			if n > 0 {
				g.log.Info("deleted expired parked rows", "rows", n)
			}
		}
	}
}

// runParkedReconcile refreshes the cached inbox counts behind backlog_max
// (spec 16) from PostgreSQL. The delivery stage pays no count per batch
// because of them, so something has to keep them honest: this loop. It is the
// only writer of base besides the one-off seed of a fresh counter, which is
// what makes a restart, a pruned state or a row parked by another node cost a
// bounded error instead of an unbounded one.
func (g *Gateway) runParkedReconcile(ctx context.Context) {
	ticker := time.NewTicker(parkedReconcile)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.reconcileParked(ctx)
		}
	}
}

func (g *Gateway) reconcileParked(ctx context.Context) {
	g.parkedMu.Lock()
	ids := make([]string, 0, len(g.parked))
	for id := range g.parked {
		ids = append(ids, id)
	}
	g.parkedMu.Unlock()
	if len(ids) == 0 {
		return
	}
	counts, err := g.store.CountParked(ctx, ids)
	if err != nil {
		g.log.Warn("sse-delivery: reconciling parked counts", "err", err)
		return
	}
	g.seedParked(ids, counts)
}

// seedParked applies one count(*) to the counters it belongs to. A missing
// key means zero rows: they were drained, expired or never existed. Counters
// are not removed when they reach zero - the map is proportional to the
// subscriptions that ever parked, and counting an empty inbox is free.
func (g *Gateway) seedParked(ids []string, counts map[string]int64) {
	g.parkedMu.Lock()
	defer g.parkedMu.Unlock()
	for _, id := range ids {
		if c := g.parked[id]; c != nil {
			c.Seed(counts[id])
		}
	}
}

// parkedCounter returns the cached count of a subscription, creating it if
// needed; a new one seeds itself on first use.
func (g *Gateway) parkedCounter(subID string) *store.ParkCounter {
	g.parkedMu.Lock()
	defer g.parkedMu.Unlock()
	return g.counterLocked(subID)
}

func (g *Gateway) counterLocked(subID string) *store.ParkCounter {
	c := g.parked[subID]
	if c == nil {
		c = &store.ParkCounter{}
		g.parked[subID] = c
	}
	return c
}

// addParked credits rows that reached the inbox to the cached counts of their
// subscriptions. It runs only after a successful batch insert: rows that were
// never written must not be counted.
func (g *Gateway) addParked(rows []store.InboxRow) {
	g.parkedMu.Lock()
	defer g.parkedMu.Unlock()
	for _, r := range rows {
		g.counterLocked(r.SubscriptionID).Add(1)
	}
}

// ackParked lowers the cached counts of rows that left the inbox. A
// subscription without a counter is left alone: it has nothing cached, and
// the next park seeds it from a fresh count anyway.
func (g *Gateway) ackParked(rows []store.InboxRow) {
	g.parkedMu.Lock()
	defer g.parkedMu.Unlock()
	for _, r := range rows {
		if c := g.parked[r.SubscriptionID]; c != nil {
			c.Add(-1)
		}
	}
}

// runDrainer moves parked deliveries back onto the live path as soon as their
// subscription has a connection again (section 6.6). The spike runs it on the
// node with SSE_DRAINER=true; a production deployment would co-locate it with
// the partition owner of the subscription.
func (g *Gateway) runDrainer(ctx context.Context) {
	ticker := time.NewTicker(drainTick)
	defer ticker.Stop()
	carry := map[string]float64{} // fractional drain budget per subscription
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.drainOnce(ctx, carry)
		}
	}
}

func (g *Gateway) drainOnce(ctx context.Context, carry map[string]float64) {
	subs := g.view.subscriptions() // subscriptions with a live connection
	if len(subs) == 0 {
		return
	}
	var (
		budget   = map[string]int{}
		eligible []string
		total    int
	)
	for _, subID := range subs {
		// drain_rate_per_s (section 7) per 500ms tick, with the remainder
		// carried over so that small rates still make progress.
		b := float64(g.drainRate(subID))*drainTick.Seconds() + carry[subID]
		n := int(b)
		carry[subID] = b - float64(n)
		if n <= 0 {
			continue
		}
		budget[subID] = n
		total += n
		eligible = append(eligible, subID)
	}
	if total == 0 {
		return
	}
	// FetchParked returns rows in (part, offset) order, which is the delivery
	// order of the subscription.
	rows, err := g.store.FetchParked(ctx, eligible, total)
	if err != nil {
		g.log.Warn("drainer: fetching parked rows", "err", err)
		return
	}
	bySub := map[string][]store.InboxRow{}
	for _, r := range rows {
		bySub[r.SubscriptionID] = append(bySub[r.SubscriptionID], r)
	}
	for subID, rs := range bySub {
		if len(rs) > budget[subID] {
			rs = rs[:budget[subID]]
		}
		target, ok := g.view.pick(subID, g.opts.NodeID)
		if !ok {
			continue // the connection vanished again; the rows stay parked
		}
		var (
			items    []deliveryItem
			rows     []store.InboxRow
			acked    []store.InboxRow
			statuses []status.Record
		)
		for _, r := range rs {
			msg, err := delivery.Decode(r.Payload)
			if err != nil {
				// An undecodable row would block the head of the queue
				// forever; log it and delete it to make progress.
				g.log.Error("drainer: decoding parked payload", "err", err,
					"subscription", subID, "part", r.Part, "offset", r.Offset)
				acked = append(acked, r)
				continue
			}
			items = append(items, deliveryItem{seq: delivery.Seq{Partition: r.Part, Offset: r.Offset}, msg: msg})
			rows = append(rows, r)
		}
		targets := make([]presenceRecord, len(items))
		for i := range targets {
			targets[i] = target
		}
		errs := g.produceLiveBatch(ctx, items, targets)
		for i, err := range errs {
			if err != nil {
				g.log.Warn("drainer: writing to sse-live, retrying next tick", "err", err, "subscription", subID)
				break // rows stay in the inbox; order holds because we stop
			}
			statuses = append(statuses, statusFor(items[i].msg, status.Sent, "", target.ConnectionID))
			acked = append(acked, rows[i])
		}
		// The inbox row is deleted only after it has been emitted; the ack
		// endpoint never deletes anything (section 6.6).
		if len(acked) > 0 {
			if err := g.store.AckParked(ctx, acked); err != nil {
				g.log.Warn("drainer: deleting parked rows", "err", err, "subscription", subID)
			} else {
				// Lower the cached counts so the subscription can park again
				// without waiting for the next reconciliation.
				g.ackParked(acked)
			}
		}
		if len(statuses) > 0 {
			if err := g.status.Produce(ctx, statuses...); err != nil {
				g.log.Warn("drainer: emitting status", "err", err, "subscription", subID)
			}
		}
	}
}

// liveMessage is the value of the sse-live topic: a delivery targeted at one
// connection on one node. Sequence is the position on the deliveries topic; it
// becomes the SSE id and therefore the ack handle.
type liveMessage struct {
	TargetNode         string            `json:"target_node"`
	TargetConnectionID string            `json:"target_connection_id"`
	Sequence           delivery.Seq      `json:"sequence"`
	Message            *delivery.Message `json:"message"`
}

// produceLiveBatch writes a batch of deliveries to the sse-live topic, keyed
// by subscription ID so that per-subscription order is preserved (section
// 6.5). franz-go serialises records of one batch onto their partition in the
// given order, so the hop keeps the order of the poll batch. The returned
// slice lines up with the arguments; a non-nil entry is that record's failure.
func (g *Gateway) produceLiveBatch(ctx context.Context, items []deliveryItem, targets []presenceRecord) []error {
	if len(items) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(items))
	for i := range items {
		it, target := &items[i], &targets[i]
		b, err := json.Marshal(liveMessage{
			TargetNode:         target.NodeID,
			TargetConnectionID: target.ConnectionID,
			Sequence:           it.seq,
			Message:            it.msg,
		})
		if err != nil {
			// Only a marshal failure of a live record can land here, and it
			// would fail again on retry; park the delivery instead.
			g.log.Error("sse-live: encoding record", "err", err, "subscription", it.msg.SubscriptionID)
			recs = append(recs, nil)
			continue
		}
		recs = append(recs, &kgo.Record{
			Topic: kafkautil.TopicSSELive,
			Key:   []byte(it.msg.SubscriptionID),
			Value: b,
		})
	}
	errs := make([]error, len(items))
	send := make([]*kgo.Record, 0, len(recs))
	idx := make([]int, 0, len(recs))
	for i, rec := range recs {
		if rec == nil {
			errs[i] = errEncode
			continue
		}
		send = append(send, rec)
		idx = append(idx, i)
	}
	if len(send) == 0 {
		return errs
	}
	results := g.livePub.ProduceSync(ctx, send...)
	for j, res := range results {
		errs[idx[j]] = res.Err
	}
	return errs
}

// errEncode marks a record that could not be encoded for the live hop.
var errEncode = errors.New("sse-live: encoding failed")

// parkRow encodes one delivery for the inbox (sections 3 and 6.6).
func parkRow(it deliveryItem, reason string) (store.InboxRow, error) {
	payload, err := it.msg.Encode()
	if err != nil {
		return store.InboxRow{}, err
	}
	return store.InboxRow{
		SubscriptionID: it.msg.SubscriptionID,
		Part:           it.seq.Partition,
		Offset:         it.seq.Offset,
		Payload:        payload,
		Reason:         reason,
		ExpiresAt:      it.msg.ExpiresAt,
	}, nil
}

// statusFor builds the status record of one delivery (section 9). It carries
// IDs and metadata only, never a payload. TS is stamped here rather than at
// produce time for the same reason as the dispatcher's emitStatus: records
// produced in one batch must not all carry the flush's millisecond (see
// internal/chquery for how same-millisecond records are ordered).
func statusFor(msg *delivery.Message, st, reason, connectionID string) status.Record {
	return status.Record{
		TS:             time.Now().UTC(),
		Owner:          msg.Owner,
		EventRef:       msg.EventRef,
		DeliveryID:     msg.DeliveryID,
		SubscriptionID: msg.SubscriptionID,
		Status:         st,
		Reason:         reason,
		ConnectionID:   connectionID,
		ReplayID:       msg.ReplayID,
		RetentionClass: msg.RetentionClass,
		SrcPartition:   msg.SrcPartition,
		SrcOffset:      msg.SrcOffset,
	}
}

// sleep waits for d or until ctx is done; it reports whether it waited the
// full duration.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
