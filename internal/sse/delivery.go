package sse

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

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
		g.processDeliveries(ctx, recs)
		// Offsets are committed only after the whole batch has been handled, so
		// a crash repeats a batch instead of losing one; the duplicates are
		// identified by the event reference (section 8).
		if len(recs) > 0 {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := g.deliveryCl.CommitRecords(cctx, recs...); err != nil {
				g.log.Warn("sse-delivery: committing offsets", "err", err)
			}
			cancel()
		}
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
	for _, rec := range recs {
		msg, err := delivery.Decode(rec.Value)
		if err != nil {
			g.log.Error("sse-delivery: decoding record", "err", err,
				"partition", rec.Partition, "offset", rec.Offset)
			continue
		}
		if !g.ownsChannel(msg) {
			continue // webhook deliveries belong to the dispatcher
		}
		it := deliveryItem{seq: delivery.Seq{Partition: rec.Partition, Offset: rec.Offset}, msg: msg}
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
			case !it.msg.ExpiresAt.After(time.Now()):
				// Section 8: after expires_at a delivery is expired.
				statuses = append(statuses, statusFor(it.msg, status.Expired, "", ""))
			case backlogCap > 0 && parked >= backlogCap:
				// Section 9: dropped because backlog_max was exceeded.
				statuses = append(statuses, statusFor(it.msg, status.Dropped, "backlog_max", ""))
			default:
				row, err := parkRow(it, "no_connection")
				if err != nil {
					g.log.Error("sse-delivery: encoding parked payload", "err", err, "subscription", subID)
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
			g.log.Error("sse-delivery: parking rows", "err", err)
			pending = nil // offsets are committed after this batch; production would retry
		} else {
			g.addParked(parkRows)
			for _, it := range pending {
				statuses = append(statuses, statusFor(it.msg, status.Pending, "no_connection", ""))
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
			}
			continue
		}
		// Written, not acknowledged: only an ack turns this into delivered
		// (sections 6.5 and 9).
		statuses = append(statuses, statusFor(it.msg, status.Sent, "", liveTargets[i].ConnectionID))
	}
	if len(fallbackRows) > 0 {
		if err := g.store.Park(ctx, fallbackRows); err != nil {
			g.log.Error("sse-delivery: parking fallback rows", "err", err)
		} else {
			g.addParked(fallbackRows)
			for _, it := range fallbackPending {
				statuses = append(statuses, statusFor(it.msg, status.Pending, "no_connection", ""))
			}
		}
	}

	if len(statuses) > 0 {
		if err := g.status.Produce(ctx, statuses...); err != nil {
			g.log.Warn("sse-delivery: emitting status", "err", err)
		}
	}
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
