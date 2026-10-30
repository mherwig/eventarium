package sse

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"pubsub/internal/delivery"
	"pubsub/internal/kafkautil"
	"pubsub/internal/status"
	"pubsub/internal/store"
)

// backlogTimeout bounds one backlog phase (section 8). A reconnecting client
// must not wait forever for history: after the timeout the phase gives up,
// keeps what it has already written and the normal event loop takes over, so a
// slow or wedged fetch never pins the connection beyond a bounded delay.
const backlogTimeout = 15 * time.Second

// listOffset asks the broker for one watermark of one deliveries partition.
// Timestamp -2 is the low watermark (oldest record still inside the 24 to 48
// hour retention of section 6.3), -1 the high watermark (the next offset to be
// written). The backlog phase needs both: the high watermark is the end of the
// fetch range, the low watermark detects a cursor that retention has already
// dropped behind (section 12). One timestamp per request: the broker rejects a
// ListOffsets request that names the same partition twice.
func listOffset(ctx context.Context, cl *kgo.Client, part int32, timestamp int64) (int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	topic := kmsg.NewListOffsetsRequestTopic()
	topic.Topic = kafkautil.TopicDeliveries
	p := kmsg.NewListOffsetsRequestTopicPartition()
	p.Partition = part
	p.Timestamp = timestamp
	topic.Partitions = append(topic.Partitions, p)
	req.Topics = append(req.Topics, topic)

	res, err := req.RequestWith(ctx, cl)
	if err != nil {
		return 0, err
	}
	for _, rt := range res.Topics {
		if rt.Topic != kafkautil.TopicDeliveries {
			continue
		}
		for _, rp := range rt.Partitions {
			if rp.Partition != part {
				continue
			}
			if rp.ErrorCode != 0 {
				return 0, fmt.Errorf("sse: list offsets: broker error code %d", rp.ErrorCode)
			}
			return rp.Offset, nil
		}
	}
	return 0, errors.New("sse: list offsets: partition missing from response")
}

// serveBacklog is the reconnect phase of section 8: "after reconnecting, the
// client fetches the backlog while buffering live events". It reads the
// deliveries topic directly from cursor+1 up to the high watermark with a
// throwaway one-shot consumer (no consumer group, no commits, no rebalances)
// and writes every kept record to the connection through the caller's write
// closure, so each event gets its deadline, flush and ack log entry.
//
// It reads Kafka and not the PostgreSQL inbox because the inbox alone cannot
// cover the gap: rows handed to a connection are deleted from the inbox, and
// the sse-live hop drops messages whose connection died in the meantime —
// exactly the events a reconnecting client missed. The deliveries topic still
// holds them for its retention (sections 6.3 and 8), so this phase is a
// cursor-aware one-shot drain: it emits the status records and deletes the
// inbox rows of everything it wrote, which keeps the drainer from sending them
// a second time.
//
// The phase is best-effort: on fetch trouble, cancellation or timeout it
// returns and the normal event loop continues; the bool result reports that a
// write to the client failed, and the caller then ends the handler so the
// client can reconnect with its own cursor and resume where it stopped.
func (g *Gateway) serveBacklog(ctx context.Context, conn *connection, subID string, cursor delivery.Seq, write func(string) error) (writeFailed bool) {
	started := time.Now()

	// Throwaway client, closed when this phase ends. It keeps transaction
	// markers: the router commits through a group transaction session
	// (cmd/router), so the deliveries log alternates a data record with its
	// marker and franz-go drops those markers from fetches by default. The
	// markers still own offsets and still count towards the high watermark, so
	// without them the loop below stops one short of `high` — usually parked on
	// a marker — and would burn the whole backlogTimeout waiting for an offset
	// that exists but is never delivered.
	cl, err := kafkautil.NewClient(kgo.KeepControlRecords())
	if err != nil {
		g.log.Warn("sse backlog: creating fetch client", "err", err, "subscription", subID)
		return false
	}
	defer cl.Close()

	// A cancelled client or a shutting-down gateway must never block the
	// handler: either one cancels the phase, and the event loop below decides
	// what happens next.
	bctx, bcancel := context.WithTimeout(ctx, backlogTimeout)
	defer bcancel()
	go func() {
		select {
		case <-g.shutdown:
			bcancel()
		case <-bctx.Done():
		}
	}()

	low, err := listOffset(bctx, cl, cursor.Partition, -2)
	if err != nil {
		g.log.Warn("sse backlog: reading low watermark", "err", err, "subscription", subID, "cursor", cursor.String())
		return false
	}
	high, err := listOffset(bctx, cl, cursor.Partition, -1)
	if err != nil {
		g.log.Warn("sse backlog: reading high watermark", "err", err, "subscription", subID, "cursor", cursor.String())
		return false
	}

	start := cursor.Offset + 1
	if start < low {
		// Retention moved past the cursor: the events between low and the old
		// cursor are unrecoverable and the client has to be told (sections 6.3
		// and 12 keep the deliveries topic for 24 to 48 hours only).
		earliest := delivery.Seq{Partition: cursor.Partition, Offset: low}
		g.log.Warn("sse backlog: cursor below retention, gap is unrecoverable",
			"subscription", subID, "cursor", cursor.String(), "earliest", earliest.String())
		if err := write(fmt.Sprintf(": backlog gap: earliest available is %s\n\n", earliest)); err != nil {
			return true
		}
		start = low
	}
	if start >= high {
		// Nothing was missed: the cursor already sits at (or past) the tip.
		g.log.Info("sse backlog served",
			"subscription", subID, "cursor", cursor.String(), "sent", 0, "elapsed", time.Since(started))
		return false
	}

	// Direct partition assignment: this client owns nothing, joins no group and
	// commits nothing, it only replays one partition from one offset (6.3).
	cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{
		kafkautil.TopicDeliveries: {cursor.Partition: kgo.NewOffset().At(start)},
	})

	var (
		written    []deliveryItem // kept events, in offset order, actually on the wire
		expired    []status.Record
		next       = start // next offset the fetch has to deliver
		capReached bool
	)
	limit := g.backlogMax(subID) // backlog_max, 0 = uncapped (section 7)
fetch:
	// Stop exactly at the high watermark read above: `next` runs up to `high`,
	// so the loop only ends once every offset below `high` has been handed
	// over, markers included (see the client above). An unreachable `high`
	// would otherwise stall the phase until backlogTimeout instead of ending
	// it the moment the tip of the log is reached (section 8).
	for next < high {
		fs := cl.PollFetches(bctx)
		if bctx.Err() != nil {
			break // cancelled, timed out or shutting down: keep what we have
		}
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				g.log.Warn("sse backlog: poll", "err", e.Err, "topic", e.Topic, "partition", e.Partition)
			}
			break
		}
		for _, rec := range fs.Records() {
			if rec.Offset >= next {
				next = rec.Offset + 1
			}
			if rec.Attrs.IsControl() {
				// A COMMIT/ABORT marker: it has no delivery payload, it only
				// closes a transaction. It still advances `next`, which is the
				// whole reason the client above keeps it (see there).
				continue
			}
			msg, err := delivery.Decode(rec.Value)
			if err != nil {
				g.log.Error("sse backlog: decoding record", "err", err,
					"partition", rec.Partition, "offset", rec.Offset)
				continue
			}
			if msg.SubscriptionID != subID {
				// One subscription sits on one partition, but other
				// subscriptions can share it through a hash collision (6.3).
				continue
			}
			// Webhook deliveries share the topic: the subscription's current
			// channel decides, so a record routed before a channel change is
			// still served here afterwards (ownsChannel).
			if !g.ownsChannel(msg) {
				continue
			}
			if msg.RetentionClass == kafkautil.ClassLive {
				// Section 12: live-class events are hot-path only and carry no
				// redelivery duty, so the backlog never re-sends them.
				continue
			}
			if !msg.ExpiresAt.After(time.Now()) {
				// Section 8: after expires_at a delivery is expired, and an
				// expired delivery is answered with a status, not with data.
				expired = append(expired, statusFor(msg, status.Expired, "", ""))
				continue
			}
			if limit > 0 && int64(len(written)) >= limit {
				capReached = true
				break fetch // backlog_max reached (section 7); the rest stays parked
			}
			o := outbound{seq: delivery.Seq{Partition: rec.Partition, Offset: rec.Offset}, msg: msg}
			frame, err := o.frame()
			if err != nil {
				g.log.Error("sse backlog: rendering event", "err", err, "connection", conn.id)
				continue
			}
			if err := write(frame); err != nil {
				g.log.Info("sse backlog: write failed", "connection", conn.id,
					"subscription", subID, "err", err, "sent", len(written))
				writeFailed = true
				break fetch
			}
			// On the wire: remember it for acks and mark it served so the event
			// loop drops the same delivery if the drainer pushes the row that
			// is still parked (section 8).
			conn.appendSent(o)
			conn.markServed(o.seq)
			written = append(written, deliveryItem{seq: o.seq, msg: msg})
		}
	}

	// Bookkeeping for everything that really reached the wire — also when the
	// phase bailed out early. It runs on a background context because the
	// client may already be gone and its request context cancelled, while the
	// records were written and must not be served a second time later.
	if len(written) > 0 || len(expired) > 0 {
		actx, acancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer acancel()
		recs := make([]status.Record, 0, len(written)+len(expired))
		keys := make([]store.InboxRow, 0, len(written))
		for _, it := range written {
			// A delivery served from Kafka may already carry a "sent" record
			// from the delivery stage or the drainer when it raced us here;
			// status history is append-only audit data (section 9), so the
			// second "sent" is acceptable.
			recs = append(recs, statusFor(it.msg, status.Sent, "", conn.id))
			// Delete the inbox row if it is still parked: the backlog phase is
			// the drain for these offsets now. Deleting a row that was never
			// parked is a no-op (section 6.6: only the drainer deletes rows,
			// and this phase takes its place for what it wrote).
			keys = append(keys, store.InboxRow{
				SubscriptionID: it.msg.SubscriptionID,
				Part:           it.seq.Partition,
				Offset:         it.seq.Offset,
			})
		}
		recs = append(recs, expired...)
		if err := g.status.Produce(actx, recs...); err != nil {
			g.log.Warn("sse backlog: emitting status", "err", err, "subscription", subID)
		}
		// The cached backlog counters (parked) are deliberately not lowered
		// here: most of these keys were never parked rows - this phase deletes
		// them best-effort - so subtracting them would undercount the inbox by
		// the whole backlog. The reconciliation corrects the count instead,
		// and while this subscription has a connection nothing parks anyway.
		if err := g.store.AckParked(actx, keys); err != nil {
			g.log.Warn("sse backlog: deleting parked rows", "err", err, "subscription", subID)
		}
	}

	g.log.Info("sse backlog served",
		"subscription", subID, "cursor", cursor.String(), "sent", len(written), "elapsed", time.Since(started))
	if capReached {
		g.log.Info("sse backlog stopped at backlog_max",
			"subscription", subID, "limit", limit, "sent", len(written))
	}
	return writeFailed
}
