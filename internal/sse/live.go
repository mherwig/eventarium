package sse

import (
	"context"
	"encoding/json"
	"time"
)

// runLive consumes the sse-live topic. The consumer group is unique per node
// ("sse-live-<NODE_ID>") and starts at the latest offset, because every node
// must see every message and drop the ones that are not its own (section 6.5).
func (g *Gateway) runLive(ctx context.Context) {
	// Time the first half of the boundary nothing outside could see: Kafka's
	// record timestamp (produce time) to the moment this consumer hands the
	// record to the connection queue. The second half - queue to socket
	// write - is timed in the connection's own goroutine, because they run
	// concurrently and cannot share an accumulator.
	var kafkaAge time.Duration
	var n int
	for {
		fs := g.liveCl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				g.log.Warn("sse-live: poll", "err", e)
			}
			if !sleep(ctx, time.Second) {
				return
			}
		}
		recs := fs.Records()
		for _, rec := range recs {
			var lm liveMessage
			if err := json.Unmarshal(rec.Value, &lm); err != nil {
				g.log.Warn("sse-live: decoding record", "err", err, "partition", rec.Partition, "offset", rec.Offset)
				continue
			}
			if lm.TargetNode != g.opts.NodeID {
				continue // another node holds the connection
			}
			// The connection must still be local: the presence record may have
			// been tombstoned in the meantime (client gone, node rebalancing).
			// Spike limitation: such a message is dropped here; the client
			// recovers through Last-Event-ID, which makes the gateway fetch the
			// missed deliveries from the deliveries topic on reconnect
			// (section 8).
			c := g.localConnection(lm.TargetConnectionID)
			if c == nil || lm.Message == nil || c.subID != lm.Message.SubscriptionID {
				continue
			}
			handed := false
			select {
			case c.out <- outbound{seq: lm.Sequence, msg: lm.Message, enqueued: time.Now()}:
				handed = true
			default:
				// Overflow: close the connection instead of buffering without
				// bound. The client reconnects with Last-Event-ID and the
				// gateway serves the backlog it missed from the deliveries
				// topic, which is the recovery path of section 8.
				c.close("delivery queue overflow, reconnect with Last-Event-ID")
			}
			if handed {
				kafkaAge += time.Since(rec.Timestamp)
				n++
				if n >= 500 {
					g.log.Info("sse-live: kafka record -> connection queue",
						"avg_ms", kafkaAge.Milliseconds()/int64(n), "n", n)
					kafkaAge, n = 0, 0
				}
			}
		}
		// Same commit rule as the delivery stage: a crash repeats a batch
		// instead of losing one, and the duplicates are dropped by the client
		// through the event reference (section 8).
		if len(recs) > 0 {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := g.liveCl.CommitRecords(cctx, recs...); err != nil {
				g.log.Warn("sse-live: committing offsets", "err", err)
			}
			cancel()
		}
	}
}
