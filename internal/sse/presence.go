package sse

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/kafkautil"
)

// presenceRecord is the value of the sse-presence topic (section 6.5). The key
// is "<subscription_id>/<connection_id>"; a tombstone (empty value) makes the
// compacted topic forget the connection, and every local view drops it.
type presenceRecord struct {
	NodeID         string    `json:"node_id"`
	ConnectionID   string    `json:"connection_id"`
	SubscriptionID string    `json:"subscription_id"`
	ConsumerID     string    `json:"consumer_id"`
	ConnectedAt    time.Time `json:"connected_at"`
}

// presenceView is the in-memory mirror of the presence topic. It answers the
// only question the delivery stage needs: which connection, on which node,
// currently owns a subscription (section 6.5).
//
// Spike limitation: a node that dies without publishing tombstones leaves
// stale entries behind until the topic is compacted away; the sse-live consumer
// then drops the targeted messages because the connection is not local.
type presenceView struct {
	mu    sync.RWMutex
	bySub map[string]map[string]presenceRecord // subscription ID -> connection ID -> record
}

func newPresenceView() *presenceView {
	return &presenceView{bySub: map[string]map[string]presenceRecord{}}
}

// apply folds one presence record into the view.
func (v *presenceView) apply(key string, value []byte) {
	subID, connID, ok := presenceKey(key)
	if !ok {
		slog.Warn("presence: unexpected record key", "key", key)
		return
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if len(value) == 0 { // tombstone
		if m := v.bySub[subID]; m != nil {
			delete(m, connID)
			if len(m) == 0 {
				delete(v.bySub, subID)
			}
		}
		return
	}
	var rec presenceRecord
	if err := json.Unmarshal(value, &rec); err != nil {
		slog.Warn("presence: decoding record", "err", err, "key", key)
		return
	}
	if rec.SubscriptionID == "" {
		rec.SubscriptionID = subID
	}
	if rec.ConnectionID == "" {
		rec.ConnectionID = connID
	}
	m := v.bySub[subID]
	if m == nil {
		m = map[string]presenceRecord{}
		v.bySub[subID] = m
	}
	m[connID] = rec
}

// pick chooses the connection that receives the next delivery of a
// subscription. A connection on this node is preferred to save the sse-live
// hop. Section 7's rendezvous hashing is not implemented in the spike: any
// remaining connection will do.
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

// subscriptions lists the subscriptions that currently have a connection.
func (v *presenceView) subscriptions() []string {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make([]string, 0, len(v.bySub))
	for subID := range v.bySub {
		out = append(out, subID)
	}
	return out
}

func presenceKey(key string) (subID, connID string, ok bool) {
	subID, connID, ok = strings.Cut(key, "/")
	return subID, connID, ok && subID != "" && connID != ""
}

// publishPresence writes a connect record or a disconnect tombstone.
func (g *Gateway) publishPresence(ctx context.Context, c *connection, tombstone bool) error {
	var value []byte
	if !tombstone {
		rec := presenceRecord{
			NodeID:         g.opts.NodeID,
			ConnectionID:   c.id,
			SubscriptionID: c.subID,
			ConsumerID:     c.consumerID,
			ConnectedAt:    c.openedAt,
		}
		var err error
		if value, err = json.Marshal(rec); err != nil {
			return err
		}
	}
	res := g.presencePub.ProduceSync(ctx, &kgo.Record{
		Topic: kafkautil.TopicPresence,
		Key:   []byte(c.subID + "/" + c.id),
		Value: value, // nil value: tombstone
	})
	return res.FirstErr()
}

// runPresenceView consumes the presence topic from the beginning (a fresh
// group) so that every node rebuilds the full connection map after a restart.
func (g *Gateway) runPresenceView(ctx context.Context) {
	for {
		fs := g.presenceCl.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				g.log.Warn("presence view: poll", "err", e)
			}
			if !sleep(ctx, time.Second) {
				return
			}
		}
		recs := fs.Records()
		for _, rec := range recs {
			g.view.apply(string(rec.Key), rec.Value)
		}
		if len(recs) > 0 {
			cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			if err := g.presenceCl.CommitRecords(cctx, recs...); err != nil {
				g.log.Warn("presence view: committing offsets", "err", err)
			}
			cancel()
		}
	}
}
