// Package status defines the delivery status records of section 9. Every stage
// emits them asynchronously to the delivery-status topic; they never carry
// payloads.
package status

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/kafkautil"
)

// Status values (section 9 of the spec).
const (
	Pending      = "pending"
	InFlight     = "in_flight"
	Retrying     = "retrying"
	Delivered    = "delivered"
	Sent         = "sent" // SSE: written to the connection but not acknowledged
	Failed       = "failed"
	DeadLettered = "dead_lettered"
	Paused       = "paused"
	Expired      = "expired"
	Dropped      = "dropped"

	// Filtered is the router's verdict for a subscription that was a
	// candidate for the event's type but rejected the event with its filter
	// (section 6.2). No delivery exists for it, so the record carries the
	// delivery ID that delivery *would* have had - the row is listed and
	// counted like any other, and it is terminal from the start.
	//
	// It is an extension: section 9 describes the statuses of a delivery,
	// and a filtered event never becomes one. It exists because the
	// alternative is silence - a filter that rejected everything left no
	// trace anywhere, which made "why did this never arrive" unanswerable
	// from the platform's own data.
	Filtered = "filtered"
)

// Record is one status transition of one delivery. The idempotency key is
// delivery ID, replay ID (if any), attempt and status.
type Record struct {
	TS             time.Time `json:"ts"`
	Owner          string    `json:"owner"`
	EventRef       string    `json:"event_ref"`
	DeliveryID     string    `json:"delivery_id"`
	SubscriptionID string    `json:"subscription_id"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	Attempt        uint16    `json:"attempt"`
	HTTPCode       uint16    `json:"http_code,omitempty"`
	LatencyMS      uint32    `json:"latency_ms,omitempty"`
	ConnectionID   string    `json:"connection_id,omitempty"`
	ReplayID       string    `json:"replay_id,omitempty"`
	RetentionClass string    `json:"retention_class"`
	SrcPartition   int32     `json:"src_partition"`
	SrcOffset      int64     `json:"src_offset"`
}

// Producer wraps a Kafka client used only for emitting status records.
type Producer struct {
	cl *kgo.Client
}

// NewProducer connects a status producer.
func NewProducer(brokers []string) (*Producer, error) {
	opts := []kgo.Opt{kgo.DefaultProduceTopic(kafkautil.TopicStatus), kgo.RequiredAcks(kgo.AllISRAcks())}
	if len(brokers) > 0 {
		opts = append(opts, kgo.SeedBrokers(brokers...))
	}
	cl, err := kafkautil.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	return &Producer{cl: cl}, nil
}

// Produce emits the records; it blocks until they are acknowledged.
func (p *Producer) Produce(ctx context.Context, recs ...Record) error {
	if len(recs) == 0 {
		return nil
	}
	rs := make([]*kgo.Record, 0, len(recs))
	for i := range recs {
		rec := recs[i]
		if rec.TS.IsZero() {
			rec.TS = time.Now().UTC()
		}
		b, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("status: encoding record: %w", err)
		}
		rs = append(rs, &kgo.Record{Value: b, Key: []byte(rec.DeliveryID)})
	}
	results := p.cl.ProduceSync(ctx, rs...)
	return firstError(results)
}

// Close flushes and closes the producer.
func (p *Producer) Close() { p.cl.Close() }

func firstError(rs kgo.ProduceResults) error {
	var err error
	for _, r := range rs {
		if r.Err != nil && err == nil {
			err = r.Err
		}
	}
	return err
}
