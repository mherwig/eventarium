package replay

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/cloudevents"
	"pubsub/internal/delivery"
	"pubsub/internal/kafkautil"
	"pubsub/internal/project"
	"pubsub/internal/registry"
	"pubsub/internal/routing"
	"pubsub/internal/status"
)

// buildReplay turns one original event record into the pair the router would
// have written (internal/routing): the delivery record on the deliveries
// topic, keyed by subscription ID (section 6.3), plus its pending status
// record (section 9). What differs from the live path is exactly what
// section 11 prescribes:
//
//   - the delivery keeps the original delivery ID (delivery.NewID: a replay
//     writes a new record for the same delivery and carries the replay ID
//     separately) and is flagged Replay/ReplayID; the dispatcher turns that
//     into the pubsub-replay-id header and copies it into every status record;
//   - only the subscription's *current projection* is applied. Filters are
//     never re-evaluated: section 287 limits a replay to existing deliveries,
//     it does not re-decide who matches;
//   - CreatedAt is now while ExpiresAt comes verbatim from the event record's
//     expires header — retention was stamped at publish (section 12) and is
//     never recomputed here (section 282);
//   - the ordering key resolves exactly like the router's, so a key_ordered
//     subscription cannot tell a replay from the original (section 283: a
//     replayed delivery uses the normal delivery path).
//
// An error means the delivery can never be built; the caller counts the row
// as not_replayable, which keeps matched = enqueued + not_replayable true for
// every job.
func buildReplay(reg *registry.Registry, job *Job, sub *registry.Subscription, m Match, rec *kgo.Record, now time.Time) ([]*kgo.Record, error) {
	e, err := cloudevents.Parse(rec.Value)
	if err != nil {
		return nil, fmt.Errorf("event record does not parse: %w", err)
	}
	owner := kafkautil.HGet(rec, kafkautil.HeaderOwner)
	if owner == "" {
		owner = job.Owner // the selector was owner scoped, so this is the same owner
	}
	class := kafkautil.HGet(rec, kafkautil.HeaderClass)
	if class == "" {
		class = m.RetentionClass
	}
	eventRef := kafkautil.HGet(rec, kafkautil.HeaderEventRef)
	if eventRef == "" {
		eventRef = m.EventRef
	}
	expires := headerTime(rec, kafkautil.HeaderExpires)

	// The ordering key is resolved from the event before the projection,
	// because the projection may remove the field it reads (section 6.2).
	var (
		dataMap map[string]any
		decoded bool
	)
	decode := func() map[string]any {
		if !decoded {
			decoded = true
			if obj, err := e.JSON(); err == nil {
				dataMap = obj
			}
		}
		return dataMap
	}
	orderingKey := routing.OrderingKey(reg, rec, e, owner, decode)

	ev := e
	if len(sub.Projection) > 0 {
		projected, err := project.Apply(e.Data, sub.Projection)
		if err != nil {
			return nil, fmt.Errorf("applying the current projection: %w", err)
		}
		cp := *e
		cp.Data = projected
		cp.DataBase64 = ""
		ev = &cp
	}

	msg := &delivery.Message{
		DeliveryID:     delivery.NewID(eventRef, sub.ID),
		EventRef:       eventRef,
		SubscriptionID: sub.ID,
		Owner:          owner,
		EventType:      e.Type,
		Channel:        sub.Channel,
		Replay:         true,
		ReplayID:       job.ID,
		RetentionClass: class,
		OrderingKey:    orderingKey,
		SrcPartition:   rec.Partition,
		SrcOffset:      rec.Offset,
		CreatedAt:      now,
		ExpiresAt:      expires,
		Projected:      len(sub.Projection) > 0,
		Event:          ev,
	}
	value, err := msg.Encode()
	if err != nil {
		return nil, fmt.Errorf("encoding the delivery: %w", err)
	}
	out := []*kgo.Record{
		{
			// Section 6.3: keyed by subscription ID, so the whole replay
			// lands on one partition of the deliveries topic and the
			// ingestion order of section 284 survives the produce.
			Topic: kafkautil.TopicDeliveries,
			Key:   []byte(sub.ID),
			Value: value,
			// Same channel header as the router: downstream stages route
			// on the delivery mode without decoding the value.
			Headers: []kgo.RecordHeader{
				kgo.RecordHeader(kafkautil.HSet(routing.HeaderChannel, sub.Channel)),
			},
		},
	}

	// Section 9: every created delivery gets exactly one pending status,
	// written in the same transaction as the delivery. Reason "replay" and
	// the replay ID mark it as belonging to this job (idempotency key:
	// delivery ID, replay ID, attempt, status).
	srec := status.Record{
		TS:             now,
		Owner:          owner,
		EventRef:       eventRef,
		DeliveryID:     msg.DeliveryID,
		SubscriptionID: sub.ID,
		Status:         status.Pending,
		Reason:         "replay",
		Attempt:        0,
		ReplayID:       job.ID,
		RetentionClass: class,
		SrcPartition:   rec.Partition,
		SrcOffset:      rec.Offset,
	}
	svalue, err := json.Marshal(srec)
	if err != nil {
		return nil, fmt.Errorf("encoding the pending status: %w", err)
	}
	out = append(out, &kgo.Record{
		Topic: kafkautil.TopicStatus,
		Key:   []byte(msg.DeliveryID),
		Value: svalue,
	})
	return out, nil
}
