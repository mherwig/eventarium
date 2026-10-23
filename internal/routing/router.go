// Package routing implements the router pipeline of the spike (spec sections
// 5, 6.2, 6.3 and 9). The router consumes the four per-class event log topics
// and fans every event out to the subscriptions that match it:
//
//   - parse the CloudEvent from the record value and read owner, retention
//     class, event reference and the ingestion/expiry timestamps from the
//     Kafka record headers (section 5: platform metadata travels in headers,
//     never inside the event);
//   - look up the subscriptions of (owner, type) in the in-memory registry,
//     which is refreshed from PostgreSQL (spike deviation of section 6.1);
//   - evaluate the precompiled CEL attribute and data filters, with the
//     equality index of section 6.2 applied first (internal/filter);
//   - compute the ordering key of the event type *before* any projection,
//     because the projection may remove the field the key is derived from
//     (section 6.2);
//   - apply the per-subscription projection allowlist to a copy of the event;
//   - build one delivery.Message per matching subscription, keyed by
//     subscription ID so that a subscription always lands on the same
//     partition of the fixed 12-partition deliveries topic (section 6.3),
//     plus one pending status.Record for it (section 9).
//
// The caller (cmd/router) writes the returned records, the consumed offsets
// and nothing else in one Kafka transaction per poll batch, so a crash can
// neither lose a pending status nor reprocess a batch into duplicates.
package routing

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/cloudevents"
	"pubsub/internal/delivery"
	"pubsub/internal/filter"
	"pubsub/internal/kafkautil"
	"pubsub/internal/project"
	"pubsub/internal/registry"
	"pubsub/internal/status"
)

// HeaderChannel is the extra header a delivery record carries so that
// downstream stages can route on the delivery mode without decoding the value.
const HeaderChannel = "pubsub-channel"

// Router turns consumed event records into delivery and status records. It is
// stateless; the registry it holds is refreshed concurrently and safe for use.
type Router struct {
	reg *registry.Registry
}

// New builds a router over an in-memory registry.
func New(reg *registry.Registry) *Router { return &Router{reg: reg} }

// Process evaluates one consumed event record and returns the records to
// produce for it: for every matching subscription one delivery record on
// kafkautil.TopicDeliveries followed by its pending status record on
// kafkautil.TopicStatus. Unparsable records, unmatched events and filter
// errors yield no records; nothing here panics or aborts the batch, the
// transactional caller commits the source offset either way.
func (r *Router) Process(rec *kgo.Record) []*kgo.Record {
	e, err := cloudevents.Parse(rec.Value)
	if err != nil {
		// Section 5: the router never crashes on a malformed record; it
		// skips it and the offset is still committed.
		slog.Warn("router: skipping unparsable event",
			"topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset, "err", err)
		return nil
	}
	owner := kafkautil.HGet(rec, kafkautil.HeaderOwner)
	class := kafkautil.HGet(rec, kafkautil.HeaderClass)
	eventRef := kafkautil.HGet(rec, kafkautil.HeaderEventRef)
	ingested := headerTime(rec, kafkautil.HeaderIngested)
	expires := headerTime(rec, kafkautil.HeaderExpires)
	if eventRef == "" || class == "" {
		slog.Warn("router: event record misses platform headers",
			"event_ref", eventRef, "type", e.Type, "partition", rec.Partition, "offset", rec.Offset)
	}
	if expires.IsZero() {
		slog.Warn("router: event record without expiry header",
			"event_ref", eventRef, "partition", rec.Partition, "offset", rec.Offset)
	}
	slog.Debug("router: event", "event_ref", eventRef, "owner", owner, "type", e.Type,
		"class", class, "ingested", ingested, "expires", expires)

	entries := r.reg.Match(owner, e.Type)
	if len(entries) == 0 {
		return nil
	}

	// Section 6.2: the filter index of lower-case CloudEvents attributes,
	// core ones plus every string extension.
	attrs := attributes(e)

	// Decode the JSON data at most once per event and only when a candidate
	// actually needs it: a data filter, a projection, or a "json:" ordering
	// key. The raw member stays available via e.Data.
	var (
		dataMap map[string]any
		decoded bool
	)
	decode := func() map[string]any {
		if !decoded {
			decoded = true
			m, err := e.JSON()
			if err != nil {
				// Not JSON, no data member, or not an object: data
				// filters then never match (filter.Matches with nil
				// data) and json: ordering keys stay empty.
				slog.Debug("router: no JSON data to decode", "event_ref", eventRef, "err", err)
			} else {
				dataMap = m
			}
		}
		return dataMap
	}

	// Section 6.2: the ordering key is computed before any projection,
	// because the projection may remove the field it reads.
	orderingKey := r.orderingKey(rec, e, owner, decode)

	out := make([]*kgo.Record, 0, 2*len(entries))
	for _, entry := range entries {
		sub := entry.Sub

		var data map[string]any
		if entry.DataFilter != nil {
			data = decode()
		}
		ok, err := filter.Matches(entry.AttrFilter, entry.DataFilter, attrs, data)
		if err != nil {
			// A bad filter is a non-match, never a crash (section 6.2:
			// sandboxed CEL with cost limits; errors still must not stop
			// the pipeline).
			slog.Warn("router: filter error, treating as non-match",
				"subscription", sub.ID, "event_ref", eventRef, "err", err)
			continue
		}
		if !ok {
			continue
		}

		ev := e
		if len(sub.Projection) > 0 {
			// Section 6.2: project a copy; the shared event must never
			// be mutated because other subscriptions still see it.
			// Non-JSON data with a projection is rejected by the
			// registry API (section 5), this guard is defensive.
			projected, err := project.Apply(e.Data, sub.Projection)
			if err != nil {
				slog.Warn("router: projection failed, skipping subscription",
					"subscription", sub.ID, "event_ref", eventRef, "err", err)
				continue
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
			RetentionClass: class,
			OrderingKey:    orderingKey,
			SrcPartition:   rec.Partition,
			SrcOffset:      rec.Offset,
			CreatedAt:      time.Now().UTC(),
			// For live-class events ingested + retention is already in
			// the past; that is intended (publish-api stamps it so).
			ExpiresAt: expires,
			Projected: len(sub.Projection) > 0,
			Event:     ev,
		}
		value, err := msg.Encode()
		if err != nil {
			slog.Warn("router: encoding delivery failed",
				"subscription", sub.ID, "event_ref", eventRef, "err", err)
			continue
		}
		// Section 6.3: keyed by subscription ID, so one subscription
		// always lands on the same partition of the deliveries topic.
		out = append(out, &kgo.Record{
			Topic: kafkautil.TopicDeliveries,
			Key:   []byte(sub.ID),
			Value: value,
			// kgo.RecordHeader and kmsg.Header are structurally
			// identical, the conversion keeps the shared helper.
			Headers: []kgo.RecordHeader{
				kgo.RecordHeader(kafkautil.HSet(HeaderChannel, sub.Channel)),
			},
		})

		// Section 9: every created delivery gets exactly one pending
		// status, written in the same transaction as the delivery.
		srec := status.Record{
			TS:             time.Now().UTC(),
			Owner:          owner,
			EventRef:       eventRef,
			DeliveryID:     msg.DeliveryID,
			SubscriptionID: sub.ID,
			Status:         status.Pending,
			Reason:         "router",
			Attempt:        0,
			RetentionClass: class,
			SrcPartition:   rec.Partition,
			SrcOffset:      rec.Offset,
		}
		svalue, err := json.Marshal(srec)
		if err != nil {
			// Cannot happen for this struct, but never drop the
			// delivery silently either.
			slog.Warn("router: encoding status failed",
				"subscription", sub.ID, "event_ref", eventRef, "err", err)
			continue
		}
		out = append(out, &kgo.Record{
			Topic: kafkautil.TopicStatus,
			Key:   []byte(msg.DeliveryID),
			Value: svalue,
		})
	}
	return out
}

// orderingKey resolves the ordering key definition of the event type
// (section 6.2). "" falls back to the partition key the publish API chose,
// "subject" to the subject attribute, "ext:<name>" to an extension attribute
// and "json:<path>" to a dotted path in the decoded JSON data. Non-string
// JSON values are stringified. An unresolvable key stays empty; every empty
// key still hashes to one deliveries partition, order is just not guaranteed
// across different keys.
func (r *Router) orderingKey(rec *kgo.Record, e *cloudevents.Event, owner string, decode func() map[string]any) string {
	def := ""
	if et, ok := r.reg.EventType(owner, e.Type); ok {
		def = et.OrderingKeyDef
	}
	switch {
	case def == "":
		return kafkautil.HGet(rec, kafkautil.HeaderPKey)
	case def == "subject":
		return e.Subject
	case strings.HasPrefix(def, "ext:"):
		v, _ := e.Extension(strings.TrimPrefix(def, "ext:"))
		return v
	case strings.HasPrefix(def, "json:"):
		return jsonPath(decode(), strings.TrimPrefix(def, "json:"))
	default:
		slog.Warn("router: unknown ordering key definition, falling back to the partition key",
			"def", def, "event_type", e.Type)
		return kafkautil.HGet(rec, kafkautil.HeaderPKey)
	}
}

// jsonPath walks a dotted path through decoded JSON data. Missing paths and
// non-object branches yield ""; non-string leaves are stringified with fmt.
func jsonPath(data map[string]any, path string) string {
	if data == nil {
		return ""
	}
	var v any = data
	for _, seg := range strings.Split(path, ".") {
		m, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		if v, ok = m[seg]; !ok {
			return ""
		}
	}
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprint(v)
}

// attributes builds the filter attribute map: the lower-case CloudEvents
// attributes plus every string-valued extension (section 6.2).
func attributes(e *cloudevents.Event) map[string]string {
	attrs := map[string]string{
		"id":              e.ID,
		"source":          e.Source,
		"type":            e.Type,
		"subject":         e.Subject,
		"datacontenttype": e.DataContentType,
	}
	for name := range e.Extensions {
		if v, ok := e.Extension(name); ok {
			attrs[strings.ToLower(name)] = v
		}
	}
	return attrs
}

// headerTime reads a unix-milliseconds platform header as a time.Time; an
// absent or malformed header yields the zero time.
func headerTime(rec *kgo.Record, key string) time.Time {
	s := kafkautil.HGet(rec, key)
	if s == "" {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
