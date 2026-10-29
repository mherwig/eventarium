package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/delivery"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/status"
)

// The event every test in this file routes: one type, one owner, no filter of
// its own.
const (
	testOwner = "team-a"
	testType  = "orders.created"
)

// fakeSource is a static registry source, the same trick internal/replay
// uses: these tests need event types and subscriptions without PostgreSQL.
type fakeSource struct {
	types []registry.EventType
	subs  []registry.Subscription
}

func (f fakeSource) EventTypes(context.Context) ([]registry.EventType, error) { return f.types, nil }

func (f fakeSource) Subscriptions(context.Context) ([]registry.Subscription, error) {
	return f.subs, nil
}

// loadRegistry builds a router over the given subscriptions.
func loadRegistry(t *testing.T, subs ...registry.Subscription) *registry.Registry {
	t.Helper()
	reg := registry.New(fakeSource{
		types: []registry.EventType{{
			Owner:          testOwner,
			Name:           testType,
			RetentionClass: kafkautil.ClassShort,
			ContentType:    "application/json",
		}},
		subs: subs,
	}, time.Minute)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("loading the fake registry: %v", err)
	}
	return reg
}

// webhookSub is a subscription without filters: it matches every event of its
// type.
func webhookSub(id string) registry.Subscription {
	return registry.Subscription{
		ID:            id,
		Owner:         testOwner,
		EventType:     testType,
		Channel:       delivery.ChannelWebhook,
		DeliveryMode:  delivery.ModeUnordered,
		CallbackURL:   "http://localhost:9091",
		WebhookFormat: "cloudevents",
	}
}

// eventRecord is the consumed Kafka record of one published event, with the
// platform headers of section 5.
func eventRecord(t *testing.T, ref string) *kgo.Record {
	t.Helper()
	return &kgo.Record{
		Topic:     kafkautil.EventsTopic(kafkautil.ClassShort),
		Partition: 3,
		Offset:    42,
		Value: []byte(fmt.Sprintf(
			`{"specversion":"1.0","id":"%s","source":"/test","type":"%s","datacontenttype":"application/json","data":{"order_id":"o-1","region":"eu"}}`,
			ref, testType)),
		Headers: []kgo.RecordHeader{
			kgo.RecordHeader(kafkautil.HSet(kafkautil.HeaderEventRef, ref)),
			kgo.RecordHeader(kafkautil.HSet(kafkautil.HeaderOwner, testOwner)),
			kgo.RecordHeader(kafkautil.HSet(kafkautil.HeaderClass, kafkautil.ClassShort)),
			kgo.RecordHeader(kafkautil.HSet(kafkautil.HeaderIngested, "1761000000000")),
			kgo.RecordHeader(kafkautil.HSet(kafkautil.HeaderExpires, "1761086400000")),
			kgo.RecordHeader(kafkautil.HSet(kafkautil.HeaderPKey, "pk")),
		},
	}
}

// statusRecords decodes every status record of a Process result, keeping the
// delivery records aside.
func statusRecords(t *testing.T, out []*kgo.Record) (deliveries []*kgo.Record, statuses []status.Record) {
	t.Helper()
	for _, rec := range out {
		if rec.Topic != kafkautil.TopicStatus {
			deliveries = append(deliveries, rec)
			continue
		}
		var s status.Record
		if err := json.Unmarshal(rec.Value, &s); err != nil {
			t.Fatalf("decoding a status record: %v", err)
		}
		statuses = append(statuses, s)
	}
	return deliveries, statuses
}

// TestProcessRecordsTheRejectedCandidate is the tracking contract: a
// subscription whose filter says no produces no delivery, but does produce a
// status row. Before this, a filter that matched nothing left no trace of the
// event anywhere - no delivery, no status, not even a log line.
func TestProcessRecordsTheRejectedCandidate(t *testing.T) {
	rejecting := webhookSub("sub-reject")
	rejecting.Filter = `attributes["region"] == "us"` // the event is "eu"

	reg := loadRegistry(t, webhookSub("sub-match"), rejecting)
	out := New(reg).Process(eventRecord(t, "ref-1"))

	deliveries, statuses := statusRecords(t, out)
	if len(deliveries) != 1 || len(statuses) != 2 {
		t.Fatalf("got %d delivery and %d status records, want 1 and 2", len(deliveries), len(statuses))
	}
	if got := string(deliveries[0].Key); got != "sub-match" {
		t.Errorf("delivery key = %q, want the matching subscription ID", got)
	}

	for _, s := range statuses {
		if s.SubscriptionID == "sub-match" {
			if s.Status != status.Pending || s.Reason != ReasonRouter {
				t.Errorf("matching subscription got %s/%s, want %s/%s",
					s.Status, s.Reason, status.Pending, ReasonRouter)
			}
			continue
		}
		if s.Status != status.Filtered {
			t.Errorf("rejected subscription got status %q, want %q", s.Status, status.Filtered)
		}
		if s.Reason != ReasonFilterMismatch {
			t.Errorf("reason = %q, want %q", s.Reason, ReasonFilterMismatch)
		}
		if want := delivery.NewID("ref-1", "sub-reject"); s.DeliveryID != want {
			t.Errorf("delivery ID = %q, want %q (the delivery that would have been)", s.DeliveryID, want)
		}
		if s.EventRef != "ref-1" || s.Owner != testOwner || s.SubscriptionID != "sub-reject" {
			t.Errorf("record identifies %s/%s/%s, want %s/%s/sub-reject",
				s.Owner, s.EventRef, s.SubscriptionID, testOwner, "ref-1")
		}
		if s.RetentionClass != kafkautil.ClassShort || s.SrcPartition != 3 || s.SrcOffset != 42 {
			t.Errorf("record carries %s/%d/%d, want the source coordinates of the event",
				s.RetentionClass, s.SrcPartition, s.SrcOffset)
		}
		if s.Attempt != 0 {
			t.Errorf("attempt = %d, want 0", s.Attempt)
		}
	}
}

// TestProcessFilterErrorIsRecorded pins the difference between a filter that
// said no and one that could not be evaluated: both reject the event, but only
// the second is a bug, and the reason is what tells them apart.
func TestProcessFilterErrorIsRecorded(t *testing.T) {
	broken := webhookSub("sub-broken")
	// Single quotes deliberately miss the equality index of section 6.2, so
	// this reaches CEL, which errors on the missing "region" key instead of
	// evaluating to false.
	broken.Filter = `attributes['region'] == 'us'`

	out := New(loadRegistry(t, broken)).Process(eventRecord(t, "ref-2"))
	deliveries, statuses := statusRecords(t, out)
	if len(deliveries) != 0 || len(statuses) != 1 {
		t.Fatalf("got %d delivery and %d status records, want 0 and 1", len(deliveries), len(statuses))
	}
	if statuses[0].Status != status.Filtered || statuses[0].Reason != ReasonFilterError {
		t.Fatalf("got %s/%s, want %s/%s",
			statuses[0].Status, statuses[0].Reason, status.Filtered, ReasonFilterError)
	}
}

// TestProcessWithoutCandidateSubscriptionIsSilent documents the deliberate
// boundary: with no subscription for the event's type there is no subscription
// ID to attach a record to, so the router stays silent and publish-api warns
// about it at the source instead.
func TestProcessWithoutCandidateSubscriptionIsSilent(t *testing.T) {
	rec := eventRecord(t, "ref-4")
	rec.Value = []byte(`{"specversion":"1.0","id":"4","source":"/test","type":"nobody.listens","datacontenttype":"application/json","data":{}}`)
	out := New(loadRegistry(t, webhookSub("sub-elsewhere"))).Process(rec)
	if len(out) != 0 {
		t.Fatalf("got %d records for an event type without subscriptions, want 0", len(out))
	}
}
