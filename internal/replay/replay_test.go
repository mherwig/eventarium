package replay

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/cloudevents"
	"pubsub/internal/delivery"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/routing"
	"pubsub/internal/status"
)

// now is a fixed instant the classification tests reason about: section 11
// compares the event's expires_at against "now", so the tests must not depend
// on the wall clock.
var now = time.Date(2026, 10, 25, 12, 0, 0, 0, time.UTC)

// TestParseSelector covers the 400s of section 11's selector rules: at least
// one criterion, known statuses, RFC3339 bounds with from <= to, max_events
// within the service cap, an honest rejection of event_types (status records
// carry no event type, section 9) and a body that stops after one selector.
func TestParseSelector(t *testing.T) {
	const maxEvents = 10000
	tests := []struct {
		name    string
		body    string
		wantErr string // "" means the selector must be accepted
	}{
		{name: "statuses", body: `{"statuses":["failed","dead_lettered"]}`},
		{name: "time range", body: `{"from":"2026-10-25T00:00:00Z","to":"2026-10-25T12:00:00Z"}`},
		{name: "from only", body: `{"from":"2026-10-25T00:00:00Z"}`},
		{name: "event refs", body: `{"event_refs":["ref-1","ref-2"]}`},
		{name: "dry run with cap", body: `{"statuses":["delivered"],"dry_run":true,"max_events":5}`},
		{name: "max at the cap", body: `{"statuses":["delivered"],"max_events":10000}`},

		{name: "empty body", body: ``, wantErr: "request body is required"},
		{name: "no criterion", body: `{}`, wantErr: "at least one of statuses, from, to or event_refs"},
		{name: "event_types rejected", body: `{"event_types":["order.created"]}`, wantErr: "event_types is not supported"},
		{
			name:    "event_types rejected before the criterion rule",
			body:    `{"event_types":["order.created"],"statuses":["failed"]}`,
			wantErr: "event_types is not supported",
		},
		{name: "unknown status", body: `{"statuses":["exploded"]}`, wantErr: `unknown status "exploded"`},
		{
			// Not a typo and not selectable on purpose: the router records
			// it for an event the filter rejected, so there is no delivery
			// to repeat - and replay applies the projection, not the filter.
			name:    "filtered is not replayable",
			body:    `{"statuses":["filtered"]}`,
			wantErr: `status "filtered" cannot be replayed`,
		},
		{name: "empty status", body: `{"statuses":["  "]}`, wantErr: "statuses must not contain an empty value"},
		{name: "empty event ref", body: `{"event_refs":[" "]}`, wantErr: "event_refs must not contain an empty value"},
		{name: "from not rfc3339", body: `{"from":"yesterday"}`, wantErr: "from must be an RFC3339 timestamp"},
		{name: "to not rfc3339", body: `{"to":"noon"}`, wantErr: "to must be an RFC3339 timestamp"},
		{
			name:    "from after to",
			body:    `{"from":"2026-10-25T12:00:00Z","to":"2026-10-25T00:00:00Z"}`,
			wantErr: "from is after to",
		},
		{name: "max_events zero", body: `{"statuses":["failed"],"max_events":0}`, wantErr: "max_events must be between 1"},
		{name: "max_events above the cap", body: `{"statuses":["failed"],"max_events":10001}`, wantErr: "max_events must be between 1"},
		{name: "unknown field", body: `{"statuses":["failed"],"event_type":"x"}`, wantErr: "invalid request body"},
		{name: "broken json", body: `{"statuses":["failed"]`, wantErr: "invalid request body"},
		{name: "second selector ignored", body: `{"statuses":["failed"]}{"statuses":["delivered"]}`, wantErr: "unexpected data after the selector"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseSelector([]byte(tc.body), maxEvents)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ParseSelector(%s) = %v, want no error", tc.body, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ParseSelector(%s) = nil error, want error containing %q", tc.body, tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ParseSelector(%s) error = %q, want it to contain %q", tc.body, err, tc.wantErr)
			}
		})
	}
}

// TestSelectorLimit checks that an explicitly submitted cap is honoured and
// the service default applies when the caller leaves max_events out — the
// pointer in Selector is what keeps "0 submitted" apart from "absent".
func TestSelectorLimit(t *testing.T) {
	sel, err := ParseSelector([]byte(`{"statuses":["failed"],"max_events":7}`), 10000)
	if err != nil {
		t.Fatalf("ParseSelector: %v", err)
	}
	if got := sel.limit(10000); got != 7 {
		t.Fatalf("limit() = %d, want the submitted 7", got)
	}
	sel, err = ParseSelector([]byte(`{"statuses":["failed"]}`), 10000)
	if err != nil {
		t.Fatalf("ParseSelector: %v", err)
	}
	if got := sel.limit(4242); got != 4242 {
		t.Fatalf("limit() = %d, want the service default 4242", got)
	}
}

// TestClassify pins the retention rules of section 11 (spec line 282): an
// event whose record is gone, or whose expires_at has passed, counts
// not_replayable and is left untouched; everything else stays replayable. The
// boundary is inclusive — at exactly expires_at the event is already expired.
func TestClassify(t *testing.T) {
	expired := &kgo.Record{Headers: headers(kafkautil.HeaderExpires, strconv.FormatInt(now.Add(-time.Millisecond).UnixMilli(), 10))}
	atBoundary := &kgo.Record{Headers: headers(kafkautil.HeaderExpires, strconv.FormatInt(now.UnixMilli(), 10))}
	valid := &kgo.Record{Headers: headers(kafkautil.HeaderExpires, strconv.FormatInt(now.Add(time.Hour).UnixMilli(), 10))}
	noHeader := &kgo.Record{}
	malformed := &kgo.Record{Headers: headers(kafkautil.HeaderExpires, "not-a-timestamp")}

	tests := []struct {
		name       string
		rec        *kgo.Record
		wantOK     bool
		wantReason string
	}{
		{name: "record no longer in the events topic", rec: nil, wantOK: false,
			wantReason: "no longer in the events topic"},
		{name: "expires_at passed", rec: expired, wantOK: false, wantReason: "has passed"},
		{name: "expires_at exactly now", rec: atBoundary, wantOK: false, wantReason: "has passed"},
		{name: "still inside retention", rec: valid, wantOK: true},
		{name: "no expires header stays replayable", rec: noHeader, wantOK: true},
		{name: "malformed expires header stays replayable", rec: malformed, wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := classify(tc.rec, now)
			if ok != tc.wantOK {
				t.Fatalf("classify() ok = %v, want %v (reason %q)", ok, tc.wantOK, reason)
			}
			if !tc.wantOK && !strings.Contains(reason, tc.wantReason) {
				t.Fatalf("classify() reason = %q, want it to contain %q", reason, tc.wantReason)
			}
		})
	}
}

// TestHeaderTime pins the platform header convention shared with the router:
// unix milliseconds, absent or malformed yields the zero time.
func TestHeaderTime(t *testing.T) {
	rec := &kgo.Record{Headers: headers(kafkautil.HeaderExpires, strconv.FormatInt(now.UnixMilli(), 10))}
	if got := headerTime(rec, kafkautil.HeaderExpires); !got.Equal(now) {
		t.Fatalf("headerTime() = %v, want %v", got, now)
	}
	if got := headerTime(&kgo.Record{}, kafkautil.HeaderExpires); !got.IsZero() {
		t.Fatalf("headerTime(absent) = %v, want the zero time", got)
	}
	if got := headerTime(&kgo.Record{Headers: headers(kafkautil.HeaderExpires, "x")}, kafkautil.HeaderExpires); !got.IsZero() {
		t.Fatalf("headerTime(malformed) = %v, want the zero time", got)
	}
}

// TestAssemble runs the classification and build step of the worker against
// fakes (section 11, retention limit): of four resolved deliveries one has no
// record left, one is past expires_at and two are replayable. The counters
// must satisfy matched = enqueued + not_replayable, the replayable rows must
// come out in selector order (section 284) and each must become the section 9
// pair — delivery plus pending status — flagged with the replay ID.
func TestAssemble(t *testing.T) {
	reg := testRegistry(t)
	entry, ok := reg.Subscription("sub-1")
	if !ok {
		t.Fatal("the fake registry does not know sub-1")
	}
	job := &Job{ID: "job-1", Owner: "team-a", SubscriptionID: "sub-1", Status: StatusPending}
	w := &Worker{reg: reg}

	mk := func(ref string, offset int64) Match {
		return Match{
			DeliveryID:     ref + "/sub-1",
			EventRef:       ref,
			RetentionClass: kafkautil.ClassShort,
			SrcPartition:   3,
			SrcOffset:      offset,
			Ingested:       now.Add(-time.Minute),
			CurrentStatus:  status.DeadLettered,
		}
	}
	m1 := mk("ref-1", 10)
	m2 := mk("ref-2", 11) // deliberately not in the records map: pruned
	m3 := mk("ref-3", 12) // present but past expires_at
	m4 := mk("ref-4", 13)

	records := map[coord]*kgo.Record{
		eventCoord(m1): eventRecord(t, m1, now.Add(time.Hour)),
		eventCoord(m3): eventRecord(t, m3, now.Add(-time.Second)),
		eventCoord(m4): eventRecord(t, m4, now.Add(time.Hour)),
	}

	matches := []Match{m1, m2, m3, m4}
	out, built, notReplayable := w.assemble(job, entry.Sub, matches, records, now)

	if built != 2 || notReplayable != 2 {
		t.Fatalf("built = %d, notReplayable = %d, want 2 and 2 (matched %d)", built, notReplayable, len(matches))
	}
	if int64(len(matches)) != built+notReplayable {
		t.Fatalf("matched %d != enqueued %d + not_replayable %d", len(matches), built, notReplayable)
	}
	if len(out) != 4 {
		t.Fatalf("produced %d records, want 4 (a delivery and a status per replayable event)", len(out))
	}

	// Section 284: selector order survives, ref-1 before ref-4.
	for i, want := range []string{"ref-1", "ref-1", "ref-4", "ref-4"} {
		var got string
		switch i % 2 {
		case 0:
			msg, err := delivery.Decode(out[i].Value)
			if err != nil {
				t.Fatalf("decoding record %d: %v", i, err)
			}
			got = msg.EventRef
		default:
			var rec status.Record
			if err := json.Unmarshal(out[i].Value, &rec); err != nil {
				t.Fatalf("decoding record %d: %v", i, err)
			}
			got = rec.EventRef
		}
		if got != want {
			t.Fatalf("record %d is for %q, want %q (order lost)", i, got, want)
		}
	}

	msg, err := delivery.Decode(out[0].Value)
	if err != nil {
		t.Fatalf("decoding the delivery: %v", err)
	}
	if !msg.Replay || msg.ReplayID != job.ID {
		t.Fatalf("delivery replay flags = %v/%q, want true/%q", msg.Replay, msg.ReplayID, job.ID)
	}
	if msg.DeliveryID != m1.DeliveryID {
		t.Fatalf("delivery ID = %q, want the original %q (section 11 re-delivers the same delivery)", msg.DeliveryID, m1.DeliveryID)
	}
	if msg.Owner != "team-a" || msg.Channel != "webhook" || msg.RetentionClass != kafkautil.ClassShort {
		t.Fatalf("delivery owner/channel/class = %q/%q/%q, want team-a/webhook/short",
			msg.Owner, msg.Channel, msg.RetentionClass)
	}
	if msg.OrderingKey != "pk-1" {
		t.Fatalf("ordering key = %q, want the original partition key pk-1", msg.OrderingKey)
	}
	if msg.SrcPartition != 3 || msg.SrcOffset != 10 {
		t.Fatalf("coordinates = %d:%d, want 3:10", msg.SrcPartition, msg.SrcOffset)
	}
	if !msg.CreatedAt.Equal(now) {
		t.Fatalf("CreatedAt = %v, want the build instant %v", msg.CreatedAt, now)
	}
	if want := now.Add(time.Hour); !msg.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt = %v, want the publish-stamped %v", msg.ExpiresAt, want)
	}
	if out[0].Topic != kafkautil.TopicDeliveries || string(out[0].Key) != "sub-1" {
		t.Fatalf("delivery record = %s/%q, want deliveries keyed by subscription", out[0].Topic, out[0].Key)
	}
	if got := kafkautil.HGet(out[0], routing.HeaderChannel); got != "webhook" {
		t.Fatalf("channel header = %q, want webhook", got)
	}

	var srec status.Record
	if err := json.Unmarshal(out[1].Value, &srec); err != nil {
		t.Fatalf("decoding the pending status: %v", err)
	}
	if srec.Status != status.Pending || srec.Reason != "replay" || srec.ReplayID != job.ID {
		t.Fatalf("status = %q reason %q replay %q, want pending/replay/%q",
			srec.Status, srec.Reason, srec.ReplayID, job.ID)
	}
	if srec.DeliveryID != msg.DeliveryID || srec.EventRef != m1.EventRef {
		t.Fatalf("status refers to %q/%q, want %q/%q", srec.DeliveryID, srec.EventRef, msg.DeliveryID, m1.EventRef)
	}
	if srec.Owner != msg.Owner || srec.SubscriptionID != msg.SubscriptionID {
		t.Fatalf("status owner/subscription = %q/%q, want %q/%q",
			srec.Owner, srec.SubscriptionID, msg.Owner, msg.SubscriptionID)
	}
	if out[1].Topic != kafkautil.TopicStatus || string(out[1].Key) != msg.DeliveryID {
		t.Fatalf("status record = %s/%q, want delivery-status keyed by delivery ID", out[1].Topic, out[1].Key)
	}
}

// TestAssembleDryRunOnlyReportsCounts documents what the dry run of section
// 282 is: the same resolve, read and classification as a real job, minus the
// produce. The caller keeps enqueued at zero for a dry run, so this only pins
// that the classification itself does not depend on dry_run.
func TestAssembleDryRunOnlyReportsCounts(t *testing.T) {
	reg := testRegistry(t)
	entry, _ := reg.Subscription("sub-1")
	job := &Job{ID: "job-2", Owner: "team-a", SubscriptionID: "sub-1", DryRun: true}
	w := &Worker{reg: reg}

	m := Match{
		DeliveryID: "ref-1/sub-1", EventRef: "ref-1",
		RetentionClass: kafkautil.ClassShort, SrcPartition: 0, SrcOffset: 5,
	}
	records := map[coord]*kgo.Record{eventCoord(m): eventRecord(t, m, now.Add(time.Hour))}
	out, built, notReplayable := w.assemble(job, entry.Sub, []Match{m}, records, now)
	if built != 1 || notReplayable != 0 || len(out) != 2 {
		t.Fatalf("built = %d, notReplayable = %d, records = %d, want 1, 0 and 2", built, notReplayable, len(out))
	}
}

// fakeSource is a static registry source: the tests need one event type and
// one subscription without PostgreSQL behind them.
type fakeSource struct {
	types []registry.EventType
	subs  []registry.Subscription
}

func (f fakeSource) EventTypes(context.Context) ([]registry.EventType, error) { return f.types, nil }

func (f fakeSource) Subscriptions(context.Context) ([]registry.Subscription, error) {
	return f.subs, nil
}

// testRegistry loads a registry that knows the owner, event type and
// subscription every test in this file uses.
func testRegistry(t *testing.T) *registry.Registry {
	t.Helper()
	reg := registry.New(fakeSource{
		types: []registry.EventType{{
			Owner:          "team-a",
			Name:           "replay.test",
			RetentionClass: kafkautil.ClassShort,
			ContentType:    "application/json",
		}},
		subs: []registry.Subscription{{
			ID:            "sub-1",
			Owner:         "team-a",
			EventType:     "replay.test",
			Channel:       delivery.ChannelWebhook,
			DeliveryMode:  delivery.ModeUnordered,
			CallbackURL:   "http://localhost:9091",
			WebhookFormat: "cloudevents",
		}},
	}, time.Minute)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("loading the fake registry: %v", err)
	}
	return reg
}

// eventCoord is where the original event of a match lives (section 9 stores
// the retention class, partition and offset on every status record).
func eventCoord(m Match) coord {
	return coord{Topic: kafkautil.EventsTopic(m.RetentionClass), Partition: m.SrcPartition, Offset: m.SrcOffset}
}

// eventRecord fabricates the original event record of one match: the
// structured CloudEvent plus the platform headers the router and the replay
// service read (section 5).
func eventRecord(t *testing.T, m Match, expires time.Time) *kgo.Record {
	t.Helper()
	e := cloudevents.Event{
		SpecVersion:     cloudevents.SpecV1,
		ID:              m.EventRef,
		Source:          "/spike",
		Type:            "replay.test",
		DataContentType: "application/json",
		Data:            json.RawMessage(`{"seq":1}`),
	}
	value, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("encoding the event: %v", err)
	}
	return &kgo.Record{
		Topic:     kafkautil.EventsTopic(m.RetentionClass),
		Partition: m.SrcPartition,
		Offset:    m.SrcOffset,
		Value:     value,
		Headers: concat(
			headers(kafkautil.HeaderOwner, "team-a"),
			headers(kafkautil.HeaderClass, m.RetentionClass),
			headers(kafkautil.HeaderPKey, "pk-1"),
			headers(kafkautil.HeaderExpires, strconv.FormatInt(expires.UnixMilli(), 10)),
		),
	}
}

// headers builds record headers the way the router does.
func headers(key, value string) []kgo.RecordHeader {
	return []kgo.RecordHeader{kgo.RecordHeader(kafkautil.HSet(key, value))}
}

func concat(groups ...[]kgo.RecordHeader) []kgo.RecordHeader {
	var out []kgo.RecordHeader
	for _, g := range groups {
		out = append(out, g...)
	}
	return out
}
