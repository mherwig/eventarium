package main

import (
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"pubsub/internal/cloudevents"
)

func testEvent(id string) *cloudevents.Event {
	return &cloudevents.Event{
		SpecVersion: "1.0",
		ID:          id,
		Source:      "/test",
		Type:        "test.type",
	}
}

func TestParseHeaderAllowList(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"", nil},
		{"   ", nil},
		{",,,", nil},
		{"x-business-process-id", []string{"x-business-process-id"}},
		// Case folds: HTTP header names are case-insensitive and CloudEvents
		// extension names are lower-case by specification, so the two have to
		// agree on one spelling for the round trip to work.
		{"X-Business-Process-Id, Traceparent", []string{"x-business-process-id", "traceparent"}},
		{"  a  ,,  b , a ", []string{"a", "b"}},
	} {
		got := parseHeaderAllowList(tc.in)
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("parseHeaderAllowList(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// The feature end to end at the unit level: a plain inbound header becomes an
// extension, and the extension becomes the Ce- header the callback consumer
// reads. If either hop breaks, the consumer silently loses the correlation id.
func TestPropagatedHeaderReachesTheCallback(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("X-Business-Process-Id", "order-4711")
	ev := testEvent("id-1")

	allow := parseHeaderAllowList("x-business-process-id")
	if err := applyPropagatedHeaders(hdr, []*cloudevents.Event{ev}, allow); err != nil {
		t.Fatalf("applyPropagatedHeaders: %v", err)
	}

	got, ok := ev.Extension("x-business-process-id")
	if !ok || got != "order-4711" {
		t.Fatalf("extension = %q (present=%v), want order-4711", got, ok)
	}

	// What the dispatcher puts on the webhook request.
	out := http.Header{}
	ev.SetHeaders(out)
	if want := "order-4711"; out.Get("Ce-X-Business-Process-Id") != want {
		t.Errorf("callback header Ce-X-Business-Process-Id = %q, want %q",
			out.Get("Ce-X-Business-Process-Id"), want)
	}
}

// The extension has to survive serialisation, or it never reaches the Kafka
// record the router reads.
func TestPropagatedHeaderSurvivesMarshalling(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("x-business-process-id", "order-4711")
	ev := testEvent("id-1")

	if err := applyPropagatedHeaders(hdr, []*cloudevents.Event{ev},
		[]string{"x-business-process-id"}); err != nil {
		t.Fatalf("applyPropagatedHeaders: %v", err)
	}

	enc, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}
	if !strings.Contains(string(enc), `"x-business-process-id":"order-4711"`) {
		t.Errorf("marshalled event does not carry the extension: %s", enc)
	}
}

// An extension the publisher set in the body is deliberate; the header is a
// convenience for context the publisher's code never sees. The deliberate one
// must win, or a request header would silently rewrite the event.
func TestExplicitExtensionWinsOverTheHeader(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("x-business-process-id", "from-header")
	ev := testEvent("id-1")
	ev.Extensions = map[string]json.RawMessage{
		"x-business-process-id": json.RawMessage(`"from-body"`),
	}

	if err := applyPropagatedHeaders(hdr, []*cloudevents.Event{ev},
		[]string{"x-business-process-id"}); err != nil {
		t.Fatalf("applyPropagatedHeaders: %v", err)
	}
	got, _ := ev.Extension("x-business-process-id")
	if got != "from-body" {
		t.Errorf("extension = %q, want from-body: the event's own attribute must win", got)
	}
}

// A header on the allow-list that was not sent must not invent a value.
func TestAbsentHeaderIsNotAdded(t *testing.T) {
	hdr := http.Header{}
	ev := testEvent("id-1")

	if err := applyPropagatedHeaders(hdr, []*cloudevents.Event{ev},
		[]string{"x-business-process-id"}); err != nil {
		t.Fatalf("applyPropagatedHeaders: %v", err)
	}
	if _, ok := ev.Extension("x-business-process-id"); ok {
		t.Error("extension created for a header that was not sent")
	}
}

// Off by default: with no allow-list nothing is read, which is what keeps this
// a no-op for every deployment that has not asked for it.
func TestEmptyAllowListChangesNothing(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("authorization", "Bearer secret")
	hdr.Set("x-business-process-id", "order-4711")

	for _, ev := range []*cloudevents.Event{testEvent("id-1")} {
		if err := applyPropagatedHeaders(hdr, []*cloudevents.Event{ev}, nil); err != nil {
			t.Fatalf("applyPropagatedHeaders: %v", err)
		}
		if len(ev.Extensions) != 0 {
			t.Errorf("extensions = %v, want none with an empty allow-list", ev.Extensions)
		}
	}
}

// Batch mode multiplies one header by every event in the request, so an
// over-long value is rejected rather than dropped: a truncated correlation id
// looks valid and is not, and a dropped one disappears without the publisher
// ever learning about it.
func TestOversizedValueRejectsTheRequest(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("x-business-process-id", strings.Repeat("a", maxPropagatedHeaderBytes+1))
	ev := testEvent("id-1")

	err := applyPropagatedHeaders(hdr, []*cloudevents.Event{ev},
		[]string{"x-business-process-id"})
	if err == nil {
		t.Fatal("oversized header was accepted, want the request rejected")
	}
	if !strings.Contains(err.Error(), "x-business-process-id") {
		t.Errorf("error %q does not name the offending header", err)
	}
	if _, ok := ev.Extension("x-business-process-id"); ok {
		t.Error("extension set for a value that was rejected")
	}
}

// One HTTP request carries the whole batch and therefore one set of headers:
// every event in it has to get the correlation id, not just the first.
func TestBatchAppliesToEveryEvent(t *testing.T) {
	hdr := http.Header{}
	hdr.Set("x-business-process-id", "order-4711")
	events := []*cloudevents.Event{testEvent("id-1"), testEvent("id-2"), testEvent("id-3")}

	if err := applyPropagatedHeaders(hdr, events, []string{"x-business-process-id"}); err != nil {
		t.Fatalf("applyPropagatedHeaders: %v", err)
	}
	for _, ev := range events {
		got, ok := ev.Extension("x-business-process-id")
		if !ok || got != "order-4711" {
			t.Errorf("event %s: extension = %q (present=%v), want order-4711", ev.ID, got, ok)
		}
	}
}
