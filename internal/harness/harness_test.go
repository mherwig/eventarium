package harness

import (
	"testing"
	"time"

	"pubsub/internal/cloudevents"
	"pubsub/internal/filter"
)

// sampleData is what a published event's data member decodes to through
// encoding/json: numbers become float64, the same shape the router evaluates.
func sampleData() map[string]any {
	return map[string]any{
		"shard":   float64(3),
		"seq":     float64(41),
		"sent_at": "2026-10-23T16:00:00Z",
		"pad":     "abc",
	}
}

// TestSeededFiltersCompile pins down the exact filter expressions `harness
// seed` creates: they must compile and evaluate against a published event.
func TestSeededFiltersCompile(t *testing.T) {
	attr, err := filter.CompileAttribute(`attributes["shard"] == "3"`)
	if err != nil {
		t.Fatalf("attribute filter: %v", err)
	}
	attrs := map[string]string{"id": "x", "source": "/harness", "type": "harness.type.0", "subject": "entity-0", "shard": "3"}
	if ok, err := attr.EvalAttribute(attrs, nil); err != nil || !ok {
		t.Fatalf("attribute filter on shard=3: ok=%v err=%v", ok, err)
	}
	attrs["shard"] = "4"
	if ok, err := attr.EvalAttribute(attrs, nil); err != nil || ok {
		t.Fatalf("attribute filter on shard=4: ok=%v err=%v", ok, err)
	}

	data, err := filter.CompileData(seededDataFilter)
	if err != nil {
		t.Fatalf("data filter %q: %v", seededDataFilter, err)
	}
	if ok, err := data.EvalData(sampleData(), nil); err != nil || ok {
		t.Fatalf("data filter %q on seq=41 (odd): ok=%v err=%v", seededDataFilter, ok, err)
	}
	if ok, err := data.EvalData(map[string]any{"seq": float64(40), "shard": float64(0), "sent_at": "x", "pad": "y"}, nil); err != nil || !ok {
		t.Fatalf("data filter %q on seq=40 (even): ok=%v err=%v", seededDataFilter, ok, err)
	}
}

// TestMatchesMirrorsOracle checks that filter.Matches composes the two
// filters exactly like expectedSet does in verify.
func TestMatchesMirrorsOracle(t *testing.T) {
	attr, err := filter.CompileAttribute(`attributes["shard"] == "3"`)
	if err != nil {
		t.Fatal(err)
	}
	attrs := map[string]string{"type": "harness.type.0", "shard": "3"}
	ok, err := filter.Matches(attr, nil, attrs, sampleData())
	if err != nil || !ok {
		t.Fatalf("match: ok=%v err=%v", ok, err)
	}
	attrs["shard"] = "0"
	ok, err = filter.Matches(attr, nil, attrs, sampleData())
	if err != nil || ok {
		t.Fatalf("non-match: ok=%v err=%v", ok, err)
	}
}

// TestParsePublishRefs pins the publish response decoding.
func TestParsePublishRefs(t *testing.T) {
	refs, err := parsePublishRefs([]byte(`[{"event_ref":"a"},{"event_ref":"b"}]`), 2)
	if err != nil || len(refs) != 2 || refs[0] != "a" || refs[1] != "b" {
		t.Fatalf("batch: %v %v", refs, err)
	}
	refs, err = parsePublishRefs([]byte(`{"event_ref":"x","retention_class":"short"}`), 1)
	if err != nil || len(refs) != 1 || refs[0] != "x" {
		t.Fatalf("single: %v %v", refs, err)
	}
	if _, err := parsePublishRefs([]byte(`{"event_ref":"x"}`), 100); err == nil {
		t.Fatal("expected mismatch error")
	}
}

// TestFromSSEID pins the delivery id parsing of the SSE consumer.
func TestFromSSEID(t *testing.T) {
	if ref, id := fromSSEID("evt-1/sub-7", ""); ref != "evt-1" || id != "evt-1/sub-7" {
		t.Fatalf("got %q %q", ref, id)
	}
	if ref, id := fromSSEID("", "plain"); ref != "plain" || id != "plain" {
		t.Fatalf("got %q %q", ref, id)
	}
	if ref, id := fromSSEID("no-slash", ""); ref != "no-slash" || id != "no-slash" {
		t.Fatalf("got %q %q", ref, id)
	}
}

// TestDataMemberIsJSON makes sure the published event data satisfies
// cloudevents' JSON checks, which the platform applies to JSON types.
func TestDataMemberIsJSON(t *testing.T) {
	p := &publisher{payload: 1024, keys: 32, types: []string{"harness.type.0"}, source: "/harness"}
	var seq int64
	evs, recs := p.makeEvents(3, &seq)
	for i := range evs {
		e := &cloudevents.Event{SpecVersion: evs[i].SpecVersion, ID: evs[i].ID, Source: evs[i].Source,
			Type: evs[i].Type, DataContentType: evs[i].DataContentType, Data: evs[i].Data}
		if err := e.Validate(); err != nil {
			t.Fatalf("event %d invalid: %v", i, err)
		}
		if !e.IsJSON() {
			t.Fatalf("event %d data is not JSON", i)
		}
		if _, err := e.JSON(); err != nil {
			t.Fatalf("event %d data: %v", i, err)
		}
		if recs[i].Data["seq"] != float64(i) {
			t.Fatalf("record seq %v, want %d", recs[i].Data["seq"], i)
		}
	}
}

// TestWithinWindow decides which receipts the status check may judge at all.
// This rule exists because getting it wrong produced 30,028 false alarms on
// one scenario: `pg` keeps delivering long after publishing stops (a drained
// backlog plus rate-limited tails), and 30,028 of its 39,719 receipts landed
// after `to` - outside the Status API's query window, so their status rows
// were invisible to a check that read absence as loss.
//
// The boundaries matter as much as the middle: a receipt a moment before
// `to` is judged, one a moment after is not, because the row for the second
// genuinely cannot be seen. A zero timestamp is not judged either - with no
// receipt time there is nothing to place in the window, and guessing would
// either invent a failure or hide one.
func TestWithinWindow(t *testing.T) {
	from := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	to := from.Add(2 * time.Minute)

	for _, tc := range []struct {
		name string
		at   time.Time
		want bool
	}{
		{"first publish", from, true},
		{"during publishing", from.Add(30 * time.Second), true},
		{"inside the padding", to.Add(-time.Second), true},
		{"exactly at the bound", to, true},
		{"after the window - the pg case", to.Add(time.Second), false},
		{"minutes later", to.Add(5 * time.Minute), false},
		{"before the window", from.Add(-time.Second), false},
		{"no timestamp recorded", time.Time{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := withinWindow(tc.at, from, to); got != tc.want {
				t.Errorf("withinWindow(%v) = %v, want %v", tc.at, got, tc.want)
			}
		})
	}
}

// TestParseModes pins down `harness seed -modes`. The default must parse to
// all three modes *in that order*: scripts/chaos.sh's start_sse_clients assigns
// SSE clients to gateways in groups of three on the assumption that three
// consecutive seeded subscriptions are one of each mode, and a different
// default would reintroduce the aliasing that made the first gwfail run pass
// vacuously.
func TestParseModes(t *testing.T) {
	all, err := parseModes("ordered,key_ordered,unordered")
	if err != nil {
		t.Fatalf("default: %v", err)
	}
	want := []string{"ordered", "key_ordered", "unordered"}
	if len(all) != len(want) {
		t.Fatalf("default parsed %v, want %v", all, want)
	}
	for i := range want {
		if all[i] != want[i] {
			t.Fatalf("default parsed %v, want %v — order matters to chaos.sh", all, want)
		}
	}

	got, err := parseModes(" unordered , key_ordered ")
	if err != nil {
		t.Fatalf("two modes with spaces: %v", err)
	}
	if len(got) != 2 || got[0] != "unordered" || got[1] != "key_ordered" {
		t.Errorf("two modes with spaces = %v, want [unordered key_ordered]", got)
	}

	for _, bad := range []string{"", "   ", "unordered,strict", "ordered;unordered", "ORDERED"} {
		if _, err := parseModes(bad); err == nil {
			t.Errorf("parseModes(%q) accepted, want an error", bad)
		}
	}
}
