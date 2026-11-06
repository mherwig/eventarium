package main

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// parse is being changed from two decodes to one, and the second decode is
// also where several skip decisions are made. These pin the behaviour each of
// them has today so a single-decode rewrite cannot quietly change which
// records are dropped: the payload guard in particular is section 9's promise
// that a payload never reaches ClickHouse, and it is the kind of check that
// passes vacuously if the field it reads stops being populated.
func TestParseBehaviour(t *testing.T) {
	// Silence the warnings these cases deliberately trigger; asserting on log
	// text would only couple the test to the wording.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

	cases := []struct {
		name  string
		value string
		want  bool
	}{
		{"valid row", benchStatusJSON, true},
		{"payload in data", `{"ts":"2026-11-06T08:00:00Z","owner":"o","event_ref":"e",` +
			`"delivery_id":"d","subscription_id":"s","status":"pending","data":{"a":1}}`, false},
		{"payload in payload", `{"delivery_id":"d","payload":"secret"}`, false},
		{"payload in body", `{"delivery_id":"d","body":"secret"}`, false},
		// An explicitly empty string IS a payload as far as this guard reads
		// it: the raw bytes of "" are two quote characters, not nothing, so
		// the original map probe's `string(v) != ""` test never excluded it
		// either - verified against the two-decode version. Only an absent
		// member or an explicit null are not payloads. Documented rather than
		// "fixed": a status record carrying `data` at all is the abnormal case
		// section 9 defends against, and changing the rule is a different
		// change from removing a duplicate decode.
		{"data empty string", `{"delivery_id":"d","data":""}`, false},
		{"data null", `{"delivery_id":"d","data":null}`, true},
		{"data object", `{"delivery_id":"d","data":{}}`, false},
		{"not an object", `123`, false},
		{"array", `[1,2,3]`, false},
		{"malformed", `{"delivery_id":`, false},
		{"wrong field type", `{"delivery_id":"d","attempt":"three"}`, false},
		{"missing delivery_id", `{"ts":"2026-11-06T08:00:00Z","status":"pending"}`, false},
		// A producer that forgets the timestamp is tolerated, not dropped.
		{"missing ts", `{"delivery_id":"d","status":"pending"}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &kgo.Record{Value: []byte(tc.value)}
			_, ok := parse(rec)
			if ok != tc.want {
				t.Errorf("parse() ok = %v, want %v", ok, tc.want)
			}
		})
	}
}

// TestParseFields checks the values, not just the verdict. The embedded
// status.Record must promote its fields rather than nest: if it did not,
// every field would decode to its zero value, parse would still report ok, and
// the zero TS would be quietly replaced by time.Now() by the "tolerate
// producers that forget the timestamp" path - so every row in ClickHouse would
// carry the insert time instead of the status time while every test passed.
func TestParseFields(t *testing.T) {
	discardLogs()
	rec := &kgo.Record{Topic: "delivery-status", Partition: 4, Offset: 4113,
		Value: []byte(benchStatusJSON)}
	e, ok := parse(rec)
	if !ok {
		t.Fatal("valid record skipped")
	}
	r := e.rec
	if r.Owner != "drain6" {
		t.Errorf("Owner = %q, want %q", r.Owner, "drain6")
	}
	if r.EventRef != "019f0000-0000-7000-8000-000000000000" {
		t.Errorf("EventRef = %q", r.EventRef)
	}
	if r.DeliveryID == "" {
		t.Error("DeliveryID empty: the row would have been skipped, not decoded")
	}
	if r.SubscriptionID == "" {
		t.Error("SubscriptionID empty")
	}
	if r.Status != "delivered" {
		t.Errorf("Status = %q, want %q", r.Status, "delivered")
	}
	if r.HTTPCode != 200 {
		t.Errorf("HTTPCode = %d, want 200", r.HTTPCode)
	}
	if r.LatencyMS != 3 {
		t.Errorf("LatencyMS = %d, want 3", r.LatencyMS)
	}
	if r.RetentionClass != "short" {
		t.Errorf("RetentionClass = %q, want %q", r.RetentionClass, "short")
	}
	if r.SrcPartition != 4 {
		t.Errorf("SrcPartition = %d, want 4", r.SrcPartition)
	}
	if r.SrcOffset != 4113 {
		t.Errorf("SrcOffset = %d, want 4113", r.SrcOffset)
	}
	// The one that a promotion failure would hide behind the zero-TS fallback.
	// The fixture carries nanoseconds, so asserting on them also proves the
	// value was decoded rather than left zero: a nested-struct failure would
	// have produced the zero time here instead.
	wantTS := time.Date(2026, time.November, 6, 8, 0, 0, 123456789, time.UTC)
	if !r.TS.Equal(wantTS) {
		t.Errorf("TS = %v, want %v (a zero value here would be replaced by time.Now())",
			r.TS, wantTS)
	}
	if e.src != rec {
		t.Error("entry.src is not the record it came from")
	}
}
