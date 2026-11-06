package main

import (
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// discardLogs keeps the benchmarks measuring decode cost rather than the cost
// of writing a warning to stderr - one of these cases fires a warning on every
// iteration, which is not a path production reaches.
func discardLogs() {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// parse decoded every status record twice: once into a map to sniff for a
// payload the record must never carry (section 9), then again into the row
// itself. At the 1,000 ev/s load test that is ~19,400 status records a second,
// so the duplicate decode is paid for on the writer's hot path - and the map
// probe is the expensive of the two, since it boxes every member's bytes into
// a json.RawMessage before discarding all but three keys.
//
// The realistic record is ~196 bytes of ClickHouse row, measured against
// pubsub.delivery_status for drain6.
const benchStatusJSON = `{"ts":"2026-11-06T08:00:00.123456789Z","owner":"drain6",` +
	`"event_ref":"019f0000-0000-7000-8000-000000000000",` +
	`"delivery_id":"019f0000-0000-7000-8000-000000000000/11111111-1111-1111-1111-111111111111",` +
	`"subscription_id":"11111111-1111-1111-1111-111111111111","status":"delivered",` +
	`"reason":"","attempt":0,"http_code":200,"latency_ms":3,` +
	`"retention_class":"short","src_partition":4,"src_offset":4113}`

func benchRecord() *kgo.Record {
	return &kgo.Record{Topic: "delivery-status", Partition: 4, Offset: 4113,
		Value: []byte(benchStatusJSON)}
}

func BenchmarkParse(b *testing.B) {
	discardLogs()
	rec := benchRecord()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := parse(rec); !ok {
			b.Fatal("parse skipped a valid record")
		}
	}
}

// BenchmarkParseWithPayload is the path that must keep working after any
// change to the decode: a record carrying a payload is dropped rather than
// copied into ClickHouse, and that check has to survive a single decode.
func BenchmarkParseWithPayload(b *testing.B) {
	discardLogs()
	rec := &kgo.Record{Value: []byte(
		`{"ts":"2026-11-06T08:00:00Z","owner":"o","event_ref":"e","delivery_id":"d",` +
			`"subscription_id":"s","status":"pending","data":{"secret":"must not be stored"}}`)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := parse(rec); ok {
			b.Fatal("parse accepted a record carrying a payload")
		}
	}
}
