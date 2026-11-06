package routing

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"pubsub/internal/cloudevents"
	"pubsub/internal/delivery"
	"pubsub/internal/status"
)

// The router builds two JSON documents per delivery: the record for the
// deliveries topic and the pending status record written beside it. Together
// they are the whole per-delivery cost of the fan-out, which is what
// stage=process measures - 50-52 ms for a 500-record batch in drain6, ~43% of
// the cycle at full batches and ~20 us per delivery once the 4.86 fan-out is
// divided out.
//
// These exist to say where that 20 us goes rather than to assume it is the
// marshal. A1 KiB payload matches the harness's -payload 1024.
func benchEvent() []byte {
	payload := strings.Repeat("x", 1024)
	return json.RawMessage(`{"specversion":"1.0","type":"bench.type.0",` +
		`"source":"urn:bench","id":"018f0000-0000-7000-8000-000000000000",` +
		`"time":"2026-11-05T20:00:00Z","datacontenttype":"application/json",` +
		`"subject":"bench-subject","data":{"d":"` + payload + `"}}`)
}

func benchMessage(raw json.RawMessage) *delivery.Message {
	now := time.Unix(1793900000, 0).UTC()
	return &delivery.Message{
		DeliveryID:     "018f0000-0000-7000-8000-000000000000/11111111-1111-1111-1111-111111111111",
		EventRef:       "018f0000-0000-7000-8000-000000000000",
		SubscriptionID: "11111111-1111-1111-1111-111111111111",
		Owner:          "bench-owner",
		EventType:      "bench.type.0",
		Channel:        "webhook",
		RetentionClass: "short",
		OrderingKey:    "bench-subject",
		SrcPartition:   4,
		SrcOffset:      4113,
		CreatedAt:      now,
		IngestedAt:     now.Add(-200 * time.Millisecond),
		ExpiresAt:      now.Add(time.Hour),
		Projected:      false,
		EventRaw:       raw,
	}
}

func benchStatus() status.Record {
	return status.Record{
		TS:             time.Unix(1793900000, 0).UTC(),
		Owner:          "bench-owner",
		EventRef:       "018f0000-0000-7000-8000-000000000000",
		DeliveryID:     "018f0000-0000-7000-8000-000000000000/11111111-1111-1111-1111-111111111111",
		SubscriptionID: "11111111-1111-1111-1111-111111111111",
		Status:         status.Pending,
		Reason:         "router",
		RetentionClass: "short",
		SrcPartition:   4,
		SrcOffset:      4113,
	}
}

// BenchmarkDeliveryEncode is the deliveries-topic record: the envelope plus a
// verbatim copy of the event bytes.
func BenchmarkDeliveryEncode(b *testing.B) {
	raw := benchEvent()
	msg := benchMessage(raw)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.Encode(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStatusEncode is the pending status record written beside it.
func BenchmarkStatusEncode(b *testing.B) {
	srec := benchStatus()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(srec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerDelivery is both documents, i.e. what the fan-out pays once per
// matched subscription.
func BenchmarkPerDelivery(b *testing.B) {
	raw := benchEvent()
	msg := benchMessage(raw)
	srec := benchStatus()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := msg.Encode(); err != nil {
			b.Fatal(err)
		}
		if _, err := json.Marshal(srec); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMessageAlloc is the envelope struct plus its id, to separate
// allocation from serialisation in the figure above.
func BenchmarkMessageAlloc(b *testing.B) {
	raw := benchEvent()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := benchMessage(raw)
		m.DeliveryID = m.EventRef + "/" + m.SubscriptionID
		if m.EventRaw == nil {
			b.Fatal("nil")
		}
	}
}

// BenchmarkParse is the per-event decode that opens Process, once per record
// and never memoised - the other half of stage=process besides the per-delivery
// encodes above.
func BenchmarkParse(b *testing.B) {
	raw := benchEvent()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cloudevents.Parse(raw); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEventMarshal is rawMemo's value: json.Marshal of the event, done
// once per event (memo across the subscriptions that share it, not across
// events - every record in the batch has its own pointer).
func BenchmarkEventMarshal(b *testing.B) {
	ev, err := cloudevents.Parse(benchEvent())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(ev); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkEventJSON is the lazy decode() path: when any candidate carries a
// data filter, Process calls e.JSON() once per event to get map[string]any.
// The seeded subscriptions put 14 of 40 behind data filters, so this runs on
// every event. It is inside the process.filter sub-stage, which measured 9.3
// ms per batch - the largest unknown in that stage.
func BenchmarkEventJSON(b *testing.B) {
	ev, err := cloudevents.Parse(benchEvent())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ev.JSON(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAttributes is the filter index built once per event for CEL
// evaluation - five fixed fields plus every extension, into a fresh map.
func BenchmarkAttributes(b *testing.B) {
	ev, err := cloudevents.Parse(benchEvent())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if attrs := attributes(ev); len(attrs) == 0 {
			b.Fatal("no attrs")
		}
	}
}
