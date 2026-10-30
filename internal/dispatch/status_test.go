package dispatch

import (
	"testing"
	"time"

	"pubsub/internal/delivery"
	"pubsub/internal/status"
)

// TestEmitStatusStampsTheEventPosition is the regression test for the stamp
// the chaos run found: SrcPartition/SrcOffset must name where the *event*
// lives in the events topic, because that is what the router stamps into
// `pending`, what the Status API returns per record and what
// replay resolve.go reads back with argMin(src_partition, ts) to re-read the
// original event.
//
// The dispatcher has a second coordinate at hand - it.seq, the delivery's own
// position on the deliveries topic - and emitStatus used to stamp that one
// instead. Nothing failed loudly: replay kept working only because the
// router's `pending` row has the earliest ts and therefore won argMin, while
// every later row of the same delivery reported a different source.
func TestEmitStatusStampsTheEventPosition(t *testing.T) {
	// No run() goroutine: records are read straight off the channel, so the
	// test asserts what was emitted rather than what a broker received.
	e := &emitter{
		ch:   make(chan status.Record, 4),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	d := &Dispatcher{emitter: e}

	it := &item{
		msg: &delivery.Message{
			DeliveryID:     "ref/sub",
			EventRef:       "ref",
			SubscriptionID: "sub",
			RetentionClass: "short",
			// Where the event actually is: events topic, partition 4, offset 4113.
			SrcPartition: 4,
			SrcOffset:    4113,
		},
		// Where the delivery is: deliveries topic, partition 2, offset 133500.
		seq: delivery.Seq{Partition: 2, Offset: 133500},
	}

	d.emitStatus(it, status.Delivered, "", 1, 200, 5*time.Millisecond)

	select {
	case rec := <-e.ch:
		if rec.SrcPartition != 4 || rec.SrcOffset != 4113 {
			t.Errorf("status carries source %d:%d, want the event position 4:4113 - "+
				"the deliveries-topic sequence would send replay to the wrong record",
				rec.SrcPartition, rec.SrcOffset)
		}
	default:
		t.Fatal("emitStatus produced no record")
	}
}

// TestEmitStatusKeepsTheInboxSequenceOutOfTheSourceFields covers the drained
// path: an item built from an inbox row carries (part, offset) of the parked
// deliveries record, which is the coordinate that must never leak into the
// source fields either.
func TestEmitStatusKeepsTheInboxSequenceOutOfTheSourceFields(t *testing.T) {
	e := &emitter{ch: make(chan status.Record, 4), stop: make(chan struct{}), done: make(chan struct{})}
	d := &Dispatcher{emitter: e}

	it := &item{
		msg: &delivery.Message{
			DeliveryID: "ref/sub", EventRef: "ref", SubscriptionID: "sub",
			SrcPartition: 7, SrcOffset: 83785,
		},
		seq:       delivery.Seq{Partition: 11, Offset: 61491},
		fromInbox: true,
	}

	d.emitStatus(it, status.Delivered, "", 3, 200, 9*time.Millisecond)

	select {
	case rec := <-e.ch:
		if rec.SrcPartition != 7 || rec.SrcOffset != 83785 {
			t.Errorf("status carries source %d:%d, want 7:83785", rec.SrcPartition, rec.SrcOffset)
		}
	default:
		t.Fatal("emitStatus produced no record")
	}
}
