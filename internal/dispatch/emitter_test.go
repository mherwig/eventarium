package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"pubsub/internal/status"
)

// fakeStatusProducer records what it was handed and fails the first n calls.
type fakeStatusProducer struct {
	mu      sync.Mutex
	fail    int // calls still to fail
	got     []status.Record
	calls   int
	proceed chan struct{} // closed once the first successful produce happened
}

func (f *fakeStatusProducer) Produce(_ context.Context, recs ...status.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail > 0 {
		f.fail--
		return errors.New("broker refusing (test)")
	}
	f.got = append(f.got, recs...)
	if f.proceed != nil {
		select {
		case <-f.proceed:
		default:
			close(f.proceed)
		}
	}
	return nil
}

func (f *fakeStatusProducer) received() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.got)
}

func rec(id string) status.Record {
	return status.Record{DeliveryID: id, Status: status.Delivered, RetentionClass: "short"}
}

// TestEmitterRetriesAFailedBatch is the change: a produce error must cost a
// retry, not the records. The batch the broker refused is handed over intact
// on the next tick, and nothing is counted as lost.
func TestEmitterRetriesAFailedBatch(t *testing.T) {
	f := &fakeStatusProducer{fail: 3, proceed: make(chan struct{})}
	e := newEmitter(f)
	defer e.Close()

	// One record: it sits in the buffer until the next tick flushes a partial
	// batch, which is where the failures and the retries happen.
	e.Emit(rec("d1"), rec("d2"), rec("d3"))

	select {
	case <-f.proceed:
	case <-time.After(5 * time.Second):
		t.Fatalf("batch was never produced: calls=%d lost=%d", f.calls, e.lostCount())
	}
	// Give the emitter a moment to settle before Close (a second flush of an
	// empty batch must not resurrect or duplicate anything).
	time.Sleep(2 * emitterTick)

	f.mu.Lock()
	got := append([]status.Record(nil), f.got...)
	f.mu.Unlock()

	if len(got) != 3 {
		t.Fatalf("produced %d records, want 3 (a held batch must arrive whole)", len(got))
	}
	for i, want := range []string{"d1", "d2", "d3"} {
		if got[i].DeliveryID != want {
			t.Errorf("record %d = %q, want %q (order must survive the retry)", i, got[i].DeliveryID, want)
		}
	}
	if e.lostCount() != 0 {
		t.Errorf("lost = %d, want 0: a retried batch is not a lost one", e.lostCount())
	}
	if f.calls < 4 {
		t.Errorf("producer was called %d times, want at least 4 (3 failures + 1 success)", f.calls)
	}
}

// TestEmitterStopsTakingWorkWhileHeld is what bounds the memory: while the
// batch is unproven the loop must not keep draining the channel into it,
// otherwise a broker that stays down grows the heap without limit. The
// pressure belongs on the buffer, where Emit drops.
func TestEmitterStopsTakingWorkWhileHeld(t *testing.T) {
	f := &fakeStatusProducer{fail: 1 << 30} // never succeeds
	e := newEmitter(f)
	defer e.Close()
	e.stopTimeout = 100 * time.Millisecond

	// Fill exactly one batch so the first flush fails and the loop goes held.
	for i := 0; i < emitterBatch; i++ {
		e.Emit(rec("a"))
	}
	// Wait for that attempt to happen.
	deadline := time.Now().Add(2 * time.Second)
	for f.calls == 0 && time.Now().Before(deadline) {
		time.Sleep(emitterTick)
	}
	if f.calls == 0 {
		t.Fatal("first flush never happened")
	}

	// Now flood. The buffer must take what room is left and drop the rest:
	// if the held loop were still draining, these would vanish into an
	// unbounded slice instead of being counted here.
	before := int(e.lostCount())
	flood := emitterBuffer + 100
	for i := 0; i < flood; i++ {
		e.Emit(rec("b"))
	}
	dropped := int(e.lostCount()) - before
	if dropped == 0 {
		t.Fatal("expected the buffer to overflow and count the drops")
	}
	if dropped > flood {
		t.Errorf("dropped %d of %d flooded records - more than were offered?", dropped, flood)
	}
	// The held batch itself must still be intact for its retry.
	if got := f.received(); got != 0 {
		t.Errorf("producer received %d records despite failing every call", got)
	}
}

// TestEmitterCloseGivesUpAfterTheDeadline keeps shutdown honest: Close must
// return even when the broker never takes the records, and what it could not
// write is counted rather than silently forgotten.
func TestEmitterCloseGivesUpAfterTheDeadline(t *testing.T) {
	f := &fakeStatusProducer{fail: 1 << 30}
	e := newEmitter(f)
	e.stopTimeout = 150 * time.Millisecond

	e.Emit(rec("x"), rec("y"))
	start := time.Now()
	e.Close()
	elapsed := time.Since(start)

	if elapsed > 2*time.Second {
		t.Errorf("Close took %v, want it bounded near stopTimeout (%v)", elapsed, e.stopTimeout)
	}
	if e.lostCount() == 0 {
		t.Error("lost = 0, want the unflushable records counted")
	}
}

// TestEmitterCloseFlushesWhatItHeld covers the other side of Close: a broker
// that recovers during shutdown still gets its records. The graceful path is
// the common one, and it must not be sacrificed to the deadline.
func TestEmitterCloseFlushesWhatItHeld(t *testing.T) {
	f := &fakeStatusProducer{fail: 2}
	e := newEmitter(f)

	e.Emit(rec("d1"), rec("d2"))
	e.Close() // retries inside drain until the two calls fail and the third works

	if got := f.received(); got != 2 {
		t.Errorf("produced %d of 2 records across Close", got)
	}
	if e.lostCount() != 0 {
		t.Errorf("lost = %d, want 0", e.lostCount())
	}
}
