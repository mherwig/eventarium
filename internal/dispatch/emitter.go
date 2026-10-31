package dispatch

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"pubsub/internal/status"
)

// statusProducer is what the emitter needs of the status producer. Naming the
// interface lets a test drive the retry loop without a broker.
type statusProducer interface {
	Produce(ctx context.Context, recs ...status.Record) error
}

// emitter decouples the workers from the status topic (section 9: status is
// emitted asynchronously, off the hot path). Records go through a buffered
// channel and are produced in small batches.
//
// A batch the broker did not take is held for the next attempt instead of
// being dropped. These records are the *history* of deliveries that already
// reached a durable outcome, and discarding them on a transient produce error
// would leave the Status API reporting a stale current status forever -
// in_flight on a delivery that succeeded - for a fault that had nothing to do
// with the delivery itself. The offset gate stays out of it: the delivery is
// durable whether or not the status reaches the topic (dispatch.go,
// offsetgate), so retrying costs only this batch and a little memory.
//
// Holding is bounded by the same numbers as before. While a batch is
// unproven the loop takes no new work, so the pressure lands on ch, where
// Emit drops rather than blocking a delivery - unchanged behaviour, and now
// the only way a record is lost besides a shutdown whose deadline expires.
// Both count into lost, which the dispatcher heartbeat reports.
type emitter struct {
	p    statusProducer
	ch   chan status.Record
	stop chan struct{}
	done chan struct{}

	// lost counts records that could not be written: dropped because the
	// buffer was full, or given up at shutdown. Read by the heartbeat from
	// another goroutine, hence atomic.
	lost atomic.Uint64

	// stopTimeout bounds the final flush of Close so a broker that is still
	// down cannot hold a shutdown open. A field rather than a constant so a
	// test can prove Close returns.
	stopTimeout time.Duration

	// State of the run goroutine only, for the throttled warning and the
	// recovery line: a line per attempt would bury the log exactly while the
	// broker is down (the pattern of parkLogFailure and of the registry's
	// reload loop - first failure, a change of the error, the recovery).
	failErr string
	failAt  time.Time
	failed  int

	// dropMu guards the overflow warning's throttle state, which Emit writes
	// from whichever worker goroutine happens to be full.
	dropMu       sync.Mutex
	dropLastWarn time.Time
}

const (
	emitterBuffer = 8192
	emitterBatch  = 512
	emitterTick   = 50 * time.Millisecond
	// emitterRetryTimeout bounds one attempt to hand a batch over, so a hung
	// broker costs a tick rather than the emitter.
	emitterRetryTimeout = 10 * time.Second
	// emitterStopTimeout bounds Close's last flush. Shorter than the
	// dispatcher's shutdownGrace on purpose: this runs *after* the workers
	// stopped, and an unflushable batch is reported as lost rather than
	// waited out forever.
	emitterStopTimeout = 5 * time.Second
	// emitterRetryLogEvery throttles a warning that would otherwise repeat
	// every tick for as long as the same error lasts.
	emitterRetryLogEvery = 30 * time.Second
)

func newEmitter(p statusProducer) *emitter {
	e := &emitter{
		p:           p,
		ch:          make(chan status.Record, emitterBuffer),
		stop:        make(chan struct{}),
		done:        make(chan struct{}),
		stopTimeout: emitterStopTimeout,
	}
	go e.run()
	return e
}

// Emit queues the records without blocking. A full buffer drops them, which
// the delivery path prefers over waiting on the status topic - and now counts.
func (e *emitter) Emit(recs ...status.Record) {
	for _, r := range recs {
		select {
		case e.ch <- r:
		default:
			e.lost.Add(1)
			e.logDrop()
		}
	}
}

// logDrop throttles the overflow warning: parking and delivery run at
// ingestion speed, so a full buffer during a broker outage would otherwise
// write one line per record. The running total carries what the throttle
// hides.
func (e *emitter) logDrop() {
	e.dropMu.Lock()
	defer e.dropMu.Unlock()
	if time.Since(e.dropLastWarn) < emitterRetryLogEvery {
		return
	}
	e.dropLastWarn = time.Now()
	slog.Warn("dispatcher: status emitter full, records dropped",
		"lost_total", e.lost.Load())
}

// Close flushes the remaining records and stops the emitter. What it cannot
// flush before stopTimeout is counted as lost and reported - a shutdown must
// not block on a broker that is not answering.
func (e *emitter) Close() {
	close(e.stop)
	<-e.done
}

// lostCount is the number of status records that could not be written, for
// the heartbeat.
func (e *emitter) lostCount() uint64 { return e.lost.Load() }

func (e *emitter) run() {
	defer close(e.done)
	batch := make([]status.Record, 0, emitterBatch)
	tick := time.NewTicker(emitterTick)
	defer tick.Stop()

	// held: the last flush failed and batch still holds its records. While
	// that is true the loop waits for the next tick instead of taking work,
	// which is what keeps memory bounded - see the type comment.
	held := false

	for {
		if held {
			select {
			case <-tick.C:
				held = !e.flush(&batch)
			case <-e.stop:
				e.drain(&batch)
				return
			}
			continue
		}
		select {
		case r := <-e.ch:
			batch = append(batch, r)
		drainLoop:
			for len(batch) < emitterBatch {
				select {
				case r2 := <-e.ch:
					batch = append(batch, r2)
				default:
					break drainLoop
				}
			}
			if len(batch) >= emitterBatch {
				held = !e.flush(&batch)
			}
		case <-tick.C:
			held = !e.flush(&batch)
		case <-e.stop:
			e.drain(&batch)
			return
		}
	}
}

// flush hands the batch to the producer and reports whether it is gone. It
// takes the batch by pointer because clearing it is part of "gone": the run
// loop keeps no reset of its own, so a success that left the slice standing
// would hand the same records to the next flush - duplication, which the
// retry test caught as 3 records arriving as 9.
//
// On failure the batch is left exactly as it is, for the next attempt -
// never cleared and never partly cleared, so a retry cannot duplicate records
// that did reach the topic either.
func (e *emitter) flush(batch *[]status.Record) bool {
	if len(*batch) == 0 {
		return true
	}
	ctx, cancel := context.WithTimeout(context.Background(), emitterRetryTimeout)
	defer cancel()
	if err := e.p.Produce(ctx, *batch...); err != nil {
		e.failed++
		text := err.Error()
		if text != e.failErr || time.Since(e.failAt) >= emitterRetryLogEvery {
			e.failErr, e.failAt = text, time.Now()
			slog.Warn("dispatcher: status produce failed, batch held for retry",
				"err", err, "records", len(*batch), "attempts", e.failed)
		}
		return false
	}
	if e.failed > 0 {
		slog.Info("dispatcher: status produce recovered",
			"attempts", e.failed, "records", len(*batch))
		e.failErr, e.failed = "", 0
	}
	*batch = (*batch)[:0]
	return true
}

// drain takes whatever is left and keeps trying until it is written or the
// deadline expires. It runs from Close, so it is the last chance for these
// records - and the last place a shutdown can stall.
func (e *emitter) drain(batch *[]status.Record) {
	deadline := time.Now().Add(e.stopTimeout)
	for {
		// Take everything the channel still holds, up to one batch.
		for len(*batch) < emitterBatch {
			select {
			case r := <-e.ch:
				*batch = append(*batch, r)
			default:
				goto flush
			}
		}
	flush:
		if len(*batch) == 0 {
			return // channel empty and nothing held
		}
		if e.flush(batch) {
			if len(e.ch) == 0 {
				return
			}
			continue
		}
		if time.Now().After(deadline) {
			n := len(*batch) + len(e.ch)
			e.lost.Add(uint64(n))
			slog.Error("dispatcher: status emitter gave up on shutdown",
				"records", n, "last_err", e.failErr, "lost_total", e.lost.Load())
			return
		}
		time.Sleep(emitterTick)
	}
}
