package dispatch

import (
	"context"
	"log/slog"
	"time"

	"pubsub/internal/status"
)

// emitter decouples the workers from the status topic (section 9: status is
// emitted asynchronously, off the hot path). Records go through a buffered
// channel and are produced in small batches; when the buffer is full the
// record is dropped with a warning rather than blocking a delivery.
type emitter struct {
	p    *status.Producer
	ch   chan status.Record
	stop chan struct{}
	done chan struct{}
}

const (
	emitterBuffer = 8192
	emitterBatch  = 512
	emitterTick   = 50 * time.Millisecond
)

func newEmitter(p *status.Producer) *emitter {
	e := &emitter{
		p:    p,
		ch:   make(chan status.Record, emitterBuffer),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go e.run()
	return e
}

// Emit queues the records without blocking.
func (e *emitter) Emit(recs ...status.Record) {
	for _, r := range recs {
		select {
		case e.ch <- r:
		default:
			slog.Warn("dispatcher: status emitter full, record dropped",
				"delivery", r.DeliveryID, "status", r.Status)
		}
	}
}

// Close flushes the remaining records and stops the emitter.
func (e *emitter) Close() {
	close(e.stop)
	<-e.done
}

func (e *emitter) run() {
	defer close(e.done)
	batch := make([]status.Record, 0, emitterBatch)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := e.p.Produce(ctx, batch...); err != nil {
			slog.Warn("dispatcher: status produce failed", "err", err, "records", len(batch))
		}
		cancel()
		batch = batch[:0]
	}
	tick := time.NewTicker(emitterTick)
	defer tick.Stop()
	for {
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
				flush()
			}
		case <-tick.C:
			flush()
		case <-e.stop:
			for {
				select {
				case r := <-e.ch:
					batch = append(batch, r)
					if len(batch) >= emitterBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}
