package dispatch

import (
	"context"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"pubsub/internal/delivery"
	"pubsub/internal/kafkautil"
	"pubsub/internal/status"
	"pubsub/internal/store"
)

// item is one delivery moving through a subscription's pipeline.
type item struct {
	msg *delivery.Message
	seq delivery.Seq
	// fromInbox marks rows drained from the PostgreSQL inbox: they are never
	// re-parked (they are already there) and use the drain rate limiter.
	fromInbox bool
}

// outcome of one item through the attempt loop.
type outcome int

const (
	outcomeDelivered outcome = iota
	outcomeParked
	// outcomeParkRetry: the park itself failed (the store did not take the
	// row), so the delivery went to the background retry. It is deliberately
	// not outcomeParked - nothing is durable yet and the offset must not move.
	outcomeParkRetry
	outcomeDead
	outcomeDropped
	outcomeExpired
	outcomeAborted
)

// subState is the per-subscription state object (spec 6.4): one bounded queue
// per worker, the shared token bucket, the circuit breaker and the 429 pause.
// It is created on first message and pruned after 10 minutes of idleness.
type subState struct {
	id  string
	d   *Dispatcher
	qs  []chan *item // one queue per worker: key_ordered needs the affinity
	rr  atomic.Uint64
	pol policy // guarded by mu; refreshed whenever a message arrives

	bucket      *tokenBucket // rate_limit_per_s, shared by the workers
	drainBucket *tokenBucket // drain_rate_per_s, used while draining the inbox
	breaker     *breaker
	parked      store.ParkCounter // cached inbox row count behind backlog_max

	mu         sync.Mutex
	pauseUntil time.Time

	draining   atomic.Bool
	inFlight   atomic.Int64
	lastActive atomic.Int64 // unix nanos, updated on submit and per item
}

// policy returns a snapshot of the current policy.
func (s *subState) policy() policy {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pol
}

func (s *subState) applyPolicyLocked(p policy) {
	s.pol = p
	s.bucket.set(p.rateLimitPerS, p.burst)
	s.drainBucket.set(p.drainRatePerS, max(p.drainRatePerS, 1))
	s.breaker.setPolicy(p.cbFailureThreshold, p.probeInterval)
}

// pause extends the subscription-wide pause (429 Retry-After, spec 6.4).
func (s *subState) pause(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	until := time.Now().Add(d)
	if until.After(s.pauseUntil) {
		s.pauseUntil = until
	}
}

func (s *subState) pauseFor() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pauseUntil.IsZero() {
		return 0
	}
	d := time.Until(s.pauseUntil)
	if d < 0 {
		s.pauseUntil = time.Time{}
		return 0
	}
	return d
}

// pick routes a message to a worker queue: key_ordered hashes the ordering key
// so that a key always lands on the same worker (and stays serialised);
// unordered spreads over the least busy queues round-robin; ordered has a
// single queue and therefore strict order with one event in flight.
func (s *subState) pick(msg *delivery.Message, pol policy) chan *item {
	n := len(s.qs)
	if n == 1 {
		return s.qs[0]
	}
	if pol.mode == delivery.ModeKeyOrdered {
		h := fnv.New32a()
		_, _ = h.Write([]byte(msg.OrderingKey))
		return s.qs[int(h.Sum32())%n]
	}
	return s.qs[int(s.rr.Add(1))%n]
}

// work is one delivery worker. Retries with backoff happen in place, so a
// worker that owns an event keeps it (and its ordering position) until the
// event reaches a terminal state. The record's offset is released only then
// (finish): an item that leaves the loop without a durable outcome - shutdown,
// or a park that is being retried in the background - keeps its partition's
// commit point where it is, so the record is replayed rather than lost.
func (s *subState) work(q chan *item) {
	defer s.d.workers.Done()
	for it := range q {
		s.inFlight.Add(1)
		out := s.attemptLoop(s.d.ctx, it, false)
		s.inFlight.Add(-1)
		s.lastActive.Store(time.Now().UnixNano())
		s.d.finish(it, out)
	}
}

// attemptLoop runs one item to a terminal state: delivered, dead-lettered,
// dropped (live only), expired, parked, handed to the park retry (the store
// refused the row), or aborted (shutdown or an inbox row that must stay for
// the next drain round).
func (s *subState) attemptLoop(ctx context.Context, it *item, drain bool) outcome {
	msg := it.msg
	live := msg.RetentionClass == kafkautil.ClassLive
	var attempt uint16
	for {
		if ctx.Err() != nil {
			return outcomeAborted
		}
		if !msg.ExpiresAt.IsZero() && !time.Now().Before(msg.ExpiresAt) {
			s.d.emitStatus(it, status.Expired, reasonExpired, attempt, 0, 0)
			return outcomeExpired
		}
		// 429: the subscription is paused; the event stays in flight and is
		// retried after the pause, consuming neither an attempt nor a
		// breaker failure (spec 6.4).
		if w := s.pauseFor(); w > 0 {
			if !sleepCtx(ctx, w) {
				return outcomeAborted
			}
		}
		if !s.breaker.allow() {
			s.d.emitStatus(it, status.Paused, reasonCircuitOpen, attempt, 0, 0)
			if it.fromInbox {
				return outcomeAborted
			}
			if live {
				// Live events have no inbox duty (spec 12): retry in memory.
				attempt++
				if attempt >= uint16(s.d.maxAttempts) {
					s.d.emitStatus(it, status.Dropped, reasonMaxAttempts, attempt, 0, 0)
					s.d.count.dropped.Add(1)
					return outcomeDropped
				}
				if !sleepCtx(ctx, backoff(attempt)) {
					return outcomeAborted
				}
				continue
			}
			if s.d.parkOrRetry(ctx, s, it, reasonCircuitOpen) {
				return outcomeParked
			}
			return outcomeParkRetry // the background retry owns it now
		}
		// Rate limit: the policy bucket for the hot path, the slower drain
		// bucket for inbox rows.
		bucket, waitCap := s.bucket, rateSaturation
		if drain {
			bucket, waitCap = s.drainBucket, 0
		}
		ok, err := bucket.Wait(ctx, waitCap)
		if err != nil {
			return outcomeAborted
		}
		if !ok {
			// Sustained saturation under load: shed to the inbox.
			if it.fromInbox {
				return outcomeAborted
			}
			if live {
				if !sleepCtx(ctx, retryBase) {
					return outcomeAborted
				}
				continue
			}
			if s.d.parkOrRetry(ctx, s, it, reasonRateLimited) {
				return outcomeParked
			}
			return outcomeParkRetry // the background retry owns it now
		}

		attempt++
		s.d.emitStatus(it, status.InFlight, "", attempt, 0, 0)
		res := s.d.send(ctx, s.policy(), it, attempt)
		switch res.kind {
		case attemptOK:
			s.d.emitStatus(it, status.Delivered, "", attempt, res.code, res.latency)
			s.breaker.success()
			s.d.count.delivered.Add(1)
			return outcomeDelivered
		case attemptGone:
			// 410 Gone: the receiver said stop. The endpoint is alive, so this
			// is not a breaker failure.
			s.d.emitStatus(it, status.DeadLettered, reasonGone, attempt, res.code, res.latency)
			s.breaker.success()
			s.d.count.deadLettered.Add(1)
			return outcomeDead
		case attempt429:
			s.pause(res.retryAfter)
			s.d.emitStatus(it, status.Retrying, reasonHTTP429, attempt, res.code, res.latency)
			s.d.count.retrying.Add(1)
			attempt-- // not an attempt: the event is retried after the pause
		case attemptFailure:
			reason := res.reason
			if reason == "" {
				reason = reasonHTTPError
			}
			if res.err != nil {
				msgText := res.err.Error()
				if len(msgText) > 200 {
					msgText = msgText[:200]
				}
				reason = reason + ": " + msgText
			}
			s.d.emitStatus(it, status.Retrying, reason, attempt, res.code, res.latency)
			s.breaker.failure()
			s.d.count.retrying.Add(1)
			if attempt >= uint16(s.d.maxAttempts) {
				if live {
					s.d.emitStatus(it, status.Dropped, reasonMaxAttempts, attempt, res.code, res.latency)
					s.d.count.dropped.Add(1)
					return outcomeDropped
				}
				s.d.emitStatus(it, status.DeadLettered, reasonMaxAttempts, attempt, res.code, res.latency)
				s.d.count.deadLettered.Add(1)
				return outcomeDead
			}
			if !sleepCtx(ctx, backoff(attempt)) {
				return outcomeAborted
			}
		}
	}
}
