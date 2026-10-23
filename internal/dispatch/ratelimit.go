package dispatch

import (
	"context"
	"sync"
	"time"
)

// tokenBucket is the per-subscription rate limiter of spec 6.4, kept in
// memory by the partition owner so no shared counter is needed. A rate of 0
// means unlimited. It is safe for concurrent use by the workers of one
// subscription.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second; 0 = unlimited
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate, burst int) *tokenBucket {
	b := &tokenBucket{}
	b.set(rate, burst)
	return b
}

// set applies refreshed policy values without losing accumulated tokens.
func (b *tokenBucket) set(rate, burst int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	b.refill(now)
	if rate < 0 {
		rate = 0
	}
	if burst <= 0 {
		burst = rate
		if burst <= 0 {
			burst = 1
		}
	}
	b.rate = float64(rate)
	b.burst = float64(burst)
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
}

func (b *tokenBucket) refill(now time.Time) {
	if b.last.IsZero() {
		b.last = now
		b.tokens = b.burst
		return
	}
	if b.rate <= 0 {
		return
	}
	elapsed := now.Sub(b.last).Seconds()
	if elapsed <= 0 {
		return
	}
	b.tokens += elapsed * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
}

// Wait blocks until one token has been taken. deadline > 0 gives up after that
// duration, reporting saturation (false, nil); a cancelled context reports
// (false, ctx.Err()). An unlimited bucket always returns immediately.
func (b *tokenBucket) Wait(ctx context.Context, deadline time.Duration) (bool, error) {
	var until time.Time
	if deadline > 0 {
		until = time.Now().Add(deadline)
	}
	for {
		b.mu.Lock()
		if b.rate <= 0 {
			b.mu.Unlock()
			return true, nil
		}
		now := time.Now()
		b.refill(now)
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return true, nil
		}
		wait := time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
		b.mu.Unlock()
		if !until.IsZero() && now.Add(wait).After(until) {
			return false, nil
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}
