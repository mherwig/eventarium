package dispatch

import (
	"log/slog"
	"sync"
	"time"
)

// Circuit breaker states (spec 6.4).
const (
	cbClosed int = iota
	cbOpen
	cbHalfOpen
)

// Spec 6.4 caps the probe interval: 5 to 15 minutes, never days.
const (
	minProbeInterval = 5 * time.Minute
	maxProbeInterval = 15 * time.Minute
)

// breaker is the per-subscription circuit breaker. It opens after threshold
// consecutive failures, stays open for one probe interval and then lets a
// single probe through: a successful probe closes it, a failed one re-opens it
// for another interval. A 429 is neither (see attemptLoop).
type breaker struct {
	id            string
	mu            sync.Mutex
	failures      int
	threshold     int
	probeInterval time.Duration
	state         int
	openUntil     time.Time
	probing       bool
}

func newBreaker(id string, threshold int, probe time.Duration) *breaker {
	if threshold <= 0 {
		threshold = 5
	}
	return &breaker{id: id, threshold: threshold, probeInterval: clampProbe(probe)}
}

func clampProbe(d time.Duration) time.Duration {
	if d <= 0 {
		return minProbeInterval
	}
	if d < minProbeInterval {
		return minProbeInterval
	}
	if d > maxProbeInterval {
		return maxProbeInterval
	}
	return d
}

// setPolicy applies refreshed policy values.
func (b *breaker) setPolicy(threshold int, probe time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if threshold > 0 {
		b.threshold = threshold
	}
	b.probeInterval = clampProbe(probe)
}

// allow reports whether an attempt may run now. The open-to-half-open
// transition happens here: the first caller after the probe interval becomes
// the probe and is allowed through, everyone else waits for its outcome.
func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case cbClosed:
		return true
	case cbOpen:
		if time.Now().Before(b.openUntil) {
			return false
		}
		b.state = cbHalfOpen
		b.probing = true
		return true
	default: // cbHalfOpen: one probe at a time
		return false
	}
}

// canAttempt is a side-effect-free peek used by the drainer: healthy means
// closed, or open with the probe interval elapsed (the drained backlog then
// provides the probe).
func (b *breaker) canAttempt() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == cbClosed {
		return true
	}
	return b.state == cbOpen && !time.Now().Before(b.openUntil)
}

// isClosed reports whether the breaker is fully closed.
func (b *breaker) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state == cbClosed
}

// success records a successful attempt.
func (b *breaker) success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == cbOpen {
		// A late success of an attempt started before the breaker opened;
		// only a probe may close the breaker.
		return
	}
	if b.state == cbHalfOpen {
		slog.Info("dispatcher: circuit breaker closed after successful probe", "subscription", b.id)
	}
	b.state = cbClosed
	b.probing = false
	b.failures = 0
	b.openUntil = time.Time{}
}

// failure records a failed attempt; threshold consecutive failures open the
// breaker, a failed probe re-opens it for another interval.
func (b *breaker) failure() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == cbHalfOpen {
		b.open()
		slog.Info("dispatcher: circuit breaker re-opened after failed probe",
			"subscription", b.id, "next_probe", b.probeInterval)
		return
	}
	if b.state == cbOpen {
		// A late failure of an attempt started before the breaker opened;
		// the interval already runs, do not extend it.
		return
	}
	b.failures++
	if b.failures >= b.threshold {
		b.open()
		slog.Info("dispatcher: circuit breaker opened",
			"subscription", b.id, "failures", b.failures, "probe_interval", b.probeInterval)
	}
}

func (b *breaker) open() {
	b.state = cbOpen
	b.probing = false
	b.openUntil = time.Now().Add(b.probeInterval)
}
