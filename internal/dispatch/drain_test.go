package dispatch

import (
	"testing"
	"time"
)

// newDrainState builds the per-subscription state the drainer inspects: one
// queue, a closed breaker, no pause, nothing in flight.
func newDrainState() *subState {
	return &subState{
		id:      "00000000-0000-0000-0000-000000000001",
		qs:      []chan *item{make(chan *item, 1)},
		breaker: newBreaker("test", 5, time.Minute),
	}
}

// TestDrainEligible pins the gate drainOnce applies before it reads the inbox:
// only a subscription that could actually attempt a parked row may cost a
// FetchParked call (section 6.6). The breaker-open case is the one that used
// to pull up to drainBatch payloads twice a second only to discard them.
func TestDrainEligible(t *testing.T) {
	healthy := func() *subState { return newDrainState() }

	openBreaker := func() *subState {
		s := newDrainState()
		s.breaker.open()
		return s
	}

	probeDue := func() *subState {
		s := newDrainState()
		s.breaker.open()
		s.breaker.mu.Lock()
		s.breaker.openUntil = time.Now().Add(-time.Second) // probe interval elapsed
		s.breaker.mu.Unlock()
		return s
	}

	probing := func() *subState {
		s := newDrainState()
		s.breaker.open()
		s.breaker.mu.Lock()
		s.breaker.state = cbHalfOpen // a probe is already running
		s.breaker.mu.Unlock()
		return s
	}

	paused := func() *subState {
		s := newDrainState()
		s.pause(time.Minute) // 429 Retry-After
		return s
	}

	inFlight := func() *subState {
		s := newDrainState()
		s.inFlight.Store(1) // a live delivery is being attempted
		return s
	}

	queued := func() *subState {
		s := newDrainState()
		s.qs[0] <- &item{} // work waiting on the hot path
		return s
	}

	tests := []struct {
		name string
		want bool
		make func() *subState
	}{
		{"healthy", true, healthy},
		{"breaker open", false, openBreaker},
		{"probe due", true, probeDue},
		{"probe already running", false, probing},
		{"429 pause active", false, paused},
		{"live work in flight", false, inFlight},
		{"delivery queued", false, queued},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := drainEligible(tt.make()); got != tt.want {
				t.Errorf("drainEligible() = %v, want %v", got, tt.want)
			}
		})
	}
}
