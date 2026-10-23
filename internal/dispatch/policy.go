package dispatch

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"pubsub/internal/delivery"
	"pubsub/internal/registry"
)

// Policy defaults (delivery policy table of spec section 7). The registry API
// applies the same defaults at creation time; the dispatcher re-applies them
// so that rows written before a default was chosen still work.
const (
	defaultMaxInFlight  = 8
	defaultCBThreshold  = 5
	defaultProbeS       = 300 * time.Second
	defaultRequestTO    = 5 * time.Second
	defaultBacklogMax   = 10000
	defaultDrainRate    = 100
	webhookFormatBinary = "binary"
)

// maxQueueCap bounds the in-memory queue regardless of backlog_max: the inbox
// is the overflow buffer, RAM should not be.
const maxQueueCap = 10000

// policy is the effective delivery policy of one subscription, derived from
// the registry entry.
type policy struct {
	mode               string
	workers            int
	callbackURL        string
	format             string
	rateLimitPerS      int
	burst              int
	drainRatePerS      int
	requestTimeout     time.Duration
	backlogMax         int
	cbFailureThreshold int
	probeInterval      time.Duration
}

// policyOf applies the defaults of the delivery policy table (section 7).
func policyOf(sub *registry.Subscription) policy {
	p := policy{
		mode:               sub.DeliveryMode,
		callbackURL:        sub.CallbackURL,
		format:             strings.ToLower(strings.TrimSpace(sub.WebhookFormat)),
		rateLimitPerS:      sub.RateLimitPerS,
		burst:              sub.Burst,
		drainRatePerS:      sub.DrainRatePerS,
		requestTimeout:     time.Duration(sub.RequestTimeoutMS) * time.Millisecond,
		backlogMax:         sub.BacklogMax,
		cbFailureThreshold: sub.CBFailureThreshold,
		probeInterval:      time.Duration(sub.ProbeIntervalS) * time.Second,
	}
	if p.mode == "" {
		p.mode = delivery.ModeOrdered
	}
	if p.mode == delivery.ModeOrdered {
		p.workers = 1
	} else {
		p.workers = sub.MaxInFlight
		if p.workers <= 0 {
			p.workers = defaultMaxInFlight
		}
	}
	if p.rateLimitPerS < 0 {
		p.rateLimitPerS = 0
	}
	if p.burst <= 0 {
		p.burst = max(p.rateLimitPerS, 1)
	}
	if p.drainRatePerS <= 0 {
		p.drainRatePerS = max(p.rateLimitPerS, defaultDrainRate)
	}
	if p.requestTimeout <= 0 {
		p.requestTimeout = defaultRequestTO
	}
	if p.backlogMax <= 0 {
		p.backlogMax = defaultBacklogMax
	}
	if p.cbFailureThreshold <= 0 {
		p.cbFailureThreshold = defaultCBThreshold
	}
	if p.probeInterval <= 0 {
		p.probeInterval = defaultProbeS
	}
	if p.format == "" {
		p.format = webhookFormatBinary
	}
	return p
}

// queueCap is the total in-memory queue size: min(backlog_max, 10000).
func (p policy) queueCap() int { return min(p.backlogMax, maxQueueCap) }

// Retry policy (spec section 8): exponential backoff with jitter, base 500 ms,
// factor 2, capped at 60 s.
const (
	retryBase = 500 * time.Millisecond
	retryCap  = 60 * time.Second
)

// backoff returns the delay before attempt n (1-based): the exponential value
// spread over [d/2, d] so that concurrent workers do not retry in lockstep.
func backoff(attempt uint16) time.Duration {
	d := retryBase
	for i := uint16(1); i < attempt && d < retryCap; i++ {
		d *= 2
	}
	if d > retryCap {
		d = retryCap
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(rand.Int64N(int64(half)))
}

// sleepCtx waits for d or until ctx is done; false means cancelled.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
