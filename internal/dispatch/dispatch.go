// Package dispatch implements the webhook dispatcher of the spike: spec
// sections 6.4 (webhook delivery, circuit breaker, rate limiting, security),
// 6.6 (inbox parking and draining), 7 (delivery modes and the per-subscription
// policy table), 8 (failure handling and retries), 9 (delivery status),
// 12 (retention classes; live events are never parked) and 16 (the backlog
// cap trades guaranteed delivery for bounded storage).
//
// The dispatcher consumes the deliveries topic in consumer group "dispatcher",
// keeps one state object per subscription (bounded queue, token bucket,
// circuit breaker) and POSTs each CloudEvent to the subscription's callback
// URL in the CloudEvents binary or structured binding. Deliveries that cannot
// right now are parked in the PostgreSQL inbox and drained back in order once
// the subscription is healthy again; every transition is reported to the
// delivery-status topic.
package dispatch

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/delivery"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/status"
	"pubsub/internal/store"
)

// Options for New.
type Config struct {
	Registry      *registry.Registry
	Store         *store.Store
	Status        *status.Producer
	MaxAttempts   int    // DISPATCHER_MAX_ATTEMPTS, default 10
	SigningSecret string // WEBHOOK_SIGNING_SECRET, empty = no signature
	AllowPrivate  bool   // ALLOW_PRIVATE_WEBHOOKS; true in the spike so the
	// local harness can receive webhooks on loopback
}

const (
	consumerGroup   = "dispatcher"
	shutdownGrace   = 10 * time.Second
	idlePruneAfter  = 10 * time.Minute
	drainInterval   = 500 * time.Millisecond
	expiryInterval  = 5 * time.Minute
	knownRefresh    = 30 * time.Second
	drainBatch      = 200
	heartbeatEvery  = 10 * time.Second
	defaultAttempts = 10
)

// accept is the decision of the submit path.
type accept int

const (
	acceptQueued accept = iota
	acceptPark
	acceptDropQueue
)

type counters struct {
	delivered    atomic.Uint64
	retrying     atomic.Uint64
	parked       atomic.Uint64
	dropped      atomic.Uint64
	deadLettered atomic.Uint64
}

// Dispatcher owns the consume loop and every per-subscription state.
type Dispatcher struct {
	reg          *registry.Registry
	st           *store.Store
	cl           *kgo.Client
	emitter      *emitter
	httpc        *http.Client
	secret       []byte
	allowPrivate bool
	maxAttempts  int

	mu   sync.Mutex
	subs map[string]*subState
	// known is the set of webhook subscription IDs used by the drainer. The
	// shared registry offers no iteration, so the IDs are snapshotted from
	// PostgreSQL; subscriptions with live traffic are always added from the
	// state map.
	known   map[string]struct{}
	knownAt time.Time

	workers sync.WaitGroup

	ctx    context.Context
	cancel context.CancelFunc
	count  counters
}

// New connects the Kafka consumer and the status emitter.
func New(cfg Config) (*Dispatcher, error) {
	if cfg.Registry == nil || cfg.Store == nil || cfg.Status == nil {
		return nil, errors.New("dispatch: Config requires Registry, Store and Status")
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultAttempts
	}
	cl, err := kafkautil.NewClient(
		kgo.ConsumerGroup(consumerGroup),
		kgo.AutoCommitMarks(),
		kgo.ConsumeTopics(kafkautil.TopicDeliveries),
	)
	if err != nil {
		return nil, err
	}
	d := &Dispatcher{
		reg:          cfg.Registry,
		st:           cfg.Store,
		cl:           cl,
		emitter:      newEmitter(cfg.Status),
		secret:       []byte(cfg.SigningSecret),
		allowPrivate: cfg.AllowPrivate,
		maxAttempts:  cfg.MaxAttempts,
		subs:         map[string]*subState{},
		known:        map[string]struct{}{},
		// No global timeout: every attempt gets its own context timeout
		// (request_timeout_ms). Redirects are never followed (spec 6.4
		// security list); a 3xx counts as an ordinary failure.
		httpc: &http.Client{
			Timeout:       0,
			CheckRedirect: func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
	return d, nil
}

// Run consumes the deliveries topic until ctx is cancelled.
func (d *Dispatcher) Run(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	d.ctx, d.cancel = ctx, cancel
	go d.heartbeat()
	go d.drainLoop()
	for {
		recs := d.cl.PollRecords(ctx, 256)
		if recs.NumRecords() == 0 {
			if err := recs.Err(); err != nil && ctx.Err() == nil {
				slog.Warn("dispatcher: poll failed", "err", err)
				sleepCtx(ctx, time.Second)
			}
			if ctx.Err() != nil {
				return
			}
			continue
		}
		recs.EachRecord(func(rec *kgo.Record) {
			d.handleRecord(rec)
		})
	}
}

// handleRecord dispatches one deliveries-topic record. The offset is marked
// for commit once the delivery is safely owned by the in-memory state, parked
// in the inbox, or terminally dropped; a failed park leaves the offset
// unmarked so the record is redelivered after a restart.
func (d *Dispatcher) handleRecord(rec *kgo.Record) {
	msg, err := delivery.Decode(rec.Value)
	if err != nil {
		slog.Warn("dispatcher: undecodable delivery, skipped",
			"partition", rec.Partition, "offset", rec.Offset, "err", err)
		d.cl.MarkCommitRecords(rec)
		return
	}
	it := &item{msg: msg, seq: delivery.Seq{Partition: rec.Partition, Offset: rec.Offset}}
	if msg.Channel != delivery.ChannelWebhook {
		// SSE deliveries belong to the sse-delivery stage; skip and commit.
		d.cl.MarkCommitRecords(rec)
		return
	}
	entry, ok := d.reg.Subscription(msg.SubscriptionID)
	if !ok || entry.Sub == nil {
		slog.Warn("dispatcher: unknown subscription, delivery dropped",
			"subscription", msg.SubscriptionID, "delivery", msg.DeliveryID)
		d.emitStatus(it, status.Dropped, reasonUnknownSub, 0, 0, 0)
		d.count.dropped.Add(1)
		d.cl.MarkCommitRecords(rec)
		return
	}
	s, how, reason := d.submit(entry.Sub, it)
	switch how {
	case acceptQueued:
		d.cl.MarkCommitRecords(rec)
	case acceptPark:
		if reason == reasonCircuitOpen {
			// The breaker opening pauses the delivery that cannot be attempted.
			d.emitStatus(it, status.Paused, reason, 0, 0, 0)
		}
		if d.park(d.ctx, s, it, reason) {
			d.cl.MarkCommitRecords(rec)
		}
	case acceptDropQueue:
		d.emitStatus(it, status.Dropped, reason, 0, 0, 0)
		d.count.dropped.Add(1)
		d.cl.MarkCommitRecords(rec)
	}
}

// submit creates or fetches the subscription state and takes the enqueue
// decision (spec 6.4/6.6): while draining, with an open breaker, or with a
// full queue, non-live deliveries are parked; live ones (spec 12: no inbox
// duty) stay in memory even when that means dropping after retries.
func (d *Dispatcher) submit(sub *registry.Subscription, it *item) (*subState, accept, string) {
	pol := policyOf(sub)
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.subs[sub.ID]
	if s == nil {
		s = d.newStateLocked(sub, pol)
	} else {
		s.applyPolicyLocked(pol)
	}
	s.lastActive.Store(time.Now().UnixNano())
	live := it.msg.RetentionClass == kafkautil.ClassLive
	if !live {
		if s.draining.Load() {
			// Spec 6.6: while the inbox drains, new events go to the inbox so
			// that relative order holds.
			return s, acceptPark, reasonDraining
		}
		if !s.breaker.isClosed() {
			return s, acceptPark, reasonCircuitOpen
		}
	}
	q := s.pick(it.msg, pol)
	select {
	case q <- it:
		return s, acceptQueued, ""
	default:
		if live {
			return s, acceptDropQueue, reasonQueueFull
		}
		return s, acceptPark, reasonQueueFull
	}
}

// stateFor gets or creates the state without enqueuing (drainer path).
func (d *Dispatcher) stateFor(sub *registry.Subscription) *subState {
	pol := policyOf(sub)
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.subs[sub.ID]
	if s == nil {
		s = d.newStateLocked(sub, pol)
	} else {
		s.applyPolicyLocked(pol)
	}
	s.lastActive.Store(time.Now().UnixNano())
	return s
}

// newStateLocked builds the state and starts its workers. The queue is split
// over the workers: key_ordered needs per-worker queues so that a key keeps
// its affinity. Callers hold d.mu.
func (d *Dispatcher) newStateLocked(sub *registry.Subscription, pol policy) *subState {
	perQueue := max(1, pol.queueCap()/pol.workers)
	s := &subState{
		id:          sub.ID,
		d:           d,
		qs:          make([]chan *item, pol.workers),
		bucket:      newTokenBucket(pol.rateLimitPerS, pol.burst),
		drainBucket: newTokenBucket(pol.drainRatePerS, max(pol.drainRatePerS, 1)),
		breaker:     newBreaker(sub.ID, pol.cbFailureThreshold, pol.probeInterval),
	}
	s.pol = pol
	s.lastActive.Store(time.Now().UnixNano())
	for i := range s.qs {
		s.qs[i] = make(chan *item, perQueue)
	}
	for i := 0; i < pol.workers; i++ {
		d.workers.Add(1)
		go s.work(s.qs[i])
	}
	d.subs[sub.ID] = s
	return s
}

// park moves the delivery into the PostgreSQL inbox (spec 6.6) and emits
// pending with the reason. Returns false when the park failed and the record
// should be redelivered.
func (d *Dispatcher) park(ctx context.Context, s *subState, it *item, reason string) bool {
	if !it.msg.ExpiresAt.IsZero() && !time.Now().Before(it.msg.ExpiresAt) {
		d.emitStatus(it, status.Expired, reasonExpired, 0, 0, 0)
		return true
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	// Spec 16: the backlog cap trades guaranteed delivery for bounded storage;
	// without it the inbox grows without bound, with it dropped appears.
	// The check reads the cached counter (parkCounter) instead of counting the
	// inbox per delivery: a count over a full inbox costs about 10 ms, and
	// parking runs at ingestion speed for as long as the endpoint is down.
	s.parked.ensure(pctx, d.st, s.id)
	if s.parked.overCap(int64(s.policy().backlogMax)) {
		d.emitStatus(it, status.Dropped, reasonBacklogMax, 0, 0, 0)
		d.count.dropped.Add(1)
		return true
	}
	payload, err := it.msg.Encode()
	if err != nil {
		slog.Warn("dispatcher: encoding parked payload failed", "err", err)
		d.emitStatus(it, status.Dropped, reasonParkFailed, 0, 0, 0)
		d.count.dropped.Add(1)
		return true
	}
	row := store.InboxRow{
		SubscriptionID: s.id,
		Part:           it.seq.Partition,
		Offset:         it.seq.Offset,
		Payload:        payload,
		Reason:         reason,
		ExpiresAt:      it.msg.ExpiresAt,
	}
	if err := d.st.Park(pctx, []store.InboxRow{row}); err != nil {
		slog.Warn("dispatcher: parking failed",
			"subscription", s.id, "seq", it.seq.String(), "err", err)
		return false
	}
	s.parked.add(1) // local delta until the drainer's next count replaces it
	d.emitStatus(it, status.Pending, reason, 0, 0, 0)
	d.count.parked.Add(1)
	return true
}

// heartbeat logs the per-stage counters every 10 seconds.
func (d *Dispatcher) heartbeat() {
	t := time.NewTicker(heartbeatEvery)
	defer t.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-t.C:
		}
		d.mu.Lock()
		depth, states := 0, 0
		for _, s := range d.subs {
			states++
			for _, q := range s.qs {
				depth += len(q)
			}
		}
		d.mu.Unlock()
		slog.Info("dispatcher heartbeat",
			"delivered", d.count.delivered.Load(),
			"retrying", d.count.retrying.Load(),
			"parked", d.count.parked.Load(),
			"dropped", d.count.dropped.Load(),
			"dead_lettered", d.count.deadLettered.Load(),
			"queue_depth", depth,
			"subscriptions", states,
		)
	}
}

// prune removes states that have been idle for more than 10 minutes.
func (d *Dispatcher) prune() {
	cutoff := time.Now().Add(-idlePruneAfter).UnixNano()
	d.mu.Lock()
	var dead []*subState
	for id, s := range d.subs {
		empty := true
		for _, q := range s.qs {
			if len(q) > 0 {
				empty = false
				break
			}
		}
		if s.lastActive.Load() < cutoff && empty && s.inFlight.Load() == 0 {
			delete(d.subs, id)
			dead = append(dead, s)
		}
	}
	d.mu.Unlock()
	// All sends happen under d.mu, so no enqueue can race the close.
	for _, s := range dead {
		for _, q := range s.qs {
			close(q)
		}
		slog.Info("dispatcher: pruned idle subscription state", "subscription", s.id)
	}
}

// Shutdown stops the workers, waits (at most 10s) for in-flight attempts and
// flushes the status emitter.
func (d *Dispatcher) Shutdown() {
	if d.cancel != nil {
		d.cancel()
	}
	d.mu.Lock()
	subs := make([]*subState, 0, len(d.subs))
	for _, s := range d.subs {
		subs = append(subs, s)
	}
	d.subs = map[string]*subState{}
	d.mu.Unlock()
	for _, s := range subs {
		for _, q := range s.qs {
			close(q)
		}
	}
	done := make(chan struct{})
	go func() {
		d.workers.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownGrace):
		slog.Warn("dispatcher: workers did not stop within grace period", "grace", shutdownGrace)
	}
	d.emitter.Close()
	d.cl.Close()
}
