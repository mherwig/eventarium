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
	"pubsub/internal/offsetgate"
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

	// parkRetryQueue bounds the hand-off to the park-retry worker. When it is
	// full the caller blocks, which is deliberate: the alternative is
	// committing an offset for a record that nobody holds any more.
	parkRetryQueue = 1024
	// parkRetryMax bounds what the retry worker keeps for itself, for the same
	// reason. A store that stays unreachable has to turn into backpressure on
	// the poll loop, not into memory - the offsets stay uncommitted either way
	// and Kafka keeps the records.
	parkRetryMax = 4096
	// parkRetryTick is how often a park that failed is attempted again.
	parkRetryTick = 2 * time.Second
	// parkLogEvery throttles the park-failure log: parking runs at ingestion
	// speed and again every retry tick, so one line per attempt would drown the
	// log exactly while the store is down.
	parkLogEvery = 30 * time.Second
)

// accept is the decision of the submit path.
type accept int

const (
	acceptQueued accept = iota
	acceptPark
	acceptDropQueue
)

// parkRetry is a delivery whose park failed: the store did not take the row,
// so the record's offset has to stay uncommitted and this job owns the
// delivery until the row is in the inbox (spec 6.6). Retrying instead of
// dropping is what keeps the partition's commit point from standing still for
// the lifetime of the process.
type parkRetry struct {
	s      *subState
	it     *item
	reason string
}

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
	// knownLoaded is the registry state the last refreshKnown covered: a
	// reload newer than this one means the drainer's webhook set may already
	// be outdated.
	knownLoaded time.Time

	workers sync.WaitGroup

	// gate holds the commit watermark of the deliveries topic: a record's
	// offset joins it when the poll returns the record, and is released only
	// when the delivery reached a durable outcome (internal/offsetgate).
	gate *offsetgate.Gate
	// parkRetry hands deliveries whose park failed to the background worker
	// that keeps trying until the store takes them.
	parkRetry chan *parkRetry

	// parkLogErr/parkLogAt throttle the park-failure warning: parking runs at
	// ingestion speed and again every retry tick (parkLogEvery).
	parkLogMu  sync.Mutex
	parkLogErr string
	parkLogAt  time.Time

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
	// The gate is built first because the consumer's assignment callback needs
	// it: a rebalance can hand a partition to another replica while this one
	// still holds records of it, and the gate has to stop tracking them - a
	// late settle must not move an offset somebody else now owns.
	gate := offsetgate.New()
	cl, err := kafkautil.NewClient(
		kgo.ConsumerGroup(consumerGroup),
		kgo.AutoCommitMarks(),
		kgo.ConsumeTopics(kafkautil.TopicDeliveries),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			gate.Keep(assigned)
		}),
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
		gate:         gate,
		parkRetry:    make(chan *parkRetry, parkRetryQueue),
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
	go d.parkRetryLoop()
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

// ownsChannel reports whether this stage handles the record. Section 6.4: it
// follows the subscription's current channel, not the stamp alone - an update
// that moved the subscription has to move the deliveries already on the topic
// with it - and it falls back to the stamp for an ID the registry does not
// know, so an orphaned webhook delivery is still dropped as
// unknown_subscription while an orphaned SSE delivery stays with the
// sse-gateway. registry.ChannelFor is the single implementation of that rule,
// because the sse-gateway asks the same question of the same data: a rule
// that differed between the two stages could let both skip a record (lost) or
// both take it (duplicated).
func (d *Dispatcher) ownsChannel(msg *delivery.Message) bool {
	return d.reg.ChannelFor(msg.SubscriptionID, msg.Channel) == delivery.ChannelWebhook
}

// handleRecord dispatches one deliveries-topic record. The record joins the
// commit gate first and its offset is released only when the delivery is
// durable: owned by the in-memory state until it reaches a terminal outcome,
// parked in the inbox, or terminally dropped. A record that is only in memory
// - queued, retrying, waiting for a park to succeed - keeps its partition's
// commit point where it is, so a crash replays it instead of skipping it
// (section 8: duplicates are identified by the event reference).
func (d *Dispatcher) handleRecord(rec *kgo.Record) {
	// Before anything else: the gate may only decide about a record it knows,
	// and a record it never learns about would be invisible to the commit
	// point, which could then step over it.
	d.gate.Track(rec)
	msg, err := delivery.Decode(rec.Value)
	if err != nil {
		slog.Warn("dispatcher: undecodable delivery, skipped",
			"partition", rec.Partition, "offset", rec.Offset, "err", err)
		d.settle(rec.Partition, rec.Offset)
		return
	}
	it := &item{msg: msg, seq: delivery.Seq{Partition: rec.Partition, Offset: rec.Offset}}
	// The gate runs before the registry lookup, so a subscription that no
	// longer exists keeps its stamped channel (see ownsChannel).
	if !d.ownsChannel(msg) {
		// SSE deliveries belong to the sse-delivery stage; skip and commit.
		d.settle(rec.Partition, rec.Offset)
		return
	}
	entry, ok := d.reg.Subscription(msg.SubscriptionID)
	if !ok || entry.Sub == nil {
		slog.Warn("dispatcher: unknown subscription, delivery dropped",
			"subscription", msg.SubscriptionID, "delivery", msg.DeliveryID)
		d.emitStatus(it, status.Dropped, reasonUnknownSub, 0, 0, 0)
		d.count.dropped.Add(1)
		d.settle(rec.Partition, rec.Offset)
		return
	}
	if entry.Sub.Channel != delivery.ChannelWebhook {
		// The registry reloaded between the gate and this lookup and the
		// subscription moved again in between. Acting now would combine the
		// channel of one snapshot with the policy of another, so leave the
		// record to the stage that owns it in the snapshot held here.
		d.settle(rec.Partition, rec.Offset)
		return
	}
	s, how, reason := d.submit(entry.Sub, it)
	switch how {
	case acceptQueued:
		// The worker releases the offset in finish() when the item reaches a
		// terminal state. Marking here - what this used to do - commits a
		// record that a shutdown would then abort from its queue: the offset
		// was already committed while the delivery was only in memory.
	case acceptPark:
		if reason == reasonCircuitOpen {
			// The breaker opening pauses the delivery that cannot be attempted.
			d.emitStatus(it, status.Paused, reason, 0, 0, 0)
		}
		if d.parkOrRetry(d.ctx, s, it, reason) {
			d.settle(rec.Partition, rec.Offset)
		}
	case acceptDropQueue:
		d.emitStatus(it, status.Dropped, reason, 0, 0, 0)
		d.count.dropped.Add(1)
		d.settle(rec.Partition, rec.Offset)
	}
}

// settle releases a record's offset: the delivery behind it reached a durable
// outcome, so the commit point of its partition may move up to the next record
// that has not. The dispatcher consumes the deliveries topic alone, hence the
// topic name; a settle for something the gate does not know (an inbox row, a
// partition a rebalance took away) is a no-op.
func (d *Dispatcher) settle(partition int32, offset int64) {
	if heads := d.gate.Settle(kafkautil.TopicDeliveries, partition, offset); len(heads) > 0 {
		d.cl.MarkCommitOffsets(heads)
	}
}

// parkOrRetry parks the delivery and reports whether it is durable. A failed
// park does not fail the record: the item goes to the background retry, which
// parks it as soon as the store answers and releases the offset then. Until
// that happens the partition's commit point stands still, so a crash replays
// the record - and if this process never comes back, that replay is the
// recovery.
func (d *Dispatcher) parkOrRetry(ctx context.Context, s *subState, it *item, reason string) bool {
	if d.park(ctx, s, it, reason) {
		return true
	}
	select {
	case d.parkRetry <- &parkRetry{s: s, it: it, reason: reason}:
	case <-ctx.Done():
		// Shutdown: nobody will park it in this process. The offset stays
		// uncommitted and the record is replayed after the restart.
	}
	return false
}

// finish settles the offset of an item that left the attempt loop - unless the
// record is not durable yet, which is exactly the case the offset must not
// move for.
func (d *Dispatcher) finish(it *item, out outcome) {
	if it.fromInbox {
		return // an inbox row carries no offset of its own; the row is the durability
	}
	switch out {
	case outcomeAborted:
		// Shutdown: the record is neither delivered nor parked, so its offset
		// stays uncommitted and whoever consumes the partition next replays it.
	case outcomeParkRetry:
		// The background retry owns it and settles the offset once the row is
		// in the inbox.
	default:
		// delivered, dead-lettered, dropped, expired, parked: all durable.
		d.settle(it.seq.Partition, it.seq.Offset)
	}
}

// parkRetryLoop keeps trying the parks that failed. A store outage must not
// cost data and must not cost the process its memory either: the queue that
// feeds this loop and the items it holds itself are both bounded, so an
// unreachable PostgreSQL turns into backpressure on the poll loop, where the
// offsets are already staying put and Kafka keeps the records.
func (d *Dispatcher) parkRetryLoop() {
	tick := time.NewTicker(parkRetryTick)
	defer tick.Stop()
	var pending []*parkRetry
	var warned bool
	for {
		if len(pending) < parkRetryMax {
			select {
			case <-d.ctx.Done():
				return
			case p := <-d.parkRetry:
				pending = append(pending, p)
				continue
			case <-tick.C:
			}
		} else {
			// Full: stop accepting and let the channel block the callers.
			select {
			case <-d.ctx.Done():
				return
			case <-tick.C:
			}
		}
		if d.ctx.Err() != nil {
			return // the items are not durable; their offsets stay uncommitted
		}
		kept, left := pending[:0], 0
		for _, p := range pending {
			if d.park(d.ctx, p.s, p.it, p.reason) {
				d.settle(p.it.seq.Partition, p.it.seq.Offset)
				continue
			}
			kept = append(kept, p)
			left++
		}
		pending = kept
		switch {
		case left > 0 && !warned:
			slog.Warn("dispatcher: parking is failing, retries queued",
				"waiting", left, "retry", parkRetryTick.String())
			warned = true
		case left == 0 && warned:
			slog.Info("dispatcher: parking recovered, retries drained")
			warned = false
		}
	}
}

// logParkFailure reports a failed park without drowning the log: parking runs
// at ingestion speed and again every retry tick, so an unreachable store would
// otherwise put one line per delivery into it. One line per distinct error and
// never more often than parkLogEvery - the pattern the drainer's lease check
// uses for the same reason.
func (d *Dispatcher) logParkFailure(sub string, seq delivery.Seq, err error) {
	d.parkLogMu.Lock()
	defer d.parkLogMu.Unlock()
	text := err.Error()
	if text == d.parkLogErr && time.Since(d.parkLogAt) < parkLogEvery {
		return
	}
	d.parkLogErr, d.parkLogAt = text, time.Now()
	slog.Warn("dispatcher: parking failed",
		"subscription", sub, "seq", seq.String(), "err", err)
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
// pending with the reason. Returns false when the park failed: the caller then
// hands the item to the background retry (parkOrRetry) instead of releasing
// its offset, because a record that is neither delivered nor parked must stay
// uncommitted.
func (d *Dispatcher) park(ctx context.Context, s *subState, it *item, reason string) bool {
	if !it.msg.ExpiresAt.IsZero() && !time.Now().Before(it.msg.ExpiresAt) {
		d.emitStatus(it, status.Expired, reasonExpired, 0, 0, 0)
		return true
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	// Spec 16: the backlog cap trades guaranteed delivery for bounded storage;
	// without it the inbox grows without bound, with it dropped appears.
	// The check reads the cached counter (store.ParkCounter) instead of
	// counting the inbox per delivery: a count over a full inbox costs about
	// 10 ms, and parking runs at ingestion speed for as long as the endpoint
	// is down. Until the cache knows the rows it inherited there is no cap -
	// the same degradation a failed count had.
	if err := s.parked.Ensure(pctx, d.st, s.id); err != nil {
		slog.Warn("dispatcher: counting parked rows for the cap failed",
			"subscription", s.id, "err", err)
	}
	if s.parked.OverCap(int64(s.policy().backlogMax)) {
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
		d.logParkFailure(s.id, it.seq, err)
		return false
	}
	s.parked.Add(1) // local delta until the drainer's next count replaces it
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
			// Records whose offset may not be committed yet: queued, retrying
			// or waiting for a park. A number that only grows means the commit
			// points are standing still while Kafka keeps the records.
			"offsets_in_flight", d.gate.Pending(),
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
