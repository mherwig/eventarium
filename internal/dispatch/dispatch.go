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
	"pubsub/internal/otelmetric"
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
	// Metrics supplies the meter the instruments are built from. Nil means
	// none, which is what every test passes: METRICS_ENABLED=false reaches the
	// same place through a noop meter, so call sites never branch on it.
	Metrics *otelmetric.Metrics
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
	// parkBatchTimeout bounds one batch's round trip. Larger than a single
	// insert needs because a batch is up to parkRetryMax rows in chunks of
	// 200; a timeout is safe rather than expensive - store.Park writes with
	// ON CONFLICT DO NOTHING, so whatever landed is skipped when the batch
	// retries, and nothing is counted durable until it returns success.
	parkBatchTimeout = 5 * time.Second

	// revokeDrain bounds how long a revoke waits for the deliveries this member
	// is still holding for the partitions it is giving up. Waiting is what
	// keeps two pods from delivering the same subscription at once: franz-go
	// does not hand the partitions to another member until this callback
	// returns, so a callback that returns immediately says "I am done with
	// these" while its workers are still delivering them. Two pods then deliver
	// one subscription at once, which for delivery_mode ordered means two
	// single-worker pods interleaving - the ordering guarantee broken by
	// exactly the mechanism meant to preserve it.
	//
	// The bound is small because the cost of getting it wrong is asymmetric in
	// the opposite direction from the benefit. This runs inside the group's
	// revoke phase and franz-go's assign path waits on it before resuming
	// fetches, so every millisecond here is a millisecond this member is not
	// consuming. During a broker recovery, where rebalances arrive with a
	// backlog behind them, a generous bound becomes partitions nobody reads
	// from: the first version of this used 10s against a 15s session timeout
	// (kafkautil.DefaultSessionTimeout, pinned by
	// TestSessionSecondsFallbackMatchesDefault) and the chaos gate lost 16-20k
	// deliveries per scenario from exactly that stall. 2s covers the measured
	// p99 delivery time (1.1-1.5s) and leaves the rest of the session budget to
	// heartbeats, the commit and the rejoin.
	//
	// It also has to be logged on success, not only when it expires: warning
	// only on timeout made a drain that succeeded after nine seconds
	// indistinguishable from one that returned immediately, which is exactly
	// how a stall hides.
	revokeDrain = 2 * time.Second
	// revokeDrainTick is how often the drain re-checks: short enough that the
	// last delivery of a quiet partition is not waited on for longer than it
	// took.
	revokeDrainTick = 25 * time.Millisecond
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
	// instruments is nil when Metrics was not supplied; every observe method
	// checks, so the send path never branches on a feature flag.
	instruments *instruments
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
	// The gate is built first because the group callbacks need it: a rebalance
	// can hand a partition to another replica while this one still holds
	// records of it, and the gate has to stop tracking them - a late settle
	// must not move an offset somebody else now owns.
	gate := offsetgate.New()
	d := &Dispatcher{
		reg:          cfg.Registry,
		st:           cfg.Store,
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
	// The client is built after d so the revoke hook can be a method on it
	// rather than a closure over a variable assigned later: the group goroutine
	// reads that hook as soon as the first poll starts, and a value racing that
	// read is the sort of thing every test passes and -race does not. Nothing
	// can call back into d before Run polls, Run is called after New returns,
	// and Shutdown closes the client last, so d.cl is set before onRevoked can
	// run.
	//
	// The hook is OnPartitionsRevoked rather than OnPartitionsAssigned. This
	// is the callback handed the partitions actually being taken away - see
	// offsetgate.Drop for why the assigned one is not - and it is the last
	// point at which this member can still hold up the rebalance.
	cl, err := kafkautil.NewClient(
		kgo.ConsumerGroup(consumerGroup),
		kgo.AutoCommitMarks(),
		kgo.ConsumeTopics(kafkautil.TopicDeliveries),
		kgo.OnPartitionsRevoked(d.onRevoked),
	)
	if err != nil {
		return nil, err
	}
	d.cl = cl

	// Instruments last: the scrape-time gauges read d.subs and d.gate,
	// and both exist by now.
	if err := d.setupInstruments(cfg.Metrics); err != nil {
		return nil, err
	}
	return d, nil
}

// onRevoked is the group's revoke hook: it waits out the deliveries this
// member still holds for the partitions it is losing, stops tracking them, and
// commits - in that order, and only then returns to let the rebalance proceed.
//
// The wait is the point. franz-go does not hand the partitions to another
// member until every member's revoke callback has returned, so a callback that
// returns immediately says "I am done with these" while its workers are still
// delivering them. Two pods then deliver the same subscription at once, which
// is the one thing delivery_mode ordered cannot survive: policy.go gives that
// mode a single worker per pod, so the config is honoured only by data
// locality - one subscription's records sit on one partition, one partition
// goes to one pod - and an overlapping handover reintroduces exactly the
// parallelism the config exists to forbid.
//
// Nothing here can deadlock. The poll loop keeps running (BlockRebalanceOnPoll
// is deliberately not set), so workers never wait on this callback, and
// franz-go invalidates the revoked partitions' buffered fetches before calling
// us, so the drain is emptying a set that is no longer being added to. The
// bound exists for the two paths where it genuinely could not finish: a
// shutdown, where ctx is already cancelled and the first check exits, and a
// store that is down and holding parks open.
func (d *Dispatcher) onRevoked(ctx context.Context, cl *kgo.Client, revoked map[string][]int32) {
	began := time.Now()
	partitions := 0
	for _, ps := range revoked {
		partitions += len(ps)
	}

	left := drainPartitions(ctx, d.gate, revoked, revokeDrain, revokeDrainTick)
	drainTook := time.Since(began)

	// Stop tracking before committing: from here a settle for these partitions
	// is a no-op, which is what stops this member from committing an offset
	// somebody else now owns.
	d.observeRevoke(drainTook, left)
	d.gate.Drop(revoked)

	// This replaces franz-go's default revoke, which with AutoCommitMarks is a
	// blocking commit of every marked offset. Omitting it would leave the last
	// marks uncommitted and make the new owner replay them - safe, but a
	// regression in duplicates for no reason. CommitMarkedOffsets commits the
	// same set defaultRevoke does (both read getUncommitted, and it reaches the
	// same g.commitOffsetsSync), and it is the variant kgo documents as safe to
	// call from here: an ordinary commit would be cancelled by the rebalance
	// that is in progress.
	//
	// The drain can observe the last record of a partition already out of the
	// gate while settle's MarkCommitOffsets is still the next statement to run,
	// so this commit can miss that one mark. The cost is one replay by the new
	// owner - a duplicate, which section 8 permits - and never a loss, because
	// the mark it misses is for a delivery that already happened.
	commitErr := ""
	commitTook := time.Duration(0)
	if ctx.Err() != nil {
		commitErr = "client context cancelled (shutdown)"
	} else {
		start := time.Now()
		if err := cl.CommitMarkedOffsets(ctx); err != nil {
			commitErr = err.Error()
		}
		commitTook = time.Since(start)
	}

	// Logged on every revoke, fast ones included. The stall this is here to
	// catch was invisible in the first version: the only signal was the
	// timeout warning, so a drain that succeeded after nine seconds looked
	// exactly like one that returned immediately - and the gate failed with
	// 16-20k missing deliveries while the logs stayed silent.
	slog.Info("dispatcher: revoke",
		"partitions", partitions,
		"drain_ms", drainTook.Milliseconds(),
		"left", left,
		"commit_ms", commitTook.Milliseconds(),
		"commit_err", commitErr)

	if left > 0 {
		// The handover goes ahead regardless: the group will not wait forever,
		// and refusing to return would turn one slow delivery into a member
		// that gets evicted. Say how much overlap was accepted rather than
		// letting it look like a clean handoff.
		slog.Warn("dispatcher: partitions revoked with deliveries still in flight",
			"left", left, "waited", drainTook.Round(time.Millisecond),
			"note", "these may be delivered again by the new owner")
	}
}

// drainPartitions waits until no record of the named partitions is still in
// flight, and returns how many were left when it stopped waiting: zero means
// every delivery for those partitions reached a durable outcome before the
// partition changed hands.
//
// It reads the gate rather than the subscription queues on purpose. What has
// to be finished is "this Kafka record reached an outcome", and that is
// precisely what the gate tracks - a record queued, being attempted, or
// waiting on a park is still pending, while one that was skipped or already
// parked is not. Counting queue entries instead would wait on deliveries the
// gate has already released and would not see the ones it has not.
func drainPartitions(ctx context.Context, g *offsetgate.Gate, want map[string][]int32, timeout, tick time.Duration) int {
	deadline := time.Now().Add(timeout)
	for {
		n := g.PendingIn(want)
		if n == 0 {
			return 0
		}
		if ctx.Err() != nil || !time.Now().Before(deadline) {
			return n
		}
		select {
		case <-time.After(tick):
		case <-ctx.Done():
			return g.PendingIn(want)
		}
	}
}

// Run consumes the deliveries topic until ctx is cancelled.
func (d *Dispatcher) Run(parent context.Context) {
	ctx, cancel := context.WithCancel(parent)
	d.ctx, d.cancel = ctx, cancel
	go d.heartbeat()
	go d.drainLoop()
	go d.parkRetryLoop()
	// Stage timings, split the same way the router's are: how long the poll
	// blocked waiting for Kafka against how long the records spent being
	// handled. Only the second can be this loop's own cost - a large poll with
	// a small handle says the dispatcher is waiting on the broker, a large
	// handle says it is the serial path (decode, registry lookups, submit's
	// mutex) and nothing else will make that faster. handle_us_per_record is
	// the number the throughput ceiling divides into: at ~150 us the loop tops
	// out near 6,700 records/s, which is the measured ceiling.
	var batches, records int
	var sumPoll, sumHandle, sumCycle time.Duration
	var lastPoll time.Time
	for {
		pollStart := time.Now()
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
		pollDur := time.Since(pollStart)
		var cycle time.Duration
		if !lastPoll.IsZero() {
			cycle = pollStart.Sub(lastPoll)
		}
		lastPoll = pollStart

		handleStart := time.Now()
		recs.EachRecord(func(rec *kgo.Record) {
			d.handleRecord(rec)
		})
		handleDur := time.Since(handleStart)

		n := recs.NumRecords()
		batches++
		records += n
		sumPoll += pollDur
		sumHandle += handleDur
		sumCycle += cycle
		if batches >= 50 {
			perRecord := int64(0)
			if records > 0 {
				perRecord = sumHandle.Microseconds() / int64(records)
			}
			slog.Info("dispatcher: stage timings",
				"poll_ms", sumPoll.Milliseconds()/int64(batches),
				"handle_ms", sumHandle.Milliseconds()/int64(batches),
				"cycle_ms", sumCycle.Milliseconds()/int64(batches),
				"handle_us_per_record", perRecord,
				"batches", batches, "records", records)
			batches, records = 0, 0
			sumPoll, sumHandle, sumCycle = 0, 0, 0
		}
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
		d.record(msg, &d.count.dropped, "dropped")
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
		d.record(msg, &d.count.dropped, "dropped")
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

// parkOrRetry hands the delivery to the background parker and reports whether
// it is already durable - which is now never, and that is the point.
//
// It used to attempt the park inline and only queue it when that attempt
// failed. The inline attempt is a synchronous PostgreSQL INSERT on the poll
// goroutine, and that goroutine is shared by every subscription the instance
// owns, so one subscription's overflow stalled all of them. Measured at
// 1,500 ev/s: the single dispatcher whose partitions carried the three
// rate-limited subscriptions ran one batch at 402 ms with a per-record high
// of 1,599 us, while the other two instances never exceeded 4 ms and 72 us in
// the same window. Removing the rate limits - same 40 subscriptions, same
// fan-out, 18% more unlimited deliveries - took those unlimited subscriptions'
// tail from p99 15.1 s / max 25.6 s to p99 415 ms / max 558 ms, and consumer
// lag from 62,028 to 4,509 and falling.
//
// Parking is not worse off in the queue: parkRetryLoop writes the row and
// settles the offset only after the write succeeds, so nothing is released
// early; it preserves arrival order (channel, then the slice in order); and
// it drains as soon as an item arrives rather than waiting out its tick. Its
// capacity is 4,096 pending against a measured peak arrival of ~116 parks/s.
//
// Returning false means "not durable yet", so the caller must NOT settle:
// the offset stays in flight until parkRetryLoop has the row, which is what
// makes a crash replay the record instead of losing it.
func (d *Dispatcher) parkOrRetry(ctx context.Context, s *subState, it *item, reason string) bool {
	// Timed from before the send: a channel with room returns immediately, so
	// anything material here means the queue was full and the poll goroutine
	// waited for the parker to drain it. That is the backpressure the bound
	// exists for - and, because this goroutine is shared by every subscription
	// on the instance, the stall the inline-park path was removed for causing.
	start := time.Now()
	select {
	case d.parkRetry <- &parkRetry{s: s, it: it, reason: reason}:
	case <-ctx.Done():
		// Shutdown: nobody will park it in this process. The offset stays
		// uncommitted and the record is replayed after the restart.
	}
	if d.instruments != nil {
		d.instruments.parkHandoff.Record(context.Background(), time.Since(start).Seconds())
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
		// One round trip for the whole batch rather than one per item - see
		// parkBatch for why that distinction decides whether this loop can
		// keep up with a burst.
		done, failed := d.parkBatch(d.ctx, pending)
		for _, p := range done {
			d.settle(p.it.seq.Partition, p.it.seq.Offset)
		}
		pending = failed
		left := len(failed)
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
// parkBatch parks a group of items in one round trip rather than one each.
//
// Why this is a batch instead of a loop over the single-item path: a park cost
// 6.896 ms end to end when measured across 6,273 parks, and 99.98% of that -
// 6.885 ms - was the single-row insert. The parker's service rate was
// therefore 145 parks/s against a burst arrival of 164-182 parks/s: it fell
// behind, pending grew to parkRetryMax, the parker stopped draining,
// parkRetryQueue filled, and the blocking send in parkOrRetry stalled the
// shared poll goroutine for 7-18 s. Every subscription on that instance then
// showed a pending -> in_flight tail of 12-21 s where the run without parks
// showed 752 ms - one subscription's overflow becoming everyone's problem,
// which is the same thing the inline park was removed for causing.
//
// store.Park already chunks at 200, so batching is caller-side: the inbox key
// is (subscription_id, part, off, expires_at) and the insert uses ON CONFLICT
// DO NOTHING, so a retry after a partial failure writes nothing twice.
//
// Durability is not decided any earlier than before. An item comes back as
// done only once its row is in the table, or when it is terminal for another
// reason (expired, over the cap, unencodable); the caller releases offsets for
// those and only those.
func (d *Dispatcher) parkBatch(ctx context.Context, batch []*parkRetry) (done, retry []*parkRetry) {
	if len(batch) == 0 {
		return nil, nil
	}
	pctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), parkBatchTimeout)
	defer cancel()

	type slot struct {
		p   *parkRetry
		row store.InboxRow
	}
	var (
		ready []slot
		// capped holds items that would exceed backlog_max once the rows being
		// written actually land. They cannot be dropped at check time: if the
		// insert fails the counter never advances, they are still eligible,
		// and dropping them would lose deliveries the cap had room for.
		capped []*parkRetry
		// The cap resolved once per subscription per batch. OverCap is
		// max > 0 && Ready && Total() >= max, so an unready counter or a max
		// of 0 means no cap at all - 'limited' records which have one.
		avail    = make(map[*subState]int64, 4)
		limited  = make(map[*subState]bool, 4)
		resolved = make(map[*subState]bool, 4)
	)

	for _, p := range batch {
		it := p.it
		// Terminal without I/O: an expired event is never parked, exactly as
		// the single-item path reported it.
		if !it.msg.ExpiresAt.IsZero() && !time.Now().Before(it.msg.ExpiresAt) {
			d.emitStatus(it, status.Expired, reasonExpired, 0, 0, 0)
			done = append(done, p)
			continue
		}
		s := p.s
		if !resolved[s] {
			resolved[s] = true
			// Spec 16: the backlog cap trades guaranteed delivery for bounded
			// storage; without it the inbox grows without bound, with it
			// dropped appears. The check reads the cached counter
			// (store.ParkCounter) instead of counting the inbox per delivery:
			// a count over a full inbox costs about 10 ms, and parking runs at
			// ingestion speed for as long as the endpoint is down. Until the
			// cache knows the rows it inherited there is no cap - the same
			// degradation a failed count had.
			tCount := time.Now()
			if err := s.parked.Ensure(pctx, d.st, s.id); err != nil {
				slog.Warn("dispatcher: counting parked rows for the cap failed",
					"subscription", s.id, "err", err)
			}
			d.observeParkStage("count", time.Since(tCount))
			if max := int64(s.policy().backlogMax); max > 0 && s.parked.Ready() {
				limited[s] = true
				avail[s] = max - s.parked.Total()
			}
		}
		if limited[s] && avail[s] <= 0 {
			capped = append(capped, p)
			continue
		}
		tEncode := time.Now()
		payload, err := it.msg.Encode()
		d.observeParkStage("encode", time.Since(tEncode))
		if err != nil {
			slog.Warn("dispatcher: encoding parked payload failed", "err", err)
			d.emitStatus(it, status.Dropped, reasonParkFailed, 0, 0, 0)
			d.record(it.msg, &d.count.dropped, "dropped")
			done = append(done, p)
			continue
		}
		if limited[s] {
			avail[s]-- // reserve, so this batch cannot overshoot the cap
		}
		ready = append(ready, slot{p: p, row: store.InboxRow{
			SubscriptionID: s.id,
			Part:           it.seq.Partition,
			Offset:         it.seq.Offset,
			Payload:        payload,
			Reason:         p.reason,
			ExpiresAt:      it.msg.ExpiresAt,
		}})
	}

	if len(ready) > 0 {
		rows := make([]store.InboxRow, len(ready))
		for i, r := range ready {
			rows[i] = r.row
		}
		tInsert := time.Now()
		err := d.st.Park(pctx, rows)
		// Amortised over the rows it carried, so all three stages mean "per
		// park" and stay comparable with the single-item measurements.
		d.observeParkStage("insert", time.Since(tInsert)/time.Duration(len(rows)))
		if err != nil {
			first := ready[0]
			d.logParkFailure(first.p.s.id, first.p.it.seq, err)
			// Nothing that needed a write is durable. Rows that did land are
			// skipped by ON CONFLICT on the retry, and no counter advanced, so
			// the reservations are void and the capped items are eligible too.
			for _, r := range ready {
				retry = append(retry, r.p)
			}
			return done, append(retry, capped...)
		}
		// Counters only now that the rows exist: the cap must not advance for
		// an insert that did not happen.
		for _, r := range ready {
			r.p.s.parked.Add(1)
			d.emitStatus(r.p.it, status.Pending, r.p.reason, 0, 0, 0)
			d.record(r.p.it.msg, &d.count.parked, "parked")
			done = append(done, r.p)
		}
	}
	// Over-cap items are durable once the rows that reached the cap landed -
	// and when nothing was pending to reach it with, Total() is already at the
	// limit, so they are over the cap now either way.
	for _, p := range capped {
		d.emitStatus(p.it, status.Dropped, reasonBacklogMax, 0, 0, 0)
		d.record(p.it.msg, &d.count.dropped, "dropped")
		done = append(done, p)
	}
	return done, retry
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
		limitedDepth, unlimitedDepth := 0, 0
		deepest, deepestSub := 0, "-"
		for id, s := range d.subs {
			states++
			var sd int
			for _, q := range s.qs {
				sd += len(q)
			}
			depth += sd
			// Split by rate limit: the three rate-limited subscriptions are
			// *supposed* to queue (50/s against an arrival rate many times
			// that), so a total that says "10,000 queued" without this split
			// cannot be read as a fault. What matters for the ceiling is
			// whether the unlimited ones are queueing too - they have no
			// reason to, and if they are, their deliveries are the tail.
			if s.policy().rateLimitPerS > 0 {
				limitedDepth += sd
			} else {
				unlimitedDepth += sd
			}
			if sd > deepest {
				deepest, deepestSub = sd, id
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
			"queue_rate_limited", limitedDepth,
			"queue_unlimited", unlimitedDepth,
			"deepest_queue", deepest,
			"deepest_sub", deepestSub,
			"subscriptions", states,
			// Records whose offset may not be committed yet: queued, retrying
			// or waiting for a park. A number that only grows means the commit
			// points are standing still while Kafka keeps the records.
			"offsets_in_flight", d.gate.Pending(),
			// Status records that never reached the topic: dropped because the
			// emitter's buffer filled during a broker outage, or given up at
			// shutdown. Deliveries are unaffected either way (the offset gate,
			// not the status record, is what makes them durable) - but a
			// non-zero and growing number says the Status API is falling
			// behind reality and replay selectors will miss those deliveries.
			"status_lost", d.emitter.lostCount(),
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
