package dispatch

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pubsub/internal/delivery"
	"pubsub/internal/otelmetric"
)

// instruments are the dispatcher's metrics. They live in one struct so the
// send path, the attempt loop and the heartbeat do not each grow a field, and
// so a Dispatcher built without a meter (tests) can hold nil and skip.
type instruments struct {
	// outcome counts what happened to a delivery, per subscription: this is
	// the number that answers "is it failing for me rather than for everyone".
	outcome metric.Int64Counter
	// attempt is the duration of one delivery attempt, per subscription -
	// "is it slow for me". Deliberately without an outcome label: a label on
	// a histogram multiplies the bucket count, and the outcome breakdown is
	// already the counter's job.
	attempt metric.Float64Histogram
	// e2e is publish-api's stamp to the consumer receiving it. Recorded here
	// for webhooks and in the gateway for SSE, so one name covers both.
	e2e metric.Float64Histogram
	// bytes counts payload bytes delivered, so summing it gives traffic
	// rather than a message count.
	bytes metric.Int64Counter
	// queueDepth and offsetsInFlight are read at scrape time rather than
	// accumulated: a backlog is a state, not an event, and a value only
	// meaningful at the moment of a log line would be useless to graph.
	queueDepth      metric.Int64ObservableGauge
	offsetsInFlight metric.Int64ObservableGauge
	// parked is the inbox backlog per subscription: rows parked because the
	// subscription was failing or over its rate limit, waiting to drain. The
	// number that answers "is my subscription backed up?" without a psql.
	parked metric.Int64ObservableGauge
	// revokeDrain and revokeAbandoned watch the handover: how long a revoke
	// held the group, and how often it gave up with deliveries still
	// outstanding. The bound exists to keep one slow delivery from stalling a
	// rebalance, so both halves of that trade have to be visible - a bound
	// that is always hit is not a bound, it is a queue.
	revokeDrain     metric.Float64Histogram
	revokeAbandoned metric.Int64Counter
	// parkQueueDepth and parkHandoff exist to answer one question: does a full
	// park queue stall the shared poll goroutine?
	//
	// parkOrRetry hands a park to a bounded channel, and a send on a full
	// channel blocks the caller - which is the poll goroutine, the one that
	// every subscription on this instance shares. That is deliberate
	// backpressure when PostgreSQL is genuinely unreachable, and it is the
	// same blast radius the inline-park path had when it was removed for
	// stalling exactly that goroutine. The difference is only what fills up:
	// one slow INSERT, or 4,096 pending + 1,024 queued parks.
	//
	// Depth says how close that is; handoff says how long the poll loop
	// actually waited, which is the quantity that would have to explain a
	// tail measured in seconds. A p50 at zero with a fat upper tail is the
	// signature of intermittent stalls rather than steady slowness.
	parkQueueDepth metric.Int64ObservableGauge
	parkHandoff    metric.Float64Histogram
	// parkStage splits park() into where its time actually goes, because the
	// obvious fix - batching the insert, which store.Park already chunks at
	// 200 - only helps if "insert" is the cost. Three stages cover the work:
	// count is the cached inbox lookup (free after the first call, so any
	// weight here means the cache is not holding), encode is CPU on the
	// payload, insert is the PostgreSQL round trip. Splitting them stops
	// "the parker is slow" from being a guess about which of three things it
	// is, and park() is not a path to change on a guess: it owns the offsets.
	parkStage metric.Float64Histogram
}

// newInstruments builds them all. An error here is a programming error in the
// instrument definitions rather than a runtime condition, but it is returned
// rather than panicked so the caller decides.
func newInstruments(meter metric.Meter) (*instruments, error) {
	outcome, err := meter.Int64Counter("pubsub.delivery.outcome",
		metric.WithDescription("Deliveries by terminal and intermediate outcome, per subscription."))
	if err != nil {
		return nil, err
	}
	attempt, err := meter.Float64Histogram("pubsub.delivery.attempt.duration",
		metric.WithUnit("s"),
		otelmetric.WithDurationBuckets(),
		metric.WithDescription("Duration of one delivery attempt, per subscription."))
	if err != nil {
		return nil, err
	}
	e2e, err := meter.Float64Histogram("pubsub.e2e.duration",
		metric.WithUnit("s"),
		otelmetric.WithDurationBuckets(),
		metric.WithDescription("Publish-api's ingest stamp to the consumer receiving the delivery."))
	if err != nil {
		return nil, err
	}
	bytes, err := meter.Int64Counter("pubsub.delivery.bytes",
		metric.WithDescription("Payload bytes handed to consumers, by channel."))
	if err != nil {
		return nil, err
	}
	queueDepth, err := meter.Int64ObservableGauge("pubsub.dispatch.queue.depth",
		metric.WithDescription("Delivery records waiting in a subscription queue, by rate-limit class."))
	if err != nil {
		return nil, err
	}
	offsetsInFlight, err := meter.Int64ObservableGauge("pubsub.dispatch.offsets_in_flight",
		metric.WithDescription("Records polled but not yet durable: queued, retrying or waiting on a park. A value that only grows means commit points are standing still."))
	if err != nil {
		return nil, err
	}
	parked, err := meter.Int64ObservableGauge("pubsub.dispatch.parked",
		metric.WithDescription("Rows in the PostgreSQL inbox for a subscription, waiting to drain."))
	if err != nil {
		return nil, err
	}
	revokeDrain, err := meter.Float64Histogram("pubsub.rebalance.drain.duration",
		metric.WithUnit("s"),
		otelmetric.WithDurationBuckets(),
		metric.WithDescription("How long a revoke waited for the departing partitions' deliveries before handing them over."))
	if err != nil {
		return nil, err
	}
	revokeAbandoned, err := meter.Int64Counter("pubsub.rebalance.drain.abandoned",
		metric.WithDescription("Revokes that reached the bound with deliveries still outstanding; those may be delivered again by the new owner."))
	if err != nil {
		return nil, err
	}
	parkQueueDepth, err := meter.Int64ObservableGauge("pubsub.dispatch.park_queue_depth",
		metric.WithDescription("Parks waiting for the background parker: buffered in the hand-off channel. park_retry_queue (1024) is the blocking bound - a value near it means the poll goroutine is about to wait."))
	if err != nil {
		return nil, err
	}
	parkHandoff, err := meter.Float64Histogram("pubsub.dispatch.park_handoff.duration",
		metric.WithUnit("s"),
		otelmetric.WithDurationBuckets(),
		metric.WithDescription("Time spent handing a park to the background parker. A non-trivial tail means a full channel stalled the shared poll goroutine, which every subscription on this instance depends on."))
	if err != nil {
		return nil, err
	}
	parkStage, err := meter.Float64Histogram("pubsub.dispatch.park_stage.duration",
		metric.WithUnit("s"),
		otelmetric.WithDurationBuckets(),
		metric.WithDescription("Where one park spends its time: count (cached inbox lookup), encode (payload to JSON), insert (the PostgreSQL round trip). Splits the parker's cost so a fix targets the stage that is actually slow."))
	if err != nil {
		return nil, err
	}
	return &instruments{
		outcome:         outcome,
		attempt:         attempt,
		e2e:             e2e,
		bytes:           bytes,
		queueDepth:      queueDepth,
		offsetsInFlight: offsetsInFlight,
		parked:          parked,
		revokeDrain:     revokeDrain,
		revokeAbandoned: revokeAbandoned,
		parkQueueDepth:  parkQueueDepth,
		parkHandoff:     parkHandoff,
		parkStage:       parkStage,
	}, nil
}

// deliveryAttrs is the label set shared by the per-subscription instruments.
// owner and channel are functionally dependent on subscription_id - one
// subscription belongs to one owner and one channel - so adding them costs no
// extra series while still letting a query aggregate by owner.
func deliveryAttrs(msg *delivery.Message) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("owner", msg.Owner),
		attribute.String("subscription_id", msg.SubscriptionID),
		attribute.String("channel", msg.Channel),
	}
}

// observeOutcome records one delivery outcome. The outcome values are the same
// names the heartbeat logs, so a line and a metric can be read together.
func (d *Dispatcher) observeOutcome(msg *delivery.Message, outcome string) {
	if d.instruments == nil {
		return
	}
	d.instruments.outcome.Add(context.Background(), 1,
		metric.WithAttributes(append(deliveryAttrs(msg),
			attribute.String("outcome", outcome))...))
}

// observeAttempt records how long one attempt took.
//
// A zero duration means no round trip happened at all - a pre-flight
// rejection such as a blocked target or an unencodable event - and recording
// it would pull the histogram towards zero for reasons that have nothing to
// do with how fast the endpoint is.
func (d *Dispatcher) observeAttempt(msg *delivery.Message, took time.Duration) {
	if d.instruments == nil || took <= 0 {
		return
	}
	d.instruments.attempt.Record(context.Background(), took.Seconds(),
		metric.WithAttributes(deliveryAttrs(msg)...))
}

// observeE2E records the whole-platform latency for one delivery, from
// publish-api's stamp to the consumer receiving it.
//
// A delivery whose IngestedAt is missing - written by a router that predates
// the field, or with the header stripped - is skipped rather than measured
// against the zero time, which would put a sample of two thousand years on
// the histogram and wreck every percentile after it.
func (d *Dispatcher) observeE2E(msg *delivery.Message) {
	if d.instruments == nil || msg.IngestedAt.IsZero() {
		return
	}
	took := time.Since(msg.IngestedAt)
	if took < 0 {
		return
	}
	d.instruments.e2e.Record(context.Background(), took.Seconds(),
		metric.WithAttributes(attribute.String("channel", msg.Channel)))
}

// observeBytes records the payload size actually handed over.
func (d *Dispatcher) observeBytes(channel string, n int) {
	if d.instruments == nil || n <= 0 {
		return
	}
	d.instruments.bytes.Add(context.Background(), int64(n),
		metric.WithAttributes(attribute.String("channel", channel)))
}

// setupInstruments builds them and registers the scrape-time gauges. A nil
// meter leaves the Dispatcher instrument-free, and every observe method checks
// for that rather than the call sites having to.
func (d *Dispatcher) setupInstruments(m *otelmetric.Metrics) error {
	if m == nil {
		return nil
	}
	instr, err := newInstruments(m.Meter())
	if err != nil {
		return err
	}
	d.instruments = instr
	// Every instrument observeGauges writes to must appear here, or OTel
	// silently never produces it: the callback's instruments are the set it is
	// allowed to fill. Missing one costs nothing visible - no error, no log -
	// just a series that never exists. Two were missing: parked, which meant
	// the dashboard's inbox-backlog panel had been empty since the day it was
	// written and read as "no backlog" rather than "never reported", and the
	// park-queue depth added alongside the hand-off histogram.
	_, err = m.Meter().RegisterCallback(func(_ context.Context, o metric.Observer) error {
		d.observeGauges(o)
		return nil
	}, instr.queueDepth, instr.offsetsInFlight, instr.parked, instr.parkQueueDepth)
	return err
}

// observeParkStage records where one park spent its time. A nil instruments
// (no meter) is the no-op case, so park() never branches on the feature.
func (d *Dispatcher) observeParkStage(stage string, took time.Duration) {
	if d.instruments == nil {
		return
	}
	d.instruments.parkStage.Record(context.Background(), took.Seconds(),
		metric.WithAttributes(attribute.String("stage", stage)))
}

// observeGauges reads the backlog at scrape time. A backlog is a state rather
// than an event: sampling it on a timer, the way the heartbeat logs it, would
// answer "what was the depth ten seconds ago" for every point on the graph.
func (d *Dispatcher) observeGauges(o metric.Observer) {
	if d.instruments == nil {
		return
	}
	var limited, unlimited int
	d.mu.Lock()
	for id, s := range d.subs {
		sd := 0
		for _, q := range s.qs {
			sd += len(q)
		}
		// Not Ready means the count has never been seeded from PostgreSQL
		// since this process started: reporting nothing is more honest than
		// reporting zero, which would read as "no backlog" when it is really
		// "unknown".
		if s.parked.Ready() {
			o.ObserveInt64(d.instruments.parked, s.parked.Total(),
				metric.WithAttributes(attribute.String("subscription_id", id)))
		}
		// Split by rate limit, as the heartbeat does: the rate-limited
		// subscriptions are meant to queue, so a total that cannot tell them
		// apart reads as a fault when it is not one.
		if s.policy().rateLimitPerS > 0 {
			limited += sd
		} else {
			unlimited += sd
		}
	}
	d.mu.Unlock()

	o.ObserveInt64(d.instruments.queueDepth, int64(limited),
		metric.WithAttributes(attribute.String("class", "rate_limited")))
	o.ObserveInt64(d.instruments.queueDepth, int64(unlimited),
		metric.WithAttributes(attribute.String("class", "unlimited")))
	// Outside the mutex: Pending takes the gate's own lock, and there is no
	// reason to hold two while reading a counter.
	o.ObserveInt64(d.instruments.offsetsInFlight, int64(d.gate.Pending()))
	// len() on a channel needs no lock and is safe concurrently. This is the
	// number that decides whether the blocking send in parkOrRetry has been
	// doing its job as backpressure or getting in the way.
	o.ObserveInt64(d.instruments.parkQueueDepth, int64(len(d.parkRetry)))
}

// record counts an outcome once, in the heartbeat's counter and in the metric.
//
// Both or neither: a metric that disagrees with the log line it is named
// after is worse than no metric at all, because the first thing anyone does
// with an odd number is compare it against the log. The counter keeps its
// type and its field so the heartbeat is untouched.
func (d *Dispatcher) record(msg *delivery.Message, counter *atomic.Uint64, outcome string) {
	counter.Add(1)
	d.observeOutcome(msg, outcome)
}

// observeRevoke records the handover's cost. Both numbers matter: the
// duration shows how long the group was held up, and the abandoned count
// shows how often the bound won - which is the trade the bound exists to
// make, and the thing worth alerting on if it ever becomes routine.
func (d *Dispatcher) observeRevoke(took time.Duration, left int) {
	if d.instruments == nil {
		return
	}
	ctx := context.Background()
	d.instruments.revokeDrain.Record(ctx, took.Seconds())
	if left > 0 {
		d.instruments.revokeAbandoned.Add(ctx, 1)
	}
}
