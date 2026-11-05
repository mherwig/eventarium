package sse

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pubsub/internal/delivery"
	"pubsub/internal/otelmetric"
)

// metrics are the gateway's instruments. Nil-safe: every method checks, so a
// Gateway built without a meter (tests) costs nothing.
//
// The gateway measures what the dispatcher cannot: it skips SSE deliveries
// because the gateway owns them, so this is the only place the SSE half of
// "was it delivered, and how long did it take" exists at all.
type metrics struct {
	// sent counts deliveries written to a client, per subscription - the SSE
	// answer to the dispatcher's outcome counter.
	sent metric.Int64Counter
	// e2e shares the name and the channel label with the dispatcher's, so one
	// query covers both channels.
	e2e metric.Float64Histogram
	// connections is this node's own streams; presence is every node's, from
	// the view the delivery stage routes against. Summing connections across
	// instances gives the cluster total, which is what a single-instance
	// graph would otherwise report as the whole truth.
	connections metric.Int64ObservableGauge
	presence    metric.Int64ObservableGauge
}

func newMetrics(meter metric.Meter) (*metrics, error) {
	sent, err := meter.Int64Counter("pubsub.sse.sent",
		metric.WithDescription("SSE deliveries written to a client, per subscription."))
	if err != nil {
		return nil, err
	}
	e2e, err := meter.Float64Histogram("pubsub.e2e.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Publish-api's ingest stamp to the consumer receiving the delivery."))
	if err != nil {
		return nil, err
	}
	connections, err := meter.Int64ObservableGauge("pubsub.sse.connections",
		metric.WithDescription("Open SSE streams on this node."))
	if err != nil {
		return nil, err
	}
	presence, err := meter.Int64ObservableGauge("pubsub.sse.presence",
		metric.WithDescription("SSE connections known across every node, from the routing view."))
	if err != nil {
		return nil, err
	}
	return &metrics{
		sent:        sent,
		e2e:         e2e,
		connections: connections,
		presence:    presence,
	}, nil
}

// setupMetrics builds them from the service's meter and registers the
// scrape-time gauges.
func (g *Gateway) setupMetrics(m *otelmetric.Metrics) error {
	if m == nil {
		return nil
	}
	instr, err := newMetrics(m.Meter())
	if err != nil {
		return err
	}
	g.instr = instr
	_, err = m.Meter().RegisterCallback(func(_ context.Context, o metric.Observer) error {
		g.observeGauges(o)
		return nil
	}, instr.connections, instr.presence)
	return err
}

// observeGauges reads connection state at scrape time: a connection count is a
// state, and sampling it on a timer would answer "what was it ten seconds ago"
// for every point on the graph.
func (g *Gateway) observeGauges(o metric.Observer) {
	if g.instr == nil {
		return
	}
	g.mu.Lock()
	local := len(g.conns)
	g.mu.Unlock()
	o.ObserveInt64(g.instr.connections, int64(local))
	o.ObserveInt64(g.instr.presence, int64(len(g.view.subscriptions())))
}

// observeSent records one delivery written to a client. Called only once the
// bytes are on the wire, so it counts deliveries a client actually received
// rather than ones this node attempted.
func (g *Gateway) observeSent(msg *delivery.Message) {
	if g.instr == nil || msg == nil {
		return
	}
	ctx := context.Background()
	g.instr.sent.Add(ctx, 1, metric.WithAttributes(
		attribute.String("owner", msg.Owner),
		attribute.String("subscription_id", msg.SubscriptionID),
	))
	// A delivery written by a router that predates IngestedAt, or with the
	// header stripped, is skipped rather than measured against the zero time -
	// which would put a sample of two thousand years on the histogram.
	if msg.IngestedAt.IsZero() {
		return
	}
	took := time.Since(msg.IngestedAt)
	if took < 0 {
		return
	}
	g.instr.e2e.Record(ctx, took.Seconds(),
		metric.WithAttributes(attribute.String("channel", "sse")))
}
