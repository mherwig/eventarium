package main

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pubsub/internal/otelmetric"
)

// routerMetrics are the router's instruments. Nil-safe: every method checks,
// so run() records unconditionally and a router built without a meter (tests)
// costs nothing.
type routerMetrics struct {
	// stage is one stage of the serial poll -> process -> transaction -> poll
	// loop, split because the stages point at different problems: a large
	// kafka_wait means queued records, a large produce_sync means Kafka, a
	// large process means this router.
	stage metric.Float64Histogram
	// batches counts completed transaction cycles.
	batches metric.Int64Counter
	// records counts records read from and written to Kafka, so rate() over
	// either answers "how much is the router processing per second".
	records metric.Int64Counter
}

func newRouterMetrics(meter metric.Meter) (*routerMetrics, error) {
	stage, err := meter.Float64Histogram("pubsub.router.stage.duration",
		metric.WithUnit("s"),
		otelmetric.WithDurationBuckets(),
		metric.WithDescription("One stage of the router's cycle, by stage name."))
	if err != nil {
		return nil, err
	}
	batches, err := meter.Int64Counter("pubsub.router.batches",
		metric.WithDescription("Committed transaction cycles."))
	if err != nil {
		return nil, err
	}
	records, err := meter.Int64Counter("pubsub.router.records",
		metric.WithDescription("Records read from and written to Kafka, by direction."))
	if err != nil {
		return nil, err
	}
	return &routerMetrics{stage: stage, batches: batches, records: records}, nil
}

// setupRouterMetrics builds them from the service's meter.
func setupRouterMetrics(m *otelmetric.Metrics) (*routerMetrics, error) {
	if m == nil {
		return nil, nil
	}
	return newRouterMetrics(m.Meter())
}

func (m *routerMetrics) observeStage(stage string, took time.Duration) {
	if m == nil {
		return
	}
	m.stage.Record(context.Background(), took.Seconds(),
		metric.WithAttributes(attribute.String("stage", stage)))
}

func (m *routerMetrics) addBatch() {
	if m == nil {
		return
	}
	m.batches.Add(context.Background(), 1)
}

func (m *routerMetrics) addRecords(direction string, n int) {
	if m == nil || n <= 0 {
		return
	}
	m.records.Add(context.Background(), int64(n),
		metric.WithAttributes(attribute.String("direction", direction)))
}
