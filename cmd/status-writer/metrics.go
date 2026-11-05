package main

import (
	"context"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	"pubsub/internal/otelmetric"
)

// statusMetrics are the writer's instruments.
//
// What is deliberately NOT here is the breakdown by delivery status. The
// status-api's status-summary already answers that from ClickHouse, and
// splitting by status would mean decoding every row on the way to the insert -
// CPU on the hot path to duplicate a number that is already queryable. These
// are the four that are not available anywhere else.
type statusMetrics struct {
	// rowsWritten is throughput. The rate of this falling to zero while the
	// dispatcher keeps producing is the signal that this stage stopped.
	rowsWritten metric.Int64Counter
	// insertFailures counts rows that did not reach ClickHouse. This is the
	// data-quality number: everything downstream - the oracle, the status API,
	// every dashboard - reads this table, so a row that fails here is a
	// delivery whose outcome is permanently unknown.
	insertFailures metric.Int64Counter
	// writeDuration is the insert itself, to tell "ClickHouse is slow" from
	// "we are producing too much".
	writeDuration metric.Float64Histogram
	// bufferDepth is rows waiting for the next flush. A buffer that only
	// grows is the earliest sign that ClickHouse cannot keep up, before the
	// lag shows up anywhere else.
	bufferDepth metric.Int64ObservableGauge
}

func newStatusMetrics(meter metric.Meter) (*statusMetrics, error) {
	rowsWritten, err := meter.Int64Counter("pubsub.status.rows_written",
		metric.WithDescription("Status rows inserted into ClickHouse."))
	if err != nil {
		return nil, err
	}
	insertFailures, err := meter.Int64Counter("pubsub.status.insert_failures",
		metric.WithDescription("Status rows that failed to reach ClickHouse; those deliveries' outcomes are unknown until the records are re-consumed."))
	if err != nil {
		return nil, err
	}
	writeDuration, err := meter.Float64Histogram("pubsub.status.write_duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of one batch insert into ClickHouse."))
	if err != nil {
		return nil, err
	}
	bufferDepth, err := meter.Int64ObservableGauge("pubsub.status.buffer_depth",
		metric.WithDescription("Status rows buffered in memory waiting for the next flush."))
	if err != nil {
		return nil, err
	}
	return &statusMetrics{
		rowsWritten:    rowsWritten,
		insertFailures: insertFailures,
		writeDuration:  writeDuration,
		bufferDepth:    bufferDepth,
	}, nil
}

// observeInserted records a successful insert of n rows.
func (m *statusMetrics) observeInserted(n int, took time.Duration) {
	if m == nil || n <= 0 {
		return
	}
	ctx := context.Background()
	m.rowsWritten.Add(ctx, int64(n))
	m.writeDuration.Record(ctx, took.Seconds())
}

// observeInsertFailure records a batch that did not land.
func (m *statusMetrics) observeInsertFailure(n int) {
	if m == nil {
		return
	}
	m.insertFailures.Add(context.Background(), int64(n))
}

// observeBufferDepth is the scrape-time read of the atomic the writer keeps
// in step with its own buffer. Reading len(w.buf) from the scrape goroutine
// would race the single goroutine that owns it.
func (m *statusMetrics) observeBufferDepth(depth int64, o metric.Observer) {
	if m == nil {
		return
	}
	o.ObserveInt64(m.bufferDepth, depth)
}

// setupStatusMetrics builds them and registers the depth gauge against the
// atomic the writer maintains.
func setupStatusMetrics(m *otelmetric.Metrics, depth *atomic.Int64) (*statusMetrics, error) {
	if m == nil {
		return nil, nil
	}
	sm, err := newStatusMetrics(m.Meter())
	if err != nil {
		return nil, err
	}
	if _, err := m.Meter().RegisterCallback(func(_ context.Context, o metric.Observer) error {
		sm.observeBufferDepth(depth.Load(), o)
		return nil
	}, sm.bufferDepth); err != nil {
		return nil, err
	}
	return sm, nil
}
