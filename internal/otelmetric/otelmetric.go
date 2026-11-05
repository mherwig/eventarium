// Package otelmetric serves OpenTelemetry metrics in Prometheus exposition
// format on /metrics, so a deployment can point Prometheus, Grafana Agent or
// Alloy at every service and build on the numbers in Grafana.
//
// The instruments are OpenTelemetry's; the exposition is Prometheus's. That
// combination is deliberate: OTel gives one API and one naming convention
// across all seven services, and a scrape endpoint works with no collector
// running - which is the state this repo is in. An OTLP exporter can be added
// later behind the same instruments without touching a single call site.
package otelmetric

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/attribute"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"pubsub/internal/config"
)

// Metrics holds the provider and the handler. One per process.
type Metrics struct {
	mp      *sdkmetric.MeterProvider
	handler http.Handler
	enabled bool
}

// New builds the provider for one service.
//
// METRICS_ENABLED defaults to true; set it to false to turn the endpoint off
// without a rebuild. Disabled returns a no-op meter rather than a nil object,
// so every call site can record unconditionally and pay nothing measurable
// for it.
func New(serviceName string) (*Metrics, error) {
	if !config.Bool("METRICS_ENABLED", true) {
		return &Metrics{enabled: false, handler: http.NotFoundHandler()}, nil
	}

	// A registry of its own rather than prometheus.DefaultRegisterer: New is
	// called more than once in tests, and registering the same collector twice
	// panics.
	reg := prometheus.NewRegistry()
	// WithoutScopeInfo: every series would otherwise carry three constant
	// otel_scope_* labels saying which library emitted it, which is noise in
	// a Grafana query and costs a label match on every scrape.
	exporter, err := promexporter.New(
		promexporter.WithRegisterer(reg),
		promexporter.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	)

	// Go runtime: goroutines, heap, GC, scheduler. A goroutine leak shows up
	// here as monotonic growth, which is the one thing a log line cannot tell
	// you about a running process.
	if err := otelruntime.Start(otelruntime.WithMeterProvider(mp)); err != nil {
		return nil, err
	}
	if err := registerProcessMetrics(mp.Meter("pubsub")); err != nil {
		return nil, err
	}

	return &Metrics{
		mp:      mp,
		handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}),
		enabled: true,
	}, nil
}

// DurationBuckets are the histogram boundaries for every duration instrument,
// in seconds.
//
// Explicit because the default is wrong for this unit. OpenTelemetry's
// fallback boundaries are [0, 5, 10, 25, 50, ...] - numbers chosen for an
// instrument whose values are milliseconds. Recording seconds into them puts
// every realistic observation in the first bucket (le=5), and PromQL's
// histogram_quantile then linearly interpolates inside [0, 5] and answers
// q x 5s for any input at all. Measured on this platform: an e2e histogram
// whose true mean (sum/count) was 332 ms reported p50 = 2,569 ms and
// p99 = 30,910 ms. The mean is computed from _sum and _count and was never
// affected, which is exactly why the bug survived - the numbers that agreed
// with the docs were the ones that were right.
//
// The range covers what this platform actually measures: an HTTP delivery is
// 1-2 ms, a publish handler 1.7-41 ms, a router stage 3-95 ms, end-to-end
// 200-500 ms, a rate-limited delivery up to 45 s, a full drain 177-465 s.
// Resolution is densest below one second because that is where a regression
// would show up, and coarse above it because those tails are by design.
//
// Cardinality note: buckets multiply a histogram's series count, and the
// delivery instruments carry owner + subscription_id. At 16 boundaries that
// is 16 series per subscription where the OTel default was 15 - deliberately
// within one of the old count. Do not add boundaries without checking
// docs/monitoring.md's series estimate.
var DurationBuckets = []float64{
	0.001, 0.002, 0.005, 0.01, 0.02, 0.05,
	0.1, 0.2, 0.5, 1, 2, 5,
	10, 30, 60, 600,
}

// WithDurationBuckets is metric.WithExplicitBucketBoundaries(DurationBuckets),
// provided so call sites can name the intent and so there is exactly one place
// to change the list. Every duration histogram in the platform passes through
// it.
func WithDurationBuckets() metric.HistogramOption {
	return metric.WithExplicitBucketBoundaries(DurationBuckets...)
}

// Meter is the meter instruments are created from. When metrics are disabled
// it is a no-op meter, so recording is free and nothing has to be guarded at
// the call site.
func (m *Metrics) Meter() metric.Meter {
	if !m.enabled {
		// The SDK's provider with no reader would still allocate on record;
		// the noop meter does not.
		return disabledMeter
	}
	return m.mp.Meter("pubsub")
}

// Handler answers /metrics. It is never nil, so callers can register it
// unconditionally; when disabled it answers 404 rather than an empty page,
// which keeps "off" distinguishable from "on but nothing recorded yet".
func (m *Metrics) Handler() http.Handler { return m.handler }

// Enabled reports whether the endpoint serves data.
func (m *Metrics) Enabled() bool { return m.enabled }

// Shutdown flushes and releases the provider. Deferred by every service.
func (m *Metrics) Shutdown(ctx context.Context) error {
	if !m.enabled {
		return nil
	}
	return m.mp.Shutdown(ctx)
}

// registerProcessMetrics adds what the runtime instrumentation does not: how
// much CPU this process has burned and how much resident memory it holds.
//
// Both are read at scrape time rather than accumulated on a timer, so they
// cost nothing between scrapes and cannot drift from the values a reader
// would compute from /proc anyway.
func registerProcessMetrics(meter metric.Meter) error {
	cpu, err := meter.Float64ObservableCounter("process.cpu.time",
		metric.WithUnit("s"),
		metric.WithDescription("Total user and system CPU time spent by the process."))
	if err != nil {
		return err
	}
	rss, err := meter.Int64ObservableGauge("process.memory.rss",
		metric.WithUnit("By"),
		metric.WithDescription("Resident set size of the process."))
	if err != nil {
		return err
	}

	// The registration is not kept: the provider owns callbacks for its own
	// lifetime and releases them on Shutdown, which every service defers.
	_, err = meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		var ru syscall.Rusage
		if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
			o.ObserveFloat64(cpu, timevalSeconds(ru.Utime), metric.WithAttributes(attribute.String("cpu", "user")))
			o.ObserveFloat64(cpu, timevalSeconds(ru.Stime), metric.WithAttributes(attribute.String("cpu", "system")))
		}
		// Read per scrape rather than holding the file open: a handle opened
		// here would be closed by this function's own defer the moment it
		// returned, which is how the first version of this reported no memory
		// at all. Opening /proc/self/statm costs microseconds every scrape and
		// has no lifetime to get wrong.
		if pages, ok := residentPages(); ok {
			o.ObserveInt64(rss, pages*int64(os.Getpagesize()))
		}
		return nil
	}, cpu, rss)
	return err
}

// timevalSeconds converts a syscall.Timeval to fractional seconds. The
// Micro field is microseconds on every platform this runs on; using Sec and
// Usec directly avoids the platform-specific raw representation.
func timevalSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}

// residentPages reads the resident-page count from /proc/self/statm. The
// first field is the total, the second the resident count; a missing or
// unreadable file reports false and the caller simply omits the observation.
func residentPages() (int64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	var size, resident int64
	if _, err := fmt.Sscan(string(data), &size, &resident); err != nil {
		return 0, false
	}
	return resident, true
}

// disabledMeter is what Meter returns when METRICS_ENABLED is false. The
// noop meter allocates nothing on record, which is the point: turning metrics
// off has to cost nothing, not merely record into nowhere.
var disabledMeter = noop.NewMeterProvider().Meter("pubsub")
