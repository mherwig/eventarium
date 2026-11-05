package httpx

import (
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics returns middleware that records request duration by route and
// status. A nil meter returns a pass-through, so callers do not branch.
//
// The route is the pattern the mux registered, e.g.
// "GET /v1/subscriptions/{id}/deliveries", rather than r.URL.Path. That
// distinction is the whole point: the path contains the subscription id, so
// using it would create one time series per subscription and per event, which
// is the cardinality problem an allow-list exists to avoid. The pattern is a
// fixed string per handler, so the label set is bounded by the number of
// routes the service has.
//
// It comes from mux.Handler(r) rather than r.Pattern, because ServeMux
// dispatches on a clone of the request - a middleware wrapping the mux runs
// against the original, where Pattern is still empty.
//
// Health and metrics endpoints are excluded for the same reason AccessLog
// excludes them: a probe every ten seconds and a scrape every fifteen are
// not traffic, and their duration says nothing about the service's work.
func Metrics(mux *http.ServeMux, m metric.Meter) (func(http.Handler) http.Handler, error) {
	if m == nil {
		return func(next http.Handler) http.Handler { return next }, nil
	}
	dur, err := m.Float64Histogram("pubsub.http.server.duration",
		metric.WithUnit("s"),
		metric.WithDescription("HTTP request duration by route and status code."))
	if err != nil {
		return nil, err
	}
	reqs, err := m.Int64Counter("pubsub.http.server.requests",
		metric.WithDescription("HTTP requests by route and status code."))
	if err != nil {
		return nil, err
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isPollPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
			next.ServeHTTP(sw, r)
			took := time.Since(start)

			_, route := mux.Handler(r)
			if route == "" {
				route = "unmatched"
			}
			attrs := []attribute.KeyValue{
				attribute.String("route", route),
				attribute.Int("status_code", sw.code),
			}
			ctx := context.Background()
			dur.Record(ctx, took.Seconds(), metric.WithAttributes(attrs...))
			reqs.Add(ctx, 1, metric.WithAttributes(attrs...))
		})
	}, nil
}

// isPollPath reports whether a path is a probe or a scrape rather than work.
func isPollPath(p string) bool {
	switch p {
	case "/healthz", "/readyz", "/metrics":
		return true
	}
	return false
}
