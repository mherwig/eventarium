package main

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"pubsub/internal/otelmetric"
)

// publishMetrics are the publish-side instruments. One struct so the handler
// and its helpers do not each grow a field, and so a publishAPI built without
// a meter (tests) can hold nil and skip.
type publishMetrics struct {
	// requests counts outcomes, including the ones that never produced an
	// event: this is what answers "is publishing failing, and for which type".
	requests metric.Int64Counter
	// events counts events that actually reached the log, per type - the
	// numerator of "how many were ingested per second, day or week".
	events metric.Int64Counter
	// duration is one publish request. Recorded per event type only for
	// single-event requests, where one request is one event and the label is
	// unambiguous; a batch is labelled "batch" rather than pretending to
	// attribute a shared round trip to several types at once.
	duration metric.Float64Histogram
	// bytes sums the data member as published, per type.
	bytes metric.Int64Counter
}

func newPublishMetrics(meter metric.Meter) (*publishMetrics, error) {
	requests, err := meter.Int64Counter("pubsub.publish.requests",
		metric.WithDescription("Publish requests by outcome, with the event type where one is known."))
	if err != nil {
		return nil, err
	}
	events, err := meter.Int64Counter("pubsub.publish.events",
		metric.WithDescription("Events written to the event log, by event type."))
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("pubsub.publish.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Time to accept and write a publish request."))
	if err != nil {
		return nil, err
	}
	bytes, err := meter.Int64Counter("pubsub.publish.bytes",
		metric.WithDescription("Data-member bytes published, by event type."))
	if err != nil {
		return nil, err
	}
	return &publishMetrics{
		requests: requests,
		events:   events,
		duration: duration,
		bytes:    bytes,
	}, nil
}

// setupPublishMetrics builds them from the service's meter. A nil *Metrics
// leaves a.publishMetrics nil, and every method below checks for that.
func (a *publishAPI) setupPublishMetrics(m *otelmetric.Metrics) error {
	if m == nil {
		return nil
	}
	pm, err := newPublishMetrics(m.Meter())
	if err != nil {
		return err
	}
	a.publishMetrics = pm
	return nil
}

// countPublish records one request outcome. eventType is "" when the request
// failed before an event type could be determined - an unreadable body or a
// rejected header - and the empty label says so rather than inventing one.
func (a *publishAPI) countPublish(eventType, outcome string) {
	if a.publishMetrics == nil {
		return
	}
	a.publishMetrics.requests.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.String("event_type", eventType),
			attribute.String("outcome", outcome),
		))
}

// countEvent records one event that reached the log, and how big it was.
func (a *publishAPI) countEvent(eventType string, payloadBytes int) {
	if a.publishMetrics == nil {
		return
	}
	ctx := context.Background()
	a.publishMetrics.events.Add(ctx, 1,
		metric.WithAttributes(attribute.String("event_type", eventType)))
	if payloadBytes > 0 {
		a.publishMetrics.bytes.Add(ctx, int64(payloadBytes),
			metric.WithAttributes(attribute.String("event_type", eventType)))
	}
}

// observePublishDuration records how long the request took, labelled with the
// event type it carried or "batch" when it carried several.
func (a *publishAPI) observePublishDuration(took time.Duration, eventType string) {
	if a.publishMetrics == nil {
		return
	}
	a.publishMetrics.duration.Record(context.Background(), took.Seconds(),
		metric.WithAttributes(attribute.String("event_type", eventType)))
}
