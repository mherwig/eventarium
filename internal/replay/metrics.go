package replay

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// jobMetrics are the replay service's instruments.
//
// A replay is a job system, so what is interesting is the job lifecycle
// rather than a throughput number: a job stuck in "running" is invisible in
// the API until somebody lists jobs, and a job that reaches "failed" is the
// thing worth alerting on.
type jobMetrics struct {
	// jobs counts status transitions. Backlog is created minus finished, and
	// a running count that grows while finished does not means the worker has
	// stopped picking jobs up.
	jobs metric.Int64Counter
	// events counts what a finished job actually did. not_replayable climbing
	// means the selectors are matching events the platform can no longer
	// deliver - expired retention, a gone topic - which is a slow way to
	// discover a configuration problem.
	events metric.Int64Counter
}

func newJobMetrics(meter metric.Meter) (*jobMetrics, error) {
	jobs, err := meter.Int64Counter("pubsub.replay.jobs",
		metric.WithDescription("Replay jobs by status transition: pending, running, done, failed, cancelled."))
	if err != nil {
		return nil, err
	}
	events, err := meter.Int64Counter("pubsub.replay.events",
		metric.WithDescription("Events a replay job classified: matched, enqueued, not_replayable."))
	if err != nil {
		return nil, err
	}
	return &jobMetrics{jobs: jobs, events: events}, nil
}

// observeJob records one status transition. A nil receiver is the no-meter
// case, which is what tests construct.
func (m *jobMetrics) observeJob(status string) {
	if m == nil {
		return
	}
	m.jobs.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("status", status)))
}

// observeEvents records what a finished job actually did.
//
// matched is deliberately absent: the job's own contract is
// matched = enqueued + not_replayable, so counting all three would report
// twice the events that were handled. These two sum to matched and split it
// into the part that was delivered and the part the platform could not
// deliver - which is the half worth alerting on.
func (m *jobMetrics) observeEvents(enqueued, notReplayable int64) {
	if m == nil {
		return
	}
	ctx := context.Background()
	if enqueued > 0 {
		m.events.Add(ctx, enqueued,
			metric.WithAttributes(attribute.String("result", "enqueued")))
	}
	if notReplayable > 0 {
		m.events.Add(ctx, notReplayable,
			metric.WithAttributes(attribute.String("result", "not_replayable")))
	}
}
