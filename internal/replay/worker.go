package replay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
)

// jobBatch bounds one read-and-produce step. It is also the unit at which a
// cancellation takes effect (section 11: the worker stops at the next batch
// boundary) and how much original event data one step holds in memory.
const jobBatch = 200

// pollInterval is how often the worker looks for a pending job.
const pollInterval = 500 * time.Millisecond

// maxErrorChars bounds what a failure stores in replay_jobs.error.
const maxErrorChars = 1000

// errCancelled is the sentinel a cancelled batch loop returns; the job then
// finishes as cancelled, never as failed.
var errCancelled = errors.New("replay: job cancelled")

// progress mirrors the three counters of replay_jobs while a job runs; they
// are persisted after every batch so a crash shows how far it got.
type progress struct {
	matched       int64
	enqueued      int64
	notReplayable int64
}

// Producer writes replay deliveries and their pending status records. It is a
// produce-only Kafka transaction: one transaction per batch commits the
// delivery records and their pending statuses atomically, which is the
// guarantee section 9 asks of the router. franz-go offers two ways into a
// transaction; this service uses Client.BeginTransaction/EndTransaction
// instead of NewGroupTransactSession because a replay consumes nothing — a
// group session would join the "router"-style consumer group for no reason.
// All records of one job share the subscription's key and are produced
// sequentially, so the ingestion order of section 284 survives: every batch
// fully commits before the next one starts.
type Producer struct {
	cl *kgo.Client
}

// NewProducer builds the transactional producer over KAFKA_BROKERS. The
// transactional ID is namespaced with "replay-": the router uses
// "<host>-<pid>", and two services on one host could otherwise draw the same
// PID and fence each other's producer.
func NewProducer() (*Producer, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	cl, err := kafkautil.NewClient(
		kgo.TransactionalID(fmt.Sprintf("replay-%s-%d", host, os.Getpid())),
	)
	if err != nil {
		return nil, fmt.Errorf("replay: building the producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// produce writes one batch inside a single transaction. Nothing outside a
// transaction can be produced by this client (franz-go fails those records
// immediately), so a partial batch is never visible: on any error the
// transaction is aborted and the job fails with its counters unchanged.
func (p *Producer) produce(ctx context.Context, recs []*kgo.Record) error {
	if len(recs) == 0 {
		return nil
	}
	if err := p.cl.BeginTransaction(); err != nil {
		return fmt.Errorf("replay: beginning the batch transaction: %w", err)
	}
	if res := p.cl.ProduceSync(ctx, recs...); res.FirstErr() != nil {
		if err := p.cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
			slog.Error("replay: aborting the batch transaction", "err", err)
		}
		return fmt.Errorf("replay: producing the batch: %w", res.FirstErr())
	}
	if err := p.cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return fmt.Errorf("replay: committing the batch: %w", err)
	}
	return nil
}

// Close flushes and closes the producer.
func (p *Producer) Close() { p.cl.Close() }

// Worker executes replay jobs (section 11). It is a single goroutine inside
// the replay-api process and runs one job at a time in creation order, which
// keeps section 286's "one active job per subscription" trivially true and
// gives the ordered produce of section 284 one sequential writer.
type Worker struct {
	jobs      *JobStore
	ch        clickhouse.Conn
	reg       *registry.Registry
	reader    eventReader
	producer  *Producer
	maxEvents int
	poll      time.Duration
}

// NewWorker wires the worker. maxEvents is REPLAY_MAX_EVENTS, the default cap
// per job; reader and producer are injected so tests can fake the Kafka side.
func NewWorker(jobs *JobStore, ch clickhouse.Conn, reg *registry.Registry,
	reader eventReader, producer *Producer, maxEvents int) *Worker {
	return &Worker{
		jobs:      jobs,
		ch:        ch,
		reg:       reg,
		reader:    reader,
		producer:  producer,
		maxEvents: maxEvents,
		poll:      pollInterval,
	}
}

// Run claims and executes jobs until ctx is cancelled. Startup recovery runs
// first (JobStore.ResetInterrupted): jobs that were running when the process
// died go back to pending and re-execute from the start, which may enqueue
// some deliveries twice — consumers are idempotent by event reference
// (section 8, spec line 230) and every status record carries the replay ID.
func (w *Worker) Run(ctx context.Context) {
	if err := w.jobs.ResetInterrupted(ctx); err != nil {
		slog.Error("replay: startup recovery failed, will retry on the next start", "err", err)
	}
	ticker := time.NewTicker(w.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		id, pending, err := w.jobs.NextPendingID(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("replay: looking for a pending job", "err", err)
			continue
		}
		if !pending {
			continue
		}
		job, claimed, err := w.jobs.Claim(ctx, id)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("replay: claiming a job", "err", err)
			continue
		}
		if !claimed {
			continue // a cancel got there first
		}
		w.runJob(ctx, job)
	}
}

// runJob executes one claimed job and writes its terminal state.
func (w *Worker) runJob(ctx context.Context, job *Job) {
	slog.Info("replay: job started",
		"replay_id", job.ID, "subscription", job.SubscriptionID, "dry_run", job.DryRun)
	counts := progress{}
	err := w.execute(ctx, job, &counts)

	switch {
	case ctx.Err() != nil:
		// Shutdown mid-job: the job stays 'running' on purpose. The next
		// startup resets it to pending and replays it from the start;
		// duplicate deliveries are acceptable, lost ones are not (section 8).
		slog.Info("replay: job interrupted by shutdown",
			"replay_id", job.ID, "matched", counts.matched, "enqueued", counts.enqueued)
		return
	case errors.Is(err, errCancelled):
		w.finish(ctx, job, counts, StatusCancelled, "")
		slog.Info("replay: job cancelled",
			"replay_id", job.ID, "matched", counts.matched, "enqueued", counts.enqueued)
	case err != nil:
		msg := truncate(err.Error(), maxErrorChars)
		w.finish(ctx, job, counts, StatusFailed, msg)
		slog.Error("replay: job failed", "replay_id", job.ID, "err", err)
	default:
		w.finish(ctx, job, counts, StatusDone, "")
		slog.Info("replay: job done",
			"replay_id", job.ID, "matched", counts.matched,
			"enqueued", counts.enqueued, "not_replayable", counts.notReplayable)
	}
}

// finish persists the terminal state; a failure to write it is logged, never
// panics — the job would otherwise be retried by the startup recovery.
func (w *Worker) finish(ctx context.Context, job *Job, counts progress, status, errMsg string) {
	if err := w.jobs.Finish(ctx, job.ID, status, counts.matched, counts.enqueued, counts.notReplayable, errMsg); err != nil {
		slog.Error("replay: writing the terminal state", "replay_id", job.ID, "err", err)
	}
}

// execute resolves the selector, then walks the matches in ingestion order in
// batches of jobBatch.
func (w *Worker) execute(ctx context.Context, job *Job, counts *progress) error {
	sel := job.Selector
	matches, err := Resolve(ctx, w.ch, job.Owner, job.SubscriptionID, sel, sel.limit(w.maxEvents))
	if err != nil {
		return err
	}
	counts.matched = int64(len(matches))
	if err := w.jobs.UpdateProgress(ctx, job.ID, counts.matched, 0, 0); err != nil {
		return err
	}

	// Fail fast when the subscription is gone or no longer belongs to the
	// owner (section 6.7); each batch re-reads it anyway so the *current*
	// projection of section 281 applies at produce time.
	if _, err := w.currentSub(job); err != nil {
		return err
	}

	for start := 0; start < len(matches); start += jobBatch {
		// Cancellation is observed at batch boundaries: no half-written
		// batch, and a cancelled job reports the counters it really
		// enqueued (section 11, cancel endpoint).
		status, err := w.jobs.Status(ctx, job.ID)
		if err != nil {
			return err
		}
		if status == StatusCancelRequested {
			return errCancelled
		}
		end := min(start+jobBatch, len(matches))
		if err := w.chunk(ctx, job, matches[start:end], counts); err != nil {
			return err
		}
	}
	return nil
}

// chunk reads one batch of original event records, classifies them, builds the
// delivery and pending status pairs and — unless this is a dry run — produces
// them. The counters are persisted before the next batch starts.
func (w *Worker) chunk(ctx context.Context, job *Job, ms []Match, counts *progress) error {
	coords := make([]coord, len(ms))
	for i, m := range ms {
		coords[i] = coord{
			Topic:     kafkautil.EventsTopic(m.RetentionClass),
			Partition: m.SrcPartition,
			Offset:    m.SrcOffset,
		}
	}
	records, err := w.reader.Read(ctx, coords)
	if err != nil {
		return err
	}
	sub, err := w.currentSub(job)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	out, built, notReplayable := w.assemble(job, sub, ms, records, now)

	if !job.DryRun && len(out) > 0 {
		if err := w.producer.produce(ctx, out); err != nil {
			return err
		}
		counts.enqueued += built
	}
	counts.notReplayable += notReplayable
	return w.jobs.UpdateProgress(ctx, job.ID, counts.matched, counts.enqueued, counts.notReplayable)
}

// assemble turns one resolved batch into the records to produce, in the order
// the selector matched them (section 284: a replay keeps the original relative
// order) and counts what cannot be replayed (section 11, retention limit):
//
//   - a coordinate without a record — pruned by retention, beyond the log end,
//     its topic gone — is not_replayable and left untouched;
//   - an event whose expires_at header has passed is not_replayable too, even
//     though the record is still readable (section 282: past expires_at counts,
//     the event is never rewritten);
//   - anything else becomes the pair of section 9 (delivery + pending status)
//     and counts as enqueued by the caller.
//
// It holds no I/O on purpose: the classification rules of section 11 are unit
// tested against fakes here, without Kafka, ClickHouse or PostgreSQL.
func (w *Worker) assemble(job *Job, sub *registry.Subscription, ms []Match,
	records map[coord]*kgo.Record, now time.Time) (out []*kgo.Record, built, notReplayable int64) {
	for _, m := range ms {
		key := coord{
			Topic:     kafkautil.EventsTopic(m.RetentionClass),
			Partition: m.SrcPartition,
			Offset:    m.SrcOffset,
		}
		rec := records[key]
		if ok, reason := classify(rec, now); !ok {
			// Section 11: past expires_at or no longer in the events topic
			// counts as not_replayable and the event is left untouched.
			slog.Debug("replay: not replayable",
				"replay_id", job.ID, "delivery_id", m.DeliveryID, "reason", reason)
			notReplayable++
			continue
		}
		rs, err := buildReplay(w.reg, job, sub, m, rec, now)
		if err != nil {
			// Not a retention problem, but the same outcome: this row can
			// never be enqueued, so it belongs in not_replayable to keep
			// matched = enqueued + not_replayable.
			slog.Warn("replay: cannot rebuild the delivery, counted not_replayable",
				"replay_id", job.ID, "delivery_id", m.DeliveryID, "err", err)
			notReplayable++
			continue
		}
		out = append(out, rs...)
		built++
	}
	return out, built, notReplayable
}

// currentSub re-reads the subscription from the registry, which refreshes
// from PostgreSQL every second (spike deviation of section 6.1): a replay
// must apply the projection and channel as they are now, not as they were
// when the event was first delivered (section 281).
func (w *Worker) currentSub(job *Job) (*registry.Subscription, error) {
	entry, found := w.reg.Subscription(job.SubscriptionID)
	if !found || entry.Sub.Owner != job.Owner {
		return nil, fmt.Errorf("subscription %s is gone or no longer belongs to owner %s",
			job.SubscriptionID, job.Owner)
	}
	return entry.Sub, nil
}

// truncate keeps a failure message within replay_jobs.error bounds.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
