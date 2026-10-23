// Command router is the fan-out stage of the spike (spec sections 5, 6.2,
// 6.3 and 9). It consumes all four per-class event log topics in the consumer
// group "router" and, for every event, matches the subscriptions of the same
// owner and event type in the in-memory registry (refreshed from PostgreSQL
// every second), evaluates their CEL filters, computes the ordering key
// before any projection, applies the per-subscription projection allowlist
// and writes one delivery record per matching subscription — keyed by
// subscription ID so that section 6.3's fixed partitioning holds — plus one
// pending status record.
//
// Exactly once (section 9): each poll batch is processed inside one Kafka
// transaction via kgo.NewGroupTransactSession. The delivery records, their
// pending status records and the consumed offset commit are all written
// through the same transactional producer, so End(TryCommit) commits them
// atomically — a crash can neither lose a pending status nor reprocess a
// batch into duplicates. (The status records go through the transactional
// session rather than internal/status.Producer on purpose: franz-go lets a
// transactional producer write to multiple topics, and spec section 9 wants
// the pending status in the same transaction as the delivery message.)
//
// Ordering invariant that makes per-key order survive (section 6.2/6.3): one
// router instance owns a source partition, processes its records serially in
// offset order, and writes to the deliveries topic through one producer.
// Since the delivery key is the subscription ID, events with the same
// ordering key land on the same deliveries partition in the order they were
// consumed from their source partition.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/config"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/routing"
	"pubsub/internal/store"
)

const (
	group           = "router"
	pollBatch       = 500
	registryEvery   = time.Second
	errorBackoff    = time.Second
	maxErrorBackoff = 30 * time.Second
)

func main() {
	setupLogger()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Control data: the registry polls PostgreSQL (spike deviation from the
	// compacted config topic of section 6.1, see docs/SPIKE.md).
	st, err := store.Open(ctx, config.Str("PG_DSN", "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"))
	if err != nil {
		slog.Error("router: opening store", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	reg := registry.New(st, registryEvery)

	// Do not join the group before the first successful reload: otherwise
	// events consumed while the index is empty would be committed without
	// any delivery. Run keeps refreshing in the background afterwards.
	for {
		if err := reg.Reload(ctx); err == nil {
			break
		} else if ctx.Err() != nil {
			return
		}
		slog.Warn("router: initial registry load failed, retrying", "err", err)
		if !sleep(ctx, errorBackoff) {
			return
		}
	}
	go reg.Run(ctx)

	topics := make([]string, 0, len(kafkautil.Classes))
	for _, class := range kafkautil.Classes {
		topics = append(topics, kafkautil.EventsTopic(class))
	}

	sess, err := newSession(topics)
	if err != nil {
		slog.Error("router: creating transactional session", "err", err)
		os.Exit(1)
	}
	defer sess.Close()

	run(ctx, sess, routing.New(reg))
	slog.Info("router: stopped")
}

// newSession builds the group transact session: a consuming client that also
// produces transactionally. The transactional ID is unique per process so
// that two instances never fence each other; a restarted instance fences its
// own predecessor, which is exactly what we want.
func newSession(topics []string) (*kgo.GroupTransactSession, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return kgo.NewGroupTransactSession(
		kgo.SeedBrokers(kafkautil.Bootstrap()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(), // offsets are committed by End(TryCommit)
		kgo.TransactionalID(fmt.Sprintf("%s-%d", host, os.Getpid())),
		kgo.ProducerLinger(2*time.Millisecond),
		kgo.ProducerBatchMaxBytes(4<<20),
	)
}

// run is the consume loop: poll a batch, process its records serially in
// offset order, then commit the deliveries, the pending statuses and the
// consumed offsets in one transaction. Errors are logged and retried after a
// backoff; the router never exits on a transient failure.
func run(ctx context.Context, sess *kgo.GroupTransactSession, r *routing.Router) {
	slog.Info("router: ready", "group", group)
	backoff := errorBackoff
	for ctx.Err() == nil {
		fetches := sess.PollRecords(ctx, pollBatch)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		// Partial fetch errors (e.g. an unreachable broker for one
		// partition) only cost the records of that partition; the rest
		// of the batch is still usable. Only an error fetch with no
		// records backs off, otherwise a down broker would spin the loop.
		fetchErr := fetches.Err()
		recs := fetches.Records()
		if len(recs) == 0 {
			if fetchErr != nil {
				slog.Warn("router: fetch error", "err", fetchErr)
				if !sleep(ctx, backoff) {
					return
				}
				backoff = nextBackoff(backoff)
			}
			continue
		}
		if fetchErr != nil {
			slog.Warn("router: fetch error, processing partial batch", "err", fetchErr)
		}

		out := make([]*kgo.Record, 0, len(recs)*2)
		for _, rec := range recs {
			out = append(out, r.Process(rec)...)
		}

		if err := sess.Begin(); err != nil {
			slog.Error("router: beginning transaction", "err", err)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		// One transaction covers the delivery records, their pending
		// status records and the offset commit: End(TryCommit) flushes
		// everything atomically (section 9).
		if res := sess.ProduceSync(ctx, out...); res.FirstErr() != nil {
			slog.Error("router: producing batch", "err", res.FirstErr())
			if _, err := sess.End(ctx, kgo.TryAbort); err != nil {
				slog.Error("router: aborting transaction", "err", err)
			}
			if !sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		if _, err := sess.End(ctx, kgo.TryCommit); err != nil {
			// The records were not committed: they are re-consumed
			// after the next rebalance or restart, which the
			// transactional produce keeps duplicate-free only for
			// batches that actually commit. Retry after backoff.
			slog.Error("router: committing transaction", "err", err)
			if !sleep(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = errorBackoff
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > maxErrorBackoff {
		d = maxErrorBackoff
	}
	return d
}

func setupLogger() {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(config.Str("LOG_LEVEL", "info"))); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
}
