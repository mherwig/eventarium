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
	"pubsub/internal/httpx"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/routing"
	"pubsub/internal/store"
)

const (
	group           = "router"
	pollBatch       = 500
	errorBackoff    = time.Second
	maxErrorBackoff = 30 * time.Second
)

func main() {
	setupLogger()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Control data: the registry reads PostgreSQL (spike deviation from the
	// compacted config topic of section 6.1, see docs/contracts.md); a NOTIFY from
	// the trigger of migration 003 refreshes it, the poll is the safety net.
	dsn := config.Str("PG_DSN", "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable")
	st, err := store.Open(ctx, dsn)
	if err != nil {
		slog.Error("router: opening store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Liveness from the first second - a PostgreSQL outage must not look like
	// a dead process - readiness only once the registry has been loaded and
	// the transactional session exists, because before that first reload the
	// router has no subscription index and would commit events it cannot
	// match anything for.
	health := httpx.NewHealth(func(ctx context.Context) error { return st.Pool.Ping(ctx) })
	go health.Serve(ctx, config.Str("HTTP_ADDR", ":8085"))
	// REGISTRY_POLL is only the safety net: the NOTIFY trigger of migration
	// 003 wakes the registry the moment a subscription changes.
	reg := registry.New(st, config.Duration("REGISTRY_POLL", time.Second), registry.WithNotify(dsn))
	// A stale index is a readiness problem for this service specifically: the
	// router commits an event's offset together with the deliveries it
	// produces, so an index that stopped moving would fan an event out to a
	// subscription set that is no longer current - and the commit would make
	// that decision final. Readiness says so instead of the log having to.
	health.AddCheck(func(context.Context) error {
		return reg.Check(config.Duration("REGISTRY_STALE_AFTER", registry.DefaultStaleAfter))
	})

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

	health.SetReady(true)
	if err := run(ctx, sess, routing.New(reg)); err != nil {
		slog.Error("router: stopped", "err", err)
		os.Exit(1)
	}
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
	// This is the one client where both halves of the failover pair matter: it
	// is a group member *and* a transactional producer, so a transaction must
	// not be able to outlive the membership carrying it. Taking both from
	// kafkautil.Timeouts keeps that rule from being configured away - a
	// session timeout chosen independently here would silently decouple it.
	session, txn := kafkautil.Timeouts()
	return kgo.NewGroupTransactSession(
		kgo.SeedBrokers(kafkautil.Bootstrap()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(), // offsets are committed by End(TryCommit)
		kgo.TransactionalID(fmt.Sprintf("%s-%d", host, os.Getpid())),
		kgo.ProducerLinger(2*time.Millisecond),
		kgo.ProducerBatchMaxBytes(4<<20),
		kgo.SessionTimeout(session),
		kgo.TransactionTimeout(txn),
		// Without this, franz-go's logs are discarded and an aborted
		// transaction is a bare "committed=false": the reason (a revoke,
		// a failed heartbeat, an abortable offset commit) is only ever
		// printed here. See kgoLogger.
		kgo.WithLogger(kgoLogger{}),
	)
}

// kgoLogger forwards franz-go's own logging to slog so that it interleaves
// with the router's and obeys LOG_LEVEL.
//
// franz-go's Info is deliberately demoted to debug: it logs roughly eight
// lines per transaction (begin, flush, AddOffsetsToTxn, end, response, ...),
// which at a few thousand events per second is a line per small batch on
// every replica. What stays at the default level is what an operator acts
// on: franz-go's own warnings and errors, plus the router's warning for an
// aborted transaction.
//
// The diagnostic path is LOG_LEVEL=debug, where "kgo: transaction session
// ending" carries was_failed and can_try_commit - the reason an abort that
// is reported to us as a plain (committed=false, err=nil) happened at all.
// Level() mirrors that mapping because franz-go consults it before building
// a message: it never reports Info, so those messages are never built when
// they would only be dropped.
type kgoLogger struct{}

func (kgoLogger) Level() kgo.LogLevel {
	ctx := context.Background()
	switch {
	case slog.Default().Enabled(ctx, slog.LevelDebug):
		return kgo.LogLevelDebug
	case slog.Default().Enabled(ctx, slog.LevelWarn):
		return kgo.LogLevelWarn
	default:
		return kgo.LogLevelError
	}
}

func (kgoLogger) Log(l kgo.LogLevel, msg string, keyvals ...any) {
	var lvl slog.Level
	switch l {
	case kgo.LogLevelError:
		lvl = slog.LevelError
	case kgo.LogLevelWarn:
		lvl = slog.LevelWarn
	default: // Info and Debug both become debug; LogLevelNone never reaches us
		lvl = slog.LevelDebug
	}
	slog.Log(context.Background(), lvl, "kgo: "+msg, keyvals...)
}

// run is the consume loop: poll a batch, process its records serially in
// offset order, then commit the deliveries, the pending statuses and the
// consumed offsets in one transaction. Transient errors (fetch, begin,
// produce, an aborted commit) are logged and retried after a backoff; only a
// non-retryable commit failure returns an error, which main turns into a
// non-zero exit so the pod is restarted with a fresh session.
func run(ctx context.Context, sess *kgo.GroupTransactSession, r *routing.Router) error {
	slog.Info("router: ready", "group", group)
	// Read once for the abort diagnosis below: franz-go returns
	// (committed=false, nil) for every abort and its own reason is logged at a
	// level kgoLogger filters out at INFO, so the only way to tell a timeout
	// from a revoke after the fact is how long the transaction lived.
	_, txnTimeout := kafkautil.Timeouts()
	backoff := errorBackoff
	// Stage timings for the hop no external measurement can split: how long
	// records waited in Kafka before this poll returned them, how long the
	// serial Process() loop took, and how long the transactional produce
	// took. Those three plus the generator and publish-api are the whole
	// publish-to-pending figure, so each is named rather than inferred.
	var batches int
	var sumWait, sumProc, sumSync, sumEnd, sumPoll, sumCycle time.Duration
	// The spread of record ages across the batches in this window: the
	// youngest and oldest seen, so the mean above can be read as either
	// "fetched live" or "fetched as a stale snapshot".
	var waitMinAll, waitMaxAll time.Duration
	var lastPoll time.Time
	for ctx.Err() == nil {
		pollStart := time.Now()
		fetches := sess.PollRecords(ctx, pollBatch)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return nil
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
					return nil
				}
				backoff = nextBackoff(backoff)
			}
			continue
		}
		if fetchErr != nil {
			slog.Warn("router: fetch error, processing partial batch", "err", fetchErr)
		}

		// How long PollRecords itself took, and how long since the previous
		// productive poll. A large pollDur means the fetch was waiting for
		// data to exist rather than for this router to be free - the
		// difference between "nothing to read yet" and "the router is slow".
		// A large cycle with a small pollDur points the other way.
		pollDur := time.Since(pollStart)
		var cycle time.Duration
		if !lastPoll.IsZero() {
			cycle = pollStart.Sub(lastPoll)
		}
		lastPoll = pollStart

		// How long these records waited in Kafka before this poll handed
		// them over - the record's produce timestamp against the moment we
		// picked it up. Queueing, distinct from the work below.
		//
		// min and max matter as much as the mean, and they exist to tell two
		// very different stories apart: a batch whose records are all about
		// equally old was fetched as one stale snapshot (the client had not
		// refetched since the previous poll), while a batch spanning from
		// near-zero to about one cycle old was fetched live as it arrived.
		// The first means this loop's poll is what delays records; the second
		// means it does not, and the wait is elsewhere. Only the mean, which
		// both produce, cannot say which.
		var kafkaWait time.Duration
		var waitMin, waitMax time.Duration
		for i, rec := range recs {
			w := time.Since(rec.Timestamp)
			if i == 0 || w < waitMin {
				waitMin = w
			}
			if w > waitMax {
				waitMax = w
			}
			kafkaWait += w
		}
		kafkaWait /= time.Duration(len(recs))

		out := make([]*kgo.Record, 0, len(recs)*2)
		procStart := time.Now()
		for _, rec := range recs {
			out = append(out, r.Process(rec)...)
		}
		procDur := time.Since(procStart)

		if err := sess.Begin(); err != nil {
			slog.Error("router: beginning transaction", "err", err)
			// The failure that matters here is "already in a transaction". A
			// failed End - the abort on the produce-failure path below, or the
			// commit - leaves this session inside an open transaction, and from
			// that moment every Begin fails the same way. Backoff cannot change
			// that state, so the router stops consuming *without exiting*:
			// nothing restarts it, its lag stays frozen, and every event it
			// should have routed is simply never delivered.
			//
			// Observed live after a broker restart: r1 and r3 failed Begin
			// every 30 s for the rest of the suite while their consumer lag sat
			// unmoving at 37,598, and the four scenarios that followed each
			// lost 31k-38k deliveries to it.
			//
			// publish-api carries txnStale for precisely this case - "without
			// clearing it first, one bad abort would take the batch path down
			// for the rest of the process's life" - so clear it here the same
			// way rather than retrying Begin against a state only an abort can
			// leave behind.
			if _, abErr := sess.End(ctx, kgo.TryAbort); abErr != nil {
				// Still inside it, and if the broker is down that is most
				// likely why Begin failed too. Keep the escalating backoff and
				// retry the whole sequence when it may have recovered.
				slog.Warn("router: aborting the open transaction", "err", abErr)
				if !sleep(ctx, backoff) {
					return nil
				}
				backoff = nextBackoff(backoff)
				continue
			}
			// Cleared: the state that made Begin fail is gone, so drop back to
			// the base delay instead of escalating as though the broker were
			// still down - the next Begin is one second away, not thirty.
			backoff = errorBackoff
			if !sleep(ctx, backoff) {
				return nil
			}
			continue
		}
		// The transaction is open from here; an abort reports how long it
		// lasted so a timeout can be told from a revoke (see below).
		begun := time.Now()
		// One transaction covers the delivery records, their pending
		// status records and the offset commit: End(TryCommit) flushes
		// everything atomically (section 9).
		//
		// ProduceSync and End are timed apart because they are different
		// costs - waiting for record acks versus committing markers across
		// every partition the batch touched - and they point at different
		// fixes: producer tuning versus the transaction itself.
		res := sess.ProduceSync(ctx, out...)
		syncDur := time.Since(begun)
		if res.FirstErr() != nil {
			slog.Error("router: producing batch", "err", res.FirstErr())
			if _, err := sess.End(ctx, kgo.TryAbort); err != nil {
				slog.Error("router: aborting transaction", "err", err)
			}
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff)
			continue
		}
		committed, err := sess.End(ctx, kgo.TryCommit)
		endDur := time.Since(begun) - syncDur
		if ctx.Err() != nil {
			return nil // shutdown mid-flush, not a failure
		}
		if err != nil {
			// franz-go documents every End error as non-retryable:
			// the transactional id is in a failed state or its retry
			// limit was hit. Continuing to poll would consume events
			// and commit nothing, ever - exit so Kubernetes restarts
			// the pod with a fresh session instead.
			return fmt.Errorf("router: committing transaction: %w", err)
		}
		if !committed {
			elapsed := time.Since(begun)
			// End aborts without an error when the group rebalanced
			// or the offset commit was rejected. Observed live: the
			// first transaction of every router pod on a cold
			// cluster, while __transaction_state was still being
			// created, came back as (false, nil) and was previously
			// indistinguishable from success.
			//
			// Nothing is lost: the aborted produce is rolled back
			// and franz-go rewinds the consumer to the last
			// committed offsets, so this very batch is polled again
			// and re-routed. That makes it visible here, and the
			// backoff stops a persistently aborting batch from
			// spinning. Downstream consumers do not see the aborted
			// copy at all (kafkautil.NewClient is read_committed).
			slog.Warn("router: transaction aborted, batch will be re-consumed",
				"records", len(out),
				"elapsed", elapsed.Round(time.Millisecond),
				// The distinction that matters when tuning
				// KAFKA_TRANSACTION_TIMEOUT: past it the broker dropped the
				// transaction, which is what the value buys and costs; short
				// of it, a revoke or a rejected offset commit did.
				"past_txn_timeout", elapsed >= txnTimeout,
				"txn_timeout", txnTimeout,
				"backoff", backoff)
			if !sleep(ctx, backoff) {
				return nil
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = errorBackoff

		// Committed: account for this batch. These three are the router's
		// share of publish-to-pending; the generator and publish-api are
		// measured separately, so the four together explain the whole figure.
		batches++
		if batches == 1 {
			waitMinAll, waitMaxAll = waitMin, waitMax
		} else {
			if waitMin < waitMinAll {
				waitMinAll = waitMin
			}
			if waitMax > waitMaxAll {
				waitMaxAll = waitMax
			}
		}
		sumWait += kafkaWait
		sumProc += procDur
		sumSync += syncDur
		sumEnd += endDur
		sumPoll += pollDur
		sumCycle += cycle
		if batches >= 50 {
			slog.Info("router: stage timings",
				"kafka_wait_ms", sumWait.Milliseconds()/int64(batches),
				"kafka_wait_min_ms", waitMinAll.Milliseconds(),
				"kafka_wait_max_ms", waitMaxAll.Milliseconds(),
				"process_ms", sumProc.Milliseconds()/int64(batches),
				"produce_sync_ms", sumSync.Milliseconds()/int64(batches),
				"produce_end_ms", sumEnd.Milliseconds()/int64(batches),
				"poll_ms", sumPoll.Milliseconds()/int64(batches),
				"cycle_ms", sumCycle.Milliseconds()/int64(batches),
				"batches", batches, "records", len(recs))
			batches, sumWait, sumProc, sumSync, sumEnd, sumPoll, sumCycle = 0, 0, 0, 0, 0, 0, 0
			waitMinAll, waitMaxAll = 0, 0
		}
	}
	return nil
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
