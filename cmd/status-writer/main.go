// status-writer consumes the delivery-status Kafka topic (section 9) and
// batch inserts every status.Record into the ClickHouse delivery_status table
// of internal/chstore. It is deliberately dumb: the topic is the queue, and
// ClickHouse is only ever written through this single consumer group
// ("status-writer").
//
// Batch inserts are the reason status data lags by about one to five seconds
// (section 9): the buffer is flushed every second or every 10k records,
// whichever comes first. Kafka offsets are committed only after the insert
// succeeded; on a ClickHouse outage nothing is committed, the records simply
// stay in Kafka (section 15.3) and the insert is retried with backoff.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/chstore"
	"pubsub/internal/config"
	"pubsub/internal/kafkautil"
	"pubsub/internal/status"
)

const (
	// Consumer group over delivery-status (docs/SPIKE.md).
	consumerGroup = "status-writer"
	// Flush triggers (docs/SPIKE.md, spec section 9): one second or 10k records.
	flushInterval = time.Second
	flushRows     = 10_000
	// pollBatch is the upper bound of one PollRecords call.
	pollBatch = 1000
	// maxBuffered caps the in-memory buffer during a ClickHouse outage.
	// Beyond it the consumer stops pulling new records out of Kafka
	// (uncommitted records stay in the topic, section 15.3) instead of
	// growing the heap without bound.
	maxBuffered = 100_000
	// Retry backoff for failed flushes: 1s doubling up to 30s.
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
	// drainTimeout bounds the final flush on graceful shutdown.
	drainTimeout = 10 * time.Second
	// maxCommitFailures bounds the retrying of offset commits. After a
	// rebalance revoked the partitions the commit can never succeed; the
	// offsets are then dropped and the records may be re-consumed
	// (at-least-once, the table is append-only, section 9).
	maxCommitFailures = 5

	defaultCHDSN = "clickhouse://pubsub:pubsub@localhost:9000/pubsub"

	// The insert spells out the table columns of internal/chstore so that a
	// schema change breaks the build, not the data.
	insertStmt = "INSERT INTO " + chstore.Table +
		" (ts, owner, event_ref, delivery_id, subscription_id, status, reason," +
		" attempt, http_code, latency_ms, connection_id, replay_id," +
		" retention_class, src_partition, src_offset)" +
		" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

func main() {
	setupLogging()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := chstore.Open(ctx, config.Str("CH_DSN", defaultCHDSN))
	if err != nil {
		slog.Error("status-writer: clickhouse", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	if err := chstore.Ensure(ctx, conn); err != nil {
		slog.Error("status-writer: creating table", "err", err)
		os.Exit(1)
	}

	// Auto-commit is off: offsets are committed by the flusher, after the
	// insert, and only then.
	cl, err := kafkautil.NewClient(
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(kafkautil.TopicStatus),
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		slog.Error("status-writer: kafka", "err", err)
		os.Exit(1)
	}
	defer cl.Close()

	slog.Info("status-writer: started", "group", consumerGroup, "topic", kafkautil.TopicStatus)
	w := &writer{conn: conn, cl: cl}
	if err := w.run(ctx); err != nil {
		slog.Error("status-writer: exiting with error", "err", err)
		os.Exit(1)
	}
}

// entry is one parsed record plus the Kafka record it came from, so that the
// offset can be committed only after the insert.
type entry struct {
	rec status.Record
	src *kgo.Record
}

type writer struct {
	conn clickhouse.Conn
	cl   *kgo.Client

	buf            []entry       // parsed records waiting for the insert
	toCommit       []*kgo.Record // inserted records waiting for the offset commit
	commitFailures int
}

// run consumes until ctx is cancelled, then flushes what is left. A failed
// flush never terminates the process: the records stay buffered and in Kafka.
func (w *writer) run(ctx context.Context) error {
	recs := make(chan []*kgo.Record)
	go w.poll(ctx, recs)

	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()

	var (
		failures  int       // consecutive failed flushes, drives the backoff
		notBefore time.Time // do not attempt another insert before this
	)
	attempt := func() {
		if err := w.flushOnce(ctx); err != nil {
			failures, notBefore = backoff(failures + 1)
			slog.Error("status-writer: flush failed, records stay in Kafka",
				"err", err, "buffered", len(w.buf),
				"retry_in", time.Until(notBefore).Round(time.Millisecond))
			return
		}
		failures = 0
		notBefore = time.Time{}
	}

	for {
		// Backpressure: while a ClickHouse outage has filled the buffer,
		// stop selecting on the poll channel. The polling goroutine then
		// blocks on its send and holds at most one fetch batch; the rest
		// stays uncommitted in Kafka.
		var in chan []*kgo.Record = recs
		if len(w.buf) >= maxBuffered {
			in = nil
		}
		select {
		case <-ctx.Done():
			return w.drain()
		case fetched := <-in:
			for _, r := range fetched {
				if e, ok := parse(r); ok {
					w.buf = append(w.buf, e)
				}
			}
			if len(w.buf) >= flushRows && time.Now().After(notBefore) {
				attempt()
			}
		case <-ticker.C:
			if len(w.buf) == 0 && len(w.toCommit) == 0 {
				continue
			}
			if time.Now().Before(notBefore) {
				continue
			}
			attempt()
		}
	}
}

// poll moves fetches from Kafka into the channel for the main loop. All
// buffer ownership stays in run; this goroutine only shuttles.
func (w *writer) poll(ctx context.Context, out chan<- []*kgo.Record) {
	for ctx.Err() == nil {
		fetches := w.cl.PollRecords(ctx, pollBatch)
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("status-writer: fetch error",
				"topic", topic, "partition", partition, "err", err)
		})
		rs := fetches.Records()
		if len(rs) == 0 {
			continue // cancelled or empty poll
		}
		select {
		case out <- rs:
		case <-ctx.Done():
			return
		}
	}
}

// flushOnce inserts the buffered records and then commits the Kafka offsets
// of everything that has been inserted. On failure the buffer is kept: a
// ClickHouse outage must not lose records, they simply stay in Kafka
// (section 15.3) and are retried with backoff.
func (w *writer) flushOnce(ctx context.Context) error {
	if len(w.buf) > 0 {
		if err := w.insert(ctx); err != nil {
			return err
		}
		for i := range w.buf {
			w.toCommit = append(w.toCommit, w.buf[i].src)
		}
		w.buf = w.buf[:0]
		slog.Debug("status-writer: inserted", "rows", len(w.toCommit))
	}
	if len(w.toCommit) > 0 {
		if err := w.cl.CommitRecords(ctx, w.toCommit...); err != nil {
			w.commitFailures++
			if w.commitFailures >= maxCommitFailures {
				// e.g. a rebalance revoked the partitions: the commit
				// can never succeed. Drop the offsets; at-least-once
				// means the records may be re-consumed and re-inserted.
				slog.Error("status-writer: dropping offsets after repeated commit failures",
					"err", err, "records", len(w.toCommit))
				w.toCommit = w.toCommit[:0]
				w.commitFailures = 0
				return nil
			}
			// The rows are already in ClickHouse; keep toCommit so the
			// next flush retries the commit (a later commit covers the
			// earlier offsets of the same partition).
			return fmt.Errorf("committing offsets: %w", err)
		}
		w.commitFailures = 0
		slog.Debug("status-writer: committed", "records", len(w.toCommit))
		w.toCommit = w.toCommit[:0]
	}
	return nil
}

func (w *writer) insert(ctx context.Context) error {
	b, err := w.conn.PrepareBatch(ctx, insertStmt)
	if err != nil {
		return fmt.Errorf("preparing batch: %w", err)
	}
	for i := range w.buf {
		r := w.buf[i].rec
		var offset uint64 // src_offset is UInt64 in ClickHouse
		if r.SrcOffset >= 0 {
			offset = uint64(r.SrcOffset)
		}
		if err := b.Append(
			r.TS, r.Owner, r.EventRef, r.DeliveryID, r.SubscriptionID,
			r.Status, r.Reason, r.Attempt, r.HTTPCode, r.LatencyMS,
			r.ConnectionID, r.ReplayID, r.RetentionClass,
			r.SrcPartition, offset,
		); err != nil {
			_ = b.Abort()
			return fmt.Errorf("appending row: %w", err)
		}
	}
	if err := b.Send(); err != nil {
		return fmt.Errorf("inserting %d rows: %w", len(w.buf), err)
	}
	return nil
}

// drain gives the buffer one last flush on graceful shutdown.
func (w *writer) drain() error {
	if len(w.buf) == 0 && len(w.toCommit) == 0 {
		slog.Info("status-writer: stopped")
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := w.flushOnce(ctx); err != nil {
		slog.Error("status-writer: final flush incomplete, uninserted records stay in Kafka",
			"err", err, "buffered", len(w.buf))
		return err
	}
	slog.Info("status-writer: stopped, buffer flushed")
	return nil
}

// parse turns a Kafka record into a table row, or reports why it was skipped.
func parse(src *kgo.Record) (entry, bool) {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(src.Value, &probe); err != nil {
		slog.Warn("status-writer: record is not a JSON object, skipped",
			"partition", src.Partition, "offset", src.Offset, "err", err)
		return entry{}, false
	}
	// Defensive (section 9): status records never carry payloads. A record
	// that does is dropped instead of copying payload bytes into
	// ClickHouse.
	for _, key := range []string{"payload", "data", "body"} {
		if v, ok := probe[key]; ok && string(v) != "" && string(v) != "null" {
			slog.Warn("status-writer: record carries a payload, skipped",
				"delivery_id", probeString(probe, "delivery_id"), "field", key)
			return entry{}, false
		}
	}
	var rec status.Record
	if err := json.Unmarshal(src.Value, &rec); err != nil {
		slog.Warn("status-writer: cannot decode status record, skipped",
			"partition", src.Partition, "offset", src.Offset, "err", err)
		return entry{}, false
	}
	if rec.DeliveryID == "" {
		slog.Warn("status-writer: record without delivery_id, skipped",
			"partition", src.Partition, "offset", src.Offset)
		return entry{}, false
	}
	if rec.TS.IsZero() {
		// Tolerate producers that forget the timestamp.
		rec.TS = time.Now().UTC()
	}
	return entry{rec: rec, src: src}, true
}

func probeString(m map[string]json.RawMessage, key string) string {
	var s string
	if v, ok := m[key]; ok {
		_ = json.Unmarshal(v, &s)
	}
	return s
}

// backoff returns the next allowed attempt time with capped exponential
// growth: 1s, 2s, 4s ... up to maxBackoff.
func backoff(failures int) (int, time.Time) {
	if failures > 16 {
		failures = 16
	}
	d := minBackoff << (failures - 1)
	if d > maxBackoff {
		d = maxBackoff
	}
	return failures, time.Now().Add(d)
}

// setupLogging sends slog to stderr at the configured level (docs/SPIKE.md).
func setupLogging() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(config.Str("LOG_LEVEL", "info")))); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}
