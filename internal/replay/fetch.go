package replay

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"pubsub/internal/kafkautil"
)

// coord identifies one original event record: the events topic of its
// retention class plus the partition and offset that the status record stored
// (sections 9 and 12). The replay service reads the event back from exactly
// there instead of re-publishing anything.
type coord struct {
	Topic     string
	Partition int32
	Offset    int64
}

// tp is the (topic, partition) pair a coordinate belongs to; the reader groups
// by it so one bounds check and one consumer cover many records.
type tp struct {
	topic     string
	partition int32
}

// eventReader reads original event records back from the events topics. It is
// an interface so the worker's not_replayable classification (section 11) can
// be unit tested against fakes without a broker.
type eventReader interface {
	// Read returns the record for every requested coordinate. A coordinate
	// without a map entry has no record — pruned by retention, beyond the
	// log end, or its topic is gone — and is counted not_replayable
	// (section 11, retention limit).
	Read(ctx context.Context, coords []coord) (map[coord]*kgo.Record, error)
}

// readIdle bounds how long one Read waits for records that the bounds check
// said exist, before it reports them absent. In-range offsets arrive
// immediately; the idle bound only exists so a wedged broker cannot hang a
// job forever.
const readIdle = 15 * time.Second

// EventReader is the Kafka implementation of eventReader: a one-shot read
// with no consumer group, exactly as section 11 describes it ("re-reads the
// original events from the events topic of their retention class by the
// stored class, partition and offset").
type EventReader struct {
	brokers []string
}

// NewEventReader builds a reader over the configured brokers
// (KAFKA_BROKERS via kafkautil.Bootstrap).
func NewEventReader() *EventReader { return &EventReader{brokers: kafkautil.Bootstrap()} }

// Read resolves every coordinate to a record in two steps:
//
//  1. a ListOffsets call per side of the log (earliest and latest) tells which
//     offsets still exist at all — an offset outside [earliest, latest) can
//     never be fetched and is reported absent immediately instead of making
//     the consumer wait for records that will not come;
//  2. the surviving offsets are consumed with a direct partition assignment at
//     the exact offsets (kgo.ConsumePartitions, no group, no commit), grouped
//     by (topic, partition) so one client covers the whole batch. A fetched
//     record is only accepted at the exact offset that was asked for, so a
//     clamped or rewound reset can never hand back the wrong event.
func (r *EventReader) Read(ctx context.Context, coords []coord) (map[coord]*kgo.Record, error) {
	got := make(map[coord]*kgo.Record, len(coords))
	if len(coords) == 0 {
		return got, nil
	}

	groups := make(map[tp][]int64, len(coords))
	unique := make(map[coord]bool, len(coords))
	for _, c := range coords {
		if unique[c] {
			continue
		}
		unique[c] = true
		groups[tp{c.Topic, c.Partition}] = append(groups[tp{c.Topic, c.Partition}], c.Offset)
	}
	for k := range groups {
		offsets := groups[k]
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		groups[k] = offsets
	}

	probe, err := kgo.NewClient(kgo.SeedBrokers(r.brokers...))
	if err != nil {
		return nil, fmt.Errorf("replay: connecting to kafka: %w", err)
	}
	defer probe.Close()

	earliest, err := listOffsets(ctx, probe, groups, -2) // log start
	if err != nil {
		return nil, err
	}
	latest, err := listOffsets(ctx, probe, groups, -1) // log end (exclusive)
	if err != nil {
		return nil, err
	}

	// Partition the coordinates into "still in the log" and "gone".
	live := make(map[tp][]int64, len(groups))
	for key, offsets := range groups {
		lo, hi := earliest[key], latest[key]
		if lo.err != nil || hi.err != nil {
			// A missing topic or partition means the record can never be
			// read back; anything else (a broker hiccup) fails the job
			// instead of silently under-replaying it.
			if lo.err == kerr.UnknownTopicOrPartition || hi.err == kerr.UnknownTopicOrPartition {
				slog.Warn("replay: events topic-partition unavailable, coordinates are not replayable",
					"topic", key.topic, "partition", key.partition, "err", lo.err)
				continue
			}
			if lo.err != nil {
				return nil, fmt.Errorf("replay: offsets for %s[%d]: %w", key.topic, key.partition, lo.err)
			}
			return nil, fmt.Errorf("replay: offsets for %s[%d]: %w", key.topic, key.partition, hi.err)
		}
		for _, off := range offsets {
			if off >= lo.offset && off < hi.offset {
				live[key] = append(live[key], off)
			}
		}
	}

	if len(live) == 0 {
		return got, nil
	}

	// One direct-assignment consumer for the whole batch, started at the
	// lowest live offset per partition; the loop below walks forward from
	// there and keeps every record whose offset is one of the wanted ones.
	tps := make(map[string]map[int32]kgo.Offset, 4)
	for key, offsets := range live {
		if tps[key.topic] == nil {
			tps[key.topic] = make(map[int32]kgo.Offset)
		}
		tps[key.topic][key.partition] = kgo.NewOffset().At(offsets[0])
	}
	consumer, err := kgo.NewClient(kgo.SeedBrokers(r.brokers...), kgo.ConsumePartitions(tps))
	if err != nil {
		return nil, fmt.Errorf("replay: opening the event reader: %w", err)
	}
	defer consumer.Close()

	pending := make(map[coord]bool, len(coords))
	maxWanted := make(map[tp]int64, len(live))
	for key, offsets := range live {
		maxWanted[key] = offsets[len(offsets)-1]
		for _, off := range offsets {
			pending[coord{key.topic, key.partition, off}] = true
		}
	}

	for len(pending) > 0 {
		pctx, cancel := context.WithTimeout(ctx, readIdle)
		fetches := consumer.PollFetches(pctx)
		cancel()
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if fetches.IsClientClosed() {
			break
		}
		recs := fetches.Records()
		if len(recs) == 0 {
			if err := fetches.Err(); err != nil {
				slog.Warn("replay: event fetch gave up on the remaining coordinates", "err", err)
			}
			// Idle timeout or a fetch error: whatever is still pending is
			// reported absent, never silently substituted.
			break
		}
		if err := fetches.Err(); err != nil {
			slog.Warn("replay: event fetch error, processing partial batch", "err", err)
		}
		passed := make(map[tp]bool)
		for _, rec := range recs {
			c := coord{rec.Topic, rec.Partition, rec.Offset}
			if pending[c] {
				got[c] = rec
				delete(pending, c)
			}
			if rec.Offset > maxWanted[tp{rec.Topic, rec.Partition}] {
				passed[tp{rec.Topic, rec.Partition}] = true
			}
		}
		// The reader walked past the highest wanted offset of a partition
		// without seeing the wanted ones: they were deleted by retention
		// after the bounds check. Drop them as not replayable.
		if len(passed) > 0 {
			for c := range pending {
				if passed[tp{c.Topic, c.Partition}] {
					delete(pending, c)
				}
			}
		}
	}
	return got, nil
}

// offsetWindow is one side of the log of a partition.
type offsetWindow struct {
	offset int64
	err    error
}

// listOffsets asks the brokers for one timestamp side (-2 earliest, -1
// latest) of every partition through a sharded ListOffsets request.
func listOffsets(ctx context.Context, cl *kgo.Client, groups map[tp][]int64, timestamp int64) (map[tp]offsetWindow, error) {
	topics := make(map[string][]int32, 4)
	for key := range groups {
		topics[key.topic] = append(topics[key.topic], key.partition)
	}
	req := kmsg.NewPtrListOffsetsRequest()
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		parts := topics[name]
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		t := kmsg.NewListOffsetsRequestTopic()
		t.Topic = name
		for _, p := range parts {
			// The request carries one timestamp per topic-partition, so
			// every partition slot is stamped with the side we ask for.
			tpReq := kmsg.NewListOffsetsRequestTopicPartition()
			tpReq.Partition = p
			tpReq.Timestamp = timestamp
			t.Partitions = append(t.Partitions, tpReq)
		}
		req.Topics = append(req.Topics, t)
	}

	resp, err := cl.Request(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("replay: listing offsets: %w", err)
	}
	lr, ok := resp.(*kmsg.ListOffsetsResponse)
	if !ok {
		return nil, fmt.Errorf("replay: listing offsets: unexpected response %T", resp)
	}

	out := make(map[tp]offsetWindow, len(groups))
	for _, t := range lr.Topics {
		for _, p := range t.Partitions {
			key := tp{t.Topic, p.Partition}
			if p.ErrorCode == 0 && p.Offset >= 0 {
				out[key] = offsetWindow{offset: p.Offset}
			} else if p.ErrorCode != 0 {
				out[key] = offsetWindow{err: kerr.ErrorForCode(p.ErrorCode)}
			} else {
				// Offset -1 without an error code: no offset for this
				// timestamp exists, e.g. an empty partition.
				out[key] = offsetWindow{offset: p.Offset, err: kerr.UnknownTopicOrPartition}
			}
		}
	}
	// A partition the broker did not answer for is unknown to it.
	for key := range groups {
		if _, ok := out[key]; !ok {
			out[key] = offsetWindow{err: kerr.UnknownTopicOrPartition}
		}
	}
	return out, nil
}

// classify decides whether one fetched event record may be replayed
// (section 11, retention limit). The record must still exist — the absence
// was decided by Read — and its expires_at header must not have passed; both
// failures are counted not_replayable and the event is left untouched. An
// event without an expiry header stays replayable, mirroring the router,
// which only warns about it (publish-api stamps the header for every class,
// section 12, and a zero expiry never expires downstream).
func classify(rec *kgo.Record, now time.Time) (ok bool, reason string) {
	if rec == nil {
		return false, "original event record is no longer in the events topic"
	}
	if expires := headerTime(rec, kafkautil.HeaderExpires); !expires.IsZero() && !now.Before(expires) {
		return false, "expires_at " + expires.UTC().Format(time.RFC3339) + " has passed"
	}
	return true, ""
}

// headerTime reads a unix-milliseconds platform header as a time.Time; an
// absent or malformed header yields the zero time, the same convention
// internal/routing uses for the very same headers (section 5).
func headerTime(rec *kgo.Record, key string) time.Time {
	s := kafkautil.HGet(rec, key)
	if s == "" {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}
