// Package offsetgate decides which offsets of a Kafka consumer group may be
// committed.
//
// franz-go's MarkCommitRecords advances a per-partition watermark to the
// highest marked record, which is only sound when every record below it has
// already reached a durable outcome. The two delivery stages cannot promise
// that: a record stays in memory while it is queued, retried or waiting for a
// park to succeed, and a watermark that steps over such a record commits it
// away - a crash after that loses it silently. The dispatcher had exactly that
// hole (an unmarked park failure was skipped by the next mark in the same
// partition) and so did the gateway (a failed park nulled its batch while the
// batch's offsets were committed anyway).
//
// A Gate inverts the rule. Every record a poll returns is handed to Track, and
// the moment one of them is durable - delivered, parked in the inbox,
// terminally dropped - it is handed to Settle. The commit point of a partition
// is the lowest tracked offset that is not durable yet, or the end of what
// this consumer has seen when all of them are, so an in-memory record always
// holds the commit point back. The cost is duplicates on replay, which section
// 8 of the spec already promises; the alternative is loss.
//
// The gate knows nothing about Kafka beyond the records it is given: it never
// commits itself. Settle returns the commit points that moved in the shape
// kgo.MarkCommitOffsets and kgo.CommitOffsets take them, so the caller decides
// when and how they reach the broker.
package offsetgate

import (
	"math"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Gate tracks the records one consumer has taken off the topic and which of
// them are durable. It is safe for concurrent use: a delivery stage settles
// from its poll goroutine while a background park retry settles from another,
// and a rebalance drops partitions from franz-go's group goroutine.
type Gate struct {
	mu    sync.Mutex
	parts map[key]*part
}

// key is one topic-partition of the consumer group.
type key struct {
	topic     string
	partition int32
}

// part is the bookkeeping of one partition.
type part struct {
	// epochs keeps the leader epoch of every tracked offset from head-1 up.
	// A commit names the record it commits up to (offset+1) together with that
	// record's epoch (KIP-320), so the epochs of the offsets the commit point
	// may still move to have to survive - but never the payload, only 16 bytes
	// per offset, which is what keeps a stalled partition cheap.
	epochs map[int64]int32
	// pending is the set of tracked offsets without a durable outcome yet.
	pending map[int64]struct{}
	// head is the commit point: everything below it is durable, nothing at or
	// above it may be committed. Zero means nothing has been committed yet.
	head int64
	// seen is the highest tracked offset + 1, the commit point to fall back to
	// once every tracked record is durable.
	seen int64
}

// New returns an empty gate.
func New() *Gate {
	return &Gate{parts: make(map[key]*part)}
}

// Track registers fetched records as in flight. Every record a poll returns
// has to be tracked before it is passed on: a record the gate never learns
// about is invisible to the commit point, which could then step over it.
func (g *Gate) Track(recs ...*kgo.Record) {
	if len(recs) == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, rec := range recs {
		k := key{rec.Topic, rec.Partition}
		p := g.parts[k]
		if p == nil {
			p = &part{
				epochs:  make(map[int64]int32, 16),
				pending: make(map[int64]struct{}, 16),
			}
			g.parts[k] = p
		}
		p.epochs[rec.Offset] = rec.LeaderEpoch
		p.pending[rec.Offset] = struct{}{}
		if rec.Offset+1 > p.seen {
			p.seen = rec.Offset + 1
		}
	}
}

// Settle marks one tracked record as durable and returns the commit points that
// moved as a result, ready for kgo's MarkCommitOffsets or CommitOffsets. A nil
// return means nothing may be committed for that partition yet - some tracked
// record below the point is still in flight - and the caller must not commit
// anything in its place. Settling something the gate does not know (an inbox
// row, a record of a partition a rebalance took away) is a no-op.
func (g *Gate) Settle(topic string, partition int32, offset int64) map[string]map[int32]kgo.EpochOffset {
	k := key{topic, partition}
	g.mu.Lock()
	defer g.mu.Unlock()
	p := g.parts[k]
	if p == nil {
		return nil
	}
	delete(p.pending, offset)
	return g.advance(k, p)
}

// CommitPoints returns the current commit point of every partition the gate
// has committed something for, for consumers that commit explicitly after a
// batch instead of marking.
func (g *Gate) CommitPoints() map[string]map[int32]kgo.EpochOffset {
	g.mu.Lock()
	defer g.mu.Unlock()
	var out map[string]map[int32]kgo.EpochOffset
	for k, p := range g.parts {
		if p.head <= 0 {
			continue
		}
		epoch, ok := p.epochs[p.head-1]
		if !ok {
			continue // not reachable: head is always expressed by a tracked record
		}
		if out == nil {
			out = make(map[string]map[int32]kgo.EpochOffset, 2)
		}
		partitions := out[k.topic]
		if partitions == nil {
			partitions = make(map[int32]kgo.EpochOffset, 2)
			out[k.topic] = partitions
		}
		partitions[k.partition] = kgo.EpochOffset{Epoch: epoch, Offset: p.head}
	}
	return out
}

// Keep drops every partition the consumer no longer owns. A rebalance hands a
// partition to another member while this consumer may still hold records of it
// in memory; those records can settle later, and settling - and therefore
// committing - a partition somebody else now owns would race the new owner.
// Dropping is the safe direction: the commit point stays where it was, so the
// new owner replays from there. Call it from OnPartitionsAssigned, which
// franz-go invokes with the full assignment after every rebalance.
func (g *Gate) Keep(assigned map[string][]int32) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for k := range g.parts {
		owned := false
		for _, partition := range assigned[k.topic] {
			if partition == k.partition {
				owned = true
				break
			}
		}
		if !owned {
			delete(g.parts, k)
		}
	}
}

// Pending counts the tracked records that are not durable yet. It is a
// diagnostic: a number that only grows means a store is refusing parks and the
// commit points of those partitions are standing still.
func (g *Gate) Pending() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := 0
	for _, p := range g.parts {
		n += len(p.pending)
	}
	return n
}

// advance recomputes the commit point of one partition and returns it when it
// moved. Callers hold g.mu.
func (g *Gate) advance(k key, p *part) map[string]map[int32]kgo.EpochOffset {
	target := p.seen
	if len(p.pending) > 0 {
		target = math.MaxInt64
		for offset := range p.pending {
			if offset < target {
				target = offset
			}
		}
	}
	if target <= p.head {
		return nil
	}
	// The commit point is expressed as the record below it: a commit names
	// "the offset after this record" together with that record's epoch.
	epoch, ok := p.epochs[target-1]
	if !ok {
		// A gap below the point (compaction, retention, a reset that skipped
		// offsets): commit up to the last tracked record instead. Committing
		// less than is safe is always allowed; committing more never is.
		marker, found := int64(-1), false
		for offset := range p.epochs {
			if offset < target && (!found || offset > marker) {
				marker, found = offset, true
			}
		}
		if !found || marker+1 <= p.head {
			return nil
		}
		epoch, target = p.epochs[marker], marker+1
	}
	p.head = target
	g.prune(p)
	return map[string]map[int32]kgo.EpochOffset{
		k.topic: {k.partition: {Epoch: epoch, Offset: target}},
	}
}

// prune forgets the bookkeeping the commit point has moved past. Callers hold
// g.mu.
func (g *Gate) prune(p *part) {
	// Everything below head is durable by the definition of head, so a pending
	// entry down there is stale bookkeeping (a record re-fetched after it was
	// already committed) rather than work to hold back.
	for offset := range p.pending {
		if offset < p.head {
			delete(p.pending, offset)
		}
	}
	// The record head is expressed by stays: re-committing the same point
	// after a failed commit needs its epoch.
	for offset := range p.epochs {
		if offset < p.head-1 {
			delete(p.epochs, offset)
		}
	}
}
