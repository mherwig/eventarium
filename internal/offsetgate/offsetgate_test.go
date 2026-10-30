package offsetgate

import (
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

const topic = "pubsub.deliveries"

func rec(offset int64, epoch int32) *kgo.Record {
	return &kgo.Record{Topic: topic, Partition: 1, Offset: offset, LeaderEpoch: epoch}
}

// head is the single commit point a test expects, or -1 for "nothing may be
// committed".
func head(t *testing.T, g *Gate) int64 {
	t.Helper()
	points := g.CommitPoints()
	switch len(points) {
	case 0:
		return -1
	case 1:
	default:
		t.Fatalf("expected one topic, got %d", len(points))
	}
	partitions := points[topic]
	if len(partitions) != 1 {
		t.Fatalf("expected one partition, got %d", len(partitions))
	}
	if _, ok := partitions[1]; !ok {
		t.Fatalf("expected partition 1, got %+v", partitions)
	}
	return partitions[1].Offset
}

// The whole point of the gate: a record that has no durable outcome holds the
// commit point of its partition back, no matter how many later records are
// already durable. Committing past it is what lost data before.
func TestUnsettledRecordHoldsTheCommitPoint(t *testing.T) {
	g := New()
	g.Track(rec(100, 4), rec(101, 4), rec(102, 4))

	if got := g.Settle(topic, 1, 100); got == nil {
		t.Fatal("settling 100 should commit 101 (101 and 102 are still in flight)")
	} else if got[topic][1].Offset != 101 || got[topic][1].Epoch != 4 {
		t.Fatalf("expected {Epoch:4 Offset:101}, got %+v", got[topic][1])
	}

	// 102 is durable, but 101 is not: the commit point may not move.
	if got := g.Settle(topic, 1, 102); got != nil {
		t.Fatalf("settling 102 while 101 is in flight must not commit, got %+v", got)
	}
	if got := head(t, g); got != 101 {
		t.Fatalf("commit point = %d, want 101", got)
	}

	// Now 101 becomes durable (the park retry got through): the point jumps to
	// the end of what was seen.
	if got := g.Settle(topic, 1, 101); got == nil {
		t.Fatal("settling the last in-flight record should commit")
	} else if got[topic][1].Offset != 103 {
		t.Fatalf("commit point = %d, want 103", got[topic][1].Offset)
	}
	if n := g.Pending(); n != 0 {
		t.Fatalf("pending = %d, want 0", n)
	}
}

// Settling out of order is normal (workers finish in any order), and the point
// may only move to the lowest record that is still missing.
func TestSettleOutOfOrder(t *testing.T) {
	g := New()
	g.Track(rec(10, 1), rec(11, 1), rec(12, 1), rec(13, 1))

	if got := g.Settle(topic, 1, 13); got != nil {
		t.Fatalf("only the highest record is durable, nothing may commit: %+v", got)
	}
	if got := g.Settle(topic, 1, 12); got != nil {
		t.Fatalf("11 is still in flight, nothing may commit: %+v", got)
	}
	g.Settle(topic, 1, 11)
	if got := g.Settle(topic, 1, 10); got == nil {
		t.Fatal("the last record should have committed")
	} else if got[topic][1].Offset != 14 {
		t.Fatalf("commit point = %d, want 14", got[topic][1].Offset)
	}
}

// Two commits in a row must keep the record the first one was expressed by
// until the second one replaces it, otherwise the epoch is gone.
func TestPruneKeepsTheCommitRecord(t *testing.T) {
	g := New()
	g.Track(rec(50, 2), rec(51, 2))
	g.Settle(topic, 1, 50) // head = 51, expressed by record 50
	g.Settle(topic, 1, 51) // head = 52, expressed by record 51

	points := g.CommitPoints()
	if got := points[topic][1]; got.Offset != 52 || got.Epoch != 2 {
		t.Fatalf("got %+v, want {Epoch:2 Offset:52}", got)
	}
	if _, ok := g.parts[key{topic, 1}].epochs[51]; !ok {
		t.Fatal("the record the commit point is expressed by was pruned")
	}
	if _, ok := g.parts[key{topic, 1}].epochs[50]; ok {
		t.Fatal("record 50 is behind the commit point and should have been pruned")
	}
}

// A gap below the commit point (compaction, retention, an offset reset) leaves
// no record to express it with; the gate then commits up to the last record it
// does know, which is less but never more.
func TestGapBelowCommitPoint(t *testing.T) {
	g := New()
	g.Track(rec(70, 9), rec(74, 9)) // 71..73 never reach this consumer
	if got := g.Settle(topic, 1, 70); got == nil {
		t.Fatal("expected a commit")
	} else if got[topic][1].Offset != 71 {
		t.Fatalf("commit point = %d, want 71", got[topic][1].Offset)
	}
	if got := g.Settle(topic, 1, 74); got == nil {
		t.Fatal("expected a commit")
	} else if got[topic][1].Offset != 75 {
		t.Fatalf("commit point = %d, want 75", got[topic][1].Offset)
	}
}

// Nothing was ever tracked below the lowest record of a partition, so there is
// no record to express a commit with - and committing nothing is right, the
// group's committed offset already sits at or below that record. The point
// becomes committable as soon as everything tracked is durable.
func TestNothingBelowTheLowestRecord(t *testing.T) {
	g := New()
	g.Track(rec(5, 3), rec(9, 3))
	if got := g.Settle(topic, 1, 9); got != nil {
		t.Fatalf("5 is still in flight and nothing below it was tracked, got %+v", got)
	}
	if got := head(t, g); got != -1 {
		t.Fatalf("commit point = %d, want nothing committed", got)
	}
	// 5 durable as well: the point jumps to the end of what was seen, which
	// record 9 expresses.
	if got := g.Settle(topic, 1, 5); got == nil {
		t.Fatal("expected a commit")
	} else if got[topic][1].Offset != 10 {
		t.Fatalf("commit point = %d, want 10", got[topic][1].Offset)
	}
}

// Settling something the gate does not know must not move anything: inbox rows
// carry the offsets of the deliveries topic but are not offset-tracked, and a
// record of a partition that has been taken away is somebody else's business.
func TestSettleUnknownIsNoop(t *testing.T) {
	g := New()
	g.Track(rec(3, 1))
	if got := g.Settle("other.topic", 1, 3); got != nil {
		t.Fatalf("unknown topic committed: %+v", got)
	}
	if got := g.Settle(topic, 7, 3); got != nil {
		t.Fatalf("unknown partition committed: %+v", got)
	}
	g.Settle(topic, 1, 3)
	if got := g.Settle(topic, 1, 3); got != nil {
		t.Fatalf("settling twice committed again: %+v", got)
	}
}

// A rebalance hands partition 1 to another member: this consumer may still
// hold records of it, but it must stop tracking - and therefore stop
// committing - them, or it would race the new owner.
func TestKeepDropsLostPartitions(t *testing.T) {
	g := New()
	g.Track(rec(40, 1))
	g.Settle(topic, 1, 40)

	g.Track(&kgo.Record{Topic: topic, Partition: 2, Offset: 8, LeaderEpoch: 1})
	g.Keep(map[string][]int32{topic: {2}})

	if len(g.parts) != 1 {
		t.Fatalf("gate keeps %d partitions, want only partition 2", len(g.parts))
	}
	if _, ok := g.parts[key{topic, 2}]; !ok {
		t.Fatal("partition 2 was dropped instead of kept")
	}
	if got := head(t, g); got != -1 {
		t.Fatalf("commit point = %d, want nothing committed for the unowned partition", got)
	}
	// A late settle for the partition that was taken away changes nothing.
	if got := g.Settle(topic, 1, 40); got != nil {
		t.Fatalf("settling a partition we no longer own committed: %+v", got)
	}
}

// Several partitions of one topic are tracked independently: one stalled
// partition must not hold back the others.
func TestPartitionsAreIndependent(t *testing.T) {
	g := New()
	g.Track(
		&kgo.Record{Topic: topic, Partition: 0, Offset: 1, LeaderEpoch: 1},
		&kgo.Record{Topic: topic, Partition: 1, Offset: 1, LeaderEpoch: 1},
	)
	g.Settle(topic, 0, 1) // partition 0 is fully durable
	points := g.CommitPoints()
	want := map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: 1, Offset: 2}}}
	if !reflect.DeepEqual(points, want) {
		t.Fatalf("got %+v, want %+v", points, want)
	}
}
