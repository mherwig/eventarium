package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/offsetgate"
)

const revokeTopic = "pubsub.deliveries"

func revokeRec(partition int32, offset int64) *kgo.Record {
	return &kgo.Record{Topic: revokeTopic, Partition: partition, Offset: offset, LeaderEpoch: 1}
}

// Nothing outstanding means the handover does not wait at all: the common case
// is a rebalance of a quiet partition and it must cost nothing.
func TestDrainPartitionsReturnsImmediatelyWhenQuiet(t *testing.T) {
	g := offsetgate.New()
	revoked := map[string][]int32{revokeTopic: {1}}

	start := time.Now()
	if left := drainPartitions(context.Background(), g, revoked, time.Second, time.Millisecond); left != 0 {
		t.Fatalf("left = %d, want 0", left)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("drained a quiet gate in %v, want it to return at once", elapsed)
	}
}

// The behaviour the whole callback exists for: a revoke must not return while
// a delivery for the departing partition is still running, because returning
// is what lets the group finish the rebalance and hand the partition over.
func TestDrainPartitionsWaitsForInFlightWork(t *testing.T) {
	g := offsetgate.New()
	g.Track(revokeRec(1, 40), revokeRec(1, 41))
	revoked := map[string][]int32{revokeTopic: {1}}

	// The delivery finishes a little into the wait, as a worker would.
	settled := make(chan struct{})
	go func() {
		time.Sleep(75 * time.Millisecond)
		g.Settle(revokeTopic, 1, 40)
		g.Settle(revokeTopic, 1, 41)
		close(settled)
	}()

	start := time.Now()
	left := drainPartitions(context.Background(), g, revoked, 5*time.Second, 5*time.Millisecond)
	elapsed := time.Since(start)

	if left != 0 {
		t.Fatalf("left = %d, want 0: the revoke handed the partition over mid-delivery", left)
	}
	if elapsed < 70*time.Millisecond {
		t.Fatalf("returned after %v, before the deliveries finished", elapsed)
	}
	<-settled
}

// Records of partitions that are being *kept* must not hold the handover: they
// are not going anywhere, and waiting on them would stall a rebalance for work
// that has no deadline.
func TestDrainPartitionsIgnoresRetainedPartitions(t *testing.T) {
	g := offsetgate.New()
	g.Track(revokeRec(1, 40)) // revoked, outstanding
	g.Track(revokeRec(2, 90)) // retained, outstanding forever
	revoked := map[string][]int32{revokeTopic: {1}}

	g.Settle(revokeTopic, 1, 40)

	start := time.Now()
	if left := drainPartitions(context.Background(), g, revoked, time.Second, time.Millisecond); left != 0 {
		t.Fatalf("left = %d, want 0: the retained partition was waited on", left)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("waited %v on a partition that was not revoked", elapsed)
	}
}

// The bound is what keeps a revoke from holding the rebalance hostage: a store
// that is down can leave parks open indefinitely, and franz-go asks that the
// group callbacks stay well inside the rebalance interval. Giving up has to
// report how much overlap was accepted rather than pretend it was clean.
func TestDrainPartitionsIsBounded(t *testing.T) {
	g := offsetgate.New()
	g.Track(revokeRec(1, 40)) // never settled
	revoked := map[string][]int32{revokeTopic: {1}}

	start := time.Now()
	left := drainPartitions(context.Background(), g, revoked, 60*time.Millisecond, 5*time.Millisecond)
	elapsed := time.Since(start)

	if left != 1 {
		t.Fatalf("left = %d, want 1: the outstanding delivery was reported as done", left)
	}
	if elapsed < 55*time.Millisecond {
		t.Fatalf("gave up after %v, before the bound", elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("gave up after %v, well past its 60ms bound", elapsed)
	}
}

// Shutdown cancels the context the callback was handed. The wait has to notice
// at once - waiting out the full bound while the process is stopping would put
// a ten second stall into every restart.
func TestDrainPartitionsStopsOnCancelledContext(t *testing.T) {
	g := offsetgate.New()
	g.Track(revokeRec(1, 40))
	revoked := map[string][]int32{revokeTopic: {1}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	left := drainPartitions(ctx, g, revoked, revokeDrain, revokeDrainTick)
	elapsed := time.Since(start)

	if left != 1 {
		t.Fatalf("left = %d, want 1: the outstanding delivery still counts", left)
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("waited %v with a cancelled context", elapsed)
	}
}
