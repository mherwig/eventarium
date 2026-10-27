package dispatch

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultTestDSN is the local compose database; the test runs against it when
// TEST_PG_DSN is unset and skips when nothing answers.
const defaultTestDSN = "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"

// TestDrainLeaseKeyIsStable pins the advisory lock key to the hash of its
// name. The value is what makes every replica agree on who leads: a new key is
// a new lock, and during a rolling deploy two builds would then both consider
// themselves the drainer and deliver the same backlog twice.
func TestDrainLeaseKeyIsStable(t *testing.T) {
	const name = "pubsub:dispatcher:drain"
	if got := int64(fnv64a(name)); got != drainLeaseKey {
		t.Fatalf("fnv64a(%q) = %d, but drainLeaseKey = %d - the key must never change",
			name, got, drainLeaseKey)
	}
}

// TestDrainLeaseElectsOne is the multi-replica contract of the drain loop:
// exactly one holder, and the standby takes over as soon as the holder lets
// go. It needs PostgreSQL, so it skips when the database is not reachable.
//
// The two leases use a private key - a test that raced the dispatcher actually
// running on this machine would be a coin toss, and the key's *value* is
// pinned by TestDrainLeaseKeyIsStable instead.
func TestDrainLeaseElectsOne(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Two pools, because a lease is held by a session and two replicas really
	// are two sessions.
	a := testPool(t, ctx, dsn)
	defer a.Close()
	b := testPool(t, ctx, dsn)
	defer b.Close()

	key := int64(fnv64a("lease-test:" + t.Name() + time.Now().String()))
	la := &drainLease{pool: a, key: key}
	defer la.close()
	lb := &drainLease{pool: b, key: key}
	defer lb.close()

	assertHold(t, ctx, la, true, "the first dispatcher takes the lease")
	assertHold(t, ctx, lb, false, "a second dispatcher must find it taken")
	assertHold(t, ctx, la, true, "the holder keeps it on the next tick (re-entrant)")

	la.close()
	assertHold(t, ctx, lb, true, "once the holder lets go, the standby takes over")
	assertHold(t, ctx, la, false, "the released lease is gone: only the standby holds it now")
}

func testPool(t *testing.T, ctx context.Context, dsn string) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("cannot build a PostgreSQL pool for %q: %v", dsn, err)
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		t.Skipf("no PostgreSQL at %q (set TEST_PG_DSN): %v", dsn, err)
	}
	return p
}

func assertHold(t *testing.T, ctx context.Context, l *drainLease, want bool, why string) {
	t.Helper()
	got, err := l.hold(ctx)
	if err != nil {
		t.Fatalf("%s: hold: %v", why, err)
	}
	if got != want {
		t.Fatalf("%s: hold = %v, want %v", why, got, want)
	}
}
