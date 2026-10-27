package dispatch

import (
	"context"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// drainLeaseKey is the PostgreSQL advisory lock that marks the one dispatcher
// of the fleet allowed to touch the inbox (section 6.6). It is derived from a
// name instead of being picked at random, because every replica - and every
// future replica of the same platform - has to arrive at the same number
// without coordination. lease_test.go pins it: a rolling deploy during which
// two builds hold different keys would elect two drainers, which is precisely
// the bug this exists to prevent.
//
//	fnv64a("pubsub:dispatcher:drain") = 0xef01295c55d46ead
const drainLeaseKey int64 = -1224652147114348883

// leaseTimeout bounds one lock round trip. It must stay well below
// drainInterval, or a stalled PostgreSQL would queue up lease checks instead
// of simply skipping a tick.
const leaseTimeout = 5 * time.Second

// fnv64a is the hash the lease key is derived from.
func fnv64a(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

// drainLease is the inbox-duty lease: draining and the expiry sweep are the
// only work in the dispatcher that is *not* partition-scoped, so with several
// replicas they must elect exactly one process. Park, count and ack all target
// the same rows, and the drain loop reads them with a plain SELECT ... LIMIT -
// no row locks, no claiming - which means every replica would fetch the same
// batch and the backlog would be delivered once per replica, at the moment the
// endpoint has just recovered.
//
// An advisory lock is used rather than a flag or a lease table because it
// needs no schema (and no migration), it lives on a database session, and it
// therefore cleans up after itself: a dispatcher that dies - crash, node loss,
// network partition - stops holding it the moment PostgreSQL notices the
// session end, and the standby takes over on its next tick (500 ms) without
// anyone having to expire anything.
//
// The lock is per session, so it must be held on a pinned connection: a
// pgxpool connection that is released goes back to the pool and its session -
// with our lock on it - would be handed to unrelated queries, or kept alive
// without anybody checking it.
//
// PostgreSQL counts repeated acquisitions of one advisory lock per session
// (acquire twice, unlock once, still held), so the lock is taken exactly once
// and every later tick only *proves* it is still ours. Heartbeating with
// pg_try_advisory_lock instead would grow a count the single unlock on
// release could never drain, and the handover would silently not happen.
type drainLease struct {
	pool     *pgxpool.Pool
	key      int64
	conn     *pgxpool.Conn
	acquired bool
}

// newDrainLease starts a lease that competes for the lock. Each dispatcher
// owns one; the pool must be able to spare one idle connection for as long as
// this process runs (the default is plenty).
func newDrainLease(pool *pgxpool.Pool) *drainLease {
	return &drainLease{pool: pool, key: drainLeaseKey}
}

// ownsSQL reports whether the calling session holds the lease. pg_locks stores
// a bigint advisory key split into its two halves (classid = high 32 bits,
// objid = low 32 bits, objsubid = 1), which is why the key is split here too.
// The halves are compared as text so that no implicit oid/integer conversion
// gets between the column and the parameter.
const ownsSQL = `SELECT EXISTS (
	SELECT 1 FROM pg_locks
	WHERE locktype = 'advisory' AND objsubid = 1 AND pid = pg_backend_pid()
	  AND classid::text = $1 AND objid::text = $2)`

// hold reports whether this dispatcher may perform inbox duty right now. It is
// called before every tick, and nothing may be drained unless it returns true:
// an error means the lease could not be verified, which is exactly the
// situation (broken connection, failover, PostgreSQL restart) in which two
// nodes could otherwise both believe they lead.
func (l *drainLease) hold(ctx context.Context) (bool, error) {
	if l.conn == nil {
		conn, err := l.pool.Acquire(ctx)
		if err != nil {
			return false, err
		}
		l.conn, l.acquired = conn, false
	}
	if l.acquired {
		// Already ours - prove it rather than taking it again.
		var held bool
		if err := l.conn.QueryRow(ctx, ownsSQL,
			strconv.FormatUint(uint64(l.key)>>32, 10),
			strconv.FormatUint(uint64(l.key)&0xffffffff, 10),
		).Scan(&held); err != nil {
			l.releaseSession()
			return false, err
		}
		if held {
			return true, nil
		}
		l.acquired = false // lost without an error: fall through and re-take
	}
	var got bool
	if err := l.conn.QueryRow(ctx,
		"SELECT pg_try_advisory_lock($1::bigint)", l.key,
	).Scan(&got); err != nil {
		// The attempt failed, so the session may or may not hold the lock;
		// releaseSession guarantees it holds none once it leaves our hands.
		l.releaseSession()
		return false, err
	}
	l.acquired = got
	return got, nil
}

// releaseSession hands the connection back to the pool making sure it does not
// go back still holding the lock: a lock nobody monitors would park the inbox
// for good. The unlock is best effort - a session that already died released
// it server-side - and a session that is still alive after a *failed* unlock
// is taken out of the pool and killed rather than returned to it. Release
// itself already destroys a closed connection, so a dead session is never
// reused either.
func (l *drainLease) releaseSession() {
	l.acquired = false
	if l.conn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), leaseTimeout)
	_, err := l.conn.Exec(ctx, "SELECT pg_advisory_unlock($1::bigint)", l.key)
	cancel()
	if err != nil {
		if conn := l.conn.Hijack(); conn != nil {
			_ = conn.Close(context.Background())
		}
	}
	l.conn.Release()
	l.conn = nil
}

// close gives the lease up. Shutdown must not leave the inbox unattended
// until the session times out: a rolling update should hand it over
// immediately.
func (l *drainLease) close() {
	l.releaseSession()
}
