package registry_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"pubsub/internal/registry"
	"pubsub/internal/store"
	"pubsub/migrations"
)

// defaultTestDSN is the local compose database; the test runs against it when
// TEST_PG_DSN is unset and skips when nothing answers (same convention as
// internal/dispatch).
const defaultTestDSN = "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"

// TestNotifyRefreshesOnCommit is the freshness contract of the fast path: a
// committed write to a control table must reach the in-memory index without
// waiting for the poll. The registry is built with a one hour interval, so if
// the index moves, the notification moved it.
//
// It also proves the two halves of the mechanism that polling cannot: that
// the write is seen only *after* the transaction committed (no early refresh
// with rows not visible yet), and that DELETE notifies as well as INSERT.
func TestNotifyRefreshesOnCommit(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Skipf("no PostgreSQL at %q (set TEST_PG_DSN): %v", dsn, err)
	}
	defer st.Close()
	// The trigger is migration 003; without it LISTEN would just never fire.
	if err := migrations.Apply(ctx, st.Pool); err != nil {
		t.Fatalf("applying migrations: %v", err)
	}

	const owner, name = "notify-test", "notify.test"
	forget := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = st.Pool.Exec(cleanupCtx, `DELETE FROM event_types WHERE owner = $1 AND name = $2`, owner, name)
	}
	forget() // a leftover from an aborted run must not satisfy the first wait
	defer forget()

	baseline := listeners(t, ctx, st)

	// One hour: if the index moves, the notification moved it, not the poll.
	const poll = time.Hour
	reg := registry.New(st, poll, registry.WithNotify(dsn))
	if err := reg.Reload(ctx); err != nil {
		t.Fatalf("initial reload: %v", err)
	}
	if _, ok := reg.EventType(owner, name); ok {
		t.Fatalf("event type %s/%s is already registered", owner, name)
	}
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go reg.Run(runCtx)
	waitListener(t, st, baseline)

	// Exec returns after the transaction committed; the notification carries
	// the same guarantee, so from here on only the listener can move the index.
	created := time.Now()
	if _, err := st.Pool.Exec(ctx,
		`INSERT INTO event_types (owner, name, retention_class, content_type) VALUES ($1, $2, 'short', 'application/json')`,
		owner, name); err != nil {
		t.Fatalf("inserting an event type: %v", err)
	}
	waitFor(t, fmt.Sprintf("the inserted event type %s/%s to show up", owner, name), func() bool {
		_, ok := reg.EventType(owner, name)
		return ok
	})
	t.Logf("the insert reached the index after %s (poll interval: %s)", time.Since(created), poll)

	if _, err := st.Pool.Exec(ctx,
		`DELETE FROM event_types WHERE owner = $1 AND name = $2`, owner, name); err != nil {
		t.Fatalf("deleting the event type: %v", err)
	}
	waitFor(t, fmt.Sprintf("the deleted event type %s/%s to disappear", owner, name), func() bool {
		_, ok := reg.EventType(owner, name)
		return !ok
	})
}

// listeners counts the connections registered under the listener's
// application_name, for waiting until LISTEN is actually established.
func listeners(t *testing.T, ctx context.Context, st *store.Store) int {
	t.Helper()
	var n int
	if err := st.Pool.QueryRow(ctx,
		`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`,
		registry.ListenAppName).Scan(&n); err != nil {
		t.Fatalf("counting listener connections: %v", err)
	}
	return n
}

// waitListener blocks until one more listener connection than before exists.
// A connection shows up in pg_stat_activity the moment it is established,
// marginally before the LISTEN itself; the short settle below keeps the test
// from inserting into that window and losing the notification.
func waitListener(t *testing.T, st *store.Store, baseline int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		n := listeners(t, ctx, st)
		cancel()
		if n > baseline {
			time.Sleep(250 * time.Millisecond)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("no %q connection appeared in pg_stat_activity within 10s", registry.ListenAppName)
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("registry did not see %s within 10s (the poll interval is one hour, so this can only be the notification path)", what)
}
