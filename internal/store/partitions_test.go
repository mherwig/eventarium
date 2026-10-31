package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// testDSN is the local compose database; the test skips when nothing answers,
// the same convention as internal/registry and cmd/registry-api.
const testDSN = "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"

// TestIsAlreadyExists pins what is swallowed and what is not. PostgreSQL's
// "already exists" counts as success only because IF NOT EXISTS asked for
// exactly that outcome - so the match has to be the server's SQLSTATE, not the
// wording of a message (a message match would swallow unrelated errors that
// happen to mention a relation).
func TestIsAlreadyExists(t *testing.T) {
	if !isAlreadyExists(&pgconn.PgError{
		Code:    "42P07",
		Message: `relation "inbox_p20990601" already exists`,
	}) {
		t.Error("SQLSTATE 42P07 must count as already-exists")
	}
	// A wrapped PgError is still a PgError: pgx wraps its own errors.
	wrapped := fmt.Errorf("store: creating partition: %w",
		&pgconn.PgError{Code: "42P07", Message: `relation "x" already exists`})
	if !isAlreadyExists(wrapped) {
		t.Error("a wrapped 42P07 must count as already-exists")
	}
	for _, err := range []error{
		// The same wording without the SQLSTATE: not ours to interpret.
		errors.New(`relation "inbox_p20990601" already exists`),
		// Real failures that must still be fatal.
		&pgconn.PgError{Code: "42701", Message: "duplicate column name"},
		&pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"},
		&pgconn.PgError{Code: "3D000", Message: `relation "inbox" does not exist`},
	} {
		if isAlreadyExists(err) {
			t.Errorf("isAlreadyExists(%v) = true, want false", err)
		}
	}
}

// TestEnsureInboxPartitionsIsSafeConcurrently is the regression test for a
// dispatcher that exited on start-up.
//
// EnsureInboxPartitions runs at start-up, so it fires when a date first needs
// its partition - a first deploy, or after a day rollover - which is exactly
// when every replica of a component is starting at once. PostgreSQL's
// IF NOT EXISTS is not atomic for partitions: six concurrent creators of one
// fresh partition produced five CREATE TABLE and one ERROR 42P07, and the
// dispatcher treated that loser's error as fatal and exited, so a rolling
// update could shed a pod over a schema that was by then already correct.
//
// Sixteen goroutines over one fresh day must all succeed.
func TestEnsureInboxPartitionsIsSafeConcurrently(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		dsn = testDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := Open(ctx, dsn)
	if err != nil {
		t.Skipf("no PostgreSQL at %q (set TEST_PG_DSN): %v", dsn, err)
	}
	defer s.Close()

	// A day nothing has created yet, so the partition really does have to be
	// created and the race really does have to happen.
	day := time.Date(2099, 7, 1, 0, 0, 0, 0, time.UTC)
	const name = "inbox_p20990701"
	drop := func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dcancel()
		_, _ = s.Pool.Exec(dctx, "DROP TABLE IF EXISTS "+name)
	}
	drop()
	t.Cleanup(drop)

	const creators = 16
	errs := make([]error, creators)
	var wg sync.WaitGroup
	for i := 0; i < creators; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = s.ensurePartitions(ctx, day, 0)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("creator %d: %v (without the 42P07 fix this is the failure that killed a replica)", i, err)
		}
	}
	var exists bool
	if err := s.Pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil {
		t.Fatalf("checking the partition: %v", err)
	}
	if !exists {
		t.Error("the partition was never created")
	}
}
