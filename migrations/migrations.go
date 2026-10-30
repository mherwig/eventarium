// Package migrations embeds the PostgreSQL schema migrations and applies them
// in order. Everything here uses standard PostgreSQL only (section 6.6, N6).
package migrations

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var files embed.FS

// Apply runs every embedded migration that has not been applied yet.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name       text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`)
	if err != nil {
		return fmt.Errorf("migrations: init: %w", err)
	}
	names, err := fs.Glob(files, "*.sql")
	if err != nil {
		return err
	}
	sort.Strings(names)
	for _, name := range names {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE name = $1)`, name).Scan(&exists); err != nil {
			return fmt.Errorf("migrations: checking %s: %w", name, err)
		}
		if exists {
			continue
		}
		blob, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, string(blob)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("migrations: applying %s: %w", name, err)
		}
		// ON CONFLICT: two services (or two test packages) can start at the
		// same moment, both see the migration as not applied and both apply
		// it. The statements are idempotent, so whoever loses the race just
		// records the same row the winner recorded.
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, name); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}
