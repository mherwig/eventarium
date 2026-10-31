package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Notification channels, next to the schema that writes to them (migrations
// 003 and 005).
const (
	// PresenceNotifyChannel is what migration 005's trigger queues on every
	// connect and disconnect.
	PresenceNotifyChannel = "sse_presence_changed"
	// PresenceListenAppName makes the listener recognisable in
	// pg_stat_activity beside the registry's own.
	PresenceListenAppName = "pubsub-sse-presence-listen"
)

// Listen keeps one connection in LISTEN on channel and calls onNotify for
// every notification until ctx is cancelled.
//
// The connection is opened directly rather than taken from the pool on
// purpose: a pooled connection would hand "presence changed" to whatever query
// acquired it next, and LISTEN state lives on the connection, not on the
// session it came from. The application_name is set so an operator can tell
// this listener from the registry's in pg_stat_activity.
//
// onNotify is also called once immediately after LISTEN, for the same reason
// the registry reloads on (re)connect: notifications are best effort - only a
// listener that is connected when the writing transaction commits receives
// them, and nothing replays them. Whatever was missed during the outage is
// read by that one call instead of surviving as staleness.
//
// A failed notification callback never costs the connection (the caller's
// reload keeps its previous state and the ticker retries), but a dead
// connection is re-established with a capped backoff that resets once a
// connection has proven stable, so a PostgreSQL restart cannot turn into a
// reconnect storm.
//
// internal/registry has an earlier copy of this loop bound to its own reload.
// It cannot use this one: store imports registry for the control-plane types,
// so the dependency only exists in that direction.
func Listen(ctx context.Context, dsn, channel, appName string, onNotify func(context.Context)) {
	const (
		backoff0    = 500 * time.Millisecond
		backoffMax  = 30 * time.Second
		stableAfter = time.Minute
	)
	backoff := backoff0
	for ctx.Err() == nil {
		started := time.Now()
		if err := listenOnce(ctx, dsn, channel, appName, onNotify); ctx.Err() != nil {
			return
		} else if err != nil {
			slog.Warn("store: listener disconnected", "channel", channel, "err", err)
		}
		if time.Since(started) > stableAfter {
			backoff = backoff0
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, backoffMax)
	}
}

func listenOnce(ctx context.Context, dsn, channel, appName string, onNotify func(context.Context)) error {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("store: parsing notify DSN: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["application_name"] = appName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("store: connecting for LISTEN: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+channel); err != nil {
		return fmt.Errorf("store: LISTEN %s: %w", channel, err)
	}
	// The catch-up call: everything queued while this listener was down is
	// covered by reading once after LISTEN, not by the notifications
	// themselves - PostgreSQL never replays them.
	onNotify(ctx)
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("store: waiting for a notification: %w", err)
		}
		slog.Debug("store: change notified", "channel", channel, "table", n.Payload)
		onNotify(ctx)
	}
}
