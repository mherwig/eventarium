// Command dispatcher is the webhook delivery stage of the spike. It consumes
// the deliveries topic in consumer group "dispatcher" and implements spec
// sections 6.4 (delivery, circuit breaker, rate limiting, security), 6.6
// (inbox parking and draining), 7 (delivery modes and policy), 8 (retries),
// 9 (status), 12 (live events are never parked) and 16 (backlog cap).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"pubsub/internal/config"
	"pubsub/internal/dispatch"
	"pubsub/internal/httpx"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/status"
	"pubsub/internal/store"
)

func main() {
	setupLog(config.Str("LOG_LEVEL", "info"))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fail := func(stage string, err error) {
		slog.Error("dispatcher: "+stage, "err", err)
		os.Exit(1)
	}

	dsn := config.Str("PG_DSN", "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable")
	st, err := store.Open(ctx, dsn)
	if err != nil {
		fail("connecting to postgres", err)
	}
	defer st.Close()

	// Liveness immediately, readiness only after the inbox partitions exist
	// and the consume loop has started: a dispatcher that has not joined the
	// group delivers nothing, however healthy PostgreSQL looks.
	health := httpx.NewHealth(func(ctx context.Context) error { return st.Pool.Ping(ctx) })
	go health.Serve(ctx, config.Str("HTTP_ADDR", ":8086"))

	producer, err := status.NewProducer(kafkautil.Bootstrap())
	if err != nil {
		fail("connecting the status producer", err)
	}
	defer producer.Close()

	// Spike deviation per docs/SPIKE.md: the registry polls PostgreSQL; the
	// dispatcher wants a fresh policy, so poll every second.
	reg := registry.New(st, time.Second)
	go reg.Run(ctx)

	// Daily inbox partitions exist before the first park (spec 6.6).
	if err := st.EnsureInboxPartitions(ctx, 8); err != nil {
		fail("creating inbox partitions", err)
	}

	d, err := dispatch.New(dispatch.Config{
		Registry:    reg,
		Store:       st,
		Status:      producer,
		MaxAttempts: config.Int("DISPATCHER_MAX_ATTEMPTS", 10),
		// Empty secret: no signature header (docs/SPIKE.md).
		SigningSecret: config.Str("WEBHOOK_SIGNING_SECRET", ""),
		// ALLOW_PRIVATE_WEBHOOKS defaults to true because the local test
		// harness (harness recv) listens on loopback; set it to 0 in real
		// deployments to enable the SSRF guard of spec 6.4.
		AllowPrivate: config.Bool("ALLOW_PRIVATE_WEBHOOKS", true),
	})
	if err != nil {
		fail("creating the dispatcher", err)
	}

	go d.Run(ctx)
	health.SetReady(true)
	<-ctx.Done()
	slog.Info("dispatcher: shutting down")
	// Graceful shutdown: stop consuming, wait (max 10s) for in-flight
	// attempts, flush the status producer.
	d.Shutdown()
}

// setupLog configures slog to stderr with the level from LOG_LEVEL.
func setupLog(level string) {
	var lvl slog.Level
	switch level {
	case "debug", "DEBUG":
		lvl = slog.LevelDebug
	case "warn", "WARN", "warning":
		lvl = slog.LevelWarn
	case "error", "ERROR":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
}
