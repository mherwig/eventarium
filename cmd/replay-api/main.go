// Command replay-api serves the Replay API of section 11 of the spec over the
// status store of section 9 and the events topics of section 12:
//
//	POST /v1/subscriptions/{id}/replays   create a job, 202 + replay ID
//	GET  /v1/replays/{replay_id}          progress: matched, enqueued,
//	                                      not_replayable plus live delivered/failed
//	POST /v1/replays/{replay_id}/cancel   stop a pending or running job
//	GET  /v1/subscriptions/{id}/replays   list the jobs of a subscription
//	GET  /healthz                         no auth
//
// The process also runs the single job worker (internal/replay.Worker): it
// resolves the selector against ClickHouse, reads the original events back
// from the events topic of their retention class by partition and offset, and
// produces replay deliveries plus their pending statuses transactionally,
// batch by batch, in ingestion order (sections 11, 9 and 284).
//
// Environment (see docs/SPIKE.md): HTTP_ADDR (default :8084), PG_DSN,
// CH_DSN, KAFKA_BROKERS, JWT_SECRET, JWT_ISSUER, JWT_AUDIENCE,
// REPLAY_MAX_EVENTS (default 10000), REGISTRY_POLL (1s) and LOG_LEVEL.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pubsub/internal/auth"
	"pubsub/internal/chstore"
	"pubsub/internal/config"
	"pubsub/internal/httpx"
	"pubsub/internal/registry"
	"pubsub/internal/replay"
	"pubsub/internal/store"
	"pubsub/migrations"
)

const (
	defaultPGDSN = "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"
	defaultCHDSN = "clickhouse://pubsub:pubsub@localhost:9000/pubsub"
)

func main() {
	setupLogging()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Control data: the replay_jobs table (migration 002) plus the
	// subscription registry. migrations.Apply is idempotent, the same call
	// registry-api makes, so whichever service starts first provisions it.
	dsn := config.Str("PG_DSN", defaultPGDSN)
	st, err := store.Open(ctx, dsn)
	if err != nil {
		slog.Error("replay-api: opening store", "err", err)
		os.Exit(1)
	}
	defer st.Close()
	if err := migrations.Apply(ctx, st.Pool); err != nil {
		slog.Error("replay-api: applying migrations", "err", err)
		os.Exit(1)
	}

	conn, err := chstore.Open(ctx, config.Str("CH_DSN", defaultCHDSN))
	if err != nil {
		slog.Error("replay-api: clickhouse", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	validator, err := auth.NewHMAC(
		config.Str("JWT_SECRET", "dev-secret"),
		config.Str("JWT_ISSUER", "dev"),
		config.Str("JWT_AUDIENCE", "pubsub"),
	)
	if err != nil {
		slog.Error("replay-api: auth", "err", err)
		os.Exit(1)
	}

	// The subscription registry is the existence and ownership oracle for the
	// API and the source of the *current* projection for the worker (spike
	// deviation of section 6.1, refreshed from PostgreSQL).
	reg := registry.New(st, config.Duration("REGISTRY_POLL", time.Second), registry.WithNotify(dsn))
	for {
		if err := reg.Reload(ctx); err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		slog.Warn("replay-api: initial registry load failed, retrying", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	go reg.Run(ctx)

	maxEvents := config.Int("REPLAY_MAX_EVENTS", 10000)
	if maxEvents < 1 {
		slog.Error("replay-api: REPLAY_MAX_EVENTS must be at least 1")
		os.Exit(1)
	}

	producer, err := replay.NewProducer()
	if err != nil {
		slog.Error("replay-api: producer", "err", err)
		os.Exit(1)
	}
	defer producer.Close()

	jobs := replay.NewJobStore(st.Pool)
	worker := replay.NewWorker(jobs, conn, reg, replay.NewEventReader(), producer, maxEvents)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		worker.Run(ctx)
	}()

	server := replay.NewServer(validator, jobs, conn, reg, maxEvents)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	server.Register(mux)

	addr := config.Str("HTTP_ADDR", ":8084")
	srv := &http.Server{Addr: addr, Handler: httpx.AccessLog(mux)}
	go func() {
		slog.Info("replay-api: listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("replay-api: server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("replay-api: shutdown", "err", err)
	}
	// Let the worker notice the cancellation and leave its job 'running' on
	// purpose (the next start re-executes it) before the producer closes.
	<-workerDone
	slog.Info("replay-api: stopped")
}

// setupLogging sends slog to stderr at the configured level (docs/SPIKE.md).
func setupLogging() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(config.Str("LOG_LEVEL", "info")))); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}
