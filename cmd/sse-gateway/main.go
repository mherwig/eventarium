// Command sse-gateway is the SSE delivery and gateway service of the spike. It
// fills three roles in one binary (sections 6.5, 6.6 and 9 of the spec): it
// terminates SSE connections and answers acks (6.5), it publishes and consumes
// connection presence so the delivery stage knows where every client lives
// (6.5), and it parks and drains the PostgreSQL inbox for subscriptions
// without a connection (6.6). It applies the per-subscription delivery policy
// of section 7 (max_connections, drain_rate_per_s, backlog_max), recovers
// through Kafka offsets and event references after a restart (8) and emits the
// delivery status records that only an ack turns from sent into delivered (9).
//
// Environment (see SPIKE.md): HTTP_ADDR (default :8082), PG_DSN,
// KAFKA_BROKERS, JWT_SECRET, JWT_ISSUER, JWT_AUDIENCE, NODE_ID (default:
// hostname), plus SSE_HEARTBEAT (15s), SSE_MAX_CONNECTIONS (3) and
// SSE_DRAINER (true; set to false on all but one node of a multi-node spike).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"pubsub/internal/config"
	"pubsub/internal/sse"
)

func main() {
	level := slog.LevelInfo
	switch strings.ToLower(config.Str("LOG_LEVEL", "info")) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g, err := sse.New(sse.Options{
		HTTPAddr:       config.Str("HTTP_ADDR", ":8082"),
		NodeID:         config.Str("NODE_ID", hostname()),
		Brokers:        nil, // kafkautil.Bootstrap() reads KAFKA_BROKERS
		PGDSN:          config.Str("PG_DSN", "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"),
		JWTSecret:      config.Str("JWT_SECRET", "dev-secret"),
		JWTIssuer:      config.Str("JWT_ISSUER", "dev"),
		JWTAudience:    config.Str("JWT_AUDIENCE", "pubsub"),
		Heartbeat:      config.Duration("SSE_HEARTBEAT", 15*time.Second),
		MaxConnections: config.Int("SSE_MAX_CONNECTIONS", 3),
		RegistryPoll:   config.Duration("REGISTRY_POLL", time.Second),
		Drainer:        config.Bool("SSE_DRAINER", true),
	})
	if err != nil {
		slog.Error("starting", "err", err)
		os.Exit(1)
	}
	if err := g.Run(ctx); err != nil {
		slog.Error("running", "err", err)
		os.Exit(1)
	}
}

func hostname() string {
	if h, err := os.Hostname(); err == nil {
		return h
	}
	return "sse-gateway"
}
