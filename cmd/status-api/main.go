// status-api serves the delivery status read interface of section 10 of the
// spec over the ClickHouse store of section 9 (see internal/chquery):
//
//	GET /v1/deliveries/{delivery_id}             full history + current status
//	GET /v1/events/{event_ref}/deliveries        all deliveries of one event
//	GET /v1/subscriptions/{id}/deliveries        filterable, keyset paginated
//	GET /v1/subscriptions/{id}/status-summary    per-status counts, last 24h
//	GET /healthz                                 no auth
//	GET /readyz                                  no auth (PostgreSQL + ClickHouse)
//
// Every endpoint except /healthz requires the status:read scope and every
// query is filtered by the owner in the verified JWT (section 6.7): a caller
// sees only rows that belong to its own owner reference.
//
// The read path is eventually consistent: status records reach ClickHouse
// through the batch inserts of status-writer, so results lag the real world
// by about one to five seconds (sections 9 and 10).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"pubsub/internal/auth"
	"pubsub/internal/chquery"
	"pubsub/internal/chstore"
	"pubsub/internal/config"
	"pubsub/internal/httpx"
	"pubsub/internal/store"
)

const (
	defaultCHDSN = "clickhouse://pubsub:pubsub@localhost:9000/pubsub"
	defaultPGDSN = "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"
)

func main() {
	setupLogging()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := chstore.Open(ctx, config.Str("CH_DSN", defaultCHDSN))
	if err != nil {
		slog.Error("status-api: clickhouse", "err", err)
		os.Exit(1)
	}
	defer conn.Close()

	// PostgreSQL is the existence and ownership oracle: a subscription ID is
	// asked of the subscriptions table, not of ClickHouse, which only ever
	// holds delivery records (section 9 keeps them metadata-free). The
	// lookup includes soft-deleted rows (migration 004) on purpose: the
	// deliveries of a subscription that has since been deleted happened, and
	// their history must stay readable under the owner that owns them.
	pg, err := store.Open(ctx, config.Str("PG_DSN", defaultPGDSN))
	if err != nil {
		slog.Error("status-api: postgres", "err", err)
		os.Exit(1)
	}
	defer pg.Close()

	validator, err := auth.NewHMAC(
		config.Str("JWT_SECRET", "dev-secret"),
		config.Str("JWT_ISSUER", "dev"),
		config.Str("JWT_AUDIENCE", "pubsub"),
	)
	if err != nil {
		slog.Error("status-api: auth", "err", err)
		os.Exit(1)
	}
	a := &api{validator: validator, store: chquery.New(conn), pg: pg}

	// Readiness needs both stores: every endpoint answers from ClickHouse
	// and the two subscription-scoped ones gate on PostgreSQL first.
	health := httpx.NewHealth(
		func(ctx context.Context) error { return conn.Ping(ctx) },
		func(ctx context.Context) error { return pg.Pool.Ping(ctx) },
	)

	mux := http.NewServeMux()
	health.Register(mux)
	// The delivery ID is event_ref + "/" + subscription_id (section 5), so
	// it spans two path segments and needs a prefix route.
	mux.HandleFunc("GET /v1/deliveries/", a.delivery)
	mux.HandleFunc("GET /v1/events/{event_ref}/deliveries", a.eventDeliveries)
	mux.HandleFunc("GET /v1/subscriptions/{id}/deliveries", a.subscriptionDeliveries)
	mux.HandleFunc("GET /v1/subscriptions/{id}/status-summary", a.statusSummary)

	addr := config.Str("HTTP_ADDR", ":8081")
	health.SetReady(true)
	srv := &http.Server{Addr: addr, Handler: httpx.AccessLog(mux)}
	go func() {
		slog.Info("status-api: listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("status-api: server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("status-api: shutdown", "err", err)
	}
}

type api struct {
	validator *auth.Validator
	store     *chquery.Store
	// pg answers "does this subscription exist for this owner?" - the one
	// question the status store cannot answer, because section 9 keeps status
	// records free of subscription metadata.
	pg *store.Store
}

// requireSubscription fails the request with 404 unless the owner really has
// this subscription. Without it, an arbitrary ID answers 200 with an empty
// list, which is the platform claiming to have checked something it did not
// (section 6.7: a foreign ID and a foreign delivery are the same not found).
// A database error is not "does not exist": it fails closed with a 500, so a
// broken lookup can never turn into a 404 that hides real history, nor into a
// 200 that invents an ownership that was never verified.
func (a *api) requireSubscription(w http.ResponseWriter, r *http.Request) (owner, id string, ok bool) {
	claims, authorized := a.validator.Authorize(w, r, auth.ScopeStatusRead)
	if !authorized {
		return "", "", false
	}
	id = r.PathValue("id")
	// A malformed ID is not a database problem and must not become a 500:
	// the $1::uuid cast would error. Like the control-plane API, it answers
	// 404 - unknown and malformed are the same "you have no such
	// subscription" (section 6.7), and a 400 would confirm nothing either.
	if _, err := uuid.Parse(id); err != nil {
		httpx.Error(w, http.StatusNotFound, "not found")
		return "", "", false
	}
	exists, err := a.pg.SubscriptionExists(r.Context(), claims.Owner, id)
	if err != nil {
		a.fail(w, "subscription lookup", err)
		return "", "", false
	}
	if !exists {
		httpx.Error(w, http.StatusNotFound, "not found")
		return "", "", false
	}
	return claims.Owner, id, true
}

// delivery returns the full history of one delivery ordered by ts, plus the
// latest record as "current" (section 10, endpoint 1). It 404s when the owner
// has no such delivery.
func (a *api) delivery(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.validator.Authorize(w, r, auth.ScopeStatusRead)
	if !ok {
		return
	}
	deliveryID := strings.TrimPrefix(r.URL.Path, "/v1/deliveries/")
	if deliveryID == "" {
		httpx.Error(w, http.StatusBadRequest, "delivery id is required")
		return
	}
	d, err := a.store.Delivery(r.Context(), claims.Owner, deliveryID)
	if err != nil {
		a.fail(w, "delivery history", err)
		return
	}
	httpx.JSON(w, http.StatusOK, d)
}

// eventDeliveries returns one row per delivery of one event, each summarised
// with its current status (the (ts, attempt, rank) latest record, see
// internal/chquery), ordered by delivery_id (section 10, endpoint 2).
func (a *api) eventDeliveries(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.validator.Authorize(w, r, auth.ScopeStatusRead)
	if !ok {
		return
	}
	items, err := a.store.EventDeliveries(r.Context(), claims.Owner, r.PathValue("event_ref"))
	if err != nil {
		a.fail(w, "event deliveries", err)
		return
	}
	httpx.JSON(w, http.StatusOK, listResponse{Items: items})
}

// subscriptionDeliveries returns the deliveries of one subscription, filterable
// by current status and time range, with keyset pagination on
// (ts, delivery_id) (section 10, endpoint 3).
func (a *api) subscriptionDeliveries(w http.ResponseWriter, r *http.Request) {
	owner, id, ok := a.requireSubscription(w, r)
	if !ok {
		return
	}
	f, err := parseFilter(r.URL.Query())
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	items, next, err := a.store.SubscriptionDeliveries(r.Context(), owner, id, f)
	if err != nil {
		a.fail(w, "subscription deliveries", err)
		return
	}
	httpx.JSON(w, http.StatusOK, listResponse{Items: items, NextCursor: next})
}

// statusSummary counts the status records per status for one subscription
// (section 10, endpoint 4), by default over the last 24 hours; an optional
// since parameter (RFC3339) narrows the window further, e.g. to one load-test
// run over the cumulative table. Backlog, delivery mode and active
// connections are PostgreSQL control data and would be added there in a later
// version; the spec marks this endpoint optional.
func (a *api) statusSummary(w http.ResponseWriter, r *http.Request) {
	owner, id, ok := a.requireSubscription(w, r)
	if !ok {
		return
	}
	since := time.Now().Add(-24 * time.Hour)
	if raw := r.URL.Query().Get("since"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, "since must be an RFC3339 timestamp")
			return
		}
		if t.After(since) {
			since = t
		}
	}
	counts, total, err := a.store.StatusCounts(r.Context(), owner, id, since)
	if err != nil {
		a.fail(w, "status summary", err)
		return
	}
	httpx.JSON(w, http.StatusOK, summaryResponse{Counts: counts, Total: total})
}

// listResponse is the envelope of the two list endpoints.
type listResponse struct {
	Items      []chquery.Item `json:"items"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

type summaryResponse struct {
	Counts map[string]int64 `json:"counts"`
	Total  int64            `json:"total"`
}

// fail maps a store error to 404 or 500. The owner scope is part of every
// query, so a missing row and a foreign row are the same 404 (section 6.7).
func (a *api) fail(w http.ResponseWriter, op string, err error) {
	if errors.Is(err, chquery.ErrNotFound) {
		httpx.Error(w, http.StatusNotFound, "not found")
		return
	}
	slog.Error("status-api: "+op, "err", err)
	httpx.Error(w, http.StatusInternalServerError, "internal error")
}

// parseFilter reads the query parameters of section 10, endpoint 3 and rejects
// malformed input with a message meant for the 400 body.
func parseFilter(q url.Values) (chquery.Filter, error) {
	f := chquery.Filter{Status: q.Get("status")}

	if q.Get("event_type") != "" {
		// Status records hold IDs and measurements only, never event
		// metadata (section 9), so delivery_status carries no event type
		// to filter on. Rejecting the parameter keeps the contract honest
		// instead of silently returning unfiltered rows; a later version
		// would resolve event_ref -> type via the registry or store the
		// type next to the ref.
		return f, errors.New("event_type is not supported: delivery status records carry no event type (section 9)")
	}

	var err error
	if raw := q.Get("from"); raw != "" {
		if f.From, err = time.Parse(time.RFC3339, raw); err != nil {
			return f, errors.New("from must be an RFC3339 timestamp")
		}
	}
	if raw := q.Get("to"); raw != "" {
		if f.To, err = time.Parse(time.RFC3339, raw); err != nil {
			return f, errors.New("to must be an RFC3339 timestamp")
		}
	}
	if !f.From.IsZero() && !f.To.IsZero() && f.From.After(f.To) {
		return f, errors.New("from is after to")
	}

	f.Limit = chquery.DefaultLimit
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > chquery.MaxLimit {
			return f, fmt.Errorf("limit must be between 1 and %d", chquery.MaxLimit)
		}
		f.Limit = n
	}

	if raw := q.Get("cursor"); raw != "" {
		cursor, err := chquery.DecodeCursor(raw)
		if err != nil {
			return f, errors.New("cursor is invalid")
		}
		f.Cursor = &cursor
	}
	return f, nil
}

// setupLogging sends slog to stderr at the configured level (docs/SPIKE.md).
func setupLogging() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToUpper(config.Str("LOG_LEVEL", "info")))); err != nil {
		level = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}
