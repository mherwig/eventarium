package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// Check probes one readiness dependency and reports why it failed.
type Check func(ctx context.Context) error

// readyTimeout bounds one readiness pass: a dependency that hangs must not
// stall the endpoint, and Kubernetes gives a probe a second by default.
const readyTimeout = 2 * time.Second

// Health serves /healthz and /readyz for the services that have no API of
// their own (router, dispatcher, status-writer). The two endpoints have to
// differ, or Kubernetes cannot do its job:
//
//   - /healthz is liveness and keeps answering while PostgreSQL or ClickHouse
//     is down. Restarting a consumer because its database had a bad minute
//     loses nothing and only disturbs the consumer groups.
//   - /readyz is readiness and fails then, so a rolling update waits for a
//     replica that can actually work instead of declaring it good after two
//     seconds of process existence.
type Health struct {
	ready  atomic.Bool
	checks []Check
}

// NewHealth builds the endpoints. The checks are probed by /readyz only.
func NewHealth(checks ...Check) *Health { return &Health{checks: checks} }

// SetReady flips the second half of readiness: the initial setup - registry
// reload, inbox partitions, Kafka client - has finished. Until then /readyz
// answers 503 "starting" no matter how healthy the dependencies look, because
// a process that has not joined its consumer group yet is not serving.
func (h *Health) SetReady(v bool) { h.ready.Store(v) }

// Ready reports the current readiness flag.
func (h *Health) Ready() bool { return h.ready.Load() }

// Mux registers the two endpoints. They deliberately skip AccessLog: two
// probes every ten seconds would drown the real traffic in noise.
func (h *Health) Mux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, http.StatusOK, map[string]any{"status": "ok", "ready": h.ready.Load()})
	})
	mux.HandleFunc("GET /readyz", h.readyz)
	return mux
}

func (h *Health) readyz(w http.ResponseWriter, r *http.Request) {
	if !h.ready.Load() {
		JSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	for _, check := range h.checks {
		if err := check(ctx); err != nil {
			JSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "error": err.Error()})
			return
		}
	}
	JSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Serve runs the endpoints on addr until ctx is done and then shuts the
// listener down. A failure to bind is logged, not fatal: the service must not
// die because its health port was already taken.
func (h *Health) Serve(ctx context.Context, addr string) {
	srv := &http.Server{Addr: addr, Handler: h.Mux(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shctx)
	}()
	slog.Info("health endpoints listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("health endpoints", "addr", addr, "err", err)
	}
}
