package httpx

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync"
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
	ready    atomic.Bool
	checksMu sync.RWMutex
	checks   []Check
	// metrics is read per request rather than captured when the mux is built,
	// so SetMetrics can be called at any point before the first scrape and no
	// service has to get the ordering right.
	metricsMu sync.RWMutex
	metrics   http.Handler
}

// NewHealth builds the endpoints. The checks are probed by /readyz only.
func NewHealth(checks ...Check) *Health { return &Health{checks: checks} }

// SetMetrics attaches the handler that answers /metrics. Every service gets
// the endpoint by calling this once; leaving it unset answers 404, which is
// how METRICS_ENABLED=false looks from outside.
//
// It is on Health rather than registered by each service because the two ways
// a service serves HTTP both come through here: the five APIs call Register on
// their own mux, and the three that serve only health go through Mux. One
// place means a new service cannot forget it.
func (h *Health) SetMetrics(handler http.Handler) {
	h.metricsMu.Lock()
	h.metrics = handler
	h.metricsMu.Unlock()
}

// AddCheck registers another readiness dependency. It exists for the services
// that can answer liveness before every dependency object exists - the router
// starts serving before its registry has even been built - and is safe to
// call while probes are running; the checks are what /readyz needs, not a
// fixed picture of the moment NewHealth was called.
func (h *Health) AddCheck(c Check) {
	h.checksMu.Lock()
	defer h.checksMu.Unlock()
	h.checks = append(h.checks, c)
}

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
	h.Register(mux)
	return mux
}

// Register adds /healthz and /readyz to an existing mux, for the services
// that serve their API from the same port and therefore already have one
// (the five HTTP APIs).
func (h *Health) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, http.StatusOK, map[string]any{"status": "ok", "ready": h.ready.Load()})
	})
	mux.HandleFunc("GET /readyz", h.readyz)
	h.registerMetrics(mux)
}

// RegisterReady adds only /readyz, for a service that already answers
// /healthz with a body of its own (sse-gateway reports its connection counts
// there) and must not lose it. It adds /metrics too, for the same reason
// Register does: this is the other door into the same mux.
func (h *Health) RegisterReady(mux *http.ServeMux) {
	mux.HandleFunc("GET /readyz", h.readyz)
	h.registerMetrics(mux)
}

// registerMetrics adds /metrics, dispatching to whatever SetMetrics attached.
func (h *Health) registerMetrics(mux *http.ServeMux) {
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		h.metricsMu.RLock()
		handler := h.metrics
		h.metricsMu.RUnlock()
		if handler == nil {
			http.NotFound(w, r)
			return
		}
		handler.ServeHTTP(w, r)
	})
}

func (h *Health) readyz(w http.ResponseWriter, r *http.Request) {
	if !h.ready.Load() {
		JSON(w, http.StatusServiceUnavailable, map[string]string{"status": "starting"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
	defer cancel()
	h.checksMu.RLock()
	checks := h.checks
	h.checksMu.RUnlock()
	for _, check := range checks {
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
