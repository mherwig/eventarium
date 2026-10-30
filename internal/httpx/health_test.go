package httpx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func get(t *testing.T, h *Health, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.Mux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

// TestHealthzNeverFails is the liveness contract: the endpoint answers 200
// before the service finished starting and while its dependencies are down,
// because a dependency outage must not get a consumer restarted.
func TestHealthzNeverFails(t *testing.T) {
	h := NewHealth(func(context.Context) error { return errors.New("postgres down") })

	rec := get(t, h, "/healthz")
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"ready":false`) {
		t.Errorf("/healthz body = %s, want ready false before setup finished", rec.Body.String())
	}

	h.SetReady(true)
	if rec := get(t, h, "/healthz"); rec.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rec.Code)
	}
}

// TestReadyzFollowsSetupAndChecks is the readiness contract: starting, then
// the dependencies, and a failing check names itself in the body.
func TestReadyzFollowsSetupAndChecks(t *testing.T) {
	healthy := func(context.Context) error { return nil }
	unhealthy := func(context.Context) error { return errors.New("clickhouse unreachable") }

	h := NewHealth(healthy)
	if rec := get(t, h, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz before SetReady = %d, want 503", rec.Code)
	} else if !strings.Contains(rec.Body.String(), "starting") {
		t.Errorf("/readyz body = %s, want starting", rec.Body.String())
	}

	h.SetReady(true)
	if rec := get(t, h, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz with a healthy check = %d, want 200", rec.Code)
	}
	if !h.Ready() {
		t.Error("Ready() must follow SetReady(true)")
	}

	broken := NewHealth(unhealthy)
	broken.SetReady(true)
	rec := get(t, broken, "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz with a failing check = %d, want 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "clickhouse unreachable") {
		t.Errorf("/readyz body = %s, want the failing check named", rec.Body.String())
	}
}

// TestReadyzWithoutChecks covers a service that has no dependency worth
// probing: setup finished is readiness.
func TestReadyzWithoutChecks(t *testing.T) {
	h := NewHealth()
	if rec := get(t, h, "/readyz"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 while starting", rec.Code)
	}
	h.SetReady(true)
	if rec := get(t, h, "/readyz"); rec.Code != http.StatusOK {
		t.Fatalf("/readyz = %d, want 200", rec.Code)
	}
}

// TestRegisterOnExistingMux is the wiring of the five APIs: they serve the
// probes from their own mux next to the API routes, and a service that keeps
// its own /healthz body (sse-gateway) registers readiness only. Both must
// answer without AccessLog, exactly like the endpoints on a Health.Mux.
func TestRegisterOnExistingMux(t *testing.T) {
	healthy := NewHealth()
	healthy.SetReady(true)
	mux := http.NewServeMux()
	healthy.Register(mux)
	mux.HandleFunc("GET /v1/ping", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz on the API mux = %d, want 200", rec.Code)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/ping", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("the API routes on the same mux must keep working, got %d", rec.Code)
	}

	ownHealthz := http.NewServeMux()
	broken := NewHealth(func(context.Context) error { return errors.New("clickhouse unreachable") })
	broken.SetReady(true)
	ownHealthz.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		JSON(w, http.StatusOK, map[string]string{"status": "ok", "connections": "7"})
	})
	broken.RegisterReady(ownHealthz)

	rec = httptest.NewRecorder()
	ownHealthz.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "connections") {
		t.Fatalf("the gateway's own /healthz body must survive, got %d: %s", rec.Code, rec.Body)
	}
	rec = httptest.NewRecorder()
	ownHealthz.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503 while a dependency fails", rec.Code)
	}
}
