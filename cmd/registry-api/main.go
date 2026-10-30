// Registry API manages the control data of the platform: the event type
// registry of section 6.1 and the subscriptions of sections 3 and 7. It
// implements the validation duties the spec assigns to the registry: CEL
// filters must compile, projection paths must be well formed, non-JSON event
// types must not carry data filters or projections (section 5), and the
// delivery policy defaults of the section 7 table are applied when a field is
// absent. Every row is owner scoped from the JWT, so a caller only ever sees
// and changes its own data; anything else answers 404 and never leaks the
// existence of a foreign resource (section 6.7).
//
// HTTP_ADDR defaults to :8080 like every API service. When publish-api and
// registry-api run on the same machine, give one of them another port, e.g.
// HTTP_ADDR=:8083 for this service.
//
// The spike reads and writes PostgreSQL directly; there is no registry
// poller here. In production the control data would be distributed through a
// compacted Kafka config topic (section 6.1).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"pubsub/internal/auth"
	"pubsub/internal/config"
	"pubsub/internal/filter"
	"pubsub/internal/httpx"
	"pubsub/internal/kafkautil"
	"pubsub/internal/project"
	"pubsub/internal/registry"
	"pubsub/internal/store"
	"pubsub/migrations"
)

// maxBodyBytes bounds the control plane request bodies; they are small.
const maxBodyBytes = 1 << 20

func main() {
	setupLogging()
	addr := config.Str("HTTP_ADDR", ":8080")
	dsn := config.Str("PG_DSN", "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, dsn)
	if err != nil {
		fatal("opening store", err)
	}
	defer st.Close()
	// Idempotent, so it is safe to run on every start of any service.
	if err := migrations.Apply(ctx, st.Pool); err != nil {
		fatal("applying migrations", err)
	}

	validator, err := auth.NewHMAC(
		config.Str("JWT_SECRET", "dev-secret"),
		config.Str("JWT_ISSUER", "dev"),
		config.Str("JWT_AUDIENCE", "pubsub"),
	)
	if err != nil {
		fatal("building token validator", err)
	}

	api := &api{auth: validator, store: st}
	mux := newMux(api)

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpx.AccessLog(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		stop() // a second signal falls back to the default: hard exit
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown", "err", err)
		}
	}()
	slog.Info("registry-api listening", "addr", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal("serving http", err)
	}
	slog.Info("registry-api stopped")
}

// newMux wires the endpoints. It is separate from main so that the handlers
// can be exercised through httptest against a real store.
func newMux(a *api) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/event-types", a.handleEventTypeCreate)
	mux.HandleFunc("GET /v1/event-types", a.handleEventTypeList)
	mux.HandleFunc("DELETE /v1/event-types/{name}", a.handleEventTypeDelete)
	mux.HandleFunc("POST /v1/subscriptions", a.handleSubscriptionCreate)
	mux.HandleFunc("GET /v1/subscriptions", a.handleSubscriptionList)
	mux.HandleFunc("GET /v1/subscriptions/{id}", a.handleSubscriptionGet)
	mux.HandleFunc("PUT /v1/subscriptions/{id}", a.handleSubscriptionUpdate)
	mux.HandleFunc("DELETE /v1/subscriptions/{id}", a.handleSubscriptionDelete)
	// Catch-all so that unknown paths answer with the platform's JSON error
	// shape instead of the mux's plain text default.
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		httpx.Error(w, http.StatusNotFound, "not found")
	})
	return mux
}

// api holds the dependencies of the control plane endpoints.
type api struct {
	auth  *auth.Validator
	store *store.Store
}

// authorize validates the bearer token (section 6.7). An empty scope accepts
// any valid token: read endpoints require the token but no specific scope.
func (a *api) authorize(w http.ResponseWriter, r *http.Request, scope string) (*auth.Claims, bool) {
	if scope != "" {
		return a.auth.Authorize(w, r, scope)
	}
	h := r.Header.Get("Authorization")
	if h == "" || !strings.HasPrefix(h, "Bearer ") {
		httpx.Error(w, http.StatusUnauthorized, "missing bearer token")
		return nil, false
	}
	claims, err := a.auth.Parse(strings.TrimPrefix(h, "Bearer "))
	if err != nil {
		httpx.Error(w, http.StatusUnauthorized, err.Error())
		return nil, false
	}
	return claims, true
}

// --- Event types (section 6.1) ---

type eventTypeRequest struct {
	Name           string `json:"name"`
	RetentionClass string `json:"retention_class"`
	ContentType    string `json:"content_type"`
	OrderingKeyDef string `json:"ordering_key_def"`
	OrderingGroup  string `json:"ordering_group"`
}

func (a *api) handleEventTypeCreate(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, auth.ScopePublish)
	if !ok {
		return
	}
	var req eventTypeRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		httpx.Error(w, http.StatusUnprocessableEntity, "name is required")
		return
	}
	class := strings.TrimSpace(req.RetentionClass)
	if class == "" {
		class = kafkautil.ClassShort // new event types default to short (section 6.1)
	}
	if !kafkautil.ValidClass(class) {
		httpx.Error(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("invalid retention_class %q: must be one of live, short, day, week", class))
		return
	}
	contentType := strings.TrimSpace(req.ContentType)
	if contentType == "" {
		contentType = "application/json"
	}
	orderingKeyDef := strings.TrimSpace(req.OrderingKeyDef)
	if err := validateOrderingKeyDef(orderingKeyDef); err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, err.Error())
		return
	}

	et := registry.EventType{
		Owner:          claims.Owner, // owner from the token only (section 6.7)
		Name:           name,
		RetentionClass: class,
		ContentType:    contentType,
		OrderingKeyDef: orderingKeyDef,
		OrderingGroup:  strings.TrimSpace(req.OrderingGroup),
	}
	if err := a.store.CreateEventType(r.Context(), et); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "storing event type failed")
		slog.Error("creating event type", "owner", et.Owner, "name", et.Name, "err", err)
		return
	}
	httpx.JSON(w, http.StatusCreated, et)
}

func (a *api) handleEventTypeList(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, "")
	if !ok {
		return
	}
	types, err := a.store.EventTypes(r.Context())
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "loading event types failed")
		slog.Error("listing event types", "err", err)
		return
	}
	out := make([]registry.EventType, 0, len(types))
	for _, t := range types {
		if t.Owner == claims.Owner {
			out = append(out, t)
		}
	}
	httpx.JSON(w, http.StatusOK, out)
}

func (a *api) handleEventTypeDelete(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, auth.ScopePublish)
	if !ok {
		return
	}
	name := r.PathValue("name")
	// Owner scoped lookup first: a foreign or unknown name answers 404 and
	// never reveals whether another owner holds that event type (section 6.7).
	if _, err := a.store.EventType(r.Context(), claims.Owner, name); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		}
		httpx.Error(w, http.StatusInternalServerError, "loading event type failed")
		slog.Error("loading event type", "name", name, "err", err)
		return
	}
	if err := a.store.DeleteEventType(r.Context(), claims.Owner, name); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "deleting event type failed")
		slog.Error("deleting event type", "name", name, "err", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// validateOrderingKeyDef checks the shape of an ordering key definition
// (section 5): "", "subject", "ext:<name>" or "json:<path>".
func validateOrderingKeyDef(def string) error {
	switch {
	case def == "" || def == "subject":
		return nil
	case strings.HasPrefix(def, "ext:"):
		name := strings.TrimPrefix(def, "ext:")
		if name == "" {
			return errors.New(`ordering_key_def "ext:" needs an extension name`)
		}
		if strings.ContainsAny(name, " \t\r\n\"") {
			return fmt.Errorf("ordering_key_def extension name %q must not contain whitespace or quotes", name)
		}
		return nil
	case strings.HasPrefix(def, "json:"):
		if err := project.Validate([]string{strings.TrimPrefix(def, "json:")}); err != nil {
			return fmt.Errorf("ordering_key_def: %w", err)
		}
		return nil
	default:
		return errors.New(`ordering_key_def must be "", "subject", "ext:<name>" or "json:<path>"`)
	}
}

// --- Subscriptions (section 7) ---

type subscriptionRequest struct {
	EventType   string   `json:"event_type"`
	Channel     string   `json:"channel"`
	CallbackURL string   `json:"callback_url"`
	Filter      string   `json:"filter"`
	DataFilter  string   `json:"data_filter"`
	Projection  []string `json:"projection"`

	DeliveryMode  string `json:"delivery_mode"`
	WebhookFormat string `json:"webhook_format"`
	// Pointer fields distinguish "absent" from an explicit zero; absent
	// fields get the defaults of the delivery policy table (section 7).
	MaxConnections     *int `json:"max_connections"`
	MaxInFlight        *int `json:"max_in_flight"`
	RateLimitPerS      *int `json:"rate_limit_per_s"`
	Burst              *int `json:"burst"`
	DrainRatePerS      *int `json:"drain_rate_per_s"`
	AckTimeoutMS       *int `json:"ack_timeout_ms"`
	AckWindow          *int `json:"ack_window"`
	CBFailureThreshold *int `json:"cb_failure_threshold"`
	ProbeIntervalS     *int `json:"probe_interval_s"`
	RequestTimeoutMS   *int `json:"request_timeout_ms"`
	BacklogMax         *int `json:"backlog_max"`
}

func (a *api) handleSubscriptionCreate(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, auth.ScopeSubscribe)
	if !ok {
		return
	}
	var req subscriptionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	sub, ok := a.buildSubscription(r.Context(), w, claims.Owner, req)
	if !ok {
		return
	}
	if err := a.store.CreateSubscription(r.Context(), sub); err != nil {
		httpx.Error(w, http.StatusInternalServerError, "storing subscription failed")
		slog.Error("creating subscription", "owner", sub.Owner, "event_type", sub.EventType, "err", err)
		return
	}
	httpx.JSON(w, http.StatusCreated, sub)
}

// handleSubscriptionUpdate replaces a subscription: PUT, full replace
// (section 7). The body is the POST body, so a read-modify-write loop through
// GET is lossless - every field the client omits returns to its default, and
// "" can only ever mean "clear this filter".
//
// The lookup happens before the body is validated: a foreign or unknown ID
// answers 404 even when the body would have failed validation, because a
// validation error would otherwise confirm that an ID exists (section 6.7).
// Only id and owner are kept from the stored row, so event_type, the channel
// with its callback, both filters, the projection and the whole delivery
// policy are all mutable. Migration 003's NOTIFY trigger fires on this UPDATE,
// so new events, drain and replay see the new configuration within
// milliseconds (see also the channel gates of dispatcher and sse-gateway).
func (a *api) handleSubscriptionUpdate(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, auth.ScopeSubscribe)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		httpx.Error(w, http.StatusNotFound, "not found")
		return
	}
	existing, err := a.store.Subscription(r.Context(), id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		}
		httpx.Error(w, http.StatusInternalServerError, "loading subscription failed")
		slog.Error("loading subscription", "id", id, "err", err)
		return
	}
	if existing.Owner != claims.Owner {
		// Section 6.7: a foreign resource looks exactly like a missing one.
		httpx.Error(w, http.StatusNotFound, "not found")
		return
	}
	var req subscriptionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	// The owner in sub is the caller's, which the lookup just proved to be
	// the stored one.
	sub, ok := a.buildSubscription(r.Context(), w, claims.Owner, req)
	if !ok {
		return
	}
	sub.ID = existing.ID
	if err := a.store.UpdateSubscription(r.Context(), sub); err != nil {
		if strings.Contains(err.Error(), "not found") {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		}
		httpx.Error(w, http.StatusInternalServerError, "storing subscription failed")
		slog.Error("updating subscription", "id", sub.ID, "owner", sub.Owner, "event_type", sub.EventType, "err", err)
		return
	}
	httpx.JSON(w, http.StatusOK, sub)
}

// buildSubscription validates a subscription body into a subscription ready to
// store. It writes the error itself and reports whether the result is usable,
// so callers return as soon as the second value is false. Create and update
// share it, which is what keeps PUT validated exactly like POST: one rule for
// channel and callback_url, delivery_mode and webhook_format, the owner scoped
// event type lookup, the JSON-only rules for data_filter and projection, and
// the defaults and bounds of the delivery policy table (section 7).
func (a *api) buildSubscription(ctx context.Context, w http.ResponseWriter, owner string, req subscriptionRequest) (*registry.Subscription, bool) {
	channel := strings.TrimSpace(req.Channel)
	if channel != "webhook" && channel != "sse" {
		httpx.Error(w, http.StatusUnprocessableEntity, `channel must be "webhook" or "sse"`)
		return nil, false
	}
	callbackURL := strings.TrimSpace(req.CallbackURL)
	switch channel {
	case "webhook":
		if callbackURL == "" {
			httpx.Error(w, http.StatusUnprocessableEntity, "callback_url is required for the webhook channel")
			return nil, false
		}
		if err := validateHTTPURL(callbackURL); err != nil {
			httpx.Error(w, http.StatusUnprocessableEntity, err.Error())
			return nil, false
		}
	case "sse":
		if callbackURL != "" {
			httpx.Error(w, http.StatusUnprocessableEntity, "callback_url must be empty for the sse channel")
			return nil, false
		}
	}

	mode := strings.TrimSpace(req.DeliveryMode)
	if mode == "" {
		mode = "ordered"
	}
	if mode != "ordered" && mode != "key_ordered" && mode != "unordered" {
		httpx.Error(w, http.StatusUnprocessableEntity,
			`delivery_mode must be "ordered", "key_ordered" or "unordered"`)
		return nil, false
	}
	format := strings.TrimSpace(req.WebhookFormat)
	if format == "" {
		format = "binary"
	}
	if format != "binary" && format != "structured" {
		httpx.Error(w, http.StatusUnprocessableEntity, `webhook_format must be "binary" or "structured"`)
		return nil, false
	}

	eventType := strings.TrimSpace(req.EventType)
	if eventType == "" {
		httpx.Error(w, http.StatusUnprocessableEntity, "event_type is required")
		return nil, false
	}
	// Owner scoped lookup: an event type of another owner is unknown here.
	et, err := a.store.EventType(ctx, owner, eventType)
	if errors.Is(err, pgx.ErrNoRows) {
		httpx.Error(w, http.StatusUnprocessableEntity, fmt.Sprintf("unknown event type %q", eventType))
		return nil, false
	}
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "loading event type failed")
		slog.Error("loading event type", "event_type", eventType, "err", err)
		return nil, false
	}

	dataFilter := strings.TrimSpace(req.DataFilter)
	attrFilter := strings.TrimSpace(req.Filter)
	if !et.IsJSON() && (dataFilter != "" || len(req.Projection) > 0) {
		// Section 5: data filters and projection exist for JSON data only.
		httpx.Error(w, http.StatusUnprocessableEntity,
			fmt.Sprintf("event type %q is not JSON: data_filter and projection are only allowed for JSON event types", eventType))
		return nil, false
	}
	if dataFilter != "" {
		if _, err := filter.CompileData(dataFilter); err != nil {
			httpx.Error(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid data_filter: %v", err))
			return nil, false
		}
	}
	if attrFilter != "" {
		if _, err := filter.CompileAttribute(attrFilter); err != nil {
			httpx.Error(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid filter: %v", err))
			return nil, false
		}
	}
	if err := project.Validate(req.Projection); err != nil {
		httpx.Error(w, http.StatusUnprocessableEntity, fmt.Sprintf("invalid projection: %v", err))
		return nil, false
	}

	sub := &registry.Subscription{
		Owner:         owner, // owner from the token only (section 6.7)
		EventType:     eventType,
		Channel:       channel,
		CallbackURL:   callbackURL,
		Filter:        attrFilter,
		DataFilter:    dataFilter,
		Projection:    req.Projection,
		DeliveryMode:  mode,
		WebhookFormat: format,
	}
	// Defaults and sanity bounds of the delivery policy table (section 7).
	// Zero stays legal for rate_limit_per_s (unlimited), burst, drain rate
	// and backlog_max; the fields the dispatcher and gateway cannot work
	// without must be positive.
	policies := []struct {
		name string
		dst  *int
		src  *int
		def  int
		min  int
	}{
		{"max_connections", &sub.MaxConnections, req.MaxConnections, 3, 1},
		{"max_in_flight", &sub.MaxInFlight, req.MaxInFlight, 1, 1},
		{"rate_limit_per_s", &sub.RateLimitPerS, req.RateLimitPerS, 0, 0},
		{"burst", &sub.Burst, req.Burst, 0, 0},
		{"drain_rate_per_s", &sub.DrainRatePerS, req.DrainRatePerS, 200, 0},
		{"ack_timeout_ms", &sub.AckTimeoutMS, req.AckTimeoutMS, 15000, 1},
		{"ack_window", &sub.AckWindow, req.AckWindow, 100, 1},
		{"cb_failure_threshold", &sub.CBFailureThreshold, req.CBFailureThreshold, 5, 1},
		{"probe_interval_s", &sub.ProbeIntervalS, req.ProbeIntervalS, 300, 1},
		{"request_timeout_ms", &sub.RequestTimeoutMS, req.RequestTimeoutMS, 10000, 1},
		{"backlog_max", &sub.BacklogMax, req.BacklogMax, 100000, 0},
	}
	for _, p := range policies {
		v, err := policyInt(p.name, p.src, p.def, p.min)
		if err != nil {
			httpx.Error(w, http.StatusUnprocessableEntity, err.Error())
			return nil, false
		}
		*p.dst = v
	}
	if len(sub.Projection) == 0 {
		sub.Projection = []string{}
	}
	return sub, true
}

func (a *api) handleSubscriptionList(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, "")
	if !ok {
		return
	}
	subs, err := a.store.Subscriptions(r.Context())
	if err != nil {
		httpx.Error(w, http.StatusInternalServerError, "loading subscriptions failed")
		slog.Error("listing subscriptions", "err", err)
		return
	}
	out := make([]registry.Subscription, 0, len(subs))
	for i := range subs {
		if subs[i].Owner == claims.Owner {
			out = append(out, subs[i])
		}
	}
	httpx.JSON(w, http.StatusOK, out)
}

func (a *api) handleSubscriptionGet(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, "")
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		httpx.Error(w, http.StatusNotFound, "not found")
		return
	}
	sub, err := a.store.Subscription(r.Context(), id)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		}
		httpx.Error(w, http.StatusInternalServerError, "loading subscription failed")
		slog.Error("loading subscription", "id", id, "err", err)
		return
	}
	if sub.Owner != claims.Owner {
		// Section 6.7: a foreign resource looks exactly like a missing one.
		httpx.Error(w, http.StatusNotFound, "not found")
		return
	}
	httpx.JSON(w, http.StatusOK, sub)
}

func (a *api) handleSubscriptionDelete(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.authorize(w, r, auth.ScopeSubscribe)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		httpx.Error(w, http.StatusNotFound, "not found")
		return
	}
	// Delete is owner scoped in the store, so a foreign id matches no row.
	if err := a.store.DeleteSubscription(r.Context(), claims.Owner, id); err != nil {
		if strings.Contains(err.Error(), "not found") {
			httpx.Error(w, http.StatusNotFound, "not found")
			return
		}
		httpx.Error(w, http.StatusInternalServerError, "deleting subscription failed")
		slog.Error("deleting subscription", "id", id, "err", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// policyInt returns the section 7 default when a delivery policy field is
// absent, and rejects values below the given lower bound.
func policyInt(name string, v *int, def, min int) (int, error) {
	if v == nil {
		return def, nil
	}
	if *v < min {
		return 0, fmt.Errorf("%s must be at least %d", name, min)
	}
	return *v, nil
}

func validateHTTPURL(raw string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("callback_url must be an absolute http or https URL")
	}
	return nil
}

// decodeJSON reads a bounded JSON request body and writes the error itself.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		httpx.Error(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return false
	}
	return true
}

func setupLogging() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(config.Str("LOG_LEVEL", "info")))); err != nil {
		slog.Warn("invalid LOG_LEVEL, staying at info", "err", err)
		return
	}
	slog.SetLogLoggerLevel(level)
}

func fatal(stage string, err error) {
	slog.Error(stage, "err", err)
	os.Exit(1)
}
