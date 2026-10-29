// Package registry holds the control data model of the platform: event types
// and subscriptions, plus the in-memory index the publish API and the router
// work against.
//
// Spike deviation from section 6.1: the index is refreshed from PostgreSQL
// instead of a compacted Kafka config topic. The interface stays the same, so
// only the source changes later. Two mechanisms keep it fresh: a poll (the
// safety net, see interval) and, when WithNotify is used, a PostgreSQL
// LISTEN/NOTIFY connection woken by the trigger of migration 003 (the fast
// path).
package registry

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"pubsub/internal/filter"
)

// NotifyChannel is the PostgreSQL notification channel the trigger of
// migration 003 writes to, and ListenAppName is the application_name the
// listener connection identifies itself with in pg_stat_activity.
const (
	NotifyChannel     = "registry_changed"
	ListenAppName     = "pubsub-registry-listen"
	listenBackoff0    = 500 * time.Millisecond
	listenBackoffMax  = 30 * time.Second
	listenStableAfter = time.Minute
)

// EventType is one registered CloudEvents type (section 6.1).
type EventType struct {
	Owner          string `json:"owner"`
	Name           string `json:"name"`
	RetentionClass string `json:"retention_class"`
	ContentType    string `json:"content_type"`
	OrderingKeyDef string `json:"ordering_key_def,omitempty"`
	OrderingGroup  string `json:"ordering_group,omitempty"`
}

// IsJSON reports whether the declared content type is JSON.
func (e EventType) IsJSON() bool {
	ct := e.ContentType
	return ct == "application/json" || len(ct) > 6 && ct[len(ct)-6:] == "+json"
}

// Subscription is one registered subscription (sections 3 and 7).
type Subscription struct {
	ID            string   `json:"id"`
	Owner         string   `json:"owner"`
	EventType     string   `json:"event_type"`
	Channel       string   `json:"channel"`
	CallbackURL   string   `json:"callback_url,omitempty"`
	Filter        string   `json:"filter,omitempty"`
	DataFilter    string   `json:"data_filter,omitempty"`
	Projection    []string `json:"projection,omitempty"`
	WebhookFormat string   `json:"webhook_format,omitempty"`

	DeliveryMode       string `json:"delivery_mode"`
	MaxConnections     int    `json:"max_connections"`
	MaxInFlight        int    `json:"max_in_flight"`
	RateLimitPerS      int    `json:"rate_limit_per_s"`
	Burst              int    `json:"burst"`
	DrainRatePerS      int    `json:"drain_rate_per_s"`
	AckTimeoutMS       int    `json:"ack_timeout_ms"`
	AckWindow          int    `json:"ack_window"`
	CBFailureThreshold int    `json:"cb_failure_threshold"`
	ProbeIntervalS     int    `json:"probe_interval_s"`
	RequestTimeoutMS   int    `json:"request_timeout_ms"`
	BacklogMax         int    `json:"backlog_max"`
}

// Entry is a subscription together with its compiled filters.
type Entry struct {
	Sub        *Subscription
	AttrFilter *filter.Compiled
	DataFilter *filter.Compiled
}

// Source loads the control data. store.Store implements it.
type Source interface {
	EventTypes(ctx context.Context) ([]EventType, error)
	Subscriptions(ctx context.Context) ([]Subscription, error)
}

// Registry is the in-memory index: owner+type to subscriptions, plus a lookup
// by owner and type. It is refreshed on a ticker, optionally woken by a
// notification, and safe for concurrent use.
type Registry struct {
	source    Source
	interval  time.Duration
	notifyDSN string

	reloadMu sync.Mutex // serialises the ticker and the listener's Reload

	mu      sync.RWMutex
	types   map[string]EventType // owner + "\x00" + name
	byType  map[string][]*Entry  // owner + "\x00" + event type
	byID    map[string]*Entry
	invalid map[string]string // subscription id -> compile error, for logging
	loaded  time.Time
}

// Option configures a Registry.
type Option func(*Registry)

// WithNotify wakes the registry on change: it keeps one dedicated connection
// (never a pooled one - a pooled connection would hand "config changed" to
// whatever query acquired it next) in LISTEN on NotifyChannel and reloads as
// soon as a control table is written. Where the poll bounds staleness by an
// interval, this bounds it by the notification round trip: milliseconds.
//
// The notification is best effort by nature. PostgreSQL only delivers it to
// listeners that are connected when the writing transaction commits and never
// replays it, so Run keeps polling and reloads once on every (re)connect to
// close any gap. A listener that is down for an hour therefore costs one
// reload on reconnect, not an hour of staleness.
//
// The DSN is used verbatim; an empty one disables the listener.
func WithNotify(dsn string) Option {
	return func(r *Registry) { r.notifyDSN = dsn }
}

// New builds a registry over a source, refreshed at the given interval.
func New(source Source, interval time.Duration, opts ...Option) *Registry {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	r := &Registry{
		source:   source,
		interval: interval,
		types:    map[string]EventType{},
		byType:   map[string][]*Entry{},
		byID:     map[string]*Entry{},
		invalid:  map[string]string{},
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Run reloads until the context is cancelled. With WithNotify a listener runs
// beside the ticker; it is the fast path, the ticker stays the safety net.
func (r *Registry) Run(ctx context.Context) {
	if r.notifyDSN != "" {
		go r.listen(ctx)
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.Reload(ctx); err != nil {
			slog.Warn("registry reload failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// listen reconnects until the context is cancelled, with a capped backoff that
// resets once a connection has proven stable: a PostgreSQL restart must not
// turn into a reconnect storm, and a listener that has been up for a while
// starts over from the short delay.
func (r *Registry) listen(ctx context.Context) {
	backoff := listenBackoff0
	for ctx.Err() == nil {
		started := time.Now()
		if err := r.listenOnce(ctx); ctx.Err() != nil {
			return
		} else if err != nil {
			slog.Warn("registry listener disconnected", "err", err)
		}
		if time.Since(started) > listenStableAfter {
			backoff = listenBackoff0
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, listenBackoffMax)
	}
}

// listenOnce connects, listens and reloads until the connection or the
// context fails. The reload directly after LISTEN is what makes the missed
// notifications of a downtime window irrelevant: whatever the listener did not
// hear is read here instead.
func (r *Registry) listenOnce(ctx context.Context) error {
	cfg, err := pgx.ParseConfig(r.notifyDSN)
	if err != nil {
		return fmt.Errorf("registry: parsing notify DSN: %w", err)
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	// So the connection is recognisable in pg_stat_activity.
	cfg.RuntimeParams["application_name"] = ListenAppName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("registry: connecting for LISTEN: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("registry: LISTEN %s: %w", NotifyChannel, err)
	}
	if err := r.Reload(ctx); err != nil && ctx.Err() == nil {
		slog.Warn("registry reload failed", "err", err)
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("registry: waiting for a notification: %w", err)
		}
		slog.Debug("registry change notified", "table", n.Payload)
		// A failed reload keeps the previous index and the ticker retries, so
		// it must not cost the listener its connection.
		if err := r.Reload(ctx); err != nil {
			slog.Warn("registry reload failed", "err", err)
		}
	}
}

// Reload pulls the control data and swaps the index.
func (r *Registry) Reload(ctx context.Context) error {
	// The ticker and the notification listener can reload at the same time.
	// Both build their maps before touching r.mu, so neither can corrupt the
	// index, but serialising them stops a slow reload from overwriting a
	// newer one that finished while it was still querying.
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	types, err := r.source.EventTypes(ctx)
	if err != nil {
		return fmt.Errorf("registry: loading event types: %w", err)
	}
	subs, err := r.source.Subscriptions(ctx)
	if err != nil {
		return fmt.Errorf("registry: loading subscriptions: %w", err)
	}
	newTypes := make(map[string]EventType, len(types))
	for i := range types {
		t := types[i]
		newTypes[key(t.Owner, t.Name)] = t
	}
	byType := map[string][]*Entry{}
	byID := map[string]*Entry{}
	invalid := map[string]string{}
	for i := range subs {
		sub := &subs[i]
		e := &Entry{Sub: sub}
		if sub.Filter != "" {
			c, err := filter.CompileAttribute(sub.Filter)
			if err != nil {
				invalid[sub.ID] = err.Error()
				continue
			}
			e.AttrFilter = c
		}
		if sub.DataFilter != "" {
			c, err := filter.CompileData(sub.DataFilter)
			if err != nil {
				invalid[sub.ID] = err.Error()
				continue
			}
			e.DataFilter = c
		}
		byType[key(sub.Owner, sub.EventType)] = append(byType[key(sub.Owner, sub.EventType)], e)
		byID[sub.ID] = e
	}
	r.mu.Lock()
	r.types, r.byType, r.byID, r.invalid, r.loaded = newTypes, byType, byID, invalid, time.Now()
	r.mu.Unlock()
	for id, msg := range invalid {
		slog.Warn("subscription has invalid filters, skipped", "subscription", id, "err", msg)
	}
	return nil
}

func key(owner, name string) string { return owner + "\x00" + name }

// EventType returns the registered event type.
func (r *Registry) EventType(owner, name string) (EventType, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.types[key(owner, name)]
	return t, ok
}

// Match returns the subscriptions of an owner and event type.
func (r *Registry) Match(owner, eventType string) []*Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byType[key(owner, eventType)]
}

// Subscription returns one subscription by ID.
func (r *Registry) Subscription(id string) (*Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byID[id]
	return e, ok
}

// Len returns the number of loaded subscriptions.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byID)
}

// LastLoaded reports when the index was last swapped.
func (r *Registry) LastLoaded() time.Time {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.loaded
}
