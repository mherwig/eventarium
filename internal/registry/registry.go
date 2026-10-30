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
	"errors"
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

// DefaultStaleAfter is the age Check accepts when the caller names no
// maximum: far beyond the poll interval (2 s by default), so that one slow
// reload does not flap readiness, and still short enough that an index which
// stopped moving - the failure this exists for - is noticed within a probe or
// two.
const DefaultStaleAfter = 30 * time.Second

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
	// failures counts the reloads that did not succeed in a row and lastErr
	// holds the text of the most recent one (empty after a success). Check
	// turns them into a readiness answer and the reload log into a line that
	// says how long the index has been standing still, not just that a query
	// failed again.
	failures int
	lastErr  string
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
	// A database outage fails every reload for as long as it lasts, and the
	// poll plus the listener both retry - one line per attempt would bury the
	// log exactly while it is most interesting. Log the first failure and
	// every change of the error, then the recovery; the line carries the
	// consecutive count and the age of the index, which is the part that
	// matters ("how long have we been working from this snapshot").
	var loggedErr string
	for {
		err := r.Reload(ctx)
		failures, age := r.status()
		switch {
		case err != nil && err.Error() != loggedErr:
			slog.Warn("registry reload failed", "err", err,
				"consecutive", failures, "index_age", age.String())
			loggedErr = err.Error()
		case err == nil && loggedErr != "":
			slog.Info("registry reload recovered", "index_age", age.String())
			loggedErr = ""
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
		return r.failed(fmt.Errorf("registry: loading event types: %w", err))
	}
	subs, err := r.source.Subscriptions(ctx)
	if err != nil {
		return r.failed(fmt.Errorf("registry: loading subscriptions: %w", err))
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
	r.failures, r.lastErr = 0, ""
	r.mu.Unlock()
	for id, msg := range invalid {
		slog.Warn("subscription has invalid filters, skipped", "subscription", id, "err", msg)
	}
	return nil
}

// failed records a reload that did not reach the index and returns the error
// unchanged, so callers keep reporting it.
func (r *Registry) failed(err error) error {
	r.mu.Lock()
	r.failures++
	r.lastErr = err.Error()
	r.mu.Unlock()
	return err
}

// status reports how many reloads failed in a row and how old the live index
// is - the two facts the reload log and /readyz need to say more than "an
// error happened".
func (r *Registry) status() (failures int, age time.Duration) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.loaded.IsZero() {
		age = time.Since(r.loaded)
	}
	return r.failures, age
}

// Check reports why the in-memory index may not be trusted right now. It is
// the readiness hook (internal/httpx): a service that routes on this index
// answers 503 while it is older than maxAge instead of quietly working
// against a snapshot that stopped moving when the database went away. A
// non-positive maxAge means DefaultStaleAfter.
//
// Check is an approved addition to this frozen package, like ChannelFor.
func (r *Registry) Check(maxAge time.Duration) error {
	if maxAge <= 0 {
		maxAge = DefaultStaleAfter
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.loaded.IsZero() {
		return errors.New("registry index has never been loaded")
	}
	if age := time.Since(r.loaded); age > maxAge {
		if r.lastErr != "" {
			return fmt.Errorf("registry index is %s old (limit %s), last reload failed: %s",
				age.Round(time.Second), maxAge, r.lastErr)
		}
		return fmt.Errorf("registry index is %s old (limit %s)", age.Round(time.Second), maxAge)
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

// ChannelFor returns the channel that decides which delivery stage owns a
// record: the subscription's current channel when the ID is known, the
// stamped channel when it is not (deleted subscription, or an ID this
// instance has not loaded yet).
//
// Both stages ask the same question of the same data, and they must agree: a
// rule that differs between dispatcher and sse-gateway would let one stage
// skip a delivery the other also skips (lost) or both take it (duplicated).
// An update can therefore make the two read different snapshots for a moment
// - that window is one registry reload wide (milliseconds) and the platform
// is at-least-once, so a rare duplicate is the accepted outcome.
//
// ChannelFor is an approved addition to this frozen package.
func (r *Registry) ChannelFor(id, stamped string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if e, ok := r.byID[id]; ok && e != nil && e.Sub != nil && e.Sub.Channel != "" {
		return e.Sub.Channel
	}
	return stamped
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
