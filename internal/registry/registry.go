// Package registry holds the control data model of the platform: event types
// and subscriptions, plus the in-memory index the publish API and the router
// work against.
//
// Spike deviation from section 6.1: the index is refreshed from PostgreSQL
// instead of a compacted Kafka config topic. The interface stays the same, so
// only the source changes later.
package registry

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"pubsub/internal/filter"
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
// by owner and type. It is refreshed on a ticker and safe for concurrent use.
type Registry struct {
	source   Source
	interval time.Duration

	mu      sync.RWMutex
	types   map[string]EventType // owner + "\x00" + name
	byType  map[string][]*Entry  // owner + "\x00" + event type
	byID    map[string]*Entry
	invalid map[string]string // subscription id -> compile error, for logging
	loaded  time.Time
}

// New builds a registry over a source, refreshed at the given interval.
func New(source Source, interval time.Duration) *Registry {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &Registry{
		source:   source,
		interval: interval,
		types:    map[string]EventType{},
		byType:   map[string][]*Entry{},
		byID:     map[string]*Entry{},
		invalid:  map[string]string{},
	}
}

// Run reloads until the context is cancelled.
func (r *Registry) Run(ctx context.Context) {
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

// Reload pulls the control data and swaps the index.
func (r *Registry) Reload(ctx context.Context) error {
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
