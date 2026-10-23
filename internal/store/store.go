// Package store is the PostgreSQL access layer: control data (event types,
// subscriptions) and the inbox of section 6.6. Only standard PostgreSQL
// features are used (N6).
package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"pubsub/internal/registry"
)

// Store wraps a pgx pool.
type Store struct {
	Pool *pgxpool.Pool
}

// Open connects and ping the database.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("store: connecting: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: pinging: %w", err)
	}
	return &Store{Pool: pool}, nil
}

// Close releases the pool.
func (s *Store) Close() { s.Pool.Close() }

// EventTypes loads every registered event type.
func (s *Store) EventTypes(ctx context.Context) ([]registry.EventType, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT owner, name, retention_class, content_type, COALESCE(ordering_key_def, ''), COALESCE(ordering_group, '')
		FROM event_types`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []registry.EventType
	for rows.Next() {
		var t registry.EventType
		if err := rows.Scan(&t.Owner, &t.Name, &t.RetentionClass, &t.ContentType, &t.OrderingKeyDef, &t.OrderingGroup); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// Subscriptions loads every registered subscription.
func (s *Store) Subscriptions(ctx context.Context) ([]registry.Subscription, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT id::text, owner, event_type, channel, COALESCE(callback_url, ''), COALESCE(filter, ''),
		       COALESCE(data_filter, ''), COALESCE(projection, '[]'::jsonb), delivery_mode,
		       max_connections, max_in_flight, rate_limit_per_s, burst, drain_rate_per_s,
		       ack_timeout_ms, ack_window, cb_failure_threshold, probe_interval_s,
		       request_timeout_ms, backlog_max, webhook_format
		FROM subscriptions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []registry.Subscription
	for rows.Next() {
		var sub registry.Subscription
		var projection []byte
		if err := rows.Scan(&sub.ID, &sub.Owner, &sub.EventType, &sub.Channel, &sub.CallbackURL, &sub.Filter,
			&sub.DataFilter, &projection, &sub.DeliveryMode, &sub.MaxConnections, &sub.MaxInFlight,
			&sub.RateLimitPerS, &sub.Burst, &sub.DrainRatePerS, &sub.AckTimeoutMS, &sub.AckWindow,
			&sub.CBFailureThreshold, &sub.ProbeIntervalS, &sub.RequestTimeoutMS, &sub.BacklogMax,
			&sub.WebhookFormat); err != nil {
			return nil, err
		}
		if len(projection) > 0 {
			_ = json.Unmarshal(projection, &sub.Projection)
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

// Subscription loads one subscription by ID.
func (s *Store) Subscription(ctx context.Context, id string) (*registry.Subscription, error) {
	subs, err := s.Subscriptions(ctx)
	if err != nil {
		return nil, err
	}
	for i := range subs {
		if subs[i].ID == id {
			return &subs[i], nil
		}
	}
	return nil, fmt.Errorf("store: subscription %s not found", id)
}

// EventType loads one event type.
func (s *Store) EventType(ctx context.Context, owner, name string) (*registry.EventType, error) {
	var t registry.EventType
	err := s.Pool.QueryRow(ctx, `
		SELECT owner, name, retention_class, content_type, COALESCE(ordering_key_def, ''), COALESCE(ordering_group, '')
		FROM event_types WHERE owner = $1 AND name = $2`, owner, name).
		Scan(&t.Owner, &t.Name, &t.RetentionClass, &t.ContentType, &t.OrderingKeyDef, &t.OrderingGroup)
	if err != nil {
		return nil, fmt.Errorf("store: event type %s/%s: %w", owner, name, err)
	}
	return &t, nil
}

// CreateEventType inserts or replaces an event type.
func (s *Store) CreateEventType(ctx context.Context, t registry.EventType) error {
	_, err := s.Pool.Exec(ctx, `
		INSERT INTO event_types (owner, name, retention_class, content_type, ordering_key_def, ordering_group)
		VALUES ($1, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''))
		ON CONFLICT (owner, name) DO UPDATE SET
			retention_class = EXCLUDED.retention_class,
			content_type    = EXCLUDED.content_type,
			ordering_key_def= EXCLUDED.ordering_key_def,
			ordering_group  = EXCLUDED.ordering_group`,
		t.Owner, t.Name, t.RetentionClass, t.ContentType, t.OrderingKeyDef, t.OrderingGroup)
	return err
}

// DeleteEventType removes an event type.
func (s *Store) DeleteEventType(ctx context.Context, owner, name string) error {
	_, err := s.Pool.Exec(ctx, `DELETE FROM event_types WHERE owner = $1 AND name = $2`, owner, name)
	return err
}

// CreateSubscription inserts a subscription with a generated ID.
func (s *Store) CreateSubscription(ctx context.Context, sub *registry.Subscription) error {
	projection, err := json.Marshal(sub.Projection)
	if err != nil {
		return err
	}
	if sub.ID == "" {
		sub.ID = uuid.NewString()
	}
	_, err = s.Pool.Exec(ctx, `
		INSERT INTO subscriptions (id, owner, event_type, channel, callback_url, filter, data_filter, projection,
			delivery_mode, max_connections, max_in_flight, rate_limit_per_s, burst, drain_rate_per_s,
			ack_timeout_ms, ack_window, cb_failure_threshold, probe_interval_s, request_timeout_ms,
			backlog_max, webhook_format)
		VALUES ($1::uuid, $2, $3, $4, NULLIF($5, ''), NULLIF($6, ''), NULLIF($7, ''), $8,
			$9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)`,
		sub.ID, sub.Owner, sub.EventType, sub.Channel, sub.CallbackURL, sub.Filter, sub.DataFilter, projection,
		sub.DeliveryMode, sub.MaxConnections, sub.MaxInFlight, sub.RateLimitPerS, sub.Burst, sub.DrainRatePerS,
		sub.AckTimeoutMS, sub.AckWindow, sub.CBFailureThreshold, sub.ProbeIntervalS, sub.RequestTimeoutMS,
		sub.BacklogMax, sub.WebhookFormat)
	return err
}

// DeleteSubscription removes a subscription.
func (s *Store) DeleteSubscription(ctx context.Context, owner, id string) error {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM subscriptions WHERE id = $1::uuid AND owner = $2`, id, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("store: subscription %s not found", id)
	}
	return nil
}

// InboxRow is one parked delivery.
type InboxRow struct {
	SubscriptionID string
	Part           int32
	Offset         int64
	Payload        []byte // encoded delivery.Message
	Reason         string
	ExpiresAt      time.Time
}

// Park stores parked deliveries. Rows that are already past their expiry are
// skipped; they can never be delivered again. Inserts are batched (section 6.6).
func (s *Store) Park(ctx context.Context, rows []InboxRow) error {
	if len(rows) == 0 {
		return nil
	}
	const chunk = 200
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		part := rows[start:end]
		args := make([]any, 0, len(part)*6)
		placeholders := make([]string, 0, len(part))
		for _, r := range part {
			if r.ExpiresAt.Before(time.Now()) {
				continue
			}
			base := len(args)
			placeholders = append(placeholders, fmt.Sprintf("($%d::uuid, $%d, $%d, $%d::jsonb, $%d, $%d)", base+1, base+2, base+3, base+4, base+5, base+6))
			args = append(args, r.SubscriptionID, r.Part, r.Offset, string(r.Payload), r.Reason, r.ExpiresAt)
		}
		if len(placeholders) == 0 {
			continue
		}
		query := `INSERT INTO inbox (subscription_id, part, off, payload, reason, expires_at) VALUES ` +
			join(placeholders, ", ") + ` ON CONFLICT DO NOTHING`
		if _, err := s.Pool.Exec(ctx, query, args...); err != nil {
			return fmt.Errorf("store: parking rows: %w", err)
		}
	}
	return nil
}

func join(parts []string, sep string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += sep
		}
		out += p
	}
	return out
}

// FetchParked returns up to limit parked rows for the given subscriptions, in
// (part, offset) order.
func (s *Store) FetchParked(ctx context.Context, subIDs []string, limit int) ([]InboxRow, error) {
	if len(subIDs) == 0 || limit <= 0 {
		return nil, nil
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT subscription_id::text, part, off, payload::text, reason, expires_at
		FROM inbox
		WHERE subscription_id = ANY($1::uuid[]) AND expires_at > now()
		ORDER BY subscription_id, part, off
		LIMIT $2`, subIDs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []InboxRow
	for rows.Next() {
		var r InboxRow
		var payload string
		if err := rows.Scan(&r.SubscriptionID, &r.Part, &r.Offset, &payload, &r.Reason, &r.ExpiresAt); err != nil {
			return nil, err
		}
		r.Payload = []byte(payload)
		out = append(out, r)
	}
	return out, rows.Err()
}

// AckParked deletes parked rows up to and including the given sequence for each
// subscription. The spike deletes in batches; the production design keeps a
// per-subscription watermark instead (section 6.6).
func (s *Store) AckParked(ctx context.Context, keys []InboxRow) error {
	if len(keys) == 0 {
		return nil
	}
	ids := make([]string, 0, len(keys))
	parts := make([]int32, 0, len(keys))
	offs := make([]int64, 0, len(keys))
	for _, k := range keys {
		ids = append(ids, k.SubscriptionID)
		parts = append(parts, k.Part)
		offs = append(offs, k.Offset)
	}
	_, err := s.Pool.Exec(ctx, `
		WITH k AS (
			SELECT unnest($1::uuid[]) AS subscription_id, unnest($2::int4[]) AS part, unnest($3::int8[]) AS off
		)
		DELETE FROM inbox i USING k
		WHERE i.subscription_id = k.subscription_id AND i.part = k.part AND i.off = k.off`,
		ids, parts, offs)
	return err
}

// CountParked returns the parked row count per subscription.
func (s *Store) CountParked(ctx context.Context, subIDs []string) (map[string]int64, error) {
	out := map[string]int64{}
	if len(subIDs) == 0 {
		return out, nil
	}
	rows, err := s.Pool.Query(ctx, `
		SELECT subscription_id::text, count(*) FROM inbox
		WHERE subscription_id = ANY($1::uuid[]) GROUP BY subscription_id`, subIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var n int64
		if err := rows.Scan(&id, &n); err != nil {
			return nil, err
		}
		out[id] = n
	}
	return out, rows.Err()
}

// DeleteExpiredParked removes parked rows whose expiry has passed. Whole
// partitions do this for free once they exist; this covers the default
// partition and rows parked before their partition existed.
func (s *Store) DeleteExpiredParked(ctx context.Context) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM inbox WHERE expires_at <= now()`)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// EnsureInboxPartitions creates the daily partitions for the next days so that
// an entire partition can be dropped at expiry (section 6.6). This is the
// fallback job for providers without pg_partman.
func (s *Store) EnsureInboxPartitions(ctx context.Context, days int) error {
	if days <= 0 {
		days = 8
	}
	day := time.Now().UTC().Truncate(24 * time.Hour)
	for i := 0; i < days+1; i++ {
		from := day.AddDate(0, 0, i)
		to := from.AddDate(0, 0, 1)
		name := fmt.Sprintf("inbox_p%s", from.Format("20060102"))
		_, err := s.Pool.Exec(ctx, fmt.Sprintf(
			`CREATE TABLE IF NOT EXISTS %s PARTITION OF inbox FOR VALUES FROM ('%s') TO ('%s')`,
			name, from.Format("2006-01-02"), to.Format("2006-01-02")))
		if err != nil {
			return fmt.Errorf("store: creating partition %s: %w", name, err)
		}
	}
	return nil
}
