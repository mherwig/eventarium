// Package chquery implements the delivery status read path of sections 9 and
// 10 of the spec against the append-only delivery_status table of
// internal/chstore: the full history of one delivery, the current status of
// many deliveries via argMax over the "latest record" key described below,
// keyset pagination on (ts, delivery_id) and the per-status summary.
//
// The read path is eventually consistent: status records reach ClickHouse
// through the batch inserts of status-writer, so everything this package
// reports lags the real world by roughly one to five seconds (sections 9 and
// 10).
//
// Every query is owner scoped from the verified JWT (section 6.7) and fully
// parameterised; user input is never concatenated into SQL. The only
// interpolated identifier is the chstore.Table constant.
package chquery

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"pubsub/internal/chstore"
	"pubsub/internal/status"
)

// Pagination limits of section 10, endpoint 3.
const (
	DefaultLimit = 100
	MaxLimit     = 1000
)

// ErrNotFound is returned when the owner has no row for the requested key
// (section 10: the owner sees only its own data, so "not someone else's row"
// is indistinguishable from "no such row").
var ErrNotFound = errors.New("chquery: not found")

// Statuses lists every status of section 9 in lifecycle order, plus filtered.
// It seeds the zero values of the status summary.
//
// filtered goes last: it is not a step of a delivery's lifecycle (there is no
// delivery), it only ever appears as the single row of its own delivery ID.
// Appending also keeps the ranks of the ten real statuses stable, which the
// latest-record key of every query is built from.
var Statuses = []string{
	status.Pending,
	status.InFlight,
	status.Retrying,
	status.Delivered,
	status.Sent,
	status.Failed,
	status.DeadLettered,
	status.Paused,
	status.Expired,
	status.Dropped,
	status.Filtered,
}

// rankSQLFor ranks a status by its place in the lifecycle: the index in
// Statuses, and the length of that list for anything unknown.
//
// It is the last component of LatestKeySQL, and it exists because ts alone
// does not identify the newest record. Status records are produced in
// batches (the dispatcher flushes at most every 50 ms), so in_flight and
// delivered of one delivery routinely carry the same millisecond - observed
// in the live cluster for all 20 of 20 deliveries - and argMax then breaks
// the tie by row order, which reported every delivery as in_flight with
// http_code 0 instead of delivered with 200.
//
// prefix qualifies the status column ("ds." in a query that aliases the
// table); it must stay "" for the history query, which orders by this
// expression bare.
func rankSQLFor(prefix string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CASE %sstatus", prefix)
	for i, st := range Statuses {
		fmt.Fprintf(&b, " WHEN '%s' THEN %d", st, i)
	}
	fmt.Fprintf(&b, " ELSE %d END", len(Statuses)) // no ")": LatestKeySQL adds the parentheses, and the history query uses this bare in an ORDER BY
	return b.String()
}

var rankSQL = rankSQLFor("")

// LatestKeySQL is the sort key that decides which record of a delivery is
// the current one: (ts, attempt, lifecycle rank), with every column
// qualified by prefix ("" for an unaliased table, "ds." for one aliased ds).
//
//   - ts first: newest transition wins.
//   - attempt before the rank, so a later attempt always beats an earlier
//     one even when both land in the same millisecond (retrying of attempt 1
//     versus in_flight of attempt 2).
//   - rank last, to order records that share both, which happens exactly
//     when two transitions of one attempt are emitted back to back.
//
// The key is built in a subquery (SELECT *, <key> AS latest) because
// ClickHouse resolves a result alias before the column of the same name, and
// argMax(status, ...) AS status would otherwise nest an aggregate in itself.
// internal/replay calls this directly instead of writing its own argMax: the
// replay selector must judge "current status" with the definition the Status
// API reads, or the two disagree about one delivery whenever two transitions
// share a millisecond.
func LatestKeySQL(prefix string) string {
	return "(" + prefix + "ts, " + prefix + "attempt, " + rankSQLFor(prefix) + ")"
}

var latestKeySQL = LatestKeySQL("")

// Transition is one status record of one delivery, exactly as stored
// (section 9): IDs and measurements, never a payload.
type Transition struct {
	TS           time.Time `json:"ts"`
	Status       string    `json:"status"`
	Reason       string    `json:"reason,omitempty"`
	Attempt      uint16    `json:"attempt"`
	HTTPCode     uint16    `json:"http_code,omitempty"`
	LatencyMS    uint32    `json:"latency_ms,omitempty"`
	ConnectionID string    `json:"connection_id,omitempty"`
	ReplayID     string    `json:"replay_id,omitempty"`
	SrcPartition int32     `json:"src_partition"`
	SrcOffset    uint64    `json:"src_offset"`
}

// Item is the current status of one delivery, as returned by the list
// endpoints of section 10. TS is the timestamp of the latest record.
type Item struct {
	DeliveryID     string    `json:"delivery_id"`
	SubscriptionID string    `json:"subscription_id"`
	EventRef       string    `json:"event_ref,omitempty"`
	TS             time.Time `json:"ts"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	Attempt        uint16    `json:"attempt"`
	HTTPCode       uint16    `json:"http_code,omitempty"`
	LatencyMS      uint32    `json:"latency_ms,omitempty"`
	ConnectionID   string    `json:"connection_id,omitempty"`
}

// Delivery is one delivery with its full history (section 10, endpoint 1).
// Current is the latest record of History.
type Delivery struct {
	DeliveryID     string       `json:"delivery_id"`
	SubscriptionID string       `json:"subscription_id"`
	EventRef       string       `json:"event_ref"`
	Current        Transition   `json:"current"`
	History        []Transition `json:"history"`
}

// Filter narrows the deliveries of one subscription (section 10, endpoint 3).
// From and To bound the ts column; Status filters the current status of a
// delivery, not its history. Zero values mean "open".
type Filter struct {
	Status string
	From   time.Time
	To     time.Time
	Limit  int
	Cursor *Cursor
}

// Cursor is the keyset pagination position: the (ts, delivery_id) of the
// last row of the previous page.
type Cursor struct {
	TS         time.Time
	DeliveryID string
}

// Encode renders the cursor in its wire form: base64 of "RFC3339|delivery_id".
func (c Cursor) Encode() string {
	raw := c.TS.UTC().Format(time.RFC3339Nano) + "|" + c.DeliveryID
	return base64.StdEncoding.EncodeToString([]byte(raw))
}

// DecodeCursor parses the wire form produced by Encode.
func DecodeCursor(s string) (Cursor, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("chquery: cursor is not base64: %w", err)
	}
	tsRaw, id, ok := strings.Cut(string(raw), "|")
	if !ok || id == "" {
		return Cursor{}, errors.New("chquery: malformed cursor")
	}
	ts, err := time.Parse(time.RFC3339Nano, tsRaw)
	if err != nil {
		return Cursor{}, fmt.Errorf("chquery: malformed cursor timestamp: %w", err)
	}
	return Cursor{TS: ts, DeliveryID: id}, nil
}

// Store answers the queries of section 10 against one ClickHouse connection.
type Store struct {
	conn clickhouse.Conn
}

// New wraps a connection opened by chstore.Open.
func New(conn clickhouse.Conn) *Store { return &Store{conn: conn} }

// Delivery returns the full history of one delivery ordered by ts (newest
// last), with Current set to the latest record. It returns ErrNotFound when
// the owner has no such delivery (section 10, endpoint 1).
func (s *Store) Delivery(ctx context.Context, owner, deliveryID string) (*Delivery, error) {
	// The table is only ever read with ts >= now()-TTL, so the partition
	// pruning on (toDate(ts), retention_class) cannot help here; the
	// (owner, delivery_id, ts) primary key carries the query.
	rows, err := s.conn.Query(ctx, `
		SELECT ts, subscription_id, event_ref, status, reason, attempt,
		       http_code, latency_ms, connection_id, replay_id,
		       src_partition, src_offset
		FROM `+chstore.Table+`
		WHERE owner = ? AND delivery_id = ?
		ORDER BY ts, attempt, `+rankSQL,
		owner, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("chquery: delivery history: %w", err)
	}
	defer rows.Close()

	d := &Delivery{
		DeliveryID: deliveryID,
		History:    make([]Transition, 0, 16),
	}
	for rows.Next() {
		var (
			t   Transition
			sub string
			ref string
		)
		if err := rows.Scan(
			&t.TS, &sub, &ref, &t.Status, &t.Reason, &t.Attempt,
			&t.HTTPCode, &t.LatencyMS, &t.ConnectionID, &t.ReplayID,
			&t.SrcPartition, &t.SrcOffset,
		); err != nil {
			return nil, fmt.Errorf("chquery: scanning delivery history: %w", err)
		}
		// Subscription ID and event ref are constant per delivery; the
		// latest row wins.
		d.SubscriptionID = sub
		d.EventRef = ref
		d.History = append(d.History, t)
	}
	if len(d.History) == 0 {
		return nil, ErrNotFound
	}
	// Current is the latest record under the same key the list queries use
	// (ts, attempt, lifecycle rank): ordering the history by that key here
	// and taking the last row must agree with argMax over it, or endpoint 1
	// and endpoint 3 would disagree about one delivery.
	d.Current = d.History[len(d.History)-1]
	return d, nil
}

// EventDeliveries returns one row per delivery of one event, each summarised
// with its current status, ordered by delivery_id (section 10, endpoint 2).
// The endpoint is not paginated in the spec; one event fans out to a bounded
// number of subscriptions.
func (s *Store) EventDeliveries(ctx context.Context, owner, eventRef string) ([]Item, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT
			delivery_id,
			subscription_id,
			argMax(status, latest)          AS status,
			argMax(reason, latest)          AS reason,
			argMax(attempt, latest)         AS attempt,
			argMax(http_code, latest)       AS http_code,
			argMax(latency_ms, latest)      AS latency_ms,
			argMax(connection_id, latest)   AS connection_id,
			argMax(ts, latest)              AS last_ts
		FROM (
			SELECT *, `+latestKeySQL+` AS latest
			FROM `+chstore.Table+`
			WHERE owner = ? AND event_ref = ?
		)
		GROUP BY delivery_id, subscription_id
		ORDER BY delivery_id, subscription_id`,
		owner, eventRef)
	if err != nil {
		return nil, fmt.Errorf("chquery: event deliveries: %w", err)
	}
	defer rows.Close()
	return scanItems(rows, false)
}

// SubscriptionDeliveries returns the deliveries of one subscription with
// keyset pagination (section 10, endpoint 3). The cursor continues strictly
// after the (ts, delivery_id) of the last row of the previous page; ordering
// is ts, delivery_id. The second return value is the cursor of the next page,
// empty when the result is exhausted.
func (s *Store) SubscriptionDeliveries(ctx context.Context, owner, subscriptionID string, f Filter) ([]Item, string, error) {
	var (
		where  = []string{"owner = ?", "subscription_id = ?"}
		having []string
		args   = []any{owner, subscriptionID}
	)
	// From/To filter the ts column; they apply before grouping, so a page
	// shows the current status within the requested time window. Bound as
	// strings with an explicit cast, for the same millisecond-precision
	// reason as the cursor below.
	if !f.From.IsZero() {
		where = append(where, "ts >= toDateTime64(?, 3)")
		args = append(args, f.From.UTC().Format("2006-01-02 15:04:05.999"))
	}
	if !f.To.IsZero() {
		where = append(where, "ts <= toDateTime64(?, 3)")
		args = append(args, f.To.UTC().Format("2006-01-02 15:04:05.999"))
	}
	// The status filter judges a delivery by its latest record, so it must
	// run on the grouped value (HAVING), never on the raw rows (WHERE).
	if f.Status != "" {
		having = append(having, "argMax(r.status, r.latest) = ?")
		args = append(args, f.Status)
	}
	if f.Cursor != nil {
		// Tuples compare field-wise in ClickHouse. The timestamp is bound as a
		// formatted string and cast explicitly: binding a time.Time here made
		// the driver render toDateTime('...') at second precision, and with
		// 100+ rows in one second the keyset restarted inside that second,
		// repeated the page forever and stalled pagination (section 10).
		// max(r.ts) and argMax(r.ts, r.latest) are the same value: ts is the
		// first component of the key, so the page boundary stays comparable
		// with the last_ts it orders by.
		having = append(having, "(max(r.ts), delivery_id) > (toDateTime64(?, 3), ?)")
		args = append(args, f.Cursor.TS.UTC().Format("2006-01-02 15:04:05.999"), f.Cursor.DeliveryID)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = DefaultLimit
	}

	q := `
		SELECT
			delivery_id,
			subscription_id,
			event_ref,
			argMax(r.status, r.latest)          AS status,
			argMax(r.reason, r.latest)          AS reason,
			argMax(r.attempt, r.latest)         AS attempt,
			argMax(r.http_code, r.latest)       AS http_code,
			argMax(r.latency_ms, r.latest)      AS latency_ms,
			argMax(r.connection_id, r.latest)   AS connection_id,
			argMax(r.ts, r.latest)              AS last_ts
		FROM (
			SELECT *, ` + latestKeySQL + ` AS latest
			FROM ` + chstore.Table + `
			WHERE ` + strings.Join(where, " AND ") + `
		) AS r`
	// One row per delivery: everything in the SELECT is either aggregated or
	// part of the key (the subquery's columns, qualified through r).
	q += `
		GROUP BY delivery_id, subscription_id, event_ref`
	if len(having) > 0 {
		q += `
		HAVING ` + strings.Join(having, " AND ")
	}
	// One extra row tells us whether a next page exists.
	q += `
		ORDER BY last_ts, delivery_id
		LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.conn.Query(ctx, q, args...)
	if err != nil {
		return nil, "", fmt.Errorf("chquery: subscription deliveries: %w", err)
	}
	defer rows.Close()
	items, err := scanItems(rows, true)
	if err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		next = Cursor{TS: last.TS, DeliveryID: last.DeliveryID}.Encode()
	}
	return items, next, nil
}

// StatusCounts counts the deliveries of one subscription that were in each
// status since the given time (the API passes now-24h) and returns the
// counts per status (zero for statuses that did not occur) plus the number
// of distinct deliveries in the window (section 10, endpoint 4).
//
// It counts delivery_id, not status records, on purpose. A delivery has one
// record per transition (pending, in_flight, delivered), so counting rows
// would report 63 "deliveries" for the 20 published here, and at-least-once
// adds duplicates: the router was observed committing the same delivery
// twice in the live cluster, which made count() answer 21 for 20 events.
// uniqExact(delivery_id) answers the same number either way. The total is
// likewise distinct deliveries, not the sum of the per-status counts - one
// delivery passes through several statuses, so that sum is not a count of
// anything a reader can use.
//
// Backlog, delivery mode and active connections are deliberately absent:
// they are PostgreSQL control data (sections 6.6 and 7) and would come from
// there in a later version; spec section 10 marks this endpoint optional.
func (s *Store) StatusCounts(ctx context.Context, owner, subscriptionID string, since time.Time) (map[string]int64, int64, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT status, uniqExact(delivery_id)
		FROM `+chstore.Table+`
		WHERE owner = ? AND subscription_id = ? AND ts >= ?
		GROUP BY status`,
		owner, subscriptionID, since)
	if err != nil {
		return nil, 0, fmt.Errorf("chquery: status counts: %w", err)
	}
	defer rows.Close()

	counts := make(map[string]int64, len(Statuses))
	for _, st := range Statuses {
		counts[st] = 0
	}
	for rows.Next() {
		var (
			st string
			n  uint64 // uniqExact is UInt64, which the driver scans into uint64
		)
		if err := rows.Scan(&st, &n); err != nil {
			return nil, 0, fmt.Errorf("chquery: scanning status counts: %w", err)
		}
		counts[st] = int64(n)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("chquery: status counts: %w", err)
	}

	// One more pass for the total: the sum of the per-status distinct counts
	// would count a delivery once per status it passed through.
	var total uint64
	if err := s.conn.QueryRow(ctx, `
		SELECT uniqExact(delivery_id)
		FROM `+chstore.Table+`
		WHERE owner = ? AND subscription_id = ? AND ts >= ?`,
		owner, subscriptionID, since).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("chquery: status total: %w", err)
	}
	return counts, int64(total), nil
}

// scanItems reads the Item rows of the grouped list queries. With withEventRef
// false the event_ref column is not present in the result.
func scanItems(rows driver.Rows, withEventRef bool) ([]Item, error) {
	items := make([]Item, 0, 64)
	for rows.Next() {
		var (
			it  Item
			err error
		)
		if withEventRef {
			err = rows.Scan(&it.DeliveryID, &it.SubscriptionID, &it.EventRef,
				&it.Status, &it.Reason, &it.Attempt, &it.HTTPCode,
				&it.LatencyMS, &it.ConnectionID, &it.TS)
		} else {
			err = rows.Scan(&it.DeliveryID, &it.SubscriptionID,
				&it.Status, &it.Reason, &it.Attempt, &it.HTTPCode,
				&it.LatencyMS, &it.ConnectionID, &it.TS)
		}
		if err != nil {
			return nil, fmt.Errorf("chquery: scanning delivery item: %w", err)
		}
		items = append(items, it)
	}
	return items, nil
}
