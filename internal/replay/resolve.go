package replay

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"pubsub/internal/chquery"
	"pubsub/internal/chstore"
	"pubsub/internal/status"
)

// Match is one delivery selected for replay: the coordinates of its original
// event record (the pending status record written with the delivery stores the
// events-topic partition and offset, section 9) plus what the worker needs to
// order and report it.
type Match struct {
	DeliveryID     string
	EventRef       string
	RetentionClass string
	SrcPartition   int32
	SrcOffset      int64
	Ingested       time.Time // first status record of the delivery: "by ingestion time" (section 280)
	CurrentStatus  string    // newest record by chquery.LatestKeySQL: what the selector judges
}

// Resolve runs the selector of section 280 against delivery_status in one
// query: event refs are narrowed before grouping (event_ref is constant per
// delivery_id, so that is safe), the status and time filters judge the
// grouped values in HAVING, and the result is ordered by ingestion time and
// delivery ID so a replay keeps the original relative order (section 284),
// capped at limit (section 11, maximum events per job).
//
// Every column is qualified with ds.: the SELECT aliases (event_ref,
// ingested, current_status) would otherwise be picked up by the WHERE and
// HAVING clauses where a bare column name appears — ClickHouse then rejects
// the query with ILLEGAL_AGGREGATION ("aggregate function ... is found in
// WHERE"). Verified against the ClickHouse 24.8 of the spike, where
// HAVING-with-alias itself works as documented.
//
// The event coordinates come from the earliest status record of the delivery,
// never from an arbitrary one: only the pending record that the router wrote
// with the delivery points into the events topic (see the inline comment).
func Resolve(ctx context.Context, conn clickhouse.Conn, owner, subscriptionID string, sel Selector, limit int) ([]Match, error) {
	// `filtered` rows are excluded from every selector, unconditionally. They
	// are not deliveries: the router wrote them for an event the filter
	// rejected, so there is nothing to repeat - and a selector that names
	// only a time range or event refs (both valid under section 11) would
	// otherwise pick them up and replay exactly what the filter said no to,
	// because a replay re-applies the projection but never the filter.
	// Excluding the rows rather than the grouped status also keeps a
	// delivery ID that carries both kinds of record - possible when the
	// registry refreshed between two deliveries of one event - replayable
	// through its real rows: only the rejected verdict is dropped.
	where := []string{"ds.owner = ?", "ds.subscription_id = ?", "ds.status != ?"}
	args := []any{owner, subscriptionID, status.Filtered}

	if len(sel.EventRefs) > 0 {
		ph := make([]string, len(sel.EventRefs))
		for i, ref := range sel.EventRefs {
			ph[i] = "?"
			args = append(args, ref)
		}
		where = append(where, "ds.event_ref IN ("+strings.Join(ph, ", ")+")")
	}

	var having []string
	if len(sel.Statuses) > 0 {
		ph := make([]string, len(sel.Statuses))
		for i, st := range sel.Statuses {
			ph[i] = "?"
			args = append(args, st)
		}
		having = append(having, "current_status IN ("+strings.Join(ph, ", ")+")")
	}
	// The time range bounds the ingestion time of the delivery, i.e. the
	// timestamp of its first status record (section 280: "by ingestion
	// time"). Bound as millisecond-precision strings with an explicit cast,
	// the same reason as internal/chquery: binding a time.Time directly made
	// the driver render toDateTime at second precision.
	from, to, err := sel.timeRange()
	if err != nil {
		return nil, err
	}
	if !from.IsZero() {
		having = append(having, "ingested >= toDateTime64(?, 3)")
		args = append(args, from.UTC().Format("2006-01-02 15:04:05.999"))
	}
	if !to.IsZero() {
		having = append(having, "ingested <= toDateTime64(?, 3)")
		args = append(args, to.UTC().Format("2006-01-02 15:04:05.999"))
	}

	q := `
		SELECT
			delivery_id,
			any(ds.event_ref)          AS event_ref,
			any(ds.retention_class)    AS retention_class,
			-- The coordinates of the original event record, taken from the
			-- *earliest* status record of the delivery: that is the pending
			-- record the router wrote together with the delivery (section 9),
			-- and it carries the events-topic partition and offset. Every later
			-- record (in_flight, delivered, ...) carries the dispatcher's
			-- position on the deliveries topic instead (internal/dispatch),
			-- so any() here could hand the reader the wrong topic-partition.
			-- event_ref and retention_class are constant per delivery, any() is
			-- exact for them.
			argMin(ds.src_partition, ds.ts) AS src_partition,
			argMin(ds.src_offset, ds.ts)    AS src_offset,
			min(ds.ts)                      AS ingested,
			-- current_status is the newest record by the same key the Status
			-- API reads (internal/chquery): (ts, attempt, lifecycle rank), not
			-- ts alone. Two transitions of one delivery routinely share a
			-- millisecond (the status emitter flushes them together), and
			-- argMax over ts alone then breaks the tie by row order - so a
			-- selector for "delivered" could miss a delivery that the Status
			-- API reports as delivered.
			argMax(ds.status, ` + chquery.LatestKeySQL("ds.") + `) AS current_status
		FROM ` + chstore.Table + ` AS ds
		WHERE ` + strings.Join(where, " AND ") + `
		GROUP BY delivery_id`
	if len(having) > 0 {
		q += `
		HAVING ` + strings.Join(having, " AND ")
	}
	q += `
		ORDER BY ingested, delivery_id
		LIMIT ?`
	args = append(args, limit)

	rows, err := conn.Query(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("replay: resolving selector: %w", err)
	}
	defer rows.Close()

	matches := make([]Match, 0, 64)
	for rows.Next() {
		var (
			m      Match
			offset uint64 // src_offset is UInt64 in ClickHouse (chstore)
		)
		if err := rows.Scan(&m.DeliveryID, &m.EventRef, &m.RetentionClass,
			&m.SrcPartition, &offset, &m.Ingested, &m.CurrentStatus); err != nil {
			return nil, fmt.Errorf("replay: scanning selector result: %w", err)
		}
		m.SrcOffset = int64(offset)
		matches = append(matches, m)
	}
	return matches, rows.Err()
}

// Progress counts the current status of every delivery of one replay job, for
// the live delivered/failed fields of GET /replays/{id} (section 11): one
// latest record per delivery (chquery.LatestKeySQL, the same definition the
// Status API reads) among the rows carrying the replay ID, owner scoped as
// every read in this service (section 6.7). The current status of a replayed
// delivery is exactly what the dispatcher wrote with the replay ID (sections
// 9 and 11), so the earlier history stays invisible here.
func Progress(ctx context.Context, conn clickhouse.Conn, owner, replayID string) (delivered, failed int64, err error) {
	rows, err := conn.Query(ctx, `
		SELECT current_status, count()
		FROM (
			SELECT argMax(status, `+chquery.LatestKeySQL("")+`) AS current_status
			FROM `+chstore.Table+`
			WHERE owner = ? AND replay_id = ?
			GROUP BY delivery_id
		) AS replayed
		GROUP BY current_status`,
		owner, replayID)
	if err != nil {
		return 0, 0, fmt.Errorf("replay: progress counts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			st string
			n  uint64 // count() is UInt64, which the driver scans into uint64
		)
		if err := rows.Scan(&st, &n); err != nil {
			return 0, 0, fmt.Errorf("replay: scanning progress counts: %w", err)
		}
		switch st {
		case "delivered":
			delivered += int64(n)
		case "failed", "dead_lettered", "dropped", "expired":
			failed += int64(n)
		}
	}
	return delivered, failed, rows.Err()
}
