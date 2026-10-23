// Package chstore owns the ClickHouse schema for delivery status (section 9):
// append-only records partitioned by day and retention class so that whole
// partitions expire cheaply.
package chstore

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// Table is the append-only status table.
const Table = "delivery_status"

// Open connects to ClickHouse and creates the database when missing. The
// bootstrap connection goes to the "default" database, because a connection
// that selects a missing database cannot even ping.
func Open(ctx context.Context, dsn string) (clickhouse.Conn, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("chstore: parsing DSN: %w", err)
	}
	db := opts.Auth.Database
	if db == "" {
		db = "default"
	}
	if db != "default" {
		bootstrap := *opts
		bootstrap.Auth.Database = "default"
		bconn, err := clickhouse.Open(&bootstrap)
		if err != nil {
			return nil, fmt.Errorf("chstore: connecting: %w", err)
		}
		if err := bconn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
			bconn.Close()
			return nil, fmt.Errorf("chstore: creating database: %w", err)
		}
		bconn.Close()
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("chstore: connecting: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("chstore: pinging: %w", err)
	}
	return conn, nil
}

// Ensure creates the table and the TTL when missing.
func Ensure(ctx context.Context, conn clickhouse.Conn) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS ` + Table + ` (
		ts             DateTime64(3, 'UTC'),
		owner          LowCardinality(String),
		event_ref      String,
		delivery_id    String,
		subscription_id String,
		status         LowCardinality(String),
		reason         LowCardinality(String),
		attempt        UInt16,
		http_code      UInt16,
		latency_ms     UInt32,
		connection_id  String,
		replay_id      String,
		retention_class LowCardinality(String),
		src_partition  Int32,
		src_offset     UInt64
	) ENGINE = MergeTree
	PARTITION BY (toDate(ts), retention_class)
	ORDER BY (owner, delivery_id, ts)
	TTL toDate(ts) + INTERVAL 7 DAY DELETE
	SETTINGS index_granularity = 8192`
	if err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("chstore: creating table: %w", err)
	}
	return nil
}
