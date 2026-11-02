// Package chstore owns the ClickHouse schema for delivery status (section 9):
// append-only records partitioned by day and retention class so that whole
// partitions expire cheaply.
package chstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"

	"pubsub/internal/config"
)

// Table is the append-only status table.
const Table = "delivery_status"

// cluster reports which ClickHouse cluster DDL should be aimed at, or "" for a
// single node.
//
// This one setting decides the whole schema shape. With CH_CLUSTER set - the
// three-node topology - the database and table are created `ON CLUSTER` and
// the table uses ReplicatedMergeTree, so every node holds every row and the
// Client Service may write to any of them. Without it, nothing changes for
// the single-node stack: plain MergeTree, no coordination service, no
// <remote_servers> entry - neither of which a one-node stack has.
//
// An unusable value is an error rather than a silent fall back to a
// non-replicated table: quietly creating an unreplicated table on a
// three-node cluster is the same failure mode as a replication factor nobody
// checked, and it would only be discovered when a node is lost.
func cluster() (string, error) {
	c := strings.TrimSpace(config.Str("CH_CLUSTER", ""))
	if c == "" {
		return "", nil
	}
	for _, r := range c {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_':
		default:
			return "", fmt.Errorf("chstore: CH_CLUSTER %q is not a cluster name (letters, digits and underscore only)", c)
		}
	}
	return c, nil
}

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
		clusterName, err := cluster()
		if err != nil {
			bconn.Close()
			return nil, err
		}
		// ON CLUSTER so a database created on one node exists on all of them:
		// a table replicated into a database that only one node has is still
		// unreachable from the others.
		if err := bconn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db+onCluster(clusterName)); err != nil {
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

// onCluster renders the ON CLUSTER clause, or "" for a single node.
func onCluster(cluster string) string {
	if cluster == "" {
		return ""
	}
	return " ON CLUSTER " + cluster
}

// Ensure creates the table and the TTL when missing.
func Ensure(ctx context.Context, conn clickhouse.Conn) error {
	clusterName, err := cluster()
	if err != nil {
		return err
	}
	// ReplicatedMergeTree takes the table path and replica identity from the
	// {shard}/{replica} macros the cluster sets per pod - the same values the
	// Keeper ensemble uses - so this stays identical for every node instead of
	// baking a hostname into the DDL. Same path on every node is what makes
	// them replicas rather than three shards.
	engine := "MergeTree"
	if clusterName != "" {
		engine = fmt.Sprintf("ReplicatedMergeTree('/clickhouse/tables/{shard}/%s', '{replica}')", Table)
	}
	ddl := `
	CREATE TABLE IF NOT EXISTS ` + Table + onCluster(clusterName) + ` (
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
	) ENGINE = ` + engine + `
	PARTITION BY (toDate(ts), retention_class)
	ORDER BY (owner, delivery_id, ts)
	TTL toDate(ts) + INTERVAL 7 DAY DELETE
	SETTINGS index_granularity = 8192`
	if err := conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("chstore: creating table: %w", err)
	}
	return nil
}
