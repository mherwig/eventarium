// Admin is the operations helper of the spike. It provisions the platform
// infrastructure: Kafka topics (section 12), the PostgreSQL schema (section
// 6.6) and the ClickHouse status table (section 9).
//
//	admin topics   create the platform topics with their retention settings
//	admin migrate  apply the PostgreSQL migrations
//	admin ch       create the ClickHouse database and status table
//	admin all      all of the above, in order
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"pubsub/internal/chstore"
	"pubsub/internal/config"
	"pubsub/internal/kafkautil"
	"pubsub/internal/store"
	"pubsub/migrations"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: admin <topics|migrate|ch|all>")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cmd := os.Args[1]
	fail := func(stage string, err error) {
		fmt.Fprintf(os.Stderr, "admin: %s: %v\n", stage, err)
		os.Exit(1)
	}

	if cmd == "topics" || cmd == "all" {
		if err := kafkautil.EnsureTopics(ctx, kafkautil.Bootstrap(), kafkautil.Specs()); err != nil {
			fail("topics", err)
		}
		fmt.Println("admin: kafka topics ready")
	}
	if cmd == "migrate" || cmd == "all" {
		st, err := store.Open(ctx, config.Str("PG_DSN", "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"))
		if err != nil {
			fail("migrate", err)
		}
		defer st.Close()
		if err := migrations.Apply(ctx, st.Pool); err != nil {
			fail("migrate", err)
		}
		fmt.Println("admin: postgres schema ready")
	}
	if cmd == "ch" || cmd == "all" {
		conn, err := chstore.Open(ctx, config.Str("CH_DSN", "clickhouse://pubsub:pubsub@localhost:9000/pubsub"))
		if err != nil {
			fail("ch", err)
		}
		defer conn.Close()
		if err := chstore.Ensure(ctx, conn); err != nil {
			fail("ch", err)
		}
		fmt.Println("admin: clickhouse schema ready")
	}
	if cmd != "topics" && cmd != "migrate" && cmd != "ch" && cmd != "all" {
		fmt.Fprintf(os.Stderr, "admin: unknown command %q\n", cmd)
		os.Exit(2)
	}
}
