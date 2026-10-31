// Package kafkautil centralises topic names, topic creation and the franz-go
// client defaults shared by every service.
package kafkautil

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"pubsub/internal/config"
)

// Topic names (section 3 of the spec).
const (
	TopicDeliveries = "deliveries"
	TopicStatus     = "delivery-status"
	TopicPresence   = "sse-presence" // compacted: connection presence per subscription
	TopicSSELive    = "sse-live"     // fan-out from the SSE delivery stage to gateway nodes
)

// Retention classes (section 12 of the spec).
const (
	ClassLive  = "live"
	ClassShort = "short"
	ClassDay   = "day"
	ClassWeek  = "week"
)

// Classes lists the retention classes in no particular order.
var Classes = []string{ClassLive, ClassShort, ClassDay, ClassWeek}

// Failover timing: one pair, two values that are only correct together.
//
// SessionTimeout is how long Kafka waits for a group member to stop
// heartbeating before it calls the member dead and rebalances. It is the crash
// floor: after a `kill -9`, an OOMKill or a lost node, the dead member's
// partitions belong to nobody until it expires, and only then move. Measured on
// the three-dispatcher group (docs/deviations-and-open-points.md): 43 s until
// the partitions were owned again with the default, against 3 s for a graceful
// SIGTERM - which is why a rolling update feels instant and a crash does not.
// The records are never at risk while the group waits; the costs are latency on
// a third of the partitions and, on recovery, the replay duplicates.
//
// TransactionTimeout must stay *below* SessionTimeout, or a
// consume-modify-produce transaction can outlive the member that owns it.
// franz-go states that rule and enforces neither end of it, so Timeouts()
// clamps the pair - and unlike a session timeout below Kafka's floor, which
// fails loudly when the member tries to join, a transaction that outlives its
// member fails silently. Heartbeats, for the record, run in franz-go's own
// background group goroutine rather than in the application's poll loop, so a
// slow publish or a long GC pause does not by itself miss them; a process
// frozen or partitioned for longer than the value does, and evicting that one
// is the point.
//
// Both are configurable (KAFKA_SESSION_TIMEOUT, KAFKA_TRANSACTION_TIMEOUT) so
// the choice can be made without a rebuild. The defaults are the values this
// codebase has actually been measured with - changing either means re-running
// the rebalance scenario, not just editing a comment.
const (
	// Both were franz-go's defaults (45s / 40s) until they were measured
	// against this platform and deliberately lowered. The chaos scenario
	// `rebalance` reports what each buys:
	//
	//	45s / 40s  handover 46s, 45s - the crash floor, unchanged since the start
	//	15s / 10s  handover 13s        - oracle identical, abort count identical
	//
	// 13s instead of 46s for a third of the partitions to resume after an
	// ungraceful death, with the oracle still reporting missing=0,
	// unexpected=0 and received==expected. The transaction half had to fall
	// too: it must stay below the session, and a transaction that outlives its
	// budget is dropped by the broker and re-consumed - duplicates, never
	// loss, which is the failure mode this platform already accepts. Measured
	// at 1,000 ev/s as well (the router writes every batch transactionally) to
	// confirm 10s holds under the rate the spike actually runs at.
	//
	// Both remain environment-tunable, so the conservative pair is one
	// KAFKA_SESSION_TIMEOUT away if a deployment would rather keep the
	// longer floor.
	DefaultSessionTimeout     = 15 * time.Second
	DefaultTransactionTimeout = 10 * time.Second
	transactionHeadroom       = 5 * time.Second // the gap the clamp keeps
)

// Timeouts returns the session and transaction timeouts as a pair, honouring
// KAFKA_SESSION_TIMEOUT / KAFKA_TRANSACTION_TIMEOUT and enforcing
// txn < session. Callers that are group members and callers that write inside
// a transaction both take their values from here, so the two can never be
// configured apart - they live in kafkautil precisely because franz-go's
// coupling of them lives in a library comment.
//
// A transaction timeout that is not below the session timeout is not rejected,
// it is adjusted: refusing would take a service down over an environment
// variable, while passing the pair through unclamped would risk a transaction
// finishing after a rebalance - the failure its own documentation warns about
// and nothing else checks. The adjustment is logged, never silent.
func Timeouts() (session, txn time.Duration) {
	session = config.Duration("KAFKA_SESSION_TIMEOUT", DefaultSessionTimeout)
	txn = config.Duration("KAFKA_TRANSACTION_TIMEOUT", DefaultTransactionTimeout)
	if txn >= session {
		adj := session - transactionHeadroom
		if adj <= 0 {
			adj = session / 2 // an absurdly small session still needs a positive txn
		}
		slog.Warn("kafkautil: KAFKA_TRANSACTION_TIMEOUT must be below KAFKA_SESSION_TIMEOUT, using the smaller value",
			"session", session, "requested_txn", txn, "using_txn", adj)
		txn = adj
	}
	return session, txn
}

// ValidClass reports whether class is a known retention class.
func ValidClass(class string) bool {
	for _, c := range Classes {
		if c == class {
			return true
		}
	}
	return false
}

// EventsTopic returns the per-class event log topic.
func EventsTopic(class string) string { return "events-" + class }

// DeliveryPartitions is the fixed partition count of the deliveries topic.
// The sequence number (section 6.3) depends on it, so it must never change.
const DeliveryPartitions = 12

// TopicSpec describes a topic to create.
type TopicSpec struct {
	Name        string
	Partitions  int32
	RetentionMS int64 // -1: no retention config, 0: leave broker default
	Compact     bool
}

// Specs returns every platform topic with its configuration. Each events topic
// keeps at least an operational floor on top of its class retention so the
// router can survive a short outage (section 12).
func Specs() []TopicSpec {
	hour := int64(time.Hour / time.Millisecond)
	return []TopicSpec{
		{Name: EventsTopic(ClassLive), Partitions: 12, RetentionMS: 2 * hour},
		{Name: EventsTopic(ClassShort), Partitions: 12, RetentionMS: 4 * hour},
		{Name: EventsTopic(ClassDay), Partitions: 12, RetentionMS: 26 * hour},
		{Name: EventsTopic(ClassWeek), Partitions: 12, RetentionMS: 7*24*hour + 6*hour},
		// The delivery topic keeps 24 to 48 hours because slow subscriptions
		// move to the inbox (section 6.3).
		{Name: TopicDeliveries, Partitions: DeliveryPartitions, RetentionMS: 24 * hour},
		{Name: TopicStatus, Partitions: 6, RetentionMS: 48 * hour},
		{Name: TopicSSELive, Partitions: 12, RetentionMS: 15 * 60 * 1000},
		{Name: TopicPresence, Partitions: 12, Compact: true},
	}
}

// Bootstrap returns the broker list from KAFKA_BROKERS.
func Bootstrap() []string { return brokerList() }

func brokerList() []string {
	brokers := envOr("KAFKA_BROKERS", "localhost:9092")
	out := []string{}
	start := 0
	for i := 0; i <= len(brokers); i++ {
		if i == len(brokers) || brokers[i] == ',' {
			if i > start {
				out = append(out, brokers[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// NewClient builds a franz-go client with the platform defaults.
//
// FetchIsolationLevel(ReadCommitted) is the platform default because two
// stages produce transactionally (section 9): the router with its group
// transact session and the replay worker. Kafka's own default is
// read_uncommitted, so a consumer would fetch records of a transaction that
// later aborted. Observed in the live cluster: the router aborted its first
// transaction and the dispatcher still delivered the rolled-back record -
// 21 delivery records for 20 published events, only 20 of them committed.
// The option affects fetches only; producers ignore it. Consumers of topics
// that are never written transactionally (the event logs) are unaffected,
// because uncommitted records do not exist there.
func NewClient(extra ...kgo.Opt) (*kgo.Client, error) {
	// Both timeouts are set for every client, transactional or not: the
	// session one only takes effect for group members and the transaction one
	// only inside a transaction, and setting them together is what stops the
	// pair from being configured apart (see Timeouts).
	session, txn := Timeouts()
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokerList()...),
		kgo.ProducerLinger(2 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(4 << 20),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.SessionTimeout(session),
		kgo.TransactionTimeout(txn),
	}
	opts = append(opts, extra...)
	return kgo.NewClient(opts...)
}

// EnsureTopics creates the missing platform topics with their configurations.
func EnsureTopics(ctx context.Context, brokers []string, specs []TopicSpec) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()

	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10000
	for _, s := range specs {
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic = s.Name
		t.NumPartitions = s.Partitions
		t.ReplicationFactor = 1
		var configs []kmsg.CreateTopicsRequestTopicConfig
		set := func(k, v string) { configs = append(configs, kmsg.CreateTopicsRequestTopicConfig{Name: k, Value: &v}) }
		if s.Compact {
			set("cleanup.policy", "compact")
			set("segment.ms", strconv.FormatInt(int64(time.Hour/time.Millisecond), 10))
		} else {
			set("cleanup.policy", "delete")
			set("segment.ms", strconv.FormatInt(int64(time.Hour/time.Millisecond), 10))
			if s.RetentionMS > 0 {
				set("retention.ms", strconv.FormatInt(s.RetentionMS, 10))
			}
		}
		t.Configs = configs
		req.Topics = append(req.Topics, t)
	}
	res, err := req.RequestWith(ctx, cl)
	if err != nil {
		return fmt.Errorf("kafkautil: creating topics: %w", err)
	}
	for _, t := range res.Topics {
		if t.ErrorCode != 0 && t.ErrorCode != 36 { // 36: TOPIC_ALREADY_EXISTS
			return fmt.Errorf("kafkautil: creating topic %s: error code %d", t.Topic, t.ErrorCode)
		}
	}
	return nil
}

// HSet builds a string record header.
func HSet(key, value string) kmsg.Header {
	return kmsg.Header{Key: key, Value: []byte(value)}
}

// HGet returns the string value of a record header, or "".
func HGet(rec *kgo.Record, key string) string {
	for _, h := range rec.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

// Header names for platform metadata that travels next to the CloudEvent
// (section 5: delivery metadata is transport metadata, never part of the event).
const (
	HeaderEventRef = "pubsub-ref"
	HeaderOwner    = "pubsub-owner"
	HeaderClass    = "pubsub-class"
	HeaderExpires  = "pubsub-expires-at" // unix milliseconds
	HeaderIngested = "pubsub-ingested-at"
	HeaderPKey     = "pubsub-partition-key"
)
