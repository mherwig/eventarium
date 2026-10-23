// Package kafkautil centralises topic names, topic creation and the franz-go
// client defaults shared by every service.
package kafkautil

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
func NewClient(extra ...kgo.Opt) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokerList()...),
		kgo.ProducerLinger(2 * time.Millisecond),
		kgo.ProducerBatchMaxBytes(4 << 20),
		kgo.RequiredAcks(kgo.AllISRAcks()),
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
