// Package delivery defines the message written to the deliveries topic and the
// identifiers derived from it. The same struct is the payload stored in the
// inbox (section 6.6 of the spec).
package delivery

import (
	"encoding/json"
	"fmt"
	"time"

	"pubsub/internal/cloudevents"
)

// Channel values.
const (
	ChannelWebhook = "webhook"
	ChannelSSE     = "sse"
)

// Delivery modes (section 7 of the spec).
const (
	ModeOrdered    = "ordered"
	ModeKeyOrdered = "key_ordered"
	ModeUnordered  = "unordered"
)

// Message is one delivery of one event to one subscription. It carries the
// already projected CloudEvent so that downstream stages never have to re-read
// the event log.
type Message struct {
	DeliveryID     string             `json:"delivery_id"` // event_ref + "/" + subscription_id
	EventRef       string             `json:"event_ref"`
	SubscriptionID string             `json:"subscription_id"`
	Owner          string             `json:"owner"`
	EventType      string             `json:"event_type"`
	Channel        string             `json:"channel"`
	ReplayID       string             `json:"replay_id,omitempty"`
	Replay         bool               `json:"replay,omitempty"`
	RetentionClass string             `json:"retention_class"`
	OrderingKey    string             `json:"ordering_key"`
	SrcPartition   int32              `json:"src_partition"` // partition of the events topic
	SrcOffset      int64              `json:"src_offset"`    // offset in the events topic
	CreatedAt      time.Time          `json:"created_at"`
	ExpiresAt      time.Time          `json:"expires_at"`
	Projected      bool               `json:"projected"`
	Event          *cloudevents.Event `json:"event"`
}

// NewID builds the delivery ID: event reference plus subscription ID
// (section 5 of the spec). A replay gets its own delivery records but the same
// delivery ID, so the replay ID is carried separately.
func NewID(eventRef, subscriptionID string) string {
	return eventRef + "/" + subscriptionID
}

// Encode serialises the message for the Kafka record value or the inbox payload.
func (m *Message) Encode() ([]byte, error) { return json.Marshal(m) }

// Decode builds a message from a Kafka record value or an inbox payload.
func Decode(b []byte) (*Message, error) {
	var m Message
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("delivery: decoding message: %w", err)
	}
	return &m, nil
}

// Seq identifies the position of a delivery on the deliveries topic. It is the
// sequence number of section 6.3: (partition, offset). The producer cannot know
// it, so it is assigned by the consuming stage.
type Seq struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (s Seq) String() string { return fmt.Sprintf("%d:%d", s.Partition, s.Offset) }
