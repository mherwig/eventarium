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
	DeliveryID     string    `json:"delivery_id"` // event_ref + "/" + subscription_id
	EventRef       string    `json:"event_ref"`
	SubscriptionID string    `json:"subscription_id"`
	Owner          string    `json:"owner"`
	EventType      string    `json:"event_type"`
	Channel        string    `json:"channel"`
	ReplayID       string    `json:"replay_id,omitempty"`
	Replay         bool      `json:"replay,omitempty"`
	RetentionClass string    `json:"retention_class"`
	OrderingKey    string    `json:"ordering_key"`
	SrcPartition   int32     `json:"src_partition"` // partition of the events topic
	SrcOffset      int64     `json:"src_offset"`    // offset in the events topic
	CreatedAt      time.Time `json:"created_at"`
	ExpiresAt      time.Time `json:"expires_at"`
	Projected      bool      `json:"projected"`
	// Event is held as the bytes it was written with rather than as a parsed
	// CloudEvent. Stages that only route need the envelope above and nothing
	// else: the dispatcher decides which subscription owns a record before it
	// ever formats a body, and about half of all records are SSE deliveries it
	// immediately hands to another stage - both would otherwise unmarshal the
	// payload out of every single record for nothing. Keeping the bytes also
	// means Encode writes back exactly what arrived instead of round-tripping
	// the event through a struct just to serialise it again, and a structured
	// webhook body can be the original bytes rather than a re-encoding of them.
	EventRaw json.RawMessage `json:"event"`

	// Parsed form, filled lazily by Event. Unexported so it never reaches the
	// wire: only EventRaw does.
	ev    *cloudevents.Event
	evErr error
	evSet bool
}

// NewID builds the delivery ID: event reference plus subscription ID
// (section 5 of the spec). A replay gets its own delivery records but the same
// delivery ID, so the replay ID is carried separately.
func NewID(eventRef, subscriptionID string) string {
	return eventRef + "/" + subscriptionID
}

// Encode serialises the message for the Kafka record value or the inbox payload.
// EventRaw goes out verbatim, so what a stage re-encodes (an inbox row, say) is
// byte-for-byte what arrived rather than a re-serialisation of a parsed struct.
func (m *Message) Encode() ([]byte, error) { return json.Marshal(m) }

// Event returns the CloudEvent, decoding EventRaw on first use. Stages that
// build a body or a header need it; stages that only route never call it and
// never pay for it.
//
// The decode is the same (*cloudevents.Event).UnmarshalJSON it has always
// been - only the moment it happens has moved, from every Decode to the first
// caller that actually wants a parsed event. Note the difference on failure:
// a record whose event cannot be parsed now decodes fine and reports here,
// where the caller decides what it means, instead of failing up front. That
// also replaces a nil-pointer dereference (a record with no event member
// used to reach msg.Event.DataContentType as nil) with a returned error.
func (m *Message) Event() (*cloudevents.Event, error) {
	if !m.evSet {
		m.evSet = true
		var e cloudevents.Event
		if err := json.Unmarshal(m.EventRaw, &e); err != nil {
			m.evErr = fmt.Errorf("delivery: decoding the event: %w", err)
		} else {
			m.ev = &e
		}
	}
	return m.ev, m.evErr
}

// SetEvent stores ev as the bytes its JSON encoding produces, so Encode and a
// structured webhook body can both use them without serialising the event
// again. Stages that build a Message must call it before Encode: without it
// the message carries no event, and Encode will happily write null.
func (m *Message) SetEvent(ev *cloudevents.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("delivery: encoding the event: %w", err)
	}
	m.EventRaw = b
	m.ev, m.evErr, m.evSet = ev, nil, true
	return nil
}

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
