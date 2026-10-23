package sse

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"pubsub/internal/cloudevents"
	"pubsub/internal/delivery"
)

// maxQueue is the bound of the per-connection delivery queue (section 6.5
// connection hygiene). When it overflows the connection is closed: the client
// reconnects with Last-Event-ID and fetches the backlog from the inbox, which
// is the recovery path of section 8. Events are never silently discarded.
const maxQueue = 1000

// maxSentLog bounds the per-connection memory used for ack bookkeeping. Only
// the most recent deliveries are remembered, so a cumulative ack that reaches
// further back than the window is honoured for what is still in the log.
const maxSentLog = 4096

// ssePayload is the data member of one SSE event.
type ssePayload struct {
	DeliveryID string             `json:"delivery_id"`
	EventRef   string             `json:"event_ref"`
	Sequence   string             `json:"sequence"`
	Replay     bool               `json:"replay,omitempty"`
	Event      *cloudevents.Event `json:"event"`
}

// outbound is one delivery queued for one connection.
type outbound struct {
	seq delivery.Seq
	msg *delivery.Message
}

// frame renders the SSE wire format. The sequence number of the deliveries
// topic is the event id, so the client can echo it back as Last-Event-ID or in
// the ack body; that id is the ack handle (section 6.3).
func (o outbound) frame() (string, error) {
	payload := ssePayload{
		DeliveryID: o.msg.DeliveryID,
		EventRef:   o.msg.EventRef,
		Sequence:   o.seq.String(),
		Replay:     o.msg.Replay,
		Event:      o.msg.Event,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	return "id: " + o.seq.String() + "\nevent: delivery\ndata: " + string(b) + "\n\n", nil
}

// connection is one open SSE stream. The handler goroutine is the only writer;
// everything else hands deliveries over through the bounded out channel.
type connection struct {
	id         string
	subID      string
	consumerID string
	owner      string
	openedAt   time.Time

	out  chan outbound
	done chan struct{}
	once sync.Once

	mu     sync.Mutex
	reason string
	sent   []outbound // deliveries written to the wire, for cumulative acks
}

func newConnection(subID, consumerID, owner string) *connection {
	return &connection{
		id:         uuid.NewString(),
		subID:      subID,
		consumerID: consumerID,
		owner:      owner,
		openedAt:   time.Now().UTC(),
		out:        make(chan outbound, maxQueue),
		done:       make(chan struct{}),
	}
}

// close ends the connection once, with a reason that is written to the client
// as a final comment.
func (c *connection) close(reason string) {
	c.once.Do(func() {
		c.mu.Lock()
		c.reason = reason
		c.mu.Unlock()
		close(c.done)
	})
}

func (c *connection) closeReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

// appendSent remembers a delivery that is on the wire so that an ack can turn
// its status from sent into delivered.
func (c *connection) appendSent(o outbound) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, o)
	if len(c.sent) > maxSentLog {
		c.sent = c.sent[len(c.sent)-maxSentLog:]
	}
}

// ackUpTo removes and returns every remembered delivery up to and including
// seq. All deliveries of one subscription share the deliveries partition of
// that subscription (section 6.3), so the comparison is the offset; an ack
// that names a foreign partition acknowledges nothing.
func (c *connection) ackUpTo(seq delivery.Seq) []outbound {
	c.mu.Lock()
	defer c.mu.Unlock()
	var acked, kept []outbound
	for _, o := range c.sent {
		if o.seq.Partition == seq.Partition && o.seq.Offset <= seq.Offset {
			acked = append(acked, o)
			continue
		}
		kept = append(kept, o)
	}
	c.sent = kept
	return acked
}
