package dispatch

import (
	"context"
	"testing"
	"time"

	"pubsub/internal/delivery"
	"pubsub/internal/registry"
)

// channelSource is a static registry source, the same trick internal/routing
// uses: the gate tests need subscriptions without PostgreSQL.
type channelSource struct {
	subs []registry.Subscription
}

func (s channelSource) EventTypes(context.Context) ([]registry.EventType, error) { return nil, nil }

func (s channelSource) Subscriptions(context.Context) ([]registry.Subscription, error) {
	return s.subs, nil
}

// TestOwnsChannel pins the dispatcher's half of the stage split. The stamped
// channel alone decides nothing: an update has to move the deliveries that
// were already routed, and an unknown subscription has to keep behaving like
// it did before the update existed (webhook: dropped as unknown_subscription,
// sse: left to the gateway).
func TestOwnsChannel(t *testing.T) {
	const (
		webhookID = "00000000-0000-0000-0000-000000000001"
		sseID     = "00000000-0000-0000-0000-000000000002"
		unknownID = "00000000-0000-0000-0000-000000000003"
	)
	reg := registry.New(channelSource{subs: []registry.Subscription{
		{ID: webhookID, Owner: "o", EventType: "t", Channel: delivery.ChannelWebhook, CallbackURL: "http://example.test/hook"},
		{ID: sseID, Owner: "o", EventType: "t", Channel: delivery.ChannelSSE},
	}}, time.Hour)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("loading the fake registry: %v", err)
	}
	d := &Dispatcher{reg: reg}

	tests := []struct {
		name, id, stamped string
		want              bool
	}{
		{"webhook delivery, webhook sub", webhookID, delivery.ChannelWebhook, true},
		{"sse delivery, webhook sub (changed after routing)", webhookID, delivery.ChannelSSE, true},
		{"webhook delivery, sse sub (changed after routing)", sseID, delivery.ChannelWebhook, false},
		{"sse delivery, sse sub", sseID, delivery.ChannelSSE, false},
		{"orphaned webhook delivery", unknownID, delivery.ChannelWebhook, true},
		{"orphaned sse delivery", unknownID, delivery.ChannelSSE, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &delivery.Message{SubscriptionID: tt.id, Channel: tt.stamped}
			if got := d.ownsChannel(msg); got != tt.want {
				t.Errorf("ownsChannel(%s stamped %s) = %v, want %v",
					tt.id, tt.stamped, got, tt.want)
			}
		})
	}
}
