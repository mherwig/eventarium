package sse

import (
	"context"
	"testing"
	"time"

	"pubsub/internal/delivery"
	"pubsub/internal/registry"
)

// channelSource is a static registry source: these tests need subscriptions,
// not PostgreSQL.
type channelSource struct {
	subs []registry.Subscription
}

func (s channelSource) EventTypes(context.Context) ([]registry.EventType, error) { return nil, nil }

func (s channelSource) Subscriptions(context.Context) ([]registry.Subscription, error) {
	return s.subs, nil
}

// TestOwnsChannel pins the gateway's half of the stage split: the same rule
// registry.ChannelFor gives the dispatcher, asked of the same registry. The
// two cases where the stamp and the current channel differ are the ones an
// update creates - a delivery routed before the change still has to reach the
// stage that owns the subscription now.
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
	g := &Gateway{reg: reg}

	tests := []struct {
		name, id, stamped string
		want              bool
	}{
		{"sse delivery, sse sub", sseID, delivery.ChannelSSE, true},
		{"webhook delivery, sse sub (changed after routing)", sseID, delivery.ChannelWebhook, true},
		{"sse delivery, webhook sub (changed after routing)", webhookID, delivery.ChannelSSE, false},
		{"webhook delivery, webhook sub", webhookID, delivery.ChannelWebhook, false},
		{"orphaned sse delivery", unknownID, delivery.ChannelSSE, true},
		{"orphaned webhook delivery", unknownID, delivery.ChannelWebhook, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := &delivery.Message{SubscriptionID: tt.id, Channel: tt.stamped}
			if got := g.ownsChannel(msg); got != tt.want {
				t.Errorf("ownsChannel(%s stamped %s) = %v, want %v",
					tt.id, tt.stamped, got, tt.want)
			}
		})
	}
}
