package registry_test

import (
	"context"
	"testing"
	"time"

	"pubsub/internal/registry"
)

// staticSource serves a fixed set of control rows. The delivery stages only
// need a registry that knows a few subscriptions, never PostgreSQL.
type staticSource struct {
	types []registry.EventType
	subs  []registry.Subscription
}

func (s staticSource) EventTypes(context.Context) ([]registry.EventType, error) { return s.types, nil }

func (s staticSource) Subscriptions(context.Context) ([]registry.Subscription, error) {
	return s.subs, nil
}

// TestChannelFor pins the rule the dispatcher and the sse-gateway share: the
// current channel of a known subscription, the stamped channel of an unknown
// one. The two "now something else" cases are the whole point of the method -
// they are what makes an update apply to deliveries that were routed before
// it and are still sitting on the topic.
func TestChannelFor(t *testing.T) {
	const (
		webhookID = "00000000-0000-0000-0000-000000000001"
		sseID     = "00000000-0000-0000-0000-000000000002"
		unknownID = "00000000-0000-0000-0000-000000000003"
	)
	reg := registry.New(staticSource{subs: []registry.Subscription{
		{ID: webhookID, Owner: "put", EventType: "put.test", Channel: "webhook", CallbackURL: "http://example.test/hook"},
		{ID: sseID, Owner: "put", EventType: "put.test", Channel: "sse"},
	}}, time.Hour)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("loading the fake registry: %v", err)
	}

	tests := []struct {
		name    string
		id      string
		stamped string
		want    string
	}{
		{"unchanged webhook", webhookID, "webhook", "webhook"},
		{"stamped sse, now webhook", webhookID, "sse", "webhook"},
		{"unchanged sse", sseID, "sse", "sse"},
		{"stamped webhook, now sse", sseID, "webhook", "sse"},
		{"unknown id keeps its stamp", unknownID, "webhook", "webhook"},
		{"unknown id keeps its sse stamp", unknownID, "sse", "sse"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reg.ChannelFor(tt.id, tt.stamped); got != tt.want {
				t.Errorf("ChannelFor(%q, %q) = %q, want %q", tt.id, tt.stamped, got, tt.want)
			}
		})
	}
}
