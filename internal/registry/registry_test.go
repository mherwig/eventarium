package registry_test

import (
	"context"
	"errors"
	"strings"
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

// flakySource fails on demand: the readiness answer of /readyz depends on
// reloads that stopped succeeding, and this is how one stops.
type flakySource struct {
	staticSource
	fail bool
}

func (s *flakySource) Subscriptions(ctx context.Context) ([]registry.Subscription, error) {
	if s.fail {
		return nil, errors.New("postgres down")
	}
	return s.staticSource.Subscriptions(ctx)
}

// TestCheck pins what /readyz sees: an index that was never loaded is not
// ready, a freshly loaded one is, and one that is older than the limit is
// not - with the reload error in the message, because "the index is stale"
// is much less actionable than "and here is why".
func TestCheck(t *testing.T) {
	ctx := context.Background()
	src := &flakySource{staticSource: staticSource{subs: []registry.Subscription{
		{ID: "00000000-0000-0000-0000-000000000001", Owner: "put", EventType: "put.test", Channel: "sse"},
	}}}
	reg := registry.New(src, time.Hour)

	if err := reg.Check(0); err == nil {
		t.Fatal("Check before the first reload must fail: nothing is loaded")
	}
	if err := reg.Reload(ctx); err != nil {
		t.Fatalf("loading the fake registry: %v", err)
	}
	if err := reg.Check(registry.DefaultStaleAfter); err != nil {
		t.Fatalf("Check after a good reload: %v", err)
	}

	// A reload that fails keeps the previous index and says so; readiness
	// only breaks once the index itself has aged out.
	src.fail = true
	if err := reg.Reload(ctx); err == nil {
		t.Fatal("Reload with a failing source must fail")
	}
	if err := reg.Check(time.Nanosecond); err == nil {
		t.Error("Check with a limit shorter than the index age must fail")
	} else if !strings.Contains(err.Error(), "postgres down") {
		t.Errorf("Check error must carry the reload failure, got %q", err)
	}

	// A successful reload swaps a fresh index in, resets the failure count
	// and clears the recorded error: readiness is back.
	src.fail = false
	if err := reg.Reload(ctx); err != nil {
		t.Fatalf("reloading after recovery: %v", err)
	}
	if err := reg.Check(registry.DefaultStaleAfter); err != nil {
		t.Fatalf("Check after recovery: %v", err)
	}
}
