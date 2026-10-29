package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"pubsub/internal/registry"
)

// sourceWithSubs is a registry source with the event type under test and an
// arbitrary set of subscriptions for it.
type sourceWithSubs struct {
	owner, eventType string
	subs             []registry.Subscription
}

func (s sourceWithSubs) EventTypes(context.Context) ([]registry.EventType, error) {
	return []registry.EventType{{
		Owner:          s.owner,
		Name:           s.eventType,
		RetentionClass: "short",
		ContentType:    "application/json",
	}}, nil
}

func (s sourceWithSubs) Subscriptions(context.Context) ([]registry.Subscription, error) {
	return s.subs, nil
}

func registryWith(t *testing.T, owner, eventType string, subs ...registry.Subscription) *registry.Registry {
	t.Helper()
	reg := registry.New(sourceWithSubs{owner: owner, eventType: eventType, subs: subs}, time.Minute)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("loading the fake registry: %v", err)
	}
	return reg
}

// TestNoSubscriberWarning is the publisher-facing half of filter tracking: an
// event type nobody subscribes to is accepted by the publish API and then
// routed to nothing, which before this was invisible at the source.
//
// It also pins the deliberate shape of the answer: a warning only for the
// zero case, never a subscription count - the count would be a candidate
// count (publish does not evaluate CEL filters) and would read like a
// delivery forecast.
func TestNoSubscriberWarning(t *testing.T) {
	owner, eventType := "team-a", "orders.created"

	none := registryWith(t, owner, eventType)
	got := noSubscriberWarning(none, owner, eventType)
	if got == "" {
		t.Fatal("no warning for an event type without subscriptions, want one")
	}
	if !strings.Contains(got, eventType) || !strings.Contains(got, "nothing will be delivered") {
		t.Errorf("warning = %q, want it to name the event type and say that nothing is delivered", got)
	}

	subscribed := registryWith(t, owner, eventType, registry.Subscription{
		ID: "sub-1", Owner: owner, EventType: eventType, Channel: "webhook",
	})
	if got := noSubscriberWarning(subscribed, owner, eventType); got != "" {
		t.Errorf("warning for a subscribed event type = %q, want none", got)
	}

	// Another owner's subscription does not count for this one.
	elsewhere := registryWith(t, owner, eventType, registry.Subscription{
		ID: "sub-2", Owner: "team-b", EventType: eventType, Channel: "webhook",
	})
	if got := noSubscriberWarning(elsewhere, owner, eventType); got == "" {
		t.Error("subscription of another owner suppressed the warning, want it reported")
	}
}
