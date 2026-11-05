package delivery

import "testing"

// Extensions are tagged json:"-" because they are not fields of the struct -
// they are whatever the publisher sent. That tag also means a plain decode
// throws them away, and this is where it mattered: the dispatcher builds the
// webhook request from Message.Event, and SetHeaders writes e.Extensions onto
// it. With the decode dropping them, every extension the publisher sent
// arrived as no header at all - including the documented `ce-region`, which
// no test had ever pushed through a delivery message rather than constructing
// an Event by hand.
//
// The extension has to survive the verbatim EventRaw the router wrote.
func TestEventKeepsExtensions(t *testing.T) {
	raw := []byte(`{"specversion":"1.0","id":"42","source":"/orders","type":"orders.created",` +
		`"data":{"order_id":"o-1"},"x-business-process-id":"order-4711","shard":7}`)

	ev, err := (&Message{EventRaw: raw}).Event()
	if err != nil {
		t.Fatalf("decoding the event: %v", err)
	}

	for _, tc := range []struct{ name, want string }{
		{"x-business-process-id", "order-4711"},
		{"shard", "7"},
	} {
		got, ok := ev.Extension(tc.name)
		if !ok {
			t.Errorf("extension %q was dropped by the decode", tc.name)
			continue
		}
		if got != tc.want {
			t.Errorf("extension %q = %q, want %q", tc.name, got, tc.want)
		}
	}

	// The attributes the struct does have must be unaffected.
	if ev.ID != "42" || ev.Type != "orders.created" {
		t.Errorf("attributes = id %q type %q, want 42 / orders.created", ev.ID, ev.Type)
	}
	if string(ev.Data) != `{"order_id":"o-1"}` {
		t.Errorf("data = %s, want the raw data member", ev.Data)
	}
}

// An event with no extensions must decode to a nil map, exactly as it did
// before UnmarshalJSON existed - nothing downstream should have to learn the
// difference between "none" and "not present".
func TestEventWithoutExtensionsKeepsNone(t *testing.T) {
	raw := []byte(`{"specversion":"1.0","id":"42","source":"/orders","type":"orders.created"}`)

	ev, err := (&Message{EventRaw: raw}).Event()
	if err != nil {
		t.Fatalf("decoding the event: %v", err)
	}
	if len(ev.Extensions) != 0 {
		t.Errorf("extensions = %v, want none", ev.Extensions)
	}
}
