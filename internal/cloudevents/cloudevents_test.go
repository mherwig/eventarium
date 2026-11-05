package cloudevents

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestSetHeadersRoundTripExtensions pins the binary-mode path that silently
// dropped extensions - the bug behind `missing=3345` in a single-event load
// run.
//
// SetHeaders used to do `json.Unmarshal(v, &s)` and skip the header when it
// failed. That works for a quoted string (`"key-24"`) and fails for a bare
// number (`24`), which is exactly what the two extensions this platform
// produces look like. Batch mode puts extensions in the JSON body, where they
// survive regardless; single/binary mode puts them in headers, where `shard`
// vanished. The router then evaluated `attributes["shard"] == "7"` against an
// empty map and rejected every event, while the oracle - reading pub.jsonl,
// which records the extensions from the generator's own event - counted them
// as matching. Same source, two answers, and an oracle that could not be
// trusted for the real publish mode.
//
// HTTP headers carry strings, so a non-string JSON extension is rendered as
// its raw JSON rather than dropped. The value changes shape (24 -> "24"); the
// attribute's presence does not, and presence is what a filter matches on.
func TestSetHeadersRoundTripExtensions(t *testing.T) {
	ev := &Event{
		SpecVersion: "1.0",
		ID:          "id-1",
		Source:      "/test",
		Type:        "test.type",
		Extensions: map[string]json.RawMessage{
			// The two shapes the platform actually produces.
			"partitionkey": json.RawMessage(`"key-24"`), // quoted string
			"shard":        json.RawMessage(`24`),       // bare number
		},
	}

	h := http.Header{}
	ev.SetHeaders(h)

	for _, tc := range []struct{ ext, want string }{
		{"partitionkey", "key-24"},
		{"shard", "24"},
	} {
		got := h.Get("Ce-" + tc.ext)
		if got != tc.want {
			t.Errorf("header for %q = %q, want %q (headers present: %v)",
				tc.ext, got, tc.want, h)
		}
	}

	parsed, err := ParseBinary(h, nil)
	if err != nil {
		t.Fatalf("ParseBinary: %v", err)
	}
	for _, name := range []string{"partitionkey", "shard"} {
		raw, ok := parsed.Extensions[name]
		if !ok {
			t.Errorf("extension %q did not survive binary mode; headers were %v", name, h)
			continue
		}
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Errorf("extension %q round-tripped as non-string %s", name, raw)
		}
	}
}

// TestParseBinaryKeepsCoreAttributes: the extension fix must not disturb the
// attributes the router and the registry both read.
func TestParseBinaryKeepsCoreAttributes(t *testing.T) {
	ev := &Event{
		SpecVersion: "1.0",
		ID:          "id-2",
		Source:      "/test",
		Type:        "test.type",
		Subject:     "entity-1",
	}
	h := http.Header{}
	ev.SetHeaders(h)

	parsed, err := ParseBinary(h, nil)
	if err != nil {
		t.Fatalf("ParseBinary: %v", err)
	}
	if parsed.ID != ev.ID || parsed.Source != ev.Source || parsed.Type != ev.Type || parsed.Subject != ev.Subject {
		t.Errorf("core attributes changed: got id=%q source=%q type=%q subject=%q",
			parsed.ID, parsed.Source, parsed.Type, parsed.Subject)
	}
}

// MarshalJSON writes extensions into the document and UnmarshalJSON has to put
// them back: a round trip through a Kafka record that silently loses them is
// how an extension stops reaching the callback, with nothing failing anywhere.
func TestMarshalUnmarshalRoundTripsExtensions(t *testing.T) {
	in := &Event{
		SpecVersion: "1.0",
		ID:          "id-1",
		Source:      "/test",
		Type:        "test.type",
		Data:        json.RawMessage(`{"a":1}`),
		Extensions: map[string]json.RawMessage{
			"x-business-process-id": json.RawMessage(`"order-4711"`),
			"shard":                 json.RawMessage(`7`),
		},
	}

	enc, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshalling: %v", err)
	}

	var out Event
	if err := json.Unmarshal(enc, &out); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}

	for _, tc := range []struct{ name, want string }{
		{"x-business-process-id", "order-4711"},
		{"shard", "7"},
	} {
		got, ok := out.Extension(tc.name)
		if !ok {
			t.Errorf("extension %q lost in the round trip: %s", tc.name, enc)
			continue
		}
		if got != tc.want {
			t.Errorf("extension %q = %q, want %q", tc.name, got, tc.want)
		}
	}
	if out.ID != "id-1" || string(out.Data) != `{"a":1}` {
		t.Errorf("attributes/data lost: id=%q data=%s", out.ID, out.Data)
	}
}

// An event with no extensions must still decode to a nil map, so that adding
// UnmarshalJSON did not change what "none" looks like to callers comparing
// against nil.
func TestUnmarshalWithoutExtensionsLeavesNone(t *testing.T) {
	var out Event
	if err := json.Unmarshal([]byte(
		`{"specversion":"1.0","id":"1","source":"/s","type":"t"}`), &out); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}
	if len(out.Extensions) != 0 {
		t.Errorf("extensions = %v, want none", out.Extensions)
	}
}
