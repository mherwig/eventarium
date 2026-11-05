package schema

import (
	"encoding/json"
	"strings"
	"testing"
)

const orderSchema = `{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["order_id", "total"],
  "properties": {
    "order_id": {"type": "string"},
    "total":    {"type": "number", "minimum": 0},
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["sku", "qty"],
        "properties": {
          "sku": {"type": "string"},
          "qty": {"type": "integer", "minimum": 1}
        }
      }
    },
    "placed_at": {"type": "string", "format": "date-time"}
  },
  "additionalProperties": false
}`

func mustCompile(t *testing.T, raw string) *Schema {
	t.Helper()
	s, err := Compile([]byte(raw))
	if err != nil {
		t.Fatalf("compiling: %v", err)
	}
	return s
}

func decode(t *testing.T, data string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(data), &v); err != nil {
		t.Fatalf("decoding test data: %v", err)
	}
	return v
}

func TestValidDataPasses(t *testing.T) {
	s := mustCompile(t, orderSchema)
	err := s.Validate(decode(t,
		`{"order_id":"o-1","total":99.9,"items":[{"sku":"a","qty":2}]}`))
	if err != nil {
		t.Fatalf("valid data rejected: %v", err)
	}
}

// The message a publisher sees is the whole feature: it has to name the
// property that is wrong, not just "validation failed".
func TestViolationNamesTheFailingLocation(t *testing.T) {
	s := mustCompile(t, orderSchema)

	for _, tc := range []struct {
		data string
		path string
		want string // a fragment the message must contain
	}{
		{`{"total": 1}`, "", "order_id"},
		{`{"order_id":"o-1","total":"99.9"}`, "/total", "number"},
		{`{"order_id":"o-1","total":1,"items":[{"sku":"a","qty":"two"}]}`,
			"/items/0/qty", "integer"},
		{`{"order_id":"o-1","total":-1}`, "/total", "1"},
		// additionalProperties reports at the object the keyword applies to,
		// not at the offending property - that is what the spec says, and the
		// message names the property anyway.
		{`{"order_id":"o-1","total":1,"extra":true}`, "", "extra"},
		{`{"order_id":"o-1","total":1,"placed_at":"yesterday"}`,
			"/placed_at", "date-time"},
	} {
		err := s.Validate(decode(t, tc.data))
		if err == nil {
			t.Errorf("%s: accepted, want a violation", tc.data)
			continue
		}
		v, ok := err.(*Violation)
		if !ok {
			t.Errorf("%s: got %T, want *Violation", tc.data, err)
			continue
		}
		if v.Path != tc.path {
			t.Errorf("%s: path = %q, want %q (message %q)", tc.data, v.Path, tc.path, v.Message)
		}
		if !strings.Contains(strings.ToLower(v.Message), strings.ToLower(tc.want)) {
			t.Errorf("%s: message %q does not mention %q", tc.data, v.Message, tc.want)
		}
		if v.Error() == "" {
			t.Error("violation renders as an empty string")
		}
	}
}

// Formats are asserted rather than treated as annotations. A quality gate
// that lets "date-time" pass as a comment is not much of a gate.
func TestFormatIsAsserted(t *testing.T) {
	s := mustCompile(t, `{"type":"object","properties":{"at":{"type":"string","format":"date-time"}}}`)
	if err := s.Validate(decode(t, `{"at":"2026-11-05T10:00:00Z"}`)); err != nil {
		t.Errorf("a real timestamp rejected: %v", err)
	}
	if err := s.Validate(decode(t, `{"at":"soon"}`)); err == nil {
		t.Error("a non-timestamp accepted despite format: date-time")
	}
}

// A local $ref has to work - composition is the main reason to reach for a
// schema at all.
func TestLocalRefResolves(t *testing.T) {
	s := mustCompile(t, `{
	  "type": "object",
	  "properties": {"a": {"$ref": "#/$defs/positive"}},
	  "$defs": {"positive": {"type": "integer", "minimum": 1}}
	}`)
	if err := s.Validate(decode(t, `{"a": 5}`)); err != nil {
		t.Errorf("local $ref rejected valid data: %v", err)
	}
	err := s.Validate(decode(t, `{"a": 0}`))
	if err == nil {
		t.Fatal("local $ref did not apply: 0 accepted")
	}
	if v, ok := err.(*Violation); !ok || v.Path != "/a" {
		t.Errorf("violation = %v, want path /a", err)
	}
}

// A remote $ref would be an SSRF vector and would make validation depend on a
// server nobody controls, so it is refused at compile time - which means at
// registration, not at publish.
func TestRemoteRefIsRejected(t *testing.T) {
	_, err := Compile([]byte(
		`{"type":"object","properties":{"a":{"$ref":"https://example.com/schema.json"}}}`))
	if err == nil {
		t.Fatal("a remote $ref compiled; only local references are allowed")
	}
	if !strings.Contains(err.Error(), "$ref") {
		t.Errorf("error %q does not mention $ref", err)
	}
}

func TestCompileRejectsWhatRegistrationShouldCatch(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{"empty", "", "empty"},
		{"not JSON", "{oops", "not valid JSON"},
		{"not an object", `[1,2,3]`, "JSON object"},
		{"unknown keyword is fine", `{"type":"nonsense"}`, ""}, // library's problem, not ours
		{"broken ref", `{"$ref": "#/$defs/missing"}`, ""},
		{"oversized", strings.Repeat(" ", MaxSchemaBytes+1), "at most"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile([]byte(tc.raw))
			if tc.want == "" {
				// Either outcome is defensible for these; what matters is
				// that they do not panic.
				return
			}
			if err == nil {
				t.Fatalf("accepted (%s)", tc.raw)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}

// A schema must be an object: an array or a bare string compiles into nothing
// useful and would silently pass everything.
func TestNonObjectSchemaIsRejected(t *testing.T) {
	if _, err := Compile([]byte(`"just a string"`)); err == nil {
		t.Fatal("a non-object schema was accepted")
	}
}
