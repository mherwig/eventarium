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
	vs := s.Validate(decode(t,
		`{"order_id":"o-1","total":99.9,"items":[{"sku":"a","qty":2}]}`))
	if len(vs) != 0 {
		t.Fatalf("valid data rejected: %v", vs)
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
		vs := s.Validate(decode(t, tc.data))
		if len(vs) == 0 {
			t.Errorf("%s: accepted, want a violation", tc.data)
			continue
		}
		v := vs[0]
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

// Every leaf, not just the first. A publisher shown one violation at a time has
// to fix, republish and discover the next - and the schema reports them all in
// one pass anyway.
func TestValidateReportsEveryViolation(t *testing.T) {
	s := mustCompile(t, `{
	  "type": "object",
	  "required": ["a", "b"],
	  "properties": {"a": {"type": "string"}, "b": {"type": "integer"}}
	}`)

	vs := s.Validate(decode(t, `{"a": 1, "b": "x"}`))
	if len(vs) != 2 {
		t.Fatalf("got %d violations (%v), want 2: a missing-required and a wrong type each",
			len(vs), vs)
	}

	got := map[string]string{}
	for _, v := range vs {
		got[v.Path] = v.Message
	}
	if !strings.Contains(got["/a"], "string") {
		t.Errorf("/a message = %q, want the expected type", got["/a"])
	}
	if !strings.Contains(got["/b"], "integer") {
		t.Errorf("/b message = %q, want the expected type", got["/b"])
	}

	// Missing properties are reported by the required keyword as one entry
	// naming all of them, which is still the right answer.
	missing := s.Validate(decode(t, `{}`))
	if len(missing) != 1 {
		t.Fatalf("got %d violations for two missing properties (%v), want 1", len(missing), missing)
	}
	if !strings.Contains(missing[0].Message, "a") || !strings.Contains(missing[0].Message, "b") {
		t.Errorf("message %q does not name both missing properties", missing[0].Message)
	}

	// One entry per bad element, each with its own index.
	arrays := mustCompile(t,
		`{"type":"object","properties":{"n":{"type":"array","items":{"type":"integer"}}}}`)
	bad := arrays.Validate(decode(t, `{"n":["x","y"]}`))
	if len(bad) != 2 {
		t.Fatalf("got %d violations for two bad elements (%v), want 2", len(bad), bad)
	}
	if bad[0].Path != "/n/0" || bad[1].Path != "/n/1" {
		t.Errorf("paths = %q, %q, want /n/0 and /n/1", bad[0].Path, bad[1].Path)
	}
}

// The joined form is what goes into a 422 body.
func TestViolationsErrorJoinsThem(t *testing.T) {
	vs := Violations{
		{Path: "/a", Message: "got number, want string"},
		{Path: "", Message: "missing property 'c'"},
	}
	got := vs.Error()
	want := "/a: got number, want string; missing property 'c'"
	if got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// Formats are asserted rather than treated as annotations. A quality gate
// that lets "date-time" pass as a comment is not much of a gate.
func TestFormatIsAsserted(t *testing.T) {
	s := mustCompile(t, `{"type":"object","properties":{"at":{"type":"string","format":"date-time"}}}`)
	if vs := s.Validate(decode(t, `{"at":"2026-11-05T10:00:00Z"}`)); len(vs) != 0 {
		t.Errorf("a real timestamp rejected: %v", vs)
	}
	if vs := s.Validate(decode(t, `{"at":"soon"}`)); len(vs) == 0 {
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
	if vs := s.Validate(decode(t, `{"a": 5}`)); len(vs) != 0 {
		t.Errorf("local $ref rejected valid data: %v", vs)
	}
	vs := s.Validate(decode(t, `{"a": 0}`))
	if len(vs) == 0 {
		t.Fatal("local $ref did not apply: 0 accepted")
	}
	if vs[0].Path != "/a" {
		t.Errorf("violation path = %q, want /a", vs[0].Path)
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
		{"oversized", strings.Repeat(" ", MaxSchemaBytes+1), "at most"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Compile([]byte(tc.raw))
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
