// Package schema validates an event's data member against a JSON Schema
// registered with its event type - the publish-time quality gate.
//
// JSON Schema rather than anything else because the payload is JSON and
// because the document stays portable: the same file validates here, in CI,
// in an editor and in a downstream consumer's own pipeline. OpenAPI describes
// HTTP APIs rather than data instances, AsyncAPI describes the messaging
// topology and does not validate at all, and Avro/Protobuf would mean a
// different wire format. Both of those hold a JSON Schema inside them, which
// is the part worth keeping.
package schema

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

// MaxSchemaBytes bounds a registered schema, mirroring MAX_EVENT_BYTES for
// payloads. Schemas are read by every service that loads the registry and
// compiled by the publisher, so one absurd document would be paid for on
// every reload rather than once.
const MaxSchemaBytes = 1 << 20 // 1 MiB

// resourceURL is the identifier the document is registered under. Refs are
// resolved against it, and the loader below refuses anything else, which is
// what makes $ref local-only: a remote reference would be both an SSRF
// vector and a promise that validation depends on a server nobody controls.
const resourceURL = "urn:pubsub:event-schema"

// localOnly refuses to fetch a schema from anywhere but the document itself.
type localOnly struct{}

func (localOnly) Load(url string) (any, error) {
	return nil, fmt.Errorf(
		"schema: $ref %q points outside this document; inline the definition instead of referencing it",
		url)
}

// Schema is a compiled JSON Schema, safe for concurrent use once built.
type Schema struct {
	sch *jsonschema.Schema
}

// Compile parses and compiles raw, returning an error a person can act on.
//
// It is called at registration, so that a schema nobody can compile is
// rejected there rather than discovered at publish time - and by the publisher
// as a safety net, in case a row was written without going through the API.
func Compile(raw []byte) (*Schema, error) {
	if len(raw) == 0 {
		return nil, errors.New("schema: empty document")
	}
	if len(raw) > MaxSchemaBytes {
		return nil, fmt.Errorf("schema: document is %d bytes; at most %d are accepted",
			len(raw), MaxSchemaBytes)
	}

	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("schema: not valid JSON: %w", err)
	}
	if _, ok := doc.(map[string]any); !ok {
		return nil, errors.New("schema: a schema must be a JSON object")
	}

	c := jsonschema.NewCompiler()
	// 2020-12 unless the document says otherwise in its $schema member, and
	// formats asserted rather than treated as annotations. A quality gate
	// that lets "format": "date-time" through as a comment is not much of a
	// gate; the trade-off is that formats are stricter than a naive
	// validator expects, which is the point of asking for one.
	c.DefaultDraft(jsonschema.Draft2020)
	c.AssertFormat()
	c.UseLoader(localOnly{})

	if err := c.AddResource(resourceURL, doc); err != nil {
		return nil, fmt.Errorf("schema: registering the document: %w", err)
	}
	sch, err := c.Compile(resourceURL)
	if err != nil {
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Schema{sch: sch}, nil
}

// Violation is one reason the data did not match: where it failed and why.
type Violation struct {
	// Path is a JSON Pointer into the data member, "" for the document root.
	Path string `json:"path"`
	// Message is the schema's own complaint, e.g. `got string, want integer`.
	Message string `json:"message"`
}

func (v Violation) Error() string {
	if v.Path == "" {
		return v.Message
	}
	return v.Path + ": " + v.Message
}

// Violations is every reason the data did not match, in the order the schema
// reported them. An empty value means the data matched.
//
// Plural because the schema will happily report several at once - two
// properties of the wrong type, a missing property alongside them, one bad
// element after another in an array - and a publisher who is shown one at a
// time has to fix, republish and discover the next. That loop is the whole
// reason this is returned rather than a single error.
type Violations []Violation

func (vs Violations) Error() string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = v.Error()
	}
	return strings.Join(parts, "; ")
}

// printer renders a schema's own message. The library builds the same one for
// its own Error(), which prefixes it with the location - split out here so a
// caller can report the path as a field rather than making every consumer
// parse it back out of a string.
var printer = message.NewPrinter(language.English)

// jsonPointer renders tokens per RFC 6901: a property containing a slash or a
// tilde has to be escaped, or the pointer names a different location than the
// one that failed.
func jsonPointer(tokens []string) string {
	if len(tokens) == 0 {
		return ""
	}
	var b strings.Builder
	for _, t := range tokens {
		b.WriteByte('/')
		b.WriteString(strings.NewReplacer("~", "~0", "/", "~1").Replace(t))
	}
	return b.String()
}

// Validate checks an already-decoded data member against the schema, returning
// every reason it did not match - empty when it did.
//
// The caller has usually parsed the data already - publish-api does, for the
// JSON-object check it already performs - so this takes the decoded value
// rather than bytes and does not parse a second time.
func (s *Schema) Validate(v any) Violations {
	err := s.sch.Validate(v)
	if err == nil {
		return nil
	}
	var ve *jsonschema.ValidationError
	if !errors.As(err, &ve) {
		// Not the shape the library documents; still a failure, and the
		// rendered form at least says something.
		return Violations{{Message: err.Error()}}
	}
	return toViolations(ve)
}

// toViolations flattens the library's error tree into one entry per leaf.
//
// The tree is nested for the compositional keywords - an anyOf failure sits at
// the instance where the choice was attempted and carries each branch's
// complaint underneath - so the top level alone would say "anyOf failed" at
// "/" and name nothing useful. Every leaf is a concrete place and a concrete
// complaint, and a document can produce several of them at once: two
// properties of the wrong type, a missing property beside them, one bad
// element after another in an array.
//
// A node with no causes is itself the leaf, which is where a single failure
// such as `required` or a root-level `type` lands.
func toViolations(ve *jsonschema.ValidationError) Violations {
	if len(ve.Causes) == 0 {
		return Violations{leafViolation(ve)}
	}
	var out Violations
	for _, cause := range ve.Causes {
		out = append(out, toViolations(cause)...)
	}
	return out
}

func leafViolation(ve *jsonschema.ValidationError) Violation {
	v := Violation{Path: jsonPointer(ve.InstanceLocation)}
	if ve.ErrorKind != nil {
		v.Message = ve.ErrorKind.LocalizedString(printer)
	}
	if v.Message == "" {
		// Not the shape we expected; the rendered form still names the
		// location, so it is better than nothing.
		v.Message = ve.Error()
	}
	return v
}
