// Package cloudevents implements the subset of the CloudEvents 1.0 specification
// the platform needs: parsing of the HTTP binary and structured binding modes,
// batch parsing, validation and re-serialisation.
package cloudevents

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	SpecV1 = "1.0"

	ContentTypeStructured = "application/cloudevents+json"
	ContentTypeBatch      = "application/cloudevents-batch+json"

	// DefaultMaxBytes is the default upper bound for one event on the wire.
	DefaultMaxBytes = 1 << 20 // 1 MB
)

// ErrBatchTooLarge is returned when a batch exceeds the configured event count limit.
var ErrBatchTooLarge = errors.New("cloudevents: batch too large")

// Event is a CloudEvent. The data member is kept as raw JSON so that a value of
// any shape (including plain strings and numbers) survives a round trip.
type Event struct {
	SpecVersion     string                     `json:"specversion"`
	ID              string                     `json:"id"`
	Source          string                     `json:"source"`
	Type            string                     `json:"type"`
	DataContentType string                     `json:"datacontenttype,omitempty"`
	DataSchema      string                     `json:"dataschema,omitempty"`
	Subject         string                     `json:"subject,omitempty"`
	Time            *time.Time                 `json:"time,omitempty"`
	Data            json.RawMessage            `json:"data,omitempty"`
	DataBase64      string                     `json:"data_base64,omitempty"`
	Extensions      map[string]json.RawMessage `json:"-"`
}

var requiredMembers = map[string]bool{
	"specversion": true,
	"id":          true,
	"source":      true,
	"type":        true,
}

var optionalMembers = map[string]bool{
	"datacontenttype": true,
	"dataschema":      true,
	"subject":         true,
	"time":            true,
	"data":            true,
	"data_base64":     true,
}

// isKnown reports whether a top-level JSON member is a CloudEvents core member.
func isKnown(name string) bool { return requiredMembers[name] || optionalMembers[name] }

// Parse decodes one event from a raw structured-mode JSON document.
func Parse(data []byte) (*Event, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("cloudevents: invalid JSON: %w", err)
	}
	e := &Event{Extensions: make(map[string]json.RawMessage)}
	get := func(name string) (json.RawMessage, bool) { v, ok := m[name]; return v, ok }

	for name := range requiredMembers {
		v, ok := get(name)
		if !ok {
			return nil, fmt.Errorf("cloudevents: missing required attribute %q", name)
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return nil, fmt.Errorf("cloudevents: attribute %q must be a string", name)
		}
		switch name {
		case "specversion":
			e.SpecVersion = s
		case "id":
			e.ID = s
		case "source":
			e.Source = s
		case "type":
			e.Type = s
		}
	}
	str := func(name string) (string, error) {
		v, ok := get(name)
		if !ok {
			return "", nil
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return "", fmt.Errorf("cloudevents: attribute %q must be a string", name)
		}
		return s, nil
	}
	var err error
	if e.DataContentType, err = str("datacontenttype"); err != nil {
		return nil, err
	}
	if e.DataSchema, err = str("dataschema"); err != nil {
		return nil, err
	}
	if e.Subject, err = str("subject"); err != nil {
		return nil, err
	}
	if v, ok := get("time"); ok {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return nil, errors.New(`cloudevents: attribute "time" must be a string`)
		}
		t, err := time.Parse(time.RFC3339Nano, s)
		if err != nil {
			return nil, fmt.Errorf("cloudevents: attribute \"time\" is not RFC3339: %w", err)
		}
		e.Time = &t
	}
	if v, ok := get("data"); ok {
		e.Data = append(json.RawMessage(nil), v...)
	}
	if e.DataBase64, err = str("data_base64"); err != nil {
		return nil, err
	}
	for k, v := range m {
		if !isKnown(k) {
			e.Extensions[k] = append(json.RawMessage(nil), v...)
		}
	}
	return e, e.Validate()
}

// ParseBinary decodes one event from HTTP binary-mode headers plus a body.
func ParseBinary(header http.Header, body []byte) (*Event, error) {
	e := &Event{
		SpecVersion:     header.Get("Ce-Specversion"),
		ID:              header.Get("Ce-Id"),
		Source:          header.Get("Ce-Source"),
		Type:            header.Get("Ce-Type"),
		DataContentType: header.Get("Ce-Datacontenttype"),
		DataSchema:      header.Get("Ce-Dataschema"),
		Subject:         header.Get("Ce-Subject"),
		Extensions:      make(map[string]json.RawMessage),
	}
	if t := header.Get("Ce-Time"); t != "" {
		parsed, err := time.Parse(time.RFC3339Nano, t)
		if err != nil {
			return nil, fmt.Errorf("cloudevents: header ce-time is not RFC3339: %w", err)
		}
		e.Time = &parsed
	}
	for name, values := range header {
		lower := strings.ToLower(name)
		if !strings.HasPrefix(lower, "ce-") || len(values) == 0 {
			continue
		}
		attr := strings.TrimPrefix(lower, "ce-")
		if isKnown(attr) {
			continue
		}
		v, err := json.Marshal(values[0])
		if err != nil {
			return nil, err
		}
		e.Extensions[attr] = v
	}
	if len(body) > 0 {
		e.Data = append(json.RawMessage(nil), body...)
	}
	return e, e.Validate()
}

// Read parses an HTTP request carrying one event (binary or structured mode) or
// a batch of events (batch mode). maxBytes bounds the request body.
func Read(r *http.Request, maxBytes int64) ([]*Event, error) {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxBytes
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("cloudevents: reading body: %w", err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("cloudevents: body exceeds %d bytes", maxBytes)
	}
	ct := strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])

	switch {
	case r.Header.Get("Ce-Specversion") != "":
		e, err := ParseBinary(r.Header, body)
		if err != nil {
			return nil, err
		}
		return []*Event{e}, nil
	case strings.EqualFold(ct, ContentTypeBatch):
		var raws []json.RawMessage
		if err := json.Unmarshal(body, &raws); err != nil {
			return nil, fmt.Errorf("cloudevents: invalid batch document: %w", err)
		}
		if len(raws) == 0 {
			return nil, errors.New("cloudevents: empty batch")
		}
		events := make([]*Event, 0, len(raws))
		for i, raw := range raws {
			e, err := Parse(raw)
			if err != nil {
				return nil, fmt.Errorf("cloudevents: batch element %d: %w", i, err)
			}
			events = append(events, e)
		}
		return events, nil
	case strings.EqualFold(ct, ContentTypeStructured), strings.EqualFold(ct, "application/json"):
		e, err := Parse(body)
		if err != nil {
			return nil, err
		}
		return []*Event{e}, nil
	default:
		return nil, fmt.Errorf("cloudevents: unsupported content type %q", ct)
	}
}

// Validate checks the required attributes and the spec version.
func (e *Event) Validate() error {
	if e.SpecVersion == "" || e.ID == "" || e.Source == "" || e.Type == "" {
		return errors.New("cloudevents: id, source, specversion and type are required")
	}
	if e.SpecVersion != SpecV1 {
		return fmt.Errorf("cloudevents: unsupported specversion %q", e.SpecVersion)
	}
	if strings.ContainsAny(e.ID+e.Source+e.Type, "\r\n") {
		return errors.New("cloudevents: id, source and type must not contain control characters")
	}
	return nil
}

// IsJSON reports whether the event data is JSON according to datacontenttype.
func (e *Event) IsJSON() bool {
	ct := strings.ToLower(strings.TrimSpace(strings.Split(e.DataContentType, ";")[0]))
	return ct == "application/json" || strings.HasSuffix(ct, "+json")
}

// HasData reports whether the event carries a data member.
func (e *Event) HasData() bool { return len(e.Data) > 0 || e.DataBase64 != "" }

// JSON decodes the data member as a generic JSON value.
func (e *Event) JSON() (map[string]any, error) {
	if !e.IsJSON() {
		return nil, errors.New("cloudevents: data is not JSON")
	}
	if len(e.Data) == 0 {
		return nil, errors.New("cloudevents: no data member")
	}
	var v map[string]any
	if err := json.Unmarshal(e.Data, &v); err != nil {
		return nil, fmt.Errorf("cloudevents: data is not a JSON object: %w", err)
	}
	return v, nil
}

// Extension returns a string extension attribute value, if present. String
// values are returned as they are; numbers and booleans are returned as their
// JSON text. CloudEvents requires extension values to be strings, but JSON
// serialisations of real publishers often carry unquoted numbers, and a filter
// such as attributes["shard"] == "0" should still work.
func (e *Event) Extension(name string) (string, bool) {
	v, ok := e.Extensions[strings.ToLower(name)]
	if !ok {
		return "", false
	}
	var s string
	if err := json.Unmarshal(v, &s); err == nil {
		return s, true
	}
	// Scalars (numbers, booleans) keep their JSON text; objects and arrays
	// have no string form and are not filterable attributes.
	if len(v) > 0 && !bytes.HasPrefix(v, []byte{'{'}) && !bytes.HasPrefix(v, []byte{'['}) &&
		!bytes.Equal(v, []byte("null")) {
		return string(v), true
	}
	return "", false
}

// ExtensionsMap returns every extension attribute as a flat string map, the
// form the CEL attribute filters evaluate against. Object and array values are
// left out because CEL attributes are strings.
func (e *Event) ExtensionsMap() map[string]string {
	if len(e.Extensions) == 0 {
		return nil
	}
	out := make(map[string]string, len(e.Extensions))
	for name := range e.Extensions {
		if v, ok := e.Extension(name); ok {
			out[name] = v
		}
	}
	return out
}

// MarshalJSON re-serialises the event in structured mode.
func (e Event) MarshalJSON() ([]byte, error) {
	m := make(map[string]json.RawMessage, 8+len(e.Extensions))
	put := func(k, v string) { m[k] = json.RawMessage(v) }
	b, _ := json.Marshal(e.SpecVersion)
	put("specversion", string(b))
	b, _ = json.Marshal(e.ID)
	put("id", string(b))
	b, _ = json.Marshal(e.Source)
	put("source", string(b))
	b, _ = json.Marshal(e.Type)
	put("type", string(b))
	if e.DataContentType != "" {
		b, _ = json.Marshal(e.DataContentType)
		put("datacontenttype", string(b))
	}
	if e.DataSchema != "" {
		b, _ = json.Marshal(e.DataSchema)
		put("dataschema", string(b))
	}
	if e.Subject != "" {
		b, _ = json.Marshal(e.Subject)
		put("subject", string(b))
	}
	if e.Time != nil {
		b, _ := json.Marshal(e.Time.UTC().Format(time.RFC3339Nano))
		put("time", string(b))
	}
	if len(e.Data) > 0 {
		m["data"] = e.Data
	}
	if e.DataBase64 != "" {
		b, _ := json.Marshal(e.DataBase64)
		put("data_base64", string(b))
	}
	for k, v := range e.Extensions {
		m[k] = v
	}
	return json.Marshal(m)
}

// SetHeaders writes the event into HTTP headers in binary mode.
func (e *Event) SetHeaders(h http.Header) {
	h.Set("Ce-Specversion", e.SpecVersion)
	h.Set("Ce-Id", e.ID)
	h.Set("Ce-Source", e.Source)
	h.Set("Ce-Type", e.Type)
	if e.DataContentType != "" {
		h.Set("Ce-Datacontenttype", e.DataContentType)
	}
	if e.DataSchema != "" {
		h.Set("Ce-Dataschema", e.DataSchema)
	}
	if e.Subject != "" {
		h.Set("Ce-Subject", e.Subject)
	}
	if e.Time != nil {
		h.Set("Ce-Time", e.Time.UTC().Format(time.RFC3339Nano))
	}
	for k, v := range e.Extensions {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			h.Set("Ce-"+strings.ToUpper(k), s)
		}
	}
}
