package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"pubsub/internal/auth"
	"pubsub/internal/cloudevents"
	"pubsub/internal/registry"
)

const (
	schemaTestSecret = "schema-test-secret"
	schemaTestOwner  = "schema-owner"
	schemaTestType   = "schema.order"
)

const orderSchema = `{
  "type": "object",
  "required": ["total"],
  "properties": {"total": {"type": "number"}, "note": {"type": "string"}}
}`

// sourceWithSchema is a registry holding one event type with a schema, which
// is the only shape the gate needs.
type sourceWithSchema struct {
	schema json.RawMessage
}

func (s sourceWithSchema) EventTypes(context.Context) ([]registry.EventType, error) {
	return []registry.EventType{{
		Owner:          schemaTestOwner,
		Name:           schemaTestType,
		RetentionClass: "short",
		ContentType:    "application/json",
		DataSchema:     s.schema,
	}}, nil
}

func (s sourceWithSchema) Subscriptions(context.Context) ([]registry.Subscription, error) {
	return nil, nil
}

// gateAPI builds the smallest publish API that can answer a request: the one
// event type, a validator, and a producer that records whether it was called.
func gateAPI(t *testing.T, mode schemaMode, schema string) (*publishAPI, *fakePlain, string) {
	t.Helper()

	v, err := auth.NewHMAC(schemaTestSecret, "dev", "pubsub")
	if err != nil {
		t.Fatalf("building the validator: %v", err)
	}
	tok, err := auth.Mint(schemaTestSecret, "dev", "pubsub", schemaTestOwner,
		[]string{auth.ScopePublish}, time.Hour)
	if err != nil {
		t.Fatalf("minting a token: %v", err)
	}

	reg := registry.New(sourceWithSchema{schema: json.RawMessage(schema)}, time.Minute)
	if err := reg.Reload(context.Background()); err != nil {
		t.Fatalf("loading the fake registry: %v", err)
	}

	plain := &fakePlain{failAt: -1}
	return &publishAPI{
		auth:       v,
		registry:   reg,
		kafka:      plain,
		maxBytes:   cloudevents.DefaultMaxBytes,
		schemaMode: mode,
	}, plain, tok
}

func publish(t *testing.T, a *publishAPI, tok, data string) *httptest.ResponseRecorder {
	t.Helper()
	body := `{"specversion":"1.0","id":"1","source":"/t","type":"` + schemaTestType +
		`","data":` + data + `}`
	req := httptest.NewRequest("POST", "/v1/events", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/cloudevents+json")
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	a.handleEvents(rec, req)
	return rec
}

func TestParseSchemaMode(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    schemaMode
		wantErr bool
	}{
		{"", schemaOff, false},
		{"off", schemaOff, false},
		{"  OFF ", schemaOff, false},
		{"warn", schemaWarn, false},
		{"enforce", schemaEnforce, false},
		// A typo must not silently become "off": the person typing it has
		// asked for a gate and would believe they have one.
		{"enabled", schemaOff, true},
		{"true", schemaOff, true},
		{"strict", schemaOff, true},
	} {
		got, err := parseSchemaMode(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseSchemaMode(%q) = %v, want an error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSchemaMode(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseSchemaMode(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// The cache is keyed by content, so the same document compiles once, two
// event types sharing a schema share the result, and a failure is remembered
// rather than recompiled on every publish.
func TestSchemaCacheKeyedByContent(t *testing.T) {
	var c schemaCache

	same := json.RawMessage(orderSchema)
	other := json.RawMessage(`{"type":"object"}`)
	bad := json.RawMessage(`{"type":"object","properties":`)

	first, err := c.get(same)
	if err != nil {
		t.Fatalf("compiling: %v", err)
	}
	again, err := c.get(json.RawMessage(orderSchema))
	if err != nil {
		t.Fatalf("recompiling: %v", err)
	}
	if first != again {
		t.Error("the same document compiled twice: the cache did not reuse it")
	}

	shared, err := c.get(same)
	if err != nil || shared != first {
		t.Error("an identical schema from another event type did not share the entry")
	}

	if _, err := c.get(other); err != nil {
		t.Fatalf("a different document failed to compile: %v", err)
	}

	// A broken document must be remembered as broken. Recompiling it on every
	// publish would turn one bad row into a per-request cost.
	if _, err := c.get(bad); err == nil {
		t.Fatal("a truncated document compiled")
	}
	if _, err := c.get(bad); err == nil {
		t.Fatal("the second attempt did not return the cached failure")
	}
	if len(c.m) != 3 {
		t.Errorf("cache holds %d entries, want 3 (two valid, one broken)", len(c.m))
	}
}

// The gate itself, at the HTTP level. The three modes have to differ in exactly
// the way they promise - and warn has to still publish, which is the part that
// is easy to get wrong: logging a violation and then skipping the event would
// make warn a silent filter rather than an advisory.
func TestSchemaGateModes(t *testing.T) {
	bad := `{"total":"not-a-number"}`
	good := `{"total": 99.9}`

	for _, tc := range []struct {
		name        string
		mode        schemaMode
		data        string
		wantStatus  int
		wantProduce bool
	}{
		{"off accepts a violation", schemaOff, bad, 201, true},
		{"warn accepts a violation", schemaWarn, bad, 201, true},
		{"enforce rejects a violation", schemaEnforce, bad, 422, false},
		{"off accepts valid data", schemaOff, good, 201, true},
		{"warn accepts valid data", schemaWarn, good, 201, true},
		{"enforce accepts valid data", schemaEnforce, good, 201, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, plain, tok := gateAPI(t, tc.mode, orderSchema)
			rec := publish(t, a, tok, tc.data)

			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if produced := plain.calls > 0; produced != tc.wantProduce {
				t.Errorf("produced = %v (calls=%d), want %v",
					produced, plain.calls, tc.wantProduce)
			}
			if tc.wantStatus == 422 && !strings.Contains(rec.Body.String(), "/total") {
				t.Errorf("body %q does not name the failing path", rec.Body.String())
			}
		})
	}
}

// A type without a schema is never validated, whatever the mode says - which is
// what makes the feature opt-in per event type rather than per deployment.
func TestTypeWithoutSchemaIsNeverValidated(t *testing.T) {
	for _, mode := range []schemaMode{schemaOff, schemaWarn, schemaEnforce} {
		a, plain, tok := gateAPI(t, mode, "")
		rec := publish(t, a, tok, `{"total":"not-a-number"}`)
		if rec.Code != 201 {
			t.Errorf("mode %v: status = %d, want 201", mode, rec.Code)
		}
		if plain.calls == 0 {
			t.Errorf("mode %v: the event was not produced", mode)
		}
	}
}

// The violations belong in the response, not only in the log: the publisher is
// the one who can fix them, and asking them to read the platform's logs to
// discover their own data was wrong is not much of an advisory.
func TestWarnModeReportsViolationsInTheResponse(t *testing.T) {
	// Two independent problems, so a response carrying only the first would
	// start the fix-republish-discover loop the plural form exists to avoid.
	a, _, tok := gateAPI(t, schemaWarn, orderSchema)
	rec := publish(t, a, tok, `{"total":"free","note":123}`)

	if rec.Code != 201 {
		t.Fatalf("status = %d, want 201 (warn accepts): %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"schema_violations", "/total", "/note"} {
		if !contains(body, want) {
			t.Errorf("response does not mention %q: %s", want, body)
		}
	}
}

// A clean publish must be byte-identical to what it was before the gate: the
// field is omitempty for the same reason the existing warning is.
func TestCleanPublishHasNoViolationsField(t *testing.T) {
	for _, mode := range []schemaMode{schemaOff, schemaWarn, schemaEnforce} {
		a, _, tok := gateAPI(t, mode, orderSchema)
		rec := publish(t, a, tok, `{"order_id":"o-1","total":9.9}`)
		if rec.Code != 201 {
			t.Errorf("mode %v: status = %d, want 201", mode, rec.Code)
		}
		if contains(rec.Body.String(), "schema_violations") {
			t.Errorf("mode %v: a clean publish carries violations: %s",
				mode, rec.Body.String())
		}
	}
}

// Enforce must report every violation in the 422, not just the first - the
// publisher fixes them in one pass rather than one per round trip.
func TestEnforceReportsEveryViolation(t *testing.T) {
	a, _, tok := gateAPI(t, schemaEnforce, orderSchema)
	rec := publish(t, a, tok, `{"total":"free","note":123}`)

	if rec.Code != 422 {
		t.Fatalf("status = %d, want 422: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{"/total", "/note"} {
		if !contains(body, want) {
			t.Errorf("422 body does not mention %q: %s", want, body)
		}
	}
}

func contains(haystack, needle string) bool {
	return strings.Contains(haystack, needle)
}
