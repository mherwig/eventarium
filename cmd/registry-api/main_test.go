package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"pubsub/internal/auth"
	"pubsub/internal/registry"
	"pubsub/internal/store"
	"pubsub/migrations"
)

// defaultTestDSN is the local compose database; the tests run against it when
// TEST_PG_DSN is unset and skip when nothing answers (same convention as
// internal/registry and internal/dispatch).
const defaultTestDSN = "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable"

const (
	testSecret   = "put-test-secret"
	testOwner    = "put-test"
	foreignOwner = "put-other"
)

// testAPI is the registry API in front of PostgreSQL plus a token per owner.
type testAPI struct {
	srv    *httptest.Server
	tokens map[string]string
}

func newTestAPI(t *testing.T) *testAPI {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		dsn = defaultTestDSN
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := store.Open(ctx, dsn)
	if err != nil {
		t.Skipf("no PostgreSQL at %q (set TEST_PG_DSN): %v", dsn, err)
	}
	if err := migrations.Apply(ctx, st.Pool); err != nil {
		st.Close()
		t.Fatalf("applying migrations: %v", err)
	}
	// Rows an aborted earlier run left behind must not satisfy these tests.
	cleanupTestRows(t, st)
	t.Cleanup(func() {
		cleanupTestRows(t, st)
		st.Close()
	})
	v, err := auth.NewHMAC(testSecret, "dev", "pubsub")
	if err != nil {
		t.Fatalf("building the validator: %v", err)
	}
	srv := httptest.NewServer(newMux(&api{auth: v, store: st}))
	t.Cleanup(srv.Close)

	tokens := map[string]string{}
	for _, owner := range []string{testOwner, foreignOwner} {
		tok, err := auth.Mint(testSecret, "dev", "pubsub", owner,
			[]string{auth.ScopePublish, auth.ScopeSubscribe}, time.Hour)
		if err != nil {
			t.Fatalf("minting a token for %s: %v", owner, err)
		}
		tokens[owner] = tok
	}
	return &testAPI{srv: srv, tokens: tokens}
}

func cleanupTestRows(t *testing.T, st *store.Store) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := st.Pool.Exec(ctx,
		`DELETE FROM subscriptions WHERE owner = $1 OR owner = $2`, testOwner, foreignOwner); err != nil {
		t.Errorf("cleaning up subscriptions: %v", err)
	}
	if _, err := st.Pool.Exec(ctx,
		`DELETE FROM event_types WHERE owner = $1 OR owner = $2`, testOwner, foreignOwner); err != nil {
		t.Errorf("cleaning up event types: %v", err)
	}
}

// do performs one request as owner and returns status and body.
func (a *testAPI) do(t *testing.T, method, path, owner string, body any) (int, []byte) {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshalling %T: %v", body, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, a.srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+a.tokens[owner])
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("reading the response of %s %s: %v", method, path, err)
	}
	return res.StatusCode, out
}

// wantStatus fails with the response body, which is where the reason is.
func wantStatus(t *testing.T, code, want int, body []byte) {
	t.Helper()
	if code != want {
		t.Fatalf("status = %d, want %d; body: %s", code, want, body)
	}
}

func wantContains(t *testing.T, body []byte, sub string) {
	t.Helper()
	if !strings.Contains(string(body), sub) {
		t.Errorf("body does not mention %q: %s", sub, body)
	}
}

func (a *testAPI) createEventType(t *testing.T, name string) {
	t.Helper()
	code, body := a.do(t, http.MethodPost, "/v1/event-types", testOwner, map[string]any{
		"name":            name,
		"retention_class": "short",
		"content_type":    "application/json",
	})
	wantStatus(t, code, http.StatusCreated, body)
}

func (a *testAPI) createSubscription(t *testing.T, body map[string]any) registry.Subscription {
	t.Helper()
	code, out := a.do(t, http.MethodPost, "/v1/subscriptions", testOwner, body)
	wantStatus(t, code, http.StatusCreated, out)
	var sub registry.Subscription
	if err := json.Unmarshal(out, &sub); err != nil {
		t.Fatalf("decoding the created subscription: %v", err)
	}
	return sub
}

// get re-reads a subscription as the caller that owns it.
func (a *testAPI) get(t *testing.T, id string) registry.Subscription {
	t.Helper()
	code, body := a.do(t, http.MethodGet, "/v1/subscriptions/"+id, testOwner, nil)
	wantStatus(t, code, http.StatusOK, body)
	var sub registry.Subscription
	if err := json.Unmarshal(body, &sub); err != nil {
		t.Fatalf("decoding the subscription: %v", err)
	}
	return sub
}

// TestSubscriptionUpdateReplacesTheRow is PUT as full replace: everything the
// body carries is written, everything it omits falls back to the section 7
// defaults, and id and owner survive. Switching the channel is the case the
// delivery stages have to follow (see registry.ChannelFor), so the row is
// read back through GET as well - the read-modify-write loop a client does
// has to be lossless.
func TestSubscriptionUpdateReplacesTheRow(t *testing.T) {
	a := newTestAPI(t)
	a.createEventType(t, "put.one")
	a.createEventType(t, "put.two")

	created := a.createSubscription(t, map[string]any{
		"event_type":       "put.one",
		"channel":          "webhook",
		"callback_url":     "https://example.test/hook",
		"filter":           `attributes["shard"] == "0"`,
		"data_filter":      `int(data.seq) % 2 == 0`,
		"projection":       []string{"shard", "seq"},
		"max_connections":  9,
		"rate_limit_per_s": 7,
	})
	if created.Channel != "webhook" || created.MaxConnections != 9 {
		t.Fatalf("create returned %+v, want a webhook subscription with 9 connections", created)
	}

	code, body := a.do(t, http.MethodPut, "/v1/subscriptions/"+created.ID, testOwner, map[string]any{
		"event_type":    "put.two",
		"channel":       "sse",
		"filter":        `attributes["shard"] == "4"`,
		"max_in_flight": 2,
	})
	wantStatus(t, code, http.StatusOK, body)
	var updated registry.Subscription
	if err := json.Unmarshal(body, &updated); err != nil {
		t.Fatalf("decoding the response: %v", err)
	}

	// The identity is not mutable.
	if updated.ID != created.ID {
		t.Errorf("id = %s, want %s", updated.ID, created.ID)
	}
	if updated.Owner != testOwner {
		t.Errorf("owner = %q, want %q", updated.Owner, testOwner)
	}
	// What the body carried is new.
	if updated.EventType != "put.two" {
		t.Errorf("event_type = %q, want %q", updated.EventType, "put.two")
	}
	if updated.Channel != "sse" {
		t.Errorf("channel = %q, want %q", updated.Channel, "sse")
	}
	if updated.CallbackURL != "" {
		t.Errorf("callback_url = %q, want it cleared for the sse channel", updated.CallbackURL)
	}
	if updated.Filter != `attributes["shard"] == "4"` {
		t.Errorf("filter = %q, want the new filter", updated.Filter)
	}
	if updated.MaxInFlight != 2 {
		t.Errorf("max_in_flight = %d, want 2", updated.MaxInFlight)
	}
	// Absent policy fields are back at the defaults of the section 7 table.
	if updated.MaxConnections != 3 {
		t.Errorf("max_connections = %d, want the default 3", updated.MaxConnections)
	}
	if updated.RateLimitPerS != 0 {
		t.Errorf("rate_limit_per_s = %d, want the default 0", updated.RateLimitPerS)
	}
	// Absent filters and projection are cleared, not inherited.
	if updated.DataFilter != "" {
		t.Errorf("data_filter = %q, want it cleared", updated.DataFilter)
	}
	if len(updated.Projection) != 0 {
		t.Errorf("projection = %v, want it cleared", updated.Projection)
	}

	// GET returns the same subscription, so read-modify-write is lossless.
	stored := a.get(t, created.ID)
	for _, f := range []struct{ name, got, want string }{
		{"event_type", stored.EventType, updated.EventType},
		{"channel", stored.Channel, updated.Channel},
		{"callback_url", stored.CallbackURL, updated.CallbackURL},
		{"filter", stored.Filter, updated.Filter},
		{"data_filter", stored.DataFilter, updated.DataFilter},
	} {
		if f.got != f.want {
			t.Errorf("GET %s = %q, want %q", f.name, f.got, f.want)
		}
	}
	if stored.MaxConnections != updated.MaxConnections || stored.MaxInFlight != updated.MaxInFlight {
		t.Errorf("GET policy = %d/%d, want %d/%d",
			stored.MaxConnections, stored.MaxInFlight,
			updated.MaxConnections, updated.MaxInFlight)
	}

	// And a second PUT that clears the filter keeps its word.
	code, body = a.do(t, http.MethodPut, "/v1/subscriptions/"+created.ID, testOwner, map[string]any{
		"event_type": "put.two",
		"channel":    "sse",
	})
	wantStatus(t, code, http.StatusOK, body)
	if got := a.get(t, created.ID); got.Filter != "" {
		t.Errorf("filter = %q after the second PUT, want it cleared", got.Filter)
	}
}

// TestSubscriptionUpdateValidatesLikeCreate runs the whole validation block
// against PUT - it is the code path shared with POST - and proves that a
// rejected update leaves the stored subscription alone.
func TestSubscriptionUpdateValidatesLikeCreate(t *testing.T) {
	a := newTestAPI(t)
	a.createEventType(t, "put.val")

	const hook = "https://example.test/hook"
	original := a.createSubscription(t, map[string]any{
		"event_type":      "put.val",
		"channel":         "webhook",
		"callback_url":    hook,
		"filter":          `attributes["shard"] == "0"`,
		"max_connections": 9,
	})
	base := func(over map[string]any) map[string]any {
		body := map[string]any{
			"event_type":   "put.val",
			"channel":      "webhook",
			"callback_url": hook,
		}
		for k, v := range over {
			body[k] = v
		}
		return body
	}

	tests := []struct {
		name     string
		body     map[string]any
		contains string
	}{
		{"webhook without callback_url", map[string]any{"event_type": "put.val", "channel": "webhook"}, "callback_url"},
		{"sse with a callback_url", base(map[string]any{"channel": "sse", "callback_url": hook}), "callback_url"},
		{"unknown event type", base(map[string]any{"event_type": "put.none"}), "unknown event type"},
		{"invalid filter", base(map[string]any{"filter": `attributes["shard" == "0"`}), "invalid filter"},
		{"invalid data_filter", base(map[string]any{"data_filter": `int(data.`}), "invalid data_filter"},
		{"projection on a non-path", base(map[string]any{"projection": []string{"order..id"}}), "invalid projection"},
		{"bad delivery_mode", base(map[string]any{"delivery_mode": "fast"}), "delivery_mode"},
		{"bad webhook_format", base(map[string]any{"webhook_format": "xml"}), "webhook_format"},
		{"channel that is neither", base(map[string]any{"channel": "email"}), "channel must be"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, body := a.do(t, http.MethodPut, "/v1/subscriptions/"+original.ID, testOwner, tt.body)
			wantStatus(t, code, http.StatusUnprocessableEntity, body)
			wantContains(t, body, tt.contains)

			// The failed update must not have touched the row.
			stored := a.get(t, original.ID)
			if stored.Channel != "webhook" || stored.CallbackURL != hook ||
				stored.Filter != original.Filter || stored.MaxConnections != 9 {
				t.Errorf("row changed after a rejected PUT: %+v", stored)
			}
		})
	}
}

// TestSubscriptionUpdateNotFound pins the ownership rule of section 6.7: an
// unknown ID, a malformed ID and a foreign subscription all answer 404, and
// none of them is validated first - a validation error would confirm that the
// ID exists.
func TestSubscriptionUpdateNotFound(t *testing.T) {
	a := newTestAPI(t)
	a.createEventType(t, "put.404")
	original := a.createSubscription(t, map[string]any{
		"event_type":   "put.404",
		"channel":      "webhook",
		"callback_url": "https://example.test/hook",
	})

	put := func(t *testing.T, id, owner string) (int, []byte) {
		t.Helper()
		// The body is invalid on purpose: if the handler validated before it
		// looked the subscription up, it would answer 422 and thereby confirm
		// that the ID exists.
		code, body := a.do(t, http.MethodPut, "/v1/subscriptions/"+id, owner, map[string]any{
			"event_type": "put.404",
			"channel":    "email",
		})
		return code, body
	}

	for _, tt := range []struct{ name, id, owner string }{
		{"unknown id", "11111111-1111-4111-8111-111111111111", testOwner},
		{"malformed id", "not-a-uuid", testOwner},
		{"foreign id", original.ID, foreignOwner},
	} {
		t.Run(tt.name, func(t *testing.T) {
			code, body := put(t, tt.id, tt.owner)
			if code != http.StatusNotFound {
				t.Errorf("status = %d, want 404; body: %s", code, body)
			}
		})
	}
	// And the row is still the original one.
	stored := a.get(t, original.ID)
	if stored.Channel != "webhook" || stored.CallbackURL != "https://example.test/hook" {
		t.Errorf("row changed: %+v", stored)
	}
}
