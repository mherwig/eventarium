package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"pubsub/internal/auth"
	"pubsub/internal/delivery"
	"pubsub/internal/registry"
)

// seededDataFilter is the CEL data filter a third of the seeded subscriptions
// get. The data member decodes through encoding/json, so numbers are doubles
// and CEL's '%' has no double overload; int() converts first (harness_test.go
// pins this down).
const seededDataFilter = "int(data.seq) % 2 == 0"

// runSeed implements `harness seed`: the subscription generator of section
// 15.2. It creates event types and subscriptions through the HTTP APIs (never
// SQL) with a deterministic mix of channels, delivery modes, filters,
// projections and rate limits, then prints the created subscriptions as JSON
// on stdout so that later steps (verify) can consume them.
func runSeed(ctx context.Context, args []string) int {
	fs := newFlagSet("seed")
	owner := fs.String("owner", envStr("OWNER", "harness"), "owner reference for the minted token (env HARNESS_OWNER)")
	typesN := fs.Int("types", envInt("TYPES", 20), "number of event types to create (env HARNESS_TYPES)")
	subsN := fs.Int("subs", envInt("SUBS", 20), "number of subscriptions to create (env HARNESS_SUBS)")
	prefix := fs.String("prefix", envStr("PREFIX", "harness"), "name prefix for the created event types (env HARNESS_PREFIX)")
	api := fs.String("api", apiDefault(), "base URL of the registry API (env API)")
	webhookURL := fs.String("webhook-url", envStr("WEBHOOK_URL", "http://localhost:9090/hook"), "callback URL for webhook subscriptions (env HARNESS_WEBHOOK_URL)")
	if code, done := parseResult(fs.Parse(args)); done {
		return code
	}
	if *owner == "" {
		fmt.Fprintln(os.Stderr, "harness seed: -owner is required")
		return 2
	}
	if *typesN <= 0 || *subsN <= 0 {
		fmt.Fprintln(os.Stderr, "harness seed: -types and -subs must be positive")
		return 2
	}

	token, err := mintToken(*owner, auth.ScopePublish, auth.ScopeSubscribe)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness seed:", err)
		return 1
	}
	client := newHTTPClient()
	base := strings.TrimRight(*api, "/")

	// Event types: JSON content so that data filters and projections are
	// allowed (section 5); retention class cycled over short and day.
	classes := []string{"short", "day"}
	typeNames := make([]string, 0, *typesN)
	fmt.Fprintf(os.Stderr, "harness seed: creating %d event types\n", *typesN)
	for i := range *typesN {
		name := fmt.Sprintf("%s.type.%d", *prefix, i)
		payload := map[string]any{
			"name":            name,
			"retention_class": classes[i%len(classes)],
			"content_type":    "application/json",
		}
		code, body, err := postJSON(ctx, client, base+"/v1/event-types", token, payload, nil)
		if err != nil {
			fmt.Fprintln(os.Stderr, "harness seed:", err)
			return 1
		}
		switch {
		case code == http.StatusConflict:
			fmt.Fprintf(os.Stderr, "harness seed: event type %s already exists, reusing\n", name)
		case code/100 == 2:
		default:
			fmt.Fprintf(os.Stderr, "harness seed: creating %s: HTTP %d: %s\n", name, code, errorBody(body))
			return 1
		}
		typeNames = append(typeNames, name)
	}

	// Subscriptions: cycle deterministically over channel (webhook/sse),
	// delivery mode (ordered/key_ordered/unordered) and filter kind
	// (attribute CEL / data CEL / projection allowlist). The least common
	// multiple of the cycles is 6, so every combination occurs.
	modes := []string{delivery.ModeOrdered, delivery.ModeKeyOrdered, delivery.ModeUnordered}
	created := make([]registry.Subscription, 0, *subsN)
	fmt.Fprintf(os.Stderr, "harness seed: creating %d subscriptions\n", *subsN)
	for j := range *subsN {
		sub := map[string]any{
			"event_type":    typeNames[j%len(typeNames)],
			"delivery_mode": modes[j%len(modes)],
		}
		channel := delivery.ChannelWebhook
		if j%2 == 1 {
			channel = delivery.ChannelSSE
		}
		sub["channel"] = channel
		if channel == delivery.ChannelWebhook {
			sub["callback_url"] = *webhookURL
			sub["webhook_format"] = "binary"
			// A few subscriptions get a low token-bucket rate limit so
			// that rate-limit overflow, parking and draining (section 7)
			// show up in every run.
			if j%7 == 0 {
				sub["rate_limit_per_s"] = 50
				sub["burst"] = 100
			}
		} else {
			sub["max_connections"] = 2
		}
		switch j % 3 {
		case 0: // CEL attribute filter on about a third
			sub["filter"] = fmt.Sprintf(`attributes["shard"] == "%d"`, j%8)
		case 1: // CEL data filter on a third
			sub["data_filter"] = seededDataFilter
		case 2: // projection allowlist on a third
			sub["projection"] = []string{"shard", "seq", "sent_at"}
		}
		var createdSub registry.Subscription
		code, body, err := postJSON(ctx, client, base+"/v1/subscriptions", token, sub, &createdSub)
		if err != nil {
			fmt.Fprintln(os.Stderr, "harness seed:", err)
			return 1
		}
		if code/100 != 2 {
			fmt.Fprintf(os.Stderr, "harness seed: creating subscription %d: HTTP %d: %s\n", j, code, errorBody(body))
			return 1
		}
		if createdSub.ID == "" {
			fmt.Fprintf(os.Stderr, "harness seed: subscription %d: response carries no id: %s\n", j, errorBody(body))
			return 1
		}
		created = append(created, createdSub)
	}

	// stdout carries the machine-readable result for later steps.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(created); err != nil {
		fmt.Fprintln(os.Stderr, "harness seed:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "harness seed: done: %d event types, %d subscriptions for owner %s\n",
		len(typeNames), len(created), *owner)
	return 0
}
