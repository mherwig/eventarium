package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"pubsub/internal/cloudevents"
)

// maxPropagatedHeaderBytes bounds one allow-listed header value.
//
// The bound exists because of batch mode: one request carries every event in
// it, so an unbounded header is multiplied by the batch size before it ever
// reaches Kafka. It is checked against the allow-list only - a client sending
// an oversized header that nobody asked to propagate is unaffected.
//
// An over-long value rejects the request rather than being dropped. Truncating
// would hand the consumer a correlation id that looks valid and is not, which
// is worse than not having one; dropping it would lose the correlation
// silently, at publish time, where the publisher can still see the failure.
const maxPropagatedHeaderBytes = 1024

// parseHeaderAllowList reads PROPAGATE_HEADERS: a comma-separated list of
// inbound HTTP header names to copy onto every event as an extension.
//
// Names are lower-cased because CloudEvents extension names are lower-case by
// specification and HTTP header names are case-insensitive, so the two agree
// on one spelling. Duplicates collapse. An unset or blank value returns nil,
// which is what makes the feature off by default: nothing is propagated unless
// a deployment names it.
func parseHeaderAllowList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	seen := make(map[string]struct{})
	var out []string
	for _, part := range strings.Split(raw, ",") {
		name := strings.ToLower(strings.TrimSpace(part))
		if name == "" {
			continue
		}
		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}
		out = append(out, name)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// applyPropagatedHeaders copies the allow-listed headers of one inbound
// request onto every event in it as a CloudEvent extension.
//
// This is the whole feature. Everything downstream already handles extensions:
// the router puts them in the filter attribute map (attributes["x-..."]), the
// dispatcher writes them onto the callback as Ce-X-... headers, and the SSE
// gateway carries them inside the data member - so mapping a plain header onto
// an extension at publish time is the only work there is.
//
// An extension the event already carries wins. The header is a convenience for
// context the publisher's own code never puts in the body (a gateway's request
// id, a tracing id injected by an agent), not an override of an attribute the
// publisher set deliberately.
//
// It applies to every event of a batch, because they share one HTTP request
// and therefore one set of headers.
func applyPropagatedHeaders(hdr http.Header, events []*cloudevents.Event, allow []string) error {
	if len(allow) == 0 {
		return nil
	}
	var forwarded, overridden []string
	for _, e := range events {
		for _, name := range allow {
			v := hdr.Get(name)
			if v == "" {
				continue
			}
			if len(v) > maxPropagatedHeaderBytes {
				return fmt.Errorf(
					"header %q is %d bytes; at most %d are propagated (PROPAGATE_HEADERS)",
					name, len(v), maxPropagatedHeaderBytes)
			}
			key := strings.ToLower(name)
			if _, exists := e.Extensions[key]; exists {
				overridden = append(overridden, name)
				continue
			}
			if e.Extensions == nil {
				e.Extensions = make(map[string]json.RawMessage, 1)
			}
			// Extensions are raw JSON: a bare string has to be quoted, which
			// is what the dispatcher's SetHeaders expects when it unmarshals
			// the value back into a header string.
			enc, err := json.Marshal(v)
			if err != nil {
				// Marshalling a string cannot fail; treating it as a missing
				// header would drop the correlation id silently.
				return fmt.Errorf("encoding header %q: %w", name, err)
			}
			e.Extensions[key] = enc
			forwarded = append(forwarded, name)
		}
	}
	// Logged once per request, not once per event: a batch of a hundred would
	// otherwise print a hundred identical lines. Both halves matter when
	// someone asks why the consumer does not see a header - "forwarded" proves
	// the allow-list worked, "overridden" is the one case where it worked and
	// the value still did not arrive.
	if len(forwarded) > 0 || len(overridden) > 0 {
		slog.Debug("publish-api: propagated headers",
			"forwarded", strings.Join(forwarded, ","),
			"overridden_by_event_extension", strings.Join(overridden, ","))
	}
	return nil
}
