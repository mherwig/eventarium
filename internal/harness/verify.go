package harness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"pubsub/internal/auth"
	"pubsub/internal/filter"
	"pubsub/internal/registry"
	"pubsub/internal/status"
)

// statusPageLimit bounds the pagination loop over the Status API so that a
// misbehaving endpoint cannot spin forever.
const statusPageLimit = 200

// runWindowSkew pads the publish window on both sides when scoping Status API
// queries to the current run, so that clock skew between publisher and status
// writer cannot push a delivery of this run outside the window.
const runWindowSkew = time.Minute

// runVerify implements `harness verify`, the independent oracle of section
// 15.1: the system never verifies itself. It recomputes the expected delivery
// set per subscription locally — the CEL attribute filter is evaluated with
// internal/filter, never by asking the platform — and compares it with the
// union of what the webhook receiver recorded and what the Status API reports
// as delivered, sent or pending. It exits non-zero when anything is missing or
// unexpected.
func runVerify(ctx context.Context, args []string) int {
	fs := newFlagSet("verify")
	events := fs.String("events", envStr("EVENTS", ""), "file of published events written by pub -out (env HARNESS_EVENTS)")
	received := fs.String("received", envStr("RECEIVED", ""), "file written by recv -out (env HARNESS_RECEIVED)")
	statusAPI := fs.String("status-api", envStr("STATUS_API", "http://localhost:8081"), "base URL of the Status API (env HARNESS_STATUS_API)")
	subFlag := fs.String("subscription", envStr("SUBSCRIPTION", ""), "comma separated subscription ids, default every subscription of the owner (env HARNESS_SUBSCRIPTION)")
	api := fs.String("api", apiDefault(), "base URL of the registry API (env API)")
	owner := fs.String("owner", envStr("OWNER", "harness"), "owner reference for the minted token (env HARNESS_OWNER)")
	if code, done := parseResult(fs.Parse(args)); done {
		return code
	}
	if *events == "" {
		fmt.Fprintln(os.Stderr, "harness verify: -events is required (run pub with -out)")
		return 2
	}
	if *received == "" {
		fmt.Fprintln(os.Stderr, "harness verify: -received is required (run recv with -out)")
		return 2
	}

	pubs, err := loadLines[pubRecord](*events)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness verify:", err)
		return 1
	}
	recvs, err := loadLines[recvRecord](*received)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness verify:", err)
		return 1
	}
	unverifiable := 0
	for _, r := range pubs {
		if r.EventRef == "" {
			unverifiable++
		}
	}
	if unverifiable > 0 {
		fmt.Fprintf(os.Stderr, "harness verify: warning: %d published events carry no event_ref and are not checked\n", unverifiable)
	}

	// The status table is cumulative (48h retention, section 10), so runs
	// before this one would surface as unexpected deliveries. Scope the
	// Status API queries to this run's publish window, padded a little on
	// both sides: the status ts is written when the delivery is attempted,
	// which can precede the publisher's own timestamp by the clock skew.
	var from, to time.Time
	for _, r := range pubs {
		if r.Time.IsZero() {
			continue
		}
		if from.IsZero() || r.Time.Before(from) {
			from = r.Time
		}
		if r.Time.After(to) {
			to = r.Time
		}
	}
	if !from.IsZero() {
		from = from.Add(-runWindowSkew)
		to = to.Add(runWindowSkew)
	}

	token, err := mintToken(*owner, auth.ScopeSubscribe, auth.ScopeStatusRead)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness verify:", err)
		return 1
	}
	client := newHTTPClient()
	subs, err := loadSubs(ctx, client, strings.TrimRight(*api, "/"), token, *subFlag)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness verify:", err)
		return 1
	}
	if len(subs) == 0 {
		fmt.Fprintln(os.Stderr, "harness verify: no subscriptions to verify")
		return 1
	}

	// What the receiver recorded, per subscription: event ref -> count.
	recvBySub := map[string]map[string]int{}
	for _, r := range recvs {
		if r.SubscriptionID == "" || r.EventRef == "" {
			continue
		}
		m := recvBySub[r.SubscriptionID]
		if m == nil {
			m = map[string]int{}
			recvBySub[r.SubscriptionID] = m
		}
		m[r.EventRef]++
	}

	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "SUBSCRIPTION\tCHANNEL\tMODE\tEXPECTED\tRECEIVED\tMISSING\tUNEXPECTED\tDUPLICATES\tSTATUS")
	var totExp, totRec, totMiss, totUnexp, totDup int
	failed := false
	for i := range subs {
		sub := &subs[i]
		expected, evalErrs, expErr := expectedSet(sub, pubs)
		if expErr != nil {
			fmt.Fprintf(os.Stderr, "harness verify: subscription %s: %v\n", sub.ID, expErr)
			failed = true
			fmt.Fprintf(w, "%s\t%s\t%s\tERROR\t0\t0\t0\t0\t?\n", sub.ID, sub.Channel, sub.DeliveryMode)
			continue
		}
		if evalErrs > 0 {
			fmt.Fprintf(os.Stderr, "harness verify: subscription %s: filter evaluation failed for %d events\n", sub.ID, evalErrs)
			failed = true
		}

		recvCounts := recvBySub[sub.ID]
		statusRefs, statusCounts, statusErr := fetchStatus(ctx, client, strings.TrimRight(*statusAPI, "/"), token, sub.ID, from, to)
		if statusErr != nil {
			fmt.Fprintf(os.Stderr, "harness verify: subscription %s: status API: %v (verdict from receiver data only)\n", sub.ID, statusErr)
		}

		// The union of what the receiver recorded and what the Status API
		// reports as delivered, sent or pending is "accounted for".
		missing := map[string]bool{}
		for ref := range expected {
			if _, got := recvCounts[ref]; got {
				continue
			}
			switch statusRefs[ref] {
			case status.Delivered, status.Sent, status.Pending:
			default:
				missing[ref] = true
			}
		}
		unexpected := map[string]bool{}
		for ref := range recvCounts {
			if !expected[ref] {
				unexpected[ref] = true
			}
		}
		for ref, st := range statusRefs {
			switch st {
			case status.Delivered, status.Sent, status.Pending:
				if !expected[ref] {
					unexpected[ref] = true
				}
			}
		}
		dup := 0
		for _, n := range recvCounts {
			if n > 1 {
				dup += n - 1
			}
		}

		if len(missing) > 0 || len(unexpected) > 0 {
			failed = true
			sampleSet("missing", sub.ID, missing)
			sampleSet("unexpected", sub.ID, unexpected)
		}
		totExp += len(expected)
		totRec += len(recvCounts)
		totMiss += len(missing)
		totUnexp += len(unexpected)
		totDup += dup
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%s\n",
			sub.ID, sub.Channel, sub.DeliveryMode,
			len(expected), len(recvCounts), len(missing), len(unexpected), dup,
			formatStatus(statusCounts, statusRefs, statusErr != nil))
	}
	_ = w.Flush()

	fmt.Printf("harness verify: subscriptions=%d expected=%d received=%d missing=%d unexpected=%d duplicates=%d\n",
		len(subs), totExp, totRec, totMiss, totUnexp, totDup)
	if failed {
		fmt.Println("harness verify: FAIL")
		return 1
	}
	fmt.Println("harness verify: OK")
	return 0
}

// expectedSet recomputes locally which of the published events must reach the
// subscription: same event type, attribute filter holds, data filter holds
// (sections 5 and 6.2). The filters are evaluated by internal/filter, the same
// library the platform uses — but fed only from the publisher's own records,
// never from anything the platform reports back.
func expectedSet(sub *registry.Subscription, pubs []pubRecord) (map[string]bool, int, error) {
	var attrF, dataF *filter.Compiled
	if sub.Filter != "" {
		c, err := filter.CompileAttribute(sub.Filter)
		if err != nil {
			return nil, 0, fmt.Errorf("compiling attribute filter %q: %w", sub.Filter, err)
		}
		attrF = c
	}
	if sub.DataFilter != "" {
		c, err := filter.CompileData(sub.DataFilter)
		if err != nil {
			return nil, 0, fmt.Errorf("compiling data filter %q: %w", sub.DataFilter, err)
		}
		dataF = c
	}
	exp := make(map[string]bool)
	evalErrs := 0
	for _, r := range pubs {
		if r.EventRef == "" || r.Type != sub.EventType {
			continue
		}
		attrs := map[string]string{
			"id":      r.ID,
			"source":  r.Source,
			"type":    r.Type,
			"subject": r.Subject,
		}
		for k, v := range r.Extensions {
			attrs[strings.ToLower(k)] = v
		}
		ok, err := filter.Matches(attrF, dataF, attrs, r.Data)
		if err != nil {
			evalErrs++
			continue
		}
		if ok {
			exp[r.EventRef] = true
		}
	}
	return exp, evalErrs, nil
}

// loadSubs reads the subscription definitions from the registry API: either
// the explicit list from -subscription, or every subscription of the owner.
func loadSubs(ctx context.Context, client *http.Client, base, token, subFlag string) ([]registry.Subscription, error) {
	if subFlag != "" {
		var subs []registry.Subscription
		for _, id := range strings.Split(subFlag, ",") {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			var sub registry.Subscription
			code, body, err := getJSON(ctx, client, base+"/v1/subscriptions/"+url.PathEscape(id), token, nil)
			if err != nil {
				return nil, err
			}
			if code/100 != 2 {
				return nil, fmt.Errorf("GET /v1/subscriptions/%s: HTTP %d: %s", id, code, errorBody(body))
			}
			if err := json.Unmarshal(body, &sub); err != nil || sub.ID == "" {
				var wrap struct {
					Subscription registry.Subscription `json:"subscription"`
				}
				if err2 := json.Unmarshal(body, &wrap); err2 != nil || wrap.Subscription.ID == "" {
					return nil, fmt.Errorf("cannot decode subscription %s from %s", id, string(body))
				}
				sub = wrap.Subscription
			}
			subs = append(subs, sub)
		}
		return subs, nil
	}
	code, body, err := getJSON(ctx, client, base+"/v1/subscriptions", token, nil)
	if err != nil {
		return nil, err
	}
	if code/100 != 2 {
		return nil, fmt.Errorf("GET /v1/subscriptions: HTTP %d: %s", code, errorBody(body))
	}
	return decodeList[registry.Subscription](body, "subscriptions", "items")
}

// fetchStatus pulls what the Status API knows about one subscription: the
// delivery records (event reference plus current status, with keyset
// pagination) and the status summary for the report. from/to scope the query
// to the current run, because the status table is cumulative across runs
// (section 10); zero values mean unbounded. The endpoint envelopes are still
// settling while the spike is built in parallel, so the decoding is
// defensive: both fail soft and the verdict falls back to the receiver data.
func fetchStatus(ctx context.Context, client *http.Client, base, token, subID string, from, to time.Time) (map[string]string, map[string]int64, error) {
	refs := map[string]string{}
	var errs []string
	cursor := ""
	seen := 0
	for page := 0; page < statusPageLimit; page++ {
		q := url.Values{}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		if !from.IsZero() {
			q.Set("from", from.UTC().Format(time.RFC3339Nano))
		}
		if !to.IsZero() {
			q.Set("to", to.UTC().Format(time.RFC3339Nano))
		}
		u := base + "/v1/subscriptions/" + url.PathEscape(subID) + "/deliveries"
		if enc := q.Encode(); enc != "" {
			u += "?" + enc
		}
		code, body, err := getJSON(ctx, client, u, token, nil)
		if err != nil {
			errs = append(errs, err.Error())
			break
		}
		if code/100 != 2 {
			errs = append(errs, fmt.Sprintf("HTTP %d: %s", code, errorBody(body)))
			break
		}
		entries, next, derr := decodeDeliveryPage(body)
		if derr != nil {
			errs = append(errs, derr.Error())
			break
		}
		for _, e := range entries {
			if e.replay || e.ref == "" {
				continue
			}
			refs[e.ref] = e.status
		}
		gained := len(refs) - seen
		seen = len(refs)
		if next == "" || next == cursor || (page > 0 && gained == 0) {
			break
		}
		cursor = next
	}

	counts, cerr := fetchSummary(ctx, client, base, token, subID, from)
	if cerr != nil {
		errs = append(errs, cerr.Error())
	}
	if len(errs) > 0 {
		return refs, counts, errors.New(strings.Join(errs, "; "))
	}
	return refs, counts, nil
}

type deliveryEntry struct {
	ref    string
	status string
	replay bool
}

// decodeDeliveryPage decodes one page of GET /subscriptions/{id}/deliveries
// and extracts the keyset cursor for the next page.
func decodeDeliveryPage(body []byte) ([]deliveryEntry, string, error) {
	entries, err := decodeList[map[string]any](body, "deliveries", "items", "rows")
	if err != nil {
		return nil, "", err
	}
	out := make([]deliveryEntry, 0, len(entries))
	for _, m := range entries {
		e := deliveryEntry{}
		e.replay, _ = m["replay"].(bool)
		e.ref, _ = m["event_ref"].(string)
		if e.ref == "" {
			if d, ok := m["delivery_id"].(string); ok {
				e.ref = strings.SplitN(d, "/", 2)[0]
			}
		}
		for _, k := range []string{"status", "current_status", "last_status"} {
			if s, ok := m[k].(string); ok {
				e.status = s
				break
			}
		}
		out = append(out, e)
	}
	var top map[string]json.RawMessage
	next := ""
	if json.Unmarshal(body, &top) == nil {
		for _, k := range []string{"next_cursor", "next", "cursor"} {
			var s string
			if raw, ok := top[k]; ok && json.Unmarshal(raw, &s) == nil && s != "" {
				next = s
				break
			}
		}
	}
	return out, next, nil
}

// fetchSummary reads GET /subscriptions/{id}/status-summary (counts per
// status, section 10). Optional in the first version, so failure is soft.
// from, when non-zero, narrows the counts to the current run; the endpoint's
// default window is the last 24 hours.
func fetchSummary(ctx context.Context, client *http.Client, base, token, subID string, from time.Time) (map[string]int64, error) {
	u := base + "/v1/subscriptions/" + url.PathEscape(subID) + "/status-summary"
	if !from.IsZero() {
		u += "?since=" + url.QueryEscape(from.UTC().Format(time.RFC3339))
	}
	code, body, err := getJSON(ctx, client, u, token, nil)
	if err != nil {
		return nil, err
	}
	if code/100 != 2 {
		return nil, fmt.Errorf("status-summary: HTTP %d: %s", code, errorBody(body))
	}
	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("status-summary: %w", err)
	}
	if inner, ok := raw["counts"].(map[string]any); ok {
		// An explicit counts object is valid even when all counts are 0.
		return numberMap(inner), nil
	}
	counts := numberMap(raw)
	if len(counts) == 0 {
		return nil, errors.New("status-summary: no counts in response")
	}
	return counts, nil
}

// numberMap keeps the numeric values of a JSON object as counts.
func numberMap(m map[string]any) map[string]int64 {
	counts := map[string]int64{}
	for k, v := range m {
		switch n := v.(type) {
		case float64:
			counts[k] = int64(n)
		case json.Number:
			if i, err := n.Int64(); err == nil {
				counts[k] = i
			}
		}
	}
	return counts
}

// formatStatus renders the STATUS column: the summary counts per status, or
// the counts derived from the delivery records, or "?" when the Status API
// could not be reached.
func formatStatus(counts map[string]int64, refs map[string]string, unavailable bool) string {
	if len(counts) == 0 && len(refs) > 0 {
		counts = map[string]int64{}
		for _, st := range refs {
			counts[st]++
		}
	}
	if len(counts) == 0 {
		if unavailable {
			return "?"
		}
		return "-"
	}
	keys := make([]string, 0, len(counts))
	for k, v := range counts {
		if v > 0 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strconv.FormatInt(counts[k], 10))
	}
	return strings.Join(parts, " ")
}

// sampleSet prints up to five ids of a failing set for debugging.
func sampleSet(kind, subID string, set map[string]bool) {
	if len(set) == 0 {
		return
	}
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if len(ids) > 5 {
		ids = append(ids[:5], "...")
	}
	fmt.Fprintf(os.Stderr, "harness verify: %s in %s: %v\n", kind, subID, ids)
}
