package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"pubsub/internal/auth"
	"pubsub/internal/cloudevents"
	"pubsub/internal/registry"
)

// pubBatchSize is the batch size of -mode batch (section 15.2: batch mode for
// throughput, one structured batch document per request).
const pubBatchSize = 100

// entityPoolSize is the number of distinct entities the publisher cycles
// through. The entity lives in the subject attribute; the ordering key is
// derived from it by hash, so the same entity always gets the same key.
const entityPoolSize = 1024

// pubRecord is one published event plus its attributes, written as a JSON line
// to -out. verify recomputes the expected delivery set from these records; the
// platform never sees this file.
type pubRecord struct {
	EventRef     string            `json:"event_ref,omitempty"`
	ID           string            `json:"id"`
	Source       string            `json:"source"`
	Type         string            `json:"type"`
	Subject      string            `json:"subject"`
	Time         time.Time         `json:"time"`
	PartitionKey string            `json:"partition_key"`
	Shard        int               `json:"shard"`
	Seq          int64             `json:"seq"`
	Data         map[string]any    `json:"data"`
	Extensions   map[string]string `json:"extensions,omitempty"`
}

// pubItem is one HTTP request's worth of events plus the records to fill in
// once the platform returns the event references.
type pubItem struct {
	evs []*cloudevents.Event
	rec []pubRecord
}

// publisher is the open-loop load generator of section 15.2: it aims at the
// configured rate and does not slow down when the server is slow. Events the
// fixed sender pool cannot pick up in time are counted as dropped, never
// queued without bound.
type publisher struct {
	client  *http.Client
	url     string
	token   string
	source  string
	types   []string
	payload int
	keys    int
	batch   int // 1 for single mode, pubBatchSize for batch mode
	timeout time.Duration
	lw      *lineWriter

	mu       sync.Mutex
	sent     uint64
	ok       uint64
	fail4xx  uint64
	fail5xx  uint64
	timeouts uint64
	neterr   uint64
	dropped  uint64
	started  time.Time
	ended    time.Time
}

func (p *publisher) bump(field *uint64, n uint64) {
	p.mu.Lock()
	*field += n
	p.mu.Unlock()
}

func (p *publisher) snapshot() (sent, ok, c4xx, c5xx, timeouts, neterr, dropped uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent, p.ok, p.fail4xx, p.fail5xx, p.timeouts, p.neterr, p.dropped
}

// runPub implements `harness pub`: the open-loop publisher of section 15.2.
func runPub(ctx context.Context, args []string) int {
	fs := newFlagSet("pub")
	api := fs.String("api", apiDefault(), "base URL of the publish API (env API)")
	owner := fs.String("owner", envStr("OWNER", "harness"), "owner reference for the minted token (env HARNESS_OWNER)")
	rate := fs.Float64("rate", envFloat("RATE", 100), "target publish rate in events per second (env HARNESS_RATE)")
	duration := fs.Duration("duration", envDur("DURATION", 30*time.Second), "how long to publish (env HARNESS_DURATION)")
	types := fs.String("types", envStr("TYPES", ""), "comma separated event types, or a prefix looked up in the registry (env HARNESS_TYPES)")
	payload := fs.Int("payload", envInt("PAYLOAD", 1024), "approximate JSON data size in bytes (env HARNESS_PAYLOAD)")
	mode := fs.String("mode", envStr("MODE", "batch"), "single, or batch with "+strconv.Itoa(pubBatchSize)+" events per request (env HARNESS_MODE)")
	keys := fs.Int("keys", envInt("KEYS", 32), "number of distinct ordering keys (env HARNESS_KEYS)")
	out := fs.String("out", envStr("OUT", ""), "append published events with attributes as JSON lines for verify (env HARNESS_OUT)")
	timeout := fs.Duration("timeout", envDur("TIMEOUT", 10*time.Second), "per-request timeout (env HARNESS_TIMEOUT)")
	workers := fs.Int("workers", envInt("WORKERS", 64), "concurrent sender goroutines (env HARNESS_WORKERS)")
	source := fs.String("source", envStr("SOURCE", "/harness"), "CloudEvents source attribute (env HARNESS_SOURCE)")
	if code, done := parseResult(fs.Parse(args)); done {
		return code
	}
	if *rate <= 0 {
		fmt.Fprintln(os.Stderr, "harness pub: -rate must be positive")
		return 2
	}
	if *keys <= 0 {
		fmt.Fprintln(os.Stderr, "harness pub: -keys must be positive")
		return 2
	}
	batch := pubBatchSize
	switch strings.ToLower(*mode) {
	case "batch":
	case "single":
		batch = 1
	default:
		fmt.Fprintf(os.Stderr, "harness pub: -mode must be batch or single, got %q\n", *mode)
		return 2
	}
	if *workers <= 0 {
		*workers = 1
	}

	token, err := mintToken(*owner, auth.ScopePublish)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness pub:", err)
		return 1
	}
	typeNames, err := resolveTypes(ctx, newHTTPClient(), strings.TrimRight(*api, "/"), token, *types)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness pub:", err)
		return 1
	}
	lw, err := openLines(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness pub:", err)
		return 1
	}
	defer lw.Close()

	p := &publisher{
		client:  newHTTPClient(),
		url:     strings.TrimRight(*api, "/") + "/v1/events",
		token:   token,
		source:  *source,
		types:   typeNames,
		payload: *payload,
		keys:    *keys,
		batch:   batch,
		timeout: *timeout,
		lw:      lw,
	}

	fmt.Fprintf(os.Stderr, "harness pub: %g ev/s for %v, mode=%s, %d types, %d keys -> %s\n",
		*rate, *duration, *mode, len(typeNames), *keys, p.url)
	p.run(ctx, *rate, *duration, *workers)
	p.report(*rate)
	return 0
}

// resolveTypes turns the -types flag into the list of event types to publish
// to: either an explicit comma separated list, or a prefix that is looked up
// in the registry.
func resolveTypes(ctx context.Context, client *http.Client, base, token, spec string) ([]string, error) {
	if spec == "" {
		return nil, errors.New("-types is required (comma separated names or a registry prefix)")
	}
	if strings.Contains(spec, ",") {
		var names []string
		for _, n := range strings.Split(spec, ",") {
			if n = strings.TrimSpace(n); n != "" {
				names = append(names, n)
			}
		}
		if len(names) == 0 {
			return nil, errors.New("-types is empty")
		}
		return names, nil
	}
	code, body, err := getJSON(ctx, client, base+"/v1/event-types", token, nil)
	if err != nil {
		return nil, err
	}
	if code/100 != 2 {
		return nil, fmt.Errorf("GET /v1/event-types: HTTP %d: %s", code, errorBody(body))
	}
	ets, err := decodeList[registry.EventType](body, "event_types", "items")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, et := range ets {
		if strings.HasPrefix(et.Name, spec) {
			names = append(names, et.Name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no event type with prefix %q in the registry", spec)
	}
	return names, nil
}

// run drives the open loop: a producer schedules batches at the target rate and
// never waits for the network; a fixed sender pool consumes them. Ctrl-C stops
// the producer and lets the senders finish their in-flight requests.
func (p *publisher) run(ctx context.Context, rate float64, duration time.Duration, workers int) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	items := make(chan pubItem, workers*2)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for it := range items {
				p.send(ctx, it)
			}
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	// Live per-second report on stderr: sends, 4xx, 5xx, timeouts.
	reported := make(chan struct{})
	go func() {
		defer close(reported)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		prev := [7]uint64{}
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				cur := p.arr()
				fmt.Fprintf(os.Stderr, "harness pub: sends=%d ok=%d 4xx=%d 5xx=%d timeouts=%d net=%d dropped=%d\n",
					cur[0]-prev[0], cur[1]-prev[1], cur[2]-prev[2], cur[3]-prev[3],
					cur[4]-prev[4], cur[5]-prev[5], cur[6]-prev[6])
				prev = cur
			}
		}
	}()

	start := time.Now()
	p.started = start
	deadline := start.Add(duration)
	interval := time.Duration(float64(time.Second) * float64(p.batch) / rate)
	var seq int64
loop:
	for i := 0; ; i++ {
		target := start.Add(time.Duration(i) * interval)
		if !target.Before(deadline) {
			break
		}
		if wait := time.Until(target); wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				break loop
			case <-t.C:
			}
		}
		evs, rec := p.makeEvents(p.batch, &seq)
		p.bump(&p.sent, uint64(len(evs)))
		select {
		case <-ctx.Done():
			break loop
		case items <- pubItem{evs: evs, rec: rec}:
		default:
			// Open loop: the sender pool is behind; count and move on.
			p.bump(&p.dropped, uint64(len(evs)))
		}
	}
	close(items)
	<-done
	cancel() // stop the reporter
	<-reported
	p.ended = time.Now()
}

// arr returns the counters as an array for delta arithmetic in the reporter.
func (p *publisher) arr() [7]uint64 {
	s, o, c4, c5, t, n, d := p.snapshot()
	return [7]uint64{s, o, c4, c5, t, n, d}
}

// makeEvents builds the next n events. The subject names an entity from a
// fixed pool; the ordering key (and the shard attribute the seeded CEL
// filters select on) is a stable hash of the subject, so the same entity
// always lands on the same key.
func (p *publisher) makeEvents(n int, seq *int64) ([]*cloudevents.Event, []pubRecord) {
	evs := make([]*cloudevents.Event, n)
	recs := make([]pubRecord, n)
	padLen := p.payload - 160
	if padLen < 16 {
		padLen = 16
	}
	if padLen > 1<<16 {
		padLen = 1 << 16
	}
	for j := range n {
		s := *seq
		*seq++
		subject := fmt.Sprintf("entity-%d", s%entityPoolSize)
		key := int(crc32.ChecksumIEEE([]byte(subject)) % uint32(p.keys))
		keyStr := "key-" + strconv.Itoa(key)
		now := time.Now().UTC()
		data := map[string]any{
			"shard":   key,
			"seq":     s,
			"sent_at": now.Format(time.RFC3339Nano),
			"pad":     randomHex(padLen),
		}
		dataJSON, err := json.Marshal(data)
		if err != nil {
			panic(err)
		}
		// The record carries the data exactly as the platform will decode
		// it (JSON numbers become float64), so the oracle in verify
		// evaluates the same shape the router evaluates.
		var decoded map[string]any
		if err := json.Unmarshal(dataJSON, &decoded); err != nil {
			panic(err)
		}
		ev := &cloudevents.Event{
			SpecVersion:     cloudevents.SpecV1,
			ID:              uuid.NewString(),
			Source:          p.source,
			Type:            p.types[s%int64(len(p.types))],
			DataContentType: "application/json",
			Subject:         subject,
			Time:            &now,
			Data:            dataJSON,
			Extensions: map[string]json.RawMessage{
				// The platform's default ordering key (section 5).
				"partitionkey": json.RawMessage(strconv.Quote(keyStr)),
				// Kept next to it so the seeded attribute filters
				// (attributes["shard"] == "k") have something to match.
				"shard": json.RawMessage(strconv.Itoa(key)),
			},
		}
		evs[j] = ev
		recs[j] = pubRecord{
			ID:           ev.ID,
			Source:       ev.Source,
			Type:         ev.Type,
			Subject:      ev.Subject,
			Time:         now,
			PartitionKey: keyStr,
			Shard:        key,
			Seq:          s,
			Data:         decoded,
			Extensions:   map[string]string{"partitionkey": keyStr, "shard": strconv.Itoa(key)},
		}
	}
	return evs, recs
}

func randomHex(n int) string {
	b := make([]byte, (n+1)/2)
	for i := range b {
		b[i] = byte(rand.UintN(256))
	}
	return fmt.Sprintf("%x", b)[:n]
}

// send performs one HTTP request (single event or batch) and records the
// outcome. It never blocks the producer: it runs on a sender goroutine.
func (p *publisher) send(ctx context.Context, it pubItem) {
	rctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	req, err := p.buildRequest(rctx, it.evs)
	if err != nil {
		p.bump(&p.neterr, uint64(len(it.evs)))
		return
	}
	req.Header.Set("Authorization", "Bearer "+p.token)

	resp, err := p.client.Do(req)
	if err != nil {
		if isTimeout(err) {
			p.bump(&p.timeouts, uint64(len(it.evs)))
		} else {
			p.bump(&p.neterr, uint64(len(it.evs)))
		}
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		p.bump(&p.ok, uint64(len(it.evs)))
		refs, rerr := parsePublishRefs(body, len(it.evs))
		if rerr != nil {
			fmt.Fprintf(os.Stderr, "harness pub: cannot parse publish response %q: %v\n", string(body), rerr)
		}
		for i := range it.evs {
			if i < len(refs) {
				it.rec[i].EventRef = refs[i]
			}
			if werr := p.lw.Write(it.rec[i]); werr != nil {
				fmt.Fprintln(os.Stderr, "harness pub: writing -out:", werr)
			}
		}
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		p.bump(&p.fail4xx, uint64(len(it.evs)))
		fmt.Fprintf(os.Stderr, "harness pub: HTTP %d: %s\n", resp.StatusCode, errorBody(body))
	default:
		p.bump(&p.fail5xx, uint64(len(it.evs)))
		fmt.Fprintf(os.Stderr, "harness pub: HTTP %d: %s\n", resp.StatusCode, errorBody(body))
	}
}

// buildRequest encodes one publish request: binary mode for a single event
// (ce-* headers plus the JSON data member as body), batch mode for a list of
// structured events.
func (p *publisher) buildRequest(ctx context.Context, evs []*cloudevents.Event) (*http.Request, error) {
	if p.batch == 1 {
		ev := evs[0]
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(ev.Data))
		if err != nil {
			return nil, err
		}
		ev.SetHeaders(req.Header)
		req.Header.Set("Content-Type", "application/json")
		return req, nil
	}
	raws := make([]json.RawMessage, len(evs))
	for i, ev := range evs {
		b, err := json.Marshal(ev)
		if err != nil {
			return nil, err
		}
		raws[i] = b
	}
	body, err := json.Marshal(raws)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", cloudevents.ContentTypeBatch)
	return req, nil
}

// parsePublishRefs extracts the event references from the 201 response: one
// object per publish, a list for batches (docs/contracts.md, publish-api).
func parsePublishRefs(body []byte, want int) ([]string, error) {
	var list []struct {
		EventRef string `json:"event_ref"`
	}
	if err := json.Unmarshal(body, &list); err == nil {
		refs := make([]string, len(list))
		for i, e := range list {
			refs[i] = e.EventRef
		}
		if len(refs) != want {
			return refs, fmt.Errorf("got %d event refs, sent %d events", len(refs), want)
		}
		return refs, nil
	}
	var one struct {
		EventRef string `json:"event_ref"`
	}
	if err := json.Unmarshal(body, &one); err == nil && one.EventRef != "" {
		if want != 1 {
			return []string{one.EventRef}, fmt.Errorf("got 1 event ref, sent %d events", want)
		}
		return []string{one.EventRef}, nil
	}
	return nil, errors.New("no event_ref in response")
}

func isTimeout(err error) bool {
	var ne interface{ Timeout() bool }
	return errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout())
}

// report prints the per-subcommand summary. The achieved rate is based on the
// real elapsed time, so an interrupted run reports honestly.
func (p *publisher) report(target float64) {
	sent, ok, c4xx, c5xx, timeouts, neterr, dropped := p.snapshot()
	failed := c4xx + c5xx + timeouts + neterr + dropped
	elapsed := p.ended.Sub(p.started).Seconds()
	if elapsed <= 0 {
		elapsed = 1
	}
	fmt.Printf("harness pub: sent=%d ok=%d failed=%d (4xx=%d 5xx=%d timeouts=%d net=%d dropped=%d)\n",
		sent, ok, failed, c4xx, c5xx, timeouts, neterr, dropped)
	fmt.Printf("harness pub: target=%.1f ev/s achieved=%.1f ev/s elapsed=%.1fs\n",
		target, float64(ok)/elapsed, elapsed)
}
