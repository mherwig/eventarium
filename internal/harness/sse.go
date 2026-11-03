package harness

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"pubsub/internal/auth"
)

// reconnectDelay is the pause between two connection attempts of one SSE
// client. There is no attempt limit: -duration (or Ctrl-C) stays the only
// bound of the run, so a client keeps reconnecting for as long as the test
// says (section 15.2).
const reconnectDelay = time.Second

// sseRecord is one event received on the SSE stream, appended as a JSON line
// to -out.
type sseRecord struct {
	ReceivedAt time.Time      `json:"received_at"`
	SSEID      string         `json:"sse_id,omitempty"`
	SSEEvent   string         `json:"sse_event,omitempty"`
	DeliveryID string         `json:"delivery_id,omitempty"`
	EventRef   string         `json:"event_ref,omitempty"`
	EventType  string         `json:"event_type,omitempty"`
	Seq        int64          `json:"seq,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
}

// splitBases parses -addr into one or more gateway base URLs, trimmed of
// whitespace and trailing slashes. A single address behaves exactly as it did
// before the list existed; several turn a dead gateway into a failover.
func splitBases(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if b := strings.TrimRight(strings.TrimSpace(part), "/"); b != "" {
			out = append(out, b)
		}
	}
	return out
}

// runSSE implements `harness sse`: the simulated SSE client of section 15.2.
// It opens GET /sse, parses the event stream (id:/event:/data: lines),
// records every event and, when -ack is set, acknowledges each event with the
// Last-Event-ID (docs/contracts.md: only an ack turns sent into delivered).
//
// It behaves like a browser that recovers from an outage (section 8): when
// the stream ends before -duration elapsed, it reconnects automatically with
// Last-Event-ID set to the id of the last event it actually received, and it
// deduplicates by event reference across those reconnects, so an event that
// arrives again — through the backlog fetch and a racing drainer, for example
// — is counted instead of written to -out a second time.
func runSSE(ctx context.Context, args []string) int {
	fs := newFlagSet("sse")
	sub := fs.String("sub", envStr("SUB", ""), "subscription id to consume (env HARNESS_SUB)")
	owner := fs.String("owner", envStr("OWNER", "harness"), "owner reference for the minted token (env HARNESS_OWNER)")
	addr := fs.String("addr", envStr("ADDR", "http://localhost:8082"), "base URL of the SSE gateway, or several comma separated to try in order (env HARNESS_ADDR)")
	out := fs.String("out", envStr("OUT", ""), "append one JSON line per received event (env HARNESS_OUT)")
	ack := fs.Bool("ack", envBool("ACK", true), "acknowledge every event with Last-Event-ID (env HARNESS_ACK)")
	duration := fs.Duration("duration", envDur("DURATION", 0), "stop after this long, 0 runs until Ctrl-C (env HARNESS_DURATION)")
	lastEventID := fs.String("last-event-id", envStr("LAST_EVENT_ID", ""), "cursor sent as Last-Event-ID on the first connect (env HARNESS_LAST_EVENT_ID)")
	if code, done := parseResult(fs.Parse(args)); done {
		return code
	}
	if *sub == "" {
		fmt.Fprintln(os.Stderr, "harness sse: -sub is required")
		return 2
	}
	token, err := mintToken(*owner, auth.ScopeSubscribe)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness sse:", err)
		return 1
	}
	lw, err := openLines(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness sse:", err)
		return 1
	}
	defer lw.Close()

	consumerID := "harness-" + uuid.NewString()[:8]
	// Several bases make the client behave like one behind a load balancer:
	// when its gateway dies it lands on a survivor instead of retrying a dead
	// address until -duration. The cursor travels with it - Last-Event-ID names
	// a position on the deliveries topic, not a node - so a failover resumes
	// the backlog from where it stopped (section 8).
	bases := splitBases(*addr)
	if len(bases) == 0 {
		fmt.Fprintln(os.Stderr, "harness sse: -addr is empty")
		return 2
	}
	base := 0
	streamURL := func() string {
		return bases[base] + "/sse?subscription_id=" + url.QueryEscape(*sub) + "&consumer_id=" + url.QueryEscape(consumerID)
	}
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	fmt.Fprintf(os.Stderr, "harness sse: consuming %s as %s (ack=%v) via %s\n", *sub, consumerID, *ack, bases[0])
	if cursor := strings.TrimSpace(*lastEventID); cursor != "" {
		fmt.Fprintf(os.Stderr, "harness sse: initial Last-Event-ID %s\n", cursor)
	}

	start := time.Now()
	var events, acks, ackErrs, reconnects, deduped atomic.Uint64
	seen := map[string]int{} // event references received, across all connects
	// cursor is the id of the last event actually received (or the
	// -last-event-id flag before anything arrived); it becomes the
	// Last-Event-ID of the next connect, so the gateway resumes the backlog
	// exactly after it (section 8).
	cursor := strings.TrimSpace(*lastEventID)
	var sseID, event string
	var data []string

	// Acks are cumulative - the gateway's ackUpTo() marks everything up to the
	// given sequence - and spec.md:7 asks for them "batched every few hundred
	// ms". They used to be one synchronous POST per event, made from inside
	// the read loop, which capped a single client at ~219 events/s and, by
	// blocking the read, backed TCP up until measured SSE delivery latency
	// was 17 s in every load test while the gateway itself was delivering in
	// ~100 ms. The read loop now records the highest sequence seen and a
	// sender coalesces them; acks and ackErrs keep the same meaning.
	ackMu := &sync.Mutex{}
	latestAck, sentAck := "", ""
	ackStop := make(chan struct{})
	ackDone := make(chan struct{})
	setAck := func(id string) {
		ackMu.Lock()
		latestAck = id
		ackMu.Unlock()
	}
	flushAck := func(ctx context.Context, base int) {
		ackMu.Lock()
		id, from := latestAck, sentAck
		ackMu.Unlock()
		if id == "" || id == from {
			return
		}
		if aerr := ackEvent(ctx, bases[base], token, *sub, id); aerr != nil {
			ackErrs.Add(1)
			fmt.Fprintln(os.Stderr, "harness sse: ack:", aerr)
			return
		}
		acks.Add(1)
		ackMu.Lock()
		sentAck = id
		ackMu.Unlock()
	}
	if *ack {
		go func() {
			defer close(ackDone)
			tick := time.NewTicker(ackInterval)
			defer tick.Stop()
			for {
				select {
				case <-ackStop:
					// A short run must still end with an accurate ack, but the
					// run's context is already cancelled by then - use one that
					// outlives it rather than silently dropping the tail.
					fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
					flushAck(fctx, base)
					cancel()
					return
				case <-tick.C:
					flushAck(ctx, base)
				}
			}
		}()
	} else {
		close(ackDone)
	}

	flush := func() {
		if len(data) == 0 {
			return
		}
		payload := strings.Join(data, "\n")
		rec := sseRecord{ReceivedAt: time.Now().UTC(), SSEID: sseID, SSEEvent: event}
		var envelope map[string]any
		if err := json.Unmarshal([]byte(payload), &envelope); err == nil {
			rec.EventRef, _ = envelope["event_ref"].(string)
			rec.DeliveryID, _ = envelope["delivery_id"].(string)
			rec.EventType, _ = envelope["type"].(string)
			if d, ok := envelope["data"].(map[string]any); ok {
				rec.Data = d
				switch v := d["seq"].(type) {
				case float64:
					rec.Seq = int64(v)
				}
			}
			if id, ok := envelope["id"].(string); ok && rec.EventRef == "" && rec.DeliveryID == "" {
				// Plain CloudEvent in the data field: the SSE id carries
				// the delivery metadata (section 5).
				rec.EventRef, rec.DeliveryID = fromSSEID(sseID, id)
			}
		} else {
			rec.EventRef, rec.DeliveryID = fromSSEID(sseID, "")
		}
		if rec.EventRef == "" {
			rec.EventRef = sseID
		}
		if sseID != "" {
			cursor = sseID // this is where a reconnect has to continue
		}
		if _, dup := seen[rec.EventRef]; dup {
			// Section 8's client-side dedup: the same event reference is
			// never written to -out twice, no matter how many reconnects or
			// delivery paths produced it; it is counted instead.
			deduped.Add(1)
		} else {
			seen[rec.EventRef]++
			events.Add(1)
			if werr := lw.Write(rec); werr != nil {
				fmt.Fprintln(os.Stderr, "harness sse: writing -out:", werr)
			}
		}
		if *ack && sseID != "" {
			// Record only: the sender coalesces, and the ack is cumulative,
			// so nothing between the previous send and this one is lost.
			setAck(sseID)
		}
		sseID, event, data = "", "", nil
	}

	// established records whether the last attempt reached a stream, which is
	// what tells "this address is gone" apart from "the server closed".
	established := false

	// connect performs one connection attempt and consumes the stream until it
	// ends. The bool result marks a client-side HTTP error that retrying
	// cannot fix (a 4xx other than 429), which ends the run; everything else —
	// clean close, EOF, reset, refused, 5xx — is a reconnect case as long as
	// the duration has not elapsed.
	connect := func() (error, bool) {
		established = false
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL(), nil)
		if err != nil {
			return err, true
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "text/event-stream")
		if cursor != "" {
			// The gateway starts fetching the backlog at this id + 1 and
			// merges it with the live stream (section 8). Without events
			// received so far there is no cursor and no header.
			req.Header.Set("Last-Event-ID", cursor)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			established = false
			return err, false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			// A rejected stream is not an address problem: 404/401 would fail
			// on every node too, and rotating would turn a real error into a
			// misleading one somewhere else.
			established = true
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			err := fmt.Errorf("HTTP %d: %s", resp.StatusCode, errorBody(b))
			fatal := resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests
			return err, fatal
		}
		established = true

		sc := bufio.NewScanner(resp.Body)
		sc.Buffer(make([]byte, 0, 64<<10), 16<<20)
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "": // end of one event
				flush()
			case strings.HasPrefix(line, ":"): // heartbeat or comment
			default:
				field, value := line, ""
				if i := strings.IndexByte(line, ':'); i >= 0 {
					field, value = line[:i], strings.TrimPrefix(line[i+1:], " ")
				}
				switch field {
				case "id":
					sseID = value
				case "event":
					event = value
				case "data":
					data = append(data, value)
				}
			}
		}
		flush()
		return sc.Err(), false
	}

	var fatal error
	for {
		err, isFatal := connect()
		if ctx.Err() != nil {
			break // duration elapsed or cancelled: the bound of the run
		}
		if isFatal {
			fatal = err
			break
		}
		switch {
		case err == nil:
			fmt.Fprintln(os.Stderr, "harness sse: stream ended by the server")
		case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
			fmt.Fprintln(os.Stderr, "harness sse: stream closed unexpectedly (EOF)")
		default:
			fmt.Fprintln(os.Stderr, "harness sse: stream error:", err)
		}
		reconnects.Add(1)
		// Advance only when the attempt could not be made at all: a stream that
		// ended is a server-side event and the same address is worth retrying,
		// while an unreachable address is not. Rotation is what lets a client
		// whose gateway died land on a survivor - the scenario that proves
		// failover across nodes needs it, since no LB exists in the stack.
		if len(bases) > 1 && !established {
			next := (base + 1) % len(bases)
			if next != base {
				fmt.Fprintf(os.Stderr, "harness sse: %s unreachable, trying %s\n", bases[base], bases[next])
				base = next
			}
		}
		if cursor != "" {
			fmt.Fprintf(os.Stderr, "harness sse: reconnecting with Last-Event-ID %s\n", cursor)
		} else {
			fmt.Fprintln(os.Stderr, "harness sse: reconnecting without a cursor")
		}
		select {
		case <-ctx.Done():
		case <-time.After(reconnectDelay):
		}
	}

	// Stop the acker and let it flush: the tail of a run must still be
	// acknowledged or deliveries stay "sent" instead of "delivered".
	close(ackStop)
	<-ackDone

	if fatal != nil {
		fmt.Fprintln(os.Stderr, "harness sse:", fatal)
	} else {
		fmt.Fprintln(os.Stderr, "harness sse: stream closed (cancelled or duration reached)")
	}
	dups := 0
	for _, n := range seen {
		if n > 1 {
			dups += n - 1
		}
	}
	elapsed := time.Since(start).Seconds()
	fmt.Printf("harness sse: events=%d unique=%d duplicates=%d acks=%d ack_errors=%d reconnects=%d deduped=%d elapsed=%.1fs\n",
		events.Load(), len(seen), dups, acks.Load(), ackErrs.Load(), reconnects.Load(), deduped.Load(), elapsed)
	if fatal != nil {
		return 1
	}
	return 0
}

// fromSSEID derives event reference and delivery ID from the SSE id field.
// The delivery ID is event_ref + "/" + subscription_id (section 5), so a
// slash splits them; without a slash both fall back to the id itself.
func fromSSEID(sseID, eventID string) (eventRef, deliveryID string) {
	if sseID == "" {
		return eventID, eventID
	}
	if i := strings.Index(sseID, "/"); i > 0 {
		return sseID[:i], sseID
	}
	return sseID, sseID
}

// ackEvent acknowledges one delivery with the Last-Event-ID (docs/contracts.md,
// sse-gateway: Last-Event-ID or body).
// ackInterval is how often the coalesced ack is sent. spec.md:7 asks for
// per-event acks "batched every few hundred ms"; 100 ms keeps the delivered
// status fresh without putting a round trip in the client's read loop.
const ackInterval = 100 * time.Millisecond

func ackEvent(ctx context.Context, base, token, subID, lastEventID string) error {
	actx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// The gateway wants the subscription and the sequence ("part:off" from
	// the SSE id field) in the body, or Last-Event-ID plus subscription_id.
	payload := map[string]string{"subscription_id": subID, "sequence": lastEventID}
	code, body, err := postJSON(actx, http.DefaultClient, base+"/sse/ack", token, payload, nil)
	if err != nil {
		return err
	}
	if code/100 != 2 {
		return fmt.Errorf("HTTP %d: %s", code, errorBody(body))
	}
	return nil
}
