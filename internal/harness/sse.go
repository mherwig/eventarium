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

// runSSE implements `harness sse`: the simulated SSE client of section 15.2.
// It opens GET /sse, parses the event stream (id:/event:/data: lines),
// records every event and, when -ack is set, acknowledges each event with the
// Last-Event-ID (docs/SPIKE.md: only an ack turns sent into delivered).
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
	addr := fs.String("addr", envStr("ADDR", "http://localhost:8082"), "base URL of the SSE gateway (env HARNESS_ADDR)")
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
	base := strings.TrimRight(*addr, "/")
	streamURL := base + "/sse?subscription_id=" + url.QueryEscape(*sub) + "&consumer_id=" + url.QueryEscape(consumerID)
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	fmt.Fprintf(os.Stderr, "harness sse: consuming %s as %s (ack=%v)\n", *sub, consumerID, *ack)
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
			if aerr := ackEvent(ctx, base, token, *sub, sseID); aerr != nil {
				ackErrs.Add(1)
				fmt.Fprintln(os.Stderr, "harness sse: ack:", aerr)
			} else {
				acks.Add(1)
			}
		}
		sseID, event, data = "", "", nil
	}

	// connect performs one connection attempt and consumes the stream until it
	// ends. The bool result marks a client-side HTTP error that retrying
	// cannot fix (a 4xx other than 429), which ends the run; everything else —
	// clean close, EOF, reset, refused, 5xx — is a reconnect case as long as
	// the duration has not elapsed.
	connect := func() (error, bool) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
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
			return err, false
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
			err := fmt.Errorf("HTTP %d: %s", resp.StatusCode, errorBody(b))
			fatal := resp.StatusCode/100 == 4 && resp.StatusCode != http.StatusTooManyRequests
			return err, fatal
		}

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

// ackEvent acknowledges one delivery with the Last-Event-ID (docs/SPIKE.md,
// sse-gateway: Last-Event-ID or body).
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
