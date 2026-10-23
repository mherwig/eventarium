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
// Last-Event-ID (SPIKE.md: only an ack turns sent into delivered).
func runSSE(ctx context.Context, args []string) int {
	fs := newFlagSet("sse")
	sub := fs.String("sub", envStr("SUB", ""), "subscription id to consume (env HARNESS_SUB)")
	owner := fs.String("owner", envStr("OWNER", "harness"), "owner reference for the minted token (env HARNESS_OWNER)")
	addr := fs.String("addr", envStr("ADDR", "http://localhost:8082"), "base URL of the SSE gateway (env HARNESS_ADDR)")
	out := fs.String("out", envStr("OUT", ""), "append one JSON line per received event (env HARNESS_OUT)")
	ack := fs.Bool("ack", envBool("ACK", true), "acknowledge every event with Last-Event-ID (env HARNESS_ACK)")
	duration := fs.Duration("duration", envDur("DURATION", 0), "stop after this long, 0 runs until Ctrl-C (env HARNESS_DURATION)")
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
	// No client timeout: the stream is long-lived, the context bounds it.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, streamURL, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness sse:", err)
		return 1
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "text/event-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			fmt.Fprintln(os.Stderr, "harness sse: cancelled before connect")
			return 0
		}
		fmt.Fprintln(os.Stderr, "harness sse:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		fmt.Fprintf(os.Stderr, "harness sse: HTTP %d: %s\n", resp.StatusCode, errorBody(b))
		return 1
	}

	start := time.Now()
	var events, acks, ackErrs atomic.Uint64
	seen := map[string]int{}
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
		events.Add(1)
		seen[rec.EventRef]++
		if werr := lw.Write(rec); werr != nil {
			fmt.Fprintln(os.Stderr, "harness sse: writing -out:", werr)
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

	dups := 0
	for _, n := range seen {
		if n > 1 {
			dups += n - 1
		}
	}
	elapsed := time.Since(start).Seconds()
	switch {
	case errors.Is(sc.Err(), context.Canceled) || errors.Is(sc.Err(), context.DeadlineExceeded) || ctx.Err() != nil:
		fmt.Fprintln(os.Stderr, "harness sse: stream closed (cancelled or duration reached)")
	case sc.Err() != nil:
		fmt.Fprintln(os.Stderr, "harness sse: stream error:", sc.Err())
	default:
		fmt.Fprintln(os.Stderr, "harness sse: stream ended")
	}
	fmt.Printf("harness sse: events=%d unique=%d duplicates=%d acks=%d ack_errors=%d elapsed=%.1fs\n",
		events.Load(), len(seen), dups, acks.Load(), ackErrs.Load(), elapsed)
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

// ackEvent acknowledges one delivery with the Last-Event-ID (SPIKE.md,
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
