package dispatch

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"pubsub/internal/cloudevents"
	"pubsub/internal/status"
)

// Reasons for status records emitted by the dispatcher.
const (
	reasonCircuitOpen = "circuit_open"
	reasonQueueFull   = "queue_full"
	reasonRateLimited = "rate_limited"
	reasonDraining    = "draining"
	reasonBacklogMax  = "backlog_max"
	reasonMaxAttempts = "max_attempts"
	reasonGone        = "gone"
	reasonExpired     = "expired"
	reasonUnknownSub  = "unknown_subscription"
	reasonParkFailed  = "park_failed"
	reasonHTTP429     = "http_429"
	reasonHTTPError   = "http_error"
	reasonTransport   = "transport_error"
)

// 429 handling (spec 6.4): Retry-After may be seconds or an HTTP date. A
// missing or unparseable header still pauses the subscription, with a sane
// default; the pause is capped so a hostile header cannot park a subscription
// for days.
const (
	defaultRetryAfter = 30 * time.Second
	maxRetryAfter     = time.Hour
	// rateSaturation bounds the wait for a rate-limiter token; beyond it the
	// limiter is considered saturated under sustained load and the event is
	// parked instead of tying up a worker.
	rateSaturation = 10 * time.Second
)

// attemptKind classifies one HTTP attempt.
type attemptKind int

const (
	attemptOK attemptKind = iota
	attemptGone
	attempt429
	attemptFailure
)

// attemptResult is the outcome of one attempt.
type attemptResult struct {
	kind       attemptKind
	code       int
	latency    time.Duration
	retryAfter time.Duration
	reason     string
	err        error
}

// send performs exactly one webhook attempt: CloudEvents binding per policy,
// delivery metadata headers (spec 5: transport metadata, never inside the
// event), optional HMAC signature, SSRF guard, no redirects, per-request
// timeout.
func (d *Dispatcher) send(ctx context.Context, pol policy, it *item, attempt uint16) attemptResult {
	msg := it.msg
	var body []byte
	h := http.Header{}
	switch pol.format {
	case "structured":
		b, err := json.Marshal(msg.Event)
		if err != nil {
			return attemptResult{kind: attemptFailure, reason: reasonTransport, err: fmt.Errorf("marshalling event: %w", err)}
		}
		body = b
		h.Set("Content-Type", cloudevents.ContentTypeStructured)
	default: // binary mode, the CloudEvents binary binding
		body = eventData(msg.Event)
		msg.Event.SetHeaders(h)
		if msg.Event.DataContentType != "" {
			h.Set("Content-Type", msg.Event.DataContentType)
		}
	}

	// Delivery metadata headers (spec 5, transport metadata).
	h.Set("pubsub-delivery-id", msg.DeliveryID)
	h.Set("pubsub-event-ref", msg.EventRef)
	h.Set("pubsub-attempt", strconv.FormatUint(uint64(attempt), 10))
	h.Set("pubsub-seq", it.seq.String())
	if msg.Replay {
		h.Set("pubsub-replay-id", msg.ReplayID)
	}
	if len(d.secret) > 0 {
		mac := hmac.New(sha256.New, d.secret)
		mac.Write(body)
		h.Set("X-Pubsub-Signature", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	// Spec 6.4 security list (cheap version): only http/https and no private
	// or loopback targets. This checks the addresses at attempt time; a full
	// fix would also pin the resolved address for the dial (DNS rebinding).
	if err := d.checkTarget(ctx, pol.callbackURL); err != nil {
		return attemptResult{kind: attemptFailure, reason: reasonTransport, err: err}
	}

	rctx, cancel := context.WithTimeout(ctx, pol.requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, pol.callbackURL, bytes.NewReader(body))
	if err != nil {
		return attemptResult{kind: attemptFailure, reason: reasonTransport, err: err}
	}
	req.Header = h

	start := time.Now()
	resp, err := d.httpc.Do(req)
	latency := time.Since(start)
	if err != nil {
		return attemptResult{kind: attemptFailure, latency: latency, reason: reasonTransport, err: err}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 8192)) // keep the connection reusable

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return attemptResult{kind: attemptOK, code: resp.StatusCode, latency: latency}
	case resp.StatusCode == http.StatusGone:
		return attemptResult{kind: attemptGone, code: resp.StatusCode, latency: latency}
	case resp.StatusCode == http.StatusTooManyRequests:
		return attemptResult{
			kind:       attempt429,
			code:       resp.StatusCode,
			latency:    latency,
			retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		}
	default:
		return attemptResult{kind: attemptFailure, code: resp.StatusCode, latency: latency, reason: reasonHTTPError}
	}
}

// checkTarget validates the callback URL: http/https only and no private,
// loopback or link-local addresses unless ALLOW_PRIVATE_WEBHOOKS is set (the
// local test harness delivers to 127.0.0.1, so the spike defaults to true).
func (d *Dispatcher) checkTarget(ctx context.Context, rawurl string) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		return fmt.Errorf("invalid callback url: %w", err)
	}
	switch u.Scheme {
	case "http", "https":
	case "":
		return errors.New("callback url has no scheme")
	default:
		return fmt.Errorf("callback url scheme %q not allowed", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return errors.New("callback url has no host")
	}
	if d.allowPrivate {
		return nil
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("resolving %s: %w", host, err)
	}
	if len(addrs) == 0 {
		return fmt.Errorf("no addresses for %s", host)
	}
	for _, a := range addrs {
		if isPrivateIP(a.IP) {
			return fmt.Errorf("refusing delivery to %s (%s): private address", host, a.IP)
		}
	}
	return nil
}

func isPrivateIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsInterfaceLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

// parseRetryAfter understands both forms of the header (spec 6.4): a delay in
// seconds or an HTTP date. Missing or invalid values fall back to a default
// pause; the result is clamped to [0, 1h].
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return defaultRetryAfter
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return defaultRetryAfter
		}
		return min(time.Duration(secs)*time.Second, maxRetryAfter)
	}
	if t, err := http.ParseTime(v); err == nil {
		d := t.Sub(now)
		if d < 0 {
			return 0
		}
		return min(d, maxRetryAfter)
	}
	return defaultRetryAfter
}

// eventData returns the raw event data bytes for the binary binding.
func eventData(e *cloudevents.Event) []byte {
	if e.DataBase64 != "" {
		if b, err := base64.StdEncoding.DecodeString(e.DataBase64); err == nil {
			return b
		}
	}
	return e.Data
}

// emitStatus sends one status transition for the item to the status topic
// (section 9). SrcPartition/SrcOffset carry the deliveries-topic sequence, the
// dispatcher's position of record, which matches the pubsub-seq header.
//
// TS is stamped here, at the transition, and not left to the producer: the
// emitter queues records and flushes them in batches of 512 or 50 ms, and
// status.Producer only fills a zero TS when it actually produces - so
// in_flight and delivered of one delivery would otherwise be stamped with the
// flush's millisecond, making the history and last_ts of a delivery say when
// a batch went out rather than what happened. (Ordering within a shared
// millisecond is then settled by the lifecycle rank, internal/chquery; the
// stamp is what makes the timestamps themselves true.)
func (d *Dispatcher) emitStatus(it *item, st, reason string, attempt uint16, code int, latency time.Duration) {
	msg := it.msg
	d.emitter.Emit(status.Record{
		TS:             time.Now().UTC(),
		Owner:          msg.Owner,
		EventRef:       msg.EventRef,
		DeliveryID:     msg.DeliveryID,
		SubscriptionID: msg.SubscriptionID,
		Status:         st,
		Reason:         reason,
		Attempt:        attempt,
		HTTPCode:       uint16(code),
		LatencyMS:      uint32(latency / time.Millisecond),
		ReplayID:       msg.ReplayID,
		RetentionClass: msg.RetentionClass,
		SrcPartition:   it.seq.Partition,
		SrcOffset:      it.seq.Offset,
	})
}
