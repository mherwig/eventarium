package harness

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pubsub/internal/cloudevents"
)

// flaky429Every answers every n-th request of a -flaky429 receiver with
// 429 + Retry-After: 1 (section 6.4: a 429 with Retry-After must pause the
// subscription and not count as a circuit-breaker failure).
const flaky429Every = 50

// recvRecord is one received webhook delivery, appended as a JSON line to -out.
// verify joins it with the published events and the Status API.
type recvRecord struct {
	ReceivedAt     time.Time `json:"received_at"`
	DeliveryID     string    `json:"delivery_id"`
	EventRef       string    `json:"event_ref"`
	SubscriptionID string    `json:"subscription_id"`
	Seq            int64     `json:"seq"`
	Attempt        int       `json:"attempt"`
}

// receiver is the simulated webhook receiver of section 15.2: configurable
// latency, error rate, occasional 429 and an offline window.
type receiver struct {
	latency   time.Duration
	errorRate float64
	flaky     bool
	lw        *lineWriter

	total   atomic.Uint64 // valid CloudEvents accepted
	ok      atomic.Uint64 // answered 200
	err500  atomic.Uint64 // answered 500 on purpose
	r429    atomic.Uint64 // answered 429 on purpose
	reject  atomic.Uint64 // answered 400 (not a CloudEvent)
	n       atomic.Uint64 // request counter, drives -flaky429
	mu      sync.Mutex
	perSub  map[string]uint64
	started time.Time
}

func (rc *receiver) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc.n.Add(1)
		body, err := io.ReadAll(io.LimitReader(r.Body, cloudevents.DefaultMaxBytes+1))
		if err != nil {
			rc.reject.Add(1)
			http.Error(w, "cannot read body", http.StatusBadRequest)
			return
		}
		// Every delivery must be a valid CloudEvents binary binding.
		ev, err := cloudevents.ParseBinary(r.Header, body)
		if err != nil {
			rc.reject.Add(1)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		deliveryID := r.Header.Get("pubsub-delivery-id")
		subID := r.Header.Get("pubsub-subscription")
		if subID == "" {
			// Delivery ID is event_ref + "/" + subscription_id (section 5).
			if i := strings.LastIndex(deliveryID, "/"); i >= 0 {
				subID = deliveryID[i+1:]
			}
		}
		eventRef := r.Header.Get("pubsub-event-ref")
		if eventRef == "" {
			eventRef = strings.SplitN(deliveryID, "/", 2)[0]
		}
		attempt, _ := strconv.Atoi(r.Header.Get("pubsub-attempt"))
		var payload struct {
			Seq int64 `json:"seq"`
		}
		_ = json.Unmarshal(ev.Data, &payload)

		rc.total.Add(1)
		rc.mu.Lock()
		if rc.perSub == nil {
			rc.perSub = map[string]uint64{}
		}
		rc.perSub[subID]++
		rc.mu.Unlock()
		if werr := rc.lw.Write(recvRecord{
			ReceivedAt:     time.Now().UTC(),
			DeliveryID:     deliveryID,
			EventRef:       eventRef,
			SubscriptionID: subID,
			Seq:            payload.Seq,
			Attempt:        attempt,
		}); werr != nil {
			fmt.Fprintln(os.Stderr, "harness recv: writing -out:", werr)
		}

		if rc.latency > 0 {
			t := time.NewTimer(rc.latency)
			select {
			case <-r.Context().Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		switch {
		case rc.flaky && rc.n.Load()%flaky429Every == 0:
			rc.r429.Add(1)
			w.Header().Set("Retry-After", "1")
			http.Error(w, "too many requests", http.StatusTooManyRequests)
		case rc.errorRate > 0 && rand.Float64() < rc.errorRate:
			rc.err500.Add(1)
			http.Error(w, "simulated failure", http.StatusInternalServerError)
		default:
			rc.ok.Add(1)
			w.WriteHeader(http.StatusOK)
		}
	})
}

func (rc *receiver) summary() {
	rc.mu.Lock()
	subs := len(rc.perSub)
	rc.mu.Unlock()
	fmt.Printf("harness recv: total=%d ok=%d 5xx=%d 429=%d rejected=%d subscriptions=%d elapsed=%.1fs\n",
		rc.total.Load(), rc.ok.Load(), rc.err500.Load(), rc.r429.Load(), rc.reject.Load(),
		subs, time.Since(rc.started).Seconds())
}

// runRecv implements `harness recv`: the simulated webhook receiver of section
// 15.2. It validates every request as a CloudEvents binary binding, records it
// and answers 200, or 500 with probability -error-rate, or an occasional
// 429 + Retry-After when -flaky429 is set. With -offline it takes itself down
// for -offline duration after -offline-after and then comes back on the same
// address, which exercises parking, circuit breaking and draining (section 7).
func runRecv(ctx context.Context, args []string) int {
	fs := newFlagSet("recv")
	addr := fs.String("addr", envStr("ADDR", ":9090"), "listen address (env HARNESS_ADDR)")
	latency := fs.Duration("latency", envDur("LATENCY", 0), "artificial handling latency (env HARNESS_LATENCY)")
	errorRate := fs.Float64("error-rate", envFloat("ERROR_RATE", 0), "probability of answering 500, 0..1 (env HARNESS_ERROR_RATE)")
	offline := fs.Duration("offline", envDur("OFFLINE", 0), "how long to stay down (env HARNESS_OFFLINE)")
	offlineAfter := fs.Duration("offline-after", envDur("OFFLINE_AFTER", 0), "go down this long after start (env HARNESS_OFFLINE_AFTER)")
	out := fs.String("out", envStr("OUT", ""), "append one JSON line per received delivery (env HARNESS_OUT)")
	flaky := fs.Bool("flaky429", envBool("FLAKY429", false), "occasionally answer 429 with Retry-After (env HARNESS_FLAKY429)")
	if code, done := parseResult(fs.Parse(args)); done {
		return code
	}
	if *errorRate < 0 || *errorRate > 1 {
		fmt.Fprintln(os.Stderr, "harness recv: -error-rate must be between 0 and 1")
		return 2
	}
	lw, err := openLines(*out)
	if err != nil {
		fmt.Fprintln(os.Stderr, "harness recv:", err)
		return 1
	}
	defer lw.Close()

	rc := &receiver{latency: *latency, errorRate: *errorRate, flaky: *flaky, lw: lw, started: time.Now()}
	srv := &http.Server{Handler: rc.handler()}
	serveErr := make(chan error, 1)
	start := func() error {
		ln, err := net.Listen("tcp", *addr)
		if err != nil {
			return err
		}
		s := srv
		go func() { serveErr <- s.Serve(ln) }()
		return nil
	}
	if err := start(); err != nil {
		fmt.Fprintln(os.Stderr, "harness recv:", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "harness recv: listening on %s (latency=%v error-rate=%g flaky429=%v)\n",
		*addr, *latency, *errorRate, *flaky)

	// Optional offline window: close the listener after -offline-after, sleep
	// -offline, then listen again on the same address. This simulates a dead
	// endpoint (section 15.3) and exercises parking, the circuit breaker and
	// draining (sections 7 and 8).
	if *offline > 0 && *offlineAfter > 0 {
		fmt.Fprintf(os.Stderr, "harness recv: going offline for %v in %v\n", *offline, *offlineAfter)
		afterT := time.NewTimer(*offlineAfter)
		select {
		case <-ctx.Done():
			afterT.Stop()
		case err := <-serveErr:
			fmt.Fprintln(os.Stderr, "harness recv: server stopped:", err)
			rc.summary()
			return 1
		case <-afterT.C:
			_ = srv.Close() // refuse new connections, reset in-flight ones
			<-serveErr      // the Serve call has returned with ErrServerClosed
			fmt.Fprintln(os.Stderr, "harness recv: OFFLINE")
			offT := time.NewTimer(*offline)
			select {
			case <-ctx.Done():
				offT.Stop()
			case <-offT.C:
			}
			if ctx.Err() == nil {
				srv = &http.Server{Handler: rc.handler()}
				if err := start(); err != nil {
					fmt.Fprintln(os.Stderr, "harness recv: cannot re-bind after offline window:", err)
					rc.summary()
					return 1
				}
				fmt.Fprintln(os.Stderr, "harness recv: back online")
			}
		}
	}

	// Run until Ctrl-C (or a server failure).
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	case err := <-serveErr:
		if err != nil && err != http.ErrServerClosed {
			fmt.Fprintln(os.Stderr, "harness recv: server stopped:", err)
			rc.summary()
			return 1
		}
	}
	fmt.Fprintln(os.Stderr, "harness recv: shutting down")
	rc.summary()
	return 0
}
