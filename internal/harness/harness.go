// Package harness implements the load-test harness of spec section 15. It is
// one binary (cmd/harness) with five subcommands, built together with the spike
// and grown with it:
//
//   - seed    – the subscription generator of section 15.2: creates event types
//     and subscriptions through the HTTP APIs, never through SQL.
//   - pub     – the publisher generator: open-loop load at a configurable rate
//     (section 15.2) with stable ordering keys and per-key sequence numbers.
//   - recv    – simulated webhook receivers with configurable latency, error
//     rates, 429 with Retry-After and offline windows.
//   - sse     – simulated SSE clients with ack behaviour.
//   - verify  – the independent oracle of section 15.1: it recomputes the
//     expected delivery set locally from the published events and the
//     subscription filters and compares it with what the receivers recorded and
//     what the Status API reports. The system never verifies itself.
//
// Every subcommand honours context cancellation (Ctrl-C) and prints a
// concise summary. Tokens are minted in-process with internal/auth (section
// 6.7: token issuance is outside the platform, so no identity provider is
// involved in tests).
package harness

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"pubsub/internal/auth"
	"pubsub/internal/config"
)

const usage = `usage: harness <subcommand> [flags]

subcommands:
  seed    create event types and subscriptions through the HTTP APIs
  pub     open-loop publisher at a configurable rate (spec 15.2)
  recv    simulated webhook receiver (spec 15.2)
  sse     simulated SSE consumer (spec 15.2)
  verify  oracle: expected vs received vs Status API (spec 15.1)

Run 'harness <subcommand> -h' for the flags of a subcommand.
Defaults come from flags; every flag is also settable through the environment:
API, JWT_SECRET, JWT_ISSUER, JWT_AUDIENCE and HARNESS_<FLAG> (upper case).`

// Run executes the harness body and returns the process exit code.
func Run(ctx context.Context, args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	var code int
	switch args[0] {
	case "seed":
		code = runSeed(ctx, args[1:])
	case "pub":
		code = runPub(ctx, args[1:])
	case "recv":
		code = runRecv(ctx, args[1:])
	case "sse":
		code = runSSE(ctx, args[1:])
	case "verify":
		code = runVerify(ctx, args[1:])
	default:
		fmt.Fprintf(os.Stderr, "harness: unknown subcommand %q\n\n", args[0])
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}
	return code
}

// newFlagSet builds a flag set that exits cleanly on -h.
func newFlagSet(name string) *flag.FlagSet {
	return flag.NewFlagSet("harness "+name, flag.ContinueOnError)
}

// parseResult converts a flag parse result into an exit code. -h is success.
func parseResult(err error) (int, bool) {
	if err == nil {
		return 0, false
	}
	if errors.Is(err, flag.ErrHelp) {
		return 0, true
	}
	return 2, true
}

// Environment-backed flag defaults. Every flag has an env twin so that a load
// run can be configured without a shell full of flags.

func envStr(name, def string) string { return config.Str("HARNESS_"+name, def) }

func envInt(name string, def int) int { return config.Int("HARNESS_"+name, def) }

func envBool(name string, def bool) bool { return config.Bool("HARNESS_"+name, def) }

func envDur(name string, def time.Duration) time.Duration {
	return config.Duration("HARNESS_"+name, def)
}

func envFloat(name string, def float64) float64 {
	v := config.Str("HARNESS_"+name, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(fmt.Sprintf("config: HARNESS_%s must be a number, got %q", name, v))
	}
	return f
}

// apiDefault is the base URL of the publish and registry API (SPIKE.md:
// HTTP_ADDR :8080 for both).
func apiDefault() string { return config.Str("API", "http://localhost:8080") }

// mintToken issues the dev JWT used by this subcommand (section 6.7). The
// spike validates HS256 tokens against the shared secret, so the harness needs
// no identity provider at all.
func mintToken(owner string, scopes ...string) (string, error) {
	return auth.Mint(
		config.Str("JWT_SECRET", "dev-secret"),
		config.Str("JWT_ISSUER", "dev"),
		config.Str("JWT_AUDIENCE", "pubsub"),
		owner,
		scopes,
		24*time.Hour,
	)
}

// newHTTPClient builds the shared client. No global timeout: long-running
// streams (SSE) and open-loop publishes bound their requests per request.
func newHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 512,
		IdleConnTimeout:     90 * time.Second,
	}}
}

// doRequest performs one HTTP call and returns the status plus a bounded body.
func doRequest(ctx context.Context, client *http.Client, method, url, token, contentType string, body []byte) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return 0, nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, b, nil
}

// postJSON sends a JSON request; out decodes a 2xx JSON response when set.
func postJSON(ctx context.Context, client *http.Client, url, token string, payload, out any) (int, []byte, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return 0, nil, err
	}
	code, resp, err := doRequest(ctx, client, http.MethodPost, url, token, "application/json", b)
	if err != nil {
		return code, nil, err
	}
	if out != nil && code >= 200 && code < 300 && len(resp) > 0 {
		if err := json.Unmarshal(resp, out); err != nil {
			return code, resp, fmt.Errorf("harness: decoding response of %s: %w", url, err)
		}
	}
	return code, resp, nil
}

// getJSON fetches a JSON document into out.
func getJSON(ctx context.Context, client *http.Client, url, token string, out any) (int, []byte, error) {
	code, resp, err := doRequest(ctx, client, http.MethodGet, url, token, "", nil)
	if err != nil {
		return code, nil, err
	}
	if out != nil && code >= 200 && code < 300 && len(resp) > 0 {
		if err := json.Unmarshal(resp, out); err != nil {
			return code, resp, fmt.Errorf("harness: decoding response of %s: %w", url, err)
		}
	}
	return code, resp, nil
}

// errorBody extracts the {"error": ...} message of an API error response.
func errorBody(b []byte) string {
	var e struct {
		Error string `json:"error"`
	}
	if json.Unmarshal(b, &e) == nil && e.Error != "" {
		return e.Error
	}
	s := strings.TrimSpace(string(b))
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

// decodeList decodes a JSON array response. While the spike services are built
// in parallel the exact envelope is not settled, so a wrapping object with any
// of the given keys is accepted as well.
func decodeList[T any](b []byte, keys ...string) ([]T, error) {
	var direct []T
	if err := json.Unmarshal(b, &direct); err == nil {
		return direct, nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err == nil {
		for _, k := range keys {
			if raw, ok := m[k]; ok {
				var list []T
				if err := json.Unmarshal(raw, &list); err == nil {
					return list, nil
				}
			}
		}
	}
	return nil, fmt.Errorf("harness: response is neither an array nor has any of %s", strings.Join(keys, ","))
}

// lineWriter appends one JSON document per line. A nil writer (empty path)
// discards everything, which keeps the subcommands usable without -out.
type lineWriter struct {
	mu sync.Mutex
	f  *os.File
}

func openLines(path string) (*lineWriter, error) {
	if path == "" {
		return &lineWriter{}, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &lineWriter{f: f}, nil
}

func (w *lineWriter) Write(v any) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	return json.NewEncoder(w.f).Encode(v)
}

func (w *lineWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	return w.f.Close()
}

// bufioScanner builds a scanner that tolerates large lines (events with a
// padded payload arrive as one SSE data line or JSON line).
func bufioScanner(r io.Reader, max int) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), max)
	return sc
}

// loadLines reads a JSON-lines file written by pub or recv.
func loadLines[T any](path string) ([]T, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufioScanner(f, 16<<20)
	var out []T
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var v T
		if err := json.Unmarshal(line, &v); err != nil {
			return nil, fmt.Errorf("harness: %s: %w", path, err)
		}
		out = append(out, v)
	}
	return out, sc.Err()
}
