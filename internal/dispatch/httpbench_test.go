package dispatch

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// The dispatcher's delivery client sets no Transport, so it inherits
// http.DefaultTransport - and DefaultMaxIdleConnsPerHost is 2. Every
// subscription gets defaultMaxInFlight (8) workers, and subscriptions that
// share an endpoint share a host, so one customer's endpoint can be the target
// of far more concurrent requests than the transport keeps idle connections
// for. Anything past that pool dials a fresh connection per request.
//
// This measures exactly that: the same 136 concurrent workers (17
// subscriptions x 8) against one local endpoint, with only the idle-pool size
// changed. ns/op is wall time over total requests, so it is the inverse of
// throughput - lower is better, and the ratio between the two runs is the
// whole effect.
//
// Localhost understates it: a loopback re-dial is a syscall or two, while a
// real endpoint over TLS pays a handshake (typically 50-100 ms) on every
// miss. A small gap here is the floor of the production gap, not the ceiling.
const benchWorkers = 17 * 8 // defaultMaxInFlight, x the subscriptions sharing one endpoint

// benchTransport clones the default rather than using http.DefaultTransport
// directly: that is a process-wide singleton, and two sub-benchmarks sharing
// one idle pool would contaminate each other.
func benchTransport(idlePerHost int) *http.Transport {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = idlePerHost
	return tr
}

func BenchmarkDeliveryHTTP(b *testing.B) {
	payload := bytes.Repeat([]byte("x"), 1024) // 1 KiB, as -payload 1024

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	b.Logf("%d concurrent workers against one host; idle conns per host is the only variable",
		benchWorkers)

	for _, idle := range []int{2, 128} {
		idle := idle
		name := "default_2_idle_per_host"
		if idle != 2 {
			name = "tuned_128_idle_per_host"
		}
		b.Run(name, func(b *testing.B) {
			tr := benchTransport(idle)
			defer tr.CloseIdleConnections()
			cl := &http.Client{Transport: tr} // Timeout 0, same as dispatch.go

			perWorker := (b.N + benchWorkers - 1) / benchWorkers
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()

			var (
				wg   sync.WaitGroup
				mu   sync.Mutex
				errs int
			)
			for w := 0; w < benchWorkers; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						req, err := http.NewRequest(http.MethodPost, srv.URL+"/hook",
							bytes.NewReader(payload))
						if err != nil {
							mu.Lock()
							errs++
							mu.Unlock()
							return
						}
						resp, err := cl.Do(req)
						if err != nil {
							mu.Lock()
							errs++
							mu.Unlock()
							return
						}
						_, _ = io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
					}
				}()
			}
			wg.Wait()
			b.StopTimer()
			if errs > 0 {
				b.Fatalf("%d request(s) failed", errs)
			}
		})
	}
}
