// Command harness is the load-test harness of spec section 15, built together
// with the spike and grown with it (section 15.1: the system never verifies
// itself). It has five subcommands:
//
//	harness seed   create the test topology through the HTTP APIs
//	harness pub    open-loop publisher at a configurable rate (section 15.2)
//	harness recv   simulated webhook receiver (latency, errors, offline)
//	harness sse    simulated SSE consumer with acks
//	harness verify independent oracle: expected vs received vs Status API
//
// Usage is printed when no subcommand is given; each subcommand prints its own
// flags with -h. Defaults come from flags, every flag also from the
// environment (API, JWT_SECRET, JWT_ISSUER, JWT_AUDIENCE, HARNESS_<FLAG>).
// Tokens are minted in-process with internal/auth, so no identity provider is
// involved (section 6.7: token issuance is outside the platform). Ctrl-C
// cancels any subcommand gracefully and still prints its summary.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"pubsub/internal/harness"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(harness.Run(ctx, os.Args[1:]))
}
