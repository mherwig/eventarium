package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The shape franz-go returns when Begin fails because the producer ID is
// dead: the wrapper added in BeginTransaction around whatever
// maybeRecoverProducerID could not recover. PRODUCER_FENCED is what a broker
// restart produced live - r2 took it at 20:52:20 and never routed another
// event until the process was restarted twelve minutes later, so this case
// has to be recognized rather than reasoned about.
func TestFatalTxnErrRecognisesAnUnrecoverableProducer(t *testing.T) {
	fenced := fmt.Errorf("producer ID has a fatal, unrecoverable error, err: %w", kerr.ProducerFenced)
	if !fatalTxnErr(fenced) {
		t.Fatal("a fenced producer was not treated as fatal: the router would retry it forever")
	}

	// Not every unrecoverable code is PRODUCER_FENCED, and the whole point of
	// matching franz-go's wrapper instead of a code list is that new ones are
	// covered without this test knowing about them.
	unknown := fmt.Errorf("producer ID has a fatal, unrecoverable error, err: %w", kerr.UnknownProducerID)
	if !fatalTxnErr(unknown) {
		t.Fatal("an unrecoverable producer-ID error was not treated as fatal")
	}
}

// The case the existing abort-and-retry path exists for. It must keep being
// retried: an open transaction is cleared by TryAbort, and exiting over it
// would turn a self-healing condition into a restart on every occurrence.
func TestFatalTxnErrLeavesAnOpenTransactionRetriable(t *testing.T) {
	inTxn := errors.New("invalid attempt to begin a transaction while already in a transaction")
	if fatalTxnErr(inTxn) {
		t.Fatal("an open transaction was treated as fatal: the abort path would never run")
	}
}

// A transient failure during a broker restart must not be fatal either - the
// escalating backoff exists precisely for it, and franz-go flags these for a
// producer-ID reload rather than reporting them as unrecoverable.
func TestFatalTxnErrLeavesTransientFailuresRetriable(t *testing.T) {
	for _, err := range []error{
		nil,
		errors.New("dial tcp 127.0.0.1:9092: connect: connection refused"),
		kerr.CoordinatorLoadInProgress,
		fmt.Errorf("beginning transaction: %w", kerr.NotCoordinator),
	} {
		if fatalTxnErr(err) {
			t.Errorf("fatalTxnErr(%v) = true, want false: a retriable failure would take the router down", err)
		}
	}
}
