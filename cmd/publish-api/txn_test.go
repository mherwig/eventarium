package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// fakePlain stands in for the non-transactional client on the single-record
// path. It records how often it was called so a test can prove that path never
// touches the transaction.
type fakePlain struct {
	failAt int // index of the record that fails, -1 for none
	calls  int
}

func (f *fakePlain) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.calls++
	out := make(kgo.ProduceResults, 0, len(rs))
	for i, r := range rs {
		var err error
		if i == f.failAt {
			err = errors.New("broker down")
		}
		out = append(out, kgo.ProduceResult{Record: r, Err: err})
	}
	return out
}

// fakeTxn stands in for the transactional client. It records the order of the
// protocol and whether two transactions were ever open at once, which is the
// property the mutex in writeBatch exists to hold and that nothing else in the
// test suite can observe (there is no -race here: no gcc on this machine).
type fakeTxn struct {
	failAt    int // index of the record that fails, -1 for none
	beginErr  error
	commitErr error
	abortErr  error
	hang      time.Duration // how long ProduceSync blocks, to force overlap

	mu       sync.Mutex
	order    []string
	inFlight int
	maxInOne int
}

func (f *fakeTxn) note(ev string) {
	f.mu.Lock()
	f.order = append(f.order, ev)
	f.mu.Unlock()
}

func (f *fakeTxn) sequence() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.order...)
}

func (f *fakeTxn) BeginTransaction() error {
	if f.beginErr != nil {
		f.note("begin-err")
		return f.beginErr
	}
	f.note("begin")
	f.mu.Lock()
	f.inFlight++
	if f.inFlight > f.maxInOne {
		f.maxInOne = f.inFlight
	}
	f.mu.Unlock()
	return nil
}

func (f *fakeTxn) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	f.note("produce")
	if f.hang > 0 {
		time.Sleep(f.hang)
	}
	out := make(kgo.ProduceResults, 0, len(rs))
	for i, r := range rs {
		var err error
		if i == f.failAt {
			err = errors.New("broker closed the connection")
		}
		out = append(out, kgo.ProduceResult{Record: r, Err: err})
	}
	return out
}

func (f *fakeTxn) EndTransaction(_ context.Context, try kgo.TransactionEndTry) error {
	f.mu.Lock()
	if try == kgo.TryCommit {
		f.order = append(f.order, "commit")
	} else {
		f.order = append(f.order, "abort")
	}
	f.inFlight--
	f.mu.Unlock()
	if try == kgo.TryCommit {
		return f.commitErr
	}
	return f.abortErr
}

func records(n int) []*kgo.Record {
	out := make([]*kgo.Record, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, &kgo.Record{Topic: "events-short", Offset: int64(i)})
	}
	return out
}

// apiFor wires a publishAPI onto the fakes: the plain client never fails so
// the single-record tests are about which path is taken, not about its errors.
func apiFor(txn transactionalProducer) (*publishAPI, *fakePlain) {
	plain := &fakePlain{failAt: -1}
	return &publishAPI{txn: txn, kafka: plain}, plain
}

// TestSingleRecordSkipsTheTransaction pins the split this change is built on:
// one record is atomic by itself, so the usual case keeps the plain path and
// the transactional client is never entered. Paying a transaction's extra
// round trip for the common publish would be the wrong trade.
func TestSingleRecordSkipsTheTransaction(t *testing.T) {
	txn := &fakeTxn{failAt: -1}
	api, plain := apiFor(txn)

	if err := api.write(context.Background(), records(1)); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := txn.sequence(); len(got) != 0 {
		t.Errorf("transactional client was used for a single record: %v, want no calls", got)
	}
	if plain.calls != 1 {
		t.Errorf("plain client produced %d times, want 1", plain.calls)
	}
}

// TestBatchCommitsOnlyWhenEverythingProduces is the happy path: begin,
// produce, commit - and never an abort.
func TestBatchCommitsOnlyWhenEverythingProduces(t *testing.T) {
	txn := &fakeTxn{failAt: -1}
	api, _ := apiFor(txn)

	if err := api.write(context.Background(), records(100)); err != nil {
		t.Fatalf("write: %v", err)
	}
	want := []string{"begin", "produce", "commit"}
	if got := txn.sequence(); !equal(got, want) {
		t.Errorf("sequence = %v, want %v", got, want)
	}
}

// TestBatchAbortsOnOneProduceError is the bug this whole change exists for.
// One record of a hundred failing must discard the other ninety-nine, not
// commit them and then tell the caller the request failed.
func TestBatchAbortsOnOneProduceError(t *testing.T) {
	txn := &fakeTxn{failAt: 57}
	api, _ := apiFor(txn)

	err := api.write(context.Background(), records(100))
	if err == nil {
		t.Fatal("write succeeded with a failing record")
	}
	if errors.Is(err, errUnconfirmedWrite) {
		t.Error("a produce failure with no commit attempted is a known outcome, not an unknown one")
	}
	want := []string{"begin", "produce", "abort"}
	if got := txn.sequence(); !equal(got, want) {
		t.Errorf("sequence = %v, want %v - a commit after a produce error is the partial-batch bug", got, want)
	}
	msg := writeFailure(100, err)
	if !strings.Contains(msg, "no event of this request was written") {
		t.Errorf("message for a batch = %q, must state that nothing was written", msg)
	}
}

// TestBatchAbortsWhenTheCallerWalksAway keeps the two sides agreeing. A
// caller whose request context died has seen no 201 and will retry, so
// committing here would write events it believes failed and duplicate them on
// the retry - the disagreement in the other direction.
func TestBatchAbortsWhenTheCallerWalksAway(t *testing.T) {
	txn := &fakeTxn{failAt: -1}
	api, _ := apiFor(txn)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the caller is gone before the commit

	if err := api.write(ctx, records(50)); err == nil {
		t.Fatal("write succeeded for an abandoned request")
	}
	want := []string{"begin", "produce", "abort"}
	if got := txn.sequence(); !equal(got, want) {
		t.Errorf("sequence = %v, want %v", got, want)
	}
}

// TestCommitFailureIsRolledBack covers the documented franz-go recovery: a
// refused commit leaves the transaction open, so the rollback that follows has
// to happen, and once it succeeds the caller can still be told nothing was
// written.
func TestCommitFailureIsRolledBack(t *testing.T) {
	txn := &fakeTxn{failAt: -1, commitErr: errors.New("coordinator refused the commit")}
	api, _ := apiFor(txn)

	err := api.write(context.Background(), records(10))
	if err == nil {
		t.Fatal("write succeeded despite a failed commit")
	}
	want := []string{"begin", "produce", "commit", "abort"}
	if got := txn.sequence(); !equal(got, want) {
		t.Errorf("sequence = %v, want %v - an unrolled-back transaction would block every later batch", got, want)
	}
	if errors.Is(err, errUnconfirmedWrite) {
		t.Error("rollback succeeded, so the outcome is known: nothing was written")
	}
	if msg := writeFailure(10, err); !strings.Contains(msg, "no event of this request was written") {
		t.Errorf("message = %q", msg)
	}
}

// TestUnconfirmedOutcomeIsReportedHonestly is the one case where a 500 cannot
// say "nothing was written": the commit went on the wire and neither it nor
// the rollback could be confirmed. Dressing that up as failure would be the
// same lie this change removes, just rarer and whole-batch instead of partial.
func TestUnconfirmedOutcomeIsReportedHonestly(t *testing.T) {
	txn := &fakeTxn{
		failAt:    -1,
		commitErr: errors.New("commit response lost"),
		abortErr:  errors.New("broker unreachable"),
	}
	api, _ := apiFor(txn)

	err := api.write(context.Background(), records(10))
	if !errors.Is(err, errUnconfirmedWrite) {
		t.Fatalf("err = %v, want errUnconfirmedWrite", err)
	}
	msg := writeFailure(10, err)
	if !strings.Contains(msg, "may or may not") {
		t.Errorf("message = %q, must not claim nothing was written", msg)
	}
	seq := txn.sequence()
	if seq[len(seq)-1] != "abort" {
		t.Errorf("sequence = %v, want the rollback to have been attempted", seq)
	}
	if !api.txnStale {
		t.Error("an unconfirmed commit must mark the transaction stale so the next request clears it")
	}
}

// TestFailedProduceWithFailedRollbackIsStillKnown is the mirror of the above
// and the reason the condition is "a commit was attempted" rather than "did
// anything produce". No commit was ever attempted, so the records sit in an
// open transaction the broker will drop at the transaction timeout; nothing
// can make them visible, and the caller can be told so.
func TestFailedProduceWithFailedRollbackIsStillKnown(t *testing.T) {
	txn := &fakeTxn{failAt: 0, abortErr: errors.New("broker unreachable")}
	api, _ := apiFor(txn)

	err := api.write(context.Background(), records(10))
	if err == nil {
		t.Fatal("write succeeded with a failing produce")
	}
	if errors.Is(err, errUnconfirmedWrite) {
		t.Error("no commit was attempted, so the outcome is known: nothing was written")
	}
	if msg := writeFailure(10, err); !strings.Contains(msg, "no event of this request was written") {
		t.Errorf("message = %q", msg)
	}
	if !api.txnStale {
		t.Error("a failed rollback must mark the transaction stale, or every later batch fails to begin")
	}
}

// TestStaleTransactionIsClearedFirst is the self-healing half of the above:
// one bad abort must not disable the batch path for the rest of the process's
// life. The next request rolls the leftover transaction back before beginning
// its own.
func TestStaleTransactionIsClearedFirst(t *testing.T) {
	txn := &fakeTxn{failAt: -1}
	api, _ := apiFor(txn)
	api.txnStale = true

	if err := api.write(context.Background(), records(10)); err != nil {
		t.Fatalf("write after a stale transaction: %v", err)
	}
	want := []string{"abort", "begin", "produce", "commit"}
	if got := txn.sequence(); !equal(got, want) {
		t.Errorf("sequence = %v, want %v - begin without the clearing abort fails outright", got, want)
	}
	if api.txnStale {
		t.Error("the clearing rollback succeeded, so txnStale must be clear")
	}
}

// TestStaleTransactionThatCannotBeClearedFailsFast: if even the rollback does
// not work, say so rather than attempting a BeginTransaction that is guaranteed
// to fail with a confusing "already in a transaction".
func TestStaleTransactionThatCannotBeClearedFailsFast(t *testing.T) {
	txn := &fakeTxn{failAt: -1, abortErr: errors.New("broker unreachable")}
	api, _ := apiFor(txn)
	api.txnStale = true

	err := api.write(context.Background(), records(10))
	if err == nil {
		t.Fatal("write succeeded with a stale, unclosable transaction")
	}
	if !strings.Contains(err.Error(), "still open") {
		t.Errorf("err = %v, want it to name the leftover transaction", err)
	}
	if got := txn.sequence(); !equal(got, []string{"abort"}) {
		t.Errorf("sequence = %v, want only the clearing abort - no begin, no produce", got)
	}
}

// TestBatchesNeverOverlap is the property the mutex exists for: a producer has
// exactly one transaction in flight and BeginTransaction must not be called
// concurrently with the client's other work. Two batches in parallel would
// interleave begin/commit across each other and one would abort the other's
// records. There is no -race detector available here, so the test observes it
// directly.
func TestBatchesNeverOverlap(t *testing.T) {
	txn := &fakeTxn{failAt: -1, hang: 50 * time.Millisecond}
	api, _ := apiFor(txn)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := api.write(context.Background(), records(5)); err != nil {
				t.Errorf("write: %v", err)
			}
		}()
	}
	wg.Wait()

	txn.mu.Lock()
	max := txn.maxInOne
	txn.mu.Unlock()
	if max != 1 {
		t.Errorf("%d transactions were open at once, want exactly 1", max)
	}
	if got := len(txn.sequence()); got != 12 {
		t.Errorf("recorded %d protocol steps, want 4 batches x (begin, produce, commit) = 12", got)
	}
}

// TestWriteFailureWording pins the three distinct messages, because the value
// of the whole change is that the reply says something true.
func TestWriteFailureWording(t *testing.T) {
	batchErr := errors.New("events-short: broker down")
	if msg := writeFailure(100, batchErr); !strings.Contains(msg, "no event of this request was written") {
		t.Errorf("batch failure = %q", msg)
	}
	if msg := writeFailure(1, batchErr); msg != "producing event failed" {
		t.Errorf("single-record failure = %q, want the plain message: one record has no partial case", msg)
	}
	if msg := writeFailure(100, errUnconfirmedWrite); !strings.Contains(msg, "may or may not") {
		t.Errorf("unconfirmed = %q", msg)
	}
}

// TestLatchedClientIsReplacedSoTheBatchPathCannotDie is the recovery for the
// failure the kafka chaos run found. franz-go latches a connection that dies
// during ApiVersions - what a broker restart does - into a producer-ID state
// it calls "fatal, unrecoverable", and then fails BeginTransaction instantly
// and forever; only a new client re-runs InitProducerID. Observed live: 99
// latched calls, 9,900 of 12,000 requests failed in 0.5ms each, while the
// single-record path was unaffected. The test asserts the latch is replaced
// and that the *same* request then succeeds rather than being sacrificed to
// detect it.
func TestLatchedClientIsReplacedSoTheBatchPathCannotDie(t *testing.T) {
	latched := &fakeTxn{beginErr: errors.New("producer ID has a fatal, unrecoverable error, err: EOF")}
	fresh := &fakeTxn{failAt: -1}
	api, _ := apiFor(latched)

	oldClosed, newClosed := 0, 0
	api.txnClose = func() { oldClosed++ }
	api.openTxn = func() (transactionalProducer, func(), error) {
		return fresh, func() { newClosed++ }, nil
	}

	if err := api.write(context.Background(), records(10)); err != nil {
		t.Fatalf("a batch must survive a latched client, got: %v", err)
	}
	if oldClosed != 1 {
		t.Errorf("the latched client was closed %d times, want 1", oldClosed)
	}
	if got, want := fresh.sequence(), []string{"begin", "produce", "commit"}; !equal(got, want) {
		t.Errorf("replacement sequence = %v, want %v", got, want)
	}
	if got := latched.sequence(); !equal(got, []string{"begin-err"}) {
		t.Errorf("latched client sequence = %v, want only its failed begin", got)
	}

	// Shutdown closes whichever client is current - the replacement, not the
	// one main opened.
	api.closeTxn()
	if newClosed != 1 || oldClosed != 1 {
		t.Errorf("after closeTxn: old=%d new=%d, want 1 and 1", oldClosed, newClosed)
	}
}

// TestReplacementThatFailsToOpenIsRetried covers the second half of the same
// problem: if the replacement itself cannot be opened (the broker is still
// down), the client ends up nil - and "no client" must be a transient state
// the next request recovers from, not a permanent one. Otherwise an outage
// would kill the batch path just as effectively as the latch does.
func TestReplacementThatFailsToOpenIsRetried(t *testing.T) {
	latched := &fakeTxn{beginErr: errors.New("producer ID has a fatal, unrecoverable error, err: EOF")}
	fresh := &fakeTxn{failAt: -1}
	api, _ := apiFor(latched)

	api.txnClose = func() {}
	opens := 0
	api.openTxn = func() (transactionalProducer, func(), error) {
		opens++
		if opens == 1 {
			return nil, nil, errors.New("no brokers reachable")
		}
		return fresh, func() {}, nil
	}

	if err := api.write(context.Background(), records(10)); err == nil {
		t.Fatal("write succeeded with no client able to open")
	}
	if opens != 1 {
		t.Errorf("openTxn called %d times on the first write, want 1", opens)
	}

	// The broker is back: the next request must open a client and work.
	if err := api.write(context.Background(), records(10)); err != nil {
		t.Fatalf("the batch path did not recover once a client could open: %v", err)
	}
	if opens != 2 {
		t.Errorf("openTxn called %d times in total, want 2", opens)
	}
	if got, want := fresh.sequence(), []string{"begin", "produce", "commit"}; !equal(got, want) {
		t.Errorf("sequence = %v, want %v", got, want)
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
