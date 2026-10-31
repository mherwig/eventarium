// Publish API ingests CloudEvents and writes them, once per retention class
// topic, to the event log. It implements the publish contract of section 6.1
// on top of the event model of section 5: HTTP binary, structured and batch
// mode (F1), rejection of unknown event types (F20), no deduplication of
// republished messages (F19), and an owner reference taken from the JWT and
// never from the event body (section 6.7).
//
// Spike deviation: the registry is polled from PostgreSQL every 2 s. In
// production it would be distributed to the publish API through a compacted
// Kafka config topic (section 6.1), so that a publish needs no database call
// at all; only the source of the in-memory index changes later.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"pubsub/internal/auth"
	"pubsub/internal/cloudevents"
	"pubsub/internal/config"
	"pubsub/internal/httpx"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/store"
)

func main() {
	setupLogging()
	addr := config.Str("HTTP_ADDR", ":8080")
	dsn := config.Str("PG_DSN", "postgres://pubsub:pubsub@localhost:5432/pubsub?sslmode=disable")
	// KAFKA_BROKERS is read through internal/config and handed to the client
	// explicitly so that every environment read of this binary goes via config.
	brokers := splitBrokers(config.Str("KAFKA_BROKERS", "localhost:9092"))
	maxBytes := int64(config.Int("MAX_EVENT_BYTES", cloudevents.DefaultMaxBytes))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, dsn)
	if err != nil {
		fatal("opening store", err)
	}
	defer st.Close()

	// The registry reads the control data through the store interface. The
	// NOTIFY trigger (migration 003) refreshes the index within milliseconds
	// of an event type or subscription changing; REGISTRY_POLL is the safety
	// net for notifications missed while the listener was down (section 6.1).
	reg := registry.New(st, config.Duration("REGISTRY_POLL", 2*time.Second), registry.WithNotify(dsn))
	if err := reg.Reload(ctx); err != nil {
		slog.Warn("initial registry load failed", "err", err)
	}
	go reg.Run(ctx)

	client, err := kafkautil.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		fatal("connecting to kafka", err)
	}
	defer client.Close()

	// Batches get their own client: a transactional client refuses every
	// produce made outside a transaction, so it cannot share the single-event
	// path. The ID and the replacement rules live with openTxnClient.
	txnClient, err := openTxnClient(brokers)
	if err != nil {
		fatal("connecting the transactional kafka client", err)
	}

	validator, err := auth.NewHMAC(
		config.Str("JWT_SECRET", "dev-secret"),
		config.Str("JWT_ISSUER", "dev"),
		config.Str("JWT_AUDIENCE", "pubsub"),
	)
	if err != nil {
		fatal("building token validator", err)
	}

	api := &publishAPI{
		auth:     validator,
		registry: reg,
		kafka:    client,
		txn:      txnClient,
		txnClose: txnClient.Close,
		openTxn: func() (transactionalProducer, func(), error) {
			cl, err := openTxnClient(brokers)
			if err != nil {
				return nil, nil, err
			}
			return cl, cl.Close, nil
		},
		maxBytes: maxBytes,
	}
	// Deliberately api.closeTxn and not txnClient.Close: a latched client may
	// have been replaced since, and this closes whichever one is current.
	defer api.closeTxn()
	// Readiness is more than "the process is up": the publish path needs
	// PostgreSQL (the registry poll) and an index that is not standing still.
	// /healthz stays answering while they are down - restarting the process
	// would not fix a database - but /readyz then fails, so a rolling update
	// does not add a replica that cannot serve (section: docs/SPIKE.md).
	health := httpx.NewHealth(
		func(ctx context.Context) error { return st.Pool.Ping(ctx) },
		func(context.Context) error {
			return reg.Check(config.Duration("REGISTRY_STALE_AFTER", registry.DefaultStaleAfter))
		},
	)
	mux := http.NewServeMux()
	health.Register(mux)
	mux.HandleFunc("POST /v1/events", api.handleEvents)
	// Catch-all so that unknown paths answer with the platform's JSON error
	// shape instead of the mux's plain text default.
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		httpx.Error(w, http.StatusNotFound, "not found")
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpx.AccessLog(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		stop() // a second signal falls back to the default: hard exit
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("http shutdown", "err", err)
		}
	}()
	// The initial registry load happened above and the Kafka client is
	// connected; from here on, readiness is only about the dependencies.
	health.SetReady(true)
	slog.Info("publish-api listening", "addr", addr, "brokers", brokers, "max_event_bytes", maxBytes)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal("serving http", err)
	}
	slog.Info("publish-api stopped")
}

const (
	// batchProduceTimeout bounds the produce phase of a transactional batch.
	// franz-go retries a failing produce for 30s by default and a transaction
	// is dropped by the broker after 40s, so a batch still running past this
	// point would be doomed anyway - and while it waited it would hold the
	// batch lock, queueing every other batch behind it. Turning "the broker
	// is down" into a 15s 500 instead of a 30s stall that then loses the
	// transaction is the better trade.
	batchProduceTimeout = 15 * time.Second

	// txnEndTimeout bounds the commit and the rollback. franz-go asks that
	// these contexts not be cancelled underneath the call - a cancelled
	// EndTransaction leaves the outcome unknowable - so they are deliberately
	// independent of the HTTP request's context.
	txnEndTimeout = 10 * time.Second
)

// errUnconfirmedWrite marks a failure where the transaction could not be
// rolled back and so could not be ruled out either. The caller must not be
// told "nothing was written" here: that would be the same lie this change
// removes, just rarer and whole-batch instead of partial.
var errUnconfirmedWrite = errors.New("the transaction could not be rolled back, so the outcome is unknown")

// producer is the slice of a Kafka client this service uses. It exists so the
// write paths can be exercised without a broker; *kgo.Client implements both
// it and transactionalProducer.
type producer interface {
	ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults
}

// transactionalProducer adds the transaction protocol on top.
type transactionalProducer interface {
	producer
	BeginTransaction() error
	EndTransaction(ctx context.Context, try kgo.TransactionEndTry) error
}

// publishAPI holds the dependencies of the single write endpoint.
type publishAPI struct {
	auth     *auth.Validator
	registry *registry.Registry
	// kafka is the plain client, used for single-record publishes. One record
	// is atomic on its own - it lands or it does not - so the usual case
	// keeps the path it always had and pays for no transaction.
	kafka producer
	// txn is the transactional client, used for batches. It cannot serve
	// single records: a transactional client fails every produce made outside
	// a transaction, which is why this is a second client rather than a flag
	// on the first.
	txn transactionalProducer
	// txnClose disposes of the current transactional client and openTxn builds
	// a replacement. Both are needed because a transactional client can be
	// latched into a state it never leaves (see beginTxn), and only a new one
	// recovers. main wires them; tests inject the client and leave them nil.
	txnClose func()
	openTxn  func() (transactionalProducer, func(), error)
	// txnMu serialises batches. A producer runs one transaction at a time and
	// BeginTransaction must not be called concurrently with the client's other
	// work; single-record publishes use kafka and are never delayed by it.
	txnMu sync.Mutex
	// txnStale records a rollback that failed, which leaves the client still
	// inside a transaction and every later BeginTransaction failing with it.
	// Without clearing it first, one bad abort would take the batch path down
	// for the rest of the process's life (see writeBatch). It is set only
	// after a successful Begin, so it always implies a live client to roll
	// back - beginTxn returns before the deferred rollback is armed, which is
	// what keeps "stale" and "no client" from ever being true together.
	txnStale bool
	maxBytes int64
}

// publishResult is one entry of the 201 response (section 6.1).
type publishResult struct {
	EventRef       string `json:"event_ref"`
	EventType      string `json:"event_type"`
	RetentionClass string `json:"retention_class"`
	ExpiresAt      string `json:"expires_at"`
	// Warning is absent unless nothing will be delivered (see
	// noSubscriberWarning). The normal response is therefore unchanged.
	Warning string `json:"warning,omitempty"`
}

// noSubscriberWarning tells a publisher that an event type nobody is
// subscribed to was accepted: it will be stored and routed to nothing, which
// is otherwise only visible later, as an absent delivery.
//
// The check is the registry the publish API already holds for the 422 of an
// unknown event type, so it is advisory - a snapshot that is refreshed on
// change - and it counts *candidates*, subscriptions registered for the type.
// It cannot say how many will pass their CEL filters; that answer lands in the
// status store as `filtered` a moment later, once the router has seen the
// event. Reporting a candidate count would read as a delivery forecast, so
// only the actionable case, zero, is reported at all.
func noSubscriberWarning(reg *registry.Registry, owner, eventType string) string {
	if len(reg.Match(owner, eventType)) > 0 {
		return ""
	}
	return fmt.Sprintf("no subscription is registered for event type %q: the event is accepted and stored, but nothing will be delivered", eventType)
}

// handleEvents implements POST /v1/events: one event in binary or structured
// mode, or many in batch mode (section 5).
func (a *publishAPI) handleEvents(w http.ResponseWriter, r *http.Request) {
	claims, ok := a.auth.Authorize(w, r, auth.ScopePublish)
	if !ok {
		return
	}
	events, err := cloudevents.Read(r, a.maxBytes)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return
	}

	// Section 6.7: the owner comes only from the token, never from the body.
	owner := claims.Owner
	// Millisecond truncation keeps the stamped times consistent across
	// PostgreSQL, Kafka headers and ClickHouse (section 9).
	ingestedAt := time.Now().UTC().Truncate(time.Millisecond)

	// Validate every event before producing anything, so that a batch request
	// is rejected as a whole.
	records := make([]*kgo.Record, 0, len(events))
	results := make([]publishResult, 0, len(events))
	for _, e := range events {
		et, ok := a.registry.EventType(owner, e.Type)
		if !ok {
			// F20: no auto-registration, the whole request is rejected.
			httpx.Error(w, http.StatusUnprocessableEntity,
				fmt.Sprintf("unknown event type %q", e.Type))
			return
		}
		if e.DataContentType == "" {
			e.DataContentType = et.ContentType
		}
		if et.IsJSON() && e.HasData() {
			// Section 5: a JSON event type must carry a JSON object as data.
			var obj map[string]any
			if err := decodeJSONObject(e.Data, &obj); err != nil {
				httpx.Error(w, http.StatusUnprocessableEntity,
					fmt.Sprintf("data of JSON event type %q must be a JSON object: %v", e.Type, err))
				return
			}
		}

		eventRef := uuid.NewString()
		expiresAt := retentionExpiry(et.RetentionClass, ingestedAt)
		pkey := partitionKey(e)
		value, err := json.Marshal(e)
		if err != nil {
			slog.Error("marshalling event", "ref", eventRef, "err", err)
			httpx.Error(w, http.StatusInternalServerError, "internal error")
			return
		}
		records = append(records, &kgo.Record{
			Topic: kafkautil.EventsTopic(et.RetentionClass),
			Key:   []byte(pkey),
			Value: value,
			Headers: recordHeaders(
				kafkautil.HSet(kafkautil.HeaderEventRef, eventRef),
				kafkautil.HSet(kafkautil.HeaderOwner, owner),
				kafkautil.HSet(kafkautil.HeaderClass, et.RetentionClass),
				kafkautil.HSet(kafkautil.HeaderExpires, strconv.FormatInt(expiresAt.UnixMilli(), 10)),
				kafkautil.HSet(kafkautil.HeaderIngested, strconv.FormatInt(ingestedAt.UnixMilli(), 10)),
				kafkautil.HSet(kafkautil.HeaderPKey, pkey),
			),
		})
		results = append(results, publishResult{
			EventRef:       eventRef,
			EventType:      e.Type,
			RetentionClass: et.RetentionClass,
			ExpiresAt:      expiresAt.Format(time.RFC3339),
			Warning:        noSubscriberWarning(a.registry, owner, e.Type),
		})
	}

	// F19: publish is not deduplicated. Publishing the same message again is
	// allowed and simply creates another event with a new reference.
	//
	// A batch is written in one Kafka transaction, so a 500 can be trusted to
	// mean nothing of the request was written — see writeBatch.
	if err := a.write(r.Context(), records); err != nil {
		slog.Error("producing event", "err", err, "events", len(records))
		httpx.Error(w, http.StatusInternalServerError, writeFailure(len(records), err))
		return
	}
	for _, res := range results {
		slog.Debug("event accepted", "owner", owner, "ref", res.EventRef,
			"type", res.EventType, "class", res.RetentionClass)
	}

	// A batch request is answered with an array, single events with one object.
	if isBatch(r) {
		httpx.JSON(w, http.StatusCreated, results)
		return
	}
	httpx.JSON(w, http.StatusCreated, results[0])
}

// openTxnClient builds a transactional client for batches. The transactional
// ID is stable per process so a replacement client fences whatever the previous
// one left behind - and must be unique to this process, because two producers
// sharing one fence each other into permanent ProducerFenced errors, which at N
// replicas would be a self-inflicted outage.
func openTxnClient(brokers []string) (*kgo.Client, error) {
	return kafkautil.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.TransactionalID(txnID()),
	)
}

// txnID is this process's transactional ID; see openTxnClient for the rules it
// follows and why fencing a *previous* run is deliberately not a goal (a
// zombie publish-api committing its own well-formed events is harmless,
// unlike a stale consume-modify-produce session).
func txnID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return "publish-api-" + host + "-" + strconv.Itoa(os.Getpid())
}

// write puts every record of one request into Kafka. One record needs no
// transaction — there is nothing for it to be partial about — so the usual
// case keeps the plain path and pays for nothing extra. Two or more go in a
// single transaction, so the request lands completely or not at all.
func (a *publishAPI) write(ctx context.Context, records []*kgo.Record) error {
	if len(records) < 2 {
		return firstProduceError(a.kafka.ProduceSync(ctx, records...))
	}
	return a.writeBatch(ctx, records)
}

// writeBatch produces the request's records inside one Kafka transaction.
//
// What it replaces: the handler produced the whole request and then returned
// 500 on the first record that errored — by which time the rest were already
// in the log and on their way to subscribers, while the caller was told
// "failed" for events it could find in the platform. An F19-compliant retry
// then duplicated every one of them. Measured on the `kafka` chaos run: 1,000
// events reported failed, 533 of their references seen at the receiver,
// 5,185 deliveries nobody expected.
//
// Here a produce error aborts instead of committing, so none of the request
// becomes visible. The one thing this cannot fix is a commit or a rollback
// whose own confirmation is lost on the wire: that is reported as
// errUnconfirmedWrite rather than dressed up as "failed".
func (a *publishAPI) writeBatch(ctx context.Context, records []*kgo.Record) (err error) {
	// One transaction at a time: a producer has exactly one in flight and
	// BeginTransaction must not be called concurrently with the client's other
	// work. Batches queue here; single-record publishes use the other client
	// and are never held up by this lock.
	a.txnMu.Lock()
	defer a.txnMu.Unlock()

	// A rollback that failed leaves the client still "in a transaction", and
	// every BeginTransaction after that fails with it — one bad abort would
	// otherwise disable the batch path for the rest of the process's life.
	// Clear it first; if that fails too, report and let the next request try.
	if a.txnStale {
		if abErr := a.endTxn(kgo.TryAbort); abErr != nil {
			return fmt.Errorf("the previous batch is still open and could not be rolled back: %w", abErr)
		}
		a.txnStale = false
	}

	if err := a.beginTxn(); err != nil {
		// Nothing was produced, so this is a known outcome regardless of how
		// bad the message looks.
		return err
	}
	committed := false
	// commitAttempted marks the one case where an aborted rollback really does
	// leave the caller not knowing: a commit that was put on the wire and not
	// confirmed can have landed. Until a commit is attempted the records live
	// in an open transaction only - uncommitted transactions are invisible and
	// the broker drops them at the transaction timeout - so even a rollback
	// that fails cannot make them appear.
	commitAttempted := false
	// produced counts the records the broker acknowledged, for the log line.
	produced := 0
	defer func() {
		if committed {
			return
		}
		if abErr := a.endTxn(kgo.TryAbort); abErr != nil {
			a.txnStale = true
			slog.Error("publish-api: rolling back a batch", "err", abErr,
				"records_produced", produced, "commit_attempted", commitAttempted)
			if commitAttempted {
				err = errUnconfirmedWrite
			}
		}
	}()

	// ProduceSync waits for every record, so by here the whole request has an
	// answer and `produced` counts the successes across all of them. A single
	// failure is enough to abort: committing the other 99 is the bug.
	pctx, cancel := context.WithTimeout(ctx, batchProduceTimeout)
	defer cancel()
	var produceErr error
	for _, res := range a.txn.ProduceSync(pctx, records...) {
		if res.Err != nil {
			if produceErr == nil {
				produceErr = fmt.Errorf("%s: %w", res.Record.Topic, res.Err)
			}
			continue
		}
		produced++
	}
	if produceErr != nil {
		return produceErr
	}

	// Commit only while the caller is still there to be told it happened. If
	// their request context is already done they saw no 201 and will retry, so
	// writing events they believe failed would be the same disagreement as the
	// one this removes — just with the platform ahead instead of behind.
	if ctx.Err() != nil {
		return fmt.Errorf("request abandoned before the commit: %w", ctx.Err())
	}
	// EndTransaction takes a context of its own: franz-go warns that
	// cancelling it leaves the outcome unknowable, and the request context
	// dies with the caller.
	commitAttempted = true
	if err := a.endTxn(kgo.TryCommit); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	committed = true
	return nil
}

// endTxn ends the open transaction on a context independent of the caller's,
// bounded by txnEndTimeout. It is used for both commit and rollback.
func (a *publishAPI) endTxn(try kgo.TransactionEndTry) error {
	ctx, cancel := context.WithTimeout(context.Background(), txnEndTimeout)
	defer cancel()
	return a.txn.EndTransaction(ctx, try)
}

// beginTxn opens a transaction, replacing the client when opening one fails.
//
// This exists for one verified failure, and the reason is worth spelling out
// because it is invisible without it: franz-go latches a connection that dies
// during ApiVersions — exactly what a broker restart does — into a
// producer-ID state it labels "fatal, unrecoverable", and then refuses
// BeginTransaction instantly and forever. Nothing in the library clears that
// state; only a fresh client re-runs InitProducerID from scratch. Left alone,
// one broker restart permanently killed the batch path: 99 latched
// BeginTransaction calls, 9,900 of 12,000 requests failing in 0.5 ms each for
// the rest of the run, while the plain single-record client — which re-inits
// lazily and never calls this path — was completely unaffected.
//
// The replacement is followed by one retry so that the request which detects
// the latch often succeeds rather than failing: if the broker is back, this
// publish goes through; if it is still down, the new client latches too and
// the next request replaces it again. Called with txnMu held.
func (a *publishAPI) beginTxn() error {
	if a.txn == nil {
		// A previous replacement failed to open. Try again rather than
		// refusing forever: "the broker was down once" must not become a
		// permanently dead batch path either.
		a.replaceTxn()
		if a.txn == nil {
			return errors.New("no transactional client could be opened")
		}
	}
	err := a.txn.BeginTransaction()
	if err == nil {
		return nil
	}
	a.replaceTxn()
	if a.txn == nil {
		return fmt.Errorf("beginning transaction: %w (and no replacement could be opened)", err)
	}
	if err2 := a.txn.BeginTransaction(); err2 != nil {
		return fmt.Errorf("beginning transaction: %w", err2)
	}
	return nil
}

// replaceTxn throws the current transactional client away and opens another.
// It must be called with txnMu held. Tests inject the client and leave openTxn
// nil; then there is nothing to open, txn itself is left untouched, and the
// retry in beginTxn fails with the original error instead of dereferencing a
// nil.
func (a *publishAPI) replaceTxn() {
	if a.txnClose != nil {
		a.txnClose() // the old client is already failing; a close error changes nothing
		a.txnClose = nil
	}
	if a.openTxn == nil {
		return
	}
	cl, closeFn, err := a.openTxn()
	if err != nil {
		slog.Error("publish-api: reopening the transactional client", "err", err)
		a.txn = nil
		return
	}
	a.txn, a.txnClose = cl, closeFn
}

// closeTxn disposes of whichever transactional client is current. main defers
// this instead of closing the client it opened, because a replacement may have
// taken its place since.
func (a *publishAPI) closeTxn() {
	if a.txnClose != nil {
		a.txnClose()
		a.txnClose = nil
	}
}

// firstProduceError returns the first failing record's error, tagged with its
// topic so the log still says where the write went wrong.
func firstProduceError(results kgo.ProduceResults) error {
	for _, res := range results {
		if res.Err != nil {
			return fmt.Errorf("%s: %w", res.Record.Topic, res.Err)
		}
	}
	return nil
}

// writeFailure words a 500 by what is actually known — the whole point of the
// transaction is that, for a batch, "nothing was written" is finally a true
// statement.
func writeFailure(records int, err error) string {
	switch {
	case errors.Is(err, errUnconfirmedWrite):
		return "producing event failed: the outcome could not be confirmed, " +
			"the events may or may not have been written"
	case records > 1:
		return "producing event failed: no event of this request was written"
	default:
		// A single record: an error means the write was not acknowledged, and
		// for one record that is the whole of what can be known.
		return "producing event failed"
	}
}

// isBatch reports whether the request used the batch media type.
func isBatch(r *http.Request) bool {
	ct := strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0])
	return strings.EqualFold(ct, cloudevents.ContentTypeBatch)
}

// decodeJSONObject reports whether the raw data member is a JSON object. Data
// that only exists as data_base64 is never a JSON object.
func decodeJSONObject(data json.RawMessage, dst *map[string]any) error {
	if len(data) == 0 {
		return errors.New("base64 data is not a JSON object")
	}
	return json.Unmarshal(data, dst)
}

// recordHeaders converts headers as built by kafkautil.HSet to the franz-go
// record header type.
func recordHeaders(hs ...kmsg.Header) []kgo.RecordHeader {
	out := make([]kgo.RecordHeader, 0, len(hs))
	for _, h := range hs {
		out = append(out, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	return out
}

// retentionExpiry maps a retention class to the immutable expiry stamped at
// publish (sections 6.1 and 12). live means expires_at = ingested_at: no
// replay duty and the event is never parked in the inbox.
func retentionExpiry(class string, ingestedAt time.Time) time.Time {
	switch class {
	case kafkautil.ClassLive:
		return ingestedAt
	case kafkautil.ClassDay:
		return ingestedAt.Add(24 * time.Hour)
	case kafkautil.ClassWeek:
		return ingestedAt.Add(7 * 24 * time.Hour)
	default: // kafkautil.ClassShort
		return ingestedAt.Add(time.Hour)
	}
}

// partitionKey implements the default ordering key of section 5: the
// partitionkey extension, else subject, else source + "/" + id.
func partitionKey(e *cloudevents.Event) string {
	if v, ok := e.Extension("partitionkey"); ok && v != "" {
		return v
	}
	if e.Subject != "" {
		return e.Subject
	}
	return e.Source + "/" + e.ID
}

func splitBrokers(s string) []string {
	out := make([]string, 0, 2)
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

func setupLogging() {
	var level slog.Level
	if err := level.UnmarshalText([]byte(strings.ToLower(config.Str("LOG_LEVEL", "info")))); err != nil {
		slog.Warn("invalid LOG_LEVEL, staying at info", "err", err)
		return
	}
	slog.SetLogLoggerLevel(level)
}

func fatal(stage string, err error) {
	slog.Error(stage, "err", err)
	os.Exit(1)
}
