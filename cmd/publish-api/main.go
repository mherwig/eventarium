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

	// The registry reads the control data through the store interface; the
	// poller keeps the index fresh after event type changes (section 6.1).
	reg := registry.New(st, 2*time.Second)
	if err := reg.Reload(ctx); err != nil {
		slog.Warn("initial registry load failed", "err", err)
	}
	go reg.Run(ctx)

	client, err := kafkautil.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		fatal("connecting to kafka", err)
	}
	defer client.Close()

	validator, err := auth.NewHMAC(
		config.Str("JWT_SECRET", "dev-secret"),
		config.Str("JWT_ISSUER", "dev"),
		config.Str("JWT_AUDIENCE", "pubsub"),
	)
	if err != nil {
		fatal("building token validator", err)
	}

	api := &publishAPI{auth: validator, registry: reg, kafka: client, maxBytes: maxBytes}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpx.JSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
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
	slog.Info("publish-api listening", "addr", addr, "brokers", brokers, "max_event_bytes", maxBytes)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		fatal("serving http", err)
	}
	slog.Info("publish-api stopped")
}

// publishAPI holds the dependencies of the single write endpoint.
type publishAPI struct {
	auth     *auth.Validator
	registry *registry.Registry
	kafka    *kgo.Client
	maxBytes int64
}

// publishResult is one entry of the 201 response (section 6.1).
type publishResult struct {
	EventRef       string `json:"event_ref"`
	EventType      string `json:"event_type"`
	RetentionClass string `json:"retention_class"`
	ExpiresAt      string `json:"expires_at"`
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
		})
	}

	// F19: publish is not deduplicated. Publishing the same message again is
	// allowed and simply creates another event with a new reference.
	for _, res := range a.kafka.ProduceSync(r.Context(), records...) {
		if res.Err != nil {
			slog.Error("producing event", "topic", res.Record.Topic, "err", res.Err)
			httpx.Error(w, http.StatusInternalServerError, "producing event failed")
			return
		}
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
