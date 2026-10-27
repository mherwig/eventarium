// Package sse implements the SSE delivery and gateway service of the spike
// (sections 6.5, 6.6, 7, 8 and 9 of the spec). One binary fills three roles:
//
//   - HTTP gateway: it terminates SSE connections for subscriptions of the
//     "sse" channel, enforces max_connections per subscription, heartbeats and
//     ends a connection when the client goes away or its token expires
//     (sections 6.5 and 6.7).
//   - Presence: every connect and disconnect is published to the compacted
//     sse-presence topic, and every node consumes that topic from the
//     beginning to learn where each connection currently lives (6.5).
//   - Delivery stage and drainer: the consumer group "sse-delivery" over the
//     deliveries topic assigns every sse delivery either to a live connection
//     (through the sse-live topic, targeted at the owning node) or to the
//     PostgreSQL inbox, which is drained back onto the live path as soon as a
//     connection exists again (6.6).
//
// Ordering (sections 6.3 and 8): the deliveries topic is keyed by subscription
// ID, so one subscription always sits on exactly one partition and is owned by
// exactly one consumer. The delivery stage handles a poll batch serially and
// produces to the sse-live topic, which is keyed by subscription ID again, so
// per-subscription order reaches the client in order. Offsets are committed
// only after a batch has been fully processed, so a crash repeats a batch
// instead of losing one; the duplicates are identified downstream by the event
// reference (8).
//
// Spike simplifications, commented where they occur: the connection assigned
// to a delivery is picked arbitrarily instead of by rendezvous hashing (7),
// acks are cumulative for every delivery mode (6.5), and the drainer runs on
// the single node with SSE_DRAINER=true instead of on the partition owner.
package sse

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"pubsub/internal/auth"
	"pubsub/internal/delivery"
	"pubsub/internal/httpx"
	"pubsub/internal/kafkautil"
	"pubsub/internal/registry"
	"pubsub/internal/status"
	"pubsub/internal/store"
)

// Options configures the gateway. cmd/sse-gateway maps the environment onto
// this struct; every field falls back to the docs/SPIKE.md default.
type Options struct {
	HTTPAddr    string   // HTTP_ADDR, default :8082
	NodeID      string   // NODE_ID, default: hostname
	Brokers     []string // KAFKA_BROKERS
	PGDSN       string   // PG_DSN
	JWTSecret   string   // JWT_SECRET
	JWTIssuer   string   // JWT_ISSUER
	JWTAudience string   // JWT_AUDIENCE

	Heartbeat      time.Duration // SSE_HEARTBEAT, default 15s
	MaxConnections int           // SSE_MAX_CONNECTIONS, default 3; a subscription policy overrides it
	RegistryPoll   time.Duration // registry poll interval, default 1s
	Drainer        bool          // run the inbox drainer on this node (SSE_DRAINER)
}

// Gateway is the running service: HTTP server, presence view, delivery stage,
// drainer and live consumer.
type Gateway struct {
	opts  Options
	log   *slog.Logger
	store *store.Store
	reg   *registry.Registry
	auth  *auth.Validator

	status      *status.Producer // delivery-status topic
	presencePub *kgo.Client      // connect records and tombstones
	livePub     *kgo.Client      // targeted deliveries for the gateway nodes
	presenceCl  *kgo.Client      // no group: whole presence topic, from the start
	deliveryCl  *kgo.Client      // group sse-delivery
	liveCl      *kgo.Client      // group sse-live-<node>

	view *presenceView

	mu    sync.Mutex
	conns map[string]*connection            // connection ID -> connection
	bySub map[string]map[string]*connection // subscription ID -> connection ID -> connection

	// parked caches the inbox row count per subscription behind backlog_max
	// (spec 16), so the delivery stage does not count the inbox per batch -
	// one count over a full inbox costs about 10 ms. Refilled by
	// runParkedReconcile; a counter that has never been seeded counts itself
	// on first use. Its own mutex: it is touched from the delivery stage, the
	// drainer and the reconciler, none of which may block on connection
	// lookups.
	parkedMu sync.Mutex
	parked   map[string]*store.ParkCounter

	shutdown chan struct{}
}

// New connects PostgreSQL, Kafka and the token validator.
func New(opts Options) (*Gateway, error) {
	if opts.HTTPAddr == "" {
		opts.HTTPAddr = ":8082"
	}
	if opts.NodeID == "" {
		if h, err := os.Hostname(); err == nil {
			opts.NodeID = h
		} else {
			opts.NodeID = "sse-gateway"
		}
	}
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 15 * time.Second
	}
	if opts.MaxConnections <= 0 {
		opts.MaxConnections = 3
	}
	if opts.RegistryPoll <= 0 {
		opts.RegistryPoll = time.Second
	}
	if len(opts.Brokers) == 0 {
		opts.Brokers = kafkautil.Bootstrap()
	}

	g := &Gateway{
		opts:     opts,
		log:      slog.Default().With("service", "sse-gateway", "node", opts.NodeID),
		view:     newPresenceView(),
		conns:    map[string]*connection{},
		bySub:    map[string]map[string]*connection{},
		parked:   map[string]*store.ParkCounter{},
		shutdown: make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var err error
	cleanup := func() { g.closeAll() }
	if g.store, err = store.Open(ctx, opts.PGDSN); err != nil {
		return nil, err
	}
	g.reg = registry.New(g.store, opts.RegistryPoll)
	if g.auth, err = auth.NewHMAC(opts.JWTSecret, opts.JWTIssuer, opts.JWTAudience); err != nil {
		cleanup()
		return nil, err
	}
	if g.status, err = status.NewProducer(opts.Brokers); err != nil {
		cleanup()
		return nil, err
	}
	if g.presencePub, err = kafkautil.NewClient(kgo.DefaultProduceTopic(kafkautil.TopicPresence)); err != nil {
		cleanup()
		return nil, err
	}
	if g.livePub, err = kafkautil.NewClient(kgo.DefaultProduceTopic(kafkautil.TopicSSELive)); err != nil {
		cleanup()
		return nil, err
	}
	// The presence view is a mirror, not work to split: pick() answers with a
	// connection on *any* node, so every gateway needs the whole topic
	// (section 6.5). A consumer group would hand each node only its slice of
	// the partitions - one third of the map at three replicas - and the
	// delivery stage would park deliveries that another node could have taken.
	// Without a group nothing is committed and nothing is shared: every node
	// consumes all partitions itself, and because there is no committed
	// offset to resume from, every start rebuilds the map from the beginning
	// of the compacted topic, including connections opened on other nodes
	// while this one was down.
	if g.presenceCl, err = kafkautil.NewClient(
		kgo.ConsumeTopics(kafkautil.TopicPresence),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	); err != nil {
		cleanup()
		return nil, err
	}
	// A fresh delivery group starts at the tip of the topic; a restarted group
	// resumes from its committed offsets.
	if g.deliveryCl, err = kafkautil.NewClient(
		kgo.ConsumerGroup("sse-delivery"),
		kgo.ConsumeTopics(kafkautil.TopicDeliveries),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
	); err != nil {
		cleanup()
		return nil, err
	}
	// One consumer group per node, starting at the latest offset: every node
	// must see every targeted message (section 6.5).
	if g.liveCl, err = kafkautil.NewClient(
		kgo.ConsumerGroup("sse-live-"+opts.NodeID),
		kgo.ConsumeTopics(kafkautil.TopicSSELive),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
	); err != nil {
		cleanup()
		return nil, err
	}
	return g, nil
}

// Run starts every goroutine and the HTTP server, then blocks until ctx is
// cancelled or the listener fails, and shuts down gracefully.
func (g *Gateway) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Daily inbox partitions so that expiry can drop whole partitions (6.6).
	if err := g.store.EnsureInboxPartitions(ctx, 8); err != nil {
		g.log.Warn("ensuring inbox partitions", "err", err)
	}

	var wg sync.WaitGroup
	start := func(name string, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(ctx)
			g.log.Info("stopped", "role", name)
		}()
	}
	go g.reg.Run(ctx)
	start("presence view", g.runPresenceView)
	start("delivery stage", g.runDeliveryStage)
	start("live consumer", g.runLive)
	start("inbox expiry", g.runInboxExpiry)
	start("parked reconcile", g.runParkedReconcile)
	if g.opts.Drainer {
		start("drainer", g.runDrainer)
	} else {
		g.log.Info("drainer disabled on this node (SSE_DRAINER=false)")
	}

	srv := &http.Server{
		Addr:              g.opts.HTTPAddr,
		Handler:           g.routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.ListenAndServe() }()
	g.log.Info("listening", "addr", g.opts.HTTPAddr)

	var runErr error
	select {
	case <-ctx.Done():
		g.log.Info("shutting down")
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = err
		}
	}

	// Graceful shutdown: first tell every connection to reconnect, which makes
	// the handlers publish their presence tombstones, then stop consuming,
	// then drain the HTTP server, and only then flush and close the producers.
	close(g.shutdown)
	cancel()

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(10 * time.Second):
		g.log.Warn("consumers did not stop in time")
	}

	shCtx, shCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := srv.Shutdown(shCtx); err != nil {
		g.log.Warn("http shutdown", "err", err)
	}
	shCancel()

	g.closeAll()
	g.log.Info("stopped")
	return runErr
}

// closeAll closes the Kafka clients and the database pool. The status producer
// is closed last because it flushes what is still queued.
func (g *Gateway) closeAll() {
	for _, cl := range []*kgo.Client{g.deliveryCl, g.liveCl, g.presenceCl, g.presencePub, g.livePub} {
		if cl != nil {
			cl.Close()
		}
	}
	if g.status != nil {
		g.status.Close()
	}
	if g.store != nil {
		g.store.Close()
	}
}

func (g *Gateway) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", httpx.AccessLog(http.HandlerFunc(g.handleHealth)))
	mux.Handle("POST /sse/ack", httpx.AccessLog(http.HandlerFunc(g.handleAck)))
	// /sse deliberately stays outside httpx.AccessLog: the SSE writer needs
	// the raw http.Flusher (the access-log writer hides it) and connect and
	// disconnect are logged by the gateway itself.
	mux.Handle("GET /sse", http.HandlerFunc(g.handleSSE))
	return mux
}

func (g *Gateway) handleHealth(w http.ResponseWriter, _ *http.Request) {
	g.mu.Lock()
	n := len(g.conns)
	g.mu.Unlock()
	httpx.JSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"node_id":       g.opts.NodeID,
		"connections":   n,
		"presence":      len(g.view.subscriptions()), // connections on every node, not just this one
		"subscriptions": g.reg.Len(),
	})
}

// handleSSE is the streaming endpoint of section 6.5.
func (g *Gateway) handleSSE(w http.ResponseWriter, r *http.Request) {
	claims, ok := g.auth.Authorize(w, r, auth.ScopeSubscribe)
	if !ok {
		return
	}
	q := r.URL.Query()
	subID := strings.TrimSpace(q.Get("subscription_id"))
	if subID == "" {
		httpx.Error(w, http.StatusBadRequest, "subscription_id is required")
		return
	}
	consumerID := strings.TrimSpace(q.Get("consumer_id"))
	if consumerID == "" {
		consumerID = "default"
	}

	// The reconnect cursor of section 8 arrives as Last-Event-ID header (what
	// a browser EventSource echoes back) or as last_event_id query parameter
	// for clients that cannot set headers; the header wins when both are
	// given. It is parsed before register() and before any header is written,
	// so a present but malformed cursor is answered with a plain 400 and never
	// half-opens a stream.
	cursorText := requestCursor(r)
	var cursor delivery.Seq
	hasCursor := cursorText != ""
	if hasCursor {
		c, err := parseSeq(cursorText)
		if err != nil {
			httpx.Error(w, http.StatusBadRequest, err.Error())
			return
		}
		if c.Partition >= kafkautil.DeliveryPartitions {
			// The sequence names a partition of the deliveries topic (6.3), so
			// an out-of-range partition is a broken cursor, not a gap.
			httpx.Error(w, http.StatusBadRequest, "sse: sequence partition out of range")
			return
		}
		cursor = c
	}

	// Unknown and foreign subscriptions answer the same 404: the existence of
	// a subscription is never leaked across owners (section 6.7).
	entry, found := g.reg.Subscription(subID)
	if !found || entry.Sub.Owner != claims.Owner {
		httpx.Error(w, http.StatusNotFound, "unknown subscription")
		return
	}
	sub := entry.Sub
	if sub.Channel != delivery.ChannelSSE {
		httpx.Error(w, http.StatusConflict, "subscription is not an sse subscription")
		return
	}
	exp, err := tokenExpiry(r)
	if err != nil {
		httpx.Error(w, http.StatusUnauthorized, err.Error())
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpx.Error(w, http.StatusInternalServerError, "streaming is not supported")
		return
	}

	limit := g.opts.MaxConnections
	if sub.MaxConnections > 0 {
		limit = sub.MaxConnections // delivery policy of section 7
	}
	conn := newConnection(subID, consumerID, claims.Owner)
	if !g.register(conn, limit) {
		g.log.Info("sse connection rejected", "subscription", subID, "consumer", consumerID, "limit", limit)
		httpx.Error(w, http.StatusTooManyRequests, "max_connections reached for this subscription")
		return
	}
	defer g.unregister(conn)

	// Presence first: the record must be visible to the delivery stage before
	// the first byte reaches the client, otherwise early deliveries are parked
	// for no reason (they are drained back a tick later, but still).
	pctx, pcancel := ctxTimeout(5 * time.Second)
	err = g.publishPresence(pctx, conn, false)
	pcancel()
	if err != nil {
		g.log.Error("publishing presence", "err", err)
		httpx.Error(w, http.StatusServiceUnavailable, "presence unavailable")
		return
	}
	defer func() {
		// The tombstone makes the compacted topic forget the connection.
		ctx, cancel := ctxTimeout(5 * time.Second)
		defer cancel()
		if err := g.publishPresence(ctx, conn, true); err != nil {
			g.log.Warn("publishing presence tombstone", "err", err, "connection", conn.id)
		}
	}()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	// Tell proxies (nginx and friends) not to buffer the stream.
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	// The initial retry hint lowers the browser's reconnection delay.
	_, _ = io.WriteString(w, "retry: 3000\n\n")
	flusher.Flush()

	// Every write gets a deadline so that a stalled client cannot pin the
	// goroutine forever (connection hygiene, section 6.5).
	rc := http.NewResponseController(w)
	writeDeadline := 3 * g.opts.Heartbeat
	if writeDeadline < 30*time.Second {
		writeDeadline = 30 * time.Second
	}
	write := func(s string) error {
		_ = rc.SetWriteDeadline(time.Now().Add(writeDeadline))
		if _, err := io.WriteString(w, s); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	g.log.Info("sse connected",
		"subscription", subID, "consumer", consumerID, "connection", conn.id, "expires_at", exp.UTC())
	defer func() {
		// No disconnect on inactivity (section 6.5): only the client, the
		// token expiry or a shutdown end the connection, otherwise events
		// would be pushed into the inbox and the latency target would break.
		g.log.Info("sse disconnected",
			"subscription", subID, "consumer", consumerID, "connection", conn.id)
	}()

	if hasCursor {
		// The backlog phase of section 8: fetch everything the client missed
		// since the cursor from the deliveries topic while live events buffer
		// in conn.out, then merge both in the event loop below. A failed write
		// ends the handler: the client reconnects with its own cursor and the
		// next attempt resumes exactly where this one stopped.
		if g.serveBacklog(r.Context(), conn, subID, cursor, write) {
			return
		}
	}

	heartbeat := time.NewTicker(g.opts.Heartbeat)
	defer heartbeat.Stop()
	expiry := time.NewTimer(time.Until(exp))
	defer expiry.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-g.shutdown:
			// Final comment, then the handler returns and net/http closes the
			// stream; the client reconnects immediately and is served from
			// its Last-Event-ID cursor if it kept one, otherwise the drainer
			// serves whatever was parked meanwhile.
			_ = write(": gateway shutting down, reconnect shortly\n\n")
			return
		case <-conn.done:
			_ = write(": closing: " + conn.closeReason() + "\n\n")
			return
		case <-expiry.C:
			// Section 6.7: a connection ends when its token expires.
			_ = write(": token expired, reconnect with a fresh token\n\n")
			return
		case <-heartbeat.C:
			// A comment line, so a heartbeat never looks like an event.
			if err := write(": hb\n\n"); err != nil {
				g.log.Info("sse heartbeat failed", "connection", conn.id, "err", err)
				return
			}
		case out := <-conn.out:
			// Dedup against the backlog phase (section 8): a row that was
			// still parked can be served from Kafka by the backlog and pushed
			// by the drainer a tick later, and the delivery stage can have
			// routed an event inside the fetched range live while we were
			// fetching. Only the backlog phase marks entries here, so the map
			// is empty for a connection without a cursor and this costs one
			// lookup; the drainer's own ack and status side effects run
			// gateway-side and are not affected. The check covers what the
			// backlog wrote (bounded by backlog_max, section 7) — beyond that,
			// the client-side dedup by event reference is the safety net that
			// section 8 mandates anyway.
			if conn.alreadyServed(out.seq) {
				continue
			}
			frame, err := out.frame()
			if err != nil {
				g.log.Error("rendering sse event", "err", err, "connection", conn.id)
				continue
			}
			if err := write(frame); err != nil {
				g.log.Info("sse write failed", "connection", conn.id, "err", err)
				return
			}
			// Only once the bytes are on the wire does the connection remember
			// the delivery, so an ack can be answered honestly.
			conn.appendSent(out)
		}
	}
}

// ackBody is the JSON body of POST /sse/ack.
type ackBody struct {
	SubscriptionID string `json:"subscription_id"`
	Sequence       string `json:"sequence"`
}

// handleAck acknowledges deliveries. The sequence comes from the body or from
// the Last-Event-ID header a reconnecting client echoes back.
func (g *Gateway) handleAck(w http.ResponseWriter, r *http.Request) {
	claims, ok := g.auth.Authorize(w, r, auth.ScopeSubscribe)
	if !ok {
		return
	}
	var body ackBody
	if raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<16)); err != nil {
		httpx.Error(w, http.StatusBadRequest, "reading body")
		return
	} else if len(strings.TrimSpace(string(raw))) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			httpx.Error(w, http.StatusBadRequest, "invalid JSON body")
			return
		}
	}
	seqText := strings.TrimSpace(body.Sequence)
	if seqText == "" {
		seqText = strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	}
	if seqText == "" {
		httpx.Error(w, http.StatusBadRequest, "sequence is required in the body or as Last-Event-ID")
		return
	}
	seq, err := parseSeq(seqText)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	subID := strings.TrimSpace(body.SubscriptionID)
	if subID == "" {
		httpx.Error(w, http.StatusBadRequest, "subscription_id is required")
		return
	}
	entry, found := g.reg.Subscription(subID)
	if !found || entry.Sub.Owner != claims.Owner {
		httpx.Error(w, http.StatusNotFound, "unknown subscription")
		return
	}
	n := g.acknowledge(r.Context(), subID, seq)
	httpx.JSON(w, http.StatusOK, map[string]any{
		"subscription_id": subID,
		"sequence":        seq.String(),
		"acked":           n,
	})
}

// requestCursor returns the reconnect cursor of the request (section 8): the
// Last-Event-ID header a client echoes back, or the last_event_id query
// parameter for clients that cannot set headers. The header wins when both
// are present. An empty string means "no cursor" and keeps the historical
// behaviour of serving the connection from the live path only.
func requestCursor(r *http.Request) string {
	if v := strings.TrimSpace(r.Header.Get("Last-Event-ID")); v != "" {
		return v
	}
	return strings.TrimSpace(r.URL.Query().Get("last_event_id"))
}

// parseSeq parses an ack handle of the form "<partition>:<offset>", which is
// the id the gateway puts on the wire (sections 6.3 and 6.5).
func parseSeq(s string) (delivery.Seq, error) {
	s = strings.TrimSpace(s)
	partText, offsetText, ok := strings.Cut(s, ":")
	if !ok {
		return delivery.Seq{}, errors.New("sse: sequence must look like \"<partition>:<offset>\"")
	}
	part, err := strconv.ParseInt(strings.TrimSpace(partText), 10, 32)
	if err != nil || part < 0 {
		return delivery.Seq{}, errors.New("sse: invalid sequence partition")
	}
	offset, err := strconv.ParseInt(strings.TrimSpace(offsetText), 10, 64)
	if err != nil || offset < 0 {
		return delivery.Seq{}, errors.New("sse: invalid sequence offset")
	}
	return delivery.Seq{Partition: int32(part), Offset: offset}, nil
}

// acknowledge answers a cumulative ack (sections 6.5 and 9): every delivery
// this node has written to a connection of the subscription up to and
// including seq becomes delivered, carrying the ID of the connection that
// wrote it. Without an ack the status stays honestly at "sent".
//
// Inbox rows are deliberately not touched here: the drainer deletes them after
// it has emitted them, so an ack can never delete a delivery that was never
// sent. The backlog phase (backlog.go) deletes the rows it wrote itself.
func (g *Gateway) acknowledge(ctx context.Context, subID string, seq delivery.Seq) int {
	var recs []status.Record
	for _, c := range g.connections(subID) {
		for _, e := range c.ackUpTo(seq) {
			recs = append(recs, statusFor(e.msg, status.Delivered, "", c.id))
		}
	}
	if len(recs) > 0 {
		if err := g.status.Produce(ctx, recs...); err != nil {
			g.log.Warn("emitting delivered status", "err", err, "subscription", subID)
		}
	}
	return len(recs)
}

// register adds a connection unless the subscription is already at its limit.
func (g *Gateway) register(c *connection, limit int) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	m := g.bySub[c.subID]
	if m == nil {
		m = map[string]*connection{}
		g.bySub[c.subID] = m
	}
	if limit > 0 && len(m) >= limit {
		return false
	}
	m[c.id] = c
	g.conns[c.id] = c
	return true
}

func (g *Gateway) unregister(c *connection) {
	g.mu.Lock()
	defer g.mu.Unlock()
	delete(g.conns, c.id)
	if m := g.bySub[c.subID]; m != nil {
		delete(m, c.id)
		if len(m) == 0 {
			delete(g.bySub, c.subID)
		}
	}
}

func (g *Gateway) connections(subID string) []*connection {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]*connection, 0, len(g.bySub[subID]))
	for _, c := range g.bySub[subID] {
		out = append(out, c)
	}
	return out
}

func (g *Gateway) localConnection(connID string) *connection {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.conns[connID]
}

// backlogMax returns the parked-row cap of the subscription (section 7), or 0
// for "no cap" when the registry has no policy or has just deleted it.
func (g *Gateway) backlogMax(subID string) int64 {
	if entry, ok := g.reg.Subscription(subID); ok && entry.Sub.BacklogMax > 0 {
		return int64(entry.Sub.BacklogMax)
	}
	return 0
}

// drainRate returns drain_rate_per_s of the subscription (section 7).
func (g *Gateway) drainRate(subID string) int {
	if entry, ok := g.reg.Subscription(subID); ok && entry.Sub.DrainRatePerS > 0 {
		return entry.Sub.DrainRatePerS
	}
	return defaultDrainRate
}

// tokenExpiry extracts the exp claim from the bearer token so the connection
// can end when the token expires (section 6.7). The signature, issuer, audience
// and expiry were already validated by auth.Validator; this only re-reads exp.
func tokenExpiry(r *http.Request) (time.Time, error) {
	token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, errors.New("sse: malformed token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return time.Time{}, errors.New("sse: malformed token payload")
	}
	var claims map[string]json.RawMessage
	if err := json.Unmarshal(payload, &claims); err != nil {
		return time.Time{}, errors.New("sse: malformed token payload")
	}
	var exp float64
	if err := json.Unmarshal(claims["exp"], &exp); err != nil {
		return time.Time{}, errors.New("sse: token carries no exp claim")
	}
	return time.Unix(int64(exp), 0), nil
}

func ctxTimeout(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), d)
}
