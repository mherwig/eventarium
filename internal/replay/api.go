package replay

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"pubsub/internal/auth"
	"pubsub/internal/httpx"
	"pubsub/internal/registry"
)

// Request bounds of the HTTP surface (section 11).
const (
	// maxBodyBytes bounds the create body; a selector is a list of statuses,
	// two timestamps and a list of event references.
	maxBodyBytes = 1 << 20
	// maxIdempotencyKey bounds the Idempotency-Key header.
	maxIdempotencyKey = 200
	// listLimit bounds GET /v1/subscriptions/{id}/replays; the endpoint is
	// an operational list, not an audit trail export.
	listLimit = 100
)

// Server serves the Replay API of section 11:
//
//	POST /v1/subscriptions/{id}/replays   create a job (202 + replay ID)
//	GET  /v1/replays/{replay_id}          progress, incl. live delivered/failed
//	POST /v1/replays/{replay_id}/cancel   stop a job
//	GET  /v1/subscriptions/{id}/replays   list the jobs of a subscription
//
// Every route except /healthz (wired by cmd/replay-api) needs the replay
// scope, and every lookup is scoped by the owner in the verified JWT
// (section 6.7): unknown and foreign resources answer the same 404.
type Server struct {
	validator *auth.Validator
	jobs      *JobStore
	ch        clickhouse.Conn
	reg       *registry.Registry
	maxEvents int
}

// NewServer wires the API against its three stores: the replay job table
// (PostgreSQL), the status table (ClickHouse) and the subscription registry.
func NewServer(validator *auth.Validator, jobs *JobStore, ch clickhouse.Conn,
	reg *registry.Registry, maxEvents int) *Server {
	return &Server{validator: validator, jobs: jobs, ch: ch, reg: reg, maxEvents: maxEvents}
}

// Register mounts the routes on mux using Go 1.22 method+pattern routing,
// the same style as status-api.
func (s *Server) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/subscriptions/{id}/replays", s.createReplay)
	mux.HandleFunc("GET /v1/subscriptions/{id}/replays", s.listReplays)
	mux.HandleFunc("GET /v1/replays/{replay_id}", s.getReplay)
	mux.HandleFunc("POST /v1/replays/{replay_id}/cancel", s.cancelReplay)
}

// createReplay validates the selector, resolves the subscription and creates
// the job (202). It answers 400 for a selector that violates section 11 (at
// least one criterion, valid statuses, from <= to, max_events within bounds,
// event_types rejected honestly because status records carry no event type),
// 404 for an unknown or foreign subscription, 409 when the subscription
// already has an active job, and the same 202 body for a repeated
// Idempotency-Key — without enqueuing a second job.
func (s *Server) createReplay(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.validator.Authorize(w, r, auth.ScopeReplay)
	if !ok {
		return
	}
	subID := r.PathValue("id")
	if !s.ownedSubscription(claims.Owner, subID) {
		httpx.Error(w, http.StatusNotFound, "unknown subscription")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, "cannot read request body")
		return
	}
	if len(body) > maxBodyBytes {
		httpx.Error(w, http.StatusBadRequest, "request body too large")
		return
	}
	sel, err := ParseSelector(body, s.maxEvents)
	if err != nil {
		httpx.Error(w, http.StatusBadRequest, err.Error())
		return
	}
	idem := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if len(idem) > maxIdempotencyKey {
		httpx.Error(w, http.StatusBadRequest, "Idempotency-Key must be at most 200 characters")
		return
	}

	job, created, err := s.jobs.Create(r.Context(), claims.Owner, subID, idem, sel)
	var active *ActiveJobError
	switch {
	case errors.As(err, &active):
		httpx.JSON(w, http.StatusConflict, map[string]string{
			"error":     "this subscription already has an active replay job",
			"replay_id": active.Job.ID,
		})
		return
	case err != nil:
		slog.Error("replay-api: creating job", "err", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !created && job.SubscriptionID != subID {
		// The key is unique per owner (migration 002); reusing it against a
		// different subscription must not hand back that other job.
		httpx.Error(w, http.StatusConflict, "Idempotency-Key was already used for a different subscription")
		return
	}
	httpx.JSON(w, http.StatusAccepted, createResponse{
		ReplayID:       job.ID,
		SubscriptionID: job.SubscriptionID,
		Status:         job.Status,
		DryRun:         job.DryRun,
	})
}

// getReplay returns one job with its progress (section 11, endpoint 2).
// matched/enqueued/not_replayable are the worker's persisted counters;
// delivered and failed are computed live from ClickHouse, per delivery among
// the status rows carrying this replay ID, so the caller sees the replay's
// outcome without polling the Status API.
func (s *Server) getReplay(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.validator.Authorize(w, r, auth.ScopeReplay)
	if !ok {
		return
	}
	job, err := s.jobs.Get(r.Context(), claims.Owner, r.PathValue("replay_id"))
	if err != nil {
		s.fail(w, "loading job", err)
		return
	}
	delivered, failed, err := Progress(r.Context(), s.ch, claims.Owner, job.ID)
	if err != nil {
		slog.Error("replay-api: progress", "err", err)
		httpx.Error(w, http.StatusInternalServerError, "internal error")
		return
	}
	view := newJobView(job)
	view.Delivered = &delivered
	view.Failed = &failed
	httpx.JSON(w, http.StatusOK, view)
}

// listReplays lists the jobs of one subscription, newest first (section 11,
// endpoint 4): job rows only, without the live ClickHouse counts.
func (s *Server) listReplays(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.validator.Authorize(w, r, auth.ScopeReplay)
	if !ok {
		return
	}
	subID := r.PathValue("id")
	if !s.ownedSubscription(claims.Owner, subID) {
		httpx.Error(w, http.StatusNotFound, "unknown subscription")
		return
	}
	jobs, err := s.jobs.List(r.Context(), claims.Owner, subID, listLimit)
	if err != nil {
		s.fail(w, "listing jobs", err)
		return
	}
	items := make([]jobView, 0, len(jobs))
	for i := range jobs {
		items = append(items, *newJobView(&jobs[i]))
	}
	httpx.JSON(w, http.StatusOK, struct {
		Items []jobView `json:"items"`
	}{Items: items})
}

// cancelReplay stops a job (section 11, endpoint 3): a pending job is
// cancelled immediately, a running one is flagged so the worker stops at its
// next batch boundary, and an already finished job is a 409.
func (s *Server) cancelReplay(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.validator.Authorize(w, r, auth.ScopeReplay)
	if !ok {
		return
	}
	job, applied, err := s.jobs.RequestCancel(r.Context(), claims.Owner, r.PathValue("replay_id"))
	if err != nil {
		s.fail(w, "cancelling job", err)
		return
	}
	if !applied {
		httpx.JSON(w, http.StatusConflict, map[string]string{
			"error":     "replay job has already finished",
			"replay_id": job.ID,
			"status":    job.Status,
		})
		return
	}
	httpx.JSON(w, http.StatusOK, cancelResponse{ReplayID: job.ID, Status: job.Status})
}

// ownedSubscription reports whether the subscription exists and belongs to
// the owner. Unknown and foreign subscriptions are deliberately
// indistinguishable (section 6.7).
func (s *Server) ownedSubscription(owner, subscriptionID string) bool {
	entry, found := s.reg.Subscription(subscriptionID)
	return found && entry.Sub.Owner == owner
}

// fail maps a store error to 404 or 500; the owner scope is part of every
// query, so a missing row and a foreign row are the same 404 (section 6.7).
func (s *Server) fail(w http.ResponseWriter, op string, err error) {
	if errors.Is(err, ErrNotFound) {
		httpx.Error(w, http.StatusNotFound, "not found")
		return
	}
	slog.Error("replay-api: "+op, "err", err)
	httpx.Error(w, http.StatusInternalServerError, "internal error")
}

// createResponse is the 202 body of the create endpoint — and, with the same
// shape, the body a repeated Idempotency-Key gets for the job that already
// exists (section 11).
type createResponse struct {
	ReplayID       string `json:"replay_id"`
	SubscriptionID string `json:"subscription_id"`
	Status         string `json:"status"`
	DryRun         bool   `json:"dry_run"`
}

// cancelResponse is the 200 body of the cancel endpoint.
type cancelResponse struct {
	ReplayID string `json:"replay_id"`
	Status   string `json:"status"`
}

// jobView is the JSON form of a job for GET /v1/replays/{id} and the list
// endpoint. Delivered and Failed are only set by the single-job endpoint
// (omitempty): they need a live ClickHouse query, which the list does not do.
type jobView struct {
	ReplayID       string     `json:"replay_id"`
	SubscriptionID string     `json:"subscription_id"`
	Status         string     `json:"status"`
	DryRun         bool       `json:"dry_run"`
	Selector       Selector   `json:"selector"`
	Matched        int64      `json:"matched"`
	Enqueued       int64      `json:"enqueued"`
	NotReplayable  int64      `json:"not_replayable"`
	Delivered      *int64     `json:"delivered,omitempty"`
	Failed         *int64     `json:"failed,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	StartedAt      *time.Time `json:"started_at"`
	FinishedAt     *time.Time `json:"finished_at"`
	Error          *string    `json:"error"`
}

// newJobView converts a job row; the nullable columns stay pointers so SQL
// NULL round-trips as JSON null instead of a zero time or an empty string.
func newJobView(job *Job) *jobView {
	v := &jobView{
		ReplayID:       job.ID,
		SubscriptionID: job.SubscriptionID,
		Status:         job.Status,
		DryRun:         job.DryRun,
		Selector:       job.Selector,
		Matched:        job.Matched,
		Enqueued:       job.Enqueued,
		NotReplayable:  job.NotReplayable,
		CreatedAt:      job.CreatedAt,
		StartedAt:      job.StartedAt,
		FinishedAt:     job.FinishedAt,
	}
	if job.Error != "" {
		msg := job.Error
		v.Error = &msg
	}
	return v
}
