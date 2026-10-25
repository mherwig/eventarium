package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job states of the replay_jobs table (migration 002, section 11).
const (
	StatusPending         = "pending"
	StatusRunning         = "running"
	StatusCancelRequested = "cancel_requested"
	StatusCancelled       = "cancelled"
	StatusDone            = "done"
	StatusFailed          = "failed"
)

// jobColumns is the projection every job query scans; keeping it in one place
// keeps the scan order honest.
const jobColumns = `id::text, owner, subscription_id::text,
	COALESCE(idempotency_key, ''), selector, dry_run, status,
	matched, enqueued, not_replayable, COALESCE(error, ''),
	created_at, started_at, finished_at`

// ErrNotFound is returned when the owner has no such replay job. Owner scope
// makes "someone else's job" indistinguishable from "no such job"
// (section 6.7), so both are the same 404.
var ErrNotFound = errors.New("replay: job not found")

// ActiveJobError reports that the subscription already has an active replay
// job (section 11, safeguards: one active job per subscription). It carries
// that job so the API can answer 409 with its replay ID.
type ActiveJobError struct {
	Job *Job
}

func (e *ActiveJobError) Error() string {
	return fmt.Sprintf("replay: subscription already has active job %s", e.Job.ID)
}

// Job is one row of replay_jobs: control data only. The counters are written
// by the worker as it goes, so a GET during the run shows real progress.
type Job struct {
	ID             string
	Owner          string
	SubscriptionID string
	IdempotencyKey string // "" when the caller sent none
	Selector       Selector
	DryRun         bool
	Status         string
	Matched        int64
	Enqueued       int64
	NotReplayable  int64
	Error          string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

// JobStore owns the replay_jobs control data (migration 002).
type JobStore struct {
	pool *pgxpool.Pool
}

// NewJobStore wraps the pgx pool of the shared PostgreSQL store.
func NewJobStore(pool *pgxpool.Pool) *JobStore { return &JobStore{pool: pool} }

// Create inserts a new job. When the caller sent an Idempotency-Key that this
// owner already used, the existing job is returned with created=false and
// nothing is enqueued twice (section 11). When the subscription already has an
// active job, an *ActiveJobError is returned for the 409 (section 11,
// safeguards). Both conflicts are re-checked after the insert lost a race, so
// the partial unique indexes of migration 002 stay the arbiter.
func (s *JobStore) Create(ctx context.Context, owner, subscriptionID, idempotencyKey string, sel Selector) (*Job, bool, error) {
	if idempotencyKey != "" {
		if job, err := s.byIdempotency(ctx, owner, idempotencyKey); err == nil {
			return job, false, nil
		} else if !errors.Is(err, ErrNotFound) {
			return nil, false, err
		}
	}
	job := &Job{
		ID:             uuid.NewString(),
		Owner:          owner,
		SubscriptionID: subscriptionID,
		IdempotencyKey: idempotencyKey,
		Selector:       sel,
		DryRun:         sel.DryRun,
		Status:         StatusPending,
		CreatedAt:      time.Now().UTC(),
	}
	selectorJSON, err := json.Marshal(sel)
	if err != nil {
		return nil, false, fmt.Errorf("replay: encoding selector: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO replay_jobs (id, owner, subscription_id, idempotency_key, selector, dry_run, status)
		VALUES ($1::uuid, $2, $3::uuid, NULLIF($4, ''), $5, $6, $7)`,
		job.ID, job.Owner, job.SubscriptionID, job.IdempotencyKey, selectorJSON, job.DryRun, StatusPending)
	if err == nil {
		return job, true, nil
	}
	var pge *pgconn.PgError
	if !errors.As(err, &pge) || pge.Code != "23505" {
		return nil, false, fmt.Errorf("replay: creating job: %w", err)
	}
	switch pge.ConstraintName {
	case "replay_jobs_idem_idx":
		// Lost a race with a concurrent request carrying the same key: the
		// existing job wins, the caller still gets its 202.
		job, err := s.byIdempotency(ctx, owner, idempotencyKey)
		if err != nil {
			return nil, false, err
		}
		return job, false, nil
	case "replay_jobs_active_idx":
		active, err := s.activeJob(ctx, subscriptionID)
		if err != nil {
			return nil, false, err
		}
		return nil, false, &ActiveJobError{Job: active}
	default:
		return nil, false, fmt.Errorf("replay: creating job: %w", err)
	}
}

// Get returns one job of the owner, or ErrNotFound (section 6.7).
func (s *JobStore) Get(ctx context.Context, owner, id string) (*Job, error) {
	return s.scan(s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+` FROM replay_jobs WHERE owner = $1 AND id = $2::uuid`, owner, id))
}

// List returns the jobs of one subscription, newest first (section 11,
// endpoint 4). It holds job rows only; the live ClickHouse counters belong to
// GET /replays/{id}.
func (s *JobStore) List(ctx context.Context, owner, subscriptionID string, limit int) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColumns+` FROM replay_jobs
		WHERE owner = $1 AND subscription_id = $2::uuid
		ORDER BY created_at DESC, id DESC
		LIMIT $3`, owner, subscriptionID, limit)
	if err != nil {
		return nil, fmt.Errorf("replay: listing jobs: %w", err)
	}
	defer rows.Close()
	out := make([]Job, 0, 16)
	for rows.Next() {
		// pgx.Rows satisfies pgx.Row, so rows scan exactly like a QueryRow.
		job, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *job)
	}
	return out, rows.Err()
}

// NextPendingID returns the oldest pending job, so jobs run in creation order.
func (s *JobStore) NextPendingID(ctx context.Context) (string, bool, error) {
	var id string
	err := s.pool.QueryRow(ctx, `
		SELECT id::text FROM replay_jobs WHERE status = $1
		ORDER BY created_at, id LIMIT 1`, StatusPending).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("replay: looking for a pending job: %w", err)
	}
	return id, true, nil
}

// Claim flips exactly one pending job to running. The conditional UPDATE is
// the lock: a second claimer of the same row gets zero rows and moves on.
func (s *JobStore) Claim(ctx context.Context, id string) (*Job, bool, error) {
	row := s.pool.QueryRow(ctx, `
		UPDATE replay_jobs SET status = $2, started_at = now()
		WHERE id = $1::uuid AND status = $3
		RETURNING `+jobColumns, id, StatusRunning, StatusPending)
	job, err := s.scan(row)
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	return job, err == nil, err
}

// Status returns the current state of a job; the worker reads it at every
// batch boundary to notice a cancellation (section 11, cancel endpoint).
func (s *JobStore) Status(ctx context.Context, id string) (string, error) {
	var st string
	err := s.pool.QueryRow(ctx, `SELECT status FROM replay_jobs WHERE id = $1::uuid`, id).Scan(&st)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotFound
	}
	return st, err
}

// UpdateProgress persists the counters after every batch, so a crash (and the
// API) can see how far a job got.
func (s *JobStore) UpdateProgress(ctx context.Context, id string, matched, enqueued, notReplayable int64) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE replay_jobs SET matched = $2, enqueued = $3, not_replayable = $4
		WHERE id = $1::uuid`, id, matched, enqueued, notReplayable)
	if err != nil {
		return fmt.Errorf("replay: updating progress: %w", err)
	}
	return nil
}

// Finish writes the terminal state of a job together with its final counters.
// errMsg is stored as NULL when empty.
func (s *JobStore) Finish(ctx context.Context, id, status string, matched, enqueued, notReplayable int64, errMsg string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE replay_jobs
		SET status = $2, matched = $3, enqueued = $4, not_replayable = $5,
		    error = NULLIF($6, ''), finished_at = now()
		WHERE id = $1::uuid`, id, status, matched, enqueued, notReplayable, errMsg)
	if err != nil {
		return fmt.Errorf("replay: finishing job: %w", err)
	}
	return nil
}

// RequestCancel moves a job towards cancellation in one conditional statement
// (race free against the worker): pending jobs are cancelled immediately,
// running ones are flagged cancel_requested so the worker stops at the next
// batch boundary (section 11). applied=false means the job had already
// finished — the caller answers 409 then. ErrNotFound means the owner has no
// such job, which is the same 404 as a foreign one (section 6.7).
func (s *JobStore) RequestCancel(ctx context.Context, owner, id string) (job *Job, applied bool, err error) {
	job, err = s.scan(s.pool.QueryRow(ctx, `
		UPDATE replay_jobs
		SET status = CASE WHEN status = $3 THEN $4 ELSE $5 END,
		    finished_at = CASE WHEN status = $3 THEN now() END
		WHERE owner = $1 AND id = $2::uuid
		  AND status IN ($3, $6, $5)
		RETURNING `+jobColumns,
		owner, id, StatusPending, StatusCancelled, StatusCancelRequested, StatusRunning))
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, false, err
	}
	// No row moved: either it does not exist for this owner (404) or it is
	// already done, failed or cancelled (409).
	job, err = s.Get(ctx, owner, id)
	if err != nil {
		return nil, false, err
	}
	return job, false, nil
}

// ResetInterrupted is the startup recovery of the worker (section 11 plus
// section 8: a restart may produce duplicates, which consumers handle
// idempotently by event reference). Jobs that were running are put back to
// pending and re-execute from the start, which may enqueue some deliveries
// twice; jobs that only waited for their cancellation to take effect are
// finished as cancelled right away instead of being resurrected.
func (s *JobStore) ResetInterrupted(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE replay_jobs SET status = $1, started_at = NULL WHERE status = $2`,
		StatusPending, StatusRunning); err != nil {
		return fmt.Errorf("replay: resetting interrupted jobs: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
		UPDATE replay_jobs SET status = $1, finished_at = now() WHERE status = $2`,
		StatusCancelled, StatusCancelRequested); err != nil {
		return fmt.Errorf("replay: finishing cancelled jobs: %w", err)
	}
	return nil
}

func (s *JobStore) byIdempotency(ctx context.Context, owner, key string) (*Job, error) {
	return s.scan(s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+` FROM replay_jobs WHERE owner = $1 AND idempotency_key = $2`, owner, key))
}

func (s *JobStore) activeJob(ctx context.Context, subscriptionID string) (*Job, error) {
	return s.scan(s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+` FROM replay_jobs
		WHERE subscription_id = $1::uuid
		  AND status IN ('pending', 'running', 'cancel_requested')
		ORDER BY created_at LIMIT 1`, subscriptionID))
}

// scan reads one job row; a missing row becomes ErrNotFound so callers can map
// it to 404 without string matching.
func (s *JobStore) scan(row pgx.Row) (*Job, error) {
	var (
		job      Job
		selector []byte
		subID    string
	)
	err := row.Scan(&job.ID, &job.Owner, &subID, &job.IdempotencyKey,
		&selector, &job.DryRun, &job.Status, &job.Matched, &job.Enqueued,
		&job.NotReplayable, &job.Error, &job.CreatedAt, &job.StartedAt, &job.FinishedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("replay: reading job: %w", err)
	}
	job.SubscriptionID = subID
	if err := json.Unmarshal(selector, &job.Selector); err != nil {
		return nil, fmt.Errorf("replay: decoding stored selector: %w", err)
	}
	return &job, nil
}
