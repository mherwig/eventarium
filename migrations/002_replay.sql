-- Replay jobs: the control data of the Replay API (section 11). Only job
-- metadata lives here; matched deliveries and their progress counters are
-- resolved from the status store of section 9, which holds no payloads.

CREATE TABLE IF NOT EXISTS replay_jobs (
  id              uuid PRIMARY KEY,
  owner           text NOT NULL,
  subscription_id uuid NOT NULL,
  idempotency_key text,                 -- NULL when the caller sent none
  selector        jsonb NOT NULL,       -- the request body as submitted
  dry_run         boolean NOT NULL DEFAULT false,
  status          text NOT NULL DEFAULT 'pending',
    -- pending | running | cancel_requested | cancelled | done | failed
  matched         bigint NOT NULL DEFAULT 0,
  enqueued        bigint NOT NULL DEFAULT 0,
  not_replayable  bigint NOT NULL DEFAULT 0,
  error           text,
  created_at      timestamptz NOT NULL DEFAULT now(),
  started_at      timestamptz,
  finished_at     timestamptz,
  CONSTRAINT replay_jobs_status_check
    CHECK (status IN ('pending', 'running', 'cancel_requested',
                      'cancelled', 'done', 'failed'))
);

-- A repeated Idempotency-Key returns the existing job instead of enqueuing a
-- second one (section 11: POST /subscriptions/{id}/replays supports
-- Idempotency-Key). The owner is part of the key so two owners never collide.
CREATE UNIQUE INDEX IF NOT EXISTS replay_jobs_idem_idx
  ON replay_jobs (owner, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

-- At most one active job per subscription (section 11, safeguards): the API
-- maps a violation of this index to HTTP 409 with the active replay ID.
CREATE UNIQUE INDEX IF NOT EXISTS replay_jobs_active_idx
  ON replay_jobs (subscription_id)
  WHERE status IN ('pending', 'running', 'cancel_requested');

-- Newest-first listing of a subscription's jobs (section 11, endpoint 4).
CREATE INDEX IF NOT EXISTS replay_jobs_sub_idx
  ON replay_jobs (subscription_id, created_at DESC);
