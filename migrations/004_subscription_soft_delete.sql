-- Soft delete for subscriptions (docs/deviations-and-open-points.md, finding
-- "DELETE forgets the subscription immediately").
--
-- A hard DELETE removes the row that the delivery path, the status API and a
-- replay all use to answer "does this subscription exist, and who owns it?".
-- After it, a parked or replayed delivery of that subscription can no longer
-- be attributed, and the status API answers 404 for history that still exists
-- in ClickHouse - the delivery happened, the record of it just lost its
-- subject.
--
-- deleted_at keeps the row. Everything that answers questions *about the
-- platform's current state* - the registry index, the control-plane API -
-- filters it out and therefore behaves exactly as before the finding: an
-- owner can neither see, update nor delete it again. Everything that answers
-- questions *about what happened* - the status API's existence check, a
-- replay's ownership lookup - reads the row including deleted_at, so history
-- stays attributable and replays stay owner-checked.
--
-- The column is nullable rather than a boolean: the timestamp of the deletion
-- is the only fact that separates "gone since ten minutes" (deliveries in
-- flight may still reference it) from "gone since March" (nothing does), and
-- a boolean would throw that away on the day it becomes interesting.
--
-- Migration 003's trigger already covers UPDATE, so this change notifies every
-- registry the moment a subscription is deleted - soft or hard.

ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS deleted_at timestamptz;

-- Deleted subscriptions keep their id, so a partial index leaves the live set
-- small; nothing joins on the deleted rows.
CREATE INDEX IF NOT EXISTS subscriptions_live_idx
  ON subscriptions (owner, event_type)
  WHERE deleted_at IS NULL;
