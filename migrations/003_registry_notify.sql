-- Registry freshness (docs/SPIKE.md section 6.1): every statement that writes
-- the control tables queues a notification on the registry_changed channel,
-- so a LISTENing service reloads its in-memory index as soon as the writing
-- transaction commits instead of at its next poll.
--
-- NOTIFY is transactional: a notification issued inside a transaction is
-- delivered only after that transaction commits, and it is invisible to the
-- reader until then. There is no window in which the index is refreshed but
-- the rows are not there yet - the failure mode this replaces.
--
-- FOR EACH STATEMENT, not FOR EACH ROW: one notification per statement keeps
-- the channel quiet for bulk writes, and the payload only names the table -
-- the readers reload both control tables anyway. Doing this in the trigger
-- rather than in Go means every writer notifies, including bin/admin and a
-- hand-run psql fix; a code path that forgets cannot exist.

CREATE OR REPLACE FUNCTION notify_registry_changed() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('registry_changed', TG_TABLE_NAME);
  RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS registry_notify_event_types ON event_types;
CREATE TRIGGER registry_notify_event_types
  AFTER INSERT OR UPDATE OR DELETE ON event_types
  FOR EACH STATEMENT EXECUTE FUNCTION notify_registry_changed();

DROP TRIGGER IF EXISTS registry_notify_subscriptions ON subscriptions;
CREATE TRIGGER registry_notify_subscriptions
  AFTER INSERT OR UPDATE OR DELETE ON subscriptions
  FOR EACH STATEMENT EXECUTE FUNCTION notify_registry_changed();
