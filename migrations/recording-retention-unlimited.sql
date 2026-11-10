-- Independent, checksum-tracked compatibility extension for schemas 22/23/24.
-- Keep the numbered schema unchanged so existing gateway images can roll back.
ALTER TABLE harbor.recording_retention_plans DROP CONSTRAINT recording_retention_plans_days_check;
ALTER TABLE harbor.recording_retention_plans ADD CONSTRAINT recording_retention_plans_days_check CHECK(days BETWEEN 0 AND 365);
CREATE OR REPLACE FUNCTION harbor.apply_capture_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE configured TEXT;days INTEGER;
BEGIN
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('harbor-recording-retention',0));
 SELECT value::jsonb->>'days' INTO configured FROM harbor.gateway_settings WHERE name='recording.retention';
 IF configured IS NOT NULL THEN
  IF configured !~ '^[0-9]{1,3}$' THEN RAISE EXCEPTION 'invalid recording retention'; END IF;
  days:=configured::INTEGER;
  IF days<0 OR days>365 THEN RAISE EXCEPTION 'invalid recording retention'; END IF;
  IF days=0 THEN
   IF TG_OP='INSERT' THEN
    IF EXISTS(SELECT 1 FROM harbor.requests WHERE id=NEW.request_id AND recording_state='expired') THEN
     NEW.expires_at:=LEAST(NEW.expires_at,NEW.updated);
    ELSE NEW.expires_at:=8640000000000000; END IF;
   ELSIF OLD.expires_at>NEW.updated THEN NEW.expires_at:=8640000000000000;
   ELSE NEW.expires_at:=LEAST(OLD.expires_at,NEW.expires_at); END IF;
  ELSIF TG_OP='INSERT' THEN NEW.expires_at:=NEW.updated+days::BIGINT*86400000;
  ELSIF NEW.expires_at=8640000000000000 THEN
   SELECT LEAST(OLD.expires_at,started+days::BIGINT*86400000) INTO NEW.expires_at
    FROM harbor.requests WHERE id=NEW.request_id;
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_update_retention BEFORE UPDATE OF expires_at ON harbor.recordings
 FOR EACH ROW EXECUTE FUNCTION harbor.apply_capture_retention();
CREATE FUNCTION harbor.sync_unlimited_context_retention() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.expires_at IS DISTINCT FROM OLD.expires_at AND
    (NEW.expires_at=8640000000000000 OR OLD.expires_at=8640000000000000) AND
    to_regclass('harbor.recording_context_snapshots') IS NOT NULL THEN
  EXECUTE 'UPDATE harbor.recording_context_snapshots SET expires_at=$1 WHERE request_id=$2 AND expires_at>$3'
   USING NEW.expires_at,NEW.request_id,NEW.updated;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_unlimited_context_retention AFTER UPDATE OF expires_at ON harbor.recordings
 FOR EACH ROW EXECUTE FUNCTION harbor.sync_unlimited_context_retention();
