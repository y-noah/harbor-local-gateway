CREATE TABLE harbor.recording_work(
 request_id TEXT PRIMARY KEY REFERENCES harbor.requests(id) ON DELETE CASCADE,
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),changed BIGINT NOT NULL,
 indexed_revision BIGINT NOT NULL DEFAULT 0,indexed_at BIGINT,last_error TEXT
);
CREATE INDEX recording_work_pending ON harbor.recording_work(changed,request_id) WHERE revision>indexed_revision;
CREATE FUNCTION harbor.mark_recording_work() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' AND NEW.persisted_sequence=OLD.persisted_sequence AND NEW.expires_at=OLD.expires_at THEN RETURN NEW; END IF;
 INSERT INTO harbor.recording_work(request_id,changed) VALUES(NEW.request_id,NEW.updated)
 ON CONFLICT(request_id) DO UPDATE SET revision=harbor.recording_work.revision+1,changed=EXCLUDED.changed,last_error=NULL;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_changed AFTER INSERT OR UPDATE OF persisted_sequence,expires_at ON harbor.recordings
 FOR EACH ROW EXECUTE FUNCTION harbor.mark_recording_work();
CREATE FUNCTION harbor.mark_recording_state_work() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.recording_state IS DISTINCT FROM OLD.recording_state AND EXISTS(SELECT 1 FROM harbor.recordings WHERE request_id=NEW.id) THEN
  INSERT INTO harbor.recording_work(request_id,changed) VALUES(NEW.id,COALESCE(NEW.ended,NEW.started))
  ON CONFLICT(request_id) DO UPDATE SET revision=harbor.recording_work.revision+1,changed=EXCLUDED.changed,last_error=NULL;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_state_changed AFTER UPDATE OF recording_state ON harbor.requests
 FOR EACH ROW EXECUTE FUNCTION harbor.mark_recording_state_work();
INSERT INTO harbor.recording_work(request_id,changed) SELECT request_id,updated FROM harbor.recordings;
CREATE TABLE harbor.recording_documents(
 request_id TEXT PRIMARY KEY REFERENCES harbor.recordings(request_id) ON DELETE CASCADE,
 source_revision BIGINT NOT NULL,source_sequence BIGINT NOT NULL,indexed_at BIGINT NOT NULL,
 state TEXT NOT NULL,turn_state TEXT NOT NULL,expires_at BIGINT NOT NULL,
 parts INTEGER NOT NULL,bytes BIGINT NOT NULL,key_version TEXT NOT NULL
);
CREATE INDEX recording_documents_expiry ON harbor.recording_documents(expires_at,request_id);
CREATE TABLE harbor.recording_document_parts(
 request_id TEXT NOT NULL REFERENCES harbor.recording_documents(request_id) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL,kind TEXT NOT NULL,field_id TEXT NOT NULL,payload_cipher BYTEA NOT NULL,
 bytes INTEGER NOT NULL,search_terms BYTEA[] NOT NULL,
 PRIMARY KEY(request_id,ordinal)
);
CREATE INDEX recording_document_search ON harbor.recording_document_parts USING GIN(search_terms);
CREATE TABLE harbor.recording_exports(
 id TEXT PRIMARY KEY REFERENCES harbor.background_jobs(id),created BIGINT NOT NULL,expires BIGINT NOT NULL,
 actor TEXT NOT NULL,options JSONB NOT NULL,visibility pg_snapshot NOT NULL,
 status TEXT NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','complete','cancelled','failed','expired')),
 after_at BIGINT,after_id TEXT,rows BIGINT NOT NULL DEFAULT 0,bytes BIGINT NOT NULL DEFAULT 0,
 chunks BIGINT NOT NULL DEFAULT 0,error_code TEXT,completed BIGINT
);
CREATE INDEX recording_exports_expiry ON harbor.recording_exports(expires,id);
CREATE TABLE harbor.recording_export_chunks(
 export_id TEXT NOT NULL REFERENCES harbor.recording_exports(id) ON DELETE CASCADE,
 ordinal BIGINT NOT NULL,bytes INTEGER NOT NULL,payload_cipher BYTEA NOT NULL,
 PRIMARY KEY(export_id,ordinal)
);
CREATE TABLE harbor.recording_export_sources(
 export_id TEXT NOT NULL REFERENCES harbor.recording_exports(id) ON DELETE CASCADE,request_id TEXT NOT NULL,
 PRIMARY KEY(export_id,request_id)
);
CREATE INDEX recording_export_source_request ON harbor.recording_export_sources(request_id,export_id);
CREATE FUNCTION harbor.expire_recording_exports() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  UPDATE harbor.recording_exports e SET expires=LEAST(e.expires,0) WHERE e.id IN (SELECT export_id FROM harbor.recording_export_sources WHERE request_id=OLD.request_id);
  RETURN OLD;
 END IF;
 IF NEW.expires_at<OLD.expires_at THEN
  UPDATE harbor.recording_exports e SET expires=LEAST(e.expires,NEW.expires_at) WHERE e.id IN (SELECT export_id FROM harbor.recording_export_sources WHERE request_id=NEW.request_id);
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_export_expiry AFTER UPDATE OF expires_at OR DELETE ON harbor.recordings
 FOR EACH ROW EXECUTE FUNCTION harbor.expire_recording_exports();
CREATE INDEX recording_request_session_order ON harbor.requests(session_id,started,id);
CREATE TABLE harbor.recording_retention_plans(
 id TEXT PRIMARY KEY,created BIGINT NOT NULL,expires BIGINT NOT NULL,days INTEGER NOT NULL CHECK(days BETWEEN 1 AND 365),
 requests BIGINT NOT NULL,bytes BIGINT NOT NULL,status TEXT NOT NULL DEFAULT 'preview' CHECK(status IN ('preview','queued','complete','cancelled')),
 job_id TEXT REFERENCES harbor.background_jobs(id),completed BIGINT,summary JSONB NOT NULL DEFAULT '{}'
);
CREATE INDEX recording_retention_expiry ON harbor.recording_retention_plans(expires,id);
CREATE TABLE harbor.recording_retention_items(
 plan_id TEXT NOT NULL REFERENCES harbor.recording_retention_plans(id),request_id TEXT NOT NULL,
 expected_expires BIGINT NOT NULL,target_expires BIGINT NOT NULL,status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','applied','changed','missing')),
 PRIMARY KEY(plan_id,request_id)
);
CREATE INDEX recording_retention_pending ON harbor.recording_retention_items(plan_id,request_id) WHERE status='pending';
CREATE FUNCTION harbor.apply_capture_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE configured TEXT;days INTEGER;
BEGIN
 SELECT value::jsonb->>'days' INTO configured FROM harbor.gateway_settings WHERE name='recording.retention';
 IF configured IS NOT NULL THEN
  IF configured !~ '^[0-9]{1,3}$' THEN RAISE EXCEPTION 'invalid recording retention'; END IF;
  days:=configured::INTEGER;
  IF days<1 OR days>365 THEN RAISE EXCEPTION 'invalid recording retention'; END IF;
  NEW.expires_at:=NEW.updated+days::BIGINT*86400000;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_capture_retention BEFORE INSERT ON harbor.recordings
 FOR EACH ROW EXECUTE FUNCTION harbor.apply_capture_retention();
