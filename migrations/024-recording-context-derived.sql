-- Derived data is keyed by immutable source objects, not by every request that
-- references them. Capture and the default migration target remain unchanged.
CREATE TABLE harbor.recording_context_work(
 object_id TEXT PRIMARY KEY REFERENCES harbor.recording_context_objects(id) ON DELETE CASCADE,
 revision BIGINT NOT NULL DEFAULT 1 CHECK(revision>0),changed BIGINT NOT NULL,
 indexed_revision BIGINT NOT NULL DEFAULT 0,indexed_at BIGINT,last_error TEXT
);
CREATE INDEX recording_context_work_pending ON harbor.recording_context_work(changed,object_id) WHERE revision>indexed_revision;
CREATE FUNCTION harbor.mark_recording_context_work() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state='complete' AND OLD.state IS DISTINCT FROM NEW.state THEN
  INSERT INTO harbor.recording_context_work(object_id,changed) VALUES(NEW.id,NEW.created)
  ON CONFLICT(object_id) DO NOTHING;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER recording_context_completed AFTER UPDATE OF state ON harbor.recording_context_objects
 FOR EACH ROW EXECUTE FUNCTION harbor.mark_recording_context_work();
INSERT INTO harbor.recording_context_work(object_id,changed)
 SELECT id,created FROM harbor.recording_context_objects WHERE state='complete';
CREATE TABLE harbor.recording_context_documents(
 object_id TEXT PRIMARY KEY REFERENCES harbor.recording_context_objects(id) ON DELETE CASCADE,
 source_revision BIGINT NOT NULL,source_key_version TEXT NOT NULL,source_fingerprint BYTEA NOT NULL,
 key_version TEXT NOT NULL,format_version INTEGER NOT NULL,indexed_at BIGINT NOT NULL,
 parts INTEGER NOT NULL CHECK(parts>=0),bytes BIGINT NOT NULL CHECK(bytes>=0)
);
CREATE TABLE harbor.recording_context_document_parts(
 object_id TEXT NOT NULL REFERENCES harbor.recording_context_documents(object_id) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL CHECK(ordinal>0),bytes INTEGER NOT NULL CHECK(bytes>=0),
 payload_cipher BYTEA NOT NULL,search_terms BYTEA[] NOT NULL,
 PRIMARY KEY(object_id,ordinal)
);
CREATE INDEX recording_context_document_search ON harbor.recording_context_document_parts USING GIN(search_terms);
-- Inserted atomically with emitted chunks and export progress. This survives a
-- worker restart, deduplicates objects across batches and protects live exports.
CREATE TABLE harbor.recording_context_export_objects(
 export_id TEXT NOT NULL REFERENCES harbor.recording_exports(id) ON DELETE CASCADE,
 object_id TEXT NOT NULL REFERENCES harbor.recording_context_objects(id),
 PRIMARY KEY(export_id,object_id)
);
CREATE INDEX recording_context_export_object ON harbor.recording_context_export_objects(object_id,export_id);
