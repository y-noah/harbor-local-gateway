-- Additive context storage. Not yet enrolled in the core migration list:
-- a separately verified compatibility release is required before activation.
CREATE TABLE harbor.recording_context_objects(
 id TEXT PRIMARY KEY,scope_digest BYTEA NOT NULL,fingerprint BYTEA NOT NULL,key_version TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('instruction','history','tool_schema','context')),
 state TEXT NOT NULL CHECK(state IN ('writing','complete')),
 bytes BIGINT NOT NULL CHECK(bytes>=0),parts INTEGER NOT NULL CHECK(parts>0),created BIGINT NOT NULL,
 UNIQUE(scope_digest,fingerprint,key_version)
);
CREATE TABLE harbor.recording_context_parts(
 object_id TEXT NOT NULL REFERENCES harbor.recording_context_objects(id) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL CHECK(ordinal>0),bytes INTEGER NOT NULL CHECK(bytes>0),content_cipher BYTEA NOT NULL,
 PRIMARY KEY(object_id,ordinal)
);
CREATE TABLE harbor.recording_context_snapshots(
 request_id TEXT PRIMARY KEY REFERENCES harbor.recordings(request_id) ON DELETE CASCADE,
 member_id TEXT NOT NULL REFERENCES harbor.members(id),session_id TEXT,
 scope_digest BYTEA NOT NULL,fingerprint BYTEA NOT NULL,key_version TEXT NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('complete','partial','truncated')),reason TEXT,coverage JSONB,
 seen INTEGER NOT NULL CHECK(seen>=0),components INTEGER NOT NULL CHECK(components>=0),bytes BIGINT NOT NULL CHECK(bytes>=0),
 expires_at BIGINT NOT NULL,created BIGINT NOT NULL
);
CREATE INDEX recording_context_snapshot_expiry ON harbor.recording_context_snapshots(expires_at,request_id);
CREATE TABLE harbor.recording_context_references(
 request_id TEXT NOT NULL REFERENCES harbor.recording_context_snapshots(request_id) ON DELETE CASCADE,
 ordinal INTEGER NOT NULL CHECK(ordinal>0),object_id TEXT NOT NULL REFERENCES harbor.recording_context_objects(id),
 truncated BOOLEAN NOT NULL DEFAULT false,
 PRIMARY KEY(request_id,ordinal)
);
CREATE INDEX recording_context_reference_object ON harbor.recording_context_references(object_id,request_id);
CREATE INDEX recording_context_object_scope ON harbor.recording_context_objects(scope_digest,id);
