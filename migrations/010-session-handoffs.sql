ALTER TABLE harbor.requests ADD COLUMN handoff_context TEXT NOT NULL DEFAULT 'unknown'
 CHECK(handoff_context IN ('unknown','replayable','opaque'));
ALTER TABLE harbor.requests ADD COLUMN handoff_input TEXT NOT NULL DEFAULT 'unknown'
 CHECK(handoff_input IN ('unknown','replayable','opaque'));
ALTER TABLE harbor.sessions ADD COLUMN handoff_requires_replay INTEGER NOT NULL DEFAULT 0
 CHECK(handoff_requires_replay IN (0,1));
CREATE INDEX request_handoff_context ON harbor.requests(session_id,route_version,query_sequence DESC);
CREATE INDEX request_handoff_overlap ON harbor.requests(session_id,route_version,ended DESC)
 WHERE handoff_context<>'replayable';
CREATE TABLE harbor.session_handoffs(
 id TEXT PRIMARY KEY,sequence BIGINT GENERATED ALWAYS AS IDENTITY,session_id TEXT NOT NULL REFERENCES harbor.sessions(id),member_id TEXT NOT NULL REFERENCES harbor.members(id),
 source_account_id TEXT NOT NULL REFERENCES harbor.accounts(id),target_account_id TEXT NOT NULL REFERENCES harbor.accounts(id),
 expected_route_version BIGINT NOT NULL CHECK(expected_route_version>=0),source_epoch BIGINT NOT NULL,target_epoch BIGINT NOT NULL,
 source_nightly_enabled INTEGER NOT NULL,source_nightly_expires BIGINT,pin_expires BIGINT,
 origin TEXT NOT NULL CHECK(origin IN ('manual','automatic')),
 status TEXT NOT NULL CHECK(status IN ('waiting','completed','cancelled','failed','superseded')),
 requested BIGINT NOT NULL,expires BIGINT NOT NULL,finished BIGINT,reason TEXT,applied_route_version BIGINT,
 CHECK(source_account_id<>target_account_id),CHECK(expires>requested),CHECK((status='waiting')=(finished IS NULL)),
 CHECK((status='completed')=(applied_route_version IS NOT NULL)),CHECK(applied_route_version IS NULL OR applied_route_version=expected_route_version+1)
);
CREATE UNIQUE INDEX session_handoff_pending ON harbor.session_handoffs(session_id) WHERE status='waiting';
CREATE INDEX session_handoff_history ON harbor.session_handoffs(session_id,sequence DESC);
CREATE INDEX session_handoff_recovery ON harbor.session_handoffs(requested,id) WHERE status='waiting';
