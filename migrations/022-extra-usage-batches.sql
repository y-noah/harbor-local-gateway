CREATE TABLE harbor.extra_usage_previews(
 id TEXT PRIMARY KEY,kind TEXT NOT NULL CHECK(kind IN ('policy','accounts')),
 created BIGINT NOT NULL,expires BIGINT NOT NULL,admin_session_ref TEXT NOT NULL,
 expected_policy_version BIGINT NOT NULL,operation JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'preview' CHECK(status IN ('preview','queued','completed')),
 job_id TEXT,result JSONB,finished BIGINT,parent_id TEXT REFERENCES harbor.extra_usage_previews(id)
);
CREATE TABLE harbor.extra_usage_preview_items(
 preview_id TEXT NOT NULL REFERENCES harbor.extra_usage_previews(id),ordinal INTEGER NOT NULL,
 account_id TEXT NOT NULL,account_name TEXT NOT NULL,before_state JSONB NOT NULL,desired_configuration JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','success','unchanged','skipped','failed')),
 reason TEXT,after_configuration JSONB,finished BIGINT,
 PRIMARY KEY(preview_id,ordinal),UNIQUE(preview_id,account_id)
);
CREATE INDEX extra_previews_history ON harbor.extra_usage_previews(created DESC,id DESC);
CREATE INDEX extra_preview_pending ON harbor.extra_usage_preview_items(preview_id,ordinal) WHERE status='pending';
CREATE INDEX extra_holds_account ON harbor.account_extra_holds(account_id);
