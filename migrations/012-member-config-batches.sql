CREATE TABLE harbor.member_config_batches(
 id TEXT PRIMARY KEY,created BIGINT NOT NULL,expires BIGINT NOT NULL CHECK(expires>created),
 actor TEXT NOT NULL,scope JSONB NOT NULL,operation JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'preview' CHECK(status IN ('preview','queued','completed','cancelled')),
 parent_id TEXT REFERENCES harbor.member_config_batches(id),direction TEXT NOT NULL DEFAULT 'apply' CHECK(direction IN ('apply','undo')),
 job_id TEXT REFERENCES harbor.background_jobs(id),finished BIGINT
);
CREATE INDEX member_config_batches_created ON harbor.member_config_batches(created DESC,id);
CREATE TABLE harbor.member_config_batch_items(
 batch_id TEXT NOT NULL REFERENCES harbor.member_config_batches(id),ordinal INTEGER NOT NULL CHECK(ordinal>0),
 member_id TEXT NOT NULL REFERENCES harbor.members(id),member_name TEXT NOT NULL,
 before_config JSONB NOT NULL,desired_config JSONB NOT NULL,exceptions JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','success','unchanged','skipped','failed','cancelled')),
 reason TEXT,after_config JSONB,finished BIGINT,
 PRIMARY KEY(batch_id,ordinal),UNIQUE(batch_id,member_id)
);
