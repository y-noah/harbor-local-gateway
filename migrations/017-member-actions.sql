ALTER TABLE harbor.members ADD COLUMN access_version BIGINT NOT NULL DEFAULT 0 CHECK(access_version>=0);
ALTER TABLE harbor.keys ADD COLUMN access_version BIGINT NOT NULL DEFAULT 0 CHECK(access_version>=0);
CREATE FUNCTION harbor.member_access_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 NEW.access_version:=OLD.access_version+CASE WHEN NEW.enabled IS DISTINCT FROM OLD.enabled OR NEW.removed_at IS DISTINCT FROM OLD.removed_at THEN 1 ELSE 0 END;
 RETURN NEW;
END $$;
CREATE TRIGGER member_access_revision BEFORE UPDATE ON harbor.members
 FOR EACH ROW EXECUTE FUNCTION harbor.member_access_revision();
CREATE FUNCTION harbor.key_access_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 NEW.access_version:=OLD.access_version+CASE WHEN NEW.enabled IS DISTINCT FROM OLD.enabled OR NEW.expires IS DISTINCT FROM OLD.expires OR NEW.removed_at IS DISTINCT FROM OLD.removed_at THEN 1 ELSE 0 END;
 RETURN NEW;
END $$;
CREATE TRIGGER key_access_revision BEFORE UPDATE ON harbor.keys
 FOR EACH ROW EXECUTE FUNCTION harbor.key_access_revision();
CREATE TABLE harbor.member_action_batches(
 id TEXT PRIMARY KEY,created BIGINT NOT NULL,expires BIGINT NOT NULL CHECK(expires>created),actor TEXT NOT NULL,
 operation JSONB NOT NULL,status TEXT NOT NULL DEFAULT 'preview' CHECK(status IN ('preview','queued','completed','cancelled')),
 direction TEXT NOT NULL DEFAULT 'apply' CHECK(direction IN ('apply','undo')),parent_id TEXT REFERENCES harbor.member_action_batches(id),
 job_id TEXT REFERENCES harbor.background_jobs(id),finished BIGINT
);
CREATE INDEX member_action_created ON harbor.member_action_batches(created DESC,id DESC);
CREATE TABLE harbor.member_action_items(
 batch_id TEXT NOT NULL REFERENCES harbor.member_action_batches(id),ordinal INTEGER NOT NULL CHECK(ordinal>0),
 member_id TEXT NOT NULL REFERENCES harbor.members(id),member_name TEXT NOT NULL,
 selected_keys JSONB,before_state JSONB NOT NULL,desired_state JSONB NOT NULL,notes JSONB NOT NULL DEFAULT '[]',
 status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','success','unchanged','skipped','failed','cancelled')),
 reason TEXT,after_state JSONB,finished BIGINT,PRIMARY KEY(batch_id,ordinal),UNIQUE(batch_id,member_id)
);
CREATE INDEX member_action_pending ON harbor.member_action_items(batch_id,ordinal) WHERE status='pending';
