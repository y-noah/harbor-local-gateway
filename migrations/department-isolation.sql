-- Independent, checksum-tracked core contract. It is never enabled at startup.
LOCK TABLE harbor.members,harbor.keys,harbor.accounts,harbor.sessions,harbor.requests,
 harbor.member_types,harbor.member_type_templates,harbor.background_jobs,
 harbor.request_query_snapshots,harbor.recording_retention_plans,harbor.recordings,harbor.member_config_batches,harbor.member_action_batches IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE harbor.departments(
 id TEXT PRIMARY KEY,name TEXT NOT NULL CHECK(length(name) BETWEEN 1 AND 60),
 enabled BOOLEAN NOT NULL DEFAULT TRUE,version BIGINT NOT NULL DEFAULT 0 CHECK(version>=0),
 created BIGINT NOT NULL,updated BIGINT NOT NULL
);
INSERT INTO harbor.departments(id,name,created,updated)
 VALUES('department-default','默认部门',floor(extract(epoch FROM clock_timestamp())*1000)::bigint,floor(extract(epoch FROM clock_timestamp())*1000)::bigint);
CREATE TABLE harbor.department_admin_keys(
 id TEXT PRIMARY KEY,department_id TEXT NOT NULL REFERENCES harbor.departments(id),
 digest TEXT NOT NULL UNIQUE CHECK(digest ~ '^[a-f0-9]{64}$'),prefix TEXT NOT NULL,
 label TEXT NOT NULL CHECK(length(label) BETWEEN 1 AND 120),enabled BOOLEAN NOT NULL DEFAULT TRUE,
 version BIGINT NOT NULL DEFAULT 0 CHECK(version>=0),created BIGINT NOT NULL,revoked_at BIGINT,
 UNIQUE(department_id,id),CHECK((enabled AND revoked_at IS NULL) OR NOT enabled)
);
CREATE INDEX department_admin_keys_history ON harbor.department_admin_keys(department_id,created DESC,id DESC);
CREATE TABLE harbor.department_settings(
 department_id TEXT NOT NULL REFERENCES harbor.departments(id),name TEXT NOT NULL,
 value JSONB NOT NULL,version BIGINT NOT NULL DEFAULT 0 CHECK(version>=0),
 PRIMARY KEY(department_id,name)
);
ALTER TABLE harbor.members ADD COLUMN department_id TEXT;
ALTER TABLE harbor.accounts ADD COLUMN department_id TEXT;
ALTER TABLE harbor.keys ADD COLUMN department_id TEXT;
ALTER TABLE harbor.sessions ADD COLUMN department_id TEXT;
ALTER TABLE harbor.requests ADD COLUMN department_id TEXT;
ALTER TABLE harbor.member_types ADD COLUMN department_id TEXT;
ALTER TABLE harbor.member_type_templates ADD COLUMN department_id TEXT;
ALTER TABLE harbor.recording_retention_plans ADD COLUMN department_id TEXT;
ALTER TABLE harbor.member_config_batches ADD COLUMN department_id TEXT;
ALTER TABLE harbor.member_config_batches ADD COLUMN actor_kind TEXT;
ALTER TABLE harbor.member_config_batches ADD COLUMN actor_id TEXT;
ALTER TABLE harbor.member_config_batches ADD COLUMN actor_version BIGINT;
UPDATE harbor.member_config_batches SET department_id='department-default',actor_kind='legacy_admin',actor_id='migration-v1',actor_version=0;
ALTER TABLE harbor.member_config_batches ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.member_config_batches ALTER COLUMN actor_kind SET NOT NULL;
ALTER TABLE harbor.member_config_batches ALTER COLUMN actor_id SET NOT NULL;
ALTER TABLE harbor.member_config_batches ALTER COLUMN actor_version SET NOT NULL;
ALTER TABLE harbor.member_config_batches ADD CONSTRAINT department_member_config_batches_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.member_config_batches ADD CONSTRAINT department_member_config_batches_identity UNIQUE(department_id,id);
ALTER TABLE harbor.member_config_batches ADD CONSTRAINT department_member_config_batches_parent_fk FOREIGN KEY(department_id,parent_id) REFERENCES harbor.member_config_batches(department_id,id);
ALTER TABLE harbor.member_config_batches ADD CONSTRAINT department_member_config_batches_actor CHECK(actor_kind IN ('super_admin','department_admin','legacy_admin') AND length(actor_id)>0 AND actor_version>=0);
CREATE INDEX department_member_config_batches_page ON harbor.member_config_batches(department_id,created DESC,id DESC);
ALTER TABLE harbor.member_action_batches ADD COLUMN department_id TEXT;
ALTER TABLE harbor.member_action_batches ADD COLUMN actor_kind TEXT;
ALTER TABLE harbor.member_action_batches ADD COLUMN actor_id TEXT;
ALTER TABLE harbor.member_action_batches ADD COLUMN actor_version BIGINT;
UPDATE harbor.member_action_batches SET department_id='department-default',actor_kind='legacy_admin',actor_id='migration-v1',actor_version=0;
ALTER TABLE harbor.member_action_batches ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.member_action_batches ALTER COLUMN actor_kind SET NOT NULL;
ALTER TABLE harbor.member_action_batches ALTER COLUMN actor_id SET NOT NULL;
ALTER TABLE harbor.member_action_batches ALTER COLUMN actor_version SET NOT NULL;
ALTER TABLE harbor.member_action_batches ADD CONSTRAINT department_member_action_batches_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.member_action_batches ADD CONSTRAINT department_member_action_batches_identity UNIQUE(department_id,id);
ALTER TABLE harbor.member_action_batches ADD CONSTRAINT department_member_action_batches_parent_fk FOREIGN KEY(department_id,parent_id) REFERENCES harbor.member_action_batches(department_id,id);
ALTER TABLE harbor.member_action_batches ADD CONSTRAINT department_member_action_batches_actor CHECK(actor_kind IN ('super_admin','department_admin','legacy_admin') AND length(actor_id)>0 AND actor_version>=0);
CREATE INDEX department_member_action_batches_page ON harbor.member_action_batches(department_id,created DESC,id DESC);
ALTER TABLE harbor.background_jobs ADD COLUMN department_id TEXT;
ALTER TABLE harbor.background_jobs ADD COLUMN scope_kind TEXT;
ALTER TABLE harbor.background_jobs ADD COLUMN scope_identity TEXT;
ALTER TABLE harbor.background_jobs ADD COLUMN actor_kind TEXT;
ALTER TABLE harbor.background_jobs ADD COLUMN actor_id TEXT;
ALTER TABLE harbor.background_jobs ADD COLUMN actor_version BIGINT;
-- All pre-upgrade rows came from the one original global business namespace.
-- Retain object IDs, credentials, counters, historical NULL links and removals.
UPDATE harbor.members SET department_id='department-default';
UPDATE harbor.accounts SET department_id='department-default';
UPDATE harbor.keys SET department_id='department-default';
UPDATE harbor.sessions SET department_id='department-default';
UPDATE harbor.requests SET department_id='department-default';
UPDATE harbor.member_types SET department_id='department-default';
UPDATE harbor.member_type_templates SET department_id='department-default';
UPDATE harbor.recording_retention_plans SET department_id='department-default';
-- Classify only handlers present in this release. Unknown legacy work blocks
-- installation instead of silently gaining a global or default-department scope.
UPDATE harbor.background_jobs SET scope_kind='global',scope_identity='global',
 actor_kind='system',actor_id='migration-v1',actor_version=0
 WHERE kind IN ('worker.startup','recording.purge','account.extra.recover');
UPDATE harbor.background_jobs SET department_id='department-default',
 scope_kind='department',scope_identity='department:department-default',
 actor_kind='legacy_admin',actor_id='migration-v1',actor_version=0
 WHERE kind IN ('routing.expiry','routing.nightly','routing.preview','member.actions',
 'member.config','account.extra.configure','recording.index','recording.context.index',
 'recording.export','recording.retention');
ALTER TABLE harbor.members ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.accounts ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.keys ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.sessions ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.requests ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.member_types ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.member_type_templates ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.recording_retention_plans ALTER COLUMN department_id SET NOT NULL;
ALTER TABLE harbor.background_jobs ALTER COLUMN scope_kind SET NOT NULL;
ALTER TABLE harbor.background_jobs ALTER COLUMN scope_identity SET NOT NULL;
ALTER TABLE harbor.background_jobs ALTER COLUMN actor_kind SET NOT NULL;
ALTER TABLE harbor.background_jobs ALTER COLUMN actor_id SET NOT NULL;
ALTER TABLE harbor.background_jobs ALTER COLUMN actor_version SET NOT NULL;
ALTER TABLE harbor.background_jobs ADD CONSTRAINT department_job_scope CHECK(
 (scope_kind='global' AND department_id IS NULL AND scope_identity='global'
  AND ((actor_kind='system' AND kind IN ('worker.startup','recording.purge','account.extra.recover'))
   OR (actor_kind='super_admin' AND kind='recording.export')))
 OR (scope_kind='department' AND department_id IS NOT NULL
  AND scope_identity='department:'||department_id
  AND actor_kind IN ('system','super_admin','department_admin','legacy_admin')));
ALTER TABLE harbor.background_jobs ADD CONSTRAINT department_job_actor CHECK(
 length(actor_id) BETWEEN 1 AND 200 AND actor_version>=0
 AND (actor_kind<>'legacy_admin' OR actor_id='migration-v1' AND actor_version=0));
ALTER TABLE harbor.members ADD CONSTRAINT department_members_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.accounts ADD CONSTRAINT department_accounts_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.recording_retention_plans ADD CONSTRAINT department_retention_plans_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.recording_retention_plans ADD CONSTRAINT department_retention_plans_identity UNIQUE(department_id,id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_requests_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.background_jobs ADD CONSTRAINT department_jobs_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.member_types ADD CONSTRAINT department_types_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
ALTER TABLE harbor.members ADD CONSTRAINT department_members_identity UNIQUE(department_id,id);
ALTER TABLE harbor.accounts ADD CONSTRAINT department_accounts_identity UNIQUE(department_id,id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_requests_identity UNIQUE(department_id,id);
ALTER TABLE harbor.keys ADD CONSTRAINT department_keys_identity UNIQUE(department_id,id);
ALTER TABLE harbor.keys ADD CONSTRAINT department_keys_member_identity UNIQUE(department_id,id,member_id);
ALTER TABLE harbor.sessions ADD CONSTRAINT department_sessions_identity UNIQUE(department_id,id);
ALTER TABLE harbor.sessions ADD CONSTRAINT department_sessions_member_identity UNIQUE(department_id,id,member_id);
ALTER TABLE harbor.background_jobs ADD CONSTRAINT department_jobs_identity UNIQUE(scope_identity,id);
ALTER TABLE harbor.member_types ADD CONSTRAINT department_types_identity UNIQUE(department_id,id);
ALTER TABLE harbor.member_type_templates ADD CONSTRAINT department_type_templates_identity UNIQUE(department_id,type_id,version);
ALTER TABLE harbor.member_types DROP CONSTRAINT member_types_code_key;
ALTER TABLE harbor.member_types ADD CONSTRAINT department_type_code UNIQUE(department_id,code);
ALTER TABLE harbor.keys ADD CONSTRAINT department_keys_member_fk FOREIGN KEY(department_id,member_id) REFERENCES harbor.members(department_id,id);
ALTER TABLE harbor.sessions ADD CONSTRAINT department_sessions_member_fk FOREIGN KEY(department_id,member_id) REFERENCES harbor.members(department_id,id);
ALTER TABLE harbor.sessions ADD CONSTRAINT department_sessions_account_fk FOREIGN KEY(department_id,account_id) REFERENCES harbor.accounts(department_id,id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_requests_member_fk FOREIGN KEY(department_id,member_id) REFERENCES harbor.members(department_id,id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_requests_account_fk FOREIGN KEY(department_id,account_id) REFERENCES harbor.accounts(department_id,id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_requests_key_member_fk FOREIGN KEY(department_id,key_id,member_id) REFERENCES harbor.keys(department_id,id,member_id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_requests_session_member_fk FOREIGN KEY(department_id,session_id,member_id) REFERENCES harbor.sessions(department_id,id,member_id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_request_identity_members CHECK((key_id IS NULL OR member_id IS NOT NULL) AND (session_id IS NULL OR member_id IS NOT NULL));
ALTER TABLE harbor.member_type_templates ADD CONSTRAINT department_type_templates_type_fk FOREIGN KEY(department_id,type_id) REFERENCES harbor.member_types(department_id,id);
ALTER TABLE harbor.member_types ADD CONSTRAINT department_type_default_fk FOREIGN KEY(department_id,id,default_version) REFERENCES harbor.member_type_templates(department_id,type_id,version);
ALTER TABLE harbor.members ADD CONSTRAINT department_member_type_fk FOREIGN KEY(department_id,usage_type_id) REFERENCES harbor.member_types(department_id,id);
ALTER TABLE harbor.requests ADD CONSTRAINT department_request_type_fk FOREIGN KEY(department_id,usage_type_id) REFERENCES harbor.member_types(department_id,id);
ALTER TABLE harbor.background_jobs ADD CONSTRAINT department_job_dependency_fk FOREIGN KEY(scope_identity,dependency_id) REFERENCES harbor.background_jobs(scope_identity,id);
ALTER TABLE harbor.request_query_snapshots ADD COLUMN department_id TEXT REFERENCES harbor.departments(id);
ALTER TABLE harbor.request_query_snapshots ADD COLUMN scope_identity TEXT;
UPDATE harbor.request_query_snapshots SET scope_identity='global';
ALTER TABLE harbor.request_query_snapshots ALTER COLUMN scope_identity SET NOT NULL;
ALTER TABLE harbor.request_query_snapshots ADD CONSTRAINT department_snapshot_scope CHECK((department_id IS NULL AND scope_identity='global') OR (department_id IS NOT NULL AND scope_identity='department:'||department_id));
CREATE INDEX department_member_page ON harbor.members(department_id,created DESC,id DESC) WHERE removed_at IS NULL;
CREATE INDEX department_key_page ON harbor.keys(department_id,created DESC,id DESC) WHERE removed_at IS NULL;
CREATE INDEX department_request_page ON harbor.requests(department_id,started DESC,id DESC);
CREATE INDEX department_request_usage ON harbor.requests(department_id,usage_at,id);
CREATE INDEX department_retention_plans_page ON harbor.recording_retention_plans(department_id,created DESC,id DESC);
CREATE INDEX department_snapshot_expiry ON harbor.request_query_snapshots(scope_identity,expires_at,id);
CREATE INDEX department_job_available ON harbor.background_jobs(department_id,kind,available_at,id) WHERE status='pending';
CREATE FUNCTION harbor.department_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.department_id IS DISTINCT FROM OLD.department_id THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='department identity is immutable';
 END IF;
 RETURN NEW;
END $$;
DO $department$
DECLARE name TEXT;
BEGIN
 FOREACH name IN ARRAY ARRAY['members','accounts','keys','sessions','requests','member_types','member_type_templates','background_jobs','recording_retention_plans','member_config_batches','member_action_batches','department_admin_keys'] LOOP
  EXECUTE format('CREATE TRIGGER department_identity_immutable BEFORE UPDATE OF department_id ON harbor.%I FOR EACH ROW EXECUTE FUNCTION harbor.department_identity_immutable()',name);
 END LOOP;
END $department$;

CREATE FUNCTION harbor.department_job_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.scope_kind,NEW.scope_identity,NEW.actor_kind,NEW.actor_id,NEW.actor_version,NEW.kind)
  IS DISTINCT FROM ROW(OLD.scope_kind,OLD.scope_identity,OLD.actor_kind,OLD.actor_id,OLD.actor_version,OLD.kind) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='job scope and actor are immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER department_job_identity_immutable BEFORE UPDATE OF scope_kind,scope_identity,actor_kind,actor_id,actor_version,kind
 ON harbor.background_jobs FOR EACH ROW EXECUTE FUNCTION harbor.department_job_identity_immutable();

CREATE FUNCTION harbor.department_batch_actor_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.actor_kind,NEW.actor_id,NEW.actor_version) IS DISTINCT FROM ROW(OLD.actor_kind,OLD.actor_id,OLD.actor_version) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='batch actor is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER department_batch_actor_immutable BEFORE UPDATE OF actor_kind,actor_id,actor_version ON harbor.member_config_batches FOR EACH ROW EXECUTE FUNCTION harbor.department_batch_actor_immutable();
CREATE TRIGGER department_batch_actor_immutable BEFORE UPDATE OF actor_kind,actor_id,actor_version ON harbor.member_action_batches FOR EACH ROW EXECUTE FUNCTION harbor.department_batch_actor_immutable();

CREATE OR REPLACE FUNCTION harbor.apply_capture_retention() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE configured TEXT;days INTEGER;department TEXT;
BEGIN
 SELECT department_id INTO department FROM harbor.requests WHERE id=NEW.request_id;
 IF department IS NULL THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='recording department is required'; END IF;
 PERFORM pg_advisory_xact_lock_shared(hashtextextended('harbor-recording-retention:'||department,0));
 SELECT value->>'days' INTO configured FROM harbor.department_settings WHERE department_id=department AND name='recording.retention';
 IF configured IS NULL THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='department recording retention is required'; ELSE
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

-- Keep harbor.audit's four-column insert contract. Authority is persisted
-- separately; detail JSON is display data and never supplies ownership.
LOCK TABLE harbor.audit IN SHARE ROW EXCLUSIVE MODE;
CREATE TABLE harbor.audit_scopes(
 audit_id TEXT PRIMARY KEY,
 department_id TEXT,scope_kind TEXT NOT NULL,
 scope_identity TEXT NOT NULL,actor_kind TEXT NOT NULL,actor_id TEXT NOT NULL,
 actor_version BIGINT NOT NULL,subject_kind TEXT,subject_id TEXT,
 CONSTRAINT department_audit_version CHECK(actor_version>=0),
 CONSTRAINT department_audit_scope CHECK((scope_kind='global' AND department_id IS NULL AND scope_identity='global') OR
  (scope_kind='department' AND department_id IS NOT NULL AND scope_identity='department:'||department_id)),
 CONSTRAINT department_audit_actor CHECK(actor_kind IN ('super_admin','department_admin','employee','system','legacy_admin')),
 CONSTRAINT department_audit_subject CHECK(length(actor_id)>0 AND (subject_kind IS NULL)=(subject_id IS NULL))
);
ALTER TABLE harbor.audit_scopes ADD CONSTRAINT department_audit_record_fk FOREIGN KEY(audit_id) REFERENCES harbor.audit(id) ON DELETE CASCADE;
ALTER TABLE harbor.audit_scopes ADD CONSTRAINT department_audit_department_fk FOREIGN KEY(department_id) REFERENCES harbor.departments(id);
-- Before this one-time upgrade, all business records belonged to the original
-- namespace. Pricing is the one existing platform-wide audit producer.
INSERT INTO harbor.audit_scopes(audit_id,department_id,scope_kind,scope_identity,actor_kind,actor_id,actor_version)
 SELECT id,CASE WHEN action='pricing.configure' THEN NULL ELSE 'department-default' END,
 CASE WHEN action='pricing.configure' THEN 'global' ELSE 'department' END,
 CASE WHEN action='pricing.configure' THEN 'global' ELSE 'department:department-default' END,
 'legacy_admin','migration-v1',0 FROM harbor.audit;
CREATE INDEX department_audit_scope ON harbor.audit_scopes(scope_identity,audit_id);
CREATE FUNCTION harbor.audit_scope_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM harbor.audit WHERE id=OLD.audit_id) THEN
   RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='audit scope cannot be removed while its audit exists';
  END IF;
  RETURN OLD;
 END IF;
 IF NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='audit scope is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER audit_scope_immutable BEFORE UPDATE OR DELETE ON harbor.audit_scopes FOR EACH ROW EXECUTE FUNCTION harbor.audit_scope_immutable();
CREATE FUNCTION harbor.require_audit_scope() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE wanted TEXT;
BEGIN
 IF TG_TABLE_NAME='audit' THEN wanted:=NEW.id; ELSE wanted:=OLD.audit_id; END IF;
 IF EXISTS(SELECT 1 FROM harbor.audit WHERE id=wanted) AND NOT EXISTS(SELECT 1 FROM harbor.audit_scopes WHERE audit_id=wanted) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='audit scope is required';
 END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER audit_scope_required AFTER INSERT OR UPDATE ON harbor.audit DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION harbor.require_audit_scope();
CREATE CONSTRAINT TRIGGER audit_scope_delete_guard AFTER DELETE ON harbor.audit_scopes DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION harbor.require_audit_scope();

-- A slot is unique within a department. Plan IDs remain globally unique,
-- while all references include department ownership where a row can join.
LOCK TABLE harbor.routing_plans,harbor.balance_runs,harbor.nightly_group_results,
 harbor.nightly_cycle_ready,harbor.nightly_plan_revisions,harbor.nightly_plan_applications,
 harbor.nightly_plan_group_results,harbor.automatic_handoff_groups,harbor.session_handoffs
 IN SHARE ROW EXCLUSIVE MODE;
ALTER TABLE harbor.routing_plans ADD COLUMN department_id TEXT;
ALTER TABLE harbor.balance_runs ADD COLUMN department_id TEXT;
ALTER TABLE harbor.nightly_group_results ADD COLUMN department_id TEXT;
ALTER TABLE harbor.nightly_cycle_ready ADD COLUMN department_id TEXT;
ALTER TABLE harbor.nightly_plan_revisions ADD COLUMN department_id TEXT;
ALTER TABLE harbor.nightly_plan_applications ADD COLUMN department_id TEXT;
ALTER TABLE harbor.nightly_plan_group_results ADD COLUMN department_id TEXT;
ALTER TABLE harbor.automatic_handoff_groups ADD COLUMN department_id TEXT;
ALTER TABLE harbor.session_handoffs ADD COLUMN department_id TEXT;
ALTER TABLE harbor.session_handoffs ADD COLUMN actor_kind TEXT;
ALTER TABLE harbor.session_handoffs ADD COLUMN actor_id TEXT;
ALTER TABLE harbor.session_handoffs ADD COLUMN actor_version BIGINT;
UPDATE harbor.routing_plans SET department_id='department-default';
UPDATE harbor.balance_runs SET department_id='department-default';
UPDATE harbor.nightly_group_results SET department_id='department-default';
UPDATE harbor.nightly_cycle_ready SET department_id='department-default';
UPDATE harbor.nightly_plan_revisions SET department_id='department-default';
UPDATE harbor.nightly_plan_applications SET department_id='department-default';
UPDATE harbor.nightly_plan_group_results SET department_id='department-default';
UPDATE harbor.automatic_handoff_groups SET department_id='department-default';
UPDATE harbor.session_handoffs SET department_id='department-default';
UPDATE harbor.session_handoffs SET actor_kind=CASE WHEN origin='manual' THEN 'legacy_admin' ELSE 'system' END,
 actor_id=CASE WHEN origin='manual' THEN 'migration-v1' ELSE 'system:routing.nightly' END,actor_version=0;
ALTER TABLE harbor.session_handoffs ALTER COLUMN actor_kind SET NOT NULL;
ALTER TABLE harbor.session_handoffs ALTER COLUMN actor_id SET NOT NULL;
ALTER TABLE harbor.session_handoffs ALTER COLUMN actor_version SET NOT NULL;
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_actor CHECK(actor_version>=0 AND length(actor_id)>0 AND
 ((origin='manual' AND actor_kind IN ('super_admin','department_admin','legacy_admin')) OR
  (origin='automatic' AND actor_kind='system' AND actor_id='system:routing.nightly' AND actor_version=0)));
CREATE TRIGGER department_batch_actor_immutable BEFORE UPDATE OF actor_kind,actor_id,actor_version ON harbor.session_handoffs FOR EACH ROW EXECUTE FUNCTION harbor.department_batch_actor_immutable();
ALTER TABLE harbor.nightly_group_results DROP CONSTRAINT nightly_group_results_day_fkey;
ALTER TABLE harbor.nightly_group_results DROP CONSTRAINT nightly_group_results_pkey;
ALTER TABLE harbor.balance_runs DROP CONSTRAINT balance_runs_pkey;
ALTER TABLE harbor.balance_runs ADD CONSTRAINT balance_runs_pkey PRIMARY KEY(department_id,day);
ALTER TABLE harbor.nightly_group_results ADD CONSTRAINT nightly_group_results_pkey PRIMARY KEY(department_id,day,group_ordinal);
ALTER TABLE harbor.nightly_cycle_ready DROP CONSTRAINT nightly_cycle_ready_pkey;
ALTER TABLE harbor.nightly_cycle_ready ADD CONSTRAINT nightly_cycle_ready_pkey PRIMARY KEY(department_id,day);
ALTER TABLE harbor.nightly_plan_revisions DROP CONSTRAINT nightly_plan_revisions_pkey;
ALTER TABLE harbor.nightly_plan_revisions ADD CONSTRAINT nightly_plan_revisions_pkey PRIMARY KEY(department_id,slot,revision);
ALTER TABLE harbor.background_jobs ADD CONSTRAINT department_jobs_department_identity UNIQUE(department_id,id);
ALTER TABLE harbor.routing_plans ADD CONSTRAINT department_routing_plans_identity UNIQUE(department_id,id);
ALTER TABLE harbor.nightly_plan_applications ADD CONSTRAINT department_nightly_applications_identity UNIQUE(department_id,plan_id);
ALTER TABLE harbor.automatic_handoff_groups ADD CONSTRAINT department_handoff_groups_identity UNIQUE(department_id,id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoffs_identity UNIQUE(department_id,id);
ALTER TABLE harbor.routing_plans ADD CONSTRAINT department_routing_plans_job_fk FOREIGN KEY(department_id,id) REFERENCES harbor.background_jobs(department_id,id);
ALTER TABLE harbor.balance_runs ADD CONSTRAINT department_balance_plan_fk FOREIGN KEY(department_id,plan_id) REFERENCES harbor.routing_plans(department_id,id);
ALTER TABLE harbor.nightly_group_results ADD CONSTRAINT department_nightly_group_run_fk FOREIGN KEY(department_id,day) REFERENCES harbor.balance_runs(department_id,day);
ALTER TABLE harbor.nightly_plan_revisions ADD CONSTRAINT department_nightly_revision_job_fk FOREIGN KEY(department_id,job_id) REFERENCES harbor.background_jobs(department_id,id);
ALTER TABLE harbor.nightly_plan_revisions ADD CONSTRAINT department_nightly_revision_parent_fk FOREIGN KEY(department_id,replaces_plan_id) REFERENCES harbor.routing_plans(department_id,id);
ALTER TABLE harbor.nightly_plan_applications ADD CONSTRAINT department_nightly_application_plan_fk FOREIGN KEY(department_id,plan_id) REFERENCES harbor.routing_plans(department_id,id);
ALTER TABLE harbor.nightly_plan_group_results ADD CONSTRAINT department_nightly_group_application_fk FOREIGN KEY(department_id,plan_id) REFERENCES harbor.nightly_plan_applications(department_id,plan_id);
ALTER TABLE harbor.automatic_handoff_groups ADD CONSTRAINT department_automatic_group_plan_fk FOREIGN KEY(department_id,plan_id) REFERENCES harbor.routing_plans(department_id,id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_session_fk FOREIGN KEY(department_id,session_id) REFERENCES harbor.sessions(department_id,id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_member_fk FOREIGN KEY(department_id,member_id) REFERENCES harbor.members(department_id,id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_session_member_fk FOREIGN KEY(department_id,session_id,member_id) REFERENCES harbor.sessions(department_id,id,member_id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_source_fk FOREIGN KEY(department_id,source_account_id) REFERENCES harbor.accounts(department_id,id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_target_fk FOREIGN KEY(department_id,target_account_id) REFERENCES harbor.accounts(department_id,id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_automatic_fk FOREIGN KEY(department_id,automatic_group_id) REFERENCES harbor.automatic_handoff_groups(department_id,id);
ALTER TABLE harbor.session_handoffs ADD CONSTRAINT department_handoff_replaces_fk FOREIGN KEY(department_id,replaces_handoff_id) REFERENCES harbor.session_handoffs(department_id,id);
DO $history_department$
DECLARE name TEXT;
BEGIN
 FOREACH name IN ARRAY ARRAY['routing_plans','balance_runs','nightly_group_results','nightly_cycle_ready','nightly_plan_revisions','nightly_plan_applications','nightly_plan_group_results','automatic_handoff_groups','session_handoffs'] LOOP
  EXECUTE format('ALTER TABLE harbor.%I ALTER COLUMN department_id SET NOT NULL',name);
  EXECUTE format('ALTER TABLE harbor.%I ADD CONSTRAINT %I FOREIGN KEY(department_id) REFERENCES harbor.departments(id)',name,'department_'||name||'_fk');
  EXECUTE format('CREATE TRIGGER department_identity_immutable BEFORE UPDATE OF department_id ON harbor.%I FOR EACH ROW EXECUTE FUNCTION harbor.department_identity_immutable()',name);
 END LOOP;
END $history_department$;
CREATE INDEX department_balance_page ON harbor.balance_runs(department_id,started DESC,day DESC);
CREATE INDEX department_nightly_application_slot ON harbor.nightly_plan_applications(department_id,slot,started,plan_id);
CREATE INDEX department_routing_plan_page ON harbor.routing_plans(department_id,created,id);
-- Old automatic barriers cannot acquire a new system actor during recovery.
-- Match the pre-upgrade gateway restart behavior without touching bindings,
-- completed decisions or a manual takeover that happens to reference a group.
WITH migration_time AS MATERIALIZED (
 SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint at
), changed_groups AS (
 UPDATE harbor.automatic_handoff_groups g SET status='cancelled',reason='gateway_restart',finished=m.at
 FROM migration_time m WHERE g.status='waiting' RETURNING g.id
), changed_handoffs AS (
 UPDATE harbor.session_handoffs h SET status='cancelled',reason='gateway_restart',finished=m.at
 FROM changed_groups g,migration_time m
 WHERE h.automatic_group_id=g.id AND h.origin='automatic' AND h.status='waiting'
 RETURNING h.id,h.session_id,h.automatic_group_id
), inserted_audits AS (
 INSERT INTO harbor.audit(id,at,action,detail)
 SELECT gen_random_uuid()::text,m.at,'session.handoff.cancelled',jsonb_build_object('handoff',h.id,'session',h.session_id,'group',h.automatic_group_id,'origin','automatic','reason','gateway_restart')::text
 FROM changed_handoffs h,migration_time m RETURNING id
)
INSERT INTO harbor.audit_scopes(audit_id,department_id,scope_kind,scope_identity,actor_kind,actor_id,actor_version)
 SELECT id,'department-default','department','department:department-default','legacy_admin','migration-v1',0 FROM inserted_audits;
CREATE FUNCTION harbor.automatic_group_identity_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.plan_id,NEW.slot,NEW.group_ordinal,NEW.requested,NEW.expires)
  IS DISTINCT FROM ROW(OLD.plan_id,OLD.slot,OLD.group_ordinal,OLD.requested,OLD.expires) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='automatic group identity is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER automatic_group_identity_immutable BEFORE UPDATE OF plan_id,slot,group_ordinal,requested,expires ON harbor.automatic_handoff_groups FOR EACH ROW EXECUTE FUNCTION harbor.automatic_group_identity_immutable();
