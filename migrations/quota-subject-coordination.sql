CREATE TABLE harbor.quota_subjects (
 ref TEXT PRIMARY KEY CHECK (ref ~ '^[a-f0-9]{64}$'),
 scope TEXT NOT NULL CHECK (scope IN ('account_metered_bucket','same_credential')),
 source TEXT NOT NULL,
 schema_version TEXT,
 shared BOOLEAN NOT NULL DEFAULT FALSE,
 checked_from BIGINT,
 checked BIGINT,
 denial_at BIGINT,
 snapshot JSONB,
 CHECK ((checked_from IS NULL AND checked IS NULL AND snapshot IS NULL) OR (checked_from IS NOT NULL AND checked IS NOT NULL AND checked_from >= 0 AND checked >= checked_from AND snapshot IS NOT NULL))
);
CREATE TABLE harbor.quota_subject_accounts (
 account_id TEXT NOT NULL REFERENCES harbor.accounts(id),
 identity_epoch BIGINT NOT NULL,
 subject_ref TEXT NOT NULL REFERENCES harbor.quota_subjects(ref),
 updated BIGINT NOT NULL,
 PRIMARY KEY(account_id,identity_epoch)
);
CREATE INDEX quota_subject_account_ref ON harbor.quota_subject_accounts(subject_ref);
CREATE TABLE harbor.quota_subject_confirmations (
 request_id TEXT PRIMARY KEY REFERENCES harbor.requests(id),
 subject_ref TEXT NOT NULL REFERENCES harbor.quota_subjects(ref),
 statement TEXT NOT NULL CHECK (statement IN ('upstream_finished','not_executed')),
 confirmed_at BIGINT NOT NULL,
 actor_kind TEXT NOT NULL CHECK (actor_kind IN ('super_admin','department_admin')),
 actor_id TEXT NOT NULL,
 actor_version BIGINT NOT NULL,
 reason TEXT NOT NULL
);
REVOKE ALL ON harbor.quota_subject_confirmations FROM PUBLIC;
ALTER TABLE harbor.accounts ADD COLUMN quota_identity_epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE harbor.requests ADD COLUMN quota_subject_ref TEXT REFERENCES harbor.quota_subjects(ref);
ALTER TABLE harbor.requests ADD COLUMN quota_subject_scope TEXT;
ALTER TABLE harbor.requests ADD COLUMN quota_account_identity_epoch BIGINT;
ALTER TABLE harbor.requests ADD COLUMN quota_ordinary BOOLEAN;
ALTER TABLE harbor.requests ADD COLUMN quota_ordinary_denied BOOLEAN;
ALTER TABLE harbor.requests ADD CONSTRAINT quota_subject_request_pair CHECK (
 (quota_subject_ref IS NULL AND quota_subject_scope IS NULL) OR
 (quota_subject_ref IS NOT NULL AND quota_subject_scope IS NOT NULL AND quota_subject_scope IN ('account_metered_bucket','same_credential'))
);
CREATE INDEX quota_subject_request_state ON harbor.requests(quota_subject_ref,ended,status) WHERE quota_subject_ref IS NOT NULL;
CREATE INDEX quota_subject_unbound_account ON harbor.requests(account_id,quota_account_identity_epoch,ended) WHERE quota_subject_ref IS NULL AND quota_ordinary IS DISTINCT FROM FALSE AND quota_account_identity_epoch IS NOT NULL;
REVOKE ALL ON harbor.quota_subjects,harbor.quota_subject_accounts FROM PUBLIC;
CREATE INDEX quota_subject_request_denial ON harbor.requests(account_id,quota_account_identity_epoch,ended) WHERE quota_ordinary_denied IS TRUE;
