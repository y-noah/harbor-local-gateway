CREATE TABLE harbor.extra_usage_policy(
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),enabled BOOLEAN NOT NULL DEFAULT FALSE,
 version BIGINT NOT NULL DEFAULT 0 CHECK(version>=0),updated BIGINT NOT NULL
);
INSERT INTO harbor.extra_usage_policy VALUES(1,FALSE,0,0);
CREATE TABLE harbor.account_extra_usage(
 account_id TEXT PRIMARY KEY REFERENCES harbor.accounts(id),mode TEXT NOT NULL DEFAULT 'inherit' CHECK(mode IN ('inherit','deny')),
 version BIGINT NOT NULL DEFAULT 0 CHECK(version>=0),budget JSONB,ledger_basis TEXT,ledger_unit TEXT,
 used_atoms NUMERIC NOT NULL DEFAULT 0 CHECK(used_atoms>=0 AND used_atoms=trunc(used_atoms)),
 reserved_atoms NUMERIC NOT NULL DEFAULT 0 CHECK(reserved_atoms>=0 AND reserved_atoms=trunc(reserved_atoms)),
 unresolved_count BIGINT NOT NULL DEFAULT 0 CHECK(unresolved_count>=0),manual_record JSONB,updated BIGINT NOT NULL,
 CHECK((ledger_basis IS NULL)=(ledger_unit IS NULL)),CHECK(budget IS NULL OR jsonb_typeof(budget)='object')
);
CREATE TABLE harbor.account_extra_entitlements(
 account_id TEXT PRIMARY KEY REFERENCES harbor.accounts(id),version BIGINT NOT NULL CHECK(version>=1),identity_epoch BIGINT NOT NULL,
 capability JSONB,entitlement JSONB,reset_cards JSONB,last_refresh_at BIGINT NOT NULL,last_refresh_state TEXT NOT NULL CHECK(last_refresh_state IN ('success','failed'))
);
-- No request FK: FK key-share could invert finish(request -> account) while
-- runtime reserves under the account lock. Admission checks the request's
-- durable account identity; these immutable facts survive retained history.
CREATE SEQUENCE harbor.extra_recovery_order;
CREATE TABLE harbor.account_extra_holds(
 request_id TEXT PRIMARY KEY,account_id TEXT NOT NULL REFERENCES harbor.accounts(id),identity_epoch BIGINT NOT NULL,
 basis TEXT NOT NULL,unit TEXT NOT NULL,reserve_atoms NUMERIC NOT NULL CHECK(reserve_atoms>0 AND reserve_atoms=trunc(reserve_atoms)),
 state TEXT NOT NULL CHECK(state IN ('reserved','dispatched','unresolved','settled','released')),
 actual_atoms NUMERIC CHECK(actual_atoms>=0 AND actual_atoms=trunc(actual_atoms)),model TEXT NOT NULL,protocol TEXT NOT NULL,
 policy_version BIGINT NOT NULL,configuration_version BIGINT NOT NULL,entitlement_version BIGINT NOT NULL,
 recovery_order BIGINT NOT NULL DEFAULT 0,created BIGINT NOT NULL,dispatched_at BIGINT,settled_at BIGINT,price_revision TEXT,settlement_source TEXT
);
CREATE INDEX extra_holds_recovery ON harbor.account_extra_holds(recovery_order,created,request_id) WHERE state IN ('reserved','dispatched','unresolved');
CREATE TABLE harbor.account_extra_records(
 id TEXT PRIMARY KEY,account_id TEXT NOT NULL REFERENCES harbor.accounts(id),identity_epoch BIGINT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('manual','reset_card_used')),idempotency_key TEXT,document JSONB NOT NULL,created BIGINT NOT NULL,admin_session_ref TEXT NOT NULL,
 UNIQUE(account_id,kind,idempotency_key)
);
CREATE INDEX extra_records_account ON harbor.account_extra_records(account_id,created DESC,id DESC);
