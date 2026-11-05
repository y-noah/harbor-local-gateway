CREATE TABLE harbor.employee_devices(
 id TEXT PRIMARY KEY,member_id TEXT NOT NULL REFERENCES harbor.members(id),name TEXT NOT NULL,system TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','approved','disabled','needs_review','replaced')),pending_renewal BOOLEAN NOT NULL DEFAULT FALSE,
 hardware_state TEXT NOT NULL CHECK(hardware_state IN ('reported','unavailable','credentials_only')),
 fingerprint TEXT,installation_fingerprint TEXT,fingerprint_source TEXT NOT NULL,review_fingerprint TEXT,review_installation_fingerprint TEXT,
 allowed_key_ids JSONB,version BIGINT NOT NULL DEFAULT 0,credential_generation BIGINT NOT NULL DEFAULT 0,
 public_key_sha256 TEXT NOT NULL,csr TEXT NOT NULL,
 created BIGINT NOT NULL,approved_at BIGINT,last_seen BIGINT,verified_request_at BIGINT,replaces_device_id TEXT REFERENCES harbor.employee_devices(id),
 enrollment_digest TEXT UNIQUE NOT NULL,enrollment_expires BIGINT NOT NULL,
 CHECK(allowed_key_ids IS NULL OR jsonb_typeof(allowed_key_ids)='array')
);
CREATE INDEX employee_devices_member ON harbor.employee_devices(member_id,created DESC,id);
CREATE TABLE harbor.device_credentials(
 fingerprint TEXT PRIMARY KEY,device_id TEXT NOT NULL REFERENCES harbor.employee_devices(id),generation BIGINT NOT NULL,
 certificate TEXT NOT NULL,issued BIGINT NOT NULL,expires BIGINT NOT NULL,revoked BIGINT,
 UNIQUE(device_id,generation)
);
CREATE INDEX device_credentials_device ON harbor.device_credentials(device_id,generation);
CREATE TABLE harbor.device_invitations(
 id TEXT PRIMARY KEY,member_id TEXT NOT NULL REFERENCES harbor.members(id),digest TEXT UNIQUE NOT NULL,
 created BIGINT NOT NULL,expires BIGINT NOT NULL,consumed BIGINT,revoked BIGINT,
 allowed_key_ids JSONB,replaces_device_id TEXT REFERENCES harbor.employee_devices(id),renews_device_id TEXT REFERENCES harbor.employee_devices(id),
 challenge_id TEXT,challenge_digest TEXT,challenge_expires BIGINT,challenge_count INTEGER NOT NULL DEFAULT 0,
 CHECK(allowed_key_ids IS NULL OR jsonb_typeof(allowed_key_ids)='array')
);
ALTER TABLE harbor.members ADD COLUMN device_mode TEXT NOT NULL DEFAULT 'compatible' CHECK(device_mode IN ('compatible','enforced'));
ALTER TABLE harbor.members ADD COLUMN device_policy_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE harbor.keys ADD COLUMN device_ids JSONB;
ALTER TABLE harbor.keys ADD COLUMN device_policy_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE harbor.keys ADD CHECK(device_ids IS NULL OR jsonb_typeof(device_ids)='array');
ALTER TABLE harbor.requests ADD COLUMN device_id TEXT REFERENCES harbor.employee_devices(id);
ALTER TABLE harbor.requests ADD COLUMN device_name TEXT;
ALTER TABLE harbor.requests ADD COLUMN device_system TEXT;
ALTER TABLE harbor.requests ADD COLUMN device_generation BIGINT;
ALTER TABLE harbor.requests ADD COLUMN device_auth_state TEXT NOT NULL DEFAULT 'historical_unknown'
 CHECK(device_auth_state IN ('authenticated','unverified_hardware','compatible_unverified','historical_unknown'));
CREATE INDEX request_device_usage ON harbor.requests(device_id,usage_at DESC,id DESC);
