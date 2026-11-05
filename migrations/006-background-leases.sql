ALTER TABLE harbor.background_jobs ADD COLUMN claim_token TEXT;
ALTER TABLE harbor.background_jobs ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 5 CHECK(max_attempts BETWEEN 1 AND 20);
CREATE INDEX jobs_expired_leases ON harbor.background_jobs(lease_until,id) WHERE status='running';
CREATE INDEX sessions_nightly_expiry ON harbor.sessions(nightly_expires,id) WHERE nightly_enabled=0 AND nightly_expires IS NOT NULL;
CREATE INDEX jobs_unfinished ON harbor.background_jobs(status,id) WHERE status IN ('pending','running');
