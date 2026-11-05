-- Retain the last attempt across gateway restarts; this is not an official
-- quota observation and must never be presented as a successful refresh.
ALTER TABLE harbor.accounts ADD COLUMN quota_attempted_at BIGINT;
