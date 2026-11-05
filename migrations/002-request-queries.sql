-- An insertion's xid stays fixed when the request later settles or is repaired.
-- xmin cannot serve this purpose: updates change it and it uses a wrapping xid.
ALTER TABLE harbor.requests ADD COLUMN inserted_xid XID8 NOT NULL DEFAULT pg_current_xact_id();
CREATE TABLE harbor.request_query_snapshots(
 id UUID PRIMARY KEY,visibility PG_SNAPSHOT NOT NULL,window_start BIGINT NOT NULL,window_end BIGINT NOT NULL,
 filters TEXT NOT NULL,created BIGINT NOT NULL,expires_at BIGINT NOT NULL
);
CREATE INDEX request_snapshot_expiration ON harbor.request_query_snapshots(expires_at);
