-- Startup reads only unfinished work; do not scan retained request history.
CREATE INDEX request_unfinished_recovery ON harbor.requests(started,id)
 WHERE ended IS NULL AND status IN ('queued','running');
CREATE INDEX request_recording_pending ON harbor.requests(id)
 WHERE recording_state='pending';
