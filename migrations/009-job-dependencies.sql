ALTER TABLE harbor.background_jobs ADD COLUMN dependency_id TEXT REFERENCES harbor.background_jobs(id);
ALTER TABLE harbor.background_jobs ADD CONSTRAINT jobs_no_self_dependency CHECK(dependency_id IS NULL OR dependency_id<>id);
CREATE INDEX jobs_dependency ON harbor.background_jobs(dependency_id,status,id) WHERE dependency_id IS NOT NULL;
-- Older queued nightly jobs could precede a recreated expiry task. Restore the
-- explicit prerequisite before making them eligible for a single-slot worker.
INSERT INTO harbor.background_jobs(id,kind,payload,available_at,created,updated)
SELECT 'routing.expiry:'||(j.payload->>'boundary'),'routing.expiry',jsonb_build_object('boundary',j.payload->'boundary'),j.created,j.created,j.updated
FROM harbor.background_jobs j WHERE j.kind='routing.nightly' AND j.status IN ('pending','running')
 AND j.id='routing.nightly:'||j.target AND jsonb_typeof(j.payload->'boundary')='number' AND j.payload->>'boundary' ~ '^[0-9]{1,16}$'
ON CONFLICT(id) DO NOTHING;
UPDATE harbor.background_jobs j SET dependency_id=d.id FROM harbor.background_jobs d
WHERE j.kind='routing.nightly' AND j.id='routing.nightly:'||j.target
 AND d.id='routing.expiry:'||(j.payload->>'boundary') AND d.kind='routing.expiry'
 AND d.payload=jsonb_build_object('boundary',j.payload->'boundary');
