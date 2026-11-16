-- Explicit optional online performance operation; never installed at application startup.
-- Run each marked statement separately in autocommit as the table owner, with
-- bounded session lock/statement timeouts. Do not wrap concurrent builds in a transaction.
-- Both indexes cover every arm of the existing progress OR, including its
-- parameterized startup arm. No payload/result JSON is copied into either index.
DO $check$
DECLARE spec RECORD;
BEGIN
 FOR spec IN SELECT * FROM (VALUES
      ('background_job_progress_claim',ARRAY['kind','status','claimed_at']::text[]),
      ('background_job_progress_update',ARRAY['kind','status','updated']::text[])
 ) AS selected(index_name,columns) LOOP
  IF EXISTS(SELECT 1 FROM pg_class WHERE oid=to_regclass('harbor.'||spec.index_name)) AND NOT EXISTS(
   SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
   WHERE c.oid=to_regclass('harbor.'||spec.index_name) AND i.indrelid='harbor.background_jobs'::regclass
    AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
    AND i.indnkeyatts=3 AND i.indnatts=3 AND i.indpred IS NULL AND i.indexprs IS NULL
    AND a.amname='btree' AND i.indoption[0]=0 AND i.indoption[1]=0 AND i.indoption[2]=0
    AND ARRAY(SELECT pg_get_indexdef(c.oid,p.ordinal,true) FROM generate_series(1,i.indnatts) p(ordinal) ORDER BY p.ordinal)=spec.columns)
  THEN RAISE EXCEPTION 'harbor_worker_progress_index_conflict'; END IF;
 END LOOP;
END $check$;
-- statement-boundary
CREATE INDEX CONCURRENTLY IF NOT EXISTS background_job_progress_claim ON harbor.background_jobs(kind,status,claimed_at);
-- statement-boundary
CREATE INDEX CONCURRENTLY IF NOT EXISTS background_job_progress_update ON harbor.background_jobs(kind,status,updated);
-- statement-boundary
DO $check$
DECLARE spec RECORD;
BEGIN
 FOR spec IN SELECT * FROM (VALUES
      ('background_job_progress_claim',ARRAY['kind','status','claimed_at']::text[]),
      ('background_job_progress_update',ARRAY['kind','status','updated']::text[])
 ) AS selected(index_name,columns) LOOP
  IF NOT EXISTS(
   SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
   WHERE c.oid=to_regclass('harbor.'||spec.index_name) AND i.indrelid='harbor.background_jobs'::regclass
    AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
    AND i.indnkeyatts=3 AND i.indnatts=3 AND i.indpred IS NULL AND i.indexprs IS NULL
    AND a.amname='btree' AND i.indoption[0]=0 AND i.indoption[1]=0 AND i.indoption[2]=0
    AND ARRAY(SELECT pg_get_indexdef(c.oid,p.ordinal,true) FROM generate_series(1,i.indnatts) p(ordinal) ORDER BY p.ordinal)=spec.columns)
  THEN RAISE EXCEPTION 'harbor_worker_progress_index_unavailable'; END IF;
 END LOOP;
END $check$;
