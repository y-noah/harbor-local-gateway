-- Install each marked statement in autocommit mode. This is an explicit
-- online performance operation, never an application-startup migration.
DO $check$
BEGIN
 IF EXISTS(SELECT 1 FROM pg_class WHERE oid=to_regclass('harbor.request_session_usage_cover'))
  AND NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
   WHERE c.oid=to_regclass('harbor.request_session_usage_cover') AND i.indrelid='harbor.requests'::regclass
    AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
    AND i.indnkeyatts=2 AND i.indnatts=4 AND i.indpred IS NULL AND i.indexprs IS NULL
    AND a.amname='btree' AND i.indoption[0]=0 AND i.indoption[1]=0
    AND ARRAY(SELECT pg_get_indexdef(c.oid,p.ordinal,true) FROM generate_series(1,i.indnatts) p(ordinal) ORDER BY p.ordinal)
      =ARRAY['session_id','usage_at','id','price_version']::text[])
 THEN RAISE EXCEPTION 'harbor_session_usage_index_conflict'; END IF;
END $check$;
-- statement-boundary
CREATE INDEX CONCURRENTLY IF NOT EXISTS request_session_usage_cover ON harbor.requests(session_id,usage_at) INCLUDE(id,price_version);
-- statement-boundary
DO $check$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
   WHERE c.oid=to_regclass('harbor.request_session_usage_cover') AND i.indrelid='harbor.requests'::regclass
    AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
    AND i.indnkeyatts=2 AND i.indnatts=4 AND i.indpred IS NULL AND i.indexprs IS NULL
    AND a.amname='btree' AND i.indoption[0]=0 AND i.indoption[1]=0
    AND ARRAY(SELECT pg_get_indexdef(c.oid,p.ordinal,true) FROM generate_series(1,i.indnatts) p(ordinal) ORDER BY p.ordinal)
      =ARRAY['session_id','usage_at','id','price_version']::text[])
 THEN RAISE EXCEPTION 'harbor_session_usage_index_unavailable'; END IF;
END $check$;
