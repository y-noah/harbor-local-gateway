-- Run each marked statement in autocommit mode. Do not put this file inside
-- the schema migration transaction: the live service must keep accepting writes.
DO $check$
BEGIN
  IF EXISTS(SELECT 1 FROM pg_class WHERE oid=to_regclass('harbor.request_overview_cover'))
     AND NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
       WHERE c.oid=to_regclass('harbor.request_overview_cover') AND i.indrelid='harbor.requests'::regclass
         AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
         AND i.indnkeyatts=1 AND i.indnatts=2 AND i.indpred IS NULL AND i.indexprs IS NULL
         AND a.amname='btree' AND i.indoption[0]=0
         AND pg_get_indexdef(c.oid,1,true)='usage_at' AND pg_get_indexdef(c.oid,2,true)='status')
  THEN RAISE EXCEPTION 'harbor_overview_index_conflict'; END IF;
END $check$;
-- statement-boundary
CREATE INDEX CONCURRENTLY IF NOT EXISTS request_overview_cover ON harbor.requests(usage_at) INCLUDE(status);
-- statement-boundary
DO $check$
BEGIN
  IF NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
       WHERE c.oid=to_regclass('harbor.request_overview_cover') AND i.indrelid='harbor.requests'::regclass
         AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
         AND i.indnkeyatts=1 AND i.indnatts=2 AND i.indpred IS NULL AND i.indexprs IS NULL
         AND a.amname='btree' AND i.indoption[0]=0
         AND pg_get_indexdef(c.oid,1,true)='usage_at' AND pg_get_indexdef(c.oid,2,true)='status')
  THEN RAISE EXCEPTION 'harbor_overview_index_unavailable'; END IF;
END $check$;
