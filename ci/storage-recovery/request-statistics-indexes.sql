-- Execute each marked statement in autocommit mode with bounded session timeouts.
DO $check$
DECLARE spec RECORD;
BEGIN
  FOR spec IN SELECT * FROM (VALUES
      ('request_member_statistics_cover',ARRAY['usage_at','id','member_id','key_id','account_id','session_id','ip','device_id','device_auth_state','started','ended','status','price_version']::text[]),
      ('request_type_statistics_cover',ARRAY['usage_at','id','price_version','started','ended','business_status','usage_type_id','usage_type_version','usage_type_code','usage_type_name']::text[])
    ) AS selected(index_name,columns) LOOP
    IF EXISTS(SELECT 1 FROM pg_class WHERE oid=to_regclass('harbor.'||spec.index_name)) AND NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
      WHERE c.oid=to_regclass('harbor.'||spec.index_name) AND i.indrelid='harbor.requests'::regclass
        AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
        AND i.indnkeyatts=1 AND i.indnatts=cardinality(spec.columns)
        AND i.indpred IS NULL AND i.indexprs IS NULL AND a.amname='btree' AND i.indoption[0]=0
        AND ARRAY(SELECT pg_get_indexdef(c.oid,parts.ordinal,true) FROM generate_series(1,i.indnatts) AS parts(ordinal) ORDER BY parts.ordinal)=spec.columns)
    THEN RAISE EXCEPTION 'harbor_statistics_index_conflict'; END IF;
  END LOOP;
END $check$;
-- statement-boundary
CREATE INDEX CONCURRENTLY IF NOT EXISTS request_member_statistics_cover ON harbor.requests(usage_at) INCLUDE(id,member_id,key_id,account_id,session_id,ip,device_id,device_auth_state,started,ended,status,price_version);
-- statement-boundary
CREATE INDEX CONCURRENTLY IF NOT EXISTS request_type_statistics_cover ON harbor.requests(usage_at) INCLUDE(id,price_version,started,ended,business_status,usage_type_id,usage_type_version,usage_type_code,usage_type_name);
-- statement-boundary
DO $check$
DECLARE spec RECORD;
BEGIN
  FOR spec IN SELECT * FROM (VALUES
      ('request_member_statistics_cover',ARRAY['usage_at','id','member_id','key_id','account_id','session_id','ip','device_id','device_auth_state','started','ended','status','price_version']::text[]),
      ('request_type_statistics_cover',ARRAY['usage_at','id','price_version','started','ended','business_status','usage_type_id','usage_type_version','usage_type_code','usage_type_name']::text[])
    ) AS selected(index_name,columns) LOOP
    IF NOT EXISTS(SELECT 1 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am a ON a.oid=c.relam
      WHERE c.oid=to_regclass('harbor.'||spec.index_name) AND i.indrelid='harbor.requests'::regclass
        AND i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisunique
        AND i.indnkeyatts=1 AND i.indnatts=cardinality(spec.columns)
        AND i.indpred IS NULL AND i.indexprs IS NULL AND a.amname='btree' AND i.indoption[0]=0
        AND ARRAY(SELECT pg_get_indexdef(c.oid,parts.ordinal,true) FROM generate_series(1,i.indnatts) AS parts(ordinal) ORDER BY parts.ordinal)=spec.columns)
    THEN RAISE EXCEPTION 'harbor_statistics_index_unavailable'; END IF;
  END LOOP;
END $check$;
