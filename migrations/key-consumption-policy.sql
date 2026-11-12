-- Independent extension: do not advance or enable optional core migrations.
CREATE FUNCTION harbor.key_consumption_valid(policy TEXT,version BIGINT) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,harbor AS $consumption$
 SELECT COALESCE(policy IN ('inherit','api_only','subscription_only','api_first') AND version BETWEEN 0 AND 9007199254740991,FALSE)
$consumption$;
CREATE FUNCTION harbor.consumption_attempts_valid(attempts JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,harbor AS $consumption$
DECLARE item JSONB;field TEXT;count INTEGER;position INTEGER:=0;
BEGIN
 IF attempts IS NULL OR jsonb_typeof(attempts)<>'array' THEN RETURN FALSE;END IF;
 count:=jsonb_array_length(attempts);IF count>2 THEN RETURN FALSE;END IF;
 FOR item IN SELECT value FROM jsonb_array_elements(attempts) LOOP
  position:=position+1;
  IF jsonb_typeof(item)<>'object' THEN RETURN FALSE;END IF;
  IF EXISTS(SELECT 1 FROM jsonb_object_keys(item) k WHERE k NOT IN ('source','account_id','state','usage_known','started','ended','error_code','input','output','cached','dispatched')) THEN RETURN FALSE;END IF;
  IF NOT COALESCE(jsonb_typeof(item->'source')='string' AND item->>'source' IN ('api','subscription') AND jsonb_typeof(item->'account_id')='string' AND item->>'account_id' ~ '^[a-zA-Z0-9-]{1,80}$' AND jsonb_typeof(item->'state')='string' AND item->>'state' IN ('dispatched','rejected','completed','failed','unknown') AND jsonb_typeof(item->'usage_known')='boolean',FALSE) THEN RETURN FALSE;END IF;
  FOREACH field IN ARRAY ARRAY['started','ended','input','output','cached'] LOOP
   IF item ? field AND item->field<>'null'::jsonb THEN
    IF jsonb_typeof(item->field)<>'number' THEN RETURN FALSE;END IF;
    IF (item->>field)::NUMERIC<0 OR (item->>field)::NUMERIC>9007199254740991 OR mod((item->>field)::NUMERIC,1)<>0 THEN RETURN FALSE;END IF;
   END IF;
  END LOOP;
  IF NOT item ? 'started' OR item->'started'='null'::jsonb THEN RETURN FALSE;END IF;
  IF item ? 'ended' AND item->'ended'<>'null'::jsonb AND (item->>'ended')::NUMERIC<(item->>'started')::NUMERIC THEN RETURN FALSE;END IF;
  IF item ? 'error_code' AND item->'error_code'<>'null'::jsonb AND NOT COALESCE(jsonb_typeof(item->'error_code')='string' AND item->>'error_code' ~ '^[a-z][a-z0-9_]{0,79}$',FALSE) THEN RETURN FALSE;END IF;
  IF item->>'usage_known'='false' AND EXISTS(SELECT 1 FROM unnest(ARRAY['input','output','cached']) f WHERE item ? f AND item->f<>'null'::jsonb) THEN RETURN FALSE;END IF;
  IF item ? 'dispatched' AND jsonb_typeof(item->'dispatched')<>'boolean' THEN RETURN FALSE;END IF;
  IF item->>'dispatched'='false' AND NOT COALESCE(item->>'state' IN ('failed','rejected') AND item->>'usage_known'='true' AND item->'input'='0'::jsonb AND item->'output'='0'::jsonb AND item->'cached'='0'::jsonb,FALSE) THEN RETURN FALSE;END IF;
  IF count=2 AND ((position=1 AND NOT COALESCE(item->>'source'='api' AND item->>'state'='rejected' AND item->>'usage_known'='true' AND item->>'error_code'='upstream_quota_exhausted' AND COALESCE(item->>'dispatched','true')='true',FALSE)) OR (position=2 AND item->>'source'<>'subscription')) THEN RETURN FALSE;END IF;
 END LOOP;
 RETURN TRUE;
END
$consumption$;
CREATE FUNCTION harbor.request_consumption_valid(policy TEXT,version BIGINT,source TEXT,fallback BOOLEAN,attempts JSONB) RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,harbor AS $consumption$
DECLARE count INTEGER;sent_count INTEGER;sent_source TEXT;
BEGIN
 IF policy IS NULL AND version IS NULL THEN RETURN source IS NULL AND fallback IS NULL AND attempts IS NULL;END IF;
 IF NOT harbor.key_consumption_valid(policy,version) OR NOT harbor.consumption_attempts_valid(attempts) OR fallback IS NULL THEN RETURN FALSE;END IF;
 count:=jsonb_array_length(attempts);
 SELECT COUNT(*)::INTEGER INTO sent_count FROM jsonb_array_elements(attempts) a WHERE COALESCE(a->>'dispatched','true')='true';
 IF fallback<>(sent_count=2) OR (count=2 AND policy<>'api_first') THEN RETURN FALSE;END IF;
 IF sent_count=0 THEN RETURN source IS NULL;END IF;
 SELECT a->>'source' INTO sent_source FROM jsonb_array_elements(attempts) WITH ORDINALITY AS items(a,n) WHERE COALESCE(a->>'dispatched','true')='true' ORDER BY n DESC LIMIT 1;
 IF source IS NULL OR source IS DISTINCT FROM sent_source THEN RETURN FALSE;END IF;
 IF policy='api_only' AND EXISTS(SELECT 1 FROM jsonb_array_elements(attempts) a WHERE a->>'source'<>'api') THEN RETURN FALSE;END IF;
 IF policy='subscription_only' AND EXISTS(SELECT 1 FROM jsonb_array_elements(attempts) a WHERE a->>'source'<>'subscription') THEN RETURN FALSE;END IF;
 RETURN TRUE;
END
$consumption$;
ALTER TABLE harbor.keys ADD COLUMN consumption_policy TEXT NOT NULL DEFAULT 'inherit',ADD COLUMN consumption_policy_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE harbor.keys ADD CONSTRAINT keys_consumption_policy_valid CHECK(harbor.key_consumption_valid(consumption_policy,consumption_policy_version));
-- Historical requests stay entirely NULL; no inferred source or fake usage.
ALTER TABLE harbor.requests ADD COLUMN consumption_policy TEXT,ADD COLUMN consumption_policy_version BIGINT,ADD COLUMN consumption_source TEXT,ADD COLUMN consumption_fallback BOOLEAN,ADD COLUMN consumption_attempts JSONB;
ALTER TABLE harbor.requests ADD CONSTRAINT requests_consumption_policy_valid CHECK(harbor.request_consumption_valid(consumption_policy,consumption_policy_version,consumption_source,consumption_fallback,consumption_attempts));
