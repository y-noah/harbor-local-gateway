-- Independent checksum-tracked extension. Core schema 22/23/24 stays unchanged.
CREATE TABLE harbor.usage_rollup_state(singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),version INTEGER NOT NULL CHECK(version=1),state TEXT NOT NULL CHECK(state IN ('backfilling','ready','disabled')),cursor TEXT,checked BIGINT);
INSERT INTO harbor.usage_rollup_state(singleton,version,state) VALUES(TRUE,1,'backfilling');
CREATE TABLE harbor.usage_contributions(request_id TEXT PRIMARY KEY,request_fact JSONB,price_fact JSONB);
CREATE TABLE harbor.usage_hour_buckets(
 bucket_key TEXT PRIMARY KEY,scope TEXT NOT NULL CHECK(scope IN ('member','key','account')),owner_id TEXT NOT NULL,hour_at BIGINT NOT NULL,revision TEXT,projected_revision TEXT,price_state TEXT,
 requests BIGINT NOT NULL DEFAULT 0,
 known BIGINT NOT NULL DEFAULT 0,
 pending BIGINT NOT NULL DEFAULT 0,
 expired BIGINT NOT NULL DEFAULT 0,
 unpriced BIGINT NOT NULL DEFAULT 0,
 unmetered BIGINT NOT NULL DEFAULT 0,
 input NUMERIC NOT NULL DEFAULT 0,
 output NUMERIC NOT NULL DEFAULT 0,
 cached NUMERIC NOT NULL DEFAULT 0,
 adjustment NUMERIC NOT NULL DEFAULT 0,
 total NUMERIC NOT NULL DEFAULT 0,
 CONSTRAINT usage_hour_counts CHECK(requests>=0 AND known>=0 AND pending>=0 AND expired>=0 AND unpriced>=0 AND unmetered>=0 AND requests=known+pending+expired+unpriced+unmetered),
 CONSTRAINT usage_hour_empty CHECK(requests<>0 OR (input=0 AND output=0 AND cached=0 AND adjustment=0 AND total=0)),
 CONSTRAINT usage_hour_boundary CHECK(mod(hour_at,3600000)=0),
 CONSTRAINT usage_hour_identity CHECK(bucket_key=jsonb_build_array(scope,owner_id,hour_at,revision,projected_revision,price_state)::text)
);
CREATE INDEX usage_hour_lookup ON harbor.usage_hour_buckets(scope,owner_id,hour_at);
CREATE FUNCTION harbor.usage_prepare_staging() RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
BEGIN
 IF to_regclass('pg_temp.harbor_usage_stage') IS NULL THEN
  CREATE TEMP TABLE harbor_usage_flushed(singleton BOOLEAN PRIMARY KEY CHECK(singleton)) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE harbor_usage_stage(request_id TEXT PRIMARY KEY,request_dirty BOOLEAN NOT NULL DEFAULT FALSE,price_dirty BOOLEAN NOT NULL DEFAULT FALSE,request_fact JSONB,price_fact JSONB) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE harbor_usage_delta(LIKE harbor.usage_hour_buckets INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DELETE ROWS;
  ALTER TABLE pg_temp.harbor_usage_delta DROP CONSTRAINT usage_hour_counts;
  ALTER TABLE pg_temp.harbor_usage_delta DROP CONSTRAINT usage_hour_empty;
 END IF;
END
$usage$;
CREATE FUNCTION harbor.usage_stage_source() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
DECLARE previous_id TEXT;current_id TEXT;fact JSONB;
BEGIN
 PERFORM harbor.usage_prepare_staging();
 IF EXISTS(SELECT 1 FROM pg_temp.harbor_usage_flushed) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='小时汇总已完成本事务归并，禁止继续修改请求或价格';END IF;
 IF TG_TABLE_NAME='requests' THEN
  IF TG_OP<>'INSERT' THEN previous_id:=OLD.id; END IF;
  IF TG_OP<>'DELETE' THEN current_id:=NEW.id;fact:=jsonb_build_object('member_id',NEW.member_id,'key_id',NEW.key_id,'account_id',NEW.account_id,'usage_at',NEW.usage_at,'price_version',NEW.price_version);END IF;
  IF previous_id IS NOT NULL AND previous_id IS DISTINCT FROM current_id THEN
   INSERT INTO pg_temp.harbor_usage_stage(request_id,request_dirty,request_fact) VALUES(previous_id,TRUE,NULL) ON CONFLICT(request_id) DO UPDATE SET request_dirty=TRUE,request_fact=NULL;
  END IF;
  IF current_id IS NOT NULL THEN
   INSERT INTO pg_temp.harbor_usage_stage(request_id,request_dirty,request_fact) VALUES(current_id,TRUE,fact) ON CONFLICT(request_id) DO UPDATE SET request_dirty=TRUE,request_fact=EXCLUDED.request_fact;
  END IF;
 ELSE
  IF TG_OP<>'INSERT' THEN previous_id:=OLD.request_id; END IF;
  IF TG_OP<>'DELETE' THEN current_id:=NEW.request_id;fact:=jsonb_build_object('state',NEW.state,'revision',NEW.revision,'input_atoms',NEW.input_atoms,'output_atoms',NEW.output_atoms,'cached_atoms',NEW.cached_atoms,'adjustment_atoms',NEW.adjustment_atoms,'total_atoms',NEW.total_atoms);END IF;
  IF previous_id IS NOT NULL AND previous_id IS DISTINCT FROM current_id THEN
   INSERT INTO pg_temp.harbor_usage_stage(request_id,price_dirty,price_fact) VALUES(previous_id,TRUE,NULL) ON CONFLICT(request_id) DO UPDATE SET price_dirty=TRUE,price_fact=NULL;
  END IF;
  IF current_id IS NOT NULL THEN
   INSERT INTO pg_temp.harbor_usage_stage(request_id,price_dirty,price_fact) VALUES(current_id,TRUE,fact) ON CONFLICT(request_id) DO UPDATE SET price_dirty=TRUE,price_fact=EXCLUDED.price_fact;
  END IF;
 END IF;
 RETURN NULL;
END
$usage$;
CREATE FUNCTION harbor.usage_add_delta(request_fact JSONB,price_fact JSONB,direction INTEGER) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
DECLARE dimension TEXT;owner TEXT;bucket BIGINT;version TEXT;projected TEXT;state TEXT;identity TEXT;known_value BIGINT;missing BOOLEAN;
BEGIN
 IF request_fact IS NULL THEN RETURN;END IF;
 bucket:=floor((request_fact->>'usage_at')::NUMERIC/3600000)::BIGINT*3600000;version:=request_fact->>'price_version';projected:=price_fact->>'revision';state:=price_fact->>'state';missing:=price_fact->>'total_atoms' IS NULL;known_value:=CASE WHEN missing THEN 0 ELSE 1 END;
 FOREACH dimension IN ARRAY ARRAY['member','key','account'] LOOP
  owner:=request_fact->>(dimension||'_id');IF owner IS NULL THEN CONTINUE;END IF;
  identity:=jsonb_build_array(dimension,owner,bucket,version,projected,state)::TEXT;
  INSERT INTO pg_temp.harbor_usage_delta(bucket_key,scope,owner_id,hour_at,revision,projected_revision,price_state,requests,known,pending,expired,unpriced,unmetered,input,output,cached,adjustment,total)
  VALUES(identity,dimension,owner,bucket,version,projected,state,direction,direction*known_value,
   direction*CASE WHEN missing AND state='pending' THEN 1 ELSE 0 END,
   direction*CASE WHEN missing AND state='expired' THEN 1 ELSE 0 END,
   direction*CASE WHEN missing AND (state='unpriced' OR state IS NULL) THEN 1 ELSE 0 END,
   direction*CASE WHEN missing AND state NOT IN ('pending','expired','unpriced') THEN 1 ELSE 0 END,
   direction*COALESCE((price_fact->>'input_atoms')::NUMERIC,0),direction*COALESCE((price_fact->>'output_atoms')::NUMERIC,0),direction*COALESCE((price_fact->>'cached_atoms')::NUMERIC,0),direction*COALESCE((price_fact->>'adjustment_atoms')::NUMERIC,0),direction*COALESCE((price_fact->>'total_atoms')::NUMERIC,0))
  ON CONFLICT(bucket_key) DO UPDATE SET requests=harbor_usage_delta.requests+EXCLUDED.requests,known=harbor_usage_delta.known+EXCLUDED.known,pending=harbor_usage_delta.pending+EXCLUDED.pending,expired=harbor_usage_delta.expired+EXCLUDED.expired,unpriced=harbor_usage_delta.unpriced+EXCLUDED.unpriced,unmetered=harbor_usage_delta.unmetered+EXCLUDED.unmetered,input=harbor_usage_delta.input+EXCLUDED.input,output=harbor_usage_delta.output+EXCLUDED.output,cached=harbor_usage_delta.cached+EXCLUDED.cached,adjustment=harbor_usage_delta.adjustment+EXCLUDED.adjustment,total=harbor_usage_delta.total+EXCLUDED.total;
 END LOOP;
END
$usage$;
CREATE FUNCTION harbor.flush_usage_rollups() RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
DECLARE changed RECORD;old_request JSONB;old_price JSONB;new_request JSONB;new_price JSONB;delta RECORD;
BEGIN
 IF to_regclass('pg_temp.harbor_usage_stage') IS NULL THEN RETURN;END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_temp.harbor_usage_stage) THEN RETURN;END IF;
 INSERT INTO pg_temp.harbor_usage_flushed VALUES(TRUE) ON CONFLICT DO NOTHING;
 -- Acquire each ledger key once in total order before acquiring any bucket key.
 FOR changed IN SELECT * FROM pg_temp.harbor_usage_stage ORDER BY request_id COLLATE "C" LOOP
  INSERT INTO harbor.usage_contributions(request_id) VALUES(changed.request_id) ON CONFLICT DO NOTHING;
  SELECT request_fact,price_fact INTO old_request,old_price FROM harbor.usage_contributions WHERE request_id=changed.request_id FOR UPDATE;
  new_request:=CASE WHEN changed.request_dirty THEN changed.request_fact ELSE old_request END;
  new_price:=CASE WHEN changed.price_dirty THEN changed.price_fact ELSE old_price END;
  IF new_request IS DISTINCT FROM old_request OR new_price IS DISTINCT FROM old_price THEN
   PERFORM harbor.usage_add_delta(old_request,old_price,-1);PERFORM harbor.usage_add_delta(new_request,new_price,1);
  END IF;
  -- Every dirty source transaction leaves a new MVCC witness, even a tombstone.
  UPDATE harbor.usage_contributions SET request_fact=new_request,price_fact=new_price WHERE request_id=changed.request_id;
 END LOOP;
 FOR delta IN SELECT * FROM pg_temp.harbor_usage_delta WHERE requests<>0 OR known<>0 OR pending<>0 OR expired<>0 OR unpriced<>0 OR unmetered<>0 OR input<>0 OR output<>0 OR cached<>0 OR adjustment<>0 OR total<>0 ORDER BY bucket_key COLLATE "C" LOOP
  INSERT INTO harbor.usage_hour_buckets(bucket_key,scope,owner_id,hour_at,revision,projected_revision,price_state) VALUES(delta.bucket_key,delta.scope,delta.owner_id,delta.hour_at,delta.revision,delta.projected_revision,delta.price_state) ON CONFLICT DO NOTHING;
  UPDATE harbor.usage_hour_buckets b SET requests=b.requests+delta.requests,known=b.known+delta.known,pending=b.pending+delta.pending,expired=b.expired+delta.expired,unpriced=b.unpriced+delta.unpriced,unmetered=b.unmetered+delta.unmetered,input=b.input+delta.input,output=b.output+delta.output,cached=b.cached+delta.cached,adjustment=b.adjustment+delta.adjustment,total=b.total+delta.total WHERE b.bucket_key=delta.bucket_key;
  DELETE FROM harbor.usage_hour_buckets WHERE bucket_key=delta.bucket_key AND requests=0;
 END LOOP;
 -- Keep empty ledger rows as MVCC witnesses against stale snapshot backfills.
 DELETE FROM pg_temp.harbor_usage_stage;DELETE FROM pg_temp.harbor_usage_delta;
END
$usage$;
CREATE FUNCTION harbor.usage_flush_deferred() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
BEGIN PERFORM harbor.flush_usage_rollups();RETURN NULL;END
$usage$;
CREATE FUNCTION harbor.usage_source_guard() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
BEGIN
 IF to_regclass('pg_temp.harbor_usage_flushed') IS NOT NULL THEN
  IF EXISTS(SELECT 1 FROM pg_temp.harbor_usage_flushed) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='小时汇总已完成本事务归并，禁止继续修改请求或价格';END IF;
 END IF;
 RETURN NULL;
END
$usage$;
CREATE FUNCTION harbor.usage_reject_truncate() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
BEGIN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='小时汇总禁止直接 TRUNCATE 请求或价格，请使用受控重建流程';END
$usage$;
CREATE TRIGGER usage_requests_guard BEFORE INSERT OR DELETE OR UPDATE OF id,member_id,key_id,account_id,started,ended,price_version ON harbor.requests FOR EACH STATEMENT EXECUTE FUNCTION harbor.usage_source_guard();
CREATE TRIGGER usage_prices_guard BEFORE INSERT OR DELETE OR UPDATE OF request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms ON harbor.request_prices FOR EACH STATEMENT EXECUTE FUNCTION harbor.usage_source_guard();
CREATE TRIGGER usage_requests_truncate BEFORE TRUNCATE ON harbor.requests FOR EACH STATEMENT EXECUTE FUNCTION harbor.usage_reject_truncate();
CREATE TRIGGER usage_prices_truncate BEFORE TRUNCATE ON harbor.request_prices FOR EACH STATEMENT EXECUTE FUNCTION harbor.usage_reject_truncate();
CREATE TRIGGER usage_requests_stage AFTER INSERT OR DELETE OR UPDATE OF id,member_id,key_id,account_id,started,ended,price_version ON harbor.requests FOR EACH ROW EXECUTE FUNCTION harbor.usage_stage_source();
CREATE TRIGGER usage_prices_stage AFTER INSERT OR DELETE OR UPDATE OF request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms ON harbor.request_prices FOR EACH ROW EXECUTE FUNCTION harbor.usage_stage_source();
CREATE CONSTRAINT TRIGGER usage_requests_flush AFTER INSERT OR UPDATE OR DELETE ON harbor.requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION harbor.usage_flush_deferred();
CREATE CONSTRAINT TRIGGER usage_prices_flush AFTER INSERT OR UPDATE OR DELETE ON harbor.request_prices DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION harbor.usage_flush_deferred();
