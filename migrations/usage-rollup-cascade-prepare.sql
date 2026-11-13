CREATE OR REPLACE FUNCTION harbor.usage_prepare_staging() RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $usage$
DECLARE contract JSONB;principals OID[];principal OID;relation OID;table_name TEXT;role_name TEXT;actual JSONB;created BOOLEAN:=FALSE;required TEXT[];allowed TEXT;row RECORD;core_oid OID;core_owner OID;function_owner OID;marker_owner OID;legacy BOOLEAN:=FALSE;occupied BOOLEAN;missing TEXT[];permission TEXT;state RECORD;current_transaction xid8;
BEGIN
 SELECT proowner INTO function_owner FROM pg_proc WHERE oid='harbor.usage_prepare_staging()'::regprocedure;
 SELECT relowner INTO marker_owner FROM pg_class WHERE oid='harbor.usage_rollup_cascade_contract'::regclass;
 IF marker_owner<>function_owner THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_marker_owner';END IF;
 SELECT c.contract INTO STRICT contract FROM harbor.usage_rollup_cascade_contract c WHERE singleton;
 IF contract->>'version'<>'1' OR (contract->>'databaseOid')::OID<>(SELECT oid FROM pg_database WHERE datname=current_database()) OR (contract->>'installerOid')::OID<>function_owner THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_contract_invalid';END IF;
 FOR table_name IN SELECT unnest(ARRAY['members','keys','sessions','requests','request_prices']) LOOP
  SELECT oid,relowner INTO core_oid,core_owner FROM pg_class WHERE oid=to_regclass('harbor.'||table_name);
  IF core_oid IS NULL OR core_oid<>(contract->'coreTables'->>table_name)::OID OR core_owner<>(contract->>'coreOwnerOid')::OID THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_core_changed';END IF;
 END LOOP;
 principals:=ARRAY(SELECT DISTINCT value FROM unnest(ARRAY[(contract->>'runtimeOid')::OID,(contract->>'coreOwnerOid')::OID,function_owner]) value);
 IF NOT (current_user::regrole::OID=ANY(principals)) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='usage_cascade_caller_invalid';END IF;
 IF to_regclass('pg_temp.harbor_usage_stage') IS NULL AND to_regclass('pg_temp.harbor_usage_flushed') IS NULL AND to_regclass('pg_temp.harbor_usage_delta') IS NULL THEN
  CREATE TEMP TABLE harbor_usage_flushed(singleton BOOLEAN PRIMARY KEY CHECK(singleton),transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),flushed BOOLEAN NOT NULL DEFAULT FALSE) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE harbor_usage_stage(request_id TEXT PRIMARY KEY,request_dirty BOOLEAN NOT NULL DEFAULT FALSE,price_dirty BOOLEAN NOT NULL DEFAULT FALSE,request_fact JSONB,price_fact JSONB) ON COMMIT DELETE ROWS;
  CREATE TEMP TABLE harbor_usage_delta(LIKE harbor.usage_hour_buckets INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DELETE ROWS;
  ALTER TABLE pg_temp.harbor_usage_delta DROP CONSTRAINT usage_hour_counts;
  ALTER TABLE pg_temp.harbor_usage_delta DROP CONSTRAINT usage_hour_empty;
  created:=TRUE;
 END IF;
 FOREACH table_name IN ARRAY ARRAY['harbor_usage_flushed','harbor_usage_stage','harbor_usage_delta'] LOOP
  relation:=to_regclass('pg_temp.'||table_name);
  SELECT c.relowner,c.relkind,c.relpersistence,c.relnamespace,c.relrowsecurity,c.relforcerowsecurity,c.relhasrules INTO row FROM pg_class c WHERE c.oid=relation;
  IF relation IS NULL OR row.relnamespace<>pg_my_temp_schema() OR row.relkind<>'r' OR row.relpersistence<>'t' OR row.relrowsecurity OR row.relforcerowsecurity OR row.relhasrules OR NOT(row.relowner=ANY(principals)) OR EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid=relation AND NOT tgisinternal) OR EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid=relation OR inhparent=relation) OR EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid=relation AND attacl IS NOT NULL) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_temp_identity';END IF;
  SELECT value INTO actual FROM (SELECT jsonb_build_object(
 'columns',(SELECT jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attcollation::text,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum) FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid=relation AND a.attnum>0 AND NOT a.attisdropped),
 'constraints',(SELECT jsonb_agg(value ORDER BY value::text) FROM (SELECT jsonb_build_array(contype,pg_get_constraintdef(oid),condeferrable,condeferred,convalidated) value FROM pg_constraint WHERE conrelid=relation) x),
 'indexes',(SELECT jsonb_agg(value ORDER BY value::text) FROM (SELECT jsonb_build_array(indisunique,indisprimary,indkey::text,indclass::text,indcollation::text,pg_get_expr(indexprs,indrelid),pg_get_expr(indpred,indrelid)) value FROM pg_index WHERE indrelid=relation) x)
) value) shape;
  IF actual IS DISTINCT FROM contract->'tempShapes'->table_name THEN
   IF table_name='harbor_usage_flushed' AND actual=contract->'legacyFlushedShape' THEN legacy:=TRUE;
   ELSE RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_temp_shape';END IF;
  END IF;
  required:=ARRAY['SELECT','INSERT','UPDATE','DELETE'];
  IF EXISTS(SELECT 1 FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.oid=relation AND (a.grantee=0 OR NOT(a.grantee=ANY(principals)) OR a.grantor<>c.relowner OR a.grantee<>c.relowner AND (a.is_grantable OR NOT(a.privilege_type=ANY(required))))) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_temp_acl';END IF;
  IF row.relowner=current_user::regrole::OID THEN
   allowed:=array_to_string(required,',');
   FOREACH principal IN ARRAY principals LOOP
    SELECT array_agg(value ORDER BY value) INTO missing FROM unnest(required) value WHERE NOT has_table_privilege(principal,relation,value);
    IF principal<>row.relowner AND cardinality(missing)>0 THEN
     SELECT rolname INTO STRICT role_name FROM pg_roles WHERE oid=principal;
     EXECUTE format('GRANT %s ON TABLE pg_temp.%I TO %I',array_to_string(missing,','),table_name,role_name);
    END IF;
   END LOOP;
  END IF;
  FOREACH principal IN ARRAY principals LOOP
   IF EXISTS(SELECT 1 FROM unnest(required) value WHERE NOT has_table_privilege(principal,relation,value)) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_temp_acl_missing';END IF;
  END LOOP;
 END LOOP;
 IF legacy THEN
  FOREACH table_name IN ARRAY ARRAY['harbor_usage_flushed','harbor_usage_stage','harbor_usage_delta'] LOOP
   relation:=to_regclass('pg_temp.'||table_name);
   IF (SELECT relowner FROM pg_class WHERE oid=relation)<>current_user::regrole::OID THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_legacy_temp_owner';END IF;
   EXECUTE format('SELECT EXISTS(SELECT 1 FROM pg_temp.%I)',table_name) INTO occupied;
   IF occupied THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_legacy_temp_busy';END IF;
  END LOOP;
  DROP TABLE pg_temp.harbor_usage_flushed,pg_temp.harbor_usage_stage,pg_temp.harbor_usage_delta;
  PERFORM harbor.usage_prepare_staging();RETURN;
 END IF;
 current_transaction:=pg_current_xact_id();
 SELECT transaction_id,flushed INTO state FROM pg_temp.harbor_usage_flushed WHERE singleton;
 IF NOT FOUND OR state.transaction_id<>current_transaction THEN
  IF EXISTS(SELECT 1 FROM pg_temp.harbor_usage_stage) OR EXISTS(SELECT 1 FROM pg_temp.harbor_usage_delta) THEN RAISE EXCEPTION USING ERRCODE='55000',MESSAGE='usage_cascade_previous_stage';END IF;
  DELETE FROM pg_temp.harbor_usage_flushed;
  INSERT INTO pg_temp.harbor_usage_flushed VALUES(TRUE,current_transaction,FALSE);
 END IF;
END
$usage$;
