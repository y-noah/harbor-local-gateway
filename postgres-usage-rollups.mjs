import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {AppError} from './store.mjs';
import {fields,referenceRollupAggregateSql} from './postgres-reference-query.mjs';
const verifiedStores=new WeakSet();

const setting='schema.usage-hour-rollups',migrationUrl=new URL('./migrations/usage-hour-rollups.sql',import.meta.url);
const requestFact="jsonb_build_object('member_id',r.member_id,'key_id',r.key_id,'account_id',r.account_id,'usage_at',r.usage_at,'price_version',r.price_version)";
const priceFact="jsonb_build_object('state',p.state,'revision',p.revision,'input_atoms',p.input_atoms,'output_atoms',p.output_atoms,'cached_atoms',p.cached_atoms,'adjustment_atoms',p.adjustment_atoms,'total_atoms',p.total_atoms)";
const measures=['requests','known','pending','expired','unpriced','unmetered','input','output','cached','adjustment','total'];
const expectedConstraints=[{"relname":"usage_contributions","conname":"usage_contributions_pkey","def":"PRIMARY KEY (request_id)"},{"relname":"usage_hour_buckets","conname":"usage_hour_boundary","def":"CHECK ((mod(hour_at, (3600000)::bigint) = 0))"},{"relname":"usage_hour_buckets","conname":"usage_hour_buckets_pkey","def":"PRIMARY KEY (bucket_key)"},{"relname":"usage_hour_buckets","conname":"usage_hour_buckets_scope_check","def":"CHECK ((scope = ANY (ARRAY['member'::text, 'key'::text, 'account'::text])))"},{"relname":"usage_hour_buckets","conname":"usage_hour_counts","def":"CHECK (((requests >= 0) AND (known >= 0) AND (pending >= 0) AND (expired >= 0) AND (unpriced >= 0) AND (unmetered >= 0) AND (requests = ((((known + pending) + expired) + unpriced) + unmetered))))"},{"relname":"usage_hour_buckets","conname":"usage_hour_empty","def":"CHECK (((requests <> 0) OR ((input = (0)::numeric) AND (output = (0)::numeric) AND (cached = (0)::numeric) AND (adjustment = (0)::numeric) AND (total = (0)::numeric))))"},{"relname":"usage_hour_buckets","conname":"usage_hour_identity","def":"CHECK ((bucket_key = (jsonb_build_array(scope, owner_id, hour_at, revision, projected_revision, price_state))::text))"},{"relname":"usage_rollup_state","conname":"usage_rollup_state_pkey","def":"PRIMARY KEY (singleton)"},{"relname":"usage_rollup_state","conname":"usage_rollup_state_singleton_check","def":"CHECK (singleton)"},{"relname":"usage_rollup_state","conname":"usage_rollup_state_state_check","def":"CHECK ((state = ANY (ARRAY['backfilling'::text, 'ready'::text, 'disabled'::text])))"},{"relname":"usage_rollup_state","conname":"usage_rollup_state_version_check","def":"CHECK ((version = 1))"}];
const expectedColumns=[{"relname":"usage_contributions","attname":"request_id","type":"text","attnotnull":true},{"relname":"usage_contributions","attname":"request_fact","type":"jsonb","attnotnull":false},{"relname":"usage_contributions","attname":"price_fact","type":"jsonb","attnotnull":false},{"relname":"usage_hour_buckets","attname":"bucket_key","type":"text","attnotnull":true},{"relname":"usage_hour_buckets","attname":"scope","type":"text","attnotnull":true},{"relname":"usage_hour_buckets","attname":"owner_id","type":"text","attnotnull":true},{"relname":"usage_hour_buckets","attname":"hour_at","type":"bigint","attnotnull":true},{"relname":"usage_hour_buckets","attname":"revision","type":"text","attnotnull":false},{"relname":"usage_hour_buckets","attname":"projected_revision","type":"text","attnotnull":false},{"relname":"usage_hour_buckets","attname":"price_state","type":"text","attnotnull":false},{"relname":"usage_hour_buckets","attname":"requests","type":"bigint","attnotnull":true},{"relname":"usage_hour_buckets","attname":"known","type":"bigint","attnotnull":true},{"relname":"usage_hour_buckets","attname":"pending","type":"bigint","attnotnull":true},{"relname":"usage_hour_buckets","attname":"expired","type":"bigint","attnotnull":true},{"relname":"usage_hour_buckets","attname":"unpriced","type":"bigint","attnotnull":true},{"relname":"usage_hour_buckets","attname":"unmetered","type":"bigint","attnotnull":true},{"relname":"usage_hour_buckets","attname":"input","type":"numeric","attnotnull":true},{"relname":"usage_hour_buckets","attname":"output","type":"numeric","attnotnull":true},{"relname":"usage_hour_buckets","attname":"cached","type":"numeric","attnotnull":true},{"relname":"usage_hour_buckets","attname":"adjustment","type":"numeric","attnotnull":true},{"relname":"usage_hour_buckets","attname":"total","type":"numeric","attnotnull":true},{"relname":"usage_rollup_state","attname":"singleton","type":"boolean","attnotnull":true},{"relname":"usage_rollup_state","attname":"version","type":"integer","attnotnull":true},{"relname":"usage_rollup_state","attname":"state","type":"text","attnotnull":true},{"relname":"usage_rollup_state","attname":"cursor","type":"text","attnotnull":false},{"relname":"usage_rollup_state","attname":"checked","type":"bigint","attnotnull":false}];
const conflict=()=>new AppError(503,'小时用量汇总结构校验不一致','database_migration_conflict');
async function source(){const sql=await readFile(migrationUrl,'utf8');return {sql,checksum:createHash('sha256').update(sql).digest('hex')};}

// This extension is opt-in and leaves the core migration chain and old readers intact.
const identifier=value=>'"'+value.replaceAll('"','""')+'"';
async function usageRuntimeRole(store,explicit){
 if(explicit!==undefined&&(typeof explicit!=='string'||!/^[a-z_][a-z0-9_]{0,62}$/.test(explicit)))throw new AppError(400,'汇总运行角色无效','usage_rollup_runtime_invalid');
 const activation=await store.get("SELECT to_regclass('harbor_operator.activation') relation");let role=explicit;
 if(activation.relation){
  const trusted=await store.get('SELECT * FROM harbor_operator.activation WHERE singleton'),current=await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()');
  if(!trusted||trusted.database_oid!==current.oid)throw new AppError(409,'汇总运行角色与受信部署记录不一致','usage_rollup_runtime_invalid');
  if(Object.hasOwn(trusted,'runtime')){
   if(explicit!==undefined&&explicit!==trusted.runtime)throw new AppError(409,'汇总运行角色与受信部署记录不一致','usage_rollup_runtime_invalid');role=trusted.runtime;
  }else{
   if(trusted.local_contract!=='local-assurance-v1'||typeof trusted.local_release_id!=='string'||!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(trusted.local_release_id)||trusted.state!=='pending')throw new AppError(409,'汇总运行角色与受信部署记录不一致','usage_rollup_runtime_invalid');
   if(explicit===undefined)throw new AppError(400,'本机保障迁移须显式指定已核实的运行角色','usage_rollup_runtime_required');
  }
 }
 if(typeof role!=='string'||!/^[a-z_][a-z0-9_]{0,62}$/.test(role))throw new AppError(400,'请指定受控运行角色或提供受信部署记录','usage_rollup_runtime_required');
 // TEMP relations are expected runtime staging; ownership of any persistent or foreign-database object remains unsafe.
 const actual=await store.get('SELECT rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1',role);
 if(!actual||Object.values(actual).some(Boolean)||await store.get("SELECT 1 FROM pg_auth_members WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=$1) OR member=(SELECT oid FROM pg_roles WHERE rolname=$1)",role)||await store.get("SELECT 1 FROM pg_shdepend d WHERE refclassid='pg_authid'::regclass AND refobjid=(SELECT oid FROM pg_roles WHERE rolname=$1) AND deptype='o' AND NOT (d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND d.classid='pg_class'::regclass AND EXISTS(SELECT 1 FROM pg_class c WHERE c.oid=d.objid AND c.relpersistence='t')) LIMIT 1",role))throw new AppError(409,'汇总运行角色超出最小权限边界','usage_rollup_runtime_invalid');
 const privilege=await store.get("SELECT has_schema_privilege($1,'harbor','CREATE') OR has_schema_privilege($1,'public','CREATE') unsafe",role);if(privilege.unsafe)throw new AppError(409,'汇总运行角色不得拥有schema创建权限','usage_rollup_runtime_invalid');return role;
}
async function grantUsageRuntime(store,role,names){
 const database=(await store.get('SELECT current_database() name')).name,quoted=identifier(role);
 await store.query('GRANT TEMP ON DATABASE '+identifier(database)+' TO '+quoted);await store.query('GRANT USAGE ON SCHEMA harbor TO '+quoted);
 await store.query('GRANT SELECT ON harbor.usage_rollup_state TO '+quoted);await store.query('GRANT SELECT,INSERT,UPDATE ON harbor.usage_contributions TO '+quoted);await store.query('GRANT SELECT,INSERT,UPDATE,DELETE ON harbor.usage_hour_buckets TO '+quoted);
 const functions=await store.all("SELECT p.oid::regprocedure::text signature FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='harbor' AND p.proname=ANY($1::text[])" ,names);
 for(const fn of functions){await store.query('REVOKE ALL ON FUNCTION '+fn.signature+' FROM PUBLIC');await store.query('GRANT EXECUTE ON FUNCTION '+fn.signature+' TO '+quoted);}
 const permission=await store.get("SELECT has_database_privilege($1,current_database(),'TEMP') temporary,has_table_privilege($1,'harbor.usage_rollup_state','SELECT') state,has_table_privilege($1,'harbor.usage_contributions','SELECT') AND has_table_privilege($1,'harbor.usage_contributions','INSERT') AND has_table_privilege($1,'harbor.usage_contributions','UPDATE') contributions,has_table_privilege($1,'harbor.usage_hour_buckets','SELECT') AND has_table_privilege($1,'harbor.usage_hour_buckets','INSERT') AND has_table_privilege($1,'harbor.usage_hour_buckets','UPDATE') AND has_table_privilege($1,'harbor.usage_hour_buckets','DELETE') buckets",role);
 if(Object.values(permission).some(value=>!value))throw new AppError(409,'汇总运行角色权限未生效','usage_rollup_runtime_invalid');
}
export async function verifyUsageRollupAvailability(store){
 const saved=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',setting);
 if(!saved){verifiedStores.delete(store);return false;}
 const bundle=await source();let value;try{value=JSON.parse(saved.value);}catch{throw conflict();}
 if(value.version!==1||value.checksum!==bundle.checksum)throw conflict();
 await verifyUsageRollups(store);verifiedStores.add(store);return true;
}
export async function migrateUsageRollups(store,{apply=false,runtimeRole}={}){
 if(typeof apply!=='boolean')throw new TypeError('usage_rollup_apply_invalid');
 if(!apply){await verifyUsageRollupAvailability(store);return false;}
 const changed=await store.transaction(async()=>{
  await store.query('SELECT pg_advisory_xact_lock($1)',[createHash('sha256').update('harbor-postgres-migrations').digest().readInt32BE()]);
  const saved=await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',setting),bundle=await source();
  if(saved){if(runtimeRole!==undefined)await usageRuntimeRole(store,runtimeRole);await verifyUsageRollupAvailability(store);return false;}
  const role=await usageRuntimeRole(store,runtimeRole),triggerAt=bundle.sql.indexOf('CREATE TRIGGER usage_requests_guard');
  await store.query(bundle.sql.slice(0,triggerAt));
  await grantUsageRuntime(store,role,[...bundle.sql.matchAll(/CREATE FUNCTION harbor\.(\w+)\(/g)].map(match=>match[1]));
  await store.query(bundle.sql.slice(triggerAt));await verifyUsageRollups(store,bundle.sql);
  await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)',setting,JSON.stringify({version:1,checksum:bundle.checksum,runtimeRole:role}));return true;
 });
 verifiedStores.add(store);return changed;
}
async function verifyUsageRollupStructure(store,sql){
 sql??=(await source()).sql;
 // Compare the executable function bodies with the bundled source, not a mutable ready flag.
 const expected=[...sql.matchAll(/CREATE FUNCTION harbor\.(\w+)\([^]*?AS \$usage\$\n([^]*?)\n\$usage\$;/g)];
 const functions=await store.all("SELECT p.oid::text oid,p.proname,p.prosrc,p.prosecdef,p.proisstrict,p.provolatile,p.proparallel,p.proconfig,l.lanname FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang WHERE n.nspname='harbor' AND p.proname=ANY($1::text[])",expected.map(m=>m[1]));
 if(functions.length!==expected.length)throw conflict();
 for(const [,name,body] of expected){const found=functions.find(f=>f.proname===name);if(!found||found.prosrc.trim()!==body.trim()||found.prosecdef||found.proisstrict||found.provolatile!=='v'||found.proparallel!=='u'||found.lanname!=='plpgsql'||JSON.stringify(found.proconfig)!==JSON.stringify(['search_path=pg_catalog, harbor']))throw conflict();}
 const triggers=await store.all("SELECT t.tgname,t.tgtype,t.tgenabled,t.tgdeferrable,t.tginitdeferred,c.relname,p.proname,t.tgfoid::text function_oid,t.tgattr::text attrs,t.tgqual IS NOT NULL has_when,t.tgnargs FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_proc p ON p.oid=t.tgfoid WHERE n.nspname='harbor' AND t.tgname=ANY($1::text[])",['usage_requests_stage','usage_prices_stage','usage_requests_flush','usage_prices_flush','usage_requests_guard','usage_prices_guard','usage_requests_truncate','usage_prices_truncate']);
 if(triggers.length!==8)throw conflict();
 for(const t of triggers){
  const kind=t.tgname.split('_').at(-1),request=t.tgname.startsWith('usage_requests_'),deferred=kind==='flush';
  const expectedFunction={stage:'usage_stage_source',flush:'usage_flush_deferred',guard:'usage_source_guard',truncate:'usage_reject_truncate'}[kind],expectedType={stage:29,flush:29,guard:30,truncate:34}[kind];
  if(t.relname!==(request?'requests':'request_prices')||t.tgtype!==expectedType||t.has_when||t.tgnargs!==0||t.tgenabled!=='O'||t.tgdeferrable!==deferred||t.tginitdeferred!==deferred||t.function_oid!==functions.find(f=>f.proname===expectedFunction)?.oid)throw conflict();
  const expectedAttrs=['flush','truncate'].includes(kind)?'':request?'1 2 3 4 5 6 18':'1 2 5 6 7 8 9 10';if(t.attrs!==expectedAttrs)throw conflict();
 }

 const constraints=await store.all("SELECT c.relname,co.conname,pg_get_constraintdef(co.oid) def FROM pg_constraint co JOIN pg_class c ON c.oid=co.conrelid WHERE c.oid=ANY(ARRAY['harbor.usage_rollup_state'::regclass,'harbor.usage_contributions'::regclass,'harbor.usage_hour_buckets'::regclass]) AND co.contype<>'n' AND co.convalidated ORDER BY c.relname,co.conname");
 if(JSON.stringify(constraints)!==JSON.stringify(expectedConstraints))throw conflict();
 const columns=await store.all("SELECT c.relname,a.attname,format_type(a.atttypid,a.atttypmod) type,a.attnotnull FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.oid=ANY(ARRAY['harbor.usage_rollup_state'::regclass,'harbor.usage_contributions'::regclass,'harbor.usage_hour_buckets'::regclass]) AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum");
 if(JSON.stringify(columns)!==JSON.stringify(expectedColumns))throw conflict();
 const tables=await store.all("SELECT c.relname,c.relkind,c.relpersistence FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND c.relname=ANY($1::text[])",['usage_rollup_state','usage_contributions','usage_hour_buckets']);
 if(tables.length!==3||tables.some(t=>t.relkind!=='r'||t.relpersistence!=='p'))throw conflict();
 return true;
}
export async function usageRollupsReady(store){
 // Startup attests the schema once. A GET only reads this transaction's state;
 // it never takes the migration lock or scans PostgreSQL catalogs.
 if(!verifiedStores.has(store))return false;
 const row=await store.get('SELECT state FROM harbor.usage_rollup_state WHERE singleton');return row?.state==='ready';
}

function bounded(value,min,max){if(!Number.isInteger(value)||value<min||value>max)throw new TypeError('usage_rollup_batch_invalid');return value;}
export async function backfillUsageRollups(store,{batchSize=100,statementTimeoutMs=5000,lockTimeoutMs=250}={}){
 bounded(batchSize,1,1000);bounded(statementTimeoutMs,100,60000);bounded(lockTimeoutMs,1,5000);
 return store.transaction(async()=>{
  await store.query("SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout',$2,true)",[String(statementTimeoutMs),String(lockTimeoutMs)]);
  await verifyUsageRollups(store);const state=await store.get('SELECT * FROM harbor.usage_rollup_state WHERE singleton FOR UPDATE');if(!state||state.state!=='backfilling')return {scanned:0,complete:state?.state==='ready'};
  const rows=await store.all(`SELECT r.id FROM harbor.requests r WHERE ($1::text IS NULL OR r.id COLLATE "C">$1 COLLATE "C") ORDER BY r.id COLLATE "C" LIMIT $2`,state.cursor,batchSize);
  await store.query('SELECT harbor.usage_prepare_staging()');
  if(rows.length)await store.query(`INSERT INTO pg_temp.harbor_usage_stage(request_id,request_dirty,price_dirty,request_fact,price_fact) SELECT r.id,true,true,${requestFact},CASE WHEN p.request_id IS NULL THEN NULL ELSE ${priceFact} END FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id WHERE r.id=ANY($1::text[]) ON CONFLICT(request_id) DO UPDATE SET request_dirty=true,price_dirty=true,request_fact=excluded.request_fact,price_fact=excluded.price_fact`,[rows.map(r=>r.id)]);
  await store.query('SELECT harbor.flush_usage_rollups()');
  if(rows.length)await store.run('UPDATE harbor.usage_rollup_state SET cursor=$1 WHERE singleton',rows.at(-1).id);
  return {scanned:rows.length,complete:false,exhausted:rows.length<batchSize};
 },{level:'REPEATABLE READ'});
}
const expectedBuckets=`WITH facts AS (SELECT d.scope,d.owner_id,floor(r.usage_at::numeric/3600000)::bigint*3600000 hour_at,r.price_version revision,p.revision projected_revision,p.state price_state,p.input_atoms,p.output_atoms,p.cached_atoms,p.adjustment_atoms,p.total_atoms FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id CROSS JOIN LATERAL (VALUES('member',r.member_id),('key',r.key_id),('account',r.account_id)) d(scope,owner_id) WHERE d.owner_id IS NOT NULL), expected AS (SELECT jsonb_build_array(scope,owner_id,hour_at,revision,projected_revision,price_state)::text bucket_key,scope,owner_id,hour_at,revision,projected_revision,price_state,COUNT(*) requests,COUNT(total_atoms) known,COUNT(*) FILTER(WHERE total_atoms IS NULL AND price_state='pending') pending,COUNT(*) FILTER(WHERE total_atoms IS NULL AND price_state='expired') expired,COUNT(*) FILTER(WHERE total_atoms IS NULL AND (price_state='unpriced' OR price_state IS NULL)) unpriced,COUNT(*) FILTER(WHERE total_atoms IS NULL AND price_state NOT IN ('pending','expired','unpriced')) unmetered,${['input','output','cached','adjustment','total'].map(f=>`COALESCE(SUM(${f}_atoms),0) ${f}`).join(',')} FROM facts GROUP BY scope,owner_id,hour_at,revision,projected_revision,price_state)`;
export async function reconcileUsageRollups(store,{publishReady=false,statementTimeoutMs=30000}={}){
 if(typeof publishReady!=='boolean')throw new TypeError('usage_rollup_ready_invalid');bounded(statementTimeoutMs,100,600000);
 return store.transaction(async()=>{
  await store.query("SELECT set_config('statement_timeout',$1,true)",[String(statementTimeoutMs)]);await verifyUsageRollups(store);
  const state=await store.get('SELECT state FROM harbor.usage_rollup_state WHERE singleton'+(publishReady?' FOR UPDATE':''));if(!state)throw conflict();
  const facts=await store.get(`WITH actual AS (SELECT r.id request_id,${requestFact} request_fact,CASE WHEN p.request_id IS NULL THEN NULL ELSE ${priceFact} END price_fact FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id) SELECT COUNT(*) mismatches FROM actual a FULL JOIN harbor.usage_contributions c USING(request_id) WHERE (a.request_id IS NULL AND (c.request_fact IS NOT NULL OR c.price_fact IS NOT NULL)) OR (a.request_id IS NOT NULL AND (c.request_id IS NULL OR a.request_fact IS DISTINCT FROM c.request_fact OR a.price_fact IS DISTINCT FROM c.price_fact))`);
  const buckets=await store.get(`${expectedBuckets}, difference AS ((SELECT to_jsonb(e) row FROM expected e EXCEPT SELECT to_jsonb(b) FROM harbor.usage_hour_buckets b) UNION ALL (SELECT to_jsonb(b) FROM harbor.usage_hour_buckets b EXCEPT SELECT to_jsonb(e) FROM expected e)) SELECT COUNT(*) mismatches FROM difference`);
  const valid=facts.mismatches===0&&buckets.mismatches===0;
  if(publishReady&&valid){if(state.state==='disabled')throw new AppError(409,'小时用量汇总已停用','usage_rollup_disabled');await store.run("UPDATE harbor.usage_rollup_state SET state='ready',checked=$1 WHERE singleton",Date.now());}
  return {valid,factMismatches:facts.mismatches,bucketMismatches:buckets.mismatches,ready:valid&&(publishReady||state.state==='ready')};
 },{level:'REPEATABLE READ',readOnly:!publishReady});
}
export async function disableUsageRollups(store){await verifyUsageRollups(store);await store.run("UPDATE harbor.usage_rollup_state SET state='disabled' WHERE singleton");}
export {measures as usageRollupMeasures};

const rawMeasures=[`COUNT(*) requests`,`COUNT(p.total_atoms) known`,...['pending','expired'].map(state=>`COUNT(*) FILTER(WHERE p.total_atoms IS NULL AND p.state='${state}') ${state}`),`COUNT(*) FILTER(WHERE p.total_atoms IS NULL AND (p.state='unpriced' OR p.state IS NULL)) unpriced`,`COUNT(*) FILTER(WHERE p.total_atoms IS NULL AND p.state NOT IN ('pending','expired','unpriced')) unmetered`,...fields.map(field=>`COALESCE(SUM(p.${field}_atoms),0) ${field}`)].join(',');
// Arguments: owner IDs, primary window [from,to), recent window [from,to).
// Boundary ranges never overlap, including sub-hour and negative-epoch windows.
export function usageRollupMoneySql(scope){
 if(!['member','key','account'].includes(scope))throw new TypeError('usage_rollup_scope_invalid');
 const valid='s.revision=s.projected_revision AND v.id IS NOT NULL';
 return `WITH windows AS (SELECT false recent,$2::bigint from_at,$3::bigint to_at UNION ALL SELECT true,$4::bigint,$5::bigint),
 aligned AS (SELECT *,ceil(from_at::numeric/3600000)::bigint*3600000 first_hour,floor(to_at::numeric/3600000)::bigint*3600000 last_hour FROM windows WHERE from_at<to_at),
 edges AS (SELECT w.recent,e.from_at,e.to_at FROM aligned w CROSS JOIN LATERAL (VALUES(w.from_at,LEAST(w.to_at,w.first_hour)),(GREATEST(w.from_at,w.first_hour,w.last_hour),w.to_at)) e(from_at,to_at) WHERE e.from_at<e.to_at),
 raw AS (
 SELECT b.owner_id,b.revision,b.projected_revision,w.recent,${measures.map(field=>'b.'+field).join(',')} FROM aligned w JOIN harbor.usage_hour_buckets b ON b.scope='${scope}' AND b.owner_id=ANY($1::text[]) AND b.hour_at>=w.first_hour AND b.hour_at<w.last_hour
 UNION ALL
 SELECT r.${scope}_id owner_id,r.price_version revision,p.revision projected_revision,w.recent,${rawMeasures} FROM edges w JOIN harbor.requests r ON r.usage_at>=w.from_at AND r.usage_at<w.to_at LEFT JOIN harbor.request_prices p ON p.request_id=r.id WHERE r.${scope}_id=ANY($1::text[]) GROUP BY r.${scope}_id,r.price_version,p.revision,w.recent
 ), partial AS (
 SELECT s.owner_id,s.revision,s.recent,s.requests,CASE WHEN ${valid} THEN s.known ELSE 0 END known,
 ${['pending','expired','unmetered'].map(field=>`CASE WHEN ${valid} THEN s.${field} ELSE 0 END ${field}`).join(',')},CASE WHEN ${valid} THEN s.unpriced ELSE s.requests END unpriced,
 ${fields.map(field=>`CASE WHEN ${valid} THEN s.${field} END ${field}`).join(',')} FROM raw s LEFT JOIN harbor.pricing_versions v ON v.id=s.revision
 ) SELECT owner_id,${referenceRollupAggregateSql({standardOnly:true,condition:'NOT recent'})},${referenceRollupAggregateSql({standardOnly:true,condition:'recent',prefix:'recent_'})} FROM partial GROUP BY owner_id`;
}

// Fixed usage-extension maintenance only; no business endpoint can select its role bindings.
const base=await readFile(migrationUrl,'utf8'),shapeSql=await readFile(new URL('./migrations/usage-rollup-cascade-temp-shape.sql',import.meta.url),'utf8'),prepare=await readFile(new URL('./migrations/usage-rollup-cascade-prepare.sql',import.meta.url),'utf8');
const names=['usage_prepare_staging','usage_source_guard','usage_stage_source','flush_usage_rollups'],nested=['usage_prepare_staging','usage_add_delta','flush_usage_rollups'],core=['members','keys','sessions','requests','request_prices'],temporary=['harbor_usage_flushed','harbor_usage_stage','harbor_usage_delta'];const tables={usage_rollup_state:['SELECT'],usage_contributions:['SELECT','INSERT','UPDATE'],usage_hour_buckets:['SELECT','INSERT','UPDATE','DELETE']};
const need=(ok,code)=>{if(!ok)throw Object.assign(Error(code),{code});},quote=name=>'"'+name.replaceAll('"','""')+'"',ordered=x=>Array.isArray(x)?x.map(ordered):x&&typeof x==='object'?Object.fromEntries(Object.keys(x).sort().map(k=>[k,ordered(x[k])])):x,hash=x=>createHash('sha256').update(typeof x==='string'?x:JSON.stringify(ordered(x))).digest('hex');
const definition=name=>[...base.matchAll(/CREATE FUNCTION harbor[.](\w+)[^]*?\$usage\$;/g)].find(m=>m[1]===name)[0];
const patches={usage_prepare_staging:prepare,usage_source_guard:definition('usage_source_guard').replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION').replace("IF to_regclass('pg_temp.harbor_usage_flushed') IS NOT NULL THEN","IF to_regclass('pg_temp.harbor_usage_flushed') IS NOT NULL THEN\n  PERFORM harbor.usage_prepare_staging();").replace('EXISTS(SELECT 1 FROM pg_temp.harbor_usage_flushed)','EXISTS(SELECT 1 FROM pg_temp.harbor_usage_flushed WHERE flushed)'),usage_stage_source:definition('usage_stage_source').replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION').replace('EXISTS(SELECT 1 FROM pg_temp.harbor_usage_flushed)','EXISTS(SELECT 1 FROM pg_temp.harbor_usage_flushed WHERE flushed)'),flush_usage_rollups:definition('flush_usage_rollups').replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION').replace("IF to_regclass('pg_temp.harbor_usage_stage') IS NULL THEN RETURN;END IF;","IF to_regclass('pg_temp.harbor_usage_stage') IS NULL THEN RETURN;END IF;\n PERFORM harbor.usage_prepare_staging();").replace('INSERT INTO pg_temp.harbor_usage_flushed VALUES(TRUE) ON CONFLICT DO NOTHING;','INSERT INTO pg_temp.harbor_usage_flushed(singleton,transaction_id,flushed) VALUES(TRUE,pg_current_xact_id(),TRUE) ON CONFLICT(singleton) DO UPDATE SET transaction_id=EXCLUDED.transaction_id,flushed=TRUE;')};
const patchedSource=names.reduce((sql,name)=>sql.replace(definition(name),patches[name].replace('CREATE OR REPLACE FUNCTION','CREATE FUNCTION')),base),patchChecksum=hash(patchedSource);
const stableBinding=b=>Object.fromEntries(['databaseOid','runtimeOid','runtimeSessionRoleOid','coreOwnerOid','coreTables','cascadeForeignKeyOid'].map(key=>[key,b[key]]));
async function identity(store,binding,{checkDeployment=true}={}){
 need(binding&&/^\d+$/.test(binding.databaseOid)&&/^\d+$/.test(binding.runtimeOid)&&/^\d+$/.test(binding.coreOwnerOid)&&/^\d+$/.test(binding.runtimeSessionRoleOid),'cascade_binding_required');
 if(checkDeployment)need(hash(JSON.stringify(await store.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version')))===binding.schemaDigest,'cascade_core_schema_changed');
 const actual=await store.get("SELECT oid::text oid FROM pg_database WHERE datname=current_database()");need(actual.oid===binding.databaseOid,'cascade_database_changed');
 let activation;if(checkDeployment){activation=await store.get('SELECT to_jsonb(a) value FROM harbor_operator.activation a WHERE singleton');need(hash(activation.value)===binding.activationDigest&&activation.value.database_oid===binding.databaseOid,'cascade_activation_changed');}
 const role=await store.get('SELECT oid::text oid,rolname,rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls,rolcanlogin FROM pg_roles WHERE oid=$1::oid',binding.runtimeOid);need(role&&!['rolsuper','rolcreatedb','rolcreaterole','rolinherit','rolreplication','rolbypassrls'].some(k=>role[k]),'cascade_runtime_invalid');if(activation&&Object.hasOwn(activation.value,'runtime'))need(role.rolname===activation.value.runtime,'cascade_runtime_changed');
 need(!await store.get("SELECT 1 FROM pg_shdepend d WHERE refclassid='pg_authid'::regclass AND refobjid=$1::oid AND deptype='o' AND NOT (d.dbid=(SELECT oid FROM pg_database WHERE datname=current_database()) AND d.classid='pg_class'::regclass AND EXISTS(SELECT 1 FROM pg_class c WHERE c.oid=d.objid AND c.relpersistence='t')) LIMIT 1",binding.runtimeOid),'cascade_runtime_ownership');
 need((await store.get("SELECT pg_has_role($1::oid,$2::oid,'SET') ok",binding.runtimeSessionRoleOid,binding.runtimeOid)).ok,'cascade_runtime_session_role_invalid');
 const owner=await store.get('SELECT oid::text oid,rolname,rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls,rolcanlogin FROM pg_roles WHERE oid=$1::oid',binding.coreOwnerOid);need(owner&&!Object.entries(owner).some(([k,v])=>k.startsWith('rol')&&k!=='rolname'&&v),'cascade_core_owner_invalid');need(!await store.get('SELECT 1 FROM pg_auth_members WHERE member=ANY($1::oid[]) LIMIT 1',[binding.coreOwnerOid,binding.runtimeOid]),'cascade_role_membership');
 const rows=await store.all("SELECT c.relname,c.oid::text oid,c.relowner::text owner,c.relkind,c.relpersistence,c.relrowsecurity FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND c.relname=ANY($1::text[]) ORDER BY c.relname",core);need(rows.length===5&&rows.every(r=>r.owner===binding.coreOwnerOid&&r.oid===binding.coreTables[r.relname]&&r.relkind==='r'&&r.relpersistence==='p'&&!r.relrowsecurity),'cascade_core_identity');
 const fk=await store.get("SELECT f.oid::text oid FROM pg_constraint f WHERE f.contype='f' AND f.conrelid='harbor.request_prices'::regclass AND f.confrelid='harbor.requests'::regclass AND f.confdeltype='c' AND f.convalidated AND f.conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=f.conrelid AND attname='request_id')]::smallint[] AND f.confkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=f.confrelid AND attname='id')]::smallint[]");need(fk?.oid===binding.cascadeForeignKeyOid,'cascade_foreign_key_changed');
 const current=await store.get('SELECT current_user::regrole::oid::text oid');return {runtime:role,owner,currentOid:current.oid};
}
async function objects(store){
 const tracked=[...new Set([...nested,...names])];const f=await store.all("SELECT p.oid::text oid,p.proname name,p.proowner::text owner,p.oid::regprocedure::text reference FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='harbor' AND p.proname=ANY($1::text[]) ORDER BY p.proname",tracked);need(f.length===5,'cascade_function_identity');
 const t=await store.all("SELECT c.oid::text oid,c.relname name,c.relowner::text owner FROM pg_class c WHERE c.oid=ANY(ARRAY['harbor.usage_rollup_state'::regclass,'harbor.usage_contributions'::regclass,'harbor.usage_hour_buckets'::regclass]) ORDER BY c.relname");return [...f.map(x=>({...x,kind:'function',privileges:nested.includes(x.name)?['EXECUTE']:[]})),...t.map(x=>({...x,kind:'table',reference:'harbor.'+quote(x.name),privileges:tables[x.name]}))];
}
async function aclDigest(store,object){
 const row=await store.get(object.kind==='function'?"SELECT COALESCE(jsonb_agg(jsonb_build_array(a.grantor::text,a.grantee::text,a.privilege_type,a.is_grantable) ORDER BY a.grantor,a.grantee,a.privilege_type),'[]'::jsonb) value FROM pg_proc c CROSS JOIN LATERAL aclexplode(coalesce(c.proacl,acldefault('f',c.proowner))) a WHERE c.oid=$1::oid":"SELECT COALESCE(jsonb_agg(jsonb_build_array(a.grantor::text,a.grantee::text,a.privilege_type,a.is_grantable) ORDER BY a.grantor,a.grantee,a.privilege_type),'[]'::jsonb) value FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.oid=$1::oid",object.oid);return hash(row.value);
}
async function inspectUsageCascade(store,{binding,requireSafe=false}={}){
 const marker=await store.get("SELECT to_regclass('harbor.usage_rollup_cascade_contract')::oid::text oid");if(!marker.oid){await verifyUsageRollupStructure(store);if(requireSafe)need(false,'usage_rollup_cascade_repair_required');return {installed:false,safe:false,legacyReadable:true};}
 const rows=await objects(store),owners=new Set(rows.map(x=>x.owner));need(owners.size===1,'cascade_installer_owner_changed');const installer=[...owners][0];
 const rel=await store.get("SELECT relowner::text owner,relkind,relpersistence,relrowsecurity,relforcerowsecurity FROM pg_class WHERE oid=$1::oid",marker.oid);need(rel.owner===installer&&rel.relkind==='r'&&rel.relpersistence==='p'&&!rel.relrowsecurity&&!rel.relforcerowsecurity,'cascade_marker_owner');
 const markerShape=await store.all("SELECT attname,format_type(atttypid,atttypmod) type,attnotnull FROM pg_attribute WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",marker.oid);need(JSON.stringify(markerShape)===JSON.stringify([{attname:'singleton',type:'boolean',attnotnull:true},{attname:'contract',type:'jsonb',attnotnull:true}]),'cascade_marker_shape');
 const markerConstraints=await store.all("SELECT pg_get_constraintdef(oid) definition FROM pg_constraint WHERE conrelid=$1::oid AND contype<>'n' ORDER BY contype",marker.oid);need(markerConstraints.length===2&&markerConstraints.some(x=>x.definition==='CHECK (singleton)')&&markerConstraints.some(x=>x.definition==='PRIMARY KEY (singleton)'),'cascade_marker_shape');
 need(!await store.get('SELECT 1 FROM pg_trigger WHERE tgrelid=$1::oid AND NOT tgisinternal',marker.oid)&&!await store.get('SELECT 1 FROM pg_attribute WHERE attrelid=$1::oid AND attacl IS NOT NULL',marker.oid),'cascade_marker_trigger');
 const saved=await store.get('SELECT contract FROM harbor.usage_rollup_cascade_contract WHERE singleton');if(!binding)binding=saved?.contract?.rollbackState?.binding;await identity(store,binding,{checkDeployment:false});
 const acl=await store.all("SELECT a.grantee::text grantee,a.privilege_type,a.is_grantable FROM pg_class c CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault('r',c.relowner))) a WHERE c.oid=$1::oid",marker.oid);need(acl.every(a=>a.grantee===installer||[binding.runtimeOid,binding.coreOwnerOid].includes(a.grantee)&&a.privilege_type==='SELECT'&&!a.is_grantable),'cascade_marker_acl');
 for(const principal of [binding.runtimeOid,binding.coreOwnerOid])need((await store.get("SELECT has_table_privilege($1::oid,$2::oid,'SELECT') ok",principal,marker.oid)).ok,'cascade_marker_select_missing');
 need(saved&&saved.contract.version===1&&saved.contract.installerOid===installer&&saved.contract.cascadeForeignKeyOid===binding.cascadeForeignKeyOid&&saved.contract.patchChecksum===patchChecksum&&saved.contract.databaseOid===binding.databaseOid&&saved.contract.runtimeOid===binding.runtimeOid&&saved.contract.coreOwnerOid===binding.coreOwnerOid&&Object.keys(saved.contract.coreTables).length===5&&core.every(name=>saved.contract.coreTables[name]===binding.coreTables[name]),'cascade_marker_binding');
 await verifyUsageRollupStructure(store,patchedSource);
 for(const object of rows)for(const privilege of object.privileges){const allowed=await store.get(object.kind==='table'?'SELECT has_table_privilege($1::oid,$2::oid,$3) ok':'SELECT has_function_privilege($1::oid,$2::oid,$3) ok',binding.coreOwnerOid,object.oid,privilege);need(allowed.ok,'cascade_required_acl_missing');}
 return {installed:true,safe:true,contract:saved.contract,markerOid:marker.oid,installerOid:installer};
}
async function assertUsageCascadeConnectionsDrained(store,{binding}={}){
 need(binding&&/^\d+$/.test(binding.runtimeSessionRoleOid),'cascade_runtime_session_role_required');
 await store.query('SELECT pg_stat_clear_snapshot()');
 const row=await store.get("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=current_database() AND usesysid=$1::oid AND pid<>pg_backend_pid()",binding.runtimeSessionRoleOid);
 need(row.n===0,'cascade_runtime_connections_not_drained');return {runtimeSessionsRemaining:0,readOnly:true};
}
async function repairUsageCascade(store,{binding,repairId,beforeCommit}={}){
 need(typeof repairId==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(repairId),'cascade_repair_id_required');
 return store.transaction(async()=>{
  await store.query("SET LOCAL lock_timeout='250ms'");await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-usage-cascade-repair',0))");
  await identity(store,binding);const prior=await inspectUsageCascade(store,{binding});if(prior.installed){need(prior.contract.repairId===repairId,'cascade_repair_id_changed');return {changed:false,safe:true,receipt:await recoverUsageCascadeReceipt(store,{binding,repairId})};}
  const id=await identity(store,binding),rows=await objects(store);need(rows.every(x=>x.owner===id.currentOid),'cascade_installer_required');
  need((await store.get('SELECT current_user::regrole::oid::text oid')).oid!==binding.runtimeOid,'cascade_installer_required');
  await assertUsageCascadeConnectionsDrained(store,{binding});
  const receipt={version:1,repairId,binding,patchChecksum,before:{},after:{},added:[]};
  for(const object of rows){receipt.before[object.kind+':'+object.oid]=await aclDigest(store,object);for(const privilege of object.privileges){const exists=await store.get(object.kind==='table'?'SELECT has_table_privilege($1::oid,$2::oid,$3) ok':'SELECT has_function_privilege($1::oid,$2::oid,$3) ok',binding.coreOwnerOid,object.oid,privilege);if(!exists.ok){await store.query('GRANT '+privilege+' ON '+(object.kind==='table'?'TABLE ':'FUNCTION ')+object.reference+' TO '+quote(id.owner.rolname));receipt.added.push({kind:object.kind,oid:object.oid,privilege});}}receipt.after[object.kind+':'+object.oid]=await aclDigest(store,object);}
  need((await store.get('SELECT has_database_privilege($1::oid,current_database(),\'TEMP\') ok',binding.coreOwnerOid)).ok,'cascade_core_temp_missing');
  await store.query('CREATE TABLE harbor.usage_rollup_cascade_contract(singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),contract JSONB NOT NULL)');await store.query('REVOKE ALL ON harbor.usage_rollup_cascade_contract FROM PUBLIC');await store.query('GRANT SELECT ON harbor.usage_rollup_cascade_contract TO '+quote(id.owner.rolname)+','+quote(id.runtime.rolname));
  need(!await store.get("SELECT 1 WHERE to_regclass('pg_temp.harbor_usage_flushed') IS NOT NULL OR to_regclass('pg_temp.harbor_usage_stage') IS NOT NULL OR to_regclass('pg_temp.harbor_usage_delta') IS NOT NULL"),'cascade_installer_temp_in_use');
  await store.query('SELECT harbor.usage_prepare_staging()');const legacyFlushedShape=(await store.get(shapeSql,(await store.get("SELECT 'pg_temp.harbor_usage_flushed'::regclass::oid oid")).oid)).value;await store.query('ALTER TABLE pg_temp.harbor_usage_flushed ADD COLUMN transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id(),ADD COLUMN flushed BOOLEAN NOT NULL DEFAULT FALSE');const tempShapes={};for(const name of temporary)tempShapes[name]=(await store.get(shapeSql,(await store.get('SELECT to_regclass($1)::oid oid','pg_temp.'+name)).oid)).value;for(const name of temporary)await store.query('DROP TABLE pg_temp.'+name);
  receipt.markerOid=(await store.get("SELECT 'harbor.usage_rollup_cascade_contract'::regclass::oid::text oid")).oid;receipt.markerAclDigest=await aclDigest(store,{kind:'table',oid:receipt.markerOid});const contract={rollbackState:JSON.parse(JSON.stringify(receipt)),version:1,repairId:receipt.repairId,databaseOid:binding.databaseOid,runtimeOid:binding.runtimeOid,coreOwnerOid:binding.coreOwnerOid,installerOid:id.currentOid,coreTables:binding.coreTables,cascadeForeignKeyOid:binding.cascadeForeignKeyOid,tempShapes,legacyFlushedShape,patchChecksum};await store.run('INSERT INTO harbor.usage_rollup_cascade_contract(contract) VALUES($1::jsonb)',JSON.stringify(contract));for(const name of names)await store.query(patches[name]);
  receipt.contractDigest=hash((await store.get('SELECT contract FROM harbor.usage_rollup_cascade_contract WHERE singleton')).contract);receipt.markerOid=(await store.get("SELECT 'harbor.usage_rollup_cascade_contract'::regclass::oid::text oid")).oid;receipt.markerAclDigest=await aclDigest(store,{kind:'table',oid:receipt.markerOid});await inspectUsageCascade(store,{binding,requireSafe:true});await beforeCommit?.();return {changed:true,safe:true,receipt};
 });
}
async function recoverUsageCascadeReceipt(store,{binding,repairId}={}){
 const status=await inspectUsageCascade(store,{binding,requireSafe:true});need(status.contract.repairId===repairId&&status.contract.rollbackState?.repairId===repairId,'cascade_receipt_id_changed');
 const receipt={...status.contract.rollbackState,contractDigest:hash(status.contract)};need(receipt.patchChecksum===patchChecksum&&receipt.markerOid===status.markerOid&&hash(stableBinding(receipt.binding))===hash(stableBinding(binding)),'cascade_receipt_binding_changed');return receipt;
}
async function rollbackUsageCascade(store,{receipt,activeBinding=receipt?.binding}={}){
 need(receipt?.version===1&&receipt.patchChecksum===patchChecksum,'cascade_rollback_receipt_invalid');
 return store.transaction(async()=>{
 await store.query("SET LOCAL lock_timeout='250ms'");await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-usage-cascade-repair',0))");
 need(hash(stableBinding(activeBinding))===hash(stableBinding(receipt.binding)),'cascade_rollback_active_binding_changed');const id=await identity(store,activeBinding);
 if(!(await store.get("SELECT to_regclass('harbor.usage_rollup_cascade_contract')::oid oid")).oid){await verifyUsageRollupStructure(store);for(const object of await objects(store))need(await aclDigest(store,object)===receipt.before[object.kind+':'+object.oid],'cascade_rollback_recovery_changed');return {rolledBack:true,alreadyRolledBack:true,onlyAddedPrivilegesRevoked:true};}
 const status=await inspectUsageCascade(store,{binding:receipt.binding,requireSafe:true}),rows=await objects(store);need(hash(Object.fromEntries(Object.entries(receipt).filter(([key])=>key!=='contractDigest')))===hash(status.contract.rollbackState),'cascade_rollback_receipt_changed');need(rows.every(x=>x.owner===id.currentOid)&&status.markerOid===receipt.markerOid&&status.contract.repairId===receipt.repairId&&hash(status.contract)===receipt.contractDigest,'cascade_rollback_binding_changed');need(await aclDigest(store,{kind:'table',oid:receipt.markerOid})===receipt.markerAclDigest,'cascade_rollback_marker_acl_changed');
 for(const object of rows)need(await aclDigest(store,object)===receipt.after[object.kind+':'+object.oid],'cascade_rollback_acl_changed');
 await assertUsageCascadeConnectionsDrained(store,{binding:activeBinding});
 for(const name of names)await store.query(definition(name).replace('CREATE FUNCTION','CREATE OR REPLACE FUNCTION'));
 for(const action of receipt.added){const object=rows.find(x=>x.kind===action.kind&&x.oid===action.oid);need(object?.privileges.includes(action.privilege),'cascade_rollback_action_invalid');await store.query('REVOKE '+action.privilege+' ON '+(object.kind==='table'?'TABLE ':'FUNCTION ')+object.reference+' FROM '+quote(id.owner.rolname));}
 for(const object of rows)need(await aclDigest(store,object)===receipt.before[object.kind+':'+object.oid],'cascade_rollback_restore_mismatch');await store.query('DROP TABLE harbor.usage_rollup_cascade_contract');await verifyUsageRollupStructure(store);return {rolledBack:true,onlyAddedPrivilegesRevoked:true};
 });
}


// Resolve a lost maintenance ACK only after its transaction has released the
// same lock. A plain MVCC read could still observe the pre-repair generation.
export async function resolveUsageRollupCascade(store,{binding}={}){
 return store.transaction(async()=>{
  await store.query("SET LOCAL lock_timeout='2000ms'");
  await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-usage-cascade-repair',0))");
  const status=await inspectUsageCascade(store,{binding});return {safe:status.safe,legacyReadable:!status.safe};
 });
}
export async function planUsageRollupCascade(store,{runtimeWitness}={}){
 if(!await verifyUsageRollupAvailability(store))return {installed:false,safe:true,repairRequired:false};
 need(runtimeWitness&&['databaseOid','currentRoleOid','sessionRoleOid','backendPid'].every(key=>typeof runtimeWitness[key]==='string'&&/^\d+$/.test(runtimeWitness[key])),'cascade_runtime_witness_required');
 await store.query('SELECT pg_stat_clear_snapshot()');
 const live=await store.get('SELECT datid::text database_oid,usesysid::text session_role_oid FROM pg_stat_activity WHERE pid=$1::integer',runtimeWitness.backendPid);
 need(live?.database_oid===runtimeWitness.databaseOid&&live?.session_role_oid===runtimeWitness.sessionRoleOid,'cascade_runtime_witness_changed');
 const activation=await store.get('SELECT to_jsonb(a) value FROM harbor_operator.activation a WHERE singleton');
 const coreRows=await store.all("SELECT relname,oid::text oid,relowner::text owner FROM pg_class WHERE oid=ANY(ARRAY['harbor.members'::regclass,'harbor.keys'::regclass,'harbor.sessions'::regclass,'harbor.requests'::regclass,'harbor.request_prices'::regclass])");
 need(coreRows.length===5&&new Set(coreRows.map(row=>row.owner)).size===1,'cascade_core_identity');
 const foreignKey=await store.get("SELECT oid::text oid FROM pg_constraint WHERE contype='f' AND conrelid='harbor.request_prices'::regclass AND confrelid='harbor.requests'::regclass AND confdeltype='c'");
 const binding={databaseOid:runtimeWitness.databaseOid,runtimeOid:runtimeWitness.currentRoleOid,runtimeSessionRoleOid:runtimeWitness.sessionRoleOid,coreOwnerOid:coreRows[0].owner,coreTables:Object.fromEntries(coreRows.map(row=>[row.relname,row.oid])),schemaDigest:hash(JSON.stringify(await store.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version'))),activationDigest:hash(activation?.value),cascadeForeignKeyOid:foreignKey?.oid};
 const actual=await identity(store,binding);const status=await inspectUsageCascade(store,{binding});return {binding,coreOwnerRole:actual.owner.rolname,safe:status.safe,repairRequired:!status.safe};
}
export async function verifyUsageRollups(store,sql){
 if(sql!==undefined)return verifyUsageRollupStructure(store,sql);
 await inspectUsageCascade(store);return true;
}
export {inspectUsageCascade as verifyUsageRollupCascadeAvailability,repairUsageCascade as repairUsageRollupCascade,rollbackUsageCascade as rollbackUsageRollupCascade,recoverUsageCascadeReceipt as recoverUsageRollupCascadeReceipt,assertUsageCascadeConnectionsDrained as verifyUsageRollupCascadeConnectionsDrained};
