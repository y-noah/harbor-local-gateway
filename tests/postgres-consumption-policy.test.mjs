import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy,verifyConsumptionPolicyAvailability,createPostgresConsumptionPolicy,normalizeConsumptionAttempts} from '../postgres-consumption-policy.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:30000};
function options(database='postgres'){const target=new URL(url);target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,poolSize:4,statementTimeoutMs:5000};}
async function fixture(run,{migrationOptions={},install=true,activationContract='operator'}={}){
 const admin=new PostgresStore(options()),database='harbor_source_'+randomUUID().replaceAll('-','');let store,other;
 try{
  await admin.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));other=new PostgresStore(options(database));await store.migrate(migrationOptions);const role=await createUsageRollupRuntime(store,{activationContract});
  await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0),('n','synthetic',0)");
  await store.query("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('k','m','synthetic-k','synthetic',0),('l','n','synthetic-l','synthetic',0)");
  const request=id=>store.run("INSERT INTO harbor.requests(id,member_id,key_id,started,status,ip) VALUES($1,'m','k',0,'running','synthetic')",id);
  await request('historical');if(install)await migrateConsumptionPolicy(store,{apply:true,runtimeRole:activationContract==='local-assurance'?role:undefined});
  await run({store,other,role,request,policy:createPostgresConsumptionPolicy(store,{clock:()=>1000})});
 }finally{await other?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+database);await admin.query('DROP ROLE IF EXISTS '+database+'_runtime');await admin.close();}
}
const attempt=(source='api',extra={})=>({source,account_id:source==='api'?'synthetic-api':'synthetic-subscription',state:'dispatched',usage_known:false,started:10,...extra});
const rejected=()=>attempt('api',{state:'rejected',usage_known:true,error_code:'upstream_quota_exhausted',ended:20,input:0,output:0,cached:0});
const metadataFields='consumption_policy,consumption_policy_version,consumption_source,consumption_fallback,consumption_attempts';
const admission=async(store,policy,requestId,snapshot={consumption_policy:'inherit',consumption_policy_version:0})=>store.transaction(()=>policy.recordAdmission(requestId,'k',snapshot));

test('attempt metadata validates types, stage boundaries and unknown usage without raw content',()=>{
 assert.deepEqual(normalizeConsumptionAttempts([attempt()]),[attempt()]);
 assert.deepEqual(normalizeConsumptionAttempts([rejected(),attempt('subscription')]),[rejected(),attempt('subscription')]);
 for(const item of [null,{},attempt('other'),attempt('api',{started:-1}),attempt('api',{started:'10'}),attempt('api',{ended:9}),attempt('api',{input:0}),attempt('api',{error_code:'raw error message'}),attempt('api',{headers:{}}),attempt('api',{body:'synthetic'}),attempt('api',{usage_known:true,input:Number.MAX_SAFE_INTEGER+1}),attempt('api',{usage_known:true,output:0.1})])assert.throws(()=>normalizeConsumptionAttempts([item]),{code:'consumption_metadata_invalid'});
 for(const items of [[attempt(),attempt('subscription')],[rejected(),attempt('api')],[rejected(),attempt('subscription'),attempt('subscription')]])assert.throws(()=>normalizeConsumptionAttempts(items),{code:'consumption_metadata_invalid'});
 assert.throws(()=>normalizeConsumptionAttempts(new Array(1)),{code:'consumption_metadata_invalid'});
});
test('independent extension stays absent by default and missing metadata fails closed',integration,()=>fixture(async({store,policy})=>{
 assert.equal(await migrateConsumptionPolicy(store),false);assert.equal(await verifyConsumptionPolicyAvailability(store),false);
 await assert.rejects(policy.read('k'),{code:'database_migration_required'});
 assert.equal((await store.get('SELECT max(version) version FROM harbor.schema_migrations')).version,22);
 assert.equal(await migrateConsumptionPolicy(store,{apply:true}),true);assert.equal(await migrateConsumptionPolicy(store,{apply:true}),false);assert.equal(await verifyConsumptionPolicyAvailability(store,{required:true}),true);
 assert.deepEqual(await policy.read('k'),{consumption_policy:'inherit',consumption_policy_version:0});
 assert.deepEqual(await store.get('SELECT '+metadataFields+" FROM harbor.requests WHERE id='historical'"),{consumption_policy:null,consumption_policy_version:null,consumption_source:null,consumption_fallback:null,consumption_attempts:null});
 assert.equal((await store.get("SELECT to_regclass('harbor.request_context') name")).name,null);
},{install:false}));
test('extension supports core22 and optional23/24 without altering the selected core version',integration,async()=>{
 for(const [version,migrationOptions] of [[22,{}],[23,{recordingContext:true}],[24,{recordingContextIndex:true}]])await fixture(async({store})=>{
  assert.equal((await store.get('SELECT max(version) version FROM harbor.schema_migrations')).version,version);
  assert.equal(await verifyConsumptionPolicyAvailability(store,{required:true}),true);assert.equal(await store.verifySchema(),true);
 },{migrationOptions});
});
test('policy changes use owner-first CAS, preserve no-op version and atomically record audit',integration,()=>fixture(async({store,policy})=>{
 assert.deepEqual(await policy.set('k',{consumption_policy:'api_only',expected_consumption_policy_version:0}),{consumption_policy:'api_only',consumption_policy_version:1});
 assert.deepEqual(await policy.set('k',{consumption_policy:'api_only',expected_consumption_policy_version:1}),{consumption_policy:'api_only',consumption_policy_version:1});
 assert.equal((await store.get("SELECT count(*) count FROM harbor.audit WHERE action='key.consumption_policy'")).count,1);
 await assert.rejects(policy.set('k',{consumption_policy:'subscription_only',expected_consumption_policy_version:0}),{status:409,code:'consumption_policy_conflict'});
 for(const value of [{consumption_policy:'invalid',expected_consumption_policy_version:1},{consumption_policy:'inherit',expected_consumption_policy_version:'1'},{consumption_policy:'inherit',expected_consumption_policy_version:1,trusted:true}])await assert.rejects(policy.set('k',value),{status:400});
 await assert.rejects(policy.read('k',{memberId:'n'}),{status:404});await assert.rejects(policy.read('k',{lock:true}),{code:'database_transaction_required'});
 assert.deepEqual(await store.transaction(()=>policy.read('k',{memberId:'m',lock:true})),{consumption_policy:'api_only',consumption_policy_version:1});
 await assert.rejects(store.transaction(async()=>{await policy.set('k',{consumption_policy:'subscription_only',expected_consumption_policy_version:1});throw Error('synthetic rollback');}),/synthetic rollback/);
 assert.equal((await policy.read('k')).consumption_policy_version,1);assert.equal((await store.get('SELECT count(*) count FROM harbor.audit')).count,1);
}));
test('concurrent policy CAS has one success and no lost update',integration,()=>fixture(async({store,other,policy})=>{
 const results=await Promise.allSettled([policy.set('k',{consumption_policy:'api_only',expected_consumption_policy_version:0}),createPostgresConsumptionPolicy(other).set('k',{consumption_policy:'subscription_only',expected_consumption_policy_version:0})]);
 assert.equal(results.filter(row=>row.status==='fulfilled').length,1);assert.equal(results.find(row=>row.status==='rejected').reason.code,'consumption_policy_conflict');
 assert.equal((await policy.read('k')).consumption_policy_version,1);assert.equal((await store.get('SELECT count(*) count FROM harbor.audit')).count,1);
}));
test('request snapshots bind the real Key policy while subsequent changes preserve admitted history',integration,()=>fixture(async({store,request,policy})=>{
 await policy.set('k',{consumption_policy:'api_only',expected_consumption_policy_version:0});await request('r');
 await assert.rejects(policy.recordAdmission('r','k',{consumption_policy:'api_only',consumption_policy_version:1}),{code:'database_transaction_required'});
 await assert.rejects(admission(store,policy,'r'),{code:'consumption_policy_conflict'});
 await admission(store,policy,'r',{consumption_policy:'api_only',consumption_policy_version:1});
 await policy.set('k',{consumption_policy:'subscription_only',expected_consumption_policy_version:1});
 await policy.recordAttempts('r','k',[attempt('api',{state:'completed',ended:30,usage_known:true,input:11,output:7,cached:3})]);
 const row=await store.get('SELECT '+metadataFields+" FROM harbor.requests WHERE id='r'");assert.equal(row.consumption_policy,'api_only');assert.equal(row.consumption_policy_version,1);assert.equal(row.consumption_source,'api');assert.equal(row.consumption_fallback,false);assert.equal(row.consumption_attempts[0].input,11);
 await assert.rejects(policy.recordAttempts('r','l',[attempt()]),{code:'consumption_policy_conflict'});
 await assert.rejects(policy.recordAttempts('r','k',[]),{code:'consumption_metadata_conflict'});
 await assert.rejects(policy.recordAttempts('r','k',[attempt('api',{state:'completed',ended:30,usage_known:true,input:0,output:7,cached:3})]),{code:'consumption_metadata_conflict'});
 await assert.rejects(policy.recordAttempts('r','k',[attempt('api',{state:'dispatched',ended:30,usage_known:true,input:11,output:7,cached:3})]),{code:'consumption_metadata_conflict'});
 await assert.rejects(admission(store,policy,'r',{consumption_policy:'subscription_only',consumption_policy_version:2}),{code:'consumption_policy_conflict'});
}));
test('fallback metadata permits only one confirmed API rejection followed by subscription',integration,()=>fixture(async({store,request,policy})=>{
 await policy.set('k',{consumption_policy:'api_first',expected_consumption_policy_version:0});await request('r');await admission(store,policy,'r',{consumption_policy:'api_first',consumption_policy_version:1});
 await policy.recordAttempts('r','k',[rejected()]);await policy.recordAttempts('r','k',[rejected(),attempt('subscription')]);
 const final=attempt('subscription',{state:'completed',usage_known:true,input:12,output:13,cached:4,ended:40});await policy.recordAttempts('r','k',[rejected(),final]);
 const row=await store.get('SELECT '+metadataFields+" FROM harbor.requests WHERE id='r'");assert.equal(row.consumption_source,'subscription');assert.equal(row.consumption_fallback,true);assert.equal(row.consumption_attempts.length,2);assert.equal(row.consumption_attempts[1].output,13);
 await assert.rejects(policy.recordAttempts('r','k',[rejected()]),{code:'consumption_metadata_conflict'});
}));
test('SQL constraints independently reject damaged policy and fabricated/unknown attempt metadata',integration,()=>fixture(async({store,request,policy})=>{
 for(const sql of ["UPDATE harbor.keys SET consumption_policy='bad' WHERE id='k'","UPDATE harbor.keys SET consumption_policy_version=-1 WHERE id='k'","UPDATE harbor.keys SET consumption_policy_version=9007199254740992 WHERE id='k'","UPDATE harbor.requests SET consumption_source='api' WHERE id='historical'"])await assert.rejects(store.query(sql),{code:'23514'});
 await request('r');await admission(store,policy,'r');
 for(const data of [[attempt('api',{input:0})],[attempt('api',{started:'10'})],[attempt('api',{body:'synthetic'})],[attempt(),attempt('subscription')],[rejected(),attempt('subscription')]])await assert.rejects(store.run("UPDATE harbor.requests SET consumption_source=$1,consumption_fallback=$2,consumption_attempts=$3::jsonb WHERE id='r'",data.at(-1).source,data.length===2,JSON.stringify(data)),{code:'23514'});
 assert.equal(await verifyConsumptionPolicyAvailability(store,{required:true}),true);
}));
test('runtime can use only granted extension functions and owner-only DDL remains rejected',integration,()=>fixture(async({store,role,policy})=>{
 await store.transaction(async()=>{await store.query('SET LOCAL ROLE "'+role+'"');assert.equal((await policy.read('k')).consumption_policy,'inherit');await policy.set('k',{consumption_policy:'api_only',expected_consumption_policy_version:0});});
 await assert.rejects(store.transaction(async()=>{await store.query('SET LOCAL ROLE "'+role+'"');await store.query("ALTER TABLE harbor.keys ADD COLUMN synthetic TEXT");}),{code:'42501'});
 assert.equal((await policy.read('k')).consumption_policy,'api_only');
}));
test('schema verification rejects marker drift, weak constraints, function edits and public execute',integration,()=>fixture(async({store})=>{
 const changes=["UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.key-consumption-policy'","ALTER TABLE harbor.keys DROP CONSTRAINT keys_consumption_policy_valid;ALTER TABLE harbor.keys ADD CONSTRAINT keys_consumption_policy_valid CHECK(true)","ALTER TABLE harbor.requests ALTER COLUMN consumption_policy SET DEFAULT 'inherit'","CREATE OR REPLACE FUNCTION harbor.key_consumption_valid(policy TEXT,version BIGINT) RETURNS BOOLEAN LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,harbor AS $$ SELECT TRUE $$","GRANT EXECUTE ON FUNCTION harbor.key_consumption_valid(TEXT,BIGINT) TO PUBLIC","DELETE FROM harbor.gateway_settings WHERE name='schema.key-consumption-policy'"];
 for(const sql of changes){await assert.rejects(store.transaction(async()=>{await store.query(sql);await verifyConsumptionPolicyAvailability(store,{required:true});}),{code:'database_migration_conflict'});assert.equal(await verifyConsumptionPolicyAvailability(store,{required:true}),true);}
}));
test('local-assurance activation requires an explicit verified runtime role',integration,()=>fixture(async({store,role})=>{
 await assert.rejects(migrateConsumptionPolicy(store,{apply:true}),{code:'consumption_runtime_invalid'});
 assert.equal(await migrateConsumptionPolicy(store,{apply:true,runtimeRole:role}),true);assert.equal(await verifyConsumptionPolicyAvailability(store,{required:true}),true);
},{activationContract:'local-assurance',install:false}));
