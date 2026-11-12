import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
import {hash,revealStoredKey} from '../store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {createPostgresMemberQueries,memberQuery} from '../postgres-member-query.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:30000};
function options(database='postgres'){const target=new URL(url);target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,poolSize:4,statementTimeoutMs:5000};}
async function fixture(run,{install=true,migrationOptions={}}={}){
 const root=new PostgresStore(options()),database='harbor_keyadmin_'+randomUUID().replaceAll('-','');let store,other,admin,second;
 const encryptionKey=randomBytes(32),at=Date.now();
 try{
  await root.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));other=new PostgresStore(options(database));await store.migrate(migrationOptions);await store.initializePricing();
  if(install){await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});}
  admin=createPostgresMemberAdmin(store,{encryptionKey,clock:()=>at});second=createPostgresMemberAdmin(other,{encryptionKey,clock:()=>at});
  const create=async(extra={})=>(await admin.handle('/api/admin/members','POST',{name:'合成员工',quota:null,expires:null,...extra})).value;
  const add=async(memberId,extra={})=>(await admin.handle('/api/admin/keys','POST',{memberId,expires:null,...extra})).value;
  const patch=(keyId,value)=>admin.handle('/api/admin/keys/'+keyId,'PATCH',value);
  await run({store,other,admin,second,create,add,patch,at,encryptionKey,database});
 }finally{await admin?.close();await second?.close();await other?.close();await store?.close();await root.query('DROP DATABASE IF EXISTS '+database);if(install)await root.query('DROP ROLE IF EXISTS '+database+'_runtime');await root.close();encryptionKey.fill(0);}
}
const columns='id,member_id,enabled,expires,quota,allowed_models,model_policy_version,model_source,consumption_policy,consumption_policy_version';
const counts=store=>store.get('SELECT (SELECT count(*) FROM harbor.members) members,(SELECT count(*) FROM harbor.keys) keys,(SELECT count(*) FROM harbor.audit) audit');
const keyQuery=params=>memberQuery(new URLSearchParams(params),{keys:true});

test('employee and Key query pagination defaults to twenty and supports fifty/hundred',()=>{
 assert.equal(memberQuery(new URLSearchParams()).pageSize,20);assert.equal(keyQuery().pageSize,20);
 for(const size of [20,50,100])assert.equal(keyQuery('page_size='+size).pageSize,size);
});

test('both creation entries store an explicit policy at version zero while preserving encryption',integration,()=>fixture(async f=>{
 for(const policy of ['inherit','api_only','subscription_only','api_first']){
  const member=await f.create({consumption_policy:policy}),key=await f.add(member.memberId,{consumption_policy:policy});
  for(const value of [member,key]){
   assert.equal(value.consumption_policy,policy);assert.equal(value.consumption_policy_version,0);
   const row=await f.store.get('SELECT * FROM harbor.keys WHERE id=$1',value.keyId);assert.equal(row.consumption_policy,policy);assert.equal(row.consumption_policy_version,0);assert.equal(hash(revealStoredKey(f.encryptionKey,row)),hash(value.secret));assert.equal(row.digest,hash(value.secret));assert.equal(row.used,0);
  }
 }
 const old=await f.create(),extra=await f.add(old.memberId);assert.equal(old.consumption_policy,'inherit');assert.equal(extra.consumption_policy_version,0);
 assert.equal((await f.store.get("SELECT count(*) count FROM harbor.audit WHERE action='key.consumption_policy'")).count,8);
}));
test('creation rejects illegal values and self-reported versions with no partial employee/Key writes',integration,()=>fixture(async f=>{
 const member=await f.create(),before=await counts(f.store);
 for(const invalid of [{consumption_policy:'invalid'},{consumption_policy:null},{consumption_policy:undefined},{consumption_policy_version:0},{expected_consumption_policy_version:0},{consumption_policy:'api_only',expected_consumption_policy_version:0},{consumption_policy:'api_only',consumption_policy_version:0}]){
  await assert.rejects(f.create(invalid),{status:400});await assert.rejects(f.add(member.memberId,invalid),{status:400});assert.deepEqual(await counts(f.store),before);
 }
 for(const value of [{consumption_policy:'api_only'},{consumption_policy_version:0},{expected_consumption_policy_version:0}])await assert.rejects(f.admin.handle('/api/admin/members/'+member.memberId,'PATCH',value),{code:'consumption_policy_invalid'});
}));
test('legacy management works without extension while explicit new fields fail closed',integration,()=>fixture(async f=>{
 const member=await f.create(),extra=await f.add(member.memberId);assert.equal(Object.hasOwn(member,'consumption_policy'),false);assert.equal(Object.hasOwn(extra,'consumption_policy_version'),false);
 const before=await counts(f.store);
 for(const consumption_policy of ['inherit','api_only','subscription_only','api_first']){
  await assert.rejects(f.create({consumption_policy}),{code:'database_migration_required'});await assert.rejects(f.add(member.memberId,{consumption_policy}),{code:'database_migration_required'});
 }
 const keyBefore=await f.store.get('SELECT * FROM harbor.keys WHERE id=$1',member.keyId);
 await assert.rejects(f.patch(member.keyId,{consumption_policy:'api_only',expected_consumption_policy_version:0,enabled:false}),{code:'database_migration_required'});assert.deepEqual(await f.store.get('SELECT * FROM harbor.keys WHERE id=$1',member.keyId),keyBefore);assert.deepEqual(await counts(f.store),before);
 await f.patch(member.keyId,{enabled:false});assert.equal((await f.store.get('SELECT enabled FROM harbor.keys WHERE id=$1',member.keyId)).enabled,0);
},{install:false}));
test('source/model/enable/expiry/quota changes commit together and policy no-op keeps its version',integration,()=>fixture(async f=>{
 const member=await f.create(),value={consumption_policy:'api_only',expected_consumption_policy_version:0,allowed_models:['gpt-6.1-sol'],expected_model_policy_version:0,enabled:false,expires:f.at+3600000,quota:123};
 const result=await f.patch(member.keyId,value);assert.deepEqual(result.value,{ok:true,consumption_policy:'api_only',consumption_policy_version:1});
 const row=await f.store.get('SELECT '+columns+' FROM harbor.keys WHERE id=$1',member.keyId);assert.equal(row.allowed_models,'["gpt-6.1-sol"]');assert.equal(row.model_policy_version,1);assert.equal(row.consumption_policy_version,1);assert.equal(row.enabled,0);assert.equal(row.expires,value.expires);assert.equal(row.quota,123);
 await f.patch(member.keyId,{consumption_policy:'api_only',expected_consumption_policy_version:1});assert.deepEqual(await f.store.get('SELECT '+columns+' FROM harbor.keys WHERE id=$1',member.keyId),row);assert.equal((await f.store.get("SELECT count(*) count FROM harbor.audit WHERE action='key.consumption_policy'")).count,1);
}));
test('partial CAS, either stale version and invalid ordinary fields cannot cause partial source/model writes',integration,()=>fixture(async f=>{
 const member=await f.create();await f.patch(member.keyId,{consumption_policy:'api_only',expected_consumption_policy_version:0,allowed_models:['gpt-6.1-sol'],expected_model_policy_version:0});
 const before=await f.store.get('SELECT '+columns+' FROM harbor.keys WHERE id=$1',member.keyId),totals=await counts(f.store);
 for(const value of [{consumption_policy:'subscription_only'},{expected_consumption_policy_version:1},{consumption_policy:'subscription_only',expected_consumption_policy_version:'1'},{consumption_policy:'subscription_only',expected_consumption_policy_version:1,consumption_policy_version:2},{consumption_policy:'subscription_only',expected_consumption_policy_version:0,allowed_models:null,expected_model_policy_version:1,enabled:false},{consumption_policy:'subscription_only',expected_consumption_policy_version:1,allowed_models:null,expected_model_policy_version:0},{consumption_policy:'subscription_only',expected_consumption_policy_version:1,enabled:'false'},{consumption_policy:'subscription_only',expected_consumption_policy_version:1,expires:-1},{consumption_policy:'subscription_only',expected_consumption_policy_version:1,quota:-1}]){
  await assert.rejects(f.patch(member.keyId,value));assert.deepEqual(await f.store.get('SELECT '+columns+' FROM harbor.keys WHERE id=$1',member.keyId),before);assert.deepEqual(await counts(f.store),totals);
 }
 const original=f.store.run.bind(f.store);f.store.run=async(sql,...args)=>{if(sql==='INSERT INTO harbor.audit VALUES($1,$2,$3,$4)'&&args[2]==='key.update')throw Error('synthetic audit failure');return original(sql,...args);};
 try{await assert.rejects(f.patch(member.keyId,{consumption_policy:'subscription_only',expected_consumption_policy_version:1,allowed_models:null,expected_model_policy_version:1}),/synthetic audit failure/);}finally{f.store.run=original;}
 assert.deepEqual(await f.store.get('SELECT '+columns+' FROM harbor.keys WHERE id=$1',member.keyId),before);assert.deepEqual(await counts(f.store),totals);
}));
test('concurrent combined updates have one winner without losing the other version or settings',integration,()=>fixture(async f=>{
 const member=await f.create();
 const outcomes=await Promise.allSettled([f.admin,f.second].map((backend,i)=>backend.handle('/api/admin/keys/'+member.keyId,'PATCH',{consumption_policy:i?'subscription_only':'api_only',expected_consumption_policy_version:0,allowed_models:i?null:['gpt-6.1-sol'],expected_model_policy_version:0,enabled:!i})));
 assert.equal(outcomes.filter(row=>row.status==='fulfilled').length,1);assert.equal(outcomes.find(row=>row.status==='rejected').reason.status,409);
 const row=await f.store.get('SELECT '+columns+' FROM harbor.keys WHERE id=$1',member.keyId);assert.equal(row.consumption_policy_version,1);assert.equal(row.model_policy_version,1);assert.equal(row.enabled,row.consumption_policy==='api_only'?1:0);assert.equal(row.allowed_models,row.consumption_policy==='api_only'?'["gpt-6.1-sol"]':null);
}));
test('deleted Key/member boundaries and employee model defaults are preserved',integration,()=>fixture(async f=>{
 const member=await f.create();await f.store.run('UPDATE harbor.members SET default_models=$1,default_models_set=1,defaults_version=3 WHERE id=$2','["gpt-6.1-sol"]',member.memberId);
 const key=await f.add(member.memberId,{consumption_policy:'subscription_only',expected_defaults_version:3});const row=await f.store.get('SELECT '+columns+' FROM harbor.keys WHERE id=$1',key.keyId);assert.equal(row.allowed_models,'["gpt-6.1-sol"]');assert.equal(row.model_source.kind,'employee');assert.equal(row.model_source.defaults_version,3);assert.equal(row.consumption_policy_version,0);
 const before=await counts(f.store);await assert.rejects(f.add(member.memberId,{consumption_policy:'api_only',expected_defaults_version:2}),{code:'employee_defaults_conflict'});assert.deepEqual(await counts(f.store),before);
 await f.admin.handle('/api/admin/keys/'+key.keyId,'DELETE',{});await assert.rejects(f.patch(key.keyId,{consumption_policy:'api_only',expected_consumption_policy_version:0}),{status:404});
 await f.admin.handle('/api/admin/members/'+member.memberId,'DELETE',{});await assert.rejects(f.add(member.memberId,{consumption_policy:'api_only'}),{status:404});await assert.rejects(f.patch(member.keyId,{consumption_policy:'api_only',expected_consumption_policy_version:0}),{status:404});
}));
test('optional context migration keeps direct Key creation policy/version and current core unchanged',integration,async()=>{
 for(const [version,migrationOptions] of [[23,{recordingContext:true}],[24,{recordingContextIndex:true}]])await fixture(async f=>{
  const member=await f.create({consumption_policy:'api_first'});assert.equal(member.consumption_policy_version,0);assert.equal((await f.store.get('SELECT max(version) version FROM harbor.schema_migrations')).version,version);
  await f.patch(member.keyId,{consumption_policy:'subscription_only',expected_consumption_policy_version:0});assert.equal((await f.store.get('SELECT consumption_policy_version FROM harbor.keys WHERE id=$1',member.keyId)).consumption_policy_version,1);
 },{migrationOptions});
});
test('real gateway admin authentication gates create/patch source policy and employee Key cannot manage it',integration,()=>fixture(async f=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-key-policy-http-'));let app;
 try{
  app=await createPostgresGateway({env:{},database:options(f.database),dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-consumption-admin')},recording:{enabled:false}});await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));
  const base='http://127.0.0.1:'+app.server.address().port,send=(path,body,headers={},method='POST')=>fetch(base+path,{method,headers:{'content-type':'application/json',...headers},body:JSON.stringify(body)});
  assert.equal((await send('/api/admin/members',{name:'synthetic',quota:null,expires:null,consumption_policy:'api_only'})).status,401);
  const login=await send('/api/auth/login',{password:'synthetic-consumption-admin'});assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
  const response=await send('/api/admin/members',{name:'合成HTTP员工',quota:null,expires:null,consumption_policy:'api_only'},{cookie});assert.equal(response.status,201);const created=await response.json();assert.equal(created.consumption_policy,'api_only');assert.equal(created.consumption_policy_version,0);
  const body={consumption_policy:'subscription_only',expected_consumption_policy_version:0};assert.equal((await send('/api/admin/keys/'+created.keyId,body,{authorization:'Bearer '+created.secret},'PATCH')).status,401);
  const changed=await send('/api/admin/keys/'+created.keyId,body,{cookie},'PATCH');assert.equal(changed.status,200);assert.equal((await changed.json()).consumption_policy_version,1);
  const authority=await fetch(base+'/api/admin/keys/'+created.keyId+'/policy',{headers:{cookie}});assert.equal(authority.status,200);const current=await authority.json();assert.equal(current.key_id,created.keyId);assert.equal(current.member_id,created.memberId);assert.equal(current.consumption_policy,'subscription_only');assert.equal(current.consumption_policy_version,1);assert.equal(current.model_policy_version,0);
  assert.equal((await fetch(base+'/api/admin/keys/'+created.keyId+'/policy',{headers:{authorization:'Bearer '+created.secret}})).status,401);
  assert.equal((await fetch(base+'/api/admin/keys/'+created.keyId+'/policy?member_id='+randomUUID(),{headers:{cookie}})).status,400);
  const listed=await fetch(base+'/api/admin/keys',{headers:{cookie}});assert.equal(listed.status,200);const listing=await listed.json();assert.equal(listing.pagination.pageSize,20);assert.equal(listing.rows[0].consumption_policy,'subscription_only');assert.equal(JSON.stringify(listing).includes(created.secret),false);assert.equal(Object.hasOwn(listing.rows[0],'digest'),false);assert.equal(Object.hasOwn(listing.rows[0],'secret_cipher'),false);
  const added=await send('/api/admin/keys',{memberId:created.memberId,expires:null,consumption_policy:'api_first'},{cookie});assert.equal(added.status,201);assert.equal((await added.json()).consumption_policy_version,0);
 }finally{await app?.close();await rm(directory,{recursive:true,force:true});}
}));
test('missing extension retains the old list/detail/policy DTO and never reads new columns',integration,()=>fixture(async f=>{
 const created=await f.create(),queries=createPostgresMemberQueries(f.other,{encryptionKey:f.encryptionKey}),sqls=[],original=f.other.query.bind(f.other);f.other.query=(sql,args)=>{sqls.push(sql);return original(sql,args);};
 const list=await queries.keys(keyQuery()),detail=await queries.keys(keyQuery(),{memberId:created.memberId,reveal:true}),policy=await queries.keyPolicy(created.keyId);
 for(const row of [list.rows[0],detail.rows[0]]){assert.equal(Object.hasOwn(row,'consumption_policy'),false);assert.equal(Object.hasOwn(row,'consumption_policy_version'),false);}
 assert.deepEqual(policy,{allowed_models:null,usage_limits:{enabled:false,rules:[],version:0}});assert.equal(list.rows[0].credentials_deferred,true);assert.equal(Object.hasOwn(list.rows[0],'full_key'),false);
 assert.equal(sqls.some(sql=>sql.startsWith('SELECT k.')&&sql.includes('consumption_policy')),false);
 for(const result of [list,policy]){assert.equal(JSON.stringify(result).includes(created.secret),false);assert.equal(JSON.stringify(result).includes('secret_cipher'),false);assert.equal(JSON.stringify(result).includes('digest'),false);}
 assert.equal(hash(detail.rows[0].full_key),hash(created.secret));
},{install:false}));
test('Key lists and policy authority endpoint return fresh valid source/model versions without credentials',integration,()=>fixture(async f=>{
 const a=await f.create({consumption_policy:'api_only',allowed_models:['gpt-6.1-sol']}),b=await f.create({consumption_policy:'subscription_only'}),queries=createPostgresMemberQueries(f.other,{encryptionKey:f.encryptionKey});
 const list=await queries.keys(keyQuery());assert.equal(list.rows.find(row=>row.id===a.keyId).consumption_policy,'api_only');assert.equal(list.rows.find(row=>row.id===b.keyId).consumption_policy,'subscription_only');
 const policy=await queries.keyPolicy(a.keyId);assert.deepEqual(policy,{allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[],version:0},key_id:a.keyId,member_id:a.memberId,model_policy_version:0,consumption_policy:'api_only',consumption_policy_version:0});
 await f.patch(a.keyId,{consumption_policy:'api_first',expected_consumption_policy_version:0,allowed_models:null,expected_model_policy_version:0});
 const changed=await queries.keyPolicy(a.keyId);assert.equal(changed.model_policy_version,1);assert.equal(changed.allowed_models,null);assert.equal(changed.consumption_policy,'api_first');assert.equal(changed.consumption_policy_version,1);
 const detail=await queries.keys(keyQuery(),{memberId:a.memberId,reveal:true});assert.equal(detail.rows[0].consumption_policy,'api_first');assert.equal(detail.rows[0].consumption_policy_version,1);assert.equal(hash(detail.rows[0].full_key),hash(a.secret));
 for(const result of [list,policy,changed]){assert.equal(JSON.stringify(result).includes(a.secret),false);assert.equal(JSON.stringify(result).includes(b.secret),false);for(const name of ['secret_cipher','digest','full_key'])assert.equal(JSON.stringify(result).includes('"'+name+'"'),false);}
}));
test('source schema initialization happens once and list query count does not grow per Key',integration,()=>fixture(async f=>{
 const a=await f.create({consumption_policy:'api_only'}),b=await f.create({consumption_policy:'subscription_only'}),queries=createPostgresMemberQueries(f.other),calls=[],original=f.other.query.bind(f.other);f.other.query=(sql,args)=>{calls.push({sql,sourceMarker:sql==='SELECT value FROM harbor.gateway_settings WHERE name=$1'&&args[0]==='schema.key-consumption-policy'});return original(sql,args);};
 await queries.keys(keyQuery());assert.equal(calls.filter(row=>row.sourceMarker).length,1);assert.ok(calls.some(row=>row.sql.includes('pg_proc')));
 calls.length=0;await queries.keys(keyQuery());const small=calls.length;assert.equal(calls.some(row=>row.sourceMarker||/pg_proc|pg_constraint|pg_attribute/.test(row.sql)),false);
 for(let i=0;i<21;i++)await f.add(a.memberId,{consumption_policy:i%2?'api_first':'subscription_only'});
 calls.length=0;const large=await queries.keys(keyQuery('page_size=50'));assert.equal(large.rows.length,23);assert.equal(calls.length,small);assert.equal(calls.some(row=>row.sourceMarker||/pg_proc|pg_constraint|pg_attribute/.test(row.sql)),false);
 calls.length=0;await queries.keyPolicy(b.keyId);assert.equal(calls.length,1);assert.equal(calls[0].sql.includes('secret_cipher'),false);assert.equal(calls[0].sql.includes('digest'),false);
 const first=await queries.keys(keyQuery()),second=await queries.keys(keyQuery('page=2'));assert.equal(first.rows.length,20);assert.equal(second.rows.length,3);assert.equal(first.pagination.pageSize,20);assert.equal(new Set([...first.rows,...second.rows].map(row=>row.id)).size,23);
}));
test('employee-scoped pages cannot mix owners and deleted Key/member policies disappear',integration,()=>fixture(async f=>{
 const a=await f.create({consumption_policy:'api_only'}),b=await f.create({consumption_policy:'subscription_only'}),extra=await f.add(a.memberId,{consumption_policy:'api_first'}),queries=createPostgresMemberQueries(f.other,{encryptionKey:f.encryptionKey});
 const scoped=await queries.keys(keyQuery('member_id='+b.memberId),{memberId:a.memberId,reveal:true});assert.equal(scoped.member.id,a.memberId);assert.deepEqual(new Set(scoped.rows.map(row=>row.id)),new Set([a.keyId,extra.keyId]));assert.ok(scoped.rows.every(row=>row.member_id===a.memberId));
 const filtered=await queries.keys(keyQuery('member_id='+b.memberId));assert.equal(filtered.rows.length,1);assert.equal(filtered.rows[0].id,b.keyId);assert.equal((await queries.keyPolicy(b.keyId)).member_id,b.memberId);
 await f.admin.handle('/api/admin/keys/'+extra.keyId,'DELETE',{});await assert.rejects(queries.keyPolicy(extra.keyId),{code:'key_not_found'});
 await f.admin.handle('/api/admin/members/'+a.memberId,'DELETE',{});await assert.rejects(queries.keyPolicy(a.keyId),{code:'key_not_found'});await assert.rejects(queries.keys(keyQuery(),{memberId:a.memberId,reveal:true}),{code:'member_not_found'});
 const remaining=await queries.keys(keyQuery());assert.equal(remaining.rows.length,1);assert.equal(remaining.rows[0].id,b.keyId);
}));
test('invalid stored explicit policy and schema drift fail closed rather than invent inherit',integration,()=>fixture(async f=>{
 const created=await f.create({consumption_policy:'api_only'}),queries=createPostgresMemberQueries(f.other);await queries.keys(keyQuery());
 await f.store.query('ALTER TABLE harbor.keys DROP CONSTRAINT keys_consumption_policy_valid');
 try{
  await f.store.run("UPDATE harbor.keys SET consumption_policy='invalid' WHERE id=$1",created.keyId);
  await assert.rejects(queries.keys(keyQuery()),{status:503,code:'consumption_policy_invalid'});await assert.rejects(queries.keyPolicy(created.keyId),{status:503,code:'consumption_policy_invalid'});
  const fresh=createPostgresMemberQueries(f.other);await assert.rejects(fresh.keys(keyQuery()),{code:'database_migration_conflict'});
 }finally{await f.store.run("UPDATE harbor.keys SET consumption_policy='api_only' WHERE id=$1",created.keyId);await f.store.query('ALTER TABLE harbor.keys ADD CONSTRAINT keys_consumption_policy_valid CHECK(harbor.key_consumption_valid(consumption_policy,consumption_policy_version))');}
}));
test('partial source schema without marker rejects initialization instead of silently serving an old DTO',integration,()=>fixture(async f=>{
 await f.create();await f.store.query('ALTER TABLE harbor.keys ADD COLUMN consumption_policy TEXT');
 const queries=createPostgresMemberQueries(f.other);await assert.rejects(queries.keys(keyQuery()),{code:'database_migration_conflict'});await assert.rejects(queries.keyPolicy(randomUUID()),{code:'database_migration_conflict'});
},{install:false}));
