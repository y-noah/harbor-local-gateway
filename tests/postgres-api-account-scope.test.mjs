import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy,createPostgresConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createPostgresConsumptionRouting} from '../postgres-consumption-routing.mjs';
import {createPostgresApiAccountScope} from '../postgres-api-account-scope.mjs';
import {createApiQuotaRejection} from '../consumption-policy.mjs';
const enabled=!!process.env.HARBOR_CAPACITY_PG_BIN,options={skip:!enabled?'需隔离PG18运行环境':false,timeout:60000};let lab;
before(async()=>{if(enabled)lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});});after(async()=>{await lab?.close();});
async function fixture(t,{schema=true}={}){
 const admin=new PostgresStore({connection:lab.connection}),database='hcr_'+randomUUID().replaceAll('-','');await admin.query('CREATE DATABASE '+database);const store=new PostgresStore({connection:{...lab.connection,database}});let runtime;
 t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+database);if(runtime)await admin.query('DROP ROLE '+runtime);await admin.close();});await store.migrate();if(schema){runtime=await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});}
 const at=Date.parse('2026-11-12T08:00:00Z'),clock=()=>at,member={id:randomUUID(),key_id:randomUUID()},otherKey=randomUUID(),policy=createPostgresConsumptionPolicy(store,{clock});await store.run("INSERT INTO harbor.members(id,name,created) VALUES($1,'合成来源员工',$2)",member.id,at);for(const key of [member.key_id,otherKey])await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$1,'synthetic',$3)",key,member.id,at);
 async function account(source,patch={}){const id=randomUUID(),quota=JSON.stringify({checked:at,remaining:50,plan:'pro',windows:[{used:50,minutes:10080,reset:at+86400000}]});await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,api_weight,quota_json,enabled,route_block) VALUES($1,'合成来源账号','synthetic','ready',$2,$3,$4,$5,$6)",id,source==='api'?'openai_api':'subscription',patch.api_weight??20,quota,patch.enabled??1,patch.route_block??null);return id;}
 const api=await account('api'),subscription=await account('subscription'),router=createPostgresRouter(store,{clock}),service=createPostgresConsumptionRouting(store,{router,clock,schemaAvailable:schema}),payload={model:'gpt-6.1-sol',input:'完整合成上下文'},headers={'thread-id':'synthetic-thread'};
 const set=async(value,key=member.key_id)=>{const current=await policy.read(key);return policy.set(key,{consumption_policy:value,expected_consumption_policy_version:current.consumption_policy_version});};
 const resolve=(value=payload,head=headers,identity=member)=>store.transaction(()=>service.resolve(identity,value,head));
 const admission=async route=>{const requestId=randomUUID(),attempt={source:'api',account_id:route.account.id,state:'rejected',usage_known:true,started:at,ended:at,input:0,output:0,cached:0,error_code:'upstream_quota_exhausted'};await store.run("INSERT INTO harbor.requests(id,member_id,key_id,account_id,session_id,route_version,started,status,ip,model,handoff_input,consumption_policy,consumption_policy_version,consumption_source,consumption_fallback,consumption_attempts) VALUES($1,$2,$3,$4,$5,$6,$7,'running','synthetic',$8,'replayable',$9,$10,'api',false,$11::jsonb)",requestId,member.id,member.key_id,route.account.id,route.session.id,route.session.route_version,at,payload.model,route.consumption.consumption_policy,route.consumption.consumption_policy_version,JSON.stringify([attempt]));return requestId;};
 const fallback=(route,requestId,patch={})=>store.transaction(()=>service.fallback({member,payload,headers,requestId,route,error:createApiQuotaRejection(),attempts:0,outputStarted:false,contextReplayable:true,...patch}));
 return {store,router,service,member,otherKey,policy,set,resolve,payload,headers,api,subscription,account,at,admission,fallback};
}

const configure=async(f,id,scope,weight)=>{const api=createPostgresApiAccountScope(f.store),saved=await api.read();return api.save(id,{api_usage_scope:scope,api_weight:weight,expected_api_scope_version:saved.version});};
test('explicit API uses configuration weight while raw row stays zero and inherit distribution is unchanged',options,async t=>{
 const f=await fixture(t);await f.store.run('UPDATE harbor.accounts SET api_weight=100 WHERE id=$1',f.api);
 const baseline=await f.router.readView({explanation:true}),extra=await f.account('api',{api_weight:0});await configure(f,extra,'explicit_api',100);
 const now=await f.router.readView({explanation:true});assert.deepEqual(now.weights,baseline.weights);assert.equal((await f.store.get('SELECT api_weight FROM harbor.accounts WHERE id=$1',extra)).api_weight,0);
 const source=await f.router.catalogCandidates({policy:'api_only'});assert.deepEqual(new Set(source.candidates.map(x=>x.account.id)),new Set([f.api,extra]));assert.equal(source.candidates.find(x=>x.account.id===extra).account.api_weight,100);
 await f.set('api_only');const seen=[];for(let n=0;n<8;n++)seen.push((await f.resolve(f.payload,{'thread-id':'scope-api-'+n})).account.id);assert.equal(seen.filter(x=>x===extra).length,4);
 await f.set('inherit');for(let n=0;n<5;n++)assert.equal((await f.resolve(f.payload,{'thread-id':'inherit-'+n,'x-harbor-api-scope':'explicit_api'})).account.id,f.api);
});
test('api-only naturally creates and resumes sessions, source change blocks ordinary and fixed inherit access',options,async t=>{
 const f=await fixture(t);await f.store.run('UPDATE harbor.accounts SET api_weight=0 WHERE id=$1',f.api);const extra=await f.account('api',{api_weight:0});await configure(f,extra,'explicit_api',100);await f.set('api_only');const route=await f.resolve();assert.equal(route.account.id,extra);assert.equal((await f.resolve()).session.id,route.session.id);
 await f.set('inherit');await assert.rejects(f.resolve(),{code:'api_account_scope_conflict'});await assert.rejects(f.resolve(f.payload,{...f.headers,'x-harbor-session':route.session.id}),{code:'api_account_scope_conflict'});
 await f.set('api_first');assert.equal((await f.resolve()).account.id,extra);
});
test('zero explicit weight remains manual only and shared zero fixed behavior stays available',options,async t=>{
 const f=await fixture(t);const extra=await f.account('api',{api_weight:0});await configure(f,extra,'explicit_api',0);await f.set('api_only');assert.equal((await f.resolve()).account.id,f.api);
 await f.store.run('UPDATE harbor.accounts SET api_weight=0 WHERE id=$1',f.api);await assert.rejects(f.resolve(f.payload,{'thread-id':'empty'}),{code:'no_account'});
 const session=randomUUID();await f.store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,$3,'responses',$4,$4)",session,f.member.id,extra,f.at);assert.equal((await f.resolve(f.payload,{'thread-id':'manual-explicit','x-harbor-session':session})).account.id,extra);
 await f.set('inherit');await assert.rejects(f.resolve(f.payload,{'thread-id':'manual-explicit','x-harbor-session':session}),{code:'api_account_scope_conflict'});
 await f.store.run('UPDATE harbor.sessions SET account_id=$1,client_key=NULL WHERE id=$2',f.api,session);assert.equal((await f.resolve(f.payload,{'thread-id':'manual-shared','x-harbor-session':session})).account.id,f.api);
});
test('weight pools, versions, subscription rejection and audit are atomic',options,async t=>{
 const f=await fixture(t),api=createPostgresApiAccountScope(f.store);const extra=await f.account('api',{api_weight:0});const first=await configure(f,extra,'explicit_api',80);assert.equal(first.stored_api_weight,0);assert.equal(first.api_scope_version,1);
 await assert.rejects(api.save(extra,{api_weight:50,expected_api_scope_version:0}),{code:'api_account_scope_conflict'});await configure(f,extra,'explicit_api',60);const audit=await f.store.all("SELECT action,detail FROM harbor.audit WHERE action IN ('account.api.scope','account.api.weight') ORDER BY at,id");assert.equal(audit.filter(x=>x.action==='account.api.scope').length,1);assert.equal(audit.filter(x=>x.action==='account.api.weight').length,1);
 const another=await f.account('api',{api_weight:0});await assert.rejects(configure(f,another,'explicit_api',50),{status:400});assert.equal((await api.read()).version,2);await assert.rejects(configure(f,f.subscription,'explicit_api',10),{code:'api_usage_scope_invalid'});
 await configure(f,extra,'shared',80);assert.equal((await f.store.get('SELECT api_weight FROM harbor.accounts WHERE id=$1',extra)).api_weight,80);assert.equal((await api.read()).accounts[extra],undefined);
});
test('dispatch scope rejects changed account epochs and changed policy before API IO',options,async t=>{
 const f=await fixture(t);await f.store.run('UPDATE harbor.accounts SET api_weight=0 WHERE id=$1',f.api);const extra=await f.account('api',{api_weight:0});await configure(f,extra,'explicit_api',50);await f.set('api_only');const route=await f.resolve(),api=createPostgresApiAccountScope(f.store);
 await f.store.transaction(()=>api.assertAccount(route.account,{policy:'api_only'}));await assert.rejects(f.store.transaction(()=>api.assertAccount(route.account,{policy:'inherit'})),{code:'api_account_scope_conflict'});
 await configure(f,extra,'explicit_api',40);await assert.rejects(f.store.transaction(()=>api.assertAccount(route.account,{policy:'api_only'})),{code:'api_account_scope_conflict'});
});
