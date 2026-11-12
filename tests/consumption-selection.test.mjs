import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {readdir,readFile,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy,createPostgresConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {canonicalConsumptionSelection} from '../consumption-selection.mjs';
const first=()=>({source:'api',account_id:'api',state:'rejected',usage_known:true,started:10,ended:20,error_code:'upstream_quota_exhausted',input:0,output:0,cached:0});
const selection=()=>({version:1,request_id:'request',member_id:'m',key_id:'k',session_id:'s',consumption_policy:'api_first',consumption_policy_version:1,from:{account_id:'api',route_version:4},to:{account_id:'subscription',route_version:5},prepared_at:21,failed_at:30,error_code:'consumption_rebind_commit_unconfirmed',first_attempt:first()});
const result=()=>({status:'failed',input:0,output:0,cached:0,usageKnown:true,consumptionAttempts:[first()],consumptionSelection:selection()});
const second=()=>({source:'subscription',account_id:'subscription',state:'failed',dispatched:false,usage_known:true,input:0,output:0,cached:0,started:21,ended:30,error_code:'consumption_rebind_commit_unconfirmed'});
const url=process.env.HARBOR_TEST_DATABASE_URL,real={skip:!url?'需独占私有PG':false,timeout:30000};
function options(name='postgres'){const target=new URL(url);target.pathname='/'+name;return {connectionString:target.href,allowPlaintextLocalNetwork:true,poolSize:2,connectionTimeoutMs:200,statementTimeoutMs:2000};}
async function populate(store){
 await store.migrate();await store.initializePricing();await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});
 await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0);INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('k','m','synthetic','synthetic',0);INSERT INTO harbor.accounts(id,name,home,auth_mode) VALUES('api','synthetic','synthetic','openai_api'),('subscription','synthetic','synthetic','subscription'),('later','synthetic','synthetic','subscription');INSERT INTO harbor.sessions(id,member_id,account_id,protocol,route_version,created,updated) VALUES('s','m','api','responses',4,0,0);INSERT INTO harbor.requests(id,member_id,key_id,session_id,account_id,route_version,started,status,ip,reserved) VALUES('request','m','k','s','api',4,10,'running','synthetic',37)");
 const policy=createPostgresConsumptionPolicy(store);await policy.set('k',{consumption_policy:'api_first',expected_consumption_policy_version:0});await store.transaction(()=>policy.recordAdmission('request','k',{consumption_policy:'api_first',consumption_policy_version:1}));await policy.recordAttempts('request','k',[first()]);return policy;
}
async function fixture(t){
 const admin=new PostgresStore(options()),name='harbor_selection_'+randomUUID().replaceAll('-','');await admin.query('CREATE DATABASE '+name);const store=new PostgresStore(options(name));t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();});return {store,policy:await populate(store)};
}
async function rebind(store){await store.run("UPDATE harbor.requests SET account_id='subscription',route_version=5 WHERE id='request'");await store.run("UPDATE harbor.sessions SET account_id='subscription',route_version=5 WHERE id='s'");}
const billing=store=>store.all("SELECT 'member' kind,used FROM harbor.members WHERE id='m' UNION ALL SELECT 'key',used FROM harbor.keys WHERE id='k' UNION ALL SELECT id,budget_used FROM harbor.accounts ORDER BY kind");
async function unchanged(store,run){const before=await store.get("SELECT * FROM harbor.requests WHERE id='request'"),usage=await billing(store),prices=await store.all('SELECT * FROM harbor.request_prices');await run();assert.deepEqual(await store.get("SELECT * FROM harbor.requests WHERE id='request'"),before);assert.deepEqual(await billing(store),usage);assert.deepEqual(await store.all('SELECT * FROM harbor.request_prices'),prices);}

test('selection canonicalizes only fixed structured facts, default sent true and an independent <=2KiB snapshot',()=>{
 assert.equal(canonicalConsumptionSelection(undefined),undefined);const input=selection(),canonical=canonicalConsumptionSelection(input,[first()]);assert.deepEqual(canonical,input);assert.ok(Buffer.byteLength(JSON.stringify(canonical))<=2048);input.from.account_id='tampered';input.first_attempt.input=5;assert.equal(canonical.from.account_id,'api');assert.equal(canonical.first_attempt.input,0);
 assert.deepEqual(canonicalConsumptionSelection({...selection(),first_attempt:{...first(),dispatched:true}},[{...first(),dispatched:true}]),selection());
});
test('selection rejects extra fields, getters, symbols, oversized IDs and invalid tuples/times/evidence',()=>{
 const malformed=[null,{}, {...selection(),headers:{authorization:'never-persist'}},{...selection(),version:2},{...selection(),request_id:'x'.repeat(81)},{...selection(),member_id:'m\n'},{...selection(),consumption_policy:'inherit'},{...selection(),consumption_policy_version:-1},{...selection(),from:{account_id:'api',route_version:4,extra:true}},{...selection(),to:{account_id:'api',route_version:5}},{...selection(),to:{account_id:'subscription',route_version:6}},{...selection(),from:{account_id:'api',route_version:Number.MAX_SAFE_INTEGER},to:{account_id:'subscription',route_version:Number.MAX_SAFE_INTEGER+1}},{...selection(),prepared_at:19},{...selection(),failed_at:20},{...selection(),error_code:'other'},{...selection(),first_attempt:{...first(),dispatched:false}},{...selection(),first_attempt:{...first(),input:1}},{...selection(),first_attempt:{...first(),usage_known:false}},{...selection(),first_attempt:{...first(),body:'private'}},{...selection(),first_attempt:{...first(),ended:undefined}}];
 for(const value of malformed)assert.throws(()=>canonicalConsumptionSelection(value,[first()]),{code:'consumption_selection_invalid'});
 const accessor=selection();let read=0;Object.defineProperty(accessor,'prepared_at',{enumerable:true,get(){read++;return 21;}});assert.throws(()=>canonicalConsumptionSelection(accessor,[first()]),{code:'consumption_selection_invalid'});assert.equal(read,0);
 const symbol=selection();symbol[Symbol('private')]='never-persist';assert.throws(()=>canonicalConsumptionSelection(symbol,[first()]),{code:'consumption_selection_invalid'});
 for(const attempts of [undefined,[],[first(),second()],[{...first(),ended:19}]])assert.throws(()=>canonicalConsumptionSelection(selection(),attempts),{code:'consumption_selection_invalid'});
});
test('real rollback leaves from tuple and settles only durable API evidence, with idempotent retries',real,async t=>{
 const f=await fixture(t);await assert.rejects(f.store.transaction(async()=>{await rebind(f.store);throw Error('synthetic rollback receipt');}),/rollback receipt/);
 assert.equal((await f.store.finish('request',result(),50)).settled,true);const row=await f.store.get("SELECT * FROM harbor.requests WHERE id='request'");assert.equal(row.account_id,'api');assert.equal(row.route_version,4);assert.deepEqual(row.consumption_attempts,[first()]);assert.equal(row.consumption_source,'api');assert.equal(row.consumption_fallback,false);assert.equal(row.reserved,0);await unchanged(f.store,async()=>assert.equal((await f.store.finish('request',result(),60)).settled,false));
});
test('real committed rebind with discarded receipt settles to tuple without touching a later session route',real,async t=>{
 const f=await fixture(t);await assert.rejects(f.store.transaction(()=>rebind(f.store)).then(()=>{throw Error('synthetic COMMIT receipt lost');}),/COMMIT receipt lost/);
 await f.store.run("UPDATE harbor.sessions SET account_id='later',route_version=99 WHERE id='s'");const session=await f.store.get("SELECT * FROM harbor.sessions WHERE id='s'");
 assert.equal((await f.store.finish('request',result(),50)).settled,true);const row=await f.store.get("SELECT * FROM harbor.requests WHERE id='request'");assert.equal(row.account_id,'subscription');assert.equal(row.route_version,5);assert.deepEqual(row.consumption_attempts,[first(),second()]);assert.equal(row.consumption_source,'api');assert.equal(row.consumption_fallback,false);assert.equal(row.usage_unknown,0);assert.deepEqual(await f.store.get("SELECT * FROM harbor.sessions WHERE id='s'"),session);assert.ok((await billing(f.store)).every(row=>row.used===0));await unchanged(f.store,async()=>assert.equal((await f.store.finish('request',result(),60)).settled,false));
});
test('identity, declaration and durable first evidence mismatch reject the complete settlement transaction',real,async t=>{
 const f=await fixture(t);for(const patch of [{request_id:'different'},{member_id:'different'},{key_id:'different'},{session_id:'different'},{consumption_policy_version:2},{from:{account_id:'api',route_version:7},to:{account_id:'subscription',route_version:8}}])await unchanged(f.store,()=>assert.rejects(f.store.finish('request',{...result(),consumptionSelection:{...selection(),...patch}},50),{code:'consumption_selection_conflict'}));
 const altered={...first(),ended:19};await unchanged(f.store,()=>assert.rejects(f.store.finish('request',{...result(),consumptionAttempts:[altered],consumptionSelection:{...selection(),first_attempt:altered}},50),{code:'consumption_selection_conflict'}));
 await unchanged(f.store,()=>assert.rejects(f.store.finish('request',{...result(),input:1},50),{code:'consumption_selection_invalid'}));
 await f.store.run("UPDATE harbor.requests SET account_id='later',route_version=6 WHERE id='request'");await unchanged(f.store,()=>assert.rejects(f.store.finish('request',result(),50),{code:'consumption_selection_conflict'}));
});
test('real dispatched second attempts and existing known billing cannot be rewritten by selection recovery',real,async t=>{
 const f=await fixture(t);await rebind(f.store);const actual={source:'subscription',account_id:'subscription',state:'completed',usage_known:true,input:11,output:7,cached:3,started:21,ended:30};await f.policy.recordAttempts('request','k',[first(),actual]);await unchanged(f.store,()=>assert.rejects(f.store.finish('request',result(),50),{code:'consumption_selection_conflict'}));
 await f.store.finish('request',{status:'ok',input:11,output:7,cached:3,consumptionAttempts:[first(),actual]},50);await unchanged(f.store,()=>assert.rejects(f.store.finish('request',result(),60),{code:'consumption_selection_conflict'}));
});
test('known request usage, manual cost, or forged no-send evidence is preserved instead of downgraded',real,async t=>{
 const f=await fixture(t);await f.store.run("UPDATE harbor.requests SET input=1 WHERE id='request'");await unchanged(f.store,()=>assert.rejects(f.store.finish('request',result(),50),{code:'consumption_selection_conflict'}));await f.store.run("UPDATE harbor.requests SET input=0,manual_cost_atoms='7' WHERE id='request'");await unchanged(f.store,()=>assert.rejects(f.store.finish('request',result(),50),{code:'consumption_selection_conflict'}));
 await f.store.run("UPDATE harbor.requests SET manual_cost_atoms=NULL WHERE id='request'");await rebind(f.store);await f.policy.recordAttempts('request','k',[first(),{...second(),error_code:'different_failure'}]);await unchanged(f.store,()=>assert.rejects(f.store.finish('request',result(),50),{code:'consumption_selection_conflict'}));
});

const bin=process.env.HARBOR_CAPACITY_PG_BIN,lib=process.env.HARBOR_CAPACITY_PG_LIB;
test('unreadable owned PG preserves selection in real encrypted spool; restart resolves committed tuple and never rebills',{skip:!bin?'需独占PG18工具链':false,timeout:60000},async t=>{
 const lab=await createPrivatePostgres({binDirectory:bin,libraryDirectory:lib});let store=new PostgresStore({connection:lab.connection,poolSize:2,connectionTimeoutMs:200,statementTimeoutMs:1000}),recovery;const key=randomBytes(32);
 t.after(async()=>{await lab.proxy.restore();await recovery?.close();await store.close();await lab.close();key.fill(0);});await populate(store);
 await assert.rejects(store.transaction(()=>rebind(store)).then(()=>{throw Error('synthetic committed receipt lost');}),/committed receipt lost/);await store.run("UPDATE harbor.sessions SET account_id='later',route_version=99 WHERE id='s'");
 const directory=join(lab.directory,'selection-spool');recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory,flushMs:10000});
 for(const bad of [{...selection(),raw:'never-enqueue'},{...selection(),first_attempt:{...first(),headers:{authorization:'never-enqueue'}}}]){assert.throws(()=>recovery.settle('request',{...result(),consumptionSelection:bad},50),{code:'consumption_selection_invalid'});assert.equal(recovery.status().queued,0);assert.deepEqual(await readdir(directory),[]);}
 await lab.proxy.disconnect();await assert.rejects(store.get('SELECT 1'));
 const command=result();assert.equal(recovery.settle('request',command,50),true);command.consumptionSelection.to.account_id='mutated-after-enqueue';await recovery.flush();assert.equal(recovery.status().files,1);const name=(await readdir(directory))[0],bytes=await readFile(join(directory,name));assert.equal(bytes.includes(Buffer.from('consumptionSelection')),false);assert.equal(bytes.includes(Buffer.from('consumption_rebind_commit_unconfirmed')),false);const opened=recovery.buffer.repository.open(bytes,name);assert.deepEqual(opened.result.consumptionSelection,selection());assert.deepEqual(opened.result.consumptionAttempts,[first()]);
 await recovery.close();await store.close();await lab.proxy.restore();store=new PostgresStore({connection:lab.connection,poolSize:2,connectionTimeoutMs:200,statementTimeoutMs:1000});recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory,flushMs:10000});await recovery.flush();assert.equal(recovery.status().files,0);assert.equal(recovery.status().queued,0);assert.equal(recovery.status().orphanFiles,0);assert.deepEqual((await store.get("SELECT consumption_attempts FROM harbor.requests WHERE id='request'")).consumption_attempts,[first(),second()]);assert.equal((await store.get("SELECT route_version FROM harbor.sessions WHERE id='s'")).route_version,99);
 const before=await billing(store),prices=await store.all('SELECT * FROM harbor.request_prices');await recovery.close();await writeFile(join(directory,name),bytes,{mode:0o600,flag:'wx'});recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory,flushMs:10000});await recovery.flush();assert.equal(recovery.status().files,0);assert.deepEqual(await billing(store),before);assert.deepEqual(await store.all('SELECT * FROM harbor.request_prices'),prices);
});
