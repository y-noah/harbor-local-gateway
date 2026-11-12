import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {once} from 'node:events';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy,createPostgresConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {isTrustedApiQuotaRejection} from '../consumption-policy.mjs';
import {createRequestStreamTiming} from '../request-stream-timing.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,real={skip:!url?'需独占私有PG':false,timeout:30000};
function options(database='postgres'){const target=new URL(url);target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,poolSize:3,statementTimeoutMs:5000};}
async function fixture(t){
 const admin=new PostgresStore(options()),database='harbor_settle_'+randomUUID().replaceAll('-','');await admin.query('CREATE DATABASE '+database);
 const store=new PostgresStore(options(database));t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+database);await admin.query('DROP ROLE IF EXISTS '+database+'_runtime');await admin.close();});
 await store.migrate();await store.initializePricing();await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});
 await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0);INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('k','m','synthetic','synthetic',0);INSERT INTO harbor.accounts(id,name,home,auth_mode) VALUES('api','synthetic','synthetic','openai_api'),('subscription','synthetic','synthetic','subscription')");
 const policy=createPostgresConsumptionPolicy(store);await policy.set('k',{consumption_policy:'api_first',expected_consumption_policy_version:0});
 async function request(id='request',account='subscription'){
  await store.run("INSERT INTO harbor.requests(id,member_id,key_id,account_id,started,status,ip,reserved) VALUES($1,'m','k',$2,10,'running','synthetic',37)",id,account);
  await store.transaction(()=>policy.recordAdmission(id,'k',{consumption_policy:'api_first',consumption_policy_version:1}));return id;
 }
 const billing=()=>store.get("SELECT m.used member_used,k.used key_used,(SELECT budget_used FROM harbor.accounts WHERE id='api') api_used,(SELECT budget_used FROM harbor.accounts WHERE id='subscription') subscription_used FROM harbor.members m JOIN harbor.keys k ON k.member_id=m.id WHERE k.id='k'");
 return {store,policy,request,billing};
}
const quota=()=>({source:'api',account_id:'api',state:'rejected',usage_known:true,started:10,ended:20,error_code:'upstream_quota_exhausted',input:0,output:0,cached:0});
const completed=()=>({source:'subscription',account_id:'subscription',state:'completed',usage_known:true,started:21,ended:30,input:11,output:7,cached:3});
const unsent=()=>({source:'subscription',account_id:'subscription',state:'failed',usage_known:true,dispatched:false,started:21,ended:30,error_code:'upstream_credentials_unavailable',input:0,output:0,cached:0});
async function transportFixture(t){
 let requests=0;const server=http.createServer(async(req,res)=>{for await(const _ of req){}requests++;if(req.url==='/quota'){res.writeHead(429,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:'insufficient_quota'}}));}else{res.writeHead(200,{'content-type':'text/event-stream'});res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-response',model:'synthetic-model',output:[],usage:{input_tokens:11,output_tokens:7,input_tokens_details:{cached_tokens:3}}}})+'\n\n');}});
 server.listen(0,'127.0.0.1');await once(server,'listening');t.after(()=>new Promise(resolve=>{server.closeAllConnections();server.close(resolve);}));
 const transport=(path,extra={})=>createResponsesTransport({apiKeyMode:path==='/quota',version:async()=>'0.160.1',credentials:async()=>({token:'synthetic-only',accountId:'synthetic-only'}),fetchImpl:(_,options)=>fetch('http://127.0.0.1:'+server.address().port+path,options),...extra});
 const run=(client,onDispatch)=>client.run({binary:'synthetic',home:'synthetic',payload:{model:'synthetic-model',input:'synthetic',stream:true},onDispatch});return {transport,run,requests:()=>requests};
}

test('two actual HTTP dispatches settle attempt evidence and billing once across duplicate finish',real,async t=>{
 const f=await fixture(t),wire=await transportFixture(t);await f.request();let sent=0;
 await assert.rejects(wire.run(wire.transport('/quota'),()=>sent++),error=>isTrustedApiQuotaRejection(error));await wire.run(wire.transport('/success'),()=>sent++);assert.equal(sent,2);assert.equal(wire.requests(),2);
 const attempts=[quota(),completed()],result={status:'ok',input:11,output:7,cached:3,consumptionAttempts:attempts};assert.equal((await f.store.finish('request',result,50)).settled,true);
 const before=await f.billing(),price=await f.store.get("SELECT * FROM harbor.request_prices WHERE request_id='request'");assert.deepEqual(before,{member_used:18,key_used:18,api_used:0,subscription_used:18});
 const row=await f.store.get("SELECT status,consumption_source,consumption_fallback,consumption_attempts FROM harbor.requests WHERE id='request'");assert.deepEqual(row,{status:'ok',consumption_source:'subscription',consumption_fallback:true,consumption_attempts:attempts});
 assert.equal((await f.store.finish('request',result,99)).settled,false);assert.deepEqual(await f.billing(),before);assert.deepEqual(await f.store.get("SELECT * FROM harbor.request_prices WHERE request_id='request'"),price);
});
test('real API refusal and subscription credential failure retain two attempts with only API sent',real,async t=>{
 const f=await fixture(t),wire=await transportFixture(t);await f.request();let apiSent=0,subscriptionSent=0;
 await assert.rejects(wire.run(wire.transport('/quota'),()=>apiSent++),error=>isTrustedApiQuotaRejection(error));
 await assert.rejects(wire.run(wire.transport('/success',{credentials:async()=>{throw Error('synthetic credentials unavailable');}}),()=>subscriptionSent++),{code:'upstream_connection',usageKnown:true});assert.equal(apiSent,1);assert.equal(subscriptionSent,0);assert.equal(wire.requests(),1);
 const attempts=[quota(),unsent()];await f.store.finish('request',{status:'failed',input:0,output:0,cached:0,usageKnown:true,consumptionAttempts:attempts},50);
 assert.deepEqual(await f.store.get("SELECT consumption_source,consumption_fallback,consumption_attempts FROM harbor.requests WHERE id='request'"),{consumption_source:'api',consumption_fallback:false,consumption_attempts:attempts});assert.deepEqual(await f.billing(),{member_used:0,key_used:0,api_used:0,subscription_used:0});
});
test('unknown sent usage stays unknown and cannot be fabricated as known-zero fields',real,async t=>{
 const f=await fixture(t);await f.request('request','api');const unknown={source:'api',account_id:'api',state:'unknown',usage_known:false,started:10,ended:20};
 await assert.rejects(f.store.finish('request',{status:'unknown',usageKnown:false,consumptionAttempts:[{...unknown,input:0}]},50),{code:'consumption_metadata_invalid'});
 await assert.rejects(f.store.run("UPDATE harbor.requests SET consumption_source='api',consumption_attempts=$1::jsonb WHERE id='request'",JSON.stringify([{...unknown,input:0}])),{code:'23514'});
 await f.store.finish('request',{status:'unknown',usageKnown:false,consumptionAttempts:[unknown]},50);const row=await f.store.get("SELECT usage_unknown,reserved,consumption_attempts FROM harbor.requests WHERE id='request'");assert.equal(row.usage_unknown,1);assert.equal(row.reserved,37);assert.deepEqual(row.consumption_attempts,[unknown]);assert.equal(Object.hasOwn(row.consumption_attempts[0],'input'),false);
});
test('last-account mismatch or invalid terminal status rolls back metadata and billing together',real,async t=>{
 const f=await fixture(t);await f.request();const baseline=await f.store.get("SELECT * FROM harbor.requests WHERE id='request'");
 await assert.rejects(f.store.finish('request',{input:9,consumptionAttempts:[{...completed(),account_id:'api'}]},50),{code:'consumption_metadata_conflict'});
 assert.deepEqual(await f.store.get("SELECT * FROM harbor.requests WHERE id='request'"),baseline);
 await assert.rejects(f.store.finish('request',{status:'invalid-terminal',consumptionAttempts:[quota(),completed()]},50),{status:400});assert.deepEqual(await f.store.get("SELECT * FROM harbor.requests WHERE id='request'"),baseline);assert.deepEqual(await f.billing(),{member_used:0,key_used:0,api_used:0,subscription_used:0});assert.equal((await f.store.get('SELECT count(*) n FROM harbor.request_prices')).n,0);
});
test('sent evidence cannot become unsent; SQL independently rejects fabricated unsent completion',real,async t=>{
 const f=await fixture(t);await f.request('request','api');const attempt={source:'api',account_id:'api',state:'failed',usage_known:true,started:10,ended:20,input:0,output:0,cached:0};await f.policy.recordAttempts('request','k',[attempt]);
 await assert.rejects(f.policy.recordAttempts('request','k',[{...attempt,dispatched:false}]),{code:'consumption_metadata_conflict'});
 for(const invalid of [{...attempt,dispatched:false,state:'completed'},{...attempt,dispatched:false,usage_known:false},{...attempt,dispatched:false,input:1}]){
  await assert.rejects(f.store.finish('request',{consumptionAttempts:[invalid]},50),{code:'consumption_metadata_invalid'});
  await assert.rejects(f.store.run("UPDATE harbor.requests SET consumption_source=NULL,consumption_attempts=$1::jsonb WHERE id='request'",JSON.stringify([invalid])),{code:'23514'});
 }
 assert.deepEqual((await f.store.get("SELECT consumption_attempts FROM harbor.requests WHERE id='request'")).consumption_attempts,[attempt]);
});
test('late stream timing enriches settled history while legacy omitted attempts and replay remain idempotent',real,async t=>{
 const f=await fixture(t);await f.request();const result={input:11,output:7,cached:3,consumptionAttempts:[quota(),completed()]};await f.store.finish('request',result,50);const before=await f.billing();
 const timing=createRequestStreamTiming();timing.bodyEnd();timing.dispatch();timing.headers();timing.transportEnd();timing.responseEnd();const summary=timing.finish({outcome:'ok'});
 assert.equal((await f.store.finish('request',{...result,streamTiming:summary},80)).settled,false);assert.equal((await f.store.finish('request',{streamTiming:summary},90)).settled,false);assert.deepEqual(await f.billing(),before);
 const row=await f.store.get("SELECT ended,phase_history,consumption_attempts FROM harbor.requests WHERE id='request'");assert.equal(row.ended,50);assert.equal(JSON.parse(row.phase_history).length,1);assert.deepEqual(JSON.parse(row.phase_history)[0].stream_timing,summary);assert.deepEqual(row.consumption_attempts,result.consumptionAttempts);
 await f.store.run("INSERT INTO harbor.requests(id,member_id,key_id,account_id,started,status,ip) VALUES('legacy','m','k','api',10,'running','synthetic')");await f.store.finish('legacy',{input:2,output:1},100);assert.deepEqual(await f.store.get("SELECT consumption_policy,consumption_attempts FROM harbor.requests WHERE id='legacy'"),{consumption_policy:null,consumption_attempts:null});assert.equal((await f.billing()).member_used,21);
});
