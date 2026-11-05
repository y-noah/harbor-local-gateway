import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {once} from 'node:events';
import {setTimeout as delay} from 'node:timers/promises';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {hash} from '../store.mjs';
import {acceptsRequest} from '../network.mjs';
import {createRequestBodyBudget} from '../request-body-budget.mjs';
import {createPostgresExtraUsageAdmin} from '../postgres-extra-usage-admin.mjs';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresAccountExtraUsage} from '../postgres-account-extra-usage.mjs';

async function fixture(options={}){
 const auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-admin-password')}),headers={};auth.login('synthetic-admin-password',{setHeader:(key,value)=>headers[key]=value});const cookie=headers['Set-Cookie'].split(';')[0],calls=[],requestBodies=createRequestBodyBudget();
 const service={readPolicy:async()=>({enabled:false,version:0}),readAccount:async owner=>({account_id:owner,configuration:{mode:'inherit',budget:null,version:0},snapshot:null}),saveAccount:async(owner,payload,actor)=>{calls.push({owner,payload,actor});return {mode:payload.mode};},recordManual:async(owner,payload,actor)=>{calls.push({owner,payload,actor});return {state:'unverified'};},recordResetCardUsage:async()=>({state:'pending_verification'}),refresh:async()=>({refreshed:false}),...options.service};
 const controller=createPostgresExtraUsageAdmin({service,adminAuthenticated:auth.authenticated,adminSession:auth.sessionReference,acceptsAdminRequest:req=>acceptsRequest(req.headers,{origins:[]}),requestBodies,...options,service,reader:options.reader??service});
 const server=http.createServer((req,res)=>{void controller.handle(req,res).then(handled=>{if(!handled){res.writeHead(404);res.end();}},error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code}));});});server.listen(0,'127.0.0.1');await once(server,'listening');const origin='http://127.0.0.1:'+server.address().port;
 const call=async(path='',{method='GET',body,authenticated=true,headers={}}={})=>{const response=await fetch(origin+'/api/admin/'+path,{method,headers:{...(authenticated?{cookie}:{}),...(body!==undefined?{'content-type':'application/json'}:{}),...headers},...(body!==undefined?{body:JSON.stringify(body)}:{})});return {status:response.status,value:await response.json()};};
 return {controller,calls,cookie,auth,origin,call,requestBodies,close:async()=>{await controller.close();await new Promise(resolve=>server.close(resolve));}};
}

test('extra admin HTTP authenticates and checks origin before reading, and uses only the real session audit identity',async()=>{
 const f=await fixture();try{
  assert.equal(f.auth.sessionReference({headers:{cookie:'harbor_session=forged'}}),null);assert.equal(f.auth.sessionReference({headers:{cookie:f.cookie}}).session_ref,hash(f.cookie.slice('harbor_session='.length)));
  const path='accounts/synthetic-account/extra-usage';assert.equal((await f.call(path,{method:'PATCH',body:{mode:'deny',session_ref:'f'.repeat(64)},authenticated:false})).status,401);assert.equal((await f.call(path,{method:'PATCH',body:{mode:'deny'},headers:{origin:'https://cross-site.invalid'}})).status,403);assert.equal(f.calls.length,0);assert.equal(f.requestBodies.status().peakBufferedBytes,0);
  const result=await f.call(path,{method:'PATCH',body:{mode:'deny',expected_version:0,expected_identity_epoch:0}});assert.equal(result.status,200);assert.equal(f.calls.length,1);assert.deepEqual(f.calls[0].actor,f.auth.sessionReference({headers:{cookie:f.cookie}}));assert.ok(!JSON.stringify(result).includes(f.cookie));assert.equal(f.requestBodies.status().bufferedBytes,0);
  assert.equal((await f.call('extra-usage/policy',{method:'PATCH',body:{enabled:true,expected_version:0}})).value.code,'extra_policy_preview_required');assert.equal((await f.call(path+'?actor=forged')).status,400);
 }finally{await f.close();}
});

test('refresh jobs are singleflight, bounded and expire; unsupported reads do not claim verified credit',async()=>{
 let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);let reads=0,now=Date.now();const f=await fixture({clock:()=>now,refreshes:{maxConcurrent:1,maxQueued:0},maxJobs:1,jobTtlMs:60000,service:{refresh:async()=>{reads++;entered();await gate;return {refreshed:false};}}});try{
  const path='accounts/one/extra-usage/refresh',first=await f.call(path,{method:'POST',body:{}});assert.equal(first.status,202);await started;const second=await f.call(path,{method:'POST',body:{}});assert.equal(second.value.id,first.value.id);assert.equal(reads,1);assert.equal((await f.call('accounts/two/extra-usage/refresh',{method:'POST',body:{}})).status,503);
  release();let result;for(let until=Date.now()+2000;;){result=await f.call(path+'/'+first.value.id);if(result.value.finished_at!==null)break;assert.ok(Date.now()<until);await delay(5);}assert.equal(result.value.state,'unsupported');assert.equal(result.value.verification,'pending');assert.equal(result.value.reason,'official_entitlement_not_supported');assert.equal((await f.call('accounts/other/extra-usage/refresh/'+first.value.id)).status,404);now+=60001;assert.equal((await f.call(path+'/'+first.value.id)).status,404);
 }finally{release?.();await f.close();}
});

test('closing cancels refresh but retains its execution slot until the actual adapter promise settles',async()=>{
 let release,entered,signal;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);const f=await fixture({refreshSupported:true,service:{refresh:async(_owner,options)=>{signal=options.signal;entered();await gate;return {refreshed:true,state:'verified'};}}});try{
  await f.call('accounts/one/extra-usage/refresh',{method:'POST',body:{}});await started;let finished=false;const closing=f.controller.close().then(()=>finished=true);await delay(10);assert.equal(signal.aborted,true);assert.equal(f.controller.status().refresh.active,1);assert.equal(finished,false);release();await closing;assert.equal(f.controller.status().refresh.active,0);assert.equal(finished,true);
 }finally{release?.();await f.close();}
});

test('a verified refresh committed at the cancellation boundary keeps the real committed result',async()=>{
 const f=await fixture({refreshSupported:true,refreshTimeoutMs:100,service:{refresh:async(_owner,{signal})=>{await new Promise(resolve=>signal.addEventListener('abort',resolve,{once:true}));return {refreshed:true,state:'verified'};}}});try{
  const path='accounts/one/extra-usage/refresh',first=await f.call(path,{method:'POST',body:{}});for(let until=Date.now()+2000;;){const result=await f.call(path+'/'+first.value.id);if(result.value.finished_at!==null){assert.equal(result.value.state,'verified');assert.equal(result.value.verification,'verified');break;}assert.ok(Date.now()<until);await delay(5);}
 }finally{await f.close();}
});

test('close retains accepted writes until service settlement and does not admit another mutation',async()=>{
 let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);let writes=0;const f=await fixture({service:{saveAccount:async()=>{writes++;entered();await gate;return {mode:'deny'};}}});try{
  const path='accounts/one/extra-usage',request=f.call(path,{method:'PATCH',body:{mode:'deny'}});await started;let finished=false;const closing=f.controller.close().then(()=>finished=true);await delay(10);assert.equal(finished,false);assert.equal(f.controller.status().requests,1);assert.equal((await f.call(path,{method:'PATCH',body:{mode:'inherit'}})).status,503);assert.equal(writes,1);release();assert.equal((await request).status,200);await closing;assert.equal(f.requestBodies.status().bufferedBytes,0);assert.equal(f.controller.status().requests,0);
 }finally{release?.();await f.close();}
});

test('close waits for body cleanup and rejects a mutation whose body had not completed',async()=>{
 let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);let writes=0,released=false;const f=await fixture({requestBodies:{read:async()=>{entered();await gate;return {mode:'deny'};},release:()=>released=true},service:{saveAccount:async()=>{writes++;return {};}}});try{
  const request=f.call('accounts/one/extra-usage',{method:'PATCH',body:{mode:'deny'}});await started;let finished=false;const closing=f.controller.close().then(()=>finished=true);await delay(10);assert.equal(finished,false);assert.equal(released,false);release();assert.equal((await request).status,503);await closing;assert.equal(writes,0);assert.equal(released,true);assert.equal(finished,true);
 }finally{release?.();await f.close();}
});

test('reset-card refresh keeps subscription and extra evidence separate and reads the new epoch in dependency order',async()=>{
 let epoch=0,quotaCalls=0,extraCalls=0;const f=await fixture({refreshSupported:true,refreshSubscriptionQuota:async owner=>{assert.equal(owner,'one');quotaCalls++;epoch++;return {refreshed:true};},service:{refresh:async()=>{extraCalls++;assert.equal(epoch,1);return {refreshed:true,state:'verified'};}}});try{
  const result=await f.call('accounts/one/extra-usage/reset-card-used',{method:'POST',body:{}});assert.equal(result.status,200);assert.equal(result.value.state,'pending_verification');for(let until=Date.now()+2000;;){const job=(await f.call('accounts/one/extra-usage/refresh/'+result.value.refresh.id)).value;if(job.finished_at!==null){assert.equal(job.state,'completed');assert.equal(job.components.subscription.state,'refreshed');assert.equal(job.components.subscription.verification,'snapshot_refreshed');assert.equal(job.components.extra.state,'verified');assert.equal(job.reset_card_verification,'pending');break;}assert.ok(Date.now()<until);await delay(5);}assert.equal(quotaCalls,1);assert.equal(extraCalls,1);
 }finally{await f.close();}
});

test('a confirmed subscription result is retained when extra entitlement is unsupported or later cancelled',async()=>{
 let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);const f=await fixture({refreshSubscriptionQuota:async()=>({refreshed:true}),service:{refresh:async()=>({refreshed:false})}});try{
  const result=await f.call('accounts/one/extra-usage/refresh',{method:'POST',body:{}});for(let until=Date.now()+2000;;){const job=(await f.call('accounts/one/extra-usage/refresh/'+result.value.id)).value;if(job.finished_at!==null){assert.equal(job.state,'completed');assert.equal(job.verification,'pending');assert.equal(job.components.subscription.state,'refreshed');assert.equal(job.components.extra.state,'unsupported');assert.equal(job.reset_card_verification,'pending');break;}assert.ok(Date.now()<until);await delay(5);}
 }finally{await f.close();}
 const g=await fixture({refreshSupported:true,refreshTimeoutMs:100,refreshSubscriptionQuota:async()=>({refreshed:true}),service:{refresh:async(_owner,{signal})=>{signal.addEventListener('abort',entered,{once:true});if(signal.aborted)entered();await gate;return {refreshed:false,reason:'extra_refresh_cancelled'};}}});try{const result=await g.call('accounts/one/extra-usage/refresh',{method:'POST',body:{}});await started;assert.equal(g.controller.status().refresh.active,1);release();for(let until=Date.now()+2000;;){const job=(await g.call('accounts/one/extra-usage/refresh/'+result.value.id)).value;if(job.finished_at!==null){assert.equal(job.state,'completed');assert.equal(job.components.subscription.state,'refreshed');assert.equal(job.components.extra.state,'cancelled');break;}assert.ok(Date.now()<until);await delay(5);}}finally{release();await g.close();}
});

test('real PostgreSQL HTTP keeps manual records unverified, audits the authenticated session and records reset-card use once',{
 skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需要隔离PG18；不访问线上':false,timeout:60000
},async()=>{
 let lab,store,queryStore,f;try{
  lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});store=new PostgresStore({connection:lab.connection});queryStore=new PostgresStore({connection:lab.connection});await store.migrate();const owner=randomUUID();await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES($1,'合成HTTP权益账号','synthetic-extra-http','ready')",owner);
  const service=createPostgresAccountExtraUsage(store),reader=createPostgresAccountExtraUsage(queryStore);f=await fixture({service,reader});const path='accounts/'+owner+'/extra-usage',budget={basis:'reference_estimate',unit:'reference_usd_atoms',limit_atoms:'10000000000000',request_reserve_atoms:'1000000000000'};
  const initial=await f.call(path);assert.equal(initial.value.configuration.version,0);assert.equal((await f.call('extra-usage/policy')).value.enabled,false);
  let result=await f.call(path,{method:'PATCH',body:{mode:'inherit',budget,expected_version:0,expected_identity_epoch:0}});assert.equal(result.status,200);assert.equal(result.value.version,1);
  result=await f.call(path+'/manual',{method:'POST',body:{remaining_atoms:'100',unit:'credits',models:['gpt-6.1-sol'],protocols:['responses'],expires_at:Date.now()+3600000,reset_cards:{remaining:2,expires_at:null,windows:['weekly']},note:'由管理员核对官方页面后登记，等待可信核验',expected_version:1,expected_identity_epoch:0}});assert.equal(result.status,200);assert.equal(result.value.state,'unverified');assert.equal((await f.call(path)).value.manual_record.source,'manual');assert.equal((await f.call(path)).value.snapshot,null);
  const reset={idempotency_key:randomUUID(),windows:['weekly'],count:1,note:'管理员在上游手动使用，尚待额度核验',expected_version:2,expected_identity_epoch:0};const recorded=await f.call(path+'/reset-card-used',{method:'POST',body:reset});assert.equal(recorded.status,200);assert.equal(recorded.value.state,'pending_verification');assert.equal((await f.call(path+'/reset-card-used',{method:'POST',body:reset})).value.replayed,true);
  for(let until=Date.now()+3000;;){const job=await f.call(path+'/refresh/'+recorded.value.refresh.id);if(job.value.finished_at!==null){assert.equal(job.value.state,'unsupported');assert.equal(job.value.verification,'pending');break;}assert.ok(Date.now()<until);await delay(5);}
  assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.account_extra_records WHERE account_id=$1 AND kind='reset_card_used'",owner)).n,1);const detail=(await f.call(path)).value;assert.equal(detail.configuration.version,3);assert.equal(detail.manual_record.reset_cards.remaining,2);assert.equal(detail.snapshot.last_refresh_state,'failed');assert.equal(detail.snapshot.entitlement,null);
  const audit=await store.all("SELECT detail FROM harbor.audit WHERE action IN ('account.extra.configure','account.extra.manual','account.extra.reset_card_record') ORDER BY at,id");assert.equal(audit.length,3);assert.ok(audit.every(row=>JSON.parse(row.detail).admin_session_ref===f.auth.sessionReference({headers:{cookie:f.cookie}}).session_ref));assert.ok(audit.every(row=>!row.detail.includes(f.cookie)));
  assert.equal((await f.call(path,{method:'PATCH',body:{mode:'deny',budget,expected_version:3,expected_identity_epoch:0,session_ref:'f'.repeat(64)}})).status,400);assert.equal((await f.call(path)).value.configuration.version,3);
 }finally{await f?.close();await queryStore?.close();await store?.close();await lab?.close();}
});
