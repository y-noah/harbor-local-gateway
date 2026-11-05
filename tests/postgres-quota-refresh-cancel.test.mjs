import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresAccountQuotas} from '../postgres-account-quotas.mjs';
import {createPostgresAccountAdmin} from '../postgres-account-admin.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';

const real={skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需要私有PG18，不访问真实上游':false,timeout:60000};
const deferred=()=>{let resolve;const promise=new Promise(yes=>resolve=yes);return {promise,resolve};};
const quota=at=>({rateLimits:{planType:'synthetic-plan',primary:{usedPercent:25,windowDurationMins:10080,resetsAt:(at+86400000)/1000}}});
test('subscription quota cancellation preserves actual settlement, singleflight and historical snapshots',real,async t=>{
 let lab,store;const resources=[];try{
  lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});store=new PostgresStore({connection:lab.connection});await store.migrate();await store.initializePricing();const now=Date.now(),router=createPostgresRouter(store,{clock:()=>now});
  const owner=async({kind='codex',auth_mode='subscription',blocked=false}={})=>{const id=randomUUID();await store.run("INSERT INTO harbor.accounts(id,name,home,state,kind,auth_mode,quota_json,route_block) VALUES($1,'合成取消额度账号','synthetic-cancel-quota','ready',$2,$3,$4,$5)",id,kind,auth_mode,JSON.stringify({plan:'synthetic-old',remaining:5,checked:now-10000,windows:[{used:95,minutes:10080,reset:now+86400000}],limitReached:false}),blocked?'quota_exhausted':null);return store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);};
  const reader=(readQuota,options={})=>{const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:2}),quotas=createPostgresAccountQuotas(store,{router,queue,clock:()=>now,readQuota,...options});resources.push(async()=>{quotas.stop();await queue.close();await quotas.close();});return {queue,quotas};};
  await t.test('pre-abort and cancellation while queued cannot begin an upstream read or write attempt metadata',async()=>{
   const account=await owner();let calls=0;const {queue,quotas}=reader(async()=>{calls++;return quota(now);}),pre=new AbortController();pre.abort();await assert.rejects(quotas.refresh(account,{force:true,signal:pre.signal}));assert.equal(calls,0);const gate=deferred(),block=queue.run(()=>gate.promise),cancel=new AbortController(),pending=quotas.refresh(account,{force:true,signal:cancel.signal});assert.equal(queue.status().queued,1);cancel.abort();await assert.rejects(pending);gate.resolve();await block;assert.equal(calls,0);assert.deepEqual(await quotas.load(account.id),account);
  });
  await t.test('an active ignored cancellation retains the slot until the real reader settles and cannot publish a new snapshot',async()=>{
   const account=await owner(),gate=deferred(),started=deferred(),cancel=new AbortController();let signal;const {queue,quotas}=reader(async(_owner,options)=>{signal=options.signal;started.resolve();await gate.promise;return quota(now);}),pending=quotas.refresh(account,{force:true,signal:cancel.signal});await started.promise;cancel.abort();assert.equal(signal.aborted,true);assert.equal(queue.status().active,1);assert.equal(quotas.status().pending,1);gate.resolve();assert.deepEqual(await pending,{cancelled:true});const after=await quotas.load(account.id);assert.equal(after.quota_json,account.quota_json);assert.equal(after.route_epoch,account.route_epoch);assert.equal(after.quota_error,account.quota_error);assert.equal(after.quota_attempted_at,now);
  });
  await t.test('a later joining caller cannot cancel the original operation or relabel its committed result',async()=>{
   const account=await owner(),gate=deferred(),started=deferred(),creator=new AbortController(),joined=new AbortController();let signal,calls=0;const {quotas}=reader(async(_owner,options)=>{calls++;signal=options.signal;started.resolve();await gate.promise;return quota(now);}),first=quotas.refresh(account,{force:true,signal:creator.signal});await started.promise;const second=quotas.refresh(account,{force:true,signal:joined.signal});joined.abort();assert.equal(signal.aborted,false);gate.resolve();assert.deepEqual(await first,{refreshed:true});assert.deepEqual(await second,{refreshed:true});assert.equal(calls,1);
  });
  await t.test('cancellation during the final transactional weight update rolls quota and route recovery back together',async()=>{
   const account=await owner({blocked:true}),cancel=new AbortController(),original=router.syncWeights;router.syncWeights=async(...args)=>{const result=await original(...args);cancel.abort();return result;};try{const {quotas}=reader(async()=>quota(now));assert.deepEqual(await quotas.refresh(account,{force:true,signal:cancel.signal}),{cancelled:true});const after=await quotas.load(account.id);for(const field of ['quota_json','quota_error','route_epoch','route_block','quota_calibration'])assert.deepEqual(after[field],account[field],field);}finally{router.syncWeights=original;}
  });
  await t.test('a real COMMIT acknowledged before the cancellation boundary keeps the committed success',async()=>{
   const account=await owner(),cancel=new AbortController(),transaction=store.transaction;let armed=true;store.transaction=async function(...args){const result=await transaction.apply(this,args);if(armed&&result?.refreshed===true){armed=false;cancel.abort();}return result;};try{const {quotas}=reader(async()=>quota(now));assert.deepEqual(await quotas.refresh(account,{force:true,signal:cancel.signal}),{refreshed:true});assert.equal(cancel.signal.aborted,true);assert.equal(JSON.parse((await quotas.load(account.id)).quota_json).checked,now);}finally{store.transaction=transaction;}
  });
  await t.test('account administrator exposes only a single-account controlled quota refresh and unsupported kinds stay explicit',async()=>{
   const first=await owner(),second=await owner(),claude=await owner({kind:'claude'}),api=await owner({auth_mode:'openai_api'}),calls=[],admin=createPostgresAccountAdmin(store,{runtime:lab.directory+'/synthetic-runtime',encryptionKey:randomBytes(32),router,clock:()=>now,checkApi:async()=>[],checkSubscription:async()=>true,readQuota:async row=>{calls.push(row.id);return quota(now);}});try{assert.deepEqual(await admin.refreshQuota(first.id),{refreshed:true});assert.deepEqual(calls,[first.id]);assert.equal((await store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',second.id)).quota_json,second.quota_json);assert.equal((await admin.refreshQuota(claude.id)).unsupported,true);assert.equal((await admin.refreshQuota(api.id)).unsupported,true);assert.deepEqual(calls,[first.id]);}finally{await admin.close();}
  });
 }finally{for(const close of resources.reverse())await close();await store?.close();await lab?.close();}
});
