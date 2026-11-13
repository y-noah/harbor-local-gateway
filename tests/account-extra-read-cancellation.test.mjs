import test from 'node:test';
import assert from 'node:assert/strict';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresAccountPackages} from '../postgres-account-packages.mjs';
import {createPostgresAccountAdmin} from '../postgres-account-admin.mjs';
import {createPostgresExtraUsageAdmin} from '../postgres-extra-usage-admin.mjs';
const deferred=()=>{let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};};
async function until(check){const end=Date.now()+2000;while(!check()){if(Date.now()>end)throw Error('condition timeout');await new Promise(r=>setTimeout(r,5));}}
const response=()=>({writeHead(){},end(){}});
const makeExtra=(reader,options={})=>createPostgresExtraUsageAdmin({service:{saveAccount:async()=>({saved:true}),recordManual:async()=>({}),recordResetCardUsage:async()=>({}),refresh:async()=>({unsupported:true})},reader,adminAuthenticated:()=>true,adminSession:()=>({session_ref:'a'.repeat(64)}),acceptsAdminRequest:()=>true,requestBodies:{read:async()=>({}),release(){}},reads:{maxConcurrent:1},...options});

test('套餐GET等待取消不执行reader，已执行读取保留实际队列槽',async()=>{
 const queue=createPostgresQueryQueue({maxConcurrent:1}),gate=deferred(),running=queue.run(()=>gate.promise),calls=[];
 const store={all:async sql=>{if(sql.startsWith('SELECT name,value FROM harbor.gateway_settings'))return [];assert.ok(sql.includes('FROM harbor.account_package_templates'));calls.push('catalog');return [];},get:async sql=>sql.startsWith("SELECT to_regclass('harbor.gateway_settings')")?{settings:false,departments:false,admin_keys:false,department_settings:false,columns:false}:null};const packages=createPostgresAccountPackages(store,{queryQueue:queue}),controller=new AbortController();
 try{for(const path of ['/api/admin/account-packages','/api/admin/accounts/synthetic/package','/api/admin/account-packages/previews/synthetic']){const c=new AbortController(),waiting=packages.handle(path,'GET',{}, {signal:c.signal});await until(()=>queue.status().queued===1);c.abort();await assert.rejects(waiting,{name:'AbortError'});assert.equal(queue.status().queued,0);}assert.deepEqual(calls,[]);gate.resolve();await running;
 const activeGate=deferred();store.all=async sql=>{if(sql.startsWith('SELECT name,value FROM harbor.gateway_settings'))return [];assert.ok(sql.includes('FROM harbor.account_package_templates'));calls.push('active');await activeGate.promise;return [];};const activeController=new AbortController(),active=packages.catalog({signal:activeController.signal});await until(()=>calls.includes('active'));activeController.abort();assert.equal(queue.status().active,1);activeGate.resolve();await active;assert.equal(calls.filter(v=>v==='active').length,1);
 }finally{gate.resolve();await running;await queue.close();}
});

test('账号订阅GET等待取消，账号PATCH不接受请求中断作为写取消',async()=>{
 const gate=deferred(),entered=deferred();let readCount=0,writes=0;
 const store={get:async sql=>{if(sql.startsWith("SELECT to_regclass('harbor.gateway_settings')"))return {settings:false,departments:false,admin_keys:false,department_settings:false,columns:false};if(sql.includes('SELECT * FROM harbor.accounts'))return {id:'synthetic',auth_mode:'subscription',enabled:1,state:'offline'};if(sql.includes('SELECT kind,auth_mode')){readCount++;entered.resolve();await gate.promise;return {kind:'codex',auth_mode:'subscription'};}return null;},query:async()=>{},run:async()=>{writes++;},transaction:async fn=>fn()};
 const router={disableAccount:async()=>({moved:0}),block:async()=>{},syncWeights:async()=>{},readView:async()=>({})};
 const admin=createPostgresAccountAdmin(store,{runtime:'/tmp',router,checkApi:async()=>[],checkSubscription:async()=>true,checks:{maxConcurrent:1}});
 try{const active=admin.handle('/api/admin/accounts/synthetic/subscription','GET');await entered.promise;const c=new AbortController(),waiting=admin.handle('/api/admin/accounts/synthetic/subscription','GET',{}, {signal:c.signal});await until(()=>admin.status().queued===1);c.abort();await assert.rejects(waiting,{name:'AbortError'});assert.equal(readCount,1);gate.resolve();await active;
 const aborted=new AbortController();aborted.abort();const result=await admin.handle('/api/admin/accounts/synthetic/subscription','PATCH',{monthly_usd:200,expected_version:0},{signal:aborted.signal});assert.equal(result.status,200);assert.ok(writes>0);
 }finally{gate.resolve();await admin.close();}
});

test('额外额度GET等待取消且已执行读取不中断，refresh POST核验和后台任务不随signal取消',async()=>{
 const gate=deferred(),entered=deferred();let calls=0;const admin=makeExtra({readPolicy:async()=>({}),readAccount:async()=>{calls++;if(calls===1){entered.resolve();await gate.promise;}return {};}});
 try{const activeController=new AbortController(),first=admin.handle({url:'/api/admin/accounts/synthetic/extra-usage',method:'GET'},response(),{signal:activeController.signal});await entered.promise;activeController.abort();assert.equal(admin.status().reads.active,1);
 const c=new AbortController(),waiting=admin.handle({url:'/api/admin/accounts/synthetic/extra-usage',method:'GET'},response(),{signal:c.signal});await until(()=>admin.status().reads.queued===1);c.abort();await assert.rejects(waiting,{name:'AbortError'});assert.equal(admin.status().reads.active,1);assert.equal(calls,1);
 const aborted=new AbortController();aborted.abort();const post=admin.handle({url:'/api/admin/accounts/synthetic/extra-usage/refresh',method:'POST'},response(),{signal:aborted.signal});await until(()=>admin.status().reads.queued===1);gate.resolve();await first;assert.equal(await post,true);assert.equal(calls,2);await until(()=>admin.status().pending===0);
 }finally{gate.resolve();await admin.close();}
});


test('额外额度全部排队GET路由取消后不读取，预先中断也不入队',async()=>{
 const gate=deferred(),entered=deferred();let reads=0;const reader={readPolicy:async()=>{entered.resolve();await gate.promise;return {};},readAccount:async()=>{reads++;return {};}};
 const batches=Object.fromEntries(['coverage','records','list','detail','previewPolicy','previewAccounts','submit','retryAccounts'].map(name=>[name,async()=>{reads++;return {};}])) ,admin=makeExtra(reader,{batches});
 try{const running=admin.handle({url:'/api/admin/extra-usage/policy',method:'GET'},response());await entered.promise;
 for(const url of ['/api/admin/extra-usage/policy','/api/admin/accounts/synthetic/extra-usage','/api/admin/accounts/synthetic/extra-usage/records','/api/admin/extra-usage/coverage','/api/admin/extra-usage/batches','/api/admin/extra-usage/batches/00000000-0000-0000-0000-000000000000']){
  const c=new AbortController(),pending=admin.handle({url,method:'GET'},response(),{signal:c.signal});await until(()=>admin.status().reads.queued===1);c.abort();await assert.rejects(pending,{name:'AbortError'});assert.equal(admin.status().reads.queued,0);
  await assert.rejects(admin.handle({url,method:'GET'},response(),{signal:c.signal}),{name:'AbortError'});
 }assert.equal(reads,0);gate.resolve();await running;
 }finally{gate.resolve();await admin.close();}
});
