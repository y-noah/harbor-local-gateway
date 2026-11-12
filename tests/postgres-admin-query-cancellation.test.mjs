import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp,mkdir,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {createPostgresRequestQueries} from '../postgres-request-query.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresResponsesHandler} from '../postgres-responses.mjs';
import {createPostgresTypeStatistics} from '../postgres-type-statistics.mjs';
import {createPostgresDeviceAccess} from '../postgres-device-access.mjs';
import {createPostgresSessionHandoffs} from '../postgres-session-handoffs.mjs';
import {createPostgresPricingRuntime} from '../postgres-pricing-runtime.mjs';
import {AppError} from '../store.mjs';

const memberId='00000000-0000-4000-8000-000000000001';
const remainingReadPaths=[
 '/api/admin/balancing/plans/synthetic-plan','/api/admin/balancing/plans/synthetic-plan/items',
 '/api/admin/balancing/explanation','/api/admin/requests/synthetic-request','/api/admin/requests/synthetic-request/context','/api/admin/requests/synthetic-request/recording',
 '/api/admin/type-statistics','/api/admin/devices','/api/admin/sessions/synthetic-session/handoffs',
 '/api/admin/member-types','/api/admin/member-types/synthetic-type/templates',
 '/api/admin/member-config-batches','/api/admin/member-config-batches/'+memberId,
 '/api/admin/member-action-batches','/api/admin/member-action-batches/'+memberId,'/api/admin/pricing',
];
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};};
async function until(predicate){const deadline=Date.now()+3000;while(!predicate()){assert.ok(Date.now()<deadline,'等待合成HTTP状态超时');await new Promise(resolve=>setTimeout(resolve,5));}}
const readStopped=()=>new AppError(503,'合成读取已进入事务','synthetic_read_started');

test('已取消的管理读取不进入事务，旧调用继续执行且signal不写入查询参数',async()=>{
 const queue=createPostgresQueryQueue(),controller=new AbortController(),reason=Error('synthetic cancellation');controller.abort(reason);let transactions=0,runs=0,initializationReads=0;
 const store={transaction:async()=>{transactions++;throw readStopped();},run:async()=>{runs++;},get:async(sql,name)=>{assert.equal(sql,'SELECT value FROM harbor.gateway_settings WHERE name=$1');assert.equal(name,'schema.usage-hour-rollups');initializationReads++;return undefined;}};
 const states=createPostgresStateQueries(store,{queryQueue:queue}),requests=createPostgresRequestQueries(store,{queryQueue:queue}),members=createPostgresMemberAdmin(store,{queryQueue:queue});
 const query={view:'overview',people:'none'};
 try{
  await assert.rejects(states.read(query,{signal:controller.signal}),error=>error===reason);
  await assert.rejects(requests.read({view:'all'},{signal:controller.signal}),error=>error===reason);
  const paths=['/api/admin/members','/api/admin/members/selection','/api/admin/keys','/api/admin/members/'+memberId,'/api/admin/members/'+memberId+'/keys','/api/admin/keys/'+memberId+'/policy','/api/admin/members/'+memberId+'/usage-limits',...remainingReadPaths.filter(path=>/\/(member-types|member-config-batches|member-action-batches)(?:\/|$)/.test(path))];
  for(const path of paths)await assert.rejects(members.handle(path,'GET',{},new URLSearchParams(),{signal:controller.signal}),error=>error===reason);
  assert.equal(transactions,0);assert.equal(runs,0);assert.equal(initializationReads,0);assert.deepEqual(query,{view:'overview',people:'none'});
  await assert.rejects(states.read(query),{code:'synthetic_read_started'});assert.equal(transactions,1);
  await assert.rejects(requests.read({view:'all'}),{code:'synthetic_read_started'});assert.equal(transactions,2);
  await assert.rejects(members.handle('/api/admin/members','GET',{},new URLSearchParams()),{code:'synthetic_read_started'});assert.equal(transactions,3);
  await assert.rejects(members.handle('/api/admin/members','GET',{},new URLSearchParams()),{code:'synthetic_read_started'});assert.equal(transactions,4);assert.equal(initializationReads,1);
 }finally{await queue.close();}
});

async function fixture(t,{write=false}={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-admin-cancel-')),recording=join(directory,'recording'),metering=join(directory,'metering');await mkdir(recording);await mkdir(metering);
 const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:4,queueTimeoutMs:3000}),gate=deferred(),closed=new Set();let transactionGate=false,transactions=0,finished=0,businessWrites=0,reads=0;
 const store={transaction:async operation=>{transactions++;if(transactionGate)await gate.promise;if(write)return operation();throw readStopped();},run:async sql=>{if(/^(INSERT INTO harbor\.(members|keys)\b|UPDATE harbor.members SET usage_limits=)/.test(sql))businessWrites++;},get:async sql=>{reads++;if(sql==='SELECT value FROM harbor.gateway_settings WHERE name=$1')return undefined;return {id:memberId,default_models_set:0,enabled:1,usage_limits:'{"enabled":false,"rules":[]}',usage_limits_version:0,book:'{}'};},all:async()=>{reads++;return [];}};
 const queries=createPostgresRequestQueries(store,{queryQueue:queue});
 const handler=createPostgresResponsesHandler({store,recordings:{},buffer:{directory:recording},meteringRecovery:{directory:metering,settle(){},flush:async()=>{},status:()=>({}),close:async()=>{}},resolveRoute:async()=>{},checkLimits:async()=>{},transportForAccount:async()=>{},adminAuthenticated:async()=>true,requestQueries:queries,keyEncryptionKey:Buffer.alloc(32,1),recordingEnabled:false});
 const clients=new Set(),server=http.createServer((req,res)=>{res.once('close',()=>closed.add(req.url));void handler.handle(req,res).then(done=>{if(!done&&!res.destroyed){res.statusCode=404;res.end();}},error=>{if(!res.destroyed){res.statusCode=error.status??500;res.end('{}');}}).finally(()=>finished++);});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 t.after(async()=>{gate.resolve();for(const client of clients)client.destroy();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await handler.closeQueries();await queue.close();await rm(directory,{recursive:true,force:true});});
 function send(path,method='GET',body){const client=http.request({host:'127.0.0.1',port:server.address().port,path,method,agent:false,headers:body?{'content-type':'application/json'}:{}},response=>response.resume());clients.add(client);client.on('error',()=>{});client.once('close',()=>clients.delete(client));client.end(body?JSON.stringify(body):undefined);return client;}
 return {queue,gate,send,closed,transactions:()=>transactions,finished:()=>finished,businessWrites:()=>businessWrites,reads:()=>reads,blockTransactions:()=>{transactionGate=true;}};
}

test('真实管理GET断开会移除等待任务，不再执行废弃查询',async t=>{
 const f=await fixture(t),blocking=f.queue.run(()=>f.gate.promise);
 const paths=['/api/admin/state?view=accounts&people=none','/api/admin/requests?view=all','/api/admin/history','/api/admin/history/nightly/2026-11-11','/api/admin/members?view=list','/api/admin/members/selection','/api/admin/keys','/api/admin/members/'+memberId,'/api/admin/members/'+memberId+'/keys','/api/admin/keys/'+memberId+'/policy','/api/admin/members/'+memberId+'/usage-limits',...remainingReadPaths];
 for(const path of paths){const prior=f.finished(),client=f.send(path);await until(()=>f.queue.status().queued===1);client.destroy();await until(()=>f.closed.has(path)&&f.finished()===prior+1);assert.deepEqual(f.queue.status(),{active:1,queued:0,closed:false});assert.equal(f.transactions(),0);assert.equal(f.reads(),0);}
 f.gate.resolve();await blocking;await until(()=>f.queue.status().active===0);assert.equal(f.transactions(),0);
});

test('已执行的真实GET断开后仍占用槽位，直到事务真实结束',async t=>{
 const f=await fixture(t);f.blockTransactions();const path='/api/admin/state?view=overview',client=f.send(path);await until(()=>f.transactions()===1);client.destroy();await until(()=>f.closed.has(path));assert.equal(f.queue.status().active,1);
 let successorStarted=false;const successor=f.queue.run(()=>{successorStarted=true;});assert.equal(f.queue.status().queued,1);assert.equal(successorStarted,false);
 f.gate.resolve();await successor;await until(()=>f.finished()===1&&f.queue.status().active===0);assert.equal(successorStarted,true);
});

test('真实POST在请求体读完并开始事务后不因客户端断开而取消写入',async t=>{
 const f=await fixture(t,{write:true});f.blockTransactions();const path='/api/admin/members',client=f.send(path,'POST',{name:'合成取消测试员工',quota:null,expires:null});await until(()=>f.transactions()===1);client.destroy();await until(()=>f.closed.has(path));assert.equal(f.businessWrites(),0);
 f.gate.resolve();await until(()=>f.finished()===1);assert.equal(f.businessWrites(),2);assert.deepEqual(f.queue.status(),{active:0,queued:0,closed:false});
});


test('真实PATCH已排队的写操作不会因客户端断开而移除',async t=>{
 const f=await fixture(t,{write:true}),blocking=f.queue.run(()=>f.gate.promise),path='/api/admin/members/'+memberId+'/usage-limits';
 const client=f.send(path,'PATCH',{enabled:false,expected_version:0});await until(()=>f.queue.status().queued===1);client.destroy();await until(()=>f.closed.has(path));
 assert.equal(f.queue.status().queued,1);assert.equal(f.transactions(),0);f.gate.resolve();await blocking;await until(()=>f.finished()===1&&f.queue.status().active===0);assert.equal(f.businessWrites(),1);
});


test('补齐GET取消不会中止类型、批次、设备策略和费率的既有写入事务',async t=>{
 const writes=[
  ['/api/admin/member-types','POST',{name:'合成类型'}],
  ['/api/admin/member-config-batches/'+memberId+'/execute','POST',{}],
  ['/api/admin/member-action-batches/'+memberId+'/execute','POST',{}],
  ['/api/admin/members/'+memberId+'/device-policy','PATCH',{mode:'compatible',expected_version:0}],
  ['/api/admin/pricing','PATCH',{}],
 ];
 for(const [path,method,body] of writes)await t.test(path,async t=>{const f=await fixture(t);f.blockTransactions();const client=f.send(path,method,body);await until(()=>f.transactions()===1);client.destroy();await until(()=>f.closed.has(path));assert.equal(f.finished(),0);f.gate.resolve();await until(()=>f.finished()===1);assert.equal(f.transactions(),1);});
});


test('新增类型、设备、转移状态和费率读取支持预取消并兼容无options调用',async()=>{
 const queue=createPostgresQueryQueue(),controller=new AbortController(),reason=Error('synthetic early cancellation');controller.abort(reason);let invoked=0;
 const fail=async()=>{invoked++;throw readStopped();},store={get:fail,all:fail,transaction:fail},types=createPostgresTypeStatistics(store,{queryQueue:queue}),devices=createPostgresDeviceAccess(store,{queryQueue:queue}),handoffs=createPostgresSessionHandoffs(store,{queryQueue:queue,gate:{register(){},stop(){}},validateTarget:async()=>true}),pricing=createPostgresPricingRuntime(store,{queryQueue:queue});
 const reads=[options=>types.read({},options),options=>devices.list(new URLSearchParams(),options),options=>handoffs.handle('/api/admin/sessions/synthetic-session/handoffs','GET',{},options),options=>pricing.status(options)];
 try{for(const read of reads)await assert.rejects(read({signal:controller.signal}),error=>error===reason);assert.equal(invoked,0);for(const read of reads)await assert.rejects(read(),{code:'synthetic_read_started'});assert.equal(invoked,4);}
 finally{await types.close();await handoffs.close();await pricing.close();await queue.close();}
});
