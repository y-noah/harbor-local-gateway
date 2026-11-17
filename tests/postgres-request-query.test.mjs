import test from 'node:test';
import assert from 'node:assert/strict';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {AppError} from '../store.mjs';
import {createPostgresRequestQueries} from '../postgres-request-query.mjs';
import {withLegacyDepartmentSchema as legacyStore} from './department-legacy-query-fixture.mjs';

test('query queue bounds waiting work, times it out and drains active work before close',{timeout:1000},async t=>{
  t.mock.timers.enable({apis:['setTimeout']});let release,entered,calls=0;const gate=new Promise(resolve=>{release=resolve;}),started=new Promise(resolve=>{entered=resolve;});
  const store={run:async()=>{},transaction:async fn=>fn(),get:async()=>{calls++;entered();await gate;throw new AppError(503,'synthetic stop','fixture_stop');}};
  const reader=createPostgresRequestQueries(legacyStore(store),{maxConcurrent:1,maxQueued:1,queueTimeoutMs:100});
  const first=assert.rejects(reader.read({view:'list'}),{code:'fixture_stop'}),second=assert.rejects(reader.read({view:'list'}),{code:'query_timeout'});
  await started;
  await assert.rejects(reader.read({view:'list'}),{code:'query_queue_full'});assert.deepEqual(reader.status(),{active:1,queued:1,closed:false});
  t.mock.timers.tick(100);await second;assert.equal(reader.status().queued,0);assert.equal(calls,1);
  const closed=reader.close();release();await Promise.all([first,closed]);assert.deepEqual(reader.status(),{active:0,queued:0,closed:true});
  await assert.rejects(reader.read({view:'list'}),{code:'query_closed'});
});

test('database statement timeout becomes an actionable query timeout and does not retain active slots',async()=>{
  const reader=createPostgresRequestQueries(legacyStore({run:async()=>{},transaction:async()=>{throw Object.assign(Error('synthetic timeout'),{code:'57014'});}}));
  await assert.rejects(reader.read({view:'summary'}),{status:503,code:'query_timeout'});await reader.close();assert.equal(reader.status().active,0);
});


test('overview and request queries share one admission budget and release it after synchronous failure',async()=>{
  let release;const gate=new Promise(resolve=>{release=resolve;});
  const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:0});
  const store={transaction:async()=>{await gate;throw new AppError(503,'synthetic state stop','fixture_stop');}};
  const states=createPostgresStateQueries(legacyStore(store),{queryQueue:queue}),requests=createPostgresRequestQueries(legacyStore(store),{queryQueue:queue});
  const first=assert.rejects(states.read(),{code:'fixture_stop'});
  await assert.rejects(requests.read({view:'list'}),{code:'query_queue_full'});assert.deepEqual(requests.status(),{active:1,queued:0,closed:false});
  release();await first;await queue.close();assert.equal(states.status().active,0);
  const another=createPostgresQueryQueue();await assert.rejects(another.run(()=>{throw new AppError(503,'synthetic sync stop','fixture_stop');}),{code:'fixture_stop'});await another.close();assert.equal(another.status().active,0);
});

test('取消已执行请求后保留真实槽位，当前阶段结束即停止后续查询',{timeout:5000},async t=>{
 for(const stage of ['cleanup','visibility','snapshot','count','rows','previews','statistics','models','members'])await t.test(stage,async()=>{
  const controller=new AbortController(),reason=new AppError(499,'合成取消','request_cancelled'),calls=[];
  let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);
  const step=async(name,value)=>{calls.push(name);if(name===stage){entered();await gate;}return value;};
  const store=legacyStore({
   run:async sql=>step(sql.startsWith('DELETE')?'cleanup':'snapshot'),
   get:async sql=>sql.includes('pg_current_snapshot')?step('visibility',{visibility:'1:2:'}):step('count',{n:0}),
   all:async sql=>step(sql.includes('WITH data AS')?'statistics':sql.startsWith('SELECT DISTINCT')?'models':sql.startsWith('SELECT m.id')?'members':'rows',[]),
   transaction:async fn=>fn(),
  });
  const reader=createPostgresRequestQueries(store,{maxConcurrent:1,maxQueued:0,readInputPreviews:async()=>step('previews',new Map())});
  const view=['statistics','models','members'].includes(stage)?'summary':'list';
  const result=assert.rejects(reader.read({view,page:1,pageSize:20},{signal:controller.signal}),error=>error===reason);
  try{
   await started;controller.abort(reason);assert.equal(reader.status().active,1);
   await assert.rejects(reader.read({view:'list',page:1,pageSize:20}),{code:'query_queue_full'});
   const before=[...calls];release();await result;await reader.close();assert.deepEqual(calls,before);assert.equal(reader.status().active,0);
  }finally{release();await result;await reader.close();}
 });
});
