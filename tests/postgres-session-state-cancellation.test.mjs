import test from 'node:test';
import assert from 'node:assert/strict';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {withLegacyDepartmentSchema} from './department-legacy-query-fixture.mjs';

function gate(){let resolve;return {promise:new Promise(done=>resolve=done),resolve:()=>resolve()};}
function fixture(blocked){
 const entered=gate(),release=gate(),events=[];
 const step=async(name,value)=>{events.push(name);if(name===blocked){entered.resolve();await release.promise;}return value;};
 const store=withLegacyDepartmentSchema({
  transaction:async(fn,options)=>{assert.deepEqual(options,{level:'REPEATABLE READ',readOnly:true});events.push('begin');try{const result=await fn();events.push('commit');return result;}catch(error){events.push('rollback');throw error;}},
  get:async sql=>{assert.match(sql,/^SELECT COUNT\(\*\) n FROM harbor.sessions/);return step('count',{n:1});},
  all:async sql=>{
   if(sql.startsWith('SELECT s.id,'))return step('page',[{id:'synthetic-session',protocol:'responses',nightly_enabled:1}]);
   if(sql.startsWith('SELECT id,name,kind'))return step('accounts',[]);
   if(sql.startsWith('WITH selected_requests'))return step('prices',[]);
   throw Error('unexpected SQL '+sql);
  }
 });
 const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:1}),reader=createPostgresStateQueries(store,{queryQueue:queue});
 return {entered,release,events,queue,reader};
}

test('会话状态读取在阶段取消后不再执行后续SQL，当前步骤结束前保留执行槽位',{timeout:3000},async t=>{
 const sequence=['count','page','accounts','prices'];
 for(const blocked of sequence)await t.test(blocked,async()=>{
  const f=fixture(blocked),controller=new AbortController(),reason=blocked==='prices'?null:Error('synthetic navigation cancelled');let settled=false;
  const observed=f.reader.read({view:'sessions'},{signal:controller.signal}).then(value=>{settled=true;return {value};},error=>{settled=true;return {error};});
  try{
   await f.entered.promise;controller.abort(reason);assert.equal(settled,false);assert.equal(f.queue.status().active,1);
   f.release.resolve();const result=await observed;assert.ok(result.error);if(reason===null){assert.equal(result.error.status,499);assert.equal(result.error.code,'request_cancelled');}else assert.equal(result.error,reason);
   assert.deepEqual(f.events,['begin',...sequence.slice(0,sequence.indexOf(blocked)+1),'rollback']);assert.equal(f.queue.status().active,0);
  }finally{f.release.resolve();await observed;await f.reader.close();}
 });
});

test('会话状态预取消与排队取消不执行SQL，未取消请求保持结果及提交',{timeout:3000},async()=>{
 const f=fixture('count'),pre=new AbortController();pre.abort();await assert.rejects(f.reader.read({view:'sessions'},{signal:pre.signal}),{name:'AbortError'});assert.deepEqual(f.events,[]);
 const first=f.reader.read({view:'sessions'});await f.entered.promise;
 const controller=new AbortController(),reason=Error('synthetic queued cancel'),second=assert.rejects(f.reader.read({view:'sessions'},{signal:controller.signal}),error=>error===reason);
 controller.abort(reason);await second;assert.equal(f.queue.status().active,1);assert.deepEqual(f.events,['begin','count']);
 f.release.resolve();const result=await first;assert.equal(result.sessions.length,1);assert.equal(result.sessions[0].request_count,0);assert.deepEqual(result.sessionsPagination,{page:1,pageSize:25,total:1,totalPages:1});assert.deepEqual(f.events,['begin','count','page','accounts','prices','commit']);await f.reader.close();
});

test('部门身份解析期间取消，在解析完成后停止且不进入业务事务',{timeout:3000},async()=>{
 const entered=gate(),release=gate(),events=[],controller=new AbortController(),reason=Error('synthetic permission read cancelled');let blocked=false;
 const legacy=withLegacyDepartmentSchema({get:async()=>{throw Error('unexpected business SQL');},transaction:async()=>{events.push('transaction');throw Error('cancelled request entered transaction');}});
 const store={...legacy,get:async(sql,...args)=>{const result=await legacy.get(sql,...args);if(!blocked){blocked=true;events.push('permission');entered.resolve();await release.promise;}return result;}};
 const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:1}),reader=createPostgresStateQueries(store,{queryQueue:queue});let settled=false;
 const observed=reader.read({view:'sessions'},{signal:controller.signal}).then(value=>{settled=true;return {value};},error=>{settled=true;return {error};});
 try{await entered.promise;controller.abort(reason);assert.equal(settled,false);assert.equal(queue.status().active,1);release.resolve();assert.equal((await observed).error,reason);assert.deepEqual(events,['permission']);assert.equal(queue.status().active,0);}
 finally{release.resolve();await observed;await reader.close();}
});
