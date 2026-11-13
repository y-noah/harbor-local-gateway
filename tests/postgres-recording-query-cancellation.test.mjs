import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresRecordingAdmin} from '../postgres-recording-admin.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {withLegacyDepartmentSchema} from './department-legacy-query-fixture.mjs';

const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};};
async function until(predicate){const deadline=Date.now()+3000;while(!predicate()){assert.ok(Date.now()<deadline,'等待合成正文查询状态超时');await new Promise(resolve=>setTimeout(resolve,5));}}
const exportId='recording.export:00000000-0000-4000-8000-000000000001';
const paths=['/api/admin/recordings/search?q=synthetic','/api/admin/recordings/context/search?q=synthetic','/api/admin/recordings/session?session=synthetic-session','/api/admin/recordings/requests/synthetic-request','/api/admin/recordings/exports','/api/admin/recordings/exports/'+exportId];

async function fixture(t){
 const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:2,queueTimeoutMs:3000}),gate=deferred(),closed=new Set();let reads=0,writes=0,transactions=0,writeBlocked=false,readBlocked=false,finished=0;
 const store={transaction:async operation=>{transactions++;if(writeBlocked||readBlocked)await gate.promise;return operation();},get:async sql=>{reads++;if(readBlocked)await gate.promise;
  if(sql.includes('recording_work'))return {count:0,oldest:null};
  if(sql.includes('recording_exports'))return {id:exportId,status:'complete',expires:Date.now()+60000,options:{format:'jsonl',max_rows:20,max_bytes:1024,include_body:false}};
  if(sql.includes('SELECT recording_state'))return {recording_state:'none'};
  return undefined;
 },all:async()=>{reads++;if(readBlocked)await gate.promise;return [];},run:async()=>{writes++;return {changes:1};}};
 const legacyStore=withLegacyDepartmentSchema(store),backend=createPostgresRecordingJobs(legacyStore,{recordingKey:Buffer.alloc(32,1),queryQueue:queue,contextIndex:true,storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});
 const admin=createPostgresRecordingAdmin(backend,{adminAuthenticated:async()=>true}),clients=new Set();
 const server=http.createServer((req,res)=>{res.once('close',()=>closed.add(req.url));void admin.handle(req,res).then(done=>{if(!done&&!res.destroyed){res.statusCode=404;res.end();}},error=>{if(!res.destroyed){res.statusCode=error.status??500;res.end('{}');}}).finally(()=>finished++);});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 t.after(async()=>{gate.resolve();for(const client of clients)client.destroy();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await admin.close({force:true});await queue.close();});
 const send=(path,method='GET',payload)=>{const client=http.request({host:'127.0.0.1',port:server.address().port,path,method,agent:false,headers:payload?{'Content-Type':'application/json'}:{}},response=>response.resume());clients.add(client);client.on('error',()=>{});client.once('close',()=>clients.delete(client));client.end(payload?JSON.stringify(payload):undefined);return client;};
 return {store:legacyStore,queue,gate,backend,admin,send,closed,reads:()=>reads,writes:()=>writes,transactions:()=>transactions,finished:()=>finished,blockRead:()=>{readBlocked=true;},blockWrite:()=>{writeBlocked=true;}};
}

test('正文HTTP外层断开同时取消内层等待读取，不留下嵌套查询',async t=>{
 const f=await fixture(t),blocking=f.queue.run(()=>f.gate.promise);
 for(const path of paths){const prior=f.finished(),client=f.send(path);await until(()=>f.queue.status().queued===1);client.destroy();await until(()=>f.closed.has(path)&&f.finished()===prior+1&&f.admin.status().pending===0);
  assert.deepEqual(f.queue.status(),{active:1,queued:0,closed:false});assert.equal(f.reads(),0);assert.equal(f.writes(),0);
 }
 f.gate.resolve();await blocking;await until(()=>f.queue.status().active===0);assert.equal(f.reads(),0);
});

test('正文已受理读取断开后继续占用真实执行槽，直到操作完成',async t=>{
 const f=await fixture(t);f.blockRead();const client=f.send('/api/admin/recordings/exports');await until(()=>f.reads()===1);client.destroy();await until(()=>f.closed.size===1);assert.equal(f.queue.status().active,1);
 let successorStarted=false;const successor=f.queue.run(()=>{successorStarted=true;});assert.equal(successorStarted,false);assert.equal(f.queue.status().queued,1);
 f.gate.resolve();await successor;await until(()=>f.admin.status().pending===0);assert.equal(f.queue.status().active,0);assert.equal(successorStarted,true);
});

test('正文后台重建已受理后，HTTP断开不撤销写入',async t=>{
 const f=await fixture(t);f.blockWrite();const client=f.send('/api/admin/recordings/rebuild','POST',{request_id:'synthetic-request'});
 // The HTTP admission reaches the transaction before the client disconnects.
 await until(()=>f.transactions()===1);
 client.destroy();await until(()=>f.closed.size===1);f.gate.resolve();await until(()=>f.admin.status().pending===0);
 assert.ok(f.writes()>0);assert.equal(f.queue.status().active,0);assert.equal(f.queue.status().queued,0);
});

test('正文独立signal不改变筛选身份，旧调用及无队列读取保持可用',async t=>{
 const f=await fixture(t),controller=new AbortController(),reason=Error('synthetic cancellation');controller.abort(reason);
 for(const path of paths){const url=new URL(path,'http://localhost');await assert.rejects(f.backend.handle(url.pathname,'GET',{},url.searchParams,{signal:controller.signal}),error=>error===reason);}
 assert.equal(f.reads(),0);assert.equal(f.writes(),0);
 const value=await f.backend.handle('/api/admin/recordings/exports','GET',{},new URLSearchParams());assert.deepEqual(value.value,{items:[],next_cursor:null});
 const unqueued=createPostgresRecordingJobs(f.store,{recordingKey:Buffer.alloc(32,1),storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});assert.deepEqual(await unqueued.exports.list(),{items:[],next_cursor:null});
 const query={from:Date.now()-60000,to:Date.now(),limit:20,member:'',type:'',model:'',session:'',status:'',outcome:'',device:'',q:'synthetic',cursor:''};
 const before=structuredClone(query);await f.backend.queries.search(query);assert.deepEqual(query,before);
});
