import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readdir,readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes,randomUUID} from 'node:crypto';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {RecordingStore} from '../recording-store.mjs';
const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const event=(index,text='正文 '+index)=>({sourceKey:'event-'+index,kind:'assistant',provenance:'upstream',content:{text,mode:'append'}});
async function fixture(t,options={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-coalescing-')),repository=new RecordingStore({},{encryptionKey:randomBytes(32)}),calls=[];
 for(const method of ['start','append','finish'])repository[method]=async(...args)=>calls.push([method,...args]);repository.store.finish=async(...args)=>calls.push(['settle',...args]);
 const buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,...options});t.after(async()=>{await buffer.close();await rm(directory,{recursive:true,force:true});});return {buffer,repository,directory,calls};
}
const exactBytes=buffer=>buffer.queue.reduce((sum,entry)=>sum+Buffer.byteLength(JSON.stringify(entry.value)),0);
test('interleaved requests coalesce only pending appends, retaining per-request lifecycle and exact memory accounting',async t=>{
 const f=await fixture(t,{maxCommands:8});for(const id of ['one','two'])assert.equal(f.buffer.start(id,'known'),true);
 for(let index=0;index<20;index++)for(const id of ['one','two']){assert.equal(f.buffer.append(id,[event(index,'问题 Authorization: Bearer synthetic-secret')]),true);assert.equal(f.buffer.status().memoryBytes,exactBytes(f.buffer));}
 assert.equal(f.buffer.status().queued,4);for(const id of ['one','two']){assert.equal(f.buffer.finish(id,{state:'complete'}),true);assert.equal(f.buffer.settle(id,{status:'ok',input:1,output:2,usageKnown:true}),true);}
 assert.equal(f.buffer.status().queued,8);assert.equal(f.buffer.status().dropped,0);await f.buffer.flush();
 for(const id of ['one','two']){const rows=f.calls.filter(row=>row[1]===id);assert.deepEqual(rows.map(row=>row[0]),['start','append','finish','settle']);assert.deepEqual(rows[1][2].map(row=>row.sourceKey),Array.from({length:20},(_,index)=>'event-'+index));}
 assert.equal(JSON.stringify(f.calls).includes('synthetic-secret'),false);assert.equal(f.buffer.status().memoryBytes,0);
});
test('each lifecycle barrier and the 128-event ceiling keep separate commands without extending admission caps',async t=>{
 const f=await fixture(t,{maxCommands:8});f.buffer.append('one',[event(0)]);f.buffer.start('one','known');f.buffer.append('one',[event(1)]);f.buffer.finish('one',{state:'complete'});f.buffer.append('one',[event(2)]);f.buffer.settle('one',{status:'ok'});f.buffer.append('one',[event(3)]);
 assert.equal(f.buffer.status().queued,7);assert.equal(f.buffer.append('two',Array.from({length:128},(_,i)=>event(i))),true);assert.equal(f.buffer.append('two',[event(128)]),false);assert.equal(f.buffer.status().diagnostics.dropReasons.command_limit,1);assert.equal(f.buffer.append('one',[event(4)]),true,'a full queue may still extend its last unattempted append');await f.buffer.flush();assert.deepEqual(f.calls.filter(row=>row[1]==='one').map(row=>row[0]),['append','start','append','finish','append','settle','append']);assert.equal(f.calls.at(-1)[2].length,128);
});
test('merged command and total memory stay within original byte limits with fallback to separate bounded commands',async t=>{
 const f=await fixture(t,{maxMemoryBytes:1024,maxCommands:10});f.repository.maxBytes=600;
 assert.equal(f.buffer.append('one',[event(0,'a'.repeat(220))]),true);assert.equal(f.buffer.append('one',[event(1,'b'.repeat(220))]),true);assert.equal(f.buffer.status().queued,2,'overlarge merged command must stay split');assert.equal(f.buffer.append('one',[event(2,'c'.repeat(220))]),false);assert.equal(f.buffer.status().diagnostics.dropReasons.memory_limit,1);assert.equal(f.buffer.status().memoryBytes,exactBytes(f.buffer));assert.ok(f.buffer.queue.every(entry=>entry.bytes<=600));await f.buffer.flush();assert.equal(f.buffer.status().memoryBytes,0);
});
test('whole attempted atomic prefix is immutable before its first await and old release preserves the new same-request anchor',async t=>{
 const entered=deferred(),release=deferred();let batches=0;const f=await fixture(t,{atomicBatch:async operation=>{if(++batches===1){entered.resolve();await release.promise;}await operation();}});
 try{f.buffer.start('one','known');f.buffer.append('one',[event(0)]);const flushing=f.buffer.flush();await entered.promise;assert.equal(f.buffer.append('one',[event(1)]),true);assert.equal(f.buffer.append('one',[event(2)]),true);assert.equal(f.buffer.status().queued,3);assert.deepEqual(f.buffer.queue[1].value.events.map(row=>row.sourceKey),['event-0']);release.resolve();await flushing;assert.deepEqual(f.calls.filter(row=>row[0]==='append').map(row=>row[2].map(e=>e.sourceKey)),[['event-0'],['event-1','event-2']]);assert.equal(f.buffer.status().memoryBytes,0);}
 finally{release.resolve();}
});
test('legacy encrypted spool freezes its selected append before sealing and never merges a later append into it',async t=>{
 const f=await fixture(t),seal=f.repository.seal.bind(f.repository);let first=true;
 f.repository.seal=(value,aad)=>{if(first){first=false;assert.equal(f.buffer.append('one',[event(1)]),true);assert.equal(f.buffer.append('one',[event(2)]),true);}return seal(value,aad);};
 assert.equal(f.buffer.append('one',[event(0)]),true);await f.buffer.flush();assert.deepEqual(f.calls.map(row=>row[2].map(e=>e.sourceKey)),[['event-0'],['event-1','event-2']]);assert.equal(f.buffer.status().queued,0);assert.equal(f.buffer.status().memoryBytes,0);assert.equal(f.buffer.status().dropped,0);
});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('real PG and encrypted spool retain interleaved event order, integrity and single billing through rollback and lost acknowledgement',{skip:!binDirectory?'需独立PG18与真实正文/计量事务':false,timeout:120000},async()=>{
 const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target;const buffers=[],directories=[];
 try{target=await createTarget({profile:'selfcheck',purpose:'seed',binDirectory,libraryDirectory});const store=target.datasetStore,owner=await store.get('SELECT m.id member_id,k.id key_id FROM harbor.members m JOIN harbor.keys k ON k.member_id=m.id LIMIT 1'),repository=new RecordingStore(store,{encryptionKey:target.recordingKey});
 for(const mode of ['healthy','rollback','lost-ack']){
  const directory=await mkdtemp(join(tmpdir(),'harbor-real-coalescing-'));directories.push(directory);const ids=Array.from({length:40},()=>randomUUID()),entered=deferred(),release=deferred();let attempts=0;const applied=[];
  for(const id of ids)await store.run("INSERT INTO harbor.requests(id,member_id,key_id,started,status,ip,agent,model,billing_model,recording_state) VALUES($1,$2,$3,$4,'running','synthetic','synthetic','gpt-6.1-sol','gpt-6.1-sol','pending')",id,owner.member_id,owner.key_id,Date.now());
  const buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicBatch:async operation=>{const attempt=++attempts;if(attempt===1){entered.resolve();await release.promise;}const result=await store.transaction(async()=>{await operation();if(attempt===1&&mode==='rollback')throw Error('synthetic-before-commit');});if(attempt===1&&mode==='lost-ack')throw Error('synthetic-after-commit');return result;}});buffers.push(buffer);
  try{
   for(const id of ids){assert.equal(buffer.start(id,'known'),true);assert.equal(buffer.append(id,[{...event(0,'输入 '+id),kind:'user',provenance:'client'}]),true);}const flushing=buffer.flush();await entered.promise;
   // 400 interleaved output appends exceed the old 256-command budget. Payload
   // bytes remain below the unchanged 4MiB memory budget throughout.
   for(let index=1;index<=10;index++)for(const id of ids)assert.equal(buffer.append(id,[event(index,'片段 '+index+' '+('x'.repeat(4096)))]),true);
   for(const id of ids){assert.equal(buffer.finish(id,{state:'complete'}),true);assert.equal(buffer.settle(id,{status:'ok',input:100,output:20,cached:0,usageKnown:true}),true);}assert.equal(buffer.status().queued,200);assert.equal(buffer.status().dropped,0);assert.equal(buffer.status().memoryBytes,exactBytes(buffer));assert.ok(buffer.status().memoryBytes<=4*1024*1024);assert.ok(buffer.queue.every(row=>row.bytes<=repository.maxBytes&&(!row.value.events||row.value.events.length<=128)));
   const original=buffer.queue.map(row=>JSON.stringify(row.value));release.resolve();await flushing;
   if(mode!=='healthy'){assert.equal(buffer.status().files,200);const names=(await readdir(directory)).sort();for(let index=0;index<names.length;index++){const cipher=await readFile(join(directory,names[index]));assert.equal(cipher.includes(Buffer.from('片段')),false);applied.push(JSON.stringify(repository.open(cipher,names[index])));}assert.deepEqual(applied,original,'uncertain prefix and later commands keep their original authenticated spool payload');}
   await buffer.flush();assert.equal(buffer.status().queued,0);assert.equal(buffer.status().files,0);assert.equal(buffer.status().memoryBytes,0);
   for(const id of ids){const body=await repository.read(id,{limit:100});assert.equal(body.state,'complete');assert.equal(body.hasMore,false);assert.equal(body.events.length,11);assert.deepEqual(body.events.map(row=>row.sequence),Array.from({length:11},(_,i)=>i+1));assert.equal(body.events[0].content.text,'输入 '+id);for(let index=1;index<=10;index++)assert.equal(body.events[index].content.text,'片段 '+index+' '+('x'.repeat(4096)));assert.deepEqual(await store.get('SELECT status,input,output,recording_state FROM harbor.requests WHERE id=$1',id),{status:'ok',input:100,output:20,recording_state:'complete'});assert.equal((await store.get('SELECT COUNT(*)::int n FROM harbor.request_prices WHERE request_id=$1',id)).n,1);}
  }finally{release.resolve();await buffer.close();}
 }
 }finally{for(const buffer of buffers)await buffer.close();await target?.close();for(const directory of directories)await rm(directory,{recursive:true,force:true});}
});
