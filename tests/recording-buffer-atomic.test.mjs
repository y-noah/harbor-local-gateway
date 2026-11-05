import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomBytes,randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {RecordingStore} from '../recording-store.mjs';
const until=async predicate=>{const deadline=performance.now()+5000;while(!predicate()){assert.ok(performance.now()<deadline);await delay(5);}};
async function fixture(t,atomicBatch){
 const directory=await mkdtemp(join(tmpdir(),'harbor-atomic-buffer-')),repository=new RecordingStore({},{encryptionKey:randomBytes(32)}),calls=[];
 for(const method of ['start','append','finish'])repository[method]=async(...args)=>calls.push([method,...args]);
 const buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicBatch});t.after(async()=>{await buffer.close();await rm(directory,{recursive:true,force:true});});return {buffer,directory,calls,repository};
}
test('atomic recording prefix stays queued until commit and newly appended work remains ordered',async t=>{
 let release,entered=false,batches=0;const gate=new Promise(resolve=>release=resolve),f=await fixture(t,async operation=>{batches++;await operation();entered=true;await gate;});
 try{
  f.buffer.start('one','known');f.buffer.finish('one',{state:'complete'});const draining=f.buffer.flush();await until(()=>entered);assert.equal(f.buffer.status().queued,2);assert.equal(f.buffer.status().memoryBytes>0,true);assert.deepEqual(await readdir(f.directory),[]);f.buffer.start('two','known');release();await draining;
  assert.equal(batches,2);assert.deepEqual(f.calls.map(row=>row.slice(0,2)),[['start','one'],['finish','one'],['start','two']]);assert.equal(f.buffer.status().queued,0);assert.equal(f.buffer.status().memoryBytes,0);assert.equal(f.buffer.status().failures,0);assert.deepEqual(await readdir(f.directory),[]);
 }finally{release();}
});
test('an atomic callback that does not execute the writer falls back to the legacy encrypted spool',async t=>{
 const f=await fixture(t,async()=>{});f.buffer.start('fallback','known');await f.buffer.flush();assert.equal(f.buffer.status().failures,1);assert.equal(f.buffer.status().files,1);assert.equal(f.calls.length,0);await f.buffer.flush();assert.equal(f.buffer.status().files,0);assert.equal(f.calls.length,1);
});
test('an older corrupt recovery file remains a fence before any atomic commit',async t=>{
 let batches=0;const f=await fixture(t,async operation=>{batches++;await operation();});const file=String(1).padStart(20,'0')+'-'+randomUUID()+'.record';
 const {writeFile}=await import('node:fs/promises');await f.buffer.close();await writeFile(join(f.directory,file),Buffer.alloc(40),{mode:0o600});const restored=await RecordingBuffer.create(f.repository,{directory:f.directory,flushMs:10000,atomicBatch:async operation=>{batches++;await operation();}});
 try{restored.start('later','known');await restored.flush();assert.equal(batches,0);assert.equal(f.calls.length,0);assert.equal(restored.status().files,2);}finally{await restored.close();}
});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('real PostgreSQL commits, rolls back and recovers uncertain atomic recording batches without duplicate text or billing',{skip:!binDirectory?'需独立PG18与真实正文／计量事务':false,timeout:120000},async()=>{
 const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target;const buffers=[],directories=[];
 try{
  target=await createTarget({profile:'selfcheck',purpose:'seed',binDirectory,libraryDirectory});const store=target.datasetStore,owner=await store.get('SELECT m.id member_id,k.id key_id FROM harbor.members m JOIN harbor.keys k ON k.member_id=m.id LIMIT 1'),repository=new RecordingStore(store,{encryptionKey:target.recordingKey}),transaction=store.transaction.bind(store);
  for(const mode of ['healthy','rollback','lost-ack']){
   const id=randomUUID(),directory=await mkdtemp(join(tmpdir(),'harbor-pg-atomic-buffer-'));directories.push(directory);await store.run("INSERT INTO harbor.requests(id,member_id,key_id,started,status,ip,agent,model,billing_model,recording_state) VALUES($1,$2,$3,$4,'running','synthetic','synthetic','gpt-6.1-sol','gpt-6.1-sol','pending')",id,owner.member_id,owner.key_id,Date.now());let first=true;
   const buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,atomicBatch:async operation=>{if(first&&mode==='rollback'){first=false;return transaction(async()=>{await operation();throw Error('synthetic-before-commit');});}const result=await transaction(operation);if(first&&mode==='lost-ack'){first=false;throw Error('synthetic-after-commit');}return result;}});buffers.push(buffer);
   assert.equal(buffer.start(id,'known'),true);assert.equal(buffer.append(id,[{sourceKey:'synthetic-user',kind:'user',provenance:'client',content:{text:'原子正文恢复样本',mode:'append'}}]),true);assert.equal(buffer.finish(id,{state:'complete'}),true);assert.equal(buffer.settle(id,{status:'ok',input:100,output:20,cached:0,usageKnown:true}),true);await buffer.flush();
   if(mode==='rollback'){assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',id)).status,'running');assert.equal((await store.get('SELECT COUNT(*)::int n FROM harbor.conversation_events WHERE request_id=$1',id)).n,0);}
   if(mode!=='healthy')assert.equal(buffer.status().files,4);else assert.equal(buffer.status().failures,0);
   await buffer.flush();assert.equal(buffer.status().queued,0);assert.equal(buffer.status().files,0);const row=await store.get('SELECT status,input,output,recording_state FROM harbor.requests WHERE id=$1',id);assert.deepEqual(row,{status:'ok',input:100,output:20,recording_state:'complete'});const body=await repository.read(id);assert.equal(body.events.length,1);assert.equal(body.events[0].content.text,'原子正文恢复样本');assert.equal((await store.get('SELECT COUNT(*)::int n FROM harbor.request_prices WHERE request_id=$1',id)).n,1);await buffer.close();
  }
 }finally{for(const buffer of buffers)await buffer.close();await target?.close();for(const directory of directories)await rm(directory,{recursive:true,force:true});}
});
