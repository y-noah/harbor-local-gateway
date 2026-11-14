import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,createDecipheriv,hkdfSync} from 'node:crypto';
import {mkdtemp,readdir,readFile,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';

test('billing recovery has independent disk/queue/encryption and survives a corrupt saturated recording spool',async t=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-metering-')),key=randomBytes(32),calls=[];let unavailable=true;
  const store={finish:async(...args)=>{if(unavailable)throw Error('synthetic DB outage');calls.push(args);}};
  const recordings=new RecordingStore(store,{encryptionKey:key});recordings.start=async()=>{throw Error('synthetic recording outage');};
  const buffer=await RecordingBuffer.create(recordings,{directory:join(root,'recording'),flushMs:10000,maxFiles:1});
  let recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory:join(root,'metering'),flushMs:10000});
  t.after(async()=>{await recovery.close();await buffer.close();await rm(root,{recursive:true,force:true});});
  buffer.start('recording-one','known');buffer.start('recording-two','known');await buffer.flush();assert.equal(buffer.status().files,1);assert.equal(buffer.status().queued,1);
  const recordFile=(await readdir(buffer.directory))[0];await writeFile(join(buffer.directory,recordFile),Buffer.alloc(40),{mode:0o600});
  const at=123456789,result={status:'ok',input:123,output:45,cached:20,usageKnown:true,actualModel:'gpt-6.1-sol',upstreamRequestId:'req_synthetic_recovery',response:{text:'NEVER-PERSIST-REPLY'},error:'NEVER-PERSIST-ERROR'};
  assert.equal(recovery.settle('synthetic-request',result,at),true);await recovery.flush();assert.equal(recovery.status().queued,0);assert.equal(recovery.status().files,1);assert.equal(calls.length,0);
  const file=(await readdir(recovery.directory))[0],bytes=await readFile(join(recovery.directory,file));
  assert.ok(!bytes.includes(Buffer.from('synthetic-request')));assert.ok(!bytes.includes(Buffer.from('NEVER-PERSIST')));
  assert.throws(()=>recordings.open(bytes,file));
  const saved=recovery.buffer.repository.open(bytes,file);assert.deepEqual(saved,{type:'settle',requestId:'synthetic-request',at,result:{status:'ok',input:123,output:45,cached:20,usageKnown:true,actualModel:'gpt-6.1-sol',upstreamRequestId:'req_synthetic_recovery'}});
  await recovery.close();unavailable=false;recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory:recovery.directory,flushMs:10000});
  await recovery.flush();assert.equal(recovery.status().files,0);assert.deepEqual(calls,[['synthetic-request',saved.result,at]]);
  await buffer.flush();assert.ok(buffer.status().files>0);assert.ok(buffer.status().queued>0);assert.deepEqual(calls,[['synthetic-request',saved.result,at]]);
});

test('billing recovery exposes bounded persistence failure, rejects invalid identities and never substitutes malformed usage for known zero',async t=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-metering-quota-')),calls=[];let unavailable=true;
  const recovery=await MeteringRecovery.create({finish:async(...args)=>{if(unavailable)throw Error('synthetic outage');calls.push(args);}},{encryptionKey:randomBytes(32),directory:join(root,'metering'),maxMemoryBytes:1024,maxDiskBytes:1024,maxCommands:2,maxFiles:1,flushMs:10000});
  t.after(async()=>{await recovery.close();await rm(root,{recursive:true,force:true});});
  assert.throws(()=>recovery.settle('invalid/id',{input:1}),{code:'metering_recovery_invalid'});
  assert.throws(()=>recovery.settle('one',{status:'nonterminal'}),{code:'metering_recovery_invalid'});
  assert.equal(recovery.settle('one',{input:10,output:1,cached:11,upstreamRequestId:'hg_synthetic_secret'},123),true);
  assert.equal(recovery.settle('two',{input:0,output:0,cached:0,usageKnown:true},124),true);
  assert.equal(recovery.settle('three',{input:0,output:0},125),false);
  let state=await recovery.flush();assert.equal(state.files,1);assert.equal(state.queued,1);assert.ok(state.diskBytes<=1024);assert.ok(state.memoryBytes<=1024);
  unavailable=false;state=await recovery.flush();assert.equal(state.files,0);assert.equal(state.queued,0);assert.equal(calls.length,2);
  assert.deepEqual(calls[0],['one',{status:'ok',input:0,output:0,cached:0,usageKnown:false},123]);assert.equal(calls[1][1].usageKnown,true);
});


test('Chat critical files use a fenced command type while new readers preserve legacy billing files',async t=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-chat-format-')),key=randomBytes(32),recovery=await MeteringRecovery.create({finish:async()=>{throw Error('synthetic unavailable');}},{encryptionKey:key,directory:root,flushMs:10000});t.after(async()=>{await recovery.close();await rm(root,{recursive:true,force:true});});
 const chat={accountEpoch:0,dispatched:true,upstreamId:'synthetic-thread',cursor:{messageCount:2,historyDigest:'a'.repeat(64),cumulative:{input:1,output:0,cached:0}}};assert.equal(recovery.settle('synthetic-chat',{input:1,usageKnown:true,chat}),true);await recovery.flush();const file=(await readdir(root))[0],bytes=await readFile(join(root,file)),cipher=createDecipheriv('aes-256-gcm',Buffer.from(hkdfSync('sha256',key,Buffer.alloc(0),'Harbor metering recovery v1',32)),bytes.subarray(0,12));cipher.setAAD(Buffer.from('metering:'+file));cipher.setAuthTag(bytes.subarray(-16));const raw=JSON.parse(Buffer.concat([cipher.update(bytes.subarray(12,-16)),cipher.final()]).toString('utf8'));assert.equal(raw.type,'settle-chat','older readers require type=settle and must reject this file');const opened=recovery.buffer.repository.open(bytes,file);assert.equal(opened.type,'settle');assert.equal(opened.result.chat.cursor.messageCount,2);
});


test('optional stream summary survives encrypted outage replay while malformed telemetry cannot break billing',async t=>{
 const {createRequestStreamTiming}=await import('../request-stream-timing.mjs');
 const timing=createRequestStreamTiming();timing.bodyEnd();timing.dispatch();timing.headers();timing.rawChunk(20);timing.decodedEvent();timing.transportEnd();timing.responseEnd();const summary=timing.finish({outcome:'ok'});
 const root=await mkdtemp(join(tmpdir(),'harbor-timing-metering-')),key=randomBytes(32),calls=[];let unavailable=true;
 const store={finish:async(...args)=>{if(unavailable)throw Error('synthetic outage');calls.push(args);}};
 let recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory:root,flushMs:10000});
 t.after(async()=>{await recovery.close();await rm(root,{recursive:true,force:true});});
 assert.equal(recovery.settle('synthetic-stream',{input:7,output:3,cached:1,streamTiming:summary},123),true);await recovery.flush();
 const file=(await readdir(root))[0],bytes=await readFile(join(root,file));assert.ok(!bytes.includes(Buffer.from('process-monotonic')));
 assert.deepEqual(recovery.buffer.repository.open(bytes,file).result.streamTiming,summary);
 await recovery.close();unavailable=false;recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory:root,flushMs:10000});await recovery.flush();
 assert.equal(calls.length,1);assert.deepEqual(calls[0][1].streamTiming,summary);assert.equal(calls[0][1].input,7);
 assert.equal(recovery.settle('synthetic-invalid',{input:2,streamTiming:{...summary,prompt:'NEVER-PERSIST'}},124),true);await recovery.flush();
 assert.equal(calls.length,2);assert.equal(calls[1][1].input,2);assert.equal(calls[1][1].streamTiming,undefined);
});

test('trusted quota rejection observation survives encrypted metering outage and replay',async t=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-denial-metering-')),key=randomBytes(32),calls=[];let unavailable=true;
  const store={finish:async(...args)=>{if(unavailable)throw Error('synthetic outage');calls.push(args);}};
  let recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory:root,flushMs:10000});
  t.after(async()=>{await recovery.close();await rm(root,{recursive:true,force:true});});
  assert.equal(recovery.settle('synthetic-denial',{status:'rejected',usageKnown:true,quotaSubjectDenial:true,error:'NEVER-PERSIST'},123),true);await recovery.flush();
  const file=(await readdir(root))[0],saved=recovery.buffer.repository.open(await readFile(join(root,file)),file);assert.equal(saved.result.quotaSubjectDenial,true);assert.equal(saved.result.error,undefined);
  await recovery.close();unavailable=false;recovery=await MeteringRecovery.create(store,{encryptionKey:key,directory:root,flushMs:10000});await recovery.flush();assert.equal(calls.length,1);assert.equal(calls[0][1].quotaSubjectDenial,true);assert.equal(calls[0][1].usageKnown,true);
  assert.throws(()=>recovery.settle('synthetic-invalid-denial',{quotaSubjectDenial:'untrusted'},124),{code:'metering_recovery_invalid'});
});
