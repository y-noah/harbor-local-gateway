import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,readdir,readFile,rm,writeFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes,randomUUID} from 'node:crypto';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';

async function fixture(t,options={}){
  const directory=await mkdtemp(join(tmpdir(),'harbor-record-buffer-')),repository=new RecordingStore({},{encryptionKey:randomBytes(32)}),calls=[];let unavailable=false;
  repository.start=async(...args)=>{if(unavailable)throw Error('injected database outage');calls.push(['start',...args]);};
  repository.append=async(...args)=>{if(unavailable)throw Error('injected database outage');calls.push(['append',...args]);};
  repository.finish=async(...args)=>{if(unavailable)throw Error('injected database outage');calls.push(['finish',...args]);};
  const buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,...options});
  t.after(async()=>{await buffer.close();await rm(directory,{recursive:true,force:true});});return {buffer,repository,directory,calls,outage:value=>{unavailable=value;}};
}
test('recording buffer persists encrypted sanitized commands and replays in order after restart',async t=>{
  const f=await fixture(t);f.outage(true);assert.equal(f.buffer.start('fixture-request','known'),true);
  assert.equal(f.buffer.append('fixture-request',[{sourceKey:'one',kind:'user',provenance:'client',content:{text:'问题 Authorization: Bearer opaque-secret'}}]),true);
  assert.equal(f.buffer.finish('fixture-request',{state:'complete'}),true);
  let state=await f.buffer.flush();assert.equal(state.files,3);assert.equal(state.queued,0);assert.equal(f.calls.length,0);
  for(const name of await readdir(f.directory)){const bytes=await readFile(join(f.directory,name));assert.ok(!bytes.includes(Buffer.from('opaque-secret')));assert.ok(!bytes.includes(Buffer.from('fixture-request')));}
  await f.buffer.close();f.outage(false);
  const restored=await RecordingBuffer.create(f.repository,{directory:f.directory,flushMs:10000});
  try{state=await restored.flush();assert.equal(state.files,0);assert.deepEqual(f.calls.map(c=>c[0]),['start','append','finish']);assert.ok(!JSON.stringify(f.calls).includes('opaque-secret'));assert.ok(JSON.stringify(f.calls).includes('已脱敏'));}
  finally{await restored.close();}
});
test('R14 complete pending files recover their original authenticated command and settlement time',async t=>{
  const f=await fixture(t);await f.buffer.close();const name='00000000000000000001-'+randomUUID()+'.record',at=123456789;
  f.repository.store.finish=async(...args)=>f.calls.push(['settle',...args]);
  await writeFile(join(f.directory,name+'.pending'),f.repository.seal({type:'settle',requestId:'fixture-request',result:{status:'ok',input:12,output:3,usageKnown:true},at},name),{mode:0o600});
  const restored=await RecordingBuffer.create(f.repository,{directory:f.directory,flushMs:10000});
  try{assert.equal(restored.status().files,1);await restored.flush();assert.equal(restored.status().files,0);assert.equal(f.calls[0][0],'settle');assert.equal(f.calls[0][3],at);assert.deepEqual(await readdir(f.directory),[]);}
  finally{await restored.close();}
});
test('R14 incomplete pending files count toward disk/file quotas and prevent a later false completion',async t=>{
  const f=await fixture(t);await f.buffer.close();const first='00000000000000000001-'+randomUUID()+'.record',later='00000000000000000002-'+randomUUID()+'.record';
  await writeFile(join(f.directory,first+'.pending'),Buffer.alloc(200),{mode:0o600});await writeFile(join(f.directory,later),f.repository.seal({type:'finish',requestId:'one',result:{state:'complete'}},later),{mode:0o600});
  const restored=await RecordingBuffer.create(f.repository,{directory:f.directory,maxFiles:2,maxDiskBytes:1024,flushMs:10000});
  try{assert.equal(restored.status().orphanFiles,1);assert.equal(restored.status().orphanBytes,200);assert.ok(restored.status().diskBytes>=200);restored.start('three','known');const status=await restored.flush();assert.equal(status.files,2);assert.equal(status.queued,1);assert.equal(f.calls.length,0);assert.equal((await readdir(f.directory)).length,2);}
  finally{await restored.close();}
});
test('buffer memory, command count and disk quotas remain bounded during storage outage',async t=>{
  const f=await fixture(t,{maxMemoryBytes:1024,maxDiskBytes:1024,maxCommands:2,maxFiles:1});f.outage(true);
  assert.equal(f.buffer.start('one','known'),true);assert.equal(f.buffer.start('two','known'),true);assert.equal(f.buffer.start('three','known'),false);
  const state=await f.buffer.flush();assert.equal(state.files,1);assert.equal(state.queued,1);assert.ok(state.memoryBytes<=1024);assert.ok(state.diskBytes<=1024);assert.equal(state.dropped,1);
  f.outage(false);await f.buffer.flush();assert.equal(f.buffer.status().queued,0);assert.equal(f.buffer.status().files,0);
});
test('corrupt spool is retained and blocks later completion instead of reporting success',async t=>{
  const f=await fixture(t);f.outage(true);f.buffer.start('one','known');f.buffer.finish('one',{state:'complete'});await f.buffer.flush();
  const first=(await readdir(f.directory)).sort()[0];const {writeFile}=await import('node:fs/promises');await writeFile(join(f.directory,first),Buffer.alloc(40));
  f.outage(false);const state=await f.buffer.flush();assert.equal(state.files,2);assert.equal(f.calls.length,0);assert.ok(state.failures>0);
});
