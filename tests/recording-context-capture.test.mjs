import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {recordingPolicyBuffer} from '../postgres-recording-storage.mjs';
import {createRequestContextCapture,observeRequestContext,incompleteRequestContext} from '../recording-context-capture.mjs';

async function fixture(t,{maxBytes=1024,maxMemoryBytes=4096}={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-context-capture-')),store={},saved=[],contexts={store,maxSnapshotBytes:Math.max(4096,maxBytes),maxComponents:4096,writeSnapshot:async(...args)=>saved.push(args)},repository=new RecordingStore(store,{encryptionKey:randomBytes(32),maxBytes,maxEventBytes:256,contextRepository:contexts}),buffer=await RecordingBuffer.create(repository,{directory,flushMs:10000,maxMemoryBytes});
 t.after(async()=>{await buffer.close();await rm(directory,{recursive:true,force:true});});return {buffer,saved};
}
test('上下文队列为真实命令封装预留预算，超限有可用前缀且不泄漏凭据',async t=>{
 const {buffer,saved}=await fixture(t),capture=createRequestContextCapture(buffer),outcome=capture('synthetic-request',{input:'Authorization: Bearer private-secret\n'+'中文🙂'.repeat(1000)});
 assert.equal(outcome.accepted,true);assert.equal(outcome.state,'incomplete');assert.equal(outcome.reason,'size_limit');assert.equal(incompleteRequestContext(outcome),true);assert.equal(buffer.queue.length,1);assert.ok(buffer.queue[0].bytes<=1024);assert.match(JSON.stringify(buffer.queue[0].value),/中文/);assert.ok(!JSON.stringify(buffer.queue[0].value).includes('private-secret'));await buffer.flush();assert.equal(saved.length,1);
});
test('低盘许可在上下文入队时复核，禁止时不入队',async t=>{
 const {buffer}=await fixture(t),policy=recordingPolicyBuffer(buffer,{snapshot:()=>({captureAllowed:false,status:'low'})}),capture=createRequestContextCapture(buffer,{policyBuffer:policy});
 assert.deepEqual(capture('synthetic-request',{input:'合成提问'}),{accepted:false,state:'complete',reason:'storage_low'});assert.equal(buffer.status().queued,0);
});
test('旁路异常、异步或非法结果只产生缺失状态，关闭时不调用适配器',async()=>{
 assert.equal(observeRequestContext(undefined,'synthetic',{}),null);assert.equal(incompleteRequestContext(null),false);
 for(const capture of [()=>{throw Error('synthetic failure');},()=>Promise.reject(Error('synthetic asynchronous failure')),()=>({accepted:true,state:'complete',reason:'queue_limit'}),()=>({accepted:true,state:'complete',reason:'private-error'})])assert.deepEqual(observeRequestContext(capture,'synthetic',{}),{accepted:false,state:'incomplete',reason:'capture_failed'});
 await new Promise(resolve=>setImmediate(resolve));assert.throws(()=>createRequestContextCapture({}),{code:'recording_configuration'});
});
test('大量短消息的已过滤组件不被缓冲再次裁掉，源预算超限仍可保存前缀',async t=>{
 for(const count of [1363,2000]){
  const {buffer,saved}=await fixture(t,{maxBytes:2*1024*1024,maxMemoryBytes:4*1024*1024}),payload={input:Array.from({length:count},()=>({role:'user',content:'合成短消息'}))},result=createRequestContextCapture(buffer)('synthetic-request',payload);
  assert.equal(result.accepted,true);assert.equal(buffer.status().diagnostics.dropReasons.sanitize,0);assert.equal(buffer.status().dropped,0);const command=buffer.queue[0].value;
  if(count===1363){assert.equal(result.state,'complete');assert.equal(command.components.length,count);}else{assert.equal(result.state,'incomplete');assert.ok(command.components.length>0&&command.components.length<count);assert.ok(command.coverage.reasons.includes('size_limit'));}
  await buffer.flush();assert.equal(saved.length,1);assert.equal(buffer.status().files,0);
 }
});
