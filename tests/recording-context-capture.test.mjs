import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {recordingPolicyBuffer} from '../postgres-recording-storage.mjs';
import {createRequestContextCapture,observeRequestContext,observeRequestContextAsync,incompleteRequestContext} from '../recording-context-capture.mjs';

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
 for(const count of [1363,2000,4096,5000]){
  const {buffer,saved}=await fixture(t,{maxBytes:2*1024*1024,maxMemoryBytes:4*1024*1024}),payload={input:Array.from({length:count},()=>({role:'user',content:'合成短消息'}))},result=createRequestContextCapture(buffer)('synthetic-request',payload);
  assert.equal(result.accepted,true);assert.equal(buffer.status().diagnostics.dropReasons.sanitize,0);assert.equal(buffer.status().dropped,0);const command=buffer.queue[0].value;
  if(count<=4096){assert.equal(result.state,'complete');assert.equal(command.components.length,count);}else{assert.equal(result.state,'incomplete');assert.equal(command.components.length,4096);assert.ok(command.coverage.reasons.includes('size_limit'));}
  await buffer.flush();assert.equal(saved.length,1);assert.equal(buffer.status().files,0);
 }
});

test('协作采集完整进入真实加密缓冲，等待结果后才报告成功',async t=>{
 const {buffer,saved}=await fixture(t,{maxBytes:2*1024*1024,maxMemoryBytes:4*1024*1024}),capture=createRequestContextCapture(buffer,{cooperative:true}),input=Array.from({length:2000},(_,index)=>({role:'user',content:'合成历史 '+index}));
 const result=await observeRequestContextAsync(capture,'synthetic-cooperative',{input});assert.deepEqual(result,{accepted:true,state:'complete',reason:null});
 await buffer.flush();assert.equal(saved.length,1);assert.equal(saved[0][1].length,input.length);assert.equal(saved[0][2].coverage.state,'complete');
});

test('协作采集中取消或许可关闭不入队，不误报完整记录',async t=>{
 for(const cancel of [true,false]){
  const {buffer}=await fixture(t,{maxBytes:2*1024*1024,maxMemoryBytes:4*1024*1024});let allowed=true;
  const policy=recordingPolicyBuffer(buffer,{snapshot:()=>({captureAllowed:allowed,status:allowed?'ok':'low'})}),capture=createRequestContextCapture(buffer,{policyBuffer:policy,cooperative:true}),controller=new AbortController();
  const interrupt=new Promise(resolve=>setImmediate(()=>{if(cancel)controller.abort();else allowed=false;resolve();}));
  const result=await observeRequestContextAsync(capture,'synthetic-cancel',{input:Array.from({length:4096},()=>({role:'user',content:'合成历史'}))},{signal:controller.signal});await interrupt;
  assert.equal(result.accepted,false);assert.equal(result.reason,cancel?'capture_failed':'storage_low');assert.equal(buffer.status().queued,0);
 }
});

test('异步旁路兼容同步结果，拒绝无效结果并消化异步异常',async()=>{
 assert.equal(await observeRequestContextAsync(undefined,'synthetic',{}),null);
 const outcome={accepted:true,state:'complete',reason:null};assert.deepEqual(await observeRequestContextAsync(()=>outcome,'synthetic',{}),outcome);
 for(const capture of [async()=>{throw Error('private-error');},async()=>({accepted:true,state:'complete',reason:'private-error'})])assert.deepEqual(await observeRequestContextAsync(capture,'synthetic',{}),{accepted:false,state:'incomplete',reason:'capture_failed'});
});

test('真实加密缓冲透传服务端请求限定的附件指纹，不保存附件字节',async t=>{
 const {buffer,saved}=await fixture(t,{maxBytes:4096,maxMemoryBytes:8192});
 const {RecordingContextStore}=await import('../recording-context-store.mjs'),context=new RecordingContextStore({}, {encryptionKey:randomBytes(32)});
 buffer.repository.contextRepository.attachmentFingerprint=context.attachmentFingerprint.bind(context);
 const body=Buffer.from('synthetic attachment bytes'),payload={input:[{role:'user',content:[{type:'input_file',file_data:body.toString('base64')}]}]};
 const result=createRequestContextCapture(buffer)('synthetic-attachment-request',payload);assert.equal(result.accepted,true);assert.equal(result.state,'complete');await buffer.flush();
 const attachment=saved[0][1][0].content.value.content[0];assert.equal(attachment.fingerprint.value,context.attachmentFingerprint('synthetic-attachment-request',body));assert.equal(attachment.bytes,body.length);assert.ok(!JSON.stringify(saved).includes(body.toString('base64')));
});

test('脱敏覆盖事实随真实spool保存，覆盖摘要计入队列预算',async t=>{
 const {buffer,saved}=await fixture(t,{maxBytes:4096,maxMemoryBytes:8192}),outcome=createRequestContextCapture(buffer)('synthetic-redaction',{input:'sk-synthetic-audit'});assert.equal(outcome.accepted,true);await buffer.flush();assert.deepEqual(saved[0][2].coverage.redaction,{version:1,policy:'credentials-v1',categories:['credential']});assert.ok(!JSON.stringify(saved).includes('sk-synthetic-audit'));
});
