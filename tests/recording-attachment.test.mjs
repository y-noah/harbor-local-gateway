import test from 'node:test';
import assert from 'node:assert/strict';
import {createHmac,randomBytes} from 'node:crypto';
import {recordingAttachment} from '../recording-attachment.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {captureRequestContext} from '../recording-request-context.mjs';
const fingerprint=bytes=>createHmac('sha256',Buffer.alloc(32,7)).update(bytes).digest('hex');
const options={fingerprint,maxContentBytes:1024};
test('附件只声明实际可见事实，不把引用或客户端摘要当成内容验证',()=>{
 const value=recordingAttachment({type:'input_file',file_url:'https://private.example/secret',sha256:'untrusted',size:9});
 assert.equal(value.filename,null);assert.equal(value.mime_type,null);assert.equal(value.bytes,9);assert.equal(value.sizeSource,'client_declared');assert.equal(value.referencePresent,true);assert.equal(value.fingerprint.state,'unavailable');
 assert.ok(!JSON.stringify(value).includes('private.example'));assert.ok(!JSON.stringify(value).includes('untrusted'));
});
test('规范化实际附件字节并标注声明大小，三种协议形状不保存附件原文',()=>{
 const body=Buffer.from('synthetic file content'),encoded=body.toString('base64');
 for(const part of [{type:'input_file',file_data:'data:text/plain;base64,'+encoded,size:999},{type:'file',file:{file_data:encoded,size:999}},{type:'input_audio',input_audio:{data:encoded,size:999}},{type:'image_url',image_url:{url:'data:image/png;base64,'+encoded},size:999}]){
  const result=recordingAttachment(part,options);assert.equal(result.fingerprint.value,fingerprint(body));assert.equal(result.fingerprint.source,'received_bytes');assert.equal(result.bytes,body.length);assert.equal(result.declaredBytes,999);assert.equal(result.sizeSource,'received_bytes');assert.equal(result.referencePresent,false);
  assert.ok(!JSON.stringify(result).includes(encoded));assert.ok(!JSON.stringify(result).includes(body.toString()));
 }
 assert.equal(recordingAttachment({type:'input_file',file_data:'data:text/plain;base64,'+encoded},options).mime_type,'text/plain');
});
test('非法、非规范、超限与不支持编码不伪造指纹，也不会调用摘要器',()=>{
 for(const [file_data,reason] of [['YQ=','invalid_encoding'],['YR==','invalid_encoding'],['YQ==\n','invalid_encoding'],['data:text/plain,abc','unsupported_encoding'],['A'.repeat(4096),'size_limit'],['data:'+'x'.repeat(4096),'size_limit']]){
  let calls=0;const result=recordingAttachment({type:'input_file',file_data},{...options,fingerprint:()=>{calls++;return 'a'.repeat(64);}});assert.equal(result.fingerprint.reason,reason);assert.equal(calls,0);
 }
});
test('附件指纹绑定服务端请求，重启稳定，不跨请求或密钥关联',()=>{
 const key=randomBytes(32),first=new RecordingContextStore({}, {encryptionKey:key}),second=new RecordingContextStore({}, {encryptionKey:key}),body=Buffer.from('synthetic');
 assert.equal(first.attachmentFingerprint('request-a',body),second.attachmentFingerprint('request-a',body));assert.notEqual(first.attachmentFingerprint('request-a',body),first.attachmentFingerprint('request-b',body));assert.notEqual(first.attachmentFingerprint('request-a',body),new RecordingContextStore({}, {encryptionKey:randomBytes(32)}).attachmentFingerprint('request-a',body));assert.throws(()=>first.attachmentFingerprint('',body));
});
test('整请求共享附件解码预算，超限保留元数据，不改变推理原请求',()=>{
 const file={type:'input_file',filename:'synthetic.txt',file_data:Buffer.alloc(700,1).toString('base64')},payload={input:[{role:'user',content:[file,file]}]},before=JSON.stringify(payload);
 const result=captureRequestContext(payload,{maxBytes:1024,attachmentFingerprint:fingerprint});assert.equal(result.bytes<=1024,true);assert.equal(JSON.stringify(payload),before);
 // Observe inspection directly as metadata itself can consume the output budget.
 let consumed=0;recordingAttachment(file,{...options,onInspect:n=>{consumed+=n;}});assert.equal(consumed,702);
 const large={type:'input_file',file_data:Buffer.alloc(3000,1).toString('base64')},limited=captureRequestContext({input:[{role:'user',content:[large,large]}]},{maxBytes:4096,attachmentFingerprint:fingerprint});
 assert.equal(limited.components[0].content.value.content[0].fingerprint.state,'verified');assert.equal(limited.components[0].content.value.content[1].fingerprint.reason,'size_limit');
 const roomy=captureRequestContext(payload,{maxBytes:4096,attachmentFingerprint:fingerprint});assert.equal(roomy.components[0].content.value.content[0].fingerprint.state,'verified');assert.equal(roomy.components[0].content.value.content[1].fingerprint.state,'verified');
});
test('工具附件与消息附件使用相同摘要机制，原stdout字符串契约不变',()=>{
 const part={type:'input_file',file_data:'YQ=='},result=captureRequestContext({input:[{type:'custom_tool_call_output',output:[part]},{role:'user',content:[part]}]},{attachmentFingerprint:fingerprint});
 assert.equal(result.components[0].content.value.output[0].fingerprint.value,result.components[1].content.value.content[0].fingerprint.value);
});
