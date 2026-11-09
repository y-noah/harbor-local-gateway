import test from 'node:test';
import assert from 'node:assert/strict';
import {RecordRedactor,redactRecordText,sanitizeRecord,redactRecordParts,recordRedactionSummary,validRecordRedaction} from '../record-redaction.mjs';
import {JsonRecordRedactor} from '../record-json.mjs';
import {ConversationCapture} from '../conversation-capture.mjs';
import {captureRequestContext} from '../recording-request-context.mjs';

test('脱敏事实来自实际过滤点，用户写占位词不产生假事实',()=>{
 for(const raw of ['[已脱敏] [附件数据已省略]','普通输出','key这个词本身不是凭据']){
  const categories=new Set();assert.equal(redactRecordText(raw,{onRedact:category=>categories.add(category)}),redactRecordText(raw));assert.deepEqual([...categories],[]);
 }
 const categories=new Set(),safe=sanitizeRecord({password:{nested:'synthetic-secret'},text:'plain'}, {onRedact:category=>categories.add(category)});
 assert.deepEqual(safe,{password:'[已脱敏]',text:'plain'});assert.deepEqual([...categories],['sensitive_field']);
});
test('流式脱敏在任意分片位置保持输出及事实一致，不把内容送入回调',()=>{
 for(const [raw,category] of [['sk-synthetic-credential','credential'],['password="synthetic secret"','sensitive_field'],['Authorization: \\synthetic','sensitive_field'],['Cookie: \\synthetic','sensitive_field'],['-----BEGIN PRIVATE KEY-----\nSYNTHETIC\n-----END PRIVATE KEY-----','private_key'],['data:image/png;base64,YWJj','attachment_data']]){
  for(let index=0;index<=raw.length;index++){
   const values=[],filter=new RecordRedactor({onRedact:value=>values.push(value)}),safe=filter.push(raw.slice(0,index))+filter.push(raw.slice(index))+filter.finish();assert.equal(safe,redactRecordText(raw));assert.ok(values.includes(category));assert.ok(values.every(value=>['credential','sensitive_field','private_key','attachment_data'].includes(value)));
  }
 }
 const seen=new Set();redactRecordParts(['sk-','synthetic'],{onRedact:category=>seen.add(category)});assert.ok(seen.has('credential'));
});
test('JSON敏感字段与编码凭据保留准确类别，已过滤对象不重复推算次数',()=>{
 const categories=new Set(),raw='{"pass\\u0077ord":{"a":"secret"},"value":"sk-synthetic"}',filter=new JsonRecordRedactor({onRedact:category=>categories.add(category)}),safe=filter.push(raw)+filter.finish();assert.deepEqual(JSON.parse(safe),{password:'[已脱敏]',value:'[已脱敏]'});assert.deepEqual([...categories].sort(),['credential','sensitive_field']);
 const observed=[];sanitizeRecord(sanitizeRecord({password:'synthetic'}),{onRedact:value=>observed.push(value)});assert.deepEqual(observed,[]);
});
test('本轮覆盖摘要独立记录脱敏与截断，不声称脱敏次数或原文长度',()=>{
 const events=[],collector=new ConversationCapture({emit:event=>events.push(event)});collector.input({input:'[已脱敏]'});collector.delta('a','assistant',{},'sk-');collector.complete('a','assistant',{},'sk-synthetic');const result=collector.finish();assert.equal(result.incomplete,false);assert.deepEqual(result.coverage.redaction,{version:1,policy:'credentials-v1',categories:['credential']});assert.ok(!JSON.stringify({events,result}).includes('sk-synthetic'));
 const plain=new ConversationCapture({emit:()=>true});plain.input({input:'[已脱敏]'});assert.deepEqual(plain.finish().coverage.redaction.categories,[]);
 const limited=new ConversationCapture({emit:()=>true,maxBytes:256});limited.input({input:'visible '.repeat(200)});const truncated=limited.finish();assert.equal(truncated.incomplete,true);assert.ok(truncated.coverage.reasons.includes('size_limit'));assert.deepEqual(truncated.coverage.redaction.categories,[]);
});
test('上下文工具schema、字面量、工具回传和附件的脱敏事实汇总',()=>{
 const result=captureRequestContext({tools:[{type:'function',name:'f',parameters:{properties:{password:{default:'private'},value:{default:{api_key:'private'}}}}}],input:[{role:'user',content:[{type:'input_file',file_data:'YQ=='}]},{type:'custom_tool_call_output',call_id:'c',output:[{type:'input_text',text:'sk-'},{type:'input_text',text:'synthetic'}]}]});
 assert.equal(result.state,'complete');assert.deepEqual(result.redaction.categories,['attachment_data','credential','sensitive_field']);assert.ok(validRecordRedaction(result.redaction));assert.deepEqual(captureRequestContext({input:'[已脱敏]'}).redaction,recordRedactionSummary([]));
 for(const value of [null,{}, {...result.redaction,categories:['untrusted']},{...result.redaction,categories:['credential','credential']},{...result.redaction,body:'untrusted'}])assert.equal(validRecordRedaction(value),false);
});

test('反斜杠开头的敏感头值在上下文摘要中保留实际脱敏事实',()=>{
 for(const name of ['Authorization','Cookie']){
  const raw=name+': '+String.fromCharCode(92)+'synthetic',view=captureRequestContext({input:raw});assert.deepEqual(view.redaction.categories,['sensitive_field']);assert.equal(view.components[0].content.value,redactRecordText(raw));assert.ok(!JSON.stringify(view).includes('synthetic'));
 }
});
