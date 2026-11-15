import test from 'node:test';
import assert from 'node:assert/strict';
import {captureRequestContext} from '../recording-request-context.mjs';
import {safeToolOutput,ConversationCapture} from '../conversation-capture.mjs';
import {sanitizeRecord,filteredRecordField,isFilteredRecord} from '../record-redaction.mjs';

test('只有context过滤产物保留schema参数定义，原始同形数据不能伪造可信provenance',()=>{
  const raw={type:'function',name:'exec',parameters:{type:'object',properties:{api_key:{type:'string',default:'opaque-fixture-secret'},path:{type:'string'}}}},view=captureRequestContext({tools:[raw]}),prepared=view.components[0].content.value;
  assert.equal(isFilteredRecord(raw),false);assert.equal(isFilteredRecord({...raw,isFilteredRecord:true}),false);assert.equal(isFilteredRecord(prepared),true);assert.equal(Object.isFrozen(prepared),true);
  let clipped=false;const options={maxBytes:4096,onTruncate:()=>{clipped=true;}},safe=safeToolOutput(prepared,options);assert.equal(safe.parameters.properties.api_key.type,'string');assert.ok(!JSON.stringify(safe).includes('opaque-fixture-secret'));assert.equal(clipped,false);
  const ordinary=safeToolOutput(raw,options);assert.equal(ordinary.parameters.properties.api_key,'[已脱敏]');assert.ok(!JSON.stringify(ordinary).includes('opaque-fixture-secret'));
  safeToolOutput(captureRequestContext({tools:[{type:'function',name:'exec',description:'visible '.repeat(200)}]}).components[0].content.value,{maxBytes:256,onTruncate:()=>{clipped=true;}});assert.equal(clipped,true);
});

test('数组只尊重内部已过滤且不可变的文本对象，原始文本对象仍按既有规则过滤',()=>{
  const prepared=filteredRecordField({type:'text',severity:'fatal',exit_code:2},'text','visible'),safe=sanitizeRecord([prepared]);assert.deepEqual(safe,[{type:'text',severity:'fatal',exit_code:2,text:'visible'}]);assert.equal(isFilteredRecord(prepared),true);assert.equal(Object.isFrozen(prepared),true);
  assert.deepEqual(sanitizeRecord([{type:'text',text:'visible',severity:'fatal',exit_code:2}]),[{type:'text',text:'visible'}]);
  let clipped=false;sanitizeRecord([filteredRecordField({type:'text'},'text','visible '.repeat(100))],{maxBytes:128,onTruncate:()=>{clipped=true;}});assert.equal(clipped,true);
});

test('MCP通用错误文本保留业务字段，额外敏感字段和媒体仍先过滤',()=>{
  const events=[],capture=new ConversationCapture({emit:event=>events.push(event)}),result=capture.finish({output:[{type:'mcp_call',id:'mcp',name:'exec',server_label:'server',arguments:'{}',output:null,status:'failed',error:{type:'mcp_tool_execution_error',content:[{type:'text',text:'visible diagnostic',severity:'fatal',exit_code:2,api_key:'opaque-fixture-secret',attachment:{type:'input_image',image_url:'data:image/png;base64,PRIVATE_IMAGE'}}]}}]});
  assert.equal(result.incomplete,false);const value=events[0].content.error.content[0];assert.equal(value.severity,'fatal');assert.equal(value.exit_code,2);assert.equal(value.api_key,'[已脱敏]');assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'));assert.ok(!JSON.stringify(events).includes('PRIVATE_IMAGE'));
});
