import test from 'node:test';
import assert from 'node:assert/strict';
import {JsonRecordRedactor} from '../record-json.mjs';
import {ConversationCapture} from '../conversation-capture.mjs';

test('JSON filtering preserves ordinary values, nesting, string escapes and Unicode across all split points',()=>{
  const source={items:[0,1,true,false,null,{nested:'中文😀\n"quoted"\\path\tend'}],name:'visible'},raw=JSON.stringify(source);
  for(let split=0;split<=raw.length;split++){const filter=new JsonRecordRedactor();const safe=filter.push(raw.slice(0,split))+filter.push(raw.slice(split))+filter.finish();assert.deepEqual(JSON.parse(safe),source);assert.equal(filter.finish(),'');}
});
test('JSON filtering decodes escaped field names before checking sensitive values',()=>{
  const raw='{"\\u0061uthorization":{"nested":[1,2]},"path":"visible.txt"}';
  for(let split=0;split<=raw.length;split++){const filter=new JsonRecordRedactor();assert.deepEqual(JSON.parse(filter.push(raw.slice(0,split))+filter.push(raw.slice(split))+filter.finish()),{authorization:'[已脱敏]',path:'visible.txt'});}
});
test('JSON structure limits truncate recording without throwing into model forwarding',()=>{
  for(const text of ['['.repeat(10000)+'0'+']'.repeat(10000),'{"'+'x'.repeat(10000)+'":"opaque"}']){
    const events=[],capture=new ConversationCapture({emit:event=>events.push(event)});assert.doesNotThrow(()=>capture.event({type:'response.function_call_arguments.delta',item_id:'tool',delta:text}));assert.equal(capture.finish().truncated,true);assert.ok(JSON.stringify(events).length<2048);
  }
  assert.throws(()=>new JsonRecordRedactor({maxDepth:0}));const filter=new JsonRecordRedactor();filter.finish();assert.throws(()=>filter.push('{}'));
});
