import test from 'node:test';
import assert from 'node:assert/strict';
import {ConversationCapture} from '../conversation-capture.mjs';
import {assembleRecording} from '../recording-search-index.mjs';

test('代码解释器logs在首次字段过滤前连续脱敏Bearer和JSON凭据',()=>{
  for(const raw of ['Authorization: Bearer opaque-fixture-secret\nvisible tail','{"api_key":"opaque-fixture-secret","path":"visible tail"}'])for(let split=0;split<=raw.length;split++){
    const events=[],capture=new ConversationCapture({emit:event=>events.push(event)}),original={type:'code_interpreter_call',id:'code',container_id:'container',code:'print("visible")',status:'completed',outputs:[{type:'logs',logs:raw.slice(0,split)},{type:'image',url:'https://synthetic.invalid/PRIVATE_IMAGE'},{type:'logs',logs:raw.slice(split)}]},before=JSON.stringify(original);
    const result=capture.finish({output:[original]}),body=assembleRecording(events.map((event,index)=>({...event,sequence:index+1}))).find(item=>item.kind==='tool_result');
    assert.equal(result.incomplete,false,'split '+split);assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'),'split '+split);assert.ok(!JSON.stringify(events).includes('PRIVATE_IMAGE'));assert.ok(JSON.parse(body.text).filter(part=>part.type==='logs').map(part=>part.logs).join('').includes('visible tail'));assert.equal(JSON.stringify(original),before);
  }
});
