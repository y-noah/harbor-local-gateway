import test from 'node:test';
import assert from 'node:assert/strict';
import {replayableResponsesInput,replayableResponsesEvent} from '../responses-replay-context.mjs';

test('plain text and client-replayed function calls retain context without rewriting any payload',()=>{
  const value={model:'synthetic',instructions:'system',input:[{role:'user',content:[{type:'input_text',text:'synthetic'}]},{type:'function_call',call_id:'call',name:'tool',arguments:'{}'},{type:'function_call_output',call_id:'call',output:'synthetic result'}],tools:[{type:'function',name:'tool',parameters:{type:'object'}}]},before=structuredClone(value);
  assert.equal(replayableResponsesInput(value),true);assert.deepEqual(value,before);
  assert.equal(replayableResponsesEvent({type:'response.output_item.done',item:{type:'function_call',call_id:'call',name:'tool',arguments:'{}'}}),true);
  assert.equal(replayableResponsesEvent({type:'response.completed',response:{output:[{type:'message',content:[{type:'output_text',text:'synthetic'}]}]}}),true);
});

test('encrypted reasoning, upstream files, hosted tools and unknown future item types remain unsupported',()=>{
  for(const input of [[{type:'reasoning',encrypted_content:'opaque'}],[{role:'user',content:[{type:'input_file',file_id:'file-opaque'}]}],[{type:'future_item'}],[{content:'missing role'}],[{type:'function_call_output',call_id:'call',output:[{type:'input_file',file_id:'file-opaque'}]}]])assert.equal(replayableResponsesInput({input}),false);
  for(const extra of [{conversation:'server-state'},{prompt:{id:'stored'}},{previous_response_id:'old'},{tools:[{type:'web_search'}]},{tools:'invalid'},{instructions:{file_id:'opaque'}}])assert.equal(replayableResponsesInput({input:'text',...extra}),false);
  for(const event of [{type:'response.reasoning.delta',delta:'opaque'},{type:'response.output_item.done',item:{type:'reasoning',encrypted_content:'opaque'}},{type:'response.completed',response:{output:[{type:'computer_call'}]}},{type:'response.content_part.done',part:{type:'audio'}},{type:'response.future_event'}])assert.equal(replayableResponsesEvent(event),false);
});
