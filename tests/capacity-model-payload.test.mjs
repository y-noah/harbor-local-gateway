import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityModelPayload} from '../tools/capacity/model-payload.mjs';
import {captureRequestContext} from '../recording-request-context.mjs';
import {currentInput} from '../conversation-capture.mjs';

test('capacity JSON has the declared expanded bytes and exercises instructions, schema and history',()=>{
 for(const bytes of [1024,102400,1048576]){
  const {payload,input}=capacityModelPayload({model:'gpt-6.1-sol',trace:'synthetic-capacity-trace',bytes});
  assert.equal(Buffer.byteLength(JSON.stringify(payload)),bytes);
  assert.deepEqual(currentInput(payload).events.map(event=>event.content.text),[input],'history must not masquerade as a newly submitted question');
  const view=captureRequestContext(payload);assert.equal(view.state,'complete',view.reasons.join(','));assert.equal(view.components.length,5);
  assert.equal(view.components.filter(component=>component.kind==='instruction').length,1);
  assert.equal(view.components.filter(component=>component.kind==='tool_schema').length,1);
  const saved=JSON.stringify(view.components);assert.match(saved,/此前的合成问题/);assert.match(saved,/此前的合成回复/);assert.match(saved,/capacity_probe/);
 }
 const first=capacityModelPayload({model:'gpt-6.1-sol',trace:'synthetic-first'}),second=capacityModelPayload({model:'gpt-6.1-sol',trace:'synthetic-second'});
 assert.notEqual(first.input,second.input,'new requests must exercise unique context storage, not only shared-object cache hits');
 assert.deepEqual(first.payload.input.slice(0,-1),second.payload.input.slice(0,-1),'the stable history still exercises legitimate reuse');
 for(const options of [{bytes:1},{bytes:NaN},{bytes:0},{bytes:1.5},{model:1},{trace:null}])assert.throws(()=>capacityModelPayload({model:'gpt-6.1-sol',trace:'synthetic',...options}),/capacity_model_payload_/);
});
