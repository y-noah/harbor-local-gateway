import test from 'node:test';
import assert from 'node:assert/strict';
import {ConversationCapture,currentInput} from '../conversation-capture.mjs';
import {redactRecordText,sanitizeRecord} from '../record-redaction.mjs';

function capture(options){const events=[],collector=new ConversationCapture({...options,emit:event=>events.push(event)});return {events,collector};}
function output(events){const fields=new Map();for(const {kind,content:c} of events)if(c.field&&kind!=='user')fields.set(c.field,c.mode==='replace'?c.text:(fields.get(c.field)||'')+c.text);return [...fields.values()].join('');}
test('current turn excludes previous context, instructions, schemas, reasoning and attachment contents',()=>{
  const payload={instructions:'private-background',tools:[{description:'private-schema'}],input:[{role:'system',content:'private-system'},{role:'user',content:'old-question'},{role:'assistant',content:'old-answer'},{role:'user',content:[{type:'input_text',text:'本轮问题'},{type:'input_image',image_url:'data:image/png;base64,PRIVATE'}]}]};
  const result=currentInput(payload);assert.equal(result.state,'uncertain');assert.equal(result.events.length,2);assert.ok(!JSON.stringify(result).includes('private'));assert.ok(!JSON.stringify(result).includes('old-'));assert.ok(!JSON.stringify(result).includes('PRIVATE'));
  assert.equal(currentInput({input:'重复问题'}).state,'known');
  assert.deepEqual(currentInput({input:[{role:'user',content:'previous'},{type:'function_call',arguments:'previous'},{type:'function_call_output',call_id:'call-1',output:'当前工具结果'}]}).events,[{kind:'tool_result',content:{callId:'call-1',output:'当前工具结果'}}]);
});
test('delta, item done and final response merge into one output without mutating protocol data',()=>{
  const {events,collector}=capture();collector.input({input:'重复问题'});
  for(const delta of ['你好','，世界'])collector.event({type:'response.output_text.delta',item_id:'msg',content_index:0,delta});
  const item={id:'msg',type:'message',content:[{type:'output_text',text:'你好，世界'}]},original=JSON.stringify(item);
  collector.event({type:'response.output_item.done',output_index:0,item});collector.finish({output:[item,{type:'reasoning',encrypted_content:'hidden'}]});
  assert.equal(output(events),'你好，世界');assert.equal(JSON.stringify(item),original);assert.ok(!JSON.stringify(events).includes('hidden'));
  const second=capture();second.collector.input({input:'重复问题'});assert.equal(second.events.length,1);
});
test('authoritative final output replaces missing deltas and final-only output is preserved',()=>{
  const {events,collector}=capture();collector.event({type:'response.output_text.delta',item_id:'msg',content_index:0,delta:'先前片段'});collector.finish({output:[{id:'msg',type:'message',content:[{type:'output_text',text:'完整回复'}]},{id:'other',type:'message',content:[{type:'refusal',refusal:'拒绝原因'}]}]});
  assert.equal(output(events),'完整回复拒绝原因');
});
test('R7 credentials remain filtered across every input content-block split and escaped JSON field name',()=>{
  for(const text of ['Authorization: Bearer opaque-fixture-secret','{"pass\\u0077ord":"opaque-fixture-secret"}','{"api\\u005fkey":"opaque-fixture-secret"}'])for(let split=0;split<=text.length;split++){
    const {events,collector}=capture();const payload={input:[{role:'user',content:[{type:'input_text',text:text.slice(0,split)},{type:'input_text',text:text.slice(split)}]}]},before=JSON.stringify(payload);
    collector.input(payload);collector.finish();assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'),'split '+split);assert.equal(JSON.stringify(payload),before);
  }
});
test('R7 tool message text parts share filtering before any emitted event',()=>{
  for(const text of ['Authorization: opaque-fixture-secret','Authorization: Bearer opaque-fixture-secret','sk-proj-opaque-fixture-secret'])for(let split=0;split<=text.length;split++){
    const raw=JSON.stringify({content:[{type:'text',text:text.slice(0,split)},{type:'text',text:text.slice(split)}]}),payload={input:[{type:'function_call_output',call_id:'call',output:raw}]},before=JSON.stringify(payload),{events,collector}=capture();
    collector.input(payload);collector.finish();assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'),'split '+split);assert.equal(JSON.stringify(payload),before);
  }
});
test('R9 input and structured traversal stop at the recording budget before processing a huge payload',()=>{
  const {events,collector}=capture({maxBytes:256});collector.input({input:'中'.repeat(1000000)});assert.equal(collector.finish().truncated,true);assert.ok(collector.rawBytes<=256);assert.ok(JSON.stringify(events).length<1024);
  let truncated=false;const safe=sanitizeRecord({nested:Array(100000).fill('large')},{maxBytes:128,maxNodes:8,onTruncate:()=>{truncated=true;}});assert.ok(truncated);assert.ok(safe.nested.length<=8);
  const parts=Array(10000).fill({type:'input_text',text:'short'});assert.ok(currentInput({input:[{role:'user',content:parts}]}).truncated);
});
test('R9 ignored content blocks consume the visit budget and mixed input retains only the visited prefix',()=>{
  let visited=0;const ignored=new Proxy(Array(10000).fill({type:'unknown'}),{get(target,key,receiver){if(/^\d+$/.test(String(key)))visited++;return Reflect.get(target,key,receiver);}});
  const result=currentInput({input:[{role:'user',content:ignored}]},{maxNodes:32});assert.ok(result.truncated);assert.equal(result.events.length,0);assert.ok(visited<=32);
  const mixed=[...Array(20).fill({type:'unknown'}),{type:'input_text',text:'visible'},...Array(100).fill({type:'unknown'})];const prefix=currentInput({input:[{role:'user',content:mixed}]},{maxNodes:32});assert.ok(prefix.truncated);assert.equal(prefix.events[0].content.text,'visible');
});
test('R10 stringified tool attachments are omitted structurally while ordinary text and source payload remain intact',()=>{
  const raw=JSON.stringify({content:[{type:'image',data:'SYNTHETIC_IMAGE',mimeType:'image/png'},{type:'audio',data:'SYNTHETIC_AUDIO'},{type:'text',text:'visible result'}]}),{events,collector}=capture();
  const payload={input:[{type:'function_call_output',call_id:'call',output:raw}]};collector.input(payload);collector.finish();const encoded=JSON.stringify(events);assert.ok(!encoded.includes('SYNTHETIC_IMAGE'));assert.ok(!encoded.includes('SYNTHETIC_AUDIO'));assert.ok(encoded.includes('visible result'));assert.equal(payload.input[0].output,raw);
});
test('R12 every tool-argument split yields the same filtered text after all defensive sanitization passes',()=>{
  const raw='{"password":"opaque-secret","path":"visible.txt"}',expected=redactRecordText(raw);
  for(let split=0;split<=raw.length;split++){
    const {events,collector}=capture();collector.event({type:'response.output_item.added',output_index:0,item:{id:'tool',type:'function_call',name:'lookup',call_id:'call'}});
    for(const delta of [raw.slice(0,split),raw.slice(split)])collector.event({type:'response.function_call_arguments.delta',output_index:0,item_id:'tool',delta});
    collector.finish();const safe=sanitizeRecord(sanitizeRecord(events));assert.equal(output(safe),expected,'split '+split);assert.doesNotThrow(()=>JSON.parse(output(safe)));
  }
});
test('R15 every JSON sensitive value type remains valid and preserves following harmless fields at every split',()=>{
  for(const name of ['password','authorization','proxy-authorization','cookie','set-cookie'])for(const value of [123456,null,true,false,{nested:'opaque-fixture-secret',items:[{deep:true}]},[1,{nested:'opaque-fixture-secret'}]]){
    const raw=JSON.stringify({[name]:value,path:'visible.txt'});
    for(let split=0;split<=raw.length;split++){
      const {events,collector}=capture();for(const delta of [raw.slice(0,split),raw.slice(split)])collector.event({type:'response.function_call_arguments.delta',item_id:'tool',delta});collector.finish();
      const safe=sanitizeRecord(sanitizeRecord(events));assert.deepEqual(JSON.parse(output(safe)),{[name]:'[已脱敏]',path:'visible.txt'},'split '+split);assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'));
    }
  }
});
test('R17 serialized safe tool results and streamed JSON string values survive defensive boundaries',()=>{
  const raw=JSON.stringify({content:[{type:'text',text:'Authorization: opaque-fixture-secret\n'},{type:'text',text:'visible tail'},{type:'image',data:'PRIVATE_BINARY'}]}),{events,collector}=capture();collector.input({input:[{type:'function_call_output',call_id:'call',output:raw}]});collector.finish();
  const content=JSON.parse(sanitizeRecord(sanitizeRecord(events))[0].content.output);assert.equal(content.content[1].text,'visible tail');assert.ok(content.content[2].content.includes('省略'));assert.ok(!JSON.stringify(content).includes('opaque-fixture-secret'));assert.ok(!JSON.stringify(content).includes('PRIVATE_BINARY'));
  const args=JSON.stringify({note:'Authorization: opaque-fixture-secret\nvisible tail',path:'visible.txt'});
  for(let split=0;split<=args.length;split++){
    const c=capture();for(const delta of [args.slice(0,split),args.slice(split)])c.collector.event({type:'response.function_call_arguments.delta',item_id:'tool',delta});c.collector.finish();
    const parsed=JSON.parse(output(sanitizeRecord(sanitizeRecord(c.events))));assert.equal(parsed.path,'visible.txt');assert.ok(parsed.note.includes('visible tail'));assert.ok(!parsed.note.includes('opaque-fixture-secret'));
  }
});
test('R18 sensitive JSON containers with PEM content emit exactly one replacement at every split',()=>{
  const pem='-----BEGIN PRIVATE KEY-----fixture-material-----END PRIVATE KEY-----';
  for(const value of [{pem},[pem,{nested:pem}]]){const raw=JSON.stringify({password:value,path:'visible.txt'});for(let split=0;split<=raw.length;split++){
    const {events,collector}=capture();for(const delta of [raw.slice(0,split),raw.slice(split)])collector.event({type:'response.function_call_arguments.delta',item_id:'tool',delta});collector.finish();assert.deepEqual(JSON.parse(output(sanitizeRecord(events))),{password:'[已脱敏]',path:'visible.txt'});
  }}
});
test('R16 deferred text is associated with its original content block around attachments',()=>{
  const payload={input:[{role:'user',content:[{type:'input_text',text:'hello'},{type:'input_image',image_url:'data:image/png;base64,PRIVATE'},{type:'input_text',text:'world'}]}]},before=JSON.stringify(payload),{events,collector}=capture();
  collector.input(payload);collector.finish();assert.deepEqual(events.map(event=>event.kind),['user','attachment','user']);assert.deepEqual(events.map(event=>event.content.position),[0,1,2]);assert.equal(events[0].content.text,'hello');assert.equal(events[2].content.text,'world');assert.equal(JSON.stringify(payload),before);
});
test('R13 tool identity survives interruption and item index/id aliases merge into one field',()=>{
  const {events,collector}=capture();collector.event({type:'response.output_item.added',output_index:1,item:{id:'tool',type:'function_call',name:'lookup',call_id:'call-1'}});
  collector.event({type:'response.function_call_arguments.delta',output_index:1,delta:'{"query":"x"}'});collector.finish();assert.ok(events.every(e=>e.content.callId==='call-1'&&e.content.name==='lookup'));assert.equal(output(events),'{"query":"x"}');
  const response=capture();response.collector.event({type:'response.output_text.delta',output_index:0,delta:'hello '});response.collector.finish({output:[{id:'message',type:'message',content:[{type:'output_text',text:'hello world'}]}]});assert.equal(output(response.events),'hello world');assert.equal(new Set(response.events.map(e=>e.content.field)).size,1);
});
test('tool argument credentials are hidden across fragments and partial output survives cancellation',()=>{
  const {events,collector}=capture();
  for(const delta of ['{"api_','key":"opaque-secret","path":"','file.txt"}'])collector.event({type:'response.function_call_arguments.delta',item_id:'tool',delta});
  collector.delta('msg:0:text','assistant',{item:'msg'},'未完成回复');collector.finish();
  assert.ok(!JSON.stringify(events).includes('opaque-secret'));assert.ok(output(events).includes('file.txt'));assert.ok(output(events).includes('未完成回复'));
});
test('recording buffers and field count are bounded and completion is idempotent',()=>{
  const {events,collector}=capture({maxFields:1,maxBytes:256});collector.delta('one','assistant',{},'一段内容');collector.delta('two','assistant',{},'other');assert.equal(collector.finish().truncated,true);const size=events.length;collector.finish();assert.equal(events.length,size);assert.equal(collector.fields.size,0);
  const large=capture({maxBytes:256});large.collector.input({input:'x'.repeat(1000)});assert.equal(large.collector.finish().truncated,true);assert.ok(large.collector.bytes<=256);assert.ok(!JSON.stringify(large.events).includes('x'.repeat(300)));
});
test('ignored final output nodes are bounded and capture stops accepting metadata after truncation',()=>{
  let visited=0;const values=new Proxy(Array(10000).fill({type:'reasoning'}),{get(target,key,receiver){if(/^\d+$/.test(String(key)))visited++;return Reflect.get(target,key,receiver);}}),{events,collector}=capture({maxNodes:16});
  assert.equal(collector.finish({output:values}).truncated,true);assert.ok(visited<=17);assert.equal(events.length,0);collector.event({type:'response.output_item.added',output_index:0,item:{id:'late',type:'function_call',name:'lookup'}});assert.equal(collector.itemMetadata.size,0);
});
