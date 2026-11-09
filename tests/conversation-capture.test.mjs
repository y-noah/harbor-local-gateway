import test from 'node:test';
import assert from 'node:assert/strict';
import {ConversationCapture,currentInput} from '../conversation-capture.mjs';
import {redactRecordText,sanitizeRecord} from '../record-redaction.mjs';
import {JsonRecordRedactor} from '../record-json.mjs';

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

function toolFields(events,kind,property='text'){
  const fields=new Map();for(const event of events){if(event.kind!==kind||!event.content.field)continue;const c=event.content,value=typeof c[property]==='string'?c[property]:JSON.stringify(c[property]);fields.set(c.field,c.mode==='replace'?value:(fields.get(c.field)||'')+(value||''));}return [...fields.values()];
}
test('custom result tails retain their current boundary and never record preceding prompts or tool inputs',()=>{
  const payload={input:[{role:'user',content:'private previous prompt'},{type:'custom_tool_call',name:'exec',call_id:'custom-1',input:'private previous command'},{type:'custom_tool_call_output',call_id:'custom-1',output:'visible tool result'}]},before=JSON.stringify(payload),current=currentInput(payload);
  assert.equal(current.events.length,1);assert.equal(current.events[0].kind,'tool_result');assert.equal(current.events[0].content.toolType,'custom_tool_call');assert.ok(!JSON.stringify(current).includes('private previous'));
  const {events,collector}=capture();collector.input(payload,current);const result=collector.finish();assert.equal(events[0].content.output,'visible tool result');assert.equal(events[0].content.callId,'custom-1');assert.equal(Object.hasOwn(events[0].content,'name'),false);assert.deepEqual(result.coverage.seen,{toolCalls:0,toolResults:1,reasoningSummaries:0});assert.deepEqual(result.coverage.captured,result.coverage.seen);assert.equal(result.incomplete,false);assert.equal(JSON.stringify(payload),before);
});
test('custom tool raw text is redacted at every split and delta, done, item and final snapshots never duplicate it',()=>{
  const raw='*** Begin Patch\n*** Add File: visible.txt\n+const value = "literal";\n*** End Patch\nAuthorization: Bearer opaque-fixture-secret\nprintf "visible tail"\n',expected=redactRecordText(raw);
  for(let split=0;split<=raw.length;split++){
    const {events,collector}=capture(),item={id:'custom',type:'custom_tool_call',name:'exec',call_id:'custom-call',input:raw},before=JSON.stringify(item);
    collector.event({type:'response.output_item.added',output_index:1,item:{...item,input:''}});
    for(const delta of [raw.slice(0,split),raw.slice(split)])collector.event({type:'response.custom_tool_call_input.delta',output_index:1,delta});
    collector.event({type:'response.custom_tool_call_input.done',item_id:'custom',output_index:1,input:raw});collector.event({type:'response.output_item.done',output_index:1,item});const result=collector.finish({output:[item]});
    assert.deepEqual(toolFields(events,'tool_call'),[expected],'split '+split);assert.ok(events.every(event=>event.content.callId==='custom-call'&&event.content.name==='exec'&&event.content.toolType==='custom_tool_call'));assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'));assert.equal(result.coverage.seen.toolCalls,1);assert.equal(result.coverage.captured.toolCalls,1);assert.equal(result.incomplete,false);assert.equal(JSON.stringify(item),before);
  }
});
test('custom tools preserve raw JSON-looking whitespace and authoritative final corrections',()=>{
  const raw=' {\n  "script": "printf 1",\n  "note": "visible"\n}\n',item={id:'custom',type:'custom_tool_call',call_id:'call',name:'exec',input:raw},{events,collector}=capture();
  collector.event({type:'response.output_item.done',output_index:0,item:{...item,input:'old partial text'}});const result=collector.finish({output:[item]});assert.deepEqual(toolFields(events,'tool_call'),[raw]);assert.equal(result.coverage.seen.toolCalls,1);assert.equal(result.incomplete,false);
  const empty=capture();empty.collector.finish({output:[{...item,input:''}]});assert.equal(empty.events.length,1);assert.equal(empty.events[0].content.text,'');assert.equal(empty.events[0].content.callId,'call');
});
test('custom multimodal results retain text and structure while credentials split across Responses text parts stay redacted',()=>{
  for(let split=0;split<32;split++){
    const secret='sk-proj-opaque-fixture-secret',parts=[{type:'input_text',text:secret.slice(0,split)},{type:'input_image',image_url:'data:image/png;base64,PRIVATE_IMAGE'},{type:'input_text',text:secret.slice(split)+'\nvisible tail'},{type:'input_file',file_data:'PRIVATE_FILE'},{type:'output_audio',data:'PRIVATE_AUDIO'}],payload={input:[{type:'custom_tool_call_output',call_id:'call',output:{content:parts,password:'opaque-password',status:'visible'}}]},before=JSON.stringify(payload),{events,collector}=capture();
    collector.input(payload);const result=collector.finish(),safe=events[0].content.output,encoded=JSON.stringify(events);assert.equal(safe.content[0].type,'input_text');assert.equal(safe.content[2].type,'input_text');assert.ok(safe.content[2].text.includes('visible tail'));assert.equal(safe.status,'visible');assert.ok(!encoded.includes('opaque-fixture-secret'),'split '+split);assert.ok(!encoded.includes('opaque-password'));assert.ok(!encoded.includes('PRIVATE_'));assert.equal(result.incomplete,false);assert.equal(JSON.stringify(payload),before);
  }
});
test('atomic custom output items deduplicate final snapshots and share one logical result count with input aliases',()=>{
  const {events,collector}=capture(),item={id:'result',type:'custom_tool_call_output',call_id:'same-call',output:JSON.stringify({content:[{type:'text',text:'visible result'},{type:'image',data:'PRIVATE_IMAGE'}]})};
  collector.input({input:[{type:'custom_tool_call_output',call_id:'same-call',output:'earlier visible result'}]});collector.event({type:'response.output_item.done',output_index:0,item});const result=collector.finish({output:[item]});assert.equal(events.filter(event=>event.kind==='tool_result').length,2);assert.equal(result.coverage.seen.toolResults,1);assert.equal(result.coverage.captured.toolResults,1);assert.equal(result.incomplete,false);assert.ok(!JSON.stringify(events).includes('PRIVATE_IMAGE'));assert.ok(events.every(event=>!Object.hasOwn(event.content,'name')));
});
test('unique tool coverage does not count repeated added/done/final identities as separate calls',()=>{
  const {collector}=capture(),first={type:'custom_tool_call',id:'one',call_id:'same-call',name:'exec',input:'visible'},second={...first,id:'two'};
  collector.event({type:'response.output_item.added',output_index:0,item:{...first,input:''}});collector.event({type:'response.output_item.added',output_index:0,item:{...first,input:''}});collector.event({type:'response.output_item.done',output_index:0,item:first});const result=collector.finish({output:[first,second]});assert.equal(result.coverage.seen.toolCalls,1);assert.equal(result.coverage.captured.toolCalls,1);assert.equal(result.incomplete,false);
});
test('unknown visible tools and content produce bounded incomplete facts without retaining their raw payload or type',()=>{
  const {events,collector}=capture({maxFields:8}),unknown={id:'future',type:'future_tool_opaque-fixture-secret',input:'PRIVATE_BODY'};
  collector.event({type:'response.created'});collector.event({type:'response.output_item.added',output_index:0,item:unknown});collector.event({type:'response.output_item.done',output_index:0,item:unknown});
  for(let i=0;i<10;i++)collector.event({type:'response.future_tool.progress',output_index:0,delta:'PRIVATE_PROGRESS'});
  const result=collector.finish({output:[unknown,{type:'message',id:'message',content:[{type:'future-visible-content',data:'PRIVATE_CONTENT'}]}]});assert.equal(result.incomplete,true);assert.equal(result.coverage.complete,false);assert.deepEqual(result.coverage.unsupported,{items:1,events:1,contentParts:1});assert.deepEqual(result.coverage.reasons,['unsupported_content','unsupported_event','unsupported_item']);assert.ok(!JSON.stringify({events,result}).includes('PRIVATE_'));assert.ok(!JSON.stringify({events,result}).includes('opaque-fixture-secret'));
  const input=capture();input.collector.input({input:[{type:'future_tool_call_output',output:'PRIVATE_UNKNOWN'}]});assert.equal(input.collector.finish().incomplete,true);assert.equal(input.events.length,0);
});
test('public reasoning summaries are deduplicated while opaque reasoning is never read or persisted',()=>{
  let reads=0;const item={type:'reasoning',id:'reason',summary:[{type:'summary_text',text:'visible summary'}]};Object.defineProperty(item,'encrypted_content',{enumerable:true,get(){reads++;throw Error('opaque content must not be read');}});
  const {events,collector}=capture();collector.event({type:'response.reasoning_summary_text.delta',item_id:'reason',summary_index:0,delta:'visible '});collector.event({type:'response.reasoning_summary_text.done',item_id:'reason',summary_index:0,text:'visible summary'});collector.event({type:'response.output_item.done',output_index:0,item});const result=collector.finish({output:[item]});assert.equal(reads,0);assert.equal(output(events),'visible summary');assert.equal(result.coverage.opaqueReasoningExcluded,1);assert.equal(result.coverage.seen.reasoningSummaries,1);assert.equal(result.coverage.captured.reasoningSummaries,1);assert.equal(result.incomplete,false);
});
test('malformed tools, truncation and failed emit cannot report complete capture',()=>{
  const malformed=capture(),result=malformed.collector.finish({output:[{type:'custom_tool_call',id:'bad',call_id:'call',name:'exec'}]});assert.equal(result.incomplete,true);assert.ok(result.coverage.reasons.includes('malformed_tool_item'));assert.equal(result.coverage.seen.toolCalls,1);assert.equal(result.coverage.captured.toolCalls,0);
  const failed=new ConversationCapture({emit:()=>false}),failure=failed.finish({output:[{type:'custom_tool_call',id:'custom',call_id:'call',name:'exec',input:'visible'}]});assert.equal(failure.failed,true);assert.equal(failure.incomplete,true);assert.equal(failure.coverage.captured.toolCalls,0);assert.ok(failure.coverage.reasons.includes('capture_failed'));
  const clipped=capture({maxBytes:256});clipped.collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output:'x'.repeat(10000)}]});const truncated=clipped.collector.finish();assert.equal(truncated.truncated,true);assert.equal(truncated.incomplete,true);assert.ok(truncated.coverage.reasons.includes('size_limit'));assert.ok(clipped.collector.bytes<=256);
});
test('custom call aliases learned after a delta retain one assembled field and final tool identity',async()=>{
  const {assembleRecording}=await import('../recording-search-index.mjs'),{events,collector}=capture(),item={type:'custom_tool_call',id:'custom-late',call_id:'late-call',name:'exec',input:'visible raw command'};
  collector.event({type:'response.custom_tool_call_input.delta',output_index:2,delta:item.input});collector.event({type:'response.output_item.added',output_index:2,item:{...item,input:''}});collector.event({type:'response.custom_tool_call_input.done',item_id:item.id,output_index:2,input:item.input});const result=collector.finish({output:[item]});
  const assembled=assembleRecording(events.map((event,index)=>({...event,sequence:index+1})));assert.equal(assembled.length,1);assert.equal(assembled[0].text,item.input);assert.equal(assembled[0].metadata.callId,'late-call');assert.equal(assembled[0].metadata.name,'exec');assert.equal(assembled[0].metadata.toolType,'custom_tool_call');assert.equal(result.coverage.seen.toolCalls,1);assert.equal(result.incomplete,false);
});
test('large custom results remain chunked, redact credentials and assemble once without broken Unicode',async()=>{
  const {assembleRecording}=await import('../recording-search-index.mjs'),raw='步骤📌'.repeat(12000)+'\nAuthorization: Bearer opaque-fixture-secret\nvisible tail',payload={input:[{type:'custom_tool_call_output',call_id:'previous-call',output:raw}]},{events,collector}=capture();collector.input(payload);const result=collector.finish(),assembled=assembleRecording(events.map((event,index)=>({...event,sequence:index+1})));
  assert.ok(events.length>1);assert.ok(events.every(event=>Buffer.byteLength(JSON.stringify(event.content))<32*1024));assert.equal(assembled.length,1);assert.equal(assembled[0].text,redactRecordText(raw));assert.equal(assembled[0].metadata.callId,'previous-call');assert.equal(result.coverage.seen.toolResults,1);assert.equal(result.coverage.captured.toolResults,1);assert.equal(result.incomplete,false);assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'));
});
test('tool result attachment facts survive and unsupported text metadata is disclosed without copying it',()=>{
  const {events,collector}=capture();collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output:[{type:'input_text',text:'visible',annotations:[{url:'PRIVATE_METADATA'}]},{type:'input_image',size:128,position:2,detail:'low',image_url:'PRIVATE_IMAGE'}]}]});const result=collector.finish();assert.deepEqual(events[0].content.output[1],{type:'input_image',bytes:128,position:2,detail:'low',content:'[附件数据已省略]'});assert.ok(!JSON.stringify(events).includes('PRIVATE_'));assert.equal(result.incomplete,true);assert.equal(result.coverage.unsupported.contentParts,1);assert.ok(result.coverage.reasons.includes('unsupported_content'));
});
test('visible content-part and summary-part done snapshots survive without separate text delta events',()=>{
  const {events,collector}=capture();collector.event({type:'response.content_part.done',output_index:0,item_id:'message',content_index:0,part:{type:'output_text',text:'visible message'}});collector.event({type:'response.reasoning_summary_part.done',output_index:1,item_id:'reason',summary_index:0,part:{type:'summary_text',text:'visible summary'}});const result=collector.finish({output:[{type:'message',id:'message',content:[{type:'output_text',text:'visible message'}]},{type:'reasoning',id:'reason',summary:[{type:'summary_text',text:'visible summary'}]}]});assert.equal(output(events),'visible messagevisible summary');assert.equal(result.coverage.seen.reasoningSummaries,1);assert.equal(result.incomplete,false);
});
test('JSON-shaped stdout remains original text formatting and numeric spelling while structured outputs remain objects',()=>{
  const raw=' {\n  "value" : 90071992547409931234567, "scientific": 1.2300e+5,\n  "message": "visible", "items" : [1, 2]\n}\n',c=capture();c.collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output:raw}]});const result=c.collector.finish();assert.equal(c.events[0].content.output,raw);assert.equal(typeof c.events[0].content.output,'string');assert.equal(result.incomplete,false);
  const structured=capture(),value={message:'visible',items:[1,2]};structured.collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output:value}]});structured.collector.finish();assert.deepEqual(structured.events[0].content.output,value);assert.equal(typeof structured.events[0].content.output,'object');
});
test('JSON stdout changes only redacted values or attachment spans and still filters cross-part credentials',()=>{
  const raw=' {\n "code": 90071992547409931234567,\n "content" : [ { "type":"input_text", "text":"sk-pr" }, {"type":"input_text","text":"oj-opaque-fixture-secret\\nvisible tail"}, { "data": "PRIVATE_BINARY", "bytes": 9, "type": "image" } ],\n "after" : "visible"\n}\n',c=capture();c.collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output:raw}]});c.collector.finish();const value=c.events[0].content.output;assert.equal(typeof value,'string');assert.ok(value.startsWith(' {\n "code": 90071992547409931234567,\n'));assert.ok(value.endsWith('\n "after" : "visible"\n}\n'));assert.ok(!value.includes('PRIVATE_BINARY'));assert.ok(!value.includes('opaque-fixture-secret'));assert.ok(value.includes('visible tail'));assert.equal(JSON.parse(value).content[2].bytes,9);
});
test('unsupported tool content parts are omitted with incomplete facts while ordinary business objects remain intact',()=>{
  for(const output of [[{type:'future_visible_part',data:'PRIVATE_BINARY'}],{content:[{type:'future_visible_part',data:'PRIVATE_BINARY'}]}]){
    const {events,collector}=capture();collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output}]});const result=collector.finish();assert.ok(!JSON.stringify(events).includes('PRIVATE_BINARY'));assert.ok(!JSON.stringify(events).includes('future_visible_part'));assert.equal(result.incomplete,true);assert.equal(result.coverage.unsupported.contentParts,1);assert.ok(result.coverage.reasons.includes('unsupported_content'));
  }
  const {events,collector}=capture(),value={rows:[{type:'invoice',data:'visible business data'}]};collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output:value}]});assert.equal(collector.finish().incomplete,false);assert.deepEqual(events[0].content.output,value);
});
test('added-only atomic tool results survive interruptions while unfinished result placeholders stay incomplete',()=>{
  const item={type:'custom_tool_call_output',id:'result',call_id:'call',output:'visible result'},c=capture();c.collector.event({type:'response.output_item.added',output_index:0,item});const result=c.collector.finish();assert.equal(c.events.length,1);assert.equal(c.events[0].content.output,'visible result');assert.equal(result.coverage.seen.toolResults,1);assert.equal(result.coverage.captured.toolResults,1);assert.equal(result.incomplete,false);
  const pending=capture();pending.collector.event({type:'response.output_item.added',output_index:0,item:{type:'custom_tool_call_output',id:'pending',call_id:'pending-call'}});const incomplete=pending.collector.finish();assert.equal(incomplete.coverage.seen.toolResults,1);assert.equal(incomplete.coverage.captured.toolResults,0);assert.equal(incomplete.incomplete,true);
  const completed=capture();completed.collector.event({type:'response.output_item.added',output_index:0,item:{...item,output:undefined}});assert.equal(completed.collector.finish({output:[item]}).incomplete,false);
});
test('closed done-only tools require associations but can learn them from an authoritative final item',()=>{
  const missing=capture();missing.collector.event({type:'response.custom_tool_call_input.done',item_id:'custom',output_index:0,input:'visible command'});const incomplete=missing.collector.finish();assert.equal(incomplete.incomplete,true);assert.ok(incomplete.coverage.reasons.includes('malformed_tool_item'));
  const completed=capture();completed.collector.event({type:'response.custom_tool_call_input.done',item_id:'custom',output_index:0,input:'visible command'});const result=completed.collector.finish({output:[{type:'custom_tool_call',id:'custom',call_id:'call',name:'exec',input:'visible command'}]});assert.equal(result.incomplete,false);assert.equal(result.coverage.seen.toolCalls,1);assert.equal(result.coverage.captured.toolCalls,1);assert.ok(completed.events.some(event=>event.content.callId==='call'&&event.content.name==='exec'));
});
test('unchanged results still publish late protocol metadata without duplicating the assembled body',async()=>{
  const {assembleRecording}=await import('../recording-search-index.mjs'),c=capture(),item={type:'custom_tool_call_output',id:'result',call_id:'call',output:'visible result'};c.collector.event({type:'response.output_item.done',output_index:0,item});const result=c.collector.finish({output:[{...item,name:'protocol-supplied-name'}]});const assembled=assembleRecording(c.events.map((event,index)=>({...event,sequence:index+1})));assert.equal(assembled.length,1);assert.equal(assembled[0].text,'visible result');assert.equal(assembled[0].metadata.name,'protocol-supplied-name');assert.equal(result.coverage.seen.toolResults,1);assert.equal(result.coverage.captured.toolResults,1);assert.equal(result.incomplete,false);
});
test('stdout JSON shape never upgrades ordinary business data into protocol content parts',()=>{
  for(const raw of ['[ { "type" : "invoice", "data" : "visible business data" } ]','{ "content" : [ {"type":"input_text","text":123,"annotations":[{"note":"visible annotation"}],"data":"visible data"} ] }','{ "content": [{"type":"future_visible_part","data":"visible future business value"}], "password": "opaque-fixture-secret" }']){
    const {events,collector}=capture(),filter=new JsonRecordRedactor(),expected=filter.push(raw)+filter.finish();collector.input({input:[{type:'custom_tool_call_output',call_id:'call',output:raw}]});const result=collector.finish();assert.equal(events[0].content.output,expected);assert.equal(result.incomplete,false);assert.equal(result.coverage.unsupported.contentParts,0);assert.ok(!JSON.stringify(events).includes('opaque-fixture-secret'));
  }
});
