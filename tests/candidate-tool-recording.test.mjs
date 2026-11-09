import test from 'node:test';
import assert from 'node:assert/strict';
import {observeCandidateProtocol,verifyCandidateToolEvents,verifyCandidateToolDocument,verifyCandidateBodyExport} from '../ci/postgres-recording-qualification.mjs';
import {assembleRecording,recordingParts} from '../recording-search-index.mjs';

const call=(id,input)=>({type:'custom_tool_call',id:'item_'+id,call_id:id,name:'exec',input});
const result=(id,output)=>({type:'custom_tool_call_output',call_id:id,output});
const event=(sequence,kind,content)=>({sequence,kind,content});
const code='const message="合成工具正文"; text(message);';
function fixture(){
  const ids=['synthetic-first','synthetic-middle','synthetic-final'],user='synthetic current user',assistant='synthetic final assistant';
  const protocols=[observeCandidateProtocol({input:user},[call('call-1',code)]),observeCandidateProtocol({input:[result('call-1','synthetic returned one')]},[call('call-2','text("synthetic second call")')]),observeCandidateProtocol({input:[result('call-2','synthetic returned two')]},[{type:'message',content:[{type:'output_text',text:assistant}]}])];
  const events=[
    [event(1,'user',{text:user}),event(2,'tool_call',{callId:'call-1',name:'exec',text:code})],
    [event(1,'tool_result',{callId:'call-1',output:'synthetic returned one'}),event(2,'tool_call',{callId:'call-2',name:'exec',text:'text("synthetic second call")'})],
    [event(1,'tool_result',{callId:'call-2',output:'synthetic returned two'}),event(2,'assistant',{text:assistant})],
  ];
  const rows=ids.map(id=>({id,price_state:'priced',price_version:'synthetic-price',total_atoms:'0'}));
  const expected={requestIds:ids,memberId:'synthetic-member',deviceId:'synthetic-device',expectedUserText:user,expectedAssistantText:assistant,protocolSummaries:protocols.map((p,i)=>({requestId:ids[i],...p}))};
  const lines=events.flatMap((items,i)=>[{kind:'request',request_id:ids[i],member_id:expected.memberId,device_id:expected.deviceId,device_generation:1,metering_status:'priced',price_version:'synthetic-price',reference_usd:'0.00'},{kind:'recording_state',request_id:ids[i],state:'complete',reason:null},...items.map(e=>({...e,kind:'recording_event',event_kind:e.kind,request_id:ids[i]}))]);
  return {ids,events,rows,expected,lines,sourceCounts:new Map(ids.map((id,i)=>[id,events[i].length]))};
}
const text=lines=>lines.map(row=>JSON.stringify(row)).join('\n')+'\n';
const parts=events=>events.filter(e=>['tool_call','tool_result'].includes(e.kind)).map((e,i)=>({kind:e.kind,field:'synthetic-field-'+i,offset:0,metadata:{callId:e.content.callId,...(e.content.name?{name:e.content.name}:{})},text:e.content.text??e.content.output}));

test('independent observation retains real custom/function values and only the current output tail',()=>{
  const observed=observeCandidateProtocol({input:[result('old','historical output'),{type:'message',role:'assistant',content:[]},result('current','actual output')]},[call('next',code)]);
  assert.equal(observed.pureControl,false);assert.equal(observed.bodyExpected,true);assert.deepEqual(observed.toolResults.map(r=>r.callId),['current']);assert.equal(observed.toolResults[0].output,'actual output');assert.equal(observed.toolCalls[0].input,code);
  const fn=observeCandidateProtocol({input:[{type:'function_call_output',call_id:'fn-old',output:{ok:true}}]},[{type:'function_call',call_id:'fn-next',name:'shell',arguments:'{"command":"printf synthetic"}'}]);
  assert.equal(fn.toolCalls[0].input,'{"command":"printf synthetic"}');assert.deepEqual(fn.toolResults[0].output,{ok:true});
  assert.equal(observeCandidateProtocol({input:[]},[]).pureControl,true);
  assert.equal(observeCandidateProtocol({input:[]},[{type:'reasoning',encrypted_content:'synthetic opaque'}]).pureControl,true);
  assert.equal(observeCandidateProtocol({input:[]},[{type:'reasoning',summary:[{type:'summary_text',text:'visible synthetic summary'}]}]).bodyExpected,true);
  assert.equal(observeCandidateProtocol({input:[result('empty','')]},[call('empty-next','')]).pureControl,false);
});

test('each observed tool call and result must survive source, Worker document and export',()=>{
  const f=fixture();
  for(let i=0;i<f.ids.length;i++){
    verifyCandidateToolEvents(f.events[i],f.expected.protocolSummaries[i]);
    verifyCandidateToolDocument(parts(f.events[i]),f.expected.protocolSummaries[i]);
  }
  assert.equal(verifyCandidateBodyExport({...f,text:text(f.lines)}).eventRows,6);
  for(const mutation of ['callId','name','text','output','kind']){
    const source=structuredClone(f.events[1]),document=parts(source),exported=structuredClone(f.lines),selected=source[mutation==='output'?0:1];
    if(mutation==='kind')selected.kind='boundary';else selected.content[mutation]='wrong';
    assert.throws(()=>verifyCandidateToolEvents(source,f.expected.protocolSummaries[1]));
    const changed=document[mutation==='output'?0:1];
    if(mutation==='kind')changed.kind='boundary';else if(mutation==='text'||mutation==='output')changed.text='wrong';else changed.metadata[mutation]='wrong';
    assert.throws(()=>verifyCandidateToolDocument(document,f.expected.protocolSummaries[1]));
    const item=exported.find(row=>row.request_id===f.ids[1]&&row.event_kind===(mutation==='output'?'tool_result':'tool_call'));
    if(mutation==='kind')item.event_kind='boundary';else item.content[mutation]='wrong';
    assert.throws(()=>verifyCandidateBodyExport({...f,text:text(exported)}));
  }
});

test('custom exec cannot use the old zero-source exemption or boundary-only content',()=>{
  const f=fixture(),missing=f.lines.filter(row=>row.request_id!==f.ids[1]||row.kind!=='recording_event');
  f.sourceCounts.set(f.ids[1],0);assert.throws(()=>verifyCandidateBodyExport({...f,text:text(missing)}),/zero source/);
  const old={...f.expected,protocolSummaries:structuredClone(f.expected.protocolSummaries)};old.protocolSummaries[1].pureControl=true;old.protocolSummaries[1].bodyExpected=false;
  assert.throws(()=>verifyCandidateBodyExport({...f,expected:old,text:text(missing)}),/actual content/);
  const boundary=f.lines.map(row=>row.request_id===f.ids[1]&&row.kind==='recording_event'?{...row,event_kind:'boundary'}:row);f.sourceCounts.set(f.ids[1],2);
  assert.throws(()=>verifyCandidateBodyExport({...f,text:text(boundary)}),/actual round tool_call/);
});

test('source deltas/replacement and Unicode document parts restore the exact tool value',()=>{
  const protocol=observeCandidateProtocol({input:[]},[call('unicode',code)]),events=[event(1,'tool_call',{callId:'unicode',name:'exec',field:'input',mode:'append',text:'stale'}),event(2,'tool_call',{callId:'unicode',name:'exec',field:'input',mode:'replace',text:code})];
  verifyCandidateToolEvents(events,protocol);const chars=Array.from(code),metadata={callId:'unicode',name:'exec'},document=[{kind:'tool_call',field:'input',offset:0,metadata,text:chars.slice(0,12).join('')},{kind:'tool_call',field:'input',offset:12,metadata,text:chars.slice(12).join('')}];
  verifyCandidateToolDocument(document,protocol);assert.throws(()=>verifyCandidateToolDocument([{...document[1],offset:13},document[0]],protocol),/contiguous/);
});

test('opaque custom input cannot lose quotes through JSON coercion',()=>{
  const protocol=observeCandidateProtocol({input:[]},[call('quoted','"opaque command"')]);
  verifyCandidateToolEvents([event(1,'tool_call',{callId:'quoted',name:'exec',text:'"opaque command"'})],protocol);
  assert.throws(()=>verifyCandidateToolEvents([event(1,'tool_call',{callId:'quoted',name:'exec',text:'opaque command'})],protocol),/payload mismatch/);
});

test('real NFKC search-only trailing parts do not invent a gap in original tool text',()=>{
  const input='ﬃ'.repeat(1000),protocol=observeCandidateProtocol({input:[]},[call('nfkc',input)]),events=[event(1,'tool_call',{callId:'nfkc',name:'exec',field:'input',text:input})];
  const document=recordingParts(assembleRecording(events)).map(({searchText,...part})=>part);
  assert.equal(document.length,2);assert.equal(document[1].offset,2048);assert.equal(document[1].text,'');verifyCandidateToolDocument(document,protocol);
  assert.throws(()=>verifyCandidateToolDocument([{...document[0],text:input.slice(1)},document[1]],protocol),/payload mismatch/);
});

test('JSON-shaped result strings retain their non-sensitive stdout formatting',()=>{
  const output='{\n  "ok": true, "value": 123456789012345678901234567890\n}\n',protocol=observeCandidateProtocol({input:[result('json-output',output)]},[]),events=[event(1,'tool_result',{callId:'json-output',output})];
  assert.equal(protocol.toolResults[0].output,output);verifyCandidateToolEvents(events,protocol);verifyCandidateToolDocument(parts(events),protocol);
  const changed=JSON.stringify(JSON.parse(output));assert.throws(()=>verifyCandidateToolEvents([event(1,'tool_result',{callId:'json-output',output:changed})],protocol),/payload mismatch/);
  assert.throws(()=>verifyCandidateToolDocument(parts([event(1,'tool_result',{callId:'json-output',output:changed})]),protocol),/payload mismatch/);
});

test('historical tool returns cannot be counted as current-round source or derived content',()=>{
  const f=fixture(),old=event(3,'tool_result',{callId:'old-history',output:'historical text'}),events=[...f.events[1],old];
  assert.throws(()=>verifyCandidateToolEvents(events,f.expected.protocolSummaries[1]),/only this actual round/);
  assert.throws(()=>verifyCandidateToolDocument(parts(events),f.expected.protocolSummaries[1]),/only this actual round/);
  f.lines.push({...old,kind:'recording_event',event_kind:old.kind,request_id:f.ids[1]});f.sourceCounts.set(f.ids[1],3);
  assert.throws(()=>verifyCandidateBodyExport({...f,text:text(f.lines)}),/only this actual round/);
});

test('tool events remain verifiable after actual JSONL fragment reconstruction',()=>{
  const f=fixture(),target=f.lines.find(row=>row.request_id===f.ids[1]&&row.event_kind==='tool_call'),encoded=Buffer.from(JSON.stringify(target)).toString('base64'),half=Math.floor(encoded.length/2);
  const fragments=[encoded.slice(0,half),encoded.slice(half)].map((data,part)=>({kind:'recording_event_fragment',request_id:target.request_id,sequence:target.sequence,encoding:'base64_json',part,last:part===1,data}));
  const lines=[...f.lines.filter(row=>row!==target),...fragments];assert.equal(verifyCandidateBodyExport({...f,text:text(lines)}).eventRows,6);
  assert.throws(()=>verifyCandidateBodyExport({...f,text:text(lines.filter(row=>row!==fragments[0]))}));
});

test('a genuinely empty observed heartbeat may retain zero events without exempting tool rounds',()=>{
  const f=fixture(),id='synthetic-heartbeat',protocol=observeCandidateProtocol({input:[]},[]);
  f.expected.requestIds.push(id);f.expected.protocolSummaries.push({requestId:id,...protocol});f.rows.push({id,price_state:'priced',price_version:'synthetic-price',total_atoms:'0'});f.sourceCounts.set(id,0);
  f.lines.push({kind:'request',request_id:id,member_id:f.expected.memberId,device_id:f.expected.deviceId,device_generation:1,metering_status:'priced',price_version:'synthetic-price',reference_usd:'0.00'},{kind:'recording_state',request_id:id,state:'complete',reason:null});
  assert.equal(verifyCandidateBodyExport({...f,text:text(f.lines)}).requestRows,4);
});
