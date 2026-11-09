import assert from 'node:assert/strict';
import {ChildProcess} from 'node:child_process';
import {basename} from 'node:path';
import {referenceUsdDisplay} from '../recording-export-format.mjs';
import {quoteReference} from '../pricing.mjs';
import {sanitizeRecord,sanitizeToolRecord} from '../record-redaction.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {JsonRecordRedactor} from '../record-json.mjs';

const callTypes=new Set(['function_call','custom_tool_call']),resultTypes=new Set(['function_call_output','custom_tool_call_output']);
const bodyText=value=>typeof value==='string'?value.length>0:Array.isArray(value)&&value.some(part=>typeof part?.text==='string'&&part.text.length>0||typeof part?.refusal==='string'&&part.refusal.length>0||['input_image','input_file','image_url'].includes(part?.type));
function observedToolOutput(value){
  if(typeof value!=='string')return sanitizeToolRecord(value);
  if(/^[\[{]/.test(value.trimStart()))try{JSON.parse(value);const filter=new JsonRecordRedactor();return filter.push(value)+filter.finish();}catch{}
  return sanitizeRecord(value);
}
// This observes the controlled upstream boundary, independently of currentInput.
export function observeCandidateProtocol(payload,output){
  const input=Array.isArray(payload.input)?payload.input:[],tail=input.at(-1),results=[];
  for(let index=input.length-1;index>=0&&resultTypes.has(input[index]?.type);index--)results.unshift(input[index]);
  const toolCalls=output.filter(item=>callTypes.has(item?.type)).map(item=>({protocolType:item.type,callId:item.call_id,name:item.name,inputPresent:Object.hasOwn(item,item.type==='custom_tool_call'?'input':'arguments'),input:item.type==='custom_tool_call'?sanitizeRecord(item.input):sanitizeToolRecord(item.arguments)}));
  const toolResults=results.map(item=>({protocolType:item.type,callId:item.call_id,outputPresent:Object.hasOwn(item,'output'),output:observedToolOutput(item.output)}));
  const inputBody=typeof payload.input==='string'?payload.input.length>0:tail?.role==='user'&&bodyText(tail.content),outputBody=output.some(item=>item?.type==='message'?bodyText(item.content):item?.type==='reasoning'?bodyText(item.summary):true);
  const bodyExpected=!!(inputBody||bodyText(payload.prompt)||outputBody||toolResults.length);
  return {inputTailType:tail?.type??(typeof payload.input==='string'?'text':null),inputTailRole:tail?.role??(typeof payload.input==='string'?'user':null),outputTypes:output.map(item=>item?.type??null),toolNames:toolCalls.map(item=>item.name),toolCalls,toolResults,bodyExpected,pureControl:!bodyExpected};
}

function checkedProtocol(protocol){
  if(protocol===undefined)return null;
  assert.ok(Array.isArray(protocol.toolCalls)&&Array.isArray(protocol.toolResults)&&Array.isArray(protocol.outputTypes),'actual observed tool values required');
  assert.equal(typeof protocol.bodyExpected,'boolean');assert.equal(protocol.pureControl,!protocol.bodyExpected);
  assert.equal(protocol.outputTypes.filter(type=>callTypes.has(type)).length,protocol.toolCalls.length,'each observed output tool needs its own values');
  if(resultTypes.has(protocol.inputTailType))assert.ok(protocol.toolResults.length>0,'actual input tool results cannot be classified as empty control');
  for(const [items,field,present] of [[protocol.toolCalls,'input','inputPresent'],[protocol.toolResults,'output','outputPresent']]){
    assert.equal(new Set(items.map(item=>item.callId)).size,items.length);
    for(const item of items){assert.ok(typeof item.callId==='string'&&item.callId);assert.equal(item[present],true,'observed tool '+field+' must be present');assert.ok(Object.hasOwn(item,field));}
  }
  for(const call of protocol.toolCalls)assert.ok(typeof call.name==='string'&&call.name,'actual observed tool name required');
  if(protocol.toolCalls.length||protocol.toolResults.length)assert.equal(protocol.bodyExpected,true,'tool calls/results are actual content, including empty values');
  return protocol;
}
const controlOnly=protocol=>{const value=checkedProtocol(protocol);return !!value&&!value.bodyExpected&&!value.toolCalls.length&&!value.toolResults.length;};
const semantic=value=>{if(typeof value!=='string')return value;try{return JSON.parse(value);}catch{return value;}};
function verifyToolItems(items,protocol,location){
  const observed=checkedProtocol(protocol);if(!observed)return {calls:0,results:0};
  for(const [expected,kind] of [[observed.toolCalls,'tool_call'],[observed.toolResults,'tool_result']])assert.deepEqual(items.filter(item=>item.kind===kind).map(item=>item.metadata.callId).sort(),expected.map(item=>item.callId).sort(),location+' must contain only this actual round '+kind+' identities');
  for(const [expected,kind,field] of [[observed.toolCalls,'tool_call','input'],[observed.toolResults,'tool_result','output']])for(const tool of expected){
    const found=items.filter(item=>item.kind===kind&&item.metadata.callId===tool.callId);assert.equal(found.length,1,location+' must retain exactly one '+kind+' for callId '+tool.callId);
    if(kind==='tool_call')assert.equal(found[0].metadata.name,tool.name,location+' tool name mismatch');
    assert.equal(found[0].payloadSeen,true,location+' tool payload missing');
    if(kind==='tool_call'&&tool.protocolType==='custom_tool_call'||kind==='tool_result'&&typeof tool[field]==='string')assert.equal(found[0].text,tool[field],location+' '+kind+' payload mismatch');
    else assert.deepEqual(semantic(found[0].text),semantic(tool[field]),location+' '+kind+' payload mismatch');
  }
  return {calls:observed.toolCalls.length,results:observed.toolResults.length};
}
export function verifyCandidateToolEvents(events,protocol,location='source'){
  const fields=new Map();
  for(const event of [...events].sort((a,b)=>a.sequence-b.sequence)){
    if(!['tool_call','tool_result'].includes(event.kind))continue;
    const content=event.content??{},key=event.kind+':'+(content.field??'event:'+event.sequence),item=fields.get(key)??{kind:event.kind,metadata:{},text:'',payloadSeen:false};
    item.metadata={...item.metadata,...content};let value;
    if(Object.hasOwn(content,'text'))value=content.text;else if(Object.hasOwn(content,'output'))value=content.output;else if(Object.hasOwn(content,'input'))value=content.input;
    if(value!==undefined){const text=typeof value==='string'?value:JSON.stringify(value);item.text=content.mode==='replace'?text:item.text+text;item.payloadSeen=true;}
    fields.set(key,item);
  }
  return verifyToolItems([...fields.values()],protocol,location);
}
export function verifyCandidateToolDocument(parts,protocol){
  const fields=new Map();
  for(const part of parts.filter(item=>['tool_call','tool_result'].includes(item.kind))){const key=part.kind+':'+part.field,list=fields.get(key)??[];list.push(part);fields.set(key,list);}
  const items=[];
  for(const list of fields.values()){
    list.sort((a,b)=>a.offset-b.offset);let offset=0,text='',searchTail=false;const seen=new Set();
    for(const [index,part] of list.entries()){
      assert.ok(Number.isSafeInteger(part.offset)&&part.offset>=0&&!seen.has(part.offset));seen.add(part.offset);assert.deepEqual(part.metadata,list[0].metadata);assert.equal(typeof part.text,'string');
      if(index>0&&part.text===''){assert.ok(part.offset>=offset&&part.continuation===true,'empty NFKC search tails must follow the source text');searchTail=true;continue;}
      assert.equal(searchTail,false,'source text cannot follow a search-only tail');assert.equal(part.offset,offset,'derived tool parts must be contiguous');text+=part.text;offset+=Array.from(part.text).length;
    }
    items.push({kind:list[0].kind,metadata:list[0].metadata,text,payloadSeen:true});
  }
  return verifyToolItems(items,protocol,'Worker document');
}
export async function candidateCondition(predicate,message,{timeoutMs=30000}={}){const deadline=Date.now()+timeoutMs;for(;;){if(await predicate())return;if(Date.now()>=deadline)throw Error(message);await new Promise(resolve=>setTimeout(resolve,50));}}
export function verifyCandidateBodyExport({text,rows,expected,sourceCounts}){
  const lines=text.trim().split('\n').map(line=>JSON.parse(line)),requests=lines.filter(row=>row.kind==='request'),states=lines.filter(row=>row.kind==='recording_state'),events=lines.filter(row=>row.kind==='recording_event'),fragments=new Map();
  for(const row of lines.filter(row=>row.kind==='recording_event_fragment')){const key=JSON.stringify([row.request_id,row.sequence]),parts=fragments.get(key)??[];assert.equal(row.encoding,'base64_json');parts.push(row);fragments.set(key,parts);}
  for(const parts of fragments.values()){parts.sort((a,b)=>a.part-b.part);assert.ok(parts.every((part,index)=>part.part===index&&part.last===(index===parts.length-1)));const encoded=parts.map(part=>part.data).join(''),bytes=Buffer.from(encoded,'base64');assert.equal(bytes.toString('base64'),encoded);const event=JSON.parse(bytes.toString());assert.equal(event.kind,'recording_event');assert.equal(event.request_id,parts[0].request_id);assert.equal(event.sequence,parts[0].sequence);events.push(event);}
  assert.equal(requests.length,rows.length);assert.deepEqual(requests.map(row=>row.request_id).sort(),[...expected.requestIds].sort());assert.ok(events.length>0,'actual export must contain nonempty recording events');assert.ok(states.every(row=>expected.requestIds.includes(row.request_id)));assert.ok(events.every(row=>expected.requestIds.includes(row.request_id)));
  for(const row of requests){const fact=rows.find(value=>value.id===row.request_id);assert.ok(fact);assert.equal(row.member_id,expected.memberId);assert.equal(row.device_id,expected.deviceId);assert.equal(row.device_generation,1);assert.equal(row.metering_status,fact.price_state);assert.equal(row.price_version,fact.price_version);assert.equal(row.reference_usd,referenceUsdDisplay(fact.total_atoms));const status=states.filter(state=>state.request_id===row.request_id);assert.ok(status.length>0&&status.every(state=>state.state==='complete'&&state.reason===null),'every expected request must retain a valid complete exported recording_state');const actual=events.filter(event=>event.request_id===row.request_id),count=sourceCounts.get(row.request_id);assert.ok(Number.isSafeInteger(count)&&count>=0);assert.equal(actual.length,count,'exported body events must match this request actual source events');assert.equal(new Set(actual.map(event=>event.sequence)).size,actual.length);if(count)assert.ok(actual.length>0);else assert.ok(controlOnly(expected.protocolSummaries?.find(protocol=>protocol.requestId===row.request_id)),'zero source is forbidden for observed tool or text content');verifyCandidateToolEvents(actual.map(event=>({...event,kind:event.event_kind})),expected.protocolSummaries?.find(protocol=>protocol.requestId===row.request_id),'export');}
  assert.ok(events.some(event=>event.event_kind==='user'&&JSON.stringify(event.content).includes(expected.expectedUserText)),'user text must occur in the actual exported user event');assert.ok(events.some(event=>event.event_kind==='assistant'&&JSON.stringify(event.content).includes(expected.expectedAssistantText)),'final assistant text must occur in the actual exported assistant event');
  return {requestRows:requests.length,eventRows:events.length};
}
export async function qualifyCandidateWorkerRecordings({store,backend,workerChild,expected,recordingKey}){
  assert.ok(workerChild instanceof ChildProcess&&workerChild.pid!==process.pid&&workerChild.exitCode===null&&workerChild.signalCode===null&&workerChild.spawnargs.some(value=>basename(value)==='postgres-worker.mjs'),'actual independent Worker CLI required');
  assert.ok(Array.isArray(expected?.requestIds)&&expected.requestIds.length>0&&new Set(expected.requestIds).size===expected.requestIds.length,'nonempty actual CLI request IDs required');assert.ok(Number.isSafeInteger(expected.expectedTokens)&&expected.expectedTokens>0);assert.ok(typeof expected.expectedUserText==='string'&&expected.expectedUserText&&typeof expected.expectedAssistantText==='string'&&expected.expectedAssistantText);
  assert.ok(Buffer.isBuffer(recordingKey)&&recordingKey.length===32,'actual recording key required for source decryption');assert.ok(Array.isArray(expected.protocolSummaries)&&expected.protocolSummaries.length===expected.requestIds.length);assert.deepEqual(expected.protocolSummaries.map(row=>row.requestId).sort(),[...expected.requestIds].sort());for(const protocol of expected.protocolSummaries)checkedProtocol(protocol);assert.ok(expected.protocolSummaries.some(row=>row.toolCalls.length)&&expected.protocolSummaries.some(row=>row.toolResults.length),'actual CLI tool calls and returned outputs required');
  await candidateCondition(async()=>{const row=await store.get('SELECT COUNT(*) n FROM harbor.recording_documents d JOIN harbor.recording_work w ON w.request_id=d.request_id WHERE d.request_id=ANY($1::text[]) AND d.source_revision=w.revision AND w.indexed_revision=w.revision',expected.requestIds);return row.n===expected.requestIds.length;},'Candidate actual CLI bodies were not indexed by the Worker');
  const rows=await store.all('SELECT r.id,r.member_id,r.key_id,r.device_id,r.device_generation,r.status,r.started,r.ended,r.model,r.billing_model,r.input,r.output,r.cached,r.manual_cost_atoms,r.usage_unknown,r.recording_state,r.price_version,p.revision,p.state price_state,p.total_atoms::text total_atoms FROM harbor.requests r LEFT JOIN harbor.request_prices p ON p.request_id=r.id WHERE r.id=ANY($1::text[]) ORDER BY r.id',expected.requestIds);assert.equal(rows.length,expected.requestIds.length);assert.ok(rows.every(row=>row.member_id===expected.memberId&&row.key_id===expected.keyId&&row.device_id===expected.deviceId&&row.device_generation===1&&row.status==='ok'&&!row.usage_unknown&&row.recording_state==='complete'&&row.price_version===row.revision),JSON.stringify(rows.map(row=>({memberMatches:row.member_id===expected.memberId,keyMatches:row.key_id===expected.keyId,deviceMatches:row.device_id===expected.deviceId,generation:row.device_generation,status:row.status,usageUnknown:row.usage_unknown,recordingState:row.recording_state,priceRevisionMatches:row.price_version===row.revision,priceState:row.price_state,hasPrice:row.total_atoms!==null}))));assert.equal(rows.reduce((sum,row)=>sum+row.input+row.output,0),expected.expectedTokens);for(const row of rows){const book=await store.pricingBook(row.price_version);assert.ok(book,'actual frozen request price book required');const quote=quoteReference(row,book,row.price_version);assert.equal(row.price_state,quote.state);assert.equal(row.total_atoms,quote.totalAtoms===null?null:String(quote.totalAtoms));}
  const documentParts=[],controlRequests=[],sourceCounts=new Map(),recordings=new RecordingStore(store,{encryptionKey:recordingKey});
  for(const id of expected.requestIds){
    const protocol=expected.protocolSummaries.find(row=>row.requestId===id),sourceEvents=(await store.get('SELECT COUNT(*) n FROM harbor.conversation_events WHERE request_id=$1',id)).n;sourceCounts.set(id,sourceEvents);
    const source=[];let after=0;
    for(;;){const page=await recordings.read(id,{after});assert.equal(page.state,'complete');source.push(...page.events);if(!page.hasMore)break;assert.ok(page.next>after);after=page.next;}
    assert.equal(source.length,sourceEvents);verifyCandidateToolEvents(source,protocol,'encrypted source');
    const encryptedSource=await store.all('SELECT sequence,content_cipher FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence',id);assert.equal(encryptedSource.length,source.length);
    for(const event of source.filter(event=>['tool_call','tool_result'].includes(event.kind))){const cipher=encryptedSource.find(row=>row.sequence===event.sequence)?.content_cipher;assert.ok(Buffer.isBuffer(cipher)&&!cipher.includes(Buffer.from(JSON.stringify(event.content))),'tool source payload must remain encrypted at rest');}
    const requestParts=[];after=0;
    for(;;){const page=await backend.queries.document(id,{after});assert.equal(page.index_pending,false);assert.equal(page.source_sequence,sourceEvents);requestParts.push(...page.items);if(!page.has_more)break;assert.ok(page.next>after);after=page.next;}
    documentParts.push(...requestParts);verifyCandidateToolDocument(requestParts,protocol);
    if(protocol.bodyExpected)assert.ok(source.some(event=>['user','assistant','tool_call','tool_result','attachment'].includes(event.kind))&&requestParts.some(part=>['user','assistant','tool_call','tool_result','attachment'].includes(part.kind)),'actual observed content cannot be replaced by boundary-only metadata');
    if(sourceEvents)assert.ok(requestParts.length>0,'nonempty actual source events require nonempty decrypted derived body');
    else assert.ok(controlOnly(protocol),'zero source events cannot exempt an observed tool round');
    if(!protocol.bodyExpected)controlRequests.push({inputTailType:protocol.inputTailType,outputTypes:protocol.outputTypes});
  }
  assert.ok(documentParts.some(part=>part.kind==='user'&&JSON.stringify(part).includes(expected.expectedUserText)),'actual CLI user text must be decrypted from a nonempty derived document');assert.ok(documentParts.some(part=>part.kind==='assistant'&&JSON.stringify(part).includes(expected.expectedAssistantText)),'actual final assistant text must be decrypted from a nonempty derived document');
  const exported=await backend.exports.enqueue({include_body:true,format:'jsonl',filters:{member:expected.memberId}});await candidateCondition(async()=>{const status=await backend.exports.status(exported.id);assert.ok(!['failed','cancelled','expired'].includes(status.status),'candidate body export failed');assert.ok(!['failed','cancelled'].includes(status.job_status),'candidate export job failed');return status.download_ready&&status.job_status==='completed';},'Candidate Worker did not complete actual nonempty body export');const state=await backend.exports.status(exported.id);assert.equal(state.rows,rows.length);assert.ok(state.bytes>0&&state.chunks>0);const job=await store.get('SELECT status,attempts FROM harbor.background_jobs WHERE id=$1',exported.id);assert.equal(job.status,'completed');assert.ok(job.attempts>0);
  const encrypted=await store.all('SELECT payload_cipher FROM harbor.recording_export_chunks WHERE export_id=$1',exported.id);assert.ok(encrypted.length>0);assert.ok(encrypted.every(row=>!row.payload_cipher.includes(Buffer.from(expected.expectedUserText))&&!row.payload_cipher.includes(Buffer.from(expected.expectedAssistantText))));let text='';for await(const chunk of backend.download(exported.id))text+=chunk.toString();const verified=verifyCandidateBodyExport({text,rows,expected,sourceCounts});assert.equal(workerChild.exitCode,null);assert.equal(workerChild.signalCode,null);
  return {independent_worker_completed_nonempty_body_export:true,actual_cli_body_indexed_and_decrypted:true,body_export_request_and_metering_identity_verified:true,body_export_request_rows:verified.requestRows,body_export_event_rows:verified.eventRows,actual_cli_tool_source_document_export_verified:true,tool_calls_verified:expected.protocolSummaries.reduce((sum,row)=>sum+row.toolCalls.length,0),tool_results_verified:expected.protocolSummaries.reduce((sum,row)=>sum+row.toolResults.length,0),textless_control_request_rows:controlRequests.length,textless_control_protocol_types:controlRequests,worker_scope:'actual independent candidate Worker CLI; no inline job execution'};
}
