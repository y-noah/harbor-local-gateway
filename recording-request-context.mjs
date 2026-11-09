import {sanitizeRecord,recordRedactionSummary,redactRecordParts,restoreAuthenticatedRecord,sensitiveRecordField,boundedRecordText} from './record-redaction.mjs';
import {safeToolOutput} from './conversation-capture.mjs';
import {recordingAttachment} from './recording-attachment.mjs';
import {recordingLiteral} from './recording-literal.mjs';
import {recordingTextMetadata,finalizeRecordingTokens} from './recording-text-metadata.mjs';

const media=new Set(['image','image_url','input_image','output_image','input_file','file','audio','input_audio','output_audio']);
const itemTypes=new Set(['message','additional_tools','function_call','function_call_output','custom_tool_call','custom_tool_call_output','item_reference','compaction']);
const textTypes=new Set(['text','input_text','output_text','summary_text','refusal']);
const scalarFields=new Set(['role','id','call_id','name','namespace','status','channel','phase','tool_call_id','input','arguments']);
const itemFields={additional_tools:['type','id','role','tools'],function_call:['type','id','call_id','name','namespace','arguments','status'],custom_tool_call:['type','id','call_id','name','namespace','input','status'],function_call_output:['type','id','call_id','name','namespace','output','status'],custom_tool_call_output:['type','id','call_id','name','namespace','output','status'],item_reference:['type','id'],compaction:['type','id','encrypted_content']};
const schemaBranches=new Set(['allOf','anyOf','oneOf','not','items','prefixItems','contains','additionalProperties','unevaluatedProperties','unevaluatedItems','propertyNames','if','then','else','additionalItems','contentSchema']);
export const requestContextReasons=Object.freeze(['invalid_payload','unsupported_tools','unsupported_input','unsupported_instructions','ambiguous_input','unsupported_content','unsupported_field','schema_name_collision','size_limit']);

// This view describes the received context, not a claim about the current turn
// or local execution. Authentication and scope come from the recording store.
function* requestContext(payload,{maxBytes=2*1024*1024,maxComponents=4096,maxNodes=65536,attachmentFingerprint}={}){
  if(!Number.isSafeInteger(maxBytes)||maxBytes<256||!Number.isSafeInteger(maxComponents)||maxComponents<1||!Number.isSafeInteger(maxNodes)||maxNodes<1)throw TypeError('Invalid request context budget');
  const redactions=new Set(),onRedact=category=>redactions.add(category);
  const components=[],reasons=new Set();let bytes=2,nodes=maxNodes,opaqueReasoningExcluded=0;
  let attachmentBytes=maxBytes,tokenSequence=[];
  const attachment=value=>recordingAttachment(value,{maxBytes:maxBytes-bytes,onTruncate:cut,onRedact,maxContentBytes:attachmentBytes,onInspect:count=>{attachmentBytes=Math.max(0,attachmentBytes-count);},fingerprint:attachmentFingerprint});
  const cut=()=>{tokenSequence.incomplete=true;reasons.add('size_limit');return '[已截断]';};
  const literal=(value,depth)=>depth>24?cut():recordingLiteral(value,{maxBytes:maxBytes-bytes,maxDepth:24-depth,onNodes:count=>{nodes-=count;return nodes>=0;},onTruncate:cut,onRedact});
  const visible=(value,depth=0,protocol=true,slot=null,sensitiveDefinition=false,schema=false)=>{
    if(--nodes<0||depth>24)return cut();
    if(schema==='tools'&&!Array.isArray(value)){reasons.add('unsupported_tools');return '[未支持的工具定义已省略]';}
    if(protocol&&(slot==='function'&&(!value||typeof value!=='object'||Array.isArray(value))||slot==='conversation'&&value!==null&&typeof value!=='string'&&(typeof value!=='object'||Array.isArray(value)))){reasons.add('unsupported_content');return '[未支持的上下文结构已省略]';}
    if(value instanceof ArrayBuffer||ArrayBuffer.isView(value)){onRedact('attachment_data');return {type:'binary',bytes:value.byteLength,content:'[附件数据已省略]'};}
    if(Array.isArray(value)){
      const result=[];for(const child of value){if(nodes<=0){result.push(cut());break;}if((protocol&&slot||schema==='tools')&&(!child||typeof child!=='object'||Array.isArray(child))){nodes--;reasons.add(schema==='tools'?'unsupported_tools':'unsupported_content');result.push({content:'[未支持的上下文结构已省略]'});continue;}result.push(visible(child,depth+1,protocol,slot,sensitiveDefinition,schema==='tools'?'envelope':schema));}
      const positions=[],texts=[];for(let i=0;i<result.length;i++){const field=result[i]?.type==='refusal'?'refusal':'text';if(textTypes.has(result[i]?.type)&&typeof result[i][field]==='string'){positions.push([i,field]);texts.push(result[i][field]);}}
      const filtered=redactRecordParts(texts,{maxBytes,maxParts:maxNodes,onTruncate:cut,onRedact}).parts;
      for(let i=0;i<positions.length;i++){const [position,field]=positions[i];result[position]={...result[position],[field]:filtered[i]??''};}return result;
    }
    if(!value||typeof value!=='object')return value;
    if(protocol&&media.has(value.type)){
      return attachment(value);
    }
    if(protocol&&value.type==='reasoning'){
      for(const key in value){if(--nodes<0){cut();break;}if(Object.hasOwn(value,key)&&!['type','id','summary','encrypted_content','status'].includes(key))reasons.add('unsupported_field');}
      if(value.encrypted_content!==undefined)opaqueReasoningExcluded++;
      if(value.summary!==undefined&&!Array.isArray(value.summary))reasons.add('unsupported_content');
      return {type:'reasoning',...(typeof value.id==='string'?{id:value.id}:{}),...(Array.isArray(value.summary)?{summary:visible(value.summary,depth+1,true,'summary')}:{})};
    }
    if(protocol&&['content','summary'].includes(slot)&&!textTypes.has(value.type)){reasons.add('unsupported_content');return {content:'[未支持的上下文结构已省略]'};}
    if(protocol&&textTypes.has(value.type)&&typeof value[value.type==='refusal'?'refusal':'text']==='string'&&value[value.type==='refusal'?'refusal':'text'].length&&(value.type!=='output_text'||!Array.isArray(value.logprobs)||!value.logprobs.length))tokenSequence.incomplete=true;
    const slotFields=slot==='function'?['name','namespace','arguments']:slot==='tool_call'?['id','type','function']:slot==='conversation'?['id']:slot==='tool_choice'?['type','name','namespace','function','mode','tools']:null;
    const fields=slotFields||itemFields[value.type]||(value.role!==undefined||value.type==='message'?['type','role','id','content','name','status','channel','phase',...(value.role==='assistant'?['tool_calls','refusal']:[]),...(value.role==='tool'?['tool_call_id']:[])]:null);
    if(protocol&&!slotFields&&value.type!==undefined&&!itemTypes.has(value.type)&&!textTypes.has(value.type)&&value.type!=='function'){reasons.add('unsupported_content');return {content:'[未支持的上下文结构已省略]'};}
    const result=Object.create(null);
    const messagePositions=[],messageTexts=[];
    if(protocol&&value.role==='assistant'&&typeof value.refusal==='string'){
      if(value.refusal.length)tokenSequence.incomplete=true;
      if(typeof value.content==='string'){messagePositions.push({field:'content'});messageTexts.push(value.content);}
      else if(Array.isArray(value.content))for(let index=0;index<value.content.length;index++){
        if(--nodes<0){cut();break;}const part=value.content[index],field=part?.type==='refusal'?'refusal':'text';
        if(textTypes.has(part?.type)&&typeof part[field]==='string'){messagePositions.push({field:'content',index,leaf:field});messageTexts.push(part[field]);}
      }
      messagePositions.push({field:'refusal'});messageTexts.push(value.refusal);
    }
    const messageSafe=messageTexts.length?redactRecordParts(messageTexts,{maxBytes:maxBytes-bytes,maxParts:maxNodes,onTruncate:cut,onRedact}).parts:[];
    for(const key in value){
      if(--nodes<0){cut();break;}
      if(!Object.hasOwn(value,key))continue;
      // Never retain opaque compaction/reasoning blobs, even as redacted data.
      if(protocol&&['encrypted_content','reasoning_content','thinking'].includes(key)){opaqueReasoningExcluded++;continue;}
      if(protocol&&fields&&!fields.includes(key)){reasons.add('unsupported_field');continue;}
      const nullableMetadata=['phase','namespace'].includes(key)&&value[key]===null;
      if(protocol&&slotFields&&['id','type','name','namespace','arguments','mode'].includes(key)&&typeof value[key]!=='string'&&!nullableMetadata){reasons.add('unsupported_content');continue;}
      if(protocol&&(value.role!==undefined||itemTypes.has(value.type))&&scalarFields.has(key)&&typeof value[key]!=='string'&&!nullableMetadata){reasons.add('unsupported_content');continue;}
      if(protocol&&key==='tool_calls'&&!Array.isArray(value[key])){reasons.add('unsupported_content');continue;}
      if(protocol&&textTypes.has(value.type)&&!['type',value.type==='refusal'?'refusal':'text',...(value.type==='output_text'?['annotations','logprobs']:[])].includes(key)){reasons.add('unsupported_field');continue;}
      if(protocol&&value.type==='output_text'&&['annotations','logprobs'].includes(key)){
        result[key]=recordingTextMetadata(key,value[key],{maxBytes:maxBytes-bytes,onNodes:count=>{nodes-=count;return nodes>=0;},onTruncate:cut,onRedact,tokenSequence,expectedText:value.text,onUnsupported:reason=>reasons.add(reason)});continue;
      }
      if(protocol&&value.type==='additional_tools'&&key==='tools'){
        result.tools=visible(value.tools,depth+1,false,null,false,'tools');continue;
      }
      if(protocol&&textTypes.has(value.type)&&['text','refusal'].includes(key)&&typeof value[key]!=='string'){reasons.add('unsupported_content');continue;}
      if(protocol&&value.role!==undefined&&key==='refusal'&&value[key]!==null&&typeof value[key]!=='string'){reasons.add('unsupported_content');continue;}
      if(protocol&&value.role!==undefined&&key==='content'&&value[key]!==null&&typeof value[key]!=='string'&&!Array.isArray(value[key])){reasons.add('unsupported_content');continue;}
      if(protocol&&(key==='output'&&['function_call_output','custom_tool_call_output'].includes(value.type)||key==='content'&&value.role==='tool')){
        Object.defineProperty(result,key,{value:safeToolOutput(value[key],{maxBytes:maxBytes-bytes,maxNodes:Math.max(1,nodes),contentParts:Array.isArray(value[key]),onTruncate:cut,onRedact,onUnsupported:()=>reasons.add('unsupported_content'),onNodes:count=>{nodes-=count;return nodes>=0;},attachmentMetadata:attachment}),enumerable:true});continue;
      }
      if(schema==='definition'&&['properties','patternProperties','$defs','definitions','dependentSchemas','dependentRequired','dependencies'].includes(key)&&value[key]&&typeof value[key]==='object'&&!Array.isArray(value[key])){
        const definitions={};
        for(const name in value[key]){
          if(--nodes<0){cut();break;}if(!Object.hasOwn(value[key],name))continue;
          const prefix=boundedRecordText(name,256);if(prefix!==name){cut();continue;}
          const safeName=sanitizeRecord(prefix,{onRedact});if(Object.hasOwn(definitions,safeName)){reasons.add('schema_name_collision');continue;}
          const definition=key==='dependentRequired'||key==='dependencies'&&Array.isArray(value[key][name])?literal(value[key][name],depth+2):visible(value[key][name],depth+2,false,null,sensitiveDefinition||sensitiveRecordField(name),'definition');
          Object.defineProperty(definitions,safeName,{value:sanitizeRecord(definition,{maxBytes,maxNodes,onTruncate:cut,onRedact}),enumerable:true});
        }
        Object.defineProperty(result,key,{value:restoreAuthenticatedRecord(definitions),enumerable:true});continue;
      }
      const nextSlot=!protocol?null:['content','summary'].includes(key)?key:key==='tool_calls'?'tool_call':key==='function'?'function':key==='tools'&&slot==='tool_choice'?'tool_choice':key==='value'&&['conversation','tool_choice'].includes(value.source)?value.source:null;
      const nextSchema=schema==='envelope'?['parameters','input_schema','schema'].includes(key)?'definition':key==='tools'?'tools':['value','function','format'].includes(key)?'envelope':false:schema==='definition'&&schemaBranches.has(key)?'definition':false;
      if(schema==='definition'&&sensitiveDefinition&&['default','examples','const','enum'].includes(key))onRedact('sensitive_field');
      let child=schema==='definition'&&sensitiveDefinition&&['default','examples','const','enum'].includes(key)?'[已脱敏]':schema&&!nextSchema?literal(value[key],depth+1):visible(value[key],depth+1,protocol&&!['input','output','arguments'].includes(key),nextSlot,sensitiveDefinition,nextSchema);
      for(let index=0;index<messagePositions.length;index++){
        const position=messagePositions[index];if(position.field!==key)continue;
        if(position.index===undefined)child=messageSafe[index]??'';
        else if(Array.isArray(child)&&textTypes.has(child[position.index]?.type))child[position.index]={...child[position.index],[position.leaf]:messageSafe[index]??''};
      }
      Object.defineProperty(result,key,{value:child,enumerable:true});
    }
    return result;
  };
  const add=(kind,content)=>{
    if(components.length>=maxComponents||bytes>=maxBytes||nodes<=0){cut();return false;}
    // Sanitize before returning anything to a queue, cache, or persistence API.
    tokenSequence=[];const envelope=kind==='tool_schema'&&content.source==='tools',prepared=visible(content,0,!envelope,null,false,envelope?'envelope':false);let budget=maxBytes-bytes,component,size;
    finalizeRecordingTokens(tokenSequence,{maxBytes:budget,onTruncate:cut,onRedact});
    for(;;){
      const safe=sanitizeRecord(prepared,{maxBytes:budget,maxNodes,onTruncate:cut,onRedact});component={kind,content:safe};size=Buffer.byteLength(JSON.stringify(component))+(components.length?1:0);
      if(bytes+size<=maxBytes||budget===0)break;cut();budget=Math.floor(budget/2);
    }
    if(bytes+size>maxBytes){cut();return false;}components.push(component);bytes+=size;return true;
  };
  if(!payload||typeof payload!=='object'||Array.isArray(payload))return {state:'incomplete',components,bytes,reasons:['invalid_payload'],opaqueReasoningExcluded,redaction:recordRedactionSummary(redactions)};
  if(typeof payload.instructions==='string'){add('instruction',{source:'instructions',value:payload.instructions});yield;}
  else if(payload.instructions!==undefined&&payload.instructions!==null)reasons.add('unsupported_instructions');
  if(Array.isArray(payload.tools))for(let index=0;index<payload.tools.length;index++){if(nodes<=0){cut();break;}const tool=payload.tools[index];if(!tool||typeof tool!=='object'||Array.isArray(tool)){nodes--;reasons.add('unsupported_tools');yield;continue;}if(!add('tool_schema',{source:'tools',index,value:tool}))break;yield;}
  else if(payload.tools!==undefined)reasons.add('unsupported_tools');
  const input=payload.input??payload.messages,source=payload.input===undefined?'messages':'input';
  if(payload.input!==undefined&&payload.messages!==undefined)reasons.add('ambiguous_input');
  if(typeof input==='string'){add('context',{source,value:input});yield;}
  else if(Array.isArray(input)){
    for(let index=0;index<input.length;index++){
      if(--nodes<0){cut();break;}
      const item=input[index];
      if(!item||typeof item!=='object'||Array.isArray(item)||!(itemTypes.has(item.type)||item.type==='reasoning'||['user','assistant','system','developer','tool'].includes(item.role))){reasons.add('unsupported_input');yield;continue;}
      if(!add(item.type==='additional_tools'?'tool_schema':['system','developer'].includes(item.role)?'instruction':'context',{source,index,value:item}))break;
      yield;
    }
  }else if(input!==undefined)reasons.add('unsupported_input');
  // Relationships are protocol evidence only; no inference about ancestry.
  for(const key of ['previous_response_id','conversation','tool_choice','parallel_tool_calls','truncation'])if(payload[key]!==undefined){
    const value=payload[key],valid=value===null||key==='parallel_tool_calls'&&typeof value==='boolean'||key!=='parallel_tool_calls'&&typeof value==='string'||['conversation','tool_choice'].includes(key)&&typeof value==='object'&&!Array.isArray(value);
    if(!valid){reasons.add('unsupported_content');continue;}add('context',{source:key,value});yield;
  }
  return {state:reasons.size?'incomplete':'complete',components,bytes,reasons:[...reasons].sort(),opaqueReasoningExcluded,redaction:recordRedactionSummary(redactions)};
}

// Both entry points consume the same filtering and budget state machine.
export function captureRequestContext(payload,options){
  const iterator=requestContext(payload,options);let step;
  do{step=iterator.next();}while(!step.done);return step.value;
}

export async function captureRequestContextAsync(payload,{sliceMs=5,signal,...options}={}){
  if(typeof sliceMs!=='number'||!Number.isFinite(sliceMs)||sliceMs<=0)throw TypeError('Invalid context capture slice');
  const iterator=requestContext(payload,options);let deadline=performance.now()+sliceMs;
  try{
    for(;;){
      signal?.throwIfAborted();const step=iterator.next();if(step.done)return step.value;
      if(performance.now()>=deadline){await new Promise(setImmediate);signal?.throwIfAborted();deadline=performance.now()+sliceMs;}
    }
  }finally{iterator.return();}
}
