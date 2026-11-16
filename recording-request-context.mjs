import {observeRecordingInputShape} from './recording-input-diagnostics.mjs';
import {sanitizeRecord,recordRedactionSummary,redactRecordParts,restoreAuthenticatedRecord,sensitiveRecordField,boundedRecordText} from './record-redaction.mjs';
import {safeToolOutput} from './conversation-capture.mjs';
import {recordingAttachment} from './recording-attachment.mjs';
import {recordingLiteral} from './recording-literal.mjs';
import {recordingTextMetadata,finalizeRecordingTokens} from './recording-text-metadata.mjs';

const media=new Set(['image','image_url','input_image','output_image','input_file','file','audio','input_audio','output_audio']);
// Parser support is independent of diagnostic recognition: observing a new
// protocol must not admit it through the generic field walker below.
const itemTypes=new Set(['message','additional_tools','function_call','function_call_output','custom_tool_call','custom_tool_call_output','shell_call','shell_call_output','apply_patch_call','apply_patch_call_output','code_interpreter_call','mcp_call','mcp_list_tools','mcp_approval_request','mcp_approval_response','web_search_call','file_search_call','computer_call','computer_call_output','item_reference','compaction']);
const textTypes=new Set(['text','input_text','output_text','summary_text','refusal']);
const scalarFields=new Set(['role','id','call_id','name','namespace','status','channel','phase','tool_call_id','input','arguments','code','container_id','server_label','approval_request_id','reason']);
const toolItems=new Set(['function_call','custom_tool_call','function_call_output','custom_tool_call_output','shell_call','shell_call_output','apply_patch_call','apply_patch_call_output','code_interpreter_call','mcp_call','mcp_list_tools','mcp_approval_request','mcp_approval_response','web_search_call','file_search_call','computer_call','computer_call_output']);
const toolFacts=['status','error','duration_ms','elapsed_ms'];
const itemFields={additional_tools:['type','id','role','tools'],function_call:['type','id','call_id','name','namespace','arguments',...toolFacts],custom_tool_call:['type','id','call_id','name','namespace','input',...toolFacts],function_call_output:['type','id','call_id','name','namespace','output',...toolFacts],custom_tool_call_output:['type','id','call_id','name','namespace','output',...toolFacts],item_reference:['type','id'],compaction:['type','id','encrypted_content']};
Object.assign(itemFields,{shell_call:['type','id','call_id','action',...toolFacts],shell_call_output:['type','id','call_id','output','max_output_length',...toolFacts],apply_patch_call:['type','id','call_id','operation',...toolFacts],apply_patch_call_output:['type','id','call_id','output',...toolFacts]});
Object.assign(itemFields,{code_interpreter_call:['type','id','code','container_id','outputs',...toolFacts],mcp_call:['type','id','name','server_label','arguments','approval_request_id','output',...toolFacts]});
Object.assign(itemFields,{mcp_list_tools:['type','id','server_label','tools','error'],mcp_approval_request:['type','id','arguments','name','server_label'],mcp_approval_response:['type','approval_request_id','approve','id','reason']});
Object.assign(itemFields,{web_search_call:['type','id','status','action'],file_search_call:['type','id','status','queries','results'],computer_call:['type','id','call_id','status','pending_safety_checks','action','actions'],computer_call_output:['type','call_id','id','status','output','acknowledged_safety_checks']});
const callerItems=new Set(['function_call','function_call_output','custom_tool_call','custom_tool_call_output','shell_call','shell_call_output','apply_patch_call','apply_patch_call_output']);
for(const type of callerItems)itemFields[type].push('caller');
for(const type of ['function_call','custom_tool_call'])itemFields[type].push('async');
itemFields.shell_call.push('environment');
const callerFields={direct:['type'],program:['type','caller_id']};
const shellEnvironmentFields={local:['type','skills'],container_reference:['type','container_id']};
const nullableItemIds=new Set(['function_call_output','shell_call','shell_call_output','apply_patch_call','apply_patch_call_output']);
const computerActionFields={click:['type','button','x','y','keys'],double_click:['type','x','y','keys'],drag:['type','path','keys'],keypress:['type','keys'],move:['type','x','y','keys'],screenshot:['type'],scroll:['type','x','y','scroll_x','scroll_y','keys'],type:['type','text'],wait:['type']};
const webActionFields={search:['type','query','queries','sources'],open_page:['type','url'],find_in_page:['type','pattern','url']};
const factFields={web_source:['type','url'],file_result:['attributes','file_id','filename','score','text'],safety_check:['id','code','message'],coordinate:['x','y'],local_skill:['name','description','path']};
const factSlots=new Set([...Object.keys(factFields),'web_action','computer_action','tool_caller','shell_environment']);
const schemaBranches=new Set(['allOf','anyOf','oneOf','not','items','prefixItems','contains','additionalProperties','unevaluatedProperties','unevaluatedItems','propertyNames','if','then','else','additionalItems','contentSchema']);
export const requestContextReasons=Object.freeze(['invalid_payload','unsupported_tools','unsupported_input','unsupported_instructions','ambiguous_input','unsupported_content','unsupported_field','schema_name_collision','size_limit']);

// This view describes the received context, not a claim about the current turn
// or local execution. Authentication and scope come from the recording store.
function* requestContext(payload,{maxBytes=2*1024*1024,maxComponents=4096,maxNodes=65536,attachmentFingerprint,onUnsupportedInputShape}={}){
  if(!Number.isSafeInteger(maxBytes)||maxBytes<256||!Number.isSafeInteger(maxComponents)||maxComponents<1||!Number.isSafeInteger(maxNodes)||maxNodes<1)throw TypeError('Invalid request context budget');
  const redactions=new Set(),onRedact=category=>redactions.add(category);
  const components=[],reasons=new Set();let bytes=2,nodes=maxNodes,opaqueReasoningExcluded=0;
  let attachmentBytes=maxBytes,tokenSequence=[];
  const attachment=value=>recordingAttachment(value,{maxBytes:maxBytes-bytes,onTruncate:cut,onRedact,maxContentBytes:attachmentBytes,onInspect:count=>{attachmentBytes=Math.max(0,attachmentBytes-count);},fingerprint:attachmentFingerprint});
  const cut=()=>{tokenSequence.incomplete=true;reasons.add('size_limit');return '[已截断]';};
  const literal=(value,depth)=>depth>24?cut():recordingLiteral(value,{maxBytes:maxBytes-bytes,maxDepth:24-depth,onNodes:count=>{nodes-=count;return nodes>=0;},onTruncate:cut,onRedact});
  const strings=value=>{
    if(!Array.isArray(value)){reasons.add('unsupported_content');return '[未支持的字符串列表已省略]';}
    const parts=[];for(const child of value){if(--nodes<0){cut();break;}if(typeof child!=='string'){reasons.add('unsupported_content');parts.push('[未支持的内容已省略]');}else parts.push(child);}
    return redactRecordParts(parts,{maxBytes:maxBytes-bytes,maxParts:maxNodes,onTruncate:cut,onRedact}).parts;
  };
  const visible=(value,depth=0,protocol=true,slot=null,sensitiveDefinition=false,schema=false)=>{
    if(--nodes<0||depth>24)return cut();
    if(schema==='tools'&&!Array.isArray(value)){reasons.add('unsupported_tools');return '[未支持的工具定义已省略]';}
    if(protocol&&(slot==='function'&&(!value||typeof value!=='object'||Array.isArray(value))||slot==='conversation'&&value!==null&&typeof value!=='string'&&(typeof value!=='object'||Array.isArray(value)))){reasons.add('unsupported_content');return '[未支持的上下文结构已省略]';}
    if(value instanceof ArrayBuffer||ArrayBuffer.isView(value)){onRedact('attachment_data');return {type:'binary',bytes:value.byteLength,content:'[附件数据已省略]'};}
    if(Array.isArray(value)){
      // Filter raw logs together before per-field filtering consumes a split
      // credential prefix. Keep the same node/byte budget as other fragments.
      let codeLogs=null,resultTexts=null;const factTextKey=slot==='file_result'||slot==='computer_action'?'text':slot==='safety_check'?'message':slot==='local_skill'?'description':null;
      if(protocol&&slot==='code_output'){
        const parts=[];for(const child of value){if(parts.length>=nodes){cut();break;}parts.push(child?.type==='logs'&&typeof child.logs==='string'?child.logs:'');}
        codeLogs=redactRecordParts(parts,{maxBytes:maxBytes-bytes,maxParts:maxNodes,onTruncate:cut,onRedact}).parts;
      }
      if(protocol&&factTextKey){const parts=[];for(const child of value){if(parts.length>=nodes){cut();break;}parts.push(typeof child?.[factTextKey]==='string'?child[factTextKey]:'');}resultTexts=redactRecordParts(parts,{maxBytes:maxBytes-bytes,maxParts:maxNodes,onTruncate:cut,onRedact}).parts;}
      const result=[];let inputIndex=0;for(const child of value){const index=inputIndex++;if(nodes<=0){result.push(cut());break;}if((protocol&&slot||schema==='tools')&&(!child||typeof child!=='object'||Array.isArray(child))){nodes--;reasons.add(schema==='tools'?'unsupported_tools':'unsupported_content');result.push({content:'[未支持的上下文结构已省略]'});continue;}const item=visible(child,depth+1,protocol,slot,sensitiveDefinition,schema==='tools'?'envelope':schema);result.push(codeLogs&&item?.type==='logs'&&typeof child.logs==='string'?{...item,logs:codeLogs[index]??''}:resultTexts&&typeof child?.[factTextKey]==='string'&&typeof item?.[factTextKey]==='string'?{...item,[factTextKey]:resultTexts[index]??''}:item);}
      const positions=[],texts=[];for(let i=0;i<result.length;i++){const field=result[i]?.type==='refusal'?'refusal':'text';if(textTypes.has(result[i]?.type)&&typeof result[i][field]==='string'){positions.push([i,field]);texts.push(result[i][field]);}}
      const filtered=redactRecordParts(texts,{maxBytes,maxParts:maxNodes,onTruncate:cut,onRedact}).parts;
      for(let i=0;i<positions.length;i++){const [position,field]=positions[i];result[position]={...result[position],[field]:filtered[i]??''};}return result;
    }
    if(protocol&&factSlots.has(slot)&&(!value||typeof value!=='object'||Array.isArray(value))){reasons.add('unsupported_content');return {content:'[未支持的上下文结构已省略]'};}
    if(!value||typeof value!=='object')return value;
    if(protocol&&(slot==='web_action'&&!Object.hasOwn(webActionFields,value.type)||slot==='computer_action'&&!Object.hasOwn(computerActionFields,value.type)||slot==='web_source'&&value.type!=='url'||slot==='tool_caller'&&!Object.hasOwn(callerFields,value.type)||slot==='shell_environment'&&!Object.hasOwn(shellEnvironmentFields,value.type))){reasons.add('unsupported_content');return {content:'[未支持的工具动作已省略]'};}
    if(protocol&&slot==='code_output'&&(!['logs','image'].includes(value.type)||value.type==='logs'&&typeof value.logs!=='string')){reasons.add('unsupported_content');return {content:'[未支持的上下文结构已省略]'};}
    if(protocol&&slot!=='mcp_tool'&&!factSlots.has(slot)&&media.has(value.type)){
      return attachment(value);
    }
    if(protocol&&slot!=='mcp_tool'&&!factSlots.has(slot)&&value.type==='reasoning'){
      // Raw reasoning content is a known exclusion, just like encrypted content;
      // do not inspect or persist it when retaining the public summary.
      for(const key in value){if(--nodes<0){cut();break;}if(Object.hasOwn(value,key)&&!['type','id','summary','content','encrypted_content','status'].includes(key))reasons.add('unsupported_field');}
      if(value.encrypted_content!==undefined||Object.hasOwn(value,'content'))opaqueReasoningExcluded++;
      if(value.summary!==undefined&&!Array.isArray(value.summary))reasons.add('unsupported_content');
      return {type:'reasoning',...(typeof value.id==='string'?{id:value.id}:{}),...(Array.isArray(value.summary)?{summary:visible(value.summary,depth+1,true,'summary')}:{})};
    }
    if(protocol&&['content','summary'].includes(slot)&&!textTypes.has(value.type)){reasons.add('unsupported_content');return {content:'[未支持的上下文结构已省略]'};}
    if(protocol&&textTypes.has(value.type)&&typeof value[value.type==='refusal'?'refusal':'text']==='string'&&value[value.type==='refusal'?'refusal':'text'].length&&(value.type!=='output_text'||!Array.isArray(value.logprobs)||!value.logprobs.length))tokenSequence.incomplete=true;
    const slotFields=slot==='function'?['name','namespace','arguments']:slot==='tool_call'?['id','type','function']:slot==='conversation'?['id']:slot==='tool_choice'?['type','name','namespace','function','mode','tools']:slot==='code_output'?['type','logs']:slot==='mcp_tool'?['name','input_schema','annotations','description']:slot==='web_action'?webActionFields[value.type]:slot==='computer_action'?computerActionFields[value.type]:slot==='tool_caller'?callerFields[value.type]:slot==='shell_environment'?shellEnvironmentFields[value.type]:factFields[slot]||null;
    const fields=slotFields||itemFields[value.type]||(value.role!==undefined||value.type==='message'?['type','role','id','content','name','status','channel','phase',...(value.role==='assistant'?['tool_calls','refusal']:[]),...(value.role==='tool'?['tool_call_id']:[])]:null);
    if(protocol&&!slotFields&&value.type!==undefined&&!itemTypes.has(value.type)&&!textTypes.has(value.type)&&value.type!=='function'){reasons.add('unsupported_content');return {content:'[未支持的上下文结构已省略]'};}
    const result=Object.create(null);
    let searchTexts=null;const searchQuery=protocol&&slot==='web_action'&&value.type==='search'&&typeof value.query==='string';
    if(protocol&&slot==='web_action'&&value.type==='search'){const parts=searchQuery?[value.query]:[];if(Array.isArray(value.queries))for(const query of value.queries){if(parts.length>=nodes){cut();break;}parts.push(typeof query==='string'?query:'');}searchTexts=redactRecordParts(parts,{maxBytes:maxBytes-bytes,maxParts:maxNodes,onTruncate:cut,onRedact}).parts;}
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
      const nullableMetadata=(['phase','namespace'].includes(key)||toolItems.has(value.type)&&key==='status'||value.type==='code_interpreter_call'&&key==='code'||value.type==='mcp_call'&&key==='approval_request_id'||value.type==='mcp_approval_response'&&['id','reason'].includes(key)||slot==='mcp_tool'&&key==='description'||value.type==='computer_call_output'&&key==='id'||slot==='safety_check'&&['code','message'].includes(key)||!slotFields&&(key==='id'&&nullableItemIds.has(value.type)||value.type==='function_call_output'&&['call_id','name'].includes(key)))&&value[key]===null;
      if(protocol&&slotFields&&['id','type','name','namespace','arguments','mode'].includes(key)&&typeof value[key]!=='string'&&!nullableMetadata){reasons.add('unsupported_content');continue;}
      if(protocol&&(value.role!==undefined||itemTypes.has(value.type))&&scalarFields.has(key)&&typeof value[key]!=='string'&&!nullableMetadata){reasons.add('unsupported_content');continue;}
      if(protocol&&slot==='mcp_tool'&&(['name','description'].includes(key)&&typeof value[key]!=='string'&&!nullableMetadata)){reasons.add('unsupported_content');continue;}
      if(protocol&&factSlots.has(slot)){
        const stringField=(slot==='web_action'&&['query','pattern','url'].includes(key)||slot==='web_source'&&key==='url'||slot==='file_result'&&['file_id','filename','text'].includes(key)||slot==='safety_check'&&['id','code','message'].includes(key)||slot==='computer_action'&&['button','text'].includes(key)||slot==='tool_caller'&&key==='caller_id'||slot==='shell_environment'&&key==='container_id'||slot==='local_skill'&&['name','description','path'].includes(key));
        const nullable=(slot==='web_action'&&key==='url'&&value.type!=='search'||slot==='safety_check'&&['code','message'].includes(key))&&value[key]===null;
        if(stringField&&typeof value[key]!=='string'&&!nullable){reasons.add('unsupported_content');continue;}
        if(['x','y','scroll_x','scroll_y'].includes(key)&&!Number.isSafeInteger(value[key])){reasons.add('unsupported_content');continue;}
        if(slot==='computer_action'&&key==='button'&&!['left','right','wheel','back','forward'].includes(value[key])){reasons.add('unsupported_content');continue;}
        if(slot==='file_result'&&key==='score'&&(!Number.isFinite(value[key])||value[key]<0||value[key]>1)){reasons.add('unsupported_content');continue;}
        if(slot==='file_result'&&key==='attributes'&&value[key]!==null){let valid=!!value[key]&&typeof value[key]==='object'&&!Array.isArray(value[key]);if(valid)for(const name in value[key]){if(--nodes<0){cut();valid=false;break;}if(!Object.hasOwn(value[key],name))continue;const child=value[key][name];if(!['string','number','boolean'].includes(typeof child)||typeof child==='number'&&!Number.isFinite(child)){valid=false;break;}}if(!valid){reasons.add('unsupported_content');continue;}}
        if(['url','file_id'].includes(key)){result[key]=value[key]===null?null:'[引用已省略]';if(value[key]!==null)onRedact('attachment_data');continue;}
        if(key==='attributes'){result[key]=literal(value[key],depth+1);continue;}
        if(['queries','keys'].includes(key)){const list=key==='keys'&&value[key]===null&&value.type!=='keypress'?null:strings(value[key]);result[key]=searchTexts&&key==='queries'&&Array.isArray(list)?list.map((part,index)=>typeof value[key][index]==='string'?searchTexts[index+(searchQuery?1:0)]??'':part):list;continue;}
        if(searchTexts&&key==='query'){result.query=searchTexts[0]??'';continue;}
        if(slot==='web_action'&&key==='sources'||slot==='computer_action'&&key==='path'){if(!Array.isArray(value[key])){reasons.add('unsupported_content');continue;}result[key]=visible(value[key],depth+1,true,key==='sources'?'web_source':'coordinate');continue;}
      }
      if(protocol&&key==='caller'&&callerItems.has(value.type)){
        if(value.caller!==null&&(!value.caller||typeof value.caller!=='object'||Array.isArray(value.caller))){reasons.add('unsupported_content');continue;}result.caller=value.caller===null?null:visible(value.caller,depth+1,true,'tool_caller');continue;
      }
      if(protocol&&key==='async'&&['function_call','custom_tool_call'].includes(value.type)&&typeof value.async!=='boolean'){reasons.add('unsupported_content');continue;}
      if(protocol&&key==='environment'&&value.type==='shell_call'){
        if(value.environment!==null&&(!value.environment||typeof value.environment!=='object'||Array.isArray(value.environment))){reasons.add('unsupported_content');continue;}result.environment=value.environment===null?null:visible(value.environment,depth+1,true,'shell_environment');continue;
      }
      if(protocol&&slot==='shell_environment'&&key==='skills'){
        if(!Array.isArray(value.skills)){reasons.add('unsupported_content');continue;}result.skills=visible(value.skills,depth+1,true,'local_skill');continue;
      }
      if(protocol&&key==='status'&&['web_search_call','file_search_call','computer_call','computer_call_output'].includes(value.type)){
        const allowed=value.type.startsWith('computer_')?['in_progress','completed','incomplete']:['in_progress','searching','completed','failed','incomplete'];if(!(value.type==='computer_call_output'&&value.status===null)&&!allowed.includes(value.status)){reasons.add('unsupported_content');continue;}
      }
      if(protocol&&value.type==='file_search_call'&&key==='queries'){result.queries=strings(value.queries);continue;}
      if(protocol&&value.type==='file_search_call'&&key==='results'||protocol&&['pending_safety_checks','acknowledged_safety_checks','actions'].includes(key)&&['computer_call','computer_call_output'].includes(value.type)){
        const nullable=value[key]===null&&(key==='results'||key==='acknowledged_safety_checks');if(!nullable&&!Array.isArray(value[key])){reasons.add('unsupported_content');continue;}
        result[key]=nullable?null:visible(value[key],depth+1,true,key==='results'?'file_result':key==='actions'?'computer_action':'safety_check');continue;
      }
      if(protocol&&key==='action'&&['web_search_call','computer_call'].includes(value.type)){
        if(!value.action||typeof value.action!=='object'||Array.isArray(value.action)){reasons.add('unsupported_content');continue;}result.action=visible(value.action,depth+1,true,value.type==='web_search_call'?'web_action':'computer_action');continue;
      }
      if(protocol&&value.type==='computer_call_output'&&key==='output'){
        const output=value.output;if(!output||typeof output!=='object'||Array.isArray(output)||output.type!=='computer_screenshot'){reasons.add('unsupported_content');continue;}
        for(const field in output){if(!Object.hasOwn(output,field))continue;if(--nodes<0){cut();break;}if(!['type','file_id','image_url'].includes(field))reasons.add('unsupported_field');else if(typeof output[field]!=='string')reasons.add('unsupported_content');}
        result.output=attachment({type:'computer_screenshot',...(typeof output.file_id==='string'?{file_id:output.file_id}:{}),...(typeof output.image_url==='string'?{image_url:output.image_url}:{})});continue;
      }
      if(protocol&&value.type==='mcp_approval_response'&&key==='approve'&&typeof value[key]!=='boolean'){reasons.add('unsupported_content');continue;}
      if(protocol&&value.type==='mcp_list_tools'&&(key==='tools'&&!Array.isArray(value[key])||key==='error'&&value[key]!==null&&typeof value[key]!=='string')){reasons.add('unsupported_content');continue;}
      if(protocol&&toolItems.has(value.type)&&['duration_ms','elapsed_ms'].includes(key)&&(!Number.isFinite(value[key])||value[key]<0)){reasons.add('unsupported_content');continue;}
      if(protocol&&value.type==='shell_call_output'&&key==='max_output_length'&&value[key]!==null&&(!Number.isSafeInteger(value[key])||value[key]<0)){reasons.add('unsupported_content');continue;}
      if(protocol&&(value.type==='shell_call'&&key==='action'||value.type==='apply_patch_call'&&key==='operation')&&(!value[key]||typeof value[key]!=='object'||Array.isArray(value[key]))){reasons.add('unsupported_content');continue;}
      if(protocol&&(value.type==='code_interpreter_call'&&key==='outputs'&&value[key]!==null&&!Array.isArray(value[key])||value.type==='mcp_call'&&key==='output'&&value[key]!==null&&typeof value[key]!=='string')){reasons.add('unsupported_content');continue;}
      if(protocol&&key==='tool_calls'&&!Array.isArray(value[key])){reasons.add('unsupported_content');continue;}
      if(protocol&&textTypes.has(value.type)&&!['type',value.type==='refusal'?'refusal':'text',...(value.type==='output_text'?['annotations','logprobs']:[])].includes(key)){reasons.add('unsupported_field');continue;}
      if(protocol&&value.type==='output_text'&&['annotations','logprobs'].includes(key)){
        result[key]=recordingTextMetadata(key,value[key],{maxBytes:maxBytes-bytes,onNodes:count=>{nodes-=count;return nodes>=0;},onTruncate:cut,onRedact,tokenSequence,expectedText:value.text,onUnsupported:reason=>reasons.add(reason)});continue;
      }
      if(protocol&&value.type==='mcp_list_tools'&&key==='tools'){
        result.tools=visible(value.tools,depth+1,true,'mcp_tool');continue;
      }
      if(protocol&&slot==='mcp_tool'&&key==='input_schema'){
        result.input_schema=visible(value.input_schema,depth+1,false,null,false,'definition');continue;
      }
      if(protocol&&slot==='mcp_tool'&&key==='annotations'){
        result.annotations=literal(value.annotations,depth+1);continue;
      }
      if(protocol&&value.type==='additional_tools'&&key==='tools'){
        result.tools=visible(value.tools,depth+1,false,null,false,'tools');continue;
      }
      if(protocol&&value.type==='code_interpreter_call'&&key==='outputs'){
        result.outputs=value.outputs===null?null:visible(value.outputs,depth+1,true,'code_output');continue;
      }
      if(protocol&&textTypes.has(value.type)&&['text','refusal'].includes(key)&&typeof value[key]!=='string'){reasons.add('unsupported_content');continue;}
      if(protocol&&value.role!==undefined&&key==='refusal'&&value[key]!==null&&typeof value[key]!=='string'){reasons.add('unsupported_content');continue;}
      if(protocol&&value.role!==undefined&&key==='content'&&value[key]!==null&&typeof value[key]!=='string'&&!Array.isArray(value[key])){reasons.add('unsupported_content');continue;}
      if(protocol&&(key==='output'&&toolItems.has(value.type)&&value.type.endsWith('_output')||key==='action'&&value.type==='shell_call'||key==='operation'&&value.type==='apply_patch_call'||value.type==='code_interpreter_call'&&key==='code'||['mcp_call','mcp_approval_request'].includes(value.type)&&['arguments','output'].includes(key)||value.type==='mcp_approval_response'&&key==='reason'||key==='error'&&toolItems.has(value.type)||key==='content'&&value.role==='tool')){
        Object.defineProperty(result,key,{value:safeToolOutput(value[key],{maxBytes:maxBytes-bytes,maxNodes:Math.max(1,nodes),protocolContent:key!=='error',contentParts:Array.isArray(value[key])&&(key==='content'||key==='output'&&['function_call_output','custom_tool_call_output'].includes(value.type)),onTruncate:cut,onRedact,onUnsupported:()=>reasons.add('unsupported_content'),onNodes:count=>{nodes-=count;return nodes>=0;},attachmentMetadata:attachment}),enumerable:true});continue;
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
  if(!payload||typeof payload!=='object'||Array.isArray(payload))return {state:'incomplete',components,bytes,nodesConsumed:0,reasons:['invalid_payload'],opaqueReasoningExcluded,redaction:recordRedactionSummary(redactions)};
  if(typeof payload.instructions==='string'){add('instruction',{source:'instructions',value:payload.instructions});yield;}
  else if(payload.instructions!==undefined&&payload.instructions!==null)reasons.add('unsupported_instructions');
  if(Array.isArray(payload.tools))for(let index=0;index<payload.tools.length;index++){if(nodes<=0){cut();break;}const tool=payload.tools[index];if(!tool||typeof tool!=='object'||Array.isArray(tool)){nodes--;reasons.add('unsupported_tools');yield;continue;}if(!add('tool_schema',{source:'tools',index,value:tool}))break;yield;}
  else if(payload.tools!==undefined)reasons.add('unsupported_tools');
  const input=payload.input??payload.messages,source=payload.input===undefined?'messages':'input',diagnosticSource=payload.input==null?'messages':'input';
  if(payload.input!==undefined&&payload.messages!==undefined)reasons.add('ambiguous_input');
  if(typeof input==='string'){add('context',{source,value:input});yield;}
  else if(Array.isArray(input)){
    for(let index=0;index<input.length;index++){
      if(--nodes<0){cut();break;}
      const item=input[index];
      if(!item||typeof item!=='object'||Array.isArray(item)||!(itemTypes.has(item.type)||item.type==='reasoning'||['user','assistant','system','developer','tool'].includes(item.role))){reasons.add('unsupported_input');observeRecordingInputShape(onUnsupportedInputShape,item,diagnosticSource,'array_item');yield;continue;}
      if(!add(item.type==='additional_tools'?'tool_schema':['system','developer'].includes(item.role)?'instruction':'context',{source,index,value:item}))break;
      yield;
    }
  }else if(input!==undefined){reasons.add('unsupported_input');observeRecordingInputShape(onUnsupportedInputShape,input,diagnosticSource,'top_level');}
  // Relationships are protocol evidence only; no inference about ancestry.
  for(const key of ['previous_response_id','conversation','tool_choice','parallel_tool_calls','truncation'])if(payload[key]!==undefined){
    const value=payload[key],valid=value===null||key==='parallel_tool_calls'&&typeof value==='boolean'||key!=='parallel_tool_calls'&&typeof value==='string'||['conversation','tool_choice'].includes(key)&&typeof value==='object'&&!Array.isArray(value);
    if(!valid){reasons.add('unsupported_content');continue;}add('context',{source:key,value});yield;
  }
  // Internal accounting lets event adapters share one request-wide budget.
  // Persistence explicitly selects components/coverage and excludes this field.
  return {state:reasons.size?'incomplete':'complete',components,bytes,nodesConsumed:Math.min(maxNodes,Math.max(0,maxNodes-nodes)),reasons:[...reasons].sort(),opaqueReasoningExcluded,redaction:recordRedactionSummary(redactions)};
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
