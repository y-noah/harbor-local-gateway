// This is a protocol capability check, never a history reconstruction. Unknown
// upstream state remains blocked until its cross-account replay is validated.
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const textContent=content=>typeof content==='string'||Array.isArray(content)&&content.every(part=>object(part)&&['input_text','output_text','text'].includes(part.type)&&typeof part.text==='string');
function itemReplayable(item,{output=false}={}){
  if(!object(item))return false;
  if(item.encrypted_content!==undefined&&item.encrypted_content!==null)return false;
  if(item.type==='function_call')return typeof item.call_id==='string'&&typeof item.name==='string'&&typeof item.arguments==='string';
  if(item.type==='function_call_output')return typeof item.call_id==='string'&&textContent(item.output);
  return (item.type===undefined||item.type==='message')&&(['user','assistant','system','developer'].includes(item.role)||output&&item.type==='message'&&item.role===undefined)&&textContent(item.content);
}
export function replayableResponsesInput(payload){
  if(!object(payload)||payload.previous_response_id||payload.conversation||payload.prompt||payload.background===true||payload.store===true||payload.instructions!==undefined&&typeof payload.instructions!=='string')return false;
  if(payload.tools!==undefined&&(!Array.isArray(payload.tools)||payload.tools.some(tool=>!object(tool)||tool.type!=='function')))return false;
  return typeof payload.input==='string'||Array.isArray(payload.input)&&payload.input.every(item=>itemReplayable(item));
}
export function replayableResponsesEvent(event){
  if(!object(event))return false;
  if(event.item&&!itemReplayable(event.item,{output:true}))return false;
  if(event.response?.output!==undefined&&(!Array.isArray(event.response.output)||!event.response.output.every(item=>itemReplayable(item,{output:true}))))return false;
  // File, reasoning and hosted-tool events do not become replayable merely
  // because a later completed event omits their earlier opaque payload.
  if(typeof event.type!=='string')return false;
  return /^(response\.(created|in_progress|queued|completed|failed|incomplete|output_item\.(added|done)|content_part\.(added|done)|output_text\.(delta|done)|function_call_arguments\.(delta|done))|error)$/.test(event.type)&&(!event.part||['input_text','output_text','text'].includes(event.part.type));
}
