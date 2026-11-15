import {captureRequestContext} from './recording-request-context.mjs';
import {isFilteredRecord} from './record-redaction.mjs';

// These are protocol records, not proof that a proposed/approved operation ran.
// Reuse the context filter so schemas, references and attachments have one policy.
export const visibleToolProtocols={
  mcp_list_tools:{kind:'tool_result',name:'mcp_list_tools'},
  mcp_approval_request:{kind:'tool_call'},
  mcp_approval_response:{kind:'tool_result',client:true},
  web_search_call:{kind:'tool_call',name:'web_search'},
  file_search_call:{kind:'tool_call',name:'file_search'},
  computer_call:{kind:'tool_call',name:'computer',callId:true},
  computer_call_output:{kind:'tool_result',name:'computer',callId:true,client:true},
};

const associatedTypes=new Set(['function_call','function_call_output','custom_tool_call','custom_tool_call_output','shell_call','shell_call_output','apply_patch_call','apply_patch_call_output']);
export function filterProtocolItem(capture,item,{maxBytes=capture.maxBytes-capture.bytes}={}){
  if(maxBytes<256||capture.remainingNodes<1){capture.truncated=true;return null;}
  const view=captureRequestContext({input:[item]},{maxBytes,maxNodes:capture.remainingNodes,maxComponents:1});
  if(!Number.isSafeInteger(view.nodesConsumed)||view.nodesConsumed<0||view.nodesConsumed>capture.remainingNodes){capture.failed=true;return null;}
  capture.remainingNodes-=view.nodesConsumed;
  for(const category of view.redaction.categories)capture.onRedact(category);
  for(const reason of view.reasons)if(reason!=='size_limit')capture.incompleteReasons.add('unsupported_content');
  return {value:view.components[0]?.content.value,clipped:view.reasons.includes('size_limit')};
}
export function captureToolAssociations(capture,item,type=item.type){
  if(!associatedTypes.has(type)){if(['caller','environment','async'].some(field=>Object.hasOwn(item,field)))capture.incompleteReasons.add('unsupported_content');return {};}
  const fields=['caller',...(['function_call','custom_tool_call'].includes(type)?['async']:[]),...(type==='shell_call'?['environment']:[])],source={type};
  if(['caller','environment','async'].some(field=>Object.hasOwn(item,field)&&!fields.includes(field)))capture.incompleteReasons.add('unsupported_content');
  if(isFilteredRecord(item))return Object.fromEntries(fields.filter(field=>Object.hasOwn(item,field)).map(field=>[field,item[field]]));
  let selected=false;for(const field of fields)if(Object.hasOwn(item,field)){source[field]=item[field];selected=true;}if(!selected)return {};
  const view=filterProtocolItem(capture,source,{maxBytes:8192});if(!view)return {};
  if(view.clipped)capture.truncated=true;
  const value=view.value??{};
  return Object.fromEntries(fields.filter(field=>Object.hasOwn(value,field)).map(field=>[field,value[field]]));
}

function metadata(capture,item,index,protocol){
  const value=capture.toolMetadata(item,item.id??String(index),item.type);
  if(protocol.name)value.name=protocol.name;
  for(const field of ['server_label','approval_request_id'])if(Object.hasOwn(item,field))value[field]=item[field];
  value.record_type=item.type;
  return value;
}

export function captureVisibleItem(capture,item,index,{partial=false,provenance='upstream',inputKey}={}){
  if(!Object.hasOwn(visibleToolProtocols,item?.type))return false;
  const protocol=visibleToolProtocols[item.type],key=inputKey??capture.identity(index,item.id),field=key+':visible';
  const view=filterProtocolItem(capture,item);if(!view)return true;const {value}=view;
  if(!value||typeof value!=='object'||Array.isArray(value)){capture.incompleteReasons.add('unsupported_content');if(view.clipped)capture.truncated=true;return true;}
  const details=metadata(capture,value,index,protocol);
  if(!partial){
    if(protocol.callId&&(typeof value.call_id!=='string'||!value.call_id.length))capture.incompleteReasons.add('malformed_tool_item');
    if(!protocol.client&&(typeof value.id!=='string'||!value.id.length))capture.incompleteReasons.add('malformed_tool_item');
    if(item.type==='mcp_approval_response'&&(typeof value.approval_request_id!=='string'||typeof value.approve!=='boolean'))capture.incompleteReasons.add('malformed_tool_item');
  }
  if(protocol.kind==='tool_result'){
    if(partial&&item.type==='mcp_list_tools'&&!Object.hasOwn(value,'tools')&&!Object.hasOwn(value,'error')){
      capture.field(field,protocol.kind,details);
    }else{capture.result(field,details,value,provenance);if(partial&&capture.fields.has(field))capture.fields.get(field).closed=false;}
  }else{
    // Store one stable field across added/done/final snapshots. The envelope
    // retains queries, safety checks and approval arguments without guessing.
    if(partial)capture.delta(field,protocol.kind,details,JSON.stringify(value));else capture.complete(field,protocol.kind,details,JSON.stringify(value));
  }
  if(view.clipped)capture.truncated=true;
  return true;
}

export function captureVisibleEvent(capture,event,key,id,type){
  const status=/^response\.(web_search_call|file_search_call|mcp_list_tools)\.(in_progress|searching|completed|failed)$/.exec(type);
  if(!status||status[1]==='mcp_list_tools'&&status[2]==='searching'||status[1]!=='mcp_list_tools'&&status[2]==='failed')return false;
  const protocol=visibleToolProtocols[status[1]],field=key+':visible',details={item:id,toolType:status[1],record_type:status[1],name:protocol.name,status:status[2]},state=capture.field(field,protocol.kind,details);
  if(state){state.metadata={...state.metadata,...details};if(protocol.kind==='tool_result')capture.send('tool_result',{...state.metadata,field,mode:'append',output:''});else capture.sendText(protocol.kind,{...state.metadata,field,mode:'append'},'');}
  return true;
}
