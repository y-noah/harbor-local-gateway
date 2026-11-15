import {sanitizeRecord} from './record-redaction.mjs';

// Hosted tools associate calls and results by their real item ID. The protocol
// does not define call_id for these types; never synthesize one for recording.
export const hostedToolProtocols={
  code_interpreter_call:{field:'code',output:'outputs',name:'code_interpreter',raw:true,nullable:true,metadata:['container_id']},
  mcp_call:{field:'arguments',output:'output',raw:false,metadata:['server_label','approval_request_id']},
};
const has=(value,key)=>Object.hasOwn(value,key);
const terminal=new Set(['completed','failed','incomplete']);
const statusEvents={
  code_interpreter_call:new Set(['in_progress','interpreting','completed']),
  mcp_call:new Set(['in_progress','completed','failed']),
};

function metadata(capture,item,id,protocol){
  const value=capture.toolMetadata(item,id,item.type);
  if(protocol.name)value.name=protocol.name;
  for(const field of protocol.metadata)if(has(item,field)){
    if(typeof item[field]==='string'||item[field]===null)value[field]=item[field];else capture.incompleteReasons.add('malformed_tool_item');
  }
  if(protocol.nullable&&has(item,protocol.field))value.code_state=item[protocol.field]===null?'unavailable':'received';
  if(has(item,protocol.output))value.output_snapshot_available=item[protocol.output]!==null;
  return sanitizeRecord(value,{maxBytes:8192,onRedact:capture.onRedact,onTruncate:()=>{capture.truncated=true;}});
}

function retainOutput(capture,state,details,partial){
  const updates=Object.fromEntries(Object.entries(details).filter(([name,value])=>name==='error'||value!==null||state.metadata[name]==null)),changed=Object.entries(updates).some(([name,value])=>state.metadata[name]!==value);
  state.metadata={...state.metadata,...updates};state.closed=!partial;state.hash=null;
  if(changed)capture.sendText('tool_result',{...state.metadata,field:state.key,mode:'append'},'');
}

export function captureHostedItem(capture,item,index,{partial=false}={}){
  if(!has(hostedToolProtocols,item?.type))return false;
  const protocol=hostedToolProtocols[item.type],key=capture.identity(index,item.id),id=item.id||String(index),details={...metadata(capture,item,id,protocol),item_pending:partial},field=key+':'+protocol.field;
  if(typeof item.id!=='string'||!item.id.length)capture.incompleteReasons.add('malformed_tool_item');
  if(!partial&&typeof item[item.type==='code_interpreter_call'?'container_id':'server_label']!=='string')capture.incompleteReasons.add('malformed_tool_item');
  if(capture.itemMetadata.has(key)||capture.itemMetadata.size<capture.maxFields)capture.itemMetadata.set(key,details);else capture.truncated=true;
  capture.observe(field,'tool_call',details);
  const value=item[protocol.field];
  if(partial){
    const state=capture.field(field,'tool_call',details);
    if(state)capture.sendText('tool_call',{...details,field,mode:'append'},'');
    if(typeof value==='string'&&value.length)capture.delta(field,'tool_call',details,value);
    else if(value!==undefined&&!(protocol.nullable&&value===null)&&typeof value!=='string')capture.incompleteReasons.add('malformed_tool_item');
  }else if(protocol.nullable&&value===null&&capture.fields.get(field)?.metadata.code_state==='received'){
    // A nullable final snapshot cannot erase code already observed in deltas.
    // Keep an unfinished stream open so finish still discloses its coverage gap.
    const state=capture.fields.get(field);state.metadata={...state.metadata,...details,code_state:'received'};
    capture.sendText('tool_call',{...state.metadata,field,mode:'append'},'');
  }else if(typeof value==='string'||protocol.nullable&&value===null)capture.complete(field,'tool_call',details,value??'');
  else capture.incompleteReasons.add('malformed_tool_item');
  const outputDetails=Object.fromEntries(Object.entries(details).filter(([name])=>name!=='code_state'));
  if(has(item,protocol.output)&&(!partial||item[protocol.output]!==null||terminal.has(item.status))){
    const output=item[protocol.output];
    const existing=capture.fields.get(key+':output');
    if(output===null&&existing?.hasVisibleOutput)retainOutput(capture,existing,outputDetails,partial);
    else if(item.type==='code_interpreter_call'&&output!==null){
      if(!Array.isArray(output)){capture.incompleteReasons.add('malformed_tool_item');return true;}
      const supported=[];
      for(const part of output){
        if(capture.remainingNodes--<=0){capture.truncated=true;break;}
        if(part?.type==='logs'&&typeof part.logs==='string'){
          supported.push({type:'logs',logs:part.logs});
          for(const name in part)if(has(part,name)&&!['type','logs'].includes(name)){capture.incompleteReasons.add('unsupported_content');break;}
        }
        else if(part?.type==='image'&&typeof part.url==='string')supported.push({type:'image'});
        else capture.incompleteReasons.add('unsupported_content');
      }
      capture.result(key+':output',outputDetails,supported);
    }else if(item.type==='mcp_call'&&output!==null&&typeof output!=='string')capture.incompleteReasons.add('malformed_tool_item');
    else capture.result(key+':output',outputDetails,output);
    if(output!==null&&capture.fields.has(key+':output'))capture.fields.get(key+':output').hasVisibleOutput=true;
    if(partial&&capture.fields.has(key+':output'))capture.fields.get(key+':output').closed=false;
  }
  if(!partial&&!has(item,protocol.output)){
    const output=capture.fields.get(key+':output');
    if(output)retainOutput(capture,output,outputDetails,false);
  }
  return true;
}

export function captureHostedEvent(capture,event,key,id,type){
  const events={
    'response.code_interpreter_call_code.delta':{tool:'code_interpreter_call',delta:true},
    'response.code_interpreter_call_code.done':{tool:'code_interpreter_call'},
    'response.mcp_call_arguments.delta':{tool:'mcp_call',delta:true},
    'response.mcp_call_arguments.done':{tool:'mcp_call'},
  };
  const descriptor=events[type];
  if(descriptor){
    if(typeof event.item_id!=='string'||!event.item_id.length)capture.incompleteReasons.add('malformed_tool_item');
    const known=capture.itemMetadata.get(key)??{},protocol=hostedToolProtocols[descriptor.tool],details={...known,item:id,toolType:descriptor.tool,item_pending:known.item_pending??true,...(protocol.name?{name:protocol.name}:{}),...(protocol.nullable?{code_state:'received'}:{})},field=key+':'+protocol.field;
    if(protocol.nullable&&typeof (descriptor.delta?event.delta:event[protocol.field])==='string'){
      const state=capture.field(field,'tool_call',details);if(state)state.metadata={...state.metadata,code_state:'received'};
    }
    if(descriptor.delta)capture.delta(field,'tool_call',details,event.delta);else capture.complete(field,'tool_call',details,event[protocol.field]);
    return true;
  }
  const status=/^response\.(code_interpreter_call|mcp_call)\.(in_progress|interpreting|calling|completed|failed|incomplete)$/.exec(type);
  if(!status||!statusEvents[status[1]].has(status[2]))return false;
  if(typeof event.item_id!=='string'||!event.item_id.length)capture.incompleteReasons.add('malformed_tool_item');
  const known=capture.itemMetadata.get(key)??{},protocol=hostedToolProtocols[status[1]],field=key+':'+protocol.field,details={...known,item:id,toolType:status[1],item_pending:known.item_pending??true,...(protocol.name?{name:protocol.name}:{}),status:status[2]},state=capture.field(field,'tool_call',details);
  if(capture.itemMetadata.has(key)||capture.itemMetadata.size<capture.maxFields)capture.itemMetadata.set(key,sanitizeRecord(details,{maxBytes:8192,onRedact:capture.onRedact,onTruncate:()=>{capture.truncated=true;}}));else capture.truncated=true;
  if(state){state.metadata={...state.metadata,status:status[2]};capture.sendText('tool_call',{...state.metadata,field,mode:'append'},'');}
  return true;
}
