import {redactRecordParts,sanitizeRecord} from './record-redaction.mjs';
import {filterProtocolItem} from './recording-visible-tools.mjs';

const channels=['stdout','stderr'];
const index=value=>Number.isSafeInteger(value)&&value>=0;

function remember(capture,key,metadata){
  const previous=capture.itemMetadata.get(key)??{},updates=Object.fromEntries(Object.entries(metadata).filter(([name,value])=>name==='error'||value!==null||previous[name]==null));
  const safe=sanitizeRecord({...previous,...updates},{maxBytes:8192,onRedact:capture.onRedact,onTruncate:()=>{capture.truncated=true;}});
  if(capture.itemMetadata.has(key)||capture.itemMetadata.size<capture.maxFields)capture.itemMetadata.set(key,safe);else capture.truncated=true;
  // IDs may arrive after a channel delta. Enrich the actual existing fields;
  // never associate another item merely because its text happens to match.
  for(const field of capture.fields.values())if(field.key.startsWith(key+':')&&(field.kind==='tool_call'||field.kind==='tool_result')){
    const updates=Object.fromEntries(Object.entries(safe).filter(([name,value])=>name==='error'||value!==null||field.metadata[name]==null));
    if(Object.entries(updates).some(([name,value])=>field.metadata[name]!==value)){
      field.metadata={...field.metadata,...updates};capture.sendText(field.kind,{...field.metadata,field:field.key,mode:'append'},'');capture.observe(field.key,field.kind,field.metadata);
    }
  }
  return safe;
}

function outputCopy(capture,item){
  if(!Array.isArray(item.output))return {item,clipped:false};
  let clipped=false;const safe={};
  const limit=Math.min(item.output.length,Math.max(1,capture.remainingNodes));if(limit<item.output.length)clipped=true;
  for(const channel of channels){const parts=[];for(let position=0;position<limit;position++)parts.push(typeof item.output[position]?.[channel]==='string'?item.output[position][channel]:'');safe[channel]=redactRecordParts(parts,{maxBytes:capture.maxBytes-capture.bytes,maxParts:limit||1,onRedact:capture.onRedact,onTruncate:()=>{clipped=true;}}).parts;}
  const output=[];
  for(let position=0;position<safe.stdout.length;position++){
    const part=item.output[position];
    output.push(part&&typeof part==='object'&&!Array.isArray(part)?{...part,...(typeof part.stdout==='string'?{stdout:safe.stdout[position]}:{}),...(typeof part.stderr==='string'?{stderr:safe.stderr[position]}:{})}:part);
  }
  return {item:{...item,output},clipped};
}

export function captureShellItem(capture,item,position,{partial=false,provenance='upstream',inputKey,commandIndex}={}){
  if(!['shell_call','shell_call_output'].includes(item?.type))return false;
  const key=inputKey??capture.identity(position,item.id),prepared=item.type==='shell_call_output'?outputCopy(capture,item):{item,clipped:false},view=filterProtocolItem(capture,prepared.item);
  if(!view)return true;const value=view.value;
  if(!value||typeof value!=='object'){capture.incompleteReasons.add('unsupported_content');if(view.clipped||prepared.clipped)capture.truncated=true;return true;}
  const details=remember(capture,key,capture.toolMetadata(value,value.id??String(position),'shell_call'));
  if(item.type==='shell_call'){
    const action=value.action;
    if(action!==undefined&&(!action||typeof action!=='object'||Array.isArray(action))){if(view.clipped)capture.truncated=true;else capture.incompleteReasons.add('malformed_tool_item');return true;}
    const configuration=Object.fromEntries(Object.entries(action??{}).filter(([name])=>name!=='commands')),callField=key+':action',callMetadata={...details,representation:'call_metadata'};
    if(partial){capture.field(callField,'tool_call',callMetadata);capture.delta(callField,'tool_call',callMetadata,JSON.stringify(configuration));}
    else capture.complete(callField,'tool_call',callMetadata,JSON.stringify(configuration));
    if(Array.isArray(action?.commands))for(let command=0;command<action.commands.length&&!capture.truncated;command++){
      const field=key+':command:'+command,metadata={...details,command_index:command,representation:'command'};
      if(partial){capture.field(field,'tool_call',metadata);capture.sendText('tool_call',{...metadata,field,mode:'append'},'');if(action.commands[command].length)capture.delta(field,'tool_call',metadata,action.commands[command]);}
      else capture.complete(field,'tool_call',metadata,action.commands[command]);
    }else if(!partial)capture.incompleteReasons.add('malformed_tool_item');
  }else if(Array.isArray(value.output)){
    const emptyField=index(commandIndex)?key+':command-output:'+commandIndex+':snapshot:empty':key+':output-snapshot:empty';
    if(!value.output.length)capture.result(emptyField,{...details,representation:index(commandIndex)?'command_output_snapshot':'empty_output_snapshot',association_state:index(commandIndex)?'provided':'unavailable',...(index(commandIndex)?{command_index:commandIndex}:{})},[],provenance);
    for(let outputPosition=0;outputPosition<value.output.length&&!capture.truncated;outputPosition++){
      const explicit=index(commandIndex),field=explicit?key+':command-output:'+commandIndex+':snapshot:'+outputPosition:key+':output-snapshot:'+outputPosition,metadata={...details,representation:explicit?'command_output_snapshot':'item_output_snapshot',output_position:outputPosition,association_state:explicit?'provided':'unavailable',...(explicit?{command_index:commandIndex}:{})};
      capture.result(field,metadata,value.output[outputPosition],provenance);
      if(partial&&capture.fields.has(field))capture.fields.get(field).closed=false;
    }
    if(partial&&!value.output.length&&capture.fields.has(emptyField))capture.fields.get(emptyField).closed=false;
    if(!partial)for(const field of capture.fields.values())if(field.key.startsWith(key+':command-output:')&&(!index(commandIndex)||field.metadata.command_index===commandIndex)&&field.metadata.representation==='stream_channel'&&!field.closed){
      const tail=field.filter.finish();if(tail)capture.sendText(field.kind,{...field.metadata,field:field.key,mode:'append'},tail);
      field.closed=true;field.hash=null;
    }
    if(!partial&&!index(commandIndex))for(const field of capture.fields.values())if(field.key.startsWith(key+':output-snapshot:')&&!field.closed){field.closed=true;field.hash=null;}
  }else if(!partial)capture.incompleteReasons.add('malformed_tool_item');
  if(view.clipped||prepared.clipped)capture.truncated=true;
  return true;
}

export function captureShellEvent(capture,event,key,id,type){
  const command=/^response\.shell_call_command\.(added|delta|done)$/.exec(type),output=/^response\.shell_call_output_content\.(delta|done)$/.exec(type);
  if(!command&&!output)return false;
  if(!index(event.command_index)){capture.incompleteReasons.add('malformed_tool_item');return true;}
  const commandIndex=event.command_index,known=capture.itemMetadata.get(key)??{},details={...known,item:typeof event.item_id==='string'?event.item_id:Object.hasOwn(known,'item')?known.item:id,toolType:'shell_call',name:'shell',command_index:commandIndex};
  if(command){
    const field=key+':command:'+commandIndex,metadata={...details,representation:'command'};
    if(command[1]==='done')capture.complete(field,'tool_call',metadata,event.command);
    else if(typeof (command[1]==='delta'?event.delta:event.command)==='string'){
      if(command[1]==='added'&&capture.fields.has(field))return true;
      capture.delta(field,'tool_call',metadata,command[1]==='delta'?event.delta:event.command);
    }else capture.incompleteReasons.add('malformed_tool_item');
  }else if(output[1]==='delta'){
    if(!event.delta||typeof event.delta!=='object'||Array.isArray(event.delta)){capture.incompleteReasons.add('malformed_tool_item');return true;}
    for(const channel of channels)if(Object.hasOwn(event.delta,channel))capture.delta(key+':command-output:'+commandIndex+':'+channel,'tool_result',{...details,representation:'stream_channel',channel,association_state:'provided'},event.delta[channel]);
    for(const name in event.delta)if(Object.hasOwn(event.delta,name)&&!channels.includes(name)){capture.incompleteReasons.add('unsupported_content');break;}
  }else{
    captureShellItem(capture,{type:'shell_call_output',...(typeof event.item_id==='string'?{id:event.item_id}:{}),...(typeof details.callId==='string'?{call_id:details.callId}:{}),output:event.output},event.output_index,{commandIndex});
    if(Array.isArray(event.output))for(const channel of channels){
      const field=key+':command-output:'+commandIndex+':'+channel;
      if(capture.fields.has(field)&&event.output.length&&event.output.every(part=>typeof part?.[channel]==='string')){
        const text=event.output.map(part=>part[channel]).join('');
        if(text.length)capture.complete(field,'tool_result',{...details,representation:'stream_channel',channel,association_state:'provided'},text);
      }
    }
  }
  return true;
}
