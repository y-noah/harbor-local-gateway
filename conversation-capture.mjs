import {createHash} from 'node:crypto';
import {RecordRedactor,sanitizeRecord,sanitizeToolRecord,boundedRecordText,redactRecordParts,filteredRecordField} from './record-redaction.mjs';
import {JsonRecordRedactor} from './record-json.mjs';

const digest=text=>createHash('sha256').update(text).digest('hex');
const textOf=content=>typeof content==='string'?[{type:'text',text:content}]:Array.isArray(content)?content:[];
const currentItem=item=>item?.role==='user'||item?.type==='function_call_output';

// The boundary is deliberately conservative when a client submits complete history.
// It never persists system instructions, tool schemas, reasoning or preceding turns.
export function currentInput(payload,{maxEvents=256,maxNodes=1024}={}){
  if(typeof payload.input==='string')return {state:'known',events:[{kind:'user',content:{text:payload.input}}]};
  const input=Array.isArray(payload.input)?payload.input:[];let start=input.length,nodes=0;
  while(start>0&&nodes<maxNodes&&input.length-start<maxEvents){nodes++;if(!currentItem(input[start-1]))break;start--;if(input[start].role==='user')break;}
  const events=[];let truncated=start>0&&input.length-start===maxEvents||nodes>=maxNodes;
  outer:for(let itemIndex=start;itemIndex<input.length;itemIndex++){const item=input[itemIndex];
    if(item.type==='function_call_output'){
      events.push({kind:'tool_result',content:{callId:item.call_id||null,output:item.output}});continue;
    }
    for(const part of textOf(item.content)){
      if(nodes>=maxNodes){truncated=true;break outer;}nodes++;
      if(events.length>=maxEvents){truncated=true;break;}
      if(['input_text','text'].includes(part?.type)&&typeof part.text==='string')events.push({kind:'user',group:String(itemIndex),content:{text:part.text}});
      else if(part&&['input_image','input_file','image_url'].includes(part.type)){
        // URLs and file IDs can themselves be bearer credentials. Only structural facts survive.
        events.push({kind:'attachment',content:{type:part.type,detail:['low','high','auto'].includes(part.detail)?part.detail:null}});
      }
    }
    if(events.length>=maxEvents&&itemIndex<input.length-1){truncated=true;break;}
  }
  return {state:start===0&&events.length?'known':'uncertain',events,truncated};
}

// emit receives only sanitized copies. Raw text is confined to bounded redactors;
// hashes identify final/delta equality without retaining another full response.
export class ConversationCapture{
  constructor({emit,maxFields=256,maxBytes=2*1024*1024,maxNodes=1024}={}){
    if(typeof emit!=='function'||!Number.isSafeInteger(maxFields)||maxFields<1||!Number.isSafeInteger(maxBytes)||maxBytes<256||!Number.isSafeInteger(maxNodes)||maxNodes<1)throw TypeError('Invalid recording capture configuration');
    this.emit=emit;this.maxFields=maxFields;this.maxBytes=maxBytes;this.remainingNodes=maxNodes;this.fields=new Map();this.identities=new Map();this.itemMetadata=new Map();this.bytes=0;this.rawBytes=0;this.sequence=0;this.inputSequence=0;this.truncated=false;this.closed=false;
  }
  send(kind,content,provenance='upstream'){
    if(this.closed||this.truncated)return;
    let clipped=false;const safe=sanitizeRecord(content,{maxBytes:Math.max(0,this.maxBytes-this.bytes),onTruncate:()=>{clipped=true;}}),bytes=Buffer.byteLength(JSON.stringify(safe));
    if(clipped){this.truncated=true;return;}
    if(this.bytes+bytes>this.maxBytes){this.truncated=true;return;}
    this.bytes+=bytes;try{if(this.emit({sourceKey:String(++this.sequence),kind,provenance,content:safe})===false)this.failed=true;}catch{this.failed=true;}if(clipped)this.truncated=true;
  }
  sendField(kind,metadata,field,text,provenance='upstream'){
    // A UTF-16 chunk of this size is at most 24 KiB when JSON-escaped,
    // leaving room for bounded metadata below the repository's event limit.
    if(!text.length){this.send(kind,filteredRecordField(metadata,field,text),provenance);return;}
    for(let offset=0;offset<text.length&&!this.truncated;){
      let end=Math.min(text.length,offset+4096);if(end<text.length&&/[\uD800-\uDBFF]/.test(text[end-1]))end--;
      this.send(kind,filteredRecordField({...metadata,...(offset?{mode:'append'}:{})},field,text.slice(offset,end)),provenance);offset=end;
    }
  }
  sendText(kind,metadata,text,provenance='upstream'){this.sendField(kind,metadata,'text',text,provenance);}
  input(payload,current=currentInput(payload)){
    let clipped=false;const users=current.events.filter(event=>event.kind==='user'),filtered=redactRecordParts(users.map(event=>event.content.text),{maxBytes:Math.max(0,this.maxBytes-this.rawBytes),onTruncate:()=>{clipped=true;}});this.rawBytes+=filtered.bytes;
    let userIndex=0;
    for(const [position,event] of current.events.entries()){
      if(this.truncated)break;
      if(event.kind==='user'){
        const text=filtered.parts[userIndex++]||'';if(text)this.sendText('user',{field:'input:'+(event.group??'0')+':'+position,position,mode:'append'},text,'client');
      }else if(event.kind==='tool_result'){
        let clipped=false;const output=sanitizeToolRecord(event.content.output,{maxBytes:Math.max(0,this.maxBytes-this.bytes),onTruncate:()=>{clipped=true;}});
        const encoded=JSON.stringify(output);
        if(Buffer.byteLength(encoded)>32*1024)this.sendField(event.kind,{callId:event.content.callId,field:'input-tool:'+(++this.inputSequence),mode:'append',output_format:typeof output==='string'?'string':'json'},'output',typeof output==='string'?output:encoded,'client');
        else this.send(event.kind,filteredRecordField({callId:event.content.callId},'output',output),'client');if(clipped)this.truncated=true;
      }else this.send(event.kind,{...event.content,position},'client');
    }
    if(current.truncated||clipped)this.truncated=true;
    return current.state;
  }
  identity(index,id){
    if(typeof id==='string'&&id.length>512){this.truncated=true;return 'invalid-item';}
    const indexKey=Number.isSafeInteger(index)&&index>=0?'index:'+index:null,idKey=typeof id==='string'?'id:'+id:null;
    const key=(idKey&&this.identities.get(idKey))||(indexKey&&this.identities.get(indexKey))||idKey||indexKey||'index:0';
    if(this.identities.size<this.maxFields*2){if(idKey)this.identities.set(idKey,key);if(indexKey)this.identities.set(indexKey,key);}else this.truncated=true;
    return key;
  }
  field(key,kind,metadata){
    if(key.length>2048){this.truncated=true;return null;}
    let state=this.fields.get(key);
    if(!state&&!this.truncated){
      if(this.fields.size>=this.maxFields){this.truncated=true;return null;}
      const safe=sanitizeRecord(metadata,{maxBytes:8192,onTruncate:()=>{this.truncated=true;}});if(this.truncated)return null;
      state={key,kind,metadata:safe,filter:kind==='tool_call'?new JsonRecordRedactor({onTruncate:()=>{this.truncated=true;}}):new RecordRedactor(),hash:createHash('sha256'),closed:false};this.fields.set(key,state);
    }
    return state;
  }
  delta(key,kind,metadata,text){
    if(typeof text!=='string'||this.closed||this.truncated)return;
    const field=this.field(key,kind,metadata);if(!field||field.closed)return;
    const prefix=boundedRecordText(text,Math.max(0,this.maxBytes-this.rawBytes));this.rawBytes+=Buffer.byteLength(prefix);
    field.hash.update(prefix);const safe=field.filter.push(prefix);if(safe)this.sendText(kind,{...field.metadata,field:key,mode:'append'},safe);
    if(prefix.length!==text.length){const tail=field.filter.finish();if(tail)this.sendText(kind,{...field.metadata,field:key,mode:'append'},tail);field.closed=true;field.hash=null;this.truncated=true;}
  }
  complete(key,kind,metadata,text){
    if(typeof text!=='string'||this.closed||this.truncated)return;
    if(boundedRecordText(text,this.maxBytes).length!==text.length){this.delta(key,kind,metadata,text);return;}
    const field=this.field(key,kind,metadata);if(!field||field.closed)return;
    const same=field.hash.copy().digest('hex')===digest(text);
    const safeMetadata=sanitizeRecord(metadata,{maxBytes:8192,onTruncate:()=>{this.truncated=true;}});if(this.truncated)return;
    const changed=Object.entries(safeMetadata).some(([name,value])=>field.metadata[name]!==value);field.metadata={...field.metadata,...safeMetadata};
    const tail=field.filter.finish();
    if(same){if(tail||changed)this.sendText(kind,{...field.metadata,field:key,mode:'append'},tail);}
    else{
      let clipped=false;const options={maxBytes:Math.max(0,this.maxBytes-this.bytes),onTruncate:()=>{clipped=true;}};
      const safe=kind==='tool_call'?sanitizeToolRecord(text,options):sanitizeRecord(text,options);this.sendText(kind,{...field.metadata,field:key,mode:'replace'},safe);if(clipped)this.truncated=true;
    }
    field.closed=true;field.hash=null;
  }
  item(item,index){
    if(this.closed||this.truncated)return;if(this.remainingNodes--<=0){this.truncated=true;return;}
    if(!item||item.type==='reasoning')return;
    const id=item.id||String(index),key=this.identity(index,item.id);
    if(item.type==='function_call')this.complete(key+':arguments','tool_call',{item:id,callId:item.call_id||null,name:item.name||null},item.arguments);
    const content=Array.isArray(item.content)?item.content:[];
    if(item.type==='message')for(let i=0;i<content.length&&!this.truncated;i++){
      if(this.remainingNodes<=0){this.truncated=true;break;}this.remainingNodes--;const part=content[i];
      if(!part||typeof part!=='object')continue;
      if(part.type==='output_text')this.complete(key+':'+i+':text','assistant',{item:id},part.text);
      if(part.type==='refusal')this.complete(key+':'+i+':refusal','assistant',{item:id,refusal:true},part.refusal);
    }
  }
  event(event){
    if(this.closed||this.truncated)return;
    const key=this.identity(event.output_index,event.item_id||event.item?.id),id=event.item_id||event.item?.id||String(event.output_index??0),index=event.content_index??0;
    if(event.type==='response.output_item.added'&&event.item?.type==='function_call'){
      if(this.itemMetadata.size<this.maxFields)this.itemMetadata.set(key,sanitizeRecord({item:id,callId:event.item.call_id||null,name:event.item.name||null}));else this.truncated=true;
      this.field(key+':arguments','tool_call',this.itemMetadata.get(key)||{item:id});this.send('tool_call',{...this.itemMetadata.get(key),field:key+':arguments',mode:'append',text:''});return;
    }
    if(event.type==='response.output_text.delta')this.delta(key+':'+index+':text','assistant',{item:id},event.delta);
    else if(event.type==='response.refusal.delta')this.delta(key+':'+index+':refusal','assistant',{item:id,refusal:true},event.delta);
    else if(event.type==='response.function_call_arguments.delta')this.delta(key+':arguments','tool_call',this.itemMetadata.get(key)||{item:id},event.delta);
    else if(event.type==='response.output_item.done')this.item(event.item,event.output_index);
  }
  finish(response){
    if(this.closed)return;
    const output=Array.isArray(response?.output)?response.output:[];
    for(let i=0;i<output.length&&!this.truncated;i++){if(this.remainingNodes<=0){this.truncated=true;break;}this.item(output[i],i);}
    for(const field of this.fields.values())if(!field.closed){
      const text=field.filter.finish();if(text)this.sendText(field.kind,{...field.metadata,field:field.key,mode:'append'},text);
    }
    this.closed=true;this.fields.clear();this.identities.clear();this.itemMetadata.clear();return {truncated:this.truncated,failed:!!this.failed,bytes:this.bytes};
  }
}
