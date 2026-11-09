import {createHash} from 'node:crypto';
import {RecordRedactor,recordRedactionSummary,sanitizeRecord,sanitizeToolRecord,boundedRecordText,redactRecordParts,filteredRecordField} from './record-redaction.mjs';
import {JsonRecordRedactor} from './record-json.mjs';

const digest=text=>createHash('sha256').update(text).digest('hex');
const textOf=content=>typeof content==='string'?[{type:'text',text:content}]:Array.isArray(content)?content:[];
const resultTypes=new Set(['function_call_output','custom_tool_call_output']);
const mediaTypes=new Set(['image','image_url','input_image','output_image','input_file','file','audio','input_audio','output_audio']);
const unknownToolResult=item=>item?.role==='tool'||typeof item?.type==='string'&&/_call_output$/.test(item.type.slice(0,256));
const currentItem=item=>item?.role==='user'||resultTypes.has(item?.type)||unknownToolResult(item);

// The boundary is deliberately conservative when a client submits complete history.
// It never persists system instructions, tool schemas, reasoning or preceding turns.
export function currentInput(payload,{maxEvents=256,maxNodes=1024}={}){
  if(typeof payload.input==='string')return {state:'known',events:[{kind:'user',content:{text:payload.input}}]};
  const input=Array.isArray(payload.input)?payload.input:[];let start=input.length,nodes=0;
  while(start>0&&nodes<maxNodes&&input.length-start<maxEvents){nodes++;if(!currentItem(input[start-1]))break;start--;if(input[start].role==='user')break;}
  const events=[];let unsupportedItems=0,unsupportedContentParts=0,truncated=start>0&&input.length-start===maxEvents||nodes>=maxNodes;
  outer:for(let itemIndex=start;itemIndex<input.length;itemIndex++){const item=input[itemIndex];
    if(resultTypes.has(item?.type)){
      events.push({kind:'tool_result',content:{callId:item.call_id||null,output:item.output,...(item.type==='custom_tool_call_output'?{toolType:'custom_tool_call'}:{}),...(typeof item.name==='string'?{name:item.name}:{}),...(typeof item.namespace==='string'?{namespace:item.namespace}:{})}});continue;
    }
    if(unknownToolResult(item)){unsupportedItems++;continue;}
    for(const part of textOf(item.content)){
      if(nodes>=maxNodes){truncated=true;break outer;}nodes++;
      if(events.length>=maxEvents){truncated=true;break;}
      if(['input_text','text'].includes(part?.type)&&typeof part.text==='string')events.push({kind:'user',group:String(itemIndex),content:{text:part.text}});
      else if(part&&['input_image','input_file','image_url'].includes(part.type)){
        // URLs and file IDs can themselves be bearer credentials. Only structural facts survive.
        events.push({kind:'attachment',content:{type:part.type,detail:['low','high','auto'].includes(part.detail)?part.detail:null}});
      }else unsupportedContentParts++;
    }
    if(events.length>=maxEvents&&itemIndex<input.length-1){truncated=true;break;}
  }
  return {state:start===0&&events.length?'known':'uncertain',events,truncated,unsupportedItems,unsupportedContentParts};
}

// Custom results can contain Responses input_text parts as well as MCP text
// parts. Filter adjacent text before any defensive per-value pass, otherwise a
// credential split across parts can lose its prefix during that first pass.
function redactToolJson(raw,{maxBytes,maxNodes,onTruncate,onNodes,attachmentMetadata,onRedact}){
  // The caller already validated the JSON. Locate only typed attachment objects
  // and adjacent text values: other source spans pass unchanged to the existing
  // JSON text redactor, including whitespace and numeric literals.
  let cursor=0,nodes=maxNodes;const edits=[],limit=Symbol('tool-json-budget');
  const whitespace=()=>{while(/\s/.test(raw[cursor]??'')&&cursor<raw.length)cursor++;};
  const string=()=>{const start=cursor++;while(cursor<raw.length){const char=raw[cursor++];if(char==='\\')cursor++;else if(char==='"')break;}return {start,end:cursor};};
  const value=depth=>{
    if(onNodes?.(1)===false||--nodes<0||depth>24)throw limit;whitespace();const start=cursor,char=raw[cursor];
    if(char==='"'){const span=string();return {...span,string:true};}
    if(char==='['){
      cursor++;whitespace();const parts=[];
      while(raw[cursor]!==']'){const child=value(depth+1);if(child?.textParts)parts.push(...child.textParts);whitespace();if(raw[cursor]===','){cursor++;continue;}break;}cursor++;
      const texts=parts.map(part=>JSON.parse(raw.slice(part.start,part.end))),safe=redactRecordParts(texts,{maxBytes,maxParts:maxNodes,onTruncate,onRedact}).parts;
      for(let i=0;i<safe.length;i++)if(safe[i]!==texts[i])edits.push({...parts[i],replacement:JSON.stringify(safe[i])});return {start,end:cursor};
    }
    if(char==='{'){
      cursor++;whitespace();let type=null,binary=false;const textParts=[],facts={};
      while(raw[cursor]!=='}'){
        if(onNodes?.(1)===false)throw limit;
        whitespace();const keySpan=string(),key=JSON.parse(raw.slice(keySpan.start,keySpan.end));whitespace();cursor++;
        const child=value(depth+1);
        if(key==='type'&&child.string){const name=JSON.parse(raw.slice(child.start,child.end));if(mediaTypes.has(name)){type=name;binary=true;}else if(!binary)type=name;}
        if(key==='text'&&child.string)textParts.push(child);
        if(['size','bytes','position','detail'].includes(key)&&child.end-child.start<128){const fact=JSON.parse(raw.slice(child.start,child.end));if(key==='detail'&&['low','high','auto'].includes(fact))facts.detail=fact;else if(Number.isSafeInteger(fact)&&fact>=0)facts[key==='size'?'bytes':key]=fact;}
        whitespace();if(raw[cursor]===','){cursor++;continue;}break;
      }cursor++;
      if(binary){onRedact?.('attachment_data');edits.push({start,end:cursor,replacement:JSON.stringify(attachmentMetadata?attachmentMetadata(JSON.parse(raw.slice(start,cursor))):{type,...facts,content:'[附件数据已省略]'})});}
      return {start,end:cursor,...(!binary&&['text','input_text','output_text'].includes(type)?{textParts}:{})};
    }
    while(cursor<raw.length&&!/[\s,\]}]/.test(raw[cursor]))cursor++;return {start,end:cursor};
  };
  try{value(0);}catch(error){if(error!==limit)throw error;onTruncate();return '[工具结构化内容超限，已省略]';}
  let position=0;const parts=[];
  for(const edit of edits.sort((a,b)=>a.start-b.start||b.end-a.end)){if(edit.start<position)continue;parts.push(raw.slice(position,edit.start),edit.replacement);position=edit.end;}parts.push(raw.slice(position));
  const filter=new JsonRecordRedactor({onTruncate,onRedact});return filter.push(parts.join(''))+filter.finish();
}
export function safeToolOutput(value,{maxBytes,maxNodes=1024,contentParts=false,onTruncate,onUnsupported=()=>{},onNodes,attachmentMetadata,onRedact}){
  if(typeof value==='string'){
    const prefix=boundedRecordText(value,maxBytes);
    if(/^[\[{]/.test(prefix.trimStart())){
      if(prefix.length!==value.length){onTruncate();return '[工具结构化内容超限，已省略]';}
      let valid=false;try{JSON.parse(value);valid=true;}catch{}
      if(valid)return redactToolJson(value,{maxBytes,maxNodes,onTruncate,onNodes,attachmentMetadata,onRedact});
    }
    if(onNodes?.(1)===false){onTruncate();return '[已截断]';}
    return sanitizeRecord(value,{maxBytes,onTruncate,onRedact});
  }
  let nodes=maxNodes,remaining=maxBytes;
  const cut=()=>{onTruncate();return '[已截断]';},text=raw=>{const prefix=boundedRecordText(raw,remaining);remaining-=Buffer.byteLength(prefix);if(prefix.length!==raw.length)onTruncate();return prefix;};
  const visit=(item,depth=0,fragment=false)=>{
    if(onNodes?.(1)===false||--nodes<0||depth>24||remaining<=0)return cut();
    if(typeof item==='string')return text(item);
    if(item instanceof ArrayBuffer||ArrayBuffer.isView(item)){onRedact?.('attachment_data');return {type:'binary',bytes:item.byteLength,content:'[附件数据已省略]'};}
    if(Array.isArray(item)){
      const result=[],parts=[],positions=[];
      for(const child of item){if(nodes<=0||remaining<=0){result.push(cut());break;}
        if(fragment&&(!child||!['text','input_text','output_text',...mediaTypes].includes(child.type)||['text','input_text','output_text'].includes(child.type)&&typeof child.text!=='string')){if(onNodes?.(1)===false){result.push(cut());break;}onUnsupported();result.push({content:'[未支持的工具内容片段已省略]'});nodes--;continue;}
        if(['text','input_text','output_text'].includes(child?.type)&&typeof child.text==='string'){
          if(onNodes?.(3)===false){result.push(cut());break;}
          nodes-=3;positions.push([result.length,child.type]);parts.push(text(child.text));result.push(null);
          for(const key in child)if(Object.hasOwn(child,key)&&!['type','text'].includes(key)){onUnsupported();break;}
        }
        else result.push(visit(child,depth+1));
      }
      const safe=redactRecordParts(parts,{maxBytes,maxParts:maxNodes,onTruncate,onRedact}).parts;
      for(let i=0;i<safe.length;i++)result[positions[i][0]]=filteredRecordField({type:positions[i][1]},'text',safe[i]);return result;
    }
    if(item&&typeof item==='object'){
      if(mediaTypes.has(item.type)){onRedact?.('attachment_data');return attachmentMetadata?attachmentMetadata(item):{type:item.type,
        ...(Number.isSafeInteger(item.size??item.bytes)&&(item.size??item.bytes)>=0?{bytes:item.size??item.bytes}:{}),
        ...(Number.isSafeInteger(item.position)&&item.position>=0?{position:item.position}:{}),...(['low','high','auto'].includes(item.detail)?{detail:item.detail}:{}),content:'[附件数据已省略]'};}
      const result=Object.create(null);for(const key in item){if(!Object.hasOwn(item,key))continue;if(onNodes?.(1)===false||nodes<=0||remaining<=0){result.recording_truncated=cut();break;}
        const name=boundedRecordText(key,Math.min(remaining,256));remaining-=Buffer.byteLength(name);if(name.length!==key.length){onTruncate();result[name]='[已脱敏]';}else result[name]=visit(item[key],depth+1,key==='content'&&Array.isArray(item[key]));
      }return result;
    }return item;
  };
  return sanitizeRecord(visit(value,0,contentParts),{maxBytes,maxNodes,onTruncate,onRedact});
}

// emit receives only sanitized copies. Raw text is confined to bounded redactors;
// hashes identify final/delta equality without retaining another full response.
export class ConversationCapture{
  constructor({emit,maxFields=256,maxBytes=2*1024*1024,maxNodes=1024}={}){
    if(typeof emit!=='function'||!Number.isSafeInteger(maxFields)||maxFields<1||!Number.isSafeInteger(maxBytes)||maxBytes<256||!Number.isSafeInteger(maxNodes)||maxNodes<1)throw TypeError('Invalid recording capture configuration');
    this.emit=emit;this.maxFields=maxFields;this.maxBytes=maxBytes;this.remainingNodes=maxNodes;this.fields=new Map();this.identities=new Map();this.itemMetadata=new Map();this.bytes=0;this.rawBytes=0;this.sequence=0;this.inputSequence=0;this.truncated=false;this.closed=false;
    this.redactionCategories=new Set();this.onRedact=category=>this.redactionCategories.add(category);this.observations=new Map();this.unsupported={items:new Set(),events:new Set(),contentParts:new Set()};this.incompleteReasons=new Set();this.opaqueReasoning=new Set();
  }
  unsupportedItem(category,key){
    const entries=this.unsupported[category];if(entries.has(key))return;
    if(entries.size>=this.maxFields*2){this.truncated=true;return;}entries.add(key);
  }
  observe(key,kind,metadata,captured=false){
    const category=kind==='tool_call'?'toolCalls':kind==='tool_result'?'toolResults':kind==='assistant'&&metadata.section==='reasoning_summary'?'reasoningSummaries':null;if(!category)return;
    let row=this.observations.get(key);
    if(!row){if(this.observations.size>=this.maxFields*2){this.truncated=true;return;}row={category,key,captured:false,call:null};this.observations.set(key,row);}
    if(row.call===null&&typeof metadata.callId==='string'&&metadata.callId.length&&metadata.callId.length<=512)row.call=digest(metadata.callId);
    row.captured||=captured;
  }
  send(kind,content,provenance='upstream'){
    if(this.closed||this.truncated)return false;
    let clipped=false;const safe=sanitizeRecord(content,{maxBytes:Math.max(0,this.maxBytes-this.bytes),onRedact:this.onRedact,onTruncate:()=>{clipped=true;}}),bytes=Buffer.byteLength(JSON.stringify(safe));
    if(clipped){this.truncated=true;return;}
    if(this.bytes+bytes>this.maxBytes){this.truncated=true;return;}
    this.bytes+=bytes;let accepted=true;try{if(this.emit({sourceKey:String(++this.sequence),kind,provenance,content:safe})===false)accepted=false;}catch{accepted=false;}if(!accepted)this.failed=true;
    if(accepted&&typeof content.field==='string')this.observe(content.field,kind,content,true);return accepted;
  }
  sendField(kind,metadata,field,text,provenance='upstream'){
    // A UTF-16 chunk of this size is at most 24 KiB when JSON-escaped,
    // leaving room for bounded metadata below the repository's event limit.
    if(!text.length)return this.send(kind,filteredRecordField(metadata,field,text),provenance);
    let accepted=false;
    for(let offset=0;offset<text.length&&!this.truncated;){
      let end=Math.min(text.length,offset+4096);if(end<text.length&&/[\uD800-\uDBFF]/.test(text[end-1]))end--;
      accepted=this.send(kind,filteredRecordField({...metadata,...(offset?{mode:'append'}:{})},field,text.slice(offset,end)),provenance)||accepted;offset=end;
    }return accepted;
  }
  sendText(kind,metadata,text,provenance='upstream'){return this.sendField(kind,metadata,'text',text,provenance);}
  input(payload,current=currentInput(payload)){
    let clipped=false;const users=current.events.filter(event=>event.kind==='user'),filtered=redactRecordParts(users.map(event=>event.content.text),{maxBytes:Math.max(0,this.maxBytes-this.rawBytes),onRedact:this.onRedact,onTruncate:()=>{clipped=true;}});this.rawBytes+=filtered.bytes;
    let userIndex=0;
    for(const [position,event] of current.events.entries()){
      if(this.truncated)break;
      if(event.kind==='user'){
        const text=filtered.parts[userIndex++]||'';if(text)this.sendText('user',{field:'input:'+(event.group??'0')+':'+position,position,mode:'append'},text,'client');
      }else if(event.kind==='tool_result'){
        this.result('input-tool:'+(++this.inputSequence),{callId:event.content.callId,toolType:event.content.toolType||'function_call',...(typeof event.content.name==='string'?{name:event.content.name}:{}),...(typeof event.content.namespace==='string'?{namespace:event.content.namespace}:{})},event.content.output,'client');
      }else this.send(event.kind,{...event.content,position},'client');
    }
    for(const [field,category] of [['unsupportedItems','items'],['unsupportedContentParts','contentParts']])for(let i=0;i<Math.min(this.maxFields,current[field]||0);i++)this.unsupportedItem(category,'input:'+i);
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
    this.observe(key,kind,metadata);let state=this.fields.get(key);
    if(!state&&!this.truncated){
      if(this.fields.size>=this.maxFields){this.truncated=true;return null;}
      const safe=sanitizeRecord(metadata,{maxBytes:8192,onRedact:this.onRedact,onTruncate:()=>{this.truncated=true;}});if(this.truncated)return null;
      state={key,kind,metadata:safe,filter:kind==='tool_call'&&metadata.toolType!=='custom_tool_call'?new JsonRecordRedactor({onRedact:this.onRedact,onTruncate:()=>{this.truncated=true;}}):new RecordRedactor({onRedact:this.onRedact}),hash:createHash('sha256'),closed:false};this.fields.set(key,state);
    }
    return state;
  }
  delta(key,kind,metadata,text){
    if(this.closed||this.truncated)return;
    if(typeof text!=='string'){if(kind==='tool_call'){this.observe(key,kind,metadata);this.incompleteReasons.add('malformed_tool_item');}else this.unsupportedItem('contentParts',key);return;}
    const field=this.field(key,kind,metadata);if(!field||field.closed)return;
    const prefix=boundedRecordText(text,Math.max(0,this.maxBytes-this.rawBytes));this.rawBytes+=Buffer.byteLength(prefix);
    field.hash.update(prefix);const safe=field.filter.push(prefix);if(safe)this.sendText(kind,{...field.metadata,field:key,mode:'append'},safe);
    if(prefix.length!==text.length){const tail=field.filter.finish();if(tail)this.sendText(kind,{...field.metadata,field:key,mode:'append'},tail);field.closed=true;field.hash=null;this.truncated=true;}
  }
  complete(key,kind,metadata,text){
    if(this.closed||this.truncated)return;
    if(typeof text!=='string'){if(kind==='tool_call'){this.observe(key,kind,metadata);this.incompleteReasons.add('malformed_tool_item');}else this.unsupportedItem('contentParts',key);return;}
    if(boundedRecordText(text,this.maxBytes).length!==text.length){if(this.fields.get(key)?.closed)this.truncated=true;else this.delta(key,kind,metadata,text);return;}
    const field=this.field(key,kind,metadata);if(!field)return;
    const finalHash=digest(text),same=field.closed?field.finalHash===finalHash:field.hash.copy().digest('hex')===finalHash;
    const safeMetadata=sanitizeRecord(metadata,{maxBytes:8192,onRedact:this.onRedact,onTruncate:()=>{this.truncated=true;}});if(this.truncated)return;
    const updates=Object.fromEntries(Object.entries(safeMetadata).filter(([name,value])=>value!==null||field.metadata[name]==null)),changed=Object.entries(updates).some(([name,value])=>field.metadata[name]!==value);field.metadata={...field.metadata,...updates};
    const tail=field.closed?'':field.filter.finish();
    if(same){if(!field.closed||tail||changed)this.sendText(kind,{...field.metadata,field:key,mode:'append'},tail);}
    else{
      let clipped=false;const options={maxBytes:Math.max(0,this.maxBytes-this.bytes),onRedact:this.onRedact,onTruncate:()=>{clipped=true;}};
      const safe=kind==='tool_call'&&field.metadata.toolType!=='custom_tool_call'?sanitizeToolRecord(text,options):sanitizeRecord(text,options);this.sendText(kind,{...field.metadata,field:key,mode:'replace'},safe);if(clipped)this.truncated=true;
    }
    field.closed=true;field.hash=null;field.finalHash=finalHash;
  }
  result(key,metadata,value,provenance='upstream'){
    const field=this.field(key,'tool_result',metadata);if(!field)return;
    if(value===undefined){this.incompleteReasons.add('malformed_tool_item');return;}
    let clipped=false,unsupported=0;const output=safeToolOutput(value,{maxBytes:this.maxBytes,maxNodes:Math.max(1,this.remainingNodes),contentParts:metadata.toolType==='custom_tool_call'&&Array.isArray(value),onRedact:this.onRedact,onTruncate:()=>{clipped=true;},onUnsupported:()=>this.unsupportedItem('contentParts',key+':output:'+(unsupported++))}),encoded=JSON.stringify(output),hash=digest(encoded??'');
    const safeMetadata=sanitizeRecord(metadata,{maxBytes:8192,onRedact:this.onRedact,onTruncate:()=>{clipped=true;}}),updates=Object.fromEntries(Object.entries(safeMetadata).filter(([name,value])=>value!==null||field.metadata[name]==null)),changed=Object.entries(updates).some(([name,value])=>field.metadata[name]!==value);field.metadata={...field.metadata,...updates};
    if(hash!==field.resultHash){
      const details={...field.metadata,field:key,mode:'replace'};
      if(Buffer.byteLength(encoded)>32*1024)this.sendField('tool_result',{...details,output_format:typeof output==='string'?'string':'json'},'output',typeof output==='string'?output:encoded,provenance);
      else this.send('tool_result',filteredRecordField(details,'output',output),provenance);
    }else if(changed)this.send('tool_result',filteredRecordField({...field.metadata,field:key,mode:'append'},'output',''),provenance);
    field.resultHash=hash;field.closed=true;field.hash=null;if(clipped)this.truncated=true;
  }
  toolMetadata(item,id,type){
    return {item:id,toolType:type,callId:typeof item.call_id==='string'?item.call_id:null,...(typeof item.name==='string'?{name:item.name}:{}),...(typeof item.namespace==='string'?{namespace:item.namespace}:{})};
  }
  item(item,index){
    if(this.closed||this.truncated)return;if(this.remainingNodes--<=0){this.truncated=true;return;}
    if(!item||typeof item!=='object'){this.unsupportedItem('items','invalid:'+index);return;}
    const id=item.id||String(index),key=this.identity(index,item.id);
    if(item.type==='function_call'||item.type==='custom_tool_call'){
      const custom=item.type==='custom_tool_call',metadata=this.toolMetadata(item,id,item.type),value=custom?item.input:item.arguments,field=key+(custom?':input':':arguments');
      this.observe(field,'tool_call',metadata);
      if(typeof value!=='string')this.incompleteReasons.add('malformed_tool_item');
      this.complete(field,'tool_call',metadata,value);return;
    }
    if(resultTypes.has(item.type)){
      this.result(key+':output',this.toolMetadata(item,id,item.type==='custom_tool_call_output'?'custom_tool_call':'function_call'),item.output);return;
    }
    if(item.type==='reasoning'){
      if(Object.hasOwn(item,'encrypted_content')||Object.hasOwn(item,'content')){
        if(this.opaqueReasoning.has(key)||this.opaqueReasoning.size<this.maxFields*2)this.opaqueReasoning.add(key);else this.truncated=true;
      }
      const summary=Array.isArray(item.summary)?item.summary:[];
      for(let i=0;i<summary.length&&!this.truncated;i++){
        if(this.remainingNodes--<=0){this.truncated=true;break;}const part=summary[i];
        if(part?.type==='summary_text'&&typeof part.text==='string')this.complete(key+':summary:'+i,'assistant',{item:id,section:'reasoning_summary'},part.text);
        else this.unsupportedItem('contentParts',key+':summary:'+i);
      }return;
    }
    if(item.type!=='message'){this.unsupportedItem('items',key);return;}
    const content=Array.isArray(item.content)?item.content:[];if(item.content!==undefined&&!Array.isArray(item.content))this.unsupportedItem('contentParts',key+':content');
    for(let i=0;i<content.length&&!this.truncated;i++){
      if(this.remainingNodes<=0){this.truncated=true;break;}this.remainingNodes--;const part=content[i];
      const metadata={item:id,...(typeof item.phase==='string'?{phase:item.phase}:{})};
      if(part?.type==='output_text')this.complete(key+':'+i+':text','assistant',metadata,part.text);
      else if(part?.type==='refusal')this.complete(key+':'+i+':refusal','assistant',{...metadata,refusal:true},part.refusal);
      else this.unsupportedItem('contentParts',key+':'+i);
    }
  }
  event(event){
    if(this.closed||this.truncated)return;
    const key=this.identity(event.output_index,event.item_id||event.item?.id),id=event.item_id||event.item?.id||String(event.output_index??0),index=event.content_index??0,type=typeof event.type==='string'?event.type.slice(0,256):'';
    if(type==='response.output_item.added'){
      if(event.item?.type==='function_call'||event.item?.type==='custom_tool_call'){
        const custom=event.item.type==='custom_tool_call',metadata=this.toolMetadata(event.item,id,event.item.type),field=key+(custom?':input':':arguments');
        this.observe(field,'tool_call',metadata);
        if(this.itemMetadata.has(key)||this.itemMetadata.size<this.maxFields)this.itemMetadata.set(key,sanitizeRecord(metadata,{maxBytes:8192,onRedact:this.onRedact,onTruncate:()=>{this.truncated=true;}}));else this.truncated=true;
        const state=this.field(field,'tool_call',this.itemMetadata.get(key)||{item:id,toolType:event.item.type});
        if(state){state.metadata={...state.metadata,...this.itemMetadata.get(key)};this.send('tool_call',{...state.metadata,field,mode:'append',text:''});}
        const value=custom?event.item.input:event.item.arguments;if(typeof value==='string'&&value.length)this.delta(field,'tool_call',this.itemMetadata.get(key),value);return;
      }
      if(resultTypes.has(event.item?.type)){
        const metadata=this.toolMetadata(event.item,id,event.item.type==='custom_tool_call_output'?'custom_tool_call':'function_call');
        if(event.item.output!==undefined)this.result(key+':output',metadata,event.item.output);else this.field(key+':output','tool_result',metadata);
      }else if(!['message','reasoning'].includes(event.item?.type))this.unsupportedItem('items',key);return;
    }
    const tool=(custom=false)=>({...this.itemMetadata.get(key),item:id,toolType:custom?'custom_tool_call':'function_call'});
    if(type==='response.output_text.delta')this.delta(key+':'+index+':text','assistant',{item:id},event.delta);
    else if(type==='response.output_text.done')this.complete(key+':'+index+':text','assistant',{item:id},event.text);
    else if(type==='response.refusal.delta')this.delta(key+':'+index+':refusal','assistant',{item:id,refusal:true},event.delta);
    else if(type==='response.refusal.done')this.complete(key+':'+index+':refusal','assistant',{item:id,refusal:true},event.refusal);
    else if(type==='response.function_call_arguments.delta')this.delta(key+':arguments','tool_call',tool(),event.delta);
    else if(type==='response.function_call_arguments.done')this.complete(key+':arguments','tool_call',tool(),event.arguments);
    else if(type==='response.custom_tool_call_input.delta')this.delta(key+':input','tool_call',tool(true),event.delta);
    else if(type==='response.custom_tool_call_input.done')this.complete(key+':input','tool_call',tool(true),event.input);
    else if(type==='response.reasoning_summary_text.delta')this.delta(key+':summary:'+(event.summary_index??0),'assistant',{item:id,section:'reasoning_summary'},event.delta);
    else if(type==='response.reasoning_summary_text.done')this.complete(key+':summary:'+(event.summary_index??0),'assistant',{item:id,section:'reasoning_summary'},event.text);
    else if(type==='response.reasoning_summary_part.done'||type==='response.reasoning_summary_part.added'){
      if(event.part?.type==='summary_text'){
        if(type.endsWith('.done'))this.complete(key+':summary:'+(event.summary_index??0),'assistant',{item:id,section:'reasoning_summary'},event.part.text);
        else if(typeof event.part.text==='string'&&event.part.text.length)this.delta(key+':summary:'+(event.summary_index??0),'assistant',{item:id,section:'reasoning_summary'},event.part.text);
      }
      else this.unsupportedItem('contentParts',key+':summary:'+(event.summary_index??0));
    }else if(type==='response.output_item.done')this.item(event.item,event.output_index);
    else if(type==='response.content_part.added'||type==='response.content_part.done'){
      const refusal=event.part?.type==='refusal',partKey=key+':'+index+(refusal?':refusal':':text'),metadata={item:id,...(refusal?{refusal:true}:{})},text=refusal?event.part.refusal:event.part?.text;
      if(!['output_text','refusal'].includes(event.part?.type))this.unsupportedItem('contentParts',key+':'+index);
      else if(type.endsWith('.done'))this.complete(partKey,'assistant',metadata,text);
      else if(typeof text==='string'&&text.length)this.delta(partKey,'assistant',metadata,text);
    }else if(/tool|_call/.test(type))this.unsupportedItem('events',key+':'+digest(type));
  }
  finish(response){
    if(this.closed)return;
    const output=Array.isArray(response?.output)?response.output:[];
    for(let i=0;i<output.length&&!this.truncated;i++){if(this.remainingNodes<=0){this.truncated=true;break;}this.item(output[i],i);}
    for(const field of this.fields.values()){
      if(!field.closed){const text=field.filter.finish();if(text)this.sendText(field.kind,{...field.metadata,field:field.key,mode:'append'},text);if(field.kind==='tool_call'||field.kind==='tool_result')this.incompleteReasons.add('malformed_tool_item');}
      if(field.kind==='tool_call'||field.kind==='tool_result'){
        if(typeof field.metadata.callId!=='string'||!field.metadata.callId.length||field.metadata.callId.length>512||field.kind==='tool_call'&&(typeof field.metadata.name!=='string'||!field.metadata.name.length))this.incompleteReasons.add('malformed_tool_item');
      }
    }
    const seen={toolCalls:0,toolResults:0,reasoningSummaries:0},captured={...seen},unique=new Map();
    for(const row of this.observations.values()){
      const key=row.category+':'+(row.call??row.key),previous=unique.get(key);if(previous)previous.captured||=row.captured;else unique.set(key,{category:row.category,captured:row.captured});
    }
    for(const row of unique.values()){seen[row.category]++;if(row.captured)captured[row.category]++;}
    const unsupported=Object.fromEntries(Object.entries(this.unsupported).map(([category,entries])=>[category,entries.size]));
    for(const [category,reason] of [['items','unsupported_item'],['events','unsupported_event'],['contentParts','unsupported_content']])if(unsupported[category])this.incompleteReasons.add(reason);
    if(this.truncated)this.incompleteReasons.add('size_limit');if(this.failed)this.incompleteReasons.add('capture_failed');
    const incomplete=this.incompleteReasons.size>0,coverage={version:1,redaction:recordRedactionSummary(this.redactionCategories),complete:!incomplete,seen,captured,unsupported,opaqueReasoningExcluded:this.opaqueReasoning.size,reasons:[...this.incompleteReasons].sort()};
    this.closed=true;this.fields.clear();this.identities.clear();this.itemMetadata.clear();this.observations.clear();for(const values of Object.values(this.unsupported))values.clear();this.opaqueReasoning.clear();
    return {truncated:this.truncated,failed:!!this.failed,bytes:this.bytes,incomplete,coverage};
  }
}
