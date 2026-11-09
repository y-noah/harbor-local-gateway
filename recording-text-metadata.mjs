import {boundedRecordText,sanitizeRecord,restoreAuthenticatedRecord,redactRecordParts} from './record-redaction.mjs';

// Metadata has its own typed positions: schema/literal exemptions must never
// admit media bodies or encoded credential bytes through a text annotation.
export function recordingTextMetadata(kind,value,{maxBytes,onNodes,onTruncate,onRedact,onUnsupported,tokenSequence,expectedText}={}){
 if(value===null&&['annotations','logprobs'].includes(kind))return null;
 let remaining=maxBytes;const primary=tokenSequence??[],start=primary.length;
 const cut=()=>{if(kind==='logprobs')primary.incomplete=true;onTruncate();return '[已截断]';},unsupported=()=>{if(kind==='logprobs')primary.incomplete=true;onUnsupported('unsupported_content');return '[未支持的正文元信息已省略]';};
 const visit=()=>onNodes(1)!==false;
 const text=value=>{if(typeof value!=='string')return unsupported();const prefix=boundedRecordText(value,Math.max(0,remaining));remaining-=Buffer.byteLength(prefix);if(prefix!==value)onTruncate();return sanitizeRecord(prefix,{maxBytes,onTruncate,onRedact});};
 const list=(items,convert)=>{
  if(!Array.isArray(items))return unsupported();const result=[];
  for(const item of items){if(!visit()||remaining<=0){result.push(cut());break;}result.push(convert(item));}
  return result;
 };
 const annotation=item=>{
  const types={url_citation:['url','title','start_index','end_index'],file_citation:['file_id','index','filename'],container_file_citation:['container_id','file_id','filename','start_index','end_index'],file_path:['file_id','index']};
  if(!item||typeof item!=='object'||Array.isArray(item)||typeof item.type!=='string'||!Object.hasOwn(types,item.type))return unsupported();
  const result={type:item.type};
  for(const key in item){
   if(!Object.hasOwn(item,key))continue;
   if(!visit()){cut();break;}if(key==='type')continue;
   if(!types[item.type].includes(key)){onUnsupported('unsupported_field');continue;}
   if(['url','file_id','container_id'].includes(key)){
    if(typeof item[key]!=='string'){result[key]=unsupported();continue;}
    onRedact('attachment_data');result[key]='[引用已省略]';continue;
   }
   if(['title','filename'].includes(key)){result[key]=text(item[key]);continue;}
   if(!Number.isSafeInteger(item[key])||item[key]<0){result[key]=unsupported();continue;}result[key]=item[key];
  }
  return result;
 };
 const probability=(item,group,allowAlternatives)=>{
  if(!item||typeof item!=='object'||Array.isArray(item)||typeof item.token!=='string')return unsupported();const result={};
  for(const key in item){
   if(!Object.hasOwn(item,key))continue;
   if(!visit()){cut();break;}
   if(key==='token'){
    if(typeof item.token!=='string'){result.token=unsupported();continue;}
    if(!allowAlternatives){onRedact('sensitive_field');result.token='[已脱敏]';continue;}
    const prefix=boundedRecordText(item.token,Math.max(0,remaining));remaining-=Buffer.byteLength(prefix);if(prefix!==item.token)cut();
    result.token='';group.push({result,text:prefix});continue;
   }
   if(key==='logprob'){result.logprob=item.logprob===null||typeof item.logprob==='number'&&Number.isFinite(item.logprob)?item.logprob:unsupported();continue;}
   if(key==='bytes'){
    if(item.bytes!==null&&!Array.isArray(item.bytes)){result.bytes=unsupported();continue;}
    // Raw numeric bytes can reconstruct a token after its text was redacted.
    // Retain only the declared element count, never the encoded byte stream.
    onRedact('attachment_data');result.bytes={omitted:true,elements:item.bytes===null?null:item.bytes.length};continue;
   }
   if(key==='top_logprobs'&&allowAlternatives){result.top_logprobs=list(item.top_logprobs,entry=>probability(entry,null,false));continue;}
   onUnsupported('unsupported_field');
  }
  return result;
 };
 const result=kind==='annotations'?list(value,annotation):kind==='logprobs'?list(value,item=>probability(item,primary,true)):unsupported();
 if(kind==='logprobs'){
  let offset=0;for(let index=start;index<primary.length;index++){const token=primary[index].text;if(typeof expectedText!=='string'||!expectedText.startsWith(token,offset))primary.incomplete=true;offset+=token.length;}
  if(typeof expectedText!=='string'||offset!==expectedText.length)primary.incomplete=true;
 }
 if(tokenSequence){(tokenSequence.metadata??=[]).push(result);return result;}
 finalizeRecordingTokens(primary,{maxBytes,onTruncate,onRedact});return restoreAuthenticatedRecord(result);
}

// A component contains one protocol message. Its emitted token stream may span
// multiple output_text blocks; mutually exclusive candidates are never joined.
export function finalizeRecordingTokens(group,{maxBytes,onTruncate,onRedact}){
 if(group.incomplete){if(group.length)onRedact('sensitive_field');for(const item of group)item.result.token='[已脱敏]';for(const result of group.metadata??[])restoreAuthenticatedRecord(result);return;}
 const safe=redactRecordParts(group.map(item=>item.text),{maxBytes,maxParts:group.length||1,onTruncate,onRedact}).parts;
 for(let index=0;index<group.length;index++)group[index].result.token=safe[index]??'';
 for(const result of group.metadata??[])restoreAuthenticatedRecord(result);
}
