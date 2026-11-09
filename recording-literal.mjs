import {sanitizeRecord,restoreAuthenticatedRecord,sensitiveRecordField,boundedRecordText} from './record-redaction.mjs';

// JSON schema literals are data, even when their shape resembles message parts.
// Every key/value is filtered before marking the assembled data as authenticated.
export function recordingLiteral(value,{maxBytes,onNodes,onTruncate,maxDepth=24,onRedact=()=>{}}={}){
 let remaining=maxBytes;
 const cut=()=>{onTruncate();return '[已截断]';};
 const visit=(item,depth)=>{
  if(onNodes(1)===false||depth>maxDepth||remaining<=0)return cut();
  if(typeof item==='string'){const prefix=boundedRecordText(item,remaining);remaining-=Buffer.byteLength(prefix);if(prefix!==item)onTruncate();return sanitizeRecord(prefix,{maxBytes,onTruncate,onRedact});}
  if(item instanceof ArrayBuffer||ArrayBuffer.isView(item)){onRedact('attachment_data');return {type:'binary',bytes:item.byteLength,content:'[附件数据已省略]'};}
  if(Array.isArray(item)){const result=[];for(const child of item){if(remaining<=0){result.push(cut());break;}result.push(visit(child,depth+1));if(onNodes(0)===false)break;}return restoreAuthenticatedRecord(result);}
  if(!item||typeof item!=='object')return item;
  const result={};for(const key in item){
   if(onNodes(1)===false||remaining<=0){cut();break;}if(!Object.hasOwn(item,key))continue;
   const prefix=boundedRecordText(key,Math.min(remaining,256)),name=sanitizeRecord(prefix,{onRedact});remaining-=Buffer.byteLength(prefix);
   if(prefix!==key){onTruncate();continue;}if(Object.hasOwn(result,name)){onTruncate();continue;}
   if(sensitiveRecordField(key))onRedact('sensitive_field');
   const child=sensitiveRecordField(key)?'[已脱敏]':visit(item[key],depth+1);Object.defineProperty(result,name,{value:child,enumerable:true});
  }return restoreAuthenticatedRecord(result);
 };
 return visit(value,0);
}
