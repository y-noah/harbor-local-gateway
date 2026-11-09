import {sanitizeRecord} from './record-redaction.mjs';

// Never fetch references or retain bearer URLs, client digests, or media bodies.
// A fingerprint is evidence about received bytes, not file identity or MIME validity.
export function recordingAttachment(value,{fingerprint,maxContentBytes=0,onInspect=()=>{},onRedact,...options}={}){
 const details=value.type==='file'&&value.file&&typeof value.file==='object'?value.file:value.type==='input_audio'&&value.input_audio&&typeof value.input_audio==='object'?value.input_audio:value;
 const image=typeof value.image_url==='string'?value.image_url:value.image_url?.url;
 const encoded=details.file_data??details.data??(typeof image==='string'&&image.startsWith('data:')?image:undefined);
 const declared=details.size??details.bytes;
 const result={attachmentType:value.type,filename:typeof details.filename==='string'?details.filename:null,mime_type:typeof details.mime_type==='string'?details.mime_type:null,
  bytes:Number.isSafeInteger(declared)&&declared>=0?declared:null,sizeSource:Number.isSafeInteger(declared)&&declared>=0?'client_declared':'unknown',
  referencePresent:typeof details.file_id==='string'||typeof details.file_url==='string'||typeof image==='string'&&!image.startsWith('data:'),
  fingerprint:{state:'unavailable',reason:typeof encoded==='string'?'content_not_verified':'content_unavailable'},content:'[附件数据已省略]'};
 if(typeof encoded==='string'&&typeof fingerprint==='function'&&Number.isSafeInteger(maxContentBytes)&&maxContentBytes>=0){
  // Check encoded length before scanning/decoding. The caller supplies a shared
  // per-request budget, so repeated attachments cannot multiply decoding work.
  const comma=encoded.startsWith('data:')?encoded.slice(0,257).indexOf(','): -1;
  const header=comma>=0&&comma<=256?encoded.slice(0,comma):null;
  const offset=header===null?0:comma+1,length=encoded.length-offset;
  if(length>4*Math.ceil(maxContentBytes/3))result.fingerprint.reason='size_limit';
  else if(encoded.startsWith('data:')&&header===null)result.fingerprint.reason='unsupported_encoding';
  else if(header!==null&&!/^data:[^;,]*;base64$/i.test(header))result.fingerprint.reason='unsupported_encoding';
  else {
   onInspect(Math.ceil(length/4)*3);
   const base64=encoded.slice(offset);
   if(base64.length%4!==0||!/^[A-Za-z0-9+/]*={0,2}$/.test(base64))result.fingerprint.reason='invalid_encoding';
   else {
    const bytes=Buffer.from(base64,'base64');
    if(bytes.length>maxContentBytes)result.fingerprint.reason='size_limit';
    else if(bytes.toString('base64')!==base64)result.fingerprint.reason='invalid_encoding';
    else {
     const digest=fingerprint(bytes);
     if(typeof digest==='string'&&/^[a-f0-9]{64}$/.test(digest)){
      result.fingerprint={state:'verified',algorithm:'hmac-sha256',scope:'request',source:'received_bytes',value:digest};
      result.declaredBytes=result.bytes;result.bytes=bytes.length;result.sizeSource='received_bytes';
      if(result.mime_type===null&&header!==null)result.mime_type=header.slice(5,-7)||null;
     }
    }
   }
  }
 }
 onRedact?.('attachment_data');
 return sanitizeRecord(result,{...options,onRedact});
}
