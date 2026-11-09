import {sanitizeRecord} from './record-redaction.mjs';

// IDs and URLs may be bearer credentials. Preserve presence and safe descriptive
// facts, never the reference itself or the attachment body.
export function recordingAttachment(value,options={}){
 const details=value.type==='file'&&value.file&&typeof value.file==='object'?value.file:value;
 return sanitizeRecord({attachmentType:value.type,
  ...Object.fromEntries(['filename','mime_type'].filter(key=>typeof details[key]==='string').map(key=>[key,details[key]])),
  ...(Number.isSafeInteger(details.size??details.bytes)&&(details.size??details.bytes)>=0?{bytes:details.size??details.bytes}:{}),
  referencePresent:typeof details.file_id==='string'||typeof details.file_url==='string'||typeof value.image_url==='string'||typeof value.image_url?.url==='string',
  content:'[附件数据已省略]'},options);
}
