import {sanitizeRecord} from './record-redaction.mjs';

// JSON tool routes are not dialogue turns. Keep sanitized prompt text and
// structural attachment facts, never base64, URLs, file IDs or hidden output.
export function captureCodexJsonInput(capture,path,payload){
  let clipped=false;const prompt=sanitizeRecord(payload.prompt,{maxBytes:Math.max(0,capture.maxBytes-capture.bytes),onTruncate:()=>{clipped=true;}});
  capture.sendText('boundary',{channel:path,part:'prompt',field:'json-input',mode:'replace'},prompt,'client');if(clipped)capture.truncated=true;
  const images=Array.isArray(payload.images)?payload.images:[];
  for(let i=0;i<images.length&&i<capture.maxFields&&!capture.truncated;i++)capture.send('attachment',{channel:path,index:i,type:'input_image',encoded_bytes:Buffer.byteLength(images[i].image_url)},'client');
  if(images.length>capture.maxFields)capture.truncated=true;
}
export function captureCodexJsonOutput(capture,path,response){
  const images=Array.isArray(response?.data)?response.data:[];
  for(let i=0;i<images.length&&i<capture.maxFields&&!capture.truncated;i++){
    capture.send('attachment',{channel:path,index:i,type:'generated_image',encoding:'base64',encoded_bytes:typeof images[i].b64_json==='string'?Buffer.byteLength(images[i].b64_json):null});
    if(typeof images[i].revised_prompt==='string')capture.complete('json-result-prompt:'+i,'boundary',{channel:path,index:i,part:'revised_prompt'},images[i].revised_prompt);
  }
  if(images.length>capture.maxFields)capture.truncated=true;
}
