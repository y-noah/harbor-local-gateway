import {AppError} from './store.mjs';
import {captureRequestContext,captureRequestContextAsync,requestContextReasons} from './recording-request-context.mjs';
import {recordingBufferWrite} from './postgres-recording-storage.mjs';
import {sanitizeRecord,recordRedactionSummary,recordRedactionCategories} from './record-redaction.mjs';

const finishReasons=new Set(['capture_failed','queue_limit','size_limit','storage_low','storage_unknown']);
export function createRequestContextCapture(buffer,{policyBuffer=buffer,cooperative=false}={}){
 if(!buffer?.repository?.contextRepository||!Number.isSafeInteger(buffer.maxMemoryBytes)||typeof policyBuffer.contextSnapshot!=='function'||typeof cooperative!=='boolean')throw new AppError(503,'上下文缓冲未配置','recording_configuration');
 return (requestId,payload,{signal}={})=>{
  // Reserve the actual command envelope, including the largest possible fixed
  // coverage metadata. The extractor's array budget then fits the same queue.
  const envelope=Buffer.byteLength(JSON.stringify({type:'context',requestId,components:[],coverage:{state:'incomplete',reasons:requestContextReasons,opaqueReasoningExcluded:Number.MAX_SAFE_INTEGER,redaction:recordRedactionSummary(recordRedactionCategories)}}))-2;
  const maxBytes=Math.min(buffer.repository.maxBytes,buffer.maxMemoryBytes,buffer.repository.contextRepository.maxSnapshotBytes)-envelope;
  if(maxBytes<256)return {accepted:false,state:'incomplete',reason:'size_limit'};
  const options={maxBytes,maxComponents:buffer.repository.contextRepository.maxComponents,attachmentFingerprint:typeof buffer.repository.contextRepository.attachmentFingerprint==='function'?bytes=>buffer.repository.contextRepository.attachmentFingerprint(requestId,bytes):undefined};
  const finish=view=>{
  signal?.throwIfAborted();const coverage={state:view.state,reasons:view.reasons,opaqueReasoningExcluded:view.opaqueReasoningExcluded,redaction:view.redaction};
  // Contents already carry filtered provenance. Validate and mark the aggregate
  // too, so the FIFO's generic walker need not rescan thousands of component
  // wrappers under its smaller default node budget. One array node, then one
  // object plus one visit for each fixed wrapper field, is the exact budget.
  let clipped=false;const components=sanitizeRecord(view.components,{maxBytes,maxNodes:1+view.components.reduce((count,component)=>count+1+Object.keys(component).length,0),onTruncate:()=>{clipped=true;}});
  if(clipped||!Array.isArray(components))return {accepted:false,state:'incomplete',reason:'size_limit'};
  const result=recordingBufferWrite(policyBuffer,'contextSnapshot',requestId,components,coverage);
  return {accepted:result.accepted,state:view.state,reason:!result.accepted?result.reason:view.state==='complete'?null:view.reasons.includes('size_limit')?'size_limit':'capture_failed'};
  };
  return cooperative?captureRequestContextAsync(payload,{...options,signal}).then(finish):finish(captureRequestContext(payload,options));
 };
}
export async function observeRequestContextAsync(capture,requestId,payload,{signal}={}){
 if(capture===undefined)return null;
 try{
  const result=await capture(requestId,payload,{signal});
  if(result&&typeof result.accepted==='boolean'&&['complete','incomplete'].includes(result.state)&&(result.accepted&&result.state==='complete'?result.reason===null:finishReasons.has(result.reason)))return result;
 }catch{}
 return {accepted:false,state:'incomplete',reason:'capture_failed'};
}

// Context capture is a side channel: an exception or malformed adapter result
// must neither disable the ordinary event collector nor change model execution.
export function observeRequestContext(capture,requestId,payload){
 if(capture===undefined)return null;
 try{const result=capture(requestId,payload);if(result&&typeof result.then==='function')void Promise.resolve(result).catch(()=>{});
  if(result&&typeof result.accepted==='boolean'&&['complete','incomplete'].includes(result.state)&&(result.accepted&&result.state==='complete'?result.reason===null:finishReasons.has(result.reason)))return result;
 }catch{}
 return {accepted:false,state:'incomplete',reason:'capture_failed'};
}
export const incompleteRequestContext=result=>!!result&&(!result.accepted||result.state!=='complete');
