import {createHash,randomUUID} from 'node:crypto';
import {readFileSync} from 'node:fs';

// The same classifier is used on an actual gateway frame and an actual decoded
// client frame. Model-supplied timestamps and heartbeat bytes are never evidence.
export function classifyStageContent(event,{maxBytes=1048576,maxItems=2048}={}){
  if(!Number.isSafeInteger(maxBytes)||maxBytes<1||!Number.isSafeInteger(maxItems)||maxItems<1)throw new Error('capacity_stage_classifier_configuration');
  if(!event||typeof event!=='object')return null;
  const type=event.type;
  if(['response.output_text.delta','response.function_call_arguments.delta'].includes(type)){
    if(typeof event.delta!=='string'||!event.delta.length)return null;
    const bytes=Buffer.byteLength(event.delta);if(bytes>maxBytes||typeof event.item_id==='string'&&event.item_id.length>200)return {incomplete:true,reason:'content_budget'};
    const digest=createHash('sha256');digest.update(type+'\0');digest.update(typeof event.item_id==='string'?event.item_id:'');digest.update('\0'+String(event.output_index??'')+'\0');digest.update(event.delta);
    return {contentKind:type==='response.output_text.delta'?'text':'arguments',hash:digest.digest('hex'),bytes};
  }
  if(type!=='response.completed'||event.response?.status!=='completed'||!Array.isArray(event.response.output))return null;
  if(event.response.output.length>maxItems)return {incomplete:true,reason:'content_budget'};
  const digest=createHash('sha256');let bytes=0,items=0;
  for(const item of event.response.output){
    let parts=[];if(item?.type==='message'&&Array.isArray(item.content)){if(item.content.length>maxItems-items)return {incomplete:true,reason:'content_budget'};items+=item.content.length;for(const part of item.content)if(typeof part?.text==='string'&&part.text.length)parts.push(part.text);}
    else if(item?.type==='function_call'&&typeof item.name==='string'&&item.name.length){items+=2;parts=[item.name,typeof item.arguments==='string'?item.arguments:''];}
    for(const text of parts){if(items>maxItems)return {incomplete:true,reason:'content_budget'};bytes+=Buffer.byteLength(text);if(bytes>maxBytes)return {incomplete:true,reason:'content_budget'};digest.update(String(Buffer.byteLength(text))+':');digest.update(text);}
  }
  return bytes?{contentKind:'terminal',hash:digest.digest('hex'),bytes}:null;
}

const validId=value=>typeof value==='string'&&/^[a-zA-Z0-9._:-]{1,200}$/.test(value);
const fields=new Set(['ordinal','contentKind','hash','bytes','startNs','endNs','queued','queue','outcome','lease','action','recordingExpected','accepted','usageKnown','status']);
const types=new Set(['body-end','dispatch','admission-attempt','admission-granted','wait','content','cancel','lease','accepted','transport-end','source-unavailable']);
const noop=Object.freeze({emit:()=>false,content:()=>false});
const epoch=randomUUID();
let ownedClock;
export function streamingStageClock(){
  if(ownedClock)return ownedClock;
  const boot=readFileSync('/proc/sys/kernel/random/boot_id','utf8').trim();if(!validId(boot))throw Error('streaming_stage_clock_unavailable');
  ownedClock=Object.freeze({kind:'linux-hrtime-ns',kernelId:'linux-'+boot,clockId:'process-'+process.pid,epoch});return ownedClock;
}
// Construction-only capabilities. Capture the trusted context once per operation;
// asynchronous stream/queue callbacks retain that scope instead of guessing ALS.
export function createStreamingStageObserver(configuration){
  if(configuration===undefined){let closed=false;return Object.freeze({capture:()=>noop,status:()=>({enabled:false,failures:0,pendingCallbacks:0,closed}),clock:null,close:()=>{closed=true;}});}
  if(!configuration||typeof configuration.context!=='function'||typeof configuration.observe!=='function')throw Error('streaming_stage_observer_configuration');
  const contextForOperation=configuration.context,observe=configuration.observe;
  let failures=0,pendingCallbacks=0,closed=false,clock=null;try{clock=streamingStageClock();}catch{failures++;}
  const failed=()=>{failures++;};
  const discardPromise=value=>{if(value&&typeof value.then==='function'){pendingCallbacks++;void Promise.resolve(value).then(result=>{pendingCallbacks--;if(result===false)failed();},()=>{pendingCallbacks--;failed();});return true;}return false;};
  function capture(){
    if(closed){failed();return noop;}let context;try{context=contextForOperation();if(discardPromise(context)){failed();return noop;}if(context===null)return noop;context=context?.trace;}catch{failed();return noop;}
    const trace=context;if(!clock||!validId(trace)){failed();return noop;}
    let ordinal=0;
    const send=(type,details={},atNs=process.hrtime.bigint().toString())=>{
      try{if(closed){failed();return false;}if(!types.has(type)||!details||Object.keys(details).some(key=>!fields.has(key))){failed();return false;}
        const event=Object.freeze({trace,type,atNs,clock,...details}),result=observe(event);if(result===false)failed();else discardPromise(result);return result!==false;
      }catch{failed();return false;}
    };
    const emit=(type,details)=>send(type,details);
    const content=event=>{try{if(closed){failed();return false;}const atNs=process.hrtime.bigint().toString(),identity=classifyStageContent(event);if(!identity)return false;if(identity.incomplete){emit('source-unavailable');return false;}return send('content',{ordinal:ordinal++,...identity},atNs);}catch{failed();return false;}};
    return Object.freeze({emit,content});
  }
  return Object.freeze({capture,status:()=>({enabled:true,failures,pendingCallbacks,closed}),clock,close:()=>{closed=true;}});
}
