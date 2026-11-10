import {preDispatchTimingStages} from './request-stream-timing.mjs';
const keys=Object.freeze({
  root:['version','clock','upstreamBoundary','rawReadBoundary','writeBoundary','clientReceiptVerified','outcome','complete','offsetsMs','durationsMs','counts','missing'],
  offsets:['started','bodyEnd','dispatch','headers','firstRawChunk','firstDecodedEvent','firstUpstreamEvent','firstUpstreamContent','lastUpstreamContent','firstWriteStart','firstContentWriteStart','firstWriteComplete','lastWriteComplete','firstContentWriteComplete','transportEnd','responseEnd','finished'],
  durations:['bodyRead','beforeDispatch','upstreamHeaders','upstreamFirstRawChunk','firstRawToDecoded','upstreamFirstEvent','upstreamFirstContent','firstContentRelay','firstContentWrite','upstream','response','total','maxContentGap','maxRelay','maxWriteWait','totalWriteWait'],
  counts:['rawChunks','rawBytes','decodedEvents','upstreamEvents','contentChunks','contentBytes','writes','completedWrites','contentWrites','completedContentWrites','retiredContentChunks','wireBytes','backpressuredWrites','failedWrites'],
});
const reasons=new Set(['clock_invalid','duration_budget','clock_unavailable','content_classification','content_budget','inflight_budget','write_bytes_invalid','content_pair_invalid','content_pair_missing','write_pair_invalid','write_failed','outcome_invalid','missing_content_write','stage_order','unpaired_events','outcome_unknown','raw_bytes_invalid','content_not_relayed',...['started','bodyEnd','dispatch','headers','firstRawChunk','firstDecodedEvent','transportEnd','responseEnd'].map(name=>'missing_'+name)]);
const outcomes=new Set(['ok','failed','cancelled','interrupted','unknown']),maxMs=30*86400000;
const invalid=()=>{throw TypeError('request_stream_timing_invalid');};
function object(value,allowed,{optional=false}={}){
  if(!value||typeof value!=='object'||Array.isArray(value)||![Object.prototype,null].includes(Object.getPrototypeOf(value)))invalid();
  const descriptors=Object.getOwnPropertyDescriptors(value),names=Reflect.ownKeys(descriptors);
  if(names.some(name=>typeof name!=='string'||!allowed.includes(name))||!optional&&allowed.some(name=>!Object.hasOwn(descriptors,name))||names.some(name=>!Object.hasOwn(descriptors[name],'value')||!descriptors[name].enumerable))invalid();
  return Object.fromEntries(names.map(name=>[name,descriptors[name].value]));
}
const duration=value=>value===null||typeof value==='number'&&Number.isFinite(value)&&value>=0&&value<=maxMs;
const count=value=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0;
// Fixed summary wire contract for DB / durable recovery. Caller-provided object
// spread is deliberately forbidden: even an authenticated spool can be stale or
// malformed. Trace samples are a separate diagnostic capability, never persisted
// through this function. No coercion, content, IDs or arbitrary reasons survive.
export function canonicalRequestStreamTiming(value){
  const root=object(value,[...keys.root,'preDispatchOffsetsMs'],{optional:true});if(keys.root.some(name=>!Object.hasOwn(root,name)))invalid();
  if(root.version!==1||root.clock!=='process-monotonic'||root.upstreamBoundary!=='decoded-response-event'||root.rawReadBoundary!=='fetch-response-body-chunk'||root.writeBoundary!=='node-socket-write-callback'||root.clientReceiptVerified!==false||!outcomes.has(root.outcome)||typeof root.complete!=='boolean')invalid();
  const offsets=object(root.offsetsMs,keys.offsets,{optional:true}),durations=object(root.durationsMs,keys.durations),counts=object(root.counts,keys.counts);
  let preparation;
  if(Object.hasOwn(root,'preDispatchOffsetsMs')){
    preparation=object(root.preDispatchOffsetsMs,preDispatchTimingStages,{optional:true});let previous=offsets.bodyEnd??offsets.started??0;
    for(const name of preDispatchTimingStages)if(Object.hasOwn(preparation,name)){const at=preparation[name];if(at===null||!duration(at)||at<previous||offsets.dispatch!==undefined&&at>offsets.dispatch||offsets.finished!==undefined&&at>offsets.finished)invalid();previous=at;}
  }
  if(Object.values(offsets).some(value=>value===null||!duration(value))||Object.values(durations).some(value=>!duration(value))||Object.entries(counts).some(([name,value])=>!count(value)&&!(value===null&&['rawChunks','rawBytes','decodedEvents'].includes(name))))invalid();
  if((counts.rawChunks===null)!==(counts.rawBytes===null)||counts.backpressuredWrites>counts.writes||counts.failedWrites>counts.writes)invalid();
  if(!Array.isArray(root.missing)||Object.getPrototypeOf(root.missing)!==Array.prototype)invalid();
  const reasonDescriptors=Object.getOwnPropertyDescriptors(root.missing),length=reasonDescriptors.length?.value;
  if(!Number.isInteger(length)||length>reasons.size||Reflect.ownKeys(reasonDescriptors).length!==length+1)invalid();
  const missing=[];for(let i=0;i<length;i++){const descriptor=reasonDescriptors[String(i)];if(!descriptor||!Object.hasOwn(descriptor,'value')||!descriptor.enumerable||!reasons.has(descriptor.value))invalid();missing.push(descriptor.value);}
  if(new Set(missing).size!==missing.length||root.complete!==(missing.length===0))invalid();
  const required=['started','bodyEnd','dispatch','headers','firstRawChunk','firstDecodedEvent','transportEnd','responseEnd'];
  for(const name of required)if(Object.hasOwn(offsets,name)===missing.includes('missing_'+name))invalid();
  if(root.complete&&[...required,'finished'].some(name=>!Object.hasOwn(offsets,name)))invalid();
  const rawObserved=Object.hasOwn(offsets,'firstRawChunk'),decodeObserved=Object.hasOwn(offsets,'firstDecodedEvent');
  if(rawObserved?(counts.rawChunks===null||counts.rawChunks<1||counts.rawBytes===null):(counts.rawChunks!==null||counts.rawBytes!==null))invalid();
  if(decodeObserved?(counts.decodedEvents===null||counts.decodedEvents<1):counts.decodedEvents!==null)invalid();
  if(counts.contentChunks>counts.upstreamEvents||counts.contentChunks===0&&counts.contentBytes!==0)invalid();
  if((counts.contentChunks>0)!==Object.hasOwn(offsets,'firstUpstreamContent')||(counts.writes>0)!==Object.hasOwn(offsets,'firstWriteStart'))invalid();
  if(counts.failedWrites>0&&!missing.includes('write_failed'))invalid();
  if(counts.completedWrites+counts.failedWrites>counts.writes||counts.contentWrites+counts.retiredContentChunks>counts.contentChunks||counts.completedContentWrites>counts.contentWrites||counts.completedContentWrites>counts.completedWrites)invalid();
  if((counts.completedWrites>0)!==Object.hasOwn(offsets,'firstWriteComplete')||(counts.completedWrites>0)!==Object.hasOwn(offsets,'lastWriteComplete')||(counts.completedContentWrites>0)!==Object.hasOwn(offsets,'firstContentWriteComplete'))invalid();
  if(counts.completedWrites>0&&offsets.firstWriteComplete>offsets.lastWriteComplete)invalid();
  if(root.complete&&(counts.completedWrites!==counts.writes||counts.failedWrites!==0||counts.contentWrites!==counts.contentChunks||counts.completedContentWrites!==counts.contentChunks||counts.retiredContentChunks!==0))invalid();
  const spans={bodyRead:['started','bodyEnd'],beforeDispatch:['bodyEnd','dispatch'],upstreamHeaders:['dispatch','headers'],upstreamFirstRawChunk:['dispatch','firstRawChunk'],firstRawToDecoded:['firstRawChunk','firstDecodedEvent'],upstreamFirstEvent:['dispatch','firstUpstreamEvent'],upstreamFirstContent:['dispatch','firstUpstreamContent'],firstContentRelay:['firstUpstreamContent','firstContentWriteStart'],firstContentWrite:['firstContentWriteStart','firstContentWriteComplete'],upstream:['dispatch','transportEnd'],response:['started','responseEnd'],total:['started','finished']};
  for(const [name,[from,to]] of Object.entries(spans)){
    if(!Object.hasOwn(offsets,from)||!Object.hasOwn(offsets,to)){if(durations[name]!==null)invalid();continue;}
    if(offsets[to]<offsets[from]){if(durations[name]!==null||!missing.includes('stage_order'))invalid();continue;}
    // Offsets and spans divide the same integer-ns readings independently. Permit
    // only the IEEE-754 rounding bound of those divisions and subtraction.
    const expected=offsets[to]-offsets[from],tolerance=Number.EPSILON*Math.max(1,offsets[from],offsets[to],durations[name]??0)*8;
    if(durations[name]===null||Math.abs(durations[name]-expected)>tolerance)invalid();
  }
  const out={version:1,clock:root.clock,upstreamBoundary:root.upstreamBoundary,rawReadBoundary:root.rawReadBoundary,writeBoundary:root.writeBoundary,clientReceiptVerified:false,outcome:root.outcome,complete:root.complete,
    offsetsMs:Object.freeze(Object.fromEntries(keys.offsets.filter(name=>Object.hasOwn(offsets,name)).map(name=>[name,offsets[name]]))),
    durationsMs:Object.freeze(Object.fromEntries(keys.durations.map(name=>[name,durations[name]]))),counts:Object.freeze(Object.fromEntries(keys.counts.map(name=>[name,counts[name]]))),missing:Object.freeze(missing.sort()),...(preparation?{preDispatchOffsetsMs:Object.freeze(Object.fromEntries(preDispatchTimingStages.filter(name=>Object.hasOwn(preparation,name)).map(name=>[name,preparation[name]])))}:{})};
  return Object.freeze(out);
}

// Recording / metering callers are fail-open for optional diagnostics only.
// Invalid facts disappear; they never turn a successful model or settlement
// into a failure and are never coerced into a superficially valid summary.
export function canonicalStreamTiming(value){try{return value===undefined?undefined:canonicalRequestStreamTiming(value);}catch{return undefined;}}
