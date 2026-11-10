import {classifyStageContent} from './streaming-stage-observer.mjs';

const outcomes=new Set(['ok','failed','cancelled','interrupted','unknown']);
// Optional checkpoints share the request clock. Missing checkpoints are unknown;
// admissionEnd is the returned transaction boundary, not a nested COMMIT proof.
export const preDispatchTimingStages=Object.freeze(['validated','sessionReady','admissionEnd','captureReady','credentialsStart','credentialsReady']);
const ms=ns=>Number(ns)/1e6;
// Construction-time only. No headers, model metadata, account IDs or content
// enter the summary. A socket callback never proves client receipt.
export function createRequestStreamTiming({clock=process.hrtime.bigint,trace=false,maxTraceEvents=128,maxDurationMs=86400000}={}){
  if(typeof clock!=='function'||typeof trace!=='boolean'||!Number.isInteger(maxTraceEvents)||maxTraceEvents<1||maxTraceEvents>2048||!Number.isSafeInteger(maxDurationMs)||maxDurationMs<1||maxDurationMs>7*86400000)throw TypeError('stream_timing_configuration');
  const marks={},preDispatchMarks={},samples=[],pending=new Map(),reasons=new Set();let origin,lastNow,closed=false,final,ordinal=0,writes=0,completedWrites=0,contentWrites=0,completedContentWrites=0,retiredContentChunks=0,upstreamEvents=0,contentChunks=0,contentBytes=0,wireBytes=0,rawChunks=0,rawBytes=0,decodedEvents=0,backpressuredWrites=0,failedWrites=0,maxGap=0n,lastContent,maxWrite=0n,totalWrite=0n,maxRelay=0n,traceTruncated=false;
  function now(){try{const value=clock();if(typeof value!=='bigint'||value<0n||lastNow!==undefined&&value<lastNow){reasons.add('clock_invalid');return null;}lastNow=value;origin??=value;if(value-origin>BigInt(maxDurationMs)*1000000n)reasons.add('duration_budget');return value-origin;}catch{reasons.add('clock_unavailable');return null;}}
  function sample(type,at,details={}){if(!trace||at===null)return;if(samples.length>=maxTraceEvents){traceTruncated=true;return;}samples.push(Object.freeze({type,atMs:ms(at),...details}));}
  function mark(name){if(closed)return;const at=now();if(at===null)return;marks[name]??=at;sample(name,at);}
  function preDispatch(name){if(closed||!preDispatchTimingStages.includes(name)||Object.hasOwn(preDispatchMarks,name)||marks.dispatch!==undefined)return;const at=now();if(at!==null)preDispatchMarks[name]=at;}
  mark('started');
  function upstreamEvent(event){
    if(closed)return null;const at=now();if(at===null)return null;marks.firstUpstreamEvent??=at;upstreamEvents++;
    if(event?.type==='response.completed'&&contentChunks)return null;
    let identity;try{identity=classifyStageContent(event,{hashContent:false});}catch{reasons.add('content_classification');return null;}
    // The terminal output repeats earlier deltas. It is still an arrival event,
    // but only supplies body bytes when no content delta was observed.
    if(!identity)return null;if(identity.incomplete){reasons.add('content_budget');return null;}
    if(identity.contentKind==='terminal'&&contentChunks)return null;
    const sequence=++ordinal;contentChunks++;contentBytes+=identity.bytes;marks.firstUpstreamContent??=at;marks.lastUpstreamContent=at;
    if(lastContent!==undefined)maxGap=maxGap>at-lastContent?maxGap:at-lastContent;lastContent=at;
    const token=Object.freeze({});pending.set(token,{at,sequence,bytes:identity.bytes,content:true});
    // Only the current relay needs a retained timestamp. Callers that do not
    // relay an event must retire it; a bounded fallback remains non-disruptive.
    if(pending.size>64){pending.delete(pending.keys().next().value);reasons.add('inflight_budget');}
    sample('upstream-content',at,{sequence,bytes:identity.bytes});return token;
  }
  function writeStart({bytes,contentToken}={}){
    if(closed)return null;if(!Number.isSafeInteger(bytes)||bytes<0){reasons.add('write_bytes_invalid');return null;}const at=now();if(at===null)return null;
    const candidate=contentToken&&pending.get(contentToken),content=candidate?.content===true?candidate:null;if(contentToken&&!content)reasons.add(candidate?'content_pair_invalid':'content_pair_missing');if(content)pending.delete(contentToken);
    if(pending.size>=64){reasons.add('inflight_budget');return null;}
    const token=Object.freeze({});pending.set(token,{at,sequence:content?.sequence??null,bytes,content:false});writes++;wireBytes+=bytes;marks.firstWriteStart??=at;
    if(content){contentWrites++;marks.firstContentWriteStart??=at;const relay=at-content.at;maxRelay=maxRelay>relay?maxRelay:relay;}
    sample('write-start',at,{sequence:content?.sequence??null,bytes});return token;
  }
  function writeEnd(token,{ok=true,backpressured=false}={}){
    if(closed||token===null||token===undefined)return;const entry=pending.get(token);if(!entry||entry.content){reasons.add('write_pair_invalid');return;}pending.delete(token);const at=now();if(at===null)return;
    const elapsed=at-entry.at;maxWrite=maxWrite>elapsed?maxWrite:elapsed;totalWrite+=elapsed;if(backpressured)backpressuredWrites++;if(!ok){failedWrites++;reasons.add('write_failed');}else{completedWrites++;if(entry.sequence!==null)completedContentWrites++;marks.firstWriteComplete??=at;marks.lastWriteComplete=at;if(entry.sequence!==null)marks.firstContentWriteComplete??=at;}
    sample('write-complete',at,{sequence:entry.sequence,bytes:entry.bytes,ok:ok===true,backpressured:backpressured===true});
  }
  function transportEnd({outcome='unknown'}={}){mark('transportEnd');if(!outcomes.has(outcome))reasons.add('outcome_invalid');}
  function finish({outcome='unknown'}={}){
    if(closed)return final;const end=now();if(end!==null)marks.finished=end;if(!outcomes.has(outcome)){outcome='unknown';reasons.add('outcome_invalid');}
    if(contentChunks&&outcome==='ok'&&marks.firstContentWriteComplete===undefined)reasons.add('missing_content_write');
    if(marks.dispatch!==undefined&&marks.bodyEnd!==undefined&&marks.dispatch<marks.bodyEnd)reasons.add('stage_order');
    for(const name of ['started','bodyEnd','dispatch','headers','firstRawChunk','firstDecodedEvent','transportEnd','responseEnd'])if(marks[name]===undefined)reasons.add('missing_'+name);
    if(pending.size)reasons.add('unpaired_events');if(outcome==='unknown')reasons.add('outcome_unknown');closed=true;
    const offsets=Object.fromEntries(Object.entries(marks).map(([name,value])=>[name,ms(value)]));
    const span=(a,b)=>{if(marks[a]===undefined||marks[b]===undefined)return null;if(marks[b]<marks[a]){reasons.add('stage_order');return null;}return ms(marks[b]-marks[a]);};
    const durations={bodyRead:span('started','bodyEnd'),beforeDispatch:span('bodyEnd','dispatch'),upstreamHeaders:span('dispatch','headers'),upstreamFirstRawChunk:span('dispatch','firstRawChunk'),firstRawToDecoded:span('firstRawChunk','firstDecodedEvent'),upstreamFirstEvent:span('dispatch','firstUpstreamEvent'),upstreamFirstContent:span('dispatch','firstUpstreamContent'),firstContentRelay:span('firstUpstreamContent','firstContentWriteStart'),firstContentWrite:span('firstContentWriteStart','firstContentWriteComplete'),upstream:span('dispatch','transportEnd'),response:span('started','responseEnd'),total:span('started','finished'),maxContentGap:ms(maxGap),maxRelay:ms(maxRelay),maxWriteWait:ms(maxWrite),totalWriteWait:ms(totalWrite)};
    final=Object.freeze({version:1,clock:'process-monotonic',upstreamBoundary:'decoded-response-event',rawReadBoundary:'fetch-response-body-chunk',writeBoundary:'node-socket-write-callback',clientReceiptVerified:false,outcome,complete:reasons.size===0,
      offsetsMs:Object.freeze(offsets),durationsMs:Object.freeze(durations),...(Object.keys(preDispatchMarks).length?{preDispatchOffsetsMs:Object.freeze(Object.fromEntries(preDispatchTimingStages.filter(name=>Object.hasOwn(preDispatchMarks,name)).map(name=>[name,ms(preDispatchMarks[name])]))) }:{}),
      counts:Object.freeze({rawChunks:marks.firstRawChunk===undefined?null:rawChunks,rawBytes:marks.firstRawChunk===undefined?null:rawBytes,decodedEvents:marks.firstDecodedEvent===undefined?null:decodedEvents,upstreamEvents,contentChunks,contentBytes,writes,completedWrites,contentWrites,completedContentWrites,retiredContentChunks,wireBytes,backpressuredWrites,failedWrites}),missing:Object.freeze([...reasons].sort()),...(trace?{trace:Object.freeze(samples),traceTruncated}: {})});pending.clear();return final;
  }
  return Object.freeze({preDispatch,headers:()=>mark('headers'),rawChunk:bytes=>{if(closed)return;if(!Number.isSafeInteger(bytes)||bytes<0){reasons.add('raw_bytes_invalid');return;}mark('firstRawChunk');rawChunks++;rawBytes+=bytes;},decodedEvent:()=>{if(closed)return;mark('firstDecodedEvent');decodedEvents++;},responseEnd:()=>mark('responseEnd'),bodyEnd:()=>mark('bodyEnd'),dispatch:()=>mark('dispatch'),upstreamEvent,writeStart,writeEnd,transportEnd,finish,retireContent:token=>{const entry=pending.get(token);if(!entry)return false;if(!entry.content){reasons.add('content_pair_invalid');return false;}reasons.add('content_not_relayed');retiredContentChunks++;return pending.delete(token);}});
}
