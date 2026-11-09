import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {createRequestStreamTiming} from '../request-stream-timing.mjs';
import {writeOutput,createOutputBudget} from '../output-budget.mjs';
const delta=text=>({type:'response.output_text.delta',delta:text,item_id:'synthetic'});
function fixture(options={}){let now=0n;return {advance:ms=>{now+=BigInt(ms)*1000000n;},timing:createRequestStreamTiming({clock:()=>now,...options})};}
test('monotonic request spans isolate upstream, relay and socket-write wait without keeping content',()=>{
 const {timing,advance}=fixture();advance(3);timing.bodyEnd();advance(7);timing.dispatch();advance(20);timing.headers();timing.rawChunk(200);timing.decodedEvent();const token=timing.upstreamEvent(delta('SYNTHETIC_PRIVATE'));advance(2);const write=timing.writeStart({bytes:200,contentToken:token});advance(5);timing.writeEnd(write,{ok:true,backpressured:true});advance(10);timing.transportEnd({outcome:'ok'});timing.responseEnd();const result=timing.finish({outcome:'ok'});
 assert.equal(result.complete,true);assert.equal(result.durationsMs.bodyRead,3);assert.equal(result.durationsMs.beforeDispatch,7);assert.equal(result.durationsMs.upstreamFirstContent,20);assert.equal(result.durationsMs.firstContentRelay,2);assert.equal(result.durationsMs.firstContentWrite,5);assert.equal(result.durationsMs.maxWriteWait,5);assert.equal(result.counts.backpressuredWrites,1);assert.equal(result.counts.contentBytes,17);assert.equal(result.clientReceiptVerified,false);assert.equal(Object.hasOwn(result,'trace'),false);assert.equal(JSON.stringify(result).includes('SYNTHETIC'),false);assert.equal(timing.finish({outcome:'failed'}),result);
});
test('trace and inflight metadata are bounded and missing observations never become false zeros',()=>{
 const {timing,advance}=fixture({trace:true,maxTraceEvents:4});timing.bodyEnd();timing.dispatch();for(let i=0;i<100;i++){advance(1);timing.upstreamEvent(delta('private'));}timing.transportEnd({outcome:'cancelled'});const result=timing.finish({outcome:'cancelled'});assert.equal(result.complete,false);assert.ok(result.missing.includes('inflight_budget'));assert.ok(result.missing.includes('unpaired_events'));assert.equal(result.trace.length,4);assert.equal(result.traceTruncated,true);assert.equal(result.durationsMs.firstContentWrite,null);assert.equal(JSON.stringify(result).includes('private'),false);
 const empty=fixture().timing.finish({outcome:'ok'});assert.equal(empty.complete,false);assert.equal(empty.durationsMs.upstreamFirstContent,null);assert.ok(empty.missing.includes('missing_dispatch'));
});
test('custom input bytes are counted and terminal duplicates do not double count deltas',()=>{
 const {timing}=fixture();timing.bodyEnd();timing.dispatch();const token=timing.upstreamEvent({type:'response.custom_tool_call_input.delta',delta:'echo synthetic',item_id:'tool'});assert.ok(token);const write=timing.writeStart({bytes:100,contentToken:token});timing.writeEnd(write);assert.equal(timing.upstreamEvent({type:'response.completed',response:{status:'completed',output:[{type:'custom_tool_call',name:'exec',input:'echo synthetic'}]}}),null);timing.transportEnd({outcome:'ok'});const result=timing.finish({outcome:'ok'});assert.equal(result.counts.contentChunks,1);assert.equal(result.counts.contentBytes,14);
});
test('invalid clocks, classification budgets and duration budgets mark missing facts without throwing',()=>{
 let clock=0n;const timing=createRequestStreamTiming({clock:()=>clock,maxDurationMs:1});clock=2000000n;timing.bodyEnd();clock=1n;timing.dispatch();assert.doesNotThrow(()=>timing.upstreamEvent(delta('x'.repeat(1048577))));const result=timing.finish({outcome:'unknown'});assert.equal(result.complete,false);assert.ok(result.missing.includes('clock_invalid'));assert.ok(result.missing.includes('duration_budget'));assert.ok(result.missing.includes('outcome_unknown'));
});
class Response extends EventEmitter{constructor(mode='ok'){super();this.mode=mode;this.headersSent=false;this.destroyed=false;}writeHead(){this.headersSent=true;}write(_wire,callback){if(this.mode==='cancel'){queueMicrotask(()=>{this.destroyed=true;this.emit('close');});}else setTimeout(()=>callback(),15);return false;}destroy(){this.destroyed=true;this.closed=true;this.emit('close');}}
test('actual output-budget callbacks report completion, backpressure and cancellation without changing bytes or release',async()=>{
 for(const mode of ['ok','cancel']){const timing=createRequestStreamTiming(),budget=createOutputBudget(),output=budget.open('synthetic'),res=new Response(mode);timing.bodyEnd();timing.dispatch();const contentToken=timing.upstreamEvent(delta('synthetic'));
 try{const operation=writeOutput(res,{synthetic:true},{output,timing,contentToken});if(mode==='cancel')await assert.rejects(operation,{code:'request_cancelled'});else await operation;timing.transportEnd({outcome:mode==='ok'?'ok':'cancelled'});const result=timing.finish({outcome:mode==='ok'?'ok':'cancelled'});assert.equal(result.counts.writes,1);assert.equal(result.counts.backpressuredWrites,1);assert.equal(result.counts.failedWrites,mode==='ok'?0:1);if(mode==='ok')assert.ok(result.durationsMs.maxWriteWait>=10);assert.equal(budget.status().bufferedBytes,0);}finally{output.close();}}
 for(const asynchronous of [false,true]){const res=new Response();const fail=()=>{if(asynchronous)return Promise.reject(Error('observer'));throw Error('observer');};await writeOutput(res,{synthetic:true},{timing:{writeStart:fail,writeEnd:fail}});await new Promise(setImmediate);}
});

test('normal timing classification skips hashes while preserving the exact default observer classification',async()=>{
 const {classifyStageContent}=await import('../streaming-stage-observer.mjs');
 for(const event of [delta('same text'),{type:'response.custom_tool_call_input.delta',delta:'echo synthetic'},{type:'response.completed',response:{status:'completed',output:[{type:'custom_tool_call',name:'exec',input:'echo synthetic'}]}}]){
  const full=classifyStageContent(event),light=classifyStageContent(event,{hashContent:false});assert.match(full.hash,/^[a-f0-9]{64}$/);const {hash,...facts}=full;assert.deepEqual(light,facts);assert.equal(Object.hasOwn(light,'hash'),false);
 }
 const {timing}=fixture();timing.bodyEnd();timing.dispatch();const token=timing.upstreamEvent(delta('delta'));timing.retireContent(token);
 const terminal={type:'response.completed',get response(){assert.fail('an already counted delta must not trigger scanning the repeated terminal output');}};assert.equal(timing.upstreamEvent(terminal),null);timing.transportEnd();assert.equal(timing.finish({outcome:'ok'}).complete,false,'unwritten content cannot be reported complete');
});


test('out-of-order stage endpoints are unknown rather than negative or complete',()=>{
 for(const early of ['headers','transportEnd','upstreamEvent']){const {timing,advance}=fixture();timing.bodyEnd();advance(1);if(early==='upstreamEvent')timing.upstreamEvent({type:'response.created'});else timing[early]();advance(1);timing.dispatch();if(early!=='transportEnd')timing.transportEnd();const result=timing.finish({outcome:'ok'});assert.equal(result.complete,false);assert.ok(result.missing.includes('stage_order'));assert.ok(Object.values(result.durationsMs).every(value=>value===null||value>=0));}
});
test('write tokens cannot masquerade as content or retire incomplete writes',()=>{
 const {timing}=fixture();timing.bodyEnd();timing.dispatch();const content=timing.upstreamEvent(delta('x')),first=timing.writeStart({bytes:10,contentToken:content});assert.equal(timing.retireContent(first),false);const second=timing.writeStart({bytes:10,contentToken:first});timing.writeEnd(second);timing.transportEnd();const result=timing.finish({outcome:'ok'});assert.equal(result.complete,false);assert.ok(result.missing.includes('content_pair_invalid'));assert.ok(result.missing.includes('unpaired_events'));
});

test('unobserved raw and decode boundaries remain missing with null counts',()=>{const {timing}=fixture();timing.bodyEnd();timing.dispatch();timing.transportEnd();timing.responseEnd();const summary=timing.finish({outcome:'ok'});assert.equal(summary.complete,false);assert.equal(summary.counts.rawChunks,null);assert.equal(summary.counts.rawBytes,null);assert.equal(summary.counts.decodedEvents,null);for(const name of ['headers','firstRawChunk','firstDecodedEvent'])assert.ok(summary.missing.includes('missing_'+name));assert.equal(summary.durationsMs.response,0);});

test('finish before actual response end is incomplete and cannot invent the missing endpoint',()=>{const {timing}=fixture();timing.bodyEnd();timing.dispatch();timing.headers();timing.rawChunk(10);timing.decodedEvent();timing.transportEnd();const result=timing.finish({outcome:'ok'});assert.equal(result.complete,false);assert.ok(result.missing.includes('missing_responseEnd'));assert.equal(result.durationsMs.response,null);timing.responseEnd();assert.equal(timing.finish({outcome:'ok'}),result);});
