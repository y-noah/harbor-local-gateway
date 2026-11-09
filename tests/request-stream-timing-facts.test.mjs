import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequestStreamTiming} from '../request-stream-timing.mjs';
import {canonicalRequestStreamTiming} from '../request-stream-timing-facts.mjs';
function sample(){const timing=createRequestStreamTiming();timing.bodyEnd();timing.dispatch();timing.headers();timing.rawChunk(10);timing.decodedEvent();const content=timing.upstreamEvent({type:'response.output_text.delta',delta:'synthetic'}),write=timing.writeStart({bytes:10,contentToken:content});timing.writeEnd(write);timing.transportEnd();timing.responseEnd();return timing.finish({outcome:'ok'});}
test('canonical timing facts survive JSON/spool roundtrip with fixed units and no mutable source references',()=>{
 const original=JSON.parse(JSON.stringify(sample())),value=canonicalRequestStreamTiming(original);assert.deepEqual(value,original);assert.ok(Object.isFrozen(value)&&Object.isFrozen(value.counts)&&Object.isFrozen(value.missing));original.counts.wireBytes=999;assert.notEqual(value.counts.wireBytes,999);assert.deepEqual(canonicalRequestStreamTiming(JSON.parse(JSON.stringify(value))),value);
 const incomplete=createRequestStreamTiming().finish({outcome:'unknown'});assert.equal(canonicalRequestStreamTiming(incomplete).complete,false);assert.equal(incomplete.counts.rawBytes,null);
});
test('summary rejects content, identifiers, trace, getters, unknown reasons and invalid numeric facts without echoing them',()=>{
 const base=sample();for(const value of [{...base,prompt:'PRIVATE_BODY'},{...base,trace:[]},{...base,accountId:'PRIVATE_ACCOUNT'},{...base,counts:{...base.counts,password:123}},{...base,missing:['PRIVATE_REASON'],complete:false},{...base,complete:false},{...base,offsetsMs:{}},{...base,offsetsMs:{...base.offsetsMs,firstRawChunk:NaN}},{...base,durationsMs:{...base.durationsMs,total:-1}},{...base,durationsMs:{...base.durationsMs,total:Infinity}},{...base,counts:{...base.counts,writes:2**53}},{...base,counts:{...base.counts,rawChunks:null}},{...base,counts:{...base.counts,backpressuredWrites:base.counts.writes+1}},{...base,clientReceiptVerified:true}])assert.throws(()=>canonicalRequestStreamTiming(value),{message:'request_stream_timing_invalid'});
 let invoked=false;const getter={...base};Object.defineProperty(getter,'outcome',{enumerable:true,get(){invoked=true;throw Error('PRIVATE_GETTER');}});assert.throws(()=>canonicalRequestStreamTiming(getter),{message:'request_stream_timing_invalid'});assert.equal(invoked,false);const symbol={...base,[Symbol('PRIVATE')]:1};assert.throws(()=>canonicalRequestStreamTiming(symbol));
});

test('reason arrays reject accessors and arbitrary properties before reading entries',()=>{const base=sample(),missing=[];let read=false;Object.defineProperty(missing,'0',{enumerable:true,get(){read=true;return 'capture_failed';}});assert.throws(()=>canonicalRequestStreamTiming({...base,complete:false,missing}));assert.equal(read,false);const withExtra=Object.assign([],{private:'PRIVATE'});assert.throws(()=>canonicalRequestStreamTiming({...base,missing:withExtra}));});

test('optional metering canonicalizer drops invalid facts without changing settlement control flow',async()=>{const {canonicalStreamTiming}=await import('../request-stream-timing-facts.mjs');assert.equal(canonicalStreamTiming(undefined),undefined);assert.equal(canonicalStreamTiming({prompt:'PRIVATE'}),undefined);assert.equal(canonicalStreamTiming({...sample(),trace:[]}),undefined);const value=sample();assert.deepEqual(canonicalStreamTiming(JSON.parse(JSON.stringify(value))),canonicalRequestStreamTiming(value));});

test('durations, observed counts and completeness reasons must agree across JSON recovery',()=>{
 const value=sample(),invalid=[{...value,counts:{...value.counts,rawChunks:null,rawBytes:null,decodedEvents:null}},{...value,offsetsMs:{...value.offsetsMs,headers:value.offsetsMs.dispatch-0.001}},{...value,durationsMs:{...value.durationsMs,upstreamHeaders:value.durationsMs.upstreamHeaders+1}},{...value,offsetsMs:{...value.offsetsMs},missing:['missing_dispatch'],complete:false}];
 for(const row of invalid){assert.throws(()=>canonicalRequestStreamTiming(row));}
 let now=0n;const reversed=createRequestStreamTiming({clock:()=>now});reversed.bodyEnd();reversed.headers();now+=1000000n;reversed.dispatch();reversed.transportEnd();reversed.responseEnd();const facts=reversed.finish({outcome:'ok'});assert.ok(facts.missing.includes('stage_order'));assert.deepEqual(canonicalRequestStreamTiming(JSON.parse(JSON.stringify(facts))),facts);
});

test('all real content and non-content write completions must balance before recovery accepts complete',async()=>{
 const {canonicalStreamTiming}=await import('../request-stream-timing-facts.mjs');
 for(const completeFirst of [false,true]){const timing=createRequestStreamTiming();timing.bodyEnd();timing.dispatch();timing.headers();timing.rawChunk(5);timing.decodedEvent();if(completeFirst){const done=timing.writeStart({bytes:10});timing.writeEnd(done);}timing.writeStart({bytes:5});timing.transportEnd();timing.responseEnd();const original=timing.finish({outcome:'ok'});assert.deepEqual(canonicalRequestStreamTiming(JSON.parse(JSON.stringify(original))),original);assert.ok(original.missing.includes('unpaired_events'));const forged={...original,complete:true,missing:[]};assert.throws(()=>canonicalRequestStreamTiming(forged));assert.equal(canonicalStreamTiming(forged),undefined);}
});
