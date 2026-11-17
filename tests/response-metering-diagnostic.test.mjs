import test from 'node:test';
import assert from 'node:assert/strict';
import {createResponsesTransport} from '../responses.mjs';
import {canonicalMeteringDiagnostic} from '../request-metering-diagnostic.mjs';
import {AppError} from '../store.mjs';
import {readOutputJson} from '../output-budget.mjs';
const usage={input_tokens:10,output_tokens:2,input_tokens_details:{cached_tokens:3}};
const frame=value=>'data: '+JSON.stringify(value)+'\n\n';
const event=(type='completed',value=usage)=>({type:'response.'+type,response:{status:type,usage:value,output:[]}});
async function run({events=[event()],fetchImpl,signal,onEvent,beforeDispatch,json=false}={}){
 const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),fetchImpl:fetchImpl??(async()=>new Response(events.map(frame).join(''),{headers:{'content-type':'text/event-stream'}}))});
 try{return await transport[json?'runJson':'run']({path:'/images/generations',binary:'unused',home:'synthetic',payload:{model:'synthetic'},signal,onEvent,beforeDispatch});}catch(error){return error;}
}
test('terminal usage diagnosis distinguishes received, missing and invalid without guessing counters',async()=>{
 for(const type of ['completed','failed','incomplete'])for(const [value,reason,known] of [[usage,'usage_received',true],[undefined,'terminal_usage_missing',false],[{...usage,input_tokens:-1},'terminal_usage_invalid',false],[{...usage,input_tokens_details:{cached_tokens:11}},'terminal_usage_invalid',false]]){
  const response=event(type);if(value===undefined)delete response.response.usage;else response.response.usage=value;
  const result=await run({events:[response]});assert.equal(result.usageKnown,known);assert.deepEqual(result.meteringDiagnostic,{version:1,reason,terminal:type,interruption:'none',usage_known:known});
 }
});
test('dispatch rejection, no dispatch and interrupted stream remain distinguishable',async()=>{
 let result=await run({beforeDispatch:async()=>{throw new AppError(403,'synthetic','denied');}});assert.equal(result.meteringDiagnostic.reason,'not_dispatched');
 result=await run({fetchImpl:async()=>new Response('{}',{status:429})});assert.equal(result.meteringDiagnostic.reason,'upstream_rejected');
 result=await run({events:[]});assert.equal(result.meteringDiagnostic.reason,'interrupted_without_terminal');assert.equal(result.meteringDiagnostic.interruption,'upstream');
});
test('client cancellation retains usage already received in buffered terminal and shutdown stays distinct',async()=>{
 for(const shutdown of [false,true])for(const terminal of [false,true]){
  const controller=new AbortController();
  const result=await run({signal:controller.signal,events:[{type:'response.output_text.delta',delta:'synthetic'},...(terminal?[event()]:[])],onEvent:async()=>{controller.abort(shutdown?new AppError(503,'synthetic','service_shutdown'):undefined);throw new AppError(499,'synthetic','request_cancelled');}});
  assert.equal(result.usageKnown,terminal);assert.equal(result.meteringDiagnostic.reason,terminal?'usage_received':'interrupted_without_terminal');assert.equal(result.meteringDiagnostic.terminal,terminal?'completed':'none');assert.equal(result.meteringDiagnostic.interruption,shutdown?'gateway_shutdown':'client_cancelled');
 }
});
test('JSON image response is explicitly JSON rather than an invented SSE terminal',async()=>{
 for(const value of [usage,undefined]){const result=await run({json:true,fetchImpl:async()=>Response.json({data:[{b64_json:'synthetic'}],usage:value})});assert.equal(result.meteringDiagnostic.terminal,'json');assert.equal(result.meteringDiagnostic.reason,value?'usage_received':'terminal_usage_missing');}
});
test('canonical diagnostic strips no unknown payload and never invokes getters',()=>{
 const valid={version:1,reason:'usage_received',terminal:'completed',interruption:'none',usage_known:true};assert.deepEqual(canonicalMeteringDiagnostic(valid),valid);
 for(const value of [undefined,null,{...valid,content:'secret'},{...valid,reason:'arbitrary text'},{...valid,usage_known:1},Object.defineProperty({...valid},'reason',{get(){throw Error('must not run');}}),new Proxy({},{ownKeys(){throw Error('untrusted');}})])assert.equal(canonicalMeteringDiagnostic(value),null);
});

test('body reader failures preserve original errors and report upstream interruption in SSE and JSON',async()=>{
 for(const json of [false,true]){
  let reads=0;const result=await run({json,fetchImpl:async()=>new Response(new ReadableStream({pull(controller){reads++;controller.error(new TypeError('synthetic connection reset'));}},{highWaterMark:0}),{headers:{'content-type':json?'application/json':'text/event-stream'}})});
  assert.equal(reads,1);assert.equal(result.code,json?'upstream_protocol':'upstream_connection');assert.equal(result.usageKnown,false);assert.deepEqual(result.meteringDiagnostic,{version:1,reason:'interrupted_without_terminal',terminal:'none',interruption:'upstream',usage_known:false});
 }
});

test('a failed read diagnostic callback cannot replace the original stream error',async()=>{
 const original=new TypeError('synthetic read failure');let calls=0;
 const response=new Response(new ReadableStream({pull(controller){controller.error(original);}},{highWaterMark:0}));
 await assert.rejects(readOutputJson(response,{maxBytes:1024,onReadFailure(){calls++;throw Error('synthetic diagnostic failure');}}),error=>error===original);
 assert.equal(calls,1);
});
