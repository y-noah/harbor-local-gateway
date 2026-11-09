import test from 'node:test';
import assert from 'node:assert/strict';
import {createResponsesTransport} from '../responses.mjs';
import {AppError} from '../store.mjs';
import {createOutputBudget} from '../output-budget.mjs';
import {createRequestStreamTiming} from '../request-stream-timing.mjs';
const delta={type:'response.output_text.delta',item_id:'synthetic-item',delta:'synthetic output'};
const terminal={type:'response.completed',response:{status:'completed',model:'gpt-6.1-sol',output:[],usage:{input_tokens:101,output_tokens:23,input_tokens_details:{cached_tokens:17}}}};
const frame=value=>'data: '+JSON.stringify(value)+'\n\n';
async function scenario({tail=frame(terminal),next,configuration={},abort=false,pressure=false,protocolError=false,rawSuffix=Buffer.alloc(0)}={}){
 const budget=createOutputBudget({maxBufferedBytes:65536}),output=budget.open('synthetic'),other=budget.open('pressure'),controller=new AbortController(),timing=createRequestStreamTiming(),events=[];
 let calls=0,pulls=0,cancels=0,onEvents=0,readBytes=0;
 const chunks=[Buffer.concat([Buffer.from((protocolError?'data: {invalid}\n\n':frame(delta))+tail),rawSuffix]),...(next?[Buffer.from(next)]:[])];
 const transport=createResponsesTransport({...configuration,credentials:async()=>({token:'synthetic',accountId:'synthetic'}),stageObserver:{context:()=>({trace:'trusted-buffer-usage'}),observe:event=>events.push(event)},fetchImpl:async()=>{calls++;return new Response(new ReadableStream({pull(reader){pulls++;if(chunks.length){const chunk=chunks.shift();readBytes+=chunk.length;reader.enqueue(chunk);}else reader.close();},cancel(){cancels++;}},{highWaterMark:0}),{headers:{'content-type':'text/event-stream'}});}});
 timing.bodyEnd();let error;
 try{await transport.run({binary:'unused',home:'synthetic',payload:{model:'gpt-6.1-sol'},signal:controller.signal,output,timing,onDispatch:()=>timing.dispatch(),onEvent:async()=>{onEvents++;if(pressure)other.resize('pressure',65536-budget.status().bufferedBytes);if(abort)controller.abort();throw new AppError(499,'synthetic downstream close','request_cancelled');}});assert.fail('downstream error must not become a successful model result');}catch(caught){error=caught;}finally{output.close();other.close();}
 assert.equal(calls,1);assert.equal(pulls,1,'must not request another body chunk after downstream failure');assert.equal(cancels,1);assert.equal(budget.status().bufferedBytes,0);assert.equal(budget.status().scopes,0);if(!protocolError)assert.equal(error.code,'request_cancelled');
 timing.responseEnd();return {error,onEvents,readBytes,events,timing:timing.finish({outcome:'cancelled'})};
}
for(const abort of [false,true])test('downstream cancellation preserves completed usage already present in the same read chunk: '+abort,async()=>{
 const result=await scenario({abort});assert.equal(result.error.status,499);assert.equal(result.error.usageKnown,true);assert.equal(result.error.input,101);assert.equal(result.error.output,23);assert.equal(result.error.cached,17);assert.equal(result.error.actualModel,'gpt-6.1-sol');assert.equal(result.onEvents,1);
 assert.equal(result.events.filter(event=>event.type==='content').length,1,'recovered billing must not masquerade as forwarded or captured terminal content');assert.equal(result.timing.counts.decodedEvents,1,'billing-only recovery does not pretend the normal stream decoder delivered the terminal');assert.equal(result.timing.counts.rawChunks,1);
});
test('a terminal in an unread following chunk and a trailing incomplete frame remain unknown',async()=>{
 for(const setup of [{tail:'',next:frame(terminal)},{tail:frame(terminal).slice(0,-2)},{tail:frame(terminal)+'data: {"type":'},{tail:frame(terminal)+'data: [DONE]'},{rawSuffix:Buffer.from([0xe4])}]){const {error}=await scenario(setup);assert.equal(error.usageKnown,false);assert.equal(error.input,undefined);}
});
test('duplicate, conflicting, malformed, non-completed and invalid usage frames never become guessed metering',async()=>{
 for(const tail of [frame(terminal)+frame(terminal),frame(terminal)+frame({...delta,delta:'after terminal'}),'data: {invalid}\n\n'+frame(terminal),frame({type:'response.failed',response:{usage:terminal.response.usage}})+frame(terminal),frame({...terminal,response:{...terminal.response,status:'in_progress'}}),frame({...terminal,response:{...terminal.response,usage:{input_tokens:1,output_tokens:2,input_tokens_details:{cached_tokens:2}}}}),frame({...terminal,response:{...terminal.response,usage:{input_tokens:-1,output_tokens:2}}}),frame({...terminal,response:{...terminal.response,usage:{input_tokens:1.5,output_tokens:2}}})]){const {error}=await scenario({tail});assert.equal(error.usageKnown,false);}
});
test('complete comment and DONE control frames are tolerated around the unique buffered terminal',async()=>{
 const {error}=await scenario({tail:': heartbeat\n\n'+frame(delta)+frame(terminal)+'data: [DONE]\n\n: trailing heartbeat\n\n'});assert.equal(error.usageKnown,true);assert.equal(error.input,101);
});
test('recovery obeys byte/frame/disabled settings and the existing live output reservation',async()=>{
 for(const setup of [{tail:frame({...terminal,response:{...terminal.response,synthetic:'x'.repeat(400)}}),configuration:{bufferedUsageMaxBytes:256}},{tail:': heartbeat\n\n'+frame(terminal),configuration:{bufferedUsageMaxFrames:1}},{configuration:{bufferedUsageMaxBytes:0}},{pressure:true},{tail:frame({...terminal,response:{...terminal.response,synthetic:'x'.repeat(400)}}),configuration:{maxJsonResponseBytes:256}}]){const {error}=await scenario(setup);assert.equal(error.usageKnown,false);}
});
test('a protocol parse failure never activates downstream usage recovery',async()=>{
 const {error,onEvents}=await scenario({protocolError:true});assert.equal(error.code,'upstream_protocol');assert.equal(error.usageKnown,false);assert.equal(onEvents,0);
});
test('buffered usage recovery budgets accept explicit disable and reject invalid construction values',()=>{
 assert.doesNotThrow(()=>createResponsesTransport({bufferedUsageMaxBytes:0}));
 for(const configuration of [{bufferedUsageMaxBytes:-1},{bufferedUsageMaxBytes:1.5},{bufferedUsageMaxBytes:512*1024*1024+1},{bufferedUsageMaxFrames:0},{bufferedUsageMaxFrames:1.5},{bufferedUsageMaxFrames:65537}])assert.throws(()=>createResponsesTransport(configuration),{code:'upstream_configuration'});
});
