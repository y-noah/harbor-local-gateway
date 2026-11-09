import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {createHash} from 'node:crypto';
import {createControlledUpstream,createControlledResponsesTransport} from '../tools/capacity/controlled-upstream.mjs';
import {createRequestStreamTiming} from '../request-stream-timing.mjs';
import {writeOutput,createOutputBudget} from '../output-budget.mjs';
const pause=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function readFrames(response,onContent=()=>{}){
 const decoder=new TextDecoder();let buffer='',text='',chunks=0,terminalAt=null,firstContentAt=null;
 for await(const chunk of response.body){buffer+=decoder.decode(chunk,{stream:true});let end;while((end=buffer.indexOf('\n\n'))>=0){const frame=buffer.slice(0,end);buffer=buffer.slice(end+2);const data=frame.split('\n').filter(line=>line.startsWith('data:')).map(line=>line.slice(5).trimStart()).join('\n');if(!data)continue;const event=JSON.parse(data);if(event.type==='response.output_text.delta'){firstContentAt??=performance.now();chunks++;text+=event.delta;await onContent(event);}if(event.type==='response.completed')terminalAt=performance.now();}}
 return {text,chunks,terminalAt,firstContentAt,hash:createHash('sha256').update(text).digest('hex')};
}
test('fixed-rate upstream replay measures actual read/decode/socket writes, preserves streaming and propagates cancellation',{timeout:15000},async()=>{
 let upstream,server;const source=[],summaries=[],budget=createOutputBudget();
 try{
  upstream=await createControlledUpstream({durationMs:300,firstContentMs:20,chunkIntervalMs:30,sink:event=>source.push(event)});
  const transport=createControlledResponsesTransport({origin:upstream.origin});
  server=http.createServer({highWaterMark:1},(req,res)=>{void(async()=>{
   const timing=createRequestStreamTiming({trace:true,maxTraceEvents:64}),output=budget.open('synthetic'),abort=new AbortController();res.once('close',()=>{if(!res.writableFinished)abort.abort();});
   const responseEnded=new Promise(resolve=>{const ended=()=>{res.off('finish',ended);res.off('close',ended);timing.responseEnd();resolve();};res.once('finish',ended);res.once('close',ended);});
   for await(const _chunk of req){}timing.bodyEnd();let outcome='unknown';
   try{const result=await transport.run({binary:'unused',home:'synthetic',payload:{model:'gpt-6.1-sol',stream:true,input:'synthetic fixed-rate comparison',metadata:{capacity_trace:'paired-replay'}},signal:abort.signal,output,timing,onDispatch:()=>timing.dispatch(),onEvent:async event=>{const contentToken=timing.upstreamEvent(event);await writeOutput(res,event,{output,signal:abort.signal,timing,contentToken,prefix:'data: ',suffix:'\n\n',headers:{'content-type':'text/event-stream','x-accel-buffering':'no'}});}});
    const contentToken=result.timingTerminalObserved?result.timingTerminalToken:timing.upstreamEvent(result.terminal);await writeOutput(res,result.terminal,{output,signal:abort.signal,timing,contentToken,prefix:'data: ',suffix:'\n\n'});res.end();outcome='ok';
   }catch{outcome=abort.signal.aborted?'cancelled':'failed';res.destroy();}
   finally{await responseEnded;summaries.push(timing.finish({outcome}));output.close();}
  })().catch(()=>res.destroy());});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  for(let pair=0;pair<2;pair++){
   const direct=await readFrames(await fetch(upstream.origin+'/responses',{method:'POST',headers:{'content-type':'application/json','x-capacity-trace':'paired-replay'},body:'{}'}));
   const relay=await readFrames(await fetch(origin,{method:'POST',body:'{}'}));
   for(const sample of [direct,relay]){assert.ok(sample.chunks>=3);assert.ok(sample.firstContentAt<sample.terminalAt,'content must arrive before the terminal, not be buffered until completion');const record=source.findLast(event=>event.kind==='upstream-complete'&&event.outputHash===sample.hash);assert.ok(record,'received content must exactly match the actual upstream hash');assert.equal(Buffer.byteLength(sample.text),record.outputBytes);}
   const summary=summaries.at(-1);assert.equal(summary.complete,true);assert.ok(summary.offsetsMs.headers>=summary.offsetsMs.dispatch);assert.ok(summary.counts.rawChunks>=3);assert.ok(summary.counts.decodedEvents>summary.counts.contentChunks);assert.equal(summary.counts.contentChunks,relay.chunks);assert.ok(summary.counts.backpressuredWrites>0,'actual ServerResponse high-water-mark backpressure must be observed');assert.equal(summary.counts.contentBytes,Buffer.byteLength(relay.text));assert.ok(summary.offsetsMs.firstContentWriteComplete<summary.offsetsMs.transportEnd);assert.equal(summary.clientReceiptVerified,false);
  }
  const before=summaries.length,abort=new AbortController(),response=await fetch(origin,{method:'POST',body:'{}',signal:abort.signal});
  await assert.rejects(readFrames(response,()=>abort.abort()));
  const deadline=performance.now()+3000;while(summaries.length===before||upstream.status().active){assert.ok(performance.now()<deadline);await pause(5);}
  assert.equal(summaries.at(-1).outcome,'cancelled');assert.equal(budget.status().bufferedBytes,0);assert.equal(budget.status().scopes,0);assert.ok(source.some(event=>event.kind==='upstream-cancel'));
  assert.equal(JSON.stringify(summaries).includes('容量合成'),false);assert.equal(JSON.stringify(summaries).includes('synthetic fixed-rate'),false);
 }finally{if(server){server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}await upstream?.close();}
});

test('raw chunks and decoded frames are separate facts and timing observer failures never alter protocol delivery',async()=>{
 const {createResponsesTransport}=await import('../responses.mjs');
 const event={type:'response.output_text.delta',item_id:'synthetic',delta:'synthetic'},terminal={type:'response.completed',response:{status:'completed',model:'gpt-6.1-sol',output:[],usage:{input_tokens:1,output_tokens:1}}},wire='data: '+JSON.stringify(event)+'\n\n'+'data: '+JSON.stringify(terminal)+'\n\n';
 for(const throwing of [false,true]){
  const timing=createRequestStreamTiming(),seen=[];timing.bodyEnd();
  const observer=throwing?Object.fromEntries(['headers','rawChunk','decodedEvent','transportEnd'].map(name=>[name,()=>Promise.reject(Error('synthetic observer'))])):timing;
  const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),fetchImpl:async()=>new Response(new ReadableStream({start(controller){controller.enqueue(Buffer.from(wire.slice(0,13)));controller.enqueue(Buffer.from(wire.slice(13)));controller.close();}}),{headers:{'content-type':'text/event-stream'}})});
  const response=await transport.run({binary:'unused',home:'synthetic',payload:{model:'gpt-6.1-sol'},timing:observer,onDispatch:()=>timing.dispatch(),onEvent:value=>seen.push(value)});assert.equal(response.usageKnown,true);assert.equal(seen.length,1);
  if(!throwing){const summary=timing.finish({outcome:'ok'});assert.equal(summary.counts.rawChunks,2);assert.equal(summary.counts.rawBytes,Buffer.byteLength(wire));assert.equal(summary.counts.decodedEvents,2);assert.ok(summary.offsetsMs.firstDecodedEvent>=summary.offsetsMs.firstRawChunk);}
  await new Promise(setImmediate);
 }
});

test('terminal-only content is timestamped at decode before transport-end and paired with the later write',async()=>{
 const {createResponsesTransport}=await import('../responses.mjs'),timing=createRequestStreamTiming(),res=new (await import('node:events')).EventEmitter();res.headersSent=false;res.destroyed=false;res.writeHead=()=>{res.headersSent=true;};res.write=(_wire,callback)=>{callback();return true;};
 const terminal={type:'response.completed',response:{status:'completed',output:[{type:'custom_tool_call',name:'exec',input:'echo synthetic'}],usage:{input_tokens:1,output_tokens:1}}};
 const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),fetchImpl:async()=>new Response('data: '+JSON.stringify(terminal)+'\n\n',{headers:{'content-type':'text/event-stream'}})});timing.bodyEnd();
 const result=await transport.run({binary:'unused',home:'synthetic',payload:{model:'gpt-6.1-sol'},timing,onDispatch:()=>timing.dispatch()});assert.equal(result.timingTerminalObserved,true);assert.ok(result.timingTerminalToken);
 await writeOutput(res,result.terminal,{timing,contentToken:result.timingTerminalToken});timing.responseEnd();const summary=timing.finish({outcome:'ok'});assert.equal(summary.complete,true);assert.equal(summary.counts.contentChunks,1);assert.ok(summary.offsetsMs.firstUpstreamContent<=summary.offsetsMs.transportEnd);assert.ok(summary.offsetsMs.firstContentWriteStart>=summary.offsetsMs.transportEnd);
});
