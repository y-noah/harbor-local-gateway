import test from 'node:test';
import assert from 'node:assert/strict';
import {createControlledUpstream,createControlledResponsesTransport} from '../tools/capacity/controlled-upstream.mjs';
import {classifyStageContent,createStreamingStageAssessment} from '../tools/capacity/streaming-stage-assessment.mjs';

const until=async predicate=>{const end=performance.now()+5000;while(!predicate()){assert.ok(performance.now()<end,'controlled source event timed out');await new Promise(resolve=>setTimeout(resolve,5));}};
const request=(transport,trace,options={})=>transport.run({binary:'must-not-spawn',home:'synthetic-home',payload:{model:'gpt-6.1-sol',input:'synthetic-source',stream:true,metadata:{capacity_trace:trace}},...options});

test('concurrent owned upstreams contain different synthetic identities and swapped client content is rejected',async()=>{
 const events=[],upstream=await createControlledUpstream({durationMs:80,firstContentMs:5,chunkIntervalMs:15,sink:event=>events.push(event)});
 try{
  const transport=createControlledResponsesTransport({origin:upstream.origin}),received=new Map();await Promise.all(['owned-alpha','owned-beta'].map(trace=>request(transport,trace,{onEvent:event=>{if(event.type==='response.output_text.delta'&&!received.has(trace))received.set(trace,event);}})));
  const a=received.get('owned-alpha'),b=received.get('owned-beta');assert.notEqual(a.item_id,b.item_id);assert.notEqual(classifyStageContent(a).hash,classifyStageContent(b).hash);
  const history=new Map(),clock={kind:'linux-hrtime-ns',kernelId:'owned-synthetic-kernel',clockId:'owned-clock',epoch:'synthetic-epoch'},collector=createStreamingStageAssessment({profile:'selfcheck',minSamples:1,referenceClock:clock,verifySource:({role})=>({role,clock,isOpen:()=>true}),verifyTopology:()=>({realHelper:true,tls:true,lan:true,proxyPreserved:true}),history:{lookup:trace=>history.get(trace)??'absent',begin:trace=>{history.set(trace,'active');return 'stored';},finalize:trace=>{history.set(trace,'final');return 'stored';}}}),gateway=collector.source({role:'gateway'}),client=collector.source({role:'client'});
  for(const [trace,sent,swapped] of [['owned-alpha',a,b],['owned-beta',b,a]]){collector.begin({trace});gateway.observe({trace,type:'accepted',atNs:'0',accepted:true,recordingExpected:true});gateway.observe({trace,type:'content',atNs:'1000000',ordinal:0,...classifyStageContent(sent)});client.observe({trace,type:'content',atNs:'2000000',ordinal:0,...classifyStageContent(swapped)});collector.finalize(trace);}const report=collector.collect();assert.equal(report.passed,false);assert.equal(report.issues.filter(issue=>issue.code==='content_identity_mismatch').length,2);assert.ok(events.every(event=>/^\d+$/.test(event.atNs)));
 }finally{await upstream.close();}
});

test('real 429 is known zero while 503 and a partial stream remain unknown and never impersonate a received gateway cancellation',async()=>{
 const events=[],upstream=await createControlledUpstream({durationMs:60,firstContentMs:5,chunkIntervalMs:15,sink:event=>events.push(event)}),transport=createControlledResponsesTransport({origin:upstream.origin});
 try{for(const mode of ['429','503','disconnect']){upstream.setFault(mode);const trace='source-'+mode;await assert.rejects(request(transport,trace),error=>{assert.equal(error.usageKnown,mode==='429');return true;});await until(()=>events.some(event=>event.kind==='upstream-terminal'&&event.trace===trace));const terminal=events.find(event=>event.kind==='upstream-terminal'&&event.trace===trace);assert.equal(terminal.usageKnown,mode==='429');assert.equal(terminal.inputTokens,mode==='429'?0:null);assert.equal(terminal.outputTokens,mode==='429'?0:null);assert.equal(terminal.cachedTokens,mode==='429'?0:null);assert.equal(events.some(event=>event.kind==='upstream-cancel'&&event.trace===trace),false);if(mode==='disconnect')assert.ok(terminal.outputBytes>0);}}
 finally{await upstream.close();}
});

test('the controlled upstream records one actual peer cancellation and sink rejection cannot repeat a model request',async()=>{
 const events=[],upstream=await createControlledUpstream({durationMs:1000,firstContentMs:5,chunkIntervalMs:20,sink:event=>{events.push(event);return Promise.reject(Error('synthetic-private-sink'));}}),transport=createControlledResponsesTransport({origin:upstream.origin,sink:()=>Promise.reject(Error('synthetic-private-transport-sink'))}),abort=new AbortController();
 try{await assert.rejects(request(transport,'peer-cancel',{signal:abort.signal,onEvent:event=>{if(event.type==='response.output_text.delta')abort.abort();}}));await until(()=>events.some(event=>event.kind==='upstream-terminal'&&event.trace==='peer-cancel'));assert.equal(events.filter(event=>event.kind==='upstream-cancel'&&event.trace==='peer-cancel').length,1);assert.equal(events.filter(event=>event.kind==='upstream-dispatch'&&event.trace==='peer-cancel').length,1);assert.equal(events.find(event=>event.kind==='upstream-terminal'&&event.trace==='peer-cancel').usageKnown,false);assert.equal(JSON.stringify(events).includes('synthetic-private-sink'),false);assert.ok(upstream.status().observerFailures>0);assert.ok(transport.sourceObserverStatus().failures>0);}
 finally{await upstream.close();}
});

test('a real payload rejection and tool shutdown cannot manufacture a received gateway cancel',async()=>{
 const rejectedEvents=[],limited=await createControlledUpstream({durationMs:60,firstContentMs:5,chunkIntervalMs:15,maxBodyBytes:16,sink:event=>rejectedEvents.push(event)});
 try{const transport=createControlledResponsesTransport({origin:limited.origin});await assert.rejects(request(transport,'oversized-owned'),error=>error.usageKnown===true);await until(()=>rejectedEvents.some(event=>event.kind==='upstream-terminal'));assert.equal(rejectedEvents.find(event=>event.kind==='upstream-terminal').status,413);assert.equal(rejectedEvents.some(event=>event.kind==='upstream-cancel'),false);}finally{await limited.close();}
 const shutdownEvents=[],upstream=await createControlledUpstream({durationMs:1000,firstContentMs:5,chunkIntervalMs:15,sink:event=>shutdownEvents.push(event)}),transport=createControlledResponsesTransport({origin:upstream.origin}),running=request(transport,'tool-shutdown');running.catch(()=>{});try{await until(()=>shutdownEvents.some(event=>event.kind==='upstream-content'));await upstream.close();await assert.rejects(running);assert.equal(shutdownEvents.some(event=>event.kind==='upstream-cancel'),false);}finally{await upstream.close();}
});
