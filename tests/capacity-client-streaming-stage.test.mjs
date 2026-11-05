import {createCapacityObservationAdapter} from '../tools/capacity/observation-adapter.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import https from 'node:https';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createCapacityClientStageObserver,createSseProbe,requestCapacityTarget,capacityAgent} from '../tools/capacity/http-client.mjs';
import {classifyStageContent,streamingStageClock} from '../streaming-stage-observer.mjs';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
const delta={type:'response.output_text.delta',item_id:'synthetic-client-item',output_index:0,delta:'SYNTHETIC_PRIVATE_中文内容',capacity_sent_wall_ms:999999999999999};
const complete={type:'response.completed',response:{id:'synthetic-client-completed',status:'completed',output:[{type:'message',content:[{type:'output_text',text:'SYNTHETIC_PRIVATE_TERMINAL'}]}]}};
const frame=value=>'data: '+JSON.stringify(value)+'\r\n\r\n';
const until=async predicate=>{const end=performance.now()+5000;while(!predicate()){assert.ok(performance.now()<end,'client stage condition timed out');await new Promise(resolve=>setTimeout(resolve,5));}};
const deferred=()=>{let resolve;const promise=new Promise(done=>resolve=done);return {promise,resolve};};
const observer=(trace,events,extra={})=>createCapacityClientStageObserver({context:({trace:candidate})=>candidate===undefined||candidate===trace?{trace}:null,observe:event=>events.push(event),...extra});
async function serverFixture(t,{tls=false}={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-client-stage-')),held=new Map(),sockets=new Set();let received=0;const traces=[];const certificates=tls?await syntheticCertificates(directory):null;
 const handle=(req,res)=>{received++;traces.push(req.headers['x-capacity-trace']??null);req.resume();if(req.url==='/bad-csv'){res.end('name,detail\nsynthetic,\"unfinished');return;}if(req.url==='/json'){res.end('{"ok":true}');return;}if(req.url==='/http429'){res.writeHead(429,{'content-type':'application/json'});res.end('{"error":{"message":"SYNTHETIC_PRIVATE_ERROR"}}');return;}res.writeHead(200,{'content-type':'text/event-stream'});res.write(': heartbeat\r\n\r\n');
  if(req.url==='/hold'||req.url==='/timeout'||req.url==='/cancel'){if(req.url==='/hold')res.write(frame(complete));if(req.url==='/cancel')res.write(frame(delta));const hold=deferred();held.set(req.url,hold);res.once('close',hold.resolve);void hold.promise.then(()=>{if(!res.destroyed)res.end();});return;}
  if(req.url==='/invalid'){res.end('data: {invalid}\n\n');return;}if(req.url==='/incomplete'){res.end();return;}
  if(req.url==='/cut'){res.write(frame(delta),()=>setImmediate(()=>res.destroy()));return;}
  if(req.url==='/terminal'){res.end(frame(complete));return;}
  const bytes=Buffer.from(frame(delta));res.write(bytes.subarray(0,bytes.indexOf(Buffer.from('中文'))+1));setImmediate(()=>res.end(Buffer.concat([bytes.subarray(bytes.indexOf(Buffer.from('中文'))+1),Buffer.from(frame({...delta,delta:'second piece'})+frame(complete))])));
 };
 const server=tls?https.createServer({...certificates.server,ca:certificates.ca,requestCert:true,rejectUnauthorized:true},handle):http.createServer(handle);server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin=(tls?'https':'http')+'://127.0.0.1:'+server.address().port,agent=capacityAgent(origin,tls?{ca:certificates.ca,...certificates.client}:{});t.after(async()=>{for(const hold of held.values())hold.resolve();agent.destroy();for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));await rm(directory,{recursive:true,force:true});});return {origin,agent,held,traces,received:()=>received};
}

test('byte-split and multi-frame SSE uses the shared classifier, ignores heartbeats and never turns probe.finish into HTTP end',()=>{
 const events=[],cap=observer('direct-probe',events),probe=createSseProbe({clientStageObserver:cap,clock:()=>1});probe.write(Buffer.from(': heartbeat\r\n\r\n'));assert.equal(events.length,0);const bytes=Buffer.from(frame(delta)+frame({...delta,type:'response.function_call_arguments.delta',delta:'{"synthetic":true}'})+frame(complete));for(const byte of bytes)probe.write(Buffer.from([byte]));const result=probe.finish();assert.equal(result.completed,true);assert.equal(events.length,3);assert.deepEqual(events.map(event=>event.ordinal),[0,1,2]);assert.deepEqual(events.map(event=>event.contentKind),['text','arguments','terminal']);assert.equal(events[0].hash,classifyStageContent(delta).hash);assert.ok(events.every(event=>event.bound==='upper'&&event.clock.kernelId===streamingStageClock().kernelId&&Object.isFrozen(event)));assert.equal(events.some(event=>event.type==='end'),false);assert.equal(JSON.stringify(events).includes('SYNTHETIC_PRIVATE'),false);assert.equal(JSON.stringify(events).includes('999999999999999'),false);assert.notEqual(events[0].atNs,'1');assert.equal(cap.source.bound,'upper');assert.ok(Object.isFrozen(cap.source));assert.throws(()=>createCapacityClientStageObserver({context:()=>({trace:'bad'}),observe:()=>{},bound:'exact'}),/configuration/);
});

test('real HTTP and mutual TLS observe decoded full content and actual response end while preserving result DTOs',async t=>{
 for(const tls of [false,true]){const f=await serverFixture(t,{tls}),trace='real-client-'+tls,events=[],cap=observer(trace,events),send=clientStageObserver=>requestCapacityTarget({origin:f.origin,path:'/stream',agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',clientStageObserver});const plain=await send(),result=await send(cap);assert.equal(result.success,true);assert.equal(result.completedId,complete.response.id);assert.deepEqual(Object.keys(result).sort(),Object.keys(plain).sort());assert.equal(result.bodyHash,plain.bodyHash);assert.equal(events.filter(event=>event.type==='content').length,3);assert.equal(events.filter(event=>event.type==='end').length,1);assert.equal(events.at(-1).outcome,'ok');assert.ok(BigInt(events.at(-1).atNs)>=BigInt(events[0].atNs));assert.equal(cap.status().failures,0);assert.equal(cap.status().unavailable,0);assert.equal(f.received(),2);}
});

test('terminal-only content is observed before the socket ends, and end waits for actual HTTP completion',async t=>{
 const f=await serverFixture(t),events=[],trace='held-terminal',cap=observer(trace,events),running=requestCapacityTarget({origin:f.origin,path:'/hold',agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',clientStageObserver:cap});await until(()=>events.some(event=>event.type==='content'));assert.equal(events.filter(event=>event.type==='end').length,0);assert.equal(events[0].contentKind,'terminal');f.held.get('/hold').resolve();assert.equal((await running).success,true);assert.equal(events.filter(event=>event.type==='end').length,1);assert.equal(events.at(-1).outcome,'ok');
});

test('explicit cancellation, a deadline and remote failure remain different facts without manufactured HTTP end',async t=>{
 for(const tls of [false,true]){const f=await serverFixture(t,{tls});
 for(const [path,kind] of [['/cancel','cancel'],['/timeout','timeout'],['/cut','failed']]){const events=[],trace='client-'+kind,cap=observer(trace,events),abort=new AbortController(),running=requestCapacityTarget({origin:f.origin,path,agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',signal:abort.signal,timeoutMs:kind==='timeout'?80:3000,clientStageObserver:cap});if(kind==='cancel'){await until(()=>events.some(event=>event.type==='content'));abort.abort();}const result=await running;assert.equal(result.success,false);assert.equal(events.filter(event=>event.type===kind).length,1);assert.equal(events.filter(event=>event.type==='end').length,0);assert.equal(events.some(event=>event.type===(kind==='cancel'?'timeout':'cancel')),false);assert.equal(events.some(event=>event.type==='failed'),kind==='failed');}}
});

test('normal HTTP rejection and incomplete or invalid SSE keep real end versus transport failure distinct',async t=>{
 const f=await serverFixture(t);for(const [path,endOutcome] of [['/http429','rejected'],['/incomplete','failed'],['/invalid',null]]){const events=[],trace='outcome-'+path.slice(1),cap=observer(trace,events),result=await requestCapacityTarget({origin:f.origin,path,agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',clientStageObserver:cap});assert.equal(result.success,false);assert.equal(events.some(event=>event.type==='cancel'||event.type==='timeout'),false);if(endOutcome){assert.equal(events.filter(event=>event.type==='end').length,1);assert.equal(events.at(-1).outcome,endOutcome);}else{assert.equal(events.some(event=>event.type==='failed'),true);assert.equal(events.some(event=>event.type==='end'),false);}assert.equal(JSON.stringify(events).includes('SYNTHETIC_PRIVATE_ERROR'),false);}
});

test('private observer failures, asynchronous rejection and invalid selection cannot change outcomes or repeat requests',async t=>{
 const f=await serverFixture(t);for(const [index,observe] of [()=>{throw Error('SYNTHETIC_PRIVATE_CALLBACK');},()=>Promise.reject(Error('SYNTHETIC_PRIVATE_CALLBACK')),()=>false].entries()){const trace='observer-failure-'+index,cap=observer(trace,[],{observe}),before=f.received(),result=await requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',clientStageObserver:cap});await new Promise(setImmediate);assert.equal(result.success,true);assert.equal(f.received(),before+1);assert.ok(cap.status().failures>0);assert.equal(JSON.stringify(cap.status()).includes('SYNTHETIC_PRIVATE'),false);}
 const events=[],mismatch=observer('private-selected',events);assert.equal((await requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers:{'x-capacity-trace':'other-scope'},mode:'sse',clientStageObserver:mismatch})).success,true);assert.equal(events.length,0);assert.equal(mismatch.status().selected,0);const bad=createCapacityClientStageObserver({context:()=>({trace:'wrong-scope'}),observe:event=>events.push(event)});assert.equal((await requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers:{'x-capacity-trace':'actual-scope'},mode:'sse',clientStageObserver:bad})).success,true);assert.equal(bad.status().unavailable,1);assert.equal(events.length,0);assert.throws(()=>requestCapacityTarget({origin:f.origin,path:'/terminal',clientStageObserver:{source:{role:'client',bound:'exact'}}}),/capability/);
});

test('pre-aborted signals report missing cancel time, and closing the capability never interrupts an otherwise valid request',async t=>{
 const f=await serverFixture(t),events=[],trace='already-aborted',cap=observer(trace,events),abort=new AbortController();abort.abort();const result=await requestCapacityTarget({origin:f.origin,path:'/stream',agent:f.agent,headers:{'x-capacity-trace':trace},signal:abort.signal,mode:'sse',clientStageObserver:cap});assert.equal(result.success,false);assert.equal(events.some(event=>event.type==='cancel'),false);assert.ok(events.some(event=>event.type==='unavailable'&&event.reason==='cancel_before_observation'));assert.ok(cap.status().unavailable>0);assert.equal(cap.source.isOpen(),true);cap.close();assert.equal(cap.source.isOpen(),false);const before=events.length;assert.equal((await requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',clientStageObserver:cap})).success,true);assert.equal(events.length,before);
});

test('classifier resource limits retain unavailable evidence and never leak or truncate content into an observation',()=>{
 const events=[],cap=observer('large-content',events),probe=createSseProbe({clientStageObserver:cap,maxEventBytes:2*1048576});probe.write(Buffer.from(frame({...delta,delta:'x'.repeat(1048577)})+frame(complete)));assert.equal(probe.finish().completed,true);assert.equal(events[0].type,'unavailable');assert.equal(events[0].reason,'content_budget');assert.ok(cap.status().unavailable>0);assert.equal(events.filter(event=>event.type==='content').length,1);assert.equal(events.find(event=>event.type==='content').contentKind,'terminal');
});

test('canonical immutable trace headers bind observation and DTO to the actual wire even with case aliases and selector mutation',async t=>{
 const f=await serverFixture(t);for(const headers of [{'x-capacity-trace':'scope-a','X-Capacity-Trace':'scope-b'},{'X-Capacity-Trace':'scope-a','x-capacity-trace':'scope-b'}]){const events=[],cap=createCapacityClientStageObserver({context:({trace})=>{assert.equal(trace,'scope-b');headers['x-capacity-trace']='changed-after-capture';return {trace};},observe:event=>events.push(event)}),result=await requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers,mode:'sse',clientStageObserver:cap});assert.equal(result.success,true);assert.equal(result.trace,'scope-b');assert.equal(f.traces.at(-1),'scope-b');assert.ok(events.every(event=>event.trace==='scope-b'));assert.equal(cap.status().unavailable,0);}
});

test('client stage selection sees the private observer-rebound final trace instead of stale case aliases',async t=>{
 const f=await serverFixture(t),events=[],trace='private-observer-rebound',cap=observer(trace,events),adapter=createCapacityObservationAdapter({sendModel:async()=>{},sendAdmin:()=>requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers:{'X-Capacity-Trace':'stale-original'},mode:'sse',clientStageObserver:cap}),observer:{beforeRequest:()=>({'x-capacity-trace':trace}),response:()=>{},requestFinished:()=>{},requestFailed:()=>{}},createLedger:async()=>({collect:async()=>({passed:null}),onResult:async()=>{}})});try{const scope=await adapter.createObservationScope({id:'rebound-client',kind:'normal'}),result=await scope.sendAdmin({actorIndex:0});assert.equal(result.success,true);assert.equal(result.trace,trace);assert.equal(f.traces.at(-1),trace);assert.ok(events.length>0);assert.ok(events.every(event=>event.trace===trace));}finally{await adapter.close();}
});

test('CSV final validation emits one failed actual end and never publishes an earlier successful end',async t=>{
 const f=await serverFixture(t),events=[],trace='incomplete-csv',cap=observer(trace,events),result=await requestCapacityTarget({origin:f.origin,path:'/bad-csv',agent:f.agent,headers:{'x-capacity-trace':trace},mode:'download',rowFormat:'csv',clientStageObserver:cap});assert.equal(result.success,false);assert.equal(result.error,'capacity_export_csv_incomplete');assert.deepEqual(events.map(event=>[event.type,event.outcome]),[['end','failed']]);
});

test('synchronous header rejection closes observation before a later deadline and retains the original rejection',async()=>{
 const events=[],trace='invalid-header',cap=observer(trace,events);await assert.rejects(requestCapacityTarget({origin:'http://127.0.0.1:1',path:'/',headers:{'x-capacity-trace':trace,'x-synthetic-invalid':'value\ninvalid'},timeoutMs:20,clientStageObserver:cap}),error=>error.code==='ERR_INVALID_CHAR');await new Promise(resolve=>setTimeout(resolve,50));assert.deepEqual(events.map(event=>[event.type,event.reason]),[['failed','request_not_sent']]);
});

test('a slow failing observer cannot move the original firstContentAt sampling of the current frame',()=>{
 let now=1;const cap=createCapacityClientStageObserver({context:()=>({trace:'slow-observer'}),observe:()=>{now=100;throw Error('synthetic-observer-delay');}}),probe=createSseProbe({clock:()=>now,clientStageObserver:cap});probe.write(Buffer.from(frame(delta)+frame(complete)));assert.equal(probe.finish().firstContentAt,1);assert.ok(cap.status().failures>0);
});

test('pending asynchronous callbacks remain visible until settlement so late rejection cannot appear delivered',async t=>{
 const f=await serverFixture(t),trace='late-observer-rejection';let reject;const pending=new Promise((_resolve,failed)=>reject=failed),cap=observer(trace,[],{observe:()=>pending}),result=await requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',clientStageObserver:cap});assert.equal(result.success,true);assert.ok(cap.status().pendingCallbacks>0);assert.equal(cap.status().failures,0);reject(Error('SYNTHETIC_PRIVATE_LATE_FAILURE'));await new Promise(setImmediate);assert.equal(cap.status().pendingCallbacks,0);assert.ok(cap.status().failures>0);assert.equal(f.received(),1);
});

test('end observer execution follows the original DTO completion timing snapshot',async t=>{
 const f=await serverFixture(t),trace='end-clock-snapshot';let now=1;const cap=observer(trace,[],{observe:event=>{if(event.type==='end'){now=100;throw Error('synthetic-end-observer-delay');}}}),result=await requestCapacityTarget({origin:f.origin,path:'/json',agent:f.agent,headers:{'x-capacity-trace':trace},clock:()=>now,clientStageObserver:cap});assert.equal(result.success,true);assert.equal(result.finishedAt,1);assert.equal(result.durationMs,0);assert.equal(cap.status().failures,1);
});

test('asynchronous observer acceptance and explicit refusal remain different after callback settlement',async t=>{
 const f=await serverFixture(t);for(const accepted of [true,false]){const events=[],trace='async-acceptance-'+accepted,cap=observer(trace,events,{observe:async event=>{events.push(event);return accepted;}}),before=f.received(),result=await requestCapacityTarget({origin:f.origin,path:'/terminal',agent:f.agent,headers:{'x-capacity-trace':trace},mode:'sse',clientStageObserver:cap});await new Promise(setImmediate);assert.equal(result.success,true);assert.equal(f.received(),before+1);assert.equal(cap.status().pendingCallbacks,0);assert.equal(cap.status().failures,accepted?0:events.length);assert.equal(events.length,2);}
});
