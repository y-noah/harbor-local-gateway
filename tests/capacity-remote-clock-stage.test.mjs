import test from 'node:test';
import assert from 'node:assert/strict';
import https from 'node:https';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,X509Certificate} from 'node:crypto';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createCapacityRemoteLauncher} from '../tools/capacity/remote-launcher.mjs';
import {connectCapacityRemoteTransport} from '../tools/capacity/remote-transport.mjs';
import {remoteHash} from '../tools/capacity/remote-protocol.mjs';
import {createCapacityRequestDriver,capacityRequestDriverAgent,capacityRequestDriverRemoteSourceBinding} from '../tools/capacity/request-driver.mjs';
import {requestCapacityTarget,createCapacityClientStageObserver} from '../tools/capacity/http-client.mjs';
import {capacityRemoteStageReceiptBinding,capacityRemoteStageSourceBinding} from '../tools/capacity/remote-stage-source.mjs';

const until=async read=>{const deadline=performance.now()+5000;for(;;){const value=read();if(value)return value;assert.ok(performance.now()<deadline,'remote clock stage condition timeout');await new Promise(resolve=>setTimeout(resolve,5));}};
async function fixture(t,options={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-remote-clock-stage-')),cert=await syntheticCertificates(directory),runtime=createOwnedRuntime(),sockets=new Set(),wireTraces=[];let launcher,transport,driver;
 const tlsConfig=side=>({caFile:join(directory,'ca.pem'),certFile:join(directory,side+'.pem'),keyFile:join(directory,side+'.key'),peerCertificateHash:remoteHash(new X509Certificate(cert[side==='server'?'client':'server'].cert).raw)});
 const server=https.createServer(cert.server,(req,res)=>{req.resume();req.once('end',()=>{wireTraces.push(req.headers['x-capacity-trace']);res.writeHead(200,{'content-type':'text/event-stream'});let index=0;const timer=setInterval(()=>{if(index++<12)res.write('data: '+JSON.stringify({type:'response.output_text.delta',delta:'synthetic content '+index})+'\n\n');else{clearInterval(timer);res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic',status:'completed'}})+'\n\n');}},30);res.once('close',()=>clearInterval(timer));});});server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
 t.after(async()=>{try{await driver?.close();}finally{await launcher?.close();for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));await runtime.close();await rm(directory,{recursive:true,force:true});}});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='https://127.0.0.1:'+server.address().port;
 launcher=await createCapacityRemoteLauncher({runtime,tls:tlsConfig('server'),agents:[{id:'model',origin,tls:{caFile:join(directory,'ca.pem')},routes:[{method:'POST',path:'/model'}]}]});transport=await connectCapacityRemoteTransport({origin:launcher.origin,runId:launcher.runId,tls:tlsConfig('client')});driver=await createCapacityRequestDriver({remote:{transport},agentPlans:[{id:'model',origin}],...options});return {driver,transport,origin,wireTraces,agent:capacityRequestDriverAgent(driver,'model')};
}

test('real remote stream seals each authenticated event during the request without converting raw remote timestamps to local time',{timeout:30000},async t=>{
 const f=await fixture(t),binding=capacityRequestDriverRemoteSourceBinding(f.driver),events=[],trace='clock-stage-'+randomUUID();assert.ok(binding);assert.equal(capacityRequestDriverRemoteSourceBinding({...f.driver}),null);assert.equal(capacityRemoteStageSourceBinding({...binding.stageSource}),null);
 const observer=createCapacityClientStageObserver({remoteSource:binding.stageSource,context:({trace})=>({trace}),observe:event=>{const row=capacityRemoteStageReceiptBinding(event.receipt);assert.ok(row);assert.equal(row.source,binding.stageSource);assert.equal(row.trace,trace);assert.equal(row.session,binding.session);assert.equal(row.clockEpoch,binding.clockEpoch);assert.ok(BigInt(row.range.lowerNs)<=BigInt(event.atNs)&&BigInt(row.range.upperNs)>=BigInt(event.atNs),'same-kernel diagnostic brackets only');assert.equal(row.range.formalQualified,false);assert.equal(capacityRemoteStageReceiptBinding({...event.receipt,...row}),null);events.push({event,row});}});t.after(()=>observer.close());
 const pending=requestCapacityTarget({origin:f.origin,path:'/model',method:'POST',mode:'sse',headers:{'x-capacity-trace':trace},agent:f.agent,clientStageObserver:observer});await until(()=>events.length>2);assert.equal(f.driver.status().pending,1);assert.ok(f.driver.status().clock.samples>2);assert.ok(f.driver.status().remoteStages.activeRequests>0);assert.ok(events[0].row.range.known);
 const result=await pending;assert.equal(result.success,true);assert.equal(result.startedAt,null);await until(()=>f.driver.status().pendingCallbacks===0);assert.equal(events.filter(row=>row.event.type==='content').length,12);assert.equal(events.at(-1).event.type,'end');assert.equal(observer.status().failures,0);assert.equal(f.driver.status().remoteStages.failures,0);assert.equal(f.driver.status().remoteStages.activeRequests,0);assert.equal(JSON.stringify(events.map(row=>row.event)).includes('synthetic content'),false);
 await f.driver.close();assert.equal(capacityRemoteStageReceiptBinding(events[0].event.receipt),null);assert.equal(capacityRequestDriverRemoteSourceBinding(f.driver),null);
});

test('mapped remote callbacks remain under the whole-driver lease until an actual slow sink settles',{timeout:30000},async t=>{
 const f=await fixture(t,{maxInFlight:1});let release,entered=0,retainedBytes=0;const held=new Promise(resolve=>{release=resolve;}),binding=capacityRequestDriverRemoteSourceBinding(f.driver),observer=createCapacityClientStageObserver({remoteSource:binding.stageSource,context:({trace})=>({trace}),observe:event=>{entered++;retainedBytes+=Buffer.byteLength(JSON.stringify(event));return held;}});t.after(()=>observer.close());
 try{const result=await requestCapacityTarget({origin:f.origin,path:'/model',method:'POST',mode:'sse',headers:{'x-capacity-trace':'held-'+randomUUID()},agent:f.agent,clientStageObserver:observer});assert.equal(result.success,true);assert.equal(entered,1);assert.equal(f.driver.status().pendingCallbacks,1);assert.ok(f.driver.status().callbackBytes>=retainedBytes);assert.ok(f.driver.status().callbackBudgetRejected>0);release();await until(()=>f.driver.status().pendingCallbacks===0);assert.equal(f.driver.status().callbackBytes,0);}finally{release();}
});

test('a real cancelled remote stream publishes its authenticated cancel receipt without inventing HTTP completion',{timeout:30000},async t=>{
 const f=await fixture(t),controller=new AbortController(),events=[],binding=capacityRequestDriverRemoteSourceBinding(f.driver),observer=createCapacityClientStageObserver({remoteSource:binding.stageSource,context:({trace})=>({trace}),observe:event=>{events.push(event);if(event.type==='content')controller.abort();}});t.after(()=>observer.close());
 const result=await requestCapacityTarget({origin:f.origin,path:'/model',method:'POST',mode:'sse',headers:{'x-capacity-trace':'cancel-'+randomUUID()},agent:f.agent,signal:controller.signal,clientStageObserver:observer});assert.equal(result.success,false);await until(()=>f.driver.status().pendingCallbacks===0);assert.ok(events.some(event=>event.type==='cancel'&&capacityRemoteStageReceiptBinding(event.receipt)));assert.equal(events.some(event=>event.type==='end'),false);
});


test('mapped sink timeout returns unknown on time and retains its driver lease until actual settlement',{timeout:30000},async t=>{
 const f=await fixture(t,{maxInFlight:1,closeTimeoutMs:100});let release;const held=new Promise(resolve=>{release=resolve;}),binding=capacityRequestDriverRemoteSourceBinding(f.driver),observer=createCapacityClientStageObserver({remoteSource:binding.stageSource,context:({trace})=>({trace}),observe:()=>held});t.after(()=>observer.close());try{
  await requestCapacityTarget({origin:f.origin,path:'/model',method:'POST',mode:'sse',headers:{'x-capacity-trace':'timeout-'+randomUUID()},agent:f.agent,clientStageObserver:observer});const started=performance.now();await assert.rejects(f.driver.close(),/callback_cleanup_unknown/);assert.ok(performance.now()-started<1500);assert.equal(f.driver.status().pendingCallbacks,1);assert.equal(f.driver.status().remoteStages.pending,1);assert.ok(f.driver.status().callbackBytes>0);release();await until(()=>f.driver.status().pendingCallbacks===0);await f.driver.close();assert.equal(f.driver.status().resourcesReleased,true);assert.equal(f.driver.status().remoteStages.pending,0);
 }finally{release();}
});

test('canonical trace headers bind selector, wire and actual remote receipt even when the selector mutates original options',{timeout:30000},async t=>{
 const f=await fixture(t),binding=capacityRequestDriverRemoteSourceBinding(f.driver);for(const kind of ['uppercase','lower-then-upper','upper-then-lower']){const trace='headers-'+randomUUID(),wrong='wrong-'+randomUUID(),headers=kind==='uppercase'?{'X-Capacity-Trace':trace}:kind==='lower-then-upper'?{'x-capacity-trace':wrong,'X-Capacity-Trace':trace}:{'X-Capacity-Trace':wrong,'x-capacity-trace':trace},events=[];const observer=createCapacityClientStageObserver({remoteSource:binding.stageSource,context:context=>{assert.equal(context.trace,trace);headers['x-capacity-trace']='mutated-'+randomUUID();headers['X-Capacity-Trace']='mutated-again';return context;},observe:event=>{assert.equal(event.trace,trace);assert.equal(capacityRemoteStageReceiptBinding(event.receipt).trace,trace);events.push(event);}});try{const result=await requestCapacityTarget({origin:f.origin,path:'/model',method:'POST',mode:'sse',headers,agent:f.agent,clientStageObserver:observer});assert.equal(result.success,true);await until(()=>f.driver.status().pendingCallbacks===0);assert.equal(f.wireTraces.at(-1),trace);assert.equal(result.trace,trace);assert.ok(events.length>0);assert.equal(observer.status().failures,0);}finally{observer.close();}}
});
