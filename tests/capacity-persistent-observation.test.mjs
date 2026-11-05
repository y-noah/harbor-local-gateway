import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createCapacityLedgerController} from '../tools/capacity/ledger-adapter.mjs';
import {createCapacityObservationAdapter} from '../tools/capacity/observation-adapter.mjs';
import {requestCapacityTarget,credentialFingerprints} from '../tools/capacity/http-client.mjs';
import {capacityWireSignature} from '../tools/capacity/gateway-probe.mjs';
const store={get:async()=>({value:JSON.stringify({active:'synthetic'})}),pricingBook:async()=>({rates:{},aliases:[]}),all:async()=>[]};
async function fixture(operation,{maxDiskBytes=8*1024*1024,maxActiveRequests=10000,unavailable=false,registrationUnavailable=false,hold=null,wireIdFor=()=>randomUUID()}={}){
 const directory=await mkdtemp(join(tmpdir(),'harbor-persistent-observation-'));let registry,adapter,server;
 try{
  registry=await createCapacityLedgerController({store:unavailable?{...store,get:async()=>{throw new Error('private storage error');}}:store,recordingKey:Buffer.alloc(32),directory,maxDiskBytes});
  const emit=event=>{registry.recordEvent(event);adapter.observeWire(event);};server=http.createServer((request,response)=>{void(async()=>{const trace=request.headers['x-capacity-trace'],wireId=wireIdFor(),signature=capacityWireSignature(request.method,request.url);emit({kind:'gateway-request-start',trace,wireId,signature,credentials:credentialFingerprints(request.headers)});request.resume();await hold?.();response.writeHead(200,{'content-type':'application/json'});response.end('{"ok":true}');response.once('finish',()=>emit({kind:'gateway-response-end',trace,wireId,signature,status:200}));})().catch(()=>response.destroy());});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port,send=()=>requestCapacityTarget({origin,path:'/health',headers:{cookie:'harbor_session=synthetic-private'},timeoutMs:2000});
  adapter=createCapacityObservationAdapter({sendModel:send,sendAdmin:send,createLedger:async({id})=>{const view=registry.createScope('obs:'+id);return registrationUnavailable?Object.freeze({...view,registerTrace:()=>false}):view;},traceRegistry:registry,maxActiveRequests,maxIssuedTraces:2,maxWireEvents:4,maxWireIdentities:2,settleTimeoutMs:10});
  await operation({registry,adapter,origin,emit});
 }finally{await adapter?.close();server?.closeAllConnections();if(server)await new Promise(resolve=>server.close(resolve));await registry?.close();await rm(directory,{recursive:true,force:true});}
}
test('persistent trace and wire history outlive old RAM ceilings while diagnostic memory stays bounded',async()=>fixture(async({adapter,registry})=>{
 const scope=await adapter.createObservationScope({id:'long-history',kind:'normal'});let first;for(let index=0;index<40;index++){const response=await scope.sendAdmin({actorIndex:0,category:'health'});first??=response.trace;assert.equal(response.success,true);}await scope.close();const report=await scope.collect();assert.equal(report.transport.passed,true,JSON.stringify(report.transport));assert.equal(report.transport.counts.reconciled,40);assert.equal(report.transport.registry.memoryTraceEntries,0);assert.equal(report.transport.registry.memoryWireEntries,0);assert.equal(report.transport.wireIdentityHistory.persistent,true);assert.equal(registry.readTrace(first).wireSettled,true);const ring=scope.readWireEvents();assert.equal(ring.events.length,4);assert.equal(ring.dropped,76);assert.equal((await scope.collect()).transport.passed,true);
}));
test('a missing persistent initializer changes evidence to unknown while real requests still succeed',async()=>fixture(async({adapter})=>{
 const scope=await adapter.createObservationScope({id:'unavailable',kind:'normal'});for(let index=0;index<3;index++)assert.equal((await scope.sendModel({actorIndex:0})).success,true);await scope.close();const report=await scope.collect();assert.equal(report.transport.passed,null);assert.equal(report.dataReconciliation.passed,null);assert.equal(report.dataReconciliation.counts,null);assert.equal(report.transport.counts.reconciled,3);
},{unavailable:true}));
test('a live metadata ceiling does not reject an authorized request or retain an unbounded fallback',async()=>{
 let release,both;let received=0;const gate=new Promise(resolve=>release=resolve),arrived=new Promise(resolve=>both=resolve);await fixture(async({adapter})=>{const scope=await adapter.createObservationScope({id:'live-ceiling',kind:'normal'}),requests=[scope.sendAdmin({actorIndex:0}),scope.sendAdmin({actorIndex:0})];await Promise.race([arrived,new Promise((_,reject)=>setTimeout(()=>reject(new Error('both actual HTTP requests must arrive')),1500).unref())]);release();assert.ok((await Promise.all(requests)).every(result=>result.success));await scope.close();const report=await scope.collect();assert.equal(report.transport.passed,null);assert.equal(report.transport.registry.untrackedRequests,1);assert.equal(report.transport.pending,0);assert.equal(report.transport.registry.memoryTraceEntries,0);},{maxActiveRequests:1,hold:()=>{if(++received===2)both();return gate;}});
});
test('persistent duplicate identities remain a confirmed failure after storage disappears',async()=>{const wire=randomUUID();await fixture(async({adapter,registry})=>{const scope=await adapter.createObservationScope({id:'identity-clash',kind:'normal'});assert.equal((await scope.sendAdmin({actorIndex:0})).success,true);assert.equal((await scope.sendAdmin({actorIndex:0})).success,true);assert.equal((await scope.collect()).transport.passed,false);await registry.close();assert.equal((await scope.sendAdmin({actorIndex:0})).success,true);const report=await scope.collect();assert.equal(report.transport.passed,false);assert.ok(report.transport.registry.health.confirmedConflicts>0);},{wireIdFor:()=>wire});});
test('private auxiliary scopes use explicit sources and cannot be confused with the normal mix',async()=>fixture(async({adapter,registry,origin})=>{
 await assert.rejects(adapter.createObservationScope({id:'leaked',kind:'auxiliary'}),/scope_invalid/);const normal=await adapter.createObservationScope({id:'normal',kind:'normal'}),auxiliary=await adapter.createAuxiliaryScope({id:'bootstrap'});await assert.rejects(async()=>adapter.runAuxiliary(normal,()=>{}),/auxiliary_scope_not_owned/);const result=await adapter.runAuxiliary(auxiliary,()=>requestCapacityTarget({origin,path:'/health',timeoutMs:2000}));assert.equal(result.success,true);assert.equal((await auxiliary.collect()).transport.passed,true);assert.equal((await normal.collect()).transport.counts.planned,0);assert.equal(registry.readTrace(result.trace).source,'auxiliary');
}));

test('known active result ownership remains strict when persistent reads disappear',async()=>fixture(async({adapter,registry})=>{
 const a=await adapter.createObservationScope({id:'active-first',kind:'normal'}),b=await adapter.createObservationScope({id:'active-second',kind:'normal'}),requestId=randomUUID();const headers=await a.observer.beforeRequest({requestId,source:'baseline',actorIndex:0,method:'GET',route:'/health',credentials:{cookie:null,authorization:null}});await registry.close();const row={scenario:'normal',kind:'admin',event:{actorIndex:0},result:{operation:'health',method:'GET',trace:headers['x-capacity-trace'],validatedSuccess:true}};
 await assert.rejects(a.onResult({...row,event:{actorIndex:1}}),/result_unbound/);await assert.rejects(b.onResult(row),/result_unbound/);await a.onResult(row);assert.equal((await a.collect()).transport.counts.adminResults,1);assert.equal((await b.collect()).transport.counts.adminResults,0);
}));

test('registration write failure followed by readable unregistered evidence remains unknown rather than a claim conflict',async()=>fixture(async({adapter,registry})=>{
 const scope=await adapter.createObservationScope({id:'registration-recovered',kind:'normal'}),result=await scope.sendAdmin({actorIndex:0});assert.equal(result.success,true);assert.equal(registry.readTrace(result.trace).registered,0);await scope.onResult({scenario:'normal',kind:'admin',event:{actorIndex:0},result:{trace:result.trace,status:result.status,success:result.success,operation:'health',method:'GET',validatedSuccess:true}});assert.equal(registry.health().confirmedConflicts,0);assert.equal((await scope.collect()).transport.passed,null);
},{registrationUnavailable:true}));
