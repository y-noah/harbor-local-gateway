import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {createStreamingStageObserver} from '../streaming-stage-observer.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {passwordHash} from '../auth.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createControlledUpstream,createControlledResponsesTransport} from '../tools/capacity/controlled-upstream.mjs';
import {startCapacityProcess} from '../tools/capacity/process-adapter.mjs';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {capacityAgent,requestCapacityTarget} from '../tools/capacity/http-client.mjs';
const tick=()=>new Promise(setImmediate);
const until=async predicate=>{const end=performance.now()+10000;while(!predicate()){assert.ok(performance.now()<end,'asynchronous source evidence timed out');await new Promise(resolve=>setTimeout(resolve,5));}};

test('source callback pending state survives close and a late explicit false cannot become clean evidence',async()=>{
 const pending=Promise.withResolvers(),events=[],observer=createStreamingStageObserver({context:()=>({trace:'source-close-late-false'}),observe:event=>{events.push(event);return pending.promise;}}),scope=observer.capture();assert.equal(scope.emit('dispatch'),true);assert.equal(observer.status().pendingCallbacks,1);assert.equal(observer.status().failures,0);observer.close();assert.equal(observer.status().closed,true);assert.equal(scope.emit('transport-end'),false);assert.equal(events.length,1);assert.equal(observer.status().pendingCallbacks,1);pending.resolve(false);await tick();assert.equal(observer.status().pendingCallbacks,0);assert.equal(observer.status().failures,2);assert.equal(observer.status().closed,true);
});

test('asynchronous accepted, refused and rejected callbacks settle their own counters without unhandled rejection',async()=>{
 for(const outcome of ['accepted','refused','rejected']){const pending=Promise.withResolvers(),observer=createStreamingStageObserver({context:()=>({trace:'source-'+outcome}),observe:()=>pending.promise});observer.capture().emit('dispatch');assert.equal(observer.status().pendingCallbacks,1);if(outcome==='rejected')pending.reject(Error('SYNTHETIC_PRIVATE_REJECTION'));else pending.resolve(outcome==='accepted');await tick();assert.equal(observer.status().pendingCallbacks,0);assert.equal(observer.status().failures,outcome==='accepted'?0:1);assert.equal(JSON.stringify(observer.status()).includes('SYNTHETIC_PRIVATE'),false);}
});

test('an unsupported asynchronous context remains failed while its later rejection is still tracked after close',async()=>{
 const pending=Promise.withResolvers(),observer=createStreamingStageObserver({context:()=>pending.promise,observe:()=>assert.fail('no selected synchronous context')});assert.equal(observer.capture().emit('dispatch'),false);assert.equal(observer.status().failures,1);assert.equal(observer.status().pendingCallbacks,1);observer.close();pending.reject(Error('SYNTHETIC_PRIVATE_CONTEXT'));await tick();assert.equal(observer.status().pendingCallbacks,0);assert.equal(observer.status().failures,2);assert.equal(observer.status().closed,true);
 const disabled=createStreamingStageObserver();disabled.capture().emit('dispatch');disabled.close();assert.deepEqual(disabled.status(),{enabled:false,failures:0,pendingCallbacks:0,closed:true});
});

test('a real controlled model finishes once while source callbacks remain pending and later refuse delivery',async()=>{
 const pending=Promise.withResolvers(),upstream=await createControlledUpstream({durationMs:70,firstContentMs:5,chunkIntervalMs:15}),transport=createControlledResponsesTransport({origin:upstream.origin,stageObserver:{context:()=>({trace:'real-source-async'}),observe:()=>pending.promise}});
 try{const result=await transport.run({binary:'must-not-launch',home:'synthetic',payload:{model:'gpt-6.1-sol',input:'synthetic source pending',metadata:{capacity_trace:'real-source-async'}}});assert.equal(result.input,1000);assert.equal(result.usageKnown,true);assert.equal(upstream.status().received,1);const count=transport.stageObserverStatus().pendingCallbacks;assert.ok(count>=4);assert.equal(transport.stageObserverStatus().failures,0);pending.resolve(false);await tick();assert.equal(transport.stageObserverStatus().pendingCallbacks,0);assert.equal(transport.stageObserverStatus().failures,count);assert.equal(upstream.status().received,1);}finally{pending.resolve(true);await upstream.close();}
});

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('owned parent IPC pending callbacks survive actual child shutdown and retain late acceptance, false and rejection',{skip:!binDirectory?'需本任务私有PG18及实际owned网关IPC':false,timeout:120000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-async-parent-'));let lab,store,upstream,gateway,agent;const holds=[];
 try{lab=await createPrivatePostgres({binDirectory,libraryDirectory});store=new PostgresStore({connection:lab.connection});await store.migrate();await store.initializePricing();const data=join(directory,'data'),runtime=join(directory,'runtime');await mkdir(data,{mode:0o700});await mkdir(runtime,{mode:0o700});await writeFile(join(data,'key-encryption.key'),randomBytes(32),{mode:0o600});await writeFile(join(data,'recording-encryption.key'),randomBytes(32),{mode:0o600});const certificates=await syntheticCertificates(directory),location=await store.get("SELECT current_setting('data_directory') directory,current_setting('unix_socket_directories') socket,current_setting('port')::int port");upstream=await createControlledUpstream({durationMs:50,firstContentMs:5,chunkIntervalMs:10});const configuration={database:{connection:lab.connection},storageDatabase:{connection:{...lab.connection,host:location.socket,port:location.port}},pgDataDir:location.directory,dataDir:data,runtimeDir:runtime,certificateDirectory:directory,upstreamOrigin:upstream.origin,streamingStages:true,config:{port:8092,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-async-parent')}};
  for(const outcome of ['accepted','refused','rejected']){const pending=Promise.withResolvers(),trace='parent-async-'+outcome,events=[];holds.push(pending);gateway=await startCapacityProcess({runtime:lab.runtime,role:'gateway',configuration,sink:event=>{if(event.trace!==trace)return true;events.push(event);return pending.promise;}});agent=capacityAgent(gateway.ready.tlsOrigin,{ca:certificates.ca});const result=await requestCapacityTarget({origin:gateway.ready.tlsOrigin,path:'/health',agent,headers:{'x-capacity-trace':trace}});assert.equal(result.success,true);await until(()=>events.some(event=>event.kind==='gateway-handler-settled'));assert.equal(events.filter(event=>event.kind==='gateway-request-start').length,1);assert.equal(gateway.observationStatus().pendingCallbacks,events.length);assert.equal(gateway.observationStatus().parentEventDropped,0);const closedGateway=gateway;let childClosed=false;gateway.child.once('close',()=>{childClosed=true;});agent.destroy();agent=null;await gateway.close();gateway=null;await until(()=>childClosed);const count=events.length;assert.equal(closedGateway.observationStatus().closed,true);assert.equal(closedGateway.stageSource.isOpen(),false);assert.equal(closedGateway.observationStatus().pendingCallbacks,count);if(outcome==='rejected')pending.reject(Error('SYNTHETIC_PRIVATE_PARENT_REJECTION'));else pending.resolve(outcome==='accepted');await tick();assert.equal(closedGateway.observationStatus().pendingCallbacks,0);assert.equal(closedGateway.observationStatus().parentEventDropped,outcome==='accepted'?0:count);assert.equal(JSON.stringify(closedGateway.observationStatus()).includes('SYNTHETIC_PRIVATE'),false);}
 }finally{for(const pending of holds)pending.resolve(true);agent?.destroy();await gateway?.close();await store?.close();await upstream?.close();await lab?.close();await rm(directory,{recursive:true,force:true});}
});
