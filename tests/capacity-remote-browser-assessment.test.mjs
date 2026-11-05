import test from 'node:test';
import assert from 'node:assert/strict';
import tls from 'node:tls';
import https from 'node:https';
import net from 'node:net';
import {mkdtemp,rm,writeFile,lstat,realpath} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,createHash,X509Certificate} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createCapacityRemoteLauncher,capacityRemoteLauncherBinding} from '../tools/capacity/remote-launcher.mjs';
import {connectCapacityRemoteTransport} from '../tools/capacity/remote-transport.mjs';
import {remoteHash} from '../tools/capacity/remote-protocol.mjs';
import {requestCapacityTarget} from '../tools/capacity/http-client.mjs';
import {createCapacityRequestDriver,capacityRequestDriverAgent,capacityRequestDriverRemoteSourceBinding} from '../tools/capacity/request-driver.mjs';
import {createCapacityBrowserAssessment,capacityBrowserTargetDescriptor} from '../tools/capacity/browser-assessment.mjs';

const pgBin=process.env.HARBOR_CAPACITY_PG_BIN,pgLib=process.env.HARBOR_CAPACITY_PG_LIB,browserModule=process.env.HARBOR_CAPACITY_BROWSER_MODULE,browserBinary=process.env.HARBOR_CAPACITY_BROWSER_BINARY;
const real={skip:!pgBin||!browserModule||!browserBinary?'需显式私有PG18、Playwright和Chromium；缺条件不冒充远端通过':false,timeout:180000};
const until=async(read,predicate=value=>!!value,timeoutMs=30000)=>{const end=Date.now()+timeoutMs;for(;;){const value=await read();if(predicate(value))return value;assert.ok(Date.now()<end,'remote browser condition timeout');await delay(10);}};
async function fixture(options={}){
 const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');const runtime=createOwnedRuntime(),directory=await mkdtemp(join(tmpdir(),'harbor-remote-browser-'));let target,launcher,transport,driver,headerServer,relay;const sockets=new Set(),relaySources=new Set();let headerPadding=0,paused=false;
 try{
  target=await createTarget({profile:'selfcheck',binDirectory:pgBin,libraryDirectory:pgLib,clientDriver:'local'});await target.prepareActorEpoch({phase:'ui-additional-load'});const descriptor=capacityBrowserTargetDescriptor(target),address=new URL(descriptor.origin);
  const serverSpkiHash=await new Promise((resolve,reject)=>{const socket=tls.connect({host:address.hostname,port:Number(address.port),ca:target.certificateAuthority,rejectUnauthorized:true},()=>{try{resolve(createHash('sha256').update(new X509Certificate(socket.getPeerCertificate().raw).publicKey.export({type:'spki',format:'der'})).digest('base64'));}catch(error){reject(error);}finally{socket.end();}});socket.once('error',reject);});
  const cert=await syntheticCertificates(directory),serverTls={caFile:join(directory,'ca.pem'),certFile:join(directory,'server.pem'),keyFile:join(directory,'server.key'),peerCertificateHash:remoteHash(new X509Certificate(cert.client.cert).raw)},clientTls={caFile:join(directory,'ca.pem'),certFile:join(directory,'client.pem'),keyFile:join(directory,'client.key'),peerCertificateHash:remoteHash(new X509Certificate(cert.server.cert).raw)};
  headerServer=https.createServer(cert.server,(req,res)=>{req.resume();res.setHeader('x-retained','x'.repeat(headerPadding));res.end('{}');});headerServer.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(resolve=>headerServer.listen(0,'127.0.0.1',resolve));const headerOrigin='https://127.0.0.1:'+headerServer.address().port;
  const settings={deepPages:3,sampleIntervalMs:options.sampleIntervalMs??100,timeoutMs:10000,maxSeries:2048,searchTerm:'capacityScenario'},profile={id:'actual-owned',origin:descriptor.origin,bindingTag:descriptor.bindingTag,epoch:descriptor.epoch,cookies:target.adminActors.slice(0,5).map(actor=>({name:'harbor_session',value:actor.headers.cookie.slice('harbor_session='.length)})),serverSpkiHash,...settings,maxActionMs:60000};
  let selectedBrowserModule=browserModule;if(options.connectFailureOnce||options.delayLaunch){selectedBrowserModule=join(directory,'browser.mjs');await writeFile(selectedBrowserModule,'import {chromium as actual} from '+JSON.stringify(browserModule)+';let first='+JSON.stringify(!!options.connectFailureOnce)+';export const chromium={launchServer:async(...args)=>{'+(options.delayLaunch?'await new Promise(resolve=>setTimeout(resolve,'+(typeof options.delayLaunch==='number'?options.delayLaunch:60000)+'));':'')+'return actual.launchServer(...args);},connect:(...args)=>{if(first){first=false;throw Error("synthetic_connect_failure");}return actual.connect(...args);}};',{mode:0o600});}
  launcher=await createCapacityRemoteLauncher({runtime,tls:serverTls,agents:[{id:'unused',origin:'https://127.0.0.1:1',tls:{caFile:clientTls.caFile},routes:[{method:'GET',path:'/ready'}]},{id:'headers',origin:headerOrigin,tls:{caFile:clientTls.caFile},routes:[{method:'GET',path:'/headers'}]}],browser:{module:selectedBrowserModule,binary:browserBinary,maxBrowsers:1,profiles:options.activationTemplate?[]:[profile],...(options.activationTemplate?{activationTemplate:Object.fromEntries(Object.entries(profile).filter(([key])=>!['bindingTag','epoch','cookies'].includes(key)))}:{}),...(options.maxCallbacks?{maxCallbacks:options.maxCallbacks}:{}),...(options.callbackTimeoutMs?{callbackTimeoutMs:options.callbackTimeoutMs}:{})}});
  let transportOrigin=launcher.origin;if(options.relay){const upstreamAddress=new URL(launcher.origin);relay=net.createServer(client=>{const source=net.connect({host:'127.0.0.1',port:Number(upstreamAddress.port)});for(const socket of [client,source]){sockets.add(socket);socket.once('close',()=>sockets.delete(socket));socket.on('error',()=>{client.destroy();source.destroy();});}relaySources.add(source);source.once('close',()=>relaySources.delete(source));client.pipe(source);source.pipe(client);source.capacityClient=client;if(paused){source.unpipe(client);source.pause();}});await new Promise(resolve=>relay.listen(0,'127.0.0.1',resolve));transportOrigin='tls://127.0.0.1:'+relay.address().port;}
  transport=await connectCapacityRemoteTransport({origin:transportOrigin,runId:launcher.runId,tls:clientTls});driver=await createCapacityRequestDriver({remote:{transport},agentPlans:[{id:'unused',origin:'https://127.0.0.1:1'},{id:'headers',origin:headerOrigin}],...options.driver});
  return {target,driver,transport,launcher,descriptor,settings,headerOrigin,activation:{bindingTag:profile.bindingTag,epoch:profile.epoch,cookies:profile.cookies},setHeaderPadding:value=>{headerPadding=value;},pauseRelay:value=>{if(paused===value)return;paused=value;for(const source of relaySources)if(value){source.unpipe(source.capacityClient);source.pause();}else source.pipe(source.capacityClient);},inspect:()=>capacityRemoteLauncherBinding(launcher).inspect(),close:async({allowUnknown=false}={})=>{let error;for(const [index,operation] of [()=>driver?.close(),()=>launcher?.close(),()=>runtime.close(),()=>target?.close(),async()=>{for(const socket of sockets)socket.destroy();if(relay)await new Promise(resolve=>relay.close(resolve));if(headerServer)await new Promise(resolve=>headerServer.close(resolve));},()=>rm(directory,{recursive:true,force:true})].entries())try{await operation();}catch(cause){if(!(allowUnknown&&index<2&&/cleanup_unknown|browser_closed|snapshot_unavailable/.test(cause.message)))error??=cause;}if(error)throw error;}};
 }catch(error){await driver?.close().catch(()=>{});await launcher?.close().catch(()=>{});await runtime.close();await target?.close();for(const socket of sockets)socket.destroy();if(relay)await new Promise(resolve=>relay.close(resolve));if(headerServer)await new Promise(resolve=>headerServer.close(resolve));await rm(directory,{recursive:true,force:true});throw error;}
}

async function observedModel(target,event){const result=await target.sendModel(event),safe=Object.fromEntries(['operation','method','variant','status','success','complete','httpEnded','wireSent','startedAt','finishedAt','durationMs','receivedBytes','firstContentAt','headersAt','connectedAt','tlsAt','reusedSocket','bodyHash','records','trace','error'].filter(key=>Object.hasOwn(result,key)).map(key=>[key,result[key]]));await target.onResult({scenario:event.scene.id,kind:'model',event:{actorIndex:event.actorIndex,sequence:event.sequence},result:{...safe,validatedSuccess:result.success===true&&result.complete===true}});return result;}

test('fixed authenticated remote scenario runs the same five-actor real API/DOM and pagination core',real,async()=>{
 const f=await fixture(),observed=new Map(),expected=f.target.adminActors.slice(0,5).map(actor=>createHash('sha256').update(actor.headers.cookie).digest('hex'));let assessment,modelsRunning=false,stage='create';const models=[];
 const observer={beforeRequest:async dto=>{assert.equal(dto.credentials.cookie,expected[dto.actorIndex]);const trace='remote-browser-'+randomUUID();observed.set(dto.requestId,{dto,trace,events:[]});return {'x-capacity-trace':trace};},response:dto=>{observed.get(dto.requestId).status=dto.status;observed.get(dto.requestId).events.push('response');},requestFinished:dto=>observed.get(dto.requestId).events.push('finished'),requestFailed:dto=>observed.get(dto.requestId).events.push(dto.cancelled?'cancelled':'failed')};
 try{
  const {registerOwnedCapacityBrowserDriver}=await import('../tools/capacity/gateway-adapter.mjs');
  await assert.rejects(createCapacityBrowserAssessment({target:f.target,launchBrowser:f.driver.launchBrowser,observer,...f.settings}),/driver_not_owned/);
  assert.throws(()=>registerOwnedCapacityBrowserDriver({...f.target},f.driver),/target_not_owned/);assert.throws(()=>registerOwnedCapacityBrowserDriver(f.target,{...f.driver}),/registration_invalid/);
  const registration=registerOwnedCapacityBrowserDriver(f.target,f.driver);assert.equal(registration.bindingTag,f.descriptor.bindingTag);assert.equal(registration.formalQualified,false);
  assessment=await createCapacityBrowserAssessment({target:f.target,launchBrowser:f.driver.launchBrowser,observer,...f.settings});stage='prepare';await assessment.prepare();assert.equal(assessment.status().contexts,5);const process=await f.inspect();assert.equal(process.execution.browsers.length,1);assert.equal(process.execution.browserScenarios.sessions,1);
  for(const view of ['requests','members','sessions'])for(const variant of ['default','maximum','deep','browser-cold']){stage=view+'/'+variant;const result=await assessment.measure({actorIndex:0,view,variant});assert.equal(result.businessVerified,true,JSON.stringify({view,variant,error:result.error}));if(variant==='maximum')assert.ok(result.rowCount<=100);}
  for(const view of ['statistics','search']){for(const variant of ['default','browser-cold'])assert.equal((await assessment.measure({actorIndex:0,view,variant})).businessVerified,true);for(const variant of ['maximum','deep'])assert.equal((await assessment.measure({actorIndex:0,view,variant})).skipped,'ui_variant_not_available');}
  for(let actorIndex=1;actorIndex<5;actorIndex++)for(const view of ['requests','members','sessions','statistics','search'])assert.equal((await assessment.measure({actorIndex,view})).businessVerified,true);
  const actors=await Promise.all(Array.from({length:5},(_,actorIndex)=>assessment.measure({actorIndex,view:'requests'})));assert.ok(actors.every(row=>row.businessVerified));assert.equal(new Set(actors.map(row=>row.actorFingerprint)).size,5);
  const scene={id:'remote-browser-mixed',durationMs:1000,modelQps:2,adminQps:5,upstreamMs:[300,300],payloadBytes:1024};await f.target.configureScenario({scene});stage='mixed';modelsRunning=true;for(let index=0;index<2;index++)models.push((async()=>{let sequence=index;while(modelsRunning)await observedModel(f.target,{actor:f.target.employeeActors[index],actorIndex:index,sequence:sequence++,scene});})());await until(()=>f.target.upstream.status().active,value=>value>0);const baseline=assessment.status().completed;await assessment.start({scene});await until(()=>assessment.status().completed,value=>value>=baseline+5);await assessment.stop();modelsRunning=false;await Promise.all(models);
  const evidence=assessment.collectEvidence();assert.ok(evidence.browserLists.measurements.some(row=>row.mixed.dom.samples>0));assert.equal(evidence.browserLists.passed,null);assert.equal(assessment.status().contexts,0);assert.equal(assessment.status().observerErrors,0);assert.ok(observed.size>0);
  const priorTag=f.descriptor.bindingTag;await f.target.prepareActorEpoch({phase:'ui-additional-load'});assert.notEqual(capacityBrowserTargetDescriptor(f.target).bindingTag,priorTag);await assert.rejects(assessment.measure({view:'requests'}),/target_epoch_changed/);assert.throws(()=>registerOwnedCapacityBrowserDriver(f.target,f.driver),/registration_mismatch/);
  await assessment.close();assert.equal(assessment.status().closed,true);assert.equal(assessment.status().observerPending,0);assert.equal((await f.inspect()).execution.browsers.length,0);
  const finished=[...observed.values()].filter(row=>row.events.includes('finished'));await until(()=>finished.every(row=>f.target.events.some(event=>event.kind==='gateway-response-end'&&event.trace===row.trace)));for(const row of finished){const starts=f.target.events.filter(event=>event.kind==='gateway-request-start'&&event.trace===row.trace);assert.equal(starts.length,1);assert.equal(starts[0].credentials.cookie,row.dto.credentials.cookie);assert.deepEqual(row.events,['response','finished']);}
  assert.equal(new Set([...observed.values()].map(row=>row.dto.credentials.cookie)).size,5);for(const actor of f.target.adminActors)assert.equal(JSON.stringify(evidence).includes(actor.headers.cookie),false);
 }catch(error){error.message+=' at '+stage+' '+(error.remoteCode??'')+' '+JSON.stringify({observerCalls:observed.size,callbackFailures:f.driver.status().callbackFailures,responsesByStatus:[...observed.values()].reduce((counts,row)=>{const key=String(row.status??'not-received');counts[key]=(counts[key]??0)+1;return counts;},{}),bridge:assessment?.status()?.remoteSnapshot});throw error;}finally{modelsRunning=false;await Promise.allSettled(models);await assessment?.close().catch(()=>{});await f.close();}
});


function observed(observer={}){
 return {beforeRequest:observer.beforeRequest??(async()=>({'x-capacity-trace':'browser-boundary-'+randomUUID()})),response:observer.response??(()=>{}),requestFinished:observer.requestFinished??(()=>{}),requestFailed:observer.requestFailed??(()=>{})};
}
async function openAssessment(f,observer){const {registerOwnedCapacityBrowserDriver}=await import('../tools/capacity/gateway-adapter.mjs');registerOwnedCapacityBrowserDriver(f.target,f.driver);return createCapacityBrowserAssessment({target:f.target,launchBrowser:f.driver.launchBrowser,observer,...f.settings});}

test('remote start failure rolls back its signal and an old abort cannot stop a corrected scene',real,async()=>{
 const f=await fixture();let assessment;const oldSignal=new AbortController();try{
  assessment=await openAssessment(f,observed());await assert.rejects(assessment.start({scene:{id:'invalid',durationMs:0},signal:oldSignal.signal}),/command_rejected/);
  const scene={id:'corrected-scene',durationMs:200};assert.equal((await assessment.start({scene})).started,true);await assert.rejects(assessment.start({scene:{id:'must-not-replace',durationMs:200}}),/scene_active/);
  oldSignal.abort();await until(()=>assessment.status().completed,value=>value>0);assert.equal(assessment.status().scene,scene.id);await assessment.stop();await assessment.close();assert.equal(assessment.status().observerPending,0);
 }finally{await assessment?.close().catch(()=>{});await f.close();}
});

for(const rejected of [false,true])test('late remote observer settlement refreshes a stopped scene without another operation'+(rejected?' with actual rejection':''),real,async()=>{
 const f=await fixture();let assessment,release,rejectHeld,hold=false,heldCalls=0;const held=new Promise((resolve,reject)=>{release=resolve;rejectHeld=reject;});try{
  assessment=await openAssessment(f,observed({requestFinished:()=>{if(hold){heldCalls++;return held;}}}));await assessment.prepare();await until(()=>assessment.status().observerPending,value=>value===0);hold=true;
  await assessment.start({scene:{id:rejected?'late-rejected':'late-finished',durationMs:300}});await until(()=>heldCalls);await assessment.stop();assert.ok(assessment.status().observerPending>0);const sequence=assessment.status().remoteSnapshot.sequence;
  if(rejected)rejectHeld(Error('synthetic delayed observer rejection'));else release();
  await until(()=>assessment.status(),value=>value.observerPending===0&&value.remoteSnapshot.sequence>sequence);assert.equal(f.driver.status().pendingCallbacks,0);if(rejected)assert.ok(assessment.status().observerErrors>0);else assert.equal(assessment.status().observerErrors,0);
  await assessment.close();assert.equal(assessment.status().contexts,0);
 }finally{release();await assessment?.close().catch(()=>{});await f.close();}
});

test('terminal remote snapshots cannot bypass a held callback and the shared byte ceiling',real,async()=>{
 const f=await fixture({driver:{maxInFlight:32,maxFrameBytes:15360,maxBufferedBytes:16384}});let assessment,release;const held=new Promise(resolve=>{release=resolve;});try{
  assessment=await openAssessment(f,observed());await assessment.prepare();await until(()=>assessment.status().observerPending,value=>value===0);
  const agent=capacityRequestDriverAgent(f.driver,'headers');let overhead;await requestCapacityTarget({origin:f.headerOrigin,agent,path:'/headers',onResponseHeaders:headers=>{overhead=Buffer.byteLength(JSON.stringify(headers));}});
  const snapshotBytes=f.driver.status().snapshotBytes;f.setHeaderPadding(16384-snapshotBytes-overhead-128);
  await requestCapacityTarget({origin:f.headerOrigin,agent,path:'/headers',onResponseHeaders:()=>held});assert.equal(f.driver.status().pendingCallbacks,1);assert.ok(f.driver.status().callbackBytes+f.driver.status().observationBytes<=16384);
  await assessment.measure({view:'requests'});const beforeClose=f.driver.status().snapshotBytes;await assert.rejects(assessment.close(),error=>error.message==='capacity_remote_browser_snapshot_unavailable'&&error.resourcesReleased===true);
  const status=assessment.status();assert.equal(status.closed,true);assert.equal(status.remoteSnapshot.terminalSnapshotAvailable,false);assert.equal(status.contexts,null);assert.throws(()=>assessment.collectEvidence(),/snapshot_unavailable/);assert.equal(f.driver.status().snapshotBytes,beforeClose);assert.ok(f.driver.status().callbackBytes+f.driver.status().observationBytes<=16384);assert.equal((await f.inspect()).execution.browsers.length,0);
  release();await f.driver.close();assert.equal(f.driver.status().snapshotBytes,0);assert.equal(f.driver.status().resourcesReleased,true);
 }finally{release();await assessment?.close().catch(()=>{});await f.close();}
});

test('real TLS backpressure reports lost remote state updates and recovers the next authenticated snapshot',real,async()=>{
 const f=await fixture({relay:true,sampleIntervalMs:1});let assessment,failure,stage='open';try{
  assessment=await openAssessment(f,observed());await assessment.start({scene:{id:'actual-wire-backpressure',durationMs:1000}});await until(()=>assessment.status().completed,value=>value>0);stage='pause';f.pauseRelay(true);
  await until(async()=>(await f.inspect()).execution.browserScenarios.snapshotDropped,value=>value>0,8000);stage='resume';f.pauseRelay(false);
  await until(()=>assessment.status().remoteSnapshot.snapshotDropped,value=>value>0);stage='stop';await assessment.stop();stage='close';await assessment.close();assert.ok(assessment.status().remoteSnapshot.snapshotDropped>0);assert.equal(assessment.status().observerPending,0);stage='transport remains open';assert.equal(f.transport.status().closed,false);
 }catch(error){failure=error;const endpointStatus=await until(()=>f.inspect().catch(()=>null),value=>!value||!value.executor.closed||value.executor.stderrFinished,1000).catch(()=>null);error.message+=' at '+stage+' '+JSON.stringify({endpoint:endpointStatus?{closed:endpointStatus.closed,draining:endpointStatus.draining,closeTransitions:endpointStatus.closeTransitions,closeReason:endpointStatus.closeReason,executorClosed:endpointStatus.executor.closed,executorExit:{code:endpointStatus.executor.exitCode,signal:endpointStatus.executor.signalCode,kind:endpointStatus.executor.stderrKind,frame:endpointStatus.executor.stderrFrame,parentClosing:endpointStatus.executor.closing,stderrFinished:endpointStatus.executor.stderrFinished,exception:{kind:endpointStatus.executor.exceptionKind,code:endpointStatus.executor.exceptionCode,frame:endpointStatus.executor.exceptionFrame,detail:endpointStatus.executor.exceptionDetail,api:endpointStatus.executor.exceptionApi},shutdownReason:endpointStatus.executor.shutdownReason,replyFailure:endpointStatus.executor.replyFailure},execution:endpointStatus.execution?.browserScenarios}:null,transport:f.transport.status(),driver:{failures:f.driver.status().failures,callbacks:f.driver.status().pendingCallbacks},browser:assessment?.status()?.remoteSnapshot});throw error;}finally{f.pauseRelay(false);await assessment?.close().catch(()=>{});await f.close({allowUnknown:!!failure});}
});


test('remote measure cancellation waits for action terminal and late trace ACK never forwards the cancelled request',real,async()=>{
 const f=await fixture();let assessment,release,heldCalls=0,hold=false;const held=new Promise(resolve=>{release=resolve;}),trace='cancelled-browser-'+randomUUID();try{
  assessment=await openAssessment(f,observed({beforeRequest:async()=>{if(hold){heldCalls++;await held;return {'x-capacity-trace':trace};}return {'x-capacity-trace':'prepare-'+randomUUID()};}}));await assessment.prepare();await until(()=>assessment.status().observerPending,value=>value===0);hold=true;
  const controller=new AbortController(),action=assessment.measure({view:'requests',signal:controller.signal});await until(()=>heldCalls);controller.abort();const result=await action;assert.equal(result.cancelled,true);assert.equal((await f.inspect()).execution.browserScenarios.actions,0);assert.ok(f.driver.status().pendingCallbacks>0);
  hold=false;release();await until(()=>f.driver.status().pendingCallbacks,value=>value===0);assert.equal(f.target.events.some(event=>event.trace===trace),false);assert.equal((await assessment.measure({view:'requests'})).businessVerified,true);await assessment.close();assert.equal(assessment.status().observerPending,0);
 }finally{release();await assessment?.close().catch(()=>{});await f.close();}
});

test('executor callback timeout and budget release only executor leases while a real controller sink is still pending',real,async()=>{
 const f=await fixture({maxCallbacks:1,callbackTimeoutMs:200});let assessment,release,heldCalls=0;const held=new Promise(resolve=>{release=resolve;}),trace='expired-browser-'+randomUUID();try{
  assessment=await openAssessment(f,observed({beforeRequest:async()=>{heldCalls++;await held;return {'x-capacity-trace':trace};}}));const preparation=assessment.prepare();await until(()=>heldCalls);await assert.rejects(preparation,/command_rejected/);
  const source=await until(async()=>(await f.inspect()).execution.browserScenarios,value=>value.pendingCallbacks===0&&value.actions===0);assert.equal(source.callbackBytes,0);assert.ok(f.driver.status().pendingCallbacks>0);assert.ok(f.driver.status().callbackBytes>0);
  release();await until(()=>f.driver.status().pendingCallbacks,value=>value===0);assert.equal(f.driver.status().callbackBytes,0);assert.equal(f.target.events.some(event=>event.trace===trace),false);await assessment.close();assert.equal((await f.inspect()).execution.browsers.length,0);
 }finally{release();await assessment?.close().catch(()=>{});await f.close();}
});

test('an actual control channel disconnect makes active browser state unknown and cleans owned execution without retry',real,async()=>{
 const f=await fixture();let assessment;try{
  assessment=await openAssessment(f,observed());await assessment.start({scene:{id:'disconnect-active-browser',durationMs:1000}});await until(()=>assessment.status().completed,value=>value>0);f.transport.disconnect();
  assert.equal(assessment.status().remoteSnapshot.disconnected,true);assert.equal(assessment.status().contexts,null);assert.equal(assessment.status().active,null);assert.equal(assessment.status().observerPending,null);await assert.rejects(assessment.measure({view:'requests'}),/browser_closed/);
  const state=await until(()=>f.inspect(),value=>value.executor.resourcesReleased===true);assert.equal(state.executor.closed,true);assert.equal(state.executor.unknownBrowserStarts,0);await assert.rejects(assessment.close(),/browser_closed/);
 }finally{await assessment?.close().catch(()=>{});await f.close({allowUnknown:true});}
});


test('an initial actual browser connection failure releases its owned slot only after confirmed server cleanup',real,async()=>{
 const f=await fixture({connectFailureOnce:true});let assessment;try{
  await assert.rejects(openAssessment(f,observed()),/command_rejected/);const failed=await f.inspect();assert.equal(failed.execution.browsers.length,0);assert.equal(failed.execution.launching,0);assert.equal(failed.execution.browserScenarios.sessions,0);
  assessment=await openAssessment(f,observed());await assessment.prepare();assert.equal((await assessment.measure({view:'requests'})).businessVerified,true);await assessment.close();assert.equal((await f.inspect()).execution.browsers.length,0);
 }finally{await assessment?.close().catch(()=>{});await f.close();}
});


test('executor death during scenario startup retains unknown browser ownership and its recovery directory',real,async()=>{
 const f=await fixture({delayLaunch:true});let open,temporaryDirectory;try{
  open=openAssessment(f,observed());const rejected=assert.rejects(open,error=>error.delivery==='unknown');const state=await until(()=>f.inspect(),value=>value.execution?.launching===1&&value.executor.unknownBrowserStarts===1);temporaryDirectory=state.executor.temporaryDirectory;assert.equal(state.execution.browsers.length,0);assert.ok(temporaryDirectory.startsWith(join(tmpdir(),'harbor-capacity-remote-executor-')));
  process.kill(state.executor.pid,'SIGKILL');await rejected;const terminal=await until(()=>f.inspect(),value=>value.executor.closed);assert.equal(terminal.executor.unknownBrowserStarts,1);assert.equal(terminal.executor.resourcesReleased,false);assert.equal(terminal.executor.temporaryResourcesReleased,false);assert.equal((await lstat(temporaryDirectory)).isDirectory(),true);
 }finally{await open?.catch(()=>{});await f.close({allowUnknown:true});if(temporaryDirectory){assert.equal(await realpath(temporaryDirectory),temporaryDirectory);assert.equal((await lstat(temporaryDirectory)).uid,process.getuid());await rm(temporaryDirectory,{recursive:true});}}
});

test('root-initialized browser template activates only epoch/cookies and retires actual old browser sessions before replacement',real,async()=>{
 const f=await fixture({activationTemplate:true}),source=capacityRequestDriverRemoteSourceBinding(f.driver);let assessment;
 try{
  assert.equal(source.transportBinding.browserProfiles.length,0);
  await assert.rejects(f.transport.activateBrowserProfile({...f.activation,origin:'https://unapproved.invalid'}),/activation_invalid/);
  await assert.rejects(f.transport.activateBrowserProfile({...f.activation,cookies:Array(5).fill(f.activation.cookies[0])}),/activation_invalid/);
  const profile=await source.activateBrowserProfile(f.activation);assert.deepEqual(profile.cookieFingerprints,f.descriptor.cookieFingerprints);assert.equal(source.executionSnapshot().browserMeasurements,0);
  const {registerOwnedCapacityBrowserDriver}=await import('../tools/capacity/gateway-adapter.mjs');registerOwnedCapacityBrowserDriver(f.target,f.driver);
  const observer={beforeRequest:()=>({'x-capacity-trace':'activation-'+randomUUID()}),response:()=>true,requestFinished:()=>true,requestFailed:()=>true};
  assessment=await createCapacityBrowserAssessment({target:f.target,launchBrowser:f.driver.launchBrowser,observer,...f.settings});await assessment.prepare();assert.equal((await assessment.measure({view:'requests'})).businessVerified,true);
  await assert.rejects(source.activateBrowserProfile({...f.activation,bindingTag:randomUUID()}),{code:'capacity_remote_command_rejected'});assert.equal(source.executionSnapshot().activatedProfile.bindingTag,f.activation.bindingTag);
  await f.target.prepareActorEpoch({phase:'ui-additional-load'});const descriptor=capacityBrowserTargetDescriptor(f.target),next={bindingTag:descriptor.bindingTag,epoch:descriptor.epoch,cookies:f.target.adminActors.slice(0,5).map(actor=>({name:'harbor_session',value:actor.headers.cookie.slice('harbor_session='.length)}))};
  const updated=await source.activateBrowserProfile(next);assert.equal(assessment.status().closed,true);assessment=null;assert.deepEqual(updated.cookieFingerprints,descriptor.cookieFingerprints);assert.equal(source.executionSnapshot().browserMeasurements,0,'previous snapshot cannot supply current evidence');registerOwnedCapacityBrowserDriver(f.target,f.driver);
  assessment=await createCapacityBrowserAssessment({target:f.target,launchBrowser:f.driver.launchBrowser,observer,...f.settings});await assessment.prepare();assert.equal((await assessment.measure({view:'requests'})).businessVerified,true);await assessment.close();assessment=null;
  assert.equal(source.executionSnapshot().browserMeasurements,1);assert.equal(source.executionSnapshot().completedBrowserAssessments,1);assert.equal(source.executionSnapshot().browserFailures,0);assert.equal(f.driver.status().pendingBrowserScenarios,0);
 }finally{await assessment?.close();await f.close();}
});

test('template-only browser launch honors the approved action budget beyond the former eleven-second child timeout',real,async()=>{
 const f=await fixture({activationTemplate:true,delayLaunch:12000});let assessment;
 try{
  await capacityRequestDriverRemoteSourceBinding(f.driver).activateBrowserProfile(f.activation);const {registerOwnedCapacityBrowserDriver}=await import('../tools/capacity/gateway-adapter.mjs');registerOwnedCapacityBrowserDriver(f.target,f.driver);const observer={beforeRequest:()=>({'x-capacity-trace':'budget-'+randomUUID()}),response:()=>true,requestFinished:()=>true,requestFailed:()=>true};
  const began=performance.now();assessment=await createCapacityBrowserAssessment({target:f.target,launchBrowser:f.driver.launchBrowser,observer,...f.settings});assert.ok(performance.now()-began>=12000,'must exercise the former child timeout');await assessment.prepare();assert.equal((await assessment.measure({view:'requests'})).businessVerified,true);await assessment.close();assessment=null;assert.equal(f.driver.status().pendingBrowserScenarios,0);
 }finally{await assessment?.close();await f.close();}
});
