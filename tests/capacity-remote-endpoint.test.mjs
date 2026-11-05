import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import https from 'node:https';
import tls from 'node:tls';
import {mkdtemp,rm,readFile,lstat} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,X509Certificate} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime,ownedProcessProof} from '../tools/capacity/owned-runtime.mjs';
import {createCapacityRemoteLauncher,capacityRemoteLauncherBinding} from '../tools/capacity/remote-launcher.mjs';
import {createCapacityRemoteEndpoint} from '../tools/capacity/remote-endpoint.mjs';
import {connectCapacityRemoteTransport,capacityRemoteTransportBinding} from '../tools/capacity/remote-transport.mjs';
import {createCapacityRequestDriver,capacityRequestDriverAgent,isOwnedCapacityRequestDriver} from '../tools/capacity/request-driver.mjs';
import {requestCapacityTarget,createCapacityClientStageObserver} from '../tools/capacity/http-client.mjs';
import {createCapacityRemoteFrames,remoteHash} from '../tools/capacity/remote-protocol.mjs';

let directory,certificates,serverTls,clientTls;
before(async()=>{
 directory=await mkdtemp(join(tmpdir(),'harbor-remote-test-'));certificates=await syntheticCertificates(directory);
 const serverHash=remoteHash(new X509Certificate(certificates.server.cert).raw),clientHash=remoteHash(new X509Certificate(certificates.client.cert).raw);
 serverTls={caFile:join(directory,'ca.pem'),certFile:join(directory,'server.pem'),keyFile:join(directory,'server.key'),peerCertificateHash:clientHash};
 clientTls={caFile:join(directory,'ca.pem'),certFile:join(directory,'client.pem'),keyFile:join(directory,'client.key'),peerCertificateHash:serverHash};
});
after(async()=>{await rm(directory,{recursive:true,force:true});});

async function until(read,predicate=value=>!!value,timeoutMs=5000){const deadline=Date.now()+timeoutMs;for(;;){const value=await read();if(predicate(value))return value;if(Date.now()>deadline)throw Error('condition timeout');await delay(10);}}
const alive=async pid=>{try{const raw=await readFile('/proc/'+pid+'/stat','utf8');return raw.slice(raw.lastIndexOf(')')+2).split(' ')[0]!=='Z';}catch(error){if(error.code==='ENOENT')return false;throw error;}};

async function fixture(options={}){
 const runtime=createOwnedRuntime(),sockets=new Set(),requests=[];let server,launcher,transport;
 try{
  server=https.createServer({...certificates.server,ca:certificates.ca,requestCert:true,rejectUnauthorized:true},(req,res)=>{requests.push(req.url);if(options.handler)return options.handler(req,res);req.resume();if(req.url==='/wait'){res.writeHead(200,{'content-type':'text/event-stream'});res.write(': alive\n\n');return;}res.setHeader('content-type','application/json');res.end(JSON.stringify({ok:true}));});
  server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
  const target='https://127.0.0.1:'+server.address().port,agents=[{id:'test',origin:target,tls:{caFile:clientTls.caFile,certFile:clientTls.certFile,keyFile:clientTls.keyFile},routes:options.routes??[{method:'GET',path:'/health'},{method:'POST',path:'/wait'}]}];
  launcher=await createCapacityRemoteLauncher({runtime,tls:serverTls,agents,...options.launcher});
  const connect=async configuration=>transport=await connectCapacityRemoteTransport({origin:launcher.origin,runId:launcher.runId,tls:clientTls,...configuration});
  if(options.connect!==false)await connect(options.transport);
  return {launcher,runtime,target,requests,connect,get transport(){return transport;},inspect:()=>capacityRemoteLauncherBinding(launcher).inspect(),request:(overrides={})=>transport.request({agentId:'test',path:'/health',method:'GET',mode:'json',timeoutMs:5000,...overrides}),close:async()=>{if(transport&&!transport.status().closed)await transport.close().catch(()=>{});let cleanupError;try{await launcher.close();}catch(error){cleanupError=error;}for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));await runtime.close();if(cleanupError)throw cleanupError;}};
 }catch(error){await transport?.close().catch(()=>{});await launcher?.close().catch(()=>{});for(const socket of sockets)socket.destroy();if(server)await new Promise(resolve=>server.close(resolve));await runtime.close();throw error;}
}

test('pinned mTLS reaches actual owned execution child and capability remains explicitly limited',{timeout:30000},async()=>{
 const f=await fixture();try{
  assert.equal(capacityRemoteLauncherBinding({...f.launcher}),null);const binding=capacityRemoteLauncherBinding(f.launcher);assert.equal(binding.scope,'local-owned-launcher-process');assert.equal(ownedProcessProof(f.runtime,binding.child).role,'helper');
  const status=await f.inspect();assert.notEqual(status.execution.pid,process.pid);assert.equal(status.execution.pid,status.executor.pid);assert.equal(status.execution.agents,1);
  const raw=await readFile('/proc/'+status.execution.pid+'/stat','utf8');assert.equal(Number(raw.slice(raw.lastIndexOf(')')+2).split(' ')[1]),binding.child.pid,'actual executor is a direct child of this local owned endpoint');
  const result=await f.request().result;assert.equal(result.success,true);assert.equal(result.json.ok,true);assert.equal(result.socketEvidence.tlsAuthorized,true);assert.deepEqual(f.requests,['/health']);
  const proof=capacityRemoteTransportBinding(f.transport);assert.equal(proof.scope,'authenticated-restricted-endpoint');assert.ok(proof.missing.includes('remote_execution_ownership_unproven'));assert.ok(proof.missing.includes('immutable_runtime_unproven'));assert.ok(proof.missing.includes('actual_lan_path_unproven'));assert.equal(capacityRemoteTransportBinding({...f.transport,...proof,lan:true,ownedExecutionVerified:true,immutableRuntime:true}),null);
  await f.transport.close();assert.equal(capacityRemoteTransportBinding(f.transport),null);
 }finally{await f.close();}
});

test('targets, routes, methods, shell fields and caller TLS cannot escape fixed executor plan',{timeout:30000},async()=>{
 const f=await fixture();try{
  for(const mutation of [{agentId:'unknown'},{path:'/private'},{method:'DELETE'},{origin:'https://example.invalid'},{command:'/bin/sh'},{tls:{rejectUnauthorized:false}},{headers:{host:'example.invalid'}},{headers:{'x-forwarded-client-cert':'forged'}}])await assert.rejects(f.request(mutation).result,/command_rejected/);
  assert.deepEqual(f.requests,[]);assert.equal((await f.request().result).success,true);
 }finally{await f.close();}
});

test('cancel ACK confirms abort command separately from actual stream terminal; no retry',{timeout:30000},async()=>{
 const f=await fixture();try{const request=f.request({path:'/wait',method:'POST',mode:'sse'});await until(()=>f.requests.length);const ack=await request.cancel();assert.deepEqual(ack,{acknowledged:true,active:true});const result=await request.result;assert.equal(result.success,false);assert.equal(result.httpEnded,false);assert.deepEqual(f.requests,['/wait']);await until(async()=>(await f.inspect()).execution.active,value=>value===0);}
 finally{await f.close();}
});

test('lost control channel leaves delivery unknown and closes child, agents and request sockets',{timeout:30000},async()=>{
 const f=await fixture();try{const before=await f.inspect(),request=f.request({path:'/wait',method:'POST',mode:'sse'}),rejected=assert.rejects(request.result,error=>error.delivery==='unknown');await until(()=>f.requests.length);f.transport.disconnect();await rejected;await until(()=>f.inspect(),value=>value.resourcesReleased===true);assert.equal(await alive(before.executor.pid),false);assert.deepEqual(f.requests,['/wait']);}
 finally{await f.close();}
});

test('wrong pinned server certificate and cross-run handshake cannot mint capability',{timeout:30000},async()=>{
 for(const mutation of [{tls:{...clientTls,peerCertificateHash:'0'.repeat(64)}},{runId:randomUUID()}]){
  const f=await fixture({connect:false});try{await assert.rejects(f.connect({...mutation,readyTimeoutMs:1000}));assert.deepEqual(f.requests,[]);assert.equal((await f.inspect()).sessionUsed,false);await f.connect();assert.equal((await f.request().result).success,true);}finally{await f.close();}
 }
});

test('wrong client certificate pin is rejected before session or execution',{timeout:30000},async()=>{
 const f=await fixture({connect:false,launcher:{tls:{...serverTls,peerCertificateHash:'0'.repeat(64)}}});try{await assert.rejects(f.connect({readyTimeoutMs:1000}));assert.equal((await f.inspect()).sessionUsed,false);assert.deepEqual(f.requests,[]);}finally{await f.close();}
});

test('same session replay closes channel and cannot duplicate dispatched request',{timeout:30000},async()=>{
 const f=await fixture({connect:false});let socket,channel;try{
  const address=new URL(f.launcher.origin),received=[];socket=tls.connect({...certificates.client,ca:certificates.ca,host:'127.0.0.1',port:Number(address.port),rejectUnauthorized:true});socket.on('error',()=>{});
  channel=createCapacityRemoteFrames(socket,{onFrame:value=>{received.push(value);return true;},onClose:()=>{}});await new Promise(resolve=>socket.once('secureConnect',resolve));
  channel.send({kind:'hello',runId:f.launcher.runId,challenge:randomUUID()},{control:true});const ready=await until(()=>received.find(row=>row.kind==='ready'));
  const request={kind:'request',runId:f.launcher.runId,session:ready.session,sequence:1,id:randomUUID(),payload:{agentId:'test',path:'/health',method:'GET',mode:'json',timeoutMs:5000}};channel.send(request);await until(()=>received.find(row=>row.id===request.id));channel.send(request);await until(()=>channel.status().closed);assert.deepEqual(f.requests,['/health']);await assert.rejects(f.connect({readyTimeoutMs:1000}));
 }finally{channel?.close();socket?.destroy();await f.close();}
});

test('local frame ceiling rejects unsent payload and configured in-flight ceiling has no HTTP side effect',{timeout:30000},async()=>{
 const f=await fixture({transport:{maxInFlight:1,frames:{maxFrameBytes:2048,maxBufferedBytes:8192,controlReserveBytes:2048}}});try{
  const active=f.request({path:'/wait',method:'POST',mode:'sse'});await until(()=>f.requests.length);await assert.rejects(f.request().result,error=>error.delivery==='not-sent'&&error.code==='capacity_remote_inflight_budget');await active.cancel();await active.result;
  await assert.rejects(f.request({body:'x'.repeat(4096)}).result,error=>error.delivery==='not-sent'&&error.code==='capacity_remote_transport_budget');assert.deepEqual(f.requests,['/wait']);assert.equal((await f.request().result).success,true);const followup=f.request({path:'/wait',method:'POST',mode:'sse'});await until(()=>f.requests.length===3);await followup.cancel();assert.equal((await followup.result).success,false);await f.transport.close();
 }finally{await f.close();}
});

test('lease expiration cleans actual executor without a client connection',{timeout:30000},async()=>{
 const f=await fixture({connect:false,launcher:{leaseMs:500}});try{const status=await f.inspect();await until(()=>f.inspect(),value=>value.resourcesReleased===true);assert.equal(await alive(status.executor.pid),false);assert.deepEqual(f.requests,[]);}finally{await f.close();}
});

test('TLS client without a certificate is rejected before handshake session',{timeout:30000},async()=>{
 const f=await fixture({connect:false});let socket;try{
  socket=tls.connect({ca:certificates.ca,host:'127.0.0.1',port:Number(new URL(f.launcher.origin).port),rejectUnauthorized:true});socket.on('error',()=>{});await new Promise(resolve=>socket.once('close',resolve));assert.equal((await f.inspect()).sessionUsed,false);assert.deepEqual(f.requests,[]);await f.connect();assert.equal((await f.request().result).success,true);
 }finally{socket?.destroy();await f.close();}
});

test('launcher parent death closes real IPC descendants and their listener',{timeout:30000},async()=>{
 const f=await fixture({connect:false});let parent,proof;try{
  const source=`import{createOwnedRuntime}from'./tools/capacity/owned-runtime.mjs';import{createCapacityRemoteLauncher,capacityRemoteLauncherBinding}from'./tools/capacity/remote-launcher.mjs';process.once('message',async config=>{try{const runtime=createOwnedRuntime(),launcher=await createCapacityRemoteLauncher({runtime,...config}),binding=capacityRemoteLauncherBinding(launcher),state=await binding.inspect();process.send({origin:launcher.origin,endpoint:binding.child.pid,executor:state.executor.pid});}catch{process.exitCode=1;process.disconnect();}});`;
  parent=spawn(process.execPath,['--input-type=module','-e',source],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc'],env:{...process.env,NODE_OPTIONS:''}});
  const received=new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('parent readiness timeout')),5000);parent.once('message',value=>{clearTimeout(timer);resolve(value);});parent.once('error',reject);});
  parent.send({tls:serverTls,agents:[{id:'unused',origin:'https://127.0.0.1:1',tls:{caFile:clientTls.caFile},routes:[{method:'GET',path:'/health'}]}]});proof=await received;
  assert.equal(await alive(proof.endpoint),true);assert.equal(await alive(proof.executor),true);parent.kill('SIGKILL');await new Promise(resolve=>parent.once('exit',resolve));await until(async()=>!await alive(proof.endpoint)&&!await alive(proof.executor));
 }finally{if(parent?.exitCode===null&&parent?.signalCode===null)parent.kill('SIGKILL');await f.close();}
});

test('browser lifecycle uses fixed real Chromium and closes it on channel loss',{skip:!process.env.HARBOR_CAPACITY_REMOTE_BROWSER_MODULE,timeout:30000},async()=>{
 const f=await fixture({launcher:{browser:{module:process.env.HARBOR_CAPACITY_REMOTE_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_REMOTE_BROWSER_BINARY,maxBrowsers:1}}});try{
  const value=await f.transport.launchBrowser();assert.equal(value.scope,'owned-browser-lifecycle-only');const status=await f.inspect();assert.equal(status.execution.browsers.length,1);const browserPid=status.execution.browsers[0].pid;assert.equal(await alive(browserPid),true);await assert.rejects(f.transport.launchBrowser(),/command_rejected/);f.transport.disconnect();await until(()=>f.inspect(),value=>value.resourcesReleased===true);assert.equal(await alive(browserPid),false);
 }finally{await f.close();}
});

test('executor failure after HTTP dispatch remains unknown, never command rejection or retry',{timeout:30000},async()=>{
 const f=await fixture();try{const state=await f.inspect(),request=f.request({path:'/wait',method:'POST',mode:'sse'}),unknown=assert.rejects(request.result,error=>error.delivery==='unknown');await until(()=>f.requests.length);process.kill(state.executor.pid,'SIGKILL');await unknown;assert.deepEqual(f.requests,['/wait']);await until(async()=>!await alive(state.executor.pid));}
 finally{await assert.rejects(f.close(),/cleanup_unknown/);}
});

test('invalid frame limits reject endpoint creation before TLS materials or listener allocation',{timeout:30000},async()=>{
 for(const frames of [{maxFrameBytes:0},{maxBufferedBytes:1024},{controlReserveBytes:0}])await assert.rejects(createCapacityRemoteEndpoint({runId:randomUUID(),frames}),error=>error.code==='capacity_remote_configuration');
 await assert.rejects(fixture({launcher:{frames:{maxFrameBytes:0}}}),/remote_configuration/);
});

test('malformed close response settles request and close promises as unknown',{timeout:30000},async()=>{
 for(const operation of ['request','close']){
  let socket,transport,channel;const runId=randomUUID(),session=randomUUID();
  const server=tls.createServer({...certificates.server,ca:certificates.ca,requestCert:true,rejectUnauthorized:true},peer=>{socket=peer;channel=createCapacityRemoteFrames(peer,{onClose:()=>{},onFrame:message=>{if(message.kind==='hello')channel.send({kind:'ready',runId,session,scope:'authenticated-restricted-endpoint',challenge:message.challenge},{control:true});else channel.send({kind:'closed',runId,session,id:message.id,resourcesReleased:false},{control:true});return true;}});});
  try{await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));transport=await connectCapacityRemoteTransport({origin:'tls://127.0.0.1:'+server.address().port,runId,tls:clientTls,commandTimeoutMs:1000});const result=operation==='request'?transport.request({agentId:'test',path:'/health',method:'GET',mode:'json',timeoutMs:1000}).result:transport.close();await assert.rejects(result,error=>error.delivery==='unknown');assert.equal(transport.status().pending,0);}
  finally{transport?.disconnect();channel?.close();socket?.destroy();await new Promise(resolve=>server.close(resolve));}
 }
});

test('abrupt executor death cleans a previously proven browser without claiming graceful release',{skip:!process.env.HARBOR_CAPACITY_REMOTE_BROWSER_MODULE,timeout:30000},async()=>{
 const f=await fixture({launcher:{browser:{module:process.env.HARBOR_CAPACITY_REMOTE_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_REMOTE_BROWSER_BINARY,maxBrowsers:1}}});try{
  await f.transport.launchBrowser();const state=await f.inspect(),browserPid=state.execution.browsers[0].pid;assert.equal(await alive(browserPid),true);process.kill(state.executor.pid,'SIGKILL');await until(async()=>!await alive(browserPid));await until(()=>f.transport.status().closed);const final=await until(()=>f.inspect(),value=>value.executor.temporaryResourcesReleased);assert.equal(final.resourcesReleased,false);await assert.rejects(lstat(state.executor.temporaryDirectory),error=>error.code==='ENOENT');
 }finally{await assert.rejects(f.close(),/cleanup_unknown/);}
});

for(const browser of [false,true])test('repeated ordered close frames enter draining only once'+(browser?' during real Chromium startup':''),{skip:browser&&!process.env.HARBOR_CAPACITY_REMOTE_BROWSER_MODULE,timeout:30000},async()=>{
 const f=await fixture({connect:false,...(browser?{launcher:{browser:{module:process.env.HARBOR_CAPACITY_REMOTE_BROWSER_MODULE,binary:process.env.HARBOR_CAPACITY_REMOTE_BROWSER_BINARY,maxBrowsers:1}}}:{})});let socket,channel;try{
  const received=[];socket=tls.connect({...certificates.client,ca:certificates.ca,host:'127.0.0.1',port:Number(new URL(f.launcher.origin).port),rejectUnauthorized:true});socket.on('error',()=>{});channel=createCapacityRemoteFrames(socket,{onFrame:value=>{received.push(value);return true;},onClose:()=>{}});await new Promise(resolve=>socket.once('secureConnect',resolve));
  channel.send({kind:'hello',runId:f.launcher.runId,challenge:randomUUID()},{control:true});const ready=await until(()=>received.find(row=>row.kind==='ready')),base={runId:f.launcher.runId,session:ready.session};
  channel.send({...base,kind:browser?'browser-start':'request',sequence:1,id:randomUUID(),payload:browser?{}:{agentId:'test',path:'/wait',method:'POST',mode:'sse',timeoutMs:5000}},{control:true});
  for(let index=0;index<100;index++)channel.send({...base,kind:'close',sequence:index+2,id:randomUUID()},{control:true});
  await until(()=>channel.status().closed);const final=await until(()=>f.inspect(),value=>value.resourcesReleased);assert.equal(final.draining,true);assert.equal(final.closeTransitions,1);assert.equal(final.executor.temporaryResourcesReleased,true);
 }finally{channel?.close();socket?.destroy();await f.close();}
});


test('remote request driver consumes fixed slots with four native workloads, actual headers and mapped stage envelopes',{timeout:30000},async()=>{
 const bytes=Buffer.from([0,255,1,3,4]),csv='name,value\n"first\nline",1\nsecond,2\n',body=Buffer.from([0,255,90,10]);let receivedBody;
 const f=await fixture({routes:[{method:'GET',path:'/health'},{method:'POST',path:'/model'},{method:'POST',path:'/bytes'},{method:'GET',path:'/csv'}],handler:(req,res)=>{
  const chunks=[];req.on('data',value=>chunks.push(value));req.on('end',()=>{
   if(req.url==='/bytes'){receivedBody=Buffer.concat(chunks);res.end(bytes);return;}
   if(req.url==='/csv'){res.end(csv);return;}
   if(req.url==='/model'){res.setHeader('content-type','text/event-stream');res.setHeader('x-stage-test','headers');res.write('data: '+JSON.stringify({type:'response.output_text.delta',delta:'hello'})+'\n\n');setTimeout(()=>res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'remote',status:'completed'}})+'\n\n'),20);return;}
   res.end(JSON.stringify({ok:true}));
  });
 }});let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'alias',slot:'test',origin:f.target}]});assert.equal(isOwnedCapacityRequestDriver(driver),true);const agent=capacityRequestDriverAgent(driver,'alias'),request=overrides=>requestCapacityTarget({origin:f.target,agent,path:'/health',mode:'json',timeoutMs:5000,...overrides});
  assert.equal((await request()).json.ok,true);
  const stages=[],legacy=[],headers=[];const observer=createCapacityClientStageObserver({context:()=>({trace:'remote-test'}),observe:value=>legacy.push(value)});
  const result=await request({path:'/model',method:'POST',mode:'sse',headers:{'x-capacity-trace':'remote-test'},onResponseHeaders:value=>headers.push(value),clientStageObserver:observer,onRemoteStage:value=>stages.push(value)});observer.close();
  assert.equal(result.success,true);assert.equal(result.completedId,'remote');assert.equal(result.startedAt,null);assert.equal(result.finishedAt,null);assert.ok(result.durationMs>=0);assert.equal(result.driverTiming.controllerTimesKnown,false);assert.equal(result.driverTiming.formalQualified,false);
  assert.equal(headers[0]['x-stage-test'],'headers');assert.ok(stages.some(value=>value.type==='content'));assert.ok(stages.some(value=>value.type==='end'));assert.ok(legacy.every(value=>value.type==='unavailable'));assert.equal(result.driverObservationDropped,0);
  for(const event of result.remoteObservations.filter(row=>row.kind==='stage')){assert.equal(event.bounds.known,true);assert.equal(event.bounds.formalQualified,false);assert.equal(event.clockEpoch,result.remoteTiming.clockEpoch);assert.ok(BigInt(event.bounds.lowerNs)<=BigInt(event.bounds.upperNs));}
  const downloaded=await request({path:'/bytes',method:'POST',mode:'download',body});assert.equal(downloaded.receivedBytes,bytes.length);assert.deepEqual(receivedBody,body);assert.equal(downloaded.bodyHash,remoteHash(bytes));
  const exported=await request({path:'/csv',mode:'download',rowFormat:'csv',csvHeader:true});assert.equal(exported.records,2);assert.equal(exported.receivedBytes,Buffer.byteLength(csv));
  assert.equal(driver.status().runtimeBindingVerified,false);assert.ok(driver.status().missing.includes('large_body_transfer_unproven'));assert.equal(driver.status().observationBytes,0);assert.ok(driver.status().observationPeakBytes>0);
  await assert.rejects(driver.addAgent({id:'evil',origin:'https://example.invalid',slot:'test'}),/remote_agent_invalid/);await assert.rejects(driver.addAgent({id:'evil',origin:f.target,slot:'test',tls:{keyFile:'/tmp/evil'}}),/remote_agent_invalid/);
  const second=await driver.addAgent({id:'second',origin:f.target,slot:'test'});assert.equal((await requestCapacityTarget({origin:f.target,agent:second,path:'/health',timeoutMs:5000})).success,true);
  await driver.close();assert.equal(driver.status().resourcesReleased,true);
 }finally{await driver?.close();await f.close();}
});

test('normal remote driver close stops admission and drains accepted request to actual HTTP end',{timeout:30000},async()=>{
 let finish;const f=await fixture({handler:(req,res)=>{req.resume();finish=()=>res.end(JSON.stringify({ok:true}));}});let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');const result=requestCapacityTarget({origin:f.target,agent,path:'/health',timeoutMs:5000});await until(()=>finish);let closed=false;const closing=driver.close().then(()=>{closed=true;});await assert.rejects(requestCapacityTarget({origin:f.target,agent,path:'/health',timeoutMs:5000}),/driver_closed/);assert.equal(closed,false);finish();assert.equal((await result).httpEnded,true);await closing;assert.deepEqual(f.requests,['/health']);
 }finally{await driver?.close();await f.close();}
});

test('remote driver cancellation and agent destroy wait for terminal without replay',{timeout:30000},async()=>{
 for(const cancel of ['signal','destroy']){
  const f=await fixture();let driver;try{
   driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test'),controller=new AbortController();
   const running=requestCapacityTarget({origin:f.target,agent,path:'/wait',method:'POST',mode:'sse',timeoutMs:5000,signal:controller.signal});await until(()=>f.requests.length);
   if(cancel==='signal')controller.abort();else agent.destroy();
   const value=await running;assert.equal(value.success,false);assert.equal(value.httpEnded,false);assert.equal(driver.status().pending,0);assert.equal(driver.status().requestBytes,0);assert.deepEqual(f.requests,['/wait']);
   if(cancel==='destroy')await assert.rejects(requestCapacityTarget({origin:f.target,agent,path:'/health',timeoutMs:5000}),/driver_closed/);
  }finally{await driver?.close();await f.close();}
 }
});

test('remote driver observation budget declares loss and never mislabels incomplete transcript complete',{timeout:30000},async()=>{
 const f=await fixture({launcher:{frames:{maxFrameBytes:8192,maxBufferedBytes:16384,controlReserveBytes:4096}},routes:[{method:'POST',path:'/many'}],handler:(req,res)=>{req.resume();res.setHeader('content-type','text/event-stream');for(let i=0;i<100;i++)res.write('data: '+JSON.stringify({type:'response.output_text.delta',delta:'delta '+i})+'\n\n');res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'many',status:'completed'}})+'\n\n');}});let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],maxFrameBytes:4096,maxBufferedBytes:8192});const agent=capacityRequestDriverAgent(driver,'test');
  const result=await requestCapacityTarget({origin:f.target,agent,path:'/many',method:'POST',mode:'sse',headers:{'x-capacity-trace':'bounded'},timeoutMs:5000});
  assert.equal(result.success,true);assert.ok(result.driverObservationDropped>0);assert.ok(result.remoteObservations.length<100);assert.ok(driver.status().observationPeakBytes<=8192);assert.equal(driver.status().observationBytes,0);assert.equal(driver.status().requestBytes,0);
 }finally{await driver?.close();await f.close();}
});

test('remote driver control loss reports unknown, retains no fabricated finish and never retries',{timeout:30000},async()=>{
 const f=await fixture();let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');
  const running=requestCapacityTarget({origin:f.target,agent,path:'/wait',method:'POST',mode:'sse',timeoutMs:5000}),unknown=assert.rejects(running,error=>error.delivery==='unknown');await until(()=>f.requests.length);f.transport.disconnect();await unknown;
  assert.equal(driver.status().pending,0);assert.equal(driver.status().requestBytes,0);assert.deepEqual(f.requests,['/wait']);await assert.rejects(driver.close(),/cleanup_unknown/);
 }finally{await driver?.close().catch(()=>{});await f.close();}
});


test('direct transport close drains a quiet accepted request beyond the normal idle lease',{timeout:30000},async()=>{
 const f=await fixture({launcher:{leaseMs:500,limits:{maxRequestTimeoutMs:3000}},transport:{heartbeatMs:100},handler:(req,res)=>{req.resume();setTimeout(()=>res.end(JSON.stringify({ok:true})),900);}});try{
  const request=f.request({timeoutMs:2500});await until(()=>f.requests.length);const closing=f.transport.close();assert.equal((await request.result).success,true);assert.deepEqual(await closing,{resourcesReleased:true});assert.deepEqual(f.requests,['/health']);
 }finally{await f.close();}
});

test('driver signal still interrupts a normal drain and leaves cleanup unknown when connection is lost',{timeout:30000},async()=>{
 const f=await fixture(),controller=new AbortController();let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],signal:controller.signal});const agent=capacityRequestDriverAgent(driver,'test');
  const running=requestCapacityTarget({origin:f.target,agent,path:'/wait',method:'POST',mode:'sse',timeoutMs:5000}),unknown=assert.rejects(running,error=>error.delivery==='unknown');await until(()=>f.requests.length);const closing=driver.close(),cleanup=assert.rejects(closing,/cleanup_unknown/);controller.abort();await unknown;await cleanup;assert.equal(driver.status().closed,true);assert.equal(driver.status().resourcesReleased,false);assert.deepEqual(f.requests,['/wait']);
 }finally{await driver?.close().catch(()=>{});await f.close();}
});

test('remote request default deadline comes from authenticated endpoint limits',{timeout:30000},async()=>{
 const f=await fixture({launcher:{limits:{maxRequestTimeoutMs:1000}}});let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');assert.equal((await requestCapacityTarget({origin:f.target,agent,path:'/health'})).success,true);await assert.rejects(requestCapacityTarget({origin:f.target,agent,path:'/health',timeoutMs:1001}),/http_configuration/);assert.deepEqual(f.requests,['/health']);
 }finally{await driver?.close();await f.close();}
});

test('held async observer sinks stay globally bounded across sequential completed HTTP requests',{timeout:30000},async()=>{
 const f=await fixture();let driver,release;const held=new Promise(resolve=>{release=resolve;});let calls=0;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],maxInFlight:2});const agent=capacityRequestDriverAgent(driver,'test'),results=[];
  for(let index=0;index<6;index++)results.push(await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>{calls++;return held;}}));
  const status=driver.status();assert.equal(status.pending,0);assert.equal(status.requestBytes,0);assert.equal(status.pendingCallbacks,2);assert.equal(calls,2);assert.ok(status.callbackBytes>0);assert.ok(status.callbackBytes<=8388608);assert.equal(results[0].driverObservationComplete,false);assert.equal(results[0].driverObservationPending,1);assert.ok(results.slice(2).every(value=>value.driverObservationDropped>0));assert.ok(status.observationDropped>=4);
  let closed=false;const closing=driver.close().then(()=>{closed=true;});assert.equal(closed,false);release();await closing;assert.equal(driver.status().pendingCallbacks,0);assert.equal(driver.status().callbackBytes,0);assert.equal(driver.status().resourcesReleased,true);
 }finally{release();await driver?.close().catch(()=>{});await f.close();}
});

test('observer callback is registered before invoking a reentrant close and normal close waits its actual settlement',{timeout:30000},async()=>{
 const f=await fixture();let driver,release,closing,registered=false,closeDone=false;const held=new Promise(resolve=>{release=resolve;});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');
  const value=await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>{registered=driver.status().pendingCallbacks===1;closing=driver.close().then(()=>{closeDone=true;});return held;}});
  assert.equal(registered,true);assert.equal(value.driverObservationComplete,false);assert.equal(closeDone,false);release();await closing;assert.equal(driver.status().pendingCallbacks,0);assert.equal(driver.status().resourcesReleased,true);
 }finally{release();await closing?.catch(()=>{});await driver?.close().catch(()=>{});await f.close();}
});

test('synchronous observer success, false, throws and rejected thenables have explicit loss accounting',{timeout:30000},async()=>{
 const f=await fixture();let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');
  for(const sink of [()=>false,()=>{throw Error('test sink');},()=>Promise.reject(Error('test async sink'))]){
   const result=await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:sink});assert.equal(result.success,true);assert.ok(result.driverObservationDropped>0);assert.equal(result.driverObservationComplete,false);
  }
  const result=await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>true});assert.equal(result.driverObservationComplete,true);assert.equal(result.driverObservationPending,0);assert.equal(driver.status().pendingCallbacks,0);assert.equal(driver.status().callbackBytes,0);assert.equal(driver.status().callbackFailures,3);
 }finally{await driver?.close().catch(()=>{});await f.close();}
});

test('observer cleanup timeout keeps actual pending callback ownership and can complete after real settlement',{timeout:30000},async()=>{
 const f=await fixture();let driver,release;const held=new Promise(resolve=>{release=resolve;});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],closeTimeoutMs:100});const agent=capacityRequestDriverAgent(driver,'test');
  const result=await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>held});assert.equal(result.driverObservationComplete,false);
  await assert.rejects(driver.close(),error=>error.message==='capacity_driver_callback_cleanup_unknown'&&error.delivery==='unknown'&&error.pendingCallbacks===1);
  const failed=driver.status();assert.equal(failed.pendingCallbacks,1);assert.ok(failed.callbackBytes>0);assert.equal(failed.resourcesReleased,false);assert.equal(failed.executionResourcesReleased,true);
  release();await until(()=>driver.status().pendingCallbacks===0);await driver.close();assert.equal(driver.status().callbackBytes,0);assert.equal(driver.status().resourcesReleased,true);
 }finally{release();await driver?.close().catch(()=>{});await f.close();}
});

test('signal interrupts held observer drain without declaring callback cleanup completed',{timeout:30000},async()=>{
 const f=await fixture(),controller=new AbortController();let driver,release;const held=new Promise(resolve=>{release=resolve;});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],signal:controller.signal});const agent=capacityRequestDriverAgent(driver,'test');
  await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>held});const closing=driver.close(),unknown=assert.rejects(closing,error=>error.message==='capacity_driver_callback_cleanup_unknown'&&error.delivery==='unknown');controller.abort();await unknown;
  assert.equal(driver.status().pendingCallbacks,1);assert.ok(driver.status().callbackBytes>0);assert.equal(driver.status().resourcesReleased,false);release();await until(()=>driver.status().pendingCallbacks===0);
 }finally{release();await driver?.close().catch(()=>{});await f.close();}
});

test('held remote stage callbacks share the same global budget as response header callbacks',{timeout:30000},async()=>{
 const f=await fixture({routes:[{method:'GET',path:'/events'}],handler:(req,res)=>{req.resume();res.setHeader('content-type','text/event-stream');res.end('data: '+JSON.stringify({type:'response.output_text.delta',delta:'first'})+'\n\ndata: '+JSON.stringify({type:'response.completed',response:{status:'completed',id:'stage'}})+'\n\n');}});let driver,release;const held=new Promise(resolve=>{release=resolve;});let stageCalls=0;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],maxInFlight:1});const agent=capacityRequestDriverAgent(driver,'test');
  for(let index=0;index<4;index++){const value=await requestCapacityTarget({origin:f.target,agent,path:'/events',mode:'sse',headers:{'x-capacity-trace':'held-stage'},onRemoteStage:()=>{stageCalls++;return held;}});assert.equal(value.driverObservationComplete,false);}
  assert.equal(stageCalls,1);assert.equal(driver.status().pendingCallbacks,1);assert.ok(driver.status().callbackBudgetRejected>0);assert.ok(driver.status().callbackBytes>0);release();await driver.close();assert.equal(driver.status().pendingCallbacks,0);
 }finally{release();await driver?.close().catch(()=>{});await f.close();}
});

test('async observer byte budget stays retained across requests even below callback count ceiling',{timeout:30000},async()=>{
 const f=await fixture({handler:(req,res)=>{req.resume();res.setHeader('x-retained', 'x'.repeat(2500));res.end(JSON.stringify({ok:true}));}});let driver,release;const held=new Promise(resolve=>{release=resolve;});let calls=0;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],maxInFlight:16,maxFrameBytes:4096,maxBufferedBytes:8192});const agent=capacityRequestDriverAgent(driver,'test');
  for(let index=0;index<8;index++)await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>{calls++;return held;}});
  const status=driver.status();assert.ok(calls>0&&calls<8);assert.equal(status.pendingCallbacks,calls);assert.ok(status.pendingCallbacks<status.maxPendingCallbacks);assert.ok(status.callbackBytes>2500&&status.callbackBytes<=8192);assert.ok(status.callbackBudgetRejected>0);assert.equal(status.observationBytes,0);release();await driver.close();assert.equal(driver.status().callbackBytes,0);
 }finally{release();await driver?.close().catch(()=>{});await f.close();}
});

test('observer rejection after HTTP completion remains visible and releases its retained quota only on settlement',{timeout:30000},async()=>{
 const f=await fixture();let driver,reject;const held=new Promise((resolve,no)=>{reject=no;});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');
  const result=await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>held});assert.equal(result.driverObservationPending,1);assert.equal(result.driverObservationComplete,false);assert.equal(driver.status().callbackFailures,0);assert.ok(driver.status().callbackBytes>0);
  reject(Error('late synthetic sink failure'));await until(()=>driver.status().pendingCallbacks===0);assert.equal(driver.status().callbackBytes,0);assert.equal(driver.status().callbackFailures,1);assert.equal(driver.status().observationDropped,1);await driver.close();assert.equal(driver.status().resourcesReleased,true);
 }finally{reject(Error('cleanup'));await driver?.close().catch(()=>{});await f.close();}
});

test('repeated timed-out and already-aborted callback cleanup removes every wait subscription',{timeout:30000},async()=>{
 for(const cancelled of [false,true]){
  const f=await fixture(),controller=new AbortController();let driver,release;const held=new Promise(resolve=>{release=resolve;});try{
   driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],signal:controller.signal,closeTimeoutMs:100});const agent=capacityRequestDriverAgent(driver,'test');
   await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>held});
   for(let attempt=0;attempt<5;attempt++){
    const closing=driver.close(),unknown=assert.rejects(closing,/callback_cleanup_unknown/);if(cancelled)controller.abort();await unknown;
    const status=driver.status();assert.equal(status.pendingCallbacks,1);assert.equal(status.callbackWaiters,0);assert.ok(status.callbackBytes>0);assert.equal(status.resourcesReleased,false);
   }
   release();await until(()=>driver.status().pendingCallbacks===0);assert.equal(driver.status().callbackWaiters,0);assert.equal(driver.status().callbackBytes,0);
   if(cancelled)await assert.rejects(driver.close(),/cleanup_unknown/);else{await driver.close();assert.equal(driver.status().resourcesReleased,true);}
  }finally{release();await driver?.close().catch(()=>{});await f.close();}
 }
});

test('observer returning its reentrant close is bounded unknown rather than a false successful cleanup',{timeout:30000},async()=>{
 const f=await fixture();let driver,closing;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],closeTimeoutMs:100});const agent=capacityRequestDriverAgent(driver,'test');
  const value=await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>closing=driver.close()});assert.equal(value.driverObservationComplete,false);await assert.rejects(closing,/callback_cleanup_unknown/);
  await until(()=>driver.status().pendingCallbacks===0);assert.equal(driver.status().callbackFailures,1);assert.equal(driver.status().callbackWaiters,0);await driver.close();assert.equal(driver.status().resourcesReleased,true);
 }finally{await closing?.catch(()=>{});await driver?.close().catch(()=>{});await f.close();}
});

test('signal remains attached after callback cleanup timeout and interrupts an actual retry waiter',{timeout:30000},async()=>{
 const f=await fixture(),controller=new AbortController();let driver,release;const held=new Promise(resolve=>{release=resolve;});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],signal:controller.signal,closeTimeoutMs:300});const agent=capacityRequestDriverAgent(driver,'test');
  await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:()=>held});await assert.rejects(driver.close(),/callback_cleanup_unknown/);assert.equal(driver.status().executionResourcesReleased,true);
  const closing=driver.close(),unknown=assert.rejects(closing,/callback_cleanup_unknown/);await until(()=>driver.status().callbackWaiters===1);controller.abort();assert.equal(driver.status().callbackWaiters,0);await unknown;assert.equal(driver.status().pendingCallbacks,1);assert.ok(driver.status().callbackBytes>0);assert.equal(driver.status().resourcesReleased,false);
  release();await until(()=>driver.status().pendingCallbacks===0);await driver.close();assert.equal(driver.status().resourcesReleased,true);
 }finally{release();await driver?.close().catch(()=>{});await f.close();}
});

test('legacy client-stage observer thenables reach remote quota and retain original capability statistics',{timeout:30000},async()=>{
 const f=await fixture(),stages=[];let driver,release;const held=new Promise(resolve=>{release=resolve;});const observer=createCapacityClientStageObserver({context:()=>({trace:'legacy-held'}),observe:value=>{stages.push(value);return held;}});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],maxInFlight:2,maxFrameBytes:4096,maxBufferedBytes:16384});const agent=capacityRequestDriverAgent(driver,'test'),results=[];
  for(let index=0;index<6;index++)results.push(await requestCapacityTarget({origin:f.target,agent,path:'/health',headers:{'x-capacity-trace':'legacy-held'},clientStageObserver:observer}));
  assert.equal(driver.status().pendingCallbacks,2);assert.equal(stages.length,2);assert.equal(observer.status().pendingCallbacks,2);assert.ok(stages.every(value=>value.type==='unavailable'));assert.ok(results.every(value=>value.driverObservationComplete===false));assert.ok(driver.status().callbackBudgetRejected>0);assert.ok(driver.status().callbackBytes>0);
  release();await driver.close();assert.equal(observer.status().pendingCallbacks,0);assert.equal(observer.status().failures,0);assert.equal(driver.status().pendingCallbacks,0);assert.equal(driver.status().resourcesReleased,true);
 }finally{release();await driver?.close().catch(()=>{});observer.close();await f.close();}
});

test('legacy client-stage observer keeps its failure count while remote marks rejected observation missing',{timeout:30000},async()=>{
 const f=await fixture();let driver;const observer=createCapacityClientStageObserver({context:()=>({trace:'legacy-rejected'}),observe:()=>Promise.reject(Error('synthetic legacy failure'))});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');
  const value=await requestCapacityTarget({origin:f.target,agent,path:'/health',headers:{'x-capacity-trace':'legacy-rejected'},clientStageObserver:observer});assert.equal(value.success,true);assert.equal(value.driverObservationComplete,false);assert.ok(value.driverObservationDropped>0);assert.ok(observer.status().failures>0);assert.ok(driver.status().callbackFailures>0);await driver.close();assert.equal(observer.status().pendingCallbacks,0);
 }finally{await driver?.close().catch(()=>{});observer.close();await f.close();}
});

test('legacy failure observation also uses remote lifetime quota after rejected HTTP dispatch',{timeout:30000},async()=>{
 const f=await fixture(),events=[];let driver,release;const held=new Promise(resolve=>{release=resolve;});const observer=createCapacityClientStageObserver({context:()=>({trace:'legacy-failed-held'}),observe:value=>{events.push(value);return held;}});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],maxInFlight:8,maxFrameBytes:4096,maxBufferedBytes:8192});const agent=capacityRequestDriverAgent(driver,'test');
  for(let index=0;index<6;index++)await assert.rejects(requestCapacityTarget({origin:f.target,agent,path:'/not-approved',headers:{'x-capacity-trace':'legacy-failed-held'},clientStageObserver:observer}),/command_rejected/);
  assert.equal(events.length,2);assert.ok(events.every(value=>value.type==='failed'));assert.equal(observer.status().pendingCallbacks,2);assert.equal(driver.status().pendingCallbacks,2);assert.equal(driver.status().callbackBytes,8192);assert.ok(driver.status().callbackBudgetRejected>=4);assert.deepEqual(f.requests,[]);
  release();await driver.close();assert.equal(observer.status().pendingCallbacks,0);assert.equal(driver.status().callbackBytes,0);assert.equal(driver.status().resourcesReleased,true);
 }finally{release();await driver?.close().catch(()=>{});observer.close();await f.close();}
});

test('legacy normal stage reserves its complete private envelope against the retained byte ceiling',{timeout:30000},async()=>{
 const f=await fixture(),events=[];let driver,release;const held=new Promise(resolve=>{release=resolve;});const observer=createCapacityClientStageObserver({context:()=>({trace:'legacy-envelope-budget'}),observe:value=>{events.push(value);return held;}});try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}],maxInFlight:32,maxFrameBytes:1024,maxBufferedBytes:4096});const agent=capacityRequestDriverAgent(driver,'test');
  for(let index=0;index<32;index++)await requestCapacityTarget({origin:f.target,agent,path:'/health',headers:{'x-capacity-trace':'legacy-envelope-budget'},clientStageObserver:observer});
  const status=driver.status(),actualBytes=events.reduce((sum,event)=>sum+Buffer.byteLength(JSON.stringify(event)),0);assert.ok(events.length>0&&events.length<=4);assert.equal(status.pendingCallbacks,events.length);assert.ok(actualBytes<=status.callbackBytes);assert.ok(status.callbackBytes<=4096);assert.ok(status.callbackBudgetRejected>0);assert.equal(status.observationBytes,0);release();await driver.close();assert.equal(observer.status().pendingCallbacks,0);assert.equal(driver.status().callbackBytes,0);
 }finally{release();await driver?.close().catch(()=>{});observer.close();await f.close();}
});

test('initial and dynamic aliases that shadow object prototypes retain accurate numeric request statistics',{timeout:30000},async()=>{
 const f=await fixture({routes:[{method:'GET',path:'/health'},{method:'POST',path:'/v1/responses'}],handler:(req,res)=>{req.resume();if(req.url==='/v1/responses'){res.setHeader('content-type','text/event-stream');res.end('data: '+JSON.stringify({type:'response.output_text.delta',delta:'content'})+'\n\ndata: '+JSON.stringify({type:'response.completed',response:{status:'completed',id:'alias'}})+'\n\n');}else res.end(JSON.stringify({ok:true}));}});let driver;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'__proto__',slot:'test',origin:f.target},{id:'constructor',slot:'test',origin:f.target}]});const agents=new Map([['__proto__',capacityRequestDriverAgent(driver,'__proto__')],['constructor',capacityRequestDriverAgent(driver,'constructor')],['toString',await driver.addAgent({id:'toString',slot:'test',origin:f.target})]]);
  for(const [alias,agent] of agents){for(let index=0;index<2;index++)assert.equal((await requestCapacityTarget({origin:f.target,agent,path:'/health'})).success,true);assert.equal((await requestCapacityTarget({origin:f.target,agent,path:'/v1/responses',method:'POST',mode:'sse'})).success,true);const status=driver.status();assert.equal(Object.hasOwn(status.executedRequestsByAgent,alias),true);assert.equal(status.executedRequestsByAgent[alias],3);assert.equal(status.executedModelRequestsByAgent[alias],1);}
 }finally{await driver?.close().catch(()=>{});await f.close();}
});

test('remote private response headers never enter result DTO and invalid callback types reject before dispatch',{timeout:30000},async()=>{
 const secret='synthetic-private-remote-cookie',f=await fixture({handler:(req,res)=>{req.resume();res.setHeader('set-cookie','harbor_session='+secret+'; HttpOnly');res.end(JSON.stringify({ok:true}));}});let driver,captured;try{
  driver=await createCapacityRequestDriver({remote:{transport:f.transport},agentPlans:[{id:'test',origin:f.target}]});const agent=capacityRequestDriverAgent(driver,'test');
  const result=await requestCapacityTarget({origin:f.target,agent,path:'/health',onResponseHeaders:headers=>{captured=headers['set-cookie'][0];}});assert.ok(captured.includes(secret));assert.equal(JSON.stringify(result).includes(secret),false);assert.equal(JSON.stringify(result).includes('set-cookie'),false);assert.ok(result.remoteObservations.some(event=>event.kind==='headers'&&!Object.hasOwn(event,'value')));
  for(const mutation of [{onResponseHeaders:'invalid'},{onRemoteStage:'invalid'}])await assert.rejects(requestCapacityTarget({origin:f.target,agent,path:'/health',...mutation}),/http_configuration/);assert.deepEqual(f.requests,['/health']);
 }finally{await driver?.close().catch(()=>{});await f.close();}
});
