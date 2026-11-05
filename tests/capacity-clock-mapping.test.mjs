import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import https from 'node:https';
import tls from 'node:tls';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,X509Certificate} from 'node:crypto';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createCapacityRemoteLauncher} from '../tools/capacity/remote-launcher.mjs';
import {connectCapacityRemoteTransport,capacityRemoteTransportBinding,capacityRemoteClockSampleBinding} from '../tools/capacity/remote-transport.mjs';
import {createCapacityClockMapping,capacityClockMappingBinding} from '../tools/capacity/clock-mapping.mjs';
import {createCapacityRemoteFrames,remoteHash} from '../tools/capacity/remote-protocol.mjs';

let directory,certificates,serverTls,clientTls;
before(async()=>{directory=await mkdtemp(join(tmpdir(),'harbor-clock-test-'));certificates=await syntheticCertificates(directory);serverTls={caFile:join(directory,'ca.pem'),certFile:join(directory,'server.pem'),keyFile:join(directory,'server.key'),peerCertificateHash:remoteHash(new X509Certificate(certificates.client.cert).raw)};clientTls={caFile:join(directory,'ca.pem'),certFile:join(directory,'client.pem'),keyFile:join(directory,'client.key'),peerCertificateHash:remoteHash(new X509Certificate(certificates.server.cert).raw)};});
after(()=>rm(directory,{recursive:true,force:true}));

async function owned(t){
 const runtime=createOwnedRuntime(),sockets=new Set(),clientConfiguration={...clientTls};let launcher,transport,requests=0;
 const server=https.createServer({...certificates.server,ca:certificates.ca,requestCert:true,rejectUnauthorized:true},(req,res)=>{requests++;req.resume();res.setHeader('content-type','application/json');res.end('{"ok":true}');});
 server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
 t.after(async()=>{if(transport&&!transport.status().closed)await transport.close().catch(()=>{});await launcher?.close();for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));await runtime.close();});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));launcher=await createCapacityRemoteLauncher({runtime,tls:serverTls,agents:[{id:'test',origin:'https://127.0.0.1:'+server.address().port,tls:{caFile:clientTls.caFile,certFile:clientTls.certFile,keyFile:clientTls.keyFile},routes:[{method:'GET',path:'/health'}]}]});transport=await connectCapacityRemoteTransport({origin:launcher.origin,runId:launcher.runId,tls:clientConfiguration});return {transport,clientConfiguration,requests:()=>requests};
}

async function fake(t,{features=['clock-sampling-v1'],mutate=()=>{},hold=false,onHello=()=>{},clientConfiguration={...clientTls}}={}){
 const runId=randomUUID(),session=randomUUID(),epoch=randomUUID(),sockets=new Set();let transport,channel,count=0,seenResolve,release;const seen=new Promise(resolve=>{seenResolve=resolve;});
 const server=tls.createServer({...certificates.server,ca:certificates.ca,requestCert:true,rejectUnauthorized:true},socket=>{
  sockets.add(socket);socket.on('error',()=>{});socket.once('close',()=>sockets.delete(socket));
  channel=createCapacityRemoteFrames(socket,{onClose:()=>{},onFrame:message=>{
   if(message.kind==='hello'){onHello();channel.send({kind:'ready',runId,session,challenge:message.challenge,scope:'authenticated-restricted-endpoint',features},{control:true});}
   else if(message.kind==='ping')channel.send({kind:'pong',runId,session,id:message.id},{control:true});
   else if(message.kind==='clock'){
    const value={schema:'capacity-clock-sample-v1',clockEpoch:epoch,receivedNs:'1',sentNs:'2'};mutate(value,++count);release=()=>channel.send({kind:'clock-result',runId,session,id:message.id,value},{control:true});seenResolve();if(!hold)release();
   }else if(message.kind==='close'){channel.send({kind:'closed',runId,session,id:message.id,resourcesReleased:true},{control:true});channel.end();}
   else channel.send({kind:'result',runId,session,id:message.id,value:{ok:true}},{control:true});return true;
  }});
 });
 t.after(async()=>{transport?.disconnect();channel?.close();for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));transport=await connectCapacityRemoteTransport({origin:'tls://127.0.0.1:'+server.address().port,runId,tls:clientConfiguration,commandTimeoutMs:1000});return {transport,seen,release:()=>release(),count:()=>count};
}

test('real owned TLS endpoint collects its actual four-timestamp transcript without accepting caller timestamps or claiming a formal host',{timeout:30000},async t=>{
 const f=await owned(t),before=process.hrtime.bigint(),sample=await f.transport.clockSample(),after=process.hrtime.bigint(),row=capacityRemoteClockSampleBinding(sample);
 assert.ok(row);assert.ok(row.controllerSentNs>=before&&row.controllerReceivedNs<=after);assert.ok(row.controllerSentNs<=row.remoteReceivedNs&&row.remoteReceivedNs<=row.remoteSentNs&&row.remoteSentNs<=row.controllerReceivedNs,'same-kernel diagnostic only');assert.equal(row.formalQualified,false);assert.equal(f.requests(),0);
 assert.equal(capacityRemoteClockSampleBinding({...sample,...row}),null);assert.equal(capacityRemoteClockSampleBinding({receivedNs:'1',sentNs:'2'}),null);assert.ok(capacityRemoteTransportBinding(f.transport).missing.includes('cross_machine_clock_mapping_unproven'));
 await assert.rejects(async()=>f.transport.clockSample({receivedNs:'1'}),/configuration/);await assert.rejects(async()=>f.transport.clockSample({timeoutMs:99}),/configuration/);
 assert.equal((await f.transport.request({agentId:'test',path:'/health',method:'GET',mode:'json',timeoutMs:1000}).result).success,true);
});

test('conservative bounds use actual preceding and following samples, with no midpoint or unobserved extrapolation',{timeout:30000},async t=>{
 const f=await owned(t),mapping=createCapacityClockMapping({transport:f.transport});t.after(()=>mapping.close());const before=process.hrtime.bigint(),first=await mapping.sample(),middle=capacityRemoteClockSampleBinding(await f.transport.clockSample());await mapping.sample();const after=process.hrtime.bigint();
 const result=mapping.bounds({timestampNs:middle.remoteSentNs.toString(),clockEpoch:first.clockEpoch});assert.equal(result.known,true);assert.ok(BigInt(result.lowerNs)>=before);assert.ok(BigInt(result.lowerNs)<=middle.remoteSentNs&&BigInt(result.upperNs)>=middle.remoteSentNs);assert.ok(BigInt(result.upperNs)<=after);assert.equal(BigInt(result.widthNs),BigInt(result.upperNs)-BigInt(result.lowerNs));assert.equal(result.eventSourceVerified,false);assert.equal(result.formalQualified,false);
 assert.equal(mapping.bounds({timestampNs:'0',clockEpoch:first.clockEpoch}).known,false);assert.equal(mapping.bounds({timestampNs:(after+1000000000000000000n).toString(),clockEpoch:first.clockEpoch}).known,false);assert.equal(mapping.bounds({timestampNs:middle.remoteSentNs.toString(),clockEpoch:randomUUID()}).reason,'clock_epoch_mismatch');
 assert.throws(()=>createCapacityClockMapping({transport:{...f.transport}}),/configuration/);assert.equal(capacityClockMappingBinding({...mapping}),null);
 assert.throws(()=>createCapacityClockMapping({transport:f.transport,now:()=>0}),/configuration/);
});

test('authenticated equal-tick samples never prove their own causal order',{timeout:15000},async t=>{
 const flat=await fake(t,{mutate:value=>{value.receivedNs=value.sentNs='10';}}),mapping=createCapacityClockMapping({transport:flat.transport});t.after(()=>mapping.close());const first=await mapping.sample();assert.equal(mapping.bounds({timestampNs:'10',clockEpoch:first.clockEpoch}).reason,'timestamp_not_bracketed');await mapping.sample();assert.equal(mapping.status().samples,2);assert.equal(mapping.bounds({timestampNs:'10',clockEpoch:first.clockEpoch}).known,false);
 const ordered=await fake(t,{mutate:(value,count)=>{value.receivedNs=value.sentNs=count===1?'8':'12';}}),bracket=createCapacityClockMapping({transport:ordered.transport});t.after(()=>bracket.close());const before=await bracket.sample();await bracket.sample();assert.equal(bracket.bounds({timestampNs:'10',clockEpoch:before.clockEpoch}).known,true);for(const timestampNs of ['8','12'])assert.equal(bracket.bounds({timestampNs,clockEpoch:before.clockEpoch}).known,false);
});

test('certificate identity stays bound to the actual authentication, even if caller configuration changes before ready or after connect',{timeout:30000},async t=>{
 const f=await owned(t),actual=clientTls.peerCertificateHash;f.clientConfiguration.peerCertificateHash='0'.repeat(64);const first=capacityRemoteClockSampleBinding(await f.transport.clockSample());assert.equal(first.endpointCertificateHash,actual);assert.equal(first.transportBinding.endpointCertificateHash,actual);
 const configuration={...clientTls},peer=await fake(t,{clientConfiguration:configuration,onHello:()=>{configuration.peerCertificateHash='f'.repeat(64);}}),second=capacityRemoteClockSampleBinding(await peer.transport.clockSample());assert.equal(second.endpointCertificateHash,actual);assert.equal(capacityRemoteTransportBinding(peer.transport).endpointCertificateHash,actual);
});

test('sample count, retention age and closed transport all revoke unsupported clock bounds',{timeout:30000},async t=>{
 const f=await owned(t),mapping=createCapacityClockMapping({transport:f.transport,maxSamples:2});t.after(()=>mapping.close());const first=await mapping.sample(),middle=capacityRemoteClockSampleBinding(await f.transport.clockSample());await mapping.sample();assert.equal(mapping.bounds({timestampNs:middle.remoteSentNs.toString(),clockEpoch:first.clockEpoch}).known,true);await mapping.sample();await mapping.sample();assert.equal(mapping.status().samples,2);assert.ok(mapping.status().evicted>=2);assert.equal(mapping.bounds({timestampNs:middle.remoteSentNs.toString(),clockEpoch:first.clockEpoch}).known,false);
 const future=process.hrtime.bigint()+BigInt(mapping.status().sampleMaxAgeMs+1)*1000000n;t.mock.method(process.hrtime,'bigint',()=>future);try{assert.equal(mapping.status().samples,0);}finally{t.mock.restoreAll();}
 f.transport.disconnect();assert.equal(capacityRemoteClockSampleBinding(middle),null);assert.equal(capacityClockMappingBinding(mapping),null);assert.equal(mapping.bounds({timestampNs:middle.remoteSentNs.toString(),clockEpoch:first.clockEpoch}).known,false);await assert.rejects(mapping.sample(),/closed/);
});

test('legacy peers without clock negotiation keep ordinary requests and reject new sampling before sending',{timeout:15000},async t=>{
 const f=await fake(t,{features:[]});await assert.rejects(f.transport.clockSample(),error=>error.code==='capacity_remote_clock_unsupported'&&error.delivery==='not-sent');assert.equal(f.count(),0);assert.equal((await f.transport.request({agentId:'test',path:'/health',method:'GET',timeoutMs:1000}).result).ok,true);assert.equal(f.transport.status().closed,false);
});

test('malformed timestamps and changed clock epochs revoke every sample on that authenticated session',{timeout:15000},async t=>{
 for(const {mutate,priorSample} of [{mutate:value=>{value.receivedNs='-1';}},{mutate:value=>{value.receivedNs='3';value.sentNs='2';}},{mutate:(value,count)=>{if(count>1)value.clockEpoch=randomUUID();},priorSample:true}]){
  const f=await fake(t,{mutate});let prior;if(priorSample)prior=await f.transport.clockSample();await assert.rejects(f.transport.clockSample(),error=>error.delivery==='unknown');assert.equal(f.transport.status().pending,0);if(prior)assert.equal(capacityRemoteClockSampleBinding(prior),null);
 }
});

test('a later nonmonotonic exchange revokes the transport and every earlier envelope instead of allowing a fresh map to hide it',{timeout:15000},async t=>{
 const f=await fake(t,{mutate:(value,count)=>{value.receivedNs=value.sentNs=count===1?'10':'9';}}),mapping=createCapacityClockMapping({transport:f.transport});t.after(()=>mapping.close());const first=await mapping.sample();assert.equal(mapping.bounds({timestampNs:'10',clockEpoch:first.clockEpoch}).known,false);await assert.rejects(mapping.sample(),error=>error.delivery==='unknown');assert.equal(mapping.status().closed,true);assert.equal(capacityClockMappingBinding(mapping),null);assert.equal(mapping.bounds({timestampNs:'10',clockEpoch:first.clockEpoch}).known,false);assert.equal(f.transport.status().closed,true);assert.throws(()=>createCapacityClockMapping({transport:f.transport}),/configuration/);
});

test('mapping close waits for its own pending clock exchange, leaves no sample and preserves the transport',{timeout:15000},async t=>{
 const f=await fake(t,{hold:true}),mapping=createCapacityClockMapping({transport:f.transport});t.after(()=>mapping.close());const sampling=assert.rejects(mapping.sample(),/sample_unproven/);await f.seen;let finished=false;const closing=mapping.close().then(()=>{finished=true;});assert.equal(mapping.status().closed,true);assert.equal(finished,false);f.release();await sampling;await closing;assert.equal(mapping.status().samples,0);assert.equal(f.transport.status().pending,0);assert.equal(f.transport.status().closed,false);
});

test('disconnect during a pending sample settles both sample and mapper cleanup',{timeout:15000},async t=>{
 const f=await fake(t,{hold:true}),mapping=createCapacityClockMapping({transport:f.transport}),sampling=assert.rejects(mapping.sample(),error=>error.delivery==='unknown');await f.seen;f.transport.disconnect();await sampling;await mapping.close();assert.equal(mapping.status().samples,0);assert.equal(f.transport.status().pending,0);
});
