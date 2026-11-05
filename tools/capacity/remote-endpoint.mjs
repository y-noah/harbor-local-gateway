import tls from 'node:tls';
import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomUUID} from 'node:crypto';
import {createOwnedRuntime} from './owned-runtime.mjs';
import {startCapacityRemoteChild} from './remote-launcher.mjs';
import {remoteTlsMaterials,remoteInteger,remoteHash,remoteId,createCapacityRemoteFrames,capacityRemoteFrameLimits,remoteFailure} from './remote-protocol.mjs';

// This is an opt-in isolated endpoint, not a deployed daemon. It listens only
// on loopback in phase one. Remote identity is not proof of LAN or ownership.
export async function createCapacityRemoteEndpoint({runId,tls:tlsConfiguration,agents,browser,limits={},frames={},leaseMs=10000,handshakeTimeoutMs=5000,maxConnections=8,attest}={}){
 if(!remoteId(runId))throw remoteFailure('capacity_remote_configuration');remoteInteger(leaseMs,500,30000);remoteInteger(handshakeTimeoutMs,100,30000);
 frames=capacityRemoteFrameLimits(frames);remoteInteger(maxConnections,1,64);
 limits={maxInFlight:16,maxRequestTimeoutMs:30000,maxResponseBytes:262144,...limits};remoteInteger(limits.maxInFlight,1,256);remoteInteger(limits.maxRequestTimeoutMs,100,600000);remoteInteger(limits.maxResponseBytes,128,2097152);
 const expectedPeerHash=tlsConfiguration?.peerCertificateHash;if(!/^[a-f0-9]{64}$/.test(expectedPeerHash??''))throw remoteFailure('capacity_remote_tls_configuration');
 if(attest!==undefined&&typeof attest!=='function')throw remoteFailure('capacity_remote_configuration');
 const materials=await remoteTlsMaterials(tlsConfiguration),runtime=createOwnedRuntime(),clockEpoch=randomUUID();let executor,server,sessionUsed=false,draining=false,closeTransitions=0,closed=false,closePromise,channel,session,sequence=0,pending=0,commands=0,resourcesReleased=false,closeReason=null;const sockets=new Set();
 const close=()=>closePromise??=(async()=>{closed=true;clearTimeout(handshakeTimer);channel?.close();for(const socket of sockets)socket.destroy();const outcomes=await Promise.allSettled([executor?.close(),server?new Promise(resolve=>server.close(resolve)):undefined]);await runtime.close();if(outcomes.some(row=>row.status==='rejected'))throw remoteFailure('capacity_remote_endpoint_cleanup_unknown');resourcesReleased=true;return {resourcesReleased:true};})();
 let handshakeTimer;
 try{
  executor=await startCapacityRemoteChild({runtime,configuration:{agents,browser,limits,clockEpoch,frames},onBrowserState:value=>{if(!channel||closed)return false;const frame={runId,session,kind:'browser-state',value},bytes=Buffer.byteLength(JSON.stringify(frame))+4,ceiling=Math.max(0,Math.min(Math.floor(frames.maxBufferedBytes/2),frames.maxBufferedBytes-frames.controlReserveBytes-frames.maxFrameBytes-4));if(bytes>frames.maxFrameBytes||channel.status().bufferedBytes+bytes>ceiling)return false;try{channel.send(frame,{control:false});return true;}catch{return false;}},onBrowserChallenge:value=>{if(!channel||closed)return false;try{channel.send({runId,session,kind:'browser-challenge',value},{control:false});return true;}catch{return false;}}});
  executor.child.once('exit',()=>{if(!closed&&!executor.status().closing){closeReason??='executor-exit';channel?.close();void close().catch(()=>{});}});
  server=tls.createServer({...materials,requestCert:true,rejectUnauthorized:true,handshakeTimeout:handshakeTimeoutMs},socket=>{
   if(closed||sessionUsed||!socket.authorized||remoteHash(socket.getPeerCertificate().raw??Buffer.alloc(0))!==expectedPeerHash){socket.destroy();return;}
   socket.setTimeout(handshakeTimeoutMs,()=>socket.destroy());let authenticated=false,localChannel,attestationPending=false;
   const send=(value,control=true)=>{try{localChannel.send({runId,session,...value},{control});}catch{closeReason??='outbound-send-failed';localChannel.close();}};
   localChannel=createCapacityRemoteFrames(socket,{...frames,onClose:failed=>{if(authenticated){closeReason??=failed?'invalid-control-frame':'control-socket-closed';void close().catch(()=>{});}},onFrame:message=>{
    const clockReceivedNs=process.hrtime.bigint();
    if(!authenticated){
     if(message.kind!=='hello'||message.runId!==runId||!remoteId(message.challenge)||Object.keys(message).some(key=>!['kind','runId','challenge'].includes(key)))return false;
     if(sessionUsed)return false;sessionUsed=true;authenticated=true;session=randomUUID();channel=localChannel;const publish=attestation=>{clearTimeout(handshakeTimer);socket.setTimeout(leaseMs);send({kind:'ready',challenge:message.challenge,scope:'authenticated-restricted-endpoint',features:['clock-sampling-v1','request-events-v1',...(attest?['root-attestation-v1']:[])],clockEpoch,requestLimits:{maxRequestTimeoutMs:limits.maxRequestTimeoutMs,maxResponseBytes:limits.maxResponseBytes},agentSlots:agents.map(({id,origin})=>({id,origin})),browserProfiles:executor.ready.browserProfiles??[],...(attestation?{attestation}:{})});};if(attest)void Promise.resolve().then(()=>attest(Object.freeze({runId,session,clockEpoch,nonce:message.challenge,peerCertificateHash:expectedPeerHash}))).then(publish,()=>localChannel.close());else publish();return true;
    }
    if(draining&&!['browser-ack','browser-cancel','browser-state-feedback','ping'].includes(message.kind)||message.runId!==runId||message.session!==session||!Number.isSafeInteger(message.sequence)||message.sequence!==sequence+1||Object.keys(message).some(key=>!['kind','runId','session','sequence','id','payload'].includes(key)))return false;sequence=message.sequence;
    if(message.kind==='ping'){send({kind:'pong',id:message.id});return true;}
    if(message.kind==='attest'){if(!attest||!remoteId(message.id)||!message.payload||Object.keys(message.payload).length!==1||!remoteId(message.payload.nonce))return false;if(attestationPending){send({kind:'rejected',id:message.id,code:'capacity_remote_attestation_busy'});return true;}attestationPending=true;void Promise.resolve().then(()=>attest(Object.freeze({runId,session,clockEpoch,nonce:message.payload.nonce,peerCertificateHash:expectedPeerHash}))).then(value=>send({kind:'attestation-result',id:message.id,value}),()=>{closeReason??='root-attestation-failed';localChannel.close();}).finally(()=>{attestationPending=false;});return true;}
    if(message.kind==='clock'){
     if(!remoteId(message.id)||!message.payload||typeof message.payload!=='object'||Array.isArray(message.payload)||Object.keys(message.payload).length)return false;
     send({kind:'clock-result',id:message.id,value:{schema:'capacity-clock-sample-v1',clockEpoch,receivedNs:clockReceivedNs.toString(),sentNs:process.hrtime.bigint().toString()}});return true;
    }
    if(message.kind==='close'){draining=true;closeTransitions++;socket.setTimeout(Math.max(limits.maxRequestTimeoutMs,...(executor.ready.browserProfiles??[]).map(row=>row.maxActionMs))+1000);void executor.close({drain:true}).then(()=>{send({kind:'closed',id:message.id,resourcesReleased:true});localChannel.end();},()=>localChannel.close());return true;}
    if(['browser-ack','browser-state-feedback'].includes(message.kind)){if(!remoteId(message.id)||!message.payload||typeof message.payload!=='object'||Array.isArray(message.payload))return false;void executor.command(message.kind,message.payload).catch(()=>{closeReason??='browser-control-child-failed';localChannel.close();});return true;}
    if(!['request','cancel','browser-start','browser-close','browser-scenario','browser-cancel'].includes(message.kind)||!remoteId(message.id)||!message.payload||typeof message.payload!=='object'||Array.isArray(message.payload))return false;
    if(commands>=limits.maxInFlight+32||message.kind==='request'&&pending>=limits.maxInFlight){send({kind:'rejected',id:message.id});return true;}
    if(message.kind==='request')pending++;
    const payload=message.kind==='request'?{...message.payload,requestId:message.id}:message.payload;
    let observationDropped=0;const sendEvent=value=>{const frame={runId,session,kind:'request-event',id:message.id,value},bytes=Buffer.byteLength(JSON.stringify(frame))+4,ceiling=Math.max(0,Math.min(Math.floor(frames.maxBufferedBytes/2),frames.maxBufferedBytes-frames.controlReserveBytes-frames.maxFrameBytes-4));if(bytes>frames.maxFrameBytes||localChannel.status().bufferedBytes+bytes>ceiling){observationDropped++;return;}try{localChannel.send(frame);}catch(error){if(error.delivery==='not-sent')observationDropped++;else localChannel.close();}};
    commands++;void executor.command(message.kind,payload,message.kind==='request'?limits.maxRequestTimeoutMs+1000:message.kind==='browser-scenario'?Math.max(10000,...(executor.ready.browserProfiles??[]).map(row=>row.maxActionMs))+1000:10000,message.kind==='request'?sendEvent:undefined).then(value=>send({kind:'result',id:message.id,value:message.kind==='request'?{...value,driverObservationDropped:(value.driverObservationDropped??0)+observationDropped}:value},!['request','browser-scenario'].includes(message.kind)),error=>{if(['capacity_remote_child_command','capacity_remote_child_not_sent'].includes(error.code))send({kind:'rejected',id:message.id,...(typeof error.remoteCode==='string'&&/^capacity_[a-z0-9_]{1,120}$/.test(error.remoteCode)?{code:error.remoteCode}:{})});else{closeReason??='executor-command-failed';localChannel.close();}}).finally(()=>{commands--;if(message.kind==='request')pending--;});return true;
   }});
  });
  server.maxConnections=maxConnections;server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});server.on('tlsClientError',()=>{});
  await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});handshakeTimer=setTimeout(()=>{void close().catch(()=>{});},leaseMs);
  return Object.freeze({origin:'tls://127.0.0.1:'+server.address().port,close,status:async()=>({closed,draining,closeTransitions,closeReason,resourcesReleased,sessionUsed,pending,executor:executor?.status(),execution:closed?null:await executor.command('status')})});
 }catch(error){await close();throw error;}
}

async function endpointMain(){
 let endpoint,initialized=false,initializing,cleanupUnknown=false;const reply=(id,ok,value)=>{if(process.connected)process.send({id,ok,...(ok?{value}:{})});};
 process.on('message',message=>{const task=(async()=>{if(!Number.isSafeInteger(message?.id))return;try{
  if(message.operation==='initialize'){if(initialized)throw Error();initialized=true;endpoint=await createCapacityRemoteEndpoint(message.payload);reply(message.id,true,{origin:endpoint.origin});}
  else if(message.operation==='status'){reply(message.id,true,await endpoint.status());}
  else if(message.operation==='close'){await initializing;if(cleanupUnknown)throw Error();await endpoint?.close();reply(message.id,true,{resourcesReleased:true});if(process.connected)process.disconnect();}
  else throw Error();
 }catch(error){if(message.operation==='initialize'&&error.code==='capacity_remote_endpoint_cleanup_unknown')cleanupUnknown=true;reply(message.id,false);}})();if(message?.operation==='initialize')initializing=task;void task;});
 const shutdown=()=>{void Promise.resolve(initializing).then(()=>endpoint?.close()).then(()=>{if(process.connected)process.disconnect();},()=>{process.exitCode=1;if(process.connected)process.disconnect();});};process.once('disconnect',shutdown);for(const signal of ['SIGTERM','SIGINT'])process.once(signal,shutdown);
}
if(process.argv[2]==='--owned-capacity-remote-endpoint'&&process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)&&process.send)await endpointMain();
