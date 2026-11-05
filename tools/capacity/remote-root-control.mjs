import tls from 'node:tls';
import {randomUUID,sign,verify} from 'node:crypto';
import {remoteTlsMaterials,remoteHash,remoteId,createCapacityRemoteFrames} from './remote-protocol.mjs';
import {inspectCapacityRemoteRootApproval,capacityRemoteTranscriptBytes} from './remote-root-approval.mjs';

const releases=new WeakMap(),fail=code=>{throw Error('capacity_remote_root_control_'+code);};
export const capacityRootRemoteReleaseBinding=handle=>releases.get(handle)??null;
const signature=value=>typeof value==='string'&&/^[A-Za-z0-9+/]{86}==$/.test(value)&&Buffer.from(value,'base64').length===64&&Buffer.from(value,'base64').toString('base64')===value;

async function serve({materials,peerCertificateHash,listen,approvalId,readStatus,authorize}){
 const sockets=new Set();let closed=false,closing;
 const server=tls.createServer({...materials,requestCert:true,rejectUnauthorized:true,handshakeTimeout:10000},socket=>{
  if(closed||!socket.authorized||remoteHash(socket.getPeerCertificate().raw??Buffer.alloc(0))!==peerCertificateHash){socket.destroy();return;}let used=false;socket.setTimeout(10000,()=>socket.destroy());let channel;
  channel=createCapacityRemoteFrames(socket,{maxFrameBytes:16384,maxBufferedBytes:65536,controlReserveBytes:4096,onClose:()=>{},onFrame:message=>{
   if(used||message.kind!=='release-status'||Object.keys(message).some(key=>!['kind','leaseId','nonce'].includes(key))||!remoteId(message.leaseId)||!remoteId(message.nonce))return false;used=true;
   void (async()=>{const key=await authorize(),state=readStatus(message.leaseId);if(!state||typeof state.known!=='boolean'||typeof state.resourcesReleased!=='boolean'||state.resourcesReleased&&!state.known)fail('state');const body={schema:'capacity-root-remote-release-v1',approvalId,leaseId:message.leaseId,nonce:message.nonce,known:state.known,resourcesReleased:state.resourcesReleased,cleanupUnknown:state.cleanupUnknown===true};channel.send({kind:'release-status-result',body,signature:sign(null,capacityRemoteTranscriptBytes(body),key).toString('base64')},{control:true});channel.end();})().catch(()=>channel.close());return true;
  }});
 });
 server.maxConnections=16;server.on('connection',socket=>{sockets.add(socket);socket.on('error',()=>{});socket.once('close',()=>sockets.delete(socket));});server.on('tlsClientError',()=>{});
 await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(listen.port,listen.host,resolve);});
 return Object.freeze({origin:'tls://127.0.0.1:'+server.address().port,close:()=>closing??=(async()=>{closed=true;for(const socket of sockets)socket.destroy();await new Promise(resolve=>server.close(resolve));})()});
}
export async function createCapacityRootRemoteControl({approval,readStatus,validateSigner}={}){
 const approved=await inspectCapacityRemoteRootApproval(approval);if(approved.side!=='broker'||typeof readStatus!=='function'||typeof validateSigner!=='function')fail('configuration');return serve({materials:await remoteTlsMaterials(approved.tls),peerCertificateHash:approved.tls.peerCertificateHash,listen:approved.plan.controlListen,approvalId:approved.plan.approvalId,readStatus,authorize:async()=>{await validateSigner();return (await inspectCapacityRemoteRootApproval(approval)).signingKey;}});
}

// Shared protocol/crypto component for isolated tests. This never mints a root
// approval or release capability, listens outside loopback, or reads a root key.
export async function createCapacityRootRemoteControlComponent({materials,peerCertificateHash,signingKey,readStatus,approvalId='component'}={}){
 if(!materials?.ca||!materials.cert||!materials.key||!/^[a-f0-9]{64}$/.test(peerCertificateHash??'')||signingKey?.asymmetricKeyType!=='ed25519'||typeof readStatus!=='function'||!remoteId(approvalId))fail('configuration');const server=await serve({materials,peerCertificateHash,listen:{host:'127.0.0.1',port:0},approvalId,readStatus,authorize:async()=>signingKey});return Object.freeze({...server,scope:'root-control-protocol-component-only',formalQualified:false});
}
async function query({origin,materials,expectedPeerHash,approvalId,publicKey,leaseId,signal}){
 signal?.throwIfAborted();const url=new URL(origin),nonce=randomUUID();let socket,channel,timer,settled=false;
 return new Promise((resolve,reject)=>{
  const finish=(error,value)=>{if(settled)return;settled=true;clearTimeout(timer);signal?.removeEventListener('abort',abort);channel?.close();socket?.destroy();error?reject(error):resolve(value);},abort=()=>finish(Error('capacity_remote_root_control_cancelled'));signal?.addEventListener('abort',abort,{once:true});
  socket=tls.connect({...materials,host:url.hostname==='[::1]'?'::1':url.hostname,port:Number(url.port),servername:url.hostname==='localhost'?'localhost':undefined,rejectUnauthorized:true,checkServerIdentity:tls.checkServerIdentity});socket.on('error',()=>finish(Error('capacity_remote_root_control_connection')));
  channel=createCapacityRemoteFrames(socket,{maxFrameBytes:16384,maxBufferedBytes:65536,controlReserveBytes:4096,onClose:()=>finish(Error('capacity_remote_root_control_unknown')),onFrame:value=>{const body=value?.body;if(value.kind!=='release-status-result'||!body||body.schema!=='capacity-root-remote-release-v1'||body.approvalId!==approvalId||body.leaseId!==leaseId||body.nonce!==nonce||typeof body.known!=='boolean'||typeof body.resourcesReleased!=='boolean'||typeof body.cleanupUnknown!=='boolean'||body.resourcesReleased&&!body.known||!signature(value.signature)||!verify(null,capacityRemoteTranscriptBytes(body),publicKey,Buffer.from(value.signature,'base64')))return false;finish(null,{nonce,body:Object.freeze({...body})});return true;}});
  socket.once('secureConnect',()=>{if(!socket.authorized||remoteHash(socket.getPeerCertificate().raw??Buffer.alloc(0))!==expectedPeerHash)return finish(Error('capacity_remote_root_control_peer'));try{channel.send({kind:'release-status',leaseId,nonce},{control:true});}catch{finish(Error('capacity_remote_root_control_send'));}});timer=setTimeout(()=>finish(Error('capacity_remote_root_control_timeout')),10000);
 });
}
export async function queryCapacityRootRemoteRelease({approval,leaseId,signal}={}){
 const approved=await inspectCapacityRemoteRootApproval(approval);if(approved.side!=='controller'||!remoteId(leaseId))fail('configuration');const result=await query({origin:approved.plan.controlOrigin,materials:await remoteTlsMaterials(approved.tls),expectedPeerHash:approved.tls.peerCertificateHash,approvalId:approved.plan.approvalId,publicKey:approved.publicKey,leaseId,signal});const receipt=Object.freeze(Object.create(null));releases.set(receipt,Object.freeze({approval,leaseId,...result}));return receipt;
}
export async function queryCapacityRootRemoteReleaseComponent({origin,materials,expectedPeerHash,publicKey,leaseId,signal,approvalId='component'}={}){
 const url=new URL(origin);if(url.protocol!=='tls:'||!['127.0.0.1','localhost','[::1]'].includes(url.hostname)||url.username||url.password||url.pathname||url.search||url.hash||!remoteId(leaseId)||publicKey?.asymmetricKeyType!=='ed25519')fail('configuration');const result=await query({origin,materials,expectedPeerHash,approvalId,publicKey,leaseId,signal});return Object.freeze({...result,scope:'root-control-protocol-component-only',formalQualified:false});
}
