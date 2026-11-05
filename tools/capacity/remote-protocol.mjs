import {readFile,lstat,realpath} from 'node:fs/promises';
import {isAbsolute} from 'node:path';
import {createHash} from 'node:crypto';

export const remoteFailure=code=>Object.assign(new Error(code),{code});
export const remoteHash=value=>createHash('sha256').update(value).digest('hex');
export const remoteId=value=>typeof value==='string'&&/^[a-zA-Z0-9_-]{1,100}$/.test(value);
export function remoteInteger(value,min,max){if(!Number.isSafeInteger(value)||value<min||value>max)throw remoteFailure('capacity_remote_configuration');return value;}
export async function remoteMaterial(path,privateKey=false){
 if(typeof path!=='string'||!isAbsolute(path))throw remoteFailure('capacity_remote_material');
 const info=await lstat(path);if(await realpath(path)!==path||!info.isFile()||info.isSymbolicLink()||info.uid!==process.getuid()||info.mode&(privateKey?0o077:0o022)||info.size>1048576)throw remoteFailure('capacity_remote_material');
 return readFile(path);
}
export async function remoteTlsMaterials(value){
 if(!value||Object.keys(value).some(key=>!['caFile','certFile','keyFile','peerCertificateHash'].includes(key))||!/^[a-f0-9]{64}$/.test(value.peerCertificateHash??''))throw remoteFailure('capacity_remote_tls_configuration');
 const [ca,cert,key]=await Promise.all([remoteMaterial(value.caFile),remoteMaterial(value.certFile),remoteMaterial(value.keyFile,true)]);return {ca,cert,key};
}

// The socket owns the only write queue. Data cannot occupy the control reserve;
// oversized input, invalid JSON and budget exhaustion permanently close it.
export function capacityRemoteFrameLimits({maxFrameBytes=262144,maxBufferedBytes=1048576,controlReserveBytes=65536}={}){
 remoteInteger(maxFrameBytes,1024,16777216);remoteInteger(maxBufferedBytes,maxFrameBytes+1024,67108864);remoteInteger(controlReserveBytes,1024,maxBufferedBytes-maxFrameBytes);
 return {maxFrameBytes,maxBufferedBytes,controlReserveBytes};
}
export function createCapacityRemoteFrames(socket,{onFrame,onClose,...configuration}={}){
 const {maxFrameBytes,maxBufferedBytes,controlReserveBytes}=capacityRemoteFrameLimits(configuration);
 if(typeof onFrame!=='function'||typeof onClose!=='function')throw remoteFailure('capacity_remote_configuration');
 let header=Buffer.alloc(4),headerBytes=0,payload=null,payloadBytes=0,closed=false,failed=false,peakBufferedBytes=0;
 const stop=()=>{if(closed)return;closed=true;payload=null;onClose(failed);};
 const fail=()=>{failed=true;socket.destroy();};
 socket.on('error',fail);socket.once('close',stop);
 socket.on('data',chunk=>{
  if(closed)return;
  try{let offset=0;while(offset<chunk.length){
   if(payload===null){const amount=Math.min(4-headerBytes,chunk.length-offset);chunk.copy(header,headerBytes,offset,offset+amount);headerBytes+=amount;offset+=amount;if(headerBytes<4)continue;const length=header.readUInt32BE(0);headerBytes=0;if(length<2||length>maxFrameBytes)throw Error();payload=Buffer.allocUnsafe(length);payloadBytes=0;}
   const amount=Math.min(payload.length-payloadBytes,chunk.length-offset);chunk.copy(payload,payloadBytes,offset,offset+amount);payloadBytes+=amount;offset+=amount;if(payloadBytes<payload.length)continue;
   const bytes=payload;payload=null;payloadBytes=0;const value=JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(bytes));if(!value||typeof value!=='object'||Array.isArray(value))throw Error();if(onFrame(value)===false)throw Error();if(closed||socket.destroyed)return;
  }}
  catch{fail();}
 });
 return Object.freeze({
  send(value,{control=false}={}){
   if(closed||socket.destroyed)throw Object.assign(remoteFailure('capacity_remote_closed'),{delivery:'not-sent'});
   const bytes=Buffer.from(JSON.stringify(value)),length=bytes.length+4,ceiling=control?maxBufferedBytes:maxBufferedBytes-controlReserveBytes;
   if(bytes.length>maxFrameBytes||socket.writableLength+length>ceiling)throw Object.assign(remoteFailure('capacity_remote_transport_budget'),{delivery:'not-sent'});
   const frame=Buffer.allocUnsafe(length);frame.writeUInt32BE(bytes.length);bytes.copy(frame,4);socket.write(frame);peakBufferedBytes=Math.max(peakBufferedBytes,socket.writableLength);
  },
  close:()=>socket.destroy(),end:()=>socket.end(),status:()=>({closed,failed,bufferedBytes:socket.writableLength,peakBufferedBytes,maxFrameBytes,maxBufferedBytes,controlReserveBytes})
 });
}
