import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {PeriodicSshWire,periodicSshWireLimits} from './postgres-periodic-ssh-wire.mjs';
import {usePeriodicTransferIndex,openPeriodicTransferSource,periodicCipherDigest,assertPeriodicTransferIndexActive} from './postgres-periodic-transfer-index.mjs';
import {createPeriodicTransferInbox,writePeriodicTransferControl,describePeriodicRemoteHandoff,receiveRemotePeriodicIndex,cleanupPeriodicTransferInbox,periodicObjectStoreLifetime} from './postgres-periodic-object-store.mjs';
import {canonical,transferError} from './migration-artifacts.mjs';

const fail=code=>{throw transferError('periodic_ssh_'+code);},same=(a,b)=>canonical(a)===canonical(b);
function record(value,kind,keys){if(!value||value.kind!==kind||Object.keys(value).sort().join(',')!==['kind',...keys].sort().join(','))fail('record_invalid');return value;}
function connection(wire){if(!(wire instanceof PeriodicSshWire))fail('wire_required');wire.assert();return wire.signal;}
async function controlBytes(directory,name,digest,signal){
 if(!/^handoff(?:-catalog-\d+)?\.enc$/.test(name)||!periodicCipherDigest(digest)||digest.bytes>1048576)fail('control_invalid');
 const file=await open(join(directory,name),constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{signal.throwIfAborted();const before=await file.stat({bigint:true});if(!before.isFile()||before.uid!==BigInt(process.getuid())||before.nlink!==1n||(before.mode&0o777n)!==0o600n||before.size!==BigInt(digest.bytes))fail('control_invalid');const bytes=Buffer.alloc(digest.bytes);let at=0;while(at<bytes.length){signal.throwIfAborted();const {bytesRead}=await file.read(bytes,at,bytes.length-at,at);if(!bytesRead)fail('control_invalid');at+=bytesRead;}const after=await file.stat({bigint:true});if(['dev','ino','size','mtimeNs','ctimeNs'].some(key=>before[key]!==after[key])||createHash('sha256').update(bytes).digest('hex')!==digest.sha256)fail('control_invalid');return bytes;}finally{await file.close();}
}

// Only the current live source capability selects immutable files. The peer
// can request a known digest and its missing suffix, never a filesystem path.
export async function servePeriodicSshSource(index,wire,{finish}={}){
 if(finish!==undefined&&typeof finish!=='function')fail('configuration_invalid');
 const signal=connection(wire);return usePeriodicTransferIndex(index,async source=>{
  let complete=false;
  try{
  await wire.writeJson({kind:'periodic-start',version:1,handoffDigest:source.handoffDigest});
  for(const part of [{name:'handoff.enc',digest:source.handoffDigest},...source.manifest.parts]){await wire.writeJson({kind:'control',name:part.name,digest:part.digest});await wire.writeBytes(await controlBytes(source.directory,part.name,part.digest,signal));}
  const entries=new Map(source.entries.map(entry=>[entry.digest.sha256,entry])),sent=new Set();
  for(;;){
   const request=await wire.readJson();
   if(request.kind==='complete'){record(request,'complete',['backupId','pointId']);if(request.backupId!==index.backupId||request.pointId!==index.pointId)fail('completion_binding');complete=true;return {backupId:index.backupId,pointId:index.pointId,transportComplete:true,restoreVerified:false,offhostVerified:false,deploymentAllowed:false};}
   record(request,'suffix',['digest','offset']);if(!periodicCipherDigest(request.digest)||!Number.isSafeInteger(request.offset)||request.offset<0||request.offset>=request.digest.bytes)fail('range_invalid');const entry=entries.get(request.digest.sha256);if(!entry||!same(entry.digest,request.digest)||sent.has(request.digest.sha256))fail('range_invalid');sent.add(request.digest.sha256);
   const input=await openPeriodicTransferSource(entry,{signal});try{
    await wire.writeJson({kind:'suffix',digest:entry.digest,offset:request.offset});const buffer=Buffer.alloc(Math.min(1048576,periodicSshWireLimits.dataBytes));
    for(let at=request.offset;at<entry.digest.bytes;){assertPeriodicTransferIndexActive(index,{signal});const bytes=Math.min(buffer.length,entry.digest.bytes-at),result=await input.read(buffer,0,bytes,at);if(result.bytesRead!==bytes)fail('source_changed');assertPeriodicTransferIndexActive(index,{signal});await wire.writeBytes(buffer.subarray(0,bytes));at+=bytes;}
   }finally{await input.close();}
  }
  }finally{await finish?.({complete});}
 },{signal});
}

// The caller supplies a genuine fixed-profile or task store capability. This
// path receives ciphertext only; full restore/signing remains a separate job.
export async function receivePeriodicSshTransfer(store,wire){
 connection(wire);const lifetime=periodicObjectStoreLifetime(store),cancel=()=>wire.cancellation.abort(lifetime.reason);lifetime.addEventListener('abort',cancel,{once:true});if(lifetime.aborted)cancel();
 try{return await receiveOwnedPeriodicSshTransfer(store,wire);}finally{lifetime.removeEventListener('abort',cancel);}
}
async function receiveOwnedPeriodicSshTransfer(store,wire){
 const signal=connection(wire),start=record(await wire.readJson(),'periodic-start',['version','handoffDigest']);if(start.version!==1||!periodicCipherDigest(start.handoffDigest)||start.handoffDigest.bytes>1048576)fail('control_invalid');
 const inbox=await createPeriodicTransferInbox(store,{signal});let released=false;
 try{
 async function receiveControl(name,digest){const part=record(await wire.readJson(),'control',['name','digest']);if(part.name!==name||!same(part.digest,digest))fail('control_invalid');await writePeriodicTransferControl(store,inbox,name,await wire.readBytes(digest.bytes),digest,{signal});}
 await receiveControl('handoff.enc',start.handoffDigest);const described=await describePeriodicRemoteHandoff(store,inbox,start.handoffDigest,{signal});for(const part of described.parts)await receiveControl(part.name,part.digest);
 let streaming=null;
 const readRange=async({digest,offset,bytes},{signal:combined})=>{
  const cancel=()=>wire.cancellation.abort(combined.reason);combined.addEventListener('abort',cancel,{once:true});if(combined.aborted)cancel();
  try{
  if(!streaming||!same(streaming.digest,digest)){if(streaming&&streaming.offset!==streaming.digest.bytes)fail('range_incomplete');await wire.writeJson({kind:'suffix',digest,offset});const accepted=record(await wire.readJson(),'suffix',['digest','offset']);if(!same(accepted.digest,digest)||accepted.offset!==offset)fail('range_invalid');streaming={digest,offset};}
  if(offset!==streaming.offset||!Number.isSafeInteger(bytes)||bytes<1||bytes>periodicSshWireLimits.dataBytes||offset+bytes>digest.bytes)fail('range_invalid');const data=await wire.readBytes(bytes);streaming.offset+=bytes;return data;
  }finally{combined.removeEventListener('abort',cancel);}
 };
 const receipt=await receiveRemotePeriodicIndex(store,{directory:inbox,handoffDigest:start.handoffDigest},readRange,{signal});if(streaming&&streaming.offset!==streaming.digest.bytes)fail('range_incomplete');await cleanupPeriodicTransferInbox(store,inbox);released=true;await wire.writeJson({kind:'complete',backupId:described.backupId,pointId:described.pointId});return receipt;
 }finally{if(!released)await cleanupPeriodicTransferInbox(store,inbox);}
}
