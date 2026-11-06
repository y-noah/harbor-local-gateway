import {open,lstat,mkdir,readdir,realpath,rename,link,unlink,statfs} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join,resolve,dirname} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {acquireInstance} from './instance.mjs';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {loadPeriodicApplicationKey,periodicApplicationKeyDigest} from './postgres-periodic-application-key.mjs';
import {copyBackupCipher,validHandoffPath} from './postgres-backup-handoff.mjs';
import {usePeriodicTransferIndex,assertPeriodicTransferIndexActive,openPeriodicTransferSource,periodicCipherIdentity,periodicCipherDigest} from './postgres-periodic-transfer-index.mjs';
import {canonical,readManifest,writeManifest,digestFile,privateDirectory,syncDirectory,transferError} from './migration-artifacts.mjs';

const stores=new WeakMap(),scope='harbor-periodic-cipher-transport-v1',profileFile='/etc/harbor-storage-periodic-transfer.json',fail=code=>{throw transferError(code);},same=(a,b)=>canonical(a)===canonical(b);
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value),sha=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const defaults={maxFiles:100000,maxBundleBytes:1099511627776,maxObjectBytes:1073741860,maxChunkBytes:1048576,maxStoreBytes:2199023255552,minFreeBytes:0,maxPoints:10000,maxStoreFiles:1000000};
function limits(value={}){if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(key=>!Object.hasOwn(defaults,key)))fail('periodic_transfer_limits_invalid');const result={...defaults,...value};if(Object.values(result).some(n=>!Number.isSafeInteger(n)||n<0)||result.maxFiles<1||result.maxFiles>1000000||result.maxPoints<1||result.maxPoints>1000000||result.maxStoreFiles<1||result.maxStoreFiles>5000000||result.maxObjectBytes<36||result.maxObjectBytes>1073741860||result.maxChunkBytes<1||result.maxChunkBytes>16777216||result.maxChunkBytes>result.maxObjectBytes||result.maxBundleBytes<result.maxObjectBytes||result.maxStoreBytes<result.maxBundleBytes)fail('periodic_transfer_limits_invalid');return Object.freeze(result);}
async function trustedDirectory(path,{root=false}={}){await privateDirectory(path);if(await realpath(path)!==resolve(path))fail('periodic_transfer_directory_untrusted');if(root)for(let at=path;;at=dirname(at)){const info=await lstat(at);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||info.mode&0o022)fail('periodic_transfer_directory_untrusted');if(at==='/')break;}}
async function protectedBytes(path,uid,maxBytes){const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const info=await file.stat();if(!info.isFile()||info.uid!==uid||(info.mode&0o777)!==0o600||info.nlink!==1||info.size<1||info.size>maxBytes)fail('periodic_transfer_profile_untrusted');return await file.readFile();}finally{await file.close();}}
function policy(value){if(!value||value.version!==1||typeof value.enabled!=='boolean'||Object.keys(value).some(key=>!['version','enabled','directory','sourceMachine','expectedCluster','keyVersion','archiveKeyFile','archiveUid','applicationEncryption','limits'].includes(key)))fail('periodic_transfer_profile_invalid');if(!value.enabled)return {version:1,enabled:false};if(typeof value.directory!=='string'||resolve(value.directory)!==value.directory||value.directory==='/'||!sha(value.sourceMachine)||!/^\d{1,20}$/.test(value.expectedCluster??'')||!/^[-a-zA-Z0-9_.]{1,64}$/.test(value.keyVersion??'')||typeof value.archiveKeyFile!=='string'||resolve(value.archiveKeyFile)!==value.archiveKeyFile||!Number.isSafeInteger(value.archiveUid)||value.archiveUid<1)fail('periodic_transfer_profile_invalid');return {...value,limits:limits(value.limits)};}
async function publishStore(value){await trustedDirectory(value.directory,{root:value.formal});for(const name of ['objects','partial','points','incoming','bundles','copies']){await mkdir(join(value.directory,name),{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await trustedDirectory(join(value.directory,name),{root:value.formal});}const token=Object.freeze({scope:value.formal?'root-periodic-transport':'task-periodic-transport'});stores.set(token,{...value,closed:false,pending:null,cancellation:null,closePromise:null});return token;}
export async function loadPeriodicObjectStore(){
 if(process.platform!=='linux'||process.getuid()!==0)fail('periodic_transfer_root_required');let bytes;try{bytes=await protectedBytes(profileFile,0,16384);}catch(error){if(error.code==='ENOENT')return null;throw error;}
 let value;try{value=policy(JSON.parse(bytes.toString('utf8')));}catch(error){if(error.code)throw error;fail('periodic_transfer_profile_invalid');}if(!value.enabled)return null;
 for(const path of [value.archiveKeyFile,value.applicationEncryption?.keyFile])if(typeof path==='string'&&(path===value.directory||path.startsWith(value.directory+'/')))fail('periodic_transfer_secret_overlap');
 const archive=await protectedBytes(value.archiveKeyFile,value.archiveUid,32);if(archive.length!==32)fail('periodic_transfer_profile_invalid');let application;try{application=await loadPeriodicApplicationKey(value.applicationEncryption,archive);if(!application.applicationKey)fail('periodic_application_key_required');return await publishStore({...value,...application,formal:true});}catch(error){application?.applicationKey?.fill(0);throw error;}finally{archive.fill(0);}
}
export async function createTaskPeriodicObjectStore({laboratory,context,directory,expectedSourceMachine,limits:budget}={}){
 const {isOwnedLaboratory}=await import('./tools/capacity/owned-runtime.mjs'),record=postgresBackupContext(context),{backupMachineIdentity}=await import('./postgres-backup-handoff.mjs');if(!isOwnedLaboratory(laboratory)||record.scope!=='task-owned'||record.closed||!record.applicationKey||!record.artifactDirectory.startsWith(laboratory.directory+'/')||typeof directory!=='string'||resolve(directory)!==directory||!directory.startsWith(laboratory.directory+'/'))fail('periodic_transfer_task_required');
 if(expectedSourceMachine!==undefined&&!sha(expectedSourceMachine))fail('periodic_transfer_profile_invalid');const budgetPolicy=limits(budget),sourceMachine=expectedSourceMachine??await backupMachineIdentity(),key=Buffer.from(record.applicationKey);try{const created=await publishStore({directory,sourceMachine,expectedCluster:record.expected.system_id,keyVersion:record.keyVersion,applicationKey:key,applicationKeyVersion:record.applicationKeyVersion,applicationKeyDigest:record.applicationKeyDigest,limits:budgetPolicy,formal:false});if(record.closed){await closePeriodicObjectStore(created);fail('backup_context_closed');}return created;}catch(error){key.fill(0);throw error;}
}
function storeValue(store){const value=stores.get(store);if(!value||value.closed||periodicApplicationKeyDigest(value.applicationKey)!==value.applicationKeyDigest)fail('periodic_transfer_store_required');return value;}
export async function closePeriodicObjectStore(store){const value=stores.get(store);if(!value)return;if(!value.closePromise){value.closed=true;value.cancellation?.abort();value.closePromise=(async()=>{await Promise.allSettled([value.pending]);value.applicationKey.fill(0);})();}await value.closePromise;}
async function exists(path){try{await lstat(path);return true;}catch(error){if(error.code==='ENOENT')return false;throw error;}}
const bound=(store,value)=>value.version===1&&value.scope===scope&&value.sourceMachine===store.sourceMachine&&value.expectedCluster===store.expectedCluster&&value.applicationKeyVersion===store.applicationKeyVersion&&value.applicationKeyDigest===store.applicationKeyDigest;
const common=store=>({version:1,scope,sourceMachine:store.sourceMachine,expectedCluster:store.expectedCluster,applicationKeyVersion:store.applicationKeyVersion,applicationKeyDigest:store.applicationKeyDigest});
async function objectPresent(store,digest){
 const path=join(store.directory,'objects',digest.sha256+'.enc'),receiptPath=join(store.directory,'objects',digest.sha256+'.verified.enc');const present=await exists(path),signed=await exists(receiptPath);
 if(!present&&!signed)return false;if(!present||!signed)return false;
 const receipt=await readManifest(receiptPath,store.applicationKey);if(!bound(store,receipt)||!same(receipt.digest,digest)||!same(receipt.identity,await periodicCipherIdentity(path)))fail('periodic_transfer_object_changed');return true;
}
async function hashHandle(file,bytes,signal){const hash=createHash('sha256'),buffer=Buffer.alloc(Math.min(1048576,Math.max(bytes,1)));for(let at=0;at<bytes;){signal?.throwIfAborted();const {bytesRead}=await file.read(buffer,0,Math.min(buffer.length,bytes-at),at);if(!bytesRead)fail('periodic_transfer_partial_changed');hash.update(buffer.subarray(0,bytesRead));at+=bytesRead;}return hash;}
async function publishingPair(store,path){
 const name=path.slice(path.lastIndexOf('/')+1),parent=dirname(path),match=parent===join(store.directory,'objects')?/^([a-f0-9]{64})\.enc$/.exec(name):parent===join(store.directory,'partial')?/^([a-f0-9]{64})\.part$/.exec(name):null;if(!match)return bundlePublishingPair(store,path);
 const hash=match[1],cursor=await readManifest(join(store.directory,'partial',hash+'.cursor.enc'),store.applicationKey);assertPublishingCursor(store,cursor,{sha256:hash,bytes:cursor.digest?.bytes});const object=await targetPathIdentity(join(store.directory,'objects',hash+'.enc'),2),partial=await targetPathIdentity(join(store.directory,'partial',hash+'.part'),2);if(!same(object,partial)||!sameAllocation(cursor.identity,object))fail('periodic_transfer_store_untrusted');return object;
}
async function storeUsage(store){
 const usage={bytes:0,files:0},inodes=new Set();const walk=async directory=>{for(const name of await readdir(directory)){const path=join(directory,name),info=await lstat(path);if(info.isSymbolicLink()||info.uid!==process.getuid()||info.mode&0o077)fail('periodic_transfer_store_untrusted');if(info.isDirectory())await walk(path);else{if(!info.isFile()||![1,2].includes(info.nlink))fail('periodic_transfer_store_untrusted');if(info.nlink===2)await publishingPair(store,path);const inode=String(info.dev)+':'+String(info.ino);if(!inodes.has(inode)){inodes.add(inode);usage.bytes+=info.size;}if(++usage.files>store.limits.maxStoreFiles||usage.bytes>store.limits.maxStoreBytes)fail('periodic_transfer_store_capacity');}}};
 for(const name of ['objects','partial','points','incoming','bundles','copies'])await walk(join(store.directory,name));return usage;
}

async function reserve(store,usage,bytes,files=0){const disk=await statfs(store.directory);if(!Number.isSafeInteger(usage.bytes+bytes)||usage.bytes+bytes>store.limits.maxStoreBytes||usage.files+files>store.limits.maxStoreFiles||Number(disk.bavail)*Number(disk.bsize)-bytes<store.limits.minFreeBytes)fail('periodic_transfer_store_capacity');usage.bytes+=bytes;usage.files+=files;}
async function writeStoreManifest(store,path,value,usage,{replace=false}={}){const present=await exists(path),old=present?Number((await periodicCipherIdentity(path)).size):0,bytes=Buffer.byteLength(JSON.stringify(value))+36;if(bytes>1048576)fail('transfer_manifest_too_large');if(bytes>=old)await reserve(store,usage,bytes-old,present?0:1);else usage.bytes-=old-bytes;await writeManifest(path,value,store.applicationKey,{replace});}

async function intake(store,index,signal,usage){
 const {manifest,directory,handoffDigest}=index;
 if(manifest.version!==1||manifest.kind!=='periodic-application'||!uuid(manifest.backupId)||!uuid(manifest.pointId)||manifest.sourceMachine!==store.sourceMachine||manifest.expected?.system_id!==store.expectedCluster||manifest.keyVersion!==store.keyVersion||manifest.applicationKeyVersion!==store.applicationKeyVersion||manifest.applicationKeyDigest!==store.applicationKeyDigest||store.formal&&manifest.scope!=='existing-cluster')fail('periodic_transfer_source_binding');
 if(!Number.isSafeInteger(manifest.count)||manifest.count<1||manifest.count>store.limits.maxFiles||!Number.isSafeInteger(manifest.totalBytes)||manifest.totalBytes<36||manifest.totalBytes>store.limits.maxBundleBytes||!Array.isArray(manifest.parts)||!manifest.parts.length||manifest.parts.length>manifest.count)fail('periodic_transfer_index_capacity');
 if(!same(await digestFile(join(directory,'handoff.enc'),{signal}),handoffDigest)||!same(await readManifest(join(directory,'handoff.enc'),store.applicationKey,{signal}),manifest))fail('periodic_transfer_index_changed');
 const entries=[],seen=new Set();let total=0;for(const [ordinal,part] of manifest.parts.entries()){
  if(part.name!=='handoff-catalog-'+ordinal+'.enc'||!periodicCipherDigest(part.digest)||!same(await digestFile(join(directory,part.name),{signal}),part.digest))fail('periodic_transfer_index_changed');const page=await readManifest(join(directory,part.name),store.applicationKey,{signal});if(page.version!==1||page.ordinal!==ordinal||!Array.isArray(page.entries)||page.entries.length!==part.count)fail('periodic_transfer_index_changed');for(const entry of page.entries){if(!validHandoffPath(entry.path,manifest.backupId,'periodic-application',manifest.pointId)||seen.has(entry.path)||!periodicCipherDigest(entry.digest)||entry.digest.bytes>store.limits.maxObjectBytes||entries.length>=store.limits.maxFiles)fail('periodic_transfer_index_changed');seen.add(entry.path);entries.push(entry);total+=entry.digest.bytes;}}
 if(entries.length!==manifest.count||total!==manifest.totalBytes||index.entries!==undefined&&!same(entries,index.entries.map(({path,digest})=>({path,digest}))))fail('periodic_transfer_index_changed');
 const pointName=manifest.backupId+'--'+manifest.pointId,final=join(store.directory,'points',pointName),pending=join(store.directory,'points','.pending-'+pointName);
 if(!await exists(pending)&&!await exists(final)){if((await readdir(join(store.directory,'points'))).length>=store.limits.maxPoints)fail('periodic_transfer_store_capacity');await mkdir(pending,{mode:0o700});}
 const target=await exists(final)?final:pending;await trustedDirectory(target,{root:store.formal});for(const name of await readdir(target))if(!['received.enc',...manifest.parts.map(row=>row.name),'handoff.enc'].includes(name))fail('periodic_transfer_point_conflict');for(const part of [...manifest.parts,{name:'handoff.enc',digest:handoffDigest}]){
  const path=join(target,part.name);if(await exists(path)){if(!same(await digestFile(path,{signal}),part.digest))fail('periodic_transfer_point_conflict');}else{await reserve(store,usage,part.digest.bytes,1);await copyBackupCipher(join(directory,part.name),path,{signal,maxBytes:part.digest.bytes});}
 }
 return {entries,manifest,handoffDigest,pending,final,target};
}
function targetIdentity(info,links=1){if(!info.isFile()||info.isSymbolicLink?.()||info.uid!==BigInt(process.getuid())||(info.mode&0o777n)!==0o600n||info.nlink!==BigInt(links))fail('periodic_transfer_partial_changed');return Object.fromEntries(['dev','ino','uid','mode','nlink','size','mtimeNs','ctimeNs'].map(name=>[name,String(info[name])]));}
async function descriptorIdentity(file,links=1){return targetIdentity(await file.stat({bigint:true}),links);}
async function targetPathIdentity(path,links=1){return targetIdentity(await lstat(path,{bigint:true}),links);}
const sameAllocation=(before,after)=>['dev','ino','uid','mode','size','mtimeNs'].every(name=>before[name]===after[name]);
async function assertDescriptorPath(file,path,expected){if(!same(expected,await descriptorIdentity(file,Number(expected.nlink)))||!same(expected,await targetPathIdentity(path,Number(expected.nlink))))fail('periodic_transfer_partial_changed');}
function assertPublishingCursor(store,cursor,digest){if(!cursor||!bound(store,cursor)||!same(cursor.digest,digest)||cursor.phase!=='publishing'||cursor.received!==digest.bytes||cursor.prefixDigest!==digest.sha256)fail('periodic_transfer_object_unclaimed');}
async function finishPublishedObject(store,file,{object,partial,cursor,receiptPath,digest,signal,usage,linked=false}){
 const allocation=await descriptorIdentity(file,linked?2:1);if(!sameAllocation(cursor.identity,allocation))fail('periodic_transfer_object_changed');await assertDescriptorPath(file,object,allocation);
 if(linked){await assertDescriptorPath(file,partial,allocation);signal?.throwIfAborted();await unlink(partial);usage.files--;await syncDirectory(dirname(partial));await syncDirectory(dirname(object));}
 const published=await descriptorIdentity(file);if(!sameAllocation(allocation,published))fail('periodic_transfer_object_changed');await assertDescriptorPath(file,object,published);if((await hashHandle(file,digest.bytes,signal)).digest('hex')!==digest.sha256)fail('periodic_transfer_object_digest');await assertDescriptorPath(file,object,published);signal?.throwIfAborted();await writeStoreManifest(store,receiptPath,{...common(store),digest,identity:published,verifiedAt:Date.now()},usage);
}

async function receiveObject(store,entry,{signal,onChunk,stats,usage,sourceReader}={}){
 const digest=entry.digest,hash=digest.sha256,object=join(store.directory,'objects',hash+'.enc'),receiptPath=join(store.directory,'objects',hash+'.verified.enc'),partial=join(store.directory,'partial',hash+'.part'),cursorPath=join(store.directory,'partial',hash+'.cursor.enc');
 if(await objectPresent(store,digest)){stats.objectsReused++;return;}
 let cursor;try{cursor=await readManifest(cursorPath,store.applicationKey,{signal});}catch(error){if(error.code!=='ENOENT')throw error;}
 if(cursor&&(!bound(store,cursor)||!same(cursor.digest,digest)||!Number.isSafeInteger(cursor.received)||cursor.received<0||cursor.received>digest.bytes||!['writing','resetting','publishing'].includes(cursor.phase)||cursor.phase==='resetting'&&(cursor.received!==0||cursor.prefixDigest!==createHash('sha256').digest('hex'))))fail('periodic_transfer_cursor_binding');
 if(await exists(object)){
  assertPublishingCursor(store,cursor,digest);const linked=await exists(partial),file=await open(linked?partial:object,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{await finishPublishedObject(store,file,{object,partial,cursor,receiptPath,digest,signal,usage,linked});stats.objectsReused++;return;}finally{await file.close();}
 }

 const newlyAllocated=!cursor;if(!cursor){if(await exists(partial))fail('periodic_transfer_partial_unclaimed');await reserve(store,usage,digest.bytes,1);const allocated=await open(partial,constants.O_RDWR|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);try{await allocated.sync();}finally{await allocated.close();}await syncDirectory(dirname(partial));cursor={...common(store),digest,phase:'writing',received:0,identity:await periodicCipherIdentity(partial),prefixDigest:createHash('sha256').digest('hex')};await writeStoreManifest(store,cursorPath,cursor,usage);}
 let output,input;try{
  output=await open(partial,constants.O_RDWR|constants.O_NOFOLLOW|constants.O_NONBLOCK);input=sourceReader?await sourceReader(entry,{signal}):await openPeriodicTransferSource(entry,{signal});
  const actual=await periodicCipherIdentity(partial),outputInfo=await output.stat({bigint:true});if(!['dev','ino','uid','mode','nlink'].every(name=>cursor.identity[name]===actual[name])||String(outputInfo.dev)!==cursor.identity.dev||String(outputInfo.ino)!==cursor.identity.ino||Number(actual.size)<cursor.received||Number(actual.size)>digest.bytes)fail('periodic_transfer_partial_changed');
  const incremental=await hashHandle(output,cursor.received,signal);if(incremental.copy().digest('hex')!==cursor.prefixDigest)fail('periodic_transfer_partial_changed');
  if(cursor.phase==='resetting'&&!same(cursor.identity,actual)&&Number(actual.size)!==0)fail('periodic_transfer_partial_changed');
  if(!same(cursor.identity,actual)||Number(actual.size)!==cursor.received||cursor.phase==='resetting'){await assertDescriptorPath(output,partial,actual);usage.bytes-=Number(actual.size)-cursor.received;await output.truncate(cursor.received);await output.sync();cursor={...cursor,phase:'writing',identity:await periodicCipherIdentity(partial)};await writeStoreManifest(store,cursorPath,cursor,usage,{replace:true});}
  if(!newlyAllocated)await reserve(store,usage,digest.bytes-cursor.received);
  const buffer=Buffer.alloc(store.limits.maxChunkBytes);while(cursor.received<digest.bytes){signal?.throwIfAborted();if(!same(cursor.identity,await periodicCipherIdentity(partial)))fail('periodic_transfer_partial_changed');const length=Math.min(buffer.length,digest.bytes-cursor.received),{bytesRead}=await input.read(buffer,0,length,cursor.received);if(bytesRead!==length)fail('periodic_transfer_source_changed');let written=0;while(written<length){const result=await output.write(buffer,written,length-written,cursor.received+written);if(!result.bytesWritten)fail('periodic_transfer_write_failed');written+=result.bytesWritten;}incremental.update(buffer.subarray(0,length));await output.sync();cursor={...cursor,received:cursor.received+length,identity:await periodicCipherIdentity(partial),prefixDigest:incremental.copy().digest('hex')};await writeStoreManifest(store,cursorPath,cursor,usage,{replace:true});stats.bytesSent+=length;if(/^backup\/[a-f0-9-]+\/(?:backup|files|file-catalog-\d+|file-\d+)\.enc$/.test(entry.path))stats.baseBytesSent+=length;await onChunk?.({bytes:length,received:cursor.received,total:digest.bytes,object:hash});}
  if(incremental.digest('hex')!==hash){await assertDescriptorPath(output,partial,cursor.identity);cursor={...cursor,phase:'resetting',received:0,prefixDigest:createHash('sha256').digest('hex')};await writeStoreManifest(store,cursorPath,cursor,usage,{replace:true});fail('periodic_transfer_object_digest');}if(!sourceReader&&!same(entry.identity,await periodicCipherIdentity(entry.sourcePath,entry.uid)))fail('periodic_transfer_source_changed');signal?.throwIfAborted();await assertDescriptorPath(output,partial,cursor.identity);cursor={...cursor,phase:'publishing'};await writeStoreManifest(store,cursorPath,cursor,usage,{replace:true});await assertDescriptorPath(output,partial,cursor.identity);
  // link is atomic no-replace. EEXIST never removes or overwrites an unknown
  // target. The signed original inode permits exact two-name or object-only
  // recovery if the process stops before unlink/receipt completion.
  await reserve(store,usage,0,1);try{await link(partial,object);}catch(error){if(error.code==='EEXIST')fail('periodic_transfer_object_conflict');throw error;}await syncDirectory(dirname(object));await finishPublishedObject(store,output,{object,partial,cursor,receiptPath,digest,signal,usage,linked:true});stats.objectsReceived++;
 }finally{await Promise.allSettled([input?.close(),output?.close()]);}
}
// The first transport implementation is a bounded local stream between genuine
// capabilities. SSH framing can call the same operations later; no server,
// credential export, restore authorization or active-service mutation occurs.
export async function transferPeriodicIndex(index,store,{signal,onChunk}={}){
 const value=storeValue(store);assertPeriodicTransferIndexActive(index,{signal});if(value.pending||onChunk!==undefined&&typeof onChunk!=='function')fail('periodic_transfer_busy');const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));value.cancellation=cancellation;
 const operation=usePeriodicTransferIndex(index,async source=>{const lease=await acquireInstance(join(value.directory,'.periodic-object-store'));try{const usage=await storeUsage(value),prepared=await intake(value,source,combined,usage),stats={bytesSent:0,baseBytesSent:0,objectsReceived:0,objectsReused:0},seen=new Set();for(const entry of source.entries){if(seen.has(entry.digest.sha256))continue;seen.add(entry.digest.sha256);await receiveObject(value,entry,{signal:combined,onChunk,stats,usage});}
  return await finishPeriodicTransfer(value,prepared,{signal:combined,stats,usage});}finally{await lease.release();}},{signal:combined});value.pending=operation;let result;try{result=await operation;}finally{value.pending=null;value.cancellation=null;}combined.throwIfAborted();assertPeriodicTransferIndexActive(index,{signal:combined});storeValue(store);return result;
}

async function finishPeriodicTransfer(value,prepared,{signal,stats,usage}){
  for(const entry of prepared.entries)if(!await objectPresent(value,entry.digest))fail('periodic_transfer_object_missing');
  const completeEntry=prepared.entries.find(row=>row.path==='backup/'+prepared.manifest.backupId+'/points/'+prepared.manifest.pointId+'/backup-complete.enc');if(!completeEntry)fail('periodic_transfer_point_binding');const complete=await readManifest(join(value.directory,'objects',completeEntry.digest.sha256+'.enc'),value.applicationKey,{signal});if(complete.version!==3||complete.backupId!==prepared.manifest.backupId||complete.pointId!==prepared.manifest.pointId||complete.backupComplete!==true||complete.restoreVerified!==false||!Number.isSafeInteger(complete.checkpointAt)||!same(complete.checkpointDigest,prepared.manifest.checkpointDigest)||!same(completeEntry.digest,prepared.manifest.backupCompleteDigest))fail('periodic_transfer_point_binding');signal.throwIfAborted();const receipt={...common(value),backupId:prepared.manifest.backupId,pointId:prepared.manifest.pointId,handoffDigest:prepared.handoffDigest,checkpointAt:complete.checkpointAt,receivedAt:Date.now(),transportComplete:true,restoreVerified:false,offhostVerified:false,deploymentAllowed:false},path=join(prepared.target,'received.enc');if(await exists(path)){const prior=await readManifest(path,value.applicationKey,{signal});if(!bound(value,prior)||!same(prior.handoffDigest,prepared.handoffDigest))fail('periodic_transfer_point_conflict');}else await writeStoreManifest(value,path,receipt,usage);if(prepared.target===prepared.pending){await rename(prepared.pending,prepared.final);await syncDirectory(dirname(prepared.final));}return {state:'periodic_transport_complete',backupId:receipt.backupId,pointId:receipt.pointId,...stats,transportComplete:true,restoreVerified:false,offhostVerified:false,deploymentAllowed:false};
}

// Transport settings are operational DTOs only. No key or restore/activation
// authority leaves the store, and remote data never selects a local path.
export function periodicObjectStoreSettings(store){const value=storeValue(store);return {directory:value.directory,formal:value.formal,limits:{...value.limits}};}
async function remoteIndex(value,directory,handoffDigest,{signal}={}){
 if(typeof directory!=='string'||dirname(directory)!==join(value.directory,'incoming')||!/^i-[a-f0-9]{24}$/.test(directory.slice(directory.lastIndexOf('/')+1)))fail('periodic_transfer_inbox_required');await trustedDirectory(directory,{root:value.formal});
 if(!periodicCipherDigest(handoffDigest)||handoffDigest.bytes>1048576||!same(await digestFile(join(directory,'handoff.enc'),{signal}),handoffDigest))fail('periodic_transfer_index_changed');
 const manifest=await readManifest(join(directory,'handoff.enc'),value.applicationKey,{signal});
 if(manifest.version!==1||manifest.kind!=='periodic-application'||!uuid(manifest.backupId)||!uuid(manifest.pointId)||manifest.sourceMachine!==value.sourceMachine||manifest.expected?.system_id!==value.expectedCluster||manifest.keyVersion!==value.keyVersion||manifest.applicationKeyVersion!==value.applicationKeyVersion||manifest.applicationKeyDigest!==value.applicationKeyDigest||value.formal&&manifest.scope!=='existing-cluster')fail('periodic_transfer_source_binding');
 if(!Number.isSafeInteger(manifest.count)||manifest.count<1||manifest.count>value.limits.maxFiles||!Number.isSafeInteger(manifest.totalBytes)||manifest.totalBytes<36||manifest.totalBytes>value.limits.maxBundleBytes||!Array.isArray(manifest.parts)||!manifest.parts.length||manifest.parts.length>manifest.count)fail('periodic_transfer_index_capacity');
 let bytes=handoffDigest.bytes;for(const [ordinal,part] of manifest.parts.entries()){if(part.name!=='handoff-catalog-'+ordinal+'.enc'||!periodicCipherDigest(part.digest)||part.digest.bytes>1048576)fail('periodic_transfer_index_changed');bytes+=part.digest.bytes;}if(bytes+manifest.totalBytes>value.limits.maxBundleBytes)fail('periodic_transfer_index_capacity');
 return {directory,manifest,handoffDigest};
}
export async function describePeriodicRemoteHandoff(store,directory,handoffDigest,{signal}={}){return controlOperation(store,signal,async(value,combined)=>{const source=await remoteIndex(value,directory,handoffDigest,{signal:combined});return {backupId:source.manifest.backupId,pointId:source.manifest.pointId,parts:structuredClone(source.manifest.parts)};});}
export async function receiveRemotePeriodicIndex(store,{directory,handoffDigest},readRange,{signal,onChunk}={}){
 const value=storeValue(store);if(value.pending||typeof readRange!=='function'||onChunk!==undefined&&typeof onChunk!=='function')fail('periodic_transfer_busy');const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));value.cancellation=cancellation;
 const operation=(async()=>{const lease=await acquireInstance(join(value.directory,'.periodic-object-store'));try{
  const source=await remoteIndex(value,directory,handoffDigest,{signal:combined}),usage=await storeUsage(value),prepared=await intake(value,source,combined,usage),stats={bytesSent:0,baseBytesSent:0,objectsReceived:0,objectsReused:0},seen=new Set();
  const sourceReader=async entry=>({async read(buffer,offset,length,position){combined.throwIfAborted();const bytes=await readRange({digest:structuredClone(entry.digest),offset:position,bytes:length},{signal:combined});if(!Buffer.isBuffer(bytes)||bytes.length!==length)fail('periodic_transfer_source_changed');bytes.copy(buffer,offset);return {bytesRead:length,buffer};},async close(){}});
  for(const entry of prepared.entries){if(seen.has(entry.digest.sha256))continue;seen.add(entry.digest.sha256);await receiveObject(value,entry,{signal:combined,onChunk,stats,usage,sourceReader});}
  return await finishPeriodicTransfer(value,prepared,{signal:combined,stats,usage});
 }finally{await lease.release();}})();value.pending=operation;let result;try{result=await operation;}finally{value.pending=null;value.cancellation=null;}combined.throwIfAborted();storeValue(store);return result;
}

// Control pages are small but still consume the same persistent store budget.
// Every failed/interrupted inbox remains visible to storeUsage on the next run.
export async function createPeriodicTransferInbox(store,{signal}={}){
 return controlOperation(store,signal,async(value,combined)=>{const lease=await acquireInstance(join(value.directory,'.periodic-object-store'));try{await storeUsage(value);if((await readdir(join(value.directory,'incoming'))).length>=value.limits.maxPoints)fail('periodic_transfer_store_capacity');combined.throwIfAborted();storeValue(store);const directory=join(value.directory,'incoming','i-'+randomBytes(12).toString('hex'));await mkdir(directory,{mode:0o700});await syncDirectory(dirname(directory));const info=await lstat(directory);let owned=incomingOwnership.get(store);if(!owned){owned=new Map();incomingOwnership.set(store,owned);}owned.set(directory,{dev:info.dev,ino:info.ino,files:new Map()});return directory;}finally{await lease.release();}});
}
async function controlOperation(store,signal,callback){
 const value=storeValue(store);if(value.pending)fail('periodic_transfer_busy');const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));value.cancellation=cancellation;const pending=Promise.resolve().then(()=>{combined.throwIfAborted();storeValue(store);return callback(value,combined);});value.pending=pending;let result;try{result=await pending;}finally{value.pending=null;value.cancellation=null;}combined.throwIfAborted();storeValue(store);return result;
}
const incomingOwnership=new WeakMap();
async function ownInbox(store,directory){const owned=incomingOwnership.get(store)?.get(directory);if(!owned)fail('periodic_transfer_inbox_required');const info=await lstat(directory);if(!info.isDirectory()||info.isSymbolicLink()||info.dev!==owned.dev||info.ino!==owned.ino||info.uid!==process.getuid()||info.mode&0o077)fail('periodic_transfer_inbox_changed');return owned;}
export async function writePeriodicTransferControl(store,directory,name,bytes,digest,{signal}={}){
 const value=storeValue(store);if(typeof directory!=='string'||dirname(directory)!==join(value.directory,'incoming')||!/^i-[a-f0-9]{24}$/.test(directory.slice(directory.lastIndexOf('/')+1))||!/^handoff(?:-catalog-\d+)?\.enc$/.test(name)||!Buffer.isBuffer(bytes)||!periodicCipherDigest(digest)||bytes.length!==digest.bytes||bytes.length>1048576||createHash('sha256').update(bytes).digest('hex')!==digest.sha256)fail('periodic_transfer_control_invalid');
 const payload=Buffer.from(bytes);return controlOperation(store,signal,async(value,combined)=>{const lease=await acquireInstance(join(value.directory,'.periodic-object-store'));try{const owned=await ownInbox(store,directory),usage=await storeUsage(value);await reserve(value,usage,payload.length,1);combined.throwIfAborted();storeValue(store);const path=join(directory,name),file=await open(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);try{const info=await file.stat();owned.files.set(name,{dev:info.dev,ino:info.ino,maxBytes:payload.length});await file.writeFile(payload);await file.sync();}finally{await file.close();}if(!same(await digestFile(path,{signal:combined}),digest))fail('periodic_transfer_control_invalid');await syncDirectory(directory);return {bytes:payload.length};}finally{await lease.release();}});
}
export async function releasePeriodicTransferInbox(store,directory,{signal}={}){
 return controlOperation(store,signal,async(value,combined)=>{
  if(typeof directory!=='string'||dirname(directory)!==join(value.directory,'incoming')||!/^i-[a-f0-9]{24}$/.test(directory.slice(directory.lastIndexOf('/')+1)))fail('periodic_transfer_inbox_required');
  const lease=await acquireInstance(join(value.directory,'.periodic-object-store'));try{
   combined.throwIfAborted();
   // A lost acknowledgement or failed parent fsync may follow a successful
   // rmdir. Absence permits only a parent sync, never ownership of new files.
   if(!await exists(directory)){await syncDirectory(dirname(directory));incomingOwnership.get(store)?.delete(directory);return;}
   const owned=await ownInbox(store,directory),names=await readdir(directory),present=new Set(names);
   if(names.some(name=>!owned.files.has(name)))fail('periodic_transfer_inbox_changed');
   for(const name of names){combined.throwIfAborted();const path=join(directory,name),info=await lstat(path),expected=owned.files.get(name);if(!info.isFile()||info.isSymbolicLink()||info.dev!==expected.dev||info.ino!==expected.ino||info.uid!==process.getuid()||(info.mode&0o777)!==0o600||info.nlink!==1||info.size>expected.maxBytes)fail('periodic_transfer_inbox_changed');}
   // An unlink can succeed before its caller observes an error. Reconcile
   // missing owned names only after all surviving files pass validation.
   for(const name of owned.files.keys())if(!present.has(name))owned.files.delete(name);
   for(const name of names){combined.throwIfAborted();await unlink(join(directory,name));owned.files.delete(name);}
   await syncDirectory(directory);await ownInbox(store,directory);combined.throwIfAborted();await (await import('node:fs/promises')).rmdir(directory);await syncDirectory(dirname(directory));incomingOwnership.get(store).delete(directory);
  }finally{await lease.release();}
 });
}

function bundleCopyId(relative){return createHash('sha256').update(relative).digest('hex');}
function validBundleRelative(relative){
 if(typeof relative!=='string')return false;const at=relative.indexOf('/'),pair=relative.slice(0,at).split('--'),path=relative.slice(at+1);return at>0&&pair.length===2&&pair.every(uuid)&&(/^(?:handoff|handoff-catalog-\d+)\.enc$/.test(path)||validHandoffPath(path,pair[0],'periodic-application',pair[1]));
}
async function bundleCopyPlan(store,id){
 if(!sha(id))fail('periodic_transfer_bundle_changed');const path=join(store.directory,'copies',id+'.plan.enc'),plan=await readManifest(path,store.applicationKey);
 if(!bound(store,plan)||plan.purpose!=='bundle-copy-v1'||!validBundleRelative(plan.destination)||bundleCopyId(plan.destination)!==id||typeof plan.temporary!=='string'||!new RegExp('^c-'+id+'-[a-f0-9]{24}\\.enc$').test(plan.temporary)||!periodicCipherDigest(plan.digest)||plan.identity?.nlink!=='1'||plan.identity.size!==String(plan.digest.bytes))fail('periodic_transfer_bundle_changed');
 return {plan,path,temporary:join(store.directory,'copies',plan.temporary),destination:join(store.directory,'bundles',plan.destination)};
}
async function bundlePublishingPair(store,path){
 const root=join(store.directory,'bundles')+'/',match=dirname(path)===join(store.directory,'copies')?/^c-([a-f0-9]{64})-[a-f0-9]{24}\.enc$/.exec(path.slice(path.lastIndexOf('/')+1)):null,id=path.startsWith(root)?bundleCopyId(path.slice(root.length)):match?.[1];
 const copy=await bundleCopyPlan(store,id),first=await targetPathIdentity(copy.temporary,2),second=await targetPathIdentity(copy.destination,2);
 if(![copy.temporary,copy.destination].includes(path)||!same(first,second)||!sameAllocation(copy.plan.identity,first))fail('periodic_transfer_store_untrusted');return first;
}
// A fully copied file is journaled before its no-replace publication. A crash
// during the earlier copy leaves an unreferenced, quota-counted temporary file,
// never a truncated final file. Unknown leftovers are preserved.
async function publishBundleCopy(store,row,destination,usage,signal){
 const relative=destination.slice(join(store.directory,'bundles').length+1),id=bundleCopyId(relative),planPath=join(store.directory,'copies',id+'.plan.enc');if(!validBundleRelative(relative))fail('periodic_transfer_bundle_changed');let copied=0;
 if(!await exists(planPath)){
  if(await exists(destination)){if(!same(await digestFile(destination,{signal}),row.digest))fail('periodic_transfer_bundle_changed');return 0;}
  await reserve(store,usage,row.digest.bytes,1);const temporaryName='c-'+id+'-'+randomBytes(12).toString('hex')+'.enc',temporary=join(store.directory,'copies',temporaryName);
  await copyBackupCipher(row.source,temporary,{signal,maxBytes:row.digest.bytes});if(!same(await digestFile(temporary,{signal}),row.digest))fail('periodic_transfer_bundle_changed');
  const identity=await periodicCipherIdentity(temporary);await writeStoreManifest(store,planPath,{...common(store),purpose:'bundle-copy-v1',destination:relative,temporary:temporaryName,digest:row.digest,identity},usage);copied=row.digest.bytes;
 }
 const copy=await bundleCopyPlan(store,id);if(copy.destination!==destination||!same(copy.plan.digest,row.digest))fail('periodic_transfer_bundle_changed');
 if(!await exists(destination)){
  if(!sameAllocation(copy.plan.identity,await periodicCipherIdentity(copy.temporary))||!same(await digestFile(copy.temporary,{signal}),row.digest))fail('periodic_transfer_bundle_changed');
  await mkdir(dirname(destination),{recursive:true,mode:0o700});await reserve(store,usage,0,1);signal.throwIfAborted();try{await link(copy.temporary,destination);}catch(error){if(error.code==='EEXIST')fail('periodic_transfer_bundle_conflict');throw error;}await syncDirectory(dirname(destination));await syncDirectory(dirname(copy.temporary));
 }
 const paired=await exists(copy.temporary),identity=await targetPathIdentity(destination,paired?2:1);if(!sameAllocation(copy.plan.identity,identity)||!same(await digestFile(destination,{signal}),row.digest))fail('periodic_transfer_bundle_changed');
 if(paired){if(!same(identity,await targetPathIdentity(copy.temporary,2)))fail('periodic_transfer_bundle_changed');signal.throwIfAborted();await unlink(copy.temporary);usage.files--;await syncDirectory(dirname(copy.temporary));}
 const final=await periodicCipherIdentity(destination);if(!sameAllocation(copy.plan.identity,final))fail('periodic_transfer_bundle_changed');const control=await periodicCipherIdentity(planPath);signal.throwIfAborted();await unlink(planPath);usage.files--;usage.bytes-=Number(control.size);await syncDirectory(dirname(planPath));await syncDirectory(dirname(destination));return copied;
}

// This copy is explicit and independent of the light receive/producer cycle.
// The cache remains immutable; only the existing typed receiver can later grant
// a live restore receipt. Root quotas include the complete second physical copy.
export async function materializePeriodicStoredBundle(store,backupId,pointId,{signal}={}){
 const value=storeValue(store);if(!uuid(backupId)||!uuid(pointId)||value.pending)fail('periodic_transfer_busy');const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));value.cancellation=cancellation;
 const operation=(async()=>{const lease=await acquireInstance(join(value.directory,'.periodic-object-store'));try{
  const directory=join(value.directory,'points',backupId+'--'+pointId);await trustedDirectory(directory,{root:value.formal});const receipt=await readManifest(join(directory,'received.enc'),value.applicationKey,{signal:combined});if(!bound(value,receipt)||receipt.backupId!==backupId||receipt.pointId!==pointId||receipt.transportComplete!==true||receipt.restoreVerified!==false||receipt.offhostVerified!==false||receipt.deploymentAllowed!==false)fail('periodic_transfer_point_binding');
  const manifest=await readManifest(join(directory,'handoff.enc'),value.applicationKey,{signal:combined}),source={directory,manifest,handoffDigest:receipt.handoffDigest},usage=await storeUsage(value),prepared=await intake(value,source,combined,usage),target=join(value.directory,'bundles',backupId+'--'+pointId);
  if(!await exists(target)){await mkdir(target,{mode:0o700});await syncDirectory(dirname(target));}await trustedDirectory(target,{root:value.formal});
  const files=[...prepared.entries.map(entry=>({path:entry.path,digest:entry.digest,source:join(value.directory,'objects',entry.digest.sha256+'.enc')})),...manifest.parts.map(part=>({path:part.name,digest:part.digest,source:join(directory,part.name)})),{path:'handoff.enc',digest:receipt.handoffDigest,source:join(directory,'handoff.enc')}],allowed=new Set(files.map(row=>row.path)),parents=new Set();
  for(const row of files)for(let at=dirname(row.path);at!=='.';at=dirname(at))parents.add(at);
  const verifyLayout=async at=>{for(const name of await readdir(at)){const path=join(at,name),relative=path.slice(target.length+1),info=await lstat(path);if(info.isSymbolicLink()||info.uid!==process.getuid()||info.mode&0o077)fail('periodic_transfer_bundle_changed');if(info.isDirectory()){if(!parents.has(relative))fail('periodic_transfer_bundle_changed');await verifyLayout(path);}else{if(!info.isFile()||![1,2].includes(info.nlink)||!allowed.has(relative))fail('periodic_transfer_bundle_changed');if(info.nlink===2)await bundlePublishingPair(value,path);}}};await verifyLayout(target);
  let bytesCopied=0;for(const row of files){combined.throwIfAborted();const path=join(target,row.path);
   if(row.path.startsWith('backup/')||row.path.startsWith('wal/')){if(!await objectPresent(value,row.digest))fail('periodic_transfer_object_missing');}
   bytesCopied+=await publishBundleCopy(value,row,path,usage,combined);
  }await verifyLayout(target);combined.throwIfAborted();return {state:'periodic_bundle_materialized',backupId,pointId,bundleDirectory:target,handoffDigest:structuredClone(receipt.handoffDigest),bytesCopied,restoreVerified:false,offhostVerified:false,deploymentAllowed:false};
 }finally{await lease.release();}})();value.pending=operation;let result;try{result=await operation;}finally{value.pending=null;value.cancellation=null;}combined.throwIfAborted();storeValue(store);return result;
}
