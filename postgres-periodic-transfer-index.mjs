import {open,lstat,realpath,readdir} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join,resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource,revalidatePeriodicSourceBase} from './postgres-backup.mjs';
import {authenticatePeriodicApplicationBackup,periodicApplicationBackupSource} from './postgres-periodic-application-backup.mjs';
import {periodicApplicationCaptureSource} from './postgres-periodic-application-materials.mjs';
import {postgresBackupRecoveryPointSource} from './postgres-backup-recovery-point.mjs';
import {periodicApplicationKey,periodicApplicationKeyBinding} from './postgres-periodic-application-key.mjs';
import {backupMachineIdentity,validHandoffPath} from './postgres-backup-handoff.mjs';
import {readWalCheckpointSegments} from './postgres-backup-wal-catalog.mjs';
import {readWalManifest} from './postgres-recovery.mjs';
import {withPostgresBackupOperation} from './postgres-backup-lock.mjs';
import {canonical,readManifest,writeManifest,digestFile,privateDirectory,transferError} from './migration-artifacts.mjs';

const indexes=new WeakMap(),sourceEntries=new WeakMap(),fail=code=>{throw transferError(code);},same=(a,b)=>canonical(a)===canonical(b),hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
export const periodicCipherDigest=value=>value&&typeof value==='object'&&Number.isSafeInteger(value.bytes)&&value.bytes>=36&&/^[a-f0-9]{64}$/.test(value.sha256??'');
export async function periodicCipherIdentity(path,uid=process.getuid()){
 const info=await lstat(path,{bigint:true});if(!info.isFile()||info.isSymbolicLink()||info.uid!==BigInt(uid)||(info.mode&0o777n)!==0o600n||info.nlink!==1n)fail('periodic_transfer_file_untrusted');
 return Object.fromEntries(['dev','ino','uid','mode','nlink','size','mtimeNs','ctimeNs'].map(name=>[name,String(info[name])]));
}
function sourceCredentials(options){
 if(options.connection){const {user,password,database}=options.connection;if(![user,password,database].every(value=>typeof value==='string'&&value))fail('backup_handoff_credentials_required');return {user,password,database};}
 const address=new URL(options.connectionString);return {user:decodeURIComponent(address.username),password:decodeURIComponent(address.password),database:decodeURIComponent(address.pathname.slice(1))};
}
function* catalogPages(entries){
 let chunk=[],bytes=0,ordinal=0;
 for(const row of entries){const entry={path:row.path,digest:row.digest},size=Buffer.byteLength(JSON.stringify(entry));if(bytes+size>524288&&chunk.length){yield {name:'handoff-catalog-'+ordinal+'.enc',value:{version:1,ordinal,entries:chunk}};ordinal++;chunk=[];bytes=0;}chunk.push(entry);bytes+=size;}
 if(chunk.length)yield {name:'handoff-catalog-'+ordinal+'.enc',value:{version:1,ordinal,entries:chunk}};
}
// The on-disk output is the existing typed handoff manifest/catalog protocol.
// Payload bytes stay in the original immutable files: no full-base copy or
// source hardlink is made. Only the live source proof can authorize an index.
export async function createPeriodicTransferIndex(context,proof,pointId,{directory,signal}={}){
 const record=postgresBackupContext(context),source=postgresBackupRestoreSource(proof);if(source.context!==context||record.closed||record.pendingBinding||record.resumeBinding||record.receiverBinding||record.applicationPlan)fail('periodic_transfer_source_required');
 if(typeof directory!=='string'||resolve(directory)!==directory||directory===record.artifactDirectory||directory.startsWith(record.artifactDirectory+'/')||directory===record.archiveDirectory||directory.startsWith(record.archiveDirectory+'/'))fail('periodic_transfer_index_directory');
 await privateDirectory(directory);if(await realpath(directory)!==directory)fail('periodic_transfer_index_directory');const existingNames=await readdir(directory),resuming=existingNames.includes('handoff.enc');if(!resuming&&existingNames.some(name=>!/^handoff-catalog-(?:0|[1-9]\d*)\.enc$/.test(name)))fail('periodic_transfer_index_incomplete');
 const archiveKey=Buffer.from(record.key),applicationKey=Buffer.from(periodicApplicationKey(record));
 const lifetime={context,active:false};let completed;try{completed=await withPostgresBackupOperation(record,async()=>{
  await revalidatePeriodicSourceBase(proof,{signal});const backup=await authenticatePeriodicApplicationBackup(context,proof,pointId,{signal}),closed=periodicApplicationBackupSource(backup),point=postgresBackupRecoveryPointSource(closed.point),capture=periodicApplicationCaptureSource(closed.capture),base=join(record.artifactDirectory,proof.backupId),prefix='backup/'+proof.backupId+'/',entries=[],seen=new Set();
  if(capture.configuration.materialRoots.some(root=>directory===root.directory||directory.startsWith(root.directory+'/')))fail('periodic_transfer_index_directory');
  const add=async(path,from,digest,uid=process.getuid())=>{signal?.throwIfAborted();if(seen.has(path))fail('periodic_transfer_duplicate_path');if(!validHandoffPath(path,proof.backupId,'periodic-application',pointId)||!periodicCipherDigest(digest))fail('periodic_transfer_entry_invalid');const identity=await periodicCipherIdentity(from,uid);if(Number(identity.size)!==digest.bytes)fail('periodic_transfer_source_changed');if(entries.length>=1000000)fail('backup_handoff_capacity');seen.add(path);const entry=Object.freeze({path,sourcePath:from,digest:Object.freeze({sha256:digest.sha256,bytes:digest.bytes}),identity:Object.freeze(identity),uid});sourceEntries.set(entry,lifetime);entries.push(entry);};
  const addControl=async(path,from,uid=process.getuid())=>add(path,from,await digestFile(from,{signal}),uid);
  const fileSet=async(root,key,relative,expected)=>{
   const manifest=await readManifest(join(root,'files.enc'),key,{signal});if(!same(await digestFile(join(root,'files.enc'),{signal}),expected))fail('periodic_transfer_catalog_changed');await add(relative+'files.enc',join(root,'files.enc'),expected);
   for(const descriptor of manifest.catalog){if(!/^file-catalog-\d+\.enc$/.test(descriptor.name)||!same(await digestFile(join(root,descriptor.name),{signal}),descriptor.digest))fail('periodic_transfer_catalog_changed');await add(relative+descriptor.name,join(root,descriptor.name),descriptor.digest);const page=await readManifest(join(root,descriptor.name),key,{signal});for(const row of page.entries)if(row.kind==='file'){if(!/^file-\d+\.enc$/.test(row.artifact??''))fail('periodic_transfer_catalog_changed');await add(relative+row.artifact,join(root,row.artifact),row.encrypted);}}
  };
  await addControl(prefix+'backup.enc',join(base,'backup.enc'));await fileSet(base,archiveKey,prefix,source.metadata.files.manifestDigest);
  const pointPrefix=prefix+'points/'+pointId+'/';for(const name of ['point.enc','checkpoint.enc','backup-complete.enc',...point.checkpoint.walCatalog.parts.map(row=>row.name)])await addControl(pointPrefix+name,join(point.directory,name));
  const capturePrefix=prefix+'application-staging/'+pointId+'/';await addControl(capturePrefix+'capture.enc',join(capture.directory,'capture.enc'));await fileSet(capture.directory,applicationKey,capturePrefix,capture.manifest.files.manifestDigest);
  const archive=record.archiveDirectory;await addControl('wal/cluster.enc',join(archive,'cluster.enc'),record.restoreUid);const walSeen=new Set();
  const wal=async(name,expected)=>{if(walSeen.has(name))return;walSeen.add(name);if(!/^(?:[0-9A-F]{24}|[0-9A-F]{8}\.history)$/.test(name))fail('periodic_transfer_wal_binding');const metadata=await readWalManifest(join(archive,name+'.meta.enc'),archiveKey,{ownerUid:record.restoreUid});if(metadata.version!==2||metadata.systemIdentifier!==record.expected.system_id||metadata.identity?.name!==name||expected&&!same(metadata.encrypted,expected))fail('periodic_transfer_wal_binding');await add('wal/'+name+'.enc',join(archive,name+'.enc'),metadata.encrypted,record.restoreUid);await addControl('wal/'+name+'.meta.enc',join(archive,name+'.meta.enc'),record.restoreUid);if(metadata.identity.history)await wal(metadata.identity.history.name);};
  for await(const row of readWalCheckpointSegments({directory:point.directory,key:archiveKey,checkpoint:point.checkpoint,signal}))await wal(row.name,row.encrypted);
  let totalBytes=0;for(const row of entries){totalBytes+=row.digest.bytes;if(!Number.isSafeInteger(totalBytes))fail('backup_handoff_capacity');}
  const claims={version:1,...periodicApplicationKeyBinding(record),kind:'periodic-application',backupId:proof.backupId,pointId,scope:record.scope,sourceMachine:await backupMachineIdentity(),configurationDigest:record.configurationDigest,keyVersion:record.keyVersion,expected:record.expected,credentials:sourceCredentials(record.adminOptions),backupCompleteDigest:closed.manifestDigest,checkpointDigest:point.manifest.checkpointDigest,captureManifestHash:hash(capture.manifest),pointManifestHash:hash(point.manifest),applicationPolicyDigest:closed.manifest.applicationPolicyDigest,count:entries.length,totalBytes};let manifest;
  if(resuming){
   const prior=await readManifest(join(directory,'handoff.enc'),applicationKey,{signal}),{parts,created,...savedClaims}=prior;
   if(!same(savedClaims,claims)||!Number.isSafeInteger(created)||created<closed.manifest.completedAt||created>Date.now()||!Array.isArray(parts)||!parts.length||parts.length>entries.length)fail('periodic_transfer_index_conflict');
   const allowed=new Set(['handoff.enc',...parts.map(row=>row.name)]);if(existingNames.length!==allowed.size||existingNames.some(name=>!allowed.has(name)))fail('periodic_transfer_index_conflict');
   let offset=0;for(const [ordinal,part] of parts.entries()){if(part.name!=='handoff-catalog-'+ordinal+'.enc'||!periodicCipherDigest(part.digest)||!same(await digestFile(join(directory,part.name),{signal}),part.digest))fail('periodic_transfer_index_conflict');const page=await readManifest(join(directory,part.name),applicationKey,{signal});if(page.version!==1||page.ordinal!==ordinal||!Array.isArray(page.entries)||page.entries.length!==part.count||!page.entries.length||!same(page.entries,entries.slice(offset,offset+part.count).map(({path,digest})=>({path,digest}))))fail('periodic_transfer_index_conflict');offset+=part.count;}if(offset!==entries.length)fail('periodic_transfer_index_conflict');manifest=prior;
  }else{
   // A cancelled first preparation may have published catalog pages without
   // its final handoff. Reuse only authenticated pages matching this live
   // source exactly; validate all survivors before writing anything new.
   const pending=new Set(existingNames),reused=new Map();
   for(const {name,value} of catalogPages(entries)){if(!pending.has(name))continue;const path=join(directory,name);await periodicCipherIdentity(path);if(!same(await readManifest(path,applicationKey,{signal}),value))fail('periodic_transfer_index_conflict');reused.set(name,await digestFile(path,{signal}));pending.delete(name);}
   if(pending.size)fail('periodic_transfer_index_conflict');
   const parts=[];for(const {name,value} of catalogPages(entries)){signal?.throwIfAborted();if(!reused.has(name))await writeManifest(join(directory,name),value,applicationKey);const digest=await digestFile(join(directory,name),{signal});if(reused.has(name)&&!same(digest,reused.get(name)))fail('periodic_transfer_index_changed');parts.push({name,count:value.entries.length,digest});}manifest={...claims,parts,created:Date.now()};
  }
  await revalidatePeriodicSourceBase(proof,{signal});if(!same(await digestFile(join(point.directory,'backup-complete.enc'),{signal}),closed.manifestDigest))fail('periodic_transfer_source_changed');signal?.throwIfAborted();if(record.closed)fail('backup_context_closed');if(!resuming)await writeManifest(join(directory,'handoff.enc'),manifest,applicationKey);
  const handoffDigest=await digestFile(join(directory,'handoff.enc'),{signal});if(!same(manifest,await readManifest(join(directory,'handoff.enc'),applicationKey,{signal})))fail('periodic_transfer_index_changed');
  return {context,proof,directory,manifest,entries:Object.freeze(entries),handoffDigest,lifetime};
 },{signal});}finally{archiveKey.fill(0);applicationKey.fill(0);}
 // All digest reads and archive-lease cleanup finish before this synchronous
 // publication boundary. A close cannot occur across another awaited step.
 signal?.throwIfAborted();if(record.closed)fail('backup_context_closed');const token=Object.freeze({backupId:proof.backupId,pointId});lifetime.active=true;indexes.set(token,completed);return token;
}
function activeIndex(index,{signal}={}){signal?.throwIfAborted();const value=indexes.get(index);if(!value||!value.lifetime.active)fail('periodic_transfer_index_required');if(postgresBackupContext(value.context).closed)fail('backup_context_closed');return value;}
export function assertPeriodicTransferIndexActive(index,options){activeIndex(index,options);}
export function inspectPeriodicTransferIndex(index){const value=activeIndex(index);return {backupId:index.backupId,pointId:index.pointId,files:value.entries.length,bytes:value.manifest.totalBytes,handoffDigest:structuredClone(value.handoffDigest),restoreVerified:false,offhostVerified:false,deploymentAllowed:false};}
export function releasePeriodicTransferIndex(index){const value=indexes.get(index);if(value)value.lifetime.active=false;indexes.delete(index);}
// Internal orchestration uses the real capability under the archive lease.
// The callback is code, not a user-supplied transport DTO or HTTP operation.
export async function usePeriodicTransferIndex(index,callback,{signal}={}){
 const value=activeIndex(index,{signal});if(typeof callback!=='function')fail('periodic_transfer_index_required');const record=postgresBackupContext(value.context);
 const result=await withPostgresBackupOperation(record,async()=>{await revalidatePeriodicSourceBase(value.proof,{signal});if(!same(await digestFile(join(value.directory,'handoff.enc'),{signal}),value.handoffDigest))fail('periodic_transfer_index_changed');activeIndex(index,{signal});const returned=await callback(value);await revalidatePeriodicSourceBase(value.proof,{signal});return returned;},{signal});
 // The archive lease's final assertions and FD-close also precede this fence.
 activeIndex(index,{signal});return result;
}

export async function openPeriodicTransferSource(entry,{signal}={}){
 const assertEntry=()=>{signal?.throwIfAborted();const life=sourceEntries.get(entry);if(!life?.active)fail('periodic_transfer_entry_capability_required');if(postgresBackupContext(life.context).closed)fail('backup_context_closed');};assertEntry();const file=await open(entry.sourcePath,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const info=await file.stat({bigint:true}),identity=Object.fromEntries(['dev','ino','uid','mode','nlink','size','mtimeNs','ctimeNs'].map(name=>[name,String(info[name])]));if(!same(identity,entry.identity)||!same(await periodicCipherIdentity(entry.sourcePath,entry.uid),identity))fail('periodic_transfer_source_changed');assertEntry();return file;}catch(error){await file.close();throw error;}
}
