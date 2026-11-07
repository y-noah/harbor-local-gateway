import {open,lstat,realpath,readdir,mkdir} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join,resolve,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource} from './postgres-backup.mjs';
import {authenticatePeriodicApplicationBackup,periodicApplicationBackupSource} from './postgres-periodic-application-backup.mjs';
import {periodicApplicationCaptureSource} from './postgres-periodic-application-materials.mjs';
import {postgresBackupRecoveryPointSource} from './postgres-backup-recovery-point.mjs';
import {periodicApplicationKey,periodicApplicationKeyBinding} from './postgres-periodic-application-key.mjs';
import {backupMachineIdentity,validHandoffPath,copyBackupCipher,handoffControlEntries} from './postgres-backup-handoff.mjs';
import {readWalCheckpointSegments} from './postgres-backup-wal-catalog.mjs';
import {readWalManifest} from './postgres-recovery.mjs';
import {acquireInstance} from './instance.mjs';
import {canonical,readManifest,writeManifest,digestFile,privateDirectory,syncDirectory,transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError(code);},same=(a,b)=>canonical(a)===canonical(b),hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
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

async function exists(path){try{await lstat(path);return true;}catch(error){if(error.code==='ENOENT')return false;throw error;}}
async function trustedDirectory(path){await privateDirectory(path);if(await realpath(path)!==path)fail('periodic_transfer_index_directory');}
// Authenticate all existing copied payloads, without requiring a full copy.
// Absent payloads remain references to the original source closure.
export async function inspectCanonicalPeriodicCopies(directory,manifest,entries,{signal}={}){
 await trustedDirectory(directory);const files=new Map(entries.map(row=>[row.path,row.digest])),controls=new Set(['handoff.enc',...manifest.parts.map(part=>part.name)]),parents=new Set(),present=new Set();
 for(const path of files.keys())for(let at=dirname(path);at!=='.';at=dirname(at))parents.add(at);
 async function walk(at){for(const name of await readdir(at)){signal?.throwIfAborted();const absolute=join(at,name),relative=absolute.slice(directory.length+1),info=await lstat(absolute);
  if(info.isDirectory()&&!info.isSymbolicLink()){if(!parents.has(relative))fail('periodic_transfer_index_conflict');await trustedDirectory(absolute);await walk(absolute);}
  else{await periodicCipherIdentity(absolute);if(controls.has(relative))continue;const expected=files.get(relative);if(!expected||!same(await digestFile(absolute,{signal}),expected))fail('periodic_transfer_index_conflict');present.add(relative);}
 }}await walk(directory);return present;
}
async function savedControls(directory,claims,entries,completedAt,{signal,allowCopies=true}={}){
 const prior=await readManifest(join(directory,'handoff.enc'),claims.applicationKey,{signal}),{parts,created,...saved}=prior,{applicationKey,...expected}=claims;
 if(!Array.isArray(parts)||!parts.length||parts.length>entries.length||!same(saved,expected)||!Number.isSafeInteger(created)||created<completedAt||created>Date.now())fail('periodic_transfer_index_conflict');
 const actual=[];for await(const entry of handoffControlEntries({directory,key:applicationKey,manifest:prior,signal,maxFiles:entries.length,maxBytes:prior.totalBytes+1048576+(parts??[]).reduce((n,row)=>n+(row.digest?.bytes??0),0)}))actual.push(entry);
 if(!same(actual,entries.map(({path,digest})=>({path,digest}))))fail('periodic_transfer_index_conflict');
 if(!allowCopies){const allowed=new Set(['handoff.enc',...prior.parts.map(part=>part.name)]);for(const name of await readdir(directory))if(!allowed.has(name))fail('periodic_transfer_index_conflict');}await inspectCanonicalPeriodicCopies(directory,prior,entries,{signal});return prior;
}
async function copyControls(from,to,manifest,{signal}={}){
 const names=[...manifest.parts.map(part=>({name:part.name,digest:part.digest})),{name:'handoff.enc',digest:await digestFile(join(from,'handoff.enc'),{signal})}];
 const allowed=new Set(names.map(row=>row.name));for(const name of await readdir(to))if(!allowed.has(name))fail('periodic_transfer_index_conflict');
 for(const row of names){signal?.throwIfAborted();const path=join(to,row.name);if(await exists(path)){await periodicCipherIdentity(path);if(!same(await digestFile(path,{signal}),row.digest))fail('periodic_transfer_index_conflict');}else await copyBackupCipher(join(from,row.name),path,{maxBytes:row.digest.bytes,signal});if(!same(await digestFile(path,{signal}),row.digest))fail('periodic_transfer_index_changed');}
 await syncDirectory(to);
}
// Caller retains its existing backup/archive operation lease. This lock only
// serializes the one canonical control closure with manual materialization.
export async function withCanonicalPeriodicHandoff(context,proof,pointId,{mirrorDirectory,signal}={},callback){
 const record=postgresBackupContext(context),source=postgresBackupRestoreSource(proof);if(source.context!==context||record.closed||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)fail('periodic_transfer_source_required');
 const archiveKey=Buffer.from(record.key),applicationKey=Buffer.from(periodicApplicationKey(record));let lease;try{
 const backup=await authenticatePeriodicApplicationBackup(context,proof,pointId,{signal}),closed=periodicApplicationBackupSource(backup),point=postgresBackupRecoveryPointSource(closed.point),capture=periodicApplicationCaptureSource(closed.capture),base=join(record.artifactDirectory,proof.backupId),prefix='backup/'+proof.backupId+'/',entries=[],seen=new Set(),directory=join(point.directory,'handoff');
 if(mirrorDirectory){if(typeof mirrorDirectory!=='string'||resolve(mirrorDirectory)!==mirrorDirectory||[record.artifactDirectory,record.archiveDirectory,...capture.configuration.materialRoots.map(root=>root.directory)].some(root=>mirrorDirectory===root||mirrorDirectory.startsWith(root+'/')))fail('periodic_transfer_index_directory');await trustedDirectory(mirrorDirectory);}
 try{lease=await acquireInstance(directory);}catch(error){if(error.code==='EADDRINUSE')fail('periodic_application_handoff_busy');throw error;}
 await mkdir(directory,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await trustedDirectory(directory);
  const add=async(path,from,digest,uid=process.getuid())=>{signal?.throwIfAborted();if(seen.has(path))fail('periodic_transfer_duplicate_path');if(!validHandoffPath(path,proof.backupId,'periodic-application',pointId)||!periodicCipherDigest(digest))fail('periodic_transfer_entry_invalid');const identity=await periodicCipherIdentity(from,uid);if(Number(identity.size)!==digest.bytes)fail('periodic_transfer_source_changed');if(entries.length>=1000000)fail('backup_handoff_capacity');seen.add(path);const entry=Object.freeze({path,sourcePath:from,digest:Object.freeze({sha256:digest.sha256,bytes:digest.bytes}),identity:Object.freeze(identity),uid});entries.push(entry);};
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

  const recheck=async()=>{signal?.throwIfAborted();if(record.closed)fail('backup_context_closed');if(!same(await digestFile(join(point.directory,'backup-complete.enc'),{signal}),closed.manifestDigest)||!same(await digestFile(join(base,'backup.enc'),{signal}),point.manifest.baseManifestDigest))fail('periodic_transfer_source_changed');if(record.closed)fail('backup_context_closed');signal?.throwIfAborted();};
  const boundClaims={...claims,applicationKey};
  if(await exists(join(directory,'handoff.enc')))manifest=await savedControls(directory,boundClaims,entries,closed.manifest.completedAt,{signal});
  else if(mirrorDirectory&&await exists(join(mirrorDirectory,'handoff.enc'))){
   manifest=await savedControls(mirrorDirectory,boundClaims,entries,closed.manifest.completedAt,{signal,allowCopies:false});await recheck();await copyControls(mirrorDirectory,directory,manifest,{signal});
  }else{
   if(mirrorDirectory){const pendingMirror=new Set(await readdir(mirrorDirectory));for(const {name,value} of catalogPages(entries)){if(!pendingMirror.has(name))continue;const from=join(mirrorDirectory,name),to=join(directory,name);await periodicCipherIdentity(from);if(!same(await readManifest(from,applicationKey,{signal}),value))fail('periodic_transfer_index_conflict');const digest=await digestFile(from,{signal});if(await exists(to)){if(!same(await digestFile(to,{signal}),digest))fail('periodic_transfer_index_conflict');}else await copyBackupCipher(from,to,{maxBytes:digest.bytes,signal});pendingMirror.delete(name);}if(pendingMirror.size)fail('periodic_transfer_index_conflict');}
   const existingNames=await readdir(directory),pending=new Set(existingNames),reused=new Map();
   for(const {name,value} of catalogPages(entries)){if(!pending.has(name))continue;const path=join(directory,name);await periodicCipherIdentity(path);if(!same(await readManifest(path,applicationKey,{signal}),value))fail('periodic_transfer_index_conflict');reused.set(name,await digestFile(path,{signal}));pending.delete(name);}
   if(pending.size)fail('periodic_transfer_index_conflict');
   const parts=[];for(const {name,value} of catalogPages(entries)){signal?.throwIfAborted();if(!reused.has(name))await writeManifest(join(directory,name),value,applicationKey);const digest=await digestFile(join(directory,name),{signal});if(reused.has(name)&&!same(digest,reused.get(name)))fail('periodic_transfer_index_changed');parts.push({name,count:value.entries.length,digest});}
   manifest={...claims,parts,created:Date.now()};await recheck();await writeManifest(join(directory,'handoff.enc'),manifest,applicationKey);await syncDirectory(point.directory);
  }
  await recheck();if(mirrorDirectory)await copyControls(directory,mirrorDirectory,manifest,{signal});
  await recheck();const handoffDigest=await digestFile(join(directory,'handoff.enc'),{signal});if(!same(manifest,await readManifest(join(directory,'handoff.enc'),applicationKey,{signal})))fail('periodic_transfer_index_changed');
  const result=await callback({directory,manifest,entries,handoffDigest,source,closed,point,capture});await recheck();if(!same(handoffDigest,await digestFile(join(directory,'handoff.enc'),{signal})))fail('periodic_transfer_index_changed');return result;
 }finally{try{await lease?.release();}finally{archiveKey.fill(0);applicationKey.fill(0);}}
}
