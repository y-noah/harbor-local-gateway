import {open,lstat} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join,resolve} from 'node:path';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource,revalidatePeriodicSourceBase} from './postgres-backup.mjs';
import {withPostgresBackupOperation} from './postgres-backup-lock.mjs';
import {canonical,digestFile,transferError} from './migration-artifacts.mjs';
import {withCanonicalPeriodicHandoff,periodicCipherIdentity,periodicCipherDigest} from './postgres-periodic-handoff.mjs';
export {periodicCipherIdentity,periodicCipherDigest};
const indexes=new WeakMap(),sourceEntries=new WeakMap(),fail=code=>{throw transferError(code);},same=(a,b)=>canonical(a)===canonical(b);
export async function createPeriodicTransferIndex(context,proof,pointId,{directory,signal}={}){
 const record=postgresBackupContext(context),source=postgresBackupRestoreSource(proof);if(source.context!==context||record.closed||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)fail('periodic_transfer_source_required');
 if(typeof directory!=='string'||resolve(directory)!==directory||directory===record.artifactDirectory||directory.startsWith(record.artifactDirectory+'/')||directory===record.archiveDirectory||directory.startsWith(record.archiveDirectory+'/'))fail('periodic_transfer_index_directory');
 const lifetime={context,active:false};const completed=await withPostgresBackupOperation(record,async()=>{
  await revalidatePeriodicSourceBase(proof,{signal});const result=await withCanonicalPeriodicHandoff(context,proof,pointId,{mirrorDirectory:directory,signal},async prepared=>{
   const entries=prepared.entries.map(entry=>{sourceEntries.set(entry,lifetime);return entry;});
   return {context,proof,directory,canonicalDirectory:prepared.directory,manifest:prepared.manifest,entries:Object.freeze(entries),handoffDigest:prepared.handoffDigest,lifetime};
  });await revalidatePeriodicSourceBase(proof,{signal});return result;
 },{signal});
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
 const result=await withPostgresBackupOperation(record,async()=>{await revalidatePeriodicSourceBase(value.proof,{signal});if(!same(await digestFile(join(value.directory,'handoff.enc'),{signal}),value.handoffDigest)||!same(await digestFile(join(value.canonicalDirectory,'handoff.enc'),{signal}),value.handoffDigest))fail('periodic_transfer_index_changed');activeIndex(index,{signal});const returned=await callback(value);if(!same(await digestFile(join(value.canonicalDirectory,'handoff.enc'),{signal}),value.handoffDigest))fail('periodic_transfer_index_changed');await revalidatePeriodicSourceBase(value.proof,{signal});return returned;},{signal});
 // The archive lease's final assertions and FD-close also precede this fence.
 activeIndex(index,{signal});return result;
}

export async function openPeriodicTransferSource(entry,{signal}={}){
 const assertEntry=()=>{signal?.throwIfAborted();const life=sourceEntries.get(entry);if(!life?.active)fail('periodic_transfer_entry_capability_required');if(postgresBackupContext(life.context).closed)fail('backup_context_closed');};assertEntry();const file=await open(entry.sourcePath,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const info=await file.stat({bigint:true}),identity=Object.fromEntries(['dev','ino','uid','mode','nlink','size','mtimeNs','ctimeNs'].map(name=>[name,String(info[name])]));if(!same(identity,entry.identity)||!same(await periodicCipherIdentity(entry.sourcePath,entry.uid),identity))fail('periodic_transfer_source_changed');assertEntry();return file;}catch(error){await file.close();throw error;}
}
