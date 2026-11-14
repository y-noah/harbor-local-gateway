import {requireMaterialMaintenanceInactive} from './harbor-material-storage.mjs';
import {open,lstat,realpath,readFile,mkdir,readdir,rm} from 'node:fs/promises';
import {constants} from 'node:fs';
import {spawn} from 'node:child_process';
import {join} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {transferError,privateDirectory,readManifest,writeManifest,digestFile,syncDirectory,canonical} from './migration-artifacts.mjs';

export const backupCoordinationProtocol='harbor-backup-flock-v1';
export const backupCoordinationFiles=Object.freeze({lock:'.backup-coordination.lock',intent:'coordination.enc',receipt:'coordination-receipt.enc'});
const fail=code=>{throw transferError(code);};
const same=(a,b)=>a.dev===b.dev&&a.ino===b.ino;
export async function backupCoordinationCodeDigest(){
 const names=['harbor-material-storage.mjs','postgres-backup-lock.mjs','postgres-recovery.mjs','storage-backup.mjs','postgres-backup.mjs','postgres-backup-context.mjs','postgres-backup-restore.mjs','postgres-backup-schedule.mjs','postgres-backup-handoff.mjs','storage-operator-backup-application.mjs','postgres-backup-retention.mjs','postgres-backup-wal-catalog.mjs','migration-artifacts.mjs','postgres-backup-recovery-point.mjs','postgres-periodic-application-retention.mjs','postgres-periodic-application-cycle.mjs','postgres-periodic-application-backup.mjs','postgres-periodic-application-materials.mjs','postgres-periodic-application-policy.mjs','postgres-periodic-application-key.mjs','postgres-periodic-application-restore.mjs','postgres-periodic-application-runtime.mjs','postgres-periodic-application-installed-runtime.mjs','postgres-periodic-application-runtime-child.mjs','postgres-periodic-wal-cache.mjs','postgres-periodic-base-verification.mjs','postgres-periodic-receiver-verification.mjs','storage-receiver-periodic-attestation.mjs','storage-restored-runtime-shutdown.mjs','instance.mjs','storage-operator-backup-recordings.mjs','recording-context-store.mjs','recording-request-context.mjs','recording-text-metadata.mjs','recording-attachment.mjs','recording-literal.mjs','conversation-capture.mjs','recording-context-configuration.mjs','record-redaction.mjs','storage-operator-backup-devices.mjs','ci/postgres-preflight.mjs','postgres-worker-startup.mjs','storage-receiver-candidate-files.mjs','postgres-storage-report.mjs'];
 const hash=createHash('sha256');for(const name of [...names,'postgres-backup-observation.mjs'])hash.update(name+'\0').update(await readFile(new URL('./'+name,import.meta.url)));return hash.digest('hex');
}

// flock(2) belongs to the open file description inherited as fd 3. The small
// utility exits after locking; the parent's FD retains the kernel lock. A
// crash closes that FD, including across mount/network namespaces.
export async function withBackupArchiveLock({directory,uid=process.getuid(),exclusive=false,signal}={},operation){
 signal?.throwIfAborted();const root=await lstat(directory);
 if(!root.isDirectory()||root.isSymbolicLink()||root.uid!==uid||root.mode&0o077||await realpath(directory)!==directory)fail('backup_coordination_directory');
 const path=join(directory,backupCoordinationFiles.lock);let file,created=false;
 try{file=await open(path,constants.O_RDWR|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);created=true;}
 catch(error){if(error.code!=='EEXIST')throw error;file=await open(path,constants.O_RDWR|constants.O_NOFOLLOW|constants.O_NONBLOCK);}
 try{
  if(created&&process.getuid()===0&&uid!==0)await file.chown(uid,root.gid);
  const initial=await file.stat();
  const valid=info=>info.isFile()&&info.uid===uid&&(info.mode&0o777)===0o600&&info.nlink===1&&same(info,initial);
  const assertPath=async()=>{signal?.throwIfAborted();if(!valid(await file.stat())||!valid(await lstat(path))||!same(root,await lstat(directory)))fail('backup_coordination_lock_changed');};
  await assertPath();const tool=await lstat('/usr/bin/flock');if(!tool.isFile()||tool.uid!==0||tool.mode&0o022)fail('backup_coordination_tool_untrusted');
  const code=await new Promise((resolve,reject)=>{
   const child=spawn('/usr/bin/flock',[exclusive?'--exclusive':'--shared','--nonblock','--conflict-exit-code','75','3'],{stdio:['ignore','ignore','ignore',file.fd],env:{PATH:'/usr/bin:/bin',LANG:'C'}});
   const timer=setTimeout(()=>child.kill('SIGKILL'),5000),abort=()=>child.kill('SIGTERM');signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();
   child.once('error',error=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);reject(error);});child.once('close',value=>{clearTimeout(timer);signal?.removeEventListener('abort',abort);resolve(value);});
  });
  signal?.throwIfAborted();if(code===75)fail('backup_coordination_busy');if(code!==0)fail('backup_coordination_lock_failed');await assertPath();
  const result=await operation(Object.freeze({assertPath,exclusive,identity:Object.freeze({dev:String(initial.dev),ino:String(initial.ino),uid,nlink:initial.nlink})}));await assertPath();return result;
 }finally{await file.close();}
}

export async function withPostgresBackupOperation(record,operation,{signal}={}){
 if(record.scope==='existing-cluster')await requireMaterialMaintenanceInactive();
 return withBackupArchiveLock({directory:record.archiveDirectory,uid:record.restoreUid,signal},async lease=>{if(record.scope==='existing-cluster')await requireMaterialMaintenanceInactive();return operation(lease);});
}

export async function readBackupCoordinationIntent({directory,key,uid=process.getuid()}={}){
 const {readWalManifest}=await import('./postgres-recovery.mjs');let value;
 try{value=await readWalManifest(join(directory,backupCoordinationFiles.intent),key,{ownerUid:uid});}catch(error){if(error.code==='ENOENT')return null;throw error;}
 if(value?.version!==1||value.protocol!==backupCoordinationProtocol||!/^[a-f0-9]{32}$/.test(value.nonce??'')||!/^\d+$/.test(value.systemId??'')||!/^\d+(?:\.\d+)?$/.test(value.postmasterStart??'')||!['commandDigest','codeDigest'].every(name=>/^[a-f0-9]{64}$/.test(value[name]??'')))fail('backup_coordination_intent_invalid');return value;
}

// Only the managed database schedule uses this private control directory. Its
// lifetime lock is independent of the archive lock, so idle polling never pins
// a shared archive lease. Stale records are preserved and cannot match a new
// process without the same kernel PID/start identity.
export const backupScheduleCoordinationDirectory='.schedule-coordination';
const scheduleScope='harbor-backup-schedule-coordination-v1';
async function scheduleProcessIdentity(pid){
 const root='/proc/'+pid,[info,description,args,boot]=await Promise.all([lstat(root),readFile(root+'/stat','utf8'),readFile(root+'/cmdline'),readFile('/proc/sys/kernel/random/boot_id','utf8')]),fields=description.slice(description.lastIndexOf(')')+2).trim().split(/\s+/),start=fields[19],bootId=boot.trim();
 if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(bootId)||!Number.isSafeInteger(pid)||pid<1||!/^\d+$/.test(start??''))fail('backup_schedule_process_identity');return {pid,uid:info.uid,start,bootId,argumentsDigest:createHash('sha256').update(args).digest('hex')};
}
function scheduleRecordValid(record,value,name){
 return value?.version===1&&value.scope===scheduleScope&&value.protocol===backupCoordinationProtocol&&value.kind==='database-schedule'&&Number.isSafeInteger(value.pid)&&value.pid>0&&Number.isSafeInteger(value.uid)&&value.uid>=0&&/^\d+$/.test(value.start??'')&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value.bootId??'')&&name===value.bootId+'-'+value.pid+'-'+value.start+'.enc'&&value.lockIdentity?.uid===value.uid&&value.lockIdentity.nlink===1&&['dev','ino'].every(key=>/^\d+$/.test(value.lockIdentity[key]??''))&&['codeDigest','configurationDigest','argumentsDigest'].every(key=>/^[a-f0-9]{64}$/.test(value[key]??''))&&/^[a-f0-9]{32}$/.test(value.nonce??'')&&value.systemId===record.expected.system_id&&value.keyVersion===record.keyVersion&&value.contextScope===record.scope&&value.archiveDirectory===record.archiveDirectory&&value.artifactDirectory===record.artifactDirectory;
}
async function scheduleRecord(record,name){
 const path=join(record.artifactDirectory,backupScheduleCoordinationDirectory,name),info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.uid!==process.getuid()||(info.mode&0o777)!==0o600||info.nlink!==1)fail('backup_schedule_registration_untrusted');const value=await readManifest(path,record.key);if(!scheduleRecordValid(record,value,name))fail('backup_schedule_registration_invalid');return value;
}
export async function inspectPostgresBackupScheduleRegistrations(record){
 const directory=join(record.artifactDirectory,backupScheduleCoordinationDirectory);await privateDirectory(directory);
 for(const name of await readdir(directory)){
  if(name===backupCoordinationFiles.lock){const info=await lstat(join(directory,name));if(!info.isFile()||info.isSymbolicLink()||info.uid!==process.getuid()||(info.mode&0o777)!==0o600||info.nlink!==1)fail('backup_schedule_registration_untrusted');continue;}
  if(/^\.manifest-[a-f0-9]{24}$/.test(name)){const info=await lstat(join(directory,name));if(!info.isFile()||info.isSymbolicLink()||info.uid!==process.getuid()||(info.mode&0o777)!==0o600||info.nlink!==1)fail('backup_schedule_registration_untrusted');continue;}
  if(!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}-[1-9]\d*-\d+\.enc$/.test(name))fail('backup_schedule_registration_unknown');await scheduleRecord(record,name);
 }
}
export async function isCoordinatedPostgresBackupScheduleProcess(record,pid){
 try{await privateDirectory(join(record.artifactDirectory,backupScheduleCoordinationDirectory));const current=await scheduleProcessIdentity(pid),value=await scheduleRecord(record,current.bootId+'-'+current.pid+'-'+current.start+'.enc'),lock=await lstat(join(record.artifactDirectory,backupScheduleCoordinationDirectory,backupCoordinationFiles.lock));if(!lock.isFile()||lock.isSymbolicLink()||lock.uid!==value.uid||(lock.mode&0o777)!==0o600||lock.nlink!==1||String(lock.dev)!==value.lockIdentity.dev||String(lock.ino)!==value.lockIdentity.ino)return false;return current.bootId===value.bootId&&current.uid===value.uid&&current.argumentsDigest===value.argumentsDigest&&value.configurationDigest===record.configurationDigest&&value.codeDigest===await backupCoordinationCodeDigest();}
 catch(error){if(['ENOENT','ESRCH','EACCES'].includes(error.code)||error.code?.startsWith('backup_schedule_')||error.code?.startsWith('transfer_'))return false;throw error;}
}
export async function withPostgresBackupScheduleRegistration(record,operation,{signal}={}){
 signal?.throwIfAborted();let operationCompleted=false,cleanupCompleted=false,result,lifetime;const directory=join(record.artifactDirectory,backupScheduleCoordinationDirectory);await mkdir(directory,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await privateDirectory(directory);await syncDirectory(record.artifactDirectory);const rootIdentity=await lstat(directory);
 try{return await withBackupArchiveLock({directory,exclusive:true,signal},async lease=>{
  lifetime=lease.identity;
  await inspectPostgresBackupScheduleRegistrations(record);const current=await scheduleProcessIdentity(process.pid),name=current.bootId+'-'+current.pid+'-'+current.start+'.enc',path=join(directory,name),value={version:1,scope:scheduleScope,protocol:backupCoordinationProtocol,kind:'database-schedule',...current,lockIdentity:lease.identity,nonce:randomBytes(16).toString('hex'),codeDigest:await backupCoordinationCodeDigest(),configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,contextScope:record.scope,archiveDirectory:record.archiveDirectory,artifactDirectory:record.artifactDirectory};
  await withPostgresBackupOperation(record,()=>writeManifest(path,value,record.key),{signal});const identity=await lstat(path),digest=await digestFile(path);
  try{result=await operation();operationCompleted=true;return result;}finally{
   // This byte/identity check remains possible after the context key is cleared.
   // Never unlink a changed record or adopt another process's stale file.
   let info;try{info=await lstat(path);}catch(error){if(error.code!=='ENOENT')throw error;}
   if(info){if(!info.isFile()||info.isSymbolicLink()||info.uid!==identity.uid||info.mode!==identity.mode||info.size!==identity.size||info.mtimeMs!==identity.mtimeMs||info.ctimeMs!==identity.ctimeMs||info.nlink!==1||!same(info,identity)||canonical(await digestFile(path))!==canonical(digest))fail('backup_schedule_registration_changed');await rm(path);}await syncDirectory(directory);cleanupCompleted=true;
  }
 });}catch(error){
  // withBackupArchiveLock has already awaited its FD-close finally here. A
  // close failure replaces the abort reason and must not be translated.
  if(operationCompleted&&cleanupCompleted&&signal?.aborted&&error===signal.reason){
   const [root,lock]=await Promise.all([lstat(directory),lstat(join(directory,backupCoordinationFiles.lock))]);
   if(!root.isDirectory()||root.isSymbolicLink()||!same(root,rootIdentity)||root.uid!==rootIdentity.uid||root.mode!==rootIdentity.mode||!lock.isFile()||lock.isSymbolicLink()||String(lock.dev)!==lifetime.dev||String(lock.ino)!==lifetime.ino||lock.uid!==lifetime.uid||(lock.mode&0o777)!==0o600||lock.nlink!==1)fail('backup_coordination_lock_changed');return result;
  }throw error;
 }
}
