import {spawn} from 'node:child_process';
import {capturePeriodicBaseIdentity,requirePeriodicBaseSource,loadPeriodicBaseVerification,periodicBaseVerificationSource} from './postgres-periodic-base-verification.mjs';
import {mkdir,readFile,rm,lstat,readdir,realpath} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {endianness} from 'node:os';
import {publishWalCheckpointCatalog,walSegmentName} from './postgres-backup-wal-catalog.mjs';
import {inspectArchivedWal} from './postgres-recovery.mjs';
import {PostgresStore} from './postgres-store.mjs';
import {acquireInstance} from './instance.mjs';
import {withPostgresBackupOperation} from './postgres-backup-lock.mjs';
import {postgresBackupContext,verifyBackupCluster} from './postgres-backup-context.mjs';
import {captureFileSet,writeManifest,readManifest,digestFile,digestOpenedFile,canonical,privateDirectory,transferError} from './migration-artifacts.mjs';
const proofs=new WeakMap(),children=new WeakMap(),fail=code=>{throw transferError(code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const lock=createHash('sha256').update('harbor-postgres-backup-v1').digest().readBigInt64BE().toString();
function processEnv(record){
 const options=record.adminOptions.connection;if(options)return {PGHOST:options.host,PGPORT:String(options.port),PGUSER:options.user,PGPASSWORD:options.password,PGDATABASE:options.database,PGSSLMODE:'disable'};
 const url=new URL(record.adminOptions.connectionString);return {PGHOST:url.hostname.replace(/^\[|\]$/g,''),PGPORT:url.port||'5432',PGUSER:decodeURIComponent(url.username),PGPASSWORD:decodeURIComponent(url.password),PGDATABASE:decodeURIComponent(url.pathname.slice(1)),PGSSLMODE:'verify-full',PGSSLROOTCERT:url.searchParams.get('sslrootcert'),...(url.searchParams.get('sslcert')?{PGSSLCERT:url.searchParams.get('sslcert'),PGSSLKEY:url.searchParams.get('sslkey')}: {})};
}
async function ownedGroup(pid,nonce){
 const members=[];for(const name of await readdir('/proc')){if(!/^[1-9]\d*$/.test(name))continue;try{const directory='/proc/'+name,text=await readFile(directory+'/stat','utf8'),fields=text.slice(text.lastIndexOf(')')+2).split(' ');if(Number(fields[2])!==pid||Number(fields[3])!==pid)continue;const env=(await readFile(directory+'/environ')).toString().split('\0');if(!env.includes('HARBOR_BACKUP_PROCESS_TOKEN='+nonce))fail('backup_process_cleanup_unknown');members.push(Number(name));}catch(error){if(error.code!=='ENOENT'&&error.code!=='ESRCH')throw error;}}
 return members;
}
async function signalOwnedGroup(pid,nonce,signal){
 if(!(await ownedGroup(pid,nonce)).length)return;try{process.kill(-pid,signal);}catch(error){if(error.code!=='ESRCH')throw error;}
}
async function finishOwnedGroup(pid,nonce){
 if(!(await ownedGroup(pid,nonce)).length)return;await signalOwnedGroup(pid,nonce,'SIGTERM');const deadline=Date.now()+2000;while((await ownedGroup(pid,nonce)).length){if(Date.now()>=deadline){await signalOwnedGroup(pid,nonce,'SIGKILL');const final=Date.now()+2000;while((await ownedGroup(pid,nonce)).length){if(Date.now()>=final)fail('backup_process_cleanup_unknown');await delay(10);}break;}await delay(10);}
 fail('backup_process_descendants_leaked');
}
async function command(record,name,args,{signal,connection=false,version=false,capture=false,applicationName}={}){
 signal?.throwIfAborted();const timeout=record.profile.processTimeoutMs;if(!Number.isSafeInteger(timeout)||timeout<1000||timeout>86400000)fail('backup_process_budget_invalid');
 return new Promise((resolveCommand,reject)=>{const nonce=randomBytes(32).toString('hex');let timedOut=false,overflow=false,killing,spawned=false,output='',cleanupError;const env={HARBOR_BACKUP_PROCESS_TOKEN:nonce,LC_ALL:'C',PATH:record.profile.pgBinDirectory+':/usr/bin:/bin',...(record.profile.libraryDirectory?{LD_LIBRARY_PATH:record.profile.libraryDirectory}:{}),...(connection?{...processEnv(record),PGAPPNAME:applicationName??'harbor-storage-backup'}:{})};
  const child=spawn(join(record.profile.pgBinDirectory,name),args,{env,detached:true,stdio:['ignore',version||capture?'pipe':'ignore','pipe']}),stop=()=>{if(!spawned)return;void signalOwnedGroup(child.pid,nonce,'SIGTERM').catch(error=>{cleanupError=error;});if(!killing){killing=setTimeout(()=>{void signalOwnedGroup(child.pid,nonce,'SIGKILL').catch(error=>{cleanupError=error;});},2000);killing.unref();}},abort=()=>stop(),timer=setTimeout(()=>{timedOut=true;stop();},timeout);let bytes=0;
  if(version||capture)child.stdout.on('data',chunk=>{output+=chunk.toString();if(Buffer.byteLength(output)>4096){overflow=true;stop();}});
  const childRecord={child,name,args:[...args],nonce,applicationName:env.PGAPPNAME};children.set(record,childRecord);
  child.once('spawn',()=>{spawned=true;if(signal?.aborted||timedOut)stop();});signal?.addEventListener('abort',abort,{once:true});child.stderr.on('data',chunk=>{bytes+=chunk.length;if(bytes>65536){overflow=true;stop();}});
  const cleanup=()=>{if(children.get(record)===childRecord)children.delete(record);clearTimeout(timer);clearTimeout(killing);signal?.removeEventListener('abort',abort);};child.once('error',()=>{cleanup();reject(signal?.aborted?signal.reason:transferError('backup_process_start_failed'));});child.once('close',code=>{cleanup();void (async()=>{try{if(child.pid)await finishOwnedGroup(child.pid,nonce);if(cleanupError)throw cleanupError;}catch{reject(transferError('backup_process_cleanup_unknown'));return;}if(signal?.aborted)reject(signal.reason);else if(code!==0||timedOut||overflow)reject(transferError(timedOut?'backup_process_timeout':'backup_process_failed'));else resolveCommand(output);})();});
 });
}

export async function inspectPostgresBackupProcess(context){
 const record=postgresBackupContext(context),current=children.get(record);if(!current||!current.child.pid||current.child.exitCode!==null||current.child.signalCode!==null)return null;
 try{const directory='/proc/'+current.child.pid,argv=(await readFile(directory+'/cmdline')).toString().split('\0').filter(Boolean),env=(await readFile(directory+'/environ')).toString().split('\0'),token=env.find(value=>value.startsWith('HARBOR_BACKUP_PROCESS_TOKEN='));if(token!=='HARBOR_BACKUP_PROCESS_TOKEN='+current.nonce)return {state:'identity_unknown',diagnosticOnly:true};const application=env.find(value=>value.startsWith('PGAPPNAME='))?.slice(10);return {state:'running',diagnosticOnly:true,pid:current.child.pid,tool:current.name,argumentNames:argv.slice(1).map(value=>value.startsWith('--')?value.split('=')[0]:'<positional>'),pgApplicationNameMatches:application===current.applicationName,pgApplicationNameHash:application?createHash('sha256').update(application).digest('hex'):null};}catch{return {state:'identity_unknown',diagnosticOnly:true};}
}

async function assertPlainCandidateUnused(path){
 for(const name of await readdir('/proc')){if(!/^[1-9]\d*$/.test(name))continue;let argv;try{argv=(await readFile('/proc/'+name+'/cmdline')).toString().split('\0');}catch(error){if(['ENOENT','ESRCH'].includes(error.code))continue;throw error;}if(argv.some(value=>value===path||value==='--pgdata='+path))fail('backup_candidate_process_active');}
}
async function removePlainCandidate(path){await assertPlainCandidateUnused(path);await rm(path,{recursive:true,force:true});}
function validId(id){if(typeof id!=='string'||!/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(id))fail('backup_id_invalid');return id;}
function mint(context,id,metadata,{verifiedAt=Date.now(),identity,full=false}={}){const proof=Object.freeze({backupId:id,scope:postgresBackupContext(context).scope,stage:'encrypted-cluster-copy'});proofs.set(proof,Object.freeze({context,metadata,verifiedAt,...(identity?{identity:deepFreeze(identity)}:{}),full}));return proof;}
export function inspectPostgresBackup(proof){const record=proofs.get(proof);if(!record)fail('backup_proof_required');return {backupId:proof.backupId,scope:proof.scope,stage:proof.stage,files:record.metadata.files.files,bytes:record.metadata.files.bytes,sourceInventoryDigest:record.metadata.inventoryDigest,applicationMaterialsVerified:false,restoreVerified:false,offhostVerified:false,deploymentAllowed:false};}
export async function verifyPostgresBackup(context,id,{signal}={}){return verifyBase(context,id,{signal});}
function deepFreeze(value){if(value&&typeof value==='object'){for(const child of Object.values(value))deepFreeze(child);Object.freeze(value);}return value;}
export function postgresBackupFullSource(proof){const value=proofs.get(proof);if(!value?.full||!value.identity)fail('periodic_base_full_verification_required');return value;}
export async function revalidatePeriodicSourceBase(proof,{signal}={}){const source=postgresBackupRestoreSource(proof);if(!source.identity)fail('periodic_base_identity_required');if(canonical(source.identity)!==canonical(await capturePeriodicBaseIdentity(source.context,proof.backupId,{signal})))fail('periodic_base_source_changed');return proof;}
export async function revalidatePeriodicSourceBaseIfPresent(proof,options){if(postgresBackupRestoreSource(proof).identity)await revalidatePeriodicSourceBase(proof,options);}
export async function loadAuthenticatedPeriodicBase(context,id,{signal}={}){const source=periodicBaseVerificationSource(await loadPeriodicBaseVerification(context,id,{signal}));return mint(context,id,deepFreeze(source.metadata),{verifiedAt:source.verifiedAt,identity:source.identity});}
export async function verifyPeriodicSourceBase(context,id,{signal}={}){requirePeriodicBaseSource(context);return verifyBase(context,id,{signal,captureIdentity:true});}
async function verifyBase(context,id,{signal,captureIdentity=false}={}){
 signal?.throwIfAborted();const record=postgresBackupContext(context);validId(id);const directory=join(record.artifactDirectory,id);await privateDirectory(directory);const metadata=await readManifest(join(directory,'backup.enc'),record.key,{signal});
 if(metadata.version!==1||metadata.backupId!==id||metadata.configurationDigest!==record.configurationDigest||metadata.systemId!==record.expected.system_id||metadata.keyVersion!==record.keyVersion||metadata.scope!==record.scope||metadata.materialCoverage!=='database-only'||metadata.inventoryDigest!==hash({databases:record.expected.databases,roles:record.expected.roles,memberships:record.expected.memberships}))fail('backup_saved_binding_mismatch');
 const before=captureIdentity?await capturePeriodicBaseIdentity(context,id,{signal,allowPlain:true}):null;
 const names=await authenticatePostgresBackupFileSet({directory,key:record.key,files:metadata.files,signal});
 if(captureIdentity){names.add('backup.enc');if(canonical([...names].sort())!==canonical(before.files.map(row=>row.name))||canonical(before)!==canonical(await capturePeriodicBaseIdentity(context,id,{signal,allowPlain:true})))fail('periodic_base_source_changed');}
 return mint(context,id,captureIdentity?deepFreeze(metadata):metadata,captureIdentity?{identity:before,full:true}:{});
}
// File authentication only. Retention may authenticate historical inventories
// without granting a source/restore capability or loosening verifyPostgresBackup.
export async function authenticatePostgresBackupFileSet({directory,key,files,signal}={}){
 const names=new Set(['files.enc']),catalog=await readManifest(join(directory,'files.enc'),key,{signal});if(canonical(await digestFile(join(directory,'files.enc'),{signal}))!==canonical(files.manifestDigest))fail('backup_saved_manifest_changed');
 let count=0;for(const item of catalog.catalog){signal?.throwIfAborted();if(!/^file-catalog-[0-9]+\.enc$/.test(item.name)||names.has(item.name)||canonical(await digestFile(join(directory,item.name),{signal}))!==canonical(item.digest))fail('backup_saved_catalog_changed');names.add(item.name);const part=await readManifest(join(directory,item.name),key,{signal});for(const entry of part.entries){signal?.throwIfAborted();count++;if(entry.kind==='file'){if(!/^file-[0-9]+\.enc$/.test(entry.artifact??'')||names.has(entry.artifact))fail('backup_saved_file_invalid');names.add(entry.artifact);if(canonical(await digestFile(join(directory,entry.artifact),{signal}))!==canonical(entry.encrypted)||canonical(await digestOpenedFile(join(directory,entry.artifact),key,{signal}))!==canonical(entry.content))fail('backup_saved_file_changed');}}}
 signal?.throwIfAborted();if(count!==files.files)fail('backup_saved_file_count');return names;
}
export async function createPostgresBaseBackup(context,options={}){return createBase(context,options);}
export async function createPeriodicSourceBase(context,options={}){requirePeriodicBaseSource(context);return createBase(context,options,true);}
async function createBase(context,{backupId=randomUUID(),signal}={},captureIdentity=false){
 const record=postgresBackupContext(context);if(record.resumeBinding)fail('backup_resume_readonly');validId(backupId);if(record.busy)fail('backup_operation_busy');record.busy=true;const cancellation=new AbortController();record.cancellation=cancellation;signal=AbortSignal.any([signal,cancellation.signal].filter(Boolean));
 const operation=withPostgresBackupOperation(record,async()=>{let store,directory,plain,preservePlain=false,lockClient,lockError;const lockFailed=error=>{lockError??=error;cancellation.abort(error);};
 try{
  const before=await verifyBackupCluster(context);store=new PostgresStore({...record.adminOptions,poolSize:1});lockClient=await store.pool.connect();lockClient.on('error',lockFailed);if(!(await lockClient.query('SELECT pg_try_advisory_lock($1::bigint) held',[lock])).rows[0].held)fail('backup_operation_busy');
  for(const tool of ['pg_basebackup','pg_verifybackup','pg_waldump','pg_controldata']){const version=await command(record,tool,['--version'],{signal,version:true});const match=/^(pg_basebackup|pg_verifybackup|pg_waldump|pg_controldata) \(PostgreSQL\) 18[. ]/.exec(version);if(!match||match[1]!==tool)fail('backup_tool_version');}
  directory=join(record.artifactDirectory,backupId);try{await mkdir(directory,{mode:0o700});}catch(error){if(error.code!=='EEXIST')throw error;await privateDirectory(directory);try{const verified=await verifyBase(context,backupId,{signal,captureIdentity});plain=join(directory,'plain');return verified;}catch(saved){if(saved.code!=='ENOENT')throw saved;let state;try{state=await readManifest(join(directory,'state.enc'),record.key);}catch(missing){if(missing.code!=='ENOENT')throw missing;if((await readdir(directory)).length)fail('backup_incomplete_state_missing');}if(state&&(state.backupId!==backupId||state.configurationDigest!==record.configurationDigest||state.systemId!==before.system_id))fail('backup_incomplete_binding');}}
  await assertPlainCandidateUnused(join(directory,'plain'));
  await writeManifest(join(directory,'state.enc'),{version:1,backupId,configurationDigest:record.configurationDigest,systemId:before.system_id,stage:'preparing'},record.key,{replace:true});
  // Only this authenticated backup directory is eligible for retry cleanup.
  for(const name of await readdir(directory))if(name!== 'state.enc'){if(!/^(?:plain|files\.enc|file-catalog-\d+\.enc|file-\d+\.enc)$/.test(name))fail('backup_incomplete_unknown_file');if(name==='plain')await removePlainCandidate(join(directory,name));else await rm(join(directory,name),{force:true});}
  plain=join(directory,'plain');await command(record,'pg_basebackup',['--pgdata',plain,'--format=plain','--wal-method=stream','--checkpoint='+record.profile.checkpointMode,'--manifest-checksums=SHA256','--no-password',...(record.profile.maxRateKb?['--max-rate='+record.profile.maxRateKb]:[])],{signal,connection:true,applicationName:'harbor-backup-'+backupId});
  await command(record,'pg_verifybackup',[plain],{signal});const manifestText=await readFile(join(plain,'backup_manifest'),'utf8'),backupManifest=JSON.parse(manifestText),control=await readFile(join(plain,'global','pg_control')),physicalSystem=(endianness()==='LE'?control.readBigUInt64LE():control.readBigUInt64BE()).toString(),manifestSystem=/"System-Identifier"\s*:\s*([0-9]+)/.exec(manifestText)?.[1];if(physicalSystem!==before.system_id||manifestSystem!==physicalSystem)fail('backup_copy_system_mismatch');if(!Array.isArray(backupManifest['WAL-Ranges'])||!backupManifest['WAL-Ranges'].length)fail('backup_wal_ranges_missing');
  const controlText=await command(record,'pg_controldata',[plain],{signal,capture:true}),recoverySettings={};for(const [name,label] of Object.entries({max_connections:'max_connections setting',max_worker_processes:'max_worker_processes setting',max_wal_senders:'max_wal_senders setting',max_prepared_transactions:'max_prepared_xacts setting',max_locks_per_transaction:'max_locks_per_xact setting'})){const line=controlText.split('\n').find(line=>line.startsWith(label+':')),value=line?.split(':').at(-1).trim();if(!/^[0-9]+$/.test(value??'')||!Number.isSafeInteger(Number(value)))fail('backup_control_parameters_invalid');recoverySettings[name]=Number(value);}
  const after=await verifyBackupCluster(context);if(before.postmaster_start!==after.postmaster_start)fail('backup_source_restarted');if(before.timeline!==after.timeline)fail('backup_timeline_changed');
  const files=await captureFileSet({roots:[{id:'pgdata',source:plain}],directory,key:record.key,maxTotalBytes:record.profile.maxBackupBytes,maxFileBytes:Math.min(1073741824,record.profile.maxBackupBytes),beforeFile:()=>signal.throwIfAborted(),afterFile:()=>signal.throwIfAborted()}),metadata={version:1,backupId,scope:record.scope,configurationDigest:record.configurationDigest,systemId:before.system_id,inventoryDigest:hash({databases:before.databases,roles:before.roles,memberships:before.memberships}),recoverySettings,postmasterStart:before.postmaster_start,timeline:before.timeline,segmentBytes:before.segment_bytes,keyVersion:record.keyVersion,materialCoverage:'database-only',files,walRanges:backupManifest['WAL-Ranges'],baseManifest:await digestFile(join(plain,'backup_manifest')),created:Date.now()};
  signal.throwIfAborted();await lockClient.query('SELECT 1');await writeManifest(join(directory,'backup.enc'),metadata,record.key);return await verifyBase(context,backupId,{signal,captureIdentity});
 }catch(error){preservePlain=error.code==='backup_process_cleanup_unknown';throw error;}finally{try{if(plain&&!preservePlain)await removePlainCandidate(plain);}finally{if(store){try{if(lockClient){try{if(!lockError)await lockClient.query('SELECT pg_advisory_unlock($1::bigint)',[lock]);}finally{lockClient.off('error',lockFailed);lockClient.release(lockError);}}}finally{await store.close();}}}}
 },{signal});record.pending=operation;try{return await operation;}finally{record.busy=false;record.pending=null;record.cancellation=null;}
}
const lsn=value=>{if(typeof value!=='string'||!/^([0-9A-F]+)\/([0-9A-F]+)$/.test(value))fail('backup_lsn_invalid');const [high,low]=value.split('/');return (BigInt('0x'+high)<<32n)+BigInt('0x'+low);};
export async function withPostgresBackupCheckpointLock(context,backupId,operation){
 const record=postgresBackupContext(context);return withPostgresBackupOperation(record,async()=>{validId(backupId);const directory=await realpath(await privateDirectory(join(record.artifactDirectory,backupId)));
 let lease;try{lease=await acquireInstance(join(directory,'.checkpoint-publication'));}catch(error){if(error.code==='EADDRINUSE')fail('backup_operation_busy');throw error;}
 try{return await operation();}finally{await lease.release();}
 });
}
// Only authenticated references under this backup's managed directory can be
// discovered here. External bundles still require an explicit scheduler pin.
export async function inspectPostgresBackupCheckpointProtection(context,backupId,{signal}={}){
 const record=postgresBackupContext(context);validId(backupId);const directory=join(record.artifactDirectory,backupId);await privateDirectory(directory);let sealed=false;
 const sameDigest=(a,b)=>a&&b&&a.bytes===b.bytes&&a.sha256===b.sha256;
 const validDigest=value=>Number.isSafeInteger(value?.bytes)&&value.bytes>=36&&/^[a-f0-9]{64}$/.test(value.sha256??'');
 for(const [path,application] of [[join(directory,'application'),true],[join(directory,'operator-final.enc'),false]]){
  let info;try{info=await lstat(path);}catch(error){if(error.code==='ENOENT')continue;throw error;}
  if(application){await privateDirectory(path);if(!info.isDirectory())fail('backup_checkpoint_reference_invalid');}
  const reference=await readManifest(application?join(path,'application.enc'):path,record.key,{signal});
  if(reference?.version!==1||reference.backupId!==backupId||reference.configurationDigest!==record.configurationDigest||application&&reference.keyVersion!==record.keyVersion)fail('backup_checkpoint_reference_invalid');
  if(application){if(!validDigest(reference.files?.manifestDigest)||!Array.isArray(reference.materialRoots)||!reference.materialRoots.some(root=>root.id==='data')||!Array.isArray(reference.materialKeys)||!/^[a-f0-9]{64}$/.test(reference.pendingBindingDigest??''))fail('backup_checkpoint_reference_invalid');}
  else if(reference.stage!=='pending-final-copy-and-local-restore'||!validDigest(reference.applicationManifestDigest)||!sameDigest(reference.applicationManifestDigest,await digestFile(join(directory,'application','application.enc'),{signal})))fail('backup_checkpoint_reference_invalid');
  if(application&&reference.checkpointDigest===null){
   // The existing pending-final sequence captures files before its first
   // checkpoint. Only that live private application plan may finish binding it.
   if(!record.pendingBinding||record.applicationPlan?.backupId!==backupId||reference.pendingBindingDigest!==record.pendingBindingDigest)fail('backup_checkpoint_reference_unbound');
   continue;
  }
  const digest=reference.checkpointDigest;if(!validDigest(digest))fail('backup_checkpoint_reference_invalid');
  if(!sameDigest(digest,await digestFile(join(directory,'checkpoint.enc'),{signal})))fail('backup_checkpoint_reference_changed');
  sealed=true;
 }
 return {sealed};
}
// Recovery must start with parameters at least as large as those recorded in
// the primary's WAL. Current settings cannot prove the historical high-water
// mark across restarts, so a new postmaster always requires a new base.
export async function isPostgresBackupRecoveryCompatible(context,metadata,{signal,current}={}){
 const record=postgresBackupContext(context),names=['max_connections','max_worker_processes','max_wal_senders','max_prepared_transactions','max_locks_per_transaction'],saved=metadata?.recoverySettings;
 signal?.throwIfAborted();if(record.closed)fail('backup_context_closed');
 if(metadata?.version!==1||metadata.configurationDigest!==record.configurationDigest||metadata.systemId!==record.expected.system_id||metadata.keyVersion!==record.keyVersion||metadata.scope!==record.scope||!saved||Object.keys(saved).sort().join(',')!==[...names].sort().join(',')||Object.values(saved).some(value=>!Number.isSafeInteger(value)||value<0))fail('backup_recovery_parameters_invalid');
 const generation=current??await verifyBackupCluster(context);if(metadata.postmasterStart!==generation.postmaster_start||metadata.timeline!==generation.timeline)return false;const store=new PostgresStore({...record.adminOptions,poolSize:1});
 try{
  const rows=await store.all('SELECT name,setting::integer value,extract(epoch FROM pg_postmaster_start_time())::text postmaster_start,(SELECT timeline_id::integer FROM pg_control_checkpoint()) timeline FROM pg_settings WHERE name=ANY($1::text[])',names);signal?.throwIfAborted();
  if(rows.length!==names.length||new Set(rows.map(row=>row.name)).size!==names.length||rows.some(row=>!names.includes(row.name)||!Number.isSafeInteger(row.value)||row.value<0))fail('backup_recovery_parameters_invalid');
  if(rows.some(row=>row.postmaster_start!==generation.postmaster_start||row.timeline!==generation.timeline))fail('backup_source_restarted');
  return rows.every(row=>row.value<=saved[row.name]);
 }finally{await store.close();}
}
async function checkpointInner(proof,{signal,timeoutMs=30000,periodic=false}={}){
 const owned=proofs.get(proof);if(!owned)fail('backup_proof_required');const {context,metadata}=owned,record=postgresBackupContext(context);const initial=await verifyBackupCluster(context);if(initial.postmaster_start!==metadata.postmasterStart)fail('backup_source_restarted');if(initial.timeline!==metadata.timeline)fail('backup_timeline_changed');if(periodic&&!await isPostgresBackupRecoveryCompatible(context,metadata,{signal,current:initial}))fail('backup_recovery_parameters_changed');if(!Number.isSafeInteger(timeoutMs)||timeoutMs<1000||timeoutMs>300000)fail('backup_archive_budget_invalid');
 const store=new PostgresStore({...record.adminOptions,poolSize:1});let point;const restorePoint='harbor_backup_'+proof.backupId.replaceAll('-','')+'_'+randomBytes(6).toString('hex');try{point=await store.get('SELECT pg_create_restore_point($1)::text lsn',restorePoint);await store.get('SELECT pg_switch_wal()');}finally{await store.close();}
 const segmentBytes=BigInt(metadata.segmentBytes),start=lsn(metadata.walRanges.at(-1)['End-LSN'])/segmentBytes,end=lsn(point.lsn)/segmentBytes,count=end-start+1n;if(count<1n||count>BigInt(record.profile.maxWalSegments))fail('backup_wal_capacity');const deadline=Date.now()+timeoutMs;
 let newlyVerified=0,reusedVerified=0,oldestVerifiedAt=null;
 async function* segments(){for(let index=start;index<=end;index++){
  const name=walSegmentName(metadata.timeline,index,segmentBytes);for(;;){signal?.throwIfAborted();try{let meta;if(periodic){const {inspectPeriodicArchivedWal}=await import('./postgres-periodic-wal-cache.mjs'),value=await inspectPeriodicArchivedWal(context,name,{sourceGeneration:initial.postmaster_start,signal});meta=value.metadata;if(value.cacheHit)reusedVerified++;else newlyVerified++;oldestVerifiedAt=oldestVerifiedAt===null?value.verifiedAt:Math.min(oldestVerifiedAt,value.verifiedAt);}else meta=await inspectArchivedWal({name,directory:record.archiveDirectory,key:record.key,ownerUid:record.restoreUid,requireBoundName:true});if(meta.systemIdentifier!==metadata.systemId||meta.identity.timeline!==metadata.timeline||meta.content.bytes!==Number(segmentBytes))fail('backup_wal_binding_mismatch');yield {name,encrypted:meta.encrypted,content:meta.content};break;}catch(error){if(error.code!=='ENOENT'&&!(periodic&&error.code==='periodic_wal_cache_publication_pending'))throw error;if(Date.now()>=deadline)fail('backup_wal_archive_timeout');await delay(20,undefined,{signal});}}
 }const actual=await verifyBackupCluster(context);if(actual.postmaster_start!==(periodic?initial.postmaster_start:metadata.postmasterStart))fail('backup_source_restarted');if(actual.timeline!==metadata.timeline)fail('backup_timeline_changed');if(periodic&&!await isPostgresBackupRecoveryCompatible(context,metadata,{signal,current:actual}))fail('backup_recovery_parameters_changed');}
 const checkpoint={backupId:proof.backupId,configurationDigest:record.configurationDigest,systemId:metadata.systemId,timeline:metadata.timeline,keyVersion:record.keyVersion,restorePoint,lsn:point.lsn,walStart:start.toString(),walEnd:end.toString(),segmentBytes:metadata.segmentBytes,...(periodic?{sourcePostmasterStart:initial.postmaster_start}:{}),created:Date.now()};
 await publishWalCheckpointCatalog({directory:join(record.artifactDirectory,proof.backupId),key:record.key,checkpoint,signal,segments:segments()});
 return {backupId:proof.backupId,scope:record.scope,stage:'encrypted-wal-checkpoint',segments:Number(count),...(periodic?{walIntegrity:{newlyVerified,reusedVerified,oldestVerifiedAt}}:{}),restoreVerified:false,offhostVerified:false,deploymentAllowed:false};
}

export function postgresBackupRestoreSource(proof){const source=proofs.get(proof);if(!source)fail('backup_proof_required');return source;}
export async function checkpointPostgresBackup(proof,options={}){return checkpointOperation(proof,{signal:options.signal,timeoutMs:options.timeoutMs??30000,periodic:false});}
export async function checkpointPeriodicPostgresBackup(proof,options={}){return checkpointOperation(proof,{signal:options.signal,timeoutMs:options.timeoutMs??30000,periodic:true});}
async function checkpointOperation(proof,{signal,timeoutMs,periodic}){
 const {context}=postgresBackupRestoreSource(proof),record=postgresBackupContext(context);if(record.closed)fail('backup_context_closed');if(record.busy)fail('backup_operation_busy');record.busy=true;const cancellation=new AbortController();record.cancellation=cancellation;
 if(periodic&&(record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)){record.busy=false;record.cancellation=null;fail('backup_periodic_context_required');}
 const combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));
 const operation=withPostgresBackupCheckpointLock(context,proof.backupId,async()=>{combined.throwIfAborted();if((await inspectPostgresBackupCheckpointProtection(context,proof.backupId,{signal:combined})).sealed)fail('backup_checkpoint_sealed');return checkpointInner(proof,{signal:combined,timeoutMs,periodic});});record.pending=operation;try{return await operation;}finally{record.busy=false;record.pending=null;record.cancellation=null;}
}
