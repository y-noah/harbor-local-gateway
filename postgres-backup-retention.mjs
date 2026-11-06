import {authenticatePeriodicBaseRetention,periodicBaseRetentionReferences} from './postgres-periodic-base-verification.mjs';
import {mkdir,open,lstat,readdir,readFile,rename,rm,rmdir,chown,realpath} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join,resolve,dirname,relative} from 'node:path';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {PostgresStore} from './postgres-store.mjs';
import {postgresBackupContext,verifyBackupCluster} from './postgres-backup-context.mjs';
import {authenticatePostgresBackupFileSet} from './postgres-backup.mjs';
import {authenticatePeriodicApplicationRetentionGroup,readPeriodicApplicationRetentionState,periodicApplicationRetentionReferences} from './postgres-periodic-application-retention.mjs';
import {inspectArchivedWal,readWalManifest} from './postgres-recovery.mjs';
import {readWalCheckpointSegments,walSegmentName} from './postgres-backup-wal-catalog.mjs';
import {withBackupArchiveLock,withPostgresBackupOperation,readBackupCoordinationIntent,backupCoordinationFiles,backupCoordinationProtocol,backupCoordinationCodeDigest,backupScheduleCoordinationDirectory,inspectPostgresBackupScheduleRegistrations,isCoordinatedPostgresBackupScheduleProcess} from './postgres-backup-lock.mjs';
import {readManifest,writeManifest,digestFile,canonical,syncDirectory,privateDirectory,transferError} from './migration-artifacts.mjs';
import {acquireInstance} from './instance.mjs';

const fail=code=>{throw transferError(code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const digest=value=>Number.isSafeInteger(value?.bytes)&&value.bytes>=36&&/^[a-f0-9]{64}$/.test(value.sha256??'');
const same=(a,b)=>canonical(a)===canonical(b),scope='harbor-backup-retention-v1';
const walName=name=>/^[0-9A-F]{24}$/.test(name),historyName=name=>/^[0-9A-F]{8}\.history$/.test(name);
const lsn=value=>{const m=/^([0-9A-F]+)\/([0-9A-F]+)$/.exec(value??'');if(!m)fail('backup_retention_lsn');return (BigInt('0x'+m[1])<<32n)+BigInt('0x'+m[2]);};
const output=(state,extra={})=>({state,retentionApplied:false,applicationMaterialsVerified:false,restoreVerified:false,offhostVerified:false,deploymentAllowed:false,...extra});

export function postgresBackupRetentionPolicy(value){
 const keys=['version','enabled','keepDatabaseCount','keepApplicationCount','minAgeMs','pinnedBackupIds','operatorStateFiles'];
 if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(key=>!keys.includes(key))||value.version!==1||typeof value.enabled!=='boolean')fail('backup_retention_policy_invalid');
 if(!value.enabled)return Object.freeze({...value,enabled:false});
 if(![value.keepDatabaseCount,value.keepApplicationCount].every(n=>Number.isSafeInteger(n)&&n>=1)||!Number.isSafeInteger(value.minAgeMs)||value.minAgeMs<1||!Array.isArray(value.pinnedBackupIds)||value.pinnedBackupIds.some(id=>!uuid(id))||new Set(value.pinnedBackupIds).size!==value.pinnedBackupIds.length||!Array.isArray(value.operatorStateFiles)||value.operatorStateFiles.some(path=>typeof path!=='string'||resolve(path)!==path)||new Set(value.operatorStateFiles).size!==value.operatorStateFiles.length)fail('backup_retention_policy_invalid');
 return Object.freeze({...value,pinnedBackupIds:Object.freeze([...value.pinnedBackupIds]),operatorStateFiles:Object.freeze([...value.operatorStateFiles])});
}
function contextRecord(context){const r=postgresBackupContext(context);if(r.closed||r.pendingBinding||r.receiverBinding||r.resumeBinding||r.applicationPlan)fail('backup_retention_context_invalid');return r;}
async function retentionOperation(context,configuration,options,operation){
 const policy=postgresBackupRetentionPolicy(configuration);if(!policy.enabled)return output('retention_disabled');const record=contextRecord(context);if(record.busy)fail('backup_operation_busy');
 record.busy=true;const cancellation=new AbortController();record.cancellation=cancellation;const signal=AbortSignal.any([options?.signal,cancellation.signal].filter(Boolean));
 const task=(async()=>{signal.throwIfAborted();return operation(policy,{...options,signal});})();record.pending=task;
 try{return await task;}finally{record.busy=false;record.pending=null;record.cancellation=null;}
}
async function recheckPolicy(record,policy){
 if(record.scope==='existing-cluster'){const {loadPostgresBackupRetentionProfile}=await import('./postgres-backup-context.mjs');if(hash(await loadPostgresBackupRetentionProfile())!==hash(policy))fail('backup_retention_policy_changed');}
}
export async function preparePostgresBackupRetention(context,configuration,options={}){return retentionOperation(context,configuration,options,(policy,parameters)=>prepareRetentionInner(context,policy,parameters));}
export async function createPostgresBackupRetentionPlan(context,configuration,options={}){return retentionOperation(context,configuration,options,(policy,parameters)=>createRetentionPlanInner(context,policy,parameters));}
export async function applyPostgresBackupRetention(context,configuration,planId,options={}){return retentionOperation(context,configuration,options,(policy,parameters)=>applyRetentionInner(context,policy,planId,parameters));}
async function commandDigest(record){const store=new PostgresStore({...record.adminOptions,poolSize:1});try{const command=(await store.get("SELECT current_setting('archive_command') value")).value;if(!command)fail('backup_retention_archive_command_missing');return hash(command);}finally{await store.close();}}
async function rejectUncoordinatedProcesses(record){
 // Managed host commands are upgraded before preparing coordination. Do not
 // infer that a process which loaded the former implementation holds our FD.
 for(const name of await readdir('/proc')){if(!/^[1-9]\d*$/.test(name)||Number(name)===process.pid)continue;try{
  const args=(await readFile('/proc/'+name+'/cmdline')).toString().split('\0');
  if(args.some(arg=>/(?:^|\/)storage-(?:backup|maintenance)\.mjs$/.test(arg))){const index=args.indexOf('--directory'),directory=index>=0?args[index+1]:null;if(directory&&directory!==record.archiveDirectory&&directory!==record.artifactDirectory&&!directory.startsWith(record.artifactDirectory+'/'))continue;const entry=args.findIndex(arg=>/(?:^|\/)storage-backup\.mjs$/.test(arg));if(entry>=0&&args[entry+1]==='schedule'&&await isCoordinatedPostgresBackupScheduleProcess(record,Number(name)))continue;fail('backup_retention_legacy_operation_present');}
 }catch(error){if(!['ENOENT','ESRCH','EACCES'].includes(error.code))throw error;}}
 if((await readdir(record.restoreDirectory)).length)fail('backup_retention_restore_directory_not_empty');
}
async function intentFor(record,current){
 const intent=await readBackupCoordinationIntent({directory:record.archiveDirectory,key:record.key,uid:record.restoreUid});
 if(!intent||intent.systemId!==current.system_id||intent.postmasterStart!==current.postmaster_start||intent.commandDigest!==await commandDigest(record)||intent.codeDigest!==await backupCoordinationCodeDigest())fail('backup_retention_coordination_required');return intent;
}
async function coordinated(record,current,lease,{witness,retainWitness=false,archiveIdentity,retirement}={}){
 const receiptPath=join(record.archiveDirectory,backupCoordinationFiles.receipt),intent=await intentFor(record,current),receipt=await readWalManifest(receiptPath,record.key,{ownerUid:record.restoreUid});
 if(receipt.version!==1||receipt.protocol!==backupCoordinationProtocol||!same(Object.fromEntries(['nonce','systemId','postmasterStart','commandDigest','codeDigest'].map(k=>[k,receipt[k]])),Object.fromEntries(['nonce','systemId','postmasterStart','commandDigest','codeDigest'].map(k=>[k,intent[k]])))||!same(receipt.lockIdentity,lease.identity)||!walName(receipt.walName))fail('backup_retention_archiver_not_coordinated');
 if(lease.exclusive){
  if(!witness||!witness.retained||witness.receiptDigest!==hash(receipt))fail('backup_retention_coordination_witness_changed');
  await verifyCoordinationWitness(record,witness);
  await rejectUncoordinatedProcesses(record);
 }else{
  const metadata=await readWalManifest(join(record.archiveDirectory,receipt.walName+'.meta.enc'),record.key,{ownerUid:record.restoreUid}),names=['cluster.enc',receipt.walName+'.meta.enc',receipt.walName+'.enc'];
  if(metadata.identity?.history){if(!historyName(metadata.identity.history.name))fail('backup_retention_history_invalid');names.push(metadata.identity.history.name+'.meta.enc',metadata.identity.history.name+'.enc');}
  const files=[];let retained=false;
  try{
   for(const path of ['',...names])files.push(await openCoordinationWitnessEntry(record,path));if(archiveIdentity){const {path,handle,...identity}=files[0];if(!same(identity,archiveIdentity))fail('backup_retention_archive_changed');}witness={receiptDigest:hash(receipt),files,retained:retainWitness};await verifyCoordinationWitness(record,witness);if(retainWitness)await archiveNames(record,{retirement});
   if(hash(await readWalManifest(join(record.archiveDirectory,receipt.walName+'.meta.enc'),record.key,{ownerUid:record.restoreUid}))!==hash(metadata))fail('backup_retention_coordination_witness_changed');
   await inspectArchivedWal({name:receipt.walName,directory:record.archiveDirectory,key:record.key,ownerUid:record.restoreUid,requireBoundName:true});
   if(hash(await readWalManifest(receiptPath,record.key,{ownerUid:record.restoreUid}))!==hash(receipt))fail('backup_retention_coordination_witness_changed');
   await verifyCoordinationWitness(record,witness);retained=retainWitness;
  }finally{if(!retained)await closeCoordinationWitness({files});}
 }
 return {...intent,receiptWalName:receipt.walName,coordinationWitness:witness};
}
async function prepareRetentionInner(context,configuration,{signal}={}){
 const policy=postgresBackupRetentionPolicy(configuration);if(!policy.enabled)return output('retention_disabled');const record=contextRecord(context),current=await verifyBackupCluster(context);
 return withBackupArchiveLock({directory:record.archiveDirectory,uid:record.restoreUid,exclusive:true,signal},async lease=>{
  await rejectUncoordinatedProcesses(record);const value={version:1,protocol:backupCoordinationProtocol,nonce:randomBytes(16).toString('hex'),systemId:current.system_id,postmasterStart:current.postmaster_start,commandDigest:await commandDigest(record),codeDigest:await backupCoordinationCodeDigest()};
  const path=join(record.archiveDirectory,backupCoordinationFiles.intent),temporary=join(record.archiveDirectory,'.coordination-'+randomBytes(12).toString('hex')+'.enc');
  try{await writeManifest(temporary,value,record.key);if(process.getuid()===0&&record.restoreUid!==0)await chown(temporary,record.restoreUid,record.restoreGid);await lease.assertPath();await rename(temporary,path);await syncDirectory(record.archiveDirectory);}finally{await rm(temporary,{force:true});}
  return output('retention_coordination_prepared',{archiverReceiptRequired:true});
 });
}

function statIdentity(s,uid){if((!s.isFile()&&!s.isDirectory())||s.isSymbolicLink()||s.uid!==BigInt(uid)||(s.mode&0o077n)!==0n||s.isFile()&&s.nlink!==1n)fail('backup_retention_entry_untrusted');return {type:s.isDirectory()?'directory':'file',dev:String(s.dev),ino:String(s.ino),bytes:Number(s.size),uid:Number(s.uid),mode:Number(s.mode&0o777n),mtime:String(s.mtimeNs),ctime:String(s.ctimeNs)};}
async function statEntry(path,uid=process.getuid()){return statIdentity(await lstat(path,{bigint:true}),uid);}
async function openCoordinationWitnessEntry(record,path){
 const handle=await open(join(record.archiveDirectory,path),constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
 try{return {path,handle,...statIdentity(await handle.stat({bigint:true}),record.restoreUid)};}catch(error){await handle.close();throw error;}
}
async function closeCoordinationWitness(witness){witness.retained=false;const results=await Promise.allSettled(witness.files.map(file=>file.handle.close()));const failed=results.find(result=>result.status==='rejected');if(failed)throw failed.reason;}
async function verifyCoordinationWitness(record,witness){
 for(const file of witness.files){const current=statIdentity(await file.handle.stat({bigint:true}),record.restoreUid),fields=['type','dev','ino','uid','mode',...(file.type==='file'?['bytes','mtime','ctime']:witness.retained?['mtime','ctime']:[])];if(fields.some(name=>current[name]!==file[name]))fail('backup_retention_identity_changed');await verifyEntry(join(record.archiveDirectory,file.path),file,{moved:file.type==='directory'&&!witness.retained,uid:record.restoreUid});}
}
async function tree(directory,allowed,{signal}={}){
 const entries=[];async function visit(path,name=''){
  signal?.throwIfAborted();const identity=await statEntry(path);if(identity.type==='file'&&!allowed.has(name))fail('backup_retention_unknown_file_requires_pin');
  const row={path:name,...identity};if(identity.type==='file')row.sha256=(await digestFile(path,{signal})).sha256;entries.push(row);if(entries.length>100000)fail('backup_retention_plan_capacity');
  if(identity.type==='directory'){if(name&&![...allowed].some(file=>file.startsWith(name+'/')))fail('backup_retention_unknown_directory_requires_pin');for(const child of (await readdir(path)).sort())await visit(join(path,child),name?name+'/'+child:child);}
 }await visit(directory);return entries;
}
async function authenticateGroup(record,path,keyName,{signal,now}={}){
 const id=keyName.split('/').at(-1),metadata=await readManifest(join(path,'backup.enc'),record.key,{signal});
 if(metadata.version!==1||metadata.backupId!==id||metadata.systemId!==record.expected.system_id||metadata.keyVersion!==record.keyVersion||metadata.scope!==record.scope||metadata.materialCoverage!=='database-only'||!/^[a-f0-9]{64}$/.test(metadata.configurationDigest??'')||!Number.isSafeInteger(metadata.created)||metadata.created>now||!Number.isInteger(metadata.timeline)||metadata.timeline<1||metadata.segmentBytes!==record.expected.segment_bytes)fail('backup_retention_backup_binding');
 const allowed=await authenticatePostgresBackupFileSet({directory:path,key:record.key,files:metadata.files,signal});allowed.add('backup.enc');
 const state=await readManifest(join(path,'state.enc'),record.key,{signal});if(state.version!==1||state.backupId!==id||state.configurationDigest!==metadata.configurationDigest||state.systemId!==metadata.systemId)fail('backup_retention_backup_state');allowed.add('state.enc');
 const checkpoint=await readManifest(join(path,'checkpoint.enc'),record.key,{signal}),cpDigest=await digestFile(join(path,'checkpoint.enc'),{signal});
 if(checkpoint.version!==2||checkpoint.backupId!==id||checkpoint.systemId!==metadata.systemId||checkpoint.keyVersion!==metadata.keyVersion||checkpoint.configurationDigest!==metadata.configurationDigest||checkpoint.timeline!==metadata.timeline||checkpoint.segmentBytes!==metadata.segmentBytes)fail('backup_retention_checkpoint_binding');allowed.add('checkpoint.enc');
 const segments=[];for await(const row of readWalCheckpointSegments({directory:path,key:record.key,checkpoint,signal}))segments.push(row);
 let application=null,completeApplication=false;
 for(const name of await readdir(path)){
  if(/^wal-catalog-[a-f0-9]{32}$/.test(name)){
   await privateDirectory(join(path,name));for(const part of await readdir(join(path,name))){if(!/^part-\d+\.enc$/.test(part))fail('backup_retention_unknown_file_requires_pin');const item=await readManifest(join(path,name,part),record.key,{signal});if(item.version!==1||item.generation!==name.slice(12)||item.ordinal!==Number(part.slice(5,-4))||!Array.isArray(item.entries))fail('backup_retention_catalog_binding');allowed.add(name+'/'+part);}
  }else if(name==='restore.enc'){
   const receipt=await readManifest(join(path,name),record.key,{signal});if(receipt.version!==1||receipt.backupId!==id||receipt.configurationDigest!==metadata.configurationDigest||!digest(receipt.checkpointDigest))fail('backup_retention_restore_binding');allowed.add(name);
  }else if(name==='application'){
   await privateDirectory(join(path,name));application=await readManifest(join(path,name,'application.enc'),record.key,{signal});
   if(application.version!==1||application.backupId!==id||application.configurationDigest!==metadata.configurationDigest||application.keyVersion!==metadata.keyVersion||!same(application.checkpointDigest,cpDigest))fail('backup_retention_application_binding');
   for(const file of await authenticatePostgresBackupFileSet({directory:join(path,name),key:record.key,files:application.files,signal}))allowed.add('application/'+file);allowed.add('application/application.enc');
   if(!Array.isArray(application.auditFiles))fail('backup_retention_application_binding');for(const audit of application.auditFiles){if(!['audit-source.enc','audit-source-manifest.enc','audit-target.enc'].includes(audit.artifact)||!same(await digestFile(join(path,name,audit.artifact),{signal}),audit.digest))fail('backup_retention_application_binding');await readManifest(join(path,name,audit.artifact),record.key,{signal});allowed.add('application/'+audit.artifact);}
  }
 }
 if((await readdir(path)).includes('operator-final.enc')){
  const final=await readManifest(join(path,'operator-final.enc'),record.key,{signal});
  if(!application||final.version!==1||final.stage!=='pending-final-copy-and-local-restore'||final.backupId!==id||final.configurationDigest!==metadata.configurationDigest||!same(final.checkpointDigest,cpDigest)||!same(final.applicationManifestDigest,await digestFile(join(path,'application/application.enc'),{signal}))||final.recovery?.restoreVerified!==true||final.recovery?.application?.filesRestoredVerified!==true||final.recovery?.application?.originalStoredCredentialsVerified!==true||final.recovery?.application?.originalRowsAndAmountsVerified!==true)fail('backup_retention_final_binding');allowed.add('operator-final.enc');completeApplication=true;
 }
 if(application&&!completeApplication)fail('backup_retention_incomplete_application_requires_pin');
 if((await readdir(path)).includes('handoff')){
  const bundle=join(path,'handoff'),handoff=await readManifest(join(bundle,'handoff.enc'),record.key,{signal});
  if(handoff.version!==1||handoff.backupId!==id||handoff.configurationDigest!==metadata.configurationDigest||handoff.keyVersion!==metadata.keyVersion||!same(handoff.checkpointDigest,cpDigest))fail('backup_retention_handoff_binding');
  const {handoffFiles}=await import('./postgres-backup-handoff.mjs');for await(const entry of handoffFiles({directory:bundle,key:record.key,manifest:handoff,signal,maxFiles:100000,maxBytes:record.profile.maxBackupBytes*2}))allowed.add('handoff/'+entry.path);allowed.add('handoff/handoff.enc');for(const part of handoff.parts)allowed.add('handoff/'+part.name);
 }
 for(const file of await authenticatePeriodicBaseRetention(record,path,{signal}))allowed.add(file);
 const periodic=await authenticatePeriodicApplicationRetentionGroup(record,path,metadata,{signal,now});for(const file of periodic.allowed)allowed.add(file);for(const segment of periodic.segments)segments.push(segment);
 const applicationCheckpointAt=periodic.backupCompletePoints.reduce((latest,point)=>Math.max(latest,point.checkpointAt),completeApplication?metadata.created:0);completeApplication ||= periodic.backupCompletePoints.length>0;
 const latestBackupAt=periodic.backupCompletePoints.reduce((latest,point)=>Math.max(latest,point.completedAt),metadata.created);
 const files=await tree(path,allowed,{signal});return {id,path:keyName,metadata,checkpoint,segments,application,completeApplication,applicationCheckpointAt,latestBackupAt,periodic,files,bytes:files.filter(f=>f.type==='file').reduce((n,f)=>n+f.bytes,0),signature:hash(files)};
}

async function externalReferences(record,policy){
 const protectedIds=new Set(policy.pinnedBackupIds),runs=new Set(),files=[];
 for(const file of policy.operatorStateFiles){const state=await readManifest(file,record.key);if(state.version!==1||!uuid(state.runId)||state.cluster!==record.expected.system_id||typeof state.phase!=='string')fail('backup_retention_operator_binding');runs.add(state.runId);if(state.baseline){if(!uuid(state.baseline.backupId))fail('backup_retention_operator_binding');protectedIds.add(state.baseline.backupId);}files.push({file,digest:await digestFile(file)});}
 return {protectedIds,runs,files};
}
async function readScheduleState(record,root,protectedIds){
 let state;try{state=await readManifest(join(root,'periodic-state.enc'),record.key);}catch(error){if(error.code==='ENOENT')return null;throw error;}
 if(state.version!==1||state.scope!=='harbor-periodic-database-backup-v1'||state.systemId!==record.expected.system_id||state.keyVersion!==record.keyVersion)fail('backup_retention_schedule_binding');
 for(const name of ['active','pending']){const item=state[name];if(item!==null){if(!uuid(item?.backupId))fail('backup_retention_schedule_binding');protectedIds.add(item.backupId);}}
 return state;
}
async function groupPaths(record){
 const paths=[];for(const name of (await readdir(record.artifactDirectory)).sort()){
  if(uuid(name)){paths.push(name);continue;}if(['periodic-state.enc','periodic-application-state.enc','.retention'].includes(name))continue;if(name===backupScheduleCoordinationDirectory){await inspectPostgresBackupScheduleRegistrations(record);continue;}
  if(/^pending-[a-f0-9-]{36}$/.test(name)&&uuid(name.slice(8))){await privateDirectory(join(record.artifactDirectory,name));for(const child of (await readdir(join(record.artifactDirectory,name))).sort()){if(!uuid(child))fail('backup_retention_unknown_directory_requires_pin');paths.push(name+'/'+child);}continue;}
  fail('backup_retention_unknown_directory_requires_pin');
 }return paths;
}
async function readRetentionApplicationState(record){
 const result=await readPeriodicApplicationRetentionState(record);if(result&&result.state.version!==3)fail('backup_retention_application_state_version');return result;
}
async function sourceReferences(record,policy){
 const refs=await externalReferences(record,policy),schedule=await readScheduleState(record,record.artifactDirectory,refs.protectedIds),applicationState=await readRetentionApplicationState(record),paths=await groupPaths(record),groups=[];for(const id of applicationState?.protectedBackupIds??[])refs.protectedIds.add(id);
 for(const path of paths){const base=join(record.artifactDirectory,path),files=[];for(const name of ['backup.enc','checkpoint.enc','application/application.enc','operator-final.enc','application-final.enc','handoff/handoff.enc'])try{files.push({name,digest:await digestFile(join(base,name))});}catch(error){if(error.code!=='ENOENT')throw error;}const metadata=await readManifest(join(base,'backup.enc'),record.key),periodic=await periodicApplicationRetentionReferences(record,base,metadata);groups.push({path,files,periodic,baseVerification:await periodicBaseRetentionReferences(record,base)});}
 return {...refs,paths,applicationState,digest:hash({policy,files:refs.files,schedule,applicationState,groups})};
}
function authenticateApplicationReferences(refs,groups){
 const state=refs.applicationState?.state;if(!state)return;
 for(const name of ['latestBackup','lastVerified','pending','pendingVerification']){
  const row=state[name];if(!row)continue;const group=groups.find(group=>group.id===row.backupId);if(!group)fail('backup_retention_application_reference_missing');
  if(name==='latestBackup'||name==='lastVerified'){
   const items=name==='latestBackup'?group.periodic.backupCompletePoints:group.periodic.verifiedPoints,point=items.find(point=>point.pointId===row.pointId),field=name==='latestBackup'?'completedAt':'verifiedAt';
   if(!point||point.checkpointAt!==row.checkpointAt||point[field]!==row[field])fail('backup_retention_application_reference_binding');
  }
 }
}
function historyAncestors(timeline,bytes){
 let previous=0,point=-1n;const ancestors=[];
 for(const line of bytes.toString('utf8').split('\n')){const text=line.trim();if(!text||text.startsWith('#'))continue;const m=/^(\d+)\s+([0-9A-F]+\/[0-9A-F]+)(?:\s+.*)?$/.exec(text);if(!m)fail('backup_retention_history_invalid');const t=Number(m[1]),at=lsn(m[2]);if(!Number.isInteger(t)||t<=previous||t>=timeline||at<point)fail('backup_retention_history_invalid');ancestors.push(t);previous=t;point=at;}
 if(timeline>1&&!ancestors.length)fail('backup_retention_history_invalid');return ancestors;
}
async function archiveNames(record,{retirement}={}){
 const directoryIdentity=await statEntry(record.archiveDirectory,record.restoreUid),names=await readdir(record.archiveDirectory),seen=new Set(names);
 for(const name of names){
  if(['cluster.enc',...Object.values(backupCoordinationFiles)].includes(name)||/^\.(?:wal|manifest)-[a-f0-9]+$/.test(name))continue;
  if(/^\.retention-[a-f0-9-]{36}$/.test(name)){
   const id=name.slice(11),plan=await readManifest(join(record.artifactDirectory,'.retention',id,'plan.enc'),record.key);if(plan.version!==1||plan.scope!==scope||plan.planId!==id||plan.systemId!==record.expected.system_id)fail('backup_retention_unknown_archive_entry');await verifyEntry(join(record.archiveDirectory,name),plan.walQuarantineIdentity,{moved:true});if((await readdir(join(record.archiveDirectory,name))).length&&!(retirement?.allowMoved&&retirement.planId===id))fail('backup_retention_previous_apply_incomplete');continue;
  }
  if(!/^(?:[0-9A-F]{24}|[0-9A-F]{8}\.history|[0-9A-F]{24}\.[0-9A-F]{8}\.backup)\.(?:meta\.)?enc$/.test(name))fail('backup_retention_unknown_archive_entry');
  if((await statEntry(join(record.archiveDirectory,name),record.restoreUid)).type!=='file')fail('backup_retention_entry_untrusted');
  const counterpart=name.endsWith('.meta.enc')?name.slice(0,-9)+'.enc':name.slice(0,-4)+'.meta.enc';if(seen.has(counterpart))continue;
  const group=retirement?.allowMoved&&retirement.groups.find(group=>group.type==='wal'&&group.files.some(file=>file.path===name));if(!group)fail('backup_retention_archive_pair_incomplete');
  for(const file of group.files){const source=join(record.archiveDirectory,file.path),target=join(record.archiveDirectory,'.retention-'+retirement.planId,file.path),from=await present(source),to=await present(target);if(from===to)fail('backup_retention_move_ambiguous');await verifyEntry(from?source:target,file,{moved:to,uid:record.restoreUid});}
 }
 if(!same(directoryIdentity,await statEntry(record.archiveDirectory,record.restoreUid)))fail('backup_retention_archive_changed');return {names,directoryIdentity};
}
async function archiveSnapshot(record,{signal,retirement}={}){
 const {names,directoryIdentity}=await archiveNames(record,{retirement}),segments=[],histories=new Map();
 for(const name of names.sort()){
  if(!/^(?:[0-9A-F]{24}|[0-9A-F]{8}\.history|[0-9A-F]{24}\.[0-9A-F]{8}\.backup)\.meta\.enc$/.test(name))continue;
  const base=name.slice(0,-9);if(retirement?.groups.some(group=>group.type==='wal'&&group.source===base))continue;let value;try{value=await inspectArchivedWal({name:base,directory:record.archiveDirectory,key:record.key,ownerUid:record.restoreUid,requireBoundName:true});}catch(error){if(error.code==='ENOENT')fail('backup_retention_archive_pair_incomplete');throw error;}
  if(value.systemIdentifier!==record.expected.system_id)fail('backup_retention_archive_binding');
  if(historyName(base))histories.set(Number.parseInt(base.slice(0,8),16),{name:base,ancestors:historyAncestors(Number.parseInt(base.slice(0,8),16),value.historyBytes)});
  else if(walName(base)){signal?.throwIfAborted();segments.push({name:base,timeline:value.identity.timeline,index:BigInt(value.identity.pageAddress)/BigInt(value.content.bytes),meta:value});}
 }
 if(!same(directoryIdentity,await statEntry(record.archiveDirectory,record.restoreUid)))fail('backup_retention_archive_changed');return {segments,histories,directoryIdentity};
}
async function retainedWalClosure(record,current,retained,{signal,retirement}={}){
 const archive=await archiveSnapshot(record,{signal,retirement}),receipt=await readWalManifest(join(record.archiveDirectory,backupCoordinationFiles.receipt),record.key,{ownerUid:record.restoreUid}),protectedWal=new Set(),minimum=new Map();
 if(!walName(receipt.walName)||!archive.segments.some(row=>row.name===receipt.walName))fail('backup_retention_archiver_not_coordinated');protectedWal.add(receipt.walName);
 for(const group of retained){const start=lsn(group.metadata.walRanges?.at(-1)?.['End-LSN'])/BigInt(group.metadata.segmentBytes),prior=minimum.get(group.metadata.timeline);minimum.set(group.metadata.timeline,prior===undefined||start<prior?start:prior);for(const segment of group.segments){const actual=archive.segments.find(row=>row.name===segment.name);if(!actual||!same(actual.meta.encrypted,segment.encrypted)||!same(actual.meta.content,segment.content))fail('backup_retention_required_wal_changed');protectedWal.add(segment.name);}}
 const currentHistory=current.timeline===1?[]:archive.histories.get(current.timeline)?.ancestors;if(!currentHistory)fail('backup_retention_current_history_missing');const lineage=new Set([...currentHistory,current.timeline]);
 for(const row of archive.segments){if(row.timeline>1&&!archive.histories.has(row.timeline))fail('backup_retention_history_missing');if(!lineage.has(row.timeline)&&!minimum.has(row.timeline))fail('backup_retention_unowned_timeline');if(minimum.has(row.timeline)&&row.index>=minimum.get(row.timeline))protectedWal.add(row.name);}
 for(const [timeline,start] of minimum){const indexes=archive.segments.filter(r=>r.timeline===timeline&&r.index>=start).map(r=>r.index).sort((a,b)=>a<b?-1:1);for(let index=0;index<indexes.length;index++)if(indexes[index]!==start+BigInt(index))fail('backup_retention_wal_gap');if(!indexes.length)fail('backup_retention_wal_gap');}
 return {archive,protectedWal};
}
async function inspectInventory(context,policy,{signal,clock=Date.now}={}){
 const record=contextRecord(context),current=await verifyBackupCluster(context),now=clock();if(!Number.isSafeInteger(now)||now<0)fail('backup_retention_clock_invalid');
 const refs=await sourceReferences(record,policy),groups=[];for(const path of refs.paths){signal?.throwIfAborted();groups.push(await authenticateGroup(record,join(record.artifactDirectory,path),path,{signal,now}));}
 if(new Set(groups.map(g=>g.id)).size!==groups.length)fail('backup_retention_duplicate_backup_id');authenticateApplicationReferences(refs,groups);
 const sorted=[...groups].sort((a,b)=>b.metadata.created-a.metadata.created||a.path.localeCompare(b.path)),keep=new Set(),reasons=new Map();
 const protect=(g,reason)=>{keep.add(g.path);reasons.set(g.path,[...(reasons.get(g.path)??[]),reason]);};
 for(const g of sorted.slice(0,policy.keepDatabaseCount))protect(g,'database_count');for(const g of sorted.filter(g=>g.completeApplication).sort((a,b)=>b.applicationCheckpointAt-a.applicationCheckpointAt||a.path.localeCompare(b.path)).slice(0,policy.keepApplicationCount))protect(g,'application_count');
 let lastVerified=null;for(const group of groups)for(const point of group.periodic.verifiedPoints)if(!lastVerified||point.verifiedAt>lastVerified.point.verifiedAt||point.verifiedAt===lastVerified.point.verifiedAt&&group.path.localeCompare(lastVerified.group.path)<0)lastVerified={group,point};if(lastVerified)protect(lastVerified.group,'last_restore_verified');
 for(const g of groups)if(g.periodic.incomplete)protect(g,'periodic_application_incomplete');
 for(const g of groups){if(refs.protectedIds.has(g.id))protect(g,'external_reference');if(g.application&&refs.runs.has(g.application.runId)||g.path.startsWith('pending-')&&refs.runs.has(g.path.split('/')[0].slice(8)))protect(g,'operator_run');if(now-g.latestBackupAt<policy.minAgeMs)protect(g,'minimum_age');}
 const retained=groups.filter(g=>keep.has(g.path));if(!retained.some(g=>g.metadata.timeline===current.timeline&&g.metadata.postmasterStart===current.postmaster_start))fail('backup_retention_current_base_required');
 const {archive,protectedWal}=await retainedWalClosure(record,current,retained,{signal});
 const removals=groups.filter(g=>!keep.has(g.path)),wal=archive.segments.filter(row=>!protectedWal.has(row.name));
 return {record,current,now,refs,groups,retained,removals,wal,reasons,historyRetained:archive.histories.size,archiveIdentity:archive.directoryIdentity};
}

function latestPeriodicPoint(points,field){return points.reduce((latest,point)=>!latest||point[field]>latest[field]||point[field]===latest[field]&&point.pointId.localeCompare(latest.pointId)<0?point:latest,null);}
function retainedGroupReference(group,reasons){return {path:group.path,id:group.id,application:group.completeApplication,reasons,latestBackupPoint:latestPeriodicPoint(group.periodic.backupCompletePoints,'checkpointAt'),lastVerifiedPoint:latestPeriodicPoint(group.periodic.verifiedPoints,'verifiedAt')};}
async function controlDirectory(record,planId,{create=false}={}){
 if(!uuid(planId))fail('backup_retention_plan_id_invalid');const root=join(record.artifactDirectory,'.retention');if(create)await mkdir(root,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await privateDirectory(root);if(create)await syncDirectory(record.artifactDirectory);const directory=join(root,planId);if(create)await mkdir(directory,{mode:0o700});await privateDirectory(directory);if(create)await syncDirectory(root);return directory;
}
async function savePages(directory,key,groups){
 const parts=[];let items=[],bytes=0,total=0;const flush=async()=>{if(!items.length)return;const name='part-'+parts.length+'.enc';await writeManifest(join(directory,name),{version:1,ordinal:parts.length,items},key);parts.push({name,count:items.length,digest:await digestFile(join(directory,name))});items=[];bytes=0;};
 for(const group of groups)for(const item of [{kind:'group',id:group.id,type:group.type,source:group.source},...group.files.map(file=>({kind:'entry',group:group.id,...file}))]){const length=Buffer.byteLength(JSON.stringify(item));if(length>524288)fail('backup_retention_plan_capacity');if(bytes+length>524288)await flush();items.push(item);bytes+=length;total++;}await flush();return {parts,count:total};
}
async function loadPages(directory,key,plan){
 if(!Array.isArray(plan.parts)||!Number.isSafeInteger(plan.count)||plan.count<0)fail('backup_retention_plan_catalog');const groups=[],map=new Map();let total=0;
 for(const [index,part] of plan.parts.entries()){
  if(part.name!=='part-'+index+'.enc'||!digest(part.digest)||!same(await digestFile(join(directory,part.name)),part.digest))fail('backup_retention_plan_changed');const value=await readManifest(join(directory,part.name),key);if(value.version!==1||value.ordinal!==index||!Array.isArray(value.items)||value.items.length!==part.count)fail('backup_retention_plan_catalog');
  for(const item of value.items){total++;if(item.kind==='group'){if(!/^g\d+$/.test(item.id??'')||map.has(item.id)||!['database','wal'].includes(item.type)||typeof item.source!=='string')fail('backup_retention_plan_catalog');const row={id:item.id,type:item.type,source:item.source,files:[]};if(row.type==='database'&&!/^(?:pending-[a-f0-9-]{36}\/)?[a-f0-9-]{36}$/.test(row.source)||row.type==='wal'&&!walName(row.source))fail('backup_retention_plan_catalog');groups.push(row);map.set(row.id,row);}else{const group=map.get(item.group);if(item.kind!=='entry'||!group||typeof item.path!=='string'||item.path.split('/').some(p=>p==='.'||p==='..')||item.path.startsWith('/')||!['file','directory'].includes(item.type))fail('backup_retention_plan_catalog');group.files.push(item);}}
 }if(total!==plan.count)fail('backup_retention_plan_catalog');return groups;
}
function sourcePath(record,group){return join(group.type==='database'?record.artifactDirectory:record.archiveDirectory,group.source);}
function quarantinePath(record,directory,planId,group){return group.type==='database'?join(directory,'quarantine',group.id):join(record.archiveDirectory,'.retention-'+planId);}
async function verifyEntry(path,expected,{moved=false,content=false,uid=process.getuid()}={}){
 const s=await lstat(path,{bigint:true}),actual={type:s.isDirectory()?'directory':s.isFile()?'file':'other',dev:String(s.dev),ino:String(s.ino),bytes:Number(s.size),uid:Number(s.uid),mode:Number(s.mode&0o777n),mtime:String(s.mtimeNs),ctime:String(s.ctimeNs)};
 if(s.isSymbolicLink()||s.isFile()&&s.nlink!==1n||actual.uid!==uid||['type','dev','ino','uid','mode',...(actual.type==='file'?['bytes','mtime']:[]),...(!moved?['ctime']:[])].some(name=>actual[name]!==expected[name])||content&&actual.type==='file'&&(await digestFile(path)).sha256!==expected.sha256)fail('backup_retention_identity_changed');
}
async function verifyGroup(record,group,base,{moved=false,content=false}={}){
 if(group.type==='database'){
  for(const file of group.files)await verifyEntry(join(base,file.path),file,{moved,content});
  const expected=new Set(group.files.map(file=>file.path));async function visit(path,prefix=''){for(const name of await readdir(path)){const relative=prefix?prefix+'/'+name:name;if(!expected.has(relative))fail('backup_retention_inventory_changed');const info=await lstat(join(path,name));if(info.isDirectory()&&!info.isSymbolicLink())await visit(join(path,name),relative);}}await visit(base);
 }else for(const file of group.files)await verifyEntry(join(base,file.path),file,{moved,content,uid:record.restoreUid});
}

async function withOperatorLocks(policy,operation){
 const locks=[];try{for(const directory of [...new Set(policy.operatorStateFiles.map(dirname))].sort()){await privateDirectory(directory);try{locks.push(await acquireInstance(await realpath(directory)));}catch(error){if(error.code==='EADDRINUSE')fail('backup_retention_operator_busy');throw error;}}return await operation();}finally{for(const lock of locks.reverse())await lock.release();}
}
async function unresolvedPlan(record,except){
 const root=join(record.artifactDirectory,'.retention');let names;try{names=await readdir(root);}catch(error){if(error.code==='ENOENT')return;throw error;}
 for(const name of names){if(!uuid(name))fail('backup_retention_control_unknown');if(name===except)continue;const directory=await controlDirectory(record,name);let journal;try{journal=await readManifest(join(directory,'journal.enc'),record.key);}catch(error){if(error.code==='ENOENT')continue;throw error;}if(journal.phase!=='complete')fail('backup_retention_previous_apply_incomplete');}
}
// A durable reservation is published before an archive directory is allocated.
// Until its inode is bound, only this reserved, empty root-owned allocation can
// be completed; no backup can be moved before the finished plan and journal.
async function publishPreparedPlan(record,directory,preparation,{signal,onTransition=()=>{}}={}){
 const plan=preparation.plan;if(preparation.version!==1||preparation.scope!==scope+'-preparation'||plan?.version!==1||plan.scope!==scope||!uuid(plan.planId)||directory!==join(record.artifactDirectory,'.retention',plan.planId)||plan.configurationDigest!==record.configurationDigest||plan.systemId!==record.expected.system_id||plan.keyVersion!==record.keyVersion||!/^[a-f0-9]{64}$/.test(plan.policyDigest??''))fail('backup_retention_preparation_binding');
 await loadPages(directory,record.key,plan);await verifyEntry(record.archiveDirectory,preparation.archiveDirectoryIdentity,{moved:true,uid:record.restoreUid});signal?.throwIfAborted();
 const path=join(record.archiveDirectory,'.retention-'+plan.planId);
 if(!await present(path)){if(preparation.walQuarantineIdentity)fail('backup_retention_identity_changed');await mkdir(path,{mode:0o700});await onTransition('plan_quarantine_created');}
 await privateDirectory(path);const identity=await statEntry(path);if(identity.mode!==0o700||(await readdir(path)).length)fail('backup_retention_allocation_not_empty');
 if(preparation.walQuarantineIdentity)await verifyEntry(path,preparation.walQuarantineIdentity,{moved:true});
 await syncDirectory(record.archiveDirectory);
 if(!preparation.walQuarantineIdentity){preparation={...preparation,walQuarantineIdentity:identity};await writeManifest(join(directory,'plan-preparation.enc'),preparation,record.key,{replace:true});}
 await onTransition('plan_quarantine_identity_recorded');signal?.throwIfAborted();const complete={...plan,walQuarantineIdentity:preparation.walQuarantineIdentity};await writeManifest(join(directory,'plan.enc'),complete,record.key);await onTransition('plan_published');return complete;
}
async function resumePreparedPlans(record,{signal}={}){
 const root=join(record.artifactDirectory,'.retention');if(!await present(root))return;await privateDirectory(root);
 for(const name of (await readdir(root)).sort()){
  const directory=await controlDirectory(record,name);if(await present(join(directory,'plan.enc')))continue;
  let preparation;try{preparation=await readManifest(join(directory,'plan-preparation.enc'),record.key);}catch(error){if(error.code==='ENOENT')continue;throw error;}
  await withBackupArchiveLock({directory,exclusive:true,signal},()=>publishPreparedPlan(record,directory,preparation,{signal}));
 }
}
async function createRetentionPlanInner(context,configuration,{signal,clock=Date.now,onTransition=()=>{}}={}){
 const policy=postgresBackupRetentionPolicy(configuration);if(!policy.enabled)return output('retention_disabled');const record=contextRecord(context);
 return withOperatorLocks(policy,()=>withPostgresBackupOperation(record,async lease=>{
  const current=await verifyBackupCluster(context),intent=await coordinated(record,current,lease);await unresolvedPlan(record);await resumePreparedPlans(record,{signal});
  const inventory=await inspectInventory(context,policy,{signal,clock}),planId=randomUUID(),directory=await controlDirectory(record,planId,{create:true}),groups=[];let index=0;
  for(const group of inventory.removals)groups.push({id:'g'+index++,type:'database',source:group.path,files:group.files});
  for(const row of inventory.wal){const files=[];for(const name of [row.name+'.enc',row.name+'.meta.enc'])files.push({path:name,...await statEntry(join(record.archiveDirectory,name),record.restoreUid),sha256:(await digestFile(join(record.archiveDirectory,name),{signal})).sha256});groups.push({id:'g'+index++,type:'wal',source:row.name,files});}
  const catalog=await savePages(directory,record.key,groups),draft={version:1,scope,planId,configurationDigest:record.configurationDigest,systemId:current.system_id,keyVersion:record.keyVersion,policyDigest:hash(policy),postmasterStart:current.postmaster_start,timeline:current.timeline,coordinationNonce:intent.nonce,created:inventory.now,referenceDigest:inventory.refs.digest,...catalog,groups:groups.length,retained:inventory.retained.map(g=>retainedGroupReference(g,inventory.reasons.get(g.path))),deleteDatabases:inventory.removals.length,deleteWalSegments:inventory.wal.length,bytes:groups.reduce((n,g)=>n+g.files.filter(f=>f.type==='file').reduce((sum,f)=>sum+f.bytes,0),0)};
  // A plan is only an authenticated preview. Apply reauthenticates its inputs
  // before the short exclusive retirement section; publication changes no data.
  await lease.assertPath();const plan=await withBackupArchiveLock({directory,exclusive:true,signal},async()=>{const preparation={version:1,scope:scope+'-preparation',plan:draft,archiveDirectoryIdentity:await statEntry(record.archiveDirectory,record.restoreUid),walQuarantineIdentity:null};await writeManifest(join(directory,'plan-preparation.enc'),preparation,record.key);await onTransition('plan_reserved');return publishPreparedPlan(record,directory,preparation,{signal,onTransition});});return output('retention_plan_created',{planId,planDigest:await digestFile(join(directory,'plan.enc')),deleteDatabases:plan.deleteDatabases,deleteWalSegments:plan.deleteWalSegments,bytes:plan.bytes,retainedDatabaseGroups:plan.retained.length,retainedApplicationGroups:plan.retained.filter(g=>g.application).length,historyFilesPreserved:inventory.historyRetained});
 },{signal}));
}
async function readPlan(record,policy,planId){
 const directory=await controlDirectory(record,planId),plan=await readManifest(join(directory,'plan.enc'),record.key);
 if(plan.version!==1||plan.scope!==scope||plan.planId!==planId||plan.configurationDigest!==record.configurationDigest||plan.systemId!==record.expected.system_id||plan.keyVersion!==record.keyVersion||plan.policyDigest!==hash(policy)||!Number.isSafeInteger(plan.created)||!/^\d+(?:\.\d+)?$/.test(plan.postmasterStart??'')||!Number.isInteger(plan.timeline)||plan.timeline<1)fail('backup_retention_plan_binding');
 const groups=await loadPages(directory,record.key,plan);if(groups.length!==plan.groups)fail('backup_retention_plan_catalog');return {directory,plan,groups};
}
async function present(path){try{await lstat(path);return true;}catch(error){if(error.code==='ENOENT')return false;throw error;}}
async function assertApplyReferences(context,record,policy,plan,lease,{witness,retainWitness=false,archiveIdentity,retirement}={}){
 const current=await verifyBackupCluster(context);let intent;
 try{intent=await coordinated(record,current,lease,{witness,retainWitness,archiveIdentity,retirement});if(current.postmaster_start!==plan.postmasterStart||current.timeline!==plan.timeline||intent.nonce!==plan.coordinationNonce)fail('backup_retention_source_generation_changed');
  const refs=await externalReferences(record,policy);await readScheduleState(record,record.artifactDirectory,refs.protectedIds);const applicationState=await readRetentionApplicationState(record);for(const id of applicationState?.protectedBackupIds??[])refs.protectedIds.add(id);return {...refs,receiptWalName:intent.receiptWalName,coordinationWitness:intent.coordinationWitness};
 }catch(error){if(retainWitness&&intent?.coordinationWitness)await closeCoordinationWitness(intent.coordinationWitness);throw error;}
}
async function recheckCandidateReferences(record,policy,groups){
 const refs=await externalReferences(record,policy);await readScheduleState(record,record.artifactDirectory,refs.protectedIds);const applicationState=await readRetentionApplicationState(record);for(const id of applicationState?.protectedBackupIds??[])refs.protectedIds.add(id);
 for(const group of groups)if(group.type==='database'&&(refs.protectedIds.has(group.source.split('/').at(-1))||group.source.startsWith('pending-')&&refs.runs.has(group.source.split('/')[0].slice(8))))fail('backup_retention_candidate_now_referenced');
}
async function quarantineGroup(record,directory,planId,group,lease,onTransition){
 const destination=quarantinePath(record,directory,planId,group);
 if(group.type==='database'){
  const source=sourcePath(record,group),from=await present(source),to=await present(destination);if(from&&to||!from&&!to)fail('backup_retention_move_ambiguous');
  if(from){await verifyGroup(record,group,source);await lease.assertPath();await rename(source,destination);await onTransition('database_renamed');}
  await syncDirectory(dirname(destination));await onTransition('database_destination_synced');await syncDirectory(dirname(source));
  await verifyGroup(record,group,destination,{moved:true});
 }else{
  await mkdir(destination,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await privateDirectory(destination);
  for(const file of group.files){const source=join(record.archiveDirectory,file.path),target=join(destination,file.path),from=await present(source),to=await present(target);if(from&&to||!from&&!to)fail('backup_retention_move_ambiguous');if(from){await verifyEntry(source,file,{uid:record.restoreUid});await lease.assertPath();await rename(source,target);await onTransition('wal_file_renamed');}else await verifyEntry(target,file,{moved:true,uid:record.restoreUid});}
  await onTransition('wal_pair_renamed');await syncDirectory(destination);await onTransition('wal_destination_synced');await syncDirectory(record.archiveDirectory);
 }
}
// A resumed journal owns only these original identities and acknowledged
// deletion cursors. It cannot turn a newly appearing source into its own data.
async function verifyRetirementLayout(record,directory,planId,groups,journal){
 const walQuarantine=join(record.archiveDirectory,'.retention-'+planId);if(await present(walQuarantine))await verifyEntry(walQuarantine,journal.walQuarantineIdentity,{moved:true});
 if(journal.databaseQuarantineIdentity)await verifyEntry(join(directory,'quarantine'),journal.databaseQuarantineIdentity,{moved:true});
 for(const [index,group] of groups.entries()){
  if(group.type==='database'){
   const source=sourcePath(record,group),target=quarantinePath(record,directory,planId,group),from=await present(source),to=await present(target);
   if(journal.phase==='prepared'){
    if(from===to||from&&index<journal.moveGroup||to&&index>journal.moveGroup)fail('backup_retention_move_ambiguous');await verifyGroup(record,group,from?source:target,{moved:to});
   }else{
    if(from)fail('backup_retention_inventory_changed');if(to){if(index<journal.deleteGroup)fail('backup_retention_inventory_changed');await verifyEntry(target,group.files.find(file=>file.path===''),{moved:true});}
    else if(journal.phase!=='deleting'||index>journal.deleteGroup||index===journal.deleteGroup&&journal.deleteFile!==group.files.filter(file=>file.type==='file').length)fail('backup_retention_quarantine_missing');
   }
   continue;
  }
  for(const [fileIndex,file] of group.files.entries()){
   const source=join(record.archiveDirectory,file.path),target=join(walQuarantine,file.path),from=await present(source),to=await present(target),deleted=journal.phase==='deleting'&&(index<journal.deleteGroup||index===journal.deleteGroup&&fileIndex<journal.deleteFile),intent=journal.phase==='deleting'&&index===journal.deleteGroup&&journal.unlinkIntent===group.id+':'+fileIndex;
   if(from&&to||deleted&&(from||to))fail('backup_retention_move_ambiguous');
   if(!from&&!to){if(!deleted&&!intent)fail('backup_retention_quarantine_missing');continue;}
   if(from&&(journal.phase!=='prepared'||index<journal.moveGroup)||to&&journal.phase==='prepared'&&index>journal.moveGroup)fail('backup_retention_move_ambiguous');await verifyEntry(from?source:target,file,{moved:to,uid:record.restoreUid});
  }
 }
}
async function authenticateResumedRetention(context,policy,plan,groups,journal,directory,{signal,clock}={}){
 const record=contextRecord(context),current=await verifyBackupCluster(context),now=clock();if(!Number.isSafeInteger(now)||now<plan.created)fail('backup_retention_clock_reversed');
 await verifyRetirementLayout(record,directory,plan.planId,groups,journal);
 const refs=await sourceReferences(record,policy),retiring=new Set(groups.filter(group=>group.type==='database').map(group=>group.source)),retained=[];
 for(const path of refs.paths){if(retiring.has(path))continue;signal?.throwIfAborted();retained.push(await authenticateGroup(record,join(record.artifactDirectory,path),path,{signal,now}));}
 const ids=new Set(groups.filter(group=>group.type==='database').map(group=>group.source.split('/').at(-1)));for(const group of retained){if(ids.has(group.id))fail('backup_retention_duplicate_backup_id');ids.add(group.id);}
 authenticateApplicationReferences(refs,retained);if(!Array.isArray(plan.retained))fail('backup_retention_plan_binding');
 for(const expected of plan.retained){
  const actual=retained.find(group=>group.path===expected.path&&group.id===expected.id);if(!actual||expected.application&&!actual.completeApplication)fail('backup_retention_retained_group_changed');
  for(const [field,items] of [['latestBackupPoint',actual.periodic.backupCompletePoints],['lastVerifiedPoint',actual.periodic.verifiedPoints]])if(expected[field]&&!items.some(point=>same(point,expected[field])))fail('backup_retention_retained_point_changed');
 }
 if(!retained.some(group=>group.metadata.timeline===current.timeline&&group.metadata.postmasterStart===current.postmaster_start))fail('backup_retention_current_base_required');
 const retirement={planId:plan.planId,groups,allowMoved:true},closure=await retainedWalClosure(record,current,retained,{signal,retirement});
 for(const group of groups)if(group.type==='wal'&&closure.protectedWal.has(group.source))fail('backup_retention_required_wal_changed');
 return {archiveIdentity:closure.archive.directoryIdentity,referenceDigest:refs.digest,retainedRoots:retained.map(group=>({path:group.path,identity:group.files.find(file=>file.path==='')}))};
}
async function syncRetiredDirectoryParent(record,directory,planId,group,path,journal,directories){
 const base=quarantinePath(record,directory,planId,group),boundary=dirname(base);let parent=dirname(path);
 for(;;){
  if(parent===boundary){if(!journal.databaseQuarantineIdentity)fail('backup_retention_journal_binding');await verifyEntry(parent,journal.databaseQuarantineIdentity,{moved:true});await syncDirectory(parent);return;}
  const expected=directories.get(parent);if(!expected)fail('backup_retention_unknown_directory_requires_pin');
  if(await present(parent)){await verifyEntry(parent,expected,{moved:true});await syncDirectory(parent);return;}parent=dirname(parent);
 }
}
async function removeQuarantinedGroup(record,directory,planId,group,journal,save,{signal,onTransition=()=>{}}={}){
 const base=quarantinePath(record,directory,planId,group),files=group.files.filter(f=>f.type==='file'),directories=new Map(group.files.filter(file=>file.type==='directory').map(file=>[join(base,file.path),file]));
 for(let index=journal.deleteFile;index<files.length;index++){
  signal?.throwIfAborted();const file=files[index],key=group.id+':'+index,path=join(base,file.path);
  if(journal.unlinkIntent!==key){if(!await present(path))fail('backup_retention_quarantine_missing');await verifyEntry(path,file,{moved:true,content:true,uid:group.type==='wal'?record.restoreUid:process.getuid()});journal.unlinkIntent=key;await save();}
  if(await present(path)){await verifyEntry(path,file,{moved:true,content:true,uid:group.type==='wal'?record.restoreUid:process.getuid()});await rm(path);await onTransition('quarantine_unlinked');}
  await syncDirectory(dirname(path));await onTransition('quarantine_unlink_synced');
  journal.deleteFile=index+1;journal.unlinkIntent=null;await save();
 }
 if(group.type==='database')for(const entry of group.files.filter(f=>f.type==='directory').sort((a,b)=>b.path.length-a.path.length)){
  signal?.throwIfAborted();const path=join(base,entry.path);if(await present(path)){await verifyEntry(path,entry,{moved:true});if((await readdir(path)).length)fail('backup_retention_quarantine_not_empty');await rmdir(path);await onTransition('quarantine_directory_removed');if(entry.path==='')await onTransition('database_group_root_removed');}
  await syncRetiredDirectoryParent(record,directory,planId,group,path,journal,directories);await onTransition('quarantine_directory_parent_synced');if(entry.path==='')await onTransition('database_group_parent_synced');
 }
}
async function applyRetentionInner(context,configuration,planId,{signal,clock=Date.now,onTransition=()=>{}}={}){
 const policy=postgresBackupRetentionPolicy(configuration);if(!policy.enabled)return output('retention_disabled');const record=contextRecord(context);
 const loaded=await readPlan(record,policy,planId);
 return withOperatorLocks(policy,()=>withBackupArchiveLock({directory:loaded.directory,exclusive:true,signal},async()=>{
  const {directory,plan,groups}=loaded;let journal,archiveIdentity,resumedInventory;
  try{journal=await readManifest(join(directory,'journal.enc'),record.key);}catch(error){if(error.code!=='ENOENT')throw error;}
  const resuming=!!journal;
  if(journal&&(journal.version!==1||journal.scope!==scope||journal.planDigest!==hash(plan)||journal.planId!==planId||!['prepared','quarantined','deleting','complete'].includes(journal.phase)||![journal.moveGroup,journal.deleteGroup,journal.deleteFile].every(n=>Number.isSafeInteger(n)&&n>=0)||journal.moveGroup>groups.length||journal.deleteGroup>groups.length))fail('backup_retention_journal_binding');
  if(journal?.phase==='complete')return output('retention_already_applied',{retentionApplied:true,planId,deleteDatabases:plan.deleteDatabases,deleteWalSegments:plan.deleteWalSegments});
  const save=async()=>{await writeManifest(join(directory,'journal.enc'),journal,record.key,{replace:true});};
  await unresolvedPlan(record,planId);
  if(journal)await withPostgresBackupOperation(record,async lease=>{await assertApplyReferences(context,record,policy,plan,lease);await recheckPolicy(record,policy);await recheckCandidateReferences(record,policy,groups);resumedInventory=await authenticateResumedRetention(context,policy,plan,groups,journal,directory,{signal,clock});archiveIdentity=resumedInventory.archiveIdentity;},{signal});
  if(!journal){
   await withPostgresBackupOperation(record,async lease=>{await assertApplyReferences(context,record,policy,plan,lease);const now=clock();if(now<plan.created)fail('backup_retention_clock_reversed');const current=await inspectInventory(context,policy,{signal,clock});archiveIdentity=current.archiveIdentity;if(current.refs.digest!==plan.referenceDigest)fail('backup_retention_plan_stale');const eligible=new Set([...current.removals.map(g=>'database:'+g.path),...current.wal.map(w=>'wal:'+w.name)]);for(const group of groups){if(!eligible.has(group.type+':'+group.source))fail('backup_retention_plan_stale');await verifyGroup(record,group,group.type==='database'?sourcePath(record,group):record.archiveDirectory,{content:true});}},{signal});await onTransition('inventory_authenticated');
   journal={version:1,scope,planId,planDigest:hash(plan),phase:'prepared',moveGroup:0,deleteGroup:0,deleteFile:0,unlinkIntent:null,walQuarantineIdentity:plan.walQuarantineIdentity,archiveDirectoryIdentity:null,databaseQuarantineIdentity:null};
  }
  // No ciphertext scans in the exclusive section: recheck authenticated small
  // references and previously witnessed identities, then retire whole groups.
  if(journal.phase==='prepared'){
   let witness;try{
   await withPostgresBackupOperation(record,async lease=>{witness=(await assertApplyReferences(context,record,policy,plan,lease,{retainWitness:true,archiveIdentity,retirement:{planId,groups,allowMoved:resuming}})).coordinationWitness;},{signal});await onTransition('coordination_witnessed');
   await withBackupArchiveLock({directory:record.archiveDirectory,uid:record.restoreUid,exclusive:true,signal},async lease=>{
   const references=await assertApplyReferences(context,record,policy,plan,lease,{witness});if(groups.some(group=>group.type==='wal'&&group.source===references.receiptWalName))fail('backup_retention_receipt_now_referenced');await recheckPolicy(record,policy);await unresolvedPlan(record,planId);await recheckCandidateReferences(record,policy,groups);
   if(resumedInventory){if((await sourceReferences(record,policy)).digest!==resumedInventory.referenceDigest)fail('backup_retention_plan_stale');for(const root of resumedInventory.retainedRoots)await verifyEntry(join(record.artifactDirectory,root.path),root.identity);}
   if(!resuming){if((await sourceReferences(record,policy)).digest!==plan.referenceDigest)fail('backup_retention_plan_stale');for(const group of groups)await verifyGroup(record,group,group.type==='database'?sourcePath(record,group):record.archiveDirectory);}
   if(journal.archiveDirectoryIdentity)await verifyEntry(record.archiveDirectory,journal.archiveDirectoryIdentity,{moved:true,uid:record.restoreUid});else journal.archiveDirectoryIdentity=await statEntry(record.archiveDirectory,record.restoreUid);
   await save();await onTransition('prepared');
   const databaseQuarantine=join(directory,'quarantine');if(journal.databaseQuarantineIdentity){await verifyEntry(databaseQuarantine,journal.databaseQuarantineIdentity,{moved:true});}else{if(journal.moveGroup!==0)fail('backup_retention_journal_binding');await mkdir(databaseQuarantine,{mode:0o700}).catch(e=>{if(e.code!=='EEXIST')throw e;});await privateDirectory(databaseQuarantine);if((await readdir(databaseQuarantine)).length)fail('backup_retention_allocation_not_empty');await syncDirectory(directory);journal.databaseQuarantineIdentity=await statEntry(databaseQuarantine);await save();}await syncDirectory(directory);await onTransition('quarantine_parent_synced');
   const walQuarantine=join(record.archiveDirectory,'.retention-'+planId);await verifyEntry(walQuarantine,journal.walQuarantineIdentity,{moved:true});
   for(let index=journal.moveGroup;index<groups.length;index++){await quarantineGroup(record,directory,planId,groups[index],lease,onTransition);journal.moveGroup=index+1;await save();await onTransition('group_quarantined');}
   journal.phase='quarantined';await save();await onTransition('quarantined');
  });
   }finally{if(witness)await closeCoordinationWitness(witness);}
  }
  if(['quarantined','deleting'].includes(journal.phase))await withPostgresBackupOperation(record,async lease=>{await assertApplyReferences(context,record,policy,plan,lease);await recheckPolicy(record,policy);await unresolvedPlan(record,planId);await recheckCandidateReferences(record,policy,groups);},{signal});
  if(journal.phase==='quarantined'){for(const group of groups)await verifyGroup(record,group,quarantinePath(record,directory,planId,group),{moved:true,content:true});journal.phase='deleting';await save();await onTransition('deleting');}
  // Deletion touches only journal-owned quarantine. The live archiver and
  // ordinary backups can proceed while ciphertext is rechecked and unlinked.
  if(journal.phase==='deleting'){
   for(let index=journal.deleteGroup;index<groups.length;index++){await verifyEntry(join(record.archiveDirectory,'.retention-'+planId),journal.walQuarantineIdentity,{moved:true});await removeQuarantinedGroup(record,directory,planId,groups[index],journal,save,{signal,onTransition});journal.deleteGroup=index+1;journal.deleteFile=0;await save();await onTransition('group_deleted');}
   const walQuarantine=join(record.archiveDirectory,'.retention-'+planId);if(await present(walQuarantine)){await verifyEntry(walQuarantine,journal.walQuarantineIdentity,{moved:true});if((await readdir(walQuarantine)).length)fail('backup_retention_quarantine_not_empty');await rmdir(walQuarantine);await onTransition('wal_quarantine_removed');}
   if(!journal.archiveDirectoryIdentity)fail('backup_retention_journal_binding');await verifyEntry(record.archiveDirectory,journal.archiveDirectoryIdentity,{moved:true,uid:record.restoreUid});await syncDirectory(record.archiveDirectory);await onTransition('wal_quarantine_parent_synced');
   journal.phase='complete';await save();
  }
  return output('retention_applied',{retentionApplied:true,planId,deleteDatabases:plan.deleteDatabases,deleteWalSegments:plan.deleteWalSegments,bytes:plan.bytes,retainedDatabaseGroups:plan.retained.length,retainedApplicationGroups:plan.retained.filter(g=>g.application).length});
 }));
}
