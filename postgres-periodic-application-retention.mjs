import {periodicApplicationKey,verifyPeriodicApplicationKeyBinding} from './postgres-periodic-application-key.mjs';
import {readdir,lstat} from 'node:fs/promises';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {authenticatePostgresBackupFileSet} from './postgres-backup.mjs';
import {readWalCheckpointSegments} from './postgres-backup-wal-catalog.mjs';
import {readManifest,digestFile,canonical,privateDirectory,transferError} from './migration-artifacts.mjs';
import {periodicApplicationPolicy} from './postgres-periodic-application-policy.mjs';

const fail=code=>{throw transferError(code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const time=value=>Number.isSafeInteger(value)&&value>=0;
const digest=value=>value&&/^[a-f0-9]{64}$/.test(value.sha256??'')&&Number.isSafeInteger(value.bytes)&&value.bytes>=36;
const same=(a,b)=>canonical(a)===canonical(b);
async function names(directory){try{await privateDirectory(directory);return (await readdir(directory)).sort();}catch(error){if(error.code==='ENOENT')return null;throw error;}}
const bound=(value,metadata)=>[1,3].includes(value?.version)&&value.backupId===metadata.backupId&&value.configurationDigest===metadata.configurationDigest&&value.systemId===metadata.systemId&&value.keyVersion===metadata.keyVersion;
const applicationBound=(value,metadata,record)=>value?.version===3&&bound(value,metadata)&&value.applicationKeyVersion===record.applicationKeyVersion&&value.applicationKeyDigest===record.applicationKeyDigest;
const lsn=value=>{if(typeof value!=='string'||!/^[A-F0-9]+\/[A-F0-9]+$/.test(value))fail('periodic_retention_wal_fence');const [a,b]=value.split('/');return (BigInt('0x'+a)<<32n)+BigInt('0x'+b);};

export async function readPeriodicApplicationRetentionState(record,{signal}={}){
  const path=join(record.artifactDirectory,'periodic-application-state.enc');let state;
  try{await lstat(path);}catch(error){if(error.code==='ENOENT')return null;throw error;}
  state=await readManifest(path,periodicApplicationKey(record),{signal});verifyPeriodicApplicationKeyBinding(state,record);
  const fields=['latestBackup','lastVerified','pending','pendingVerification'];
  if(state.version!==3||state.scope!=='harbor-periodic-application-state-v3'||state.configurationDigest!==record.configurationDigest||state.systemId!==record.expected.system_id||state.keyVersion!==record.keyVersion||!time(state.updatedAt)||Object.keys(state).some(name=>!['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest',...fields,'updatedAt'].includes(name)))fail('periodic_retention_state_binding');
  for(const name of fields){
    const row=state[name];if(row===null)continue;
    const timestamp=name==='lastVerified'?'verifiedAt':['latest','latestBackup'].includes(name)?'completedAt':null;
    if(!row||!uuid(row.backupId)||!uuid(row.pointId)||Object.keys(row).some(key=>!(timestamp?['backupId','pointId',timestamp,'checkpointAt']:['backupId','pointId']).includes(key))||timestamp&&(!time(row[timestamp])||!time(row.checkpointAt)||row[timestamp]<row.checkpointAt))fail('periodic_retention_state_binding');
  }
  return {state,digest:await digestFile(path,{signal}),protectedBackupIds:[...new Set(fields.map(field=>state[field]?.backupId).filter(Boolean))]};
}

// Authentication only, using each historical base's own immutable metadata.
// This returns a deletion/retention inventory, never a restore capability.
export async function authenticatePeriodicApplicationRetentionGroup(record,directory,metadata,{signal,now=Date.now()}={}){
  const allowed=new Set(),segments=[],captures=new Map(),reservations=new Map(),points=new Map(),completeApplications=[],backupCompletePoints=[],verifiedPoints=[];let incomplete=false;
  const inspectDirectory=async name=>{const result=await names(join(directory,name));if(result!==null)allowed.add(name+'/');return result??[];};
  for(const name of await inspectDirectory('application-cycles')){
    signal?.throwIfAborted();const id=name.slice(0,-4);if(!name.endsWith('.enc')||!uuid(id)||reservations.size>=100000)fail('periodic_retention_reservation_name');
    const row=await readManifest(join(directory,'application-cycles',name),periodicApplicationKey(record),{signal});
    if(!applicationBound(row,metadata,record)||row.scope!=='harbor-periodic-application-cycle-v3'||row.pointId!==id||!['reserved','captured','checkpointed','backup_complete','failed'].includes(row.stage)||!time(row.reservedAt)||!time(row.updatedAt)||row.updatedAt<row.reservedAt||row.updatedAt>now||!/^[a-f0-9]{64}$/.test(row.applicationPolicyDigest??'')||row.failureCode!==undefined&&!/^[a-z][a-z0-9_]{0,95}$/.test(row.failureCode)||Object.keys(row).some(key=>!['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','applicationPolicyDigest','backupId','pointId','stage','reservedAt','updatedAt','failureCode'].includes(key)))fail('periodic_retention_reservation_binding');
    allowed.add('application-cycles/'+name);reservations.set(id,row);if(!['complete','backup_complete','failed'].includes(row.stage))incomplete=true;
  }
  for(const id of await inspectDirectory('application-staging')){
    signal?.throwIfAborted();if(!uuid(id)||captures.size>=100000)fail('periodic_retention_capture_name');const relative='application-staging/'+id,path=join(directory,relative),children=await names(path);
    if(children===null)fail('periodic_retention_capture_missing');allowed.add(relative+'/');
    if(!children.includes('capture.enc')){if(children.length||!reservations.has(id))fail('periodic_retention_capture_incomplete_unverifiable');if(reservations.get(id).stage!=='failed')incomplete=true;continue;}
    const capture=await readManifest(join(path,'capture.enc'),periodicApplicationKey(record),{signal}),fence=capture.walFence;
    if(Object.keys(capture).some(key=>!['version','scope','captureId','backupId','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','applicationPolicy','applicationPolicyDigest','files','walFence','capturedAt'].includes(key))||!applicationBound(capture,metadata,record)||capture.scope!=='harbor-periodic-application-capture-v3'||capture.captureId!==id||!/^[a-f0-9]{64}$/.test(capture.applicationPolicyDigest??'')||!time(capture.capturedAt)||capture.capturedAt>now||typeof fence?.postmasterStart!=='string'||!/^\d+(?:\.\d+)?$/.test(fence.postmasterStart)||fence.timeline!==metadata.timeline||!Number.isSafeInteger(capture.files?.files)||capture.files.files<0||capture.files.files>1000000||!Number.isSafeInteger(capture.files?.bytes)||capture.files.bytes<0||capture.files.bytes>record.profile.maxBackupBytes-metadata.files.bytes||reservations.has(id)&&reservations.get(id).applicationPolicyDigest!==capture.applicationPolicyDigest)fail('periodic_retention_capture_binding');
    if(capture.version===3&&hash(periodicApplicationPolicy(capture.applicationPolicy))!==capture.applicationPolicyDigest)fail('periodic_retention_capture_policy');
    lsn(fence.walInsertLsn);
    const files=await readManifest(join(path,'files.enc'),periodicApplicationKey(record),{signal});if(!Array.isArray(files.roots)||files.roots.length!==3||files.roots.map(row=>row.id).sort().join(',')!=='data,runtime,secrets')fail('periodic_retention_capture_roots');
    for(const file of await authenticatePostgresBackupFileSet({directory:path,key:periodicApplicationKey(record),files:capture.files,signal}))allowed.add(relative+'/'+file);
    allowed.add(relative+'/capture.enc');captures.set(id,capture);
  }
  for(const id of await inspectDirectory('points')){
    signal?.throwIfAborted();if(!uuid(id)||points.size>=100000)fail('periodic_retention_point_name');const relative='points/'+id,path=join(directory,relative),children=await names(path);
    if(children===null)fail('periodic_retention_point_missing');allowed.add(relative+'/');
    if(!children.includes('point.enc')){if(children.length||!reservations.has(id))fail('periodic_retention_point_incomplete_unverifiable');if(reservations.get(id).stage!=='failed')incomplete=true;continue;}
    const point=await readManifest(join(path,'point.enc'),record.key,{signal}),checkpoint=await readManifest(join(path,'checkpoint.enc'),record.key,{signal}),checkpointDigest=await digestFile(join(path,'checkpoint.enc'),{signal});
    if(point.version!==1||!bound(point,metadata)||point.scope!=='harbor-backup-recovery-point-v1'||point.pointId!==id||!time(point.created)||point.created>now||!digest(point.sourceCheckpointDigest)||!same(point.baseManifestDigest,await digestFile(join(directory,'backup.enc'),{signal}))||!same(point.checkpointDigest,checkpointDigest)||checkpoint.version!==2||checkpoint.backupId!==metadata.backupId||checkpoint.configurationDigest!==metadata.configurationDigest||checkpoint.systemId!==metadata.systemId||checkpoint.keyVersion!==metadata.keyVersion||checkpoint.timeline!==metadata.timeline||checkpoint.segmentBytes!==metadata.segmentBytes||!new RegExp('^harbor_backup_'+metadata.backupId.replaceAll('-','')+'_[a-f0-9]{12}$').test(checkpoint.restorePoint))fail('periodic_retention_point_binding');
    const capture=captures.get(id),reference=point.applicationCapture;
    if(reference!==undefined){
      if(!capture||reference.captureId!==id||reference.manifestHash!==hash(capture)||!same(reference.walFence,capture.walFence)||capture.walFence.postmasterStart!==(checkpoint.sourcePostmasterStart??metadata.postmasterStart)||lsn(checkpoint.lsn)<=lsn(capture.walFence.walInsertLsn))fail('periodic_retention_point_capture_binding');
    }
    let count=0;for await(const row of readWalCheckpointSegments({directory:path,key:record.key,checkpoint,signal})){if(++count>record.profile.maxWalSegments||row.content.bytes!==Number(metadata.segmentBytes))fail('periodic_retention_wal_capacity');segments.push(row);}
    allowed.add(relative+'/point.enc');allowed.add(relative+'/checkpoint.enc');for(const part of checkpoint.walCatalog.parts)allowed.add(relative+'/'+part.name);
    let backup=null;if(children.includes('backup-complete.enc')){
      backup=await readManifest(join(path,'backup-complete.enc'),periodicApplicationKey(record),{signal});
      if(Object.keys(backup).some(key=>!['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','backupId','pointId','applicationPolicyDigest','captureManifestHash','pointManifestHash','checkpointDigest','checkpointAt','completedAt','baseVerifiedAt','backupComplete','restoreVerified','deploymentAllowed','walIntegrity'].includes(key))||!reference||!capture||capture.version!==3||!applicationBound(backup,metadata,record)||backup.scope!=='harbor-periodic-application-backup-v3'||backup.pointId!==id||backup.applicationPolicyDigest!==capture.applicationPolicyDigest||backup.captureManifestHash!==hash(capture)||backup.pointManifestHash!==hash(point)||!same(backup.checkpointDigest,checkpointDigest)||backup.checkpointAt!==checkpoint.created||!time(backup.completedAt)||backup.completedAt<backup.checkpointAt||backup.completedAt>now||!time(backup.baseVerifiedAt)||backup.baseVerifiedAt>backup.completedAt||backup.backupComplete!==true||backup.restoreVerified!==false||backup.deploymentAllowed!==false)fail('periodic_retention_backup_complete_binding');
      allowed.add(relative+'/backup-complete.enc');backupCompletePoints.push({pointId:id,checkpointAt:backup.checkpointAt,completedAt:backup.completedAt});
    }
    let restored=null;if(children.includes('restore.enc')){
      restored=await readManifest(join(path,'restore.enc'),record.key,{signal});
      if(restored.version!==1||restored.backupId!==metadata.backupId||restored.recoveryPointId!==id||restored.configurationDigest!==metadata.configurationDigest||!same(restored.checkpointDigest,checkpointDigest)||restored.systemIdVerified!==true||restored.restoreVerified!==true)fail('periodic_retention_restore_binding');allowed.add(relative+'/restore.enc');
    }
    let sealed=null;if(children.includes('application-restore.enc')){
      sealed=await readManifest(join(path,'application-restore.enc'),periodicApplicationKey(record),{signal});verifyPeriodicApplicationKeyBinding(sealed,record);verifyPeriodicApplicationKeyBinding(sealed.application,record);
      if(sealed.version!==1||sealed.scope!==metadata.scope||sealed.backupId!==metadata.backupId||sealed.recoveryPointId!==id||sealed.configurationDigest!==metadata.configurationDigest||!same(sealed.checkpointDigest,checkpointDigest)||sealed.systemIdVerified!==true||sealed.restoreVerified!==true||sealed.recoveryExitedVerified!==true||sealed.writeReadbackVerified!==true||!time(sealed.periodicRestoreStartedAt)||!time(sealed.created)||sealed.periodicRestoreStartedAt<checkpoint.created||sealed.created<sealed.periodicRestoreStartedAt)fail('periodic_retention_application_restore_binding');
      allowed.add(relative+'/application-restore.enc');
    }
    if(children.includes('application-final.enc')){
      restored=sealed;
      const final=await readManifest(join(path,'application-final.enc'),periodicApplicationKey(record),{signal}),application=restored?.application,runtime=application?.runtime,modern=final.version===3,verifiedAt=final.verifiedAt;
      if(modern&&Object.keys(final).some(key=>!['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','backupId','pointId','applicationPolicyDigest','captureManifestHash','pointManifestHash','backupCompleteDigest','restoreReport','restoreDigest','checkpointAt','startedAt','verifiedAt','restoreDurationMs','restoreVerified','applicationMaterialsVerified','candidateVerified','offhostVerified','deploymentAllowed'].includes(key))||!reference||!capture||!restored||!applicationBound(final,metadata,record)||final.scope!=='harbor-periodic-application-final-v3'||modern&&(!backup||final.restoreVerified!==true||!same(final.backupCompleteDigest,await digestFile(join(path,'backup-complete.enc'),{signal})))||final.pointId!==id||final.applicationPolicyDigest!==capture.applicationPolicyDigest||final.captureManifestHash!==hash(capture)||final.pointManifestHash!==hash(point)||final.restoreReport!=='application-restore.enc'||!same(final.restoreDigest,await digestFile(join(path,final.restoreReport),{signal}))||!['checkpointAt','startedAt','restoreDurationMs'].every(field=>time(final[field]))||!time(verifiedAt)||final.checkpointAt!==checkpoint.created||final.startedAt!==restored.periodicRestoreStartedAt||verifiedAt!==restored.created||final.startedAt<final.checkpointAt||verifiedAt<final.startedAt||verifiedAt>now||final.restoreDurationMs!==verifiedAt-final.startedAt||final.applicationMaterialsVerified!==true||final.candidateVerified!==false||final.offhostVerified!==false||final.deploymentAllowed!==false||application.captureId!==id||application.applicationMaterialsVerified!==true||runtime?.scope!==(metadata.scope==='existing-cluster'?'installed-restored-periodic-source-runtime':'task-restored-periodic-source-runtime')||!['gatewayStartedVerified','independentWorkerStartedVerified','workerProgressVerified','originalKeyDatabaseVerified','restoredTlsServerVerified','freshRestoredDatabaseReportVerified','upstreamCallsDisabled'].every(key=>runtime[key]===true)||!['applicationProcessesExitedVerified','applicationDrainVerified','publisherStoppedVerified','temporaryLoginRevokedVerified'].every(key=>runtime.cleanup?.[key]===true))fail('periodic_retention_final_binding');
      if(metadata.scope==='existing-cluster'&&(runtime.crossUidExecutionVerified!==true||runtime.publisherUidVerified!==true||!Number.isSafeInteger(runtime.restoreUid)||runtime.restoreUid<1))fail('periodic_retention_final_uid_binding');
      if(application.filesRestoredVerified!==true||application.originalStoredCredentialsVerified!==true||application.accountHomeMapping?.verified!==true||application.recordingVerification?.allVerified!==true||!['existingDeviceTrustVerified','serverTlsIdentityVerified','reportKeyRestoredVerified'].every(key=>application.deviceVerification?.[key]===true))fail('periodic_retention_final_application_incomplete');
      allowed.add(relative+'/application-final.enc');completeApplications.push({pointId:id,checkpointAt:final.checkpointAt,completedAt:verifiedAt});verifiedPoints.push({pointId:id,checkpointAt:final.checkpointAt,verifiedAt});
    }else if(reference!==undefined&&!backup&&reservations.get(id)?.stage!=='failed')incomplete=true;
    if(children.includes('handoff')){
      const bundle=join(path,'handoff'),handoff=await readManifest(join(bundle,'handoff.enc'),periodicApplicationKey(record),{signal});
      if(handoff.applicationKeyVersion!==record.applicationKeyVersion||handoff.applicationKeyDigest!==record.applicationKeyDigest||!backup||handoff.version!==1||handoff.kind!=='periodic-application'||handoff.backupId!==metadata.backupId||handoff.pointId!==id||handoff.scope!==metadata.scope||handoff.configurationDigest!==metadata.configurationDigest||handoff.keyVersion!==metadata.keyVersion||handoff.expected?.system_id!==metadata.systemId||!same(handoff.checkpointDigest,checkpointDigest)||!same(handoff.backupCompleteDigest,await digestFile(join(path,'backup-complete.enc'),{signal}))||handoff.captureManifestHash!==hash(capture)||handoff.pointManifestHash!==hash(point)||handoff.applicationPolicyDigest!==capture.applicationPolicyDigest)fail('periodic_retention_handoff_binding');
      const {handoffFiles}=await import('./postgres-backup-handoff.mjs'),maxBytes=record.profile.maxBackupBytes*2+record.profile.maxWalSegments*Number(metadata.segmentBytes)*2;
      if(!Number.isSafeInteger(maxBytes))fail('periodic_retention_handoff_capacity');
      for await(const entry of handoffFiles({directory:bundle,key:periodicApplicationKey(record),manifest:handoff,signal,maxFiles:100000,maxBytes}))allowed.add(relative+'/handoff/'+entry.path);
      allowed.add(relative+'/handoff/handoff.enc');for(const part of handoff.parts)allowed.add(relative+'/handoff/'+part.name);
    }
    const receiverNames=['periodic-receiver.enc','periodic-receiver-diagnostic.enc'].filter(name=>children.includes(name));
    if(receiverNames.length){
      if(!backup||!children.includes('handoff'))fail('periodic_retention_receiver_binding');
      const {validatePeriodicReceiverVerificationRecord}=await import('./postgres-periodic-receiver-verification.mjs'),expected={backupId:metadata.backupId,pointId:id,configurationDigest:metadata.configurationDigest,clusterDigest:hash(metadata.systemId),keyVersion:metadata.keyVersion,checkpointAt:checkpoint.created,handoffDigest:await digestFile(join(path,'handoff','handoff.enc'),{signal}),checkpointDigest,backupCompleteDigest:await digestFile(join(path,'backup-complete.enc'),{signal}),applicationPolicyDigest:capture.applicationPolicyDigest,applicationKeyVersion:record.applicationKeyVersion,applicationKeyDigest:record.applicationKeyDigest};
      for(const name of receiverNames){
        const value=validatePeriodicReceiverVerificationRecord(await readManifest(join(path,name),periodicApplicationKey(record),{signal}),expected);
        if((name==='periodic-receiver.enc')!==(value.scope==='approved-periodic-offhost-recovery')||value.scope==='approved-periodic-offhost-recovery'&&metadata.scope!=='existing-cluster')fail('periodic_retention_receiver_binding');
        if(children.includes('receiver-attestation.json')&&!same(await digestFile(join(path,'receiver-attestation.json'),{signal}),value.attestationDigest))fail('periodic_retention_receiver_input_changed');allowed.add(relative+'/'+name);
      }
      if(children.includes('receiver-attestation.json'))allowed.add(relative+'/receiver-attestation.json');
    }else if(children.includes('receiver-attestation.json'))fail('periodic_retention_receiver_input_pending');
    points.set(id,point);
  }
  for(const [id,row] of reservations){
    if(row.stage==='complete'&&!completeApplications.some(item=>item.pointId===id))fail('periodic_retention_completed_point_missing');
    if(row.stage==='backup_complete'&&!backupCompletePoints.some(item=>item.pointId===id))fail('periodic_retention_backup_complete_missing');
    if(!['reserved','failed'].includes(row.stage)&&!captures.has(id))fail('periodic_retention_reserved_capture_missing');
  }
  for(const [id] of captures)if(reservations.get(id)?.stage!=='failed'&&!completeApplications.some(item=>item.pointId===id)&&!backupCompletePoints.some(item=>item.pointId===id))incomplete=true;
  // No unrecognised child is permitted merely because its parent was signed.
  // The caller's existing immutable tree walk checks this explicit allowlist.
  return {allowed,segments,completeApplications,backupCompletePoints,verifiedPoints,incomplete,referenceDigest:hash({reservations:[...reservations],captures:[...captures],points:[...points],backupCompletePoints,verifiedPoints})};
}

// The exclusive retirement phase rechecks only authenticated control roots.
// Ciphertext/catalog bodies are validated by the shared-lock group traversal.
export async function periodicApplicationRetentionReferences(record,directory,metadata,{signal}={}){
  const files=[],links=[],layout=[];
  async function add(name){
    signal?.throwIfAborted();let value;try{await lstat(join(directory,name));}catch(error){if(error.code==='ENOENT')return;throw error;}
    const application=name.startsWith('application-cycles/')||name.startsWith('application-staging/')||name.includes('/handoff/')||/(?:^|\/)(?:backup-complete|application-final|application-restore|periodic-receiver(?:-diagnostic)?)\.enc$/.test(name);value=await readManifest(join(directory,name),application?periodicApplicationKey(record):record.key,{signal});
    files.push({name,digest:await digestFile(join(directory,name),{signal})});
    if(name.endsWith('/files.enc'))links.push({name,catalog:value.catalog});
    if(name.endsWith('/checkpoint.enc'))links.push({name,walCatalog:value.walCatalog});
  }
  const cycleNames=await names(join(directory,'application-cycles'));
  layout.push({name:'application-cycles',children:cycleNames});
  for(const name of cycleNames??[]){if(!name.endsWith('.enc')||!uuid(name.slice(0,-4)))fail('periodic_retention_reservation_name');await add('application-cycles/'+name);}
  const stagingNames=await names(join(directory,'application-staging'));layout.push({name:'application-staging',children:stagingNames});
  for(const id of stagingNames??[]){if(!uuid(id))fail('periodic_retention_capture_name');layout.push({name:'application-staging/'+id,children:await names(join(directory,'application-staging',id))});await add('application-staging/'+id+'/capture.enc');await add('application-staging/'+id+'/files.enc');}
  const pointNames=await names(join(directory,'points'));layout.push({name:'points',children:pointNames});
  for(const id of pointNames??[]){
    if(!uuid(id))fail('periodic_retention_point_name');
    layout.push({name:'points/'+id,children:await names(join(directory,'points',id))});
    for(const name of ['point.enc','checkpoint.enc','backup-complete.enc','application-final.enc','application-restore.enc','restore.enc','periodic-receiver.enc','periodic-receiver-diagnostic.enc'])await add('points/'+id+'/'+name);
    const receiverInput='points/'+id+'/receiver-attestation.json';try{await lstat(join(directory,receiverInput));files.push({name:receiverInput,digest:await digestFile(join(directory,receiverInput),{signal})});}catch(error){if(error.code!=='ENOENT')throw error;}
    const bundle='points/'+id+'/handoff',bundleNames=await names(join(directory,bundle));layout.push({name:bundle,children:bundleNames});
    if(bundleNames!==null){await add(bundle+'/handoff.enc');const manifest=await readManifest(join(directory,bundle,'handoff.enc'),periodicApplicationKey(record),{signal});for(const [ordinal,part] of (manifest.parts??[]).entries()){if(part.name!=='handoff-catalog-'+ordinal+'.enc')fail('periodic_retention_handoff_binding');await add(bundle+'/'+part.name);}}
  }
  if(files.length>100000||layout.reduce((count,row)=>count+(row.children?.length??0),0)>100000)fail('periodic_retention_reference_capacity');
  return {files,referenceDigest:hash({backupId:metadata.backupId,files,links,layout})};
}
