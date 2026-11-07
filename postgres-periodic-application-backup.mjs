import {revalidatePeriodicSourceBaseIfPresent} from './postgres-backup.mjs';
import {periodicApplicationKey,periodicApplicationKeyBinding,verifyPeriodicApplicationKeyBinding} from './postgres-periodic-application-key.mjs';
import {mkdir,lstat,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {randomUUID,createHash} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {acquireInstance} from './instance.mjs';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource,checkpointPeriodicPostgresBackup} from './postgres-backup.mjs';
import {withPostgresBackupOperation} from './postgres-backup-lock.mjs';
import {periodicApplicationPolicy,installedPeriodicApplicationPolicy,verifyInstalledPeriodicApplicationPolicy} from './postgres-periodic-application-policy.mjs';
import {capturePeriodicApplicationMaterials,verifyPeriodicApplicationCapture,authenticatePeriodicApplicationCapture,periodicApplicationCaptureSource} from './postgres-periodic-application-materials.mjs';
import {capturePostgresBackupRecoveryPoint,verifyPostgresBackupRecoveryPoint,postgresBackupRecoveryPointSource} from './postgres-backup-recovery-point.mjs';
import {prepareArchivedPeriodicApplicationRestore} from './postgres-periodic-application-restore.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from './postgres-backup-restore.mjs';
import {readManifest,writeManifest,digestFile,canonical,privateDirectory,syncDirectory,transferError} from './migration-artifacts.mjs';

const backupScope='harbor-periodic-application-backup-v3',stateScope='harbor-periodic-application-state-v3',cycleScope='harbor-periodic-application-cycle-v3',finalScope='harbor-periodic-application-final-v3';
const backups=new WeakMap(),fail=code=>{throw transferError(code);};
const hash=value=>createHash('sha256').update(canonical(value)).digest('hex'),same=(a,b)=>canonical(a)===canonical(b);
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value),time=value=>Number.isSafeInteger(value)&&value>=0;
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>keys.includes(key));
const common=record=>({version:3,...periodicApplicationKeyBinding(record),configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion});
const bound=(value,record)=>value?.version===3&&value.applicationKeyVersion===record.applicationKeyVersion&&value.applicationKeyDigest===record.applicationKeyDigest&&value.configurationDigest===record.configurationDigest&&value.systemId===record.expected.system_id&&value.keyVersion===record.keyVersion;
const result=(state,extra={})=>({state,scope:'periodic-application',backupComplete:false,restoreVerified:false,applicationMaterialsVerified:false,offhostVerified:false,deploymentAllowed:false,...extra});
const cleanupUnknown=error=>error instanceof AggregateError||typeof error?.code==='string'&&error.code.endsWith('_cleanup_unknown');
async function exists(path){try{await lstat(path);return true;}catch(error){if(error.code==='ENOENT')return false;throw error;}}
function contextFor(context,proof,{archived=false}={}){
  const record=postgresBackupContext(context),source=postgresBackupRestoreSource(proof);periodicApplicationKey(record);
  if(record.closed||source.context!==context||record.pendingBinding||record.receiverBinding&&(!archived||record.periodicReceiverBinding!==record.receiverBinding)||record.resumeBinding||record.applicationPlan||record.profile.rootProtectedRuntime)fail('periodic_application_backup_context');
  if(record.scope!=='existing-cluster'&&(record.scope!=='task-owned'||record.restoreUid!==process.getuid()))fail('periodic_application_backup_context');
  return {record,source,base:join(record.artifactDirectory,proof.backupId)};
}
function validateState(state,record){
  if(!exact(state,['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','latestBackup','lastVerified','pending','pendingVerification','updatedAt'])||!bound(state,record)||state.scope!==stateScope||!time(state.updatedAt))fail('periodic_application_state_binding');
  for(const name of ['latestBackup','lastVerified','pending','pendingVerification']){
    const row=state[name],at=name==='latestBackup'?'completedAt':name==='lastVerified'?'verifiedAt':null;
    if(row===null)continue;
    if(!exact(row,at?['backupId','pointId','checkpointAt',at]:['backupId','pointId'])||!uuid(row?.backupId)||!uuid(row?.pointId)||at&&(!time(row[at])||!time(row.checkpointAt)||row[at]<row.checkpointAt))fail('periodic_application_state_binding');
  }
  return state;
}
export async function readPeriodicApplicationBackupState(context,{signal}={}){
  const record=postgresBackupContext(context);signal?.throwIfAborted();
  try{return validateState(await readManifest(join(record.artifactDirectory,'periodic-application-state.enc'),periodicApplicationKey(record),{signal}),record);}
  catch(error){if(error.code!=='ENOENT')throw error;return {...common(record),scope:stateScope,latestBackup:null,lastVerified:null,pending:null,pendingVerification:null,updatedAt:Date.now()};}
}
async function stateChange(context,change,{signal}={}){
  const record=postgresBackupContext(context),deadline=Date.now()+Math.min(record.profile.processTimeoutMs,30000);let lock;
  while(!lock){signal?.throwIfAborted();try{lock=await acquireInstance(join(record.artifactDirectory,'.periodic-application-control'));}catch(error){if(error.code!=='EADDRINUSE')throw error;if(Date.now()>=deadline)fail('periodic_application_control_busy');await delay(10,undefined,{signal});}}
  try{const state=await readPeriodicApplicationBackupState(context,{signal}),value=await change(state);state.updatedAt=Date.now();validateState(state,record);await writeManifest(join(record.artifactDirectory,'periodic-application-state.enc'),state,periodicApplicationKey(record),{replace:true});return value;}
  finally{await lock.release();}
}
function validCycle(value,record,backupId,pointId){
  if(!exact(value,['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','applicationPolicyDigest','backupId','pointId','stage','reservedAt','updatedAt','failureCode'])||!bound(value,record)||value.scope!==cycleScope||value.backupId!==backupId||value.pointId!==pointId||!uuid(pointId)||!/^[a-f0-9]{64}$/.test(value.applicationPolicyDigest??'')||!['reserved','captured','checkpointed','backup_complete','failed'].includes(value.stage)||!time(value.reservedAt)||!time(value.updatedAt)||value.updatedAt<value.reservedAt||value.failureCode!==undefined&&!/^[a-z][a-z0-9_]{0,95}$/.test(value.failureCode))fail('periodic_application_cycle_binding');return value;
}
export async function inspectPeriodicApplicationBackupReservations(context,proof,{signal}={}){
  const {record,base}=contextFor(context,proof),path=join(base,'application-cycles');if(!await exists(path))return [];
  await privateDirectory(path);const rows=[];
  for(const name of await readdir(path)){if(!name.endsWith('.enc')||!uuid(name.slice(0,-4)))fail('periodic_application_unknown_reservation');rows.push(validCycle(await readManifest(join(path,name),periodicApplicationKey(record),{signal}),record,proof.backupId,name.slice(0,-4)));}return rows;
}
async function operate(context,proof,kind,callback,{signal,archived=false}={}){
  const {record}=contextFor(context,proof,{archived});if(record.periodicApplicationPending||record.busy)fail('periodic_application_cycle_busy');
  const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));record.periodicApplicationCancellation=cancellation;
  const pending=withPostgresBackupOperation(record,async()=>{let lock;try{lock=await acquireInstance(join(record.artifactDirectory,'.periodic-application-'+kind));}catch(error){if(error.code==='EADDRINUSE')fail('periodic_application_cycle_busy');throw error;}try{return await callback(combined);}finally{await lock.release();}},{signal:combined});record.periodicApplicationPending=pending;
  try{return await pending;}finally{record.periodicApplicationPending=null;record.periodicApplicationCancellation=null;}
}

// This capability authenticates the durable, source-independent recovery point.
// A plain JSON manifest cannot grant an archived restore or installed runtime.
export async function authenticatePeriodicApplicationBackup(context,proof,pointId,{signal}={}){
  const {record,source}=contextFor(context,proof,{archived:true}),point=await verifyPostgresBackupRecoveryPoint(context,proof,pointId,{signal}),selected=postgresBackupRecoveryPointSource(point),capture=await authenticatePeriodicApplicationCapture(context,proof,pointId,{signal}),materials=periodicApplicationCaptureSource(capture),manifest=await readManifest(join(selected.directory,'backup-complete.enc'),periodicApplicationKey(record),{signal});
  if(!exact(manifest,['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','backupId','pointId','applicationPolicyDigest','captureManifestHash','pointManifestHash','checkpointDigest','checkpointAt','completedAt','baseVerifiedAt','backupComplete','restoreVerified','deploymentAllowed','walIntegrity'])||!bound(manifest,record)||manifest.scope!==backupScope||manifest.backupId!==proof.backupId||manifest.pointId!==pointId||manifest.applicationPolicyDigest!==hash(materials.configuration)||manifest.captureManifestHash!==hash(materials.manifest)||manifest.captureManifestHash!==selected.manifest.applicationCapture?.manifestHash||manifest.pointManifestHash!==hash(selected.manifest)||!same(manifest.checkpointDigest,selected.manifest.checkpointDigest)||manifest.checkpointAt!==selected.checkpoint.created||!time(manifest.completedAt)||manifest.completedAt<manifest.checkpointAt||!time(manifest.baseVerifiedAt)||manifest.baseVerifiedAt>manifest.completedAt||manifest.backupComplete!==true||manifest.restoreVerified!==false||manifest.deploymentAllowed!==false||source.metadata.timeline!==materials.manifest.walFence.timeline)fail('periodic_application_backup_binding');
  const token=Object.freeze({backupId:proof.backupId,pointId,scope:backupScope});backups.set(token,{context,proof,point,capture,manifest,directory:selected.directory,manifestDigest:await digestFile(join(selected.directory,'backup-complete.enc'),{signal})});return token;
}
export function periodicApplicationBackupSource(token,{context,proof}={}){
  const value=backups.get(token);if(!value||context!==undefined&&value.context!==context||proof!==undefined&&value.proof!==proof||postgresBackupContext(value.context).closed)fail('periodic_application_backup_required');return {...value,manifest:structuredClone(value.manifest),manifestDigest:structuredClone(value.manifestDigest)};
}

// Producer: small application material capture and closed WAL only. It never
// restores the shared base and never reports an untested point as verified.
export async function capturePeriodicApplicationBackup(context,proof,configuration,{signal}={}){
  const policy=periodicApplicationPolicy(configuration);if(!policy.enabled)return result('periodic_application_disabled');
  const {record,source,base}=contextFor(context,proof);if(record.scope==='existing-cluster')installedPeriodicApplicationPolicy(policy);
  return operate(context,proof,'producer',async signal=>{
    if(record.scope==='existing-cluster')await verifyInstalledPeriodicApplicationPolicy(policy,{signal});
    const reservations=join(base,'application-cycles');await mkdir(reservations,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await privateDirectory(reservations);await syncDirectory(base);
    const policyDigest=hash(policy),rows=await inspectPeriodicApplicationBackupReservations(context,proof,{signal});let cycle;
    await stateChange(context,async state=>{
      if(state.pending){
        const path=join(record.artifactDirectory,state.pending.backupId,'application-cycles',state.pending.pointId+'.enc'),saved=validCycle(await readManifest(path,periodicApplicationKey(record),{signal}),record,state.pending.backupId,state.pending.pointId);
        if(saved.backupId===proof.backupId&&saved.applicationPolicyDigest===policyDigest&&saved.stage!=='failed')cycle=saved;
        else{if(!['failed','backup_complete'].includes(saved.stage))await writeManifest(path,{...saved,stage:'failed',failureCode:'periodic_application_configuration_changed',updatedAt:Date.now()},periodicApplicationKey(record),{replace:true});state.pending=null;}
      }
      for(const row of rows.sort((a,b)=>a.reservedAt-b.reservedAt||a.pointId.localeCompare(b.pointId))){
        if(['failed','backup_complete'].includes(row.stage)||row.pointId===cycle?.pointId)continue;
        if(!cycle&&row.applicationPolicyDigest===policyDigest)cycle=row;
        else await writeManifest(join(reservations,row.pointId+'.enc'),{...row,stage:'failed',failureCode:'periodic_application_orphan_superseded',updatedAt:Date.now()},periodicApplicationKey(record),{replace:true});
      }
      if(!cycle){const at=Date.now();cycle={...common(record),scope:cycleScope,applicationPolicyDigest:policyDigest,backupId:proof.backupId,pointId:randomUUID(),stage:'reserved',reservedAt:at,updatedAt:at};await writeManifest(join(reservations,cycle.pointId+'.enc'),cycle,periodicApplicationKey(record));}
      state.pending={backupId:proof.backupId,pointId:cycle.pointId};
    },{signal});
    const pointId=cycle.pointId,path=join(reservations,pointId+'.enc'),stage=async value=>{signal?.throwIfAborted();cycle={...cycle,stage:value,updatedAt:Date.now()};await writeManifest(path,cycle,periodicApplicationKey(record),{replace:true});};
    const complete=async()=>{
      await revalidatePeriodicSourceBaseIfPresent(proof,{signal});
      const token=await authenticatePeriodicApplicationBackup(context,proof,pointId,{signal}),manifest=periodicApplicationBackupSource(token).manifest;await stage('backup_complete');
      await stateChange(context,state=>{if(!state.latestBackup||manifest.checkpointAt>=state.latestBackup.checkpointAt)state.latestBackup={backupId:proof.backupId,pointId,checkpointAt:manifest.checkpointAt,completedAt:manifest.completedAt};if(state.pending?.backupId===proof.backupId&&state.pending.pointId===pointId)state.pending=null;},{signal});
      return result('application_backup_completed',{backupId:proof.backupId,pointId,checkpointAt:manifest.checkpointAt,completedAt:manifest.completedAt,backupComplete:true,walIntegrity:manifest.walIntegrity});
    };
    try{
      if(await exists(join(base,'points',pointId,'backup-complete.enc')))return await complete();
      const capturePath=join(base,'application-staging',pointId);let capture;
      if(await exists(join(capturePath,'capture.enc')))capture=await verifyPeriodicApplicationCapture(context,proof,policy,pointId,{signal});
      else{if(cycle.stage!=='reserved'||await exists(capturePath))fail('periodic_application_capture_interrupted');capture=await capturePeriodicApplicationMaterials(context,proof,policy,{captureId:pointId,signal});}
      if(cycle.stage==='reserved')await stage('captured');
      let walIntegrity;
      if(cycle.stage==='captured'){walIntegrity=(await checkpointPeriodicPostgresBackup(proof,{signal})).walIntegrity;await stage('checkpointed');}
      const point=await capturePostgresBackupRecoveryPoint(context,proof,{pointId,applicationCapture:capture,signal}),selected=postgresBackupRecoveryPointSource(point);
      // A second actual source scan after the WAL fence is essential. Archive
      // authentication alone cannot prove a live application/DB pairing.
      capture=await verifyPeriodicApplicationCapture(context,proof,policy,pointId,{signal});
      const material=periodicApplicationCaptureSource(capture),checkpoint=selected.checkpoint;
      if(record.scope==='existing-cluster')await verifyInstalledPeriodicApplicationPolicy(policy,{signal});
      signal?.throwIfAborted();const manifest={...common(record),scope:backupScope,backupId:proof.backupId,pointId,applicationPolicyDigest:policyDigest,captureManifestHash:hash(material.manifest),pointManifestHash:hash(selected.manifest),checkpointDigest:selected.manifest.checkpointDigest,checkpointAt:checkpoint.created,completedAt:Date.now(),baseVerifiedAt:source.verifiedAt,backupComplete:true,restoreVerified:false,deploymentAllowed:false,...(walIntegrity?{walIntegrity}:{})};
      await revalidatePeriodicSourceBaseIfPresent(proof,{signal});
      await writeManifest(join(selected.directory,'backup-complete.enc'),manifest,periodicApplicationKey(record));return await complete();
    }catch(error){
      if(cleanupUnknown(error)||signal?.aborted||record.closed)throw error;
      cycle.failureCode=typeof error.code==='string'&&/^[a-z][a-z0-9_]{0,95}$/.test(error.code)?error.code:'periodic_application_backup_failed';await stage('failed');
      await stateChange(context,state=>{if(state.pending?.backupId===proof.backupId&&state.pending.pointId===pointId)state.pending=null;},{signal});
      return result('application_backup_failed',{backupId:proof.backupId,failedPointId:pointId,failureCode:cycle.failureCode});
    }
  },{signal});
}

function validateReport(restored,record,proof,selected){
  verifyPeriodicApplicationKeyBinding(restored,record);const application=restored?.application,runtime=application?.runtime;verifyPeriodicApplicationKeyBinding(application,record);
  if(restored?.version!==1||restored.scope!==record.scope||restored.backupId!==proof.backupId||restored.recoveryPointId!==selected.pointId||restored.configurationDigest!==record.configurationDigest||!same(restored.checkpointDigest,selected.manifest.checkpointDigest)||!['restoreVerified','systemIdVerified','recoveryExitedVerified','writeReadbackVerified'].every(name=>restored[name]===true)||application?.captureId!==selected.pointId||!['applicationMaterialsVerified','filesRestoredVerified','originalStoredCredentialsVerified'].every(name=>application[name]===true)||application.accountHomeMapping?.verified!==true||application.recordingVerification?.allVerified!==true||!['existingDeviceTrustVerified','serverTlsIdentityVerified','reportKeyRestoredVerified'].every(name=>application.deviceVerification?.[name]===true)||runtime?.scope!==(record.scope==='existing-cluster'?'installed-restored-periodic-source-runtime':'task-restored-periodic-source-runtime')||!['gatewayStartedVerified','independentWorkerStartedVerified','workerProgressVerified','originalKeyDatabaseVerified','restoredTlsServerVerified','freshRestoredDatabaseReportVerified','upstreamCallsDisabled'].every(name=>runtime[name]===true)||!['applicationProcessesExitedVerified','applicationDrainVerified','publisherStoppedVerified','temporaryLoginRevokedVerified'].every(name=>runtime.cleanup?.[name]===true)||!time(restored.periodicRestoreStartedAt)||!time(restored.created)||restored.periodicRestoreStartedAt<selected.checkpoint.created||restored.created<restored.periodicRestoreStartedAt)fail('periodic_application_restore_incomplete');
  if(record.scope==='existing-cluster'&&(runtime.crossUidExecutionVerified!==true||runtime.publisherUidVerified!==true||runtime.restoreUid!==record.restoreUid))fail('periodic_application_restore_incomplete');
}
async function verifiedFinal(context,proof,backup,{signal}={}){
  const {record}=contextFor(context,proof,{archived:true}),source=periodicApplicationBackupSource(backup),selected=postgresBackupRecoveryPointSource(source.point),final=await readManifest(join(source.directory,'application-final.enc'),periodicApplicationKey(record),{signal}),report=await readManifest(join(source.directory,'application-restore.enc'),periodicApplicationKey(record),{signal});
  validateReport(report,record,proof,selected);
  if(!same(await digestFile(join(source.directory,'backup-complete.enc'),{signal}),source.manifestDigest))fail('periodic_application_backup_changed');
  if(!exact(final,['version','scope','configurationDigest','systemId','keyVersion','applicationKeyVersion','applicationKeyDigest','backupId','pointId','applicationPolicyDigest','captureManifestHash','pointManifestHash','backupCompleteDigest','restoreReport','restoreDigest','checkpointAt','startedAt','verifiedAt','restoreDurationMs','restoreVerified','applicationMaterialsVerified','candidateVerified','offhostVerified','deploymentAllowed'])||!bound(final,record)||final.scope!==finalScope||final.backupId!==proof.backupId||final.pointId!==source.manifest.pointId||!same(final.backupCompleteDigest,source.manifestDigest)||final.applicationPolicyDigest!==source.manifest.applicationPolicyDigest||final.captureManifestHash!==source.manifest.captureManifestHash||final.pointManifestHash!==source.manifest.pointManifestHash||final.restoreReport!=='application-restore.enc'||!same(final.restoreDigest,await digestFile(join(source.directory,'application-restore.enc'),{signal}))||final.checkpointAt!==source.manifest.checkpointAt||final.startedAt!==report.periodicRestoreStartedAt||final.verifiedAt!==report.created||final.restoreDurationMs!==final.verifiedAt-final.startedAt||final.restoreVerified!==true||final.applicationMaterialsVerified!==true||final.candidateVerified!==false||final.offhostVerified!==false||final.deploymentAllowed!==false)fail('periodic_application_final_binding');return final;
}

export async function verifyPeriodicApplicationBackupFinal(context,proof,backup,{signal}={}){
  return verifiedFinal(context,proof,backup,{signal});
}

// Independent, explicitly selected real restore. A producer may publish newer
// backup-complete points while this invocation verifies its immutable point.
export async function verifyPeriodicApplicationBackup(context,proof,pointId,{signal}={}){
  if(!uuid(pointId))fail('periodic_application_capture_id');const {record}=contextFor(context,proof,{archived:true});
  return operate(context,proof,'verifier',async signal=>{
    const backup=await authenticatePeriodicApplicationBackup(context,proof,pointId,{signal}),source=periodicApplicationBackupSource(backup),selected=postgresBackupRecoveryPointSource(source.point);
    await stateChange(context,state=>{
      if(state.pendingVerification&&(state.pendingVerification.backupId!==proof.backupId||state.pendingVerification.pointId!==pointId))fail('periodic_application_verification_pending');
      state.pendingVerification={backupId:proof.backupId,pointId};
    },{signal});
    if(!await exists(join(source.directory,'application-final.enc'))){
      const reportPath=join(source.directory,'application-restore.enc');
      if(!await exists(reportPath)){
        const plan=await prepareArchivedPeriodicApplicationRestore(context,proof,backup,{signal}),startedAt=Date.now(),receipt=await restorePostgresBackup(proof,{recoveryPoint:source.point,periodicApplication:plan,signal}),restored={...inspectPostgresBackupRestore(receipt),...periodicApplicationKeyBinding(record),periodicRestoreStartedAt:startedAt};
        validateReport(restored,record,proof,selected);await writeManifest(reportPath,restored,periodicApplicationKey(record));
      }
      const restored=await readManifest(reportPath,periodicApplicationKey(record),{signal});validateReport(restored,record,proof,selected);
      const final={...common(record),scope:finalScope,backupId:proof.backupId,pointId,applicationPolicyDigest:source.manifest.applicationPolicyDigest,captureManifestHash:source.manifest.captureManifestHash,pointManifestHash:source.manifest.pointManifestHash,backupCompleteDigest:source.manifestDigest,restoreReport:'application-restore.enc',restoreDigest:await digestFile(reportPath,{signal}),checkpointAt:source.manifest.checkpointAt,startedAt:restored.periodicRestoreStartedAt,verifiedAt:restored.created,restoreDurationMs:restored.created-restored.periodicRestoreStartedAt,restoreVerified:true,applicationMaterialsVerified:true,candidateVerified:false,offhostVerified:false,deploymentAllowed:false};
      signal?.throwIfAborted();if(!same(await digestFile(join(source.directory,'backup-complete.enc'),{signal}),source.manifestDigest))fail('periodic_application_backup_changed');await writeManifest(join(source.directory,'application-final.enc'),final,periodicApplicationKey(record));
    }
    const final=await verifiedFinal(context,proof,backup,{signal});
    await stateChange(context,state=>{if(!state.lastVerified||final.checkpointAt>=state.lastVerified.checkpointAt)state.lastVerified={backupId:proof.backupId,pointId,checkpointAt:final.checkpointAt,verifiedAt:final.verifiedAt};if(state.pendingVerification?.backupId===proof.backupId&&state.pendingVerification.pointId===pointId)state.pendingVerification=null;},{signal});
    return result('application_point_verified',{backupId:proof.backupId,pointId,checkpointAt:final.checkpointAt,verifiedAt:final.verifiedAt,restoreDurationMs:final.restoreDurationMs,backupComplete:true,restoreVerified:true,applicationMaterialsVerified:true});
  },{signal,archived:true});
}

// Manual typed export of one closed point. It contains no current source paths
// to execute and never claims the source point has been restored or activated.
export async function preparePeriodicApplicationHandoff(context,proof,pointId,{signal}={}){
 const {record}=contextFor(context,proof),{withCanonicalPeriodicHandoff,inspectCanonicalPeriodicCopies,periodicCipherIdentity}=await import('./postgres-periodic-handoff.mjs'),{copyBackupCipher,handoffFiles,handoffControlEntries,backupMachineIdentity}=await import('./postgres-backup-handoff.mjs');
 return operate(context,proof,'handoff-'+pointId,async signal=>{
  const backup=await authenticatePeriodicApplicationBackup(context,proof,pointId,{signal}),closed=periodicApplicationBackupSource(backup),destination=join(closed.directory,'handoff');
  if(await exists(join(destination,'handoff.enc'))){
   let lease;try{lease=await acquireInstance(destination);}catch(error){if(error.code==='EADDRINUSE')fail('periodic_application_handoff_busy');throw error;}try{
   const manifest=await readManifest(join(destination,'handoff.enc'),periodicApplicationKey(record),{signal});
   if(manifest.applicationKeyVersion!==record.applicationKeyVersion||manifest.applicationKeyDigest!==record.applicationKeyDigest||manifest.version!==1||manifest.kind!=='periodic-application'||manifest.backupId!==proof.backupId||manifest.pointId!==pointId||manifest.configurationDigest!==record.configurationDigest||manifest.keyVersion!==record.keyVersion||!same(manifest.backupCompleteDigest,closed.manifestDigest)||!same(manifest.checkpointDigest,closed.manifest.checkpointDigest)||manifest.sourceMachine!==await backupMachineIdentity())fail('periodic_application_handoff_binding');
   const entries=[],maxBytes=manifest.totalBytes+manifest.parts.reduce((n,row)=>n+row.digest.bytes,1048576);for await(const entry of handoffControlEntries({directory:destination,key:periodicApplicationKey(record),manifest,signal,maxFiles:manifest.count,maxBytes}))entries.push(entry);
   const present=await inspectCanonicalPeriodicCopies(destination,manifest,entries,{signal});
   if(present.size===entries.length)return {version:1,kind:'periodic-application',backupId:proof.backupId,pointId,handoffDigest:await digestFile(join(destination,'handoff.enc'),{signal}),files:manifest.count,bytes:manifest.totalBytes,backupComplete:true,restoreVerified:false,offhostVerified:false,deploymentAllowed:false};
   }finally{await lease.release();}
  }
  return withCanonicalPeriodicHandoff(context,proof,pointId,{signal},async source=>{
  const present=await inspectCanonicalPeriodicCopies(source.directory,source.manifest,source.entries,{signal});
  for(const entry of source.entries){signal?.throwIfAborted();if(present.has(entry.path))continue;
   if(!same(entry.identity,await periodicCipherIdentity(entry.sourcePath,entry.uid)))fail('backup_handoff_source_changed');
   await copyBackupCipher(entry.sourcePath,join(source.directory,entry.path),{signal,maxBytes:entry.digest.bytes});
   if(!same(entry.identity,await periodicCipherIdentity(entry.sourcePath,entry.uid))||!same(await digestFile(join(source.directory,entry.path),{signal}),entry.digest))fail('backup_handoff_source_changed');
  }
  for await(const unused of handoffFiles({directory:source.directory,key:periodicApplicationKey(record),manifest:source.manifest,signal,maxFiles:source.manifest.count,maxBytes:source.manifest.totalBytes+source.manifest.parts.reduce((n,row)=>n+row.digest.bytes,1048576)})){}
  return {version:1,kind:'periodic-application',backupId:proof.backupId,pointId,handoffDigest:source.handoffDigest,files:source.manifest.count,bytes:source.manifest.totalBytes,backupComplete:true,restoreVerified:false,offhostVerified:false,deploymentAllowed:false};
 });},{signal});
}
