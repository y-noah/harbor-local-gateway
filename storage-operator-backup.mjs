import {join} from 'node:path';
import {createHash,timingSafeEqual} from 'node:crypto';
import {withPendingStorageOperatorBackup,inspectPendingStorageOperatorBackup} from './storage-operator-backup-binding.mjs';
import {createPendingPostgresBackupContext,closePostgresBackupContext,postgresBackupContext,verifyPostgresBackupPendingBinding} from './postgres-backup-context.mjs';
import {createPostgresBaseBackup,checkpointPostgresBackup,inspectPostgresBackup} from './postgres-backup.mjs';
import {restorePostgresBackup,verifyPostgresBackupRestore,inspectPostgresBackupRestore} from './postgres-backup-restore.mjs';
import {captureStorageOperatorBackupApplication,bindStorageOperatorApplicationCheckpoint,restoredRuntimeTlsIdentity} from './storage-operator-backup-application.mjs';
import {canonical,readManifest,writeManifest,transferError} from './migration-artifacts.mjs';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {withVerifiedStorageOperatorBaseline,storageOperatorBaselineMode} from './storage-operator-baseline.mjs';
const proofs=new WeakMap(),fail=code=>{throw transferError(code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
export function inspectStorageOperatorBackup(proof){const value=proofs.get(proof);if(!value)fail('storage_operator_final_backup_proof_required');return structuredClone(value.summary);}
export async function createVerifiedStorageOperatorFinalBackup({context,baselineVerification,backupKey,signal}={}){
 const trusted=storageOperatorContext(context);if(storageOperatorBaselineMode(baselineVerification)!=='resume')fail('storage_operator_final_resumed_baseline_required');
 return withVerifiedStorageOperatorBaseline(baselineVerification,{context,signal,requireFormal:trusted.scope!=='task-owned'},async({verification,backupContext,backupProof,backupRestore})=>{
  const state=await readManifest(join(trusted.configuration.runDirectory,'operator.enc'),backupKey,{signal});if(state.phase!=='pending'||!state.baseline||canonical(state.baseline)!==canonical(verification))fail('storage_operator_final_baseline_binding');
  // createPendingPostgresBackupContext rechecks the same resume state after the
  // final-backup callback has acquired the operator lock and SQLite write lease.
  return createStorageOperatorBackup({operatorContext:context,baselineContext:backupContext,baselineProof:backupProof,baselineRestore:backupRestore,backupKey,signal,prepareReceiverBundle:true});
 });
}
export async function createStorageOperatorBackup({operatorContext,baselineContext,baselineProof,baselineRestore,backupKey,signal,prepareReceiverBundle=false,verifyRestoredRuntime=false,restoredRuntimeTlsHost,restoredRuntimeCandidate}={}){
 if(typeof verifyRestoredRuntime!=='boolean')fail('storage_operator_runtime_flag_invalid');if(verifyRestoredRuntime)restoredRuntimeTlsIdentity(restoredRuntimeTlsHost);if(typeof prepareReceiverBundle!=='boolean')fail('storage_operator_bundle_flag_invalid');signal?.throwIfAborted();return withPendingStorageOperatorBackup({context:operatorContext,backupKey},async pending=>{
  let context;try{context=await createPendingPostgresBackupContext(baselineContext,baselineProof,baselineRestore,pending,{signal});const first=await inspectPendingStorageOperatorBackup(pending),record=postgresBackupContext(context);if(!timingSafeEqual(record.key,backupKey))fail('storage_operator_final_backup_key_mismatch');signal?.throwIfAborted();
   const database=await createPostgresBaseBackup(context,{signal});await verifyPostgresBackupPendingBinding(context);await captureStorageOperatorBackupApplication({context,proof:database,operatorContext,backupKey,signal,verifyRestoredRuntime,restoredRuntimeTlsHost,restoredRuntimeCandidate});await checkpointPostgresBackup(database,{signal});const application=await bindStorageOperatorApplicationCheckpoint(context,database),restored=await restorePostgresBackup(database,{signal});await verifyPostgresBackupRestore(restored,database,{signal});
   const final=await inspectPendingStorageOperatorBackup(pending);if(canonical(first.binding)!==canonical(final.binding))fail('storage_operator_final_backup_binding_changed');const recovery=inspectPostgresBackupRestore(restored);if(!recovery.restoreVerified||!recovery.recoveryExitedVerified||!recovery.application?.filesRestoredVerified||!recovery.application.originalStoredCredentialsVerified||!recovery.application.originalRowsAndAmountsVerified)fail('storage_operator_final_backup_incomplete');
   const summary={version:1,stage:'pending-final-copy-and-local-restore',scope:record.scope,backupId:database.backupId,runDigest:hash(final.binding.runId),pendingBindingDigest:record.pendingBindingDigest,configurationDigest:record.configurationDigest,databaseCopy:inspectPostgresBackup(database),applicationManifestDigest:application.applicationManifestDigest,filesManifestDigest:application.filesManifestDigest,checkpointDigest:application.checkpointDigest,recovery,applicationMaterialsVerified:false,offhostVerified:false,deploymentAllowed:false,missing:[...recovery.application.missing,...(record.scope==='task-owned'?['actual_docker_source_fence']:[]),'offhost_restore_verification','deployment_activation_consumer']};await writeManifest(join(record.artifactDirectory,database.backupId,'operator-final.enc'),summary,record.key);await verifyPostgresBackupPendingBinding(context);if(prepareReceiverBundle){const {preparePostgresBackupHandoff}=await import('./postgres-backup-handoff.mjs');summary.receiverBundle=await preparePostgresBackupHandoff({context,proof:database,restore:restored,signal});}const proof=Object.freeze({backupId:database.backupId,scope:record.scope});proofs.set(proof,{summary});return proof;
  }finally{if(context)await closePostgresBackupContext(context);}
 });
}
