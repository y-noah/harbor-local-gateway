import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join} from 'node:path';
import {createHash,createPublicKey} from 'node:crypto';
import {PostgresStore} from './postgres-store.mjs';
import {postgresBackupContext,verifyBackupBaselineCluster,createPostgresBackupContext,createResumingPostgresBackupContext,closePostgresBackupContext} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource,verifyPostgresBackup} from './postgres-backup.mjs';
import {verifyPostgresBackupRestore,restorePostgresBackup} from './postgres-backup-restore.mjs';
import {backupMachineIdentity} from './postgres-backup-handoff.mjs';
import {verifyStorageReceiverAttestation,verifyOffhostStorageReceiverAttestation} from './storage-receiver-attestation.mjs';
import {canonical,digestFile,readManifest,privateDirectory,transferError} from './migration-artifacts.mjs';

const fail=code=>{throw transferError(code);},same=(a,b)=>canonical(a)===canonical(b),hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const verifiedBaselines=new WeakMap();
async function trustedFile(path,maxBytes){const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);try{const info=await file.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.size<1||info.size>maxBytes)fail('storage_operator_baseline_file_not_trusted');return await file.readFile();}finally{await file.close();}}
async function approvedFiles(trusted){
 if(trusted.scope!=='docker-systemd'||process.getuid()!==0)fail('storage_operator_baseline_formal_required');
 if(!/^[a-f0-9]{64}$/.test(trusted.configuration.baselineReceiverMachine??''))fail('storage_operator_baseline_receiver_required');await privateDirectory(trusted.configuration.runDirectory);
 const publicKey=await trustedFile('/etc/harbor-storage-receiver.pub',4096),bytes=await trustedFile(join(trusted.configuration.runDirectory,'baseline-receiver-attestation.json'),16384);let attestation;try{attestation=JSON.parse(bytes.toString('utf8'));}catch{fail('storage_operator_baseline_attestation_invalid');}return {publicKey,attestation};
}
async function verifyBaseline({context,backupContext,backupProof,backupRestore,attestation,publicKey,formal,signal}){
 signal?.throwIfAborted();const {storageOperatorContext}=await import('./storage-operator-context.mjs'),trusted=storageOperatorContext(context),record=postgresBackupContext(backupContext),source=postgresBackupRestoreSource(backupProof);
 if(formal?trusted.scope!=='docker-systemd'||record.scope!=='existing-cluster':!['task-owned','task-readiness-only'].includes(trusted.scope)||record.scope!=='task-owned')fail('storage_operator_baseline_scope');
 if(record.receiverBinding||record.pendingBinding||record.applicationPlan||source.context!==backupContext||trusted.configuration.backupKeyVersion!==record.keyVersion)fail('storage_operator_baseline_binding');
 // Keep the exact verified authority in process; caller-owned JSON and key
 // buffers must not change the authority used by a later prepare check.
 attestation=structuredClone(attestation);publicKey=publicKey?.type==='public'?createPublicKey({key:publicKey.export({format:'der',type:'spki'}),format:'der',type:'spki'}):createPublicKey(publicKey);
 const check=async()=>{signal?.throwIfAborted();const actual=await verifyBackupBaselineCluster(backupContext,{signal});if(actual.postmaster_start!==source.metadata.postmasterStart||actual.timeline!==source.metadata.timeline)fail('storage_operator_baseline_source_restarted');return actual;};
 const actual=await check();await verifyPostgresBackupRestore(backupRestore,backupProof,{signal});const directory=join(record.artifactDirectory,backupProof.backupId),manifest=await readManifest(join(directory,'handoff','handoff.enc'),record.key,{signal}),sourceMachine=await backupMachineIdentity();
 if(manifest.kind!=='cluster-baseline'||manifest.backupId!==backupProof.backupId||manifest.scope!==record.scope||manifest.configurationDigest!==record.configurationDigest||manifest.sourceMachine!==sourceMachine||!same(manifest.expected,record.expected))fail('storage_operator_baseline_package_binding');
 const receiverMachine=formal?trusted.configuration.baselineReceiverMachine:sourceMachine,expected={kind:'cluster-baseline',backupId:backupProof.backupId,sourceMachine,receiverMachine,configurationDigest:record.configurationDigest,clusterDigest:hash(actual.system_id),handoffDigest:await digestFile(join(directory,'handoff','handoff.enc'),{signal}),checkpointDigest:await digestFile(join(directory,'checkpoint.enc'),{signal})};
 const verified=(formal?verifyOffhostStorageReceiverAttestation:verifyStorageReceiverAttestation)(attestation,publicKey,{expected});if(!formal&&verified.scope!=='task-owned-receiver')fail('storage_operator_baseline_scope');
 const admin=new PostgresStore({...trusted.adminOptions,poolSize:1});let summary;
 try{const identity=await admin.get('SELECT current_database() database,(SELECT oid::text FROM pg_database WHERE datname=current_database()) database_oid,(SELECT system_identifier::text FROM pg_control_system()) cluster');if(identity.cluster!==actual.system_id||trusted.configuration.expectedCluster!==null&&trusted.configuration.expectedCluster!==identity.cluster||identity.database!==trusted.configuration.registry.database||identity.database_oid!==trusted.configuration.registry.databaseOid)fail('storage_operator_baseline_operator_cluster');await check();await verifyPostgresBackupRestore(backupRestore,backupProof,{signal});await check();
  summary={version:1,scope:formal?'approved-existing-cluster-baseline':'task-baseline-diagnostic',baselineVerified:true,localRestoreVerified:true,receiverSignatureVerified:true,offhostBaselineVerified:formal,backupId:backupProof.backupId,operatorConfigurationDigest:trusted.configurationDigest,backupConfigurationDigest:record.configurationDigest,handoffDigest:expected.handoffDigest,checkpointDigest:expected.checkpointDigest,receiverResultDigest:verified.resultDigest,deploymentAllowed:false,missing:[...(!formal?['formal_root_baseline_and_offhost_receiver']:[]),'pending_final_backup_and_candidate_restore','deployment_activation_consumer']};
 }finally{await admin.close();}signal?.throwIfAborted();for(const value of [summary.missing,summary.handoffDigest,summary.checkpointDigest])Object.freeze(value);Object.freeze(summary);verifiedBaselines.set(summary,{context,backupContext,backupProof,backupRestore,attestation,publicKey,formal});return summary;
}
// A summary read from disk, copied or supplied by the caller is never a permit.
// Recheck the live source and all restore/signature bindings at consumption;
// this baseline proof alone does not enable migration or deployment.
export async function revalidateStorageOperatorBaseline(proof,{context,signal,requireFormal=true}={}){
 signal?.throwIfAborted();const verified=verifiedBaselines.get(proof);if(!verified)fail('storage_operator_baseline_proof_required');if(verified.context!==context)fail('storage_operator_baseline_context_mismatch');if(typeof requireFormal!=='boolean'||requireFormal&&!verified.formal)fail('storage_operator_baseline_formal_required');return verifyBaseline({...verified,signal});
}
export function storageOperatorBaselineMode(proof){const verified=verifiedBaselines.get(proof);if(!verified)fail('storage_operator_baseline_proof_required');return postgresBackupContext(verified.backupContext).resumeBinding?'resume':'first-run';}
// Consumers receive only the original branded backup/restore pair, never a
// caller-selected pair that merely reports matching public summary fields.
export async function withVerifiedStorageOperatorBaseline(proof,{context,signal,requireFormal=true}={},consume){
 if(typeof consume!=='function')fail('storage_operator_baseline_consumer_required');const verification=await revalidateStorageOperatorBaseline(proof,{context,signal,requireFormal}),record=verifiedBaselines.get(verification);return consume(Object.freeze({verification,backupContext:record.backupContext,backupProof:record.backupProof,backupRestore:record.backupRestore}));
}
// Root's trust key, receipt path and expected receiver identity come solely from
// installed operator configuration. Caller JSON cannot choose a trust anchor.
export async function verifyStorageOperatorBaseline({context,backupContext,backupProof,backupRestore,signal}={}){const {storageOperatorContext}=await import('./storage-operator-context.mjs'),trusted=storageOperatorContext(context),approved=await approvedFiles(trusted);return verifyBaseline({context,backupContext,backupProof,backupRestore,...approved,formal:true,signal});}
export async function verifyTaskStorageOperatorBaseline(options={}){return verifyBaseline({...options,formal:false});}

// Runs before any migration target exists. It never fences the SQLite source,
// creates operator roles/databases, changes readiness or consumes activation.
export async function checkInstalledStorageOperatorBaseline(context,{signal}={}){
 return withInstalledStorageOperatorBaseline(context,({verification})=>verification,{signal});
}
// Keep the verified backup context alive for the awaited preparation. Returning
// the status from check-baseline intentionally does not prolong this lifetime.
export async function withInstalledStorageOperatorBaseline(context,consume,{signal,resumeBinding}={}){
 if(typeof consume!=='function')fail('storage_operator_baseline_consumer_required');
 signal?.throwIfAborted();const {storageOperatorContext}=await import('./storage-operator-context.mjs'),trusted=storageOperatorContext(context),approved=await approvedFiles(trusted),id=approved.attestation?.payload?.backupId;if(typeof id!=='string'||!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(id))fail('storage_operator_baseline_attestation_invalid');const backupContext=resumeBinding?await createResumingPostgresBackupContext({context,resumeBinding,signal}):await createPostgresBackupContext();let summary;
 try{const backupProof=await verifyPostgresBackup(backupContext,id,{signal}),backupRestore=await restorePostgresBackup(backupProof,{signal}),verification=await verifyBaseline({context,backupContext,backupProof,backupRestore,...approved,formal:true,signal});summary=await consume(Object.freeze({verification,backupContext,backupProof,backupRestore}));}finally{await closePostgresBackupContext(backupContext);}signal?.throwIfAborted();return summary;
}
