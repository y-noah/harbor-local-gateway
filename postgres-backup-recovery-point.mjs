import {mkdir,lstat,realpath,readdir} from 'node:fs/promises';
import {join,resolve,dirname} from 'node:path';
import {randomUUID,createHash} from 'node:crypto';
import {postgresBackupContext,verifyBackupCluster} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource,withPostgresBackupCheckpointLock} from './postgres-backup.mjs';
import {withPostgresBackupOperation} from './postgres-backup-lock.mjs';
import {periodicApplicationCaptureSource} from './postgres-periodic-application-materials.mjs';
import {publishWalCheckpointCatalog,readWalCheckpointSegments} from './postgres-backup-wal-catalog.mjs';
import {readManifest,writeManifest,digestFile,canonical,privateDirectory,syncDirectory,transferError} from './migration-artifacts.mjs';

const points=new WeakMap(),scope='harbor-backup-recovery-point-v1';
const fail=code=>{throw transferError(code);};
const same=(a,b)=>canonical(a)===canonical(b);
const hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const digest=value=>value&&/^[a-f0-9]{64}$/.test(value.sha256??'')&&Number.isSafeInteger(value.bytes)&&value.bytes>=36;
const lsn=value=>{if(typeof value!=='string'||!/^[A-F0-9]+\/[A-F0-9]+$/.test(value))fail('backup_recovery_point_application_fence');const [high,low]=value.split('/');return (BigInt('0x'+high)<<32n)+BigInt('0x'+low);};
function applicationFence(reference,checkpoint,metadata){
  if(reference===undefined)return;
  const fence=reference.walFence;
  if(!fence||Object.keys(fence).sort().join(',')!=='postmasterStart,timeline,walInsertLsn'||fence.postmasterStart!==(checkpoint.sourcePostmasterStart??metadata.postmasterStart)||fence.timeline!==metadata.timeline||checkpoint.timeline!==fence.timeline||lsn(checkpoint.lsn)<=lsn(fence.walInsertLsn))fail('backup_recovery_point_application_fence');
}
function sourceFor(context,proof){
  const record=postgresBackupContext(context),source=postgresBackupRestoreSource(proof);
  if(record.closed||record.pendingBinding||record.receiverBinding&&record.periodicReceiverBinding!==record.receiverBinding||record.resumeBinding||record.applicationPlan||source.context!==context)fail('backup_recovery_point_context');
  return {record,source,base:join(record.artifactDirectory,proof.backupId)};
}
async function directory(path){
  try{await mkdir(path,{mode:0o700});}catch(error){if(error.code!=='EEXIST')throw error;}
  await privateDirectory(path);
  const info=await lstat(path);
  if(info.uid!==process.getuid()||await realpath(path)!==resolve(path))fail('backup_recovery_point_directory');
  await syncDirectory(dirname(path));
}
async function baseBinding(record,source,base,{signal}={}){
  const manifest=await readManifest(join(base,'backup.enc'),record.key,{signal});
  if(!same(manifest,source.metadata))fail('backup_recovery_point_base_changed');
  return digestFile(join(base,'backup.enc'),{signal});
}
async function validatePoint(context,proof,pointId,{signal}={}){
  if(!uuid(pointId))fail('backup_recovery_point_id');
  const {record,source,base}=sourceFor(context,proof),path=join(base,'points',pointId);
  await privateDirectory(join(base,'points'));await privateDirectory(path);
  const manifest=await readManifest(join(path,'point.enc'),record.key,{signal});
  const keys=['version','scope','backupId','pointId','configurationDigest','systemId','keyVersion','baseManifestDigest','sourceCheckpointDigest','checkpointDigest','created','applicationCapture'];
  if(!manifest||Object.keys(manifest).some(key=>!keys.includes(key))||manifest.version!==1||manifest.scope!==scope||manifest.backupId!==proof.backupId||manifest.pointId!==pointId||manifest.configurationDigest!==record.configurationDigest||manifest.systemId!==record.expected.system_id||manifest.keyVersion!==record.keyVersion||!Number.isSafeInteger(manifest.created)||manifest.created<0||![manifest.baseManifestDigest,manifest.sourceCheckpointDigest,manifest.checkpointDigest].every(digest))fail('backup_recovery_point_binding');
  if(!same(manifest.baseManifestDigest,await baseBinding(record,source,base,{signal}))||!same(manifest.checkpointDigest,await digestFile(join(path,'checkpoint.enc'),{signal})))fail('backup_recovery_point_changed');
  if(manifest.applicationCapture!==undefined&&(manifest.applicationCapture?.captureId!==pointId||!/^[a-f0-9]{64}$/.test(manifest.applicationCapture.manifestHash??'')||Object.keys(manifest.applicationCapture).some(key=>!['captureId','manifestHash','walFence'].includes(key))))fail('backup_recovery_point_application_capture');
  const checkpoint=await readManifest(join(path,'checkpoint.enc'),record.key,{signal});
  if(checkpoint.version!==2||checkpoint.backupId!==proof.backupId||checkpoint.configurationDigest!==record.configurationDigest||checkpoint.systemId!==source.metadata.systemId||checkpoint.keyVersion!==record.keyVersion||checkpoint.timeline!==source.metadata.timeline||!new RegExp('^harbor_backup_'+proof.backupId.replaceAll('-','')+'_[a-f0-9]{12}$').test(checkpoint.restorePoint))fail('backup_recovery_point_checkpoint');
  applicationFence(manifest.applicationCapture,checkpoint,source.metadata);
  let count=0;for await(const entry of readWalCheckpointSegments({directory:path,key:record.key,checkpoint,signal})){if(entry.content.bytes!==Number(source.metadata.segmentBytes)||++count>record.profile.maxWalSegments)fail('backup_recovery_point_wal_capacity');}
  signal?.throwIfAborted();
  if(!same(manifest.checkpointDigest,await digestFile(join(path,'checkpoint.enc'),{signal})))fail('backup_recovery_point_changed');
  const token=Object.freeze({backupId:proof.backupId,pointId,scope});
  points.set(token,{context,proof,directory:path,manifest,checkpoint});return token;
}

async function operate(context,proof,operation,{signal}={}){
  const {record}=sourceFor(context,proof);
  if(record.busy)fail('backup_operation_busy');
  record.busy=true;
  const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean));
  record.cancellation=cancellation;
  const pending=withPostgresBackupOperation(record,()=>operation(combined),{signal:combined});
  record.pending=pending;
  try{return await pending;}finally{record.busy=false;record.pending=null;record.cancellation=null;}
}

// A point copies only the authenticated WAL catalog and checkpoint. The large
// immutable base remains shared, so frequent application snapshots do not turn
// into a full pg_basebackup on every cycle. A crash leaves a private unfinished
// point, never a completion marker; unknown contents are not removed or adopted.
export async function capturePostgresBackupRecoveryPoint(context,proof,{pointId=randomUUID(),signal,applicationCapture}={}){
  if(!uuid(pointId))fail('backup_recovery_point_id');
  const {record,source,base}=sourceFor(context,proof);if(record.receiverBinding)fail('backup_receiver_readonly');
  let applicationReference;
  if(applicationCapture!==undefined){const capture=periodicApplicationCaptureSource(applicationCapture,{context,proof});if(capture.captureId!==pointId||!capture.sourceVerified)fail('backup_recovery_point_application_capture');applicationReference={captureId:capture.captureId,manifestHash:hash(capture.manifest),walFence:structuredClone(capture.manifest.walFence)};}
  return operate(context,proof,signal=>withPostgresBackupCheckpointLock(context,proof.backupId,async()=>{
    await verifyBackupCluster(context);signal?.throwIfAborted();await syncDirectory(record.artifactDirectory);
    const baseManifestDigest=await baseBinding(record,source,base,{signal}),root=join(base,'points'),path=join(root,pointId);
    await directory(root);await directory(path);
    const names=await readdir(path);
    if(names.length){
      if(!names.includes('point.enc'))fail('backup_recovery_point_incomplete');
      const token=await validatePoint(context,proof,pointId,{signal});
      if(applicationReference){const saved=postgresBackupRecoveryPointSource(token).manifest.applicationCapture;if(!saved||!same(saved,applicationReference))fail('backup_recovery_point_application_capture');}
      return token;
    }
    const sourceCheckpoint=join(base,'checkpoint.enc'),sourceCheckpointDigest=await digestFile(sourceCheckpoint,{signal}),checkpoint=await readManifest(sourceCheckpoint,record.key,{signal});
    if(!same(sourceCheckpointDigest,await digestFile(sourceCheckpoint,{signal})))fail('backup_recovery_point_source_changed');
    if(checkpoint.backupId!==proof.backupId||checkpoint.configurationDigest!==record.configurationDigest||checkpoint.systemId!==source.metadata.systemId||checkpoint.keyVersion!==record.keyVersion||checkpoint.timeline!==source.metadata.timeline)fail('backup_recovery_point_checkpoint');
    applicationFence(applicationReference,checkpoint,source.metadata);
    let count=0;
    async function* segments(){for await(const entry of readWalCheckpointSegments({directory:base,key:record.key,checkpoint,signal})){if(++count>record.profile.maxWalSegments)fail('backup_recovery_point_wal_capacity');yield entry;}}
    const {walCatalog:unused,version:unusedVersion,...fields}=checkpoint;
    await publishWalCheckpointCatalog({directory:path,key:record.key,checkpoint:fields,segments:segments(),signal});
    signal?.throwIfAborted();
    if(!same(sourceCheckpointDigest,await digestFile(sourceCheckpoint,{signal}))||!same(baseManifestDigest,await baseBinding(record,source,base,{signal})))fail('backup_recovery_point_source_changed');
    const manifest={version:1,scope,backupId:proof.backupId,pointId,configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,baseManifestDigest,sourceCheckpointDigest,checkpointDigest:await digestFile(join(path,'checkpoint.enc'),{signal}),created:Date.now(),...(applicationReference?{applicationCapture:applicationReference}:{})};
    await writeManifest(join(path,'point.enc'),manifest,record.key);
    return validatePoint(context,proof,pointId,{signal});
  }),{signal});
}

export async function verifyPostgresBackupRecoveryPoint(context,proof,pointId,{signal}={}){
  return operate(context,proof,signal=>validatePoint(context,proof,pointId,{signal}),{signal});
}

export function postgresBackupRecoveryPointSource(token,{context,proof}={}){
  const point=points.get(token);
  if(!point||context!==undefined&&point.context!==context||proof!==undefined&&postgresBackupRestoreSource(proof).context!==point.context||proof!==undefined&&proof.backupId!==point.proof.backupId)fail('backup_recovery_point_proof_required');
  if(postgresBackupContext(point.context).closed)fail('backup_context_closed');
  return {context:point.context,backupId:point.proof.backupId,pointId:token.pointId,directory:point.directory,manifest:structuredClone(point.manifest),checkpoint:structuredClone(point.checkpoint)};
}
