import {periodicApplicationKey,periodicApplicationKeyBinding} from './postgres-periodic-application-key.mjs';
import {mkdir,readFile} from 'node:fs/promises';
import {join,dirname,resolve} from 'node:path';
import {createHash} from 'node:crypto';
import {PostgresStore} from './postgres-store.mjs';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {periodicApplicationCaptureSource,verifyPeriodicApplicationCapture} from './postgres-periodic-application-materials.mjs';
import {postgresBackupRecoveryPointSource,verifyPostgresBackupRecoveryPoint} from './postgres-backup-recovery-point.mjs';
import {inspectPostgresBackupRestoreSession} from './postgres-backup-restore.mjs';
import {verifyStoredCredentials} from './ci/postgres-preflight.mjs';
import {verifyRestoredRecordingCiphertexts} from './storage-operator-backup-recordings.mjs';
import {verifyRestoredDeviceMaterials} from './storage-operator-backup-devices.mjs';
import {canonical,regularFile,restoreFileSet,transferError} from './migration-artifacts.mjs';
import {periodicApplicationPolicy,verifyInstalledPeriodicApplicationPolicy} from './postgres-periodic-application-policy.mjs';

const plans=new WeakMap(),fail=code=>{throw transferError(code);};
const hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
async function keyFile(path){const info=await regularFile(path);if(info.uid!==process.getuid()||(info.mode&0o077)||info.size!==32)fail('periodic_application_key_invalid');return readFile(path);}
async function mapAccountHomes(store,configuration,destination,{signal}={}){
  let count=0;
  await store.transaction(async()=>{
    let after='';for(;;){
      signal?.throwIfAborted();const rows=await store.all('SELECT id,home FROM harbor.accounts WHERE id>$1 ORDER BY id LIMIT 32 FOR UPDATE',after);
      for(const row of rows){
        const home=row.home;if(typeof home!=='string'||home.includes('\0')||home!==resolve(home))fail('periodic_application_account_home_unmapped');
        const matches=configuration.materialRoots.filter(root=>{const prefix=root.databasePrefix??root.directory;return home===prefix||home.startsWith(prefix+'/');});
        if(matches.length!==1)fail('periodic_application_account_home_unmapped');
        const root=matches[0],prefix=root.databasePrefix??root.directory,target=join(destination,root.id,home.slice(prefix.length));
        if(target!==join(destination,root.id)&&!target.startsWith(join(destination,root.id)+'/'))fail('periodic_application_account_home_unmapped');
        await store.run('UPDATE harbor.accounts SET home=$1 WHERE id=$2',target,row.id);after=row.id;count++;
      }
      if(rows.length<32)break;
    }
  });return {verified:true,accounts:count};
}

// Only verified, opaque capture and point capabilities can select materials.
// This does not confer migration, activation or root candidate authority.
export async function preparePeriodicApplicationRestore(context,proof,configuration,capture,point,{signal}={}){
  const policy=periodicApplicationPolicy(configuration);if(postgresBackupContext(context).scope==='existing-cluster')await verifyInstalledPeriodicApplicationPolicy(policy,{signal});
  const selected=periodicApplicationCaptureSource(capture,{context,proof}),selectedPoint=postgresBackupRecoveryPointSource(point,{context,proof});
  const verified=await verifyPeriodicApplicationCapture(context,proof,configuration,selected.captureId,{signal}),verifiedPoint=await verifyPostgresBackupRecoveryPoint(context,proof,selectedPoint.pointId,{signal});
  const source=periodicApplicationCaptureSource(verified,{context,proof}),recovery=postgresBackupRecoveryPointSource(verifiedPoint,{context,proof});
  if(canonical(selected.manifest)!==canonical(source.manifest)||canonical(selectedPoint.manifest)!==canonical(recovery.manifest)||recovery.pointId!==source.captureId||recovery.manifest.applicationCapture?.manifestHash!==hash(source.manifest)||canonical(recovery.manifest.applicationCapture.walFence)!==canonical(source.manifest.walFence))fail('periodic_application_restore_binding');
  const token=Object.freeze({backupId:proof.backupId,pointId:recovery.pointId});plans.set(token,{context,proof,source,recovery,policy});return token;
}

// Only an authenticated backup-complete capability can restore historical
// material roots. This path never inspects the original source directories or
// treats its archived policy as a currently installed producer configuration.
export async function prepareArchivedPeriodicApplicationRestore(context,proof,backup,{signal}={}){
  const {periodicApplicationBackupSource}=await import('./postgres-periodic-application-backup.mjs');
  const selected=periodicApplicationBackupSource(backup,{context,proof}),source=periodicApplicationCaptureSource(selected.capture,{context,proof}),recovery=postgresBackupRecoveryPointSource(selected.point,{context,proof});
  if(source.sourceVerified!==false||recovery.pointId!==source.captureId||selected.manifest.captureManifestHash!==hash(source.manifest)||selected.manifest.pointManifestHash!==hash(recovery.manifest))fail('periodic_application_restore_binding');
  signal?.throwIfAborted();const token=Object.freeze({backupId:proof.backupId,pointId:recovery.pointId});plans.set(token,{context,proof,source,recovery,policy:source.configuration,backup});return token;
}

export async function revalidateArchivedPeriodicApplicationRestore(token,session,{signal}={}){
  const live=inspectPostgresBackupRestoreSession(session),plan=plans.get(token);inspectPeriodicApplicationRestore(token,{...live});
  if(!plan?.backup)fail('periodic_application_backup_required');
  const {periodicApplicationBackupSource}=await import('./postgres-periodic-application-backup.mjs'),{digestFile}=await import('./migration-artifacts.mjs');
  const backup=periodicApplicationBackupSource(plan.backup,{context:live.context,proof:plan.proof});
  if(canonical(await digestFile(join(backup.directory,'backup-complete.enc'),{signal}))!==canonical(backup.manifestDigest))fail('periodic_application_backup_changed');
  return true;
}

export function inspectPeriodicApplicationRestore(token,{context,proof,recoveryPointId,checkpointDigest}={}){
  const plan=plans.get(token);
  if(!plan||plan.context!==context||plan.proof!==proof||plan.recovery.pointId!==recoveryPointId||canonical(plan.recovery.manifest.checkpointDigest)!==canonical(checkpointDigest))fail('periodic_application_restore_required');
  const record=postgresBackupContext(context);if(record.closed||record.pendingBinding||record.receiverBinding&&(!plan.backup||record.periodicReceiverBinding!==record.receiverBinding)||record.resumeBinding||record.applicationPlan||record.profile.rootProtectedRuntime)fail('periodic_application_restore_context');
  return {context,proof,source:structuredClone({...plan.source,context:undefined}),recovery:structuredClone({...plan.recovery,context:undefined}),...(record.scope==='existing-cluster'?{installedPolicy:plan.backup?null:plan.policy,archivedBackup:!!plan.backup}:{})};
}

export async function restorePeriodicApplication({session,token,destination,signal}={}){
  const live=inspectPostgresBackupRestoreSession(session),{source}=inspectPeriodicApplicationRestore(token,{...live}),record=postgresBackupContext(live.context);
  if(destination!==join(dirname(live.connection.host),'application'))fail('periodic_application_restore_path');
  await mkdir(destination,{mode:0o700});
  await restoreFileSet({directory:source.directory,destination,key:periodicApplicationKey(record),maxTotalBytes:source.configuration.maxBytes,expectedManifestDigest:source.manifest.files.manifestDigest,signal});
  const store=new PostgresStore({connection:{...live.connection,database:source.configuration.database.name},poolSize:2});let master,recording;
  try{
    const identity=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid,(SELECT system_identifier::text FROM pg_control_system()) system_id,pg_is_in_recovery() recovering');
    if(identity.name!==source.configuration.database.name||identity.oid!==source.configuration.database.oid||identity.system_id!==record.expected.system_id||identity.recovering)fail('periodic_application_restored_database');
    await store.verifySchema();master=await keyFile(join(destination,'data','key-encryption.key'));recording=await keyFile(join(destination,'data','recording-encryption.key'));
    await verifyStoredCredentials(store,master);
    const binding=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='recording.key.binding'");
    if(binding){let saved;try{saved=JSON.parse(binding.value);}catch{fail('periodic_application_recording_binding');}if(saved.version!==1||saved.digest!==createHash('sha256').update(Buffer.concat([Buffer.from('harbor-recording-key-binding-v1\0'),recording])).digest('hex'))fail('periodic_application_recording_binding');}
    else if((await store.get('SELECT EXISTS(SELECT 1 FROM harbor.recordings) present')).present)fail('periodic_application_recording_binding');
    const recordingVerification=await verifyRestoredRecordingCiphertexts(store,recording,{signal});if(!recordingVerification.allVerified)fail('periodic_application_recordings_unverified');
    const deviceVerification=await verifyRestoredDeviceMaterials(store,destination,{candidate:source.configuration,hasSecrets:true,signal});
    if(!deviceVerification.existingDeviceTrustVerified||!deviceVerification.serverTlsIdentityVerified||!deviceVerification.reportKeyRestoredVerified||deviceVerification.missing.length)fail('periodic_application_devices_unverified');
    inspectPostgresBackupRestoreSession(session);signal?.throwIfAborted();
    const accountHomeMapping=await mapAccountHomes(store,source.configuration,destination,{signal});
    const {verifyPeriodicApplicationRuntime}=await import('./postgres-periodic-application-runtime.mjs');
    const runtime=await verifyPeriodicApplicationRuntime(session,{token,destination,signal});
    return {...periodicApplicationKeyBinding(record),scope:'periodic-restored-application',captureId:source.captureId,filesRestoredVerified:true,originalStoredCredentialsVerified:true,accountHomeMapping,recordingVerification,deviceVerification,runtime,applicationMaterialsVerified:true,candidateVerified:false,offhostVerified:false,deploymentAllowed:false};
  }finally{master?.fill(0);recording?.fill(0);await store.close();}
}
