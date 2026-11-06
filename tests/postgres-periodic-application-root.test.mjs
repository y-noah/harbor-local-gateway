import {generateKeyPairSync} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import test from 'node:test';
import assert from 'node:assert/strict';
import {postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPostgresBaseBackup,checkpointPostgresBackup} from '../postgres-backup.mjs';
import {capturePeriodicApplicationMaterials} from '../postgres-periodic-application-materials.mjs';
import {capturePostgresBackupRecoveryPoint} from '../postgres-backup-recovery-point.mjs';
import {preparePeriodicApplicationRestore} from '../postgres-periodic-application-restore.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from '../postgres-backup-restore.mjs';
import {periodicApplicationPolicy,loadPeriodicApplicationPolicy,verifyInstalledPeriodicApplicationPolicy} from '../postgres-periodic-application-policy.mjs';
import {storageBackupCommand} from '../storage-backup.mjs';
import {storageReceiverCommand} from '../storage-receiver.mjs';

test('plain policy descriptions cannot grant fixed-root application runtime authority',async()=>{
  await assert.rejects(verifyInstalledPeriodicApplicationPolicy(periodicApplicationPolicy({version:1,enabled:false})),{code:'periodic_application_installed_policy_required'});
});

test('isolated fixed-root application point restores through real service UID gateway, worker and publisher',{
  skip:process.env.HARBOR_PERIODIC_ROOT_TEST!=='1'?'仅自有mount namespace中实际root验收，不读取宿主固定配置':false,timeout:240000
},async t=>{
  assert.equal(process.getuid(),0,'显式root验收必须真实UID0');
  const {createPeriodicRootFixture}=await import('./periodic-root-fixture.mjs');
  const fixture=await createPeriodicRootFixture({directory:process.env.HARBOR_PERIODIC_ROOT_DIRECTORY,pgBinDirectory:process.env.HARBOR_CAPACITY_PG_BIN,pgLibraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB,nodeFile:process.execPath,sourceDirectory:new URL('..',import.meta.url).pathname,uid:65534,gid:65534});
  t.after(()=>fixture.close());
  assert.equal(fixture.context.scope,'existing-cluster');
  const {readFile}=await import('node:fs/promises'),{createPostgresBackupContext}=await import('../postgres-backup-context.mjs'),recordKey=postgresBackupContext(fixture.context),originalProfile=await readFile('/etc/harbor-storage-backup.json'),profile=JSON.parse(originalProfile);
  assert.ok(recordKey.applicationKey);assert.notDeepEqual(recordKey.applicationKey,recordKey.key);
  const {writeFile:writeProfile}=await import('node:fs/promises');try{await writeProfile('/etc/harbor-storage-backup.json',JSON.stringify({...profile,applicationEncryption:{...profile.applicationEncryption,keyDigest:'0'.repeat(64)}}),{mode:0o600});await assert.rejects(createPostgresBackupContext(),{code:'periodic_application_key_digest_mismatch'});}finally{await writeProfile('/etc/harbor-storage-backup.json',originalProfile,{mode:0o600});}
  const before=await fixture.store.all('SELECT id,home FROM harbor.accounts ORDER BY id'),proof=await createPostgresBaseBackup(fixture.context),capture=await capturePeriodicApplicationMaterials(fixture.context,proof,fixture.policy);
  await assert.rejects(capturePeriodicApplicationMaterials(fixture.context,proof,{...fixture.policy}),{code:'periodic_application_installed_policy_required'});
  await checkpointPostgresBackup(proof);const point=await capturePostgresBackupRecoveryPoint(fixture.context,proof,{pointId:capture.captureId,applicationCapture:capture}),plan=await preparePeriodicApplicationRestore(fixture.context,proof,fixture.policy,capture,point);
  const restored=inspectPostgresBackupRestore(await restorePostgresBackup(proof,{recoveryPoint:point,periodicApplication:plan})),application=restored.application,runtime=application.runtime;
  assert.equal(application.applicationMaterialsVerified,true);assert.equal(runtime.scope,'installed-restored-periodic-source-runtime');
  for(const name of ['crossUidExecutionVerified','publisherUidVerified','gatewayStartedVerified','independentWorkerStartedVerified','workerProgressVerified','originalKeyDatabaseVerified','restoredTlsServerVerified','freshRestoredDatabaseReportVerified','upstreamCallsDisabled'])assert.equal(runtime[name],true,name);
  assert.equal(runtime.restoreUid,65534);assert.ok(Object.values(runtime.cleanup).every(value=>value===true));assert.match(runtime.sourceDigest,/^[a-f0-9]{64}$/);assert.match(runtime.nodeDigest,/^[a-f0-9]{64}$/);
  assert.equal(application.candidateVerified,false);assert.equal(application.offhostVerified,false);assert.equal(application.deploymentAllowed,false);
  assert.deepEqual(await fixture.store.all('SELECT id,home FROM harbor.accounts ORDER BY id'),before);
  assert.equal((await fixture.store.get("SELECT COUNT(*)::text n FROM pg_roles WHERE rolname LIKE 'harbor_periodic_%'")).n,'0');
  const {readdir}=await import('node:fs/promises');assert.deepEqual(await readdir(postgresBackupContext(fixture.context).restoreDirectory),[]);
  const {writeFile,rename}=await import('node:fs/promises');
  await writeFile('/etc/harbor-storage-schedule.json',JSON.stringify({version:1,enabled:true,reuseAuthenticatedBase:true,fullIntervalMs:86400000,checkpointIntervalMs:1000,pollIntervalMs:1000,pinnedBackupIds:[]}),{flag:'wx',mode:0o600});
  const maintained=await storageBackupCommand(['maintain-base']);assert.equal(maintained.state,'base_candidate_prepared');
  const {join:joinReceipt}=await import('node:path'),{readManifest:readReceipt}=await import('../migration-artifacts.mjs'),{lstat:statReceipt,unlink:removeReceipt}=await import('node:fs/promises'),receiptPath=joinReceipt(recordKey.artifactDirectory,maintained.backupId,'base-verification','current.enc'),receipt=await readReceipt(receiptPath,recordKey.applicationKey),receiptBytes=await readFile(receiptPath),receiptInfo=await statReceipt(receiptPath);
  assert.equal(receiptInfo.uid,0);assert.equal(receiptInfo.mode&0o777,0o600);assert.equal(receiptInfo.nlink,1);
  const scheduleBefore=await readReceipt(joinReceipt(recordKey.artifactDirectory,'periodic-state.enc'),recordKey.key);assert.equal(scheduleBefore.active,null);assert.equal(scheduleBefore.pending.backupId,maintained.backupId);
  // A fresh CLI process loads the fixed root profile and independently holds
  // both keys. It can reuse only the authenticated source receipt, preserving
  // the original full verification time rather than refreshing it on restart.
  const coldScript=[
    'import {createPostgresBackupContext,closePostgresBackupContext} from '+JSON.stringify(new URL('../postgres-backup-context.mjs',import.meta.url).href)+';',
    'import {loadAuthenticatedPeriodicBase,postgresBackupRestoreSource} from '+JSON.stringify(new URL('../postgres-backup.mjs',import.meta.url).href)+';',
    'let context;try{context=await createPostgresBackupContext();const proof=await loadAuthenticatedPeriodicBase(context,'+JSON.stringify(maintained.backupId)+');process.stdout.write(JSON.stringify({scope:proof.scope,verifiedAt:postgresBackupRestoreSource(proof).verifiedAt}));}catch(error){process.stderr.write(error.code??"unexpected_error");process.exitCode=1;}finally{if(context)await closePostgresBackupContext(context);}'
  ].join('\n'),cold=()=>promisify(execFile)(process.execPath,['--input-type=module','--eval',coldScript],{timeout:30000,maxBuffer:4096,env:{PATH:'/usr/bin:/bin'}});
  const coldResult=JSON.parse((await cold()).stdout);assert.equal(coldResult.scope,'existing-cluster');assert.equal(coldResult.verifiedAt,receipt.verifiedAt);
  try{await removeReceipt(receiptPath);await assert.rejects(cold(),error=>error.code===1&&error.stderr==='ENOENT');}
  finally{await writeFile(receiptPath,receiptBytes,{mode:0o600});}
  try{const corrupted=Buffer.from(receiptBytes);corrupted[corrupted.length-1]^=1;await writeFile(receiptPath,corrupted,{mode:0o600});await assert.rejects(cold(),error=>error.code===1&&error.stderr==='transfer_manifest_authentication_failed');}
  finally{await writeFile(receiptPath,receiptBytes,{mode:0o600});}
  assert.equal(JSON.parse((await cold()).stdout).verifiedAt,receipt.verifiedAt);
  const controller=new AbortController(),cycles=[];
  const result=await storageBackupCommand(['schedule'],{signal:controller.signal,onScheduleCycle:cycle=>{cycles.push(cycle);if(cycles.length===2)controller.abort();}});
  assert.equal(result.state,'schedule_stopped');assert.equal(cycles.length,2);assert.equal(cycles[0].backupId,cycles[1].backupId,'actual fixed-root schedule reuses its physical base');
  for(const cycle of cycles){assert.equal(cycle.scope,'database-and-application');assert.equal(cycle.application.state,'application_backup_completed');assert.equal(cycle.application.backupComplete,true);assert.equal(cycle.application.restoreVerified,false);assert.equal(cycle.application.applicationMaterialsVerified,false);}
  assert.notEqual(cycles[0].application.pointId,cycles[1].application.pointId);
  const scheduleAfter=await readReceipt(joinReceipt(recordKey.artifactDirectory,'periodic-state.enc'),recordKey.key);assert.equal(scheduleAfter.active.backupId,maintained.backupId);assert.equal(scheduleAfter.pending,null);
  for(const cycle of cycles){const complete=await readReceipt(joinReceipt(recordKey.artifactDirectory,cycle.backupId,'points',cycle.application.pointId,'backup-complete.enc'),recordKey.applicationKey);assert.equal(complete.baseVerifiedAt,receipt.verifiedAt,'fixed-root producer inherits the genuine base verification time');}
  const verified=await storageBackupCommand(['verify-point',cycles[0].backupId,cycles[0].application.pointId]);assert.equal(verified.state,'application_point_verified');assert.equal(verified.applicationMaterialsVerified,true);
  assert.deepEqual(await fixture.store.all('SELECT id,home FROM harbor.accounts ORDER BY id'),before);
  assert.deepEqual(await readdir(postgresBackupContext(fixture.context).restoreDirectory),[]);
  await rename('/etc/harbor-storage-application.json','/etc/harbor-storage-application.saved');
  try{const historical=await storageBackupCommand(['verify-point',cycles[1].backupId,cycles[1].application.pointId]);assert.equal(historical.state,'application_point_verified');assert.equal(historical.applicationMaterialsVerified,true);
    const handoff=await storageBackupCommand(['periodic-handoff',cycles[1].backupId,cycles[1].application.pointId]),record=postgresBackupContext(fixture.context),{mkdir,chown}=await import('node:fs/promises'),{join}=await import('node:path'),{readManifest}=await import('../migration-artifacts.mjs');
    assert.equal(handoff.kind,'periodic-application');const bundleDirectory=join(record.artifactDirectory,handoff.backupId,'points',handoff.pointId,'handoff'),manifest=await readManifest(join(bundleDirectory,'handoff.enc'),record.applicationKey),workDirectory=join(fixture.directory,'w'),recoveryDirectory=join(fixture.directory,'r');
    assert.ok(Buffer.byteLength(join(recoveryDirectory,'r-'+'0'.repeat(12),'r','0'.repeat(12),'socket','.s.PGSQL.55479'))<108,'synthetic receiver paths must fit the real Unix socket budget');
    await mkdir(workDirectory,{mode:0o700});await mkdir(recoveryDirectory,{mode:0o700});await chown(recoveryDirectory,65534,65534);
    await writeFile('/etc/harbor-storage-receiver.json',JSON.stringify({version:1,bundleDirectory,workDirectory,recoveryDirectory,keyFile:record.archiveKeyFile,keyVersion:record.keyVersion,sourceMachine:manifest.sourceMachine,expectedCluster:record.expected.system_id,handoffDigest:handoff.handoffDigest,handoffKind:'periodic-application',maxBundleBytes:record.profile.maxBackupBytes*2,maxFiles:100000,applicationEncryption:record.applicationEncryption,restoreUid:65534,restoreGid:65534,pgBinDirectory:record.profile.pgBinDirectory,...(record.profile.libraryDirectory?{libraryDirectory:record.profile.libraryDirectory}:{}),processTimeoutMs:record.profile.processTimeoutMs}),{flag:'wx',mode:0o600});
    const {privateKey,publicKey}=generateKeyPairSync('ed25519');await writeFile('/etc/harbor-storage-receiver-signing.key',privateKey.export({type:'pkcs8',format:'pem'}),{flag:'wx',mode:0o600});
    const attestation=await storageReceiverCommand(['restore-and-attest-periodic']),{version:unusedVersion,purpose:unusedPurpose,scope:unusedScope,resultDigest:unusedResult,checks:unusedChecks,...expected}=attestation.payload,{verifyStorageReceiverPeriodicAttestation,verifyOffhostStorageReceiverPeriodicAttestation}=await import('../storage-receiver-periodic-attestation.mjs'),received=verifyStorageReceiverPeriodicAttestation(attestation,publicKey,{expected});
    assert.equal(received.kind,'periodic-application');assert.equal(received.pointId,handoff.pointId);assert.equal(received.scope,'root-profile-receiver');assert.equal(received.checks.sourceFreeRestoreVerified,true);assert.equal(received.checks.applicationMaterialsVerified,true);assert.equal(received.runtime.crossUidExecutionVerified,true);assert.equal(received.runtime.publisherUidVerified,true);assert.equal(received.runtime.restoreUid,65534);assert.equal(received.sourceMachine,received.receiverMachine);assert.equal(received.checks.offhostDatabaseVerified,false);assert.equal(received.checks.deploymentAllowed,false);assert.throws(()=>verifyOffhostStorageReceiverPeriodicAttestation(attestation,publicKey,{expected}),{code:'periodic_receiver_attestation_offhost_required'});
    assert.deepEqual(await readdir(workDirectory),[]);assert.deepEqual(await readdir(recoveryDirectory),[]);assert.deepEqual(await loadPeriodicApplicationPolicy(),{version:1,enabled:false});
    assert.equal((await storageBackupCommand(['verify-periodic-receiver',handoff.backupId,handoff.pointId])).state,'periodic_receiver_verification_disabled');
    const {receiverPublisherSourceManifest}=await import('../storage-receiver-candidate.mjs'),{canonical,digestFile}=await import('../migration-artifacts.mjs'),{createHash}=await import('node:crypto'),{backupMachineIdentity}=await import('../postgres-backup-handoff.mjs');
    const approvedRuntime={sourceDigest:createHash('sha256').update(canonical(await receiverPublisherSourceManifest(fixture.sourceDirectory))).digest('hex'),nodeDigest:(await digestFile(process.execPath)).sha256,restoreUid:65534};
    await writeFile('/etc/harbor-storage-receiver.pub',publicKey.export({type:'spki',format:'pem'}),{flag:'wx',mode:0o600});
    await writeFile('/etc/harbor-storage-periodic-receiver.json',JSON.stringify({version:1,enabled:true,receiverMachine:await backupMachineIdentity(),receiverRuntime:approvedRuntime}),{flag:'wx',mode:0o600});
    await writeFile(join(record.artifactDirectory,handoff.backupId,'points',handoff.pointId,'receiver-attestation.json'),JSON.stringify(attestation),{flag:'wx',mode:0o600});
    await assert.rejects(storageBackupCommand(['verify-periodic-receiver',handoff.backupId,handoff.pointId]),{code:'periodic_receiver_attestation_offhost_required'});
    await assert.rejects(readFile(join(record.artifactDirectory,handoff.backupId,'points',handoff.pointId,'periodic-receiver.enc')),{code:'ENOENT'});
await assert.rejects(verifyInstalledPeriodicApplicationPolicy(fixture.policy),{code:'periodic_application_installed_policy_changed'});}
  finally{await rename('/etc/harbor-storage-application.saved','/etc/harbor-storage-application.json');}
});
