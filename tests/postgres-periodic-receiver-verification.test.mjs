import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID,generateKeyPairSync,sign} from 'node:crypto';
import {mkdir,writeFile,readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPostgresBaseBackup,postgresBackupRestoreSource} from '../postgres-backup.mjs';
import {capturePeriodicApplicationBackup,preparePeriodicApplicationHandoff} from '../postgres-periodic-application-backup.mjs';
import {periodicReceiverVerificationPolicy,loadPeriodicReceiverVerificationPolicy,createTaskPeriodicReceiverVerificationPolicy,verifyTaskPeriodicReceiverAttestation,inspectPeriodicReceiverVerification} from '../postgres-periodic-receiver-verification.mjs';
import {canonical,readManifest} from '../migration-artifacts.mjs';

test('periodic receiver source verification defaults off and rejects unknown trust policy fields',()=>{
  assert.deepEqual(periodicReceiverVerificationPolicy({version:1,enabled:false}),{version:1,enabled:false});
  const value={version:1,enabled:true,receiverMachine:'a'.repeat(64),receiverRuntime:{sourceDigest:'b'.repeat(64),nodeDigest:'c'.repeat(64),restoreUid:65534}};
  assert.deepEqual(periodicReceiverVerificationPolicy(value),value);
  for(const bad of [{...value,unknown:true},{...value,publicKey:'caller-selected'},{...value,receiverMachine:'invalid'},{...value,receiverRuntime:{...value.receiverRuntime,restoreUid:0}},{...value,receiverRuntime:{...value.receiverRuntime,unknown:true}}])assert.throws(()=>periodicReceiverVerificationPolicy(bad),{code:'periodic_receiver_verification_policy_invalid'});
});
test('copied contexts and qualification DTOs cannot grant source or offhost authority',async()=>{
  assert.throws(()=>createTaskPeriodicReceiverVerificationPolicy({scope:'task-owned'},{version:1,enabled:false}),{code:'backup_context_required'});
  await assert.rejects(inspectPeriodicReceiverVerification({scope:'approved-periodic-offhost-recovery'}),{code:'periodic_receiver_verification_qualification_required'});
  if(process.getuid()!==0)await assert.rejects(loadPeriodicReceiverVerificationPolicy(),{code:'periodic_receiver_verification_root_policy_required'});
});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('a real opaque point accepts only a diagnostic signed claim and never upgrades it to root/offhost or refreshes checkpoint age',{
  skip:!binDirectory?'需自有PG18工具；不连接共享数据库':false,timeout:180000
},async t=>{
  const archiveKey=randomBytes(32),applicationKey=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:archiveKey}}),store=new PostgresStore({connection:lab.connection});let context,other;
  t.after(async()=>{try{if(context)await closePostgresBackupContext(context);if(other)await closePostgresBackupContext(other);}finally{try{await store.close();}finally{await lab.close();archiveKey.fill(0);applicationKey.fill(0);}}});
  await store.migrate();await store.initializePricing();const database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid'),roots=[];
  for(const id of ['data','runtime','secrets']){const directory=join(lab.directory,'diagnostic-'+id);await mkdir(directory,{mode:0o700});await writeFile(join(directory,'synthetic.txt'),'synthetic protocol fixture',{mode:0o600});roots.push({id,directory});}
  const directory=join(lab.directory,'periodic-backups'),options={laboratory:lab,directory,archiveKey,applicationKey,binDirectory,libraryDirectory};context=await createTaskPostgresBackupContext(options);other=await createTaskPostgresBackupContext(options);
  const proof=await createPostgresBaseBackup(context),policy={version:1,enabled:true,database,materialRoots:roots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:4096,maxFiles:20},captured=await capturePeriodicApplicationBackup(context,proof,policy);assert.equal(captured.backupComplete,true,captured.failureCode);
  const handoff=await preparePeriodicApplicationHandoff(context,proof,captured.pointId),record=postgresBackupContext(context),point=join(directory,proof.backupId,'points',captured.pointId),manifest=await readManifest(join(point,'handoff','handoff.enc'),applicationKey),runtime={sourceDigest:null,nodeDigest:null,restoreUid:process.getuid(),crossUidExecutionVerified:false,publisherUidVerified:false};
  const {createHash}=await import('node:crypto'),hash=value=>createHash('sha256').update(canonical(value)).digest('hex'),{privateKey,publicKey}=generateKeyPairSync('ed25519');
  // Deliberately only a signed task claim, without running a receiver. The
  // consumer must retain diagnostic scope and cannot certify a real restore.
  const payload={version:1,purpose:'harbor-storage-receiver-periodic-v1',scope:'task-owned-receiver',kind:'periodic-application',backupId:proof.backupId,pointId:captured.pointId,sourceMachine:manifest.sourceMachine,receiverMachine:manifest.sourceMachine,configurationDigest:record.configurationDigest,clusterDigest:hash(record.expected.system_id),handoffDigest:handoff.handoffDigest,checkpointDigest:manifest.checkpointDigest,backupCompleteDigest:manifest.backupCompleteDigest,applicationPolicyDigest:manifest.applicationPolicyDigest,applicationKeyVersion:record.applicationKeyVersion,applicationKeyDigest:record.applicationKeyDigest,resultDigest:'d'.repeat(64),runtime,checks:{sourceFreeRestoreVerified:true,receiverRestoreVerified:true,recoveryExitedVerified:true,inventoryVerified:true,writeReadbackVerified:true,applicationMaterialsVerified:true,offhostDatabaseVerified:false,offhostDatabaseAndFilesVerified:false,deploymentAllowed:false}},claim={payload,signature:sign(null,Buffer.from('harbor-storage-receiver-periodic-attestation-v1\0'+canonical(payload)),privateKey).toString('base64')};
  const trust=createTaskPeriodicReceiverVerificationPolicy(context,{version:1,enabled:true,receiverMachine:manifest.sourceMachine,receiverRuntime:{sourceDigest:null,nodeDigest:null,restoreUid:process.getuid()}},publicKey),disabled=createTaskPeriodicReceiverVerificationPolicy(context,{version:1,enabled:false});
  assert.equal((await verifyTaskPeriodicReceiverAttestation(context,null,null,null,disabled)).state,'periodic_receiver_verification_disabled');
  await assert.rejects(verifyTaskPeriodicReceiverAttestation(other,proof,captured.pointId,claim,trust),{code:'periodic_receiver_verification_policy_required'});
  await assert.rejects(verifyTaskPeriodicReceiverAttestation(context,proof,captured.pointId,claim,{...trust}),{code:'periodic_receiver_verification_policy_required'});
  await assert.rejects(verifyTaskPeriodicReceiverAttestation(context,proof,captured.pointId,claim,trust,{signal:AbortSignal.abort()}),{name:'AbortError'});
  const qualification=await verifyTaskPeriodicReceiverAttestation(context,proof,captured.pointId,claim,trust),observed=await inspectPeriodicReceiverVerification(qualification),output=join(point,'periodic-receiver-diagnostic.enc'),original=await readFile(output);
  assert.equal(observed.scope,'task-periodic-receiver-diagnostic');assert.equal(observed.signatureVerified,true);assert.equal(observed.applicationMaterialsVerified,false);assert.equal(observed.offhostApplicationRestoreVerified,false);assert.equal(observed.offhostVerified,false);assert.equal(observed.deploymentAllowed,false);assert.equal(observed.checkpointAt,captured.checkpointAt);
  await assert.rejects(readFile(join(point,'periodic-receiver.enc')),{code:'ENOENT'});await assert.rejects(readManifest(output,archiveKey),{code:'transfer_manifest_authentication_failed'});
  const {authenticatePeriodicApplicationRetentionGroup,periodicApplicationRetentionReferences}=await import('../postgres-periodic-application-retention.mjs'),group=await authenticatePeriodicApplicationRetentionGroup(record,join(directory,proof.backupId),postgresBackupRestoreSource(proof).metadata);assert.ok(group.allowed.has('points/'+captured.pointId+'/periodic-receiver-diagnostic.enc'));assert.equal(group.verifiedPoints.length,0,'diagnostic receiver records never count as verified application points');
  const controls=await periodicApplicationRetentionReferences(record,join(directory,proof.backupId),postgresBackupRestoreSource(proof).metadata);assert.ok(controls.files.some(row=>row.name.endsWith('/periodic-receiver-diagnostic.enc')));
  const repeated=await inspectPeriodicReceiverVerification(await verifyTaskPeriodicReceiverAttestation(context,proof,captured.pointId,claim,trust));assert.equal(repeated.acceptedAt,observed.acceptedAt);assert.equal(repeated.checkpointAt,observed.checkpointAt);assert.deepEqual(await readFile(output),original);
  await assert.rejects(inspectPeriodicReceiverVerification({...qualification}),{code:'periodic_receiver_verification_qualification_required'});await assert.rejects(inspectPeriodicReceiverVerification(qualification,{context:other}),{code:'periodic_receiver_verification_qualification_required'});
  const changed=structuredClone(claim);changed.payload.pointId=randomUUID();await assert.rejects(verifyTaskPeriodicReceiverAttestation(context,proof,captured.pointId,changed,trust),{code:'periodic_receiver_attestation_signature'});
  changed.signature=sign(null,Buffer.from('harbor-storage-receiver-periodic-attestation-v1\0'+canonical(changed.payload)),privateKey).toString('base64');await assert.rejects(verifyTaskPeriodicReceiverAttestation(context,proof,captured.pointId,changed,trust),{code:'periodic_receiver_attestation_binding'});
  const corrupt=Buffer.from(original);corrupt[25]^=1;await writeFile(output,corrupt,{mode:0o600});await assert.rejects(inspectPeriodicReceiverVerification(qualification),{code:'transfer_manifest_authentication_failed'});await writeFile(output,original,{mode:0o600});
  await closePostgresBackupContext(context);context=null;assert.ok(record.applicationKey.every(byte=>byte===0));await assert.rejects(inspectPeriodicReceiverVerification(qualification),{code:'backup_context_closed'});
});
