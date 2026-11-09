import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID,createHash,generateKeyPairSync} from 'node:crypto';
import {mkdir,writeFile,readFile,readdir,symlink} from 'node:fs/promises';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {encryptStoredKey,hash} from '../store.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext} from '../postgres-backup-context.mjs';
import {createPostgresBaseBackup,checkpointPostgresBackup} from '../postgres-backup.mjs';
import {capturePeriodicApplicationMaterials,verifyPeriodicApplicationCapture} from '../postgres-periodic-application-materials.mjs';
import {capturePostgresBackupRecoveryPoint,verifyPostgresBackupRecoveryPoint} from '../postgres-backup-recovery-point.mjs';
import {preparePeriodicApplicationRestore,inspectPeriodicApplicationRestore} from '../postgres-periodic-application-restore.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from '../postgres-backup-restore.mjs';
import {certificateFixture} from './device-fixture.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {recordingQuery} from '../postgres-recording-query.mjs';
import {createPostgresRecordingContextQueries} from '../postgres-recording-context-query.mjs';
import {postgresBackupContext} from '../postgres-backup-context.mjs';
import {createRecordingIndexCrypto} from '../recording-search-index.mjs';
import {runPeriodicApplicationCycle} from '../postgres-periodic-application-cycle.mjs';
import {readManifest,writeManifest} from '../migration-artifacts.mjs';
import {verifyPostgresBackup} from '../postgres-backup.mjs';
import fs from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
import {runPostgresBackupScheduleCycle} from '../postgres-backup-schedule.mjs';
import {setTimeout as delay} from 'node:timers/promises';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('copied application restore descriptions never confer restore capability',()=>{
  assert.throws(()=>inspectPeriodicApplicationRestore({backupId:randomUUID(),pointId:randomUUID()}),{code:'periodic_application_restore_required'});
});

for(const withContext of [false,true])test('private periodic application point restores original encrypted credentials, '+(withContext?'shared context/index/export dependencies':'four ciphertext forms')+', TLS and actual gateway/worker',{skip:!binDirectory?'需自有PG18，不连接共享环境':false,timeout:240000},async t=>{
  const backupKey=randomBytes(32),applicationKey=randomBytes(32),master=randomBytes(32),recording=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey}}),store=new PostgresStore({connection:lab.connection});let context,producerContext,receiver,storeClosed=false,ca,releaseRestore;const originalRename=fs.promises.rename,originalVerifySchema=PostgresStore.prototype.verifySchema;let restoredContextReads=0;
  t.after(async()=>{releaseRestore?.();PostgresStore.prototype.verifySchema=originalVerifySchema;fs.promises.rename=originalRename;syncBuiltinESMExports();try{if(receiver)await (await import('../postgres-backup-receiver.mjs')).closePostgresBackupReceiver(receiver);ca?.issuer.close();if(producerContext)await closePostgresBackupContext(producerContext);if(context)await closePostgresBackupContext(context);}finally{try{if(!storeClosed)await store.close();}finally{await lab.close();backupKey.fill(0);applicationKey.fill(0);master.fill(0);recording.fill(0);}}});
  await store.migrate({recordingContext:withContext,recordingContextIndex:withContext});await store.initializePricing();const member=(await createPostgresMemberAdmin(store,{encryptionKey:master}).handle('/api/admin/members','POST',{name:'周期恢复合成员工',expires:null,quota:null,allowed_models:null})).value;
  const materialRoots=[];for(const id of ['data','runtime','secrets']){const directory=join(lab.directory,'application-'+id);await mkdir(directory,{mode:0o700});materialRoots.push({id,directory});}
  const data=materialRoots[0].directory,runtime=materialRoots[1].directory,secrets=materialRoots[2].directory;await mkdir(join(data,'recording-spool'),{mode:0o700});await mkdir(join(secrets,'device'),{mode:0o700});
  await writeFile(join(data,'settings.json'),JSON.stringify({port:8092,codexBinary:process.execPath,adminPasswordHash:{salt:'a'.repeat(32),digest:'b'.repeat(64)}}),{mode:0o600});
  for(const [name,key] of [['key-encryption.key',master],['recording-encryption.key',recording]])await writeFile(join(data,name),key,{mode:0o600});
  ca=await certificateFixture(join(secrets,'device'));for(const [name,value] of [['ca.pem',ca.ca],['server.pem',ca.cert],['server.key',ca.key],['ca.key',await readFile(join(secrets,'device','ca-key.pem'))]])await writeFile(join(secrets,'device',name),value,{mode:0o600});
  await writeFile(join(secrets,'postgres-report.key'),randomBytes(32),{mode:0o600});await writeFile(join(secrets,'postgres-ca.crt'),ca.ca,{mode:0o600});
  const account='synthetic-'+randomUUID(),upstream='synthetic-upstream-'+randomUUID(),databaseHome='/synthetic-container/runtime';materialRoots[1].databasePrefix=databaseHome;
  await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,api_secret_cipher,api_secret_digest) VALUES($1,'合成离线上游',$2,'ready','openai_api',$3,$4)",account,databaseHome,encryptStoredKey(master,upstream,'upstream:'+account),hash(upstream));
  const actor=await store.authenticate(member.secret),requestId=await store.observe(actor,'synthetic','synthetic','gpt-6.1-sol'),records=new RecordingStore(store,{encryptionKey:recording}),crypto=createRecordingIndexCrypto(recording);
  try{
    await records.start(requestId,{turnState:'known'});await records.append(requestId,[{sourceKey:'synthetic-user',kind:'user',provenance:'client',content:{text:'synthetic original periodic body'}}]);await records.finish(requestId);
    const revision=(await store.get('SELECT revision::text revision FROM harbor.recording_work WHERE request_id=$1',requestId)).revision,at=Date.now(),part={kind:'user',field:'synthetic-field',text:'synthetic derived body',searchText:'synthetic derived body'},bytes=Buffer.byteLength(JSON.stringify(part));
    await store.run("INSERT INTO harbor.recording_documents VALUES($1,$2,1,$3,'complete','known',$4,1,$5,$6)",requestId,revision,at,at+86400000,bytes,crypto.keyVersion);await store.run("INSERT INTO harbor.recording_document_parts VALUES($1,1,'user','synthetic-field',$2,$3,'{}')",requestId,crypto.seal(part,'document:'+requestId+':'+revision+':1'),bytes);
    const exportId=randomUUID(),text='synthetic encrypted export';await store.run("INSERT INTO harbor.background_jobs(id,kind,status,available_at,created,updated) VALUES($1,'recording.export','completed',$2,$2,$2)",exportId,at);await store.run("INSERT INTO harbor.recording_exports(id,created,expires,actor,options,visibility,status,rows,bytes,chunks) VALUES($1,$2,$3,'synthetic','{}',pg_current_snapshot(),'complete',1,$4,1)",exportId,at,at+86400000,Buffer.byteLength(text));await store.run('INSERT INTO harbor.recording_export_chunks VALUES($1,1,$2,$3)',exportId,Buffer.byteLength(text),crypto.seal({text},'export:'+exportId+':1'));
  }finally{records.key.fill(0);}
  if(withContext){
    const sessionId=randomUUID();await store.run('INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) VALUES($1,$2,$3,$4,$4)',sessionId,member.memberId,account,Date.now());await store.run('UPDATE harbor.requests SET session_id=$1 WHERE id=$2',sessionId,requestId);
    const source=new RecordingStore(store,{encryptionKey:recording}),contexts=new RecordingContextStore(store,{encryptionKey:recording}),shared=[{kind:'history',content:'synthetic shared baseline'},{kind:'history',content:'synthetic shared baseline'}];
    await contexts.writeSnapshot(requestId,shared);const followup=await store.observe(actor,'synthetic','synthetic','gpt-6.1-sol');await store.run('UPDATE harbor.requests SET session_id=$1 WHERE id=$2',sessionId,followup);await source.start(followup);await contexts.writeSnapshot(followup,shared);source.key.fill(0);
    await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',Date.now()-1,requestId);await contexts.gc();assert.equal((await contexts.read(requestId)).state,'expired');
    const jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey:recording,contextIndex:true,storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});await backend.schedule();let task;while(task=await jobs.claim(['recording.context.index']))await jobs.complete(task,await backend.handlers[task.kind](task));
    const exported=await backend.exports.enqueue({include_body:true,format:'jsonl',filters:{session:sessionId}});while(task=await jobs.claim(['recording.export']))await jobs.complete(task,await backend.handlers[task.kind](task));
    contexts.key.fill(0);contexts.fingerprintKey.fill(0);
    // Observe the actual isolated restore connection, without substituting its
    // verification or creating new authority/credentials. Source remains read-only.
    PostgresStore.prototype.verifySchema=async function(){
      const result=await originalVerifySchema.call(this);if(!context)return result;
      const directory=(await this.get("SELECT current_setting('data_directory') directory")).directory;
      if(!directory.startsWith(postgresBackupContext(context).restoreDirectory+'/'))return result;
      const restored=new RecordingContextStore(this,{encryptionKey:recording});try{
        assert.equal((await restored.read(requestId)).state,'expired');const page=await restored.read(followup);assert.equal(page.state,'complete');assert.equal(page.parts.length,2);assert.ok(page.parts.every(part=>JSON.parse(part.text)==='synthetic shared baseline'));
        const found=await createPostgresRecordingContextQueries(this,{crypto:createRecordingIndexCrypto(recording)}).search(recordingQuery(new URLSearchParams({q:'synthetic shared baseline',member:member.memberId}),{search:true}));assert.equal(found.items.length,1);assert.equal(found.items[0].request_id,followup);
        assert.equal((await this.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects WHERE export_id=$1',exported.id)).n,1);const chunks=await this.all('SELECT ordinal,payload_cipher FROM harbor.recording_export_chunks WHERE export_id=$1 ORDER BY ordinal',exported.id),derived=createRecordingIndexCrypto(recording),lines=chunks.flatMap(chunk=>derived.open(chunk.payload_cipher,`export:${exported.id}:${chunk.ordinal}`).text.trim().split('\n').map(line=>JSON.parse(line)));assert.equal(lines.filter(line=>line.kind==='context_object').length,1);assert.equal(lines.filter(line=>line.kind==='context_reference').length,2);restoredContextReads++;
      }finally{restored.key.fill(0);restored.fingerprintKey.fill(0);}return result;
    };
  }
  await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('recording.key.binding',$1)",JSON.stringify({version:1,digest:createHash('sha256').update(Buffer.concat([Buffer.from('harbor-recording-key-binding-v1\0'),recording])).digest('hex')}));
  const launcher=join(runtime,'accounts','synthetic','tmp','arg0');await mkdir(launcher,{recursive:true,mode:0o700});await symlink(join(data,'key-encryption.key'),join(launcher,'apply_patch'));
  const database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid'),policy={version:1,enabled:true,database,materialRoots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:16777216,maxFiles:100,runtimeRebuildableCaches:['codex-launcher-aliases']};
  context=await createTaskPostgresBackupContext({laboratory:lab,directory:join(lab.directory,'periodic-backups'),archiveKey:backupKey,applicationKey,binDirectory,libraryDirectory});
  const proof=await createPostgresBaseBackup(context);await checkpointPostgresBackup(proof);
  await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',randomUUID(),'必须出现在capture之后checkpoint里的合成行',Date.now());
  const capture=await capturePeriodicApplicationMaterials(context,proof,policy);
  await assert.rejects(capturePostgresBackupRecoveryPoint(context,proof,{pointId:capture.captureId,applicationCapture:capture}),{code:'backup_recovery_point_application_fence'});
  await checkpointPostgresBackup(proof);const point=await capturePostgresBackupRecoveryPoint(context,proof,{pointId:capture.captureId,applicationCapture:capture});
  const plan=await preparePeriodicApplicationRestore(context,proof,policy,capture,point);
  await assert.rejects(restorePostgresBackup(proof,{recoveryPoint:point,periodicApplication:{...plan}}),{code:'periodic_application_restore_required'});
  const receipt=await restorePostgresBackup(proof,{recoveryPoint:point,periodicApplication:plan}),summary=inspectPostgresBackupRestore(receipt),application=summary.application;
  assert.equal(summary.restoreVerified,true);assert.equal(application.applicationMaterialsVerified,true);assert.equal(application.recordingVerification.verified,withContext?7:4);if(withContext){assert.ok(restoredContextReads>0);assert.equal(application.recordingVerification.contexts.objects,1);assert.equal(application.recordingVerification.contexts.references,2);assert.equal(application.recordingVerification.contexts.documents,1);assert.equal(application.recordingVerification.contexts.exportDependencies,1);}
  assert.deepEqual(application.accountHomeMapping,{verified:true,accounts:1});
  const memberTable=(await store.get("SELECT 'harbor.members'::regclass::oid::text oid")).oid;assert.equal(summary.tableCounts.find(row=>row.tableOid===memberTable).rows,'2','a checkpoint after capture includes the new business row');
  for(const name of ['gatewayStartedVerified','independentWorkerStartedVerified','workerProgressVerified','originalKeyDatabaseVerified','oldKeyHttpVerified','restoredTlsServerVerified','freshRestoredDatabaseReportVerified','upstreamCallsDisabled'])assert.equal(application.runtime[name],true,name);
  assert.ok(Object.values(application.runtime.cleanup).every(value=>value===true));assert.equal(application.offhostVerified,false);assert.equal(application.deploymentAllowed,false);assert.equal(summary.applicationMaterialsVerified,false,'database receipt never upgrades its old scope');
  assert.equal((await store.authenticate(member.secret)).id,member.memberId);assert.equal((await store.get("SELECT COUNT(*)::text n FROM pg_roles WHERE rolname LIKE 'harbor_periodic_%'")).n,'0','temporary role only exists in discarded restored clone');
  assert.equal((await store.get('SELECT home FROM harbor.accounts WHERE id=$1',account)).home,databaseHome,'original database home is never rewritten');
  assert.deepEqual(await readdir(join(lab.directory,'backup-restore')),[]);
  const {capturePeriodicApplicationBackup,verifyPeriodicApplicationBackup,authenticatePeriodicApplicationBackup,periodicApplicationBackupSource,readPeriodicApplicationBackupState}=await import('../postgres-periodic-application-backup.mjs');
  const record=postgresBackupContext(context),first=await capturePeriodicApplicationBackup(context,proof,policy);
  assert.equal(first.state,'application_backup_completed',first.failureCode);assert.equal(first.backupComplete,true);assert.equal(first.restoreVerified,false);assert.equal(first.applicationMaterialsVerified,false);
  const firstDirectory=join(record.artifactDirectory,proof.backupId,'points',first.pointId);
  await assert.rejects(readFile(join(firstDirectory,'application-final.enc')),{code:'ENOENT'});
  await assert.rejects(readManifest(join(firstDirectory,'backup-complete.enc'),backupKey),{code:'transfer_manifest_authentication_failed'});
  await assert.rejects(readManifest(join(record.artifactDirectory,proof.backupId,'application-staging',first.pointId,'capture.enc'),backupKey),{code:'transfer_manifest_authentication_failed'});
  const second=await capturePeriodicApplicationBackup(context,proof,policy);assert.equal(second.state,'application_backup_completed',second.failureCode);assert.notEqual(second.pointId,first.pointId);
  assert.deepEqual(await readdir(record.restoreDirectory),[],'producer never restores a base');
  let state=await readPeriodicApplicationBackupState(context);assert.equal(state.version,3);assert.equal(state.latestBackup.pointId,second.pointId);assert.equal(state.lastVerified,null);
  const authenticated=await authenticatePeriodicApplicationBackup(context,proof,first.pointId);assert.throws(()=>periodicApplicationBackupSource({...authenticated}),{code:'periodic_application_backup_required'});
  producerContext=await createTaskPostgresBackupContext({laboratory:lab,directory:record.artifactDirectory,archiveKey:backupKey,applicationKey,binDirectory,libraryDirectory});
  const producerProof=await verifyPostgresBackup(producerContext,proof.backupId),sourceText=await readFile(join(data,'settings.json'));
  let reached;const restoring=new Promise(resolve=>{reached=resolve;}),paused=new Promise(resolve=>{releaseRestore=resolve;});
  fs.promises.rename=async(from,to)=>{if(to.startsWith(record.restoreDirectory+'/')&&to.endsWith('/pgdata')){reached();await paused;}return originalRename(from,to);};syncBuiltinESMExports();
  const verification=verifyPeriodicApplicationBackup(context,proof,first.pointId);void verification.catch(()=>{});
  const barrierTimeout=new AbortController();try{await Promise.race([restoring,delay(30000,undefined,{signal:barrierTimeout.signal}).then(()=>assert.fail('actual restore never reached private pgdata publication'))]);}finally{barrierTimeout.abort();}
  const concurrent=await capturePeriodicApplicationBackup(producerContext,producerProof,policy);assert.equal(concurrent.state,'application_backup_completed','new backup is not blocked by independent actual restore');
  state=await readPeriodicApplicationBackupState(producerContext);assert.equal(state.latestBackup.pointId,concurrent.pointId);assert.equal(state.pendingVerification.pointId,first.pointId,'producer preserves live verification reference');
  await writeFile(join(data,'settings.json'),'source deliberately changed after backup',{mode:0o600});releaseRestore();
  const verified=await verification;fs.promises.rename=originalRename;syncBuiltinESMExports();assert.equal(verified.state,'application_point_verified');assert.equal(verified.restoreVerified,true);assert.ok(verified.restoreDurationMs>0);
  state=await readPeriodicApplicationBackupState(context);assert.equal(state.latestBackup.pointId,concurrent.pointId,'verifier preserves concurrently produced backup');assert.equal(state.lastVerified.pointId,first.pointId);assert.equal(state.pendingVerification,null);
  const sealed=await readFile(join(firstDirectory,'application-restore.enc')),final=await readFile(join(firstDirectory,'application-final.enc'));
  assert.equal((await verifyPeriodicApplicationBackup(context,proof,first.pointId)).verifiedAt,verified.verifiedAt,'retry authenticates immutable actual proof');
  assert.deepEqual(await readFile(join(firstDirectory,'application-restore.enc')),sealed);assert.deepEqual(await readFile(join(firstDirectory,'application-final.enc')),final);
  await writeFile(join(data,'settings.json'),sourceText,{mode:0o600});
  const {authenticatePeriodicApplicationRetentionGroup}=await import('../postgres-periodic-application-retention.mjs');
  const group=await authenticatePeriodicApplicationRetentionGroup(record,join(record.artifactDirectory,proof.backupId),(await import('../postgres-backup.mjs')).postgresBackupRestoreSource(proof).metadata);
  assert.equal(group.backupCompletePoints.length,3);assert.equal(group.verifiedPoints.length,1);
  const {preparePeriodicApplicationHandoff}=await import('../postgres-periodic-application-backup.mjs'),handoff=await preparePeriodicApplicationHandoff(context,proof,second.pointId),bundleDirectory=join(record.artifactDirectory,proof.backupId,'points',second.pointId,'handoff');
  assert.equal(handoff.kind,'periodic-application');assert.equal(handoff.restoreVerified,false);
  const {handoffFiles}=await import('../postgres-backup-handoff.mjs'),handoffManifest=await readManifest(join(bundleDirectory,'handoff.enc'),applicationKey),paths=[];
  for await(const row of handoffFiles({directory:bundleDirectory,key:applicationKey,manifest:handoffManifest,maxFiles:300000,maxBytes:2147483648}))paths.push(row.path);
  assert.ok(paths.some(path=>path.includes('/points/'+second.pointId+'/backup-complete.enc')));assert.ok(paths.every(path=>!path.includes('/points/'+first.pointId+'/')&&!path.endsWith('/operator-final.enc')&&!path.endsWith('/application-final.enc')),'bundle contains only selected point, never operator or another point proof');
  const withHandoff=await authenticatePeriodicApplicationRetentionGroup(record,join(record.artifactDirectory,proof.backupId),(await import('../postgres-backup.mjs')).postgresBackupRestoreSource(proof).metadata);assert.equal(withHandoff.backupCompletePoints.length,3);
  const receiverDirectory=join(lab.directory,'periodic-receiver');await mkdir(receiverDirectory,{mode:0o700});
  await closePostgresBackupContext(producerContext);producerContext=null;await closePostgresBackupContext(context);context=null;await store.close();storeClosed=true;await lab.runtime.stop(lab.process);
  const {rm}=await import('node:fs/promises');await rm(lab.dataDirectory,{recursive:true});await rm(join(lab.directory,'wal'),{recursive:true});for(const root of materialRoots)await rm(root.directory,{recursive:true});
  const {createTaskPostgresBackupReceiver}=await import('../postgres-backup-receiver.mjs'),{restorePostgresReceiver,inspectPostgresReceiverRestore,storageReceiverRestoreAuthority}=await import('../storage-receiver.mjs');
  const receiverOptions={laboratory:lab,bundleDirectory,directory:receiverDirectory,archiveKey:backupKey,applicationKey,handoffDigest:handoff.handoffDigest};
  await assert.rejects(createTaskPostgresBackupReceiver(receiverOptions),{code:'transfer_manifest_authentication_failed'});
  await assert.rejects(createTaskPostgresBackupReceiver({...receiverOptions,handoffKind:'periodic-application',applicationKey:backupKey}),{code:'periodic_application_independent_key_required'});
  await assert.rejects(createTaskPostgresBackupReceiver({...receiverOptions,handoffKind:'periodic-application',applicationKeyVersion:'wrong-version'}),{code:'backup_receiver_periodic_binding'});
  receiver=await createTaskPostgresBackupReceiver({...receiverOptions,handoffKind:'periodic-application'});
  const received=await restorePostgresReceiver(receiver),recovered=inspectPostgresReceiverRestore(received);
  assert.equal(recovered.kind,'periodic-application');assert.equal(recovered.pointId,second.pointId);assert.equal(recovered.sourceFreeRestoreVerified,true);assert.equal(recovered.receiverRestoreVerified,true);assert.equal(recovered.applicationMaterialsVerified,true);assert.equal(recovered.recovery.application.recordingVerification.verified,withContext?7:4);
  assert.equal(recovered.differentMachine,false,'same-machine task cannot claim offhost proof');assert.equal(recovered.offhostVerified,false);assert.equal(recovered.deploymentAllowed,false);
  await assert.rejects(storageReceiverRestoreAuthority(received),{code:'periodic_receiver_attestation_required'});
  const {attestTaskStorageReceiverPeriodic,attestStorageReceiverPeriodic,verifyStorageReceiverPeriodicAttestation,verifyOffhostStorageReceiverPeriodicAttestation}=await import('../storage-receiver-periodic-attestation.mjs'),{privateKey,publicKey}=generateKeyPairSync('ed25519');
  const attestation=await attestTaskStorageReceiverPeriodic(received,privateKey),{version:unusedVersion,purpose:unusedPurpose,scope:unusedScope,resultDigest:unusedResult,checks:unusedChecks,...expected}=attestation.payload;
  assert.equal(verifyStorageReceiverPeriodicAttestation(attestation,publicKey,{expected}).checks.applicationMaterialsVerified,true);
  assert.throws(()=>verifyOffhostStorageReceiverPeriodicAttestation(attestation,publicKey,{expected}),{code:'periodic_receiver_attestation_offhost_required'});
  assert.throws(()=>verifyStorageReceiverPeriodicAttestation(attestation,publicKey,{expected:{...expected,pointId:randomUUID()}}),{code:'periodic_receiver_attestation_binding'});
  await assert.rejects(attestTaskStorageReceiverPeriodic({...received},privateKey),{code:'periodic_receiver_receipt_required'});await assert.rejects(attestStorageReceiverPeriodic(received),{code:'periodic_receiver_attestation_formal_required'});
  await assert.rejects(attestTaskStorageReceiverPeriodic(received,privateKey,{signal:AbortSignal.abort()}),{name:'AbortError'});
  const {inspectPostgresBackupReceiver,closePostgresBackupReceiver}=await import('../postgres-backup-receiver.mjs'),liveReceiver=await inspectPostgresBackupReceiver(receiver),manifestPath=join(liveReceiver.working,'handoff.enc'),originalManifest=await readFile(manifestPath);
  await writeManifest(manifestPath,{...liveReceiver.manifest,pointId:randomUUID()},applicationKey,{replace:true});await assert.rejects(attestTaskStorageReceiverPeriodic(received,privateKey),{code:'backup_receiver_identity_changed'});await writeFile(manifestPath,originalManifest,{mode:0o600});
  assert.equal(verifyStorageReceiverPeriodicAttestation(await attestTaskStorageReceiverPeriodic(received,privateKey),publicKey,{expected}).pointId,second.pointId);
  const receivedContext=postgresBackupContext(liveReceiver.context);await closePostgresBackupReceiver(receiver);receiver=null;
  assert.ok(receivedContext.key.every(byte=>byte===0));assert.ok(receivedContext.applicationKey.every(byte=>byte===0));assert.ok(liveReceiver.key.every(byte=>byte===0));assert.ok(liveReceiver.applicationKey.every(byte=>byte===0));
  await assert.rejects(attestTaskStorageReceiverPeriodic(received,privateKey),{code:'backup_receiver_capability_required'});

  assert.deepEqual(await readdir(record.restoreDirectory),[]);
});
