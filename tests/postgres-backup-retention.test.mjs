import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {readFile,readdir,stat,mkdir,writeFile,rm} from 'node:fs/promises';
import {spawn} from 'node:child_process';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,startOwnedCapacityChild} from '../tools/capacity/owned-runtime.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPostgresBaseBackup,checkpointPostgresBackup,verifyPostgresBackup} from '../postgres-backup.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from '../postgres-backup-restore.mjs';
import {postgresBackupRetentionPolicy,preparePostgresBackupRetention,createPostgresBackupRetentionPlan,applyPostgresBackupRetention} from '../postgres-backup-retention.mjs';
import {withBackupArchiveLock,backupCoordinationFiles,backupScheduleCoordinationDirectory} from '../postgres-backup-lock.mjs';
import {readWalManifest,archiveWal,inspectArchivedWal} from '../postgres-recovery.mjs';
import {readWalCheckpointSegments} from '../postgres-backup-wal-catalog.mjs';
import {captureFileSet,writeManifest,readManifest,digestFile} from '../migration-artifacts.mjs';

const policy={version:1,enabled:true,keepDatabaseCount:1,keepApplicationCount:1,minAgeMs:1,pinnedBackupIds:[],operatorStateFiles:[]};
test('retention is opt-in and requires explicit counts, minimum age and reference scope',async()=>{
 assert.equal((await createPostgresBackupRetentionPlan(null,{version:1,enabled:false})).state,'retention_disabled');assert.equal((await applyPostgresBackupRetention(null,{version:1,enabled:false},'not-a-plan')).retentionApplied,false);
 for(const value of [{...policy,keepDatabaseCount:0},{...policy,keepApplicationCount:0},{...policy,minAgeMs:0},{...policy,unknown:true},{...policy,pinnedBackupIds:['../backup']},{...policy,operatorStateFiles:['relative']}])assert.throws(()=>postgresBackupRetentionPolicy(value),{code:'backup_retention_policy_invalid'});
});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,real={skip:!binDirectory?'需自有PG18工具，不连接共享数据库':false,timeout:240000};
async function until(operation){const end=Date.now()+15000;for(;;){try{const value=await operation();if(value)return value;}catch(e){if(!['ENOENT','backup_coordination_busy'].includes(e.code))throw e;}assert.ok(Date.now()<end,'bounded coordination condition');await delay(20);}}
async function fixture(t,{application=false}={}){
 const key=randomBytes(32),applicationKey=application?randomBytes(32):null,lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:key}}),store=new PostgresStore({connection:lab.connection});let context,storeClosed=false;const closeStore=async()=>{if(!storeClosed){storeClosed=true;await store.close();}};
 t.after(async()=>{try{if(context)await closePostgresBackupContext(context);}finally{try{await closeStore();}finally{await lab.close();key.fill(0);applicationKey?.fill(0);}}});
 await store.migrate();await store.initializePricing();await store.query('CREATE TABLE harbor.retention_probe(id text PRIMARY KEY,value bigint NOT NULL)');await store.run("INSERT INTO harbor.retention_probe VALUES('before',1)");await store.get('SELECT pg_switch_wal()');
 context=await createTaskPostgresBackupContext({laboratory:lab,directory:join(lab.directory,'retention-backups'),archiveKey:key,binDirectory,libraryDirectory,...(applicationKey?{applicationKey,applicationKeyVersion:'task-application-v3'}:{})});const record=postgresBackupContext(context);
 return {context,record,key,lab,store,policy,closeStore};
}
async function backups(f){
 const first=await createPostgresBaseBackup(f.context);await f.store.run("INSERT INTO harbor.retention_probe VALUES('after_a',2)");await checkpointPostgresBackup(first);
 const middle=await createPostgresBaseBackup(f.context);await f.store.run("INSERT INTO harbor.retention_probe VALUES('after_b',3)");await checkpointPostgresBackup(middle);
 const latest=await createPostgresBaseBackup(f.context);await f.store.run("INSERT INTO harbor.retention_probe VALUES('after_c',4)");await checkpointPostgresBackup(latest);
 await preparePostgresBackupRetention(f.context,policy);await assert.rejects(createPostgresBackupRetentionPlan(f.context,policy),error=>error.code==='ENOENT'||error.code==='backup_retention_archiver_not_coordinated');await checkpointPostgresBackup(latest);
 await until(()=>readWalManifest(join(f.record.archiveDirectory,backupCoordinationFiles.receipt),f.key,{ownerUid:f.record.restoreUid}));await until(()=>withBackupArchiveLock({directory:f.record.archiveDirectory,exclusive:true},()=>true));
 return {first,middle,latest,policy:{...policy,pinnedBackupIds:[first.backupId]}};
}
test('real retention removes an obsolete complete base and unnecessary WAL while oldest and latest retained points still reach genuine PITR',real,async t=>{
 const f=await fixture(t),b=await backups(f),oldCheckpoint=await readFile(join(f.record.artifactDirectory,b.first.backupId,'checkpoint.enc')),latestCheckpoint=await readFile(join(f.record.artifactDirectory,b.latest.backupId,'checkpoint.enc'));
 const plan=await createPostgresBackupRetentionPlan(f.context,b.policy);assert.equal(plan.deleteDatabases,1);assert.ok(plan.deleteWalSegments>0);assert.equal(plan.retentionApplied,false);
 const applied=await applyPostgresBackupRetention(f.context,b.policy,plan.planId);assert.equal(applied.retentionApplied,true);assert.equal(applied.deleteDatabases,1);assert.equal(applied.deleteWalSegments,plan.deleteWalSegments);
 await assert.rejects(stat(join(f.record.artifactDirectory,b.middle.backupId)),{code:'ENOENT'});assert.deepEqual(await readFile(join(f.record.artifactDirectory,b.first.backupId,'checkpoint.enc')),oldCheckpoint);assert.deepEqual(await readFile(join(f.record.artifactDirectory,b.latest.backupId,'checkpoint.enc')),latestCheckpoint);
 const table=(await f.store.get("SELECT 'harbor.retention_probe'::regclass::oid::text oid")).oid;
 for(const [proof,rows] of [[b.first,'2'],[b.latest,'4']]){const restored=inspectPostgresBackupRestore(await restorePostgresBackup(await verifyPostgresBackup(f.context,proof.backupId)));assert.equal(restored.restoreVerified,true);assert.equal(restored.recoveryExitedVerified,true);assert.equal(restored.tableCounts.find(row=>row.tableOid===table).rows,rows);}
 assert.equal((await applyPostgresBackupRetention(f.context,b.policy,plan.planId)).state,'retention_already_applied');assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.retention_probe')).n,4);
});

test('authenticated preview refuses stale references, unknown point groups and uncoordinated operations before retirement',real,async t=>{
 const f=await fixture(t),b=await backups(f),middle=join(f.record.artifactDirectory,b.middle.backupId),before=await readFile(join(middle,'checkpoint.enc'));let plan=await createPostgresBackupRetentionPlan(f.context,b.policy);
 const points=join(middle,'unknown-point-format');await mkdir(points,{mode:0o700});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_retention_unknown_directory_requires_pin'});await rm(points,{recursive:true});plan=await createPostgresBackupRetentionPlan(f.context,b.policy);
 const orphan=join(f.record.archiveDirectory,'00000001000000FF00000000.enc');await writeFile(orphan,'synthetic-orphaned-ciphertext',{mode:0o600});await assert.rejects(createPostgresBackupRetentionPlan(f.context,b.policy),{code:'backup_retention_archive_pair_incomplete'});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_retention_archive_pair_incomplete'});await rm(orphan);
 const receiptPath=join(f.record.archiveDirectory,backupCoordinationFiles.receipt),originalReceipt=await readWalManifest(receiptPath,f.key,{ownerUid:f.record.restoreUid});
 await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId,{onTransition:async phase=>{if(phase==='coordination_witnessed')await withBackupArchiveLock({directory:f.record.archiveDirectory},()=>writeManifest(receiptPath,{...originalReceipt,created:originalReceipt.created+1},f.key,{replace:true}));}}),{code:'backup_retention_coordination_witness_changed'});
 await assert.rejects(stat(join(f.record.artifactDirectory,'.retention',plan.planId,'journal.enc')),{code:'ENOENT'});assert.deepEqual(await readFile(join(middle,'checkpoint.enc')),before);
 await withBackupArchiveLock({directory:f.record.archiveDirectory},()=>writeManifest(receiptPath,originalReceipt,f.key,{replace:true}));
 for(const phase of ['inventory_authenticated','coordination_witnessed']){
  const lateOrphan=join(f.record.archiveDirectory,'00000001000000FE00000000.enc');
  await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId,{onTransition:async observed=>{if(observed===phase)await writeFile(lateOrphan,'synthetic-late-orphan',{mode:0o600});}}),{code:phase==='inventory_authenticated'?'backup_retention_archive_changed':'backup_retention_identity_changed'});
  assert.equal(await readFile(lateOrphan,'utf8'),'synthetic-late-orphan');await assert.rejects(stat(join(f.record.artifactDirectory,'.retention',plan.planId,'journal.enc')),{code:'ENOENT'});assert.deepEqual(await readFile(join(middle,'checkpoint.enc')),before);await rm(lateOrphan);
 }
 const stateFile=join(f.record.artifactDirectory,'periodic-state.enc');await writeManifest(stateFile,{version:1,scope:'harbor-periodic-database-backup-v1',systemId:f.record.expected.system_id,keyVersion:f.record.keyVersion,active:{backupId:b.middle.backupId},pending:null},f.key);await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_retention_plan_stale'});await rm(stateFile);
 await withBackupArchiveLock({directory:f.record.archiveDirectory},async()=>{await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_coordination_busy'});});
 const child=spawn(process.execPath,['-e','setInterval(()=>{},1000)','/synthetic/storage-backup.mjs'],{stdio:'ignore'});try{await new Promise((resolve,reject)=>{child.once('spawn',resolve);child.once('error',reject);});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_retention_legacy_operation_present'});}finally{child.kill('SIGTERM');await new Promise(resolve=>child.once('close',resolve));}
 const planPath=join(f.record.artifactDirectory,'.retention',plan.planId,'plan.enc'),saved=await readFile(planPath);await writeFile(planPath,Buffer.from('damaged authenticated plan'),{mode:0o600});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'transfer_manifest_authentication_failed'});await writeFile(planPath,saved,{mode:0o600});
 assert.deepEqual(await readFile(join(middle,'checkpoint.enc')),before);assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.retention_probe')).n,4);
 assert.equal((await applyPostgresBackupRetention(f.context,b.policy,plan.planId)).retentionApplied,true);
});

test('a prepared retirement resumes precise groups after lost acknowledgements without adopting or deleting unrelated files',real,async t=>{
 const f=await fixture(t),b=await backups(f),plan=await createPostgresBackupRetentionPlan(f.context,b.policy),control=join(f.record.artifactDirectory,'.retention',plan.planId);
 for(const phase of ['prepared','quarantine_parent_synced','database_renamed','database_destination_synced','group_quarantined','wal_file_renamed','wal_pair_renamed','wal_destination_synced','quarantined','deleting','quarantine_unlinked','quarantine_unlink_synced','quarantine_directory_removed','quarantine_directory_parent_synced','database_group_root_removed','database_group_parent_synced','group_deleted','wal_quarantine_removed','wal_quarantine_parent_synced']){
  await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId,{onTransition:async observed=>{if(observed===phase){if(phase==='quarantine_parent_synced')assert.equal((await stat(join(f.record.artifactDirectory,b.middle.backupId))).isDirectory(),true);throw Error('synthetic-retention-interruption');}}}),/synthetic-retention-interruption/);
  const journal=await readManifest(join(control,'journal.enc'),f.key);assert.notEqual(journal.phase,'complete');await assert.rejects(createPostgresBackupRetentionPlan(f.context,b.policy),{code:'backup_retention_previous_apply_incomplete'});
 }
 const foreign=join(control,'quarantine','foreign-unowned');await writeFile(foreign,'preserve',{mode:0o600});
 const result=await applyPostgresBackupRetention(f.context,b.policy,plan.planId);assert.equal(result.retentionApplied,true);assert.equal(await readFile(foreign,'utf8'),'preserve');assert.equal((await applyPostgresBackupRetention(f.context,b.policy,plan.planId)).state,'retention_already_applied');
 assert.equal((await readManifest(join(control,'journal.enc'),f.key)).phase,'complete');
});


test('closing a retention context keeps its key until cancellation settles and a reopened context resumes the authenticated journal',real,async t=>{
 const f=await fixture(t),b=await backups(f),plan=await createPostgresBackupRetentionPlan(f.context,b.policy);let closing;
 const applying=applyPostgresBackupRetention(f.context,b.policy,plan.planId,{onTransition:phase=>{
  if(phase==='prepared'){closing=closePostgresBackupContext(f.context);assert.equal(f.record.closed,true);assert.ok(f.record.key.some(byte=>byte!==0));}
 }});
 await assert.rejects(applying,{name:'AbortError'});await closing;
 assert.equal(f.record.busy,false);assert.equal(f.record.pending,null);assert.ok(f.record.key.every(byte=>byte===0));
 const directory=f.record.artifactDirectory,reopened=await createTaskPostgresBackupContext({laboratory:f.lab,directory,archiveKey:f.key,binDirectory,libraryDirectory});
 try{const result=await applyPostgresBackupRetention(reopened,b.policy,plan.planId);assert.equal(result.retentionApplied,true);assert.equal((await readManifest(join(directory,'.retention',plan.planId,'journal.enc'),f.key)).phase,'complete');}finally{await closePostgresBackupContext(reopened);}
});


async function planGroups(f,id){const directory=join(f.record.artifactDirectory,'.retention',id),plan=await readManifest(join(directory,'plan.enc'),f.key),groups=[];for(const part of plan.parts){const value=await readManifest(join(directory,part.name),f.key);groups.push(...value.items.filter(row=>row.kind==='group'));}return groups;}
test('an authenticated old-WAL receipt stays outside retirement candidates and prepared reentry keeps its coordination evidence',real,async t=>{
 const f=await fixture(t),b=await backups(f),initial=await createPostgresBackupRetentionPlan(f.context,b.policy),old=(await planGroups(f,initial.planId)).find(row=>row.type==='wal');assert.ok(old);
 const receiptPath=join(f.record.archiveDirectory,backupCoordinationFiles.receipt),receipt=await readWalManifest(receiptPath,f.key,{ownerUid:f.record.restoreUid});
 // Input-contract fixture for a later archive-wal acknowledgement of an already
 // authenticated old segment. It grants no new runtime or restore capability.
 await withBackupArchiveLock({directory:f.record.archiveDirectory},()=>writeManifest(receiptPath,{...receipt,walName:old.source},f.key,{replace:true}));
 await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,initial.planId),{code:'backup_retention_plan_stale'});
 const protectedPlan=await createPostgresBackupRetentionPlan(f.context,b.policy);assert.equal((await planGroups(f,protectedPlan.planId)).some(row=>row.type==='wal'&&row.source===old.source),false);
 await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,protectedPlan.planId,{onTransition:phase=>{if(phase==='group_quarantined')throw Error('old-receipt-interruption');}}),/old-receipt-interruption/);
 assert.equal((await stat(join(f.record.archiveDirectory,old.source+'.enc'))).isFile(),true);
 assert.equal((await applyPostgresBackupRetention(f.context,b.policy,protectedPlan.planId)).retentionApplied,true);
 assert.equal((await stat(join(f.record.archiveDirectory,old.source+'.meta.enc'))).isFile(),true);
});

test('durable authenticated plan reservations resume each prepublication interruption and preserve foreign nonempty allocations',real,async t=>{
 const f=await fixture(t),b=await backups(f),root=join(f.record.artifactDirectory,'.retention');await mkdir(root,{mode:0o700});
 for(const phase of ['plan_reserved','plan_quarantine_created','plan_quarantine_identity_recorded']){
  const before=new Set(await readdir(root));await assert.rejects(createPostgresBackupRetentionPlan(f.context,b.policy,{onTransition:observed=>{if(observed===phase)throw Error('plan-publication-interruption');}}),/plan-publication-interruption/);
  const id=(await readdir(root)).find(name=>!before.has(name));assert.ok(id);const directory=join(root,id),preparation=await readManifest(join(directory,'plan-preparation.enc'),f.key);assert.equal(preparation.plan.planId,id);await assert.rejects(stat(join(directory,'plan.enc')),{code:'ENOENT'});
  if(phase==='plan_quarantine_created'){
   const foreign=join(f.record.archiveDirectory,'.retention-'+id,'foreign');await writeFile(foreign,'preserve',{mode:0o600});await assert.rejects(createPostgresBackupRetentionPlan(f.context,b.policy),{code:'backup_retention_allocation_not_empty'});assert.equal(await readFile(foreign,'utf8'),'preserve');await rm(foreign);
  }
  await createPostgresBackupRetentionPlan(f.context,b.policy);
  const plan=await readManifest(join(directory,'plan.enc'),f.key),allocation=await stat(join(f.record.archiveDirectory,'.retention-'+id));assert.equal(plan.walQuarantineIdentity.ino,String(allocation.ino));assert.equal((await stat(join(f.record.artifactDirectory,b.middle.backupId))).isDirectory(),true);
 }
 const finalPlan=await createPostgresBackupRetentionPlan(f.context,b.policy);assert.equal((await applyPostgresBackupRetention(f.context,b.policy,finalPlan.planId)).retentionApplied,true);
});


async function quarantineSnapshot(directory){const rows=[];async function visit(path,prefix=''){for(const name of (await readdir(path)).sort()){const relative=prefix?prefix+'/'+name:name,item=join(path,name),info=await stat(item);if(info.isDirectory()){rows.push({path:relative,type:'directory'});await visit(item,relative);}else rows.push({path:relative,type:'file',digest:await digestFile(item)});}}await visit(directory);return rows;}
for(const interruptedPhase of ['quarantined','deleting'])test('a '+interruptedPhase+' journal refuses to delete quarantine after a genuine source postmaster restart',real,async t=>{
 const f=await fixture(t),b=await backups(f),plan=await createPostgresBackupRetentionPlan(f.context,b.policy),control=join(f.record.artifactDirectory,'.retention',plan.planId),database=join(control,'quarantine'),wal=join(f.record.archiveDirectory,'.retention-'+plan.planId);
 await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId,{onTransition:phase=>{if(phase===interruptedPhase)throw Error('pre-restart-interruption');}}),/pre-restart-interruption/);
 const journal=await readFile(join(control,'journal.enc')),databaseBefore=await quarantineSnapshot(database),walBefore=await quarantineSnapshot(wal);assert.ok(databaseBefore.length&&walBefore.length);
 await f.closeStore();await f.lab.runtime.stop(f.lab.process);
 const restarted=startOwnedCapacityChild(f.lab.runtime,join(binDirectory,'postgres'),['-D',f.lab.dataDirectory],{role:'database',env:{LD_LIBRARY_PATH:libraryDirectory??''}}),probe=new PostgresStore({connection:{...f.lab.connection,host:f.lab.socketDirectory,port:f.lab.databasePort}});
 try{
  await until(async()=>{if(restarted.exitCode!==null)throw Error('owned-retention-restart-failed');try{return (await probe.get('SELECT extract(epoch from pg_postmaster_start_time())::text value')).value!==f.record.expected.postmaster_start;}catch{return false;}});
  await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_retention_coordination_required'});
  assert.deepEqual(await readFile(join(control,'journal.enc')),journal);assert.deepEqual(await quarantineSnapshot(database),databaseBefore);assert.deepEqual(await quarantineSnapshot(wal),walBefore);
 }finally{await probe.close();await f.lab.runtime.stop(restarted);}
});


async function optionalQuarantineSnapshot(directory){try{return await quarantineSnapshot(directory);}catch(error){if(error.code==='ENOENT')return null;throw error;}}
test('journal reentry authenticates retained ciphertexts, unknown formats and required WAL before any prepared or deleting cursor advances',real,async t=>{
 const f=await fixture(t),b=await backups(f),plan=await createPostgresBackupRetentionPlan(f.context,b.policy),control=join(f.record.artifactDirectory,'.retention',plan.planId),latest=join(f.record.artifactDirectory,b.latest.backupId),fileSet=await readManifest(join(latest,'files.enc'),f.key);let encryptedFile;
 for(const descriptor of fileSet.catalog){const part=await readManifest(join(latest,descriptor.name),f.key),entry=part.entries.find(entry=>entry.kind==='file');if(entry){encryptedFile=join(latest,entry.artifact);break;}}assert.ok(encryptedFile);
 const receipt=await readWalManifest(join(f.record.archiveDirectory,backupCoordinationFiles.receipt),f.key,{ownerUid:f.record.restoreUid}),checkpoint=await readManifest(join(f.record.artifactDirectory,b.first.backupId,'checkpoint.enc'),f.key);let required;
 for await(const row of readWalCheckpointSegments({directory:join(f.record.artifactDirectory,b.first.backupId),key:f.key,checkpoint})){if(row.name!==receipt.walName){required=join(f.record.archiveDirectory,row.name+'.enc');break;}}assert.ok(required);
 const ciphertext=await readFile(encryptedFile),walCiphertext=await readFile(required);
 for(const phase of ['prepared','quarantined','deleting']){
  await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId,{onTransition:observed=>{if(observed===phase)throw Error('retained-authentication-interruption');}}),/retained-authentication-interruption/);
  const journal=await readFile(join(control,'journal.enc')),paths=[join(f.record.artifactDirectory,b.middle.backupId),join(control,'quarantine'),join(f.record.archiveDirectory,'.retention-'+plan.planId)],before=await Promise.all(paths.map(optionalQuarantineSnapshot));
  const unchanged=async()=>{assert.deepEqual(await readFile(join(control,'journal.enc')),journal);assert.deepEqual(await Promise.all(paths.map(optionalQuarantineSnapshot)),before);};
  const corrupt=Buffer.from(ciphertext);corrupt[0]^=1;await writeFile(encryptedFile,corrupt);await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_saved_file_changed'});await unchanged();await writeFile(encryptedFile,ciphertext);
  for(const name of ['points','foreign-after-journal']){const directory=join(latest,name);await mkdir(directory,{mode:0o700});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_retention_unknown_directory_requires_pin'});assert.deepEqual(await readdir(directory),[]);await unchanged();await rm(directory,{recursive:true});}
  const unknown=join(latest,'foreign-after-journal.enc');await writeFile(unknown,'preserve',{mode:0o600});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_retention_unknown_file_requires_pin'});assert.equal(await readFile(unknown,'utf8'),'preserve');await unchanged();await rm(unknown);
  const brokenWal=Buffer.from(walCiphertext);brokenWal[brokenWal.length-1]^=1;await writeFile(required,brokenWal);await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'recovery_wal_conflict'});await unchanged();await writeFile(required,walCiphertext);
 }
 assert.equal((await applyPostgresBackupRetention(f.context,b.policy,plan.planId)).retentionApplied,true);assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.retention_probe')).n,4);
});


const scheduleProtocolChild=String.raw`
 const configuration=await new Promise(resolve=>process.once('message',resolve));
 const {withPostgresBackupScheduleRegistration,withPostgresBackupOperation}=await import(configuration.module);let stop,release,active=Promise.resolve();const stopped=new Promise(resolve=>{stop=resolve;});
 let record;if(configuration.modern)record={...configuration.record,key:Buffer.from(configuration.key,'hex')};
 const finish=()=>{release?.();stop();};process.on('SIGTERM',finish);process.on('message',message=>{
  if(message.command==='release')release?.();if(message.command==='stop')finish();
  if(message.command==='hold')active=withPostgresBackupOperation(record,async()=>{process.send({state:'active'});await new Promise(resolve=>{release=resolve;});}).then(()=>process.send({state:'idle'}),error=>process.send({state:'error',code:error.code??error.name}));
 });
 try{if(configuration.modern)await withPostgresBackupScheduleRegistration(record,async()=>{process.send({state:'registered'});await stopped;await active;});else{process.send({state:'legacy_ready'});await stopped;}}
 catch(error){process.send({state:'error',code:error.code??error.name});process.exitCode=1;}finally{record?.key.fill(0);process.disconnect();}
`;
async function scheduleParticipant(t,f,{modern=true}={}){
 const child=spawn(process.execPath,['--input-type=module','-e',scheduleProtocolChild,'/synthetic/storage-backup.mjs','schedule'],{stdio:['ignore','ignore','ignore','ipc']}),messages=[],closed=new Promise(resolve=>child.once('close',resolve));child.on('message',message=>messages.push(message));child.on('error',error=>messages.push({state:'error',code:error.code??error.name}));let joined=false;
 const wait=state=>until(()=>{const error=messages.find(message=>message.state==='error');if(error)throw Error('schedule-protocol-child-'+error.code);const index=messages.findIndex(message=>message.state===state);if(index>=0)return messages.splice(index,1)[0];if(child.exitCode!==null||child.signalCode!==null)throw Error('schedule-protocol-child-ended');return false;});
 const waitClosed=async timeoutMs=>{let timer;try{await Promise.race([closed,new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(Error('schedule-protocol-child-stop-timeout')),timeoutMs);})]);}finally{clearTimeout(timer);}};const stop=async(signal)=>{if(joined)return;if(signal)child.kill(signal);else if(child.connected)child.send({command:'stop'},()=>{});try{await waitClosed(10000);joined=true;}catch(error){if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');await waitClosed(5000);joined=true;throw error;}};t.after(()=>stop());
 const record=f.record;child.send({modern,module:new URL('../postgres-backup-lock.mjs',import.meta.url).href,...(modern?{key:f.key.toString('hex'),record:{scope:record.scope,archiveDirectory:record.archiveDirectory,artifactDirectory:record.artifactDirectory,restoreUid:record.restoreUid,expected:{system_id:record.expected.system_id},configurationDigest:record.configurationDigest,keyVersion:record.keyVersion}}:{})});await wait(modern?'registered':'legacy_ready');return {child,wait,stop};
}
test('nonsegment archives retain the WAL receipt and a real registered schedule protocol permits idle cleanup while active and legacy processes stay fenced',real,async t=>{
 const f=await fixture(t),b=await backups(f),receiptPath=join(f.record.archiveDirectory,backupCoordinationFiles.receipt),receipt=await readWalManifest(receiptPath,f.key,{ownerUid:f.record.restoreUid}),before=await readFile(receiptPath),inputs=[['00000002.history','1\t0/1000000\tsynthetic-history-only\n'],[receipt.walName+'.'+randomBytes(4).toString('hex').toUpperCase()+'.backup','synthetic backup history input\n']];
 // Real archive API against this fixture's private pg_wal; not a promotion or
 // a PostgreSQL-produced backup history proof.
 for(const [name,body] of inputs){const source=join(f.record.expected.data_directory,'pg_wal',name);await writeFile(source,body,{flag:'wx',mode:0o600});await archiveWal({source,name,directory:f.record.archiveDirectory,key:f.key});assert.equal((await inspectArchivedWal({name,directory:f.record.archiveDirectory,key:f.key})).identity.kind,name.endsWith('.history')?'history':'backup');assert.deepEqual(await readFile(receiptPath),before);}
 // This child exercises the genuine registration/flock protocol used by watch;
 // it does not claim to be a complete fixed-root CLI schedule acceptance.
 const modern=await scheduleParticipant(t,f);assert.equal((await preparePostgresBackupRetention(f.context,b.policy)).state,'retention_coordination_prepared');await checkpointPostgresBackup(b.latest);
 const intent=await readWalManifest(join(f.record.archiveDirectory,backupCoordinationFiles.intent),f.key,{ownerUid:f.record.restoreUid});await until(async()=>{const value=await readWalManifest(receiptPath,f.key,{ownerUid:f.record.restoreUid});return value.nonce===intent.nonce;});await until(()=>withBackupArchiveLock({directory:f.record.archiveDirectory,exclusive:true},()=>true));
 const plan=await createPostgresBackupRetentionPlan(f.context,b.policy);modern.child.send({command:'hold'});await modern.wait('active');await assert.rejects(preparePostgresBackupRetention(f.context,b.policy),{code:'backup_coordination_busy'});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId),{code:'backup_coordination_busy'});modern.child.send({command:'release'});await modern.wait('idle');assert.equal((await applyPostgresBackupRetention(f.context,b.policy,plan.planId)).retentionApplied,true);
 const bootDirectory=join(f.record.artifactDirectory,backupScheduleCoordinationDirectory),liveName=(await readdir(bootDirectory)).find(name=>name.endsWith('.enc')&&name.split('-').at(-2)===String(modern.child.pid)),live=await readManifest(join(bootDirectory,liveName),f.key),foreignBoot={...live,bootId:randomUUID()},foreignName=foreignBoot.bootId+'-'+foreignBoot.pid+'-'+foreignBoot.start+'.enc';assert.notEqual(foreignBoot.bootId,live.bootId);await writeManifest(join(bootDirectory,foreignName),foreignBoot,f.key);await rm(join(bootDirectory,liveName));
 await assert.rejects(preparePostgresBackupRetention(f.context,b.policy),{code:'backup_retention_legacy_operation_present'});const oldBootBytes=await readFile(join(bootDirectory,foreignName));await modern.stop();
 const replacement=await scheduleParticipant(t,f);assert.equal((await preparePostgresBackupRetention(f.context,b.policy)).state,'retention_coordination_prepared');assert.deepEqual(await readFile(join(bootDirectory,foreignName)),oldBootBytes);await replacement.stop();
 // Restore a fresh registered participant for the crash/legacy assertions.
 const modernAfterBoot=await scheduleParticipant(t,f);
 const registrationDirectory=join(f.record.artifactDirectory,backupScheduleCoordinationDirectory),name=(await readdir(registrationDirectory)).find(name=>name.endsWith('.enc')&&name.split('-').at(-2)===String(modernAfterBoot.child.pid)),registration=await readFile(join(registrationDirectory,name));await modernAfterBoot.stop('SIGKILL');assert.deepEqual(await readFile(join(registrationDirectory,name)),registration);
 await checkpointPostgresBackup(b.latest);const refreshed=await readWalManifest(join(f.record.archiveDirectory,backupCoordinationFiles.intent),f.key,{ownerUid:f.record.restoreUid});await until(async()=>{const value=await readWalManifest(receiptPath,f.key,{ownerUid:f.record.restoreUid});return value.nonce===refreshed.nonce;});await until(()=>withBackupArchiveLock({directory:f.record.archiveDirectory,exclusive:true},()=>true));
 const later=await createPostgresBackupRetentionPlan(f.context,b.policy),legacy=await scheduleParticipant(t,f,{modern:false});await assert.rejects(preparePostgresBackupRetention(f.context,b.policy),{code:'backup_retention_legacy_operation_present'});await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,later.planId),{code:'backup_retention_legacy_operation_present'});await legacy.stop();assert.equal((await applyPostgresBackupRetention(f.context,b.policy,later.planId)).retentionApplied,true);assert.deepEqual(await readFile(join(registrationDirectory,name)),registration);
 assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.retention_probe')).n,4);
});

async function applicationCatalogFixture(f,proof,runId){
 // An authenticated policy input with genuinely encrypted files; this is not
 // an operator capability, runtime approval or independent recovery proof.
 const source=join(f.lab.directory,'application-source-'+proof.backupId);await mkdir(source,{mode:0o700});await writeFile(join(source,'settings.json'),JSON.stringify({synthetic:true}),{mode:0o600});await writeFile(join(source,'key-encryption.key'),randomBytes(32),{mode:0o600});
 const base=join(f.record.artifactDirectory,proof.backupId),application=join(base,'application');await mkdir(application,{mode:0o700});const files=await captureFileSet({roots:[{id:'data',source}],directory:application,key:f.key});
 const checkpointDigest=await digestFile(join(base,'checkpoint.enc')),manifest={version:1,backupId:proof.backupId,configurationDigest:f.record.configurationDigest,keyVersion:f.record.keyVersion,runId,pendingBindingDigest:randomBytes(32).toString('hex'),materialRoots:[{id:'data',directory:source}],materialKeys:[],auditFiles:[],files,checkpointDigest};await writeManifest(join(application,'application.enc'),manifest,f.key);
 await writeManifest(join(base,'operator-final.enc'),{version:1,stage:'pending-final-copy-and-local-restore',backupId:proof.backupId,configurationDigest:f.record.configurationDigest,checkpointDigest,applicationManifestDigest:await digestFile(join(application,'application.enc')),recovery:{restoreVerified:true,application:{filesRestoredVerified:true,originalStoredCredentialsVerified:true,originalRowsAndAmountsVerified:true}}},f.key);
}
test('application self references can age out as a whole while an independent application count and actual external operator reference remain protected',real,async t=>{
 const f=await fixture(t),b=await backups(f),runId=randomUUID();await applicationCatalogFixture(f,b.first,randomUUID());await applicationCatalogFixture(f,b.middle,runId);
 const unpinned={...policy},plan=await createPostgresBackupRetentionPlan(f.context,unpinned);assert.equal(plan.retainedApplicationGroups,1);assert.equal(plan.deleteDatabases,1);
 const run=join(f.lab.directory,'external-operator');await mkdir(run,{mode:0o700});const file=join(run,'operator.enc');await writeManifest(file,{version:1,runId,cluster:f.record.expected.system_id,phase:'pending',baseline:{backupId:b.first.backupId}},f.key);
 const external={...policy,operatorStateFiles:[file]},protectedPlan=await createPostgresBackupRetentionPlan(f.context,external);assert.equal(protectedPlan.deleteDatabases,0);assert.equal(protectedPlan.retainedApplicationGroups,2);
 await assert.rejects(applyPostgresBackupRetention(f.context,external,plan.planId),{code:'backup_retention_plan_binding'});
 await rm(file); // The external owner has finished; the active policy no longer registers it.
 const applied=await applyPostgresBackupRetention(f.context,unpinned,plan.planId);assert.equal(applied.deleteDatabases,1);await assert.rejects(stat(join(f.record.artifactDirectory,b.first.backupId)),{code:'ENOENT'});assert.equal((await stat(join(f.record.artifactDirectory,b.middle.backupId,'application/application.enc'))).isFile(),true);
});


test('periodic v3 reservations and all four state references protect a whole base without granting verification',real,async t=>{
 const f=await fixture(t,{application:true}),b=await backups(f),base=join(f.record.artifactDirectory,b.middle.backupId),metadata=await readManifest(join(base,'backup.enc'),f.key),id=randomUUID(),cycles=join(base,'application-cycles');
 await mkdir(cycles,{mode:0o700});const now=Date.now(),row={version:3,scope:'harbor-periodic-application-cycle-v3',configurationDigest:metadata.configurationDigest,systemId:metadata.systemId,keyVersion:metadata.keyVersion,applicationKeyVersion:f.record.applicationKeyVersion,applicationKeyDigest:f.record.applicationKeyDigest,applicationPolicyDigest:'b'.repeat(64),backupId:b.middle.backupId,pointId:id,stage:'reserved',reservedAt:now,updatedAt:now};
 await writeManifest(join(cycles,id+'.enc'),row,f.record.applicationKey);assert.equal((await createPostgresBackupRetentionPlan(f.context,b.policy)).deleteDatabases,0,'authenticated unfinished work keeps the entire base');
 await writeManifest(join(cycles,id+'.enc'),{...row,stage:'failed',failureCode:'synthetic_capture_cancelled'},f.record.applicationKey,{replace:true});assert.equal((await createPostgresBackupRetentionPlan(f.context,b.policy)).deleteDatabases,1,'closed authenticatable failures do not pin forever');
 const statePath=join(f.record.artifactDirectory,'periodic-application-state.enc'),state={version:3,scope:'harbor-periodic-application-state-v3',configurationDigest:f.record.configurationDigest,systemId:f.record.expected.system_id,keyVersion:f.record.keyVersion,applicationKeyVersion:f.record.applicationKeyVersion,applicationKeyDigest:f.record.applicationKeyDigest,latestBackup:null,lastVerified:null,pending:null,pendingVerification:null,updatedAt:Date.now()};
 await writeManifest(statePath,{...state,version:2,scope:'harbor-periodic-application-state-v2'},f.record.applicationKey);await assert.rejects(createPostgresBackupRetentionPlan(f.context,b.policy),{code:'periodic_retention_state_binding'});await rm(statePath);
 for(const name of ['pending','pendingVerification']){
  await writeManifest(statePath,{...state,[name]:{backupId:b.middle.backupId,pointId:id}},f.record.applicationKey,{replace:true});assert.equal((await createPostgresBackupRetentionPlan(f.context,b.policy)).deleteDatabases,0,name+' protects its referenced base');await rm(statePath);
 }
 for(const name of ['latestBackup','lastVerified']){
  const reference={backupId:b.middle.backupId,pointId:id,checkpointAt:now,...(name==='latestBackup'?{completedAt:now}:{verifiedAt:now})};
  await writeManifest(statePath,{...state,[name]:reference},f.record.applicationKey,{replace:true});await assert.rejects(createPostgresBackupRetentionPlan(f.context,b.policy),{code:'backup_retention_application_reference_binding'});await rm(statePath);
 }
 const pointRoot=join(f.record.artifactDirectory,b.first.backupId,'points');await mkdir(pointRoot,{mode:0o700});const orphan=join(pointRoot,randomUUID()),layoutPlan=await createPostgresBackupRetentionPlan(f.context,b.policy);
 await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,layoutPlan.planId,{onTransition:async phase=>{if(phase==='inventory_authenticated')await mkdir(orphan,{mode:0o700});}}),{code:'backup_retention_plan_stale'});await assert.rejects(stat(join(f.record.artifactDirectory,'.retention',layoutPlan.planId,'journal.enc')),{code:'ENOENT'});await rm(orphan,{recursive:true});
 const plan=await createPostgresBackupRetentionPlan(f.context,b.policy);
 await assert.rejects(applyPostgresBackupRetention(f.context,b.policy,plan.planId,{onTransition:async phase=>{if(phase==='inventory_authenticated')await writeManifest(statePath,{...state,pendingVerification:{backupId:b.middle.backupId,pointId:id}},f.record.applicationKey);}}),{code:'backup_retention_candidate_now_referenced'});
 await assert.rejects(stat(join(f.record.artifactDirectory,'.retention',plan.planId,'journal.enc')),{code:'ENOENT'});await rm(statePath);assert.equal((await applyPostgresBackupRetention(f.context,b.policy,plan.planId)).retentionApplied,true);
});


const periodicFailureCode=value=>/^[a-z][a-z0-9_]{0,95}$/.test(value?.failureCode??'')?value.failureCode:'no_sanitized_failure_code';
async function periodicPolicyFixture(f){
 const materialRoots=[];for(const id of ['data','runtime','secrets']){const directory=join(f.lab.directory,'periodic-policy-'+id);await mkdir(directory,{mode:0o700});materialRoots.push({id,directory});}
 await writeFile(join(materialRoots[0].directory,'synthetic.txt'),'retention policy input',{mode:0o600});const database=f.record.expected.databases.find(row=>!['postgres','template0','template1'].includes(row.name))??f.record.expected.databases.find(row=>row.name==='postgres');
 assert.ok(database);return {version:1,enabled:true,database:{name:database.name,oid:database.oid},materialRoots,publicOrigin:'http://127.0.0.1:43127',deviceOrigin:'https://127.0.0.1:43128',maxBytes:1048576,maxFiles:100};
}
test('lightweight periodic completion orders application groups by checkpoint and refreshes minimum age on an old base',real,async t=>{
 const {capturePeriodicApplicationBackup}=await import('../postgres-periodic-application-backup.mjs'),f=await fixture(t,{application:true}),b=await backups(f),configuration=await periodicPolicyFixture(f);
 // Actual capture/checkpoint producer, without a PITR or a fabricated verified marker.
 const first=await capturePeriodicApplicationBackup(f.context,b.first,configuration);assert.equal(first.backupComplete,true,'first capture: '+periodicFailureCode(first));assert.equal(first.restoreVerified,false);
 const middle=await capturePeriodicApplicationBackup(f.context,b.middle,configuration);assert.equal(middle.backupComplete,true,'middle capture: '+periodicFailureCode(middle));
 const firstBase=join(f.record.artifactDirectory,b.first.backupId),middleBase=join(f.record.artifactDirectory,b.middle.backupId),firstPoint=await readManifest(join(firstBase,'points',first.pointId,'backup-complete.enc'),f.record.applicationKey),middlePoint=await readManifest(join(middleBase,'points',middle.pointId,'backup-complete.enc'),f.record.applicationKey),middleMetadata=await readManifest(join(middleBase,'backup.enc'),f.key);
 const applicationFiles=join(firstBase,'application-staging',first.pointId,'files.enc');await assert.rejects(readManifest(applicationFiles,f.key),{code:'transfer_manifest_authentication_failed'});assert.equal((await readManifest(applicationFiles,f.record.applicationKey)).roots.length,3);
 let observed=Date.now();assert.ok(observed>=Math.max(firstPoint.completedAt,middlePoint.completedAt));const clock=()=>observed,minAgeMs=clock()-middleMetadata.created;assert.ok(minAgeMs>clock()-firstPoint.completedAt);
 const protectedPlan=await createPostgresBackupRetentionPlan(f.context,{...policy,minAgeMs},{clock});assert.equal(protectedPlan.deleteDatabases,0,'recent application completion protects an old base beyond its original creation age');
 const initial=await createPostgresBackupRetentionPlan(f.context,policy,{clock});assert.equal(initial.deleteDatabases,1);assert.equal((await planGroups(f,initial.planId)).some(group=>group.type==='database'&&group.source===b.first.backupId),true);
 const newest=await capturePeriodicApplicationBackup(f.context,b.first,configuration);assert.equal(newest.backupComplete,true,'newest capture: '+periodicFailureCode(newest));const source=join(firstBase,'points',newest.pointId,'backup-complete.enc'),newestPoint=await readManifest(source,f.record.applicationKey);observed=Date.now();assert.ok(observed>=newestPoint.completedAt);
 const plan=await createPostgresBackupRetentionPlan(f.context,policy,{clock});assert.equal(plan.deleteDatabases,1);assert.equal(plan.retainedApplicationGroups,1);assert.equal((await planGroups(f,plan.planId)).some(group=>group.type==='database'&&group.source===b.middle.backupId),true,'a newer checkpoint on the older base wins independently from base order');
 const saved=await readFile(source);await writeFile(source,'damaged-completion',{mode:0o600});await assert.rejects(applyPostgresBackupRetention(f.context,policy,plan.planId,{clock}),{code:'transfer_manifest_authentication_failed'});await writeFile(source,saved,{mode:0o600});
 await assert.rejects(applyPostgresBackupRetention(f.context,policy,plan.planId,{clock,onTransition:phase=>{if(phase==='prepared')throw Error('synthetic-v3-retirement-interruption');}}),/synthetic-v3-retirement-interruption/);
 const journalPath=join(f.record.artifactDirectory,'.retention',plan.planId,'journal.enc'),journalBefore=await readFile(journalPath);await writeFile(source,'damaged-retained-completion',{mode:0o600});await assert.rejects(applyPostgresBackupRetention(f.context,policy,plan.planId,{clock}),{code:'transfer_manifest_authentication_failed'});assert.deepEqual(await readFile(journalPath),journalBefore,'a failed retained-point reauthentication never advances a journal cursor');await writeFile(source,saved,{mode:0o600});
 assert.equal((await applyPostgresBackupRetention(f.context,policy,plan.planId,{clock})).retentionApplied,true);await assert.rejects(stat(middleBase),{code:'ENOENT'});assert.equal((await stat(source)).isFile(),true);assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.retention_probe')).n,4);
});
