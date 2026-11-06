import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {readFile,readdir,writeFile,mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,startOwnedCapacityChild} from '../tools/capacity/owned-runtime.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {verifyPostgresBackup,checkpointPostgresBackup,checkpointPeriodicPostgresBackup,withPostgresBackupCheckpointLock} from '../postgres-backup.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from '../postgres-backup-restore.mjs';
import {readManifest,writeManifest,digestFile} from '../migration-artifacts.mjs';
import {acquireInstance} from '../instance.mjs';
import {postgresBackupSchedulePolicy,runPostgresBackupScheduleCycle,watchPostgresBackupSchedule,maintainPostgresBackupBase} from '../postgres-backup-schedule.mjs';

const policy={version:1,enabled:true,fullIntervalMs:10000,checkpointIntervalMs:1000,pollIntervalMs:1000};
test('periodic backup policy is opt-in, bounded and cannot reinterpret unknown options',async()=>{
  assert.equal((await runPostgresBackupScheduleCycle(null,{version:1,enabled:false})).state,'schedule_disabled');
  assert.equal((await watchPostgresBackupSchedule(null,{version:1,enabled:false})).deploymentAllowed,false);
  assert.equal(postgresBackupSchedulePolicy({version:1,enabled:true}).fullIntervalMs,86400000);
  for(const value of [{...policy,enabled:'1'},{...policy,pollIntervalMs:2000},{...policy,fullIntervalMs:999},{...policy,checkpointIntervalMs:10001},{...policy,unknown:true},{...policy,pinnedBackupIds:['../backup']},{...policy,pinnedBackupIds:Array(2).fill('11111111-1111-1111-1111-111111111111')}])assert.throws(()=>postgresBackupSchedulePolicy(value),{code:'backup_schedule_policy_invalid'});
});

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const real={skip:!binDirectory?'需要本任务独占PG18工具，不连接共享数据库':false,timeout:180000};
async function fixture(t,{application=false}={}){
  const key=randomBytes(32),applicationKey=application?randomBytes(32):null,lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:key}}),store=new PostgresStore({connection:lab.connection});let context;
  t.after(async()=>{try{if(context)await closePostgresBackupContext(context);}finally{try{await store.close();}finally{await lab.close();key.fill(0);applicationKey?.fill(0);}}});
  await store.migrate();await store.initializePricing();const directory=join(lab.directory,'periodic-backups');
  context=await createTaskPostgresBackupContext({laboratory:lab,directory,archiveKey:key,binDirectory,libraryDirectory,...(applicationKey?{applicationKey,applicationKeyVersion:'task-application-v3'}:{})});
  return {key,lab,store,context,directory,stateFile:join(directory,'periodic-state.enc')};
}
test('periodic real base/WAL cycle resumes, pins immutable baselines and restores a genuine checkpoint',real,async t=>{
  const f=await fixture(t);let now=Date.now();const options={clock:()=>now};
  const first=await runPostgresBackupScheduleCycle(f.context,policy,options);assert.equal(first.state,'base_and_checkpoint_created');assert.equal(first.applicationMaterialsVerified,false);
  const before=await readFile(join(f.directory,first.backupId,'checkpoint.enc'));
  assert.equal((await runPostgresBackupScheduleCycle(f.context,policy,options)).state,'checkpoint_not_due');assert.deepEqual(await readFile(join(f.directory,first.backupId,'checkpoint.enc')),before);
  now+=1000;const checkpoint=await runPostgresBackupScheduleCycle(f.context,policy,options);assert.equal(checkpoint.state,'checkpoint_created');assert.equal(checkpoint.backupId,first.backupId);
  assert.equal(inspectPostgresBackupRestore(await restorePostgresBackup(await verifyPostgresBackup(f.context,first.backupId))).restoreVerified,true);
  const pinned=await readFile(join(f.directory,first.backupId,'checkpoint.enc'));now+=1000;
  const second=await runPostgresBackupScheduleCycle(f.context,{...policy,pinnedBackupIds:[first.backupId]},options);assert.notEqual(second.backupId,first.backupId);assert.deepEqual(await readFile(join(f.directory,first.backupId,'checkpoint.enc')),pinned);
  const lock=await acquireInstance(join(f.directory,'.periodic-backup'));try{await assert.rejects(runPostgresBackupScheduleCycle(f.context,policy,options),{code:'backup_schedule_busy'});}finally{await lock.release();}
  now+=10000;const third=await runPostgresBackupScheduleCycle(f.context,policy,options);assert.notEqual(third.backupId,second.backupId);assert.equal((await readManifest(f.stateFile,f.key)).pending,null);
  const cold=await import('../postgres-backup-schedule.mjs?cold-reload');
  assert.equal((await cold.runPostgresBackupScheduleCycle(f.context,policy,options)).backupId,third.backupId);
  const payload=join(f.directory,third.backupId,(await readdir(join(f.directory,third.backupId))).find(name=>/^file-\d+\.enc$/.test(name))),original=await readFile(payload),damaged=Buffer.from(original);damaged[0]^=1;await writeFile(payload,damaged);
  const anotherCold=await import('../postgres-backup-schedule.mjs?cold-corrupt');await assert.rejects(anotherCold.runPostgresBackupScheduleCycle(f.context,policy,options));await writeFile(payload,original);
  const saved=await readFile(f.stateFile),names=await readdir(f.directory);await writeFile(f.stateFile,Buffer.from('invalid authenticated state'),{mode:0o600});await assert.rejects(runPostgresBackupScheduleCycle(f.context,policy,options));assert.deepEqual(await readdir(f.directory),names);await writeFile(f.stateFile,saved,{mode:0o600});
  now--;await assert.rejects(runPostgresBackupScheduleCycle(f.context,policy,options),{code:'backup_schedule_clock_reversed'});assert.deepEqual(await readFile(f.stateFile),saved);
});

test('authenticated application references seal manual checkpoints and rotate the periodic base without a configured pin',real,async t=>{
 const f=await fixture(t),first=await runPostgresBackupScheduleCycle(f.context,policy),directory=join(f.directory,first.backupId),checkpoint=await readFile(join(directory,'checkpoint.enc')),record=postgresBackupContext(f.context),proof=await verifyPostgresBackup(f.context,first.backupId);
 const application=join(directory,'application');await mkdir(application,{mode:0o700});
 const reference={version:1,backupId:first.backupId,configurationDigest:record.configurationDigest,keyVersion:record.keyVersion,pendingBindingDigest:randomBytes(32).toString('hex'),files:{manifestDigest:await digestFile(join(directory,'files.enc'))},materialRoots:[{id:'data',directory:'synthetic-private-data'}],materialKeys:[],checkpointDigest:await digestFile(join(directory,'checkpoint.enc'))};
 await writeManifest(join(application,'application.enc'),reference,f.key);
 await assert.rejects(checkpointPostgresBackup(proof),{code:'backup_checkpoint_sealed'});assert.deepEqual(await readFile(join(directory,'checkpoint.enc')),checkpoint);
 await withPostgresBackupCheckpointLock(f.context,first.backupId,async()=>{await assert.rejects(checkpointPostgresBackup(proof),{code:'backup_operation_busy'});});
 assert.equal(record.busy,false);await assert.rejects(checkpointPostgresBackup(proof),{code:'backup_checkpoint_sealed'});
 const second=await runPostgresBackupScheduleCycle(f.context,policy);assert.equal(second.state,'base_and_checkpoint_created');assert.notEqual(second.backupId,first.backupId);assert.deepEqual(await readFile(join(directory,'checkpoint.enc')),checkpoint);
 // Invalid references must fail before even a due or explicitly pinned cycle
 // can change its state or create another base.
 const secondDirectory=join(f.directory,second.backupId),secondApplication=join(secondDirectory,'application');await mkdir(secondApplication,{mode:0o700});
 const state=await readFile(f.stateFile),before=await readdir(f.directory),secondProof=await verifyPostgresBackup(f.context,second.backupId),secondCheckpoint=await readFile(join(secondDirectory,'checkpoint.enc'));
 const valid={...reference,backupId:second.backupId,files:{manifestDigest:await digestFile(join(secondDirectory,'files.enc'))},checkpointDigest:await digestFile(join(secondDirectory,'checkpoint.enc'))};
 for(const [value,code] of [[{...valid,version:2},'backup_checkpoint_reference_invalid'],[{...valid,backupId:first.backupId},'backup_checkpoint_reference_invalid'],[{...valid,checkpointDigest:null},'backup_checkpoint_reference_unbound'],[{...valid,checkpointDigest:{...valid.checkpointDigest,sha256:'0'.repeat(64)}},'backup_checkpoint_reference_changed']]){
  await writeManifest(join(secondApplication,'application.enc'),value,f.key,{replace:true});
  await assert.rejects(checkpointPostgresBackup(secondProof),{code});await assert.rejects(runPostgresBackupScheduleCycle(f.context,{...policy,pinnedBackupIds:[second.backupId]}),{code});
  assert.deepEqual(await readFile(f.stateFile),state);assert.deepEqual(await readdir(f.directory),before);assert.deepEqual(await readFile(join(secondDirectory,'checkpoint.enc')),secondCheckpoint);
 }
 await writeFile(join(secondApplication,'application.enc'),Buffer.from('invalid authenticated reference'),{mode:0o600});
 await assert.rejects(checkpointPostgresBackup(secondProof),{code:'transfer_manifest_authentication_failed'});await assert.rejects(runPostgresBackupScheduleCycle(f.context,policy),{code:'transfer_manifest_authentication_failed'});
 assert.deepEqual(await readFile(join(secondDirectory,'checkpoint.enc')),secondCheckpoint);assert.deepEqual(await readFile(f.stateFile),state);
});

test('an interrupted new cycle preserves its UUID and resumes the same authenticated backup',real,async t=>{
  const f=await fixture(t),controller=new AbortController();let finished=false;
  const running=runPostgresBackupScheduleCycle(f.context,policy,{signal:controller.signal});void running.finally(()=>{finished=true;}).catch(()=>{});
  const until=Date.now()+10000;let pending;
  while(Date.now()<until&&!finished){try{pending=(await readManifest(f.stateFile,f.key)).pending;if(pending){controller.abort();break;}}catch(error){if(error.code!=='ENOENT')throw error;}await delay(1);}
  assert.ok(pending,'pending identity must be durable before physical work');await assert.rejects(running);
  const result=await runPostgresBackupScheduleCycle(f.context,policy);assert.equal(result.backupId,pending.backupId);assert.equal(result.state,'base_and_checkpoint_created');assert.equal((await readManifest(f.stateFile,f.key)).pending,null);
  const stop=new AbortController(),seen=[];
  const stopped=await watchPostgresBackupSchedule(f.context,policy,{signal:stop.signal,onCycle:value=>{seen.push(value);stop.abort();}});
  assert.equal(stopped.state,'schedule_stopped');assert.equal(seen.length,1);assert.equal(seen[0].backupId,pending.backupId);
  const failedStop=new AbortController();await assert.rejects(watchPostgresBackupSchedule(f.context,policy,{signal:failedStop.signal,onCycle:()=>{failedStop.abort();throw Object.assign(Error('cleanup failed'),{code:'backup_process_cleanup_unknown'});}}),{code:'backup_process_cleanup_unknown'});
});

test('a durable pending intent resumes an empty directory left before its initialization journal, but rejects unknown contents',real,async t=>{
  const f=await fixture(t),record=postgresBackupContext(f.context);
  const intent=async id=>{const now=Date.now();await writeManifest(f.stateFile,{version:1,scope:'harbor-periodic-database-backup-v1',configurationDigest:record.configurationDigest,systemId:record.expected.system_id,keyVersion:record.keyVersion,active:null,pending:{backupId:id,postmasterStart:record.expected.postmaster_start,timeline:record.expected.timeline,createdAt:now},updatedAt:now},f.key,{replace:true});await mkdir(join(f.directory,id),{mode:0o700});};
  const id=randomUUID();await intent(id);const result=await runPostgresBackupScheduleCycle(f.context,policy);assert.equal(result.backupId,id);assert.equal(result.state,'base_and_checkpoint_created');
  const foreign=randomUUID();await intent(foreign);const path=join(f.directory,foreign,'unknown-file');await writeFile(path,'preserve',{mode:0o600});await assert.rejects(runPostgresBackupScheduleCycle(f.context,policy),{code:'backup_incomplete_state_missing'});assert.equal(await readFile(path,'utf8'),'preserve');assert.deepEqual(await readdir(join(f.directory,foreign)),['unknown-file']);
});

test('a real postmaster generation change creates a new base without modifying the old checkpoint',real,async t=>{
  const f=await fixture(t),first=await runPostgresBackupScheduleCycle(f.context,policy),old=await readFile(join(f.directory,first.backupId,'checkpoint.enc'));
  await f.lab.runtime.stop(f.lab.process);const child=startOwnedCapacityChild(f.lab.runtime,join(binDirectory,'postgres'),['-D',f.lab.dataDirectory],{role:'database',env:{LD_LIBRARY_PATH:libraryDirectory??''}});
  const probe=new PostgresStore({connection:{...f.lab.connection,host:f.lab.socketDirectory,port:f.lab.databasePort}});
  try{const until=Date.now()+10000;let ready=false;while(Date.now()<until){assert.equal(child.exitCode,null);try{ready=(await probe.get('SELECT 1 n')).n===1;if(ready)break;}catch{}await delay(10);}assert.equal(ready,true);}
  finally{await probe.close();}
  const second=await runPostgresBackupScheduleCycle(f.context,policy);assert.notEqual(second.backupId,first.backupId);assert.equal(second.state,'base_and_checkpoint_created');assert.deepEqual(await readFile(join(f.directory,first.backupId,'checkpoint.enc')),old);
});


test('application producer rejects an incompatible old base after parameter restart and maintenance rotates before interval for genuine source-free recovery',real,async t=>{
  const {capturePeriodicApplicationBackup}=await import('../postgres-periodic-application-backup.mjs'),{verifyPostgresBackupRecoveryPoint}=await import('../postgres-backup-recovery-point.mjs'),f=await fixture(t,{application:true}),record=postgresBackupContext(f.context),materialRoots=[];
  for(const id of ['data','runtime','secrets']){const directory=join(f.lab.directory,'parameter-material-'+id);await mkdir(directory,{mode:0o700});await writeFile(join(directory,'synthetic.txt'),'owned parameter-restart material',{mode:0o600});materialRoots.push({id,directory});}
  const database=await f.store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid'),applicationConfiguration={version:1,enabled:true,database,materialRoots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:4096,maxFiles:20},configuration={version:1,enabled:true,fullIntervalMs:86400000,checkpointIntervalMs:300000,pollIntervalMs:1000,pinnedBackupIds:[]},options={applicationConfiguration};
  const initial=await maintainPostgresBackupBase(f.context,configuration,options);assert.equal(initial.state,'base_maintenance_completed');const first=await runPostgresBackupScheduleCycle(f.context,configuration,options);assert.equal(first.application.backupComplete,true,first.application.failureCode);
  assert.equal((await maintainPostgresBackupBase(f.context,configuration,options)).state,'base_maintenance_not_due');
  const oldProof=await verifyPostgresBackup(f.context,initial.backupId),oldMetadata=await readManifest(join(f.directory,initial.backupId,'backup.enc'),f.key),oldCheckpoint=await readFile(join(f.directory,initial.backupId,'checkpoint.enc')),increased=oldMetadata.recoverySettings.max_connections+1;
  let child=f.lab.process,previousGeneration=record.expected.postmaster_start;
  const restart=async connections=>{
    await f.lab.runtime.stop(child);child=startOwnedCapacityChild(f.lab.runtime,join(binDirectory,'postgres'),['-D',f.lab.dataDirectory,'-c','max_connections='+connections],{role:'database',env:{LD_LIBRARY_PATH:libraryDirectory??''}});const probe=new PostgresStore({connection:{...f.lab.connection,host:f.lab.socketDirectory,port:f.lab.databasePort}});
    try{const deadline=Date.now()+15000;let ready=false;while(Date.now()<deadline){assert.equal(child.exitCode,null);try{const observed=await probe.get("SELECT current_setting('max_connections')::integer connections,extract(epoch FROM pg_postmaster_start_time())::text started,(SELECT timeline_id::integer FROM pg_control_checkpoint()) timeline");ready=observed.connections===connections&&observed.started!==previousGeneration&&observed.timeline===oldMetadata.timeline;if(ready){previousGeneration=observed.started;break;}}catch{}await delay(10);}assert.equal(ready,true,'owned source restarts with the requested real parameter on the same timeline');}finally{await probe.close();}
  };
  await restart(increased);
  await assert.rejects(checkpointPostgresBackup(oldProof),{code:'backup_source_restarted'});await assert.rejects(checkpointPeriodicPostgresBackup(oldProof),{code:'backup_source_restarted'});
  const blocked=await runPostgresBackupScheduleCycle(f.context,configuration,options);assert.equal(blocked.state,'base_maintenance_required');assert.equal(blocked.reason,'source_generation_changed');
  const direct=await capturePeriodicApplicationBackup(f.context,oldProof,applicationConfiguration);assert.equal(direct.backupComplete,false);assert.equal(direct.failureCode,'backup_source_restarted');assert.deepEqual(await readFile(join(f.directory,initial.backupId,'checkpoint.enc')),oldCheckpoint);
  await restart(oldMetadata.recoverySettings.max_connections);
  await assert.rejects(checkpointPostgresBackup(oldProof),{code:'backup_source_restarted'});await assert.rejects(checkpointPeriodicPostgresBackup(oldProof),{code:'backup_source_restarted'});
  const lowered=await runPostgresBackupScheduleCycle(f.context,configuration,options);assert.equal(lowered.state,'base_maintenance_required');assert.equal(lowered.reason,'source_generation_changed','returning to the old values cannot prove the WAL high-water mark');
  const rejected=await capturePeriodicApplicationBackup(f.context,oldProof,applicationConfiguration);assert.equal(rejected.backupComplete,false);assert.equal(rejected.failureCode,'backup_source_restarted');assert.deepEqual(await readFile(join(f.directory,initial.backupId,'checkpoint.enc')),oldCheckpoint);
  const oldPoint=await verifyPostgresBackupRecoveryPoint(f.context,oldProof,first.application.pointId),oldRestored=inspectPostgresBackupRestore(await restorePostgresBackup(oldProof,{recoveryPoint:oldPoint}));assert.equal(oldRestored.restoreVerified,true);assert.equal(oldRestored.recoverySettings.max_connections,oldMetadata.recoverySettings.max_connections,'an already authenticated point before both restarts remains restorable');
  const replacement=await maintainPostgresBackupBase(f.context,configuration,options);assert.equal(replacement.state,'base_maintenance_completed');assert.notEqual(replacement.backupId,initial.backupId,'a different source generation rotates before the daily interval even after parameters return');
  const current=await runPostgresBackupScheduleCycle(f.context,configuration,options);assert.equal(current.application.backupComplete,true,current.application.failureCode);assert.equal(current.backupId,replacement.backupId);const metadata=await readManifest(join(f.directory,replacement.backupId,'backup.enc'),f.key);assert.equal(metadata.recoverySettings.max_connections,oldMetadata.recoverySettings.max_connections);
  const proof=await verifyPostgresBackup(f.context,replacement.backupId),point=await verifyPostgresBackupRecoveryPoint(f.context,proof,current.application.pointId);await f.lab.runtime.stop(child);
  const restored=inspectPostgresBackupRestore(await restorePostgresBackup(proof,{recoveryPoint:point}));assert.equal(restored.restoreVerified,true);assert.equal(restored.recoverySettings.max_connections,oldMetadata.recoverySettings.max_connections);assert.equal(restored.recoveryPointId,current.application.pointId);assert.deepEqual(await readFile(join(f.directory,initial.backupId,'checkpoint.enc')),oldCheckpoint);
});
