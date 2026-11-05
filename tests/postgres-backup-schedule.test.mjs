import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {readFile,readdir,writeFile,mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,startOwnedCapacityChild} from '../tools/capacity/owned-runtime.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {verifyPostgresBackup} from '../postgres-backup.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from '../postgres-backup-restore.mjs';
import {readManifest,writeManifest} from '../migration-artifacts.mjs';
import {acquireInstance} from '../instance.mjs';
import {postgresBackupSchedulePolicy,runPostgresBackupScheduleCycle,watchPostgresBackupSchedule} from '../postgres-backup-schedule.mjs';

const policy={version:1,enabled:true,fullIntervalMs:10000,checkpointIntervalMs:1000,pollIntervalMs:1000};
test('periodic backup policy is opt-in, bounded and cannot reinterpret unknown options',async()=>{
  assert.equal((await runPostgresBackupScheduleCycle(null,{version:1,enabled:false})).state,'schedule_disabled');
  assert.equal((await watchPostgresBackupSchedule(null,{version:1,enabled:false})).deploymentAllowed,false);
  assert.equal(postgresBackupSchedulePolicy({version:1,enabled:true}).fullIntervalMs,86400000);
  for(const value of [{...policy,enabled:'1'},{...policy,pollIntervalMs:2000},{...policy,fullIntervalMs:999},{...policy,checkpointIntervalMs:10001},{...policy,unknown:true},{...policy,pinnedBackupIds:['../backup']},{...policy,pinnedBackupIds:Array(2).fill('11111111-1111-1111-1111-111111111111')}])assert.throws(()=>postgresBackupSchedulePolicy(value),{code:'backup_schedule_policy_invalid'});
});

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const real={skip:!binDirectory?'需要本任务独占PG18工具，不连接共享数据库':false,timeout:180000};
async function fixture(t){
  const key=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:key}}),store=new PostgresStore({connection:lab.connection});let context;
  t.after(async()=>{try{if(context)await closePostgresBackupContext(context);}finally{try{await store.close();}finally{await lab.close();key.fill(0);}}});
  await store.migrate();await store.initializePricing();const directory=join(lab.directory,'periodic-backups');
  context=await createTaskPostgresBackupContext({laboratory:lab,directory,archiveKey:key,binDirectory,libraryDirectory});
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
