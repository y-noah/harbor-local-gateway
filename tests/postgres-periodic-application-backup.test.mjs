import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
import {randomBytes} from 'node:crypto';
import {mkdir,writeFile,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {maintainPostgresBackupBase,runPostgresBackupScheduleCycle} from '../postgres-backup-schedule.mjs';
import {capturePeriodicApplicationBackup,periodicApplicationBackupSource,readPeriodicApplicationBackupState} from '../postgres-periodic-application-backup.mjs';
import {readManifest} from '../migration-artifacts.mjs';

test('disabled producer and base maintenance perform no database work; JSON cannot grant archived restore authority',async()=>{
  assert.equal((await capturePeriodicApplicationBackup(null,null,{version:1,enabled:false})).state,'periodic_application_disabled');
  assert.equal((await maintainPostgresBackupBase(null,{version:1,enabled:false})).state,'base_maintenance_disabled');
  assert.throws(()=>periodicApplicationBackupSource({scope:'harbor-periodic-application-backup-v3'}),{code:'periodic_application_backup_required'});
});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('real application producer preserves old active base while independent maintenance builds and authenticates a replacement',{
  skip:!binDirectory?'需自有PG18工具，不连接共享环境':false,timeout:180000
},async t=>{
  const key=randomBytes(32),applicationKey=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:key}}),store=new PostgresStore({connection:lab.connection});let context,maintenanceContext,release;
  const originalLink=fs.promises.link;
  t.after(async()=>{release?.();fs.promises.link=originalLink;syncBuiltinESMExports();try{if(context)await closePostgresBackupContext(context);if(maintenanceContext)await closePostgresBackupContext(maintenanceContext);}finally{try{await store.close();}finally{await lab.close();key.fill(0);applicationKey.fill(0);}}});
  await store.migrate();await store.initializePricing();const database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid'),materialRoots=[];
  for(const id of ['data','runtime','secrets']){const directory=join(lab.directory,'material-'+id);await mkdir(directory,{mode:0o700});materialRoots.push({id,directory});await writeFile(join(directory,'synthetic.txt'),'synthetic '+id,{mode:0o600});}
  const application={version:1,enabled:true,database,materialRoots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:4096,maxFiles:20},configuration={version:1,enabled:true,fullIntervalMs:1000,checkpointIntervalMs:1000,pollIntervalMs:1000,pinnedBackupIds:[]},directory=join(lab.directory,'periodic-backups');
  context=await createTaskPostgresBackupContext({laboratory:lab,directory,archiveKey:key,applicationKey,binDirectory,libraryDirectory});maintenanceContext=await createTaskPostgresBackupContext({laboratory:lab,directory,archiveKey:key,applicationKey,binDirectory,libraryDirectory});
  let offset=0;const clock=()=>Date.now()+offset,options={clock,applicationConfiguration:application};
  assert.equal((await runPostgresBackupScheduleCycle(context,configuration,options)).state,'bootstrap_not_ready');
  const initial=await maintainPostgresBackupBase(maintenanceContext,configuration,options);assert.equal(initial.state,'base_maintenance_completed');
  const first=await runPostgresBackupScheduleCycle(context,configuration,options);assert.equal(first.application.state,'application_backup_completed',first.application.failureCode);assert.equal(first.backupId,initial.backupId);assert.equal(first.application.restoreVerified,false);
  offset+=2000;
  let enteredResolve;const entered=new Promise(resolve=>{enteredResolve=resolve;}),blocked=new Promise(resolve=>{release=resolve;});
  fs.promises.link=async(source,target)=>{if(target.endsWith('/backup.enc')&&!target.includes('/'+initial.backupId+'/')){enteredResolve();await blocked;}return originalLink(source,target);};syncBuiltinESMExports();
  const maintenance=maintainPostgresBackupBase(maintenanceContext,configuration,options);void maintenance.catch(()=>{});
  const timeout=new AbortController();try{await Promise.race([entered,delay(30000,undefined,{signal:timeout.signal}).then(()=>assert.fail('new physical base never reached durable publication'))]);}finally{timeout.abort();}
  const during=await readManifest(join(directory,'periodic-state.enc'),key);assert.equal(during.active.backupId,initial.backupId);assert.notEqual(during.pending.backupId,initial.backupId);
  const concurrent=await runPostgresBackupScheduleCycle(context,configuration,options);assert.equal(concurrent.application.state,'application_backup_completed',concurrent.application.failureCode);assert.equal(concurrent.backupId,initial.backupId);assert.notEqual(concurrent.application.pointId,first.application.pointId);
  release();const replaced=await maintenance;assert.equal(replaced.state,'base_maintenance_completed');assert.notEqual(replaced.backupId,initial.backupId);
  const after=await readManifest(join(directory,'periodic-state.enc'),key);assert.equal(after.active.backupId,replaced.backupId);assert.equal(after.pending,null);
  const state=await readPeriodicApplicationBackupState(context);assert.equal(state.latestBackup.pointId,concurrent.application.pointId);assert.equal(state.lastVerified,null);
  assert.deepEqual(await readdir(postgresBackupContext(context).restoreDirectory),[],'neither light producer nor base maintenance runs PITR');
});
