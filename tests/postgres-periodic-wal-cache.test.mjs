import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {join} from 'node:path';
import {readFile,writeFile} from 'node:fs/promises';
import {setTimeout as delay} from 'node:timers/promises';
import {createPrivatePostgres,startOwnedCapacityChild} from '../tools/capacity/owned-runtime.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,verifyBackupCluster,postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPostgresBaseBackup,checkpointPostgresBackup} from '../postgres-backup.mjs';
import {readManifest} from '../migration-artifacts.mjs';
import {readWalCheckpointSegments} from '../postgres-backup-wal-catalog.mjs';
import {inspectPeriodicArchivedWal} from '../postgres-periodic-wal-cache.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('periodic WAL cache binds unchanged real bytes and generation, while replacements, corruption and scrub are reverified',{
  skip:!binDirectory?'需自有PG18，不连接共享数据库':false,timeout:180000
},async t=>{
  const key=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:key}});let context;
  t.after(async()=>{try{if(context)await closePostgresBackupContext(context);}finally{await lab.close();key.fill(0);}});
  context=await createTaskPostgresBackupContext({laboratory:lab,directory:join(lab.directory,'cache-backups'),archiveKey:key,binDirectory,libraryDirectory});
  const proof=await createPostgresBaseBackup(context);await checkpointPostgresBackup(proof);
  const record=postgresBackupContext(context),checkpoint=await readManifest(join(record.artifactDirectory,proof.backupId,'checkpoint.enc'),key);let name;
  for await(const row of readWalCheckpointSegments({directory:join(record.artifactDirectory,proof.backupId),key,checkpoint})){name=row.name;break;}
  const sourceGeneration=(await verifyBackupCluster(context)).postmaster_start,first=await inspectPeriodicArchivedWal(context,name,{sourceGeneration}),second=await inspectPeriodicArchivedWal(context,name,{sourceGeneration});
  assert.equal(first.cacheHit,false);assert.equal(second.cacheHit,true);assert.equal(second.verifiedAt,first.verifiedAt,'identity reuse never claims a fresh byte scrub');
  const path=join(record.archiveDirectory,name+'.enc'),original=await readFile(path),corrupt=Buffer.from(original);corrupt[50]^=1;
  await writeFile(path,corrupt,{mode:0o600});await assert.rejects(inspectPeriodicArchivedWal(context,name,{sourceGeneration}),{code:'recovery_wal_conflict'});
  await writeFile(path,original,{mode:0o600});assert.equal((await inspectPeriodicArchivedWal(context,name,{sourceGeneration})).cacheHit,false);
  assert.equal((await inspectPeriodicArchivedWal(context,name,{sourceGeneration,scrub:true})).cacheHit,false,'explicit scrub authenticates all bytes');
  await lab.runtime.stop(lab.process);
  const restarted=startOwnedCapacityChild(lab.runtime,join(binDirectory,'postgres'),['-D',lab.dataDirectory],{role:'database',env:{LD_LIBRARY_PATH:libraryDirectory??''}}),deadline=Date.now()+15000;let current;
  for(;;){assert.equal(restarted.exitCode,null);try{current=await verifyBackupCluster(context);if(current.postmaster_start!==sourceGeneration)break;}catch{}assert.ok(Date.now()<deadline,'owned PostgreSQL restart did not become ready');await delay(10);}
  assert.equal((await inspectPeriodicArchivedWal(context,name,{sourceGeneration:current.postmaster_start})).cacheHit,false,'real source restart invalidates a previous generation cache entry');
});
