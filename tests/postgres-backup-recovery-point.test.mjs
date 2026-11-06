import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {join} from 'node:path';
import {mkdir,readFile,writeFile,readdir} from 'node:fs/promises';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPostgresBaseBackup,checkpointPostgresBackup} from '../postgres-backup.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore,verifyPostgresBackupRestore} from '../postgres-backup-restore.mjs';
import {readManifest,digestFile} from '../migration-artifacts.mjs';
import {capturePostgresBackupRecoveryPoint,verifyPostgresBackupRecoveryPoint,postgresBackupRecoveryPointSource} from '../postgres-backup-recovery-point.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const real={skip:!binDirectory?'需私有PG18；不连接共享数据库':false,timeout:180000};
async function fixture(t){
  const key=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:key}}),store=new PostgresStore({connection:lab.connection});let context;
  t.after(async()=>{try{if(context)await closePostgresBackupContext(context);}finally{try{await store.close();}finally{await lab.close();key.fill(0);}}});
  await store.migrate();await store.initializePricing();const directory=join(lab.directory,'point-backups');
  context=await createTaskPostgresBackupContext({laboratory:lab,directory,archiveKey:key,binDirectory,libraryDirectory});
  const proof=await createPostgresBaseBackup(context);await checkpointPostgresBackup(proof);
  return {key,lab,store,context,directory,proof};
}

test('a copied recovery point description cannot grant a checkpoint selection capability',()=>{
  assert.throws(()=>postgresBackupRecoveryPointSource({backupId:randomUUID(),pointId:randomUUID(),scope:'harbor-backup-recovery-point-v1'}),{code:'backup_recovery_point_proof_required'});
});

test('private PostgreSQL points keep independent immutable WAL catalogs while reusing one real base',real,async t=>{
  const f=await fixture(t),base=join(f.directory,f.proof.backupId),baseDigest=await digestFile(join(base,'backup.enc'));
  const first=await capturePostgresBackupRecoveryPoint(f.context,f.proof),source=postgresBackupRecoveryPointSource(first,{context:f.context,proof:f.proof}),before=await readFile(join(source.directory,'checkpoint.enc'));
  assert.equal(source.manifest.systemId,source.checkpoint.systemId);
  assert.deepEqual(source.manifest.baseManifestDigest,baseDigest);
  assert.notDeepEqual(source.manifest.checkpointDigest,source.manifest.sourceCheckpointDigest);
  assert.notEqual(source.checkpoint.walCatalog.generation,(await readManifest(join(base,'checkpoint.enc'),f.key)).walCatalog.generation);
  source.manifest.checkpointDigest.sha256='mutated clone';
  assert.notEqual(postgresBackupRecoveryPointSource(first).manifest.checkpointDigest.sha256,'mutated clone');
  const member=randomUUID();await f.store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'恢复点之间的合成变更',Date.now());
  await checkpointPostgresBackup(f.proof);
  const second=await capturePostgresBackupRecoveryPoint(f.context,f.proof),later=postgresBackupRecoveryPointSource(second);
  assert.equal(second.backupId,first.backupId);assert.notEqual(second.pointId,first.pointId);
  assert.notEqual(later.checkpoint.restorePoint,postgresBackupRecoveryPointSource(first).checkpoint.restorePoint);
  assert.deepEqual(await readFile(join(postgresBackupRecoveryPointSource(first).directory,'checkpoint.enc')),before);
  assert.equal((await verifyPostgresBackupRecoveryPoint(f.context,f.proof,first.pointId)).pointId,first.pointId);
  assert.equal((await capturePostgresBackupRecoveryPoint(f.context,f.proof,{pointId:first.pointId})).pointId,first.pointId);
  assert.deepEqual(await digestFile(join(base,'backup.enc')),baseDigest);
  assert.equal((await readdir(f.directory)).filter(name=>/^[a-f0-9-]{36}$/.test(name)).length,1);
  await f.store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',randomUUID(),'晚于两个恢复点的合成变更',Date.now());
  await checkpointPostgresBackup(f.proof);
  const memberTable=(await f.store.get("SELECT 'harbor.members'::regclass::oid::text oid")).oid;
  const firstRestore=await restorePostgresBackup(f.proof,{recoveryPoint:first}),firstSummary=inspectPostgresBackupRestore(firstRestore);
  assert.equal(firstSummary.recoveryPointId,first.pointId);assert.equal(firstSummary.restoreVerified,true);
  assert.equal(firstSummary.tableCounts.find(row=>row.tableOid===memberTable).rows,'0');
  const secondRestore=await restorePostgresBackup(f.proof,{recoveryPoint:second}),secondSummary=inspectPostgresBackupRestore(secondRestore);
  assert.equal(secondSummary.recoveryPointId,second.pointId);assert.equal(secondSummary.restoreVerified,true);
  assert.equal(secondSummary.tableCounts.find(row=>row.tableOid===memberTable).rows,'1');
  assert.equal((await f.store.get('SELECT COUNT(*)::text n FROM harbor.members')).n,'2');
  await checkpointPostgresBackup(f.proof);
  await verifyPostgresBackupRestore(firstRestore,f.proof);await verifyPostgresBackupRestore(secondRestore,f.proof);
  assert.equal((await readManifest(join(postgresBackupRecoveryPointSource(first).directory,'restore.enc'),f.key)).recoveryPointId,first.pointId);
  assert.deepEqual(await readdir(postgresBackupContext(f.context).restoreDirectory),[]);
  const stoppedId=randomUUID(),capturing=capturePostgresBackupRecoveryPoint(f.context,f.proof,{pointId:stoppedId}),record=postgresBackupContext(f.context);
  assert.equal(record.busy,true);assert.ok(record.pending&&record.cancellation);
  const rejected=assert.rejects(capturing,{name:'AbortError'});
  await closePostgresBackupContext(f.context);await rejected;
  assert.equal(record.busy,false);assert.equal(record.pending,null);assert.ok(record.key.every(byte=>byte===0));
  assert.deepEqual(await readFile(join(source.directory,'checkpoint.enc')),before);
  assert.equal((await readdir(join(base,'points'))).includes(stoppedId),false);
});

test('point verification rejects changed authenticated catalogs and preserves incomplete foreign contents',real,async t=>{
  const f=await fixture(t),point=await capturePostgresBackupRecoveryPoint(f.context,f.proof),source=postgresBackupRecoveryPointSource(point),catalog=join(source.directory,source.checkpoint.walCatalog.parts[0].name),original=await readFile(catalog),corrupt=Buffer.from(original);corrupt[0]^=1;
  await writeFile(catalog,corrupt);
  await assert.rejects(verifyPostgresBackupRecoveryPoint(f.context,f.proof,point.pointId),{code:'backup_wal_catalog_changed'});
  await writeFile(catalog,original);
  await assert.rejects(restorePostgresBackup(f.proof,{recoveryPoint:{...point}}),{code:'backup_recovery_point_proof_required'});
  const id=randomUUID(),unfinished=join(f.directory,f.proof.backupId,'points',id);await mkdir(unfinished,{mode:0o700});await writeFile(join(unfinished,'unknown'),'preserve',{mode:0o600});
  await assert.rejects(capturePostgresBackupRecoveryPoint(f.context,f.proof,{pointId:id}),{code:'backup_recovery_point_incomplete'});
  assert.equal(await readFile(join(unfinished,'unknown'),'utf8'),'preserve');
  const controller=new AbortController(),reason=Error('owned point cancelled');controller.abort(reason);
  await assert.rejects(capturePostgresBackupRecoveryPoint(f.context,f.proof,{signal:controller.signal}),error=>error===reason);
  const saved=await readFile(join(source.directory,'point.enc')),verifying=verifyPostgresBackupRecoveryPoint(f.context,f.proof,point.pointId),record=postgresBackupContext(f.context);
  assert.equal(record.busy,true);assert.ok(record.pending&&record.cancellation);
  const rejected=assert.rejects(verifying,{name:'AbortError'});
  await closePostgresBackupContext(f.context);await rejected;
  assert.equal(record.busy,false);assert.equal(record.pending,null);assert.ok(record.key.every(byte=>byte===0));
  assert.deepEqual(await readFile(join(source.directory,'point.enc')),saved);
  assert.equal(await readFile(join(unfinished,'unknown'),'utf8'),'preserve');
});
