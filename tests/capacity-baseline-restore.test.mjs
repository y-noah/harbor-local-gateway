import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,fixtureId,fixtureDatabase,verifyDataset} from '../tools/capacity/dataset.mjs';
import {installCapacityBaselineGuard,sealCapacityBaselineGuard,readCapacityBaselineGuard,suspendCapacityBaselineGuard,resumeCapacityBaselineGuard} from '../tools/capacity/baseline-guard.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('physical rollback retains parent-observed losses and rechecks recovered seed and actual index before a new interval',{skip:!binDirectory?'需本任务私有PG18':false,timeout:180000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:randomBytes(32)}}),admin=new PostgresStore({connection:lab.connection}),manifest=datasetManifest({profile:'selfcheck',metadata30d:40,metadata7d:20,bodyRequests:40}),recordingKey=randomBytes(32);let fixture,recovered;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store,id=index=>fixtureId(manifest.runId,'request',index);await seedDataset(store,manifest,{recordingKey});
  const guard=await store.transaction(async()=>{const checked=await verifyDataset(store,manifest,{recordingKey});assert.equal(checked.verified,true);return installCapacityBaselineGuard(store,manifest,checked.pricingRevision);});
  const jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey,storagePaths:[lab.directory]});
  for(let attempt=0;attempt<=manifest.bodyRequests;attempt++){await backend.schedule();let job;while(job=await jobs.claim(['recording.index']))await jobs.complete(job,await backend.handlers['recording.index'](job));if(!(await store.get('SELECT COUNT(*) n FROM harbor.recording_work WHERE revision>indexed_revision')).n)break;}
  await sealCapacityBaselineGuard(guard);const initial=await readCapacityBaselineGuard(guard);assert.equal(initial.lostSeedRows,0);
  await assert.rejects(resumeCapacityBaselineGuard(guard,{recordingKey}),/not_suspended/);
  const artifactDirectory=join(lab.directory,'backup'),destination=join(lab.directory,'recovered');await mkdir(artifactDirectory,{mode:0o700});await mkdir(destination,{mode:0o700});await lab.recovery.baseBackup({artifactDirectory,additionalRoots:[]});
  const point=await lab.recovery.mark();await lab.recovery.flushWal();
  await store.run('UPDATE harbor.requests SET ip=$1 WHERE id=$2','synthetic-observed-loss',id(0));assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,1);
  const ended=await suspendCapacityBaselineGuard(guard);assert.equal(ended.lostSeedRows,1);assert.equal((await readCapacityBaselineGuard(guard)).reason,'baseline_suspended');
  await assert.rejects(suspendCapacityBaselineGuard(guard),/already_suspended/);await assert.rejects(resumeCapacityBaselineGuard(guard,{recordingKey}),/restore_source_mismatch/);
  await store.close();await lab.recovery.faultSource();await lab.recovery.recover({artifactDirectory,destination,point,databaseName:fixtureDatabase(manifest)});
  recovered=new PostgresStore({connection:{...lab.connection,database:fixtureDatabase(manifest)},statementTimeoutMs:600000});
  assert.equal((await recovered.get('SELECT COUNT(*) n FROM harbor_capacity.baseline_losses')).n,0,'physical rollback removed the database loss row');
  await assert.rejects(resumeCapacityBaselineGuard(guard,{store:recovered,recordingKey:randomBytes(32)}),/restored_seed_unverified/);assert.equal((await readCapacityBaselineGuard(guard)).available,false);
  await recovered.run('DELETE FROM harbor.recording_document_parts WHERE request_id=$1 AND ordinal=1',id(1));
  const resumed=await resumeCapacityBaselineGuard(guard,{store:recovered,recordingKey});assert.equal(resumed.resumed,true);assert.ok(resumed.from>ended.through);
  const facts=await readCapacityBaselineGuard(guard);assert.equal(facts.available,true);assert.equal(facts.lostSeedRows,2,'parent loss and recovered index loss both survive');assert.equal(facts.metadata7d,initial.metadata7d-2);assert.equal(facts.metadata30d,initial.metadata30d-2);assert.equal(facts.segments.length,1);assert.equal(facts.segments[0].through,ended.through);assert.equal(facts.from,resumed.from);assert.ok(facts.from>facts.segments[0].through,'the recovered interval does not claim the fault gap');
  facts.segments[0].through=Number.MAX_SAFE_INTEGER;assert.equal((await readCapacityBaselineGuard(guard)).segments[0].through,ended.through,'copied evidence cannot extend private coverage');
  await recovered.query('TRUNCATE harbor_capacity.baseline_losses');assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,2,'clearing restored loss rows cannot clear parent observations');
 }finally{await recovered?.close();if(fixture&&!fixture.store.pool.ended)await fixture.store.close();await admin.close();await lab.close();}
});
