import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {usageWindow} from '../store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,verifyDataset} from '../tools/capacity/dataset.mjs';
import {capacitySeedWindowCounts,capacitySeedActiveBodyRanges} from '../tools/capacity/dataset-time.mjs';
import {createCapacityProfileBinding,assertCapacityProfileBinding} from '../tools/capacity/profile-binding.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('preparation across midnight verifies full seed separately from current windows and real expiry',{skip:!binDirectory?'需本任务私有PG18，不访问外部数据库':false,timeout:120000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32);
 const at=Date.now(),anchor=usageWindow(at).end-86400000-60000,manifest=datasetManifest({profile:'selfcheck',anchor,metadata30d:130,metadata7d:28,bodyRequests:130});let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});
  const store=fixture.store,seed=await seedDataset(store,manifest,{recordingKey}),expected=capacitySeedWindowCounts(manifest,seed.observedAt);
  assert.equal(seed.verified,true);assert.equal(seed.totalMetadata,130);assert.equal(seed.metadata7d,expected.metadata7d);assert.equal(seed.metadata30d,expected.metadata30d);assert.ok(seed.metadata30d<130);
  const active=capacitySeedActiveBodyRanges(manifest,seed.observedAt).reduce((n,[first,end])=>n+end-first,0);assert.equal(seed.activeBodyRequests,active);assert.ok(active<130);
  const expired=await store.all('SELECT request_id FROM harbor.recordings WHERE expires_at<=$1',seed.observedAt);assert.ok(expired.length>0);
  const jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey,storagePaths:[lab.directory]});await backend.schedule();
  const purge=await jobs.claim(['recording.purge']);assert.ok(purge);await jobs.complete(purge,await backend.handlers['recording.purge'](purge));
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.recordings WHERE request_id=ANY($1::text[])',expired.map(row=>row.request_id))).n,0);
  const checked=await verifyDataset(store,manifest,{recordingKey});assert.equal(checked.verified,true,'the real purge may delete only canonically expired seed recordings');assert.equal(checked.seedBodyRequests,130);assert.equal(checked.bodyRequests+checked.expiredSeedBodyRequests,130);
  const options={laboratory:lab,manifest,store,recordingKey,isOpen:()=>true};assert.equal(assertCapacityProfileBinding(await createCapacityProfileBinding(options),'selfcheck').datasetVerified,true);
  await store.transaction(async()=>{await store.run('UPDATE harbor_capacity.body_receipts SET request_id=$1 WHERE request_id=$2','missing-seed-request',expired[0].request_id);assert.equal((await verifyDataset(store,manifest,{recordingKey})).verified,false);throw Error('rollback-orphan-receipt');}).catch(error=>{assert.equal(error.message,'rollback-orphan-receipt');});
  const live=await store.get('SELECT request_id FROM harbor.recordings WHERE expires_at>$1 LIMIT 1',Date.now());
  await store.transaction(async()=>{await store.run('UPDATE harbor.recordings SET expires_at=0 WHERE request_id=$1',live.request_id);assert.equal((await verifyDataset(store,manifest,{recordingKey})).verified,false);throw Error('rollback-early-expiry');}).catch(error=>{assert.equal(error.message,'rollback-early-expiry');});
  await store.transaction(async()=>{await store.run('DELETE FROM harbor.conversation_events WHERE request_id=$1',live.request_id);assert.equal((await verifyDataset(store,manifest,{recordingKey})).verified,false);throw Error('rollback-live-loss');}).catch(error=>{assert.equal(error.message,'rollback-live-loss');});
  await store.transaction(async()=>{await store.run('DELETE FROM harbor.recordings WHERE request_id=$1',live.request_id);assert.equal((await verifyDataset(store,manifest,{recordingKey})).verified,false);throw Error('rollback-live-recording-loss');}).catch(error=>{assert.equal(error.message,'rollback-live-recording-loss');});
  await store.transaction(async()=>{await store.run('UPDATE harbor.requests SET ended=ended+86400000 WHERE id=$1',live.request_id);assert.equal((await verifyDataset(store,manifest,{recordingKey})).verified,false);throw Error('rollback-seed-time');}).catch(error=>{assert.equal(error.message,'rollback-seed-time');});
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});
