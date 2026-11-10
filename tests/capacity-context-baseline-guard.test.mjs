import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,seedDatasetContexts,fixtureId} from '../tools/capacity/dataset.mjs';
import {installCapacityBaselineGuard,sealCapacityBaselineGuard,readCapacityBaselineGuard} from '../tools/capacity/baseline-guard.mjs';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {setTimeout as delay} from 'node:timers/promises';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('context baseline protects shared source and index owners, rejects incomplete source and never erases committed losses',{skip:!binDirectory?'需本任务私有PG18':false,timeout:180000},async t=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck',metadata30d:16,metadata7d:14,bodyRequests:16,batchSize:8});let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store,id=index=>fixtureId(manifest.runId,'request',index);await seedDataset(store,manifest,{recordingKey});await seedDatasetContexts(store,manifest,{recordingKey});
  const revision=(await store.get('SELECT pricing_revision FROM harbor_capacity.fixture')).pricing_revision,install=options=>store.transaction(()=>installCapacityBaselineGuard(store,manifest,revision,options),{level:'REPEATABLE READ'});
  await assert.rejects(install({recordingContext:'true',recordingKey}),/configuration/);await assert.rejects(install({recordingContext:true,recordingKey:randomBytes(32)}),/recording_key_invalid/);
  await assert.rejects(store.transaction(async()=>{await store.run('DELETE FROM harbor.recording_context_snapshots WHERE request_id=$1',id(0));await installCapacityBaselineGuard(store,manifest,revision,{recordingContext:true,recordingKey});},{level:'REPEATABLE READ'}),/context_source_unverified/);
  assert.equal((await store.get("SELECT to_regclass('harbor_capacity.baseline_guard') present")).present,null);
  const guard=await install({recordingContext:true,recordingKey}),jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey,contextIndex:true,storagePaths:[lab.directory]});
  const deadline=Date.now()+120000;let rounds=0;
  for(;;){await backend.schedule();let job;while(job=await jobs.claim(['recording.index','recording.context.index']))await jobs.complete(job,await backend.handlers[job.kind](job));const pending=await store.get('SELECT (SELECT COUNT(*) FROM harbor.recording_work WHERE revision>indexed_revision) body,(SELECT COUNT(*) FROM harbor.recording_context_work WHERE revision>indexed_revision) context,(SELECT COUNT(*) FROM harbor.recording_context_work WHERE last_error IS NOT NULL) failed');if(++rounds%10===0)t.diagnostic(JSON.stringify({indexBuild:pending,rounds}));assert.equal(pending.failed,0);if(!pending.body&&!pending.context)break;assert.ok(Date.now()<deadline,JSON.stringify({reason:'actual Worker handlers must finish both source indexes',...pending}));}
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor_capacity.baseline_losses')).n,0,'initial index construction is not damage');
  const object=async(index,ordinal=3)=>(await store.get('SELECT object_id FROM harbor.recording_context_references WHERE request_id=$1 AND ordinal=$2',id(index),ordinal)).object_id;
  await store.run('DELETE FROM harbor.recording_context_document_parts WHERE object_id=$1 AND ordinal=1',await object(9));await sealCapacityBaselineGuard(guard);
  let observed=await readCapacityBaselineGuard(guard);assert.equal(observed.recordingContextProtected,true);assert.equal(observed.lostSeedRows,1,'seal checks actual context index pieces');
  await store.run('UPDATE harbor.sessions SET updated=updated+1,state=state WHERE id=(SELECT session_id FROM harbor.requests WHERE id=$1)',id(0));assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,1,'routine session bookkeeping is not a source identity change');
  const shared=await object(0,1);
  await store.run('UPDATE harbor.recording_context_parts SET content_cipher=set_byte(content_cipher,0,(get_byte(content_cipher,0)+1)%256) WHERE object_id=$1 AND ordinal=1',shared);assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,2,'source corruption damages its original reference');
  await store.run('UPDATE harbor.recording_context_references SET request_id=$1,ordinal=6 WHERE request_id=$2 AND ordinal=3',id(2),id(1));assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,4,'moving a reference damages old and new request owners');
  await store.run('UPDATE harbor.recording_context_document_parts SET object_id=$1,ordinal=999 WHERE object_id=$2 AND ordinal=1',await object(4),await object(3));assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,6,'moving derived parts damages both object owners');
  await assert.rejects(store.transaction(async()=>{await store.run('DELETE FROM harbor.recording_context_parts WHERE object_id=$1',await object(5));throw Error('rollback-context-loss');}),/rollback-context-loss/);assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,6);
  await store.run('UPDATE harbor.sessions SET member_id=$1 WHERE id=(SELECT session_id FROM harbor.requests WHERE id=$2)',fixtureId(manifest.runId,'member',6),id(5));assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,7,'session ownership is part of source identity');
  await store.run('DELETE FROM harbor.recording_context_snapshots WHERE request_id=$1',id(7));assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,8,'cascade deletion preserves the source loss');
  await store.query('TRUNCATE harbor.recording_context_work');observed=await readCapacityBaselineGuard(guard);assert.equal(observed.available,false);assert.equal(observed.reason,'baseline_invalidated');
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});

test('factory waits for delayed context indexes before sealing and normal requests preserve context coverage',{skip:!binDirectory?'需本任务私有PG18':false,timeout:180000},async()=>{
 let target,lockClient,fallback,sawContextWait=false;
 const unlock=async()=>{clearTimeout(fallback);const client=lockClient;lockClient=null;if(client)try{await client.query('COMMIT');}finally{client.release();}};
 try{
  target=await createTarget({profile:'selfcheck',binDirectory,libraryDirectory,employeeCount:1,trackCoverage:true,
   onSetup:async({store})=>{
    const deadline=Date.now()+30000;while((await store.get('SELECT COUNT(*) n FROM harbor.recording_work WHERE revision>indexed_revision')).n){assert.ok(Date.now()<deadline,'body indexes finish before delaying context indexes');await delay(20);}
    lockClient=await store.pool.connect();await lockClient.query('BEGIN');await lockClient.query('LOCK TABLE harbor.recording_context_documents IN SHARE MODE');
    fallback=setTimeout(()=>{void unlock();},15000);
    await store.run('UPDATE harbor.recording_context_work SET revision=revision+1,last_error=NULL');
   },
   onProgress:async row=>{if(row.kind==='seed-indexing'&&row.body_pending===0&&row.context_pending>0&&lockClient){assert.equal((await lockClient.query('SELECT is_sealed FROM harbor_capacity.baseline_guard')).rows[0].is_sealed,false);sawContextWait=true;await unlock();}}
  });
  assert.equal(sawContextWait,true,'a completed body index cannot seal a delayed context index');
  assert.equal((await target.store.get('SELECT is_sealed FROM harbor_capacity.baseline_guard')).is_sealed,true);
  assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor_capacity.baseline_losses')).n,0);
  const result=await target.sendModel({actor:target.employeeActors[0],actorIndex:0,sequence:0,slot:0,scene:{id:'context-guard-normal',employees:1,streamMs:30,payloadBytes:1024}});assert.equal(result.success,true);assert.equal(result.complete,true);
  assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor_capacity.baseline_losses')).n,0,'actual request settlement changes session bookkeeping without damaging historical context');
 }finally{await unlock();await target?.close();}
});


test('one shared context source mutation covers every original request without requiring unrelated index construction',{skip:!binDirectory?'需本任务私有PG18':false,timeout:60000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck',metadata30d:602,metadata7d:300,bodyRequests:602,batchSize:50});let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store,id=index=>fixtureId(manifest.runId,'request',index);await seedDataset(store,manifest,{recordingKey});await seedDatasetContexts(store,manifest,{recordingKey});const revision=(await store.get('SELECT pricing_revision FROM harbor_capacity.fixture')).pricing_revision;
  const guard=await store.transaction(()=>installCapacityBaselineGuard(store,manifest,revision,{recordingContext:true,recordingKey}),{level:'REPEATABLE READ'}),objects=await store.all('SELECT request_id,object_id FROM harbor.recording_context_references WHERE request_id=ANY($1::text[]) AND ordinal=1',[id(0),id(600)]);assert.equal(objects.length,2);assert.equal(objects[0].object_id,objects[1].object_id);
  await store.run('UPDATE harbor.recording_context_parts SET content_cipher=set_byte(content_cipher,0,(get_byte(content_cipher,0)+1)%256) WHERE object_id=$1 AND ordinal=1',objects[0].object_id);assert.deepEqual((await store.all('SELECT fixture_index FROM harbor_capacity.baseline_losses ORDER BY fixture_index')).map(row=>row.fixture_index),[0,600]);assert.equal((await readCapacityBaselineGuard(guard)).available,false,'a source-only check does not claim that indexes are sealed');
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});
