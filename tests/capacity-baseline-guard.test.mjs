import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,fixtureId,verifyDataset} from '../tools/capacity/dataset.mjs';
import {capacitySeedWindowCounts} from '../tools/capacity/dataset-time.mjs';
import {installCapacityBaselineGuard,sealCapacityBaselineGuard,readCapacityBaselineGuard} from '../tools/capacity/baseline-guard.mjs';
import {createCapacityProfileBinding,sealCapacityProfileCoverage,collectCapacityProfileCoverage} from '../tools/capacity/profile-binding.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('sealing checks actual index parts and reparenting damages both seed owners',{skip:!binDirectory?'需本任务私有PG18，不访问外部数据库':false,timeout:120000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck',metadata30d:16,metadata7d:14,bodyRequests:16});let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store,id=index=>fixtureId(manifest.runId,'request',index);
  await seedDataset(store,manifest,{recordingKey});
  const guard=await store.transaction(async()=>{const verified=await verifyDataset(store,manifest,{recordingKey});assert.equal(verified.verified,true);return installCapacityBaselineGuard(store,manifest,verified.pricingRevision);});
  const jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey,storagePaths:[lab.directory]});let job;
  for(let batch=0;batch<=manifest.bodyRequests;batch++){await backend.schedule();while(job=await jobs.claim(['recording.index']))await jobs.complete(job,await backend.handlers['recording.index'](job));if(!(await store.get('SELECT COUNT(*) n FROM harbor.recording_work WHERE revision>indexed_revision')).n)break;}
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.recording_work WHERE revision>indexed_revision')).n,0);
  assert.ok((await store.run('DELETE FROM harbor.recording_document_parts WHERE request_id=$1 AND ordinal=1',id(0))).changes>0);
  await store.run('UPDATE harbor.recording_document_parts SET bytes=bytes+1 WHERE request_id=$1 AND ordinal=1',id(1));
  await sealCapacityBaselineGuard(guard);assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,2,'version fields alone cannot prove complete index parts');
  const extra=randomUUID();
  await store.run('INSERT INTO harbor.requests(id,member_id,key_id,session_id,account_id,started,ended,status,business_status,ip,model) SELECT $1,member_id,key_id,session_id,account_id,started,ended,status,business_status,ip,model FROM harbor.requests WHERE id=$2',extra,id(3));
  await store.run('INSERT INTO harbor.recordings SELECT $1,next_sequence,persisted_sequence,turn_state,expires_at,bytes,updated,preview_cipher FROM harbor.recordings WHERE request_id=$2',extra,id(3));
  await store.run('INSERT INTO harbor.recording_documents SELECT $1,source_revision,source_sequence,indexed_at,state,turn_state,expires_at,parts,bytes,key_version FROM harbor.recording_documents WHERE request_id=$2',extra,id(3));
  const clonePart=()=>store.run('INSERT INTO harbor.recording_document_parts SELECT $1,ordinal,kind,field_id,payload_cipher,bytes,search_terms FROM harbor.recording_document_parts WHERE request_id=$2 AND ordinal=1',extra,id(3));
  await clonePart();assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,2,'non-seed traffic does not damage the baseline');
  await store.run('UPDATE harbor.recording_document_parts SET request_id=$1,ordinal=999 WHERE request_id=$2',id(4),extra);
  assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,3,'non-seed to seed changes must mark the new owner');
  await store.run('UPDATE harbor.recording_document_parts SET request_id=$1,ordinal=999 WHERE request_id=$2 AND ordinal=1',id(5),id(3));
  assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,5,'seed to seed changes must mark both owners');
  await store.run('INSERT INTO harbor.recording_document_parts SELECT $1,1,kind,field_id,payload_cipher,bytes,search_terms FROM harbor.recording_document_parts WHERE request_id=$2 AND ordinal=999',extra,id(5));
  await assert.rejects(store.transaction(async()=>{await store.run('UPDATE harbor.recording_document_parts SET request_id=$1,ordinal=1000 WHERE request_id=$2',id(6),extra);throw Error('rollback-reparent');}),/rollback-reparent/);
  assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,5,'a rolled-back owner change leaves no damage');
  await store.run('INSERT INTO harbor.conversation_events SELECT $1,sequence,source_key,kind,provenance,created,content_cipher,search_terms FROM harbor.conversation_events WHERE request_id=$2 LIMIT 1',extra,id(7));
  await store.run("UPDATE harbor.conversation_events SET request_id=$1,sequence=999,source_key='moved-event' WHERE request_id=$2",id(6),extra);
  assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,6,'event reparenting also marks the new owner');
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});
test('private baseline losses survive restoration and ignore compensating new rows',{skip:!binDirectory?'需本任务私有PG18，不访问外部数据库':false,timeout:120000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck',metadata30d:130,metadata7d:28,bodyRequests:130});let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store;
  await seedDataset(store,manifest,{recordingKey});
  const guard=await store.transaction(async()=>{const verified=await verifyDataset(store,manifest,{recordingKey});assert.equal(verified.verified,true);return installCapacityBaselineGuard(store,manifest,verified.pricingRevision);});
  await assert.rejects(readCapacityBaselineGuard({}),/not_owned/);assert.equal((await readCapacityBaselineGuard(guard)).available,false);
  const jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey,storagePaths:[lab.directory]});let job,remaining;
  for(let batch=0;batch<=manifest.bodyRequests;batch++){
   await backend.schedule();while(job=await jobs.claim(['recording.index']))await jobs.complete(job,await backend.handlers['recording.index'](job));
   remaining=await store.get('SELECT COUNT(*) pending FROM harbor.recording_work WHERE revision>indexed_revision');if(!remaining.pending)break;
  }
  assert.equal(remaining.pending,0);
  await sealCapacityBaselineGuard(guard);const initial=await readCapacityBaselineGuard(guard);assert.equal(initial.available,true);assert.equal(initial.lostSeedRows,0);assert.equal(initial.formalQualified,false);assert.deepEqual({metadata7d:initial.metadata7d,metadata30d:initial.metadata30d},capacitySeedWindowCounts(manifest,initial.through));
  const originalRecent=manifest.metadata7d;manifest.metadata7d++;try{assert.equal((await readCapacityBaselineGuard(guard)).metadata7d,initial.metadata7d);}finally{manifest.metadata7d=originalRecent;}
  const id=index=>fixtureId(manifest.runId,'request',index);
  for(const ip of ['changed','restored','changed-again'])await store.run('UPDATE harbor.requests SET ip=$1 WHERE id=$2',ip,id(0));
  let facts=await readCapacityBaselineGuard(guard);assert.equal(facts.lostSeedRows,1);assert.equal(facts.metadata7d,initial.metadata7d-1);
  await store.run(`INSERT INTO harbor.requests(id,member_id,key_id,session_id,account_id,started,ended,status,business_status,ip,model)
   SELECT $1,member_id,key_id,session_id,account_id,started,ended,status,business_status,ip,model FROM harbor.requests WHERE id=$2`,randomUUID(),id(0));
  assert.equal((await readCapacityBaselineGuard(guard)).metadata7d,facts.metadata7d,'new rows cannot repay a baseline loss');
  await store.run('DELETE FROM harbor.conversation_events WHERE request_id=$1',id(1));
  await store.run('DELETE FROM harbor.recordings WHERE request_id=$1',id(2));
  await store.run('UPDATE harbor.recording_work SET indexed_at=indexed_at+1 WHERE request_id=$1',id(3));
  facts=await readCapacityBaselineGuard(guard);assert.equal(facts.lostSeedRows,4);assert.equal(facts.metadata7d,initial.metadata7d-4);
  await assert.rejects(store.transaction(async()=>{await store.run('UPDATE harbor.requests SET ip=$1 WHERE id=$2','rolled-back',id(4));throw Error('rollback-loss');}),/rollback-loss/);
  assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,4,'rolled-back writes must not damage committed evidence');
  await store.transaction(async()=>{await assert.rejects(readCapacityBaselineGuard(guard),/nested_transaction/);});
  await assert.rejects(store.transaction(async()=>{await store.query('TRUNCATE harbor.conversation_events');assert.equal((await store.get('SELECT invalidated FROM harbor_capacity.baseline_guard WHERE singleton=1')).invalidated,true);throw Error('rollback-truncate');}),/rollback-truncate/);
  assert.equal((await readCapacityBaselineGuard(guard)).available,true);
  await store.transaction(async()=>{await store.query("SET LOCAL session_replication_role='replica'");await store.run('UPDATE harbor.requests SET ip=$1 WHERE id=$2','replica-write',id(5));});
  assert.equal((await readCapacityBaselineGuard(guard)).lostSeedRows,5,'ALWAYS triggers retain writes in replica sessions');
  const settings=await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'");
  await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({...JSON.parse(settings.value),active:'synthetic-other-book'}));
  await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",settings.value);
  assert.equal((await readCapacityBaselineGuard(guard)).reason,'baseline_invalidated','a changed pricing generation remains visible after restoration');
  await store.query('ALTER TABLE harbor.requests DISABLE TRIGGER capacity_baseline_row');await assert.rejects(readCapacityBaselineGuard(guard),/protection_changed/);
  await store.query('ALTER TABLE harbor.requests ENABLE ALWAYS TRIGGER capacity_baseline_row');await assert.rejects(readCapacityBaselineGuard(guard),/protection_changed/);
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});

test('coverage collection requires the private verified profile and never promotes selfcheck',{skip:!binDirectory?'需本任务私有PG18，不访问外部数据库':false,timeout:120000},async t=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck',metadata30d:50,metadata7d:14,bodyRequests:50});let fixture,opened=true;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store;await seedDataset(store,manifest,{recordingKey});
  const binding=await createCapacityProfileBinding({laboratory:lab,manifest,store,recordingKey,isOpen:()=>opened,trackCoverage:true});
  await assert.rejects(collectCapacityProfileCoverage({passed:true}),/not_owned/);await assert.rejects(sealCapacityProfileCoverage({}),/not_owned/);
  assert.equal((await collectCapacityProfileCoverage(binding)).passed,null);
  const jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey,storagePaths:[lab.directory]});let job;
  for(let batch=0;batch<=manifest.bodyRequests;batch++){await backend.schedule();while(job=await jobs.claim(['recording.index']))await jobs.complete(job,await backend.handlers['recording.index'](job));if(!(await store.get('SELECT COUNT(*) n FROM harbor.recording_work WHERE revision>indexed_revision')).n)break;}
  await sealCapacityProfileCoverage(binding);const facts=await collectCapacityProfileCoverage(binding);assert.equal(facts.available,true);assert.equal(facts.passed,null);assert.equal(facts.formalQualified,false);assert.equal(facts.lostSeedRows,0);
  await store.transaction(async()=>{await assert.rejects(collectCapacityProfileCoverage(binding),/nested_transaction/);});
  const database=(await store.get('SELECT current_database() name')).name,writer=new PostgresStore({connection:{...lab.connection,database}});
  let clock=Date.now()+1000,afterSnapshot;
  const now=t.mock.method(Date,'now',()=>clock),originalGet=store.get.bind(store);
  const getter=t.mock.method(store,'get',async(...args)=>{
   const row=await originalGet(...args);
   if(afterSnapshot&&args[0].startsWith('SELECT current_database() database,')){
    const change=afterSnapshot;afterSnapshot=null;clock+=1000;await change();
   }
   return row;
  });
  try{
   let started=clock;
   afterSnapshot=()=>writer.run('UPDATE harbor.recording_work SET indexed_at=indexed_at+1 WHERE request_id=$1',fixtureId(manifest.runId,'request',0));
   const oldIndex=await collectCapacityProfileCoverage(binding);
   assert.equal(afterSnapshot,null,'the second connection committed after the reader snapshot');
   assert.equal(oldIndex.available,true);assert.equal(oldIndex.lostSeedRows,0);
   assert.equal(oldIndex.through,started);assert.ok(oldIndex.through<clock,'old evidence cannot cover the concurrent loss');
   assert.equal((await collectCapacityProfileCoverage(binding)).lostSeedRows,1,'a fresh snapshot must see the committed loss');
   started=clock;
   afterSnapshot=()=>writer.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({active:'concurrent-other-book'}));
   const oldPrice=await collectCapacityProfileCoverage(binding);
   assert.equal(afterSnapshot,null);assert.equal(oldPrice.available,true);assert.equal(oldPrice.through,started);
   assert.ok(oldPrice.through<clock,'old pricing evidence cannot cover the concurrent generation change');
   assert.equal((await collectCapacityProfileCoverage(binding)).reason,'baseline_invalidated');
  }finally{getter.mock.restore();now.mock.restore();await writer.close();}
  opened=false;await assert.rejects(collectCapacityProfileCoverage(binding),/not_owned/);
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});
