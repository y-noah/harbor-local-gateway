import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,verifyDataset,fixtureId} from '../tools/capacity/dataset.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('original seed verification excludes runtime traffic without hiding missing or corrupted original rows',{skip:!binDirectory?'需本任务独占PG18':false,timeout:120000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),manifest=datasetManifest({profile:'selfcheck',metadata30d:50,metadata7d:20,bodyRequests:32}),recordingKey=randomBytes(32);let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store;
  const seeded=await seedDataset(store,manifest,{recordingKey});assert.equal(seeded.verified,true);
  const member={id:fixtureId(manifest.runId,'member',0),key_id:fixtureId(manifest.runId,'key',0)},runtime=await store.observe(member,'synthetic','synthetic','gpt-6.1-sol');
  await store.finish(runtime,{input:13,output:2,cached:0});const recordings=new RecordingStore(store,{encryptionKey:recordingKey});
  await recordings.start(runtime,{turnState:'known'});await recordings.append(runtime,[{sourceKey:'runtime-user',kind:'user',provenance:'client',content:{field:'runtime',mode:'append',text:'运行后新增的合成提问'}}]);await recordings.finish(runtime,{state:'complete'});
  assert.equal((await verifyDataset(store,manifest,{recordingKey})).verified,false,'the original strict whole-database mode is unchanged');
  const original=await verifyDataset(store,manifest,{recordingKey,seedOnly:true});assert.equal(original.verified,true);assert.equal(original.verificationScope,'original-seed-only');
  for(const field of ['totalMetadata','metadata7d','metadata30d','bodyRequests','seedBodyRequests','events','pricingRows','pricingRevision'])assert.equal(original[field],seeded[field],field);
  const seedBody=fixtureId(manifest.runId,'request',0),metadataOnly=fixtureId(manifest.runId,'request',manifest.bodyRequests);
  const corrupt=async operation=>{
   await assert.rejects(store.transaction(async()=>{await operation();assert.equal((await verifyDataset(store,manifest,{recordingKey,seedOnly:true})).verified,false);throw Error('rollback-seed-corruption');},{level:'REPEATABLE READ'}),/rollback-seed-corruption/);
   assert.equal((await verifyDataset(store,manifest,{recordingKey,seedOnly:true})).verified,true);
  };
  await corrupt(()=>store.run('UPDATE harbor.requests SET input=input+1 WHERE id=$1',metadataOnly));
  await corrupt(()=>store.run('DELETE FROM harbor.requests WHERE id=$1',metadataOnly));
  await corrupt(()=>store.run('DELETE FROM harbor.conversation_events WHERE request_id=$1',seedBody));
  await corrupt(()=>store.run('DELETE FROM harbor_capacity.body_receipts WHERE request_id=$1',seedBody));
  await corrupt(()=>store.run('UPDATE harbor_capacity.body_receipts SET request_id=$1 WHERE request_id=$2',runtime,seedBody));
  assert.equal((await verifyDataset(store,manifest,{recordingKey:randomBytes(32),seedOnly:true})).verified,false);
  await store.transaction(()=>assert.rejects(verifyDataset(store,manifest,{recordingKey,seedOnly:true}),/snapshot_required/));
  for(const seedOnly of [null,1,'true',{}])await assert.rejects(verifyDataset(store,manifest,{recordingKey,seedOnly}),/scope_invalid/);
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});
