import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,seedDatasetContexts,verifyDatasetContexts,fixtureId,fixtureTimestamp} from '../tools/capacity/dataset.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('context source verification reads the original corpus without repairing missing or damaged sources',{skip:!binDirectory?'需本任务私有PG18':false,timeout:120000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck',metadata30d:120,metadata7d:60,bodyRequests:120,batchSize:17});let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store,id=index=>fixtureId(manifest.runId,'request',index),verify=(options={})=>verifyDatasetContexts(store,manifest,{recordingKey,at:manifest.anchor,...options});await seedDataset(store,manifest,{recordingKey});
  const missing=await verify();assert.equal(missing.verified,false);assert.deepEqual(missing.missing,['context_receipts_missing']);assert.equal((await store.get("SELECT to_regclass('harbor_capacity.context_receipts') present")).present,null);
  await assert.rejects(verify({recordingKey:randomBytes(32)}),/recording_key_invalid/);await assert.rejects(verifyDatasetContexts(admin,manifest,{recordingKey}),/database_not_owned/);
  await seedDatasetContexts(store,manifest,{recordingKey,until:17});const partial=await verify();assert.equal(partial.verified,false);assert.ok(partial.missing.includes('context_receipt_coverage_incomplete'));
  await seedDatasetContexts(store,manifest,{recordingKey});const complete=await verify();assert.equal(complete.verified,true);assert.equal(complete.receipts,120);assert.equal(complete.active,120);assert.deepEqual(complete.readableSizes.sort((a,b)=>a-b),[8192,32768,131072,1048576]);assert.equal(complete.formalQualified,false);assert.equal(complete.scope,'original-context-source-only');
  const rollback=async mutate=>{await assert.rejects(store.transaction(async()=>{await mutate();const result=await verify();assert.equal(result.verified,false);throw Error('rollback-context-verification');},{level:'REPEATABLE READ'}),/rollback-context-verification/);};
  await rollback(()=>store.run('DELETE FROM harbor.recording_context_snapshots WHERE request_id=$1',id(3)));
  await rollback(()=>store.run('DELETE FROM harbor_capacity.context_receipts WHERE request_id=$1',id(4)));
  await rollback(()=>store.run("UPDATE harbor_capacity.context_receipts SET generator_hash='different-generator' WHERE request_id=$1",id(5)));
  await rollback(()=>store.run('UPDATE harbor.recording_context_snapshots SET expires_at=expires_at+1 WHERE request_id=$1',id(6)));
  await rollback(()=>store.run('UPDATE harbor.sessions SET member_id=$1 WHERE id=(SELECT session_id FROM harbor.requests WHERE id=$2)',fixtureId(manifest.runId,'member',2),id(7)));
  await rollback(()=>store.run('UPDATE harbor.recording_context_parts SET content_cipher=set_byte(content_cipher,0,(get_byte(content_cipher,0)+1)%256) WHERE object_id=(SELECT object_id FROM harbor.recording_context_references WHERE request_id=$1 AND ordinal=3) AND ordinal=1',id(8)));
  await rollback(()=>store.run('DELETE FROM harbor.recording_context_parts WHERE object_id=(SELECT object_id FROM harbor.recording_context_references WHERE request_id=$1 AND ordinal=4) AND ordinal=1',id(9)));
  await rollback(()=>store.run('UPDATE harbor_capacity.context_receipts SET fixture_index=-1 WHERE request_id=$1',id(0)));
  await rollback(()=>store.run('UPDATE harbor_capacity.context_receipts SET fixture_index=$1 WHERE request_id=$2',manifest.bodyRequests,id(0)));
  const expires=fixtureTimestamp(manifest,60)+30*86400000;
  await assert.rejects(store.transaction(async()=>{
   await store.run('DELETE FROM harbor.recordings WHERE request_id=$1',id(60));assert.equal((await verify({at:expires-1})).verified,false);
   const expired=await verify({at:expires});assert.equal(expired.verified,true);assert.equal(expired.expired,1);assert.equal(expired.active,119);throw Error('rollback-expired-context');
  },{level:'REPEATABLE READ'}),/rollback-expired-context/);
  await assert.rejects(store.transaction(()=>verify()),/snapshot_required/);
  const cancel=new AbortController();cancel.abort();await assert.rejects(verify({signal:cancel.signal}),{name:'AbortError'});assert.deepEqual(await verify(),complete);
 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});
