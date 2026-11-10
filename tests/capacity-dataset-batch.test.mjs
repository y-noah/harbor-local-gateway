import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,fixtureId,fixtureTimestamp} from '../tools/capacity/dataset.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('cancelled body generation rolls back the whole batch and resumes over an existing recording without duplication',{
  skip:!binDirectory?'需要本任务私有PG二进制，不使用共享数据库':false,timeout:30000
},async()=>{
  const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection});
  const manifest=datasetManifest({profile:'selfcheck',metadata30d:60,metadata7d:20,bodyRequests:32,batchSize:7}),recordingKey=randomBytes(32);let fixture;
  try{
    fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});
    const store=fixture.store,controller=new AbortController(),run=store.run.bind(store);let inserted=false;
    store.run=async(sql,...parameters)=>{
      const result=await run(sql,...parameters);
      if(!inserted&&/INSERT INTO harbor\.conversation_events/.test(sql)){inserted=true;controller.abort();}
      return result;
    };
    try{await assert.rejects(seedDataset(store,manifest,{recordingKey,signal:controller.signal}),{name:'AbortError'});}
    finally{store.run=run;}
    assert.equal(inserted,true);
    assert.deepEqual(await store.get('SELECT metadata_cursor,body_cursor FROM harbor_capacity.fixture'),{metadata_cursor:60,body_cursor:0});
    for(const table of ['recordings','conversation_events'])assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.'+table)).n,0);
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor_capacity.body_receipts')).n,0);
    const requestId=fixtureId(manifest.runId,'request',0),recordings=new RecordingStore(store,{encryptionKey:recordingKey,clock:()=>manifest.anchor});
    await recordings.start(requestId,{turnState:'known'});
    assert.equal((await store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',requestId)).expires_at,manifest.anchor+30*86400000);
    const result=await seedDataset(store,manifest,{recordingKey});assert.equal(result.verified,true);assert.equal(result.bodyReadable,true);assert.equal(result.bodyRequests,32);
    assert.equal((await store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',requestId)).expires_at,fixtureTimestamp(manifest,0)+30*86400000);
    const before=await store.all('SELECT request_id,sequence,source_key,content_cipher FROM harbor.conversation_events ORDER BY request_id,sequence');
    await seedDataset(store,manifest,{recordingKey});
    assert.deepEqual(await store.all('SELECT request_id,sequence,source_key,content_cipher FROM harbor.conversation_events ORDER BY request_id,sequence'),before);
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.recordings')).n,32);
    await store.run('UPDATE harbor_capacity.fixture SET body_cursor=0 WHERE singleton=1');
    const modifiedExpiry=fixtureTimestamp(manifest,0)+30*86400000+1;
    await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',modifiedExpiry,requestId);
    assert.equal((await seedDataset(store,manifest,{recordingKey})).verified,false,'a completed receipt must not be silently repaired');
    assert.equal((await store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',requestId)).expires_at,modifiedExpiry);
  }finally{await fixture?.destroy();await admin.close();await lab.close();recordingKey.fill(0);}
});
