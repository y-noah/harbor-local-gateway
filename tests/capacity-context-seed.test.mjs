import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {PostgresStore} from '../postgres-store.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingContextJobs} from '../postgres-recording-context-jobs.mjs';
import {createPostgresRecordingContextQueries} from '../postgres-recording-context-query.mjs';
import {createRecordingIndexCrypto} from '../recording-search-index.mjs';
import {recordingQuery} from '../postgres-recording-query.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,seedDatasetContexts,fixtureId} from '../tools/capacity/dataset.mjs';
import {capacityHistoricalContext} from '../tools/capacity/context-payload.mjs';

test('historical context corpus has exact serialized bytes, stable shared instructions and independent turns',()=>{
 for(const bytes of [8192,32768,131072,1048576]){
  const payload=capacityHistoricalContext(0,bytes);assert.equal(payload.bytes,bytes);assert.equal(payload.components.reduce((n,row)=>n+Buffer.byteLength(JSON.stringify(row.content)),0),bytes);assert.deepEqual(capacityHistoricalContext(0,bytes),payload);assert.deepEqual(payload.components.map(row=>row.kind),['instruction','tool_schema','history','history','context']);assert.notEqual(capacityHistoricalContext(600,bytes).contentHash,payload.contentHash);assert.deepEqual(capacityHistoricalContext(600,bytes).components[0],payload.components[0]);
 }
 for(const [index,bytes] of [[-1,8192],[0,12],[0,1048577],[null,8192]])assert.throws(()=>capacityHistoricalContext(index,bytes),/configuration/);
});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('context seeding uses actual encryption, atomic cancellation, immutable resume, scope reuse and Worker search',{skip:!binDirectory?'需本任务私有PG18':false,timeout:120000},async t=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:lab.connection}),recordingKey=randomBytes(32),manifest=datasetManifest({profile:'selfcheck',metadata30d:620,metadata7d:300,bodyRequests:620,batchSize:7});let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory:lab,recordingKey,encryptionKey:randomBytes(32),connectionFor:database=>({connection:{...lab.connection,database}})});const store=fixture.store;await seedDataset(store,manifest,{recordingKey});
  await assert.rejects(seedDatasetContexts(admin,manifest,{recordingKey}),/database_not_owned/);await assert.rejects(seedDatasetContexts(store,manifest,{recordingKey:randomBytes(32)}),/recording_key_invalid/);
  const controller=new AbortController(),get=store.get.bind(store);let inserted=false;
  store.get=async(...args)=>{const row=await get(...args);if(!inserted&&args[0].startsWith('INSERT INTO harbor.recording_context_snapshots')){inserted=true;controller.abort();}return row;};
  try{await assert.rejects(seedDatasetContexts(store,manifest,{recordingKey,until:7,signal:controller.signal}),{name:'AbortError'});}finally{store.get=get;}
  assert.equal(inserted,true);for(const table of ['recording_context_snapshots','recording_context_objects','recording_context_parts','recording_context_references'])assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.'+table)).n,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor_capacity.context_receipts')).n,0);
  const began=performance.now(),result=await seedDatasetContexts(store,manifest,{recordingKey,until:12});assert.equal(result.snapshots,12);assert.equal(result.formalQualified,false);t.diagnostic('measured initial context seed ms='+Math.round(performance.now()-began));
  await seedDatasetContexts(store,manifest,{recordingKey,first:600,until:602});const id=index=>fixtureId(manifest.runId,'request',index),contexts=new RecordingContextStore(store,{encryptionKey:recordingKey});
  const read=async requestId=>{let after={component:0,part:0};const components=new Map();for(;;){const page=await contexts.read(requestId,{after});assert.equal(page.state,'complete');for(const part of page.parts){const row=components.get(part.component)??{kind:part.kind,text:''};row.text+=part.text;components.set(part.component,row);}if(!page.hasMore)break;after=page.next;}return [...components.values()].map(row=>({kind:row.kind,content:JSON.parse(row.text)}));};
  assert.deepEqual(await read(id(0)),capacityHistoricalContext(0,8192).components);
  const refs=await store.all('SELECT request_id,ordinal,object_id FROM harbor.recording_context_references WHERE request_id=ANY($1::text[]) ORDER BY request_id,ordinal',[id(0),id(600)]);for(const ordinal of [1,2])assert.equal(refs.find(row=>row.request_id===id(0)&&row.ordinal===ordinal).object_id,refs.find(row=>row.request_id===id(600)&&row.ordinal===ordinal).object_id,'same session reuses authenticated shared objects');
  const before=await store.all('SELECT object_id,ordinal,content_cipher FROM harbor.recording_context_parts ORDER BY object_id,ordinal');assert.equal((await seedDatasetContexts(store,manifest,{recordingKey,until:12})).reused,12);assert.deepEqual(await store.all('SELECT object_id,ordinal,content_cipher FROM harbor.recording_context_parts ORDER BY object_id,ordinal'),before);
  await assert.rejects(store.transaction(async()=>{await store.run('DELETE FROM harbor.recording_context_snapshots WHERE request_id=$1',id(0));await assert.rejects(seedDatasetContexts(store,manifest,{recordingKey,until:12}),/cipher_mismatch/);assert.equal(await store.get('SELECT request_id FROM harbor.recording_context_snapshots WHERE request_id=$1',id(0)),undefined,'published snapshots are not reconstructed');throw Error('rollback-deleted-snapshot');}),/rollback-deleted-snapshot/);
  await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.recording_context_snapshots SET scope_digest=decode(repeat('00',32),'hex') WHERE request_id=$1",id(0));await assert.rejects(seedDatasetContexts(store,manifest,{recordingKey,until:12}),/cipher_mismatch/);throw Error('rollback-context-scope');}),/rollback-context-scope/);
  await assert.rejects(store.transaction(async()=>{await store.run('UPDATE harbor.sessions SET member_id=$1 WHERE id=(SELECT session_id FROM harbor.requests WHERE id=$2)',fixtureId(manifest.runId,'member',1),id(0));await assert.rejects(seedDatasetContexts(store,manifest,{recordingKey,until:12}),/cipher_mismatch/);throw Error('rollback-context-owner');}),/rollback-context-owner/);
  for(const wrongVersion of [false,true])await assert.rejects(store.transaction(async()=>{
   const objectId=randomUUID(),source=await store.get('SELECT object_id FROM harbor.recording_context_references WHERE request_id=$1 AND ordinal=1',id(0));
   await store.run('INSERT INTO harbor.recording_context_objects SELECT $1,scope_digest,$2,$3,kind,state,bytes,parts,created FROM harbor.recording_context_objects WHERE id=$4',objectId,randomBytes(32),wrongVersion?'f'.repeat(64):contexts.keyVersion,source.object_id);
   if(wrongVersion)await store.run('INSERT INTO harbor.recording_context_parts SELECT $1,ordinal,bytes,content_cipher FROM harbor.recording_context_parts WHERE object_id=$2',objectId,source.object_id);
   await store.run('INSERT INTO harbor.recording_context_references VALUES($1,6,$2,false)',id(0),objectId);
   await assert.rejects(seedDatasetContexts(store,manifest,{recordingKey,until:12}),/cipher_mismatch/);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.recording_context_references WHERE request_id=$1',id(0))).n,6,'invalid references are rejected without rewriting');throw Error('rollback-extra-context-reference');
  }),/rollback-extra-context-reference/);
  const victim=await store.get('SELECT p.object_id,p.ordinal,p.content_cipher FROM harbor.recording_context_references r JOIN harbor.recording_context_parts p ON p.object_id=r.object_id WHERE r.request_id=$1 AND r.ordinal=3 ORDER BY p.ordinal LIMIT 1',id(0));await store.run('UPDATE harbor.recording_context_parts SET content_cipher=set_byte(content_cipher,0,(get_byte(content_cipher,0)+1)%256) WHERE object_id=$1 AND ordinal=$2',victim.object_id,victim.ordinal);await assert.rejects(seedDatasetContexts(store,manifest,{recordingKey,until:12}),/cipher_mismatch/);await store.run('UPDATE harbor.recording_context_parts SET content_cipher=$1 WHERE object_id=$2 AND ordinal=$3',victim.content_cipher,victim.object_id,victim.ordinal);
  const crypto=createRecordingIndexCrypto(recordingKey),jobs=createPostgresJobs(store),worker=createPostgresRecordingContextJobs(store,{jobs,recordingKey,crypto});const objectCount=(await store.get('SELECT COUNT(*) n FROM harbor.recording_context_objects')).n;for(let batch=0;batch<=objectCount;batch++){await worker.schedule();let job;while(job=await jobs.claim(['recording.context.index']))await jobs.complete(job,await worker.handlers[job.kind](job));if(!(await worker.status()).context_index_pending)break;}assert.equal((await worker.status()).context_index_pending,0);
  const queries=createPostgresRecordingContextQueries(store,{crypto}),found=await queries.search(recordingQuery(new URLSearchParams({q:'context_seed_600',from:String(manifest.anchor-30*86400000),to:String(manifest.anchor+1),limit:'25'}),{search:true}));assert.ok(found.items.some(row=>row.request_id===id(600)||row.id===id(600)));assert.ok(found.items.every(row=>row.snippet.includes('context_seed_600')));
  const query=store.query.bind(store);let sqlCalls=0;store.query=async(...args)=>{sqlCalls++;return query(...args);};const started=performance.now();let measured,constructionMs;
  try{measured=await seedDatasetContexts(store,manifest,{recordingKey,first:12,until:100});constructionMs=performance.now()-started;}finally{store.query=query;}
  assert.equal(measured.snapshots,88);assert.deepEqual(await read(id(27)),capacityHistoricalContext(27,1048576).components);t.diagnostic(JSON.stringify({measurement:'context-construction-bounded-sample',snapshots:measured.snapshots,elapsedMs:Math.round(constructionMs),sqlCalls,includesSavepointStatements:false,formalQualified:false}));

 }finally{await fixture?.destroy();await admin.close();await lab.close();}
});
