import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,randomUUID,createHash} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {ConversationCapture} from '../conversation-capture.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,fixtureId} from '../tools/capacity/dataset.mjs';
import {createStreamingStageReader} from '../tools/capacity/streaming-stage-reader.mjs';
const digest=text=>({hash:createHash('sha256').update(text).digest('hex'),bytes:Buffer.byteLength(text)});
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,integration={skip:!binDirectory?'需要本任务独占私有PG，绝不使用外部数据库':false};
test('single-pass reader validates real owned PostgreSQL usage and bounded decrypted pages',integration,async t=>{
 const laboratory=await createPrivatePostgres({binDirectory,libraryDirectory}),admin=new PostgresStore({connection:laboratory.connection}),manifest=datasetManifest({profile:'selfcheck'}),recordingKey=randomBytes(32);let fixture;
 try{
  fixture=await createDatasetDatabase(admin,manifest,{laboratory,encryptionKey:randomBytes(32),recordingKey,connectionFor:database=>({connection:{...laboratory.connection,database}})});const store=fixture.store,memberId=fixtureId(manifest.runId,'member',0),keyId=fixtureId(manifest.runId,'key',0),deviceId=randomUUID(),at=Date.now();
  await store.run("INSERT INTO harbor.employee_devices(id,member_id,name,system,hardware_state,fingerprint_source,public_key_sha256,csr,created,enrollment_digest,enrollment_expires) VALUES($1,$2,'合成设备','linux','credentials_only','synthetic','synthetic','synthetic',$3,$1,$3)",deviceId,memberId,at);
  const recordings=new RecordingStore(store,{encryptionKey:recordingKey}),jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{recordingKey,jobs,storagePolicy:async()=>({captureAllowed:true,exportAllowed:true,status:'healthy'})}),options={laboratory,manifest,store,recordingKey,pageSize:1},reader=await createStreamingStageReader(options);
  const organize=async()=>{await backend.schedule();let job;while(job=await jobs.claim(['recording.index']))await jobs.complete(job,await backend.handlers[job.kind](job));};
  const request=async({text='合成用户正文',input,reply='合成回复',model='gpt-6.1-sol',usageKnown=true,status='ok',state='complete',index=true}={})=>{
   const requestId=await store.observe({id:memberId,key_id:keyId},'127.0.0.1','synthetic',model,{device:{deviceId}}),started=(await store.get('SELECT started FROM harbor.requests WHERE id=$1',requestId)).started;
   await store.finish(requestId,{status,input:31,output:7,cached:3,actualModel:model,usageKnown});await recordings.start(requestId,{turnState:'known'});const events=[],capture=new ConversationCapture({emit:event=>events.push(event)});capture.input({input:input??text});if(reply)capture.event({type:'response.output_text.delta',item_id:'message',output_index:0,delta:reply});capture.finish({output:reply?[{id:'message',type:'message',content:[{type:'output_text',text:reply}]}]:[]});if(events.length)await recordings.append(requestId,events);await recordings.finish(requestId,{state});if(index)await organize();
   const expected={source:'model',admissions:[{request_id:requestId,member_id:memberId,key_id:keyId,device_id:deviceId,started_at:started,confirmed:1}],candidateCount:1,expectedMemberId:memberId,expectedKeyId:keyId,expectedDeviceId:deviceId,plannedInput:digest(text),terminalCount:1,terminal:{outcome:status,usageKnown,model,inputTokens:31,outputTokens:7,cachedTokens:3,outputHash:digest(reply).hash,outputBytes:digest(reply).bytes,outputComplete:state==='complete',outputEvidenceComplete:true,userInputExpected:true}};
   return {requestId,expected,read:(projection,extra={})=>reader.read({projection,requestId,expected,...extra})};
  };
  await t.test('requires live owned fixture and canonical manifest',async()=>{
   await assert.rejects(createStreamingStageReader({...options,laboratory:{}}),/configuration/);await assert.rejects(createStreamingStageReader({...options,store:{}}),/configuration/);await assert.rejects(createStreamingStageReader({...options,recordingKey:randomBytes(32)}),/fixture/);await assert.rejects(createStreamingStageReader({...options,manifest:{...manifest,manifestHash:'bad'}}),/manifest/);await assert.rejects(createStreamingStageReader({...options,store:admin}),/fixture_identity/);
  });
  await t.test('pending actual indexing then unicode multipage digest and real usage',async()=>{
   const f=await request({text:'用户😀𐐀文'.repeat(1100),reply:'回复😀𐐀'.repeat(1200),index:false});assert.equal((await f.read('body')).state,'pending');await organize();assert.deepEqual(await f.read('body'),{state:'ready',readVerified:true});assert.deepEqual(await f.read('usage'),{state:'ready',readVerified:true});
  });
  await t.test('NFKC expansion produces legitimate empty search-padding parts',async()=>{
   const f=await request({text:'ﬃ'.repeat(2000),reply:'ﬄ'.repeat(2500)});assert.deepEqual(await f.read('body'),{state:'ready',readVerified:true});
  });
  await t.test('multiple same-kind fields need independent field evidence instead of concatenation',async()=>{
   const first='第一段'.repeat(900),second='第二段'.repeat(900),f=await request({text:first+second,input:[{role:'user',content:[{type:'input_text',text:first},{type:'input_text',text:second}]}]});assert.deepEqual(await f.read('body'),{state:'unknown',readVerified:false,reason:'body_field_evidence_unavailable'});
  });
  await t.test('missing actor/terminal remains pending; mismatches never verify',async()=>{
   const f=await request();assert.deepEqual(await f.read('usage',{expected:{...f.expected,passed:false}}),{state:'mismatch',readVerified:false,reason:'expected_evidence_conflict'});assert.deepEqual(await f.read('body',{expected:{...f.expected,available:false}}),{state:'unknown',readVerified:false,reason:'expected_evidence_unavailable'});assert.equal((await f.read('usage',{expected:{...f.expected,candidateCount:0}})).reason,'admission_ambiguous');assert.equal((await f.read('usage',{expected:{...f.expected,expectedDeviceId:null}})).reason,'actor_pending');assert.equal((await f.read('body',{expected:{...f.expected,terminalCount:0}})).reason,'terminal_pending');assert.equal((await f.read('usage',{expected:{...f.expected,expectedMemberId:randomUUID()}})).state,'mismatch');assert.equal((await f.read('usage',{expected:{...f.expected,terminal:{...f.expected.terminal,inputTokens:32}}})).reason,'usage_tokens_mismatch');assert.equal((await f.read('body',{expected:{...f.expected,plannedInput:digest('different')}})).reason,'body_digest_mismatch');
   const inputMismatch=await f.read('body',{expected:{...f.expected,plannedInput:digest('different')}});assert.equal(inputMismatch.mismatchPart,'input');assert.equal(inputMismatch.mismatchState,'complete');
   const wrongOutput=digest('different'),outputMismatch=await f.read('body',{expected:{...f.expected,terminal:{...f.expected.terminal,outputHash:wrongOutput.hash,outputBytes:wrongOutput.bytes}}});assert.equal(outputMismatch.reason,'body_digest_mismatch');assert.equal(outputMismatch.mismatchPart,'output');assert.equal(outputMismatch.mismatchState,'complete');
   await store.run('UPDATE harbor.request_prices SET total_atoms=total_atoms+1 WHERE request_id=$1',f.requestId);assert.equal((await f.read('usage')).reason,'projection_mismatch');
  });
  await t.test('unknown future pricing and unknown usage verify without fabricated zeroes',async()=>{
   const future=await request({model:'synthetic-future-model'});assert.deepEqual(await future.read('usage'),{state:'ready',readVerified:true});assert.equal((await store.get('SELECT state,total_atoms FROM harbor.request_prices WHERE request_id=$1',future.requestId)).state,'unpriced');
   const unknown=await request({usageKnown:false,status:'failed',state:'partial'});assert.deepEqual(await unknown.read('usage'),{state:'ready',readVerified:true});assert.deepEqual(await store.get('SELECT state,total_atoms FROM harbor.request_prices WHERE request_id=$1',unknown.requestId),{state:'unmetered',total_atoms:null});assert.equal((await unknown.read('body')).reason,'partial_output_unproven');assert.deepEqual(await unknown.read('body',{expected:{...unknown.expected,partialOutput:digest('合成回复')}}),{state:'ready',readVerified:true});
  });
  await t.test('known empty output and input are verified through the real document path',async()=>{
   const f=await request({reply:''});assert.deepEqual(await f.read('body'),{state:'ready',readVerified:true});const empty=await request({reply:'',text:''});assert.deepEqual(await empty.read('body'),{state:'ready',readVerified:true});
  });
  await t.test('real ciphertext corruption, read budgets and cancellation fail closed',async()=>{
   const f=await request({text:'长正文'.repeat(3000)}),small=await createStreamingStageReader({...options,maxPageBytes:1024});assert.equal((await small.read({projection:'body',...f})).reason,'page_bytes_budget');const controller=new AbortController();controller.abort();assert.deepEqual(await f.read('usage',{signal:controller.signal}),{state:'unknown',readVerified:false,reason:'cancelled'});
   await store.run('UPDATE harbor.recording_document_parts SET payload_cipher=set_byte(payload_cipher,15,(get_byte(payload_cipher,15)+1)%256) WHERE request_id=$1 AND ordinal=1',f.requestId);const bad=await f.read('body');assert.deepEqual(bad,{state:'unknown',readVerified:false,reason:'document_unreadable'});assert.ok(!JSON.stringify(bad).includes('长正文'));
  });
  await t.test('fresh cluster, fixture marker drift and request actor drift cannot verify',async()=>{
   const foreign=await createPrivatePostgres({binDirectory,libraryDirectory});try{await assert.rejects(createStreamingStageReader({...options,laboratory:foreign}),/fixture_identity/);}finally{await foreign.close();}
   const f=await request();await store.run('UPDATE harbor_capacity.fixture SET owner_nonce=$1',randomUUID());try{assert.deepEqual(await f.read('usage'),{state:'unknown',readVerified:false,reason:'read_unavailable'});}finally{await store.run('UPDATE harbor_capacity.fixture SET owner_nonce=$1',manifest.ownerNonce);}
   await store.run('UPDATE harbor.requests SET member_id=$1 WHERE id=$2',fixtureId(manifest.runId,'member',1),f.requestId);assert.equal((await f.read('body')).reason,'actor_mismatch');assert.equal((await f.read('usage')).reason,'actor_mismatch');
  });
  await t.test('cancellation between pages and total read/part limits stop before another decrypt',async()=>{
   const f=await request({text:'有限分页内容'.repeat(1600)}),original=store.all.bind(store),controller=new AbortController();let fetched=0;
   try{store.all=async(sql,...args)=>{const rows=await original(sql,...args);if(sql.startsWith('SELECT ordinal,payload_cipher')){fetched++;controller.abort();}return rows;};assert.equal((await f.read('body',{signal:controller.signal})).reason,'cancelled');assert.equal(fetched,1);}finally{store.all=original;}
   const one=await createStreamingStageReader({...options,maxParts:1});fetched=0;try{store.all=async(sql,...args)=>{if(sql.startsWith('SELECT ordinal,payload_cipher'))fetched++;return original(sql,...args);};assert.equal((await one.read({projection:'body',...f})).reason,'part_budget');assert.equal(fetched,1);}finally{store.all=original;}
   const short=await createStreamingStageReader({...options,maxPageBytes:16384,maxReadBytes:16384});assert.equal((await short.read({projection:'body',...f})).reason,'read_bytes_budget');
  });
  await t.test('cancellation during a pending snapshot overrides the pending result',async()=>{
   const f=await request({index:false}),original=store.all.bind(store),controller=new AbortController();try{store.all=async(sql,...args)=>{const rows=await original(sql,...args);if(sql.includes('recording_updated'))controller.abort();return rows;};assert.deepEqual(await f.read('body',{signal:controller.signal}),{state:'unknown',readVerified:false,reason:'cancelled'});}finally{store.all=original;}
   assert.deepEqual(await f.read('usage',{expected:{...f.expected,admissions:[null]}}),{state:'unknown',readVerified:false,reason:'admission_invalid'});
  });
  await t.test('partial prefix must agree with independent full-output evidence',async()=>{
   const f=await request({status:'failed',state:'partial'});assert.equal((await f.read('body',{expected:{...f.expected,partialOutput:digest('a longer unverified reply')}})).reason,'partial_output_mismatch');assert.equal((await f.read('body',{expected:{...f.expected,partialOutput:digest('完全不同')}})).reason,'partial_output_mismatch');
  });
  await t.test('cross-page source revision change yields pending rather than mixed verification',async()=>{
   const f=await request({text:'跨页数据😀'.repeat(1400)}),original=store.all.bind(store);let pages=0;
   const other=new PostgresStore({connection:{...laboratory.connection,database:(await store.get('SELECT current_database() name')).name}});try{store.all=async(sql,...args)=>{const value=await original(sql,...args);if(sql.startsWith('SELECT ordinal,payload_cipher')&&++pages===1)await other.run('UPDATE harbor.recording_work SET revision=revision+1 WHERE request_id=$1',f.requestId);return value;};assert.equal((await f.read('body')).reason,'source_changed');}finally{store.all=original;await other.close();}
  });
 }finally{await fixture?.destroy();await admin.close();await laboratory.close();}
});
