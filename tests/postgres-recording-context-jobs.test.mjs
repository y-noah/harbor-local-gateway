import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createPostgresWorker} from '../postgres-worker.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createRecordingIndexCrypto} from '../recording-search-index.mjs';
import {recordingQuery} from '../postgres-recording-query.mjs';
import {createPostgresRecordingContextQueries} from '../postgres-recording-context-query.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需要独占合成PG':false};
const kind='recording.context.index';
const connection=name=>{const value=new URL(url);value.pathname='/'+(name||'postgres');return {connectionString:value.href,allowPlaintextLocalNetwork:true};};
async function fixture(t,{indexBatch=32,schemaIndex=true}={}){
 const admin=new PostgresStore(connection()),name='harbor_context_jobs_'+randomUUID().replaceAll('-','');await admin.query('CREATE DATABASE '+name);const store=new PostgresStore({...connection(name),poolSize:8});
 t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);await admin.close();});await store.migrate({recordingContext:true,recordingContextIndex:schemaIndex});await store.initializePricing();
 let now=Date.now();const clock=()=>now,key=randomBytes(32),jobs=createPostgresJobs(store,{clock,leaseMs:1000}),contexts=new RecordingContextStore(store,{encryptionKey:key,clock}),recordings=new RecordingStore(store,{encryptionKey:key,clock}),crypto=createRecordingIndexCrypto(key),backend=createPostgresRecordingJobs(store,{recordingKey:key,jobs,clock,contextIndex:schemaIndex,contextStorage:true,indexBatch,storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});
 const member=randomUUID(),session=randomUUID();await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成索引员工',now);await store.run("INSERT INTO harbor.accounts(id,name,home) VALUES('synthetic','合成上游','synthetic')");await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) VALUES($1,$2,'synthetic',$3,$3)",session,member,now);
 const request=async(content='可检索的上下文',ttl=10000)=>{const id=randomUUID();await store.run("INSERT INTO harbor.requests(id,member_id,session_id,started,ended,status,business_status,ip,model) VALUES($1,$2,$3,$4,$4,'ok','ok','synthetic','gpt-6.1-sol')",id,member,session,now);await recordings.start(id);await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',now+ttl,id);await contexts.writeSnapshot(id,[{kind:'history',content}]);return id;};
 const run=async()=>{let job;while(job=await jobs.claim([kind]))await jobs.complete(job,await backend.handlers[kind](job));};
 const document=async()=>{const d=await store.get('SELECT * FROM harbor.recording_context_documents ORDER BY object_id LIMIT 1');if(!d)return null;const parts=await store.all('SELECT * FROM harbor.recording_context_document_parts WHERE object_id=$1 ORDER BY ordinal',d.object_id);return {d,parts,text:parts.map(p=>crypto.open(p.payload_cipher,`context-document:${d.object_id}:${d.source_revision}:1:${p.ordinal}`).text).join('')};};
 return {store,jobs,contexts,recordings,backend,key,clock,advance:ms=>{now+=ms;},request,run,document,crypto,connection:connection(name)};
}

test('真实PG：共享对象只索引一次，代码引号可检索，重建不改采集与计量',integration,async t=>{
 const f=await fixture(t),content={role:'user',text:'排查中文故障 await readFile("业务.json")'},a=await f.request(content),b=await f.request(content);
 await f.backend.schedule();await f.run();const result=await f.document();assert.match(result.text,/readFile\("业务.json"\)/);assert.ok(result.parts.every(p=>p.search_terms.length>0&&!p.payload_cipher.includes(Buffer.from('业务.json'))));
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_documents')).n,1);assert.equal((await f.backend.status()).context_index_pending,0);
 const work=await f.store.get('SELECT revision,indexed_revision FROM harbor.recording_context_work');await f.request(content);await f.backend.schedule();await f.run();assert.deepEqual(await f.store.get('SELECT revision,indexed_revision FROM harbor.recording_context_work'),work);
 await f.backend.rebuild({request_id:b});await f.backend.schedule();await f.run();assert.equal((await f.document()).d.source_revision,work.revision+1);assert.equal((await f.contexts.read(a)).state,'complete');assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,3);
});

test('真实PG：提交前来源到期不索引，后续新引用可恢复未完成的对象索引',integration,async t=>{
 const f=await fixture(t),content='同一会话后续仍需要的基线';await f.request(content,10);await f.backend.schedule();const job=await f.jobs.claim([kind]),original=f.jobs.commitEffect;
 f.jobs.commitEffect=async(...args)=>{f.advance(20);return original(...args);};const result=await f.backend.handlers[kind](job);assert.equal(result.expired,true);await f.jobs.complete(job,result);f.jobs.commitEffect=original;assert.equal(await f.document(),null);
 await f.request(content);await f.backend.schedule();await f.backend.schedule();await f.run();assert.match((await f.document()).text,/后续仍需要/);assert.equal((await f.backend.status()).context_index_pending,0);
});

test('真实PG：旧租约与取消不能提交派生数据，新租约可安全继续',integration,async t=>{
 const f=await fixture(t);await f.request();await f.backend.schedule();const old=await f.jobs.claim([kind]);f.advance(1001);const fresh=await f.jobs.claim([kind]);assert.equal(fresh.id,old.id);assert.notEqual(fresh.claim_token,old.claim_token);
 await assert.rejects(f.backend.handlers[kind](old),{code:'job_lease_lost'});assert.equal(await f.document(),null);await f.jobs.complete(fresh,await f.backend.handlers[kind](fresh));assert.ok(await f.document());
 await f.backend.rebuild({request_id:(await f.store.get('SELECT id FROM harbor.requests')).id});await f.backend.schedule();const cancelled=await f.jobs.claim([kind]);await f.jobs.cancel(cancelled.id);await assert.rejects(f.backend.handlers[kind](cancelled),{code:'job_lease_lost'});assert.equal((await f.document()).d.source_revision,1);
});

test('真实PG：坏对象单独报错，不阻塞后续，人工重建恢复索引',integration,async t=>{
 const f=await fixture(t,{indexBatch:1}),first=await f.request('坏对象需要恢复'),part=await f.store.get('SELECT object_id,content_cipher FROM harbor.recording_context_parts');await f.store.run('UPDATE harbor.recording_context_parts SET content_cipher=$1 WHERE object_id=$2',randomBytes(part.content_cipher.length),part.object_id);
 await f.backend.schedule();const failed=await f.jobs.claim([kind]);await assert.rejects(f.backend.handlers[kind](failed),{code:'recording_context_unreadable'});await f.jobs.fail(failed,{retry:false});f.advance(1);await f.request('后续好对象');await f.backend.schedule();await f.run();assert.match((await f.document()).text,/后续好对象/);assert.equal((await f.backend.status()).context_index_failed,1);
 await f.store.run('UPDATE harbor.recording_context_parts SET content_cipher=$1 WHERE object_id=$2',part.content_cipher,part.object_id);await f.backend.rebuild({request_id:first});await f.backend.schedule();await f.run();assert.equal((await f.backend.status()).context_index_pending,0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_documents')).n,2);
});

test('真实PG：后台清理最终回收过期上下文及派生索引，原请求统计仍保留',integration,async t=>{
 const f=await fixture(t);await f.request('到期后不应留派生索引',10);await f.backend.schedule();await f.run();assert.ok(await f.document());f.advance(20);
 const purge=await f.jobs.claim(['recording.purge']);await f.jobs.complete(purge,await f.backend.handlers['recording.purge'](purge));assert.equal(await f.document(),null);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_objects')).n,0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
});


test('真实Worker：24结构自动接索引，低空间不消耗尝试次数，恢复后原任务继续',integration,async t=>{
 const f=await fixture(t),root=await mkdtemp(join(tmpdir(),'harbor-context-worker-'));let worker,low=true;
 const condition=async predicate=>{const end=Date.now()+8000;while(!await predicate()){assert.ok(Date.now()<end,'Worker状态等待超时');await new Promise(resolve=>setTimeout(resolve,20));}};
 try{
  await writeFile(join(root,'recording-encryption.key'),f.key,{mode:0o600});await f.request('真实Worker恢复上下文');await f.backend.schedule();
  const probe=async()=>[{total:1000000000000,free:low?0:900000000000}];probe.status=()=>({source:'controlled_test_measurement'});
  worker=await createPostgresWorker({database:f.connection,dataDir:root,env:{},clock:f.clock,poolSize:3,recording:{enabled:true,storage:{probe,minFreeBytes:1,minFreeRatio:0,cacheMs:0}},runtime:{concurrency:1,pollMs:100}});worker.start();
  await condition(()=>worker.status().completed>0);assert.ok(worker.status().recording.pausedKinds.includes(kind));assert.equal((await f.store.get('SELECT SUM(attempts)::integer attempts FROM harbor.background_jobs WHERE kind=$1',kind)).attempts,0);assert.equal(await f.document(),null);
  low=false;await condition(async()=>Boolean(await f.document()));assert.equal(worker.status().recording.storage.status,'healthy');assert.equal((await f.backend.status()).context_index_pending,0);
 }finally{await worker?.close();await rm(root,{recursive:true,force:true});}
});


test('真实PG：上下文检索按共享对象去重、按实际来源筛选，到期和归属变化立即不可见',integration,async t=>{
 const f=await fixture(t),content={text:'检索精确代码 readFile("业务.json")'},first=await f.request(content),second=await f.request(content);await f.backend.schedule();await f.run();
 const query=extra=>recordingQuery(new URLSearchParams({q:'readFile("业务.json")',...extra}),{clock:f.clock,search:true});
 let found=await f.backend.contextQueries.search(query());assert.equal(found.items.length,1);assert.equal(found.items[0].association,'shared_context');assert.equal(found.association,'shared_object_once_per_part');
 const member=(await f.store.get('SELECT member_id FROM harbor.requests WHERE id=$1',first)).member_id;found=await f.backend.contextQueries.search(query({member}));assert.equal(found.items.length,1);
 assert.equal((await f.backend.contextQueries.search(query({model:'not-a-model'}))).items.length,0);
 await f.store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',f.clock()-1,first);assert.equal((await f.backend.contextQueries.search(query())).items.length,1);
 await f.store.run('UPDATE harbor.requests SET session_id=NULL WHERE id=$1',second);assert.equal((await f.backend.contextQueries.search(query())).items.length,0);
 const legacy=createPostgresRecordingJobs(f.store,{recordingKey:f.key,storagePolicy:async()=>({})});assert.equal(legacy.contextQueries,null);assert.equal(await legacy.handle('/api/admin/recordings/context/search','GET',{},new URLSearchParams({q:'业务'})),null);
});

test('真实PG：上下文搜索有界候选逐页前进，不泄露其他员工且拒绝混用游标',integration,async t=>{
 const f=await fixture(t);for(let i=0;i<4;i++)await f.request('分页检索记录 '+i);await f.backend.schedule();await f.run();
 const queries=createPostgresRecordingContextQueries(f.store,{crypto:f.crypto,clock:f.clock,maxCandidates:1}),seen=new Set();let cursor='';
 for(let guard=0;guard<10;guard++){
  const query=recordingQuery(new URLSearchParams({q:'分页检索',...(cursor?{cursor}: {})}),{clock:f.clock,search:true}),result=await queries.search(query);
  for(const item of result.items){assert.ok(!seen.has(item.object_id));seen.add(item.object_id);}if(!result.has_more)break;assert.ok(result.next&&result.next!==cursor);cursor=result.next;
 }
 assert.equal(seen.size,4);assert.ok(cursor);
 await assert.rejects(queries.search(recordingQuery(new URLSearchParams({q:'不同查询',cursor}),{clock:f.clock,search:true})),{code:'recording_cursor_invalid'});
 const other=randomUUID();await f.store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',other,'其他合成员工',f.clock());
 assert.equal((await queries.search(recordingQuery(new URLSearchParams({q:'分页检索',member:other}),{clock:f.clock,search:true}))).items.length,0);
});


test('真实PG：23兼容模式未开索引仍清理过期上下文，不误访问24派生表',integration,async t=>{
 const f=await fixture(t,{schemaIndex:false});await f.request('旧结构过期正文',10);await f.backend.schedule();assert.equal(f.backend.handlers[kind],undefined);assert.equal((await f.store.get("SELECT to_regclass('harbor.recording_context_work') present")).present,null);
 f.advance(20);const purge=await f.jobs.claim(['recording.purge']);await f.jobs.complete(purge,await f.backend.handlers['recording.purge'](purge));assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_objects')).n,0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
});
