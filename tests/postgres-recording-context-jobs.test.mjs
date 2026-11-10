import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createPostgresWorker} from '../postgres-worker.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {unlimitedRecordingExpiry} from '../recording-retention.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createRecordingIndexCrypto} from '../recording-search-index.mjs';
import {recordingQuery} from '../postgres-recording-query.mjs';
import {createPostgresRecordingExports} from '../postgres-recording-export.mjs';
import {verifyRestoredRecordingCiphertexts,hasStoredRecordingCiphertexts} from '../storage-operator-backup-recordings.mjs';
import {createPostgresRecordingContextQueries} from '../postgres-recording-context-query.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需要独占合成PG':false};
const kind='recording.context.index';
const connection=name=>{const value=new URL(url);value.pathname='/'+(name||'postgres');return {connectionString:value.href,allowPlaintextLocalNetwork:true};};
async function fixture(t,{indexBatch=32,schemaIndex=true}={}){
 const admin=new PostgresStore(connection()),name='harbor_context_jobs_'+randomUUID().replaceAll('-','');await admin.query('CREATE DATABASE '+name);const store=new PostgresStore({...connection(name),poolSize:8});
 t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);await admin.close();});await store.migrate({recordingContext:true,recordingContextIndex:schemaIndex,recordingRetentionUnlimited:true});await store.initializePricing();
 let now=Date.now();const clock=()=>now,key=randomBytes(32),jobs=createPostgresJobs(store,{clock,leaseMs:1000}),contexts=new RecordingContextStore(store,{encryptionKey:key,clock}),recordings=new RecordingStore(store,{encryptionKey:key,clock}),crypto=createRecordingIndexCrypto(key),backend=createPostgresRecordingJobs(store,{recordingKey:key,jobs,clock,contextIndex:schemaIndex,contextStorage:true,indexBatch,storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});
 const member=randomUUID(),session=randomUUID();await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$2,$3)',member,'合成索引员工',now);await store.run("INSERT INTO harbor.accounts(id,name,home) VALUES('synthetic','合成上游','synthetic')");await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) VALUES($1,$2,'synthetic',$3,$3)",session,member,now);
 const request=async(content='可检索的上下文',ttl=10000,components,coverage=null)=>{const id=randomUUID();await store.run("INSERT INTO harbor.requests(id,member_id,session_id,started,ended,status,business_status,ip,model) VALUES($1,$2,$3,$4,$4,'ok','ok','synthetic','gpt-6.1-sol')",id,member,session,now);await recordings.start(id);await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',now+ttl,id);await contexts.writeSnapshot(id,components??[{kind:'history',content}],{coverage});return id;};
 const run=async()=>{let job;while(job=await jobs.claim([kind]))await jobs.complete(job,await backend.handlers[kind](job));};
 const document=async()=>{const d=await store.get('SELECT * FROM harbor.recording_context_documents ORDER BY object_id LIMIT 1');if(!d)return null;const parts=await store.all('SELECT * FROM harbor.recording_context_document_parts WHERE object_id=$1 ORDER BY ordinal',d.object_id);return {d,parts,text:parts.map(p=>crypto.open(p.payload_cipher,`context-document:${d.object_id}:${d.source_revision}:1:${p.ordinal}`).text).join('')};};
 return {store,jobs,contexts,recordings,backend,key,clock,advance:ms=>{now+=ms;},request,run,document,crypto,connection:connection(name)};
}

test('真实PG：无限期保留同步保护上下文来源，改回按天后可正常清理',integration,async t=>{
 const f=await fixture(t),content={text:'无限期上下文检索'},id=await f.request(content,10000);
 const preview=await f.backend.retentionPreview(null);await f.backend.retentionApply({id:preview.id,expected_version:0,confirm:true});let job=await f.jobs.claim(['recording.retention']);await f.jobs.complete(job,await f.backend.handlers['recording.retention'](job));
 assert.equal((await f.store.get('SELECT expires_at FROM harbor.recording_context_snapshots WHERE request_id=$1',id)).expires_at,unlimitedRecordingExpiry);
 f.advance(20000);await f.backend.schedule();await f.run();job=await f.jobs.claim(['recording.purge']);await f.jobs.complete(job,await f.backend.handlers['recording.purge'](job));
 assert.equal((await f.contexts.read(id)).state,'complete');const query=recordingQuery(new URLSearchParams({q:'无限期上下文检索'}),{clock:f.clock,search:true});assert.equal((await f.backend.contextQueries.search(query)).items.length,1);
 const finite=await f.backend.retentionPreview(1);await f.backend.retentionApply({id:finite.id,expected_version:1,confirm:true});job=await f.jobs.claim(['recording.retention']);await f.jobs.complete(job,await f.backend.handlers['recording.retention'](job));f.advance(2*86400000);await f.backend.schedule();job=await f.jobs.claim(['recording.purge']);await f.jobs.complete(job,await f.backend.handlers['recording.purge'](job));
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_objects')).n,0);assert.ok(await f.store.get('SELECT id FROM harbor.requests WHERE id=$1',id));
});

test('真实PG：索引补充只入队待办，上下文版本维护保持定时执行',integration,async t=>{
 const f=await fixture(t);await f.request();await f.backend.refill();assert.equal((await f.store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind='recording.purge'")).n,0);await f.run();
 const before=await f.store.get('SELECT object_id,revision,indexed_revision FROM harbor.recording_context_work');assert.ok(before.indexed_revision>0);await f.store.run("UPDATE harbor.recording_context_documents SET key_version='synthetic-old-key' WHERE object_id=$1",before.object_id);
 await f.backend.refill();assert.deepEqual(await f.store.get('SELECT object_id,revision,indexed_revision FROM harbor.recording_context_work WHERE object_id=$1',before.object_id),before);
 await f.backend.schedule();assert.equal((await f.store.get('SELECT revision FROM harbor.recording_context_work WHERE object_id=$1',before.object_id)).revision,before.revision+1);
});

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
 let found=await f.backend.contextQueries.search(query());assert.equal(found.items.length,1);assert.equal(found.items[0].association,'shared_context');assert.equal(found.association,'shared_object_once_per_part');assert.ok(Number.isSafeInteger(found.items[0].expires_at)&&found.items[0].expires_at>f.clock());assert.equal(Object.hasOwn(found.items[0],'usage_type_version'),true);
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


const downloadLines=async(exporter,id)=>{const chunks=[];for await(const chunk of exporter.download(id))chunks.push(chunk);return Buffer.concat(chunks).toString().trim().split('\n').filter(Boolean).map(line=>JSON.parse(line));};
const contextExporter=f=>createPostgresRecordingExports(f.store,{jobs:f.jobs,crypto:f.crypto,recordings:f.recordings,contexts:f.contexts,clock:f.clock,batchRows:1,chunkBytes:1024});
const exportBody={include_body:true,format:'jsonl',filters:{}};

test('真实PG：导出保留重复引用及顺序，同对象跨批次只写一次，元数据CSV不含正文',integration,async t=>{
 const f=await fixture(t),components=[{kind:'history',content:'相同问题重复出现'},{kind:'history',content:'相同问题重复出现'}],a=await f.request('',10000,components),b=await f.request('相同问题重复出现');const exporter=contextExporter(f),created=await exporter.enqueue(exportBody),job=await f.jobs.claim(['recording.export']);await f.jobs.complete(job,await exporter.handlers[job.kind](job));
 const lines=await downloadLines(exporter,created.id),objects=lines.filter(x=>x.kind==='context_object'),refs=lines.filter(x=>x.kind==='context_reference');assert.equal(objects.length,1);assert.equal(refs.length,3);assert.equal(new Set(refs.map(x=>x.object_id)).size,1);assert.deepEqual(refs.filter(x=>x.request_id===a).map(x=>x.ordinal),[1,2]);assert.equal(lines.filter(x=>x.kind==='context_snapshot').length,2);
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects WHERE export_id=$1',created.id)).n,1);const chunks=await f.store.all('SELECT payload_cipher FROM harbor.recording_export_chunks');assert.ok(chunks.every(x=>!x.payload_cipher.includes(Buffer.from('相同问题'))));
 const csv=await exporter.enqueue({include_body:false,format:'csv',filters:{}}),csvJob=await f.jobs.claim(['recording.export']);await f.jobs.complete(csvJob,await exporter.handlers[csvJob.kind](csvJob));let text='';for await(const value of exporter.download(csv.id))text+=value;assert.ok(!text.includes('相同问题'));assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects WHERE export_id=$1',csv.id)).n,0);
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_export_sources WHERE export_id=$1',created.id)).n,2);await f.store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',f.clock()-1,a);assert.equal((await exporter.status(created.id)).status,'expired');await assert.rejects(downloadLines(exporter,created.id),{code:'recording_export_unavailable'});assert.equal((await f.contexts.read(b)).state,'complete');
});

test('真实PG：导出检查点之后中断，重建exporter恢复时不重复共享正文',integration,async t=>{
 const f=await fixture(t);await f.request('跨进程继续的共享正文');await f.request('跨进程继续的共享正文');let exporter=contextExporter(f);const created=await exporter.enqueue(exportBody),first=await f.jobs.claim(['recording.export']),owned=f.jobs.owned;let injected=false;
 f.jobs.owned=async(...args)=>{const result=await owned(...args);if(!injected&&(await f.store.get('SELECT rows FROM harbor.recording_exports WHERE id=$1',created.id)).rows===1){injected=true;throw Error('synthetic committed checkpoint interruption');}return result;};
 try{await assert.rejects(exporter.handlers[first.kind](first),/synthetic committed checkpoint/);}finally{f.jobs.owned=owned;}
 assert.equal(injected,true);assert.equal((await f.store.get('SELECT rows FROM harbor.recording_exports WHERE id=$1',created.id)).rows,1);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects')).n,1);
 await f.jobs.fail(first,{retry:true,retryDelayMs:0});exporter=contextExporter(f);const resumed=await f.jobs.claim(['recording.export']);await f.jobs.complete(resumed,await exporter.handlers[resumed.kind](resumed));const lines=await downloadLines(exporter,created.id);assert.equal(lines.filter(x=>x.kind==='context_object').length,1);assert.equal(lines.filter(x=>x.kind==='context_reference').length,2);assert.equal((await exporter.status(created.id)).rows,2);
});

test('真实PG：导出依赖写入故障回滚分块和进度，重试后引用正文完整',integration,async t=>{
 const f=await fixture(t);await f.request('原子写入唯一基线');const exporter=contextExporter(f),created=await exporter.enqueue(exportBody),job=await f.jobs.claim(['recording.export']);
 await f.store.query("CREATE FUNCTION harbor.reject_context_export_dependency() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic dependency failure'; END $$; CREATE TRIGGER reject_context_export_dependency BEFORE INSERT ON harbor.recording_context_export_objects FOR EACH ROW EXECUTE FUNCTION harbor.reject_context_export_dependency()");
 await assert.rejects(exporter.handlers[job.kind](job),/synthetic dependency failure/);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_export_chunks')).n,0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects')).n,0);assert.equal((await f.store.get('SELECT rows FROM harbor.recording_exports WHERE id=$1',created.id)).rows,0);
 await f.store.query('DROP TRIGGER reject_context_export_dependency ON harbor.recording_context_export_objects; DROP FUNCTION harbor.reject_context_export_dependency()');await f.jobs.complete(job,await exporter.handlers[job.kind](job));const lines=await downloadLines(exporter,created.id);assert.equal(lines.filter(x=>x.kind==='context_object').length,1);assert.equal(lines.filter(x=>x.kind==='context_reference').length,1);
});

test('真实PG：只有上下文没有普通事件的来源也受导出到期保护，取消立即清理依赖',integration,async t=>{
 const f=await fixture(t),source=await f.request('到期前读取的基线'),exporter=contextExporter(f),created=await exporter.enqueue(exportBody),job=await f.jobs.claim(['recording.export']),readObject=f.contexts.readObject.bind(f.contexts);let changed=false;
 f.contexts.readObject=async(...args)=>{const result=await readObject(...args);if(!changed){changed=true;await f.store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',f.clock()-1,source);}return result;};
 try{await assert.rejects(exporter.handlers[job.kind](job),{code:'recording_export_expired'});}finally{f.contexts.readObject=readObject;}
 assert.equal(changed,true);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_export_chunks')).n,0);assert.equal((await exporter.status(created.id)).download_ready,false);
 await exporter.cancel(created.id);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects')).n,0);
});

test('真实PG：大上下文导出分片可重建原JSON，取消已提交批次同时清除分块和依赖',integration,async t=>{
 const f=await fixture(t),content='较长中文上下文'.repeat(300);await f.request(content);await f.request(content);const exporter=contextExporter(f),created=await exporter.enqueue(exportBody),job=await f.jobs.claim(['recording.export']);await f.jobs.complete(job,await exporter.handlers[job.kind](job));const lines=await downloadLines(exporter,created.id),fragments=lines.filter(x=>x.kind==='context_object_fragment');assert.ok(fragments.length>1);assert.equal(lines.filter(x=>x.kind==='context_object').length,0);assert.equal(JSON.parse(Buffer.from(fragments.map(x=>x.data).join(''),'base64').toString()).content,content);assert.equal(fragments.at(-1).last,true);
 const cancelled=await exporter.enqueue(exportBody),task=await f.jobs.claim(['recording.export']),owned=f.jobs.owned;let injected=false;
 f.jobs.owned=async(...args)=>{const result=await owned(...args);if(!injected&&(await f.store.get('SELECT rows FROM harbor.recording_exports WHERE id=$1',cancelled.id)).rows===1){injected=true;await exporter.cancel(cancelled.id);}return result;};
 try{await assert.rejects(exporter.handlers[task.kind](task),{code:'job_lease_lost'});}finally{f.jobs.owned=owned;}
 assert.equal(injected,true);assert.equal((await exporter.status(cancelled.id)).status,'cancelled');assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_export_objects WHERE export_id=$1',cancelled.id)).n,0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_export_chunks WHERE export_id=$1',cancelled.id)).n,0);
});


test('真实PG：离线校验覆盖只有上下文的密文、过期旧基线及仍有效的引用，不返回正文',integration,async t=>{
 const f=await fixture(t),components=[{kind:'history',content:'只能在内部校验的上下文'},{kind:'history',content:'只能在内部校验的上下文'}],old=await f.request('',10,components);await f.request('',10000,components);f.advance(20);await f.contexts.gc();await f.backend.schedule();await f.run();
 assert.equal(await hasStoredRecordingCiphertexts(f.store),true);
 await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,randomBytes(32)),{code:'recording_context_unreadable'});
 const exporter=contextExporter(f),created=await exporter.enqueue(exportBody),job=await f.jobs.claim(['recording.export']);await f.jobs.complete(job,await exporter.handlers[job.kind](job));
 const result=await verifyRestoredRecordingCiphertexts(f.store,f.key);assert.equal(result.allVerified,true);assert.equal(result.contexts.objects,1);assert.equal(result.contexts.snapshots,1);assert.equal(result.contexts.references,2);assert.equal(result.contexts.documents,1);assert.equal(result.contexts.exportDependencies,1);assert.ok(result.verified>=3);assert.ok(!JSON.stringify(result).includes('只能在内部'));
 assert.equal((await f.contexts.read(old)).state,'expired');assert.equal(await hasStoredRecordingCiphertexts(f.store),true);
 await f.store.run('DELETE FROM harbor.recordings');await assert.rejects(f.contexts.readObject((await f.store.get('SELECT id FROM harbor.recording_context_objects')).id),{code:'recording_context_expired'});const orphan=await verifyRestoredRecordingCiphertexts(f.store,f.key);assert.equal(orphan.allVerified,true);assert.equal(orphan.contexts.snapshots,0);
});

test('真实PG：离线校验拒绝引用重排、覆盖事实篡改及密文损坏',integration,async t=>{
 const f=await fixture(t),components=[{kind:'instruction',content:'不可伪造的指令'},{kind:'history',content:'不可伪造的历史'}],id=await f.request('',10000,components,{state:'incomplete',reasons:['unsupported_input'],opaqueReasoningExcluded:1});assert.equal((await verifyRestoredRecordingCiphertexts(f.store,f.key)).allVerified,true);
 await f.store.run('UPDATE harbor.recording_context_references SET ordinal=ordinal+10 WHERE request_id=$1',id);await f.store.run('UPDATE harbor.recording_context_references SET ordinal=13-ordinal WHERE request_id=$1',id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'recording_context_unreadable'});
 await f.store.run('UPDATE harbor.recording_context_references SET ordinal=ordinal+10 WHERE request_id=$1',id);await f.store.run('UPDATE harbor.recording_context_references SET ordinal=13-ordinal WHERE request_id=$1',id);
 const saved=await f.store.get('SELECT coverage FROM harbor.recording_context_snapshots WHERE request_id=$1',id);await f.store.run("UPDATE harbor.recording_context_snapshots SET coverage=$1::jsonb WHERE request_id=$2",JSON.stringify({state:'complete',reasons:[],opaqueReasoningExcluded:0}),id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'recording_context_unreadable'});await f.store.run('UPDATE harbor.recording_context_snapshots SET coverage=$1::jsonb WHERE request_id=$2',JSON.stringify(saved.coverage),id);
 const part=await f.store.get('SELECT object_id,ordinal,content_cipher FROM harbor.recording_context_parts LIMIT 1');await f.store.run('UPDATE harbor.recording_context_parts SET content_cipher=$1 WHERE object_id=$2 AND ordinal=$3',randomBytes(part.content_cipher.length),part.object_id,part.ordinal);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'recording_context_unreadable'});
});

test('真实PG：离线校验拒绝上下文索引缺块和搜索词损坏，取消可中断校验',integration,async t=>{
 const f=await fixture(t);const requestId=await f.request('恢复后仍可搜索中文代码');await f.backend.schedule();await f.run();const original=await f.store.get('SELECT object_id,ordinal,search_terms FROM harbor.recording_context_document_parts');await f.store.run('UPDATE harbor.recording_context_document_parts SET search_terms=$1::bytea[] WHERE object_id=$2',[randomBytes(32)],original.object_id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'storage_operator_application_context_search_mismatch'});await f.store.run('UPDATE harbor.recording_context_document_parts SET search_terms=$1::bytea[] WHERE object_id=$2',original.search_terms,original.object_id);
 assert.equal((await verifyRestoredRecordingCiphertexts(f.store,f.key)).allVerified,true);await f.store.run('DELETE FROM harbor.recording_context_documents WHERE object_id=$1',original.object_id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'storage_operator_application_context_index_incomplete'});await f.backend.rebuild({request_id:requestId});await f.backend.schedule();await f.run();assert.equal((await verifyRestoredRecordingCiphertexts(f.store,f.key)).allVerified,true);await f.store.run('DELETE FROM harbor.recording_context_document_parts WHERE object_id=$1',original.object_id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'storage_operator_application_context_index_incomplete'});
 const controller=new AbortController();controller.abort(Error('synthetic verification cancelled'));await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key,{signal:controller.signal}),/synthetic verification cancelled/);
});


test('真实PG：恢复校验在取数组前拒绝小密文配超大搜索词，正常待处理工作仍合法',integration,async t=>{
 const f=await fixture(t);await f.request('AA');assert.equal((await verifyRestoredRecordingCiphertexts(f.store,f.key)).allVerified,true);await f.backend.schedule();await f.run();const original=await f.store.get('SELECT object_id,search_terms FROM harbor.recording_context_document_parts');assert.equal(original.search_terms.length,1);
 const get=f.store.get.bind(f.store),all=f.store.all.bind(f.store);let receivedTerms=0;
 f.store.get=async(...args)=>{const row=await get(...args);if(Array.isArray(row?.terms))receivedTerms+=row.terms.length;return row;};
 f.store.all=async(...args)=>{const rows=await all(...args);assert.ok(rows.every(row=>!Object.hasOwn(row,'search_terms')),'未校验的搜索数组不得由批读取返回');return rows;};
 try{
  await f.store.run('UPDATE harbor.recording_context_document_parts SET search_terms=array_fill($1::bytea,ARRAY[$2::integer]) WHERE object_id=$3',randomBytes(32),Math.floor(f.recordings.maxBytes/32)+1,original.object_id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'storage_operator_application_context_index_capacity'});assert.equal(receivedTerms,0);
  // A healthy GIN rejects oversized elements before they reach storage. Drop
  // only this private fixture index to inject a damaged restored column.
  await f.store.query('DROP INDEX harbor.recording_context_document_search');
  await f.store.run('UPDATE harbor.recording_context_document_parts SET search_terms=ARRAY[$1::bytea] WHERE object_id=$2',Buffer.alloc(f.recordings.maxBytes+1),original.object_id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'storage_operator_application_context_search_mismatch'});assert.equal(receivedTerms,0);
 }finally{f.store.get=get;f.store.all=all;}
 await f.store.run('UPDATE harbor.recording_context_document_parts SET search_terms=$1::bytea[] WHERE object_id=$2',original.search_terms,original.object_id);await f.store.query('CREATE INDEX recording_context_document_search ON harbor.recording_context_document_parts USING GIN(search_terms)');assert.equal((await verifyRestoredRecordingCiphertexts(f.store,f.key)).allVerified,true);
 await f.store.run('DELETE FROM harbor.recording_context_documents WHERE object_id=$1',original.object_id);await f.store.run('DELETE FROM harbor.recording_context_work WHERE object_id=$1',original.object_id);await assert.rejects(verifyRestoredRecordingCiphertexts(f.store,f.key),{code:'storage_operator_application_context_index_incomplete'});
});
