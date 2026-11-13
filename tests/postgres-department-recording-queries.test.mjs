import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {createRecordingIndexCrypto} from '../recording-search-index.mjs';
import {createPostgresRecordingQueries,recordingQuery} from '../postgres-recording-query.mjs';
import {createPostgresRecordingContextQueries} from '../postgres-recording-context-query.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPostgresRequestQueries} from '../postgres-request-query.mjs';
import {postgresRequestObservation} from '../postgres-observability.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立合成PG测试库':false,timeout:60000};
const at=Date.parse('2026-11-13T12:00:00+08:00'),clock=()=>at;
const options=name=>{const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:2,statementTimeoutMs:10000};};
async function fixture(run){
 const admin=new PostgresStore(options()),database='harbor_dept_body_'+randomUUID().replaceAll('-','');let store,created=false,requestReader;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate({recordingContext:true,recordingContextIndex:true});await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),rootAuth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-root')},{clock,scopeIssuer:issuer}),headers={};rootAuth.login('synthetic-root',{setHeader:(name,value)=>{headers[name]=value;}});const auth={global:await rootAuth.context({headers:{cookie:headers['Set-Cookie'].split(';')[0]}})},departments=createPostgresDepartments(store,{issuer,clock,authorizeSuperAdmin:rootAuth.authorizeSuperAdmin}),key=randomBytes(32),recordings=new RecordingStore(store,{encryptionKey:key,clock}),contexts=new RecordingContextStore(store,{encryptionKey:key,clock}),crypto=createRecordingIndexCrypto(key),ids={a:[],b:[]},objects={a:[],b:[]};
  for(const suffix of ['a','b']){
   const department=(await departments.createDepartment(auth.global,{name:'合成部门'+suffix})).id,management=await departments.issueManagementKey(auth.global,department,{label:'合成管理者'});auth[suffix]=await departments.authenticateManagementKey(management.secret);
   await store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES($1,'合成员工',$2,$3)",'member-'+suffix,at,department);
   await store.run("INSERT INTO harbor.accounts(id,name,home,department_id) VALUES($1,'合成账号','synthetic',$2)",'account-'+suffix,department);
   await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,protocol,department_id) VALUES($1,$2,$3,$4,$4,'responses',$5)",'session-'+suffix,'member-'+suffix,'account-'+suffix,at,department);
   for(let n=0;n<(suffix==='a'?2:3);n++){
    const id=randomUUID(),text='共同检索 '+(suffix==='a'?'甲部门独有':'乙部门独有')+' '+n;ids[suffix].push(id);
    await store.run("INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,business_status,ip,model,department_id) VALUES($1,$2,$3,$4,$5,$5,'ok','ok','synthetic','synthetic-model',$6)",id,'member-'+suffix,'account-'+suffix,'session-'+suffix,at-n,department);
    await recordings.start(id,{turnState:'known'});await recordings.append(id,[{sourceKey:'synthetic-input',kind:'user',provenance:'client',content:{text}}]);await recordings.finish(id,{state:'complete'});await contexts.writeSnapshot(id,[{kind:'history',content:{text}}]);
    // Build query fixtures from genuinely persisted and authorized ciphertext.
    // This verifies reading/index boundaries, not unadapted worker execution.
    const body=await recordings.read(id,{scope:auth[suffix]}),work=await store.get('SELECT revision FROM harbor.recording_work WHERE request_id=$1',id),row=await store.get('SELECT * FROM harbor.recordings WHERE request_id=$1',id),part={kind:'user',field:'message',text:body.events[0].content.text,searchText:body.events[0].content.text},payload=JSON.stringify(part);
    await store.run('INSERT INTO harbor.recording_documents VALUES($1,$2,$3,$4,\'complete\',\'known\',$5,1,$6,$7)',id,work.revision,row.persisted_sequence,at,row.expires_at,Buffer.byteLength(payload),crypto.keyVersion);
    await store.run('INSERT INTO harbor.recording_document_parts VALUES($1,1,\'user\',\'message\',$2,$3,$4)',id,crypto.seal(part,`document:${id}:${work.revision}:1`),Buffer.byteLength(payload),crypto.terms(part.searchText));
    const snapshot=await contexts.describeSnapshot(id,{scope:auth[suffix]}),objectId=snapshot.references[0].object_id,source=await contexts.readObject(objectId,{scope:auth[suffix]}),contextWork=await store.get('SELECT revision FROM harbor.recording_context_work WHERE object_id=$1',objectId);objects[suffix].push(objectId);
    const contextPart={kind:'context',field:'context:'+objectId,text:source.content.text,searchText:source.content.text},contextPayload=JSON.stringify(contextPart);
    await store.run('INSERT INTO harbor.recording_context_documents VALUES($1,$2,$3,$4,$5,1,$6,1,$7)',objectId,contextWork.revision,source.keyVersion,source.fingerprint,crypto.keyVersion,at,Buffer.byteLength(contextPayload));
    await store.run('INSERT INTO harbor.recording_context_document_parts VALUES($1,1,$2,$3,$4)',objectId,Buffer.byteLength(contextPayload),crypto.seal(contextPart,`context-document:${objectId}:${contextWork.revision}:1:1`),crypto.terms(contextPart.searchText));
    if(suffix==='a'){await store.run('UPDATE harbor.recording_work SET indexed_revision=revision WHERE request_id=$1',id);await store.run('UPDATE harbor.recording_context_work SET indexed_revision=revision WHERE object_id=$1',objectId);}
   }
  }
  const queries=createPostgresRecordingQueries(store,{crypto,clock}),contextQueries=createPostgresRecordingContextQueries(store,{crypto,clock,scopeDigestForSource:source=>contexts.scopeDigestForSource(source)}),backend=createPostgresRecordingJobs(store,{recordingKey:key,clock,contextIndex:true,storagePolicy:async()=>({status:'healthy',captureAllowed:true,exportAllowed:true,min_free_bytes:99999})});
  requestReader=createPostgresRequestQueries(store,{clock,readInputPreviews:(ids,options)=>recordings.previews(ids,options)});
  await run({store,issuer,auth,ids,objects,recordings,contexts,crypto,queries,contextQueries,backend,requestReader});
 }finally{await requestReader?.close();await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();}
}

test('直接请求/原始正文/上下文/输入摘要只读本部门；猜到另一部门ID不取密文',integration,()=>fixture(async f=>{
 const {auth,ids,objects,recordings,contexts,store}=f;let opened=0;const open=recordings.open.bind(recordings);recordings.open=(...args)=>{opened++;return open(...args);};
 await assert.rejects(recordings.read(ids.b[0],{scope:auth.a}),e=>e.status===404);assert.equal(opened,0);
 const previews=await recordings.previews([ids.a[0],ids.b[0]],{scope:auth.a});assert.match(previews.get(ids.a[0]).input_preview,/甲部门独有/);assert.equal(previews.get(ids.b[0]).input_preview_state,'unavailable');assert.equal(opened,1);
 assert.match((await recordings.read(ids.a[0],{scope:auth.a})).events[0].content.text,/甲部门独有/);
 await assert.rejects(postgresRequestObservation(store,ids.b[0],at,{scope:auth.a}),e=>e.status===404);assert.equal((await postgresRequestObservation(store,ids.a[0],at,{scope:auth.a})).id,ids.a[0]);
 let cipherReads=0;const all=store.all.bind(store);store.all=(sql,...args)=>{if(sql.includes('content_cipher')&&!sql.includes('octet_length'))cipherReads++;return all(sql,...args);};
 try{await assert.rejects(contexts.read(ids.b[0],{scope:auth.a}),e=>e.status===404);await assert.rejects(contexts.describeSnapshot(ids.b[0],{scope:auth.a}),e=>e.status===404);await assert.rejects(contexts.readObject(objects.b[0],{scope:auth.a}),e=>e.status===404);assert.equal(cipherReads,0);}finally{store.all=all;}
 assert.match((await contexts.read(ids.a[0],{scope:auth.a})).parts.map(p=>p.text).join(''),/甲部门独有/);
 const list=await f.requestReader.read({view:'list',page:1,pageSize:20},{scope:auth.a});assert.equal(list.rows.length,2);assert.ok(list.rows.every(r=>r.input_preview.includes('甲部门独有')));
 for(const scope of [undefined,{...auth.a},f.issuer.system({purpose:'recording-status'})])await assert.rejects(recordings.read(ids.a[0],{scope}),e=>e.status===403);
}));

test('正文与上下文搜索、游标、详情和索引积压均绑定部门，未适配维护入口拒绝',integration,()=>fixture(async f=>{
 const query=recordingQuery(new URLSearchParams({q:'共同检索'}),{clock,search:true}),a=await f.queries.search(query,{scope:f.auth.a}),b=await f.queries.search(query,{scope:f.auth.b});
 assert.equal(a.items.length,2);assert.equal(a.index_backlog,0);assert.equal(a.complete,true);assert.equal(b.items.length,3);assert.equal(b.index_backlog,3);assert.ok(a.items.every(r=>!r.snippet.includes('乙部门独有')));
 const context=await f.contextQueries.search(query,{scope:f.auth.a});assert.equal(context.items.length,2);assert.ok(context.items.every(r=>!r.snippet.includes('乙部门独有')));assert.ok(context.items.every(r=>!Object.hasOwn(r,'source_scope')&&!Object.hasOwn(r,'object_scope')));
 for(const reader of [f.queries,f.contextQueries]){const first=await reader.search({...query,limit:1},{scope:f.auth.a});assert.ok(first.next);await assert.rejects(reader.search({...query,limit:1,cursor:first.next},{scope:f.auth.b}),e=>e.code==='recording_cursor_invalid');}
 await assert.rejects(f.queries.document(f.ids.b[0],{}, {scope:f.auth.a}),e=>e.status===404);
 const session=recordingQuery(new URLSearchParams({session:'session-b'}),{clock});await assert.rejects(f.queries.session(session,{scope:f.auth.a}),e=>e.code==='recording_query_invalid');
 const status=await f.backend.status({scope:f.auth.a});assert.equal(status.index_pending,0);assert.equal(status.context_index_pending,0);assert.ok(!Object.hasOwn(status.storage,'min_free_bytes'));assert.ok(!Object.hasOwn(status,'key_version'));
 const other=await f.backend.status({scope:f.auth.b});assert.equal(other.index_pending,3);assert.equal(other.context_index_pending,3);
 await assert.rejects(f.backend.handle('/api/admin/recordings/exports','GET',{},new URLSearchParams(),{scope:f.auth.a}),e=>e.code==='department_operation_unavailable');
 await assert.rejects(f.backend.handle('/api/admin/recordings/rebuild','POST',{request_id:f.ids.b[0]},new URLSearchParams(),{scope:f.auth.a}),e=>e.code==='department_operation_unavailable');
 for(const include_body of [true,false])await assert.rejects(f.backend.handle('/api/admin/recordings/exports','POST',{include_body,format:include_body?'jsonl':'csv',filters:{}},new URLSearchParams(),{scope:f.auth.global}),e=>e.code==='department_operation_unavailable');
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_exports')).n,0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.background_jobs')).n,0);
}));

test('跨部门上下文引用及伪造来源摘要在解密前拒绝，物理去重不能授予访问',integration,()=>fixture(async f=>{
 const request=f.ids.a[0],foreign=f.objects.b[0],foreignScope=(await f.store.get('SELECT scope_digest FROM harbor.recording_context_objects WHERE id=$1',foreign)).scope_digest;
 await f.store.run('UPDATE harbor.recording_context_references SET object_id=$1 WHERE request_id=$2 AND ordinal=1',foreign,request);
 await assert.rejects(f.contexts.read(request,{scope:f.auth.a}),e=>e.code==='recording_context_scope');await assert.rejects(f.contexts.readObject(foreign,{scope:f.auth.a}),e=>e.status===404);
 const query=recordingQuery(new URLSearchParams({q:'乙部门独有'}),{clock,search:true});assert.deepEqual((await f.contextQueries.search(query,{scope:f.auth.a})).items,[]);
 // Even a corrupt stored scope digest cannot make the B object's valid
 // ciphertext decryptable as an A request's context.
 await f.store.run('UPDATE harbor.recording_context_snapshots SET scope_digest=$1 WHERE request_id=$2',foreignScope,request);
 let opened=0;const original=f.crypto.open;f.crypto.open=(...args)=>{opened++;return original(...args);};
 await assert.rejects(f.contextQueries.search(query,{scope:f.auth.a}),e=>e.code==='recording_context_scope');assert.equal(opened,0);
 await assert.rejects(f.contexts.read(request,{scope:f.auth.a}),e=>e.code==='recording_context_scope');await assert.rejects(f.contexts.readObject(foreign,{scope:f.auth.a}),e=>e.code==='recording_context_scope');
}));
