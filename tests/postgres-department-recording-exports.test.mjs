import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import http from 'node:http';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createPostgresRecordingJobs} from '../postgres-recording-jobs.mjs';
import {createPostgresRecordingAdmin} from '../postgres-recording-admin.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingContextStore} from '../recording-context-store.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createQueryDepartments} from '../postgres-query-departments.mjs';
import {createPostgresDepartmentHttp} from '../postgres-department-http.mjs';

const databaseUrl=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!databaseUrl?'需独立真实PG':false,timeout:60000};
const at=Date.parse('2026-11-13T12:00:00+08:00'),clock=()=>at;
const boundaryContext='x'.repeat(2047-Buffer.byteLength(JSON.stringify({kind:'context_object',object_id:'a'.repeat(36),context_kind:'history',content:{text:''}})));
const config=name=>{const url=new URL(databaseUrl);if(name)url.pathname='/'+name;return {connectionString:url.href,allowPlaintextLocalNetwork:true,poolSize:4};};
async function fixture(t){
 const root=new PostgresStore(config()),name='harbor_dept_export_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(config(name));
 t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});
 await store.migrate({recordingContext:true,recordingContextIndex:true});await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0});let superEnabled=true;
 const departments=createPostgresDepartments(store,{issuer,clock,authorizeSuperAdmin:({credentialId,credentialVersion})=>superEnabled&&credentialId===superAdmin.credential_id&&credentialVersion===superAdmin.credential_version?superAdmin:null});
 const scopes={global:superAdmin},keys={},departmentRows={},requests={a:[],b:[]},key=randomBytes(32),recordings=new RecordingStore(store,{encryptionKey:key,clock}),contexts=new RecordingContextStore(store,{encryptionKey:key,clock});
 for(const suffix of ['a','b']){
  const department=await departments.createDepartment(superAdmin,{name:'合成部门'+suffix});departmentRows[suffix]=department;
  const issued=await departments.issueManagementKey(superAdmin,department.id,{label:'合成管理员'+suffix});keys[suffix]=issued;scopes[suffix]=await departments.authenticateManagementKey(issued.secret);
  await store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES($1,'合成员工',$2,$3)",'member-'+suffix,at,department.id);
  await store.run("INSERT INTO harbor.accounts(id,name,home,department_id) VALUES($1,'合成账号','synthetic',$2)",'account-'+suffix,department.id);
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,protocol,department_id) VALUES($1,$2,$3,$4,$4,'responses',$5)",'session-'+suffix,'member-'+suffix,'account-'+suffix,at,department.id);
  for(let n=0;n<2;n++){
   const id=randomUUID();requests[suffix].push(id);await store.run("INSERT INTO harbor.requests(id,member_id,account_id,session_id,started,ended,status,business_status,ip,model,department_id) VALUES($1,$2,$3,$4,$5,$5,'ok','ok','synthetic','synthetic-model',$6)",id,'member-'+suffix,'account-'+suffix,'session-'+suffix,at-n,department.id);
   await recordings.start(id,{turnState:'known'});await recordings.append(id,[{sourceKey:'synthetic-'+n,kind:'user',provenance:'client',content:{text:('仅部门'+suffix+'的正文 ').repeat(150)}}]);await recordings.finish(id,{state:'complete'});
   await contexts.writeSnapshot(id,[{kind:'history',content:{text:n===0?boundaryContext:'部门'+suffix+'的上下文 '+n}}]);
  }
 }
 const jobs=createPostgresJobs(store,{clock,departmentActors:departments,scopedKinds:['recording.export','recording.retention','recording.index','recording.context.index','recording.purge']}),backend=createPostgresRecordingJobs(store,{jobs,recordingKey:key,clock,contextIndex:true,exports:{batchRows:1,chunkBytes:2048},storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});
 const call=(path,method,payload,scope=scopes.a,params=new URLSearchParams())=>backend.handle(path,method,payload,params,{scope});
 const enqueue=async(scope=scopes.a,include_body=true,format=include_body?'jsonl':'csv')=>(await call('/api/admin/recordings/exports','POST',{include_body,format,filters:{}},scope)).value;
 const execute=async()=>{const job=await jobs.claim(['recording.export']);assert.ok(job);const result=await backend.handlers[job.kind](job);await jobs.complete(job,result);return job;};
 const download=async(id,scope)=>{const chunks=[];for await(const chunk of backend.download(id,{scope}))chunks.push(chunk);return Buffer.concat(chunks).toString();};
 return {store,issuer,departments,scopes,keys,departmentRows,requests,recordings,contexts,jobs,backend,call,enqueue,execute,download,disableSuper:()=>{superEnabled=false;}};
}

test('部门导出贯穿真实队列、正文与上下文，跨部门详情下载取消和游标均拒绝',integration,async t=>{
 const f=await fixture(t),a=await f.enqueue(),b=await f.enqueue(f.scopes.b);await f.execute();await f.execute();
 const text=await f.download(a.id,f.scopes.a),rows=text.trim().split('\n').map(JSON.parse);assert.ok(rows.some(row=>row.kind==='context_object'));
 assert.ok(rows.every(row=>row.department_id===f.departmentRows.a.id));assert.ok(rows.filter(row=>row.kind==='request').every(row=>f.requests.a.includes(row.request_id)));assert.ok(!text.includes('部门b'));
 assert.equal((await f.backend.exports.list(new URLSearchParams(),{scope:f.scopes.a})).items.length,1);
 for(const action of [()=>f.backend.exports.status(a.id,{scope:f.scopes.b}),()=>f.download(a.id,f.scopes.b),()=>f.backend.exports.cancel(a.id,{scope:f.scopes.b})])await assert.rejects(action(),{code:'recording_export_not_found'});
 await assert.rejects(f.download(b.id,{...f.scopes.b}),{code:'department_scope_required'});
 await f.enqueue();const page=await f.backend.exports.list(new URLSearchParams({limit:'1'}),{scope:f.scopes.a});assert.ok(page.next_cursor);
 await assert.rejects(f.backend.exports.list(new URLSearchParams({before:page.next_cursor,limit:'1'}),{scope:f.scopes.b}),{code:'recording_export_cursor_invalid'});
});
test('显式超管全局导出包含各部门标识，部门身份不能下载全局产物',integration,async t=>{
 const f=await fixture(t),global=await f.enqueue(f.scopes.global);await f.execute();const rows=(await f.download(global.id,f.scopes.global)).trim().split('\n').map(JSON.parse);
 assert.deepEqual(new Set(rows.filter(row=>row.kind==='request').map(row=>row.department_id)),new Set([f.departmentRows.a.id,f.departmentRows.b.id]));assert.ok(rows.every(row=>typeof row.department_id==='string'));
 await assert.rejects(f.download(global.id,f.scopes.a),{code:'recording_export_not_found'});
 const csv=await f.enqueue(f.scopes.global,false);await f.execute();const text=await f.download(csv.id,f.scopes.global);assert.match(text,/^"department_id",/);assert.ok(text.includes(f.departmentRows.a.id)&&text.includes(f.departmentRows.b.id));
 f.disableSuper();await assert.rejects(f.download(global.id,f.scopes.global),error=>error.status===403);
});
test('上下文临界大小计算包含部门字段，部门与全局导出分片均可重组且不超预算',integration,async t=>{
 const f=await fixture(t);
 for(const scope of [f.scopes.a,f.scopes.global]){
  const exported=await f.enqueue(scope);await f.execute();const lines=(await f.download(exported.id,scope)).trimEnd().split('\n'),fragments=new Map();
  for(const line of lines){assert.ok(Buffer.byteLength(line)+1<=2048);const row=JSON.parse(line);if(row.kind!=='context_object_fragment')continue;const list=fragments.get(row.object_id)??[];list.push(row);fragments.set(row.object_id,list);}
  assert.equal(fragments.size,scope.view==='global'?2:1);
  for(const parts of fragments.values()){
   parts.sort((a,b)=>a.part-b.part);assert.equal(parts.at(-1).last,true);const decoded=JSON.parse(Buffer.from(parts.map(row=>row.data).join(''),'base64').toString());
   assert.equal(decoded.content.text,boundaryContext);assert.ok(parts.every(row=>row.department_id===decoded.department_id));
  }
 }
});
test('原管理密钥撤销阻止未执行导出，分块中退出或部门停用立即拒绝后续内容',integration,async t=>{
 const f=await fixture(t),first=await f.enqueue();await f.execute();let allowed=true;
 const iterator=f.backend.download(first.id,{scope:f.scopes.a,revalidateScope:async()=>allowed?f.scopes.a:null});assert.equal((await iterator.next()).done,false);allowed=false;await assert.rejects(iterator.next(),{code:'department_forbidden'});
 const second=await f.enqueue(),job=await f.jobs.claim(['recording.export']);assert.equal(job.id,second.id);await f.departments.revokeManagementKey(f.scopes.global,f.departmentRows.a.id,f.keys.a.key.id,{expected_version:0});
 await assert.rejects(f.backend.handlers[job.kind](job),error=>error.status===403);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_export_chunks WHERE export_id=$1',second.id)).n,0);
 await assert.rejects(f.download(first.id,f.scopes.global),error=>error.status===403);
 const other=await f.enqueue(f.scopes.b);await f.execute();const stream=f.backend.download(other.id,{scope:f.scopes.b});assert.equal((await stream.next()).done,false);
 await f.departments.updateDepartment(f.scopes.global,f.departmentRows.b.id,{enabled:false,expected_version:0});await assert.rejects(stream.next(),error=>error.status===403);
});
test('真实HTTP部门Cookie只读取本部门导出，退出后原下载授权不可复用',integration,async t=>{
 const f=await fixture(t),a=await f.enqueue(),b=await f.enqueue(f.scopes.b);await f.execute();await f.execute();
 const auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-admin')},{clock,scopeIssuer:f.issuer,departmentKeys:{authenticate:f.departments.authenticateManagementKey,validate:f.departments.validateManagementSession}}),response={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};
 await auth.loginAsync(f.keys.a.secret,response);
 const adapter=createPostgresRecordingAdmin(f.backend,{adminAuthenticated:auth.authenticated,adminContext:async req=>{const scope=await auth.context(req);if(!scope)throw Object.assign(new Error('denied'),{status:401});return scope;},revalidateAdminScope:auth.revalidateScope});
 const server=http.createServer((req,res)=>{adapter.handle(req,res).then(handled=>{if(!handled){res.writeHead(404);res.end();}},error=>{if(res.headersSent){res.destroy();return;}res.writeHead(error.status??500);res.end(JSON.stringify({code:error.code}));});});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(async()=>{await adapter.close({force:true});server.closeAllConnections();await new Promise(resolve=>server.close(resolve));});const origin='http://127.0.0.1:'+server.address().port,headers={cookie:response.cookie};
 const own=await fetch(origin+'/api/admin/recordings/exports/'+a.id+'/download',{headers});assert.equal(own.status,200);const body=await own.text();assert.ok(!body.includes('部门b'));
 const foreign=await fetch(origin+'/api/admin/recordings/exports/'+b.id+'/download',{headers});assert.equal(foreign.status,404);await foreign.text();
 auth.logout({headers},{setHeader(){}});const denied=await fetch(origin+'/api/admin/recordings/exports/'+a.id+'/download',{headers});assert.equal(denied.status,401);await denied.text();
});
test('部门保留期预览、执行、无限期与新采集独立生效，其他部门及全局配置不变',integration,async t=>{
 const f=await fixture(t),setting=()=>f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='recording.retention'"),original=await setting(),expiry=async id=>(await f.store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',id)).expires_at,beforeB=await expiry(f.requests.b[0]);
 const preview=await f.backend.retentionPreview(1,{scope:f.scopes.a});assert.equal(preview.affected_requests,2);
 await assert.rejects(f.backend.retentionStatus(preview.id,{scope:f.scopes.b}),error=>error.status===404);
 await assert.rejects(f.backend.retentionApply({id:preview.id,expected_version:0,confirm:true},{scope:f.scopes.b}),error=>error.status===404);
 await assert.rejects(f.backend.retentionPreview(1,{scope:f.scopes.global}),{code:'department_forbidden'});
 await f.backend.retentionApply({id:preview.id,expected_version:0,confirm:true},{scope:f.scopes.a});let job=await f.jobs.claim(['recording.retention']);await f.jobs.complete(job,await f.backend.handlers[job.kind](job));
 assert.equal(await expiry(f.requests.a[0]),at+86400000);assert.equal(await expiry(f.requests.b[0]),beforeB);assert.deepEqual(await setting(),original);
 assert.deepEqual((await f.backend.retentionStatus(preview.id,{scope:f.scopes.a})).counts,{applied:2});
 const unlimited=await f.backend.retentionPreview(null,{scope:f.scopes.a});await f.backend.retentionApply({id:unlimited.id,expected_version:1,confirm:true},{scope:f.scopes.a});job=await f.jobs.claim(['recording.retention']);await f.jobs.complete(job,await f.backend.handlers[job.kind](job));
 const id=randomUUID();await f.store.run("INSERT INTO harbor.requests(id,member_id,started,status,ip,department_id) VALUES($1,'member-a',$2,'queued','synthetic',$3)",id,at,f.departmentRows.a.id);await f.recordings.start(id);
 assert.equal(await expiry(id),8640000000000000);assert.equal(await expiry(f.requests.b[0]),beforeB);
});
test('保留期后台按持久部门拒绝混入来源，原管理Key撤销后任务不能继续',integration,async t=>{
 const f=await fixture(t),preview=await f.backend.retentionPreview(1,{scope:f.scopes.a});await f.backend.retentionApply({id:preview.id,expected_version:0,confirm:true},{scope:f.scopes.a});
 const foreign=f.requests.b[0],original=(await f.store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',foreign)).expires_at;
 await f.store.run('INSERT INTO harbor.recording_retention_items(plan_id,request_id,expected_expires,target_expires) VALUES($1,$2,$3,$4)',preview.id,foreign,original,at+86400000);
 const job=await f.jobs.claim(['recording.retention']);await assert.rejects(f.backend.handlers[job.kind](job),{code:'department_forbidden'});assert.equal((await f.store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',foreign)).expires_at,original);
 await f.store.run('DELETE FROM harbor.recording_retention_items WHERE plan_id=$1 AND request_id=$2',preview.id,foreign);await f.departments.revokeManagementKey(f.scopes.global,f.departmentRows.a.id,f.keys.a.key.id,{expected_version:0});
 await assert.rejects(f.backend.handlers[job.kind](job),error=>error.status===403);assert.equal((await f.store.get("SELECT COUNT(*) n FROM harbor.recording_retention_items WHERE plan_id=$1 AND status<>'pending'",preview.id)).n,0);
});

test('legacy部门HTTP上下文缓存不能让分块下载绕过中途注销，迭代器与队列均释放',{timeout:10000},async t=>{
 const issuer=createDepartmentScopeIssuer(),auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-admin')},{scopeIssuer:issuer}),response={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};
 await auth.loginAsync('synthetic-admin',response);const headers={cookie:response.cookie};
 const departmentHttp=createPostgresDepartmentHttp({auth,issuer,installed:false,departments:{getDepartment:async()=>{throw Error('legacy must not query departments');}},readBody:async()=>({})});
 let returned=false,yielded=0;
 const backend={handle:async()=>null,exports:{status:async()=>({download_ready:true,status:'complete',format:'jsonl',bytes:4096,limits:{bytes:4096}})},download:async function*(){try{for(let i=0;i<4;i++){yielded++;yield Buffer.alloc(1024,97);if(i===1)auth.logout({headers},{setHeader(){}});}}finally{returned=true;}}};
 const adapter=createPostgresRecordingAdmin(backend,{adminAuthenticated:auth.authenticated,adminContext:departmentHttp.context,revalidateAdminScope:departmentHttp.revalidate});
 const server=http.createServer((req,res)=>{adapter.handle(req,res).catch(error=>{if(res.headersSent)res.destroy();else{res.writeHead(error.status??500);res.end();}});});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(async()=>{await adapter.close({force:true});server.closeAllConnections();await new Promise(resolve=>server.close(resolve));});
 await assert.rejects(async()=>{const result=await fetch('http://127.0.0.1:'+server.address().port+'/api/admin/recordings/exports/recording.export:11111111-1111-1111-1111-111111111111/download',{headers});await result.arrayBuffer();});
 const deadline=Date.now()+2000;while(adapter.status().pending){assert.ok(Date.now()<deadline,'download lease must be released');await new Promise(resolve=>setTimeout(resolve,10));}
 assert.equal(returned,true);assert.ok(yielded<4);assert.equal(adapter.status().downloads.active,0);assert.equal(adapter.status().pending,0);
});

test('内部上下文索引读取只接受固定purpose的本部门system，普通管理读取仍拒绝',integration,async t=>{
 const f=await fixture(t),scope=f.issuer.system({purpose:'recording.context.index',departmentId:f.departmentRows.a.id}),object=(await f.contexts.describeSnapshot(f.requests.a[0],{scope:f.scopes.a})).references[0].object_id;
 const decoded=await f.contexts.readObject(object,{scope});assert.ok(decoded.content);
 assert.equal(await f.contexts.withLiveObject(object,decoded,async()=>42,{scope}),42);
 const blocked=[f.issuer.system({purpose:'recording.context.index'}),f.issuer.system({purpose:'recording.index',departmentId:f.departmentRows.a.id}),f.issuer.system({purpose:'recording.context.index',departmentId:f.departmentRows.b.id}),{...scope}];
 for(const denied of blocked){await assert.rejects(f.contexts.readObject(object,{scope:denied}),error=>[403,404].includes(error.status));await assert.rejects(f.contexts.withLiveObject(object,decoded,async()=>{throw Error('cross-scope callback');},{scope:denied}),error=>[403,404].includes(error.status));}
 await assert.rejects(f.contexts.describeSnapshot(f.requests.a[0],{scope}),error=>error.status===403);
 await assert.rejects(createQueryDepartments(f.store).resolve(scope),error=>error.status===403);
});

test('系统索引按真实部门入队与执行，跨部门重建拒绝，purge只清理到期内容',integration,async t=>{
 const f=await fixture(t);await f.backend.schedule();
 const queued=await f.store.all("SELECT kind,department_id,actor_kind,actor_id FROM harbor.background_jobs WHERE kind<>'recording.purge'");assert.ok(queued.length>=8);assert.ok(queued.every(row=>row.actor_kind==='system'&&row.actor_id==='system:'+row.kind&&[f.departmentRows.a.id,f.departmentRows.b.id].includes(row.department_id)));
 for(let i=0;i<20;i++){const job=await f.jobs.claim(['recording.index','recording.context.index']);if(!job)break;const result=await f.backend.handlers[job.kind](job);await f.jobs.complete(job,result);}
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_documents')).n,4);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_context_documents')).n,4);
 await assert.rejects(f.backend.rebuild({request_id:f.requests.b[0]},{scope:f.scopes.a}),error=>error.status===404);
 await f.backend.rebuild({request_id:f.requests.a[0]},{scope:f.scopes.a});await f.backend.refill();
 const job=await f.jobs.claim(['recording.index']);assert.equal(job.department_id,f.departmentRows.a.id);
 await f.departments.updateDepartment(f.scopes.global,f.departmentRows.a.id,{enabled:false,expected_version:0});await assert.rejects(f.backend.handlers[job.kind](job),error=>error.status===403);
 await f.store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',at-1,f.requests.a[1]);
 const purge=await f.jobs.claim(['recording.purge']);assert.ok(purge);await f.backend.handlers[purge.kind](purge);
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recordings WHERE request_id=$1',f.requests.a[1])).n,0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recordings WHERE request_id=ANY($1::text[])',f.requests.b)).n,2);
});
test('停用部门积压及入队竞争不阻塞其他部门索引与全局清理，恢复后继续处理',integration,async t=>{
 for(const raceKind of [null,'recording.index','recording.context.index']){
  const f=await fixture(t),disable=()=>f.departments.updateDepartment(f.scopes.global,f.departmentRows.a.id,{enabled:false,expected_version:0});let raced=false;
  if(raceKind===null)await disable();
  else{const enqueue=f.jobs.enqueue;f.jobs.enqueue=async(kind,options)=>{if(!raced&&kind===raceKind&&options.scope?.department_id===f.departmentRows.a.id){raced=true;await disable();}return enqueue(kind,options);};}
  await f.backend.schedule();await f.backend.schedule();
  if(raceKind)assert.equal(raced,true);
  const rows=await f.store.all('SELECT kind,department_id FROM harbor.background_jobs');for(const kind of ['recording.index','recording.context.index'])assert.ok(rows.some(row=>row.kind===kind&&row.department_id===f.departmentRows.b.id));assert.ok(rows.some(row=>row.kind==='recording.purge'));
  for(let i=0;i<20;i++){const job=await f.jobs.claim(['recording.index','recording.context.index']);if(!job)break;if(job.department_id===f.departmentRows.a.id){await f.jobs.cancel(job.id);continue;}await f.jobs.complete(job,await f.backend.handlers[job.kind](job));}
  assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_documents d JOIN harbor.requests r ON r.id=d.request_id WHERE r.department_id=$1',f.departmentRows.b.id)).n,2);
  await f.departments.updateDepartment(f.scopes.global,f.departmentRows.a.id,{enabled:true,expected_version:1});await f.backend.schedule();
  assert.ok((await f.store.all("SELECT id FROM harbor.background_jobs WHERE kind='recording.context.index' AND department_id=$1",f.departmentRows.a.id)).length>0);
 }
});
