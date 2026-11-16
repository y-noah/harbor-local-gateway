import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createRequestContextCapture} from '../recording-context-capture.mjs';
import {passwordHash} from '../auth.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需隔离真实PostgreSQL及HTTP':false,timeout:60000};
function database(name='postgres'){const value=new URL(url);value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:2};}
async function fixture(t,{enabled,env={},contextEnabled=true,maxRequests=2,ttlMs=10}={}){
 const admin=new PostgresStore(database()),name='harbor_shape_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-input-shape-'));let app,store,created=false,now=Date.now(),calls=0;
 t.after(async()=>{await app?.close();await store?.close();if(created)await admin.query('DROP DATABASE '+name);await admin.close();await rm(directory,{recursive:true,force:true});});
 await admin.query('CREATE DATABASE '+name);created=true;store=new PostgresStore(database(name));await store.migrate({recordingContext:true,recordingContextIndex:true});await migrateDepartmentIsolation(store,{apply:true});
 const pgDataDir=(await store.get("SELECT current_setting('data_directory') value")).value;
 const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'synthetic',fetchImpl:async()=>{calls++;return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-response',model:'gpt-6.1-sol',output:[{id:'synthetic-message',type:'message',content:[{type:'output_text',text:'synthetic model output'}]}],usage:{input_tokens:10,output_tokens:5}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});}});
 app=await createPostgresGateway({env,dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),database:database(name),clock:()=>now,config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-admin')},responsesTransport:transport,apiResponsesTransport:transport,recording:{contextEnabled,inputDiagnostics:{...(enabled===undefined?{}:{enabled}),maxRequests,maxGroups:1,maxItemsPerRequest:1,ttlMs},storage:{pgDataDir,probe:async()=>[{total:1000,free:900}],minFreeBytes:0,minFreeRatio:0,cacheMs:0}},recordingBuffer:{flushMs:20}});
 await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
 const login=async password=>{const r=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password,includeOverview:false})});assert.equal(r.status,200);await r.arrayBuffer();return r.headers.get('set-cookie').split(';')[0];};
 const cookie=await login('synthetic-admin'),call=(path,{body,selected=false,auth=cookie,extra={}}={})=>fetch(origin+path,{method:body===undefined?'GET':'POST',headers:{cookie:auth,...(selected?{'x-harbor-department':'department-default'}:{}),...(body===undefined?{}:{'content-type':'application/json'}),...extra},...(body===undefined?{}:{body:JSON.stringify(body)})});
 const create=await call('/api/admin/members',{selected:true,body:{name:'合成结构诊断员工',quota:null,expires:null,allowed_models:null}});assert.equal(create.status,201);const member=await create.json();
 await store.run("INSERT INTO harbor.accounts(id,name,home,state,department_id) VALUES($1,'合成上游','synthetic-home','ready','department-default')",randomUUID());
 const send=async(input=[{type:'synthetic_private_type',content:'synthetic private user text',input:'synthetic private parameters',api_key:'synthetic-private-key'}])=>{const r=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','x-harbor-recording-input-diagnostics':'1'},body:JSON.stringify({model:'gpt-6.1-sol',stream:false,input,recording_input_diagnostics:true})});assert.equal(r.status,200);assert.equal((await r.json()).output[0].content[0].text,'synthetic model output');await app.buffer.flush();};
 // This trusted in-process probe is not an HTTP/global authorization route.
 const diagnosticScope=createDepartmentScopeIssuer().superAdmin({credentialId:'synthetic-diagnostic-test',credentialVersion:0});
 const internalStatus=()=>app.recordingBackend.status({scope:diagnosticScope});
 return {app,store,origin,cookie,call,login,member,send,internalStatus,calls:()=>calls,advance:n=>{now+=n;}};
}
for(const enabled of [false,true])test('真实PG/HTTP输入结构诊断 '+(enabled?'开启后内部collector有界，所有业务HTTP均不暴露全局诊断':'关闭无新增字段且客户端不能开启'),integration,async t=>{
 const f=await fixture(t,{enabled});await f.send();
 const response=await f.call('/api/admin/recordings/status');assert.equal(response.status,200);const status=await response.json();assert.equal(Object.hasOwn(status,'input_shape_diagnostics'),false);
 const internal=await f.internalStatus();assert.equal(Object.hasOwn(internal,'input_shape_diagnostics'),enabled);
 const row=await f.store.get('SELECT business_status,recording_state,recording_reason FROM harbor.requests');assert.equal(row.business_status,'ok');assert.equal(row.recording_state,'partial');assert.equal(row.recording_reason,'capture_failed');
 if(enabled){
  const d=internal.input_shape_diagnostics;assert.equal(d.requests,1);assert.equal(d.groups.length,1);assert.equal(d.groups[0].type,'unknown_token');assert.equal(d.groups[0].has_content,true);
  const text=JSON.stringify(d);for(const secret of ['synthetic_private_type','synthetic private user text','synthetic private parameters','synthetic-private-key'])assert.ok(!text.includes(secret));
  const scoped=await f.call('/api/admin/recordings/status',{selected:true});assert.equal(scoped.status,200);assert.equal(Object.hasOwn(await scoped.json(),'input_shape_diagnostics'),false);
  const issued=await f.call('/api/admin/departments/department-default/management-keys',{body:{label:'合成部门管理员'}});assert.equal(issued.status,201);const departmentCookie=await f.login((await issued.json()).secret);
  const department=await f.call('/api/admin/recordings/status',{auth:departmentCookie});assert.equal(department.status,200);assert.equal(Object.hasOwn(await department.json(),'input_shape_diagnostics'),false);
  const forged=await f.call('/api/admin/recordings/status',{auth:departmentCookie,extra:{'x-harbor-department':'global'}});assert.notEqual(forged.status,200);
  const rootGlobal=await f.call('/api/admin/recordings/status',{extra:{'x-harbor-department':'global'}});assert.equal(rootGlobal.status,403);await rootGlobal.arrayBuffer();
  const created=await f.call('/api/admin/departments',{body:{name:'合成其他部门'}});assert.equal(created.status,201);const otherId=(await created.json()).id;
  const otherKey=await f.call('/api/admin/departments/'+otherId+'/management-keys',{body:{label:'合成其他部门管理员'}});assert.equal(otherKey.status,201);const otherCookie=await f.login((await otherKey.json()).secret);
  const otherResponse=await f.call('/api/admin/recordings/status',{auth:otherCookie});assert.equal(otherResponse.status,200);const other=await otherResponse.json();assert.equal(Object.hasOwn(other,'input_shape_diagnostics'),false);assert.equal(Object.hasOwn(other,'key_version'),false);assert.equal(other.index_pending,0);assert.ok(status.index_pending>0);
  for(const options of [{extra:{'x-harbor-department':otherId}},{auth:otherCookie,extra:{'x-harbor-department':'department-default'}},{auth:otherCookie,extra:{'x-harbor-department':'global'}}]){const denied=await f.call('/api/admin/recordings/status',options);assert.equal(denied.status,403);await denied.arrayBuffer();}
  const employee=await fetch(f.origin+'/api/admin/recordings/status',{headers:{authorization:'Bearer '+f.member.secret}});assert.equal(employee.status,401);await employee.arrayBuffer();
  await f.send();await f.send();const capped=await f.internalStatus();assert.equal(capped.input_shape_diagnostics.requests,2);assert.equal(capped.input_shape_diagnostics.groups[0].count,2);assert.equal(capped.input_shape_diagnostics.state,'request_limit');
  f.advance(10);const expired=await f.internalStatus();assert.equal(expired.input_shape_diagnostics.state,'expired');assert.equal(expired.input_shape_diagnostics.groups[0].count,2);assert.equal(f.calls(),3);
 }
});
test('真实PG/HTTP agent_message未知字段仍partial，诊断不携带正文或身份',integration,async t=>{
 const f=await fixture(t,{enabled:true});
 await f.send([{type:'agent_message',author:'synthetic author',recipient:'synthetic recipient',content:[{type:'input_text',text:'synthetic agent content'}],auxiliary:'synthetic auxiliary value'}]);
 const response=await f.call('/api/admin/recordings/status');assert.equal(response.status,200);
 const status=await response.json();assert.equal(Object.hasOwn(status,'input_shape_diagnostics'),false);
 for(const value of ['synthetic author','synthetic recipient','synthetic agent content','auxiliary','synthetic auxiliary value'])assert.ok(!JSON.stringify(status).includes(value));
 const row=await f.store.get('SELECT business_status,recording_state,recording_reason FROM harbor.requests');
 assert.equal(row.business_status,'ok');assert.equal(row.recording_state,'partial');assert.equal(row.recording_reason,'capture_failed');assert.equal(f.calls(),1);
});
test('环境启用与构造关闭、上下文关闭组合不会绕过关闭行为',integration,async t=>{
 for(const options of [{enabled:false,env:{HARBOR_RECORDING_INPUT_DIAGNOSTICS:'1'}},{env:{HARBOR_RECORDING_INPUT_DIAGNOSTICS:'1'},contextEnabled:false}])await t.test(JSON.stringify(options),async t=>{
  const f=await fixture(t,options);await f.send();const status=await(await f.call('/api/admin/recordings/status')).json();assert.equal(Object.hasOwn(status,'input_shape_diagnostics'),false);assert.equal(f.calls(),1);
 });
});
test('真实加密上下文缓冲隔离诊断callback异常，既有partial原因和原始过滤保持不变',integration,async t=>{
 const f=await fixture(t,{enabled:false});await f.send();const row=await f.store.get('SELECT id FROM harbor.requests'),payload={input:[{type:'synthetic_private_type',content:'synthetic private user text',input:'synthetic private parameters',api_key:'synthetic-private-key'}]};
 const baseline=await createRequestContextCapture(f.app.buffer,{cooperative:true})(row.id,payload);
 for(const beginRequest of [()=>{throw Error('synthetic begin failure');},()=>()=>{throw Error('synthetic observer failure');},()=>()=>Promise.reject(Error('synthetic rejected callback'))]){
  const capture=createRequestContextCapture(f.app.buffer,{cooperative:true,inputDiagnostics:{beginRequest}});assert.deepEqual(await capture(row.id,payload),baseline);
 }
 await f.app.buffer.flush();const current=await f.store.get('SELECT business_status,recording_state,recording_reason FROM harbor.requests');assert.equal(current.business_status,'ok');assert.equal(current.recording_state,'partial');assert.equal(current.recording_reason,'capture_failed');assert.equal(f.calls(),1);
});

test('可信服务端环境1实际启用诊断，缺省预算仍受构造参数约束',integration,async t=>{
 const f=await fixture(t,{env:{HARBOR_RECORDING_INPUT_DIAGNOSTICS:'1'}});await f.send();
 const response=await f.call('/api/admin/recordings/status');assert.equal(response.status,200);const status=await response.json();assert.equal(Object.hasOwn(status,'input_shape_diagnostics'),false);const internal=await f.internalStatus();assert.equal(internal.input_shape_diagnostics.requests,1);assert.equal(internal.input_shape_diagnostics.limits.maxRequests,2);
});

test('真实HTTP/PG保存agent明文并排除opaque内容，身份归属仍来自员工Key',integration,async t=>{
 const f=await fixture(t,{enabled:true}),input=[{type:'agent_message',id:null,author:'synthetic-untrusted-author',recipient:'synthetic-recipient',content:[{type:'input_text',text:'synthetic visible agent text Bearer synthetic-secret'},{type:'encrypted_content',encrypted_content:'SYNTHETIC_OPAQUE_MUST_NOT_PERSIST'}]}];
 await f.send(input);const row=await f.store.get('SELECT id,member_id,business_status,recording_state FROM harbor.requests');assert.equal(row.business_status,'ok');assert.equal(row.recording_state,'complete');assert.equal(row.member_id,f.member.memberId);
 const snapshot=await f.store.get('SELECT member_id,state,coverage FROM harbor.recording_context_snapshots WHERE request_id=$1',row.id);assert.equal(snapshot.member_id,f.member.memberId);assert.equal(snapshot.state,'complete');assert.equal(snapshot.coverage.opaqueReasoningExcluded,1);
 const scope=createDepartmentScopeIssuer().superAdmin({credentialId:'synthetic-test-only',credentialVersion:0}),view=await f.app.buffer.repository.contextRepository.read(row.id,{scope});assert.equal(view.state,'complete');
 const contents=view.parts.map(p=>JSON.parse(p.text)),agent=contents.find(p=>p.value?.type==='agent_message').value;
 assert.equal(agent.author,input[0].author);assert.equal(agent.recipient,input[0].recipient);assert.equal(Object.hasOwn(agent,'role'),false);assert.match(JSON.stringify(agent),/synthetic visible agent text/);assert.deepEqual(agent.content[1],{type:'encrypted_content'});
 for(const forbidden of ['synthetic-secret','SYNTHETIC_OPAQUE_MUST_NOT_PERSIST'])assert.ok(!JSON.stringify(view).includes(forbidden));assert.equal(f.calls(),1);
});
