import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createPostgresDepartmentHttp} from '../postgres-department-http.mjs';
import {createRequestBodyBudget} from '../request-body-budget.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createPostgresSessionHandoffs} from '../postgres-session-handoffs.mjs';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';

const databaseUrl=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!databaseUrl?'需独立真实PG测试库':false,timeout:60000};
function options(database){const url=new URL(databaseUrl);if(process.env.HARBOR_TEST_PG_SOCKET)return {connection:{host:process.env.HARBOR_TEST_PG_SOCKET,port:Number(process.env.HARBOR_TEST_PG_PORT),user:decodeURIComponent(url.username),password:decodeURIComponent(url.password),database:database||'postgres'},poolSize:2};if(database)url.pathname='/'+database;return {connectionString:url.href,allowPlaintextLocalNetwork:true,poolSize:2};}

test('真实HTTP部门管理只由超管执行，选择头不能提升部门权限且撤销即时生效',integration,async()=>{
 const admin=new PostgresStore(options()),database='harbor_department_http_'+randomUUID().replaceAll('-','');let store,server,created=false,bodies;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),departments=createPostgresDepartments(store,{issuer}),auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-super')},{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}});
  bodies=createRequestBodyBudget();const access=createPostgresDepartmentHttp({auth,departments,issuer,installed:true,readBody:bodies.read});
  server=http.createServer(async(req,res)=>{
   try{
    if(req.url==='/login'&&req.method==='POST'){const body=await bodies.read(req,{owner:'login'});await auth.loginAsync(body.password,res);res.end('{}');return;}
    if(req.url==='/scope'){const scope=await access.context(req);res.end(JSON.stringify({role:scope.role,view:scope.view,department_id:scope.department_id}));return;}
    if(await access.handle(req,res))return;res.writeHead(404);res.end('{}');
   }catch(error){res.writeHead(error.status??500,{'Content-Type':'application/json'});res.end(JSON.stringify({code:error.code??'internal_error'}));}
   finally{bodies.release(req);}
  });await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const call=async(path,{cookie,method='GET',body,department}={})=>{
   const response=await fetch(origin+path,{method,headers:{...(cookie?{cookie}:{}),...(body?{'Content-Type':'application/json'}:{}),...(department?{'X-Harbor-Department':department}:{})},body:body?JSON.stringify(body):undefined});
   return {status:response.status,value:await response.json(),cookie:response.headers.get('set-cookie')?.split(';')[0]};
  };
  const superLogin=await call('/login',{method:'POST',body:{password:'synthetic-super'}});assert.equal(superLogin.status,200);const cookie=superLogin.cookie;
  const a=await call('/api/admin/departments',{cookie,method:'POST',body:{name:'合成A'}}),b=await call('/api/admin/departments',{cookie,method:'POST',body:{name:'合成B'}});assert.equal(a.status,201);assert.equal(b.status,201);
  const first=await call('/api/admin/departments',{cookie});assert.equal(first.status,200);assert.equal(first.value.rows.length,3);
  assert.equal((await call('/api/admin/departments?limit=21',{cookie})).status,400);
  const issued=await call('/api/admin/departments/'+a.value.id+'/management-keys',{cookie,method:'POST',body:{label:'合成管理者'}});assert.equal(issued.status,201);assert.match(issued.value.secret,/^hdm_/);
  const listed=await call('/api/admin/departments/'+a.value.id+'/management-keys',{cookie});assert.equal(JSON.stringify(listed.value).includes(issued.value.secret),false);assert.equal(Object.hasOwn(listed.value.rows[0],'digest'),false);
  const departmentLogin=await call('/login',{method:'POST',body:{password:issued.value.secret}}),departmentCookie=departmentLogin.cookie;
  const own=await call('/scope',{cookie:departmentCookie});assert.equal(own.value.department_id,a.value.id);
  assert.equal((await call('/scope',{cookie:departmentCookie,department:b.value.id})).status,403);
  assert.equal((await call('/scope',{cookie:departmentCookie,department:'global'})).status,403);
  assert.equal((await call('/api/admin/departments',{cookie:departmentCookie})).status,403);
  assert.equal((await call('/api/admin/departments/'+b.value.id+'/management-keys',{cookie:departmentCookie,method:'POST',body:{label:'forbidden'}})).status,403);
  const selected=await call('/scope',{cookie,department:a.value.id});assert.equal(selected.value.department_id,a.value.id);assert.equal(selected.value.role,'super_admin');
  assert.equal((await call('/api/admin/departments',{cookie,department:a.value.id,method:'POST',body:{name:'forbidden-global-write'}})).status,403);
  const revoked=await call('/api/admin/departments/'+a.value.id+'/management-keys/'+issued.value.key.id+'/revoke',{cookie,method:'POST',body:{expected_version:issued.value.key.version}});assert.equal(revoked.status,200);
  assert.equal((await call('/scope',{cookie:departmentCookie})).status,401);
  assert.equal((await call('/api/admin/departments',{cookie})).value.rows.length,3);
 }finally{if(server)await new Promise(resolve=>server.close(resolve));await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();}
});

test('真实网关隔离部门首屏、员工及模型调用归属，拒跨部门详情和会话',integration,async()=>{
 const admin=new PostgresStore(options()),database='harbor_department_gateway_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-department-http-'));let store,app,upstream,created=false;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),departments=createPostgresDepartments(store,{issuer}),superScope=issuer.superAdmin({credentialId:'synthetic-setup',credentialVersion:0});
  const a=await departments.createDepartment(superScope,{name:'合成A'}),b=await departments.createDepartment(superScope,{name:'合成B'}),keyA=await departments.issueManagementKey(superScope,a.id,{label:'合成A'}),keyB=await departments.issueManagementKey(superScope,b.id,{label:'合成B'});
  const upstreamCalls=[];upstream=http.createServer((req,res)=>{if(req.method==='GET'){res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({models:[{slug:'gpt-6.1-sol'}]}));return;}void(async()=>{const chunks=[];for await(const chunk of req)chunks.push(chunk);const payload=JSON.parse(Buffer.concat(chunks));upstreamCalls.push({account:req.headers['chatgpt-account-id'],model:payload.model});res.writeHead(200,{'Content-Type':'text/event-stream'});res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-response',status:'completed',model:payload.model,output:[{id:'message',type:'message',content:[{type:'output_text',text:'合成完成'}]}],usage:{input_tokens:10,output_tokens:3,input_tokens_details:{cached_tokens:0}}}})+'\n\n');})().catch(()=>res.destroy());});await new Promise(resolve=>upstream.listen(0,'127.0.0.1',resolve));
  const upstreamOrigin='http://127.0.0.1:'+upstream.address().port,responsesTransport=createResponsesTransport({credentials:async(_binary,home)=>({token:'synthetic-subscription',accountId:home}),version:async()=>'0.160.1',fetchImpl:(url,init)=>fetch(upstreamOrigin+new URL(url).pathname,init)});
  app=await createPostgresGateway({env:{},database:options(database),dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-super')},recording:{enabled:true,storage:{pgDataDir:(await store.get("SELECT current_setting('data_directory') value")).value,minFreeBytes:0,minFreeRatio:0,cacheMs:0}},recordingBuffer:{flushMs:20},responsesTransport});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  const call=async(path,{cookie,method='GET',body,department}={})=>{const response=await fetch(origin+path,{method,headers:{...(cookie?{cookie}:{}),...(body?{'Content-Type':'application/json'}:{}),...(department?{'X-Harbor-Department':department}:{})},body:body?JSON.stringify(body):undefined});return {status:response.status,value:await response.json(),cookie:response.headers.get('set-cookie')?.split(';')[0]};};
  const loginA=await call('/api/auth/login',{method:'POST',body:{password:keyA.secret,includeOverview:true}}),loginB=await call('/api/auth/login',{method:'POST',body:{password:keyB.secret,includeOverview:true}});
  assert.equal(loginA.status,200);assert.equal(loginA.value.identity.department_id,a.id);assert.equal(loginA.value.identity.department_name,'合成A');const sessionA=await call('/api/auth/session',{cookie:loginA.cookie});assert.equal(sessionA.value.identity.department_name,'合成A');assert.ok(loginA.value.initialState);assert.equal(Object.hasOwn(loginA.value.initialState,'busy'),false);assert.equal(loginB.status,200);
  const planning=await call('/api/admin/balancing/plans',{cookie:loginA.cookie,method:'POST',body:{}});assert.equal(planning.status,202,JSON.stringify(planning.value));
  const planningJob=await store.get('SELECT department_id,actor_kind,actor_id FROM harbor.background_jobs WHERE id=$1',planning.value.id);assert.equal(planningJob.department_id,a.id);assert.equal(planningJob.actor_kind,'department_admin');assert.equal(planningJob.actor_id,keyA.key.id);
  const ownPlan=await call('/api/admin/balancing/plans/'+planning.value.id,{cookie:loginA.cookie});assert.equal(ownPlan.status,200,JSON.stringify(ownPlan.value));assert.equal(ownPlan.value.ready,false);
  assert.equal((await call('/api/admin/balancing/plans/'+planning.value.id,{cookie:loginB.cookie})).status,404);
  assert.equal((await call('/api/admin/balancing/plans/'+planning.value.id+'/items?section=moves&limit=20',{cookie:loginB.cookie})).status,404);
  assert.equal((await call('/api/admin/balancing/plans',{cookie:loginA.cookie,method:'POST',body:{department_id:b.id}})).status,400);
  const memberA=await call('/api/admin/members',{cookie:loginA.cookie,method:'POST',body:{name:'A的员工',quota:null,expires:null}}),memberB=await call('/api/admin/members',{cookie:loginB.cookie,method:'POST',body:{name:'B的员工',quota:null,expires:null}});assert.equal(memberA.status,201);assert.equal(memberB.status,201);
  const listing=await call('/api/admin/members',{cookie:loginA.cookie});assert.equal(listing.status,200);assert.equal(JSON.stringify(listing.value).includes('B的员工'),false);assert.ok(JSON.stringify(listing.value).includes('A的员工'));
  assert.equal((await call('/api/admin/members/'+memberB.value.memberId,{cookie:loginA.cookie})).status,404);
  assert.equal((await call('/api/admin/members',{cookie:loginA.cookie,department:b.id})).status,403);
  assert.equal((await call('/api/admin/status',{cookie:loginA.cookie})).status,401);
  const superLogin=await call('/api/auth/login',{method:'POST',body:{password:'synthetic-super',includeOverview:true}});assert.equal(superLogin.status,200);assert.ok(superLogin.value.initialState);
  const global=await call('/api/admin/members',{cookie:superLogin.cookie});assert.equal(global.status,200);assert.ok(JSON.stringify(global.value).includes('A的员工')&&JSON.stringify(global.value).includes('B的员工'));
  assert.equal((await call('/api/admin/members',{cookie:superLogin.cookie,method:'POST',body:{name:'缺部门',quota:null,expires:null}})).status,403);
  assert.equal((await call('/api/admin/pricing',{cookie:loginA.cookie})).status,403);
  assert.equal((await call('/api/admin/pricing',{cookie:superLogin.cookie,department:a.id})).status,403);
  const devices=await call('/api/admin/devices',{cookie:loginA.cookie});assert.equal(devices.status,200,JSON.stringify(devices.value));
  assert.equal((await call('/api/admin/devices?member_id='+memberB.value.memberId,{cookie:loginA.cookie})).status,404);
  const policy=await call('/api/admin/members/'+memberA.value.memberId+'/device-policy',{cookie:loginA.cookie});assert.equal(policy.status,200,JSON.stringify(policy.value));assert.equal(policy.value.member_id,memberA.value.memberId);
  assert.equal((await call('/api/admin/members/'+memberB.value.memberId+'/device-policy',{cookie:loginA.cookie})).status,404);
  const accountA=randomUUID(),accountB=randomUUID(),quota=JSON.stringify({plan:'pro',checked:Date.now(),remaining:50,windows:[{used:50,minutes:10080,reset:Date.now()+86400000}]});
  for(const [account,department] of [[accountA,a.id],[accountB,b.id]])await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,state,quota_json) VALUES($1,$2,'合成订阅',$1,'ready',$3)",account,department,quota);
  const invoke=async(secret,fixed)=>{const response=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+secret,'Content-Type':'application/json','thread-id':'synthetic-shared-client-thread',...(fixed?{'X-Harbor-Session':fixed}:{})},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成部门隔离',stream:false})});return {status:response.status,value:await response.json(),requestId:response.headers.get('x-harbor-request-id')};};
  const resultA=await invoke(memberA.value.secret),resultB=await invoke(memberB.value.secret);assert.equal(resultA.status,200,JSON.stringify(resultA.value));assert.equal(resultB.status,200,JSON.stringify(resultB.value));
  assert.deepEqual(upstreamCalls.map(row=>row.account),[accountA,accountB]);
  const requestA=await store.get('SELECT department_id,account_id,session_id FROM harbor.requests WHERE id=$1',resultA.requestId),requestB=await store.get('SELECT department_id,account_id,session_id FROM harbor.requests WHERE id=$1',resultB.requestId);assert.equal(requestA.department_id,a.id);assert.equal(requestB.department_id,b.id);assert.notEqual(requestA.session_id,requestB.session_id);
  const extraA=await call('/api/admin/accounts/'+accountA+'/extra-usage',{cookie:loginA.cookie});assert.equal(extraA.status,200,JSON.stringify(extraA.value));assert.equal(extraA.value.account_id,accountA);
  assert.equal((await call('/api/admin/accounts/'+accountA+'/extra-usage',{cookie:loginB.cookie})).status,404);
  assert.equal((await call('/api/admin/extra-usage/policy',{cookie:loginA.cookie})).status,200);
  const handoff=await call('/api/admin/sessions/'+requestA.session_id+'/handoffs',{cookie:loginA.cookie});assert.equal(handoff.status,200,JSON.stringify(handoff.value));assert.equal(handoff.value.binding.account_id,accountA);
  assert.equal((await call('/api/admin/sessions/'+requestA.session_id+'/handoffs',{cookie:loginB.cookie})).status,404);
  assert.equal((await call('/api/admin/sessions/'+requestA.session_id+'/handoffs',{cookie:loginA.cookie,method:'POST',body:{account_id:accountB,expected_route_version:(await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',requestA.session_id)).route_version}})).status,404);
  const nextAccount=randomUUID();await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,state,quota_json) VALUES($1,$2,'合成A转移目标',$1,'ready',$3)",nextAccount,a.id,quota);
  const version=(await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',requestA.session_id)).route_version;
  const transfer=await call('/api/admin/sessions/'+requestA.session_id+'/handoffs',{cookie:loginA.cookie,method:'POST',body:{account_id:nextAccount,expected_route_version:version}});assert.equal(transfer.status,202,JSON.stringify(transfer.value));
  const transferDeadline=Date.now()+10000;let decision;
  do{decision=await store.get('SELECT * FROM harbor.session_handoffs WHERE id=$1',transfer.value.handoff_id);if(decision?.status!=='waiting')break;if(Date.now()>transferDeadline)throw Error('部门交接未在期限内结束');await new Promise(resolve=>setTimeout(resolve,5));}while(true);
  assert.equal(decision.status,'completed',JSON.stringify(decision));assert.equal(decision.department_id,a.id);assert.equal(decision.actor_id,keyA.key.id);
  assert.equal((await store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',requestA.session_id)).account_id,nextAccount);
  await app.buffer.flush();
  const captured=await store.all('SELECT r.id,r.department_id,r.recording_state,COUNT(e.sequence)::int events FROM harbor.requests r LEFT JOIN harbor.conversation_events e ON e.request_id=r.id WHERE r.id=ANY($1::text[]) GROUP BY r.id',[resultA.requestId,resultB.requestId]);
  assert.equal(captured.length,2);assert.ok(captured.every(row=>row.recording_state==='complete'&&row.events>=2),JSON.stringify({captured,capture:app.status().recordingCapture,storage:app.status().recordingStorage}));
  const ownBody=await call('/api/admin/requests/'+resultA.requestId+'/recording',{cookie:loginA.cookie});
  assert.equal(ownBody.status,200,JSON.stringify(ownBody.value));
  assert.equal((await call('/api/admin/requests/'+resultA.requestId+'/recording',{cookie:loginB.cookie})).status,404);
  for(const [employee,department] of [[memberA,a],[memberB,b]]){
   const response=await fetch(origin+'/api/me',{headers:{authorization:'Bearer '+employee.value.secret}}),value=await response.json();
   assert.equal(response.status,200,JSON.stringify(value));assert.equal(value.department_id,department.id);assert.equal(value.standard.requests,1);assert.equal(value.keyStandard.requests,1);
  }
  const forbidden=await invoke(memberB.value.secret,requestA.session_id);assert.ok([403,404].includes(forbidden.status));assert.equal(upstreamCalls.length,2);
  const preview=await call('/api/admin/member-action-batches/preview',{cookie:loginA.cookie,method:'POST',body:{kind:'member_access',enabled:false,member_ids:[memberA.value.memberId]}});
  assert.equal(preview.status,201,JSON.stringify(preview.value));
  assert.equal((await call('/api/admin/member-action-batches/'+preview.value.id,{cookie:loginB.cookie})).status,404);
  assert.equal((await call('/api/admin/member-action-batches/'+preview.value.id+'/execute',{cookie:loginB.cookie,method:'POST',body:{}})).status,404);
  const submitted=await call('/api/admin/member-action-batches/'+preview.value.id+'/execute',{cookie:loginA.cookie,method:'POST',body:{}});
  assert.equal(submitted.status,202,JSON.stringify(submitted.value));
  const job=await store.get("SELECT department_id,actor_kind,actor_id FROM harbor.background_jobs WHERE kind='member.actions' AND target=$1",preview.value.id);
  assert.equal(job.department_id,a.id);assert.equal(job.actor_kind,'department_admin');assert.equal(job.actor_id,keyA.key.id);
  for(const [cookie,department] of [[loginA.cookie,a],[loginB.cookie,b]]){const history=await call('/api/admin/history?kind=audit',{cookie});assert.equal(history.status,200,JSON.stringify(history.value));assert.equal(history.value.pagination.pageSize,20);assert.ok(history.value.rows.length>0);assert.ok(history.value.rows.every(row=>row.department_id===department.id));}
  for(const [path,payload,table] of [
   ['/api/admin/members',{name:'注销后不得创建',quota:null,expires:null},'members'],
   ['/api/admin/device-invitations',{member_id:memberA.value.memberId},'device_invitations'],
   ['/api/admin/accounts/'+accountA+'/extra-usage/manual',{remaining_atoms:'1000',unit:'credits',models:['gpt-6.1-sol'],protocols:['responses'],expires_at:null,reset_cards:null,note:'合成注销后不得提交',expected_version:0,expected_identity_epoch:0},'account_extra_records'],
  ]){
   const login=await call('/api/auth/login',{method:'POST',body:{password:keyA.secret}});assert.equal(login.status,200);
   const before=(await store.get('SELECT COUNT(*) n FROM harbor.'+table)).n,body=JSON.stringify(payload);let request;
   const result=new Promise((resolve,reject)=>{request=http.request(origin+path,{method:'POST',headers:{cookie:login.cookie,'Content-Type':'application/json','Content-Length':Buffer.byteLength(body)}},response=>{const chunks=[];response.on('data',chunk=>chunks.push(chunk));response.on('end',()=>resolve({status:response.statusCode,value:JSON.parse(Buffer.concat(chunks).toString())}));});request.on('error',reject);request.setTimeout(10000,()=>request.destroy(new Error('慢请求测试超时')));request.write(body.slice(0,1));});
   try{
    const deadline=Date.now()+5000;while(app.status().requestBodies.readers!==1){if(Date.now()>deadline)throw Error('未观察到请求体读取');await new Promise(resolve=>setTimeout(resolve,5));}
    assert.equal((await call('/api/auth/logout',{cookie:login.cookie,method:'POST',body:{}})).status,200);
    request.end(body.slice(1));const response=await result;assert.ok([401,403].includes(response.status),JSON.stringify(response));
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.'+table)).n,before);
   }finally{request.destroy();await result.catch(()=>{});}
  }
  for(const revoked of [false,true]){
   const management=await departments.issueManagementKey(superScope,a.id,{label:'合成恢复'+revoked}),scope=await departments.authenticateManagementKey(management.secret),services=[];let ticket;
   const service=()=>{const gate=createSessionRequestHandoff(),value=createPostgresSessionHandoffs(store,{gate,departmentActors:departments,validateTarget:async()=>true});services.push(value);return {gate,value};};
   try{
    const first=service();ticket=first.gate.arrive(memberA.value.memberId);await ticket.bind(requestA.session_id);
    const before=await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',requestA.session_id),target=before.account_id===accountA?nextAccount:accountA;
    const pending=await first.value.handle('/api/admin/sessions/'+requestA.session_id+'/handoffs','POST',{account_id:target,expected_route_version:before.route_version},{scope});assert.equal(pending.status,202);
    await first.value.close();ticket.close();assert.equal((await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',pending.value.handoff_id)).status,'waiting');
    if(revoked)await departments.revokeManagementKey(superScope,a.id,management.key.id,{expected_version:management.key.version});
    const second=service();await second.value.recover();const deadline=Date.now()+10000;let row;
    do{row=await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',pending.value.handoff_id);if(row.status!=='waiting')break;if(Date.now()>deadline)throw Error('恢复交接未在期限内结束');await new Promise(resolve=>setTimeout(resolve,5));}while(true);
    assert.equal(row.status,revoked?'cancelled':'completed');assert.equal((await store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',requestA.session_id)).account_id,revoked?before.account_id:target);
   }finally{ticket?.close();await Promise.all(services.map(value=>value.close()));}
  }
  await departments.updateDepartment(superScope,a.id,{expected_version:a.version,enabled:false});assert.equal((await invoke(memberA.value.secret)).status,403);assert.equal(upstreamCalls.length,2);assert.equal((await invoke(memberB.value.secret)).status,200);assert.equal(upstreamCalls.length,3);
 }finally{await app?.close();if(upstream)await new Promise(resolve=>upstream.close(resolve));await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();await rm(directory,{recursive:true,force:true});}
});
