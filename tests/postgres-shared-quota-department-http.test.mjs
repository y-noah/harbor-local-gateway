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

import {createPostgresQuotaSubjects} from '../postgres-quota-subjects.mjs';
import {createCodexQuotaSubjectIdentity,codexQuotaSubject} from '../quota-subject.mjs';
import {normalizeLimits} from '../rates.mjs';

test('同一上游跨部门并发和未知消耗均可派发，历史归属和部门可见性保持隔离',integration,async()=>{
 const admin=new PostgresStore(options()),database='harbor_shared_http_'+randomUUID().replaceAll('-',''),directory=await mkdtemp(join(tmpdir(),'harbor-shared-http-'));let store,app,upstream,created=false,release;const gate=new Promise(resolve=>{release=resolve;}),calls=[];let omitUsage=true,holding=true;
 const until=async check=>{const end=Date.now()+8000;while(!await check()){assert.ok(Date.now()<end,'shared dispatch deadline');await new Promise(resolve=>setTimeout(resolve,5));}};
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate({quotaSubjects:true});await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),departments=createPostgresDepartments(store,{issuer}),root=issuer.superAdmin({credentialId:'synthetic-setup',credentialVersion:0}),a=await departments.createDepartment(root,{name:'合成A'}),b=await departments.createDepartment(root,{name:'合成B'}),ka=await departments.issueManagementKey(root,a.id,{label:'A'}),kb=await departments.issueManagementKey(root,b.id,{label:'B'});
  upstream=http.createServer((req,res)=>{void(async()=>{for await(const chunk of req){}calls.push(req.headers['chatgpt-account-id']);if(holding)await gate;res.writeHead(200,{'content-type':'text/event-stream'});res.end('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-response',status:'completed',model:'gpt-6.1-sol',output:[{type:'message',id:'synthetic-message',role:'assistant',content:[{type:'output_text',text:'合成完成'}]}],...(!omitUsage?{usage:{input_tokens:10,output_tokens:3}}:{})}})+'\n\n');})().catch(()=>res.destroy());});await new Promise(resolve=>upstream.listen(0,'127.0.0.1',resolve));
  const transport=createResponsesTransport({credentials:async()=>({token:'synthetic-subscription',accountId:'same-upstream-account'}),version:async()=>'0.160.1',fetchImpl:(url,init)=>fetch('http://127.0.0.1:'+upstream.address().port+new URL(url).pathname,init)});
  app=await createPostgresGateway({env:{},database:options(database),dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-super')},recording:{enabled:false},responsesTransport:transport});await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  const call=async(path,{cookie,body}={})=>{const r=await fetch(origin+path,{method:body?'POST':'GET',headers:{...(cookie?{cookie}:{}),...(body?{'content-type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,value:await r.json(),cookie:r.headers.get('set-cookie')?.split(';')[0]};};
  const la=await call('/api/auth/login',{body:{password:ka.secret}}),lb=await call('/api/auth/login',{body:{password:kb.secret}}),ma=await call('/api/admin/members',{cookie:la.cookie,body:{name:'合成A员工',quota:null,expires:null}}),mb=await call('/api/admin/members',{cookie:lb.cookie,body:{name:'合成B员工',quota:null,expires:null}});assert.equal(ma.status,201);assert.equal(mb.status,201);
  const aid=randomUUID(),bid=randomUUID(),identity=createCodexQuotaSubjectIdentity(Buffer.alloc(32,17),{schemaVersion:'0.167.2'}),subjects=createPostgresQuotaSubjects(store,{identity});
  const quota=normalizeLimits({accountId:'same-upstream-account',ordinaryUsageAllowed:true,rateLimits:{limitId:'codex',primary:{usedPercent:30,windowDurationMins:300,resetsAt:Math.ceil(Date.now()/1000)+3600}}},Date.now(),{subjectIdentity:identity});
  for(const [id,department] of [[aid,a.id],[bid,b.id]]){await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,state,quota_json) VALUES($1,$2,'合成订阅',$1,'ready',$3)",id,department,JSON.stringify(quota));const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);await store.transaction(()=>subjects.refresh(account,codexQuotaSubject(quota),quota,{checkedFrom:quota.checked-1}));}
  const invoke=async secret=>{const r=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json','thread-id':randomUUID()},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成并发请求',stream:false})});return {status:r.status,value:await r.json(),id:r.headers.get('x-harbor-request-id')};};
  const pa=invoke(ma.value.secret),pb=invoke(mb.value.secret);await until(()=>calls.length===2);holding=false;release();const [ra,rb]=await Promise.all([pa,pb]);assert.equal(ra.status,200);assert.equal(rb.status,200);assert.deepEqual(calls,['same-upstream-account','same-upstream-account']);
  const before=await store.all('SELECT id,department_id,account_id,quota_subject_ref,usage_unknown,input,output FROM harbor.requests ORDER BY id');assert.equal(before.length,2);assert.ok(before.every(r=>r.usage_unknown===1));assert.equal(before[0].quota_subject_ref,before[1].quota_subject_ref);assert.ok(before[0].quota_subject_ref);assert.ok(before.some(r=>r.department_id===a.id&&r.account_id===aid));assert.ok(before.some(r=>r.department_id===b.id&&r.account_id===bid));
  omitUsage=false;assert.equal((await invoke(ma.value.secret)).status,200);assert.equal((await invoke(mb.value.secret)).status,200);assert.equal(calls.length,4);assert.deepEqual(await store.all('SELECT id,department_id,account_id,quota_subject_ref,usage_unknown,input,output FROM harbor.requests WHERE id=ANY($1::text[]) ORDER BY id',before.map(r=>r.id)),before);
  const own=await call('/api/admin/requests/'+ra.id,{cookie:la.cookie});assert.equal(own.status,200);assert.doesNotMatch(JSON.stringify(own.value),/quota_subject_ref|same-upstream-account/);assert.equal((await call('/api/admin/requests/'+ra.id,{cookie:lb.cookie})).status,404);assert.equal((await call('/api/admin/requests/'+rb.id,{cookie:la.cookie})).status,404);
 }finally{release();await app?.close();if(upstream){upstream.closeAllConnections();await new Promise(resolve=>upstream.close(resolve));}await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();await rm(directory,{recursive:true,force:true});}
});
