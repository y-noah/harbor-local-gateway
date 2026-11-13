import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomBytes,randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {migrateConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createPostgresChatState} from '../postgres-chat-state.mjs';
import {createPostgresChatService} from '../postgres-chat-service.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';

const integration={skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需独立PG18真实HTTP':false,timeout:60000};
const until=async check=>{const end=Date.now()+5000;while(!await check()){assert.ok(Date.now()<end,'文本请求条件等待超时');await new Promise(resolve=>setTimeout(resolve,10));}};
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};};
async function fixture(t){
 const lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB}),store=new PostgresStore({connection:lab.connection,poolSize:8});let server,admin,service;const active=new Set(),calls=[],gates=[];
 t.after(async()=>{for(const gate of gates)gate.resolve();for(const controller of active)controller.abort();await until(()=>!active.size).catch(()=>{});server?.closeAllConnections();if(server)await new Promise(resolve=>server.close(resolve));await admin?.close();await store.close();await lab.close();});
 await store.migrate();await store.initializePricing();await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer}),a=await departments.createDepartment(superAdmin,{name:'合成部门A'}),b=await departments.createDepartment(superAdmin,{name:'合成部门B'}),scopeA=issuer.forDepartment(superAdmin,a.id),scopeB=issuer.forDepartment(superAdmin,b.id);
 admin=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32)});const member=async(scope=scopeA)=>(await admin.handle('/api/admin/members','POST',{name:'合成文本员工',quota:null,expires:null},undefined,{scope})).value,ma=await member(),mb=await member(scopeB);
 const account=async department=>{const id=randomUUID();await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,state,token_budget) VALUES($1,$2,'合成文本账号',$1,'ready',1000000)",id,department);return id;},aa=await account(a.id),ab=await account(b.id);
 const state=createPostgresChatState(store,{checkLimits:async()=>{}}),activity={active,members:new Map(),preparing:new Map(),accounts:new Map(),preparation:{enter:async()=>()=>{}},isStopping:()=>false};let gate=null;
 service=createPostgresChatService({store,recordingEnabled:false,meteringRecovery:{settle:()=>false},checkLimits:async()=>{},activity,runChat:async args=>{calls.push({accountId:args.account.id,sessionId:args.session.id});if(gate)await gate.promise;return {text:'合成回复',threadId:args.session.upstream_id??randomUUID(),input:100,output:20,cached:0,usageKnown:true,actualModel:'gpt-6.1-sol',cumulativeUsage:{input:args.session.cum_input+100,output:args.session.cum_output+20,cached:args.session.cum_cached}};}});
 server=http.createServer((req,res)=>{void service.handle(req,res).catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code??'internal'}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
 const send=async({person=ma,session,signal,headers={}}={})=>{const response=await fetch('http://127.0.0.1:'+server.address().port+'/v1/chat/completions',{method:'POST',signal,headers:{authorization:'Bearer '+person.secret,'content-type':'application/json',...headers},body:JSON.stringify({model:'codex',messages:[{role:'user',content:'合成文本请求'}],...(session?{session_id:session}:{})})});return {status:response.status,value:await response.json()};};
 return {store,departments,superAdmin,a,b,scopeA,scopeB,ma,mb,aa,ab,member,state,calls,service,send,gate:()=>{gate=deferred();gates.push(gate);return gate;}};
}

test('文本HTTP只选本部门账号，忽略伪造部门头并给会话请求落库部门',integration,async t=>{
 const f=await fixture(t);let result=await f.send({headers:{'x-harbor-department':f.b.id}});assert.equal(result.status,200,JSON.stringify(result.value));assert.equal(f.calls[0].accountId,f.aa);
 result=await f.send({person:f.mb});assert.equal(result.status,200);assert.equal(f.calls[1].accountId,f.ab);
 const rows=await f.store.all('SELECT r.department_id request_department,s.department_id session_department,a.department_id account_department,r.status FROM harbor.requests r JOIN harbor.sessions s ON s.id=r.session_id JOIN harbor.accounts a ON a.id=r.account_id');assert.equal(rows.length,2);for(const row of rows){assert.equal(row.request_department,row.session_department);assert.equal(row.request_department,row.account_department);assert.equal(row.status,'ok');}
 const forbidden=await f.send({person:f.mb,session:f.calls[0].sessionId});assert.equal(forbidden.status,404);assert.equal(f.calls.length,2);await f.store.run('UPDATE harbor.accounts SET enabled=0 WHERE id=$1',f.aa);assert.equal((await f.send()).status,503);assert.equal(f.calls.length,2);
});

test('文本排队期间部门停复，原始身份不能借重新认证恢复派发，运行请求继续结算',integration,async t=>{
 const f=await fixture(t),gate=f.gate(),first=f.send();await until(()=>f.calls.length===1);const secondMember=await f.member(),second=f.send({person:secondMember});await until(()=>f.service.status().queued===1);
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});gate.resolve();assert.equal((await first).status,200);assert.equal((await second).status,403);assert.equal(f.calls.length,1);
 const rows=await f.store.all('SELECT status,ended,reserved FROM harbor.requests ORDER BY query_sequence');assert.equal(rows[0].status,'ok');assert.equal(rows[1].status,'rejected');assert.ok(rows.every(row=>row.ended!==null&&row.reserved===0));
});

test('最终授权前Key停复、伪造scope或跨部门scope均拒绝，不调用执行器',integration,async t=>{
 const f=await fixture(t),body={model:'codex',messages:[{role:'user',content:'合成请求'}]},admission=await f.state.admit(f.ma.secret,body),personB=await f.store.authenticate(f.mb.secret);
 await assert.rejects(f.state.dispatch(f.ma.secret,admission.requestId,{scope:f.store.departmentScope(personB)}),{code:'department_forbidden'});
 await assert.rejects(f.state.dispatch(f.ma.secret,admission.requestId,{scope:JSON.parse(JSON.stringify(admission.scope))}),{code:'department_scope_required'});
 const ready=await f.state.dispatch(f.ma.secret,admission.requestId,{scope:admission.scope,deferAuthorization:true});const boundary=f.state.dispatchBoundary(f.ma.secret,{...admission,...ready});
 await f.store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',f.ma.keyId);await f.store.run('UPDATE harbor.keys SET enabled=1 WHERE id=$1',f.ma.keyId);
 await assert.rejects(boundary.run(async()=>assert.fail('失效身份不能执行CLI')),{code:'department_forbidden'});assert.equal(f.calls.length,0);
});

test('正文读取等待中部门停复不能按新身份受理，已撤销排队请求正常结算',integration,async t=>{
 const f=await fixture(t),person=await f.store.authenticate(f.ma.secret),scope=f.store.departmentScope(person);await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});
 await assert.rejects(f.state.admit(f.ma.secret,{model:'codex',messages:[{role:'user',content:'合成请求'}]},{scope}),{code:'department_forbidden'});assert.equal((await f.store.get('SELECT COUNT(*) count FROM harbor.requests')).count,0);
 const gate=f.gate(),first=f.send();await until(()=>f.calls.length===1);const secondMember=await f.member(),controller=new AbortController(),second=f.send({person:secondMember,signal:controller.signal}).catch(error=>error);await until(()=>f.service.status().queued===1);controller.abort();await second;await until(()=>f.service.status().queued===0);gate.resolve();assert.equal((await first).status,200);await until(()=>f.service.status().requests===0);assert.equal((await f.store.get('SELECT status,reserved FROM harbor.requests WHERE member_id=$1',secondMember.memberId)).status,'cancelled');assert.equal(f.calls.length,1);
});


test('文本耗尽账号仅按本部门人工正权重使用额外额度，不读取全局权重',integration,async t=>{
 const f=await fixture(t),router=createPostgresRouter(f.store);await router.saveSettings({weighted:true},{scope:f.scopeA});await router.saveWeights({mode:'manual',weights:{[f.aa]:100},expected_version:0},{scope:f.scopeA});await f.store.run("UPDATE harbor.accounts SET route_block='quota_exhausted' WHERE id=$1",f.aa);
 await f.store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('routing_weights',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",JSON.stringify({version:99,mode:'manual',expires:Date.now()+3600000,weights:{[f.aa]:0,[f.ab]:100}}));
 const result=await f.send();assert.equal(result.status,200,JSON.stringify(result.value));assert.equal(f.calls[0].accountId,f.aa);assert.equal((await f.store.get('SELECT COUNT(*) count FROM harbor.account_extra_holds')).count,0);
});
