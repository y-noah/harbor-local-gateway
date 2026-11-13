import http from 'node:http';
import {createHash} from 'node:crypto';
import {createPostgresAccountExtraUsage} from '../postgres-account-extra-usage.mjs';
import {createPostgresExtraUsageBatches} from '../postgres-extra-usage-batches.mjs';
import {createPostgresExtraUsageAdmin} from '../postgres-extra-usage-admin.mjs';
import {createRequestBodyBudget} from '../request-body-budget.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需独立PG18':false};
function options(database){if(url){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,password:process.env.HARBOR_TEST_PG_PASSWORD,database:database??'postgres'}};}
async function fixture(t,{install=true}={}){
 const cleanups=[];const database='harbor_dj_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});await root.query('CREATE DATABASE '+database);const store=new PostgresStore({...options(database),poolSize:4});t.after(async()=>{for(const cleanup of cleanups)await cleanup();await store.close();await root.query('DROP DATABASE '+database);await root.close();});await store.migrate();if(install)await migrateDepartmentIsolation(store,{apply:true});
 let at=Date.parse('2026-11-13T18:05:00Z');const clock=()=>at,issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')},auth=createAdminAuth(config,{clock,scopeIssuer:issuer}),res={headers:{},setHeader(k,v){this.headers[k]=v;}};auth.login('synthetic-root',res);const superAdmin=await auth.context({headers:{cookie:res.headers['Set-Cookie'].split(';')[0]}}),departments=createPostgresDepartments(store,{issuer,clock,authorizeSuperAdmin:auth.authorizeSuperAdmin});
 let a,b,scopeA,scopeB,keyA;if(install){a=await departments.createDepartment(superAdmin,{name:'合成A'});b=await departments.createDepartment(superAdmin,{name:'合成B'});keyA=await departments.issueManagementKey(superAdmin,a.id,{label:'A负责人'});const keyB=await departments.issueManagementKey(superAdmin,b.id,{label:'B负责人'});scopeA=await departments.authenticateManagementKey(keyA.secret);scopeB=await departments.authenticateManagementKey(keyB.secret);}
 await store.initializePricing();const jobs=createPostgresJobs(store,{clock,departmentActors:departments,scopedKinds:['account.extra.configure'],leaseMs:30000});return {cleanups,database,store,clock,advance:n=>{at+=n;},issuer,config,auth,superAdmin,departments,jobs,a,b,scopeA,scopeB,keyA};
}


const actor={session_ref:createHash('sha256').update('synthetic-extra-department').digest('hex')};
async function setup(t,{install=true}={}){
 const f=await fixture(t,{install}),reader=new PostgresStore({...options(f.database),poolSize:2});f.cleanups.push(()=>reader.close());const service=createPostgresAccountExtraUsage(f.store,{clock:f.clock}),batches=createPostgresExtraUsageBatches(f.store,{readStore:reader,service,jobs:f.jobs,clock:f.clock}),accounts=[];
 for(const department of install?[f.a,f.b]:[null]){const account=randomUUID();await f.store.run("INSERT INTO harbor.accounts(id,name,home,state"+(install?',department_id':'')+") VALUES($1,'合成权益账号','synthetic','ready'"+(install?',$2':'')+")",account,...(install?[department.id]:[]));accounts.push(account);}
 return {...f,reader,service,batches,accounts};
}
test('coverage/records/preview/list/detail隔离并持久原管理actor，Worker只改本部门',integration,async t=>{
 const f=await setup(t),scope=f.scopeA,manager=f.batches;
 assert.equal((await manager.coverage(undefined,{scope})).rows.length,1);assert.equal((await manager.coverage(undefined,{scope:f.scopeB})).rows[0].account_id,f.accounts[1]);await assert.rejects(manager.coverage(),{status:403});await assert.rejects(manager.records(f.accounts[1],undefined,{scope}),{status:404});
 const p=await manager.previewAccounts({account_ids:[f.accounts[0]],mode:'deny',expected_policy_version:0},actor,{scope});assert.equal((await manager.detail(p.id,{scope})).items.length,1);await assert.rejects(manager.detail(p.id,{scope:f.scopeB}),{status:404});assert.equal((await manager.list(undefined,{scope:f.scopeB})).rows.length,0);
 const row=await f.store.get('SELECT department_id,actor_kind,actor_id,actor_version FROM harbor.extra_usage_previews WHERE id=$1',p.id);assert.deepEqual(row,{department_id:f.a.id,actor_kind:scope.role,actor_id:scope.credential_id,actor_version:scope.credential_version});
 await assert.rejects(manager.submit(p.id,{session_ref:'0'.repeat(64)},{scope}),{code:'extra_preview_owner_mismatch'});const queued=await manager.submit(p.id,actor,{scope}),job=await f.jobs.claim(['account.extra.configure']);assert.equal(job.id,queued.job_id);const result=await manager.handlers[job.kind]({...job,department_id:f.b.id,actor_id:f.scopeB.credential_id});await f.jobs.complete(job,result);
 assert.equal((await f.service.readAccount(f.accounts[0],{scope})).configuration.mode,'deny');assert.equal((await f.service.readAccount(f.accounts[1],{scope:f.scopeB})).configuration.mode,'inherit');assert.equal((await manager.detail(p.id,{scope})).status,'completed');
});
test('策略预览按部门应用，原actor、会话与版本均保护',integration,async t=>{
 const f=await setup(t),scope=f.scopeA,p=await f.batches.previewPolicy({enabled:true,expected_version:0},actor,{scope});const otherKey=await f.departments.issueManagementKey(f.superAdmin,f.a.id,{label:'第二合成管理者'}),other=await f.departments.authenticateManagementKey(otherKey.secret);
 await assert.rejects(f.batches.policyPreviews.apply({preview_id:p.id},actor,{scope:other}),{code:'extra_preview_owner_mismatch'});await f.batches.policyPreviews.apply({preview_id:p.id},actor,{scope});assert.equal((await f.service.readPolicy({scope})).enabled,true);assert.equal((await f.service.readPolicy({scope:f.scopeB})).enabled,false);
 await assert.rejects(f.batches.previewAccounts({account_ids:[f.accounts[1]],mode:'deny',expected_policy_version:1},actor,{scope}),{code:'extra_preview_account_changed'});
});
test('撤销管理Key或停用部门后Worker拒绝写入，retry仅原actor',integration,async t=>{
 for(const disabled of [false,true]){const f=await setup(t),scope=f.scopeA,p=await f.batches.previewAccounts({account_ids:[f.accounts[0]],mode:'deny',expected_policy_version:0},actor,{scope});await f.batches.submit(p.id,actor,{scope});const job=await f.jobs.claim(['account.extra.configure']);if(disabled)await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});else await f.departments.revokeManagementKey(f.superAdmin,f.a.id,f.keyA.key.id,{expected_version:0});await assert.rejects(f.batches.handlers[job.kind](job),{status:403});assert.equal((await f.store.get('SELECT status FROM harbor.extra_usage_preview_items WHERE preview_id=$1',p.id)).status,'pending');}
});
test('旧无部门批量流程保持原session保护与执行',integration,async t=>{
 const f=await setup(t,{install:false}),p=await f.batches.previewAccounts({account_ids:f.accounts,mode:'deny',expected_policy_version:0},actor);await f.batches.submit(p.id,actor);const job=await f.jobs.claim(['account.extra.configure']);await f.jobs.complete(job,await f.batches.handlers[job.kind](job));assert.equal((await f.service.readAccount(f.accounts[0])).configuration.mode,'deny');
});
test('首项完成后撤销actor，下一项及failed/completed标记均停止写入',integration,async t=>{
 const f=await setup(t),scope=f.scopeA,second=randomUUID();await f.store.run("INSERT INTO harbor.accounts(id,name,home,state,department_id) VALUES($1,'合成第二账号','synthetic','ready',$2)",second,f.a.id);const p=await f.batches.previewAccounts({account_ids:[f.accounts[0],second],mode:'deny',expected_policy_version:0},actor,{scope});await f.batches.submit(p.id,actor,{scope});let revoked=false;
 const jobs={...f.jobs,owned:async(...args)=>{const result=await f.jobs.owned(...args);if(!revoked&&(await f.store.get("SELECT count(*)::int n FROM harbor.extra_usage_preview_items WHERE preview_id=$1 AND status='success'",p.id)).n===1){revoked=true;await f.departments.revokeManagementKey(f.superAdmin,f.a.id,f.keyA.key.id,{expected_version:0});}return result;}},worker=createPostgresExtraUsageBatches(f.store,{service:f.service,jobs,clock:f.clock}),job=await jobs.claim(['account.extra.configure']);await assert.rejects(worker.handlers[job.kind](job),{status:403});assert.deepEqual((await f.store.all('SELECT status FROM harbor.extra_usage_preview_items WHERE preview_id=$1 ORDER BY ordinal',p.id)).map(row=>row.status),['success','pending']);assert.equal((await f.store.get('SELECT status FROM harbor.extra_usage_previews WHERE id=$1',p.id)).status,'queued');
});
test('失败重试建立同部门子预览并重新快照版本',integration,async t=>{
 const f=await setup(t),scope=f.scopeA,p=await f.batches.previewAccounts({account_ids:[f.accounts[0]],mode:'deny',expected_policy_version:0},actor,{scope});await f.batches.submit(p.id,actor,{scope});await f.store.run('UPDATE harbor.accounts SET route_epoch=route_epoch+1 WHERE id=$1',f.accounts[0]);const job=await f.jobs.claim(['account.extra.configure']);await f.jobs.complete(job,await f.batches.handlers[job.kind](job));assert.equal((await f.batches.detail(p.id,{scope})).items[0].status,'skipped');await assert.rejects(f.batches.retryAccounts(p.id,actor,{scope:f.scopeB}),{status:404});const retry=await f.batches.retryAccounts(p.id,actor,{scope});assert.equal((await f.store.get('SELECT parent_id FROM harbor.extra_usage_previews WHERE id=$1',retry.id)).parent_id,p.id);await f.batches.submit(retry.id,actor,{scope});const next=await f.jobs.claim(['account.extra.configure']);await f.jobs.complete(next,await f.batches.handlers[next.kind](next));assert.equal((await f.batches.detail(retry.id,{scope})).items[0].status,'success');
});

test('真实HTTP批量预览与执行继承可信部门，读取队列不串部门',integration,async t=>{
 const f=await setup(t),keyB=await f.departments.issueManagementKey(f.superAdmin,f.b.id,{label:'HTTP合成B'}),bodies=createRequestBodyBudget();
 const invalid=()=>Object.assign(new Error('unauthorized'),{status:401});
 const admin=createPostgresExtraUsageAdmin({service:f.service,batches:f.batches,policyPreviews:f.batches.policyPreviews,adminAuthenticated:()=>false,adminSession:()=>actor,adminContext:async req=>{const scope=await f.departments.authenticateManagementKey(req.headers.authorization?.slice(7));if(!scope)throw invalid();return scope;},revalidateAdminScope:async(req,scope)=>{const valid=await f.departments.validateManagementSession(scope);if(!valid)throw invalid();return valid;},acceptsAdminRequest:()=>true,requestBodies:bodies,clock:f.clock});
 const server=http.createServer((req,res)=>{admin.handle(req,res).then(handled=>{if(!handled){res.statusCode=404;res.end();}}).catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code??'failure'}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));f.cleanups.unshift(async()=>{await admin.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));});const base='http://127.0.0.1:'+server.address().port;
 const request=async(path,key=f.keyA.secret,method='GET',body)=>{const response=await fetch(base+path,{method,headers:{Authorization:'Bearer '+key,...(body?{'Content-Type':'application/json'}:{})},...(body?{body:JSON.stringify(body)}:{})});return {status:response.status,body:await response.json()};};
 const cover=await Promise.all([request('/api/admin/extra-usage/coverage'),request('/api/admin/extra-usage/coverage',keyB.secret)]);assert.equal(cover[0].body.rows[0].account_id,f.accounts[0]);assert.equal(cover[1].body.rows[0].account_id,f.accounts[1]);
 const preview=await request('/api/admin/extra-usage/batches/preview',f.keyA.secret,'POST',{account_ids:[f.accounts[0]],mode:'deny',expected_policy_version:0});assert.equal(preview.status,201);const path='/api/admin/extra-usage/batches/'+preview.body.id;assert.equal((await request(path,keyB.secret)).status,404);assert.equal((await request(path+'/execute',keyB.secret,'POST',{})).status,404);assert.equal((await request(path+'/execute',f.keyA.secret,'POST',{})).status,202);
 const job=await f.jobs.claim(['account.extra.configure']);await f.jobs.complete(job,await f.batches.handlers[job.kind](job));assert.equal((await request(path)).body.status,'completed');assert.equal(bodies.status().leases,0);
});
