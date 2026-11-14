import {createPostgresPlanningJobs,departmentPlanningJobId} from '../postgres-planning-jobs.mjs';
import {createPostgresPlanningInput} from '../postgres-planning-input.mjs';
import {createPostgresRoutingMaintenance} from '../postgres-routing-maintenance.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
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
 const database='harbor_dj_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});await root.query('CREATE DATABASE '+database);const store=new PostgresStore({...options(database),poolSize:4});t.after(async()=>{await store.close();await root.query('DROP DATABASE '+database);await root.close();});await store.migrate();if(install)await migrateDepartmentIsolation(store,{apply:true});
 let at=Date.parse('2026-11-13T18:05:00Z');const clock=()=>at,issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')},auth=createAdminAuth(config,{clock,scopeIssuer:issuer}),res={headers:{},setHeader(k,v){this.headers[k]=v;}};auth.login('synthetic-root',res);const superAdmin=await auth.context({headers:{cookie:res.headers['Set-Cookie'].split(';')[0]}}),departments=createPostgresDepartments(store,{issuer,clock,authorizeSuperAdmin:auth.authorizeSuperAdmin});
 let a,b,scopeA,scopeB,keyA;if(install){a=await departments.createDepartment(superAdmin,{name:'合成A'});b=await departments.createDepartment(superAdmin,{name:'合成B'});keyA=await departments.issueManagementKey(superAdmin,a.id,{label:'A负责人'});const keyB=await departments.issueManagementKey(superAdmin,b.id,{label:'B负责人'});scopeA=await departments.authenticateManagementKey(keyA.secret);scopeB=await departments.authenticateManagementKey(keyB.secret);}
 await store.initializePricing();const jobs=createPostgresJobs(store,{clock,departmentActors:departments,scopedKinds:['routing.preview','routing.expiry','routing.nightly'],leaseMs:30000});return {database,store,clock,advance:n=>{at+=n;},issuer,config,auth,superAdmin,departments,jobs,a,b,scopeA,scopeB,keyA};
}

test('预览完整写入部门计划，跨部门及缺失scope不能查看',integration,async t=>{
 const f=await fixture(t),planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock});
 const queued=await planning.enqueue({scope:f.scopeA}),job=await f.jobs.claim(['routing.preview']);const result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);
 assert.equal(result.plan_id,queued.id);assert.equal((await f.store.get('SELECT department_id FROM harbor.routing_plans WHERE id=$1',queued.id)).department_id,f.a.id);
 assert.equal((await planning.status(queued.id,{scope:f.scopeA})).ready,true);assert.equal((await planning.page(queued.id,{scope:f.scopeA})).items.length,0);
 await assert.rejects(planning.status(queued.id,{scope:f.scopeB}),{code:'planning_not_found'});await assert.rejects(planning.enqueue(),{code:'department_scope_required'});
});
test('同日周期恢复按部门入队，恢复权重只改本部门',integration,async t=>{
 const f=await fixture(t),router=createPostgresRouter(f.store,{clock:f.clock}),maintenance=createPostgresRoutingMaintenance(f.store,{jobs:f.jobs,router});
 for(const department of [f.a,f.b])await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='routing_weights'",JSON.stringify({mode:'manual',weights:{},expires:f.clock()-3600000,version:1}),department.id);
 const scope=f.issuer.system({purpose:'routing.expiry',departmentId:f.a.id}),other=f.issuer.system({purpose:'routing.expiry',departmentId:f.b.id});
 assert.equal(await maintenance.schedule({scope}),null);assert.equal(await maintenance.schedule({scope:other}),null);
 await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='balancing'",JSON.stringify({nightly:true}),f.a.id);assert.equal(await maintenance.schedule({scope:other}),null,'other disabled department stays disabled');
 await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='balancing'",JSON.stringify({nightly:true}),f.b.id);
 const a=await maintenance.schedule({scope}),b=await maintenance.schedule({scope:other});assert.notEqual(a.id,b.id);
 const job=await f.jobs.claim(['routing.expiry']);await maintenance.handlers[job.kind](job);
 const values=await f.store.all("SELECT department_id,value FROM harbor.department_settings WHERE name='routing_weights' AND department_id=ANY($1::text[])",[f.a.id,f.b.id]);
 assert.equal(values.find(row=>row.department_id===job.department_id).value.mode,'auto');assert.equal(values.find(row=>row.department_id!==job.department_id).value.mode,'manual');
});
test('夜间同slot任务及依赖独立，错误expiryScope不能借用',integration,async t=>{
 const f=await fixture(t),planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock});
 for(const department of [f.a,f.b])await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='balancing'",JSON.stringify({nightly:true}),department.id);
 const input=department=>({scope:f.issuer.system({purpose:'routing.nightly',departmentId:department.id}),expiryScope:f.issuer.system({purpose:'routing.expiry',departmentId:department.id})});
 const a=await planning.schedule(input(f.a)),b=await planning.schedule(input(f.b));assert.ok(a&&b);assert.notEqual(a.id,b.id);
 const rows=await f.store.all("SELECT department_id,dependency_id FROM harbor.background_jobs WHERE kind='routing.nightly'");assert.equal(rows.length,2);assert.notEqual(rows[0].dependency_id,rows[1].dependency_id);
 await assert.rejects(planning.schedule({...input(f.a),expiryScope:input(f.b).expiryScope}),{code:'department_forbidden'});
});
test('领取后管理员撤销不能产出计划，legacy预览不变',integration,async t=>{
 const f=await fixture(t),planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock});await planning.enqueue({scope:f.scopeA});const job=await f.jobs.claim(['routing.preview']);await f.departments.revokeManagementKey(f.superAdmin,f.a.id,f.keyA.key.id,{expected_version:0});await assert.rejects(planning.handlers[job.kind](job),{code:'department_forbidden'});assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.routing_plans')).n,0);
 const legacy=await fixture(t,{install:false}),old=createPostgresPlanningJobs(legacy.store,{jobs:legacy.jobs,clock:legacy.clock});await old.enqueue();const legacyJob=await legacy.jobs.claim(['routing.preview']);assert.ok((await old.handlers[legacyJob.kind](legacyJob)).plan_id);
});
test('夜间依赖完成后真实生成计划，只读取本部门账号与设置',integration,async t=>{
 const f=await fixture(t),planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock}),reader=createPostgresPlanningInput(f.store,{clock:f.clock}),router=createPostgresRouter(f.store,{clock:f.clock}),maintenance=createPostgresRoutingMaintenance(f.store,{jobs:f.jobs,router});
 for(const department of [f.a,f.b])await f.store.run("INSERT INTO harbor.accounts(id,name,home,state,department_id) VALUES($1,'合成账号','synthetic','ready',$2)",randomUUID(),department.id);
 await f.store.run("UPDATE harbor.accounts SET quota_json=$1 WHERE department_id=ANY($2::text[])",JSON.stringify({checked:f.clock(),remaining:50,plan:'pro',windows:[{used:50,minutes:10080,reset:f.clock()+86400000}]}),[f.a.id,f.b.id]);
 const snapshot=await reader.read({scope:f.scopeA});assert.equal(snapshot.accounts.length,1);assert.equal(snapshot.accounts[0].department_id,f.a.id);
 await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='balancing'",JSON.stringify({nightly:true}),f.a.id);
 await f.store.run("UPDATE harbor.department_settings SET value='{\"weighted\":true}'::jsonb WHERE department_id=$1 AND name='routing_strategy'",f.a.id);
 const scope=f.issuer.system({purpose:'routing.nightly',departmentId:f.a.id}),expiryScope=f.issuer.system({purpose:'routing.expiry',departmentId:f.a.id});await planning.schedule({scope,expiryScope});
 const expiry=await f.jobs.claim(['routing.expiry']);await f.jobs.complete(expiry,await maintenance.handlers[expiry.kind](expiry));const job=await f.jobs.claim(['routing.nightly']);assert.ok(job);
 await f.store.run('INSERT INTO harbor.nightly_cycle_ready(day,at,department_id) VALUES($1,$2,$3)',job.target,f.clock(),f.a.id);
 const result=await planning.handlers[job.kind](job);const plan=await f.store.get('SELECT department_id,header FROM harbor.routing_plans WHERE id=$1',result.plan_id);assert.equal(plan.department_id,f.a.id);assert.equal(plan.header.configuration.balancing.nightly,true);
 assert.equal((await f.store.get("SELECT count(*)::int n FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='accounts'",result.plan_id)).n,1);
 const audit=await f.store.all("SELECT s.department_id FROM harbor.audit a JOIN harbor.audit_scopes s ON s.audit_id=a.id WHERE a.action='balancing.weights.auto'");assert.deepEqual(audit.map(row=>row.department_id),[f.a.id]);assert.equal((await f.store.get("SELECT status FROM harbor.background_jobs WHERE id=$1",expiry.id)).status,'completed');
});
test('规划请求估计样本和活跃会话不跨部门',integration,async t=>{
 const f=await fixture(t),ids=[];
 for(const department of [f.a,f.b]){
  const member=randomUUID(),key=randomUUID(),account=randomUUID(),session=randomUUID();ids.push(session);
  await f.store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES($1,'合成员工',$2,$3)",member,f.clock()-1000,department.id);
  await f.store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created,department_id) VALUES($1,$2,$1,'synthetic',$3,$4)",key,member,f.clock()-1000,department.id);
  await f.store.run("INSERT INTO harbor.accounts(id,name,home,state,department_id) VALUES($1,'合成账号','synthetic','ready',$2)",account,department.id);
  await f.store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated,department_id) VALUES($1,$2,$3,'responses',$4,$4,$5)",session,member,account,f.clock()-1000,department.id);
  await f.store.run("INSERT INTO harbor.requests(id,member_id,key_id,session_id,account_id,route_version,started,status,ip,model,handoff_context,handoff_input,source_channel,department_id) VALUES($1,$2,$3,$4,$5,0,$6,'running','synthetic','gpt-6.1-sol','replayable','replayable','responses',$7)",randomUUID(),member,key,session,account,f.clock()-500,department.id);
 }
 const reader=createPostgresPlanningInput(f.store,{clock:f.clock});assert.deepEqual((await reader.read({scope:f.scopeA,requestBoundary:true})).workloads.map(row=>row.id),[ids[0]]);assert.deepEqual((await reader.read({scope:f.scopeB,requestBoundary:true})).workloads.map(row=>row.id),[ids[1]]);
});
