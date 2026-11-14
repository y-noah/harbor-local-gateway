import {createPostgresPlanningJobs as createPriorPlanning} from './fixtures/prior-daily-planning.mjs';
import {createPostgresNightlyRevisions as createPriorRevisions} from './fixtures/prior-daily-revisions.mjs';
import {transferWindow} from '../transfer-schedule.mjs';
import {scheduledRecoveryId} from '../scheduled-task-identity.mjs';
import {createPostgresNightlyRevisions} from '../postgres-nightly-revisions.mjs';
import {createPostgresNightly} from '../postgres-nightly.mjs';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';
import {createPostgresSessionHandoffs} from '../postgres-session-handoffs.mjs';
import {createRoutingPreparation} from '../routing-preparation.mjs';
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


async function runtime(t,f,{modern=true}={}){
 const departmentScopes=async()=>[f.a,f.b].filter(Boolean).map(d=>f.issuer.system({purpose:'routing.nightly',departmentId:d.id})),refreshed=[],gate=createSessionRequestHandoff(),preparation=createRoutingPreparation(),handoffAdmin=modern?createPostgresSessionHandoffs(f.store,{gate,clock:f.clock,departmentActors:f.departments,validateTarget:async()=>true}):undefined;
 const nightly=createPostgresNightly(f.store,{clock:f.clock,preparation,sessionHandoffs:gate,handoffAdmin,departmentActors:f.departments,departmentScopes,refreshQuotas:async({scope}={})=>{refreshed.push(scope?.department_id??null);}});t.after(async()=>{await nightly.close();await handoffAdmin?.close();});
 return {nightly,refreshed,departmentScopes,gate};
}
async function prepare(f,department,{modern=true}={}){
 const scope=department?f.issuer.system({purpose:'routing.nightly',departmentId:department.id}):undefined,expiryScope=department?f.issuer.system({purpose:'routing.expiry',departmentId:department.id}):undefined;
 if(department){await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='balancing'",JSON.stringify({nightly:true}),department.id);await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='request_handoff_policy'",JSON.stringify({mode:modern?'request-boundary':'quiet',version:1}),department.id);}
 else {await f.store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('balancing',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",JSON.stringify({nightly:true}));await f.store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='request_handoff_policy'",JSON.stringify({mode:modern?'request-boundary':'quiet',version:1}));}
 const planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock}),maintenance=createPostgresRoutingMaintenance(f.store,{jobs:f.jobs,router:createPostgresRouter(f.store,{clock:f.clock})});await planning.schedule({scope,expiryScope});const expiry=await f.jobs.claim(['routing.expiry']);await f.jobs.complete(expiry,await maintenance.handlers[expiry.kind](expiry));return {scope,planning};
}
test('两个部门同slot额度刷新、计划执行和结果独立，重启不串汇总',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f),plans=[];for(const d of [f.a,f.b])await prepare(f,d);
 await r.nightly.runOnce();assert.equal(r.nightly.status().lastError,null);assert.deepEqual(new Set(r.refreshed),new Set([f.a.id,f.b.id]));assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.nightly_cycle_ready')).n,2);
 const planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock});for(let i=0;i<2;i++){const job=await f.jobs.claim(['routing.nightly']);const result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);plans.push(result.plan_id);}
 await Promise.all(plans.map(id=>r.nightly.apply(id)));const rows=await f.store.all('SELECT department_id,status FROM harbor.balance_runs');assert.equal(rows.length,2);assert.ok(rows.every(row=>row.status==='completed'));assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.nightly_plan_applications')).n,2);
 await r.nightly.recover();assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.balance_runs')).n,2);
});
test('停用部门拒绝apply且不阻断另一部门，非boundary复合day路径可完成',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f,{modern:false}),plans=[];for(const d of [f.a,f.b])await prepare(f,d,{modern:false});await r.nightly.runOnce();
 const planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock});for(let i=0;i<2;i++){const job=await f.jobs.claim(['routing.nightly']);const result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);plans.push({id:result.plan_id,department:job.department_id});}
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await assert.rejects(r.nightly.apply(plans.find(p=>p.department===f.a.id).id),{status:403});await r.nightly.apply(plans.find(p=>p.department===f.b.id).id);assert.equal((await f.store.get('SELECT status FROM harbor.balance_runs WHERE department_id=$1',f.b.id)).status,'completed');await r.nightly.runOnce();assert.equal(r.nightly.status().lastError,null);
});
async function workload(f,department){
 const member=randomUUID(),key=randomUUID(),accounts=[randomUUID(),randomUUID()],sessions=[];
 await f.store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES($1,'合成夜间员工',$2,$3)",member,f.clock()-7200000,department.id);
 await f.store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created,department_id) VALUES($1,$2,$1,'synthetic',$3,$4)",key,member,f.clock()-7200000,department.id);
 const quota=JSON.stringify({plan:'pro',checked:f.clock(),remaining:50,limitReached:false,ordinaryUsageAllowed:true,windows:[{used:50,minutes:10080,reset:f.clock()+86400000}]});
 for(const account of accounts)await f.store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json,department_id) VALUES($1,'合成夜间账号','synthetic','ready',$2,$3)",account,quota,department.id);
 const revision=JSON.parse((await f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value).active;
 for(let i=0;i<6;i++){
  const session=randomUUID(),request=randomUUID();sessions.push(session);await f.store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated,department_id) VALUES($1,$2,$3,'responses',$4,$4,$5)",session,member,accounts[0],f.clock()-7200000,department.id);
  await f.store.run("INSERT INTO harbor.requests(id,member_id,key_id,session_id,account_id,route_version,started,ended,status,ip,model,input,price_version,handoff_context,handoff_input,source_channel,department_id) VALUES($1,$2,$3,$4,$5,0,$6,$7,'ok','synthetic','gpt-6.1-sol',1000,$8,'replayable','replayable','responses',$9)",request,member,key,session,accounts[0],f.clock()-7200000,f.clock()-3600000,revision,department.id);await f.store.projectPrice(await f.store.get('SELECT * FROM harbor.requests WHERE id=$1',request),f.clock());
 }
 return {member,key,accounts,sessions};
}
test('真实非boundary整组迁移只写原部门，另一部门会话与结果不变',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f,{modern:false}),a=await workload(f,f.a),b=await workload(f,f.b),{planning}=await prepare(f,f.a,{modern:false});await r.nightly.runOnce();const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);
 const plan=await f.store.get('SELECT header FROM harbor.routing_plans WHERE id=$1',result.plan_id);assert.ok(plan.header.counts.moves>0);await r.nightly.apply(result.plan_id);
 const moved=await f.store.all('SELECT id FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.a.id,a.accounts[1]);assert.equal(moved.length,plan.header.counts.moves);assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.b.id,b.accounts[0])).n,6);
 assert.equal((await f.store.get("SELECT status FROM harbor.balance_runs WHERE department_id=$1",f.a.id)).status,'completed');assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.nightly_group_results WHERE department_id=$1',f.b.id)).n,0);
});
test('未安装部门隔离时保留原slot和执行结果',integration,async t=>{
 const f=await fixture(t,{install:false}),r=await runtime(t,f,{modern:false}),{planning}=await prepare(f,undefined,{modern:false});await r.nightly.runOnce();assert.deepEqual(r.refreshed,[null]);const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);await r.nightly.apply(result.plan_id);assert.equal((await f.store.get('SELECT status FROM harbor.balance_runs')).status,'completed');
});

test('现代请求边界实际整组迁移，原部门审计和handoff持久身份一致',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f),a=await workload(f,f.a),b=await workload(f,f.b),{planning}=await prepare(f,f.a);await r.nightly.runOnce();const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);const plan=await f.store.get('SELECT header FROM harbor.routing_plans WHERE id=$1',result.plan_id);assert.ok(plan.header.counts.moves>0);
 await r.nightly.apply(result.plan_id);const moved=await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.a.id,a.accounts[1]);assert.equal(moved.n,plan.header.counts.moves);assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.b.id,b.accounts[0])).n,6);
 const handoffs=await f.store.all('SELECT department_id,actor_id,status FROM harbor.session_handoffs');assert.equal(handoffs.length,plan.header.counts.moves);assert.ok(handoffs.every(h=>h.department_id===f.a.id&&h.actor_id==='system:routing.nightly'&&h.status==='completed'));assert.equal((await f.store.get('SELECT status FROM harbor.balance_runs WHERE department_id=$1',f.a.id)).status,'completed');
});
async function waitFor(predicate){const end=Date.now()+5000;while(!await predicate()){assert.ok(Date.now()<end,'bounded nightly condition');await new Promise(resolve=>setTimeout(resolve,5));}}
for(const invalidate of [false,true])test('现代整组在途保护'+(invalidate?'与条件变化全组不写入':'等待完成后提交'),integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f),a=await workload(f,f.a),{planning}=await prepare(f,f.a);await r.nightly.runOnce();const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);
 const move=(await f.store.get("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' ORDER BY ordinal LIMIT 1",result.plan_id)).payload;
 const ticket=r.gate.arrive(a.member);await ticket.bind(move.session_id);t.after(()=>ticket.close());const applying=r.nightly.apply(result.plan_id);applying.catch(()=>{});
 await waitFor(async()=>!!await f.store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE plan_id=$1 AND status='waiting'",result.plan_id));assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.a.id,a.accounts[0])).n,6);
 if(invalidate)await f.store.run('UPDATE harbor.accounts SET route_epoch=route_epoch+1 WHERE id=$1',move.to);
 ticket.close();await applying;
 const moved=(await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.a.id,a.accounts[1])).n;
 if(invalidate){assert.equal(moved,0);assert.equal((await f.store.get("SELECT count(*)::int n FROM harbor.session_handoffs WHERE status='completed'")).n,0);}else assert.ok(moved>0);
});
test('部门停用收尾在途自动组，不迁移且不阻塞其他部门循环',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f),a=await workload(f,f.a),{planning}=await prepare(f,f.a);await r.nightly.runOnce();const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);
 const move=(await f.store.get("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' ORDER BY ordinal LIMIT 1",result.plan_id)).payload,ticket=r.gate.arrive(a.member);await ticket.bind(move.session_id);t.after(()=>ticket.close());const applying=r.nightly.apply(result.plan_id);applying.catch(()=>{});
 await waitFor(async()=>!!await f.store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE plan_id=$1 AND status='waiting'",result.plan_id));await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await r.nightly.runOnce();ticket.close();await applying.catch(()=>{});
 assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.a.id,a.accounts[0])).n,6);assert.equal((await f.store.get("SELECT count(*)::int n FROM harbor.automatic_handoff_groups WHERE status='waiting'")).n,0);assert.equal((await f.store.get("SELECT count(*)::int n FROM harbor.session_handoffs WHERE status='waiting'")).n,0);assert.equal(r.nightly.status().lastError,null);assert.equal((await f.store.get('SELECT status FROM harbor.nightly_plan_applications WHERE plan_id=$1',result.plan_id)).status,'interrupted');assert.equal((await f.store.get('SELECT status FROM harbor.balance_runs WHERE plan_id=$1',result.plan_id)).status,'interrupted');
});

test('非boundary部门在途等待不阻塞另一部门刷新和应用',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f,{modern:false}),a=await workload(f,f.a);await prepare(f,f.a,{modern:false});await prepare(f,f.b,{modern:false});await r.nightly.runOnce();const planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock}),plans=[];
 for(let i=0;i<2;i++){const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);plans.push({id:result.plan_id,department:job.department_id});}
 const planA=plans.find(p=>p.department===f.a.id),planB=plans.find(p=>p.department===f.b.id),move=(await f.store.get("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' ORDER BY ordinal LIMIT 1",planA.id)).payload,ticket=r.gate.arrive(a.member);await ticket.bind(move.session_id);t.after(()=>ticket.close());
 await r.nightly.runOnce();await waitFor(async()=>(await f.store.get('SELECT status FROM harbor.balance_runs WHERE plan_id=$1',planB.id))?.status==='completed');assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.a.id,a.accounts[0])).n,6);assert.ok(r.nightly.status().running);ticket.close();await r.nightly.apply(planA.id);
});
test('非boundary在途部门停用后批次终态且重启恢复不复活',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f,{modern:false}),a=await workload(f,f.a),{planning}=await prepare(f,f.a,{modern:false});await r.nightly.runOnce();const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);const move=(await f.store.get("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' ORDER BY ordinal LIMIT 1",result.plan_id)).payload,ticket=r.gate.arrive(a.member);await ticket.bind(move.session_id);t.after(()=>ticket.close());const applying=r.nightly.apply(result.plan_id);applying.catch(()=>{});
 await waitFor(async()=>!!await f.store.get("SELECT plan_id FROM harbor.balance_runs WHERE plan_id=$1 AND status='running'",result.plan_id));await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await r.nightly.runOnce();ticket.close();await applying.catch(()=>{});assert.equal((await f.store.get('SELECT status FROM harbor.balance_runs WHERE plan_id=$1',result.plan_id)).status,'interrupted');await r.nightly.recover();assert.equal((await f.store.get('SELECT status FROM harbor.balance_runs WHERE plan_id=$1',result.plan_id)).status,'interrupted');assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.sessions WHERE department_id=$1 AND account_id=$2',f.a.id,a.accounts[0])).n,6);
});

for(const completedGroup of [false,true])test('现代部门停用恢复'+(completedGroup?'最后组已完成但application未收口':'ensure后尚无自动组')+'的批次终态',integration,async t=>{
 const f=await fixture(t),r=await runtime(t,f),{planning,scope}=await prepare(f,f.a);await r.nightly.runOnce();const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);const plan=await f.store.get('SELECT p.*,j.target FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id WHERE p.id=$1',result.plan_id),revisions=createPostgresNightlyRevisions(f.store,{clock:f.clock,departmentActors:f.departments});await revisions.ensure(plan,{scope});
 if(completedGroup){await f.store.run("INSERT INTO harbor.routing_plan_items(plan_id,section,ordinal,payload,bytes) VALUES($1,'groups',1,'{}',2)",plan.id);await f.store.run("INSERT INTO harbor.automatic_handoff_groups(id,plan_id,slot,group_ordinal,requested,expires,status,finished,department_id) VALUES($1,$2,$3,1,$4,$5,'completed',$4,$6)",randomUUID(),plan.id,plan.target,f.clock(),f.clock()+60000,f.a.id);}
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await r.nightly.recover();assert.equal((await f.store.get('SELECT status FROM harbor.nightly_plan_applications WHERE plan_id=$1',plan.id)).status,'interrupted');assert.equal((await f.store.get('SELECT status FROM harbor.balance_runs WHERE plan_id=$1',plan.id)).status,'interrupted');await r.nightly.runOnce();assert.equal(r.nightly.status().lastError,null);
});

for(const install of [false,true])for(const revised of [false,true])test('旧基线10点任务升级恢复：部门 '+install+' 已有修订 '+revised,integration,async t=>{
 const f=await fixture(t,{install});f.advance(8*3600000);const department=install?f.a:null,scope=department?f.issuer.system({purpose:'routing.nightly',departmentId:department.id}):undefined,expiryScope=department?f.issuer.system({purpose:'routing.expiry',departmentId:department.id}):undefined,input={scope,expiryScope},slot=transferWindow(f.clock());
 const setting=async(name,value)=>{if(department)await f.store.run('UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name=$3',JSON.stringify(value),department.id,name);else await f.store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',name,JSON.stringify(value));};
 await setting('balancing',{nightly:true});await setting('request_handoff_policy',{mode:revised?'request-boundary':'quiet',version:1});
 const prior=createPriorPlanning(f.store,{jobs:f.jobs,clock:f.clock}),old=await prior.schedule(input);let oldExpiry=await f.jobs.claim(['routing.expiry']);assert.equal(oldExpiry.payload.boundary,slot.end-8*3600000);await f.jobs.complete(oldExpiry,{sessionsRestored:0,weightsExpired:false});
 await f.store.run('INSERT INTO harbor.nightly_cycle_ready(day,at'+(department?',department_id':'')+') VALUES($1,$2'+(department?',$3':'')+')',slot.key,f.clock(),...(department?[department.id]:[]));
 if(revised){
  const job=await f.jobs.claim(['routing.nightly']),result=await prior.handlers[job.kind](job);await f.jobs.complete(job,result);
  const parent={...await f.store.get('SELECT * FROM harbor.routing_plans WHERE id=$1',job.id),target:slot.key},oldRevisions=createPriorRevisions(f.store,{clock:f.clock,departmentActors:f.departments});await oldRevisions.ensure(parent,{scope});await oldRevisions.request(parent,'gateway_restart',{scope});await prior.schedule(input);
 }
 const pending=await f.store.get("SELECT * FROM harbor.background_jobs WHERE kind='routing.nightly' AND status='pending' ORDER BY id DESC LIMIT 1"),oldRevision=await f.store.all('SELECT * FROM harbor.nightly_plan_revisions ORDER BY revision');assert.ok(pending);assert.equal(pending.dependency_id,oldExpiry.id);
 const planner=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock}),maintenance=createPostgresRoutingMaintenance(f.store,{jobs:f.jobs,router:createPostgresRouter(f.store,{clock:f.clock})}),replacement=await planner.schedule(input);assert.equal(replacement.id,scheduledRecoveryId(pending.id));assert.equal((await planner.schedule(input)).id,replacement.id);
 const preserved=await f.store.get('SELECT * FROM harbor.background_jobs WHERE id=$1',pending.id);assert.equal(preserved.status,'cancelled');assert.equal(preserved.result,null);assert.equal(preserved.dependency_id,pending.dependency_id);assert.deepEqual(preserved.payload,pending.payload);assert.deepEqual(await f.store.all('SELECT * FROM harbor.nightly_plan_revisions ORDER BY revision'),oldRevision);
 assert.equal(await f.jobs.claim(['routing.nightly']),null,'old 02 completion must not release the replacement');const expiry=await f.jobs.claim(['routing.expiry']);assert.equal(expiry.payload.boundary,slot.end);await f.jobs.complete(expiry,await maintenance.handlers[expiry.kind](expiry));
 const job=await f.jobs.claim(['routing.nightly']);assert.equal(job.id,replacement.id);const result=await planner.handlers[job.kind](job);await f.jobs.complete(job,result);const r=await runtime(t,f,{modern:revised});await r.nightly.apply(job.id);
 const run=await f.store.get('SELECT plan_id,status FROM harbor.balance_runs WHERE day=$1'+(department?' AND department_id=$2':''),slot.key,...(department?[department.id]:[]));assert.equal(run.plan_id,job.id);assert.equal(run.status,'completed');
 assert.equal((await planner.schedule(input)).id,job.id);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,revised?2:1,'completed effects are not recreated');
 if(revised){const revisions=createPostgresNightlyRevisions(f.store,{clock:f.clock,departmentActors:f.departments}),summary=await revisions.summary(slot.key,{scope});assert.equal(summary.revisions.at(-1).revision,1);assert.equal(summary.revisions.at(-1).plan_id,job.id);}
});

test('部门调度保存与人工交接双连接交错遵循同一锁序',integration,async t=>{
 const f=await fixture(t),a=await workload(f,f.a),sessionId=a.sessions[0],gate=createSessionRequestHandoff();
 const handoffs=createPostgresSessionHandoffs(f.store,{gate,clock:f.clock,departmentActors:f.departments,validateTarget:async()=>true});
 const ticket=gate.arrive(a.member);await ticket.bind(sessionId);
 await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='balancing'",JSON.stringify({nightly:true}),f.a.id);
 await f.store.run('UPDATE harbor.sessions SET nightly_enabled=0,nightly_expires=$1 WHERE id=$2',f.clock()+3600000,sessionId);
 const accepted=await handoffs.handle('/api/admin/sessions/'+sessionId+'/handoffs','POST',{account_id:a.accounts[1],expected_route_version:0},{scope:f.scopeA});
 assert.equal(accepted.status,202);
 const query=f.store.query.bind(f.store);let rebaseHeld=false,configPid,handoffPid,release;
 const barrier=new Promise(resolve=>{release=resolve;});let saving;
 f.store.query=async(sql,args=[])=>{
  const pid=f.store.context.getStore()?.client.processID;
  if(sql.startsWith('UPDATE harbor.session_handoffs h SET source_nightly_expires=')){configPid=pid;rebaseHeld=true;await barrier;}
  if(rebaseHeld&&sql.includes("pg_advisory_xact_lock(hashtextextended('harbor-routing-state'"))handoffPid=pid;
  return query(sql,args);
 };
 try{
  saving=createPostgresRouter(f.store,{clock:f.clock}).saveSettings({nightly:false},{scope:f.scopeA});saving.catch(()=>{});
  await waitFor(()=>rebaseHeld);ticket.close();await waitFor(()=>!!handoffPid);
  assert.notEqual(configPid,handoffPid,'configuration and handoff must own distinct real PG connections');
  await waitFor(async()=>!!await f.store.get("SELECT pid FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock'",handoffPid));
  release();await saving;
  await waitFor(async()=> (await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',accepted.value.handoff_id)).status!=='waiting');
  const decision=await f.store.get('SELECT status,reason,source_nightly_expires,pin_expires FROM harbor.session_handoffs WHERE id=$1',accepted.value.handoff_id);
  assert.deepEqual(decision,{status:'completed',reason:null,source_nightly_expires:null,pin_expires:null});
  assert.deepEqual(await f.store.get('SELECT account_id,route_version,nightly_expires FROM harbor.sessions WHERE id=$1',sessionId),{account_id:a.accounts[1],route_version:1,nightly_expires:null});
  assert.equal(handoffs.status().lastError,null);
 }finally{release();ticket.close();await saving?.catch(()=>{});await handoffs.close();f.store.query=query;}
});
