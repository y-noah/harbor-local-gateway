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
 const denied=new Set();const departmentScopes=async()=>[f.a,f.b].filter(Boolean).map(d=>f.issuer.system({purpose:'routing.nightly',departmentId:d.id})),refreshed=[],gate=createSessionRequestHandoff(),preparation=createRoutingPreparation(),handoffAdmin=modern?createPostgresSessionHandoffs(f.store,{gate,clock:f.clock,departmentActors:f.departments,validateTarget:async target=>!denied.has(target.id)}):undefined;
 const nightly=createPostgresNightly(f.store,{clock:f.clock,preparation,sessionHandoffs:gate,handoffAdmin,departmentActors:f.departments,departmentScopes,refreshQuotas:async({scope}={})=>{refreshed.push(scope?.department_id??null);}});t.after(async()=>{await nightly.close();await handoffAdmin?.close();});
 return {nightly,refreshed,departmentScopes,gate,handoffAdmin,denied};
}
async function prepare(f,department,{modern=true}={}){
 const scope=department?f.issuer.system({purpose:'routing.nightly',departmentId:department.id}):undefined,expiryScope=department?f.issuer.system({purpose:'routing.expiry',departmentId:department.id}):undefined;
 if(department){await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='balancing'",JSON.stringify({nightly:true}),department.id);await f.store.run("UPDATE harbor.department_settings SET value=$1::jsonb WHERE department_id=$2 AND name='request_handoff_policy'",JSON.stringify({mode:modern?'request-boundary':'quiet',version:1}),department.id);}
 else {await f.store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('balancing',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",JSON.stringify({nightly:true}));await f.store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='request_handoff_policy'",JSON.stringify({mode:modern?'request-boundary':'quiet',version:1}));}
 const planning=createPostgresPlanningJobs(f.store,{jobs:f.jobs,clock:f.clock}),maintenance=createPostgresRoutingMaintenance(f.store,{jobs:f.jobs,router:createPostgresRouter(f.store,{clock:f.clock})});await planning.schedule({scope,expiryScope});const expiry=await f.jobs.claim(['routing.expiry']);await f.jobs.complete(expiry,await maintenance.handlers[expiry.kind](expiry));return {scope,planning};
}
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
async function waitFor(predicate){const end=Date.now()+5000;while(!await predicate()){assert.ok(Date.now()<end,'bounded nightly condition');await new Promise(resolve=>setTimeout(resolve,5));}}

async function pending(t){
 const f=await fixture(t),r=await runtime(t,f),a=await workload(f,f.a),{planning}=await prepare(f,f.a);await r.nightly.runOnce();const job=await f.jobs.claim(['routing.nightly']),result=await planning.handlers[job.kind](job);await f.jobs.complete(job,result);const move=(await f.store.get("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' ORDER BY ordinal LIMIT 1",result.plan_id)).payload;
 const ticket=r.gate.arrive(a.member);await ticket.bind(move.session_id);t.after(()=>ticket.close());const applying=r.nightly.apply(result.plan_id);applying.catch(()=>{});await waitFor(async()=>!!await f.store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE plan_id=$1 AND status='waiting'",result.plan_id));
 const original=await f.store.get("SELECT id,automatic_group_id FROM harbor.session_handoffs WHERE session_id=$1 AND origin='automatic'",move.session_id),target=randomUUID();await f.store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,department_id) VALUES($1,'合成人工目标','synthetic','ready','openai_api',$2)",target,f.a.id);
 const path='/api/admin/sessions/'+move.session_id+'/handoffs',request=(accountId=target,scope=f.scopeA)=>r.handoffAdmin.handle(path,'POST',{account_id:accountId,expected_route_version:0},{scope});
 return {...f,...r,workload:a,move,original,target,ticket,applying,path,request};
}
for(const mode of ['cancel','validation','revoked'])test('部门人工覆盖自动组后'+mode+'恢复真实原组',integration,async t=>{
 const f=await pending(t),manual=await f.request();assert.equal(manual.status,202);assert.equal((await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',f.original.id)).status,'superseded');
 if(mode==='cancel')await f.handoffAdmin.handle(f.path+'/'+manual.value.handoff_id,'DELETE',{},{scope:f.scopeA});
 if(mode==='validation')f.denied.add(f.target);
 if(mode==='revoked')await f.departments.revokeManagementKey(f.superAdmin,f.a.id,f.keyA.key.id,{expected_version:0});
 f.ticket.close();await f.applying;await waitFor(async()=>(await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',manual.value.handoff_id)).status!=='waiting');
 assert.equal((await f.store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',f.move.session_id)).account_id,f.move.to);assert.equal((await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',f.original.id)).status,'completed');
});
test('登记失败不破坏已接受自动组，跨部门目标和替换拒绝',integration,async t=>{
 const f=await pending(t),foreign=randomUUID();await f.store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,department_id) VALUES($1,'合成其他部门目标','synthetic','ready','openai_api',$2)",foreign,f.b.id);
 await assert.rejects(f.request(f.target,f.scopeB),{status:404});await assert.rejects(f.request(foreign),{status:404});assert.equal((await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',f.original.id)).status,'waiting');f.ticket.close();await f.applying;assert.equal((await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',f.original.id)).status,'completed');
});
test('人工覆盖成功不恢复原自动决定',integration,async t=>{
 const f=await pending(t),manual=await f.request();f.ticket.close();await waitFor(async()=>(await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',manual.value.handoff_id)).status==='completed');await f.applying.catch(()=>{});assert.equal((await f.store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',f.move.session_id)).account_id,f.target);assert.notEqual((await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',f.original.id)).status,'waiting');
});
test('停用部门不复活原自动组且人工决定可终态化',integration,async t=>{
 const f=await pending(t),manual=await f.request();await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});f.ticket.close();await waitFor(async()=>(await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',manual.value.handoff_id)).status!=='waiting');await f.nightly.runOnce();await f.applying.catch(()=>{});assert.equal((await f.store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',f.move.session_id)).account_id,f.move.from);assert.notEqual((await f.store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',f.original.id)).status,'waiting');
});
