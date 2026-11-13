import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createPostgresNightlyRevisions} from '../postgres-nightly-revisions.mjs';
import {departmentPlanningJobId} from '../postgres-planning-jobs.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需独立PG18':false};
function options(database){if(url){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,password:process.env.HARBOR_TEST_PG_PASSWORD,database:database??'postgres'}};}
async function fixture(t,{install=true}={}){
 const database='harbor_dj_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});await root.query('CREATE DATABASE '+database);const store=new PostgresStore({...options(database),poolSize:4});t.after(async()=>{await store.close();await root.query('DROP DATABASE '+database);await root.close();});await store.migrate();if(install)await migrateDepartmentIsolation(store,{apply:true});
 let at=Date.parse('2026-11-13T18:05:00Z');const clock=()=>at,issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')},auth=createAdminAuth(config,{clock,scopeIssuer:issuer}),res={headers:{},setHeader(k,v){this.headers[k]=v;}};auth.login('synthetic-root',res);const superAdmin=await auth.context({headers:{cookie:res.headers['Set-Cookie'].split(';')[0]}}),departments=createPostgresDepartments(store,{issuer,clock,authorizeSuperAdmin:auth.authorizeSuperAdmin});
 let a,b,scopeA,scopeB,keyA;if(install){a=await departments.createDepartment(superAdmin,{name:'合成A'});b=await departments.createDepartment(superAdmin,{name:'合成B'});keyA=await departments.issueManagementKey(superAdmin,a.id,{label:'A负责人'});const keyB=await departments.issueManagementKey(superAdmin,b.id,{label:'B负责人'});scopeA=await departments.authenticateManagementKey(keyA.secret);scopeB=await departments.authenticateManagementKey(keyB.secret);}
 await store.initializePricing();const jobs=createPostgresJobs(store,{clock,departmentActors:departments,scopedKinds:['routing.preview','routing.expiry','routing.nightly'],leaseMs:30000});return {database,store,clock,advance:n=>{at+=n;},issuer,config,auth,superAdmin,departments,jobs,a,b,scopeA,scopeB,keyA};
}

test('同slot夜间修订按部门独立，修订任务与FK在同一事务创建',integration,async t=>{
 const f=await fixture(t),revisions=createPostgresNightlyRevisions(f.store,{clock:f.clock,departmentActors:f.departments}),slot='2026-11-14-0200',plans=[];
 for(const department of [f.a,f.b]){
  const scope=f.issuer.system({purpose:'routing.nightly',departmentId:department.id}),expiry=f.issuer.system({purpose:'routing.expiry',departmentId:department.id}),dependencyId=departmentPlanningJobId('routing.expiry','boundary',scope),planId=departmentPlanningJobId('routing.nightly',slot,scope);
  await f.jobs.enqueue('routing.expiry',{id:dependencyId,scope:expiry});let job=await f.jobs.claim(['routing.expiry']);await f.jobs.complete(job,{done:true});
  await f.jobs.enqueue('routing.nightly',{id:planId,target:slot,payload:{boundary:f.clock()},dependencyId,scope});job=await f.jobs.claim(['routing.nightly']);await f.jobs.complete(job,{plan_id:planId});
  const plan={id:planId,target:slot,header:{counts:{moves:0},metric:'reference_usd',window:{key:slot}}};
  await f.store.run('INSERT INTO harbor.routing_plans(id,created,expires,header,bytes,department_id) VALUES($1,$2,$3,$4::jsonb,1,$5)',planId,f.clock(),f.clock()+60000,JSON.stringify(plan.header),department.id);
  await revisions.ensure(plan,{scope});plans.push({plan,scope,department});
 }
 assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.balance_runs WHERE day=$1',slot)).n,2);
 await assert.rejects(revisions.ensure(plans[1].plan,{scope:plans[0].scope}),{status:404});
 await assert.rejects(revisions.summary(slot),{code:'department_scope_required'});
 const ids=await Promise.all(plans.map(async({plan,scope,department})=>{
  const id=await revisions.request(plan,'planning_expired',{scope});assert.equal(await revisions.request(plan,'planning_expired',{scope}),id);
  const row=await f.store.get('SELECT department_id,dependency_id,payload FROM harbor.background_jobs WHERE id=$1',id);assert.equal(row.department_id,department.id);assert.equal(row.payload.revision,1);assert.ok(row.dependency_id.includes(department.id));
  const summary=await revisions.summary(slot,{scope});assert.equal(summary.pending_revision,id);assert.equal(summary.revisions.length,1);return id;
 }));assert.notEqual(ids[0],ids[1]);
 const before=await f.store.all('SELECT department_id,summary FROM harbor.balance_runs WHERE day=$1 ORDER BY department_id',slot);await revisions.reconcile(slot,{scope:plans[0].scope});assert.deepEqual(await f.store.all('SELECT department_id,summary FROM harbor.balance_runs WHERE day=$1 ORDER BY department_id',slot),before);
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{expected_version:f.a.version,enabled:false});await assert.rejects(revisions.summary(slot,{scope:plans[0].scope}),{status:403});assert.ok(await revisions.summary(slot,{scope:plans[1].scope}));
});
