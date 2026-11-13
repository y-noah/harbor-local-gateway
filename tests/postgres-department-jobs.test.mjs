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
 let at=1000;const clock=()=>at,issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')},auth=createAdminAuth(config,{clock,scopeIssuer:issuer}),res={headers:{},setHeader(k,v){this.headers[k]=v;}};auth.login('synthetic-root',res);const superAdmin=await auth.context({headers:{cookie:res.headers['Set-Cookie'].split(';')[0]}}),departments=createPostgresDepartments(store,{issuer,clock,authorizeSuperAdmin:auth.authorizeSuperAdmin});
 let a,b,scopeA,scopeB,keyA;if(install){a=await departments.createDepartment(superAdmin,{name:'合成A'});b=await departments.createDepartment(superAdmin,{name:'合成B'});keyA=await departments.issueManagementKey(superAdmin,a.id,{label:'A负责人'});const keyB=await departments.issueManagementKey(superAdmin,b.id,{label:'B负责人'});scopeA=await departments.authenticateManagementKey(keyA.secret);scopeB=await departments.authenticateManagementKey(keyB.secret);}
 const jobs=createPostgresJobs(store,{clock,departmentActors:departments,scopedKinds:['worker.startup','recording.export','recording.index'],leaseMs:1000});return {database,store,clock,advance:n=>{at+=n;},issuer,config,auth,superAdmin,departments,jobs,a,b,scopeA,scopeB,keyA};
}
test('入队持久可信部门actor，幂等/依赖绑定scope，未适配handler与无scope明确拒绝',integration,async t=>{
 const f=await fixture(t),{jobs}=f;assert.equal(jobs.supportsScopedKind('recording.export'),true);assert.equal(jobs.supportsScopedKind('member.config'),false);
 assert.equal((await jobs.verifyReadiness({handlers:{'worker.startup':async()=>{},'recording.export':async()=>{}}})).installed,true);await assert.rejects(jobs.verifyReadiness({handlers:{'recording.purge':async()=>{}}}),{code:'department_job_kind_unavailable'});
 await assert.rejects(jobs.enqueue('member.config',{scope:f.scopeA}),{code:'department_job_kind_unavailable'});await assert.rejects(jobs.enqueue('recording.export'),{code:'department_scope_required'});
 const input={id:'export-a',scope:f.scopeA,payload:{scope_kind:'global',actor_kind:'super_admin',department_id:f.b.id}};assert.equal((await jobs.enqueue('recording.export',input)).created,true);assert.equal((await jobs.enqueue('recording.export',input)).created,false);
 assert.deepEqual(await f.store.get("SELECT department_id,scope_kind,scope_identity,actor_kind,actor_id,actor_version FROM harbor.background_jobs WHERE id='export-a'"),{department_id:f.a.id,scope_kind:'department',scope_identity:'department:'+f.a.id,actor_kind:'department_admin',actor_id:f.scopeA.credential_id,actor_version:0});
 await assert.rejects(jobs.enqueue('recording.export',{...input,scope:f.scopeB}),{code:'job_identity_conflict'});
 await assert.rejects(jobs.enqueue('recording.export',{id:'bad-dependency',scope:f.scopeB,dependencyId:'export-a'}),{code:'job_dependency_invalid'});assert.equal((await jobs.enqueue('recording.export',{id:'same-department-child',scope:f.scopeA,dependencyId:'export-a'})).created,true);
 await assert.rejects(jobs.enqueue('recording.export',{scope:JSON.parse(JSON.stringify(f.scopeA))}),{code:'department_scope_required'});
 const unconfigured=createPostgresJobs(f.store);assert.equal(unconfigured.supportsScopedKind('worker.startup'),false);await assert.rejects(unconfigured.verifyReadiness({handlers:{'worker.startup':async()=>{}}}),{code:'department_job_authorization_unavailable'});
});

test('恢复按DB lease和原owner重核，不从传入JSON造权限，撤销与停恢复使旧任务失权',integration,async t=>{
 const f=await fixture(t);await f.jobs.enqueue('recording.export',{id:'export-a',scope:f.scopeA});const job=await f.jobs.claim(['recording.export']);assert.equal(job.actor_id,f.scopeA.credential_id);assert.equal(job.scope_identity,'department:'+f.a.id);
 const recovered=await f.jobs.authorizePersistedActor({...job,department_id:f.b.id,actor_id:f.scopeB.credential_id,actor_kind:'super_admin'});assert.equal(recovered.department_id,f.a.id);assert.equal(recovered.credential_id,f.scopeA.credential_id);
 await assert.rejects(f.jobs.authorizePersistedActor({...job,claim_token:'wrong-token'}),{code:'job_lease_lost'});await assert.rejects(f.jobs.authorizePersistedActor({id:job.id}),{code:'job_lease_lost'});assert.equal((await f.jobs.authorizePersistedActor({id:job.id},{requireLease:false})).department_id,f.a.id);
 await f.jobs.complete(job,{done:true});await assert.rejects(f.jobs.authorizePersistedActor(job),{code:'job_lease_lost'});assert.equal((await f.jobs.authorizePersistedActor({id:job.id},{requireLease:false})).department_id,f.a.id);
 await f.departments.revokeManagementKey(f.superAdmin,f.a.id,f.keyA.key.id,{expected_version:0});await assert.rejects(f.jobs.authorizePersistedActor({id:job.id},{requireLease:false}),{code:'department_forbidden'});
 await f.jobs.enqueue('recording.export',{id:'export-b',scope:f.scopeB});const other=await f.jobs.claim(['recording.export']);await f.departments.updateDepartment(f.superAdmin,f.b.id,{enabled:false,expected_version:0});await f.departments.updateDepartment(f.superAdmin,f.b.id,{enabled:true,expected_version:1});await assert.rejects(f.jobs.authorizePersistedActor(other),{code:'department_forbidden'});await f.jobs.fail(other,{retry:false,errorCode:'department_forbidden'});assert.equal((await f.store.get("SELECT status FROM harbor.background_jobs WHERE id='export-b'")).status,'failed');
});

test('全局导出仅当前超管，密码配置变化撤销旧job；系统scope只固定handler且不能导出',integration,async t=>{
 const f=await fixture(t);await f.jobs.enqueue('recording.export',{id:'global-export',scope:f.superAdmin});const global=await f.jobs.claim(['recording.export']);assert.equal(global.department_id,null);assert.equal((await f.jobs.authorizePersistedActor(global)).view,'global');
 const system=f.issuer.system({purpose:'worker.startup'});await f.jobs.enqueue('worker.startup',{scope:system});const probe=await f.jobs.claim(['worker.startup']);assert.equal((await f.jobs.authorizePersistedActor(probe)).role,'system');
 await assert.rejects(f.jobs.enqueue('recording.export',{scope:f.issuer.system({purpose:'recording.export'})}),{code:'department_forbidden'});await assert.rejects(f.jobs.enqueue('worker.startup',{scope:f.superAdmin}),{code:'department_forbidden'});await assert.rejects(f.jobs.enqueue('worker.startup',{scope:f.issuer.system({purpose:'different-purpose'})}),{code:'department_forbidden'});
 const departmentRoot=f.issuer.forDepartment(f.superAdmin,f.a.id);await f.jobs.enqueue('recording.export',{id:'department-root-export',scope:departmentRoot});assert.equal((await f.jobs.authorizePersistedActor({id:'department-root-export'},{requireLease:false})).department_id,f.a.id);
 f.config.adminPasswordHash=passwordHash('synthetic-rotated');await assert.rejects(f.jobs.authorizePersistedActor(global),{code:'department_forbidden'});await assert.rejects(f.jobs.authorizePersistedActor({id:'department-root-export'},{requireLease:false}),{code:'department_forbidden'});
 f.advance(1001);await assert.rejects(f.jobs.authorizePersistedActor(probe),{code:'job_lease_lost'});
});

test('legacy入队不变，热安装旧工厂拒绝；迁移来源不伪造当前管理凭据',integration,async t=>{
 const f=await fixture(t,{install:false}),legacy=createPostgresJobs(f.store,{clock:f.clock});await legacy.enqueue('worker.startup',{id:'legacy-probe'});await legacy.enqueue('recording.export',{id:'legacy-export'});await assert.rejects(legacy.enqueue('worker.startup',{scope:f.issuer.system({purpose:'worker.startup',departmentId:'department-a'})}),{code:'department_unavailable'});assert.equal((await legacy.verifyReadiness()).installed,false);assert.equal(await legacy.authorizePersistedActor({id:'legacy-probe'}),null);
 await migrateDepartmentIsolation(f.store,{apply:true});await assert.rejects(legacy.enqueue('worker.startup'),{code:'department_restart_required'});
 const installed=createPostgresJobs(f.store,{clock:f.clock,departmentActors:f.departments,scopedKinds:['worker.startup','recording.export']});const probe=await installed.claim(['worker.startup']);assert.equal((await installed.authorizePersistedActor(probe)).role,'system');const oldExport=await installed.claim(['recording.export']);await assert.rejects(installed.authorizePersistedActor(oldExport),{code:'department_forbidden'});
});
