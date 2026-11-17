import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer,defaultDepartmentId} from '../department-access.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash,createAdminAuth} from '../auth.mjs';
import {createPostgresDepartmentHttp} from '../postgres-department-http.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立真实PG入口':false,timeout:60000};
function options(database){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:3};}
test('超管显式切换团队保持单团队业务归属，默认团队和部门管理员边界不变',integration,async()=>{
 const name='business_boundary_'+randomUUID().replaceAll('-',''),admin=new PostgresStore(options()),directory=await mkdtemp(join(tmpdir(),'harbor-business-boundary-'));let store,app;
 try{
  await admin.query('CREATE DATABASE '+name);store=new PostgresStore(options(name));await store.migrate();await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-setup',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer}),other=await departments.createDepartment(root,{name:'其他团队'}),otherKey=await departments.issueManagementKey(root,other.id,{label:'负责人'});
  for(const [department,suffix] of [[defaultDepartmentId,'own'],[other.id,'foreign']]){
   await store.run('INSERT INTO harbor.members(id,department_id,name,created) VALUES($1,$2,$3,$4)','member-'+suffix,department,'合成员工-'+suffix,Date.now());
   await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,state) VALUES($1,$2,$3,'synthetic','ready')",'account-'+suffix,department,'合成账号-'+suffix);
   await store.run("INSERT INTO harbor.requests(id,department_id,member_id,account_id,started,ended,status,ip,model) VALUES($1,$2,$3,$4,$5,$5,'ok','synthetic','gpt-6.1-sol')",'request-'+suffix,department,'member-'+suffix,'account-'+suffix,Date.now());
  }
  const probe=async()=>[{total:1e12,free:9e11}];probe.status=()=>({source:'controlled_test_measurement'});
  app=await createPostgresGateway({env:{},database:options(name),dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-super')},recording:{enabled:true,storage:{probe,minFreeBytes:0,minFreeRatio:0,cacheMs:0}}});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  const call=async(path,{cookie,method='GET',body,department}={})=>{const response=await fetch(origin+path,{method,headers:{...(cookie?{Cookie:cookie}:{}),...(department?{'X-Harbor-Department':department}:{}),...(body!==undefined?{'Content-Type':'application/json'}:{})},...(body!==undefined?{body:JSON.stringify(body)}:{})});return {status:response.status,value:await response.json(),cookie:response.headers.get('set-cookie')?.split(';')[0]};};
  const login=await call('/api/auth/login',{method:'POST',body:{password:'synthetic-super',includeOverview:true}});assert.equal(login.status,200);assert.equal(login.value.identity.role,'super_admin');assert.equal(login.value.identity.view,'department');assert.equal(login.value.identity.department_id,defaultDepartmentId);assert.deepEqual(login.value.initialState.accounts.map(row=>row.id),['account-own']);const cookie=login.cookie;
  const session=await call('/api/auth/session',{cookie});assert.equal(session.value.identity.department_id,defaultDepartmentId);
  const status=await call('/api/admin/status',{cookie});assert.equal(status.status,200);assert.deepEqual(Object.keys(status.value).sort(),['eventLoop','ownership','storage']);assert.equal((await call('/api/admin/status',{cookie,department:other.id})).status,200);
  const teams=await call('/api/admin/departments',{cookie});assert.equal(teams.status,200);assert.ok(teams.value.rows.some(row=>row.id===other.id));assert.ok(teams.value.rows.every(row=>!Object.hasOwn(row,'members')&&!Object.hasOwn(row,'accounts')));
  assert.equal((await call('/api/admin/departments/'+other.id+'/management-keys',{cookie})).status,200);
  const teamCreated=await call('/api/admin/departments',{cookie,method:'POST',body:{name:'可管理新团队'}});assert.equal(teamCreated.status,201);
  const own=await call('/api/admin/state?view=accounts&people=none',{cookie});assert.equal(own.status,200);assert.deepEqual(own.value.accounts.map(row=>row.id),['account-own']);assert.ok(own.value.balancing);
  const requests=await call('/api/admin/requests',{cookie});assert.equal(requests.status,200);assert.equal(JSON.stringify(requests.value).includes('foreign'),false);
  const members=await call('/api/admin/members?view=list',{cookie});assert.equal(members.status,200);assert.equal(JSON.stringify(members.value).includes('foreign'),false);
  for(const path of ['/api/admin/state?view=accounts&people=none','/api/admin/requests','/api/admin/members?view=list','/api/admin/requests/request-foreign/recording','/api/admin/recordings/exports'])assert.equal((await call(path,{cookie,department:'global'})).status,403,path);
  const selected=await call('/api/admin/state?view=accounts&people=none',{cookie,department:other.id});assert.equal(selected.status,200);assert.deepEqual(selected.value.accounts.map(row=>row.id),['account-foreign']);
  const selectedIdentity=await call('/api/auth/session',{cookie,department:other.id});assert.equal(selectedIdentity.value.identity.department_id,other.id);assert.equal(selectedIdentity.value.identity.home_department_id,defaultDepartmentId);
  for(const path of ['/api/admin/requests','/api/admin/members?view=list']){const result=await call(path,{cookie,department:other.id});assert.equal(result.status,200);assert.equal(JSON.stringify(result.value).includes('foreign'),true);assert.equal(JSON.stringify(result.value).includes('member-own'),false);}
  assert.deepEqual((await call('/api/admin/state?view=accounts&people=none',{cookie})).value.accounts.map(row=>row.id),['account-own']);
  for(const path of ['/api/admin/members/member-foreign','/api/admin/members/member-foreign/device-policy','/api/admin/requests/request-foreign','/api/admin/requests/request-foreign/recording','/api/admin/requests/request-foreign/context','/api/admin/accounts/account-foreign/extra-usage'])assert.equal((await call(path,{cookie})).status,404,path);
  assert.equal((await call('/api/admin/members/member-foreign',{cookie,method:'PATCH',body:{name:'不得修改'}})).status,404);
  const selectedPlan=await call('/api/admin/balancing/plans',{cookie,department:other.id,method:'POST',body:{}});assert.equal(selectedPlan.status,202);assert.equal((await store.get('SELECT department_id FROM harbor.background_jobs WHERE id=$1',selectedPlan.value.id)).department_id,other.id);
  const selectedExport=await call('/api/admin/recordings/exports',{cookie,department:other.id,method:'POST',body:{include_body:false,format:'csv',filters:{}}});assert.equal(selectedExport.status,202);assert.equal((await store.get('SELECT department_id FROM harbor.background_jobs WHERE id=$1',selectedExport.value.id)).department_id,other.id);
  assert.equal((await call('/api/admin/recordings/exports',{cookie,method:'POST',body:{include_body:false,format:'csv',filters:{department_id:other.id}}})).status,400);
  const departmentLogin=await call('/api/auth/login',{method:'POST',body:{password:otherKey.secret}});assert.equal(departmentLogin.value.identity.department_id,other.id);
  const foreignOwn=await call('/api/admin/state?view=accounts&people=none',{cookie:departmentLogin.cookie});assert.deepEqual(foreignOwn.value.accounts.map(row=>row.id),['account-foreign']);
  const foreignExport=await call('/api/admin/recordings/exports',{cookie:departmentLogin.cookie,method:'POST',body:{include_body:false,format:'csv',filters:{}}});assert.equal(foreignExport.status,202);
  for(const suffix of ['', '/download'])assert.equal((await call('/api/admin/recordings/exports/'+encodeURIComponent(foreignExport.value.id)+suffix,{cookie})).status,404);
  const ownPlan=await call('/api/admin/balancing/plans',{cookie,method:'POST',body:{}});assert.equal(ownPlan.status,202);assert.equal((await store.get('SELECT department_id FROM harbor.background_jobs WHERE id=$1',ownPlan.value.id)).department_id,defaultDepartmentId);
  assert.equal((await call('/api/admin/departments',{cookie:departmentLogin.cookie})).status,403);
  for(const path of ['/api/admin/state?view=accounts&people=none','/api/admin/requests','/api/admin/members?view=list','/api/admin/requests/request-own/recording','/api/admin/recordings/exports'])for(const department of ['global',defaultDepartmentId])assert.equal((await call(path,{cookie:departmentLogin.cookie,department})).status,403,path);
  const reviewAuth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-revalidate')},{scopeIssuer:issuer}),reviewResponse={headers:{},setHeader(name,value){this.headers[name]=value;}};reviewAuth.login('synthetic-revalidate',reviewResponse);
  const reviewRequest={headers:{cookie:reviewResponse.headers['Set-Cookie'].split(';')[0]}},reviewAccess=createPostgresDepartmentHttp({auth:reviewAuth,departments,issuer,installed:true,readBody:async()=>({})}),beforeDisable=await reviewAccess.context(reviewRequest);
  const defaultDepartment=await departments.getDepartment(root,defaultDepartmentId);await departments.updateDepartment(root,defaultDepartmentId,{enabled:false,expected_version:defaultDepartment.version});
  await assert.rejects(reviewAccess.revalidate(reviewRequest,beforeDisable),{code:'department_forbidden'});
  for(const path of ['/api/admin/state?view=accounts&people=none','/api/admin/requests','/api/admin/members?view=list'])assert.equal((await call(path,{cookie})).status,403,path);
  const disabledSession=await call('/api/auth/session',{cookie});assert.equal(disabledSession.status,200);assert.equal(disabledSession.value.identity.business_available,false);
  const disabledLogin=await call('/api/auth/login',{method:'POST',body:{password:'synthetic-super',includeOverview:true}});assert.equal(disabledLogin.status,200);assert.equal(disabledLogin.value.identity.business_available,false);assert.equal(disabledLogin.value.initialState,undefined);
  assert.equal((await call('/api/admin/departments',{cookie:disabledLogin.cookie})).status,200);
  assert.equal((await call('/api/admin/departments',{cookie})).status,200);
  const restored=await call('/api/admin/departments/'+defaultDepartmentId,{cookie,method:'PATCH',body:{enabled:true,expected_version:defaultDepartment.version+1}});assert.equal(restored.status,200);
  assert.equal((await call('/api/admin/state?view=accounts&people=none',{cookie})).status,200);
 }finally{await app?.close();await store?.close();if(store)await admin.query('DROP DATABASE '+name);await admin.close();await rm(directory,{recursive:true,force:true});}
});
