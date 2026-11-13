import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';

const databaseUrl=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!databaseUrl?'需独立真实PG测试库':false,timeout:60000};
const response=()=>({headers:{},setHeader(name,value){this.headers[name]=value;}});
const request=res=>({headers:{cookie:res.headers['Set-Cookie'].split(';')[0]}});
function options(database){const url=new URL(databaseUrl);if(database)url.pathname='/'+database;return {connectionString:url.href,allowPlaintextLocalNetwork:true,poolSize:1};}

test('真实PG管理密钥接入原登录会话，撤销及部门停用恢复不复活旧Cookie',integration,async()=>{
 const admin=new PostgresStore(options()),database='harbor_department_auth_'+randomUUID().replaceAll('-','');let store,created=false;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),superScope=issuer.superAdmin({credentialId:'synthetic-super',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer});
  const a=await departments.createDepartment(superScope,{name:'合成同名部门'}),b=await departments.createDepartment(superScope,{name:'合成同名部门'});assert.notEqual(a.id,b.id);
  const keyA=await departments.issueManagementKey(superScope,a.id,{label:'合成负责人A'}),keyB=await departments.issueManagementKey(superScope,b.id,{label:'合成负责人B'});
  const auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-super')},{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}});
  const login=async secret=>{const res=response();await auth.loginAsync(secret,res);return request(res);};
  const reqA=await login(keyA.secret),reqB=await login(keyB.secret);
  assert.equal(auth.authenticated(reqA),false);assert.equal((await auth.context(reqA)).department_id,a.id);assert.equal((await auth.context(reqB)).department_id,b.id);
  const listing=await departments.listManagementKeys(superScope,a.id);assert.equal(listing.rows.length,1);assert.equal(Object.hasOwn(listing.rows[0],'digest'),false);assert.equal(Object.hasOwn(listing.rows[0],'secret'),false);
  await assert.rejects(departments.listDepartments(await auth.context(reqA)),{code:'department_forbidden'});
  // The A Cookie is deliberately not presented while disabled. A later restore
  // must still revoke it, rather than depending on a request during the outage.
  const disabled=await departments.updateDepartment(superScope,a.id,{expected_version:a.version,enabled:false});
  await departments.updateDepartment(superScope,a.id,{expected_version:disabled.version,enabled:true});
  assert.equal(await auth.context(reqA),null);assert.equal((await auth.context(reqB)).department_id,b.id);
  const refreshed=await login(keyA.secret);assert.equal((await auth.context(refreshed)).department_id,a.id);
  const current=(await departments.listManagementKeys(superScope,a.id)).rows[0];await departments.revokeManagementKey(superScope,a.id,current.id,{expected_version:current.version});
  assert.equal(await auth.context(refreshed),null);await assert.rejects(login(keyA.secret),{code:'password_incorrect'});
  const reqSuper=await login('synthetic-super');assert.equal(auth.authenticated(reqSuper),true);assert.equal((await auth.context(reqSuper)).view,'global');
 }finally{await store?.close();if(created)await admin.query('DROP DATABASE '+database);await admin.close();}
});
