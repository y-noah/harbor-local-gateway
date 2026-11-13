import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {hash} from '../store.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
function options(database){if(url){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,password:process.env.HARBOR_TEST_PG_PASSWORD,database:database??'postgres'}};}
test('部门创建/新增Key与真实消耗来源扩展共存，保持策略版本和原secret/digest',{skip:!url&&!socket?'需独立PG18':false},async()=>{
 const database='harbor_dc_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});let store,admin,role;
 try{
  await root.query('CREATE DATABASE '+database);store=new PostgresStore({...options(database),poolSize:4});await store.migrate();await store.initializePricing();role=await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});await migrateDepartmentIsolation(store,{apply:true});
  const issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer}),a=await departments.createDepartment(superAdmin,{name:'合成A'}),b=await departments.createDepartment(superAdmin,{name:'合成B'}),ka=await departments.issueManagementKey(superAdmin,a.id,{label:'合成A'}),kb=await departments.issueManagementKey(superAdmin,b.id,{label:'合成B'}),scopeA=await departments.authenticateManagementKey(ka.secret),scopeB=await departments.authenticateManagementKey(kb.secret);
  admin=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32)});const call=(path,method,payload,scope=scopeA)=>admin.handle(path,method,payload,new URLSearchParams(),{scope});
  const employee=(await call('/api/admin/members','POST',{name:'合成',quota:null,expires:null,consumption_policy:'api_only'})).value;
  const extra=(await call('/api/admin/keys','POST',{memberId:employee.memberId,expires:null,consumption_policy:'subscription_only'})).value;
  assert.equal(employee.consumption_policy,'api_only');assert.equal(extra.consumption_policy,'subscription_only');
  for(const value of [employee,extra])assert.deepEqual(await store.get('SELECT department_id,digest,consumption_policy,consumption_policy_version FROM harbor.keys WHERE id=$1',value.keyId),{department_id:a.id,digest:hash(value.secret),consumption_policy:value.consumption_policy,consumption_policy_version:0});
  await call('/api/admin/keys/'+extra.keyId,'PATCH',{consumption_policy:'api_first',expected_consumption_policy_version:0});assert.equal((await call('/api/admin/keys/'+extra.keyId+'/policy','GET')).value.consumption_policy_version,1);
  await assert.rejects(call('/api/admin/keys/'+extra.keyId,'PATCH',{consumption_policy:'inherit',expected_consumption_policy_version:1},scopeB),{code:'department_object_not_found'});
  assert.equal((await store.get('SELECT digest FROM harbor.keys WHERE id=$1',extra.keyId)).digest,hash(extra.secret));
 }finally{await admin?.close();await store?.close();await root.query('DROP DATABASE IF EXISTS '+database);if(role)await root.query('DROP ROLE '+role);await root.close();}
});
