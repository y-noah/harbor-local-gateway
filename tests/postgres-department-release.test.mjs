import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {departmentReleaseOperation} from '../postgres-department-release.mjs';
import {verifyDepartmentIsolationAvailability} from '../postgres-departments.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!url?'需独立PG18':false,timeout:60000};
const options=name=>{const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:1};};
test('受控部门发布核验真实runtime、排空、owner及默认归属；重跑不覆盖数据',integration,async()=>{
 const database='dept_release_'+randomUUID().replaceAll('-','').slice(0,16),admin=new PostgresStore(options()),owner=database+'_owner';let store,warm,runtime;
 try{
  await admin.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));await store.migrate({recordingContext:true,recordingContextIndex:true});await store.initializePricing();runtime=await createUsageRollupRuntime(store);
  await store.query('CREATE ROLE '+owner+' NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');await store.query('ALTER SCHEMA harbor OWNER TO '+owner);await store.query('GRANT USAGE ON SCHEMA harbor_operator TO '+owner);await store.query('GRANT SELECT ON harbor_operator.activation TO '+owner);
  for(const row of await store.all("SELECT format('ALTER TABLE %I.%I OWNER TO %I',n.nspname,c.relname,$1::text) sql FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND c.relkind IN ('r','p')",owner))await store.query(row.sql);
  for(const row of await store.all("SELECT format('ALTER FUNCTION %s OWNER TO %I',p.oid::regprocedure,$1::text) sql FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='harbor'",owner))await store.query(row.sql);
  await store.run("INSERT INTO harbor.members(id,name,created) VALUES('kept-member','合成员工',1)");
  const c=new URL(options(database).connectionString);await store.query((await store.get("SELECT format('ALTER ROLE %I LOGIN PASSWORD %L',$1::text,$2::text) sql",runtime,decodeURIComponent(c.password))).sql);c.username=runtime;warm=new PostgresStore({connectionString:c.href,allowPlaintextLocalNetwork:true,poolSize:1});
  const witness=await warm.get('SELECT (SELECT oid::text FROM pg_database WHERE datname=current_database()) "databaseOid",current_user::regrole::oid::text "currentRoleOid",session_user::regrole::oid::text "sessionRoleOid",pg_backend_pid()::text "backendPid"'),expected={databaseOid:witness.databaseOid,runtimeOid:witness.currentRoleOid,owner,runtime};
  await assert.rejects(departmentReleaseOperation(store,'plan',{expected,runtimeWitness:{...witness,backendPid:'1'}}),{code:'department_release_runtime_witness_changed'});
  const plan=await departmentReleaseOperation(store,'plan',{expected,runtimeWitness:witness});assert.equal(plan.installed,false);assert.equal(await verifyDepartmentIsolationAvailability(store),false);
  await assert.rejects(departmentReleaseOperation(store,'install',{binding:plan.binding}),{code:'department_release_activation_required'});
  await assert.rejects(departmentReleaseOperation(store,'install',{binding:plan.binding,activate:true}),{code:'department_release_runtime_not_drained'});
  await warm.close();warm=null;
  await assert.rejects(departmentReleaseOperation(store,'install',{binding:{...plan.binding,databaseOid:'1'},activate:true}),{code:'department_release_database_changed'});
  await assert.rejects(departmentReleaseOperation(store,'install',{binding:{...plan.binding,tables:{...plan.binding.tables,members:'1'}},activate:true}),{code:'department_release_binding_changed'});
  const installed=await departmentReleaseOperation(store,'install',{binding:plan.binding,activate:true});assert.equal(installed.changed,true);assert.equal(installed.installed,true);assert.deepEqual(await store.get("SELECT id,department_id FROM harbor.members WHERE id='kept-member'"),{id:'kept-member',department_id:'department-default'});
  assert.equal((await departmentReleaseOperation(store,'status',{binding:plan.binding})).installed,true);assert.equal((await departmentReleaseOperation(store,'install',{binding:plan.binding,activate:true})).changed,false);
  const cold=new PostgresStore({connectionString:c.href,allowPlaintextLocalNetwork:true,poolSize:1});try{assert.equal(await verifyDepartmentIsolationAvailability(cold),true);assert.equal((await cold.get('SELECT count(*) n FROM harbor.departments')).n,1);}finally{await cold.close();}
  await store.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.department-isolation'");await assert.rejects(departmentReleaseOperation(store,'status',{binding:plan.binding}),{code:'department_migration_conflict'});
 }finally{await warm?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+database);for(const role of [runtime,owner].filter(Boolean))await admin.query('DROP ROLE IF EXISTS '+role);await admin.close();}
});

async function activationPrivileges(store,owner){
 const effective=await store.get("SELECT has_schema_privilege($1,'harbor_operator','USAGE') schema_read,has_table_privilege($1,'harbor_operator.activation','SELECT') activation_read,has_schema_privilege($1,'harbor_operator','USAGE WITH GRANT OPTION') schema_grantable,has_table_privilege($1,'harbor_operator.activation','SELECT WITH GRANT OPTION') activation_grantable",owner);
 const acl=await store.all(`SELECT 'schema' object,a.grantor::text,a.grantee::text,a.privilege_type,a.is_grantable
   FROM pg_namespace n CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.nspname='harbor_operator'
   UNION ALL SELECT 'activation' object,a.grantor::text,a.grantee::text,a.privilege_type,a.is_grantable
   FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a WHERE n.nspname='harbor_operator' AND c.relname='activation'
   ORDER BY object,grantor,grantee,privilege_type,is_grantable`);
 return {effective,acl};
}

for(const grants of ['none','schema','activation','both'])for(const failMigration of [false,true])test(`部门迁移临时补权${grants}，${failMigration?'失败回滚':'成功提交'}保留原授权且不遗留新增权限`,integration,async()=>{
 const database='dept_acl_'+randomUUID().replaceAll('-','').slice(0,16),owner=database+'_owner',admin=new PostgresStore(options());let store,warm,runtime;
 try{
  await admin.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));await store.migrate({recordingContext:true,recordingContextIndex:true});await store.initializePricing();runtime=await createUsageRollupRuntime(store);
  await store.query('CREATE ROLE '+owner+' NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');await store.query('ALTER SCHEMA harbor OWNER TO '+owner);
  // Only the selected pre-existing rights are granted. The none case mirrors
  // production: the migration owner cannot read harbor_operator.activation.
  if(grants==='schema'||grants==='both')await store.query('GRANT USAGE ON SCHEMA harbor_operator TO '+owner+' WITH GRANT OPTION');
  if(grants==='activation'||grants==='both')await store.query('GRANT SELECT ON harbor_operator.activation TO '+owner+' WITH GRANT OPTION');
  for(const row of await store.all("SELECT format('ALTER TABLE %I.%I OWNER TO %I',n.nspname,c.relname,$1::text) sql FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND c.relkind IN ('r','p')",owner))await store.query(row.sql);
  for(const row of await store.all("SELECT format('ALTER FUNCTION %s OWNER TO %I',p.oid::regprocedure,$1::text) sql FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='harbor'",owner))await store.query(row.sql);
  await store.run("INSERT INTO harbor.members(id,name,created) VALUES('acl-kept-member','合成权限回归员工',1)");
  const c=new URL(options(database).connectionString);await store.query((await store.get("SELECT format('ALTER ROLE %I LOGIN PASSWORD %L',$1::text,$2::text) sql",runtime,decodeURIComponent(c.password))).sql);c.username=runtime;warm=new PostgresStore({connectionString:c.href,allowPlaintextLocalNetwork:true,poolSize:1});
  const witness=await warm.get('SELECT (SELECT oid::text FROM pg_database WHERE datname=current_database()) "databaseOid",current_user::regrole::oid::text "currentRoleOid",session_user::regrole::oid::text "sessionRoleOid",pg_backend_pid()::text "backendPid"');
  const plan=await departmentReleaseOperation(store,'plan',{expected:{databaseOid:witness.databaseOid,runtimeOid:witness.currentRoleOid,owner,runtime},runtimeWitness:witness});await warm.close();warm=null;
  const before=await activationPrivileges(store,owner),originalRole=(await store.get('SELECT current_user name')).name;
  assert.equal(before.effective.schema_read,grants==='schema'||grants==='both');assert.equal(before.effective.activation_read,grants==='activation'||grants==='both');
  const query=store.query.bind(store);let reachedMigration=false;const revokedAs=[];
  store.query=async(sql,...args)=>{
   if(/^REVOKE (?:SELECT ON harbor_operator\.activation|USAGE ON SCHEMA harbor_operator)/.test(sql))revokedAs.push((await store.get('SELECT current_user name')).name);
   if(/CREATE TABLE harbor\.departments\b/.test(sql)){
    reachedMigration=true;assert.equal((await store.get('SELECT current_user name')).name,owner);
    assert.deepEqual((await activationPrivileges(store,owner)).effective,{schema_read:true,activation_read:true,schema_grantable:before.effective.schema_grantable,activation_grantable:before.effective.activation_grantable});
    if(failMigration)return query('SELECT 1/0');
   }
   return query(sql,...args);
  };
  try{
   if(failMigration)await assert.rejects(departmentReleaseOperation(store,'install',{binding:plan.binding,activate:true}),{code:'department_migration_failed'});
   else{const result=await departmentReleaseOperation(store,'install',{binding:plan.binding,activate:true});assert.equal(result.installed,true);assert.equal(result.changed,true);}
  }finally{store.query=query;}
  assert.equal(reachedMigration,true,'real migration must run with temporary activation read rights');
  assert.equal((await store.get('SELECT current_user name')).name,originalRole);assert.deepEqual(await activationPrivileges(store,owner),before,'effective privileges and existing grant options/ACLs must survive unchanged');
  assert.equal(await verifyDepartmentIsolationAvailability(store),!failMigration);
  if(failMigration){assert.equal((await store.get("SELECT to_regclass('harbor.departments') relation")).relation,null);assert.deepEqual(revokedAs,[],'failed migration must roll back instead of issuing cleanup inside an aborted transaction');}
  else{assert.equal(revokedAs.length,2-(before.effective.schema_read?1:0)-(before.effective.activation_read?1:0));assert.ok(revokedAs.every(role=>role===originalRole));assert.equal((await store.get("SELECT department_id FROM harbor.members WHERE id='acl-kept-member'")).department_id,'department-default');}
 }finally{await warm?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+database);for(const role of [runtime,owner].filter(Boolean))await admin.query('DROP ROLE IF EXISTS '+role);await admin.close();}
});
