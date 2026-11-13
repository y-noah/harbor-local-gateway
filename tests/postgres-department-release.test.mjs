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
