import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {departmentReleaseOperation} from '../postgres-department-release.mjs';
import {verifyDepartmentIsolationAvailability} from '../postgres-departments.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateUsageRollups,verifyUsageRollupAvailability} from '../postgres-usage-rollups.mjs';
import {migrateRecordingRetention} from '../recording-retention.mjs';
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

for(const rollback of [false,true])test(`已装小时汇总的部门发布不更新历史请求；${rollback?'后置失败完整回滚':'提交保留物理身份和计量'}`,integration,async()=>{
 const database='dept_rollup_'+randomUUID().replaceAll('-','').slice(0,16),owner=database+'_owner',admin=new PostgresStore(options());let store,warm,runtime;
 try{
  await admin.query('CREATE DATABASE '+database);store=new PostgresStore(options(database));await store.migrate({recordingContext:true,recordingContextIndex:true});await store.initializePricing();runtime=await createUsageRollupRuntime(store);
  await migrateRecordingRetention(store,{apply:true});await migrateUsageRollups(store,{apply:true,runtimeRole:runtime});assert.equal(await verifyUsageRollupAvailability(store),true);
  await store.run("INSERT INTO harbor.members(id,name,created) VALUES('rollup-member','合成汇总员工',1)");await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('rollup-key','rollup-member',$1,'synthetic',1)",'b'.repeat(64));
  const revision=(await store.get("SELECT value::jsonb->>'active' revision FROM harbor.gateway_settings WHERE name='pricing.state'")).revision;
  await store.transaction(async()=>{
   await store.run("INSERT INTO harbor.requests(id,member_id,key_id,started,ended,status,ip,model,input,output,price_version) VALUES('rollup-priced','rollup-member','rollup-key',1,2,'ok','synthetic','gpt-6.1-sol',100,20,$1)",revision);
   await store.run("INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) VALUES('rollup-priced','priced',$1,100,200,0,0,300,2)",revision);
   await store.run("INSERT INTO harbor.requests(id,started,ended,status,ip) VALUES('rollup-orphan',1,2,'error','synthetic')");
  });
  await store.query('CREATE TABLE harbor.department_update_probe(count INTEGER NOT NULL)');await store.run('INSERT INTO harbor.department_update_probe VALUES(0)');
  await store.query('CREATE FUNCTION harbor.department_update_probe() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN UPDATE harbor.department_update_probe SET count=count+1;RETURN NEW;END$$');await store.query('CREATE TRIGGER department_update_probe AFTER UPDATE ON harbor.requests FOR EACH ROW EXECUTE FUNCTION harbor.department_update_probe()');
  await store.query('CREATE ROLE '+owner+' NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');await store.query('ALTER SCHEMA harbor OWNER TO '+owner);
  for(const row of await store.all("SELECT format('ALTER TABLE %I.%I OWNER TO %I',n.nspname,c.relname,$1::text) sql FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='harbor' AND c.relkind IN ('r','p')",owner))await store.query(row.sql);
  for(const row of await store.all("SELECT format('ALTER FUNCTION %s OWNER TO %I',p.oid::regprocedure,$1::text) sql FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='harbor'",owner))await store.query(row.sql);
  const c=new URL(options(database).connectionString);await store.query((await store.get("SELECT format('ALTER ROLE %I LOGIN PASSWORD %L',$1::text,$2::text) sql",runtime,decodeURIComponent(c.password))).sql);c.username=runtime;warm=new PostgresStore({connectionString:c.href,allowPlaintextLocalNetwork:true,poolSize:1});
  const witness=await warm.get('SELECT (SELECT oid::text FROM pg_database WHERE datname=current_database()) "databaseOid",current_user::regrole::oid::text "currentRoleOid",session_user::regrole::oid::text "sessionRoleOid",pg_backend_pid()::text "backendPid"');
  const plan=await departmentReleaseOperation(store,'plan',{expected:{databaseOid:witness.databaseOid,runtimeOid:witness.currentRoleOid,owner,runtime},runtimeWitness:witness});await warm.close();warm=null;
  const originalColumns=new Set((await store.all("SELECT attname FROM pg_attribute WHERE attrelid='harbor.requests'::regclass AND attnum>0 AND NOT attisdropped")).map(row=>row.attname));
  const requests=async()=>(await store.all('SELECT ctid::text tuple,xmin::text version,to_jsonb(r) value FROM harbor.requests r ORDER BY id')).map(row=>({...row,value:Object.fromEntries(Object.entries(row.value).filter(([name])=>originalColumns.has(name)))})),metering=async()=>({prices:await store.all('SELECT to_jsonb(p) value FROM harbor.request_prices p ORDER BY request_id'),contributions:await store.all('SELECT to_jsonb(c) value FROM harbor.usage_contributions c ORDER BY request_id'),buckets:await store.all('SELECT to_jsonb(b) value FROM harbor.usage_hour_buckets b ORDER BY bucket_key')});
  const before={requests:await requests(),metering:await metering(),privileges:await activationPrivileges(store,owner),file:(await store.get("SELECT pg_relation_filenode('harbor.requests') file")).file};assert.equal(before.requests.length,2);assert.ok(before.metering.buckets.length>0);
  const run=store.run.bind(store);let reachedMarker=false,injectedFailure=false,markerAssertion;
  store.run=async(sql,...args)=>{
   if(sql==='INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2)'&&args[0]==='schema.department-isolation'){
    reachedMarker=true;try{assert.equal((await store.get('SELECT count FROM harbor.department_update_probe')).count,0);assert.deepEqual(await requests(),before.requests);}catch(error){markerAssertion=error;throw error;}
    if(rollback){injectedFailure=true;return run('SELECT 1/0');}
   }
   return run(sql,...args);
  };
  try{if(rollback)await assert.rejects(departmentReleaseOperation(store,'install',{binding:plan.binding,activate:true}),{code:'department_migration_failed'});else assert.equal((await departmentReleaseOperation(store,'install',{binding:plan.binding,activate:true})).installed,true);}catch(error){throw markerAssertion??error;}finally{store.run=run;}
  if(markerAssertion)throw markerAssertion;assert.equal(injectedFailure,rollback);
  assert.equal(reachedMarker,true,'full real migration must reach its publication marker without pending trigger events');assert.equal((await store.get('SELECT count FROM harbor.department_update_probe')).count,0);assert.deepEqual(await requests(),before.requests);assert.deepEqual(await metering(),before.metering);assert.deepEqual(await activationPrivileges(store,owner),before.privileges);assert.equal((await store.get("SELECT pg_relation_filenode('harbor.requests') file")).file,before.file);
  assert.equal(await verifyDepartmentIsolationAvailability(store),!rollback);assert.equal(await verifyUsageRollupAvailability(store),true);assert.equal(await migrateRecordingRetention(store),false);
  const column=await store.get("SELECT a.attnotnull,a.atthasmissing,pg_get_expr(d.adbin,d.adrelid) default_value FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum WHERE a.attrelid='harbor.requests'::regclass AND a.attname='department_id' AND NOT a.attisdropped");
  if(rollback){assert.equal(column,undefined);assert.equal((await store.get("SELECT to_regclass('harbor.departments') relation")).relation,null);}
  else{
   assert.deepEqual(column,{attnotnull:true,atthasmissing:true,default_value:null});assert.deepEqual((await store.all('SELECT DISTINCT department_id FROM harbor.requests')).map(row=>row.department_id),['department-default']);
   await assert.rejects(store.run("INSERT INTO harbor.requests(id,started,status,ip) VALUES('missing-department',1,'error','synthetic')"),{code:'23502'});
   await store.run("INSERT INTO harbor.departments(id,name,created,updated) VALUES('department-b','合成B',1,1)");
   await assert.rejects(store.run("INSERT INTO harbor.requests(id,department_id,member_id,started,status,ip) VALUES('cross-department','department-b','rollup-member',1,'error','synthetic')"),{code:'23503'});
   assert.equal((await store.get("SELECT member_id FROM harbor.requests WHERE id='rollup-orphan'")).member_id,null);
  }
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
