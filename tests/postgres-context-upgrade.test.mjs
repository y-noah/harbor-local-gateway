import test from 'node:test';import assert from 'node:assert/strict';import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';import {upgradeOwnedContext} from '../ci/upgrade-recording-context.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,configuration={skip:!url?'需隔离PostgreSQL及角色管理权限':false};
const options=name=>{const value=new URL(url);value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:1};};
test('上下文维护迁移保持原所有者与运行角色边界，授权和版本推进同事务提交',configuration,async()=>{
 const root=new PostgresStore(options('postgres'));try{
 for(const scenario of ['success','binding','grant-failure']){
  const id=randomUUID().replaceAll('-',''),database='ctx_'+id,owner='ctx_o_'+id,runtime='ctx_r_'+id,migration='ctx_m_'+id;let store;
  try{
   for(const role of [owner,runtime,migration])await root.query('CREATE ROLE "'+role+'" NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS');
   await root.query('CREATE DATABASE "'+database+'" OWNER "'+migration+'"');store=new PostgresStore(options(database));
   await store.transaction(async()=>{await store.query('SET LOCAL ROLE "'+migration+'"');await store.migrate();});
   await store.query('REASSIGN OWNED BY "'+migration+'" TO "'+owner+'"');await store.query('REVOKE ALL ON SCHEMA harbor,public FROM PUBLIC,"'+runtime+'"');await store.query('GRANT USAGE ON SCHEMA harbor TO "'+runtime+'"');
   for(const row of await store.all("SELECT table_name FROM information_schema.tables WHERE table_schema='harbor' AND table_type='BASE TABLE'"))await store.query('GRANT '+(['schema_migrations','data_contract','recording_storage_reports'].includes(row.table_name)?'SELECT':'SELECT,INSERT,UPDATE,DELETE')+' ON TABLE harbor."'+row.table_name+'" TO "'+runtime+'"');
   const binding={databaseOid:(await store.get('SELECT oid::text oid FROM pg_database WHERE datname=current_database()')).oid,ownerOid:(await root.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',owner)).oid,runtimeOid:(await root.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',runtime)).oid};
   if(scenario==='binding'){await assert.rejects(upgradeOwnedContext(store,{...binding,runtimeOid:'0'}),/context_role_generation_changed/);}
   else if(scenario==='grant-failure'){
    const query=store.query.bind(store);store.query=(sql,...args)=>sql.startsWith('GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE harbor."recording_context_')?Promise.reject(Error('synthetic_grant_failure')):query(sql,...args);
    await assert.rejects(upgradeOwnedContext(store,binding),/synthetic_grant_failure/);store.query=query;
   }else{
    assert.deepEqual(await upgradeOwnedContext(store,binding),{version:24,changed:true,contextTables:8,runtimeContextDml:true});assert.equal((await upgradeOwnedContext(store,binding)).changed,false);
    await store.transaction(async()=>{await store.query('SET LOCAL ROLE "'+runtime+'"');await store.run("INSERT INTO harbor.recording_context_objects VALUES('synthetic', $1,$2,'synthetic','history','writing',1,1,1)",Buffer.alloc(32,1),Buffer.alloc(32,2));await store.run("UPDATE harbor.recording_context_objects SET state='complete' WHERE id='synthetic'");assert.equal((await store.get('SELECT COUNT(*)::int n FROM harbor.recording_context_work')).n,1);await assert.rejects(store.transaction(()=>store.query('CREATE TABLE harbor.forbidden(synthetic integer)')),e=>e.code==='42501');await store.run("DELETE FROM harbor.recording_context_objects WHERE id='synthetic'");});
   }
   if(scenario!=='success'){assert.equal((await store.get('SELECT MAX(version) n FROM harbor.schema_migrations')).n,22);assert.equal((await store.get("SELECT to_regclass('harbor.recording_context_objects') relation")).relation,null);}

  }finally{await store?.close();await root.query('DROP DATABASE IF EXISTS "'+database+'"');for(const role of [owner,runtime,migration])await root.query('DROP ROLE IF EXISTS "'+role+'"');}
 }
 }finally{await root.close();}
});
