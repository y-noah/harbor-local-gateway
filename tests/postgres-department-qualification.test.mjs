import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {qualifyCandidateDepartments,closeDepartmentQualification} from '../ci/postgres-department-qualification.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('部门candidate验收拒绝普通库和不具备实际存储证明的TCP本机入口',async()=>{
 await assert.rejects(qualifyCandidateDepartments({env:{HARBOR_DATABASE_URL:'postgresql://localhost/production',HARBOR_CANDIDATE_TEST:'1'},directory:'/tmp/synthetic'}));
 await assert.rejects(qualifyCandidateDepartments({env:{HARBOR_DATABASE_URL:'postgresql://capacity_owner@localhost/harbor_candidate_'+'a'.repeat(32),HARBOR_CANDIDATE_TEST:'1',HARBOR_DATA_DIR:'/tmp/synthetic/data'},directory:'/tmp/synthetic'}));
});
test('必选candidate部门正文验收通过真实本机PG socket并保持跨部门拒绝',{skip:!binDirectory?'独立PG工具链不在普通TCP测试中；必选候选镜像直接调用同一验收函数':false,timeout:60000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),database='harbor_candidate_'+randomUUID().replaceAll('-',''),root=new PostgresStore({connection:lab.connection,allowPlaintextLocalNetwork:true});let store;
 try{
  await root.query('CREATE DATABASE '+database);const url=new URL('postgresql://capacity_owner@localhost/'+database);url.password=lab.connection.password;url.port=(await root.get("SELECT current_setting('port') value")).value;url.searchParams.set('host',join(lab.directory,'socket'));store=new PostgresStore({connectionString:url.href});await store.migrate();await store.initializePricing();const pgData=(await store.get("SELECT current_setting('data_directory') value")).value;await store.close();store=null;
  const data=join(lab.directory,'gateway-data'),runtime=join(lab.directory,'gateway-runtime');await mkdir(data);await mkdir(runtime);
  const result=await qualifyCandidateDepartments({env:{HARBOR_CANDIDATE_TEST:'1',HARBOR_DATABASE_URL:url.href,HARBOR_DATA_DIR:data,HARBOR_RUNTIME_DIR:runtime,HARBOR_RECORDING_STORAGE_MODE:'local',HARBOR_RECORDING_PG_DATA_DIR:pgData,HARBOR_RECORDING_STORAGE_MIN_FREE_BYTES:'0',HARBOR_RECORDING_STORAGE_MIN_FREE_RATIO:'0'},directory:lab.directory});assert.deepEqual(result,{candidate_department_recording_real_storage:true,candidate_department_recording_cross_access_rejected:true});
 }finally{await store?.close();await root.query('DROP DATABASE IF EXISTS '+database);await root.close();await lab.close();}
});

test('部门验收网关关闭失败仍关闭上游及数据库，保留全部关闭错误',async()=>{
 const calls=[],gatewayFailure=new Error('synthetic gateway close failure'),sourceFailure=new Error('synthetic source close failure'),storeFailure=new Error('synthetic store close failure');
 await assert.rejects(closeDepartmentQualification({app:{close:async()=>{calls.push('gateway');throw gatewayFailure;}},source:{closeAllConnections(){calls.push('source-connections');},close(done){calls.push('source');done(sourceFailure);}},store:{close:async()=>{calls.push('store');throw storeFailure;}}}),error=>error instanceof AggregateError&&error.errors.length===3&&error.errors[0]===gatewayFailure&&error.errors[1]===sourceFailure&&error.errors[2]===storeFailure);
 assert.deepEqual(calls,['gateway','source-connections','source','store']);
});
