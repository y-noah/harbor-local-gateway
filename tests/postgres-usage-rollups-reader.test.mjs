import test from 'node:test';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberQueries} from '../postgres-member-query.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {reconcileUsageRollups} from '../postgres-usage-rollups.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:15000};
function options(name='postgres'){const u=new URL(url);u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:1,statementTimeoutMs:3000};}
function gate(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};}
test('PG单连接独立reader构造后立即读取不互锁，排队取消不执行SQL',integration,async()=>{
 const admin=new PostgresStore(options()),name='harbor_rollup_reader_'+randomUUID().replaceAll('-','');let store,reader,queue;
 try{
  await admin.query('CREATE DATABASE '+name);store=new PostgresStore(options(name));await store.migrate();await createUsageRollupRuntime(store);await store.migrate({usageHourRollups:true});await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0)");await store.query("INSERT INTO harbor.requests(id,member_id,started,status,ip) VALUES('r','m',0,'ok','synthetic')");await reconcileUsageRollups(store,{publishReady:true});
  reader=new PostgresStore(options(name));queue=createPostgresQueryQueue({maxConcurrent:1});const queries=createPostgresMemberQueries(reader,{clock:()=>1000,queryQueue:queue});assert.equal((await queries.detail('m')).standard.requests,1);
  const entered=gate(),release=gate(),held=queue.run(async()=>{entered.resolve();await release.promise;});await entered.promise;
  const original=reader.query.bind(reader);let reads=0;reader.query=(sql,args)=>{reads++;return original(sql,args);};const controller=new AbortController(),pending=queries.detail('m',{signal:controller.signal});controller.abort(new Error('synthetic cancel'));await assert.rejects(pending,/synthetic cancel/);assert.equal(reads,0);release.resolve();await held;assert.equal((await queries.detail('m')).standard.requests,1);
 }finally{await queue?.close();await reader?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();}
});
