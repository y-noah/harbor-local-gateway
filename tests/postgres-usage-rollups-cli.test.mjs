import test from 'node:test';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {PostgresStore} from '../postgres-store.mjs';
import {usageRollupArguments,runUsageRollupsCommand} from '../postgres-usage-rollups-cli.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:30000};
function options(name='postgres'){const u=new URL(url);u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true};}
test('小时汇总CLI默认只读且参数有界、不接受重复或跨动作写入选项',()=>{
 assert.deepEqual(usageRollupArguments([]),{action:'verify'});assert.deepEqual(usageRollupArguments(['verify','--publish-ready']),{action:'verify',publishReady:true});assert.deepEqual(usageRollupArguments(['backfill','--batch-size=20','--batches=3','--budget-ms=1000']),{action:'backfill',batchSize:20,batches:3,budgetMs:1000});
 for(const args of [['install','--publish-ready'],['backfill','--publish-ready'],['disable','--batch-size=100'],['backfill','--batch-size=1001'],['backfill','--batches=101'],['backfill','--budget-ms=0'],['verify','--publish-ready','--publish-ready'],['backfill','--batch-size=20','--batch-size=30'],['verify','--secret=synthetic']])assert.throws(()=>usageRollupArguments(args),e=>e.code==='database_configuration');
});
test('真实PG小时汇总CLI显式安装、有界回填、校验后发布、禁用，默认不安装',integration,async()=>{
 const admin=new PostgresStore(options()),name='harbor_rollup_cli_'+randomUUID().replaceAll('-','');let store;
 try{
  await admin.query('CREATE DATABASE '+name);store=new PostgresStore(options(name));await store.migrate();await createUsageRollupRuntime(store);
  await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0)");await store.query("INSERT INTO harbor.requests(id,member_id,started,status,ip) SELECT 'r'||i,'m',i,'ok','synthetic' FROM generate_series(1,5) i");
  assert.deepEqual(await runUsageRollupsCommand([],options(name)),{action:'verify',installed:false,ready:false});assert.equal((await store.get("SELECT to_regclass('harbor.usage_hour_buckets') name")).name,null);
  assert.deepEqual(await runUsageRollupsCommand(['install'],options(name)),{action:'install',installed:true,changed:true,ready:false});assert.equal((await runUsageRollupsCommand(['verify','--publish-ready'],options(name))).ready,false);
  const first=await runUsageRollupsCommand(['backfill','--batch-size=2','--batches=1'],options(name));assert.equal(first.scanned,2);assert.equal(first.batches,1);assert.equal(first.ready,false);
  const rest=await runUsageRollupsCommand(['backfill','--batch-size=2','--batches=3'],options(name));assert.equal(rest.scanned,3);assert.equal(rest.exhausted,true);assert.equal((await runUsageRollupsCommand(['verify'],options(name))).ready,false);
  assert.equal((await runUsageRollupsCommand(['verify','--publish-ready'],options(name))).ready,true);assert.equal((await runUsageRollupsCommand(['disable'],options(name))).ready,false);assert.equal((await store.get("SELECT count(*) n FROM harbor.requests")).n,5);
 }finally{await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();}
});
test('小时汇总CLI非法参数在连接前拒绝、不泄漏环境凭据',async()=>{
 const run=promisify(execFile);await assert.rejects(run(process.execPath,[new URL('../postgres-usage-rollups-cli.mjs',import.meta.url).pathname,'install','--publish-ready'],{env:{...process.env,HARBOR_DATABASE_URL:'synthetic-secret-not-a-url'},timeout:5000}),error=>error.code===1&&error.stderr.includes('参数无效')&&!error.stderr.includes('synthetic-secret'));
});
