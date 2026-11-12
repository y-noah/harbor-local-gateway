import test from 'node:test';
import assert from 'node:assert/strict';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {usageWindow} from '../store.mjs';
import {migrateUsageRollups,backfillUsageRollups,reconcileUsageRollups,disableUsageRollups} from '../postgres-usage-rollups.mjs';
const integration={skip:!process.env.HARBOR_CAPACITY_PG_BIN?'需自有隔离PG工具，不访问线上':false,timeout:30000},at=Date.parse('2026-11-12T18:23:45.678+08:00');
function gate(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};}
async function fixture(run){
 let lab,store,readStore,legacyStore;const readers=[];
 try{
  lab=await createPrivatePostgres({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});store=new PostgresStore({connection:lab.connection});readStore=new PostgresStore({connection:lab.connection,poolSize:1});legacyStore=new PostgresStore({connection:lab.connection,poolSize:1});await store.migrate();await store.initializePricing();
  await store.query("INSERT INTO harbor.accounts(id,name,home,state,api_secret_digest) VALUES('a','合成账号A','synthetic-a','ready',NULL),('b','合成账号B','synthetic-b','ready','synthetic-digest'),('empty','合成空账号','synthetic-empty','offline',NULL)");await store.run("UPDATE harbor.accounts SET auth_mode='openai_api',api_weight=10 WHERE id='b'");
  await store.query("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('synthetic-v1',0,'{}'),('synthetic-v2',0,'{}')");
  const window=usageWindow(at),times=[window.start-1,window.start,window.start+1,window.start+3600000-1,window.start+3600000,at-1,at,window.end-1,window.end];
  for(const [i,time] of times.entries()){
   const id='synthetic-request-'+i,revision=i===3?'synthetic-missing':'synthetic-v1';await store.run("INSERT INTO harbor.requests(id,account_id,started,ended,status,ip,model,price_version) VALUES($1,$2,$3,$4,'ok','synthetic','gpt-6.1-sol',$5)",id,i===6?'b':'a',time-1,time,revision);
   if(i===5)continue;
   await store.run("INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) VALUES($1,$2,$3,$4,20,1,-1,$5,$6)",id,i===4?'pending':'priced',i===2?'synthetic-v2':'synthetic-v1',i===4?null:'900719925474099312345',i===4?null:'900719925474099312365',at);
  }
  const make=(target=readStore,options={})=>{const router=createPostgresRouter(target,{clock:()=>at}),reader=createPostgresStateQueries(target,{clock:()=>at,balancingReader:time=>router.readView({at:time}),...options});readers.push(reader);return reader;};
  const legacy=make(legacyStore);const baseline=await legacy.read({view:'accounts',people:'none'});
  await run({store,readStore,legacyStore,make,legacy,baseline,window});
 }finally{for(const reader of readers)await reader.close();await readStore?.close();await legacyStore?.close();await store?.close();await lab?.close();}
}
async function install(store){await migrateUsageRollups(store,{apply:true});for(;;){const value=await backfillUsageRollups(store,{batchSize:3});if(value.scanned<3)break;}}
const money=sql=>sql.includes('harbor.usage_hour_buckets')&&sql.includes("b.scope='account'");

test('账号完整DTO未安装/未ready/ready/disabled一致，真实queryStore只启动核验一次且GET不取迁移锁',integration,()=>fixture(async({store,readStore,make,legacy,baseline})=>{
 assert.equal(await store.get("SELECT value FROM harbor.gateway_settings WHERE name='schema.usage-hour-rollups'"),undefined);
 await install(store);const statements=[],query=readStore.query.bind(readStore);readStore.query=async(sql,args=[])=>{statements.push(sql);return query(sql,args);};const reader=make();
 assert.deepEqual(await reader.read({view:'accounts',people:'none'}),baseline);assert.ok(!statements.some(money));
 await reconcileUsageRollups(store,{publishReady:true});statements.length=0;assert.deepEqual(await reader.read({view:'accounts',people:'none'}),baseline);assert.equal(statements.filter(money).length,1);assert.equal(statements.filter(sql=>sql==='SELECT state FROM harbor.usage_rollup_state WHERE singleton').length,1);assert.ok(!statements.some(sql=>/pg_advisory|pg_proc|pg_trigger|pg_constraint|pg_attribute/.test(sql)));
 const expected=await legacy.read({view:'accounts'});assert.deepEqual(await reader.read({view:'accounts'}),expected);
 await store.run("DELETE FROM harbor.pricing_versions WHERE id='synthetic-v1'");assert.deepEqual(await reader.read({view:'accounts'}),await legacy.read({view:'accounts'}));
 await store.run("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('synthetic-v1',0,'{}')");await store.run("UPDATE harbor.request_prices SET total_atoms=123,input_atoms=103 WHERE request_id='synthetic-request-1'");assert.deepEqual(await reader.read({view:'accounts'}),await legacy.read({view:'accounts'}));
 await disableUsageRollups(store);statements.length=0;assert.deepEqual(await reader.read({view:'accounts'}),await legacy.read({view:'accounts'}));assert.ok(!statements.some(money));
 assert.equal((await reader.read({view:'accounts'})).accounts.find(row=>row.id==='empty').standard.requests,0);
}));

test('账号小时金额与原读取共享同一RR快照，并发提交不会混合新旧金额',integration,()=>fixture(async({store,readStore,make,legacy,baseline})=>{
 await install(store);await reconcileUsageRollups(store,{publishReady:true});const reader=make();await reader.read({view:'accounts'});const seen=gate(),release=gate(),query=readStore.query.bind(readStore);let armed=true;
 readStore.query=async(sql,args=[])=>{const value=await query(sql,args);if(armed&&sql==='SELECT state FROM harbor.usage_rollup_state WHERE singleton'){armed=false;seen.resolve();await release.promise;}return value;};
 const pending=reader.read({view:'accounts'});try{await seen.promise;await store.run("UPDATE harbor.request_prices SET total_atoms=42,input_atoms=22 WHERE request_id='synthetic-request-1'");release.resolve();assert.deepEqual(await pending,baseline);}finally{release.resolve();await pending;readStore.query=query;}
 const updated=await reader.read({view:'accounts'});assert.deepEqual(updated,await legacy.read({view:'accounts'}));assert.notDeepEqual(updated,baseline);
}));

test('账号空池与其它状态视图保留既有契约，不启动金额汇总',integration,()=>fixture(async({store,readStore,make,legacy})=>{
 await install(store);await reconcileUsageRollups(store,{publishReady:true});const reader=make(),statements=[],query=readStore.query.bind(readStore);await reader.read({view:'accounts'});readStore.query=async(sql,args=[])=>{statements.push(sql);return query(sql,args);};
 for(const view of ['overview','sessions']){statements.length=0;assert.deepEqual(await reader.read({view}),await legacy.read({view}));assert.ok(!statements.some(money));assert.ok(!statements.some(sql=>sql==='SELECT state FROM harbor.usage_rollup_state WHERE singleton'));}
 await store.query('DELETE FROM harbor.requests');await store.query('DELETE FROM harbor.accounts');statements.length=0;const empty=await reader.read({view:'accounts'});assert.deepEqual(empty,await legacy.read({view:'accounts'}));assert.deepEqual(empty.accounts,[]);assert.ok(!statements.some(money));
}));

test('账号取消排队及非法参数沿用原门禁，不执行汇总或改变状态',integration,()=>fixture(async({store,readStore,make})=>{
 await install(store);await reconcileUsageRollups(store,{publishReady:true});const queue=createPostgresQueryQueue({maxConcurrent:1}),reader=make(readStore,{queryQueue:queue}),seen=gate(),release=gate();const blocker=queue.run(async()=>{seen.resolve();await release.promise;});await seen.promise;
 const abort=new AbortController(),waiting=reader.read({view:'accounts'},{signal:abort.signal});abort.abort(Error('synthetic account cancellation'));await assert.rejects(waiting,/synthetic account cancellation/);release.resolve();await blocker;
 await assert.rejects(reader.read({view:'accounts',sessionPage:0}),e=>e.status===400);await assert.rejects(reader.read({view:'accounts',sessionAccount:'invalid account'}),e=>e.status===400);assert.equal((await store.get('SELECT state FROM harbor.usage_rollup_state WHERE singleton')).state,'ready');assert.equal((await reader.read({view:'accounts'})).accounts.length,3);
}));
