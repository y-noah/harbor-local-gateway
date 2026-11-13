import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {usageWindow} from '../store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateUsageRollups,reconcileUsageRollups,disableUsageRollups} from '../postgres-usage-rollups.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL;
const integration={skip:!url?'需独立合成PG测试库':false,timeout:60000};
function options(database){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true,poolSize:1,statementTimeoutMs:10000};}

test('首页七天消耗使用全部账号小时桶并补充无账号请求，完整DTO保持精确且停用回退',integration,async()=>{
 const admin=new PostgresStore(options()),database='harbor_cost_'+randomUUID().replaceAll('-','');let store,reader,created=false;
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();
  const runtime=await createUsageRollupRuntime(store);
  const at=Date.parse('2026-11-13T12:34:56.789+08:00'),window=usageWindow(at);
  reader=createPostgresStateQueries(store,{clock:()=>at});
  const empty=await reader.read({view:'overview-cost'});assert.deepEqual(empty.daily,[]);
  await migrateUsageRollups(store,{apply:true,runtimeRole:runtime});
  await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','合成员工',0)");
  await store.query("INSERT INTO harbor.accounts(id,name,home,removed_at) VALUES('current','合成账号','synthetic',NULL),('removed','历史账号','synthetic',1)");
  await store.query("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('v1',0,'{}'),('v2',0,'{}')");
  const states=['priced','pending','expired','unmetered','unpriced','unknown','missing','mismatch','missing-book','partial-atoms'];
  let ordinal=0;
  // Seven Beijing days, both sides of midnight, running requests, removed
  // accounts and account-less failures must all survive the same projection.
  for(const account of ['current','removed',null])for(const [day,time] of [window.start-1,window.start,window.start+3599999,window.start+3600000,window.start+86400000-1,window.start+86400000,at,window.end-1,window.end].entries())for(const [i,state] of states.entries()){
   const id='synthetic-'+ordinal++,version=state==='missing-book'?'absent':i%2?'v2':'v1',ended=day===6?null:time;
   await store.run("INSERT INTO harbor.requests(id,member_id,account_id,started,ended,status,ip,price_version) VALUES($1,'m',$2,$3,$4,$5,'synthetic',$6)",id,account,ended===null?time:time-1,ended,ended===null?'running':'failed',version);
   if(state==='missing')continue;
   const total=['priced','mismatch','missing-book'].includes(state)?String(900719925474099312345n+BigInt(ordinal)):null;
   await store.run('INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) VALUES($1,$2,$3,$4,5,1,-1,$5,$6)',id,['mismatch','missing-book','partial-atoms'].includes(state)?'priced':state,state==='mismatch'?'other':version,state==='partial-atoms'?'123':total,total,at);
  }
  const original=store.all.bind(store);let rawSql;
  store.all=async(sql,...args)=>{if(sql.startsWith('WITH source AS (')&&sql.includes('SELECT day,'))rawSql=sql;return original(sql,...args);};
  const fallback=await reader.read({view:'overview-cost'}),full=await reader.read({people:'none'});assert.deepEqual(fallback.daily,full.daily);assert.ok(rawSql);
  assert.equal((await reconcileUsageRollups(store,{publishReady:true})).ready,true);
  const sqls=[];store.all=async(sql,...args)=>{sqls.push(sql);return original(sql,...args);};
  const rolled=await reader.read({view:'overview-cost'});assert.deepEqual(rolled,fallback);
  assert.ok(sqls.some(sql=>sql.includes('harbor.usage_hour_buckets')));
  assert.ok(!sqls.some(sql=>/pg_advisory|pg_proc|pg_constraint/.test(sql)),'GET must reuse the attested extension');
  const bucketSql=sqls.find(sql=>sql.includes('harbor.usage_hour_buckets'));
  const plan=await original('EXPLAIN (FORMAT JSON) '+bucketSql,window.start,window.end);
  const predicates=[];function visit(node){for(const field of ['Index Cond','Filter','Join Filter','Recheck Cond'])if(node[field])predicates.push(node[field]);for(const child of node.Plans??[])visit(child);}visit(plan[0]['QUERY PLAN'][0].Plan);
  assert.ok(!/usage_at\)?\)?::numeric/.test(predicates.join('\n')),'range predicates must retain bigint time-index compatibility');
  const canonical=rows=>rows.map(row=>({...row,revisions:row.revisions?.sort()??null}));
  for(const bounds of [[window.start+1,window.end-1],[window.start+100,window.start+1000],[window.start+100,window.start+3600001],[window.start,window.start],[-3600001,-1]]){
   assert.deepEqual(canonical(await original(bucketSql,...bounds)),canonical(await original(rawSql,...bounds)),'partial hour '+bounds);
  }
  // A removed price book must invalidate both raw and bucket projections.
  await store.query("DELETE FROM harbor.pricing_versions WHERE id='v1'");
  const changed=await reader.read({view:'overview-cost'});assert.deepEqual(changed.daily,(await reader.read({people:'none'})).daily);
  await disableUsageRollups(store);sqls.length=0;assert.deepEqual(await reader.read({view:'overview-cost'}),changed);assert.ok(!sqls.some(sql=>sql.includes('harbor.usage_hour_buckets')));
 }finally{await reader?.close();await store?.close();if(created){await admin.query('DROP DATABASE '+database);await admin.query('DROP ROLE IF EXISTS '+database+'_runtime');}await admin.close();}
});
