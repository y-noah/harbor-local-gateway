import test from 'node:test';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import assert from 'node:assert/strict';
import {fields,validPrice,joins,referencePartialAggregateSql,referenceRollupAggregateSql} from '../postgres-reference-query.mjs';
import {randomUUID} from 'node:crypto';
import {createPostgresMemberQueries} from '../postgres-member-query.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateUsageRollups,verifyUsageRollups,backfillUsageRollups,reconcileUsageRollups,usageRollupsReady,disableUsageRollups,usageRollupMoneySql} from '../postgres-usage-rollups.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:20000};
function options(name='postgres'){const u=new URL(url);u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:4,statementTimeoutMs:5000};}
async function fixture(run,{beforeInstall}={}){
 const admin=new PostgresStore(options()),name='harbor_rollup_'+randomUUID().replaceAll('-','');let store,other;
 try{
  await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");store=new PostgresStore(options(name));other=new PostgresStore(options(name));await store.migrate();await createUsageRollupRuntime(store);
  await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0),('n','synthetic',0)");await store.query("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('k','m','synthetic-k','synthetic',0),('l','n','synthetic-l','synthetic',0)");await store.query("INSERT INTO harbor.accounts(id,name,home) VALUES('a','synthetic','synthetic'),('b','synthetic','synthetic')");
  const request=(id,started=0)=>store.run("INSERT INTO harbor.requests(id,member_id,key_id,account_id,started,status,ip,price_version) VALUES($1,'m','k','a',$2,'running','synthetic','revision')",id,started);
  const price=(id,total='900719925474099312345.125')=>store.run("INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,total_atoms,measured_at) VALUES($1,'priced','revision',$2,$2,0)",id,total);
  await beforeInstall?.({store,request,price});await migrateUsageRollups(store,{apply:true});await run({store,other,request,price});
 }finally{await other?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();}
}
async function check(store){assert.deepEqual(await reconcileUsageRollups(store),{valid:true,factMismatches:0,bucketMismatches:0,ready:false});}
function gate(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};}

test('PG小时汇总安装默认回退，显式安装不自动ready，幂等且原子发布',integration,()=>fixture(async({store})=>{
 assert.equal(await migrateUsageRollups(store,{apply:true}),false);assert.equal(await usageRollupsReady(store),false);
 assert.deepEqual(await reconcileUsageRollups(store,{publishReady:true}),{valid:true,factMismatches:0,bucketMismatches:0,ready:true});assert.equal(await usageRollupsReady(store),true);await disableUsageRollups(store);assert.equal(await usageRollupsReady(store),false);await assert.rejects(reconcileUsageRollups(store,{publishReady:true}),e=>e.code==='usage_rollup_disabled');
}));
test('PG所有维度新增/计价/改归属/改小时/级联删除保持精确NUMERIC及事务完整',integration,()=>fixture(async({store,request,price})=>{
 await request('r',-1);await check(store);assert.equal((await store.get('SELECT hour_at FROM harbor.usage_hour_buckets LIMIT 1')).hour_at,-3600000);
 await price('r');await check(store);assert.equal((await store.get('SELECT total::text FROM harbor.usage_hour_buckets LIMIT 1')).total,'900719925474099312345.125');
 await store.transaction(async()=>{await store.run("UPDATE harbor.requests SET ended=7200001,member_id='n',key_id='l',account_id='b' WHERE id='r'");await store.run("UPDATE harbor.request_prices SET total_atoms=123.5,input_atoms=NULL,output_atoms=5,cached_atoms=2,adjustment_atoms=-1,state='unknown',revision=NULL WHERE request_id='r'");});await check(store);
 const rows=await store.all('SELECT scope,owner_id,hour_at,requests,known,total::text FROM harbor.usage_hour_buckets ORDER BY scope');assert.deepEqual(rows.map(r=>[r.owner_id,r.hour_at,r.requests,r.known,r.total]),[['b',7200000,1,1,'123.5'],['l',7200000,1,1,'123.5'],['n',7200000,1,1,'123.5']]);
 await store.run("DELETE FROM harbor.requests WHERE id='r'");await check(store);assert.equal((await store.get('SELECT count(*) count FROM harbor.usage_contributions WHERE request_fact IS NOT NULL OR price_fact IS NOT NULL')).count,0);assert.equal((await store.get('SELECT count(*) count FROM harbor.usage_hour_buckets')).count,0);
}));
test('PG同事务多次更新只应用最终贡献，回滚和保存点不泄漏增量',integration,()=>fixture(async({store,request,price})=>{
 await store.transaction(async()=>{await request('r');await price('r','100');await store.run("UPDATE harbor.requests SET ended=3600001 WHERE id='r'");await store.run("UPDATE harbor.requests SET ended=7200001 WHERE id='r'");await store.run("UPDATE harbor.request_prices SET total_atoms=200 WHERE request_id='r'");});await check(store);
 await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.request_prices SET total_atoms=999 WHERE request_id='r'");throw Error('synthetic rollback');}),/synthetic rollback/);await check(store);
 await store.transaction(async()=>{await assert.rejects(store.transaction(async()=>{await store.run("DELETE FROM harbor.requests WHERE id='r'");throw Error('savepoint rollback');}));await store.run("UPDATE harbor.requests SET ended=10800001 WHERE id='r'");});await check(store);assert.equal((await store.get('SELECT total::text FROM harbor.usage_hour_buckets LIMIT 1')).total,'200');
}));
test('PG缺投影及所有状态、畸形NULL金额保持原始贡献而非猜测修正',integration,()=>fixture(async({store,request})=>{
 for(const [i,state] of ['pending','expired','unpriced','unmetered','unknown','priced'].entries()){const id='r'+i;await request(id);await store.run('INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,total_atoms,measured_at) VALUES($1,$2,$3,$4,$5,0)',id,state,i%2?'other':null,123,i===5?'456':null);}await request('missing');await check(store);
 const counts=await store.get("SELECT SUM(requests)::bigint requests,SUM(known)::bigint known,SUM(pending)::bigint pending,SUM(expired)::bigint expired,SUM(unpriced)::bigint unpriced,SUM(unmetered)::bigint unmetered FROM harbor.usage_hour_buckets WHERE scope='member'");assert.deepEqual(counts,{requests:7,known:1,pending:1,expired:1,unpriced:2,unmetered:2});
}));
test('PG回填有界、保留大额精度、已触发数据幂等并拒绝遗漏ready',integration,()=>fixture(async({store,request})=>{
 assert.equal((await reconcileUsageRollups(store,{publishReady:true})).ready,false);const scans=[];for(let i=0;i<4;i++)scans.push((await backfillUsageRollups(store,{batchSize:2})).scanned);assert.deepEqual(scans,[2,2,1,0]);await check(store);await request('a-late');await check(store);assert.equal((await reconcileUsageRollups(store,{publishReady:true})).ready,true);
},{beforeInstall:async({request,price})=>{for(let i=0;i<5;i++){await request('r'+i,i*3600000);await price('r'+i);}}}));
test('PG已建ledger并发只改request与只改price合并最新组件',integration,()=>fixture(async({store,other,request,price})=>{
 await request('r');await price('r','100');const a=gate(),b=gate(),release=gate();
 const first=store.transaction(async()=>{await store.run("UPDATE harbor.requests SET ended=7200001 WHERE id='r'");a.resolve();await release.promise;});const second=other.transaction(async()=>{await other.run("UPDATE harbor.request_prices SET total_atoms=200 WHERE request_id='r'");b.resolve();await release.promise;});await Promise.all([a.promise,b.promise]);release.resolve();await Promise.all([first,second]);await check(store);
}));
test('PG首次ledger缺失并发request与price不丢失另一组件',integration,()=>fixture(async({store,other})=>{
 const a=gate(),b=gate(),release=gate();const first=store.transaction(async()=>{await store.run("UPDATE harbor.requests SET ended=7200001 WHERE id='r'");a.resolve();await release.promise;});const second=other.transaction(async()=>{await other.run("UPDATE harbor.request_prices SET total_atoms=200 WHERE request_id='r'");b.resolve();await release.promise;});await Promise.all([a.promise,b.promise]);release.resolve();await Promise.all([first,second]);await check(store);
},{beforeInstall:async({request,price})=>{await request('r');await price('r','100');}}));
test('PG两事务跨请求反向命中桶时统一排序而不死锁',integration,()=>fixture(async({store,other,request,price})=>{
 for(const id of ['a','b','c','d']){await request(id);await price(id,'100');}const a=gate(),b=gate(),release=gate();
 const first=store.transaction(async()=>{await store.run("UPDATE harbor.requests SET ended=7200001 WHERE id IN ('a','b')");await store.run("UPDATE harbor.request_prices SET total_atoms=200 WHERE request_id IN ('a','b')");a.resolve();await release.promise;});const second=other.transaction(async()=>{await other.run("UPDATE harbor.requests SET ended=-1 WHERE id IN ('c','d')");await other.run("UPDATE harbor.request_prices SET total_atoms=300 WHERE request_id IN ('c','d')");b.resolve();await release.promise;});await Promise.all([a.promise,b.promise]);release.resolve();await Promise.all([first,second]);await check(store);
}));
test('PG回填旧快照与并发结算冲突整批40001，重跑后精确一致',integration,()=>fixture(async({store,other})=>{
 const seen=gate(),release=gate(),query=store.query.bind(store);let armed=true;store.query=async(sql,args)=>{const result=await query(sql,args);if(armed&&sql.startsWith('SELECT r.id FROM harbor.requests')){armed=false;seen.resolve();await release.promise;}return result;};
 const pending=backfillUsageRollups(store);await seen.promise;await other.run("UPDATE harbor.requests SET ended=7200001 WHERE id='r'");release.resolve();await assert.rejects(pending,e=>e.code==='40001');store.query=query;await backfillUsageRollups(store);await check(store);
},{beforeInstall:async({request,price})=>{await request('r');await price('r','100');}}));
test('PGready对账快照与并发新提交共存，源与贡献原子可见',integration,()=>fixture(async({store,other})=>{
 const seen=gate(),release=gate(),query=store.query.bind(store);let armed=true;store.query=async(sql,args)=>{const result=await query(sql,args);if(armed&&sql.startsWith('WITH actual AS')){armed=false;seen.resolve();await release.promise;}return result;};const ready=reconcileUsageRollups(store,{publishReady:true});await seen.promise;await other.run("INSERT INTO harbor.requests(id,member_id,started,status,ip) VALUES('r','m',0,'ok','synthetic')");release.resolve();assert.equal((await ready).ready,true);store.query=query;assert.equal((await reconcileUsageRollups(store)).valid,true);
}));
test('PGready不能掩盖同名伪约束、停用触发器或函数篡改',integration,()=>fixture(async({store})=>{
 for(const sql of ["ALTER TABLE harbor.usage_hour_buckets DROP CONSTRAINT usage_hour_counts;ALTER TABLE harbor.usage_hour_buckets ADD CONSTRAINT usage_hour_counts CHECK(true)","ALTER TABLE harbor.requests DISABLE TRIGGER usage_requests_stage","CREATE OR REPLACE FUNCTION harbor.usage_prepare_staging() RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,harbor AS $$ BEGIN RETURN;END $$"]){await assert.rejects(store.transaction(async()=>{await store.query(sql);await verifyUsageRollups(store);}),e=>e.code==='database_migration_conflict');await verifyUsageRollups(store);}
}));

test('PG小时读数与原始精确查询跨维度/非整小时/短窗/负时间/费率簿变化一致',integration,()=>fixture(async({store,request,price})=>{
 await store.run("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('revision',0,'{}'),('other',0,'{}')");
 const times=[-10800001,-7200000,-3600001,-3600000,-1,0,1,999,3599999,3600000,3600001,7199999,7200000,7200001,10800000,14400000,18000000];
 for(const [i,time] of times.entries()){const id='r'+i;await request(id,time);if(i%5!==0){await price(id,String(900719925474099312345n+BigInt(i)));if(i%4===0)await store.run("UPDATE harbor.request_prices SET total_atoms=NULL,state='pending' WHERE request_id=$1",id);if(i%3===0)await store.run("UPDATE harbor.request_prices SET revision='other' WHERE request_id=$1",id);}}
 await check(store);
 const windows=[[-14400000,21600000,-5000001,5000001],[-1,1,-999,999],[100,1000,100,200],[-999,-100,-800,-600],[0,3600000,3600000,7200000],[0,0,0,0],[100,7200001,999,10800000]];
 function baseline(scope){return `WITH windows AS (SELECT false recent,$2::bigint from_at,$3::bigint to_at UNION ALL SELECT true,$4::bigint,$5::bigint),priced AS (SELECT r.${scope}_id owner_id,w.recent,r.price_version revision,CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,${fields.map(f=>`CASE WHEN ${validPrice} THEN p.${f}_atoms END ${f}`).join(',')} FROM windows w JOIN harbor.requests r ON r.usage_at>=w.from_at AND r.usage_at<w.to_at ${joins} WHERE r.${scope}_id=ANY($1::text[])),partial AS (SELECT owner_id,revision,recent,${referencePartialAggregateSql({standardOnly:true})} FROM priced GROUP BY owner_id,revision,recent) SELECT owner_id,${referenceRollupAggregateSql({standardOnly:true,condition:'NOT recent'})},${referenceRollupAggregateSql({standardOnly:true,condition:'recent',prefix:'recent_'})} FROM partial GROUP BY owner_id`;}
 const canonical=rows=>rows.map(r=>({...r,revisions:r.revisions?.sort()??null,recent_revisions:r.recent_revisions?.sort()??null})).sort((a,b)=>a.owner_id.localeCompare(b.owner_id));
 for(const scope of ['member','key','account'])for(const w of windows){const ids=scope==='member'?['m','n']:scope==='key'?['k','l']:['a','b'];assert.deepEqual(canonical(await store.all(usageRollupMoneySql(scope),ids,...w)),canonical(await store.all(baseline(scope),ids,...w)),scope+':'+w);}
 await store.run("DELETE FROM harbor.pricing_versions WHERE id='revision'");for(const scope of ['member','key','account']){const ids=scope==='member'?['m']:scope==='key'?['k']:['a'];assert.deepEqual(canonical(await store.all(usageRollupMoneySql(scope),ids,...windows[0])),canonical(await store.all(baseline(scope),ids,...windows[0])));}
 await check(store);
}));

test('PG独立queryStore员工/Key完整DTO启用前后等价，ready后实际走桶且GET不取迁移锁',integration,()=>fixture(async({store,other,request,price})=>{
 const at=Date.parse('2026-11-12T18:23:45.678+08:00');await store.run("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('revision',0,'{}')");
 for(let i=0;i<12;i++){const id='r'+i;await request(id,at-i*3600000);await price(id,String(900719925474099312345n+BigInt(i)));}
 const query=createPostgresMemberQueries(other,{clock:()=>at}),spec={page:1,pageSize:20,q:'',typeId:'',status:''};
 const before={list:await query.list(spec),detail:await query.detail('m'),keys:await query.keys({page:1,pageSize:20,q:''})};
 assert.equal((await reconcileUsageRollups(store,{publishReady:true})).ready,true);const sqls=[],original=other.query.bind(other);other.query=(sql,args)=>{sqls.push(sql);return original(sql,args);};
 const after={list:await query.list(spec),detail:await query.detail('m'),keys:await query.keys({page:1,pageSize:20,q:''})};assert.deepEqual(after,before);assert.ok(sqls.some(sql=>sql.includes('harbor.usage_hour_buckets')));assert.ok(!sqls.some(sql=>/pg_advisory|pg_proc|pg_constraint/.test(sql)));
 await disableUsageRollups(store);sqls.length=0;assert.deepEqual(await query.detail('m'),before.detail);assert.ok(!sqls.some(sql=>sql.includes('harbor.usage_hour_buckets')));
}));
test('PG默认迁移不安装扩展，显式扩展保留22且verify拒绝篡改',integration,async()=>{
 const admin=new PostgresStore(options()),name='harbor_rollup_'+randomUUID().replaceAll('-','');let store;
 try{await admin.query('CREATE DATABASE '+name);store=new PostgresStore(options(name));assert.deepEqual(await store.migrate(),{version:22,changed:true});assert.equal((await store.get("SELECT to_regclass('harbor.usage_hour_buckets') name")).name,null);assert.equal(await usageRollupsReady(store),false);await createUsageRollupRuntime(store);assert.deepEqual(await store.migrate({usageHourRollups:true}),{version:22,changed:true});assert.equal(await store.verifySchema(),true);assert.deepEqual(await store.migrate(),{version:22,changed:false});await store.query('ALTER TABLE harbor.requests DISABLE TRIGGER usage_requests_stage');await assert.rejects(store.verifySchema(),e=>e.code==='database_migration_conflict');}
 finally{await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();}
});
