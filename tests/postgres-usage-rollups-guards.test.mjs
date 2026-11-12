import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateUsageRollups,verifyUsageRollups,usageRollupMoneySql,backfillUsageRollups,reconcileUsageRollups,disableUsageRollups} from '../postgres-usage-rollups.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立PG测试库':false,timeout:15000};
function options(name='postgres'){const u=new URL(url);u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:2,statementTimeoutMs:3000};}
function gate(){let resolve;const promise=new Promise(r=>resolve=r);return {promise,resolve};}
async function fixture(run,{beforeInstall}={}){
 const admin=new PostgresStore(options()),name='harbor_guard_'+randomUUID().replaceAll('-','');let store,other;
 try{await admin.query('CREATE DATABASE '+name);store=new PostgresStore(options(name));other=new PostgresStore(options(name));await store.migrate();const role=await createUsageRollupRuntime(store);await store.query("INSERT INTO harbor.members(id,name,created) VALUES('m','synthetic',0)");await store.query("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('k','m','synthetic','synthetic',0)");
  await beforeInstall?.({store,other,role,name});await migrateUsageRollups(store,{apply:true});await run({store,other,role,name});
 }finally{await other?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();}
}
const initial=async({store})=>{await store.query("INSERT INTO harbor.requests(id,member_id,key_id,started,status,ip) VALUES('r','m','k',0,'ok','synthetic')");await store.query("INSERT INTO harbor.request_prices(request_id,state,total_atoms,measured_at) VALUES('r','priced',100,0)");};

test('PG受控runtime安装补齐TEMP与最小权限，observe/finish/级联删除在未ready和disabled均正常',integration,()=>fixture(async({store,role})=>{
 await store.initializePricing();const asRuntime=operation=>store.transaction(async()=>{await store.query('SET LOCAL ROLE "'+role+'"');return operation();});
 const permissions=await store.get("SELECT has_database_privilege($1,current_database(),'TEMP') temporary,has_schema_privilege($1,'harbor','CREATE') schema_create,has_table_privilege($1,'harbor.usage_rollup_state','UPDATE') state_write,has_table_privilege($1,'harbor.usage_contributions','DELETE') ledger_delete",role);assert.deepEqual(permissions,{temporary:true,schema_create:false,state_write:false,ledger_delete:false});
 const exposed=await store.get("SELECT count(*) n FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE n.nspname='harbor' AND p.proname IN ('usage_prepare_staging','usage_stage_source','usage_add_delta','flush_usage_rollups','usage_flush_deferred','usage_source_guard','usage_reject_truncate') AND a.grantee=0 AND a.privilege_type='EXECUTE'");assert.equal(exposed.n,0);
 for(const disabled of [false,true]){if(disabled)await disableUsageRollups(store);const id=await asRuntime(()=>store.observe({id:'m',key_id:'k'},'synthetic','synthetic','gpt-6.1-sol',{at:1000}));await asRuntime(()=>store.finish(id,{input:1000,output:100,cached:100,usageKnown:true},2000));assert.equal((await reconcileUsageRollups(store)).valid,true);await asRuntime(()=>store.run('DELETE FROM harbor.requests WHERE id=$1',id));assert.equal((await reconcileUsageRollups(store)).valid,true);}
},{beforeInstall:async({store,role})=>assert.equal((await store.get("SELECT has_database_privilege($1,current_database(),'TEMP') temporary",role)).temporary,false)}));

for(const kind of ['request','price'])test('PG首次ledger缺失并发删除'+kind+'保留MVCC见证，旧回填40001且不复活',integration,()=>fixture(async({store,other})=>{
 if(kind==='price')await store.run("INSERT INTO harbor.usage_contributions(request_id) VALUES('r')");
 const seen=gate(),release=gate(),query=store.query.bind(store);let armed=true;store.query=async(sql,args)=>{const result=await query(sql,args);if(armed&&sql.startsWith('SELECT r.id FROM harbor.requests')){armed=false;seen.resolve();await release.promise;}return result;};
 const pending=backfillUsageRollups(store);try{await seen.promise;await other.run(kind==='request'?"DELETE FROM harbor.requests WHERE id='r'":"DELETE FROM harbor.request_prices WHERE request_id='r'");release.resolve();await assert.rejects(pending,e=>e.code==='40001');}finally{release.resolve();await pending.catch(()=>{});store.query=query;}
 await backfillUsageRollups(store);assert.equal((await reconcileUsageRollups(store,{publishReady:true})).valid,true);assert.equal((await store.get('SELECT count(*) n FROM harbor.request_prices')).n,0);if(kind==='request')assert.equal((await store.get('SELECT count(*) n FROM harbor.usage_hour_buckets')).n,0);
},{beforeInstall:initial}));

test('PG跨schema同名函数改绑触发器不能通过结构验证',integration,()=>fixture(async({store})=>{
 await assert.rejects(store.transaction(async()=>{await store.query("CREATE SCHEMA synthetic_evil;CREATE FUNCTION synthetic_evil.usage_stage_source() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NULL;END$$;DROP TRIGGER usage_requests_stage ON harbor.requests;CREATE TRIGGER usage_requests_stage AFTER INSERT OR DELETE OR UPDATE OF id,member_id,key_id,account_id,started,ended,price_version ON harbor.requests FOR EACH ROW EXECUTE FUNCTION synthetic_evil.usage_stage_source()");await verifyUsageRollups(store);}),e=>e.code==='database_migration_conflict');await verifyUsageRollups(store);
}));

test('PG默认verify在只读连接成功，长对账不持state锁阻塞disable',integration,()=>fixture(async({store,other,name})=>{
 const readOnly=new PostgresStore({...options(name),connection:{options:'-c default_transaction_read_only=on'}});try{assert.equal((await reconcileUsageRollups(readOnly)).valid,true);}finally{await readOnly.close();}
 const seen=gate(),release=gate(),query=store.query.bind(store);let armed=true;store.query=async(sql,args)=>{const result=await query(sql,args);if(armed&&sql.startsWith('WITH actual AS')){armed=false;seen.resolve();await release.promise;}return result;};const pending=reconcileUsageRollups(store);
 try{await seen.promise;await other.transaction(async()=>{await other.query("SET LOCAL lock_timeout='100ms'");await disableUsageRollups(other);});}finally{release.resolve();await pending;store.query=query;}assert.equal((await store.get('SELECT state FROM harbor.usage_rollup_state')).state,'disabled');
}));

test('PG拒绝源TRUNCATE，整事务更新回滚且disabled也不解除保护',integration,()=>fixture(async({store})=>{
 await initial({store});for(const target of ['harbor.requests CASCADE','harbor.request_prices']){await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.members SET name='changed' WHERE id='m'");await store.query('TRUNCATE '+target);}),e=>e.code==='55000'&&e.message.includes('TRUNCATE'));assert.equal((await store.get("SELECT name FROM harbor.members WHERE id='m'")).name,'synthetic');assert.equal((await store.get("SELECT ended FROM harbor.requests WHERE id='r'")).ended,null);assert.equal((await reconcileUsageRollups(store)).valid,true);}await disableUsageRollups(store);await assert.rejects(store.query('TRUNCATE harbor.request_prices'),e=>e.code==='55000');
}));

test('PG提前flush后拒绝后续源写、事务完整回滚、下一默认事务仍可完成r+p',integration,()=>fixture(async({store})=>{
 await initial({store});for(const sql of ["UPDATE harbor.requests SET ended=7200000 WHERE id='r'","UPDATE harbor.request_prices SET total_atoms=999 WHERE request_id='r'"]){await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.requests SET ended=3600000 WHERE id='r'");await store.query('SET CONSTRAINTS ALL IMMEDIATE');await store.query(sql);}),e=>e.code==='55000'&&e.message.includes('禁止继续修改'));assert.equal((await store.get("SELECT ended FROM harbor.requests WHERE id='r'")).ended,null);assert.equal((await store.get("SELECT total_atoms::text total FROM harbor.request_prices WHERE request_id='r'")).total,'100');}
 await store.transaction(async()=>{await store.run("UPDATE harbor.requests SET ended=7200000 WHERE id='r'");await store.run("UPDATE harbor.request_prices SET total_atoms=200 WHERE request_id='r'");});assert.equal((await reconcileUsageRollups(store)).valid,true);
}));

test('PG受信角色不可被覆盖，unsafe/缺角色安装原子拒绝，显式安全role可安装',integration,async()=>{
 const admin=new PostgresStore(options()),name='harbor_guard_'+randomUUID().replaceAll('-','');let store;
 try{await admin.query('CREATE DATABASE '+name);store=new PostgresStore(options(name));await store.migrate();const role=await createUsageRollupRuntime(store);await assert.rejects(migrateUsageRollups(store,{apply:true,runtimeRole:'postgres'}),e=>e.code==='usage_rollup_runtime_invalid');assert.equal((await store.get("SELECT to_regclass('harbor.usage_hour_buckets') name")).name,null);await store.query('DROP TABLE harbor_operator.activation');await assert.rejects(migrateUsageRollups(store,{apply:true}),e=>e.code==='usage_rollup_runtime_required');const user=(await store.get('SELECT current_user name')).name;await assert.rejects(migrateUsageRollups(store,{apply:true,runtimeRole:user}),e=>e.code==='usage_rollup_runtime_invalid');assert.equal((await store.get("SELECT to_regclass('harbor.usage_hour_buckets') name")).name,null);assert.equal(await migrateUsageRollups(store,{apply:true,runtimeRole:role}),true);await verifyUsageRollups(store);}
 finally{await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.query('DROP ROLE IF EXISTS '+name+'_runtime');await admin.close();}
});

test('PG函数STRICT/稳定性/并行属性篡改拒启动，正常缺投影仍完整计数',integration,()=>fixture(async({store})=>{
 for(const attribute of ['STRICT','STABLE','PARALLEL SAFE']){
  await assert.rejects(store.transaction(async()=>{await store.query('ALTER FUNCTION harbor.usage_add_delta(jsonb,jsonb,integer) '+attribute);await store.verifySchema();}),error=>error.code==='database_migration_conflict');
  await store.verifySchema();
 }
 await store.query("INSERT INTO harbor.requests(id,member_id,started,status,ip) VALUES('missing-price','m',1,'ok','synthetic')");
 assert.equal((await reconcileUsageRollups(store,{publishReady:true})).valid,true);
 const row=await store.get(usageRollupMoneySql('member'),['m'],0,3600000,0,0);assert.equal(row.requests,1);assert.equal(row.unpriced,1);assert.equal(row.known,0);
}));
