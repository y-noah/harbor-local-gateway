import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresAudit} from '../postgres-audit.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {setTimeout as delay} from 'node:timers/promises';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {createPostgresRequestQueries} from '../postgres-request-query.mjs';
import {createPostgresTypeStatistics} from '../postgres-type-statistics.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateUsageRollups,backfillUsageRollups,reconcileUsageRollups} from '../postgres-usage-rollups.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立合成PG测试库':false,timeout:120000};
const at=Date.parse('2026-11-13T12:00:00+08:00');
async function until(check){const deadline=Date.now()+5000;while(!check()){assert.ok(Date.now()<deadline,'查询未在期限内达到目标状态');await delay(5);}}
function options(name){const u=new URL(url);if(name)u.pathname='/'+name;return {connectionString:u.href,allowPlaintextLocalNetwork:true,poolSize:1,statementTimeoutMs:15000};}
const query={page:1,pageSize:20,view:'all',member:'',model:'',status:''};
async function fixture(run){
 const admin=new PostgresStore(options()),database='harbor_dept_queries_'+randomUUID().replaceAll('-','');let created=false,store;const readers=[];
 try{
  await admin.query('CREATE DATABASE '+database);created=true;store=new PostgresStore(options(database));await store.migrate();
  const state=()=>{const r=createPostgresStateQueries(store,{clock:()=>at});readers.push(r);return r;},requests=()=>{const r=createPostgresRequestQueries(store,{clock:()=>at});readers.push(r);return r;},types=()=>{const r=createPostgresTypeStatistics(store,{clock:()=>at});readers.push(r);return r;};
  await run({store,state,requests,types});
 }finally{for(const r of readers)await r.close();await store?.close();if(created){await admin.query('DROP DATABASE '+database);await admin.query('DROP ROLE IF EXISTS '+database+'_runtime');}await admin.close();}
}
async function seed(store){
 await store.query("INSERT INTO harbor.pricing_versions(id,created,book) VALUES('synthetic-price',0,'{}')");
 for(const suffix of ['a','b']){
  const department='dept-'+suffix;await store.run('INSERT INTO harbor.departments(id,name,created,updated) VALUES($1,$2,$3,$3)',department,'同名部门',at);
  await store.run('INSERT INTO harbor.department_admin_keys(id,department_id,digest,prefix,label,created) VALUES($1,$2,$3,$4,$4,$5)','admin-'+suffix,department,'f'.repeat(63)+suffix,'synthetic-'+suffix,at);
  await store.run("INSERT INTO harbor.member_types(id,code,name,description,department_id) VALUES($1,'A','同名类型','synthetic',$2)",'type-'+suffix,department);
  await store.run("INSERT INTO harbor.members(id,name,created,usage_type_id,department_id) VALUES($1,'同名员工',$2,$3,$4)",'member-'+suffix,at,'type-'+suffix,department);
  await store.run("INSERT INTO harbor.accounts(id,name,home,department_id) VALUES($1,'同名账号','synthetic',$2)",'account-'+suffix,department);
  await store.run("INSERT INTO harbor.accounts(id,name,home,removed_at,department_id) VALUES($1,'历史账号','synthetic',$2,$3)",'removed-'+suffix,at,department);
  await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created,department_id) VALUES($1,$2,$3,'synthetic',$4,$5)",'key-'+suffix,'member-'+suffix,'synthetic-'+suffix,at,department);
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,protocol,department_id) VALUES($1,$2,$3,$4,$4,'responses',$5)",'session-'+suffix,'member-'+suffix,'account-'+suffix,at,department);
  for(let n=0;n<26;n++){
   const id='request-'+suffix+'-'+n,account=n===25?null:n===24?'removed-'+suffix:'account-'+suffix;
   await store.run("INSERT INTO harbor.requests(id,member_id,key_id,account_id,session_id,started,ended,status,ip,model,price_version,usage_type_id,usage_type_version,usage_type_code,usage_type_name,department_id) VALUES($1,$2,$3,$4,$5,$6,$7,'ok','synthetic',$8,'synthetic-price',$9,1,'A','同名类型',$10)",id,'member-'+suffix,'key-'+suffix,account,n===25?null:'session-'+suffix,at-n-1,at-n,n%2?'model-shared':'model-'+suffix+'-only','type-'+suffix,department);
   await store.run("INSERT INTO harbor.request_prices(request_id,state,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at) VALUES($1,'priced','synthetic-price',1000000000000,0,0,0,1000000000000,$2)",id,at);
  }
 }
 const issuer=createDepartmentScopeIssuer();return {issuer,a:issuer.departmentAdmin({departmentId:'dept-a',credentialId:'admin-a',credentialVersion:0}),b:issuer.departmentAdmin({departmentId:'dept-b',credentialId:'admin-b',credentialVersion:0}),global:issuer.superAdmin({credentialId:'root',credentialVersion:0}),superA:issuer.superAdmin({departmentId:'dept-a',credentialId:'root',credentialVersion:0})};
}
const invisible=(value,suffix)=>{const text=JSON.stringify(value);for(const prefix of ['member-','account-','removed-','session-','request-','type-'])assert.ok(!text.includes(prefix+suffix),'must not disclose '+prefix+suffix);assert.ok(!text.includes('model-'+suffix+'-only'));};

test('部门查询范围先于总数、候选、分组和分页；假scope、跨部门快照与游标均拒绝',integration,()=>fixture(async({store,state,requests,types})=>{
 await migrateDepartmentIsolation(store,{apply:true});const auth=await seed(store),requestReader=requests(),typeReader=types(),stateReader=state();
 for(const scope of [undefined,null,{role:'super_admin',view:'global'},{...auth.a},auth.issuer.employee({departmentId:'dept-a',memberId:'member-a',memberVersion:0,departmentVersion:0,credentialId:'key-a',credentialVersion:0})]){
  await assert.rejects(requestReader.read(query,{scope}),e=>e.status===403);await assert.rejects(stateReader.read({view:'overview'},{scope}),e=>e.status===403);await assert.rejects(typeReader.read({},{scope}),e=>e.status===403);
 }
 const a=await requestReader.read(query,{scope:auth.a}),b=await requestReader.read(query,{scope:auth.b}),global=await requestReader.read(query,{scope:auth.global});
 assert.equal(a.summary.requests,26);assert.equal(b.summary.requests,26);assert.equal(global.summary.requests,52);assert.equal(a.pagination.total,26);assert.equal(a.rows.length,20);assert.deepEqual(a.models,['model-a-only','model-shared']);assert.deepEqual(a.members.map(r=>r.id),['member-a']);invisible(a,'b');invisible(b,'a');
 assert.equal(a.byMember.length,1);assert.equal(a.byMember[0].member_id,'member-a');
 await assert.rejects(requestReader.read({...query,snapshot:a.snapshot},{scope:auth.b}),e=>e.code==='invalid_request_snapshot');
 await assert.rejects(requestReader.read({...query,page:2,snapshot:a.snapshot,cursor:a.pagination.nextCursor},{scope:auth.b}),e=>e.code==='invalid_request_snapshot');
 const page=await requestReader.read({...query,page:2,snapshot:a.snapshot,cursor:a.pagination.nextCursor},{scope:auth.a});assert.equal(page.rows.length,6);invisible(page,'b');
 assert.equal((await requestReader.read({...query,member:'member-b'},{scope:auth.a})).summary.requests,0);
 for(const groupBy of ['type','member','model']){const value=await typeReader.read({groupBy,pageSize:20},{scope:auth.a});assert.equal(value.summary.requests,26);invisible(value,'b');}
 assert.equal((await typeReader.read({member:'member-b'},{scope:auth.a})).summary.requests,0);
 const superA=await requestReader.read(query,{scope:auth.superA});assert.equal(superA.summary.requests,26);invisible(superA,'b');
 auth.issuer.revoke(auth.a);await assert.rejects(requestReader.read(query,{scope:auth.a}),e=>e.code==='department_scope_required');
}));

test('state五视图与小时桶只聚合本部门，历史账号和无账号请求不漏且不跨部门',integration,()=>fixture(async({store,state})=>{
 await migrateDepartmentIsolation(store,{apply:true});const auth=await seed(store),reader=state();const baseline=new Map();
 for(const scope of [auth.a,auth.b])await createPostgresAudit(store,{clock:()=>at}).write(scope,{action:'member.update',detail:'member-'+scope.department_id.at(-1),subject:{kind:'member',id:'member-'+scope.department_id.at(-1)}});
 for(const view of ['overview','overview-cost','sessions','accounts','full']){
  const value=await reader.read({view,sessionPageSize:20},{scope:auth.a});baseline.set(view,value);invisible(value,'b');if(view==='overview')assert.equal(value.totals.requests,26);if(view==='full'){assert.equal(value.totals.requests,26);assert.equal(value.memberCount,1);assert.equal(value.keyCount,1);assert.ok(value.audit.length>0);for(const event of value.audit)assert.equal((await store.get('SELECT department_id FROM harbor.audit_scopes WHERE audit_id=$1',event.id)).department_id,auth.a.department_id);assert.ok(!Object.hasOwn(value,'balancing'));}if(view==='overview-cost')assert.equal(value.daily.reduce((n,r)=>n+r.requests,0),26);if(view==='sessions')assert.equal(value.sessionsPagination.total,1);
 }
 for(const size of [20,50,100]){const value=await reader.read({view:'sessions',sessionPageSize:size},{scope:auth.a});assert.equal(value.sessionsPagination.pageSize,size);assert.equal(value.sessionsPagination.total,1);invisible(value,'b');}
 const runtime=await createUsageRollupRuntime(store);await migrateUsageRollups(store,{apply:true,runtimeRole:runtime});for(let n=0;n<5;n++){const page=await backfillUsageRollups(store,{batchSize:1000});if(page.exhausted)break;}assert.equal((await reconcileUsageRollups(store,{publishReady:true})).ready,true);
 for(const view of ['overview-cost','accounts'])assert.deepEqual(await reader.read({view,sessionPageSize:20},{scope:auth.a}),baseline.get(view));
 assert.equal((await reader.read({view:'overview-cost'},{scope:auth.global})).daily.reduce((n,r)=>n+r.requests,0),52);
}));

test('状态请求在真实队列等待期间撤销或停用后不能返回数据，full缺owner明确拒绝',integration,()=>fixture(async({store})=>{
 await migrateDepartmentIsolation(store,{apply:true});const auth=await seed(store),queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:3}),reader=createPostgresStateQueries(store,{clock:()=>at,queryQueue:queue});
 let release;
 try{
  for(const kind of ['key','department']){
   const blocker=queue.run(()=>new Promise(resolve=>release=resolve)),pending=reader.read({view:'full',people:'none'},{scope:auth.a});await until(()=>queue.status().queued===1);
   if(kind==='key')await store.run("UPDATE harbor.department_admin_keys SET version=version+1 WHERE id='admin-a'");else await store.run("UPDATE harbor.departments SET enabled=FALSE WHERE id='dept-a'");
   release();await blocker;await assert.rejects(pending,{code:'department_forbidden'});await until(()=>queue.status().active===0);
   await store.run("UPDATE harbor.department_admin_keys SET version=0 WHERE id='admin-a'");
  }
  assert.equal((await reader.read({view:'full',people:'none'},{scope:auth.superA})).totals.requests,26);await until(()=>queue.status().active===0);
  await store.run("UPDATE harbor.departments SET enabled=TRUE WHERE id='dept-a'");
  const event=await createPostgresAudit(store,{clock:()=>at}).write(auth.a,{action:'member.update',detail:'member-a',subject:{kind:'member',id:'member-a'}});
  await assert.rejects(store.transaction(async()=>{
   await store.query('ALTER TABLE harbor.audit_scopes DISABLE TRIGGER audit_scope_immutable');await store.run('DELETE FROM harbor.audit_scopes WHERE audit_id=$1',event);
   for(const scope of [auth.a,auth.global])await assert.rejects(reader.read({view:'full',people:'none'},{scope}),{code:'audit_scope_required'});
   throw Error('synthetic rollback');
  }),/synthetic rollback/);
 }finally{release?.();await reader.close();}
}));

test('旧库原入口保留；热安装后旧reader拒绝运行直到重建，不能静默获得全局权限',integration,()=>fixture(async({store,state})=>{
 const legacy=state();assert.equal((await legacy.read({view:'overview'})).totals.requests,0);
 await migrateDepartmentIsolation(store,{apply:true});const issuer=createDepartmentScopeIssuer(),scope=issuer.superAdmin({credentialId:'root',credentialVersion:0});
 await assert.rejects(legacy.read({view:'overview'},{scope}),e=>e.code==='department_restart_required');
 const fresh=state();await assert.rejects(fresh.read({view:'overview'}),e=>e.code==='department_scope_required');assert.equal((await fresh.read({view:'overview'},{scope})).totals.requests,0);
}));

test('首次部门核验真实数据库超时后，下次请求可恢复且仍须可信scope',integration,()=>fixture(async({store,state})=>{
 await migrateDepartmentIsolation(store,{apply:true});const issuer=createDepartmentScopeIssuer(),scope=issuer.superAdmin({credentialId:'root',credentialVersion:0}),reader=state(),original=store.get.bind(store);let injected=false;
 store.get=async(sql,...args)=>{
  if(!injected&&sql.includes("to_regclass('harbor.gateway_settings')")){injected=true;return store.transaction(async()=>{await store.query("SELECT set_config('statement_timeout','1',true)");return original('SELECT pg_sleep(0.02)');});}
  return original(sql,...args);
 };
 try{await assert.rejects(reader.read({view:'overview'},{scope}),e=>e.code==='query_timeout');}finally{store.get=original;}
 assert.equal(injected,true);assert.equal((await reader.read({view:'overview'},{scope})).totals.requests,0);
 await assert.rejects(reader.read({view:'overview'}),e=>e.code==='department_scope_required');
}));
