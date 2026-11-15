import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID,randomBytes} from 'node:crypto';
import {AppError} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation,verifyDepartmentIsolationAvailability} from '../postgres-departments.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createPostgresDepartmentHttp} from '../postgres-department-http.mjs';
import {createPostgresAccountAdmin} from '../postgres-account-admin.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {mkdtemp,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立真实PG':false,timeout:60000};
const database=name=>{const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:8};};
async function fixture(t,overrides={}){
 const cleanup=[];const after=fn=>cleanup.push(fn);t.after(async()=>{for(const fn of cleanup.reverse())await fn();});
 const root=new PostgresStore(database()),name='harbor_dept_account_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')};let auth;const departments=createPostgresDepartments(store,{issuer,authorizeSuperAdmin:credential=>auth.authorizeSuperAdmin(credential)});auth=createAdminAuth(config,{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}});
 const login=async secret=>{const res={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};await auth.loginAsync(secret,res);return res.cookie;};const cookies={root:await login('synthetic-root')},superAdmin=await auth.context({headers:{cookie:cookies.root}}),rows={},keys={};
 for(const label of ['a','b']){rows[label]=await departments.createDepartment(superAdmin,{name:'合成类型部门'+label});keys[label]=await departments.issueManagementKey(superAdmin,rows[label].id,{label:'合成管理Key'});cookies[label]=await login(keys[label].secret);}
 const body=async req=>{let text='';for await(const chunk of req)text+=chunk;return text?JSON.parse(text):{};},boundary=createPostgresDepartmentHttp({auth,departments,issuer,installed:true,readBody:body}),runtime=await mkdtemp(join(tmpdir(),'harbor-dept-account-')),admin=createPostgresAccountAdmin(store,{runtime,encryptionKey:randomBytes(32),router:createPostgresRouter(store),checkApi:async()=>[{slug:'gpt-6.1-sol'}],checkSubscription:async()=>true,readQuota:async()=>({rateLimits:{planType:'synthetic-plan',primary:{usedPercent:25,windowDurationMins:10080,resetsAt:(Date.now()+86400000)/1000}}}),...overrides});after(()=>rm(runtime,{recursive:true,force:true}));
 const server=http.createServer((req,res)=>{(async()=>{const scope=await boundary.context(req),path=new URL(req.url,'http://localhost'),result=await admin.handle(path.pathname,req.method,await body(req),{scope,revalidateScope:original=>auth.revalidateScope(req,original)});res.writeHead(result?.status??404,{'content-type':'application/json'});res.end(JSON.stringify(result?.value??{}));})().catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));after(async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await admin.close();});
 const call=async(label,path='/api/admin/accounts',method='GET',payload,department)=>{const response=await fetch('http://127.0.0.1:'+server.address().port+path,{method,headers:{cookie:cookies[label],...(department?{'x-harbor-department':department}:{}),'content-type':'application/json'},...(payload?{body:JSON.stringify(payload)}:{})});return {status:response.status,value:await response.json()};};
 return {cleanup:after,connection:database(name),store,departments,rows,keys,superAdmin,call,admin,auth,cookies,login};
}

import {createQueryDepartments} from '../postgres-query-departments.mjs';
const fullStructure=sql=>sql.startsWith('SELECT c.relname,a.attname,a.attnotnull');
function observe(store){const original=store.query.bind(store);let full=0,total=0;store.query=(sql,args)=>{total++;if(fullStructure(sql))full++;return original(sql,args);};return {read:()=>({full,total}),reset:()=>{full=0;total=0;}};}
test('同store已核验的套餐HTTP复用结构证明但保持DTO与授权实时校验',integration,async t=>{
 const f=await fixture(t);await verifyDepartmentIsolationAvailability(f.store);const count=observe(f.store);
 const cold=await f.call('a','/api/admin/account-packages');assert.equal(cold.status,200);assert.equal(count.read().full,0);const coldCount=count.read().total;count.reset();
 const warm=await f.call('a','/api/admin/account-packages');assert.deepEqual(warm,cold);assert.equal(count.read().full,0);assert.equal(count.read().total,coldCount);
 await f.store.run('UPDATE harbor.department_admin_keys SET enabled=false WHERE id=$1',f.keys.a.key.id);assert.ok([401,403].includes((await f.call('a','/api/admin/account-packages')).status));
 await f.store.run('UPDATE harbor.departments SET enabled=false WHERE id=$1',f.rows.b.id);assert.ok([401,403].includes((await f.call('b','/api/admin/account-packages')).status));
});
test('新store并发查询只核验一次且不同store独立，初始化瞬态失败可重试',integration,async t=>{
 const f=await fixture(t),scope=await f.auth.context({headers:{cookie:f.cookies.a}}),a=new PostgresStore(f.connection),b=new PostgresStore(f.connection);f.cleanup(async()=>{await a.close();await b.close();});
 const ca=observe(a),cb=observe(b);await Promise.all(Array.from({length:6},()=>createQueryDepartments(a).resolve(scope)));assert.equal(ca.read().full,1);
 await createQueryDepartments(b).resolve(scope);assert.equal(cb.read().full,1);
 const c=new PostgresStore(f.connection);f.cleanup(()=>c.close());const original=c.query.bind(c);let failed=false;c.query=(sql,args)=>{if(!failed){failed=true;throw Object.assign(Error('synthetic temporary connection failure'),{code:'ECONNRESET'});}return original(sql,args);};
 const query=createQueryDepartments(c);await assert.rejects(query.resolve(scope),{code:'ECONNRESET'});assert.equal((await query.resolve(scope)).department_id,scope.department_id);
});
test('结构证据不得越过部门或quota marker漂移，也不替代显式完整核验',integration,async t=>{
 const f=await fixture(t),scope=await f.auth.context({headers:{cookie:f.cookies.a}}),query=createQueryDepartments(f.store);await query.resolve(scope);
 for(const name of ['schema.department-isolation','schema.quota-subject-coordination']){
  await assert.rejects(f.store.transaction(async()=>{await f.store.run('UPDATE harbor.gateway_settings SET value=$1 WHERE name=$2','{}',name);await assert.rejects(query.resolve(scope),{code:'department_restart_required'});throw Error('rollback marker');}),/rollback marker/);
 }
 await assert.rejects(f.store.transaction(async()=>{await f.store.query('ALTER TABLE harbor.quota_subject_confirmations RENAME TO synthetic_missing_quota');await assert.rejects(query.resolve(scope),{code:'department_restart_required'});throw Error('rollback quota');}),/rollback quota/);
 await assert.rejects(f.store.transaction(async()=>{await f.store.query('ALTER TABLE harbor.requests DROP CONSTRAINT department_requests_member_fk');await assert.rejects(verifyDepartmentIsolationAvailability(f.store),{code:'department_migration_conflict'});await assert.rejects(query.resolve(scope),{code:'department_migration_conflict'});throw Error('rollback structure');}),/rollback structure/);
 await query.resolve(scope);
});
test('缓存结构后仍拒绝数据库角色漂移与实时quota表权限撤销',integration,async t=>{
 const f=await fixture(t),scope=await f.auth.context({headers:{cookie:f.cookies.a}}),query=createQueryDepartments(f.store),role='query_guard_'+randomUUID().replaceAll('-',''),control=new PostgresStore(f.connection);f.cleanup(()=>control.close());
 await f.store.query('CREATE ROLE '+role+' NOLOGIN');f.cleanup(async()=>{await control.query('DROP OWNED BY '+role);await control.query('DROP ROLE '+role);});await f.store.query('GRANT USAGE ON SCHEMA harbor TO '+role);await f.store.query('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA harbor TO '+role);
 await query.resolve(scope);
 await f.store.transaction(async()=>{await f.store.query('SET LOCAL ROLE '+role);await assert.rejects(query.resolve(scope),{code:'department_restart_required'});});
 await f.store.transaction(async()=>{await f.store.query('SET LOCAL ROLE '+role);assert.equal(await verifyDepartmentIsolationAvailability(f.store),true);await query.resolve(scope);await control.query('REVOKE UPDATE ON harbor.quota_subjects FROM '+role);await assert.rejects(query.resolve(scope),{code:'department_restart_required'});});
 assert.equal((await query.resolve(scope)).department_id,scope.department_id);
});

test('未安装store的旧查询实例不能通过热迁移自动取得部门能力',integration,async t=>{
 const root=new PostgresStore(database()),name='query_legacy_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));
 try{await store.migrate();await store.initializePricing();const reader=createQueryDepartments(store);assert.equal(await reader.resolve(undefined),null);await migrateDepartmentIsolation(store,{apply:true});await assert.rejects(reader.resolve(undefined),{code:'department_restart_required'});
 const scope=createDepartmentScopeIssuer().superAdmin({credentialId:'synthetic-root',credentialVersion:0});assert.equal((await createQueryDepartments(store).resolve(scope)).view,'global');
 }finally{await store.close();await root.query('DROP DATABASE '+name);await root.close();}
});
for(const outcome of ['rollback','commit-failure','outside-reader-before-rollback','transaction-initialization-inflight'])test('事务内核验证据不可共享给新查询：'+outcome,integration,async()=>{
 const root=new PostgresStore(database()),name='query_uncommitted_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));let transaction,release;
 try{
  await store.migrate();await store.initializePricing();
  if(outcome==='outside-reader-before-rollback'||outcome==='transaction-initialization-inflight'){
   let entered;const ready=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);
   transaction=store.transaction(async()=>{await migrateDepartmentIsolation(store,{apply:true});
    if(outcome==='transaction-initialization-inflight'){
     const original=store.query.bind(store);let paused=false;store.query=async(sql,args)=>{if(!paused&&store.context.getStore()&&sql.startsWith("SELECT to_regclass('harbor.gateway_settings')")){paused=true;entered();await gate;}return original(sql,args);};
     const scope=createDepartmentScopeIssuer().superAdmin({credentialId:'synthetic-root',credentialVersion:0});await createQueryDepartments(store).resolve(scope);
    }else{entered();await gate;}
    throw Error('synthetic rollback');}).then(()=>null,error=>error);
   await ready;try{assert.equal(await createQueryDepartments(store).resolve(undefined),null);}finally{release();}
   assert.match((await transaction).message,/synthetic rollback/);transaction=null;
  }else{
   await assert.rejects(store.transaction(async()=>{await migrateDepartmentIsolation(store,{apply:true});
    if(outcome==='rollback')throw Error('synthetic rollback');
    await store.query('CREATE TABLE public.synthetic_deferred(id integer PRIMARY KEY,parent integer REFERENCES public.synthetic_deferred(id) DEFERRABLE INITIALLY DEFERRED)');await store.query('INSERT INTO public.synthetic_deferred VALUES(1,2)');
   }),outcome==='rollback'?/synthetic rollback/:{code:'23503'});
  }
  assert.equal(await createQueryDepartments(store).resolve(undefined),null);
 }finally{release?.();await transaction;await store.close();await root.query('DROP DATABASE '+name);await root.close();}
});

for(const outcome of ['commit','rollback','concurrent'])test('角色事务核验不能污染已有热查询：'+outcome,integration,async t=>{
 const f=await fixture(t),scope=await f.auth.context({headers:{cookie:f.cookies.a}}),query=createQueryDepartments(f.store),role='query_local_'+randomUUID().replaceAll('-','');
 await f.store.query('CREATE ROLE '+role+' NOLOGIN');f.cleanup(async()=>{await f.store.query('DROP OWNED BY '+role);await f.store.query('DROP ROLE '+role);});await f.store.query('GRANT USAGE ON SCHEMA harbor TO '+role);await f.store.query('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA harbor TO '+role);
 await query.resolve(scope);let transaction,release;
 try{
  let entered;const ready=new Promise(resolve=>entered=resolve),gate=new Promise(resolve=>release=resolve);
  transaction=f.store.transaction(async()=>{await f.store.query('SET LOCAL ROLE '+role);await assert.rejects(query.resolve(scope),{code:'department_restart_required'});assert.equal(await verifyDepartmentIsolationAvailability(f.store),true);await query.resolve(scope);entered();if(outcome==='concurrent')await gate;if(outcome==='rollback')throw Error('synthetic role rollback');}).then(()=>null,error=>error);
  if(outcome==='concurrent'){await ready;try{assert.equal((await query.resolve(scope)).department_id,scope.department_id);}finally{release();}}
  const error=await transaction;transaction=null;if(outcome==='rollback')assert.match(error?.message,/synthetic role rollback/);else assert.equal(error,null);
  assert.equal((await query.resolve(scope)).department_id,scope.department_id);
 }finally{release?.();await transaction;}
});
for(const scenario of ['inherit','child-rollback','child-commit','child-failure','siblings'])test('嵌套事务证明沿真实祖先链继承且不反向发布：'+scenario,integration,async t=>{
 const f=await fixture(t),scope=await f.auth.context({headers:{cookie:f.cookies.a}}),query=createQueryDepartments(f.store),roles=['parent','child'].map(label=>'query_'+label+'_'+randomUUID().replaceAll('-',''));
 for(const role of roles){await f.store.query('CREATE ROLE '+role+' NOLOGIN');f.cleanup(async()=>{await f.store.query('DROP OWNED BY '+role);await f.store.query('DROP ROLE '+role);});await f.store.query('GRANT USAGE ON SCHEMA harbor TO '+role);await f.store.query('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA harbor TO '+role);}
 await query.resolve(scope);
 await f.store.transaction(async()=>{
  await f.store.query('SET LOCAL ROLE '+roles[0]);await verifyDepartmentIsolationAvailability(f.store);await query.resolve(scope);
  const childRole=async()=>{await f.store.query('SET LOCAL ROLE '+roles[1]);await assert.rejects(query.resolve(scope),{code:'department_restart_required'});await verifyDepartmentIsolationAvailability(f.store);await query.resolve(scope);};
  if(scenario==='inherit')await f.store.transaction(()=>f.store.transaction(async()=>{assert.equal((await query.resolve(scope)).department_id,scope.department_id);}));
  if(scenario==='child-rollback'){await assert.rejects(f.store.transaction(async()=>{await childRole();throw Error('synthetic child rollback');}),/synthetic child rollback/);await query.resolve(scope);}
  if(scenario==='child-commit'){await f.store.transaction(childRole);await assert.rejects(query.resolve(scope),{code:'department_restart_required'});await verifyDepartmentIsolationAvailability(f.store);await query.resolve(scope);}
  if(scenario==='child-failure'){
   await assert.rejects(f.store.transaction(async()=>{await f.store.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.department-isolation'");await assert.rejects(verifyDepartmentIsolationAvailability(f.store),{code:'department_migration_conflict'});await assert.rejects(query.resolve(scope),{code:'department_migration_conflict'});throw Error('synthetic child failure');}),/synthetic child failure/);await query.resolve(scope);
  }
  if(scenario==='siblings'){
   const results=await Promise.allSettled([f.store.transaction(async()=>{await childRole();throw Error('synthetic sibling rollback');}),f.store.transaction(()=>query.resolve(scope))]);assert.equal(results[0].status,'rejected');assert.match(results[0].reason.message,/synthetic sibling rollback/);assert.equal(results[1].status,'fulfilled');assert.equal(results[1].value.department_id,scope.department_id);await query.resolve(scope);
  }
 });
 assert.equal((await query.resolve(scope)).department_id,scope.department_id);
});
