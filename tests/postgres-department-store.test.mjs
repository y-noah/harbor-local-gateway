import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer,requireDepartmentScope} from '../department-access.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需独立PG18':false};
function options(database){if(url){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,password:process.env.HARBOR_TEST_PG_PASSWORD,database:database??'postgres'}};}
async function fixture(t,{install=true}={}){
 const database='harbor_ds_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});await root.query('CREATE DATABASE '+database);const store=new PostgresStore({...options(database),poolSize:4});let admin;t.after(async()=>{await admin?.close();await store.close();await root.query('DROP DATABASE '+database);await root.close();});await store.migrate();await store.initializePricing();if(install)await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer,clock:()=>1000});let a,b,scopeA,scopeB;
 if(install){a=await departments.createDepartment(superAdmin,{name:'合成A'});b=await departments.createDepartment(superAdmin,{name:'合成B'});scopeA=issuer.forDepartment(superAdmin,a.id);scopeB=issuer.forDepartment(superAdmin,b.id);}
 admin=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32),clock:()=>1000});const create=async scope=>(await admin.handle('/api/admin/members','POST',{name:'合成员工',quota:null,expires:null},undefined,{scope})).value,created=await create(scopeA),other=install?await create(scopeB):null;
 return {database,store,departments,superAdmin,a,b,created,other,at:1000};
}
const observe=(store,member,extra={})=>store.observe(member,'synthetic','synthetic','gpt-6.1-sol',{at:1000,...extra});
test('真实认证原对象独占scope，observe隐式/显式归属写入且拒DTO与跨部门身份',integration,async t=>{
 const f=await fixture(t),member=await f.store.authenticate(f.created.secret,{at:f.at}),other=await f.store.authenticate(f.other.secret,{at:f.at}),scope=f.store.departmentScope(member);assert.equal(requireDepartmentScope(scope,{roles:['employee']}).department_id,f.a.id);assert.equal(scope.credential_id,f.created.keyId);assert.equal(scope.member_id,f.created.memberId);
 for(const copy of [{...member},JSON.parse(JSON.stringify(member))]){assert.throws(()=>f.store.departmentScope(copy),{code:'department_scope_required'});await assert.rejects(observe(f.store,copy),{code:'department_scope_required'});}
 const request=await observe(f.store,member);assert.deepEqual(await f.store.get('SELECT department_id,member_id,key_id FROM harbor.requests WHERE id=$1',request),{department_id:f.a.id,member_id:f.created.memberId,key_id:f.created.keyId});
 await assert.rejects(observe(f.store,member,{scope:f.store.departmentScope(other)}),{code:'department_forbidden'});await assert.rejects(observe(f.store,member,{scope:JSON.parse(JSON.stringify(scope))}),{code:'department_scope_required'});
 const explicit=await observe(f.store,{...member},{scope});assert.equal((await f.store.get('SELECT department_id FROM harbor.requests WHERE id=$1',explicit)).department_id,f.a.id);
 const transient=await f.store.authenticate(f.created.secret,{at:f.at}),originalQuery=f.store.query.bind(f.store);let changed=false;
 try{f.store.query=async(sql,args)=>{const result=await originalQuery(sql,args);if(!changed&&sql.startsWith('SELECT id,usage_type_id,usage_type_version FROM harbor.members')){changed=true;transient.id=other.id;transient.key_id=other.key_id;transient.department_id=other.department_id;}return result;};const stable=await observe(f.store,transient);assert.deepEqual(await f.store.get('SELECT department_id,member_id,key_id FROM harbor.requests WHERE id=$1',stable),{department_id:f.a.id,member_id:f.created.memberId,key_id:f.created.keyId});}finally{f.store.query=originalQuery;}
 const second=new PostgresStore({...options(f.database),poolSize:1});try{const same=await second.authenticate(f.created.secret,{at:f.at});assert.throws(()=>f.store.departmentScope(same),{code:'department_scope_required'});assert.throws(()=>second.departmentScope(member),{code:'department_scope_required'});}finally{await second.close();}
 member.key_id=other.key_id;assert.throws(()=>f.store.departmentScope(member),{code:'department_forbidden'});
});

test('成员/Key/部门停用恢复分别使旧scope失效，key版本alias不借用member版本',integration,async t=>{
 const f=await fixture(t),initial=await f.store.authenticate(f.created.secret,{at:f.at});await f.store.run('UPDATE harbor.members SET enabled=0 WHERE id=$1',initial.id);await assert.rejects(f.store.authenticate(f.created.secret,{at:f.at}),{code:'key_disabled'});await f.store.run('UPDATE harbor.members SET enabled=1 WHERE id=$1',initial.id);await assert.rejects(observe(f.store,initial),{code:'department_forbidden'});
 const memberFresh=await f.store.authenticate(f.created.secret,{at:f.at}),memberScope=f.store.departmentScope(memberFresh);assert.equal(memberFresh.access_version,2);assert.equal(memberFresh.key_access_version,0);assert.equal(memberScope.member_version,2);assert.equal(memberScope.credential_version,0);await observe(f.store,memberFresh);
 await f.store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',initial.key_id);await assert.rejects(f.store.authenticate(f.created.secret,{at:f.at}),{code:'key_disabled'});await f.store.run('UPDATE harbor.keys SET enabled=1 WHERE id=$1',initial.key_id);await assert.rejects(observe(f.store,memberFresh),{code:'department_forbidden'});const keyFresh=await f.store.authenticate(f.created.secret,{at:f.at});assert.equal(f.store.departmentScope(keyFresh).credential_version,2);
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await assert.rejects(f.store.authenticate(f.created.secret,{at:f.at}),{code:'department_forbidden'});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});await assert.rejects(observe(f.store,keyFresh),{code:'department_forbidden'});const newest=await f.store.authenticate(f.created.secret,{at:f.at});assert.equal(f.store.departmentScope(newest).department_version,2);await observe(f.store,newest);
 await f.store.run('UPDATE harbor.keys SET expires=$1 WHERE id=$2',f.at,initial.key_id);await assert.rejects(f.store.authenticate(f.created.secret,{at:f.at}),{code:'key_expired'});await assert.rejects(f.store.authenticate('synthetic-unknown'),{code:'invalid_api_key'});
});

test('legacy认证/普通DTO观察保持，热安装和partial fail closed，初始连接错误可重试',integration,async t=>{
 const f=await fixture(t,{install:false}),member=await f.store.authenticate(f.created.secret,{at:f.at});assert.equal(f.store.departmentScope(member),undefined);await observe(f.store,{...member});await migrateDepartmentIsolation(f.store,{apply:true});await assert.rejects(f.store.authenticate(f.created.secret,{at:f.at}),{code:'department_restart_required'});await assert.rejects(observe(f.store,member),{code:'department_restart_required'});
 const retry=new PostgresStore({...options(f.database),poolSize:1});try{const original=retry.get.bind(retry);let once=true;retry.get=(...args)=>{if(once){once=false;return Promise.reject(Object.assign(Error('synthetic connection failure'),{code:'ECONNRESET'}));}return original(...args);};await assert.rejects(retry.authenticate(f.created.secret,{at:f.at}),{code:'ECONNRESET'});const success=await retry.authenticate(f.created.secret,{at:f.at});assert.equal(retry.departmentScope(success).department_id,'department-default');}finally{await retry.close();}
 const partial=await fixture(t,{install:false});await partial.store.query('CREATE TABLE harbor.departments(id text)');await assert.rejects(partial.store.authenticate(partial.created.secret,{at:partial.at}),{code:'department_migration_required'});
});

test('真实两连接认证先dept再member再Key，observe锁身份期间停用等待且无死锁',integration,async t=>{
 const f=await fixture(t),authStore=new PostgresStore({...options(f.database),poolSize:1}),writer=new PostgresStore({...options(f.database),poolSize:1});let release,locked,authResult,writeResult;
 const held=new Promise(resolve=>{locked=resolve;}),gate=new Promise(resolve=>{release=resolve;});
 try{
  await authStore.query("SET application_name='synthetic-store-department-auth'");const original=authStore.query.bind(authStore);authStore.query=async(sql,args)=>{const result=await original(sql,args);if(sql.startsWith('SELECT * FROM harbor.members WHERE id=$1 AND department_id=$2')&&sql.endsWith('FOR UPDATE')){locked();await gate;}return result;};
  authResult=authStore.transaction(async()=>{const member=await authStore.authenticate(f.created.secret,{at:f.at,exclusive:true});return observe(authStore,member);}).then(value=>({value}),error=>({error}));await held;
  const pid=(await f.store.get("SELECT pid FROM pg_stat_activity WHERE application_name='synthetic-store-department-auth'")).pid;assert.equal((await f.store.get("SELECT count(*) n FROM pg_locks WHERE pid=$1 AND relation='harbor.departments'::regclass AND mode='RowShareLock'",pid)).n,1);
  writeResult=writer.transaction(()=>writer.run('UPDATE harbor.departments SET enabled=false,version=version+1 WHERE id=$1',f.a.id)).then(value=>({value}),error=>({error}));const deadline=Date.now()+5000;for(;;){const waiting=await f.store.get("SELECT count(*) n FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'UPDATE harbor.departments SET enabled=false%'");if(waiting.n>0)break;assert.ok(Date.now()<deadline,'部门停用应等身份锁');await new Promise(resolve=>setTimeout(resolve,10));}
  release();const [authenticated,disabled]=await Promise.all([authResult,writeResult]);assert.equal(authenticated.error,undefined);assert.equal(disabled.error,undefined);assert.equal((await f.store.get('SELECT department_id FROM harbor.requests WHERE id=$1',authenticated.value)).department_id,f.a.id);await assert.rejects(authStore.authenticate(f.created.secret,{at:f.at}),{code:'department_forbidden'});
 }finally{release?.();await Promise.all([authResult,writeResult]);await writer.close();await authStore.close();}
});
