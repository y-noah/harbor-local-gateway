import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresAccountAdmin} from '../postgres-account-admin.mjs';
import {hash} from '../store.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需独立PG18':false};
function options(database){if(url){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,password:process.env.HARBOR_TEST_PG_PASSWORD,database:database??'postgres'}};}
async function fixture(t,{install=true}={}){
 const database='harbor_admin_dept_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});await root.query('CREATE DATABASE '+database);const store=new PostgresStore({...options(database),poolSize:6});
 const runtime=await mkdtemp(join(tmpdir(),'harbor-admin-department-'));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+database);await root.close();await rm(runtime,{recursive:true,force:true});});
 await store.migrate({recordingContextIndex:true});await store.initializePricing();if(install)await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer});
 let a,b,scopeA,scopeB,keyA;
 if(install){a=await departments.createDepartment(superAdmin,{name:'同名部门'});b=await departments.createDepartment(superAdmin,{name:'同名部门'});keyA=await departments.issueManagementKey(superAdmin,a.id,{label:'合成A'});const keyB=await departments.issueManagementKey(superAdmin,b.id,{label:'合成B'});scopeA=await departments.authenticateManagementKey(keyA.secret);scopeB=await departments.authenticateManagementKey(keyB.secret);}
 return {store,runtime,issuer,superAdmin,departments,a,b,scopeA,scopeB,keyA};
}
const body={name:'同名员工',quota:null,expires:null};
test('可信scope贯穿成员Key创建、类型关联、读取与撤销，拒跨部门和混合批量',integration,async t=>{
 const f=await fixture(t),{store,scopeA,scopeB,a,b}=f,admin=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32)});t.after(()=>admin.close());
 const call=(path,method,payload,scope=scopeA,params=new URLSearchParams())=>admin.handle(path,method,payload,params,{scope});
 const ma=(await call('/api/admin/members','POST',body)).value,mb=(await call('/api/admin/members','POST',body,scopeB)).value;
 const stored=await store.get('SELECT m.department_id,k.department_id key_department,k.digest FROM harbor.members m JOIN harbor.keys k ON k.member_id=m.id WHERE k.id=$1',ma.keyId);assert.deepEqual(stored,{department_id:a.id,key_department:a.id,digest:hash(ma.secret)});
 const second=(await call('/api/admin/keys','POST',{memberId:ma.memberId,expires:null})).value;assert.equal((await store.get('SELECT department_id FROM harbor.keys WHERE id=$1',second.keyId)).department_id,a.id);
 for(const payload of [{memberId:mb.memberId,expires:null},{memberId:ma.memberId,expires:null,department_id:b.id}])await assert.rejects(call('/api/admin/keys','POST',payload),error=>[403,404].includes(error.status));
 await assert.rejects(admin.handle('/api/admin/members','POST',body),{code:'department_scope_required'});
 await assert.rejects(call('/api/admin/members','POST',body,JSON.parse(JSON.stringify(scopeA))),{code:'department_scope_required'});
 await assert.rejects(call('/api/admin/members','POST',body,f.superAdmin),{code:'department_forbidden'});
 const typeA=await store.get("SELECT id FROM harbor.member_types WHERE department_id=$1 AND code='A'",a.id),typeB=await store.get("SELECT id FROM harbor.member_types WHERE department_id=$1 AND code='A'",b.id);
 await assert.rejects(call('/api/admin/members','POST',{...body,type_id:typeB.id,expected_type_version:0,expected_template_version:1}),{code:'department_object_not_found'});
 const typed=(await call('/api/admin/members','POST',{...body,type_id:typeA.id,expected_type_version:0,expected_template_version:1})).value;assert.equal((await store.get('SELECT usage_type_id FROM harbor.members WHERE id=$1',typed.memberId)).usage_type_id,typeA.id);
 const listed=(await call('/api/admin/members','GET',undefined,scopeA,new URLSearchParams('view=list'))).value;assert.deepEqual(new Set(listed.rows.map(row=>row.id)),new Set([ma.memberId,typed.memberId]));assert.ok(listed.types.every(row=>row.id!==typeB.id));assert.equal(listed.pagination.total,2);
 const selected=(await call('/api/admin/members/selection','GET')).value;assert.equal(selected.total,2);assert.ok(selected.members.every(row=>row.id!==mb.memberId));
 assert.equal((await call('/api/admin/keys','GET')).value.pagination.total,3);
 const revealed=(await call('/api/admin/members/'+ma.memberId+'/keys','GET')).value;assert.ok(revealed.rows.some(row=>row.full_key===ma.secret));
 for(const [path,method,payload] of [['/api/admin/members/'+mb.memberId,'GET'],['/api/admin/members/'+mb.memberId,'PATCH',{name:'越权'}],['/api/admin/keys/'+mb.keyId+'/policy','GET'],['/api/admin/keys/'+mb.keyId,'DELETE',{}]])await assert.rejects(call(path,method,payload),{code:'department_object_not_found'});
 const batchCount=async()=>(await store.get('SELECT (SELECT COUNT(*) FROM harbor.member_config_batches)+(SELECT COUNT(*) FROM harbor.member_action_batches) n')).n,beforeBatches=await batchCount();
 await assert.rejects(call('/api/admin/member-config-batches/preview','POST',{scope:{member_ids:[ma.memberId,mb.memberId]},type_id:typeA.id,expected_type_version:0,models:false,limits:false,classify:true,overwrite_exceptions:false}),{code:'member_batch_conflict'});
 await assert.rejects(call('/api/admin/member-action-batches/preview','POST',{kind:'member_access',enabled:false,member_ids:[ma.memberId,mb.memberId]}),{code:'member_action_selection_changed'});
 assert.equal(await batchCount(),beforeBatches);
 await f.departments.updateDepartment(f.superAdmin,a.id,{enabled:false,expected_version:0});const rootA=f.issuer.forDepartment(f.superAdmin,a.id);
 assert.equal((await call('/api/admin/members','GET',undefined,rootA)).value.pagination.total,2);
 assert.equal((await call('/api/admin/members/'+ma.memberId,'GET',undefined,rootA)).value.id,ma.memberId);
 await assert.rejects(call('/api/admin/members','POST',body,rootA),{code:'department_forbidden'});
 await assert.rejects(call('/api/admin/members/'+ma.memberId,'PATCH',{name:'不允许'},rootA),{code:'department_forbidden'});
 await assert.rejects(call('/api/admin/members','GET'),{code:'department_forbidden'});
 await f.departments.updateDepartment(f.superAdmin,a.id,{enabled:true,expected_version:1});
 await assert.rejects(call('/api/admin/members','GET'),{code:'department_forbidden'});
 await f.departments.revokeManagementKey(f.superAdmin,a.id,f.keyA.key.id,{expected_version:1});await assert.rejects(call('/api/admin/members','GET'),{code:'department_forbidden'});await assert.rejects(call('/api/admin/members','POST',body),{code:'department_forbidden'});
});

test('账号创建同部门权重、ID读取和写入隔离，套餐目录保持本部门范围',integration,async t=>{
 const f=await fixture(t),calls=[],admin=createPostgresAccountAdmin(f.store,{runtime:f.runtime,encryptionKey:randomBytes(32),router:{disableAccount:async()=>{calls.push('disable');},block:async()=>{calls.push('block');},syncWeights:async()=>{calls.push('sync');}},checkApi:async()=>[],checkSubscription:async()=>true});t.after(()=>admin.close());
 const call=(path,method,payload={},scope=f.scopeA)=>admin.handle(path,method,payload,{scope});
 const input={name:'同名账号',auth_mode:'openai_api',api_key:'synthetic-upstream',api_weight:80};
 const a=(await call('/api/admin/accounts','POST',input)).value,b=(await call('/api/admin/accounts','POST',input,f.scopeB)).value;
 assert.equal((await f.store.get('SELECT department_id FROM harbor.accounts WHERE id=$1',a.id)).department_id,f.a.id);assert.equal((await call('/api/admin/accounts/'+a.id,'GET')).value.department_id,f.a.id);
 await assert.rejects(call('/api/admin/accounts/'+b.id,'GET'),{code:'department_object_not_found'});await assert.rejects(call('/api/admin/accounts/'+b.id,'PATCH',{api_weight:1}),{code:'department_object_not_found'});
 await assert.rejects(call('/api/admin/accounts','POST',{...input,department_id:f.b.id}),{code:'department_forbidden'});
 const files=await readdir(join(f.runtime,'accounts'));await assert.rejects(call('/api/admin/accounts','POST',input),{status:400});assert.deepEqual(await readdir(join(f.runtime,'accounts')),files);
 const catalog=await call('/api/admin/account-packages','GET');assert.equal(catalog.status,200);assert.deepEqual(catalog.value.items,[]);
 assert.deepEqual(calls,[]);await call('/api/admin/accounts/'+a.id,'PATCH',{enabled:false});assert.deepEqual(calls,['disable']);
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});const rootA=f.issuer.forDepartment(f.superAdmin,f.a.id);assert.equal((await call('/api/admin/accounts/'+a.id,'GET',{},rootA)).value.id,a.id);await assert.rejects(call('/api/admin/accounts','POST',input,rootA),{code:'department_forbidden'});await assert.rejects(call('/api/admin/accounts/'+a.id,'PATCH',{api_weight:1},rootA),{code:'department_forbidden'});await assert.rejects(call('/api/admin/accounts/'+a.id,'GET'),{code:'department_forbidden'});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});
 const fresh=await f.departments.authenticateManagementKey(f.keyA.secret);await call('/api/admin/accounts/'+a.id,'DELETE',{},fresh);assert.equal((await f.store.get('SELECT removed_at FROM harbor.accounts WHERE id=$1',b.id)).removed_at,null);
});

test('迁移前旧管理创建不变，热安装后旧实例拒绝继续无scope写入',integration,async t=>{
 const f=await fixture(t,{install:false}),admin=createPostgresMemberAdmin(f.store,{encryptionKey:randomBytes(32)});t.after(()=>admin.close());
 const created=(await admin.handle('/api/admin/members','POST',body)).value;assert.equal((await f.store.get('SELECT digest FROM harbor.keys WHERE id=$1',created.keyId)).digest,hash(created.secret));
 await migrateDepartmentIsolation(f.store,{apply:true});await assert.rejects(admin.handle('/api/admin/members','POST',body),{code:'department_restart_required'});
});

test('账号详情和套餐预取消不查询，排队与容量限制覆盖部门校验',async()=>{
 let reads=0,release,started;const gate=new Promise(resolve=>{release=resolve;}),entered=new Promise(resolve=>{started=resolve;});
 const store={get:async sql=>{reads++;if(sql.startsWith("SELECT to_regclass('harbor.gateway_settings')"))return {settings:false,departments:false,admin_keys:false,department_settings:false,columns:false};if(sql.startsWith('SELECT * FROM harbor.accounts')){started();await gate;return {id:'synthetic-account',name:'合成',auth_mode:'subscription',kind:'codex',state:'offline'};}throw Error('unexpected synthetic query');}};
 const admin=createPostgresAccountAdmin(store,{runtime:'/synthetic-unused',router:{disableAccount:async()=>{},block:async()=>{},syncWeights:async()=>{}},checkApi:async()=>[],checkSubscription:async()=>true,checks:{maxConcurrent:1,maxQueued:1}});
 try{
  const aborted=new AbortController(),reason=Error('synthetic aborted');aborted.abort(reason);
  for(const path of ['/api/admin/accounts/synthetic-account','/api/admin/account-packages','/api/admin/accounts/synthetic-account/subscription'])await assert.rejects(admin.handle(path,'GET',{}, {signal:aborted.signal}),error=>error===reason);assert.equal(reads,0);
  const first=admin.handle('/api/admin/accounts/synthetic-account','GET');await entered;const before=reads,controller=new AbortController(),queued=admin.handle('/api/admin/accounts/synthetic-account','GET',{}, {signal:controller.signal});assert.equal(admin.status().queued,1);
  await assert.rejects(admin.handle('/api/admin/accounts/synthetic-account','GET'),{code:'query_queue_full'});assert.equal(reads,before);controller.abort(reason);await assert.rejects(queued,error=>error===reason);assert.equal(admin.status().queued,0);assert.equal(reads,before);
  release();assert.equal((await first).status,200);
 }finally{release();await admin.close();}
});
