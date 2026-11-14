import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresMemberTypes} from '../postgres-member-types.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresMemberBatches} from '../postgres-member-batches.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立真实PG':false};
function database(name){const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true};}
async function fixture(t,{departments=false}={}){
 const root=new PostgresStore(database()),name='harbor_type_summary_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));t.after(async()=>{await store.close();try{await root.query('DROP DATABASE '+name);}finally{await root.close();}});await store.migrate();await store.initializePricing();
 const types=createPostgresMemberTypes(store),defaults={allowed_models:null,usage_limits:{enabled:false,rules:[]}},issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-summary-root',credentialVersion:0}),scopes={};
 if(departments){await migrateDepartmentIsolation(store,{apply:true});for(const id of ['department-a','department-b']){await store.run('INSERT INTO harbor.departments(id,name,created,updated) VALUES($1,$1,1,1)',id);scopes[id]=issuer.forDepartment(superAdmin,id);}}
 const admin=createPostgresMemberAdmin(store,{encryptionKey:Buffer.alloc(32,7)});t.after(()=>admin.close());
 const request=async(path,method='GET',payload,scope)=>(await admin.handle('/api/admin/'+path,method,payload,new URLSearchParams(),{scope})).value;
 const create=async(scope,name='合成默认类型')=>{const type=await request('member-types','POST',{name},scope);await request('member-types/'+type.id,'PATCH',{expected_version:0,defaults},scope);return {...type,version:1,default_version:1};};
 const update=async(type,scope)=>{await request('member-types/'+type.id,'PATCH',{expected_version:type.version,defaults},scope);type.version++;type.default_version++;};
 const member=async(name,type,{models='template',limits='template',scope}={})=>request('members','POST',{name,quota:null,expires:null,type_id:type.id,expected_type_version:type.version,expected_template_version:type.default_version,models_mode:models,limits_mode:limits,...(models==='custom'?{allowed_models:null}:{}),...(limits==='custom'?{usage_limits:defaults.usage_limits}:{})},scope);

 return {store,types,create,update,member,request,scopes,superAdmin};
}
test('类型待应用与个人例外按来源版本统计，可重叠且不猜相同配置；只读单查询不读取用量或凭据',integration,async t=>{
 const f=await fixture(t),type=await f.create();
 const old=await f.member('合成旧版',type),mixed=await f.member('合成混合例外',type,{limits:'custom'}),removed=await f.member('合成删除员工',type);
 const retained=await f.member('合成保留',type);await f.request('members/'+retained.memberId+'/type','PATCH',{type_id:type.id,expected_type_version:1,expected_usage_type_version:1,apply_defaults:false});
 const custom=await f.member('合成自定义',type,{models:'custom'});
 // Real creation produces employee-without-type_id for a custom employee default.
 const customSource=(await f.store.get('SELECT model_source FROM harbor.keys WHERE id=$1',custom.keyId)).model_source;assert.equal(customSource.kind,'employee');assert.equal(customSource.type_id,undefined);
 await f.request('members/'+removed.memberId,'DELETE',{});await f.update(type);
 const current=await f.member('合成当前',type),customKey=await f.member('合成独立Key',type),deletedKey=await f.member('合成删除Key',type);
 await f.request('keys/'+customKey.keyId,'PATCH',{allowed_models:null,expected_model_policy_version:0});await f.request('keys/'+deletedKey.keyId,'DELETE',{});
 const unconfigured=await f.request('member-types','POST',{name:'合成待配置'});await f.member('合成未配置个人',unconfigured,{models:'custom',limits:'custom'});
 const batches=createPostgresMemberBatches(f.store),preview=await batches.preview({scope:{type_id:type.id},type_id:type.id,expected_type_version:2,models:true,limits:true,classify:false,overwrite_exceptions:false}),status=await batches.status(preview.id);
 const inheritedItem=status.items.find(row=>row.member_id===current.memberId),customItem=status.items.find(row=>row.member_id===custom.memberId);assert.equal(inheritedItem.exceptions.length,0);assert.ok(customItem.exceptions.some(row=>row.component==='key'&&row.source==='employee'));assert.deepEqual(customItem.before_config.keys,customItem.desired_config.keys);
 const before=await f.store.all('SELECT id,configuration_source FROM harbor.members ORDER BY id'),all=f.store.all.bind(f.store),queries=[];f.store.all=(sql,...args)=>{queries.push(sql);return all(sql,...args);};let result;try{result=await f.types.list();}finally{f.store.all=all;}
 const summary=result.types.find(r=>r.id===type.id);assert.equal(summary.members,7);assert.equal(summary.pending_members,3);assert.equal(summary.exception_members,4);assert.equal(summary.default_version,2);
 const empty=result.types.find(r=>r.id===unconfigured.id);assert.equal(empty.configured,false);assert.equal(empty.members,1);assert.equal(empty.pending_members,0);assert.equal(empty.exception_members,1);
 const counts=queries.filter(sql=>sql.startsWith('WITH typed_members'));assert.equal(counts.length,1);assert.doesNotMatch(counts[0],/harbor\.requests|harbor\.request_prices|secret_cipher|api_secret|digest/);assert.deepEqual(await f.store.all('SELECT id,configuration_source FROM harbor.members ORDER BY id'),before);
 // Equal permissions are deliberately used for v1/v2; source version still
 // distinguishes an unapplied update from explicit personal retention.
 assert.deepEqual((await f.store.all('SELECT allowed_models,usage_limits FROM harbor.member_type_templates WHERE type_id=$1 ORDER BY version',type.id)).map(row=>row.allowed_models),[null,null]);
 const jobs=createPostgresJobs(f.store);await batches.submit(preview.id);const job=await jobs.claim(['member.config']);await jobs.complete(job,await batches.handlers['member.config'](job));const applied=(await f.types.list()).types.find(row=>row.id===type.id);assert.equal(applied.pending_members,0);assert.equal(applied.exception_members,4);const appliedResult=await batches.status(preview.id);assert.deepEqual(appliedResult.items.find(row=>row.member_id===retained.memberId).before_config.keys,appliedResult.items.find(row=>row.member_id===retained.memberId).after_config.keys);
});
test('类型计数只含可信部门员工及Key，全局可汇总且不暴露其他部门或逐人信息',integration,async t=>{
 const f=await fixture(t,{departments:true}),a=await f.create(f.scopes['department-a']),b=await f.create(f.scopes['department-b']);
 await f.member('合成部门甲',a,{scope:f.scopes['department-a']});await f.member('合成部门乙',b,{limits:'custom',scope:f.scopes['department-b']});await f.update(b,f.scopes['department-b']);
 const left=await f.types.list({scope:f.scopes['department-a']}),right=await f.types.list({scope:f.scopes['department-b']});assert.deepEqual(left.types.map(r=>r.id),[a.id]);assert.deepEqual(right.types.map(r=>r.id),[b.id]);
 assert.deepEqual(left.types.map(({members,pending_members,exception_members})=>({members,pending_members,exception_members})),[{members:1,pending_members:0,exception_members:0}]);assert.deepEqual(right.types.map(({members,pending_members,exception_members})=>({members,pending_members,exception_members})),[{members:1,pending_members:1,exception_members:1}]);assert.doesNotMatch(JSON.stringify(left),/department-b|member-b|key-b|configuration_source/);
 const global=await f.types.list({scope:f.superAdmin});assert.equal(global.types.find(r=>r.id===b.id).pending_members,1);assert.equal(global.types.find(r=>r.id===a.id).pending_members,0);
});
