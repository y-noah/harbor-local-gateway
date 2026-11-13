import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresMemberBatches} from '../postgres-member-batches.mjs';
import {createPostgresMemberActions} from '../postgres-member-actions.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立真实PG':false,timeout:60000};
const database=name=>{const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:6};};
async function fixture(t,{legacyPreview=false}={}){
 const root=new PostgresStore(database()),name='harbor_dept_batch_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});await store.migrate();await store.initializePricing();const legacy={configuration:randomUUID(),actions:randomUUID()};if(legacyPreview){const at=Date.now();await store.run("INSERT INTO harbor.member_config_batches(id,created,expires,actor,scope,operation) VALUES($1,$2,$3,'admin','{}','{}')",legacy.configuration,at,at+60000);await store.run("INSERT INTO harbor.member_action_batches(id,created,expires,actor,operation) VALUES($1,$2,$3,'admin','{}')",legacy.actions,at,at+60000);}await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-super',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer,authorizeSuperAdmin:()=>superAdmin}),rows={},scopes={},keys={},members={},types={};
 for(const label of ['a','b']){rows[label]=await departments.createDepartment(superAdmin,{name:'合成批量部门'+label});keys[label]=await departments.issueManagementKey(superAdmin,rows[label].id,{label:'合成管理员'});scopes[label]=await departments.authenticateManagementKey(keys[label].secret);types[label]=await store.all('SELECT id,version FROM harbor.member_types WHERE department_id=$1 ORDER BY code',rows[label].id);members[label]=[];for(let n=0;n<2;n++){const id=randomUUID(),key=randomUUID();await store.run("INSERT INTO harbor.members(id,name,created,department_id,usage_type_id) VALUES($1,'合成员工',$2,$3,$4)",id,Date.now(),rows[label].id,types[label][0].id);await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created,expires,department_id) VALUES($1,$2,$1,'synthetic',$3,$4,$5)",key,id,Date.now(),Date.now()+3600000,rows[label].id);members[label].push({id,key});}}
 const jobs=createPostgresJobs(store,{departmentActors:departments,scopedKinds:['member.config','member.actions']}),configuration=createPostgresMemberBatches(store,{jobs}),actions=createPostgresMemberActions(store,{jobs});
 const input=(label='a')=>({scope:{member_ids:members[label].map(row=>row.id)},type_id:types[label][1].id,expected_type_version:types[label][1].version,models:false,limits:false,classify:true,overwrite_exceptions:false});
 const execute=async(backend,kind,id,scope=scopes.a)=>{await backend.submit(id,{scope});const job=await jobs.claim([kind]);assert.equal(job.target,id);await jobs.complete(job,await backend.handlers[kind](job));return job;};
 return {store,legacy,issuer,superAdmin,departments,rows,scopes,keys,members,types,jobs,configuration,actions,input,execute};
}
test('两类部门批量预览拒绝混合员工/Key/类型，详情列表游标和操作不跨部门',integration,async t=>{
 const f=await fixture(t);await assert.rejects(f.configuration.preview({...f.input(),scope:{member_ids:[f.members.a[0].id,f.members.b[0].id]}},{scope:f.scopes.a}));await assert.rejects(f.configuration.preview({...f.input(),type_id:f.types.b[1].id},{scope:f.scopes.a}));
 await assert.rejects(f.actions.preview({kind:'member_access',enabled:false,member_ids:[f.members.a[0].id,f.members.b[0].id]},{scope:f.scopes.a}));await assert.rejects(f.actions.preview({kind:'key_renewal',expires:null,key_ids:[f.members.a[0].key,f.members.b[0].key]},{scope:f.scopes.a}));
 for(const [backend,path,body,detail] of [[f.configuration,'member-config-batches',f.input(),'status'],[f.actions,'member-action-batches',{kind:'member_access',enabled:false,member_ids:f.members.a.map(row=>row.id)},'detail']]){
  await assert.rejects(backend.preview(body,{scope:f.superAdmin}),error=>error.status===403);const p=await backend.preview(body,{scope:f.scopes.a});
  for(const call of [()=>backend[detail](p.id,{scope:f.scopes.b}),()=>backend.submit(p.id,{scope:f.scopes.b}),()=>backend.cancel(p.id,{scope:f.scopes.b}),()=>backend.derived(p.id,'undo',{scope:f.scopes.b}),()=>backend.handle('/api/admin/'+path,'GET',{},new URLSearchParams({before:p.id}),{scope:f.scopes.b})])await assert.rejects(call(),error=>error.status===404);
  assert.equal((await backend.handle('/api/admin/'+path,'GET',{},new URLSearchParams(),{scope:f.scopes.b})).value.items.length,0);assert.equal((await backend.handle('/api/admin/'+path,'GET',{},new URLSearchParams(),{scope:f.scopes.a})).value.items.length,1);
 }
});
test('部门批量配置与访问动作真实执行、安全撤销和取消保留原版本/Key语义',integration,async t=>{
 const f=await fixture(t),p=await f.configuration.preview(f.input(),{scope:f.scopes.a});await f.execute(f.configuration,'member.config',p.id);assert.equal((await f.configuration.status(p.id,{scope:f.scopes.a})).counts.success,2);assert.ok((await f.store.all('SELECT usage_type_id FROM harbor.members WHERE department_id=$1',f.rows.a.id)).every(row=>row.usage_type_id===f.types.a[1].id));assert.ok((await f.store.all('SELECT usage_type_id FROM harbor.members WHERE department_id=$1',f.rows.b.id)).every(row=>row.usage_type_id===f.types.b[0].id));
 const undo=await f.configuration.derived(p.id,'undo',{scope:f.scopes.a});await f.execute(f.configuration,'member.config',undo.id);assert.equal((await f.configuration.status(undo.id,{scope:f.scopes.a})).counts.success,2);
 const disabled=await f.actions.preview({kind:'member_access',enabled:false,member_ids:f.members.a.map(row=>row.id)},{scope:f.scopes.a});await f.execute(f.actions,'member.actions',disabled.id);assert.equal((await f.actions.detail(disabled.id,{scope:f.scopes.a})).counts.success,2);const restore=await f.actions.derived(disabled.id,'undo',{scope:f.scopes.a});await f.execute(f.actions,'member.actions',restore.id);assert.equal((await f.store.get('SELECT enabled FROM harbor.members WHERE id=$1',f.members.a[0].id)).enabled,1);
 const renewal=await f.actions.preview({kind:'key_renewal',expires:null,key_ids:[f.members.a[0].key]},{scope:f.scopes.a});await f.actions.cancel(renewal.id,{scope:f.scopes.a});assert.equal((await f.actions.detail(renewal.id,{scope:f.scopes.a})).status,'cancelled');assert.notEqual((await f.store.get('SELECT expires FROM harbor.keys WHERE id=$1',f.members.a[0].key)).expires,null);
});
test('原owner不能由同部门另一Key替代，执行中撤销停在下一项，新预览可恢复待办',integration,async t=>{
 const f=await fixture(t),newKey=await f.departments.issueManagementKey(f.superAdmin,f.rows.a.id,{label:'合成接任管理Key'}),nextScope=await f.departments.authenticateManagementKey(newKey.secret),p=await f.actions.preview({kind:'member_access',enabled:false,member_ids:f.members.a.map(row=>row.id)},{scope:f.scopes.a});
 await assert.rejects(f.actions.submit(p.id,{scope:nextScope}),{code:'department_batch_owner'});await f.actions.submit(p.id,{scope:f.scopes.a});const job=await f.jobs.claim(['member.actions']),owned=f.jobs.owned;let revoked=false;
 f.jobs.owned=async(...args)=>{const result=await owned(...args);if(!revoked&&(await f.store.get("SELECT COUNT(*) n FROM harbor.member_action_items WHERE batch_id=$1 AND status='success'",p.id)).n===1){revoked=true;await f.departments.revokeManagementKey(f.superAdmin,f.rows.a.id,f.keys.a.key.id,{expected_version:0});}return result;};
 try{await assert.rejects(f.actions.handlers['member.actions'](job),error=>error.status===403);}finally{f.jobs.owned=owned;}
 assert.equal((await f.actions.detail(p.id,{scope:nextScope})).counts.pending,1);await f.jobs.fail(job,{retry:false,errorCode:'job_failed'});const retry=await f.actions.derived(p.id,'retry',{scope:nextScope});assert.equal(retry.members,1);await f.execute(f.actions,'member.actions',retry.id,nextScope);assert.equal((await f.actions.detail(retry.id,{scope:nextScope})).counts.success,1);
});
test('批量预览归属和owner不可改写，伪造异部门desired Key不会写入',integration,async t=>{
 const f=await fixture(t),p=await f.configuration.preview(f.input(),{scope:f.scopes.a});await assert.rejects(f.store.run('UPDATE harbor.member_config_batches SET department_id=$1 WHERE id=$2',f.rows.b.id,p.id),{code:'23514'});await assert.rejects(f.store.run("UPDATE harbor.member_config_batches SET actor_id='other' WHERE id=$1",p.id),{code:'23514'});
 await f.store.run("UPDATE harbor.member_config_batch_items SET desired_config=jsonb_set(desired_config,'{keys,0,id}',to_jsonb($1::text)) WHERE batch_id=$2 AND ordinal=1",f.members.b[0].key,p.id);await f.configuration.submit(p.id,{scope:f.scopes.a});const job=await f.jobs.claim(['member.config']);await assert.rejects(f.configuration.handlers['member.config'](job),{code:'department_batch_owner'});assert.equal((await f.store.get('SELECT model_policy_version FROM harbor.keys WHERE id=$1',f.members.b[0].key)).model_policy_version,0);
});

test('旧批量预览归默认部门但不恢复旧admin执行权，新写入不能省略owner',integration,async t=>{
 const f=await fixture(t,{legacyPreview:true}),scope=f.issuer.forDepartment(f.superAdmin,'department-default');
 for(const [backend,id,table,detail] of [[f.configuration,f.legacy.configuration,'member_config_batches','status'],[f.actions,f.legacy.actions,'member_action_batches','detail']]){
  const row=await backend[detail](id,{scope});assert.equal(row.department_id,'department-default');assert.equal(row.actor_kind,'legacy_admin');assert.equal(row.actor,'admin');await assert.rejects(backend.submit(id,{scope}),{code:'department_batch_owner'});await backend.cancel(id,{scope});assert.equal((await backend[detail](id,{scope})).status,'cancelled');
  await assert.rejects(f.store.run('UPDATE harbor.'+table+' SET actor_version=actor_version+1 WHERE id=$1',id),{code:'23514'});
 }
});
