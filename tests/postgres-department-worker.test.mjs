import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,writeFile,rm,chmod,symlink} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresWorker} from '../postgres-worker.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createPostgresMemberBatches} from '../postgres-member-batches.mjs';
import {createPostgresMemberActions} from '../postgres-member-actions.mjs';
import {RecordingStore} from '../recording-store.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立真实PG':false,timeout:30000};
const database=name=>{const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true};};
const until=async condition=>{const end=Date.now()+8000;while(!await condition()){assert.ok(Date.now()<end,'department worker condition timed out');await new Promise(resolve=>setTimeout(resolve,20));}};
async function fixture(t){
 const root=new PostgresStore(database()),name='harbor_dept_worker_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name)),dataDir=await mkdtemp(join(tmpdir(),'harbor-dept-worker-')),workers=[];
 t.after(async()=>{for(const worker of workers)await worker.close();await store.close();await root.query('DROP DATABASE '+name);await root.close();await rm(dataDir,{recursive:true,force:true});});
 await store.migrate({recordingContext:true,recordingContextIndex:true});await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const config={adminPasswordHash:passwordHash('synthetic-root')},settings=join(dataDir,'settings.json'),key=randomBytes(32);await writeFile(settings,JSON.stringify(config),{mode:0o600});await writeFile(join(dataDir,'recording-encryption.key'),key,{mode:0o600});
 const issuer=createDepartmentScopeIssuer(),auth=createAdminAuth(config,{scopeIssuer:issuer}),response={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};await auth.loginAsync('synthetic-root',response);const superAdmin=await auth.context({headers:{cookie:response.cookie}}),departments=createPostgresDepartments(store,{issuer,authorizeSuperAdmin:auth.authorizeSuperAdmin}),department=await departments.createDepartment(superAdmin,{name:'合成Worker部门'}),management=await departments.issueManagementKey(superAdmin,department.id,{label:'合成管理Key'}),scope=await departments.authenticateManagementKey(management.secret);
 const request=randomUUID();await store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES('synthetic-worker-member','合成员工',$1,$2)",Date.now(),department.id);await store.run("INSERT INTO harbor.requests(id,member_id,started,ended,status,business_status,ip,model,department_id) VALUES($1,'synthetic-worker-member',$2,$2,'ok','ok','synthetic','synthetic',$3)",request,Date.now(),department.id);const recordings=new RecordingStore(store,{encryptionKey:key});await recordings.start(request);await recordings.append(request,[{sourceKey:'synthetic-user',kind:'user',provenance:'client',content:{text:'合成Worker正文'}}]);await recordings.finish(request,{state:'complete'});
 const options={database:database(name),dataDir,env:{},recording:{enabled:true,storage:{probe:async()=>[{total:10000000000,free:9000000000}]}},runtime:{pollMs:100}};
 const create=async changes=>{const worker=await createPostgresWorker({...options,...changes});workers.push(worker);return worker;};
 return {store,dataDir,settings,config,create,options,departments,department,management,scope,superAdmin,request};
}
test('部门Worker使用同库actor执行真实索引导出及startup，未适配任务不运行',integration,async t=>{
 const f=await fixture(t),worker=await f.create(),exported=await worker.recordingBackend.exports.enqueue({include_body:true,format:'jsonl',filters:{}},{scope:f.scope});worker.start();
 await until(async()=>{const row=await f.store.get("SELECT status FROM harbor.background_jobs WHERE id=$1",exported.id);return row.status==='completed';});await until(async()=>(await f.store.get('SELECT COUNT(*) n FROM harbor.recording_documents WHERE request_id=$1',f.request)).n===1);await until(async()=>(await f.store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind='worker.startup' AND status='completed'")).n===1);
 const startup=await f.store.get("SELECT scope_kind,actor_kind,actor_id FROM harbor.background_jobs WHERE kind='worker.startup'");assert.deepEqual(startup,{scope_kind:'global',actor_kind:'system',actor_id:'system:worker.startup'});
 const status=worker.status();assert.equal(status.departments.installed,true);assert.ok(status.departments.supportedKinds.includes('routing.expiry'));assert.ok(status.departments.supportedKinds.includes('routing.nightly'));assert.ok(status.departments.supportedKinds.includes('routing.preview'));assert.ok(status.departments.supportedKinds.includes('member.config'));assert.ok(status.departments.supportedKinds.includes('member.actions'));await until(async()=>(await f.store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind='routing.expiry' AND department_id=$1 AND status='completed'",f.department.id)).n===1);
 let body='';for await(const chunk of worker.recordingBackend.download(exported.id,{scope:f.scope}))body+=chunk;assert.ok(body.includes('合成Worker正文'));
});
test('部门Worker后台导出复验管理Key撤销与settings密码变更，不写正文产物',integration,async t=>{
 const f=await fixture(t),worker=await f.create(),departmentExport=await worker.recordingBackend.exports.enqueue({include_body:true,format:'jsonl',filters:{}},{scope:f.scope}),rootExport=await worker.recordingBackend.exports.enqueue({include_body:true,format:'jsonl',filters:{}},{scope:f.superAdmin});
 await f.departments.revokeManagementKey(f.superAdmin,f.department.id,f.management.key.id,{expected_version:0});await writeFile(f.settings,JSON.stringify({adminPasswordHash:passwordHash('synthetic-new-root')}),{mode:0o600});worker.start();
 for(const id of [departmentExport.id,rootExport.id]){await until(async()=>(await f.store.get('SELECT attempts FROM harbor.background_jobs WHERE id=$1',id)).attempts>0);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.recording_export_chunks WHERE export_id=$1',id)).n,0);}
 await until(()=>worker.status().failed>=2);assert.ok(worker.status().failed>=2);
});
test('部门Worker缺失或宽权限配置和运行时handler覆盖拒绝，正文关闭仍检查身份',integration,async t=>{
 const f=await fixture(t);await rm(f.settings);await assert.rejects(f.create({recording:{enabled:false}}),{code:'worker_admin_configuration'});
 await writeFile(f.settings,JSON.stringify(f.config),{mode:0o644});await assert.rejects(f.create({recording:{enabled:false}}),{code:'worker_admin_configuration'});await chmod(f.settings,0o600);
 await assert.rejects(f.create({runtime:{handlers:{'worker.startup':async()=>({})}}}),{code:'worker_configuration'});
 const worker=await f.create({recording:{enabled:false}});worker.start();await until(async()=>(await f.store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind='worker.startup' AND status='completed'")).n===1);assert.deepEqual(worker.status().departments.supportedKinds,['worker.startup','member.config','member.actions','routing.expiry','routing.preview','routing.nightly','account.extra.configure','account.extra.recover']);
});

test('部门Worker拒绝symlink与无写入端FIFO，不阻塞启动',{...integration,timeout:10000},async t=>{
 const f=await fixture(t),target=join(f.dataDir,'synthetic-settings.json');await writeFile(target,JSON.stringify(f.config),{mode:0o600});await rm(f.settings);await symlink(target,f.settings);await assert.rejects(f.create({recording:{enabled:false}}),{code:'worker_admin_configuration'});
 await rm(f.settings);await promisify(execFile)('mkfifo',[f.settings]);const started=performance.now();await assert.rejects(f.create({recording:{enabled:false}}),{code:'worker_admin_configuration'});assert.ok(performance.now()-started<2000,'FIFO must fail closed without waiting for a writer');
});

test('真实部门Worker执行员工配置与访问批量，八类支持清单与实际handler一致',integration,async t=>{
 const f=await fixture(t),worker=await f.create({recording:{enabled:false}}),member=randomUUID(),types=await f.store.all('SELECT id,version FROM harbor.member_types WHERE department_id=$1 ORDER BY code',f.department.id);await f.store.run("INSERT INTO harbor.members(id,name,created,department_id,usage_type_id) VALUES($1,'合成批量员工',$2,$3,$4)",member,Date.now(),f.department.id,types[0].id);
 const configuration=createPostgresMemberBatches(worker.store,{jobs:worker.jobs}),actions=createPostgresMemberActions(worker.store,{jobs:worker.jobs}),p=await configuration.preview({scope:{member_ids:[member]},type_id:types[1].id,expected_type_version:types[1].version,models:false,limits:false,classify:true,overwrite_exceptions:false},{scope:f.scope});await configuration.submit(p.id,{scope:f.scope});worker.start();await until(async()=>(await configuration.status(p.id,{scope:f.scope})).status==='completed');assert.equal((await f.store.get('SELECT usage_type_id FROM harbor.members WHERE id=$1',member)).usage_type_id,types[1].id);
 const disabled=await actions.preview({kind:'member_access',enabled:false,member_ids:[member]},{scope:f.scope});await actions.submit(disabled.id,{scope:f.scope});await until(async()=>(await actions.detail(disabled.id,{scope:f.scope})).status==='completed');assert.equal((await f.store.get('SELECT enabled FROM harbor.members WHERE id=$1',member)).enabled,0);
});
