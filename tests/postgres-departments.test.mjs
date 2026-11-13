import {readFile} from 'node:fs/promises';
import {migrateRecordingRetention,requireUnlimitedRecordingRetention,unlimitedRecordingExpiry} from '../recording-retention.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation,verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness,createPostgresDepartments} from '../postgres-departments.mjs';
import {balanceSettings} from '../balancing.mjs';
import {transferSchedule} from '../transfer-schedule.mjs';
import {createDepartmentScopeIssuer,requireDepartmentScope,departmentManagementKeyDigest} from '../department-access.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需既有隔离PG18测试入口':false};
function options(database){
 if(url){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.toString(),allowPlaintextLocalNetwork:true};}
 return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',password:process.env.HARBOR_TEST_PG_PASSWORD,database:database||'postgres'}};
}
async function fixture(t){
 const name='harbor_department_'+randomUUID().replaceAll('-',''),admin=new PostgresStore({...options(),poolSize:1});
 await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
 const store=new PostgresStore({...options(name),poolSize:4});
 t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);await admin.close();});
 await store.migrate({recordingContextIndex:true});return {store,name};
}
async function departments(store){for(const id of ['department-a','department-b'])await store.run('INSERT INTO harbor.departments(id,name,created,updated) VALUES($1,$2,1,1)',id,'同名部门');}
async function person(store,department,id){await store.run('INSERT INTO harbor.members(id,department_id,name,created) VALUES($1,$2,$3,1)',id,department,'同名员工');}
async function account(store,department,id){await store.run("INSERT INTO harbor.accounts(id,department_id,name,home,api_secret_digest) VALUES($1,$2,$3,$4,$5)",id,department,'同名账号','/synthetic/'+id,'a'.repeat(64));}
async function key(store,department,id,member,digest){await store.run('INSERT INTO harbor.keys(id,department_id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,$5,1)',id,department,member,digest,'synthetic');}

test('默认回填保留旧ID/Key与无关联历史，未装和热安装边界明确',integration,async t=>{
 const {store,name}=await fixture(t),reader=new PostgresStore({...options(name),poolSize:1});try{
 assert.equal(await verifyDepartmentIsolationAvailability(reader),false);assert.deepEqual(await migrateDepartmentIsolation(store),{changed:false,installed:false,complete:false});
 await store.run("INSERT INTO harbor.members(id,name,created) VALUES('legacy-member','合成员工',1)");
 await store.run("INSERT INTO harbor.accounts(id,name,home) VALUES('legacy-account','合成账号','/synthetic/legacy')");
 await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('legacy-key','legacy-member',$1,'synthetic',1)",'f'.repeat(64));
 await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated) VALUES('legacy-session','legacy-member','legacy-account',1,1)");
 await store.run("INSERT INTO harbor.requests(id,started,status,ip) VALUES('legacy-orphan',1,'error','synthetic')");
 await store.run("INSERT INTO harbor.recording_retention_plans(id,created,expires,days,requests,bytes) VALUES('legacy-plan',1,2,30,0,0)");
 const result=await migrateDepartmentIsolation(store,{apply:true});assert.equal(result.installed,true);assert.equal(result.complete,false);
 assert.equal((await store.get("SELECT department_id FROM harbor.members WHERE id='legacy-member'")).department_id,'department-default');
 const saved=await store.get("SELECT digest,department_id FROM harbor.keys WHERE id='legacy-key'");assert.equal(saved.digest,'f'.repeat(64));assert.equal(saved.department_id,'department-default');
 assert.equal((await store.get("SELECT department_id FROM harbor.recording_retention_plans WHERE id='legacy-plan'")).department_id,'department-default');
 await assert.rejects(store.run("INSERT INTO harbor.recording_retention_plans(id,created,expires,days,requests,bytes) VALUES('no-plan-scope',1,2,30,0,0)"),{code:'23502'});
 const historical=await store.get("SELECT department_id,member_id,key_id,account_id FROM harbor.requests WHERE id='legacy-orphan'");assert.deepEqual(historical,{department_id:'department-default',member_id:null,key_id:null,account_id:null});
 await assert.rejects(assertDepartmentIsolationReadiness(reader,false),{code:'department_restart_required'});
 assert.equal(await verifyDepartmentIsolationAvailability(store),true);assert.equal(await assertDepartmentIsolationReadiness(store,true),true);
 assert.equal((await migrateDepartmentIsolation(store,{apply:true})).changed,false);
 await assert.rejects(store.run("INSERT INTO harbor.members(id,name,created) VALUES('missing-scope','synthetic',1)"),{code:'23502'});
 }finally{await reader.close();}
});

test('复合关系拒绝跨部门与同部门错员工Key，会话换绑不改历史请求账号',integration,async t=>{
 const {store}=await fixture(t);await migrateDepartmentIsolation(store,{apply:true});await departments(store);
 for(const [d,m,a] of [['department-a','member-a','account-a'],['department-b','member-b','account-b']]){await person(store,d,m);await account(store,d,a);}
 await person(store,'department-a','member-a2');await account(store,'department-a','account-a2');
 await key(store,'department-a','key-a','member-a','1'.repeat(64));
 await assert.rejects(key(store,'department-a','bad-key','member-b','2'.repeat(64)),{code:'23503'});
 await assert.rejects(store.run("INSERT INTO harbor.sessions(id,department_id,member_id,account_id,created,updated) VALUES('bad-session','department-a','member-a','account-b',1,1)"),{code:'23503'});
 await store.run("INSERT INTO harbor.sessions(id,department_id,member_id,account_id,created,updated) VALUES('session-a','department-a','member-a','account-a',1,1)");
 await assert.rejects(store.run("INSERT INTO harbor.requests(id,department_id,member_id,key_id,started,status,ip) VALUES('bad-request','department-a','member-a2','key-a',1,'ok','synthetic')"),{code:'23503'});
 await assert.rejects(store.run("INSERT INTO harbor.requests(id,department_id,member_id,session_id,started,status,ip) VALUES('bad-session-request','department-a','member-a2','session-a',1,'ok','synthetic')"),{code:'23503'});
 await store.run("INSERT INTO harbor.requests(id,department_id,member_id,key_id,account_id,session_id,started,status,ip) VALUES('request-a','department-a','member-a','key-a','account-a','session-a',1,'ok','synthetic')");
 await store.run("UPDATE harbor.sessions SET account_id='account-a2' WHERE id='session-a'");assert.equal((await store.get("SELECT account_id FROM harbor.requests WHERE id='request-a'")).account_id,'account-a');
 await assert.rejects(store.run("UPDATE harbor.members SET department_id='department-b' WHERE id='member-a2'"),{code:'23514'});
});

test('分类code与查询snapshot分别绑定部门，global必须明确NULL',integration,async t=>{
 const {store}=await fixture(t);await migrateDepartmentIsolation(store,{apply:true});await departments(store);
 for(const d of ['department-a','department-b'])await store.run("INSERT INTO harbor.member_types(id,department_id,code,name,description) VALUES($1,$2,'A','同名分类','synthetic')",'type-'+d,d);
 await assert.rejects(store.run("INSERT INTO harbor.member_types(id,department_id,code,name,description) VALUES('duplicate','department-a','A','重复','synthetic')"),{code:'23505'});
 const insert=(department,scope)=>store.run('INSERT INTO harbor.request_query_snapshots(id,visibility,window_start,window_end,filters,created,expires_at,department_id,scope_identity) VALUES($1,pg_current_snapshot(),0,10,$2,1,10,$3,$4)',randomUUID(),'{}',department,scope);
 await insert(null,'global');await insert('department-a','department:department-a');await assert.rejects(insert('department-a','department:department-b'),{code:'23514'});await assert.rejects(insert(null,'department:department-a'),{code:'23514'});
});

test('部分安装、未知marker与删除关系约束均拒绝旧全局能力',integration,async t=>{
 const {store}=await fixture(t);
 await store.query('CREATE TABLE harbor.departments(id text)');await assert.rejects(verifyDepartmentIsolationAvailability(store),{code:'department_migration_required'});await assert.rejects(migrateDepartmentIsolation(store,{apply:true}),{code:'department_migration_required'});await store.query('DROP TABLE harbor.departments');
 await migrateDepartmentIsolation(store,{apply:true});
 await assert.rejects(store.transaction(async()=>{await store.query('ALTER TABLE harbor.requests DROP CONSTRAINT department_requests_member_fk');await assert.rejects(verifyDepartmentIsolationAvailability(store),{code:'department_migration_conflict'});throw Error('synthetic rollback');}),/synthetic rollback/);
 assert.equal(await verifyDepartmentIsolationAvailability(store),true);
 await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='schema.department-isolation'",JSON.stringify({version:999}));await assert.rejects(verifyDepartmentIsolationAvailability(store),{code:'department_migration_conflict'});throw Error('synthetic rollback');}),/synthetic rollback/);
 assert.equal(await verifyDepartmentIsolationAvailability(store),true);
});

test('部门与管理密钥权限、撤销、恢复及平台默认快照保持独立',integration,async t=>{
 const {store}=await fixture(t);await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-super-admin',credentialVersion:0}),service=createPostgresDepartments(store,{issuer,clock:()=>1000});
 const platformSeeds=Object.fromEntries((await store.all("SELECT name,value FROM harbor.gateway_settings WHERE name IN ('request_handoff_policy','account_package_policy')")).map(row=>[row.name,JSON.parse(row.value)]));
 await store.run("INSERT INTO harbor.department_settings(department_id,name,value) VALUES('department-default','balancing',$1::jsonb)",JSON.stringify({failover:true,nightly:true,quietMinutes:5,weighted:true}));
 await store.run("UPDATE harbor.department_settings SET value='{\"mode\":\"legacy\",\"version\":2}'::jsonb WHERE department_id='department-default' AND name='request_handoff_policy'");
 const a=await service.createDepartment(root,{name:'同名部门'}),b=await service.createDepartment(root,{name:'同名部门'});assert.notEqual(a.id,b.id);
 const snapshot=Object.fromEntries((await store.all('SELECT name,value,version FROM harbor.department_settings WHERE department_id=$1',a.id)).map(row=>[row.name,{value:row.value,version:row.version}]));
 assert.deepEqual(snapshot,{balancing:{value:balanceSettings(),version:0},routing_strategy:{value:{weighted:false},version:0},routing_weights:{value:{mode:'auto',weights:{},expires:null,version:0},version:0},transfer_schedule:{value:transferSchedule(),version:0},request_handoff_policy:{value:platformSeeds.request_handoff_policy,version:platformSeeds.request_handoff_policy.version},account_package_policy:{value:platformSeeds.account_package_policy,version:platformSeeds.account_package_policy.version},'recording.retention':{value:{days:30,version:0},version:0}});
 const issued=await service.issueManagementKey(root,a.id,{label:'合成负责人'}),other=await service.issueManagementKey(root,b.id,{label:'合成负责人'});
 const aScope=await service.authenticateManagementKey(issued.secret),bScope=await service.authenticateManagementKey(other.secret);assert.equal(requireDepartmentScope(aScope).department_id,a.id);assert.equal(requireDepartmentScope(bScope).department_id,b.id);
 assert.equal((await service.getDepartment(aScope)).id,a.id);
 await assert.rejects(service.getDepartment(aScope,b.id),{code:'department_forbidden'});
 await assert.rejects(service.issueManagementKey(aScope,a.id,{label:'不允许再授权'}),{code:'department_forbidden'});
 await assert.rejects(service.listDepartments(JSON.parse(JSON.stringify(root))),{code:'department_scope_required'});
 await assert.rejects(service.listDepartments(root,{limit:10}),{code:'department_input_invalid'});
 await assert.rejects(service.listManagementKeys(root,a.id,{limit:10}),{code:'department_input_invalid'});
 const keys=await service.listManagementKeys(root,a.id);assert.equal(keys.rows.length,1);assert.equal(Object.hasOwn(keys.rows[0],'secret'),false);assert.equal(Object.hasOwn(keys.rows[0],'digest'),false);
 assert.equal((await service.readSetting(aScope,a.id,'balancing')).value.nightly,false);
 assert.deepEqual(await service.readSetting(aScope,a.id,'request_handoff_policy'),{value:{mode:'request-boundary',version:1},version:1});
 assert.deepEqual((await store.get("SELECT value FROM harbor.department_settings WHERE department_id='department-default' AND name='request_handoff_policy'")).value,{mode:'legacy',version:2});
 assert.equal((await store.get("SELECT value FROM harbor.department_settings WHERE department_id='department-default' AND name='balancing'")).value.nightly,true);
 assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.member_types WHERE department_id=$1 AND default_version=1',a.id)).n,6);
 assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.member_type_templates WHERE department_id=$1',a.id)).n,6);
 assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.accounts WHERE department_id=$1',a.id)).n,0);
 const scopedRoot=issuer.forDepartment(root,a.id);await assert.rejects(service.updateDepartment(scopedRoot,b.id,{enabled:false,expected_version:0}),{code:'department_forbidden'});
 await service.updateDepartment(root,a.id,{enabled:false,expected_version:0});assert.equal(await service.authenticateManagementKey(issued.secret),null);
 await service.updateDepartment(root,a.id,{enabled:true,expected_version:1});
 assert.equal(await service.validateManagementSession(aScope),null);const fresh=await service.authenticateManagementKey(issued.secret);assert.equal(fresh.credential_version,1);assert.notEqual(await service.validateManagementSession(bScope),null);
 await service.revokeManagementKey(root,a.id,issued.key.id,{expected_version:1});assert.equal(await service.validateManagementSession(fresh),null);
 await service.updateDepartment(root,a.id,{enabled:false,expected_version:2});await service.updateDepartment(root,a.id,{enabled:true,expected_version:3});assert.equal(await service.authenticateManagementKey(issued.secret),null);assert.notEqual(await service.authenticateManagementKey(other.secret),null);
 assert.equal(await service.authenticateManagementKey('hg_'+'a'.repeat(43)),null);
 const audit=await store.all("SELECT detail FROM harbor.audit WHERE action LIKE 'department.%'");assert.equal(audit.every(row=>!row.detail.includes(issued.secret)&&!row.detail.includes(other.secret)&&!row.detail.includes(departmentManagementKeyDigest(issued.secret))),true);
});

test('受控迁移把新表权限授给真实最小LOGIN角色，不借owner或BYPASSRLS',integration,async t=>{
 const {store,name}=await fixture(t),role='harbor_dept_runtime_'+randomUUID().replaceAll('-','').slice(0,20),password=randomBytes(24).toString('hex');let runtime;
 await store.query('CREATE ROLE "'+role+'" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD \''+password+'\'');
 try{
  await store.query('GRANT USAGE ON SCHEMA harbor TO "'+role+'"');await store.query('GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA harbor TO "'+role+'"');
  await migrateDepartmentIsolation(store,{apply:true,runtimeRole:role});
  const base=options(name);let connection;if(base.connectionString){const parsed=new URL(base.connectionString);parsed.username=role;parsed.password=password;connection={connectionString:parsed.toString(),allowPlaintextLocalNetwork:true};}else connection={connection:{...base.connection,user:role,password}};
  runtime=new PostgresStore({...connection,poolSize:1});assert.equal(await verifyDepartmentIsolationAvailability(runtime),true);
  const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-super-admin',credentialVersion:0}),backend=createPostgresDepartments(runtime,{issuer}),department=await backend.createDepartment(root,{name:'运行角色部门'});
  const issued=await backend.issueManagementKey(root,department.id,{label:'合成管理密钥'});assert.notEqual(await backend.authenticateManagementKey(issued.secret),null);
  const flags=await runtime.get('SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user');assert.equal(flags.rolsuper,false);assert.equal(flags.rolbypassrls,false);
 }finally{await runtime?.close();await store.query('DROP OWNED BY "'+role+'"');await store.query('DROP ROLE "'+role+'"');}
});

test('任务迁移显式分类系统扫描与部门业务，依赖和身份不可跨scope',integration,async t=>{
 const {store}=await fixture(t);
 const legacy=(id,kind,dependency=null)=>store.run('INSERT INTO harbor.background_jobs(id,kind,created,updated,available_at,dependency_id) VALUES($1,$2,1,1,1,$3)',id,kind,dependency);
 await legacy('legacy-system','worker.startup');await legacy('legacy-expiry','routing.expiry');await legacy('legacy-nightly','routing.nightly','legacy-expiry');
 await migrateDepartmentIsolation(store,{apply:true});await departments(store);
 assert.deepEqual(await store.get("SELECT department_id,scope_kind,scope_identity,actor_kind FROM harbor.background_jobs WHERE id='legacy-system'"),{department_id:null,scope_kind:'global',scope_identity:'global',actor_kind:'system'});
 assert.deepEqual(await store.get("SELECT department_id,scope_kind,actor_kind FROM harbor.background_jobs WHERE id='legacy-nightly'"),{department_id:'department-default',scope_kind:'department',actor_kind:'legacy_admin'});
 const insert=(id,kind,department,scope,actor,dependency=null)=>store.run('INSERT INTO harbor.background_jobs(id,kind,created,updated,available_at,department_id,scope_kind,scope_identity,actor_kind,actor_id,actor_version,dependency_id) VALUES($1,$2,1,1,1,$3,$4,$5,$6,$7,0,$8)',id,kind,department,department===null?'global':'department',scope,actor,'synthetic-system',dependency);
 await insert('global-purge','recording.purge',null,'global','system');
 await insert('global-export','recording.export',null,'global','super_admin');
 await assert.rejects(insert('system-export','recording.export',null,'global','system'),{code:'23514'});
 await assert.rejects(insert('department-admin-export','recording.export',null,'global','department_admin'),{code:'23514'});
 await insert('a-expiry','routing.expiry','department-a','department:department-a','system');
 await insert('a-nightly','routing.nightly','department-a','department:department-a','system','a-expiry');
 await assert.rejects(insert('bad-global','routing.nightly',null,'global','system'),{code:'23514'});
 await assert.rejects(insert('bad-actor','worker.startup',null,'global','department_admin'),{code:'23514'});
 await assert.rejects(insert('bad-scope','routing.expiry','department-a','department:department-b','system'),{code:'23514'});
 await assert.rejects(insert('bad-dependency','routing.nightly','department-b','department:department-b','system','a-expiry'),{code:'23503'});
 await assert.rejects(insert('global-dependency','recording.purge',null,'global','system','a-expiry'),{code:'23503'});
 await assert.rejects(legacy('missing-scope','worker.startup'),{code:'23502'});
 await assert.rejects(store.run("UPDATE harbor.background_jobs SET actor_id='forged' WHERE id='a-expiry'"),{code:'23514'});
 await assert.rejects(store.run("UPDATE harbor.background_jobs SET scope_identity='global',scope_kind='global',department_id=NULL,kind='recording.purge' WHERE id='a-expiry'"),{code:'23514'});
 assert.equal(await verifyDepartmentIsolationAvailability(store),true);
});

test('未知旧任务或跨scope旧依赖阻断整个迁移且保留原数据',integration,async t=>{
 const {store}=await fixture(t);
 await store.run("INSERT INTO harbor.background_jobs(id,kind,created,updated,available_at) VALUES('unknown','future.handler',1,1,1)");
 await assert.rejects(migrateDepartmentIsolation(store,{apply:true}),{code:'department_relationship_conflict'});
 assert.equal(await verifyDepartmentIsolationAvailability(store),false);assert.equal((await store.get("SELECT kind FROM harbor.background_jobs WHERE id='unknown'")).kind,'future.handler');
 await store.run("DELETE FROM harbor.background_jobs WHERE id='unknown'");
 await store.run("INSERT INTO harbor.background_jobs(id,kind,created,updated,available_at) VALUES('system','worker.startup',1,1,1)");
 await store.run("INSERT INTO harbor.background_jobs(id,kind,created,updated,available_at,dependency_id) VALUES('business','routing.nightly',1,1,1,'system')");
 await assert.rejects(migrateDepartmentIsolation(store,{apply:true}),{code:'department_relationship_conflict'});
 assert.equal(await verifyDepartmentIsolationAvailability(store),false);assert.equal((await store.get("SELECT dependency_id FROM harbor.background_jobs WHERE id='business'")).dependency_id,'system');
});


test('真实capture trigger按request部门读取保留期，unlimited兼容不覆盖部门函数且拒漂移',integration,async t=>{
 const {store}=await fixture(t),at=1000;await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('recording.retention','{\"days\":7,\"version\":4}')");await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),service=createPostgresDepartments(store,{issuer,clock:()=>at}),a=await service.createDepartment(root,{name:'合成A'}),b=await service.createDepartment(root,{name:'合成B'});
 await store.run("UPDATE harbor.department_settings SET value='{\"days\":0,\"version\":1}'::jsonb WHERE department_id=$1 AND name='recording.retention'",a.id);await store.run("UPDATE harbor.department_settings SET value='{\"days\":2,\"version\":1}'::jsonb WHERE department_id=$1 AND name='recording.retention'",b.id);await store.run("UPDATE harbor.gateway_settings SET value='{\"days\":365,\"version\":5}' WHERE name='recording.retention'");
 const capture=async(id,department)=>{await store.run("INSERT INTO harbor.requests(id,department_id,started,status,ip) VALUES($1,$2,$3,'queued','synthetic')",id,department,at);await store.run("INSERT INTO harbor.recordings(request_id,turn_state,expires_at,updated) VALUES($1,'known',$2,$3)",id,at+30*86400000,at);return (await store.get('SELECT expires_at FROM harbor.recordings WHERE request_id=$1',id)).expires_at;};
 assert.equal(await capture('capture-default','department-default'),at+7*86400000);assert.equal(await capture('capture-a',a.id),unlimitedRecordingExpiry);assert.equal(await capture('capture-b',b.id),at+2*86400000);
 assert.equal(await migrateRecordingRetention(store,{apply:true}),false);await requireUnlimitedRecordingRetention(store);assert.equal(await capture('capture-b-after-compat',b.id),at+2*86400000);
 await store.run("UPDATE harbor.department_settings SET value='{\"days\":1,\"version\":2}'::jsonb WHERE department_id=$1 AND name='recording.retention'",a.id);await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',unlimitedRecordingExpiry,'capture-a');assert.equal((await store.get("SELECT expires_at FROM harbor.recordings WHERE request_id='capture-a'")).expires_at,at+86400000);
 await store.run("DELETE FROM harbor.department_settings WHERE department_id=$1 AND name='recording.retention'",b.id);await assert.rejects(capture('capture-missing-policy',b.id),{code:'23514'});
 const previous=await readFile(new URL('../migrations/recording-retention-unlimited.sql',import.meta.url),'utf8'),oldFunction=previous.slice(previous.indexOf('CREATE OR REPLACE FUNCTION harbor.apply_capture_retention()'),previous.indexOf('CREATE TRIGGER recording_update_retention'));
 await assert.rejects(store.transaction(async()=>{await store.query(oldFunction);await assert.rejects(verifyDepartmentIsolationAvailability(store),{code:'department_migration_conflict'});throw Error('synthetic function rollback');}),/synthetic function rollback/);assert.equal(await verifyDepartmentIsolationAvailability(store),true);
});
