import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {migrateConsumptionPolicy,createPostgresConsumptionPolicy} from '../postgres-consumption-policy.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresAccountAdmin} from '../postgres-account-admin.mjs';
import {createPostgresSessionAdmin} from '../postgres-session-admin.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createPostgresConsumptionRouting} from '../postgres-consumption-routing.mjs';
import {createPostgresExtraRuntime} from '../postgres-extra-runtime.mjs';
import {createApiQuotaRejection} from '../consumption-policy.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,optionsTest={skip:!url&&!socket?'需私有PG18':false};
function options(database){if(url){const target=new URL(url);if(database)target.pathname='/'+database;return {connectionString:target.href,allowPlaintextLocalNetwork:true};}return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,password:process.env.HARBOR_TEST_PG_PASSWORD,database:database??'postgres'}};}
async function fixture(t,{installed=true}={}){
 const database='harbor_dr_'+randomUUID().replaceAll('-',''),root=new PostgresStore({...options(),poolSize:1});await root.query('CREATE DATABASE '+database);const store=new PostgresStore({...options(database),poolSize:6});let role,admin;
 t.after(async()=>{await admin?.close();await store.close();await root.query('DROP DATABASE '+database);if(role)await root.query('DROP ROLE '+role);await root.close();});await store.migrate();await store.initializePricing();role=await createUsageRollupRuntime(store);await migrateConsumptionPolicy(store,{apply:true});if(installed)await migrateDepartmentIsolation(store,{apply:true});
 const at=Date.parse('2026-11-13T08:00:00Z'),clock=()=>at,issuer=createDepartmentScopeIssuer(),superAdmin=issuer.superAdmin({credentialId:'synthetic-root',credentialVersion:0}),departments=createPostgresDepartments(store,{issuer,clock});let a,b,scopeA,scopeB;
 if(installed){a=await departments.createDepartment(superAdmin,{name:'合成A'});b=await departments.createDepartment(superAdmin,{name:'合成B'});scopeA=issuer.forDepartment(superAdmin,a.id);scopeB=issuer.forDepartment(superAdmin,b.id);}
 admin=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32),clock});const created=(await admin.handle('/api/admin/members','POST',{name:'同名合成员工',quota:null,expires:null},undefined,{scope:scopeA})).value,createdB=installed?(await admin.handle('/api/admin/members','POST',{name:'同名合成员工',quota:null,expires:null},undefined,{scope:scopeB})).value:null;
 const member=await store.authenticate(created.secret,{at}),memberB=createdB&&await store.authenticate(createdB.secret,{at});
 async function employeeScope(identity=member){if(!installed)return undefined;const key=await store.get('SELECT access_version FROM harbor.keys WHERE id=$1',identity.key_id),owner=await store.get('SELECT access_version FROM harbor.members WHERE id=$1',identity.id),department=await store.get('SELECT version FROM harbor.departments WHERE id=$1',identity.department_id);return issuer.employee({departmentId:identity.department_id,memberId:identity.id,credentialId:identity.key_id,credentialVersion:key.access_version,memberVersion:owner.access_version,departmentVersion:department.version});}
 async function account(source,department=a?.id,patch={}){const id=randomUUID(),quota=JSON.stringify({checked:at,remaining:50,plan:'pro',windows:[{used:50,minutes:10080,reset:at+86400000}]});await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,api_weight,quota_json"+(installed?',department_id':'')+") VALUES($1,'合成来源账号','synthetic','ready',$2,$3,$4"+(installed?',$5':'')+")",id,source==='api'?'openai_api':'subscription',patch.api_weight??20,quota,...(installed?[department]:[]));return id;}
 const api=await account('api'),subscription=await account('subscription'),otherApi=installed?await account('api',b.id,{api_weight:100}):null,otherSubscription=installed?await account('subscription',b.id):null,router=createPostgresRouter(store,{clock}),consumption=createPostgresConsumptionRouting(store,{router,clock}),policy=createPostgresConsumptionPolicy(store,{clock}),employee=await employeeScope(),payload={model:'gpt-6.1-sol',input:'完整合成上下文'},headers={'thread-id':'same-client-thread'};
 const set=async value=>{const prior=await policy.read(member.key_id);return policy.set(member.key_id,{consumption_policy:value,expected_consumption_policy_version:prior.consumption_policy_version},{scope:scopeA});};
 const resolve=(scope=employee,identity=member,head=headers)=>store.transaction(()=>consumption.resolve(identity,payload,head,{scope}));
 return {database,store,issuer,departments,superAdmin,a,b,scopeA,scopeB,member,memberB,created,createdB,employeeScope,employee,api,subscription,otherApi,otherSubscription,router,consumption,set,resolve,payload,headers,at,clock};
}
test('候选、会话与部门设置隔离，拒伪造scope和错员工Key及global系统选账号',optionsTest,async t=>{
 const f=await fixture(t),{router}=f;
 await router.saveSettings({weighted:true},{scope:f.scopeA});await router.saveWeights({mode:'manual',weights:{[f.subscription]:100},expected_version:0},{scope:f.scopeA});
 const a=await router.readView({explanation:true,scope:f.scopeA}),b=await router.readView({explanation:true,scope:f.scopeB});assert.deepEqual(new Set(a.accounts.map(row=>row.id)),new Set([f.api,f.subscription]));assert.deepEqual(new Set(b.accounts.map(row=>row.id)),new Set([f.otherApi,f.otherSubscription]));assert.equal((await router.readView({scope:f.scopeB})).settings.weighted,false);
 await assert.rejects(router.saveWeights({mode:'manual',weights:{[f.subscription]:50,[f.otherSubscription]:50},expected_version:1},{scope:f.scopeA}),{status:400});assert.equal((await router.readView({scope:f.scopeA})).weights.configuration.version,1);
 assert.equal((await f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_strategy'")),undefined);
 await router.saveTransferSchedule({times:['03:00'],expected_version:0},{scope:f.scopeA});assert.deepEqual((await router.readView({scope:f.scopeA})).transfers.times,['03:00']);assert.notDeepEqual((await router.readView({scope:f.scopeB})).transfers.times,['03:00']);
 await f.set('api_only');const route=await f.resolve();assert.equal(route.account.id,f.api);assert.equal(route.session.department_id,f.a.id);assert.equal(route.session.member_id,f.member.id);
 await assert.rejects(f.resolve(JSON.parse(JSON.stringify(f.employee))),{code:'department_scope_required'});await assert.rejects(f.resolve(f.employee,f.memberB),{code:'department_forbidden'});
 const otherKey=f.issuer.employee({departmentId:f.a.id,memberId:f.member.id,credentialId:f.createdB.keyId,credentialVersion:0,memberVersion:0,departmentVersion:0});await assert.rejects(f.resolve(otherKey),{code:'department_forbidden'});
 for(const scope of [undefined,f.superAdmin,f.issuer.system({purpose:'scan'})])await assert.rejects(router.catalogCandidates({scope}));
 const view=await router.catalogCandidates({scope:f.employee,policy:'api_only'});assert.deepEqual(view.sources.api.accountIds,[f.api]);
 await f.store.run('UPDATE harbor.accounts SET enabled=0 WHERE id=$1',f.api);await assert.rejects(f.resolve(f.employee,f.member,{'thread-id':'no-local-api'}),{code:'no_account'});
});
test('固定API绑定与账号停用迁移只用同部门对象且事务失败不污染其他部门',optionsTest,async t=>{
 const f=await fixture(t),sessions=createPostgresSessionAdmin(f.store,{clock:f.clock}),create=(account,member=f.member,scope=f.scopeA)=>sessions.handle('/api/admin/sessions','POST',{member_id:member.id,account_id:account},{scope});
 await assert.rejects(create(f.otherApi),{code:'department_object_not_found'});await assert.rejects(create(f.api,f.memberB),{code:'department_object_not_found'});
 const fixed=(await create(f.api)).value.session_id;await assert.rejects(sessions.handle('/api/admin/sessions/'+fixed,'PATCH',{account_id:f.otherApi,expected_route_version:0},{scope:f.scopeA}),{code:'department_object_not_found'});
 const replacement=await f.store.get('SELECT * FROM harbor.sessions WHERE id=$1',fixed);assert.equal(replacement.account_id,f.api);
 const accountAdmin=createPostgresAccountAdmin(f.store,{runtime:'/synthetic-no-io',router:f.router,checkApi:async()=>[],checkSubscription:async()=>true,clock:f.clock});t.after(()=>accountAdmin.close());const migrated=await accountAdmin.handle('/api/admin/accounts/'+f.api,'PATCH',{enabled:false},{scope:f.scopeA});assert.equal(migrated.value.moved,1);assert.equal((await f.store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',fixed)).account_id,f.subscription);assert.equal((await f.store.get('SELECT enabled FROM harbor.accounts WHERE id=$1',f.otherApi)).enabled,1);
 await assert.rejects(f.router.disableAccount(f.subscription,{scope:f.scopeA}),{code:'disable_no_target'});assert.equal((await f.store.get('SELECT enabled FROM harbor.accounts WHERE id=$1',f.subscription)).enabled,1);
});
test('真实耗尽备用保持原请求归属且不借其他部门订阅，旧三版本scope恢复后不可派发',optionsTest,async t=>{
 const f=await fixture(t);await f.set('api_first');const route=await f.resolve(),requestId=randomUUID(),attempt={source:'api',account_id:f.api,state:'rejected',usage_known:true,started:f.at,ended:f.at,input:0,output:0,cached:0,error_code:'upstream_quota_exhausted'};
 await f.store.run("INSERT INTO harbor.requests(id,department_id,member_id,key_id,account_id,session_id,route_version,started,status,ip,model,handoff_input,consumption_policy,consumption_policy_version,consumption_source,consumption_fallback,consumption_attempts) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'running','synthetic',$9,'replayable',$10,$11,'api',false,$12::jsonb)",requestId,f.a.id,f.member.id,f.member.key_id,f.api,route.session.id,route.session.route_version,f.at,f.payload.model,route.consumption.consumption_policy,route.consumption.consumption_policy_version,JSON.stringify([attempt]));
 const fallback=()=>f.store.transaction(()=>f.consumption.fallback({member:f.member,payload:f.payload,headers:f.headers,options:{scope:f.employee},requestId,route,error:createApiQuotaRejection(),attempts:0,outputStarted:false,contextReplayable:true}));
 await f.store.run('UPDATE harbor.accounts SET enabled=0 WHERE id=$1',f.subscription);await assert.rejects(fallback(),{code:'no_account'});await f.store.run('UPDATE harbor.accounts SET enabled=1 WHERE id=$1',f.subscription);assert.equal((await fallback()).account.id,f.subscription);assert.equal((await f.store.get('SELECT account_id FROM harbor.requests WHERE id=$1',requestId)).account_id,f.api);
 await f.store.run('UPDATE harbor.members SET enabled=0 WHERE id=$1',f.member.id);await f.store.run('UPDATE harbor.members SET enabled=1 WHERE id=$1',f.member.id);await assert.rejects(f.resolve(),{code:'department_forbidden'});const fresh=await f.employeeScope();await f.resolve(fresh,f.member,{'thread-id':'fresh'});
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});await assert.rejects(f.resolve(fresh),{code:'department_forbidden'});
 const newest=await f.employeeScope();await f.store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',f.member.key_id);await f.store.run('UPDATE harbor.keys SET enabled=1 WHERE id=$1',f.member.key_id);await assert.rejects(f.resolve(newest),{code:'department_forbidden'});
});
test('最终派发分类按部门设置判断，停用部门超管只读且迁移前legacy路由保持',optionsTest,async t=>{
 const f=await fixture(t),extra=createPostgresExtraRuntime(f.store,{clock:f.clock});await f.router.saveSettings({weighted:true},{scope:f.scopeA});await f.router.saveWeights({mode:'manual',weights:{[f.subscription]:100},expected_version:0},{scope:f.scopeA});await f.store.run("UPDATE harbor.accounts SET route_block='quota_exhausted' WHERE id=$1",f.subscription);
 const account=await f.store.get('SELECT * FROM harbor.accounts WHERE id=$1',f.subscription);assert.equal((await extra.classify(account,{scope:f.employee,model:f.payload.model,protocol:'responses'})).mode,'manual_extra');await assert.rejects(extra.classify(account,{scope:await f.employeeScope(f.memberB)}),{code:'department_object_not_found'});
 await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});assert.equal((await f.router.readView({scope:f.scopeA})).settings.weighted,true);await assert.rejects(f.router.saveSettings({weighted:false},{scope:f.scopeA}),{code:'department_forbidden'});await assert.rejects(f.resolve(),{code:'department_forbidden'});
 const legacy=await fixture(t,{installed:false});await legacy.set('api_only');assert.equal((await legacy.resolve()).account.id,legacy.api);
});

test('真实最终派发在供应方调用前重验三版本及请求归属，已派发结算不受停用回滚',optionsTest,async t=>{
 const f=await fixture(t),runtime=createPostgresExtraRuntime(f.store,{clock:f.clock});await f.set('api_only');const route=await f.resolve();let calls=0;
 const queued=async()=>{const requestId=randomUUID();await f.store.run("INSERT INTO harbor.requests(id,department_id,member_id,key_id,account_id,session_id,started,status,ip,model) VALUES($1,$2,$3,$4,$5,$6,$7,'queued','synthetic',$8)",requestId,f.a.id,f.member.id,f.member.key_id,f.api,route.session.id,f.at,f.payload.model);return requestId;};
 const edge=(requestId,scope)=>runtime.boundary({requestId,protocol:'responses',scope,packagePreference:f.router.dispatchPreferenceFor(scope),prepare:async()=>{},validate:async()=>{}});
 const accepted=await queued();await edge(accepted,f.employee).run(async()=>{calls++;});assert.equal(calls,1);assert.equal((await f.store.get('SELECT status FROM harbor.requests WHERE id=$1',accepted)).status,'running');
 const pending=await queued();await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:false,expected_version:0});await f.departments.updateDepartment(f.superAdmin,f.a.id,{enabled:true,expected_version:1});await assert.rejects(edge(pending,f.employee).run(async()=>{calls++;}),{code:'department_forbidden'});assert.equal(calls,1);
 await f.store.finish(accepted,{status:'ok',input:1,output:1,cached:0},f.at+1000);assert.deepEqual(await f.store.get('SELECT department_id,status FROM harbor.requests WHERE id=$1',accepted),{department_id:f.a.id,status:'ok'});
 const scopeB=await f.employeeScope(f.memberB);await assert.rejects(edge(pending,scopeB).run(async()=>{calls++;}),{status:404,code:'department_object_not_found'});assert.equal(calls,1);
});

test('两个真实连接中员工选账号与同员工会话PATCH遵守身份先于路由锁',optionsTest,async t=>{
 const f=await fixture(t),sessionAdmin=createPostgresSessionAdmin(f.store,{clock:f.clock}),session=(await sessionAdmin.handle('/api/admin/sessions','POST',{member_id:f.member.id,account_id:f.api},{scope:f.scopeA})).value.session_id;
 const selectStore=new PostgresStore({...options(f.database),poolSize:1}),adminStore=new PostgresStore({...options(f.database),poolSize:1});let releaseMember,memberHeld,selectResult,adminResult;
 const held=new Promise(resolve=>{memberHeld=resolve;}),release=new Promise(resolve=>{releaseMember=resolve;});
 try{
  await selectStore.query("SET application_name='synthetic-department-select'");await adminStore.query("SET application_name='synthetic-department-session-admin'");
  const original=adminStore.query.bind(adminStore);adminStore.query=async(sql,args)=>{const result=await original(sql,args);if(sql.startsWith('SELECT * FROM harbor.members WHERE id=$1')&&sql.endsWith('FOR UPDATE')){memberHeld();await release;}return result;};
  const manager=createPostgresSessionAdmin(adminStore,{clock:f.clock}),router=createPostgresRouter(selectStore,{clock:f.clock});
  adminResult=manager.handle('/api/admin/sessions/'+session,'PATCH',{enabled:false},{scope:f.scopeA}).then(value=>({value}),error=>({error}));await held;
  selectResult=selectStore.transaction(()=>router.selectAccount({scope:f.employee,model:f.payload.model})).then(value=>({value}),error=>({error}));
  const deadline=Date.now()+5000;for(;;){const waiting=await f.store.get("SELECT pid FROM pg_stat_activity WHERE application_name='synthetic-department-select' AND wait_event_type='Lock'");if(waiting){assert.equal((await f.store.get("SELECT count(*) n FROM pg_locks WHERE pid=$1 AND locktype='advisory' AND granted",waiting.pid)).n,0,'等待员工身份时不能持有路由advisory锁');break;}assert.ok(Date.now()<deadline,'员工选账号应等待同员工身份锁');await new Promise(resolve=>setTimeout(resolve,10));}
  releaseMember();const [selected,managed]=await Promise.all([selectResult,adminResult]);assert.equal(selected.error,undefined);assert.equal(managed.error,undefined);assert.ok([f.api,f.subscription].includes(selected.value.id));assert.equal(managed.value.status,200);
 }finally{releaseMember?.();await Promise.all([selectResult,adminResult]);await adminStore.close();await selectStore.close();}
});
