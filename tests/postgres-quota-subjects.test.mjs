import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {migrateQuotaSubjects,verifyQuotaSubjectAvailability,createPostgresQuotaSubjects} from '../postgres-quota-subjects.mjs';
import {createCodexQuotaSubjectIdentity,codexQuotaSubject} from '../quota-subject.mjs';
import {normalizeLimits} from '../rates.mjs';
import {decorateReferenceRequest} from '../pricing.mjs';
import {createPostgresAccountQuotas} from '../postgres-account-quotas.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresRouter,lockPostgresRouting} from '../postgres-routing.mjs';
import {createPostgresAccountAdmin} from '../postgres-account-admin.mjs';
import {invalidateQuotaSubjectIdentity} from '../postgres-quota-subjects.mjs';
import {createPostgresStateQueries} from '../postgres-state-query.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments} from '../postgres-departments.mjs';
import {createPostgresQuotaConfirmation,quotaConfirmationView} from '../postgres-quota-confirmation.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需既有隔离PG18测试入口':false},identity=createCodexQuotaSubjectIdentity(Buffer.alloc(32,13),{schemaVersion:'0.167.2'});
function options(database){
  if(url){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.toString(),allowPlaintextLocalNetwork:true};}
  return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',password:process.env.HARBOR_TEST_PG_PASSWORD,database:database||'postgres'}};
}
async function fixture(t,{install=true,departments=false}={}){
  const name='harbor_subject_'+randomUUID().replaceAll('-',''),admin=new PostgresStore({...options(),poolSize:1});
  await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8'");
  const store=new PostgresStore({...options(name),poolSize:4});
  t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);await admin.close();});
  await store.migrate({quotaSubjects:install});
  if(departments){await migrateDepartmentIsolation(store,{apply:true});for(const id of ['department-a','department-b'])await store.run('INSERT INTO harbor.departments(id,name,created,updated) VALUES($1,$1,1,1)',id);}
  let now=1800000000000;const subjects=createPostgresQuotaSubjects(store,{identity,clock:()=>now});
  async function account(id,{department,apiDigest}={}){
    await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,api_secret_digest"+(departments?',department_id':'')+") VALUES($1,$1,$2,'ready',$3,$4"+(departments?',$5':'')+")",id,'/synthetic/'+id,apiDigest?'openai_api':'subscription',apiDigest??null,...(departments?[department??'department-a']:[]));return store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);
  }
  async function bind(account,{backend='fixture-shared-account',used=30,permission=true,checkedFrom=now-1}={}){
    const quota=normalizeLimits({accountId:backend,ordinaryUsageAllowed:permission,rateLimits:{limitId:'codex',primary:{usedPercent:used,windowDurationMins:300,resetsAt:(now+60000)/1000},planType:'pro'}},now,{subjectIdentity:identity});
    await store.transaction(()=>subjects.refresh(account,codexQuotaSubject(quota),quota,{checkedFrom}));return quota;
  }
  async function request(id,account){await store.run("INSERT INTO harbor.requests(id,account_id,started,status,ip"+(departments?',department_id':'')+") VALUES($1,$2,$3,'queued','synthetic'"+(departments?',$4':'')+")",id,account.id,now,...(departments?[account.department_id]:[]));}
  const admit=(id,account,options)=>store.transaction(()=>subjects.admit(id,account,options));
  return {store,subjects,account,bind,request,admit,now:()=>now,advance:n=>now+=n};
}

test('共享主体是可选增量合同，部分安装、marker漂移拒绝且不改旧迁移链',integration,async t=>{
  const f=await fixture(t,{install:false});assert.equal(await verifyQuotaSubjectAvailability(f.store),false);
  const a=await f.account('a');await f.request('r',a);assert.deepEqual(await f.admit('r',a),{coordinated:false});
  const before=await f.store.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version');assert.equal(await migrateQuotaSubjects(f.store,{apply:true}),true);assert.equal(await migrateQuotaSubjects(f.store,{apply:true}),false);
  assert.deepEqual(await f.store.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version'),before);await f.store.verifySchema();
  await assert.rejects(f.store.transaction(async()=>{await f.store.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.quota-subject-coordination'");await verifyQuotaSubjectAvailability(f.store);}),{code:'quota_subject_contract_invalid'});
});

test('部分DDL和同名弱化约束不能伪装成已验证主体合同',integration,async t=>{
  const f=await fixture(t,{install:false});
  await f.store.query('CREATE TABLE harbor.quota_subjects(fake text)');await assert.rejects(verifyQuotaSubjectAvailability(f.store),{code:'quota_subject_contract_invalid'});await f.store.query('DROP TABLE harbor.quota_subjects');
  await migrateQuotaSubjects(f.store,{apply:true});
  await assert.rejects(f.store.transaction(async()=>{await f.store.query('ALTER TABLE harbor.requests DROP CONSTRAINT quota_subject_request_pair');await f.store.query('ALTER TABLE harbor.requests ADD CONSTRAINT quota_subject_request_pair CHECK(true)');await verifyQuotaSubjectAvailability(f.store);}),{code:'quota_subject_contract_invalid'});
  assert.equal(await verifyQuotaSubjectAvailability(f.store),true);
});

test('跨部门相同可信桶共用许可，单绑定和未知仍走原行为',integration,async t=>{
  const f=await fixture(t,{departments:true}),a=await f.account('a',{department:'department-a'}),b=await f.account('b',{department:'department-b'}),unknown=await f.account('unknown',{department:'department-b'});
  await f.bind(a);await f.request('single-1',a);await f.request('single-2',a);assert.equal((await f.admit('single-1',a)).coordinated,true);assert.equal((await f.admit('single-2',a)).coordinated,true);
  await f.bind(b);await f.request('b-request',b);await assert.rejects(f.admit('b-request',b),{code:'shared_supplier_quota_busy'});
  await f.request('unknown-request',unknown);assert.equal((await f.admit('unknown-request',unknown)).coordinated,false);
  await assert.rejects(f.admit('b-request',a),{code:'quota_subject_request_binding_conflict'});
  assert.equal((await f.store.get("SELECT account_id FROM harbor.requests WHERE id='b-request'")).account_id,b.id);
});

test('同时受理只一方取得共享许可，已知取消和真实终态立即释放且重结算不重复消费',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.bind(a);await f.bind(b);
  const accept=(id,account)=>f.store.transaction(async()=>{await f.request(id,account);return f.subjects.admit(id,account);});
  const results=await Promise.allSettled([accept('r-a',a),accept('r-b',b)]);assert.equal(results.filter(r=>r.status==='fulfilled').length,1);
  const winner=results[0].status==='fulfilled'?'r-a':'r-b',loser=winner==='r-a'?'r-b':'r-a',target=loser==='r-a'?a:b;
  await f.store.finish(winner,{status:'cancelled',usageKnown:true,input:0,output:0});assert.equal((await accept(loser,target)).coordinated,true);
  await f.store.finish(loser,{status:'ok',usageKnown:true,input:100,output:10});await f.store.finish(loser,{status:'ok',usageKnown:true,input:999,output:999});
  assert.equal((await f.store.get('SELECT input+output n FROM harbor.requests WHERE id=$1',loser)).n,110);
});

test('未知执行保留许可，刷新起点不覆盖查询期间完成的未知消耗',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.bind(a);await f.bind(b);await f.request('unknown',a);await f.admit('unknown',a);
  await f.store.run("UPDATE harbor.requests SET ended=$1,status='unknown',usage_unknown=1 WHERE id='unknown'",f.now());await f.request('next',b);
  await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_busy'});
  const started=f.now();f.advance(10);await f.bind(b,{checkedFrom:started});await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_busy'});
  f.advance(1);await f.bind(b);assert.equal((await f.admit('next',b)).coordinated,true);
});

test('共享新快照保留查询期间已知消耗，不把各部门百分比相加',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.bind(a,{used:99});await f.bind(b,{used:99});
  await f.request('spent',a);await f.admit('spent',a);await f.store.run("UPDATE harbor.requests SET ended=$1,status='ok',input=10001,usage_unknown=0 WHERE id='spent'",f.now());await f.request('next',b);
  await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_unavailable'});
  const queryStart=f.now();f.advance(5);await f.bind(b,{used:99,checkedFrom:queryStart});await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_unavailable'});
  f.advance(1);await f.bind(b,{used:90});assert.equal((await f.admit('next',b)).coordinated,true);
});

test('同一API凭据协调而不同凭据不推断同项目；重登不改已冻结请求主体',integration,async t=>{
  const f=await fixture(t),a=await f.account('a',{apiDigest:'a'.repeat(64)}),b=await f.account('b',{apiDigest:'a'.repeat(64)}),c=await f.account('c',{apiDigest:'b'.repeat(64)});
  await f.request('a-request',a);await f.admit('a-request',a);await f.request('b-request',b);await assert.rejects(f.admit('b-request',b),{code:'shared_supplier_quota_busy'});
  await f.request('c-request',c);await f.admit('c-request',c);
  const saved=(await f.store.get("SELECT quota_subject_ref FROM harbor.requests WHERE id='a-request'")).quota_subject_ref;
  await f.store.run("UPDATE harbor.accounts SET route_epoch=route_epoch+1,api_secret_digest=$1 WHERE id='a'",'c'.repeat(64));
  const changed=await f.store.get("SELECT * FROM harbor.accounts WHERE id='a'");await f.request('new-identity',changed);await f.admit('new-identity',changed);
  assert.equal((await f.store.get("SELECT quota_subject_ref FROM harbor.requests WHERE id='a-request'")).quota_subject_ref,saved);
  await assert.rejects(f.admit('b-request',b),{code:'shared_supplier_quota_busy'});
});

test('人工额外额度不冒认为普通桶，公共请求DTO不泄露内部主体',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.bind(a,{permission:false});await f.bind(b,{permission:false});await f.request('r',a);
  assert.deepEqual(await f.admit('r',a,{mode:'manual_extra'}),{coordinated:false,reason:'extra_scope_unknown'});
  const row=await f.store.get("SELECT * FROM harbor.requests WHERE id='r'");assert.equal(row.quota_subject_ref,null);
  const dto=decorateReferenceRequest({...row,quota_subject_ref:'d'.repeat(64),quota_subject_scope:'account_metered_bucket'}, {},0);assert.doesNotMatch(JSON.stringify(dto),/quota_subject|dddddddd/);
});

test('真实额度恢复与停用再启用保持主体，只有身份变更使绑定失效',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b'),router=createPostgresRouter(f.store,{clock:f.now}),queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:2});
  const quotas=createPostgresAccountQuotas(f.store,{router,queue,clock:f.now,subjectIdentity:identity,readQuota:async()=>({accountId:'fixture-shared-account',ordinaryUsageAllowed:true,rateLimits:{limitId:'codex',planType:'pro',primary:{usedPercent:30,windowDurationMins:300,resetsAt:(f.now()+60000)/1000}}})});
  const admin=createPostgresAccountAdmin(f.store,{runtime:'/synthetic/subject-admin',encryptionKey:Buffer.alloc(32,13),router,clock:f.now,checkApi:async()=>[],checkSubscription:async()=>true});
  t.after(async()=>{quotas.stop();await queue.close();await quotas.close();await admin.close();});
  await quotas.refresh(a,{force:true});await quotas.refresh(b,{force:true});await f.request('running-other',b);await f.admit('running-other',b);
  await router.block(await quotas.load(a.id),'quota_exhausted');const before=await quotas.load(a.id);assert.equal((await quotas.refresh(before,{force:true})).refreshed,true);
  const recovered=await quotas.load(a.id);assert.ok(recovered.route_epoch>before.route_epoch);assert.equal(recovered.quota_identity_epoch,0);await f.request('after-recover',recovered);await assert.rejects(f.admit('after-recover',recovered),{code:'shared_supplier_quota_busy'});
  await admin.handle('/api/admin/accounts/'+a.id,'PATCH',{enabled:false});await admin.handle('/api/admin/accounts/'+a.id,'PATCH',{enabled:true});const enabled=await quotas.load(a.id);await f.request('after-enable',enabled);await assert.rejects(f.admit('after-enable',enabled),{code:'shared_supplier_quota_busy'});
  const frozen=(await f.store.get("SELECT quota_subject_ref FROM harbor.requests WHERE id='running-other'")).quota_subject_ref;
  await f.store.transaction(()=>invalidateQuotaSubjectIdentity(f.store,a.id));const changed=await quotas.load(a.id);await f.request('new-identity',changed);assert.equal((await f.admit('new-identity',changed)).coordinated,false);
  assert.equal((await f.store.get("SELECT quota_subject_ref FROM harbor.requests WHERE id='running-other'")).quota_subject_ref,frozen);
});

test('账号DTO仅显示本部门安全范围枚举，跨scope及身份变化不复用旧提示',integration,async t=>{
  const f=await fixture(t,{departments:true});await f.store.initializePricing();const a=await f.account('a',{department:'department-a'}),b=await f.account('b',{department:'department-b'});await f.bind(a);
  const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-subject-root',credentialVersion:0}),departments=createPostgresDepartments(f.store,{issuer,clock:f.now});
  const keyA=await departments.issueManagementKey(root,'department-a',{label:'合成A'}),keyB=await departments.issueManagementKey(root,'department-b',{label:'合成B'}),scopeA=await departments.authenticateManagementKey(keyA.secret),scopeB=await departments.authenticateManagementKey(keyB.secret),queries=createPostgresStateQueries(f.store,{clock:f.now});t.after(()=>queries.close());
  const read=scope=>queries.read({view:'accounts',people:'none',sessionPage:1,sessionPageSize:20},{scope});
  const dtoA=await read(scopeA);assert.deepEqual(dtoA.accounts.map(row=>row.id),[a.id]);assert.deepEqual(dtoA.accounts[0].supplier_quota,{state:'known',ordinary_scope:'account_metered_bucket',extra_scope:'unknown'});
  assert.doesNotMatch(JSON.stringify(dtoA),/quota_subject_ref|quota_identity_epoch|fixture-shared-account|department-b/);
  const dtoB=await read(scopeB);assert.deepEqual(dtoB.accounts.map(row=>row.id),[b.id]);assert.deepEqual(dtoB.accounts[0].supplier_quota,{state:'unknown',ordinary_scope:'unknown',extra_scope:'unknown'});
  await f.store.transaction(()=>invalidateQuotaSubjectIdentity(f.store,a.id));assert.equal((await read(scopeA)).accounts[0].supplier_quota.state,'unknown');
});

test('查询开始后的新拒绝不能被该查询较早生成的正向快照清除',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.bind(a);await f.bind(b);await f.request('denied',a);await f.admit('denied',a);
  const queryStarted=f.now();f.advance(10);await f.store.finish('denied',{status:'rejected',usageKnown:true,quotaSubjectDenial:true},f.now());f.advance(10);
  await f.bind(b,{checkedFrom:queryStarted});await f.request('next',b);await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_unavailable'});
  f.advance(1);await f.bind(b);assert.equal((await f.admit('next',b)).coordinated,true);
});

test('首次识别纳入同代际未知在途，重登后旧代际映射仍保留但不改历史ref',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.request('already-running',a);assert.equal((await f.admit('already-running',a)).coordinated,false);
  await f.bind(a);await f.bind(b);await f.request('next',b);await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_busy'});
  const unknown=await f.store.get("SELECT quota_subject_ref,quota_account_identity_epoch,quota_ordinary FROM harbor.requests WHERE id='already-running'");assert.deepEqual(unknown,{quota_subject_ref:null,quota_account_identity_epoch:0,quota_ordinary:true});
  await f.store.transaction(()=>invalidateQuotaSubjectIdentity(f.store,a.id));const changed=await f.store.get('SELECT * FROM harbor.accounts WHERE id=$1',a.id);await f.bind(changed,{backend:'fixture-other-backend'});
  await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_busy'});assert.equal((await f.store.get("SELECT quota_subject_ref FROM harbor.requests WHERE id='already-running'")).quota_subject_ref,null);
  await f.store.finish('already-running',{status:'ok',usageKnown:true,input:10},f.now());assert.equal((await f.admit('next',b)).coordinated,true);
});

test('管理员仅释放本部门已终态未知请求许可，原用量保留且重试只记一次事实与审计',integration,async t=>{
  const f=await fixture(t,{departments:true}),a=await f.account('a',{department:'department-a',apiDigest:'a'.repeat(64)}),b=await f.account('b',{department:'department-b',apiDigest:'a'.repeat(64)});
  await f.request('unknown-api',a);await f.admit('unknown-api',a);await f.store.finish('unknown-api',{status:'unknown',usageKnown:false},f.now());await f.request('waiting-b',b);await assert.rejects(f.admit('waiting-b',b),{code:'shared_supplier_quota_busy'});
  const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-confirmation-root',credentialVersion:0}),departments=createPostgresDepartments(f.store,{issuer,clock:f.now}),keyA=await departments.issueManagementKey(root,'department-a',{label:'合成A'}),keyB=await departments.issueManagementKey(root,'department-b',{label:'合成B'}),scopeA=await departments.authenticateManagementKey(keyA.secret),scopeB=await departments.authenticateManagementKey(keyB.secret),admin=createPostgresQuotaConfirmation(f.store,{clock:f.now}),body={confirmed:true,statement:'upstream_finished',reason:'已核实上游任务结束'};
  const original=await f.store.get("SELECT status,business_status,usage_unknown,input,output,manual_cost_atoms,ended FROM harbor.requests WHERE id='unknown-api'");
  await assert.rejects(admin.confirm('unknown-api',body,{scope:scopeB}),{code:'department_object_not_found'});await assert.rejects(admin.confirm('unknown-api',body,{scope:JSON.parse(JSON.stringify(scopeA))}),{code:'department_scope_required'});
  const result=await admin.confirm('unknown-api',body,{scope:scopeA});assert.equal(result.source,'administrator_confirmation');assert.equal(result.replayed,false);
  assert.equal((await admin.confirm('unknown-api',{...body,reason:'重复请求不得覆盖原理由'},{scope:scopeA})).replayed,true);
  assert.deepEqual(await f.store.get("SELECT status,business_status,usage_unknown,input,output,manual_cost_atoms,ended FROM harbor.requests WHERE id='unknown-api'"),original);
  const fact=await f.store.get("SELECT * FROM harbor.quota_subject_confirmations WHERE request_id='unknown-api'");assert.equal(fact.reason,body.reason);assert.equal(fact.actor_id,scopeA.credential_id);assert.equal((await f.store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='request.upstream.confirmed'")).n,1);
  const view=await quotaConfirmationView(f.store,await f.store.get("SELECT * FROM harbor.requests WHERE id='unknown-api'"));assert.equal(view.can_confirm_upstream,false);assert.equal(view.upstream_confirmation.source,'administrator_confirmation');assert.doesNotMatch(JSON.stringify(view),/subject_ref|actor_id|shared/);
  assert.equal((await f.admit('waiting-b',b)).coordinated,true);
});

test('真在途、已知用量和核对期间撤销管理凭据均不能人工释放',integration,async t=>{
  const f=await fixture(t,{departments:true}),a=await f.account('a',{department:'department-a',apiDigest:'a'.repeat(64)}),issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-confirmation-root',credentialVersion:0}),departments=createPostgresDepartments(f.store,{issuer,clock:f.now}),key=await departments.issueManagementKey(root,'department-a',{label:'合成A'}),scope=await departments.authenticateManagementKey(key.secret),admin=createPostgresQuotaConfirmation(f.store,{clock:f.now}),body={confirmed:true,statement:'not_executed',reason:'已核实未执行'};
  await f.request('active',a);await f.admit('active',a);await assert.rejects(admin.confirm('active',body,{scope}),{code:'upstream_confirmation_not_eligible'});
  await f.store.finish('active',{status:'cancelled',usageKnown:true},f.now());await assert.rejects(admin.confirm('active',body,{scope}),{code:'upstream_confirmation_not_eligible'});
  await f.request('unknown',a);await f.admit('unknown',a);await f.store.finish('unknown',{status:'unknown',usageKnown:false},f.now());
  const database=(await f.store.get('SELECT current_database() name')).name,control=new PostgresStore({...options(database),poolSize:1});
  try{
    await assert.rejects(admin.confirm('unknown',body,{scope,revalidateScope:async original=>{await control.run('UPDATE harbor.department_admin_keys SET enabled=false,version=version+1 WHERE id=$1',scope.credential_id);return original;}}),{code:'department_forbidden'});
    const renewed=await departments.issueManagementKey(root,'department-a',{label:'合成新管理Key'}),current=await departments.authenticateManagementKey(renewed.secret);
    await assert.rejects(admin.confirm('unknown',body,{scope:current,revalidateScope:async original=>{await control.run("UPDATE harbor.departments SET enabled=false WHERE id='department-a'");return original;}}),{code:'department_forbidden'});
    assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.quota_subject_confirmations')).n,0);assert.equal((await f.store.get("SELECT usage_unknown FROM harbor.requests WHERE id='unknown'")).usage_unknown,1);
  }finally{await control.close();}
});

test('人工核对只释放指定未知许可，不解除官方普通额度拒绝',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.bind(a);await f.bind(b);await f.request('unknown-denied',a);await f.admit('unknown-denied',a);await f.store.finish('unknown-denied',{status:'unknown',usageKnown:false,quotaSubjectDenial:true},f.now());
  const issuer=createDepartmentScopeIssuer(),scope=issuer.superAdmin({credentialId:'synthetic-legacy-admin',credentialVersion:0}),admin=createPostgresQuotaConfirmation(f.store,{clock:f.now});
  await admin.confirm('unknown-denied',{confirmed:true,statement:'upstream_finished',reason:'仅核实上游已结束，未核实消耗'},{scope});await f.request('next',b);
  await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_unavailable'});assert.equal((await f.store.get("SELECT usage_unknown FROM harbor.requests WHERE id='unknown-denied'")).usage_unknown,1);
  f.advance(2);await f.bind(b);assert.equal((await f.admit('next',b)).coordinated,true);
});


test('首次可信绑定前两个旧路由请求的拒绝保留原终态，并阻断查询期间的陈旧正快照',integration,async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');
  await f.request('first-denial',a);await f.admit('first-denial',a);await f.request('late-denial',a);await f.admit('late-denial',a);
  const router=createPostgresRouter(f.store,{clock:f.now});
  f.advance(1);await router.block(a,'quota_exhausted');await f.store.finish('first-denial',{status:'rejected',usageKnown:true,quotaSubjectDenial:true},f.now());
  f.advance(1);const queryStarted=f.now();f.advance(1);assert.equal(await router.block(a,'quota_exhausted'),false);
  await f.store.finish('late-denial',{status:'rejected',usageKnown:true,quotaSubjectDenial:true},f.now());const denialAt=f.now();
  const terminal=await f.store.get("SELECT quota_subject_ref,quota_ordinary_denied,usage_unknown,input,output FROM harbor.requests WHERE id='late-denial'");
  assert.deepEqual(terminal,{quota_subject_ref:null,quota_ordinary_denied:true,usage_unknown:0,input:0,output:0});
  f.advance(1);await f.bind(a,{checkedFrom:queryStarted});await f.bind(b,{checkedFrom:queryStarted});await f.request('next',b);
  await assert.rejects(f.admit('next',b),{code:'shared_supplier_quota_unavailable'});
  assert.equal((await f.store.get('SELECT denial_at FROM harbor.quota_subjects')).denial_at,denialAt);
  assert.equal((await f.store.get("SELECT quota_subject_ref FROM harbor.requests WHERE id='late-denial'")).quota_subject_ref,null);
  f.advance(2);await f.bind(b);assert.equal((await f.admit('next',b)).coordinated,true);
  // Rebinding/replaying an old exact denial cannot undo a later trusted read.
  await f.store.finish('late-denial',{status:'rejected',usageKnown:true,quotaSubjectDenial:true},denialAt);await f.bind(a);
  await f.store.finish('next',{status:'ok',usageKnown:true},f.now());await f.request('after-replay',b);assert.equal((await f.admit('after-replay',b)).coordinated,true);
  assert.doesNotMatch(JSON.stringify(decorateReferenceRequest({...terminal,quota_ordinary_denied:true},{},0)),/quota_ordinary_denied/);
});


test('普通拒绝结算和额度刷新统一先取路由边界，双连接交错不会形成账号与主体死锁',{...integration,timeout:15000},async t=>{
  const f=await fixture(t),a=await f.account('a'),b=await f.account('b');await f.bind(a);await f.bind(b);await f.request('denial-lock-order',b);await f.admit('denial-lock-order',b);
  const second=new PostgresStore({...options(f.store.pool.options.database),poolSize:1,statementTimeoutMs:4000});
  let ready;const readyPromise=new Promise(resolve=>ready=resolve),query=second.query.bind(second);
  second.query=(text,...args)=>{if(typeof text==='string'&&text.includes('pg_advisory_xact_lock'))ready();return query(text,...args);};
  let release;const releasePromise=new Promise(resolve=>release=resolve);
  const router=createPostgresRouter(f.store,{clock:f.now});let refresh,finish;
  try{
    refresh=f.store.transaction(async()=>{
      await lockPostgresRouting(f.store);await f.store.get('SELECT * FROM harbor.accounts WHERE id=$1 FOR UPDATE',a.id);await f.bind(a);release();
      await readyPromise;await router.syncWeights(false,f.now());return 'refreshed';
    });
    await releasePromise;finish=second.finish('denial-lock-order',{status:'rejected',usageKnown:true,quotaSubjectDenial:true},f.now());
    assert.equal((await Promise.all([refresh,finish]))[0],'refreshed');
    assert.equal((await f.store.get("SELECT usage_unknown FROM harbor.requests WHERE id='denial-lock-order'")).usage_unknown,0);
    assert.equal((await f.store.get("SELECT quota_ordinary_denied FROM harbor.requests WHERE id='denial-lock-order'")).quota_ordinary_denied,true);
    assert.equal((await f.store.get('SELECT snapshot FROM harbor.quota_subjects')).snapshot.ordinaryUsageAllowed,false);
  }finally{ready();release();await Promise.allSettled([refresh,finish]);await second.close();}
});
