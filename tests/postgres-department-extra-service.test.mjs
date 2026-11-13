import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,createHash} from 'node:crypto';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresAccountExtraUsage} from '../postgres-account-extra-usage.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,integration={skip:!binDirectory?'需本机私有PG':false,timeout:60000};
const actor={session_ref:createHash('sha256').update('synthetic-extra-department').digest('hex')},model='gpt-6.1-sol',protocol='responses';
const budget={basis:'reference_estimate',unit:'reference_usd_atoms',limit_atoms:'10000000000000',request_reserve_atoms:'1000000000000'};
async function fixture(t){
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),store=new PostgresStore({connection:lab.connection,poolSize:6});t.after(async()=>{await store.close();await lab.close();});await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),departments=createPostgresDepartments(store,{issuer}),root=issuer.superAdmin({credentialId:'synthetic-extra-root',credentialVersion:0}),owners={};
 for(const name of ['a','b']){const department=await departments.createDepartment(root,{name:'合成额度部门'+name}),management=await departments.issueManagementKey(root,department.id,{label:'合成管理员'}),scope=await departments.authenticateManagementKey(management.secret),account=randomUUID();await store.run("INSERT INTO harbor.accounts(id,name,home,state,department_id) VALUES($1,$2,'synthetic','ready',$3)",account,'合成额度账号'+name,department.id);owners[name]={department,management,scope,account};}
 return {store,issuer,departments,root,...owners};
}
test('部门额外额度策略与单账号配置独立，跨部门及伪造身份拒绝',integration,async t=>{
 const f=await fixture(t),service=createPostgresAccountExtraUsage(f.store);
 await service.savePolicy({enabled:true,expected_version:0},actor,{scope:f.a.scope});
 assert.deepEqual(await service.readPolicy({scope:f.a.scope}),{enabled:true,version:1});assert.deepEqual(await service.readPolicy({scope:f.b.scope}),{enabled:false,version:0});
 await service.saveAccount(f.a.account,{mode:'inherit',budget,expected_version:0,expected_identity_epoch:0},actor,{scope:f.a.scope});
 assert.equal((await service.readAccount(f.a.account,{scope:f.a.scope})).configuration.version,1);
 await assert.rejects(service.readAccount(f.a.account,{scope:f.b.scope}),{status:404});
 await assert.rejects(service.saveAccount(f.a.account,{mode:'deny',budget:null,expected_version:1,expected_identity_epoch:0},actor,{scope:f.b.scope}),{status:404});
 await assert.rejects(service.readAccount(f.a.account,{scope:{...f.a.scope}}),{status:403});await assert.rejects(service.readPolicy(),{status:403});
 const audit=await f.store.all("SELECT o.department_id FROM harbor.audit a JOIN harbor.audit_scopes o ON o.audit_id=a.id WHERE a.action LIKE 'account.extra.%'");assert.ok(audit.length>=2);assert.ok(audit.every(row=>row.department_id===f.a.department.id));
});
test('并发首次部门策略写入只接受一个版本，等待上游期间撤销管理员不提交权益',integration,async t=>{
 const f=await fixture(t),basic=createPostgresAccountExtraUsage(f.store),outcome=await Promise.allSettled([basic.savePolicy({enabled:true,expected_version:0},actor,{scope:f.a.scope}),basic.savePolicy({enabled:false,expected_version:0},actor,{scope:f.a.scope})]);
 assert.equal(outcome.filter(row=>row.status==='fulfilled').length,1);assert.equal(outcome.find(row=>row.status==='rejected').reason.code,'extra_version_conflict');
 let release,started;const ready=new Promise(resolve=>started=resolve),gate=new Promise(resolve=>release=resolve),at=Date.now();
 const service=createPostgresAccountExtraUsage(f.store,{readEntitlement:async()=>{started();await gate;return {capability:{supported:true,platform:'codex',auth_mode:'subscription',accounting_unit:'credits',manual_verification:false},entitlement:{state:'verified',source:'official',remaining_atoms:'1000',unit:'credits',models:[model],protocols:[protocol],verified_at:at,valid_until:at+60000,expires_at:null},reset_cards:null};}});
 const pending=service.refresh(f.a.account,{scope:f.a.scope});await ready;await f.departments.revokeManagementKey(f.root,f.a.department.id,f.a.management.key.id,{expected_version:0});release();
 await assert.rejects(pending,{status:403});assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.account_extra_entitlements WHERE account_id=$1',f.a.account)).n,0);
 assert.equal((await service.readAccount(f.b.account,{scope:f.b.scope})).configuration.version,0);
});
