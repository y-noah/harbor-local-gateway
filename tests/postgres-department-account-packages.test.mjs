import {createPostgresAccountPackages} from '../postgres-account-packages.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID,randomBytes} from 'node:crypto';
import {AppError} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
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
 const root=new PostgresStore(database()),name='harbor_dept_account_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')};let auth;const departments=createPostgresDepartments(store,{issuer,authorizeSuperAdmin:credential=>auth.authorizeSuperAdmin(credential)});auth=createAdminAuth(config,{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}});
 const login=async secret=>{const res={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};await auth.loginAsync(secret,res);return res.cookie;};const cookies={root:await login('synthetic-root')},superAdmin=await auth.context({headers:{cookie:cookies.root}}),rows={},keys={};
 for(const label of ['a','b']){rows[label]=await departments.createDepartment(superAdmin,{name:'合成类型部门'+label});keys[label]=await departments.issueManagementKey(superAdmin,rows[label].id,{label:'合成管理Key'});cookies[label]=await login(keys[label].secret);}
 const body=async req=>{let text='';for await(const chunk of req)text+=chunk;return text?JSON.parse(text):{};},boundary=createPostgresDepartmentHttp({auth,departments,issuer,installed:true,readBody:body}),runtime=await mkdtemp(join(tmpdir(),'harbor-dept-account-')),admin=createPostgresAccountAdmin(store,{runtime,encryptionKey:randomBytes(32),router:createPostgresRouter(store),checkApi:async()=>[{slug:'gpt-6.1-sol'}],checkSubscription:async()=>true,readQuota:async()=>({rateLimits:{planType:'synthetic-plan',primary:{usedPercent:25,windowDurationMins:10080,resetsAt:(Date.now()+86400000)/1000}}}),...overrides});t.after(()=>rm(runtime,{recursive:true,force:true}));
 const server=http.createServer((req,res)=>{(async()=>{const scope=await boundary.context(req),path=new URL(req.url,'http://localhost'),result=await admin.handle(path.pathname,req.method,await body(req),{scope,revalidateScope:original=>auth.revalidateScope(req,original)});res.writeHead(result?.status??404,{'content-type':'application/json'});res.end(JSON.stringify(result?.value??{}));})().catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await admin.close();});
 const call=async(label,path='/api/admin/accounts',method='GET',payload,department)=>{const response=await fetch('http://127.0.0.1:'+server.address().port+path,{method,headers:{cookie:cookies[label],...(department?{'x-harbor-department':department}:{}),'content-type':'application/json'},...(payload?{body:JSON.stringify(payload)}:{})});return {status:response.status,value:await response.json()};};
 return {store,departments,rows,keys,superAdmin,call,admin,auth,cookies,login};
}

const wait=async predicate=>{const end=Date.now()+5000;while(!await predicate()){if(Date.now()>end)throw Error('condition timeout');await new Promise(resolve=>setTimeout(resolve,10));}};
const create=async(f,label,extra={})=>{const result=await f.call(label,undefined,'POST',{name:'合成账号',...extra});assert.equal(result.status,201,JSON.stringify(result));return result.value.id;};
const definition=()=>({name:'合成部门套餐',platform:'codex',auth_modes:['subscription'],protocols:['responses'],models:['*'],comparison:{group:'synthetic-common',unit:'common-unit',rule_id:'synthetic-rule',rule_version:1,units:{base:1}},official:{ids:['synthetic-plan'],granularity:'exact'},source:{kind:'administrator',reference:'合成配置依据',confirmed:true},effective_at:Date.now()-1,windows:[{id:'weekly',minutes:10080,capacity:100,unit:'base'}]});
async function preview(f,label,ids=[],template_id='synthetic-'+label,expected_version=0){const result=await f.call(label,'/api/admin/account-packages/preview','POST',{template_id,expected_version,definition:definition(),account_ids:ids});assert.equal(result.status,201,JSON.stringify(result));return result.value;}
async function apply(f,label,id){const result=await f.call(label,'/api/admin/account-packages/previews/'+id+'/apply','POST',{});assert.equal(result.status,200,JSON.stringify(result));return result.value;}

test('部门空套餐目录不阻断Codex仅月费和API创建，仍拒绝API附订阅套餐',integration,async t=>{
 const f=await fixture(t);assert.deepEqual((await f.call('a','/api/admin/account-packages')).value.items,[]);
 const a=await create(f,'a',{subscription_monthly_usd:20}),b=await create(f,'b',{subscription_monthly_usd:200}),api=await create(f,'a',{auth_mode:'openai_api',api_key:'sk-'+'x'.repeat(40)});
 for(const [label,accountId] of [['a',a],['b',b]]){const result=await f.call(label,'/api/admin/accounts/'+accountId+'/package');assert.equal(result.status,200);assert.equal(result.value.selection.mode,'unknown');assert.equal((await f.store.get('SELECT department_id FROM harbor.account_package_selections WHERE account_id=$1',accountId)).department_id,f.rows[label].id);}
 assert.equal((await f.call('a','/api/admin/accounts','POST',{name:'合成API',auth_mode:'openai_api',api_key:'sk-'+'y'.repeat(40),package:{template_id:null}})).status,400);
 assert.equal((await f.call('a','/api/admin/accounts/'+api+'/package','PATCH',{expected_revision:0,template_id:null})).status,400);
 assert.equal((await f.call('a','/api/admin/accounts/'+b+'/package')).status,404);
 assert.equal((await f.call('root','/api/admin/account-packages/policy','PATCH',{enabled:true,expected_version:0})).status,403);
});
test('套餐目录、选择、覆盖预览和模板版本只作用于本部门，原actor固定',integration,async t=>{
 const f=await fixture(t),a=await create(f,'a'),b=await create(f,'b'),pa=await preview(f,'a',[a]),pb=await preview(f,'b',[b]);
 assert.equal((await f.call('b','/api/admin/account-packages/previews/'+pa.id)).status,404);assert.equal((await f.call('b','/api/admin/account-packages/previews/'+pa.id+'/apply','POST',{})).status,404);
 await apply(f,'a',pa.id);await apply(f,'b',pb.id);
 for(const label of ['a','b']){const catalog=await f.call(label,'/api/admin/account-packages');assert.equal(catalog.status,200);assert.deepEqual(catalog.value.items.map(x=>x.id),['synthetic-'+label]);assert.equal(catalog.value.items[0].accounts,1);}
 assert.equal((await f.call('a','/api/admin/accounts/'+a+'/package','PATCH',{expected_revision:2,template_id:'synthetic-b',template_version:1})).status,404);
 const p=await preview(f,'a',[a],'synthetic-a',1),replacement=await f.departments.issueManagementKey(f.superAdmin,f.rows.a.id,{label:'合成替代管理员'}),oldCookie=f.cookies.a;f.cookies.a=await f.login(replacement.secret);
 assert.equal((await f.call('a','/api/admin/account-packages/previews/'+p.id+'/apply','POST',{})).status,403);f.cookies.a=oldCookie;await apply(f,'a',p.id);
 const scopes=await f.store.all("SELECT o.department_id,o.actor_kind FROM harbor.audit a JOIN harbor.audit_scopes o ON o.audit_id=a.id WHERE a.action LIKE 'account.package.%'");assert.ok(scopes.length>=4);assert.ok(scopes.every(x=>[f.rows.a.id,f.rows.b.id].includes(x.department_id)&&x.actor_kind==='department_admin'));
 await assert.rejects(f.store.run('UPDATE harbor.account_package_selections SET template_id=$1,template_version=1 WHERE account_id=$2','synthetic-b',a),{code:'23503'});
 await assert.rejects(f.store.run('UPDATE harbor.account_package_previews SET department_id=$1 WHERE id=$2',f.rows.b.id,p.id),{code:'23514'});
 await assert.rejects(f.store.run("UPDATE harbor.account_package_previews SET actor_id='spoofed' WHERE id=$1",p.id),{code:'23514'});
});
test('跨部门混入、损坏固定目标、缺失或伪造scope均不产生套餐副作用',integration,async t=>{
 const f=await fixture(t),a=await create(f,'a'),b=await create(f,'b'),payload={template_id:'mixed',expected_version:0,definition:definition(),account_ids:[a,b]};
 assert.equal((await f.call('a','/api/admin/account-packages/preview','POST',payload)).status,409);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.account_package_previews')).n,0);
 const scope=await f.departments.authenticateManagementKey(f.keys.a.secret);await assert.rejects(f.admin.packages.preview({...payload,account_ids:[a]}),{status:403});await assert.rejects(f.admin.packages.preview({...payload,account_ids:[a]},{scope:JSON.parse(JSON.stringify(scope))}),{status:403});
 const p=await preview(f,'a',[a]),row=await f.store.get('SELECT targets FROM harbor.account_package_previews WHERE id=$1',p.id);row.targets[0].id=b;await f.store.run('UPDATE harbor.account_package_previews SET targets=$1::jsonb WHERE id=$2',JSON.stringify(row.targets),p.id);
 assert.equal((await f.call('a','/api/admin/account-packages/previews/'+p.id+'/apply','POST',{})).status,404);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.account_package_templates')).n,0);assert.equal((await f.store.get('SELECT mode FROM harbor.account_package_selections WHERE account_id=$1',a)).mode,'unknown');
});
test('套餐策略按部门保存并同步本部门权重；撤销actor和停用部门使旧预览不可应用',integration,async t=>{
 const f=await fixture(t),a=await create(f,'a'),b=await create(f,'b'),p=await preview(f,'a',[a]);
 const result=await f.call('a','/api/admin/account-packages/policy','PATCH',{enabled:true,expected_version:0});assert.equal(result.status,200,JSON.stringify(result));assert.equal((await f.call('a','/api/admin/account-packages')).value.policy.enabled,true);assert.equal((await f.call('b','/api/admin/account-packages')).value.policy.enabled,false);
 assert.equal((await f.call('a','/api/admin/account-packages/coverage')).status,200);const settings=await f.store.all("SELECT department_id,value FROM harbor.department_settings WHERE name='account_package_policy'");assert.equal(settings.find(x=>x.department_id===f.rows.a.id).value.enabled,true);assert.equal(settings.find(x=>x.department_id===f.rows.b.id).value.enabled,false);
 const scope=await f.departments.authenticateManagementKey(f.keys.a.secret);await f.departments.revokeManagementKey(f.superAdmin,f.rows.a.id,f.keys.a.key.id,{expected_version:0});await assert.rejects(f.admin.packages.apply(p.id,{},{scope}),{status:403});
 const pb=await preview(f,'b',[b]),scopeB=await f.departments.authenticateManagementKey(f.keys.b.secret);await f.store.run('UPDATE harbor.departments SET enabled=false WHERE id=$1',f.rows.b.id);await assert.rejects(f.admin.packages.apply(pb.id,{},{scope:scopeB}),{status:403});assert.equal((await f.store.get('SELECT mode FROM harbor.account_package_selections WHERE account_id=$1',b)).mode,'unknown');
});

test('部门迁移保留原套餐版本、选择和策略，旧预览不能冒充新管理身份',integration,async t=>{
 const root=new PostgresStore(database()),name='harbor_package_migration_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});await store.migrate();await store.initializePricing();
 const accountId=randomUUID();await store.run("INSERT INTO harbor.accounts(id,name,home,kind,auth_mode,state) VALUES($1,'合成历史订阅','synthetic','codex','subscription','ready')",accountId);const before=createPostgresAccountPackages(store),p=await before.preview({template_id:'historical',expected_version:0,definition:definition(),account_ids:[accountId]});await before.apply(p.id,{});await before.savePolicy({enabled:true,expected_version:0});const selection=await store.get('SELECT * FROM harbor.account_package_selections WHERE account_id=$1',accountId),old=await before.preview({template_id:'historical',expected_version:1,definition:definition(),account_ids:[accountId]});
 await migrateDepartmentIsolation(store,{apply:true});const issuer=createDepartmentScopeIssuer(),scope=issuer.forDepartment(issuer.superAdmin({credentialId:'synthetic-migration',credentialVersion:0}),'department-default'),backend=createPostgresAccountPackages(store);
 const saved=await store.get('SELECT * FROM harbor.account_package_selections WHERE account_id=$1',accountId),{department_id,...rest}=saved;assert.deepEqual(rest,selection);assert.equal(department_id,'department-default');assert.equal((await backend.policy(store,{scope})).enabled,true);assert.equal((await backend.catalog({scope})).items[0].latest_version,1);assert.equal((await backend.detail(accountId,{scope})).selection.template_id,'historical');await assert.rejects(backend.apply(old.id,{},{scope}),{code:'package_preview_owner'});
 await assert.rejects(store.run('UPDATE harbor.account_package_versions SET department_id=$1 WHERE template_id=$2','spoofed','historical'),{code:'23514'});
});

test('首次后台额度刷新和官方套餐变化保留原系统身份完成本部门权重审计',integration,async t=>{
 let plan='synthetic-first-plan';const f=await fixture(t,{readQuota:async()=>({rateLimits:{planType:plan,primary:{usedPercent:25,windowDurationMins:10080,resetsAt:(Date.now()+86400000)/1000}}})}),a=await create(f,'a'),b=await create(f,'b');await f.store.run("UPDATE harbor.accounts SET state='ready' WHERE id=ANY($1::text[])",[a,b]);await f.store.run('UPDATE harbor.departments SET enabled=false WHERE id=$1',f.rows.b.id);
 // No manual quota refresh may prime the saved official identity first.
 const initial=(await f.store.get('SELECT recognition_version FROM harbor.account_package_selections WHERE account_id=$1',a)).recognition_version;
 let result=await f.admin.refreshQuotas({force:true});assert.equal(result.refreshed,1,JSON.stringify(result));assert.equal(result.failed,0);let selection=await f.store.get('SELECT observed_plan,recognition_version FROM harbor.account_package_selections WHERE account_id=$1',a);assert.equal(selection.observed_plan,plan);assert.equal(selection.recognition_version,initial+1);
 plan='synthetic-changed-plan';result=await f.admin.refreshQuotas({force:true});assert.equal(result.refreshed,1,JSON.stringify(result));assert.equal(result.failed,0);selection=await f.store.get('SELECT observed_plan,recognition_version FROM harbor.account_package_selections WHERE account_id=$1',a);assert.equal(selection.observed_plan,plan);assert.equal(selection.recognition_version,initial+2);assert.equal(JSON.parse((await f.store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',a)).quota_json).plan,plan);assert.equal((await f.store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',b)).quota_json,null);
 const audit=await f.store.all("SELECT o.department_id,o.actor_kind,o.actor_id FROM harbor.audit a JOIN harbor.audit_scopes o ON o.audit_id=a.id WHERE a.action='balancing.weights.auto' AND o.actor_kind='system'");assert.ok(audit.length>=2);assert.ok(audit.every(row=>row.department_id===f.rows.a.id&&row.actor_id==='system:account.quota.refresh'));
});
