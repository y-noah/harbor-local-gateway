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
test('账号HTTP独立目录、月费版本、API管理及跨部门拒绝',integration,async t=>{
 const f=await fixture(t),a=await create(f,'a',{subscription_monthly_usd:20}),b=await create(f,'b',{subscription_monthly_usd:200}),api=await create(f,'a',{auth_mode:'openai_api',api_key:'sk-'+ 'x'.repeat(40)});
 const rows=await f.store.all('SELECT id,home,department_id FROM harbor.accounts WHERE id=ANY($1::text[])',[a,b,api]);assert.equal(new Set(rows.map(row=>row.home)).size,3);for(const row of rows)assert.ok(row.home.endsWith('/accounts/'+row.id));
 for(const suffix of ['check','quota','login','subscription'])assert.equal((await f.call('a','/api/admin/accounts/'+b+'/'+suffix,suffix==='subscription'?'GET':'POST',suffix==='subscription'?undefined:{})).status,404);
 assert.equal((await f.call('root',undefined,'POST',{name:'未选部门'})).status,403);
 const path='/api/admin/accounts/'+a+'/subscription';assert.deepEqual((await f.call('a',path)).value,{monthly_usd:20,version:1});assert.equal((await f.call('a',path,'PATCH',{monthly_usd:25,expected_version:1})).status,200);assert.equal((await f.call('a',path,'PATCH',{monthly_usd:30,expected_version:1})).status,409);
 assert.equal((await f.call('a','/api/admin/accounts/'+api+'/check','POST',{})).status,200);assert.equal((await f.store.get('SELECT state FROM harbor.accounts WHERE id=$1',api)).state,'ready');
 await f.store.run("UPDATE harbor.accounts SET state='ready' WHERE id=ANY($1::text[])",[a,b]);assert.equal((await f.call('a','/api/admin/accounts/'+a+'/quota','POST',{})).status,200);assert.equal((await f.store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',b)).quota_json,null);
});
test('API检查跨IO撤销管理Key后不写ready',integration,async t=>{
 let release,entered=false;const f=await fixture(t,{checkApi:async()=>{entered=true;await new Promise(resolve=>release=resolve);return [{slug:'gpt-6.1-sol'}];}}),a=await create(f,'a',{auth_mode:'openai_api',api_key:'sk-'+ 'x'.repeat(40)});
 const request=f.call('a','/api/admin/accounts/'+a+'/check','POST',{});await wait(()=>entered);await f.departments.revokeManagementKey(f.superAdmin,f.rows.a.id,f.keys.a.key.id,{expected_version:0});release();assert.ok([401,403].includes((await request).status));assert.equal((await f.store.get('SELECT state FROM harbor.accounts WHERE id=$1',a)).state,'offline');
});
test('官方额度跨IO退出Cookie后不写快照',integration,async t=>{
 let release,entered=false;const quota={rateLimits:{planType:'synthetic-plan',primary:{usedPercent:25,windowDurationMins:10080,resetsAt:(Date.now()+86400000)/1000}}};const f=await fixture(t,{readQuota:async()=>{entered=true;await new Promise(resolve=>release=resolve);return quota;}}),a=await create(f,'a');await f.store.run("UPDATE harbor.accounts SET state='ready' WHERE id=$1",a);
 const request=f.call('a','/api/admin/accounts/'+a+'/quota','POST',{});await wait(()=>entered);f.auth.logout({headers:{cookie:f.cookies.a}},{setHeader(){}});release();assert.ok([401,403].includes((await request).status));assert.equal((await f.store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',a)).quota_json,null);
});
test('自动额度扫描只处理启用部门，手动批量只处理当前部门',integration,async t=>{
 const f=await fixture(t),a=await create(f,'a'),b=await create(f,'b');await f.store.run("UPDATE harbor.accounts SET state='ready' WHERE id=ANY($1::text[])",[a,b]);
 const manual=await f.call('a','/api/admin/quotas/refresh','POST',{});assert.equal(manual.status,200);assert.equal(manual.value.refreshed,1);assert.equal((await f.store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',b)).quota_json,null);
 await f.store.run('UPDATE harbor.departments SET enabled=false WHERE id=$1',f.rows.b.id);const automatic=await f.admin.refreshQuotas({force:true});assert.equal(automatic.refreshed,1);assert.equal(automatic.failed,0);assert.equal((await f.store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',b)).quota_json,null);
});
function loginFlow(){let authorize,complete,cancelled=false;const challenge=new Promise(resolve=>authorize=resolve),completed=new Promise(resolve=>complete=resolve);return {challenge,completed,authorize,complete,cancel(){cancelled=true;complete({success:false});},get cancelled(){return cancelled;}};}
for(const loss of ['key','cookie','department'])test('官方登录跨IO身份失效不授ready并释放资源：'+loss,integration,async t=>{
 let flow,released=0;const f=await fixture(t,{startLogin:()=>flow=loginFlow(),reserveLogin:()=>({release(){released++;}})}),a=await create(f,'a');
 const pending=f.call('a','/api/admin/accounts/'+a+'/login','POST',{});await wait(()=>!!flow);
 if(loss==='key')await f.departments.revokeManagementKey(f.superAdmin,f.rows.a.id,f.keys.a.key.id,{expected_version:0});
 if(loss==='cookie')f.auth.logout({headers:{cookie:f.cookies.a}},{setHeader(){}});
 if(loss==='department')await f.store.run('UPDATE harbor.departments SET enabled=false WHERE id=$1',f.rows.a.id);
 flow.authorize({type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'});flow.complete({success:true});assert.ok([401,403].includes((await pending).status));await wait(()=>f.admin.status().login.pending===0);assert.equal((await f.store.get('SELECT state FROM harbor.accounts WHERE id=$1',a)).state,'offline');assert.equal(flow.cancelled,true);assert.ok(released>0);
});

for(const status of [403,404])test('上游拒绝保留API降级及额度错误：'+status,integration,async t=>{
 const fail=async()=>{throw new AppError(status,'synthetic upstream','upstream_'+(status===403?'forbidden':'not_found'));};const f=await fixture(t,{checkApi:fail,readQuota:fail}),a=await create(f,'a',{auth_mode:'openai_api',api_key:'sk-'+ 'x'.repeat(40)}),b=await create(f,'a');await f.store.run("UPDATE harbor.accounts SET state='ready' WHERE id=ANY($1::text[])",[a,b]);
 assert.equal((await f.call('a','/api/admin/accounts/'+a+'/check','POST',{})).status,status);assert.equal((await f.store.get('SELECT state FROM harbor.accounts WHERE id=$1',a)).state,'degraded');assert.equal((await f.call('a','/api/admin/accounts/'+b+'/quota','POST',{})).value.failed,true);assert.ok((await f.store.get('SELECT quota_error FROM harbor.accounts WHERE id=$1',b)).quota_error);
});
for(const action of ['check','quota'])for(const logout of ['a','second'])test('同管理Key独立Cookie跨IO不共享授权：'+action+'/'+logout,integration,async t=>{
 const releases=[];const upstream=async()=>{await new Promise(resolve=>releases.push(resolve));return action==='check'?[{slug:'gpt-6.1-sol'}]:{rateLimits:{planType:'synthetic-plan',primary:{usedPercent:25,windowDurationMins:10080,resetsAt:(Date.now()+86400000)/1000}}};};
 const f=await fixture(t,action==='check'?{checkApi:upstream}:{readQuota:upstream}),a=await create(f,'a',action==='check'?{auth_mode:'openai_api',api_key:'sk-'+ 'x'.repeat(40)}:{});await f.store.run("UPDATE harbor.accounts SET state='ready' WHERE id=$1",a);f.cookies.second=await f.login(f.keys.a.secret);
 const first=f.call('a','/api/admin/accounts/'+a+'/'+action,'POST',{});await wait(()=>releases.length===1);const second=f.call('second','/api/admin/accounts/'+a+'/'+action,'POST',{});await wait(()=>releases.length===2);f.auth.logout({headers:{cookie:f.cookies[logout]}},{setHeader(){}});for(const release of releases)release();const results=await Promise.all([first,second]);assert.ok([401,403].includes(results[logout==='a'?0:1].status));assert.equal(results[logout==='a'?1:0].status,200);
});
test('登录挑战后旧Cookie注销，新Cookie轮询立即清理旧登录',integration,async t=>{
 let flow;const f=await fixture(t,{startLogin:()=>flow=loginFlow()}),a=await create(f,'a');const started=f.call('a','/api/admin/accounts/'+a+'/login','POST',{});await wait(()=>!!flow);flow.authorize({type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'});assert.equal((await started).status,200);f.auth.logout({headers:{cookie:f.cookies.a}},{setHeader(){}});f.cookies.second=await f.login(f.keys.a.secret);
 assert.ok([401,403].includes((await f.call('second','/api/admin/accounts/'+a+'/login')).status));await wait(()=>f.admin.status().login.pending===0);assert.equal(flow.cancelled,true);assert.equal((await f.store.get('SELECT state FROM harbor.accounts WHERE id=$1',a)).state,'offline');
});
test('账号审计真实归属与停用部门启动登录收尾',integration,async t=>{
 const f=await fixture(t,{startLogin:()=>loginFlow()}),a=await create(f,'a');await f.store.run("UPDATE harbor.accounts SET state='login' WHERE id=$1",a);await f.store.run('UPDATE harbor.departments SET enabled=false WHERE id=$1',f.rows.a.id);await f.admin.recover();assert.equal((await f.store.get('SELECT state FROM harbor.accounts WHERE id=$1',a)).state,'offline');const rows=await f.store.all('SELECT s.*,a.action FROM harbor.audit_scopes s JOIN harbor.audit a ON a.id=s.audit_id WHERE s.subject_id=$1',a);assert.ok(rows.length>=2);assert.ok(rows.every(row=>row.department_id===f.rows.a.id));assert.ok(rows.some(row=>row.actor_kind==='system'&&row.action==='account.login.failed'));
});
