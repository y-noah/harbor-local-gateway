import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID,randomBytes} from 'node:crypto';
import {hash} from '../store.mjs';
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
const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需独立真实PG':false,timeout:60000};
const database=name=>{if(!url)return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT),user:process.env.HARBOR_TEST_PG_USER,database:name||'postgres'}};const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:8};};
async function fixture(t,overrides={}){
 const root=new PostgresStore(database()),name='harbor_dept_account_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')};let auth;const departments=createPostgresDepartments(store,{issuer,authorizeSuperAdmin:credential=>auth.authorizeSuperAdmin(credential)});auth=createAdminAuth(config,{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}});
 const login=async secret=>{const res={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};await auth.loginAsync(secret,res);return res.cookie;};const cookies={root:await login('synthetic-root')},superAdmin=await auth.context({headers:{cookie:cookies.root}}),rows={},keys={};
 for(const label of ['a','b']){rows[label]=await departments.createDepartment(superAdmin,{name:'合成类型部门'+label});keys[label]=await departments.issueManagementKey(superAdmin,rows[label].id,{label:'合成管理Key'});cookies[label]=await login(keys[label].secret);}
 const body=async req=>{let text='';for await(const chunk of req)text+=chunk;return text?JSON.parse(text):{};},boundary=createPostgresDepartmentHttp({auth,departments,issuer,installed:true,readBody:body}),runtime=await mkdtemp(join(tmpdir(),'harbor-dept-account-')),admin=createPostgresAccountAdmin(store,{runtime,encryptionKey:randomBytes(32),router:createPostgresRouter(store),checkApi:async()=>[{slug:'gpt-6.1-sol'}],checkSubscription:async()=>true,readQuota:async()=>({rateLimits:{planType:'synthetic-plan',primary:{usedPercent:25,windowDurationMins:10080,resetsAt:(Date.now()+86400000)/1000}}}),...overrides});t.after(()=>rm(runtime,{recursive:true,force:true}));
 const server=http.createServer((req,res)=>{(async()=>{const scope=await boundary.context(req),path=new URL(req.url,'http://localhost'),result=await admin.handle(path.pathname,req.method,await body(req),{scope,revalidateScope:original=>auth.revalidateScope(req,original)});res.writeHead(result?.status??404,{'content-type':'application/json'});res.end(JSON.stringify(result?.value??{}));})().catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await admin.close();});
 const call=async(label,path='/api/admin/accounts',method='GET',payload,department)=>{const response=await fetch('http://127.0.0.1:'+server.address().port+path,{method,headers:{cookie:cookies[label],...(department?{'x-harbor-department':department}:{}),'content-type':'application/json'},...(payload?{body:JSON.stringify(payload)}:{})});return {status:response.status,value:await response.json()};};
 return {store,departments,rows,keys,superAdmin,call,admin,auth,cookies,login,origin:"http://127.0.0.1:"+server.address().port};
}

const create=async(f,scope='a',payload={})=>{const result=await f.call(scope,undefined,'POST',{name:'合成范围账号',auth_mode:'openai_api',api_key:'sk-synthetic-'+randomUUID(),...payload});assert.equal(result.status,201,JSON.stringify(result));return result.value.id;};
const json=value=>typeof value==='string'?JSON.parse(value):value;
const path=id=>'/api/admin/accounts/'+id;
test('HTTP专用API权重独立保存、普通池不变、配置读取与非权重更新保持范围',integration,async t=>{
 const f=await fixture(t),shared=await create(f,'a',{api_weight:100}),explicit=await create(f,'a',{api_usage_scope:'explicit_api',api_weight:100});
 const before=(await f.call('a',path(explicit))).value;assert.equal(before.api_weight,100);assert.equal(before.api_usage_scope,'explicit_api');assert.ok(before.api_scope_version>0);assert.equal((await f.store.get('SELECT api_weight FROM harbor.accounts WHERE id=$1',explicit)).api_weight,0);assert.equal((await f.call('a',path(shared))).value.api_weight,100);
 for(const payload of [{enabled:false},{enabled:true},{api_key:'sk-synthetic-rotated-'+randomUUID()}]){assert.equal((await f.call('a',path(explicit),'PATCH',payload)).status,200);const current=(await f.call('a',path(explicit))).value;assert.equal(current.api_weight,100);assert.equal(current.api_usage_scope,'explicit_api');assert.equal(current.api_scope_version,before.api_scope_version);assert.equal((await f.store.get('SELECT api_weight FROM harbor.accounts WHERE id=$1',explicit)).api_weight,0);}
 for(const api_usage_scope of ['shared','explicit_api']){const count=(await f.store.get('SELECT count(*)::int n FROM harbor.accounts')).n;const result=await f.call('a',undefined,'POST',{name:'超出预算',auth_mode:'openai_api',api_key:'sk-synthetic-'+randomUUID(),api_usage_scope,api_weight:1});assert.equal(result.status,400);assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.accounts')).n,count);}
});
test('HTTP范围版本、订阅拒绝、预算冲突原子回滚及删除清理',integration,async t=>{
 const f=await fixture(t),shared=await create(f,'a',{api_weight:100}),explicit=await create(f,'a',{api_usage_scope:'explicit_api',api_weight:60}),before=(await f.call('a',path(explicit))).value;
 for(const patch of [{api_usage_scope:'other'},{expected_api_scope_version:-1},{expected_api_scope_version:1.5},{api_weight:101}])assert.equal((await f.call('a',path(explicit),'PATCH',patch)).status,400,JSON.stringify(patch));
 assert.equal((await f.call('a',path(explicit),'PATCH',{api_usage_scope:'explicit_api',api_weight:50,expected_api_scope_version:before.api_scope_version-1})).status,409);
 const raw=await f.store.get('SELECT api_weight,enabled,route_epoch FROM harbor.accounts WHERE id=$1',explicit),settings=await f.store.all("SELECT value,version FROM harbor.department_settings WHERE department_id=$1 AND name='api_account_scope'",f.rows.a.id);
 assert.equal((await f.call('a',path(explicit),'PATCH',{api_usage_scope:'shared',api_weight:60,enabled:false,expected_api_scope_version:before.api_scope_version})).status,400);assert.deepEqual(await f.store.get('SELECT api_weight,enabled,route_epoch FROM harbor.accounts WHERE id=$1',explicit),raw);assert.deepEqual(await f.store.all("SELECT value,version FROM harbor.department_settings WHERE department_id=$1 AND name='api_account_scope'",f.rows.a.id),settings);assert.equal((await f.call('a',path(shared))).value.api_weight,100);
 const subscription=await f.call('a',undefined,'POST',{name:'合成订阅'});assert.equal(subscription.status,201);assert.equal((await f.call('a',path(subscription.value.id),'PATCH',{api_usage_scope:'explicit_api',expected_api_scope_version:before.api_scope_version})).status,400);assert.equal((await f.call('a',undefined,'POST',{name:'非法订阅',api_usage_scope:'explicit_api'})).status,400);
 assert.equal((await f.call('a',path(explicit),'DELETE')).status,200);const saved=json((await f.store.get("SELECT value FROM harbor.department_settings WHERE department_id=$1 AND name='api_account_scope'",f.rows.a.id)).value);assert.equal(Object.hasOwn(saved.accounts,explicit),false);assert.ok(saved.version>before.api_scope_version);assert.equal((await f.call('a',path(explicit))).status,404);
});
test('HTTP专用范围按团队隔离，其他团队及员工Key不能管理',integration,async t=>{
 const f=await fixture(t),a=await create(f,'a',{api_usage_scope:'explicit_api',api_weight:100}),b=await create(f,'b',{api_usage_scope:'explicit_api',api_weight:100});
 for(const method of ['GET','PATCH','DELETE'])assert.equal((await f.call('b',path(a),method,method==='PATCH'?{api_weight:0}:undefined)).status,404);
 assert.equal((await f.call('b',path(a),'GET',undefined,f.rows.a.id)).status,403);assert.equal((await f.call('a',path(a))).value.api_weight,100);assert.equal((await f.call('b',path(b))).value.api_weight,100);
 const member=randomUUID(),key=randomUUID(),secret='hg-synthetic-'+randomUUID();await f.store.run("INSERT INTO harbor.members(id,name,created,department_id) VALUES($1,'合成员工',1,$2)",member,f.rows.a.id);await f.store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created,department_id) VALUES($1,$2,$3,'synthetic',1,$4)",key,member,hash(secret),f.rows.a.id);
 const response=await fetch(f.origin+path(a),{method:'PATCH',headers:{authorization:'Bearer '+secret,'content-type':'application/json','x-harbor-department':f.rows.a.id},body:JSON.stringify({api_usage_scope:'shared',api_weight:0})});assert.equal(response.status,401);assert.equal((await f.call('a',path(a))).value.api_weight,100);
});

test('HTTP显式null范围及权重不是省略字段',integration,async t=>{const f=await fixture(t),id=await create(f,'a',{api_usage_scope:'explicit_api',api_weight:100});for(const payload of [{api_usage_scope:null},{api_weight:null}])await t.test(JSON.stringify(payload),async()=>{assert.equal((await f.call('a',path(id),'PATCH',payload)).status,400);});});
