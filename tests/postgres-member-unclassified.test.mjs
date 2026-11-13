import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID,randomBytes} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createPostgresDepartments,migrateDepartmentIsolation} from '../postgres-departments.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createPostgresDepartmentHttp} from '../postgres-department-http.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立真实PG':false,timeout:60000};
const database=name=>{const value=new URL(url);if(name)value.pathname='/'+name;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:8};};
async function fixture(t){
 const root=new PostgresStore(database()),name='harbor_dept_type_'+randomUUID().replaceAll('-','');await root.query('CREATE DATABASE '+name);const store=new PostgresStore(database(name));t.after(async()=>{await store.close();await root.query('DROP DATABASE '+name);await root.close();});await store.migrate();await store.initializePricing();await migrateDepartmentIsolation(store,{apply:true});
 const issuer=createDepartmentScopeIssuer(),config={adminPasswordHash:passwordHash('synthetic-root')};let auth;const departments=createPostgresDepartments(store,{issuer,authorizeSuperAdmin:credential=>auth.authorizeSuperAdmin(credential)});auth=createAdminAuth(config,{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}});
 const login=async secret=>{const res={setHeader(name,value){if(name==='Set-Cookie')this.cookie=value.split(';')[0];}};await auth.loginAsync(secret,res);return res.cookie;};const cookies={root:await login('synthetic-root')},superAdmin=await auth.context({headers:{cookie:cookies.root}}),rows={},keys={};
 for(const label of ['a','b']){rows[label]=await departments.createDepartment(superAdmin,{name:'合成类型部门'+label});keys[label]=await departments.issueManagementKey(superAdmin,rows[label].id,{label:'合成管理Key'});cookies[label]=await login(keys[label].secret);}
 const body=async req=>{let text='';for await(const chunk of req)text+=chunk;return text?JSON.parse(text):{};},boundary=createPostgresDepartmentHttp({auth,departments,issuer,installed:true,readBody:body}),admin=createPostgresMemberAdmin(store,{encryptionKey:randomBytes(32),departmentActors:departments});
 const server=http.createServer((req,res)=>{(async()=>{const scope=await boundary.context(req),path=new URL(req.url,'http://localhost'),result=await admin.handle(path.pathname,req.method,await body(req),path.searchParams,{scope});res.writeHead(result?.status??404,{'content-type':'application/json'});res.end(JSON.stringify(result?.value??{}));})().catch(error=>{res.writeHead(error.status??500,{'content-type':'application/json'});res.end(JSON.stringify({code:error.code}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));await admin.close();});
 const call=async(label,path='/api/admin/member-types',method='GET',payload,department)=>{const response=await fetch('http://127.0.0.1:'+server.address().port+path,{method,headers:{cookie:cookies[label],...(department?{'x-harbor-department':department}:{}),'content-type':'application/json'},...(payload?{body:JSON.stringify(payload)}:{})});return {status:response.status,value:await response.json()};};
 return {store,departments,rows,keys,superAdmin,call};
}

test('未分类显式配置经真实HTTP创建并由后续Key继承',integration,async t=>{
 const f=await fixture(t),models=(await f.call('a')).value.models;assert.ok(models.length);const allowed=[models[0]],payload={name:'合成未分类',quota:null,expires:null,type_id:null,models_mode:'custom',limits_mode:'custom',allowed_models:allowed,usage_limits:{enabled:false,rules:[]}};
 const result=await f.call('a','/api/admin/members','POST',payload);assert.equal(result.status,201);const member=await f.store.get('SELECT usage_type_id,usage_type_version,default_models,default_models_set,defaults_version,configuration_source FROM harbor.members WHERE id=$1',result.value.memberId);
 assert.equal(member.usage_type_id,null);assert.equal(member.usage_type_version,0);assert.equal(member.default_models_set,1);assert.equal(member.defaults_version,2);assert.deepEqual(JSON.parse(member.default_models),allowed);assert.deepEqual(member.configuration_source,{models:{kind:'custom'},limits:{kind:'custom'}});
 const first=await f.store.get('SELECT allowed_models FROM harbor.keys WHERE member_id=$1',result.value.memberId);assert.deepEqual(JSON.parse(first.allowed_models),allowed);
 const extra=await f.call('a','/api/admin/keys','POST',{memberId:result.value.memberId,expires:null,expected_defaults_version:member.defaults_version});assert.equal(extra.status,201);const keys=await f.store.all('SELECT allowed_models FROM harbor.keys WHERE member_id=$1',result.value.memberId);assert.equal(keys.length,2);assert.ok(keys.every(row=>JSON.stringify(JSON.parse(row.allowed_models))===JSON.stringify(allowed)));
});
test('未分类不允许省略配置或借用模板版本，旧无type_id保持legacy',integration,async t=>{
 const f=await fixture(t),payload={name:'合成配置',quota:null,expires:null,type_id:null,models_mode:'custom',limits_mode:'custom',allowed_models:null,usage_limits:{enabled:false,rules:[]}};
 for(const field of ['models_mode','limits_mode','allowed_models','usage_limits']){const invalid={...payload};delete invalid[field];assert.equal((await f.call('a','/api/admin/members','POST',invalid)).status,400,field);}
 for(const patch of [{models_mode:'template'},{limits_mode:'template'},{expected_type_version:0},{expected_template_version:1},{allowed_models:['unknown-synthetic-model']}])assert.equal((await f.call('a','/api/admin/members','POST',{...payload,...patch})).status,400);
 assert.equal((await f.store.get('SELECT count(*)::int n FROM harbor.members')).n,0);
 const legacy=await f.call('a','/api/admin/members','POST',{name:'合成旧入口',quota:null,expires:null});assert.equal(legacy.status,201);const member=await f.store.get('SELECT usage_type_id,usage_type_version,default_models_set,configuration_source FROM harbor.members WHERE id=$1',legacy.value.memberId);assert.equal(member.usage_type_version,0);assert.equal(member.default_models_set,0);assert.deepEqual(member.configuration_source,{models:{kind:'legacy'},limits:{kind:'legacy'}});
});
test('模型目录仅返回ID及本部门platform模型，校验与下拉共用目录',integration,async t=>{
 const f=await fixture(t);for(const label of ['a','b'])await f.store.run("INSERT INTO harbor.accounts(id,name,home,kind,auth_mode,platform_models,department_id) VALUES($1,'合成目录账号','synthetic','qwen','platform',$2,$3)",randomUUID(),JSON.stringify(['synthetic-'+label]),f.rows[label].id);
 const a=await f.call('a'),b=await f.call('b');assert.ok(a.value.models.every(model=>typeof model==='string'));assert.ok(a.value.models.includes('qwen/synthetic-a'));assert.ok(!a.value.models.includes('qwen/synthetic-b'));assert.ok(b.value.models.includes('qwen/synthetic-b'));assert.ok(!b.value.models.includes('qwen/synthetic-a'));assert.deepEqual(Object.keys(a.value).sort(),['models','types']);
 const payload={name:'合成目录使用',quota:null,expires:null,type_id:null,models_mode:'custom',limits_mode:'custom',allowed_models:['qwen/synthetic-b'],usage_limits:{enabled:false,rules:[]}};assert.equal((await f.call('a','/api/admin/members','POST',payload)).status,400);assert.equal((await f.call('b','/api/admin/members','POST',payload)).status,201);
});
