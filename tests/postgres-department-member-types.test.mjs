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
test('真实管理HTTP类型目录与并发新建按部门独立，跨部门管理和全局写拒绝',integration,async t=>{
 const f=await fixture(t),a=await f.call('a'),b=await f.call('b');assert.equal(a.status,200);assert.ok(a.value.types.length>0);assert.ok(a.value.types.every(row=>row.department_id===f.rows.a.id));assert.ok(b.value.types.every(row=>row.department_id===f.rows.b.id));
 const [first,second,other]=await Promise.all([f.call('a',undefined,'POST',{name:'合成自定义一'}),f.call('a',undefined,'POST',{name:'合成自定义二'}),f.call('b',undefined,'POST',{name:'合成自定义一'})]);assert.ok([first,second,other].every(result=>result.status===201));assert.notEqual(first.value.code,second.value.code);assert.ok([first.value.code,second.value.code].includes(other.value.code));
 assert.equal((await f.call('a','/api/admin/member-types/'+other.value.id,'PATCH',{name:'越界',expected_version:0})).status,404);assert.equal((await f.call('a','/api/admin/member-types/'+other.value.id+'/templates')).status,404);
 assert.equal((await f.call('root',undefined,'POST',{name:'未选择部门'})).status,403);assert.equal((await f.call('root',undefined,'POST',{name:'合成超管新建'},f.rows.a.id)).status,201);
 const global=await f.call('root');assert.ok(global.value.types.some(row=>row.department_id===f.rows.a.id));assert.ok(global.value.types.some(row=>row.department_id===f.rows.b.id));
 await f.departments.revokeManagementKey(f.superAdmin,f.rows.a.id,f.keys.a.key.id,{expected_version:0});assert.equal((await f.call('a')).status,401);assert.equal((await f.call('b')).status,200);
});
test('真实HTTP模板按部门写入与恢复，历史统一20/50/100，模型目录不借其他部门账号',integration,async t=>{
 const f=await fixture(t),a=(await f.call('a')).value.types[0],b=(await f.call('b')).value.types[0],defaults={allowed_models:null,usage_limits:{enabled:false,rules:[]}},baseline=(a.default_version??0),baselineB=(b.default_version??0);
 await f.store.run("INSERT INTO harbor.accounts(id,name,home,kind,auth_mode,platform_models,department_id) VALUES($1,'合成模型账号','synthetic','qwen','platform',$2,$3)",randomUUID(),JSON.stringify(['synthetic-only']),f.rows.b.id);
 assert.equal((await f.call('a','/api/admin/member-types/'+a.id,'PATCH',{expected_version:0,defaults:{...defaults,allowed_models:['qwen/synthetic-only']}})).status,400);assert.equal((await f.call('b','/api/admin/member-types/'+b.id,'PATCH',{expected_version:0,defaults:{...defaults,allowed_models:['qwen/synthetic-only']}})).status,200);
 for(let i=0;i<22;i++)assert.equal((await f.call('a','/api/admin/member-types/'+a.id,'PATCH',{expected_version:i,defaults})).status,200);
 const page=await f.call('a','/api/admin/member-types/'+a.id+'/templates');assert.equal(page.status,200);assert.equal(page.value.items.length,20);assert.equal(page.value.has_more,true);const second=await f.call('a','/api/admin/member-types/'+a.id+'/templates?after='+page.value.after+'&limit=20');assert.equal(second.value.items.length,baseline+2);assert.equal(second.value.has_more,false);
 for(const limit of [50,100])assert.equal((await f.call('a','/api/admin/member-types/'+a.id+'/templates?limit='+limit)).value.items.length,baseline+22);for(const limit of [1,25,0,101])assert.equal((await f.call('a','/api/admin/member-types/'+a.id+'/templates?limit='+limit)).status,400);
 assert.equal((await f.call('a','/api/admin/member-types/'+a.id,'PATCH',{expected_version:22,restore_version:1})).status,200);assert.equal((await f.call('b','/api/admin/member-types/'+a.id,'PATCH',{expected_version:23,restore_version:1})).status,404);
 const saved=await f.store.all('SELECT department_id FROM harbor.member_type_templates WHERE type_id=$1',a.id);assert.equal(saved.length,baseline+23);assert.ok(saved.every(row=>row.department_id===f.rows.a.id));assert.equal((await f.store.get('SELECT default_version FROM harbor.member_types WHERE id=$1',b.id)).default_version,baselineB+1);
});
