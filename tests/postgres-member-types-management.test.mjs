import test from 'node:test';
import assert from 'node:assert/strict';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;

test('真实HTTP使用类型新增、改名与归档保留员工配置，认证、并发和历史均受保护',{skip:!binDirectory?'requires private PG18':false,timeout:60000},async()=>{
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory});let app;
 try{
  const setup=new PostgresStore({connection:lab.connection});try{await setup.migrate();}finally{await setup.close();}
  app=await createPostgresGateway({env:{},database:{connection:lab.connection},dataDir:join(lab.directory,'types-app'),runtimeDir:join(lab.directory,'types-runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-types-admin')},recording:{enabled:false}});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port;
  assert.equal((await fetch(origin+'/api/admin/member-types',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({name:'拒绝未认证'})})).status,401);
  const login=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password:'synthetic-types-admin'})});assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
  const admin=async(path,method='GET',body)=>{const response=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});return {status:response.status,value:await response.json()};};
  for(const body of [null,[],{}, {name:' '},{name:'x'.repeat(61)},{name:'合成',description:'x'.repeat(401)},{name:'合成',code:'A'},{name:'合成',defaults:{allowed_models:null}}])assert.equal((await admin('member-types','POST',body)).status,400);
  assert.equal((await admin('member-types')).value.types.length,6);
  const created=await Promise.all(Array.from({length:4},(_,i)=>admin('member-types','POST',{name:'合成新增类型'+i,description:''})));assert.ok(created.every(row=>row.status===201));assert.deepEqual(created.map(row=>row.value.code).sort(),['G','H','I','J']);assert.equal(new Set(created.map(row=>row.value.id)).size,4);
  for(const {value} of created){assert.equal(value.version,0);assert.equal(value.configured,false);assert.equal(value.default_version,null);assert.equal(value.defaults,null);}
  const type=created[0].value;
  const employeeBody={name:'合成保留配置员工',quota:null,expires:null,type_id:type.id,expected_type_version:0};assert.equal((await admin('members','POST',employeeBody)).status,409);
  const defaults={allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'2.00'}]}};
  assert.equal((await admin('member-types/'+type.id,'PATCH',{defaults,expected_version:0})).status,200);
  const employee=await admin('members','POST',{...employeeBody,expected_type_version:1,expected_template_version:1});assert.equal(employee.status,201);
  const before=await app.store.get('SELECT * FROM harbor.members WHERE id=$1',employee.value.memberId),keys=await app.store.all('SELECT * FROM harbor.keys WHERE member_id=$1 ORDER BY id',employee.value.memberId),templates=await app.store.all('SELECT * FROM harbor.member_type_templates WHERE type_id=$1 ORDER BY version',type.id);
  const staff=await fetch(origin+'/api/admin/member-types',{method:'POST',headers:{authorization:'Bearer '+employee.value.secret,'content-type':'application/json'},body:JSON.stringify({name:'拒绝员工管理'})});assert.equal(staff.status,401);
  const renamed=await admin('member-types/'+type.id,'PATCH',{name:'改名后的合成类型',description:'',expected_version:1});assert.equal(renamed.status,200);assert.equal(renamed.value.code,type.code);assert.equal(renamed.value.default_version,1);
  const archived=await admin('member-types/'+type.id,'PATCH',{enabled:false,expected_version:2});assert.equal(archived.status,200);assert.equal(archived.value.enabled,0);
  assert.deepEqual(await app.store.get('SELECT * FROM harbor.members WHERE id=$1',employee.value.memberId),before);assert.deepEqual(await app.store.all('SELECT * FROM harbor.keys WHERE member_id=$1 ORDER BY id',employee.value.memberId),keys);assert.deepEqual(await app.store.all('SELECT * FROM harbor.member_type_templates WHERE type_id=$1 ORDER BY version',type.id),templates);
  assert.equal((await fetch(origin+'/api/me',{headers:{authorization:'Bearer '+employee.value.secret}})).status,200);
  assert.equal((await admin('members','POST',{...employeeBody,expected_type_version:3,expected_template_version:1})).status,400);
  const restored=await admin('member-types/'+type.id,'PATCH',{enabled:true,expected_version:3});assert.equal(restored.status,200);assert.equal(restored.value.code,type.code);assert.equal(restored.value.default_version,1);
  const saved=await Promise.all(['并发名称一','并发名称二'].map(name=>admin('member-types/'+type.id,'PATCH',{name,expected_version:4})));assert.deepEqual(saved.map(row=>row.status).sort(),[200,409]);
  const history=await admin('history?kind=audit&action=member.type.create');assert.equal(history.status,200);assert.equal(history.value.rows.length,4);assert.ok(history.value.rows.every(row=>row.detail.code&&row.detail.type));assert.ok(!JSON.stringify(history).includes(employee.value.secret));
  const configure=await admin('history?kind=audit&action=member.type.configure');assert.equal(configure.status,200);assert.ok(configure.value.rows.some(row=>row.detail.enabled_after===false));assert.ok(configure.value.rows.some(row=>row.detail.enabled_after===true));assert.ok(configure.value.rows.some(row=>row.detail.name_changed===true));
 }finally{await app?.close();await lab.close();}
});
