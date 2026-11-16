import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {migrateDepartmentIsolation,createPostgresDepartments} from '../postgres-departments.mjs';
import {migrateDepartmentKeySecrets,verifyDepartmentKeySecretsAvailability} from '../postgres-department-key-secrets.mjs';
import {departmentKeySecretsArguments,runDepartmentKeySecretsCommand} from '../postgres-department-key-secrets-cli.mjs';
import {createDepartmentScopeIssuer} from '../department-access.mjs';
import {createUsageRollupRuntime} from './usage-rollup-fixture.mjs';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createPostgresDepartmentHttp} from '../postgres-department-http.mjs';
import {createRequestBodyBudget} from '../request-body-budget.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,integration={skip:!url?'需独立真实PG测试入口':false,timeout:60000};
function options(database){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.href,allowPlaintextLocalNetwork:true,poolSize:3};}
async function fixture(t,{contract='operator'}={}){
 const name='dkeys_'+randomUUID().replaceAll('-',''),admin=new PostgresStore(options());await admin.query('CREATE DATABASE '+name);
 const store=new PostgresStore(options(name));let role;
 t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);if(role)await admin.query('DROP ROLE "'+role+'"');await admin.close();});
 await store.migrate({recordingContextIndex:true});await migrateDepartmentIsolation(store,{apply:true});
 role=await createUsageRollupRuntime(store,{activationContract:contract});
 const issuer=createDepartmentScopeIssuer(),root=issuer.superAdmin({credentialId:'synthetic-super',credentialVersion:0}),encryptionKey=randomBytes(32);
 const service=createPostgresDepartments(store,{issuer,encryptionKey}),department=await service.createDepartment(root,{name:'合成部门'});
 return {store,role,issuer,root,encryptionKey,service,department,name};
}

test('部门Key扩展显式安装、最小权限与本机保障角色校验',integration,async t=>{
 const {store,role,name}=await fixture(t,{contract:'local-assurance'});
 assert.equal(await verifyDepartmentKeySecretsAvailability(store),false);
 assert.equal(await migrateDepartmentKeySecrets(store),false);
 await assert.rejects(migrateDepartmentKeySecrets(store,{apply:true}),{code:'department_key_storage_conflict'});
 await assert.rejects(migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:'capacity_owner'}),{code:'department_key_runtime_invalid'});
 assert.equal(await migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:role}),true);
 assert.equal(await migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:role}),false);
 assert.equal(await verifyDepartmentKeySecretsAvailability(store,{required:true}),true);
 const privileges=await store.get("SELECT has_table_privilege($1,'harbor.department_admin_key_secrets','SELECT') readable,has_table_privilege($1,'harbor.department_admin_key_secrets','INSERT') insertable,has_table_privilege($1,'harbor.department_admin_key_secrets','DELETE') removable,has_table_privilege($1,'harbor.department_admin_key_secrets','UPDATE') writable",role);
 assert.deepEqual(privileges,{readable:true,insertable:true,removable:true,writable:false});
 await assert.rejects(store.transaction(async()=>{await store.query('GRANT SELECT ON harbor.department_admin_key_secrets TO PUBLIC');await verifyDepartmentKeySecretsAvailability(store);}),{code:'department_key_storage_conflict'});
 await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.gateway_settings SET value='{}' WHERE name='schema.department-key-secrets'");await verifyDepartmentKeySecretsAvailability(store);}),{code:'department_key_storage_conflict'});
 assert.deepEqual(await runDepartmentKeySecretsCommand(['verify','--require-installed'],options(name)),{action:'verify',installed:true});
});

test('新部门Key加密回读，旧Key保留、列表与审计不泄漏，撤销原子移除副本',integration,async t=>{
 const {store,role,issuer,root,encryptionKey,service,department}=await fixture(t);
 const legacy=await service.issueManagementKey(root,department.id,{label:'旧Key'});
 assert.deepEqual(await service.revealManagementKey(root,department.id,legacy.key.id),{available:false,reason:'storage_not_enabled'});
 await migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:role});
 await assert.rejects(service.issueManagementKey(root,department.id,{label:'旧进程'}),{code:'department_key_storage_restart_required'});
 const updated=createPostgresDepartments(store,{issuer,encryptionKey}),issued=await updated.issueManagementKey(root,department.id,{label:'新Key'});
 assert.deepEqual(await updated.revealManagementKey(root,department.id,issued.key.id),{available:true,secret:issued.secret});
 assert.deepEqual(await updated.revealManagementKey(root,department.id,legacy.key.id),{available:false,reason:'not_stored'});
 assert.ok(await updated.authenticateManagementKey(legacy.secret));
 const row=await store.get('SELECT * FROM harbor.department_admin_key_secrets WHERE key_id=$1',issued.key.id);
 assert.equal(JSON.stringify(row).includes(issued.secret),false);
 assert.equal(JSON.stringify(await updated.listManagementKeys(root,department.id)).includes(issued.secret),false);
 assert.equal(JSON.stringify(await store.all('SELECT * FROM harbor.audit')).includes(issued.secret),false);
 const scope=await updated.authenticateManagementKey(issued.secret);
 await assert.rejects(updated.revealManagementKey(scope,department.id,issued.key.id),{code:'department_forbidden'});
 await assert.rejects(updated.revealManagementKey({...root},department.id,issued.key.id),{code:'department_scope_required'});
 const other=await updated.createDepartment(root,{name:'其他部门'}),selected=issuer.forDepartment(root,other.id);
 await assert.rejects(updated.revealManagementKey(selected,department.id,issued.key.id),{code:'department_forbidden'});
 await assert.rejects(updated.revealManagementKey(root,other.id,issued.key.id),{code:'department_management_key_not_found'});
 await assert.rejects(updated.revealManagementKey(root,department.id,issued.key.id,{scope:'global'}),{code:'department_input_invalid'});
 await updated.revokeManagementKey(root,department.id,issued.key.id,{expected_version:0});
 assert.equal(await store.get('SELECT * FROM harbor.department_admin_key_secrets WHERE key_id=$1',issued.key.id),undefined);
 await assert.rejects(updated.revealManagementKey(root,department.id,issued.key.id),{code:'department_management_key_not_found'});
 assert.equal(await updated.authenticateManagementKey(issued.secret),null);
 await store.transaction(async()=>{
  await store.query('SET LOCAL ROLE "'+role+'"');
  const restricted=createPostgresDepartments(store,{issuer,encryptionKey}),next=await restricted.issueManagementKey(root,department.id,{label:'受限运行角色'});
  assert.equal((await restricted.revealManagementKey(root,department.id,next.key.id)).secret,next.secret);
  await restricted.revokeManagementKey(root,department.id,next.key.id,{expected_version:0});
 });
});

test('主密钥缺失或不匹配、篡改与交换密文均拒绝，失败签发不残留Key',integration,async t=>{
 const {store,role,issuer,root,encryptionKey,department}=await fixture(t);await migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:role});
 const service=createPostgresDepartments(store,{issuer,encryptionKey}),a=await service.issueManagementKey(root,department.id,{label:'A'}),b=await service.issueManagementKey(root,department.id,{label:'B'});
 for(const key of [undefined,randomBytes(32)]){
  const invalid=createPostgresDepartments(store,{issuer,encryptionKey:key});await assert.rejects(invalid.revealManagementKey(root,department.id,a.key.id),{code:'department_key_decryption_unavailable'});
 }
 const absent=createPostgresDepartments(store,{issuer});await assert.rejects(absent.issueManagementKey(root,department.id,{label:'不可签发'}),{code:'department_key_decryption_unavailable'});
 assert.equal((await service.listManagementKeys(root,department.id)).rows.length,2);
 for(const update of [()=>store.run("UPDATE harbor.department_admin_key_secrets SET secret_cipher=$1 WHERE key_id=$2",'A'.repeat(80),a.key.id),()=>store.run('UPDATE harbor.department_admin_key_secrets SET secret_cipher=(SELECT secret_cipher FROM harbor.department_admin_key_secrets WHERE key_id=$1),secret_digest=(SELECT secret_digest FROM harbor.department_admin_key_secrets WHERE key_id=$1) WHERE key_id=$2',b.key.id,a.key.id)]){
  await assert.rejects(store.transaction(async()=>{await update();await service.revealManagementKey(root,department.id,a.key.id);}),{code:'department_key_decryption_unavailable'});
 }
 assert.equal((await service.revealManagementKey(root,department.id,a.key.id)).secret,a.secret);
});

test('真实HTTP回读仅限超管和选中部门，拒绝GET/参数，撤销后不可访问',integration,async t=>{
 const {store,role,issuer,encryptionKey}=await fixture(t);await migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:role});
 const departments=createPostgresDepartments(store,{issuer,encryptionKey}),auth=createAdminAuth({adminPasswordHash:passwordHash('synthetic-super')},{scopeIssuer:issuer,departmentKeys:{authenticate:departments.authenticateManagementKey,validate:departments.validateManagementSession}}),bodies=createRequestBodyBudget(),access=createPostgresDepartmentHttp({auth,departments,issuer,installed:true,readBody:bodies.read});
 const server=http.createServer(async(req,res)=>{try{
  if(req.url==='/login'&&req.method==='POST'){const body=await bodies.read(req,{owner:'login'});await auth.loginAsync(body.password,res);res.end('{}');return;}
  if(await access.handle(req,res))return;res.writeHead(404);res.end('{}');
 }catch(error){res.writeHead(error.status??500,{'Content-Type':'application/json'});res.end(JSON.stringify({code:error.code}));}finally{bodies.release(req);}});
 await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));t.after(async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));});
 const origin='http://127.0.0.1:'+server.address().port;
 const call=async(path,{cookie,method='GET',body,headers={}}={})=>{const response=await fetch(origin+path,{method,headers:{...(cookie?{Cookie:cookie}:{}),...(body!==undefined?{'Content-Type':'application/json'}:{}),...headers},...(body!==undefined?{body:JSON.stringify(body)}:{})});return {response,value:await response.json()};};
 const login=await call('/login',{method:'POST',body:{password:'synthetic-super'}}),cookie=login.response.headers.get('set-cookie').split(';')[0];
 const created=await call('/api/admin/departments',{cookie,method:'POST',body:{name:'HTTP部门'}}),department=created.value.id;
 const issued=await call('/api/admin/departments/'+department+'/management-keys',{cookie,method:'POST',body:{label:'HTTPKey'}}),key=issued.value,path='/api/admin/departments/'+department+'/management-keys/'+key.key.id+'/reveal';
 const reveal=await call(path,{cookie,method:'POST',body:{}});assert.equal(reveal.response.status,200);assert.equal(reveal.response.headers.get('cache-control'),'no-store');assert.deepEqual(reveal.value,{available:true,secret:key.secret});
 assert.equal((await call(path,{cookie})).response.status,405);
 assert.equal((await call(path+'?secret=yes',{cookie,method:'POST',body:{}})).response.status,400);
 assert.equal((await call(path,{method:'POST',body:{}})).response.status,401);
 const other=await call('/api/admin/departments',{cookie,method:'POST',body:{name:'其他HTTP部门'}});
 assert.equal((await call(path,{cookie,method:'POST',body:{},headers:{'X-Harbor-Department':other.value.id}})).response.status,403);
 const scoped=await call('/login',{method:'POST',body:{password:key.secret}}),departmentCookie=scoped.response.headers.get('set-cookie').split(';')[0];
 assert.equal((await call(path,{cookie:departmentCookie,method:'POST',body:{}})).response.status,403);
 await call(path.replace('/reveal','/revoke'),{cookie,method:'POST',body:{expected_version:0}});
 assert.equal((await call(path,{cookie,method:'POST',body:{}})).response.status,404);
});

test('部门密钥CLI默认只读，拒绝重复或未知参数',()=>{
 assert.deepEqual(departmentKeySecretsArguments([]),{action:'verify'});
 assert.deepEqual(departmentKeySecretsArguments(['install','--runtime-role=runtime']),{action:'install',runtimeRole:'runtime'});
 for(const args of [['install','--require-installed'],['verify','--runtime-role=x'],['install','--runtime-role=x','--runtime-role=y']])assert.throws(()=>departmentKeySecretsArguments(args),{code:'database_configuration'});
});

test('真实网关仅有部门密文时也禁止生成替代主密钥，保留原密钥可回读',integration,async t=>{
 const {store,role,issuer,root,encryptionKey,department,name}=await fixture(t);await migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:role});
 const service=createPostgresDepartments(store,{issuer,encryptionKey}),issued=await service.issueManagementKey(root,department.id,{label:'恢复验证'}),directory=await mkdtemp(join(tmpdir(),'harbor-department-key-'));
 let app;try{
  const parameters={env:{},database:options(name),dataDir:join(directory,'data'),runtimeDir:join(directory,'runtime'),config:{port:43127,codexBinary:process.execPath,adminPasswordHash:passwordHash('synthetic-super')},recording:{enabled:false}};
  await assert.rejects(createPostgresGateway(parameters),{code:'encryption_material_missing'});
  await mkdir(join(directory,'data'),{recursive:true,mode:0o700});await writeFile(join(directory,'data','key-encryption.key'),encryptionKey,{mode:0o600});
  app=await createPostgresGateway(parameters);await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));
  const origin='http://127.0.0.1:'+app.server.address().port,login=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:'synthetic-super'})});assert.equal(login.status,200);await login.json();
  const response=await fetch(origin+'/api/admin/departments/'+department.id+'/management-keys/'+issued.key.id+'/reveal',{method:'POST',headers:{Cookie:login.headers.get('set-cookie').split(';')[0],'Content-Type':'application/json'},body:'{}'});
  assert.equal(response.status,200);assert.deepEqual(await response.json(),{available:true,secret:issued.secret});
 }finally{await app?.close();await rm(directory,{recursive:true,force:true});}
});


test('发布预检分页验证部门管理密文及登录摘要，拒绝错主密钥或篡改',integration,async t=>{
 const {verifyStoredCredentials}=await import('../ci/postgres-preflight.mjs');
 const {store,role,issuer,root,encryptionKey,department}=await fixture(t);
 await verifyStoredCredentials(store,encryptionKey);
 await migrateDepartmentKeySecrets(store,{apply:true,runtimeRole:role});
 const service=createPostgresDepartments(store,{issuer,encryptionKey}),issued=[];
 for(let n=0;n<17;n++)issued.push(await service.issueManagementKey(root,department.id,{label:'合成分页'+n}));
 const before=await store.all('SELECT * FROM harbor.department_admin_key_secrets ORDER BY key_id');
 await verifyStoredCredentials(store,encryptionKey);
 await assert.rejects(verifyStoredCredentials(store,randomBytes(32)),/Stored credential material mismatch/);
 assert.deepEqual(await store.all('SELECT * FROM harbor.department_admin_key_secrets ORDER BY key_id'),before);
 const last=before.at(-1);
 await assert.rejects(store.transaction(async()=>{
  await store.run("UPDATE harbor.department_admin_keys SET digest=$1 WHERE id=$2",'a'.repeat(64),last.key_id);
  await verifyStoredCredentials(store,encryptionKey);
 }),/Stored credential material mismatch/);
 await assert.rejects(store.transaction(async()=>{
  await store.run("UPDATE harbor.department_admin_key_secrets SET secret_cipher=$1 WHERE key_id=$2",'x'.repeat(80),last.key_id);
  await verifyStoredCredentials(store,encryptionKey);
 }),/Stored credential material mismatch/);
 await verifyStoredCredentials(store,encryptionKey);
});
