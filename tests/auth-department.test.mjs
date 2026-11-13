import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminAuth,passwordHash} from '../auth.mjs';
import {createDepartmentScopeIssuer,issueDepartmentManagementKey} from '../department-access.mjs';

const response=()=>({headers:{},setHeader(name,value){this.headers[name]=value;}});
const request=res=>({headers:{cookie:res.headers['Set-Cookie'].split(';')[0]}});
function fixture(){
 const issuer=createDepartmentScopeIssuer(),key=issueDepartmentManagementKey();let enabled=true,version=1,departmentId='department-a',reads=0;
 const scope=()=>issuer.departmentAdmin({departmentId,credentialId:'admin-key-a',credentialVersion:version});
 const provider={authenticate:async secret=>secret===key.secret&&enabled?scope():null,validate:async old=>{reads++;return enabled&&old.credential_version===version&&old.department_id===departmentId?scope():null;}};
 const config={adminPasswordHash:passwordHash('synthetic-super-admin')},auth=createAdminAuth(config,{departmentKeys:provider,scopeIssuer:issuer});
 return {auth,config,provider,issuer,key,disable:()=>{enabled=false;},rotate:()=>{version++;},move:()=>{departmentId='department-b';},reads:()=>reads};
}

test('同一登录入口接受部门密钥但旧布尔管理入口不能将其视为超管',async()=>{
 const f=fixture(),res=response();await f.auth.loginAsync(f.key.secret,res);const req=request(res);
 assert.equal(f.auth.authenticated(req),false);assert.equal(f.auth.sessionReference(req),null);
 const before=f.reads(),one=await f.auth.context(req),two=await f.auth.context(req);
 assert.equal(one.role,'department_admin');assert.equal(one.department_id,'department-a');assert.equal(two.credential_id,'admin-key-a');assert.equal(f.reads(),before+2);
 assert.equal(JSON.stringify(one).includes(f.key.secret),false);
});
test('现有超管同步和异步登录继续可用，不依赖部门密钥查询',async()=>{
 const f=fixture();f.provider.authenticate=async()=>{throw Error('must not query department keys');};
 const sync=response();f.auth.login('synthetic-super-admin',sync);assert.equal(f.auth.authenticated(request(sync)),true);
 const async=response();await f.auth.loginAsync('synthetic-super-admin',async);const req=request(async);assert.equal(f.auth.authenticated(req),true);
 const scope=await f.auth.context(req);assert.equal(scope.role,'super_admin');assert.equal(scope.view,'global');assert.equal(scope.department_id,null);assert.equal(f.reads(),0);
});
test('管理密钥撤销、轮换和部门绑定变化使已有Cookie后续操作失效',async()=>{
 for(const change of ['disable','rotate','move']){
  const f=fixture(),res=response();await f.auth.loginAsync(f.key.secret,res);const req=request(res);assert.ok(await f.auth.context(req));
  f[change]();assert.equal(await f.auth.context(req),null);assert.equal(await f.auth.context(req),null);assert.equal(f.auth.authenticated(req),false);
 }
});
test('部门登录成功不累计超管错误次数，错误凭据仍受原有锁定保护',async()=>{
 const f=fixture();for(let i=0;i<6;i++){const res=response();await f.auth.loginAsync(f.key.secret,res);assert.ok(await f.auth.context(request(res)));}
 for(let i=0;i<5;i++)await assert.rejects(f.auth.loginAsync('wrong',response()),{code:'password_incorrect'});
 await assert.rejects(f.auth.loginAsync(f.key.secret,response()),{code:'login_limited'});
});
test('部门解析器不能用普通对象或超管身份升级部门登录',async()=>{
 for(const forged of [{role:'department_admin',department_id:'department-a'},'super']){
  const f=fixture();f.provider.authenticate=async()=>forged==='super'?f.issuer.superAdmin({credentialId:'forged-super',credentialVersion:0}):forged;
  const res=response();await assert.rejects(f.auth.loginAsync(f.key.secret,res),error=>['department_scope_required','department_forbidden'].includes(error.code));assert.equal(res.headers['Set-Cookie'],undefined);
 }
});
test('退出或过期后迟到的数据库校验不能恢复已结束的部门会话',async()=>{
 const f=fixture(),res=response();await f.auth.loginAsync(f.key.secret,res);const req=request(res);let release,entered;
 const ready=new Promise(resolve=>{entered=resolve;});f.provider.validate=old=>new Promise(resolve=>{release=()=>resolve(old);entered();});
 const pending=f.auth.context(req);await ready;f.auth.logout(req,response());release();assert.equal(await pending,null);assert.equal(await f.auth.context(req),null);
 let at=0;const auth=createAdminAuth(f.config,{clock:()=>at,departmentKeys:{authenticate:async()=>f.issuer.departmentAdmin({departmentId:'department-a',credentialId:'a',credentialVersion:1}),validate:async value=>value}}),expired=response();
 await auth.loginAsync(f.key.secret,expired);at=12*3600000;assert.equal(await auth.context(request(expired)),null);
});
test('超管后台身份跨重启稳定，密码变化拒绝原任务身份及原Cookie',async()=>{
 const f=fixture(),res=response();await f.auth.loginAsync('synthetic-super-admin',res);const req=request(res),scope=await f.auth.context(req);
 const credential={credentialId:scope.credential_id,credentialVersion:scope.credential_version};
 const restarted=createAdminAuth({...f.config});assert.equal(restarted.authorizeSuperAdmin(credential)?.role,'super_admin');
 assert.equal(restarted.authorizeSuperAdmin({...credential,credentialVersion:1}),null);
 assert.equal(restarted.authorizeSuperAdmin({credentialId:'legacy-admin-password',credentialVersion:0}),null);
 assert.equal(JSON.stringify(scope).includes(f.config.adminPasswordHash.digest),false);
 f.config.adminPasswordHash=passwordHash('synthetic-new-password');
 assert.equal(f.auth.authorizeSuperAdmin(credential),null);assert.equal(f.auth.authenticated(req),false);assert.equal(await f.auth.context(req),null);
});
test('分块读取重验当前Cookie，保留超管部门范围并拒绝换身份和退出',async()=>{
 const f=fixture(),res=response();await f.auth.loginAsync('synthetic-super-admin',res);const req=request(res),global=await f.auth.context(req),limited=f.issuer.forDepartment(global,'department-a');
 assert.equal((await f.auth.revalidateScope(req,limited)).department_id,'department-a');
 assert.equal((await f.auth.revalidateScope(req,global)).view,'global');
 await assert.rejects(f.auth.revalidateScope(req,{...limited}),{code:'department_scope_required'});
 const department=response();await f.auth.loginAsync(f.key.secret,department);const departmentReq=request(department),departmentScope=await f.auth.context(departmentReq);
 assert.equal(await f.auth.revalidateScope(departmentReq,global),null);assert.equal(await f.auth.revalidateScope(req,departmentScope),null);
 f.rotate();assert.equal(await f.auth.revalidateScope(departmentReq,departmentScope),null);
 f.auth.logout(req,response());assert.equal(await f.auth.revalidateScope(req,limited),null);
});
