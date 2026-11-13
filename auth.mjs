import {randomBytes,scrypt,scryptSync,timingSafeEqual} from 'node:crypto';
import {AppError,hash} from './store.mjs';
import {createDepartmentScopeIssuer,requireDepartmentScope} from './department-access.mjs';

export function passwordHash(password,salt=randomBytes(16).toString('hex')){
  return {salt,digest:scryptSync(password,salt,32).toString('hex')};
}
export function adminCookieName(config){
  const name=config.adminCookieName;if(name===undefined)return 'harbor_session';
  if(typeof name!=='string'||name.length<1||name.length>64||!/^[A-Za-z]/.test(name)||/[^A-Za-z0-9_-]/.test(name))throw new AppError(503,'管理Cookie名称配置无效','admin_cookie_configuration');
  return name;
}
export function createAdminAuth(config,{clock=Date.now,departmentKeys,scopeIssuer=createDepartmentScopeIssuer()}={}){
  if(departmentKeys!==undefined&&(!departmentKeys||typeof departmentKeys.authenticate!=='function'||typeof departmentKeys.validate!=='function')||typeof scopeIssuer?.superAdmin!=='function')throw new AppError(503,'部门登录配置无效','department_identity_configuration');
  const cookieName=adminCookieName(config);
  function superAdminCredential(){
    const expected=config.adminPasswordHash;
    if(!expected||typeof expected.salt!=='string'||typeof expected.digest!=='string')return null;
    return {credentialId:'admin-config:'+hash('harbor-admin-configuration\0'+JSON.stringify([expected.salt,expected.digest])),credentialVersion:0};
  }
  function authorizeSuperAdmin({credentialId,credentialVersion}={}){
    const current=superAdminCredential();
    return current&&current.credentialId===credentialId&&current.credentialVersion===credentialVersion?scopeIssuer.superAdmin(current):null;
  }
  const maxSessions=100,sessions=new Map();let failures=0,blockedUntil=0,pendingLogins=0,loginTail=Promise.resolve();
  const cookie=(token,seconds)=>`${cookieName}=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${seconds}`;
  function token(req){return String(req.headers.cookie||'').split(';').map(x=>x.trim()).find(x=>x.startsWith(cookieName+'='))?.slice(cookieName.length+1)||'';}
  function session(req){
    const key=token(req),value=sessions.get(key);if(!value)return null;
    if(value.expires<=clock()||value.identity.role==='super_admin'&&!authorizeSuperAdmin({credentialId:value.identity.credential_id,credentialVersion:value.identity.credential_version})){sessions.delete(key);return null;}return value;
  }
  // Legacy boolean-only handlers are super-admin-only until they consume and
  // enforce a trusted department context. A department cookie never grants them access.
  const authenticated=req=>session(req)?.identity.role==='super_admin';
  async function context(req){
    const value=session(req);if(!value)return null;
    if(value.identity.role==='super_admin')return value.identity;
    const current=await departmentKeys?.validate(value.identity);
    if(!current){if(session(req)===value)sessions.delete(token(req));return null;}
    const verified=requireDepartmentScope(current,{roles:['department_admin']});
    if(session(req)!==value)return null;
    if(['department_id','credential_id','credential_version'].some(field=>verified[field]!==value.identity[field])){sessions.delete(token(req));return null;}
    return verified;
  }
  async function revalidateScope(req,scope){
    const original=requireDepartmentScope(scope,{allowGlobal:true});
    const current=await context(req);if(!current)return null;
    if(['role','credential_id','credential_version'].some(field=>original[field]!==current[field]))return null;
    if(current.role==='department_admin')return original.department_id===current.department_id&&original.view===current.view?current:null;
    return original.view==='department'?scopeIssuer.forDepartment(current,original.department_id):current;
  }
  function checkLogin(password){
    if(blockedUntil>clock())throw new AppError(429,'密码尝试次数过多，请 1 分钟后重试','login_limited');
    if(typeof password!=='string'||password.length>256)throw new AppError(400,'请输入有效密码');
  }
  function completeLogin(expected,actual,res,identity){
    if(identity!==undefined)requireDepartmentScope(identity,{roles:['department_admin']});
    if(identity===undefined&&(!expected||!timingSafeEqual(actual,Buffer.from(expected.digest,'hex')))){
      failures++;if(failures>=5){blockedUntil=clock()+60000;failures=0;}
      throw new AppError(401,'密码不正确','password_incorrect');
    }
    failures=0;blockedUntil=0;
    for(const [key,value] of sessions)if(value.expires<=clock())sessions.delete(key);
    if(sessions.size>=maxSessions)sessions.delete(sessions.keys().next().value);
    const key=randomBytes(32).toString('base64url');sessions.set(key,{expires:clock()+12*3600000,identity:identity??scopeIssuer.superAdmin(superAdminCredential())});
    res.setHeader('Set-Cookie',cookie(key,12*3600));
    return sessions.get(key).identity;
  }
  function login(password,res){
    checkLogin(password);const expected=config.adminPasswordHash;
    return completeLogin(expected,expected?scryptSync(password,expected.salt,32):Buffer.alloc(32),res);
  }
  // Keep the synchronous internal API, but never run a network login's KDF on
  // the gateway event loop. One KDF runs at a time; the existing session budget
  // also bounds retained login work instead of flooding the libuv worker pool.
  function loginAsync(password,res){
    try{checkLogin(password);if(pendingLogins>=maxSessions)throw new AppError(503,'登录繁忙，请稍后重试','login_busy');}catch(error){return Promise.reject(error);}
    pendingLogins++;
    const operation=loginTail.then(async()=>{
      checkLogin(password);const current=config.adminPasswordHash,expected=current?{salt:current.salt,digest:current.digest}:null;
      let actual;try{actual=expected?await new Promise((resolve,reject)=>scrypt(password,expected.salt,32,(error,value)=>error?reject(error):resolve(value))):Buffer.alloc(32);}catch{throw new AppError(503,'登录验证暂不可用，请稍后重试','login_unavailable');}
      // Synchronous legacy callers or configuration changes can run while the
      // worker computes. Neither may let an old accepted result bypass a lock.
      checkLogin(password);if((config.adminPasswordHash?.salt??null)!==(expected?.salt??null)||(config.adminPasswordHash?.digest??null)!==(expected?.digest??null))throw new AppError(503,'登录配置已变化，请重试','login_configuration_changed');
      const superAdmin=expected&&actual.length===Buffer.from(expected.digest,'hex').length&&timingSafeEqual(actual,Buffer.from(expected.digest,'hex'));
      let identity;
      if(!superAdmin&&departmentKeys){
        const candidate=await departmentKeys.authenticate(password);
        if(candidate){
          const checked=requireDepartmentScope(candidate,{roles:['department_admin']});
          identity=await departmentKeys.validate(checked);
          if(identity){requireDepartmentScope(identity,{roles:['department_admin']});if(['department_id','credential_id','credential_version'].some(field=>identity[field]!==checked[field]))throw new AppError(503,'部门登录状态已变化，请重试','login_configuration_changed');}
        }
      }
      checkLogin(password);if((config.adminPasswordHash?.salt??null)!==(expected?.salt??null)||(config.adminPasswordHash?.digest??null)!==(expected?.digest??null))throw new AppError(503,'登录配置已变化，请重试','login_configuration_changed');
      return completeLogin(expected,actual,res,identity||undefined);
    });
    loginTail=operation.then(()=>{},()=>{});return operation.finally(()=>{pendingLogins--;});
  }
  function logout(req,res){sessions.delete(token(req));res.setHeader('Set-Cookie',cookie('',0));}
  // Audit identity comes from this authenticated session, never a request field.
  const sessionReference=req=>authenticated(req)?Object.freeze({session_ref:hash(token(req))}):null;
  const managementSessionReference=async req=>await context(req)?Object.freeze({session_ref:hash(token(req))}):null;
  return {authenticated,context,revalidateScope,authorizeSuperAdmin,sessionReference,managementSessionReference,login,loginAsync,logout};
}
