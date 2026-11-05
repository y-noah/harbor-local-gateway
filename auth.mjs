import {randomBytes,scrypt,scryptSync,timingSafeEqual} from 'node:crypto';
import {AppError,hash} from './store.mjs';

export function passwordHash(password,salt=randomBytes(16).toString('hex')){
  return {salt,digest:scryptSync(password,salt,32).toString('hex')};
}
export function createAdminAuth(config,{clock=Date.now}={}){
  const maxSessions=100,sessions=new Map();let failures=0,blockedUntil=0,pendingLogins=0,loginTail=Promise.resolve();
  const cookie=(token,seconds)=>`harbor_session=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${seconds}`;
  function token(req){return String(req.headers.cookie||'').split(';').map(x=>x.trim()).find(x=>x.startsWith('harbor_session='))?.slice(15)||'';}
  function authenticated(req){
    const key=token(req),expires=sessions.get(key);if(!expires)return false;
    if(expires<=clock()){sessions.delete(key);return false;}return true;
  }
  function checkLogin(password){
    if(blockedUntil>clock())throw new AppError(429,'密码尝试次数过多，请 1 分钟后重试','login_limited');
    if(typeof password!=='string'||password.length>256)throw new AppError(400,'请输入有效密码');
  }
  function completeLogin(expected,actual,res){
    if(!expected||!timingSafeEqual(actual,Buffer.from(expected.digest,'hex'))){
      failures++;if(failures>=5){blockedUntil=clock()+60000;failures=0;}
      throw new AppError(401,'密码不正确','password_incorrect');
    }
    failures=0;blockedUntil=0;
    for(const [key,expires] of sessions)if(expires<=clock())sessions.delete(key);
    if(sessions.size>=maxSessions)sessions.delete(sessions.keys().next().value);
    const key=randomBytes(32).toString('base64url');sessions.set(key,clock()+12*3600000);
    res.setHeader('Set-Cookie',cookie(key,12*3600));
  }
  function login(password,res){
    checkLogin(password);const expected=config.adminPasswordHash;
    completeLogin(expected,expected?scryptSync(password,expected.salt,32):Buffer.alloc(32),res);
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
      completeLogin(expected,actual,res);
    });
    loginTail=operation.then(()=>{},()=>{});return operation.finally(()=>{pendingLogins--;});
  }
  function logout(req,res){sessions.delete(token(req));res.setHeader('Set-Cookie',cookie('',0));}
  // Audit identity comes from this authenticated session, never a request field.
  const sessionReference=req=>authenticated(req)?Object.freeze({session_ref:hash(token(req))}):null;
  return {authenticated,sessionReference,login,loginAsync,logout};
}
