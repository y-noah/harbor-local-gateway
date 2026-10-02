import {randomBytes,scryptSync,timingSafeEqual} from 'node:crypto';
import {AppError} from './store.mjs';

export function passwordHash(password,salt=randomBytes(16).toString('hex')){
  return {salt,digest:scryptSync(password,salt,32).toString('hex')};
}
export function createAdminAuth(config,{clock=Date.now}={}){
  const sessions=new Map();let failures=0,blockedUntil=0;
  const cookie=(token,seconds)=>`harbor_session=${token}; HttpOnly; SameSite=Strict; Path=/; Max-Age=${seconds}`;
  function token(req){return String(req.headers.cookie||'').split(';').map(x=>x.trim()).find(x=>x.startsWith('harbor_session='))?.slice(15)||'';}
  function authenticated(req){
    const key=token(req),expires=sessions.get(key);if(!expires)return false;
    if(expires<=clock()){sessions.delete(key);return false;}return true;
  }
  function login(password,res){
    if(blockedUntil>clock())throw new AppError(429,'密码尝试次数过多，请 1 分钟后重试','login_limited');
    if(typeof password!=='string'||password.length>256)throw new AppError(400,'请输入有效密码');
    const expected=config.adminPasswordHash;
    const actual=expected?scryptSync(password,expected.salt,32):Buffer.alloc(32);
    if(!expected||!timingSafeEqual(actual,Buffer.from(expected.digest,'hex'))){
      failures++;if(failures>=5){blockedUntil=clock()+60000;failures=0;}
      throw new AppError(401,'密码不正确','password_incorrect');
    }
    failures=0;blockedUntil=0;
    for(const [key,expires] of sessions)if(expires<=clock())sessions.delete(key);
    if(sessions.size>=100)sessions.delete(sessions.keys().next().value);
    const key=randomBytes(32).toString('base64url');sessions.set(key,clock()+12*3600000);
    res.setHeader('Set-Cookie',cookie(key,12*3600));
  }
  function logout(req,res){sessions.delete(token(req));res.setHeader('Set-Cookie',cookie('',0));}
  return {authenticated,login,logout};
}
