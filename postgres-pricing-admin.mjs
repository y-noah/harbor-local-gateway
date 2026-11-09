import {AppError} from './store.mjs';
import {readJsonBody} from './request-body.mjs';
import {createPostgresPricingRuntime} from './postgres-pricing-runtime.mjs';

// The enclosing admin adapter retains the shared Origin/CSRF protection.
export function createPostgresPricingAdmin(store,{adminAuthenticated,readBody=readJsonBody,...options}={}){
 if(typeof adminAuthenticated!=='function'||typeof readBody!=='function')throw new AppError(503,'费率管理鉴权或请求读取未配置','pricing_configuration');
 const runtime=createPostgresPricingRuntime(store,options);
 async function handle(req,res,{signal}={}){
  const path=new URL(req.url,'http://localhost').pathname;
  if(path!=='/api/admin/pricing'&&path!=='/api/admin/pricing/refresh')return false;
  if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
  let result;
  if(path==='/api/admin/pricing'&&req.method==='GET')result=await runtime.status();
  else if(path==='/api/admin/pricing'&&req.method==='PATCH')result=await runtime.configure(await readBody(req,{signal}));
  else if(path==='/api/admin/pricing/refresh'&&req.method==='POST')result=await runtime.refresh({force:true});
  else throw new AppError(405,'费率管理不支持此请求方法','method_not_allowed');
  res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(JSON.stringify(result));return true;
 }
 return {handle,tick:runtime.tick,close:runtime.close,runtime};
}
