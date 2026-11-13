import {AppError} from './store.mjs';
import {beginAdminRequestTiming,finishAdminRequestTiming} from './admin-request-timing.mjs';
import {readJsonBody} from './request-body.mjs';
import {createPostgresPricingRuntime} from './postgres-pricing-runtime.mjs';
import {requireDepartmentScope} from './department-access.mjs';

// The enclosing admin adapter retains the shared Origin/CSRF protection.
export function createPostgresPricingAdmin(store,{adminAuthenticated,adminContext,readBody=readJsonBody,...options}={}){
 if(typeof adminAuthenticated!=='function'||typeof readBody!=='function')throw new AppError(503,'费率管理鉴权或请求读取未配置','pricing_configuration');
 const runtime=createPostgresPricingRuntime(store,options);
 async function handle(req,res,{signal}={}){
  beginAdminRequestTiming(req,res);
  const path=new URL(req.url,'http://localhost').pathname;
  if(path!=='/api/admin/pricing'&&path!=='/api/admin/pricing/refresh')return false;
  const scope=adminContext?await adminContext(req):undefined;
  if(scope){const trusted=requireDepartmentScope(scope,{allowGlobal:true,roles:['super_admin']});if(trusted.view!=='global')throw new AppError(403,'费率仅允许在超管全局视图管理','department_forbidden');}
  else if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
  let result;
  if(path==='/api/admin/pricing'&&req.method==='GET')result=await runtime.status({signal});
  else if(path==='/api/admin/pricing'&&req.method==='PATCH')result=await runtime.configure(await readBody(req,{signal}),{scope});
  else if(path==='/api/admin/pricing/refresh'&&req.method==='POST')result=await runtime.refresh({force:true});
  else throw new AppError(405,'费率管理不支持此请求方法','method_not_allowed');
  const encoded=JSON.stringify(result);finishAdminRequestTiming(res,200,{authenticated:true});res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(encoded);return true;
 }
 return {handle,tick:runtime.tick,close:runtime.close,runtime};
}
