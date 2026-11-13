import {AppError} from './store.mjs';
import {beginAdminRequestTiming,finishAdminRequestTiming} from './admin-request-timing.mjs';
import {createPostgresTypeStatistics,typeStatisticsQuery} from './postgres-type-statistics.mjs';
import {requireDepartmentScope} from './department-access.mjs';

export function createPostgresTypeStatisticsAdmin(store,{adminAuthenticated,adminContext,...options}={}){
 if(typeof adminAuthenticated!=='function')throw new AppError(503,'类型统计管理鉴权未配置','type_statistics_configuration');
 if(adminContext!==undefined&&typeof adminContext!=='function')throw new AppError(503,'类型统计部门鉴权未配置','type_statistics_configuration');
 const queries=createPostgresTypeStatistics(store,options);
 async function handle(req,res,{signal}={}){
  beginAdminRequestTiming(req,res);
  const url=new URL(req.url,'http://localhost');if(url.pathname!=='/api/admin/type-statistics')return false;
  let scope;
  if(adminContext){scope=await adminContext(req);if(scope!==undefined)requireDepartmentScope(scope,{allowGlobal:true});}
  else if(!await adminAuthenticated(req))throw new AppError(401,'请先输入管理密码登录','admin_required');
  if(req.method!=='GET')throw new AppError(405,'类型统计只支持查询','method_not_allowed');
  const result=await queries.read(typeStatisticsQuery(url.searchParams),{signal,scope});
  // The statistics response is bounded by the reader's byte budget and contains
  // request-time identities and reference costs only, never keys or body text.
  const encoded=JSON.stringify(result);finishAdminRequestTiming(res,200,{authenticated:true});res.writeHead(200,{'Content-Type':'application/json; charset=utf-8','Cache-Control':'no-store'});res.end(encoded);return true;
 }
 return {handle,close:queries.close,status:queries.status};
}
