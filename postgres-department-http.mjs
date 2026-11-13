import {AppError} from './store.mjs';
import {requireDepartmentScope} from './department-access.mjs';

const denied=()=>{throw new AppError(403,'当前身份无权访问此部门','department_forbidden');};
const invalid=()=>{throw new AppError(400,'部门管理参数无效','department_query_invalid');};
function selection(req){
 const value=req.headers['x-harbor-department'];
 if(value===undefined)return null;
 if(typeof value!=='string'||!/^([a-zA-Z0-9][a-zA-Z0-9-]{0,99})$/.test(value))invalid();
 if(Array.isArray(req.rawHeaders)&&req.rawHeaders.filter((name,index)=>index%2===0&&name.toLowerCase()==='x-harbor-department').length!==1)invalid();
 return value;
}
function pagination(params){
 const allowed=['limit','after_created','after_id'];
 for(const name of params.keys())if(!allowed.includes(name)||params.getAll(name).length!==1)invalid();
 const limit=params.has('limit')&&/^\d+$/.test(params.get('limit'))?Number(params.get('limit')):params.has('limit')?NaN:20;
 if(![20,50,100].includes(limit))invalid();
 if(params.has('after_created')!==params.has('after_id'))invalid();
 if(!params.has('after_id'))return {limit};
 const created=/^\d+$/.test(params.get('after_created'))?Number(params.get('after_created')):NaN;
 if(!Number.isSafeInteger(created)||created<0||!/^[a-zA-Z0-9][a-zA-Z0-9-]{0,99}$/.test(params.get('after_id')))invalid();
 return {limit,after:{created,id:params.get('after_id')}};
}

// The header selects a view only after authentication. It never grants a role
// or changes a department administrator's ownership.
export function createPostgresDepartmentHttp({auth,departments,issuer,installed,readBody}={}){
 if(typeof installed!=='boolean'||!auth||typeof auth.context!=='function'||typeof auth.revalidateScope!=='function'||!departments||typeof departments.getDepartment!=='function'||typeof issuer?.forDepartment!=='function'||typeof readBody!=='function')throw new AppError(503,'部门HTTP入口未配置完整','department_http_configuration');
 const contexts=new WeakMap();
 async function resolveContext(req){
  const selected=selection(req),base=await auth.context(req);
  if(!base)throw new AppError(401,'请先输入管理密钥登录','admin_required');
  const scope=requireDepartmentScope(base,{allowGlobal:true});
  if(!installed){if(selected!==null||scope.role!=='super_admin')throw new AppError(503,'部门隔离尚未启用','department_unavailable');return undefined;}
  if(scope.role==='department_admin'){
   if(selected!==null&&selected!==scope.department_id)denied();
   return scope;
  }
  if(selected===null||selected==='global')return scope;
  await departments.getDepartment(scope,selected);
  return issuer.forDepartment(scope,selected);
 }
 function context(req){
  let pending=contexts.get(req);if(!pending){pending=resolveContext(req);contexts.set(req,pending);}return pending;
 }
 async function revalidate(req,scope){
  const current=await auth.revalidateScope(req,scope);if(!current)throw new AppError(401,'管理会话已失效，请重新登录','admin_required');
  if(current.view==='department')await departments.getDepartment(current,current.department_id);
  return current;
 }
 async function handle(req,res,{signal}={}){
  const url=new URL(req.url,'http://localhost'),path=url.pathname;
  if(!/^\/api\/admin\/departments(?:\/|$)/.test(path))return false;
  const scope=await context(req);
  if(!installed)throw new AppError(503,'部门隔离尚未启用','department_unavailable');
  requireDepartmentScope(scope,{allowGlobal:true,roles:['super_admin']});
  const match=path.match(/^\/api\/admin\/departments(?:\/([a-zA-Z0-9][a-zA-Z0-9-]{0,99})(?:\/management-keys(?:\/([a-zA-Z0-9][a-zA-Z0-9-]{0,99})\/revoke)?)?)?$/);
  if(!match)throw new AppError(404,'部门管理入口不存在','not_found');
  const departmentId=match[1],keyId=match[2],keys=path.includes('/management-keys');
  let value,status=200;
  if(req.method==='GET'&&!keyId){
   if(keys)value=await departments.listManagementKeys(scope,departmentId,pagination(url.searchParams));
   else if(departmentId){if([...url.searchParams].length)invalid();value=await departments.getDepartment(scope,departmentId);}
   else value=await departments.listDepartments(scope,pagination(url.searchParams));
  }else if(['POST','PATCH'].includes(req.method)){
   if([...url.searchParams].length)invalid();
   const payload=await readBody(req,{owner:'admin',maxBytes:65536,signal});
   const current=await revalidate(req,scope);
   if(req.method==='POST'&&!departmentId){value=await departments.createDepartment(current,payload);status=201;}
   else if(req.method==='PATCH'&&departmentId&&!keys)value=await departments.updateDepartment(current,departmentId,payload);
   else if(req.method==='POST'&&keys&&!keyId){value=await departments.issueManagementKey(current,departmentId,payload);status=201;}
   else if(req.method==='POST'&&keyId)value=await departments.revokeManagementKey(current,departmentId,keyId,payload);
   else throw new AppError(405,'此入口不支持该操作','method_not_allowed');
  }else throw new AppError(405,'此入口不支持该操作','method_not_allowed');
  if(signal?.aborted)throw signal.reason;
  res.setHeader('Cache-Control','no-store');res.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});res.end(JSON.stringify(value));return true;
 }
 return {context,revalidate,handle,installed};
}
