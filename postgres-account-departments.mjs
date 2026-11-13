import {AppError} from './store.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {createDepartmentScopeIssuer,requireDepartmentScope} from './department-access.mjs';
import {verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness} from './postgres-departments.mjs';
const denied=()=>{throw new AppError(403,'账号管理身份或部门范围已失效','department_forbidden');};
export function createAccountDepartments(store,{quotaSystem=false,clock=Date.now}={}){
 const sessions=new WeakMap();let nextSession=0;
 const admin=createAdminDepartments(store),routing=createRoutingDepartments(store,{clock}),issuer=createDepartmentScopeIssuer();let initialized;
 async function installed(){const pending=initialized??=verifyDepartmentIsolationAvailability(store);let initial;try{initial=await pending;}catch(error){if(initialized===pending)initialized=undefined;throw error;}await assertDepartmentIsolationReadiness(store,initial);return initial;}
 async function resolve(options={}, {write=false,lock=false}={}){
  let scope=options.scope;
  if(options.revalidateScope!==undefined){if(typeof options.revalidateScope!=='function')throw new AppError(503,'账号身份复验配置无效','account_configuration');const current=await options.revalidateScope(scope);if(current===null||current===false)denied();if(scope&&(!current||['role','view','department_id','credential_id','credential_version'].some(key=>current[key]!==scope[key])))denied();if(scope)scope=current;}
  if(scope?.role==='system'){
   if(!quotaSystem)denied();const trusted=requireDepartmentScope(scope,{allowSystem:true,roles:['system']});if(trusted.purpose!=='account.quota.refresh')denied();return routing.resolve(trusted,{write,lock});
  }
  const trusted=await admin.resolve(scope,{write});if(lock)await admin.current(trusted,{lock:true,write});return trusted;
 }
 async function account(accountId,options={}, {write=false,lock=false,includeRemoved=false}={}){
  const scope=await resolve(options,{write,lock}),row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1'+(includeRemoved?'':' AND removed_at IS NULL')+(scope?.view==='department'?' AND department_id=$2':'')+(lock?' FOR UPDATE':''),accountId,...(scope?.view==='department'?[scope.department_id]:[]));if(!row)throw new AppError(404,'账号不存在或已移除','department_object_not_found');return {scope,row};
 }
 async function quotaScope(accountId){
  if(!quotaSystem)denied();if(!await installed())return undefined;
  const row=await store.get('SELECT a.department_id FROM harbor.accounts a JOIN harbor.departments d ON d.id=a.department_id AND d.enabled WHERE a.id=$1 AND a.removed_at IS NULL',accountId);if(!row)throw new AppError(404,'账号不存在或所属部门已停用','department_object_not_found');return issuer.system({purpose:'account.quota.refresh',departmentId:row.department_id});
 }
 function identity({scope,revalidateScope}={}){let session='';if(revalidateScope){if(!sessions.has(revalidateScope))sessions.set(revalidateScope,++nextSession);session=sessions.get(revalidateScope);}return (scope?[scope.role,scope.department_id,scope.credential_id,scope.credential_version].join(':'):'legacy')+':'+session;}
 return {resolve,account,quotaScope,installed,identity};
}

export const accountAuthorizationError=error=>['department_forbidden','department_scope_required','department_object_not_found'].includes(error?.code);
