import {AppError} from './store.mjs';
import {requireDepartmentScope} from './department-access.mjs';
import {verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness} from './postgres-departments.mjs';

const authorize=scope=>requireDepartmentScope(scope,{allowGlobal:true});
const retryableInitialization=error=>typeof error?.code==='string'&&(/^(08|53)[0-9A-Z]{3}$/.test(error.code)||['57014','57P01','57P02','57P03','ECONNRESET','ECONNREFUSED','ETIMEDOUT','EPIPE'].includes(error.code));
export function createQueryDepartments(store,{systemReadPurposes=[]}={}){
 if(!Array.isArray(systemReadPurposes)||systemReadPurposes.some(value=>typeof value!=='string'||!value.length))throw new TypeError('department_system_purposes_invalid');
 const purposes=new Set(systemReadPurposes);
 const authorizeRead=scope=>{
  if(scope?.role!=='system')return authorize(scope);
  const trusted=requireDepartmentScope(scope,{allowSystem:true,roles:['system']});
  if(!purposes.has(trusted.purpose))throw new AppError(403,'后台身份不能读取此数据','department_forbidden');
  return trusted;
 };
 let initialization;
 return {async resolve(scope){
  const pending=initialization??=verifyDepartmentIsolationAvailability(store);let installed;
  try{installed=await pending;}catch(error){if(initialization===pending&&retryableInitialization(error))initialization=undefined;throw error;}
  // An old reader cannot acquire new authority from a live schema installation.
  await assertDepartmentIsolationReadiness(store,installed);
  if(installed)return authorizeRead(scope);
  if(scope!==undefined&&authorize(scope).view!=='global')throw new AppError(503,'部门隔离尚未启用','department_unavailable');
  return null;
 },predicate(scope,alias,args,offset=0){return predicate(scope,alias,args,offset,authorizeRead);}};
}

export function departmentPredicate(scope,alias,args,offset=0){return predicate(scope,alias,args,offset,authorize);}
function predicate(scope,alias,args,offset,authorizeRead){
 if(!/^[a-z][a-z0-9_]*$/.test(alias))throw new TypeError('department_alias_invalid');
 if(scope===null)return 'TRUE';
 const trusted=authorizeRead(scope);if(trusted.view==='global')return 'TRUE';
 args.push(trusted.department_id);return alias+'.department_id=$'+(offset+args.length);
}
export function queryDepartmentIdentity(scope){
 if(scope===null)return 'legacy-global';
 const trusted=authorize(scope);return trusted.view==='global'?'global':'department:'+trusted.department_id;
}
