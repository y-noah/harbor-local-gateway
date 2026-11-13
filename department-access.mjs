import {createHash,randomBytes} from 'node:crypto';
import {AppError} from './store.mjs';

export const defaultDepartmentId='department-default';
const scopes=new WeakMap();
const roles=new Set(['super_admin','department_admin','employee','system']);
const forbidden=()=>{throw new AppError(403,'当前身份无权执行此操作','department_forbidden');};
const invalid=()=>{throw new AppError(503,'部门身份配置无效','department_identity_configuration');};
const identifier=value=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,127}$/.test(value);
const department=value=>typeof value==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9-]{0,99}$/.test(value);
function input(value,allowed){
 if(!value||typeof value!=='object'||![Object.prototype,null].includes(Object.getPrototypeOf(value)))invalid();
 const fields=Object.getOwnPropertyDescriptors(value);
 if(Reflect.ownKeys(fields).some(key=>typeof key!=='string'||!allowed.includes(key)||!Object.hasOwn(fields[key],'value')||!fields[key].enumerable))invalid();
 return Object.fromEntries(Object.entries(fields).map(([key,field])=>[key,field.value]));
}
function identity(role,value,{global=false,employee=false}={}){
 const data=input(value,['departmentId','credentialId','credentialVersion',...(employee?['memberId','memberVersion','departmentVersion']:[])]);
 if(!identifier(data.credentialId)||!Number.isSafeInteger(data.credentialVersion)||data.credentialVersion<0)invalid();
 const departmentId=data.departmentId??null;
 if(departmentId===null?!global:!department(departmentId))invalid();
 if(employee&&(!identifier(data.memberId)||[data.memberVersion,data.departmentVersion].some(value=>!Number.isSafeInteger(value)||value<0)))invalid();
 return {role,view:departmentId===null?'global':'department',department_id:departmentId,
  credential_id:data.credentialId,credential_version:data.credentialVersion,...(employee?{member_id:data.memberId,member_version:data.memberVersion,department_version:data.departmentVersion}:{})};
}

// Only server-owned authentication and job recovery hold an issuer. Public
// request fields and serialized job payloads must be reauthenticated before minting.
export function createDepartmentScopeIssuer(){
 const owner={};
 const issue=value=>{const scope=Object.freeze(value);scopes.set(scope,owner);return scope;};
 return Object.freeze({
  superAdmin:value=>issue(identity('super_admin',value,{global:true})),
  departmentAdmin:value=>issue(identity('department_admin',value)),
  employee:value=>issue(identity('employee',value,{employee:true})),
  system(value){
   const {purpose,departmentId=null}=input(value,['purpose','departmentId']);
   if(typeof purpose!=='string'||!/^[a-z][a-z0-9_.-]{0,79}$/.test(purpose))invalid();
   return issue({...identity('system',{departmentId,credentialId:'system:'+purpose,credentialVersion:0},{global:true}),purpose});
  },
  forDepartment(scope,departmentId){
   if(!scopes.has(scope)||scopes.get(scope)!==owner)forbidden();
   if(!department(departmentId))invalid();
   if(scope.role!=='super_admin'&&!(scope.view==='department'&&scope.department_id===departmentId))forbidden();
   return issue({...scope,view:'department',department_id:departmentId});
  },
  revoke(scope){if(scopes.get(scope)!==owner)forbidden();scopes.delete(scope);},
 });
}

// A brand is provenance, not an authentication cache. Callers still recheck
// department/key status on every protected operation and each export batch.
export function requireDepartmentScope(scope,{allowGlobal=false,allowSystem=false,roles:allowed=['super_admin','department_admin']}={}){
 if(typeof allowGlobal!=='boolean'||typeof allowSystem!=='boolean'||!Array.isArray(allowed)||allowed.length===0||allowed.some(role=>!roles.has(role)))invalid();
 if(!scope||typeof scope!=='object'||!scopes.has(scope))throw new AppError(403,'缺少可信部门范围','department_scope_required');
 if(!allowed.includes(scope.role)||scope.role==='system'&&!allowSystem)forbidden();
 if(scope.view==='global'&&(!allowGlobal||!['super_admin','system'].includes(scope.role)))forbidden();
 return scope;
}

const managementKeyPattern=/^hdm_[A-Za-z0-9_-]{43}$/;
export function departmentManagementKeyDigest(secret){
 if(typeof secret!=='string'||!managementKeyPattern.test(secret))return null;
 return createHash('sha256').update('harbor-department-admin\0').update(secret).digest('hex');
}
export function issueDepartmentManagementKey(){
 const secret='hdm_'+randomBytes(32).toString('base64url');
 return Object.freeze({secret,digest:departmentManagementKeyDigest(secret),prefix:secret.slice(0,12)});
}
