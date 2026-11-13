import {requireDepartmentScope} from './department-access.mjs';
import {AppError} from './store.mjs';
import {createQueryDepartments} from './postgres-query-departments.mjs';

const forbidden=()=>{throw new AppError(403,'当前身份无权访问此部门','department_forbidden');};
export function createAdminDepartments(store){
 const departments=createQueryDepartments(store);
 async function current(scope,{lock=false,write=lock}={}){
  if(scope===null)return scope;requireDepartmentScope(scope,{allowGlobal:true});if(scope.view==='global')return scope;
  const row=scope.role==='department_admin'?await store.get(`SELECT d.id FROM harbor.departments d JOIN harbor.department_admin_keys k ON k.department_id=d.id
   WHERE d.id=$1 AND d.enabled AND k.id=$2 AND k.version=$3 AND k.enabled AND k.revoked_at IS NULL`+(lock?' FOR SHARE OF d,k':''),scope.department_id,scope.credential_id,scope.credential_version):await store.get('SELECT id FROM harbor.departments WHERE id=$1'+(write?' AND enabled':'')+(lock?' FOR SHARE':''),scope.department_id);
  if(!row)forbidden();return scope;
 }
 async function resolve(scope,{write=false,payload}={}){
  const trusted=await departments.resolve(scope);
  if(trusted!==null){
   if(write&&trusted.view!=='department')forbidden();
   if(payload&&Object.hasOwn(payload,'department_id')&&payload.department_id!==trusted.department_id)forbidden();
   await current(trusted,{write});
  }else if(payload&&Object.hasOwn(payload,'department_id'))throw new AppError(503,'部门隔离尚未启用','department_unavailable');
  return trusted;
 }
 function same(scope,row){if(!row||scope!==null&&scope.view==='department'&&row.department_id!==scope.department_id)throw new AppError(404,'对象不存在或无权访问','department_object_not_found');return row;}
 const unsupported=()=>{throw new AppError(503,'此管理入口尚未完成部门隔离适配','department_operation_unavailable');};
 return {resolve,current,same,unsupported};
}
