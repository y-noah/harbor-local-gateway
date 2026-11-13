import {AppError} from './store.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {departmentPredicate} from './postgres-query-departments.mjs';
import {requireDepartmentScope} from './department-access.mjs';
const forbidden=()=>{throw new AppError(403,'批量操作身份已失效或不匹配，请重新生成预览','department_batch_owner');};
export function createMemberBatchDepartments(store,jobs,table){
 if(!['member_config_batches','member_action_batches'].includes(table))throw new TypeError('member_batch_table_invalid');
 const departments=createAdminDepartments(store);
 const resolve=(scope,{write=false}={})=>departments.resolve(scope,{write});
 function owner(scope,row){if(scope!==null){requireDepartmentScope(scope);if(row.department_id!==scope.department_id||row.actor_kind!==scope.role||row.actor_id!==scope.credential_id||row.actor_version!==scope.credential_version)forbidden();}}
 async function job(job,batchId){
  const scope=await jobs.authorizePersistedActor(job);
  if(scope!==null){requireDepartmentScope(scope);await departments.current(scope,{lock:true,write:true});const row=await store.get('SELECT department_id,actor_kind,actor_id,actor_version,job_id FROM harbor.'+table+' WHERE id=$1 AND department_id=$2',batchId,scope.department_id);if(!row||row.job_id!==job.id)forbidden();owner(scope,row);}
  return scope;
 }
 return {resolve,owner,job,predicate:departmentPredicate,columns:scope=>scope?',department_id,actor_kind,actor_id,actor_version':'',values:scope=>scope?[scope.department_id,scope.role,scope.credential_id,scope.credential_version]:[],placeholders:(scope,offset)=>scope?', $'+offset+', $'+(offset+1)+', $'+(offset+2)+', $'+(offset+3):'',current:departments.current};
}
