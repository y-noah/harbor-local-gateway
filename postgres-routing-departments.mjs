import {nextTransferReset} from './transfer-schedule.mjs';
import {AppError} from './store.mjs';
import {requireDepartmentScope} from './department-access.mjs';
import {verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness,departmentSettingNames} from './postgres-departments.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
const instances=new WeakMap();
const denied=()=>{throw new AppError(403,'路由缺少有效部门身份或对象关系不匹配','department_forbidden');};
export function createRoutingDepartments(store,{clock=Date.now}={}){
 if(instances.has(store))return instances.get(store);
 let initialization;const admin=createAdminDepartments(store);
 async function resolve(scope,{member,write=false,employeeOnly=false,management=false,lock=write}={}){
  const first=initialization===undefined,pending=initialization??=verifyDepartmentIsolationAvailability(store);let installed;
  try{installed=await pending;}catch(error){if(initialization===pending)initialization=undefined;throw error;}
  if(!first)await assertDepartmentIsolationReadiness(store,installed);
  if(!installed){if(scope!==undefined&&scope!==null){requireDepartmentScope(scope,{allowGlobal:true,allowSystem:true,roles:['super_admin','department_admin','employee','system']});if(scope.view!=='global')throw new AppError(503,'部门隔离尚未启用','department_unavailable');}return null;}
  const trusted=requireDepartmentScope(scope,{allowSystem:!management,roles:employeeOnly?['employee']:management?['super_admin','department_admin']:['super_admin','department_admin','employee','system']});
  if(trusted.view!=='department')denied();
  if(lock&&!store.context.getStore())throw new AppError(503,'部门路由锁必须位于受理事务内','database_transaction_required');
  if(trusted.role==='super_admin'||trusted.role==='department_admin')await admin.current(trusted,{write,lock});
  else{
   const department=await store.get('SELECT id,version FROM harbor.departments WHERE id=$1 AND enabled'+(lock?' FOR SHARE':''),trusted.department_id);if(!department)denied();
   if(trusted.role==='employee'){
    if(member&&(trusted.member_id!==member.id||trusted.credential_id!==member.key_id||member.department_id!==trusted.department_id))denied();
    const owner=await store.get('SELECT id,access_version FROM harbor.members WHERE id=$1 AND department_id=$2 AND enabled=1 AND removed_at IS NULL'+(lock?' FOR SHARE':''),trusted.member_id,trusted.department_id);if(!owner||owner.access_version!==trusted.member_version||department.version!==trusted.department_version)denied();
    const key=await store.get('SELECT id FROM harbor.keys WHERE id=$1 AND member_id=$2 AND department_id=$3 AND access_version=$4 AND enabled=1 AND removed_at IS NULL AND (expires IS NULL OR expires>$5)'+(lock?' FOR SHARE':''),trusted.credential_id,trusted.member_id,trusted.department_id,trusted.credential_version,clock());if(!key)denied();
   }
  }
  return trusted;
 }
 function same(scope,row){if(!row||scope!==null&&row.department_id!==scope.department_id)throw new AppError(404,'对象不存在或无权访问','department_object_not_found');return row;}
 const identity=scope=>scope===null?'legacy-global':'department:'+scope.department_id;
 async function settings(scope,names,{lock=false}={}){
  if(names.some(name=>!departmentSettingNames.includes(name)))throw new TypeError('department_setting_invalid');
  return new Map((await store.all(scope?'SELECT name,value FROM harbor.department_settings WHERE department_id=$1 AND name=ANY($2::text[])'+(lock?' FOR SHARE':''):'SELECT name,value FROM harbor.gateway_settings WHERE name=ANY($1::text[])'+(lock?' FOR SHARE':''),...(scope?[scope.department_id,names]:[names]))).map(row=>[row.name,scope?row.value:JSON.parse(row.value)]));
 }
 async function setting(scope,name,{lock=false}={}){return (await settings(scope,[name],{lock})).get(name);}
 async function write(scope,name,value){
  if(!departmentSettingNames.includes(name))throw new TypeError('department_setting_invalid');
  if(scope)await store.run('INSERT INTO harbor.department_settings(department_id,name,value,version) VALUES($1,$2,$3::jsonb,$4) ON CONFLICT(department_id,name) DO UPDATE SET value=EXCLUDED.value,version=harbor.department_settings.version+1',scope.department_id,name,JSON.stringify(value),value.version??0);
  else await store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',name,JSON.stringify(value));
 }
 async function nextReset(scope,at=clock()){const values=await settings(scope,['balancing','transfer_schedule']);return nextTransferReset(at,values.get('balancing')??{},values.get('transfer_schedule')??{});}
 const result={resolve,same,identity,settings,setting,write,nextReset};instances.set(store,result);return result;
}
