import {AsyncLocalStorage} from 'node:async_hooks';
import {AppError} from './store.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {createDepartmentScopeIssuer,requireDepartmentScope} from './department-access.mjs';
import {verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness} from './postgres-departments.mjs';

const denied=()=>{throw new AppError(403,'设备所属部门或当前身份无效','department_forbidden');};
export function createDeviceDepartments(store,{clock=Date.now}={}){
 const actors=new AsyncLocalStorage(),admins=new WeakMap(),routing=createRoutingDepartments(store,{clock}),issuer=createDepartmentScopeIssuer();let initialization;
 const admin=target=>{let value=admins.get(target);if(!value){value=createAdminDepartments(target);admins.set(target,value);}return value;};
 async function installed(){const pending=initialization??=verifyDepartmentIsolationAvailability(store);let value;try{value=await pending;}catch(error){if(initialization===pending)initialization=undefined;throw error;}await assertDepartmentIsolationReadiness(store,value);return value;}
 async function sessionCurrent(actor){
  if(actor.scope===null||!actor.revalidateScope)return;
  const current=await actor.revalidateScope(actor.scope);requireDepartmentScope(current,{allowGlobal:true});
  if(['role','view','department_id','credential_id','credential_version'].some(key=>current[key]!==actor.scope[key]))denied();
 }
 async function management(op,scope,{write=false,revalidateScope}={}){
  if(revalidateScope!==undefined&&typeof revalidateScope!=='function')throw new AppError(503,'设备身份复验配置无效','device_configuration');
  const trusted=await admin(store).resolve(scope,{write}),actor={kind:'management',scope:trusted,write,revalidateScope};
  return actors.run(actor,async()=>{await sessionCurrent(actor);return op();});
 }
 async function publicOperation(op){return actors.run({kind:'invitation'},op);}
 async function employee(op,scope,identity){const trusted=await routing.resolve(scope,{member:identity,employeeOnly:true});return actors.run({kind:'employee',scope:trusted},op);}
 async function member(memberId,{lock=false,target=store}={}){
  const enabled=await installed(),actor=actors.getStore();if(enabled&&!actor)denied();
  if(enabled&&actor.kind==='management'){await sessionCurrent(actor);await admin(target).current(actor.scope,{lock,write:actor.write});}
  if(enabled&&actor.kind==='employee')await routing.resolve(actor.scope,{employeeOnly:true,lock,write:lock});
  // A public invitation is a narrow capability; resolve its owner from the DB,
  // then lock the department before its member and invitation/device rows.
  if(enabled&&actor.kind==='invitation'){
   const owner=await target.get('SELECT department_id FROM harbor.members WHERE id=$1 AND removed_at IS NULL',memberId);if(!owner)denied();
   if(!await target.get('SELECT id FROM harbor.departments WHERE id=$1 AND enabled'+(lock?' FOR SHARE':''),owner.department_id))denied();
  }
  const bound=enabled&&actor.scope?.view==='department';
  const row=await target.get('SELECT id,enabled,device_mode,device_policy_version'+(enabled?',department_id':'')+' FROM harbor.members WHERE id=$1 AND removed_at IS NULL'+(bound?' AND department_id=$2':'')+(lock?(actor?.kind==='employee'?' FOR SHARE':' FOR UPDATE'):''),memberId,...(bound?[actor.scope.department_id]:[]));
  if(!row)throw new AppError(404,'员工不存在','device_member_not_found');
  if(enabled){
   if(actor.kind==='invitation'&&!row.enabled)denied();
   if(actor.scope?.view==='department'&&row.department_id!==actor.scope.department_id)throw new AppError(404,'对象不存在或无权访问','department_object_not_found');
   if(actor.kind==='employee'&&row.id!==actor.scope.member_id)denied();
  }
  return row;
 }
 function bind(op){const actor=actors.getStore();return (...args)=>actors.run(actor,()=>op(...args));}
 async function revalidate({target=store}={}){const actor=actors.getStore();if(await installed()&&actor?.kind!=='management')denied();if(actor?.kind==='management'&&actor.scope!==null){await sessionCurrent(actor);await admin(target).current(actor.scope,{write:actor.write});}}
 function current(){return actors.getStore()?.scope??null;}
 async function auditScope(memberId){const actor=actors.getStore();if(!await installed())return undefined;if(actor?.scope)return actor.scope;if(actor?.kind!=='invitation')denied();const owner=await member(memberId);return issuer.system({departmentId:owner.department_id,purpose:'device.enrollment'});}
 return {management,publicOperation,employee,member,installed,current,revalidate,bind,auditScope};
}
