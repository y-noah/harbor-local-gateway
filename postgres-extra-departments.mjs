import {AsyncLocalStorage} from 'node:async_hooks';
import {createDepartmentScopeIssuer,requireDepartmentScope} from './department-access.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness} from './postgres-departments.mjs';
import {AppError} from './store.mjs';

// This context carries only a live branded identity, never request JSON or a
// serialized actor. Readers on the separate query pool revalidate it themselves.
const contexts=new AsyncLocalStorage();
export function createExtraUsageDepartments(store){
 const routing=createRoutingDepartments(store),issuer=createDepartmentScopeIssuer();let initialization;
 const denied=()=>{throw new AppError(403,'额外额度缺少有效部门身份','department_forbidden');};
 async function installed(){const pending=initialization??=verifyDepartmentIsolationAvailability(store);let value;try{value=await pending;}catch(error){if(initialization===pending)initialization=undefined;throw error;}await assertDepartmentIsolationReadiness(store,value);return value;}
 async function current({lock=false,management=false}={}){
  const context=contexts.getStore(),scope=context?.scope;
  if(!await installed()){await routing.resolve(scope);return null;}
  const trusted=requireDepartmentScope(scope,{allowSystem:!management,roles:management?['super_admin','department_admin']:['super_admin','department_admin','employee','system']});
  if(context?.recovery){
   if(trusted.role!=='system'||trusted.purpose!=='account.extra.recover'||trusted.view!=='department')denied();
   if(!await store.get('SELECT id FROM harbor.departments WHERE id=$1'+(lock?' FOR SHARE':''),trusted.department_id))denied();return trusted;
  }
  if(trusted.role==='system')denied();
  if(context?.revalidateScope)await context.revalidateScope(trusted);
  return routing.resolve(trusted,{management,write:lock,lock});
 }
 async function run(scope,operation,{management=false,revalidateScope}={}){
  const context=contexts.getStore();return contexts.run({scope:scope??context?.scope,recovery:context?.recovery===true,revalidateScope:revalidateScope??context?.revalidateScope},async()=>{await current({management});return operation();});
 }
 async function recovery(requestId,operation){
  if(!await installed())return operation();
  const owner=await store.get('SELECT r.department_id FROM harbor.account_extra_holds h JOIN harbor.requests r ON r.id=h.request_id JOIN harbor.accounts a ON a.id=h.account_id AND a.department_id=r.department_id WHERE h.request_id=$1 AND r.account_id=h.account_id',requestId);
  if(!owner)throw new AppError(404,'额外额度请求不存在或归属不匹配','extra_hold_not_found');
  return contexts.run({scope:issuer.system({purpose:'account.extra.recover',departmentId:owner.department_id}),recovery:true},operation);
 }
 async function account(accountId,{lock=false,target=store}={}){
  const scope=await current({lock}),row=await target.get('SELECT id'+(scope?',department_id':'')+' FROM harbor.accounts WHERE id=$1',accountId);routing.same(scope,row);return scope;
 }
 async function request(requestId){
  const scope=await current(),row=await store.get('SELECT id,member_id,key_id'+(scope?',department_id':'')+' FROM harbor.requests WHERE id=$1',requestId);routing.same(scope,row);
  if(scope?.role==='employee'&&(row.member_id!==scope.member_id||row.key_id!==scope.credential_id))denied();return scope;
 }
 async function policy({lock=false,target=store,exclusive=false}={}){
  const scope=await current({lock});if(!scope)return target.get('SELECT enabled,version FROM harbor.extra_usage_policy WHERE singleton=1'+(lock?exclusive?' FOR UPDATE':' FOR SHARE':''));
  if(lock)await target.query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))",['harbor.extra.policy:'+scope.department_id]);
  const row=await target.get("SELECT value FROM harbor.department_settings WHERE department_id=$1 AND name='extra_usage_policy'"+(lock?exclusive?' FOR UPDATE':' FOR SHARE':''),scope.department_id);return row?.value??{enabled:false,version:0};
 }
 async function writePolicy(value){const scope=await current({lock:true,management:true});if(!scope)return store.run('UPDATE harbor.extra_usage_policy SET enabled=$1,version=$2,updated=$3 WHERE singleton=1',value.enabled,value.version,Date.now());await routing.write(scope,'extra_usage_policy',value);}
 return {current,run,recovery,account,request,policy,writePolicy,same:routing.same,bind:operation=>{const context=contexts.getStore();return (...args)=>contexts.run(context,()=>operation(...args));}};
}
