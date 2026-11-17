import {AppError} from './store.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {createAdminDepartments} from './postgres-admin-departments.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
import {apiWeightValue} from './admin-input.mjs';
const fail=(status,code)=>{throw new AppError(status,'API账号使用范围无效或已变化，请刷新后重试',code);};
const object=value=>value&&typeof value==='object'&&!Array.isArray(value);
const identifier=value=>typeof value==='string'&&/^[a-zA-Z0-9-]{1,80}$/.test(value);
export function parseApiAccountScope(value){
 if(value===undefined)return {version:0,accounts:{}};
 if(!object(value)||Object.keys(value).sort().join(',')!=='accounts,version'||!Number.isSafeInteger(value.version)||value.version<0||!object(value.accounts)||Object.keys(value.accounts).length>10000)fail(503,'api_account_scope_invalid');
 const accounts=Object.create(null);
 for(const [id,row] of Object.entries(value.accounts)){
  if(!identifier(id)||!object(row)||Object.keys(row).sort().join(',')!=='api_usage_scope,api_weight'||row.api_usage_scope!=='explicit_api')fail(503,'api_account_scope_invalid');
  try{apiWeightValue(row.api_weight,0);}catch{fail(503,'api_account_scope_invalid');}
  accounts[id]={api_usage_scope:'explicit_api',api_weight:row.api_weight};
 }
 try{apiWeightValue(Object.values(accounts).reduce((sum,row)=>sum+row.api_weight,0),0);}catch{fail(503,'api_account_scope_invalid');}
 return {version:value.version,accounts:{...accounts}};
}
export function apiAccountScopeAllowed(account,policy='inherit'){
 return account?.auth_mode!=='openai_api'||account.api_usage_scope!=='explicit_api'||['api_only','api_first'].includes(policy);
}
export function decorateApiAccountScope(accounts,{configuration,policy='inherit',allocation=false}={}){
 const saved=parseApiAccountScope(configuration);
 if(!['inherit','api_only','api_first','subscription_only'].includes(policy))fail(503,'api_account_scope_invalid');
 return accounts.map(account=>{
  const explicit=Object.hasOwn(saved.accounts,account.id)?saved.accounts[account.id]:null;
  if(explicit&&(account.auth_mode!=='openai_api'||Number(account.api_weight)!==0))fail(503,'api_account_scope_storage_conflict');
  const scope=explicit?'explicit_api':'shared',weight=explicit?.api_weight??account.api_weight;
  return {...account,api_usage_scope:scope,api_scope_weight:weight,api_scope_version:saved.version,...(allocation&&explicit&&apiAccountScopeAllowed({...account,api_usage_scope:scope},policy)?{api_weight:weight}:{})};
 });
}
export function createPostgresApiAccountScope(store,{clock=Date.now}={}){
 const access=createRoutingDepartments(store,{clock}),admin=createAdminDepartments(store),audit=createPostgresAudit(store,{clock});
 async function read({scope,lock=false,target=store}={}){
  const routing=target===store?access:createRoutingDepartments(target,{clock}),trusted=await routing.resolve(scope,{write:lock,lock});
  return parseApiAccountScope(await routing.setting(trusted,'api_account_scope',{lock}));
 }
 const decorate=(accounts,options)=>decorateApiAccountScope(accounts,options);
 async function validateWeight(accountId,scopeName,weight,{scope,configuration}={}){
  if(!['shared','explicit_api'].includes(scopeName))fail(400,'api_usage_scope_invalid');
  const saved=configuration??await read({scope}),rows=await store.all("SELECT id,auth_mode,api_weight FROM harbor.accounts WHERE auth_mode='openai_api' AND removed_at IS NULL"+(scope?' AND department_id=$1':''),...(scope?[scope.department_id]:[]));
  const others=decorate(rows,{configuration:saved}).filter(a=>a.id!==accountId&&a.api_usage_scope===scopeName).reduce((sum,a)=>sum+Number(a.api_scope_weight),0);
  return apiWeightValue(weight,others);
 }
 async function save(accountId,payload,{scope}={}){
  if(!identifier(accountId)||!object(payload)||Object.keys(payload).some(k=>!['api_usage_scope','api_weight','expected_api_scope_version'].includes(k))||!Number.isSafeInteger(payload.expected_api_scope_version)||payload.expected_api_scope_version<0)fail(400,'api_usage_scope_invalid');
  if(Object.hasOwn(payload,'api_usage_scope')&&!['shared','explicit_api'].includes(payload.api_usage_scope))fail(400,'api_usage_scope_invalid');
  if(Object.hasOwn(payload,'api_weight'))apiWeightValue(payload.api_weight,0);
  const trusted=await admin.resolve(scope,{write:true});
  return store.transaction(async()=>{
   await admin.current(trusted,{lock:true,write:true});await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-routing-state',0))");
   const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL FOR UPDATE',accountId);if(!account)fail(404,'department_object_not_found');access.same(trusted,account);if(account.auth_mode!=='openai_api')fail(400,'api_usage_scope_invalid');
   const previous=await read({scope:trusted,lock:true});if(previous.version!==payload.expected_api_scope_version)fail(409,'api_account_scope_conflict');
   const current=decorate([account],{configuration:previous})[0],scopeName=payload.api_usage_scope??current.api_usage_scope,weight=await validateWeight(accountId,scopeName,payload.api_weight??current.api_scope_weight,{scope:trusted,configuration:previous});
   if(scopeName===current.api_usage_scope&&weight===current.api_scope_weight)return {api_usage_scope:scopeName,api_weight:weight,api_scope_version:previous.version,stored_api_weight:account.api_weight};
   if(previous.version>=Number.MAX_SAFE_INTEGER)fail(409,'api_account_scope_conflict');
   const next={version:previous.version+1,accounts:{...previous.accounts}};if(scopeName==='explicit_api')Object.defineProperty(next.accounts,accountId,{value:{api_usage_scope:scopeName,api_weight:weight},enumerable:true,writable:true,configurable:true});else delete next.accounts[accountId];
   const stored=scopeName==='explicit_api'?0:weight;
   await store.run('UPDATE harbor.accounts SET api_weight=$1,route_epoch=route_epoch+1 WHERE id=$2',stored,accountId);await access.write(trusted,'api_account_scope',next);
   await audit.write(trusted,{action:scopeName===current.api_usage_scope?'account.api.weight':'account.api.scope',detail:scopeName===current.api_usage_scope?{account:accountId,from:current.api_scope_weight,to:weight}:{account:accountId,from:{api_usage_scope:current.api_usage_scope,api_weight:current.api_scope_weight},to:{api_usage_scope:scopeName,api_weight:weight},version:next.version},subject:{kind:'account',id:accountId}});
   return {api_usage_scope:scopeName,api_weight:weight,api_scope_version:next.version,stored_api_weight:stored};
  });
 }
 async function remove(accountId,{scope}={}){
  const trusted=await admin.resolve(scope,{write:true});return store.transaction(async()=>{
   await admin.current(trusted,{lock:true,write:true});await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-routing-state',0))");
   const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 FOR UPDATE',accountId);if(!account)fail(404,'department_object_not_found');access.same(trusted,account);
   const previous=await read({scope:trusted,lock:true});if(!Object.hasOwn(previous.accounts,accountId))return;
   if(previous.version>=Number.MAX_SAFE_INTEGER)fail(409,'api_account_scope_conflict');const next={version:previous.version+1,accounts:{...previous.accounts}};delete next.accounts[accountId];await access.write(trusted,'api_account_scope',next);
  });
 }
 async function assertAccount(account,{scope,policy='inherit'}={}){
  const actual=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL FOR SHARE',account.id);if(!actual)fail(409,'api_account_scope_conflict');access.same(scope??null,actual);
  if(actual.route_epoch!==account.route_epoch)fail(409,'api_account_scope_conflict');
  const [decorated]=decorate([actual],{configuration:await read({scope}),policy});if(!apiAccountScopeAllowed(decorated,policy))fail(409,'api_account_scope_conflict');return decorated;
 }
 return {read,decorate,save,remove,validateWeight,assertAccount};
}
