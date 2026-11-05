import {codexQuotaDenials} from './codex-entitlements.mjs';
import {AppError} from './store.mjs';

const object=(value,fields)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>fields.includes(key));
const invalid=message=>{throw new AppError(400,message,'extra_usage_configuration_invalid');};
const integer=value=>Number.isSafeInteger(value)&&value>=0;
const atoms=value=>typeof value==='string'&&/^(0|[1-9][0-9]{0,39})$/.test(value)?BigInt(value):null;
const list=value=>Array.isArray(value)&&value.length>0&&value.length<=200&&new Set(value).size===value.length&&value.every(item=>typeof item==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(item));

export function extraUsagePolicy(value){
  if(!object(value,['enabled','version'])||typeof value.enabled!=='boolean'||!integer(value.version))invalid('额外额度总开关或版本无效');
  return {enabled:value.enabled,version:value.version};
}

// Atomic amounts have an explicit accounting unit and never imply an official
// balance. Reference USD uses the existing pricing atoms (10^-12 USD).
export function extraUsageBudget(value){
  if(value===null)return null;
  if(!object(value,['basis','unit','limit_atoms','request_reserve_atoms'])||!['reference_estimate','official'].includes(value.basis)||typeof value.unit!=='string'||!/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,79}$/.test(value.unit)||atoms(value.limit_atoms)===null||atoms(value.request_reserve_atoms)===null||atoms(value.request_reserve_atoms)<=0n||atoms(value.request_reserve_atoms)>atoms(value.limit_atoms))invalid('额外额度预算须有明确单位、总额及单请求预留');
  if(value.basis==='reference_estimate'&&value.unit!=='reference_usd_atoms')invalid('参考消耗预算须使用参考USD原子单位，不能冒充官方余额');
  return {...value};
}

export function extraUsageAccountConfiguration(value){
  if(!object(value,['mode','budget','version'])||!['inherit','deny'].includes(value.mode)||!integer(value.version))invalid('账号额外额度须选择继承全局或禁止');
  return {mode:value.mode,budget:extraUsageBudget(value.budget),version:value.version};
}

// Inputs are server-owned snapshots. HTTP payloads must pass the administrator
// service, version checks and upstream-verification boundary before reaching
// this evaluator. A manually recorded balance or unused reset card is not a
// verification source and cannot create dispatch permission.
export function accountExtraUsageDecision({account,policy={enabled:false,version:0},configuration={mode:'inherit',budget:null,version:0},entitlement,capability,usage,model,protocol,packageAvailable,at=Date.now()}={}){
  const deny=reason=>({allowed:false,mode:'blocked',reason,estimated:null});
  if(!integer(at))return deny('extra_clock_invalid');
  if(!account||!account.enabled||account.removed_at||!['ready','degraded'].includes(account.state)||account.route_block&&account.route_block!=='quota_exhausted')return deny('account_unavailable');
  if(account.auth_mode==='openai_api')return deny('extra_not_applicable');
  let quota;try{quota=typeof account.quota_json==='string'?JSON.parse(account.quota_json):account.quota_json;}catch{}const denial=codexQuotaDenials(quota);if(denial.spend_control||denial.workspace)return deny('official_spending_denied');if(denial.ordinary&&packageAvailable===true)return deny('official_package_denied');
  // Package traffic is unaffected by the extra-use switch or extra budget.
  if(packageAvailable===true)return {allowed:true,mode:'package',reason:null,estimated:false};
  if(packageAvailable!==false)return deny('package_availability_unknown');
  let global,config;
  try{global=extraUsagePolicy(policy);config=extraUsageAccountConfiguration(configuration);}catch{return deny('extra_configuration_invalid');}
  if(!global.enabled)return deny('extra_global_disabled');
  if(config.mode==='deny')return deny('extra_account_denied');
  if(!capability||capability.supported!==true||capability.platform!==account.kind||capability.auth_mode!==account.auth_mode)return deny('extra_capability_unverified');
  if(!entitlement||entitlement.state!=='verified')return deny(entitlement?.state==='rejected'?'extra_upstream_rejected':'extra_entitlement_unverified');
  if(entitlement.account_id!==account.id||!integer(account.route_epoch)||entitlement.identity_epoch!==account.route_epoch)return deny('extra_identity_changed');
  if(!integer(entitlement.verified_at)||!integer(entitlement.valid_until)||entitlement.verified_at>at||entitlement.valid_until<=at)return deny('extra_verification_expired');
  if(entitlement.expires_at!==null&&(!integer(entitlement.expires_at)||entitlement.expires_at<=at))return deny('extra_entitlement_expired');
  if(!['official','manual_verified'].includes(entitlement.source)||entitlement.source==='manual_verified'&&capability.manual_verification!==true)return deny('extra_source_unverified');
  if(!list(entitlement.models)||!list(entitlement.protocols)||typeof model!=='string'||typeof protocol!=='string'||!entitlement.models.includes(model)||!entitlement.protocols.includes(protocol))return deny('extra_scope_mismatch');
  const remaining=atoms(entitlement.remaining_atoms);
  if(remaining===null)return deny('extra_balance_unknown');
  if(remaining===0n)return deny('extra_balance_exhausted');
  if(!config.budget)return deny('extra_budget_unconfigured');
  const budget=config.budget;
  // The upstream balance and the local reference estimate remain separate.
  // Only comparable official units can constrain each other numerically.
  if(budget.basis==='official'&&(entitlement.unit!==budget.unit||capability.accounting_unit!==budget.unit))return deny('extra_budget_unit_incomparable');
  if(!usage||usage.account_id!==account.id||usage.identity_epoch!==account.route_epoch)return deny('extra_usage_unavailable');
  if(usage.unit!==budget.unit||usage.basis!==budget.basis)return deny('extra_usage_unit_incomparable');
  const used=atoms(usage.used_atoms),reserved=atoms(usage.reserved_atoms),reserve=atoms(budget.request_reserve_atoms),limit=atoms(budget.limit_atoms);
  if(used===null||reserved===null||usage.unresolved===true||typeof usage.unresolved!=='boolean')return deny('extra_usage_unresolved');
  if(used+reserved+reserve>limit)return deny('extra_budget_exhausted');
  if(budget.basis==='official'&&reserved+reserve>remaining)return deny('extra_balance_insufficient');
  return {allowed:true,mode:'extra',reason:null,estimated:budget.basis==='reference_estimate',reserve_atoms:budget.request_reserve_atoms,unit:budget.unit,basis:budget.basis,policy_version:global.version,configuration_version:config.version,identity_epoch:account.route_epoch};
}

// Call only after model/protocol/login/concurrency eligibility is established.
// Weights are resolved within the selected tier by the existing router.
export function extraUsageCandidateTier(candidates){
  const packageCandidates=candidates.filter(candidate=>candidate.decision?.allowed===true&&candidate.decision.mode==='package');
  return packageCandidates.length?{mode:'package',candidates:packageCandidates}:{mode:'extra',candidates:candidates.filter(candidate=>candidate.decision?.allowed===true&&candidate.decision.mode==='extra')};
}
