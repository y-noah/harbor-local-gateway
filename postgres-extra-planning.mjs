import {AppError} from './store.mjs';
import {planningAccountGroup} from './balancing.mjs';
import {classifyExtraPackage} from './postgres-extra-runtime.mjs';
import {isExtraUsageScopeIdentifier} from './postgres-account-extra-usage.mjs';

// Verification uses persisted request scope and the original comparison groups.
// This is a read-only snapshot, never an upstream refresh or a reservation.
export async function annotateExtraPlanning(store,{accounts,workloads,at,packagePolicy,requestBoundary=false,service}={}){
 if(!store.context.getStore())return store.transaction(()=>annotateExtraPlanning(store,{accounts,workloads,at,packagePolicy,requestBoundary,service}),{level:'REPEATABLE READ',readOnly:true});
 const accountGroups=new Map(accounts.map(account=>[account.id,planningAccountGroup(account,{at,packagePolicy})])),groups=new Map();
 for(const workload of workloads){
  if(workload.protocol!=='responses'||requestBoundary&&workload.contextReplayable===false)continue;
  const group=accountGroups.get(workload.account_id)?.key;if(!group)continue;
  if(!groups.has(group))groups.set(group,{observed:new Map(),models:new Set(),reason:null});const scope=groups.get(group);
  if(typeof workload.contextModel!=='string'||!workload.contextModel.length){scope.reason??='extra_scope_unknown';continue;}
  const protocol=workload.contextProtocol;scope.observed.set(JSON.stringify([protocol,workload.contextModel]),{model:workload.contextModel,protocol});
  if(protocol!=='responses')scope.reason??='extra_protocol_scope_unsupported';
  if(!isExtraUsageScopeIdentifier(workload.contextModel))scope.reason??='extra_scope_identifier_unsupported';
  scope.models.add(workload.contextModel);
 }
 const classifications=new Map(),modelScopes=new Map(),scopeReasons=new Map();
 for(const account of accounts){
  const group=accountGroups.get(account.id),scope=groups.get(group.key),models=[...(scope?.models??[])].sort(),observed=[...(scope?.observed.values()??[])],assess=value=>{let result=classifyExtraPackage(account,{at,protocol:value?value.protocol??undefined:'responses',model:value?.model,packagePolicy});const denied=account.planningQuotaDenials;if(result.reason!=='package_scope_mismatch'&&denied){if(denied.spend_control||denied.workspace)result={mode:'blocked',packageAvailable:null,reason:'official_spending_denied'};else if(denied.ordinary&&['package','package_compatibility'].includes(result.mode))result={mode:'extra',packageAvailable:false,reason:'official_package_denied'};}return value&&!value.protocol?['package','package_compatibility'].includes(result.mode)?{...result,mode:'package_compatibility'}:result.mode==='extra'?{mode:'blocked',packageAvailable:null,reason:'extra_protocol_scope_unsupported'}:result:result;},assessments=observed.length?observed.map(assess):[assess()];
  // Every known actual model/protocol must fit, including ordinary package use.
  const classification=assessments.find(value=>value.mode==='blocked')??assessments.find(value=>value.mode==='extra')??assessments[0];classifications.set(account.id,classification);
  const reason=!group.key?'extra_planning_group_unknown':scope?.reason??(!models.length?'extra_scope_unknown':models.length>200?'extra_scope_capacity':null);scopeReasons.set(account.id,reason);modelScopes.set(account.id,reason?[]:models);
 }
 // Automatic plans only use ordinary package capacity. A manual positive
 // weight permits an individual upstream attempt; it is not an extra-budget
 // pool and must not create automatic migrations from historical permissions.
 return accounts.map(account=>{const classification=classifications.get(account.id),models=modelScopes.get(account.id),normal=['package','package_compatibility'].includes(classification.mode);return {...account,dispatch_planning:{mode:normal?classification.mode:'blocked',allowed:normal,reason:normal?null:classification.mode==='extra'?'manual_positive_weight_required':classification.reason??scopeReasons.get(account.id)??'extra_unavailable',models,allocation:null}};});
}

export function planningDispatchIdentity(account){const value=account.dispatch_planning;if(!value)throw new AppError(409,'计划缺少统一派发资格','nightly_plan_changed');return JSON.stringify(value.mode==='extra'?[value.mode,value.allocation?.basis,value.allocation?.unit,value.allocation?.available_atoms,value.models,value.identity_epoch,value.policy_version,value.configuration_version,value.entitlement_version]:[['package','package_compatibility'].includes(value.mode)?'package':value.mode]);}
