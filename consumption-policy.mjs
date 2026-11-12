import {AppError} from './store.mjs';
import {replayableResponsesInput} from './responses-replay-context.mjs';

export const consumptionPolicies=Object.freeze(['inherit','api_only','subscription_only','api_first']);
const apiQuotaRejections=new WeakSet();
const object=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const invalid=()=>{throw new AppError(400,'消耗来源或版本无效','consumption_policy_invalid');};

export function normalizeConsumptionPolicy(value,{defaultValue='inherit'}={}){
  const policy=value===undefined?defaultValue:value;
  if(!consumptionPolicies.includes(policy))invalid();
  return policy;
}
export function normalizeConsumptionPolicyVersion(value){
  if(!Number.isSafeInteger(value)||value<0)invalid();
  return value;
}
// Only an entirely absent legacy pair can inherit. Partial/corrupt persisted
// settings must never silently broaden an explicit source restriction.
export function readConsumptionPolicy(key,{allowLegacy=true}={}){
  if(!object(key))invalid();
  const policy=key.consumption_policy,version=key.consumption_policy_version;
  if(allowLegacy&&policy===undefined&&version===undefined)return {consumption_policy:'inherit',consumption_policy_version:0};
  if(policy===undefined)invalid();
  return {consumption_policy:normalizeConsumptionPolicy(policy),consumption_policy_version:normalizeConsumptionPolicyVersion(version)};
}
// The caller persists this result with its normal transaction, CAS and audit.
export function updateConsumptionPolicy(current,value,expectedVersion){
  const previous=readConsumptionPolicy(current),policy=normalizeConsumptionPolicy(value,{defaultValue:null}),expected=normalizeConsumptionPolicyVersion(expectedVersion);
  if(previous.consumption_policy_version!==expected)throw new AppError(409,'消耗来源已变化，请重新读取','consumption_policy_conflict');
  if(policy===previous.consumption_policy)return {...previous};
  if(expected===Number.MAX_SAFE_INTEGER)throw new AppError(409,'消耗来源版本已达上限','consumption_policy_conflict');
  return {consumption_policy:policy,consumption_policy_version:expected+1};
}

export function consumptionSource(account){
  if(!object(account)||account.kind!=='codex')return null;
  if(account.auth_mode==='openai_api')return 'api';
  if(account.auth_mode==='subscription')return 'subscription';
  return null;
}
export function assertConsumptionSource(policy,account){
  policy=normalizeConsumptionPolicy(policy,{defaultValue:null});
  const source=consumptionSource(account);
  if(policy==='inherit'||policy==='api_first'&&source!==null||policy==='api_only'&&source==='api'||policy==='subscription_only'&&source==='subscription')return source;
  throw new AppError(409,'会话绑定来源不符合当前Key配置','consumption_source_conflict');
}
// Input must already be admitted by the existing model/health/weight rules.
// These groups only narrow candidates; they never re-enable a blocked account,
// assign weights, authorize fallback, or prove a model's protocol compatibility.
export function consumptionCandidateGroups(policy,candidates){
  policy=normalizeConsumptionPolicy(policy,{defaultValue:null});
  if(!Array.isArray(candidates))invalid();
  if(policy==='inherit')return [{source:null,candidates:[...candidates]}];
  const group=source=>({source,candidates:candidates.filter(account=>consumptionSource(account)===source)});
  if(policy==='api_only')return [group('api')];
  if(policy==='subscription_only')return [group('subscription')];
  return [group('api'),group('subscription')];
}
// Only the official API HTTP429 insufficient_quota transport branch calls this
// factory. A private identity mark distinguishes it from subscription failures
// that the legacy transport may map to exactly the same public error fields.
export function createApiQuotaRejection(){
  const error=Object.assign(new AppError(429,'官方API额度不足，请在官方账单后台核对','upstream_quota_exhausted'),{routeFailure:'quota_exhausted',usageKnown:true});
  apiQuotaRejections.add(error);return error;
}
// Error messages, copied public fields and client-supplied trusted booleans
// cannot establish a non-executed official API quota refusal.
export function isTrustedApiQuotaRejection(error){
  return error instanceof AppError&&apiQuotaRejections.has(error)&&error.status===429&&error.code==='upstream_quota_exhausted'&&error.routeFailure==='quota_exhausted'&&error.usageKnown===true;
}
// Permission for one further dispatch, not a dispatch or a session rebind.
// The integration must still recheck permissions, model/protocol support,
// in-flight protection and the chosen subscription in its transaction.
export function subscriptionFallbackDecision({policy,account,error,payload,attempts,outputStarted,contextReplayable}={}){
  policy=normalizeConsumptionPolicy(policy,{defaultValue:null});
  const denied=reason=>({allowed:false,reason});
  if(policy!=='api_first')return denied('policy_disallows_fallback');
  if(consumptionSource(account)!=='api')return denied('not_api_source');
  if(attempts!==0)return denied('fallback_already_used_or_unknown');
  if(outputStarted!==false)return denied('output_started_or_unknown');
  if(!isTrustedApiQuotaRejection(error))return denied('quota_rejection_unconfirmed');
  if(contextReplayable!==true||!replayableResponsesInput(payload))return denied('session_context_unsupported');
  return {allowed:true,reason:'api_quota_exhausted'};
}
