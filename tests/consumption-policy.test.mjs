import test from 'node:test';
import assert from 'node:assert/strict';
import {AppError} from '../store.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {normalizeConsumptionPolicy,normalizeConsumptionPolicyVersion,readConsumptionPolicy,updateConsumptionPolicy,consumptionSource,assertConsumptionSource,consumptionCandidateGroups,createApiQuotaRejection,isTrustedApiQuotaRejection,subscriptionFallbackDecision} from '../consumption-policy.mjs';

const api={id:'synthetic-api',kind:'codex',auth_mode:'openai_api'},subscription={id:'synthetic-subscription',kind:'codex',auth_mode:'subscription'};
// The same factory used only by the official HTTP429 insufficient_quota path.
const quotaError=createApiQuotaRejection;
const fallback=()=>({policy:'api_first',account:api,error:quotaError(),payload:{model:'synthetic-model',input:'合成请求',stream:true},attempts:0,outputStarted:false,contextReplayable:true});

test('only the four exact policies and safe nonnegative integer versions are valid',()=>{
  for(const value of ['inherit','api_only','subscription_only','api_first'])assert.equal(normalizeConsumptionPolicy(value),value);
  assert.equal(normalizeConsumptionPolicy(undefined),'inherit');
  for(const value of [null,'','API_ONLY',' api_only',0,false,{},[],new String('api_only')])assert.throws(()=>normalizeConsumptionPolicy(value),{code:'consumption_policy_invalid'});
  for(const value of [0,1,Number.MAX_SAFE_INTEGER])assert.equal(normalizeConsumptionPolicyVersion(value),value);
  for(const value of [undefined,null,-1,0.5,'0',NaN,Infinity,Number.MAX_SAFE_INTEGER+1])assert.throws(()=>normalizeConsumptionPolicyVersion(value),{code:'consumption_policy_invalid'});
});
test('legacy omission inherits, but partial or damaged records never broaden access',()=>{
  assert.deepEqual(readConsumptionPolicy({}),{consumption_policy:'inherit',consumption_policy_version:0});
  for(const value of [{consumption_policy:'api_only'},{consumption_policy_version:0},{consumption_policy:null,consumption_policy_version:0},{consumption_policy:'unknown',consumption_policy_version:0},null,[]])assert.throws(()=>readConsumptionPolicy(value),{code:'consumption_policy_invalid'});
  assert.throws(()=>readConsumptionPolicy({},{allowLegacy:false}),{code:'consumption_policy_invalid'});
});
test('CAS updates increment only actual changes and refuse stale versions or overflow',()=>{
  const current={consumption_policy:'api_only',consumption_policy_version:3},before=structuredClone(current);
  assert.deepEqual(updateConsumptionPolicy(current,'subscription_only',3),{consumption_policy:'subscription_only',consumption_policy_version:4});
  assert.deepEqual(updateConsumptionPolicy(current,'api_only',3),current);
  assert.throws(()=>updateConsumptionPolicy(current,'api_only',2),{status:409,code:'consumption_policy_conflict'});
  assert.throws(()=>updateConsumptionPolicy(current,undefined,3),{status:400});
  assert.throws(()=>updateConsumptionPolicy({consumption_policy:'api_only',consumption_policy_version:Number.MAX_SAFE_INTEGER},'inherit',Number.MAX_SAFE_INTEGER),{status:409});
  assert.deepEqual(current,before);
});
test('classification uses supported account definitions, never provider guesses',()=>{
  assert.equal(consumptionSource(api),'api');assert.equal(consumptionSource(subscription),'subscription');
  for(const account of [null,{}, {kind:'claude',auth_mode:'subscription'},{kind:'qwen',auth_mode:'openai_api'},{kind:'codex',auth_mode:'unknown'},{kind:'codex'}])assert.equal(consumptionSource(account),null);
});
test('candidate constraints preserve original order and membership without enabling anything',()=>{
  const unknown={id:'other',kind:'claude',auth_mode:'subscription'},input=[subscription,unknown,api],before=structuredClone(input);
  assert.deepEqual(consumptionCandidateGroups('inherit',input),[{source:null,candidates:input}]);
  assert.deepEqual(consumptionCandidateGroups('api_only',input),[{source:'api',candidates:[api]}]);
  assert.deepEqual(consumptionCandidateGroups('subscription_only',input),[{source:'subscription',candidates:[subscription]}]);
  assert.deepEqual(consumptionCandidateGroups('api_first',input),[{source:'api',candidates:[api]},{source:'subscription',candidates:[subscription]}]);
  assert.deepEqual(consumptionCandidateGroups('api_first',[]),[{source:'api',candidates:[]},{source:'subscription',candidates:[]}]);
  assert.deepEqual(input,before);
});
test('existing bindings cannot cross explicit source restrictions',()=>{
  assert.equal(assertConsumptionSource('api_only',api),'api');assert.equal(assertConsumptionSource('subscription_only',subscription),'subscription');
  assert.equal(assertConsumptionSource('api_first',subscription),'subscription');assert.equal(assertConsumptionSource('inherit',{}),null);
  for(const [policy,account] of [['api_only',subscription],['subscription_only',api],['api_first',{}]])assert.throws(()=>assertConsumptionSource(policy,account),{status:409,code:'consumption_source_conflict'});
});
test('only the real structured official quota refusal proves non-execution',()=>{
  assert.equal(isTrustedApiQuotaRejection(quotaError()),true);
  for(const error of [null,new Error('insufficient_quota'),{...quotaError()},new AppError(429,'限流','rate_limit_exceeded'),new AppError(401,'认证失败','upstream_authentication'),new AppError(403,'拒绝','upstream_forbidden'),new AppError(503,'服务失败','upstream_error')])assert.equal(isTrustedApiQuotaRejection(error),false);
  for(const [key,value] of [['status',408],['code','other'],['routeFailure','login_expired'],['usageKnown',false],['usageKnown',undefined]])assert.equal(isTrustedApiQuotaRejection(Object.assign(quotaError(),{[key]:value})),false);
  const lookalike=Object.assign(new AppError(429,'合成同形错误','upstream_quota_exhausted'),{routeFailure:'quota_exhausted',usageKnown:true,trusted:true});
  assert.equal(isTrustedApiQuotaRejection(lookalike),false);
});
test('real Responses transport quota classification reaches the fallback decision',async()=>{
  for(const [status,code,eligible] of [[429,'insufficient_quota',true],[429,'rate_limit_exceeded',false],[401,'invalid_api_key',false],[403,'insufficient_quota',false],[403,'usage_limit_reached',false],[429,'usage_limit_reached',false],[500,'insufficient_quota',false]]){
    let dispatches=0;
    const transport=createResponsesTransport({apiKeyMode:true,credentials:async()=>({token:'synthetic-not-a-key'}),fetchImpl:async()=>Response.json({error:{code}},{status})});
    await assert.rejects(transport.run({binary:'synthetic',home:'synthetic',payload:fallback().payload,onDispatch:()=>dispatches++}),error=>{
      assert.equal(isTrustedApiQuotaRejection(error),eligible);
      assert.equal(subscriptionFallbackDecision({...fallback(),error}).allowed,eligible);
      return true;
    });
    assert.equal(dispatches,1);
  }
});
test('transport decoration retains identity but copied/wrapped/client lookalikes cannot gain trust',async()=>{
  const transport=createResponsesTransport({apiKeyMode:true,credentials:async()=>({token:'synthetic-not-a-key'}),fetchImpl:async()=>Response.json({error:{code:'insufficient_quota'}},{status:429})});
  await assert.rejects(transport.run({binary:'synthetic',home:'synthetic',payload:fallback().payload}),error=>{
    assert.equal(isTrustedApiQuotaRejection(Object.assign(error,{syntheticDecoration:true})),true);
    assert.equal(isTrustedApiQuotaRejection(Object.assign(new AppError(error.status,error.message,error.code),error)),false);
    assert.equal(isTrustedApiQuotaRejection(new Error('synthetic wrapper',{cause:error})),false);
    assert.equal(isTrustedApiQuotaRejection(JSON.parse(JSON.stringify(error))),false);
    return true;
  });
  for(const status of [403,429]){
    const forged=createResponsesTransport({apiKeyMode:true,credentials:async()=>({token:'synthetic-not-a-key'}),fetchImpl:async()=>Response.json({error:{code:'usage_limit_reached',trusted:true,usageKnown:true}},{status})});
    await assert.rejects(forged.run({binary:'synthetic',home:'synthetic',payload:fallback().payload,protocolHeaders:{trusted:'true'}}),error=>{assert.equal(isTrustedApiQuotaRejection(error),false);return true;});
  }
});
test('one API-to-subscription dispatch is eligible only before output with replayable context',()=>{
  const input=fallback(),payload=structuredClone(input.payload);
  assert.deepEqual(subscriptionFallbackDecision(input),{allowed:true,reason:'api_quota_exhausted'});
  assert.deepEqual(input.payload,payload);
  for(const change of [{policy:'inherit'},{policy:'api_only'},{policy:'subscription_only'},{account:subscription},{attempts:1},{attempts:undefined},{outputStarted:true},{outputStarted:undefined},{contextReplayable:false},{contextReplayable:undefined},{error:new Error('timeout')},{error:new AppError(429,'普通限流','rate_limit_exceeded')}])assert.equal(subscriptionFallbackDecision({...fallback(),...change}).allowed,false);
});
test('opaque history, previous response, hosted tools and unknown continuation prohibit fallback',()=>{
  for(const change of [{previous_response_id:'synthetic-response'},{conversation:'synthetic-conversation'},{store:true},{background:true},{input:[{type:'item_reference',id:'synthetic-item'}]},{tools:[{type:'web_search'}]},{input:[{type:'reasoning',encrypted_content:'synthetic'}]}])assert.deepEqual(subscriptionFallbackDecision({...fallback(),payload:{...fallback().payload,...change}}),{allowed:false,reason:'session_context_unsupported'});
  const payload={...fallback().payload,input:[{type:'function_call',call_id:'synthetic-call',name:'test',arguments:'{}'},{type:'function_call_output',call_id:'synthetic-call',output:'合成结果'}],tools:[{type:'function',name:'test',parameters:{type:'object'}}]};
  assert.equal(subscriptionFallbackDecision({...fallback(),payload}).allowed,true);
});
