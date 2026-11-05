import test from 'node:test';
import assert from 'node:assert/strict';
import {extraUsagePolicy,extraUsageBudget,extraUsageAccountConfiguration,accountExtraUsageDecision,extraUsageCandidateTier} from '../account-extra-usage.mjs';

function fixture(){return {at:1000,model:'model-a',protocol:'responses',packageAvailable:false,
  account:{id:'account-a',kind:'codex',auth_mode:'subscription',enabled:1,state:'ready',route_block:'quota_exhausted',route_epoch:3},
  policy:{enabled:true,version:2},configuration:{mode:'inherit',version:4,budget:{basis:'reference_estimate',unit:'reference_usd_atoms',limit_atoms:'20000000000000',request_reserve_atoms:'1000000000000'}},
  capability:{supported:true,platform:'codex',auth_mode:'subscription',manual_verification:false,accounting_unit:'official_credits'},
  entitlement:{account_id:'account-a',identity_epoch:3,state:'verified',source:'official',verified_at:900,valid_until:2000,expires_at:3000,models:['model-a'],protocols:['responses'],unit:'official_credits',remaining_atoms:'10'},
  usage:{account_id:'account-a',identity_epoch:3,unit:'reference_usd_atoms',basis:'reference_estimate',used_atoms:'0',reserved_atoms:'0',unresolved:false}};}
const decision=(change={})=>accountExtraUsageDecision({...fixture(),...change});

test('extra permission requires explicit global policy and package traffic remains independent',()=>{
  assert.equal(accountExtraUsageDecision({account:fixture().account,packageAvailable:false}).reason,'extra_global_disabled');
  assert.equal(decision({policy:{enabled:false,version:3}}).allowed,false);
  assert.deepEqual(decision({policy:{enabled:false,version:3},configuration:null,packageAvailable:true}),{allowed:true,mode:'package',reason:null,estimated:false});
  assert.equal(decision({packageAvailable:null}).reason,'package_availability_unknown');
  assert.equal(decision({configuration:{...fixture().configuration,mode:'deny'}}).reason,'extra_account_denied');
});

test('known verification, identity, scope and upstream refusal cannot be overridden by manual routing',()=>{
  for(const [field,value,reason] of [['state','recorded','extra_entitlement_unverified'],['state','rejected','extra_upstream_rejected'],['identity_epoch',2,'extra_identity_changed'],['account_id','other','extra_identity_changed'],['valid_until',1000,'extra_verification_expired'],['verified_at',1001,'extra_verification_expired'],['expires_at',1000,'extra_entitlement_expired'],['remaining_atoms',null,'extra_balance_unknown'],['remaining_atoms','0','extra_balance_exhausted'],['source','manual','extra_source_unverified'],['source','manual_verified','extra_source_unverified'],['models',['other'],'extra_scope_mismatch'],['protocols',['images'],'extra_scope_mismatch']])assert.equal(decision({entitlement:{...fixture().entitlement,[field]:value}}).reason,reason,field+':'+value);
  for(const capability of [null,{...fixture().capability,supported:false},{...fixture().capability,platform:'other'}])assert.equal(decision({capability}).reason,'extra_capability_unverified');
  assert.equal(decision({entitlement:{...fixture().entitlement,source:'manual_verified'},capability:{...fixture().capability,manual_verification:true}}).allowed,true);
  for(const account of [{...fixture().account,enabled:0},{...fixture().account,route_block:'login_expired'},{...fixture().account,state:'login_required'}])assert.equal(decision({account,packageAvailable:true}).reason,'account_unavailable');
});

test('budget uses exact atoms and counts shared reservations, preserving unknown consumption',()=>{
  const value=decision();assert.equal(value.allowed,true);assert.equal(value.estimated,true);assert.equal(value.reserve_atoms,'1000000000000');
  assert.equal(decision({configuration:{...fixture().configuration,budget:null}}).reason,'extra_budget_unconfigured');
  assert.equal(decision({usage:{...fixture().usage,used_atoms:'19000000000000'}}).allowed,true);
  assert.equal(decision({usage:{...fixture().usage,used_atoms:'19000000000000',reserved_atoms:'1'}}).reason,'extra_budget_exhausted');
  for(const usage of [null,{...fixture().usage,account_id:'other'},{...fixture().usage,identity_epoch:2}])assert.equal(decision({usage}).reason,'extra_usage_unavailable');
  assert.equal(decision({usage:{...fixture().usage,unresolved:true}}).reason,'extra_usage_unresolved');
  assert.equal(decision({usage:{...fixture().usage,used_atoms:null}}).reason,'extra_usage_unresolved');
  assert.equal(decision({usage:{...fixture().usage,unit:'USD'}}).reason,'extra_usage_unit_incomparable');
});

test('official and reference budgets never compare incompatible balances or imply official billing',()=>{
  const original=fixture(),configuration={...original.configuration,budget:{basis:'official',unit:'official_credits',limit_atoms:'20',request_reserve_atoms:'5'}},usage={...original.usage,basis:'official',unit:'official_credits'};
  assert.equal(decision({configuration,usage}).estimated,false);
  assert.equal(decision({configuration,usage:{...usage,reserved_atoms:'6'}}).reason,'extra_balance_insufficient');
  assert.equal(decision({configuration,usage,entitlement:{...original.entitlement,unit:'other'}}).reason,'extra_budget_unit_incomparable');
  assert.equal(decision({configuration,usage,capability:{...original.capability,accounting_unit:'other'}}).reason,'extra_budget_unit_incomparable');
  assert.equal(decision({configuration,usage:{...usage,used_atoms:'16'}}).reason,'extra_budget_exhausted');
});

test('strict configuration rejects implicit unlimited budgets, lossy amounts and unknown modes',()=>{
  assert.deepEqual(extraUsagePolicy({enabled:false,version:0}),{enabled:false,version:0});
  for(const value of [null,{enabled:1,version:0},{enabled:true,version:0,account_override:true}])assert.throws(()=>extraUsagePolicy(value),{status:400});
  assert.equal(extraUsageBudget(null),null);
  const base=fixture().configuration.budget;
  for(const value of [{...base,limit_atoms:20},{...base,limit_atoms:'1.2'},{...base,request_reserve_atoms:'0'},{...base,limit_atoms:'1'},{...base,unit:'official_credits'},{...base,basis:'subscription_price'}])assert.throws(()=>extraUsageBudget(value),{status:400});
  assert.throws(()=>extraUsageAccountConfiguration({mode:'allow',version:0,budget:base}),{status:400});
});

test('package tier wins independently of extra weights and zero candidates remain explicit',()=>{
  const extra={id:'extra',weight:100,decision:decision()},subscription={id:'package',weight:1,decision:decision({packageAvailable:true})};
  assert.deepEqual(extraUsageCandidateTier([extra,subscription]),{mode:'package',candidates:[subscription]});
  assert.deepEqual(extraUsageCandidateTier([extra]),{mode:'extra',candidates:[extra]});
  assert.deepEqual(extraUsageCandidateTier([{id:'blocked',decision:decision({policy:{enabled:false,version:0}})}]),{mode:'extra',candidates:[]});
});

test('verified platform accounts use the same policy while pay-as-you-go API keys remain independent',()=>{
  const original=fixture(),account={...original.account,kind:'qoder',auth_mode:'platform'},capability={...original.capability,platform:'qoder',auth_mode:'platform'};
  assert.equal(decision({account,capability}).mode,'extra');
  assert.equal(decision({account,capability:null,packageAvailable:true}).mode,'package');
  assert.equal(decision({account,capability:null}).reason,'extra_capability_unverified');
  assert.equal(decision({account,capability:original.capability}).reason,'extra_capability_unverified');
  assert.equal(decision({account:{...account,auth_mode:'openai_api'},capability,packageAvailable:true}).reason,'extra_not_applicable');
});
