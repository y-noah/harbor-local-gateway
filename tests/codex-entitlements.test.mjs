import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizeLimits,accountCapacity} from '../rates.mjs';
import {normalizeCodexEntitlements,retainCodexQuotaDenials} from '../codex-entitlements.mjs';

const at=1800000000000;
const rate=overrides=>({primary:{usedPercent:100,windowDurationMins:300,resetsAt:1800000300},planType:'pro',...overrides});
const input=overrides=>({rateLimits:rate(),...overrides});
const card=overrides=>({id:'opaque-reset-fixture',grantedAt:1799999000,resetType:'codexRateLimits',status:'available',...overrides});

test('official balances preserve decimal precision and unknown unit, without granting permission',()=>{
  const q=normalizeLimits(input({ordinaryUsageAllowed:false,rateLimits:rate({credits:{hasCredits:true,unlimited:false,balance:'12345678901234567890.12345678901234567890'}})}),at);
  assert.equal(q.remaining,0);assert.equal(q.ordinaryUsageAllowed,false);
  assert.equal(q.entitlements.credits.balance,'12345678901234567890.12345678901234567890');
  assert.equal(q.entitlements.credits.unit,null);assert.equal(q.entitlements.credits.accounting_scale,null);
  assert.equal(q.entitlements.dispatch_permission_verified,false);assert.equal(q.entitlements.credits.dispatch_permission_verified,false);
});

test('missing, zero, unlimited, malformed and unknown credit states stay distinct',()=>{
  assert.equal(normalizeLimits(input(),at).entitlements,null);
  for(const [balance,expected] of [['0','0'],[null,null],['not a balance',null],[1,null],['1e9',null],['-1','-1']]){
    const result=normalizeCodexEntitlements({},rate({credits:{balance,hasCredits:false,unlimited:true}}),at);
    assert.equal(result.credits.balance,expected);assert.equal(result.credits.has_credits,false);assert.equal(result.credits.unlimited,true);
  }
  const q=normalizeLimits(input({rateLimits:rate({credits:{}}),ordinaryUsageAllowed:null}),at);
  assert.equal(q.entitlements.credits.has_credits,null);assert.equal(q.entitlements.ordinary_usage_allowed,null);
});

test('reset count is independent from unavailable, empty or truncated detail lists',()=>{
  for(const [count,rows,state] of [[0,null,'unknown'],[0,[],'complete'],[5,[],'partial'],[5,[card()],'partial'],[1,[card()],'complete']]){
    const result=normalizeCodexEntitlements({rateLimitResetCredits:{availableCount:count,credits:rows}},rate(),at).reset_cards;
    assert.equal(result.remaining,count);assert.equal(result.details_state,state);
    assert.equal(result.cards===null,rows===null);
  }
  const result=normalizeCodexEntitlements({rateLimitResetCredits:{credits:[]}},rate(),at).reset_cards;
  assert.equal(result.remaining,null);assert.equal(result.details_state,'partial');
});

test('reset details redact text, hide opaque IDs and preserve unknown expiration and windows',()=>{
  const rows=[card({title:'fixture sk-abcdefghijklmnopqrstuvwx1234567890',expiresAt:null}),card({id:'second-fixture'}),card({id:'third-fixture',expiresAt:1800000600})];
  const result=normalizeCodexEntitlements({accountId:'private-account-fixture',rateLimitResetCredits:{availableCount:3,credits:rows}},rate(),at);
  assert.equal(result.reset_cards.cards[0].expiry_known,true);assert.equal(result.reset_cards.cards[0].expires_at,null);
  assert.equal(result.reset_cards.cards[1].expiry_known,false);assert.equal(result.reset_cards.cards[1].expires_at,null);
  assert.equal(result.reset_cards.cards[2].expires_at,1800000600000);
  assert.equal(result.reset_cards.cards[0].granted_at,1799999000000);
  assert.match(result.reset_cards.cards[0].ref,/^[a-f0-9]{64}$/);assert.equal(result.reset_cards.cards[0].windows,null);
  assert.doesNotMatch(JSON.stringify(result),/opaque-reset-fixture|private-account-fixture|sk-abcdefghijkl/);
});

test('bounded reset details cannot inflate totals or infer usable balances',()=>{
  const rows=Array.from({length:140},(_,i)=>card({id:'fixture-'+i}));
  const a=normalizeCodexEntitlements({rateLimitResetCredits:{availableCount:140,credits:rows}},rate(),at);
  assert.equal(a.reset_cards.cards.length,128);assert.equal(a.reset_cards.remaining,140);assert.equal(a.reset_cards.details_state,'partial');
  const b=normalizeCodexEntitlements({rateLimitResetCredits:{availableCount:2,credits:[card(),card()]}},rate(),at);
  assert.equal(b.reset_cards.cards.length,1);assert.equal(b.reset_cards.remaining,2);assert.equal(b.reset_cards.details_state,'partial');
  assert.equal(a.credits,null);assert.equal(a.dispatch_permission_verified,false);
});

test('multi-bucket Codex selection and official refusal facts survive normalization',()=>{
  for(const refusal of ['workspace_owner_credits_depleted','workspace_member_credits_depleted','workspace_owner_usage_limit_reached','workspace_member_usage_limit_reached']){
    const q=normalizeLimits(input({ordinaryUsageAllowed:false,rateLimitsByLimitId:{other:rate({credits:{hasCredits:true,balance:'999',unlimited:true}}),codex:rate({rateLimitReachedType:refusal,spendControlReached:true,credits:{hasCredits:false,balance:'0',unlimited:false}})}}),at);
    assert.equal(q.limitReached,true);assert.equal(q.limitReachedType,refusal);assert.equal(q.spendControlReached,true);assert.equal(q.ordinaryUsageAllowed,false);assert.equal(q.entitlements.credits.balance,'0');
  }
  const q=normalizeLimits(input({rateLimits:rate({rateLimitReachedType:'future_secret_refusal'})}),at);
  assert.equal(q.limitReached,true);assert.equal(q.limitReachedType,null);assert.doesNotMatch(JSON.stringify(q),/future_secret_refusal/);
});

test('unknown reads cannot clear ordinary, spend-control or workspace denials',()=>{
  const blocked=retainCodexQuotaDenials(null,{ordinaryUsageAllowed:false,spendControlReached:true,limitReachedType:'workspace_member_credits_depleted'});
  assert.deepEqual(blocked.officialDenial,{ordinary:true,spend_control:true,workspace:true});
  const sparse=retainCodexQuotaDenials(blocked,{ordinaryUsageAllowed:null,spendControlReached:null,limitReachedType:null,remaining:100});
  assert.deepEqual(sparse.officialDenial,blocked.officialDenial);assert.equal(sparse.ordinaryUsageAllowed,null);
  const ordinary=retainCodexQuotaDenials(sparse,{ordinaryUsageAllowed:true,spendControlReached:null});
  assert.deepEqual(ordinary.officialDenial,{ordinary:false,spend_control:true,workspace:false});
  const cleared=retainCodexQuotaDenials(ordinary,{ordinaryUsageAllowed:null,spendControlReached:false});
  assert.deepEqual(cleared.officialDenial,{ordinary:false,spend_control:false,workspace:false});
});

test('legacy explicit denial fields bootstrap latches but healthy unknown remains compatible',()=>{
  for(const previous of [null,{}, {ordinaryUsageAllowed:null,spendControlReached:null}])assert.deepEqual(retainCodexQuotaDenials(previous,{}).officialDenial,{ordinary:false,spend_control:false,workspace:false});
  assert.deepEqual(retainCodexQuotaDenials({ordinaryUsageAllowed:false,spendControlReached:true,limitReachedType:'workspace_owner_usage_limit_reached'},{}).officialDenial,{ordinary:true,spend_control:true,workspace:true});
  assert.equal(retainCodexQuotaDenials({officialDenial:{workspace:true}},{ordinaryUsageAllowed:true,limitReachedType:'workspace_owner_credits_depleted'}).officialDenial.workspace,true);
  assert.equal(retainCodexQuotaDenials({officialDenial:{workspace:true}},{ordinaryUsageAllowed:true,spendControlReached:true}).officialDenial.workspace,true);
});

test('official no-window responses retain independent entitlements and denial without inventing package balance',()=>{
  for(const value of [{rateLimits:{},ordinaryUsageAllowed:false,rateLimitResetCredits:{availableCount:7,credits:null}},{rateLimits:{credits:{hasCredits:true,unlimited:false,balance:'3'}}},{rateLimits:{spendControlReached:true}},{ordinaryUsageAllowed:false}]){
    const q=normalizeLimits(value,at);assert.deepEqual(q.windows,[]);assert.equal(q.remaining,null);
  }
  const q=normalizeLimits({rateLimits:{},ordinaryUsageAllowed:false,rateLimitResetCredits:{availableCount:7,credits:null}},at);
  assert.equal(q.ordinaryUsageAllowed,false);assert.equal(q.entitlements.reset_cards.remaining,7);
  assert.equal(normalizeLimits({rateLimits:{}},at),null);
});

test('windowless unknown keeps local capacity compatibility but explicit denial cannot fall back',()=>{
  const account={token_budget:100000,budget_used:10000,quota_json:null};
  const unknown=normalizeLimits({rateLimits:{},ordinaryUsageAllowed:null},at);
  assert.deepEqual(accountCapacity({...account,quota_json:JSON.stringify(unknown)},10000,0,at),accountCapacity(account,10000,0,at));
  const denied=retainCodexQuotaDenials({ordinaryUsageAllowed:false},unknown);
  assert.equal(accountCapacity({...account,quota_json:JSON.stringify(denied)},10000,0,at).eligible,false);
  assert.equal(accountCapacity({...account,quota_json:JSON.stringify(denied)},10000,0,at+600000).eligible,false);
});

test('windowless explicit rate refusal never falls back to local budget or clears on an unknown read',()=>{
  const account={token_budget:100000,budget_used:10000};
  for(const refusal of ['rate_limit_reached','future_refusal']){
    const q=normalizeLimits({rateLimits:{rateLimitReachedType:refusal}},at);
    assert.equal(accountCapacity({...account,quota_json:JSON.stringify(q)},10000,0,at).eligible,false);
    const sparse=retainCodexQuotaDenials(q,normalizeLimits({rateLimits:{},ordinaryUsageAllowed:null},at));
    assert.equal(sparse.officialDenial.ordinary,true);assert.equal(accountCapacity({...account,quota_json:JSON.stringify(sparse)},10000,0,at).eligible,false);
    assert.equal(retainCodexQuotaDenials(sparse,{ordinaryUsageAllowed:true,limitReached:false}).officialDenial.ordinary,false);
  }
});
