import test from 'node:test';
import assert from 'node:assert/strict';
import {packageDefinition,accountPackageBudget,packageBudget,packageFingerprint} from '../account-package-budget.mjs';
const at=1800000000000;
const definition=(capacity=100)=>({name:'合成套餐',platform:'synthetic',auth_modes:['subscription'],protocols:['responses'],models:['*'],comparison:{group:'synthetic-common',unit:'common-unit',rule_id:'synthetic-rule',rule_version:1,units:{base:1,kilo:1000}},official:{ids:['synthetic-plan'],granularity:'exact'},source:{kind:'administrator',reference:'合成已确认容量；不代表官方倍率',confirmed:true},effective_at:at-1,windows:[{id:'weekly',minutes:10080,capacity,unit:'base'}]});
const account=(id,capacity=100,used=50)=>({id,kind:'synthetic',auth_mode:'subscription',enabled:1,state:'ready',route_epoch:0,quota_json:JSON.stringify({checked:at,plan:'synthetic-plan',windows:[{id:'weekly',minutes:10080,used,reset:at+168*3600000}]}),package_selection:{mode:'selected',template_id:id,template_version:1,revision:1,definition:definition(capacity)}});
const options={at,enabled:true};
test('capacity, percentage and time produce configurable ratios with no commercial plan assumptions',()=>{
 let result=packageBudget([account('a'),account('b',400)],options);assert.deepEqual(result.groups[0].weights,{a:20,b:80});result=packageBudget([account('a'),account('b',400,90)],options);assert.ok(Math.abs(result.groups[0].weights.a-55.55555555555556)<1e-10);assert.equal(result.smoothing_hours,24);
});
test('multiple windows constrain common hourly capacity instead of adding balances',()=>{
 const a=account('a');a.package_selection.definition.windows.push({id:'short',minutes:300,capacity:10,unit:'base'});const q=JSON.parse(a.quota_json);q.windows.push({id:'short',minutes:300,used:50,reset:at+3600000});a.quota_json=JSON.stringify(q);const result=accountPackageBudget(a,options);assert.equal(result.eligible,true);assert.equal(result.score,5/24);assert.equal(result.windows.find(w=>w.id==='short').denominator,24);
});
test('official absolute remaining takes precedence without multiplying by package capacity or percentage',()=>{
 const a=account('a',100000,99),q=JSON.parse(a.quota_json);q.windows[0].absolute={source:'official',remaining:2,unit:'kilo'};a.quota_json=JSON.stringify(q);const result=accountPackageBudget(a,options);assert.equal(result.score,2000/168);assert.equal(result.estimate,false);assert.equal(result.windows[0].source,'official_absolute');
});
test('same common-unit name does not bypass versioned conversion or scope compatibility',()=>{
 for(const mutate of [b=>b.package_selection.definition.comparison.rule_version++,b=>b.package_selection.definition.comparison.units.kilo=999,b=>b.package_selection.definition.models=['synthetic-model'],b=>{b.kind='other';b.package_selection.definition.platform='other';}]){const b=account('b');mutate(b);assert.equal(packageBudget([account('a'),b],options).groups.length,2);}
});
test('unknown/ambiguous/missing conversion windows do not invent weights',()=>{
 for(const mutate of [a=>a.package_selection.mode='unknown',a=>a.package_selection.definition.windows[0].capacity=null,a=>a.package_selection.definition.source.confirmed=false,a=>a.package_selection.definition.windows[0].minutes=300,a=>a.package_selection.definition.windows.push({id:'missing',minutes:300,capacity:1,unit:'base'})]){const a=account('a');mutate(a);const result=accountPackageBudget(a,options);assert.equal(result.eligible,false);assert.equal(result.score,null);assert.equal(packageBudget([a],options).groups.length,0);}
});
test('family-level official identification requires explicit manual capacity confirmation',()=>{
 const a=account('a');a.package_selection.definition.official.granularity='family';assert.equal(accountPackageBudget(a,options).recognition.status,'partial');assert.equal(accountPackageBudget(a,options).reason,'capacity_basis_unconfirmed');a.package_selection.official_confirmation={plan:'synthetic-plan',template_version:1,recognition_version:0,basis_confirmed:true};assert.equal(accountPackageBudget(a,options).eligible,true);a.package_selection.template_version++;assert.equal(accountPackageBudget(a,options).eligible,false);
});
test('official conflicts remain visible and confirmations cannot survive official identity changes',()=>{
 const a=account('a'),q=JSON.parse(a.quota_json);q.plan='different';a.quota_json=JSON.stringify(q);assert.equal(accountPackageBudget(a,options).reason,'official_plan_conflict');a.package_selection.official_confirmation={plan:'different',template_version:1,recognition_version:0,basis_confirmed:true};assert.equal(accountPackageBudget(a,options).recognition.exception_confirmed,true);q.plan='changed-again';a.quota_json=JSON.stringify(q);assert.equal(accountPackageBudget(a,options).eligible,false);
});
test('exhaustion, stale snapshots, expired reset and login failure remain distinct from unknown zero',()=>{
 for(const [mutate,reason] of [[a=>a.route_block='quota_exhausted','quota_exhausted'],[a=>a.route_block='login_expired','login_expired'],[a=>{const q=JSON.parse(a.quota_json);q.checked-=300001;a.quota_json=JSON.stringify(q);},'quota_unavailable'],[a=>{const q=JSON.parse(a.quota_json);q.windows[0].reset=at;a.quota_json=JSON.stringify(q);},'quota_expired']]){const a=account('a');mutate(a);const result=accountPackageBudget(a,options);assert.equal(result.reason,reason);assert.equal(result.score,null);}
});
test('any valid exhausted window excludes automatic allocation even if other windows remain',()=>{
 const a=account('a');a.package_selection.definition.windows.push({id:'short',minutes:300,capacity:1,unit:'base'});const q=JSON.parse(a.quota_json);q.windows.push({id:'short',minutes:300,used:100,reset:at+3600000});a.quota_json=JSON.stringify(q);assert.equal(accountPackageBudget(a,options).reason,'quota_exhausted');
});
test('disabled policy preserves legacy path; API shares stay explicitly manual',()=>{
 const a=account('a');assert.equal(accountPackageBudget(a,{at}).mode,'legacy');a.package_selection={mode:'legacy'};assert.equal(accountPackageBudget(a,options).mode,'legacy');a.auth_mode='openai_api';assert.equal(accountPackageBudget(a,options).mode,'manual_api');assert.equal(accountPackageBudget(a,options).reason,'api_manual_share');
});
test('identity changes on assignment/scope/official identity but not ordinary remaining percentages',()=>{
 const a=account('a'),before=accountPackageBudget(a,options).identity,q=JSON.parse(a.quota_json);q.windows[0].used=75;a.quota_json=JSON.stringify(q);assert.equal(accountPackageBudget(a,options).identity,before);a.package_selection.template_version++;assert.notEqual(accountPackageBudget(a,options).identity,before);assert.equal(packageFingerprint({b:1,a:2}),packageFingerprint({a:2,b:1}));
});
test('definition validation rejects missing units, invalid capacity, unknown fields and invented smoothing',()=>{
 for(const mutate of [d=>d.comparison.units={},d=>d.windows[0].capacity=-1,d=>d.smoothing_hours=1,d=>d.comparison.rule_version=0,d=>d.auth_modes=['openai_api']]){const d=definition();mutate(d);assert.throws(()=>packageDefinition(d),{code:'package_configuration_invalid'});}
});
test('relative account overrides are explicit estimates and scope failures do not broaden access',()=>{
 const a=account('a');a.package_selection.capacity_override={reason:'合成个体容量差异',capacities:{weekly:200},confirmed:true};const result=accountPackageBudget(a,options);assert.equal(result.score,100/168);assert.equal(result.windows[0].source,'account_estimate');assert.equal(accountPackageBudget(a,{...options,protocol:'unsupported'}).reason,'package_scope_mismatch');
});

test('P3 malformed nullable quota windows degrade one account without throwing across the whole pool',()=>{
 const a=account('a'),q=JSON.parse(a.quota_json);for(const window of [null,4,[],false]){q.windows=[window];a.quota_json=JSON.stringify(q);assert.equal(accountPackageBudget(a,options).reason,'quota_unavailable');assert.equal(packageBudget([a,account('b')],options).groups[0].accounts[0],'b');}
});
test('P1 descriptive notes and record timestamps do not change effective capacity identity',()=>{
 const a=account('a'),before=accountPackageBudget(a,options).identity;a.package_selection.updated=at+1;a.package_selection.reason='仅管理员说明';assert.equal(accountPackageBudget(a,options).identity,before);
});

test('P5 official positive absolute remaining wins over a rounded 100-percent window but not official blocking',()=>{
 const a=account('a',100,100),q=JSON.parse(a.quota_json);q.windows[0].absolute={source:'official',remaining:2,unit:'base'};a.quota_json=JSON.stringify(q);assert.equal(accountPackageBudget(a,options).score,2/168);q.limitReached=true;a.quota_json=JSON.stringify(q);assert.equal(accountPackageBudget(a,options).reason,'quota_exhausted');
});
test('P6 fingerprints bind actual quota-window and absolute-unit scope independently of ordinary usage',()=>{
 const a=account('a'),before=accountPackageBudget(a,options).identity,q=JSON.parse(a.quota_json);q.windows[0].minutes=300;a.quota_json=JSON.stringify(q);assert.notEqual(accountPackageBudget(a,options).identity,before);q.windows[0].minutes=10080;q.windows[0].absolute={source:'official',remaining:20,unit:'base'};a.quota_json=JSON.stringify(q);const absolute=accountPackageBudget(a,options).identity;q.windows[0].absolute.unit='unknown';a.quota_json=JSON.stringify(q);assert.notEqual(accountPackageBudget(a,options).identity,absolute);
});
