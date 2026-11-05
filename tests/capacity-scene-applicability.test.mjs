import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityPlan,assertFormalPlan} from '../tools/capacity/scenarios.mjs';
import {createCapacityMetrics,evaluateCapacity} from '../tools/capacity/metrics.mjs';

const record=(metrics,operation,{samples=1000,scenario='mixed',durationMs=1,group='normal',variant='steady',category='read'}={})=>{for(let index=0;index<samples;index++)metrics.record({operation,scenario,durationMs,group,variant,category,success:true,at:index});};
const evaluate=(plan,metrics)=>evaluateCapacity({plan,metrics:metrics.view(),preflight:{eligible:true}});
const stateAliases=plan=>{for(const id of ['overview','account-list','session-list','account-detail','session-detail'])Object.assign(plan.operations.find(row=>row.id===id),{method:'GET',path:'/api/admin/state?people=none',canonicalHttp:'GET /api/admin/state',workloadClass:'summary'});};

test('a main scene requires its actual four-way background and retains suite-wide coverage gates for other APIs',()=>{
 const plan=structuredClone(capacityPlan());plan.scenarios=[plan.scenarios[0]];const metrics=createCapacityMetrics();
 for(const operation of ['model','first-content','request-list','employee-detail','request-summary','employee-save'])record(metrics,operation);
 let result=evaluate(plan,metrics);assert.ok(!result.incomplete.some(value=>value.startsWith('场景管理类别未测:mixed/')));assert.ok(!result.incomplete.some(value=>value.startsWith('场景接口未测:')));
 for(const operation of ['health','models','batch-status'])assert.ok(result.incomplete.includes('接口未测:'+operation));
 const missing=createCapacityMetrics();for(const operation of ['model','first-content','health','employee-detail','request-summary','employee-save'])record(missing,operation);
 result=evaluate(plan,missing);assert.ok(result.incomplete.includes('场景管理类别未测:mixed/list'),'a health check cannot replace the list workload');
});

test('canonical aliases share physical route samples but keep each purpose budget and variant minimum',()=>{
 const plan=structuredClone(capacityPlan());stateAliases(plan);const metrics=createCapacityMetrics();record(metrics,'overview',{durationMs:501,samples:999});
 let result=evaluate(plan,metrics);
 for(const id of ['overview','account-list','session-list','account-detail','session-detail']){assert.ok(!result.incomplete.includes('接口未测:'+id));assert.ok(result.incomplete.includes('样本不足:'+id+'/steady'));}
 assert.ok(result.failures.includes('接口延迟:session-list/steady'));assert.ok(result.failures.includes('五分钟窗口超标:session-list:0'));
 assert.ok(!result.failures.includes('接口延迟:overview/steady'),'the overview purpose retains its own bound while the list purpose is enforced');
 record(metrics,'overview',{samples:1,durationMs:501});record(metrics,'overview',{samples:1,variant:'cold'});result=evaluate(plan,metrics);
 assert.ok(!result.incomplete.includes('样本不足:session-list/steady'));assert.ok(result.incomplete.includes('样本不足:session-list/cold'));
});

test('wrong methods, views and paths cannot alias requests and formal canonical metadata must match the actual binding',()=>{
 const plan=structuredClone(capacityPlan());stateAliases(plan);assertFormalPlan(plan);
 for(const changed of [{method:'POST'},{path:'/api/admin/requests?view=list'},{canonicalHttp:null}]){const invalid=structuredClone(plan);Object.assign(invalid.operations.find(row=>row.id==='session-list'),changed);assert.throws(()=>assertFormalPlan(invalid),/canonical_mismatch/);}
 const metrics=createCapacityMetrics();record(metrics,'overview');const invalid=structuredClone(plan);Object.assign(invalid.operations.find(row=>row.id==='session-list'),{path:'/api/admin/members'});const result=evaluate(invalid,metrics);
 assert.ok(result.failures.includes('接口路由身份无效:session-list'));assert.ok(result.incomplete.includes('接口未测:session-list'));
 const requests=plan.operations.find(row=>row.id==='request-list');Object.assign(requests,{canonicalHttp:'GET /api/admin/requests?view=list'});const summary=plan.operations.find(row=>row.id==='request-summary');Object.assign(summary,{canonicalHttp:'GET /api/admin/requests?view=summary'});
 const separate=createCapacityMetrics();record(separate,'request-summary');assert.ok(evaluate(plan,separate).incomplete.includes('接口未测:request-list'));
});

test('burst management below one thousand stays incomplete and aliases cannot fabricate missing scene categories',()=>{
 const plan=structuredClone(capacityPlan());stateAliases(plan);plan.scenarios=[plan.scenarios.find(row=>row.id==='burst')];const metrics=createCapacityMetrics();
 record(metrics,'model',{scenario:'burst',group:'stress'});record(metrics,'first-content',{scenario:'burst'});record(metrics,'overview',{scenario:'burst',group:'stress',samples:900});
 const result=evaluate(plan,metrics);assert.ok(result.incomplete.includes('场景管理请求样本不足:burst'));for(const category of ['list','detail','configuration'])assert.ok(result.incomplete.includes('场景管理类别未测:burst/'+category));assert.ok(!result.incomplete.includes('场景管理类别未测:burst/summary'));
});

test('health, self-service and task samples cannot fill a missing prescribed background sample budget',()=>{
 for(const extra of ['health','models','batch-status']){
  const plan=structuredClone(capacityPlan());plan.scenarios=[plan.scenarios.find(row=>row.id==='burst')];const metrics=createCapacityMetrics();
  for(const [operation,samples] of [['request-list',450],['employee-detail',180],['request-summary',180],['employee-save',90],[extra,100]])record(metrics,operation,{scenario:'burst',group:'stress',samples});
  const result=evaluate(plan,metrics);assert.ok(result.incomplete.includes('场景管理请求样本不足:burst'));assert.ok(!result.incomplete.some(value=>value.startsWith('场景管理类别未测:burst/')));
 }
});

test('writes sharing a route retain independent payload and business verification coverage',()=>{
 const plan=structuredClone(capacityPlan());for(const id of ['key-save','model-permissions-save'])Object.assign(plan.operations.find(row=>row.id===id),{method:'PATCH',path:'/api/admin/keys/{key_id}',canonicalHttp:'PATCH /api/admin/keys/:id'});
 assertFormalPlan(plan);const metrics=createCapacityMetrics();record(metrics,'key-save',{category:'write'});let result=evaluate(plan,metrics);
 assert.ok(result.incomplete.includes('接口未测:model-permissions-save'));record(metrics,'model-permissions-save',{category:'write',samples:999});result=evaluate(plan,metrics);
 assert.ok(result.incomplete.includes('样本不足:model-permissions-save/steady'));assert.ok(!result.incomplete.includes('样本不足:key-save/steady'));
});
