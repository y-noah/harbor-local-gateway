import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityPlan,assertFormalPlan} from '../tools/capacity/scenarios.mjs';
import {createCapacityMetrics,evaluateCapacity} from '../tools/capacity/metrics.mjs';

test('formal route aliases may tighten the purpose budget but cannot weaken class targets',()=>{
 const plan=structuredClone(capacityPlan()),overview=plan.operations.find(row=>row.id==='overview');
 Object.assign(overview,{p95:500,p99:1000});assertFormalPlan(plan);
 for(const changed of [{p95:2001,p99:4000},{p95:2000,p99:4001},{p95:0,p99:1000},{p95:NaN,p99:1000},{p95:1001,p99:1000},{p95:500,p99:Infinity},{category:'list',p95:500,p99:1000}]){
  const invalid=structuredClone(plan);Object.assign(invalid.operations.find(row=>row.id==='overview'),changed);
  assert.throws(()=>assertFormalPlan(invalid),/operation_targets_changed/);
 }
});

test('stricter alias budgets apply to aggregate and every five-minute window without changing other purposes',()=>{
 const plan=structuredClone(capacityPlan());Object.assign(plan.operations.find(row=>row.id==='overview'),{p95:500,p99:1000});
 const evaluate=duration=>{const metrics=createCapacityMetrics();for(let index=0;index<1000;index++)for(const operation of ['overview','type-statistics'])metrics.record({operation,scenario:'mixed',durationMs:duration,success:true,at:index});return evaluateCapacity({plan,preflight:{eligible:true},metrics:metrics.view()});};
 const boundary=evaluate(500);assert.ok(!boundary.failures.includes('接口延迟:overview/steady'));
 const slow=evaluate(501);assert.ok(slow.failures.includes('接口延迟:overview/steady'));assert.ok(slow.failures.includes('五分钟窗口超标:overview:0'));
 assert.ok(!slow.failures.some(value=>value.includes('type-statistics')));
 assert.equal(slow.status,'failed');assert.ok(slow.incomplete.length>0,'local timing rows cannot replace full formal evidence');
});

test('direct formal and selfcheck assessment cannot disable timing checks with invalid budgets',()=>{
 for(const profile of ['formal','selfcheck'])for(const values of [{p95:NaN,p99:NaN},{p95:500,p99:Infinity},{p95:null,p99:1000},{p95:0,p99:1000},{p95:1001,p99:1000},{p95:2001,p99:4000}]){
  const plan=structuredClone(capacityPlan(profile));Object.assign(plan.operations.find(row=>row.id==='overview'),values);
  const metrics=createCapacityMetrics();for(let index=0;index<1000;index++)metrics.record({operation:'overview',scenario:'mixed',durationMs:10000,success:true,at:index});
  const result=evaluateCapacity({plan,metrics:metrics.view()});assert.equal(result.status,'failed');assert.ok(result.failures.includes('接口预算无效:overview'));assert.ok(result.failures.includes('接口延迟:overview/steady'));assert.ok(result.failures.includes('五分钟窗口超标:overview:0'));
 }
 const plan=structuredClone(capacityPlan('selfcheck')),overview=plan.operations.find(row=>row.id==='overview');delete overview.p95;delete overview.p99;
 const metrics=createCapacityMetrics();metrics.record({operation:'overview',scenario:'mixed',durationMs:1,success:true,at:0});
 assert.ok(!evaluateCapacity({plan,metrics:metrics.view()}).failures.includes('接口预算无效:overview'),'omitted optional budgets retain the class baseline');
});
