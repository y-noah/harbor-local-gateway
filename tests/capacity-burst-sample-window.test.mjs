import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityPlan,targets,assertFormalPlan} from '../tools/capacity/scenarios.mjs';
import {runOpenLoop} from '../tools/capacity/driver.mjs';
import {createCapacityMetrics} from '../tools/capacity/metrics.mjs';

test('formal burst preserves its stress and recovery deadlines while actually scheduling enough same-scene management samples',async()=>{
 const plan=capacityPlan(),scene=plan.scenarios.find(row=>row.id==='burst'),metrics=createCapacityMetrics(),categories=['list','list','list','list','list','detail','detail','summary','summary','configuration'],counts={};let time=0;
 assertFormalPlan(plan);assert.equal(scene.modelQps,10);assert.equal(scene.adminQps,5);assert.deepEqual(scene.burst,{atMs:60000,durationMs:60000,qps:30,recoveryMs:60000});assert.ok(scene.durationMs>=scene.burst.atMs+scene.burst.durationMs+scene.burst.recoveryMs);
 await runOpenLoop({durationMs:scene.durationMs,segments:[{startMs:0,endMs:scene.durationMs,qps:scene.adminQps}],scenario:scene.id,metrics,now:()=>time,sleep:async ms=>{time+=ms;},send:async event=>{assert.equal(event.scenario,scene.id);const category=categories[event.sequence%categories.length];counts[category]=(counts[category]??0)+1;}});
 const observed=metrics.view();assert.ok(observed.arrivals.sent>=targets.minSamples);assert.equal(observed.arrivals.missed,0);assert.equal(observed.arrivals.inflight,0);assert.equal(observed.violations.length,0);assert.deepEqual(counts,{list:500,detail:200,summary:200,configuration:100});
 const short=structuredClone(plan);short.scenarios.find(row=>row.id==='burst').durationMs=180000;assert.throws(()=>assertFormalPlan(short),/capacity_formal_targets_changed/);
});
