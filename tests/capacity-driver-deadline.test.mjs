import test from 'node:test';
import assert from 'node:assert/strict';
import {runOpenLoop} from '../tools/capacity/driver.mjs';
import {createCapacityMetrics} from '../tools/capacity/metrics.mjs';

test('early timer wakeups cannot dispatch before scheduled arrivals or truncate the observation interval',async()=>{
  let clock=0;const events=[],metrics=createCapacityMetrics(),result=await runOpenLoop({durationMs:1000,segments:[{startMs:0,endMs:1000,qps:10}],now:()=>clock,sleep:async ms=>{clock+=ms-0.1;},metrics,send:async event=>events.push(event)});
  assert.equal(events.length,10);assert.ok(events.every(event=>event.startedAt>=event.scheduledAt&&event.arrivalLagMs>=0));assert.ok(result.durationMs>=1000);assert.equal(metrics.view().arrivals.missed,0);assert.equal(metrics.view().arrivals.inflight,0);
});

test('cancellation during repeated deadline waits emits no future arrival and drains prior sends',async()=>{
  let clock=0,sleeps=0,released=false;const controller=new AbortController(),metrics=createCapacityMetrics(),events=[];
  await assert.rejects(runOpenLoop({durationMs:1000,segments:[{startMs:0,endMs:1000,qps:10}],now:()=>clock,sleep:async ms=>{clock+=ms-0.1;if(++sleeps===2)controller.abort(new Error('synthetic-cancel'));},signal:controller.signal,metrics,send:async event=>{events.push(event);await Promise.resolve();released=true;}}),/synthetic-cancel/);
  assert.equal(events.length,1);assert.equal(released,true);assert.equal(metrics.view().arrivals.inflight,0);
});

test('deadline overrun remains missed arrivals and never generates a catch-up burst',async()=>{
  let clock=0;const events=[],metrics=createCapacityMetrics();await runOpenLoop({durationMs:1000,segments:[{startMs:0,endMs:1000,qps:10}],now:()=>clock,sleep:async ms=>{clock+=ms+2000;},metrics,send:async event=>events.push(event)});assert.equal(events.length,1);assert.equal(metrics.view().arrivals.missed,9);assert.ok(metrics.view().violations.length>0);
});
