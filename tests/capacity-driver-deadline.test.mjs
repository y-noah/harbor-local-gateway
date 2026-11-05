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

test('missed slots retain native deadlines and fixed reasons without changing sent numbering across segments',async()=>{
 let clock=0,sleeps=0;const events=[],metrics=createCapacityMetrics();await runOpenLoop({durationMs:800,segments:[{startMs:0,endMs:400,qps:10},{startMs:400,endMs:800,qps:5}],scenario:'raw-path-credential-must-not-appear',diagnosticScenario:'api-coverage-model',now:()=>clock,sleep:async ms=>{clock+=ms;if(++sleeps===1)clock+=150;},metrics,send:async event=>events.push(event)});
 const view=metrics.view();assert.deepEqual(view.arrivals,{planned:6,sent:5,offered:5,wireSent:0,httpEnded:0,unknownTransport:0,missed:1,inflight:0,peak:5});assert.deepEqual(events.map(row=>row.sequence),[0,1,2,3,4]);assert.deepEqual(view.arrivalDiagnostics.missed,[{scenario:'api-coverage-model',slotSequence:1,clock:'controller-performance-now',scheduledAt:100,actualAt:250,lagMs:150,lagLimitMs:100,reason:'arrival_lag_exceeded'}]);assert.deepEqual(view.violations,[{code:'capacity_arrival_late',detail:view.arrivalDiagnostics.missed[0]}]);assert.equal(JSON.stringify(view).includes('raw-path-credential'),false);
});
test('exhausted duration and inflight limit preserve their original counters and do not add violations',async()=>{
 let clock=0,release;const held=new Promise(resolve=>{release=resolve;}),metrics=createCapacityMetrics();const running=runOpenLoop({durationMs:500,segments:[{startMs:0,endMs:500,qps:10}],maxInFlight:1,diagnosticScenario:'api-coverage-admin',now:()=>clock,sleep:async ms=>{clock+=ms;},metrics,send:()=>held});
 for(let turn=0;turn<30&&metrics.view().arrivals.planned<5;turn++)await Promise.resolve();assert.equal(metrics.view().arrivals.sent,1);assert.equal(metrics.view().arrivals.missed,4);assert.deepEqual(metrics.view().violations,[]);assert.ok(metrics.view().arrivalDiagnostics.missed.every(row=>row.reason==='inflight_limit'));release();await running;
 clock=0;const expired=createCapacityMetrics();await runOpenLoop({durationMs:300,segments:[{startMs:0,endMs:300,qps:10}],metrics:expired,now:()=>clock,sleep:async ms=>{clock+=ms+1000;},send:async()=>{}});assert.equal(expired.view().arrivals.missed,2);assert.ok(expired.view().arrivalDiagnostics.missed.every(row=>row.reason==='duration_exhausted'));assert.equal(expired.view().violations.length,2);
});
test('miss evidence remains bounded and sanitizes labels without weakening original dropped or missed counts',async()=>{
 let clock=0;const metrics=createCapacityMetrics();await runOpenLoop({durationMs:200000,segments:[{startMs:0,endMs:200000,qps:10}],diagnosticScenario:'secret-path-do-not-store',now:()=>clock,sleep:async ms=>{clock+=ms+300000;},metrics,send:async()=>{}});const view=metrics.view();assert.equal(view.arrivals.missed,1999);assert.equal(view.arrivalDiagnostics.limit,1000);assert.equal(view.arrivalDiagnostics.retained,1000);assert.equal(view.arrivalDiagnostics.dropped,999);assert.equal(view.arrivalDiagnostics.truncated,true);assert.equal(view.violations.length,1000);assert.equal(view.dropped,999);assert.ok(view.arrivalDiagnostics.missed.every(row=>row.scenario==='other'));assert.equal(JSON.stringify(view).includes('secret-path'),false);
});
test('a real short controller block records actual missed time and never catches up expired slots',async()=>{
 const {performance}=await import('node:perf_hooks'),metrics=createCapacityMetrics(),sent=[];let blocked=false;await runOpenLoop({durationMs:250,segments:[{startMs:0,endMs:250,qps:20}],diagnosticScenario:'api-coverage-probe',metrics,send:async event=>{sent.push(event);if(!blocked){blocked=true;const end=performance.now()+120;while(performance.now()<end){/* Deliberate local-only controller CPU stall. */}}}});const view=metrics.view();assert.equal(view.arrivals.planned,5);assert.ok(view.arrivals.missed>=1);assert.equal(view.arrivals.sent+view.arrivals.missed,5);assert.ok(view.arrivalDiagnostics.missed.every(row=>row.actualAt>=row.scheduledAt&&row.lagMs===row.actualAt-row.scheduledAt));assert.ok(view.arrivalDiagnostics.missed.some(row=>row.reason==='arrival_lag_exceeded'||row.reason==='duration_exhausted'));assert.equal(sent.length,view.arrivals.sent);assert.equal(view.arrivals.inflight,0);
});

test('persisted driver violations cannot include free-form exception codes or diagnostic fields',async()=>{
 let clock=0;const metrics=createCapacityMetrics();await runOpenLoop({durationMs:100,segments:[{startMs:0,endMs:100,qps:10}],metrics,now:()=>clock,sleep:async ms=>{clock+=ms;},send:async()=>{throw Object.assign(Error('private-customer-body'),{code:'private-path-and-credential'});}});assert.deepEqual(metrics.view().violations,[{code:'capacity_driver_request_failed',detail:{code:'driver_error'}}]);
 metrics.arrival({dispatched:false,diagnostic:{scenario:'private-path-and-credential',slotSequence:1,scheduledAt:100,actualAt:200,lagMs:100,lagLimitMs:50,reason:'arrival_lag_exceeded',body:'private-customer-body',token:'private-path-and-credential'}});const encoded=JSON.stringify(metrics.view());assert.equal(encoded.includes('private-path-and-credential'),false);assert.equal(encoded.includes('private-customer-body'),false);assert.equal(metrics.view().arrivalDiagnostics.missed[0].scenario,'other');
});

test('a real short HTTP window reconciles actual arrivals with reported misses after a controller stall',async()=>{
 const http=await import('node:http'),{performance}=await import('node:perf_hooks'),{requestCapacityTarget,capacityAgent}=await import('../tools/capacity/http-client.mjs');let received=0,blocked=false;const server=http.createServer((_request,response)=>{received++;response.setHeader('content-type','application/json');response.end('{"ok":true}');});let agent;
 try{await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});const origin='http://127.0.0.1:'+server.address().port;agent=capacityAgent(origin);const metrics=createCapacityMetrics(),results=[];await runOpenLoop({durationMs:300,segments:[{startMs:0,endMs:300,qps:20}],diagnosticScenario:'api-coverage-probe',metrics,send:async()=>{results.push(await requestCapacityTarget({origin,path:'/health',agent,timeoutMs:1000}));if(!blocked){blocked=true;const until=performance.now()+130;while(performance.now()<until){/* Synthetic controller stall, never a service mutation. */}}}});const view=metrics.view();assert.equal(view.arrivals.planned,6);assert.ok(view.arrivals.missed>=1);assert.equal(view.arrivals.sent+view.arrivals.missed,6);assert.equal(received,view.arrivals.sent);assert.equal(results.length,received);assert.ok(results.every(row=>row.success));assert.ok(view.arrivalDiagnostics.missed.every(row=>row.lagMs===row.actualAt-row.scheduledAt));assert.equal(view.arrivals.inflight,0);
 }finally{agent?.destroy();await new Promise(resolve=>server.close(resolve));}
});
