import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {createControlledUpstream} from '../tools/capacity/controlled-upstream.mjs';
import {registerHooks} from 'node:module';
import {capacityPlan} from '../tools/capacity/scenarios.mjs';
import {credentialFingerprints} from '../tools/capacity/http-client.mjs';

test('scene timings apply to new requests while an existing stream retains its captured duration',async()=>{
  const events=[],upstream=await createControlledUpstream({durationMs:180,firstContentMs:5,chunkIntervalMs:10,sink:row=>events.push(row)});
  try{
    const first=fetch(upstream.origin+'/responses',{method:'POST',headers:{'x-capacity-trace':'first'},body:'{}'}).then(response=>response.text());
    for(let index=0;index<100&&!events.some(row=>row.kind==='upstream-dispatch');index++)await delay(5);
    assert.ok(events.some(row=>row.kind==='upstream-dispatch'));
    upstream.setScenario({durationMs:30,firstContentMs:5,chunkIntervalMs:5});
    const next=await fetch(upstream.origin+'/responses',{method:'POST',headers:{'x-capacity-trace':'next'},body:'{}'});assert.match(await next.text(),/response.completed/);
    assert.equal(events.some(row=>row.kind==='upstream-complete'&&row.trace==='first'),false);
    assert.match(await first,/response.completed/);
    for(const [trace,duration] of [['first',180],['next',30]])assert.equal(events.find(row=>row.kind==='upstream-dispatch'&&row.trace===trace).durationMs,duration);
    assert.throws(()=>upstream.setScenario({durationMs:1,firstContentMs:2,chunkIntervalMs:1}),/scenario_invalid/);
  }finally{await upstream.close();}
});

function fixture(){
  const calls=[],cookie='capacity_admin=test-cookie',secret='synthetic';
  const result={success:true,complete:true,httpEnded:true,wireSent:true,durationMs:1,startedAt:0,firstContentAt:1};
  return {calls,target:{employeeActors:[{memberId:'m',keyId:'k',deviceId:'d',secret}],adminActors:[{headers:{cookie}}],sendModel:async()=>{calls.push('model');return {...result,operation:'model',method:'POST',credentials:credentialFingerprints({authorization:'Bearer '+secret})};},sendAdmin:async({category})=>{calls.push(category);return {...result,operation:'health',method:'GET',requestSignature:'GET /health',credentials:credentialFingerprints({cookie})};}}};
}
function plan(){return {...capacityPlan('selfcheck'),scenarios:[{id:'hooks',durationMs:100,modelQps:10,adminQps:100,employees:1,administrators:1}]};}

// These assertions cover hook ordering and category counts, not wall-clock
// scheduling. Keep the real driver and give each loop its own controlled clock.
async function runHookSuite(options){
  const entry=new URL('../tools/capacity/runner.mjs?deterministic-hook-fixture',import.meta.url).href,driver=new URL('../tools/capacity/driver.mjs',import.meta.url).href;
  const hooks=registerHooks({resolve(specifier,context,nextResolve){
    if(context.parentURL===entry&&specifier==='./driver.mjs')return {url:'data:text/javascript,'+encodeURIComponent('import {runOpenLoop as real,runConcurrent} from '+JSON.stringify(driver)+';export {runConcurrent};export function runOpenLoop(options){let time=0;return real({...options,now:()=>time,sleep:async ms=>{time+=ms;}});}'),shortCircuit:true};return nextResolve(specifier,context);
  }});
  try{return await (await import(entry)).runCapacitySuite(options);}finally{hooks.deregister();}
}

test('scene hooks surround traffic and separate probes cannot change the main 50/20/20/10 mix or measurement counts',async()=>{
  const {calls,target}=fixture();target.onScenarioStart=async()=>calls.push('start');target.onScenarioEnd=async()=>calls.push('end');target.runQualificationProbes=async()=>{calls.push('probe');return {completed:true,passed:true,samples:10000,secret:'do-not-report'};};
  target.collectEvidence=async()=>({qualificationProbes:{passed:true,samples:999999}});
  const report=await runHookSuite({plan:plan(),target});
  assert.equal(calls[0],'start');assert.deepEqual(calls.slice(-2),['end','probe']);
  assert.deepEqual(calls.filter(value=>['list','detail','summary','configuration'].includes(value)),['list','list','list','list','list','detail','detail','summary','summary','configuration']);
  assert.equal(report.metrics.arrivals.sent,11);assert.deepEqual(report.evidence.qualificationProbes,{completed:true,passed:true,samples:10000,countsTowardMainMix:false});assert.equal(JSON.stringify(report).includes('do-not-report'),false);
});

test('a failed scene setup emits no traffic and a failed cleanup cannot claim a completed scene',async()=>{
  const setup=fixture();setup.target.onScenarioStart=async()=>{throw new Error('secret');};const first=await runHookSuite({plan:plan(),target:setup.target});assert.deepEqual(setup.calls,[]);assert.equal(first.evidence.scenarios.hooks.reason,'scenario_start_failed');assert.equal(JSON.stringify(first).includes('secret'),false);
  const cleanup=fixture();cleanup.target.onScenarioEnd=async()=>{throw new Error('secret');};const second=await runHookSuite({plan:plan(),target:cleanup.target});assert.equal(second.evidence.scenarios.hooks.completed,false);assert.ok(second.metrics.violations.some(row=>row.code==='scenario_operation_failed'));
});

test('failed and incomplete qualification probes block a passing verdict without adding samples',async()=>{
  for(const outcome of ['false','missing','throw']){
    const {target}=fixture();target.runQualificationProbes=async()=>{if(outcome==='throw')throw new Error('secret');return outcome==='false'?{completed:true,passed:false,samples:1}:{completed:false,samples:0};};
    const report=await runHookSuite({plan:plan(),target});assert.notEqual(report.verdict.status,'passed');assert.equal(report.metrics.arrivals.sent,11);
    if(outcome==='false')assert.ok(report.verdict.failures.includes('qualification_probes_failed'));else assert.ok(report.verdict.incomplete.includes('独立资格检查未完整通过'));
  }
});

test('Windows identity removal failure preserves state and permits a precise cleanup retry',async()=>{
  const {createCapacityIdentityCleanup}=await import('../tools/capacity/device-adapter.mjs');let stopped=0,removed=0,directories=0;const close=createCapacityIdentityCleanup({stop:async()=>{stopped++;},removeIdentity:async()=>{if(++removed===1)throw new Error('synthetic CNG failure');},removeDirectory:async()=>{directories++;}});
  await assert.rejects(close(),/synthetic/);assert.equal(directories,0);await close();await close();assert.equal(stopped,1);assert.equal(removed,2);assert.equal(directories,1);
});

test('task completion uses each actual submission deadline and rejects slow terminal responses',async()=>{
  const {pollCapacityTask}=await import('../tools/capacity/inventory-adapter.mjs');let clock=0,budget;
  await assert.rejects(pollCapacityTask({submittedAt:0,now:()=>clock,readStatus:async options=>{budget=options.timeoutMs;clock=10001;return {success:true,json:{status:'complete'}};},isTerminal:value=>value.status==='complete'}),/result_timeout/);assert.equal(budget,10000);
  clock=8000;await assert.rejects(pollCapacityTask({submittedAt:0,now:()=>clock,readStatus:async options=>{assert.equal(options.timeoutMs,2000);clock+=3000;return {success:true,json:{status:'complete'}};},isTerminal:value=>value.status==='complete'}),/result_timeout/);
  clock=12000;let called=false;await assert.rejects(pollCapacityTask({submittedAt:0,now:()=>clock,readStatus:async()=>{called=true;},isTerminal:()=>true}),/result_timeout/);assert.equal(called,false);
  clock=0;let attempts=0;const result=await pollCapacityTask({submittedAt:0,now:()=>clock,sleep:async ms=>{clock+=ms;},readStatus:async()=>({success:true,json:{complete:++attempts===2}}),isTerminal:value=>value.complete});assert.equal(result.complete,true);assert.equal(clock,20);
});
