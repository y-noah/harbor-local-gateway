import test from 'node:test';
import assert from 'node:assert/strict';
import {registerHooks} from 'node:module';
import {createNormalExecution} from '../tools/capacity/normal-execution.mjs';
import {runCapacityExecution} from '../tools/capacity/execution.mjs';
import {runCapacitySuite} from '../tools/capacity/runner.mjs';
import {capacityPlan} from '../tools/capacity/scenarios.mjs';

test('formal cannot disable observation and an unsigned target cannot acquire normal scopes',async()=>{
  await assert.rejects(createNormalExecution({profile:'formal',enabled:false}),/capacity_normal_observation_required/);
  await assert.rejects(createNormalExecution({target:{},profile:'selfcheck'}),/capacity_target_not_owned/);
  const target={synthetic:true},qualification=await createNormalExecution({target,profile:'selfcheck',enabled:false});assert.strictEqual(qualification.target,target);await qualification.close();
});

test('all scene logins failing before scope creation retain missing counts rather than pass an empty reconciliation',async()=>{
  const entry=new URL('../tools/capacity/normal-execution.mjs?login-failure-case',import.meta.url).href,hooks=registerHooks({resolve(specifier,context,nextResolve){
    if(context.parentURL===entry&&specifier==='./gateway-adapter.mjs')return {url:'data:text/javascript,'+encodeURIComponent(`export function ownedCapacityTarget(){return {prepareActorEpoch:async()=>{throw Error('private-login-error');},configureScenario:()=>{},createObservationScope:()=>{throw Error('unexpected-scope');},collectBaseEvidence:async()=>({metricIntegrity:{passed:true}}),collectEvidenceForScope:()=>{throw Error('unexpected-scope-collection');},runQualificationProbes:async()=>({completed:true,passed:true,samples:1})};}`),shortCircuit:true};
    return nextResolve(specifier,context);
  }});
  try{
    const {createNormalExecution:create}=await import(entry),normal=await create({target:{employeeActors:[],adminActors:[]}}),plan={...capacityPlan('selfcheck'),scenarios:['first-login-failed','second-login-failed'].map(id=>({id,durationMs:20,modelQps:50,adminQps:50,employees:1,administrators:1}))};
    try{
      const report=await runCapacitySuite({plan,target:normal.target});assert.equal(report.verdict.status,'failed');assert.equal(report.evidence.dataReconciliation.passed,null);assert.equal(report.evidence.dataReconciliation.counts,null);assert.equal(report.evidence.normalObservation.passed,null);assert.equal(report.metrics.arrivals.sent,0);assert.equal(report.evidence.dataReconciliation.scenarios.length,0);assert.equal(report.evidence.scenarios['first-login-failed'].reason,'scenario_start_failed');assert.equal(JSON.stringify(report).includes('private-login-error'),false);
    }finally{await normal.close();}
  }finally{hooks.deregister();}
});

test('real per-scene scopes follow fresh login epochs and exclude independent qualification requests',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:180000},async()=>{
  const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target;
  try{
    target=await createTarget({binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB,employeeCount:1});
    const scenes=['normal-first','normal-second'].map(id=>({id,durationMs:200,modelQps:5,adminQps:5,employees:1,administrators:1,upstreamMs:[30,40],payloadBytes:1024}));
    const report=await runCapacityExecution({plan:{...target.plan,scenarios:scenes},target,preflight:target.preflight});
    assert.equal(report.evidence.qualificationProbes.passed,true);assert.equal(report.evidence.dataReconciliation.passed,true);assert.equal(report.evidence.dataReconciliation.counts.accepted,2);assert.equal(report.evidence.dataReconciliation.counts.persisted,2);assert.equal(report.evidence.actorCoverage.passed,true);assert.equal(report.evidence.metricIntegrity.passed,true);assert.equal(report.evidence.serverArrivals.passed,true);
    assert.equal(report.evidence.normalObservation.scenarios.length,2);assert.equal(new Set(report.evidence.normalObservation.scenarios.map(row=>row.epoch)).size,2);assert.equal(report.evidence.normalObservation.qualificationOutsideNormalScopes,true);assert.equal(report.evidence.dataReadiness.passed,true);assert.equal(report.evidence.dataReadiness.provesFormalTimeliness,false);assert.equal(report.evidence.dataReadiness.scenarios.length,2);for(const row of report.evidence.dataReadiness.scenarios){assert.equal(row.counts.ready,1);assert.equal(row.counts.pending,0);}
    for(const row of report.evidence.serverArrivals.scenarios){assert.equal(row.counts.planned,2);assert.equal(row.counts.wireStarted,2);assert.equal(row.counts.reconciled,2);assert.equal(row.counts.unattributed,0);assert.equal(row.mix.alignedToPlannedCounts,true);assert.equal(row.mix.plannedAdminCalls,1);}
    assert.equal(report.evidence.streamingStages.scenarios.length,2);for(const row of report.evidence.streamingStages.scenarios){assert.equal(row.evidence.counts.started,1);assert.equal(row.evidence.counts.finished,1);assert.equal(row.evidence.runtime.active,0);assert.equal(row.evidence.runtime.hardFailures,0);assert.equal(row.evidence.metrics.bodyVisibility.upper.samples,1);assert.equal(row.evidence.metrics.usageVisibility.upper.samples,1);}assert.equal(report.evidence.streamingStages.passed,null,'loopback cannot replace formal helper topology');
    assert.notEqual(report.verdict.status,'passed');
  }finally{await target?.close();}
});

for(const readiness of [true,false,null,'cancelled'])test('normal scene drains readiness before closing and preserves '+readiness+' evidence',async()=>{
 const key='__harborNormalDrain'+Math.random().toString(16).slice(2),entry=new URL('../tools/capacity/normal-execution.mjs?'+key,import.meta.url).href,order=[];let release;
 const gate=new Promise(resolve=>release=resolve),scope={sendModel(){},sendAdmin(){},onResult(){},waitForDataReadiness:async({signal})=>{order.push('readiness');await gate;if(readiness==='cancelled')signal.throwIfAborted();return {passed:readiness};},close:async()=>{order.push('close');},collect:async()=>({dataReconciliation:{passed:true,counts:{accepted:1,dispatched:1,persisted:1}},transport:{passed:true,mix:{alignedToPlannedCounts:true}},dataReadiness:readiness==='cancelled'?null:{passed:readiness}})};
 globalThis[key]={prepareActorEpoch:async()=>{},configureScenario:()=>{},createObservationScope:async()=>scope,collectBaseEvidence:async()=>({metricIntegrity:{passed:true}}),collectEvidenceForScope:async()=>({scopeEvidence:await scope.collect()}),runQualificationProbes:async()=>{order.push('qualification');return {};}};
 const hooks=registerHooks({resolve(specifier,context,nextResolve){if(context.parentURL===entry&&specifier==='./gateway-adapter.mjs')return {url:'data:text/javascript,'+encodeURIComponent('export function ownedCapacityTarget(){return globalThis['+JSON.stringify(key)+'];}'),shortCircuit:true};if(context.parentURL===entry&&specifier==='./observation-adapter.mjs')return {url:'data:text/javascript,export const isOwnedObservationScope=()=>true;',shortCircuit:true};return nextResolve(specifier,context);}});let normal;
 try{
  const {createNormalExecution:create}=await import(entry);normal=await create({target:{employeeActors:[],adminActors:[]}});await normal.target.onScenarioStart({scene:{id:'drain'}});const abort=new AbortController(),end=normal.target.onScenarioEnd({signal:abort.signal});assert.deepEqual(order,['readiness']);if(readiness==='cancelled')abort.abort();release();if(readiness==='cancelled')await assert.rejects(end,{name:'AbortError'});else await end;assert.deepEqual(order,['readiness','close']);await normal.target.runQualificationProbes();assert.deepEqual(order,['readiness','close','qualification']);const evidence=await normal.target.collectEvidence();assert.equal(evidence.dataReadiness.passed,readiness==='cancelled'?null:readiness);assert.equal(evidence.normalObservation.passed,readiness==='cancelled'?null:readiness);assert.equal(evidence.dataReadiness.provesFormalTimeliness,false);
 }finally{release();await normal?.close();hooks.deregister();delete globalThis[key];}
});
