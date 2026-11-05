import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {writeFile} from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
import {createTarget,ownedCapacityTarget} from '../tools/capacity/gateway-adapter.mjs';
import {createCapacityInventoryAdapter} from '../tools/capacity/inventory-adapter.mjs';
import {createNormalExecution} from '../tools/capacity/normal-execution.mjs';
import {runCapacitySuite} from '../tools/capacity/runner.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,real={skip:!binDirectory?'需真实私有PG、gateway、worker；不将隔离接口资格冒作正式容量':false,timeout:180000};
const starts=target=>{const count=target.eventCounts?.byKind?.['gateway-request-start'];assert.ok(Number.isSafeInteger(count),'需factory完整事件标量而非诊断ring');return count;};
const options=(target,store=target.store)=>({origin:target.gateway.ready.tlsOrigin,agent:target.employeeActors[0].agent,adminActors:target.adminActors,employeeActors:target.employeeActors,store});

test('actual normal 10 model / 5 admin QPS uses one HTTP per slot with the unchanged four-way mix',real,async()=>{
 let target,normal;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:2});
  const all=await ownedCapacityTarget(target).getApiCoverageInventory();
  const state=all.filter(row=>row.canonicalHttp==='GET /api/admin/state');assert.ok(state.length>=5);assert.ok(state.every(row=>row.workloadClass==='summary'&&row.apiBudget.p95===500&&row.apiBudget.p99===1000));
  assert.equal(all.find(row=>row.operationId==='key-detail').workloadClass,'list');assert.equal(all.find(row=>row.operationId==='device-detail').workloadClass,'list');assert.equal(all.find(row=>row.operationId==='type-default-read').workloadClass,'configuration');
  const actorsBefore=await target.store.all('SELECT id,name,enabled,usage_limits_version FROM harbor.members WHERE id=ANY($1::text[]) ORDER BY id',target.employeeActors.map(row=>row.memberId)),devicesBefore=await target.store.all('SELECT id,name,state,version FROM harbor.employee_devices WHERE id=ANY($1::text[]) ORDER BY id',target.employeeActors.map(row=>row.deviceId));
  normal=await createNormalExecution({target});const scene={id:'actual-normal-mix',durationMs:20000,modelQps:10,adminQps:5,employees:2,administrators:5,upstreamMs:[100,200],payloadBytes:1024};
  const report=await runCapacitySuite({plan:{...target.plan,scenarios:[scene]},preflight:target.preflight,target:normal.target});
  const wire=report.evidence.serverArrivals.scenarios[0];if(wire.mix.alignedToPlannedCounts!==true||report.evidence.dataReconciliation.passed!==true)await writeFile('/tmp/harbor-a-normal-mix-regression.json',JSON.stringify({mix:wire.mix,counts:wire.counts,pending:wire.pending,pendingInvocations:wire.pendingInvocations,data:report.evidence.dataReconciliation,normal:report.evidence.normalObservation}),{mode:0o600});assert.equal(wire.mix.alignedToPlannedCounts,true);assert.equal(wire.mix.plannedAdminCalls,100);assert.equal(wire.mix.actualBaselineHttpCalls,100);assert.equal(wire.mix.auxiliaryBaselineHttp,0);assert.deepEqual(wire.mix.actualBaselineByClass,{list:50,detail:20,summary:20,configuration:10});
  const actualReads=report.metrics.series.filter(row=>row.group==='normal'&&row.category==='read'),canonicalSamples=new Map();for(const row of actualReads){const binding=all.find(item=>item.operationId===row.operation);assert.ok(binding,row.operation);assert.ok(!canonicalSamples.has(binding.canonicalHttp)||canonicalSamples.get(binding.canonicalHttp)===row.operation,'只读alias不得重复造独立主池入口');canonicalSamples.set(binding.canonicalHttp,row.operation);}assert.ok(report.metrics.series.filter(row=>row.group==='normal'&&row.category==='write').every(row=>all.find(item=>item.operationId===row.operation)?.method!=='GET'));
  assert.equal(report.evidence.dataReconciliation.passed,true);assert.ok(report.metrics.series.filter(row=>row.category==='model').every(row=>row.successRate===1));assert.equal(report.evidence.qualificationProbes.passed,true);
  assert.deepEqual(await target.store.all('SELECT id,name,enabled,usage_limits_version FROM harbor.members WHERE id=ANY($1::text[]) ORDER BY id',target.employeeActors.map(row=>row.memberId)),actorsBefore);assert.deepEqual(await target.store.all('SELECT id,name,state,version FROM harbor.employee_devices WHERE id=ANY($1::text[]) ORDER BY id',target.employeeActors.map(row=>row.deviceId)),devicesBefore);
 }finally{await normal?.close();await target?.close();}
});

test('configuration fixture exhaustion fails without extra HTTP or hidden queue and excludes DB checks from API duration',real,async()=>{
 let target,release;try{
  target=await createTarget({binDirectory,libraryDirectory,profile:'selfcheck',employeeCount:1});let enteredResolve;const entered=new Promise(resolve=>enteredResolve=resolve),gate=new Promise(resolve=>release=resolve);let blocking=true;
  const store=new Proxy(target.store,{get(object,key){if(key==='get')return async(sql,...values)=>{if(sql==='SELECT name FROM harbor.members WHERE id=$1'){if(blocking){enteredResolve();await gate;}await delay(80);}return object.get(sql,...values);};const value=Reflect.get(object,key);return typeof value==='function'?value.bind(object):value;}});
  const inventory=await createCapacityInventoryAdapter({...options(target,store),configurationTargetCount:1});const before=starts(target),started=performance.now(),writing=inventory.execute('employee-save');await entered;
  await assert.rejects(inventory.execute('employee-save'),/capacity_configuration_pool_busy/);assert.equal(starts(target)-before,1);blocking=false;release();const result=await writing;assert.equal(result.businessVerified,true);assert.ok(performance.now()-started-result.durationMs>=70);
  assert.equal((await inventory.execute('employee-save')).businessVerified,true);assert.equal(inventory.fixture.configurationTargets.length,1);assert.ok(!target.employeeActors.some(actor=>actor.memberId===inventory.fixture.configurationTargets[0].memberId));
 }finally{release?.();await target?.close();}
});
