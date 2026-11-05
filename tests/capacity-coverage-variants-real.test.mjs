import test from 'node:test';
import assert from 'node:assert/strict';
import {writeFile} from 'node:fs/promises';
import {createTarget,ownedCapacityTarget} from '../tools/capacity/gateway-adapter.mjs';
import {apiCoveragePlan,runIndependentApiCoverage,getApiCoverageFailureReport} from '../tools/capacity/api-coverage.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const real={skip:!binDirectory?'需要独立私有PG/mTLS与真实handler witness':false,timeout:240000};
const safe=result=>Object.fromEntries(['operation','method','variant','status','success','complete','httpEnded','wireSent','startedAt','finishedAt','durationMs','receivedBytes','firstContentAt','headersAt','connectedAt','tlsAt','reusedSocket','bodyHash','records','trace','error'].filter(key=>Object.hasOwn(result,key)).map(key=>[key,result[key]]));

test('expanded variants execute controller queries through real private factory handles and handler proof',real,async()=>{
 let target,prepareScope,measureScope;try{
  target=await createTarget({profile:'selfcheck',coverageVariants:'expanded',binDirectory,libraryDirectory,employeeCount:30});const owned=ownedCapacityTarget(target),inventory=await owned.getApiCoverageInventory({prepare:true}),plan=apiCoveragePlan({profile:'selfcheck',inventory,maxDurationMs:240000,probeQps:5,batchSize:4});
  assert.ok(plan.groups.some(row=>row.variant==='maximum'));assert.ok(plan.groups.some(row=>row.variant==='cold'));
  prepareScope=await owned.createObservationScope({id:'variants-real-prepare',kind:'normal'});measureScope=await owned.createObservationScope({id:'variants-real-measure',kind:'normal'});let sequence=0;
  for(const group of plan.groups.filter(row=>row.variant!=='steady')){
   const count=group.variant==='deep'?3:1;for(let index=0;index<count;index++){
    const mutableGroup={...group,purposes:[...group.purposes]},handles=await owned.prepareApiCoverageBatch({scope:prepareScope,group:mutableGroup,count:1});try{
     if(group.operationId==='request-list'&&group.variant==='default'){mutableGroup.variant='maximum';const before=target.eventCounts.byKind['gateway-request-start'];await assert.rejects(owned.executeApiCoverageHandle({scope:measureScope,handle:handles[0],group:mutableGroup,actorIndex:0}),/factory_handle_invalid/);assert.equal(target.eventCounts.byKind['gateway-request-start'],before);}
     const before=target.eventCounts.byKind['gateway-request-start'],result=await owned.executeApiCoverageHandle({scope:measureScope,handle:handles[0],group,actorIndex:sequence%5});assert.equal(result.success,true,group.operationId+'/'+group.variant+' HTTP '+result.status);assert.equal(result.businessVerified,true,group.operationId+'/'+group.variant+' business verification');assert.equal(result.variant,group.variant);assert.equal(target.eventCounts.byKind['gateway-request-start']-before,1,'one measured HTTP per handle');await measureScope.onResult({scenario:'expanded-variants-real',kind:'admin',event:{sequence,actorIndex:sequence%5},result:{...safe(result),validatedSuccess:true}});sequence++;
    }finally{await owned.releaseApiCoverageHandles({handles});}
   }
  }
  assert.ok(sequence>=40);const evidence=owned.getApiCoverageVariantEvidence();assert.ok(evidence.filter(row=>row.variant==='deep').every(row=>row.maxDepth>=2));assert.ok(evidence.filter(row=>row.variant==='cold').every(row=>row.databaseColdPassed===null&&row.operatingSystemColdPassed===null));assert.ok(evidence.every(row=>row.verified===row.measured));
 }finally{await prepareScope?.close();await measureScope?.close();await target?.close();}
});

test('complete expanded C retains the original mixed load and reports actual variant scope',{
 skip:!binDirectory||process.env.HARBOR_CAPACITY_C_FULL!=='1'?'仅显式运行完整expanded C阶段':false,timeout:660000
},async()=>{
 let target;try{
  target=await createTarget({profile:'selfcheck',coverageVariants:'expanded',binDirectory,libraryDirectory,employeeCount:60,maxLedgerDiskBytes:128*1024*1024});let report;
  try{report=await runIndependentApiCoverage({target,maxDurationMs:600000,probeQps:5,batchSize:5,onProgress:row=>writeFile('/tmp/harbor-c-expanded-progress.json',JSON.stringify({...row,observedAt:Date.now()}),{mode:0o600})});}catch(error){report=getApiCoverageFailureReport(error);throw error;}finally{if(report)await writeFile('/tmp/harbor-c-expanded-summary.json',JSON.stringify({qualification:report.qualification,variantEvidence:report.variantEvidence,background:report.background,groups:report.groups?.map(row=>({operationId:row.operationId,variant:row.variant,samples:row.samples,observedSamples:row.observedSamples,failed:row.failed,passed:row.passed,missing:row.missing,failures:row.failures})),scopes:report.scopes?.map(row=>({role:row.role,closed:row.closed,dataReadiness:row.dataReadiness,counts:row.evidence?.transport?.counts,mix:row.evidence?.transport?.mix})),arrivals:report.metrics?.arrivals,driverViolations:report.metrics?.violations,arrivalDiagnostics:report.metrics?.arrivalDiagnostics,recordingReasons:await target.store.all('SELECT recording_state,recording_reason,COUNT(*) n FROM harbor.requests GROUP BY recording_state,recording_reason'),runtime:(await target.gateway.command('status')).runtime.recording}),{mode:0o600});}
  assert.equal(report.qualification.passed,true,JSON.stringify(report.qualification));assert.equal(report.normalMixReplaced,false);assert.equal(report.plan.background.modelQps,10);assert.equal(report.plan.background.adminQps,5);assert.equal(report.plan.background.payloadBytes,102400);assert.deepEqual(report.plan.background.upstreamMs,[3000,6000]);assert.ok(report.groups.every(row=>row.samples===5&&row.passed===true));assert.ok(['default','maximum','deep','filtered-sorted','cold','steady'].every(variant=>report.groups.some(row=>row.variant===variant)));assert.ok(report.variantEvidence.filter(row=>row.variant==='deep').every(row=>row.maxDepth>=2));assert.ok(report.variantEvidence.filter(row=>row.variant==='cold').every(row=>row.databaseColdPassed===null&&row.operatingSystemColdPassed===null));assert.ok(report.scopes.every(row=>row.closed));assert.equal(target.employeeActors.length,60);assert.equal(ownedCapacityTarget(target).modelActorCount,60);
 }finally{await target?.close();}
});
