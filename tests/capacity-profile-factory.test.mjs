import test from 'node:test';
import assert from 'node:assert/strict';
import {createTarget,ownedCapacityTarget} from '../tools/capacity/gateway-adapter.mjs';
import {createNormalExecution} from '../tools/capacity/normal-execution.mjs';
import {runIndependentApiCoverage} from '../tools/capacity/api-coverage.mjs';
import {runIndependentUiAcceptance} from '../tools/capacity/ui-acceptance.mjs';

test('real factory profile is bound before normal, API or UI traffic and public plan changes cannot promote it',{
  skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:180000
},async()=>{
  const target=await createTarget({profile:'selfcheck',employeeCount:1,binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});
  try{
    assert.ok(ownedCapacityTarget(target,{profile:'selfcheck'}));
    const before=target.eventCounts.byKind['gateway-request-start']??0,dispatchesBefore=target.eventCounts.byKind['upstream-dispatch']??0;
    assert.throws(()=>ownedCapacityTarget(target,{profile:'formal'}),/capacity_profile_mismatch/);
    await assert.rejects(createNormalExecution({target,profile:'formal'}),/capacity_profile_mismatch/);
    await assert.rejects(runIndependentApiCoverage({target,profile:'formal',maxDurationMs:40000000}),/capacity_profile_mismatch/);
    await assert.rejects(runIndependentUiAcceptance({target,profile:'formal',maxDurationMs:40000000}),/capacity_profile_mismatch/);
    const originalPlan=target.plan;
    try{
      target.plan={...originalPlan,profile:'formal'};
      await assert.rejects(runIndependentApiCoverage({target,maxDurationMs:40000000}),/capacity_profile_mismatch/);
      await assert.rejects(runIndependentUiAcceptance({target,maxDurationMs:40000000}),/capacity_profile_mismatch/);
    }finally{target.plan=originalPlan;}
    assert.equal(target.eventCounts.byKind['gateway-request-start']??0,before);
    assert.equal(target.eventCounts.byKind['upstream-dispatch']??0,dispatchesBefore);
    const normal=await createNormalExecution({target,profile:'selfcheck'});await normal.close();
  }finally{await target.close();}
  assert.throws(()=>ownedCapacityTarget(target,{profile:'selfcheck'}),/capacity_target_not_owned/);
});
