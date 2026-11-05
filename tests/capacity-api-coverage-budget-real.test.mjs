import test from 'node:test';
import assert from 'node:assert/strict';
import {rm} from 'node:fs/promises';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {runIndependentApiCoverage,getApiCoverageFailureReport} from '../tools/capacity/api-coverage.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('owned API coverage budget expiry is not reported as a user cancellation',{
 skip:!binDirectory?'需自有PG18与真实网关，不读取线上':false,timeout:120000
},async()=>{
 let target,artifact;
 try{
  target=await createTarget({profile:'selfcheck',employeeCount:2,binDirectory,libraryDirectory});
  let report;try{report=await runIndependentApiCoverage({target,maxDurationMs:5000,probeQps:5,batchSize:5});}catch(error){report=getApiCoverageFailureReport(error);assert.ok(report,'failure must retain its real phase report');}
  assert.ok(report.window,'the budget must reach an actual measurement phase');assert.equal(report.window.capReached,true);assert.equal(report.window.cancelled,false);assert.ok(report.interruptions.includes('api_coverage_duration_exhausted'));assert.notEqual(report.qualification.passed,true);
 }finally{
  if(target)try{await target.close();}catch(error){assert.equal(error.cleanupComplete,true,'budget expiry must still stop owned sources');artifact=error.recoveryDirectory;assert.ok(artifact?.startsWith('/tmp/harbor-capacity-stage-evidence-'));}
  if(artifact)await rm(artifact,{recursive:true,force:true});
 }
});
