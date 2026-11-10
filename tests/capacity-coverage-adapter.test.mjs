import test from 'node:test';
import assert from 'node:assert/strict';
import {summarizeCapacityCoverage,capacityLiveWindow} from '../tools/capacity/coverage-adapter.mjs';
test('unknown live observations remain null rather than becoming zero-sized data',()=>{
  const valid={at:1,metadata7d:1260000,metadata30d:5400000,pricing_revision:'synthetic-v1'},unknown={at:2,verified:false};
  const result=summarizeCapacityCoverage([valid,unknown],{profile:'formal'});assert.equal(result.passed,null);assert.equal(result.minimumMetadata7d,null);assert.equal(result.minimumMetadata30d,null);assert.equal(result.observedMinimumMetadata7d,1260000);assert.equal(result.validSamples,1);assert.deepEqual(result.validSampleRange,{from:1,to:1});assert.deepEqual(result.unknownSampleRange,{from:2,to:2});
  const missing=summarizeCapacityCoverage([unknown],{profile:'formal'});assert.equal(missing.observedMinimumMetadata7d,null);assert.equal(missing.minimumMetadata30d,null);assert.equal(missing.passed,null);assert.equal(summarizeCapacityCoverage([]).minimumMetadata7d,null);
});
test('actual below-scale data and actual pricing revisions are failures while full live coverage remains unproven',()=>{
  const valid={at:1,metadata7d:1260000,metadata30d:5400000,pricing_revision:'synthetic-v1'};assert.equal(summarizeCapacityCoverage([valid],{profile:'formal'}).passed,null);assert.equal(summarizeCapacityCoverage([{...valid,metadata7d:1259999}],{profile:'formal'}).passed,false);assert.equal(summarizeCapacityCoverage([valid,{...valid,at:2,pricing_revision:'synthetic-v2'}],{profile:'formal'}).passed,false);
});

test('live 30-day coverage follows the same Beijing natural-day boundary as dataset verification',()=>{
  const before=capacityLiveWindow(Date.parse('2026-11-03T23:59:59.999+08:00')),after=capacityLiveWindow(Date.parse('2026-11-04T00:00:00.000+08:00'));
  assert.equal(before.metadata30dStart,before.start-23*86400000);assert.equal(after.metadata30dStart,after.start-23*86400000);assert.equal(after.metadata30dStart-before.metadata30dStart,86400000);const leaving=before.metadata30dStart+1000;assert.equal(leaving>=before.metadata30dStart,true);assert.equal(leaving>=after.metadata30dStart,false);assert.equal(after.metadata30dStart,Date.parse('2026-10-06T00:00:00.000+08:00'));
});

test('continuous baseline evidence covers snapshot gaps without hiding them or known failures',()=>{
 const sample={at:2,metadata7d:1260000,metadata30d:5400000,pricing_revision:'synthetic-v1'},proof={passed:true,protected:true,sealed:true,available:true,method:'monotone-seed-loss-lower-bound',from:1,through:3,metadata7d:1260000,metadata30d:5400000,pricingRevision:'synthetic-v1'};
 const summarize=(samples,continuousCoverage=proof,profile='formal')=>summarizeCapacityCoverage(samples,{profile,continuousCoverage});
 const result=summarize([sample,{at:3,verified:false}]);assert.equal(result.passed,true);assert.equal(result.unknownSamples,1);assert.equal(result.minimumMetadata7d,null);assert.deepEqual(result.missing,[]);
 for(const changed of [{passed:true},{...proof,from:3},{...proof,through:1},{...proof,protected:false},{...proof,metadata7d:1259999}])assert.equal(summarize([sample],changed).passed,null);
 assert.equal(summarize([sample],proof,'selfcheck').passed,null);assert.equal(summarize([]).passed,null);
 assert.equal(summarize([{...sample,metadata7d:1259999}]).passed,false);assert.equal(summarize([{...sample,pricing_revision:'synthetic-other'}]).passed,false);
 assert.equal(summarize([{...sample,at:4}]).passed,null,'old evidence does not extend its own observation interval');
});
