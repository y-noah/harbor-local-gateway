import test from 'node:test';
import assert from 'node:assert/strict';
import {datasetManifest,fixtureTimestamp} from '../tools/capacity/dataset.mjs';
import {capacityTargetDatasetManifest,capacitySeedWindowCounts} from '../tools/capacity/dataset-coverage-plan.mjs';
import {capacityLiveWindow} from '../tools/capacity/coverage-adapter.mjs';
import {capacityBodySourceBytes,observeCapacityStorageBudget} from '../tools/capacity/storage-budget.mjs';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';

const anchor=Date.parse('2026-11-10T08:00:00+08:00'),hour=3600000;
test('binary window counts match every actual generated row at natural-day boundaries',()=>{
 const manifest=datasetManifest({profile:'selfcheck',anchor});
 for(let offset=0;offset<=48;offset++){
  const at=anchor+offset*hour,window=capacityLiveWindow(at),rows=Array.from({length:manifest.metadata30d},(_,index)=>fixtureTimestamp(manifest,index));
  assert.deepEqual(capacitySeedWindowCounts(manifest,at),{metadata7d:rows.filter(at=>at>=window.start&&at<window.end).length,metadata30d:rows.filter(at=>at>=window.metadata30dStart&&at<window.end).length});
 }
});
test('formal lifetime reserve survives each visible window without inventing future requests',()=>{
 const original=datasetManifest({profile:'formal',anchor}),expired=capacitySeedWindowCounts(original,anchor+24*hour);
 assert.ok(expired.metadata7d<1260000&&expired.metadata30d<5400000);
 for(const hours of [1,16,24,48,120]){
  const manifest=capacityTargetDatasetManifest({profile:'formal',anchor,runLifetimeMs:hours*hour});
  assert.equal(manifest.bodyRequests,manifest.metadata30d);assert.equal(manifest.bodyRequests%100,0);
  assert.deepEqual(datasetManifest(manifest),manifest);
  for(let offset=0;offset<=hours;offset++){
   const counts=capacitySeedWindowCounts(manifest,anchor+offset*hour);assert.ok(counts.metadata7d>=1260000);assert.ok(counts.metadata30d>=5400000);
  }
  assert.ok(fixtureTimestamp(manifest,manifest.metadata7d-1)<=anchor);
  assert.ok(fixtureTimestamp(manifest,manifest.metadata30d-1)<=anchor);
 }
});
test('selfcheck and separate seed/verify commands retain their corpus and impossible horizons reject',()=>{
 assert.equal(capacityTargetDatasetManifest({profile:'selfcheck',anchor,runLifetimeMs:24*hour}).metadata30d,120);
 for(const purpose of ['seed','verify'])assert.equal(capacityTargetDatasetManifest({profile:'formal',purpose,anchor,runLifetimeMs:24*hour}).metadata30d,5400000);
 assert.equal(capacityTargetDatasetManifest({profile:'formal',anchor}).metadata30d,5400000);
 assert.throws(()=>capacityTargetDatasetManifest({profile:'formal',anchor,runLifetimeMs:7*24*hour}),/coverage_horizon_unavailable/);
 assert.throws(()=>capacityTargetDatasetManifest({profile:'formal',anchor,runLifetimeMs:0}),/run_lifetime_invalid/);
 assert.throws(()=>capacitySeedWindowCounts(datasetManifest({profile:'selfcheck'}),NaN),/window_invalid/);
});
test('a rejected formal horizon releases the lifetime timer before allocating runtime resources',async t=>{
 const pending=new Set(),set=globalThis.setTimeout,clear=globalThis.clearTimeout;
 t.mock.method(globalThis,'setTimeout',(...args)=>{const handle=set(...args);pending.add(handle);return handle;});
 t.mock.method(globalThis,'clearTimeout',handle=>{pending.delete(handle);return clear(handle);});
 try{await assert.rejects(createTarget({profile:'formal',runLifetimeMs:7*24*hour}),/coverage_horizon_unavailable/);assert.equal(pending.size,0);}
 finally{for(const handle of pending)clear(handle);t.mock.restoreAll();}
});
test('extra historical bodies are charged to the real pre-seed disk budget',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-coverage-plan-'));
 try{
  const original=datasetManifest({profile:'formal',anchor}),manifest=capacityTargetDatasetManifest({profile:'formal',anchor,runLifetimeMs:24*hour});
  assert.ok(capacityBodySourceBytes(manifest)>capacityBodySourceBytes(original));
  const budget={dataBytes:Number(capacityBodySourceBytes(original)),metadataBytes:1,indexBytes:1,walBytes:1,backupBytes:1,temporaryBytes:1,growthBytes:1};
  const observed=await observeCapacityStorageBudget({storageRoot:root,manifest,budget});assert.equal(observed.passed,null);assert.ok(observed.missing.includes('data_budget_below_known_source_bytes'));
 }finally{await rm(root,{recursive:true,force:true});}
});
