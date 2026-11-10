import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,readdir,rm,symlink,chmod} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {capacityStorageDirectory} from '../tools/capacity/storage-location.mjs';
import {capacityBodySourceBytes,capacityContextSourceBytes,observeCapacityStorageBudget} from '../tools/capacity/storage-budget.mjs';
import {capacityHistoricalContext} from '../tools/capacity/context-payload.mjs';
import {datasetManifest} from '../tools/capacity/dataset.mjs';

test('body footprint follows the actual generator periods and distribution without iterating formal rows',()=>{
 for(const bodyRequests of [1,32,99,100,101,203]){const manifest=datasetManifest({metadata30d:300,metadata7d:30,bodyRequests}),bins=manifest.bodyDistribution;let expected=0n;for(let index=0;index<bodyRequests;index++){let upper=0;for(const bin of bins){upper+=bin.percent;if(index*37%100<upper){expected+=BigInt(bin.bytes);break;}}}assert.equal(capacityBodySourceBytes(manifest),expected);}
 const formal=datasetManifest({profile:'formal'});assert.equal(capacityBodySourceBytes(formal),186679296000n);assert.throws(()=>capacityBodySourceBytes({...formal,bodyRequests:1}),/manifest_invalid/);
});

test('selected storage observes a real owner directory and preserves preexisting materials',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-storage-budget-'));try{
  await writeFile(join(root,'other-task'),'preserve');const manifest=datasetManifest({profile:'selfcheck'}),dataBytes=Number(capacityBodySourceBytes(manifest)),budget={dataBytes,metadataBytes:1,indexBytes:1,walBytes:1,backupBytes:1,temporaryBytes:1,growthBytes:1};
  const report=await observeCapacityStorageBudget({storageRoot:root,manifest,budget});assert.equal(report.passed,true);assert.equal(report.knownBodySourceBytes,String(dataBytes));assert.equal(report.requiredBytes,String(dataBytes+6));assert.equal(report.planning,'explicit_components_not_a_full_scale_measurement');assert.deepEqual(await readdir(root),['other-task']);
  const insufficient=await observeCapacityStorageBudget({storageRoot:root,manifest,budget:{...budget,walBytes:Number.MAX_SAFE_INTEGER}});assert.equal(insufficient.passed,null);assert.ok(insufficient.missing.includes('storage_free_below_declared_budget'));
  for(const override of [{dataBytes:dataBytes-1},{indexBytes:0},{growthBytes:-1},{backupBytes:NaN},{otherField:1}])assert.equal((await observeCapacityStorageBudget({storageRoot:root,manifest,budget:{...budget,...override}})).passed,null);
  assert.ok((await observeCapacityStorageBudget({manifest,budget})).missing.includes('dedicated_storage_root_missing'));
 }finally{await rm(root,{recursive:true,force:true});}
});

test('storage rejects symlinks, aliases and writable shared parents without fixing their permissions',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-storage-root-'));try{
  await symlink(root,join(root,'alias'));for(const path of ['relative',root+'/.',join(root,'alias')])await assert.rejects(capacityStorageDirectory(path),/storage_root_invalid/);
  await chmod(root,0o777);await assert.rejects(capacityStorageDirectory(root),/storage_root_invalid/);await chmod(root,0o700);assert.equal(await capacityStorageDirectory(root),root);assert.equal(await capacityStorageDirectory(),tmpdir());
  const child=join(root,'private-child');await mkdir(child,{mode:0o700});await chmod(root,0o777);await assert.rejects(capacityStorageDirectory(child),/storage_root_invalid/);await chmod(root,0o1777);assert.equal(await capacityStorageDirectory(child),child);await chmod(root,0o700);
 }finally{await rm(root,{recursive:true,force:true});}
});


test('context planning counts actual session-scoped shared components instead of doubling or omitting context bytes',()=>{
 for(const bodyRequests of [1,32,99,100,599,600,601,620]){
  const manifest=datasetManifest({profile:'selfcheck',metadata30d:700,metadata7d:70,bodyRequests}),seen=new Set();let bytes=0n;
  for(let index=0;index<bodyRequests;index++){let upper=0,size;for(const bin of manifest.bodyDistribution){upper+=bin.percent;if(index*37%100<upper){size=bin.bytes;break;}}const scope=index%manifest.employees+':'+Math.floor(index/manifest.employees)%manifest.sessionsPerEmployee;
   for(const component of capacityHistoricalContext(index,size).components){const content=JSON.stringify(component.content),identity=scope+':'+component.kind+':'+content;if(seen.has(identity))continue;seen.add(identity);bytes+=BigInt(Buffer.byteLength(content));}
  }
  assert.equal(capacityContextSourceBytes(manifest),bytes);if(bodyRequests>600)assert.ok(bytes<capacityBodySourceBytes(manifest));
 }
});
test('context-enabled storage rejects a body-only budget and keeps legacy opt-out explicit',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-context-storage-budget-'));try{
  const manifest=datasetManifest({profile:'selfcheck'}),body=capacityBodySourceBytes(manifest),context=capacityContextSourceBytes(manifest),budget={dataBytes:Number(body),metadataBytes:1,indexBytes:1,walBytes:1,backupBytes:1,temporaryBytes:1,growthBytes:1};
  assert.equal((await observeCapacityStorageBudget({storageRoot:root,manifest,budget,recordingContextEnabled:false})).passed,true);
  const missing=await observeCapacityStorageBudget({storageRoot:root,manifest,budget,recordingContextEnabled:true});assert.equal(missing.passed,null);assert.ok(missing.missing.includes('data_budget_below_known_source_bytes'));assert.equal(missing.knownContextSourceBytes,String(context));
  const covered=await observeCapacityStorageBudget({storageRoot:root,manifest,budget:{...budget,dataBytes:Number(body+context)},recordingContextEnabled:true});assert.equal(covered.passed,true);assert.equal(covered.knownSourceBytes,String(body+context));
  await assert.rejects(observeCapacityStorageBudget({storageRoot:root,manifest,budget,recordingContextEnabled:'false'}),/context_configuration/);
 }finally{await rm(root,{recursive:true,force:true});}
});
