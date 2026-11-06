import {statfs,lstat} from 'node:fs/promises';
import {datasetManifest} from './dataset.mjs';
import {capacityStorageDirectory} from './storage-location.mjs';

const components=['dataBytes','metadataBytes','indexBytes','walBytes','backupBytes','temporaryBytes','growthBytes'];
function budgetIssues(budget){
 if(!budget||typeof budget!=='object'||Array.isArray(budget)||Object.keys(budget).some(key=>!components.includes(key)))return ['explicit_storage_components_missing'];
 return components.filter(key=>!Number.isSafeInteger(budget[key])||budget[key]<=0).map(key=>'storage_component_invalid:'+key);
}
export function normalizeCapacityDiskBudget(budget){
 if(budgetIssues(budget).length)throw Error('capacity_storage_components_invalid');
 return Object.freeze(Object.fromEntries(components.map(key=>[key,budget[key]])));
}
export function capacityBodySourceBytes(manifest){
 const canonical=datasetManifest(manifest);if(JSON.stringify(canonical)!==JSON.stringify(manifest))throw Error('capacity_storage_manifest_invalid');
 const cycles=Math.floor(manifest.bodyRequests/100),tail=manifest.bodyRequests%100;let bytes=0n;
 for(let index=0;index<100;index++){
  const point=index*37%100;let upper=0;
  for(const bin of manifest.bodyDistribution){upper+=bin.percent;if(point<upper){bytes+=BigInt(cycles+(index<tail?1:0))*BigInt(bin.bytes);break;}}
 }
 return bytes;
}

// This proves free space for an explicit storage plan, not that a small sample
// predicts full-scale index/WAL/backup sizes. Those components are never zeroed
// or guessed from Token counts. The report retains that planning distinction.
export async function observeCapacityStorageBudget({storageRoot,manifest,budget}={}){
 const directory=await capacityStorageDirectory(storageRoot),sourceBytes=capacityBodySourceBytes(manifest);
 const issues=budgetIssues(budget),missing=[];let required=0n;
 if(storageRoot===undefined)missing.push('dedicated_storage_root_missing');
 missing.push(...issues);if(!issues.length)for(const key of components)required+=BigInt(budget[key]);
 if(!missing.length&&BigInt(budget.dataBytes)<sourceBytes)missing.push('data_budget_below_known_source_bytes');
 const [fs,identity]=await Promise.all([statfs(directory,{bigint:true}),lstat(directory,{bigint:true})]);
 const available=fs.bavail*fs.bsize;if(!missing.length&&required>available)missing.push('storage_free_below_declared_budget');
 return Object.freeze({passed:missing.length?null:true,source:'observed-filesystem-and-explicit-storage-plan',observedAt:Date.now(),volume:{device:String(identity.dev),inode:String(identity.ino),availableBytes:String(available),totalBytes:String(fs.blocks*fs.bsize)},manifestHash:manifest.manifestHash,knownBodySourceBytes:String(sourceBytes),declaredBudget:budget?Object.fromEntries(components.filter(key=>Number.isSafeInteger(budget[key])).map(key=>[key,String(budget[key])])):null,requiredBytes:missing.some(code=>code.startsWith('storage_component_')||code==='explicit_storage_components_missing')?null:String(required),planning:'explicit_components_not_a_full_scale_measurement',missing});
}
