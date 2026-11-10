import {statfs,lstat} from 'node:fs/promises';
import {datasetManifest} from './dataset.mjs';
import {capacityHistoricalContext} from './context-payload.mjs';
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

// The generator shares only instruction/tool definitions within each session.
// History and current context include the request index and remain distinct.
export function capacityContextSourceBytes(manifest){
 const logical=capacityBodySourceBytes(manifest),sample=capacityHistoricalContext(0,manifest.bodyDistribution[0].bytes);
 const shared=sample.components.filter(row=>row.kind==='instruction'||row.kind==='tool_schema').reduce((sum,row)=>sum+Buffer.byteLength(JSON.stringify(row.content)),0);
 const scopes=Math.min(manifest.bodyRequests,manifest.employees*manifest.sessionsPerEmployee);
 return logical-BigInt(manifest.bodyRequests-scopes)*BigInt(shared);
}

// This proves free space for an explicit storage plan, not that a small sample
// predicts full-scale index/WAL/backup sizes. Those components are never zeroed
// or guessed from Token counts. The report retains that planning distinction.
export async function observeCapacityStorageBudget({storageRoot,manifest,budget,recordingContextEnabled=false}={}){
 if(typeof recordingContextEnabled!=='boolean')throw Error('capacity_storage_context_configuration');
 const directory=await capacityStorageDirectory(storageRoot),sourceBytes=capacityBodySourceBytes(manifest),contextBytes=recordingContextEnabled?capacityContextSourceBytes(manifest):0n;
 const issues=budgetIssues(budget),missing=[];let required=0n;
 if(storageRoot===undefined)missing.push('dedicated_storage_root_missing');
 missing.push(...issues);if(!issues.length)for(const key of components)required+=BigInt(budget[key]);
 if(!missing.length&&BigInt(budget.dataBytes)<sourceBytes+contextBytes)missing.push('data_budget_below_known_source_bytes');
 const [fs,identity]=await Promise.all([statfs(directory,{bigint:true}),lstat(directory,{bigint:true})]);
 const available=fs.bavail*fs.bsize;if(!missing.length&&required>available)missing.push('storage_free_below_declared_budget');
 return Object.freeze({passed:missing.length?null:true,source:'observed-filesystem-and-explicit-storage-plan',observedAt:Date.now(),volume:{device:String(identity.dev),inode:String(identity.ino),availableBytes:String(available),totalBytes:String(fs.blocks*fs.bsize)},manifestHash:manifest.manifestHash,knownBodySourceBytes:String(sourceBytes),knownContextSourceBytes:String(contextBytes),knownSourceBytes:String(sourceBytes+contextBytes),recordingContextEnabled,declaredBudget:budget?Object.fromEntries(components.filter(key=>Number.isSafeInteger(budget[key])).map(key=>[key,String(budget[key])])):null,requiredBytes:missing.some(code=>code.startsWith('storage_component_')||code==='explicit_storage_components_missing')?null:String(required),planning:'explicit_components_not_a_full_scale_measurement',missing});
}
