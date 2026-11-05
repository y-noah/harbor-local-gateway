import {capacityWireSignature} from './gateway-probe.mjs';

// These are acceptance targets, never claims that the current host passed.
export const targets=Object.freeze({metadata7d:1260000,metadata30d:5400000,employees:60,administrators:5,modelQps:10,adminQps:5,minSamples:1000,windowMs:300000,successRate:0.999,cpuFraction:0.8,memoryFraction:0.8,batchCompletionMs:10000,bodyVisibility:{p95:5000,p99:10000},eventLoop:{p95:50,p99:100},preDispatch:{p95:200,p99:500},admission:{p95:500,p99:2000},relay:{p95:50,p99:100},recordingExtraP95:50,cancelPropagationMs:1000,releaseAfterUpstreamMs:5000,dbOutageMs:60000,dbRecoveryMs:60000,workerOutageMs:60000,workerRecoveryMs:300000,processRecoveryMs:300000,hostRpoMs:900000,hostRecoveryMs:7200000});
export const apiClasses=Object.freeze({health:{p95:100,p99:300},readiness:{p95:500,p99:1000},list:{p95:500,p99:1000},detail:{p95:500,p99:1000},recording:{p95:800,p99:1500},summary:{p95:2000,p99:4000},configuration:{p95:500,p99:1000},batchPreview:{p95:2000,p99:4000},task:{p95:1000,p99:2000},self:{p95:500,p99:1000}});
export const operations=Object.freeze([
  ['health','GET','/health','health'],['ready','GET','/ready','readiness'],['status','GET','/api/admin/status','readiness'],
  ['request-list','GET','/api/admin/requests?view=list','list'],['request-detail','GET','/api/admin/requests/{request_id}','detail'],['recording-page','GET','/api/admin/requests/{request_id}/recording','recording'],
  ['request-summary','GET','/api/admin/requests?view=summary','summary'],['request-all','GET','/api/admin/requests?view=all','summary'],['overview','GET','/api/admin/state','summary'],
  ...['employee-list','key-list','account-list','session-list','device-list','audit-history'].map(id=>[id,'GET',null,'list']),
  ...['type-statistics','recording-search'].map(id=>[id,'GET',null,'summary']),
  ...['employee-detail','key-detail','account-detail','session-detail','device-detail'].map(id=>[id,'GET',null,'detail']),
  ...['extra-policy-read','extra-account-read'].map(id=>[id,'GET',null,'detail']),
  ...['extra-coverage-read','extra-records-read'].map(id=>[id,'GET',null,'list']),
  ...['extra-account-save','extra-manual-save'].map(id=>[id,null,null,'configuration']),
  ...['employee-save','key-save','model-permissions-save','limits-save','type-default-read','type-default-save','device-state-save'].map(id=>[id,null,null,'configuration']),
  ['batch-preview',null,null,'batchPreview'],...['batch-submit','batch-status','export-submit','export-status','account-check-submit','quota-refresh-submit','extra-refresh-submit','extra-refresh-status','handoff-submit','handoff-status'].map(id=>[id,null,null,'task']),
  ['self','GET','/api/me','self'],['models','GET','/v1/models','self']
].map(([id,method,path,category])=>Object.freeze({id,method,path,category,...apiClasses[category]})));
export const formalScenarios=Object.freeze([
  {id:'mixed',durationMs:1800000,modelQps:10,adminQps:5,employees:60,administrators:5,upstreamMs:[3000,6000],payloadBytes:102400},
  {id:'long-60',durationMs:7200000,concurrency:60,streamMs:300000,adminQps:5},
  // >=30min; 45min permits >=1000 first-content samples for 120 five-minute flows.
  {id:'long-120',durationMs:2700000,concurrency:120,streamMs:300000,adminQps:5},
  // Preserve the burst/recovery deadlines; extend only the tail to obtain the
  // required management sample count at the unchanged background arrival rate.
  {id:'burst',durationMs:Math.max(180000,Math.ceil(targets.minSamples/targets.adminQps)*1000),modelQps:10,adminQps:5,burst:{atMs:60000,durationMs:60000,qps:30,recoveryMs:60000}},
  {id:'steady-8h',durationMs:28800000,modelQps:10,adminQps:5,requiredRealTransferSlots:2},
  {id:'interference',durationMs:1800000,modelQps:10,adminQps:5,tasks:{search:true,metadataExportRows:100000,bodyExportMaxBytes:1073741824,batchEmployees:60}},
  ...['worker-pause','database-disconnect','upstream-429','upstream-disconnect','gateway-drain','gateway-crash','host-restore'].map(id=>({id,durationMs:id==='host-restore'?8100000:600000,modelQps:10,adminQps:5,fault:{atMs:60000,durationMs:60000}}))
]);
export const variants=Object.freeze({lists:['page-25-first','page-100-first','filtered-sorted','deep-continuous'],queries:['cold','steady'],connections:['new-tls','reuse-tls'],recording:['on','off-control'],payloads:['100KiB','long-context','gzip','near-protocol-limit'],ui:[{id:'main-list',p95:1500},{id:'statistics-search',p95:3000}]});
export function capacityPlan(profile='formal'){
  if(!['formal','selfcheck'].includes(profile))throw new Error('capacity_profile_invalid');
  return {schema:'harbor-capacity-v1',profile,targets,operations,variants,adminMix:{list:0.5,detail:0.2,summary:0.2,configuration:0.1},scenarios:profile==='formal'?formalScenarios:[{id:'transport-selfcheck',durationMs:1000,modelQps:10,adminQps:5,employees:2,administrators:1,upstreamMs:[20,40],payloadBytes:1024}],formalEligible:profile==='formal',notes:['正式判断需要实际资源/数据/协议/故障与完整场景证据。','selfcheck仅验证工具工作，不能作为容量通过。']};
}

// Undefined preserves old plans; null is invalid and must never join aliases.
export function capacityOperationCanonical(operation){
  if(operation.canonicalHttp===undefined)return undefined;
  return ['GET','POST','PUT','PATCH','DELETE','HEAD','OPTIONS'].includes(operation.method)&&typeof operation.path==='string'&&operation.path.startsWith('/')&&operation.canonicalHttp===capacityWireSignature(operation.method,operation.path.replace(/\{[a-z_]+\}/g,':id'))?operation.canonicalHttp:null;
}

export function assertFormalPlan(plan){
  if(plan.profile!=='formal'||JSON.stringify(plan.targets)!==JSON.stringify(targets)||JSON.stringify(plan.scenarios)!==JSON.stringify(formalScenarios)||JSON.stringify(plan.variants)!==JSON.stringify(variants))throw new Error('capacity_formal_targets_changed');
  const ids=new Set();for(const operation of plan.operations??[]){if(ids.has(operation.id))throw new Error('capacity_operation_duplicate');ids.add(operation.id);const baseline=operations.find(value=>value.id===operation.id);if(!baseline||operation.category!==baseline.category||!Number.isFinite(operation.p95)||!Number.isFinite(operation.p99)||operation.p95<=0||operation.p99<operation.p95||operation.p95>baseline.p95||operation.p99>baseline.p99)throw new Error('capacity_operation_targets_changed');if(capacityOperationCanonical(operation)===null)throw new Error('capacity_operation_canonical_mismatch');}if(ids.size!==operations.length)throw new Error('capacity_operation_inventory_incomplete');
}

const deepFreeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))deepFreeze(child);Object.freeze(value);}return value;};
for(const value of [targets,apiClasses,operations,formalScenarios,variants])deepFreeze(value);
