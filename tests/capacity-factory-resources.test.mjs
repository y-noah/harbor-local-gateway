import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createTarget,inspectCapacityTargetPrerequisites} from '../tools/capacity/gateway-adapter.mjs';
import {createCapacityResourceAllocator} from '../tools/capacity/resource-allocator.mjs';
import {validateCapacityFactoryJob} from '../tools/capacity/factory-owner.mjs';
import {normalizeCapacityResourceLimits} from '../tools/capacity/resources.mjs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;

test('factory rejects copied or unowned allocator objects before any private resource or model payload',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-factory-resource-'));try{let entered=false;await assert.rejects(createTarget({resourceAllocator:{start:()=>{entered=true;},observe:()=>({passed:true})},storageRoot:root}),/resource_allocator_not_owned/);assert.equal(entered,false);assert.deepEqual(await readdir(root),[]);}finally{await rm(root,{recursive:true,force:true});}
});

test('formal preparation retains measured storage facts but neither a JSON resource claim nor network booleans can allocate',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-formal-preparation-'));try{
  const diskBudget={dataBytes:200000000000,metadataBytes:1000000,indexBytes:1000000,walBytes:1000000,backupBytes:1000000,temporaryBytes:1000000,growthBytes:1000000},options={profile:'formal',storageRoot:root,diskBudget,resourceAllocator:{observe:async()=>({passed:true,missing:[]})},ownerAuthority:{owned:true,scope:'private-native-factory-authority'},remotePrerequisites:{missing:[],softwareMissing:[],passed:true},proxyPreserved:true,loadClientLanBaseline:true};
  const observed=await inspectCapacityTargetPrerequisites(options);assert.equal(observed.eligible,false);assert.equal(observed.phase,'before_allocation');assert.ok(observed.missing.includes('owned_formal_resource_allocator'));assert.ok(observed.missing.includes('owned_formal_factory_owner'));assert.equal(observed.evidence.factoryOwner.owned,false);assert.ok(observed.missing.includes('preserved_proxy_and_lan_baseline'));assert.equal(observed.evidence.storage.knownBodySourceBytes,'186679296000');assert.deepEqual(await readdir(root),[]);
  await assert.rejects(createTarget({profile:'formal',storageRoot:root,diskBudget,binDirectory:'/not-available/postgresql/18/bin'}),error=>{assert.equal(error.message,'capacity_formal_adapter_incomplete');assert.ok(error.preflight.missing.includes('preserved_proxy_and_lan_baseline'));return true;});assert.deepEqual(await readdir(root),[]);
 }finally{await rm(root,{recursive:true,force:true});}
});

test('selected storage owns both actual PG and gateway lab directories and cleans only its nonce children',{skip:!binDirectory,timeout:120000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-factory-storage-'));let target;try{await writeFile(join(root,'other-task'),'keep');target=await createTarget({profile:'selfcheck',employeeCount:1,binDirectory,libraryDirectory,storageRoot:root});assert.ok(target.laboratory.directory.startsWith(root+'/harbor-capacity-'));assert.equal(target.datasetManifest.profile,'selfcheck');assert.equal(target.gateway.ready.role,'gateway');assert.equal(target.worker.ready.role,'worker');assert.equal((await target.gateway.command('status')).runtime.ownership,true);await target.close();target=null;assert.deepEqual(await readdir(root),['other-task']);assert.equal(await readFile(join(root,'other-task'),'utf8'),'keep');}finally{await target?.close();await rm(root,{recursive:true,force:true});}
});

test('owned allocator really places factory PG and gateway/Worker before their payloads run',{
 skip:!binDirectory||!process.env.HARBOR_CAPACITY_DELEGATED_CGROUP?'需真实独立委派cgroup和PG18；本机不足不冒充正向隔离':false,timeout:180000
},async()=>{
 let allocator,target;try{allocator=await createCapacityResourceAllocator({delegatedRoot:process.env.HARBOR_CAPACITY_DELEGATED_CGROUP});target=await createTarget({profile:'selfcheck',binDirectory,libraryDirectory,employeeCount:1,resourceAllocator:allocator});const report=await allocator.observe();assert.equal(report.passed,true,JSON.stringify(report.missing));assert.ok(report.roles.gatewayWorker.processRoles.includes('gateway'));assert.ok(report.roles.gatewayWorker.processRoles.includes('worker'));assert.deepEqual(report.roles.database.processRoles,['database']);}finally{await target?.close();await allocator?.close();}
});

const deliveryLimits=()=>({gatewayWorker:{cpuQuota:1.5,memoryLimitBytes:2684354560},database:{cpuQuota:1,memoryLimitBytes:2147483648},processLimits:{gateway:{cpuQuota:1,memoryLimitBytes:2147483648},worker:{cpuQuota:0.5,memoryLimitBytes:536870912},database:{cpuQuota:0.5,memoryLimitBytes:1073741824}}});
test('native owner job validates and copies the explicit limits without trusting a resource DTO',async()=>{
 const resourceLimits=deliveryLimits(),job=validateCapacityFactoryJob({target:{profile:'selfcheck',purpose:'verify',resourceDelegatedRoot:'/sys/fs/cgroup/task-only',resourceLimits}});assert.deepEqual(job.target.resourceLimits,normalizeCapacityResourceLimits(resourceLimits));resourceLimits.processLimits.gateway.cpuQuota=9;assert.equal(job.target.resourceLimits.processLimits.gateway.cpuQuota,1);
 assert.throws(()=>validateCapacityFactoryJob({target:{profile:'selfcheck',resourceLimits:deliveryLimits()}}),/delegation_required/);assert.throws(()=>validateCapacityFactoryJob({target:{profile:'selfcheck',resourceDelegatedRoot:'/sys/fs/cgroup/task-only',resourceLimits:{gatewayWorker:{cpuQuota:0,memoryLimitBytes:1}}}}),/limits_invalid/);
 let allocated=false;await assert.rejects(createTarget({resourceLimits:deliveryLimits(),onAllocation:()=>{allocated=true;}}),/allocator_required/);assert.equal(allocated,false);
});
test('CLI forwards a complete declaration and rejects malformed limits before adapter allocation',async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-limits-cli-')),adapter=join(root,'adapter.mjs'),input=join(root,'limits.json'),marker=join(root,'allocated.json'),execute=promisify(execFile);
 try{
  await writeFile(adapter,'import {writeFile} from "node:fs/promises";export async function createTarget(options){await writeFile('+JSON.stringify(marker)+',JSON.stringify(options.resourceLimits),{mode:0o600});throw Error("capacity_limits_forwarded");}');await writeFile(input,JSON.stringify(deliveryLimits()),{mode:0o600});const args=['tools/capacity/cli.mjs','verify','--profile','selfcheck','--adapter',adapter,'--resource-limits-file',input];
  await assert.rejects(execute(process.execPath,args),error=>JSON.parse(error.stderr).error.code==='capacity_limits_forwarded');assert.deepEqual(JSON.parse(await readFile(marker,'utf8')),normalizeCapacityResourceLimits(deliveryLimits()));await rm(marker);
  await writeFile(input,JSON.stringify({...deliveryLimits(),database:{cpuQuota:0,memoryLimitBytes:1}}));await assert.rejects(execute(process.execPath,args),error=>JSON.parse(error.stderr).error.code==='capacity_resource_limits_invalid');assert.deepEqual((await readdir(root)).sort(),['adapter.mjs','limits.json']);
 }finally{await rm(root,{recursive:true,force:true});}
});
