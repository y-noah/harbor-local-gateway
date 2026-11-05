import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,readdir,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createTarget,inspectCapacityTargetPrerequisites} from '../tools/capacity/gateway-adapter.mjs';
import {createCapacityResourceAllocator} from '../tools/capacity/resource-allocator.mjs';
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
