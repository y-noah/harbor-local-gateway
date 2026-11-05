import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {createPrivatePostgres,createOwnedRuntime,ownedProcessProof} from '../tools/capacity/owned-runtime.mjs';
import {startCapacityProcess} from '../tools/capacity/process-adapter.mjs';
import {createCapacityResourceAllocator} from '../tools/capacity/resource-allocator.mjs';
import {PostgresStore} from '../postgres-store.mjs';

for(const resourceAllocator of [null,{},Object.freeze({start(){throw Error('must not invoke public allocator');}})])test('startup rejects an unsigned resource allocator before allocation: '+String(resourceAllocator),async()=>{
 await assert.rejects(createPrivatePostgres({resourceAllocator}),/capacity_resource_allocator_not_owned/);
 await assert.rejects(createPrivatePostgres({resourceAllocator,recovery:{}}),/capacity_resource_allocator_not_owned/);
 const runtime=createOwnedRuntime();let started=false;runtime.start=()=>{started=true;throw Error('must not launch');};
 try{await assert.rejects(startCapacityProcess({runtime,role:'worker',configuration:{},resourceAllocator}),/capacity_resource_allocator_not_owned/);assert.equal(started,false);}finally{await runtime.close();}
});

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,delegatedRoot=process.env.HARBOR_CAPACITY_DELEGATED_CGROUP;
for(const recovery of [false,true])test('real private PostgreSQL starts through its delegated database allocation: recovery='+recovery,{skip:!binDirectory||!delegatedRoot?'需明确委派cgroup、足够独立资源和PG18；不以默认启动代替隔离成功':false,timeout:120000},async()=>{
 let allocator,lab,store;
 try{
  allocator=await createCapacityResourceAllocator({delegatedRoot});lab=await createPrivatePostgres({binDirectory,libraryDirectory,resourceAllocator:allocator,...(recovery?{recovery:{backupKey:Buffer.alloc(32,1)}}:{})});
  assert.equal(ownedProcessProof(lab.runtime,lab.process).role,'database');assert.match(await readFile('/proc/'+lab.process.pid+'/cgroup','utf8'),/\/database\/launch-[a-f0-9-]+\n/);
  store=new PostgresStore({connection:lab.connection});assert.equal((await store.get('SELECT 42 AS n')).n,42);await store.close();store=null;
 }finally{try{await store?.close();}finally{try{await lab?.close();}finally{await allocator?.close();}}}
});
