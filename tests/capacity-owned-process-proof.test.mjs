import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import * as ownedRuntime from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createOwnedFaultAdapter} from '../tools/capacity/faults.mjs';
import {createCapacityResourceAssessment} from '../tools/capacity/resource-assessment.mjs';
import {createCapacityBrowserAssessment} from '../tools/capacity/browser-assessment.mjs';
import {createTarget} from '../tools/capacity/gateway-adapter.mjs';
import {collectCgroupRole} from '../tools/capacity/resources.mjs';
import {datasetManifest,fixtureDatabase,createDatasetDatabase} from '../tools/capacity/dataset.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,integration={skip:!binDirectory?'需显式本任务私有PostgreSQL18工具路径':false,timeout:120000};
const timer=(runtime,role)=>runtime.start(process.execPath,['-e','setInterval(()=>{},1000)'],{role});

test('R68 resource observation rejects a real foreign lab child even when public proof forges its role',integration,async()=>{
  let first,second,admin,fixture,assessment;try{
    first=await ownedRuntime.createPrivatePostgres({binDirectory,libraryDirectory});second=await ownedRuntime.createPrivatePostgres({binDirectory,libraryDirectory});admin=new PostgresStore({connection:first.connection});const manifest=datasetManifest({profile:'selfcheck',metadata30d:50,metadata7d:20,bodyRequests:32});fixture=await createDatasetDatabase(admin,manifest,{laboratory:first,connectionFor:name=>({connection:{...first.connection,database:name}}),encryptionKey:randomBytes(32),recordingKey:randomBytes(32)});
    const gateway=timer(second.runtime,'gateway'),worker=timer(second.runtime,'worker');first.runtime.proof=child=>({pid:child.pid,role:child===gateway?'gateway':child===worker?'worker':'database'});assessment=await createCapacityResourceAssessment({laboratory:first,store:fixture.store,manifest,hooks:{processes:()=>({gateway:{child:gateway},worker:{child:worker}})}});const sampled=await assessment.sample();assert.equal(sampled.sampled,false,'foreign child must not become a valid observed epoch');const report=await assessment.collect();assert.equal(report.resources.qualification.passed,false);assert.equal(report.resources.epochs.length,0);assert.equal(report.resources.passed,null);assert.ok(report.resources.gaps.some(row=>row.code==='owned_process_or_database_unavailable'));assert.equal(gateway.exitCode,null);assert.equal(worker.exitCode,null);
  }finally{await assessment?.close();await fixture?.destroy();await admin?.close();await first?.close();await second?.close();}
});
test('R68 fault metadata rejects a real foreign worker despite replaced public proof',integration,async()=>{
  let first,second;try{first=await ownedRuntime.createPrivatePostgres({binDirectory,libraryDirectory});second=await ownedRuntime.createPrivatePostgres({binDirectory,libraryDirectory});const child=timer(second.runtime,'worker');first.runtime.proof=()=>({role:'worker',pid:child.pid});assert.throws(()=>createOwnedFaultAdapter({runtime:first.runtime,worker:child}),/not_owned/);await assert.rejects(collectCgroupRole('/sys/fs/cgroup',{processes:[{runtime:first.runtime,child}]}),/not_owned/);assert.equal(child.exitCode,null);}finally{await first?.close();await second?.close();}
});
test('trusted process proof ignores public replacement and rejects cloned, fake and borrowed child ownership',async()=>{
  const first=ownedRuntime.createOwnedRuntime(),second=ownedRuntime.createOwnedRuntime();try{const own=timer(first,'worker'),foreign=timer(second,'worker');first.proof=child=>({pid:child.pid,role:'database'});assert.equal(ownedRuntime.ownedProcessProof(first,own).role,'worker');assert.equal(ownedRuntime.ownedProcessProof(first,own).pid,own.pid);assert.throws(()=>ownedRuntime.ownedProcessProof(first,foreign),/not_owned/);assert.throws(()=>ownedRuntime.ownedProcessProof(first,{...own}),/not_owned/);assert.throws(()=>ownedRuntime.ownedProcessProof({...first},own),/not_owned/);assert.throws(()=>ownedRuntime.ownedProcessProof({proof:()=>({pid:own.pid,role:'worker'})},own),/not_owned/);await first.stop(own);assert.throws(()=>ownedRuntime.ownedProcessProof(first,own),/not_owned/);}finally{await first.close();await second.close();}
});

test('R68 public proof replacement cannot reject the genuine pinned gateway at the browser boundary',integration,async()=>{
  let target,assessment;try{target=await createTarget({binDirectory,libraryDirectory,employeeCount:1,onSetup:async({laboratory})=>{laboratory.runtime.proof=child=>({pid:child.pid,role:'worker'});}});let launched=0;await assert.rejects(createCapacityBrowserAssessment({target,launchBrowser:async()=>{launched++;throw Error('reached_owned_browser_launch');}}),/reached_owned_browser_launch/);assert.equal(launched,1);}finally{await assessment?.close();await target?.close();}
});
