import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import {createTarget,ownedCapacityTarget} from '../tools/capacity/gateway-adapter.mjs';
import {createCapacityRequestDriver,capacityRequestDriverAgent} from '../tools/capacity/request-driver.mjs';
import {createCapacityInventoryAdapter} from '../tools/capacity/inventory-adapter.mjs';
import {createCapacityResourceAssessment} from '../tools/capacity/resource-assessment.mjs';
import * as proxy from '../tools/capacity/formal-proxy-proof.mjs';
import * as lan from '../tools/capacity/formal-lan-proof.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const sha=value=>createHash('sha256').update(value).digest('hex');
test('proxy/LAN DTOs, copied handles and live allocation claims cannot qualify topology',async()=>{
 for(const producer of [proxy,lan]){await assert.rejects(producer.prepareCapacityProof(Object.freeze({phase:'final'}),{phase:'final'}),/proof_not_owned/);await assert.rejects(producer.inspectCapacityProof(Object.freeze({}),Object.freeze({phase:'final'}),{phase:'final'}),/capability_not_owned/);}assert.throws(()=>proxy.inspectCapacityPublicEntryLease({passed:true}),/observation_not_owned/);assert.throws(()=>lan.inspectCapacityNetworkLease({passed:true}),/observation_not_owned/);
});
test('inventory consumes the current actual driver after physical restore and never falls back to an invalid or retired agent',{skip:!binDirectory?'需独立PG18':false,timeout:180000},async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-topology-inventory-'));let target,driver,other,selected,primary,runAuxiliary;
 try{
  target=await createTarget({employeeCount:1,binDirectory,libraryDirectory,recovery:true,onSetup:options=>{runAuxiliary=options.runAuxiliaryProbe;}});const origin=target.gateway.ready.tlsOrigin,caFile=join(directory,'ca.pem');await writeFile(caFile,target.certificateAuthority,{mode:0o600});driver=await createCapacityRequestDriver({runtime:target.laboratory.runtime,agentPlans:[{id:'admin',origin,tls:{caFile}}]});selected=capacityRequestDriverAgent(driver,'admin');
  const inventory=await runAuxiliary(()=>createCapacityInventoryAdapter({origin,agent:selected,getAgent:()=>selected,adminActors:target.adminActors,employeeActors:target.employeeActors,store:target.store,configurationTargetCount:1,coverageTargetCount:1})),fixture=JSON.stringify(inventory.fixture),oldCookie=sha(target.adminActors[0].headers.cookie);assert.equal((await runAuxiliary(()=>inventory.execute('health'))).success,true);await driver.close();
  const before=target.eventCounts.byKind['gateway-request-start'];selected=undefined;await assert.rejects(runAuxiliary(()=>inventory.execute('health')),/agent_unavailable/);selected={};await assert.rejects(runAuxiliary(()=>inventory.execute('health')),/agent_unavailable/);selected=capacityRequestDriverAgent(driver,'admin');await assert.rejects(runAuxiliary(()=>inventory.execute('health')),/agent_retired/);assert.equal(target.eventCounts.byKind['gateway-request-start'],before);
  const controller=ownedCapacityTarget(target).restoreAssessment;await controller.prepare();await controller.checkpoint();const report=await controller.run();assert.equal(report.hostRestoreRpo.qualification.passed,true,JSON.stringify(report));assert.equal(target.gateway.ready.tlsOrigin,origin);assert.notEqual(sha(target.adminActors[0].headers.cookie),oldCookie);
  other=await createCapacityRequestDriver({runtime:target.laboratory.runtime,agentPlans:[{id:'admin',origin,tls:{caFile}}]});selected=capacityRequestDriverAgent(other,'admin');assert.equal((await runAuxiliary(()=>inventory.execute('health'))).success,true);assert.equal((await runAuxiliary(()=>inventory.execute('overview'))).success,true);assert.equal(JSON.stringify(inventory.fixture),fixture);assert.ok(await target.store.get('SELECT id FROM harbor.members WHERE id=$1',inventory.fixture.memberId));
 }catch(error){primary=error;throw error;}finally{let cleanup;for(const entry of [other,driver,target])try{await entry?.close();}catch(error){cleanup??=error;}await rm(directory,{recursive:true,force:true});if(cleanup&&!primary)throw cleanup;}
});
test('formal network consumption rejects a configured DTO instead of falling back to a successful loopback baseline',{skip:!binDirectory?'需独立PG18':false,timeout:120000},async()=>{
 let target,assessment;try{target=await createTarget({employeeCount:1,binDirectory,libraryDirectory});assessment=await createCapacityResourceAssessment({laboratory:target.laboratory,store:target.store,manifest:target.manifest,hooks:{processes:()=>({gateway:target.gateway,worker:target.worker}),tlsOptions:()=>({ca:target.certificateAuthority}),getNetworkBaselineProof:()=>({passed:true,lan:true})}});await assert.rejects(assessment.measureNetwork({samples:2}),/observation_not_owned/);const result=await assessment.collect();assert.equal(result.networkBaseline.passed,null);}finally{await assessment?.close();await target?.close();}
});

test('an actor refresh does not publish new cookies or a new snapshot when its actual worker is stopped',{skip:!binDirectory?'需独立PG18':false,timeout:120000},async()=>{
 let target,paused=false;try{target=await createTarget({employeeCount:1,binDirectory,libraryDirectory});const before=ownedCapacityTarget(target),cookie=sha(target.adminActors[0].headers.cookie),worker=target.worker.child;process.kill(worker.pid,'SIGSTOP');paused=true;await assert.rejects(before.prepareActorEpoch({phase:'normal-scenario'}),/runtime_epoch_changed|runtime_epoch_lost/);assert.equal(ownedCapacityTarget(target),before);assert.equal(sha(target.adminActors[0].headers.cookie),cookie);process.kill(worker.pid,'SIGCONT');paused=false;await before.prepareActorEpoch({phase:'normal-scenario'});assert.notEqual(ownedCapacityTarget(target),before);assert.notEqual(sha(target.adminActors[0].headers.cookie),cookie);}finally{if(paused&&target.worker.child.exitCode===null&&target.worker.child.signalCode===null)process.kill(target.worker.child.pid,'SIGCONT');await target?.close();}
});

test('a normal process cannot approve an external public origin by adding permit DTOs',{skip:!binDirectory?'需独立PG18':false,timeout:120000},async()=>{
 const {startCapacityProcess}=await import('../tools/capacity/process-adapter.mjs'),{fixtureDatabase}=await import('../tools/capacity/dataset.mjs');let target;const directory=await mkdtemp(join(tmpdir(),'harbor-origin-rejection-'));try{target=await createTarget({employeeCount:1,binDirectory,libraryDirectory});const connection={...target.laboratory.connection,database:fixtureDatabase(target.manifest)};await assert.rejects(startCapacityProcess({runtime:target.laboratory.runtime,role:'worker',configuration:{database:{connection},storageDatabase:{connection},dataDir:directory,runtimeDir:directory,pgDataDir:target.laboratory.dataDirectory??join(target.laboratory.directory,'data'),publicOrigins:['https://unapproved.invalid'],approvedPublicOrigins:['https://unapproved.invalid'],formalTopology:true}}),/command_failed/);}finally{await target?.close();await rm(directory,{recursive:true,force:true});}
});

async function baselineEpochBoundaryChild(){
 const assert=(await import('node:assert/strict')).default,{mock}=await import('node:test'),{pathToFileURL}=await import('node:url'),{join}=await import('node:path');
 let target,assessment,release,entered,capGateway;const gate=new Promise(resolve=>{release=resolve;}),started=new Promise(resolve=>{entered=resolve;}),cap=Object.freeze({});
 mock.module(pathToFileURL(join(process.cwd(),'tools/capacity/formal-lan-proof.mjs')).href,{namedExports:{
  inspectCapacityNetworkBaseline:async(_cap,expected)=>{if(expected.gateway!==capGateway)throw Error('unit_boundary_lease_revoked');entered();await gate;return Object.freeze({passed:true,lanVerified:true,scope:'mocked-verification-boundary-only'});},
  inspectCapacityNetworkLease:(_cap,expected)=>{if(expected.gateway!==capGateway||target.gateway!==capGateway)throw Error('unit_boundary_lease_revoked');return {lan:true};}
 }});
 const {createTarget,ownedCapacityTarget}=await import('./tools/capacity/gateway-adapter.mjs'),{createCapacityResourceAssessment}=await import('./tools/capacity/resource-assessment.mjs');
 try{
  target=await createTarget({employeeCount:1,binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB,recovery:true});capGateway=target.gateway;
  assessment=await createCapacityResourceAssessment({laboratory:target.laboratory,store:target.store,manifest:target.manifest,hooks:{processes:()=>({gateway:target.gateway,worker:target.worker}),getNetworkBaselineProof:()=>cap}});
  const measuring=assessment.measureNetwork(),rejected=assert.rejects(measuring,/network_process_changed|unit_boundary_lease_revoked/);await started;
  const controller=ownedCapacityTarget(target).restoreAssessment;await controller.prepare();await controller.checkpoint();const restored=await controller.run();assert.equal(restored.hostRestoreRpo.qualification.passed,true);assert.notEqual(target.gateway,capGateway);release();await rejected;
  const collected=await assessment.collect();assert.equal(collected.networkBaseline.passed,null);process.stdout.write(JSON.stringify({actualPhysicalRestore:true,verificationBoundaryMocked:true,staleBaselineRejected:true,formalProven:false})+'\n');
 }finally{release();await assessment?.close();await target?.close();mock.restoreAll();}
}
test('an actual restore cannot relabel a delayed old baseline across a mocked proof-verification boundary',{skip:!binDirectory?'需独立PG18；仅证明发布并发边界，不授formal':false,timeout:180000},async()=>{
 const {execFile}=await import('node:child_process'),{promisify}=await import('node:util'),execute=promisify(execFile),program='('+baselineEpochBoundaryChild.toString()+')().catch(error=>{console.error(error);process.exitCode=1;});',result=await execute(process.execPath,['--experimental-test-module-mocks','--input-type=module','-e',program],{cwd:process.cwd(),timeout:150000,maxBuffer:1048576,env:Object.fromEntries(['PATH','HOME','TMPDIR','HARBOR_CAPACITY_PG_BIN','HARBOR_CAPACITY_PG_LIB'].filter(name=>process.env[name]!==undefined).map(name=>[name,process.env[name]]))});const status=JSON.parse(result.stdout.trim().split('\n').at(-1));assert.deepEqual(status,{actualPhysicalRestore:true,verificationBoundaryMocked:true,staleBaselineRejected:true,formalProven:false});
});
