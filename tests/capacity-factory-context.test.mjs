import test,{before,after} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,readdir,readFile} from 'node:fs/promises';
import {spawn,execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createTarget,ownedCapacityTarget,inspectCapacityTargetPrerequisites,inspectCapacityBrowserPublication,collectCapacityTargetRemoteConsumption,inspectCapacityTargetRemoteConsumption} from '../tools/capacity/gateway-adapter.mjs';
import {capacityFormalProofRequirements,inspectCapacityFormalSoftware,createCapacityFormalProofConsumer,collectCapacityFormalProofs,recheckCapacityFormalProofs} from '../tools/capacity/formal-proof-catalogue.mjs';
import {buildCapacityPreparationEntry,runCapacityOwnedFactory} from '../tools/capacity/preparation-context.mjs';
import {inspectCapacityRemotePrerequisites} from '../tools/capacity/formal-remote-proof.mjs';
import {inspectCapacityHelperWorkload} from '../tools/capacity/scope-stage-factory.mjs';
import {createNormalExecution} from '../tools/capacity/normal-execution.mjs';
import {validateCapacityFactoryJob} from '../tools/capacity/factory-owner.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,real={skip:!binDirectory?'需独立PG18工具路径':false,timeout:120000};let entryDirectory;
before(async()=>{if(binDirectory){entryDirectory=await mkdtemp(join(tmpdir(),'harbor-factory-entry-'));await buildCapacityPreparationEntry({directory:entryDirectory});}});
after(async()=>{if(entryDirectory)await rm(entryDirectory,{recursive:true,force:true});});

test('formal software gaps name fixed producers, phases and concrete consumer bridges',async()=>{
 const contracts=capacityFormalProofRequirements();assert.deepEqual(contracts.map(row=>row.id),['runtime','remote','lan','proxy','clock','helperClock','helper']);assert.ok(contracts.every(row=>row.phases.join(',')==='allocation,pre-seed,final,restore'));const report=await inspectCapacityFormalSoftware();assert.deepEqual(report.softwareMissing,[]);
 const readiness=await inspectCapacityTargetPrerequisites({profile:'formal'});assert.equal(readiness.allocationEligible,false);assert.equal(readiness.seedEligible,false);assert.equal(readiness.softwareMissing.includes('formal_process_launch_bridge_consumer_not_implemented'),false);const remote=await inspectCapacityRemotePrerequisites();assert.deepEqual(readiness.evidence.remoteConsumers,remote.evidence);assert.ok(remote.softwareMissing.every(code=>readiness.softwareMissing.includes(code)));assert.equal(readiness.softwareMissing.includes('formal_remote_driver_browser_consumer_not_implemented'),false);assert.equal(readiness.softwareMissing.includes('formal_remote_clock_mapping_consumer_not_implemented'),false);assert.ok(readiness.missing.includes('owned_formal_factory_owner'));
});
test('proof DTO, copied consumer and phase claims never authorize a seed',async()=>{
 for(const claim of [{passed:true,phase:'pre-seed'},{formalDataAllowed:true},{},Object.freeze({nonce:'known'})]){await assert.rejects(collectCapacityFormalProofs(claim,'pre-seed'),/proof_not_owned/);await assert.rejects(recheckCapacityFormalProofs(claim,'pre-seed'),/proof_not_owned/);}
 const spoofed=createCapacityFormalProofConsumer({isOpen:()=>true,getBinding:()=>Object.freeze({phase:'pre-seed',nonce:'fake',manifest:{profile:'formal'}})});await assert.rejects(collectCapacityFormalProofs(spoofed,'pre-seed'),/proof_not_owned/);await assert.rejects(createTarget({ownerAuthority:Object.freeze({})}),/owner_not_owned/);
 for(const extra of [{manifest:{profile:'formal'}},{target:{profile:'formal'}},{receipt:{passed:true}},{resourceAllocator:{observe:()=>({passed:true})}},{onSetup:()=>{}}])assert.throws(()=>validateCapacityFactoryJob({target:{profile:'selfcheck'},execution:{},...extra}),/configuration|not_authorized/);
});
test('browser publication never accepts a caller snapshot or capability DTO',async()=>{
 for(const value of [undefined,null,{},Object.freeze({}),{target:{},snapshot:{requestDriver:{},adminActors:[]}},JSON.parse('{"passed":true,"publicationPending":false}')]){assert.throws(()=>inspectCapacityBrowserPublication(value),/publication_not_owned/);assert.throws(()=>inspectCapacityTargetRemoteConsumption(value),/consumption_not_owned/);await assert.rejects(inspectCapacityHelperWorkload(value,{gateway:{}}),/source_not_owned/);await assert.rejects(collectCapacityTargetRemoteConsumption(value),/target_not_owned/);}
});

test('private publication blocks ordinary consumers and expires after a real actor epoch and close',real,async()=>{
 // Only the test module exports the private lifecycle helpers. The production
 // API has no mint or finish method and still rejects every caller DTO.
 const source=(await readFile(new URL('../tools/capacity/gateway-adapter.mjs',import.meta.url),'utf8')).replace(/from '([^']+)'/g,(full,path)=>path.startsWith('.')?"from '"+new URL('../tools/capacity/'+path,import.meta.url).href+"'":full)+'\nexport {beginCapacityBrowserPublication,finishCapacityBrowserPublication};';
 const adapter=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));let target;
 try{
  target=await adapter.createTarget({employeeCount:1,binDirectory,libraryDirectory});await assert.rejects(adapter.collectCapacityTargetRemoteConsumption(target),/profile/);const snapshot=adapter.ownedCapacityTarget(target),publication=adapter.beginCapacityBrowserPublication(target);
  assert.throws(()=>adapter.ownedCapacityTarget(target),/target_not_owned/);assert.equal(adapter.inspectCapacityBrowserPublication(publication).snapshot,snapshot);assert.equal(adapter.inspectCapacityBrowserPublication(publication).target,target);
  const helperSource=adapter.inspectCapacityBrowserPublication(publication).snapshot.helperWorkloadSource;await assert.rejects(inspectCapacityHelperWorkload(helperSource,{gateway:snapshot.gatewayChild}),/normal.*not_owned|run.*not_owned/);
  adapter.finishCapacityBrowserPublication(publication);assert.equal(adapter.ownedCapacityTarget(target),snapshot);
  await target.prepareActorEpoch({phase:'normal-scenario'});assert.notEqual(adapter.ownedCapacityTarget(target),snapshot);assert.equal(adapter.ownedCapacityTarget(target).helperWorkloadSource,helperSource);assert.throws(()=>adapter.inspectCapacityBrowserPublication(publication),/publication_not_owned/);
  const next=adapter.beginCapacityBrowserPublication(target);await target.close();assert.throws(()=>adapter.inspectCapacityBrowserPublication(next),/publication_not_owned|target_not_owned/);await assert.rejects(inspectCapacityHelperWorkload(helperSource,{gateway:snapshot.gatewayChild}),/run_not_owned/);target=null;
 }finally{await target?.close();}
});


test('helper workload run survives UI refresh but expires for another normal run, runtime or close',real,async()=>{
 let target,first,second;
 try{
  target=await createTarget({employeeCount:1,binDirectory,libraryDirectory});const snapshot=ownedCapacityTarget(target),source=snapshot.helperWorkloadSource;
  first=await createNormalExecution({target});await first.close();const options={gateway:snapshot.gatewayChild,run:first.helperWorkload};
  const before=await inspectCapacityHelperWorkload(source,options);assert.equal(before.passed,null);assert.ok(before.missing.includes('helper_workload_actual_model_unproven'));
  await target.prepareActorEpoch({phase:'ui-additional-load'});const after=await inspectCapacityHelperWorkload(source,options);assert.equal(after.passed,null);assert.deepEqual(after.evidence,before.evidence);
  await assert.rejects(inspectCapacityHelperWorkload(source,{...options,gateway:{}}),/run_not_owned/);
  second=await createNormalExecution({target});await second.close();await assert.rejects(inspectCapacityHelperWorkload(source,options),/not_owned|revoked|current|binding/);
  const next={gateway:snapshot.gatewayChild,run:second.helperWorkload};assert.equal((await inspectCapacityHelperWorkload(source,next)).passed,null);
  await target.close();target=null;await assert.rejects(inspectCapacityHelperWorkload(source,next),/epoch_changed|not_owned|closed/);
 }finally{await first?.close();await second?.close();await target?.close();}
});

test('real factory prepares empty DB then seeds same manifest and verifies a fresh final epoch',real,async()=>{
 let target;const order=[];try{
  target=await createTarget({profile:'selfcheck',binDirectory,libraryDirectory,employeeCount:1,onProgress:event=>order.push({...event})});
  const preparation=order.findIndex(row=>row.kind==='preparation-ready'),metadata=order.findIndex(row=>row.kind==='metadata'),final=order.findIndex(row=>row.kind==='final-runtime-ready');assert.ok(preparation>=0&&metadata>preparation&&final>metadata);
  const evidence=(await target.collectEvidence()).adapter.factory;assert.equal(evidence.phase,'ready');assert.equal(evidence.formalQualified,false);assert.equal(evidence.manifestHash,target.datasetManifest.manifestHash);assert.equal(evidence.epochs,2);assert.deepEqual(evidence.receipts.map(row=>row.stage),['pre-seed','final']);assert.ok(evidence.receipts.every(row=>row.manifestHash===evidence.manifestHash&&row.wire.started===1&&row.wire.ended===1&&row.wire.settled===1));assert.notEqual(evidence.receipts[0].traceHash,evidence.receipts[1].traceHash);assert.equal(target.dataset.verified,true);assert.equal((await target.gateway.command('runtime-sources')).scope,'not-installed');assert.deepEqual(await target.gateway.command('runtime-storage'),{scope:'not-installed',verified:false});
 }finally{await target?.close();}
});
test('fixed native factory owner seeds and verifies privately then removes its own lab',real,async()=>{
 const storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-native-'));try{
  const result=await runCapacityOwnedFactory({entryDirectory,job:{target:{profile:'selfcheck',purpose:'verify',binDirectory,libraryDirectory,storageRoot},execution:{}}});assert.equal(result.verified,true);assert.equal(result.formalScale,false);assert.equal(result.laboratoryLifecycle.destroyedAfterCommand,true);assert.deepEqual(await readdir(storageRoot),[]);
 }finally{await rm(storageRoot,{recursive:true,force:true});}
});
test('native owner keeps actual target and runner together for the small positive execution',real,async()=>{
 const storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-runner-'));try{
  const report=await runCapacityOwnedFactory({entryDirectory,job:{target:{profile:'selfcheck',purpose:'run',binDirectory,libraryDirectory,storageRoot,employeeCount:2},execution:{}}});assert.equal(report.profile,'selfcheck');assert.notEqual(report.verdict.status,'passed');assert.equal(report.evidence.adapter.factory.phase,'ready');assert.equal(report.evidence.adapter.factory.receipts.length,2);assert.ok(report.metrics.series.some(row=>row.operation==='model'&&row.successRate===1));assert.deepEqual(await readdir(storageRoot),[]);
 }finally{await rm(storageRoot,{recursive:true,force:true});}
});

test('native factory cancellation after the ready receipt drains its own resources',real,async()=>{
 const storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-abort-')),controller=new AbortController();let seen=false;try{
  await assert.rejects(runCapacityOwnedFactory({entryDirectory,signal:controller.signal,onProgress:event=>{if(event.kind==='preparation-ready'){seen=true;controller.abort();}},job:{target:{profile:'selfcheck',purpose:'run',binDirectory,libraryDirectory,storageRoot},execution:{}}}));assert.equal(seen,true);assert.deepEqual(await readdir(storageRoot),[]);
 }finally{await rm(storageRoot,{recursive:true,force:true});}
});

async function waitUntil(read,predicate,timeoutMs=20000){const deadline=performance.now()+timeoutMs;for(;;){const value=await read();if(predicate(value))return value;if(performance.now()>deadline)throw Error('factory condition timeout');await new Promise(resolve=>setTimeout(resolve,10));}}
async function runningPid(pid){try{const raw=await readFile('/proc/'+pid+'/stat','utf8');return !['Z','X','x'].includes(raw.slice(raw.lastIndexOf(')')+2).split(' ')[0]);}catch(error){if(error.code==='ENOENT')return false;throw error;}}
test('native factory owner survives controller loss only to retire the actual owned lab',real,async()=>{
 const storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-disconnect-'));let ownerPid,child;
 try{
  const script=`import {readFile} from 'node:fs/promises';import {runCapacityOwnedFactory} from './tools/capacity/preparation-context.mjs';process.once('message',configuration=>{void runCapacityOwnedFactory({...configuration,onProgress:async event=>{if(event.kind!=='preparation-ready')return;const children=(await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\\s+/).filter(Boolean).map(Number);for(const pid of children)try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('--owned-capacity-preparation'))process.send({ownerPid:pid});}catch{}}}).then(()=>process.exit(0),()=>process.exit(1));});`;
  child=spawn(process.execPath,['--input-type=module','-e',script],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc']});child.on('message',message=>{ownerPid=message.ownerPid;});child.send({entryDirectory,job:{target:{profile:'selfcheck',purpose:'run',binDirectory,libraryDirectory,storageRoot},execution:{}}});
  await waitUntil(()=>ownerPid,Boolean);child.kill('SIGKILL');await waitUntil(()=>runningPid(ownerPid),value=>!value);await waitUntil(()=>readdir(storageRoot),rows=>rows.length===0);
 }finally{if(child?.exitCode===null&&child?.signalCode===null)child.kill('SIGKILL');if(!ownerPid||!await runningPid(ownerPid))await rm(storageRoot,{recursive:true,force:true});}
});

test('factory subreaper closes stopped adopted PG descendants when postmaster dies before cancellation',real,async()=>{
 const storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-adopted-')),controller=new AbortController();let mutation,paused=[];
 try{
  await assert.rejects(runCapacityOwnedFactory({entryDirectory,signal:controller.signal,onProgress:event=>{
   if(event.kind!=='preparation-ready'||mutation)return;mutation=(async()=>{
    const owner=await waitUntil(async()=>{for(const pid of (await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean))try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('--owned-capacity-preparation'))return Number(pid);}catch{}},Boolean);
    const postgres=await waitUntil(async()=>{for(const pid of (await readFile('/proc/'+owner+'/task/'+owner+'/children','utf8')).trim().split(/\s+/).filter(Boolean))try{if((await readFile('/proc/'+pid+'/comm','utf8')).trim()==='postgres')return Number(pid);}catch{}},Boolean);
    paused=(await readFile('/proc/'+postgres+'/task/'+postgres+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);assert.ok(paused.length>0);for(const pid of paused)process.kill(pid,'SIGSTOP');process.kill(postgres,'SIGKILL');controller.abort();
   })();mutation.catch(()=>controller.abort());
  },job:{target:{profile:'selfcheck',purpose:'run',binDirectory,libraryDirectory,storageRoot},execution:{}}}));await mutation;assert.ok(paused.length>0);for(const pid of paused)assert.equal(await runningPid(pid),false);assert.deepEqual(await readdir(storageRoot),[]);
 }finally{for(const pid of paused)if(await runningPid(pid))try{process.kill(pid,'SIGCONT');process.kill(pid,'SIGKILL');}catch{}await rm(storageRoot,{recursive:true,force:true});}
});

test('builtin CLI uses fixed owner and reports software gaps without paths or formal allocation',real,async()=>{
 const execute=promisify(execFile),storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-cli-'));const common=['tools/capacity/cli.mjs','verify','--adapter','tools/capacity/gateway-adapter.mjs','--owner-entry',entryDirectory,'--pg-bin',binDirectory,'--storage-root',storageRoot,...(libraryDirectory?['--pg-lib',libraryDirectory]:[])];
 try{
  const result=await execute(process.execPath,[...common,'--profile','selfcheck'],{timeout:90000,maxBuffer:1048576});assert.equal(JSON.parse(result.stdout).verified,true);assert.deepEqual(await readdir(storageRoot),[]);
  await assert.rejects(execute(process.execPath,[...common,'--profile','formal'],{timeout:30000,maxBuffer:1048576}),error=>{const report=JSON.parse(error.stderr);assert.equal(report.error.code,'capacity_formal_adapter_incomplete');assert.equal(report.preflight.softwareMissing.includes('formal_remote_driver_browser_consumer_not_implemented'),false);assert.ok(report.preflight.missing.includes('owned_formal_resource_allocator'));assert.equal(report.preflight.seedEligible,false);assert.equal(error.stderr.includes(storageRoot),false);return true;});
  const invalidBinary=[...common];invalidBinary[invalidBinary.indexOf('--pg-bin')+1]='/nonexistent/postgresql/18/bin';await assert.rejects(execute(process.execPath,[...invalidBinary,'--profile','formal'],{timeout:30000,maxBuffer:1048576}),error=>{const report=JSON.parse(error.stderr);assert.equal(report.error.code,'capacity_formal_adapter_incomplete');assert.equal(report.preflight.softwareMissing.includes('formal_remote_driver_browser_consumer_not_implemented'),false);assert.ok(report.preflight.missing.includes('owned_formal_resource_allocator'));return true;});assert.deepEqual(await readdir(storageRoot),[]);
 }finally{await rm(storageRoot,{recursive:true,force:true});}
});

async function finalOwnedRoles(){const owner=await waitUntil(async()=>{for(const pid of (await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean))try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('--owned-capacity-preparation'))return Number(pid);}catch{}},Boolean);const roles=[];for(const pid of (await readFile('/proc/'+owner+'/task/'+owner+'/children','utf8')).trim().split(/\s+/).filter(Boolean))try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('--owned-capacity-child'))roles.push(Number(pid));}catch{}assert.equal(roles.length,2);return {owner,roles};}
test('cancel wakes both stopped final roles before waiting for close',real,async()=>{
 const storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-finalstop-')),controller=new AbortController();let mutation,roles=[];
 try{await assert.rejects(runCapacityOwnedFactory({entryDirectory,signal:controller.signal,onProgress:event=>{if(event.kind!=='final-runtime-ready'||mutation)return;mutation=(async()=>{({roles}=await finalOwnedRoles());for(const pid of roles)process.kill(pid,'SIGSTOP');controller.abort();})();mutation.catch(()=>controller.abort());},job:{target:{profile:'selfcheck',purpose:'run',binDirectory,libraryDirectory,storageRoot},execution:{}}}));await mutation;assert.equal(roles.length,2);for(const pid of roles)assert.equal(await runningPid(pid),false);assert.deepEqual(await readdir(storageRoot),[]);}
 finally{for(const pid of roles)if(await runningPid(pid))try{process.kill(pid,'SIGCONT');process.kill(pid,'SIGKILL');}catch{}await rm(storageRoot,{recursive:true,force:true});}
});
test('controller SIGKILL with both final roles stopped leaves no detached process or lab',real,async()=>{
 const storageRoot=await mkdtemp(join(tmpdir(),'harbor-factory-finaldisconnect-'));let ownerPid,roles=[],child;
 try{const script=`import {readFile} from 'node:fs/promises';import {runCapacityOwnedFactory} from './tools/capacity/preparation-context.mjs';process.once('message',configuration=>{void runCapacityOwnedFactory({...configuration,onProgress:async event=>{if(event.kind!=='final-runtime-ready')return;let ownerPid;for(const pid of (await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\\s+/).filter(Boolean))try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('--owned-capacity-preparation'))ownerPid=Number(pid);}catch{}const roles=[];for(const pid of (await readFile('/proc/'+ownerPid+'/task/'+ownerPid+'/children','utf8')).trim().split(/\\s+/).filter(Boolean))try{if((await readFile('/proc/'+pid+'/cmdline','utf8')).includes('--owned-capacity-child')){roles.push(Number(pid));process.kill(Number(pid),'SIGSTOP');}}catch{}process.send({ownerPid,roles});}}).then(()=>process.exit(0),()=>process.exit(1));});`;
 child=spawn(process.execPath,['--input-type=module','-e',script],{cwd:process.cwd(),stdio:['ignore','ignore','ignore','ipc']});child.on('message',message=>{ownerPid=message.ownerPid;roles=message.roles;});child.send({entryDirectory,job:{target:{profile:'selfcheck',purpose:'run',binDirectory,libraryDirectory,storageRoot},execution:{}}});await waitUntil(()=>ownerPid,Boolean);assert.equal(roles.length,2);child.kill('SIGKILL');await waitUntil(()=>runningPid(ownerPid),value=>!value,30000);for(const pid of roles)assert.equal(await runningPid(pid),false);assert.deepEqual(await readdir(storageRoot),[]);
 }finally{if(child?.exitCode===null&&child?.signalCode===null)child.kill('SIGKILL');for(const pid of roles)if(await runningPid(pid))try{process.kill(pid,'SIGCONT');process.kill(pid,'SIGKILL');}catch{}if(!ownerPid||!await runningPid(ownerPid))await rm(storageRoot,{recursive:true,force:true});}
});

test('runtime producer rejects copied factory, allocation handles and observed source DTOs',async()=>{
 const runtime=await import('../tools/capacity/formal-runtime-proof.mjs');for(const phase of runtime.capacityProofPhases){const binding=Object.freeze({phase,nonce:'copied',epoch:1});await assert.rejects(runtime.prepareCapacityProof(binding,{phase}),/proof_not_owned/);await assert.rejects(runtime.inspectCapacityProof(Object.freeze({}),binding,{phase}),/capability_not_owned/);}await assert.rejects(runtime.launchCapacityRuntimeProcess(Object.freeze({passed:true,formalQualified:true}),{}),/launch_not_owned/);await assert.rejects(runtime.inspectCapacityRuntimePair({gateway:{child:{}},worker:{child:{}}}),/pair_not_owned/);assert.throws(()=>runtime.inspectCapacityRuntimeObservationLease(Object.freeze({passed:true,runtimeBindingVerified:true}),{}),/observation_not_owned/);
});
