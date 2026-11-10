import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {mkdir,writeFile,readFile,lstat,readdir} from 'node:fs/promises';
import {readFileSync} from 'node:fs';
import {setTimeout as delay} from 'node:timers/promises';
import {join} from 'node:path';
import {once} from 'node:events';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,isOwnedMaterialRoot} from '../tools/capacity/owned-runtime.mjs';
import {datasetManifest,createDatasetDatabase,seedDataset,seedDatasetContexts,fixtureDatabase,verifyDataset,fixtureId} from '../tools/capacity/dataset.mjs';
import {createCapacityRestoreAssessment,isObservedRestoreAssessment,readObservedCapacityRestoreWindow} from '../tools/capacity/restore-assessment.mjs';
import {validateRollback} from '../postgres-recovery.mjs';
import {summarizeCapacityCoverage} from '../tools/capacity/coverage-adapter.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const integration={skip:!binDirectory?'需显式本任务私有 PostgreSQL18 工具路径':false,timeout:180000};
const roles=['configuration','key_encryption','recording_key','device_material','account_material','metering_spool','recording_spool'];
async function setup(){
  const lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:randomBytes(32)}}),admin=new PostgresStore({connection:lab.connection}),manifest=datasetManifest({profile:'selfcheck',metadata30d:50,metadata7d:20,bodyRequests:32,batchSize:16}),recordingKey=randomBytes(32);let fixture;
  try{fixture=await createDatasetDatabase(admin,manifest,{connectionFor:name=>({connection:{...lab.connection,database:name}}),encryptionKey:randomBytes(32),recordingKey,laboratory:lab});await seedDataset(fixture.store,manifest,{recordingKey});return {lab,admin,manifest,recordingKey,fixture,close:async()=>{if(!fixture.store.pool.ended)await fixture.store.close();await admin.close();await lab.close();}};}catch(error){await fixture?.store.close();await admin.close();await lab.close();throw error;}
}
async function materials(lab){const root=await lab.createMaterialRoot('server-materials'),requirements=[];for(const role of roles){await writeFile(join(root.path,role),'synthetic-'+role,{mode:0o600});requirements.push({role,root,relative:role,kind:'file'});}return {root,requirements};}
// Controlled process doubles exercise failure qualification only. Real gateway,
// worker and device recovery are verified by the business adapter integration.
async function processes(lab){
  const source=`const http=require('node:http');const s=http.createServer((q,r)=>{r.setHeader('content-type','application/json');r.end(JSON.stringify({ready:true}));});s.listen(0,'127.0.0.1',()=>process.send({origin:'http://127.0.0.1:'+s.address().port}));`;
  const gateway=lab.runtime.start(process.execPath,['-e',source],{role:'gateway',stdio:['ignore','pipe','pipe','ipc']}),worker=lab.runtime.start(process.execPath,['-e','setInterval(()=>{},1000)'],{role:'worker'}),[ready]=await once(gateway,'message');return {gateway:{child:gateway,ready},worker:{child:worker,command:async()=>({})}};
}
test('recovery laboratory physically restores the actual fixture, cluster identity, archived point and private server materials',integration,async()=>{
  const owned=await setup();const {lab,fixture,manifest,recordingKey}=owned;let restoredStore;
  try{
    const {root}=await materials(lab);assert.equal(isOwnedMaterialRoot(lab,root),true);assert.equal(isOwnedMaterialRoot(lab,{...root}),false);await assert.rejects(lab.createMaterialRoot('server-materials'),/material_root_invalid/);
    const artifactDirectory=join(lab.directory,'backup'),destination=join(lab.directory,'recovered');await mkdir(artifactDirectory,{mode:0o700});await mkdir(destination,{mode:0o700});
    await fixture.store.query('CREATE TABLE harbor_capacity.physical_probe(id TEXT PRIMARY KEY)');await lab.recovery.baseBackup({artifactDirectory,additionalRoots:[{id:root.id,source:root.path}]});
    await fixture.store.run("INSERT INTO harbor_capacity.physical_probe VALUES('included')");const point=await lab.recovery.mark();await fixture.store.run("INSERT INTO harbor_capacity.physical_probe VALUES('excluded')");await lab.recovery.flushWal();const oldPid=lab.process.pid,oldData=lab.dataDirectory,identity=(await fixture.store.get('SELECT system_identifier::text value FROM pg_control_system()')).value;
    await fixture.store.close();await lab.recovery.faultSource();const restored=await lab.recovery.recover({artifactDirectory,destination,point,databaseName:fixtureDatabase(manifest)});assert.notEqual(lab.process.pid,oldPid);assert.notEqual(lab.dataDirectory,oldData);assert.equal(lab.runtime.proof(lab.process).role,'database');
    restoredStore=new PostgresStore({connection:{...lab.connection,database:fixtureDatabase(manifest)}});assert.equal((await restoredStore.get('SELECT system_identifier::text value FROM pg_control_system()')).value,identity);assert.deepEqual((await restoredStore.all('SELECT id FROM harbor_capacity.physical_probe')).map(row=>row.id),['included']);assert.equal((await verifyDataset(restoredStore,manifest,{recordingKey})).verified,true);
    assert.equal(await readFile(join(destination,root.id,'key_encryption'),'utf8'),'synthetic-key_encryption');assert.equal((await lstat(join(destination,root.id,'key_encryption'))).mode&0o077,0);
    assert.equal((await validateRollback({recovery:restored})).deploymentAllowed,false);await assert.rejects(validateRollback({recovery:{...restored}}),{code:'recovery_evidence_required'});await assert.rejects(lab.recovery.recover({artifactDirectory,destination,point}),/state_invalid/);
  }finally{await restoredStore?.close();await owned.close();}
});
test('restore assessment refuses forged ownership and missing checkpoints without killing owned processes',integration,async()=>{
  const owned=await setup();let controller;try{const {lab,manifest}=owned,{requirements}=await materials(lab),hooks={processes:()=>({}),restartRuntime:async()=>({}),verifyRecovered:async()=>({})};await assert.rejects(createCapacityRestoreAssessment({recordingKey:owned.recordingKey,laboratory:{...lab},manifest,requirements,hooks}),/configuration/);await assert.rejects(createCapacityRestoreAssessment({recordingKey:owned.recordingKey,laboratory:lab,manifest,requirements:requirements.map(row=>({...row,root:{...row.root}})),hooks}),/material_requirements/);controller=await createCapacityRestoreAssessment({recordingKey:owned.recordingKey,laboratory:lab,manifest,requirements,hooks});await assert.rejects(controller.run(),/checkpoint_required/);assert.equal(lab.runtime.proof(lab.process).role,'database');assert.equal(controller.evidence(),null);assert.equal(isObservedRestoreAssessment({}),false);}finally{await controller?.close();await owned.close();}
});
for(const businessVerified of [false,true,'cancel'])test('physical restoration keeps formal gate incomplete; business probes '+businessVerified,integration,async t=>{
  const owned=await setup();let controller;const aborter=new AbortController(),cancelled=businessVerified==='cancel',qualified=businessVerified===true;try{const {lab,manifest}=owned,{requirements}=await materials(lab);const cluster=(await owned.fixture.store.get('SELECT system_identifier::text value FROM pg_control_system()')).value;let current=await processes(lab);const actualNow=performance.now.bind(performance),actualWall=Date.now.bind(Date);let offset=0,wallOffset=0;const clock=qualified?t.mock.method(performance,'now',()=>actualNow()+offset):null,wall=qualified?t.mock.method(Date,'now',()=>actualWall()+wallOffset):null;controller=await createCapacityRestoreAssessment({recordingKey:owned.recordingKey,laboratory:lab,manifest,requirements,hooks:{beforeFault:qualified?async()=>{assert.equal((await owned.fixture.store.get('SELECT 1 n')).n,1);offset+=90000;wallOffset+=45000;}:undefined,processes:()=>current,restartRuntime:async()=>current=await processes(lab),verifyRecovered:async()=>{if(cancelled)aborter.abort();return {originalEmployeeKey:true,upstreamAuthenticationMaterial:true,bodyDecryption:businessVerified!==false,deviceEnforcement:true,accounting:true,noDuplicateDispatch:true};}}});await controller.prepare();await controller.checkpoint();const result=await controller.run({signal:aborter.signal});clock?.mock.restore();wall?.mock.restore();if(qualified){assert.ok(result.timing.coverageClosureEndedAt-result.timing.coverageClosureStartedAt>=90000);assert.ok(result.timing.failedAt>=result.timing.coverageClosureEndedAt);assert.ok(result.hostRestoreRpo.rpoMs>=90000,'pre-fault wait increases age of the recovered checkpoint');assert.equal(result.hostRestoreRpo.latencyMs,result.timing.hostReadyAt-result.timing.failedAt);assertObservedCoverage(result,manifest,cluster);}assert.equal(result.hostRestoreRpo.passed,qualified?null:false);assert.equal(result.hostRestoreRpo.qualification.passed,qualified);assert.equal(result.hostRestoreRpo.qualification.materialFilesVerified,true);assert.equal(result.hostRestoreRpo.qualification.checks.bodyDecryption,businessVerified!==false);assert.equal(result.formal,false);if(!qualified){result.hostRestoreRpo.qualification.passed=true;assert.equal(readObservedCapacityRestoreWindow(result,{manifestHash:manifest.manifestHash,sourceCluster:cluster,sourceDatabase:fixtureDatabase(manifest)}),null,'changing a failed public report cannot authorize a gap');}assert.equal(isObservedRestoreAssessment(result),true);assert.equal(result.timing.hostReadyAt!==null,qualified);assert.equal(result.hostRestoreRpo.qualification.sourceFilesUnavailable,!cancelled);await assert.rejects(lstat(lab.recovery.originalDataDirectory),{code:'ENOENT'});assert.ok(!(await readdir(lab.directory)).some(name=>name.includes('.lost-')));assert.ok(result.timing.checkpointArchivedAt<=result.timing.failedAt);}finally{await controller?.close();await owned.close();}
});

test('a restored original seed corruption rejects before restarting gateway or worker',integration,async()=>{
 const owned=await setup();let controller,restarted=false,probed=false;
 try{
  const {lab,manifest,recordingKey,fixture}=owned,{requirements}=await materials(lab),current=await processes(lab);
  await fixture.store.run('UPDATE harbor.requests SET input=input+1 WHERE id=$1',fixtureId(manifest.runId,'request',manifest.bodyRequests));
  controller=await createCapacityRestoreAssessment({laboratory:lab,manifest,recordingKey,requirements,hooks:{processes:()=>current,restartRuntime:async()=>{restarted=true;return current;},verifyRecovered:async()=>{probed=true;return {};}}});
  await controller.prepare();await controller.checkpoint();const result=await controller.run();
  assert.equal(result.hostRestoreRpo.qualification.passed,false);assert.equal(result.hostRestoreRpo.qualification.failure,'capacity_restore_seed_unverified');
  assert.equal(result.hostRestoreRpo.qualification.seedDataset.verified,false);assert.equal(restarted,false);assert.equal(probed,false);assert.equal(result.timing.hostReadyAt,null);
  await assert.rejects(lstat(lab.recovery.originalDataDirectory),{code:'ENOENT'});
 }finally{await controller?.close();await owned.close();}
});

test('R2 cancellation during initdb rejects and cleans only its newly created recovery laboratory',integration,async()=>{
  const controller=new AbortController(),signal=controller.signal,add=signal.addEventListener.bind(signal);let root,observed=false;
  signal.addEventListener=(type,...args)=>{add(type,...args);if(type!=='abort'||observed)return;const children=readFileSync('/proc/self/task/'+process.pid+'/children','utf8').trim().split(/\s+/).filter(Boolean);for(const pid of children){try{const argv=readFileSync('/proc/'+pid+'/cmdline').toString().split('\0'),data=argv[argv.indexOf('--pgdata')+1];if(argv[0]===join(binDirectory,'initdb')&&data?.startsWith('/tmp/harbor-capacity-recovery-')){root=data.slice(0,-'/primary'.length);observed=true;queueMicrotask(()=>controller.abort());break;}}catch{}}};
  await assert.rejects(createPrivatePostgres({binDirectory,libraryDirectory,signal,recovery:{backupKey:randomBytes(32)}}),{name:'AbortError'});assert.equal(observed,true,'cancel inside the actual initdb command');await assert.rejects(lstat(root),{code:'ENOENT'});
});
test('R3 close waits for a physical prepare in progress and prevents publishing prepared after close',integration,async()=>{
  const owned=await setup();let controller;try{const {lab,manifest}=owned,{requirements}=await materials(lab);controller=await createCapacityRestoreAssessment({recordingKey:owned.recordingKey,laboratory:lab,manifest,requirements,hooks:{processes:()=>({}),restartRuntime:async()=>({}),verifyRecovered:async()=>({})}});let settled=false;const preparing=controller.prepare().then(value=>{settled=true;return value;},error=>{settled=true;return error;});const deadline=Date.now()+10000;while(!(await readdir(lab.directory)).some(name=>name.startsWith('physical-'))){assert.ok(Date.now()<deadline,'physical backup must start before closing');await delay(10);}assert.equal(settled,false);await controller.close();assert.equal(settled,true);assert.equal((await preparing).message,'capacity_restore_closed');assert.equal(controller.status().state,'closed');await assert.rejects(controller.prepare(),/state_invalid/);assert.equal(lab.runtime.proof(lab.process).role,'database');}finally{await controller?.close();await owned.close();}
});


test('pre-fault observation failure records no injected fault and keeps the original source running',integration,async()=>{
 const owned=await setup();let controller;
 try{
  const {lab,manifest,recordingKey}=owned,{requirements}=await materials(lab),current=await processes(lab),originalPid=lab.process.pid;
  controller=await createCapacityRestoreAssessment({laboratory:lab,manifest,recordingKey,requirements,hooks:{beforeFault:async()=>{throw Error('capacity_observation_refused');},processes:()=>current,restartRuntime:async()=>assert.fail('must not restart'),verifyRecovered:async()=>assert.fail('must not probe')}});
  await controller.prepare();await controller.checkpoint();const result=await controller.run();assert.equal(result.hostRestoreRpo.qualification.passed,false);assert.equal(result.hostRestoreRpo.qualification.failure,'capacity_observation_refused');assert.equal(result.timing.failedAt,null);assert.equal(result.hostRestoreRpo.rpoMs,null);assert.equal(result.hostRestoreRpo.latencyMs,null);assert.ok(result.timing.coverageClosureEndedAt>=result.timing.coverageClosureStartedAt);assert.equal(lab.process.pid,originalPid);assert.equal(lab.runtime.proof(current.gateway.child).role,'gateway');assert.equal((await owned.fixture.store.get('SELECT 1 n')).n,1);
 }finally{await controller?.close();await owned.close();}
});


function assertObservedCoverage(evidence,manifest,sourceCluster){
 const timing=evidence.timing,wall=timing.coverageWall,identity={manifestHash:manifest.manifestHash,sourceCluster,sourceDatabase:fixtureDatabase(manifest)};
 const window=readObservedCapacityRestoreWindow(evidence,identity);assert.ok(window);assert.equal(window.failedAt,wall.failedAt);assert.equal(window.clock,'controller-wall-ms');assert.ok(Math.abs(window.failedAt-timing.failedAt)>40000,'classification must not compare independent clock domains');
 assert.equal(readObservedCapacityRestoreWindow({...evidence},identity),null);assert.equal(readObservedCapacityRestoreWindow(evidence,{...identity,sourceCluster:'other-synthetic-cluster'}),null);
 // Synthetic scale DTOs test the pure classifier only; the actual target remains
 // selfcheck and cannot pass the independent profile/resource/host-recovery gates.
 const before={...identity,protected:true,sealed:true,available:true,method:'monotone-seed-loss-lower-bound',from:wall.from-1000,through:wall.from,metadata7d:1260000,metadata30d:5400000,pricingRevision:'synthetic-price'},after={...before,passed:true,from:wall.readyAt,through:wall.readyAt+1000,segments:[before]};
 const sample=at=>({at,metadata7d:1260000,metadata30d:5400000,pricing_revision:'synthetic-price'}),fault={at:Math.floor((wall.failedAt+wall.readyAt)/2),verified:false},samples=[sample(before.through-1),fault,sample(after.through)];
 const summarize=(rows=samples,receipt=evidence,proof=after,profile='formal')=>summarizeCapacityCoverage(rows,{profile,continuousCoverage:proof,recoveryEvidence:receipt});
 const result=summarize();assert.equal(result.passed,true);assert.equal(result.continuous,false);assert.equal(result.evaluationScope,'normal-and-observed-recovery');assert.equal(result.faultSamples,1);assert.equal(result.unknownSamples,1);assert.equal(result.samples,3);assert.equal(result.uncoveredSamples,0);assert.equal(result.minimumMetadata7d,null);
 assert.equal(summarize(samples,{...evidence}).passed,null);assert.equal(summarize(samples,evidence,after,'selfcheck').passed,null);assert.equal(evidence.hostRestoreRpo.passed,null);assert.equal(evidence.formal,false);
 const preFault={at:Math.floor((wall.from+wall.failedAt)/2),verified:false};assert.ok(preFault.at<wall.failedAt);const unjustified=summarize([samples[0],preFault,samples[2]]);assert.equal(unjustified.passed,null);assert.equal(unjustified.uncoveredSamples,1,'pre-fault observation delays are not fault exemptions');
 assert.equal(summarize([samples[0],fault,{...samples[2],metadata7d:1}]).passed,false);assert.equal(summarize([samples[0],fault,{...samples[2],pricing_revision:'other-price'}]).passed,false);
 const foreign='f'.repeat(64);evidence.hostRestoreRpo.qualification.seedDataset.manifestHash=foreign;assert.equal(readObservedCapacityRestoreWindow(evidence,{...identity,manifestHash:foreign}),null,'tampering with public corpus identity cannot move the restore to another corpus');assert.ok(readObservedCapacityRestoreWindow(evidence,identity));
}


test('restored historical context loss rejects before restarting application processes',integration,async()=>{
 const owned=await setup();let controller,restarted=false,probed=false;
 try{
  const {lab,manifest,recordingKey,fixture}=owned,{requirements}=await materials(lab),current=await processes(lab);await seedDatasetContexts(fixture.store,manifest,{recordingKey});
  await fixture.store.run('DELETE FROM harbor.recording_context_snapshots WHERE request_id=$1',fixtureId(manifest.runId,'request',0));
  controller=await createCapacityRestoreAssessment({laboratory:lab,manifest,recordingKey,recordingContext:true,requirements,hooks:{processes:()=>current,restartRuntime:async()=>{restarted=true;return current;},verifyRecovered:async()=>{probed=true;return {};}}});await controller.prepare();await controller.checkpoint();const result=await controller.run();
  assert.equal(result.hostRestoreRpo.qualification.passed,false);assert.equal(result.hostRestoreRpo.qualification.failure,'capacity_restore_context_unverified');assert.equal(result.hostRestoreRpo.qualification.seedDataset.verified,true);assert.equal(result.hostRestoreRpo.qualification.seedDataset.contextCoverage.verified,false);assert.ok(result.hostRestoreRpo.qualification.seedDataset.contextCoverage.missing.includes('context_source_mismatch'));assert.equal(restarted,false);assert.equal(probed,false);assert.equal(result.timing.hostReadyAt,null);
 }finally{await controller?.close();await owned.close();}
});
