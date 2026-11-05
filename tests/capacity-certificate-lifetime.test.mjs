import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,rm,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {X509Certificate} from 'node:crypto';
import {registerHooks} from 'node:module';
import {credentialFingerprints} from '../tools/capacity/http-client.mjs';
import {capacityRunLifetime,capacityRunBudget,createCapacityCertificateLifetime} from '../tools/capacity/certificate-lifetime.mjs';
import {syntheticCertificates} from '../tools/capacity/selfcheck.mjs';
import {capacityPlan} from '../tools/capacity/scenarios.mjs';
import {createTarget,capacityTargetRunLifetime,ownedCapacityTarget} from '../tools/capacity/gateway-adapter.mjs';
import {runCapacityExecution} from '../tools/capacity/execution.mjs';
const exec=promisify(execFile),day=86400000;

test('formal lifetime is explicit, bounded and greater than all serial declared phases',()=>{
  assert.throws(()=>capacityRunLifetime(undefined,{required:true}),/capacity_run_lifetime_required/);
  for(const value of [0,-1,NaN,null,1.5,365*day])assert.throws(()=>capacityRunLifetime(value),/capacity_run_lifetime_invalid/);
  const plan=capacityPlan(),normal=plan.scenarios.reduce((sum,scene)=>sum+scene.durationMs,0);assert.equal(normal,54200000);
  const scheduled=normal+2*day;
  assert.throws(()=>capacityRunBudget({plan,runLifetimeMs:scheduled,apiMaxDurationMs:day,uiMaxDurationMs:day}),/below_budget/);
  assert.deepEqual(capacityRunBudget({plan,runLifetimeMs:scheduled+1234,apiMaxDurationMs:day,uiMaxDurationMs:day}),{runLifetimeMs:scheduled+1234,scheduledMs:scheduled,preparationAndFinalizationAllowanceMs:1234});
  const legacy=createCapacityCertificateLifetime();assert.equal(legacy.days,1);legacy.dispose();
  const precision=createCapacityCertificateLifetime({runLifetimeMs:day});assert.equal(precision.days,2);precision.dispose();
});

test('real X509 CA and both synthetic leaves verify forty hours ahead; short CA or leaf is rejected',{timeout:30000},async t=>{
  const directory=await mkdtemp(join(tmpdir(),'harbor-lifetime-x509-')),shortDirectory=await mkdtemp(join(tmpdir(),'harbor-lifetime-short-')),lifetime=createCapacityCertificateLifetime({runLifetimeMs:70*3600000});
  try{
    assert.equal(lifetime.days,3);const certificates=await syntheticCertificates(directory,{days:lifetime.days}),short=await syntheticCertificates(shortDirectory);
    lifetime.registerAuthority(certificates.ca);lifetime.registerCertificate('server',certificates.server.cert);lifetime.registerCertificate('client',certificates.client.cert);assert.equal(lifetime.assertCoverage().checkedCertificates,3);
    const future=Math.floor((Date.now()+40*3600000)/1000);
    for(const role of ['server','client'])await exec('openssl',['verify','-CAfile',join(directory,'ca.pem'),'-attime',String(future),join(directory,role+'.pem')]);
    await assert.rejects(exec('openssl',['verify','-CAfile',join(shortDirectory,'ca.pem'),'-attime',String(future),join(shortDirectory,'server.pem')]));
    const shortCa=createCapacityCertificateLifetime({runLifetimeMs:70*3600000});try{assert.throws(()=>shortCa.registerAuthority(short.ca),/lifetime_insufficient/);}finally{shortCa.dispose();}
    // A correctly signed short leaf under the long-lived CA must still fail.
    await exec('openssl',['x509','-req','-in',join(directory,'client.csr'),'-CA',join(directory,'ca.pem'),'-CAkey',join(directory,'ca.key'),'-set_serial','17','-days','1','-out',join(directory,'short.pem')]);
    const {readFile}=await import('node:fs/promises'),shortLeaf=await readFile(join(directory,'short.pem'));assert.throws(()=>lifetime.registerCertificate('short',shortLeaf),/lifetime_insufficient/);
    assert.equal(lifetime.assertCoverage().checkedCertificates,3);
    assert.equal(lifetime.assertRegisteredCertificate('server',certificates.server.cert).checkedCertificates,3);assert.throws(()=>lifetime.assertRegisteredCertificate('server',short.server.cert),/material_changed/);
    const mono=performance.now();t.mock.method(performance,'now',()=>mono+60*3600000);assert.throws(()=>lifetime.assertCoverage({minimumRemainingMs:15*3600000}),/remaining_below_budget/);t.mock.restoreAll();
    t.mock.method(Date,'now',()=>future*1000);assert.throws(()=>lifetime.assertCoverage(),/lifetime_insufficient/);
  }finally{lifetime.dispose();await rm(directory,{recursive:true,force:true});await rm(shortDirectory,{recursive:true,force:true});}
});

test('deadline cancellation does not wait for a callback to release, and remains observable after dispose',{timeout:5000},async()=>{
  const lifetime=createCapacityCertificateLifetime({runLifetimeMs:25});
  const deadline=performance.now()+1000;while(!lifetime.signal.aborted){assert.ok(performance.now()<deadline);await new Promise(resolve=>setTimeout(resolve,2));}
  assert.equal(lifetime.evidence().deadlineReached,true);assert.equal(lifetime.signal.reason.message,'capacity_run_lifetime_exhausted');lifetime.dispose();assert.equal(lifetime.evidence().deadlineReached,true);
});

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
for(const windowsHelper of [false,true])test('real PG/mTLS device lifetime and candidate leaves'+(windowsHelper?' with Windows CNG':''),{skip:!binDirectory?'requires private PG18':windowsHelper&&process.env.HARBOR_TEST_WINDOWS_DEVICE!=='1'?'requires explicit Windows helper':false,timeout:180000},async()=>{
  let target;
  try{
    target=await createTarget({binDirectory,libraryDirectory,employeeCount:1,windowsHelper,runLifetimeMs:70*3600000});
    const lifetime=capacityTargetRunLifetime(target);assert.equal(lifetime.days,3);assert.equal(lifetime.assertCoverage().checkedCertificates,4);
    const row=await target.store.get('SELECT certificate FROM harbor.device_credentials WHERE revoked IS NULL LIMIT 1'),certificate=new X509Certificate(row.certificate);assert.ok(Date.parse(certificate.validTo)>Date.now()+69*3600000);
    const future=Math.floor((Date.now()+40*3600000)/1000),directory=await mkdtemp(join(tmpdir(),'harbor-device-lifetime-'));
    try{await writeFile(join(directory,'ca.pem'),target.certificateAuthority,{mode:0o600});await writeFile(join(directory,'device.pem'),row.certificate,{mode:0o600});await exec('openssl',['verify','-CAfile',join(directory,'ca.pem'),'-attime',String(future),join(directory,'device.pem')]);}finally{await rm(directory,{recursive:true,force:true});}
    const scope=ownedCapacityTarget(target);await scope.getApiCoverageInventory({prepare:true,signal:lifetime.signal});assert.equal(lifetime.assertCoverage().checkedCertificates,6,'two new API candidate leaves registered');
    await assert.rejects(runCapacityExecution({target,plan:target.plan,runLifetimeMs:day}),/target_mismatch/);
    const report=await runCapacityExecution({target,plan:{...target.plan,scenarios:[{id:'lifetime-real',durationMs:200,modelQps:5,adminQps:5,employees:1,administrators:1,upstreamMs:[30,40],payloadBytes:1024}]},observeNormal:false});
    assert.equal(report.runLifetime.deadlineReached,false);assert.equal(report.runLifetime.scheduledMs,200);assert.ok(report.runLifetime.preparationAndFinalizationAllowanceMs>0);assert.notEqual(report.verdict.status,'passed');
  }finally{await target?.close();}
});

test('CLI rejects missing, malformed and undersized full budgets before importing allocator',{timeout:15000},async()=>{
  const directory=await mkdtemp(join(tmpdir(),'harbor-lifetime-cli-')),adapter=join(directory,'adapter.mjs'),launcher=join(directory,'launcher.mjs'),marker=join(directory,'allocated');
  try{
    await writeFile(adapter,'import {writeFileSync} from "node:fs";writeFileSync('+JSON.stringify(marker)+',"unexpected");export async function createTarget(){}');await writeFile(launcher,'export async function launchBrowser(){}');
    const base=['tools/capacity/cli.mjs','run','--execute-long','--adapter',adapter,'--browser-launcher',launcher,'--ui-max-duration-ms','12000000','--api-max-duration-ms','86400000'];
    for(const [extra,code] of [[[],'required'],[['--run-lifetime-ms','1.2'],'invalid'],[['--run-lifetime-ms','152600000'],'below_budget']])await assert.rejects(exec(process.execPath,[...base,...extra]),error=>error.stderr.includes('capacity_run_lifetime_'+code));
    const {stat}=await import('node:fs/promises');await assert.rejects(stat(marker),{code:'ENOENT'});
  }finally{await rm(directory,{recursive:true,force:true});}
});

test('execution waits for actual callback return after deadline and cannot keep a passed verdict',{timeout:5000},async()=>{
  const lifetime=createCapacityCertificateLifetime({runLifetimeMs:80}),entry=new URL('../tools/capacity/execution.mjs?lifetime-deadline',import.meta.url).href;
  const directory=await mkdtemp(join(tmpdir(),'harbor-deadline-report-'));let done=false,hooks;
  try{
    const certificates=await syntheticCertificates(directory,{days:lifetime.days});
    // Start a fresh short lifetime after certificate preparation.
    lifetime.dispose();const active=createCapacityCertificateLifetime({runLifetimeMs:80});active.registerAuthority(certificates.ca);active.registerCertificate('server',certificates.server.cert);
    const key='__harborLifetimeTest_'+Date.now();globalThis[key]=active;
    hooks=registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&specifier==='./gateway-adapter.mjs')return {shortCircuit:true,url:'data:text/javascript,'+encodeURIComponent('export function capacityTargetRunLifetime(){return globalThis['+JSON.stringify(key)+'];}')};if(context.parentURL===entry&&specifier==='./runner.mjs')return {shortCircuit:true,url:'data:text/javascript,'+encodeURIComponent('export async function runCapacitySuite(options){await options.onProgress({kind:"synthetic-lifetime-callback"});return {profile:"formal",verdict:{status:"passed",failures:[],incomplete:[]}};}')};return next(specifier,context);}});
    try{
      const {runCapacityExecution:run}=await import(entry);const report=await run({observeNormal:false,plan:{profile:'selfcheck',scenarios:[]},onProgress:async()=>{await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('deadline not delivered')),1000);active.signal.addEventListener('abort',()=>{clearTimeout(timer);resolve();},{once:true});});assert.equal(done,false);await new Promise(resolve=>setTimeout(resolve,5));done=true;}});
      assert.equal(done,true);assert.equal(report.verdict.status,'incomplete');assert.ok(report.verdict.incomplete.includes('capacity_run_lifetime_exhausted'));assert.equal(report.runLifetime.deadlineReached,true);
    }finally{active.dispose();delete globalThis[key];}
  }finally{hooks?.deregister();lifetime.dispose();await rm(directory,{recursive:true,force:true});}
});

test('physical recovery preserves the same long-lived CA, issuer TTL and budget identity',{skip:!binDirectory?'requires private PG18':false,timeout:180000},async()=>{
  let target,primary;const diagnostics=[];try{
    target=await createTarget({binDirectory,libraryDirectory,employeeCount:1,recovery:true,runLifetimeMs:70*3600000,onRestoreDiagnostic:row=>diagnostics.push(row)});const lifetime=capacityTargetRunLifetime(target),before=lifetime.evidence(),restore=ownedCapacityTarget(target).restoreAssessment;await restore.prepare();await restore.checkpoint();const result=await restore.run();assert.ok(diagnostics.length>0);assert.ok(diagnostics.every(row=>Object.keys(row).every(key=>['stage','event','category','code'].includes(key))));assert.equal(result.hostRestoreRpo.qualification.passed,true);assert.strictEqual(capacityTargetRunLifetime(target),lifetime);assert.equal(lifetime.evidence().startedAt,before.startedAt);assert.equal(lifetime.days,3);assert.equal(lifetime.assertCoverage().checkedCertificates,4);
    await ownedCapacityTarget(target).getApiCoverageInventory({prepare:true,signal:lifetime.signal});assert.equal(lifetime.assertCoverage().checkedCertificates,6);const leaves=await target.store.all('SELECT certificate FROM harbor.device_credentials WHERE revoked IS NULL');assert.ok(leaves.every(row=>Date.parse(new X509Certificate(row.certificate).validTo)>Date.now()+69*3600000));
  }catch(error){primary=error;error.restoreDiagnostics=diagnostics;throw error;}finally{try{await target?.close();}catch(error){if(primary)primary.cleanup={codes:error.codes,cleanupComplete:error.cleanupComplete};else throw error;}}
});

test('real normal runner retains cutoff reports before first scene, between scenes, in flight, prior failure and missing collection',{timeout:15000},async t=>{
  const directory=await mkdtemp(join(tmpdir(),'harbor-cutoff-normal-'));
  const awaitCutoff=async signal=>{if(signal.aborted)return;await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('cutoff was not delivered')),2000);signal.addEventListener('abort',()=>{clearTimeout(timer);resolve();},{once:true});});};
  try{
    const certificates=await syntheticCertificates(directory);
    for(const mode of ['before-first','between','in-flight','prior-failure','missing-collection','unknown-start-error','unknown-progress-error'])await t.test(mode,async()=>{
      const entry=new URL('../tools/capacity/execution.mjs?real-cutoff-'+mode,import.meta.url).href,key='__harborRealCutoff_'+mode;let lifetime,hooks,callbackExited=false,probes=0,sends=0;
      hooks=registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&specifier==='./gateway-adapter.mjs')return {shortCircuit:true,url:'data:text/javascript,'+encodeURIComponent('export function capacityTargetRunLifetime(){return globalThis['+JSON.stringify(key)+'];}')};return next(specifier,context);}});
      try{
        const {runCapacityExecution:run}=await import(entry);lifetime=createCapacityCertificateLifetime({runLifetimeMs:500});globalThis[key]=lifetime;lifetime.registerAuthority(certificates.ca);lifetime.registerCertificate('server',certificates.server.cert);
        const plan=structuredClone(capacityPlan('selfcheck'));plan.scenarios=['first','second'].map(id=>({id,durationMs:200,modelQps:5,adminQps:5,employees:1,administrators:1}));
        const secret='synthetic-cutoff-key',cookie='harbor_session=synthetic-cutoff-cookie',base={success:true,complete:true,httpEnded:true,wireSent:true,durationMs:1,startedAt:0,firstContentAt:1};
        const report=await run({plan,observeNormal:false,target:{employeeActors:[{secret}],adminActors:[{headers:{cookie}}],sendModel:async()=>{sends++;if(mode==='in-flight'){await awaitCutoff(lifetime.signal);await new Promise(resolve=>setTimeout(resolve,5));callbackExited=true;}return {...base,operation:'model',method:'POST',success:mode!=='prior-failure',error:mode==='prior-failure'?'synthetic_model_failure':null,credentials:credentialFingerprints({authorization:'Bearer '+secret})};},sendAdmin:async()=>{sends++;return {...base,operation:'health',method:'GET',requestSignature:'GET /health',credentials:credentialFingerprints({cookie})};},onScenarioStart:async()=>{if(mode==='unknown-start-error'){await awaitCutoff(lifetime.signal);throw Error('private-start-error');}},runQualificationProbes:async()=>{probes++;return {completed:true,passed:true};},collectEvidence:async()=>{if(mode==='missing-collection')throw Error('private-collection-error');return {existingEvidence:{passed:true}};}},onProgress:async event=>{if(mode==='unknown-progress-error'&&event.kind==='scenario-start'){await awaitCutoff(lifetime.signal);throw Error('private-progress-error');}if(mode==='before-first'&&event.kind==='scenario-start'&&event.id==='first'||['between','prior-failure','missing-collection'].includes(mode)&&event.kind==='scenario-start'&&event.id==='second')await awaitCutoff(lifetime.signal);}});
        assert.equal(probes,0);assert.equal(report.runLifetime.deadlineReached,true);assert.notEqual(report.verdict.status,'passed');assert.ok(report.verdict.incomplete.includes('capacity_run_lifetime_exhausted'));assert.equal(report.evidence.scenarios.second.completed,false);assert.equal(report.evidence.execution.cancelled,true);
        if(['before-first','unknown-start-error','unknown-progress-error'].includes(mode))assert.equal(sends,0);else assert.ok(report.metrics.arrivals.sent>=2);
        if(mode==='in-flight')assert.equal(callbackExited,true);else if(!['before-first','unknown-start-error','unknown-progress-error'].includes(mode))assert.equal(report.evidence.scenarios.first.completed,true);
        if(mode==='unknown-start-error'){assert.equal(report.verdict.status,'failed');assert.ok(report.verdict.failures.includes('scenario_start_failed'));assert.equal(report.evidence.scenarios.first.reason,'scenario_start_failed');}
        if(mode==='unknown-progress-error'){assert.equal(report.verdict.status,'failed');assert.ok(report.verdict.failures.includes('capacity_progress_callback_failed'));}
        assert.equal(JSON.stringify(report).includes('private-start-error'),false);assert.equal(JSON.stringify(report).includes('private-progress-error'),false);
        if(mode==='prior-failure'){assert.equal(report.verdict.status,'failed');assert.ok(report.verdict.failures.some(value=>value.startsWith('正常组成功率:model')));}
        if(mode==='missing-collection'){assert.equal(report.evidence.collection.completed,false);assert.ok(report.verdict.incomplete.includes('capacity_evidence_collection_failed'));assert.equal(JSON.stringify(report).includes('private-collection-error'),false);}else assert.equal(report.evidence.existingEvidence.passed,true);
      }finally{hooks.deregister();lifetime?.dispose();delete globalThis[key];}
    });
  }finally{await rm(directory,{recursive:true,force:true});}
});
