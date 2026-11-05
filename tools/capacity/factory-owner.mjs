import {stat,readFile} from 'node:fs/promises';
import {join,isAbsolute} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createTarget,capacityTargetRunLifetime} from './gateway-adapter.mjs';
import {runCapacityExecution,apiCoverageDuration} from './execution.mjs';
import {capacityPlan} from './scenarios.mjs';
import {capacityRunLifetime,capacityRunBudget} from './certificate-lifetime.mjs';
import {uiAcceptancePlan} from './ui-acceptance.mjs';
import {verifyDataset} from './dataset.mjs';

const invalid=()=>{throw Error('capacity_factory_owner_configuration');};
const safeKeys=(value,allowed)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.includes(key));
export function validateCapacityFactoryJob(job){
 if(!safeKeys(job,['target','execution'])||!safeKeys(job.target,['profile','purpose','binDirectory','libraryDirectory','storageRoot','runLifetimeMs','windowsHelper','employeeCount','resourceDelegatedRoot','diskBudget'])||!safeKeys(job.execution??{},['executeLong','browserLauncher','uiMaxDurationMs','apiMaxDurationMs']))invalid();
 const {target,execution={}}=job;capacityPlan(target.profile??'selfcheck');if(!['run','seed','verify'].includes(target.purpose??'run'))invalid();
 for(const key of ['binDirectory','libraryDirectory','storageRoot','resourceDelegatedRoot'])if(target[key]!==undefined&&(typeof target[key]!=='string'||!isAbsolute(target[key])))invalid();
 if(execution.browserLauncher!==undefined&&(typeof execution.browserLauncher!=='string'||!isAbsolute(execution.browserLauncher)))invalid();
 if(execution.executeLong!==undefined&&typeof execution.executeLong!=='boolean')invalid();
 const formal=target.profile==='formal',run=(target.purpose??'run')==='run';if(formal&&run&&execution.executeLong!==true)throw Error('capacity_formal_execution_not_authorized');
 capacityRunLifetime(target.runLifetimeMs,{required:formal&&run});if(run){if(formal&&!execution.browserLauncher)throw Error('capacity_browser_launcher_required');apiCoverageDuration(execution.apiMaxDurationMs,{required:formal});if(execution.browserLauncher)uiAcceptancePlan({profile:target.profile??'selfcheck',maxDurationMs:execution.uiMaxDurationMs});capacityRunBudget({plan:capacityPlan(target.profile??'selfcheck'),runLifetimeMs:target.runLifetimeMs,apiMaxDurationMs:execution.apiMaxDurationMs,uiMaxDurationMs:execution.browserLauncher?execution.uiMaxDurationMs:undefined});}
 return structuredClone(job);
}
async function processIdentity(pid){const raw=await readFile('/proc/'+pid+'/stat','utf8'),fields=raw.slice(raw.lastIndexOf(')')+2).split(' ');return {parent:Number(fields[1]),state:fields[0],started:fields[19]};}

// Invoked only by the fixed native subreaper owner. Target, allocator, browser
// launcher, private proof capabilities and runner all stay in this process.
export async function createInnerCapacityFactoryJob(input,signal,progress=()=>{},ownerAuthority){
 const job=validateCapacityFactoryJob(input),controller=new AbortController();const cancel=()=>controller.abort(Error('capacity_cancelled'));signal?.addEventListener('abort',cancel,{once:true});if(signal?.aborted)cancel();
 let lastProgress=0,progressCount=0;const reportProgress=event=>{const kind=event?.kind;if(!['preparation-ready','metadata','body','final-runtime-ready','seed-indexes-ready'].includes(kind)||progressCount>=256)return;const now=performance.now();if(['metadata','body'].includes(kind)&&now-lastProgress<1000)return;lastProgress=now;progressCount++;progress({kind,...Object.fromEntries(['completed','total','pending'].filter(key=>Number.isSafeInteger(event[key])&&event[key]>=0).map(key=>[key,event[key]]))});};
 let target,allocator,running,closePromise,forceTimer,forceWork,closed=false,phase='idle',pgExecutable;
 const known=new Map();

 const remember=async()=>{if(!pgExecutable)return;const children=(await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);for(const pid of children)try{const identity=await processIdentity(pid),binary=await stat('/proc/'+pid+'/exe');if(identity.parent===process.pid&&binary.dev===pgExecutable.dev&&binary.ino===pgExecutable.ino&&binary.uid===pgExecutable.uid)known.set(pid,identity);}catch{}};
 const wakeOrKill=async mode=>{await remember();for(const [pid,expected] of known)try{const actual=await processIdentity(pid);if(actual.started!==expected.started||['Z','X','x'].includes(actual.state))continue;process.kill(pid,'SIGCONT');if(mode==='kill')process.kill(pid,'SIGKILL');}catch{}};
 const revoke=()=>{cancel();void wakeOrKill('wake').catch(()=>{});if(!forceTimer&&!forceWork)forceTimer=setTimeout(()=>{forceTimer=null;forceWork=finishPg();forceWork.catch(()=>{});},5000);};
 const finishPg=async()=>{const deadline=performance.now()+5000;for(;;){await wakeOrKill('kill');await new Promise(resolve=>setImmediate(resolve));await remember();let live=false;for(const [pid,expected] of known)try{const actual=await processIdentity(pid);if(actual.started===expected.started&&!['Z','X','x'].includes(actual.state))live=true;}catch{}if(!live)return;if(performance.now()>deadline)throw Error('capacity_factory_cleanup_unknown');await new Promise(resolve=>setTimeout(resolve,10));}};
 const close=()=>closePromise??=(async()=>{closed=true;phase='closing';revoke();const errors=[];const step=async operation=>{try{await operation();}catch{errors.push(true);}};try{await running?.catch(()=>{});await step(()=>target?.close());await step(()=>allocator?.close());await step(finishPg);await step(()=>forceWork);if(errors.length)throw Error('capacity_factory_cleanup_unknown');return {resourcesReleased:true};}finally{clearTimeout(forceTimer);signal?.removeEventListener('abort',cancel);phase='closed';}})();
 return {close,cancel:revoke,inspect:async()=>({scope:'owned-capacity-factory-job',phase,ownerPid:process.pid}),
  run:()=>{
   if(running||closed)throw Error('capacity_factory_job_already_used');phase='running';running=(async()=>{
    let result;try{
     controller.signal.throwIfAborted();if(job.target.resourceDelegatedRoot){const {createCapacityResourceAllocator}=await import('./resource-allocator.mjs');allocator=await createCapacityResourceAllocator({delegatedRoot:job.target.resourceDelegatedRoot});}
     let launchBrowser;const execution=job.execution??{};if(execution.browserLauncher){const module=await import(pathToFileURL(execution.browserLauncher).href);if(typeof module.launchBrowser!=='function')throw Error('capacity_browser_launcher_invalid');launchBrowser=module.launchBrowser;}
     const {resourceDelegatedRoot,...configuration}=job.target;target=await createTarget({...configuration,signal:controller.signal,resourceAllocator:allocator,ownerAuthority,onAllocation:async()=>{if(job.target.binDirectory)pgExecutable=await stat(join(job.target.binDirectory,'postgres'));},onProgress:event=>{controller.signal.throwIfAborted();reportProgress(event);}});
     if((job.target.purpose??'run')==='run')result=await runCapacityExecution({plan:target.plan,target,preflight:target.preflight,signal:controller.signal,executeLong:execution.executeLong,launchBrowser,uiMaxDurationMs:execution.uiMaxDurationMs,apiMaxDurationMs:execution.apiMaxDurationMs,runLifetimeMs:job.target.runLifetimeMs});
     else result={...await verifyDataset(target.datasetStore,target.datasetManifest,{recordingKey:target.recordingKey}),laboratoryLifecycle:{persistence:'ephemeral',destroyedAfterCommand:true,continuation:'new_lab_each_command'}};
     const lifetime=capacityTargetRunLifetime(target);if(result?.verdict&&lifetime?.evidence().deadlineReached)result={...result,runLifetime:{...result.runLifetime,...lifetime.evidence()},verdict:{...result.verdict,status:result.verdict.status==='failed'?'failed':'incomplete',incomplete:[...new Set([...result.verdict.incomplete,'capacity_run_lifetime_exhausted'])]}};
     return result;
    }finally{try{await target?.close();}finally{await allocator?.close();phase=closed?'closing':'finished';}}
   })();return running;
  }
 };
}
