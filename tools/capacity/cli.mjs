import {capacityRunLifetime,capacityRunBudget} from './certificate-lifetime.mjs';
import {capacityTargetRunLifetime} from './gateway-adapter.mjs';
import {readFile,writeFile,lstat} from 'node:fs/promises';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {capacityPlan} from './scenarios.mjs';
import {collectHostEvidence,collectCgroupRole,resourcePreflight,normalizeCapacityResourceLimits} from './resources.mjs';
import {runCapacitySelfcheck} from './selfcheck.mjs';
import {runCapacityExecution,apiCoverageDuration} from './execution.mjs';
import {uiAcceptancePlan} from './ui-acceptance.mjs';
import {isOwnedLaboratory} from './owned-runtime.mjs';
import {seedDataset,verifyDataset} from './dataset.mjs';

async function readJson(path){const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size>2097152)throw new Error('capacity_input_file_invalid');return JSON.parse(await readFile(path,'utf8'));}
async function main(){
  const [command,...args]=process.argv.slice(2),options={};for(let index=0;index<args.length;index++){const key=args[index];if(!key.startsWith('--')||Object.hasOwn(options,key))throw new Error('capacity_cli_arguments_invalid');if(['--execute-long','--private-pg','--windows-helper'].includes(key))options[key]=true;else{const value=args[++index];if(!value||value.startsWith('--'))throw new Error('capacity_cli_arguments_invalid');options[key]=value;}}
  let result;
  if(command==='plan')result=capacityPlan(options['--profile']??'formal');
  else if(command==='preflight'){const manifest=options['--manifest']?await readJson(options['--manifest']):{},roles={};for(const [role,path] of Object.entries(manifest.cgroups??{}))roles[role]=await collectCgroupRole(path);result=resourcePreflight({host:await collectHostEvidence(),roles,resourceLimits:manifest.resourceLimits,dataset:manifest.dataset,protocol:manifest.protocol,operations:manifest.operations??capacityPlan().operations});}
  else if(command==='selfcheck')result=await runCapacitySelfcheck({privatePg:!!options['--private-pg'],binDirectory:options['--pg-bin'],libraryDirectory:options['--pg-lib']});
  else if(['run','seed','verify'].includes(command)){
    const profile=options['--profile']??'formal',builtinFactory=options['--adapter']&&resolve(options['--adapter'])===fileURLToPath(new URL('./gateway-adapter.mjs',import.meta.url)),ownedFactory=!!builtinFactory&&(profile==='formal'||!!options['--owner-entry']);capacityPlan(profile);const resourceLimits=options['--resource-limits-file']?normalizeCapacityResourceLimits(await readJson(options['--resource-limits-file'])):undefined;if(command==='run'&&profile==='formal'&&!options['--execute-long'])throw new Error('capacity_formal_execution_not_authorized');
    let launchBrowser,uiMaxDurationMs,apiMaxDurationMs;if(command==='run'){
      if(profile==='formal'&&!options['--browser-launcher'])throw new Error('capacity_browser_launcher_required');
      if(options['--browser-launcher']&&!ownedFactory){const browserModule=await import(pathToFileURL(resolve(options['--browser-launcher'])).href);if(typeof browserModule.launchBrowser!=='function')throw new Error('capacity_browser_launcher_invalid');launchBrowser=browserModule.launchBrowser;}
      if(options['--ui-max-duration-ms']!==undefined){if(!/^[1-9]\d*$/.test(options['--ui-max-duration-ms']))throw new Error('capacity_ui_duration_below_sample_budget');uiMaxDurationMs=Number(options['--ui-max-duration-ms']);}
      if(options['--browser-launcher'])uiAcceptancePlan({profile,maxDurationMs:uiMaxDurationMs});
      if(options['--api-max-duration-ms']!==undefined&&!/^[1-9]\d*$/.test(options['--api-max-duration-ms']))throw new Error('capacity_api_max_duration_invalid');
      apiMaxDurationMs=apiCoverageDuration(options['--api-max-duration-ms']===undefined?undefined:Number(options['--api-max-duration-ms']),{required:profile==='formal'});
    }
    let runLifetimeMs;if(options['--run-lifetime-ms']!==undefined){if(!/^[1-9]\d*$/.test(options['--run-lifetime-ms']))throw new Error('capacity_run_lifetime_invalid');runLifetimeMs=Number(options['--run-lifetime-ms']);}
    capacityRunLifetime(runLifetimeMs,{required:command==='run'&&profile==='formal'});
    if(command==='run')capacityRunBudget({plan:capacityPlan(profile),runLifetimeMs,apiMaxDurationMs,uiMaxDurationMs:options['--browser-launcher']?uiMaxDurationMs:undefined});
    if(!options['--adapter'])throw new Error('capacity_owned_adapter_required');
    if(ownedFactory){
      if(!options['--owner-entry'])throw new Error('capacity_preparation_entry_unproven');
      const controller=new AbortController(),cancel=()=>controller.abort(new Error('capacity_cancelled'));for(const name of ['SIGINT','SIGTERM'])process.once(name,cancel);
      try{const {runCapacityOwnedFactory}=await import('./preparation-context.mjs');result=await runCapacityOwnedFactory({entryDirectory:resolve(options['--owner-entry']),signal:controller.signal,job:{target:{profile,purpose:command,binDirectory:options['--pg-bin']?resolve(options['--pg-bin']):undefined,libraryDirectory:options['--pg-lib']?resolve(options['--pg-lib']):undefined,storageRoot:options['--storage-root']?resolve(options['--storage-root']):undefined,resourceDelegatedRoot:options['--delegated-cgroup']?resolve(options['--delegated-cgroup']):undefined,windowsHelper:!!options['--windows-helper'],runLifetimeMs,resourceLimits},execution:{executeLong:!!options['--execute-long'],browserLauncher:options['--browser-launcher']?resolve(options['--browser-launcher']):undefined,uiMaxDurationMs,apiMaxDurationMs}}});}
      finally{for(const name of ['SIGINT','SIGTERM'])process.removeListener(name,cancel);}
    }else{
    const module=await import(pathToFileURL(resolve(options['--adapter'])).href);if(typeof module.createTarget!=='function')throw new Error('capacity_adapter_invalid');const controller=new AbortController();for(const signal of ['SIGINT','SIGTERM'])process.once(signal,()=>controller.abort(new Error('capacity_cancelled')));const target=await module.createTarget({signal:controller.signal,profile:options['--profile']??'formal',binDirectory:options['--pg-bin'],libraryDirectory:options['--pg-lib'],windowsHelper:!!options['--windows-helper'],purpose:command,runLifetimeMs,resourceLimits});const lifetime=capacityTargetRunLifetime(target);
    try{if(!isOwnedLaboratory(target.laboratory))throw new Error('capacity_target_not_task_owned');if(command==='run')result=await runCapacityExecution({plan:target.plan??capacityPlan(options['--profile']??'formal'),preflight:target.preflight,target,signal:controller.signal,executeLong:!!options['--execute-long'],launchBrowser,uiMaxDurationMs,apiMaxDurationMs,runLifetimeMs});else{if(!target.datasetStore||!target.datasetManifest)throw new Error('capacity_dataset_adapter_required');result=await (command==='seed'?seedDataset:verifyDataset)(target.datasetStore,target.datasetManifest,{recordingKey:target.recordingKey,signal:controller.signal});result={...result,laboratoryLifecycle:{persistence:'ephemeral',destroyedAfterCommand:true,continuation:'new_lab_each_command'}};}}
    finally{try{await target.close();}catch(error){
      if(command!=='run'||!result?.verdict)throw error;
      result={...result,cleanup:{passed:false,code:'capacity_target_cleanup_failed'},verdict:{...result.verdict,status:'failed',failures:[...result.verdict.failures,'capacity_target_cleanup_failed']}};process.exitCode=1;
    }if(command==='run'&&result?.verdict&&lifetime?.evidence().deadlineReached){result={...result,runLifetime:{...result.runLifetime,...lifetime.evidence()},verdict:{...result.verdict,status:result.verdict.status==='failed'?'failed':'incomplete',incomplete:[...new Set([...result.verdict.incomplete,'capacity_run_lifetime_exhausted'])]}};}}
    }
  }else throw new Error('capacity_command_invalid');
  const encoded=JSON.stringify(result,null,2)+'\n';if(options['--output'])await writeFile(resolve(options['--output']),encoded,{flag:'wx',mode:0o600});else process.stdout.write(encoded);
}
function publicPreflight(source){const codes=values=>Array.isArray(values)?values.filter(value=>typeof value==='string'&&/^[a-zA-Z0-9_:-]{1,160}$/.test(value)).slice(0,128):[];return {eligible:source.eligible===true,allocationEligible:source.allocationEligible===true,seedEligible:false,phase:['before_allocation','allocation','pre-seed','final','restore'].includes(source.phase)?source.phase:'unknown',missing:codes(source.missing),softwareMissing:codes(source.softwareMissing)};}
main().catch(error=>{process.stderr.write(JSON.stringify({error:{code:/^capacity_[a-z0-9_]+$/.test(error.message)?error.message:'capacity_operation_failed'},formalCapacity:'not_proven',...(error.preflight?{preflight:publicPreflight(error.preflight)}:{})})+'\n');process.exitCode=1;});
