import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {mkdtemp,writeFile,readFile,readdir,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {normalizeCapacityDiskBudget} from '../tools/capacity/storage-budget.mjs';
import {validateCapacityFactoryJob} from '../tools/capacity/factory-owner.mjs';
import {buildCapacityPreparationEntry} from '../tools/capacity/preparation-context.mjs';

const execute=promisify(execFile),cwd=new URL('..',import.meta.url),budget={dataBytes:2097152,metadataBytes:1048576,indexBytes:1048576,walBytes:1048576,backupBytes:1048576,temporaryBytes:1048576,growthBytes:1048576};
const formalExecution={executeLong:true,browserLauncher:'/not-loaded/browser.mjs',apiMaxDurationMs:30000000,uiMaxDurationMs:30000000};
const errorCode=error=>JSON.parse(error.stderr.trim()).error.code;

test('disk plans require all seven positive byte components and copy caller data',()=>{
 const input={...budget},normalized=normalizeCapacityDiskBudget(input);assert.deepEqual(normalized,input);assert.notStrictEqual(normalized,input);assert.ok(Object.isFrozen(normalized));input.walBytes=1;assert.equal(normalized.walBytes,budget.walBytes);
 for(const value of [undefined,null,[],{},'budget',{...budget,unknown:1},{...budget,dataBytes:0},{...budget,indexBytes:-1},{...budget,walBytes:1.5},{...budget,backupBytes:'1'},{...budget,temporaryBytes:Number.MAX_SAFE_INTEGER+1}])assert.throws(()=>normalizeCapacityDiskBudget(value),/capacity_storage_components_invalid/);
});

test('native factory requires real recovery for formal runs and keeps seed/selfcheck defaults',()=>{
 const input={target:{profile:'formal',purpose:'run',runLifetimeMs:70*3600000,diskBudget:{...budget}},execution:formalExecution},result=validateCapacityFactoryJob(input);assert.equal(result.target.recovery,true);assert.equal(input.target.recovery,undefined);assert.deepEqual(result.target.diskBudget,budget);assert.ok(Object.isFrozen(result.target.diskBudget));
 assert.throws(()=>validateCapacityFactoryJob({...input,target:{...input.target,recovery:false}}),/capacity_formal_recovery_required/);
 for(const recovery of ['true',0,null])assert.throws(()=>validateCapacityFactoryJob({...input,target:{...input.target,recovery}}),/capacity_factory_owner_configuration/);
 assert.throws(()=>validateCapacityFactoryJob({...input,target:{...input.target,diskBudget:{...budget,backupBytes:0}}}),/capacity_storage_components_invalid/);
 for(const target of [{profile:'selfcheck',purpose:'run'},{profile:'formal',purpose:'seed'},{profile:'formal',purpose:'verify'}])assert.equal(validateCapacityFactoryJob({target,execution:{}}).target.recovery,undefined);
 assert.throws(()=>validateCapacityFactoryJob({target:{profile:'formal',purpose:'seed',recovery:true}}),/capacity_factory_owner_configuration/);
});

test('CLI rejects invalid disk files before loading a caller adapter and passes valid plans and storage roots',{timeout:30000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-cli-disk-')),file=join(root,'disk.json'),adapter=join(root,'adapter.mjs'),observed=join(root,'observed.json');
 try{
  await writeFile(adapter,`import {writeFileSync} from 'node:fs';export function createTarget({signal,...options}){writeFileSync(${JSON.stringify(observed)},JSON.stringify(options));throw Error('capacity_cli_probe_complete');}`);
  const args=['tools/capacity/cli.mjs','verify','--profile','selfcheck','--adapter',adapter,'--disk-budget-file',file,'--storage-root',root];
  for(const invalid of [{...budget,backupBytes:0},{...budget,additional:1},{}]){await writeFile(file,JSON.stringify(invalid));await assert.rejects(execute(process.execPath,args,{cwd,timeout:10000}),error=>errorCode(error)==='capacity_storage_components_invalid');assert.equal((await readdir(root)).includes('observed.json'),false);}
  await writeFile(file,JSON.stringify(budget));
  for(const command of ['run','seed','verify']){const next=[...args];next[1]=command;await assert.rejects(execute(process.execPath,next,{cwd,timeout:10000}),error=>errorCode(error)==='capacity_cli_probe_complete');const value=JSON.parse(await readFile(observed,'utf8'));assert.deepEqual(value.diskBudget,budget);assert.equal(value.storageRoot,root);assert.equal(value.purpose,command);assert.equal(value.recovery,undefined);}
 }finally{await rm(root,{recursive:true,force:true});}
});

test('CLI sends disk budgets and mandatory recovery to the existing native factory boundary',{timeout:30000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-cli-owned-disk-')),file=join(root,'disk.json'),preload=join(root,'capture.mjs'),observed=join(root,'job.json'),entry=join(root,'unused-entry');
 try{
  const factory=new URL('../tools/capacity/preparation-context.mjs',import.meta.url).href;
  await writeFile(file,JSON.stringify(budget));
  await writeFile(preload,`import {mock} from 'node:test';import {writeFileSync} from 'node:fs';const original=await import(${JSON.stringify(factory)});mock.module(${JSON.stringify(factory)},{namedExports:{...original,runCapacityOwnedFactory:async options=>{writeFileSync(${JSON.stringify(observed)},JSON.stringify(options.job));throw Error('capacity_cli_probe_complete');}}});`);
  const args=['--no-warnings','--experimental-test-module-mocks','--import',preload,'tools/capacity/cli.mjs','run','--profile','formal','--adapter','tools/capacity/gateway-adapter.mjs','--owner-entry',entry,'--disk-budget-file',file,'--storage-root',root,'--execute-long','--browser-launcher',formalExecution.browserLauncher,'--run-lifetime-ms',String(70*3600000),'--api-max-duration-ms',String(formalExecution.apiMaxDurationMs),'--ui-max-duration-ms',String(formalExecution.uiMaxDurationMs)];
  await assert.rejects(execute(process.execPath,args,{cwd,timeout:20000}),error=>errorCode(error)==='capacity_cli_probe_complete');const job=JSON.parse(await readFile(observed,'utf8'));assert.deepEqual(job.target.diskBudget,budget);assert.equal(job.target.recovery,true);assert.equal(job.target.storageRoot,root);assert.equal(job.execution.executeLong,true);assert.equal(validateCapacityFactoryJob(job).target.recovery,true);assert.equal((await readdir(root)).includes('unused-entry'),false);
 }finally{await rm(root,{recursive:true,force:true});}
});

test('real CLI/native owner accepts a disk plan while retaining private PG verification and cleanup',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-cli-native-disk-')),entry=join(root,'entry'),storage=join(root,'storage'),file=join(root,'disk.json');
 try{
  const {mkdir}=await import('node:fs/promises');await mkdir(entry,{mode:0o700});await mkdir(storage,{mode:0o700});await buildCapacityPreparationEntry({directory:entry});await writeFile(file,JSON.stringify(budget));
  const result=await execute(process.execPath,['tools/capacity/cli.mjs','verify','--profile','selfcheck','--adapter','tools/capacity/gateway-adapter.mjs','--owner-entry',entry,'--storage-root',storage,'--disk-budget-file',file,'--pg-bin',process.env.HARBOR_CAPACITY_PG_BIN,...(process.env.HARBOR_CAPACITY_PG_LIB?['--pg-lib',process.env.HARBOR_CAPACITY_PG_LIB]:[])],{cwd,timeout:110000,maxBuffer:1048576});
  const report=JSON.parse(result.stdout);assert.equal(report.verified,true);assert.equal(report.formalScale,false);assert.equal(report.laboratoryLifecycle.destroyedAfterCommand,true);assert.deepEqual(await readdir(storage),[]);
 }finally{await rm(root,{recursive:true,force:true});}
});
