import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {verifyStorageOperatorSourceFence} from '../storage-operator-source-supervisor.mjs';

if(process.argv.includes('--binding-fixture')){
  // Deterministically pause the last asynchronous source-lease assertion. The
  // real module owns its private WeakMap and runs its normal finally/cleanup;
  // only external IO dependencies are synthetic for this lifecycle boundary.
  const canonical=JSON.stringify,hash=value=>createHash('sha256').update(canonical(value)).digest('hex'),manifest={database:'db',user:'migration',schemaChecksums:[]},files={roots:[]},source={sourceDataDir:'/source',content:{},files:{manifestDigest:{}}};
  const state={version:1,phase:'pending',configurationDigest:'config',cluster:'cluster',runId:'run',databaseOid:'1',owner:'owner',runtime:'runtime',allocation:{migrationOid:'2'},roleGenerations:{ownerOid:'3',runtimeOid:'4'},registrySchemaOid:'5'};
  state.targetDigest=hash({cluster:state.cluster,database:manifest.database,user:manifest.user,schema:manifest.schemaChecksums});state.bindingDigest=hash({configuration:'config',target:state.targetDigest,source});
  const changedSource=process.argv.includes('--changed-source-epoch');let observations=0;const epoch={scope:'docker-systemd',sourceStopped:true,supervisorFenced:true,productionVerified:true,sourceImageId:'sha256:'+'a'.repeat(64),sourceContainerId:'b'.repeat(64),sourceStartedAt:'original-start',sourceFinishedAt:'original-stop'};if(changedSource)state.sourceFence=epoch;
  let closed=false,calls=0,reach,release;const reached=new Promise(resolve=>reach=resolve),gate=new Promise(resolve=>release=resolve);
  const trusted={scope:changedSource?'docker-systemd':'task-owned',configuration:{runDirectory:'/run',sourceDataDir:'/source',fileRoots:[],backupKeyVersion:'v1'},configurationDigest:'config',adminOptions:{connection:{}},observe:async()=>{observations++;return {...epoch,...(changedSource&&observations===5?{sourceStartedAt:'restarted-source'}:{})};}};
  class PostgresStore{constructor(){this.pool={options:{}};}async close(){}async all(){return ['migration','owner','runtime'].map((name,index)=>({name,oid:String(index+2),superuser:false,replication:false,can_login:false}));}async get(sql){if(sql.includes('current_database()'))return {database:'db',database_oid:'1',cluster:'cluster'};if(sql.includes('activation'))return {state:'pending',run_id:'run',binding:state.bindingDigest,database_oid:'1',runtime:'runtime'};}}
  const dependencies={verifyStorageOperatorSourceFence:changedSource?verifyStorageOperatorSourceFence:()=>assert.fail('task source must not request a Docker fence'),createHash,join:(...args)=>args.join('/'),resolve:value=>value,lstat:async()=>{throw Object.assign(Error(),{code:'ENOENT'});},PostgresStore,acquireInstance:async()=>({release:async()=>{}}),storageOperatorContext:()=>trusted,openStorageOperatorRegistry:async()=>({}),closeStorageOperatorRegistry:async()=>{},verifyStorageOperatorAllocation:async()=>{},readStorageOperatorRoleGenerations:async()=>state.roleGenerations,acquireSqliteSourceLease:async()=>({close:async()=>{closed=true;}}),assertSqliteSourceLease:async()=>{if(closed)throw Error('closed lease');if(++calls===6&&!changedSource){reach();await gate;}},inspectSqliteSourceLease:async()=>({source:'/source',content:{}}),verifyTransferredSqliteSnapshot:async()=>{},verifyStorageOperatorMaterialContents:async()=>{},artifactKey:()=>{},readManifest:async path=>path.endsWith('operator.enc')?state:path.endsWith('target.enc')?manifest:path.endsWith('source-manifest.enc')?source:files,canonical,digestFile:async()=>({}),verifyFileSetSources:async()=>{},privateDirectory:async()=>{},transferError:code=>Object.assign(Error(code),{code})};
  const context=vm.createContext({Buffer}),module=new vm.SourceTextModule(await readFile(new URL('../storage-operator-backup-binding.mjs',import.meta.url),'utf8'),{context});await module.link(()=>new vm.SyntheticModule(Object.keys(dependencies),function(){for(const [name,value] of Object.entries(dependencies))this.setExport(name,value);},{context}));await module.evaluate();
  if(changedSource){await assert.rejects(module.namespace.withPendingStorageOperatorBackup({context:{},backupKey:Buffer.alloc(32)},async binding=>{await module.namespace.inspectPendingStorageOperatorBackup(binding);}),{code:'storage_operator_source_fence_changed'});assert.equal(closed,true);process.stdout.write(JSON.stringify({closed:true,rejected:true,observations})+'\n');}else{
  const finishAfterVerification=process.argv.includes('--finish-after-verification');let inspection;await module.namespace.withPendingStorageOperatorBackup({context:{},backupKey:Buffer.alloc(32)},async binding=>{inspection=module.namespace.inspectPendingStorageOperatorBackup(binding);await reached;if(finishAfterVerification){release();await Promise.resolve();}});assert.equal(closed,true);if(!finishAfterVerification)release();await assert.rejects(inspection,{code:finishAfterVerification?'storage_operator_pending_backup_binding_required':'storage_operator_pending_backup_binding_closed'});process.stdout.write(JSON.stringify({closed:true,rejected:true})+'\n');
  }
}else{
  test('a changed source epoch at the last pending observation is rejected before returning a binding',{timeout:15000},async()=>{
    const result=await promisify(execFile)(process.execPath,['--experimental-vm-modules',fileURLToPath(import.meta.url),'--binding-fixture','--changed-source-epoch'],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(result.stdout.trim()),{closed:true,rejected:true,observations:5});
  });
  test('inspection waiting at its last asynchronous lease assertion cannot return after callback scope closes',{timeout:15000},async()=>{
    const result=await promisify(execFile)(process.execPath,['--experimental-vm-modules',fileURLToPath(import.meta.url),'--binding-fixture'],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(result.stdout.trim()),{closed:true,rejected:true});
  });
  test('inspection continuation cannot return a DTO when scope closes after verification returns',{timeout:15000},async()=>{
    const result=await promisify(execFile)(process.execPath,['--experimental-vm-modules',fileURLToPath(import.meta.url),'--binding-fixture','--finish-after-verification'],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(result.stdout.trim()),{closed:true,rejected:true});
  });
}
