import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,mkdir,writeFile,rm,stat,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomBytes,randomUUID} from 'node:crypto';
import {createPostgresBackupReceiver,createPeriodicPostgresBackupReceiver,createTaskPostgresBackupReceiver,inspectPostgresBackupReceiver,copyReceiverWalArchive} from '../postgres-backup-receiver.mjs';
import {createReceivedPostgresBackupContext} from '../postgres-backup-context.mjs';
import {storageReceiverCommand,storagePeriodicReceiverCommand,inspectPostgresReceiverRestore} from '../storage-receiver.mjs';
import {handoffFiles,preparePostgresBackupHandoff,preparePostgresBaselineHandoff,validHandoffPath,copyBackupCipher,digestBackupCipher} from '../postgres-backup-handoff.mjs';
import {writeManifest,digestFile} from '../migration-artifacts.mjs';
test('receiver rejects copied capabilities, arbitrary profiles, commands and unowned task paths before creating resources',async()=>{
 await assert.rejects(inspectPostgresBackupReceiver({scope:'root-profile-receiver'}),{code:'backup_receiver_capability_required'});await assert.rejects(createReceivedPostgresBackupContext({scope:'root-profile-receiver'}),{code:'backup_receiver_capability_required'});assert.throws(()=>inspectPostgresReceiverRestore({backupId:'synthetic'}),{code:'backup_receiver_restore_receipt_required'});await assert.rejects(createPostgresBackupReceiver('/tmp/caller-profile.json'),{code:'backup_receiver_root_profile_required'});await assert.rejects(createTaskPostgresBackupReceiver({laboratory:{directory:'/tmp'}}),{code:'backup_receiver_task_ownership'});await assert.rejects(preparePostgresBackupHandoff({context:{},proof:{}}),{code:'backup_context_required'});for(const args of [[],['restore','extra'],['shell'],['postgresql://synthetic']])await assert.rejects(storageReceiverCommand(args),{code:'backup_receiver_command_invalid'});
});
test('authenticated handoff pagination rejects traversal, duplicate entries, changed files and metadata exceeding the combined byte budget',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-handoff-test-')),key=randomBytes(32),id=randomUUID(),path='backup/'+id+'/backup.enc';try{await mkdir(join(directory,'backup',id),{recursive:true,mode:0o700});await writeFile(join(directory,path),randomBytes(80),{mode:0o600});const entry={path,digest:await digestFile(join(directory,path))};async function fixture(entries){await writeManifest(join(directory,'handoff-catalog-0.enc'),{version:1,ordinal:0,entries},key,{replace:true});const part={name:'handoff-catalog-0.enc',count:entries.length,digest:await digestFile(join(directory,'handoff-catalog-0.enc'))},manifest={version:1,backupId:id,count:entries.length,totalBytes:entries.reduce((sum,row)=>sum+row.digest.bytes,0),parts:[part]};await writeManifest(join(directory,'handoff.enc'),manifest,key,{replace:true});return manifest;}async function read(manifest,maxBytes=100000){const result=[];for await(const value of handoffFiles({directory,key,manifest,maxFiles:10,maxBytes}))result.push(value);return result;}
 let manifest=await fixture([entry]);assert.equal((await read(manifest)).length,1);await assert.rejects(read(manifest,manifest.totalBytes),{code:'backup_handoff_capacity'});manifest=await fixture([entry,entry]);await assert.rejects(read(manifest),{code:'backup_handoff_catalog_invalid'});manifest=await fixture([{...entry,path:'backup/'+id+'/../../outside.enc'}]);await assert.rejects(read(manifest),{code:'backup_handoff_catalog_invalid'});manifest=await fixture([entry]);await writeFile(join(directory,path),randomBytes(80),{mode:0o600});await assert.rejects(read(manifest),{code:'backup_handoff_file_changed'});
 }finally{key.fill(0);await rm(directory,{recursive:true,force:true});}
});

test('actual copying rejects an incoming file enlarged after its digest was approved and never publishes excess output',async()=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-handoff-bound-')),source=join(directory,'source.enc'),target=join(directory,'copy.enc');try{await writeFile(source,randomBytes(80),{mode:0o600});const approved=await digestFile(source);await writeFile(source,randomBytes(262144),{mode:0o600});await assert.rejects(copyBackupCipher(source,target,{maxBytes:approved.bytes}),{code:'backup_handoff_source_changed'});await assert.rejects(stat(target),{code:'ENOENT'});await assert.rejects(digestBackupCipher(source,{maxBytes:approved.bytes}),{code:'backup_handoff_source_changed'});assert.equal((await stat(source)).size,262144);}finally{await rm(directory,{recursive:true,force:true});}
});
test('baseline paths cannot carry final application claims and its public entry requires a live context',async()=>{
 const id=randomUUID(),prefix='backup/'+id+'/';for(const path of ['backup.enc','files.enc','checkpoint.enc','restore.enc','wal-catalog-'+('a'.repeat(32))+'/part-0.enc'])assert.equal(validHandoffPath(prefix+path,id,'cluster-baseline'),true,path);for(const path of ['operator-final.enc','application/application.enc','application/files.enc']){assert.equal(validHandoffPath(prefix+path,id,'cluster-baseline'),false,path);assert.equal(validHandoffPath(prefix+path,id),true,path);}assert.equal(validHandoffPath(prefix+'backup.enc',id,'unapproved-kind'),false);await assert.rejects(preparePostgresBaselineHandoff({context:{},proof:{},restore:{}}),{code:'backup_context_required'});
});
test('the actual WAL archive transfer crosses filesystem devices, verifies ciphertext and preserves input after cancellation',{skip:process.platform!=='linux'},async t=>{
 const directory=await mkdtemp(join(tmpdir(),'harbor-wal-crossfs-'));let second;try{try{second=await mkdtemp('/dev/shm/harbor-wal-crossfs-');}catch(error){if(['ENOENT','EACCES','EPERM','EROFS'].includes(error.code)){t.skip('缺少可写的独立tmpfs挂载');return;}throw error;}assert.notEqual((await stat(directory)).dev,(await stat(second)).dev,'must exercise distinct actual filesystem devices');await mkdir(join(directory,'wal'),{mode:0o700});const key=randomBytes(32),source=join(directory,'wal','cluster.enc');try{await writeManifest(source,{version:1,systemIdentifier:'123'},key);}finally{key.fill(0);}const entry={path:'wal/cluster.enc',digest:await digestFile(source)},destination=join(second,'copied');await copyReceiverWalArchive({directory,destination,entries:[entry]});assert.deepEqual(await readFile(join(destination,'cluster.enc')),await readFile(source));assert.deepEqual(await digestFile(source),entry.digest);const aborted=join(second,'cancelled');await assert.rejects(copyReceiverWalArchive({directory,destination:aborted,entries:[entry],signal:AbortSignal.abort()}),{name:'AbortError'});await assert.rejects(stat(aborted),{code:'ENOENT'});assert.deepEqual(await digestFile(source),entry.digest);}finally{await rm(directory,{recursive:true,force:true});if(second)await rm(second,{recursive:true,force:true});}
});

// The independent periodic fixed profile cannot select the migration profile
// or request pending-final/candidate capabilities.
test('periodic fixed receiver rejects caller profiles and nonperiodic signing commands',async()=>{
 for(const options of [{path:'/tmp/override'},{profile:{handoffKind:'periodic-application'}},{candidateRuntime:{}}])await assert.rejects(createPeriodicPostgresBackupReceiver(options),{code:'backup_receiver_root_profile_required'});
 for(const command of ['restore-and-attest','restore-and-attest-application','shell'])await assert.rejects(storagePeriodicReceiverCommand([command]),{code:'backup_receiver_command_invalid'});
 if(process.getuid()!==0)await assert.rejects(createPeriodicPostgresBackupReceiver(),{code:'backup_receiver_root_profile_required'});
});

test('periodic restore CLI cannot select caller paths, profiles or migration commands',async()=>{
 const {storagePeriodicRestoreCommand}=await import('../storage-periodic-restore.mjs');
 for(const args of [[],['verify'],['restore-and-attest','x','y'],['verify','/etc','/tmp'],['verify','00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000','/tmp/profile']])await assert.rejects(storagePeriodicRestoreCommand(args),{code:'periodic_restore_arguments_invalid'});
 const {inspectPeriodicStoredBundle}=await import('../postgres-periodic-object-store.mjs');await assert.rejects(inspectPeriodicStoredBundle({bundleDirectory:'/etc',handoffDigest:{}}),{code:'periodic_transfer_bundle_required'});
});

test('periodic restore publishes no signature after cancellation or timeout during awaited cleanup',async()=>{
 const {execFile}=await import('node:child_process'),{promisify}=await import('node:util');
 const fixture=String.raw`
 import assert from 'node:assert/strict';
 import {readFile,mkdtemp,rm} from 'node:fs/promises';
 import {tmpdir} from 'node:os';
 import {join} from 'node:path';
 import {pathToFileURL} from 'node:url';
 import {SourceTextModule,SyntheticModule,createContext} from 'node:vm';
 const source=await readFile(process.argv[1],'utf8'),{acquireInstance:actualAcquire}=await import(new URL('./instance.mjs',pathToFileURL(process.argv[1])));
 for(const mode of ['success','cancel','timeout']){
  const directory=await mkdtemp(join(tmpdir(),'harbor-periodic-cancel-')),cancel=new AbortController(),timeout=new AbortController(),events=[];let entered,release;
  const cleanupEntered=new Promise(resolve=>entered=resolve),cleanupGate=new Promise(resolve=>release=resolve),signature={payload:{purpose:'synthetic-periodic-result'},signature:'synthetic'};
  const context=createContext({process:{argv:[]},AbortSignal:{any:AbortSignal.any.bind(AbortSignal),timeout(ms){assert.equal(ms,1000);return timeout.signal;}}});
  const bindings={
   './postgres-periodic-object-store.mjs':{loadPeriodicObjectStore:async()=>({}),closePeriodicObjectStore:async()=>events.push('store-closed'),materializePeriodicStoredBundle:async()=>({}),periodicObjectStoreSettings:()=>({directory})},
   './postgres-backup-receiver.mjs':{periodicPostgresReceiverPolicy:async()=>({enabled:true,processTimeoutMs:1000}),createPeriodicPostgresBackupReceiver:async()=>({}),closePostgresBackupReceiver:async()=>{entered();await cleanupGate;events.push('receiver-closed');}},
   './storage-receiver.mjs':{restorePostgresReceiver:async()=>({})},
   './storage-receiver-periodic-attestation.mjs':{attestStorageReceiverPeriodic:async()=>{events.push('signed');return signature;}},
   './instance.mjs':{acquireInstance:async path=>{const lease=await actualAcquire(path);return {release:async()=>{await lease.release();events.push('lease-released');}};}},
   './migration-artifacts.mjs':{transferError:code=>Object.assign(new Error(code),{code})}
  };
  const module=new SourceTextModule(source,{context,initializeImportMeta(meta){meta.url='file:///synthetic/storage-periodic-restore.mjs';}});
  await module.link(async name=>{const values=bindings[name]??await import(name),names=Object.keys(values);return new SyntheticModule(names,function(){for(const key of names)this.setExport(key,values[key]);},{context});});await module.evaluate();
  const pending=module.namespace.storagePeriodicRestoreCommand(['verify','00000000-0000-0000-0000-000000000000','00000000-0000-0000-0000-000000000000'],{signal:cancel.signal});
  await cleanupEntered;assert.deepEqual(events,['signed']);
  if(mode==='cancel')cancel.abort(new Error('synthetic_cancel'));if(mode==='timeout')timeout.abort(new Error('synthetic_timeout'));
  release();if(mode==='success')assert.equal(await pending,signature);else await assert.rejects(pending,{message:'synthetic_'+mode});
  assert.deepEqual(events,['signed','receiver-closed','store-closed','lease-released']);const reacquired=await actualAcquire(join(directory,'.periodic-restore'));await reacquired.release();await rm(directory,{recursive:true});
 }
 `;
 await promisify(execFile)(process.execPath,['--experimental-vm-modules','--input-type=module','-e',fixture,new URL('../storage-periodic-restore.mjs',import.meta.url).pathname],{timeout:10000,maxBuffer:8192});
});
