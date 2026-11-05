import test from 'node:test';
import assert from 'node:assert/strict';
import {verifyRestoredApplicationRuntime} from '../storage-restored-runtime.mjs';
import {markPostgresRestoreApplicationCleanupUnknown} from '../postgres-backup-restore.mjs';
import {inspectRestoredApplicationRuntime,restoredApplicationRuntimePolicy,restoredRuntimeTlsIdentity} from '../storage-operator-backup-application.mjs';
import {createStorageOperatorBackup} from '../storage-operator-backup.mjs';
test('restored runtime verification and cleanup retention cannot be supplied as JSON or copied sessions',async()=>{
 await assert.rejects(verifyRestoredApplicationRuntime({connection:{host:'/tmp'},runtimeVerified:true}),{code:'backup_restore_session_required'});assert.throws(()=>markPostgresRestoreApplicationCleanupUnknown({active:true}),{code:'backup_restore_session_required'});assert.throws(()=>inspectRestoredApplicationRuntime({active:true}),{code:'backup_restore_session_required'});assert.throws(()=>restoredApplicationRuntimePolicy({scope:'task-owned'}),{code:'backup_context_required'});await assert.rejects(createStorageOperatorBackup({verifyRestoredRuntime:'yes'}),{code:'storage_operator_runtime_flag_invalid'});
});

import {closeRestoredRuntimeApplication,collectRestoredRuntimeCloses,restoredRuntimeClosesSucceeded} from '../storage-restored-runtime-shutdown.mjs';
test('shutdown errors and forced/pending gateway or Worker results cannot become a successful drain acknowledgement',async()=>{
 const original=Error('synthetic close failure');await assert.rejects(closeRestoredRuntimeApplication('gateway',{close:async()=>{throw original;}}),error=>error===original);const normal={forced:false,ownershipLost:false,metering:{queued:0,files:0},recordingStorage:{pending:false}};for(const result of [{...normal,forced:true},{...normal,ownershipLost:true},{...normal,metering:{queued:1}},{...normal,recordingStorage:{pending:true}}])await assert.rejects(closeRestoredRuntimeApplication('gateway',{close:async()=>result}),{code:'storage_restored_runtime_drain_failed'});await assert.rejects(closeRestoredRuntimeApplication('worker',{close:async()=>({recordingStorage:{pending:true}})}),{code:'storage_restored_runtime_drain_failed'});const acknowledged=await closeRestoredRuntimeApplication('gateway',{close:async()=>normal}),results=[{status:'fulfilled',value:acknowledged}];assert.equal(restoredRuntimeClosesSucceeded(results,[{exitCode:0,signalCode:null}]),true);assert.equal(restoredRuntimeClosesSucceeded(results,[{exitCode:1,signalCode:null}]),false);assert.equal(restoredRuntimeClosesSucceeded([{status:'fulfilled',value:undefined}],[]),false);assert.equal(restoredRuntimeClosesSucceeded([{status:'rejected',reason:original}],[]),false);
});
test('a close command that never settles hits its actual deadline and remains a failed drain after process termination',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});const operation=collectRestoredRuntimeCloses([{close:()=>new Promise(()=>{})}],{timeoutMs:20});await Promise.resolve();t.mock.timers.tick(20);const results=await operation;assert.equal(results[0].status,'rejected');assert.equal(results[0].reason.code,'storage_restored_runtime_close_timeout');assert.equal(restoredRuntimeClosesSucceeded(results,[{exitCode:0,signalCode:null}]),false);t.mock.timers.reset();
});

test('the explicit restored TLS identity is a hostname rather than an address override, URL or caller credential',()=>{for(const value of ['localhost','harbor.synthetic.test','192.0.2.23','[::1]'])assert.equal(restoredRuntimeTlsIdentity(value),value);for(const value of [undefined,'','https://harbor.synthetic.test','host:443','user@host','host/path','host?query','host#fragment','host\n'])assert.throws(()=>restoredRuntimeTlsIdentity(value),{code:'storage_operator_runtime_tls_identity_required'});});

import {restoreOwnedProcesses,assertPostgresRestoreDirectoryUnused} from '../postgres-backup-processes.mjs';
import {mkdtemp,mkdir,writeFile,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {spawn} from 'node:child_process';
test('unknown root/subdirectory use is retained across approved UID boundaries while an adjacent name is not an occupant',{skip:process.platform!=='linux'},async()=>{
 const root=await mkdtemp(join(tmpdir(),'harbor-foreign-pgdata-')),data=join(root,'pgdata'),sub=join(data,'base'),neighbor=join(root,'pgdata-neighbor');await mkdir(data,{mode:0o700});await mkdir(sub,{mode:0o700});await mkdir(neighbor,{mode:0o700});try{for(const [cwd,occupied] of [[data,true],[sub,true],[neighbor,false]]){const child=spawn(process.execPath,['-e',"process.send({ready:true});setInterval(()=>{},1000)"],{cwd,stdio:['ignore','ignore','ignore','ipc']});try{await new Promise((resolve,reject)=>{child.once('error',reject);child.once('message',resolve);});const owner={pid:-1,nonce:'synthetic',uid:process.getuid()+1,data,members:new Map(),start:null};if(occupied){await assert.rejects(restoreOwnedProcesses(owner),{code:'backup_restore_process_identity_unknown'});await assert.rejects(assertPostgresRestoreDirectoryUnused(data),{code:'backup_restore_process_identity_unknown'});}else{assert.deepEqual(await restoreOwnedProcesses(owner),[]);await assertPostgresRestoreDirectoryUnused(data);}assert.equal(child.exitCode,null);assert.equal(child.signalCode,null);assert.equal(owner.members.size,0);}finally{if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');await new Promise(resolve=>child.once('exit',resolve));}}}}finally{await rm(root,{recursive:true,force:true});}
});
