import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,writeFile,readFile,readdir,lstat,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import fs from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
import pg from 'pg';
import Connection from 'pg/lib/connection.js';
import {storagePeriodicTransferCommand,sendLatestPeriodicBackup,receiveFixedPeriodicBackup} from '../storage-periodic-transfer.mjs';
import {postgresBackupContext} from '../postgres-backup-context.mjs';
import {createPeriodicSourceBase} from '../postgres-backup.mjs';
import {publishPeriodicBaseVerification} from '../postgres-periodic-base-verification.mjs';
import {capturePeriodicApplicationBackup} from '../postgres-periodic-application-backup.mjs';
import {backupMachineIdentity} from '../postgres-backup-handoff.mjs';
import {periodicSshPolicyFile} from '../postgres-periodic-ssh-policy.mjs';
import {digestFile} from '../migration-artifacts.mjs';

const exec=promisify(execFile),quote=value=>"'"+value.replaceAll("'","'\\''")+"'";
test('fixed periodic CLI rejects path and command overrides',async()=>{
 for(const args of [[],['send','/tmp/profile'],['receive','/tmp/key'],['restore'],['/bin/sh']])await assert.rejects(storagePeriodicTransferCommand(args),{code:'periodic_ssh_command_invalid'});
});
test('private namespace executes fixed source and receiver using a local SSH executable substitute',{
 skip:process.env.HARBOR_PERIODIC_ROOT_TEST!=='1'?'仅真实root私有mount namespace，不连接SSH或异机':false,timeout:180000
},async t=>{
 const {createPeriodicRootFixture}=await import('./periodic-root-fixture.mjs'),fixture=await createPeriodicRootFixture({directory:process.env.HARBOR_PERIODIC_ROOT_DIRECTORY,nodeFile:process.execPath,sourceDirectory:new URL('..',import.meta.url).pathname,pgBinDirectory:process.env.HARBOR_CAPACITY_PG_BIN,pgLibraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});
 t.after(()=>fixture.close());assert.equal((await sendLatestPeriodicBackup()).state,'periodic_ssh_disabled');assert.equal((await receiveFixedPeriodicBackup()).state,'periodic_receiver_disabled');
 const record=postgresBackupContext(fixture.context),indices=join(fixture.directory,'indices'),objects=join(fixture.directory,'objects'),identity=join(fixture.directory,'synthetic-identity'),known=join(fixture.directory,'synthetic-known-hosts'),fake=join(fixture.directory,'fake-ssh'),marker=join(fixture.directory,'ssh-processes'),cli=new URL('../storage-periodic-transfer.mjs',import.meta.url).pathname;
 await mkdir(indices,{mode:0o700});await mkdir(objects,{mode:0o700});await writeFile(identity,'synthetic non-credential',{mode:0o600});await writeFile(known,'synthetic host record',{mode:0o600});await writeFile(marker,'',{mode:0o600});
 const configuration={version:1,enabled:true,host:'synthetic.invalid',port:22,user:'synthetic',identityFile:identity,knownHostsFile:known,indexDirectory:indices,timeoutMs:60000,connectTimeoutSeconds:1},writePolicy=value=>writeFile(periodicSshPolicyFile,JSON.stringify(value),{mode:0o600});await writePolicy(configuration);
 assert.equal((await sendLatestPeriodicBackup()).state,'periodic_ssh_not_ready');assert.equal(await readFile(marker,'utf8'),'');
 // Slow preparation is a real query on the original verify-full connection.
 // Cancellation must complete that query before closing and wiping both keys;
 // a fast rejected Promise with a live pg_sleep backend is not sufficient.
 for(const mode of ['deadline','abort']){
  await writePolicy({...configuration,timeoutMs:mode==='deadline'?1000:60000});const cancellation=new AbortController(),originalQuery=pg.Client.prototype.query,originalCancel=Connection.prototype.cancel,originalOpen=fs.promises.open,originalFrom=Buffer.from,archiveRefs=[],applicationRefs=[];let selected,started,ready=false,cancels=0,inject=true,gateTimer;const entered=new Promise((resolve,reject)=>{started=()=>{clearTimeout(gateTimer);resolve();};gateTimer=setTimeout(()=>reject(Error('synthetic_prepare_enter_deadline')),5000);});
  pg.Client.prototype.query=function(text,...args){if(inject&&typeof text==='string'&&text.startsWith('SELECT spcname FROM pg_tablespace')){inject=false;selected=this;assert.equal(this.connection.stream.encrypted,true);assert.equal(this.connection.stream.authorized,true);this.connection.once('readyForQuery',()=>{ready=true;});const query=originalQuery.call(this,'SELECT pg_sleep(10)',...args);started();return query;}return originalQuery.call(this,text,...args);};
  Connection.prototype.cancel=function(pid,secret){assert.ok(this.stream.encrypted&&this.stream.authorized===true,'CancelRequest is sent only after verified TLS');assert.ok(selected&&pid===selected.processID&&secret===selected.secretKey,'CancelRequest belongs to the original session');cancels++;return originalCancel.call(this,pid,secret);};
  fs.promises.open=async(path,...args)=>{const handle=await originalOpen(path,...args);if(String(path)===record.archiveKeyFile){const read=handle.readFile.bind(handle);handle.readFile=async(...readArgs)=>{const value=await read(...readArgs);if(Buffer.isBuffer(value))archiveRefs.push(value);return value;};}return handle;};Buffer.from=function(value,...args){const output=originalFrom.call(Buffer,value,...args);if(Buffer.isBuffer(value)&&value.length===32&&value.equals(record.applicationKey))applicationRefs.push(output);return output;};syncBuiltinESMExports();
  let operation;const preparedAt=Date.now();try{operation=sendLatestPeriodicBackup({signal:cancellation.signal});const rejected=assert.rejects(operation,error=>mode==='deadline'?error.code==='periodic_ssh_deadline':error.name==='AbortError');await entered;if(mode==='abort')cancellation.abort();await rejected;const elapsedMs=Date.now()-preparedAt;assert.ok(elapsedMs<5000,'preparation cancellation must settle before the original 10-second query');t.diagnostic(JSON.stringify({preparation:mode,elapsedMs,verifiedTlsCancel:true}));assert.ok(cancels>0);assert.equal(ready,true);assert.equal(selected._ended,true);assert.ok(archiveRefs.length>0&&archiveRefs.every(value=>value.every(byte=>byte===0)),'archive key is wiped after failed preparation');assert.ok(applicationRefs.length>0&&applicationRefs.every(value=>value.every(byte=>byte===0)),'application key copies are wiped after failed preparation');assert.equal(await readFile(marker,'utf8'),'','SSH has not started during failed preparation');
   const deadline=Date.now()+5000;for(;;){if(!(await fixture.store.get('SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1) present',selected.processID)).present)break;if(Date.now()>=deadline)throw Error('synthetic_prepare_backend_still_alive');await new Promise(resolve=>setImmediate(resolve));}
  }finally{clearTimeout(gateTimer);cancellation.abort();await Promise.allSettled([operation]);pg.Client.prototype.query=originalQuery;Connection.prototype.cancel=originalCancel;fs.promises.open=originalOpen;Buffer.from=originalFrom;syncBuiltinESMExports();}
 }
 await writePolicy(configuration);
 const proof=await createPeriodicSourceBase(fixture.context);await publishPeriodicBaseVerification(proof);const point=await capturePeriodicApplicationBackup(fixture.context,proof,fixture.policy);assert.equal(point.backupComplete,true,point.failureCode);
 // Cancel after the catalog is durable but before handoff publication. The
 // original send entry must retry the same point without removing or
 // overwriting authenticated survivors or an unrelated file.
 const indexDirectory=join(indices,proof.backupId+'--'+point.pointId),catalogPath=join(indexDirectory,'handoff-catalog-0.enc'),originalLink=fs.promises.link,indexCancellation=new AbortController();let interruptedCatalog=false;
 fs.promises.link=async(from,to)=>{const result=await originalLink(from,to);if(String(to)===catalogPath){interruptedCatalog=true;indexCancellation.abort();}return result;};syncBuiltinESMExports();
 try{await assert.rejects(sendLatestPeriodicBackup({signal:indexCancellation.signal}),{name:'AbortError'});}finally{fs.promises.link=originalLink;syncBuiltinESMExports();}
 assert.equal(interruptedCatalog,true);await assert.rejects(readFile(join(indexDirectory,'handoff.enc')),{code:'ENOENT'});const preservedCatalog=await digestFile(catalogPath);assert.equal(await readFile(marker,'utf8'),'','cancelled index preparation never starts SSH');
 const foreign=join(indexDirectory,'foreign.keep');await writeFile(foreign,'synthetic foreign metadata',{flag:'wx',mode:0o600});await assert.rejects(sendLatestPeriodicBackup(),{code:'periodic_transfer_index_incomplete'});assert.equal(await readFile(foreign,'utf8'),'synthetic foreign metadata');assert.deepEqual(await digestFile(catalogPath),preservedCatalog);await rm(foreign);
 const originalCatalog=await readFile(catalogPath),corruptedCatalog=Buffer.from(originalCatalog);corruptedCatalog[corruptedCatalog.length-1]^=1;await writeFile(catalogPath,corruptedCatalog);await assert.rejects(sendLatestPeriodicBackup(),{code:'transfer_manifest_authentication_failed'});assert.deepEqual(await readFile(catalogPath),corruptedCatalog,'unrecognized metadata is preserved on rejection');await assert.rejects(readFile(join(indexDirectory,'handoff.enc')),{code:'ENOENT'});await writeFile(catalogPath,originalCatalog);
 const before=await readdir(record.artifactDirectory);await writePolicy({...configuration,indexDirectory:record.artifactDirectory});await assert.rejects(sendLatestPeriodicBackup(),{code:'periodic_ssh_index_overlap'});assert.deepEqual(await readdir(record.artifactDirectory),before);await writePolicy(configuration);
 await writeFile('/etc/harbor-storage-periodic-transfer.json',JSON.stringify({version:1,enabled:true,directory:objects,sourceMachine:await backupMachineIdentity(),expectedCluster:record.expected.system_id,keyVersion:record.keyVersion,archiveKeyFile:record.archiveKeyFile,archiveUid:record.restoreUid,applicationEncryption:record.applicationEncryption}),{mode:0o600});
 const diagnostic=join(fixture.directory,'receiver-diagnostic'),prefix='#!/bin/sh\nfor periodic_arg do periodic_last=$periodic_arg; done\n[ "$periodic_last" = harbor-periodic-receive ] || exit 41\nprintf "%s\\n" "$$" >> '+quote(marker)+'\n',receiver=prefix+'exec '+quote(process.execPath)+' '+quote(cli)+' receive 2>'+quote(diagnostic)+'\n';
 await writeFile(fake,receiver,{mode:0o755});await exec('/bin/mount',['--bind',fake,'/usr/bin/ssh'],{timeout:5000});let mounted=true;t.after(async()=>{if(mounted)await exec('/bin/umount',['/usr/bin/ssh'],{timeout:5000});});
 let sent;try{sent=JSON.parse((await exec(process.execPath,[cli,'send'],{timeout:75000,maxBuffer:4096,env:{PATH:'/usr/bin:/bin'}})).stdout);}catch(error){console.error(JSON.stringify({syntheticReceiverDiagnostic:await readFile(diagnostic,'utf8'),receivedObjects:(await readdir(join(objects,'objects'))).length,partialFiles:(await readdir(join(objects,'partial'))).length}));throw error;}assert.equal(sent.transportComplete,true);assert.equal(sent.backupId,proof.backupId);assert.equal(sent.pointId,point.pointId);for(const name of ['restoreVerified','offhostVerified','deploymentAllowed'])assert.equal(sent[name],false);assert.deepEqual(await readdir(join(objects,'incoming')),[]);assert.ok((await readdir(join(objects,'objects'))).some(name=>name.endsWith('.verified.enc')));
 assert.deepEqual(await digestFile(catalogPath),preservedCatalog,'retry reuses the authenticated catalog from the interrupted preparation byte for byte');assert.equal(JSON.parse((await exec(process.execPath,[cli,'send'],{timeout:75000,maxBuffer:4096,env:{PATH:'/usr/bin:/bin'}})).stdout).transportComplete,true,'repeated fixed CLI reuses immutable index and cached objects');
 // The substitute is local, not an SSH server. Failure and timeout still run
 // the real fixed child argv and must leave no child alive or source lease.
 await writeFile(fake,prefix+'exit 45\n',{mode:0o755});await assert.rejects(sendLatestPeriodicBackup(),{code:'periodic_ssh_process_failed'});
 const spawnedBeforeDeadline=(await readFile(marker,'utf8')).trim().split('\n').length;await writeFile(fake,prefix+'exec /bin/sleep 60\n',{mode:0o755});await writePolicy({...configuration,timeoutMs:1000});await assert.rejects(sendLatestPeriodicBackup(),error=>['periodic_ssh_process_failed','periodic_ssh_deadline'].includes(error.code));assert.equal((await readFile(marker,'utf8')).trim().split('\n').length,spawnedBeforeDeadline+1,'deadline actually terminates the spawned fixed child');
 for(const pid of (await readFile(marker,'utf8')).trim().split('\n')){assert.match(pid,/^\d+$/);await assert.rejects(lstat('/proc/'+pid),{code:'ENOENT'});}
 await writeFile(fake,receiver,{mode:0o755});await writePolicy(configuration);assert.equal((await sendLatestPeriodicBackup()).transportComplete,true,'failed children and deadline release the same source mutex and archive lease');
 await exec('/bin/umount',['/usr/bin/ssh'],{timeout:5000});mounted=false;
});
