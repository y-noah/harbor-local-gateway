import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes,createHash} from 'node:crypto';
import fs from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
import {execFile,spawn} from 'node:child_process';
import {once,getEventListeners} from 'node:events';
import {promisify} from 'node:util';
import {mkdir,readFile,readdir,writeFile,rm,lstat} from 'node:fs/promises';
import {join} from 'node:path';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createTaskPostgresBackupContext,closePostgresBackupContext,postgresBackupContext,createPeriodicBasePreparationContext} from '../postgres-backup-context.mjs';
import {createPeriodicSourceBase,loadAuthenticatedPeriodicBase} from '../postgres-backup.mjs';
import {publishPeriodicBaseVerification} from '../postgres-periodic-base-verification.mjs';
import {capturePeriodicApplicationBackup} from '../postgres-periodic-application-backup.mjs';
import {createPeriodicTransferIndex,usePeriodicTransferIndex,openPeriodicTransferSource,releasePeriodicTransferIndex,inspectPeriodicTransferIndex} from '../postgres-periodic-transfer-index.mjs';
import {acquireInstance} from '../instance.mjs';
import {createTaskPeriodicObjectStore,closePeriodicObjectStore,createPeriodicTransferInbox,writePeriodicTransferControl,describePeriodicRemoteHandoff,receiveRemotePeriodicIndex,releasePeriodicTransferInbox,periodicObjectStoreSettings,materializePeriodicStoredBundle,periodicObjectStoreLifetime} from '../postgres-periodic-object-store.mjs';
import {digestFile} from '../migration-artifacts.mjs';
import {PeriodicSshWire} from '../postgres-periodic-ssh-wire.mjs';
import {servePeriodicSshSource,receivePeriodicSshTransfer} from '../postgres-periodic-ssh-transfer.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
async function boundedGate(promise,label){let timer;try{return await Promise.race([promise,new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(Error('synthetic_gate_deadline_'+label)),5000);})]);}finally{clearTimeout(timer);}}
test('remote intake never accepts a copied store capability or caller file access',async()=>{
 await assert.rejects(receiveRemotePeriodicIndex({scope:'root-periodic-transport'},{directory:'/etc'},()=>{}),{code:'periodic_transfer_store_required'});
 assert.throws(()=>periodicObjectStoreSettings({verified:true}),{code:'periodic_transfer_store_required'});
});

test('authenticated remote ranges resume real encrypted objects, enforce inbox quotas and reuse an existing base',{
 skip:!binDirectory?'需自有PG18；不连接共享数据库或SSH服务器':false,timeout:180000
},async t=>{
 const archiveKey=randomBytes(32),applicationKey=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:archiveKey}}),db=new PostgresStore({connection:lab.connection});let context,sender;const stores=[],indexes=[],originalLink=fs.promises.link,originalUnlink=fs.promises.unlink,originalOpen=fs.promises.open,originalMkdir=fs.promises.mkdir;
 t.after(async()=>{fs.promises.link=originalLink;fs.promises.unlink=originalUnlink;fs.promises.open=originalOpen;fs.promises.mkdir=originalMkdir;syncBuiltinESMExports();try{for(const store of stores)await closePeriodicObjectStore(store);for(const index of indexes)releasePeriodicTransferIndex(index);if(sender)await closePostgresBackupContext(sender);if(context)await closePostgresBackupContext(context);}finally{try{await db.close();}finally{await lab.close();archiveKey.fill(0);applicationKey.fill(0);}}});
 await db.migrate();await db.initializePricing();const database=await db.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid'),materialRoots=[];
 for(const id of ['data','runtime','secrets']){const directory=join(lab.directory,'material-'+id);await mkdir(directory,{mode:0o700});await writeFile(join(directory,'synthetic.txt'),'synthetic '+id,{mode:0o600});materialRoots.push({id,directory});}
 context=await createTaskPostgresBackupContext({laboratory:lab,directory:join(lab.directory,'backups'),archiveKey,applicationKey,binDirectory,libraryDirectory});const proof=await createPeriodicSourceBase(context);await publishPeriodicBaseVerification(proof);sender=createPeriodicBasePreparationContext(context);const sourceProof=await loadAuthenticatedPeriodicBase(sender,proof.backupId),application={version:1,enabled:true,database,materialRoots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:4096,maxFiles:20};
 const limits={maxFiles:10000,maxObjectBytes:67108864,maxBundleBytes:536870912,maxStoreBytes:1073741824,maxChunkBytes:262144,maxPoints:10};
 async function storeAt(name,changes={}){const directory=join(lab.directory,name);await mkdir(directory,{mode:0o700});const store=await createTaskPeriodicObjectStore({laboratory:lab,context,directory,limits:{...limits,...changes}});stores.push(store);return {store,directory};}
 async function indexAt(name){const point=await capturePeriodicApplicationBackup(context,proof,application);assert.equal(point.backupComplete,true,point.failureCode);const directory=join(lab.directory,name);await mkdir(directory,{mode:0o700});const index=await createPeriodicTransferIndex(sender,sourceProof,point.pointId,{directory});indexes.push(index);return {index,point};}
 const first=await indexAt('index-one'),target=await storeAt('network-store');let calls=0,firstDigest;
 async function deliver(index,store,{interrupt=false,corrupt=false}={}){return usePeriodicTransferIndex(index,async source=>{
  const inbox=await createPeriodicTransferInbox(store),handoff=await readFile(join(source.directory,'handoff.enc'));await writePeriodicTransferControl(store,inbox,'handoff.enc',handoff,source.handoffDigest);
  const described=await describePeriodicRemoteHandoff(store,inbox,source.handoffDigest);assert.deepEqual(Object.keys(described).sort(),['backupId','parts','pointId']);
  for(const part of described.parts)await writePeriodicTransferControl(store,inbox,part.name,await readFile(join(source.directory,part.name)),part.digest);
  const byDigest=new Map(source.entries.map(entry=>[entry.digest.sha256,entry]));const readRange=async request=>{calls++;const entry=byDigest.get(request.digest.sha256);assert.ok(entry);assert.deepEqual(request.digest,entry.digest);assert.ok(request.offset>=0&&request.bytes>0&&request.offset+request.bytes<=entry.digest.bytes);const input=await openPeriodicTransferSource(entry);try{const bytes=Buffer.alloc(request.bytes),result=await input.read(bytes,0,bytes.length,request.offset);assert.equal(result.bytesRead,bytes.length);if(corrupt)bytes[0]^=1;return bytes;}finally{await input.close();}};
  let interrupted=false;try{const result=await receiveRemotePeriodicIndex(store,{directory:inbox,handoffDigest:source.handoffDigest},readRange,{onChunk:interrupt?event=>{if(!interrupted){interrupted=true;firstDigest=event.object;throw Error('synthetic disconnect after durable chunk');}}:undefined});await releasePeriodicTransferInbox(store,inbox);return result;}catch(error){if(!interrupt)throw error;assert.match(error.message,/synthetic disconnect/);return {inbox};}
 });}
 const stopped=await deliver(first.index,target.store,{interrupt:true});assert.ok(firstDigest);assert.ok((await readdir(join(target.directory,'partial'))).some(name=>name.endsWith('.cursor.enc')));assert.ok((await readdir(join(target.directory,'points'))).every(name=>name.startsWith('.pending-')));
 const resumed=await deliver(first.index,target.store);assert.equal(resumed.transportComplete,true);for(const field of ['restoreVerified','offhostVerified','deploymentAllowed'])assert.equal(resumed[field],false);const before=calls,repeated=await deliver(first.index,target.store);assert.equal(repeated.bytesSent,0);assert.equal(calls,before,'received object identities avoid all source ranges');
 const next=await indexAt('index-two'),delivered=await deliver(next.index,target.store);assert.equal(delivered.baseBytesSent,0,'new point reuses the same authenticated encrypted base');assert.ok(delivered.bytesSent>0);
 await releasePeriodicTransferInbox(target.store,stopped.inbox);
 function pipePeers(){const forward=spawn('/bin/cat',[],{stdio:['pipe','pipe','ignore']}),reverse=spawn('/bin/cat',[],{stdio:['pipe','pipe','ignore']}),exited=[once(forward,'exit'),once(reverse,'exit')],source=new PeriodicSshWire(reverse.stdout,forward.stdin,{timeoutMs:3600000}),receiver=new PeriodicSshWire(forward.stdout,reverse.stdin,{timeoutMs:3600000});return {source,receiver,async close(){source.close();receiver.close();forward.kill('SIGKILL');reverse.kill('SIGKILL');await Promise.all(exited);}};}
 // The owner lifetime spans the whole receiver, including before start and
 // while a control frame has not yet reached any object-store operation.
 for(const stage of ['before-start','control-header','control-cipher']){
  const ending=await storeAt('owner-close-'+stage),lifetime=periodicObjectStoreLifetime(ending.store),listeners=getEventListeners(lifetime,'abort').length,pipes=pipePeers();let waitingCipher=false;const readBytes=pipes.receiver.readBytes.bind(pipes.receiver);pipes.receiver.readBytes=async bytes=>{waitingCipher=true;return readBytes(bytes);};
  const receiving=receivePeriodicSshTransfer(ending.store,pipes.receiver),rejected=assert.rejects(receiving,{name:'AbortError'});assert.equal(getEventListeners(lifetime,'abort').length,listeners+1);
  try{
   if(stage!=='before-start'){const handoffDigest=inspectPeriodicTransferIndex(first.index).handoffDigest;await pipes.source.writeJson({kind:'periodic-start',version:1,handoffDigest});if(stage==='control-cipher'){await pipes.source.writeJson({kind:'control',name:'handoff.enc',digest:handoffDigest});await pipes.source.writeBytes(Buffer.alloc(1));}
    const deadline=Date.now()+5000;for(;;){if((await readdir(join(ending.directory,'incoming'))).length===1&&pipes.receiver.reading&&(stage!=='control-cipher'||waitingCipher&&pipes.receiver.input.readableLength===0))break;if(Date.now()>=deadline)throw Error('synthetic_owner_close_stage_deadline');await new Promise(resolve=>setImmediate(resolve));}
   }
   await boundedGate(closePeriodicObjectStore(ending.store),'owner-'+stage);await boundedGate(rejected,'receiver-'+stage);assert.deepEqual(await readdir(join(ending.directory,'incoming')),[]);assert.equal(getEventListeners(lifetime,'abort').length,listeners);
  }finally{await pipes.close();}
 }
 // More failures than maxPoints must not strand private control inboxes. A
 // subsequent normal protocol transfer below uses this exact same store.
 for(let attempt=0;attempt<=limits.maxPoints;attempt++){
  const pipes=pipePeers(),receiving=receivePeriodicSshTransfer(target.store,pipes.receiver),rejected=assert.rejects(receiving,{code:'periodic_ssh_control_invalid'}),handoffDigest=inspectPeriodicTransferIndex(first.index).handoffDigest;
  try{await pipes.source.writeJson({kind:'periodic-start',version:1,handoffDigest});await pipes.source.writeJson({kind:'control',name:'foreign.enc',digest:handoffDigest});await rejected;assert.deepEqual(await readdir(join(target.directory,'incoming')),[]);}finally{await pipes.close();}
 }
 // Keep the peer connected but withhold the requested suffix. Store close
 // must cancel that real pipe read, release its lease and clean only controls.
 const blocked=await storeAt('blocked-wire');await usePeriodicTransferIndex(first.index,async source=>{
  const pipes=pipePeers(),receiving=receivePeriodicSshTransfer(blocked.store,pipes.receiver),rejected=assert.rejects(receiving,{name:'AbortError'});
  try{await pipes.source.writeJson({kind:'periodic-start',version:1,handoffDigest:source.handoffDigest});for(const part of [{name:'handoff.enc',digest:source.handoffDigest},...source.manifest.parts]){await pipes.source.writeJson({kind:'control',name:part.name,digest:part.digest});await pipes.source.writeBytes(await readFile(join(source.directory,part.name)));}assert.equal((await pipes.source.readJson()).kind,'suffix');await boundedGate(closePeriodicObjectStore(blocked.store),'closed-store-network-read');await boundedGate(rejected,'closed-store-receiver-cleanup');assert.deepEqual(await readdir(join(blocked.directory,'incoming')),[]);assert.ok((await readdir(join(blocked.directory,'partial'))).some(name=>name.endsWith('.cursor.enc')),'partial remains resumable');const lease=await acquireInstance(join(blocked.directory,'.periodic-object-store'));await lease.release();}
  finally{await pipes.close();}
 });
 // Standard subprocess pipes carry the fixed protocol. Both peers still use
 // genuine in-process index/store capabilities; no capability is serialized.
 const wirePoint=await indexAt('index-wire'),forward=spawn('/bin/cat',[],{stdio:['pipe','pipe','ignore']}),reverse=spawn('/bin/cat',[],{stdio:['pipe','pipe','ignore']}),forwardExit=once(forward,'exit'),reverseExit=once(reverse,'exit'),sourceWire=new PeriodicSshWire(reverse.stdout,forward.stdin,{timeoutMs:30000}),receiverWire=new PeriodicSshWire(forward.stdout,reverse.stdin,{timeoutMs:30000});
 try{const [sent,received]=await Promise.all([servePeriodicSshSource(wirePoint.index,sourceWire),receivePeriodicSshTransfer(target.store,receiverWire)]);assert.equal(sent.transportComplete,true);assert.equal(received.transportComplete,true);assert.equal(received.baseBytesSent,0);assert.ok(received.bytesSent>0);for(const field of ['restoreVerified','offhostVerified','deploymentAllowed']){assert.equal(sent[field],false);assert.equal(received[field],false);}}
 finally{sourceWire.close();receiverWire.close();forward.kill('SIGKILL');reverse.kill('SIGKILL');await Promise.all([forwardExit,reverseExit]);}
 // A correct-length but corrupt stream leaves a durable reset intention. A
 // retry must re-read the object, including a crash after truncation but before
 // the cursor can record the new empty-file identity.
 const corruptTarget=await storeAt('corrupt-range');await assert.rejects(deliver(first.index,corruptTarget.store,{corrupt:true}),{code:'periodic_transfer_object_digest'});
 const corruptPartial=(await readdir(join(corruptTarget.directory,'partial'))).find(name=>name.endsWith('.part'));assert.ok(corruptPartial);assert.ok((await lstat(join(corruptTarget.directory,'partial',corruptPartial))).size>0);
 let resetInterrupted=false;fs.promises.open=async(path,...args)=>{const handle=await originalOpen(path,...args);if(String(path)===join(corruptTarget.directory,'partial',corruptPartial)){const truncate=handle.truncate.bind(handle);handle.truncate=async size=>{await truncate(size);if(!resetInterrupted){resetInterrupted=true;throw Error('synthetic interruption after reset truncate');}};}return handle;};syncBuiltinESMExports();
 try{await assert.rejects(deliver(first.index,corruptTarget.store),/synthetic interruption after reset truncate/);}finally{fs.promises.open=originalOpen;syncBuiltinESMExports();}
 assert.equal(resetInterrupted,true);assert.equal((await lstat(join(corruptTarget.directory,'partial',corruptPartial))).size,0);assert.equal((await deliver(first.index,corruptTarget.store)).transportComplete,true);
 // Materialization is a separate explicit action; the transport receipt above
 // has no restore/offhost authority. Existing complete files are immutable.
 const materialized=await materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId),bundle=materialized.bundleDirectory,bundleHandoff=join(bundle,'handoff.enc'),originalHandoff=await readFile(bundleHandoff),beforeStat=await lstat(bundleHandoff);assert.ok(materialized.bytesCopied>0);assert.equal(materialized.restoreVerified,false);
 assert.equal((await materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId)).bytesCopied,0);assert.equal((await lstat(bundleHandoff)).ino,beforeStat.ino);
 for(const boundary of ['after-link','after-unlink']){
  await fs.promises.unlink(bundleHandoff);let interrupted=false;
  fs.promises.link=async(from,to,...args)=>{const result=await originalLink(from,to,...args);if(boundary==='after-link'&&!interrupted&&String(to)===bundleHandoff){interrupted=true;throw Error('synthetic interruption after link');}return result;};
  fs.promises.unlink=async(path,...args)=>{const result=await originalUnlink(path,...args);if(boundary==='after-unlink'&&!interrupted&&String(path).startsWith(join(target.directory,'copies','c-'))){interrupted=true;throw Error('synthetic interruption after unlink');}return result;};syncBuiltinESMExports();
  try{await assert.rejects(materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId),/synthetic interruption/);}finally{fs.promises.link=originalLink;fs.promises.unlink=originalUnlink;syncBuiltinESMExports();}
  assert.equal(interrupted,true);assert.equal((await lstat(bundleHandoff)).nlink,boundary==='after-link'?2:1);assert.ok((await readdir(join(target.directory,'copies'))).some(name=>name.endsWith('.plan.enc')));
  await materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId);assert.deepEqual(await readFile(bundleHandoff),originalHandoff);assert.equal((await lstat(bundleHandoff)).nlink,1);assert.equal((await readdir(join(target.directory,'copies'))).filter(name=>name.endsWith('.plan.enc')).length,0);
 }
 // A target inserted just before publication remains byte-for-byte untouched.
 await fs.promises.unlink(bundleHandoff);let collided=false;const foreign=Buffer.from('foreign synthetic final; never overwrite');fs.promises.link=async(from,to,...args)=>{if(!collided&&String(to)===bundleHandoff){collided=true;await writeFile(to,foreign,{flag:'wx',mode:0o600});}return originalLink(from,to,...args);};syncBuiltinESMExports();
 try{await assert.rejects(materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId),{code:'periodic_transfer_bundle_conflict'});}finally{fs.promises.link=originalLink;syncBuiltinESMExports();}assert.deepEqual(await readFile(bundleHandoff),foreign);await fs.promises.unlink(bundleHandoff);await materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId);
 // Actual SIGKILL leaves an unpublished truncated copy. A fresh call must
 // finish without adopting/deleting the unclaimed file or rewriting a bundle.
 await fs.promises.unlink(bundleHandoff);const relative=proof.backupId+'--'+first.point.pointId+'/handoff.enc',copyId=createHash('sha256').update(relative).digest('hex'),orphan=join(target.directory,'copies','c-'+copyId+'-'+randomBytes(12).toString('hex')+'.enc');
 await assert.rejects(promisify(execFile)(process.execPath,['-e',"require('fs').writeFileSync(process.argv[1],Buffer.alloc(29,7),{mode:0o600});process.kill(process.pid,'SIGKILL')",orphan]),error=>error.signal==='SIGKILL');const orphanBytes=await readFile(orphan);assert.equal(orphanBytes.length,29);
 await materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId);assert.deepEqual(await readFile(orphan),orphanBytes);assert.deepEqual(await readFile(bundleHandoff),originalHandoff);assert.equal((await materializePeriodicStoredBundle(target.store,proof.backupId,first.point.pointId)).bytesCopied,0);

 // Interrupted inboxes count against the same persistent quota; an orphaned
 // or foreign control file is preserved rather than removed by a later caller.
 const bounded=await storeAt('bounded-inbox',{maxStoreFiles:1}),inbox=await createPeriodicTransferInbox(bounded.store),content=randomBytes(40),digest={bytes:40,sha256:(await import('node:crypto')).createHash('sha256').update(content).digest('hex')};await writePeriodicTransferControl(bounded.store,inbox,'handoff.enc',content,digest);
 await assert.rejects(writePeriodicTransferControl(bounded.store,inbox,'handoff-catalog-0.enc',content,digest),{code:'periodic_transfer_store_capacity'});assert.deepEqual(await readFile(join(inbox,'handoff.enc')),content);await writeFile(join(inbox,'foreign.enc'),content,{mode:0o600});await assert.rejects(releasePeriodicTransferInbox(bounded.store,inbox),{code:'periodic_transfer_inbox_changed'});assert.deepEqual(await readFile(join(inbox,'foreign.enc')),content);
 for(const boundary of ['unlink-error','unlink-abort','parent-sync']){
  const ending=await storeAt('release-'+boundary),created=await createPeriodicTransferInbox(ending.store),cancellation=new AbortController();for(const name of ['handoff.enc','handoff-catalog-0.enc'])await writePeriodicTransferControl(ending.store,created,name,content,digest);let interrupted=false;
  fs.promises.unlink=async(path,...args)=>{const result=await originalUnlink(path,...args);if(boundary.startsWith('unlink-')&&!interrupted&&String(path).startsWith(created+'/')){interrupted=true;if(boundary==='unlink-abort')cancellation.abort();else throw Error('synthetic release boundary');}return result;};
  fs.promises.open=async(path,...args)=>{const handle=await originalOpen(path,...args);if(boundary==='parent-sync'&&String(path)===join(ending.directory,'incoming')){const sync=handle.sync.bind(handle);handle.sync=async()=>{if(!interrupted){interrupted=true;throw Error('synthetic release boundary');}return sync();};}return handle;};syncBuiltinESMExports();
  try{await assert.rejects(releasePeriodicTransferInbox(ending.store,created,{signal:cancellation.signal}),boundary==='unlink-abort'?{name:'AbortError'}:/synthetic release boundary/);}finally{fs.promises.unlink=originalUnlink;fs.promises.open=originalOpen;syncBuiltinESMExports();}
  assert.equal(interrupted,true);await releasePeriodicTransferInbox(ending.store,created);await assert.rejects(lstat(created),{code:'ENOENT'});await releasePeriodicTransferInbox(ending.store,created);
 }
 const emptyLimited=await storeAt('bounded-empty-inboxes',{maxPoints:1});await createPeriodicTransferInbox(emptyLimited.store);await assert.rejects(createPeriodicTransferInbox(emptyLimited.store),{code:'periodic_transfer_store_capacity'});
 // Typed authentication fails before a range callback can select any source.
 const tampered=await storeAt('tampered-control'),bad=await createPeriodicTransferInbox(tampered.store);await writePeriodicTransferControl(tampered.store,bad,'handoff.enc',content,digest);await assert.rejects(receiveRemotePeriodicIndex(tampered.store,{directory:bad,handoffDigest:digest},()=>assert.fail('unauthenticated input requested bytes')),{code:'transfer_manifest_authentication_failed'});
 for(const boundary of ['inbox-create','control-write']){
  const ending=await storeAt('close-'+boundary),created=boundary==='control-write'?await createPeriodicTransferInbox(ending.store):null;let reached,continueWork,closed=false,handleClosed=false;const entered=new Promise(resolve=>{reached=resolve;}),gate=new Promise(resolve=>{continueWork=resolve;});
  fs.promises.mkdir=async(path,...args)=>{const result=await originalMkdir(path,...args);if(boundary==='inbox-create'&&String(path).startsWith(join(ending.directory,'incoming','i-'))){reached();await boundedGate(gate,'release');}return result;};
  fs.promises.open=async(path,...args)=>{const handle=await originalOpen(path,...args);if(boundary==='control-write'&&String(path)===join(created,'handoff.enc')){const close=handle.close.bind(handle);handle.close=async()=>{handleClosed=true;return close();};reached();await boundedGate(gate,'release');}return handle;};syncBuiltinESMExports();
  let writing,closing;
  try{writing=boundary==='inbox-create'?createPeriodicTransferInbox(ending.store):writePeriodicTransferControl(ending.store,created,'handoff.enc',content,digest);await boundedGate(entered,'enter');closing=closePeriodicObjectStore(ending.store).then(()=>{closed=true;});await Promise.resolve();assert.equal(closed,false,'close waits for the actual pending operation and lease cleanup');continueWork();await assert.rejects(boundedGate(writing,'write-settle'),{name:'AbortError'});await boundedGate(closing,'close-settle');assert.equal(closed,true);if(boundary==='control-write')assert.equal(handleClosed,true);else assert.deepEqual(await readdir(join(ending.directory,'incoming')),[],'an inbox created but not delivered is rolled back after close');}
  finally{continueWork();fs.promises.open=originalOpen;fs.promises.mkdir=originalMkdir;syncBuiltinESMExports();await boundedGate(Promise.allSettled([writing,closing]),'cleanup');}
 }
 assert.equal((await db.get('SELECT COUNT(*)::text n FROM harbor.accounts')).n,'0');
});
