import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import {syncBuiltinESMExports} from 'node:module';
import {randomBytes} from 'node:crypto';
import {mkdir,writeFile,readFile,readdir,unlink} from 'node:fs/promises';
import {join,dirname,basename} from 'node:path';
import {setTimeout as delay} from 'node:timers/promises';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createTaskPostgresBackupContext,createPeriodicBasePreparationContext,closePostgresBackupContext,postgresBackupContext} from '../postgres-backup-context.mjs';
import {maintainPostgresBackupBase,runPostgresBackupScheduleCycle,postgresBackupSchedulePolicy} from '../postgres-backup-schedule.mjs';
import {loadAuthenticatedPeriodicBase,verifyPostgresBackup,postgresBackupRestoreSource} from '../postgres-backup.mjs';
import {publishPeriodicBaseVerification,authenticatePeriodicBaseRetention} from '../postgres-periodic-base-verification.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from '../postgres-backup-restore.mjs';
import {readManifest,writeManifest} from '../migration-artifacts.mjs';

const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
test('source-only persistent base reuse is explicit, rejects JSON authority and cannot turn on for DB-only schedules',async()=>{
 assert.equal(postgresBackupSchedulePolicy({version:1,enabled:true}).reuseAuthenticatedBase,false);
 assert.throws(()=>postgresBackupSchedulePolicy({version:1,enabled:true,reuseAuthenticatedBase:'true'}),{code:'backup_schedule_policy_invalid'});
 await assert.rejects(runPostgresBackupScheduleCycle(null,{version:1,enabled:true,reuseAuthenticatedBase:true}),{code:'periodic_base_application_required'});
 await assert.rejects(publishPeriodicBaseVerification({backupId:'untrusted',verified:true}),{code:'periodic_base_full_verification_required'});
});

test('real source receipts remove cold/day-switch full base reads, preserve verification time and revoke changed immutable identities',{
 skip:!binDirectory?'需自有合成PG18，不连接共享数据库':false,timeout:180000
},async t=>{
 const key=randomBytes(32),applicationKey=randomBytes(32),lab=await createPrivatePostgres({binDirectory,libraryDirectory,recovery:{backupKey:key}}),store=new PostgresStore({connection:lab.connection}),contexts=[];let release;
 const originalRead=fs.createReadStream,originalLink=fs.promises.link,originalReadFile=fs.promises.readFile;
 t.after(async()=>{release?.();fs.createReadStream=originalRead;fs.promises.link=originalLink;fs.promises.readFile=originalReadFile;syncBuiltinESMExports();try{for(const context of contexts)await closePostgresBackupContext(context);}finally{try{await store.close();}finally{await lab.close();key.fill(0);applicationKey.fill(0);}}});
 await store.migrate();await store.initializePricing();const database=await store.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid'),materialRoots=[];
 for(const id of ['data','runtime','secrets']){const directory=join(lab.directory,'material-'+id);await mkdir(directory,{mode:0o700});materialRoots.push({id,directory});await writeFile(join(directory,'synthetic.txt'),'synthetic '+id,{mode:0o600});}
 const application={version:1,enabled:true,database,materialRoots,publicOrigin:'http://localhost:18094',deviceOrigin:'https://localhost:18095',maxBytes:4096,maxFiles:20},configuration={version:1,enabled:true,reuseAuthenticatedBase:true,fullIntervalMs:1000,checkpointIntervalMs:1000,pollIntervalMs:1000,pinnedBackupIds:[]},directory=join(lab.directory,'periodic-backups');
 const producer=await createTaskPostgresBackupContext({laboratory:lab,directory,archiveKey:key,applicationKey,binDirectory,libraryDirectory});contexts.push(producer);
 const maintenance=createPeriodicBasePreparationContext(producer);contexts.push(maintenance);let offset=0;const options={clock:()=>Date.now()+offset,applicationConfiguration:application};
 const initial=await maintainPostgresBackupBase(maintenance,configuration,options);assert.equal(initial.state,'base_candidate_prepared');let state=await readManifest(join(directory,'periodic-state.enc'),key);assert.equal(state.active,null);assert.equal(state.pending.backupId,initial.backupId);
 const receipt=await readManifest(join(directory,initial.backupId,'base-verification','current.enc'),applicationKey),fullReads=[];
 fs.createReadStream=(path,...args)=>{if(dirname(String(path))===join(directory,initial.backupId)&&/^file-\d+\.enc$/.test(basename(String(path))))fullReads.push(String(path));return originalRead(path,...args);};syncBuiltinESMExports();
 const cold=createPeriodicBasePreparationContext(producer);contexts.push(cold);const proof=await loadAuthenticatedPeriodicBase(cold,initial.backupId);assert.equal(postgresBackupRestoreSource(proof).verifiedAt,receipt.verifiedAt);assert.equal(Object.isFrozen(postgresBackupRestoreSource(proof).metadata),true);
 await assert.rejects(publishPeriodicBaseVerification(proof),{code:'periodic_base_full_verification_required'});
 const first=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(first.application?.backupComplete,true,first.application?.failureCode);assert.equal(fullReads.length,0,'cold adoption/producer must not read the base body');
 state=await readManifest(join(directory,'periodic-state.enc'),key);assert.equal(state.active.backupId,initial.backupId);assert.equal(state.pending,null);
 await verifyPostgresBackup(cold,initial.backupId);assert.ok(fullReads.length>0,'ordinary verification still completely authenticates body with a receipt present');fullReads.length=0;assert.equal(inspectPostgresBackupRestore(await restorePostgresBackup(proof)).restoreVerified,true);assert.ok(fullReads.length>0,'ordinary restore must fully authenticate a receipt-loaded proof');fullReads.length=0;
 const receiptPath=join(directory,initial.backupId,'base-verification','current.enc'),receiptBytes=await readFile(receiptPath);
 for(const changed of [{...receipt,execution:{...receipt.execution,machine:'0'.repeat(64)}},{...receipt,execution:{...receipt.execution,nodeVersion:'v0.0.0'}},{...receipt,execution:{...receipt.execution,codeDigest:'0'.repeat(64)}},{...receipt,applicationKeyVersion:'different-key-version'}]){
  await writeManifest(receiptPath,changed,applicationKey,{replace:true});await assert.rejects(loadAuthenticatedPeriodicBase(cold,initial.backupId));
 }
 await writeFile(receiptPath,receiptBytes);assert.equal(postgresBackupRestoreSource(await loadAuthenticatedPeriodicBase(cold,initial.backupId)).verifiedAt,receipt.verifiedAt);
 // Receipt loss alone does not invalidate an in-memory, identity-checked base.
 // Pause only the independent preparation's first metadata read, then prove
 // producer progress while that complete authentication is still suspended.
 await unlink(receiptPath);let prepareEnteredResolve;const prepareEntered=new Promise(resolve=>{prepareEnteredResolve=resolve;}),prepareBlocked=new Promise(resolve=>{release=resolve;});let pausePreparation=true;
 fs.promises.readFile=async(path,...args)=>{if(pausePreparation&&String(path)===join(directory,initial.backupId,'backup.enc')){pausePreparation=false;prepareEnteredResolve();await prepareBlocked;}return originalReadFile(path,...args);};syncBuiltinESMExports();offset+=2000;
 const stillTrusted=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(stillTrusted.backupId,initial.backupId);assert.equal(stillTrusted.application?.backupComplete,true,stillTrusted.application?.failureCode);await prepareEntered;assert.ok(postgresBackupContext(producer).periodicBasePreparationPending);release();release=null;await postgresBackupContext(producer).periodicBasePreparationPending;fs.promises.readFile=originalReadFile;syncBuiltinESMExports();fullReads.length=0;
 const allowed=await authenticatePeriodicBaseRetention(postgresBackupContext(producer),join(directory,initial.backupId));assert.ok(allowed.includes('base-verification/current.enc'));
 offset+=2000;let enteredResolve;const entered=new Promise(resolve=>{enteredResolve=resolve;}),blocked=new Promise(resolve=>{release=resolve;});
 fs.promises.link=async(source,target)=>{if(target.endsWith('/backup.enc')&&!target.includes('/'+initial.backupId+'/')){enteredResolve();await blocked;}return originalLink(source,target);};syncBuiltinESMExports();
 const maintaining=maintainPostgresBackupBase(maintenance,configuration,options);void maintaining.catch(()=>{});const timeout=new AbortController();try{await Promise.race([entered,delay(30000,undefined,{signal:timeout.signal}).then(()=>assert.fail('candidate did not reach publication'))]);}finally{timeout.abort();}
 const concurrent=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(concurrent.backupId,initial.backupId);assert.equal(concurrent.application?.backupComplete,true,concurrent.application?.failureCode);assert.equal(fullReads.length,0);
 release();release=null;const replacement=await maintaining;assert.equal(replacement.state,'base_candidate_prepared');state=await readManifest(join(directory,'periodic-state.enc'),key);assert.equal(state.active.backupId,initial.backupId);assert.equal(state.pending.backupId,replacement.backupId);
 fs.promises.link=originalLink;fs.createReadStream=(path,...args)=>{if([initial.backupId,replacement.backupId].some(id=>dirname(String(path))===join(directory,id))&&/^file-\d+\.enc$/.test(basename(String(path))))fullReads.push(String(path));return originalRead(path,...args);};syncBuiltinESMExports();
 // A pending candidate with no receipt must not evict the still-trusted
 // active capability, even when the active's persisted receipt is also gone.
 // Keep candidate full authentication suspended while the active produces.
 const oldReceiptBytes=await readFile(receiptPath),candidateReceiptPath=join(directory,replacement.backupId,'base-verification','current.enc');await unlink(receiptPath);await unlink(candidateReceiptPath);
 let candidateEnteredResolve;const candidateEntered=new Promise(resolve=>{candidateEnteredResolve=resolve;}),candidateBlocked=new Promise(resolve=>{release=resolve;});let pauseCandidate=true;
 fs.promises.readFile=async(path,...args)=>{if(pauseCandidate&&String(path)===join(directory,replacement.backupId,'backup.enc')){pauseCandidate=false;candidateEnteredResolve();await candidateBlocked;}return originalReadFile(path,...args);};syncBuiltinESMExports();offset+=2000;
 const missingBoth=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(missingBoth.backupId,initial.backupId);assert.equal(missingBoth.application?.backupComplete,true,missingBoth.application?.failureCode);assert.equal(fullReads.length,0,'pending full authentication cannot force old active body reads');await candidateEntered;assert.ok(postgresBackupContext(producer).periodicBasePreparationPending);
 state=await readManifest(join(directory,'periodic-state.enc'),key);assert.equal(state.active.backupId,initial.backupId);assert.equal(state.pending.backupId,replacement.backupId);
 await writeFile(receiptPath,oldReceiptBytes,{mode:0o600});release();release=null;await postgresBackupContext(producer).periodicBasePreparationPending;fs.promises.readFile=originalReadFile;syncBuiltinESMExports();fullReads.length=0;
 // A fully authenticated candidate base does not authorize a missing or
 // damaged mutable checkpoint, and neither case may terminate the producer.
 const candidateCheckpoint=join(directory,replacement.backupId,'checkpoint.enc'),candidateCheckpointBytes=await readFile(candidateCheckpoint);
 for(const mode of ['missing','corrupt']){
  if(mode==='missing')await unlink(candidateCheckpoint);else await writeFile(candidateCheckpoint,Buffer.from('damaged candidate checkpoint'),{mode:0o600});offset+=2000;
  const unavailable=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(unavailable.backupId,initial.backupId);assert.equal(unavailable.application?.backupComplete,true,unavailable.application?.failureCode);assert.deepEqual(unavailable.candidate,{backupId:replacement.backupId,state:'candidate_unavailable',reason:mode==='missing'?'checkpoint_missing':'checkpoint_invalid'});assert.equal(fullReads.length,0,'candidate checkpoint failures cannot force any full base body reads');
  state=await readManifest(join(directory,'periodic-state.enc'),key);assert.equal(state.active.backupId,initial.backupId);assert.equal(state.pending.backupId,replacement.backupId);
  if(mode==='missing')await assert.rejects(readFile(candidateCheckpoint),{code:'ENOENT'});else assert.deepEqual(await readFile(candidateCheckpoint),Buffer.from('damaged candidate checkpoint'),'producer must preserve the broken candidate rather than overwrite it');
  await writeFile(candidateCheckpoint,candidateCheckpointBytes,{mode:0o600});
 }
 const switched=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(switched.backupId,replacement.backupId);assert.equal(switched.application?.backupComplete,true,switched.application?.failureCode);assert.equal(fullReads.length,0,'candidate adoption must not reread the newly authenticated base');
 const replacementReceipt=await readManifest(join(directory,replacement.backupId,'base-verification','current.enc'),applicationKey);
 const payload=join(directory,replacement.backupId,(await readdir(join(directory,replacement.backupId))).find(name=>/^file-\d+\.enc$/.test(name))),bytes=await readFile(payload),damaged=Buffer.from(bytes);damaged[0]^=1;await writeFile(payload,damaged);
 await assert.rejects(loadAuthenticatedPeriodicBase(producer,replacement.backupId),{code:'periodic_base_source_changed'});offset+=2000;const revoked=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(revoked.state,'base_verification_pending','an old in-memory proof cannot authorize a modified base');
 const pending=postgresBackupContext(producer).periodicBasePreparationPending;if(pending)await pending;assert.equal((await readManifest(join(directory,replacement.backupId,'base-verification','current.enc'),applicationKey)).verifiedAt,replacementReceipt.verifiedAt);
 await writeFile(payload,bytes);const preparing=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(preparing.state,'base_verification_pending');await postgresBackupContext(producer).periodicBasePreparationPending;
 fullReads.length=0;const recovered=await runPostgresBackupScheduleCycle(producer,configuration,options);assert.equal(recovered.application?.backupComplete,true,recovered.application?.failureCode);assert.equal(fullReads.length,0);const renewed=await readManifest(join(directory,replacement.backupId,'base-verification','current.enc'),applicationKey);assert.ok(renewed.verifiedAt>=receipt.verifiedAt);
 // Losing only the receipt starts a cancellable background full preparation;
 // context shutdown waits for it before zeroing either independently held key.
 await unlink(join(directory,replacement.backupId,'base-verification','current.enc'));const stopping=createPeriodicBasePreparationContext(producer);contexts.push(stopping);assert.equal((await runPostgresBackupScheduleCycle(stopping,configuration,options)).state,'base_verification_pending');const childRecord=postgresBackupContext(stopping);await closePostgresBackupContext(stopping);assert.equal(childRecord.periodicBasePreparationPending,null);assert.ok(childRecord.key.every(value=>value===0));assert.ok(childRecord.applicationKey.every(value=>value===0));assert.ok(postgresBackupContext(producer).applicationKey.some(value=>value!==0));
});
