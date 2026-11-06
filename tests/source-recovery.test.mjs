import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,readFile,writeFile,rename} from 'node:fs/promises';
import {randomBytes,randomUUID} from 'node:crypto';
import {join} from 'node:path';
import {Store} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createTaskStorageOperatorContext,storageOperatorContext} from '../storage-operator-context.mjs';
import {runStorageOperator} from '../storage-operator.mjs';
import {readManifest,writeManifest} from '../migration-artifacts.mjs';
import {backupInventory} from '../postgres-backup-inventory.mjs';
import {enablePrivateRuntimeLease} from '../storage-operator-role-lease.mjs';
const source=(await readFile(new URL('../ci/verify-source-recovery.mjs',import.meta.url),'utf8')).replaceAll('/opt/harbor-storage-tools/app/',new URL('../',import.meta.url).href);
const {verifySourceRecovery}=await import('data:text/javascript;base64,'+Buffer.from(source+'\n//# sourceURL=harbor-source-recovery-under-test.mjs').toString('base64'));
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,options={skip:!binDirectory?'requires isolated PG18':false,timeout:120000};
async function fixture(stage){
 const laboratory=await createPrivatePostgres({binDirectory,libraryDirectory}),runtime=createOwnedRuntime(),sourceDataDir=join(laboratory.directory,'source'),runDirectory=join(laboratory.directory,'operator'),backupKey=randomBytes(32),activationKey=randomBytes(32),master=randomBytes(32);let admin;
 try{
  await mkdir(sourceDataDir,{mode:0o700});await mkdir(runDirectory,{mode:0o700});await writeFile(join(sourceDataDir,'key-encryption.key'),master,{mode:0o600});await writeFile(join(sourceDataDir,'settings.json'),JSON.stringify({adminPasswordHash:{salt:'a'.repeat(32),digest:'b'.repeat(64)}}),{mode:0o600});
  const sqlite=new Store(join(sourceDataDir,'gateway.sqlite'),{encryptionKey:master});sqlite.createMember('synthetic-member',1000,null);sqlite.close();
  const entry=join(laboratory.directory,'legacy.mjs');await writeFile(entry,'import {acquireInstance} from '+JSON.stringify(new URL('../instance.mjs',import.meta.url).href)+';const lock=await acquireInstance(process.env.HARBOR_DATA_DIR);const timer=setInterval(()=>{},1000);process.send({ready:true});process.on("SIGTERM",async()=>{clearInterval(timer);await lock.release();process.exit(0)});');
  const sourceChild=runtime.start(process.execPath,[entry],{role:'gateway',env:{HARBOR_DATA_DIR:sourceDataDir},stdio:['ignore','pipe','pipe','ipc']});await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('fixture startup timeout')),10000);sourceChild.once('message',()=>{clearTimeout(timer);resolve();});sourceChild.once('error',reject);});
  const context=await createTaskStorageOperatorContext({laboratory,runtime,sourceChild,sourceDataDir,runDirectory,fileRoots:[{id:'data',source:sourceDataDir}]});admin=new PostgresStore({connection:laboratory.connection,allowPlaintextLocalNetwork:true,poolSize:1});
  const expected={...await admin.get("SELECT system_identifier::text system_id,current_setting('data_directory') data_directory FROM pg_control_system()"),...await backupInventory(admin)},taskBaseline={expected,configurationDigest:'a'.repeat(64)};
  const common={context,backupKey,activationKey};
  if(stage==='before-allocation'){await writeManifest(join(runDirectory,'operator.enc'),{version:1,runId:randomUUID(),configurationDigest:storageOperatorContext(context).configurationDigest,cluster:expected.system_id,phase:'preparing',runtimePassword:randomBytes(32).toString('base64url')},backupKey);await storageOperatorContext(context).fence();}
  else if(stage==='pending')await runStorageOperator(common);
  else await assert.rejects(runStorageOperator({...common,afterPhase:phase=>{if(phase===stage)throw Error('injected '+stage);}}),new RegExp('injected '+stage));
  return {laboratory,runtime,sourceDataDir,runDirectory,sourceChild,context,backupKey,admin,taskBaseline,state:()=>readManifest(join(runDirectory,'operator.enc'),backupKey),verify:()=>verifySourceRecovery({context,backupKey,taskBaseline}),target:async()=>{const state=await readManifest(join(runDirectory,'operator.enc'),backupKey);return new PostgresStore({connection:{...laboratory.connection,database:state.allocation.intent.database},allowPlaintextLocalNetwork:true,poolSize:1});},close:async()=>{await admin.close();await runtime.close();await laboratory.close();}};
 }catch(error){await admin?.close();await runtime.close();await laboratory.close();throw error;}
}
async function role(f,name){return f.admin.get('SELECT rolcanlogin can_login FROM pg_roles WHERE rolname=$1',name);}

test('actual interrupted capture verifies fresh source without a completed old capture',options,async()=>{
 const f=await fixture('capturing');try{
  const state=await f.state();assert.equal((await role(f,state.allocation.intent.migration)).can_login,true);const original=await readFile(join(f.sourceDataDir,'settings.json'));
  const proof=await f.verify();assert.equal(proof.contract,'harbor-source-recovery-diagnostic');assert.equal(proof.activationState,'not-created');assert.equal(proof.sourceVerificationMode,'fenced-current-capture');assert.deepEqual(proof.roleStates,{migration:'NOLOGIN',owner:'not-created',runtime:'not-created'});assert.equal(proof.targetSessionCount,0);assert.equal(f.sourceChild.exitCode,0);assert.deepEqual(await readFile(join(f.sourceDataDir,'settings.json')),original);assert.equal((await role(f,state.allocation.intent.migration)).can_login,false);
  assert.equal((await f.state()).phase,'capturing');assert.equal(proof.storageMarker.contract,'postgres-conversation-v2');assert.match(proof.storageMarker.database_digest,/^[a-f0-9]{64}$/);
 }finally{await f.close();}
});
test('authenticated no-allocation state requires exact baseline and creates no registry',options,async()=>{
 const f=await fixture('before-allocation');try{
  const before=await backupInventory(f.admin),proof=await f.verify();assert.equal(proof.activationState,'not-created');assert.equal(proof.storageMarker,null);assert.deepEqual(proof.roleStates,{migration:'not-created',owner:'not-created',runtime:'not-created'});assert.deepEqual(await backupInventory(f.admin),before);assert.equal(await f.admin.get("SELECT 1 FROM pg_namespace WHERE nspname='harbor_operator_registry'"),undefined);
 }finally{await f.close();}
});
test('witnessed migration role without database or target manifest is positively verified',options,async()=>{
 const f=await fixture('allocation_role_committed');try{
  await storageOperatorContext(f.context).fence();const state=await f.state();const proof=await f.verify();assert.equal(proof.activationState,'not-created');assert.equal(proof.storageMarker,null);assert.deepEqual(proof.roleStates,{migration:'NOLOGIN',owner:'not-created',runtime:'not-created'});assert.equal((await role(f,state.allocation.intent.migration)).can_login,false);assert.equal(await f.admin.get('SELECT 1 FROM pg_database WHERE datname=$1',state.allocation.intent.database),undefined);
 }finally{await f.close();}
});
test('missing target file is never mistaken for missing DB but signed allocation still proves no runtime writer',options,async()=>{
 const f=await fixture('importing');try{
  await rename(join(f.runDirectory,'artifacts','target.enc'),join(f.runDirectory,'target.preserved.enc'));const proof=await f.verify();assert.equal(proof.activationState,'not-created');assert.equal(proof.sourceVerified,true);assert.equal(proof.registryVerified,true);assert.equal(proof.inventoryVerified,true);
 }finally{await f.close();}
});
test('actual interrupted import uses the authenticated original capture and retains target',options,async()=>{
 const f=await fixture('importing');try{
  const state=await f.state(),before=await f.admin.get('SELECT oid::text oid FROM pg_database WHERE datname=$1',state.allocation.intent.database),proof=await f.verify();
  assert.equal(proof.sourceVerificationMode,'authenticated-captured-source-match');assert.equal(proof.activationState,'not-created');assert.deepEqual(await f.admin.get('SELECT oid::text oid FROM pg_database WHERE datname=$1',state.allocation.intent.database),before);assert.equal((await f.state()).phase,'importing');
 }finally{await f.close();}
});
test('promotion committed before phase save can recover without changing signed phase',options,async()=>{
 const f=await fixture('promotion_committed_before_state');try{
  assert.equal((await f.state()).phase,'promotion_pending');const proof=await f.verify();assert.equal(proof.activationState,'pending');assert.deepEqual(proof.roleStates,{migration:'NOLOGIN',owner:'NOLOGIN',runtime:'NOLOGIN'});assert.equal(proof.targetSessionCount,0);assert.equal((await f.state()).phase,'promotion_pending');
 }finally{await f.close();}
});
test('known preflight lease is revoked and owned sessions end before proof',options,async()=>{
 const f=await fixture('pending');let target,client;try{
  const state=await f.state();target=await f.target();const password=randomBytes(32).toString('base64url'),lease=await enablePrivateRuntimeLease(target,state,{password,timeoutMs:60000});
  client=new PostgresStore({connection:{...f.laboratory.connection,database:state.allocation.intent.database,user:state.runtime,password,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1});await client.get('SELECT 1');
  const proof=await f.verify();assert.equal(proof.activationState,'pending');assert.equal(proof.rolesNoLogin,true);assert.equal(proof.targetSessionCount,0);assert.equal((await target.get('SELECT revoked FROM harbor_operator.preflight_lease WHERE singleton=true')).revoked,true);assert.equal((await role(f,state.runtime)).can_login,false);
 }finally{await client?.close();await target?.close();await f.close();}
});
test('any deployment consumption record rejects recovery without revoking unrelated roles',options,async()=>{
 const f=await fixture('pending');let target;try{
  target=await f.target();await target.query('CREATE TABLE harbor_operator.deployment_activation(marker integer)');await target.query('INSERT INTO harbor_operator.deployment_activation VALUES(1)');const before=await backupInventory(f.admin);
  await assert.rejects(f.verify(),{code:'source_recovery_deployment_activation_present'});assert.deepEqual(await backupInventory(f.admin),before);assert.equal(f.sourceChild.exitCode,0);
  await target.query('DROP TABLE harbor_operator.deployment_activation');await target.query("UPDATE harbor_operator.activation SET state='active' WHERE singleton=true");await assert.rejects(f.verify(),{code:'source_recovery_activation_not_pending'});
 }finally{await target?.close();await f.close();}
});
test('unknown runtime session is never terminated or reported safe',options,async()=>{
 const f=await fixture('pending');let target,foreign;try{
  const state=await f.state();target=await f.target();const password=randomBytes(32).toString('base64url');await enablePrivateRuntimeLease(target,state,{password,timeoutMs:60000});
  foreign=new PostgresStore({connection:{...f.laboratory.connection,database:state.allocation.intent.database,user:state.runtime,password,application_name:'synthetic-foreign-session'},allowPlaintextLocalNetwork:true,poolSize:1});await foreign.get('SELECT 1');
  await assert.rejects(f.verify(),{code:'storage_operator_foreign_runtime_sessions'});assert.equal((await foreign.get('SELECT 1 n')).n,1);assert.equal((await role(f,state.runtime)).can_login,false);
 }finally{await foreign?.close();await target?.close();await f.close();}
});
test('foreign inventory and changed original materials reject before role permission changes',options,async()=>{
 const f=await fixture('importing');try{
  const state=await f.state();await f.admin.query('CREATE DATABASE unrelated_database');await assert.rejects(f.verify(),{code:'storage_operator_resume_inventory_changed'});assert.equal((await role(f,state.allocation.intent.migration)).can_login,true);await f.admin.query('DROP DATABASE unrelated_database');
  await writeFile(join(f.sourceDataDir,'settings.json'),JSON.stringify({adminPasswordHash:{salt:'c'.repeat(32),digest:'d'.repeat(64)}}));await assert.rejects(f.verify(),{code:'transfer_files_changed'});assert.equal((await role(f,state.allocation.intent.migration)).can_login,true);
 }finally{await f.close();}
});
test('fresh capture really validates existing encrypted credentials',options,async()=>{
 const f=await fixture('capturing');try{
  const state=await f.state();await writeFile(join(f.sourceDataDir,'key-encryption.key'),randomBytes(32));await assert.rejects(f.verify(),{code:'transfer_source_key_unreadable'});assert.equal((await role(f,state.allocation.intent.migration)).can_login,true);
 }finally{await f.close();}
});
