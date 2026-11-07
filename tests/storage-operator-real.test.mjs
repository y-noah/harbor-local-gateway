import {enablePrivateRuntimeLease,revokePrivateRuntimeLease} from '../storage-operator-role-lease.mjs';
import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,writeFile,readFile,rm,lstat,symlink} from 'node:fs/promises';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {Store,revealStoredKey} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createTaskStorageOperatorContext,createTaskStorageOperatorReadinessContext,createDockerStorageOperatorContext,storageOperatorContext} from '../storage-operator-context.mjs';
import {runStorageOperator} from '../storage-operator.mjs';
import {cleanupStorageOperator} from '../storage-operator-cleanup.mjs';
import {openStorageOperatorRegistry,closeStorageOperatorRegistry,inspectStorageOperatorRegistry,prepareStorageOperatorTarget,readStorageOperatorAllocation} from '../storage-operator-state.mjs';
import {verifyStorageOperatorReceipt} from '../storage-operator-receipt.mjs';
import {readManifest,writeManifest} from '../migration-artifacts.mjs';

import {openRehearsalTarget,verifyRehearsalIsolation,destroyRehearsalTarget,transferSqlite,verifyTransferredSqliteSnapshot} from '../sqlite-postgres-transfer.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB;
const options={skip:!binDirectory?'requires isolated PG18':false,timeout:180000};
async function fixture({transferBatchRows,requestCount=3}={}){
  const laboratory=await createPrivatePostgres({binDirectory,libraryDirectory}),runtime=createOwnedRuntime(),source=join(laboratory.directory,'source'),homes=join(laboratory.directory,'source-runtime'),runDirectory=join(laboratory.directory,'operator'),master=randomBytes(32),backupKey=randomBytes(32),activationKey=randomBytes(32);let store,child;
  try{
    for(const directory of [source,homes,runDirectory,join(homes,'synthetic-account')])await mkdir(directory,{mode:0o700});await writeFile(join(source,'key-encryption.key'),master,{mode:0o600});await writeFile(join(source,'settings.json'),JSON.stringify({port:0,codexBinary:process.execPath,adminPasswordHash:{salt:'a'.repeat(32),digest:'b'.repeat(64)}}),{mode:0o600});await writeFile(join(homes,'synthetic-account','auth.json'),'{"synthetic":"offline"}',{mode:0o600});
    store=new Store(join(source,'gateway.sqlite'),{encryptionKey:master});const member=store.createMember('合成迁移员工',null,null,['gpt-6.1-sol']),actor=store.authenticate(member.secret);store.run("INSERT INTO accounts(id,name,home,state,auth_mode) VALUES('synthetic-api','合成账号',?,'ready','openai_api')",join(homes,'synthetic-account'));store.saveUpstreamKey('synthetic-api','synthetic-upstream-key');for(let index=0;index<requestCount;index++){const id=store.observe(actor,'synthetic-network','synthetic-agent','gpt-6.1-sol');store.finish(id,{input:100+index,output:10,cached:5});if(index===0)store.run('UPDATE requests SET manual_cost_atoms=? WHERE id=?','123456789123456789123',id);}store.close();store=null;
    const entry=join(laboratory.directory,'legacy-fixture.mjs'),gateway=new URL('../server.mjs',import.meta.url).href,instance=new URL('../instance.mjs',import.meta.url).href;
    await writeFile(entry,'import {createGateway} from '+JSON.stringify(gateway)+';import {acquireInstance} from '+JSON.stringify(instance)+';const instance=await acquireInstance(process.env.HARBOR_DATA_DIR);const app=createGateway({dataDir:process.env.HARBOR_DATA_DIR,runtimeDir:process.env.HARBOR_RUNTIME_DIR,pricingFetch:async()=>({ok:false,status:503})});app.server.listen(0,"127.0.0.1",()=>process.send({ready:true}));process.on("SIGTERM",()=>app.close().then(()=>instance.release()).then(()=>process.exit(0)));\n');
    child=runtime.start(process.execPath,[entry],{role:'gateway',env:{HARBOR_DATA_DIR:source,HARBOR_RUNTIME_DIR:homes},stdio:['ignore','pipe','pipe','ipc']});await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('source fixture readiness timeout')),15000);child.once('error',reject);child.on('message',message=>{if(message.ready){clearTimeout(timer);resolve();}});child.once('exit',()=>{clearTimeout(timer);reject(Error('source fixture exited'));});});
    const fileRoots=[{id:'data',source},{id:'runtime',source:homes,destinationPrefix:'/app/runtime'}],context=await createTaskStorageOperatorContext({laboratory,runtime,sourceChild:child,sourceDataDir:source,runDirectory,fileRoots,transferBatchRows});
    return {laboratory,runtime,source,homes,fileRoots,sourceChild:child,runDirectory,backupKey,activationKey,member,master,legacyEntry:entry,context,close:async()=>{await runtime.close();await laboratory.close();}};
  }catch(error){store?.close();await runtime.close();await laboratory.close();throw error;}
}
async function adminTarget(f){const manifest=await readManifest(join(f.runDirectory,'artifacts','target.enc'),f.backupKey);return {manifest,store:new PostgresStore({connection:{...f.laboratory.connection,database:manifest.database},allowPlaintextLocalNetwork:true,poolSize:1})};}

test('real offline import promotes least-privilege roles, privately verifies original Keys and remains blocked on actual backup/offhost/consumer',options,async()=>{
  const f=await fixture();let target;
  try{
    const result=await runStorageOperator(f);assert.equal(result.state,'pending');assert.equal(result.scope,'task-owned');assert.equal(result.deploymentAllowed,false);for(const gap of ['actual_docker_source_fence','existing_cluster_backup_verification','offhost_restore_verification','deployment_activation_consumer'])assert.ok(result.missing.includes(gap));
    target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),row=await target.store.get('SELECT rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1',state.runtime);assert.ok(Object.values(row).every(value=>value===false));
    assert.equal((await target.store.get('SELECT pg_get_userbyid(datdba) owner FROM pg_database WHERE datname=current_database()')).owner,state.owner);
    const client=await target.store.pool.connect();try{await client.query('SET ROLE "'+state.runtime+'"');assert.equal((await client.query('SELECT COUNT(*) n FROM harbor.requests')).rows[0].n,3);for(const sql of ['CREATE TABLE harbor.must_not_create(id int)','UPDATE harbor.data_contract SET name=name','SELECT pg_read_file(\'/etc/passwd\')'])await assert.rejects(client.query(sql),error=>error.code==='42501');await client.query('RESET ROLE');}finally{client.release();}
    const key=await target.store.get('SELECT * FROM harbor.keys WHERE id=$1',f.member.keyId);assert.equal(revealStoredKey(f.master,key),f.member.secret);assert.equal((await target.store.get('SELECT SUM(manual_cost_atoms::numeric)::text amount FROM harbor.requests')).amount,'123456789123456789123');
    assert.equal(verifyStorageOperatorReceipt(result.receipt,f.activationKey,{bindingDigest:state.bindingDigest}).deploymentAllowed,false);assert.throws(()=>verifyStorageOperatorReceipt(result.receipt,f.activationKey,{bindingDigest:state.bindingDigest,requireActivation:true}),{code:'storage_operator_activation_not_available'});const tampered=structuredClone(result.receipt);tampered.payload.deploymentAllowed=true;assert.throws(()=>verifyStorageOperatorReceipt(tampered,f.activationKey,{bindingDigest:state.bindingDigest}),{code:'storage_operator_receipt_invalid'});
    await target.store.close();target=null;await assert.rejects(transferSqlite({directory:join(f.runDirectory,'artifacts'),key:f.backupKey}));await assert.rejects(destroyRehearsalTarget({connection:f.laboratory.connection,allowPlaintextLocalNetwork:true},{directory:join(f.runDirectory,'artifacts'),key:f.backupKey}));
    const resumed=await runStorageOperator(f);assert.equal(resumed.state,'pending');assert.equal(resumed.receipt.payload.bindingDigest,state.bindingDigest);
  }finally{await target?.store.close();await f.close();}
});

test('committed import interruption resumes without overwriting records; promoted DB/file disagreement and changed source remain blocked',options,async()=>{
  const f=await fixture();let target,interrupted=false;
  try{
    await assert.rejects(runStorageOperator({...f,afterBatch:row=>{if(row.table==='requests'&&!interrupted){interrupted=true;throw Error('synthetic import interruption');}}}),/synthetic import interruption/);assert.equal(interrupted,true);
    const result=await runStorageOperator(f);assert.equal(result.state,'pending');target=await adminTarget(f);assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,3);await target.store.run("UPDATE harbor.members SET name='changed after promotion'");await target.store.close();target=null;await assert.rejects(runStorageOperator(f),{code:'transfer_target_conflict'});
  }finally{await target?.store.close();await f.close();}
});

test('operator rejects forged contexts and copied source ownership before touching shared paths',async()=>{await assert.rejects(runStorageOperator({context:{scope:'docker-systemd'},backupKey:randomBytes(32),activationKey:randomBytes(32)}),{code:'storage_operator_context_required'});if(process.getuid?.()!==0)await assert.rejects(createDockerStorageOperatorContext('/tmp/not-approved'),{code:'storage_operator_root_required'});});

test('filesystem rename before state persistence and DB promotion before caller acknowledgement recover idempotently',options,async()=>{
  const f=await fixture();try{
    let renamed=false;await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='materials_renamed'&&!renamed){renamed=true;throw Error('synthetic filesystem acknowledgement interruption');}}}),/synthetic filesystem/);assert.equal(renamed,true);
    let promoted=false;await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='roles_promoted'&&!promoted){promoted=true;throw Error('synthetic database acknowledgement interruption');}}}),/synthetic database/);assert.equal(promoted,true);
    const result=await runStorageOperator(f);assert.equal(result.state,'pending');assert.equal(result.deploymentAllowed,false);const target=await adminTarget(f);try{assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,3);assert.equal((await target.store.get("SELECT state FROM harbor_operator.activation")).state,'pending');}finally{await target.store.close();}
  }finally{await f.close();}
});

test('source runtime-file changes while frozen block promotion rather than trusting the earlier capture',options,async()=>{
  const f=await fixture();try{
    await assert.rejects(runStorageOperator({...f,afterPhase:async phase=>{if(phase==='verified')await writeFile(join(f.homes,'synthetic-account','auth.json'),'{"synthetic":"changed after capture"}',{mode:0o600});}}),{code:'transfer_files_changed'});
    const target=await adminTarget(f);try{const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey);assert.equal((await target.store.get('SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1) found',state.runtime)).found,false);}finally{await target.store.close();}
  }finally{await f.close();}
});

test('source directory aliases cannot claim task authority and a separate writer through an alias remains blocked by the real SQLite lock',options,async()=>{
  const f=await fixture(),alias=join(f.laboratory.directory,'source-alias');try{
    await symlink(f.source,alias);await assert.rejects(createTaskStorageOperatorContext({laboratory:f.laboratory,runtime:f.runtime,sourceChild:f.sourceChild,sourceDataDir:alias,runDirectory:f.runDirectory,fileRoots:[{id:'data',source:alias}]}),{code:'storage_operator_source_identity'});
    const {execFile}=await import('node:child_process'),{promisify}=await import('node:util'),execute=promisify(execFile);let observed=false;
    const result=await runStorageOperator({...f,afterPhase:async phase=>{if(phase==='capturing'){const code="import {DatabaseSync} from 'node:sqlite';const db=new DatabaseSync(process.argv[1]);try{db.exec('BEGIN IMMEDIATE');console.log('writer accepted')}catch(e){console.log('writer blocked')}finally{db.close()}";const actual=await execute(process.execPath,['--input-type=module','-e',code,join(alias,'gateway.sqlite')]);assert.match(actual.stdout,/writer blocked/);observed=true;}}});assert.equal(observed,true);assert.equal(result.state,'pending');assert.ok(result.missing.includes('actual_docker_source_fence'));
  }finally{await f.close();}
});

test('cancellation stops the offline transition without signing an activation status',options,async()=>{
  const f=await fixture();try{
    const controller=new AbortController();await assert.rejects(runStorageOperator({...f,signal:controller.signal,afterPhase:phase=>{if(phase==='verified')controller.abort(Error('synthetic guard cancellation'));}}),/synthetic guard cancellation/);await assert.rejects(lstat(join(f.runDirectory,'activation-status.enc')),{code:'ENOENT'});
    const result=await runStorageOperator(f);assert.equal(result.state,'pending');
  }finally{await f.close();}
});

test('runtime role has no login outside private verification and shared-cluster public CONNECT is reported without changing other databases',options,async()=>{
  const f=await fixture();let target;try{
    const result=await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey);assert.equal(result.crossDatabaseDenyGuaranteed,false);
    const runtime=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword},allowPlaintextLocalNetwork:true,poolSize:1});try{await assert.rejects(runtime.get('SELECT 1'),error=>['28000','28P01'].includes(error.code));}finally{await runtime.close();}
    const admin=new PostgresStore({connection:f.laboratory.connection,allowPlaintextLocalNetwork:true,poolSize:1});try{assert.equal((await admin.get("SELECT has_database_privilege($1,'postgres','CONNECT') allowed",state.runtime)).allowed,true);}finally{await admin.close();}
    await target.store.query('GRANT CREATE ON SCHEMA harbor TO "'+state.runtime+'"');await target.store.close();target=null;assert.equal((await runStorageOperator(f)).state,'pending');target=await adminTarget(f);assert.equal((await target.store.get("SELECT has_schema_privilege($1,'harbor','CREATE') allowed",state.runtime)).allowed,false);
  }finally{await target?.store.close();await f.close();}
});

test('a drained process is insufficient when the captured source still contains an active request',options,async()=>{
  const f=await fixture();try{
    const {DatabaseSync}=await import('node:sqlite'),db=new DatabaseSync(join(f.source,'gateway.sqlite'));try{db.exec("UPDATE requests SET status='queued' WHERE id=(SELECT id FROM requests LIMIT 1)");}finally{db.close();}
    await assert.rejects(runStorageOperator(f),{code:'transfer_source_not_drained'});await assert.rejects(lstat(join(f.runDirectory,'activation-status.enc')),{code:'ENOENT'});
  }finally{await f.close();}
});

test('replacing the source directory after capture invalidates actual source identity',options,async()=>{
  const f=await fixture();try{
    const {rename}=await import('node:fs/promises');await assert.rejects(runStorageOperator({...f,afterPhase:async phase=>{if(phase==='verified'){await rename(f.source,f.source+'-retired');await mkdir(f.source,{mode:0o700});}}}),{code:'storage_operator_source_identity'});await assert.rejects(lstat(join(f.runDirectory,'activation-status.enc')),{code:'ENOENT'});
  }finally{await f.close();}
});

test('actual Docker/systemd source and digest-bound candidate preflight positive proof remains unavailable without an explicitly authorized isolated host',{skip:'No authorized isolated root Docker/systemd environment; task-owned fixtures cannot fill production evidence'},()=>{});

test('known missing formal prerequisites return before any source fence, state file or target creation',options,async()=>{
  const f=await fixture();let admin;try{
    assert.throws(()=>{storageOperatorContext(f.context).adminOptions.connection={host:'untrusted-target'};},TypeError);
    admin=new PostgresStore({connection:f.laboratory.connection,allowPlaintextLocalNetwork:true,poolSize:1});const before=await admin.all('SELECT datname FROM pg_database ORDER BY datname');
    const context=await createTaskStorageOperatorReadinessContext({laboratory:f.laboratory,runtime:f.runtime,sourceChild:f.sourceChild,sourceDataDir:f.source,runDirectory:f.runDirectory,fileRoots:f.fileRoots});
    const result=await runStorageOperator({context});assert.equal(result.state,'blocked');assert.equal(result.completedPhase,'prerequisites');assert.equal(result.sourceTouched,false);assert.equal(result.targetPrepared,false);assert.equal(f.sourceChild.exitCode,null);assert.equal(f.runtime.proof(f.sourceChild).role,'gateway');assert.deepEqual(await admin.all('SELECT datname FROM pg_database ORDER BY datname'),before);assert.deepEqual(await (await import('node:fs/promises')).readdir(f.runDirectory),[]);
    await assert.rejects(runStorageOperator({context:{...context,scope:'task-owned'}}),{code:'storage_operator_context_required'});
  }finally{await admin?.close();await f.close();}
});

test('cancellation at the end of the real promotion transaction rolls back ownership and role changes',options,async()=>{
  const f=await fixture();let target;try{
    const controller=new AbortController();await assert.rejects(runStorageOperator({...f,signal:controller.signal,afterPhase:phase=>{if(phase==='promotion_before_commit')controller.abort(Error('synthetic cancellation before commit'));}}),/synthetic cancellation before commit/);
    target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey);assert.equal((await target.store.get('SELECT pg_get_userbyid(datdba) owner FROM pg_database WHERE datname=current_database()')).owner,target.manifest.user);assert.equal((await target.store.get('SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1) found',state.runtime)).found,false);await target.store.close();target=null;assert.equal((await runStorageOperator(f)).state,'pending');
  }finally{await target?.store.close();await f.close();}
});

test('expired private password rejects new authentication while an existing session remains until epoch-scoped recovery revokes it, even when source files changed',options,async()=>{
  const f=await fixture();let target,existing,child;
  try{
    await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),entry=join(f.laboratory.directory,'private-lease-child.mjs'),helper=new URL('../storage-operator-role-lease.mjs',import.meta.url).href,storeModule=new URL('../postgres-store.mjs',import.meta.url).href;
    await writeFile(entry,'import {enablePrivateRuntimeLease} from '+JSON.stringify(helper)+';import {PostgresStore} from '+JSON.stringify(storeModule)+';process.once("message",async data=>{try{const store=new PostgresStore({connection:data.connection,allowPlaintextLocalNetwork:true,poolSize:1});const lease=await enablePrivateRuntimeLease(store,data.state,{password:data.state.runtimePassword,timeoutMs:1500});process.send({enabled:true,lease});setInterval(()=>{},1000);}catch(error){process.send({error:error.code??"private_lease_failed"});process.exitCode=1;}});\n');
    child=f.runtime.start(process.execPath,[entry],{role:'helper',stdio:['ignore','pipe','pipe','ipc']});const lease=await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('private lease acknowledgement timeout')),10000);child.once('message',message=>{clearTimeout(timer);message.enabled?resolve(message.lease):reject(Error(message.error));});child.send({connection:{...f.laboratory.connection,database:target.manifest.database},state});});
    existing=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1,statementTimeoutMs:1000});assert.equal((await existing.get('SELECT 1 value')).value,1);
    const exited=new Promise(resolve=>child.once('exit',resolve));child.kill('SIGKILL');await exited;assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,true,'hard termination skips finally, so independent expiry is necessary');
    const deadline=performance.now()+5000;while((await target.store.get('SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint at')).at<lease.expiresMs){assert.ok(performance.now()<deadline);await existing.get('SELECT 1 value');await new Promise(resolve=>setTimeout(resolve,10));}
    assert.equal((await existing.get('SELECT 1 value')).value,1,'password expiry does not terminate an existing session');
    const late=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword},allowPlaintextLocalNetwork:true,poolSize:1});try{await assert.rejects(late.get('SELECT 1'),error=>error.code==='28P01');}finally{await late.close();}
    await target.store.close();target=null;await writeFile(join(f.homes,'synthetic-account','auth.json'),'{"synthetic":"changed while operator was down"}',{mode:0o600});await assert.rejects(runStorageOperator(f),{code:'transfer_files_changed'});
    target=await adminTarget(f);assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,false);assert.equal((await target.store.get('SELECT COUNT(*) n FROM pg_stat_activity WHERE usename=$1 AND application_name=$2',state.runtime,lease.application)).n,0);
  }finally{await existing?.close();await target?.store.close();await f.close();}
});

test('epoch cleanup ends only its private target sessions and preserves an existing session in another database',options,async()=>{
  const f=await fixture();let target,own,other;try{
    await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),lease=await enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:10000});
    own=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1});other=new PostgresStore({connection:{...f.laboratory.connection,database:'postgres',user:state.runtime,password:state.runtimePassword,application_name:'synthetic-other-database'},allowPlaintextLocalNetwork:true,poolSize:1});await own.get('SELECT 1');await other.get('SELECT 1');
    await assert.rejects(revokePrivateRuntimeLease(target.store,state),{code:'storage_operator_foreign_runtime_sessions'});assert.equal((await other.get('SELECT 1 value')).value,1);assert.equal((await target.store.get('SELECT COUNT(*) n FROM pg_stat_activity WHERE datid=$1::oid AND usename=$2 AND application_name=$3',state.databaseOid,state.runtime,lease.application)).n,0);
    await other.close();other=null;assert.equal((await revokePrivateRuntimeLease(target.store,state)).ownPreflightSessionsEnded,true);
  }finally{await own?.close();await other?.close();await target?.store.close();await f.close();}
});

test('non-password HBA authentication cannot claim a password-expiry lease and never enables runtime login',options,async()=>{
  const f=await fixture();let target;try{
    await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),path=(await target.store.get("SELECT current_setting('hba_file') path")).path,original=await readFile(path,'utf8');
    await writeFile(path,original.replaceAll('scram-sha-256','trust'),{mode:0o600});await target.store.get('SELECT pg_reload_conf()');await assert.rejects(enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:1000}),{code:'storage_operator_password_expiry_auth_unverified'});assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,false);
  }finally{await target?.store.close();await f.close();}
});

test('a changed context reclaims the authenticated old private epoch before rejecting configuration and never stops the new source',options,async()=>{
  const f=await fixture();let target,own,other,issuer;
  try{
    await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),entry=join(f.laboratory.directory,'abandoned-context-lease.mjs'),helper=new URL('../storage-operator-role-lease.mjs',import.meta.url).href,storeModule=new URL('../postgres-store.mjs',import.meta.url).href;
    await writeFile(entry,'import {enablePrivateRuntimeLease} from '+JSON.stringify(helper)+';import {PostgresStore} from '+JSON.stringify(storeModule)+';process.once("message",async data=>{try{const store=new PostgresStore({connection:data.connection,allowPlaintextLocalNetwork:true,poolSize:1});const lease=await enablePrivateRuntimeLease(store,data.state,{password:data.state.runtimePassword,timeoutMs:10000});process.send({enabled:true,lease});setInterval(()=>{},1000);}catch(error){process.send({error:error.code??"private_lease_failed"});process.exitCode=1;}});\n');
    issuer=f.runtime.start(process.execPath,[entry],{role:'helper',stdio:['ignore','pipe','pipe','ipc']});const lease=await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('private lease acknowledgement timeout')),10000);issuer.once('message',message=>{clearTimeout(timer);message.enabled?resolve(message.lease):reject(Error(message.error));});issuer.send({connection:{...f.laboratory.connection,database:target.manifest.database},state});});
    own=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1});other=new PostgresStore({connection:{...f.laboratory.connection,database:'postgres',user:state.runtime,password:state.runtimePassword,application_name:'synthetic-other-context-db'},allowPlaintextLocalNetwork:true,poolSize:1});await own.get('SELECT 1');await other.get('SELECT 1');const died=new Promise(resolve=>issuer.once('exit',resolve));issuer.kill('SIGKILL');await died;
    const replacement=f.runtime.start(process.execPath,[f.legacyEntry],{role:'gateway',env:{HARBOR_DATA_DIR:f.source,HARBOR_RUNTIME_DIR:f.homes},stdio:['ignore','pipe','pipe','ipc']});await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('replacement source readiness timeout')),10000);replacement.once('message',()=>{clearTimeout(timer);resolve();});replacement.once('exit',()=>{clearTimeout(timer);reject(Error('replacement source exited'));});});
    const changedRoots=structuredClone(f.fileRoots);changedRoots[1].destinationPrefix='/app/changed-runtime';const changed=await createTaskStorageOperatorContext({laboratory:f.laboratory,runtime:f.runtime,sourceChild:replacement,sourceDataDir:f.source,runDirectory:f.runDirectory,fileRoots:changedRoots});await target.store.close();target=null;
    await assert.rejects(runStorageOperator({...f,context:changed}),error=>error.code==='storage_operator_state_mismatch'&&error.runtimeLeaseCleanup==='foreign_sessions_remain');assert.equal(replacement.exitCode,null);assert.equal(f.runtime.proof(replacement).role,'gateway');assert.equal((await other.get('SELECT 1 value')).value,1);
    target=await adminTarget(f);assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,false);assert.equal((await target.store.get('SELECT COUNT(*) n FROM pg_stat_activity WHERE datid=$1::oid AND usename=$2 AND application_name=$3',state.databaseOid,state.runtime,lease.application)).n,0);
  }finally{await own?.close();await other?.close();await target?.store.close();await f.close();}
});

test('private preflight sessions have actual server statement, idle and transaction bounds independent of password expiry',options,async t=>{
  const f=await fixture();let target,session,borrowed;
  try{
    await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey);
    for(const kind of ['statement','idle','transaction']){
      const lease=await enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:1000});let errorCode,ended=false;const errorEvents=[];session=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1,statementTimeoutMs:1000});
      session.pool.on('connect',client=>{client.on('error',error=>{errorEvents.push(error.code??'uncoded');errorCode??=error.code??'missing_code';});client.on('end',()=>{ended=true;});});
      const limits=Object.fromEntries((await session.all("SELECT name,setting::int setting FROM pg_settings WHERE name IN ('statement_timeout','idle_session_timeout','transaction_timeout')")).map(row=>[row.name,row.setting]));assert.deepEqual(limits,{idle_session_timeout:1000,statement_timeout:1000,transaction_timeout:1000});
      if(kind==='statement')await assert.rejects(session.get('SELECT pg_sleep(3)'),error=>['57014','25P04'].includes(error.code));
      else{if(kind==='transaction'){borrowed=await session.pool.connect();await borrowed.query('BEGIN');}const deadline=performance.now()+5000;while(!errorCode){if(performance.now()>=deadline){const active=await target.store.all('SELECT state,wait_event FROM pg_stat_activity WHERE usename=$1 AND application_name=$2',state.runtime,lease.application);assert.fail('actual timeout event missing for '+kind+' '+JSON.stringify({ended,active,pool:session.status()}));}await new Promise(resolve=>setTimeout(resolve,10));}assert.equal(errorCode,kind==='idle'?'57P05':'25P04');borrowed?.release(Error('synthetic terminated private preflight'));borrowed=null;}
      t.diagnostic('native private '+kind+' error sequence: '+errorEvents.join(','));await session.close();session=null;assert.equal((await revokePrivateRuntimeLease(target.store,state)).ownPreflightSessionsEnded,true);
    }
  }finally{borrowed?.release(Error('synthetic cleanup'));await session?.close();await target?.store.close();await f.close();}
});


test('independent cleanup does not create targets or stop a live source when no operator state exists',options,async()=>{
 const f=await fixture();try{const result=await cleanupStorageOperator(f);assert.equal(result.state,'nothing_to_reclaim');assert.equal(result.deploymentAllowed,false);assert.equal(f.sourceChild.exitCode,null);const admin=new PostgresStore({connection:f.laboratory.connection});try{assert.equal((await admin.get("SELECT COUNT(*) n FROM pg_database WHERE datname LIKE 'harbor_rehearsal_%'")).n,0);assert.equal((await admin.get("SELECT to_regnamespace('harbor_operator_registry') oid")).oid,null);}finally{await admin.close();}await assert.rejects(cleanupStorageOperator({context:{...f.context},backupKey:f.backupKey}),{code:'storage_operator_context_required'});}finally{await f.close();}
});
test('independent cleanup reclaims only its private lease even with blocked readiness, preserves foreign sessions and is retryable',options,async()=>{
 const f=await fixture();let target,own,foreign;try{await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),lease=await enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:120000});
 own=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1});foreign=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:'synthetic-unrelated-cleanup'},allowPlaintextLocalNetwork:true,poolSize:1});await own.get('SELECT 1');const foreignPid=(await foreign.get('SELECT pg_backend_pid() pid')).pid;
 const replacement=f.runtime.start(process.execPath,[f.legacyEntry],{role:'gateway',env:{HARBOR_DATA_DIR:f.source,HARBOR_RUNTIME_DIR:f.homes},stdio:['ignore','pipe','pipe','ipc']});await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('cleanup replacement readiness timeout')),10000);replacement.once('message',()=>{clearTimeout(timer);resolve();});replacement.once('error',reject);});const context=await createTaskStorageOperatorReadinessContext({...f,sourceChild:replacement,sourceDataDir:f.source});
 const result=await cleanupStorageOperator({context,backupKey:f.backupKey});assert.equal(result.state,'partial');assert.equal(result.foreignSessionsRemain,true);assert.equal(result.ownPreflightSessionsEnded,true);assert.equal(replacement.exitCode,null);assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,false);assert.equal((await target.store.get('SELECT COUNT(*) n FROM pg_stat_activity WHERE usename=$1 AND application_name=$2',state.runtime,lease.application)).n,0);assert.equal((await foreign.get('SELECT pg_backend_pid() pid')).pid,foreignPid);await own.close();own=null;await foreign.close();foreign=null;assert.equal((await cleanupStorageOperator({context,backupKey:f.backupKey})).state,'reclaimed');assert.equal((await cleanupStorageOperator({context,backupKey:f.backupKey})).state,'reclaimed');assert.equal(replacement.exitCode,null);assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,3);
 }finally{await own?.close();await foreign?.close();await target?.store.close();await f.close();}
});


test('independent cleanup refuses a same-named replacement runtime role without disabling it',options,async()=>{
 const f=await fixture();let target;try{await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),quoted='"'+state.runtime+'"',old=(await target.store.get('SELECT oid::text oid FROM pg_roles WHERE rolname=$1',state.runtime)).oid;await target.store.query('DROP OWNED BY '+quoted);await target.store.query('DROP ROLE '+quoted);await target.store.query('CREATE ROLE '+quoted+' LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');const now=await target.store.get('SELECT oid::text oid,rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime);assert.notEqual(now.oid,old);await assert.rejects(cleanupStorageOperator(f),{code:'storage_operator_role_generation'});assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,true);assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,3);
 }finally{await target?.store.close();await f.close();}
});
test('independent cleanup cannot revoke a runtime once database activation is no longer pending',options,async()=>{
 const f=await fixture();let target;try{await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey);await enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:120000});await target.store.run("UPDATE harbor_operator.activation SET state='active' WHERE singleton=true");await assert.rejects(cleanupStorageOperator(f),{code:'storage_operator_cleanup_activation_changed'});assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,true);assert.equal((await target.store.get('SELECT revoked FROM harbor_operator.preflight_lease WHERE singleton=true')).revoked,false);
 }finally{await target?.store.close();await f.close();}
});


test('independent cleanup commits login revocation before the final scan can race with a real reconnect',options,async()=>{
 const f=await fixture();let target,reconnecting;const original=PostgresStore.prototype.get;let challenged=false;try{await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),lease=await enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:120000});PostgresStore.prototype.get=async function(sql,...args){const result=await original.call(this,sql,...args);if(!challenged&&sql==='SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=$1) present'&&args[0]===state.runtime){challenged=true;reconnecting=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1});await assert.rejects(reconnecting.get('SELECT 1'),error=>['28000','28P01'].includes(error.code));}return result;};const result=await cleanupStorageOperator(f);assert.equal(challenged,true);assert.equal(result.state,'reclaimed');assert.equal((await target.store.get('SELECT COUNT(*) n FROM pg_stat_activity WHERE usename=$1',state.runtime)).n,0);assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,false);
 }finally{PostgresStore.prototype.get=original;await reconnecting?.close();await target?.store.close();await f.close();}
});
test('independent cleanup preserves same-application sessions outside the issued window as partial without undoing NOLOGIN',options,async()=>{
 const f=await fixture();let target,unknown;try{await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey),lease=await enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:120000});unknown=new PostgresStore({connection:{...f.laboratory.connection,database:target.manifest.database,user:state.runtime,password:state.runtimePassword,application_name:lease.application},allowPlaintextLocalNetwork:true,poolSize:1});const pid=(await unknown.get('SELECT pg_backend_pid() pid')).pid,backend=(await target.store.get('SELECT floor(extract(epoch FROM backend_start)*1000)::bigint at FROM pg_stat_activity WHERE pid=$1',pid)).at;await target.store.run('UPDATE harbor_operator.preflight_lease SET issued_ms=$1 WHERE singleton=true',backend+1);const result=await cleanupStorageOperator(f);assert.equal(result.state,'partial');assert.equal(result.foreignSessionsRemain,true);assert.equal(result.ownPreflightSessionsEnded,true);assert.equal((await unknown.get('SELECT pg_backend_pid() pid')).pid,pid);assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,false);await unknown.close();unknown=null;assert.equal((await cleanupStorageOperator(f)).state,'reclaimed');
 }finally{await unknown?.close();await target?.store.close();await f.close();}
});


test('independent cleanup cancellation after the first real commit keeps NOLOGIN and permits later exact recovery',options,async()=>{
 const f=await fixture();let target;const originalQuery=PostgresStore.prototype.query,originalTransaction=PostgresStore.prototype.transaction,abort=new AbortController();let wrote=false,cancelled=false;try{await runStorageOperator(f);target=await adminTarget(f);const state=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey);await enablePrivateRuntimeLease(target.store,state,{password:state.runtimePassword,timeoutMs:120000});PostgresStore.prototype.query=async function(sql,...args){const result=await originalQuery.call(this,sql,...args);if(sql.startsWith('ALTER ROLE "'+state.runtime+'" NOLOGIN'))wrote=true;return result;};PostgresStore.prototype.transaction=async function(...args){const result=await originalTransaction.apply(this,args);if(wrote&&!cancelled){cancelled=true;abort.abort();}return result;};await assert.rejects(cleanupStorageOperator({...f,signal:abort.signal}),{name:'AbortError'});assert.equal(cancelled,true);PostgresStore.prototype.query=originalQuery;PostgresStore.prototype.transaction=originalTransaction;assert.equal((await target.store.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,false);assert.equal((await cleanupStorageOperator(f)).state,'reclaimed');
 }finally{PostgresStore.prototype.query=originalQuery;PostgresStore.prototype.transaction=originalTransaction;await target?.store.close();await f.close();}
});

test('read-only registry inspection never creates or adopts metadata and cannot allocate a target',options,async()=>{
 const f=await fixture(),admin=new PostgresStore({connection:f.laboratory.connection});let writable,readonly;
 try{
  const cluster=(await admin.get('SELECT system_identifier::text id FROM pg_control_system()')).id,args={key:f.backupKey,cluster,readOnly:true};
  await assert.rejects(openStorageOperatorRegistry(f.context,{...args,readOnly:'true'}),{code:'storage_operator_registry_mode'});
  await assert.rejects(openStorageOperatorRegistry(f.context,args),{code:'storage_operator_registry_foreign'});assert.equal((await admin.get("SELECT to_regnamespace('harbor_operator_registry') oid")).oid,null);
  await admin.query('CREATE SCHEMA harbor_operator_registry');try{await assert.rejects(openStorageOperatorRegistry(f.context,args),{code:'storage_operator_registry_foreign'});assert.equal((await admin.get("SELECT to_regclass('harbor_operator_registry.runs') oid")).oid,null);}finally{await admin.query('DROP SCHEMA harbor_operator_registry');}
  writable=await openStorageOperatorRegistry(f.context,{key:f.backupKey,cluster});const original=inspectStorageOperatorRegistry(writable);await closeStorageOperatorRegistry(writable);writable=null;
  readonly=await openStorageOperatorRegistry(f.context,{...args,expectedSchemaOid:original.schemaOid});assert.deepEqual(inspectStorageOperatorRegistry(readonly),original);assert.equal(await readStorageOperatorAllocation(readonly,{runId:'synthetic-unallocated'}),null);
  const state={};let saves=0;await assert.rejects(prepareStorageOperatorTarget(f.context,{registry:readonly,state,save:async()=>{saves++;},directory:join(f.runDirectory,'artifacts'),key:f.backupKey}),{code:'storage_operator_registry_readonly'});assert.deepEqual(state,{});assert.equal(saves,0);
  assert.equal((await admin.get('SELECT COUNT(*) n FROM harbor_operator_registry.runs')).n,0);assert.equal((await admin.get("SELECT COUNT(*) n FROM pg_database WHERE datname LIKE 'harbor_rehearsal_%'")).n,0);assert.equal(f.sourceChild.exitCode,null);await assert.rejects(lstat(join(f.runDirectory,'operator.enc')),{code:'ENOENT'});
  await admin.query('DROP SCHEMA harbor_operator_registry CASCADE');writable=await openStorageOperatorRegistry(f.context,{key:f.backupKey,cluster});assert.notEqual(inspectStorageOperatorRegistry(writable).schemaOid,original.schemaOid);await assert.rejects(readStorageOperatorAllocation(readonly,{runId:'synthetic-unallocated'}),{code:'storage_operator_registry_foreign'});
 }finally{if(readonly)await closeStorageOperatorRegistry(readonly);if(writable)await closeStorageOperatorRegistry(writable);await admin.close();await f.close();}
});

test('configured 500-row operator batches resume cancellation and retain complete row verification',options,async()=>{
 const f=await fixture({transferBatchRows:500,requestCount:1201});let target;
 try{
  assert.equal(storageOperatorContext(f.context).configuration.transferBatchRows,500);const cancelled=new AbortController(),first=[];
  await assert.rejects(runStorageOperator({...f,signal:cancelled.signal,afterBatch:row=>{if(row.table==='requests'){first.push(row.rows);cancelled.abort();}}}),{code:'transfer_interrupted'});assert.deepEqual(first,[500]);
  const resumed=[];const result=await runStorageOperator({...f,afterBatch:row=>{if(row.table==='requests')resumed.push(row.rows);}});assert.equal(result.state,'pending');assert.deepEqual(resumed,[500,201]);
  target=await adminTarget(f);assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1201);assert.equal((await target.store.get('SELECT COUNT(*) n FROM harbor.request_prices')).n,1201);
  const verified=await verifyTransferredSqliteSnapshot({directory:join(f.runDirectory,'artifacts'),key:f.backupKey,store:target.store,batchRows:500});assert.ok(verified.rows>=1201);
  await target.store.run("UPDATE harbor.requests SET model='synthetic-tampered' WHERE id=(SELECT id FROM harbor.requests ORDER BY id DESC LIMIT 1)");await assert.rejects(verifyTransferredSqliteSnapshot({directory:join(f.runDirectory,'artifacts'),key:f.backupKey,store:target.store,batchRows:500}),{code:'transfer_target_conflict'});
 }finally{await target?.store.close();await f.close();}
});

test('allocated legacy operator state rejects an explicit batch configuration change and still resumes its original context',options,async()=>{
 const f=await fixture();
 try{
  const trusted=storageOperatorContext(f.context);assert.equal(Object.hasOwn(trusted.configuration,'transferBatchRows'),false);
  await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='target_manifest_prepared')throw Error('synthetic allocated pause');}}),/synthetic allocated pause/);
  const original=await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey);assert.ok(original.allocation.databaseOid);
  const changed=await createTaskStorageOperatorContext({laboratory:f.laboratory,runtime:f.runtime,sourceChild:f.sourceChild,sourceDataDir:f.source,runDirectory:f.runDirectory,fileRoots:f.fileRoots,transferBatchRows:500});assert.notEqual(storageOperatorContext(changed).configurationDigest,trusted.configurationDigest);
  await assert.rejects(runStorageOperator({...f,context:changed}),{code:'storage_operator_state_mismatch'});assert.equal((await readManifest(join(f.runDirectory,'operator.enc'),f.backupKey)).configurationDigest,original.configurationDigest);assert.equal(f.sourceChild.exitCode,null,'configuration drift must refuse before fencing the owned source');
  assert.equal((await runStorageOperator(f)).state,'pending');
 }finally{await f.close();}
});
