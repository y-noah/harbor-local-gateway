import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,writeFile,readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {Store} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createTaskStorageOperatorContext} from '../storage-operator-context.mjs';
import {runStorageOperator} from '../storage-operator.mjs';
import {readManifest,writeManifest} from '../migration-artifacts.mjs';
import {withPendingStorageOperatorBackup,inspectPendingStorageOperatorBackup} from '../storage-operator-backup-binding.mjs';
import {createRehearsalTarget} from '../sqlite-postgres-transfer.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,options={skip:!binDirectory?'requires isolated PG18':false,timeout:120000};
async function fixture(){
  const laboratory=await createPrivatePostgres({binDirectory,libraryDirectory}),runtime=createOwnedRuntime(),source=join(laboratory.directory,'source'),runDirectory=join(laboratory.directory,'operator'),backupKey=randomBytes(32),activationKey=randomBytes(32),master=randomBytes(32);let admin;
  try{
    await mkdir(source,{mode:0o700});await mkdir(runDirectory,{mode:0o700});await writeFile(join(source,'key-encryption.key'),master,{mode:0o600});await writeFile(join(source,'settings.json'),JSON.stringify({adminPasswordHash:{salt:'a'.repeat(32),digest:'b'.repeat(64)}}),{mode:0o600});const sqlite=new Store(join(source,'gateway.sqlite'),{encryptionKey:master});sqlite.close();
    const entry=join(laboratory.directory,'legacy.mjs');await writeFile(entry,'import {acquireInstance} from '+JSON.stringify(new URL('../instance.mjs',import.meta.url).href)+';const lease=await acquireInstance(process.env.HARBOR_DATA_DIR);const timer=setInterval(()=>{},1000);process.send({ready:true});process.on("SIGTERM",async()=>{clearInterval(timer);await lease.release();process.exit(0)});');
    const sourceChild=runtime.start(process.execPath,[entry],{role:'gateway',env:{HARBOR_DATA_DIR:source},stdio:['ignore','pipe','pipe','ipc']});await new Promise((resolve,reject)=>{const timer=setTimeout(()=>reject(Error('fixture timeout')),10000);sourceChild.once('message',()=>{clearTimeout(timer);resolve();});sourceChild.once('error',reject);});
    const context=await createTaskStorageOperatorContext({laboratory,runtime,sourceChild,sourceDataDir:source,runDirectory,fileRoots:[{id:'data',source}]});admin=new PostgresStore({connection:laboratory.connection,allowPlaintextLocalNetwork:true,poolSize:1});
    return {laboratory,runtime,context,sourceChild,runDirectory,backupKey,activationKey,admin,state:()=>readManifest(join(runDirectory,'operator.enc'),backupKey),target:async()=>{const manifest=await readManifest(join(runDirectory,'artifacts','target.enc'),backupKey);return new PostgresStore({connection:{...laboratory.connection,database:manifest.database},allowPlaintextLocalNetwork:true,poolSize:1});},close:async()=>{await admin.close();await runtime.close();await laboratory.close();}};
  }catch(error){await admin?.close();await runtime.close();await laboratory.close();throw error;}
}
test('allocation and promotion commit acknowledgement loss recover only from authenticated actual OID witnesses',options,async()=>{
  const f=await fixture();try{
    for(const stage of ['allocation_role_committed','allocation_database_witnessed','promotion_committed_before_state']){
      await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase===stage)throw Error('synthetic '+stage);}}),new RegExp(stage));
    }
    const before=await f.state();assert.equal(before.roleGenerations,undefined);const result=await runStorageOperator(f);assert.equal(result.state,'pending');const state=await f.state();for(const oid of [state.allocation.databaseOid,state.allocation.migrationOid,state.roleGenerations.ownerOid,state.roleGenerations.runtimeOid])assert.match(oid,/^\d+$/);
    const registry=await f.admin.get('SELECT record::text record FROM harbor_operator_registry.runs WHERE run_id=$1',state.runId);assert.ok(!registry.record.includes(state.allocation.migrationPassword));assert.equal(state.databaseOid,state.allocation.databaseOid);
    let saved;await withPendingStorageOperatorBackup(f,async binding=>{saved=binding;const inspected=await inspectPendingStorageOperatorBackup(binding);assert.equal(inspected.binding.runId,state.runId);assert.equal(inspected.binding.database.oid,state.databaseOid);assert.equal(inspected.binding.roles.length,3);assert.equal(inspected.binding.deploymentAllowed,false);assert.equal(inspected.binding.applicationMaterialsVerified,false);assert.ok(Object.isFrozen(inspected.binding));await assert.rejects(inspectPendingStorageOperatorBackup({...binding}),{code:'storage_operator_pending_backup_binding_required'});});await assert.rejects(inspectPendingStorageOperatorBackup(saved),{code:'storage_operator_pending_backup_binding_required'});
  }finally{await f.close();}
});
test('CREATE DATABASE without its first OID witness stays unknown and preserves source and database',options,async()=>{
  const f=await fixture();try{
    await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='allocation_database_created_unwitnessed')throw Error('synthetic lost create acknowledgement');}}),/lost create acknowledgement/);const state=await f.state();assert.equal(state.allocation.databaseOid,undefined);assert.equal(f.sourceChild.exitCode,null);assert.equal((await f.admin.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.allocation.intent.migration)).rolcanlogin,false);
    await assert.rejects(runStorageOperator(f),{code:'storage_operator_database_creation_unknown'});assert.equal(f.sourceChild.exitCode,null);assert.ok(await f.admin.get('SELECT oid FROM pg_database WHERE datname=$1',state.allocation.intent.database));assert.equal((await f.admin.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.allocation.intent.migration)).rolcanlogin,false);
  }finally{await f.close();}
});
test('registry rejects an attached executable trigger before any further registry write or source fence',options,async()=>{
  const f=await fixture();try{
    await assert.rejects(runStorageOperator({...f,afterPhase:async phase=>{if(phase==='allocation_role_committed'){await f.admin.query('CREATE TABLE public.trigger_effects(id int)');await f.admin.query("CREATE FUNCTION public.registry_side_effect() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO public.trigger_effects VALUES(1); RETURN NEW; END $$");await f.admin.query('CREATE TRIGGER unexpected BEFORE INSERT OR UPDATE ON harbor_operator_registry.runs FOR EACH ROW EXECUTE FUNCTION public.registry_side_effect()');}}}),{code:'storage_operator_registry_foreign'});
    await assert.rejects(runStorageOperator(f),{code:'storage_operator_registry_foreign'});assert.equal((await f.admin.get('SELECT COUNT(*) n FROM public.trigger_effects')).n,0);assert.equal(f.sourceChild.exitCode,null);
  }finally{await f.close();}
});
test('missing generation table cannot route cleanup into a same-name foreign LOGIN role',options,async()=>{
  const f=await fixture();let target;try{
    await runStorageOperator(f);const state=await f.state();target=await f.target();await target.query('DROP TABLE harbor_operator.role_generations');await target.query('DROP OWNED BY "'+state.runtime+'"');await target.query('DROP ROLE "'+state.runtime+'"');await target.query('CREATE ROLE "'+state.runtime+'" LOGIN PASSWORD \'synthetic-foreign-password\'');await target.query('ALTER ROLE "'+state.runtime+'" SET statement_timeout=12345');const before=await target.get('SELECT oid::text oid,rolcanlogin,rolpassword FROM pg_authid WHERE rolname=$1',state.runtime),settings=await target.all('SELECT setdatabase::text database,setconfig FROM pg_db_role_setting WHERE setrole=$1::oid',before.oid);
    await target.close();target=null;await assert.rejects(runStorageOperator(f),{code:'storage_operator_allocation_generation_missing'});const after=await f.admin.get('SELECT oid::text oid,rolcanlogin,rolpassword FROM pg_authid WHERE rolname=$1',state.runtime);assert.deepEqual(after,before);assert.deepEqual(await f.admin.all('SELECT setdatabase::text database,setconfig FROM pg_db_role_setting WHERE setrole=$1::oid',before.oid),settings);
  }finally{await target?.close();await f.close();}
});
test('explicit fresh owned run directory can reprepare without adopting or deleting an unknown earlier database',options,async()=>{
  const f=await fixture();try{
    await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='allocation_database_created_unwitnessed')throw Error('synthetic unknown create');}}),/unknown create/);const old=await f.state(),runDirectory=join(f.laboratory.directory,'explicit-reprepare');await mkdir(runDirectory,{mode:0o700});const source=join(f.laboratory.directory,'source'),context=await createTaskStorageOperatorContext({laboratory:f.laboratory,runtime:f.runtime,sourceChild:f.sourceChild,sourceDataDir:source,runDirectory,fileRoots:[{id:'data',source}]});
    const result=await runStorageOperator({...f,context});assert.equal(result.state,'pending');const current=await readManifest(join(runDirectory,'operator.enc'),f.backupKey);assert.notEqual(current.allocation.intent.database,old.allocation.intent.database);assert.ok(await f.admin.get('SELECT oid FROM pg_database WHERE datname=$1',old.allocation.intent.database));assert.equal((await f.admin.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',old.allocation.intent.migration)).rolcanlogin,false);
  }finally{await f.close();}
});
test('target manifest from another authenticated run cannot fill the target-state acknowledgement gap',options,async()=>{
  const f=await fixture();let target;try{
    await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='target_manifest_prepared')throw Error('synthetic manifest acknowledgement gap');}}),/acknowledgement gap/);const state=await f.state();assert.equal(state.databaseOid,undefined);assert.equal(state.targetDigest,undefined);assert.equal(f.sourceChild.exitCode,null);
    const other=join(f.laboratory.directory,'other-approved-target');await mkdir(other,{mode:0o700});await createRehearsalTarget({connection:f.laboratory.connection,allowPlaintextLocalNetwork:true},{directory:other,key:f.backupKey});const manifest=await readManifest(join(other,'target.enc'),f.backupKey);await writeFile(join(f.runDirectory,'artifacts','target.enc'),await readFile(join(other,'target.enc')));
    await assert.rejects(runStorageOperator(f),{code:'storage_operator_target_allocation_mismatch'});assert.equal(f.sourceChild.exitCode,null);assert.equal((await f.state()).databaseOid,undefined);
    for(const database of [state.allocation.intent.database,manifest.database]){target=new PostgresStore({connection:{...f.laboratory.connection,database},allowPlaintextLocalNetwork:true,poolSize:1});assert.equal((await target.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);assert.equal((await target.get('SELECT pg_get_userbyid(datdba) owner FROM pg_database WHERE datname=current_database()')).owner,database===manifest.database?manifest.user:state.allocation.intent.migration);await target.close();target=null;}
  }finally{await target?.close();await f.close();}
});
test('foreign same-name registry and replaced registry relation fail closed before source fencing',options,async()=>{
  const f=await fixture();try{await f.admin.query('CREATE SCHEMA harbor_operator_registry');await f.admin.query('CREATE TABLE harbor_operator_registry.runs(unrelated text)');await assert.rejects(runStorageOperator(f),{code:'storage_operator_registry_foreign'});assert.equal(f.sourceChild.exitCode,null);assert.equal((await f.admin.get('SELECT COUNT(*) n FROM pg_database WHERE datname LIKE $1','harbor_rehearsal_%')).n,0);}finally{await f.close();}
  const g=await fixture();try{await runStorageOperator(g);const row=await g.admin.get("SELECT obj_description(oid,'pg_namespace') value FROM pg_namespace WHERE nspname='harbor_operator_registry'");await g.admin.query('ALTER TABLE harbor_operator_registry.runs RENAME TO foreign_runs');await g.admin.query('CREATE TABLE harbor_operator_registry.runs(run_id text PRIMARY KEY,record jsonb NOT NULL)');await assert.rejects(withPendingStorageOperatorBackup(g,async()=>{}),{code:'storage_operator_registry_foreign'});assert.ok(row.value);}finally{await g.close();}
});
test('same-name runtime role replacement is rejected before cleanup can alter the foreign role',options,async()=>{
  const f=await fixture();let target;try{await runStorageOperator(f);const state=await f.state();target=await f.target();await target.query('DROP OWNED BY "'+state.runtime+'"');await target.query('DROP ROLE "'+state.runtime+'"');await target.query('CREATE ROLE "'+state.runtime+'" LOGIN');await target.close();target=null;await assert.rejects(runStorageOperator(f),{code:'storage_operator_role_generation'});assert.equal((await f.admin.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin,true);await assert.rejects(withPendingStorageOperatorBackup(f,async()=>{}),{code:'storage_operator_role_generation'});}finally{await target?.close();await f.close();}
});
test('old pending state without role generation is reclaimed but cannot mint a new binding',options,async()=>{
  const f=await fixture();try{await runStorageOperator(f);const state=await f.state();delete state.roleGenerations;delete state.allocation;await writeManifest(join(f.runDirectory,'operator.enc'),state,f.backupKey,{replace:true});await assert.rejects(withPendingStorageOperatorBackup(f,async()=>{}),{code:'storage_operator_pending_backup_state_required'});await assert.rejects(runStorageOperator(f),{code:'storage_operator_allocation_generation_missing'});assert.ok(await readFile(join(f.runDirectory,'artifacts','target.enc')));}finally{await f.close();}
});
