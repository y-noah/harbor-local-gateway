import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdir,writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import {randomBytes} from 'node:crypto';
import {Store} from '../store.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres,createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createTaskStorageOperatorContext} from '../storage-operator-context.mjs';
import {runStorageOperator} from '../storage-operator.mjs';
import {readManifest,writeManifest} from '../migration-artifacts.mjs';
import {backupInventory} from '../postgres-backup-inventory.mjs';
import {createStorageOperatorResumeBinding,inspectStorageOperatorResumeBinding,closeStorageOperatorResumeBinding} from '../storage-operator-resume-binding.mjs';
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
async function baseline(f){return {...await backupInventory(f.admin),...await f.admin.get("SELECT system_identifier::text system_id,current_setting('data_directory') data_directory FROM pg_control_system()")};}
async function inspect(f,binding,expected){return inspectStorageOperatorResumeBinding(binding,{context:f.context,actual:await baseline(f),baselineExpected:expected,baselineConfigurationDigest:'a'.repeat(64)});}
test('resume preserves original inventory at every interrupted allocation/promotion phase',options,async()=>{
  const f=await fixture();try{
    const expected=await baseline(f);
    for(const stage of ['allocation_role_committed','allocation_database_witnessed','capturing','importing','verified','promotion_pending','promotion_committed_before_state','runtime_verified','pending']){
      await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase===stage)throw Error('interrupted '+stage);}}),new RegExp(stage));
      const binding=await createStorageOperatorResumeBinding(f);try{
        const proof=await inspect(f,binding,expected);assert.equal(proof.deploymentAllowed,false);assert.equal(proof.addedRoles.length,['promotion_committed_before_state','runtime_verified','pending'].includes(stage)?3:1);assert.equal(proof.addedDatabases.length,stage==='allocation_role_committed'?0:1);
        await assert.rejects(inspect(f,{...binding},expected),{code:'storage_operator_resume_binding_required'});
      }finally{closeStorageOperatorResumeBinding(binding);}
      await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_resume_binding_required'});
    }
  }finally{await f.close();}
});
test('unknown CREATE DATABASE acknowledgement is rejected without changing source or database',options,async()=>{
  const f=await fixture();try{
    const expected=await baseline(f);await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='allocation_database_created_unwitnessed')throw Error('unknown');}}),/unknown/);
    const before=await baseline(f),binding=await createStorageOperatorResumeBinding(f);try{await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_database_creation_unknown'});assert.deepEqual(await baseline(f),before);assert.equal(f.sourceChild.exitCode,null);}finally{closeStorageOperatorResumeBinding(binding);}
  }finally{await f.close();}
});
test('read-only inspection rejects unknown DB, role mutation, membership and state replacement',options,async()=>{
  const f=await fixture();try{
    const expected=await baseline(f);await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='allocation_role_committed')throw Error('interrupted');}}),/interrupted/);
    const binding=await createStorageOperatorResumeBinding(f),state=await f.state(),role='"'+state.allocation.intent.migration+'"';try{
      await f.admin.query('CREATE DATABASE foreign_database');await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_resume_inventory_changed'});await f.admin.query('DROP DATABASE foreign_database');
      await f.admin.query('ALTER ROLE '+role+' CREATEDB');await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_resume_role_unsafe'});await f.admin.query('ALTER ROLE '+role+' NOCREATEDB');
      await f.admin.query('CREATE ROLE foreign_role');await f.admin.query('GRANT foreign_role TO '+role);await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_resume_inventory_changed'});await f.admin.query('REVOKE foreign_role FROM '+role);await f.admin.query('DROP ROLE foreign_role');
      await inspect(f,binding,expected);assert.equal(f.sourceChild.exitCode,null);
      await writeManifest(join(f.runDirectory,'operator.enc'),{...state,phase:'capturing'},f.backupKey,{replace:true});await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_resume_state_changed'});
    }finally{closeStorageOperatorResumeBinding(binding);}
  }finally{await f.close();}
});
test('preparing without allocation permits only original cluster and does not create registry',options,async()=>{
  const f=await fixture();try{
    const expected=await baseline(f),{storageOperatorContext}=await import('../storage-operator-context.mjs'),trusted=storageOperatorContext(f.context);
    await writeManifest(join(f.runDirectory,'operator.enc'),{version:1,runId:'1'.repeat(36),configurationDigest:trusted.configurationDigest,cluster:expected.system_id,phase:'preparing',runtimePassword:'a'.repeat(43)},f.backupKey);
    const binding=await createStorageOperatorResumeBinding(f);try{
      assert.equal((await inspect(f,binding,expected)).addedRoles.length,0);assert.equal(await f.admin.get("SELECT 1 FROM pg_namespace WHERE nspname='harbor_operator_registry'"),undefined);assert.equal(f.sourceChild.exitCode,null);
      const signal=AbortSignal.abort(Error('cancelled'));await assert.rejects(inspectStorageOperatorResumeBinding(binding,{context:f.context,signal,actual:await baseline(f),baselineExpected:expected,baselineConfigurationDigest:'a'.repeat(64)}),/cancelled/);
    }finally{closeStorageOperatorResumeBinding(binding);}
  }finally{await f.close();}
});
test('same-name replacement roles and original baseline mutations cannot be adopted',options,async()=>{
  const f=await fixture();try{
    await f.admin.query('CREATE ROLE baseline_role');const expected=await baseline(f);
    await assert.rejects(runStorageOperator({...f,afterPhase:phase=>{if(phase==='allocation_role_committed')throw Error('interrupted');}}),/interrupted/);
    const binding=await createStorageOperatorResumeBinding(f);try{
      await f.admin.query('ALTER ROLE baseline_role CREATEDB');await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_resume_inventory_changed'});await f.admin.query('ALTER ROLE baseline_role NOCREATEDB');
      const state=await f.state(),role='"'+state.allocation.intent.migration+'"';await f.admin.query('DROP ROLE '+role);await f.admin.query('CREATE ROLE '+role+' LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');
      const before=await baseline(f);await assert.rejects(inspect(f,binding,expected),{code:'storage_operator_role_generation'});assert.deepEqual(await baseline(f),before);assert.equal(f.sourceChild.exitCode,null);
    }finally{closeStorageOperatorResumeBinding(binding);}
  }finally{await f.close();}
});
test('witnessed runtime lease stays usable and foreign sessions are never terminated',options,async()=>{
  const f=await fixture();let target,client,binding;try{
    const expected=await baseline(f);await runStorageOperator(f);const state=await f.state();
    target=await f.target();const {enablePrivateRuntimeLease,revokePrivateRuntimeLease}=await import('../storage-operator-role-lease.mjs');
    await enablePrivateRuntimeLease(target,state,{password:state.runtimePassword,timeoutMs:120000});
    client=new PostgresStore({connection:{...f.laboratory.connection,database:state.allocation.intent.database,user:state.runtime,password:state.runtimePassword,application_name:'foreign-session'},allowPlaintextLocalNetwork:true,poolSize:1});
    const before=await client.get('SELECT pg_backend_pid() pid'),roles=await baseline(f);binding=await createStorageOperatorResumeBinding(f);
    assert.equal((await inspect(f,binding,expected)).addedRoles.length,3);assert.deepEqual(await client.get('SELECT pg_backend_pid() pid'),before);assert.deepEqual(await baseline(f),roles);
    await client.close();client=null;await revokePrivateRuntimeLease(target,state);assert.equal((await inspect(f,binding,expected)).addedRoles.length,3);
    await assert.rejects(inspectStorageOperatorResumeBinding(binding,{context:f.context,actual:await baseline(f),baselineExpected:{...expected,memberships:[{}]},baselineConfigurationDigest:'a'.repeat(64)}),{code:'storage_operator_resume_baseline_changed'});
  }finally{if(binding)closeStorageOperatorResumeBinding(binding);await client?.close();await target?.close();await f.close();}
});
