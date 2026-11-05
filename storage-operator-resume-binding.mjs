import {createHash} from 'node:crypto';
import {join} from 'node:path';
import {PostgresStore} from './postgres-store.mjs';
import {canonical,artifactKey,readManifest,transferError} from './migration-artifacts.mjs';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {backupInventory} from './postgres-backup-inventory.mjs';
import {openStorageOperatorRegistry,closeStorageOperatorRegistry,readStorageOperatorAllocation,readStorageOperatorRoleGenerations} from './storage-operator-state.mjs';

const bindings=new WeakMap(),fail=code=>{throw transferError(code);};
const digest=value=>createHash('sha256').update(canonical(value)).digest('hex');
const phases=new Set(['preparing','capturing','importing','verified','promotion_pending','runtime_verified','pending']);
function record(binding){const value=bindings.get(binding);if(!value||value.closed)fail('storage_operator_resume_binding_required');return value;}
async function stateOf(value,signal){
  signal?.throwIfAborted();const trusted=storageOperatorContext(value.context),state=await readManifest(join(trusted.configuration.runDirectory,'operator.enc'),value.key,{signal});
  if(state.version!==1||!/^[-a-f0-9]{36}$/.test(state.runId??'')||!phases.has(state.phase)||!/^[A-Za-z0-9_-]{43}$/.test(state.runtimePassword??'')||state.configurationDigest!==trusted.configurationDigest||trusted.configuration.expectedCluster!==null&&state.cluster!==trusted.configuration.expectedCluster)fail('storage_operator_resume_state_mismatch');
  if(value.stateDigest&&digest(state)!==value.stateDigest)fail('storage_operator_resume_state_changed');return state;
}
export async function createStorageOperatorResumeBinding({context,backupKey,signal}={}){
  storageOperatorContext(context);artifactKey(backupKey);const value={context,key:Buffer.from(backupKey),closed:false};
  try{const state=await stateOf(value,signal);value.stateDigest=digest(state);const binding=Object.freeze({});bindings.set(binding,value);return binding;}catch(error){value.key.fill(0);throw error;}
}
export function closeStorageOperatorResumeBinding(binding){const value=record(binding);value.closed=true;value.key.fill(0);}
function inventory(value){if(!value||!['databases','roles','memberships'].every(key=>Array.isArray(value[key])))fail('storage_operator_resume_inventory_required');return {databases:value.databases,roles:value.roles,memberships:value.memberships};}
function safeRole(row,kind){
  if(!row||['superuser','replication','create_database','create_role','inherit','bypass_rls'].some(key=>row[key]!==false)||row.connection_limit!==-1||row.configuration!==null||typeof row.can_login!=='boolean')fail('storage_operator_resume_role_unsafe');
  if(kind==='owner'&&(row.can_login||row.valid_until!==null)||kind==='migration'&&row.valid_until!==null)fail('storage_operator_resume_role_unsafe');
  if(kind==='runtime'&&!row.can_login&&row.valid_until!==null&&Number(row.valid_until)!==0)fail('storage_operator_resume_role_unsafe');
}
// This capability authorizes only a read-only comparison against the original
// baseline supplied by the fixed-profile consumer. It never owns source fencing,
// registry creation, lease cleanup, or permission to make a replacement backup.
export async function inspectStorageOperatorResumeBinding(binding,{context,actual,baselineExpected,baselineConfigurationDigest,signal}={}){
  const value=record(binding);if(context!==value.context)fail('storage_operator_resume_context_mismatch');
  if(!/^[a-f0-9]{64}$/.test(baselineConfigurationDigest??''))fail('storage_operator_resume_baseline_required');
  const expected=structuredClone(inventory(baselineExpected)),state=await stateOf(value,signal),trusted=storageOperatorContext(context);
  if(state.baseline){if(state.baseline.operatorConfigurationDigest!==trusted.configurationDigest||state.baseline.backupConfigurationDigest!==baselineConfigurationDigest)fail('storage_operator_resume_baseline_mismatch');}
  else if(trusted.scope!=='task-owned')fail('storage_operator_resume_baseline_required');
  const baselineDigest=digest({expected:baselineExpected,configurationDigest:baselineConfigurationDigest});if(value.baselineDigest&&value.baselineDigest!==baselineDigest)fail('storage_operator_resume_baseline_changed');value.baselineDigest=baselineDigest;
  const admin=new PostgresStore({...trusted.adminOptions,poolSize:1});let registry,target;
  try{
    const identity=await admin.get('SELECT (SELECT system_identifier::text FROM pg_control_system()) cluster,current_setting(\'data_directory\') data_directory');
    if(identity.cluster!==state.cluster||baselineExpected.system_id!==state.cluster||baselineExpected.data_directory!==identity.data_directory||actual?.system_id!==identity.cluster||actual?.data_directory!==identity.data_directory)fail('storage_operator_resume_cluster_mismatch');
    const observed=await backupInventory(admin);if(canonical(inventory(actual))!==canonical(observed))fail('storage_operator_resume_observation_changed');
    let saved;const namespace=await admin.get("SELECT oid::text oid FROM pg_namespace WHERE nspname='harbor_operator_registry'");
    if(namespace){registry=await openStorageOperatorRegistry(context,{key:value.key,cluster:state.cluster,expectedSchemaOid:state.registrySchemaOid,readOnly:true});saved=await readStorageOperatorAllocation(registry,state);}
    const allowedRoles=new Map(),allowedDatabases=[];
    if(state.allocation){
      const allocation=state.allocation,intent=allocation.intent;
      if(!intent||!/^harbor_rehearsal_[a-f0-9]{24}$/.test(intent.database??'')||intent.nonce!==intent.database.slice(17)||intent.migration!=='harbor_migration_'+intent.nonce||!/^[A-Za-z0-9_-]{43}$/.test(allocation.migrationPassword??''))fail('storage_operator_resume_allocation_binding');
      if(!saved){if(state.phase!=='preparing'||allocation.stage!=='intent'||allocation.migrationOid||allocation.databaseOid)fail('storage_operator_resume_allocation_missing');}
      else{
        if(!/^\d+$/.test(saved.migrationOid??'')||!['role_created','database_creating','database_witnessed'].includes(saved.stage)||allocation.migrationOid&&allocation.migrationOid!==saved.migrationOid||allocation.databaseOid&&allocation.databaseOid!==saved.databaseOid)fail('storage_operator_resume_allocation_binding');
        allowedRoles.set(intent.migration,{oid:saved.migrationOid,kind:'migration'});
        if(!saved.databaseOid){if(saved.stage!=='role_created'||observed.databases.some(row=>row.name===intent.database)||state.phase!=='preparing')fail('storage_operator_database_creation_unknown');}
        else{
          if(saved.stage!=='database_witnessed'||state.databaseOid&&state.databaseOid!==saved.databaseOid)fail('storage_operator_resume_allocation_binding');
          allowedDatabases.push({name:intent.database,oid:saved.databaseOid});
          const db=await admin.get('SELECT oid::text oid,datdba::text owner FROM pg_database WHERE datname=$1',intent.database);
          if(db?.oid!==saved.databaseOid)fail('storage_operator_resume_database_identity');
          const raw=admin.pool.options;target=new PostgresStore({connection:{host:raw.host,port:raw.port,user:raw.user,password:raw.password,database:intent.database,...(raw.ssl?{ssl:raw.ssl}:{})},allowPlaintextLocalNetwork:trusted.adminOptions.allowPlaintextLocalNetwork===true,poolSize:1});
          const owner='harbor_owner_'+intent.nonce,runtime='harbor_runtime_'+intent.nonce,hasRoles=observed.roles.some(row=>[owner,runtime].includes(row.name));
          if(hasRoles||state.roleGenerations||['runtime_verified','pending'].includes(state.phase)){
            if(!['promotion_pending','runtime_verified','pending'].includes(state.phase)||state.owner!==owner||state.runtime!==runtime)fail('storage_operator_resume_role_generation');
            const generations=await readStorageOperatorRoleGenerations(target,state);allowedRoles.set(owner,{oid:generations.ownerOid,kind:'owner'});allowedRoles.set(runtime,{oid:generations.runtimeOid,kind:'runtime'});
          }
          if(db.owner!==(allowedRoles.get(owner)?.oid??saved.migrationOid))fail('storage_operator_resume_database_identity');
        }
      }
    }else if(state.phase!=='preparing'||saved)fail('storage_operator_resume_allocation_missing');
    const extraRoles=[];for(const [name,allowed] of allowedRoles){
      if(expected.roles.some(row=>row.name===name||row.oid===allowed.oid))fail('storage_operator_resume_baseline_collision');
      const row=observed.roles.find(row=>row.name===name);if(row?.oid!==allowed.oid)fail('storage_operator_role_generation');safeRole(row,allowed.kind);
      if(allowed.kind==='runtime'&&row.can_login){
        const lease=await target.get('SELECT * FROM harbor_operator.preflight_lease WHERE singleton=true');
        if(!lease||lease.revoked||lease.run_id!==state.runId||lease.binding!==state.bindingDigest||lease.database_oid!==state.databaseOid||lease.runtime!==name||!(lease.expires_ms>lease.issued_ms)||Number(row.valid_until)*1000!==lease.expires_ms)fail('storage_operator_resume_runtime_lease');
      }
      extraRoles.push(row);
    }
    for(const row of allowedDatabases)if(expected.databases.some(old=>old.name===row.name||old.oid===row.oid))fail('storage_operator_resume_baseline_collision');
    const byName=(a,b)=>a.name.localeCompare(b.name,'en');
    // Keep the baseline rows byte-for-byte: only this run's proven additions may
    // be removed from the current inventory before the exact comparison.
    const remaining={databases:observed.databases.filter(row=>!allowedDatabases.some(allowed=>canonical(allowed)===canonical(row))),roles:observed.roles.filter(row=>!allowedRoles.has(row.name)),memberships:observed.memberships};
    if(canonical(remaining)!==canonical(expected)||allowedDatabases.some(row=>!observed.databases.some(found=>canonical(found)===canonical(row))))fail('storage_operator_resume_inventory_changed');
    signal?.throwIfAborted();await stateOf(value,signal);record(binding);
    const second=await backupInventory(admin);if(canonical(second)!==canonical(observed))fail('storage_operator_resume_observation_changed');
    if(registry&&canonical(await readStorageOperatorAllocation(registry,state))!==canonical(saved))fail('storage_operator_resume_allocation_changed');
    if(allowedRoles.size===3)await readStorageOperatorRoleGenerations(target,state);
    signal?.throwIfAborted();await stateOf(value,signal);record(binding);
    return Object.freeze({version:1,runId:state.runId,phase:state.phase,stateDigest:value.stateDigest,baselineConfigurationDigest,baselineInventoryDigest:digest(expected),inventoryDigest:digest(observed),allocationDigest:digest(saved??null),addedDatabases:Object.freeze(allowedDatabases.map(row=>Object.freeze({...row}))),addedRoles:Object.freeze(extraRoles.sort(byName).map(row=>Object.freeze({name:row.name,oid:row.oid}))),deploymentAllowed:false});
  }finally{await Promise.allSettled([target?.close(),admin.close(),registry&&closeStorageOperatorRegistry(registry)]);}
}
