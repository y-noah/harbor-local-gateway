import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {PostgresStore} from './postgres-store.mjs';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {openStorageOperatorRegistry,closeStorageOperatorRegistry,verifyStorageOperatorAllocation,readStorageOperatorRoleGenerations} from './storage-operator-state.mjs';
import {revokePrivateRuntimeLogin,revokePrivateRuntimeLease} from './storage-operator-role-lease.mjs';
import {acquireInstance} from './instance.mjs';
import {artifactKey,privateDirectory,readManifest,canonical,transferError} from './migration-artifacts.mjs';
const fail=code=>{throw transferError(code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');

// Cleanup never consumes deployment readiness, fences a source, allocates a
// target or promotes a receipt. Only authenticated, witnessed epochs qualify.
export async function cleanupStorageOperator({context,backupKey,signal}={}){
 signal?.throwIfAborted();const trusted=storageOperatorContext(context);artifactKey(backupKey);await privateDirectory(trusted.configuration.runDirectory);
 const operation=await acquireInstance(trusted.configuration.runDirectory);let admin,target,registry;
 try{
  let state;try{state=await readManifest(join(trusted.configuration.runDirectory,'operator.enc'),backupKey,{signal});}catch(error){if(error.code==='ENOENT')return {state:'nothing_to_reclaim',sourceTouched:false,targetPrepared:false,deploymentAllowed:false};throw error;}
  if(state.version!==1||!['promotion_pending','runtime_verified','pending'].includes(state.phase)||!/^[a-f0-9-]{36}$/.test(state.runId??'')||!/^\d+$/.test(state.registrySchemaOid??'')||!/^\d+$/.test(state.databaseOid??'')||!state.allocation||!/^harbor_rehearsal_[a-f0-9]{24}$/.test(state.allocation.intent?.database??'')||!/^harbor_runtime_[a-f0-9]{24}$/.test(state.runtime??'')||!/^[a-f0-9]{64}$/.test(state.bindingDigest??''))fail('storage_operator_cleanup_state_unwitnessed');
  const manifest=await readManifest(join(trusted.configuration.runDirectory,'artifacts','target.enc'),backupKey,{signal}),suffix=state.allocation.intent.database.slice('harbor_rehearsal_'.length);
  if(manifest.version!==1||manifest.rehearsal!==true||manifest.database!==state.allocation.intent.database||manifest.user!=='harbor_migration_'+suffix||manifest.systemIdentifier!==state.cluster||state.runtime!=='harbor_runtime_'+suffix||state.owner!=='harbor_owner_'+suffix||state.targetDigest!==hash({cluster:state.cluster,database:manifest.database,user:manifest.user,schema:manifest.schemaChecksums}))fail('storage_operator_cleanup_target_mismatch');
  admin=new PostgresStore({...trusted.adminOptions,poolSize:1});const cluster=(await admin.get('SELECT system_identifier::text value FROM pg_control_system()')).value;
  if(cluster!==state.cluster||trusted.configuration.expectedCluster!==null&&trusted.configuration.expectedCluster!==cluster)fail('storage_operator_cluster_mismatch');
  registry=await openStorageOperatorRegistry(context,{key:backupKey,cluster,expectedSchemaOid:state.registrySchemaOid});await verifyStorageOperatorAllocation(registry,state,{manifest,databaseOid:state.databaseOid});
  const raw=admin.pool.options;target=new PostgresStore({connection:{host:raw.host,port:raw.port,user:raw.user,password:raw.password,database:manifest.database,...(raw.ssl?{ssl:raw.ssl}:{})},poolSize:1,allowPlaintextLocalNetwork:trusted.adminOptions.allowPlaintextLocalNetwork===true});
  const identity=await target.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid');if(identity.name!==manifest.database||identity.oid!==state.databaseOid)fail('storage_operator_database_identity');signal?.throwIfAborted();
  const locked=action=>target.transaction(async()=>{
   // Catalog row locks prevent a same-named replacement role between its OID
   // witness and ALTER ROLE. The activation row also excludes promotion races.
   await target.all('SELECT oid FROM pg_catalog.pg_authid WHERE rolname=ANY($1::text[]) ORDER BY oid FOR UPDATE',[manifest.user,state.owner,state.runtime]);await readStorageOperatorRoleGenerations(target,state);
   const activation=await target.get('SELECT * FROM harbor_operator.activation WHERE singleton=true FOR UPDATE');if(!activation||activation.state!=='pending'||activation.run_id!==state.runId||activation.binding!==state.bindingDigest||activation.database_oid!==state.databaseOid||activation.runtime!==state.runtime)fail('storage_operator_cleanup_activation_changed');signal?.throwIfAborted();
   return action();
  });
  // Authentication must see committed NOLOGIN before the final activity scan.
  // Later cancellation/failed cleanup must never roll that revocation back.
  const revoked=await locked(async()=>{
   if(!(await target.get("SELECT to_regclass('harbor_operator.preflight_lease') relation")).relation){if((await target.get('SELECT rolcanlogin FROM pg_roles WHERE rolname=$1',state.runtime)).rolcanlogin!==false)fail('storage_operator_cleanup_lease_missing');return false;}
   await revokePrivateRuntimeLogin(target,state);return true;
  });
  if(!revoked)return {state:'no_private_lease',loginRevoked:true,ownPreflightSessionsEnded:true,foreignSessionsRemain:false,sourceTouched:false,targetPrepared:false,deploymentAllowed:false};
  signal?.throwIfAborted();return await locked(async()=>{
   let foreign=false;try{await revokePrivateRuntimeLease(target,state);}catch(error){if(error.code!=='storage_operator_foreign_runtime_sessions')throw error;foreign=true;}
   await readStorageOperatorRoleGenerations(target,state);signal?.throwIfAborted();return {state:foreign?'partial':'reclaimed',loginRevoked:true,ownPreflightSessionsEnded:true,foreignSessionsRemain:foreign,sourceTouched:false,targetPrepared:false,deploymentAllowed:false};
  });
 }finally{
  const results=await Promise.allSettled([target?.close(),admin?.close(),registry&&closeStorageOperatorRegistry(registry)]);try{await operation.release();}finally{if(results.some(row=>row.status==='rejected'))fail('storage_operator_cleanup_unknown');}
 }
}
