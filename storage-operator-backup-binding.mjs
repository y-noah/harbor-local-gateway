import {createHash} from 'node:crypto';
import {join,resolve} from 'node:path';
import {lstat} from 'node:fs/promises';
import {PostgresStore} from './postgres-store.mjs';
import {acquireInstance} from './instance.mjs';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {openStorageOperatorRegistry,closeStorageOperatorRegistry,verifyStorageOperatorAllocation,readStorageOperatorRoleGenerations} from './storage-operator-state.mjs';
import {acquireSqliteSourceLease,assertSqliteSourceLease,inspectSqliteSourceLease,verifyTransferredSqliteSnapshot} from './sqlite-postgres-transfer.mjs';
import {verifyStorageOperatorMaterialContents} from './storage-operator.mjs';
import {verifyStorageOperatorSourceFence} from './storage-operator-source-supervisor.mjs';
import {artifactKey,readManifest,canonical,digestFile,verifyFileSetSources,privateDirectory,transferError} from './migration-artifacts.mjs';

const bindings=new WeakMap(),fail=code=>{throw transferError(code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
function record(binding){const value=bindings.get(binding);if(!value||value.closed)fail('storage_operator_pending_backup_binding_required');return value;}
const freeze=value=>{if(value&&typeof value==='object'&&!Buffer.isBuffer(value)){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};

async function verify(value){
  if(value.closed)fail('storage_operator_pending_backup_binding_closed');const {trusted,state,store,lease,registry,directory,materials,key}=value;
  const fence=await trusted.observe();if(trusted.scope==='docker-systemd')verifyStorageOperatorSourceFence(state.sourceFence,fence);await assertSqliteSourceLease(lease);const current=await readManifest(join(trusted.configuration.runDirectory,'operator.enc'),key);if(canonical(current)!==canonical(state))fail('storage_operator_pending_backup_state_changed');
  const manifest=await readManifest(join(directory,'target.enc'),key),source=await readManifest(join(directory,'source-manifest.enc'),key),files=await readManifest(join(directory,'files.enc'),key);
  if(hash({cluster:state.cluster,database:manifest.database,user:manifest.user,schema:manifest.schemaChecksums})!==state.targetDigest||hash({configuration:trusted.configurationDigest,target:state.targetDigest,source})!==state.bindingDigest||canonical(await digestFile(join(directory,'files.enc')))!==canonical(source.files?.manifestDigest))fail('storage_operator_pending_backup_binding_changed');
  const identity=await store.get('SELECT current_database() database,(SELECT oid::text FROM pg_database WHERE datname=current_database()) database_oid,(SELECT system_identifier::text FROM pg_control_system()) cluster');
  await verifyStorageOperatorAllocation(registry,state,{manifest,databaseOid:identity.database_oid});const generations=await readStorageOperatorRoleGenerations(store,state);
  if(identity.database!==manifest.database||identity.database_oid!==state.databaseOid||identity.cluster!==state.cluster)fail('storage_operator_pending_backup_target_changed');
  const activation=await store.get('SELECT * FROM harbor_operator.activation WHERE singleton=true');if(!activation||activation.state!=='pending'||activation.run_id!==state.runId||activation.binding!==state.bindingDigest||activation.database_oid!==state.databaseOid||activation.runtime!==state.runtime)fail('storage_operator_pending_backup_activation_changed');
  const names=[manifest.user,state.owner,state.runtime],roles=await store.all('SELECT rolname name,oid::text oid,rolsuper superuser,rolreplication replication,rolcanlogin can_login,rolcreatedb,rolcreaterole,rolinherit,rolbypassrls FROM pg_roles WHERE rolname=ANY($1::text[]) ORDER BY rolname',names);
  if(roles.length!==3||roles.some(role=>Object.entries(role).some(([name,flag])=>!['name','oid'].includes(name)&&flag!==false))||await store.get('SELECT 1 FROM pg_auth_members WHERE roleid=ANY($1::oid[]) OR member=ANY($1::oid[])',roles.map(role=>role.oid)))fail('storage_operator_pending_backup_role_unsafe');
  if(await store.get('SELECT 1 FROM pg_stat_activity WHERE usesysid=ANY($1::oid[]) LIMIT 1',roles.map(role=>role.oid)))fail('storage_operator_pending_backup_live_sessions');
  const observed=await inspectSqliteSourceLease(lease,{directory:trusted.configuration.runDirectory});if(observed.source!==source.sourceDataDir||canonical(observed.content)!==canonical(source.content))fail('storage_operator_source_changed');
  await verifyFileSetSources(files,{exclude:trusted.configuration.fileRoots.filter(root=>resolve(root.source)===source.sourceDataDir).flatMap(root=>['gateway.sqlite','gateway.sqlite-wal','gateway.sqlite-shm'].map(relative=>({root:root.id,relative})))});
  await verifyTransferredSqliteSnapshot({directory,key,store});await verifyStorageOperatorMaterialContents(directory,materials,key);const finalFence=await trusted.observe();if(trusted.scope==='docker-systemd')verifyStorageOperatorSourceFence(state.sourceFence,finalFence);await assertSqliteSourceLease(lease);
  const materialKeys=[];for(const name of ['key-encryption.key','recording-encryption.key']){
    const path=join(materials,'data',name);try{const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size!==32||(info.mode&0o077)!==0)fail('storage_operator_pending_backup_material_key');materialKeys.push({name,...await digestFile(path)});}catch(error){if(error.code!=='ENOENT')throw error;}
  }
  if(value.closed)fail('storage_operator_pending_backup_binding_closed');await assertSqliteSourceLease(lease);if(value.closed)fail('storage_operator_pending_backup_binding_closed');
  return freeze({scope:trusted.scope,runId:state.runId,configurationDigest:state.configurationDigest,bindingDigest:state.bindingDigest,targetDigest:state.targetDigest,cluster:state.cluster,database:{name:manifest.database,oid:state.databaseOid},roles:roles.map(({name,oid,superuser,replication,can_login})=>({name,oid,superuser,replication,can_login})),roleGenerations:{migrationOid:state.allocation.migrationOid,...generations},materialRoots:files.roots.map(root=>({id:root.id,directory:join(materials,root.id)})),materialManifestDigest:source.files.manifestDigest,materialKeys,backupKeyVersion:trusted.configuration.backupKeyVersion,applicationMaterialsVerified:false,missing:['full_candidate_configuration_and_device_material_verification','final_backup_restore_verification','offhost_restore_verification','deployment_activation_consumer'],deploymentAllowed:false});
}

// Only this callback owns the fenced SQLite lease and instance lock. Await the
// real backup/verification operation here; returning a DTO cannot prolong it.
export async function withPendingStorageOperatorBackup({context,backupKey}={},consume){
  if(typeof consume!=='function')fail('storage_operator_pending_backup_consumer_required');artifactKey(backupKey);const trusted=storageOperatorContext(context),config=trusted.configuration;await trusted.observe();await privateDirectory(config.runDirectory);
  const operation=await acquireInstance(config.runDirectory);let registry,lease,store,binding,value;try{
    const state=await readManifest(join(config.runDirectory,'operator.enc'),backupKey);if(state.version!==1||state.phase!=='pending'||state.configurationDigest!==trusted.configurationDigest||!state.allocation||!state.roleGenerations||!state.registrySchemaOid)fail('storage_operator_pending_backup_state_required');
    registry=await openStorageOperatorRegistry(context,{key:backupKey,cluster:state.cluster,expectedSchemaOid:state.registrySchemaOid});const manifest=await readManifest(join(config.runDirectory,'artifacts','target.enc'),backupKey);
    const admin=new PostgresStore({...trusted.adminOptions,poolSize:1});try{const raw=admin.pool.options;store=new PostgresStore({connection:{host:raw.host,port:raw.port,user:raw.user,password:raw.password,database:manifest.database,...(raw.ssl?{ssl:raw.ssl}:{})},poolSize:1,allowPlaintextLocalNetwork:trusted.adminOptions.allowPlaintextLocalNetwork===true});}finally{await admin.close();}
    lease=await acquireSqliteSourceLease(config.sourceDataDir,{sourceOwnerUid:trusted.sourceOwnerUid});value={trusted,state,store,registry,lease,directory:join(config.runDirectory,'artifacts'),materials:join(config.runDirectory,'materials'),key:Buffer.from(backupKey),closed:false};binding=Object.freeze({scope:trusted.scope});bindings.set(binding,value);await verify(value);return await consume(binding);
  }finally{if(value){value.closed=true;value.key.fill(0);}const cleanup=await Promise.allSettled([store?.close(),lease?.close(),registry&&closeStorageOperatorRegistry(registry)]);await operation.release();if(cleanup.some(result=>result.status==='rejected'))fail('storage_operator_pending_backup_cleanup_unknown');}
}
// Private module integration boundary. No caller-supplied connection, JSON
// record or boolean can mint this capability. Each consumption rechecks reality.
export async function inspectPendingStorageOperatorBackup(binding){const value=record(binding),verified=await verify(value);record(binding);return {binding:verified,adminOptions:value.trusted.adminOptions};}
