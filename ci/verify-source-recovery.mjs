// Fixed host endpoint. The controller owns deployment/controller locks and
// prevents concurrent consume; no caller-supplied flag authorizes recovery.
import {open,lstat,mkdir} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createHash,randomUUID,timingSafeEqual} from 'node:crypto';
import {join,dirname,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {createDockerStorageOperatorContext,storageOperatorContext} from '/opt/harbor-storage-tools/app/storage-operator-context.mjs';
import {createStorageOperatorResumeBinding,inspectStorageOperatorResumeBinding,closeStorageOperatorResumeBinding} from '/opt/harbor-storage-tools/app/storage-operator-resume-binding.mjs';
import {createResumingPostgresBackupContext,postgresBackupContext,verifyBackupBaselineCluster,closePostgresBackupContext} from '/opt/harbor-storage-tools/app/postgres-backup-context.mjs';
import {openStorageOperatorRegistry,closeStorageOperatorRegistry,readStorageOperatorAllocation,verifyStorageOperatorAllocation,readStorageOperatorRoleGenerations} from '/opt/harbor-storage-tools/app/storage-operator-state.mjs';
import {verifyStorageOperatorSourceFence} from '/opt/harbor-storage-tools/app/storage-operator-source-supervisor.mjs';
import {revokePrivateRuntimeLease} from '/opt/harbor-storage-tools/app/storage-operator-role-lease.mjs';
import {acquireSqliteSourceLease,inspectSqliteSourceLease,assertSqliteSourceLease,captureSqliteSource} from '/opt/harbor-storage-tools/app/sqlite-postgres-transfer.mjs';
import {acquireInstance} from '/opt/harbor-storage-tools/app/instance.mjs';
import {PostgresStore} from '/opt/harbor-storage-tools/app/postgres-store.mjs';
import {backupInventory} from '/opt/harbor-storage-tools/app/postgres-backup-inventory.mjs';
import {readManifest,digestFile,verifyFileSetSources,privateDirectory,artifactKey,canonical} from '/opt/harbor-storage-tools/app/migration-artifacts.mjs';
const fail=code=>{throw Object.assign(Error(code),{code:'source_recovery_'+code});},need=(ok,code)=>{if(!ok)fail(code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const phases=new Set(['preparing','capturing','importing','verified','promotion_pending','runtime_verified','pending']);
const roleIdentifier=name=>{need(/^harbor_(?:migration|owner|runtime)_[a-f0-9]{24}$/.test(name),'role_identifier');return '"'+name+'"';};
async function exists(path){try{await lstat(path);return true;}catch(error){if(error.code==='ENOENT')return false;throw error;}}
async function stateOf(trusted,key){
 const state=await readManifest(join(trusted.configuration.runDirectory,'operator.enc'),key);
 need(state.version===1&&phases.has(state.phase)&&state.configurationDigest===trusted.configurationDigest&&(trusted.configuration.expectedCluster===null?trusted.scope==='task-owned':state.cluster===trusted.configuration.expectedCluster),'state_binding');
 need(/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(state.runId??''),'run_id');return state;
}
async function actualInventory(admin){return {...await admin.get("SELECT system_identifier::text system_id,current_setting('data_directory') data_directory FROM pg_control_system()"),...await backupInventory(admin)};}
function targetStore(admin,database,trusted){
 const raw=admin.pool.options;return new PostgresStore({connection:{host:raw.host,port:raw.port,user:raw.user,password:raw.password,database,...(raw.ssl?{ssl:raw.ssl}:{})},poolSize:1,allowPlaintextLocalNetwork:trusted.adminOptions.allowPlaintextLocalNetwork===true});
}
async function sourceProof({trusted,state,key,lease,signal}){
 const config=trusted.configuration,directory=join(config.runDirectory,'artifacts');let capture=directory,mode='authenticated-captured-source-match';
 if(!await exists(join(capture,'source-manifest.enc'))){
  signal?.throwIfAborted();capture=join(config.runDirectory,'source-recovery-'+randomUUID());await mkdir(capture,{mode:0o700});await privateDirectory(capture);
  // Existing capture validates SQLite integrity, original encrypted credentials,
  // settings and all configured account home/material roots. Old partial files
  // stay untouched; this is current-source verification, not a history claim.
  await captureSqliteSource({sourceDataDir:config.sourceDataDir,directory:capture,key,fileRoots:config.fileRoots,sourceLease:lease});mode='fenced-current-capture';
 }
 const source=await readManifest(join(capture,'source-manifest.enc'),key),observed=await inspectSqliteSourceLease(lease,{directory:config.runDirectory});
 need(source.sourceDataDir===config.sourceDataDir&&source.sourceContract==='sqlite-metadata'&&source.files&&observed.source===source.sourceDataDir&&canonical(observed.content)===canonical(source.content),'source_changed');
 if(mode==='authenticated-captured-source-match'&&state.bindingDigest)need(hash({configuration:trusted.configurationDigest,target:state.targetDigest,source})===state.bindingDigest,'source_binding_changed');
 const files=await readManifest(join(capture,'files.enc'),key);need(canonical(await digestFile(join(capture,'files.enc')))===canonical(source.files.manifestDigest),'source_catalog_changed');
 await verifyFileSetSources(files,{exclude:config.fileRoots.filter(root=>resolve(root.source)===source.sourceDataDir).flatMap(root=>['gateway.sqlite','gateway.sqlite-wal','gateway.sqlite-shm'].map(relative=>({root:root.id,relative})))});
 await assertSqliteSourceLease(lease);signal?.throwIfAborted();return {mode,capture};
}
async function recheckSource({trusted,key,lease,proof,signal}){
 const source=await readManifest(join(proof.capture,'source-manifest.enc'),key),observed=await inspectSqliteSourceLease(lease,{directory:trusted.configuration.runDirectory});
 need(observed.source===source.sourceDataDir&&canonical(observed.content)===canonical(source.content),'source_changed');
 const files=await readManifest(join(proof.capture,'files.enc'),key);need(canonical(await digestFile(join(proof.capture,'files.enc')))===canonical(source.files.manifestDigest),'source_catalog_changed');
 await verifyFileSetSources(files,{exclude:trusted.configuration.fileRoots.filter(root=>resolve(root.source)===source.sourceDataDir).flatMap(root=>['gateway.sqlite','gateway.sqlite-wal','gateway.sqlite-shm'].map(relative=>({root:root.id,relative})))});
 await assertSqliteSourceLease(lease);signal?.throwIfAborted();
}
async function authorization(target,state,saved,roles){
 if(!target){need(!roles.length||roles.length===1&&!roles[0].name.startsWith('harbor_runtime_'),'allocation_without_database');return {activationState:'not-created',generations:null};}
 const identity=await target.get('SELECT current_database() name,(SELECT oid::text FROM pg_database WHERE datname=current_database()) oid');need(identity.name===saved.intent.database&&identity.oid===saved.databaseOid,'database_generation');
 const relation=await target.get("SELECT to_regclass('harbor_operator.deployment_activation') relation");need(!relation.relation||!await target.get('SELECT 1 FROM harbor_operator.deployment_activation LIMIT 1'),'deployment_activation_present');
 const activationRelation=await target.get("SELECT to_regclass('harbor_operator.activation') relation"),generationRelation=await target.get("SELECT to_regclass('harbor_operator.role_generations') relation");
 if(roles.length<=1){
  need(!generationRelation.relation&&!activationRelation.relation,'unwitnessed_authorization');
  need(['preparing','capturing','importing','verified','promotion_pending'].includes(state.phase),'missing_promoted_roles');return {activationState:'not-created',generations:null};
 }
 need(roles.length===3&&activationRelation.relation&&generationRelation.relation,'role_generation_missing');
 const generations=await readStorageOperatorRoleGenerations(target,state),activation=await target.get('SELECT * FROM harbor_operator.activation WHERE singleton=true');
 need(activation?.state==='pending'&&activation.run_id===state.runId&&activation.binding===state.bindingDigest&&activation.database_oid===saved.databaseOid&&activation.runtime===state.runtime,'activation_not_pending');return {activationState:'pending',generations};
}
// Task contexts are real isolated PostgreSQL/SQLite fixtures and can only return
// a diagnostic contract. The fixed root CLI constructs its own formal context.
export async function verifySourceRecovery({context,backupKey,taskBaseline,signal}={}){
 const trusted=storageOperatorContext(context),formal=trusted.scope==='docker-systemd';need(formal?process.getuid()===0&&taskBaseline===undefined:trusted.scope==='task-owned'&&taskBaseline,'context_scope');if(formal)need(trusted.configuration.sourceController?.supervisorKind==='docker-restart','source_supervisor');artifactKey(backupKey);signal?.throwIfAborted();
 await privateDirectory(trusted.configuration.runDirectory);const operation=await acquireInstance(trusted.configuration.runDirectory);let binding,backupContext,registry,admin,target,lease,observation;
 try{
  const state=await stateOf(trusted,backupKey),original=canonical(state);binding=await createStorageOperatorResumeBinding({context,backupKey,signal});
  admin=new PostgresStore({...trusted.adminOptions,poolSize:1});
  const verifyInventory=async()=>{
   signal?.throwIfAborted();let actual,expected,digest;
   if(formal){
    backupContext??=await createResumingPostgresBackupContext({context,resumeBinding:binding,signal});const record=postgresBackupContext(backupContext);need(timingSafeEqual(record.key,backupKey),'backup_key_binding');actual=await verifyBackupBaselineCluster(backupContext,{signal});expected=record.expected;digest=record.configurationDigest;
   }else{actual=await actualInventory(admin);expected=taskBaseline.expected;digest=taskBaseline.configurationDigest;}
   return inspectStorageOperatorResumeBinding(binding,{context,actual,baselineExpected:expected,baselineConfigurationDigest:digest,signal});
  };
  const inventory=await verifyInventory();need(inventory.runId===state.runId,'inventory_run');
  let fence=await trusted.observe();if(formal&&state.sourceFence)verifyStorageOperatorSourceFence(state.sourceFence,fence);else need(fence.sourceStopped===true&&fence.supervisorFenced===true,'source_not_fenced');
  const namespace=await admin.get("SELECT oid::text oid FROM pg_namespace WHERE nspname='harbor_operator_registry'");let saved;
  if(namespace){registry=await openStorageOperatorRegistry(context,{key:backupKey,cluster:state.cluster,expectedSchemaOid:state.registrySchemaOid,readOnly:true});saved=await readStorageOperatorAllocation(registry,state);}
  if(state.allocation?.migrationOid||state.allocation?.databaseOid)need(saved,'allocation_not_witnessed');
  const roles=inventory.addedRoles;need([0,1,3].includes(roles.length),'role_scope');
  if(saved){need(roles.some(role=>role.name===saved.intent.migration&&role.oid===saved.migrationOid),'migration_generation');if(saved.databaseOid)target=targetStore(admin,saved.intent.database,trusted);}
  else need(roles.length===0&&inventory.addedDatabases.length===0,'unwitnessed_addition');
  if(await exists(join(trusted.configuration.runDirectory,'artifacts','target.enc'))){
   need(saved?.databaseOid&&registry,'unwitnessed_target_manifest');const manifest=await readManifest(join(trusted.configuration.runDirectory,'artifacts','target.enc'),backupKey);
   need(manifest.systemIdentifier===state.cluster,'target_cluster');await verifyStorageOperatorAllocation(registry,state,{manifest,databaseOid:saved.databaseOid});
   if(state.targetDigest)need(state.targetDigest===hash({cluster:state.cluster,database:manifest.database,user:manifest.user,schema:manifest.schemaChecksums}),'target_digest_changed');
  }
  let observed=await authorization(target,state,saved,roles);
  lease=await acquireSqliteSourceLease(trusted.configuration.sourceDataDir,{sourceOwnerUid:trusted.sourceOwnerUid});
  const source=await sourceProof({trusted,state,key:backupKey,lease,signal});signal?.throwIfAborted();
  // Never revoke an arbitrary caller-selected identity. The signed registry,
  // exact role OIDs and current pending lease have already been revalidated.
  if(observed.generations)await revokePrivateRuntimeLease(target,{...state,roleGenerations:observed.generations});
  await admin.transaction(async()=>{
   for(const role of roles){
    const row=await admin.get('SELECT oid::text oid FROM pg_catalog.pg_authid WHERE rolname=$1 FOR UPDATE',role.name);need(row?.oid===role.oid,'role_replaced');
    if(role.name===saved.intent.migration)await admin.query('ALTER ROLE '+roleIdentifier(role.name)+' NOLOGIN');
   }
  });
  const actualRoles=roles.length?await admin.all('SELECT rolname name,oid::text oid,rolcanlogin can_login FROM pg_roles WHERE oid=ANY($1::oid[]) ORDER BY oid',roles.map(role=>role.oid)):[];
  need(actualRoles.length===roles.length&&actualRoles.every(role=>!role.can_login&&roles.some(expected=>expected.name===role.name&&expected.oid===role.oid)),'role_login_or_generation');
  await admin.get('SELECT pg_stat_clear_snapshot()');const activity=await admin.get('SELECT COUNT(*)::text n FROM pg_stat_activity WHERE usesysid=ANY($1::oid[])',roles.map(role=>role.oid));need(activity.n==='0','target_sessions_present');
  observed=await authorization(target,state,saved,roles);await verifyInventory();await recheckSource({trusted,key:backupKey,lease,proof:source,signal});need(canonical(await stateOf(trusted,backupKey))===original,'operator_state_changed');
  fence=await trusted.observe();if(formal&&state.sourceFence)verifyStorageOperatorSourceFence(state.sourceFence,fence);else need(fence.sourceStopped===true&&fence.supervisorFenced===true,'source_not_fenced');
  let storageMarker=null;
  if(target&&(await target.get("SELECT to_regclass('harbor.data_contract') relation")).relation){
   const contract=await target.get('SELECT name FROM harbor.data_contract WHERE singleton=1'),identity=await target.get('SELECT current_database() database,(SELECT system_identifier::text FROM pg_control_system()) cluster');
   need(contract?.name==='postgres-conversation-v2'&&identity.cluster===state.cluster&&identity.database===saved.intent.database,'storage_contract');storageMarker={contract:contract.name,database_digest:createHash('sha256').update(identity.cluster+'\0'+identity.database).digest('hex')};
  }
  const kinds={migration:'not-created',owner:'not-created',runtime:'not-created'};for(const role of actualRoles){const kind=/^harbor_(migration|owner|runtime)_/.exec(role.name)?.[1];need(kind,'role_scope');kinds[kind]='NOLOGIN';}
  observation={contract:formal?'harbor-source-recovery-v1':'harbor-source-recovery-diagnostic',runId:state.runId,configurationDigest:trusted.configurationDigest,sourceFence:fence,sourceDataDir:trusted.configuration.sourceDataDir,supervisorKind:formal?trusted.configuration.sourceController.supervisorKind:'task-owned',storageMarker,activationState:observed.activationState,deploymentActivationAbsent:true,roleStates:kinds,rolesNoLogin:true,targetSessionCount:Number(activity.n),sourceVerified:true,sourceVerificationMode:source.mode,registryVerified:true,inventoryVerified:true};
 }finally{
  if(binding)closeStorageOperatorResumeBinding(binding);
  const result=await Promise.allSettled([target?.close(),admin?.close(),lease?.close(),registry&&closeStorageOperatorRegistry(registry),backupContext&&closePostgresBackupContext(backupContext)]);
  try{await operation.release();}finally{need(result.every(row=>row.status==='fulfilled'),'cleanup_unknown');}
 }
 // All verifier-owned DB/source leases have ended before the controller starts
 // its pinned old source. Its deployment mutex must remain held throughout.
 return observation;
}
async function rootKey(){
 const path='/etc/harbor-storage-backup.key';for(let parent=dirname(path);;parent=dirname(parent)){const info=await lstat(parent);need(info.isDirectory()&&!info.isSymbolicLink()&&info.uid===0&&!(info.mode&0o022),'key_parent');if(parent==='/')break;}
 const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);
 try{const info=await file.stat();need(info.isFile()&&info.uid===0&&(info.mode&0o777)===0o600&&info.nlink===1&&info.size===32,'key_private');return await file.readFile();}finally{await file.close();}
}
async function main(){
 need(process.platform==='linux'&&process.getuid()===0&&process.argv.length===2,'fixed_root_required');const controller=new AbortController();for(const name of ['SIGINT','SIGTERM'])process.once(name,()=>controller.abort());
 const context=await createDockerStorageOperatorContext('/etc/harbor-storage-operator.json'),key=await rootKey();
 try{process.stdout.write(JSON.stringify(await verifySourceRecovery({context,backupKey:key,signal:controller.signal}))+'\n');}finally{key.fill(0);}
}
if(import.meta.url.startsWith('file:')&&process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url)main().catch(error=>{process.stderr.write(JSON.stringify({error:/^(?:source_recovery|storage_operator|transfer|backup)_[a-z_]+$/.test(error?.code??'')?error.code:'source_recovery_denied',sourceRecoveryAuthorized:false})+'\n');process.exitCode=1;});
