import {enablePrivateRuntimeLease,revokePrivateRuntimeLease} from './storage-operator-role-lease.mjs';
import {readFile,mkdir,readdir,lstat,rm,chown,open,rename} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash,randomBytes,randomUUID} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {PostgresStore} from './postgres-store.mjs';
import {acquireInstance} from './instance.mjs';
import {createRehearsalTarget,openRehearsalTarget,transferSqlite,captureSqliteSource,acquireSqliteSourceLease,inspectSqliteSourceLease,assertSqliteSourceLease,verifyTransferredSqliteSnapshot} from './sqlite-postgres-transfer.mjs';
import {privateDirectory,artifactKey,canonical,writeManifest,readManifest,digestFile,restoreFileSet,verifyFileSetSources,transferError} from './migration-artifacts.mjs';
import {storageOperatorContext,createDockerStorageOperatorContext,storageOperatorReadiness} from './storage-operator-context.mjs';
import {pendingStorageOperatorReceipt} from './storage-operator-receipt.mjs';
import {revalidateStorageOperatorBaseline,storageOperatorBaselineMode} from './storage-operator-baseline.mjs';
import {verifyStorageOperatorSourceFence} from './storage-operator-source-supervisor.mjs';
import {assertStorageOperatorTargetIdle} from './storage-operator-quiescence.mjs';
import {openStorageOperatorRegistry,closeStorageOperatorRegistry,prepareStorageOperatorTarget,verifyStorageOperatorAllocation,witnessStorageOperatorPromotion,readStorageOperatorRoleGenerations} from './storage-operator-state.mjs';
const fail=code=>{throw transferError(code);},ident=value=>{if(!/^[a-z][a-z0-9_]{0,62}$/.test(value))fail('storage_operator_identifier');return '"'+value+'"';},hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
async function exists(path){try{await lstat(path);return true;}catch(error){if(error.code==='ENOENT')return false;throw error;}}
const targetOptions=(admin,manifest,allowPlaintextLocalNetwork)=>{const raw=admin.pool.options;return {connection:{host:raw.host,port:raw.port,user:raw.user,password:raw.password,database:manifest.database,...(raw.ssl?{ssl:raw.ssl}:{})},poolSize:1,allowPlaintextLocalNetwork};};
async function privateTreeOwner(directory,uid){const info=await lstat(directory);if(info.isSymbolicLink()||!info.isFile()&&!info.isDirectory())fail('storage_operator_material_type');if(info.isDirectory())for(const name of await readdir(directory))await privateTreeOwner(join(directory,name),uid);await chown(directory,uid,uid);}
export async function prepareStorageOperatorMaterialOwners(materials,rootIds,uid=1000){
  await privateDirectory(materials);if(!Number.isInteger(uid)||uid<0||uid>0xfffffffe||uid!==process.getuid()&&process.getuid()!==0||!Array.isArray(rootIds)||!rootIds.length||new Set(rootIds).size!==rootIds.length||rootIds.some(id=>!/^[a-z][a-z0-9-]{0,79}$/.test(id)))fail('storage_operator_material_owner_configuration');
  for(const id of rootIds)await privateTreeOwner(join(materials,id),uid);await privateDirectory(materials);return {operatorDirectoryOwner:process.getuid(),mountedSubtreeOwner:uid};
}
async function cleanupIncompleteCapture(directory){
  for(const name of await readdir(directory)){if(name==='target.enc')continue;if(!/^(source(?:-manifest)?|files|file-[0-9]+|file-catalog-[0-9]+)\.enc$/.test(name)&&!/^\.source-[a-f0-9]+\.sqlite$/.test(name))fail('storage_operator_capture_requires_inspection');const path=join(directory,name),info=await lstat(path);if(!info.isFile()||info.isSymbolicLink())fail('storage_operator_capture_requires_inspection');await rm(path);}
}
export async function verifyStorageOperatorMaterialContents(directory,materials,key){
  const manifest=await readManifest(join(directory,'files.enc'),key);let count=0;for(const root of manifest.roots){if(!/^[a-z][a-z0-9-]{0,79}$/.test(root.id))fail('storage_operator_file_path');const info=await lstat(join(materials,root.id));if(info.isSymbolicLink()||!info.isDirectory())fail('storage_operator_material_changed');}
  for(const part of manifest.catalog){if(!/^file-catalog-[0-9]+\.enc$/.test(part.name)||canonical(await digestFile(join(directory,part.name)))!==canonical(part.digest))fail('storage_operator_file_catalog_changed');const chunk=await readManifest(join(directory,part.name),key);
    for(const entry of chunk.entries){if(!manifest.roots.some(root=>root.id===entry.root)||entry.relative.split('/').some(name=>!name||name==='.'||name==='..')||entry.relative.startsWith('/'))fail('storage_operator_file_path');const path=join(materials,entry.root,...entry.relative.split('/')),info=await lstat(path);if(info.isSymbolicLink()||(entry.kind==='directory'?!info.isDirectory():!info.isFile()))fail('storage_operator_material_changed');if(entry.kind==='file'&&canonical(await digestFile(path))!==canonical(entry.content))fail('storage_operator_material_changed');count++;}
  }
  const walk=async path=>{let total=0;for(const name of await readdir(path)){const info=await lstat(join(path,name));if(info.isSymbolicLink())fail('storage_operator_material_changed');total++;if(info.isDirectory())total+=await walk(join(path,name));}return total;};let actual=0;for(const root of manifest.roots)actual+=await walk(join(materials,root.id));if(actual!==count||count!==manifest.files)fail('storage_operator_material_changed');
}
const verifyMaterialContents=verifyStorageOperatorMaterialContents;
async function verifyCluster(store,expected){const row=await store.get('SELECT system_identifier::text id FROM pg_control_system()');if(expected!==null&&row.id!==expected)fail('storage_operator_cluster_mismatch');return row.id;}

export async function runStorageOperator({context,backupKey,activationKey,baselineVerification,signal,afterBatch,afterPhase}={}){
  const trusted=storageOperatorContext(context),config=trusted.configuration,readiness=storageOperatorReadiness(context);if(!readiness.eligible&&trusted.scope!=='task-owned'&&baselineVerification===undefined)return {state:'blocked',completedPhase:'prerequisites',deploymentAllowed:false,scope:trusted.scope,missing:readiness.missing,sourceTouched:false,targetPrepared:false};
  const verifyBaseline=async()=>baselineVerification===undefined?null:revalidateStorageOperatorBaseline(baselineVerification,{context,signal,requireFormal:trusted.scope!=='task-owned'});let baseline=await verifyBaseline();artifactKey(backupKey);artifactKey(activationKey);if(backupKey.equals(activationKey))fail('storage_operator_independent_key_required');
  await privateDirectory(config.runDirectory);const operation=await acquireInstance(config.runDirectory),directory=join(config.runDirectory,'artifacts'),materials=join(config.runDirectory,'materials'),statePath=join(config.runDirectory,'operator.enc');let lease,admin,targetStore,state,registry;
  const save=async()=>writeManifest(statePath,state,backupKey,{replace:await exists(statePath)});
  const phase=async value=>{state.phase=value;await save();await afterPhase?.(value);};
  const guard=async()=>{signal?.throwIfAborted();const observed=await trusted.observe();if(trusted.scope==='docker-systemd')verifyStorageOperatorSourceFence(state.sourceFence,observed);if(lease)await assertSqliteSourceLease(lease);};
  try{
    // Recheck after acquiring the single operator lock and before the first
    // state/registry write. First-run and resume capabilities cannot interchange.
    const resuming=baselineVerification!==undefined&&storageOperatorBaselineMode(baselineVerification)==='resume';
    if(baselineVerification!==undefined){const saved=await exists(statePath);if(saved&&!resuming)fail('storage_operator_baseline_resume_required');if(!saved&&resuming)fail('storage_operator_baseline_resume_state_required');baseline=await verifyBaseline();}
    admin=new PostgresStore({...trusted.adminOptions,poolSize:1});const cluster=await verifyCluster(admin,null);let configurationMismatch=false,reclamationForeign=false,reclamationPerformed=false;
    if(await exists(statePath)){state=await readManifest(statePath,backupKey);if(state.version!==1||!/^[-a-f0-9]{36}$/.test(state.runId??'')||!/^[A-Za-z0-9_-]{43}$/.test(state.runtimePassword??'')||!['preparing','capturing','importing','verified','promotion_pending','runtime_verified','pending'].includes(state.phase)||state.cluster!==cluster)fail('storage_operator_state_mismatch');configurationMismatch=state.configurationDigest!==trusted.configurationDigest||config.expectedCluster!==null&&config.expectedCluster!==cluster;}
    else{if(config.expectedCluster!==null&&config.expectedCluster!==cluster)fail('storage_operator_cluster_mismatch');if(await exists(directory)&&(await readdir(directory)).length)fail('storage_operator_unowned_artifacts');state={version:1,runId:randomUUID(),configurationDigest:trusted.configurationDigest,cluster,phase:'preparing',runtimePassword:randomBytes(32).toString('base64url'),...(baseline?{baseline:structuredClone(baseline)}:{})};await save();}
    if(resuming&&(!state.baseline||canonical(state.baseline)!==canonical(baseline)))fail('storage_operator_baseline_resume_binding_changed');
    if(configurationMismatch&&!await exists(join(directory,'target.enc')))fail('storage_operator_state_mismatch');
    if(!await exists(directory))await mkdir(directory,{mode:0o700});await privateDirectory(directory);
    if(!await exists(join(directory,'target.enc'))){if(state.phase!=='preparing')fail('storage_operator_target_missing');registry=await openStorageOperatorRegistry(context,{key:backupKey,cluster,expectedSchemaOid:state.registrySchemaOid});await prepareStorageOperatorTarget(context,{registry,state,save,directory,key:backupKey,afterPhase});}
    const manifest=await readManifest(join(directory,'target.enc'),backupKey);if(manifest.version!==1||manifest.rehearsal!==true||!/^harbor_rehearsal_[a-f0-9]{24}$/.test(manifest.database??'')||manifest.user!=='harbor_migration_'+manifest.database.slice('harbor_rehearsal_'.length)||manifest.connection?.database!==manifest.database||manifest.connection?.user!==manifest.user||manifest.systemIdentifier!==cluster)fail('storage_operator_target_cluster');
    const database=await admin.get('SELECT oid::text oid FROM pg_database WHERE datname=$1',manifest.database);if(!database||state.databaseOid&&state.databaseOid!==database.oid)fail('storage_operator_database_identity');
    const targetDigest=hash({cluster,database:manifest.database,user:manifest.user,schema:manifest.schemaChecksums}),owner='harbor_owner_'+manifest.database.slice('harbor_rehearsal_'.length),runtime='harbor_runtime_'+manifest.database.slice('harbor_rehearsal_'.length);if(state.targetDigest&&state.targetDigest!==targetDigest||state.owner&&state.owner!==owner||state.runtime&&state.runtime!==runtime)fail('storage_operator_target_mismatch');
    if(['promotion_pending','runtime_verified','pending'].includes(state.phase)){
      if(!state.databaseOid||!state.bindingDigest||!state.targetDigest||state.runtime!==runtime)fail('storage_operator_state_mismatch');targetStore=new PostgresStore(targetOptions(admin,manifest,trusted.adminOptions.allowPlaintextLocalNetwork===true));
      if(state.allocation){const witnessed=(await targetStore.get("SELECT to_regclass('harbor_operator.role_generations') relation")).relation,rolesExist=await targetStore.get('SELECT 1 FROM pg_roles WHERE rolname=ANY($1::text[]) LIMIT 1',[state.owner,state.runtime]);if(witnessed||state.roleGenerations||state.phase!=='promotion_pending'||rolesExist)await readStorageOperatorRoleGenerations(targetStore,state);}
      if((await targetStore.get('SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1) found',runtime)).found)try{await revokePrivateRuntimeLease(targetStore,state);reclamationPerformed=true;}catch(error){if(error.code!=='storage_operator_foreign_runtime_sessions')throw error;reclamationForeign=true;reclamationPerformed=true;}
    }
    // Old authenticated leases are a cleanup responsibility, independent of
    // whether this new context is permitted to continue the migration.
    if(configurationMismatch){const error=transferError('storage_operator_state_mismatch');error.runtimeLeaseCleanup=reclamationForeign?'foreign_sessions_remain':reclamationPerformed?'own_epoch_reclaimed':'no_prior_private_epoch';throw error;}
    // Legacy pending state is only eligible for cleanup, never upgraded by
    // treating currently same-named roles as the original creation epoch.
    if(!state.allocation)fail('storage_operator_allocation_generation_missing');
    registry??=await openStorageOperatorRegistry(context,{key:backupKey,cluster,expectedSchemaOid:state.registrySchemaOid});await verifyStorageOperatorAllocation(registry,state,{manifest,databaseOid:database.oid});
    if(reclamationForeign)fail('storage_operator_foreign_runtime_sessions');state.databaseOid=database.oid;state.targetDigest=targetDigest;state.owner=owner;state.runtime=runtime;await save();
    signal?.throwIfAborted();if(trusted.scope==='docker-systemd'){if(!state.sourceFence&&state.phase!=='preparing')fail('storage_operator_source_fence_missing');if(state.sourceFence)verifyStorageOperatorSourceFence(state.sourceFence,await trusted.observe());}
    const sourceFence=await trusted.fence();if(trusted.scope==='docker-systemd'){verifyStorageOperatorSourceFence(state.sourceFence??sourceFence,sourceFence);if(!state.sourceFence){state.sourceFence=sourceFence;await save();}}lease=await acquireSqliteSourceLease(config.sourceDataDir,{sourceOwnerUid:trusted.sourceOwnerUid});await guard();
    if(!await exists(join(directory,'source-manifest.enc'))){if(!['preparing','capturing'].includes(state.phase))fail('storage_operator_source_missing');await phase('capturing');await cleanupIncompleteCapture(directory);await captureSqliteSource({sourceDataDir:config.sourceDataDir,directory,key:backupKey,fileRoots:config.fileRoots,sourceLease:lease});}
    const source=await readManifest(join(directory,'source-manifest.enc'),backupKey);if(!source.files)fail('storage_operator_files_required');const binding=hash({configuration:trusted.configurationDigest,target:state.targetDigest,source});if(state.bindingDigest&&state.bindingDigest!==binding)fail('storage_operator_source_mismatch');state.bindingDigest=binding;await save();
    const verifySource=async()=>{await guard();const observed=await inspectSqliteSourceLease(lease,{directory:config.runDirectory});if(observed.source!==source.sourceDataDir||canonical(observed.content)!==canonical(source.content))fail('storage_operator_source_changed');const files=await readManifest(join(directory,'files.enc'),backupKey);if(canonical(await digestFile(join(directory,'files.enc')))!==canonical(source.files.manifestDigest))fail('storage_operator_file_manifest_changed');await verifyFileSetSources(files,{exclude:config.fileRoots.filter(root=>resolve(root.source)===source.sourceDataDir).flatMap(root=>['gateway.sqlite','gateway.sqlite-wal','gateway.sqlite-shm'].map(relative=>({root:root.id,relative})))});};
    await verifySource();
    if(!['promotion_pending','runtime_verified','pending'].includes(state.phase)){
      await phase('importing');await transferSqlite({directory,key:backupKey,signal,beforeBatch:guard,afterBatch:async row=>{await guard();await afterBatch?.(row);}});await verifySource();await phase('verified');
    }
    if(!state.filesRestored){
      if(!await exists(materials)){
        const staging=join(config.runDirectory,'materials-stage-'+state.runId);if(await exists(staging)){await privateDirectory(staging);await rm(staging,{recursive:true});}await mkdir(staging,{mode:0o700});
        await restoreFileSet({directory,destination:staging,key:backupKey,expectedManifestDigest:source.files.manifestDigest});await verifyMaterialContents(directory,staging,backupKey);await rename(staging,materials);await afterPhase?.('materials_renamed');
      }
      await privateDirectory(materials);await verifyMaterialContents(directory,materials,backupKey);state.filesRestored=true;await save();
    }await privateDirectory(materials);
    await verifyMaterialContents(directory,materials,backupKey);await verifySource();
    if(trusted.scope==='docker-systemd')await prepareStorageOperatorMaterialOwners(materials,(await readManifest(join(directory,'files.enc'),backupKey)).roots.map(root=>root.id),1000);
    targetStore??=new PostgresStore(targetOptions(admin,manifest,trusted.adminOptions.allowPlaintextLocalNetwork===true));await verifyCluster(targetStore,cluster);
    await verifyTransferredSqliteSnapshot({directory,key:backupKey,store:targetStore});
    if(!['promotion_pending','runtime_verified','pending'].includes(state.phase)){await phase('promotion_pending');}
    await guard();
    const preflightDeadline=config.infrastructurePublisher?performance.now()+trusted.preflightTimeoutMs:undefined;
    await targetStore.transaction(async()=>{
      const lock=createHash('sha256').update('harbor-storage-operator:'+manifest.database).digest().readInt32BE();if(!(await targetStore.get('SELECT pg_try_advisory_xact_lock($1) held',lock)).held)fail('storage_operator_target_busy');
      const row=await targetStore.get('SELECT oid::text oid,pg_get_userbyid(datdba) owner FROM pg_database WHERE datname=current_database()');if(row.oid!==state.databaseOid)fail('storage_operator_database_identity');if(![manifest.user,state.owner].includes(row.owner))fail('storage_operator_owner_changed');
      await assertStorageOperatorTargetIdle(targetStore,{allowWait:!!config.infrastructurePublisher,deadline:preflightDeadline,signal});
      await targetStore.query('CREATE SCHEMA IF NOT EXISTS harbor_operator');await targetStore.query('REVOKE ALL ON SCHEMA harbor_operator FROM PUBLIC');
      await witnessStorageOperatorPromotion(targetStore,state);
      for(const role of [state.owner,state.runtime]){
        const actual=await targetStore.get('SELECT rolsuper,rolcreatedb,rolcreaterole,rolinherit,rolreplication,rolbypassrls FROM pg_roles WHERE rolname=$1',role);if(!actual)await targetStore.query('CREATE ROLE '+ident(role)+' NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS');else if(Object.values(actual).some(Boolean))fail('storage_operator_role_unsafe');
        if(await targetStore.get('SELECT 1 FROM pg_auth_members WHERE roleid=(SELECT oid FROM pg_roles WHERE rolname=$1) OR member=(SELECT oid FROM pg_roles WHERE rolname=$1)',role))fail('storage_operator_role_membership');
      }
      if(await targetStore.get("SELECT 1 FROM pg_shdepend WHERE refclassid='pg_authid'::regclass AND refobjid=(SELECT oid FROM pg_roles WHERE rolname=$1) AND deptype='o' LIMIT 1",state.runtime))fail('storage_operator_runtime_owns_objects');
      await targetStore.query('ALTER ROLE '+ident(state.owner)+' NOLOGIN');await targetStore.query('ALTER ROLE '+ident(state.runtime)+' NOLOGIN');
      await targetStore.query('REASSIGN OWNED BY '+ident(manifest.user)+' TO '+ident(state.owner));
      await targetStore.query('ALTER DATABASE '+ident(manifest.database)+' OWNER TO '+ident(state.owner));await targetStore.query('REVOKE ALL ON DATABASE '+ident(manifest.database)+' FROM PUBLIC');await targetStore.query('REVOKE CREATE ON SCHEMA public FROM PUBLIC');await targetStore.query('REVOKE ALL ON SCHEMA harbor FROM PUBLIC');
      await targetStore.query('REVOKE ALL ON DATABASE '+ident(manifest.database)+' FROM '+ident(state.runtime));await targetStore.query('REVOKE ALL ON SCHEMA harbor,public FROM '+ident(state.runtime));await targetStore.query('REVOKE ALL ON ALL TABLES IN SCHEMA harbor FROM PUBLIC,'+ident(state.runtime));await targetStore.query('REVOKE ALL ON ALL SEQUENCES IN SCHEMA harbor FROM PUBLIC,'+ident(state.runtime));
      await targetStore.query('GRANT CONNECT ON DATABASE '+ident(manifest.database)+' TO '+ident(state.runtime));await targetStore.query('GRANT USAGE ON SCHEMA harbor TO '+ident(state.runtime));
      for(const row of await targetStore.all("SELECT table_name FROM information_schema.tables WHERE table_schema='harbor' AND table_type='BASE TABLE' ORDER BY table_name"))await targetStore.query('GRANT '+(['schema_migrations','data_contract','recording_storage_reports'].includes(row.table_name)?'SELECT':'SELECT,INSERT,UPDATE,DELETE')+' ON TABLE harbor.'+ident(row.table_name)+' TO '+ident(state.runtime));
      await targetStore.query('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA harbor TO '+ident(state.runtime));await targetStore.query('ALTER ROLE '+ident(manifest.user)+' NOLOGIN');await targetStore.query('REVOKE ALL ON DATABASE '+ident(manifest.database)+' FROM '+ident(manifest.user));
      await targetStore.query('CREATE SCHEMA IF NOT EXISTS harbor_operator');await targetStore.query('REVOKE ALL ON SCHEMA harbor_operator FROM PUBLIC,'+ident(state.runtime));await targetStore.query('CREATE TABLE IF NOT EXISTS harbor_operator.activation(singleton boolean PRIMARY KEY CHECK(singleton),run_id text NOT NULL,binding text NOT NULL,database_oid text NOT NULL,runtime text NOT NULL,state text NOT NULL)');
      const saved=await targetStore.get('SELECT * FROM harbor_operator.activation WHERE singleton=true FOR UPDATE');if(saved&&(saved.run_id!==state.runId||saved.binding!==state.bindingDigest||saved.database_oid!==state.databaseOid||saved.runtime!==state.runtime))fail('storage_operator_database_state_conflict');
      if(!saved)await targetStore.run("INSERT INTO harbor_operator.activation VALUES(true,$1,$2,$3,$4,'pending')",state.runId,state.bindingDigest,state.databaseOid,state.runtime);
      await targetStore.query('SET LOCAL ROLE '+ident(state.runtime));await targetStore.verifySchema();await targetStore.query('RESET ROLE');await afterPhase?.('promotion_before_commit');await guard();
    });
    await afterPhase?.('promotion_committed_before_state');state.roleGenerations=await readStorageOperatorRoleGenerations(targetStore,state);await save();await afterPhase?.('roles_promoted');await guard();await verifyMaterialContents(directory,materials,backupKey);
    let runtimeStore,preflight,runtimeLease;
    try{
      // This credential is exposed only to the private preflight scope, never to
      // the service/deployment config. It is NOLOGIN again before returning.
      const leaseTimeoutMs=preflightDeadline===undefined?trusted.preflightTimeoutMs:Math.floor(preflightDeadline-performance.now());if(leaseTimeoutMs<1000)fail('storage_operator_preflight_deadline');
      state.runtimePassword=randomBytes(32).toString('base64url');await save();runtimeLease=await enablePrivateRuntimeLease(targetStore,state,{password:state.runtimePassword,timeoutMs:leaseTimeoutMs,deadline:preflightDeadline,signal,beforeCommit:guard});await afterPhase?.('runtime_login_enabled');
      const connection={...targetOptions(admin,manifest).connection,user:state.runtime,password:state.runtimePassword,application_name:runtimeLease.application};runtimeStore=new PostgresStore({connection,poolSize:1,statementTimeoutMs:Math.min(10000,trusted.preflightTimeoutMs),allowPlaintextLocalNetwork:trusted.adminOptions.allowPlaintextLocalNetwork===true});
      const authenticated=await runtimeStore.get('SELECT system_user authentication');if(!authenticated.authentication?.startsWith('scram-sha-256:'))fail('storage_operator_password_expiry_auth_unverified');
      preflight=await trusted.runtimePreflight({store:runtimeStore,connection,materials,runDirectory:config.runDirectory,signal,deadline:preflightDeadline,target:{cluster,database:manifest.database,databaseOid:state.databaseOid,schemaDigest:createHash('sha256').update(JSON.stringify(manifest.schemaChecksums)).digest('hex')}});await guard();
    }finally{try{await runtimeStore?.close();}finally{await revokePrivateRuntimeLease(targetStore,state);}}
    state.preflight=preflight;await phase('runtime_verified');await verifySource();
    const crossDatabasePublic=await admin.all("SELECT datname FROM pg_database WHERE datallowconn AND datname<>$1 AND has_database_privilege($2,oid,'CONNECT')",manifest.database,state.runtime);
    state.missing=[...(preflight.missing??[]),...(trusted.scope!=='docker-systemd'?['actual_docker_source_fence']:[]),'existing_cluster_backup_verification','offhost_restore_verification','deployment_activation_consumer'];state.crossDatabaseDenyGuaranteed=crossDatabasePublic.length===0;await phase('pending');
    const receipt=pendingStorageOperatorReceipt({runId:state.runId,bindingDigest:state.bindingDigest,targetDigest:state.targetDigest,completedPhase:'runtime_verified',scope:trusted.scope,created:Date.now(),missing:state.missing},activationKey);await writeManifest(join(config.runDirectory,'activation-status.enc'),receipt,backupKey,{replace:await exists(join(config.runDirectory,'activation-status.enc'))});
    return {state:'pending',completedPhase:'runtime_verified',deploymentAllowed:false,scope:trusted.scope,missing:receipt.payload.missing,crossDatabaseDenyGuaranteed:state.crossDatabaseDenyGuaranteed,receipt};
  }finally{const results=await Promise.allSettled([targetStore?.close(),admin?.close(),lease?.close(),registry&&closeStorageOperatorRegistry(registry)]);try{await operation.release();}finally{if(results.some(row=>row.status==='rejected'))fail('storage_operator_cleanup_unknown');}}
}

async function main(){
  const args=process.argv.slice(2);if(args.length!==2||!['run','prepare','resume','backup-final','activate','check-baseline','cleanup'].includes(args[0])||args[1]!=='/etc/harbor-storage-operator.json')fail('storage_operator_arguments');const context=await createDockerStorageOperatorContext(args[1]);if(args[0]==='check-baseline'){const controller=new AbortController();for(const name of ['SIGTERM','SIGINT'])process.once(name,()=>controller.abort());const {checkInstalledStorageOperatorBaseline}=await import('./storage-operator-baseline.mjs');process.stdout.write(JSON.stringify(await checkInstalledStorageOperatorBaseline(context,{signal:controller.signal}))+'\n');return;}
  const load=async path=>{const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);try{const info=await file.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o077)!==0||info.size!==32)fail('storage_operator_key_permissions');return await file.readFile();}finally{await file.close();}};
  if(args[0]==='cleanup'){const controller=new AbortController();for(const name of ['SIGTERM','SIGINT'])process.once(name,()=>controller.abort());const {cleanupStorageOperator}=await import('./storage-operator-cleanup.mjs');process.stdout.write(JSON.stringify(await cleanupStorageOperator({context,backupKey:await load('/etc/harbor-storage-backup.key'),signal:controller.signal}))+'\n');return;}
  if(args[0]==='prepare'){const controller=new AbortController();for(const name of ['SIGTERM','SIGINT'])process.once(name,()=>controller.abort());const {withInstalledStorageOperatorBaseline}=await import('./storage-operator-baseline.mjs');const result=await withInstalledStorageOperatorBaseline(context,async({verification})=>runStorageOperator({context,baselineVerification:verification,backupKey:await load('/etc/harbor-storage-backup.key'),activationKey:await load('/etc/harbor-storage-activation.key'),signal:controller.signal}),{signal:controller.signal});process.stdout.write(JSON.stringify(result)+'\n');return;}
  if(['resume','backup-final','activate'].includes(args[0])){const controller=new AbortController();for(const name of ['SIGTERM','SIGINT'])process.once(name,()=>controller.abort());const {createStorageOperatorResumeBinding,closeStorageOperatorResumeBinding}=await import('./storage-operator-resume-binding.mjs'),{withInstalledStorageOperatorBaseline}=await import('./storage-operator-baseline.mjs');let backupKey,activationKey,binding;
   try{backupKey=await load('/etc/harbor-storage-backup.key');if(['resume','activate'].includes(args[0]))activationKey=await load('/etc/harbor-storage-activation.key');binding=await createStorageOperatorResumeBinding({context,backupKey,signal:controller.signal});const result=await withInstalledStorageOperatorBaseline(context,async({verification})=>{
    if(args[0]==='resume')return runStorageOperator({context,baselineVerification:verification,backupKey,activationKey,signal:controller.signal});
    if(args[0]==='activate'){
     const {withVerifiedStorageOperatorActivationProof}=await import('./storage-operator-activation-proof.mjs'),{issueStorageActivation}=await import('./storage-activation-issuer.mjs'),directory=storageOperatorContext(context).configuration.runDirectory;
     return withVerifiedStorageOperatorActivationProof({context,baselineVerification:verification,backupKey,signal:controller.signal},async qualification=>{
      for(const name of ['deployment-secret.enc','deployment-preflight-secret.enc'])if(await exists(join(directory,name)))fail('storage_operator_activation_deployment_in_progress');
      const receipt=await issueStorageActivation({context,qualification,backupKey,activationKey,signal:controller.signal});controller.signal.throwIfAborted();await writeManifest(join(directory,'activation-approved.enc'),receipt,backupKey,{replace:await exists(join(directory,'activation-approved.enc'))});controller.signal.throwIfAborted();return {state:'activation-approved',deploymentAllowed:false,missing:['gitlab_deployment_consumption']};
     });
    }
    const {createVerifiedStorageOperatorFinalBackup,inspectStorageOperatorBackup}=await import('./storage-operator-backup.mjs');return inspectStorageOperatorBackup(await createVerifiedStorageOperatorFinalBackup({context,baselineVerification:verification,backupKey,signal:controller.signal}));
   },{signal:controller.signal,resumeBinding:binding});process.stdout.write(JSON.stringify(result)+'\n');}finally{if(binding)closeStorageOperatorResumeBinding(binding);backupKey?.fill(0);activationKey?.fill(0);}return;}
  if(!storageOperatorReadiness(context).eligible){process.stdout.write(JSON.stringify(await runStorageOperator({context}))+'\n');return;}
  const controller=new AbortController();for(const signal of ['SIGTERM','SIGINT'])process.once(signal,()=>controller.abort());const result=await runStorageOperator({context,backupKey:await load('/etc/harbor-storage-backup.key'),activationKey:await load('/etc/harbor-storage-activation.key'),signal:controller.signal});process.stdout.write(JSON.stringify(result)+'\n');
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))main().catch(error=>{process.stderr.write((/^storage_operator_[a-z_]+$/.test(error.code??'')?error.code:'storage_operator_incomplete')+'\n');process.exitCode=1;});
