import {open,lstat,realpath,readFile,mkdir} from 'node:fs/promises';
import {constants} from 'node:fs';
import {resolve,join} from 'node:path';
import {createHash} from 'node:crypto';
import {endianness} from 'node:os';
import {PostgresStore} from './postgres-store.mjs';
import {backupInventory} from './postgres-backup-inventory.mjs';
import {canonical,artifactKey,readManifest,transferError} from './migration-artifacts.mjs';
const contexts=new WeakMap(),digest=value=>createHash('sha256').update(canonical(value)).digest('hex'),fail=code=>{throw transferError(code);};
export function postgresBackupContext(value){const record=contexts.get(value);if(!record)fail('backup_context_required');return record;}
function freeze(value){if(value&&typeof value==='object'&&!Buffer.isBuffer(value)){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;}
function publish(record){for(const name of ['scope','profile','configurationDigest','adminOptions','expected','artifactDirectory','archiveDirectory','key','keyVersion','archiveProfileDigest','restoreDirectory','restoreUid','restoreGid','archiveKeyFile','pendingBinding','pendingBindingDigest','receiverBinding','resumeBinding','resumeOperatorContext'])if(Object.hasOwn(record,name))Object.defineProperty(record,name,{value:freeze(record[name]),enumerable:true,writable:false,configurable:false});record.busy=false;record.pending=null;record.cancellation=null;record.applicationPlan=null;Object.seal(record);const value=Object.freeze({scope:record.scope});contexts.set(value,record);return value;}
async function observeBackupCluster(options){
 const store=new PostgresStore({...options,poolSize:1});try{
  if((await store.all("SELECT spcname FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')")).length)fail('backup_external_tablespaces_unsupported');
  const control=await store.get("SELECT system_identifier::text system_id,extract(epoch from pg_postmaster_start_time())::text postmaster_start,current_setting('server_version_num')::integer version,current_setting('data_directory') data_directory,pg_size_bytes(current_setting('wal_segment_size'))::text segment_bytes FROM pg_control_system()"),checkpoint=await store.get('SELECT timeline_id::integer timeline FROM pg_control_checkpoint()');
  if(control.version<180000||control.version>=190000)fail('backup_postgres_version');
  return {...control,...checkpoint,...await backupInventory(store)};
 }finally{await store.close();}
}
export async function verifyBackupCluster(context){
 const record=postgresBackupContext(context);if(record.receiverBinding)fail('backup_receiver_readonly');if(record.resumeBinding)fail('backup_resume_readonly');await verifyPostgresBackupPendingBinding(context);const actual=await observeBackupCluster(record.adminOptions);
 if(actual.system_id!==record.expected.system_id||actual.data_directory!==record.expected.data_directory||canonical(actual.databases)!==canonical(record.expected.databases)||canonical(actual.roles)!==canonical(record.expected.roles)||canonical(actual.memberships)!==canonical(record.expected.memberships))fail('backup_cluster_scope_changed');
 return actual;
}
// A resumed baseline remains the original backup inventory. Its live source
// may additionally contain only the authenticated operator's witnessed epochs.
export async function verifyBackupBaselineCluster(context,{signal}={}){
 const record=postgresBackupContext(context);if(!record.resumeBinding)return verifyBackupCluster(context);if(record.closed)fail('backup_context_closed');signal?.throwIfAborted();const actual=await observeBackupCluster(record.adminOptions);
 if(actual.system_id!==record.expected.system_id||actual.data_directory!==record.expected.data_directory)fail('backup_cluster_scope_changed');
 const {inspectStorageOperatorResumeBinding}=await import('./storage-operator-resume-binding.mjs');await inspectStorageOperatorResumeBinding(record.resumeBinding,{context:record.resumeOperatorContext,actual,baselineExpected:record.expected,baselineConfigurationDigest:record.configurationDigest,signal});signal?.throwIfAborted();return actual;
}
async function protectedJson(path,{root=true,mode=0o600}={}){
 const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);try{const info=await file.stat();if(!info.isFile()||(root&&info.uid!==0)||(info.mode&0o777)!==mode||info.size>1048576)fail('backup_profile_not_trusted');return JSON.parse(await file.readFile('utf8'));}finally{await file.close();}
}
export async function loadPostgresBackupScheduleProfile(){
 const {postgresBackupSchedulePolicy}=await import('./postgres-backup-schedule.mjs');
 return postgresBackupSchedulePolicy(await protectedJson('/etc/harbor-storage-schedule.json'));
}
function inventory(profile){
 if(profile.dedicatedCluster!==true||!/^\d{1,20}$/.test(profile.expectedCluster??'')||!Array.isArray(profile.databases)||!profile.databases.length||!Array.isArray(profile.roles)||!profile.roles.length||!Array.isArray(profile.memberships))fail('backup_cluster_scope_required');
 for(const rows of [profile.databases,profile.roles]){if(rows.some(row=>!row||typeof row.name!=='string'||!row.name||!/^[1-9]\d*$/.test(row.oid??''))||new Set(rows.map(row=>row.name)).size!==rows.length)fail('backup_cluster_scope_required');}
 if(profile.roles.some(row=>['superuser','replication','can_login','create_database','create_role','inherit','bypass_rls'].some(key=>typeof row[key]!=='boolean')))fail('backup_cluster_scope_required');
}
async function ownedDirectory(path,uid){const info=await lstat(path);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==uid||(info.mode&0o077)!==0||await realpath(path)!==resolve(path))fail('backup_directory_not_private');return resolve(path);}
export async function loadPostgresBackupArchiveProfile(path='/etc/harbor-storage-archive.json'){
 if(path!=='/etc/harbor-storage-archive.json')fail('backup_archive_profile_path');const config=await protectedJson(path,{mode:0o644});
 if(config.version!==1||!/^\d{1,20}$/.test(config.expectedCluster??'')||!Number.isSafeInteger(config.uid)||config.uid<1||typeof config.keyVersion!=='string'||!/^[-a-zA-Z0-9_.]{1,64}$/.test(config.keyVersion)||!['pgDataDirectory','archiveDirectory','keyFile'].every(key=>typeof config[key]==='string'&&config[key]===resolve(config[key])))fail('backup_archive_profile_invalid');
 await ownedDirectory(config.pgDataDirectory,config.uid);await ownedDirectory(config.archiveDirectory,config.uid);
 if((await readFile(join(config.pgDataDirectory,'PG_VERSION'),'utf8')).trim()!=='18')fail('backup_postgres_version');
 const global=await lstat(join(config.pgDataDirectory,'global'));if(!global.isDirectory()||global.isSymbolicLink())fail('backup_archive_data_identity');
 const control=await open(join(config.pgDataDirectory,'global','pg_control'),constants.O_RDONLY|constants.O_NOFOLLOW);try{const bytes=Buffer.alloc(8);if((await control.read(bytes,0,8,0)).bytesRead!==8||(endianness()==='LE'?bytes.readBigUInt64LE():bytes.readBigUInt64BE()).toString()!==config.expectedCluster)fail('backup_archive_data_identity');}finally{await control.close();}
 const file=await open(config.keyFile,constants.O_RDONLY|constants.O_NOFOLLOW);let key;try{const info=await file.stat();if(!info.isFile()||info.uid!==config.uid||(info.mode&0o777)!==0o600||info.size!==32)fail('backup_archive_key_not_private');key=artifactKey(await file.readFile());}finally{await file.close();}
 return {configuration:config,configurationDigest:digest(config),key};
}
export async function createPostgresBackupContext(path='/etc/harbor-storage-backup.json'){
 return createInstalledPostgresBackupContext(path);
}
export async function createResumingPostgresBackupContext({context,resumeBinding,signal}={},path='/etc/harbor-storage-backup.json'){
 if(!resumeBinding)fail('backup_resume_binding_required');signal?.throwIfAborted();return createInstalledPostgresBackupContext(path,{context,resumeBinding,signal});
}
async function createInstalledPostgresBackupContext(path,resume){
 if(process.platform!=='linux'||process.getuid()!==0||path!=='/etc/harbor-storage-backup.json')fail('backup_root_profile_required');
 const profile=await protectedJson(path);inventory(profile);if(profile.version!==1||Object.keys(profile).some(key=>!['version','dedicatedCluster','expectedCluster','databases','roles','memberships','artifactDirectory','pgBinDirectory','libraryDirectory','archiveProfile','processTimeoutMs','maxBackupBytes','maxWalSegments','checkpointMode','maxRateKb','restoreDirectory'].includes(key)))fail('backup_profile_invalid');
 if(!Number.isSafeInteger(profile.processTimeoutMs)||profile.processTimeoutMs<1000||profile.processTimeoutMs>86400000||!Number.isSafeInteger(profile.maxBackupBytes)||profile.maxBackupBytes<1||!Number.isSafeInteger(profile.maxWalSegments)||profile.maxWalSegments<1||profile.maxWalSegments>1048576||!['fast','spread'].includes(profile.checkpointMode)||typeof profile.pgBinDirectory!=='string'||resolve(profile.pgBinDirectory)!==profile.pgBinDirectory)fail('backup_profile_invalid');
 if(profile.maxRateKb!==undefined&&(!Number.isSafeInteger(profile.maxRateKb)||profile.maxRateKb<32||profile.maxRateKb>1048576))fail('backup_profile_invalid');
 if(profile.archiveProfile!=='/etc/harbor-storage-archive.json')fail('backup_archive_profile_path');
 const secret=await open('/etc/harbor-storage-operator.env',constants.O_RDONLY|constants.O_NOFOLLOW);let connectionString;try{const info=await secret.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.size>16384)fail('backup_admin_connection_not_private');const match=/^HARBOR_STORAGE_OPERATOR_ADMIN_DATABASE_URL=(postgres(?:ql)?:\/\/[^\r\n]+)\n?$/.exec(await secret.readFile('utf8'));if(!match)fail('backup_admin_connection_required');connectionString=match[1];}finally{await secret.close();}
 const address=new URL(connectionString);if(address.searchParams.get('sslmode')!=='verify-full'||!address.searchParams.get('sslrootcert'))fail('backup_verified_database_tls_required');
 const archive=await loadPostgresBackupArchiveProfile();let retained=false;try{const actual=await observeBackupCluster({connectionString});
 if(profile.expectedCluster!==actual.system_id||archive.configuration.expectedCluster!==actual.system_id||archive.configuration.pgDataDirectory!==actual.data_directory||!resume&&(canonical(profile.databases)!==canonical(actual.databases)||canonical(profile.roles)!==canonical(actual.roles)||canonical(profile.memberships)!==canonical(actual.memberships)))fail('backup_cluster_scope_mismatch');
 const expected=resume?{...actual,databases:profile.databases,roles:profile.roles,memberships:profile.memberships}:actual;
 if(resume){const {inspectStorageOperatorResumeBinding}=await import('./storage-operator-resume-binding.mjs');await inspectStorageOperatorResumeBinding(resume.resumeBinding,{context:resume.context,actual,baselineExpected:expected,baselineConfigurationDigest:digest(profile),signal:resume.signal});}
 const artifactDirectory=await ownedDirectory(profile.artifactDirectory,0),restoreDirectory=await ownedDirectory(profile.restoreDirectory,archive.configuration.uid),restoreOwner=await lstat(restoreDirectory);
 if(restoreDirectory===actual.data_directory||restoreDirectory.startsWith(actual.data_directory+'/')||actual.data_directory.startsWith(restoreDirectory+'/')||restoreDirectory===artifactDirectory||restoreDirectory.startsWith(artifactDirectory+'/'))fail('backup_restore_directory_overlap');
 if(profile.libraryDirectory!==undefined&&(typeof profile.libraryDirectory!=='string'||resolve(profile.libraryDirectory)!==profile.libraryDirectory))fail('backup_toolchain_not_trusted');
 for(const directory of [profile.pgBinDirectory,...(profile.libraryDirectory?[profile.libraryDirectory]:[])])for(let parent=directory;;parent=resolve(parent,'..')){const info=await lstat(parent);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||(info.mode&0o022)!==0)fail('backup_toolchain_not_trusted');if(parent==='/')break;}
 for(const name of ['pg_basebackup','pg_verifybackup','pg_waldump','pg_controldata']){const file=await lstat(join(profile.pgBinDirectory,name));if(!file.isFile()||file.isSymbolicLink()||file.uid!==0||(file.mode&0o022)!==0)fail('backup_toolchain_not_trusted');}
 resume?.signal?.throwIfAborted();const context=publish({scope:'existing-cluster',profile,configurationDigest:digest(profile),adminOptions:{connectionString},expected,artifactDirectory,archiveDirectory:archive.configuration.archiveDirectory,key:archive.key,keyVersion:archive.configuration.keyVersion,archiveProfileDigest:archive.configurationDigest,restoreDirectory,restoreUid:restoreOwner.uid,restoreGid:restoreOwner.gid,archiveKeyFile:archive.configuration.keyFile,...(resume?{resumeBinding:resume.resumeBinding,resumeOperatorContext:resume.context}:{}),closed:false});retained=true;return context;
 }finally{if(!retained)archive.key.fill(0);}
}
export async function createTaskPostgresBackupContext({laboratory,directory,archiveKey,binDirectory,libraryDirectory,maxBackupBytes=1073741824,maxWalSegments=1024,maxRateKb}={}){
 const {isOwnedLaboratory,ownedProcessProof}=await import('./tools/capacity/owned-runtime.mjs');
 if(!isOwnedLaboratory(laboratory)||!laboratory.recovery||ownedProcessProof(laboratory.runtime,laboratory.process).role!=='database')fail('backup_task_ownership_required');
 const artifactDirectory=resolve(directory);if(!artifactDirectory.startsWith(laboratory.directory+'/'))fail('backup_task_path_not_owned');await mkdir(artifactDirectory,{mode:0o700});await ownedDirectory(artifactDirectory,process.getuid());
 const connection={...laboratory.connection,host:laboratory.socketDirectory,port:laboratory.databasePort};
 const actual=await observeBackupCluster({connection}),archiveDirectory=join(laboratory.directory,'wal'),key=Buffer.from(artifactKey(archiveKey)),manifest=await readManifest(join(archiveDirectory,'cluster.enc'),key);
 if(manifest.rehearsal!==true||manifest.systemIdentifier!==actual.system_id||manifest.primaryData!==actual.data_directory)fail('backup_task_archive_binding');
 const restoreDirectory=join(laboratory.directory,'backup-restore');await mkdir(restoreDirectory,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await ownedDirectory(restoreDirectory,process.getuid());
 const profile={version:1,pgBinDirectory:binDirectory,libraryDirectory,processTimeoutMs:120000,maxBackupBytes,maxWalSegments,checkpointMode:'fast',...(maxRateKb===undefined?{}:{maxRateKb})};
 return publish({scope:'task-owned',profile,configurationDigest:digest({scope:'task-owned',system:actual.system_id,artifactDirectory}),adminOptions:{connection},expected:actual,artifactDirectory,archiveDirectory,key,keyVersion:'task-owned',restoreDirectory,restoreUid:process.getuid(),restoreGid:process.getgid(),archiveKeyFile:join(laboratory.directory,'archive.key'),closed:false});
}

export async function createTaskResumingPostgresBackupContext({laboratory,baselineContext,resumeBinding,context,archiveKey,signal}={}){
 const original=postgresBackupContext(baselineContext);if(original.scope!=='task-owned'||original.pendingBinding||original.receiverBinding||original.resumeBinding||original.applicationPlan)fail('backup_resume_baseline_required');if(!resumeBinding)fail('backup_resume_binding_required');signal?.throwIfAborted();
 const {isOwnedLaboratory,ownedProcessProof}=await import('./tools/capacity/owned-runtime.mjs');if(!isOwnedLaboratory(laboratory)||!laboratory.recovery||ownedProcessProof(laboratory.runtime,laboratory.process).role!=='database'||!original.artifactDirectory.startsWith(laboratory.directory+'/'))fail('backup_task_ownership_required');
 await ownedDirectory(original.artifactDirectory,process.getuid());await ownedDirectory(original.restoreDirectory,process.getuid());const key=Buffer.from(artifactKey(archiveKey));let result;
 try{const connection={...laboratory.connection,host:laboratory.socketDirectory,port:laboratory.databasePort},actual=await observeBackupCluster({connection}),archive=await readManifest(join(laboratory.directory,'wal','cluster.enc'),key);
  if(archive.rehearsal!==true||archive.systemIdentifier!==actual.system_id||archive.primaryData!==actual.data_directory||actual.system_id!==original.expected.system_id||actual.data_directory!==original.expected.data_directory)fail('backup_resume_source_mismatch');
  const {inspectStorageOperatorResumeBinding}=await import('./storage-operator-resume-binding.mjs');await inspectStorageOperatorResumeBinding(resumeBinding,{context,actual,baselineExpected:original.expected,baselineConfigurationDigest:original.configurationDigest,signal});result=publish({...original,key,adminOptions:{connection},resumeBinding,resumeOperatorContext:context,closed:false});signal?.throwIfAborted();return result;
 }catch(error){if(result)await closePostgresBackupContext(result);else key.fill(0);throw error;}
}

export async function closePostgresBackupContext(context){const record=postgresBackupContext(context);record.closed=true;record.cancellation?.abort();try{await record.pending;}catch{}if(record.busy)fail('backup_operation_busy');record.key.fill(0);}

export async function verifyPostgresBackupPendingBinding(context){
 const record=postgresBackupContext(context);if(record.closed)fail('backup_context_closed');if(record.resumeBinding){await verifyBackupBaselineCluster(context);return null;}if(record.receiverBinding){const {inspectPostgresBackupReceiver}=await import('./postgres-backup-receiver.mjs');await inspectPostgresBackupReceiver(record.receiverBinding);return null;}if(!record.pendingBinding)return null;const {inspectPendingStorageOperatorBackup}=await import('./storage-operator-backup-binding.mjs'),observed=await inspectPendingStorageOperatorBackup(record.pendingBinding);if(digest(observed.binding)!==record.pendingBindingDigest)fail('backup_pending_binding_changed');const actual=await observeBackupCluster(record.adminOptions);if(actual.postmaster_start!==record.expected.postmaster_start)fail('backup_pending_source_restarted');if(actual.system_id!==record.expected.system_id||actual.data_directory!==record.expected.data_directory||canonical(actual.databases)!==canonical(record.expected.databases)||canonical(actual.roles)!==canonical(record.expected.roles)||canonical(actual.memberships)!==canonical(record.expected.memberships))fail('backup_pending_scope_changed');return observed.binding;
}
export async function createPendingPostgresBackupContext(baselineContext,baselineProof,baselineRestore,pendingBinding,{signal}={}){
 signal?.throwIfAborted();
 const baseline=postgresBackupContext(baselineContext);if(baseline.receiverBinding)fail('backup_receiver_readonly');if(baseline.closed||baseline.busy)fail('backup_baseline_context_unavailable');
 const {verifyPostgresBackupRestore}=await import('./postgres-backup-restore.mjs'),{postgresBackupRestoreSource}=await import('./postgres-backup.mjs'),{inspectPendingStorageOperatorBackup}=await import('./storage-operator-backup-binding.mjs');
 const baselineSource=postgresBackupRestoreSource(baselineProof);if(baselineSource.context!==baselineContext)fail('backup_baseline_context_mismatch');if(baseline.resumeBinding)await verifyBackupBaselineCluster(baselineContext,{signal});await verifyPostgresBackupRestore(baselineRestore,baselineProof,{signal});
 const pending=await inspectPendingStorageOperatorBackup(pendingBinding),binding=pending.binding;
 if(!/^[a-f0-9-]{36}$/.test(binding.runId??'')||binding.cluster!==baseline.expected.system_id||binding.backupKeyVersion!==baseline.keyVersion||binding.scope!==(baseline.scope==='task-owned'?'task-owned':'docker-systemd')||binding.roles.length!==3)fail('backup_pending_source_mismatch');
 const actual=await observeBackupCluster(baseline.adminOptions),operator=await observeBackupCluster(pending.adminOptions);
 if(canonical(actual)!==canonical(operator))fail('backup_pending_source_mismatch');if(actual.postmaster_start!==baselineSource.metadata.postmasterStart)fail('backup_pending_source_restarted');if(actual.timeline!==baselineSource.metadata.timeline)fail('backup_pending_timeline_changed');
 const names=new Set(binding.roles.map(role=>role.name)),oids=new Set(binding.roles.map(role=>role.oid));
 if(names.size!==3||oids.size!==3||baseline.expected.databases.some(row=>row.name===binding.database.name||row.oid===binding.database.oid)||baseline.expected.roles.some(row=>names.has(row.name)||oids.has(row.oid)))fail('backup_pending_scope_overlap');
 const database=actual.databases.find(row=>row.name===binding.database.name);if(database?.oid!==binding.database.oid||canonical(actual.databases.filter(row=>row.name!==binding.database.name))!==canonical(baseline.expected.databases)||canonical(actual.roles.filter(row=>!names.has(row.name)))!==canonical(baseline.expected.roles)||canonical(actual.memberships)!==canonical(baseline.expected.memberships))fail('backup_pending_scope_changed');
 for(const allowed of binding.roles){const role=actual.roles.find(row=>row.name===allowed.name);if(!role||role.oid!==allowed.oid||['superuser','replication','can_login','create_database','create_role','inherit','bypass_rls'].some(key=>role[key]!==false)||role.connection_limit!==-1||role.configuration!==null||(role.oid===binding.roleGenerations.runtimeOid?(role.valid_until===null||!Number.isFinite(Number(role.valid_until))||Number(role.valid_until)>Date.now()/1000):role.valid_until!==null))fail('backup_pending_role_changed');}
 signal?.throwIfAborted();if(baseline.resumeBinding)await verifyBackupBaselineCluster(baselineContext,{signal});const artifactDirectory=join(baseline.artifactDirectory,'pending-'+binding.runId);await mkdir(artifactDirectory,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});await ownedDirectory(artifactDirectory,process.getuid());
 if(baseline.closed||baseline.busy)fail('backup_baseline_context_unavailable');
 const pendingBindingDigest=digest(binding),context=publish({scope:baseline.scope,profile:baseline.profile,configurationDigest:digest({baseline:baseline.configurationDigest,pending:pendingBindingDigest}),adminOptions:baseline.adminOptions,expected:actual,artifactDirectory,archiveDirectory:baseline.archiveDirectory,key:Buffer.from(baseline.key),keyVersion:baseline.keyVersion,restoreDirectory:baseline.restoreDirectory,restoreUid:baseline.restoreUid,restoreGid:baseline.restoreGid,archiveKeyFile:baseline.archiveKeyFile,pendingBinding,pendingBindingDigest,closed:false});
 try{await verifyPostgresBackupPendingBinding(context);signal?.throwIfAborted();if(baseline.closed||baseline.busy)fail('backup_baseline_context_unavailable');return context;}catch(error){await closePostgresBackupContext(context);throw error;}
}

// The only source-free context constructor consumes the receiver's live brand.
// No source database connection or passed JSON can create this capability.
export async function createReceivedPostgresBackupContext(receiver){
 const {inspectPostgresBackupReceiver,bindPostgresReceiverContext}=await import('./postgres-backup-receiver.mjs'),value=await inspectPostgresBackupReceiver(receiver),{profile,manifest}=value;
 const context=publish({scope:manifest.scope,profile:{pgBinDirectory:profile.pgBinDirectory,...(profile.libraryDirectory?{libraryDirectory:profile.libraryDirectory}:{}),processTimeoutMs:profile.processTimeoutMs,maxBackupBytes:profile.maxBundleBytes,maxWalSegments:profile.maxFiles,...(value.formal&&value.candidateRuntimeApproval&&profile.candidateRuntime?{rootProtectedRuntime:true}:{})},configurationDigest:manifest.configurationDigest,adminOptions:{connection:manifest.credentials},expected:manifest.expected,artifactDirectory:join(value.working,'backup'),archiveDirectory:value.archiveDirectory,key:Buffer.from(value.key),keyVersion:manifest.keyVersion,restoreDirectory:value.restoreDirectory,restoreUid:profile.restoreUid,restoreGid:profile.restoreGid,archiveKeyFile:value.keyFile,receiverBinding:receiver,closed:false});
 try{await bindPostgresReceiverContext(receiver,context);await verifyPostgresBackupPendingBinding(context);return context;}catch(error){await closePostgresBackupContext(context);throw error;}
}
