import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {lstat,chown} from 'node:fs/promises';
import {createPostgresBackupContext,closePostgresBackupContext,loadPostgresBackupArchiveProfile,loadPostgresBackupScheduleProfile,verifyBackupCluster,postgresBackupContext} from './postgres-backup-context.mjs';
import {createPostgresBaseBackup,verifyPostgresBackup,checkpointPostgresBackup,inspectPostgresBackup} from './postgres-backup.mjs';
import {restorePostgresBackup,inspectPostgresBackupRestore} from './postgres-backup-restore.mjs';
import {preparePostgresBaselineHandoff} from './postgres-backup-handoff.mjs';
import {archiveWal,readWalManifest} from './postgres-recovery.mjs';
import {writeManifest,readManifest,canonical,transferError} from './migration-artifacts.mjs';
import {join} from 'node:path';
const fail=code=>{throw transferError(code);};
export async function storageBackupCommand(args,{signal,onScheduleCycle}={}){
 if(!Array.isArray(args)||!['check','prepare-archive','backup','verify','checkpoint','restore','baseline-handoff','archive-wal','schedule'].includes(args[0]))fail('backup_command_invalid');
 if(args[0]==='archive-wal'){
  if(args.length!==2||!/^(?:[0-9A-F]{24}|[0-9A-F]{8}\.history|[0-9A-F]{24}\.[0-9A-F]{8}\.backup)$/.test(args[1]))fail('backup_wal_name_invalid');
  const archive=await loadPostgresBackupArchiveProfile();try{if(process.getuid()!==archive.configuration.uid)fail('backup_archive_owner_required');const cluster=await readManifest(join(archive.configuration.archiveDirectory,'cluster.enc'),archive.key);if(cluster.scope!=='existing-cluster'||cluster.systemIdentifier!==archive.configuration.expectedCluster||cluster.primaryData!==archive.configuration.pgDataDirectory||cluster.archiveProfileDigest!==archive.configurationDigest||cluster.keyVersion!==archive.configuration.keyVersion)fail('backup_archive_binding');await archiveWal({source:join(cluster.primaryData,'pg_wal',args[1]),name:args[1],directory:archive.configuration.archiveDirectory,key:archive.key});return {state:'wal_archived'};}finally{archive.key.fill(0);}
 }
 if(args.length!==(['verify','checkpoint','restore','baseline-handoff'].includes(args[0])?2:1))fail('backup_arguments_invalid');
 if(args[0]==='baseline-handoff'&&!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(args[1]))fail('backup_arguments_invalid');
 if(args[0]==='schedule'){
  const configuration=await loadPostgresBackupScheduleProfile(),{watchPostgresBackupSchedule}=await import('./postgres-backup-schedule.mjs');
  if(!configuration.enabled)return watchPostgresBackupSchedule(null,configuration,{signal});
  const context=await createPostgresBackupContext();try{return await watchPostgresBackupSchedule(context,configuration,{signal,...(onScheduleCycle?{onCycle:onScheduleCycle}:{})});}finally{await closePostgresBackupContext(context);}
 }
 const context=await createPostgresBackupContext();try{
  if(args[0]==='check'){await verifyBackupCluster(context);return {state:'cluster_scope_verified',scope:'existing-cluster',deploymentAllowed:false};}
  if(args[0]==='prepare-archive'){
   const record=postgresBackupContext(context),archive=await loadPostgresBackupArchiveProfile();try{const value={version:1,scope:'existing-cluster',systemIdentifier:record.expected.system_id,primaryData:record.expected.data_directory,archiveProfileDigest:archive.configurationDigest,keyVersion:archive.configuration.keyVersion},path=join(record.archiveDirectory,'cluster.enc');let prior;try{prior=await readWalManifest(path,record.key,{ownerUid:archive.configuration.uid});}catch(error){if(error.code!=='ENOENT')throw error;}if(prior&&canonical(prior)!==canonical(value))fail('backup_archive_existing_binding');if(!prior){await writeManifest(path,value,record.key);await chown(path,archive.configuration.uid,(await lstat(archive.configuration.pgDataDirectory)).gid);}return {state:'archive_metadata_prepared',postgresConfigurationChanged:false};}finally{archive.key.fill(0);}
  }
  const proof=args[0]==='backup'?await createPostgresBaseBackup(context,{signal}):await verifyPostgresBackup(context,args[1],{signal});if(args[0]==='baseline-handoff')return await preparePostgresBaselineHandoff({context,proof,restore:await restorePostgresBackup(proof,{signal}),signal});if(args[0]==='restore')return inspectPostgresBackupRestore(await restorePostgresBackup(proof,{signal}));return args[0]==='checkpoint'?await checkpointPostgresBackup(proof,{signal}):inspectPostgresBackup(proof);
 }finally{await closePostgresBackupContext(context);}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 const controller=new AbortController();for(const name of ['SIGINT','SIGTERM'])process.once(name,()=>controller.abort());try{const result=await storageBackupCommand(process.argv.slice(2),{signal:controller.signal,onScheduleCycle:value=>process.stdout.write(JSON.stringify(value)+'\n')});if(process.argv[2]!=='archive-wal')process.stdout.write(JSON.stringify(result)+'\n');}catch(error){if(process.argv[2]!=='archive-wal')process.stderr.write(JSON.stringify({error:/^[a-z][a-z0-9_]+$/.test(error.code??'')?error.code:'backup_operation_failed',deploymentAllowed:false})+'\n');process.exitCode=1;}
}
