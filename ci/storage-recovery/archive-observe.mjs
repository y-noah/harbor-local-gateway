import {join,resolve} from 'node:path';
import {open,readFile} from 'node:fs/promises';
import {constants} from 'node:fs';
import {pathToFileURL} from 'node:url';
import {createRecordingProbeStore} from '../../postgres-recording-storage.mjs';
import {loadPostgresBackupArchiveProfile} from '../../postgres-backup-context.mjs';
import {readWalManifest,inspectArchivedWal} from '../../postgres-recovery.mjs';
const command='/usr/local/bin/node /opt/harbor-storage-report/storage-backup.mjs archive-wal %f';
// File integrity is not a deployment capability; the fixed entry additionally
// loads the installed root profile and observes the actual owned PG instance.
export async function inspectInfrastructureArchiveFiles({archive,observed}={}){
 const result={profileBound:false,metadataReady:false,walArchiveVerified:false,missing:[]},config=archive?.configuration;
 if(!config||config.expectedCluster!==observed?.cluster||config.pgDataDirectory!==observed.directory){result.missing.push('archive_profile_binding');return result;}result.profileBound=true;
 let cluster;try{cluster=await readWalManifest(join(config.archiveDirectory,'cluster.enc'),archive.key,{ownerUid:config.uid});}catch{result.missing.push('archive_metadata');return result;}
 if(cluster.scope!=='existing-cluster'||cluster.systemIdentifier!==config.expectedCluster||cluster.primaryData!==config.pgDataDirectory||cluster.archiveProfileDigest!==archive.configurationDigest||cluster.keyVersion!==config.keyVersion){result.missing.push('archive_metadata_binding');return result;}result.metadataReady=true;
 if(!Number.isSafeInteger(observed.archivedCount)||observed.archivedCount<1||!/^[0-9A-F]{24}$/.test(observed.lastArchivedWal??'')){result.missing.push('actual_wal_archive');return result;}
 try{const wal=await inspectArchivedWal({name:observed.lastArchivedWal,directory:config.archiveDirectory,key:archive.key,ownerUid:config.uid,requireBoundName:true});if(wal.systemIdentifier!==config.expectedCluster||wal.identity?.kind!=='segment')throw Error();result.walArchiveVerified=true;}catch{result.missing.push('actual_wal_integrity');}return result;
}
export async function observeInstalledInfrastructureArchive(){
 let store,archive;try{
  if(process.getuid()!==999||process.argv.length!==2)throw Error('archive_observer_entry');
  const secret=await open('/run/harbor-postgres-password',constants.O_RDONLY|constants.O_NOFOLLOW);let password;try{const info=await secret.stat();if(!info.isFile()||info.uid!==999||(info.mode&0o777)!==0o600||info.size<1||info.size>256)throw Error('archive_observer_secret');password=(await secret.readFile('utf8')).trim();if(!password)throw Error('archive_observer_secret');}finally{await secret.close();}
  store=createRecordingProbeStore({connection:{host:'127.0.0.1',port:5432,user:'postgres',database:'postgres',password,ssl:{ca:await readFile('/run/harbor-pg-tls/ca.pem','utf8'),rejectUnauthorized:true,servername:'localhost'}}});
  const value=await store.get("SELECT system_identifier::text cluster,current_setting('data_directory') directory,current_setting('server_version_num')::integer version,current_setting('archive_mode') mode,current_setting('archive_command') command,extract(epoch from pg_postmaster_start_time())::text postmaster,(SELECT archived_count FROM pg_stat_archiver)::bigint archived_count,(SELECT last_archived_wal FROM pg_stat_archiver) last_wal,(SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()) tls FROM pg_control_system()");
  if(value.tls!==true)throw Error('archive_observer_tls');
  const observed={tlsVerified:true,uid:process.getuid(),cluster:value.cluster,directory:value.directory,version:value.version,mode:value.mode,command:value.command,postmaster:value.postmaster,archivedCount:value.archived_count,lastArchivedWal:value.last_wal};
  let status={profileBound:false,metadataReady:false,walArchiveVerified:false,missing:['archive_profile_binding']};
  try{if(observed.mode!=='on'||observed.command!==command)throw Error('archive_command_binding');archive=await loadPostgresBackupArchiveProfile();status=await inspectInfrastructureArchiveFiles({archive,observed});}catch{}
  return {schema:'harbor-infrastructure-archive-observation-v1',...observed,...status,deploymentAllowed:false};
 }finally{archive?.key.fill(0);await store?.close();}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(resolve(process.argv[1])).href)observeInstalledInfrastructureArchive().then(value=>console.log(JSON.stringify(value))).catch(()=>{process.stderr.write('Harbor归档实际状态尚未核实\n');process.exitCode=1;});
