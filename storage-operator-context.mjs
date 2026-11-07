import {infrastructurePublisherOption,bindInstalledInfrastructureReport} from './storage-infrastructure-report-binding.mjs';
import {cleanupStorageOperatorPreflight} from './storage-operator-preflight.mjs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {readFile,lstat,realpath,open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {resolve,join} from 'node:path';
import {createHash,randomBytes} from 'node:crypto';
import {isOwnedRuntime,isOwnedLaboratory,ownedProcessProof} from './tools/capacity/owned-runtime.mjs';
import {canonical,transferError} from './migration-artifacts.mjs';
import {verifyStoredCredentials} from './ci/postgres-preflight.mjs';
import {PostgresStore} from './postgres-store.mjs';
import {storageOperatorCandidateMaterialRoots,storageOperatorCandidateMaterialMounts} from './storage-operator-materials.mjs';
import {networkConfig} from './network.mjs';
import {storageOperatorSourceSupervisor,fenceStorageOperatorSupervisor,verifyStorageOperatorSupervisor} from './storage-operator-source-supervisor.mjs';

const exec=promisify(execFile),contexts=new WeakMap(),hash=value=>createHash('sha256').update(canonical(value)).digest('hex');
const fail=code=>{throw transferError(code);};
function transferBatchRowsOption(value){
  if(!Object.hasOwn(value,'transferBatchRows'))return {};
  if(!Number.isInteger(value.transferBatchRows)||value.transferBatchRows<1||value.transferBatchRows>500)fail('storage_operator_transfer_batch_rows');
  return {transferBatchRows:value.transferBatchRows};
}
function observeCommand({signal,deadline}){return (binary,args,options={})=>{signal?.throwIfAborted();const remaining=deadline===undefined?Infinity:Math.floor(deadline-performance.now());if(remaining<=0)fail('storage_operator_private_preflight_timeout');return exec(binary,args,{...options,signal,timeout:Math.min(options.timeout??30000,remaining)});};}
export function storageOperatorContext(context){const value=contexts.get(context);if(!value)fail('storage_operator_context_required');return value;}
export function storageOperatorCandidateOrigins(candidate){
  try{const origins={};for(const name of ['publicOrigin','deviceOrigin']){const value=candidate?.[name];if(typeof value!=='string'||!value)fail('storage_operator_candidate_origin');const parsed=networkConfig({HARBOR_PUBLIC_ORIGINS:value}).origins;if(parsed.length!==1||parsed[0]!==value)fail('storage_operator_candidate_origin');origins[name]=value;}if(new URL(origins.deviceOrigin).protocol!=='https:')fail('storage_operator_candidate_origin');return Object.freeze(origins);}catch{fail('storage_operator_candidate_origin');}
}
export function storageOperatorReadiness(context){
  const record=storageOperatorContext(context);
  // Subsequent batches must replace these missing capabilities with verified
  // backup/off-host bindings and a trusted deployment consumer, never booleans.
  return {eligible:false,scope:record.scope,missing:['existing_cluster_backup_verification','offhost_restore_verification','deployment_activation_consumer']};
}
async function sourceDirectory(path){const info=await lstat(path),actual=await realpath(path);if(!info.isDirectory()||info.isSymbolicLink()||actual!==resolve(path))fail('storage_operator_source_identity');return actual;}
const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};
const physical=async path=>{const info=await lstat(path,{bigint:true});return {device:String(info.dev),inode:String(info.ino)};};
function publish(record){freeze(record.configuration);freeze(record.adminOptions);const context=Object.freeze({scope:record.scope});contexts.set(context,Object.freeze(record));return context;}

// Only task-owned real processes can enter this test path. This scope can never
// certify a Docker stop, candidate image, off-host backup or production activation.
export async function createTaskStorageOperatorContext({laboratory,runtime,sourceChild,sourceDataDir,runDirectory,fileRoots,preflightTimeoutMs=120000,transferBatchRows}={}){
  if(!isOwnedLaboratory(laboratory)||!isOwnedRuntime(runtime)||ownedProcessProof(runtime,sourceChild).role!=='gateway')fail('storage_operator_task_ownership');
  if(!Number.isSafeInteger(preflightTimeoutMs)||preflightTimeoutMs<1000||preflightTimeoutMs>120000)fail('storage_operator_preflight_budget');
  const transfer=transferBatchRowsOption(transferBatchRows===undefined?{}:{transferBatchRows});
  const source=await sourceDirectory(sourceDataDir),identity=await physical(source),pid=sourceChild.pid;let fenced=false;
  if(!source.startsWith(laboratory.directory+'/')||!resolve(runDirectory).startsWith(laboratory.directory+'/')||!Array.isArray(fileRoots)||fileRoots.some(root=>!resolve(root.source).startsWith(laboratory.directory+'/')))fail('storage_operator_task_path_not_owned');
  const observe=async()=>{if(!fenced||sourceChild.pid!==pid||sourceChild.exitCode!==0||sourceChild.signalCode!==null)fail('storage_operator_source_not_fenced');await sourceDirectory(source);if(canonical(await physical(source))!==canonical(identity))fail('storage_operator_source_identity');return {scope:'task-owned',sourceStopped:true,supervisorFenced:true,productionVerified:false};};
  const admin=new PostgresStore({connection:laboratory.connection,allowPlaintextLocalNetwork:true,poolSize:1});let registry;try{const row=await admin.get('SELECT current_database() database,(SELECT oid::text FROM pg_database WHERE datname=current_database()) database_oid');registry={database:row.database,databaseOid:row.database_oid,schema:'harbor_operator_registry',allowMetadataWrites:true};}finally{await admin.close();}
  const configuration=structuredClone({sourceDataDir:source,runDirectory:resolve(runDirectory),fileRoots,expectedCluster:null,sourceIdentity:identity,registry,backupKeyVersion:'task-owned',...transfer});
  return publish({scope:'task-owned',preflightTimeoutMs,sourceOwnerUid:process.getuid(),configuration,configurationDigest:hash(configuration),adminOptions:{connection:laboratory.connection,allowPlaintextLocalNetwork:true},
    fence:async()=>{if(!fenced){await runtime.stop(sourceChild);fenced=true;}return observe();},observe,
    runtimePreflight:async({store,materials})=>{await store.verifySchema();await verifyStoredCredentials(store,await readFile(join(materials,'data','key-encryption.key')));return {scope:'task-runtime-core',credentialsVerified:true,schemaVerified:true,candidateVerified:false,missing:['actual_candidate_image_preflight','fresh_storage_report','server_device_tls_preflight']};}
  });
}

export async function createTaskStorageOperatorReadinessContext(options){
  const full=storageOperatorContext(await createTaskStorageOperatorContext(options));return publish({...full,scope:'task-readiness-only',fence:undefined,runtimePreflight:undefined});
}

async function rootFile(path){if(path!=='/etc/harbor-storage-operator.json')fail('storage_operator_configuration_path');const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);try{const info=await file.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o077)!==0||info.size>1048576)fail('storage_operator_configuration_not_trusted');return JSON.parse(await file.readFile('utf8'));}finally{await file.close();}}
const digest=/^sha256:[a-f0-9]{64}$/;
export async function createDockerStorageOperatorContext(configFile){
  if(process.platform!=='linux'||process.getuid()!==0)fail('storage_operator_root_required');
  const config=await rootFile(configFile),allowed=['version','source','runDirectory','fileRoots','expectedCluster','candidate','backupRequired','registry','backupKeyVersion','baselineReceiverMachine','infrastructurePublisher','transferBatchRows'];
  const transfer=transferBatchRowsOption(config);
  storageOperatorSourceSupervisor(config.source);
  if(config.baselineReceiverMachine!==undefined&&!/^[a-f0-9]{64}$/.test(config.baselineReceiverMachine))fail('storage_operator_baseline_receiver_required');
  if(typeof config.backupKeyVersion!=='string'||!/^[-a-zA-Z0-9_.]{1,64}$/.test(config.backupKeyVersion))fail('storage_operator_backup_key_version');
  if(!config.registry||Object.keys(config.registry).sort().join(',')!=='allowMetadataWrites,database,databaseOid,schema'||config.registry.allowMetadataWrites!==true||config.registry.schema!=='harbor_operator_registry'||typeof config.registry.database!=='string'||!/^[-a-zA-Z0-9_]{1,63}$/.test(config.registry.database)||!/^\d{1,10}$/.test(config.registry.databaseOid??''))fail('storage_operator_registry_configuration');
  if(config.version!==1||Object.keys(config).some(key=>!allowed.includes(key))||!/^\d{1,20}$/.test(config.expectedCluster??'')||!Array.isArray(config.fileRoots)||!/^[-a-z0-9]{1,63}$/.test(config.source?.project??'')||!/^[-a-zA-Z0-9_]{1,63}$/.test(config.source?.container??'')||!digest.test(config.source?.imageId??'')||!Number.isInteger(config.source?.uid)||config.source.uid<0||config.source.uid>0xfffffffe||typeof config.source.user!=='string'||!/^[-a-zA-Z0-9_:]{1,64}$/.test(config.source.user)||!digest.test(config.candidate?.imageDigest??'')||typeof config.candidate?.image!=='string'||!config.candidate.image.endsWith('@'+config.candidate.imageDigest)||!/^[-a-zA-Z0-9_.:/]+@sha256:[a-f0-9]{64}$/.test(config.candidate.image)||!/^[-a-zA-Z0-9_]{1,63}$/.test(config.candidate.network??'')||typeof config.candidate.secretsDirectory!=='string'||!resolve(config.candidate.secretsDirectory).startsWith('/opt/'))fail('storage_operator_configuration_invalid');
  const infrastructurePublisher=infrastructurePublisherOption(config.infrastructurePublisher);
  const candidateOrigins=storageOperatorCandidateOrigins(config.candidate);
  if(config.backupRequired?.local!==true||config.backupRequired?.offhost!==true)fail('storage_operator_backup_policy_required');
  storageOperatorCandidateMaterialRoots({sourceDataDir:config.source.dataDirectory,fileRoots:config.fileRoots,candidate:config.candidate});
  const source=await sourceDirectory(config.source.dataDirectory),identity=await physical(source);if(!resolve(config.runDirectory).startsWith('/opt/')||config.fileRoots.some(root=>typeof root.source!=='string'||!resolve(root.source).startsWith('/opt/')))fail('storage_operator_configuration_invalid');
  const secret=await open('/etc/harbor-storage-operator.env',constants.O_RDONLY|constants.O_NOFOLLOW);let connectionString;try{const info=await secret.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o077)!==0||info.size>16384)fail('storage_operator_admin_connection_permissions');const text=await secret.readFile('utf8'),match=/^HARBOR_STORAGE_OPERATOR_ADMIN_DATABASE_URL=(postgres(?:ql)?:\/\/[^\r\n]+)\n?$/.exec(text);if(!match)fail('storage_operator_admin_connection_required');connectionString=match[1];}finally{await secret.close();}
  const docker=async args=>JSON.parse((await exec('/usr/bin/docker',args,{timeout:30000,maxBuffer:1048576})).stdout);
  const inspect=async(command=exec)=>{const rows=JSON.parse((await command('/usr/bin/docker',['inspect',config.source.container],{timeout:30000,maxBuffer:1048576})).stdout);if(rows.length!==1)fail('storage_operator_source_identity');const row=rows[0],mount=row.Mounts?.filter(value=>value.Type==='bind'&&value.Destination==='/app/data');if(row.Image!==config.source.imageId||row.Config?.Labels?.['com.aistrong.project']!==config.source.project||row.Config?.User!==config.source.user||mount?.length!==1||await realpath(mount[0].Source)!==source)fail('storage_operator_source_identity');return row;};
  // A fresh read-only observe may attach to the already stopped container;
  // authenticated migration state separately checks its original epoch. Keep
  // context creation independent of Docker so cleanup can revoke DB access
  // even when the source container has disappeared.
  let containerId;
  const observe=async({signal,deadline}={})=>{
    const command=observeCommand({signal,deadline});
    await verifyStorageOperatorSupervisor(config.source,command);
    if(canonical(await physical(source))!==canonical(identity))fail('storage_operator_source_identity');const row=await inspect(command);if(!/^[a-f0-9]{64}$/.test(row.Id??'')||containerId!==undefined&&row.Id!==containerId||row.State?.Running||row.State?.Restarting||row.State?.Paused||row.State?.Pid!==0||row.State?.ExitCode!==0||row.State?.OOMKilled||row.State?.Error||row.HostConfig?.RestartPolicy?.Name!=='no')fail('storage_operator_source_not_fenced');
    const active=(await command('/usr/bin/docker',['ps','--quiet','--filter','label=com.aistrong.project='+config.source.project],{timeout:15000,maxBuffer:16384})).stdout.trim();if(active)fail('storage_operator_other_source_active');
    containerId??=row.Id;return {scope:'docker-systemd',sourceStopped:true,supervisorFenced:true,productionVerified:true,sourceImageId:row.Image,sourceContainerId:row.Id,sourceStartedAt:row.State.StartedAt,sourceFinishedAt:row.State.FinishedAt};
  };
  const configuration={sourceDataDir:source,runDirectory:resolve(config.runDirectory),fileRoots:config.fileRoots,expectedCluster:config.expectedCluster,candidate:config.candidate,backupRequired:config.backupRequired,sourceIdentity:identity,sourceController:structuredClone(config.source),...(infrastructurePublisher?{infrastructurePublisher}:{}),registry:config.registry,backupKeyVersion:config.backupKeyVersion,...transfer,...(config.baselineReceiverMachine?{baselineReceiverMachine:config.baselineReceiverMachine}:{})};
  const context=publish({scope:'docker-systemd',preflightTimeoutMs:120000,sourceOwnerUid:config.source.uid,configuration,configurationDigest:hash(configuration),adminOptions:{connectionString},
    fence:async()=>{const row=await inspect();if(!/^[a-f0-9]{64}$/.test(row.Id??'')||containerId!==undefined&&row.Id!==containerId)fail('storage_operator_source_identity');containerId??=row.Id;await fenceStorageOperatorSupervisor(config.source,exec);await exec('/usr/bin/docker',['update','--restart=no',containerId],{timeout:15000,maxBuffer:16384});if((await inspect()).State.Running)await exec('/usr/bin/docker',['stop','--time','300',containerId],{timeout:330000,maxBuffer:16384});return observe();},observe,
    runtimePreflight:async({connection,materials,target,signal,deadline:callerDeadline})=>{
      const entered=performance.now();if(callerDeadline!==undefined&&!Number.isFinite(callerDeadline))fail('storage_operator_private_preflight_timeout');const deadline=Math.min(entered+120000,callerDeadline??Infinity),duration=Math.ceil(deadline-entered);if(duration<=0)fail('storage_operator_private_preflight_timeout');const preflightSignal=AbortSignal.any([signal,AbortSignal.timeout(duration)].filter(Boolean)),remaining=()=>{preflightSignal.throwIfAborted();const value=Math.floor(deadline-performance.now());if(value<=0)fail('storage_operator_private_preflight_timeout');return value;};
      const privateDocker=async args=>JSON.parse((await exec('/usr/bin/docker',args,{timeout:Math.min(30000,remaining()),signal:preflightSignal,maxBuffer:1048576})).stdout);
      await observe({signal:preflightSignal,deadline});remaining();const images=await privateDocker(['image','inspect',config.candidate.image]),image=images[0];if(images.length!==1||!image.RepoDigests?.includes(config.candidate.image)||!digest.test(image.Id)||image.Config?.Labels?.['com.harbor.ci-validation-only']==='true')fail('storage_operator_candidate_image');
      if(infrastructurePublisher)await bindInstalledInfrastructureReport({context,target,signal:preflightSignal,deadline});
      const endpoint=new URL('postgresql://localhost');endpoint.hostname=connection.host;endpoint.port=String(connection.port);endpoint.username=connection.user;endpoint.password=connection.password;endpoint.pathname='/'+connection.database;endpoint.searchParams.set('sslmode','verify-full');endpoint.searchParams.set('sslrootcert','/run/secrets/postgres-ca.crt');endpoint.searchParams.set('application_name',connection.application_name);
      const deviceOrigin=new URL(candidateOrigins.deviceOrigin);
      const values={HARBOR_DATABASE_URL:endpoint.toString(),HARBOR_DATA_DIR:'/app/data',HARBOR_RUNTIME_DIR:'/app/runtime',HARBOR_RECORDING_ENABLED:'1',HARBOR_RECORDING_WORKER_ENABLED:'1',HARBOR_RECORDING_STORAGE_MODE:'report',HARBOR_RECORDING_STORAGE_REPORT_KEY_FILE:'/run/secrets/postgres-report.key',HARBOR_DEVICE_TLS_ENABLED:'1',HARBOR_DEVICE_TLS_PORT:deviceOrigin.port||'443',HARBOR_DEVICE_PUBLIC_ORIGIN:deviceOrigin.origin,HARBOR_PUBLIC_ORIGINS:config.candidate.publicOrigin+','+deviceOrigin.origin,HARBOR_DEVICE_TLS_KEY_FILE:'/run/secrets/device/server.key',HARBOR_DEVICE_TLS_CERT_FILE:'/run/secrets/device/server.pem',HARBOR_DEVICE_CA_CERT_FILE:'/run/secrets/device/ca.pem',HARBOR_DEVICE_CA_KEY_FILE:'/run/secrets/device/ca.key'};
      const name='harbor-storage-preflight-'+randomBytes(12).toString('hex'),token=randomBytes(16).toString('hex'),args=['create','--name',name,'--label','com.harbor.storage-operator-preflight=true','--label','com.harbor.storage-operator-preflight-token='+token,'--user','1000:1000','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,noexec,nosuid,size=64m','--network',config.candidate.network,'--env-file','/dev/stdin',...storageOperatorCandidateMaterialMounts(configuration,materials),'--entrypoint','node',config.candidate.image,'/app/ci/postgres-preflight.mjs'];
      try{
        await new Promise((resolve,reject)=>{const child=execFile('/usr/bin/docker',args,{timeout:Math.min(30000,remaining()),signal:preflightSignal,maxBuffer:16384},error=>error?reject(transferError('storage_operator_private_preflight_failed')):resolve());child.stdin.on('error',()=>{});child.stdin.end(Object.entries(values).map(([key,value])=>key+'='+value).join('\n')+'\n');});
        const inspected=await privateDocker(['inspect',name]);if(inspected.length!==1||inspected[0].Image!==image.Id||Object.keys(inspected[0].HostConfig?.PortBindings??{}).length||inspected[0].Config?.Labels?.['com.harbor.storage-operator-preflight']!=='true')fail('storage_operator_private_preflight_identity');
        const value=JSON.parse((await exec('/usr/bin/docker',['start','--attach',name],{timeout:remaining(),signal:preflightSignal,maxBuffer:65536})).stdout),ended=(await privateDocker(['inspect',name]))[0];
        const databaseDigest=createHash('sha256').update(target.cluster+'\0'+target.database).digest('hex');if(ended.State?.Running||ended.State?.ExitCode!==0||value.contract!=='postgres-conversation-v2'||value.storage_verified!==true||value.credentials_verified!==true||value.device_tls_verified!==true||value.database_digest!==databaseDigest||value.schema_digest!==target.schemaDigest||!/^[a-f0-9]{64}$/.test(value.source_digest??''))fail('storage_operator_private_preflight_failed');await observe({signal:preflightSignal,deadline});remaining();
        return {scope:'docker-candidate',candidateVerified:true,credentialsVerified:true,schemaVerified:true,sourceDigest:value.source_digest,imageId:image.Id,imageDigest:config.candidate.imageDigest,missing:[]};
      }finally{await cleanupStorageOperatorPreflight({name,imageId:image.Id,token,inspect:async name=>{try{return (await docker(['inspect','--type','container',name]))[0];}catch(error){if(error.code===1&&/No such (?:object|container)/.test(error.stderr??''))return null;throw transferError('storage_operator_preflight_cleanup_unknown');}},remove:async name=>{await exec('/usr/bin/docker',['rm','--force',name],{timeout:30000,maxBuffer:16384});}});}
    }
  });
  return context;
}
