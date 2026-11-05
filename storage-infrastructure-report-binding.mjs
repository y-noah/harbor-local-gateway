import {constants} from 'node:fs';
import {open,lstat,realpath,rename,unlink} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {createHash,randomBytes,timingSafeEqual} from 'node:crypto';
import {performance} from 'node:perf_hooks';
import {setTimeout as delay} from 'node:timers/promises';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {lookup} from 'node:dns/promises';
import {acquireInstance} from './instance.mjs';
import {PostgresStore} from './postgres-store.mjs';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {verifyRecordingStorageReport} from './postgres-storage-report.mjs';
import {transferError,canonical} from './migration-artifacts.mjs';
const ROOT='/opt/product-development/harbor-local-gateway/postgres-infra',BOOTSTRAP='/etc/harbor-postgres-bootstrap.json',CONTROL=join(ROOT,'control'),DATA=join(ROOT,'db/18/docker'),exec=promisify(execFile);
const fail=code=>{throw transferError('storage_infrastructure_report_'+code);},hash=value=>createHash('sha256').update(value).digest('hex');
const exact=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...keys].sort().join(',');
export function infrastructurePublisherOption(value){if(value===undefined)return undefined;if(!exact(value,['mode'])||value.mode!=='installed-bootstrap-v1')fail('configuration');return Object.freeze({mode:value.mode});}
export function infrastructurePublisherPlan(current,target){
 if(!exact(current,['version','database','pgDataDir','socketDirectory','port','keyFile'])||current.version!==1||current.pgDataDir!==DATA||current.socketDirectory!=='/var/run/postgresql'||current.port!==5432||current.keyFile!=='/run/harbor-report.key'||current.database!==null&&current.database!==target.database)fail('control_configuration');
 return {...current,database:target.database};
}
export function infrastructureBootstrapFingerprint(value){return hash('{'+Object.keys(value).filter(key=>key!=='publisherDatabase').sort().map(key=>JSON.stringify(key)+': '+JSON.stringify(value[key])).join(', ')+'}');}
async function directory(path,uid,...modes){
 const info=await lstat(path);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==uid||!modes.includes(info.mode&0o777)||await realpath(path)!==resolve(path))fail('directory_identity');return {dev:info.dev,ino:info.ino};
}
async function infrastructureAdminEndpoint(connectionString,port,resolver=lookup){
 const endpoint=new URL(connectionString);if(!['127.0.0.1','localhost','harbor-local-gateway-postgres-infra'].includes(endpoint.hostname)||Number(endpoint.port||5432)!==port)fail('endpoint_identity');
 const addresses=await resolver(endpoint.hostname,{all:true});if(!addresses.length||addresses.some(row=>!['127.0.0.1','::1'].includes(row.address)))fail('endpoint_identity');return endpoint;
}
async function protectedFile(path,uid,mode,max,size){
 const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const before=await file.stat();if(!before.isFile()||before.uid!==uid||(before.mode&0o777)!==mode||before.nlink!==1||before.size>max||size!==undefined&&before.size!==size)fail('material_identity');const value=await file.readFile(),after=await file.stat(),named=await lstat(path);if(value.length!==before.size||before.dev!==after.dev||before.ino!==after.ino||before.mtimeMs!==after.mtimeMs||before.ctimeMs!==after.ctimeMs||named.dev!==after.dev||named.ino!==after.ino||named.isSymbolicLink())fail('material_changed');return value;}finally{await file.close();}}
async function json(path,mode,max=8192){return JSON.parse((await protectedFile(path,0,mode,max)).toString());}
const same=(a,b)=>canonical(a)===canonical(b);
function budget(deadline,signal){signal?.throwIfAborted();const remaining=Math.floor(deadline-performance.now());if(remaining<=0)fail('timeout');return remaining;}
export function infrastructureContainerIdentity(row,bootstrap,state,imageId){
 const runtime=row?.Config,host=row?.HostConfig,mounts=row?.Mounts;if(!/^[a-f0-9]{64}$/.test(row?.Id??'')||row.Image!==imageId||runtime?.Image!==bootstrap.infrastructureImage||runtime?.Labels?.['com.harbor.bootstrap-owner']!==state.owner||runtime?.Labels?.['com.aistrong.project']!=='harbor-local-gateway-infra'||runtime.Entrypoint?.length!==1||runtime.Entrypoint[0]!=='/usr/local/bin/harbor-postgres-infra'||runtime.Cmd?.length||row.State?.Running!==true||row.State?.Restarting||row.State?.Paused||!Array.isArray(mounts))fail('container_identity');
 if(host?.NetworkMode!=='harbor-local-gateway-postgres-infra'||!same(host.PortBindings,{'5432/tcp':[{HostIp:'127.0.0.1',HostPort:String(bootstrap.databasePort)}]}))fail('container_identity');
 const required=new Map([[join(ROOT,'db'),[join(ROOT,'db'),true]],['/var/lib/postgresql',[join(ROOT,'image-volume'),false]],['/run/harbor-pg-tls',[join(ROOT,'pg-tls'),false]],['/run/harbor-infra-control',[CONTROL,false]],['/run/harbor-report.key',[join(ROOT,'report-pg.key'),false]],['/run/harbor-postgres-password',[join(ROOT,'postgres-password'),false]]]);
 // db 根的同路径挂载保持源端/备份端 PGDATA 无别名；归档可增加自己的独立挂载。
 const protectedRoots=[...required.keys(),'/opt/harbor-storage-report','/usr','/bin','/sbin','/lib','/lib64','/etc'];
 if((runtime.Env??[]).some(value=>/^(?:NODE_OPTIONS|NODE_PATH|LD_PRELOAD|LD_LIBRARY_PATH)=./.test(value)))fail('container_environment');
 const archives=new Map([['/etc/harbor-storage-archive.json',['/etc/harbor-storage-archive.json',false]],[join(ROOT,'archive.key'),[join(ROOT,'archive.key'),false]],[join(ROOT,'archive'),[join(ROOT,'archive'),true]]]);
 const destinations=new Set();
 for(const mount of mounts){
  if(typeof mount.Destination!=='string'||destinations.has(mount.Destination))fail('container_mount');destinations.add(mount.Destination);
  if(required.has(mount.Destination)||archives.has(mount.Destination)){
   const [source,rw]=(required.get(mount.Destination)??archives.get(mount.Destination));
   if(mount.Type!=='bind'||mount.Source!==source||mount.RW!==rw)fail('container_mount');required.delete(mount.Destination);
  }else if(mount.Destination==='/var/run/docker.sock'||mount.Destination===ROOT||mount.Destination==='/'||protectedRoots.some(path=>path===mount.Destination||path.startsWith(mount.Destination+'/')||mount.Destination.startsWith(path+'/')))fail('container_mount');
 }
 if(required.size)fail('container_mount');
 return {id:row.Id,started:row.State.StartedAt,imageId};
}
export async function infrastructureTargetIdentity(store,target){
 if(!target||!/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(target.database??'')||!/^\d{1,20}$/.test(target.cluster??'')||!/^\d{1,10}$/.test(target.databaseOid??'')||!/^[a-f0-9]{64}$/.test(target.schemaDigest??''))fail('target_identity');
 const identity=await store.get("SELECT current_database() database,(SELECT oid::text FROM pg_database WHERE datname=current_database()) database_oid,(SELECT system_identifier::text FROM pg_control_system()) cluster,current_setting('data_directory') data_directory,pg_postmaster_start_time()::text postmaster,floor(extract(epoch FROM clock_timestamp())*1000)::bigint at");
 if(identity?.database!==target.database||identity.database_oid!==target.databaseOid||identity.cluster!==target.cluster||!Number.isSafeInteger(identity.at))fail('target_identity');
 const activation=await store.get('SELECT database_oid,state FROM harbor_operator.activation WHERE singleton=true');if(activation?.database_oid!==target.databaseOid||activation.state!=='pending')fail('target_activation');
 await store.verifySchema();if(hash(JSON.stringify(await store.all('SELECT version,checksum FROM harbor.schema_migrations ORDER BY version')))!==target.schemaDigest)fail('target_schema');return identity;
}
export async function waitInfrastructureTargetReport({store,key,target,minimumSample,deadline,signal}){
 for(;;){budget(deadline,signal);const identity=await infrastructureTargetIdentity(store,target),record=await store.get('SELECT CASE WHEN octet_length(payload::text)<=16384 THEN payload END payload,signature FROM harbor.recording_storage_reports WHERE singleton=true');try{const payload=verifyRecordingStorageReport(record,key,identity);if(payload.sampled_at>=minimumSample){const final=await infrastructureTargetIdentity(store,target);verifyRecordingStorageReport(record,key,final);budget(deadline,signal);return {sampledAt:payload.sampled_at,expiresAt:payload.expires_at,identity:final};}}catch(error){if(!error.code?.startsWith('recording_storage_report_'))throw error;}
  await delay(Math.min(250,budget(deadline,signal)),undefined,{signal});
 }
}
async function replaceControl(control,current,next,check){
 const raw=await protectedFile(join(control,'publisher.json'),0,0o644,4096);if(!same(JSON.parse(raw),current))fail('control_changed');if(same(current,next))return;
 const path=join(control,'.publisher-binding-'+randomBytes(16).toString('hex')),file=await open(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);let published=false;try{await file.writeFile(JSON.stringify(next)+'\n');await file.chmod(0o644);await file.sync();await check();if(!(await protectedFile(join(control,'publisher.json'),0,0o644,4096)).equals(raw))fail('control_changed');await rename(path,join(control,'publisher.json'));published=true;const parent=await open(control,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await parent.sync();}finally{await parent.close();}}finally{await file.close();if(!published)await unlink(path);}}
async function acquirePublisherBindingLock(control){try{return await acquireInstance(join(control,'publisher-binding'));}catch(error){if(error.code==='EADDRINUSE')fail('lock_busy');throw error;}}
export async function bindInstalledInfrastructureReport({context,target,deadline,signal}){
 const trusted=storageOperatorContext(context),configuration=trusted.configuration;if(process.platform!=='linux'||process.arch!=='x64'||process.getuid()!==0||trusted.scope!=='docker-systemd'||infrastructurePublisherOption(configuration.infrastructurePublisher)?.mode!=='installed-bootstrap-v1')fail('authority_required');
 if(!Number.isFinite(deadline)||deadline>performance.now()+120001)fail('timeout');budget(deadline,signal);
 for(let path=ROOT;;path=resolve(path,'..')){const info=await lstat(path);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||(info.mode&0o022)!==0)fail('directory_identity');if(path==='/')break;}
 // 新 bootstrap 仅在此根增加穿越位，子目录和材料仍按各自私有权限核验。
 await directory(ROOT,0,0o700,0o711);const bootstrap=await json(BOOTSTRAP,0o600),state=await json(join(ROOT,'state.json'),0o600);if(!exact(bootstrap,['version','businessImage','infrastructureImage','sourceImageId','publicOrigin','deviceOrigin','databasePort','publisherDatabase'])||bootstrap.version!==1||bootstrap.businessImage!==configuration.candidate.image||bootstrap.sourceImageId!==configuration.sourceController.imageId||bootstrap.publicOrigin!==configuration.candidate.publicOrigin||bootstrap.deviceOrigin!==configuration.candidate.deviceOrigin||!/^[-a-zA-Z0-9_.:/]+@sha256:[a-f0-9]{64}$/.test(bootstrap.infrastructureImage??'')||!Number.isInteger(bootstrap.databasePort)||bootstrap.databasePort<1024||bootstrap.databasePort>65535||bootstrap.publisherDatabase!==null&&bootstrap.publisherDatabase!==target.database||state.version!==1||!/^([a-f0-9]{32})$/.test(state.owner??'')||state.configuration!==infrastructureBootstrapFingerprint(bootstrap)||configuration.expectedCluster!==target.cluster||configuration.candidate.secretsDirectory!==join(ROOT,'app-secrets'))fail('bootstrap_identity');
 const controlIdentity=await directory(CONTROL,0,0o755),dataIdentity=await directory(DATA,999,0o700),current=await json(join(CONTROL,'publisher.json'),0o644,4096),next=infrastructurePublisherPlan(current,target);let operation,key,appKey,store;
 const docker=async args=>JSON.parse((await exec('/usr/bin/docker',args,{timeout:Math.min(30000,budget(deadline,signal)),signal,maxBuffer:1048576})).stdout);
 try{
  operation=await acquirePublisherBindingLock(CONTROL);
  key=await protectedFile(join(ROOT,'report-pg.key'),999,0o600,32,32);appKey=await protectedFile(join(ROOT,'app-secrets/postgres-report.key'),1000,0o600,32,32);if(!timingSafeEqual(key,appKey))fail('key_mismatch');
  const images=await docker(['image','inspect',bootstrap.infrastructureImage]),image=images[0];if(images.length!==1||!/^sha256:[a-f0-9]{64}$/.test(image?.Id??'')||!image.RepoDigests?.includes(bootstrap.infrastructureImage)||image.Config?.Labels?.['com.harbor.infrastructure']!=='postgres18-report-v1'||image.Config?.Labels?.['com.harbor.publisher-business-image']!==bootstrap.businessImage||image.Config?.Labels?.['com.harbor.ci-validation-only']==='true')fail('image_identity');
  const observe=async()=>{const rows=await docker(['inspect','harbor-local-gateway-postgres-infra']);if(rows.length!==1)fail('container_identity');return infrastructureContainerIdentity(rows[0],bootstrap,state,image.Id);},initial=await observe();
  const control=await protectedFile(join(DATA,'global/pg_control'),999,0o600,8192,8192);if(control.readBigUInt64LE(0).toString()!==target.cluster||(await protectedFile(join(DATA,'PG_VERSION'),999,0o600,32)).toString().trim()!=='18')fail('pgdata_identity');
  const endpoint=await infrastructureAdminEndpoint(trusted.adminOptions.connectionString,bootstrap.databasePort);endpoint.pathname='/'+target.database;
  store=new PostgresStore({connectionString:endpoint.toString(),poolSize:1,connectionTimeoutMs:1000,statementTimeoutMs:1000});store.pool.options.query_timeout=1000;const identity=await infrastructureTargetIdentity(store,target);if(identity.data_directory!==DATA)fail('pgdata_identity');
  const check=async()=>{budget(deadline,signal);if(!same(await json(BOOTSTRAP,0o600),bootstrap)||!same(await json(join(ROOT,'state.json'),0o600),state)||!same(await directory(CONTROL,0,0o755),controlIdentity)||!same(await directory(DATA,999,0o700),dataIdentity)||!same(await observe(),initial))fail('source_changed');const currentIdentity=await infrastructureTargetIdentity(store,target);if(currentIdentity.data_directory!==DATA||currentIdentity.postmaster!==identity.postmaster)fail('source_changed');};
  await check();await replaceControl(CONTROL,current,next,check);
  const proof=await waitInfrastructureTargetReport({store,key,target,minimumSample:identity.at,deadline,signal});await check();if(!same(await json(join(CONTROL,'publisher.json'),0o644,4096),next)||!(await protectedFile(join(ROOT,'report-pg.key'),999,0o600,32,32)).equals(key)||!(await protectedFile(join(ROOT,'app-secrets/postgres-report.key'),1000,0o600,32,32)).equals(appKey))fail('source_changed');
  return {scope:'installed-bootstrap-v1',databaseOid:target.databaseOid,sampledAt:proof.sampledAt,expiresAt:proof.expiresAt};
 }finally{key?.fill(0);appKey?.fill(0);try{await store?.close();}finally{await operation?.release();}}
}
