import {createHash} from 'node:crypto';
import {constants,createReadStream,createWriteStream} from 'node:fs';
import {open,lstat,mkdir,mkdtemp,rm,statfs} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Readable,Transform} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {pathToFileURL} from 'node:url';
import {parseDigestManifest} from './postgres-candidate-pipeline.mjs';
import {parseInfrastructureDigest} from './postgres-infra-pipeline.mjs';

const fail=(code,httpStatus)=>{const error=Error('capacity_export_'+code);if(Number.isInteger(httpStatus)&&httpStatus>=400&&httpStatus<=599)error.httpStatus=httpStatus;throw error;};
const hash=data=>createHash('sha256').update(data).digest('hex');
const isDigest=value=>typeof value==='string'&&/^sha256:[a-f0-9]{64}$/.test(value);
const integer=value=>typeof value==='string'&&/^[1-9]\d*$/.test(value);
const authChecks=new Set(['realm_url','realm_origin','realm_path','realm_parameters','service','scope']);
const rejectAuth=authCheck=>{throw Object.assign(Error('capacity_export_registry_auth_destination'),{authCheck});};
const manifestTypes=['application/vnd.oci.image.manifest.v1+json','application/vnd.docker.distribution.manifest.v2+json'];
const configTypes=['application/vnd.oci.image.config.v1+json','application/vnd.docker.container.image.v1+json'];
const layerTypes=['application/vnd.oci.image.layer.v1.tar+gzip','application/vnd.oci.image.layer.v1.tar','application/vnd.oci.image.layer.v1.tar+zstd','application/vnd.docker.image.rootfs.diff.tar.gzip'];

export function capacityExportContext(env){
 const kind=env.HARBOR_CAPACITY_EXPORT_KIND??'business';if(!['business','infrastructure'].includes(kind))fail('kind');const infrastructure=kind==='infrastructure',jobName=infrastructure?'export_postgres_infrastructure_oci':'export_capacity_business_oci';
 if((infrastructure?env.HARBOR_POSTGRES_INFRA_SMOKE:env.HARBOR_CAPACITY_EXPORT)!=='1'||env.CI!=='true'||env.CI_JOB_NAME!==jobName)fail('not_enabled');
 if(infrastructure&&!integer(env.HARBOR_POSTGRES_INFRASTRUCTURE_PRODUCER_JOB_ID))fail('identity');
 for(const key of ['CI_PROJECT_ID','CI_PIPELINE_ID','CI_JOB_ID','HARBOR_GATEWAY_PRODUCER_JOB_ID'])if(!integer(env[key]))fail('identity');
 if(!/^[a-f0-9]{40}$/.test(env.CI_COMMIT_SHA??'')||!['HARBOR_PROJECT','CI_PROJECT_NAME'].every(key=>/^[a-z0-9._-]+$/.test(env[key]??''))||!/^[a-z0-9.:-]+$/.test(env.HARBOR_REGISTRY_HOST??''))fail('identity');
 let server,project,api,registry;
 try{server=new URL(env.CI_SERVER_URL);project=new URL(env.CI_PROJECT_URL);api=new URL(env.CI_API_V4_URL);registry=new URL('http://'+env.HARBOR_REGISTRY_HOST);}catch{fail('destination');}
 for(const value of [server,project,api,registry])if(!['http:','https:'].includes(value.protocol)||value.username||value.password||value.search||value.hash)fail('destination');
 if(server.pathname!=='/'||project.origin!==server.origin||api.origin!==server.origin||api.pathname!=='/api/v4'||registry.pathname!=='/'||!registry.hostname||registry.host!==env.HARBOR_REGISTRY_HOST)fail('destination');
 const raw=env.HARBOR_CAPACITY_EXPORT_MAX_BYTES??String(3*1024**3),maxBytes=Number(raw);
 if(!integer(raw)||!Number.isSafeInteger(maxBytes)||maxBytes<1024**2||maxBytes>8*1024**3)fail('budget');
 const duration=env.HARBOR_CAPACITY_EXPORT_TIMEOUT_MS??'900000',timeoutMs=Number(duration);if(!integer(duration)||!Number.isSafeInteger(timeoutMs)||timeoutMs<1000||timeoutMs>3600000)fail('timeout');
 const registryAuthMode=env.HARBOR_CAPACITY_EXPORT_REGISTRY_AUTH_MODE??'strict';if(!['strict','registry-origin'].includes(registryAuthMode))fail('registry_auth_mode');
 return Object.freeze({kind,jobName,infrastructureProducer:infrastructure?env.HARBOR_POSTGRES_INFRASTRUCTURE_PRODUCER_JOB_ID:undefined,outputDirectory:infrastructure?'.ci-postgres-infrastructure-materials':'.ci-capacity-materials',project:env.CI_PROJECT_ID,pipeline:env.CI_PIPELINE_ID,job:env.CI_JOB_ID,commit:env.CI_COMMIT_SHA,gatewayProducer:env.HARBOR_GATEWAY_PRODUCER_JOB_ID,repository:`${env.HARBOR_REGISTRY_HOST}/${env.HARBOR_PROJECT}/${env.CI_PROJECT_NAME}`,repositoryPath:`${env.HARBOR_PROJECT}/${env.CI_PROJECT_NAME}${infrastructure?'-pg-infra':''}`,registry:registry.origin,registryAuthMode,api:api.href,packageName:infrastructure?'harbor-postgres-infrastructure-oci':'harbor-capacity-oci',packageVersion:`${env.CI_PIPELINE_ID}-${env.CI_JOB_ID}`,maxBytes,timeoutMs});
}

async function smallBody(response,limit,signal){
 const chunks=[];let size=0;
 try{for await(const chunk of Readable.fromWeb(response.body)){signal?.throwIfAborted();size+=chunk.length;if(size>limit)fail('metadata_budget');chunks.push(chunk);}return Buffer.concat(chunks);}
 finally{if(!response.bodyUsed)await response.body?.cancel();}
}
function descriptor(row,types){if(!row||!isDigest(row.digest)||!Number.isSafeInteger(row.size)||row.size<0||!types.includes(row.mediaType))fail('descriptor');return row;}
function credential(value){if(typeof value!=='string'||!value||value.length>65536||/[\0\r\n]/.test(value))fail('credential');return value;}

export async function exportCapacityBusiness({env=process.env,directory=process.cwd(),signal}={}){
 const context=capacityExportContext(env),jobToken=credential(env.CI_JOB_TOKEN),username=credential(env.GATEWAY_REGISTRY_USERNAME),password=credential(env.GATEWAY_REGISTRY_PASSWORD);
 signal=AbortSignal.any([AbortSignal.timeout(context.timeoutMs),...(signal?[signal]:[])]);signal.throwIfAborted();
 const readProducer=async name=>{const file=await open(join(directory,'.ci-image-digests',context.pipeline,name),constants.O_RDONLY|constants.O_NOFOLLOW);try{const info=await file.stat();if(!info.isFile()||info.size>4096)fail('producer_manifest');const text=await file.readFile('utf8');if(Buffer.byteLength(text)>4096)fail('producer_manifest');return text;}finally{await file.close();}};
 const businessSource=await readProducer(`gateway-${context.gatewayProducer}.manifest`),business=parseDigestManifest(businessSource,context,'gateway');if(business.parent!=='none')fail('producer_manifest');
 const infrastructure=context.kind==='infrastructure',source=infrastructure?await readProducer(`postgres-infrastructure-${context.infrastructureProducer}.manifest`):businessSource,producer=infrastructure?parseInfrastructureDigest(source,context):business;if(infrastructure&&producer.parent!==business.digest)fail('producer_manifest');
 let bearer,temporary,consumed=0;const files=[];
 const request=(url,options={})=>fetch(url,{...options,redirect:'error',signal});
 const registryResponse=async(kind,digest)=>{
  if(!['manifests','blobs'].includes(kind)||!isDigest(digest))fail('registry_path');
  const url=`${context.registry}/v2/${context.repositoryPath}/${kind}/${digest}`,headers={accept:manifestTypes.join(',')};if(bearer)headers.authorization='Bearer '+bearer;
  let response=await request(url,{headers});
  if(response.status===401&&!bearer){
   const challenge=response.headers.get('www-authenticate')??'';await response.body?.cancel();if(!/^Bearer /i.test(challenge))fail('registry_auth');
   const fields=Object.fromEntries([...challenge.matchAll(/([a-z_]+)="([^"\r\n]*)"/g)].map(row=>[row[1],row[2]]));let realm;try{realm=new URL(fields.realm);}catch{rejectAuth('realm_url');}
   if(context.registryAuthMode==='strict'&&realm.origin!==context.registry)rejectAuth('realm_origin');
   if(!['http:','https:'].includes(realm.protocol))rejectAuth('realm_url');
   if(realm.pathname!=='/service/token')rejectAuth('realm_path');
   if(realm.username||realm.password||realm.search||realm.hash)rejectAuth('realm_parameters');
   if(!/^[a-z0-9._-]{1,80}$/.test(fields.service??''))rejectAuth('service');
   if(fields.scope&&fields.scope!==`repository:${context.repositoryPath}:pull`)rejectAuth('scope');
   // An internal registry may advertise its public canonical hostname. This
   // explicit mode never follows that hostname: robot credentials still go
   // only to the already approved registry origin and fixed Harbor path.
   if(context.registryAuthMode==='registry-origin')realm=new URL('/service/token',context.registry);
   realm.search=new URLSearchParams({service:fields.service,scope:`repository:${context.repositoryPath}:pull`}).toString();
   const tokenResponse=await request(realm,{headers:{authorization:'Basic '+Buffer.from(username+':'+password).toString('base64')}});
   if(tokenResponse.status!==200){await tokenResponse.body?.cancel();fail('registry_auth',tokenResponse.status);}
   const token=JSON.parse((await smallBody(tokenResponse,65536,signal)).toString());bearer=credential(token.token??token.access_token);headers.authorization='Bearer '+bearer;response=await request(url,{headers});
  }
  if(response.status!==200){await response.body?.cancel();fail('registry_read',response.status);}return response;
 };
 const jsonBlob=async(kind,digest,size)=>{
  const data=await smallBody(await registryResponse(kind,digest),1024**2,signal);consumed+=data.length;
  if(consumed>context.maxBytes||'sha256:'+hash(data)!==digest||size!==undefined&&data.length!==size)fail('blob_integrity');return {data,value:JSON.parse(data.toString())};
 };
 const put=async(name,body,size)=>{
  const url=`${context.api}/projects/${context.project}/packages/generic/${context.packageName}/${context.packageVersion}/${name}`;
  const response=await request(url,{method:'PUT',headers:{'JOB-TOKEN':jobToken,'content-type':'application/octet-stream','content-length':String(size)},body,...(typeof body?.pipe==='function'?{duplex:'half'}:{})});
  await response.body?.cancel();if(response.status!==201)fail('package_write',response.status);
 };
 const putBytes=async(name,data)=>{await put(name,data,data.length);files.push({name,size:data.length,sha256:hash(data)});};
 try{
  signal?.throwIfAborted();
  const ownJobResponse=await request(context.api+'/job',{headers:{'JOB-TOKEN':jobToken}});
  if(ownJobResponse.status!==200){await ownJobResponse.body?.cancel();fail('job_identity',ownJobResponse.status);}
  const ownJob=JSON.parse((await smallBody(ownJobResponse,1024**2,signal)).toString());
  if(String(ownJob.id)!==context.job||ownJob.name!==context.jobName||String(ownJob.pipeline?.id)!==context.pipeline||String(ownJob.pipeline?.project_id)!==context.project||ownJob.commit?.id!==context.commit)fail('job_identity');
  const manifest=await jsonBlob('manifests',producer.digest),value=manifest.value;
  if(value.schemaVersion!==2||!manifestTypes.includes(value.mediaType)||!Array.isArray(value.layers)||value.layers.length<1||value.layers.length>128)fail('manifest');
  const configDescriptor=descriptor(value.config,configTypes),layers=value.layers.map(row=>descriptor(row,layerTypes));
  if(manifest.data.length+configDescriptor.size+layers.reduce((sum,row)=>sum+row.size,0)>context.maxBytes)fail('budget');
  const config=await jsonBlob('blobs',configDescriptor.digest,configDescriptor.size),labels=config.value.config?.Labels??{},platform={os:config.value.os,architecture:config.value.architecture};
  if(platform.os!=='linux'||!['amd64','arm64'].includes(platform.architecture)||labels['com.harbor.ci-validation-only']==='true'||config.value.rootfs?.type!=='layers'||config.value.rootfs.diff_ids?.length!==layers.length||!config.value.rootfs.diff_ids.every(isDigest))fail('image_identity');
  if(infrastructure?labels['com.harbor.infrastructure']!=='postgres18-report-v1'||labels['com.harbor.publisher-business-image']!==business.repository+'@'+business.digest:labels['org.opencontainers.image.revision']!==context.commit||labels['com.harbor.storage.postgres']!=='postgres-conversation-v2')fail('image_identity');
  temporary=await mkdtemp(join(tmpdir(),'harbor-ci-oci-export-'));const disk=await statfs(temporary);if(Number(disk.bavail)*Number(disk.bsize)<Math.max(...layers.map(row=>row.size))+1024**2)fail('disk_budget');
  await putBytes(producer.digest.slice(7),manifest.data);await putBytes(configDescriptor.digest.slice(7),config.data);
  const seen=new Set([producer.digest,configDescriptor.digest]);
  for(const row of layers){
   if(seen.has(row.digest))continue;signal?.throwIfAborted();const path=join(temporary,row.digest.slice(7)),checksum=createHash('sha256');let bytes=0;
   const response=await registryResponse('blobs',row.digest);
   await pipeline(Readable.fromWeb(response.body),new Transform({transform(chunk,_encoding,callback){bytes+=chunk.length;consumed+=chunk.length;if(bytes>row.size||consumed>context.maxBytes){callback(Error('capacity_export_budget'));return;}checksum.update(chunk);callback(null,chunk);}}),createWriteStream(path,{flags:'wx',mode:0o600}),{signal});
   if(bytes!==row.size||'sha256:'+checksum.digest('hex')!==row.digest)fail('blob_integrity');
   const stream=createReadStream(path);try{await put(row.digest.slice(7),stream,bytes);}finally{stream.destroy();}
   files.push({name:row.digest.slice(7),size:bytes,sha256:row.digest.slice(7)});seen.add(row.digest);await rm(path);
  }
  const index=Buffer.from(JSON.stringify({schemaVersion:2,mediaType:'application/vnd.oci.image.index.v1+json',manifests:[{mediaType:value.mediaType,digest:producer.digest,size:manifest.data.length,platform}]}));
  await putBytes('oci-layout',Buffer.from('{"imageLayoutVersion":"1.0.0"}'));await putBytes('ci-producer.manifest',Buffer.from(source));if(infrastructure)await putBytes('ci-business-producer.manifest',Buffer.from(businessSource));await putBytes('index.json',index);
  const receipt={schema:infrastructure?'harbor-postgres-infrastructure-ci-material-v1':'harbor-capacity-ci-material-v1',project:context.project,pipeline:context.pipeline,exportJob:context.job,producer:infrastructure?context.infrastructureProducer:context.gatewayProducer,commit:context.commit,businessDigest:business.digest,...(infrastructure?{infrastructureDigest:producer.digest,businessProducer:context.gatewayProducer}:{}),packageName:context.packageName,packageVersion:context.packageVersion,files,compressedBytes:consumed,scope:infrastructure?'postgres-infrastructure-image-materials-only':'business-image-materials-only',runtimeVerified:false,candidateVerified:false};
  // No awaited resource cleanup may turn a cancelled operation into a completed
  // artifact after publication. Package objects are intentionally retained.
  await rm(temporary,{recursive:true,force:true});temporary=undefined;signal.throwIfAborted();
  const output=join(directory,context.outputDirectory);await mkdir(output,{recursive:true,mode:0o700});signal.throwIfAborted();
  const path=join(output,`${context.pipeline}-${context.job}.json`);let completion,identity;
  try{
   completion=await open(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);identity=await completion.stat({bigint:true});signal.throwIfAborted();
   await completion.writeFile(JSON.stringify(receipt)+'\n',{signal});await completion.sync();await completion.close();completion=null;signal.throwIfAborted();return {path,files:files.length,bytes:consumed};
  }catch(error){
   try{await completion?.close();}finally{if(identity){let current;try{current=await lstat(path,{bigint:true});}catch(failure){if(failure.code!=='ENOENT')throw failure;}if(current){if(current.dev!==identity.dev||current.ino!==identity.ino||!current.isFile()||current.isSymbolicLink())fail('completion_cleanup_unknown');await rm(path);}}}throw error;
  }
 }finally{if(temporary)await rm(temporary,{recursive:true,force:true});}
}

if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){
 const controller=new AbortController(),stop=()=>controller.abort();for(const name of ['SIGINT','SIGTERM'])process.once(name,stop);
 try{const result=await exportCapacityBusiness({signal:controller.signal});console.log(JSON.stringify({state:'material_exported',files:result.files,bytes:result.bytes,runtimeVerified:false,candidateVerified:false}));}
 catch(error){console.error(JSON.stringify({error:/^capacity_export_[a-z_]+$/.test(error.message)?error.message:'capacity_export_failed',...(error.httpStatus?{httpStatus:error.httpStatus}:{}),...(authChecks.has(error.authCheck)?{authCheck:error.authCheck}:{})}));process.exitCode=1;}
 finally{for(const name of ['SIGINT','SIGTERM'])process.removeListener(name,stop);}
}
