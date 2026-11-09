import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {createReadStream} from 'node:fs';
import {mkdtemp,readFile,writeFile,rm,mkdir,lstat} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {tmpdir} from 'node:os';
import {Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {pathToFileURL} from 'node:url';
import {parseDigestManifest} from './postgres-candidate-pipeline.mjs';
import {parseInfrastructureDigest} from './postgres-infra-pipeline.mjs';

const fail=code=>{throw Error('harbor_image_stage_'+code);};
const number=value=>typeof value==='string'&&/^[1-9]\d{0,19}$/.test(value);
const digest=value=>typeof value==='string'&&/^sha256:[a-f0-9]{64}$/.test(value);
export function transportContext(env){
 if(env.CI!=='true'||env.HARBOR_IMAGE_STAGE!=='1')fail('disabled');
 const role=env.HARBOR_IMAGE_STAGE_ROLE??'gateway';
 if(!['gateway','postgres-infrastructure'].includes(role))fail('role');
 for(const name of ['CI_PROJECT_ID','CI_PIPELINE_ID','CI_JOB_ID','HARBOR_GATEWAY_PRODUCER_JOB_ID'])if(!number(env[name]))fail('identity');
 if(role==='postgres-infrastructure'&&!number(env.HARBOR_POSTGRES_INFRASTRUCTURE_PRODUCER_JOB_ID))fail('identity');
 if(!/^[a-f0-9]{40}$/.test(env.CI_COMMIT_SHA??'')||!/^[a-z0-9.:-]+$/.test(env.HARBOR_REGISTRY_HOST??'')||!['HARBOR_PROJECT','CI_PROJECT_NAME'].every(name=>/^[a-z0-9._-]+$/.test(env[name]??'')))fail('identity');
 if(!/^[a-zA-Z0-9][a-zA-Z0-9.-]*$/.test(env.GATEWAY_DEPLOY_HOST??''))fail('ssh_host');
 const job=role==='gateway'?'prepare_gateway_image_testing':'prepare_postgres_tools_image_testing';
 if(env.CI_JOB_NAME!==job)fail('job');
 return {project:env.CI_PROJECT_ID,pipeline:env.CI_PIPELINE_ID,commit:env.CI_COMMIT_SHA,job:env.CI_JOB_ID,role,repository:`${env.HARBOR_REGISTRY_HOST}/${env.HARBOR_PROJECT}/${env.CI_PROJECT_NAME}`,gatewayProducer:env.HARBOR_GATEWAY_PRODUCER_JOB_ID,infrastructureProducer:env.HARBOR_POSTGRES_INFRASTRUCTURE_PRODUCER_JOB_ID,host:env.GATEWAY_DEPLOY_HOST};
}

export async function releaseProducer(context,directory){
 const root=join(directory,'.ci-image-digests',context.pipeline);
 async function manifest(role,producer){
  const path=join(root,role+'-'+producer+'.manifest'),info=await lstat(path);
  if(!info.isFile()||info.isSymbolicLink()||info.size>4096)fail('producer_file');
  return readFile(path,'utf8');
 }
 const business=parseDigestManifest(await manifest('gateway',context.gatewayProducer),context,'gateway');
 if(business.parent!=='none')fail('parent');
 if(context.role==='gateway')return {...business,producer:context.gatewayProducer};
 const infrastructure=parseInfrastructureDigest(await manifest('postgres-infrastructure',context.infrastructureProducer),context);
 if(infrastructure.parent!==business.digest)fail('parent');
 return {...infrastructure,producer:context.infrastructureProducer};
}

// Wait for the real process close, including cancellation and bounded output.
export async function runCommand(command,args,{input,signal,timeoutMs=60000,maxBytes=1024**2}={}){
 const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),timeoutMs),combined=AbortSignal.any([signal,abort.signal].filter(Boolean));
 let child,inputTask,forceTimer;
 try{
  combined.throwIfAborted();child=spawn(command,args,{stdio:['pipe','pipe','pipe'],signal:combined});
  const force=()=>{forceTimer??=setTimeout(()=>{if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');},5000);};combined.addEventListener('abort',force,{once:true});
  const stdout=[],stderr=[];let bytes=0,error;
  for(const [stream,chunks] of [[child.stdout,stdout],[child.stderr,stderr]])stream.on('data',chunk=>{bytes+=chunk.length;if(bytes>maxBytes){error=Error('harbor_image_stage_output_budget');abort.abort();}else chunks.push(chunk);});
  child.on('error',()=>{error??=Error('harbor_image_stage_command_failed');});
  const closed=new Promise(resolve=>child.once('close',(code,exitSignal)=>resolve({code,exitSignal})));
  inputTask=pipeline(input===undefined?Readable.from([]):typeof input==='string'||Buffer.isBuffer(input)?Readable.from([input]):input,child.stdin).catch(()=>{error??=Error('harbor_image_stage_input_failed');});
  const result=await closed;await inputTask;combined.removeEventListener('abort',force);
  if(error||result.code!==0||result.exitSignal||combined.aborted)fail('command_failed');
  return Buffer.concat(stdout).toString('utf8');
 }finally{
  clearTimeout(timer);clearTimeout(forceTimer);
  if(child&&child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');await new Promise(resolve=>child.once('close',resolve));}
 }
}

export function stageOutput(text,expected){
 let value;try{value=JSON.parse(text);}catch{fail('response');}
 const result=value?.image_staging,identity=result?.identity;
 if(!result||result.contract!=='harbor-image-stage-v1'||result.ciVerified!==false||result.deploymentAllowed!==false||result.businessContainersTouched!==false||!identity)fail('response');
 for(const field of ['image','role','manifestDigest','configDigest','parentDigest','archiveSha256','archiveBytes'])if(identity[field]!==expected[field])fail('response_binding');
 const binding=Object.fromEntries(['image','role','manifestDigest','configDigest','parentDigest'].sort().map(key=>[key,expected[key]]));
 if(result.stageId!==createHash('sha256').update(JSON.stringify(binding)).digest('hex')||!['absent','queued','importing','complete','failed'].includes(result.phase))fail('response');
 if(result.phase==='complete'&&(result.loadCompleted!==true||result.loadClientExitCode!==0||result.nativeImageImported!==true||result.allConfigAndLayersVerified!==true))fail('incomplete_receipt');
 return result;
}

export async function transportReleaseImage({env=process.env,directory=process.cwd(),signal,run=runCommand,observe=value=>process.stdout.write(JSON.stringify(value)+'\n'),wait=ms=>new Promise(resolve=>setTimeout(resolve,ms))}={}){
 const context=transportContext(env),producer=await releaseProducer(context,directory);
 if(!digest(producer.digest))fail('digest');
 for(const name of ['GATEWAY_REGISTRY_USERNAME','GATEWAY_REGISTRY_PASSWORD'])if(typeof env[name]!=='string'||!env[name]||/[\0\r\n]/.test(env[name]))fail('registry_credentials');
 const temporary=await mkdtemp(join(tmpdir(),'harbor-image-transport-')),auth=join(temporary,'registry.json'),layout=join(temporary,'oci'),archive=join(temporary,'image.oci.tar');
 const image=producer.repository+':'+context.commit,ssh=['-o','BatchMode=yes','-o','ConnectTimeout=15','-o','ServerAliveInterval=15','-o','ServerAliveCountMax=3','-o','StrictHostKeyChecking=yes','-i',join(env.HOME??'/root','.ssh','harbor_deploy'),'harbor_deploy@'+context.host];
 try{
  observe({phase:'preparing',role:context.role});
  await run('skopeo',['login','--authfile',auth,'--tls-verify=false','--username',env.GATEWAY_REGISTRY_USERNAME,'--password-stdin',env.HARBOR_REGISTRY_HOST],{input:env.GATEWAY_REGISTRY_PASSWORD+'\n',signal});
  await run('skopeo',['copy','--src-authfile',auth,'--src-tls-verify=false','--preserve-digests','--override-os','linux','--override-arch','amd64','docker://'+producer.repository+'@'+producer.digest,'oci:'+layout+':'+context.commit],{signal,timeoutMs:900000});
  const raw=await run('python3',[join(directory,'ci/release-image.py'),'prepare',layout,archive,image,producer.digest,context.role,producer.parent,context.project,context.pipeline,producer.producer],{signal,timeoutMs:180000});
  let request;try{request=JSON.parse(raw);}catch{fail('prepared_request');}
  if(request.image!==image||request.manifestDigest!==producer.digest||request.role!==context.role||request.parentDigest!==producer.parent)fail('prepared_binding');
  const query=()=>run('ssh',ssh,{input:JSON.stringify({...request,action:'image-stage-status'})+'\n',signal,timeoutMs:45000});
  const sendArchive=(payload,path)=>{async function* bytes(){yield JSON.stringify(payload)+'\n';yield* createReadStream(path);}return run('ssh',ssh,{input:Readable.from(bytes()),signal,timeoutMs:1000000});};
  let result=stageOutput(await query(),request);
  if(result.phase==='absent'){
   const metadata=join(temporary,'metadata.tar'),delta=join(temporary,'delta.tar');
   let metadataInfo;try{metadataInfo=JSON.parse(await run('python3',[join(directory,'ci/release-image.py'),'delta',archive,metadata,'[]'],{signal,timeoutMs:180000}));}catch{fail('metadata_package');}
   const plan=stageOutput(await sendArchive({...request,...metadataInfo,action:'image-stage-plan'},metadata),request);
   if(!Array.isArray(plan.missingBlobs)||plan.missingBlobs.length>512||plan.missingBlobs.some(d=>!digest(d))||new Set(plan.missingBlobs).size!==plan.missingBlobs.length)fail('delta_plan');
   let deltaInfo;try{deltaInfo=JSON.parse(await run('python3',[join(directory,'ci/release-image.py'),'delta',archive,delta,JSON.stringify(plan.missingBlobs)],{signal,timeoutMs:180000}));}catch{fail('delta_package');}
   observe({phase:'transferring',role:context.role,bytes:deltaInfo.transportBytes,fullArchiveBytes:request.archiveBytes,metadataBytes:metadataInfo.transportBytes,reuseBytes:plan.reuseBytes});
   try{result=stageOutput(await sendArchive({...request,...deltaInfo},delta),request);}
   catch(error){
    // The import may have started before the SSH observer disconnected. Query
    // this exact persistent identity; never repeat upload or Docker load here.
    result=stageOutput(await query(),request);if(result.phase==='absent')throw error;
   }
  }
  const deadline=Date.now()+1260000;let previous;
  while(result.phase!=='complete'){
   signal?.throwIfAborted();
   if(result.phase==='failed')fail('remote_failed_retained');
   if(result.phase==='absent'||Date.now()>=deadline)fail('import_not_confirmed');
   if(result.phase!==previous){observe({phase:result.phase,role:context.role,stageId:result.stageId});previous=result.phase;}
   await wait(2000);result=stageOutput(await query(),request);
  }
  const output=join(directory,'.ci-image-stage',context.pipeline);await mkdir(output,{recursive:true,mode:0o700});
  await writeFile(join(output,context.role+'-'+context.job+'.json'),JSON.stringify(result)+'\n',{mode:0o600,flag:'wx'});
  observe({phase:'complete',role:context.role,stageId:result.stageId,loadCompleted:true,allConfigAndLayersVerified:true,deploymentAllowed:false});return result;
 }finally{await rm(temporary,{recursive:true,force:true});}
}

if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){
 process.umask(0o077);const controller=new AbortController();for(const name of ['SIGINT','SIGTERM'])process.once(name,()=>controller.abort());
 transportReleaseImage({signal:controller.signal}).catch(error=>{process.stderr.write(/^harbor_image_stage_[a-z_]+$/.test(error.message??'')?error.message+'\n':'harbor_image_stage_failed\n');process.exitCode=1;});
}
