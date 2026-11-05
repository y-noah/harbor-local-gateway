import {capacityRuntimeArtifactBinding,inspectCapacityRuntimeArtifact,readCapacityRuntimeRegular} from './runtime-artifact.mjs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {readFile,chmod,writeFile,lstat} from 'node:fs/promises';
import {join} from 'node:path';
import {randomUUID,sign,createHash} from 'node:crypto';
import {deviceEnrollmentMessage,deviceHardwareDigest} from '../../device-proof.mjs';
import {capacityAgent,requestCapacityTarget} from './http-client.mjs';
import {capacityDriverAgentBinding} from './request-driver.mjs';
import {currentCapacityObservation} from './observation-adapter.mjs';
import {observeCapacityWindowsHelper} from './windows-helper-observer.mjs';
const execute=promisify(execFile),driverMaterials=new WeakMap(),deliveryBindings=new WeakMap(),compiledHelpers=new Map();
export const capacityDeviceDeliveryBinding=actor=>deliveryBindings.get(actor)??null;
export const capacityDeviceDriverMaterial=actor=>driverMaterials.get(actor)??null;

// Linux client certificates qualify server-side enrollment only. They never
// constitute evidence for the real Windows CNG / WSL helper requirement.
export async function enrollCapacityDevice({directory,origin,ca,adminHeaders,adminAgent,actor,signal,registerCleanup,createDriverAgent,onCertificate=()=>{}}={}){
  const label='capacity-'+randomUUID(),key=join(directory,label+'.key'),csr=join(directory,label+'.csr');let agent;
  const request=async(path,body,{headers=adminHeaders,selectedAgent=adminAgent,mode='json'}={})=>{
    const result=await requestCapacityTarget({origin,path,method:body===undefined?'GET':'POST',body,headers:{'content-type':'application/json',...headers},agent:selectedAgent,mode,signal,timeoutMs:30000});
    if(!result.success)throw new Error('capacity_device_http_failed_'+result.status+'_'+(result.json?.error?.code??'unknown'));return result.json;
  };
  registerCleanup?.(()=>agent?.destroy());
  try{
    await execute('openssl',['req','-newkey','rsa:2048','-nodes','-keyout',key,'-out',csr,'-subj','/CN=Harbor Capacity Synthetic Device'],{timeout:30000,maxBuffer:32768,signal});await chmod(key,0o600);
    const invitation=await request('/api/admin/device-invitations',{member_id:actor.memberId}),challenge=await request('/api/device/enroll/challenge',{invitation:invitation.token},{headers:{}});
    const payload={invitation:invitation.token,challenge:challenge.challenge,nonce:challenge.nonce,scope:challenge.scope,csr:await readFile(csr,'utf8'),name:'容量合成设备',system:'Linux TLS protocol qualification',fingerprint:deviceHardwareDigest(invitation.scope,randomUUID()),installation_fingerprint:deviceHardwareDigest(invitation.scope,randomUUID()),fingerprint_source:'smbios_uuid'};
    payload.signature=sign('sha256',Buffer.from(deviceEnrollmentMessage(payload)),await readFile(key)).toString('base64');
    const registration=await request('/api/device/enroll/register',payload,{headers:{}}),listed=await request('/api/admin/devices?member_id='+actor.memberId),device=listed.rows.find(row=>row.id===registration.device_id);if(!device)throw new Error('capacity_device_not_listed');
    await request('/api/admin/devices/'+device.id+'/approve',{expected_version:device.version,credentials_only:false});
    const status=await request('/api/device/enroll/status',{enrollment_token:registration.enrollment_token},{headers:{}});if(status.state!=='approved'||!status.certificate)throw new Error('capacity_device_not_approved');
    await onCertificate({deviceId:device.id,certificate:status.certificate});
    const certFile=join(directory,label+'.pem');await writeFile(certFile,status.certificate,{mode:0o600});const material=Object.freeze({keyFile:key,certFile,caFile:join(directory,'ca.pem')});
    if(createDriverAgent){agent=await createDriverAgent({origin,tls:material});if(capacityDriverAgentBinding(agent)?.origin!==origin)throw new Error('capacity_device_driver_agent_invalid');}else agent=capacityAgent(origin,{ca,key:await readFile(key),cert:status.certificate});const headers={authorization:'Bearer '+actor.secret,'x-harbor-hardware':payload.fingerprint,'x-harbor-installation':payload.installation_fingerprint};
    const proof=await requestCapacityTarget({origin,path:'/v1/responses',method:'POST',headers:{'content-type':'application/json',...headers},body:{stream:true,model:'gpt-6.1-sol',input:'容量工具设备资格验证',metadata:{capacity_trace:'device-'+randomUUID()}},mode:'sse',agent,signal,timeoutMs:30000});if(!proof.success||!proof.complete)throw new Error('capacity_device_model_proof_failed_'+proof.status+'_'+proof.error);
    const policy=await request('/api/admin/members/'+actor.memberId+'/device-policy');
    const enforced=await requestCapacityTarget({origin,path:'/api/admin/members/'+actor.memberId+'/device-policy',method:'PATCH',headers:{'content-type':'application/json',...adminHeaders},body:{mode:'enforced',expected_version:policy.version},agent:adminAgent,signal,timeoutMs:30000});if(!enforced.success)throw new Error('capacity_device_enforcement_failed');
    const rejected=await requestCapacityTarget({origin,path:'/v1/responses',method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+actor.secret,'x-harbor-device-id':device.id},body:{stream:true,model:'gpt-6.1-sol',input:'拒绝伪造设备证明'},mode:'json',agent:adminAgent,signal,timeoutMs:30000});if(rejected.success||![401,403].includes(rejected.status))throw new Error('capacity_device_unsigned_not_rejected');
    const enrolled={...actor,deviceId:device.id,headers,agent,origin,realHelper:false,deviceEnforced:true,close:()=>agent.destroy()};driverMaterials.set(enrolled,material);return enrolled;
  }catch(error){agent?.destroy();throw error;}
}


export function createCapacityIdentityCleanup({stop,removeIdentity,removeDirectory}){
  let stopped=false,removed=false,complete=false;
  return async()=>{if(complete)return;if(!stopped){await stop();stopped=true;}if(!removed){await removeIdentity();removed=true;}await removeDirectory();complete=true;};
}

const powershell='/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe';
const compiler='/mnt/c/Windows/Microsoft.NET/Framework64/v4.0.30319/csc.exe';
const quote=value=>"'"+value.replaceAll("'","''")+"'";
async function ps(source){return (await execute(powershell,['-NoProfile','-NonInteractive','-EncodedCommand',Buffer.from(source,'utf16le').toString('base64')],{timeout:15000,maxBuffer:65536})).stdout.trim();}
const windowsPath=async path=>(await execute('wslpath',['-w',path],{timeout:15000,maxBuffer:8192})).stdout.trim();
const releaseHelperRecord=(path,entry)=>{if(entry.owners===0&&entry.users===0&&compiledHelpers.get(path)===entry)compiledHelpers.delete(path);};
export function releaseCompiledCapacityWindowsHelper(path){const entry=compiledHelpers.get(path);if(!entry)return;if(entry.owners>0)entry.owners--;releaseHelperRecord(path,entry);}
const retainHelper=path=>{const entry=compiledHelpers.get(path);if(!entry||entry.owners<1)throw Error('capacity_helper_binary_not_owned');entry.users++;let released=false;return ()=>{if(released)return;released=true;entry.users--;releaseHelperRecord(path,entry);};};
export async function loadCapacityTrustedWindowsHelper(artifact){
 await inspectCapacityRuntimeArtifact(artifact);const trusted=capacityRuntimeArtifactBinding(artifact),tool=trusted.trust.tools.windowsHelper;if(!tool)throw Error('capacity_helper_trusted_binary_unavailable');const sourcePaths={},sources={};for(const name of ['clients/harbor-device/HarborDevice.cs','clients/harbor-device/CapacityDeliveryObservation.cs']){sourcePaths[name]=join(trusted.trust.harness.directory,name);sources[name]=createHash('sha256').update(await readCapacityRuntimeRegular(sourcePaths[name],4*1024**2,{trusted:true})).digest('hex');if(sources[name]!==trusted.harness.files[name]||sources[name]!==tool.sourceHashes[name])throw Error('capacity_helper_trusted_source_unproven');}
 const binary=await readCapacityRuntimeRegular(tool.file,16*1024**2,{trusted:true});if(createHash('sha256').update(binary).digest('hex')!==tool.sha256)throw Error('capacity_helper_trusted_binary_changed');const previous=compiledHelpers.get(tool.file);if(previous){if(previous.record.binarySha256!==tool.sha256||previous.record.trustedHarnessSha256!==trusted.trust.harness.manifestSha256)throw Error('capacity_helper_trusted_binary_changed');previous.owners++;return tool.file;}
 if(compiledHelpers.size>=128)throw Error('capacity_helper_compile_budget');compiledHelpers.set(tool.file,{owners:1,users:0,record:Object.freeze({binarySha256:tool.sha256,sources:Object.freeze(sources),sourcePaths:Object.freeze(sourcePaths),sourceArtifact:artifact,trustedHarnessSha256:trusted.trust.harness.manifestSha256})});return tool.file;
}
export async function inspectCompiledCapacityWindowsHelper(path){const row=compiledHelpers.get(path)?.record;if(!row||createHash('sha256').update(await readFile(path)).digest('hex')!==row.binarySha256)throw Error('capacity_helper_binary_changed');if(row.sourceArtifact)await inspectCapacityRuntimeArtifact(row.sourceArtifact);for(const [name,value] of Object.entries(row.sources))if(createHash('sha256').update(row.sourceArtifact?await readCapacityRuntimeRegular(row.sourcePaths[name],4*1024**2,{trusted:true}):await readFile(row.sourcePaths[name])).digest('hex')!==value)throw Error('capacity_helper_source_changed');return row;}
export async function compileCapacityWindowsHelper(directory){
  const {fileURLToPath}=await import('node:url'),helper=join(directory,'HarborDevice.exe'),sources={},sourcePaths={};for(const [path,entry] of compiledHelpers)if(entry.users===0)try{await lstat(path);}catch(error){if(error.code==='ENOENT')compiledHelpers.delete(path);else throw error;}if(compiledHelpers.has(helper))throw Error('capacity_helper_compile_already_owned');if(compiledHelpers.size>=128)throw Error('capacity_helper_compile_budget');for(const name of ['clients/harbor-device/HarborDevice.cs','clients/harbor-device/CapacityDeliveryObservation.cs']){sourcePaths[name]=fileURLToPath(new URL('../../'+name,import.meta.url));sources[name]=createHash('sha256').update(await readFile(sourcePaths[name])).digest('hex');}
  await execute(compiler,['/nologo','/define:HARBOR_CAPACITY_OBSERVATION','/target:exe','/out:'+await windowsPath(helper),'/r:System.Net.Http.dll','/r:System.Management.dll','/r:System.Web.Extensions.dll',await windowsPath(sourcePaths['clients/harbor-device/HarborDevice.cs']),await windowsPath(sourcePaths['clients/harbor-device/CapacityDeliveryObservation.cs'])],{timeout:30000,maxBuffer:65536});await chmod(helper,0o700);compiledHelpers.set(helper,{owners:1,users:0,record:Object.freeze({binarySha256:createHash('sha256').update(await readFile(helper)).digest('hex'),sources:Object.freeze(sources),sourcePaths:Object.freeze(sourcePaths),sourceArtifact:null,trustedHarnessSha256:null})});await inspectCompiledCapacityWindowsHelper(helper);return helper;
}
export async function enrollCapacityWindowsDevice({directory,origin,ca,adminHeaders,adminAgent,actor,signal,runtime,helper,registerCleanup,onCertificate=()=>{},observeDelivery=false,onDeliveryEvent=()=>{},healthProof='model'}={}){
  const [{isOwnedRuntime},{createWindowsDeviceBridge}]=await Promise.all([import('./owned-runtime.mjs'),import('../../clients/harbor-device/wsl-bridge.mjs')]);
  if(!isOwnedRuntime(runtime)||typeof helper!=='string'||!helper.startsWith(directory+'/')&&!compiledHelpers.get(helper)?.record.sourceArtifact)throw new Error('capacity_helper_not_owned');
  if(!['model','catalog'].includes(healthProof)||typeof observeDelivery!=='boolean'||typeof onDeliveryEvent!=='function')throw new Error('capacity_helper_delivery_configuration');
  const invoke=async(args,input='')=>{const {spawn}=await import('node:child_process'),ambient=currentCapacityObservation();return new Promise((resolve,reject)=>{
    const child=spawn(helper,ambient?[...args,'--capacity-observe-http']:args,{stdio:['pipe','pipe','pipe']}),chunks=[],requests=new Map();let size=0,failure,buffer='',protocol=Promise.resolve();const timer=setTimeout(()=>{failure=new Error('capacity_helper_timeout');child.kill();},30000);
    const line=value=>{if(!value.startsWith('{"capacity_http":')){chunks.push(value+'\n');return;}protocol=protocol.then(async()=>{const event=JSON.parse(value);if(!ambient||!event||typeof event.id!=='string'||!/^[a-f0-9-]{36}$/i.test(event.id))throw new Error('capacity_helper_observation_invalid');if(event.capacity_http==='before'){
      if(requests.size>=3||requests.has(event.id)||event.method!=='POST'||!['/api/device/enroll/challenge','/api/device/enroll/register','/api/device/enroll/status'].includes(event.path))throw new Error('capacity_helper_observation_invalid');const requestId=randomUUID(),headers=await ambient.observer.beforeRequest({requestId,source:ambient.source,actorIndex:ambient.actorIndex,method:'POST',route:event.path,credentials:{cookie:null,authorization:null}});requests.set(event.id,{requestId,terminal:false});child.stdin.write(headers['x-capacity-trace']+'\n');
    }else if(event.capacity_http==='terminal'){
      const request=requests.get(event.id);if(!request||request.terminal||!Number.isInteger(event.status)||event.status!==0&&(event.status<100||event.status>599)||typeof event.complete!=='boolean'||event.complete&&event.status===0)throw new Error('capacity_helper_observation_invalid');request.terminal=true;if(event.status)await ambient.observer.response({requestId:request.requestId,status:event.status});if(event.complete)await ambient.observer.requestFinished({requestId:request.requestId});else await ambient.observer.requestFailed({requestId:request.requestId,cancelled:false});
    }else throw new Error('capacity_helper_observation_invalid');}).catch(()=>{failure=new Error('capacity_helper_observation_failed');child.kill();});};
    child.stdout.on('data',chunk=>{size+=chunk.length;if(size>262144){failure=new Error('capacity_helper_output_budget');child.kill();return;}buffer+=chunk.toString();let end;while((end=buffer.indexOf('\n'))>=0){const value=buffer.slice(0,end).replace(/\r$/,'');buffer=buffer.slice(end+1);line(value);}});child.stderr.resume();child.once('error',()=>{clearTimeout(timer);reject(new Error('capacity_helper_start_failed'));});child.once('close',async(code,exitSignal)=>{clearTimeout(timer);if(buffer){if(buffer.startsWith('{"capacity_http":'))line(buffer);else chunks.push(buffer);}await protocol;for(const request of requests.values())if(!request.terminal)try{await ambient.observer.requestFailed({requestId:request.requestId,cancelled:false});}catch{}failure||code!==0||exitSignal?reject(failure??new Error('capacity_helper_command_failed')):resolve(chunks.join(''));});child.stdin.on('error',()=>{});if(ambient)child.stdin.write(input);else child.stdin.end(input);
  });};
  const request=async(path,{method='GET',body}={})=>{const result=await requestCapacityTarget({origin,path,method,body,headers:{'content-type':'application/json',...adminHeaders},agent:adminAgent,signal,timeoutMs:30000});if(!result.success)throw new Error('capacity_helper_admin_http_failed');return result.json;};
  const releaseHelper=retainHelper(helper);let windowsRoot,state,prepared=false,serving,bridge,deliveryObservation;
  const cleanupIdentity=createCapacityIdentityCleanup({stop:async()=>{await bridge?.close({force:true});if(deliveryObservation)try{await deliveryObservation.stop();}catch{}if(serving?.pid&&serving.exitCode===null&&serving.signalCode===null)await runtime.stop(serving);},removeIdentity:async()=>{if(prepared&&await ps('Test-Path -LiteralPath '+quote(state))==='True')await invoke(['remove','--state',state]);},removeDirectory:async()=>{if(windowsRoot)await ps('if(Test-Path -LiteralPath '+quote(windowsRoot)+'){Remove-Item -LiteralPath '+quote(windowsRoot)+' -Recurse -Force}');}});
  const close=async()=>{await cleanupIdentity();await deliveryObservation?.closeWithin();releaseHelper();};
  registerCleanup?.(close);
  try{
    signal?.throwIfAborted();windowsRoot=await ps('[IO.Path]::Combine([IO.Path]::GetTempPath(),'+quote('HarborCapacity-'+randomUUID())+')');state=windowsRoot+'\\state.json';
    const invitation=await request('/api/admin/device-invitations',{method:'POST',body:{member_id:actor.memberId}}),port=await ps('$s=[Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback,0);try{$s.Start();$s.LocalEndpoint.Port}finally{$s.Stop()}');
    // Mark cleanup necessary before prepare: partial failures may already create a CNG key.
    prepared=true;const pending=JSON.parse(await invoke(['prepare','--state',state,'--gateway',origin,'--ca-file',await windowsPath(join(directory,'ca.pem')),'--port',port,'--allow-software-key','--name','容量实机合成设备'],invitation.token+'\n'));if(pending.state!=='pending')throw new Error('capacity_helper_not_pending');
    const list=await request('/api/admin/devices?member_id='+actor.memberId),device=list.rows.find(row=>row.state==='pending');if(!device)throw new Error('capacity_helper_not_listed');
    await request('/api/admin/devices/'+device.id+'/approve',{method:'POST',body:{expected_version:device.version,credentials_only:device.hardware_state==='unavailable'}});if(JSON.parse(await invoke(['complete','--state',state])).state!=='approved')throw new Error('capacity_helper_not_approved');
    // Export only the public DER of the exact certificate selected by the helper.
    const publicDer=await ps('$state=Get-Content -Raw -LiteralPath '+quote(state)+'|ConvertFrom-Json;$store=[Security.Cryptography.X509Certificates.X509Store]::new("My","CurrentUser");try{$store.Open([Security.Cryptography.X509Certificates.OpenFlags]::ReadOnly);$certs=$store.Certificates.Find([Security.Cryptography.X509Certificates.X509FindType]::FindByThumbprint,$state.CertificateThumbprint,$false);if($certs.Count -ne 1){throw "capacity_certificate_missing"};[Convert]::ToBase64String($certs[0].RawData)}finally{$store.Close()}');
    await onCertificate({deviceId:device.id,certificate:Buffer.from(publicDer,'base64')});
    serving=runtime.start(helper,['serve','--state',state,...(observeDelivery?['--capacity-observe-delivery','--capacity-control-stdin']:[])],{role:'helper',stdio:[observeDelivery?'pipe':'ignore','pipe','pipe']});
    if(observeDelivery)deliveryObservation=observeCapacityWindowsHelper({runtime,child:serving,onEvent:onDeliveryEvent});
    let ready=false;serving.stdout.on('data',chunk=>{if(chunk.toString().includes('"state":"running"'))ready=true;});const deadline=Date.now()+15000;while(!ready){signal?.throwIfAborted();if(serving.exitCode!==null||serving.signalCode!==null||Date.now()>deadline)throw new Error('capacity_helper_not_running');await new Promise(resolve=>setTimeout(resolve,20));}
    if(deliveryObservation)await deliveryObservation.sample();
    const localToken=await invoke(['local-token','--state',state]);bridge=createWindowsDeviceBridge({helper,stateFile:state,localToken});const address=await bridge.listen(),bridgeOrigin='http://127.0.0.1:'+address.port,headers={authorization:'Bearer '+actor.secret,'x-harbor-local-token':localToken};
    const proof=await requestCapacityTarget({origin:bridgeOrigin,path:healthProof==='model'?'/v1/responses':'/v1/models',method:healthProof==='model'?'POST':'GET',headers:{'content-type':'application/json',...headers},...(healthProof==='model'?{body:{stream:true,model:'gpt-6.1-sol',input:'容量实机设备资格验证'}}:{}),mode:healthProof==='model'?'sse':'json',signal,timeoutMs:30000});if(!proof.success||healthProof==='model'&&!proof.complete||healthProof==='catalog'&&!Array.isArray(proof.json?.models))throw new Error('capacity_helper_'+healthProof+'_proof_failed');
    if(healthProof==='model'){
    const policy=await request('/api/admin/members/'+actor.memberId+'/device-policy');await request('/api/admin/members/'+actor.memberId+'/device-policy',{method:'PATCH',body:{mode:'enforced',expected_version:policy.version}});
    const rejected=await requestCapacityTarget({origin,path:'/v1/responses',method:'POST',headers:{'content-type':'application/json',authorization:'Bearer '+actor.secret,'x-harbor-device-id':device.id},body:{stream:true,model:'gpt-6.1-sol',input:'拒绝伪造设备证明'},mode:'json',agent:adminAgent,signal,timeoutMs:30000});if(rejected.success||![401,403].includes(rejected.status))throw new Error('capacity_helper_unsigned_not_rejected');
    }
    const enrolled={...actor,agent:undefined,deviceId:device.id,headers,origin:bridgeOrigin,realHelper:true,deviceEnforced:healthProof==='model',...(deliveryObservation?{deliveryObservation}:{}),close};if(deliveryObservation)deliveryBindings.set(enrolled,Object.freeze({observer:deliveryObservation,deviceId:device.id,memberId:actor.memberId,keyId:actor.keyId,helper,healthProof,upstreamOrigin:origin,inspectCredential:async()=>{await inspectCompiledCapacityWindowsHelper(helper);const value=JSON.parse(await invoke(['diagnose','--state',state]));return Object.freeze({ready:value.state==='ready'&&value.key_available===true&&value.certificate_available===true,keyProtection:value.key_protection,hardware:value.hardware});}}));return enrolled;
  }catch(error){await close();throw error;}
}
