import {readFile,open,lstat,stat} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createHash,sign,verify,createPrivateKey,createPublicKey} from 'node:crypto';
import {join,isAbsolute} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createOwnedRuntime,ownedProcessProof} from './owned-runtime.mjs';
import {capacityRuntimeArtifactBinding,inspectCapacityRuntimeArtifact,readCapacityRuntimeRegular} from './runtime-artifact.mjs';
const generations=new WeakMap(),fail=code=>{throw Error('capacity_nginx_generation_'+code);},sha=value=>createHash('sha256').update(value).digest('hex');
const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};
const identity=info=>({device:String(info.dev),inode:String(info.ino),size:String(info.size),mtimeNs:String(info.mtimeNs),ctimeNs:String(info.ctimeNs)});
const same=(left,right)=>['device','inode','size','mtimeNs','ctimeNs'].every(key=>left[key]===right[key]);
async function processRecord(pid){const raw=await readFile('/proc/'+pid+'/stat','utf8'),parts=raw.slice(raw.lastIndexOf(')')+2).trim().split(/\s+/),status=await readFile('/proc/'+pid+'/status','utf8'),uid=/^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)/m.exec(status);if(!uid||uid.slice(1).some(value=>value!==uid[1])||['T','t','Z','X','x'].includes(parts[0]))fail('process');return {pid,startTicks:parts[19],uid:Number(uid[1]),commandSha256:sha(await readFile('/proc/'+pid+'/cmdline'))};}
export function capacityNginxGenerationBinding(handle){return generations.get(handle)??null;}
async function capture(configuration,{trusted=false,artifact,signal}={}){
 if(!configuration||configuration.type!=='nginx'||configuration.mode!=='stream-tls-passthrough'||!isAbsolute(configuration.binary?.file??'')||!isAbsolute(configuration.configurationEntry??'')||!isAbsolute(configuration.prefix??'')||!Array.isArray(configuration.materials)||configuration.materials.length>256)fail('configuration');
 const runtime=createOwnedRuntime(),fds=[],materials=[];let totalBytes=0;let child,closePromise;
 const close=()=>closePromise??=runtime.close();
 try{
  for(const row of [{...configuration.binary,role:'binary'},...configuration.materials]){signal?.throwIfAborted();const content=await readCapacityRuntimeRegular(row.file,row.role==='configuration'?4*1024**2:128*1024**2,{trusted,signal});totalBytes+=content.length;if(totalBytes>256*1024**2||sha(content)!==row.sha256)fail('material');const fd=await open(row.file,constants.O_RDONLY|constants.O_NOFOLLOW),info=await fd.stat({bigint:true});fds.push({fd,row,before:identity(info)});materials.push({file:row.file,role:row.role,sha256:row.sha256,identity:identity(info)});}
  const clean=Object.fromEntries(Object.keys(process.env).map(key=>[key,undefined]));child=runtime.start(configuration.binary.file,['-c',configuration.configurationEntry,'-p',configuration.prefix+'/'],{role:'helper',cwd:configuration.prefix,env:{...clean,PATH:'/usr/sbin:/usr/bin:/bin',LANG:'C'}});const deadline=performance.now()+10000;let workers;
  for(;;){signal?.throwIfAborted();ownedProcessProof(runtime,child);try{workers=(await readFile('/proc/'+child.pid+'/task/'+child.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);}catch{}if(workers?.length)break;if(performance.now()>=deadline)fail('startup_timeout');await new Promise(resolve=>setTimeout(resolve,5));}
  const master=await processRecord(child.pid),workerRecords=await Promise.all(workers.map(processRecord));
  for(const {fd,row,before} of fds){if(!same(before,identity(await fd.stat({bigint:true})))||!same(before,identity(await lstat(row.file,{bigint:true})))||sha(await readCapacityRuntimeRegular(row.file,row.role==='configuration'?4*1024**2:128*1024**2,{trusted,signal}))!==row.sha256)fail('changed_during_start');}
  ownedProcessProof(runtime,child);const bootId=(await readFile('/proc/sys/kernel/random/boot_id','utf8')).trim(),record=freeze({schema:'capacity-nginx-loaded-generation-v1',capture:'owned-controlled-start',bootId,master,workers:workerRecords,materials}),handle=Object.freeze(Object.create(null));generations.set(handle,Object.freeze({record,trusted,artifact,isOpen:()=>child.exitCode===null&&child.signalCode===null}));return {generation:handle,configuration:{...structuredClone(configuration),master,workers:workerRecords},child,close};
 }catch(error){try{await close();}catch{}throw error;}finally{for(const {fd} of fds)await fd.close();}
}
// This actually starts the isolated fixture, rather than signing facts read
// after an unrelated old process has already loaded different configuration.
export const startCapacityNginxDiagnosticGeneration=configuration=>capture(configuration);
// The operator may explicitly use this at an approved original proxy start.
// No topology observer calls it, and it never reloads an existing process.
export async function startCapacityApprovedNginxGeneration({artifact,signal}={}){
 await inspectCapacityRuntimeArtifact(artifact);const owner=capacityRuntimeArtifactBinding(artifact),relative='tools/capacity/nginx-generation-witness.mjs',expected=owner.harness.files[relative],ownPath=fileURLToPath(import.meta.url);if(ownPath!==join(owner.trust.harness.directory,relative)||!expected||sha(await readCapacityRuntimeRegular(ownPath,1048576,{trusted:true}))!==expected)fail('immutable_producer');
 const approval=owner.trust.nginxStartup;if(!approval||!isAbsolute(approval.file??'')||!/^[a-f0-9]{64}$/.test(approval.sha256??''))fail('root_approval');const bytes=await readCapacityRuntimeRegular(approval.file,131072,{trusted:true,signal});if(sha(bytes)!==approval.sha256)fail('root_approval');let plan;try{plan=JSON.parse(bytes);}catch{fail('root_approval');}if(plan?.schema!=='capacity-nginx-controlled-start-v1')fail('root_approval');const keyInfo=await lstat(plan.signingKeyFile);if(keyInfo.uid!==0||(keyInfo.mode&0o077)!==0)fail('signing_key');const key=createPrivateKey(await readCapacityRuntimeRegular(plan.signingKeyFile,16384,{trusted:true,signal})),publicKey=createPublicKey(key).export({type:'spki',format:'der'});if(key.asymmetricKeyType!=='ed25519'||sha(publicKey)!==plan.signerSha256)fail('signing_key');
 const result=await capture(plan.configuration,{trusted:true,artifact,signal});try{await inspectCapacityRuntimeArtifact(artifact);const record=capacityNginxGenerationBinding(result.generation).record,receipt=Buffer.from(JSON.stringify({...record,producerSha256:expected,signerSha256:plan.signerSha256})),signature=sign(null,receipt,key);return {...result,receipt,signature};}catch(error){await result.close();throw error;}
}
export async function inspectCapacityNginxLoadedGeneration(configuration,{generation,trusted=false,artifact}={}){
 let record;
 if(!trusted){const owned=capacityNginxGenerationBinding(generation);if(!owned||owned.trusted||!owned.isOpen())fail('diagnostic_generation_not_owned');record=owned.record;}
 else{
  await inspectCapacityRuntimeArtifact(artifact);const source=configuration.loadedGeneration;if(!source||!isAbsolute(source.receiptFile??'')||!isAbsolute(source.signatureFile??'')||!isAbsolute(source.publicKeyFile??'')||!/^[a-f0-9]{64}$/.test(source.receiptSha256??'')||!/^[a-f0-9]{64}$/.test(source.signerSha256??''))fail('loaded_witness_unavailable');
  const receipt=await readCapacityRuntimeRegular(source.receiptFile,131072,{trusted:true}),signature=await readCapacityRuntimeRegular(source.signatureFile,1024,{trusted:true}),publicBytes=await readCapacityRuntimeRegular(source.publicKeyFile,16384,{trusted:true}),key=createPublicKey(publicBytes);if(key.asymmetricKeyType!=='ed25519'||signature.length!==64||sha(receipt)!==source.receiptSha256||sha(key.export({type:'spki',format:'der'}))!==source.signerSha256||!verify(null,receipt,key,signature))fail('loaded_witness_signature');try{record=JSON.parse(receipt);}catch{fail('loaded_witness');}const expected=capacityRuntimeArtifactBinding(artifact).harness.files['tools/capacity/nginx-generation-witness.mjs'];if(!expected||record.producerSha256!==expected||record.signerSha256!==source.signerSha256)fail('loaded_witness_producer');
 }
 if(record?.schema!=='capacity-nginx-loaded-generation-v1'||record.capture!=='owned-controlled-start'||record.bootId!==(await readFile('/proc/sys/kernel/random/boot_id','utf8')).trim()||JSON.stringify(record.master)!==JSON.stringify(configuration.master)||JSON.stringify(record.workers)!==JSON.stringify(configuration.workers))fail('loaded_generation_changed');
 const expected=[{...configuration.binary,role:'binary'},...configuration.materials];if(!Array.isArray(record.materials)||record.materials.length!==expected.length)fail('loaded_material_changed');
 for(const row of expected){const loaded=record.materials.find(value=>value.file===row.file);if(!loaded||loaded.role!==row.role||loaded.sha256!==row.sha256||!same(loaded.identity,identity(await lstat(row.file,{bigint:true}))))fail('loaded_material_changed');
  if(row.role==='module')for(const worker of configuration.workers){const maps=(await readFile('/proc/'+worker.pid+'/maps','utf8')).split('\n'),mapped=maps.filter(line=>line.endsWith(' '+row.file));if(!mapped.length||mapped.some(line=>line.trim().split(/\s+/)[4]!==loaded.identity.inode))fail('loaded_module_unproven');}
 }
 return Object.freeze({scope:trusted?'root-signed-controlled-loaded-generation':'task-owned-controlled-loaded-generation',materials:record.materials.length});
}
