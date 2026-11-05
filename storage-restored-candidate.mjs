import {mkdir,readFile,writeFile,lstat,realpath,readdir} from 'node:fs/promises';
import {join} from 'node:path';
import {createHash} from 'node:crypto';
import {parseDigestManifest} from './ci/postgres-candidate-pipeline.mjs';
const contexts=new WeakMap(),hash=b=>createHash('sha256').update(b).digest('hex');
const fail=()=>{throw Error('storage_restored_candidate_binding');};
async function evidence({layout,revision,manifestDigest,ci}){
 if(!ci||!['project','pipeline','producer','bridge','verificationPipeline','verificationJob'].every(k=>/^[1-9]\d*$/.test(String(ci[k])))||typeof ci.repository!=='string'||ci.repository.endsWith('-pg-validation'))fail();
 const read=async name=>{const path=join(layout,name),info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||info.size>1048576)fail();return readFile(path);};
 const manifest=await read('ci-producer.manifest'),bridgeBytes=await read('candidate-bridge.json'),verificationBytes=await read('candidate-verification.json');
 const produced=parseDigestManifest(manifest.toString(),{project:String(ci.project),pipeline:String(ci.pipeline),commit:revision,repository:ci.repository,gatewayProducer:String(ci.producer)},'gateway'),bridge=JSON.parse(bridgeBytes),verification=JSON.parse(verificationBytes);
 if(produced.parent!=='none'||produced.digest!==manifestDigest||bridge.id!==Number(ci.bridge)||bridge.name!=='verify_postgres_candidate'||bridge.status!=='success'||bridge.pipeline?.id!==Number(ci.pipeline)||bridge.downstream_pipeline?.id!==Number(ci.verificationPipeline)||bridge.downstream_pipeline?.sha!==revision||verification.id!==Number(ci.verificationJob)||verification.name!=='verify_postgres_candidate'||verification.status!=='success'||verification.commit?.id!==revision||verification.pipeline?.id!==Number(ci.verificationPipeline)||verification.pipeline?.project_id!==Number(ci.project))fail();
 const {inspectCapacityRuntimeLayout}=await import('./tools/capacity/runtime-artifact.mjs');const image=await inspectCapacityRuntimeLayout({layout,manifestDigest,revision});return {image,evidenceSha256:hash(Buffer.concat([manifest,bridgeBytes,verificationBytes]))};
}
// This is explicitly a same-UID task capability, never root trust or a formal
// deployment/OCI runtime proof. CI identities are pinned by the caller's already
// verified source, then checked against actual local OCI objects on each launch.
export async function createTaskRestoredCandidate({directory,layout,revision,manifestDigest,ci,signal}={}){
 signal?.throwIfAborted();const info=await lstat(directory);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid()||info.mode&0o077||await realpath(directory)!==directory||(await readdir(directory)).length)fail();
 layout=await realpath(layout);ci=structuredClone(ci);const binding={layout,revision,manifestDigest,ci},observed=await evidence(binding),entryDirectory=join(directory,'native'),harness=join(directory,'harness');await mkdir(entryDirectory,{mode:0o700});await mkdir(harness,{mode:0o700});await mkdir(join(harness,'tools'),{mode:0o700});await mkdir(join(harness,'tools','capacity'),{mode:0o700});const files={};
 for(const name of ['runtime-source-loader.mjs','restored-runtime-entry.mjs']){const bytes=await readFile(new URL('./tools/capacity/'+name,import.meta.url)),path='tools/capacity/'+name;files[path]=hash(bytes);await writeFile(join(harness,path),bytes,{mode:0o600,flag:'wx'});}
 const {buildCapacityRuntimeEntry}=await import('./tools/capacity/runtime-entry.mjs');await buildCapacityRuntimeEntry({directory:entryDirectory,signal});const token=Object.freeze({scope:'task-restored-oci-runtime',revision,manifestDigest});contexts.set(token,{binding,...observed,entryDirectory,harness,files});return token;
}
export function inspectTaskRestoredCandidate(token){const row=contexts.get(token);if(!row)fail();return Object.freeze({scope:'task-restored-oci-runtime',revision:row.binding.revision,manifestDigest:row.binding.manifestDigest,evidenceSha256:row.evidenceSha256,harnessSha256:hash(JSON.stringify(row.files)),formalQualified:false,deploymentAllowed:false});}
export async function startTaskRestoredCandidate(token,{runtime,role,directory,materials,signal}={}){
 const row=contexts.get(token);if(!row)fail();const current=await evidence(row.binding);if(current.evidenceSha256!==row.evidenceSha256||JSON.stringify(current.image)!==JSON.stringify(row.image))fail();for(const [path,sha] of Object.entries(row.files))if(hash(await readFile(join(row.harness,path)))!==sha)fail();await mkdir(directory,{mode:0o700});const owner=await lstat(directory,{bigint:true});
 const {startCapacityRuntimeSandbox}=await import('./tools/capacity/runtime-entry.mjs');return startCapacityRuntimeSandbox({entryDirectory:row.entryDirectory,runtime,role,signal,configuration:{schema:'capacity-runtime-launch-v1',entry:'restored-report',directory,owner:{device:String(owner.dev),inode:String(owner.ino)},tmpfsBytes:2*1024**3,budget:{maxBytes:1024**3,maxFiles:16384,timeoutSeconds:120},layout:row.image.layout,layers:row.image.layers,diffIds:row.image.diffIds,revision:row.image.revision,harness:{directory:row.harness,files:row.files},materials}});
}
