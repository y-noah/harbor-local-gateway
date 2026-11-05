import {open,lstat,realpath} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join,isAbsolute,dirname} from 'node:path';
import {createHash} from 'node:crypto';
import {parseDigestManifest} from '../../ci/postgres-candidate-pipeline.mjs';

export const capacityRuntimeHarnessPath=path=>typeof path==='string'&&(/^tools\/capacity\/[a-z0-9-]+\.(mjs|py|c)$/.test(path)||['clients/harbor-device/HarborDevice.cs','clients/harbor-device/CapacityDeliveryObservation.cs'].includes(path));
const artifacts=new WeakMap(),fixedTrustFile='/etc/harbor-capacity/runtime.json';
const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};
const hash=value=>createHash('sha256').update(value).digest('hex');
const fail=code=>{throw Error(code);};
const digest=value=>typeof value==='string'&&/^sha256:[a-f0-9]{64}$/.test(value);
const regularInfo=info=>info.isFile()&&!info.isSymbolicLink();
const same=(a,b)=>a.dev===b.dev&&a.ino===b.ino&&a.size===b.size&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;
async function pathParents(path,{trusted=false}={}){
 if(typeof path!=='string'||!isAbsolute(path)||await realpath(path)!==path)fail('capacity_runtime_artifact_path');
 for(let at=dirname(path);;at=dirname(at)){const info=await lstat(at);if(!info.isDirectory()||info.isSymbolicLink()||trusted&&(info.uid!==0||info.mode&0o022))fail('capacity_runtime_artifact_parent');if(at==='/')break;}
}
async function bytes(path,maxBytes,{trusted=false,signal}={}){
 signal?.throwIfAborted();if(!Number.isSafeInteger(maxBytes)||maxBytes<0||maxBytes>128*1024**2)fail('capacity_runtime_artifact_budget');
 await pathParents(path,{trusted});const before=await lstat(path,{bigint:true});if(!regularInfo(before)||before.size>BigInt(maxBytes)||trusted&&(before.uid!==0n||before.mode&0o022n))fail('capacity_runtime_artifact_file');
 const fd=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{const actual=await fd.stat({bigint:true});if(!actual.isFile()||!same(actual,before))fail('capacity_runtime_artifact_changed');const chunks=[];let total=0;for(;;){signal?.throwIfAborted();const buffer=Buffer.alloc(Math.min(65536,maxBytes+1-total)),{bytesRead}=await fd.read(buffer,0,buffer.length,null);signal?.throwIfAborted();if(!bytesRead)break;total+=bytesRead;if(total>maxBytes)fail('capacity_runtime_artifact_budget');chunks.push(buffer.subarray(0,bytesRead));}const content=Buffer.concat(chunks,total);if(content.length>maxBytes||!same(await fd.stat({bigint:true}),before)||!same(await lstat(path,{bigint:true}),before))fail('capacity_runtime_artifact_changed');return content;}finally{await fd.close();}
}
async function json(path,maxBytes,options){try{return JSON.parse(await bytes(path,maxBytes,options));}catch(error){if(/^capacity_runtime_/.test(error.message))throw error;fail('capacity_runtime_artifact_json');}}
async function blob(layout,descriptor,maxBytes){
 if(!descriptor||!digest(descriptor.digest)||!Number.isSafeInteger(descriptor.size)||descriptor.size<1||descriptor.size>maxBytes)fail('capacity_runtime_oci_descriptor');const content=await bytes(join(layout,'blobs','sha256',descriptor.digest.slice(7)),maxBytes);if(content.length!==descriptor.size||'sha256:'+hash(content)!==descriptor.digest)fail('capacity_runtime_oci_metadata_digest');try{return JSON.parse(content);}catch{fail('capacity_runtime_oci_metadata_json');}
}
function layers(config,manifest){
 if(!Array.isArray(manifest.layers)||manifest.layers.length<1||manifest.layers.length>256||config.rootfs?.type!=='layers'||!Array.isArray(config.rootfs.diff_ids)||config.rootfs.diff_ids.length!==manifest.layers.length||config.rootfs.diff_ids.some(value=>!digest(value)))fail('capacity_runtime_oci_layers');
 for(const row of manifest.layers)if(!digest(row.digest)||!Number.isSafeInteger(row.size)||row.size<1||row.size>16*1024**3||!['application/vnd.oci.image.layer.v1.tar','application/vnd.oci.image.layer.v1.tar+gzip','application/vnd.docker.image.rootfs.diff.tar.gzip'].includes(row.mediaType))fail('capacity_runtime_oci_layer_descriptor');
}

// Structural inspection is intentionally public and does not mint a trusted B
// artifact. In particular, a generated test layout or workspace manifest never
// acquires the private capability needed by a formal launcher.
export async function inspectCapacityRuntimeLayout({layout,manifestDigest,revision}={}){
 if(typeof layout!=='string'||!isAbsolute(layout)||!digest(manifestDigest)||!/^[a-f0-9]{40}$/.test(revision??''))fail('capacity_runtime_oci_configuration');
 if((await json(join(layout,'oci-layout'),1024)).imageLayoutVersion!=='1.0.0')fail('capacity_runtime_oci_layout');
 const index=await json(join(layout,'index.json'),1048576);if(index.schemaVersion!==2||!Array.isArray(index.manifests)||index.manifests.length>32)fail('capacity_runtime_oci_index');
 const matches=index.manifests.filter(row=>row.digest===manifestDigest);if(matches.length!==1)fail('capacity_runtime_oci_index_binding');const manifest=await blob(layout,matches[0],4194304);
 if(manifest.schemaVersion!==2||!['application/vnd.oci.image.manifest.v1+json','application/vnd.docker.distribution.manifest.v2+json'].includes(manifest.mediaType))fail('capacity_runtime_oci_manifest');
 if(!['application/vnd.oci.image.config.v1+json','application/vnd.docker.container.image.v1+json'].includes(manifest.config?.mediaType))fail('capacity_runtime_oci_config');const config=await blob(layout,manifest.config,4194304);
 if(config.os!=='linux'||config.architecture!=='amd64'||config.config?.Labels?.['org.opencontainers.image.revision']!==revision||config.config?.Labels?.['com.harbor.storage.postgres']!=='postgres-conversation-v2')fail('capacity_runtime_oci_business_identity');layers(config,manifest);
 return Object.freeze({scope:'oci-business-layout-metadata-only',formalQualified:false,layout,manifestDigest,revision,configDigest:manifest.config.digest,layers:Object.freeze(manifest.layers.map(row=>Object.freeze({...row}))),diffIds:Object.freeze([...config.rootfs.diff_ids]),layerBytes:manifest.layers.reduce((n,row)=>n+row.size,0)});
}

// This validates already authenticated inputs; it never mints an artifact.
export function validateCapacityRuntimeToolBinding(tools,manifest){
 if(!tools||!manifest||manifest.schema!=='capacity-runtime-entry-v1'||manifest.binarySha256!==tools.launcher?.sha256||manifest.python!==tools.python?.file||manifest.pythonSha256!==tools.python?.sha256||manifest.helper!==tools.extractor?.file||manifest.helperSha256!==tools.extractor?.sha256||!/^[a-f0-9]{64}$/.test(manifest.nativeSha256??''))fail('capacity_runtime_tool_build_binding');
 return true;
}

// Operator trust is a fixed root-owned input, not a CLI/environment override.
// It pins the actual CI B producer manifest and successful qualification job;
// a V digest, tag, local workspace hash or self-reported source JSON cannot bind.
export async function loadCapacityTrustedRuntimeArtifact(){
 let trust,trustBytes;try{trustBytes=await bytes(fixedTrustFile,65536,{trusted:true});trust=JSON.parse(trustBytes);}catch(error){if(error.code==='ENOENT')fail('capacity_runtime_root_trust_unavailable');throw error;}
 if(trust.schema!=='capacity-runtime-trust-v1'||!trust.business||!trust.harness||!trust.tools)fail('capacity_runtime_trust_schema');
 const business=trust.business,ci=business.ci;if(!ci||!['project','pipeline','buildJob','bridgeJob','verificationPipeline','verificationJob'].every(key=>/^[1-9]\d*$/.test(String(ci[key])))||!digest(business.manifestDigest)||!/^[a-f0-9]{40}$/.test(business.revision??'')||!/^[a-f0-9]{64}$/.test(ci.manifestSha256??'')||!/^[a-f0-9]{64}$/.test(ci.verificationSha256??''))fail('capacity_runtime_ci_binding');
 const producerBytes=await bytes(ci.manifestFile,4096,{trusted:true});if(hash(producerBytes)!==ci.manifestSha256)fail('capacity_runtime_ci_manifest_digest');
 const produced=parseDigestManifest(producerBytes.toString('utf8'),{project:String(ci.project),pipeline:String(ci.pipeline),commit:business.revision,repository:ci.repository,gatewayProducer:String(ci.buildJob)},'gateway');
 if(produced.parent!=='none'||produced.digest!==business.manifestDigest||ci.repository?.endsWith('-pg-validation'))fail('capacity_runtime_ci_business_identity');
 const bridgeBytes=await bytes(ci.bridgeFile,1048576,{trusted:true});if(!/^[a-f0-9]{64}$/.test(ci.bridgeSha256??'')||hash(bridgeBytes)!==ci.bridgeSha256)fail('capacity_runtime_ci_bridge_digest');let bridge;try{bridge=JSON.parse(bridgeBytes);}catch{fail('capacity_runtime_ci_bridge');}if(bridge.id!==Number(ci.bridgeJob)||bridge.name!=='verify_postgres_candidate'||bridge.status!=='success'||bridge.pipeline?.id!==Number(ci.pipeline)||bridge.downstream_pipeline?.id!==Number(ci.verificationPipeline)||bridge.downstream_pipeline?.sha!==business.revision)fail('capacity_runtime_ci_bridge');
 const verifiedBytes=await bytes(ci.verificationFile,1048576,{trusted:true});if(hash(verifiedBytes)!==ci.verificationSha256)fail('capacity_runtime_ci_verification_digest');let verified;try{verified=JSON.parse(verifiedBytes);}catch{fail('capacity_runtime_ci_verification');}
 if(verified.id!==Number(ci.verificationJob)||verified.status!=='success'||verified.name!=='verify_postgres_candidate'||verified.commit?.id!==business.revision||verified.pipeline?.id!==Number(ci.verificationPipeline)||verified.pipeline?.project_id!==Number(ci.project))fail('capacity_runtime_ci_verification');
 const observed=await inspectCapacityRuntimeLayout({layout:business.layout,manifestDigest:business.manifestDigest,revision:business.revision});
 for(const name of ['launcher','python','extractor']){const item=trust.tools[name];if(!item||!/^[a-f0-9]{64}$/.test(item.sha256??'')||hash(await bytes(item.file,128*1024**2,{trusted:true}))!==item.sha256)fail('capacity_runtime_trusted_tool');}
 const launcher=trust.tools.launcher;if(!/^[a-f0-9]{64}$/.test(launcher.manifestSha256??''))fail('capacity_runtime_tool_build_binding');const buildBytes=await bytes(launcher.manifestFile,65536,{trusted:true});if(hash(buildBytes)!==launcher.manifestSha256)fail('capacity_runtime_tool_build_binding');let build;try{build=JSON.parse(buildBytes);}catch{fail('capacity_runtime_tool_build_binding');}validateCapacityRuntimeToolBinding(trust.tools,build);
 if(typeof trust.harness.directory!=='string'||!isAbsolute(trust.harness.directory)||!/^[a-f0-9]{64}$/.test(trust.harness.manifestSha256??''))fail('capacity_runtime_harness_identity');const harnessBytes=await bytes(trust.harness.manifestFile,4194304,{trusted:true});if(hash(harnessBytes)!==trust.harness.manifestSha256)fail('capacity_runtime_harness_identity');let harness;try{harness=JSON.parse(harnessBytes);}catch{fail('capacity_runtime_harness_identity');}if(harness.schema!=='capacity-runtime-harness-v1'||!harness.files||Object.entries(harness.files).some(([path,digest])=>!capacityRuntimeHarnessPath(path)||!/^[a-f0-9]{64}$/.test(digest)))fail('capacity_runtime_harness_identity');
 if(trust.tools.windowsHelper){const helper=trust.tools.windowsHelper,names=['clients/harbor-device/HarborDevice.cs','clients/harbor-device/CapacityDeliveryObservation.cs'];if(!helper.sourceHashes||Object.keys(helper.sourceHashes).length!==2||names.some(name=>!harness.files[name]||helper.sourceHashes[name]!==harness.files[name])||!/^[a-f0-9]{64}$/.test(helper.sha256??'')||hash(await bytes(helper.file,16*1024**2,{trusted:true}))!==helper.sha256)fail('capacity_runtime_windows_helper_identity');}
 if(hash(await bytes(fixedTrustFile,65536,{trusted:true}))!==hash(trustBytes))fail('capacity_runtime_trust_changed');const handle=Object.freeze(Object.create(null));artifacts.set(handle,Object.freeze({trust:freeze(trust),harness:freeze(harness),observed,trustHash:hash(trustBytes)}));return handle;
}
export function capacityRuntimeArtifactBinding(handle){const record=artifacts.get(handle);if(!record)fail('capacity_runtime_artifact_not_owned');return record;}

export async function inspectCapacityRuntimeArtifact(handle){const record=capacityRuntimeArtifactBinding(handle);if(hash(await bytes(fixedTrustFile,65536,{trusted:true}))!==record.trustHash)fail('capacity_runtime_trust_changed');const refreshed=capacityRuntimeArtifactBinding(await loadCapacityTrustedRuntimeArtifact());if(refreshed.trustHash!==record.trustHash)fail('capacity_runtime_trust_changed');return record.observed;}

export const readCapacityRuntimeRegular=(path,maxBytes,options)=>bytes(path,maxBytes,options);
