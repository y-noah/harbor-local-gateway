import {createHash,createPublicKey,createPrivateKey} from 'node:crypto';
import {lstat,realpath} from 'node:fs/promises';
import {isAbsolute} from 'node:path';
import {capacityRuntimeArtifactBinding,inspectCapacityRuntimeArtifact,readCapacityRuntimeRegular} from './runtime-artifact.mjs';
import {remoteTlsMaterials,remoteId} from './remote-protocol.mjs';
import {readCapacityRemoteCertificateMaterials} from './remote-certificate-materials.mjs';

const approvals=new WeakMap(),fail=code=>{throw Error('capacity_remote_root_'+code);},hash=value=>createHash('sha256').update(value).digest('hex');
const keys=(value,allowed)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.includes(key));
const sha=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const freeze=value=>{if(value&&typeof value==='object'){for(const next of Object.values(value))freeze(next);Object.freeze(value);}return value;};
export function capacityRemoteRootApprovalBinding(handle){return approvals.get(handle)??null;}
async function pinnedFile(item,max,privateKey=false){if(!keys(item,['file','sha256'])||!sha(item.sha256))fail('material');const value=await readCapacityRuntimeRegular(item.file,max,{trusted:true});if(hash(value)!==item.sha256)fail('material_changed');if(privateKey){const info=await lstat(item.file);if(info.mode&0o077)fail('signer_permissions');}return value;}
function https(value){let url;try{url=new URL(value);}catch{fail('origin');}if(url.protocol!=='https:'||url.origin!==value||url.username||url.password)fail('origin');return value;}
async function tlsConfiguration(plan){if(!keys(plan,['caFile','certFile','keyFile','peerCertificateHash','caSha256','certSha256','keySha256'])||!['caSha256','certSha256','keySha256','peerCertificateHash'].every(key=>sha(plan[key])))fail('tls');const tls=Object.fromEntries(['caFile','certFile','keyFile','peerCertificateHash'].map(key=>[key,plan[key]])),actual=await remoteTlsMaterials(tls);for(const key of ['ca','cert','key'])if(hash(actual[key])!==plan[key+'Sha256'])fail('tls_changed');return tls;}

// The only configuration source is the existing fixed, root-owned runtime
// artifact. Neither environment variables nor a caller DTO can mint approval.
export async function loadCapacityRemoteRootApproval({artifact,side}={}){
 if(!['controller','broker'].includes(side))fail('role');await inspectCapacityRuntimeArtifact(artifact);const artifactRow=capacityRuntimeArtifactBinding(artifact),remote=artifactRow.trust.remote;
 if(!keys(remote,['schema','controller','broker'])||remote.schema!=='capacity-remote-root-v1')fail('unavailable');const plan=remote[side];if(!plan||!remoteId(plan.approvalId)||!sha(plan.machineHash)||!sha(plan.bootHash)||!sha(plan.serverCertificateHash)||!sha(plan.caSha256))fail('configuration');https(plan.publicOrigin);
 const tls=await tlsConfiguration(plan.tls);let signingKey,publicKey,tools,certificateMaterialsHash=null;
 if(side==='controller'){
  if(!keys(plan,['approvalId','controlOrigin','dataOrigin','tls','publicKey','machineHash','bootHash','publicOrigin','proxyAddress','caSha256','serverCertificateHash','agentSlots','browserRequired','closeTimeoutMs']))fail('configuration');let address;try{address=new URL(plan.controlOrigin);}catch{fail('origin');}const dataAddress=new URL(plan.dataOrigin);if(dataAddress.protocol!=='tls:'||!dataAddress.hostname||!dataAddress.port||dataAddress.username||dataAddress.password||dataAddress.pathname||dataAddress.search||dataAddress.hash)fail('origin');if(address.protocol!=='tls:'||!address.hostname||!address.port||address.username||address.password||address.pathname||address.search||address.hash)fail('origin');
  publicKey=createPublicKey(await pinnedFile(plan.publicKey,65536));if(publicKey.asymmetricKeyType!=='ed25519')fail('signer_type');if(!Number.isSafeInteger(plan.closeTimeoutMs)||plan.closeTimeoutMs<1000||plan.closeTimeoutMs>60000||typeof plan.browserRequired!=='boolean'||!Array.isArray(plan.agentSlots)||!plan.agentSlots.length||plan.agentSlots.length>256||plan.agentSlots.some(id=>!remoteId(id))||new Set(plan.agentSlots).size!==plan.agentSlots.length)fail('slots');
 }else{
  if(!keys(plan,['approvalId','listen','controlListen','tls','signingKey','machineHash','bootHash','publicOrigin','proxyAddress','caSha256','serverCertificateHash','agents','browser','remoteTools','certificateDirectory','runtimeDirectory','shutdownMs','limits','frames']))fail('configuration');if(!keys(plan.controlListen,['host','port'])||typeof plan.controlListen.host!=='string'||!Number.isInteger(plan.controlListen.port)||plan.controlListen.port<1||plan.controlListen.port>65535||!keys(plan.listen,['host','port'])||typeof plan.listen.host!=='string'||!Number.isInteger(plan.listen.port)||plan.listen.port<1||plan.listen.port>65535||!Number.isSafeInteger(plan.shutdownMs)||plan.shutdownMs<1000||plan.shutdownMs>30000)fail('listen');
  signingKey=createPrivateKey(await pinnedFile(plan.signingKey,65536,true));if(signingKey.asymmetricKeyType!=='ed25519')fail('signer_type');publicKey=createPublicKey(signingKey);
  for(const directory of [plan.runtimeDirectory,plan.certificateDirectory]){if(typeof directory!=='string'||!isAbsolute(directory)||await realpath(directory)!==directory)fail('directory');const info=await lstat(directory);if(!info.isDirectory()||info.uid!==0||info.mode&0o077)fail('directory');}
  if(plan.signingKey.file===plan.certificateDirectory||plan.signingKey.file.startsWith(plan.certificateDirectory+'/'))fail('signer_boundary');
  if(plan.remoteTools){tools=JSON.parse(await pinnedFile(plan.remoteTools,4*1024**2));if(tools.schema!=='capacity-remote-tools-v1'||!Array.isArray(tools.files)||!tools.files.length||tools.files.some(row=>row.sha256===plan.signingKey.sha256||row.sha256===plan.tls.keySha256))fail('tools');}
  if(!Array.isArray(plan.agents)||!plan.agents.length||plan.agents.length>256||plan.agents.some(row=>!remoteId(row.id)||row.origin!==plan.publicOrigin||!Array.isArray(row.routes)||!row.routes.length))fail('slots');
  const materials=await readCapacityRemoteCertificateMaterials(plan,signingKey,{maxBytes:artifactRow.trust.runtime?.maxBytes,maxFiles:artifactRow.trust.runtime?.maxFiles});certificateMaterialsHash=hash(JSON.stringify(materials.map(row=>[row.name,row.sha256,row.privateMaterial]).sort()));
 }
 await inspectCapacityRuntimeArtifact(artifact);const handle=Object.freeze(Object.create(null));approvals.set(handle,Object.freeze({artifact,side,plan:freeze(structuredClone(plan)),tls:freeze(tls),publicKey,signingKey,certificateMaterialsHash,tools:tools?freeze(tools):null,artifactHash:artifactRow.trustHash}));return handle;
}
export async function inspectCapacityRemoteRootApproval(handle){const row=approvals.get(handle);if(!row)fail('not_owned');await inspectCapacityRuntimeArtifact(row.artifact);const refreshed=capacityRemoteRootApprovalBinding(await loadCapacityRemoteRootApproval({artifact:row.artifact,side:row.side}));if(refreshed.artifactHash!==row.artifactHash||refreshed.certificateMaterialsHash!==row.certificateMaterialsHash||JSON.stringify(refreshed.plan)!==JSON.stringify(row.plan)||!refreshed.publicKey.equals(row.publicKey))fail('changed');return row;}
export function capacityRemoteTranscriptBytes(value){const order=item=>Array.isArray(item)?item.map(order):item&&typeof item==='object'?Object.fromEntries(Object.keys(item).sort().map(key=>[key,order(item[key])])):item;return Buffer.from(JSON.stringify(order(value)));}
