import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createPrivateKey,createPublicKey,sign,verify} from 'node:crypto';
import {canonical,transferError} from './migration-artifacts.mjs';

const domain='harbor-storage-receiver-periodic-attestation-v1\0';
const fail=code=>{throw transferError('periodic_receiver_attestation_'+code);};
const exact=(value,fields)=>value!==null&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...fields].sort().join(',');
const hash=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value);
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value);
const digest=value=>exact(value,['bytes','sha256'])&&Number.isSafeInteger(value.bytes)&&value.bytes>0&&hash(value.sha256);
const bindingFields=['kind','backupId','pointId','sourceMachine','receiverMachine','configurationDigest','clusterDigest','handoffDigest','checkpointDigest','backupCompleteDigest','applicationPolicyDigest','applicationKeyVersion','applicationKeyDigest','runtime'];
const checkFields=['sourceFreeRestoreVerified','receiverRestoreVerified','recoveryExitedVerified','inventoryVerified','writeReadbackVerified','applicationMaterialsVerified','offhostDatabaseVerified','offhostDatabaseAndFilesVerified','deploymentAllowed'];
const requiredChecks=['sourceFreeRestoreVerified','receiverRestoreVerified','recoveryExitedVerified','inventoryVerified','writeReadbackVerified','applicationMaterialsVerified'];
const runtimeFields=['sourceDigest','nodeDigest','restoreUid','crossUidExecutionVerified','publisherUidVerified'];
const encoded=payload=>Buffer.from(domain+canonical(payload));

// This purpose proves only a selected periodic recovery point. It cannot be
// consumed by the pending-final, candidate OCI or deployment activation gates.
export function validStorageReceiverPeriodicPayload(value){
  if(!exact(value,['version','purpose','scope',...bindingFields,'resultDigest','checks'])||value.version!==1||value.purpose!=='harbor-storage-receiver-periodic-v1'||value.kind!=='periodic-application'||!['root-profile-receiver','task-owned-receiver'].includes(value.scope)||!uuid(value.backupId)||!uuid(value.pointId))return false;
  if(!['sourceMachine','receiverMachine','configurationDigest','clusterDigest','applicationPolicyDigest','applicationKeyDigest','resultDigest'].every(name=>hash(value[name]))||!['handoffDigest','checkpointDigest','backupCompleteDigest'].every(name=>digest(value[name]))||typeof value.applicationKeyVersion!=='string'||!/^[a-zA-Z0-9_.-]{1,64}$/.test(value.applicationKeyVersion))return false;
  const runtime=value.runtime,checks=value.checks;
  if(!exact(runtime,runtimeFields)||!Number.isSafeInteger(runtime.restoreUid)||runtime.restoreUid<0||runtime.restoreUid>4294967294||!['crossUidExecutionVerified','publisherUidVerified'].every(name=>typeof runtime[name]==='boolean')||!exact(checks,checkFields)||!checkFields.every(name=>typeof checks[name]==='boolean'))return false;
  if(value.scope==='root-profile-receiver'){
    if(!hash(runtime.sourceDigest)||!hash(runtime.nodeDigest)||runtime.restoreUid===0||runtime.crossUidExecutionVerified!==true||runtime.publisherUidVerified!==true)return false;
  }else if(runtime.sourceDigest!==null||runtime.nodeDigest!==null||runtime.crossUidExecutionVerified!==false||runtime.publisherUidVerified!==false)return false;
  if((value.scope!=='root-profile-receiver'||value.sourceMachine===value.receiverMachine)&&(checks.offhostDatabaseVerified||checks.offhostDatabaseAndFilesVerified))return false;
  return !checks.offhostDatabaseAndFilesVerified||(checks.offhostDatabaseVerified&&checks.applicationMaterialsVerified);
}

async function authority(receipt,signal){
  signal?.throwIfAborted();
  const {storageReceiverPeriodicAuthority}=await import('./storage-receiver.mjs');
  const payload=await storageReceiverPeriodicAuthority(receipt,{signal});
  signal?.throwIfAborted();
  if(!validStorageReceiverPeriodicPayload(payload))fail('binding');
  if(requiredChecks.some(name=>payload.checks[name]!==true)||payload.checks.deploymentAllowed!==false)fail('incomplete');
  return payload;
}
function signingKey(value){
  let key;try{key=value?.type==='private'?value:createPrivateKey(value);}catch{fail('key_invalid');}
  if(key.type!=='private'||key.asymmetricKeyType!=='ed25519')fail('key_invalid');
  return key;
}
function seal(payload,key,signal){signal?.throwIfAborted();return {payload,signature:sign(null,encoded(payload),key).toString('base64')};}

export async function attestStorageReceiverPeriodic(receipt,{signal}={}){
  const payload=await authority(receipt,signal);
  if(process.platform!=='linux'||process.getuid()!==0||payload.scope!=='root-profile-receiver')fail('formal_required');
  const file=await open('/etc/harbor-storage-receiver-signing.key',constants.O_RDONLY|constants.O_NOFOLLOW);let bytes;
  try{
    const before=await file.stat({bigint:true});
    if(!before.isFile()||before.uid!==0n||(before.mode&0o777n)!==0o600n||before.nlink!==1n||before.size<1n||before.size>4096n)fail('key_not_private');
    bytes=await file.readFile();signal?.throwIfAborted();
    const key=signingKey(bytes),after=await file.stat({bigint:true});
    if(['dev','ino','size','mtimeNs','ctimeNs','uid','mode','nlink'].some(name=>before[name]!==after[name]))fail('key_changed');
    if(canonical(await authority(receipt,signal))!==canonical(payload))fail('changed');
    return seal(payload,key,signal);
  }finally{bytes?.fill(0);await file.close();signal?.throwIfAborted();}
}

// A caller-supplied test signer still requires the real live task receipt.
export async function attestTaskStorageReceiverPeriodic(receipt,key,{signal}={}){
  const payload=await authority(receipt,signal);
  if(payload.scope!=='task-owned-receiver')fail('task_required');
  const privateKey=signingKey(key);
  if(canonical(await authority(receipt,signal))!==canonical(payload))fail('changed');
  return seal(payload,privateKey,signal);
}

export function verifyStorageReceiverPeriodicAttestation(attestation,publicKey,{expected}={}){
  if(!exact(attestation,['payload','signature'])||!validStorageReceiverPeriodicPayload(attestation.payload))fail('binding');
  let key;try{key=publicKey?.type==='public'?publicKey:createPublicKey(publicKey);}catch{fail('key_invalid');}
  if(key.type!=='public'||key.asymmetricKeyType!=='ed25519')fail('key_invalid');
  const {payload,signature}=attestation;
  if(typeof signature!=='string'||!/^[A-Za-z0-9+/]{86}==$/.test(signature)||!verify(null,encoded(payload),key,Buffer.from(signature,'base64')))fail('signature');
  if(!exact(expected,bindingFields)||bindingFields.some(name=>canonical(payload[name])!==canonical(expected[name])))fail('binding');
  if(requiredChecks.some(name=>payload.checks[name]!==true)||payload.checks.deploymentAllowed!==false)fail('incomplete');
  return structuredClone(payload);
}

export function verifyOffhostStorageReceiverPeriodicAttestation(attestation,publicKey,options){
  const payload=verifyStorageReceiverPeriodicAttestation(attestation,publicKey,options);
  if(payload.scope!=='root-profile-receiver'||payload.sourceMachine===payload.receiverMachine||payload.checks.offhostDatabaseVerified!==true||payload.checks.offhostDatabaseAndFilesVerified!==true)fail('offhost_required');
  return payload;
}
