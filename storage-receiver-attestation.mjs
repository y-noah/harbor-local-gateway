import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createPrivateKey,createPublicKey,sign,verify} from 'node:crypto';
import {canonical,transferError} from './migration-artifacts.mjs';
import {storageReceiverRestoreAuthority} from './storage-receiver.mjs';

const domain='harbor-storage-receiver-attestation-v1\0',fail=code=>{throw transferError(code);};
const encoded=payload=>Buffer.from(domain+canonical(payload));
const same=(a,b)=>canonical(a)===canonical(b);
const digest=value=>value&&Object.keys(value).sort().join(',')==='bytes,sha256'&&Number.isSafeInteger(value.bytes)&&value.bytes>0&&/^[a-f0-9]{64}$/.test(value.sha256??'');
const bindingFields=['kind','backupId','sourceMachine','receiverMachine','configurationDigest','clusterDigest','handoffDigest','checkpointDigest'];
const checkFields=['sourceFreeRestoreVerified','receiverRestoreVerified','recoveryExitedVerified','inventoryVerified','writeReadbackVerified','offhostDatabaseVerified','offhostDatabaseAndFilesVerified','applicationMaterialsVerified','deploymentAllowed'];
function validPayload(value){return value&&Object.keys(value).sort().join(',')===['version','purpose','scope',...bindingFields,'resultDigest','checks'].sort().join(',')&&value.version===1&&value.purpose==='harbor-storage-receiver-result-v1'&&['cluster-baseline','pending-final'].includes(value.kind)&&['root-profile-receiver','task-owned-receiver'].includes(value.scope)&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value.backupId??'')&&['sourceMachine','receiverMachine','configurationDigest','clusterDigest','resultDigest'].every(name=>typeof value[name]==='string'&&/^[a-f0-9]{64}$/.test(value[name]))&&digest(value.handoffDigest)&&digest(value.checkpointDigest)&&value.checks&&Object.keys(value.checks).sort().join(',')===[...checkFields].sort().join(',')&&checkFields.every(name=>typeof value.checks[name]==='boolean');}
export {validPayload as validStorageReceiverRestorePayload};
function signingKey(value){let key;try{key=value?.type==='private'?value:createPrivateKey(value);}catch{fail('backup_receiver_signing_key_invalid');}if(key.type!=='private'||key.asymmetricKeyType!=='ed25519')fail('backup_receiver_signing_key_invalid');return key;}
function seal(payload,key,signal){signal?.throwIfAborted();if(!validPayload(payload))fail('backup_receiver_attestation_binding');return {payload,signature:sign(null,encoded(payload),key).toString('base64')};}

// Root installs this independent signing key on the recovery host only. The
// source's shared archive-encryption key cannot mint a receiver attestation.
export async function attestStorageReceiverRestore(receipt,{signal}={}){
 signal?.throwIfAborted();const payload=await storageReceiverRestoreAuthority(receipt,{signal});signal?.throwIfAborted();if(process.platform!=='linux'||process.getuid()!==0||payload.scope!=='root-profile-receiver')fail('backup_receiver_formal_attestation_required');
 const handle=await open('/etc/harbor-storage-receiver-signing.key',constants.O_RDONLY|constants.O_NOFOLLOW);let bytes,attestation;
 try{const info=await handle.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.size<1||info.size>4096)fail('backup_receiver_signing_key_not_private');bytes=await handle.readFile();signal?.throwIfAborted();const key=signingKey(bytes),current=await storageReceiverRestoreAuthority(receipt,{signal});if(!same(current,payload))fail('backup_receiver_attestation_binding');attestation=seal(payload,key,signal);}finally{bytes?.fill(0);await handle.close();}
 signal?.throwIfAborted();return attestation;
}
// Tests still consume the real live restore capability. Neither a passed key
// nor a copied result can promote task scope into formal receiver authority.
export async function attestTaskStorageReceiverRestore(receipt,key,{signal}={}){signal?.throwIfAborted();const payload=await storageReceiverRestoreAuthority(receipt,{signal});if(payload.scope!=='task-owned-receiver')fail('backup_receiver_task_attestation_required');return seal(payload,signingKey(key),signal);}

export function verifyStorageReceiverAttestation(attestation,publicKey,{expected}={}){
 let key;try{key=publicKey?.type==='public'?publicKey:createPublicKey(publicKey);}catch{fail('backup_receiver_verification_key_invalid');}if(key.type!=='public'||key.asymmetricKeyType!=='ed25519')fail('backup_receiver_verification_key_invalid');
 const payload=attestation?.payload,signature=attestation?.signature;
 if(!validPayload(payload))fail('backup_receiver_attestation_binding');
 if(typeof signature!=='string'||!/^[A-Za-z0-9+/]{86}==$/.test(signature)||!verify(null,encoded(payload),key,Buffer.from(signature,'base64')))fail('backup_receiver_attestation_signature');
 if(!expected||typeof expected!=='object'||Array.isArray(expected)||Object.keys(expected).sort().join(',')!==[...bindingFields].sort().join(',')||bindingFields.some(name=>!same(payload[name],expected[name])))fail('backup_receiver_attestation_binding');
 if(!/^[a-f0-9]{64}$/.test(payload.resultDigest??'')||['sourceFreeRestoreVerified','receiverRestoreVerified','recoveryExitedVerified','inventoryVerified','writeReadbackVerified'].some(name=>payload.checks?.[name]!==true)||payload.checks.deploymentAllowed!==false)fail('backup_receiver_attestation_incomplete');return structuredClone(payload);
}
// This stricter consumer is the only one suitable for an operator off-host gate.
// Signature validity alone never grants activation or final application proof.
export function verifyOffhostStorageReceiverAttestation(attestation,publicKey,options){const payload=verifyStorageReceiverAttestation(attestation,publicKey,options);if(payload.scope!=='root-profile-receiver'||payload.sourceMachine===payload.receiverMachine||payload.checks.offhostDatabaseVerified!==true||payload.kind==='pending-final'&&payload.checks.offhostDatabaseAndFilesVerified!==true)fail('backup_receiver_offhost_attestation_required');return payload;}
