import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {createPrivateKey,createPublicKey,sign,verify} from 'node:crypto';
import {canonical,transferError} from './migration-artifacts.mjs';
import {validStorageReceiverRestorePayload} from './storage-receiver-attestation.mjs';
import {storageReceiverApplicationAuthority} from './storage-receiver.mjs';

const fail=code=>{throw transferError('backup_receiver_application_'+code);},domain='harbor-storage-receiver-application-attestation-v1\0';
const keys=(value,names)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...names].sort().join(',');
const digest=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value),uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value),oid=value=>typeof value==='string'&&/^[1-9]\d{0,9}$/.test(value);
const restoreFields=['kind','backupId','sourceMachine','receiverMachine','configurationDigest','clusterDigest','handoffDigest','checkpointDigest'];
const applicationFields=['runId','bindingDigest','targetDigest','database','roles','materialDigest','keyMaterialDigest','deviceCaDigest','deviceInventory','candidate','tlsHost'];
function valid(payload){
 if(!keys(payload,['version','purpose','restore','application'])||payload.version!==1||payload.purpose!=='harbor-storage-receiver-application-v1'||!validStorageReceiverRestorePayload(payload.restore))return false;
 const restore=payload.restore,a=payload.application;
 if(restore.scope!=='root-profile-receiver'||restore.kind!=='pending-final'||restore.sourceMachine===restore.receiverMachine||['sourceFreeRestoreVerified','receiverRestoreVerified','recoveryExitedVerified','inventoryVerified','writeReadbackVerified','offhostDatabaseVerified','offhostDatabaseAndFilesVerified'].some(key=>restore.checks[key]!==true)||restore.checks.deploymentAllowed!==false||!keys(a,applicationFields)||!uuid(a.runId)||!['bindingDigest','targetDigest','materialDigest','keyMaterialDigest','deviceCaDigest'].every(key=>digest(a[key])))return false;
 if(!keys(a.database,['name','oid'])||!/^harbor_rehearsal_[a-f0-9]{24}$/.test(a.database.name??'')||!oid(a.database.oid)||!keys(a.roles,['owner','runtime']))return false;
 for(const kind of ['owner','runtime'])if(!keys(a.roles[kind],['name','oid'])||!new RegExp('^harbor_'+kind+'_[a-f0-9]{24}$').test(a.roles[kind].name??'')||!oid(a.roles[kind].oid))return false;
 if(a.roles.owner.oid===a.roles.runtime.oid||!keys(a.candidate,['manifestDigest','configDigest','revision','schemaDigest','sourceDigest'])||!['manifestDigest','configDigest'].every(key=>/^sha256:[a-f0-9]{64}$/.test(a.candidate[key]??''))||!/^[a-f0-9]{40}$/.test(a.candidate.revision??'')||!['schemaDigest','sourceDigest'].every(key=>digest(a.candidate[key])))return false;
 if(!keys(a.deviceInventory,['count','sha256','mode'])||a.deviceInventory.count!==0||a.deviceInventory.mode!=='authenticated-empty'||!digest(a.deviceInventory.sha256)||typeof a.tlsHost!=='string'||a.tlsHost.length<1||a.tlsHost.length>253||/[\s/@?#\\]/.test(a.tlsHost))return false;
 return true;
}
const encoded=payload=>Buffer.from(domain+canonical(payload));

export async function attestStorageReceiverApplication(receipt,{signal}={}){
 signal?.throwIfAborted();const payload=await storageReceiverApplicationAuthority(receipt,{signal});if(!valid(payload))fail('binding');
 const file=await open('/etc/harbor-storage-receiver-signing.key',constants.O_RDONLY|constants.O_NOFOLLOW);let bytes,result;
 try{const info=await file.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.size<1||info.size>4096)fail('key_not_private');bytes=await file.readFile();signal?.throwIfAborted();const key=createPrivateKey(bytes);if(key.type!=='private'||key.asymmetricKeyType!=='ed25519')fail('key_invalid');if(canonical(await storageReceiverApplicationAuthority(receipt,{signal}))!==canonical(payload))fail('changed');signal?.throwIfAborted();result={payload,signature:sign(null,encoded(payload),key).toString('base64')};}finally{bytes?.fill(0);await file.close();}
 signal?.throwIfAborted();return result;
}

export function verifyStorageReceiverApplicationAttestation(attestation,publicKey,{expectedRestore,expectedApplication}={}){
 const payload=attestation?.payload;if(!keys(attestation,['payload','signature'])||!valid(payload))fail('binding');
 let key;try{key=publicKey?.type==='public'?publicKey:createPublicKey(publicKey);}catch{fail('key_invalid');}if(key.type!=='public'||key.asymmetricKeyType!=='ed25519')fail('key_invalid');
 if(typeof attestation.signature!=='string'||!/^[A-Za-z0-9+/]{86}==$/.test(attestation.signature)||!verify(null,encoded(payload),key,Buffer.from(attestation.signature,'base64')))fail('signature');
 if(!keys(expectedRestore,restoreFields)||restoreFields.some(name=>canonical(payload.restore[name])!==canonical(expectedRestore[name]))||!keys(expectedApplication,applicationFields)||canonical(expectedApplication)!==canonical(payload.application))fail('binding');
 return structuredClone(payload);
}
