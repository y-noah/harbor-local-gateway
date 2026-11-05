import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync,sign,randomUUID,createHmac} from 'node:crypto';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {canonical} from '../migration-artifacts.mjs';
import {attestStorageReceiverRestore,attestTaskStorageReceiverRestore,verifyStorageReceiverAttestation,verifyOffhostStorageReceiverAttestation} from '../storage-receiver-attestation.mjs';

// Synthetic cryptographic boundaries only; the separate actual source-free PG
// test consumes a real live receipt and cannot mint root/off-host authority.
const keys=generateKeyPairSync('ed25519'),other=generateKeyPairSync('ed25519'),digest={bytes:120,sha256:'a'.repeat(64)},fields=['kind','backupId','sourceMachine','receiverMachine','configurationDigest','clusterDigest','handoffDigest','checkpointDigest'];
function fixture(){const payload={version:1,purpose:'harbor-storage-receiver-result-v1',scope:'root-profile-receiver',kind:'cluster-baseline',backupId:randomUUID(),sourceMachine:'1'.repeat(64),receiverMachine:'2'.repeat(64),configurationDigest:'3'.repeat(64),clusterDigest:'4'.repeat(64),handoffDigest:{...digest},checkpointDigest:{...digest},resultDigest:'5'.repeat(64),checks:{sourceFreeRestoreVerified:true,receiverRestoreVerified:true,recoveryExitedVerified:true,inventoryVerified:true,writeReadbackVerified:true,offhostDatabaseVerified:true,offhostDatabaseAndFilesVerified:false,applicationMaterialsVerified:false,deploymentAllowed:false}};return {payload,expected:Object.fromEntries(fields.map(name=>[name,structuredClone(payload[name])]))};}
const seal=payload=>({payload,signature:sign(null,Buffer.from('harbor-storage-receiver-attestation-v1\0'+canonical(payload)),keys.privateKey).toString('base64')});
test('receiver signatures bind approved package, stage and independent host identity without granting deployment',()=>{
 const {payload,expected}=fixture(),attestation=seal(payload),verified=verifyOffhostStorageReceiverAttestation(attestation,keys.publicKey,{expected});assert.equal(verified.checks.deploymentAllowed,false);assert.equal(verified.checks.applicationMaterialsVerified,false);verified.kind='pending-final';assert.equal(attestation.payload.kind,'cluster-baseline');
 assert.throws(()=>verifyStorageReceiverAttestation(attestation,other.publicKey,{expected}),{code:'backup_receiver_attestation_signature'});for(const field of fields){const changed=structuredClone(expected);changed[field]=field.endsWith('Digest')&&typeof changed[field]==='object'?{...digest,bytes:121}:field==='kind'?'pending-final':field==='backupId'?randomUUID():'b'.repeat(64);assert.throws(()=>verifyStorageReceiverAttestation(attestation,keys.publicKey,{expected:changed}),{code:'backup_receiver_attestation_binding'},field);}
 const modified=structuredClone(attestation);modified.payload.receiverMachine='9'.repeat(64);assert.throws(()=>verifyStorageReceiverAttestation(modified,keys.publicKey,{expected}),{code:'backup_receiver_attestation_signature'});
 const sharedArchiveMac=createHmac('sha256',Buffer.alloc(32,1)).update(canonical(payload)).digest('base64');assert.throws(()=>verifyStorageReceiverAttestation({payload,signature:sharedArchiveMac},keys.publicKey,{expected}),{code:'backup_receiver_attestation_signature'});
});
test('a valid signature cannot turn task/same-machine/incomplete restore into an offhost or final application gate',()=>{
 for(const kind of ['task','same-host','missing-db','missing-files','incomplete','activation']){const {payload,expected}=fixture();if(kind==='task')payload.scope='task-owned-receiver';if(kind==='same-host'){payload.receiverMachine=payload.sourceMachine;expected.receiverMachine=payload.receiverMachine;}if(kind==='missing-db')payload.checks.offhostDatabaseVerified=false;if(kind==='missing-files'){payload.kind='pending-final';expected.kind='pending-final';}if(kind==='incomplete')payload.checks.recoveryExitedVerified=false;if(kind==='activation')payload.checks.deploymentAllowed=true;assert.throws(()=>verifyOffhostStorageReceiverAttestation(seal(payload),keys.publicKey,{expected}),{code:['incomplete','activation'].includes(kind)?'backup_receiver_attestation_incomplete':'backup_receiver_offhost_attestation_required'},kind);}
});
test('malformed and oversized claims are rejected before signing-domain encoding and DTO copies cannot request signatures',async()=>{
 const {payload,expected}=fixture();for(const change of [{extra:'x'.repeat(100000)},{kind:'unknown'},{checks:{...payload.checks,unexpected:true}},{handoffDigest:{...digest,bytes:-1}},{sourceMachine:null}])assert.throws(()=>verifyStorageReceiverAttestation({payload:{...payload,...change},signature:'x'},keys.publicKey,{expected}),{code:'backup_receiver_attestation_binding'});
 await assert.rejects(attestStorageReceiverRestore({passed:true}),{code:'backup_receiver_restore_receipt_required'});await assert.rejects(attestTaskStorageReceiverRestore({passed:true},keys.privateKey),{code:'backup_receiver_restore_receipt_required'});
});
test('actual signing and CLI modules refuse success when cancellation arrives during key read or asynchronous cleanup',{timeout:15000},async()=>{
 const {stdout}=await promisify(execFile)(process.execPath,['--experimental-vm-modules',new URL('./receiver-attestation-lifecycle-fixture.mjs',import.meta.url).pathname],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(stdout),{keyReadCancellation:true,keyCloseCancellation:true,keyCleared:true,closeFailurePreserved:true,receiverCleanupCancellation:true,defaultRestorePreserved:true});
});

test('actual receiver CLI finishes module evaluation before its dynamic signer imports the receiver authority',async()=>{
 const result=await promisify(execFile)(process.execPath,['--import='+new URL('./receiver-attestation-lifecycle-fixture.mjs',import.meta.url).pathname,new URL('../storage-receiver.mjs',import.meta.url).pathname,'restore-and-attest'],{timeout:10000,maxBuffer:65536}).then(value=>({success:true,...value}),error=>error);
 assert.equal(result.code,1,'task receiver must reach the real signer and fail its existing formal authority check');
 assert.equal(result.stdout,'');assert.deepEqual(JSON.parse(result.stderr),{error:'backup_receiver_formal_attestation_required',deploymentAllowed:false});
});
