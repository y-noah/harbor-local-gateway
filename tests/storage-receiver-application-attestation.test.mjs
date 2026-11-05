import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync,sign,randomUUID,createHash} from 'node:crypto';
import {canonical} from '../migration-artifacts.mjs';
import {verifyStorageReceiverApplicationAttestation,attestStorageReceiverApplication} from '../storage-receiver-application-attestation.mjs';
import {storageReceiverApplicationAuthority} from '../storage-receiver.mjs';
import {restoredApplicationBinding} from '../storage-restored-application-binding.mjs';

const keys=generateKeyPairSync('ed25519'),other=generateKeyPairSync('ed25519'),hash=value=>createHash('sha256').update(canonical(value)).digest('hex'),nonce='a'.repeat(24),d={bytes:128,sha256:'a'.repeat(64)};
function fixture(){
 const restore={version:1,purpose:'harbor-storage-receiver-result-v1',scope:'root-profile-receiver',kind:'pending-final',backupId:randomUUID(),sourceMachine:'1'.repeat(64),receiverMachine:'2'.repeat(64),configurationDigest:'3'.repeat(64),clusterDigest:'4'.repeat(64),handoffDigest:{...d},checkpointDigest:{...d},resultDigest:'5'.repeat(64),checks:Object.fromEntries(['sourceFreeRestoreVerified','receiverRestoreVerified','recoveryExitedVerified','inventoryVerified','writeReadbackVerified','offhostDatabaseVerified','offhostDatabaseAndFilesVerified','applicationMaterialsVerified','deploymentAllowed'].map(key=>[key,!['applicationMaterialsVerified','deploymentAllowed'].includes(key)]))};
 const application={runId:randomUUID(),bindingDigest:'6'.repeat(64),targetDigest:'7'.repeat(64),database:{name:'harbor_rehearsal_'+nonce,oid:'16400'},roles:{owner:{name:'harbor_owner_'+nonce,oid:'16401'},runtime:{name:'harbor_runtime_'+nonce,oid:'16402'}},materialDigest:'8'.repeat(64),keyMaterialDigest:'9'.repeat(64),deviceCaDigest:'b'.repeat(64),deviceInventory:{count:0,sha256:hash({devices:0,credentials:0}),mode:'authenticated-empty'},candidate:{manifestDigest:'sha256:'+'c'.repeat(64),configDigest:'sha256:'+'d'.repeat(64),revision:'e'.repeat(40),schemaDigest:'f'.repeat(64),sourceDigest:'0'.repeat(64)},tlsHost:'harbor.example.invalid'};
 const payload={version:1,purpose:'harbor-storage-receiver-application-v1',restore,application};
 const expectedRestore=Object.fromEntries(['kind','backupId','sourceMachine','receiverMachine','configurationDigest','clusterDigest','handoffDigest','checkpointDigest'].map(key=>[key,structuredClone(restore[key])])),expectedApplication=structuredClone(application);
 return {payload,expected:{expectedRestore,expectedApplication}};
}
const seal=payload=>({payload,signature:sign(null,Buffer.from('harbor-storage-receiver-application-attestation-v1\0'+canonical(payload)),keys.privateKey).toString('base64')});

test('independent application signature binds original final package and exact restored candidate and material facts',()=>{
 const {payload,expected}=fixture(),attestation=seal(payload),result=verifyStorageReceiverApplicationAttestation(attestation,keys.publicKey,expected);assert.deepEqual(result,payload);result.application.candidate.revision='f'.repeat(40);assert.notEqual(result.application.candidate.revision,payload.application.candidate.revision);
 assert.throws(()=>verifyStorageReceiverApplicationAttestation(attestation,other.publicKey,expected),{code:'backup_receiver_application_signature'});
 for(const field of Object.keys(expected.expectedApplication)){
  const changed=structuredClone(expected);changed.expectedApplication[field]=null;assert.throws(()=>verifyStorageReceiverApplicationAttestation(attestation,keys.publicKey,changed),{code:'backup_receiver_application_binding'},field);
 }
 for(const field of Object.keys(expected.expectedRestore)){
  const changed=structuredClone(expected);changed.expectedRestore[field]=null;assert.throws(()=>verifyStorageReceiverApplicationAttestation(attestation,keys.publicKey,changed),{code:'backup_receiver_application_binding'},field);
 }
 const tampered=structuredClone(attestation);tampered.payload.application.keyMaterialDigest='c'.repeat(64);assert.throws(()=>verifyStorageReceiverApplicationAttestation(tampered,keys.publicKey,expected),{code:'backup_receiver_application_signature'});
});

test('even valid signatures cannot promote database-only, same-host, task or unchallenged devices into application authority',()=>{
 for(const change of [p=>{p.restore.scope='task-owned-receiver';},p=>{p.restore.kind='cluster-baseline';},p=>{p.restore.receiverMachine=p.restore.sourceMachine;},p=>{p.restore.checks.offhostDatabaseAndFilesVerified=false;},p=>{p.restore.checks.deploymentAllowed=true;},p=>{p.application.deviceInventory={count:1,sha256:'a'.repeat(64),mode:'existing-client-challenge'};},p=>{p.application.candidate.configDigest=null;},p=>{p.application.extra='untrusted';},p=>{p.restore.extra='untrusted';}]){
  const {payload,expected}=fixture();change(payload);assert.throws(()=>verifyStorageReceiverApplicationAttestation(seal(payload),keys.publicKey,expected),{code:'backup_receiver_application_binding'});
 }
 const {payload}=fixture();assert.throws(()=>verifyStorageReceiverApplicationAttestation(seal(payload),keys.publicKey),{code:'backup_receiver_application_binding'});
});

test('public or copied restore results cannot request application signatures',async()=>{
 for(const receipt of [undefined,{},fixture().payload,{scope:'root-profile-receiver',applicationMaterialsVerified:true}]){
  await assert.rejects(storageReceiverApplicationAuthority(receipt),{code:'backup_receiver_restore_receipt_required'});
  await assert.rejects(attestStorageReceiverApplication(receipt),{code:'backup_receiver_restore_receipt_required'});
 }
 await assert.rejects(attestStorageReceiverApplication({}, {signal:AbortSignal.abort()}),{name:'AbortError'});
});

test('empty device exemption derives from both real inventory counts, not a supplied boolean',async()=>{
 const {payload}=fixture(),a=payload.application,manifest={runId:a.runId,targetDigest:a.targetDigest,database:a.database,roles:Object.values(a.roles),roleGenerations:{ownerOid:a.roles.owner.oid,runtimeOid:a.roles.runtime.oid},sourceMaterialManifestDigest:d,materialKeys:[{name:'key-encryption.key',...d}]},activation={run_id:a.runId,binding:a.bindingDigest,database_oid:a.database.oid,runtime:a.roles.runtime.name};
 for(const counts of [{devices:'0',credentials:'0'},{devices:'1',credentials:'0'},{devices:'0',credentials:'1'}]){
  const binding=await restoredApplicationBinding({store:{get:async()=>counts},manifest,activation,ca:Buffer.from('synthetic ca')});
  if(counts.devices==='0'&&counts.credentials==='0')assert.deepEqual(binding.deviceInventory,{count:0,sha256:hash({devices:0,credentials:0}),mode:'authenticated-empty'});else assert.equal(binding.deviceInventory,null);
  assert.equal(binding.keyMaterialDigest,hash(manifest.materialKeys));assert.equal(binding.materialDigest,hash(d));
 }
 await assert.rejects(restoredApplicationBinding({store:{get:async()=>({devices:'unknown',credentials:'0'})},manifest,activation,ca:Buffer.from('synthetic ca')}),{code:'storage_restored_application_inventory'});
 await assert.rejects(restoredApplicationBinding({store:{get:async()=>({devices:'0',credentials:'0'})},manifest,activation:{...activation,runtime:'wrong'},ca:Buffer.from('synthetic ca')}),{code:'storage_restored_application_binding'});
});
