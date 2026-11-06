import test from 'node:test';
import assert from 'node:assert/strict';
import {generateKeyPairSync,randomUUID,sign,createHmac} from 'node:crypto';
import {canonical} from '../migration-artifacts.mjs';
import {verifyStorageReceiverAttestation} from '../storage-receiver-attestation.mjs';
import {verifyStorageReceiverApplicationAttestation} from '../storage-receiver-application-attestation.mjs';
import {attestStorageReceiverPeriodic,attestTaskStorageReceiverPeriodic,verifyStorageReceiverPeriodicAttestation,verifyOffhostStorageReceiverPeriodicAttestation} from '../storage-receiver-periodic-attestation.mjs';

const keys=generateKeyPairSync('ed25519'),other=generateKeyPairSync('ed25519');
const domain='harbor-storage-receiver-periodic-attestation-v1\0';
const fields=['kind','backupId','pointId','sourceMachine','receiverMachine','configurationDigest','clusterDigest','handoffDigest','checkpointDigest','backupCompleteDigest','applicationPolicyDigest','applicationKeyVersion','applicationKeyDigest','runtime'];
function fixture(){
  const payload={version:1,purpose:'harbor-storage-receiver-periodic-v1',scope:'root-profile-receiver',kind:'periodic-application',backupId:randomUUID(),pointId:randomUUID(),sourceMachine:'1'.repeat(64),receiverMachine:'2'.repeat(64),configurationDigest:'3'.repeat(64),clusterDigest:'4'.repeat(64),handoffDigest:{sha256:'5'.repeat(64),bytes:200},checkpointDigest:{sha256:'6'.repeat(64),bytes:220},backupCompleteDigest:{sha256:'7'.repeat(64),bytes:240},applicationPolicyDigest:'8'.repeat(64),applicationKeyVersion:'synthetic-application-v1',applicationKeyDigest:'9'.repeat(64),resultDigest:'a'.repeat(64),runtime:{sourceDigest:'b'.repeat(64),nodeDigest:'c'.repeat(64),restoreUid:65534,crossUidExecutionVerified:true,publisherUidVerified:true},checks:{sourceFreeRestoreVerified:true,receiverRestoreVerified:true,recoveryExitedVerified:true,inventoryVerified:true,writeReadbackVerified:true,applicationMaterialsVerified:true,offhostDatabaseVerified:true,offhostDatabaseAndFilesVerified:true,deploymentAllowed:false}};
  return {payload,expected:Object.fromEntries(fields.map(name=>[name,structuredClone(payload[name])]))};
}
const seal=(payload,signingDomain=domain)=>({payload,signature:sign(null,Buffer.from(signingDomain+canonical(payload)),keys.privateKey).toString('base64')});

// These are synthetic signature/consumer boundaries. Real receipt issuance is
// covered by the independent private PostgreSQL and fixed-root CLI chains.
test('periodic signatures bind the selected point, both key identities and installed runtime without granting activation',()=>{
  const {payload,expected}=fixture(),attestation=seal(payload),verified=verifyOffhostStorageReceiverPeriodicAttestation(attestation,keys.publicKey,{expected});
  assert.equal(verified.pointId,payload.pointId);assert.equal(verified.checks.deploymentAllowed,false);
  verified.runtime.restoreUid=0;assert.equal(attestation.payload.runtime.restoreUid,65534);
  for(const field of ['backupId','pointId','sourceMachine','receiverMachine','configurationDigest','clusterDigest','applicationPolicyDigest','applicationKeyVersion','applicationKeyDigest']){
    const changed=structuredClone(expected);changed[field]=field.endsWith('Id')?randomUUID():field==='applicationKeyVersion'?'other-v1':'d'.repeat(64);
    assert.throws(()=>verifyStorageReceiverPeriodicAttestation(attestation,keys.publicKey,{expected:changed}),{code:'periodic_receiver_attestation_binding'},field);
  }
  for(const field of ['handoffDigest','checkpointDigest','backupCompleteDigest']){
    const changed=structuredClone(expected);changed[field].bytes++;
    assert.throws(()=>verifyStorageReceiverPeriodicAttestation(attestation,keys.publicKey,{expected:changed}),{code:'periodic_receiver_attestation_binding'},field);
  }
  const changed=structuredClone(expected);changed.runtime.sourceDigest='e'.repeat(64);
  assert.throws(()=>verifyStorageReceiverPeriodicAttestation(attestation,keys.publicKey,{expected:changed}),{code:'periodic_receiver_attestation_binding'});
});

test('WAL keys, foreign signatures and legacy purposes cannot mint or consume periodic recovery proof',()=>{
  const {payload,expected}=fixture(),attestation=seal(payload);
  assert.throws(()=>verifyStorageReceiverPeriodicAttestation(attestation,other.publicKey,{expected}),{code:'periodic_receiver_attestation_signature'});
  assert.throws(()=>verifyStorageReceiverPeriodicAttestation(seal(payload,'harbor-storage-receiver-attestation-v1\0'),keys.publicKey,{expected}),{code:'periodic_receiver_attestation_signature'});
  const mac=createHmac('sha256',Buffer.alloc(32,1)).update(canonical(payload)).digest('base64');
  assert.throws(()=>verifyStorageReceiverPeriodicAttestation({payload,signature:mac},keys.publicKey,{expected}),{code:'periodic_receiver_attestation_signature'});
  assert.throws(()=>verifyStorageReceiverAttestation(attestation,keys.publicKey,{expected}),{code:'backup_receiver_attestation_binding'});
  assert.throws(()=>verifyStorageReceiverApplicationAttestation(attestation,keys.publicKey),{code:'backup_receiver_application_binding'});
  const changed=structuredClone(attestation);changed.payload.pointId=randomUUID();
  assert.throws(()=>verifyStorageReceiverPeriodicAttestation(changed,keys.publicKey,{expected}),{code:'periodic_receiver_attestation_signature'});
});

test('valid task, same-host and incomplete results cannot satisfy the formal offhost consumer',()=>{
  for(const kind of ['task','same-host','missing-db','missing-files','incomplete','activation']){
    const {payload,expected}=fixture();
    if(kind==='task'){payload.scope='task-owned-receiver';payload.runtime={sourceDigest:null,nodeDigest:null,restoreUid:1000,crossUidExecutionVerified:false,publisherUidVerified:false};expected.runtime=structuredClone(payload.runtime);payload.checks.offhostDatabaseVerified=false;payload.checks.offhostDatabaseAndFilesVerified=false;}
    if(kind==='same-host'){payload.receiverMachine=payload.sourceMachine;expected.receiverMachine=payload.receiverMachine;payload.checks.offhostDatabaseVerified=false;payload.checks.offhostDatabaseAndFilesVerified=false;}
    if(kind==='missing-db'){payload.checks.offhostDatabaseVerified=false;payload.checks.offhostDatabaseAndFilesVerified=false;}
    if(kind==='missing-files')payload.checks.offhostDatabaseAndFilesVerified=false;
    if(kind==='incomplete')payload.checks.writeReadbackVerified=false;
    if(kind==='activation')payload.checks.deploymentAllowed=true;
    const attestation=seal(payload);
    if(!['incomplete','activation'].includes(kind))assert.equal(verifyStorageReceiverPeriodicAttestation(attestation,keys.publicKey,{expected}).kind,'periodic-application');
    assert.throws(()=>verifyOffhostStorageReceiverPeriodicAttestation(attestation,keys.publicKey,{expected}),{code:['incomplete','activation'].includes(kind)?'periodic_receiver_attestation_incomplete':'periodic_receiver_attestation_offhost_required'},kind);
  }
});

test('unbounded claims and malformed identities are rejected before encoding, and cancellation cannot request a signature',async()=>{
  const {payload,expected}=fixture();
  for(const change of [{extra:'x'.repeat(100000)},{pointId:'unapproved'},{applicationKeyVersion:'x'.repeat(65)},{runtime:{...payload.runtime,restoreUid:0}},{runtime:{...payload.runtime,extra:true}},{backupCompleteDigest:{sha256:'f'.repeat(64),bytes:-1}},{checks:{...payload.checks,extra:true}}]){
    assert.throws(()=>verifyStorageReceiverPeriodicAttestation({payload:{...payload,...change},signature:'invalid'},keys.publicKey,{expected}),{code:'periodic_receiver_attestation_binding'});
  }
  const controller=new AbortController();controller.abort();
  await assert.rejects(attestStorageReceiverPeriodic({}, {signal:controller.signal}),{name:'AbortError'});
  await assert.rejects(attestTaskStorageReceiverPeriodic({},keys.privateKey,{signal:controller.signal}),{name:'AbortError'});
});
