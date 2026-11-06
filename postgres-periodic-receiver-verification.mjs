import {open,lstat} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join} from 'node:path';
import {createHash,createPublicKey} from 'node:crypto';
import {acquireInstance} from './instance.mjs';
import {postgresBackupContext} from './postgres-backup-context.mjs';
import {postgresBackupRestoreSource} from './postgres-backup.mjs';
import {authenticatePeriodicApplicationBackup,periodicApplicationBackupSource} from './postgres-periodic-application-backup.mjs';
import {postgresBackupRecoveryPointSource} from './postgres-backup-recovery-point.mjs';
import {periodicApplicationKey,periodicApplicationKeyBinding} from './postgres-periodic-application-key.mjs';
import {withPostgresBackupOperation} from './postgres-backup-lock.mjs';
import {verifyStorageReceiverPeriodicAttestation,verifyOffhostStorageReceiverPeriodicAttestation,validStorageReceiverPeriodicPayload} from './storage-receiver-periodic-attestation.mjs';
import {canonical,readManifest,writeManifest,digestFile,transferError} from './migration-artifacts.mjs';

const policies=new WeakMap(),qualifications=new WeakMap(),configFile='/etc/harbor-storage-periodic-receiver.json',publicFile='/etc/harbor-storage-receiver.pub';
const fail=code=>{throw transferError('periodic_receiver_verification_'+code);};
const exact=(value,fields)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join(',')===[...fields].sort().join(',');
const hash=value=>createHash('sha256').update(canonical(value)).digest('hex'),hashBytes=value=>createHash('sha256').update(value).digest('hex');
const sha=value=>typeof value==='string'&&/^[a-f0-9]{64}$/.test(value),same=(a,b)=>canonical(a)===canonical(b);
const runtimeFields=['sourceDigest','nodeDigest','restoreUid'];
function configuration(value,{task=false}={}){
  if(!value||value.version!==1||typeof value.enabled!=='boolean'||Object.keys(value).some(name=>!['version','enabled','receiverMachine','receiverRuntime'].includes(name)))fail('policy_invalid');
  if(!value.enabled&&Object.keys(value).length===2)return Object.freeze({version:1,enabled:false});
  const runtime=value.receiverRuntime;
  if(!exact(value,['version','enabled','receiverMachine','receiverRuntime'])||!sha(value.receiverMachine)||!exact(runtime,runtimeFields)||!Number.isSafeInteger(runtime.restoreUid)||runtime.restoreUid<(task?0:1)||runtime.restoreUid>4294967294||!(task?runtime.sourceDigest===null&&runtime.nodeDigest===null:sha(runtime.sourceDigest)&&sha(runtime.nodeDigest)))fail('policy_invalid');
  return Object.freeze({version:1,enabled:value.enabled,receiverMachine:value.receiverMachine,receiverRuntime:Object.freeze({...runtime})});
}
export function periodicReceiverVerificationPolicy(value){return configuration(value);}
function publicKey(value){
  let key;try{key=value?.type==='public'?createPublicKey({key:value.export({format:'der',type:'spki'}),format:'der',type:'spki'}):createPublicKey(value);}catch{fail('public_key_invalid');}
  if(key.type!=='public'||key.asymmetricKeyType!=='ed25519')fail('public_key_invalid');return key;
}
async function rootBytes(path,maxBytes){
  const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);try{
    const before=await file.stat({bigint:true});if(!before.isFile()||before.uid!==0n||(before.mode&0o777n)!==0o600n||before.nlink!==1n||before.size<1n||before.size>BigInt(maxBytes))fail('file_untrusted');
    const buffer=Buffer.alloc(maxBytes+1);let used=0;while(used<buffer.length){const {bytesRead}=await file.read(buffer,used,buffer.length-used,used);if(!bytesRead)break;used+=bytesRead;}const after=await file.stat({bigint:true});if(['dev','ino','size','mtimeNs','ctimeNs','uid','mode','nlink'].some(name=>before[name]!==after[name])||used!==Number(before.size)||used>maxBytes)fail('file_changed');return buffer.subarray(0,used);
  }finally{await file.close();}
}
function mintPolicy(config,key,{scope,context,files}={}){
  const keyDigest=key?hashBytes(key.export({format:'der',type:'spki'})):null,token=Object.freeze({scope,enabled:config.enabled});
  policies.set(token,{config,key,keyDigest,scope,context,files,digest:hash({config,keyDigest,scope})});return token;
}
export async function loadPeriodicReceiverVerificationPolicy(){
  if(process.platform!=='linux'||process.getuid()!==0)fail('root_policy_required');let bytes;
  try{bytes=await rootBytes(configFile,16384);}catch(error){if(error.code==='ENOENT')return mintPolicy(configuration({version:1,enabled:false}),null,{scope:'root-policy',files:null});throw error;}
  let value;try{value=JSON.parse(bytes.toString('utf8'));}catch{fail('policy_invalid');}const config=configuration(value);
  if(!config.enabled)return mintPolicy(config,null,{scope:'root-policy',files:{configDigest:hashBytes(bytes),publicDigest:null}});
  const pub=await rootBytes(publicFile,4096);return mintPolicy(config,publicKey(pub),{scope:'root-policy',files:{configDigest:hashBytes(bytes),publicDigest:hashBytes(pub)}});
}
// Diagnostic configuration cannot create formal/offhost authority, even if its
// caller supplies a key that signed a root-shaped JSON payload.
export function createTaskPeriodicReceiverVerificationPolicy(context,value,key){
  const record=postgresBackupContext(context);if(record.closed||record.scope!=='task-owned'||record.restoreUid!==process.getuid())fail('task_scope_required');
  const config=configuration(value,{task:true});return mintPolicy(config,config.enabled?publicKey(key):null,{scope:'task-policy',context});
}
function selectedPolicy(token,context,{task}={}){
  const policy=policies.get(token);if(!policy||policy.scope!==(task?'task-policy':'root-policy')||task&&policy.context!==context)fail('policy_required');return policy;
}
async function revalidatePolicy(token,context,{task,signal}={}){
  signal?.throwIfAborted();const policy=selectedPolicy(token,context,{task});
  if(!task){const current=policies.get(await loadPeriodicReceiverVerificationPolicy());if(current.digest!==policy.digest||!same(current.files,policy.files)||!current.config.enabled)fail('policy_changed');}
  signal?.throwIfAborted();return policy;
}
async function sourceClosure(context,proof,pointId,{signal}={}){
  const record=postgresBackupContext(context),base=postgresBackupRestoreSource(proof);periodicApplicationKey(record);
  if(base.context!==context||record.pendingBinding||record.receiverBinding||record.resumeBinding||record.applicationPlan)fail('source_context');
  const backup=await authenticatePeriodicApplicationBackup(context,proof,pointId,{signal}),source=periodicApplicationBackupSource(backup),point=postgresBackupRecoveryPointSource(source.point),directory=source.directory,handoffPath=join(directory,'handoff','handoff.enc'),before=await digestFile(handoffPath,{signal}),handoff=await readManifest(handoffPath,periodicApplicationKey(record),{signal});
  if(!same(before,await digestFile(handoffPath,{signal}))||handoff.version!==1||handoff.kind!=='periodic-application'||handoff.backupId!==proof.backupId||handoff.pointId!==pointId||handoff.scope!==record.scope||handoff.configurationDigest!==record.configurationDigest||handoff.keyVersion!==record.keyVersion||handoff.applicationKeyVersion!==record.applicationKeyVersion||handoff.applicationKeyDigest!==record.applicationKeyDigest||handoff.expected?.system_id!==record.expected.system_id||!sha(handoff.sourceMachine)||!same(handoff.checkpointDigest,source.manifest.checkpointDigest)||!same(handoff.backupCompleteDigest,source.manifestDigest)||handoff.applicationPolicyDigest!==source.manifest.applicationPolicyDigest||handoff.captureManifestHash!==source.manifest.captureManifestHash||handoff.pointManifestHash!==source.manifest.pointManifestHash)fail('handoff_binding');
  const controls={backupId:proof.backupId,pointId,configurationDigest:record.configurationDigest,clusterDigest:hash(record.expected.system_id),sourceMachine:handoff.sourceMachine,handoffDigest:before,checkpointDigest:source.manifest.checkpointDigest,backupCompleteDigest:source.manifestDigest,applicationPolicyDigest:source.manifest.applicationPolicyDigest,...periodicApplicationKeyBinding(record),checkpointAt:point.checkpoint.created};
  return {record,directory,controls,digest:hash(controls)};
}
function expectedBinding(source,policy,{task}={}){
  const {checkpointAt:unused,...controls}=source.controls;
  return {kind:'periodic-application',...controls,receiverMachine:policy.config.receiverMachine,runtime:{...policy.config.receiverRuntime,crossUidExecutionVerified:!task,publisherUidVerified:!task}};
}
async function inputEvidence(path,provided,{task,signal}={}){
  signal?.throwIfAborted();if(task){const copy=structuredClone(provided),bytes=Buffer.from(canonical(copy));return {attestation:copy,digest:{sha256:hashBytes(bytes),bytes:bytes.length}};}
  const bytes=await rootBytes(path,65536);let value;try{value=JSON.parse(bytes.toString('utf8'));}catch{fail('input_invalid');}return {attestation:value,digest:{sha256:hashBytes(bytes),bytes:bytes.length}};
}
function verifyEvidence(evidence,source,policy,{task}={}){
  const expected=expectedBinding(source,policy,{task}),payload=(task?verifyStorageReceiverPeriodicAttestation:verifyOffhostStorageReceiverPeriodicAttestation)(evidence.attestation,policy.key,{expected});
  if(task&&payload.scope!=='task-owned-receiver')fail('task_scope_required');return payload;
}
async function supervised(record,signal,operation){
  if(record.closed||record.busy||record.periodicApplicationPending)fail('busy');
  const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal,AbortSignal.timeout(record.profile.processTimeoutMs)].filter(Boolean));record.periodicApplicationCancellation=cancellation;
  const pending=withPostgresBackupOperation(record,()=>operation(combined),{signal:combined});record.periodicApplicationPending=pending;
  try{return await pending;}finally{record.periodicApplicationPending=null;record.periodicApplicationCancellation=null;}
}
async function run(context,proof,pointId,policyToken,provided,{task,signal}={}){
  const policy=selectedPolicy(policyToken,context,{task});if(!policy.config.enabled)return {state:'periodic_receiver_verification_disabled',deploymentAllowed:false};
  const record=postgresBackupContext(context);
  if(task?record.scope!=='task-owned':process.getuid()!==0||record.scope!=='existing-cluster')fail('scope_required');
  return supervised(record,signal,async combined=>{
    const first=await sourceClosure(context,proof,pointId,{signal:combined});let lease;
    try{lease=await acquireInstance(join(first.directory,'.receiver-verification'));}catch(error){if(error.code==='EADDRINUSE')fail('busy');throw error;}
    try{
      const inputPath=join(first.directory,'receiver-attestation.json'),evidence=await inputEvidence(inputPath,provided,{task,signal:combined}),payload=verifyEvidence(evidence,first,policy,{task});
      const recheck=async()=>{
        await revalidatePolicy(policyToken,context,{task,signal:combined});const current=await sourceClosure(context,proof,pointId,{signal:combined}),input=await inputEvidence(inputPath,provided,{task,signal:combined});
        if(current.digest!==first.digest||!same(input.digest,evidence.digest))fail('source_changed');verifyEvidence(input,current,policy,{task});periodicApplicationKey(record);return current;
      };
      await recheck();const output=join(first.directory,task?'periodic-receiver-diagnostic.enc':'periodic-receiver.enc');
      const facts={version:1,scope:task?'task-periodic-receiver-diagnostic':'approved-periodic-offhost-recovery',...first.controls,keyVersion:record.keyVersion,receiverMachine:payload.receiverMachine,receiverRuntime:structuredClone(payload.runtime),receiverPolicyDigest:policy.digest,receiverPublicKeyDigest:policy.keyDigest,attestationDigest:evidence.digest,receiverResultDigest:payload.resultDigest,attestation:structuredClone(evidence.attestation),signatureVerified:true,offhostApplicationRestoreVerified:!task,applicationMaterialsVerified:!task,candidateVerified:false,offhostVerified:false,deploymentAllowed:false};
      let saved;try{saved=await readManifest(output,periodicApplicationKey(record),{signal:combined});}catch(error){if(error.code!=='ENOENT')throw error;}
      if(saved){if(!Number.isSafeInteger(saved.acceptedAt)||saved.acceptedAt<0||!same(saved,{...facts,acceptedAt:saved.acceptedAt}))fail('record_binding');}
      else{combined.throwIfAborted();const acceptedAt=Date.now();if(acceptedAt<facts.checkpointAt)fail('clock_reversed');saved={...facts,acceptedAt};await writeManifest(output,saved,periodicApplicationKey(record));}
      await recheck();if(!same(saved,await readManifest(output,periodicApplicationKey(record),{signal:combined})))fail('record_changed');
      combined.throwIfAborted();periodicApplicationKey(record);const token=Object.freeze({backupId:proof.backupId,pointId,scope:saved.scope});qualifications.set(token,{context,proof,pointId,policyToken,provided:task?structuredClone(provided):undefined,task,output,saved,sourceDigest:first.digest});return token;
    }finally{await lease.release();}
  });
}
export async function verifyPeriodicReceiverAttestation(context,proof,pointId,{signal}={}){
  const policy=await loadPeriodicReceiverVerificationPolicy();return run(context,proof,pointId,policy,undefined,{signal,task:false});
}
export async function verifyTaskPeriodicReceiverAttestation(context,proof,pointId,attestation,policy,{signal}={}){
  return run(context,proof,pointId,policy,structuredClone(attestation),{signal,task:true});
}
export async function inspectPeriodicReceiverVerification(token,{context,proof,signal}={}){
  signal?.throwIfAborted();const saved=qualifications.get(token);if(!saved||context!==undefined&&context!==saved.context||proof!==undefined&&proof!==saved.proof)fail('qualification_required');
  const record=postgresBackupContext(saved.context);periodicApplicationKey(record);
  return supervised(record,signal,async combined=>{
    const policy=await revalidatePolicy(saved.policyToken,saved.context,{task:saved.task,signal:combined}),source=await sourceClosure(saved.context,saved.proof,saved.pointId,{signal:combined}),input=await inputEvidence(join(source.directory,'receiver-attestation.json'),saved.provided,{task:saved.task,signal:combined});
    if(source.digest!==saved.sourceDigest||!same(input.digest,saved.saved.attestationDigest)||!same(await readManifest(saved.output,periodicApplicationKey(record),{signal:combined}),saved.saved))fail('record_changed');
    verifyEvidence(input,source,policy,{task:saved.task});combined.throwIfAborted();periodicApplicationKey(record);return structuredClone(saved.saved);
  });
}

// Historical record authentication for retention/monitoring only. It does not
// mint the live qualification returned by the fixed-root verifier above.
export function validatePeriodicReceiverVerificationRecord(value,expected){
  const fields=['version','scope','backupId','pointId','configurationDigest','clusterDigest','sourceMachine','receiverMachine','handoffDigest','checkpointDigest','backupCompleteDigest','applicationPolicyDigest','applicationKeyVersion','applicationKeyDigest','checkpointAt','keyVersion','receiverRuntime','receiverPolicyDigest','receiverPublicKeyDigest','attestationDigest','receiverResultDigest','attestation','signatureVerified','offhostApplicationRestoreVerified','applicationMaterialsVerified','candidateVerified','offhostVerified','deploymentAllowed','acceptedAt'];
  if(!exact(value,fields)||value.version!==1||!['task-periodic-receiver-diagnostic','approved-periodic-offhost-recovery'].includes(value.scope)||!Number.isSafeInteger(value.acceptedAt)||!Number.isSafeInteger(value.checkpointAt)||value.checkpointAt<0||value.acceptedAt<value.checkpointAt||!['receiverPolicyDigest','receiverPublicKeyDigest','receiverResultDigest'].every(name=>sha(value[name]))||!exact(value.attestationDigest,['sha256','bytes'])||!sha(value.attestationDigest.sha256)||!Number.isSafeInteger(value.attestationDigest.bytes)||value.attestationDigest.bytes<1||value.signatureVerified!==true||value.candidateVerified!==false||value.offhostVerified!==false||value.deploymentAllowed!==false||!exact(value.attestation,['payload','signature'])||!validStorageReceiverPeriodicPayload(value.attestation.payload)||!(/^[A-Za-z0-9+/]{86}==$/.test(value.attestation.signature??'')))fail('record_binding');
  for(const name of ['backupId','pointId','configurationDigest','clusterDigest','keyVersion','checkpointAt','handoffDigest','checkpointDigest','backupCompleteDigest','applicationPolicyDigest','applicationKeyVersion','applicationKeyDigest'])if(!same(value[name],expected[name]))fail('record_binding');
  const payload=value.attestation.payload,task=value.scope==='task-periodic-receiver-diagnostic';
  if(payload.scope!==(task?'task-owned-receiver':'root-profile-receiver')||value.offhostApplicationRestoreVerified!==!task||value.applicationMaterialsVerified!==!task||!same(payload.runtime,value.receiverRuntime)||payload.sourceMachine!==value.sourceMachine||payload.receiverMachine!==value.receiverMachine||payload.resultDigest!==value.receiverResultDigest||!task&&(payload.sourceMachine===payload.receiverMachine||payload.checks.offhostDatabaseVerified!==true||payload.checks.offhostDatabaseAndFilesVerified!==true)||['sourceFreeRestoreVerified','receiverRestoreVerified','recoveryExitedVerified','inventoryVerified','writeReadbackVerified','applicationMaterialsVerified'].some(name=>payload.checks[name]!==true))fail('record_binding');
  for(const name of ['backupId','pointId','configurationDigest','clusterDigest','handoffDigest','checkpointDigest','backupCompleteDigest','applicationPolicyDigest','applicationKeyVersion','applicationKeyDigest'])if(!same(value[name],payload[name]))fail('record_binding');
  return structuredClone(value);
}
