import {open} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join} from 'node:path';
import {createHash,createPublicKey} from 'node:crypto';
import {storageOperatorContext} from './storage-operator-context.mjs';
import {withVerifiedStorageOperatorBaseline,revalidateStorageOperatorBaseline,storageOperatorBaselineMode} from './storage-operator-baseline.mjs';
import {withPendingStorageOperatorBackup,inspectPendingStorageOperatorBackup} from './storage-operator-backup-binding.mjs';
import {createPendingPostgresBackupContext,closePostgresBackupContext,postgresBackupContext,verifyPostgresBackupPendingBinding} from './postgres-backup-context.mjs';
import {verifyPostgresBackup} from './postgres-backup.mjs';
import {backupMachineIdentity} from './postgres-backup-handoff.mjs';
import {verifyStorageReceiverApplicationAttestation} from './storage-receiver-application-attestation.mjs';
import {validateStorageActivationFacts} from './storage-activation-receipt.mjs';
import {PostgresStore} from './postgres-store.mjs';
import {canonical,readManifest,digestFile,transferError} from './migration-artifacts.mjs';

const proofs=new WeakMap(),fail=code=>{throw transferError('storage_operator_activation_'+code);},hash=value=>createHash('sha256').update(canonical(value)).digest('hex'),same=(a,b)=>canonical(a)===canonical(b);
async function trustedBytes(path,maxBytes){const file=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);try{const info=await file.stat();if(!info.isFile()||info.uid!==0||(info.mode&0o777)!==0o600||info.size<1||info.size>maxBytes)fail('file_not_trusted');return await file.readFile();}finally{await file.close();}}
function record(proof,context){const value=proofs.get(proof);if(!value||value.closed)fail('proof_required');if(value.context!==context)fail('context_mismatch');return value;}
const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};

async function verify(value,signal){
 signal?.throwIfAborted();if(value.closed)fail('proof_required');const {context,pending,baselineVerification,backupContext,attestation,publicKey,key}=value,trusted=storageOperatorContext(context);
 await revalidateStorageOperatorBaseline(baselineVerification,{context,signal});const {binding}=await inspectPendingStorageOperatorBackup(pending),configuration=trusted.configuration,backup=postgresBackupContext(backupContext);
 const state=await readManifest(join(configuration.runDirectory,'operator.enc'),key,{signal});
 if(state.phase!=='pending'||!same(state.baseline,value.baselineSummary)||!state.preflight?.candidateVerified||state.preflight.scope!=='docker-candidate'||state.preflight.imageDigest!==configuration.candidate.imageDigest||binding.configurationDigest!==trusted.configurationDigest)fail('candidate_preflight_required');
 const id=attestation?.payload?.restore?.backupId;if(typeof id!=='string'||!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(id))fail('backup_id');
 const directory=join(backup.artifactDirectory,id),handoffDirectory=join(directory,'handoff');
 await verifyPostgresBackup(backupContext,id,{signal});const handoff=await readManifest(join(handoffDirectory,'handoff.enc'),key,{signal}),application=await readManifest(join(directory,'application','application.enc'),key,{signal}),target=await readManifest(join(configuration.runDirectory,'artifacts','target.enc'),key,{signal});
 const checkpoint=await digestFile(join(directory,'checkpoint.enc'),{signal}),handoffDigest=await digestFile(join(handoffDirectory,'handoff.enc'),{signal});
 if(handoff.kind!=='pending-final'||handoff.backupId!==id||handoff.configurationDigest!==backup.configurationDigest||handoff.pendingBindingDigest!==hash(binding)||!same(handoff.checkpointDigest,checkpoint)||!same(handoff.applicationManifestDigest,await digestFile(join(directory,'application','application.enc'),{signal}))||application.backupId!==id||application.configurationDigest!==backup.configurationDigest||application.pendingBindingDigest!==hash(binding)||application.runId!==binding.runId||application.targetDigest!==binding.targetDigest||!same(application.database,binding.database)||!same(application.roles,binding.roles)||!same(application.roleGenerations,binding.roleGenerations)||!same(application.checkpointDigest,checkpoint)||!same(application.sourceMaterialManifestDigest,binding.materialManifestDigest)||!same(application.materialKeys,binding.materialKeys))fail('final_backup_binding');
 const sourceMachine=await backupMachineIdentity();if(handoff.sourceMachine!==sourceMachine||!configuration.baselineReceiverMachine||configuration.baselineReceiverMachine===sourceMachine)fail('receiver_required');
 const sourceCandidate=configuration.candidate,receiverCandidate=attestation.payload.application?.candidate;
 if(receiverCandidate?.manifestDigest!==sourceCandidate.imageDigest||receiverCandidate.sourceDigest!==state.preflight.sourceDigest||receiverCandidate.schemaDigest!==createHash('sha256').update(JSON.stringify(target.schemaChecksums)).digest('hex')||attestation.payload.application?.tlsHost!==new URL(sourceCandidate.deviceOrigin).hostname.replace(/^\[|\]$/g,''))fail('candidate_binding');
 const secrets=binding.materialRoots.find(root=>root.id==='secrets');if(!secrets)fail('device_material_required');
 const raw=value.adminOptions,admin=new PostgresStore({...raw,poolSize:1});let store,counts;
 try{const options=admin.pool.options;store=new PostgresStore({connection:{host:options.host,port:options.port,user:options.user,password:options.password,database:binding.database.name,...(options.ssl?{ssl:options.ssl}:{})},poolSize:1,allowPlaintextLocalNetwork:raw.allowPlaintextLocalNetwork===true});counts=await store.get('SELECT (SELECT COUNT(*)::text FROM harbor.employee_devices) devices,(SELECT COUNT(*)::text FROM harbor.device_credentials) credentials');}finally{await store?.close();await admin.close();}
 if(counts?.devices!=='0'||counts?.credentials!=='0')fail('existing_client_challenge_required');
 const role=kind=>{const oid=binding.roleGenerations[kind+'Oid'],row=binding.roles.find(row=>row.oid===oid);if(!row)fail('role_generation');return {name:row.name,oid};};
 const expectedApplication={runId:binding.runId,bindingDigest:binding.bindingDigest,targetDigest:binding.targetDigest,database:binding.database,roles:{owner:role('owner'),runtime:role('runtime')},materialDigest:hash(binding.materialManifestDigest),keyMaterialDigest:hash(binding.materialKeys),deviceCaDigest:(await digestFile(join(secrets.directory,'device','ca.pem'),{signal})).sha256,deviceInventory:{count:0,sha256:hash({devices:0,credentials:0}),mode:'authenticated-empty'},candidate:receiverCandidate,tlsHost:attestation.payload.application.tlsHost};
 const verified=verifyStorageReceiverApplicationAttestation(attestation,publicKey,{expectedRestore:{kind:'pending-final',backupId:id,sourceMachine,receiverMachine:configuration.baselineReceiverMachine,configurationDigest:backup.configurationDigest,clusterDigest:hash(binding.cluster),handoffDigest,checkpointDigest:checkpoint},expectedApplication});
 await verifyPostgresBackupPendingBinding(backupContext);signal?.throwIfAborted();if(value.closed)fail('proof_required');
 const {tlsHost,...applicationFacts}=expectedApplication;
 return freeze(structuredClone(validateStorageActivationFacts({...applicationFacts,configurationDigest:binding.configurationDigest,cluster:binding.cluster,backupId:id,checkpointDigest:checkpoint.sha256,receiverResultDigest:verified.restore.resultDigest,sourceMachine,receiverMachine:verified.restore.receiverMachine,publicOrigin:sourceCandidate.publicOrigin,deviceOrigin:sourceCandidate.deviceOrigin,stack:'/opt/product-development/harbor-local-gateway'})));
}

// The lock, fenced source lease and live baseline remain owned by this callback.
// JSON persisted by a prior command cannot mint or prolong this capability.
export async function withVerifiedStorageOperatorActivationProof({context,baselineVerification,backupKey,signal}={},consume){
 const trusted=storageOperatorContext(context);if(process.platform!=='linux'||process.getuid()!==0||trusted.scope!=='docker-systemd')fail('root_required');if(typeof consume!=='function')fail('consumer_required');if(storageOperatorBaselineMode(baselineVerification)!=='resume')fail('resumed_baseline_required');
 const bytes=await trustedBytes(join(trusted.configuration.runDirectory,'final-receiver-application-attestation.json'),32768),publicKey=createPublicKey(await trustedBytes('/etc/harbor-storage-receiver.pub',4096));let attestation;try{attestation=JSON.parse(bytes.toString('utf8'));}catch{fail('attestation_invalid');}
 return withVerifiedStorageOperatorBaseline(baselineVerification,{context,signal},async({verification,backupContext:baselineContext,backupProof,backupRestore})=>withPendingStorageOperatorBackup({context,backupKey},async pending=>{
  let backupContext,value;try{backupContext=await createPendingPostgresBackupContext(baselineContext,backupProof,backupRestore,pending,{signal});const actual=await inspectPendingStorageOperatorBackup(pending);value={context,pending,baselineVerification,baselineSummary:verification,backupContext,attestation,publicKey,adminOptions:actual.adminOptions,key:Buffer.from(backupKey),closed:false};const handle=Object.freeze({});proofs.set(handle,value);await verify(value,signal);return await consume(handle);}finally{if(value){value.closed=true;value.key.fill(0);}if(backupContext)await closePostgresBackupContext(backupContext);}
 }));
}
export async function inspectStorageOperatorActivationProof(proof,{context,signal}={}){const value=record(proof,context),facts=await verify(value,signal);record(proof,context);return facts;}
