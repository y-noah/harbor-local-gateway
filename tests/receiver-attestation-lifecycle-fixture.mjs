// Executes the unmodified production modules with isolated I/O boundaries.
// Root identity and recovery facts here are stubs, never formal proof.
import assert from 'node:assert/strict';
import vm from 'node:vm';
import {registerHooks} from 'node:module';
import {fileURLToPath} from 'node:url';
import {readFile} from 'node:fs/promises';
import {generateKeyPairSync,randomUUID} from 'node:crypto';
import {canonical,transferError} from '../migration-artifacts.mjs';

const unexpectedPeriodic=()=>assert.fail('legacy lifecycle must not enter periodic recovery');
async function load(file,overrides){
 const context=vm.createContext({Buffer,URL,AbortController,AbortSignal,structuredClone,process:{platform:'linux',getuid:()=>0,argv:[]}}),cache=new Map();
 async function dependency(specifier,dynamic=false){let item=cache.get(specifier);if(!item){const values=overrides[specifier]??(specifier.startsWith('node:')?await import(specifier):null);assert.ok(values,'unexpected dependency '+specifier);item=new vm.SyntheticModule(Object.keys(values),function(){for(const [name,value] of Object.entries(values))this.setExport(name,value);},{context});cache.set(specifier,item);}if(dynamic){if(item.status==='unlinked')await item.link(()=>{});if(item.status==='linked')await item.evaluate();}if(dynamic)assert.equal(typeof item.namespace.attestStorageReceiverRestore,'function',Object.keys(item.namespace).join(','));return item;}
 const module=new vm.SourceTextModule(await readFile(file,'utf8'),{context,identifier:file.href,initializeImportMeta:meta=>{meta.url=file.href;},importModuleDynamically:specifier=>dependency(specifier,true)});await module.link(specifier=>dependency(specifier));await module.evaluate();return module.namespace;
}
const digest={bytes:120,sha256:'a'.repeat(64)},payload={version:1,purpose:'harbor-storage-receiver-result-v1',scope:'root-profile-receiver',kind:'cluster-baseline',backupId:randomUUID(),sourceMachine:'1'.repeat(64),receiverMachine:'2'.repeat(64),configurationDigest:'3'.repeat(64),clusterDigest:'4'.repeat(64),handoffDigest:digest,checkpointDigest:digest,resultDigest:'5'.repeat(64),checks:{sourceFreeRestoreVerified:true,receiverRestoreVerified:true,recoveryExitedVerified:true,inventoryVerified:true,writeReadbackVerified:true,offhostDatabaseVerified:true,offhostDatabaseAndFilesVerified:false,applicationMaterialsVerified:false,deploymentAllowed:false}},material=generateKeyPairSync('ed25519').privateKey.export({format:'pem',type:'pkcs8'}),common={'./migration-artifacts.mjs':{canonical,transferError,readManifest:unexpectedPeriodic},'./postgres-periodic-application-key.mjs':{periodicApplicationKey:unexpectedPeriodic,loadPeriodicApplicationKey:unexpectedPeriodic}};
if(process.argv[1]===fileURLToPath(new URL('../storage-receiver.mjs',import.meta.url))){
 // Run the actual CLI and real dynamic signer import. Only recovery I/O is
 // isolated; the task scope must be refused before any root signing key read.
 const entry=new URL('../storage-receiver.mjs',import.meta.url).href;let closed=false,signerLoaded=false;
 const source={
  './postgres-backup-receiver.mjs':`export const createPostgresBackupReceiver=async()=>({}),inspectPostgresBackupReceiver=async()=>(${JSON.stringify({formal:false,receiverMachine:payload.receiverMachine,manifest:{kind:payload.kind,backupId:payload.backupId,sourceMachine:payload.sourceMachine,configurationDigest:payload.configurationDigest,expected:{system_id:'123456789'}},profile:{handoffDigest:digest}})});export const closePostgresBackupReceiver=async()=>globalThis.__receiverCliClosed();`,
  './postgres-backup-context.mjs':'export const createReceivedPostgresBackupContext=async()=>({});export const postgresBackupContext=()=>{throw Error("unexpected periodic context");};',
  './postgres-backup.mjs':`export const verifyPostgresBackup=async()=>({backupId:${JSON.stringify(payload.backupId)}});export const postgresBackupRestoreSource=()=>{throw Error("unexpected periodic source");};`,
  './postgres-backup-restore.mjs':`export const restorePostgresBackup=async()=>({}),inspectPostgresBackupRestore=()=>(${JSON.stringify({restoreVerified:true,recoveryExitedVerified:true,inventoryVerified:true,writeReadbackVerified:true,application:null,checkpointDigest:digest})}),verifyPostgresBackupRestore=async()=>({});`,
  './storage-operator-backup-application.mjs':'export const installReceivedStorageOperatorApplication=async()=>{};'
 };
 globalThis.__receiverCliClosed=()=>{closed=true;};
 registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&source[specifier])return {url:'data:text/javascript,'+encodeURIComponent(source[specifier]),shortCircuit:true};return next(specifier,context);},load(url,context,next){if(url===new URL('../storage-receiver-attestation.mjs',import.meta.url).href)signerLoaded=true;return next(url,context);}});
 process.once('exit',()=>{assert.equal(signerLoaded,true,'actual dynamic signer module must load');assert.equal(closed,true,'CLI must complete receiver cleanup');});
}else{
for(const phase of ['none','read','close','close-failure']){
 const cancellation=new AbortController();let bytes,closed=false;
 const signer=await load(new URL('../storage-receiver-attestation.mjs',import.meta.url),{...common,'./storage-receiver.mjs':{storageReceiverRestoreAuthority:async()=>structuredClone(payload)},'node:fs/promises':{open:async path=>{assert.equal(path,'/etc/harbor-storage-receiver-signing.key');return {stat:async()=>({isFile:()=>true,uid:0,mode:0o100600,size:Buffer.byteLength(material)}),readFile:async()=>{bytes=Buffer.from(material);if(phase==='read')cancellation.abort();return bytes;},close:async()=>{closed=true;if(phase==='close')cancellation.abort();if(phase==='close-failure')throw Error('synthetic close failure');}};}}});
 if(phase==='none')assert.equal((await signer.attestStorageReceiverRestore({}, {signal:cancellation.signal})).payload.kind,'cluster-baseline');else await assert.rejects(signer.attestStorageReceiverRestore({}, {signal:cancellation.signal}),phase==='close-failure'?/synthetic close failure/:{name:'AbortError'});assert.equal(closed,true);assert.ok(bytes.every(value=>value===0),'private key bytes must clear on every path');
}
for(const mode of ['restore','restore-and-attest']){
 const cancellation=new AbortController();let closed=false;
 const command=await load(new URL('../storage-receiver.mjs',import.meta.url),{...common,'./postgres-backup-receiver.mjs':{createPostgresBackupReceiver:async()=>({}),inspectPostgresBackupReceiver:async()=>({formal:false,receiverMachine:'2'.repeat(64),manifest:{kind:'cluster-baseline',sourceMachine:'1'.repeat(64)},profile:{handoffDigest:digest}}),closePostgresBackupReceiver:async()=>{closed=true;cancellation.abort();}},'./postgres-backup-context.mjs':{createReceivedPostgresBackupContext:async()=>({}),postgresBackupContext:unexpectedPeriodic},'./postgres-backup.mjs':{verifyPostgresBackup:async()=>({backupId:payload.backupId}),postgresBackupRestoreSource:unexpectedPeriodic},'./postgres-backup-restore.mjs':{restorePostgresBackup:async()=>({}),inspectPostgresBackupRestore:()=>({restoreVerified:true,recoveryExitedVerified:true,inventoryVerified:true,writeReadbackVerified:true,application:null,checkpointDigest:digest}),verifyPostgresBackupRestore:async()=>({})},'./storage-operator-backup-application.mjs':{installReceivedStorageOperatorApplication:async()=>{}},'./storage-receiver-attestation.mjs':{attestStorageReceiverRestore:async()=>({payload:structuredClone(payload),signature:'synthetic'})}});
 if(mode==='restore-and-attest')await assert.rejects(command.storageReceiverCommand([mode],{signal:cancellation.signal}),{name:'AbortError'});else assert.equal((await command.storageReceiverCommand([mode],{signal:cancellation.signal})).receiverRestoreVerified,true,'default restore keeps the existing cleanup contract');assert.equal(closed,true);
}
process.stdout.write(JSON.stringify({keyReadCancellation:true,keyCloseCancellation:true,keyCleared:true,closeFailurePreserved:true,receiverCleanupCancellation:true,defaultRestorePreserved:true})+'\n');

}
