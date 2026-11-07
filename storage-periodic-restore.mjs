import {resolve,join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {loadPeriodicObjectStore,closePeriodicObjectStore,materializePeriodicStoredBundle,periodicObjectStoreSettings} from './postgres-periodic-object-store.mjs';
import {periodicPostgresReceiverPolicy,createPeriodicPostgresBackupReceiver,closePostgresBackupReceiver} from './postgres-backup-receiver.mjs';
import {restorePostgresReceiver} from './storage-receiver.mjs';
import {attestStorageReceiverPeriodic} from './storage-receiver-periodic-attestation.mjs';
import {acquireInstance} from './instance.mjs';
import {transferError} from './migration-artifacts.mjs';

// Selection is limited to authenticated stored points; no paths, keys or trust
// fields can be supplied by a caller. The migration profile is never consulted.
export async function storagePeriodicRestoreCommand(args,{signal}={}){
 if(!Array.isArray(args)||args.length!==3||args[0]!=='verify'||!args.slice(1).every(value=>typeof value==='string'&&/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(value)))throw transferError('periodic_restore_arguments_invalid');
 signal?.throwIfAborted();const policy=await periodicPostgresReceiverPolicy();signal?.throwIfAborted();
 const disabled={state:'periodic_restore_disabled',restoreVerified:false,offhostVerified:false,deploymentAllowed:false};if(!policy.enabled)return disabled;
 const combined=AbortSignal.any([signal,AbortSignal.timeout(policy.processTimeoutMs)].filter(Boolean));let store,receiver,lease,result;
 try{
  store=await loadPeriodicObjectStore();if(!store)result=disabled;else{
  lease=await acquireInstance(join(periodicObjectStoreSettings(store).directory,'.periodic-restore'));
  const bundle=await materializePeriodicStoredBundle(store,args[1],args[2],{signal:combined});
  receiver=await createPeriodicPostgresBackupReceiver({storedBundle:bundle,signal:combined});if(!receiver)result=disabled;else{
  const receipt=await restorePostgresReceiver(receiver,{signal:combined});result=await attestStorageReceiverPeriodic(receipt,{signal:combined});}}
 }finally{try{if(receiver)await closePostgresBackupReceiver(receiver);}finally{try{if(store)await closePeriodicObjectStore(store);}finally{await lease?.release();}}}
 combined.throwIfAborted();return result;
}
async function main(){const cancellation=new AbortController();for(const name of ['SIGINT','SIGTERM'])process.once(name,()=>cancellation.abort());process.stdout.write(JSON.stringify(await storagePeriodicRestoreCommand(process.argv.slice(2),{signal:cancellation.signal}))+'\n');}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))void main().catch(error=>{process.stderr.write(JSON.stringify({error:/^[a-z][a-z0-9_]+$/.test(error?.code??'')?error.code:'periodic_restore_failed',deploymentAllowed:false})+'\n');process.exitCode=1;});
