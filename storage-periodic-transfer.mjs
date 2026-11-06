import {spawn} from 'node:child_process';
import {mkdir,lstat,realpath} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {performance} from 'node:perf_hooks';
import {loadPeriodicSshPolicy,buildPeriodicSshArguments,periodicSshExecutable} from './postgres-periodic-ssh-policy.mjs';
import {createPostgresBackupContext,closePostgresBackupContext,postgresBackupContext} from './postgres-backup-context.mjs';
import {loadAuthenticatedPeriodicBase} from './postgres-backup.mjs';
import {readPeriodicApplicationBackupState,authenticatePeriodicApplicationBackup,periodicApplicationBackupSource} from './postgres-periodic-application-backup.mjs';
import {periodicApplicationCaptureSource} from './postgres-periodic-application-materials.mjs';
import {loadPeriodicApplicationPolicy} from './postgres-periodic-application-policy.mjs';
import {createPeriodicTransferIndex,releasePeriodicTransferIndex} from './postgres-periodic-transfer-index.mjs';
import {loadPeriodicObjectStore,closePeriodicObjectStore} from './postgres-periodic-object-store.mjs';
import {PeriodicSshWire} from './postgres-periodic-ssh-wire.mjs';
import {servePeriodicSshSource,receivePeriodicSshTransfer} from './postgres-periodic-ssh-transfer.mjs';
import {acquireInstance} from './instance.mjs';
import {transferError} from './migration-artifacts.mjs';

const fail=code=>{throw transferError('periodic_ssh_'+code);},overlap=(a,b)=>a===b||a.startsWith(b+'/')||b.startsWith(a+'/');
const state=value=>({state:value,transportComplete:false,restoreVerified:false,offhostVerified:false,deploymentAllowed:false});

// The child is always the fixed executable with fixed-policy argv. stderr is
// discarded rather than relaying hostnames, identities or server diagnostics.
async function sendIndex(index,policy,{signal,timeoutMs}){
 signal.throwIfAborted();const child=spawn(periodicSshExecutable,buildPeriodicSshArguments(policy),{stdio:['pipe','pipe','ignore'],env:{PATH:'/usr/bin:/bin',LANG:'C'}});let spawnFailed=false,exit=null;
 const closed=new Promise(resolve=>{child.once('error',()=>{spawnFailed=true;});child.once('close',(code,terminated)=>{exit={code,terminated};resolve();});});
 const wire=new PeriodicSshWire(child.stdout,child.stdin,{signal,timeoutMs}),kill=()=>child.kill('SIGKILL');wire.signal.addEventListener('abort',kill,{once:true});if(wire.signal.aborted)kill();
 let completed=false;
 // This callback runs inside the source's archive lease. On both success and
 // failure it waits for actual process close before that lease can unwind.
 const finish=async()=>{if(!completed)kill();await closed;if(spawnFailed||exit.code!==0||exit.terminated)fail('process_failed');};
 try{return await servePeriodicSshSource(index,wire,{finish:async({complete})=>{completed=complete;await finish();}});}
 finally{kill();await closed;wire.signal.removeEventListener('abort',kill);wire.close();}
}

export async function sendLatestPeriodicBackup({signal}={}){
 const policy=await loadPeriodicSshPolicy();if(!policy.enabled)return state('periodic_ssh_disabled');
 const cancellation=new AbortController(),combined=AbortSignal.any([signal,cancellation.signal].filter(Boolean)),deadline=performance.now()+policy.timeoutMs,timer=setTimeout(()=>cancellation.abort(transferError('periodic_ssh_deadline')),policy.timeoutMs);let context,index,lease;
 try{
  combined.throwIfAborted();context=await createPostgresBackupContext(undefined,{signal:combined,deadline});combined.throwIfAborted();const record=postgresBackupContext(context),current=await loadPeriodicApplicationPolicy(),saved=await readPeriodicApplicationBackupState(context,{signal:combined});if(!saved.latestBackup)return state('periodic_ssh_not_ready');
  const {backupId,pointId}=saved.latestBackup;let proof;try{proof=await loadAuthenticatedPeriodicBase(context,backupId,{signal:combined});}catch(error){if(error.code==='ENOENT')return state('periodic_ssh_not_ready');throw error;}
  const authenticated=await authenticatePeriodicApplicationBackup(context,proof,pointId,{signal:combined}),capture=periodicApplicationCaptureSource(periodicApplicationBackupSource(authenticated).capture),roots=[record.artifactDirectory,record.archiveDirectory,record.restoreDirectory,record.expected.data_directory,...[capture.configuration,...(current.enabled?[current]:[])].flatMap(value=>value.materialRoots.flatMap(root=>[root.directory,...(root.databasePrefix?[root.databasePrefix]:[])]))].filter(value=>typeof value==='string');
  if(roots.some(root=>overlap(policy.indexDirectory,root)))fail('index_overlap');combined.throwIfAborted();lease=await acquireInstance(join(policy.indexDirectory,'.periodic-ssh-source'));
  const directory=join(policy.indexDirectory,backupId+'--'+pointId);await mkdir(directory,{mode:0o700}).catch(error=>{if(error.code!=='EEXIST')throw error;});const info=await lstat(directory);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==0||(info.mode&0o777)!==0o700||await realpath(directory)!==directory)fail('index_untrusted');
  index=await createPeriodicTransferIndex(context,proof,pointId,{directory,signal:combined});const remaining=Math.floor(deadline-performance.now());if(remaining<100)fail('deadline');return await sendIndex(index,policy,{signal:combined,timeoutMs:remaining});
 }finally{clearTimeout(timer);if(index)releasePeriodicTransferIndex(index);await lease?.release();if(context)await closePostgresBackupContext(context);}
}

export async function receiveFixedPeriodicBackup({signal}={}){
 signal?.throwIfAborted();const store=await loadPeriodicObjectStore();if(!store)return state('periodic_receiver_disabled');let wire;
 try{wire=new PeriodicSshWire(process.stdin,process.stdout,{signal,timeoutMs:3600000});return await receivePeriodicSshTransfer(store,wire);}finally{wire?.close();await closePeriodicObjectStore(store);}
}
export async function storagePeriodicTransferCommand(args,{signal}={}){if(!Array.isArray(args)||args.length!==1||!['send','receive'].includes(args[0]))fail('command_invalid');return args[0]==='send'?sendLatestPeriodicBackup({signal}):receiveFixedPeriodicBackup({signal});}
async function main(){const cancellation=new AbortController();for(const name of ['SIGINT','SIGTERM'])process.once(name,()=>cancellation.abort());const result=await storagePeriodicTransferCommand(process.argv.slice(2),{signal:cancellation.signal});if(process.argv[2]==='send')process.stdout.write(JSON.stringify(result)+'\n');}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))void main().catch(error=>{process.stderr.write(JSON.stringify({error:/^[a-z][a-z0-9_]+$/.test(error?.code??'')?error.code:'periodic_ssh_failed',deploymentAllowed:false})+'\n');process.exitCode=1;});
