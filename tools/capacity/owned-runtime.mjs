import {spawn,execFile} from 'node:child_process';
import {mkdtemp,mkdir,writeFile,readFile,rm,open,realpath,lstat} from 'node:fs/promises';
import {join,dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {randomUUID,randomBytes} from 'node:crypto';
import {promisify} from 'node:util';
import {setTimeout as delay} from 'node:timers/promises';
import {createPitrRehearsal,validateRollback,validateRehearsalServerSettings} from '../../postgres-recovery.mjs';
import {capacityStorageDirectory} from './storage-location.mjs';
import {restoreOwnedProcesses} from '../../postgres-backup-processes.mjs';
const routingControls=new WeakMap(),materialOwners=new WeakMap();
export const isOwnedMaterialRoot=(laboratory,root)=>materialOwners.get(root)===laboratory;
const exec=promisify(execFile),ownership=new WeakMap(),originalProofs=new WeakMap(),runtimeCapabilities=new WeakMap(),runtimes=new WeakSet(),proxies=new WeakSet(),laboratories=new WeakSet();
export const isOwnedRuntime=value=>runtimes.has(value);
export function ownedProcessProof(runtime,child){const proof=originalProofs.get(runtime);if(!proof)throw new Error('capacity_fault_runtime_not_owned');return proof(child);}
export function startOwnedCapacityChild(runtime,...args){const capability=runtimeCapabilities.get(runtime);if(!capability)throw new Error('capacity_fault_runtime_not_owned');return capability.start(...args);}
export function terminateOwnedCapacityChild(runtime,child,options){const capability=runtimeCapabilities.get(runtime);if(!capability)throw new Error('capacity_fault_runtime_not_owned');return capability.terminate(child,options);}
export const isOwnedDatabaseProxy=value=>proxies.has(value);
export const isOwnedLaboratory=value=>laboratories.has(value);
async function waitExit(child,timeoutMs=10000){if(!child.pid||child.exitCode!==null||child.signalCode!==null)return;await new Promise((resolve,reject)=>{const done=()=>{clearTimeout(timer);child.off('exit',done);resolve();},timer=setTimeout(()=>{child.off('exit',done);reject(new Error('capacity_owned_process_exit_timeout'));},timeoutMs);child.once('exit',done);});}
export function createOwnedRuntime(){
  const nonce=randomUUID(),children=new Set(),resources=new Set();let closed=false,closePromise;
  function owned(child){const row=ownership.get(child);if(!row||row.nonce!==nonce||!children.has(child)||row.initialPid!==child.pid||!Number.isSafeInteger(row.initialPid)||row.spawnFailed||child.exitCode!==null||child.signalCode!==null)throw new Error('capacity_fault_target_not_owned');return row;}
  function start(command,args,{role,cwd,env={},stdio=['ignore','pipe','pipe']}={}){if(closed||!['gateway','worker','database','upstream','helper'].includes(role)||typeof command!=='string'||!Array.isArray(args))throw new Error('capacity_owned_spawn_invalid');const child=spawn(command,args,{cwd,env:{...process.env,...env},stdio,detached:process.platform!=='win32'});children.add(child);ownership.set(child,{nonce,role,command:resolve(command),requestedAt:Date.now(),initialPid:child.pid});child.stdout?.resume();child.stderr?.resume();child.once('error',()=>{ownership.get(child).spawnFailed=true;});return child;}
  const signalChild=(child,signal)=>{const row=ownership.get(child);if(!row||row.nonce!==nonce||!Number.isSafeInteger(row.initialPid))throw new Error('capacity_fault_target_not_owned');try{process.kill(row.initialPid,signal);}catch(error){if(error.code!=='ESRCH')throw error;}};
  async function pause(child,durationMs,{signal}={}){const row=owned(child);if(row.role!=='worker'||process.platform==='win32'||!Number.isSafeInteger(durationMs)||durationMs<1||durationMs>600000)throw new Error('capacity_worker_pause_invalid');const started=Date.now();signalChild(child,'SIGSTOP');try{await delay(durationMs,undefined,{signal});}finally{if(child.exitCode===null&&child.signalCode===null)signalChild(child,'SIGCONT');}return {role:row.role,ownership:'task-spawned-process',durationMs:Date.now()-started};}
  async function stop(child,{abrupt=false}={}){const row=owned(child);if(abrupt&&row.role!=='gateway')throw new Error('capacity_abrupt_target_invalid');signalChild(child,abrupt?'SIGKILL':'SIGTERM');await waitExit(child);return {role:row.role,ownership:'task-spawned-process',abrupt};}
  async function terminate(child,{graceMs=1000}={}){
    const row=ownership.get(child);if(!row||row.nonce!==nonce||!children.has(child)||row.initialPid!==child.pid)throw new Error('capacity_fault_target_not_owned');
    if(process.platform!=='linux'||!Number.isSafeInteger(graceMs)||graceMs<0||graceMs>10000)throw new Error('capacity_owned_termination_invalid');
    if(!Number.isSafeInteger(row.initialPid)||child.exitCode!==null||child.signalCode!==null)return {role:row.role,leaderExited:true,descendantsVerified:false};
    const groupSignal=signal=>{try{process.kill(-row.initialPid,signal);}catch(error){if(error.code!=='ESRCH')throw error;}};
    groupSignal('SIGCONT');groupSignal('SIGTERM');
    try{await waitExit(child,graceMs);}catch{if(child.exitCode===null&&child.signalCode===null){groupSignal('SIGKILL');await waitExit(child,5000);}}
    // A dead numeric PGID may be reused. Descendant completion is verified by
    // the caller's private per-launch cgroup, never by signalling that old ID.
    return {role:row.role,ownership:'task-spawned-process',leaderExited:true,descendantsVerified:false};
  }
  async function crashHostChild(child){const row=owned(child);if(!['gateway','worker','database'].includes(row.role)||process.platform==='win32')throw new Error('capacity_host_crash_invalid');try{process.kill(-row.initialPid,'SIGKILL');}catch(error){if(error.code!=='ESRCH')throw error;}await waitExit(child);}
  async function crashDatabase(child){if(owned(child).role!=='database')throw new Error('capacity_database_crash_invalid');await crashHostChild(child);}
  function close(){return closePromise??=(async()=>{closed=true;const errors=[];for(const resource of resources)try{await resource.close();}catch(error){errors.push(error);}for(const child of children)if(child.pid&&child.exitCode===null&&child.signalCode===null)try{signalChild(child,'SIGCONT');signalChild(child,'SIGTERM');}catch(error){errors.push(error);}for(const child of children)try{await waitExit(child);}catch{if(child.pid&&child.exitCode===null&&child.signalCode===null){signalChild(child,'SIGKILL');await waitExit(child);}}if(errors.length)throw new Error('capacity_owned_cleanup_failed');})();}
  const runtime={start,pauseWorker:pause,stop,crashDatabase,crashHostChild,close,register:resource=>{if(closed||!resource||typeof resource.close!=='function')throw new Error('capacity_resource_invalid');resources.add(resource);},proof:child=>({...owned(child),nonce:undefined,command:undefined,pid:child.pid})};originalProofs.set(runtime,runtime.proof);runtimeCapabilities.set(runtime,Object.freeze({start,terminate,isStopped:()=>[...children].every(child=>!child.pid||child.exitCode!==null||child.signalCode!==null)}));runtimes.add(runtime);return runtime;
}
async function cleanupLaboratory({runtime,directory,operations,code,clearKey=()=>{},verifyStopped}){
  const errors=[];for(const operation of operations)try{await operation();}catch(error){errors.push(error);}let removed=false;
  try{if(runtimeCapabilities.get(runtime).isStopped()){await verifyStopped?.();await rm(directory,{recursive:true,force:true});removed=true;}}catch(error){errors.push(error);}finally{clearKey();}
  if(errors.length){const error=new AggregateError(errors,code,{cause:errors[0]});error.cleanupComplete=removed;throw error;}if(!removed)throw Error('capacity_laboratory_cleanup_unverified');
}
async function createOwnedDatabaseProxy(options={}){
  const {createOwnedDatabaseProxyHelper}=await import('./owned-database-proxy.mjs'),helper=await createOwnedDatabaseProxyHelper(options);
  const proxy=Object.freeze({address:helper.address,disconnect:helper.disconnect,restore:helper.restore,status:helper.status,close:helper.close});proxies.add(proxy);routingControls.set(proxy,helper.retarget);return proxy;
}
export async function createPrivatePostgres({binDirectory,libraryDirectory,signal,recovery,resourceAllocator,storageRoot}={}){
  if(resourceAllocator!==undefined&&!(await import('./resource-allocator.mjs')).isOwnedCapacityResourceAllocator(resourceAllocator))throw new Error('capacity_resource_allocator_not_owned');
  if(recovery!==undefined)return createRecoveryPostgres({binDirectory,libraryDirectory,signal,recovery,resourceAllocator,storageRoot});
  if(typeof binDirectory!=='string'||!resolve(binDirectory).endsWith('/postgresql/18/bin'))throw new Error('capacity_pg_binary_invalid');const root=await mkdtemp(join(await capacityStorageDirectory(storageRoot),'harbor-capacity-')),data=join(root,'data'),socket=join(root,'socket'),runtime=createOwnedRuntime(),password=randomBytes(32).toString('hex'),passwordFile=join(root,'database-password');let child,proxy,closePromise;
  const env=libraryDirectory?{LD_LIBRARY_PATH:libraryDirectory+(process.env.LD_LIBRARY_PATH?':'+process.env.LD_LIBRARY_PATH:'')}:{};
  const close=()=>closePromise??=cleanupLaboratory({runtime,directory:root,operations:[()=>proxy?.close(),()=>runtime.close()],code:'capacity_private_pg_cleanup_failed'});
  try{await mkdir(socket,{mode:0o700});await writeFile(passwordFile,password+'\n',{mode:0o600});await writeFile(join(root,'owner.json'),JSON.stringify({schema:'harbor-capacity-owner-v1',nonce:randomUUID(),created:Date.now()}),{mode:0o600});await exec(join(binDirectory,'initdb'),['-D',data,'-U','capacity_owner','--auth=scram-sha-256','--pwfile',passwordFile,'--no-locale','--encoding=UTF8'],{env:{...process.env,...env},maxBuffer:1048576,signal});const port=55482;child=await (resourceAllocator?resourceAllocator.start(runtime,join(binDirectory,'postgres'),['-D',data,'-k',socket,'-h','','-p',String(port),'-c','max_connections=100'],{role:'database',env}):runtime.start(join(binDirectory,'postgres'),['-D',data,'-k',socket,'-h','','-p',String(port),'-c','max_connections=100'],{role:'database',env}));
    const deadline=Date.now()+15000;for(;;){signal?.throwIfAborted();if(child.exitCode!==null)throw new Error('capacity_private_pg_start_failed');try{await exec(join(binDirectory,'pg_isready'),['-h',socket,'-p',String(port),'-U','capacity_owner'],{env:{...process.env,...env},maxBuffer:4096});break;}catch{if(Date.now()>=deadline)throw new Error('capacity_private_pg_ready_timeout');await delay(50,undefined,{signal});}}
    proxy=await createOwnedDatabaseProxy({runtime,targetPath:join(socket,'.s.PGSQL.'+port),ownedDirectory:root});const lab={directory:root,runtime,process:child,proxy,connection:{host:'127.0.0.1',port:proxy.address.port,user:'capacity_owner',password,database:'postgres'},close};Object.freeze(lab.connection);Object.freeze(lab);laboratories.add(lab);return lab;
  }catch(error){try{await close();}catch(cleanup){throw new AggregateError([error,cleanup],'capacity_private_pg_initialize_cleanup_failed',{cause:error});}throw error;}
}

// The recovery fault must stop the actual backend tree, including PostgreSQL
// sessions outside the postmaster process group, before source files disappear.
// The nonce is only an observation marker; it grants no backup capability.
async function crashRecoveryProcessTree(runtime,child,owner){
  ownedProcessProof(runtime,child);let killed=false,drained=false;
  const send=async signal=>{const rows=(await restoreOwnedProcesses(owner,{checkUnknown:false})).map(({pid,uid,start})=>({pid,uid,start}));if(!rows.length)return;const frame=JSON.stringify({signal,rows});if(Buffer.byteLength(frame)>524288)throw Error('capacity_recovery_tree_signal_capacity');const operation=exec('/usr/bin/python3',['-I','-B',fileURLToPath(new URL('./process-signal.py',import.meta.url))],{env:{PATH:'/usr/bin:/bin'},timeout:5000,maxBuffer:4096});operation.child.stdin.on('error',()=>{});operation.child.stdin.end(frame);try{await operation;}catch{throw Error('capacity_recovery_tree_signal_unverified');}};
  try{
    await restoreOwnedProcesses(owner);await send('SIGSTOP');const freezeDeadline=Date.now()+5000;
    for(;;){const rows=await restoreOwnedProcesses(owner);if(rows.every(row=>row.state==='T'||row.state==='t'))break;if(Date.now()>=freezeDeadline)throw Error('capacity_recovery_tree_freeze_timeout');await send('SIGSTOP');await delay(10);}
    killed=true;await send('SIGKILL');await waitExit(child,5000);const deadline=Date.now()+5000;
    while((await restoreOwnedProcesses(owner)).length){if(Date.now()>=deadline)throw Error('capacity_recovery_tree_exit_unverified');await delay(10);}drained=true;
  }finally{if(!drained)await send(killed?'SIGKILL':'SIGCONT');}
}

async function createRecoveryPostgres({binDirectory,libraryDirectory,signal,recovery,resourceAllocator,storageRoot}){
  if(typeof binDirectory!=='string'||!resolve(binDirectory).endsWith('/postgresql/18/bin')||!recovery||Object.keys(recovery).some(key=>!['backupKey','serverSettings'].includes(key))||!Buffer.isBuffer(recovery.backupKey)||recovery.backupKey.length!==32)throw new Error('capacity_recovery_configuration');
  const serverSettings=validateRehearsalServerSettings(recovery.serverSettings);
  signal?.throwIfAborted();const directory=await mkdtemp(join(await capacityStorageDirectory(storageRoot),'harbor-capacity-recovery-')),runtime=createOwnedRuntime(),key=Buffer.from(recovery.backupKey),password=randomBytes(32).toString('hex'),children=new Map(),env=libraryDirectory?{LD_LIBRARY_PATH:libraryDirectory+(process.env.LD_LIBRARY_PATH?':'+process.env.LD_LIBRARY_PATH:'')}:{};const materialRoots=new Map(),processOwners=new Map();let rehearsal,proxy,activeChild,activeData,activeSocket,closed=false,failed=false,recovered=false,closePromise;
  const inside=async path=>{const actual=await realpath(path);if(!actual.startsWith(directory+'/'))throw new Error('capacity_recovery_path_not_owned');return actual;};
  const alive=()=>{if(closed)throw new Error('capacity_recovery_closed');signal?.throwIfAborted();};
  const close=()=>{if(!closePromise){closed=true;closePromise=cleanupLaboratory({runtime,directory,operations:[()=>proxy?.close(),async()=>{for(const owner of processOwners.values())await restoreOwnedProcesses(owner,{checkUnknown:false});},()=>runtime.close(),()=>rehearsal?.close()],code:'capacity_recovery_cleanup_failed',clearKey:()=>key.fill(0),verifyStopped:async()=>{for(const owner of processOwners.values())if((await restoreOwnedProcesses(owner)).length)throw Error('capacity_recovery_tree_exit_unverified');}});}return closePromise;};
  try{
    rehearsal=await createPitrRehearsal({directory,key,pgBinDir:binDirectory,libraryDirectory,signal,serverSettings,adminCredentials:{user:'capacity_owner',password},launchPostgres:async({binary,data,log})=>{signal?.throwIfAborted();const output=await open(log,'ax',0o600);try{const nonce=randomBytes(32).toString('hex'),options={role:'database',env:{...env,HARBOR_BACKUP_RESTORE_TOKEN:nonce},stdio:['ignore',output.fd,output.fd]},child=await (resourceAllocator?resourceAllocator.start(runtime,binary,['-D',data],options):runtime.start(binary,['-D',data],options));children.set(resolve(data),child);processOwners.set(child,{pid:child.pid,nonce,uid:process.getuid(),data,protectedRoot:data,members:new Map(),start:null});return child;}finally{await output.close();}}});
    signal?.throwIfAborted();
    activeData=rehearsal.primary;activeSocket=rehearsal.adminOptions.connection.host;activeChild=children.get(resolve(activeData));ownedProcessProof(runtime,activeChild);await restoreOwnedProcesses(processOwners.get(activeChild));const port=rehearsal.adminOptions.connection.port;
    if(Number((await readFile(join(activeData,'postmaster.pid'),'utf8')).split('\n')[0])!==activeChild.pid)throw new Error('capacity_recovery_process_not_owned');
    proxy=await createOwnedDatabaseProxy({runtime,targetPath:join(activeSocket,'.s.PGSQL.'+port),ownedDirectory:directory});const connection=Object.freeze({...rehearsal.adminOptions.connection,host:'127.0.0.1',port:proxy.address.port});
    const control=Object.freeze({
      originalDataDirectory:activeData,
      baseBackup:async options=>{alive();if(failed||recovered)throw new Error('capacity_recovery_source_unavailable');await inside(options.artifactDirectory);for(const root of options.additionalRoots??[])await inside(root.source);return rehearsal.baseBackup(options);},
      mark:async()=>{alive();if(failed||recovered)throw new Error('capacity_recovery_source_unavailable');return rehearsal.mark();},
      flushWal:async()=>{alive();if(failed||recovered)throw new Error('capacity_recovery_source_unavailable');return rehearsal.flushWal();},
      faultSource:async()=>{alive();if(failed||recovered)throw new Error('capacity_recovery_source_unavailable');await proxy.disconnect();await crashRecoveryProcessTree(runtime,activeChild,processOwners.get(activeChild));await rehearsal.stopPrimary();failed=true;return {failedAt:Date.now(),processTreeDrained:true};},
      recover:async options=>{alive();if(!failed||recovered)throw new Error('capacity_recovery_state_invalid');await inside(options.artifactDirectory);await inside(options.destination);const result=await rehearsal.recover(options);const verified=await validateRollback({recovery:result});if(!verified.storageCompatible)throw new Error('capacity_recovery_storage_invalid');const data=join(options.destination,'pgdata'),child=children.get(resolve(data));ownedProcessProof(runtime,child);await restoreOwnedProcesses(processOwners.get(child));if(Number((await readFile(join(data,'postmaster.pid'),'utf8')).split('\n')[0])!==child.pid)throw new Error('capacity_recovery_process_not_owned');const socket=result.database.connection.host;await inside(socket);await routingControls.get(proxy)(join(socket,'.s.PGSQL.'+port));activeChild=child;activeData=data;activeSocket=socket;recovered=true;return result;}
    });
    signal?.throwIfAborted();const lab=Object.freeze({directory,runtime,proxy,connection,get process(){return activeChild;},get dataDirectory(){return activeData;},get socketDirectory(){return activeSocket;},databasePort:port,recovery:control,createMaterialRoot:async id=>{alive();if(!/^[a-z][a-z0-9-]{0,63}$/.test(id)||id==='pgdata'||materialRoots.has(id))throw new Error('capacity_material_root_invalid');const path=join(directory,'materials-'+id);await mkdir(path,{mode:0o700});const token=Object.freeze({id,path});materialOwners.set(token,lab);materialRoots.set(id,token);return token;},get materialRoots(){return Object.freeze([...materialRoots.values()]);},close});laboratories.add(lab);return lab;
  }catch(error){try{await close();}catch(cleanup){throw new AggregateError([error,cleanup],'capacity_recovery_initialize_cleanup_failed',{cause:error});}throw error;}
}
