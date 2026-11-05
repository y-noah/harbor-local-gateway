import {fileURLToPath} from 'node:url';
import {join,resolve} from 'node:path';
import {mkdir,writeFile,readFile,lstat,stat,realpath,readdir,readlink,chmod} from 'node:fs/promises';
import {fstatSync,closeSync} from 'node:fs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {randomUUID,randomBytes,X509Certificate,createHash} from 'node:crypto';
import tls from 'node:tls';
import {createOwnedRuntime,startOwnedCapacityChild,ownedProcessProof,terminateOwnedCapacityChild,createPrivatePostgres} from './owned-runtime.mjs';
import {PostgresStore} from '../../postgres-store.mjs';
import {passwordHash} from '../../auth.mjs';
import {datasetManifest,createDatasetDatabase,fixtureDatabase} from './dataset.mjs';
import {syntheticCertificates} from './selfcheck.mjs';
import {createControlledUpstream} from './controlled-upstream.mjs';
import {startCapacityProcess} from './process-adapter.mjs';
import {ownsListener} from './resource-assessment.mjs';
import {createCapacityRemoteLauncher,capacityRemoteLauncherBinding} from './remote-launcher.mjs';
import {connectCapacityRemoteTransport,capacityRemoteTransportBinding} from './remote-transport.mjs';

const nativeFactoryAuthorities=new WeakSet();
export const isOwnedCapacityFactoryOwnerAuthority=value=>nativeFactoryAuthorities.has(value);
const contexts=new WeakMap(),receipts=new WeakMap(),entry=fileURLToPath(import.meta.url);
const fail=code=>{throw Object.assign(new Error(code),{code});};
const scope='owned-loopback-preparation-only';
const missing=Object.freeze(['formal_dataset_not_created','formal_resources_unproven','remote_execution_ownership_unproven','immutable_runtime_unproven','cross_machine_clock_mapping_unproven','actual_lan_path_unproven','preserved_proxy_identity_unproven','helper_delivery_unproven']);
const safeError=error=>/^capacity_[a-z0-9_]+$/.test(error?.code??error?.message??'')?error.code??error.message:'capacity_preparation_operation_failed';
const sameFile=(a,b)=>a.dev===b.dev&&a.ino===b.ino&&a.uid===b.uid;
const exec=promisify(execFile);
const digest=value=>createHash('sha256').update(value).digest('hex');

async function entryInputs(){
 const node=await realpath(process.execPath),module=await realpath(entry),native=fileURLToPath(new URL('./preparation-entry.c',import.meta.url));for(const path of [node,module,native]){const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||await realpath(path)!==path||![0,process.getuid()].includes(info.uid)||info.mode&0o022)fail('capacity_preparation_entry_unproven');}return {node,module,nativeSha256:digest(await readFile(native)),nodeSha256:digest(await readFile(node)),moduleSha256:digest(await readFile(module))};
}
async function entryFile(path,maxBytes,mode){const info=await lstat(path);if(!info.isFile()||info.isSymbolicLink()||await realpath(path)!==path||info.uid!==process.getuid()||(info.mode&0o777)!==mode||info.size>maxBytes)fail('capacity_preparation_entry_unproven');return info;}

// Explicit tool preparation, not a step of createContext. Its designated output
// directory is a reusable artifact; interrupted output is never launchable.
export async function buildCapacityPreparationEntry({directory,signal}={}){
 if(process.platform!=='linux'||typeof directory!=='string'||await realpath(directory)!==directory)fail('capacity_preparation_entry_unproven');const identity=await lstat(directory);if(!identity.isDirectory()||identity.isSymbolicLink()||identity.uid!==process.getuid()||(identity.mode&0o777)!==0o700||(await readdir(directory)).length)fail('capacity_preparation_entry_unproven');const binary=join(directory,'entry'),manifest=join(directory,'manifest.json'),inputs=await entryInputs();
 try{
  await exec('/usr/bin/cc',['-std=c11','-O2','-Wall','-Wextra','-Werror','-DHARBOR_PREPARATION_NODE='+JSON.stringify(inputs.node),'-DHARBOR_PREPARATION_MODULE='+JSON.stringify(inputs.module),fileURLToPath(new URL('./preparation-entry.c',import.meta.url)),'-o',binary],{signal,timeout:30000,maxBuffer:65536});await chmod(binary,0o700);if(JSON.stringify(await entryInputs())!==JSON.stringify(inputs)||!sameFile(await lstat(directory),identity))fail('capacity_preparation_entry_changed');await entryFile(binary,16*1024**2,0o700);signal?.throwIfAborted();await writeFile(manifest,JSON.stringify({schema:'capacity-preparation-entry-v1',...inputs,binarySha256:digest(await readFile(binary))}),{mode:0o600,flag:'wx'});return {directory,binary};
 }catch(error){throw error;}
}
async function loadEntry(directory){
 try{
  if(process.platform!=='linux'||typeof directory!=='string'||await realpath(directory)!==directory)throw Error();const info=await lstat(directory);if(!info.isDirectory()||info.isSymbolicLink()||info.uid!==process.getuid()||(info.mode&0o777)!==0o700)throw Error();const binary=join(directory,'entry'),manifest=join(directory,'manifest.json');await entryFile(binary,16*1024**2,0o700);await entryFile(manifest,16384,0o600);const saved=JSON.parse(await readFile(manifest,'utf8')),inputs=await entryInputs();if(saved.schema!=='capacity-preparation-entry-v1'||Object.keys(inputs).some(key=>saved[key]!==inputs[key])||saved.binarySha256!==digest(await readFile(binary)))throw Error();return {binary};
 }catch{fail('capacity_preparation_entry_unproven');}
}
async function inheritedEntry(){
 const directoryFd=Number(process.env.HARBOR_PREPARATION_ENTRY_DIRECTORY_FD),binaryFd=Number(process.env.HARBOR_PREPARATION_ENTRY_BINARY_FD);if(!Number.isSafeInteger(directoryFd)||directoryFd<4||!Number.isSafeInteger(binaryFd)||binaryFd<4||directoryFd===binaryFd)fail('capacity_preparation_entry_unproven');
 const identity=fstatSync(directoryFd),binaryIdentity=fstatSync(binaryFd),directory=await readlink('/proc/self/fd/'+directoryFd),binary=await readlink('/proc/self/fd/'+binaryFd);
 if(!identity.isDirectory()||!binaryIdentity.isFile()||identity.uid!==process.getuid()||binaryIdentity.uid!==process.getuid()||identity.mode&0o077||binaryIdentity.mode&0o077||binary!==join(directory,'entry'))fail('capacity_preparation_entry_unproven');const loaded=await loadEntry(directory);if(loaded.binary!==binary||!sameFile(await lstat(directory),identity)||!sameFile(await lstat(binary),binaryIdentity))fail('capacity_preparation_entry_unproven');
 let closed=false;return {directory,close:async()=>{if(closed)return;closeSync(directoryFd);closeSync(binaryFd);closed=true;}};
}

// A fixed owner process is necessary because the private PG child has no IPC
// to the controller. The owner outlives controller death just long enough to
// stop its actual PG/gateway/worker/transport descendants and remove its lab.
export async function createCapacityPreparationContext(options={}){return createOwnerContext(options);}

export async function runCapacityOwnedFactory({job,...options}={}){
 const {validateCapacityFactoryJob}=await import('./factory-owner.mjs');const configuration=validateCapacityFactoryJob(job);const context=await createOwnerContext(options,configuration);try{return await context.run();}finally{await context.close();}
}

async function createOwnerContext(options={},factoryJob=null){
 if(!options||typeof options!=='object'||Object.keys(options).some(key=>!['entryDirectory','binDirectory','libraryDirectory','storageRoot','signal','readyTimeoutMs','livenessTimeoutMs','closeGraceMs','onProgress'].includes(key)))fail('capacity_preparation_configuration');
 const {signal,entryDirectory,onProgress=()=>{},readyTimeoutMs=60000,...configuration}=options;if(typeof onProgress!=='function')fail('capacity_preparation_configuration');if(!Number.isSafeInteger(readyTimeoutMs)||readyTimeoutMs<1000||readyTimeoutMs>120000)fail('capacity_preparation_configuration');signal?.throwIfAborted();
 for(const [value,min,max] of [[configuration.livenessTimeoutMs??1000,100,10000],[configuration.closeGraceMs??5000,100,30000]])if(!Number.isSafeInteger(value)||value<min||value>max)fail('capacity_preparation_configuration');
 const nativeEntry=await loadEntry(entryDirectory),runtime=createOwnedRuntime();signal?.throwIfAborted();const child=startOwnedCapacityChild(runtime,nativeEntry.binary,[],{role:'helper',cwd:fileURLToPath(new URL('../..',import.meta.url)),stdio:['ignore','pipe','pipe','ipc'],env:{NODE_OPTIONS:'',NODE_PATH:''}}),pending=new Map();let sequence=0,closed=false,closing=false,closePromise,phase='initializing';
 const command=(operation,payload={},timeoutMs=readyTimeoutMs)=>new Promise((resolve,reject)=>{
  if(closed||!child.connected||pending.size>=(['close','cancel-challenge'].includes(operation)?10:8))return reject(Object.assign(Error('capacity_preparation_owner_unavailable'),{code:'capacity_preparation_owner_unavailable'}));
  const id=++sequence,timer=setTimeout(()=>{pending.delete(id);reject(Object.assign(Error('capacity_preparation_owner_timeout'),{code:'capacity_preparation_owner_timeout'}));},timeoutMs);pending.set(id,{resolve,reject,timer});child.send({id,operation,payload},error=>{if(!error)return;clearTimeout(timer);pending.delete(id);reject(Object.assign(Error('capacity_preparation_owner_send_failed'),{code:'capacity_preparation_owner_send_failed'}));});
 });
 const retired=()=>{closed=true;phase='closed';for(const row of pending.values()){clearTimeout(row.timer);row.reject(Object.assign(Error('capacity_preparation_owner_lost'),{code:'capacity_preparation_owner_lost'}));}pending.clear();};child.once('exit',retired);child.once('error',retired);
 child.on('message',message=>{if(factoryJob&&message?.kind==='factory-progress'){try{const value=onProgress(message.value);if(value&&typeof value.then==='function')void value.catch(()=>{});}catch{}return;}const row=pending.get(message?.id);if(!row)return;pending.delete(message.id);clearTimeout(row.timer);if(message.ok)row.resolve(message.value);else row.reject(Object.assign(new Error(/^capacity_[a-z0-9_]+$/.test(message.code??'')?message.code:'capacity_preparation_operation_failed'),{code:message.code,...(message.preflight?{preflight:message.preflight}:{})}));});
 const isOpen=()=>{try{return !closed&&!closing&&ownedProcessProof(runtime,child).role==='helper';}catch{return false;}};
 const close=()=>closePromise??=(async()=>{
  closing=true;phase='closing';signal?.removeEventListener('abort',abort);let released=false;try{if(!closed)released=(await command('close',{},60000))?.resourcesReleased===true;}finally{await terminateOwnedCapacityChild(runtime,child,{graceMs:1000});await runtime.close();}if(!released)fail('capacity_preparation_cleanup_unknown');return {resourcesReleased:true};
 })();
 const abort=()=>{void close().catch(()=>{});};signal?.addEventListener('abort',abort,{once:true});
 try{
  const expectedScope=factoryJob?'owned-capacity-factory-job':scope;const initialized=await command(factoryJob?'initialize-factory':'initialize',factoryJob??configuration);signal?.throwIfAborted();if(!isOpen()||initialized?.scope!==expectedScope)fail('capacity_preparation_owner_unavailable');phase='idle';
  if(factoryJob)return Object.freeze({run:()=>command('run-factory',{},Math.min(2147483000,(factoryJob.target.runLifetimeMs??600000)+60000)),close});
  const context=Object.freeze({
   inspect:()=>command('inspect'),status:()=>({scope,phase,closed,closing,ownerPid:child.pid,entryDirectory:resolve(nativeEntry.binary,'..'),formalDataAllowed:false,missing:[...missing]}),close,
   challenge:async({signal:requestSignal,timeoutMs=10000}={})=>{
    if(!isOpen()||phase!=='idle')fail('capacity_preparation_challenge_already_used');if(!Number.isSafeInteger(timeoutMs)||timeoutMs<100||timeoutMs>30000)fail('capacity_preparation_configuration');requestSignal?.throwIfAborted();phase='pending';
    const cancel=()=>{void command('cancel-challenge').catch(()=>{});};requestSignal?.addEventListener('abort',cancel,{once:true});
    try{const result=await command('challenge',{timeoutMs},timeoutMs+10000);requestSignal?.throwIfAborted();if(!isOpen()||result?.scope!==scope||typeof result.receiptId!=='string')fail('capacity_preparation_receipt_unavailable');const receipt=Object.freeze(Object.create(null));receipts.set(receipt,{context,receiptId:result.receiptId,used:false});phase='issued';return receipt;}
    catch(error){if(!closed&&!closing)phase='failed';throw error;}finally{requestSignal?.removeEventListener('abort',cancel);}
   }
  });
  contexts.set(context,{isOpen,command});return context;
 }catch(error){await close().catch(()=>{});throw error;}
}

// Always ask the live owner again. A copied DTO or a previously returned
// summary cannot prove the gateway epoch is still running, nor permit seeding.
export async function inspectCapacityPreparationReceipt(context,receipt,{consume=false}={}){
 const owner=contexts.get(context),record=receipts.get(receipt);if(!owner||!owner.isOpen()||record?.context!==context||record.used)fail('capacity_preparation_receipt_not_owned');
 if(consume)record.used=true;const result=await owner.command('receipt',{receiptId:record.receiptId,consume});if(!owner.isOpen())fail('capacity_preparation_receipt_not_owned');return result;
}

async function nativeIdentity(pid){const raw=await readFile('/proc/'+pid+'/stat','utf8'),fields=raw.slice(raw.lastIndexOf(')')+2).split(' ');return {state:fields[0],parent:Number(fields[1]),started:fields[19]};}
const stopped=value=>['T','t','Z','X','x'].includes(value.state);

async function createInnerContext(configuration,signal,registerRevoke){
 const {livenessTimeoutMs=1000,closeGraceMs=5000,...laboratoryConfiguration}=configuration,manifest=datasetManifest({profile:'selfcheck'}),epoch=randomUUID(),encryptionKey=randomBytes(32),recordingKey=randomBytes(32),tlsProbes=new Set(),pgDescendants=new Map(),cleanupErrors=[];let lab,admin,database,gateway,worker,upstream,launcher,transport,transportIdentity,endpointTls,pgIdentity,gatewayIdentity,workerIdentity,endpointIdentity,executorIdentity,endpointPid,executorPid,pgExecutable,closed=false,closePromise,phase='initializing',active,wireEvidence,receiptRecord,observedRequests=0,revoked=false,revocationPromise,forceTimer,forcePromise,probeNumber=0,probeState=null;
 const bounded=async promise=>{let timer;try{return await Promise.race([promise,new Promise((_,reject)=>{timer=setTimeout(()=>reject(Error('capacity_preparation_liveness_unproven')),livenessTimeoutMs);})]);}finally{clearTimeout(timer);}};
 const wake=()=>{if(!wireEvidence)return;if(wireEvidence.invalid){wireEvidence.reject(Error('capacity_preparation_wire_invalid'));return;}if(wireEvidence.started===1&&wireEvidence.ended===1&&wireEvidence.settled===1)wireEvidence.resolve();};
 const sink=event=>{
  if(event?.kind==='gateway-request-start')observedRequests++;
  const row=wireEvidence;if(!row||event?.trace!==row.trace)return true;
  if(event.kind==='gateway-request-start'){row.started++;if(typeof event.wireId!=='string'||!/^[-a-f0-9]{36}$/.test(event.wireId)||event.signature!=='GET /ready'||row.wireId&&row.wireId!==event.wireId||event.credentials?.authorization!==null||event.credentials?.cookie!==null)row.invalid=true;row.wireId??=event.wireId;}
  else if(event.kind==='gateway-response-end'){row.ended++;if(event.wireId!==row.wireId||event.signature!=='GET /ready'||event.status!==200)row.invalid=true;}
  else if(event.kind==='gateway-handler-settled'){row.settled++;if(event.wireId!==row.wireId||event.signature!=='GET /ready'||event.outcome!=='resolved')row.invalid=true;}
  else if(['gateway-response-abort','gateway-handler-unobservable'].includes(event.kind))row.invalid=true;
  if(row.started>1||row.ended>1||row.settled>1)row.invalid=true;wake();return true;
 };
 const assertIdentity=()=>{
  if(closed||wireEvidence?.invalid||!gateway||!worker||!transportIdentity||capacityRemoteTransportBinding(transport)!==transportIdentity||!capacityRemoteLauncherBinding(launcher)?.isOpen())fail('capacity_preparation_epoch_lost');
  for(const [child,role] of [[lab.process,'database'],[gateway.child,'gateway'],[worker.child,'worker']])if(ownedProcessProof(lab.runtime,child).role!==role)fail('capacity_preparation_epoch_lost');
 };
 const endpointRoundTrip=()=>new Promise((resolve,reject)=>{
  const address=new URL(launcher.origin),socket=tls.connect({...endpointTls,host:'127.0.0.1',port:Number(address.port),rejectUnauthorized:true});tlsProbes.add(socket);let settled=false;const finish=ok=>{if(settled)return;settled=true;socket.destroy();ok?resolve():reject(Error('capacity_preparation_liveness_unproven'));};socket.once('close',()=>{tlsProbes.delete(socket);finish(false);});socket.once('error',()=>finish(false));socket.setTimeout(livenessTimeoutMs,()=>finish(false));socket.once('secureConnect',()=>finish(socket.authorized&&createHash('sha256').update(socket.getPeerCertificate().raw??Buffer.alloc(0)).digest('hex')===endpointTls.peerHash));
 });
 const assertLive=async()=>{
  const row={id:++probeNumber,gatewayReplied:false,workerReplied:false,endpointReplied:false};probeState=row;
  const checkRoles=(measurements=false)=>Promise.all([[lab.process.pid,pgIdentity],[gateway.child.pid,gatewayIdentity],[worker.child.pid,workerIdentity],...(measurements?[[endpointPid,endpointIdentity],[executorPid,executorIdentity]]:[])].map(async([pid,expected])=>{const current=await nativeIdentity(pid);if(stopped(current)||current.started!==expected.started)fail('capacity_preparation_liveness_unproven');}));
  assertIdentity();try{await bounded(checkRoles());await bounded(Promise.all([ownsListener(gateway.child,gateway.ready.tlsOrigin),gateway.command('status').then(value=>{row.gatewayReplied=true;return value;}),worker.command('status').then(value=>{row.workerReplied=true;return value;}),database.store.get('SELECT 1 AS alive'),capacityRemoteLauncherBinding(launcher).inspect().then(value=>{row.endpointReplied=true;return value;}),endpointRoundTrip()]));await bounded(checkRoles(true));}catch{fail('capacity_preparation_liveness_unproven');}assertIdentity();
 };
 const cancel=()=>{active?.controller.abort();};
 const rememberPgTree=async()=>{
  // The native entry makes this exact owner a subreaper. A direct adopted
  // child with the already observed PG executable inode is kernel-owned here,
  // even after its postmaster has died and the original tree no longer exists.
  const adopted=(await readFile('/proc/self/task/'+process.pid+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);
  for(const pid of adopted)try{const actual=await nativeIdentity(pid),binary=await stat('/proc/'+pid+'/exe');if(pid!==lab?.process?.pid&&actual.parent===process.pid&&pgExecutable&&sameFile(binary,pgExecutable))pgDescendants.set(pid,actual);}catch{}
  if(!lab?.process)return;
  if(lab.process.exitCode!==null||lab.process.signalCode!==null)return;ownedProcessProof(lab.runtime,lab.process);const queue=[lab.process.pid];
  for(let index=0;index<queue.length;index++){if(queue.length>256)fail('capacity_preparation_pg_tree_budget');const parent=queue[index];let children;try{children=(await readFile('/proc/'+parent+'/task/'+parent+'/children','utf8')).trim().split(/\s+/).filter(Boolean).map(Number);}catch{continue;}
   for(const pid of children){let actual;try{actual=await nativeIdentity(pid);}catch{continue;}if(actual.parent!==parent)continue;pgDescendants.set(pid,actual);queue.push(pid);}
  }
 };
 const signalRememberedPg=async signal=>{for(const [pid,proof] of pgDescendants){let current;try{current=await nativeIdentity(pid);}catch{continue;}if(current.started!==proof.started||['Z','X','x'].includes(current.state))continue;try{process.kill(pid,signal);}catch(error){if(error.code!=='ESRCH')throw error;}}};
 const resumePg=async()=>{await rememberPgTree();if(lab?.process)try{ownedProcessProof(lab.runtime,lab.process);process.kill(-lab.process.pid,'SIGCONT');}catch{}await signalRememberedPg('SIGCONT');};
 const forcePg=async()=>{
  await rememberPgTree();await signalRememberedPg('SIGCONT');if(lab?.process&&lab.process.exitCode===null&&lab.process.signalCode===null)try{await lab.runtime.crashDatabase(lab.process);}catch(error){if(lab.process.exitCode===null&&lab.process.signalCode===null)throw error;}
  await signalRememberedPg('SIGKILL');const deadline=performance.now()+5000;for(;;){let live=false;for(const [pid,proof] of pgDescendants)try{const actual=await nativeIdentity(pid);if(actual.started===proof.started&&!['Z','X','x'].includes(actual.state))live=true;}catch{}if(!live)return true;if(performance.now()>deadline)fail('capacity_preparation_pg_tree_cleanup_unknown');await new Promise(resolve=>setTimeout(resolve,10));}
 };
 const armForce=()=>{forceTimer=setTimeout(()=>{forceTimer=null;forcePromise=forcePg().then(stopped=>{if(!stopped)forcePromise=null;},()=>{cleanupErrors.push(true);});},closeGraceMs);};
 const beginRevoke=()=>{
  if(revoked)return;revoked=true;closed=true;phase='closed';cancel();for(const socket of tlsProbes)socket.destroy();revocationPromise=resumePg().catch(()=>{cleanupErrors.push(true);});armForce();
 };
 const close=()=>closePromise??=(async()=>{
  beginRevoke();await revocationPromise;const errors=cleanupErrors,step=async task=>{try{await task();}catch{errors.push(true);}};
  // Resume the actual owned PG group before any pool.end() can wait behind a
  // stopped query; independently force it down if graceful draining stalls.
  await step(resumePg);if(!forceTimer&&!forcePromise)armForce();
  await step(async()=>{if(transport&&!transport.status().closed)await transport.close();});await step(()=>launcher?.close());
  for(const child of [gateway?.child,worker?.child])try{if(child&&ownedProcessProof(lab.runtime,child))process.kill(child.pid,'SIGCONT');}catch{}
  await step(()=>gateway?.close());await step(()=>worker?.close());await step(()=>upstream?.close());await step(()=>database?.close());await step(()=>admin?.close());
  if(lab?.process&&lab.process.exitCode===null&&lab.process.signalCode===null)await step(()=>lab.runtime.stop(lab.process));await forcePromise;let treeStopped=false;try{await forcePg();treeStopped=true;}catch{errors.push(true);}clearTimeout(forceTimer);await forcePromise;if(treeStopped)await step(()=>lab?.close());else await step(()=>lab?.runtime.close());encryptionKey.fill(0);recordingKey.fill(0);if(errors.length)fail('capacity_preparation_cleanup_unknown');return {resourcesReleased:true};
 })();
 registerRevoke(beginRevoke);
 try{
  if(typeof laboratoryConfiguration.binDirectory==='string')pgExecutable=await stat(join(laboratoryConfiguration.binDirectory,'postgres'));
  lab=await createPrivatePostgres({...laboratoryConfiguration,signal});pgIdentity=await nativeIdentity(lab.process.pid);pgExecutable=await stat('/proc/'+lab.process.pid+'/exe');signal?.throwIfAborted();const root=join(lab.directory,'preparation'),dataDir=join(root,'data'),runtimeDir=join(root,'runtime');await mkdir(dataDir,{recursive:true,mode:0o700});await mkdir(runtimeDir,{mode:0o700});admin=new PostgresStore({connection:lab.connection});database=await createDatasetDatabase(admin,manifest,{laboratory:lab,connectionFor:name=>({connection:{...lab.connection,database:name}}),encryptionKey,recordingKey});signal?.throwIfAborted();
  await writeFile(join(dataDir,'key-encryption.key'),encryptionKey,{mode:0o600});await writeFile(join(dataDir,'recording-encryption.key'),recordingKey,{mode:0o600});const certificates=await syntheticCertificates(root),password=randomBytes(32).toString('hex');signal?.throwIfAborted();
  upstream=await createControlledUpstream({durationMs:30,firstContentMs:5,chunkIntervalMs:10});const location=await admin.get("SELECT current_setting('data_directory') directory,current_setting('unix_socket_directories') socket,current_setting('port')::int port"),processConfiguration={pgDataDir:location.directory,storageDatabase:{connection:{...lab.connection,host:location.socket,port:location.port,database:fixtureDatabase(manifest)}},database:{connection:{...lab.connection,database:fixtureDatabase(manifest)}},dataDir,runtimeDir,certificateDirectory:root,upstreamOrigin:upstream.origin,streamingStages:true,recordingEnabled:true,config:{port:8092,codexBinary:process.execPath,adminPasswordHash:passwordHash(password)}};
  gateway=await startCapacityProcess({runtime:lab.runtime,role:'gateway',configuration:processConfiguration,sink});worker=await startCapacityProcess({runtime:lab.runtime,role:'worker',configuration:processConfiguration});gatewayIdentity=await nativeIdentity(gateway.child.pid);workerIdentity=await nativeIdentity(worker.child.pid);signal?.throwIfAborted();
  const fingerprint=certificate=>createHash('sha256').update(new X509Certificate(certificate).raw).digest('hex'),tlsFiles={caFile:join(root,'ca.pem'),certFile:join(root,'server.pem'),keyFile:join(root,'server.key')};
  launcher=await createCapacityRemoteLauncher({runtime:lab.runtime,tls:{...tlsFiles,peerCertificateHash:fingerprint(certificates.client.cert)},agents:[{id:'ready',origin:gateway.ready.tlsOrigin,tls:{caFile:tlsFiles.caFile},routes:[{method:'GET',path:'/ready'}]}],limits:{maxInFlight:1,maxRequestTimeoutMs:30000,maxResponseBytes:4096}});
  endpointPid=capacityRemoteLauncherBinding(launcher).child.pid;endpointIdentity=await nativeIdentity(endpointPid);executorPid=(await capacityRemoteLauncherBinding(launcher).inspect()).executor.pid;executorIdentity=await nativeIdentity(executorPid);if(endpointIdentity.parent!==process.pid||executorIdentity.parent!==endpointPid)fail('capacity_preparation_epoch_lost');
  transport=await connectCapacityRemoteTransport({origin:launcher.origin,runId:launcher.runId,tls:{caFile:tlsFiles.caFile,certFile:join(root,'client.pem'),keyFile:join(root,'client.key'),peerCertificateHash:fingerprint(certificates.server.cert)}});transportIdentity=capacityRemoteTransportBinding(transport);endpointTls={...certificates.client,ca:certificates.ca,peerHash:fingerprint(certificates.server.cert)};await assertLive();phase='idle';
  return {
   close,cancel,
   inspect:async()=>({scope,phase,epoch,manifestHash:manifest.manifestHash,profile:manifest.profile,formalDataAllowed:false,missing:[...missing],observedRequests,livenessProbe:probeState?{...probeState}:null,pids:{owner:process.pid,database:lab.process.pid,gateway:gateway.child.pid,worker:worker.child.pid,endpoint:capacityRemoteLauncherBinding(launcher)?.child.pid},directory:lab.directory,...await database.store.get('SELECT (SELECT COUNT(*) FROM harbor.requests) requests,(SELECT COUNT(*) FROM harbor.recordings) recordings,metadata_cursor,body_cursor FROM harbor_capacity.fixture WHERE singleton=1')}),
   challenge:async({timeoutMs})=>{
    if(phase!=='idle')fail('capacity_preparation_challenge_already_used');phase='pending';const controller=new AbortController(),trace=randomUUID();let resolveWire,rejectWire;const wire=new Promise((resolve,reject)=>{resolveWire=resolve;rejectWire=reject;});wire.catch(()=>{});active=wireEvidence={trace,controller,started:0,ended:0,settled:0,wireId:null,invalid:false,resolve:resolveWire,reject:rejectWire};
    let operation,rejectAborted;const aborted=new Promise((_,reject)=>{rejectAborted=reject;});aborted.catch(()=>{});const awaitStage=promise=>Promise.race([promise,aborted]),abort=()=>{const error=Error('capacity_preparation_cancelled');rejectWire(error);rejectAborted(error);if(operation)void operation.cancel().catch(()=>{});},timer=setTimeout(()=>controller.abort(),timeoutMs);controller.signal.addEventListener('abort',abort,{once:true});
    try{
     await awaitStage(assertLive());controller.signal.throwIfAborted();operation=transport.request({agentId:'ready',path:'/ready',method:'GET',mode:'json',headers:{'x-capacity-trace':trace},timeoutMs});const [result]=await awaitStage(Promise.all([operation.result,wire]));if(!result.success||!result.httpEnded||result.status!==200||result.json?.ready!==true||!result.socketEvidence?.tlsAuthorized)fail('capacity_preparation_client_unverified');
     await awaitStage(assertLive());const metrics=await awaitStage(gateway.command('status'));await awaitStage(assertLive());controller.signal.throwIfAborted();if(active.invalid||gateway.observationStatus().parentEventDropped||gateway.observationStatus().pendingCallbacks||metrics.eventDropped||metrics.probe?.dropped||metrics.probe?.unobservableHandlers)fail('capacity_preparation_wire_invalid');
     receiptRecord={id:randomUUID(),epoch,manifestHash:manifest.manifestHash,used:false,traceHash:createHash('sha256').update(trace).digest('hex'),wireHash:createHash('sha256').update(active.wireId).digest('hex')};phase='issued';return {scope,receiptId:receiptRecord.id};
    }catch(error){if(!closed)phase='failed';throw error;}finally{clearTimeout(timer);controller.signal.removeEventListener('abort',abort);active=null;}
   },
   receipt:async({receiptId,consume})=>{if(!receiptRecord||receiptRecord.id!==receiptId||receiptRecord.used||phase!=='issued')fail('capacity_preparation_receipt_not_owned');await assertLive();if(consume)receiptRecord.used=true;return {scope,received:true,epoch,manifestHash:manifest.manifestHash,traceHash:receiptRecord.traceHash,wireHash:receiptRecord.wireHash,clientReady:true,wire:{started:1,ended:1,settled:1},formalDataAllowed:false,missing:[...missing]};}
  };
 }catch(error){try{await close();}catch{fail('capacity_preparation_cleanup_unknown');}throw error;}
}

async function ownerMain(){
 let entryResource;try{entryResource=await inheritedEntry();}catch{process.exitCode=1;if(process.connected)process.disconnect();return;}
 const factoryAuthority=Object.freeze(Object.create(null));nativeFactoryAuthorities.add(factoryAuthority);
 let inner,initialized=false,initializing,closePromise,cleanupUnknown=false,revokeInitialization;const controller=new AbortController();
 const reply=(id,ok,value,error)=>{const source=error?.preflight,safeCodes=values=>Array.isArray(values)?values.filter(value=>typeof value==='string'&&/^[a-zA-Z0-9_:-]{1,160}$/.test(value)).slice(0,128):[];const preflight=source?{eligible:source.eligible===true,allocationEligible:source.allocationEligible===true,seedEligible:false,phase:['before_allocation','allocation','pre-seed','final','restore'].includes(source.phase)?source.phase:'unknown',missing:safeCodes(source.missing),softwareMissing:safeCodes(source.softwareMissing)}:null;if(process.connected)process.send({id,ok,...(ok?{value}:{code:safeError(error),...(preflight?{preflight}:{})})});};
 const close=()=>closePromise??=(async()=>{controller.abort();revokeInitialization?.();await initializing;try{if(cleanupUnknown)fail('capacity_preparation_cleanup_unknown');return inner?await inner.close():{resourcesReleased:true};}finally{await entryResource.close();}})();
 process.on('message',message=>{const task=(async()=>{if(!Number.isSafeInteger(message?.id))return;try{
  let value;if(message.operation==='initialize'||message.operation==='initialize-factory'){if(initialized)fail('capacity_preparation_configuration');initialized=true;
   if(message.operation==='initialize-factory'){const {createInnerCapacityFactoryJob}=await import('./factory-owner.mjs');inner=await createInnerCapacityFactoryJob(message.payload,controller.signal,event=>{if(process.connected)process.send({kind:'factory-progress',value:event});},factoryAuthority);revokeInitialization=inner.cancel;value={scope:'owned-capacity-factory-job'};}
   else{inner=await createInnerContext(message.payload,controller.signal,revoke=>{revokeInitialization=revoke;if(controller.signal.aborted)revoke();});value={scope};}}
  else if(message.operation==='close'){value=await close();reply(message.id,true,value);if(process.connected)process.disconnect();return;}
  else if(!inner)fail('capacity_preparation_owner_unavailable');
  else if(message.operation==='run-factory'){if(typeof inner.run!=='function')fail('capacity_preparation_operation_invalid');value=await inner.run();}
  else if(message.operation==='inspect')value={...await inner.inspect(),entryDirectory:entryResource.directory};
  else if(message.operation==='challenge')value=await inner.challenge(message.payload);
  else if(message.operation==='cancel-challenge'){inner.cancel();value={acknowledged:true};}
  else if(message.operation==='receipt')value=await inner.receipt(message.payload);
  else fail('capacity_preparation_operation_invalid');reply(message.id,true,value);
 }catch(error){if(error.code==='capacity_preparation_cleanup_unknown')cleanupUnknown=true;reply(message.id,false,null,error);}})();if(['initialize','initialize-factory'].includes(message?.operation))initializing=task;void task;});
 const stop=()=>{void close().then(()=>{if(process.connected)process.disconnect();},()=>{process.exitCode=1;if(process.connected)process.disconnect();});};process.once('disconnect',stop);for(const signal of ['SIGINT','SIGTERM'])process.once(signal,stop);
 if(!process.connected)stop();
}
if(process.argv[2]==='--owned-capacity-preparation'&&process.argv[1]&&resolve(process.argv[1])===entry&&process.send)await ownerMain();
