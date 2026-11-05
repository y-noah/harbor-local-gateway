import {fileURLToPath} from 'node:url';
import {resolve} from 'node:path';
import {createCapacityRemoteEndpoint} from './remote-endpoint.mjs';

const entry='/app/tools/capacity/remote-broker-runtime.mjs';
const fail=code=>{throw Error('capacity_remote_broker_'+code);};
// The root signer owns this private IPC. No signing key or host proc mount is
// present inside the native, read-only PID namespace.
export async function runCapacityRemoteBrokerRuntime(){
 if(fileURLToPath(import.meta.url)!==entry||resolve(process.argv[1]??'')!==entry||process.argv[2]!=='--owned-capacity-remote-broker'||!process.send||process.pid!==2||!process.execArgv.includes('--import=/app/tools/capacity/runtime-source-loader.mjs'))fail('entry');
 const {capacityRuntimeSourceObservation}=await import('/app/tools/capacity/runtime-source-loader.mjs');let endpoint,initializing,closed=false,closing,initialized=false,cleanupUnknown=false,next=0,ipcBytes=0;const pending=new Map(),maximumBytes=1048576;
 const send=message=>{if(!process.connected)fail('parent_closed');const bytes=Buffer.byteLength(JSON.stringify(message));if(bytes>262144||ipcBytes+bytes>maximumBytes)fail('ipc_budget');ipcBytes+=bytes;try{process.send(message,error=>{ipcBytes-=bytes;if(error)void close().catch(()=>{});});}catch(error){ipcBytes-=bytes;throw error;}};
 const attest=context=>new Promise((resolve,reject)=>{if(closed||pending.size>=1)return reject(Error('capacity_remote_broker_attestation_budget'));const id=++next,timer=setTimeout(()=>{pending.delete(id);reject(Error('capacity_remote_broker_attestation_timeout'));},10000);pending.set(id,{resolve,reject,timer});try{send({kind:'root-attestation-request',id,context});}catch(error){clearTimeout(timer);pending.delete(id);reject(error);}});
 const close=()=>closing??=(async()=>{closed=true;await initializing?.catch(()=>{});if(cleanupUnknown)fail('cleanup_unknown');for(const row of pending.values()){clearTimeout(row.timer);row.reject(Error('capacity_remote_broker_closed'));}pending.clear();await endpoint?.close();return {resourcesReleased:true};})();
 const reply=(id,ok,value)=>{try{send({id,ok,...(ok?{value}:{})});}catch{void close().catch(()=>{});}};
 process.on('message',message=>{
  if(message?.kind==='root-attestation-response'){const row=pending.get(message.id);if(row){pending.delete(message.id);clearTimeout(row.timer);message.ok===true?row.resolve(message.value):row.reject(Error('capacity_remote_broker_attestation_rejected'));}return;}
  if(!Number.isSafeInteger(message?.id)||!message.payload||typeof message.payload!=='object'||Array.isArray(message.payload))return;const {id,operation,payload}=message;
  const task=(async()=>{try{
   if(operation==='initialize'){if(initialized||closed)fail('already_initialized');initialized=true;if(Object.keys(payload).some(key=>!['runId','tls','agents','browser','limits','frames','leaseMs','handshakeTimeoutMs'].includes(key)))fail('configuration');endpoint=await createCapacityRemoteEndpoint({...payload,attest});reply(id,true,{origin:endpoint.origin,source:capacityRuntimeSourceObservation({limit:1})});}
   else if(operation==='status'){if(!endpoint||closed)fail('not_ready');reply(id,true,{endpoint:await endpoint.status(),source:capacityRuntimeSourceObservation({limit:1})});}
   else if(operation==='close'){const value=await close();reply(id,true,value);if(process.connected)process.disconnect();}
   else fail('command');
  }catch(error){if(operation==='initialize'&&error?.code==='capacity_remote_endpoint_cleanup_unknown')cleanupUnknown=true;reply(id,false);}})();if(operation==='initialize')initializing=task;
 });
 const shutdown=()=>{void close().then(()=>{if(process.connected)process.disconnect();},()=>{process.exitCode=1;if(process.connected)process.disconnect();});};process.once('disconnect',shutdown);for(const signal of ['SIGTERM','SIGINT'])process.once(signal,shutdown);
}
if(process.argv[2]==='--owned-capacity-remote-broker')await runCapacityRemoteBrokerRuntime();
