import {stat,realpath} from 'node:fs/promises';
import {resolve,relative,isAbsolute,join} from 'node:path';
import {createLocalRecordingStorageProbe} from '../../postgres-recording-storage.mjs';

// Only the fixed readonly preload's process adapter installs this constructor.
// The JSON binding is not authority: every sample needs a fresh reply through
// its original parent's private IPC, whose nonce/epoch lease cannot be copied.
export function createCapacityRuntimeStorageProbe({store,pgDataDir,appPaths,binding}={}){
 if(!process.execArgv.includes('--import=/app/tools/capacity/runtime-source-loader.mjs')||typeof process.send!=='function'||binding?.schema!=='capacity-runtime-storage-v1'||pgDataDir!=='/capacity-material/postgres-data')throw Error('capacity_runtime_storage_not_owned');
 const send=process.send.bind(process),expected=JSON.stringify(binding);let sequence=0,pending=null,closed=false,fault=null,inFlight=null;
 const reject=()=>{closed=true;if(pending){clearTimeout(pending.timer);pending.reject(Error('capacity_runtime_storage_closed'));pending=null;}};
 const listener=message=>{if(message?.kind!=='capacity-runtime-storage-reply'||!pending||message.id!==pending.id)return;const row=pending;pending=null;clearTimeout(row.timer);if(message.ok!==true||JSON.stringify(message.value)!==expected)row.reject(Error('capacity_runtime_storage_binding_lost'));else row.resolve();};
 process.on('message',listener);process.once('disconnect',reject);
 const receipt=()=>new Promise((resolve,reject)=>{if(closed||!process.connected||pending){reject(Error('capacity_runtime_storage_unavailable'));return;}const id=++sequence,timer=setTimeout(()=>{if(pending?.id===id)pending=null;reject(Error('capacity_runtime_storage_timeout'));},1000);pending={id,timer,resolve,reject};send({kind:'capacity-runtime-storage-probe',id},error=>{if(!error||pending?.id!==id)return;clearTimeout(timer);pending=null;reject(Error('capacity_runtime_storage_unavailable'));});});
 const base=createLocalRecordingStorageProbe(store,{pgDataDir,appPaths,verifyDatabaseDirectory:async(identity,directory)=>{await receipt();if(identity.directory!==binding.hostDirectory||identity.identifier!==binding.cluster)return false;const info=await stat(directory,{bigint:true}),current=await store.get('SELECT pg_postmaster_start_time()::text started');return info.isDirectory()&&String(info.dev)===binding.device&&String(info.ino)===binding.inode&&current.started===binding.postmasterStarted;}});
 const probe=()=>{if(closed)return Promise.reject(Error('capacity_runtime_storage_closed'));return inFlight??=(async()=>{try{const samples=await base();await receipt();fault=null;return samples;}catch(error){fault='capacity_runtime_storage_unverified';throw error;}})().finally(()=>{inFlight=null;});};
 probe.status=()=>({...base.status(),source:'owned_capacity_namespace_filesystem',verified:!closed&&!fault&&base.status().verified,reason:closed?'capacity_runtime_storage_closed':fault??base.status().reason});
 probe.close=()=>{reject();process.off('message',listener);process.off('disconnect',reject);};return probe;
}

export function createCapacityRuntimeAccountHomeMapper(binding){
 if(!process.execArgv.includes('--import=/app/tools/capacity/runtime-source-loader.mjs')||binding?.schema!=='capacity-runtime-storage-v1'||typeof binding.runtimeDirectory!=='string'||!isAbsolute(binding.runtimeDirectory)||resolve(binding.runtimeDirectory)!==binding.runtimeDirectory)throw Error('capacity_runtime_material_not_owned');
 return async home=>{if(typeof home!=='string'||!isAbsolute(home)||resolve(home)!==home)throw Error('capacity_runtime_material_home');const suffix=relative(binding.runtimeDirectory,home);if(!suffix||isAbsolute(suffix)||suffix.split('/').includes('..'))throw Error('capacity_runtime_material_home');const root='/capacity-material/runtime',info=await stat(root,{bigint:true});if(String(info.dev)!==binding.runtimeDevice||String(info.ino)!==binding.runtimeInode)throw Error('capacity_runtime_material_home');const target=join(root,suffix);if(await realpath(target)!==target)throw Error('capacity_runtime_material_home');return target;};
}
