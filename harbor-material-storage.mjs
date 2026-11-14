import {execFile} from 'node:child_process';
import {lstat} from 'node:fs/promises';

export const materialStorageGuardContract='harbor-material-storage-v1';

// The root-owned marker is on the system disk. Data-disk loss cannot turn the
// guard into its legacy, never-enabled state, and no caller environment disables it.
export async function requireMaterialMaintenanceInactive(){
 try{await lstat('/etc/harbor-material-maintenance.json');}catch(error){if(error.code==='ENOENT')return;throw error;}
 throw Object.assign(Error('Harbor存储正在维护'),{code:'harbor_material_maintenance_in_progress'});
}
export async function requireInstalledMaterialStorage(purpose,path,{signal}={}){
 await requireMaterialMaintenanceInactive();
 try{await lstat('/etc/harbor-data-paths.json');}catch(error){if(error.code==='ENOENT')return {enabled:false};throw error;}
 try{
  signal?.throwIfAborted();
  await new Promise((resolve,reject)=>execFile('/usr/bin/python3',['-I','-B','/usr/local/lib/harbor/harbor-material-storage.py','guard-path',purpose,path],{signal,timeout:20000,maxBuffer:16384,env:{PATH:'/usr/bin:/bin',LANG:'C'}},error=>error?reject(error):resolve()));
  return {enabled:true};
 }catch(error){if(signal?.aborted)throw signal.reason;throw Object.assign(Error('Harbor存储挂载未就绪'),{code:'harbor_material_storage_unavailable'});}
}
