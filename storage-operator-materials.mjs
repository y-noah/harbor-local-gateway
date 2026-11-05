import {isAbsolute,join,resolve,sep} from 'node:path';
import {transferError} from './migration-artifacts.mjs';

const fail=()=>{throw transferError('storage_operator_candidate_material_coverage');};
const absolute=value=>typeof value==='string'&&isAbsolute(value)&&resolve(value)===value&&!/[\r\n,]/.test(value);

// This describes explicit backup coverage, not a certificate, restore or
// activation capability. The capture/restore path still verifies real files.
export function storageOperatorCandidateMaterialRoots({sourceDataDir,fileRoots,candidate}={}){
  if(!absolute(sourceDataDir)||!absolute(candidate?.secretsDirectory)||!Array.isArray(fileRoots)||fileRoots.length<3)fail();
  const roots=new Map();
  for(const root of fileRoots){
    if(!root||!/^[a-z][a-z0-9-]{0,79}$/.test(root.id??'')||roots.has(root.id)||!absolute(root.source))fail();
    for(const other of roots.values())if(root.source===other.source||root.source.startsWith(other.source+sep)||other.source.startsWith(root.source+sep))fail();
    roots.set(root.id,{...root});
  }
  const data=roots.get('data'),runtime=roots.get('runtime'),secrets=roots.get('secrets');
  if(data?.source!==sourceDataDir||!runtime||runtime.destinationPrefix!=='/app/runtime'||runtime.databasePrefix!==undefined&&!absolute(runtime.databasePrefix)||secrets?.source!==candidate.secretsDirectory)fail();
  return Object.freeze(['data','runtime','secrets'].map(id=>Object.freeze({id,source:roots.get(id).source,destination:id==='secrets'?'/run/secrets':'/app/'+id})));
}

export function storageOperatorCandidateMaterialMounts(configuration,materials){
  if(!absolute(materials))fail();
  return storageOperatorCandidateMaterialRoots(configuration).flatMap(root=>['--mount','type=bind,src='+join(materials,root.id)+',dst='+root.destination+',readonly']);
}
