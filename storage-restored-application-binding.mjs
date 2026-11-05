import {createHash} from 'node:crypto';
import {canonical,transferError} from './migration-artifacts.mjs';

const hash=value=>createHash('sha256').update(canonical(value)).digest('hex');

// Called only by the real restored runtime while its original DB, TLS and
// material files are live. This projection is data, never an activation permit.
export async function restoredApplicationBinding({store,manifest,activation,ca,signal}={}){
  signal?.throwIfAborted();
  const counts=await store.get('SELECT (SELECT COUNT(*)::text FROM harbor.employee_devices) devices,(SELECT COUNT(*)::text FROM harbor.device_credentials) credentials');
  if(!counts||!['devices','credentials'].every(key=>/^\d+$/.test(counts[key])))throw transferError('storage_restored_application_inventory');
  const owner=manifest.roles.find(row=>row.oid===manifest.roleGenerations.ownerOid),runtime=manifest.roles.find(row=>row.oid===manifest.roleGenerations.runtimeOid);
  if(!owner||!runtime||activation.run_id!==manifest.runId||activation.database_oid!==manifest.database.oid||activation.runtime!==runtime.name)throw transferError('storage_restored_application_binding');
  signal?.throwIfAborted();
  return {runId:activation.run_id,bindingDigest:activation.binding,targetDigest:manifest.targetDigest,database:structuredClone(manifest.database),roles:{owner:{name:owner.name,oid:owner.oid},runtime:{name:runtime.name,oid:runtime.oid}},materialDigest:hash(manifest.sourceMaterialManifestDigest),keyMaterialDigest:hash(manifest.materialKeys),deviceCaDigest:createHash('sha256').update(ca).digest('hex'),deviceInventory:counts.devices==='0'&&counts.credentials==='0'?{count:0,sha256:hash({devices:0,credentials:0}),mode:'authenticated-empty'}:null};
}
