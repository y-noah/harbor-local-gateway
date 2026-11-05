import {createHmac,timingSafeEqual} from 'node:crypto';
import {artifactKey,canonical,transferError} from './migration-artifacts.mjs';
const sign=(payload,key)=>createHmac('sha256',artifactKey(key)).update('harbor-storage-operator-status-v1\0').update(canonical(payload)).digest('hex');
// This batch cannot prove actual off-host backup or deployment consumption.
// Its authenticated status is deliberately not an activation capability.
export function pendingStorageOperatorReceipt(binding,key){
  const payload={version:1,...binding,status:'pending',deploymentAllowed:false,missing:[...new Set([...(binding.missing??[]),'existing_cluster_backup_verification','offhost_restore_verification','deployment_activation_consumer'])]};
  return {payload,signature:sign(payload,key)};
}
export function verifyStorageOperatorReceipt(receipt,key,{bindingDigest,requireActivation=false}={}){
  const signature=receipt?.signature;if(typeof signature!=='string'||!/^[a-f0-9]{64}$/.test(signature)||!timingSafeEqual(Buffer.from(signature,'hex'),Buffer.from(sign(receipt.payload,key),'hex')))throw transferError('storage_operator_receipt_invalid');
  if(receipt.payload.version!==1||receipt.payload.bindingDigest!==bindingDigest||receipt.payload.status!=='pending'||receipt.payload.deploymentAllowed!==false)throw transferError('storage_operator_receipt_binding');
  if(requireActivation)throw transferError('storage_operator_activation_not_available');return receipt.payload;
}
