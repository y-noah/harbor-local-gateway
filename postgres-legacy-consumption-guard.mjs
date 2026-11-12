import {AppError} from './store.mjs';
import {createPostgresConsumptionPolicy,verifyConsumptionPolicyAvailability} from './postgres-consumption-policy.mjs';

// These legacy protocols do not implement source routing. An explicit source
// policy must never fall through to their older account selectors.
export function createPostgresLegacyConsumptionGuard(store,{clock=Date.now}={}){
 const policies=createPostgresConsumptionPolicy(store,{clock}),snapshots=new WeakMap();let initialization;
 const available=async()=>{
  if(!initialization)initialization=verifyConsumptionPolicyAvailability(store).catch(error=>{initialization=undefined;throw error;});
  return initialization;
 };
 async function check(member,{lock=false}={}){
  if(!await available())return null;
  const policy=await policies.read(member.key_id,{memberId:member.id,lock});
  if(policy.consumption_policy!=='inherit')throw new AppError(400,'此通道不支持Key消耗来源策略，请使用Responses入口','consumption_policy_protocol_unsupported');
  const snapshot=Object.freeze(policy);snapshots.set(snapshot,{memberId:member.id,keyId:member.key_id});return snapshot;
 }
 async function record(requestId,member,snapshot){
  if(snapshot===null)return;
  const identity=snapshots.get(snapshot);
  if(!identity||identity.memberId!==member.id||identity.keyId!==member.key_id)throw new AppError(409,'旧协议受理策略快照不一致','consumption_policy_conflict');
  await policies.recordAdmission(requestId,member.key_id,snapshot);
 }
 return {check,record};
}
