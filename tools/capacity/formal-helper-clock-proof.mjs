import {inspectCapacityFactoryHelperOwner,inspectCapacityFactoryProofLease} from './factory-context.mjs';
import {inspectCapacityHelperSources} from './formal-helper-proof.mjs';
import {inspectCapacityRuntimeArtifact} from './runtime-artifact.mjs';
export const capacityProofContract='capacity-formal-proof-v1';
export const capacityProofPhases=Object.freeze(['allocation','pre-seed','final','restore']);
export const capacityProofBridges=Object.freeze(['helperClockMapping']);
const capabilities=new WeakMap();
// This is only the QPC/controller part of the complete clock requirement.
// The catalogue retains its independent remote/Linux clock producer contract.
export async function prepareCapacityProof(binding,{phase}={}){
 const owner=await inspectCapacityFactoryHelperOwner(binding,{phase}),evidence=await owner.inspect(binding,{kind:'clock'}),artifact=await inspectCapacityHelperSources(evidence);inspectCapacityFactoryProofLease(binding);owner.assertLease(binding);const handle=Object.freeze(Object.create(null));capabilities.set(handle,{binding,phase,owner,artifact});return handle;
}
export async function inspectCapacityProof(handle,binding,{phase}={}){
 const row=capabilities.get(handle);if(!row||row.binding!==binding||row.phase!==phase||row.owner!==await inspectCapacityFactoryHelperOwner(binding,{phase}))throw Error('capacity_helper_clock_capability_not_owned');const evidence=await row.owner.inspect(binding,{kind:'clock'});await inspectCapacityRuntimeArtifact(row.artifact);await inspectCapacityHelperSources(evidence);inspectCapacityFactoryProofLease(binding);row.owner.assertLease(binding);
 return Object.freeze({contract:capacityProofContract,phase,passed:true,missing:Object.freeze([]),softwareMissing:Object.freeze([]),evidence,handles:Object.freeze({helperClockMapping:handle})});
}
