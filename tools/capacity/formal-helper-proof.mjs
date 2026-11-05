import {inspectCapacityFactoryHelperOwner,inspectCapacityFactoryProofLease} from './factory-context.mjs';
import {loadCapacityTrustedRuntimeArtifact,capacityRuntimeArtifactBinding,inspectCapacityRuntimeArtifact} from './runtime-artifact.mjs';
export const capacityProofContract='capacity-formal-proof-v1';
export const capacityProofPhases=Object.freeze(['allocation','pre-seed','final','restore']);
export const capacityProofBridges=Object.freeze(['deviceEnrollment']);
const capabilities=new WeakMap();
// Root-trusted harness hashes must cover the source actually fed to csc. A
// locally compiled executable hash on its own cannot authenticate its source.
export async function inspectCapacityHelperSources(evidence){
 const artifact=await loadCapacityTrustedRuntimeArtifact();await inspectCapacityRuntimeArtifact(artifact);const owner=capacityRuntimeArtifactBinding(artifact),expected=owner?.harness?.files;
 for(const source of evidence.sources){if(source.trustedHarnessSha256!==owner.trust.harness.manifestSha256||source.binarySha256!==owner.trust.tools.windowsHelper?.sha256)throw Error('capacity_helper_trusted_compiler_source_unproven');for(const name of ['clients/harbor-device/HarborDevice.cs','clients/harbor-device/CapacityDeliveryObservation.cs'])if(!expected?.[name]||source.sourceHashes[name]!==expected[name])throw Error('capacity_helper_trusted_source_unproven');}
 return artifact;
}
export async function prepareCapacityProof(binding,{phase}={}){
 const owner=await inspectCapacityFactoryHelperOwner(binding,{phase}),evidence=await owner.inspect(binding,{kind:'helper'}),artifact=await inspectCapacityHelperSources(evidence);inspectCapacityFactoryProofLease(binding);owner.assertLease(binding);const handle=Object.freeze(Object.create(null));capabilities.set(handle,{binding,phase,owner,artifact});return handle;
}
export async function inspectCapacityProof(handle,binding,{phase}={}){
 const row=capabilities.get(handle);if(!row||row.binding!==binding||row.phase!==phase||row.owner!==await inspectCapacityFactoryHelperOwner(binding,{phase}))throw Error('capacity_helper_capability_not_owned');const evidence=await row.owner.inspect(binding,{kind:'helper'});await inspectCapacityRuntimeArtifact(row.artifact);await inspectCapacityHelperSources(evidence);inspectCapacityFactoryProofLease(binding);row.owner.assertLease(binding);
 return Object.freeze({contract:capacityProofContract,phase,passed:true,missing:Object.freeze([]),softwareMissing:Object.freeze([]),evidence,handles:Object.freeze({deviceEnrollment:handle})});
}
