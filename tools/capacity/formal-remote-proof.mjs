import {createHash,X509Certificate} from 'node:crypto';
import {capacityRequestDriverRemoteSourceBinding} from './request-driver.mjs';
import {capacityBrowserSnapshotDescriptor} from './browser-driver-registration.mjs';
import {capacityTopologyOwnerBinding} from './formal-topology-owner.mjs';
import {loadCapacityTopologyConfiguration} from './nginx-source.mjs';
import {readCapacityRuntimeRegular} from './runtime-artifact.mjs';
import {inspectCapacityHelperWorkload} from './scope-stage-factory.mjs';
import {inspectCapacityNormalHelperWorkload} from './normal-execution.mjs';
export const capacityProofContract='capacity-formal-proof-v1';
export const capacityProofPhases=Object.freeze(['allocation','pre-seed','final','restore']);
export const capacityProofBridges=Object.freeze(['requestDriver','browserExecution']);
const capabilities=new WeakMap(),fail=()=>{throw Error('capacity_remote_capability_not_owned');};
const freeze=value=>{if(value&&typeof value==='object'){for(const next of Object.values(value))freeze(next);Object.freeze(value);}return value;};
async function inspectSource(binding,phase){
 const {inspectCapacityFactoryTopologyOwner,inspectCapacityFactoryProofLease}=await import('./factory-context.mjs'),owner=await inspectCapacityFactoryTopologyOwner(binding,{phase}),evidence=await owner.inspect(binding,{kind:'remote'}),driver=owner.getRemoteDriver(binding),source=capacityRequestDriverRemoteSourceBinding(driver);
 if(!source||evidence.remote?.offhost!==true||evidence.remote?.rootOwnedNative!==true)fail();inspectCapacityFactoryProofLease(binding);owner.assertLease(binding);return {owner,evidence,driver,source};
}
async function current(handle){const row=capabilities.get(handle);if(!row)fail();const source=await inspectSource(row.binding,row.phase);if(source.owner!==row.owner||source.driver!==row.driver||source.source!==row.source)fail();return row;}
async function helper(binding,phase){if(phase==='allocation')return null;const {inspectCapacityFactoryHelperOwner}=await import('./factory-context.mjs'),owner=await inspectCapacityFactoryHelperOwner(binding,{phase}),evidence=await owner.inspect(binding,{kind:'helper'});owner.assertLease(binding);return evidence;}
async function browserPolicy(row){
 const artifact=capacityTopologyOwnerBinding(row.owner)?.artifact;if(!artifact)fail();const config=await loadCapacityTopologyConfiguration(artifact),server=config.tls.files.find(file=>file.name==='server.pem');if(!server)fail();const bytes=await readCapacityRuntimeRegular(server.file,1048576,{trusted:true});if(createHash('sha256').update(bytes).digest('hex')!==server.sha256)fail();const cert=new X509Certificate(bytes),pin=createHash('sha256').update(cert.publicKey.export({type:'spki',format:'der'})).digest('base64'),template=row.source.transportBinding.browserActivationTemplate;
 if(!template||template.origin!==row.owner.getPublicOrigin()||template.serverSpkiHash!==pin)return null;return template;
}
// Software inspection carries no factory capability and authorizes no seed.
export async function inspectCapacityRemotePrerequisites(){
 const missing=[],softwareMissing=[];
 for(const [file,names] of [['./remote-browser-profile.mjs',['capacityRemoteBrowserActivationTemplate','capacityRemoteBrowserActivation']],['./remote-browser-service.mjs',['createCapacityRemoteBrowserService']],['./remote-request-driver.mjs',['createRemoteCapacityRequestDriver']]]){
  try{const module=await import(file);if(names.some(name=>typeof module[name]!=='function'))softwareMissing.push('formal_remote_consumer_contract');}catch{softwareMissing.push('formal_remote_consumer_load_failed');}
 }
 return freeze({missing,softwareMissing:[...new Set(softwareMissing)],evidence:{scope:'remote-consumer-software-only',factoryProofRequired:true,actualExecutionVerified:false}});
}
export async function prepareCapacityProof(binding,{phase}={}){const source=await inspectSource(binding,phase),handle=Object.freeze(Object.create(null));capabilities.set(handle,{binding,phase,...source,publication:null});return handle;}
export async function inspectCapacityProof(handle,binding,{phase}={}){
 const row=await current(handle);if(row.binding!==binding||row.phase!==phase)fail();const missing=[],softwareMissing=[],helperEvidence=await helper(binding,phase),template=await browserPolicy(row);
 if(typeof row.source.activateBrowserProfile!=='function'||typeof row.source.executionSnapshot!=='function')softwareMissing.push('formal_remote_execution_consumer_contract');
 if(!template)missing.push('formal_remote_approved_browser_activation_template_unavailable');
 if(helperEvidence&&phase!=='pre-seed'&&helperEvidence.modelVerified!==true)missing.push('formal_remote_current_helper_model_unproven');
 return freeze({contract:capacityProofContract,phase,passed:!missing.length&&!softwareMissing.length,missing,softwareMissing,evidence:{scope:phase==='allocation'?'root-native-offhost-consumers-ready-only':'same-factory-execution-consumers-ready-only',requestDriverVerified:true,helperExecutionVerified:helperEvidence?.modelVerified===true,modelExecutionVerified:false,browserExecutionVerified:false,actualWorkloadStillRequired:true},handles:{requestDriver:handle,browserExecution:handle}});
}
export async function activateCapacityRemoteBrowserProfile({capability,driver,publication}={}){
 const row=await current(capability);if(row.driver!==driver||!['final','restore'].includes(row.phase))fail();const {inspectCapacityBrowserPublication}=await import('./gateway-adapter.mjs');if(typeof inspectCapacityBrowserPublication!=='function')fail();
 const original=inspectCapacityBrowserPublication(publication),snapshot=original.snapshot;if(snapshot.laboratory!==row.binding.laboratory||snapshot.gatewayChild!==row.binding.gateway.child||snapshot.workerChild!==row.binding.worker.child||snapshot.requestDriver!==driver||snapshot.runLifetime!==row.binding.runLifetime)fail();
 const template=await browserPolicy(row);if(!template)throw Error('formal_remote_approved_browser_activation_template_unavailable');const descriptor=capacityBrowserSnapshotDescriptor(snapshot);if(descriptor.origin!==template.origin)fail();
 const cookies=snapshot.adminActors.slice(0,5).map(actor=>({name:'harbor_session',value:actor.headers.cookie.slice('harbor_session='.length)}));
 if(inspectCapacityBrowserPublication(publication).snapshot!==snapshot)fail();const profile=await row.source.activateBrowserProfile({bindingTag:descriptor.bindingTag,epoch:descriptor.epoch,cookies});await current(capability);
 if(inspectCapacityBrowserPublication(publication).snapshot!==snapshot||profile.bindingTag!==descriptor.bindingTag||profile.epoch!==descriptor.epoch||JSON.stringify(profile.cookieFingerprints)!==JSON.stringify(descriptor.cookieFingerprints))fail();row.publication=publication;return freeze({scope:'owned-factory-browser-profile-activation',profileId:profile.id,bindingTag:profile.bindingTag,epoch:profile.epoch,actualBrowserMeasurementsVerified:false});
}
// Only internal driver counters and authenticated terminal browser snapshots
// contribute. Caller DTOs, readiness and a profile activation cannot pass this.
export async function inspectCapacityRemoteConsumption(capability,{helperWorkload}={}){
 const row=await current(capability),publication=row.publication,observed=row.source.executionSnapshot?.();if(!observed)fail();const helperEvidence=await helper(row.binding,row.phase),missing=[],failures=[],status=row.driver.status();
 if(!row.publication)missing.push('formal_remote_browser_profile_not_activated');else{const {inspectCapacityBrowserPublication}=await import('./gateway-adapter.mjs');const {snapshot}=inspectCapacityBrowserPublication(row.publication),descriptor=capacityBrowserSnapshotDescriptor(snapshot),profile=observed.activatedProfile;if(!profile||profile.bindingTag!==descriptor.bindingTag||profile.epoch!==descriptor.epoch||JSON.stringify(profile.cookieFingerprints)!==JSON.stringify(descriptor.cookieFingerprints)||!row.source.transportBinding.browserProfiles.includes(profile))fail();}
 let workload=null;if(publication){const {inspectCapacityBrowserPublication}=await import('./gateway-adapter.mjs');const {snapshot}=inspectCapacityBrowserPublication(publication);workload=await inspectCapacityHelperWorkload(snapshot.helperWorkloadSource,{gateway:snapshot.gatewayChild,run:helperWorkload});missing.push(...workload.missing);failures.push(...workload.failures);}else missing.push('formal_current_helper_workload_unproven');
 if(helperEvidence?.modelVerified!==true)missing.push('formal_remote_actual_helper_model_unproven');
 if(!observed.browserMeasurements||!observed.completedBrowserAssessments)missing.push('formal_remote_actual_browser_measurement_unproven');
 if(observed.browserFailures||status.callbackFailures||status.callbackBudgetRejected||status.observationDropped)failures.push('formal_remote_observation_failed');
 if(status.pending||status.pendingCallbacks||status.pendingBrowserScenarios||status.pendingBrowserStarts)missing.push('formal_remote_observation_pending');
 await current(capability);if(row.publication!==publication||row.source.executionSnapshot().activatedProfile!==observed.activatedProfile)fail();if(publication){const {inspectCapacityBrowserPublication}=await import('./gateway-adapter.mjs');const {snapshot}=inspectCapacityBrowserPublication(publication);inspectCapacityNormalHelperWorkload(helperWorkload,{source:snapshot.helperWorkloadSource,gateway:snapshot.gatewayChild});}return freeze({passed:failures.length?false:missing.length?null:true,missing,failures,evidence:{scope:'same-factory-current-driver-authenticated-execution',modelExecutionVerified:workload?.passed===true,modelExecutionScope:'same-factory-current-helper-workload-stage-ledger',helperExecutionVerified:helperEvidence?.modelVerified===true,helperExecutionScope:'same-factory-current-cng-source-separate-from-offhost-driver',browserExecutionVerified:observed.browserMeasurements>0&&observed.completedBrowserAssessments>0,successfulModels:workload?.evidence.successfulModels??0,helperWorkload:workload?.evidence??null,browserMeasurements:observed.browserMeasurements,completedBrowserAssessments:observed.completedBrowserAssessments,profileEpoch:observed.activatedProfile?.epoch??null}});
}
