import {createHash} from 'node:crypto';
import {PostgresStore} from '../../postgres-store.mjs';
import {usageWindow} from '../../store.mjs';
import {isOwnedLaboratory} from './owned-runtime.mjs';
import {datasetManifest,fixtureDatabase,verifyDataset} from './dataset.mjs';
import {targets} from './scenarios.mjs';

const bindings=new WeakMap(),profiles=new Set(['selfcheck','formal']);
const fail=code=>{throw new Error(code);};
const open=isOpen=>{try{return isOpen()===true;}catch{return false;}};
const digest=key=>createHash('sha256').update(key).digest('hex');

function matchesBodyDistribution(manifest,observed){
 const expected=new Map(manifest.bodyDistribution.map(bin=>[bin.bytes,0])),cycles=Math.floor(manifest.bodyRequests/100),tail=manifest.bodyRequests%100;
 // The existing generator assigns index*37 modulo 100. Account for its
 // complete periods and exact remainder without scanning millions of rows.
 for(let index=0;index<100;index++){
  const point=index*37%100;let upper=0;
  for(const bin of manifest.bodyDistribution){upper+=bin.percent;if(point<upper){expected.set(bin.bytes,expected.get(bin.bytes)+cycles+(index<tail?1:0));break;}}
 }
 const nonempty=[...expected].filter(([,requests])=>requests>0);
 return Array.isArray(observed)&&observed.length===nonempty.length&&new Set(observed.map(row=>row.source_bytes)).size===observed.length&&observed.every(row=>Number.isSafeInteger(row.requests)&&row.requests>0&&expected.get(row.source_bytes)===row.requests);
}

// Mint once during factory preparation, before model traffic changes the seed.
// A public plan/profile or copied verification DTO cannot issue this capability.
// This deliberately reruns the source verifier instead of trusting its caller's
// ordinary `verified` object. It is not a per-request or per-epoch full scan.
export async function createCapacityProfileBinding({laboratory,manifest,store,recordingKey,isOpen}={}){
 if(!isOwnedLaboratory(laboratory)||!(store instanceof PostgresStore)||!Buffer.isBuffer(recordingKey)||recordingKey.length!==32||typeof isOpen!=='function')fail('capacity_profile_binding_configuration');
 if(store.context.getStore())fail('capacity_profile_binding_nested_transaction');
 if(!open(isOpen))fail('capacity_profile_binding_closed');
 let canonical;try{canonical=datasetManifest(manifest);if(JSON.stringify(canonical)!==JSON.stringify(manifest))throw Error();}catch{fail('capacity_profile_manifest_invalid');}
 const database=fixtureDatabase(canonical),keyDigest=digest(recordingKey),owner=new PostgresStore({connection:laboratory.connection,poolSize:1});let cluster;
 try{cluster=(await owner.get('SELECT system_identifier::text id FROM pg_control_system()')).id;}catch{fail('capacity_profile_laboratory_unavailable');}finally{await owner.close();}
 if(!open(isOpen))fail('capacity_profile_binding_closed');
 const at=Date.now();let verified;
 try{
  verified=await store.transaction(async()=>{
   const actual=await store.get('SELECT current_database() name,(SELECT system_identifier::text FROM pg_control_system()) cluster');
   if(actual?.name!==database||actual.cluster!==cluster)fail('capacity_profile_source_mismatch');
   const marker=await store.get('SELECT run_id,owner_nonce,manifest_hash,recording_key_digest FROM harbor_capacity.fixture WHERE singleton=1');
   if(marker?.run_id!==canonical.runId||marker.owner_nonce!==canonical.ownerNonce||marker.manifest_hash!==canonical.manifestHash||marker.recording_key_digest!==keyDigest)fail('capacity_profile_source_mismatch');
   if(!open(isOpen))fail('capacity_profile_binding_closed');
   // verifyDataset only reads. The existing store's statement/idle timeouts
   // remain in force; timeout or a failed COMMIT never yields a binding.
   return verifyDataset(store,canonical,{recordingKey,at});
  },{level:'REPEATABLE READ',readOnly:true});
 }catch(error){if(!open(isOpen))fail('capacity_profile_binding_closed');if(['capacity_profile_source_mismatch','capacity_profile_binding_closed'].includes(error?.message))throw error;fail('capacity_profile_verification_unavailable');}
 if(!open(isOpen))fail('capacity_profile_binding_closed');
 if(usageWindow(Date.now()).start!==usageWindow(at).start)fail('capacity_profile_verification_stale');
 if(verified?.verified!==true||verified.manifestHash!==canonical.manifestHash||verified.source!=='isolated-synthetic-dataset')fail('capacity_profile_dataset_unverified');
 if(!matchesBodyDistribution(canonical,verified.bodyDistribution))fail('capacity_profile_body_distribution_mismatch');
 const formalDatasetVerified=canonical.profile==='formal'&&verified.formalScale===true&&verified.formalBodyFootprintVerified===true&&verified.bodyDistributionVerified===true&&canonical.metadata7d>=targets.metadata7d&&canonical.metadata30d>=targets.metadata30d&&canonical.bodyRequests===canonical.metadata30d&&verified.metadata7d>=targets.metadata7d&&verified.metadata30d>=targets.metadata30d&&verified.bodyRequests===canonical.bodyRequests&&verified.activeBodyRequests===canonical.bodyRequests;
 if(canonical.profile==='formal'&&!formalDatasetVerified)fail('capacity_profile_formal_dataset_incomplete');
 const binding=Object.freeze(Object.create(null));bindings.set(binding,{profile:canonical.profile,isOpen,formalDatasetVerified});return binding;
}

// Consumers receive binding only through the factory's private signed snapshot.
// This guard establishes profile/data identity, not resource or runtime fitness;
// all existing formal resource, storage, transport and execution gates still run.
export function assertCapacityProfileBinding(binding,requestedProfile){
 if(!profiles.has(requestedProfile))fail('capacity_profile_invalid');
 const owned=bindings.get(binding);if(!owned)fail('capacity_profile_binding_not_owned');
 if(!open(owned.isOpen))fail('capacity_profile_binding_closed');
 if(requestedProfile!==owned.profile)fail('capacity_profile_mismatch');
 if(requestedProfile==='formal'&&!owned.formalDatasetVerified)fail('capacity_profile_formal_dataset_incomplete');
 return Object.freeze({profile:owned.profile,datasetVerified:true,formalDatasetVerified:owned.formalDatasetVerified});
}
