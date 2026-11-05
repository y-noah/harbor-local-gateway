import {inspectCapacityFactoryProofBinding} from './factory-context.mjs';
// Fixed producer contracts. Neither the CLI nor a caller-supplied DTO chooses
// a producer, supplies a capability, or turns a planned allocation into proof.
const definitions=Object.freeze([
 {id:'runtime',file:'./formal-runtime-proof.mjs',bridges:['processLaunch','immutableSources']},
 {id:'remote',file:'./formal-remote-proof.mjs',bridges:['requestDriver','browserExecution']},
 {id:'lan',file:'./formal-lan-proof.mjs',bridges:['networkBaseline']},
 {id:'proxy',file:'./formal-proxy-proof.mjs',bridges:['publicEntry','tlsMaterials']},
 {id:'clock',file:'./formal-clock-proof.mjs',bridges:['clockMapping']},
 {id:'helperClock',file:'./formal-helper-clock-proof.mjs',bridges:['helperClockMapping']},
 {id:'helper',file:'./formal-helper-proof.mjs',bridges:['deviceEnrollment']}
].map(row=>Object.freeze({...row,bridges:Object.freeze(row.bridges)})));
export const capacityFormalProofContract='capacity-formal-proof-v1';
export const capacityFormalProofPhases=Object.freeze(['allocation','pre-seed','final','restore']);
const records=new WeakMap();
const invalid=()=>{throw Error('capacity_factory_proof_not_owned');};

export function capacityFormalProofRequirements(){return definitions.map(row=>({...row,phases:[...capacityFormalProofPhases],bridges:[...row.bridges]}));}
async function loadProducer(definition){
 try{
  const producer=await import(definition.file);
  if(producer.capacityProofContract!==capacityFormalProofContract||typeof producer.prepareCapacityProof!=='function'||typeof producer.inspectCapacityProof!=='function'||!capacityFormalProofPhases.every(phase=>producer.capacityProofPhases?.includes(phase)))return {softwareMissing:['formal_'+definition.id+'_producer_contract']};
  const missing=definition.bridges.filter(bridge=>!producer.capacityProofBridges?.includes(bridge));return missing.length?{softwareMissing:missing.map(bridge=>'formal_'+definition.id+'_bridge_'+bridge)}:{producer};
 }catch(error){return {softwareMissing:['formal_'+definition.id+(error.code==='ERR_MODULE_NOT_FOUND'?'_producer_not_implemented':'_producer_load_failed')]};}
}
export async function inspectCapacityFormalSoftware(){
 const softwareMissing=[];for(const definition of definitions)softwareMissing.push(...(await loadProducer(definition)).softwareMissing??[]);return Object.freeze({contract:capacityFormalProofContract,softwareMissing:Object.freeze(softwareMissing),requirements:capacityFormalProofRequirements()});
}

// Binding is created in the actual factory and never travels over IPC. Each
// phase receives a new epoch binding; capabilities from another run/epoch are
// never imported. Producers must also verify their own private capability map.
export function createCapacityFormalProofConsumer({isOpen,getBinding}={}){
 if(typeof isOpen!=='function'||typeof getBinding!=='function')invalid();const handle=Object.freeze(Object.create(null));records.set(handle,{isOpen,getBinding,phase:null,rows:[]});return handle;
}
export async function collectCapacityFormalProofs(handle,phase){
 const record=records.get(handle);if(!record||!record.isOpen()||!capacityFormalProofPhases.includes(phase))invalid();
 const binding=record.getBinding();if(!binding||typeof binding!=='object'||!Object.isFrozen(binding)||binding.phase!==phase)invalid();await inspectCapacityFactoryProofBinding(binding,{phase});
 const missing=[],softwareMissing=[],evidence={},bridges=Object.create(null),rows=[];
 for(const definition of definitions){
  if(!record.isOpen()||record.getBinding()!==binding)invalid();const loaded=await loadProducer(definition);if(loaded.softwareMissing){softwareMissing.push(...loaded.softwareMissing);continue;}
  const producer=loaded.producer;
  try{
   const capability=await producer.prepareCapacityProof(binding,{phase});if(!record.isOpen()||record.getBinding()!==binding)invalid();
   const observed=await producer.inspectCapacityProof(capability,binding,{phase});if(!record.isOpen()||record.getBinding()!==binding)invalid();
   if(!observed||observed.contract!==capacityFormalProofContract||observed.phase!==phase||!Array.isArray(observed.missing)||!Array.isArray(observed.softwareMissing)||observed.passed!==true){missing.push('formal_'+definition.id+'_unproven');}
   else if(definition.bridges.some(name=>!observed.handles?.[name]))softwareMissing.push('formal_'+definition.id+'_bridge_handles_unavailable');
   else {rows.push({definition,producer,capability});for(const name of definition.bridges)bridges[name]=observed.handles[name];}
   missing.push(...(observed?.missing??[]));softwareMissing.push(...(observed?.softwareMissing??[]));evidence[definition.id]=observed?.evidence??null;
  }catch{missing.push('formal_'+definition.id+'_unavailable');}
 }
 if(!record.isOpen()||record.getBinding()!==binding)invalid();record.phase=phase;record.rows=rows;
 return Object.freeze({contract:capacityFormalProofContract,phase,passed:!missing.length&&!softwareMissing.length,missing:Object.freeze([...new Set(missing)]),softwareMissing:Object.freeze([...new Set(softwareMissing)]),evidence:Object.freeze(evidence),handles:Object.freeze(bridges)});
}

export async function recheckCapacityFormalProofs(handle,phase){
 const record=records.get(handle);if(!record||!record.isOpen()||record.phase!==phase||record.rows.length!==definitions.length)invalid();const binding=record.getBinding();await inspectCapacityFactoryProofBinding(binding,{phase});
 for(const row of record.rows){const observed=await row.producer.inspectCapacityProof(row.capability,binding,{phase});if(!record.isOpen()||record.getBinding()!==binding||observed?.contract!==capacityFormalProofContract||observed.phase!==phase||observed.passed!==true||observed.missing?.length||observed.softwareMissing?.length)invalid();}return true;
}
