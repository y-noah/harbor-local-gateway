import {createCapacityTopologyOwner,capacityTopologyOwnerBinding} from './formal-topology-owner.mjs';
import {runCapacityFactoryInternalObservation} from './observation-adapter.mjs';
import {createCapacityHelperOwner,capacityHelperOwnerBinding} from './helper-owner-context.mjs';
import {isOwnedCapacityFactoryOwnerAuthority} from './preparation-context.mjs';
import {randomUUID,createHash,X509Certificate} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {join} from 'node:path';
import {isOwnedLaboratory,ownedProcessProof,terminateOwnedCapacityChild} from './owned-runtime.mjs';
import {fixtureDatabase} from './dataset.mjs';
import {startCapacityProcess} from './process-adapter.mjs';
import {ownsListener} from './resource-assessment.mjs';
import {createCapacityRemoteLauncher,capacityRemoteLauncherBinding} from './remote-launcher.mjs';
import {connectCapacityRemoteTransport} from './remote-transport.mjs';
import {createCapacityFormalProofConsumer,collectCapacityFormalProofs,recheckCapacityFormalProofs} from './formal-proof-catalogue.mjs';

const internalAuthorities=new WeakMap(),ownedProofBindings=new WeakMap();
export function isCapacityFactoryInternalObservationAuthority(value){const verify=internalAuthorities.get(value);return verify?.()===true;}
export function inspectCapacityFactoryProofLease(binding){const record=ownedProofBindings.get(binding);if(!record||!record.isCurrent()||!isOwnedCapacityFactoryOwnerAuthority(record.ownerAuthority))throw Error('capacity_factory_proof_not_owned');record.signal.throwIfAborted();binding.runLifetime.assertCoverage();return Object.freeze({phase:binding.phase,epoch:binding.epoch});}
export async function inspectCapacityFactoryProofBinding(binding,{phase}={}){const record=ownedProofBindings.get(binding);if(!record||!record.isCurrent()||binding.phase!==phase||!isOwnedCapacityFactoryOwnerAuthority(record.ownerAuthority))throw Error('capacity_factory_proof_not_owned');await record.validate();if(!record.isCurrent())throw Error('capacity_factory_proof_not_owned');return Object.freeze({phase,epoch:binding.epoch,scope:'fixed-native-owner-same-factory'});}
export async function inspectCapacityFactoryHelperOwner(binding,{phase}={}){await inspectCapacityFactoryProofBinding(binding,{phase});const row=ownedProofBindings.get(binding),owner=row?.helperOwner,source=capacityHelperOwnerBinding(owner);if(!source||!source.isOpen()||source.manifest!==binding.manifest||source.laboratory!==binding.laboratory||source.store!==binding.store)throw Error('capacity_factory_helper_owner_unavailable');return owner;}
export async function inspectCapacityFactoryTopologyOwner(binding,{phase}={}){await inspectCapacityFactoryProofBinding(binding,{phase});const owner=ownedProofBindings.get(binding)?.topologyOwner,source=capacityTopologyOwnerBinding(owner);if(!source||!source.isOpen()||source.manifest!==binding.manifest||source.laboratory!==binding.laboratory||source.store!==binding.store)throw Error('capacity_factory_topology_owner_unavailable');return owner;}
const fail=code=>{throw Error(code);};
const hash=value=>createHash('sha256').update(value).digest('hex');
const fingerprint=value=>hash(new X509Certificate(value).raw);
async function identity(child){const raw=await readFile('/proc/'+child.pid+'/stat','utf8'),row=raw.slice(raw.lastIndexOf(')')+2).split(' ');return {started:row[19],state:row[0]};}
const running=row=>!['T','t','Z','X','x'].includes(row.state);

// This context is instantiated only inside the real target factory. It owns
// phases, receipts and the one-shot seed action, not a transferable DTO permit.
// The original preparation-context diagnostic receipt is never accepted here.
export async function createCapacityFactoryContext({manifest,laboratory,store,recordingKey,isOpen,signal,resourceAllocator,runLifetime,ownerAuthority,helperConfiguration}={}){
 if(!isOwnedLaboratory(laboratory)||typeof isOpen!=='function'||!manifest||!store||!Buffer.isBuffer(recordingKey))fail('capacity_factory_context_configuration');
 const manifestHash=manifest.manifestHash,nonce=randomUUID(),cluster=(await store.get('SELECT system_identifier::text id FROM pg_control_system()')).id;
 let phase='planned',epoch=0,binding=null,activeWire=null,preparationGateway,preparationWorker,closed=false,closePromise,preSeedReceipt,finalReceipt,proofs,pendingEpoch=null,epochStarting=false;
 let topologyHandles=null;const topologyOwner=manifest.profile==='formal'?await createCapacityTopologyOwner({manifest,laboratory,store,isOpen,signal,runLifetime,ownerAuthority}):null;
 const helperOwner=helperConfiguration?createCapacityHelperOwner({...helperConfiguration,manifest,laboratory,store,runLifetime,isOpen,signal}):null;
 const evidence=[],contextController=new AbortController(),contextSignal=AbortSignal.any([signal,contextController.signal].filter(Boolean));
 const guarded=promise=>new Promise((resolve,reject)=>{let finished=false;const finish=(callback,value)=>{if(finished)return;finished=true;clearTimeout(timer);contextSignal.removeEventListener('abort',abort);callback(value);},abort=()=>finish(reject,Error('capacity_factory_cancelled')),timer=setTimeout(()=>finish(reject,Error('capacity_factory_liveness_unproven')),10000);Promise.resolve(promise).then(value=>finish(resolve,value),error=>finish(reject,error));contextSignal.addEventListener('abort',abort,{once:true});if(contextSignal.aborted)abort();});
 const assertContext=async()=>{
  signal?.throwIfAborted();if(closed||!isOpen()||!isOwnedLaboratory(laboratory)||manifest.manifestHash!==manifestHash)fail('capacity_factory_context_revoked');
  const marker=await guarded(store.get('SELECT current_database() name,(SELECT system_identifier::text FROM pg_control_system()) cluster,run_id,owner_nonce,manifest_hash,recording_key_digest,metadata_cursor,body_cursor FROM harbor_capacity.fixture WHERE singleton=1'));
  if(marker?.name!==fixtureDatabase(manifest)||marker.cluster!==cluster||marker.run_id!==manifest.runId||marker.owner_nonce!==manifest.ownerNonce||marker.manifest_hash!==manifestHash||marker.recording_key_digest!==hash(recordingKey))fail('capacity_factory_dataset_binding_lost');
  signal?.throwIfAborted();if(closed||!isOpen())fail('capacity_factory_context_revoked');return marker;
 };
 const assertEmpty=async()=>{const marker=await assertContext(),counts=await guarded(store.get('SELECT (SELECT COUNT(*) FROM harbor.requests) requests,(SELECT COUNT(*) FROM harbor.recordings) recordings'));if(marker.metadata_cursor!==0||marker.body_cursor!==0||counts.requests!==0||counts.recordings!==0)fail('capacity_factory_preseed_not_empty');};
 const observe=event=>{
  if(topologyOwner?.observe(event)||helperOwner?.observe(event))return true;
  const wire=activeWire;if(!wire||event?.trace!==wire.trace)return false;
  if(event.kind==='gateway-request-start'){wire.started++;if(event.signature!=='GET /ready'||!/^[-a-f0-9]{36}$/.test(event.wireId??'')||event.credentials?.authorization!==null||event.credentials?.cookie!==null)wire.invalid=true;wire.id??=event.wireId;}
  else if(event.kind==='gateway-response-end'){wire.ended++;if(event.signature!=='GET /ready'||event.wireId!==wire.id||event.status!==200)wire.invalid=true;}
  else if(event.kind==='gateway-handler-settled'){wire.settled++;if(event.signature!=='GET /ready'||event.wireId!==wire.id||event.outcome!=='resolved')wire.invalid=true;}
  else if(['gateway-response-abort','gateway-handler-unobservable'].includes(event.kind))wire.invalid=true;
  if(wire.started>1||wire.ended>1||wire.settled>1)wire.invalid=true;
  if(wire.invalid)wire.reject(Error('capacity_factory_ready_wire_invalid'));else if(wire.started===1&&wire.ended===1&&wire.settled===1)wire.resolve();return true;
 };
 const makeBinding=(next,gateway,worker,runtimeStage=next)=>{if(binding?.phase===next&&binding.epoch===epoch&&binding.gateway===gateway&&binding.worker===worker&&binding.runtimeStage===runtimeStage)return binding;topologyHandles=null;const value=Object.freeze({phase:next,runtimeStage,nonce,epoch,manifest,laboratory,store,cluster,gateway,worker,runLifetime,resourceAllocator,isOpen:()=>!closed&&isOpen()});binding=value;const internalAuthority=Object.freeze(Object.create(null));internalAuthorities.set(internalAuthority,()=>binding===value&&!closed&&isOpen());ownedProofBindings.set(value,{internalAuthority,ownerAuthority,helperOwner,topologyOwner,signal:contextSignal,isCurrent:()=>binding===value&&!closed&&isOpen(),validate:async()=>{await assertContext();if(gateway&&worker)await assertLive(gateway,worker,await Promise.all([laboratory.process,gateway.child,worker.child].map(identity)));}});return value;};
 proofs=createCapacityFormalProofConsumer({isOpen:()=>!closed&&isOpen(),getBinding:()=>binding});
 const assertLive=async(gateway,worker,expected)=>{
  await assertContext();for(const [index,[child,role]] of [[laboratory.process,'database'],[gateway.child,'gateway'],[worker.child,'worker']].entries()){
   if(ownedProcessProof(laboratory.runtime,child).role!==role)fail('capacity_factory_runtime_not_owned');const actual=await identity(child);if(!running(actual)||actual.started!==expected[index].started)fail('capacity_factory_runtime_epoch_lost');
  }
  const [listening]=await guarded(Promise.all([ownsListener(gateway.child,gateway.ready.tlsOrigin),gateway.command('status'),worker.command('status'),store.get('SELECT 1 AS alive')]));if(!listening)fail('capacity_factory_listener_not_owned');
  // Last kernel check follows all IPC/SQL waits; an old reply cannot hide a
  // role stopped while a slower endpoint/worker was still replying.
  await assertContext();
  for(const [index,child] of [laboratory.process,gateway.child,worker.child].entries()){const actual=await identity(child);if(!running(actual)||actual.started!==expected[index].started)fail('capacity_factory_runtime_epoch_lost');}
 };
 const challenge=async({gateway,worker,configuration,certificates,stage})=>{
  if(topologyOwner){const value=makeBinding(stage,gateway,worker),identities=await Promise.all([laboratory.process,gateway.child,worker.child].map(identity));await assertLive(gateway,worker,identities);const observed=await topologyOwner.inspect(value,{kind:'proxy'}),wire=observed.proxy?.readyWitness;if(!observed.liveGateway||!wire||wire.started!==1||wire.ended!==1||wire.settled!==1)fail('capacity_factory_public_ready_unproven');await assertLive(gateway,worker,identities);const receipt=Object.freeze({stage,epoch,manifestHash,traceHash:wire.traceHash,wireHash:wire.wireHash,clientReady:true,wire:Object.freeze({started:wire.started,ended:wire.ended,settled:wire.settled}),scope:'same-owner-current-epoch-original-public-tls'});evidence.push(receipt);return receipt;}
  const identities=await Promise.all([laboratory.process,gateway.child,worker.child].map(identity));let launcher,transport,timer;const trace=randomUUID();let resolve,reject;const wire=new Promise((yes,no)=>{resolve=yes;reject=no;});wire.catch(()=>{});activeWire={trace,resolve,reject,started:0,ended:0,settled:0,invalid:false};
  const cancel=()=>reject(Error('capacity_factory_cancelled'));signal?.addEventListener('abort',cancel,{once:true});timer=setTimeout(()=>reject(Error('capacity_factory_ready_timeout')),10000);
  try{
   await assertLive(gateway,worker,identities);signal?.throwIfAborted();const root=configuration.certificateDirectory,caFile=join(root,'ca.pem');
   launcher=await createCapacityRemoteLauncher({runtime:laboratory.runtime,tls:{caFile,certFile:join(root,'server.pem'),keyFile:join(root,'server.key'),peerCertificateHash:fingerprint(certificates.client.cert)},agents:[{id:'ready',origin:gateway.ready.tlsOrigin,tls:{caFile},routes:[{method:'GET',path:'/ready'}]}],limits:{maxInFlight:1,maxRequestTimeoutMs:10000,maxResponseBytes:4096}});
   const endpointBinding=capacityRemoteLauncherBinding(launcher);if(!endpointBinding?.isOpen())fail('capacity_factory_endpoint_not_owned');
   transport=await connectCapacityRemoteTransport({origin:launcher.origin,runId:launcher.runId,tls:{caFile,certFile:join(root,'client.pem'),keyFile:join(root,'client.key'),peerCertificateHash:fingerprint(certificates.server.cert)}});
   const operation=transport.request({agentId:'ready',path:'/ready',method:'GET',mode:'json',headers:{'x-capacity-trace':trace},timeoutMs:10000});
   const [result]=await guarded(Promise.all([operation.result,wire]));if(!result.success||!result.httpEnded||result.status!==200||result.json?.ready!==true||!result.socketEvidence?.tlsAuthorized)fail('capacity_factory_ready_client_unverified');
   await assertLive(gateway,worker,identities);const status=await gateway.command('status');await assertLive(gateway,worker,identities);signal?.throwIfAborted();
   if(activeWire.invalid||gateway.observationStatus().parentEventDropped||gateway.observationStatus().pendingCallbacks||status.eventDropped||status.probe?.dropped||status.probe?.unobservableHandlers)fail('capacity_factory_ready_wire_invalid');
   const receipt=Object.freeze({stage,epoch,manifestHash,traceHash:hash(trace),wireHash:hash(activeWire.id),clientReady:true,wire:Object.freeze({started:1,ended:1,settled:1}),scope:'same-owner-same-laboratory'});evidence.push(receipt);return receipt;
  }finally{clearTimeout(timer);signal?.removeEventListener('abort',cancel);activeWire=null;try{await transport?.close();}finally{await launcher?.close();}}
 };
 const startEpoch=async({stage,configuration,gatewaySink=()=>true,workerSink=()=>true})=>{
  await assertContext();epoch++;configuration=topologyOwner?topologyOwner.configuration(configuration):configuration;let runtimeLaunch;if(manifest.profile==='formal'){const allocation=makeBinding('allocation',null,null,stage),producer=await import('./formal-runtime-proof.mjs'),cap=await producer.prepareCapacityProof(allocation,{phase:'allocation'}),observed=await producer.inspectCapacityProof(cap,allocation,{phase:'allocation'});if(!observed.passed){const error=Error('capacity_formal_adapter_incomplete');error.preflight=observed;throw error;}runtimeLaunch=observed.handles.processLaunch;if(topologyOwner)await runCapacityFactoryInternalObservation(ownedProofBindings.get(allocation).internalAuthority,()=>topologyOwner.activate(allocation,{phase:'allocation',configuration}));}
  let gateway,worker;try{gateway=await startCapacityProcess({runtime:laboratory.runtime,resourceAllocator,role:'gateway',configuration,sink:gatewaySink,runtimeLaunch,signal:contextSignal});worker=await startCapacityProcess({runtime:laboratory.runtime,resourceAllocator,role:'worker',configuration:stage==='pre-seed'?configuration:{...configuration,recordingEnabled:true},sink:workerSink,runtimeLaunch,signal:contextSignal});if(topologyOwner){const liveBinding=makeBinding(stage,gateway,worker);await runCapacityFactoryInternalObservation(ownedProofBindings.get(liveBinding).internalAuthority,()=>topologyOwner.activate(liveBinding,{phase:stage,configuration}));}if(stage!=='pre-seed')pendingEpoch={stage,gateway,worker};return {gateway,worker};}catch(error){await Promise.allSettled([gateway?.close(),worker?.close()]);throw error;}
 };
 const close=()=>{if(closePromise)return closePromise;closePromise=(async()=>{closed=true;phase='closed';contextController.abort();binding=null;preSeedReceipt=null;finalReceipt=null;activeWire?.reject(Error('capacity_factory_context_revoked'));for(const child of [preparationGateway?.child,preparationWorker?.child])try{if(child&&ownedProcessProof(laboratory.runtime,child))process.kill(child.pid,'SIGCONT');}catch{}const closePreparation=async role=>{if(!role)return;try{await role.close();}catch(error){if(role.child.exitCode===null&&role.child.signalCode===null)await terminateOwnedCapacityChild(laboratory.runtime,role.child,{graceMs:1000});if(role.child.exitCode===null&&role.child.signalCode===null)throw error;}};const results=await Promise.allSettled([topologyOwner?.close(),helperOwner?.close(),closePreparation(preparationGateway),closePreparation(preparationWorker)]);if(results.some(row=>row.status==='rejected'))fail('capacity_factory_preparation_cleanup_failed');})().catch(error=>{closePromise=null;throw error;});return closePromise;};
 return Object.freeze({observe,close,assertCurrentRuntime:async(gateway,worker)=>{const expected=binding;if(phase!=='ready'||expected?.gateway!==gateway||expected.worker!==worker)fail('capacity_actor_runtime_epoch_changed');await assertLive(gateway,worker,await Promise.all([laboratory.process,gateway.child,worker.child].map(identity)));if(binding!==expected||phase!=='ready')fail('capacity_actor_runtime_epoch_changed');return Object.freeze({epoch:expected.epoch});},prepareTls:directory=>topologyOwner?.prepareTls(directory),publicOrigin:()=>topologyOwner?.getPublicOrigin()??null,remoteDriver:()=>topologyOwner?.getRemoteDriver(binding)??null,topologyHandles:()=>topologyHandles,helperExecutable:()=>helperOwner?.executable(),
  prepare:async({configuration,certificates})=>{
   if(phase!=='planned')fail('capacity_factory_phase_invalid');await assertEmpty();phase='preparing';
   ({gateway:preparationGateway,worker:preparationWorker}=await startEpoch({stage:'pre-seed',configuration,gatewaySink:event=>{observe(event);return true;}}));
   preSeedReceipt=await challenge({gateway:preparationGateway,worker:preparationWorker,configuration,certificates,stage:'pre-seed'});await assertEmpty();
   const helperBinding=makeBinding('pre-seed',preparationGateway,preparationWorker);if(helperOwner)await runCapacityFactoryInternalObservation(ownedProofBindings.get(helperBinding).internalAuthority,()=>helperOwner.activate(helperBinding,{phase:'pre-seed',configuration:topologyOwner?{...configuration,publicOrigin:topologyOwner.getPublicOrigin()}:configuration,certificates}));await assertEmpty();if(topologyOwner&&helperOwner)await runCapacityFactoryInternalObservation(ownedProofBindings.get(helperBinding).internalAuthority,()=>topologyOwner.bindDeviceOwner(helperBinding,helperOwner));
   if(manifest.profile==='formal'){const facts=await collectCapacityFormalProofs(proofs,'pre-seed');if(!facts.passed){const error=Error('capacity_formal_adapter_incomplete');error.preflight=facts;throw error;}await recheckCapacityFormalProofs(proofs,'pre-seed');topologyHandles=facts.handles;}
   phase='seed-authorized';return preSeedReceipt;
  },
  seed:async operation=>{
   if(phase!=='seed-authorized'||!preSeedReceipt||preSeedReceipt.epoch!==epoch||typeof operation!=='function')fail('capacity_factory_seed_not_authorized');
   await assertEmpty();await assertLive(preparationGateway,preparationWorker,await Promise.all([laboratory.process,preparationGateway.child,preparationWorker.child].map(identity)));if(manifest.profile==='formal')await recheckCapacityFormalProofs(proofs,'pre-seed');signal?.throwIfAborted();
   // Invalidate the live receipt before stopping its processes. The following
   // seed action is private, one-shot, and cannot authorize another target.
   await helperOwner?.retirePreSeed();await assertEmpty();preSeedReceipt=null;binding=null;phase='stopping-preparation';
   await preparationGateway.close();await preparationWorker.close();
   if(preparationGateway.child.exitCode===null&&preparationGateway.child.signalCode===null||preparationWorker.child.exitCode===null&&preparationWorker.child.signalCode===null)fail('capacity_factory_preparation_still_running');
   await assertEmpty();phase='seeding';const result=await operation();await assertContext();phase='seeded';return result;
  },
  startEpoch:async options=>{if(pendingEpoch||epochStarting)fail('capacity_factory_epoch_pending');if(options.stage==='final'&&phase!=='seeded'||options.stage==='restore'&&phase!=='ready'||!['final','restore'].includes(options.stage))fail('capacity_factory_phase_invalid');epochStarting=true;try{return await startEpoch(options);}finally{epochStarting=false;}},
  finalize:async({gateway,worker,configuration,certificates,stage='final',helperActors})=>{
   if(epochStarting)fail('capacity_factory_epoch_pending');if(stage==='final'&&phase!=='seeded'||stage==='restore'&&phase!=='ready'||!['final','restore'].includes(stage))fail('capacity_factory_phase_invalid');if(pendingEpoch?.stage===stage&&pendingEpoch.gateway===gateway&&pendingEpoch.worker===worker)pendingEpoch=null;else{if(manifest.profile==='formal')fail('capacity_runtime_process_binding');epoch++;}phase=stage+'-verifying';finalReceipt=await challenge({gateway,worker,configuration,certificates,stage});const helperBinding=makeBinding(stage,gateway,worker);if(helperOwner)await runCapacityFactoryInternalObservation(ownedProofBindings.get(helperBinding).internalAuthority,()=>helperOwner.activate(helperBinding,{phase:stage,configuration:topologyOwner?{...configuration,publicOrigin:topologyOwner.getPublicOrigin()}:configuration,certificates,actors:helperActors}));if(topologyOwner&&helperOwner)await runCapacityFactoryInternalObservation(ownedProofBindings.get(helperBinding).internalAuthority,async()=>{await topologyOwner.bindDeviceOwner(helperBinding,helperOwner);await topologyOwner.proveModelBehavior(helperBinding,helperActors);});
   if(manifest.profile==='formal'){const facts=await collectCapacityFormalProofs(proofs,stage);if(!facts.passed){const error=Error('capacity_formal_adapter_incomplete');error.preflight=facts;throw error;}await recheckCapacityFormalProofs(proofs,stage);topologyHandles=facts.handles;}phase='ready';return finalReceipt;
  },
  evidence:()=>Object.freeze({phase,manifestHash,epochs:epoch,receipts:Object.freeze([...evidence]),formalQualified:manifest.profile==='formal'&&phase==='ready',helper:helperOwner?.evidence()??null})
 });
}
