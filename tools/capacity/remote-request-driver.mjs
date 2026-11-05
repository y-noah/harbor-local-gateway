import {connectCapacityRemoteTransport,capacityRemoteTransportBinding,capacityRemoteEventReceiptBinding} from './remote-transport.mjs';
import {createCapacityRemoteStageSource} from './remote-stage-source.mjs';
import {createCapacityClockMapping} from './clock-mapping.mjs';
import {capacityHttpDefaultTimeoutMs} from './http-client.mjs';

const fail=code=>{throw Error(code);};
const timingFields=['startedAt','finishedAt','firstByteAt','headersAt','connectedAt','tlsAt','firstContentAt'];
const requestFields=['path','method','headers','body','mode','rowFormat','csvHeader','timeoutMs','maxResponseBytes'];
const id=value=>typeof value==='string'&&/^[a-zA-Z0-9_-]{1,64}$/.test(value);

// The endpoint owns the allowlist, origins and TLS materials. Controller aliases
// can select a pre-approved slot, never upload credentials, paths or new routes.
export async function createRemoteCapacityRequestDriver({remote,agentPlans,signal,maxInFlight,maxFrameBytes,maxBufferedBytes,closeTimeoutMs,register,makeAgent}={}){
 if(!remote||typeof remote!=='object'||Array.isArray(remote)||Object.keys(remote).some(key=>!['transport','origin','runId','tls','rootApproval','frames','readyTimeoutMs','commandTimeoutMs','heartbeatMs','drainTimeoutMs'].includes(key)))fail('capacity_driver_remote_configuration');
 if(!Number.isSafeInteger(maxInFlight)||maxInFlight<1||maxInFlight>256||!Number.isSafeInteger(maxFrameBytes)||maxFrameBytes<1024||maxFrameBytes>16777216||!Number.isSafeInteger(maxBufferedBytes)||maxBufferedBytes<maxFrameBytes||maxBufferedBytes>67108864)fail('capacity_driver_remote_configuration');
 if(!Number.isSafeInteger(closeTimeoutMs)||closeTimeoutMs<100||closeTimeoutMs>30000)fail('capacity_driver_remote_configuration');
 if(remote.transport&&Object.keys(remote).length!==1)fail('capacity_driver_remote_configuration');signal?.throwIfAborted();
 const transport=remote.transport??await connectCapacityRemoteTransport({...remote,signal,maxInFlight});
 const owner=capacityRemoteTransportBinding(transport);if(!owner?.requestEventsSupported||!owner.clockEpoch){if(!remote.transport)await transport.close();fail('capacity_driver_remote_capability');}
 const available=new Map(),disabled=new Set(),slots=new Map(owner.agentSlots.map(row=>[row.id,row]));
 const plan=input=>{if(!input||Object.keys(input).some(key=>!['id','origin','slot'].includes(key))||!id(input.id)||!id(input.slot??input.id))fail('capacity_driver_remote_agent_invalid');const slot=slots.get(input.slot??input.id);if(!slot||input.origin!==slot.origin)fail('capacity_driver_remote_agent_invalid');return {id:input.id,origin:slot.origin,slot:slot.id};};
 try{if(!Array.isArray(agentPlans)||!agentPlans.length||agentPlans.length>256)fail('capacity_driver_remote_agent_invalid');for(const input of agentPlans){const row=plan(input);if(available.has(row.id))fail('capacity_driver_remote_agent_invalid');available.set(row.id,row);}}catch(error){if(!remote.transport)await transport.close();throw error;}
 const mapping=createCapacityClockMapping({transport,sampleMaxAgeMs:600000}),stageSource=createCapacityRemoteStageSource({transport,mapping,maxPending:maxInFlight,maxBytes:maxBufferedBytes});
 let closed=false,closing=false,closePromise,peak=0,failures=0,requestBytes=0,requestPeakBytes=0,observationBytes=0,observationPeakBytes=0,observationDropped=0,callbackBytes=0,callbackPeakBytes=0,callbackPeak=0,callbackFailures=0,callbackBudgetRejected=0,executionResourcesReleased=false,resourcesReleased=false;const active=new Set(),browserAssessments=new Set(),pendingBrowserStarts=new Set(),snapshotBytes=new Map(),callbacks=new Set(),callbackWaiters=new Set(),closeInterrupt=new AbortController(),counts=Object.create(null),modelCounts=Object.create(null);
 const remoteIdentity=Object.freeze({runId:owner.runId,endpointCertificateHash:owner.endpointCertificateHash,clockEpoch:owner.clockEpoch,browserProfiles:owner.browserProfiles,isOpen:()=>!closed&&capacityRemoteTransportBinding(transport)===owner});
 const lostCallback=state=>{state.dropped++;observationDropped++;};
 const observe=(callback,value,state,reservedBytes)=>{
  if(typeof callback!=='function')return false;
  const bytes=reservedBytes??Buffer.byteLength(JSON.stringify(value));
  if(callbacks.size>=maxInFlight||callbackBytes+observationBytes+bytes>maxBufferedBytes){callbackBudgetRejected++;lostCallback(state);return false;}
  const entry={bytes};
  // Reserve before user code runs: a synchronous callback can reenter close().
  callbacks.add(entry);callbackBytes+=bytes;state.pending++;callbackPeak=Math.max(callbackPeak,callbacks.size);callbackPeakBytes=Math.max(callbackPeakBytes,callbackBytes);
  const settle=failed=>{if(!callbacks.delete(entry))return;callbackBytes-=bytes;state.pending--;if(failed){failures++;callbackFailures++;lostCallback(state);}if(!callbacks.size){if(closed)signal?.removeEventListener('abort',abortDriver);for(const finish of [...callbackWaiters])finish();}};
  try{const task=callback(value);if(task&&typeof task.then==='function')void Promise.resolve(task).then(value=>settle(value===false),()=>settle(true));else settle(task===false);}catch{settle(true);}return true;
 };
 const waitCallbacks=()=>new Promise((resolve,reject)=>{
  if(!callbacks.size)return resolve();
  let timer;const finish=error=>{callbackWaiters.delete(finish);clearTimeout(timer);closeInterrupt.signal.removeEventListener('abort',abort);error?reject(error):resolve();},unknown=()=>Object.assign(Error('capacity_driver_callback_cleanup_unknown'),{delivery:'unknown',pendingCallbacks:callbacks.size}),abort=()=>finish(unknown());
  callbackWaiters.add(finish);closeInterrupt.signal.addEventListener('abort',abort,{once:true});timer=setTimeout(()=>finish(unknown()),closeTimeoutMs);
  if(closeInterrupt.signal.aborted)abort();
 });
 const runBrowserCallback=(callback,value)=>new Promise((resolve,reject)=>{const state={pending:0,dropped:0};const accepted=observe(dto=>{let result;try{result=callback(dto);}catch(error){reject(error);throw error;}return Promise.resolve(result).then(value=>{resolve(value);return value;},error=>{reject(error);throw error;});},value,state);if(!accepted)reject(Error('capacity_driver_browser_callback_budget'));});
 const reserveSnapshot=(key,bytes)=>{const old=snapshotBytes.get(key)??0;if(observationBytes-old+callbackBytes+bytes>maxBufferedBytes){observationDropped++;return false;}observationBytes+=bytes-old;snapshotBytes.set(key,bytes);observationPeakBytes=Math.max(observationPeakBytes,observationBytes);return true;};
 const releaseSnapshot=key=>{observationBytes-=snapshotBytes.get(key)??0;snapshotBytes.delete(key);};
 const createBrowserAssessment=async options=>{
  if(closed||closing)fail('capacity_driver_closed');let done;const marker=new Promise(resolve=>{done=resolve;});pendingBrowserStarts.add(marker);let assessment;
  try{const {createCapacityRemoteBrowserAssessment}=await import('./remote-browser-assessment.mjs');assessment=await createCapacityRemoteBrowserAssessment({...options,assertAdmission:()=>{if(closed||closing)fail('capacity_driver_closed');options.assertCurrent();},transport,runCallback:runBrowserCallback,reserveSnapshot,releaseSnapshot,onReleased:()=>browserAssessments.delete(assessment)});if(closed||closing){await assessment.close();fail('capacity_driver_closed');}browserAssessments.add(assessment);return assessment;}finally{pendingBrowserStarts.delete(marker);done();}
 };
 const request=async(agentId,options,{onStage,onHeaders,onMappedStage,mappedStageSource,onRemoteStage=options.onRemoteStage}={})=>{
  if(mappedStageSource!==undefined&&mappedStageSource!==stageSource.source)fail('capacity_driver_remote_stage_mismatch');
  if([onStage,onHeaders,onMappedStage,onRemoteStage].some(value=>value!==undefined&&typeof value!=='function'))fail('capacity_http_configuration');
  if(closed||closing||!available.has(agentId)||disabled.has(agentId)||capacityRemoteTransportBinding(transport)!==owner)fail('capacity_driver_closed');
  if(options.origin!==available.get(agentId).origin)fail('capacity_driver_origin_mismatch');if(options.clock!==undefined)fail('capacity_driver_clock_override');if(active.size>=maxInFlight)fail('capacity_driver_inflight_budget');
  let payload=Object.fromEntries(requestFields.filter(key=>Object.hasOwn(options,key)).map(key=>[key,options[key]]));payload.agentId=available.get(agentId).slot;payload.mode??='json';payload.method??='GET';payload.timeoutMs??=Math.min(capacityHttpDefaultTimeoutMs,owner.requestLimits.maxRequestTimeoutMs);
  if(!Number.isSafeInteger(payload.timeoutMs)||payload.timeoutMs<1||payload.timeoutMs>owner.requestLimits.maxRequestTimeoutMs)fail('capacity_http_configuration');
  if(Buffer.isBuffer(payload.body)&&Math.ceil(payload.body.length/3)*4>maxFrameBytes||typeof payload.body==='string'&&Buffer.byteLength(payload.body)>maxFrameBytes)throw Object.assign(Error('capacity_driver_frame_budget'),{delivery:'not-sent'});
  if(Buffer.isBuffer(payload.body)){payload.body=payload.body.toString('base64');payload.bodyEncoding='base64';}
  const encoded=JSON.stringify(payload),payloadBytes=Buffer.byteLength(encoded);if(payloadBytes>maxFrameBytes)throw Object.assign(Error('capacity_driver_frame_budget'),{delivery:'not-sent'});if(requestBytes+payloadBytes>maxBufferedBytes)throw Object.assign(Error('capacity_driver_request_byte_budget'),{delivery:'not-sent'});payload=JSON.parse(encoded);
  options.signal?.throwIfAborted();
  const row={agentId,operation:null,events:[],bytes:0,dropped:0,observerState:{pending:0,dropped:0},unavailable:false};active.add(row);requestBytes+=payloadBytes;requestPeakBytes=Math.max(requestPeakBytes,requestBytes);peak=Math.max(peak,active.size);
  const abort=()=>{if(row.operation)void row.operation.cancel().catch(()=>{failures++;});};options.signal?.addEventListener('abort',abort,{once:true});
  row.done=(async()=>{let stageLease;try{
   if(onMappedStage)stageLease=await stageSource.begin();else await mapping.sample();options.signal?.throwIfAborted();if(disabled.has(agentId))throw Object.assign(Error('capacity_driver_agent_disabled'),{delivery:'not-sent'});
   row.operation=transport.request(payload,{onEvent:event=>{
    if(event.clockEpoch!==owner.clockEpoch){failures++;return;}
    if(event.kind==='headers')observe(onHeaders,event.value,row.observerState);
    if(onMappedStage&&event.kind==='stage'){const authenticated=capacityRemoteEventReceiptBinding(event.receipt),bytes=Math.max(Buffer.byteLength(JSON.stringify(event)),Buffer.byteLength(JSON.stringify({...event.value,trace:authenticated?.trace,receipt:{},clock:stageSource.source.clock,bound:'upper'})));observe(dto=>stageSource.deliver(dto,onMappedStage),event,row.observerState,bytes);}
    if(!onMappedStage&&!row.unavailable&&event.kind==='stage'){row.unavailable=true;observe(onStage,{type:'unavailable',reason:'remote_clock_requires_mapping'},row.observerState,maxFrameBytes);}
    const retained=event.kind==='stage'?{kind:event.kind,value:event.value,clockEpoch:event.clockEpoch,sequence:event.sequence}:event.kind==='headers'?{kind:'headers',clockEpoch:event.clockEpoch,sequence:event.sequence}:event,bytes=Buffer.byteLength(JSON.stringify(retained));
    if(row.events.length>=2049||row.bytes+bytes>maxFrameBytes||observationBytes+callbackBytes+bytes>maxBufferedBytes){row.dropped++;observationDropped++;return;}
    row.events.push(retained);row.bytes+=bytes;observationBytes+=bytes;observationPeakBytes=Math.max(observationPeakBytes,observationBytes);
    if(event.kind==='stage')observe(onRemoteStage,{...event.value,remoteClock:{clockEpoch:event.clockEpoch,endpointCertificateHash:owner.endpointCertificateHash,runId:owner.runId,session:owner.session,bounds:mapping.bounds({timestampNs:event.value.atNs,clockEpoch:event.clockEpoch})},formalQualified:false},row.observerState);
   }});if(options.signal?.aborted)abort();
   const value=await row.operation.result;await mapping.sample();
   if(value?.remoteTiming?.clockEpoch!==owner.clockEpoch)fail('capacity_driver_remote_clock_unverified');
   const remoteObservations=row.events.map(event=>({...event,...(event.kind==='stage'?{bounds:mapping.bounds({timestampNs:event.value.atNs,clockEpoch:event.clockEpoch})}:{})}));
   const timestamps=Object.fromEntries(Object.entries(value.remoteTiming.timestamps??{}).map(([key,interval])=>[key,{...interval,lower:mapping.bounds({timestampNs:interval.lowerNs,clockEpoch:owner.clockEpoch}),upper:mapping.bounds({timestampNs:interval.upperNs,clockEpoch:owner.clockEpoch})}]));
   if(row.dropped+row.observerState.dropped+(value.driverObservationDropped??0))observe(onStage,{type:'unavailable',reason:'remote_observation_budget'},row.observerState,maxFrameBytes);const missingObservations=row.dropped+row.observerState.dropped+(value.driverObservationDropped??0);
   counts[agentId]=(counts[agentId]??0)+1;if(payload.path==='/v1/responses')modelCounts[agentId]=(modelCounts[agentId]??0)+1;
   // Native performance timestamps belong to the executor and are deliberately
   // unavailable as controller timestamps. Duration is an executor-local delta.
   return {...value,...Object.fromEntries(timingFields.map(key=>[key,null])),remoteTiming:{...value.remoteTiming,timestamps,endpointCertificateHash:owner.endpointCertificateHash,runId:owner.runId,session:owner.session},remoteObservations,driverObservationDropped:missingObservations,driverObservationPending:row.observerState.pending,driverObservationComplete:missingObservations===0&&row.observerState.pending===0,driverTiming:{scope:'authenticated-remote-executor',controllerReceiptUsedAsFinish:false,controllerTimesKnown:false,formalQualified:false}};
  }catch(error){failures++;throw error;}finally{if(stageLease)stageSource.end(stageLease);active.delete(row);requestBytes-=payloadBytes;observationBytes-=row.bytes;row.events.length=0;options.signal?.removeEventListener('abort',abort);}})();return row.done;
 };
 const close=()=>closePromise??=(async()=>{
  closing=true;let callbackError,transportError,browserObservationError;
  try{
   await Promise.allSettled([...pendingBrowserStarts]);const browserResults=await Promise.allSettled([...browserAssessments].map(row=>row.close()));if(browserResults.some(row=>row.status==='rejected'&&row.reason?.resourcesReleased!==true))transportError=Error('capacity_driver_browser_cleanup_unknown');if(browserResults.some(row=>row.status==='rejected'&&row.reason?.resourcesReleased===true))browserObservationError=Object.assign(Error('capacity_driver_browser_snapshot_unavailable'),{resourcesReleased:true});
   await Promise.allSettled([...active].map(row=>row.done));
   try{await waitCallbacks();}catch(error){callbackError=error;}
   try{await transport.close();executionResourcesReleased=true;}catch(error){transportError=error;}
   resourcesReleased=executionResourcesReleased&&callbacks.size===0&&!callbackError&&!transportError;
   if(callbackError||transportError||browserObservationError)throw callbackError??transportError??browserObservationError;
  }finally{try{await stageSource.close();await mapping.close();}finally{closed=true;for(const key of [...snapshotBytes.keys()])releaseSnapshot(key);if(!callbacks.size)signal?.removeEventListener('abort',abortDriver);}}
 })().catch(error=>{if(error.message==='capacity_driver_callback_cleanup_unknown')closePromise=null;throw error;});
 const abortDriver=()=>{closeInterrupt.abort();transport.disconnect();void close().catch(()=>{});};
 const driver=Object.freeze({status:()=>({scope:'authenticated-remote-request-driver',closed,closing,resourcesReleased,executionResourcesReleased,pendingBrowserScenarios:browserAssessments.size,pendingBrowserStarts:pendingBrowserStarts.size,snapshotBytes:[...snapshotBytes.values()].reduce((sum,value)=>sum+value,0),pendingCallbacks:callbacks.size,callbackWaiters:callbackWaiters.size,callbackBytes,callbackPeakBytes,callbackPeak,maxPendingCallbacks:maxInFlight,callbackFailures,callbackBudgetRejected,pending:active.size,peak,failures,requestBytes,requestPeakBytes,observationBytes,observationPeakBytes,observationDropped,executedRequestsByAgent:{...counts},executedModelRequestsByAgent:{...modelCounts},runtimeBindingVerified:false,formalQualified:false,clock:mapping.status(),remoteStages:stageSource.status(),missing:[...owner.missing,'remote_clock_consumer_unproven','large_body_transfer_unproven'],transport:transport.status()}),
  addAgent:async input=>{const row=plan(input);if(closed||closing||available.has(row.id)||available.size>=256)fail('capacity_driver_remote_agent_invalid');available.set(row.id,row);return makeAgent(driver,row.id);},
  launchBrowser:async()=>fail('capacity_driver_remote_browser_scenario_required'),close});
 register(driver,{request,available,disabled,remoteSource:Object.freeze({driver,transport,transportBinding:owner,clockMapping:mapping,stageSource:stageSource.source,isOpen:remoteIdentity.isOpen,runId:owner.runId,session:owner.session,clockEpoch:owner.clockEpoch}),remoteIdentity,createBrowserAssessment,observeDriverFailure:callback=>{const state={pending:0,dropped:0};if(closed||closing){callbackBudgetRejected++;lostCallback(state);return;}/* The legacy emitter owns its envelope; reserve a full configured frame. */observe(callback,null,state,maxFrameBytes);},evidence:{scope:'authenticated-remote-request-driver'},destroyAgent:agentId=>{disabled.add(agentId);for(const row of active)if(row.operation&&row.agentId===agentId)void row.operation.cancel().catch(()=>{});}});
 signal?.addEventListener('abort',abortDriver,{once:true});if(signal?.aborted)abortDriver();return driver;
}
