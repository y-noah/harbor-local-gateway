import {capacityRemoteTransportBinding} from './remote-transport.mjs';
import {capacityWireSignature} from './gateway-probe.mjs';
import {remoteFailure} from './remote-protocol.mjs';

const fields=(value,allowed)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.includes(key));
const requestId=value=>typeof value==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(value);
const equal=(left,right)=>JSON.stringify(left)===JSON.stringify(right);
// The caller is the owned browser wrapper. All network destinations, cookies
// and executable materials have already been approved at endpoint startup.
export async function createCapacityRemoteBrowserAssessment({transport,descriptor,observer,readUpstreamActive,assertCurrent,assertAdmission,runCallback,reserveSnapshot,releaseSnapshot,onReleased,configuration}={}){
 const owner=capacityRemoteTransportBinding(transport);
 if(!owner||typeof assertCurrent!=='function'||typeof readUpstreamActive!=='function'||!observer||['beforeRequest','response','requestFinished','requestFailed'].some(name=>typeof observer[name]!=='function'))throw remoteFailure('capacity_remote_browser_controller_configuration');
 const plan=owner.browserProfiles.find(row=>row.origin===descriptor.origin&&row.bindingTag===descriptor.bindingTag&&row.epoch===descriptor.epoch&&equal(row.cookieFingerprints,descriptor.cookieFingerprints)&&['deepPages','sampleIntervalMs','timeoutMs','maxSeries','searchTerm'].every(key=>row[key]===configuration[key]));
 if(!plan)throw remoteFailure('capacity_remote_browser_profile_mismatch');assertCurrent();
 let sessionId,snapshot,receivedAt=null,closed=false,closing=false,disconnected=false,closePromise,pendingOperations=0,snapshotDropped=0,terminalSnapshotAvailable=true,scene=null,lastCallbackFailure=null,sceneSignal,sceneAbort;const pending=new Set();
 const update=value=>{
  if(!value||value.browserSession!==sessionId||value.profileId!==plan.id||value.bindingTag!==descriptor.bindingTag||value.clockEpoch!==owner.clockEpoch||!Number.isSafeInteger(value.sequence)||!value.status||!value.evidence)throw remoteFailure('capacity_remote_browser_snapshot_invalid');if(snapshot&&value.sequence<=snapshot.sequence)return true;
  if(!reserveSnapshot(sessionId,Buffer.byteLength(JSON.stringify(value)))){snapshotDropped++;return false;}snapshot=value;receivedAt=performance.now();return true;
 };
 const challenge=value=>{
  if(value.browserSession!==sessionId)return;
  const task=runCallback(async challenge=>{
   assertCurrent();const {type,dto}=challenge;
   if(type==='upstreamActive'){if(Object.keys(dto).length!==1||!['before','after'].includes(dto.phase))throw remoteFailure('capacity_remote_browser_callback_invalid');const active=await readUpstreamActive(dto.phase);if(!Number.isSafeInteger(active)||active<0)throw remoteFailure('capacity_remote_browser_upstream_unknown');return {active};}
   if(type==='beforeRequest'){
    if(!fields(dto,['requestId','source','actorIndex','method','route','credentials'])||!requestId(dto.requestId)||!fields(dto.credentials,['authorization','cookie'])||dto.credentials.authorization!==null||capacityWireSignature(dto.method,dto.route)!==dto.method+' '+dto.route||dto.source!=='browser'||!Number.isInteger(dto.actorIndex)||dto.actorIndex<0||dto.actorIndex>4||typeof dto.requestId!=='string'||typeof dto.route!=='string'||typeof dto.method!=='string'||!dto.credentials||dto.credentials.cookie===null||!/^[a-f0-9]{64}$/.test(dto.credentials.cookie)||dto.credentials.cookie!==descriptor.cookieHeaderFingerprints[dto.actorIndex])throw remoteFailure('capacity_remote_browser_callback_invalid');
    const extra=await observer.beforeRequest(dto);if(!extra||Object.keys(extra).length!==1||!/^[a-zA-Z0-9-]{1,100}$/.test(extra['x-capacity-trace']??''))throw remoteFailure('capacity_remote_browser_trace_invalid');assertCurrent();return extra;
   }
   if(!['response','requestFinished','requestFailed'].includes(type)||!requestId(dto.requestId)||!fields(dto,type==='response'?['requestId','status']:type==='requestFinished'?['requestId']:['requestId','cancelled','notSent'])||type==='response'&&(!Number.isInteger(dto.status)||dto.status<100||dto.status>599)||type==='requestFailed'&&(typeof dto.cancelled!=='boolean'||dto.notSent!==undefined&&typeof dto.notSent!=='boolean'))throw remoteFailure('capacity_remote_browser_callback_invalid');
   const result=await observer[type](dto);if(result===false)throw remoteFailure('capacity_remote_browser_observation_rejected');return {accepted:true};
  },value);
  void task.then(result=>{if(!disconnected)try{transport.acknowledgeBrowserChallenge({browserSession:sessionId,callbackId:value.callbackId,ok:true,value:result});}catch{disconnected=true;}},error=>{lastCallbackFailure=['capacity_remote_browser_callback_invalid','capacity_remote_browser_upstream_unknown','capacity_remote_browser_trace_invalid','capacity_remote_browser_observation_rejected','capacity_browser_target_epoch_changed','capacity_driver_browser_callback_budget'].includes(error?.message)?error.message:'observer_failed';if(!disconnected)try{transport.acknowledgeBrowserChallenge({browserSession:sessionId,callbackId:value.callbackId,ok:false});}catch{disconnected=true;}});
 };
 const opened=await transport.openBrowserScenario(plan.id,{onChallenge:challenge,onClose:()=>{disconnected=true;},onState:update});sessionId=opened.browserSession;
 if(!update(opened.snapshot)){try{await transport.browserScenario(sessionId,'close').result;}finally{transport.releaseBrowserScenario(sessionId);releaseSnapshot(sessionId);}throw remoteFailure('capacity_remote_browser_snapshot_unavailable');}
 const operation=async(name,payload={},signal)=>{
  if(closed||closing&&name!=='close'||disconnected)throw remoteFailure('capacity_remote_browser_closed');if(!['close','stop','status'].includes(name))assertAdmission();
  const request=transport.browserScenario(sessionId,name,payload),abort=()=>{void request.cancel().catch(()=>{});};signal?.addEventListener('abort',abort,{once:true});if(signal?.aborted)abort();pending.add(request);pendingOperations++;
  try{const result=await request.result;if(result?.snapshot&&!update(result.snapshot))transport.rejectBrowserState(sessionId,result.snapshot.sequence);return Object.hasOwn(result??{},'value')?result.value:result;}finally{signal?.removeEventListener('abort',abort);pending.delete(request);pendingOperations--;}
 };
 const stop=async()=>{const current=scene;scene=null;current?.signal?.removeEventListener('abort',current.abort);sceneSignal=null;sceneAbort=null;return operation('stop');};
 const close=()=>closePromise??=(async()=>{
  closing=true;sceneSignal?.removeEventListener('abort',sceneAbort);let outcome;
  try{outcome=await operation('close');if(outcome?.resourcesReleased!==true)throw remoteFailure('capacity_remote_browser_cleanup_unknown');terminalSnapshotAvailable=outcome.snapshot?.sequence===snapshot.sequence&&snapshot.status.closed===true;closed=true;transport.releaseBrowserScenario(sessionId);onReleased?.();if(!terminalSnapshotAvailable)throw Object.assign(remoteFailure('capacity_remote_browser_snapshot_unavailable'),{resourcesReleased:true});return {resourcesReleased:true,snapshotAvailable:true};}catch(error){throw Object.assign(error,{cleanup:close});}
 })();
 return Object.freeze({
  prepare:()=>operation('prepare'),measure:({actorIndex=0,view,variant='default',signal}={})=>operation('measure',{actorIndex,view,variant},signal),
  start:async({scene:requestedScene,signal}={})=>{if(scene)throw remoteFailure('capacity_remote_browser_scene_active');const current={signal,abort:()=>{if(scene===current)void stop().catch(()=>{});}};scene=current;sceneSignal=signal;sceneAbort=current.abort;signal?.addEventListener('abort',current.abort,{once:true});try{const result=await operation('start',{scene:{id:requestedScene?.id,durationMs:requestedScene?.durationMs}},signal);if(signal?.aborted&&scene===current)await stop();return result;}catch(error){if(scene===current){scene=null;sceneSignal=null;sceneAbort=null;}signal?.removeEventListener('abort',current.abort);throw error;}},
  stop,close,observerVersion:'capacity-ui-observer-v1',transportEvidence:Object.freeze({origin:plan.origin,verified:false,scope:'authenticated-remote-browser-scenario',clockEpoch:owner.clockEpoch,formalQualified:false}),
  status:()=>({...snapshot.status,...(disconnected||!terminalSnapshotAvailable?{active:null,contexts:null,observerPending:null}:{}),closed:closed||snapshot.status.closed,remoteSnapshot:{scope:'last-authenticated-remote-snapshot',receivedAtMs:receivedAt,ageMs:receivedAt===null?null:performance.now()-receivedAt,sequence:snapshot.sequence,snapshotDropped:snapshotDropped+(snapshot.snapshotDropped??0),terminalSnapshotAvailable,lastCallbackFailure,disconnected,pendingOperations,bindingTag:descriptor.bindingTag,formalQualified:false}}),
  collectEvidence:()=>{if(!closed)assertCurrent();if(!terminalSnapshotAvailable)throw remoteFailure('capacity_remote_browser_snapshot_unavailable');return {...snapshot.evidence,remoteSnapshot:{scope:'last-authenticated-remote-snapshot',sequence:snapshot.sequence,snapshotDropped:snapshotDropped+(snapshot.snapshotDropped??0),disconnected,bindingTag:descriptor.bindingTag,formalQualified:false}};}
 });
}
