import {randomUUID,createHash} from 'node:crypto';
import {capacityRemoteBrowserActivationTemplate,capacityRemoteBrowserActivation} from './remote-browser-profile.mjs';
import {createCapacityBrowserScenarioCore} from './browser-assessment.mjs';
import {remoteFailure,remoteInteger,remoteId} from './remote-protocol.mjs';

const hash=value=>createHash('sha256').update(value).digest('hex');
const keys=(value,allowed)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).every(key=>allowed.includes(key));
const stages=new Set(['beforeRequest','response','requestFinished','requestFailed','upstreamActive']);

// Root initialization fixes profiles or an explicit bounded activation template.
// Activation never accepts a URL, executable, script, path or measurement policy.
export function createCapacityRemoteBrowserService({profiles=[],activationTemplate=null,launchBrowser,emitChallenge,emitState,clockEpoch,frames,maxSessions=1,maxActions=16,maxCallbacks=256,callbackTimeoutMs=10000}={}){
 if(!Array.isArray(profiles)||profiles.length>8||typeof launchBrowser!=='function'||typeof emitChallenge!=='function'||typeof emitState!=='function'||!remoteId(clockEpoch))throw remoteFailure('capacity_remote_browser_configuration');
 remoteInteger(maxSessions,1,8);remoteInteger(maxActions,1,256);remoteInteger(maxCallbacks,1,256);remoteInteger(callbackTimeoutMs,100,30000);
 const template=capacityRemoteBrowserActivationTemplate(activationTemplate);if(template&&(profiles.length>=8||profiles.some(row=>row.id===template.id)))throw remoteFailure('capacity_remote_browser_configuration');let activating=false,activationFault=false;
 const approved=new Map(),sessions=new Map(),callbacks=new Map(),actions=new Map();let callbackBytes=0,peakCallbackBytes=0,sequence=0,closing=false,aborting=false,closePromise;
 for(const input of profiles){
  if(!keys(input,['id','origin','bindingTag','epoch','cookies','serverSpkiHash','deepPages','sampleIntervalMs','timeoutMs','maxSeries','searchTerm','maxActionMs'])||!remoteId(input.id)||approved.has(input.id)||!remoteId(input.bindingTag))throw remoteFailure('capacity_remote_browser_configuration');
  const origin=new URL(input.origin);if(origin.protocol!=='https:'||origin.origin!==input.origin||origin.username||origin.password||!/^[A-Za-z0-9+/]{43}=$/.test(input.serverSpkiHash??''))throw remoteFailure('capacity_remote_browser_configuration');
  remoteInteger(input.epoch,0,Number.MAX_SAFE_INTEGER);
  if(!Array.isArray(input.cookies)||input.cookies.length!==5||input.cookies.some(row=>!keys(row,['name','value'])||row.name!=='harbor_session'||typeof row.value!=='string'||!row.value.length||row.value.length>8192||/[\r\n;]/.test(row.value))||new Set(input.cookies.map(row=>hash(row.value))).size!==5)throw remoteFailure('capacity_remote_browser_configuration');
  const plan={deepPages:100,sampleIntervalMs:1000,timeoutMs:15000,maxSeries:2048,searchTerm:'capacityScenario',maxActionMs:300000,...input,cookies:input.cookies.map(row=>({...row}))};
  remoteInteger(plan.deepPages,2,1000);remoteInteger(plan.sampleIntervalMs,1,60000);remoteInteger(plan.timeoutMs,100,60000);remoteInteger(plan.maxSeries,1,10000);remoteInteger(plan.maxActionMs,100,600000);if(typeof plan.searchTerm!=='string'||Array.from(plan.searchTerm).length<2||Array.from(plan.searchTerm).length>128)throw remoteFailure('capacity_remote_browser_configuration');approved.set(plan.id,plan);
 }
 const summary=()=>[...approved.values()].map(row=>({id:row.id,origin:row.origin,bindingTag:row.bindingTag,epoch:row.epoch,cookieFingerprints:row.cookies.map(value=>hash(value.value)),deepPages:row.deepPages,sampleIntervalMs:row.sampleIntervalMs,timeoutMs:row.timeoutMs,maxSeries:row.maxSeries,searchTerm:row.searchTerm,maxActionMs:row.maxActionMs}));
 const snapshot=(sessionId,selected)=>{const row=selected??sessions.get(sessionId);if(!row?.assessment)throw remoteFailure('capacity_remote_browser_not_ready');return {browserSession:sessionId,profileId:row.plan.id,bindingTag:row.plan.bindingTag,clockEpoch,sequence:++sequence,atNs:process.hrtime.bigint().toString(),status:row.assessment.status(),evidence:row.assessment.collectEvidence(),snapshotDropped:row.snapshotDropped};};
 const release=(id,error,value)=>{const row=callbacks.get(id);if(!row)return false;callbacks.delete(id);callbackBytes-=row.bytes;clearTimeout(row.timer);error?row.reject(error):row.resolve(value);return true;};
 const rejectCallbacks=sessionId=>{for(const [id,row] of callbacks)if(sessionId===undefined||row.sessionId===sessionId)release(id,remoteFailure('capacity_remote_browser_callback_closed'));};
 const challenge=(sessionId,type,dto)=>new Promise((resolve,reject)=>{
  if(aborting||!sessions.has(sessionId)||!stages.has(type))return reject(remoteFailure('capacity_remote_browser_closed'));
  const callbackId=randomUUID(),value={browserSession:sessionId,callbackId,sequence:++sequence,clockEpoch,type,dto},bytes=Buffer.byteLength(JSON.stringify(value));
  if(callbacks.size>=maxCallbacks||bytes>frames.maxFrameBytes||callbackBytes+bytes>frames.maxBufferedBytes-frames.controlReserveBytes)return reject(remoteFailure('capacity_remote_browser_callback_budget'));
  const timer=setTimeout(()=>release(callbackId,remoteFailure('capacity_remote_browser_callback_timeout')),callbackTimeoutMs);callbacks.set(callbackId,{sessionId,type,resolve,reject,timer,bytes});callbackBytes+=bytes;peakCallbackBytes=Math.max(peakCallbackBytes,callbackBytes);
  try{if(emitChallenge(value)!==true)throw remoteFailure('capacity_remote_browser_callback_not_sent');}catch(error){release(callbackId,error);}
 });
 const acknowledge=payload=>{
  if(!keys(payload,['browserSession','callbackId','ok','value'])||!remoteId(payload.browserSession)||!remoteId(payload.callbackId)||typeof payload.ok!=='boolean')throw remoteFailure('capacity_remote_browser_callback_invalid');
  const row=callbacks.get(payload.callbackId);if(!row||row.sessionId!==payload.browserSession)return false;
  if(!payload.ok){release(payload.callbackId,remoteFailure('capacity_remote_browser_callback_rejected'));return;}
  const value=payload.value;
  if(row.type==='beforeRequest'){if(!keys(value,['x-capacity-trace'])||Object.keys(value).length!==1||!/^[a-zA-Z0-9-]{1,100}$/.test(value['x-capacity-trace']??''))throw remoteFailure('capacity_remote_browser_callback_invalid');}
  else if(row.type==='upstreamActive'){if(!keys(value,['active'])||Object.keys(value).length!==1||!Number.isSafeInteger(value.active)||value.active<0)throw remoteFailure('capacity_remote_browser_callback_invalid');}
  else if(!keys(value,['accepted'])||value.accepted!==true||Object.keys(value).length!==1)throw remoteFailure('capacity_remote_browser_callback_invalid');
  release(payload.callbackId,null,value);
 };
 const closeSession=async(sessionId,{drain=false}={})=>{
  const row=sessions.get(sessionId);if(!row)throw remoteFailure('capacity_remote_browser_session_unknown');if(row.closing)return row.closing;
  row.closed=true;clearInterval(row.timer);row.closing=(async()=>{await row.ready;if(row.cleanupError)throw remoteFailure('capacity_remote_browser_cleanup_unknown');if(!row.assessment){sessions.delete(sessionId);return {resourcesReleased:true};}
   if(!drain){for(const action of actions.values())if(action.sessionId===sessionId)action.controller.abort();rejectCallbacks(sessionId);}
   // A started scene is intentionally stopped; a separately accepted measure
   // drains to its real terminal during normal shutdown.
   if(row.assessment.status().scene)await row.assessment.stop();
   await Promise.allSettled([...actions.values()].filter(action=>action.sessionId===sessionId).map(action=>action.task));
   await row.assessment.close();rejectCallbacks(sessionId);const finalSnapshot=snapshot(sessionId,row);sessions.delete(sessionId);return {resourcesReleased:true,snapshot:finalSnapshot};
  })();return row.closing;
 };
 const activate=async payload=>{
  if(!template||closing||activating||activationFault)throw remoteFailure('capacity_remote_browser_activation_unavailable');const selected=capacityRemoteBrowserActivation(payload),previous=approved.get(template.id);
  if(previous){if(previous.bindingTag===selected.bindingTag&&previous.epoch===selected.epoch&&JSON.stringify(previous.cookies)===JSON.stringify(selected.cookies))return {profile:summary().find(row=>row.id===template.id),resourcesReleased:true,retiredBrowserSessions:[]};if(selected.epoch<=previous.epoch)throw remoteFailure('capacity_remote_browser_activation_stale');}
  activating=true;try{
   const old=[...sessions.entries()].filter(([,row])=>row.plan.id===template.id);const outcomes=await Promise.allSettled(old.map(([id])=>closeSession(id)));
   if(outcomes.some(row=>row.status==='rejected')||closing){activationFault=true;throw remoteFailure('capacity_remote_browser_cleanup_unknown');}
   approved.set(template.id,{...template,...selected});return {profile:summary().find(row=>row.id===template.id),resourcesReleased:true,retiredBrowserSessions:old.map(([id])=>id)};
  }finally{activating=false;}
 };
 const command=async(operation,payload={},actionId)=>{
  if(closing||activating||activationFault)throw remoteFailure('capacity_remote_browser_closed');
  if(operation==='open'){
   if(!keys(payload,['profileId'])||Object.keys(payload).length!==1||sessions.size>=maxSessions)throw remoteFailure('capacity_remote_browser_command');const plan=approved.get(payload.profileId);if(!plan)throw remoteFailure('capacity_remote_browser_profile');
   // Reserve the slot before an asynchronous browser launch can overlap.
   let ready;const sessionId=randomUUID(),row={plan,assessment:null,closed:false,closing:null,snapshotDropped:0,dirty:false,ready:new Promise(resolve=>{ready=resolve;})};sessions.set(sessionId,row);let browser;
   try{
    browser=await launchBrowser(plan.serverSpkiHash);const observer=Object.fromEntries(['beforeRequest','response','requestFinished','requestFailed'].map(type=>[type,dto=>challenge(sessionId,type,dto)]));
    row.assessment=await createCapacityBrowserScenarioCore({browser,origin:plan.origin,cookies:plan.cookies,profile:'selfcheck',deepPages:plan.deepPages,sampleIntervalMs:plan.sampleIntervalMs,timeoutMs:plan.timeoutMs,maxSeries:plan.maxSeries,searchTerm:plan.searchTerm,observer,readUpstreamActive:async phase=>(await challenge(sessionId,'upstreamActive',{phase})).active,formalEligible:false,onObserverSettled:()=>{row.dirty=true;},transportEvidence:{scope:'authenticated-remote-browser-executor',verified:false,clockEpoch}});
    if(closing||row.closed)throw remoteFailure('capacity_remote_browser_closed');row.timer=setInterval(()=>{if(!row.closed&&(row.assessment.status().scene||row.dirty))try{if(emitState(snapshot(sessionId))!==true){row.snapshotDropped++;row.dirty=true;}else row.dirty=false;}catch{row.snapshotDropped++;}},plan.sampleIntervalMs);row.timer.unref();return {browserSession:sessionId,profileId:plan.id,clockEpoch,scope:'fixed-browser-scenario',formalQualified:false,snapshot:snapshot(sessionId)};
   }catch(error){clearInterval(row.timer);try{await browser?.close();sessions.delete(sessionId);}catch(cleanup){row.cleanupError=cleanup;row.closed=true;}throw error;}finally{ready();}
  }
  if(!keys(payload,['browserSession','actorIndex','view','variant','scene'])||!remoteId(payload.browserSession))throw remoteFailure('capacity_remote_browser_command');const row=sessions.get(payload.browserSession);if(!row?.assessment||row.closed)throw remoteFailure('capacity_remote_browser_session_unknown');
  const extra=Object.keys(payload).filter(key=>key!=='browserSession');
  if(operation==='status'||operation==='collect'||operation==='close'){if(extra.length)throw remoteFailure('capacity_remote_browser_command');return operation==='close'?closeSession(payload.browserSession,{drain:true}):{value:operation==='status'?{...row.assessment.status(),callbackPending:[...callbacks.values()].filter(value=>value.sessionId===payload.browserSession).length}:row.assessment.collectEvidence(),snapshot:snapshot(payload.browserSession)};}
  if(!['prepare','measure','start','stop'].includes(operation)||!remoteId(actionId)||actions.has(actionId)||actions.size>=maxActions)throw remoteFailure('capacity_remote_browser_command');
  if(operation==='measure'){if(extra.some(key=>!['actorIndex','view','variant'].includes(key))||!Number.isInteger(payload.actorIndex)||payload.actorIndex<0||payload.actorIndex>4||!['requests','members','sessions','statistics','search'].includes(payload.view)||!['default','maximum','deep','browser-cold'].includes(payload.variant))throw remoteFailure('capacity_remote_browser_command');}
  else if(operation==='start'){if(extra.length!==1||!keys(payload.scene,['id','durationMs'])||!remoteId(payload.scene.id)||!Number.isSafeInteger(payload.scene.durationMs)||payload.scene.durationMs<1)throw remoteFailure('capacity_remote_browser_command');}
  else if(extra.length)throw remoteFailure('capacity_remote_browser_command');
  const controller=new AbortController(),action={controller,sessionId:payload.browserSession,task:null};actions.set(actionId,action);const timer=setTimeout(()=>controller.abort(),row.plan.maxActionMs);
  const abort=()=>{if(operation==='prepare')void row.assessment.close().catch(()=>{});};controller.signal.addEventListener('abort',abort,{once:true});
  action.task=Promise.resolve().then(()=>operation==='prepare'?row.assessment.prepare():operation==='measure'?row.assessment.measure({actorIndex:payload.actorIndex,view:payload.view,variant:payload.variant,signal:controller.signal}):operation==='start'?row.assessment.start({scene:payload.scene,signal:controller.signal}):row.assessment.stop()).finally(()=>{clearTimeout(timer);controller.signal.removeEventListener('abort',abort);actions.delete(actionId);});return action.task.then(value=>({value,snapshot:snapshot(payload.browserSession)}));
 };
 const stateDeliveryFailed=sessionId=>{const row=sessions.get(sessionId);if(row&&!row.closed){row.snapshotDropped++;row.dirty=true;}};
 const cancel=actionId=>{const row=actions.get(actionId);row?.controller.abort();return {acknowledged:true,active:!!row};};
 const abort=()=>{aborting=true;rejectCallbacks();for(const row of actions.values())row.controller.abort();};
 const close=({drain=false}={})=>{if(!drain)abort();return closePromise??=(async()=>{closing=true;const outcomes=await Promise.allSettled([...sessions.keys()].map(id=>closeSession(id,{drain})));if(outcomes.some(row=>row.status==='rejected'))throw remoteFailure('capacity_remote_browser_cleanup_unknown');return {resourcesReleased:true};})();};
 return Object.freeze({summary,activationTemplate:template,activate,command,acknowledge,stateDeliveryFailed,cancel,abort,close,status:()=>({sessions:sessions.size,actions:actions.size,pendingCallbacks:callbacks.size,callbackBytes,peakCallbackBytes,snapshotDropped:[...sessions.values()].reduce((sum,row)=>sum+row.snapshotDropped,0),closing})});
}
