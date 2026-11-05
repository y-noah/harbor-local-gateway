import {createHash} from 'node:crypto';
import {streamingStageClock} from '../../streaming-stage-observer.mjs';
import {createCapacityClientStageObserver} from './http-client.mjs';
import {isOwnedUpstream} from './controlled-upstream.mjs';
import {createStreamingStageAssessment} from './streaming-stage-assessment.mjs';
import {capacityWindowsHelperDeliveryReceipt,capacityWindowsHelperObserverBinding} from './windows-helper-observer.mjs';
import {capacityRequestDriverRemoteSourceBinding} from './request-driver.mjs';
import {capacityRemoteStageReceiptBinding} from './remote-stage-source.mjs';
import {targets} from './scenarios.mjs';
const id=value=>typeof value==='string'&&/^[a-zA-Z0-9._:-]{1,200}$/.test(value),ns=value=>typeof value==='string'&&/^\d{1,30}$/.test(value);
const projections=['body','usage'],leaseNames=['execution','output','body','member'];
const endpointKeys=['receipt','trace','type','atNs','ordinal','contentKind','hash','bytes','startNs','endNs','queued','queue','outcome','lease','action','accepted','recordingExpected'];
const gatewayTypes=new Set(['body-end','dispatch','admission-attempt','admission-granted','wait','content','cancel','lease','accepted']);
// Only these reader results prove an actual projection was not ready at query
// start. Metadata latches and source_changed merely require another attempt.
const projectionPending=new Set(['request_pending','usage_projection_pending','body_pending','index_key_pending']);
const mismatchReasons=new Set(['actor_mismatch', 'admission_actor_mismatch', 'admission_ambiguous', 'body_digest_mismatch', 'detail_actor_mismatch', 'detail_status_mismatch', 'document_cursor', 'document_field_sequence', 'document_part_count', 'document_part_gap', 'document_part_shape', 'document_provenance', 'expected_evidence_conflict', 'expected_source_mismatch', 'model_mismatch', 'partial_output_mismatch', 'price_revision_mismatch', 'projection_mismatch', 'request_binding_mismatch', 'standard_mismatch', 'terminal_ambiguous', 'terminal_status_mismatch', 'usage_known_mismatch', 'usage_tokens_mismatch']);
const synchronous=value=>{if(value&&typeof value.then==='function'){void Promise.resolve(value).catch(()=>{});throw Error('capacity_stage_async_capability');}return value;};
const descriptor=value=>value&&value.clock?.kind==='linux-hrtime-ns'&&['kernelId','clockId','epoch'].every(key=>id(value.clock[key]))&&typeof value.isOpen==='function';
const copyClock=value=>Object.freeze({...value,...(value.mapping?{mapping:Object.freeze({...value.mapping})}:{})});

// One private factory scope owns this instance. The only long-lived history is
// view.stageHistory; the maps/queue below contain bounded in-flight work only.
export function createStreamingStageRuntime({view,group='normal',gatewaySource,upstream,reader,verifyTopology,topologyProof,readObserverStatus,profile='formal',minSamples=targets.minSamples,maxActive=10000,readerConcurrency=2,pollIntervalMs=100,maxIssues=100,helperRequired=false,getHelperForTrace=()=>null,helperTimeoutMs=5000,remoteDriver=null}={}){
 if(typeof helperRequired!=='boolean'||typeof getHelperForTrace!=='function'||!Number.isSafeInteger(helperTimeoutMs)||helperTimeoutMs<100||helperTimeoutMs>30000||!view||!id(view.scope)||['readTrace','readStageExpectations','readHandlerWitness','flush'].some(key=>typeof view[key]!=='function')||!view.stageHistory||!descriptor(gatewaySource)||gatewaySource.role!=='gateway'||!isOwnedUpstream(upstream)||typeof reader?.read!=='function'||typeof verifyTopology!=='function'||readObserverStatus!==undefined&&typeof readObserverStatus!=='function'||!['normal','fault','cancel','stress','comparison'].includes(group)||![maxActive,readerConcurrency,pollIntervalMs,maxIssues].every(value=>Number.isSafeInteger(value)&&value>0)||readerConcurrency>64||pollIntervalMs>1000)throw Error('capacity_stage_runtime_configuration');
 const remoteBinding=remoteDriver===null?null:capacityRequestDriverRemoteSourceBinding(remoteDriver);if(remoteDriver!==null&&!remoteBinding)throw Error('capacity_stage_remote_driver_not_owned');
 const gatewayIsOpen=gatewaySource.isOpen,upstreamStatus=upstream.status.bind(upstream);let epochLost=false;const gatewayOpen=()=>{if(epochLost)return false;try{if(synchronous(gatewayIsOpen())===true)return true;}catch{}epochLost=true;return false;};
 const clock=streamingStageClock();if(!clock)throw Error('capacity_stage_runtime_clock');
 const readerMismatches={body:Object.create(null),usage:Object.create(null)},bodyDigestMismatches={input:Object.create(null),output:Object.create(null),unclassified:Object.create(null)},active=new Map(),queue=[],missing=new Set(),issues=[],waiters=new Set(),proofs=new WeakMap();let head=0,running=0,timerCount=0,peakActive=0,peakQueued=0,peakRunning=0,started=0,finished=0,readerCalls=0,hardFailures=0,closed=false,stopping=false,pumping=false,closePromise=null;
 const now=()=>process.hrtime.bigint(),wake=()=>{for(const done of waiters)done();waiters.clear();};
 const lack=(reason,trace)=>{missing.add(reason);collector.invalidate({...(trace?{trace}:{}),reason});};
 const hard=(reason,state)=>{if(state?.hard.has(reason))return;state?.hard.add(reason);hardFailures++;if(issues.length<maxIssues)issues.push({code:reason,...(state?{reference:createHash('sha256').update(state.trace).digest('hex').slice(0,16)}:{})});wake();};
 const bind=(role,value)=>{const proof=Object.freeze({});proofs.set(proof,Object.freeze({...value,role,clock:copyClock(value.clock)}));return proof;};
 const gatewayProof=bind('gateway',{...gatewaySource,isOpen:gatewayOpen}),upstreamProof=bind('upstream',{clock,isOpen:()=>!closed}),readerProof=bind('reader',{clock,isOpen:()=>!closed});
 let clientStageObserver;
 const collector=createStreamingStageAssessment({referenceClock:clock,history:view.stageHistory,profile,minSamples,maxActive,maxIssues,helperRequired,verifyTopology,topologyProof,verifySource:({role,proof})=>proofs.get(proof)?.role===role?proofs.get(proof):null});
 const sources={gateway:collector.source({role:'gateway',proof:gatewayProof}),upstream:collector.source({role:'upstream',proof:upstreamProof}),reader:collector.source({role:'reader',proof:readerProof})};
 const helperProof=bind('helper',{clock,isOpen:()=>!closed,endpoint:'helper-socket-write-complete',readReceipt:receipt=>{
  const row=capacityWindowsHelperDeliveryReceipt(receipt),state=row&&active.get(row.event.trace);
  if(!state?.helper||row.observer!==state.helper.observer||!capacityWindowsHelperObserverBinding(row.observer))return null;return row;
 }});
 if(helperRequired)sources.helper=collector.source({role:'helper',proof:helperProof});
 const readExpected=state=>{try{const value=synchronous(view.readStageExpectations(state.trace));if(value?.passed===false)hard('stage_expectation_conflict',state);if(!value||value.available!==true){lack('ledger_unavailable',state.trace);return null;}if(value.scope!==view.scope||value.source!=='model'||value.trace!==state.trace){hard('stage_expectation_owner_conflict',state);return null;}return value;}catch{lack('ledger_unavailable',state.trace);return null;}};
 function clearTimer(state){if(state.timer){clearTimeout(state.timer);state.timer=null;timerCount--;}}
 function schedule(state){if(closed||state.epochClosed||!active.has(state.trace)||state.timer)return;let ms=pollIntervalMs;if(state.deadline!==null&&!state.readDone)ms=Math.min(ms,Math.max(1,Number((state.deadline-now()+999999n)/1000000n)));timerCount++;state.timer=setTimeout(()=>{state.timer=null;timerCount--;tick(state);},ms);}
 const expectedProjections=state=>state.policy?.accepted?state.policy.recordingExpected?projections:['usage']:[];
 function expire(state){if(state.deadline===null||now()<state.deadline||state.readDone)return;for(const projection of expectedProjections(state))if(state.reads[projection]==='pending')state.reads[projection]='unknown';if(state.policy===null)lack('gateway_source_unavailable',state.trace);else if(expectedProjections(state).some(key=>state.reads[key]==='unknown'))lack('reader_timeout',state.trace);state.readDone=true;state.readerAbort.abort();}
 function enqueue(state){if(state.epochClosed)return;if(now()<state.nextPoll)return;if(state.queued||state.reading||state.readDone||state.clientEnd===null||!state.policy?.accepted)return;state.queued=true;queue.push(state);peakQueued=Math.max(peakQueued,queue.length-head);if(!pumping){pumping=true;queueMicrotask(pump);}}
 async function round(state){
  const expected=readExpected(state);if(!expected){for(const key of expectedProjections(state))state.reads[key]='unknown';return;}
  const admissions=expected.admissions;if(expected.candidateCount>1||Array.isArray(admissions)&&admissions.length>1){hard('stage_admission_conflict',state);return;}
  // A committed ID, never a transaction-local or inferred HTTP status, binds reads.
  if(!Array.isArray(admissions)||admissions.length!==1||expected.candidateCount!==1||admissions[0].confirmed!==1&&admissions[0].confirmed!==true)return;
  const requestId=admissions[0].request_id;if(!id(requestId)){lack('admission_unconfirmed',state.trace);return;}
  for(const projection of expectedProjections(state)){
   if(state.reads[projection]!=='pending'||state.readerAbort.signal.aborted)continue;expire(state);if(state.readDone)break;
   const queryStartNs=now().toString();let result;readerCalls++;try{result=await reader.read({projection,requestId,expected,signal:state.readerAbort.signal});}catch{result={state:'unknown',readVerified:false};}const atNs=now().toString();
   if(result?.state==='mismatch'){const reason=mismatchReasons.has(result.reason)?result.reason:'unclassified';readerMismatches[projection][reason]=(readerMismatches[projection][reason]??0)+1;if(projection==='body'&&reason==='body_digest_mismatch'){const part=['input','output'].includes(result.mismatchPart)?result.mismatchPart:'unclassified',capture=['complete','partial','truncated'].includes(result.mismatchState)?result.mismatchState:'unclassified';bodyDigestMismatches[part][capture]=(bodyDigestMismatches[part][capture]??0)+1;}hard('stage_reader_mismatch',state);state.reads[projection]='mismatch';continue;}
   if(state.readerAbort.signal.aborted||now()>state.deadline){state.reads[projection]='unknown';lack('reader_timeout',state.trace);continue;}
   if(result?.state==='ready'&&result.readVerified===true){sources.reader.observe({trace:state.trace,type:'visibility',projection,queryStartNs,atNs,ready:true,readVerified:true});state.reads[projection]='ready';}
   else if(result?.state==='pending'&&result.readVerified===false){if(projectionPending.has(result.reason))sources.reader.observe({trace:state.trace,type:'visibility',projection,queryStartNs,atNs,ready:false,readVerified:false});}
   else{state.reads[projection]='unknown';lack('reader_unavailable',state.trace);}
  }
 }
 function pump(){pumping=false;while(!closed&&running<readerConcurrency&&head<queue.length){const state=queue[head++];state.queued=false;if(!active.has(state.trace)||state.epochClosed||state.readDone)continue;expire(state);if(state.readDone)continue;state.reading=true;running++;peakRunning=Math.max(peakRunning,running);void round(state).catch(()=>lack('reader_unavailable',state.trace)).finally(()=>{state.reading=false;state.nextPoll=now()+BigInt(pollIntervalMs)*1000000n;running--;tick(state);wake();pump();});}if(head>1024&&head*2>queue.length){queue.splice(0,head);head=0;}if(head===queue.length){queue.length=0;head=0;}}
 function tick(state){
  if(closed||state.epochClosed||!active.has(state.trace))return;if(!gatewayOpen()){state.epochClosed=true;clearTimer(state);state.readerAbort.abort();lack('source_epoch_closed',state.trace);return;}expire(state);
  if(state.clientEnd!==null&&state.policy!==null&&!state.readDone){const required=expectedProjections(state);if(required.every(key=>state.reads[key]!=='pending'))state.readDone=true;else enqueue(state);}
  let witness;try{witness=synchronous(view.readHandlerWitness(state.trace));if(witness?.passed===false)hard('stage_handler_conflict',state);}catch{lack('handler_unavailable',state.trace);}
  let provenUnsent=false;if(witness?.passed!==false&&state.clientFailed&&!state.gatewaySeen&&!state.dispatched&&!state.upstreamSeen){try{const owner=synchronous(view.readTrace(state.trace));provenUnsent=owner?.scope===view.scope&&owner.source==='model'&&owner.notSent===true;}catch{lack('ledger_unavailable',state.trace);}}const handlerDone=witness?.settled===true&&witness.passed===true||provenUnsent;
  const leasesDone=state.dispatched?leaseNames.every(name=>state.leases.get(name)?.has('acquire')&&state.leases.get(name)?.has('release')):[...state.leases.values()].every(actions=>!actions.has('acquire')||actions.has('release'));
  const helperNeeded=helperRequired&&state.gatewayContentSeen;
  if(helperNeeded&&!state.helperDone&&state.helperDeadline!==null&&now()>=state.helperDeadline){lack('helper_timeout',state.trace);state.helperDone=true;}
  let helperIdentityReady=!helperNeeded;
  if(helperNeeded){const expected=readExpected(state);let owner;try{owner=synchronous(view.readTrace(state.trace));}catch{lack('ledger_unavailable',state.trace);}
   const confirmed=expected?.admissions?.filter(row=>row.confirmed===1||row.confirmed===true)??[];
   if(state.helper&&confirmed.length===1&&confirmed[0].device_id&&confirmed[0].device_id!==state.helper.deviceId)hard('stage_helper_device_conflict',state);
   if(owner?.resultSeen&&expected?.expectedDeviceId){helperIdentityReady=true;if(!state.helper||expected.expectedDeviceId!==state.helper.deviceId)hard('stage_helper_device_conflict',state);}
   else if(state.helperDeadline!==null&&now()>=state.helperDeadline){lack('helper_source_unavailable',state.trace);helperIdentityReady=true;}
  }
  if((state.clientEnd!==null||state.clientFailed)&&helperIdentityReady&&(!helperNeeded||state.helperDone)&&state.helperPending===0&&state.readDone&&handlerDone&&(!state.dispatched&&!state.upstreamSeen||state.upstreamEnd)&&leasesDone&&!state.reading&&!state.queued){
   const expected=readExpected(state);if(state.policy?.accepted===false&&(expected?.admissions?.some(row=>row.confirmed===1||row.confirmed===true)||state.upstreamSeen))hard('stage_admission_policy_conflict',state);if(expected?.admissions?.some(row=>row.confirmed!==1&&row.confirmed!==true))lack('admission_unconfirmed',state.trace);
   collector.finalize(state.trace);clearTimer(state);active.delete(state.trace);finished++;wake();return;
  }
  schedule(state);
 }
 function context({trace}={}){
  if(stopping||closed||!id(trace))return null;if(!gatewayOpen()){lack('source_epoch_closed');return null;}let owner;try{owner=synchronous(view.readTrace(trace));}catch{lack('ledger_unavailable');return null;}
  if(owner&&owner.scope!==view.scope){hard('stage_client_owner_conflict');return null;}if(owner&&owner.source!=='model')return null;if(!owner||owner.registered!==1&&owner.registered!==true){lack('ledger_unavailable');return null;}
  if(active.has(trace)){hard('stage_duplicate_client_context',active.get(trace));return null;}if(active.size>=maxActive){lack('active_trace_budget');return null;}
  if(!collector.begin({trace,group}))return null;
  const helper=helperRequired?synchronous(getHelperForTrace({trace,owner})):null;
  const state={trace,helper,helperDone:false,helperPending:0,helperTail:Promise.resolve(),helperDeadline:null,gatewayContentSeen:false,policy:null,clientEnd:null,clientFailed:false,deadline:null,reads:{body:'pending',usage:'pending'},readDone:false,readerAbort:new AbortController(),leases:new Map(),gatewaySeen:false,dispatched:false,upstreamSeen:false,upstreamEnd:false,epochClosed:false,queued:false,reading:false,nextPoll:0n,timer:null,hard:new Set()};active.set(trace,state);started++;peakActive=Math.max(peakActive,active.size);schedule(state);return {trace};
 }
 function route(role,value){
  if(!value||!id(value.trace))return false;const state=active.get(value.trace);if(!state){let owner;try{owner=synchronous(view.readTrace(value.trace));}catch{lack('ledger_unavailable');return false;}if(owner?.scope!==view.scope||owner.source!=='model')return true;return sources[role].observe(value);}
  const observed=sources[role].observe(value);
  if(role==='gateway'){
   state.gatewaySeen=true;if(value.type==='content')state.gatewayContentSeen=true;
   if(value.type==='accepted'&&observed&&state.policy===null){state.policy={accepted:value.accepted,recordingExpected:value.recordingExpected};}
   if(value.type==='dispatch')state.dispatched=true;
   if(value.type==='lease'&&leaseNames.includes(value.lease)){let actions=state.leases.get(value.lease);if(!actions){actions=new Set();state.leases.set(value.lease,actions);}actions.add(value.action);}
  }else if(role==='upstream'){state.upstreamSeen=true;if(value.type==='end')state.upstreamEnd=true;}
  else if(role==='client'&&value.type==='end'&&observed&&state.clientEnd===null&&ns(value.atNs)){
   const remote=remoteBinding?capacityRemoteStageReceiptBinding(value.receipt):null;if(remoteBinding&&(!remote||remote.source!==remoteBinding.stageSource)){lack('client_source_unavailable',state.trace);return false;}
   state.helperDeadline=now()+BigInt(helperTimeoutMs)*1000000n;state.clientEnd=remote?remote.range.upperNs:value.atNs;state.deadline=BigInt(state.clientEnd)+BigInt(targets.bodyVisibility.p99)*1000000n;
   // Establish both watches at actual callback receipt, before any query/job.
   const atNs=now().toString();for(const projection of projections)sources.reader.observe({trace:state.trace,type:'watch-start',projection,atNs});
  }
  tick(state);return observed;
 }
 const pick=value=>Object.fromEntries(endpointKeys.filter(key=>Object.hasOwn(value,key)).map(key=>[key,value[key]]));
 const observeClient=value=>{if(!value||!id(value.trace))return false;if(!remoteBinding&&value.receipt){lack('client_source_unavailable',value.trace);return false;}if(['content','end'].includes(value.type))return route('client',pick(value));if(value.type==='cancel'){const observed=route('client',pick(value)),state=active.get(value.trace);if(state){lack('client_source_unavailable',state.trace);state.clientFailed=true;state.helperDeadline??=now()+BigInt(helperTimeoutMs)*1000000n;state.readDone=true;state.readerAbort.abort();tick(state);}return observed;}const state=active.get(value.trace);lack(value.type==='timeout'?'client_timeout':value.type==='failed'?'client_failed':'client_source_unavailable',state?.trace);if(state&&(['timeout','failed'].includes(value.type)||value.type==='unavailable'&&value.reason==='cancel_before_observation')){state.clientFailed=true;state.helperDeadline??=now()+BigInt(helperTimeoutMs)*1000000n;state.readDone=true;state.readerAbort.abort();tick(state);}return true;};
 clientStageObserver=createCapacityClientStageObserver({context,observe:observeClient,...(remoteBinding?{remoteSource:remoteBinding.stageSource}:{})});const clientProof=bind('client',clientStageObserver.source);sources.client=collector.source({role:'client',proof:clientProof});
 function observeGateway(value){if(value?.kind==='gateway-request-start'){const state=active.get(value.trace);if(state)state.gatewaySeen=true;return true;}if(value?.kind==='gateway-streaming-stage'){if(value.type==='source-unavailable'){if(active.has(value.trace))lack('gateway_source_unavailable',value.trace);return true;}if(value.type==='transport-end')return true;if(!gatewayTypes.has(value.type)){lack('gateway_source_unavailable',active.has(value.trace)?value.trace:undefined);return false;}return route('gateway',pick(value));}if(['gateway-handler-settled','gateway-handler-unobservable','gateway-admitted','gateway-admission-unconfirmed'].includes(value?.kind))notify(value.trace);return true;}
 function observeUpstream(value){if(!value||!id(value.trace))return true;const state=active.get(value.trace);if(value.kind==='upstream-terminal')return route('upstream',{trace:value.trace,type:'end',atNs:value.atNs,outcome:value.outcome});if(value.kind==='upstream-cancel')return route('upstream',{trace:value.trace,type:'cancel',atNs:value.atNs});if(value.kind==='upstream-dispatch'&&state){state.upstreamSeen=true;tick(state);}return true;}
 async function observeHelper({binding,event}={}){
  if(!helperRequired)return false;const state=active.get(event?.trace);
  if(!state){let owner;try{owner=synchronous(view.readTrace(event?.trace));}catch{lack('ledger_unavailable');}if(owner?.scope===view.scope&&owner.source==='model'&&binding!==synchronous(getHelperForTrace({trace:event.trace,owner}))){hard('stage_helper_owner_conflict');return false;}return event?.type==='end'?true:route('helper',{trace:event?.trace,type:'content'});}
  if(!binding||state.helper!==binding||!capacityWindowsHelperObserverBinding(binding.observer)){hard('stage_helper_owner_conflict',state);return false;}
  if(state.helperPending>=128){lack('helper_source_unavailable',state.trace);return false;}state.helperPending++;
  const task=state.helperTail.then(async()=>{try{
   if(event.type==='unavailable'){lack('helper_source_unavailable',state.trace);state.helperDone=true;return true;}
   const receipt=await binding.observer.resolveDelivery(event);if(!receipt){lack('helper_source_unavailable',state.trace);if(event.type==='end')state.helperDone=true;return true;}
   const row=capacityWindowsHelperDeliveryReceipt(receipt);if(row?.observer!==binding.observer||row.event!==event){hard('stage_helper_owner_conflict',state);return false;}
   const value={trace:event.trace,type:event.type,receipt,...(event.type==='content'?{ordinal:event.ordinal,contentKind:event.contentKind,hash:event.hash,bytes:event.bytes}:{outcome:event.complete?'ok':'failed'})};
   const accepted=sources.helper.observe(value);if(event.type==='end')state.helperDone=true;return accepted;
  }catch{lack('helper_source_unavailable',state.trace);if(event?.type==='end')state.helperDone=true;return true;}
  finally{state.helperPending--;tick(state);wake();}});
  state.helperTail=task.then(()=>{},()=>{});return task;
 }
 function notify(trace){const state=active.get(trace);if(state){state.nextPoll=0n;tick(state);}return !!state;}
 async function checkStatus(){
  if(remoteBinding){if(capacityRequestDriverRemoteSourceBinding(remoteDriver)!==remoteBinding)lack('source_epoch_closed');const remote=remoteDriver.status();if(remote.pendingCallbacks||remote.remoteStages?.pending)lack('observer_pending');if(remote.callbackFailures||remote.callbackBudgetRejected||remote.remoteStages?.failures||remote.observationDropped)lack('observer_dropped');}
  const cap=clientStageObserver.status();if(cap.failures||cap.unavailable)lack('observer_failed');if(cap.pendingCallbacks)lack('observer_pending');
  try{const status=upstreamStatus();if(status.observerFailures)lack('observer_failed');if(!gatewayOpen())lack('source_epoch_closed');if(readObserverStatus){const value=await readObserverStatus();if(!value||typeof value!=='object')lack('observer_failed');else{const visit=(row,depth=0,branch=null)=>{if(!row||typeof row!=='object'||depth>5)return;if(row.failures||row.eventDropped||row.parentEventDropped||row.unobservableHandlers||row.dropped)lack('observer_dropped');if(row.pendingCallbacks||row.pendingHandlers||row.pendingAdmissions||branch==='stageProbe'&&row.pending)lack('observer_pending');for(const key of ['parent','child','helper','gateway','streamingStages','handler','requestBodies','preparation','handoff','subscription','api','probe','stageProbe'])visit(row[key],depth+1,key);};visit(value);}}else lack('observer_failed');}catch{lack('observer_failed');}
 }
 const status=()=>Object.freeze({started,finished,active:active.size,timers:timerCount,queued:queue.length-head,running,peakActive,peakQueued,peakRunning,readerCalls,hardFailures,stopping,closed});
 async function collect(){await checkStatus();try{const storage=await view.flush();if(storage?.durable!==true||storage.incomplete||storage.writeFailures)lack('ledger_unavailable');}catch{lack('ledger_unavailable');}const report=collector.collect(),knownFalse=hardFailures>0;return Object.freeze({...report,passed:knownFalse?false:missing.size&&report.passed!==false?null:report.passed,incomplete:report.incomplete||missing.size>0,runtime:{...status(),readerMismatches:structuredClone(readerMismatches),bodyDigestMismatches:structuredClone(bodyDigestMismatches),issues:issues.map(value=>({...value})),missing:[...missing]}});}
 async function drain({timeoutMs=30000,signal}={}){if(!Number.isSafeInteger(timeoutMs)||timeoutMs<1)throw Error('capacity_stage_runtime_drain');const deadline=Date.now()+timeoutMs;while(active.size||running||queue.length-head){if(signal?.aborted||Date.now()>=deadline){lack('observer_pending');break;}await new Promise(resolve=>{let timer;const done=()=>{clearTimeout(timer);waiters.delete(done);resolve();};waiters.add(done);timer=setTimeout(done,Math.min(50,deadline-Date.now()));});}return collect();}
 async function close(options={}){if(closed)return collect();if(closePromise)return closePromise;stopping=true;closePromise=(async()=>{const report=await drain(options);if(active.size||running||queue.length-head)return report;collector.close();clientStageObserver.close();closed=true;return Object.freeze({...report,runtime:{...report.runtime,...status()}});})();try{return await closePromise;}finally{closePromise=null;}}
 return Object.freeze({clientStageObserver,observeGateway,observeUpstream,observeHelper,notify,drain,collect,close,status});
}
