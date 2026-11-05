import {streamingStageQualification} from './streaming-stage-report.mjs';
import {performance} from 'node:perf_hooks';
import {setTimeout as delay} from 'node:timers/promises';
import {credentialFingerprints} from './http-client.mjs';
import {runOpenLoop} from './driver.mjs';
import {targets} from './scenarios.mjs';
import {Histogram,createCapacityMetrics} from './metrics.mjs';
import {ownedCapacityTarget} from './gateway-adapter.mjs';
import {createCapacityBrowserAssessment} from './browser-assessment.mjs';
import {createUiTrafficLedger} from './ui-traffic-ledger.mjs';

const fail=code=>{throw new Error(code);};
const uiFailureReports=new WeakMap();
export const getUiFailureReport=error=>uiFailureReports.get(error);
const isCancellation=(error,signal)=>signal?.aborted===true&&(error===signal.reason||error?.name==='AbortError'||error?.cause===signal.reason);

const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};
const categoryCycle=['list','list','list','list','list','detail','detail','summary','summary','configuration'];
const coreSeries=[...['requests','members','sessions'].flatMap(view=>['default','maximum','deep'].map(variant=>({view,variant,budgetMs:1500}))),...['statistics','search'].map(view=>({view,variant:'default',budgetMs:3000}))];
export function uiAcceptancePlan({profile='selfcheck',actionQps=1,durationMs,maxDurationMs,deepPages=profile==='formal'?100:3}={}){
 if(!['formal','selfcheck'].includes(profile)||!Number.isFinite(actionQps)||actionQps<=0||actionQps>5||!Number.isInteger(deepPages)||deepPages<2||deepPages>1000)fail('capacity_ui_plan_configuration');
 if(profile==='formal'&&(maxDurationMs===undefined||maxDurationMs===null))fail('capacity_ui_max_duration_required');
 const minSamples=profile==='formal'?1000:5,series=coreSeries.map(row=>({...row,minSamples})),minimumActions=series.reduce((sum,row)=>sum+row.minSamples,0),minimumScheduledMs=Math.ceil(minimumActions/actionQps*1000);
 durationMs??=profile==='formal'?Math.max(1800000,minimumScheduledMs):Math.max(3000,minimumScheduledMs);
 maxDurationMs??=durationMs;
 if(!Number.isSafeInteger(maxDurationMs)||maxDurationMs<durationMs||maxDurationMs>86400000)fail('capacity_ui_duration_below_sample_budget');
 if(!Number.isSafeInteger(durationMs)||durationMs<1||durationMs>86400000||profile==='formal'&&durationMs<Math.max(1800000,minimumScheduledMs))fail('capacity_ui_duration_below_sample_budget');
 return freeze({id:profile==='formal'?'ui-additional-load':'ui-qualification',profile,scope:'independent-ui-additional-load',normalMixReplaced:false,durationMs,maxDurationMs,actionQps,deepPages,minimumActions,minimumScheduledMs,model:{qps:profile==='formal'?10:2,employees:profile==='formal'?60:2,upstreamMs:profile==='formal'?[3000,6000]:[1000,1000],payloadBytes:profile==='formal'?102400:1024},baselineAdmin:{qps:profile==='formal'?5:2,actors:5,mix:{list:0.5,detail:0.2,summary:0.2,configuration:0.1}},series:Object.freeze(series.map(Object.freeze)),coverage:'three primary lists and statistics/search; other primary views require signed coverage proof'});
}

function seriesTracker(plan,origin){
 const rows=new Map(plan.series.map(spec=>[[spec.view,spec.variant].join('/'),{...spec,histogram:new Histogram(),actors:new Set(),success:0,businessSuccess:0,outsideModelLoad:0,liveOrderDriftRows:0,paginationConsistency:null,failed:0,cancelled:0,errorCounts:{},skipped:0,unverifiedEnds:0,inflight:0,maxDepth:0,windows:new Map()}])),chains=new Map();let cursor=0;
 function choose(actor){const chain=chains.get(actor);if(chain)return rows.get(chain);const candidates=[...rows.entries()];for(let i=0;i<candidates.length;i++){const [key,row]=candidates[(cursor+i)%candidates.length];if(row.success+row.inflight<row.minSamples||!row.actors.has(actor)){cursor=(cursor+i+1)%candidates.length;if(row.variant==='deep')chains.set(actor,key);return row;}}return candidates[(cursor++)%candidates.length][1];}
 function record(row,actor,result,at=performance.now()){row.inflight--;if(result.cancelled){row.cancelled++;chains.delete(actor);return;}if(result.skipped){row.skipped++;if(result.skipped==='pagination_end'&&result.paginationEndVerified!==true)row.unverifiedEnds++;chains.delete(actor);return;}if(!result.businessVerified||!Number.isFinite(result.durationMs)||result.durationMs<0){row.failed++;const code=/^capacity_[a-z0-9_]+$/.test(result.error??'')?result.error:'capacity_ui_action_failed';row.errorCounts[code]=(row.errorCounts[code]??0)+1;chains.delete(actor);return;}row.businessSuccess++;row.liveOrderDriftRows+=result.liveOrderDriftRows??0;row.paginationConsistency=result.paginationConsistency??row.paginationConsistency;if(result.activeModelBefore>0&&result.activeModelAfter>0){row.success++;row.histogram.add(result.durationMs);row.actors.add(actor);const bucket=Math.floor(Math.max(0,at-origin)/300000),window=row.windows.get(bucket)??new Histogram();window.add(result.durationMs);row.windows.set(bucket,window);}else row.outsideModelLoad++;row.maxDepth=Math.max(row.maxDepth,result.depth??1);if(row.variant==='deep'&&result.depth>=plan.deepPages)chains.delete(actor);}
 function collect(){return [...rows.values()].map(({histogram,actors,inflight,windows,...row})=>({...row,...histogram.view(),actors:actors.size,inflight,windows:[...windows].map(([index,hist])=>({startMs:index*300000,endMs:(index+1)*300000,...hist.view()}))}));}
 return {choose,record,collect,complete:()=>[...rows.values()].every(row=>row.success>=row.minSamples&&row.actors.size>=5)};
}

// Callback-only scheduling core. It creates no credentials, browsers or network
// targets, so the public owned-target boundary remains in the outer function.
export async function runUiScheduledPhase({plan,sendModel,sendAdmin,onResult=async()=>{},measure,begin=()=>{},end=()=>{},signal,onProgress=()=>{}}={}){
 if(!plan||typeof sendModel!=='function'||typeof sendAdmin!=='function'||typeof measure!=='function'||typeof begin!=='function'||typeof end!=='function')fail('capacity_ui_phase_configuration');
 const started=performance.now(),metrics=createCapacityMetrics(),tracker=seriesTracker(plan,started),busy=new Set(),scene={id:plan.id,durationMs:plan.maxDurationMs,modelQps:plan.model.qps,adminQps:plan.baselineAdmin.qps,employees:plan.model.employees,administrators:5,upstreamMs:plan.model.upstreamMs,payloadBytes:plan.model.payloadBytes},failures=[],interruptions=[],backgroundOutcomes={succeeded:0,failed:0,cancelled:0},backgroundSeries=new Map();let active=false,windowEnd=null,completedUi=0,stopReason='sample-budget-exhausted';const stopArrivals=new AbortController(),arrivalSignal=AbortSignal.any([stopArrivals.signal,signal].filter(Boolean)),collectedReason=new Error('capacity_ui_arrivals_stopped');
 const context=(kind,event)=>({...event,scene,actorIndex:event.sequence%(kind==='model'?plan.model.employees:5),category:categoryCycle[event.sequence%10],signal});
 const send=kind=>async event=>{let recorded=false;try{
  const input=context(kind,event),result=await (kind==='model'?sendModel(input):sendAdmin(input)),isWrite=kind==='baseline'&&result.method!=='GET',success=result.success===true&&(kind!=='model'||result.complete===true)&&(!isWrite||result.businessVerified===true);
  const cancelled=(result.cancelled===true||result.error==='client_cancelled')&&signal?.aborted===true&&!success&&!(result.status>=400);backgroundOutcomes[cancelled?'cancelled':success?'succeeded':'failed']++;recorded=true;const seriesKey=[kind==='model'?'ui-background-model':result.operation??'ui-baseline-unbound',result.variant??'steady',kind==='model'?'model':isWrite?'write':'read'].join('/'),counts=backgroundSeries.get(seriesKey)??{series:seriesKey,succeeded:0,failed:0,cancelled:0};counts[cancelled?'cancelled':success?'succeeded':'failed']++;backgroundSeries.set(seriesKey,counts);
  metrics.record({operation:kind==='model'?'ui-background-model':result.operation??'ui-baseline-unbound',scenario:plan.id,category:kind==='model'?'model':isWrite?'write':'read',group:'ui-background',variant:result.variant??'steady',at:Date.now(),durationMs:result.durationMs,success,error:success?null:'ui_background_request_failed'});
  const safeResult=Object.fromEntries(['operation','method','variant','status','success','complete','httpEnded','wireSent','startedAt','finishedAt','durationMs','receivedBytes','firstContentAt','headersAt','connectedAt','tlsAt','reusedSocket','bodyHash','records','trace'].filter(key=>Object.hasOwn(result,key)).map(key=>[key,result[key]]));safeResult.validatedSuccess=success;safeResult.error=cancelled?'client_cancelled':success?null:'request_failed';await onResult({scenario:plan.id,kind:kind==='model'?'model':'admin',event:{sequence:event.sequence,actorIndex:input.actorIndex,scheduledAt:event.scheduledAt,startedAt:event.startedAt,arrivalLagMs:event.arrivalLagMs},result:safeResult});
 }catch(error){if(isCancellation(error,signal)){if(!recorded)backgroundOutcomes.cancelled++;return;}if(!recorded)backgroundOutcomes.failed++;const code=/^capacity_[a-z0-9_]+$/.test(error?.message??'')?error.message:'capacity_ui_background_failed';throw Object.assign(new Error(code),{code});}};
 const ui=async event=>{
  const actionAt=performance.now(),actor=event.sequence%5,row=tracker.choose(actor);row.inflight++;
  if(busy.has(actor)){tracker.record(row,actor,{businessVerified:false});failures.push('ui_scheduled_actor_busy');return;}
  busy.add(actor);let result;
  try{result=await measure({actorIndex:actor,view:row.view,variant:row.variant,signal});}catch(error){if(isCancellation(error,signal))result={cancelled:true};else{result={businessVerified:false};failures.push('ui_action_threw');}}
  finally{busy.delete(actor);}
  tracker.record(row,actor,result,actionAt);completedUi++;
  try{await onProgress({kind:'ui-action',view:row.view,variant:row.variant,completed:completedUi});}catch(error){if(isCancellation(error,signal))interruptions.push('ui_progress_cancelled');else failures.push('ui_progress_callback_failed');}
 };
 begin();active=true;
 const drive=(qps,callback)=>runOpenLoop({durationMs:plan.maxDurationMs,segments:[{startMs:0,endMs:plan.maxDurationMs,qps}],scenario:plan.id,signal:arrivalSignal,metrics,send:callback});
 const tasks=[drive(plan.model.qps,send('model')),drive(plan.baselineAdmin.qps,send('baseline')),drive(plan.actionQps,ui)];
 // Attach handlers immediately; independent producer failure must not become an
 // unhandled rejection while waiting for the scheduled observation deadline.
 const outcomes=Promise.allSettled(tasks);
 try{
  for(;;){signal?.throwIfAborted();const elapsed=performance.now()-started;if(elapsed>=plan.durationMs&&tracker.complete()){stopReason='samples-collected';break;}if(elapsed>=plan.maxDurationMs)break;await delay(Math.max(1,Math.min(25,plan.maxDurationMs-elapsed)),undefined,{signal});}
 }catch(error){if(isCancellation(error,signal)){stopReason='cancelled';interruptions.push('ui_measurement_cancelled');}else{stopReason='failed';failures.push('ui_measurement_failed');}}
 finally{if(active){windowEnd=performance.now();try{end();}catch{failures.push('ui_window_close_failed');}active=false;}stopArrivals.abort(collectedReason);}
 for(const result of await outcomes)if(result.status==='rejected'&&result.reason!==collectedReason&&result.reason?.cause!==collectedReason&&!isCancellation(result.reason,signal))failures.push('ui_load_generator_failed');
 const rows=tracker.collect(),observedMs=windowEnd-started,missing=[...interruptions];if(stopReason==='sample-budget-exhausted'&&!tracker.complete())missing.push('ui_sample_budget_exhausted');
 for(const row of rows){if(row.success<row.minSamples)missing.push('ui_samples_below_target:'+row.view+'/'+row.variant);if(row.actors<5)missing.push('ui_five_actors_not_covered:'+row.view+'/'+row.variant);if(row.unverifiedEnds)missing.push('ui_pagination_end_unverified:'+row.view+'/'+row.variant);if(row.failed)failures.push('ui_business_failure:'+row.view+'/'+row.variant);if(row.p95!==null&&row.p95>row.budgetMs)failures.push('ui_dom_budget:'+row.view+'/'+row.variant);for(const window of row.windows)if(window.p95>row.budgetMs)failures.push('ui_five_minute_dom_budget:'+row.view+'/'+row.variant+'/'+window.startMs);}
 const measured=metrics.view();if(measured.dropped||measured.arrivals.missed)missing.push('ui_load_generation_incomplete');if(measured.violations.length)failures.push('ui_background_driver_failure');if(assessUiBackgroundOutcomes([...backgroundSeries.values()]).passed===false)failures.push('ui_background_success_rate');
 return {scope:'independent-ui-additional-load',normalMixReplaced:false,window:{observedMs,declaredMs:plan.durationMs,maxDurationMs:plan.maxDurationMs,stopReason,finished:!signal?.aborted&&observedMs>=plan.durationMs},series:rows,metrics:measured,backgroundOutcomes:{...backgroundOutcomes,series:[...backgroundSeries.values()]},interruptions,failures:[...new Set(failures)],missing:[...new Set(missing)]};
}

// Raw metrics intentionally retain cancelled outcomes; only the qualification
// denominator excludes transport-confirmed active cancellation for that series.
export function assessUiBackgroundOutcomes(series){
 const rows=series.map(row=>({...row,normalAttempts:row.succeeded+row.failed,successRate:row.succeeded+row.failed?row.succeeded/(row.succeeded+row.failed):null}));
 return {passed:rows.some(row=>row.successRate!==null&&row.successRate<targets.successRate)?false:rows.some(row=>row.normalAttempts)?true:null,series:rows};
}

export function assessUiHttpOutcomes(traffic){
 const groups=new Map();for(const row of traffic.counts??[]){const key=row.phase+'/'+row.source,c=groups.get(key)??{phase:row.phase,source:row.source,attempts:0,succeeded:0,failed:0,cancelled:0,observedNonSuccess:0,cancelledHttpNonSuccess:0,statusClasses:{}};c.attempts+=row.normalAttempts??0;c.succeeded+=row.httpSucceeded??0;c.failed+=row.httpFailed??0;c.cancelled+=row.clientCancelled??0;c.observedNonSuccess+=row.httpNonSuccess??0;c.cancelledHttpNonSuccess+=row.cancelledHttpNonSuccess??0;for(const [status,count] of Object.entries(row.statusClasses??{}))c.statusClasses[status]=(c.statusClasses[status]??0)+count;groups.set(key,c);}
 const rows=[...groups.values()].map(row=>({...row,successRate:row.attempts?row.succeeded/row.attempts:null,qualifiedAttempts:row.attempts+row.cancelledHttpNonSuccess,qualifiedSuccessRate:row.attempts+row.cancelledHttpNonSuccess?row.succeeded/(row.attempts+row.cancelledHttpNonSuccess):null})),failures=rows.filter(row=>row.qualifiedSuccessRate!==null&&row.qualifiedSuccessRate<targets.successRate).map(row=>'ui_http_success_rate:'+row.phase+'/'+row.source);return {passed:failures.length?false:rows.some(row=>row.attempts>0)?true:null,target:targets.successRate,groups:rows,failures};
}

export function assessUiQualification({phase,traffic,scopeEvidence,dataReadiness,httpOutcomes,browserStatus,missing=[],failures=[]}={}){
 const gaps=[...missing,...phase?.missing??[]],errors=[...failures,...phase?.failures??[]];
 if(!phase)gaps.push('ui_phase_not_run');
 if(traffic?.passed!==true)gaps.push('ui_wire_reconciliation_not_complete');if(traffic?.passed===false)errors.push('ui_wire_integrity_failed');
 if(scopeEvidence?.transport?.passed!==true)gaps.push('ui_scope_transport_not_complete');if(scopeEvidence?.transport?.passed===false)errors.push('ui_scope_transport_integrity_failed');
 if(scopeEvidence?.dataReconciliation?.passed!==true)gaps.push('ui_model_data_reconciliation_not_complete');if(scopeEvidence?.dataReconciliation?.passed===false)errors.push('ui_model_data_reconciliation_failed');
 if(dataReadiness?.passed!==true)gaps.push('ui_model_data_readiness_incomplete');if(dataReadiness?.passed===false)errors.push('ui_model_data_readiness_failed');
 if(httpOutcomes?.passed!==true)gaps.push('ui_http_outcomes_not_complete');if(httpOutcomes?.passed===false)errors.push(...httpOutcomes.failures);
 if(!browserStatus)gaps.push('ui_browser_status_missing');if(browserStatus?.scriptErrors)errors.push('ui_browser_script_failed');if(browserStatus?.observerErrors)errors.push('ui_browser_observer_failed');if(browserStatus?.observerPending)gaps.push('ui_browser_observer_pending');
 return {passed:!gaps.length&&!errors.length,missing:[...new Set(gaps)],failures:[...new Set(errors)]};
}

const incomplete=(plan,missing)=>({scope:'independent-ui-additional-load',normalMixReplaced:false,plan,qualification:{passed:false,missing},browserLists:{passed:null,missing},browserStatistics:{passed:null,missing}});
export async function runIndependentUiAcceptance({target,launchBrowser,profile=target?.plan?.profile??'selfcheck',actionQps=1,durationMs,maxDurationMs,maxRequests=500000,wirePollMs=250,signal,onProgress=()=>{}}={}){
 if(!Number.isInteger(wirePollMs)||wirePollMs<100||wirePollMs>5000)fail('capacity_ui_wire_poll_configuration');
 const plan=uiAcceptancePlan({profile,actionQps,durationMs,maxDurationMs});let owned=ownedCapacityTarget(target,{profile}),actorPreparation=null;
 if(typeof owned.prepareActorEpoch==='function'){actorPreparation=await owned.prepareActorEpoch({phase:'ui-additional-load',signal});owned=ownedCapacityTarget(target);}else if(profile==='formal')return incomplete(plan,['factory_actor_refresh_not_connected']);
 if(typeof owned.createObservationScope!=='function')return incomplete(plan,['factory_observation_scope_not_connected']);
 const module=await import('./observation-adapter.mjs');
 const entry=owned.entry??{origin:owned.origin};const ledger=createUiTrafficLedger({origin:entry.origin,maxRequests,adminFingerprints:owned.adminActors.map(actor=>credentialFingerprints(actor.headers).cookie),employeeFingerprints:target.employeeActors.map(actor=>credentialFingerprints(actor.headers).authorization)});
 let scope,scopeClosed=false,assessment,cursor=0,phase,scopeEvidence,dataReadiness,formalEvidence,traffic,wireTimer,wirePending,browserStatus,executionError;const missing=[],failures=[],formalMissing=[],formalFailures=[];
 const consumeWire=async()=>{const batch=await scope.readWireEvents({cursor});if(!batch||!Array.isArray(batch.events)||!Number.isSafeInteger(batch.next)||batch.next<cursor||!Number.isSafeInteger(batch.dropped)||batch.dropped<0)fail('capacity_ui_wire_snapshot_invalid');if(batch.dropped)missing.push('ui_wire_events_dropped');for(const event of batch.events)ledger.server(event);cursor=batch.next;};
 const readWire=()=>{if(wirePending)return wirePending;wirePending=consumeWire().finally(()=>{wirePending=null;});return wirePending;};
 try{
  scope=await owned.createObservationScope({id:plan.id+'-'+ledger.scopeId,kind:'ui',observer:ledger.observer});
  if(typeof module.isOwnedObservationScope!=='function'||!module.isOwnedObservationScope(scope))fail('capacity_ui_scope_not_owned');
  if(!['sendModel','sendAdmin','onResult','collect','close','readWireEvents'].every(key=>typeof scope[key]==='function'))throw new Error('capacity_ui_scope_contract_incomplete');
  wireTimer=setInterval(()=>{void readWire().catch(()=>{if(!missing.includes('ui_wire_reader_failed'))missing.push('ui_wire_reader_failed');});},wirePollMs);wireTimer.unref();
  assessment=await createCapacityBrowserAssessment({target,launchBrowser,profile,deepPages:plan.deepPages,observer:scope.observer,entry:owned.entry});
  if(assessment.observerVersion!=='capacity-ui-observer-v1')throw new Error('capacity_ui_browser_observer_not_connected');
  const actualTransport=assessment.transportEvidence;if(profile==='formal'&&(actualTransport?.origin!==entry.origin||new URL(entry.origin).protocol!=='https:'||actualTransport?.verified!==true))formalMissing.push('browser_delivery_tls_transport_not_verified');
  await assessment.prepare();await readWire();const configure=owned.configureScenario??(profile==='selfcheck'?target.onScenarioStart:null);if(typeof configure!=='function')throw new Error('capacity_ui_scenario_configuration_not_connected');await configure({scene:{...plan.model,id:plan.id,durationMs:plan.maxDurationMs,modelQps:plan.model.qps,upstreamMs:plan.model.upstreamMs},signal});
  phase=await runUiScheduledPhase({plan,sendModel:scope.sendModel,sendAdmin:scope.sendAdmin,onResult:scope.onResult,measure:assessment.measure,begin:()=>ledger.beginMeasurement(),end:()=>ledger.endMeasurement(),signal,onProgress});
  await assessment.close();browserStatus=assessment.status();if(browserStatus.scriptErrors)failures.push('ui_browser_script_failed');if(browserStatus.observerErrors)failures.push('ui_browser_observer_failed');if(browserStatus.observerPending)missing.push('ui_browser_observer_pending');assessment=null;
  if(typeof scope.waitForDataReadiness==='function'){dataReadiness=await scope.waitForDataReadiness({signal});if(dataReadiness?.passed===false)failures.push('ui_model_data_readiness_failed');else if(dataReadiness?.passed!==true)missing.push('ui_model_data_readiness_incomplete');}else missing.push('ui_model_data_readiness_not_connected');
  // End the private stage readers and flush their ledger writes before taking
  // the authoritative reconciliation snapshot. Traffic has already drained.
  try{await scope.close();scopeClosed=true;}catch{failures.push('ui_cleanup_failed');executionError={completed:false,cancelled:false,code:'capacity_ui_cleanup_failed'};throw Error('capacity_ui_cleanup_failed');}
  const wireDeadline=performance.now()+5000;for(;;){await readWire();scopeEvidence=await scope.collect();traffic=ledger.collect();if(!traffic.pending&&scopeEvidence.transport?.pending===0||performance.now()>=wireDeadline||signal?.aborted)break;await delay(25,undefined,{signal});}
  clearInterval(wireTimer);await wirePending;
  if(scopeEvidence.transport?.passed!==true)missing.push('ui_scope_transport_not_complete');if(scopeEvidence.transport?.passed===false)failures.push('ui_scope_transport_integrity_failed');
  const httpOutcomes=assessUiHttpOutcomes(traffic);failures.push(...httpOutcomes.failures);if(traffic.passed!==true)missing.push('ui_wire_reconciliation_not_complete');
  if(scopeEvidence?.dataReconciliation?.passed!==true)missing.push('ui_model_data_reconciliation_not_complete');
  if(scopeEvidence?.dataReconciliation?.passed===false||traffic.passed===false)failures.push('ui_data_or_wire_integrity_failed');
  if(typeof scope.verifyFormalContext==='function')formalEvidence=await scope.verifyFormalContext({window:phase.window,plan});
  else formalMissing.push('formal_entry_tls_lan_source_resources_not_connected');
  const formalKeys=['entry','sources','resources','networkBaseline','dataset','protocol','modelWorkload','uiCoverage'];
  for(const key of formalKeys)if(formalEvidence?.[key]?.passed===false)formalFailures.push('formal_ui_evidence_failed:'+key);else if(formalEvidence?.[key]?.passed!==true)formalMissing.push('formal_ui_evidence_missing:'+key);
  if(profile!=='formal')formalMissing.push('qualification_only');
 }catch(error){
  const cancelled=isCancellation(error,signal);executionError={completed:false,cancelled,code:cancelled?'capacity_ui_phase_cancelled':'capacity_ui_phase_failed'};
  if(cancelled)missing.push('ui_execution_cancelled');else failures.push('ui_execution_failed');
 }finally{
  clearInterval(wireTimer);await wirePending?.catch(()=>{});
  // Cleanup cannot replace the already measured evidence, including known false.
  for(const close of [()=>assessment?.close(),()=>scopeClosed?undefined:scope?.close()])try{await close();}catch{failures.push('ui_cleanup_failed');executionError??={completed:false,cancelled:false,code:'capacity_ui_cleanup_failed'};}
  if(assessment)try{browserStatus=assessment.status();}catch{failures.push('ui_browser_status_failed');}
  if(typeof scope?.readWireEvents==='function')try{await readWire();}catch{failures.push('ui_wire_reader_failed');executionError??={completed:false,cancelled:false,code:'capacity_ui_wire_reader_failed'};}
  if(typeof scope?.collect==='function')try{scopeEvidence=await scope.collect();}catch{failures.push('ui_scope_collect_failed');executionError??={completed:false,cancelled:false,code:'capacity_ui_scope_collect_failed'};}
  try{traffic=ledger.collect();}catch{failures.push('ui_traffic_collect_failed');}
  try{ledger.close();}catch{failures.push('ui_cleanup_failed');executionError??={completed:false,cancelled:false,code:'capacity_ui_cleanup_failed'};}
 }
 const httpOutcomes=traffic?assessUiHttpOutcomes(traffic):undefined;
 const stage=streamingStageQualification(scopeEvidence?.streamingStages,{profile});if(stage.failed)failures.push('ui_streaming_stages_failed');if(stage.missing)missing.push('ui_streaming_stages_incomplete');
 const qualification=assessUiQualification({phase,traffic,scopeEvidence,dataReadiness,httpOutcomes,browserStatus,missing,failures}),allFailures=[...qualification.failures,...formalFailures],allMissing=[...qualification.missing,...formalMissing];
 const passed=allFailures.length?false:allMissing.length?null:true,facet={passed,scope:'independent-ui-additional-load',normalMixReplaced:false,failures:[...new Set(allFailures)],missing:[...new Set(allMissing)]};
 const report={scope:'independent-ui-additional-load',normalMixReplaced:false,plan,actorPreparation,phase,traffic,httpOutcomes,scopeEvidence,dataReadiness,formalEvidence,browserStatus,qualification,browserLists:{...facet,series:phase?.series.filter(row=>row.budgetMs===1500)??[]},browserStatistics:{...facet,series:phase?.series.filter(row=>row.budgetMs===3000)??[]}};
 if(executionError){report.execution=executionError;const error=new Error(executionError.code);uiFailureReports.set(error,freeze(report));throw error;}
 return report;
}
