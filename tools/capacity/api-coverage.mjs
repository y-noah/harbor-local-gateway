import {streamingStageQualification} from './streaming-stage-report.mjs';
import {createHash} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {performance} from 'node:perf_hooks';
import {runOpenLoop} from './driver.mjs';
import {createCapacityMetrics,Histogram} from './metrics.mjs';
import {targets,operations,apiClasses} from './scenarios.mjs';
import {ownedCapacityTarget} from './gateway-adapter.mjs';
import {isOwnedObservationScope,capacityObservedApiClass} from './observation-adapter.mjs';
import {getCoverageFactoryFailureReason,getCoverageFactoryPendingReason} from './coverage-factory.mjs';
import {getPendingCoverageReason,getPreparedCoverageLease,getCoverageInventoryFailureReason} from './inventory-adapter.mjs';
import {capacityWireSignature} from './gateway-probe.mjs';
import {sanitizeCapacityNetworkFailure,capacityNetworkFailureFlags} from './http-client.mjs';

const fail=code=>{throw new Error(code);};
const operationIds=new Set(operations.map(row=>row.id));
const variants=new Set(['default','maximum','deep','filtered-sorted','cold','steady']);
const categories=['list','list','list','list','list','detail','detail','summary','summary','configuration'];
const reports=new WeakMap(),epochErrors=new WeakMap();
const pendingReason=error=>getPendingCoverageReason(error)??getCoverageFactoryPendingReason(error)??epochErrors.get(error);
const failEpoch=()=>{const error=new Error('capacity_api_coverage_actor_epoch_incomplete');epochErrors.set(error,'api_coverage_actor_epoch_incomplete');throw error;};
export function apiCoverageActorRefreshDue(evidence,now=Date.now()){
 if(evidence?.passed!==true||!Number.isSafeInteger(evidence.epoch)||!Number.isFinite(evidence.startedAt)||!Number.isFinite(evidence.finishedAt)||evidence.finishedAt<evidence.startedAt||!Number.isFinite(evidence.cookieMaxAgeSeconds)||evidence.cookieMaxAgeSeconds<=0||!Number.isFinite(now)||now<evidence.startedAt)return null;
 return now-evidence.startedAt>=evidence.cookieMaxAgeSeconds*500;
}
export const getApiCoverageFailureReport=error=>reports.get(error);
const httpFailure=status=>Number.isInteger(status)&&(status<200||status>=300);
const cancelled=(error,signal)=>signal?.aborted===true&&(error===signal.reason||error?.name==='AbortError'||error?.cause===signal.reason);
const unique=values=>[...new Set(values)];
const verdict=(failures,missing)=>failures.length?false:missing.length?null:true;
const freeze=value=>{if(value&&typeof value==='object'){for(const child of Object.values(value))freeze(child);Object.freeze(value);}return value;};

// Reserve one arrival interval before expiry so a batch never intentionally
// schedules an expired handle; zero remaining runway is incomplete evidence.
export function apiCoverageExecutableCount({count,remainingMs,probeQps}){
 if(!Number.isInteger(count)||count<1||!Number.isFinite(remainingMs)||!Number.isFinite(probeQps)||probeQps<=0)return 0;
 return Math.min(count,Math.max(0,Math.floor(remainingMs*probeQps/1000)));
}

// Only meaningful variants actually supplied by the owned inventory are used.
// Aliases reference one physical HTTP sample group and retain all purpose gates.
export function apiCoveragePlan({profile='selfcheck',inventory,maxDurationMs,probeQps=1,batchSize=64}={}){
 if(!['formal','selfcheck'].includes(profile)||!Number.isSafeInteger(maxDurationMs)||maxDurationMs<1||maxDurationMs>86400000||!Number.isFinite(probeQps)||probeQps<=0||probeQps>5||!Number.isInteger(batchSize)||batchSize<1||batchSize>1000||!Array.isArray(inventory)||!inventory.length||inventory.length>1000)fail('capacity_api_coverage_configuration');
 const groups=new Map();
 for(const item of inventory){
  const purposeIds=item.purposes??[item.operationId],method=item.method,canonicalHttp=item.canonicalHttp,variant=item.variant,batchCapacity=item.batchCapacity??batchSize;
  if(item.available!==undefined&&typeof item.available!=='boolean'||!Object.hasOwn(apiClasses,item.workloadClass)||!Number.isInteger(batchCapacity)||batchCapacity<1||batchCapacity>1000||!operationIds.has(item.operationId)||!Array.isArray(purposeIds)||!purposeIds.length||purposeIds.some(id=>!operationIds.has(id))||!variants.has(variant)||typeof canonicalHttp!=='string'||!canonicalHttp.startsWith(method+' ')||capacityWireSignature(method,canonicalHttp.slice(method.length+1))!==canonicalHttp||!['GET','POST','PATCH','PUT','DELETE'].includes(method)||item.workloadClass!==capacityObservedApiClass({method,route:canonicalHttp.slice(method.length+1)})||![item.apiBudget?.p95,item.apiBudget?.p99].every(value=>Number.isFinite(value)&&value>0)||item.apiBudget.p99<item.apiBudget.p95)fail('capacity_api_coverage_inventory_invalid');
  if(method!=='GET'&&purposeIds.some(id=>id!==item.operationId))fail('capacity_api_coverage_write_purposes_combined');
  if(item.handleTtlMs!==undefined&&(!Number.isSafeInteger(item.handleTtlMs)||item.handleTtlMs<1))fail('capacity_api_coverage_inventory_invalid');
  const key=canonicalHttp+' '+variant+(method==='GET'?'':' '+item.operationId),previous=groups.get(key),allPurposes=unique([...(previous?.purposes??[]),...purposeIds,item.operationId]),purposeBudgets=allPurposes.map(id=>operations.find(row=>row.id===id));
  groups.set(key,{handleTtlMs:Number.isFinite(Math.min(previous?.handleTtlMs??Infinity,item.handleTtlMs??Infinity))?Math.min(previous?.handleTtlMs??Infinity,item.handleTtlMs??Infinity):null,available:previous?.available!==false&&item.available!==false,batchCapacity:Math.min(previous?.batchCapacity??Infinity,batchCapacity),id:'api-'+createHash('sha256').update(key).digest('hex').slice(0,16),operationId:previous?.operationId??item.operationId,purposes:allPurposes,method,canonicalHttp,variant,workloadClass:item.workloadClass,apiBudget:{p95:Math.min(previous?.apiBudget.p95??Infinity,item.apiBudget.p95,apiClasses[item.workloadClass].p95,...purposeBudgets.map(row=>row.p95)),p99:Math.min(previous?.apiBudget.p99??Infinity,item.apiBudget.p99,apiClasses[item.workloadClass].p99,...purposeBudgets.map(row=>row.p99))}});
 }
 const backgroundBudgets={};for(const group of groups.values())for(const purpose of group.purposes){const previous=backgroundBudgets[purpose];backgroundBudgets[purpose]={method:group.method,workloadClass:group.workloadClass,canonicalHttp:group.canonicalHttp,variants:unique([...(previous?.variants??[]),group.variant]),ambiguous:!!previous&&(previous.method!==group.method||previous.workloadClass!==group.workloadClass||previous.canonicalHttp!==group.canonicalHttp||previous.ambiguous),apiBudget:{p95:Math.min(previous?.apiBudget.p95??Infinity,group.apiBudget.p95),p99:Math.min(previous?.apiBudget.p99??Infinity,group.apiBudget.p99)}};}
 return freeze({profile,maxDurationMs,probeQps,batchSize,backgroundBudgets,minSamples:profile==='formal'?targets.minSamples:5,background:{modelQps:10,adminQps:5,administrators:5,upstreamMs:[3000,6000],payloadBytes:102400},groups:[...groups.values()],scope:'independent-additional-api-coverage',normalMixReplaced:false});
}

// Input timing rows contain only non-cancelled observations. Raw metrics are
// returned unchanged alongside this qualification view.
export function assessApiCoverageLatency({plan,series=[],windows=[]}={}){
 const failures=[],missing=[],lookup=new Map(plan.groups.map(group=>[group.operationId+'/'+group.variant,group]));
 const check=(row,window)=>{
  if(row.category==='model')return;
  const spec=row.group==='api-coverage'?lookup.get(row.operation+'/'+row.variant):plan.backgroundBudgets?.[row.operation];
  if(!spec||spec.ambiguous||!variants.has(row.variant)||(row.group==='api-coverage'?spec.variant!==row.variant:!spec.variants?.includes(row.variant))){missing.push('api_coverage_budget_unknown:'+row.operation);return;}
  const budget=spec.apiBudget;if(!budget||![budget.p95,budget.p99].every(Number.isFinite)){missing.push('api_coverage_budget_unknown:'+row.operation);return;}
  if(row.p95!==null&&row.p95!==undefined&&(row.p95>budget.p95||row.p99>budget.p99))failures.push((window===undefined?'api_coverage_aggregate_budget:':'api_coverage_five_minute_budget:')+row.operation+'/'+row.variant+(window===undefined?'':':'+window));
 };
 for(const row of series)check(row);for(const window of windows)for(const row of window.series)check(row,window.start);
 return {passed:verdict(failures,missing),failures:unique(failures),missing:unique(missing)};
}

function safeResult(result,success,isCancelled){
 const row=Object.fromEntries(['operation','method','variant','status','success','complete','httpEnded','wireSent','startedAt','finishedAt','durationMs','receivedBytes','firstContentAt','headersAt','connectedAt','tlsAt','reusedSocket','bodyHash','records','trace'].filter(key=>Object.hasOwn(result,key)).map(key=>[key,result[key]]));
 return {...row,validatedSuccess:success,error:isCancelled?'client_cancelled':success?null:'request_failed'};
}
function outcome(result,signal,{model=false,verify=false}={}){
 const success=result.success===true&&(!model||result.complete===true)&&(!verify||result.businessVerified===true),isCancelled=!success&&signal.aborted&&(result.error==='client_cancelled'||result.cancelled===true)&&!httpFailure(result.status);
 return {success,isCancelled};
}

// Aggregate fixed failure labels only; response bodies and free-form errors
// must not enter the capacity report. Keep unknown transport flags distinct.
const backgroundFailureCodes=new Set(['client_cancelled','client_deadline','connection_failed','response_aborted','response_stream_error','stream_incomplete']);
function noteBackgroundFailure(diagnostics,category,result,isCancelled,{thrown=false}={}){
 const bucket=diagnostics[category][isCancelled?'cancelled':'failed'];bucket.count++;
 const status=Number.isInteger(result?.status)&&result.status>=100&&result.status<=599?String(result.status):'unknown';bucket.statuses[status]=(bucket.statuses[status]??0)+1;
 const reason=thrown?'operation_threw':status!=='unknown'&&httpFailure(Number(status))?'http_non_success':backgroundFailureCodes.has(result?.error)?result.error:'unclassified';bucket.reasons[reason]=(bucket.reasons[reason]??0)+1;
 for(const key of ['wireSent','httpEnded','complete']){const value=result?.[key]===true?'yes':result?.[key]===false?'no':'unknown';bucket[key][value]++;}
 const network=sanitizeCapacityNetworkFailure(result?.networkFailure),flags=bucket.networkFailure,tri=value=>value===true?'yes':value===false?'no':'unknown';
 const reuse=flags.byCodeAndReuse[network.code]??={yes:0,no:0,unknown:0};reuse[tri(network.reusedSocket)]++;
 for(const key of capacityNetworkFailureFlags)if(key!=='reusedSocket')flags[key][tri(network[key])]++;
}

// Callback-only core. The outer entry supplies private factory capabilities;
// handles never enter metrics, callbacks, scope evidence or returned reports.
export async function runApiCoveragePhase({plan,employees=60,openScope,prepareBatch,executeHandle,releaseHandles=async()=>{},signal,budgetSignal,callerSignal=signal,onProgress=()=>{},beforeBoundary=async()=>{}}={}){
 if(!plan||![openScope,prepareBatch,executeHandle,releaseHandles,onProgress,beforeBoundary].every(fn=>typeof fn==='function')||!Number.isInteger(employees)||employees<1||employees>60)fail('capacity_api_coverage_phase_configuration');
 const started=performance.now(),cap=new AbortController(),stop=new AbortController(),runSignal=AbortSignal.any([signal,cap.signal].filter(Boolean)),arrivals=AbortSignal.any([runSignal,stop.signal]),stopReason=new Error('capacity_api_coverage_arrivals_stopped'),timer=setTimeout(()=>cap.abort(new DOMException('capacity_api_coverage_duration_exhausted','AbortError')),plan.maxDurationMs);timer.unref();
 const latency=new Map(),pendingHandles=new Set(),scopes=[],groups=[],failures=[],missing=[],interruptions=[],cleanupDiagnostics={first:null,counts:Object.create(null)},metrics=createCapacityMetrics(),background={model:{succeeded:0,failed:0,cancelled:0},read:{succeeded:0,failed:0,cancelled:0},write:{succeeded:0,failed:0,cancelled:0}},scene={id:'additional-api-coverage',durationMs:plan.maxDurationMs,modelQps:10,adminQps:5,employees,administrators:5,upstreamMs:plan.background.upstreamMs,payloadBytes:plan.background.payloadBytes};let measuredEnd,executionError,main,preparationScope,measurementScope,drivers=Promise.resolve([]),sequence=0;
 const backgroundDiagnostics=Object.fromEntries(['model','read','write'].map(role=>[role,Object.fromEntries(['failed','cancelled'].map(kind=>[kind,{count:0,statuses:{},reasons:{},...Object.fromEntries(['wireSent','httpEnded','complete'].map(key=>[key,{yes:0,no:0,unknown:0}])),networkFailure:{byCodeAndReuse:{},...Object.fromEntries(capacityNetworkFailureFlags.filter(key=>key!=='reusedSocket').map(key=>[key,{yes:0,no:0,unknown:0}]))}}]))]));
 const addScope=async(role,group)=>{runSignal.throwIfAborted();const scope=await openScope({role,group,signal:runSignal}),row={role,groupId:group?.id??null,scope,startedAt:performance.now(),closed:false,knownClientNonSuccess:0,unattributedKnownHttpFailures:0,evidence:null};if(typeof scope?.close==='function')scopes.push(row);if(!scope||!['sendModel','sendAdmin','onResult','collect','close'].every(key=>typeof scope[key]==='function'))fail('capacity_api_coverage_scope_contract');return row;};
 const cleanupError=error=>{const pending=pendingReason(error),code=pending??getCoverageFactoryFailureReason(error)??getCoverageInventoryFailureReason(error)??'unclassified';cleanupDiagnostics.first??=code;cleanupDiagnostics.counts[code]=(cleanupDiagnostics.counts[code]??0)+1;if(pending)missing.push(pending);else{failures.push('api_coverage_handle_cleanup_failed');executionError??={completed:false,cancelled:false,code:'capacity_api_coverage_cleanup_failed'};}};
 const closeScope=async row=>{if(row.closed)return;try{await row.scope.close();row.closed=true;row.finishedAt=performance.now();}catch{failures.push('api_coverage_cleanup_failed');executionError??={completed:false,cancelled:false,code:'capacity_api_coverage_cleanup_failed'};}};
 const record=async({row,result,event,group,model=false,expectedClass,onRecorded=()=>{}})=>{
  metrics.transport({wireSent:result?.wireSent,httpEnded:result?.httpEnded});
  if(!result||!Number.isFinite(result.durationMs)||result.durationMs<0)fail('capacity_api_coverage_result_invalid');
  if(Number.isInteger(result.status)&&(result.status<200||result.status>=300))row.knownClientNonSuccess++;
  const operation=group?.operationId??(model?'model':result.operation),timingSpec=group??plan.backgroundBudgets?.[operation],methodKnown=['GET','POST','PUT','PATCH','DELETE','HEAD','OPTIONS'].includes(result.method),expectedMethod=timingSpec?.method,variant=typeof result.variant==='string'?result.variant:'unknown',variantKnown=typeof result.variant==='string'&&(group?result.variant===group.variant:timingSpec?.variants?.includes(result.variant));
  const state=outcome(result,runSignal,{model,verify:!!group||(expectedMethod??result.method)!=='GET'&&!model}),kind=model?'model':'admin',category=model?'model':(methodKnown?result.method:expectedMethod)==='GET'?'read':'write',evidenceMissing=[],evidenceFailures=[];
  if(!model){
   if(!methodKnown)evidenceMissing.push('api_coverage_http_method_unknown');
   if(!variantKnown)evidenceMissing.push('api_coverage_variant_unknown:'+operation);
   if(!timingSpec||timingSpec.ambiguous||!variants.has(variant))evidenceMissing.push('api_coverage_budget_unknown:'+operation);
   if(timingSpec&&!timingSpec.ambiguous){
    if(methodKnown&&result.method!==timingSpec.method)evidenceFailures.push('api_coverage_http_method_mismatch');
    if(!methodKnown||typeof result.requestSignature!=='string'||!result.requestSignature.startsWith(result.method+' '))evidenceMissing.push('api_coverage_client_route_unknown');
    else{const route=result.requestSignature.slice(result.method.length+1).replace(/\/[a-z0-9_.-]+::id(?=\/|$)/gi,'/:id');if(capacityWireSignature(result.method,route)!==timingSpec.canonicalHttp)evidenceFailures.push('api_coverage_client_route_mismatch');}
    if(!group&&(methodKnown&&timingSpec.method!==result.method||timingSpec.workloadClass!==expectedClass))evidenceFailures.push('api_coverage_background_operation_mismatch');
   }
  }
  failures.push(...evidenceFailures);missing.push(...evidenceMissing);const qualified=evidenceMissing.length===0&&evidenceFailures.length===0;
  if(group){group.attempts++;group[state.isCancelled?'cancelled':state.success?'succeeded':'failed']++;if(!state.isCancelled){if(qualified)group.histogram.add(result.durationMs);else group.unqualified=(group.unqualified??0)+1;if(!variantKnown)group.variantUnverified=(group.variantUnverified??0)+1;}group.evidenceFailures=unique([...(group.evidenceFailures??[]),...evidenceFailures]);group.evidenceMissing=unique([...(group.evidenceMissing??[]),...evidenceMissing]);if(!state.success&&!state.isCancelled)group.errors[httpFailure(result.status)?'http_non_success':'business_or_transport_failed']=(group.errors[httpFailure(result.status)?'http_non_success':'business_or_transport_failed']??0)+1;}
  else{background[category][state.isCancelled?'cancelled':state.success?'succeeded':'failed']++;if(!state.success)noteBackgroundFailure(backgroundDiagnostics,category,result,state.isCancelled);}onRecorded();
  if(!model&&qualified&&!state.isCancelled){const key=[group?'api-coverage':'coverage-background',operation,variant].join('/'),row=latency.get(key)??{operation,variant,category,group:group?'api-coverage':'coverage-background',histogram:new Histogram(),windows:new Map()},bucket=Math.floor(Date.now()/targets.windowMs)*targets.windowMs,window=row.windows.get(bucket)??new Histogram();row.histogram.add(result.durationMs);window.add(result.durationMs);row.windows.set(bucket,window);latency.set(key,row);}
  metrics.record({operation,scenario:scene.id,variant,category,group:group?'api-coverage':'coverage-background',at:Date.now(),durationMs:result.durationMs,success:state.success,error:state.success?null:state.isCancelled?'client_cancelled':'request_failed'});
  await row.scope.onResult({scenario:scene.id,kind,event:{sequence:event.sequence,actorIndex:event.actorIndex,scheduledAt:event.scheduledAt,startedAt:event.startedAt,arrivalLagMs:event.arrivalLagMs},result:safeResult({...result,operation,variant},state.success,state.isCancelled)});
 };
 const backgroundSend=model=>async event=>{const actorIndex=event.sequence%(model?employees:5),role=model?'model':categories[event.sequence%10]==='configuration'?'write':'read';let recorded=false;try{const result=await(model?main.scope.sendModel:main.scope.sendAdmin)({...event,scene,actorIndex,category:categories[event.sequence%10],signal:runSignal});await record({row:main,result,event:{...event,actorIndex},model,expectedClass:categories[event.sequence%10],onRecorded:()=>{recorded=true;}});}catch(error){if(cancelled(error,runSignal)){if(!recorded){background[role].cancelled++;noteBackgroundFailure(backgroundDiagnostics,role,null,true,{thrown:true});}interruptions.push('api_coverage_background_cancelled');}else{if(!recorded){background[role].failed++;noteBackgroundFailure(backgroundDiagnostics,role,null,false,{thrown:true});}failures.push('api_coverage_background_failed');executionError??={completed:false,cancelled:false,code:'capacity_api_coverage_background_failed'};throw new Error('capacity_api_coverage_background_failed');}}};

 try{
  main=await addScope('background');
  drivers=Promise.allSettled([[10,true],[5,false]].map(([qps,model])=>runOpenLoop({durationMs:plan.maxDurationMs,segments:[{startMs:0,endMs:plan.maxDurationMs,qps}],send:backgroundSend(model),diagnosticScenario:model?'api-coverage-model':'api-coverage-admin',metrics,signal:arrivals})));
  preparationScope=await addScope('preparation');measurementScope=await addScope('measurement');
  for(const definition of plan.groups){
   await beforeBoundary({kind:'group',signal:runSignal});
   const group={...definition,attempts:0,succeeded:0,failed:0,cancelled:0,errors:{},histogram:new Histogram(),batches:0,preparationMs:0,measurementMs:0};groups.push(group);if(definition.available===false){missing.push('api_coverage_group_not_connected:'+definition.id);continue;}
   while(group.succeeded+group.failed<plan.minSamples){
    await beforeBoundary({kind:'batch',signal:runSignal});runSignal.throwIfAborted();const count=Math.min(plan.batchSize,group.batchCapacity,plan.minSamples-group.succeeded-group.failed,Number.isFinite(group.handleTtlMs)?Math.max(1,Math.floor(group.handleTtlMs*plan.probeQps/1000)):Infinity),preparation=preparationScope,preparationStarted=performance.now();let handles;
    handles=await prepareBatch({scope:preparation.scope,group:definition,count,signal:runSignal});if(Array.isArray(handles))for(const handle of handles)pendingHandles.add(handle);if(!Array.isArray(handles)||handles.length!==count)fail('capacity_api_coverage_prepared_batch_invalid');group.preparationMs+=performance.now()-preparationStarted;
    const preparedHandles=handles,lease=getPreparedCoverageLease(preparedHandles);let executableCount=count;if(lease){for(;;){const remaining=getPreparedCoverageLease(preparedHandles);executableCount=apiCoverageExecutableCount({count:handles.length,remainingMs:remaining.remainingMs,probeQps:plan.probeQps});if(executableCount===handles.length)break;const excess=handles.slice(executableCount);handles=handles.slice(0,executableCount);try{await releaseHandles({handles:excess});for(const handle of excess)pendingHandles.delete(handle);}catch(error){cleanupError(error);}if(!handles.length)break;}if(!executableCount){missing.push('api_coverage_prepared_lease_too_short:'+group.id);break;}}
    const batchHandles=[...handles],measurement=measurementScope,batchStart=performance.now();let completed=0;
    try{
     await runOpenLoop({durationMs:Math.max(1,Math.ceil((executableCount-0.5)/plan.probeQps*1000)),segments:[{startMs:0,endMs:Math.max(1,Math.ceil((executableCount-0.5)/plan.probeQps*1000)),qps:plan.probeQps}],scenario:scene.id,diagnosticScenario:'api-coverage-probe',signal:runSignal,metrics,send:async event=>{
      if(lease&&getPreparedCoverageLease(preparedHandles).remainingMs<=0){group.skippedExpired=(group.skippedExpired??0)+1;missing.push('api_coverage_prepared_lease_expired:'+group.id);return;}if(event.sequence>=handles.length)fail('capacity_api_coverage_handle_missing');const handle=handles[event.sequence],actorIndex=sequence++%5;handles[event.sequence]=undefined;
      let recorded=false;try{const result=await executeHandle({scope:measurement.scope,handle,group:definition,actorIndex,signal:runSignal});await record({row:measurement,result,event:{...event,actorIndex},group,onRecorded:()=>{recorded=true;}});completed++;}catch(error){if(cancelled(error,runSignal)){if(!recorded){group.attempts++;group.cancelled++;}interruptions.push('api_coverage_probe_cancelled');}else{if(!recorded){group.attempts++;group.failed++;}group.errors.tool_failed=(group.errors.tool_failed??0)+1;failures.push('api_coverage_probe_failed:'+group.id);executionError??={completed:false,cancelled:false,code:'capacity_api_coverage_probe_failed'};throw new Error('capacity_api_coverage_probe_failed');}}
     }});await delay(Math.max(0,executableCount/plan.probeQps*1000-(performance.now()-batchStart)),undefined,{signal:runSignal});
    }finally{group.batches++;group.measurementMs+=performance.now()-batchStart;try{await releaseHandles({handles:batchHandles});for(const handle of batchHandles)pendingHandles.delete(handle);}catch(error){cleanupError(error);}await onProgress({kind:'api-coverage-batch',groupId:group.id,samples:group.succeeded+group.failed,completed,observedMs:performance.now()-batchStart});}
    if(executionError)fail(executionError.code);
    if(!completed){missing.push('api_coverage_batch_without_samples:'+group.id);break;}
   }
  }
 }catch(error){
  if(pendingReason(error))missing.push(pendingReason(error));
  else if(cancelled(error,runSignal)){interruptions.push(cap.signal.aborted||budgetSignal?.aborted?'api_coverage_duration_exhausted':'api_coverage_cancelled');missing.push('api_coverage_execution_incomplete');}
  else{const detail=getCoverageFactoryFailureReason(error)??getCoverageInventoryFailureReason(error);if(detail)failures.push('api_coverage_factory_failed:'+detail);failures.push('api_coverage_execution_failed');executionError??={completed:false,cancelled:false,code:'capacity_api_coverage_execution_failed'};}
 }finally{
  measuredEnd=performance.now();stop.abort(stopReason);const outcomes=await drivers;for(const row of outcomes)if(row.status==='rejected'&&row.reason!==stopReason&&row.reason?.cause!==stopReason&&!cancelled(row.reason,runSignal))failures.push('api_coverage_background_driver_failed');
  // Creator-bound cleanup must finish while its observation scope is open.
  if(pendingHandles.size)try{await releaseHandles({handles:[...pendingHandles]});pendingHandles.clear();}catch(error){cleanupError(error);}
  for(const row of scopes){if(row.scope.waitForDataReadiness)try{const ready=await row.scope.waitForDataReadiness({signal:runSignal});row.dataReadiness={passed:ready?.passed??null,counts:ready?.counts??null};const noModels=row!==main&&ready?.counts?.total===0&&ready?.counts?.eligible===0;if(ready?.passed===false)failures.push('api_coverage_model_readiness_failed');else if(ready?.passed!==true&&!noModels)missing.push('api_coverage_model_readiness_incomplete');}catch(error){if(cancelled(error,runSignal))missing.push('api_coverage_model_readiness_incomplete');else{failures.push('api_coverage_model_readiness_failed');executionError??={completed:false,cancelled:false,code:'capacity_api_coverage_readiness_failed'};}}else missing.push('api_coverage_model_readiness_not_connected');}
  for(const row of scopes)await closeScope(row);
  for(const row of scopes)try{const value=await row.scope.collect();row.evidence={transport:value.transport,dataReconciliation:value.dataReconciliation,streamingStages:value.streamingStages};const stage=streamingStageQualification(value.streamingStages,{profile:plan.profile});if(stage.failed)failures.push('api_coverage_streaming_stages_failed');if(stage.missing)missing.push('api_coverage_streaming_stages_incomplete');const knownHttpFailures=value.transport?.counts?.httpNonSuccess;if(row.role!=='preparation'&&(!Number.isSafeInteger(knownHttpFailures)||knownHttpFailures<0))missing.push('api_coverage_http_status_evidence_incomplete');if(row.role!=='preparation'&&Number.isSafeInteger(knownHttpFailures)&&knownHttpFailures<row.knownClientNonSuccess)failures.push('api_coverage_http_status_mismatch');if(row.role!=='preparation'&&Number.isSafeInteger(knownHttpFailures)&&knownHttpFailures>row.knownClientNonSuccess){row.unattributedKnownHttpFailures=knownHttpFailures-row.knownClientNonSuccess;missing.push('api_coverage_http_error_attribution_incomplete');}if(value.dataReconciliation?.passed===false)failures.push('api_coverage_data_failed');if((value.transport?.counts?.modelResults??0)>0&&value.dataReconciliation?.passed!==true)missing.push('api_coverage_data_incomplete');if(value.transport?.passed===false)failures.push('api_coverage_transport_failed');else if(value.transport?.passed!==true)missing.push('api_coverage_transport_incomplete');if(row.role==='measurement'){if(value.transport?.mix?.auxiliaryBaselineHttp>0)failures.push('api_coverage_measured_extra_http');if(value.transport?.mix?.alignedToPlannedCounts===false)failures.push('api_coverage_measured_http_mismatch');else if(value.transport?.mix?.alignedToPlannedCounts!==true)missing.push('api_coverage_measured_http_incomplete');}if(row.role==='background'){if(value.transport?.mix?.alignedToPlannedCounts===false)failures.push('api_coverage_background_mix_failed');else if(value.transport?.mix?.alignedToPlannedCounts!==true)missing.push('api_coverage_background_mix_incomplete');if(value.dataReconciliation?.passed===false)failures.push('api_coverage_data_failed');else if(value.dataReconciliation?.passed!==true)missing.push('api_coverage_data_incomplete');}}catch{failures.push('api_coverage_evidence_collect_failed');executionError??={completed:false,cancelled:false,code:'capacity_api_coverage_evidence_collect_failed'};}
  clearTimeout(timer);
 }
 const measured=metrics.view();if(measured.dropped||measured.arrivals.missed)missing.push('api_coverage_load_generation_incomplete');if(measured.arrivals.unknownTransport)missing.push('api_coverage_transport_metrics_incomplete');if(measured.violations.length)failures.push('api_coverage_driver_failed');
 const latencyWindows=new Map(),latencySeries=[];for(const row of latency.values()){
  const labels={operation:row.operation,variant:row.variant,category:row.category,group:row.group};latencySeries.push({...labels,...row.histogram.view()});for(const [start,histogram] of row.windows){const values=latencyWindows.get(start)??[];values.push({...labels,...histogram.view()});latencyWindows.set(start,values);}
 }
 const latencyQualification=assessApiCoverageLatency({plan,series:latencySeries,windows:[...latencyWindows].map(([start,series])=>({start,series}))});failures.push(...latencyQualification.failures);missing.push(...latencyQualification.missing);
 const results=groups.map(({histogram,...group})=>{const samples=group.succeeded+group.failed,successRate=samples?group.succeeded/samples:null,timing=histogram.view(),qualifiedSamples=samples-(group.unqualified??0),localFailures=[...(group.evidenceFailures??[])],localMissing=[...(group.evidenceMissing??[])];if(group.variantUnverified)localMissing.push('api_coverage_variant_unknown');if(qualifiedSamples<plan.minSamples)localMissing.push('api_coverage_samples_below_target');if(successRate!==null&&successRate<targets.successRate)localFailures.push('api_coverage_success_rate');if(timing.p95!==null&&(timing.p95>group.apiBudget.p95||timing.p99>group.apiBudget.p99))localFailures.push('api_coverage_api_budget');failures.push(...localFailures.map(code=>code+':'+group.id));missing.push(...localMissing.map(code=>code+':'+group.id));return {...group,...timing,samples:qualifiedSamples,observedSamples:samples,successRate,passed:verdict(localFailures,localMissing),failures:localFailures,missing:localMissing};});
 for(const definition of plan.groups)if(!results.some(row=>row.id===definition.id))missing.push('api_coverage_group_not_run:'+definition.id);
 for(const [role,row] of Object.entries(background)){const count=row.succeeded+row.failed;if(!count)missing.push('api_coverage_background_samples_missing:'+role);else if(row.succeeded/count<targets.successRate)failures.push('api_coverage_background_success_rate:'+role);}
 const summary=scopes.map(({scope,closed,startedAt,finishedAt,...row})=>({...row,closed,observedMs:(finishedAt??performance.now())-startedAt,httpRequests:row.evidence?.transport?.counts?.wireStarted??null,actualHttpQps:Number.isSafeInteger(row.evidence?.transport?.counts?.wireStarted)&&finishedAt>startedAt?row.evidence.transport.counts.wireStarted/((finishedAt-startedAt)/1000):null}));
 const report={scope:plan.scope,normalMixReplaced:false,plan,window:{observedMs:measuredEnd-started,maxDurationMs:plan.maxDurationMs,capReached:cap.signal.aborted||budgetSignal?.aborted===true,cancelled:callerSignal?.aborted===true},groups:results,background,backgroundDiagnostics,latencyQualification,metrics:measured,scopes:summary,cleanupDiagnostics:structuredClone(cleanupDiagnostics),interruptions:unique(interruptions),qualification:{passed:verdict(failures,missing),failures:unique(failures),missing:unique(missing)}};
 if(executionError){report.execution=executionError;const error=new Error(executionError.code);reports.set(error,freeze(report));throw error;}return report;
}

const includeAuxiliary=(report,auxiliaryPreparation)=>{const qualification=report.qualification;if(auxiliaryPreparation?.passed===false)return {...report,auxiliaryPreparation,qualification:{...qualification,passed:false,failures:unique([...qualification.failures,'api_coverage_auxiliary_preparation_failed'])}};if(auxiliaryPreparation&&auxiliaryPreparation.passed!==true)return {...report,auxiliaryPreparation,qualification:{...qualification,passed:qualification.passed===false?false:null,missing:unique([...qualification.missing,'api_coverage_auxiliary_preparation_incomplete'])}};return {...report,auxiliaryPreparation};};
const includeVariants=(report,owned)=>{if(typeof owned.getApiCoverageVariantEvidence!=='function')return {...report,variantEvidence:null};try{const evidence=owned.getApiCoverageVariantEvidence();if(!Array.isArray(evidence))throw new Error('capacity_coverage_variant_evidence_invalid');return {...report,variantEvidence:evidence};}catch{return {...report,variantEvidence:null,qualification:{...report.qualification,passed:false,failures:unique([...report.qualification.failures,'api_coverage_variant_collection_failed'])}};}};

// Factory contract: inventory is safe metadata; preparation returns opaque
// single-use handles. Both hooks must route all HTTP through the supplied scope.
export async function runIndependentApiCoverage({target,profile=target?.plan?.profile??'selfcheck',maxDurationMs,probeQps=1,batchSize=64,signal,onProgress}={}){
 let owned=ownedCapacityTarget(target,{profile});const required=['getApiCoverageInventory','prepareApiCoverageBatch','executeApiCoverageHandle','releaseApiCoverageHandles','prepareActorEpoch','createObservationScope','configureScenario'];
 if(required.some(key=>typeof owned[key]!=='function')||!Number.isInteger(owned.modelActorCount)||owned.modelActorCount<1||owned.modelActorCount>60||owned.adminActors.length!==5||profile==='formal'&&owned.modelActorCount!==60)return {scope:'independent-additional-api-coverage',normalMixReplaced:false,qualification:{passed:null,failures:[],missing:['factory_api_coverage_not_connected']}};
 if(!Number.isSafeInteger(maxDurationMs)||maxDurationMs<1||maxDurationMs>86400000)fail('capacity_api_coverage_configuration');
 const started=performance.now(),budget=new AbortController(),combined=AbortSignal.any([signal,budget.signal].filter(Boolean)),timer=setTimeout(()=>budget.abort(new DOMException('capacity_api_coverage_duration_exhausted','AbortError')),maxDurationMs);timer.unref();let setupObservedMs,epochEvidence,phaseReport;const actorRefreshes=[];
 const refreshActors=async(reason,requestSignal)=>{const evidence=await owned.prepareActorEpoch({phase:'normal-scenario',signal:requestSignal});owned=ownedCapacityTarget(target);actorRefreshes.push(Object.freeze(Object.fromEntries(['epoch','startedAt','finishedAt','cookieMaxAgeSeconds','passed','scope','requests','wireStarted','wireEnded'].map(key=>[key,evidence?.[key]]).concat([['reason',reason],['sourceClass','auxiliary'],['observedMs',Number.isFinite(evidence?.finishedAt)&&Number.isFinite(evidence?.startedAt)?evidence.finishedAt-evidence.startedAt:null]]))));if(apiCoverageActorRefreshDue(evidence)===null)failEpoch();epochEvidence=evidence;};
 try{
  await refreshActors('entry',combined);
  const inventory=await owned.getApiCoverageInventory({signal:combined,prepare:true});combined.throwIfAborted();
  let remaining=Math.floor(maxDurationMs-(performance.now()-started));if(remaining<1){budget.abort(new DOMException('capacity_api_coverage_duration_exhausted','AbortError'));combined.throwIfAborted();}
  const plan=apiCoveragePlan({profile,inventory,maxDurationMs:remaining,probeQps,batchSize});await owned.configureScenario({scene:{...plan.background,modelQps:10,durationMs:plan.maxDurationMs},signal:combined});combined.throwIfAborted();setupObservedMs=performance.now()-started;let scopeIndex=0,groupsStarted=0;
  const report=phaseReport=await runApiCoveragePhase({plan,employees:owned.modelActorCount,signal:combined,budgetSignal:budget.signal,callerSignal:signal??null,onProgress,openScope:async({role})=>{const scope=await owned.createObservationScope({id:'api-coverage-'+Date.now()+'-'+scopeIndex+++'-'+role,kind:'normal'});if(!isOwnedObservationScope(scope))fail('capacity_api_coverage_scope_not_owned');return scope;},beforeBoundary:async({kind,signal:boundarySignal})=>{const due=apiCoverageActorRefreshDue(epochEvidence);if(due===null)failEpoch();if(kind==='group'&&groupsStarted++>0||due)await refreshActors(kind==='group'?'group-boundary':'cookie-half-life',boundarySignal);},prepareBatch:async input=>{const handles=await owned.prepareApiCoverageBatch(input);if(!getPreparedCoverageLease(handles))fail('capacity_api_coverage_batch_lease_not_owned');return handles;},executeHandle:input=>owned.executeApiCoverageHandle(input),releaseHandles:input=>owned.releaseApiCoverageHandles(input)});
  const auxiliaryPreparation=typeof owned.getApiCoverageAuxiliaryEvidence==='function'?await owned.getApiCoverageAuxiliaryEvidence():null;return {...includeVariants(includeAuxiliary(report,auxiliaryPreparation),owned),actorRefreshes,setupObservedMs,totalObservedMs:performance.now()-started,declaredMaxDurationMs:maxDurationMs};
 }catch(error){
  let auxiliaryPreparation,auxiliaryFailed=false;try{auxiliaryPreparation=await owned.getApiCoverageAuxiliaryEvidence?.();}catch{auxiliaryFailed=true;}const registered=getApiCoverageFailureReport(error),partial=registered??phaseReport;if(partial){let report=includeVariants(includeAuxiliary(partial,auxiliaryPreparation),owned);if(!registered||auxiliaryFailed)report={...report,qualification:{...report.qualification,passed:false,failures:unique([...report.qualification.failures,'api_coverage_auxiliary_collection_failed'])}};const safe=registered?error:new Error('capacity_api_coverage_auxiliary_collection_failed');reports.set(safe,freeze({...report,actorRefreshes,setupObservedMs,totalObservedMs:performance.now()-started,declaredMaxDurationMs:maxDurationMs}));throw safe;}

  const isCancelled=cancelled(error,combined),pending=pendingReason(error),report={scope:'independent-additional-api-coverage',normalMixReplaced:false,auxiliaryPreparation,actorRefreshes,setupObservedMs:performance.now()-started,declaredMaxDurationMs:maxDurationMs,qualification:{passed:isCancelled||pending?null:false,failures:isCancelled||pending?[]:['api_coverage_setup_failed'],missing:pending?[pending]:isCancelled?[budget.signal.aborted?'api_coverage_duration_exhausted':'api_coverage_cancelled']:[]}};
  const safe=new Error(isCancelled?'capacity_api_coverage_setup_cancelled':'capacity_api_coverage_setup_failed');const finalReport=includeVariants(includeAuxiliary(report,auxiliaryPreparation),owned);if(auxiliaryFailed)finalReport.qualification={...finalReport.qualification,passed:false,failures:unique([...finalReport.qualification.failures,'api_coverage_auxiliary_collection_failed'])};reports.set(safe,freeze(finalReport));throw safe;
 }finally{clearTimeout(timer);}
}
