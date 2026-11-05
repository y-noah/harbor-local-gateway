import test from 'node:test';
import assert from 'node:assert/strict';
import {setTimeout as delay} from 'node:timers/promises';
import {apiCoveragePlan,runApiCoveragePhase,assessApiCoverageLatency,getApiCoverageFailureReport,runIndependentApiCoverage,apiCoverageActorRefreshDue,apiCoverageExecutableCount} from '../tools/capacity/api-coverage.mjs';
const inventory=[{operationId:'health',method:'GET',canonicalHttp:'GET /health',variant:'steady',workloadClass:'health',apiBudget:{p95:100,p99:300}},{operationId:'request-detail',method:'GET',canonicalHttp:'GET /api/admin/requests/:id',variant:'steady',workloadClass:'detail',variants:['steady'],apiBudget:{p95:500,p99:1000}}];
const signatures={'health':'GET /health','request-detail':'GET /api/admin/requests/:id','employee-list':'GET /api/admin/members','request-summary':'GET /api/admin/requests?view=summary','employee-save':'PATCH /api/admin/members/:id','model':'POST /v1/responses','status':'GET /api/admin/status'};
const result=(operation='request-detail',extra={})=>({operation,requestSignature:signatures[operation],variant:'steady',method:'GET',status:200,success:true,complete:true,businessVerified:true,durationMs:1,wireSent:true,httpEnded:true,trace:'synthetic-safe-trace',...extra});

const backgroundBudgets={
 'employee-list':{canonicalHttp:signatures['employee-list'],method:'GET',workloadClass:'list',variants:['steady'],apiBudget:{p95:500,p99:1000}},
 'request-detail':{canonicalHttp:signatures['request-detail'],method:'GET',workloadClass:'detail',variants:['steady'],apiBudget:{p95:500,p99:1000}},
 'request-summary':{canonicalHttp:signatures['request-summary'],method:'GET',workloadClass:'summary',variants:['steady'],apiBudget:{p95:2000,p99:4000}},
 'employee-save':{canonicalHttp:signatures['employee-save'],method:'PATCH',workloadClass:'configuration',variants:['steady'],apiBudget:{p95:500,p99:1000}}
};
const runPhase=options=>runApiCoveragePhase({...options,plan:{...options.plan,backgroundBudgets:{...backgroundBudgets,...options.plan.backgroundBudgets}}});
function fixture({preparation,execution,closeFailure=false,dataPassed=true}={}){
 const scopes=[],seen=[],consumed=new WeakSet(),handles=new Set(),released=new Set();let modelCalls=0,adminCalls=0;
 const openScope=async({role,group})=>{
  let wireStarted=0,httpNonSuccess=0,closed=false;const categories={};const scope={role,group,closed:()=>closed,
   sendModel:async()=>{wireStarted++;modelCalls++;return result('model',{method:'POST'});},
   sendAdmin:async({category})=>{wireStarted++;adminCalls++;categories[category]=(categories[category]??0)+1;return result(({list:'employee-list',detail:'request-detail',summary:'request-summary',configuration:'employee-save'})[category],{method:category==='configuration'?'PATCH':'GET'});},
   onResult:async row=>{seen.push(row);},waitForDataReadiness:async({signal})=>{signal.throwIfAborted();return {passed:dataPassed};},
   collect:async()=>({transport:{passed:true,counts:{wireStarted,httpNonSuccess},mix:{alignedToPlannedCounts:true,actualBaselineByClass:categories}},dataReconciliation:{passed:dataPassed}}),
   close:async()=>{closed=true;if(closeFailure&&role==='preparation')throw Error('raw-secret-close');},
   addWire:(status=200)=>{wireStarted++;if(Number.isInteger(status)&&(status<200||status>=300))httpNonSuccess++;}};scopes.push(scope);return scope;
 };
 return {scopes,seen,released,calls:()=>({modelCalls,adminCalls}),openScope,
  prepareBatch:async input=>{input.scope.addWire();if(preparation)await preparation(input);return Array.from({length:input.count},()=>{const handle={secret:'raw-private-handle'};handles.add(handle);return handle;});},
  releaseHandles:async({handles:batch})=>{for(const handle of batch)released.add(handle);},
  executeHandle:async input=>{assert.ok(handles.has(input.handle));assert.ok(!consumed.has(input.handle));consumed.add(input.handle);const value=execution?await execution(input):result(input.group.operationId,{json:{secret:'raw-private-response'}});input.scope.addWire(value.status);return value;}};
}

test('coverage groups physical aliases once with strict purpose budgets and meaningful variants only',()=>{
 const state={operationId:'overview',method:'GET',canonicalHttp:'GET /api/admin/state',variant:'default',workloadClass:'summary',variants:['steady'],apiBudget:{p95:2000,p99:4000}};
 const plan=apiCoveragePlan({inventory:[state,{...state,operationId:'session-list',apiBudget:{p95:500,p99:1000}}],profile:'formal',maxDurationMs:4000000});assert.equal(plan.groups.length,1);assert.deepEqual(plan.groups[0].purposes,['overview','session-list']);assert.deepEqual(plan.groups[0].apiBudget,{p95:500,p99:1000});assert.equal(plan.minSamples,1000);assert.deepEqual(plan.background,{modelQps:10,adminQps:5,administrators:5,upstreamMs:[3000,6000],payloadBytes:102400});assert.ok(Object.isFrozen(plan.groups[0]));
 assert.throws(()=>apiCoveragePlan({inventory}),/configuration/);assert.throws(()=>apiCoveragePlan({inventory:[{...state,variant:'invented'}],maxDurationMs:100}),/inventory_invalid/);assert.throws(()=>apiCoveragePlan({inventory:[{...state,workloadClass:'list'}],maxDurationMs:100}),/inventory_invalid/);
});

test('callback-only coverage keeps continuous 10/5 background and separate bounded preparation / single-use measured batches',async()=>{
 const target=fixture(),plan=apiCoveragePlan({inventory,maxDurationMs:5000,probeQps:5,batchSize:2}),report=await runPhase({plan,...target});
 assert.equal(report.qualification.passed,true);assert.ok(report.groups.every(row=>row.samples===5&&row.passed===true));assert.equal(report.groups.length,2);assert.equal(target.scopes.length,3);assert.ok(target.scopes.every(scope=>scope.closed()));assert.ok(report.scopes.every(row=>Number.isFinite(row.actualHttpQps)));
 const background=target.scopes.find(scope=>scope.role==='background'),data=await background.collect();const actual=target.calls(),expected={};for(let index=0;index<actual.adminCalls;index++){const category=['list','list','list','list','list','detail','detail','summary','summary','configuration'][index%10];expected[category]=(expected[category]??0)+1;}assert.deepEqual(data.transport.mix.actualBaselineByClass,expected);assert.ok(actual.adminCalls>=10);assert.ok(actual.modelCalls>=20);assert.equal(target.released.size,10);assert.ok(!JSON.stringify(report).includes('raw-private'));assert.ok(!JSON.stringify(target.seen).includes('raw-private'));assert.equal(report.normalMixReplaced,false);
});

test('duration cap during preparation closes every scope and leaves insufficient evidence incomplete',async()=>{
 const target=fixture({preparation:({signal})=>delay(1000,undefined,{signal})}),plan=apiCoveragePlan({inventory,maxDurationMs:30,probeQps:5,batchSize:2}),report=await runPhase({plan,...target});assert.equal(report.qualification.passed,null);assert.equal(report.window.capReached,true);assert.ok(report.interruptions.includes('api_coverage_duration_exhausted'));assert.ok(target.scopes.every(scope=>scope.closed()));assert.ok(report.qualification.missing.some(code=>code.startsWith('api_coverage_samples_below_target')));assert.ok(!report.qualification.failures.length);
});

test('a measured 500 remains false when later cancellation interrupts the batch',async()=>{
 const abort=new AbortController(),target=fixture({execution:async()=>result('health',{status:500,success:false,businessVerified:false})}),plan=apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5,batchSize:1}),report=await runPhase({plan,...target,signal:abort.signal,onProgress:()=>abort.abort()});assert.equal(report.qualification.passed,false);assert.equal(report.groups[0].failed,1);assert.equal(report.groups[0].samples,1);assert.equal(report.groups[0].cancelled,0);assert.ok(target.scopes.every(scope=>scope.closed()));
});

test('preparation and cleanup failures preserve safe partial evidence and close remaining scopes',async()=>{
 for(const closeFailure of [false,true]){const target=fixture({closeFailure,preparation:async()=>{throw Error('raw-secret-preparation');}}),plan=apiCoveragePlan({inventory,maxDurationMs:1000});let error;try{await runPhase({plan,...target});}catch(value){error=value;}assert.ok(error);const report=getApiCoverageFailureReport(error);assert.equal(report.qualification.passed,false);assert.ok(target.scopes.every(scope=>scope.closed()));assert.ok(!JSON.stringify(report).includes('raw-secret'));assert.equal(getApiCoverageFailureReport(Object.assign(new Error('fake'),{report})),undefined);if(closeFailure)assert.ok(report.qualification.failures.includes('api_coverage_cleanup_failed'));}
});

test('external target lookalikes cannot invoke coverage factories',async()=>{let called=0;await assert.rejects(runIndependentApiCoverage({target:{getApiCoverageInventory:()=>{called++;}},maxDurationMs:1000}),/capacity_target_not_owned/);assert.equal(called,0);});

test('a failed handle release retries before closing its creator scope without erasing the first failure',async()=>{
 const target=fixture(),release=target.releaseHandles,openAtRelease=[];let attempts=0;
 target.releaseHandles=async input=>{const creator=target.scopes.find(scope=>scope.role==='preparation');openAtRelease.push(!creator.closed());if(++attempts===1)throw Error('synthetic first cleanup failure');if(creator.closed())throw Error('synthetic closed creator');return release(input);};
 let report;await assert.rejects(runPhase({...target,plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:3000,probeQps:5,batchSize:5})}),error=>{report=getApiCoverageFailureReport(error);return error.message==='capacity_api_coverage_cleanup_failed'&&!!report;});assert.equal(attempts,2);assert.deepEqual(openAtRelease,[true,true]);assert.equal(target.released.size,5);assert.ok(target.scopes.every(scope=>scope.closed()));assert.equal(report.qualification.passed,false);assert.ok(report.qualification.failures.includes('api_coverage_handle_cleanup_failed'));assert.equal(report.cleanupDiagnostics.first,'unclassified');assert.deepEqual(report.cleanupDiagnostics.counts,{unclassified:1});assert.equal(JSON.stringify(report).includes('synthetic first cleanup failure'),false);
});

test('inventory capacity bounds batch preparation and cancellation releases unconsumed private handles',async()=>{
 const abort=new AbortController(),target=fixture({execution:async()=>{abort.abort();throw abort.signal.reason;}}),plan=apiCoveragePlan({inventory:[{...inventory[0],batchCapacity:1}],maxDurationMs:2000,probeQps:5,batchSize:64});const report=await runPhase({plan,...target,signal:abort.signal});assert.equal(plan.groups[0].batchCapacity,1);assert.equal(target.released.size,1);assert.equal(report.groups[0].cancelled,1);assert.equal(report.groups[0].failed,0);assert.equal(report.qualification.passed,null);
});

test('measurement scope rejects hidden extra HTTP while preparation remains explicitly additional',async()=>{
 const target=fixture(),open=target.openScope;target.openScope=async input=>{const scope=await open(input),collect=scope.collect;scope.collect=async()=>{const result=await collect();if(input.role==='measurement')result.transport.mix={alignedToPlannedCounts:false,auxiliaryBaselineHttp:1};return result;};return scope;};
 const plan=apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5,batchSize:5}),report=await runPhase({plan,...target});assert.equal(report.qualification.passed,false);assert.ok(report.qualification.failures.includes('api_coverage_measured_extra_http'));assert.ok(report.qualification.failures.includes('api_coverage_measured_http_mismatch'));assert.equal(target.scopes.length,3);
});

test('non-integral probe periods dispatch exactly one prepared handle per scheduled arrival',async()=>{
 for(const batchSize of [1,2]){const target=fixture(),plan=apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:3000,probeQps:3,batchSize}),report=await runPhase({plan,...target});assert.equal(report.groups[0].attempts,5);assert.equal(report.groups[0].samples,5);assert.equal(report.groups[0].succeeded,5);assert.equal(report.released,undefined);assert.equal(target.released.size,5);assert.deepEqual(report.metrics.violations,[]);assert.ok(!report.qualification.failures.length);}
});

test('cancellation cannot erase explicitly false measurement HTTP alignment',async()=>{
 const abort=new AbortController(),target=fixture({execution:async()=>{abort.abort();throw abort.signal.reason;}}),open=target.openScope;target.openScope=async input=>{const scope=await open(input),collect=scope.collect;scope.collect=async()=>{const value=await collect();if(input.role==='measurement')value.transport.mix.alignedToPlannedCounts=false;return value;};return scope;};
 const report=await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:1000}),...target,signal:abort.signal});assert.equal(report.qualification.passed,false);assert.ok(report.qualification.failures.includes('api_coverage_measured_http_mismatch'));
});

test('driver-swallowed execution and result-observer errors still produce branded safe partial failures',async()=>{
 for(const mode of ['execute','result']){const target=fixture({execution:mode==='execute'?async()=>{throw Error('raw-secret-driver');}:undefined}),open=target.openScope;if(mode==='result')target.openScope=async input=>{const scope=await open(input);if(input.role==='measurement')scope.onResult=async()=>{throw Error('raw-secret-observer');};return scope;};let error;try{await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:1000,probeQps:5}),...target});}catch(value){error=value;}assert.ok(error);const report=getApiCoverageFailureReport(error);assert.equal(report.qualification.passed,false);assert.ok(report.groups[0].attempts>0);assert.ok(target.scopes.every(scope=>scope.closed()));assert.ok(!JSON.stringify(report).includes('raw-secret'));}
});

test('every returned handle is released when preparation returns too few or too many',async()=>{
 for(const size of [1,3]){const target=fixture(),released=new Set(),handles=Array.from({length:size},()=>({}));let executions=0,error;try{await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:1000,batchSize:2}),...target,prepareBatch:async()=>handles,releaseHandles:async({handles})=>handles.forEach(handle=>released.add(handle)),executeHandle:async()=>{executions++;return result();}});}catch(value){error=value;}assert.ok(getApiCoverageFailureReport(error));assert.equal(released.size,size);assert.equal(executions,0);assert.ok(target.scopes.every(scope=>scope.closed()));}
});

test('raw transport counters retain sent, unsent and unknown outcomes',async()=>{
 let index=0;const target=fixture({execution:async()=>{const n=index++;return result('health',n%3===0?{wireSent:true,httpEnded:true}:n%3===1?{wireSent:false,httpEnded:false}:{wireSent:undefined,httpEnded:undefined});}}),report=await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5}),...target});assert.ok(report.metrics.arrivals.wireSent>=2);assert.ok(report.metrics.arrivals.httpEnded>=2);assert.ok(report.metrics.arrivals.unknownTransport>0);
});

test('background failure diagnostics retain bounded HTTP, stream and transport facts without free-form errors',async()=>{
 const target=fixture(),open=target.openScope;let index=0;
 const samples=[{status:502,error:'private-response-text',wireSent:true,httpEnded:true,complete:false},{status:200,error:'stream_incomplete',wireSent:true,httpEnded:true,complete:false},{status:null,error:'client_deadline',wireSent:true,httpEnded:false,complete:false,networkFailure:{code:'ETIMEDOUT',reusedSocket:false,socketAssigned:true,tcpConnectObserved:true,tlsSecureConnectObserved:false,message:'private-timeout-text'}},{status:-1,error:'private-transport-text',wireSent:undefined,httpEnded:undefined,complete:undefined,networkFailure:{code:'private-arbitrary-code',reusedSocket:true,socketAssigned:'private-flag',tcpConnectObserved:false,tlsSecureConnectObserved:'private-tls',address:'private-address',body:'private-body'}}];
 target.openScope=async input=>{const scope=await open(input),send=scope.sendModel;if(input.role==='background')scope.sendModel=async event=>{const base=await send(event),sample=samples[index++];return sample?{...base,...sample,success:false}:base;};return scope;};
 const report=await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5}),...target}),row=report.backgroundDiagnostics.model.failed;
 assert.equal(row.count,4);assert.equal(report.background.model.failed,4);assert.deepEqual(row.statuses,{'200':1,'502':1,unknown:2});assert.deepEqual(row.reasons,{http_non_success:1,stream_incomplete:1,client_deadline:1,unclassified:1});assert.deepEqual(row.wireSent,{yes:3,no:0,unknown:1});assert.deepEqual(row.httpEnded,{yes:2,no:1,unknown:1});assert.deepEqual(row.complete,{yes:0,no:3,unknown:1});assert.deepEqual(row.networkFailure,{byCodeAndReuse:{NOT_REPORTED:{yes:0,no:0,unknown:2},ETIMEDOUT:{yes:0,no:1,unknown:0},OTHER:{yes:1,no:0,unknown:0}},socketAssigned:{yes:1,no:0,unknown:3},tcpConnectObserved:{yes:1,no:1,unknown:2},tlsSecureConnectObserved:{yes:0,no:1,unknown:3}});assert.equal(report.backgroundDiagnostics.read.failed.count,0);assert.equal(report.backgroundDiagnostics.write.failed.count,0);assert.equal(JSON.stringify(report).includes('private-'),false);assert.equal(report.qualification.passed,false);
});

test('thrown background failures and caller cancellation keep distinct diagnostic counts without double accounting',async()=>{
 for(const cancelled of [false,true]){const abort=new AbortController(),target=fixture(),open=target.openScope;let first=true;
  target.openScope=async input=>{const scope=await open(input),send=scope.sendModel;if(input.role==='background')scope.sendModel=async event=>{if(first){first=false;if(cancelled){abort.abort();throw abort.signal.reason;}throw Error('private-exception-text');}return send(event);};return scope;};
  let report;try{report=await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5}),...target,signal:abort.signal});}catch(error){report=getApiCoverageFailureReport(error);assert.equal(cancelled,false);}
  const kind=cancelled?'cancelled':'failed',row=report.backgroundDiagnostics.model[kind];assert.equal(row.count,1);assert.equal(report.background.model[kind],1);assert.deepEqual(row.statuses,{unknown:1});assert.deepEqual(row.reasons,{operation_threw:1});assert.deepEqual(row.httpEnded,{yes:0,no:0,unknown:1});assert.equal(JSON.stringify(report).includes('private-exception-text'),false);
 }
});

test('different write payload purposes never share one formal sample group',()=>{
 const base={method:'PATCH',canonicalHttp:'PATCH /api/admin/keys/:id',variant:'steady',workloadClass:'configuration',variants:['steady'],apiBudget:{p95:500,p99:1000}},plan=apiCoveragePlan({inventory:[{...base,operationId:'key-save'},{...base,operationId:'model-permissions-save'}],profile:'formal',maxDurationMs:10000});assert.equal(plan.groups.length,2);assert.ok(plan.groups.every(group=>group.purposes.length===1));assert.notEqual(plan.groups[0].id,plan.groups[1].id);assert.throws(()=>apiCoveragePlan({inventory:[{...base,operationId:'key-save',purposes:['key-save','model-permissions-save']}],maxDurationMs:1000}),/write_purposes_combined/);
});

test('both probe and background five-minute latency budgets survive a green aggregate',()=>{
 const plan={...apiCoveragePlan({inventory,maxDurationMs:1000}),backgroundBudgets},row={operation:'health',variant:'steady',category:'read',group:'api-coverage',p95:1,p99:1},background={operation:'employee-list',variant:'steady',category:'read',group:'coverage-background',p95:1,p99:1};
 const assessed=assessApiCoverageLatency({plan,series:[row,background],windows:[{start:300000,series:[{...row,p95:101},{...background,p99:1001}]}]});assert.equal(assessed.passed,false);assert.ok(assessed.failures.includes('api_coverage_five_minute_budget:health/steady:300000'));assert.ok(assessed.failures.includes('api_coverage_five_minute_budget:employee-list/steady:300000'));
 assert.equal(assessApiCoverageLatency({plan,series:[{...background,operation:'unknown-operation'}]}).passed,null);assert.equal(assessApiCoverageLatency({plan,series:[{...background,operation:'model',category:'model',p95:600000,p99:600000}]}).passed,true);
});

test('readiness non-cancellation errors reject safely while explicit false remains a business verdict',async()=>{
 for(const throws of [false,true]){const target=fixture(),open=target.openScope;target.openScope=async input=>{const scope=await open(input);if(input.role==='background')scope.waitForDataReadiness=async()=>{if(throws)throw Error('raw-private-readiness');return {passed:false};};return scope;};const call=runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5}),...target});if(throws){let error;try{await call;}catch(value){error=value;}assert.ok(error);const report=getApiCoverageFailureReport(error);assert.equal(report.qualification.passed,false);assert.ok(!JSON.stringify(report).includes('raw-private'));}else assert.equal((await call).qualification.passed,false);assert.ok(target.scopes.every(scope=>scope.closed()));}
});

test('an unclassified background operation cannot qualify through HTTP success alone',async()=>{
 const target=fixture(),open=target.openScope;target.openScope=async input=>{const scope=await open(input);if(input.role==='background')scope.sendAdmin=async()=>result('status');return scope;};const report=await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5}),...target});assert.equal(report.qualification.passed,null);assert.ok(report.qualification.missing.includes('api_coverage_budget_unknown:status'));
});

test('canonical class budgets remain stricter than a wider declared purpose in aggregate and windows',()=>{
 const plan=apiCoveragePlan({inventory:[{...inventory[0],operationId:'overview',apiBudget:{p95:2000,p99:4000}}],maxDurationMs:1000});assert.deepEqual(plan.groups[0].apiBudget,{p95:100,p99:300});const row={operation:'overview',variant:'steady',category:'read',group:'api-coverage',p95:1500,p99:3000};assert.equal(assessApiCoverageLatency({plan,series:[row]}).passed,false);assert.equal(assessApiCoverageLatency({plan,windows:[{start:0,series:[row]}]}).passed,false);
});

test('missing or undeclared background variants remain incomplete instead of becoming steady evidence',async()=>{
 for(const variant of [undefined,'maximum']){const target=fixture(),open=target.openScope;target.openScope=async input=>{const scope=await open(input),send=scope.sendAdmin;scope.sendAdmin=async event=>({...await send(event),variant});return scope;};const report=await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5}),...target});assert.equal(report.qualification.passed,null);assert.ok(report.qualification.missing.some(code=>code.startsWith('api_coverage_variant_unknown:')));}
});

test('an unavailable purpose remains in the inventory and report as not connected',async()=>{
 const target=fixture(),plan=apiCoveragePlan({inventory:[{...inventory[0],available:false}],maxDurationMs:1000}),report=await runPhase({plan,...target});assert.equal(report.groups.length,1);assert.deepEqual(report.groups[0].purposes,['health']);assert.equal(report.groups[0].samples,0);assert.equal(report.qualification.passed,null);assert.ok(report.qualification.missing.some(code=>code.startsWith('api_coverage_group_not_connected:')));
});

test('a scope-observed non-2xx hidden by the returned cancelled status stays reported and incomplete',async()=>{
 const target=fixture(),open=target.openScope;target.openScope=async input=>{const scope=await open(input),collect=scope.collect;scope.collect=async()=>{const value=await collect();if(input.role==='measurement')value.transport.counts.httpNonSuccess=1;return value;};return scope;};const report=await runPhase({plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:2000,probeQps:5}),...target});assert.equal(report.qualification.passed,null);assert.ok(report.qualification.missing.includes('api_coverage_http_error_attribution_incomplete'));assert.equal(report.scopes.find(row=>row.role==='measurement').unattributedKnownHttpFailures,1);
});

test('an allocated malformed scope is still closed before its contract error is reported',async()=>{
 const target=fixture(),open=target.openScope;target.openScope=async input=>{const scope=await open(input);if(input.role==='preparation')delete scope.onResult;return scope;};let error;try{await runPhase({plan:apiCoveragePlan({inventory,maxDurationMs:1000}),...target});}catch(value){error=value;}assert.ok(getApiCoverageFailureReport(error));assert.ok(target.scopes.every(scope=>scope.closed()));
});


test('actor refresh uses actual signed cookie half-life and rejects missing or failed epoch proof',()=>{
 const evidence={epoch:1,startedAt:1000,finishedAt:1010,cookieMaxAgeSeconds:2,passed:true};
 assert.equal(apiCoverageActorRefreshDue(evidence,1999),false);assert.equal(apiCoverageActorRefreshDue(evidence,2000),true);
 for(const change of [{passed:null},{cookieMaxAgeSeconds:null},{cookieMaxAgeSeconds:0},{startedAt:3000},{finishedAt:999}])assert.equal(apiCoverageActorRefreshDue({...evidence,...change},2000),null);
});

test('coverage calls group and batch boundaries while fixed background load remains active',async()=>{
 const f=fixture(),boundaries=[],plan=apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:10000,probeQps:5,batchSize:2});
 const report=await runPhase({...f,plan,beforeBoundary:async row=>{boundaries.push(row.kind);assert.equal(f.scopes[0].closed(),false);}});
 assert.deepEqual(boundaries,['group','batch','batch','batch']);assert.equal(report.groups[0].samples,5);assert.ok(f.calls().modelCalls>0);
});

test('untrusted pending-looking cleanup errors remain real failures',async()=>{
 const f=fixture(),plan=apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:10000,probeQps:5});
 await assert.rejects(runPhase({...f,plan,releaseHandles:async()=>{const error=Error('capacity_coverage_job_identity_unknown');error.code='capacity_coverage_job_identity_unknown';throw error;}}),error=>{const report=getApiCoverageFailureReport(error);assert.equal(report.qualification.passed,false);assert.ok(report.qualification.failures.includes('api_coverage_handle_cleanup_failed'));return true;});
});


test('batch starts fit the actual remaining lease at 64 capacity / 1QPS, including slow preparation',()=>{
 assert.equal(apiCoverageExecutableCount({count:64,remainingMs:60000,probeQps:1}),60);
 assert.equal(apiCoverageExecutableCount({count:64,remainingMs:30500,probeQps:1}),30);
 assert.equal(apiCoverageExecutableCount({count:64,remainingMs:999,probeQps:1}),0);
 const plan=apiCoveragePlan({inventory:[{...inventory[0],batchCapacity:64,handleTtlMs:60000}],profile:'formal',maxDurationMs:100000});assert.equal(plan.groups[0].handleTtlMs,60000);
});


test('prepared probes require the actual variant and retain unknown raw metrics without qualified latency',async()=>{
 for(const variant of [undefined,'maximum']){const f=fixture({execution:async()=>result('health',{variant})}),report=await runPhase({...f,plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:3000,probeQps:2})});
 assert.equal(report.qualification.passed,null);assert.ok(report.qualification.missing.includes('api_coverage_variant_unknown:health'));assert.equal(report.groups[0].p95,null);assert.equal(report.groups[0].samples,0);assert.equal(report.groups[0].observedSamples,5);assert.equal(report.groups[0].passed,null);assert.equal(report.latencyQualification.passed,true);assert.ok(report.metrics.series.some(row=>row.group==='api-coverage'&&row.variant===(variant??'unknown')));}
});

test('all allocated scopes settle data readiness before closing, including preparation model results',async()=>{
 const f=fixture(),open=f.openScope,ready=new Set();f.openScope=async input=>{const scope=await open(input),close=scope.close;scope.waitForDataReadiness=async()=>{assert.equal(scope.closed(),false);ready.add(input.role);return input.role==='measurement'?{passed:null,counts:{total:0,eligible:0}}:{passed:true,counts:{total:1,eligible:1}};};scope.close=async()=>{assert.ok(ready.has(input.role));return close();};return scope;};
 const report=await runPhase({...f,plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:3000,probeQps:2})});assert.equal(report.qualification.passed,true);assert.deepEqual([...ready],['background','preparation','measurement']);
});


test('unknown actual method is incomplete for M and P while known wrong methods and non-2xx remain false',async()=>{
 for(const source of ['background','probe']){const f=fixture({execution:source==='probe'?async()=>result('health',{method:undefined}):undefined});if(source==='background'){const open=f.openScope;f.openScope=async input=>{const scope=await open(input),send=scope.sendAdmin;scope.sendAdmin=async event=>({...await send(event),method:undefined});return scope;};}
 const report=await runPhase({...f,plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:3000,probeQps:2})});assert.equal(report.qualification.passed,null);assert.ok(report.qualification.missing.includes('api_coverage_http_method_unknown'));if(source==='probe'){assert.equal(report.groups[0].passed,null);assert.equal(report.groups[0].samples,0);assert.equal(report.groups[0].observedSamples,5);}}
 for(const extra of [{method:'POST'},{method:undefined,status:500,success:false}]){const f=fixture({execution:async()=>result('health',extra)}),report=await runPhase({...f,plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:3000,probeQps:2})});assert.equal(report.qualification.passed,false);assert.equal(report.groups[0].passed,false);}
});

test('missing actual route is incomplete at the group level without qualifying target samples',async()=>{
 const f=fixture({execution:async()=>result('health',{requestSignature:undefined})}),report=await runPhase({...f,plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:3000,probeQps:2})});assert.equal(report.qualification.passed,null);assert.equal(report.groups[0].passed,null);assert.equal(report.groups[0].samples,0);assert.equal(report.groups[0].observedSamples,5);assert.ok(report.qualification.missing.includes('api_coverage_client_route_unknown'));
});


test('known non-2xx including redirects cannot become a cancellation-only incomplete result',async()=>{
 for(const status of [302,503]){const abort=new AbortController(),f=fixture({execution:async()=>{abort.abort();return result('health',{status,success:false,httpEnded:false,error:'client_cancelled'});}}),report=await runPhase({...f,signal:abort.signal,plan:apiCoveragePlan({inventory:[inventory[0]],maxDurationMs:1000})});assert.equal(report.qualification.passed,false);assert.equal(report.groups[0].failed,1);assert.equal(report.groups[0].cancelled,0);assert.ok(report.groups[0].errors.http_non_success>0);}
});
