import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {writeFile} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {uiAcceptancePlan,runUiScheduledPhase,runIndependentUiAcceptance,assessUiHttpOutcomes,assessUiQualification,assessUiBackgroundOutcomes} from '../tools/capacity/ui-acceptance.mjs';
import {createUiTrafficLedger} from '../tools/capacity/ui-traffic-ledger.mjs';
import {createCapacityBrowserAssessment} from '../tools/capacity/browser-assessment.mjs';

const result=()=>({operation:'employee-list',method:'GET',success:true,complete:true,businessVerified:true,durationMs:1,trace:'synthetic-trace'});
test('UI is an additional independent phase with its own duration and unchanged normal background',()=>{
 const plan=uiAcceptancePlan({profile:'formal',actionQps:1,maxDurationMs:12000000});assert.equal(plan.minimumActions,11000);assert.equal(plan.minimumScheduledMs,11000000);assert.equal(plan.durationMs,11000000);assert.equal(plan.maxDurationMs,12000000);assert.throws(()=>uiAcceptancePlan({profile:'formal'}),/capacity_ui_max_duration_required/);assert.throws(()=>uiAcceptancePlan({profile:'formal',maxDurationMs:1800000}),/capacity_ui_duration_below_sample_budget/);assert.equal(plan.model.qps,10);assert.equal(plan.model.employees,60);assert.equal(plan.model.payloadBytes,102400);assert.deepEqual(plan.model.upstreamMs,[3000,6000]);assert.equal(plan.baselineAdmin.qps,5);assert.deepEqual(plan.baselineAdmin.mix,{list:0.5,detail:0.2,summary:0.2,configuration:0.1});assert.equal(plan.normalMixReplaced,false);assert.equal(plan.series.filter(row=>row.budgetMs===1500).length,9);assert.equal(plan.series.filter(row=>row.budgetMs===3000).length,2);assert.throws(()=>uiAcceptancePlan({profile:'formal',actionQps:1,durationMs:1800000,maxDurationMs:12000000}),/below_sample_budget/);assert.ok(Object.isFrozen(plan.series[0]));
});
test('scheduled preparation and draining do not inflate the actual UI observation window',async()=>{
 const plan=uiAcceptancePlan({durationMs:80,actionQps:1});let measured=false,ended=false,received=0;const safe=[];
 const report=await runUiScheduledPhase({plan,begin:()=>{measured=true;},end:()=>{ended=true;},sendModel:async()=>{assert.equal(measured,true);await delay(180);return {...result(),json:{private_key:'synthetic-not-for-dto'}};},sendAdmin:async()=>result(),onResult:async value=>{received++;safe.push(value);},measure:async()=>{await delay(140);assert.equal(ended,true,'slow actions complete during separately accounted drain');return {businessVerified:true,durationMs:140,depth:1,activeModelBefore:1,activeModelAfter:1};}});
 assert.equal(ended,true);assert.equal(received,2);assert.ok(report.window.observedMs>=80);assert.ok(report.window.observedMs<160);assert.equal(report.normalMixReplaced,false);assert.ok(report.missing.some(code=>code.startsWith('ui_samples_below_target')));assert.ok(!JSON.stringify(safe).includes('synthetic-not-for-dto'));assert.equal(safe[0].result.validatedSuccess,true);
});
test('busy administrators produce explicit missed-action failures rather than a slower closed loop',async()=>{
 const plan=uiAcceptancePlan({durationMs:1200,actionQps:5});let actions=0;
 const report=await runUiScheduledPhase({plan,sendModel:async()=>result(),sendAdmin:async()=>result(),measure:async()=>{actions++;await delay(1100);return {businessVerified:true,durationMs:1100,depth:1};}});
 assert.ok(report.failures.includes('ui_scheduled_actor_busy'));assert.equal(actions,5);assert.equal(report.metrics.arrivals.inflight,0);assert.ok(report.series.some(row=>row.failed>0));
});
test('cancellation stops each producer and remains an incomplete window',async()=>{
 const abort=new AbortController(),plan=uiAcceptancePlan({durationMs:1000});const promise=runUiScheduledPhase({plan,signal:abort.signal,sendModel:async()=>result(),sendAdmin:async()=>result(),measure:async()=>({businessVerified:true,durationMs:1})});abort.abort();const report=await promise;assert.equal(report.window.finished,false);assert.ok(report.interruptions.includes('ui_measurement_cancelled'));assert.deepEqual(report.failures,[]);assert.equal(report.metrics.arrivals.inflight,0);
});
test('caller-made target objects cannot start independent browser or model traffic',async()=>{
 let launched=0;await assert.rejects(runIndependentUiAcceptance({target:{},launchBrowser:async()=>{launched++;}}),/capacity_target_not_owned/);assert.equal(launched,0);
});

test('observer failures do not double-record an action or abandon background draining',async()=>{
 const plan=uiAcceptancePlan({durationMs:20});let drained=false;const report=await runUiScheduledPhase({plan,sendModel:async()=>{await delay(70);drained=true;return result();},sendAdmin:async()=>result(),measure:async()=>({businessVerified:true,durationMs:1,depth:1,activeModelBefore:1,activeModelAfter:1}),onProgress:async()=>{throw Error('synthetic_observer_failure');},end:()=>{throw Error('synthetic_end_failure');}});assert.equal(drained,true);assert.equal(report.metrics.arrivals.inflight,0);assert.ok(report.failures.includes('ui_progress_callback_failed'));assert.ok(report.failures.includes('ui_window_close_failed'));assert.ok(report.series.every(row=>row.inflight===0));assert.equal(report.series.reduce((sum,row)=>sum+row.failed,0),0);
});

test('fully reconciled unsampled refresh 500s fail the separate UI HTTP success gate',()=>{const traffic={passed:true,counts:[{phase:'measurement',source:'browser',normalAttempts:1000,httpSucceeded:900,httpFailed:100,httpNonSuccess:100,clientCancelled:7,statusClasses:{'2xx':900,'5xx':100}}]};const result=assessUiHttpOutcomes(traffic);assert.equal(result.passed,false);assert.ok(result.failures.includes('ui_http_success_rate:measurement/browser'));assert.equal(result.groups[0].cancelled,7);assert.equal(result.groups[0].attempts,1000);assert.equal(result.groups[0].statusClasses['5xx'],100);assert.equal(assessUiHttpOutcomes({counts:[{phase:'drain',source:'browser',normalAttempts:0,httpSucceeded:0,httpFailed:0,clientCancelled:1}]}).passed,null);});

test('sample completion extends beyond skipped actions then stops arrivals before the explicit cap',async()=>{const base=uiAcceptancePlan({durationMs:50,maxDurationMs:2000,actionQps:5}),plan={...base,series:[{view:'requests',variant:'deep',budgetMs:1500,minSamples:5}]};let first=true;const report=await runUiScheduledPhase({plan,sendModel:async()=>result(),sendAdmin:async()=>result(),measure:async()=>{if(first){first=false;return {skipped:'pagination_end',paginationEndVerified:true,depth:2,totalPages:2};}return {businessVerified:true,durationMs:1,depth:3,activeModelBefore:1,activeModelAfter:1};}});assert.equal(report.window.stopReason,'samples-collected');assert.ok(report.window.observedMs>=1000);assert.ok(report.window.observedMs<1800);assert.equal(report.series[0].success,5);assert.equal(report.series[0].actors,5);assert.equal(report.series[0].skipped,1);assert.equal(report.metrics.arrivals.inflight,0);assert.ok(!report.failures.includes('ui_load_generator_failed'));});

const pgBin=process.env.HARBOR_CAPACITY_PG_BIN,pgLib=process.env.HARBOR_CAPACITY_PG_LIB,browserModule=process.env.HARBOR_CAPACITY_BROWSER_MODULE,browserBinary=process.env.HARBOR_CAPACITY_BROWSER_BINARY;
test('independent UI qualification uses actual gateway, device models, scoped billing and all browser wire traffic',{skip:!pgBin||!browserModule||!browserBinary?'需显式私有PG18、Playwright及Chromium；不冒称真实UI主链通过':false,timeout:240000},async()=>{
 const {createTarget}=await import('../tools/capacity/gateway-adapter.mjs'),{chromium}=await import(browserModule);let target;
 try{
  target=await createTarget({profile:'selfcheck',binDirectory:pgBin,libraryDirectory:pgLib});
  const report=await runIndependentUiAcceptance({target,profile:'selfcheck',maxDurationMs:120000,onProgress:event=>{if(event.completed%10===0)process.stdout.write('UI qualification completed actions: '+event.completed+'\n');},launchBrowser:()=>chromium.launch({executablePath:browserBinary,headless:true,args:['--no-sandbox'],env:process.env})});
  await writeFile('/tmp/harbor-ui-independent-report.json',JSON.stringify(report),{mode:0o600});assert.equal(report.normalMixReplaced,false);assert.ok(report.phase,'factory and browser hooks must be genuinely connected');assert.deepEqual(report.phase.failures,[]);assert.deepEqual(report.phase.missing,[]);assert.equal(report.phase.window.stopReason,'samples-collected');assert.ok(report.phase.window.observedMs>=report.plan.durationMs);assert.ok(report.phase.window.observedMs<=report.plan.maxDurationMs+1000);assert.ok(report.phase.series.every(row=>row.success>=5&&row.actors===5));
  assert.equal(report.traffic.passed,true);assert.equal(report.traffic.pending,0);assert.equal(report.traffic.unattributed.length,0);assert.ok(report.traffic.counts.some(row=>row.source==='browser'&&row.phase==='preparation'));assert.ok(report.traffic.counts.some(row=>row.source==='browser'&&row.phase==='measurement'));assert.ok(report.traffic.counts.some(row=>row.source==='model'));assert.ok(report.traffic.counts.some(row=>row.source==='baseline'));
  assert.equal(report.scopeEvidence.transport.passed,true);assert.equal(report.scopeEvidence.dataReconciliation.passed,true);assert.equal(report.qualification.passed,true);assert.equal(report.browserLists.passed,null);assert.equal(report.browserStatistics.passed,null);assert.ok(report.browserLists.missing.includes('qualification_only'));assert.ok(report.browserLists.missing.includes('formal_ui_evidence_missing:entry'));
  const stages=report.scopeEvidence.streamingStages;assert.ok(stages.counts.started>0);assert.equal(stages.counts.started,stages.counts.finished);assert.equal(stages.runtime.active,0);assert.equal(stages.runtime.hardFailures,0);assert.equal(stages.metrics.bodyVisibility.upper.samples,stages.counts.started);assert.equal(stages.metrics.usageVisibility.upper.samples,stages.counts.started);assert.equal(stages.passed,null,'browser qualification does not prove formal helper/LAN topology');
  assert.equal((await target.store.get('SELECT 1 value')).value,1,'UI owns its browser/scope, not the caller target');
  const {ownedCapacityTarget}=await import('../tools/capacity/gateway-adapter.mjs'),owned=ownedCapacityTarget(target),ledger=createUiTrafficLedger({origin:owned.origin,adminFingerprints:owned.adminFingerprints});let release,reserved=0,cancelBrowser,cancelAssessment,cancelScope;const gate=new Promise(resolve=>release=resolve),until=async predicate=>{const deadline=Date.now()+10000;while(!predicate()){assert.ok(Date.now()<deadline,'trace reservation barrier timed out');await delay(10);}};
  try{
   cancelScope=await owned.createObservationScope({id:'ui-cancel-'+randomUUID(),kind:'ui',observer:{...ledger.observer,beforeRequest:async dto=>{const headers=await ledger.observer.beforeRequest(dto);reserved++;await gate;return headers;}}});ledger.beginMeasurement();
   cancelAssessment=await createCapacityBrowserAssessment({target,observer:cancelScope.observer,launchBrowser:async()=>cancelBrowser=await chromium.launch({executablePath:browserBinary,headless:true,args:['--no-sandbox'],env:process.env})});
   const preparing=cancelAssessment.prepare().then(()=>({prepared:true}),()=>({cancelled:true}));await until(()=>reserved===5);const closing=cancelAssessment.close();await until(()=>cancelAssessment.status().closed);release();await closing;await preparing;ledger.endMeasurement();
   const wire=cancelScope.readWireEvents({cursor:0});for(const event of wire.events)ledger.server(event);const traffic=ledger.collect(),scoped=await cancelScope.collect();assert.equal(traffic.passed,true);assert.equal(traffic.notSent,5);assert.equal(traffic.matched,0);assert.equal(traffic.pending,0);assert.equal(scoped.transport.passed,true);assert.equal(scoped.transport.pending,0);assert.equal(scoped.transport.counts.notSent,5);assert.equal(scoped.transport.counts.clientCancelled,5);assert.equal(scoped.transport.counts.wireStarted,0);assert.equal(cancelAssessment.status().observerErrors,0);assert.equal(cancelAssessment.status().observerPending,0);
  }finally{release?.();await cancelAssessment?.close();await cancelScope?.close();ledger.close();}
  let scriptAssessment,scriptBrowser;
  try{
   scriptAssessment=await createCapacityBrowserAssessment({target,launchBrowser:async()=>scriptBrowser=await chromium.launch({executablePath:browserBinary,headless:true,args:['--no-sandbox'],env:process.env})});await scriptAssessment.prepare();
   await scriptBrowser.contexts()[0].pages()[0].evaluate(()=>setTimeout(()=>{throw Error('synthetic_ui_probe_exception');},0));await until(()=>scriptAssessment.status().scriptErrors===1);assert.equal((await scriptAssessment.measure({view:'requests'})).businessVerified,true,'the injected page exception must not destroy the successful API/DOM control');await scriptAssessment.close();
   const rejected=assessUiQualification({phase:report.phase,traffic:report.traffic,scopeEvidence:report.scopeEvidence,dataReadiness:report.dataReadiness,httpOutcomes:report.httpOutcomes,browserStatus:scriptAssessment.status()});assert.equal(rejected.passed,false);assert.ok(rejected.failures.includes('ui_browser_script_failed'));
  }finally{await scriptAssessment?.close();}
 }finally{await target?.close();}
});

test('page, observer and wire-reader errors cannot contradict a green tool qualification',()=>{const ready={phase:{missing:[],failures:[]},traffic:{passed:true},scopeEvidence:{transport:{passed:true},dataReconciliation:{passed:true}},dataReadiness:{passed:true},httpOutcomes:{passed:true,failures:[]},browserStatus:{scriptErrors:0,observerErrors:0,observerPending:0}};assert.equal(assessUiQualification(ready).passed,true);for(const key of ['scriptErrors','observerErrors','observerPending'])assert.equal(assessUiQualification({...ready,browserStatus:{...ready.browserStatus,[key]:1}}).passed,false,key);assert.equal(assessUiQualification({...ready,missing:['ui_wire_reader_failed']}).passed,false);});

// Only the owned-target / browser factory boundaries are substituted. The UI
// scheduler, report assembly, error branding and cleanup execute unchanged.
test('UI partial reports survive readiness cancellation, collection and cleanup failures without leaking originals',()=>{
 const entry=new URL('../tools/capacity/ui-acceptance.mjs',import.meta.url).href;
 const script=`import {registerHooks} from 'node:module';
 import assert from 'node:assert/strict';
 const entry=${JSON.stringify(entry)};
 const sources={
 './gateway-adapter.mjs':'export const ownedCapacityTarget=()=>globalThis.fixture.owned;',
 './browser-assessment.mjs':'export const createCapacityBrowserAssessment=async()=>globalThis.fixture.browser;',
 './observation-adapter.mjs':'export const isOwnedObservationScope=()=>true;'
 };
 const hooks=registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&sources[specifier])return {url:'data:text/javascript,'+encodeURIComponent(sources[specifier]),shortCircuit:true};return next(specifier,context);}});
 const {runIndependentUiAcceptance,getUiFailureReport}=await import(entry);
 const ok=()=>({operation:'employee-list',method:'GET',success:true,complete:true,businessVerified:true,durationMs:1});
 for(const mode of ['script-cancel','business-cancel','pure-cancel','collect-error','browser-close-error','scope-close-error','scope-close-retry']){
  const abort=new AbortController();let browserClosed=0,scopeClosed=0,measured=0;
  const privateError=()=>Error('raw-secret-request-body');
  const scope={observer:{},sendModel:async()=>ok(),sendAdmin:async()=>ok(),onResult:async()=>{},readWireEvents:async()=>({events:[],next:0,dropped:0}),
   waitForDataReadiness:async()=>{if(mode.endsWith('cancel')){abort.abort();throw new DOMException('raw-secret-request-body','AbortError');}return {passed:true};},
   collect:async()=>{if(mode==='collect-error')throw privateError();return {transport:{passed:true,pending:0,closedAtCollect:scopeClosed>0},dataReconciliation:{passed:true}};},
   close:async()=>{scopeClosed++;if(mode==='scope-close-error'||mode==='scope-close-retry'&&scopeClosed===1)throw privateError();}};
  globalThis.fixture={owned:{origin:'http://127.0.0.1:1',adminActors:[{headers:{Cookie:'x=y'}}],configureScenario:async()=>{},createObservationScope:async()=>scope},
   browser:{observerVersion:'capacity-ui-observer-v1',prepare:async()=>{},measure:async()=>{measured++;return {businessVerified:mode!=='business-cancel',durationMs:1,activeModelBefore:1,activeModelAfter:1};},status:()=>({scriptErrors:mode==='script-cancel'?1:0,observerErrors:0,observerPending:0}),close:async()=>{browserClosed++;if(mode==='browser-close-error')throw privateError();}}};
  let thrown;try{await runIndependentUiAcceptance({target:{employeeActors:[{headers:{Authorization:'Bearer x'}}]},durationMs:20,signal:abort.signal});}catch(error){thrown=error;}
  assert.ok(thrown,mode);const report=getUiFailureReport(thrown);assert.ok(report,mode);assert.equal(measured,1);assert.equal(report.phase.series.reduce((n,row)=>n+row.businessSuccess+row.failed,0),1);assert.ok(browserClosed>=1);assert.equal(scopeClosed,mode.startsWith('scope-close-')?2:1);if(mode!=='collect-error')assert.equal(report.scopeEvidence.transport.closedAtCollect,true);assert.ok(!JSON.stringify(report).includes('raw-secret'));assert.ok(!JSON.stringify(thrown).includes('raw-secret'));assert.ok(Object.isFrozen(report));assert.equal(getUiFailureReport(Object.assign(new Error('fake'),{report})),undefined);
  if(mode==='pure-cancel'){assert.equal(report.browserLists.passed,null);assert.deepEqual(report.qualification.failures,[]);}
  else{assert.equal(report.browserLists.passed,false,mode);assert.equal(report.browserStatistics.passed,false,mode);}
  if(mode==='script-cancel')assert.ok(report.qualification.failures.includes('ui_browser_script_failed'));
  if(mode==='business-cancel')assert.ok(report.phase.failures.some(code=>code.startsWith('ui_business_failure:')));
  if(mode.startsWith('scope-close-')||mode.endsWith('close-error'))assert.ok(report.qualification.failures.includes('ui_cleanup_failed'));
 }
 hooks.deregister();`;
 execFileSync(process.execPath,['--input-type=module','-e',script],{timeout:15000,stdio:'pipe'});
});

test('active cancellation is separate from genuine model and UI failures and leaves original metrics unchanged',async()=>{
 for(const realFailure of [false,true]){
  const abort=new AbortController(),plan=uiAcceptancePlan({durationMs:1000});
  const phase=await runUiScheduledPhase({plan,signal:abort.signal,
   sendModel:async()=>({...result(),success:!realFailure,status:realFailure?500:200}),
   sendAdmin:async()=>{await delay(10);abort.abort();throw new DOMException('cancelled','AbortError');},
   measure:async()=>{await delay(20);return {cancelled:true};}});
  assert.equal(phase.backgroundOutcomes.failed,realFailure?1:0);assert.equal(phase.backgroundOutcomes.cancelled,1);assert.equal(phase.series[0].cancelled,1);assert.equal(phase.series[0].failed,0);assert.ok(phase.interruptions.includes('ui_measurement_cancelled'));
  assert.equal(phase.failures.includes('ui_background_success_rate'),realFailure);assert.equal(phase.metrics.series.find(row=>row.operation==='ui-background-model').successRate,realFailure?0:1);
 }
});

test('returned transport cancellations keep raw failed metrics and use the fixed cancellation marker for data reconciliation',async()=>{
 const abort=new AbortController(),plan=uiAcceptancePlan({durationMs:1000}),seen=[];
 const phase=await runUiScheduledPhase({plan,signal:abort.signal,
  sendModel:async()=>{await delay(10);abort.abort();return {...result(),success:false,complete:false,status:null,error:'client_cancelled'};},
  sendAdmin:async()=>result(),onResult:async row=>seen.push(row.result),
  measure:async()=>{await delay(20);return {businessVerified:true,durationMs:1};},
  onProgress:()=>{throw abort.signal.reason;}});
 assert.equal(phase.backgroundOutcomes.cancelled,1);assert.equal(phase.backgroundOutcomes.failed,0);assert.deepEqual(phase.failures,[]);assert.ok(phase.interruptions.includes('ui_progress_cancelled'));
 assert.equal(phase.metrics.series.find(row=>row.operation==='ui-background-model').successRate,0);assert.ok(seen.some(row=>row.error==='client_cancelled'&&row.validatedSuccess===false));
});

test('qualification excludes only the same-series confirmed cancellations while retaining non-cancelled failures',()=>{
 const rows=[{series:'model',succeeded:9899,failed:1,cancelled:100}];const gate=assessUiBackgroundOutcomes(rows);assert.equal(gate.passed,true);assert.equal(gate.series[0].normalAttempts,9900);assert.equal(gate.series[0].successRate,9899/9900);
 assert.equal(assessUiBackgroundOutcomes([{series:'model',succeeded:899,failed:1,cancelled:100}]).passed,false);assert.equal(assessUiBackgroundOutcomes([{series:'model',succeeded:0,failed:0,cancelled:100}]).passed,null);
});
test('cancelled non-2xx and a separate network failure count as two distinct HTTP failures',()=>{
 const base={phase:'measurement',source:'browser',normalAttempts:1001,httpSucceeded:1000,httpFailed:1,clientCancelled:1,httpNonSuccess:1,cancelledHttpNonSuccess:1};const mixed=assessUiHttpOutcomes({counts:[base]});assert.equal(mixed.passed,false);assert.equal(mixed.groups[0].qualifiedAttempts,1002);assert.equal(mixed.groups[0].qualifiedSuccessRate,1000/1002);
 const regular=assessUiHttpOutcomes({counts:[{...base,clientCancelled:0,cancelledHttpNonSuccess:0}]});assert.equal(regular.passed,true);assert.equal(regular.groups[0].qualifiedAttempts,1001);
});
