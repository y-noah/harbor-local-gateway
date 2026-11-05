import {capacityRunBudget} from './certificate-lifetime.mjs';
import {capacityTargetRunLifetime} from './gateway-adapter.mjs';
import {runCapacitySuite} from './runner.mjs';
import {runIndependentUiAcceptance,uiAcceptancePlan,getUiFailureReport} from './ui-acceptance.mjs';
import {createNormalExecution} from './normal-execution.mjs';
import {runIndependentApiCoverage,getApiCoverageFailureReport} from './api-coverage.mjs';
import {targets} from './scenarios.mjs';

const browserKeys=['browserLists','browserStatistics'];

// Preserve the original API report. Only its missing browser facets may be
// satisfied by the separate UI phase; an API failure must never disappear.
export function combineCapacityReports(normal,ui,apiCoverage){
  if(!normal?.verdict||!Array.isArray(normal.verdict.failures)||!Array.isArray(normal.verdict.incomplete))throw new Error('capacity_execution_report_invalid');
  const failures=[...normal.verdict.failures],incomplete=[...normal.verdict.incomplete];
  const independent=ui?.scope==='independent-ui-additional-load'&&ui.normalMixReplaced===false&&ui.plan?.profile==='formal'&&ui.phase?.window?.finished===true&&ui.phase.window.declaredMs===ui.plan.durationMs&&ui.phase.window.observedMs>=ui.plan.durationMs;
  for(const key of browserKeys){
    if(ui?.[key]?.passed===false)failures.push('独立浏览器验收失败:'+key);
    else if(independent&&ui?.[key]?.passed===true&&normal.evidence?.[key]?.passed!==false){
      const index=incomplete.indexOf('缺少验收证据:'+key);if(index!==-1)incomplete.splice(index,1);
    }else incomplete.push('独立浏览器验收证据不足:'+key);
  }
  if(ui?.phase?.failures?.length||ui?.traffic?.passed===false||ui?.scopeEvidence?.dataReconciliation?.passed===false)failures.push('独立浏览器负载或对账失败');
  const api=combineApiCoverageEvidence(normal,apiCoverage);failures.push(...api.failures);
  for(const value of api.resolved){const index=incomplete.indexOf(value);if(index!==-1)incomplete.splice(index,1);}
  incomplete.push(...api.incomplete);
  if(normal.profile!=='formal')incomplete.push('仅为小规模工具自验');
  if(normal.verdict.status==='failed'&&!failures.length)failures.push('原API验收失败');
  if(normal.verdict.status==='incomplete'&&!incomplete.length&&normal.verdict.incomplete.length===0)incomplete.push('原API验收未完整通过');
  return {schema:'harbor-capacity-execution-v1',profile:normal.profile,startedAt:normal.startedAt,endedAt:Date.now(),phases:{normal,ui:ui??null,...(apiCoverage?{apiCoverage}:{})},verdict:{status:failures.length?'failed':incomplete.length?'incomplete':'passed',failures:[...new Set(failures)],incomplete:[...new Set(incomplete)],observations:[...(normal.verdict.observations??[]),'浏览器为独立附加载荷阶段，时长与请求不计入原API混合场景。']}};
}

// Coverage may fill only suite-wide API omissions, never per-scene/model gates
// or an already established failure. The original API report stays unchanged.
function combineApiCoverageEvidence(normal,coverage){
  const result={failures:[],incomplete:[],resolved:[]};if(!coverage)return result;
  if(coverage.qualification?.passed===false)result.failures.push('独立接口补覆盖失败');
  else if(coverage.qualification?.passed!==true)result.incomplete.push('独立接口补覆盖未完整通过');
  const verified=coverage.scope==='independent-additional-api-coverage'&&coverage.normalMixReplaced===false&&coverage.plan?.profile==='formal'&&coverage.qualification?.passed===true&&coverage.latencyQualification?.passed===true&&Array.isArray(coverage.plan.groups)&&coverage.plan.groups.length>0&&coverage.plan.groups.every(expected=>coverage.groups?.some(actual=>actual.id===expected.id&&actual.passed===true&&actual.samples>=targets.minSamples&&actual.variant===expected.variant&&actual.canonicalHttp===expected.canonicalHttp));
  if(coverage.qualification?.passed===true&&!verified)result.incomplete.push('独立接口补覆盖缺少正式完整性证据');
  if(verified)for(const value of normal.verdict.incomplete){
    const purpose=/^接口未测:([^/]+)$/.exec(value),sample=/^样本不足:([^/]+)\/(.+)$/.exec(value),id=purpose?.[1]??sample?.[1];
    if(id&&coverage.plan.groups.some(group=>group.purposes.includes(id)&&(!sample||group.variant===sample[2])))result.resolved.push(value);
  }
  return result;
}

export function apiCoverageDuration(value,{required=false}={}){
  if(value===undefined){if(required)throw new Error('capacity_api_max_duration_required');return undefined;}
  if(!Number.isSafeInteger(value)||value<1||value>86400000)throw new Error('capacity_api_max_duration_invalid');return value;
}

async function runCapacityExecutionPhases({launchBrowser,uiMaxDurationMs,apiMaxDurationMs,observeNormal=true,...options}={}){
  if(options.plan?.profile==='formal'&&typeof launchBrowser!=='function')throw new Error('capacity_browser_launcher_required');
  if(typeof launchBrowser==='function')uiAcceptancePlan({profile:options.plan?.profile,maxDurationMs:uiMaxDurationMs});
  apiCoverageDuration(apiMaxDurationMs,{required:options.plan?.profile==='formal'});
  const observation=await createNormalExecution({target:options.target,profile:options.plan?.profile,enabled:observeNormal});let normal;
  try{normal=await runCapacitySuite({...options,target:observation.target});}
  finally{try{await observation.close();}catch(error){if(!normal)throw error;normal={...normal,verdict:{...normal.verdict,status:'failed',failures:[...normal.verdict.failures,'capacity_normal_scope_cleanup_failed']}};}}
  // Keep the existing small API qualification entry compatible. Supplying an
  // actual browser explicitly opts into the supplementary qualification too.
  let apiCoverage;
  if(apiMaxDurationMs!==undefined){
    try{options.signal?.throwIfAborted();await options.onProgress?.({kind:'independent-api-start'});capacityTargetRunLifetime(options.target)?.assertCoverage({minimumRemainingMs:apiMaxDurationMs+(typeof launchBrowser==='function'?uiMaxDurationMs??0:0)});apiCoverage=await runIndependentApiCoverage({target:options.target,profile:options.plan.profile,maxDurationMs:apiMaxDurationMs,signal:options.signal,onProgress:options.onProgress});await options.onProgress?.({kind:'independent-api-end'});}
    catch(error){const partial=getApiCoverageFailureReport(error);apiCoverage??=partial;const missing=partial?.qualification?.missing??[],cancelled=partial?.window?.cancelled===true||missing.includes('api_coverage_cancelled')||options.signal?.aborted===true&&(error===options.signal.reason||error?.name==='AbortError'||error?.cause===options.signal.reason),exhausted=partial?.window?.capReached===true||missing.includes('api_coverage_duration_exhausted');const prior=apiCoverage?.qualification,incomplete=cancelled||exhausted||partial?.qualification?.passed===null;apiCoverage={...apiCoverage,scope:'independent-additional-api-coverage',normalMixReplaced:false,execution:{completed:false,cancelled,code:cancelled?'capacity_api_phase_cancelled':exhausted?'capacity_api_phase_duration_exhausted':'capacity_api_phase_failed'},qualification:{...prior,passed:prior?.passed===false?false:incomplete?null:false,failures:[...(prior?.failures??[]),...(incomplete?[]:['capacity_api_phase_failed'])],missing:[...(prior?.missing??[]),...(cancelled?['capacity_api_phase_cancelled']:[])]}};}
  }
  if(typeof launchBrowser!=='function'){
    if(!apiCoverage)return normal;
    const api=combineApiCoverageEvidence(normal,apiCoverage),failures=[...normal.verdict.failures,...api.failures],incomplete=[...normal.verdict.incomplete.filter(value=>!api.resolved.includes(value)),...api.incomplete];
    return {schema:'harbor-capacity-execution-v1',profile:normal.profile,startedAt:normal.startedAt,endedAt:Date.now(),phases:{normal,apiCoverage,ui:null},verdict:{...normal.verdict,status:failures.length?'failed':incomplete.length?'incomplete':'passed',failures:[...new Set(failures)],incomplete:[...new Set(incomplete)]}};
  }
  let ui;
  try{
    options.signal?.throwIfAborted();
    await options.onProgress?.({kind:'independent-ui-start'});
    capacityTargetRunLifetime(options.target)?.assertCoverage({minimumRemainingMs:uiMaxDurationMs??0});
    ui=await runIndependentUiAcceptance({target:options.target,launchBrowser,profile:options.plan.profile,maxDurationMs:uiMaxDurationMs,signal:options.signal,onProgress:options.onProgress});
    await options.onProgress?.({kind:'independent-ui-end'});
  }catch(error){
    const partial=getUiFailureReport(error);ui??=partial;
    const cancelled=typeof partial?.execution?.cancelled==='boolean'?partial.execution.cancelled:options.signal?.aborted===true&&(error===options.signal.reason||error?.name==='AbortError'||error?.cause===options.signal.reason),code=cancelled?'capacity_ui_phase_cancelled':/^capacity_[a-z0-9_]{1,100}$/.test(error?.message??'')?error.message:'capacity_ui_phase_failed';
    ui={...ui,scope:'independent-ui-additional-load',normalMixReplaced:false,execution:{...ui?.execution,completed:false,cancelled,code},...Object.fromEntries(browserKeys.map(key=>[key,{...ui?.[key],passed:ui?.[key]?.passed===false?false:cancelled?null:false,missing:[...(ui?.[key]?.missing??[]),code]}]))};
  }
  return combineCapacityReports(normal,ui,apiCoverage);
}

export async function runCapacityExecution(options={}){
  // Preserve existing validation order and fail before constructing observations.
  if(options.plan?.profile==='formal'&&typeof options.launchBrowser!=='function')throw new Error('capacity_browser_launcher_required');
  if(typeof options.launchBrowser==='function')uiAcceptancePlan({profile:options.plan?.profile,maxDurationMs:options.uiMaxDurationMs});
  apiCoverageDuration(options.apiMaxDurationMs,{required:options.plan?.profile==='formal'});
  const lifetime=capacityTargetRunLifetime(options.target);
  if(options.runLifetimeMs!==undefined&&options.runLifetimeMs!==lifetime?.runLifetimeMs)throw new Error('capacity_run_lifetime_target_mismatch');
  const budget=capacityRunBudget({plan:options.plan,runLifetimeMs:lifetime?.runLifetimeMs,apiMaxDurationMs:options.apiMaxDurationMs,uiMaxDurationMs:typeof options.launchBrowser==='function'?options.uiMaxDurationMs:undefined});
  lifetime?.assertCoverage({minimumRemainingMs:budget.scheduledMs});
  const signal=lifetime?AbortSignal.any([options.signal,lifetime.signal].filter(Boolean)):options.signal;
  const report=await runCapacityExecutionPhases({...options,signal});
  if(lifetime?.runLifetimeMs!==undefined){
    const evidence=lifetime.evidence();report.runLifetime={...budget,...evidence};
    if(evidence.deadlineReached){report.verdict={...report.verdict,status:report.verdict.status==='failed'?'failed':'incomplete',incomplete:[...new Set([...report.verdict.incomplete,'capacity_run_lifetime_exhausted'])]};}
  }
  return report;
}
