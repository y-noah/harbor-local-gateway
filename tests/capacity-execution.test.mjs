import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtemp,writeFile,stat,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {registerHooks} from 'node:module';
import {combineCapacityReports,runCapacityExecution,apiCoverageDuration} from '../tools/capacity/execution.mjs';
import {capacityPlan} from '../tools/capacity/scenarios.mjs';
import {credentialFingerprints} from '../tools/capacity/http-client.mjs';

const normal=()=>({profile:'formal',startedAt:1,evidence:{},verdict:{status:'incomplete',failures:[],incomplete:['缺少验收证据:browserLists','缺少验收证据:browserStatistics'],observations:[]}});
const ui=()=>({scope:'independent-ui-additional-load',normalMixReplaced:false,plan:{profile:'formal',durationMs:11000000},phase:{window:{finished:true,declaredMs:11000000,observedMs:11000001},failures:[]},traffic:{passed:true},scopeEvidence:{dataReconciliation:{passed:true}},browserLists:{passed:true},browserStatistics:{passed:true}});

test('independent UI only fills its missing facets and preserves the full API report',()=>{
  const source=normal(),snapshot=structuredClone(source),browser=ui();const report=combineCapacityReports(source,browser);
  assert.equal(report.verdict.status,'incomplete');assert.deepEqual(report.verdict.incomplete,['缺少验收证据:remoteConsumption']);assert.strictEqual(report.phases.normal,source);assert.strictEqual(report.phases.ui,browser);assert.deepEqual(source,snapshot);
  source.verdict.incomplete.push('缺少验收证据:hostRestoreRpo');assert.equal(combineCapacityReports(source,browser).verdict.status,'incomplete');
});

test('API failures, existing failed browser results and independent integrity failures cannot be hidden',()=>{
  const source=normal();source.verdict.status='failed';source.verdict.failures=['正常组成功率:model/steady'];source.evidence.browserLists={passed:false};
  let report=combineCapacityReports(source,ui());assert.equal(report.verdict.status,'failed');assert.ok(report.verdict.failures.includes('正常组成功率:model/steady'));assert.ok(report.verdict.incomplete.includes('缺少验收证据:browserLists'));
  const bad=ui();bad.traffic.passed=false;report=combineCapacityReports(normal(),bad);assert.equal(report.verdict.status,'failed');assert.ok(report.verdict.failures.includes('独立浏览器负载或对账失败'));
});

test('qualification, interrupted windows and mixed-in browser reports cannot replace formal evidence',()=>{
  for(const mutate of [value=>value.plan.profile='selfcheck',value=>value.phase.window.finished=false,value=>value.phase.window.observedMs=1,value=>value.normalMixReplaced=true,value=>value.scope='normal']){
    const browser=ui();mutate(browser);const report=combineCapacityReports(normal(),browser);assert.equal(report.verdict.status,'incomplete');assert.ok(report.verdict.incomplete.includes('缺少验收证据:browserLists'));
  }
  const source=normal();source.profile='selfcheck';assert.equal(combineCapacityReports(source,ui()).verdict.status,'incomplete');
});

test('formal execution without a browser launcher fails before generating model or admin traffic',async()=>{
  let calls=0;await assert.rejects(runCapacityExecution({plan:capacityPlan(),target:{sendModel:()=>calls++,sendAdmin:()=>calls++},executeLong:true}),/capacity_browser_launcher_required/);assert.equal(calls,0);
});

function smallExecution({afterApi=()=>{}}={}){
  const plan=structuredClone(capacityPlan('selfcheck'));plan.scenarios=[{id:'api-before-ui',durationMs:20,modelQps:50,adminQps:50,employees:1,administrators:1}];
  const secret='synthetic-execution-key',cookie='harbor_session=synthetic-execution-cookie',base={success:true,complete:true,httpEnded:true,wireSent:true,durationMs:1,startedAt:0,firstContentAt:1};
  return {plan,observeNormal:false,preflight:{eligible:false},launchBrowser:async()=>{throw Error('must-not-start-an-unowned-browser');},target:{employeeActors:[{memberId:'m',keyId:'k',deviceId:'d',secret}],adminActors:[{headers:{cookie}}],sendModel:async()=>({...base,operation:'model',method:'POST',success:false,error:'synthetic_model_failure',credentials:credentialFingerprints({authorization:'Bearer '+secret})}),sendAdmin:async()=>({...base,operation:'health',method:'GET',requestSignature:'GET /health',credentials:credentialFingerprints({cookie})}),collectEvidence:async()=>{afterApi();return {};}}};
}

test('UI exception retains already collected API failures and measurements',async()=>{
  const report=await runCapacityExecution(smallExecution());assert.equal(report.verdict.status,'failed');assert.equal(report.phases.normal.metrics.arrivals.sent,2);assert.ok(report.phases.normal.verdict.failures.some(value=>value.startsWith('正常组成功率:model')));assert.ok(report.verdict.failures.some(value=>value.startsWith('正常组成功率:model')));assert.equal(report.phases.ui.execution.code,'capacity_target_not_owned');assert.equal(report.phases.ui.browserLists.passed,false);
});

test('cancellation after API collection preserves the API report and marks UI incomplete',async()=>{
  const controller=new AbortController(),options=smallExecution({afterApi:()=>controller.abort(new Error('synthetic-private-cancellation-detail'))}),report=await runCapacityExecution({...options,signal:controller.signal});
  assert.equal(report.phases.normal.metrics.arrivals.sent,2);assert.equal(report.phases.ui.execution.cancelled,true);assert.equal(report.phases.ui.execution.code,'capacity_ui_phase_cancelled');assert.equal(report.phases.ui.browserLists.passed,null);assert.equal(report.verdict.status,'failed');assert.equal(JSON.stringify(report).includes('synthetic-private-cancellation-detail'),false);
});

test('progress callback errors do not discard API data or disclose arbitrary error messages',async()=>{
  const report=await runCapacityExecution({...smallExecution(),onProgress:async value=>{if(value.kind==='independent-ui-start')throw Error('private-error-body');}});assert.equal(report.phases.ui.execution.code,'capacity_ui_phase_failed');assert.equal(report.phases.normal.metrics.arrivals.sent,2);assert.equal(JSON.stringify(report).includes('private-error-body'),false);
});

test('an unrelated error after cancellation is not reclassified as a cancellation or allowed to inject a forged report',async()=>{
  const controller=new AbortController(),report=await runCapacityExecution({...smallExecution(),signal:controller.signal,onProgress:async value=>{if(value.kind==='independent-ui-start'){controller.abort();const error=new Error('private-unrelated-error');error.report={forgedErrorReport:'private-report-payload'};throw error;}}});
  assert.equal(report.phases.ui.execution.cancelled,false);assert.equal(report.phases.ui.execution.code,'capacity_ui_phase_failed');assert.equal(report.verdict.status,'failed');assert.equal(JSON.stringify(report).includes('private-unrelated-error'),false);assert.equal(JSON.stringify(report).includes('private-report-payload'),false);assert.equal(Object.hasOwn(report.phases.ui,'forgedErrorReport'),false);
});

test('cancelling an end callback cannot downgrade already established UI failures',async()=>{
  // Isolate only the two phase boundaries; execute the unchanged production
  // orchestrator and cancellation handler, without running a formal long test.
  const entry=new URL('../tools/capacity/execution.mjs?interruption-case',import.meta.url).href,source=normal(),browser=ui(),controller=new AbortController();browser.browserLists={passed:false,failures:['formal_ui_evidence_failed:resources']};browser.browserStatistics={passed:false,failures:['formal_ui_evidence_failed:resources']};browser.formalEvidence={resources:{passed:false}};
  const hooks=registerHooks({resolve(specifier,context,nextResolve){
    if(context.parentURL===entry&&['./runner.mjs','./ui-acceptance.mjs'].includes(specifier)){
      const name=specifier==='./runner.mjs'?'runCapacitySuite':'runIndependentUiAcceptance',value=name==='runCapacitySuite'?source:browser;
      return {url:'data:text/javascript,'+encodeURIComponent('export async function '+name+'(){return '+JSON.stringify(value)+';}'+(name==='runIndependentUiAcceptance'?'export function uiAcceptancePlan(){return {};}export function getUiFailureReport(){return undefined;}':'')),shortCircuit:true};
    }
    return nextResolve(specifier,context);
  }});
  try{
    const {runCapacityExecution:run}=await import(entry),report=await run({plan:{profile:'selfcheck'},observeNormal:false,launchBrowser:()=>{},signal:controller.signal,onProgress:({kind})=>{if(kind==='independent-ui-end'){controller.abort();throw Error('private-end-detail');}}});
    assert.equal(report.verdict.status,'failed');assert.equal(report.phases.ui.browserLists.passed,false);assert.equal(report.phases.ui.browserStatistics.passed,false);assert.deepEqual(report.phases.ui.browserLists.failures,['formal_ui_evidence_failed:resources']);assert.equal(report.phases.ui.formalEvidence.resources.passed,false);assert.deepEqual(report.phases.normal,source);assert.equal(JSON.stringify(report).includes('private-end-detail'),false);
  }finally{hooks.deregister();}
});

test('formal CLI rejects missing or invalid browser configuration before importing the allocating adapter',async()=>{
  const root=await mkdtemp(join(tmpdir(),'harbor-capacity-execution-')),adapter=join(root,'adapter.mjs'),marker=join(root,'allocated'),launcher=join(root,'launcher.mjs'),validLauncher=join(root,'valid-launcher.mjs'),execute=promisify(execFile);
  try{
    await writeFile(adapter,'import {writeFileSync} from "node:fs";writeFileSync('+JSON.stringify(marker)+',"allocated");export async function createTarget(){throw Error("unexpected_allocation");}\n');await writeFile(launcher,'export const launchBrowser=false;\n');await writeFile(validLauncher,'export async function launchBrowser(){throw Error("unexpected_launch");}\n');
    for(const [extra,code] of [[[],'capacity_browser_launcher_required'],[['--browser-launcher',launcher],'capacity_browser_launcher_invalid'],[['--browser-launcher',validLauncher],'capacity_ui_max_duration_required'],[['--browser-launcher',validLauncher,'--ui-max-duration-ms','1'],'capacity_ui_duration_below_sample_budget'],[['--browser-launcher',validLauncher,'--ui-max-duration-ms','1e9'],'capacity_ui_duration_below_sample_budget'],[['--browser-launcher',validLauncher,'--ui-max-duration-ms','12000000'],'capacity_api_max_duration_required'],[['--browser-launcher',validLauncher,'--ui-max-duration-ms','12000000','--api-max-duration-ms','0'],'capacity_api_max_duration_invalid']]){
      await assert.rejects(execute(process.execPath,['tools/capacity/cli.mjs','run','--execute-long','--adapter',adapter,...extra],{cwd:new URL('..',import.meta.url),timeout:10000,maxBuffer:65536}),error=>error.stderr.includes(code));await assert.rejects(stat(marker),{code:'ENOENT'});
    }
  }finally{await rm(root,{recursive:true,force:true});}
});

const apiCoverage=()=>({scope:'independent-additional-api-coverage',normalMixReplaced:false,plan:{profile:'formal',groups:[{id:'health-default',variant:'default',canonicalHttp:'GET /health',purposes:['health']}]},groups:[{id:'health-default',variant:'default',canonicalHttp:'GET /health',samples:1000,passed:true}],qualification:{passed:true},latencyQualification:{passed:true}});
test('independent API coverage fills only matching API gaps and never replaces scene gates or prior failures',()=>{
 const source=normal();source.verdict.incomplete.push('接口未测:health','样本不足:health/default','样本不足:health/maximum','接口未测:ready','场景测量样本不足:burst/model','场景管理请求样本不足:burst');source.verdict.failures.push('接口延迟:health/default');source.verdict.status='failed';const before=structuredClone(source),report=combineCapacityReports(source,ui(),apiCoverage());
 assert.equal(report.verdict.status,'failed');assert.ok(report.verdict.failures.includes('接口延迟:health/default'));assert.equal(report.verdict.incomplete.includes('接口未测:health'),false);assert.equal(report.verdict.incomplete.includes('样本不足:health/default'),false);for(const key of ['样本不足:health/maximum','接口未测:ready','场景测量样本不足:burst/model','场景管理请求样本不足:burst'])assert.ok(report.verdict.incomplete.includes(key));assert.deepEqual(source,before);assert.strictEqual(report.phases.normal,source);assert.equal(report.phases.apiCoverage.qualification.passed,true);
});
test('incomplete, failed, short or mismatched API coverage never clears original missing evidence',()=>{
 for(const change of [value=>value.qualification.passed=null,value=>value.qualification.passed=false,value=>value.normalMixReplaced=true,value=>value.plan.profile='selfcheck',value=>value.groups[0].samples=999,value=>value.latencyQualification.passed=null,value=>value.groups[0].variant='maximum',value=>value.groups[0].canonicalHttp='GET /ready',value=>value.groups=[],value=>value.plan.groups=[]]){
  const coverage=apiCoverage();change(coverage);const source=normal();source.verdict.incomplete.push('接口未测:health');const report=combineCapacityReports(source,ui(),coverage);assert.ok(report.verdict.incomplete.includes('接口未测:health'));assert.notEqual(report.verdict.status,'passed');
 }
});
test('API coverage duration is explicit and bounded before formal traffic starts',async()=>{
 assert.equal(apiCoverageDuration(undefined),undefined);assert.equal(apiCoverageDuration(86400000),86400000);for(const value of [0,-1,Infinity,NaN,86400001,'1000'])assert.throws(()=>apiCoverageDuration(value),/capacity_api_max_duration_invalid/);assert.throws(()=>apiCoverageDuration(undefined,{required:true}),/capacity_api_max_duration_required/);
 let calls=0;await assert.rejects(runCapacityExecution({plan:{profile:'formal'},target:{sendModel:()=>calls++},launchBrowser:()=>{},uiMaxDurationMs:12000000}),/capacity_api_max_duration_required/);assert.equal(calls,0);
});

for(const outcome of ['success','budget','cancelled','known-failed','untrusted-error'])test('API phase orchestration retains original report and handles '+outcome,async()=>{
 const entry=new URL('../tools/capacity/execution.mjs?api-'+outcome,import.meta.url).href,source=normal(),coverage=apiCoverage(),key='__harborApiExecution'+outcome,controller=new AbortController();let partial;
 if(outcome==='budget')coverage.qualification={passed:null,failures:[],missing:['api_coverage_duration_exhausted']};if(outcome==='known-failed')coverage.qualification={passed:false,failures:['api_coverage_source_conflict'],missing:[]};
 globalThis[key]={source,coverage,run:async()=>{if(outcome==='success')return coverage;const error=new Error('private-error-detail');if(outcome==='untrusted-error')error.report={forged:'private-forged-report',qualification:{passed:true}};else if(outcome!=='cancelled')partial=error;throw error;},partial:error=>error===partial?coverage:undefined};
 const hooks=registerHooks({resolve(specifier,context,nextResolve){if(context.parentURL===entry&&specifier==='./runner.mjs')return {url:'data:text/javascript,'+encodeURIComponent('export async function runCapacitySuite(){return globalThis['+JSON.stringify(key)+'].source;}'),shortCircuit:true};if(context.parentURL===entry&&specifier==='./api-coverage.mjs')return {url:'data:text/javascript,'+encodeURIComponent('const x=globalThis['+JSON.stringify(key)+'];export const runIndependentApiCoverage=x.run,getApiCoverageFailureReport=x.partial;'),shortCircuit:true};return nextResolve(specifier,context);}});
 try{const {runCapacityExecution:run}=await import(entry),report=await run({plan:{profile:'selfcheck'},target:{},observeNormal:false,apiMaxDurationMs:1000,signal:controller.signal,onProgress:({kind})=>{if(outcome==='cancelled'&&kind==='independent-api-start'){controller.abort();throw controller.signal.reason;}}});assert.strictEqual(report.phases.normal,source);assert.equal(report.phases.apiCoverage.qualification.passed,outcome==='success'?true:['budget','cancelled'].includes(outcome)?null:false);assert.equal(JSON.stringify(report).includes('private-error-detail'),false);assert.equal(JSON.stringify(report).includes('private-forged-report'),false);if(outcome==='known-failed')assert.ok(report.phases.apiCoverage.qualification.failures.includes('api_coverage_source_conflict'));if(outcome==='budget')assert.ok(report.phases.apiCoverage.qualification.missing.includes('api_coverage_duration_exhausted'));}
 finally{hooks.deregister();delete globalThis[key];}
});

test('real API setup cancellation wrapper retains private cancellation provenance',async()=>{
 const key='__harborRealApiCancellation',entry=new URL('../tools/capacity/execution.mjs?real-api-cancellation',import.meta.url).href,api=new URL('../tools/capacity/api-coverage.mjs?real-api-cancellation',import.meta.url).href,source=normal(),controller=new AbortController();
 globalThis[key]={source,owned:{getApiCoverageInventory(){},prepareApiCoverageBatch(){},executeApiCoverageHandle(){},releaseApiCoverageHandles(){},createObservationScope(){},configureScenario(){},modelActorCount:1,adminActors:Array.from({length:5},()=>({})),prepareActorEpoch:async({signal})=>{controller.abort();signal.throwIfAborted();}}};
 const hooks=registerHooks({resolve(specifier,context,nextResolve){if(context.parentURL===entry&&specifier==='./runner.mjs')return {url:'data:text/javascript,'+encodeURIComponent('export async function runCapacitySuite(){return globalThis['+JSON.stringify(key)+'].source;}'),shortCircuit:true};if(context.parentURL===entry&&specifier==='./api-coverage.mjs')return {url:api,shortCircuit:true};if(context.parentURL===api&&specifier==='./gateway-adapter.mjs')return {url:'data:text/javascript,'+encodeURIComponent('export const ownedCapacityTarget=()=>globalThis['+JSON.stringify(key)+'].owned;'),shortCircuit:true};return nextResolve(specifier,context);}});
 try{const {runCapacityExecution:run}=await import(entry),report=await run({plan:{profile:'selfcheck'},target:{},observeNormal:false,apiMaxDurationMs:1000,signal:controller.signal});assert.equal(report.phases.apiCoverage.execution.cancelled,true);assert.equal(report.phases.apiCoverage.execution.code,'capacity_api_phase_cancelled');assert.equal(report.phases.apiCoverage.qualification.passed,null);assert.ok(report.phases.apiCoverage.qualification.missing.includes('api_coverage_cancelled'));assert.strictEqual(report.phases.normal,source);}
 finally{hooks.deregister();delete globalThis[key];}
});


test('plain remote execution DTO cannot complete a formal combined report',()=>{
 for(const dto of [undefined,null,{passed:true,missing:[],failures:[],evidence:{actualExecutionVerified:true}},Object.freeze({})]){
  const report=combineCapacityReports(normal(),ui(),undefined,dto);assert.equal(report.verdict.status,'incomplete');assert.ok(report.verdict.incomplete.includes('缺少验收证据:remoteConsumption'));assert.equal(report.phases.remoteConsumption.passed,null);
 }
});

for(const outcome of ['passed','pending','failed'])test('final remote consumption is collected after UI cleanup and preserves '+outcome+' facts',async()=>{
 // Only the owned-source and phase boundaries are mocked. This executes the
 // real orchestration/combiner and never claims actual remote/formal proof.
 const entry=new URL('../tools/capacity/execution.mjs?remote-final-'+outcome,import.meta.url).href,key='__harborFinalRemote'+outcome,source=normal(),browser=ui(),result={passed:outcome==='passed'?true:outcome==='failed'?false:null,missing:outcome==='pending'?['actual_browser_not_measured']:[],failures:outcome==='failed'?['actual_remote_observation_failed']:[],evidence:{scope:'mocked-source-boundary-only'}};
 if(outcome==='failed'){source.verdict.status='failed';source.verdict.failures.push('正常组成功率:model/steady');}
 const state={normal:source,ui:browser,coverage:apiCoverage(),result,helperWorkload:Object.freeze({}),order:[],uiClosed:false,lifetime:{runLifetimeMs:100000,signal:new AbortController().signal,assertCoverage(){},evidence:()=>({deadlineReached:false})}};globalThis[key]=state;
 const prefix='const state=globalThis['+JSON.stringify(key)+'];',modules={
  './gateway-adapter.mjs':prefix+`const receipts=new WeakMap();export function capacityTargetRunLifetime(){return state.lifetime;}export async function collectCapacityTargetRemoteConsumption(target,{helperWorkload}={}){if(helperWorkload!==state.helperWorkload)throw Error('wrong_normal_run');if(!state.uiClosed)throw Error('collected_before_ui_cleanup');state.order.push('remote');const handle=Object.freeze({});receipts.set(handle,state.result);return handle;}export function inspectCapacityTargetRemoteConsumption(handle){const row=receipts.get(handle);if(!row)throw Error('not_owned');return row;}`,
  './runner.mjs':prefix+`export async function runCapacitySuite(){state.order.push('normal');return state.normal;}`,
  './normal-execution.mjs':prefix+`export async function createNormalExecution({target}){return {target,helperWorkload:state.helperWorkload,close:async()=>state.order.push('normalClosed')};}`,
  './api-coverage.mjs':prefix+`export async function runIndependentApiCoverage(){state.order.push('api');return state.coverage;}export function getApiCoverageFailureReport(){return undefined;}`,
  './ui-acceptance.mjs':prefix+`export function uiAcceptancePlan(){return {};}export function getUiFailureReport(){return undefined;}export async function runIndependentUiAcceptance(){state.uiClosed=true;state.order.push('uiClosed');return state.ui;}`
 };
 const hooks=registerHooks({resolve(specifier,context,nextResolve){if(context.parentURL===entry&&modules[specifier])return {url:'data:text/javascript,'+encodeURIComponent(modules[specifier]),shortCircuit:true};return nextResolve(specifier,context);}});
 try{const {runCapacityExecution:run}=await import(entry),before=structuredClone(source),report=await run({plan:{profile:'formal',scenarios:[]},target:{},launchBrowser:()=>{},apiMaxDurationMs:1,uiMaxDurationMs:1});assert.deepEqual(state.order,['normal','normalClosed','api','uiClosed','remote']);assert.equal(report.verdict.status,outcome==='passed'?'passed':outcome==='pending'?'incomplete':'failed');assert.deepEqual(report.phases.normal,source);assert.deepEqual(source,before);assert.equal(report.phases.remoteConsumption.passed,result.passed);assert.equal(report.verdict.incomplete.includes('缺少验收证据:sources'),false);if(outcome==='failed'){assert.ok(report.verdict.failures.includes('正常组成功率:model/steady'));assert.ok(report.verdict.failures.includes('远端实际执行验收失败'));}}
 finally{hooks.deregister();delete globalThis[key];}
});
