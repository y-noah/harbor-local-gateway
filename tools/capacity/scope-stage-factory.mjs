import {capacityDeviceDeliveryBinding} from './device-adapter.mjs';
import {capacityWindowsHelperObserverBinding} from './windows-helper-observer.mjs';
import {createStreamingStageRuntime} from './streaming-stage-runtime.mjs';
import {capacityRequestDriverRemoteSourceBinding} from './request-driver.mjs';
import {targets,capacityPlan} from './scenarios.mjs';
const kinds=new Set(['normal','ui','auxiliary']),groups=new Set(['normal','fault','cancel','stress','comparison']);
const clone=value=>structuredClone(value);
export function capacityStageScenarioGroup({profile='selfcheck',scenarioId}={}){const plan=capacityPlan(profile);if(scenarioId===undefined)return 'normal';if(typeof scenarioId!=='string')throw Error('capacity_stage_scenario_unknown');const scene=plan.scenarios.find(row=>row.id===scenarioId);if(!scene&&profile==='formal')throw Error('capacity_stage_scenario_unknown');return scene?.fault?'fault':scene?.burst?'stress':'normal';}
// Per-scope wrappers are private capabilities issued only by the owned factory.
// Source routing consults the persistent owner, never a caller-supplied role.
export function createScopeStageFactory({ledgerRoot,getGateway,upstream,reader,profile='selfcheck',maxScopes=128,maxActive=10000,verifyTopology,topologyProof,areSourcesStopped=()=>false,closeTimeoutMs=targets.bodyVisibility.p99+targets.releaseAfterUpstreamMs,helperRequired=false,getRemoteDriver=()=>null}={}){
 if(typeof getRemoteDriver!=='function'||typeof helperRequired!=='boolean'||!ledgerRoot||['createScope','readTrace','recordEvent'].some(key=>typeof ledgerRoot[key]!=='function')||typeof getGateway!=='function'||typeof reader?.read!=='function'||typeof verifyTopology!=='function'||typeof areSourcesStopped!=='function'||!['selfcheck','formal'].includes(profile)||![maxScopes,maxActive,closeTimeoutMs].every(value=>Number.isSafeInteger(value)&&value>0)||maxScopes>128)throw Error('capacity_scope_stages_configuration');
 const scopes=new Map(),helpers=new Map();let stopped=false,routeFailures=0;
 function createLedger({id,kind,group='normal'}={}){
  if(stopped||!kinds.has(kind)||!groups.has(group)||typeof id!=='string'||scopes.size>=maxScopes)throw Error('capacity_scope_stages_scope');const raw=ledgerRoot.createScope('obs:'+id);if(kind==='auxiliary')return raw;if(scopes.has(raw.scope))throw Error('capacity_scope_stages_duplicate');
  const gateway=getGateway();if(!gateway?.stageSource)throw Error('capacity_scope_stages_gateway_unavailable');
  const remoteDriver=getRemoteDriver(),remoteBinding=capacityRequestDriverRemoteSourceBinding(remoteDriver);
  const runtime=createStreamingStageRuntime({remoteDriver:remoteBinding?remoteDriver:null,view:raw,group,helperRequired,getHelperForTrace:({owner})=>helpers.get(owner.actorIndex)??null,gatewaySource:gateway.stageSource,upstream,reader,profile,...(profile==='selfcheck'?{minSamples:1}:{}),maxActive,verifyTopology,topologyProof,readObserverStatus:async()=>({parent:gateway.observationStatus(),child:await gateway.command('status'),helper:{failures:[...helpers.values()].some(row=>row.observer.status().fault)?1:0}})});
  const row={scope:raw.scope,runtime,gateway,last:null,closed:false,dirty:false};scopes.set(raw.scope,row);
  return Object.freeze({...raw,clientStageObserver:runtime.clientStageObserver,onResult:async value=>{try{return await raw.onResult(value);}finally{runtime.notify(value?.result?.trace);}},collectStages:()=>collectRow(row),finishObservation:async()=>{row.last=await runtime.close({timeoutMs:gateway.stageSource.isOpen()?closeTimeoutMs:1});row.closed=row.last.runtime.closed===true;return row.closed;}});
 }
 async function collectRow(row){if(row.closed&&!row.dirty&&row.last)return clone(row.last);row.last=await row.runtime.collect();row.dirty=false;return clone(row.last);}
 function route(method,event){
  if(stopped)return false;if(typeof event?.trace!=='string')return true;let owner;try{owner=ledgerRoot.readTrace(event.trace);}catch{routeFailures++;return false;}if(!owner||owner.source!=='model')return true;const row=scopes.get(owner.scope);if(!row){routeFailures++;return false;}try{row.dirty=true;return row.runtime[method](event);}catch{routeFailures++;return false;}
 }
 function registerHelper({actorIndex,actor}={}){
  const binding=capacityDeviceDeliveryBinding(actor);if(stopped||!helperRequired||!Number.isSafeInteger(actorIndex)||actorIndex<0||helpers.size>=128||helpers.has(actorIndex)||!binding||!capacityWindowsHelperObserverBinding(binding.observer))throw Error('capacity_scope_helper_not_owned');helpers.set(actorIndex,binding);binding.observer.startSampling();return true;
 }
 async function prepareHelper(actorIndex){if(!helperRequired)return true;const binding=helpers.get(actorIndex);if(!binding)throw Error('capacity_scope_helper_not_owned');await binding.observer.sample();return true;}
 function observeHelper({actorIndex,event}={}){
  if(stopped)return false;let owner;try{owner=ledgerRoot.readTrace(event?.trace);}catch{routeFailures++;return false;}if(!owner||owner.source!=='model')return true;
  const binding=helpers.get(actorIndex),row=scopes.get(owner.scope);if(!row||!binding){routeFailures++;return false;}if(owner.actorIndex!==actorIndex)routeFailures++;row.dirty=true;return row.runtime.observeHelper({binding,event});
 }
 const status=()=>({scopes:scopes.size,maxScopes,routeFailures,stopped,rows:[...scopes.values()].map(row=>({scope:row.scope,closed:row.closed,...row.runtime.status()}))});
 async function snapshots(){const rows=[];for(const row of scopes.values()){try{row.last=await collectRow(row);}catch{row.last={passed:row.last?.passed===false?false:null,incomplete:true,runtime:{...row.runtime.status(),hardFailures:row.last?.runtime?.hardFailures??0,missing:['stage_snapshot_unavailable']}};}rows.push({scope:row.scope,...clone(row.last)});}return {passed:rows.some(row=>row.passed===false||row.runtime?.hardFailures>0)?false:null,routeFailures,scopes:rows};}
 async function settleStoppedSources({timeoutMs=15000}={}){
  if(areSourcesStopped()!==true)return {released:false,reason:'owned_sources_not_stopped',...await snapshots()};stopped=true;const deadline=performance.now()+timeoutMs;
  for(const row of scopes.values())if(!row.closed)try{row.last=await collectRow(row);}catch{}
  for(;;){const pending=[...scopes.values()].some(row=>{const state=row.runtime.status();return state.timers||state.running||state.queued;});if(!pending)return {released:true,...await snapshots()};if(performance.now()>=deadline)return {released:false,...await snapshots()};await new Promise(resolve=>setTimeout(resolve,10));}
 }
 return Object.freeze({createLedger,registerHelper,prepareHelper,observeHelper,observeGateway:event=>route('observeGateway',event),observeUpstream:event=>route('observeUpstream',event),status,snapshots,settleStoppedSources});
}
