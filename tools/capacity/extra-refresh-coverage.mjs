import {performance} from 'node:perf_hooks';
import {setTimeout as delay} from 'node:timers/promises';
import {currentCapacityObservation} from './observation-adapter.mjs';
import {isCapacityCoverageCleanup,runCapacityCoverageCleanup} from './coverage-factory.mjs';

const errors=new WeakMap(),leases=new WeakMap();
export const getExtraRefreshCoverageFailure=error=>errors.get(error);
const pending=new Set(['capacity_coverage_extra_refresh_identity_unknown','capacity_coverage_extra_refresh_still_active','capacity_coverage_extra_refresh_transport_incomplete','capacity_coverage_extra_refresh_status_unavailable']);
export const getExtraRefreshCoveragePending=error=>pending.has(errors.get(error))?errors.get(error):undefined;
const fail=code=>{const error=Error(code);errors.set(error,code);throw error;};
const uuid=value=>typeof value==='string'&&/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i.test(value);
const componentTerminal=new Set(['refreshed','verified','pending_verification','rejected','failed','cancelled','unsupported','skipped','not_started']);
const jobTerminal=new Set(['completed','verified','pending_verification','rejected','failed','cancelled','unsupported']);
const readState=lease=>{const state=leases.get(lease);if(!state)fail('capacity_coverage_extra_refresh_lease_not_owned');return state;};
const requireObservation=scope=>{const ambient=currentCapacityObservation();if(!ambient?.ledger||ambient.scopeId!==scope?.id||!['baseline','auxiliary'].includes(ambient.source))fail('capacity_coverage_extra_refresh_observation_missing');return ambient;};
function validJob(value,state){return value?.id===state.jobId&&value.account_id===state.accountId&&Number.isSafeInteger(value.created_at)&&value.created_at>=0&&value.components&&typeof value.components==='object'&&Object.keys(value.components).length===2&&['subscription','extra'].every(name=>typeof value.components[name]?.state==='string');}
function terminalJob(value){return jobTerminal.has(value.state)&&Number.isSafeInteger(value.finished_at)&&value.finished_at>=value.created_at&&['subscription','extra'].every(name=>componentTerminal.has(value.components[name].state));}

// Jobs belong to the gateway's in-memory controller. Neither HTTP 202 nor a
// closed client socket proves its readers have finished. Every status read is
// issued through the original factory-created auxiliary capability and witnessed.
export function createExtraRefreshCoverage({call,store,awaitCoverageTransport,timeoutMs=10000,now=()=>performance.now(),sleep=ms=>delay(ms)}={}){
 if(typeof call!=='function'||!store||typeof store.get!=='function'||typeof awaitCoverageTransport!=='function'||!Number.isSafeInteger(timeoutMs)||timeoutMs<1||timeoutMs>600000||typeof now!=='function'||typeof sleep!=='function')fail('capacity_coverage_extra_refresh_configuration');
 const stateFor=lease=>{const state=readState(lease);if(state.owner!==api)fail('capacity_coverage_extra_refresh_lease_not_owned');return state;};
 async function witness(transport){if(typeof transport.trace!=='string')fail('capacity_coverage_extra_refresh_transport_incomplete');const value=await awaitCoverageTransport({scope:transport.scope,trace:transport.trace});if(value?.passed===false)fail('capacity_coverage_extra_refresh_transport_conflict');if(value?.passed!==true||value.settled!==true)fail('capacity_coverage_extra_refresh_transport_incomplete');transport.settled=true;return value;}
 async function tracked(state,scope,path,options){requireObservation(scope);const transport={scope,trace:null,settled:false};state.transports.push(transport);state.recordTransport(transport);const result=await call(path,options);transport.trace=result.trace;return {result,transport};}
 function captureSubmission(state,result){
  if(result.success===true&&result.status===202&&uuid(result.json?.id)&&result.json.account_id===state.accountId){state.jobId=result.json.id;state.identityUnknown=false;return true;}
  if(Number.isInteger(result.status)&&(result.status<200||result.status>=300))state.rejected=true;
  else state.identityUnknown=true;
  return false;
 }
 async function effects(state,job){
  // This is independent database evidence of the completed real quota reader,
  // not an inference from a caller-supplied terminal-looking result DTO.
  const account=await store.get('SELECT quota_json,quota_error,route_epoch FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL',state.accountId);
  if(job.components.subscription.state!=='refreshed'||job.components.subscription.verification!=='snapshot_refreshed')fail('capacity_coverage_extra_refresh_failed');
  let quota;try{quota=JSON.parse(account?.quota_json);}catch{}
  // A job can join a real quota singleflight whose read finished before this
  // job existed, but whose transaction committed afterwards. Its checked time
  // precedes COMMIT. Prove a new persisted snapshot relative to preparation in
  // that case; a terminal-looking DTO cannot make an unchanged old snapshot new.
  const baseline=state.quotaBaseline,advanced=baseline&&Number.isSafeInteger(baseline.checked)&&Number.isSafeInteger(quota?.checked)&&quota.checked>baseline.checked&&account?.quota_json!==baseline.json;
  if(!account||!baseline||account.route_epoch!==baseline.identityEpoch||account.quota_error!==null||!Number.isSafeInteger(quota?.checked)||quota.checked<job.created_at&&!advanced||!Array.isArray(quota.windows))fail('capacity_coverage_extra_refresh_effect_mismatch');
  const extra=job.components.extra;
  if(['verified','pending_verification','rejected'].includes(extra.state)){
   const snapshot=await store.get('SELECT identity_epoch,last_refresh_at,last_refresh_state,entitlement FROM harbor.account_extra_entitlements WHERE account_id=$1',state.accountId);
   if(!snapshot||snapshot.identity_epoch!==account.route_epoch||snapshot.last_refresh_state!=='success'||!Number.isSafeInteger(snapshot.last_refresh_at)||snapshot.last_refresh_at<job.created_at||snapshot.entitlement?.state!==({verified:'verified',rejected:'rejected',pending_verification:'unknown'})[extra.state])fail('capacity_coverage_extra_refresh_effect_mismatch');
  }else if(extra.state!=='unsupported')fail('capacity_coverage_extra_refresh_failed');
 }
 async function poll(state,{scope,actor},signal){
  const deadline=now()+timeoutMs;
  for(;;){signal?.throwIfAborted();const remaining=deadline-now();if(remaining<=0)fail('capacity_coverage_extra_refresh_still_active');
   const {result,transport}=await tracked(state,scope,state.path+'/'+state.jobId,{actor,signal,timeoutMs:Math.max(1,Math.floor(remaining))});await witness(transport);if(signal?.aborted&&result.error==='client_cancelled'&&!(Number.isInteger(result.status)&&(result.status<200||result.status>=300)))signal.throwIfAborted();
   if(result.success!==true||result.status!==200)fail('capacity_coverage_extra_refresh_status_unavailable');
   if(!validJob(result.json,state))fail('capacity_coverage_extra_refresh_status_mismatch');
   if(terminalJob(result.json)){state.terminal=true;try{await effects(state,result.json);}catch(error){state.terminalError=error;throw error;}state.finishedAt=result.json.finished_at;return result.json;}
   if(now()>=deadline)fail('capacity_coverage_extra_refresh_still_active');await sleep(Math.min(20,Math.max(1,deadline-now())));
  }
 }
 async function submit(state,context,signal){
  requireObservation(context.scope);signal?.throwIfAborted();if(state.started)fail('capacity_coverage_extra_refresh_already_submitted');state.started=true;state.identityUnknown=true;
  const response=await tracked(state,context.scope,state.path,{method:'POST',body:{},actor:context.actor,signal});state.submissionTransport=response.transport;const accepted=captureSubmission(state,response.result);return {...response,accepted};
 }
 const api={
  create({accountId,cleanupCapability,recordTransport=()=>{}}={}){if(typeof accountId!=='string'||!(/^[a-zA-Z0-9][a-zA-Z0-9._:-]{0,199}$/).test(accountId)||!isCapacityCoverageCleanup(cleanupCapability)||typeof recordTransport!=='function')fail('capacity_coverage_extra_refresh_lease_configuration');const lease=Object.freeze({});leases.set(lease,{owner:api,accountId,path:'/api/admin/accounts/'+encodeURIComponent(accountId)+'/extra-usage/refresh',capability:cleanupCapability,recordTransport,transports:[],started:false,jobId:null,identityUnknown:false,rejected:false,terminal:false,released:false,operation:null});return lease;},
  async prepare(lease,{operationId,signal}={}){const state=stateFor(lease);if(state.operation||!['extra-refresh-submit','extra-refresh-status'].includes(operationId))fail('capacity_coverage_extra_refresh_operation_invalid');state.operation=operationId;
   const before=await store.get('SELECT quota_json,quota_error,route_epoch FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL',state.accountId);let quota;try{quota=JSON.parse(before?.quota_json);}catch{}
   state.quotaBaseline=before&&Number.isSafeInteger(before.route_epoch)?{identityEpoch:before.route_epoch,error:before.quota_error,json:before.quota_json,checked:Number.isSafeInteger(quota?.checked)&&Array.isArray(quota.windows)?quota.checked:null}:null;
   if(operationId==='extra-refresh-status')await runCapacityCoverageCleanup(state.capability,async context=>{const response=await submit(state,context,signal);await witness(response.transport);if(!response.accepted)fail(state.identityUnknown?'capacity_coverage_extra_refresh_identity_unknown':'capacity_coverage_extra_refresh_submission_failed');await poll(state,context,signal);});
   return {path:state.operation==='extra-refresh-submit'?state.path:state.path+'/'+state.jobId,method:state.operation==='extra-refresh-submit'?'POST':'GET',...(state.operation==='extra-refresh-submit'?{body:{}}:{})};
  },
  async execute(lease,{scope,actor,signal}={}){const state=stateFor(lease);if(state.released||!state.operation)fail('capacity_coverage_extra_refresh_lease_invalid');
   if(state.operation==='extra-refresh-submit'){const response=await submit(state,{scope,actor},signal);return {...response.result,businessVerified:response.accepted};}
   if(!state.terminal||state.terminalError)fail('capacity_coverage_extra_refresh_status_not_prepared');const {result,transport}=await tracked(state,scope,state.path+'/'+state.jobId,{actor,signal});await witness(transport);let verified=result.success===true&&result.status===200&&validJob(result.json,state)&&terminalJob(result.json);if(verified){await effects(state,result.json);verified=result.json.finished_at===state.finishedAt;}return {...result,businessVerified:verified===true};
  },
  async release(lease){const state=stateFor(lease);if(state.released)return;return runCapacityCoverageCleanup(state.capability,async context=>{
   for(const transport of state.transports)if(!transport.settled)await witness(transport);
   if(state.started&&!state.jobId&&!state.rejected){if(state.submissionTransport){const proof=await witness(state.submissionTransport);if(proof.reasonCodes?.includes('coverage_request_not_sent'))state.rejected=true;}if(!state.rejected)fail('capacity_coverage_extra_refresh_identity_unknown');}
   if(state.jobId&&!state.terminal)await poll(state,context);
   state.released=true;if(state.terminalError)throw state.terminalError;
  });},
  status(lease){const state=stateFor(lease);return Object.freeze({started:state.started,jobId:state.jobId,terminal:state.terminal,released:state.released,identityUnknown:state.identityUnknown});}
 };
 return Object.freeze(api);
}
