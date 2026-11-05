import http from 'node:http';
import https from 'node:https';
import {capacityDriverAgentBinding} from './request-driver.mjs';
import {setTimeout as delay} from 'node:timers/promises';
import {performance} from 'node:perf_hooks';
import {randomUUID} from 'node:crypto';
import {requestCapacityTarget} from './http-client.mjs';
import {capacityObservedApiClass} from './observation-adapter.mjs';
import {capacityWireSignature} from './gateway-probe.mjs';
import {operations} from './scenarios.mjs';
import {createExtraRefreshCoverage,getExtraRefreshCoverageFailure,getExtraRefreshCoveragePending} from './extra-refresh-coverage.mjs';
import {isCapacityCoverageCleanup,runCapacityCoverageCleanup} from './coverage-factory.mjs';
import {createCoverageVariants,getCoverageVariantFailure} from './coverage-variants.mjs';

const coverageErrors=new WeakMap();
export const getCoverageInventoryFailureReason=error=>coverageErrors.get(error);
const httpFailure=status=>Number.isInteger(status)&&(status<200||status>=300);
const pendingCoverageErrors=new WeakMap(),preparedCoverageLeases=new WeakMap();
export const getPreparedCoverageLease=handles=>preparedCoverageLeases.get(handles)?.();
export const getPendingCoverageReason=error=>pendingCoverageErrors.get(error);
const pendingCoverageCodes=new Set(['capacity_coverage_transport_settlement_not_connected','capacity_coverage_transport_settlement_incomplete','capacity_coverage_job_still_active','capacity_coverage_job_identity_unknown','capacity_coverage_job_missing','capacity_coverage_pool_initialization_incomplete','capacity_coverage_handoff_candidate_settlement_not_connected','capacity_coverage_variant_data_incomplete','capacity_coverage_extra_refresh_identity_unknown','capacity_coverage_extra_refresh_still_active','capacity_coverage_extra_refresh_transport_incomplete','capacity_coverage_extra_refresh_status_unavailable']);

export async function pollCapacityTask({submittedAt,readStatus,isTerminal,signal,timeoutMs=10000,now=()=>performance.now(),sleep=ms=>new Promise(resolve=>setTimeout(resolve,ms))}={}){
  if(!Number.isFinite(submittedAt)||!Number.isInteger(timeoutMs)||timeoutMs<1||typeof readStatus!=='function'||typeof isTerminal!=='function')throw new Error('capacity_task_poll_configuration');const deadline=submittedAt+timeoutMs;
  for(;;){signal?.throwIfAborted();const remaining=deadline-now();if(remaining<=0)throw new Error('capacity_async_result_timeout');const response=await readStatus({timeoutMs:Math.max(1,Math.floor(remaining))});if(now()>deadline)throw new Error('capacity_async_result_timeout');if(!response.success)throw new Error('capacity_async_status_failed');if(isTerminal(response.json))return response.json;await sleep(Math.min(20,Math.max(1,deadline-now())));}
}

// Independent configuration objects are reserved without delaying HTTP arrivals;
// exhausted fixture capacity is explicit rather than silently serializing load.
export async function createCapacityInventoryAdapter({origin,agent,getAgent=null,adminActors,employeeActors,store,signal,configurationTargetCount=adminActors?.length,coverageTargetCount=configurationTargetCount,coverageHandleTtlMs=60000,coverageClock=Date.now,coverageVariants='steady',prepareCoverageHandoffCandidate=null,settleCoverageHandoffCandidate=null,getAdditionalPendingReason=null,awaitCoverageTransport=null,getDirectCleanupCapability=null}={}){
  if(getDirectCleanupCapability!==null&&typeof getDirectCleanupCapability!=='function')throw new Error('capacity_coverage_cleanup_configuration');
  if(!['steady','expanded'].includes(coverageVariants))throw new Error('capacity_coverage_variants_configuration');
  if(!Number.isInteger(coverageTargetCount)||coverageTargetCount<1||coverageTargetCount>1000||!Number.isSafeInteger(coverageHandleTtlMs)||coverageHandleTtlMs<1||coverageHandleTtlMs>3600000||typeof coverageClock!=='function'||prepareCoverageHandoffCandidate!==null&&typeof prepareCoverageHandoffCandidate!=='function'||settleCoverageHandoffCandidate!==null&&typeof settleCoverageHandoffCandidate!=='function'||getAdditionalPendingReason!==null&&typeof getAdditionalPendingReason!=='function'||awaitCoverageTransport!==null&&typeof awaitCoverageTransport!=='function'||!Number.isInteger(configurationTargetCount)||configurationTargetCount<1||configurationTargetCount>1000)throw new Error('capacity_configuration_pool_invalid');
  if(getAgent!==null&&typeof getAgent!=='function')throw Error('capacity_inventory_agent_getter_invalid');
  const currentAgent=()=>{if(getAgent===null)return agent;const value=getAgent(),binding=capacityDriverAgentBinding(value);if(!value||!binding&&!(value instanceof http.Agent)&&!(value instanceof https.Agent))throw Error('capacity_inventory_agent_unavailable');if(binding){const state=binding.evidence();if(state.closed||state.closing)throw Error('capacity_inventory_agent_retired');}return value;};
  const call=(path,{method='GET',body,actor=adminActors[0],employee=false,mode='json',timeoutMs=30000,signal:requestSignal=signal}={})=>requestCapacityTarget({origin:employee?actor.origin:origin,path,method,body,headers:{'content-type':'application/json',...actor.headers},agent:employee?actor.agent:currentAgent(),mode,signal:requestSignal,timeoutMs});
  const requireOk=result=>{if(!result.success)throw new Error('capacity_inventory_http_failed');return result.json;};
  const fixture=requireOk(await call('/api/admin/members',{method:'POST',body:{name:'容量完整接口合成员工',expires:null,quota:null,allowed_models:['gpt-6.1-sol']}})),memberId=fixture.memberId;
  const key=await store.get('SELECT id FROM harbor.keys WHERE member_id=$1',memberId);if(!key||!memberId)throw new Error('capacity_inventory_fixture_failed');
  const request=await store.get('SELECT r.id,r.session_id,r.member_id,r.usage_at,r.model,r.billing_model,r.status FROM harbor.requests r JOIN harbor.recordings c ON c.request_id=r.id WHERE c.persisted_sequence>0 ORDER BY r.started DESC,r.id DESC LIMIT 1'),account=await store.get("SELECT id FROM harbor.accounts WHERE state='ready' AND auth_mode='subscription' ORDER BY id LIMIT 1"),device=employeeActors[0];
  if(!request||!account||!device)throw new Error('capacity_inventory_fixture_missing');
  const apiAccount=requireOk(await call('/api/admin/accounts',{method:'POST',body:{name:'容量受控目录验证账号',kind:'codex',auth_mode:'openai_api',api_key:'synthetic-capacity-not-real-'+randomUUID(),api_weight:0}}));
  const types=requireOk(await call('/api/admin/member-types')),type=types.types.at(-1);if(!type)throw new Error('capacity_inventory_type_missing');
  // These objects are outside the 60 model actors. Preparation completes before
  // any measured management slot; no HTTP read-back is hidden in the baseline.
  const configurationTargets=[],baseDevice=await store.get('SELECT csr,public_key_sha256 FROM harbor.employee_devices WHERE id=$1',device.deviceId);
  if(!baseDevice)throw new Error('capacity_configuration_device_missing');
  const createConfigurationTarget=async(index,requestSignal=signal,context=null)=>{
    const name='容量独立配置目标'+index,state=context?.state??{};
    if(context&&state.failure)throw state.failure;
    if(!state.started){state.started=true;state.scope=context?.scope;try{const response=await call('/api/admin/members',{method:'POST',body:{name,expires:null,quota:null,allowed_models:['gpt-6.1-sol']},actor:context?.actor??adminActors[0],signal:requestSignal});state.response={success:response.success,status:response.status,cancelled:response.error==='client_cancelled',callerCancelled:response.error==='client_cancelled'&&requestSignal?.aborted===true&&!httpFailure(response.status),trace:response.trace,memberId:response.json?.memberId??null};}catch(error){state.failure=error;throw error;}}
    const created=state.response;if(!created){if(context)failCoverage('capacity_coverage_pool_initialization_incomplete');throw new Error('capacity_configuration_member_missing');}
    if(context&&!state.primaryFailure){const code=httpFailure(created.status)?'capacity_coverage_pool_create_http_failed':!created.success&&!created.callerCancelled?'capacity_coverage_pool_create_transport_failed':created.success&&(typeof created.memberId!=='string'||!created.memberId)?'capacity_coverage_pool_create_contract_failed':null;if(code)state.primaryFailure=coverageError(code);}
    if(context){try{await requireTransportWitness({scope:state.scope,trace:created.trace});state.proofFailure=null;}catch(error){state.proofFailure=error;if(!state.primaryFailure)throw error;}if(state.primaryFailure)throw state.primaryFailure;}
    if(created.cancelled&&requestSignal?.aborted&&!httpFailure(created.status))requestSignal.throwIfAborted();
    if(!state.memberId){if(created.success&&created.memberId)state.memberId=created.memberId;else if(context&&created.callerCancelled&&!httpFailure(created.status)){const matches=await store.all('SELECT id FROM harbor.members WHERE name=$1 AND removed_at IS NULL LIMIT 2',name);if(matches.length!==1)failCoverage('capacity_coverage_pool_initialization_incomplete');state.memberId=matches[0].id;}else if(context&&!httpFailure(created.status))failCoverage('capacity_coverage_pool_initialization_incomplete');else throw new Error('capacity_inventory_http_failed');}
    requestSignal?.throwIfAborted();
    if(!state.target){const configurationKey=await store.get('SELECT id FROM harbor.keys WHERE member_id=$1',state.memberId);if(!configurationKey)throw new Error('capacity_configuration_key_missing');state.target={memberId:state.memberId,keyId:configurationKey.id,deviceId:randomUUID(),typeId:randomUUID(),threadId:randomUUID(),extraAccountId:randomUUID()};}
    const {memberId,keyId,deviceId,typeId,threadId,extraAccountId}=state.target;
    if(!state.initialized)try{await store.transaction(async()=>{
      await store.run('INSERT INTO harbor.member_types(id,code,name,description) VALUES($1,$2,$3,$4)',typeId,'capacity-'+typeId,'容量独立配置类型'+index,'只用于隔离容量测试配置写入');
      // Never add configuration-only accounts to the live model routing pool.
      await store.run("INSERT INTO harbor.accounts(id,name,home,enabled,state) VALUES($1,'容量独立权益配置',$2,0,'offline')",extraAccountId,'synthetic-capacity-extra-'+extraAccountId);
      await store.run(`INSERT INTO harbor.employee_devices(id,member_id,name,system,state,hardware_state,fingerprint_source,public_key_sha256,csr,created,enrollment_digest,enrollment_expires) VALUES($1,$2,$3,'capacity-fixture','pending','unavailable','capacity-fixture',$4,$5,$6,$7,$6)`,deviceId,memberId,'容量独立配置设备'+index,baseDevice.public_key_sha256,baseDevice.csr,Date.now(),randomUUID());
    });state.initialized=true;}catch(error){state.failure=error;throw error;}
    return {...state.target,...(context?{creationTransports:[{scope:state.scope,trace:created.trace,settled:true}]}:{})};
  };
  for(let index=0;index<configurationTargetCount;index++)configurationTargets.push(await createConfigurationTarget(index));
  const busyConfigurationTargets=new Set();
  let lastBatch=null,lastExport=null,lastHandoff=null,batchSubmittedAt=null,exportSubmittedAt=null,handoffSubmittedAt=null;
  const bindings=new Map(),add=(id,category,path,method='GET',execute)=>bindings.set(id,{id,category,path,method,execute});
  for(const [id,category,path] of [
    ['health','health','/health'],['ready','readiness','/ready'],['status','readiness','/api/admin/status'],
    ['request-list','list','/api/admin/requests?view=list'],['request-detail','detail','/api/admin/requests/'+request.id],['recording-page','recording','/api/admin/requests/'+request.id+'/recording'],
    ['request-summary','summary','/api/admin/requests?view=summary'],['request-all','summary','/api/admin/requests?view=all'],['overview','summary','/api/admin/state'],
    ['employee-list','list','/api/admin/members'],['key-list','list','/api/admin/keys'],['account-list','list','/api/admin/state?people=none'],['session-list','list','/api/admin/state?people=none'],['device-list','list','/api/admin/devices'],['audit-history','list','/api/admin/history'],
    ['type-statistics','summary','/api/admin/type-statistics'],['recording-search','summary','/api/admin/recordings/search?q='+encodeURIComponent('容量')],
    ['employee-detail','detail','/api/admin/members/'+memberId],['key-detail','detail','/api/admin/members/'+memberId+'/keys'],['account-detail','detail','/api/admin/state?people=none'],['session-detail','detail','/api/admin/state?people=none'],['device-detail','detail','/api/admin/devices?member_id='+device.memberId],
    ['type-default-read','configuration','/api/admin/member-types/'+type.id+'/templates']
  ])add(id,category,path);
  const extraPath=target=>'/api/admin/accounts/'+target.extraAccountId+'/extra-usage';
  add('extra-policy-read','detail','/api/admin/extra-usage/policy');
  add('extra-coverage-read','list','/api/admin/extra-usage/coverage');
  add('extra-account-read','detail',extraPath(configurationTargets[0]));
  add('extra-records-read','list',extraPath(configurationTargets[0])+'/records');
  const configurationWriters=new Map();
  const write=(id,pathFor,payload,verify,method='PATCH')=>{configurationWriters.set(id,{pathFor,payload,verify});add(id,'configuration',pathFor(configurationTargets[0]),method,async(actor,target)=>{
    const body=await payload(target),result=await call(pathFor(target),{method,body,actor});
    return {...result,businessVerified:result.success&&await verify(target,body,result.json)};
  });};
  write('employee-save',t=>'/api/admin/members/'+t.memberId,async()=>({name:'容量配置-'+randomUUID()}),async(t,body)=>(await store.get('SELECT name FROM harbor.members WHERE id=$1',t.memberId)).name===body.name);
  write('key-save',t=>'/api/admin/keys/'+t.keyId,async()=>({enabled:true}),async t=>(await store.get('SELECT enabled FROM harbor.keys WHERE id=$1',t.keyId)).enabled===1);
  write('model-permissions-save',t=>'/api/admin/keys/'+t.keyId,async t=>({allowed_models:['gpt-6.1-sol'],expected_model_policy_version:(await store.get('SELECT model_policy_version FROM harbor.keys WHERE id=$1',t.keyId)).model_policy_version}),async(t,body)=>{const row=await store.get('SELECT allowed_models,model_policy_version FROM harbor.keys WHERE id=$1',t.keyId);return row.model_policy_version===body.expected_model_policy_version+1&&JSON.parse(row.allowed_models)[0]==='gpt-6.1-sol';});
  write('limits-save',t=>'/api/admin/members/'+t.memberId+'/usage-limits',async t=>({enabled:false,rules:[],expected_version:(await store.get('SELECT usage_limits_version FROM harbor.members WHERE id=$1',t.memberId)).usage_limits_version}),async(t,body)=>(await store.get('SELECT usage_limits_version FROM harbor.members WHERE id=$1',t.memberId)).usage_limits_version===body.expected_version+1);
  write('type-default-save',t=>'/api/admin/member-types/'+t.typeId,async t=>({defaults:{allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]}},expected_version:(await store.get('SELECT version FROM harbor.member_types WHERE id=$1',t.typeId)).version}),async(t,body)=>(await store.get('SELECT version FROM harbor.member_types WHERE id=$1',t.typeId)).version===body.expected_version+1);
  write('device-state-save',t=>'/api/admin/devices/'+t.deviceId,async t=>({name:'容量设备-'+randomUUID(),expected_version:(await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',t.deviceId)).version}),async(t,body)=>{const row=await store.get('SELECT name,version FROM harbor.employee_devices WHERE id=$1',t.deviceId);return row.name===body.name&&row.version===body.expected_version+1;});
  const extraConfiguration=target=>store.get('SELECT a.route_epoch,COALESCE(e.version,0)::integer version,e.mode FROM harbor.accounts a LEFT JOIN harbor.account_extra_usage e ON e.account_id=a.id WHERE a.id=$1',target.extraAccountId);
  write('extra-account-save',extraPath,async target=>{const row=await extraConfiguration(target);return {mode:row.mode==='deny'?'inherit':'deny',budget:{basis:'reference_estimate',unit:'reference_usd_atoms',limit_atoms:'1000000000000',request_reserve_atoms:'1000000000'},expected_version:row.version,expected_identity_epoch:row.route_epoch};},async(target,body)=>{const row=await store.get('SELECT version,mode,budget FROM harbor.account_extra_usage WHERE account_id=$1',target.extraAccountId);return row?.version===body.expected_version+1&&row.mode===body.mode&&Object.keys(body.budget).every(key=>row.budget?.[key]===body.budget[key]);});
  write('extra-manual-save',target=>extraPath(target)+'/manual',async target=>{const row=await extraConfiguration(target);return {remaining_atoms:null,unit:'unknown',models:['gpt-6.1-sol'],protocols:['responses'],expires_at:null,reset_cards:null,note:'容量合成人工记录；不授予消费资格',expected_version:row.version,expected_identity_epoch:row.route_epoch};},async(target,body,json)=>{const row=await store.get('SELECT version,manual_record FROM harbor.account_extra_usage WHERE account_id=$1',target.extraAccountId);return json?.state==='unverified'&&row?.version===body.expected_version+1&&row.manual_record?.id===json.id&&row.manual_record?.source==='manual'&&row.manual_record?.state==='unverified';},'POST');
  const preview=async actor=>{
    const current=await store.get('SELECT version FROM harbor.member_types WHERE id=$1',type.id),result=await call('/api/admin/member-config-batches/preview',{method:'POST',body:{scope:{member_ids:[memberId]},type_id:type.id,expected_type_version:current.version,models:false,limits:false,classify:true,overwrite_exceptions:false},actor});
    if(result.success)lastBatch=result.json.id;return {...result,businessVerified:result.success&&!!await store.get('SELECT id FROM harbor.member_config_batches WHERE id=$1',result.json.id)};
  };
  add('batch-preview','batchPreview','/api/admin/member-config-batches/preview','POST',preview);
  add('batch-submit','task','/api/admin/member-config-batches/{batch_id}/execute','POST',async actor=>{const prepared=await preview(actor);if(!prepared.businessVerified)return prepared;const submittedAt=performance.now(),result=await call('/api/admin/member-config-batches/'+lastBatch+'/execute',{method:'POST',body:{},actor});if(result.success)batchSubmittedAt=submittedAt;return {...result,businessVerified:result.success&&!!await store.get("SELECT id FROM harbor.background_jobs WHERE target=$1 AND kind='member.config'",lastBatch)};});
  add('batch-status','task','/api/admin/member-config-batches/{batch_id}','GET',async actor=>{if(!lastBatch)await preview(actor);return call('/api/admin/member-config-batches/'+lastBatch,{actor});});
  add('export-submit','task','/api/admin/recordings/exports','POST',async actor=>{const submittedAt=performance.now(),result=await call('/api/admin/recordings/exports',{method:'POST',body:{include_body:false,format:'jsonl',filters:{member:device.memberId}},actor});if(result.success){lastExport=result.json.id;exportSubmittedAt=submittedAt;}return {...result,businessVerified:result.success&&!!await store.get('SELECT id FROM harbor.recording_exports WHERE id=$1',lastExport)};});
  add('export-status','task','/api/admin/recordings/exports/{export_id}','GET',async actor=>{if(!lastExport){const submitted=await bindings.get('export-submit').execute(actor);if(!submitted.businessVerified)return submitted;}return call('/api/admin/recordings/exports/'+lastExport,{actor});});
  add('account-check-submit','task','/api/admin/accounts/'+apiAccount.id+'/check','POST',async actor=>{const result=await call('/api/admin/accounts/'+apiAccount.id+'/check',{method:'POST',body:{},actor});return {...result,businessVerified:result.success&&result.json?.ready===true&&(await store.get('SELECT state FROM harbor.accounts WHERE id=$1',apiAccount.id)).state==='ready'};});
  add('quota-refresh-submit','task','/api/admin/accounts/'+account.id+'/quota','POST',async actor=>{const result=await call('/api/admin/accounts/'+account.id+'/quota',{method:'POST',body:{},actor});const row=await store.get('SELECT quota_attempted_at,quota_json FROM harbor.accounts WHERE id=$1',account.id);return {...result,businessVerified:result.success&&result.json?.refreshed===true&&Number.isSafeInteger(row.quota_attempted_at)&&JSON.parse(row.quota_json).plan==='synthetic-capacity-plan'};});
  let handoffSession=await store.get("SELECT s.id FROM harbor.sessions s JOIN harbor.requests r ON r.session_id=s.id WHERE r.device_id=$1 AND r.status='ok' ORDER BY r.started DESC LIMIT 1",device.deviceId);
  if(!handoffSession)throw new Error('capacity_inventory_handoff_fixture_missing');
  add('handoff-submit','task','/api/admin/sessions/'+handoffSession.id+'/handoffs','POST',async actor=>{const source=await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',handoffSession.id),destination=await store.get("SELECT id FROM harbor.accounts WHERE id<>$1 AND state='ready' AND auth_mode='subscription' ORDER BY id LIMIT 1",source.account_id),submittedAt=performance.now(),result=await call('/api/admin/sessions/'+handoffSession.id+'/handoffs',{method:'POST',body:{account_id:destination.id,expected_route_version:source.route_version},actor});if(result.success){lastHandoff=result.json.handoff_id;handoffSubmittedAt=submittedAt;}return {...result,businessVerified:result.success&&!!await store.get('SELECT id FROM harbor.session_handoffs WHERE id=$1 AND session_id=$2',result.json.handoff_id,handoffSession.id)};});
  add('handoff-status','task','/api/admin/sessions/'+handoffSession.id+'/handoffs','GET',actor=>call('/api/admin/sessions/'+handoffSession.id+'/handoffs',{actor}));
  add('extra-refresh-submit','task','/api/admin/accounts/'+account.id+'/extra-usage/refresh','POST',()=>directRefresh('extra-refresh-submit'));
  add('extra-refresh-status','task','/api/admin/accounts/'+account.id+'/extra-usage/refresh/:id','GET',()=>directRefresh('extra-refresh-status'));
  add('self','self','/api/me','GET',()=>call('/api/me',{actor:device,employee:true}));add('models','self','/v1/models','GET',()=>call('/v1/models',{actor:device,employee:true}));
    // Traffic class follows the actual wire route. Purpose budgets remain strict,
  // and aliases reference a single HTTP identity rather than inventing APIs.
  const canonicalBudgets=new Map();for(const binding of bindings.values()){
    const purpose=operations.find(row=>row.id===binding.id);const route=binding.path.replace(/\{[a-z_]+\}/g,':id');binding.workloadClass=capacityObservedApiClass({method:binding.method,route});binding.canonicalHttp=capacityWireSignature(binding.method,route);binding.purposeBudget={p95:purpose.p95,p99:purpose.p99};
    const previous=canonicalBudgets.get(binding.canonicalHttp);canonicalBudgets.set(binding.canonicalHttp,{p95:Math.min(previous?.p95??Infinity,purpose.p95),p99:Math.min(previous?.p99??Infinity,purpose.p99)});
  }
  for(const binding of bindings.values()){binding.apiBudget=Object.freeze(canonicalBudgets.get(binding.canonicalHttp));Object.freeze(binding.purposeBudget);Object.freeze(binding);}
  let queue=Promise.resolve();const execute=async(id,{actor=adminActors[0]}={})=>{
    const binding=bindings.get(id);if(!binding)throw new Error('capacity_inventory_operation_missing');
    const run=async(target,selected=actor)=>({...await(binding.execute?binding.execute(selected,target):call(binding.path,{actor:selected})),operation:id,method:binding.method,variant:'steady'});
    if(binding.category==='configuration'&&binding.method!=='GET'){
      const target=configurationTargets.find(row=>!busyConfigurationTargets.has(row));if(!target)throw new Error('capacity_configuration_pool_busy');busyConfigurationTargets.add(target);try{return await run(target);}finally{busyConfigurationTargets.delete(target);}
    }
    const invoke=()=>id==='quota-refresh-submit'?directQuotaRefresh(run):run();if(binding.method==='GET')return invoke();const result=queue.then(invoke);queue=result.catch(()=>{});return result;
  };
  const coverageTargets=[],coveragePoolSlots=[],coverageHandles=new WeakMap(),activeCoverageTokens=new Set(),pendingCoverageCleanup=new Set(),coverageAccountCheck={id:apiAccount.id},coverageQuota={id:account.id};let preparingCoverage=false,coverageExportSequence=0;
  const pendingReason=error=>getPendingCoverageReason(error)??getExtraRefreshCoveragePending(error)??getAdditionalPendingReason?.(error);
  const coverageError=code=>{const error=new Error(code);coverageErrors.set(error,code);if(pendingCoverageCodes.has(code))pendingCoverageErrors.set(error,code);return error;};
  const failCoverage=code=>{throw coverageError(code);};
  async function requireTransportWitness(transport){if(!awaitCoverageTransport)failCoverage('capacity_coverage_transport_settlement_not_connected');const witness=await awaitCoverageTransport({scope:transport.scope,trace:transport.trace});if(witness?.passed===false)failCoverage('capacity_coverage_transport_settlement_conflict');if(witness?.passed!==true||witness?.settled!==true)failCoverage('capacity_coverage_transport_settlement_incomplete');transport.settled=true;return witness;}
  const extraRefresh=createExtraRefreshCoverage({call,store,awaitCoverageTransport:input=>awaitCoverageTransport?awaitCoverageTransport(input):failCoverage('capacity_coverage_transport_settlement_not_connected')});
  const callExtra=async operation=>{try{return await operation();}catch(error){const code=getExtraRefreshCoverageFailure(error);if(code)throw coverageError(code);throw error;}};
  const extraOperations=new Set(['extra-refresh-submit','extra-refresh-status']),directRefreshEntries=new Set();
  function finishDirectRefresh(entry){
    if(entry.operationActive)failCoverage('capacity_coverage_preparation_busy');if(entry.settlement)return entry.settlement;
    const release=()=>{directRefreshEntries.delete(entry);busyConfigurationTargets.delete(coverageQuota);};
    const operation=(async()=>{
      if(entry.kind==='extra'){try{await callExtra(()=>extraRefresh.release(entry.lease));}finally{if(extraRefresh.status(entry.lease).released)release();}return;}
      if(!entry.started||!entry.capability&&entry.result?.complete===true){release();return;}
      if(!entry.capability)failCoverage('capacity_coverage_transport_settlement_not_connected');
      await runCapacityCoverageCleanup(entry.capability,async context=>{await requireTransportWitness({scope:context.scope,trace:entry.trace});});release();
    })();entry.settlement=operation;
    void operation.finally(()=>{if(entry.settlement===operation)entry.settlement=null;}).catch(()=>{});return operation;
  }
  async function recoverDirectRefreshes(){for(const entry of directRefreshEntries){if(entry.operationActive||entry.settlement)failCoverage('capacity_coverage_preparation_busy');await finishDirectRefresh(entry);}}
  async function runDirectRefresh(entry,operation){
    busyConfigurationTargets.add(coverageQuota);directRefreshEntries.add(entry);entry.operationActive=true;
    entry.operation=Promise.resolve().then(operation).finally(()=>{entry.operationActive=false;});
    try{return await entry.operation;}finally{await finishDirectRefresh(entry);}
  }
  async function directQuotaRefresh(run){
    await recoverDirectRefreshes();if(busyConfigurationTargets.has(coverageQuota))failCoverage('capacity_coverage_preparation_busy');
    const capability=getDirectCleanupCapability?.();if(getDirectCleanupCapability!==null&&!isCapacityCoverageCleanup(capability))failCoverage('capacity_coverage_cleanup_not_owned');
    const entry={kind:'quota',capability,started:false,trace:null,result:null,settlement:null};
    const invoke=async actor=>{entry.started=true;entry.result=await run(undefined,actor);entry.trace=entry.result.trace;return entry.result;};
    return runDirectRefresh(entry,()=>capability?runCapacityCoverageCleanup(capability,context=>invoke(context.actor)):invoke());
  }
  async function directRefresh(operationId){
    await recoverDirectRefreshes();if(busyConfigurationTargets.has(coverageQuota))failCoverage('capacity_coverage_preparation_busy');
    const capability=getDirectCleanupCapability?.();if(!isCapacityCoverageCleanup(capability))failCoverage('capacity_coverage_cleanup_not_owned');
    const entry={kind:'extra',capability,lease:extraRefresh.create({accountId:coverageQuota.id,cleanupCapability:capability}),settlement:null};
    return runDirectRefresh(entry,async()=>{await callExtra(()=>extraRefresh.prepare(entry.lease,{operationId,signal}));return runCapacityCoverageCleanup(capability,context=>callExtra(()=>extraRefresh.execute(entry.lease,{scope:context.scope,actor:context.actor,signal})));});
  }
  const variantAdapter=createCoverageVariants({mode:coverageVariants,request,device,memberId,clock:coverageClock});
  const metadata=()=>Object.freeze([...bindings.values()].flatMap(binding=>variantAdapter.metadata(binding.id).map(({variant,batchCapacity})=>Object.freeze({operationId:binding.id,purposes:Object.freeze([binding.id]),method:binding.method,canonicalHttp:binding.canonicalHttp,variant,handleTtlMs:coverageHandleTtlMs,workloadClass:binding.workloadClass,apiBudget:binding.apiBudget,batchCapacity:batchCapacity??(['account-check-submit','quota-refresh-submit','extra-refresh-submit','extra-refresh-status'].includes(binding.id)?1:coverageTargetCount),available:(!binding.id.startsWith('handoff-')||!!prepareCoverageHandoffCandidate)&&(!extraOperations.has(binding.id)||isCapacityCoverageCleanup(getDirectCleanupCapability?.()))}))));
  const coverageInventory=metadata;
  const leasedRead=new Set(['employee-detail','key-detail','device-detail','type-default-read','batch-status','handoff-status','extra-account-read','extra-records-read','extra-refresh-status']);
  async function coveragePool(requestSignal,actor,scope){
    while(coverageTargets.length<coverageTargetCount){requestSignal?.throwIfAborted();const index=coverageTargets.length,state=coveragePoolSlots[index]??=(Object.create(null));coverageTargets.push(await createConfigurationTarget(configurationTargetCount+index,requestSignal,{state,actor,scope}));}
    return coverageTargets;
  }
  const paged=json=>Array.isArray(json?.rows)&&json.pagination&&Number.isInteger(json.pagination.page)&&Number.isInteger(json.pagination.total);
  async function verifyRead(id,json,target){
    if(!json||typeof json!=='object')return false;
    if(id==='extra-policy-read')return typeof json.enabled==='boolean'&&Number.isSafeInteger(json.version);
    if(id==='extra-coverage-read')return paged(json)&&json.rows.every(row=>typeof row.account_id==='string');
    if(id==='extra-account-read')return json.account_id===target.extraAccountId&&Number.isSafeInteger(json.configuration?.version)&&json.usage?.account_id===target.extraAccountId;
    if(id==='extra-records-read')return json.account_id===target.extraAccountId&&Array.isArray(json.items)&&typeof json.has_more==='boolean'&&json.items.every(row=>typeof row.id==='string'&&row.source==='manual'&&row.verification==='unverified');
    if(id==='health')return json.ok===true;
    if(id==='ready')return json.ready===true;
    if(id==='status')return json.ownership===true&&!!json.pools;
    if(id==='request-detail')return json.id===request.id;
    if(id==='recording-page')return Array.isArray(json.events)&&json.events.length>0;
    if(id==='request-summary')return !!json.summary&&Number.isInteger(json.summary.requests);
    if(id==='request-all')return paged(json)&&!!json.summary;
    if(['overview','account-list','session-list'].includes(id))return Array.isArray(json.accounts)&&json.accounts.length>0&&Array.isArray(json.sessions)&&json.sessions.length>0&&!!json.sessionsPagination;
    if(id==='account-detail'){const row=json.accounts?.find(row=>row.id===account.id);return !!row&&typeof row.auth_mode==='string'&&typeof row.state==='string';}
    if(id==='session-detail'){const row=json.sessions?.[0];return !!row&&typeof row.id==='string'&&typeof row.member_id==='string'&&typeof row.account_id==='string'&&Number.isSafeInteger(row.route_version)&&Object.hasOwn(row,'standard');}
    if(id==='employee-detail')return json.id===target.memberId;
    if(id==='key-detail')return paged(json)&&json.member?.id===target.memberId&&json.rows.some(row=>row.id===target.keyId)&&json.rows.every(row=>row.member_id===target.memberId);
    if(id==='device-detail')return paged(json)&&json.member?.id===target.memberId&&json.rows.some(row=>row.id===target.deviceId);
    if(id==='type-default-read')return json.type?.id===target.typeId&&Array.isArray(json.items)&&json.items.length>0;
    if(id==='type-statistics')return paged(json)&&!!json.summary;
    if(id==='recording-search')return Array.isArray(json.items)&&json.items.length>0&&typeof json.search_kind==='string';
    if(id==='self')return json.id===device.memberId&&!!json.standard;
    if(id==='models')return Array.isArray(json.data)&&json.data.length>0;
    return paged(json);
  }
  const previewBody=async target=>({scope:{member_ids:[target.memberId]},type_id:target.typeId,expected_type_version:(await store.get('SELECT version FROM harbor.member_types WHERE id=$1',target.typeId)).version,models:false,limits:false,classify:true,overwrite_exceptions:false});
  const rememberCandidate=(node,candidate)=>{if(typeof candidate?.sessionId==='string')node.candidateSessionId=candidate.sessionId;if(typeof candidate?.trace==='string'&&/^[a-zA-Z0-9-]{1,100}$/.test(candidate.trace)){node.candidateTrace=candidate.trace;if(!node.transports.some(row=>row.trace===candidate.trace))node.transports.push({scope:node.scope,trace:candidate.trace,settled:false});}};
  const readCoverageCandidate=node=>store.get("SELECT s.* FROM harbor.sessions s WHERE s.id=$1 AND s.client_session_id=$2 AND EXISTS(SELECT 1 FROM harbor.requests r JOIN harbor.recordings c ON c.request_id=r.id WHERE r.session_id=s.id AND r.status='ok' AND r.device_id IS NOT NULL AND c.persisted_sequence>0)",node.candidateSessionId,node.target.threadId);
  async function preparedDescriptor(binding,target,{actor,scope,signal:requestSignal,cleanupCapability},node){
    const callPrepared=async(path,options)=>{node.preparationStarted=true;const transport={scope,trace:null,settled:false};node.transports.push(transport);const result=await call(path,options);transport.trace=result.trace;node.trace=result.trace;if(result.error==='client_cancelled'&&requestSignal?.aborted&&!httpFailure(result.status))requestSignal.throwIfAborted();return result;};
    const id=binding.id;
    if(extraOperations.has(id)){node.extraRefreshLease=extraRefresh.create({accountId:target.id,cleanupCapability,recordTransport:row=>node.transports.push(row)});return {...await callExtra(()=>extraRefresh.prepare(node.extraRefreshLease,{operationId:id,signal:requestSignal})),extraRefreshLease:node.extraRefreshLease};}
    try{const variant=await variantAdapter.prepare(binding,node.group.variant,{target,signal:requestSignal,callPrepared:(path,options)=>callPrepared(path,{...options,actor,signal:requestSignal})});if(variant)return variant;}catch(error){const code=getCoverageVariantFailure(error);if(code)failCoverage(code);throw error;}
    if(configurationWriters.has(id)){
      const writer=configurationWriters.get(id),body=await writer.payload(target);return {path:writer.pathFor(target),method:binding.method,body,verify:json=>writer.verify(target,body,json)};
    }
    if(id==='batch-preview')return {path:binding.path,method:'POST',body:await previewBody(target),verify:async json=>!!await store.get('SELECT id FROM harbor.member_config_batches WHERE id=$1 AND scope->\'member_ids\' @> $2::jsonb',json.id,JSON.stringify([target.memberId]))};
    if(['batch-submit','batch-status'].includes(id)){
      const preview=requireOk(await callPrepared('/api/admin/member-config-batches/preview',{method:'POST',body:await previewBody(target),actor,signal:requestSignal})),path='/api/admin/member-config-batches/'+preview.id;
      if(id==='batch-status'){
        node.job={kind:'batch',target:preview.id};node.preparationJobStarted=true;requireOk(await callPrepared(path+'/execute',{method:'POST',body:{},actor,signal:requestSignal}));await pollCapacityTask({submittedAt:performance.now(),signal:requestSignal,readStatus:({timeoutMs})=>callPrepared(path,{actor,signal:requestSignal,timeoutMs}),isTerminal:value=>value.status==='completed'});
        return {path,method:'GET',verify:async json=>json.id===preview.id&&json.status==='completed'&&Object.keys(json.counts??{}).every(key=>['success','unchanged'].includes(key))};
      }
      return {path:path+'/execute',method:'POST',body:{},expires:preview.expires,job:{kind:'batch',target:preview.id},verify:async()=>!!await store.get("SELECT id FROM harbor.background_jobs WHERE target=$1 AND kind='member.config'",preview.id)};
    }
    if(['export-submit','export-status'].includes(id)){
      const body={include_body:false,format:'jsonl',filters:{member:request.member_id,from:String(request.usage_at-(++coverageExportSequence)),to:String(request.usage_at+1)}};
      if(id==='export-submit')return {path:binding.path,method:'POST',body,submissionIntent:{kind:'export',query:body.filters,format:body.format,includeBody:body.include_body},verify:async(json,node)=>{node.job={kind:'export',target:json.id};return !!await store.get('SELECT id FROM harbor.recording_exports WHERE id=$1',json.id);}};
      node.submissionIntent={kind:'export',query:body.filters,format:body.format,includeBody:body.include_body};node.preparationJobStarted=true;const submitted=requireOk(await callPrepared('/api/admin/recordings/exports',{method:'POST',body,actor,signal:requestSignal})),path='/api/admin/recordings/exports/'+submitted.id;
      node.job={kind:'export',target:submitted.id};node.preparationJobStarted=true;const completed=await pollCapacityTask({submittedAt:performance.now(),signal:requestSignal,readStatus:({timeoutMs})=>callPrepared(path,{actor,signal:requestSignal,timeoutMs}),isTerminal:value=>value.status==='complete'});
      if(completed.rows<1)failCoverage('capacity_coverage_export_fixture_empty');
      return {path,method:'GET',verify:async json=>json.id===submitted.id&&json.status==='complete'&&json.rows===completed.rows&&json.download_ready===true};
    }
    if(id==='account-check-submit')return {path:binding.path,method:'POST',body:{},verify:async json=>json.ready===true&&(await store.get('SELECT state FROM harbor.accounts WHERE id=$1',apiAccount.id)).state==='ready'};
    if(id==='quota-refresh-submit')return {path:binding.path,method:'POST',body:{},verify:async json=>json.refreshed===true&&JSON.parse((await store.get('SELECT quota_json FROM harbor.accounts WHERE id=$1',account.id)).quota_json).plan==='synthetic-capacity-plan'};
    if(id.startsWith('handoff-')){
      if(!prepareCoverageHandoffCandidate)failCoverage('capacity_coverage_handoff_not_connected');
      node.candidatePending=true;const candidate=await prepareCoverageHandoffCandidate({scope,threadId:target.threadId,signal:requestSignal});rememberCandidate(node,candidate);if(!node.candidateTrace)failCoverage('capacity_coverage_handoff_candidate_settlement_not_connected');const session=await readCoverageCandidate(node);
      if(!session)failCoverage('capacity_coverage_handoff_fixture_invalid');node.candidatePending=false;const path='/api/admin/sessions/'+session.id+'/handoffs';const prior=await store.get('SELECT COALESCE(MAX(sequence),0) sequence FROM harbor.session_handoffs WHERE session_id=$1',session.id);
      const destination=await store.get("SELECT id FROM harbor.accounts WHERE id<>$1 AND state='ready' AND auth_mode='subscription' ORDER BY id LIMIT 1",session.account_id);if(!destination)failCoverage('capacity_coverage_handoff_destination_missing');
      if(id==='handoff-status'){
        node.submissionIntent={kind:'handoff',sessionId:session.id,afterSequence:prior.sequence,version:session.route_version,targetAccount:destination.id};node.preparationJobStarted=true;
        const submitted=requireOk(await callPrepared(path,{method:'POST',body:{account_id:destination.id,expected_route_version:session.route_version},actor,signal:requestSignal}));node.job={kind:'handoff',target:submitted.handoff_id};
        await pollCapacityTask({submittedAt:performance.now(),signal:requestSignal,readStatus:({timeoutMs})=>callPrepared(path,{actor,signal:requestSignal,timeoutMs}),isTerminal:value=>value.id===submitted.handoff_id&&value.status==='completed'});
        return {path,method:'GET',verify:async json=>json.id===submitted.handoff_id&&json.status==='completed'&&json.binding?.account_id===destination.id};
      }
      return {path,method:'POST',body:{account_id:destination.id,expected_route_version:session.route_version},submissionIntent:{kind:'handoff',sessionId:session.id,afterSequence:prior.sequence,version:session.route_version,targetAccount:destination.id},verify:async(json,node)=>{node.job={kind:'handoff',target:json.handoff_id};return !!await store.get('SELECT id FROM harbor.session_handoffs WHERE id=$1 AND session_id=$2',json.handoff_id,session.id);}};
    }
    let path=binding.path;
    if(id==='extra-account-read')path=extraPath(target);
    if(id==='extra-records-read')path=extraPath(target)+'/records';
    if(id==='employee-detail')path='/api/admin/members/'+target.memberId;
    if(id==='key-detail')path='/api/admin/members/'+target.memberId+'/keys';
    if(id==='device-detail')path='/api/admin/devices?member_id='+target.memberId;
    if(id==='type-default-read'){
      const current=await store.get('SELECT default_version FROM harbor.member_types WHERE id=$1',target.typeId);
      if(current.default_version===null){const writer=configurationWriters.get('type-default-save'),body=await writer.payload(target);requireOk(await callPrepared(writer.pathFor(target),{method:'PATCH',body,actor,signal:requestSignal}));}
      path='/api/admin/member-types/'+target.typeId+'/templates';
    }
    return {path,method:binding.method,employee:['self','models'].includes(id),verify:async json=>verifyRead(id,json,target)};
  }
  async function recoverSubmission(node){
    const intent=node.submissionIntent;if(!intent)return;
    const rows=intent.kind==='export'?await store.all(`SELECT id FROM harbor.recording_exports WHERE options->>'format'=$1 AND (options->>'include_body')::boolean=$2 AND options->'query'->>'member'=$3 AND (options->'query'->>'from')::bigint=$4 AND (options->'query'->>'to')::bigint=$5 LIMIT 2`,intent.format,intent.includeBody,intent.query.member,Number(intent.query.from),Number(intent.query.to)):
      await store.all("SELECT id FROM harbor.session_handoffs WHERE session_id=$1 AND sequence>$2 AND expected_route_version=$3 AND target_account_id=$4 AND origin='manual' ORDER BY sequence LIMIT 2",intent.sessionId,intent.afterSequence,intent.version,intent.targetAccount);
    if(rows.length>1||node.job&&rows.length!==1||node.job&&rows[0]?.id!==node.job.target)failCoverage('capacity_coverage_job_identity_unknown');
    node.job=rows.length?{kind:intent.kind,target:rows[0].id}:null;node.submissionIntent=null;
  }
  async function settleJob(node){
    if(node.extraRefreshLease){try{await callExtra(()=>extraRefresh.release(node.extraRefreshLease));}finally{node.jobTerminal=extraRefresh.status(node.extraRefreshLease).released;}return;}
    if(!node.job||!node.started&&!node.preparationJobStarted)return;const deadline=performance.now()+10000;
    for(;;){
      const job=node.job,row=job.kind==='batch'?await store.get('SELECT status FROM harbor.member_config_batches WHERE id=$1',job.target):job.kind==='export'?await store.get('SELECT status FROM harbor.recording_exports WHERE id=$1',job.target):await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',job.target);
      if(!row)failCoverage('capacity_coverage_job_missing');if(job.kind==='batch'&&row.status==='preview')return;if(['completed','complete'].includes(row.status)){
        node.jobTerminal=true;
        if(job.kind==='batch'){
          const item=await store.get('SELECT status,desired_config,after_config FROM harbor.member_config_batch_items WHERE batch_id=$1 AND member_id=$2',job.target,node.target.memberId),actual=await store.get('SELECT usage_type_id,usage_type_version,defaults_version,usage_limits_version FROM harbor.members WHERE id=$1',node.target.memberId);
          if(!item||!['success','unchanged'].includes(item.status)||item.desired_config.usage_type_id!==node.target.typeId||item.after_config?.usage_type_id!==actual?.usage_type_id||['usage_type_version','defaults_version','usage_limits_version'].some(field=>item.after_config?.[field]!==actual?.[field]))failCoverage('capacity_coverage_batch_effect_mismatch');
        }else if(job.kind==='export'){
          const exported=await store.get('SELECT rows,options,visibility::text visibility FROM harbor.recording_exports WHERE id=$1',job.target),query=exported.options.query,expected=await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE member_id=$1 AND usage_at>=$2 AND usage_at<$3 AND pg_visible_in_snapshot(inserted_xid,$4::pg_snapshot)',query.member,query.from,query.to,exported.visibility);
          if(exported.rows<1||exported.rows!==expected.n)failCoverage('capacity_coverage_export_amount_mismatch');
        }else{
          const binding=await store.get('SELECT h.target_account_id,h.applied_route_version,s.account_id,s.route_version FROM harbor.session_handoffs h JOIN harbor.sessions s ON s.id=h.session_id WHERE h.id=$1',job.target);
          if(!binding||binding.target_account_id!==binding.account_id||binding.applied_route_version!==binding.route_version)failCoverage('capacity_coverage_handoff_effect_mismatch');
        }
        return;
      }if(['failed','cancelled','expired','skipped','superseded'].includes(row.status)){node.jobTerminal=true;failCoverage('capacity_coverage_job_failed');}
      if(performance.now()>=deadline)failCoverage('capacity_coverage_job_still_active');await delay(20);
    }
  }
  function releaseNode(handle,node){
    if(node.state==='released')return Promise.resolve();if(node.releasePromise)return node.releasePromise;
    // Close consumption synchronously, before any await can race executePrepared.
    node.state='releasing';
    const operation=(async()=>{
      let terminalError;
      if(node.executing)await node.done;
      if(node.candidatePending){if((!node.candidateSessionId||!node.candidateTrace)&&settleCoverageHandoffCandidate){const candidate=await settleCoverageHandoffCandidate({scope:node.scope,threadId:node.target.threadId});rememberCandidate(node,candidate);if(candidate?.settled===true&&!candidate.sessionId){node.candidatePending=false;node.target.threadId=randomUUID();}}if(node.candidatePending){if(node.candidateTrace&&typeof node.candidateSessionId==='string'&&await readCoverageCandidate(node))node.candidatePending=false;else failCoverage('capacity_coverage_handoff_candidate_settlement_not_connected');}}
      for(const transport of node.transports)await requireTransportWitness(transport);
      await recoverSubmission(node);
      try{await settleJob(node);}catch(error){if(!node.jobTerminal)throw error;terminalError=error;}
      if(node.target)busyConfigurationTargets.delete(node.target);node.state='released';activeCoverageTokens.delete(handle);pendingCoverageCleanup.delete(handle);
      if(terminalError)throw terminalError;
    })();
    node.releasePromise=operation;
    operation.catch(()=>{if(node.state!=='released'){node.state='cleanup_pending';pendingCoverageCleanup.add(handle);}}).finally(()=>{node.releasePromise=null;}).catch(()=>{});
    return operation;
  }
  async function releasePrepared(handles){
    if(!Array.isArray(handles))failCoverage('capacity_coverage_handles_invalid');let error;
    for(const handle of handles){const node=coverageHandles.get(handle);if(!node||node.state==='released')continue;try{await releaseNode(handle,node);}catch(cause){if(!error||pendingReason(error)&&!pendingReason(cause))error=cause;}}
    if(error)throw error;
  }
  async function prepareCoverageBatch(group,count,{actor=adminActors[0],scope,signal:requestSignal=signal,cleanupCapability}={}){
    const binding=bindings.get(group?.operationId),purposes=group?.purposes??[];
    if(!binding||!variantAdapter.variants(binding.id).includes(group.variant)||group.canonicalHttp!==binding.canonicalHttp||group.method!==binding.method||!purposes.length||purposes.some(id=>!bindings.has(id)||bindings.get(id).canonicalHttp!==binding.canonicalHttp||!variantAdapter.variants(id).includes(group.variant))||binding.method!=='GET'&&purposes.some(id=>id!==binding.id))failCoverage('capacity_coverage_group_invalid');
    const spec=metadata().find(row=>row.operationId===binding.id&&row.variant===group.variant);if(!spec.available)failCoverage('capacity_coverage_handoff_not_connected');
    if(!Number.isInteger(count)||count<1||count>spec.batchCapacity||preparingCoverage)failCoverage('capacity_coverage_preparation_busy');
    // Own preparation before cleanup can await or alter another batch's nodes.
    preparingCoverage=true;const prepared=[],reserved=[];
    try{
      await recoverDirectRefreshes();
      if(pendingCoverageCleanup.size)await releasePrepared([...pendingCoverageCleanup]);
      const expired=[...activeCoverageTokens].filter(handle=>{const node=coverageHandles.get(handle);return node.state==='prepared'&&coverageClock()>=node.expires;});if(expired.length)await releasePrepared(expired);
      if(activeCoverageTokens.size)failCoverage('capacity_coverage_preparation_busy');
      for(let index=0;index<count;index++){
        requestSignal?.throwIfAborted();let target=null;
        if(binding.method!=='GET'||purposes.some(id=>leasedRead.has(id))||binding.id==='device-list'&&group.variant==='filtered-sorted'){
          target=binding.id==='account-check-submit'?coverageAccountCheck:['quota-refresh-submit','extra-refresh-submit','extra-refresh-status'].includes(binding.id)?coverageQuota:(await coveragePool(requestSignal,actor,scope)).find(row=>!busyConfigurationTargets.has(row));
          if(!target||busyConfigurationTargets.has(target))failCoverage('capacity_coverage_pool_busy');for(const transport of target.creationTransports??[])await requireTransportWitness(transport);busyConfigurationTargets.add(target);reserved.push(target);
        }
        const handle=Object.freeze({id:randomUUID()}),node={binding,group:{...group,purposes:[...purposes]},target,state:'preparing',scope,started:false,transports:[...(target?.creationTransports??[])]};
        coverageHandles.set(handle,node);activeCoverageTokens.add(handle);prepared.push(handle);const descriptor=await preparedDescriptor(binding,target,{actor,scope,signal:requestSignal,cleanupCapability},node);node.descriptor=descriptor;node.job=descriptor.job??node.job??null;
      }
      const publishedAt=coverageClock();for(const handle of prepared){const node=coverageHandles.get(handle);node.state='prepared';node.expires=Math.min(publishedAt+coverageHandleTtlMs,node.descriptor.expires??Infinity);}
      const leaseNodes=prepared.map(handle=>coverageHandles.get(handle));preparedCoverageLeases.set(prepared,()=>Object.freeze({remainingMs:Math.min(...leaseNodes.map(node=>node.expires))-coverageClock(),count:leaseNodes.length}));return prepared;
    }catch(error){let cleanupError;try{await releasePrepared(prepared);}catch(cause){cleanupError=cause;}for(const target of reserved)if(!prepared.some(handle=>coverageHandles.get(handle)?.target===target))busyConfigurationTargets.delete(target);const primaryCancelled=requestSignal?.aborted&&(error===requestSignal.reason||error?.name==='AbortError'||error?.cause===requestSignal.reason);throw cleanupError&&(!pendingReason(cleanupError)||pendingReason(error)||primaryCancelled)?cleanupError:error;}finally{preparingCoverage=false;}
  }
  async function executePrepared(handle,{actor=adminActors[0],scope,signal:requestSignal=signal}={}){
    const node=coverageHandles.get(handle);if(!node||node.state!=='prepared')failCoverage('capacity_coverage_handle_invalid');
    if(coverageClock()>=node.expires){await releasePrepared([handle]);failCoverage('capacity_coverage_handle_expired');}
    requestSignal?.throwIfAborted();node.state='executing';node.executing=true;node.scope=scope;node.done=new Promise(resolve=>{node.resolve=resolve;});
    try{
      const descriptor=node.descriptor;if(descriptor.extraRefreshLease){node.started=true;const result=await callExtra(()=>extraRefresh.execute(descriptor.extraRefreshLease,{actor,scope,signal:requestSignal}));return {...result,operation:node.binding.id,method:descriptor.method,variant:node.group.variant};}if(descriptor.submissionIntent)node.submissionIntent=descriptor.submissionIntent;node.started=true;const transport={scope,trace:null,settled:false};node.transports.push(transport);
      const result=await call(descriptor.path,{method:descriptor.method,body:descriptor.body,actor:descriptor.employee?device:actor,employee:descriptor.employee,signal:requestSignal});node.trace=result.trace;transport.trace=result.trace;
      let verified=result.success&&await descriptor.verify(result.json,node);if(verified&&descriptor.method==='GET'&&!['batch-status','export-status','handoff-status'].includes(node.binding.id))for(const purpose of node.group.purposes)verified=verified&&await verifyRead(purpose,result.json,node.target);return {...result,operation:node.binding.id,method:descriptor.method,variant:node.group.variant,businessVerified:verified===true};
    }finally{node.executing=false;if(node.state==='executing')node.state='consumed';node.resolve();}
  }
  const qualifyAsync=async()=>{
    const [batch,exported,handoff]=await Promise.all([
      pollCapacityTask({submittedAt:batchSubmittedAt,signal,readStatus:({timeoutMs})=>call('/api/admin/member-config-batches/'+lastBatch,{timeoutMs}),isTerminal:value=>value.status==='completed'}),
      pollCapacityTask({submittedAt:exportSubmittedAt,signal,readStatus:({timeoutMs})=>call('/api/admin/recordings/exports/'+lastExport,{timeoutMs}),isTerminal:value=>value.status==='complete'}),
      pollCapacityTask({submittedAt:handoffSubmittedAt,signal,readStatus:({timeoutMs})=>call('/api/admin/sessions/'+handoffSession.id+'/handoffs',{timeoutMs}),isTerminal:value=>value.status==='completed'})
    ]);if(Object.keys(batch.counts).some(status=>!['success','unchanged'].includes(status))||!await store.get('SELECT id FROM harbor.members WHERE id=$1 AND usage_type_id=$2',memberId,type.id))throw new Error('capacity_batch_effect_mismatch');
    const source=await store.get('SELECT options,visibility::text visibility FROM harbor.recording_exports WHERE id=$1',lastExport),query=source.options.query,expected=await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE member_id=$1 AND usage_at>=$2 AND usage_at<$3 AND pg_visible_in_snapshot(inserted_xid,$4::pg_snapshot)',device.memberId,query.from,query.to,source.visibility);if(exported.rows!==expected.n||!exported.download_ready||exported.rows<1)throw new Error('capacity_export_amount_mismatch');
    const bound=await store.get('SELECT s.account_id,h.target_account_id,h.status FROM harbor.sessions s JOIN harbor.session_handoffs h ON h.session_id=s.id WHERE h.id=$1',lastHandoff);if(handoff.id!==lastHandoff||bound.account_id!==bound.target_account_id||bound.status!=='completed')throw new Error('capacity_handoff_effect_mismatch');
    return {passed:true,batchMembers:batch.members??null,exportRows:exported.rows,handoffApplied:true};
  };
  return {bindings,execute,coverageInventory,coverageVariantEvidence:variantAdapter.evidence,prepareCoverageBatch,executePrepared,releasePrepared,qualifyAsync,fixture:{memberId,keyId:key.id,typeId:type.id,configurationTargets:configurationTargets.map(row=>({...row}))}};
}
