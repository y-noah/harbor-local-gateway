import test from 'node:test';
import assert from 'node:assert/strict';
import {registerHooks} from 'node:module';
import {randomUUID} from 'node:crypto';
import {apiCoveragePlan,runApiCoveragePhase,getApiCoverageFailureReport} from '../tools/capacity/api-coverage.mjs';
import {createCapacityCoverageFactory,getCoverageFactoryPendingReason} from '../tools/capacity/coverage-factory.mjs';

// These tests isolate HTTP/DB failure boundaries only. Actual protocol and
// persistence remain covered by capacity-prepared-inventory.test.mjs.
async function fixture({handoff=false,candidate=null,settleCandidate=null,clock=null,coverageCount=1,ttlMs=60000,coverageVariants='steady'}={}){
 const entry=new URL('../tools/capacity/inventory-adapter.mjs?lifecycle='+randomUUID(),import.meta.url).href,state={sequence:100,members:new Map(),types:new Map(),batches:new Map(),exports:new Map(),handoffs:new Map(),sessions:new Map(),dbFailed:false,transportWitness:{passed:true,settled:true},httpCalls:0,verifyGate:null};
 const id=()=>`00000000-0000-4000-8000-${String(++state.sequence).padStart(12,'0')}`;
 const ok=json=>({success:true,complete:true,status:200,httpEnded:true,wireSent:true,durationMs:1,json,trace:randomUUID()});
 const baseHttp=async({path,body,method='GET'})=>{
  state.httpCalls++;
  if(path==='/api/admin/members'&&method==='POST'){const memberId=id();state.latestMember=memberId;state.members.set(memberId,{name:body.name,usage_type_id:null,usage_type_version:0,defaults_version:0,usage_limits_version:0});return ok({memberId});}
  if(path.match(/^\/api\/admin\/sessions\/[^/]+\/handoffs$/)){const sessionId=path.split('/').at(-2);if(method==='POST'){const jobId=id(),session=state.sessions.get(sessionId);state.handoffs.set(jobId,{id:jobId,session_id:sessionId,status:'running',sequence:1,expected_route_version:body.expected_route_version,target_account_id:body.account_id});return ok({handoff_id:jobId});}const job=[...state.handoffs.values()].find(row=>row.session_id===sessionId);return ok({...job,binding:{account_id:state.sessions.get(sessionId).account_id}});}
  if(path==='/api/admin/accounts')return ok({id:id()});if(path==='/api/admin/member-types')return ok({types:[{id:'usage-f'}]});
  if(path.startsWith('/api/admin/members/')&&method==='PATCH'){state.members.get(path.split('/').at(-1)).name=body.name;return ok({});}
  if(path.endsWith('/preview')){const batchId=id();state.batches.set(batchId,{id:batchId,status:'preview',memberId:body.scope.member_ids[0],typeId:body.type_id});return ok({id:batchId,expires:Date.now()+60000});}
  if(path.endsWith('/execute')){const batch=state.batches.get(path.split('/').at(-2));batch.status='queued';return ok({id:batch.id});}
  if(path.startsWith('/api/admin/member-config-batches/'))return ok({...state.batches.get(path.split('/').at(-1)),counts:{success:1}});
  if(path==='/api/admin/recordings/exports'&&method==='POST'){const exportId='recording.export:'+id(),query={...body.filters,from:Number(body.filters.from),to:Number(body.filters.to)};state.exports.set(exportId,{id:exportId,status:'running',rows:1,options:{query,format:body.format,include_body:body.include_body},visibility:'synthetic'});return ok({id:exportId});}
  return ok({});
 };
 state.http=baseHttp;
 const baseGet=async(sql,...args)=>{
  if(state.dbFailed&&sql.includes('SELECT status FROM'))throw Error('synthetic-db-unavailable');
  if(sql.startsWith('SELECT id FROM harbor.keys'))return {id:id()};if(sql.startsWith('SELECT r.id'))return {id:id(),session_id:id(),member_id:id(),usage_at:Date.now()-1000};if(sql.includes("SELECT id FROM harbor.accounts"))return {id:id()};if(sql.startsWith('SELECT csr'))return {csr:'synthetic-csr',public_key_sha256:'synthetic-hash'};if(sql.startsWith('SELECT s.id'))return {id:id()};
  if(sql.startsWith('SELECT s.*'))return state.sessions.get(args[0]);
  if(sql.startsWith('SELECT COALESCE(MAX(sequence)'))return {sequence:0};
  if(sql==='SELECT status FROM harbor.session_handoffs WHERE id=$1')return {status:state.handoffs.get(args[0]).status};
  if(sql.startsWith('SELECT h.target_account_id')){const job=state.handoffs.get(args[0]),session=state.sessions.get(job.session_id);return {...job,...session};}
  if(sql==='SELECT version FROM harbor.member_types WHERE id=$1')return {version:0};
  if(sql==='SELECT name FROM harbor.members WHERE id=$1'){await state.verifyGate;return {name:state.members.get(args[0]).name};}
  if(sql==='SELECT status FROM harbor.member_config_batches WHERE id=$1')return {status:state.batches.get(args[0]).status};
  if(sql==='SELECT status,desired_config,after_config FROM harbor.member_config_batch_items WHERE batch_id=$1 AND member_id=$2'){const batch=state.batches.get(args[0]),member=state.members.get(batch.memberId);return {status:'success',desired_config:{usage_type_id:batch.typeId},after_config:{...member}};}
  if(sql==='SELECT usage_type_id,usage_type_version,defaults_version,usage_limits_version FROM harbor.members WHERE id=$1')return {...state.members.get(args[0])};
  if(sql==='SELECT id FROM harbor.recording_exports WHERE id=$1')return state.exports.get(args[0])?{id:args[0]}:null;
  if(sql==='SELECT status FROM harbor.recording_exports WHERE id=$1')return {status:state.exports.get(args[0]).status};
  if(sql==='SELECT rows,options,visibility::text visibility FROM harbor.recording_exports WHERE id=$1')return {...state.exports.get(args[0])};
  if(sql.startsWith('SELECT COUNT(*) n FROM harbor.requests'))return {n:1};throw Error('unimplemented synthetic DB boundary');
 };
 state.get=baseGet;
 const store={get:(...args)=>state.get(...args),all:async(sql,...args)=>{if(sql.startsWith('SELECT id FROM harbor.members WHERE name=$1'))return [...state.members].filter(([,member])=>member.name===args[0]).map(([id])=>({id}));if(sql.startsWith('SELECT id FROM harbor.session_handoffs'))return [...state.handoffs.values()].filter(row=>row.session_id===args[0]&&row.sequence>args[1]&&row.expected_route_version===args[2]&&row.target_account_id===args[3]).map(row=>({id:row.id}));if(sql.startsWith('SELECT id FROM harbor.recording_exports'))return [...state.exports.values()].filter(row=>row.options.format===args[0]&&row.options.include_body===args[1]&&row.options.query.member===args[2]&&row.options.query.from===args[3]&&row.options.query.to===args[4]).map(row=>({id:row.id}));throw Error('unimplemented synthetic DB collection');},run:async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.member_types'))state.types.set(state.latestMember,args[0]);},transaction:async operation=>operation()};
 const key='__coverageBoundary'+randomUUID().replaceAll('-','');globalThis[key]=options=>state.http(options);
 const hooks=registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&specifier==='./http-client.mjs')return {url:'data:text/javascript,'+encodeURIComponent(`export const requestCapacityTarget=options=>globalThis['${key}'](options);`),shortCircuit:true};return next(specifier,context);}});
 const {createCapacityInventoryAdapter,getPendingCoverageReason,getPreparedCoverageLease,getCoverageInventoryFailureReason}=await import(entry);hooks.deregister();
 const inventory=await createCapacityInventoryAdapter({origin:'http://127.0.0.1:1',adminActors:[{headers:{}}],employeeActors:[{memberId:id(),deviceId:id()}],store,configurationTargetCount:1,coverageTargetCount:coverageCount,coverageHandleTtlMs:ttlMs,coverageVariants,coverageClock:clock?()=>clock.at:Date.now,settleCoverageHandoffCandidate:settleCandidate,getAdditionalPendingReason:getCoverageFactoryPendingReason,prepareCoverageHandoffCandidate:candidate??(handoff?async({threadId})=>{const sessionId=id();state.sessions.set(sessionId,{id:sessionId,client_session_id:threadId,account_id:id(),route_version:1});return {sessionId,trace:randomUUID()};}:null),awaitCoverageTransport:async()=>state.transportWitness});
 const spec=operationId=>inventory.coverageInventory().find(row=>row.operationId===operationId);
 const complete=()=>{for(const batch of state.batches.values()){batch.status='completed';const member=state.members.get(batch.memberId);member.usage_type_id=batch.typeId;member.usage_type_version=1;member.defaults_version=1;}for(const job of state.exports.values())job.status='complete';for(const job of state.handoffs.values()){job.status='completed';job.applied_route_version=2;Object.assign(state.sessions.get(job.session_id),{route_version:2,account_id:job.target_account_id});}};
 return {inventory,state,spec,baseHttp,baseGet,complete,getPendingCoverageReason,getPreparedCoverageLease,getCoverageInventoryFailureReason,close:()=>{delete globalThis[key];}};
}

test('release closes prepared consumption before its first asynchronous wait',async()=>{
 const f=await fixture();try{const [handle]=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),before=f.state.httpCalls,releasing=f.inventory.releasePrepared([handle]);await assert.rejects(f.inventory.executePrepared(handle),/handle_invalid/);await releasing;assert.equal(f.state.httpCalls,before);const next=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1);await f.inventory.releasePrepared(next);}finally{f.close();}
});

test('a response-lost export is tracked through its real unique submission intent until its job finishes',async()=>{
 const f=await fixture();try{const abort=new AbortController(),spec=f.spec('export-submit'),[handle]=await f.inventory.prepareCoverageBatch(spec,1);f.state.http=async options=>{const result=await f.baseHttp(options);if(options.path==='/api/admin/recordings/exports'){abort.abort();return {...result,success:false,httpEnded:false,status:null,error:'client_cancelled',json:undefined};}return result;};
  const result=await f.inventory.executePrepared(handle,{signal:abort.signal});assert.equal(result.error,'client_cancelled');f.state.dbFailed=true;await assert.rejects(f.inventory.releasePrepared([handle]),/synthetic-db-unavailable/);await assert.rejects(f.inventory.prepareCoverageBatch(spec,1),/synthetic-db-unavailable/);f.state.dbFailed=false;f.complete();f.state.http=f.baseHttp;const next=await f.inventory.prepareCoverageBatch(spec,1);assert.equal(next.length,1);await f.inventory.releasePrepared(next);
 }finally{f.close();}
});

test('an unpublished partially prepared handle can recover after DB failure and task completion',async()=>{
 const f=await fixture();try{const abort=new AbortController(),spec=f.spec('batch-status');f.state.http=async options=>{const result=await f.baseHttp(options);if(options.path.endsWith('/execute')){f.state.dbFailed=true;abort.abort();}return result;};await assert.rejects(f.inventory.prepareCoverageBatch(spec,1,{signal:abort.signal}),/synthetic-db-unavailable/);f.state.dbFailed=false;f.complete();f.state.http=f.baseHttp;const next=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1);assert.equal(next.length,1);await f.inventory.releasePrepared(next);}finally{f.close();}
});

test('two concurrent release calls share the executing operation and cannot free a blocked verifier',async()=>{
 const f=await fixture();let unblock;try{const [handle]=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1);f.state.verifyGate=new Promise(resolve=>unblock=resolve);const executing=f.inventory.executePrepared(handle);await Promise.resolve();let finished=0;const a=f.inventory.releasePrepared([handle]).then(()=>finished++),b=f.inventory.releasePrepared([handle]).then(()=>finished++);await Promise.resolve();assert.equal(finished,0);await assert.rejects(f.inventory.executePrepared(handle),/handle_invalid/);await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),/preparation_busy/);unblock();assert.equal((await executing).businessVerified,true);await Promise.all([a,b]);assert.equal(finished,2);const next=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1);await f.inventory.releasePrepared(next);}finally{unblock?.();f.close();}
});

test('ambiguous submission identities preserve the lease instead of picking an arbitrary job',async()=>{
 const f=await fixture();try{const abort=new AbortController(),spec=f.spec('export-submit'),[handle]=await f.inventory.prepareCoverageBatch(spec,1);f.state.http=async options=>{const result=await f.baseHttp(options);if(options.path==='/api/admin/recordings/exports'){const original=[...f.state.exports.values()].at(-1);f.state.exports.set('duplicate-job',{...original,id:'duplicate-job'});abort.abort();return {...result,status:null,success:false,httpEnded:false,error:'client_cancelled',json:undefined};}return result;};await f.inventory.executePrepared(handle,{signal:abort.signal});await assert.rejects(f.inventory.releasePrepared([handle]),/job_identity_unknown/);await assert.rejects(f.inventory.prepareCoverageBatch(spec,1),/job_identity_unknown/);}finally{f.close();}
});


test('response-lost handoff status preparation recovers the actual job identity before lease reuse',async()=>{
 const f=await fixture({handoff:true});try{const abort=new AbortController();f.state.http=async options=>{const result=await f.baseHttp(options);if(options.method==='POST'&&options.path.endsWith('/handoffs')){abort.abort();f.complete();return {...result,status:null,success:false,httpEnded:false,error:'client_cancelled',json:undefined};}return result;};
  await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('handoff-status'),1,{signal:abort.signal}),error=>error===abort.signal.reason);
  f.state.http=f.baseHttp;const next=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1);assert.equal(next.length,1);await f.inventory.releasePrepared(next);
 }finally{f.close();}
});

test('only native pending errors expose an incomplete reason, never arbitrary error text',async()=>{
 const f=await fixture();try{assert.equal(f.getPendingCoverageReason(Error('capacity_coverage_job_identity_unknown')),undefined);
 const spec=f.spec('export-submit'),[handle]=await f.inventory.prepareCoverageBatch(spec,1);f.state.http=async options=>{const result=await f.baseHttp(options);if(options.path==='/api/admin/recordings/exports'){const original=[...f.state.exports.values()].at(-1);f.state.exports.set('duplicate-job',{...original,id:'duplicate-job'});}return result;};
 await f.inventory.executePrepared(handle);await assert.rejects(f.inventory.releasePrepared([handle]),error=>{assert.equal(f.getPendingCoverageReason(error),'capacity_coverage_job_identity_unknown');return true;});
 }finally{f.close();}
});


test('response-lost export status preparation retains its running task until database recovery and completion',async()=>{
 const f=await fixture();try{const abort=new AbortController();f.state.http=async options=>{const result=await f.baseHttp(options);if(options.method==='POST'&&options.path==='/api/admin/recordings/exports'){abort.abort();f.state.dbFailed=true;return {...result,status:null,success:false,httpEnded:false,error:'client_cancelled',json:undefined};}return result;};
 await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('export-status'),1,{signal:abort.signal}),/synthetic-db-unavailable/);
 await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),/synthetic-db-unavailable/);
 f.state.dbFailed=false;f.complete();f.state.http=f.baseHttp;const next=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1);assert.equal(next.length,1);await f.inventory.releasePrepared(next);
 }finally{f.close();}
});


test('an interrupted handoff candidate does not silently return its private object lease',async()=>{
 const abort=new AbortController(),f=await fixture({candidate:async()=>{abort.abort();throw abort.signal.reason;}});try{
 await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('handoff-status'),1,{signal:abort.signal}),error=>{assert.equal(f.getPendingCoverageReason(error),'capacity_coverage_handoff_candidate_settlement_not_connected');return true;});
 await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),/handoff_candidate_settlement_not_connected/);
 }finally{f.close();}
});


test('known candidate failure remains false-worthy even when cleanup lacks settlement evidence',async()=>{
 const failure=Error('synthetic-candidate-known-failure'),f=await fixture({candidate:async()=>{throw failure;}});try{
 await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('handoff-status'),1),error=>error===failure);
 await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),/handoff_candidate_settlement_not_connected/);
 }finally{f.close();}
});


test('a completed handoff candidate can recover its lease after a transient DB verification failure',async()=>{
 const f=await fixture({handoff:true});try{f.state.get=async(sql,...args)=>{if(sql.startsWith('SELECT s.*'))throw Error('synthetic-session-read-failed');return f.baseGet(sql,...args);};
 await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('handoff-submit'),1),/synthetic-session-read-failed/);
 f.state.get=f.baseGet;const next=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),1);assert.equal(next.length,1);await f.inventory.releasePrepared(next);
 }finally{f.close();}
});


test('private batch lease starts on publication after slow preparation and remains valid after consumer array mutation',async()=>{
 const clock={at:1000},f=await fixture({clock,coverageCount:64});try{f.state.http=async options=>{clock.at+=1000;return f.baseHttp(options);};const handles=await f.inventory.prepareCoverageBatch(f.spec('employee-save'),64);
 const lease=f.getPreparedCoverageLease(handles);assert.equal(lease.count,64);assert.equal(lease.remainingMs,60000);assert.equal(f.getPreparedCoverageLease([...handles]),undefined);
 const originals=[...handles];handles[0]=undefined;clock.at+=30000;assert.equal(f.getPreparedCoverageLease(handles).remainingMs,30000);await f.inventory.releasePrepared(originals);
 }finally{f.close();}
});


test('slow trim cleanup rechecks the real lease instead of dispatching expired handles',async()=>{
 const clock={at:1000},f=await fixture({clock,coverageCount:5,ttlMs:1000}),entry=new URL('../tools/capacity/api-coverage.mjs?lease='+randomUUID(),import.meta.url).href,key='__coverageLease'+randomUUID().replaceAll('-','');
 globalThis[key]={getPreparedCoverageLease:f.getPreparedCoverageLease,getPendingCoverageReason:f.getPendingCoverageReason,getCoverageInventoryFailureReason:f.getCoverageInventoryFailureReason};
 const hooks=registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&specifier==='./inventory-adapter.mjs')return {url:'data:text/javascript,'+encodeURIComponent(`export const getPreparedCoverageLease=value=>globalThis['${key}'].getPreparedCoverageLease(value);export const getPendingCoverageReason=value=>globalThis['${key}'].getPendingCoverageReason(value);export const getCoverageInventoryFailureReason=value=>globalThis['${key}'].getCoverageInventoryFailureReason(value);`),shortCircuit:true};return next(specifier,context);}});
 try{const {apiCoveragePlan,runApiCoveragePhase}=await import(entry);hooks.deregister();let executions=0,releases=0;const inventory=[f.spec('health')],plan=apiCoveragePlan({inventory,maxDurationMs:5000,probeQps:5,batchSize:5});
 const result={operation:'health',requestSignature:'GET /health',variant:'steady',method:'GET',status:200,success:true,complete:true,businessVerified:true,durationMs:1,wireSent:true,httpEnded:true};
 const report=await runApiCoveragePhase({plan,openScope:async()=>({sendModel:async()=>({...result,method:'POST'}),sendAdmin:async()=>result,onResult:async()=>{},waitForDataReadiness:async()=>({passed:true}),close:async()=>{},collect:async()=>({transport:{passed:true,counts:{wireStarted:0,httpNonSuccess:0},mix:{alignedToPlannedCounts:true}},dataReconciliation:{passed:true}})}),prepareBatch:async({group,count})=>{const handles=await f.inventory.prepareCoverageBatch(group,count);clock.at+=500;return handles;},executeHandle:async()=>{executions++;throw Error('expired handle must never dispatch');},releaseHandles:async({handles})=>{releases++;clock.at+=600;await f.inventory.releasePrepared(handles);}});
 assert.equal(executions,0);assert.ok(releases>=2);assert.equal(report.groups[0].failed,0);assert.ok(report.qualification.missing.some(code=>code.startsWith('api_coverage_prepared_lease_too_short:')));
 }finally{hooks.deregister();f.close();delete globalThis[key];}
});


test('concurrent prepare cannot sweep unpublished nodes while a later descriptor is blocked past TTL',async()=>{
 const clock={at:1000},f=await fixture({clock,coverageCount:2,ttlMs:100});let unblock;try{let enteredResolve,reads=0;const entered=new Promise(resolve=>enteredResolve=resolve),gate=new Promise(resolve=>unblock=resolve);
 f.state.get=async(sql,...args)=>{if(sql==='SELECT version FROM harbor.member_types WHERE id=$1'&&++reads===2){enteredResolve();await gate;}return f.baseGet(sql,...args);};
 const preparing=f.inventory.prepareCoverageBatch(f.spec('type-default-save'),2);await entered;clock.at+=101;const before=f.state.httpCalls;await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),/preparation_busy/);assert.equal(f.state.httpCalls,before);unblock();const handles=await preparing;assert.equal(f.getPreparedCoverageLease(handles).remainingMs,100);
 for(const handle of handles)assert.equal((await f.inventory.executePrepared(handle)).success,true);await f.inventory.releasePrepared(handles);
 }finally{unblock?.();f.close();}
});


test('release requires an explicit private settled witness and retries unknown proof without unlocking',async()=>{
 for(const witness of [undefined,{passed:null,settled:null},{passed:true,settled:null}]){const f=await fixture();try{const abort=new AbortController(),spec=f.spec('employee-save'),[handle]=await f.inventory.prepareCoverageBatch(spec,1);f.state.http=async options=>{const response=await f.baseHttp(options);abort.abort();return {...response,status:null,success:false,httpEnded:false,error:'client_cancelled'};};await f.inventory.executePrepared(handle,{signal:abort.signal});f.state.transportWitness=witness;await assert.rejects(f.inventory.releasePrepared([handle]),error=>f.getPendingCoverageReason(error)==='capacity_coverage_transport_settlement_incomplete');await assert.rejects(f.inventory.prepareCoverageBatch(spec,1),/settlement_incomplete/);f.state.transportWitness={passed:true,settled:true};f.state.http=f.baseHttp;const next=await f.inventory.prepareCoverageBatch(spec,1);await f.inventory.releasePrepared(next);
 }finally{f.close();}}
});

test('a private handler witness contradiction is a real failure and never an incomplete cleanup',async()=>{
 const f=await fixture();try{const abort=new AbortController(),spec=f.spec('employee-save'),[handle]=await f.inventory.prepareCoverageBatch(spec,1);f.state.http=async options=>{const response=await f.baseHttp(options);abort.abort();return {...response,status:null,success:false,httpEnded:false,error:'client_cancelled'};};await f.inventory.executePrepared(handle,{signal:abort.signal});f.state.transportWitness={passed:false,settled:null};await assert.rejects(f.inventory.releasePrepared([handle]),error=>{assert.match(error.message,/settlement_conflict/);assert.equal(f.getPendingCoverageReason(error),undefined);return true;});await assert.rejects(f.inventory.prepareCoverageBatch(spec,1),/settlement_conflict/);
 }finally{f.close();}
});


test('even a complete successful HTTP response cannot bypass unknown or false handler proof',async()=>{
 for(const passed of [null,false]){const f=await fixture();try{const spec=f.spec('employee-save'),[handle]=await f.inventory.prepareCoverageBatch(spec,1);assert.equal((await f.inventory.executePrepared(handle)).httpEnded,true);f.state.transportWitness={passed,settled:null};await assert.rejects(f.inventory.releasePrepared([handle]),passed===false?/settlement_conflict/:/settlement_incomplete/);await assert.rejects(f.inventory.prepareCoverageBatch(spec,1),passed===false?/settlement_conflict/:/settlement_incomplete/);f.state.transportWitness={passed:true,settled:true};const next=await f.inventory.prepareCoverageBatch(spec,1);await f.inventory.releasePrepared(next);}finally{f.close();}}
});


test('lazy pool uses the supplied private actor and waits for its creator handler without repeating HTTP',async()=>{
 const f=await fixture();try{const spec=f.spec('employee-save'),before=f.state.httpCalls,privateActor={headers:{cookie:'synthetic-private-actor'}};let used;f.state.http=async options=>{used=options.headers.cookie;return f.baseHttp(options);};f.state.transportWitness={passed:null,settled:null};await assert.rejects(f.inventory.prepareCoverageBatch(spec,1,{actor:privateActor}),/settlement_incomplete/);assert.equal(used,'synthetic-private-actor');assert.equal(f.state.httpCalls-before,1);
 f.state.transportWitness={passed:true,settled:true};const [handle]=await f.inventory.prepareCoverageBatch(spec,1,{actor:privateActor});assert.equal(f.state.httpCalls-before,1);f.state.transportWitness={passed:null,settled:null};await assert.rejects(f.inventory.releasePrepared([handle]),/settlement_incomplete/);f.state.transportWitness={passed:true,settled:true};await f.inventory.releasePrepared([handle]);assert.equal(f.state.httpCalls-before,1);
 }finally{f.close();}
});

test('cancelled lazy pool creation recovers the unique committed member after proof without another POST',async()=>{
 const f=await fixture();try{const spec=f.spec('employee-save'),before=f.state.httpCalls,abort=new AbortController();f.state.http=async options=>{const response=await f.baseHttp(options);if(options.path==='/api/admin/members'){abort.abort();return {...response,status:null,success:false,httpEnded:false,error:'client_cancelled',json:undefined};}return response;};await assert.rejects(f.inventory.prepareCoverageBatch(spec,1,{signal:abort.signal}),error=>error===abort.signal.reason);assert.equal(f.state.httpCalls-before,1);f.state.http=f.baseHttp;const handles=await f.inventory.prepareCoverageBatch(spec,1);assert.equal(f.state.httpCalls-before,1);await f.inventory.releasePrepared(handles);
 }finally{f.close();}
});


test('a definite candidate failure wins over privately branded factory pending cleanup',async()=>{
 const scope={id:'pending-error-owned',kind:'normal'},factory=createCapacityCoverageFactory({getBusiness:()=>({}),getAdminActors:()=>[],getCandidateActors:()=>[],getNormalActorCount:()=>2,prepareCandidates:async()=>{},ownsScope:value=>value===scope,runOwnedOperation:async(_scope,operation)=>operation(),validateOwnedOperation:()=>true,readHandlerWitness:()=>null,readTrace:()=>null,readTraceEvidence:()=>null,store:{get:async()=>null,all:async()=>[]}});let pending;try{await factory.settleCoverageHandoffCandidate({scope,threadId:randomUUID()});}catch(error){pending=error;}assert.equal(getCoverageFactoryPendingReason(pending),'capacity_coverage_candidate_settlement_incomplete');
 const known=Error('synthetic-known-candidate-failure'),f=await fixture({candidate:async()=>{throw known;},settleCandidate:async()=>{throw pending;}});try{await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('handoff-status'),1),error=>error===known);assert.equal(getCoverageFactoryPendingReason(known),undefined);await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),error=>error===pending);}finally{f.close();}
});


test('phase stays false when definite candidate failure meets factory pending cleanup',async()=>{
 const ownedScope={id:'pending-phase-owned',kind:'normal'},factory=createCapacityCoverageFactory({getBusiness:()=>({}),getAdminActors:()=>[],getCandidateActors:()=>[],getNormalActorCount:()=>2,prepareCandidates:async()=>{},ownsScope:value=>value===ownedScope,runOwnedOperation:async(_scope,operation)=>operation(),validateOwnedOperation:()=>true,readHandlerWitness:()=>null,readTrace:()=>null,readTraceEvidence:()=>null,store:{get:async()=>null,all:async()=>[]}});let pending;try{await factory.settleCoverageHandoffCandidate({scope:ownedScope,threadId:randomUUID()});}catch(error){pending=error;}
 const f=await fixture({candidate:async()=>{throw Error('synthetic-known-phase-failure');},settleCandidate:async()=>{throw pending;}});try{const plan=apiCoveragePlan({inventory:[f.spec('handoff-status')],maxDurationMs:1000}),result={operation:'model',method:'POST',variant:'steady',status:200,success:true,complete:true,businessVerified:true,durationMs:1,wireSent:true,httpEnded:true};await assert.rejects(runApiCoveragePhase({plan,openScope:async()=>({sendModel:async()=>result,sendAdmin:async()=>({...result,operation:'employee-list',method:'GET'}),onResult:async()=>{},waitForDataReadiness:async()=>({passed:true}),close:async()=>{},collect:async()=>({transport:{passed:true,counts:{wireStarted:0,httpNonSuccess:0},mix:{alignedToPlannedCounts:true}},dataReconciliation:{passed:true}})}),prepareBatch:({group,count})=>f.inventory.prepareCoverageBatch(group,count),executeHandle:async()=>result,releaseHandles:({handles})=>f.inventory.releasePrepared(handles)}),error=>{const report=getApiCoverageFailureReport(error);assert.equal(report.qualification.passed,false);assert.ok(report.qualification.failures.includes('api_coverage_execution_failed'));return true;});}finally{f.close();}
});


test('a known lazy-pool HTTP failure remains primary before unknown handler proof and on retry',async()=>{
 for(const status of [302,503]){const f=await fixture();try{const before=f.state.httpCalls;f.state.http=async options=>({...await f.baseHttp(options),status,success:false});f.state.transportWitness={passed:null,settled:null};let primary;await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),error=>{primary=error;assert.equal(f.getCoverageInventoryFailureReason(error),'capacity_coverage_pool_create_http_failed');assert.equal(f.getPendingCoverageReason(error),undefined);return true;});await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),error=>error===primary);assert.equal(f.state.httpCalls-before,1);f.state.transportWitness={passed:true,settled:true};await assert.rejects(f.inventory.prepareCoverageBatch(f.spec('employee-save'),1),error=>error===primary);assert.equal(f.state.httpCalls-before,1);
 }finally{f.close();}}
});

test('phase keeps lazy-pool 503 false when its handler witness is still unknown',async()=>{
 const f=await fixture(),entry=new URL('../tools/capacity/api-coverage.mjs?pool-failure='+randomUUID(),import.meta.url).href,key='__coveragePoolFailure'+randomUUID().replaceAll('-','');globalThis[key]=f;const hooks=registerHooks({resolve(specifier,context,next){if(context.parentURL===entry&&specifier==='./inventory-adapter.mjs')return {url:'data:text/javascript,'+encodeURIComponent(`export const getPreparedCoverageLease=value=>globalThis['${key}'].getPreparedCoverageLease(value);export const getPendingCoverageReason=value=>globalThis['${key}'].getPendingCoverageReason(value);export const getCoverageInventoryFailureReason=value=>globalThis['${key}'].getCoverageInventoryFailureReason(value);`),shortCircuit:true};return next(specifier,context);}});
 try{const {apiCoveragePlan,runApiCoveragePhase,getApiCoverageFailureReport}=await import(entry);hooks.deregister();f.state.http=async options=>({...await f.baseHttp(options),status:503,success:false});f.state.transportWitness={passed:null,settled:null};const plan=apiCoveragePlan({inventory:[f.spec('employee-save')],maxDurationMs:1000}),result={operation:'model',method:'POST',variant:'steady',status:200,success:true,complete:true,businessVerified:true,durationMs:1,wireSent:true,httpEnded:true};
 await assert.rejects(runApiCoveragePhase({plan,openScope:async()=>({sendModel:async()=>result,sendAdmin:async()=>({...result,operation:'employee-list',method:'GET'}),onResult:async()=>{},waitForDataReadiness:async()=>({passed:true}),close:async()=>{},collect:async()=>({transport:{passed:true,counts:{wireStarted:0,httpNonSuccess:0},mix:{alignedToPlannedCounts:true}},dataReconciliation:{passed:true}})}),prepareBatch:({group,count})=>f.inventory.prepareCoverageBatch(group,count),executeHandle:async()=>result,releaseHandles:({handles})=>f.inventory.releasePrepared(handles)}),error=>{const report=getApiCoverageFailureReport(error);assert.equal(report.qualification.passed,false);assert.ok(report.qualification.failures.includes('api_coverage_factory_failed:capacity_coverage_pool_create_http_failed'));return true;});
 }finally{hooks.deregister();f.close();delete globalThis[key];}
});


test('expanded deep preparation preserves definite page-contract failure versus legitimate data shortage',async()=>{
 for(const malformed of [true,false]){const f=await fixture({coverageVariants:'expanded'});try{const group=f.inventory.coverageInventory().find(row=>row.operationId==='request-list'&&row.variant==='deep');f.state.http=async options=>{const response=await f.baseHttp(options);return {...response,json:{rows:[],snapshot:randomUUID(),pagination:{page:1,pageSize:malformed?100:25,total:0,totalPages:1,nextCursor:null}}};};await assert.rejects(f.inventory.prepareCoverageBatch(group,1),error=>{assert.equal(f.getCoverageInventoryFailureReason(error),malformed?'capacity_coverage_variant_first_page_contract_failed':'capacity_coverage_variant_data_incomplete');assert.equal(f.getPendingCoverageReason(error),malformed?undefined:'capacity_coverage_variant_data_incomplete');return true;});}finally{f.close();}}
});
