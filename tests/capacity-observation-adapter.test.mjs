import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {randomUUID} from 'node:crypto';
import {setTimeout as delay} from 'node:timers/promises';
import {createCapacityObservationAdapter,observeCapacityHttp,isOwnedObservationScope,capacityObservedApiClass} from '../tools/capacity/observation-adapter.mjs';
import {credentialFingerprints,requestCapacityTarget} from '../tools/capacity/http-client.mjs';
import {capacityWireSignature} from '../tools/capacity/gateway-probe.mjs';

async function fixture(operation,configuration={}){
  let adapter;const rows=[],identities=[{memberId:randomUUID(),keyId:randomUUID(),deviceId:randomUUID()}],cookie='harbor_session=synthetic-observation-private',authorization='Bearer synthetic-observation-private-key';
  const server=http.createServer((req,res)=>{const wireId=randomUUID(),trace=req.headers['x-capacity-trace']??null,signature=capacityWireSignature(req.method,req.url);adapter.observeWire({kind:'gateway-request-start',trace,wireId,signature,credentials:credentialFingerprints(req.headers),at:Date.now()});req.resume();res.writeHead(200,{'content-type':'application/json'});res.end('{"ok":true}');res.once('finish',()=>adapter.observeWire({kind:'gateway-response-end',trace,wireId,signature,status:200,at:Date.now()}));});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  const send=(kind,event)=>{const options={origin,path:event.path??'/api/admin/status',headers:{...(kind==='model'?{authorization}:{cookie})},mode:'json'};return configuration.dispatch?observeCapacityHttp(options,configuration.dispatch):requestCapacityTarget(options);};
  adapter=createCapacityObservationAdapter({sendModel:event=>send('model',event),sendAdmin:async event=>{const result=await send('admin',event);event.afterFirst?.();if(event.hold)await event.hold;if(event.nested)return send('admin',{...event,path:'/ready'});return result;},employeeIdentities:identities,createLedger:async()=>({onResult:async row=>rows.push(row),collect:async()=>({passed:null,scope:'transport-only-qualification'})}),...configuration});
  try{await operation({adapter,origin,cookie,authorization,rows,identities});}finally{await adapter.close();server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}
}

test('normal and UI requests keep independent private wire counts and expose only credential fingerprints',async()=>fixture(async({adapter,cookie,authorization})=>{
  const normal=await adapter.createObservationScope({id:'normal-phase',kind:'normal'}),ui=await adapter.createObservationScope({id:'ui-phase',kind:'ui'});assert.equal(isOwnedObservationScope(normal),true);assert.equal(isOwnedObservationScope({...normal}),false);assert.ok(Object.isFrozen(normal));
  await Promise.all([normal.sendAdmin({actorIndex:0}),ui.sendAdmin({actorIndex:1})]);await delay(5);const a=await normal.collect(),b=await ui.collect();for(const result of [a,b]){assert.equal(result.transport.passed,true);assert.equal(result.transport.counts.planned,1);assert.equal(result.transport.counts.wireStarted,1);assert.equal(result.transport.counts.reconciled,1);assert.equal(result.transport.counts.unattributed,0);assert.equal(result.formalEvidence.passed,null);}
  const wire=ui.readWireEvents();assert.equal(wire.events.length,2);assert.equal(wire.events[0].credentials.cookie,credentialFingerprints({cookie}).cookie);assert.equal(JSON.stringify([a,b,wire]).includes(cookie),false);assert.equal(JSON.stringify([a,b,wire]).includes(authorization),false);
  await normal.close();await assert.rejects(normal.sendAdmin({actorIndex:0}),/scope_closed/);
}));

test('a real untraced gateway request is counted as unattributed instead of silently joining normal or UI traffic',async()=>fixture(async({adapter,origin,cookie})=>{
  const scope=await adapter.createObservationScope({id:'ui-extra',kind:'ui'});await scope.sendAdmin({actorIndex:0});await requestCapacityTarget({origin,path:'/health',headers:{cookie}});await delay(5);const result=await scope.collect();assert.equal(result.transport.passed,null);assert.equal(result.transport.counts.planned,1);assert.equal(result.transport.counts.unattributed,2);assert.equal(scope.readWireEvents().events.filter(row=>row.trace===null).length,2);
}));

test('nested auxiliary HTTP calls share a scope while preserving the actual top-level business invocation',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'nested',kind:'normal'});await scope.sendAdmin({actorIndex:2,nested:true});await delay(5);const report=await scope.collect();assert.equal(report.transport.passed,true);assert.equal(report.transport.counts.planned,2);assert.equal(report.transport.counts.reconciled,2);assert.deepEqual(scope.readWireEvents().events.filter(row=>row.kind==='gateway-request-start').map(row=>row.signature),['GET /api/admin/status','GET /ready']);
}));

test('raw headers/body DTOs and auth-changing observer headers are rejected before forwarding',async()=>fixture(async({adapter,cookie})=>{
  const scope=await adapter.createObservationScope({id:'privacy',kind:'ui',observer:{beforeRequest:async()=>({'x-capacity-trace':randomUUID(),authorization:cookie}),response:async()=>{},requestFinished:async()=>{},requestFailed:async()=>{}}});
  await assert.rejects(scope.observer.beforeRequest({requestId:randomUUID(),source:'browser',actorIndex:0,method:'GET',route:'/health',credentials:{cookie,authorization:null}}),/request_invalid/);
  await assert.rejects(scope.observer.beforeRequest({requestId:randomUUID(),source:'browser',actorIndex:0,method:'GET',route:'/health',credentials:{cookie:null,authorization:null},body:'must-not-forward'}),/request_invalid/);
  await assert.rejects(scope.sendAdmin({actorIndex:0}),/headers_invalid/);const report=await scope.collect();assert.equal(report.transport.counts.planned,0);assert.equal(JSON.stringify(report).includes(cookie),false);
}));

test('duplicate traces across scopes cannot be attributed to two traffic groups',async()=>fixture(async({adapter})=>{
  const fixed=randomUUID(),observer={beforeRequest:async()=>({'x-capacity-trace':fixed}),response:async()=>{},requestFinished:async()=>{},requestFailed:async()=>{}},normal=await adapter.createObservationScope({id:'one',kind:'normal',observer}),ui=await adapter.createObservationScope({id:'two',kind:'ui',observer});await normal.sendAdmin({actorIndex:0});await assert.rejects(ui.sendAdmin({actorIndex:0}),/trace_duplicate/);assert.equal((await ui.collect()).transport.passed,false);
}));

test('model/admin result callbacks use the existing ledger once and reject cross-scope or raw payload results',async()=>fixture(async({adapter,rows,cookie})=>{
  const a=await adapter.createObservationScope({id:'result-a',kind:'normal'}),b=await adapter.createObservationScope({id:'result-b',kind:'ui'}),response=await a.sendAdmin({actorIndex:0}),result={scenario:'mixed',kind:'admin',event:{sequence:0,actorIndex:0},result:{operation:'status',method:'GET',status:200,success:true,httpEnded:true,wireSent:true,validatedSuccess:true,trace:response.trace,error:null}};
  await assert.rejects(b.onResult(result),/result_unbound/);await a.onResult(result);assert.equal(rows.length,1);await assert.rejects(a.onResult(result),/result_duplicate/);assert.equal(rows.length,1);await assert.rejects(a.onResult({...result,result:{...result.result,json:{secret:cookie}}}),/result_invalid/);await assert.rejects(a.onResult({...result,result:{...result.result,status:{secret:cookie}}}),/result_invalid/);assert.equal(JSON.stringify(rows).includes(cookie),false);
}));

test('model ledger identities come from the captured actor binding rather than browser-supplied identity fields',async()=>fixture(async({adapter,rows,identities})=>{
  const scope=await adapter.createObservationScope({id:'model-identities',kind:'ui'}),response=await scope.sendModel({actorIndex:0});await scope.onResult({scenario:'mixed',kind:'model',event:{sequence:0,actorIndex:0},result:{operation:'model',method:'GET',status:200,success:true,complete:true,wireSent:true,httpEnded:true,validatedSuccess:true,trace:response.trace}});assert.equal(rows.length,1);assert.equal(rows[0].event.expectedMemberId,identities[0].memberId);assert.equal(rows[0].event.expectedKeyId,identities[0].keyId);assert.equal(rows[0].event.expectedDeviceId,identities[0].deviceId);
}));

test('actual API classes come from the final wire route rather than an operation label',async()=>fixture(async({adapter})=>{
  assert.equal(capacityObservedApiClass({method:'GET',route:'/api/admin/state'}),'summary');assert.equal(capacityObservedApiClass({method:'GET',route:'/api/admin/requests?view=all'}),'summary');assert.equal(capacityObservedApiClass({method:'GET',route:'/api/admin/requests?view=list'}),'list');
  const scope=await adapter.createObservationScope({id:'wire-mix',kind:'normal'});await scope.sendAdmin({actorIndex:0,category:'list',path:'/api/admin/state'});await delay(5);const report=await scope.collect();assert.equal(report.transport.mix.plannedByCategory.list,1);assert.equal(report.transport.mix.actualBaselineByClass.summary,1);assert.equal(report.transport.mix.alignedToPlannedCounts,false);assert.equal(report.formalEvidence.passed,null);
}));

test('scope close waits for already started business calls including their second auxiliary request',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'closing-work',kind:'ui'});let release,entered;const hold=new Promise(resolve=>{release=resolve;}),first=new Promise(resolve=>{entered=resolve;}),running=scope.sendAdmin({actorIndex:0,category:'list',nested:true,hold,afterFirst:entered});await first;let closed=false;const closing=scope.close().then(()=>{closed=true;});await Promise.resolve();assert.equal(closed,false);release();await running;await closing;const result=await scope.collect();assert.equal(result.transport.passed,true);assert.equal(result.transport.counts.planned,2);assert.equal(result.transport.mix.auxiliaryBaselineHttp,1);assert.equal(result.scope.closed,true);
}));

test('concurrent repeated request IDs are reserved before an asynchronous observer is awaited',async()=>fixture(async({adapter})=>{
  let release;const gate=new Promise(resolve=>{release=resolve;}),scope=await adapter.createObservationScope({id:'duplicate-reservation',kind:'ui',observer:{beforeRequest:async()=>{await gate;return {'x-capacity-trace':randomUUID()};},response:async()=>{},requestFinished:async()=>{},requestFailed:async()=>{}}}),dto={requestId:randomUUID(),source:'browser',actorIndex:0,method:'GET',route:'/health',credentials:{cookie:null,authorization:null}},first=scope.observer.beforeRequest(dto);await assert.rejects(scope.observer.beforeRequest(dto),/request_duplicate/);release();const headers=await first,wireId=randomUUID();adapter.observeWire({kind:'gateway-request-start',trace:headers['x-capacity-trace'],wireId,signature:'GET /health',credentials:{cookie:null,authorization:null},at:1});adapter.observeWire({kind:'gateway-response-end',trace:headers['x-capacity-trace'],wireId,signature:'GET /health',status:200,at:2});await scope.observer.response({requestId:dto.requestId,status:200});await scope.observer.requestFinished({requestId:dto.requestId});assert.equal((await scope.collect()).transport.counts.planned,1);assert.equal((await scope.collect()).transport.passed,false);
}));

test('exhausted cursor history stays visible instead of claiming complete wire coverage',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'wire-budget',kind:'ui'});for(let index=0;index<2;index++)await scope.sendAdmin({actorIndex:0});const cursor=scope.readWireEvents({cursor:0});assert.equal(cursor.dropped,2);assert.equal(cursor.events.length,2);assert.equal((await scope.collect()).transport.passed,null);
},{maxWireEvents:2}));

test('real wire authentication fingerprints must match the observer request fingerprint',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'wire-auth',kind:'normal'});await scope.sendAdmin({actorIndex:0,category:'list'});await delay(5);const result=await scope.collect();assert.equal(result.transport.passed,false);assert.ok(result.transport.issues.some(row=>row.code==='wire_credential_mismatch'));
},{dispatch:options=>requestCapacityTarget({...options,headers:{...options.headers,cookie:'harbor_session=synthetic-changed-after-observer'}})}));

test('collect returns a detached frozen snapshot while the live scope continues counting',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'snapshots',kind:'normal'}),initial=await scope.collect();assert.equal(initial.transport.counts.planned,0);await scope.sendAdmin({actorIndex:0,category:'list'});await delay(5);const first=await scope.collect();assert.equal(first.transport.counts.planned,1);await scope.sendAdmin({actorIndex:0,category:'summary',path:'/api/admin/state'});await delay(5);const second=await scope.collect();assert.equal(second.transport.counts.planned,2);assert.equal(first.transport.counts.planned,1);assert.equal(first.transport.mix.plannedByCategory.summary,undefined);assert.equal(second.transport.mix.actualHttpByClass.summary,1);assert.ok(Object.isFrozen(first.transport.mix.plannedByCategory));
}));

test('adapter close blocks every scope before waiting for an earlier scope to finish',async()=>fixture(async({adapter})=>{
  const a=await adapter.createObservationScope({id:'close-a',kind:'normal'}),b=await adapter.createObservationScope({id:'close-b',kind:'ui'});let release,entered;const hold=new Promise(resolve=>{release=resolve;}),first=new Promise(resolve=>{entered=resolve;}),running=a.sendAdmin({actorIndex:0,nested:true,hold,afterFirst:entered});await first;const closing=adapter.close();await assert.rejects(b.sendAdmin({actorIndex:0}),/scope_closed/);release();await running;await closing;assert.equal((await a.collect()).transport.counts.planned,2);assert.equal((await b.collect()).transport.counts.planned,0);
}));

test('close waits for ledger allocation and refuses to publish its newly allocated scope',async()=>{
  let release,entered,disposed=false;const gate=new Promise(resolve=>{release=resolve;}),started=new Promise(resolve=>{entered=resolve;}),adapter=createCapacityObservationAdapter({sendModel:async()=>{},sendAdmin:async()=>{},createLedger:async()=>{entered();await gate;return {onResult:async()=>{},collect:async()=>({passed:null}),close:async()=>{disposed=true;}};}}),creating=adapter.createObservationScope({id:'allocating',kind:'ui'});await started;let done=false;const closing=adapter.close().then(()=>{done=true;});await Promise.resolve();assert.equal(done,false);release();await assert.rejects(creating,/scope_closed/);await closing;assert.equal(disposed,true);
});

test('issued factory scopes observe real TLS/PG traffic, retain private actor bindings and reconcile rebound model traces',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
  const {createTarget,ownedCapacityTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target;
  try{
    target=await createTarget({profile:'selfcheck',employeeCount:1,binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});
    const trusted=ownedCapacityTarget(target),create=trusted.createObservationScope;assert.equal(typeof create,'function');target.createObservationScope=()=>{throw new Error('public replacement');};target.sendModel=()=>{throw new Error('public replacement');};Object.defineProperty(target,'events',{value:[]});
    const normal=await create({id:'normal-real',kind:'normal'}),ui=await create({id:'ui-real',kind:'ui'}),scene={id:'observed-real',employees:1,streamMs:30,payloadBytes:1024};await target.onScenarioStart({scene});
    target.employeeActors[0].origin='http://127.0.0.1:1';target.employeeActors[0].headers={authorization:'Bearer public-replacement'};target.adminActors[0].headers.cookie='harbor_session=public-replacement';const result=await normal.sendModel({actorIndex:0,sequence:0,slot:0,scene});assert.equal(result.success,true);const safe=Object.fromEntries(['operation','method','variant','status','success','complete','httpEnded','wireSent','startedAt','finishedAt','durationMs','receivedBytes','firstContentAt','headersAt','connectedAt','tlsAt','reusedSocket','bodyHash','records','trace','error'].filter(key=>Object.hasOwn(result,key)).map(key=>[key,result[key]]));
    await normal.onResult({scenario:scene.id,kind:'model',event:{actorIndex:0,sequence:0,slot:0},result:{...safe,validatedSuccess:true}});
    const admin=await ui.sendAdmin({actorIndex:0,category:'configuration'});assert.equal(admin.success,true);const written=await ui.sendAdmin({actorIndex:0,category:'configuration'});assert.equal(written.success,true);assert.equal(written.businessVerified,true);await normal.close();await ui.close();
    const readiness=await normal.waitForDataReadiness();assert.equal(readiness.passed,true,JSON.stringify(readiness));assert.equal(readiness.counts.ready,1);assert.equal(readiness.counts.pending,0);const deadline=performance.now()+10000;let a;for(;;){a=await normal.collect();if(a.dataReconciliation.passed===true)break;if(performance.now()>=deadline)assert.fail(JSON.stringify(a.dataReconciliation));await delay(20);}const b=await ui.collect();assert.equal(a.transport.passed,true);assert.equal(a.transport.counts.planned,1);assert.equal(a.transport.counts.modelResults,1);assert.equal(a.dataReconciliation.passed,true,JSON.stringify(a.dataReconciliation));assert.equal(b.transport.passed,true,JSON.stringify(b.transport));assert.equal(b.transport.counts.planned,2);assert.equal(b.transport.mix.plannedAdminCalls,2);assert.equal(b.transport.mix.auxiliaryBaselineHttp,0,'these two inventory configuration operations verify through DB rather than extra HTTP');assert.equal(b.transport.mix.actualHttpBySource.model,undefined);assert.equal(a.transport.counts.unattributed,0);assert.equal(b.transport.counts.unattributed,0);
    const wire=normal.readWireEvents().events;assert.ok(wire.some(row=>row.kind==='gateway-request-start'&&row.trace===result.trace));assert.ok(Object.isFrozen(trusted));
  }finally{await target?.close();}
});


test('collect cannot certify transport between a finished first HTTP and its pending auxiliary HTTP',async()=>fixture(async({adapter})=>{
  let release,entered;const gate=new Promise(resolve=>release=resolve),first=new Promise(resolve=>entered=resolve),scope=await adapter.createObservationScope({id:'pending-business',kind:'normal'}),running=scope.sendAdmin({actorIndex:0,category:'readiness',nested:true,hold:gate,afterFirst:entered});await first;const during=await scope.collect();assert.equal(during.transport.passed,null);assert.equal(during.transport.mix.alignedToPlannedCounts,null);assert.equal(during.transport.pendingInvocations,1);release();await running;await scope.close();const after=await scope.collect();assert.equal(after.transport.passed,true);assert.equal(after.transport.pendingInvocations,0);assert.equal(after.transport.mix.auxiliaryBaselineHttp,1);
}));

test('target close establishes the scope barrier before releasing actors and waits for its in-flight model',{skip:!process.env.HARBOR_CAPACITY_PG_BIN,timeout:120000},async()=>{
  const {createTarget,ownedCapacityTarget}=await import('../tools/capacity/gateway-adapter.mjs');let target;try{target=await createTarget({profile:'selfcheck',employeeCount:1,binDirectory:process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory:process.env.HARBOR_CAPACITY_PG_LIB});const scope=await ownedCapacityTarget(target).createObservationScope({id:'close-inflight',kind:'normal'}),scene={employees:1,streamMs:300,payloadBytes:1024};await target.onScenarioStart({scene});const running=scope.sendModel({actorIndex:0,sequence:0,slot:0,scene});await delay(20);const closing=target.close();await assert.rejects(scope.sendModel({actorIndex:0,sequence:1,scene}),/scope_closed/);assert.equal((await running).success,true);await closing;assert.throws(()=>ownedCapacityTarget(target),/target_not_owned/);}finally{await target?.close();}
});

test('a cancelled client before response headers is separate from the server terminal status',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'preheader-cancel',kind:'normal'});await scope.sendAdmin({actorIndex:0,category:'readiness'});await scope.close();const result=await scope.collect();assert.equal(result.transport.passed,true);assert.equal(result.transport.counts.cancelledBeforeResponse,1);assert.equal(result.transport.counts.clientCancelled,1);assert.equal(result.transport.counts.clientFinished,0);
},{dispatch:async options=>{const result=await requestCapacityTarget(options);return {...result,status:null,httpEnded:false,error:'client_cancelled'};}}));

test('a cancelled client with an observed conflicting response status still fails wire reconciliation',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'postheader-cancel',kind:'normal'});await scope.sendAdmin({actorIndex:0,category:'readiness'});await scope.close();const result=await scope.collect();assert.equal(result.transport.passed,false);assert.ok(result.transport.issues.some(row=>row.code==='wire_client_status_mismatch'));assert.equal(result.transport.counts.cancelledBeforeResponse,0);
},{dispatch:async options=>{const result=await requestCapacityTarget(options);return {...result,status:201,httpEnded:false,error:'client_cancelled'};}}));

test('an older readiness waiter cannot restore a cache invalidated by a new result',async()=>{
  let release,entered,received=0,hold=true;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);await fixture(async({adapter})=>{const scope=await adapter.createObservationScope({id:'readiness-generation',kind:'normal'}),submit=async sequence=>{const response=await scope.sendModel({actorIndex:0});await scope.onResult({scenario:'readiness-cache',kind:'model',event:{actorIndex:0,sequence},result:{operation:'model',method:'GET',trace:response.trace,validatedSuccess:true}});};await submit(0);const waiting=scope.waitForDataReadiness();await started;await submit(1);assert.equal((await scope.collect()).dataReadiness,null);const current=await scope.waitForDataReadiness();assert.equal(current.counts.total,2);release();assert.equal((await waiting).invalidated,true);assert.equal((await scope.collect()).dataReadiness.counts.total,2);},{createLedger:async()=>({onResult:async()=>received++,collect:async()=>({passed:null}),waitForDataReadiness:async()=>{const total=received;if(hold){hold=false;entered();await gate;}return {passed:true,counts:{total,ready:total}};}})});
});

test('a readiness cancellation after its delegated query cannot populate the scope cache',async()=>{
  let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);await fixture(async({adapter})=>{const scope=await adapter.createObservationScope({id:'readiness-cancel',kind:'normal'}),controller=new AbortController(),waiting=scope.waitForDataReadiness({signal:controller.signal});await started;controller.abort('custom reason');release();await assert.rejects(waiting,{name:'AbortError'});assert.equal((await scope.collect()).dataReadiness,null);},{createLedger:async()=>({onResult:async()=>{},collect:async()=>({passed:null}),waitForDataReadiness:async()=>{entered();await gate;return {passed:true,counts:{total:0,ready:0}};}})});
});

for(const lateWire of [false,true])test('confirmed unsent browser reservation closes without wire and '+(lateWire?'rejects a later contradictory arrival':'stays explicitly excluded'),async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'not-sent-'+lateWire,kind:'ui'}),requestId=randomUUID(),headers=await scope.observer.beforeRequest({requestId,source:'browser',actorIndex:0,method:'GET',route:'/api/admin/state',credentials:{cookie:null,authorization:null}});await scope.observer.requestFailed({requestId,cancelled:true,notSent:true});await scope.close();const settled=await scope.collect();assert.equal(settled.transport.passed,true);assert.equal(settled.transport.pending,0);assert.equal(settled.transport.counts.notSent,1);assert.equal(settled.transport.counts.clientCancelled,1);assert.equal(settled.transport.counts.reconciled,0);if(lateWire){adapter.observeWire({kind:'gateway-request-start',trace:headers['x-capacity-trace'],wireId:randomUUID(),signature:'GET /api/admin/state',credentials:{cookie:null,authorization:null}});const after=await scope.collect();assert.equal(after.transport.passed,false);assert.ok(after.transport.issues.some(row=>row.code==='wire_after_not_sent'));assert.equal(after.transport.counts.unattributed,0);}
}));

test('notSent cannot erase a request that has already reached the server',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'not-sent-conflict',kind:'ui'}),requestId=randomUUID(),headers=await scope.observer.beforeRequest({requestId,source:'browser',actorIndex:0,method:'GET',route:'/api/admin/state',credentials:{cookie:null,authorization:null}}),wireId=randomUUID();adapter.observeWire({kind:'gateway-request-start',trace:headers['x-capacity-trace'],wireId,signature:'GET /api/admin/state',credentials:{cookie:null,authorization:null}});await assert.rejects(scope.observer.requestFailed({requestId,cancelled:true,notSent:true}),/not_sent_conflict/);await scope.observer.requestFailed({requestId,cancelled:true});adapter.observeWire({kind:'gateway-response-abort',trace:headers['x-capacity-trace'],wireId,signature:'GET /api/admin/state',status:200});await scope.close();const result=await scope.collect();assert.equal(result.transport.passed,false);assert.equal(result.transport.counts.notSent,0);assert.equal(result.transport.pending,0);
}));

test('scope shutdown waits for its running data query before releasing the factory resources',async()=>{
  let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);await fixture(async({adapter})=>{const scope=await adapter.createObservationScope({id:'readiness-close',kind:'normal'}),waiting=scope.waitForDataReadiness();await started;let closed=false;const closing=adapter.close().then(()=>closed=true);await delay(5);assert.equal(closed,false);await assert.rejects(scope.waitForDataReadiness(),/scope_closed/);release();await waiting;await closing;assert.equal(closed,true);},{createLedger:async()=>({onResult:async()=>{},collect:async()=>({passed:null}),waitForDataReadiness:async()=>{entered();await gate;return {passed:null,counts:{total:0,ready:0}};}})});
});

test('scope close blocks newly started data queries while preserving reads after close',async()=>{
  let release,entered,queries=0;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);await fixture(async({adapter})=>{const scope=await adapter.createObservationScope({id:'scope-readiness-close',kind:'normal'}),first=scope.waitForDataReadiness();await started;let finished=false;const closing=scope.close().then(()=>finished=true);await delay(5);await assert.rejects(scope.waitForDataReadiness(),/scope_closed/);assert.equal(queries,1);assert.equal(finished,false);release();await first;await closing;await scope.waitForDataReadiness();assert.equal(queries,2);},{createLedger:async()=>({onResult:async()=>{},collect:async()=>({passed:null}),waitForDataReadiness:async()=>{queries++;entered();await gate;return {passed:null,counts:{total:0,ready:0}};}})});
});

async function wireIdentityFixture(operation,options={}){
  const adapter=createCapacityObservationAdapter({sendModel:async()=>{},sendAdmin:async()=>{},createLedger:async()=>({collect:async()=>({passed:null}),onResult:async()=>{}}),settleTimeoutMs:5,...options});
  const reserve=async scope=>{const requestId=randomUUID(),trace=(await scope.observer.beforeRequest({requestId,source:'baseline',actorIndex:0,method:'GET',route:'/health',credentials:{cookie:null,authorization:null}}))['x-capacity-trace'];return {requestId,trace};},start=(request,wireId)=>adapter.observeWire({kind:'gateway-request-start',trace:request.trace,wireId,signature:'GET /health',credentials:{cookie:null,authorization:null}}),end=(request,wireId,trace=request.trace)=>adapter.observeWire({kind:'gateway-response-end',trace,wireId,signature:'GET /health',status:200}),finish=async(scope,request)=>{await scope.observer.response({requestId:request.requestId,status:200});await scope.observer.requestFinished({requestId:request.requestId});};
  try{await operation({adapter,reserve,start,end,finish});}finally{await adapter.close();}
}

for(const mode of ['matching','wrong-id','missing-id','wrong-trace','missing-trace'])test('wire terminal identity must match its start: '+mode,async()=>wireIdentityFixture(async({adapter,reserve,start,end,finish})=>{
  const scope=await adapter.createObservationScope({id:'terminal-'+mode,kind:'normal'}),request=await reserve(scope),wireId=randomUUID();start(request,wireId);end(request,mode==='wrong-id'?randomUUID():mode==='missing-id'?undefined:wireId,mode==='wrong-trace'?randomUUID():mode==='missing-trace'?null:request.trace);await finish(scope,request);const report=await scope.collect();assert.equal(report.transport.passed,mode==='matching');if(mode!=='matching'){assert.ok(report.transport.issueCount>0);assert.equal(report.transport.pending,1,'an unrelated terminal cannot release the real request');}else assert.equal(report.transport.pending,0);
}));

test('concurrent requests cannot borrow another request wire identity even with their own valid trace',async()=>wireIdentityFixture(async({adapter,reserve,start,end,finish})=>{
  const a=await adapter.createObservationScope({id:'wire-owner-a',kind:'normal'}),b=await adapter.createObservationScope({id:'wire-owner-b',kind:'ui'}),first=await reserve(a),second=await reserve(b),wireA=randomUUID(),wireB=randomUUID();start(first,wireA);start(second,wireB);end(first,wireB);end(second,wireA);await finish(a,first);await finish(b,second);for(const scope of [a,b]){const report=await scope.collect();assert.equal(report.transport.passed,false);assert.equal(report.transport.pending,1);assert.ok(report.transport.issues.some(row=>row.code==='wire_identity_owner_mismatch'||row.code==='wire_terminal_identity_mismatch'));}
}));

test('settled wire identities remain unique across later scopes and duplicate terminals',async()=>wireIdentityFixture(async({adapter,reserve,start,end,finish})=>{
  const a=await adapter.createObservationScope({id:'settled-wire-a',kind:'normal'}),first=await reserve(a),wireId=randomUUID();start(first,wireId);end(first,wireId);await finish(a,first);await a.close();assert.equal((await a.collect()).transport.passed,true);end(first,wireId);assert.ok((await a.collect()).transport.issues.some(row=>row.code==='wire_after_settled'));
  const b=await adapter.createObservationScope({id:'settled-wire-b',kind:'normal'}),second=await reserve(b);start(second,wireId.toUpperCase());end(second,wireId.toUpperCase());await finish(b,second);assert.equal((await b.collect()).transport.passed,false);assert.equal((await a.collect()).transport.passed,false);assert.ok((await b.collect()).transport.issues.some(row=>row.code==='wire_identity_reused'));
}));

test('wire identity history exhaustion keeps real requests successful and makes evidence incomplete',async()=>fixture(async({adapter})=>{
  const scope=await adapter.createObservationScope({id:'wire-history-budget',kind:'normal'});for(let index=0;index<2;index++){const response=await scope.sendModel({actorIndex:0});assert.equal(response.success,true);assert.equal(response.status,200);}await scope.close();const report=await scope.collect();assert.equal(report.transport.passed,null);assert.equal(report.transport.pending,0);assert.equal(report.transport.counts.reconciled,2);assert.deepEqual(report.transport.wireIdentityHistory,{retained:1,limit:1,budgetExceeded:1});assert.equal(report.transport.issueCount,0);
},{maxWireIdentities:1}));

test('a confirmed identity reuse remains false after the history budget was exhausted',async()=>wireIdentityFixture(async({adapter,reserve,start,end,finish})=>{
  const scope=await adapter.createObservationScope({id:'budget-then-conflict',kind:'normal'}),retained=randomUUID();for(const wireId of [retained,randomUUID()]){const request=await reserve(scope);start(request,wireId);end(request,wireId);await finish(scope,request);}assert.equal((await scope.collect()).transport.passed,null);const repeated=await reserve(scope);start(repeated,retained);end(repeated,retained);await finish(scope,repeated);const report=await scope.collect();assert.equal(report.transport.passed,false);assert.equal(report.transport.wireIdentityHistory.retained,1);assert.ok(report.transport.wireIdentityHistory.budgetExceeded>0);
},{maxWireIdentities:1}));
