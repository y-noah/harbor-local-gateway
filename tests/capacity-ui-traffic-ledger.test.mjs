import test from 'node:test';
import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {createUiTrafficLedger,uiTrafficCategory} from '../tools/capacity/ui-traffic-ledger.mjs';
const hash=value=>createHash('sha256').update(value).digest('hex');
const admin=hash('synthetic-cookie'),employee=hash('synthetic-authorization');
const create=options=>createUiTrafficLedger({origin:'http://127.0.0.1:1234',adminFingerprints:[admin],employeeFingerprints:[employee],...options});
const wireIds=new Map();
const start=(ledger,trace,signature='GET /api/admin/members',credentials={cookie:admin})=>{const wireId=wireIds.get(trace)??randomUUID();wireIds.set(trace,wireId);ledger.server({kind:'gateway-request-start',trace,wireId,signature,credentials});};
const finish=(ledger,trace,status=200)=>{ledger.client(trace,{kind:'response',status});ledger.client(trace,{kind:'finished'});ledger.server({kind:'gateway-response-end',trace,wireId:wireIds.get(trace),status});};

test('all preparation, measurement, refresh and drain requests remain visible without replacing normal mix',async()=>{
 let time=10;const ledger=create({now:()=>time}),issue=source=>ledger.plan({source,actorIndex:0,method:'GET',route:'/api/admin/members',credentials:{cookie:admin}});
 const bootstrap=issue('browser');start(ledger,bootstrap);finish(ledger,bootstrap);ledger.beginMeasurement();time=20;
 const baseline=issue('baseline');start(ledger,baseline);finish(ledger,baseline);
 const dto={requestId:'synthetic-browser-request',actorIndex:0,method:'GET',route:'/api/admin/state',credentials:{cookie:admin}};
 const headers=await ledger.observer.beforeRequest(dto);assert.deepEqual(Object.keys(headers),['x-capacity-trace']);const browser=headers['x-capacity-trace'];start(ledger,browser,'GET /api/admin/state');ledger.observer.response({requestId:dto.requestId,status:200});ledger.observer.requestFinished({requestId:dto.requestId});ledger.server({kind:'gateway-response-end',trace:browser,wireId:wireIds.get(browser),status:200});
 time=30;ledger.endMeasurement();const cleanup=issue('auxiliary');start(ledger,cleanup);ledger.client(cleanup,{kind:'cancelled'});ledger.server({kind:'gateway-response-abort',trace:cleanup,wireId:wireIds.get(cleanup)});
 const report=ledger.collect();assert.equal(report.passed,true);assert.equal(report.planned,4);assert.equal(report.matched,4);assert.equal(report.measurementMs,20);assert.equal(report.normalMixReplaced,false);assert.deepEqual(new Set(report.counts.map(row=>row.phase)),new Set(['preparation','measurement','drain']));assert.ok(report.counts.some(row=>row.source==='browser'&&row.category==='summary'));assert.ok(!JSON.stringify(report).includes('synthetic-cookie'));
});
test('server truth detects duplicate, forged credentials and mismatched completion',()=>{
 const ledger=create();ledger.beginMeasurement();const trace=ledger.plan({actorIndex:0,route:'/api/admin/members',credentials:{cookie:admin}});start(ledger,trace);start(ledger,trace,'GET /api/admin/keys',{cookie:hash('another-cookie')});finish(ledger,trace);ledger.server({kind:'gateway-response-end',trace,wireId:wireIds.get(trace),status:200});ledger.endMeasurement();const report=ledger.collect();assert.equal(report.passed,false);for(const code of ['duplicate_server_arrival','server_signature_mismatch','server_credential_mismatch','duplicate_server_terminal'])assert.ok(report.issues.some(row=>row.code===code));
 const other=create();other.beginMeasurement();const id=other.plan({route:'/api/admin/members',credentials:{cookie:admin}});start(other,id);other.client(id,{kind:'response',status:200});other.client(id,{kind:'finished'});other.server({kind:'gateway-response-abort',trace:id,wireId:wireIds.get(id)});other.endMeasurement();assert.equal(other.collect().passed,false);
});
test('client initiation alone and untraced server traffic cannot pass reconciliation',()=>{
 const ledger=create();ledger.beginMeasurement();const trace=ledger.plan({route:'/api/admin/members',credentials:{cookie:admin}});ledger.client(trace,{kind:'response',status:200});ledger.client(trace,{kind:'finished'});ledger.server({kind:'gateway-request-start',trace:null,signature:'GET /api/admin/state'});ledger.endMeasurement();const report=ledger.collect();assert.equal(report.passed,null);assert.ok(report.missing.includes('unattributed_server_traffic'));assert.ok(report.missing.includes('unreconciled_client_server_requests'));assert.equal(report.counts[0].serverArrivals,0);
});
test('source binding, budget and DTO boundaries reject spoofed inputs',async()=>{
 const ledger=create({maxRequests:1});assert.throws(()=>ledger.plan({route:'/api/admin/members',credentials:{cookie:employee}}),/credential_mismatch/);assert.throws(()=>ledger.plan({source:'model',route:'/v1/responses',credentials:{authorization:admin}}),/credential_mismatch/);assert.throws(()=>ledger.plan({route:'//example.invalid/api',credentials:{cookie:admin}}),/ledger_route/);
 await ledger.observer.beforeRequest({requestId:'one',actorIndex:0,method:'GET',route:'/api/admin/members',credentials:{cookie:admin}});await assert.rejects(ledger.observer.beforeRequest({requestId:'one',actorIndex:0,method:'GET',route:'/api/admin/members',credentials:{cookie:admin}}),/request_identity/);assert.throws(()=>ledger.plan({route:'/api/admin/members',credentials:{cookie:admin}}),/budget/);assert.ok(ledger.collect().missing.includes('traffic_ledger_budget_exhausted'));ledger.close();assert.throws(()=>ledger.plan({route:'/api/admin/members',credentials:{cookie:admin}}),/closed/);
});
test('the state endpoint and summary requests never masquerade as list traffic',()=>{
 assert.equal(uiTrafficCategory('GET','/api/admin/state?people=none'),'summary');assert.equal(uiTrafficCategory('GET','/api/admin/requests?view=list'),'list');assert.equal(uiTrafficCategory('GET','/api/admin/requests?view=summary'),'summary');assert.equal(uiTrafficCategory('GET','/api/auth/session'),'auxiliary');assert.equal(uiTrafficCategory('GET','/app.js'),'asset');assert.equal(uiTrafficCategory('GET','/api/:other-1234567890abcdef'),'auxiliary');
});

test('terminal identities must match their own unique wire arrival',()=>{for(const mode of ['wrong','missing','reused']){const ledger=create();ledger.beginMeasurement();const trace=ledger.plan({route:'/api/admin/members',credentials:{cookie:admin}}),wireId=randomUUID();ledger.server({kind:'gateway-request-start',trace,wireId,signature:'GET /api/admin/members',credentials:{cookie:admin}});if(mode==='reused'){const other=ledger.plan({route:'/api/admin/members',credentials:{cookie:admin}});ledger.server({kind:'gateway-request-start',trace:other,wireId,signature:'GET /api/admin/members',credentials:{cookie:admin}});}ledger.client(trace,{kind:'response',status:200});ledger.client(trace,{kind:'finished'});ledger.server({kind:'gateway-response-end',trace,wireId:mode==='wrong'?randomUUID():mode==='missing'?undefined:wireId,status:200});ledger.endMeasurement();assert.equal(ledger.collect().passed,false,mode);}});

test('a reserved trace cancelled before transport is settled without fabricating a server arrival',async()=>{const ledger=create();ledger.beginMeasurement();await ledger.observer.beforeRequest({requestId:'reserved-not-sent',source:'browser',actorIndex:0,method:'GET',route:'/api/admin/state',credentials:{cookie:admin}});ledger.observer.requestFailed({requestId:'reserved-not-sent',cancelled:true,notSent:true});ledger.endMeasurement();let report=ledger.collect();assert.equal(report.passed,true);assert.equal(report.pending,0);assert.equal(report.notSent,1);assert.equal(report.matched,0);assert.equal(report.accounted,1);const trace=ledger.scopeId+'-1';ledger.server({kind:'gateway-request-start',trace,wireId:randomUUID(),signature:'GET /api/admin/state',credentials:{cookie:admin}});report=ledger.collect();assert.equal(report.passed,false);assert.ok(report.issues.some(row=>row.code==='wire_after_not_sent'));});

test('cancelled HTTP non-success intersection is counted per request without duplicating ordinary failures',()=>{
 const ledger=create();ledger.beginMeasurement();
 for(const [outcome,status] of [['finished',500],['failed',null],['cancelled',500],['cancelled',null]]){
  const trace=ledger.plan({route:'/api/admin/members',credentials:{cookie:admin}});start(ledger,trace);if(status!==null)ledger.client(trace,{kind:'response',status});ledger.client(trace,{kind:outcome});ledger.server({kind:outcome==='finished'?'gateway-response-end':'gateway-response-abort',trace,wireId:wireIds.get(trace),status});
 }
 ledger.endMeasurement();const report=ledger.collect();assert.equal(report.passed,true);const row=report.counts[0];assert.equal(row.normalAttempts,2);assert.equal(row.httpFailed,2);assert.equal(row.httpNonSuccess,2);assert.equal(row.clientCancelled,2);assert.equal(row.cancelledHttpNonSuccess,1);
});
