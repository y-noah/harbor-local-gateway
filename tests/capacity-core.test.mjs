import test from 'node:test';
import assert from 'node:assert/strict';
import {capacityPlan,assertFormalPlan,targets} from '../tools/capacity/scenarios.mjs';
import {Histogram,createCapacityMetrics,evaluateCapacity} from '../tools/capacity/metrics.mjs';
import {runOpenLoop} from '../tools/capacity/driver.mjs';
import {resourcePreflight,collectHostEvidence,assessCgroupHierarchy,normalizeCapacityResourceLimits,assessCapacityProcessLimits} from '../tools/capacity/resources.mjs';
import {assessStages,assessRecovery,assessResourceWindows,reconcileCapacityLedger} from '../tools/capacity/assessments.mjs';
import {createOwnedRuntime} from '../tools/capacity/owned-runtime.mjs';
import {createOwnedFaultAdapter} from '../tools/capacity/faults.mjs';
import {credentialFingerprints} from '../tools/capacity/http-client.mjs';
import {EventEmitter} from 'node:events';
import {createGatewayProbe} from '../tools/capacity/gateway-probe.mjs';
import {runCapacitySuite} from '../tools/capacity/runner.mjs';
import {summarizeCapacityCoverage} from '../tools/capacity/coverage-adapter.mjs';

test('formal blueprint retains all required scale, long scenarios, API thresholds and frozen acceptance targets',()=>{
  const plan=capacityPlan();assert.equal(plan.targets.metadata7d,1260000);assert.equal(plan.targets.metadata30d,5400000);assert.equal(plan.scenarios.find(row=>row.id==='steady-8h').durationMs,28800000);assert.ok(plan.scenarios.find(row=>row.id==='long-120').durationMs>=1800000);assert.equal(plan.scenarios.find(row=>row.id==='long-60').durationMs,7200000);assert.equal(plan.scenarios.find(row=>row.id==='burst').burst.qps,30);assertFormalPlan(plan);
  assert.throws(()=>{plan.scenarios[0].durationMs=1;},TypeError);const changed=structuredClone(plan);changed.scenarios[0].durationMs=1;assert.throws(()=>assertFormalPlan(changed),/targets_changed/);assert.throws(()=>assertFormalPlan({...plan,operations:[]}),/inventory_incomplete/);
});

test('formal final verdict cannot omit unknown or failed live dataset coverage',()=>{
  // Isolate this evidence gate; this minimal plan is not a runnable formal plan.
  const plan={profile:'formal',operations:[],scenarios:[]},metrics=createCapacityMetrics().view();
  const sample={at:1,metadata7d:1260000,metadata30d:5400000,pricing_revision:'synthetic-v1'};
  const verdict=coverage=>evaluateCapacity({plan,metrics,preflight:{eligible:true},evidence:new Proxy({liveDatasetCoverage:coverage,realTransferSlots:[{actual:true,applied:1},{actual:true,applied:1}]},{get:(value,key)=>Object.hasOwn(value,key)?value[key]:{passed:true}})});
  for(const coverage of [undefined,null,summarizeCapacityCoverage([sample],{profile:'formal'}),summarizeCapacityCoverage([{...sample,verified:false}],{profile:'formal'})]){
    const result=verdict(coverage);assert.equal(result.status,'incomplete');assert.ok(result.incomplete.includes('缺少验收证据:liveDatasetCoverage'));
  }
  const insufficient=verdict(summarizeCapacityCoverage([{...sample,metadata7d:1259999}],{profile:'formal'}));
  assert.equal(insufficient.status,'failed');assert.ok(insufficient.failures.includes('验收失败:liveDatasetCoverage'));
  const changedPrice=verdict(summarizeCapacityCoverage([sample,{...sample,at:2,pricing_revision:'synthetic-v2'}],{profile:'formal'}));
  assert.equal(changedPrice.status,'failed');assert.ok(changedPrice.failures.includes('验收失败:liveDatasetCoverage'));
  assert.equal(verdict({passed:true}).status,'passed','the isolated gate preserves explicitly verified coverage');
});

test('histogram reports conservative quantiles and bounded memory rather than keeping every sample',()=>{
  const histogram=new Histogram();for(let index=1;index<=10000;index++)histogram.add(index/100);assert.ok(histogram.quantile(0.95)>=95);assert.ok(histogram.quantile(0.95)<=95.1);assert.ok(histogram.bins.size<10000);assert.equal(histogram.view().samples,10000);assert.throws(()=>histogram.add(NaN));assert.throws(()=>histogram.quantile(-1));
});

test('open-loop arrivals continue at the planned rate when slow responses fill the client budget',async()=>{
  let clock=0,release,calls=0;const held=new Promise(resolve=>{release=resolve;}),metrics=createCapacityMetrics();const running=runOpenLoop({durationMs:1000,segments:[{startMs:0,endMs:1000,qps:10}],metrics,maxInFlight:2,now:()=>clock,sleep:async ms=>{clock+=ms;},send:async()=>{calls++;await held;}});
  for(let turn=0;turn<30&&metrics.view().arrivals.planned<10;turn++)await Promise.resolve();assert.equal(metrics.view().arrivals.planned,10);assert.equal(metrics.view().arrivals.sent,2);assert.equal(metrics.view().arrivals.missed,8);release();const result=await running;assert.equal(calls,2);assert.equal(metrics.view().arrivals.inflight,0);assert.ok(result.durationMs>=1000);
});

test('slow individual endpoints and full five-minute windows cannot be hidden by a fast site average',()=>{
  const metrics=createCapacityMetrics();for(const [operation,time] of [['health',20],['request-list',501]])for(let index=0;index<1000;index++)metrics.record({operation,scenario:'mixed',variant:'steady',category:'read',group:'normal',at:index,durationMs:time,success:true});
  const verdict=evaluateCapacity({plan:capacityPlan(),preflight:{eligible:false},metrics:metrics.view()});assert.equal(verdict.status,'failed');assert.ok(verdict.failures.some(row=>row.includes('request-list')));assert.ok(verdict.incomplete.some(row=>row.includes('接口未测')));
  const limited=createCapacityMetrics({maxSeries:1,maxBuckets:4});limited.record({operation:'one',scenario:'test',at:0,durationMs:1,success:true});limited.record({operation:'two',scenario:'test',at:0,durationMs:1,success:true});assert.equal(limited.view().dropped,1);
});

test('hand-entered resource claims and selfcheck samples never qualify as formal capacity',async()=>{
  const host=await collectHostEvidence(),proof={source:'observed-cgroup',cgroupIdentity:'invented',cpuQuota:4,memoryLimitBytes:8*1024**3};const result=resourcePreflight({host,roles:{gatewayWorker:proof,database:{...proof,cgroupIdentity:'other'}},dataset:{verified:true,metadata7d:1260000,metadata30d:5400000,bodyRequests:5400000,bodyBytes:1,bodyDistributionVerified:true,formalBodyFootprintVerified:true}});assert.equal(result.eligible,false);assert.ok(result.missing.some(row=>row.includes('配额')));
  let called=false;await assert.rejects(runCapacitySuite({plan:capacityPlan(),preflight:{eligible:true},target:{sendModel:()=>{called=true;},sendAdmin:()=>{}},executeLong:true}),/preflight_blocked/);assert.equal(called,false);
});

test('recovery, CPU and memory assessment require real timing and complete evidence',()=>{
  assert.equal(assessRecovery({kind:'host'}).passed,null);assert.equal(assessRecovery({kind:'database',faultRestoredAt:100,acceptingAt:60200,dataVerified:true}).passed,false);assert.equal(assessRecovery({kind:'worker',faultRestoredAt:100,backlogClearedAt:200,dataVerified:true}).passed,true);
  const roles={gatewayWorker:{cpuQuota:4,memoryLimitBytes:1000},database:{cpuQuota:4,memoryLimitBytes:1000}},samples=Array.from({length:61},(_,index)=>index*5000).map((at,index)=>({at,roles:Object.fromEntries(Object.keys(roles).map(key=>[key,{cpuUsageUs:at*1000*4*0.9,memoryCurrentBytes:500}]))}));assert.equal(assessResourceWindows(samples,{roles,idleVerified:true}).passed,false);assert.equal(assessResourceWindows(samples.slice(0,1),{roles}).passed,null);
});

test('ledger detects replay, exact amount mismatch, wrong device and loss without retaining bodies',async()=>{
  const accepted=[{requestId:'r1',trace:'t1',memberId:'m1',keyId:'k1',deviceId:'d1',group:'normal',recordingExpected:true,expectedMemberId:'m1',expectedKeyId:'k1',expectedDeviceId:'d1',expectedAtoms:'9007199254740993'}],base={requestId:'r1',memberId:'m1',keyId:'k1',deviceId:'d1',status:'ok',usageUnknown:false,pricePresent:true,totalAtoms:'9007199254740993',recordingState:'complete',recordingEvents:2};assert.equal((await reconcileCapacityLedger({accepted,dispatches:[{trace:'t1'}],persisted:[base],expectedCounts:{accepted:1,dispatched:1,persisted:1}})).passed,true);
  const bad=await reconcileCapacityLedger({accepted,dispatches:[{trace:'t1'},{trace:'t1'}],persisted:[{...base,totalAtoms:'9007199254740992',deviceId:'other'}]});assert.equal(bad.passed,false);for(const code of ['repeated_model_dispatch','identity_mismatch','amount_mismatch'])assert.ok(bad.issues.some(row=>row.code===code));assert.equal((await reconcileCapacityLedger({accepted,dispatches:[],persisted:[]})).passed,false);assert.equal((await reconcileCapacityLedger({accepted:[...accepted,{...accepted[0],requestId:'r2',trace:'t2'}],dispatches:[],persisted:[],maxEntries:1})).passed,null);
});

test('fault control rejects foreign processes and only pauses its own new worker child',async()=>{
  const first=createOwnedRuntime(),second=createOwnedRuntime();try{assert.throws(()=>createOwnedFaultAdapter({runtime:{}}),/not_owned/);const child=first.start(process.execPath,['-e','setInterval(()=>{},1000)'],{role:'worker'});await assert.rejects(second.pauseWorker(child,1),/not_owned/);const adapter=createOwnedFaultAdapter({runtime:first,worker:child});assert.deepEqual(adapter.supported,['worker-pause']);const result=await adapter.run('worker-pause',{durationMs:20});assert.equal(result.ownership,'task-created-isolated-targets');await first.stop(child);assert.throws(()=>first.proof(child),/not_owned/);}finally{await first.close();await second.close();}
});


test('P3 resource hierarchy rejects ancestor roles, constrained common parents and insufficient host capacity',()=>{
  const GiB=1024**3,host={cpuLogical:16,memoryTotal:32*GiB,memoryAvailable:24*GiB},root={identity:'root',cpuQuota:8,memoryLimitBytes:16*GiB},role=id=>({cgroupIdentity:id,memoryCurrentBytes:0,ancestors:[{identity:id,cpuQuota:4,memoryLimitBytes:8*GiB},root]});
  assert.deepEqual(assessCgroupHierarchy({host,roles:{gatewayWorker:role('g'),database:role('d')}}),[]);const nested=role('d');nested.ancestors.splice(1,0,{identity:'g',cpuQuota:4,memoryLimitBytes:8*GiB});assert.ok(assessCgroupHierarchy({host,roles:{gatewayWorker:role('g'),database:nested}}).some(row=>row.includes('祖孙')));
  const small={identity:'root',cpuQuota:4,memoryLimitBytes:12*GiB},g=role('g'),d=role('d');g.ancestors[1]=small;d.ancestors[1]=small;assert.ok(assessCgroupHierarchy({host,roles:{gatewayWorker:g,database:d}}).some(row=>row.includes('共同祖先')));assert.ok(assessCgroupHierarchy({host:{...host,memoryTotal:12*GiB},roles:{gatewayWorker:role('g'),database:role('d')}}).some(row=>row.includes('总容量')));
});

test('P4 empty or consistently truncated ledgers cannot claim independent data reconciliation',async()=>{
  assert.equal((await reconcileCapacityLedger({accepted:[],dispatches:[],persisted:[]})).passed,null);const row={requestId:'r',trace:'t',memberId:'m',keyId:'k',deviceId:'d',expectedMemberId:'m',expectedKeyId:'k',expectedDeviceId:'d',group:'normal',expectedAtoms:'1'};const result=await reconcileCapacityLedger({accepted:[row],dispatches:[{trace:'t'}],persisted:[{requestId:'r',memberId:'m',keyId:'k',deviceId:'d',status:'ok',usageUnknown:false,pricePresent:true,totalAtoms:'1'}],expectedCounts:{accepted:2,dispatched:2,persisted:2}});assert.equal(result.passed,false);assert.ok(result.issues.some(value=>value.code==='coverage_count_mismatch'));
});


test('probe close is idempotent and restores the original dispatcher even when final sampling fails',async()=>{
  const server=new EventEmitter(),handler=()=>{},observe=async()=> 'r',finish=async()=>({settled:true});server.on('request',handler);const app={server,store:{observe,finish},sessionHandoffs:{arrive:()=>({bind:async()=>{}})},status:()=>{throw new Error('synthetic-final-sample');}},probe=createGatewayProbe();probe.attach(app);await assert.rejects(probe.close(),/synthetic-final-sample/);await assert.rejects(probe.close(),/synthetic-final-sample/);assert.deepEqual(server.listeners('request'),[handler]);assert.equal(app.store.observe,observe);assert.equal(app.store.finish,finish);assert.throws(()=>probe.attach(app),/already_attached/);
});


test('measurement callbacks receive planned identity but never execution credentials or response bodies',async()=>{
  const plan=structuredClone(capacityPlan('selfcheck'));plan.scenarios=[{id:'privacy-check',durationMs:20,modelQps:50,adminQps:50,employees:1,administrators:1}];const secret='synthetic-secret-must-not-enter-log',cookie='capacity_admin=synthetic-cookie',rows=[];const base={success:true,complete:true,httpEnded:true,wireSent:true,durationMs:1,startedAt:0,firstContentAt:1,json:{api_key:secret}};
  const result=await runCapacitySuite({plan,preflight:{eligible:false},target:{employeeActors:[{memberId:'m',keyId:'k',deviceId:'d',secret}],adminActors:[{headers:{cookie}}],sendModel:async event=>{assert.equal(event.actor.secret,secret);return {...base,operation:'model',method:'POST',credentials:credentialFingerprints({authorization:'Bearer '+event.actor.secret})};},sendAdmin:async()=>({...base,operation:'health',method:'GET',requestSignature:'GET /health',credentials:credentialFingerprints({cookie})}),onResult:async row=>rows.push(row)}});assert.equal(rows.length,2);assert.equal(JSON.stringify(rows).includes(secret),false);assert.equal(JSON.stringify(rows).includes(cookie),false);assert.equal(Object.hasOwn(rows[0].result,'json'),false);assert.equal(result.evidence.actorCoverage.passed,true);
});


test('P5 scheduler pauses miss expired arrivals instead of emitting a catch-up burst',async()=>{
  let clock=0,sent=0;const metrics=createCapacityMetrics();await runOpenLoop({durationMs:1000,segments:[{startMs:0,endMs:1000,qps:10}],metrics,now:()=>clock,sleep:async ms=>{clock+=ms+2000;},send:async()=>{sent++;}});assert.equal(sent,1);assert.equal(metrics.view().arrivals.missed,9);assert.ok(metrics.view().violations.length);
});
test('P6 sparse resource samples cannot interpolate complete five-minute windows',()=>{
  const roles={gatewayWorker:{cpuQuota:4,memoryLimitBytes:1000},database:{cpuQuota:4,memoryLimitBytes:1000}},samples=[0,600000].map(at=>({at,roles:Object.fromEntries(Object.keys(roles).map(key=>[key,{cpuUsageUs:at*1000*4*0.5,memoryCurrentBytes:500}]))}));const verdict=assessResourceWindows(samples,{roles,idleVerified:true});assert.equal(verdict.passed,null);assert.ok(verdict.missing.some(value=>value.includes('间隔')));
});
test('P7 every cancelled request needs cancellation and release endpoints',()=>{
  const normal={bodyEndAt:0,dispatchAt:1,deductibleWaitMs:0,admissionWaitMs:1,gatewayContentAt:1,helperDeliveredAt:2,completedAt:3,bodyVisibleAt:4,upstreamEndedAt:3,releasedAt:4},cancel={cancelAt:0,upstreamCancelAt:1,upstreamEndedAt:1,releasedAt:2};assert.equal(assessStages([normal,cancel],{minSamples:1}).passed,true);const incomplete=assessStages([normal,cancel,{cancelAt:1}],{minSamples:1});assert.equal(incomplete.passed,null);assert.ok(incomplete.missing.some(value=>value.includes('取消请求')));
});

test('P3 actual process cgroup trees account for shared child limits and overlapping CPU affinity',async()=>{
  const {effectiveQuota,parseCpuSet}=await import('../tools/capacity/quota-graph.mjs'),GiB=1024**3,host={cpuLogical:8,memoryTotal:16*GiB},node=(identity,parentIdentity,cpuQuota,memoryGiB,cpuIds=parseCpuSet('0-7'))=>({identity,parentIdentity,cpuQuota,memoryLimitBytes:memoryGiB*GiB,cpuIds}),root=node('root',null,8,16),parent=node('p','root',4,8),small=node('small','p',1,1),process=(cgroupIdentity,cpuIds=parseCpuSet('0-7'))=>({cgroupIdentity,cpuIds});assert.deepEqual(effectiveQuota({host,nodes:[root,parent,small],processes:[process('small'),process('small')]}),{cpuQuota:1,memoryLimitBytes:GiB});assert.deepEqual(effectiveQuota({host,nodes:[root,parent,node('g','p',3,6),node('w','p',1,2)],processes:[process('g'),process('w')]}),{cpuQuota:4,memoryLimitBytes:8*GiB});const overlap=effectiveQuota({host,nodes:[root,node('a','root',4,8),node('b','root',4,8)],processes:[process('a',parseCpuSet('0-3')),process('b',parseCpuSet('0-3'))]});assert.equal(overlap.cpuQuota,4);assert.equal(overlap.memoryLimitBytes,16*GiB);assert.throws(()=>parseCpuSet('0-99999'),/budget/);
});

test('P7 normal completions cannot borrow another request cancellation/release evidence',()=>{
 const normal={bodyEndAt:0,dispatchAt:1,deductibleWaitMs:0,admissionWaitMs:1,gatewayContentAt:1,helperDeliveredAt:2,completedAt:3,bodyVisibleAt:4};assert.equal(assessStages([normal,{cancelAt:0,upstreamCancelAt:1,upstreamEndedAt:1,releasedAt:2}],{minSamples:1}).passed,null);assert.equal(assessStages([{...normal,upstreamEndedAt:3,releasedAt:6000},{cancelAt:0,upstreamCancelAt:1,upstreamEndedAt:1,releasedAt:2}],{minSamples:1}).passed,false);
});
test('P9 scenario duration and generic pass markers cannot replace actual model and API samples',()=>{
 const plan=capacityPlan(),metrics=createCapacityMetrics(),evidence=new Proxy({scenarios:Object.fromEntries(plan.scenarios.map(scene=>[scene.id,{completed:true,durationMs:scene.durationMs}])),realTransferSlots:[{actual:true,applied:1},{actual:true,applied:1}]},{get:(object,key)=>key in object?object[key]:{passed:true}});for(const operation of plan.operations)for(let index=0;index<1000;index++)metrics.record({operation:operation.id,scenario:'mixed',category:'read',at:index,durationMs:1,success:true});for(const scene of plan.scenarios)for(const operation of ['model','first-content'])metrics.record({operation,scenario:scene.id,category:operation==='model'?'model':'diagnostic',group:scene.fault?'fault':scene.burst&&operation==='model'?'stress':'normal',at:0,durationMs:1,success:true});const verdict=evaluateCapacity({plan,metrics:metrics.view(),preflight:{eligible:true},evidence});assert.equal(verdict.status,'incomplete');assert.ok(verdict.incomplete.includes('场景测量样本不足:long-120/first-content'));assert.ok(verdict.incomplete.includes('场景管理类别未测:long-120/list'));
});

test('declared low resource roles still require all ancestors, exact leaves and the aggregate CPU affinity',async()=>{
 const GiB=1024**3,resourceLimits=normalizeCapacityResourceLimits({gatewayWorker:{cpuQuota:1.5,memoryLimitBytes:2.5*GiB},database:{cpuQuota:1,memoryLimitBytes:2*GiB},processLimits:{gateway:{cpuQuota:1,memoryLimitBytes:2*GiB},worker:{cpuQuota:0.5,memoryLimitBytes:0.5*GiB}}}),host={cpuLogical:8,memoryTotal:32*GiB,memoryAvailable:24*GiB,node:process.version},node=(identity,parentIdentity,cpuQuota,memoryLimitBytes)=>({identity,parentIdentity,cpuQuota,memoryLimitBytes,cpuIds:[0,1,2,3]}),root=node('root',null,8,32*GiB),g=node('g','root',1.5,2.5*GiB),d=node('d','root',1,2*GiB),gl=node('gl','g',1,2*GiB),wl=node('wl','g',0.5,0.5*GiB),processRow=(role,cgroupIdentity)=>({role,cgroupIdentity,cpuIds:[0,1,2,3]});
 const roles={gatewayWorker:{source:'observed-cgroup',cgroupIdentity:'g',cpuQuota:1.5,memoryLimitBytes:2.5*GiB,memoryCurrentBytes:0,ancestors:[g,root],nodes:[root,g,gl,wl],processes:[processRow('gateway','gl'),processRow('worker','wl')],mappingVerified:true,processRoles:['gateway','worker']},database:{source:'observed-cgroup',cgroupIdentity:'d',cpuQuota:1,memoryLimitBytes:2*GiB,memoryCurrentBytes:0,ancestors:[d,root],nodes:[root,d],processes:[processRow('database','d')],mappingVerified:true,processRoles:['database']}};
 assert.deepEqual(assessCgroupHierarchy({host,roles,resourceLimits}),[]);assert.deepEqual(assessCapacityProcessLimits(roles,resourceLimits),[]);assert.equal(resourcePreflight({host,roles,resourceLimits}).eligible,false,'JSON roles cannot acquire an observed cgroup identity');
 const shared=structuredClone(roles);for(const role of Object.values(shared))role.ancestors.find(row=>row.identity==='root').cpuQuota=2;assert.ok(assessCgroupHierarchy({host,roles:shared,resourceLimits}).some(code=>code.includes('共同祖先')));
 const leaf=structuredClone(roles);leaf.gatewayWorker.nodes.find(row=>row.identity==='gl').memoryLimitBytes=GiB;assert.ok(assessCapacityProcessLimits(leaf,resourceLimits).some(code=>code.includes('gateway')));
 const affinity=structuredClone(roles);for(const role of Object.values(affinity))for(const entry of role.processes)entry.cpuIds=[0,1];assert.ok(resourcePreflight({host,roles:affinity,resourceLimits}).missing.some(code=>code.includes('共享后')));
});

test('decimal CPU max-flow rounding cannot reject 0.3 plus 0.6 while a genuine shortfall still fails',async()=>{
 const {effectiveQuota}=await import('../tools/capacity/quota-graph.mjs'),GiB=1024**3,resourceLimits=normalizeCapacityResourceLimits({gatewayWorker:{cpuQuota:0.3,memoryLimitBytes:2*GiB},database:{cpuQuota:0.6,memoryLimitBytes:2*GiB}}),host={cpuLogical:8,memoryTotal:32*GiB,memoryAvailable:24*GiB,node:process.version},node=(identity,parentIdentity,cpuQuota,memoryLimitBytes)=>({identity,parentIdentity,cpuQuota,memoryLimitBytes,cpuIds:[0,1]}),root=node('r',null,8,32*GiB),g=node('g','r',0.3,2*GiB),d=node('d','r',0.6,2*GiB),gl=node('gl','g',0.3,2*GiB),wl=node('wl','g',0.3,2*GiB),processRow=(role,cgroupIdentity)=>({role,cgroupIdentity,cpuIds:[0,1]}),roles={gatewayWorker:{cgroupIdentity:'g',cpuQuota:0.3,memoryLimitBytes:2*GiB,memoryCurrentBytes:0,ancestors:[g,root],nodes:[root,g,gl,wl],processes:[processRow('gateway','gl'),processRow('worker','wl')]},database:{cgroupIdentity:'d',cpuQuota:0.6,memoryLimitBytes:2*GiB,memoryCurrentBytes:0,ancestors:[d,root],nodes:[root,d],processes:[processRow('database','d')]}};
 const observed=effectiveQuota({host,nodes:[root,g,gl,wl,d],processes:[...roles.gatewayWorker.processes,...roles.database.processes]});assert.ok(observed.cpuQuota<0.9,'this graph reproduces the IEEE max-flow sum');const exact=resourcePreflight({host,roles,resourceLimits});assert.equal(exact.missing.some(code=>code.includes('CPU亲和性资源不足')||code.includes('资源共享后不足')),false);assert.equal(exact.eligible,false,'synthetic graph remains unbranded');
 const insufficient=structuredClone(roles);for(const role of Object.values(insufficient))for(const item of role.nodes)if(item.identity==='r')item.cpuQuota=0.899999;assert.ok(resourcePreflight({host,roles:insufficient,resourceLimits}).missing.some(code=>code.includes('资源共享后不足')));
 const leaf=structuredClone(roles);leaf.database.nodes.find(row=>row.identity==='d').cpuQuota=0.5;assert.ok(assessCapacityProcessLimits(leaf,resourceLimits).some(code=>code.includes('database')));const constrained={...resourceLimits,processLimits:{...resourceLimits.processLimits,database:{cpuQuota:0.5,memoryLimitBytes:2*GiB}}};assert.deepEqual(assessCapacityProcessLimits(leaf,constrained),[]);assert.ok(resourcePreflight({host,roles:leaf,resourceLimits:constrained}).missing.some(code=>code.includes('CPU亲和性资源不足:database')));
});
