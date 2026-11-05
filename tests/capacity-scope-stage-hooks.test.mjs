import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {createCapacityObservationAdapter,observeCapacityHttp} from '../tools/capacity/observation-adapter.mjs';
const deferred=()=>{let resolve;const promise=new Promise(done=>{resolve=done;});return {promise,resolve};};

function fixture({finishObservation,dispatch}={}){
  const rebound=[],ownedObserver=Object.freeze({owner:'private-scope'}),forgedObserver=Object.freeze({owner:'caller'}),original=randomUUID();
  const ledger={clientStageObserver:ownedObserver,rebindTrace:(before,after)=>rebound.push({before,after}),onResult:async()=>{},collect:async()=>({passed:null}),...(finishObservation?{finishObservation}:{})};
  const send=source=>observeCapacityHttp({path:source==='model'?'/v1/responses':'/ready',method:source==='model'?'POST':'GET',headers:{'x-capacity-trace':original},...(source==='model'?{clientStageObserver:forgedObserver}:{})},async options=>{await dispatch?.(options,{source,ownedObserver,forgedObserver,rebound,original});return {status:200,httpEnded:true,success:true};});
  const adapter=createCapacityObservationAdapter({sendModel:()=>send('model'),sendAdmin:()=>send('admin'),createLedger:async()=>ledger,employeeIdentities:[{memberId:randomUUID(),keyId:randomUUID(),deviceId:randomUUID()}],settleTimeoutMs:1});
  return {adapter,ownedObserver,original,rebound};
}

test('model transport receives the private scope observer after final trace rebinding',async()=>{
  let models=0,administrators=0;
  const {adapter}=fixture({dispatch:(options,{source,ownedObserver,forgedObserver,rebound,original})=>{
    if(source==='model'){
      models++;assert.equal(options.clientStageObserver,ownedObserver);assert.notEqual(options.clientStageObserver,forgedObserver);
      assert.deepEqual(rebound,[{before:original,after:options.headers['x-capacity-trace']}]);
    }else{administrators++;assert.equal(Object.hasOwn(options,'clientStageObserver'),false);}
  }});
  try{const scope=await adapter.createObservationScope({id:'scope-stage-cap',kind:'normal'});await scope.sendModel({actorIndex:0});await scope.sendAdmin({actorIndex:0});assert.equal(models,1);assert.equal(administrators,1);}finally{await adapter.close();}
});

test('scope close waits for stage finalization and remains retryable when sources are still active',async()=>{
  let complete=false,calls=0;const {adapter}=fixture({finishObservation:async()=>{calls++;return complete;}});
  try{
    const scope=await adapter.createObservationScope({id:'scope-stage-close',kind:'normal'});
    await assert.rejects(scope.close(),/capacity_observation_stage_pending/);
    assert.equal((await scope.collect()).scope.closed,false);assert.equal(calls,1);
    complete=true;await scope.close();assert.equal((await scope.collect()).scope.closed,true);assert.equal(calls,2);
  }finally{complete=true;await adapter.close();}
});

test('adapter close retries unfinished stage hooks without closing their source prematurely',async()=>{
  let complete=false,calls=0;const {adapter}=fixture({finishObservation:async()=>{calls++;return complete;}});
  const scope=await adapter.createObservationScope({id:'adapter-stage-close',kind:'normal'});
  try{
    await assert.rejects(adapter.close(),/capacity_observation_stage_pending/);
    assert.equal((await scope.collect()).scope.closed,false);
    complete=true;await adapter.close();assert.equal((await scope.collect()).scope.closed,true);assert.equal(calls,2);
  }finally{complete=true;await adapter.close();}
});

test('a ledger created during shutdown remains in the retryable stage barrier before its own close',async()=>{
  const creation=deferred();let complete=false,stageCalls=0,ledgerCloses=0;
  const ledger={collect:async()=>({passed:null}),onResult:async()=>{},finishObservation:async()=>{stageCalls++;return complete;},close:async()=>{ledgerCloses++;}};
  const adapter=createCapacityObservationAdapter({sendModel:()=>{},sendAdmin:()=>{},createLedger:()=>creation.promise,settleTimeoutMs:1});
  const building=adapter.createObservationScope({id:'created-while-closing',kind:'normal'}),creationRejected=assert.rejects(building,/capacity_observation_scope_closed/);
  const firstClose=assert.rejects(adapter.close(),/capacity_observation_stage_pending/);creation.resolve(ledger);
  await Promise.all([creationRejected,firstClose]);assert.equal(stageCalls,1);assert.equal(ledgerCloses,0);
  assert.doesNotThrow(()=>adapter.observeWire({kind:'gateway-request-start',trace:null,wireId:randomUUID(),signature:'GET /ready',credentials:{cookie:null,authorization:null},at:Date.now()}));
  complete=true;await adapter.close();assert.equal(stageCalls,2);assert.equal(ledgerCloses,1);
});

test('concurrent scope and adapter close share one stage attempt and retry the actual hook after failure',async()=>{
  const entered=deferred(),release=deferred();let complete=false,calls=0;
  const {adapter}=fixture({finishObservation:async()=>{calls++;entered.resolve();return complete||release.promise;}});
  const scope=await adapter.createObservationScope({id:'concurrent-stage-close',kind:'normal'});
  const first=assert.rejects(scope.close(),/capacity_observation_stage_pending/);await entered.promise;
  const second=assert.rejects(adapter.close(),/capacity_observation_stage_pending/);release.resolve(false);
  await Promise.all([first,second]);assert.equal(calls,1);assert.equal((await scope.collect()).scope.closed,false);
  complete=true;await adapter.close();assert.equal(calls,2);assert.equal((await scope.collect()).scope.closed,true);
});
