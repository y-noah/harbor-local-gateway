import test from 'node:test';
import assert from 'node:assert/strict';
import {classifyCapacityDataReadiness} from '../tools/capacity/data-readiness.mjs';
const source=(changes={})=>({status:'ok',ended:100100,recording_state:'complete',persisted_sequence:3,recording_updated:100110,expires_at:999999,turn_state:'known',revision:4,indexed_revision:4,indexed_at:100200,last_error:null,source_revision:4,source_sequence:3,document_state:'complete',document_turn_state:'known',document_expires_at:999999,...changes});
const observe=(row,finishedAt=100,observedAt=300)=>classifyCapacityDataReadiness({row,finishedAt,observedAt,timeOrigin:100000});
test('readiness requires the exact organized source revision, sequence, state and expiry',()=>{
  assert.equal(observe(source()).state,'ready');for(const [key,value] of [['status','running'],['indexed_revision',3],['source_revision',3],['source_sequence',2],['document_state','pending'],['document_turn_state','uncertain'],['document_expires_at',999998],['indexed_at',null]])assert.equal(observe(source({[key]:value})).state,'pending',key);
});
test('a late final collection never renews the per-client ten second deadline',()=>{
  const lateObserved=observe(source(),100,20000);assert.equal(lateObserved.state,'ready');assert.equal(lateObserved.timelinessUnknown,true,'source markers do not prove when a late first query could see the body');const late=observe(source({indexed_at:110101}),100,20000);assert.equal(late.state,'ready');assert.equal(late.timelinessUnknown,true);assert.equal(late.sourceMarkerAfterDeadline,true);assert.equal(late.deadline,10100);assert.equal(observe(source({source_revision:3}),100,10100).state,'timedOut');assert.equal(observe(undefined,100,10100).state,'timedOut');
});
test('partial, expired and terminal errors are explicit instead of successful readiness',()=>{
  for(const recording_state of ['partial','truncated'])assert.equal(observe(source({recording_state})).state,'partial');assert.equal(observe(source({recording_state:'expired'})).state,'expired');assert.equal(observe(source({expires_at:100250})).state,'expired');assert.equal(observe(source({last_error:'index_attempts_exhausted'})).state,'failed');assert.equal(observe(source({status:'failed'})).state,'failed');
});
test('missing or future client finish and missing source timestamps cannot be replaced with now',()=>{
  for(const finishedAt of [undefined,null,NaN,Infinity,-1,301])assert.equal(classifyCapacityDataReadiness({row:source(),finishedAt,observedAt:300,timeOrigin:100000}).state,'unknownFinishedAt');assert.equal(observe(source({ended:null})).state,'pending');
});

test('readiness normalizes an already cancelled empty set and cancellation during a completing query',async()=>{
  const {createCapacityLedgerAdapter}=await import('../tools/capacity/ledger-adapter.mjs'),{randomUUID}=await import('node:crypto');let release,entered;const gate=new Promise(resolve=>release=resolve),query=new Promise(resolve=>entered=resolve),trace=randomUUID(),id=randomUUID(),store={get:async()=>({value:JSON.stringify({active:'synthetic-readiness'})}),pricingBook:async()=>({}),all:async()=>{entered();await gate;return [{...source({ended:Date.now(),recording_updated:Date.now(),indexed_at:Date.now(),expires_at:Date.now()+60000,document_expires_at:Date.now()+60000}),request_id:id}];}},ledger=await createCapacityLedgerAdapter({store,events:[{kind:'gateway-admitted',trace,requestId:id},{kind:'upstream-dispatch',trace}],recordingKey:Buffer.alloc(32)});
  const pre=new AbortController();pre.abort('custom reason');await assert.rejects(ledger.waitForDataReadiness({signal:pre.signal}),{name:'AbortError'});await ledger.onResult({kind:'model',scenario:'readiness-cancel',event:{sequence:0},result:{trace,validatedSuccess:true,finishedAt:performance.now()}});const controller=new AbortController(),waiting=ledger.waitForDataReadiness({signal:controller.signal});await query;controller.abort(new Error('custom reason'));release();await assert.rejects(waiting,{name:'AbortError'});
});

test('successful requests after a fault still require source and derived readiness',async()=>{
  const {createCapacityLedgerAdapter}=await import('../tools/capacity/ledger-adapter.mjs'),{randomUUID}=await import('node:crypto'),trace=randomUUID(),id=randomUUID(),at=Date.now(),store={get:async()=>({value:JSON.stringify({active:'synthetic-readiness'})}),pricingBook:async()=>({}),all:async()=>[{...source({ended:at,recording_updated:at,indexed_at:at,expires_at:at+60000,document_expires_at:at+60000}),request_id:id}]},ledger=await createCapacityLedgerAdapter({store,events:[{kind:'gateway-admitted',trace,requestId:id},{kind:'upstream-dispatch',trace}],recordingKey:Buffer.alloc(32)});await ledger.onResult({kind:'model',scenario:'database-disconnect',event:{sequence:0},result:{trace,validatedSuccess:true,finishedAt:performance.now()}});const result=await ledger.waitForDataReadiness();assert.equal(result.counts.eligible,1);assert.equal(result.counts.ready,1);assert.equal(result.counts.faultExcluded,0);assert.equal(result.passed,true);assert.equal(result.observations.clock,'controller-performance-now');
});
