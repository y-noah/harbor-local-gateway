import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdminRequestTiming} from '../admin-request-timing.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';

const deferred=()=>{let resolve;const promise=new Promise(value=>resolve=value);return {promise,resolve};};
const drain=async(queue,expected)=>{const deadline=Date.now()+2000;while(queue.status().active!==expected){assert.ok(Date.now()<deadline,'bounded queue state');await new Promise(resolve=>setImmediate(resolve));}};
function response(){const headers=new Map();return {headers,getHeader:name=>headers.get(name),setHeader:(name,value)=>headers.set(name,value)};}
function setup(options={}){let at=0;const timing=createAdminRequestTiming({clock:()=>at}),res=response(),controller=new AbortController(),base=createPostgresQueryQueue(options),queue=timing.wrapQueue(base);assert.equal(timing.begin({method:'GET',url:'/api/admin/test'},res),true);assert.equal(timing.registerSignal(res,controller.signal),true);return {timing,res,controller,base,queue,clock:value=>at=value};}
function metrics(value){return Object.fromEntries(value.split(', ').map(item=>{const [name,duration]=item.split(';dur=');return [name,Number(duration)];}));}

test('queue wait and actual operation settlement are separately observed without changing values',async()=>{
 const s=setup({maxConcurrent:1}),blocker=deferred(),work=deferred();const first=s.base.run(()=>blocker.promise);s.clock(5);const second=s.queue.run(()=>work.promise,{signal:s.controller.signal});assert.equal(s.queue.status().queued,1);s.clock(15);blocker.resolve();await first;await drain(s.queue,1);await new Promise(resolve=>setImmediate(resolve));s.clock(35);work.resolve('actual-result');assert.equal(await second,'actual-result');await drain(s.queue,0);s.clock(40);assert.equal(s.timing.finish(s.res,200,{authenticated:true}),true);assert.deepEqual(metrics(s.res.getHeader('Server-Timing')),{app:40,qwait:10,qwork:20});await s.queue.close();
});

test('overlapping waiting and executing operations use interval unions, not duration sums',async()=>{
 const s=setup({maxConcurrent:1}),blocker=deferred(),one=deferred(),two=deferred();const initial=s.base.run(()=>blocker.promise);s.clock(5);const a=s.queue.run(()=>one.promise,{signal:s.controller.signal});s.clock(10);const b=s.queue.run(()=>two.promise,{signal:s.controller.signal});s.clock(20);blocker.resolve();await initial;await new Promise(resolve=>setImmediate(resolve));s.clock(30);one.resolve();await a;await new Promise(resolve=>setImmediate(resolve));s.clock(40);two.resolve();await b;await drain(s.queue,0);s.clock(50);s.timing.finish(s.res,200,{authenticated:true});assert.deepEqual(metrics(s.res.getHeader('Server-Timing')),{app:50,qwait:25,qwork:20});await s.queue.close();
});

test('parallel execution intervals are also unions',async()=>{
 const s=setup({maxConcurrent:2}),one=deferred(),two=deferred();s.clock(5);const a=s.queue.run(()=>one.promise,{signal:s.controller.signal});s.clock(10);const b=s.queue.run(()=>two.promise,{signal:s.controller.signal});s.clock(25);one.resolve();await a;s.clock(30);two.resolve();await b;await drain(s.queue,0);s.clock(40);s.timing.finish(s.res,200,{authenticated:true});assert.deepEqual(metrics(s.res.getHeader('Server-Timing')),{app:40,qwait:0,qwork:25});await s.queue.close();
});

test('full queues and operation failures retain their original rejection and capacity',async()=>{
 const s=setup({maxConcurrent:1,maxQueued:0}),gate=deferred(),first=s.base.run(()=>gate.promise);await assert.rejects(s.queue.run(()=>1,{signal:s.controller.signal}),{code:'query_queue_full'});assert.equal(s.queue.status().active,1);gate.resolve();await first;await drain(s.queue,0);const failure=Error('synthetic-operation-failure');await assert.rejects(s.queue.run(()=>{throw failure;},{signal:s.controller.signal}),error=>error===failure);await drain(s.queue,0);assert.equal(s.queue.status().queued,0);await s.queue.close();
});

test('queued cancellation removes waiting work while in-flight cancellation retains its real slot',async()=>{
 const s=setup({maxConcurrent:1,maxQueued:1}),gate=deferred();const first=s.queue.run(()=>gate.promise,{signal:s.controller.signal});const queuedController=new AbortController();s.timing.registerSignal(s.res,queuedController.signal);const rejected=assert.rejects(s.queue.run(()=>assert.fail('cancelled operation admitted'),{signal:queuedController.signal}),error=>error===queuedController.signal.reason);queuedController.abort(Error('synthetic-queued-cancel'));await rejected;assert.equal(s.queue.status().queued,0);s.controller.abort(Error('synthetic-active-cancel'));assert.equal(s.queue.status().active,1);gate.resolve('still-completed');assert.equal(await first,'still-completed');await drain(s.queue,0);await s.queue.close();
});

test('unbound signals, non-admin candidates and unauthenticated responses expose no queue metrics',async()=>{
 const timing=createAdminRequestTiming(),base=createPostgresQueryQueue(),queue=timing.wrapQueue(base),nonAdmin=response(),res=response(),signal=new AbortController().signal;assert.equal(timing.begin({method:'GET',url:'/health'},nonAdmin),false);assert.equal(timing.registerSignal(nonAdmin,signal),false);timing.begin({method:'GET',url:'/api/admin/test'},res);assert.equal(await queue.run(()=>42,{signal}),42);assert.equal(timing.finish(res,200),false);assert.equal(res.getHeader('Server-Timing'),undefined);assert.equal(timing.finish(res,200,{authenticated:true}),true);assert.deepEqual(Object.keys(metrics(res.getHeader('Server-Timing'))),['app']);await queue.close();
});

test('throwing and invalid observation clocks never fail operations or strand queue slots',async()=>{
 let broken=false,at=0;const timing=createAdminRequestTiming({clock:()=>{if(broken)throw Error('synthetic-clock-failure');return at;}}),res=response(),signal=new AbortController().signal,base=createPostgresQueryQueue(),queue=timing.wrapQueue(base);timing.begin({method:'GET',url:'/api/admin/test'},res);timing.registerSignal(res,signal);broken=true;assert.equal(await queue.run(()=>42,{signal}),42);await drain(queue,0);assert.equal(timing.finish(res,200,{authenticated:true}),false);broken=false;at=20;assert.equal(timing.finish(res,200,{authenticated:true}),true);assert.deepEqual(metrics(res.getHeader('Server-Timing')),{app:20});await queue.close();
 const invalid=createAdminRequestTiming({clock:()=>NaN}),other=response();assert.equal(invalid.begin({method:'GET',url:'/api/admin/test'},other),false);
});

test('queue wrappers forward close/status and preserve existing app plus unrelated metrics',async()=>{
 const s=setup();s.res.setHeader('Server-Timing','cache;dur=2, app;dur=3');await s.queue.run(()=>true,{signal:s.controller.signal});await drain(s.queue,0);s.clock(10);assert.equal(s.timing.finish(s.res,200,{authenticated:true}),true);assert.deepEqual(metrics(s.res.getHeader('Server-Timing')),{cache:2,app:3,qwait:0,qwork:0});assert.equal(s.timing.finish(s.res,200,{authenticated:true}),false);await s.queue.close();assert.equal(s.base.status().closed,true);
});

test('registration and response header observation failures are fail-open',async()=>{
 const timing=createAdminRequestTiming(),res=response(),base=createPostgresQueryQueue(),queue=timing.wrapQueue(base);timing.begin({method:'GET',url:'/api/admin/test'},res);
 assert.equal(timing.registerSignal(res,{get addEventListener(){throw Error('synthetic-observation-getter');}}),false);
 assert.equal(await queue.run(()=>17),17);await drain(queue,0);res.setHeader=()=>{throw Error('synthetic-header-observation');};assert.equal(timing.finish(res,200,{authenticated:true}),false);await queue.close();
});
