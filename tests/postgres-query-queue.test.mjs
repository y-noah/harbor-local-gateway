import test from 'node:test';
import assert from 'node:assert/strict';
import {getEventListeners} from 'node:events';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
const deferred=()=>{let resolve;const promise=new Promise(r=>{resolve=r;});return {promise,resolve};};
const turns=async()=>{for(let i=0;i<12;i++)await Promise.resolve();};
test('a pre-aborted queue request never acquires a slot or listener',async()=>{
 const queue=createPostgresQueryQueue(),controller=new AbortController(),reason=Error('synthetic cancellation');controller.abort(reason);let invoked=false;await assert.rejects(queue.run(()=>{invoked=true;},{signal:controller.signal}),error=>error===reason);assert.equal(invoked,false);assert.deepEqual(queue.status(),{active:0,queued:0,closed:false});assert.equal(getEventListeners(controller.signal,'abort').length,0);await queue.close();
});
test('waiting cancellation immediately releases capacity and clears its timer/listener',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});const cleared=t.mock.method(globalThis,'clearTimeout'),queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:1,queueTimeoutMs:100}),gate=deferred(),first=queue.run(()=>gate.promise),controller=new AbortController();let called=false;const pending=queue.run(()=>{called=true;},{signal:controller.signal}),rejected=assert.rejects(pending,{name:'AbortError'}),before=cleared.mock.callCount();assert.equal(queue.status().queued,1);controller.abort();await rejected;assert.equal(queue.status().queued,0);assert.equal(cleared.mock.callCount(),before+1);assert.equal(getEventListeners(controller.signal,'abort').length,0);t.mock.timers.tick(1000);gate.resolve();await first;await turns();assert.equal(called,false);await queue.close();
});
test('abort racing a pump cannot execute cancelled waiting work',async()=>{
 const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:1}),gate=deferred(),first=queue.run(()=>gate.promise),controller=new AbortController();let invoked=false;const next=queue.run(()=>{invoked=true;},{signal:controller.signal}),rejected=assert.rejects(next,{name:'AbortError'});gate.resolve();controller.abort();await first;await rejected;await turns();assert.equal(invoked,false);assert.equal(queue.status().active,0);await queue.close();
});
test('abort after admission never reports the underlying active operation as released',async()=>{
 const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:0}),gate=deferred(),started=deferred(),controller=new AbortController(),operation=queue.run(async()=>{started.resolve();await gate.promise;return 42;},{signal:controller.signal});await started.promise;controller.abort();assert.equal(queue.status().active,1);assert.equal(getEventListeners(controller.signal,'abort').length,0);await assert.rejects(queue.run(()=>0),{code:'query_queue_full'});let closed=false;const closing=queue.close().then(()=>{closed=true;});await turns();assert.equal(closed,false);gate.resolve();assert.equal(await operation,42);await closing;assert.equal(queue.status().active,0);
});
test('timeout and close both remove pending cancellation listeners and cannot reject work twice',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});for(const mode of ['timeout','close']){const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:1,queueTimeoutMs:100}),gate=deferred(),first=queue.run(()=>gate.promise),controller=new AbortController(),pending=queue.run(()=>0,{signal:controller.signal}),rejected=assert.rejects(pending,{code:mode==='timeout'?'query_timeout':'query_closed'});let closing;if(mode==='timeout')t.mock.timers.tick(100);else closing=queue.close();await rejected;assert.equal(getEventListeners(controller.signal,'abort').length,0);controller.abort();t.mock.timers.tick(1000);assert.equal(queue.status().queued,0);assert.equal(queue.status().active,1);gate.resolve();await first;await turns();await closing;await queue.close();}
});
test('no-signal FIFO, queue capacity, database timeout mapping and closed contracts stay unchanged',async()=>{
 const queue=createPostgresQueryQueue({maxConcurrent:1,maxQueued:1}),gate=deferred(),order=[],first=queue.run(async()=>{order.push('first');await gate.promise;return 1;}),second=queue.run(()=>{order.push('second');return 2;});await assert.rejects(queue.run(()=>3),{code:'query_queue_full'});gate.resolve();assert.deepEqual(await Promise.all([first,second]),[1,2]);assert.deepEqual(order,['first','second']);await turns();await assert.rejects(queue.run(async()=>{throw {code:'57014'};}),{code:'query_timeout'});await queue.close();await assert.rejects(queue.run(()=>4),{code:'query_closed'});
});
