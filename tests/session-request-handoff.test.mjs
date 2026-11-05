import test from 'node:test';
import assert from 'node:assert/strict';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';

const deferred=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};};
const tick=async()=>{for(let n=0;n<8;n++)await Promise.resolve();};
const register=(gate,memberId='a',sessionId='one',decisionId='decision')=>gate.register({memberId,sessionId,decisionId});

test('old uploads and all admitted requests finish before a committed handoff, unrelated sessions continue',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('a'),upload=gate.arrive('a'),otherOld=gate.arrive('a');await old.bind('one');await otherOld.bind('other');
  const intent=register(gate),commit=deferred();let applied=0,newReady=false;
  intent.activate(async()=>{applied++;await commit.promise;});
  const next=gate.arrive('a'),waiting=next.bind('one').then(()=>{newReady=true;});
  const other=gate.arrive('a');await other.bind('other');const anotherMember=gate.arrive('b');await anotherMember.bind('one');
  assert.deepEqual(intent.status(),{state:'draining',oldRequests:2,waiting:1});
  old.close();await tick();assert.equal(applied,0);
  // This body was unknown at registration; binding to a different session
  // removes it from the old cohort without waiting for that other model.
  await upload.bind('other');await tick();assert.equal(applied,1);assert.equal(newReady,false);
  commit.resolve();await waiting;assert.equal(newReady,true);assert.deepEqual(await intent.done,{state:'completed'});
  for(const ticket of [old,upload,otherOld,next,other,anotherMember]){ticket.close();ticket.close();}
  assert.deepEqual(gate.status(),{tickets:0,pending:0,waiting:0,stopped:false});
});

test('a slow pre-registration upload keeps its original route even after identifying the target session',async()=>{
  const gate=createSessionRequestHandoff(),upload=gate.arrive('a'),intent=register(gate);let applied=0;
  intent.activate(async()=>{applied++;});await upload.bind('one');await tick();assert.equal(applied,0);
  upload.close();assert.deepEqual(await intent.done,{state:'completed'});assert.equal(applied,1);assert.equal(gate.status().pending,0);
});

test('registering does not apply before durable commit and rollback releases only new requests',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('a');await old.bind('one');const intent=register(gate),next=gate.arrive('a');let ready=false;
  const wait=next.bind('one').then(()=>{ready=true;});old.close();await tick();assert.equal(ready,false);assert.equal(intent.status().state,'registering');
  assert.equal(intent.cancel(),true);await wait;assert.deepEqual(await intent.done,{state:'cancelled'});next.close();assert.equal(gate.status().tickets,0);
});

test('cancelled applying transaction retains its barrier through actual rollback',async()=>{
  const gate=createSessionRequestHandoff(),intent=register(gate),commit=deferred();let signal;
  intent.activate(async current=>{signal=current;await commit.promise;});await tick();assert.ok(signal);const next=gate.arrive('a');let ready=false;
  const wait=next.bind('one').then(()=>{ready=true;});intent.cancel();await tick();assert.equal(signal.aborted,true);assert.equal(ready,false);
  assert.throws(()=>register(gate),{code:'session_handoff_conflict'});commit.reject(Error('synthetic rollback'));await wait;assert.deepEqual(await intent.done,{state:'cancelled'});next.close();
});

test('a request cancelled while waiting never becomes an old request or leaves capacity behind',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('a'),intent=register(gate),controller=new AbortController(),next=gate.arrive('a',controller.signal);let applied=0;
  intent.activate(async()=>{applied++;});const wait=next.bind('one');controller.abort();await assert.rejects(wait,{code:'request_cancelled'});
  assert.equal(gate.status().waiting,0);assert.equal(gate.status().tickets,2);next.close();old.close();await intent.done;assert.equal(applied,1);assert.equal(gate.status().tickets,0);
});

test('aborting an old model does not hand off until its actual cleanup completes',async()=>{
  const gate=createSessionRequestHandoff(),controller=new AbortController(),old=gate.arrive('a',controller.signal);await old.bind('one');const intent=register(gate);let applied=0;
  intent.activate(async()=>{applied++;});controller.abort();await tick();assert.equal(applied,0);assert.equal(intent.status().oldRequests,1);
  old.close();await intent.done;assert.equal(applied,1);assert.equal(gate.status().tickets,0);
});

test('waiting bounds cancel a draining handoff so old-source requests can continue',async()=>{
  const gate=createSessionRequestHandoff({maxWaitingPerSession:1}),old=gate.arrive('a'),intent=register(gate),first=gate.arrive('a'),second=gate.arrive('a');let applied=0;
  intent.activate(async()=>{applied++;});const wait=first.bind('one');await second.bind('one');await wait;assert.deepEqual(await intent.done,{state:'wait_capacity'});assert.equal(applied,0);
  for(const ticket of [old,first,second])ticket.close();assert.equal(gate.status().waiting,0);
});

test('wait timeout cancels a draining decision without interrupting the old request',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const gate=createSessionRequestHandoff({waitMs:100,pendingMs:1000}),old=gate.arrive('a'),intent=register(gate),next=gate.arrive('a');
  intent.activate(async()=>{assert.fail('old request still active');});const wait=next.bind('one');t.mock.timers.tick(100);await wait;assert.deepEqual(await intent.done,{state:'wait_timeout'});
  assert.equal(gate.status().tickets,2);old.close();next.close();assert.equal(gate.status().waiting,0);
});

test('a timed-out applying waiter exits but later requests cannot race its unresolved transaction',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const gate=createSessionRequestHandoff({waitMs:100,pendingMs:1000,maxWaitingPerSession:1}),intent=register(gate),commit=deferred();
  intent.activate(async()=>commit.promise);await tick();const next=gate.arrive('a'),wait=next.bind('one');t.mock.timers.tick(100);await assert.rejects(wait,{code:'session_handoff_timeout'});assert.equal(gate.status().pending,1);next.close();
  const final=gate.arrive('a'),lastWait=final.bind('one'),excess=gate.arrive('a');await assert.rejects(excess.bind('one'),{code:'session_handoff_busy'});
  commit.reject(Error('synthetic rollback'));await lastWait;await intent.done;final.close();excess.close();assert.equal(gate.status().pending,0);
});

test('pending expiry and shutdown release waits, applying failures do not leave a barrier',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const gate=createSessionRequestHandoff({waitMs:100,pendingMs:500}),old=gate.arrive('a'),intent=register(gate);intent.activate(async()=>{assert.fail();});
  t.mock.timers.tick(500);assert.deepEqual(await intent.done,{state:'expired'});old.close();
  const failed=register(gate);failed.activate(async()=>{throw Error('synthetic transaction failure');});assert.deepEqual(await failed.done,{state:'failed'});
  const pending=register(gate,'b'),next=gate.arrive('b'),wait=next.bind('one');gate.stop();await wait;assert.deepEqual(await pending.done,{state:'stopped'});next.close();assert.throws(()=>gate.arrive('a'),{code:'service_stopping'});
  assert.deepEqual(gate.status(),{tickets:0,pending:0,waiting:0,stopped:true});
});

test('resource and identity validation reject unsafe configuration and do not expose identifiers in status',async()=>{
  for(const options of [{maxTickets:0},{maxPending:1.5},{maxWaitingPerMember:0},{waitMs:120001},{pendingMs:1}])assert.throws(()=>createSessionRequestHandoff(options),{code:'session_handoff_configuration'});
  const gate=createSessionRequestHandoff({maxTickets:1,maxPending:1});assert.throws(()=>gate.arrive(''),{code:'session_handoff_configuration'});
  const ticket=gate.arrive('synthetic-private-member');assert.throws(()=>gate.arrive('b'),{code:'session_handoff_busy'});
  const intent=register(gate,'synthetic-private-member');assert.throws(()=>register(gate,'b'),{code:'session_handoff_busy'});assert.equal(JSON.stringify(gate.status()).includes('synthetic-private'),false);
  ticket.close();await assert.rejects(ticket.bind('one'),{code:'request_cancelled'});intent.cancel();await intent.done;
});

test('a replacement preserves the old cohort and existing wait deadlines without leaking or releasing requests',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const gate=createSessionRequestHandoff({maxPending:1,waitMs:100,pendingMs:1000}),old=gate.arrive('a');await old.bind('one');
  const first=register(gate),next=gate.arrive('a');let ready=false,applied=0;const wait=next.bind('one').then(()=>{ready=true;});first.activate(async()=>{assert.fail('superseded decision');});t.mock.timers.tick(60);
  const second=gate.register({memberId:'a',sessionId:'one',decisionId:'new',replaceDecisionId:'decision'});second.activate(async()=>{applied++;});assert.deepEqual(await first.done,{state:'superseded'});assert.equal(first.cancel(),false);
  assert.deepEqual(second.status(),{state:'draining',oldRequests:1,waiting:1});await tick();assert.equal(ready,false);t.mock.timers.tick(40);await wait;assert.deepEqual(await second.done,{state:'wait_timeout'});
  assert.equal(applied,0);assert.equal(gate.status().waiting,0);old.close();next.close();
});

test('the latest replacement applies once after old requests end, with original waiters using the new decision',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('a'),first=register(gate),next=gate.arrive('a'),wait=next.bind('one');let applied='';
  first.activate(async()=>{applied='old';});const latest=gate.register({memberId:'a',sessionId:'one',decisionId:'new',replaceDecisionId:'decision'});latest.activate(async()=>{applied='new';});
  old.close();await wait;assert.deepEqual(await latest.done,{state:'completed'});assert.equal(applied,'new');assert.equal(latest.status().state,'completed');next.close();assert.equal(gate.status().waiting,0);
});

test('a cancellation racing a successful commit cannot report the actual new binding as cancelled',async()=>{
  const gate=createSessionRequestHandoff(),intent=register(gate),committed=deferred();intent.activate(async()=>committed.promise);await tick();intent.cancel();
  committed.resolve();assert.deepEqual(await intent.done,{state:'completed'});assert.equal(intent.status().state,'completed');assert.equal(gate.status().pending,0);
});

test('rejected replacement restores the accepted callback and its original request cohort without releasing waiters',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('a'),first=register(gate),next=gate.arrive('a');let applied=0,ready=false;
  first.activate(async()=>{applied++;});const wait=next.bind('one').then(()=>{ready=true;}),replacement=gate.register({memberId:'a',sessionId:'one',decisionId:'new',replaceDecisionId:'decision'});
  assert.equal(first.status().state,'suspended');old.close();await tick();assert.equal(applied,0);assert.equal(ready,false);
  replacement.cancel();assert.deepEqual(await replacement.done,{state:'cancelled'});await wait;assert.deepEqual(await first.done,{state:'completed'});assert.equal(applied,1);next.close();assert.equal(gate.status().pending,0);
});

test('a rejected replacement retains the original waiter timeout and shutdown cancels both reservations',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const gate=createSessionRequestHandoff({waitMs:100,pendingMs:1000}),old=gate.arrive('a'),first=register(gate),next=gate.arrive('a');first.activate(async()=>{assert.fail();});const wait=next.bind('one');
  t.mock.timers.tick(60);const replacement=gate.register({memberId:'a',sessionId:'one',decisionId:'new',replaceDecisionId:'decision'});replacement.cancel();t.mock.timers.tick(40);await wait;assert.deepEqual(await first.done,{state:'wait_timeout'});old.close();next.close();
  const held=gate.arrive('a'),third=register(gate);third.activate(async()=>{});const pending=gate.register({memberId:'a',sessionId:'one',decisionId:'third',replaceDecisionId:'decision'});gate.stop();assert.deepEqual(await third.done,{state:'stopped'});assert.deepEqual(await pending.done,{state:'stopped'});held.close();assert.equal(gate.status().pending,0);
});

test('R38 original expiry while replacement is registering cancels both reservations without reviving a stale callback',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const gate=createSessionRequestHandoff({waitMs:200,pendingMs:1000}),old=gate.arrive('a'),first=register(gate);let applied=0;first.activate(async()=>{applied++;});
  t.mock.timers.tick(990);const next=gate.arrive('a'),wait=next.bind('one'),replacement=gate.register({memberId:'a',sessionId:'one',decisionId:'new',replaceDecisionId:'decision'});assert.equal(gate.status().waiting,1);
  t.mock.timers.tick(10);assert.deepEqual(await first.done,{state:'expired'});assert.deepEqual(await replacement.done,{state:'expired'});await wait;assert.throws(()=>replacement.activate(async()=>{applied++;}),{code:'session_handoff_conflict'});assert.equal(gate.status().pending,0);assert.equal(gate.status().waiting,0);
  old.close();next.close();await tick();assert.equal(applied,0);assert.equal(gate.status().tickets,0);
});
