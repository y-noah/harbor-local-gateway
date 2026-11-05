import test from 'node:test';
import assert from 'node:assert/strict';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';
import {registerRequestHandoffGroup} from '../request-handoff-group.mjs';

const deferred=()=>{let resolve,reject;const promise=new Promise((yes,no)=>{resolve=yes;reject=no;});return {promise,resolve,reject};};
const tick=async()=>{for(let i=0;i<16;i++)await Promise.resolve();};
const members=[{memberId:'member',sessionId:'a',decisionId:'decision-a'},{memberId:'member',sessionId:'b',decisionId:'decision-b'}];

test('handoff group waits every old request and preserves every barrier until one real commit',async()=>{
  const gate=createSessionRequestHandoff(),a=gate.arrive('member'),b=gate.arrive('member'),upload=gate.arrive('member');await a.bind('a');await b.bind('b');
  const group=registerRequestHandoffGroup(gate,members),commit=deferred();let calls=0,open=0;
  group.activate(async signal=>{calls++;assert.equal(signal.aborted,false);await commit.promise;return {applied:2};});
  const nextA=gate.arrive('member'),nextB=gate.arrive('member'),waitingA=nextA.bind('a').then(()=>open++),waitingB=nextB.bind('b').then(()=>open++);
  const unrelated=gate.arrive('member');await unrelated.bind('other');const otherMember=gate.arrive('other');await otherMember.bind('a');
  a.close();await upload.bind('other');await tick();assert.equal(group.status().ready,1);assert.equal(calls,0);assert.equal(open,0);
  b.close();await tick();assert.equal(calls,1);assert.equal(open,0);assert.equal(gate.status().pending,2);assert.equal(group.status().applying,true);
  commit.resolve();await Promise.all([waitingA,waitingB]);assert.deepEqual(await group.done,{state:'completed',value:{applied:2}});assert.equal(open,2);assert.equal(calls,1);
  for(const ticket of [a,b,upload,nextA,nextB,unrelated,otherMember])ticket.close();assert.deepEqual(gate.status(),{tickets:0,pending:0,waiting:0,stopped:false});
});

test('handoff group registration rollback releases only its new barriers and preserves an existing decision',async()=>{
  const gate=createSessionRequestHandoff(),existing=gate.register({memberId:'member',sessionId:'b',decisionId:'existing'});
  assert.throws(()=>registerRequestHandoffGroup(gate,members),{code:'session_handoff_conflict'});assert.equal(gate.status().pending,1);assert.equal(existing.status().state,'registering');existing.cancel();await existing.done;assert.equal(gate.status().pending,0);
});

test('handoff group rejects duplicate and invalid members before changing the gate',()=>{
  const gate=createSessionRequestHandoff();
  for(const value of [[],null,[members[0],members[0]],[members[0],{...members[1],decisionId:members[0].decisionId}],[members[0],{...members[1],replaceDecisionId:'other'}],[{memberId:'member',sessionId:'',decisionId:'id'}]])assert.throws(()=>registerRequestHandoffGroup(gate,value),{code:'session_handoff_configuration'});
  assert.deepEqual(gate.status(),{tickets:0,pending:0,waiting:0,stopped:false});
});

test('handoff group capacity failure cannot leave a partial reservation',()=>{
  const gate=createSessionRequestHandoff({maxPending:1});assert.throws(()=>registerRequestHandoffGroup(gate,members),{code:'session_handoff_busy'});assert.equal(gate.status().pending,0);
});

test('handoff group cancellation during partial readiness releases all waiting requests without applying',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('member');await old.bind('b');const group=registerRequestHandoffGroup(gate,members);let calls=0;group.activate(async()=>{calls++;});await tick();assert.equal(group.status().ready,1);
  const nextA=gate.arrive('member'),nextB=gate.arrive('member'),waiting=Promise.all([nextA.bind('a'),nextB.bind('b')]);group.cancel();await waiting;assert.equal((await group.done).state,'cancelled');assert.equal(calls,0);assert.equal(gate.status().pending,0);old.close();nextA.close();nextB.close();assert.equal(gate.status().tickets,0);
});

for(const commitWins of [false,true])test('handoff group cancellation waits the real '+(commitWins?'commit':'rollback')+' before releasing any request',async()=>{
  const gate=createSessionRequestHandoff(),group=registerRequestHandoffGroup(gate,members),transaction=deferred();let entered=false,open=0,signal;
  group.activate(async value=>{signal=value;entered=true;await transaction.promise;if(!commitWins)throw value.reason;return 'committed';});await tick();assert.equal(entered,true);
  const a=gate.arrive('member'),b=gate.arrive('member'),waiting=Promise.all([a.bind('a').then(()=>open++),b.bind('b').then(()=>open++)]);group.cancel();await tick();assert.equal(signal.aborted,true);assert.equal(open,0);assert.equal(gate.status().pending,2);
  transaction.resolve();await waiting;const result=await group.done;assert.equal(result.state,commitWins?'completed':'cancelled');if(commitWins)assert.equal(result.value,'committed');assert.equal(open,2);a.close();b.close();assert.equal(gate.status().tickets,0);
});

test('handoff group failed apply retains its cause and releases all sessions after cleanup',async()=>{
  const gate=createSessionRequestHandoff(),group=registerRequestHandoffGroup(gate,members),failure=Error('synthetic audit failure'),cleanup=deferred();let entered=false,open=false;
  group.activate(async()=>{entered=true;await cleanup.promise;throw failure;});await tick();assert.equal(entered,true);const ticket=gate.arrive('member'),waiting=ticket.bind('a').then(()=>{open=true;});assert.equal(open,false);cleanup.resolve();await waiting;const result=await group.done;assert.equal(result.state,'failed');assert.equal(result.error,failure);assert.equal(gate.status().pending,0);ticket.close();
});

test('handoff group timeout on one member cancels all without losing the timeout cause',async t=>{
  t.mock.timers.enable({apis:['setTimeout']});const gate=createSessionRequestHandoff({waitMs:100,pendingMs:1000}),old=gate.arrive('member');await old.bind('b');const group=registerRequestHandoffGroup(gate,members);let calls=0;group.activate(async()=>{calls++;});await tick();const next=gate.arrive('member'),waiting=assert.rejects(next.bind('a'),{code:'session_handoff_timeout'});
  t.mock.timers.tick(100);await waiting;assert.equal((await group.done).state,'wait_timeout');assert.equal(calls,0);assert.equal(gate.status().pending,0);next.close();old.close();assert.equal(gate.status().waiting,0);
});

test('handoff group shutdown preserves stop reason and does not force old requests to end',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('member');await old.bind('b');const group=registerRequestHandoffGroup(gate,members);group.activate(async()=>assert.fail('cannot commit before the old request ends'));await tick();gate.stop();assert.equal((await group.done).state,'stopped');assert.equal(gate.status().pending,0);assert.equal(gate.status().tickets,1);old.close();assert.equal(gate.status().tickets,0);
});

test('handoff group replacement of one still-draining member cancels its old group without trapping the new decision',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('member');await old.bind('b');const group=registerRequestHandoffGroup(gate,members);let grouped=0,replacementCalls=0;group.activate(async()=>{grouped++;});await tick();assert.equal(group.status().ready,1);
  const replacement=gate.register({...members[1],decisionId:'replacement',replaceDecisionId:'decision-b'});replacement.activate(async()=>{replacementCalls++;});assert.equal((await group.done).state,'superseded');assert.equal(grouped,0);assert.equal(replacement.status().state,'draining');old.close();assert.equal((await replacement.done).state,'completed');assert.equal(replacementCalls,1);assert.equal(gate.status().pending,0);
});

test('handoff group can reserve an entire configured planning-sized group without the old 64-session ceiling',async()=>{
  const gate=createSessionRequestHandoff({maxPending:20000+64}),members=Array.from({length:257},(_,i)=>({memberId:'member',sessionId:'session-'+i,decisionId:'decision-'+i})),group=registerRequestHandoffGroup(gate,members);let calls=0;group.activate(async()=>{calls++;assert.equal(gate.status().pending,257);return 'atomic';});assert.deepEqual(await group.done,{state:'completed',value:'atomic'});assert.equal(calls,1);assert.equal(gate.status().pending,0);
});

test('R40 handoff group cancellation work grows linearly when every member reports its terminal state',async()=>{
  const measure=async count=>{
    const real=createSessionRequestHandoff({maxPending:count}),members=Array.from({length:count},(_,i)=>({memberId:'owner',sessionId:'session-'+i,decisionId:'decision-'+i}));let reads=0;
    const gate={register(member){const handle=real.register(member);return {...handle,get signal(){reads++;return handle.signal;}};}},group=registerRequestHandoffGroup(gate,members),before=reads;
    group.cancel();assert.equal((await group.done).state,'cancelled');assert.equal(real.status().pending,0);return reads-before;
  };
  const small=await measure(100),large=await measure(1000);assert.ok(small>0);assert.equal(large*100,small*1000,'member completion notifications must not rescan the entire cancelled group');
});
