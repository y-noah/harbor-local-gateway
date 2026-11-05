import test from 'node:test';
import assert from 'node:assert/strict';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';
import {registerRequestHandoffGroup} from '../request-handoff-group.mjs';
import {planBalanceWorkloads} from '../balancing.mjs';

const tick=async()=>{for(let n=0;n<24;n++)await Promise.resolve();};
const identities=[{memberId:'owner',sessionId:'ready',decisionId:'automatic-ready'},{memberId:'owner',sessionId:'busy',decisionId:'automatic-busy'}];

test('manual registration suspends an already-ready automatic member and a rejected replacement restores the same group',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('owner');await old.bind('busy');const group=registerRequestHandoffGroup(gate,identities);let applied=0;
  group.activate(async()=>{applied++;});await tick();assert.equal(group.status().ready,1);
  const waiter=gate.arrive('owner');let opened=false;const waiting=waiter.bind('ready').then(()=>{opened=true;});
  const manual=gate.register({memberId:'owner',sessionId:'ready',decisionId:'manual',replaceDecisionId:'automatic-ready'});old.close();await tick();assert.equal(group.status().ready,2);assert.equal(applied,0);assert.equal(opened,false);
  manual.cancel();assert.equal((await manual.done).state,'cancelled');await waiting;assert.equal((await group.done).state,'completed');assert.equal(applied,1);waiter.close();assert.equal(gate.status().pending,0);assert.equal(gate.status().waiting,0);
});
test('accepted manual replacement of a ready automatic member cancels the original group without cancelling the manual decision',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('owner');await old.bind('busy');const group=registerRequestHandoffGroup(gate,identities);let grouped=0,manually=0;
  group.activate(async()=>{grouped++;});await tick();const manual=gate.register({memberId:'owner',sessionId:'ready',decisionId:'manual',replaceDecisionId:'automatic-ready'});manual.activate(async()=>{manually++;});
  assert.equal((await group.done).state,'superseded');assert.equal((await manual.done).state,'completed');assert.equal(grouped,0);assert.equal(manually,1);old.close();assert.equal(gate.status().pending,0);
});
test('manual cannot replace an automatic group after its actual commit callback has begun',async()=>{
  const gate=createSessionRequestHandoff(),group=registerRequestHandoffGroup(gate,identities);let release;group.activate(async()=>new Promise(resolve=>{release=resolve;}));await tick();assert.equal(group.status().applying,true);
  assert.throws(()=>gate.register({memberId:'owner',sessionId:'ready',decisionId:'manual',replaceDecisionId:'automatic-ready'}),{code:'session_handoff_conflict'});release();assert.equal((await group.done).state,'completed');
});
test('a failed accepted manual takeover restores the original automatic group only after durable restoration settles',async()=>{
  const gate=createSessionRequestHandoff(),group=registerRequestHandoffGroup(gate,identities);let releaseCheck,releaseRestore,restoring=false,applied=0;
  group.activate(async(_signal,beginCommit)=>{await new Promise(resolve=>{releaseCheck=resolve;});await beginCommit();applied++;},{deferCommit:true});await tick();
  const manual=gate.register({memberId:'owner',sessionId:'ready',decisionId:'manual',replaceDecisionId:'automatic-ready',retainPrevious:true,beforeRestore:async()=>{restoring=true;await new Promise(resolve=>{releaseRestore=resolve;});}});
  const next=gate.arrive('owner');let opened=false;const waiting=next.bind('ready').then(()=>{opened=true;});manual.activate(async()=>{throw Error('synthetic unsupported model');});releaseCheck();await tick();assert.equal(restoring,true);assert.equal(applied,0);assert.equal(opened,false);
  releaseRestore();assert.equal((await manual.done).state,'failed');await waiting;assert.equal((await group.done).state,'completed');assert.equal(applied,1);next.close();assert.equal(gate.status().pending,0);
});
test('disabling an automatic group leaves its accepted manual takeover alive',async()=>{
  const gate=createSessionRequestHandoff(),old=gate.arrive('owner');await old.bind('ready');const group=registerRequestHandoffGroup(gate,identities);group.activate(async()=>assert.fail('automatic group must not apply'));
  const manual=gate.register({memberId:'owner',sessionId:'ready',decisionId:'manual',replaceDecisionId:'automatic-ready',retainPrevious:true});let applied=0;manual.activate(async()=>{applied++;});group.cancel();assert.equal((await group.done).state,'cancelled');assert.equal(manual.status().state,'draining');old.close();assert.equal((await manual.done).state,'completed');assert.equal(applied,1);
});
test('request-boundary planning considers active Responses without changing measured workload or legacy policy',()=>{
  const at=Date.parse('2026-11-03T10:00:01+08:00'),accounts=['a','b'].map(id=>({id,kind:'codex',auth_mode:'subscription',enabled:1,removed_at:null,state:'ready',route_block:null,route_epoch:0,quota_json:JSON.stringify({plan:'same',checked:at,windows:[{minutes:10080,used:50,reset:at+86400000}],limitReached:false})}));
  const workloads=[60,40,40,20].map((amount,index)=>({id:'s'+index,member_id:'owner',account_id:index<2?'a':'b',member_enabled:1,member_removed:null,protocol:'responses',enabled:1,nightly_enabled:1,route_version:0,created:index,measuredAtoms:BigInt(amount)*1000000000000n,estimatedAtoms:0,unknown:false,running:true,afterBoundary:true,last:at}));
  const original=structuredClone(workloads),legacy=planBalanceWorkloads({accounts,workloads,at}),boundary=planBalanceWorkloads({accounts,workloads,at,requestBoundary:true});
  assert.equal(legacy.moves.length,0);assert.ok(boundary.moves.length>0);assert.deepEqual(workloads,original);assert.ok(boundary.summaries.every(group=>group.fixed.length===0));
  const unknown=workloads.map((row,index)=>index===0?{...row,unknown:true}:row);assert.equal(planBalanceWorkloads({accounts,workloads:unknown,at,requestBoundary:true}).moves.length,0);
});
