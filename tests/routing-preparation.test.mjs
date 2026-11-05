import test from 'node:test';
import assert from 'node:assert/strict';
import {createRoutingPreparation} from '../routing-preparation.mjs';

test('preparation reservations are all-or-nothing and waiting readers take ownership before another writer',async()=>{
  const gate=createRoutingPreparation(),first=await gate.enter('a');
  assert.equal(gate.reserve(['a','b']),null);const releaseB=gate.reserve(['b']);assert.ok(releaseB);releaseB();first();first();
  const writer=gate.reserve(['a','a','b']);assert.ok(writer);let ready=false;
  const pending=gate.enter('a').then(release=>{ready=true;return release;});assert.equal(ready,false);assert.equal(gate.status().waiting,1);
  writer();writer();assert.equal(gate.reserve(['a']),null);const reader=await pending;assert.equal(ready,true);assert.equal(gate.isPreparing('a'),true);reader();assert.deepEqual(gate.status(),{readers:0,waiting:0,blockedMembers:0});
});

test('cancelled and rejected waiting requests cannot leak ownership or defeat the wait bound',async()=>{
  const gate=createRoutingPreparation({maxWaiting:1}),writer=gate.reserve(['a']),controller=new AbortController(),pending=gate.enter('a',controller.signal);
  await assert.rejects(gate.enter('a'),{code:'routing_preparation_busy'});controller.abort();await assert.rejects(pending,{code:'request_cancelled'});assert.deepEqual(gate.status(),{readers:0,waiting:0,blockedMembers:1});
  writer();assert.deepEqual(gate.status(),{readers:0,waiting:0,blockedMembers:0});await assert.rejects(gate.enter('a',controller.signal),{code:'request_cancelled'});assert.equal(gate.status().readers,0);
});
