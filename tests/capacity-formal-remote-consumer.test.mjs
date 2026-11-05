import test from 'node:test';
import assert from 'node:assert/strict';
import {inspectCapacityRemotePrerequisites,inspectCapacityProof,activateCapacityRemoteBrowserProfile,inspectCapacityRemoteConsumption} from '../tools/capacity/formal-remote-proof.mjs';
import {capacityRemoteBrowserActivationTemplate,capacityRemoteBrowserActivation} from '../tools/capacity/remote-browser-profile.mjs';
test('remote consumer contracts do not mint factory proof and copied capability DTOs cannot activate or report execution',async()=>{
 const result=await inspectCapacityRemotePrerequisites();assert.deepEqual(result.softwareMissing,[]);assert.equal(result.evidence.factoryProofRequired,true);assert.equal(result.evidence.actualExecutionVerified,false);
 for(const handle of [{},Object.freeze({scope:'root-native-offhost-control-lease-only',passed:true})]){
  await assert.rejects(inspectCapacityProof(handle,{}, {phase:'final'}),/capability_not_owned/);
  await assert.rejects(activateCapacityRemoteBrowserProfile({capability:handle,driver:{},publication:{}}),/capability_not_owned/);
  await assert.rejects(inspectCapacityRemoteConsumption(handle),/capability_not_owned/);
 }
});
test('browser activation defaults closed and cannot carry network policy, executable paths or fewer than five distinct sessions',()=>{
 assert.equal(capacityRemoteBrowserActivationTemplate(),null);
 const template={id:'factory',origin:'https://approved.invalid',serverSpkiHash:Buffer.alloc(32,1).toString('base64')};assert.equal(capacityRemoteBrowserActivationTemplate(template).deepPages,100);
 for(const extra of [{binary:'/bin/sh'},{module:'/tmp/unapproved.mjs'},{url:'https://other.invalid'}])assert.throws(()=>capacityRemoteBrowserActivationTemplate({...template,...extra}),/activation_invalid/);
 const activation={bindingTag:'current',epoch:1,cookies:Array.from({length:5},(_,n)=>({name:'harbor_session',value:'synthetic-'+n}))};assert.equal(capacityRemoteBrowserActivation(activation).cookies.length,5);
 for(const extra of [{origin:template.origin},{serverSpkiHash:template.serverSpkiHash},{script:'anything'},{cookies:activation.cookies.slice(0,4)},{cookies:Array(5).fill(activation.cookies[0])}])assert.throws(()=>capacityRemoteBrowserActivation({...activation,...extra}),/activation_invalid/);
});
