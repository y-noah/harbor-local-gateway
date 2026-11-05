import test from 'node:test';
import assert from 'node:assert/strict';
import {inspectStorageOperatorActivationProof,withVerifiedStorageOperatorActivationProof} from '../storage-operator-activation-proof.mjs';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {fileURLToPath} from 'node:url';

test('caller JSON, status receipts and copied summaries cannot mint the live activation qualification',async()=>{
 for(const proof of [undefined,{}, {scope:'docker-systemd',deploymentAllowed:true},{baselineVerified:true,offhostVerified:true},{runId:'synthetic',candidateVerified:true}]){
  await assert.rejects(inspectStorageOperatorActivationProof(proof),{code:'storage_operator_activation_proof_required'});
 }
 for(const context of [undefined,{}, {scope:'docker-systemd'}])await assert.rejects(withVerifiedStorageOperatorActivationProof({context},()=>assert.fail('forged context consumed')),{code:'storage_operator_context_required'});
});

for(const scenario of ['valid','late-inspection','changed-material','nonempty-devices'])test('actual qualification lifetime and cryptographic projection with synthetic external IO: '+scenario,{timeout:15000},async()=>{
 const result=await promisify(execFile)(process.execPath,['--experimental-vm-modules',fileURLToPath(new URL('./operator-activation-proof-lifecycle-fixture.mjs',import.meta.url)),scenario],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(result.stdout),{scenario,privateLifetimeVerified:true,externalIoSynthetic:true,formalPassed:false});
});
