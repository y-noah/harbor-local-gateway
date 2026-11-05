import test from 'node:test';
import assert from 'node:assert/strict';
import {createReceiverRestoredCandidate,inspectReceiverRestoredCandidate,receiverRestoredCandidateBinding,verifyReceiverCandidateDescription} from '../storage-receiver-candidate.mjs';
import {createReceiverCandidateProcesses} from '../storage-receiver-candidate-processes.mjs';
import {createTaskRestoredCandidate} from '../storage-restored-candidate.mjs';

test('formal receiver candidate authority cannot be supplied as a task token, copied fields, verified DTO or arbitrary descriptor',async()=>{
 for(const value of [undefined,{}, {scope:'task-restored-oci-runtime',formalQualified:true},{scope:'root-profile-restored-oci-runtime',formalReceiverAuthority:true}]){assert.throws(()=>inspectReceiverRestoredCandidate(value),{code:'backup_receiver_candidate_required'});assert.throws(()=>receiverRestoredCandidateBinding(value),{code:'backup_receiver_candidate_required'});assert.throws(()=>createReceiverCandidateProcesses(value,{protectedRoot:'/tmp'}),{code:'backup_receiver_candidate_required'});await assert.rejects(createReceiverRestoredCandidate(value),{code:'backup_receiver_capability_required'});}
 assert.equal(typeof createTaskRestoredCandidate,'function');
});
test('candidate metadata alone cannot bypass immutable approved root materials',async()=>{
 for(const value of [undefined,{}, {version:1,business:{revision:'a'.repeat(40),manifestDigest:'sha256:'+'a'.repeat(64)},timeoutMs:30000,tmpfsBytes:1024**3,tlsHost:'localhost',ciVerified:true}])await assert.rejects(verifyReceiverCandidateDescription(value));
});
