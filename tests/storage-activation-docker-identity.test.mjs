import test from 'node:test';
import assert from 'node:assert/strict';
import {matchesStorageActivationImageId,exactCandidate} from '../storage-activation-deploy.mjs';
const candidate={configDigest:'sha256:'+'1'.repeat(64),manifestDigest:'sha256:'+'2'.repeat(64)},image='registry.invalid/harbor-local-gateway/harbor-local-gateway:'+'a'.repeat(40),deploymentId='00000000-0000-4000-8000-000000000001',receiptDigest='3'.repeat(64);
const context={facts:{candidate},image,deploymentId,receiptDigest},row=id=>({Image:id,Config:{Image:image,Labels:{'com.aistrong.project':'harbor-local-gateway','com.harbor.activation-deployment-id':deploymentId,'com.harbor.activation-receipt':receiptDigest}}});
test('preflight imageId retains distinct signed config/manifest meanings and rejects tag aliases',()=>{
 for(const id of [candidate.configDigest,candidate.manifestDigest])assert.equal(matchesStorageActivationImageId(id,candidate),true);
 for(const id of [null,undefined,image,'sha256:'+'4'.repeat(64),candidate.manifestDigest.slice(7)])assert.equal(matchesStorageActivationImageId(id,candidate),false);
 assert.notEqual(candidate.configDigest,candidate.manifestDigest);
});
for(const id of [candidate.configDigest,candidate.manifestDigest])test('shared candidate predicate binds '+(id===candidate.configDigest?'classic config':'snapshotter manifest')+' for recovery and denial cleanup',()=>{
 assert.equal(exactCandidate(row(id),context),true);
 for(const changed of [{Image:'sha256:'+'4'.repeat(64)},{Config:{...row(id).Config,Image:image+'-alias'}},{Config:{...row(id).Config,Labels:{...row(id).Config.Labels,'com.harbor.activation-deployment-id':'other'}}},{Config:{...row(id).Config,Labels:{...row(id).Config.Labels,'com.harbor.activation-receipt':'other'}}}])assert.equal(exactCandidate({...row(id),...changed},context),false);
 // revoke/abandon do not depend on a mutable tag still resolving to the old B.
 const recovery={...context,safetyRevoke:true};assert.equal(exactCandidate({...row(id),Config:{...row(id).Config,Image:'retagged-approved-digest'}},recovery),true);assert.equal(exactCandidate({...row(id),Image:'sha256:'+'4'.repeat(64)},recovery),false);
});
