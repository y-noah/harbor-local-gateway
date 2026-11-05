import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
test('actual receiver receipt logic revalidates approval after cleanup and rejects profile/material changes or cancellation',{timeout:15000},async()=>{
 const {stdout}=await promisify(execFile)(process.execPath,['--experimental-vm-modules',new URL('./receiver-candidate-lifecycle-fixture.mjs',import.meta.url).pathname],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(stdout),{afterCleanupProfileChangeRejected:true,afterCleanupMaterialChangeRejected:true,afterCleanupCancellationRejected:true,unchangedAccepted:true,defaultPreserved:true,afterCleanupLayerMutationRejected:true,afterCleanupLayerDeletionRejected:true,beforeAuthorityLayerMutationRejected:true,beforeAuthorityLayerDeletionRejected:true,duringBackupProfileChangeRejected:true,duringBackupLayerMutationRejected:true,duringBackupLayerDeletionRejected:true});
});
