import test from 'node:test';
import assert from 'node:assert/strict';
import {gatewayEntryStatusFailure} from '../ci/gateway-entry-activation-status.mjs';

test('only explicit network or starting-PG errors permit read-only readiness waiting',()=>{
 for(const code of ['ECONNREFUSED','ECONNRESET','57P03'])assert.equal(gatewayEntryStatusFailure({code}),'entry_status_unavailable');
 for(const code of ['28P01','28000','42501','storage_activation_deploy_signature_invalid','storage_activation_deploy_candidate_mismatch',undefined])assert.equal(gatewayEntryStatusFailure({code}),'entry_status_denied');
 assert.equal(gatewayEntryStatusFailure({message:'connection refused'}),'entry_status_denied');
});
