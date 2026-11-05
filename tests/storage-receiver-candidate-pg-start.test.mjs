import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
test('actual PG restore controller refuses new starts after approval changes while preserving cleanup',{timeout:15000},async()=>{const {stdout}=await promisify(execFile)(process.execPath,['--experimental-vm-modules',new URL('./receiver-candidate-pg-start-lifecycle-fixture.mjs',import.meta.url).pathname],{timeout:10000,maxBuffer:65536});assert.deepEqual(JSON.parse(stdout),{revokedDuringRestoreStartsNothing:true,revokedDuringVerifyStartsNoPostgres:true});});
