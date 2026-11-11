import test from 'node:test';
import assert from 'node:assert/strict';
import {readdirSync} from 'node:fs';
import {testPartition} from '../ci/run-test-partition.mjs';

test('CI partitions cover every suite exactly once, including newly added suites',()=>{
  const compute=testPartition('compute'),docker=testPartition('docker');
  const all=readdirSync(new URL('./',import.meta.url)).filter(name=>name.endsWith('.test.mjs')).map(name=>'tests/'+name).sort();
  assert.ok(compute.length&&docker.length);
  assert.equal(new Set([...compute,...docker]).size,compute.length+docker.length);
  assert.deepEqual([...compute,...docker].sort(),all);
  assert.throws(()=>testPartition('unknown'),/unknown_test_partition/);
});
