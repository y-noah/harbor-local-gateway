import test from 'node:test';
import assert from 'node:assert/strict';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import {RecordRedactor} from '../record-redaction.mjs';
import {lazyWindowDigests} from './record-redaction-lazy-window-vectors.mjs';

// Original implementation at fcce49c69662d30e0bcca264cf4dd03ab7ba3e29:
// record-redaction.mjs SHA256 75653fdd34b055a7afa8ba70aa08500f4c2f100e5b78f9d85d4aad624da6ed70.
// Derived by executing that exact Git blob, without changing these vectors.
test('lazy lookbehind exactly preserves trusted output, callback provenance and observed UTF-16 windows',()=>{
  assert.deepEqual(lazyWindowDigests(RecordRedactor),{
    count:110,
    output:'f527bf4e8f3ce6ea3c4e79fed5e0cfa2f1acf9c6ecd1fd581c122e0a9683a01b',
    emits:'bfc358bd603b20f0fc3ee1cff3d4f1c9ae70a3ef0b5067de20e10c8256caa6f0',
    windows:'13a469b1acb764ae27a798f06f24df8f8a90141d81588816e33add426aa68ca2'
  });
});
test('lookbehind never retains the raw chunk after a late observer failure',()=>{
  let emitted=0;const filter=new RecordRedactor({onEmit:text=>{emitted+=text.length;if(emitted>=5000)throw Error('synthetic observer failure');}}),raw='visible '.repeat(128*1024);
  assert.throws(()=>filter.push(raw),/synthetic observer failure/);
  assert.equal(filter.recentText,null);assert.equal(filter.recentBase,'');assert.ok(filter.recent.length<=100);
});
test('the retained UTF-16 window is an independent small string in the actual V8 heap',{timeout:30000},async()=>{
  const {stdout}=await promisify(execFile)(process.execPath,['--expose-gc',new URL('./record-redaction-window-heap-fixture.mjs',import.meta.url).pathname],{timeout:20000,maxBuffer:4096});
  const result=JSON.parse(stdout);assert.equal(result.type,'string');assert.ok(result.bytes<=512);assert.equal(result.windowUnits,100);
});
