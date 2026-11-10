import test from 'node:test';
import assert from 'node:assert/strict';
import {randomBytes} from 'node:crypto';
import crypto from 'node:crypto';
import {syncBuiltinESMExports} from 'node:module';
import {createRecordingIndexCrypto,assembleRecording,recordingParts} from '../recording-search-index.mjs';
import {csvCell,referenceUsdDisplay} from '../recording-export-format.mjs';
import {recordingQuery} from '../postgres-recording-query.mjs';

test('recording index assembles append/replace by field without deduplicating repeated user messages',()=>{
  const items=assembleRecording([{sequence:1,kind:'user',provenance:'client',content:{field:'u1',text:'same',mode:'append'}},{sequence:2,kind:'user',provenance:'client',content:{field:'u2',text:'same',mode:'append'}},{sequence:3,kind:'assistant',content:{field:'a',text:'wrong',mode:'append'}},{sequence:4,kind:'assistant',content:{field:'a',text:'correct',mode:'replace'}}]);assert.deepEqual(items.map(item=>item.text),['same','same','correct']);
  const parts=recordingParts([{...items[0],text:'甲'.repeat(2047)+'中文查询'+'乙'.repeat(2048)}]);assert.ok(parts.some(part=>part.searchText.includes('中文查询')));assert.equal(parts.map(part=>part.text).join(''),'甲'.repeat(2047)+'中文查询'+'乙'.repeat(2048));assert.equal(parts[1].continuation,true);
});
test('recording blind index keys are domain separated, ciphertext authenticates scope, and case/Unicode normalization is explicit',()=>{
  const key=randomBytes(32),first=createRecordingIndexCrypto(key),same=createRecordingIndexCrypto(key),other=createRecordingIndexCrypto(randomBytes(32));assert.deepEqual(first.terms('中文CreateHandler'),same.terms('中文CREATEHANDLER'));assert.notDeepEqual(first.terms('中文'),other.terms('中文'));const cipher=first.seal({text:'private text'},'one');assert.ok(!cipher.includes(Buffer.from('private text')));assert.deepEqual(first.open(cipher,'one'),{text:'private text'});assert.throws(()=>first.open(cipher,'two'),{code:'recording_index_unreadable'});
});
test('blind indexing hashes each distinct normalized gram once while preserving the original digest order',()=>{
 const index=createRecordingIndexCrypto(randomBytes(32)),expected=index.terms('甲乙ab甲'),original=crypto.createHmac;let calls=0;
 try{crypto.createHmac=(...args)=>{calls++;return original(...args);};syncBuiltinESMExports();assert.deepEqual(index.terms('甲乙ＡB'.repeat(4096)),expected);assert.equal(calls,4,'重复正文不应重复计算相同规范化二元项的HMAC');}
 finally{crypto.createHmac=original;syncBuiltinESMExports();}
});
test('per-document term scopes preserve independent digests across Unicode parts and caller mutation',()=>{
 const key=randomBytes(32),index=createRecordingIndexCrypto(key),scope=index.createTermsScope(),other=index.createTermsScope();
 const texts=['甲乙ＡB'.repeat(512),'甲乙ab'.repeat(512),'e\u0301😀中文\t\n全角ＡＢ','短','','\t\n','尾部中文'.repeat(1024)];
 for(let round=0;round<3;round++)for(const text of texts){const expected=index.terms(text),actual=scope(text);assert.deepEqual(actual,expected);for(const buffer of actual)buffer.fill(0);assert.deepEqual(scope(text),expected);assert.deepEqual(other(text),expected);}
 assert.notDeepEqual(createRecordingIndexCrypto(randomBytes(32)).createTermsScope()('中文'),scope('中文'));
});
test('term reuse stays inside one indexing scope and avoids repeated HMAC work across parts',()=>{
 const index=createRecordingIndexCrypto(randomBytes(32)),expected=index.terms('甲乙ab甲'),scope=index.createTermsScope(),original=crypto.createHmac;let calls=0;
 try{
  crypto.createHmac=(...args)=>{calls++;return original(...args);};syncBuiltinESMExports();
  for(let i=0;i<20;i++)assert.deepEqual(scope('甲乙ＡB'.repeat(512)),expected);
  assert.equal(calls,4);assert.deepEqual(index.createTermsScope()('甲乙ＡB'.repeat(512)),expected);assert.equal(calls,8,'新任务必须使用独立作用域');
 }finally{crypto.createHmac=original;syncBuiltinESMExports();}
});
test('B5 normalized search windows retain a maximum-length query across a combining-character boundary',()=>{
  const text='x'.repeat(1920)+'e\u0301'.repeat(128)+'z',parts=recordingParts([{kind:'user',field:'u',text}]),query='é'.repeat(128);assert.ok(parts.some(part=>part.searchText.includes(query)));assert.equal(parts.map(part=>part.text).join(''),text);
});
test('CSV exports quote data and neutralize formulas after whitespace and control prefixes',()=>{
  for(const text of ['=1+1',' +SUM(A1:A2)','\t@cmd','\r-2'])assert.ok(csvCell(text).startsWith('"\''));assert.equal(csvCell('a"b'),'"a""b"');assert.equal(csvCell(null),'""');
});
test('reference USD export formats two decimals with exact integer rounding and preserves unknown',()=>{assert.equal(referenceUsdDisplay(null),null);assert.equal(referenceUsdDisplay('1'),'0.00');assert.equal(referenceUsdDisplay('1004999999999'),'1.00');assert.equal(referenceUsdDisplay('1005000000000'),'1.01');assert.equal(referenceUsdDisplay('999999999999999999999'),'1000000000.00');});
test('body query rejects short/invalid/filter injection and default window remains stable across cursor calls',()=>{
  const at=Date.parse('2026-11-03T05:00:00Z'),clock=()=>at;for(const params of ['q=a','q=中文&type=x&type=y','q=中文&limit=0','q=中文&member=bad%20id'])assert.throws(()=>recordingQuery(new URLSearchParams(params),{clock,search:true}),{code:'recording_query_invalid'});const first=recordingQuery(new URLSearchParams('q=中文'),{clock,search:true}),later=recordingQuery(new URLSearchParams('q=中文'),{clock:()=>at+5000,search:true});assert.equal(first.from,later.from);assert.equal(first.to,later.to);
});
