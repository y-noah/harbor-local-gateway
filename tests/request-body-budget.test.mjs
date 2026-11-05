import test from 'node:test';
import assert from 'node:assert/strict';
import {Readable,PassThrough} from 'node:stream';
import {zstdCompressSync} from 'node:zlib';
import {randomBytes} from 'node:crypto';
import {createRequestBodyBudget} from '../request-body-budget.mjs';
const request=(chunks,headers={})=>Object.assign(Readable.from(chunks),{headers:{'content-type':'application/json',...headers}});
const empty=budget=>{const state=budget.status();assert.equal(state.readers,0);assert.equal(state.leases,0);assert.equal(state.bufferedBytes,0);};

test('body budget retains parsed payload bytes until explicit idempotent release and frees reading slots',async()=>{
 const raw=Buffer.from(JSON.stringify({input:'x'.repeat(80)})),budget=createRequestBodyBudget({maxBufferedBytes:512,maxReaders:1}),first=request([raw],{'content-length':String(raw.length)});assert.deepEqual(await budget.read(first),JSON.parse(raw));assert.equal(budget.status().readers,0);assert.equal(budget.status().leases,1);assert.equal(budget.status().bufferedBytes,raw.length);const second=request([raw]);await budget.read(second);assert.equal(budget.status().bufferedBytes,raw.length*2);budget.release(first);budget.release(first);assert.equal(budget.status().bufferedBytes,raw.length);budget.release(second);empty(budget);
});

test('body budget charges concatenation and UTF16 conversion before allocation and rejects overload without truncation',async()=>{
 const raw=Buffer.from(JSON.stringify({input:'x'.repeat(100)}));for(const maximum of [raw.length*3-1,raw.length*3]){const budget=createRequestBodyBudget({maxBufferedBytes:maximum}),req=request([raw.subarray(0,30),raw.subarray(30)]);if(maximum<raw.length*3){await assert.rejects(budget.read(req),{status:503,code:'request_body_busy'});empty(budget);}else{assert.equal((await budget.read(req)).input.length,100);assert.equal(budget.status().peakBufferedBytes,raw.length*3);budget.release(req);empty(budget);}}
});

test('body budget limits unfinished uploads before retaining bytes and rejects declared bodies before reading',async()=>{
 const budget=createRequestBodyBudget({maxBufferedBytes:1024,maxReaders:1}),first=Object.assign(new PassThrough(),{headers:{'content-type':'application/json','content-length':'100'}});const pending=budget.read(first);assert.equal(budget.status().readers,1);assert.equal(budget.status().bufferedBytes,100);const second=request([Buffer.from('{}')]);await assert.rejects(budget.read(second),{code:'request_body_busy'});first.end(Buffer.from('{}'));await pending;budget.release(first);empty(budget);const tooBig=request([] ,{'content-length':'1025'});await assert.rejects(budget.read(tooBig),{code:'request_body_busy'});empty(budget);
});

test('body budget is shared across retained request payloads and allows a new read after the owner releases',async()=>{
 const raw=Buffer.from(JSON.stringify({input:'x'.repeat(100)})),budget=createRequestBodyBudget({maxBufferedBytes:raw.length*3}),first=request([raw]),second=request([raw]);await budget.read(first);await assert.rejects(budget.read(second),{code:'request_body_busy'});assert.equal(budget.status().bufferedBytes,raw.length);budget.release(first);const third=request([raw]);assert.equal((await budget.read(third)).input.length,100);budget.release(third);empty(budget);
});

test('body budget accounts zstd expanded chunks, preserves transport limits and releases malformed or disconnected reads',async()=>{
 const raw=Buffer.from(JSON.stringify({input:randomBytes(2048).toString('hex')})),packed=zstdCompressSync(raw),budget=createRequestBodyBudget({maxBufferedBytes:raw.length*4}),req=request([packed],{'content-encoding':'zstd'});assert.equal((await budget.read(req,{compressed:true})).input,JSON.parse(raw).input);assert.equal(budget.status().bufferedBytes,raw.length);budget.release(req);empty(budget);
 for(const [chunks,headers,options,code] of [[[Buffer.from('{')],{}, {},'invalid_json'],[[packed.subarray(0,8)],{'content-encoding':'zstd'},{compressed:true},'invalid_compressed_body'],[[packed],{'content-encoding':'zstd'},{compressed:true,maxBytes:raw.length-1},'request_too_large']]){await assert.rejects(budget.read(request(chunks,headers),options),{code});empty(budget);}
 const failed=request((async function*(){yield Buffer.from('{"input":"');throw Error('synthetic disconnect');})());await assert.rejects(budget.read(failed),/synthetic disconnect/);empty(budget);
});

test('body resource configuration is server-owned and invalid sizes or reader counts are rejected',()=>{
 for(const options of [{maxBufferedBytes:255},{maxBufferedBytes:NaN},{maxBufferedBytes:8*1024**3+1},{maxReaders:0},{maxReaders:129},{maxReaders:1.5},{maxBufferedBytesPerOwner:255},{maxBufferedBytes:512,maxBufferedBytesPerOwner:513},{maxReadersPerOwner:0},{maxReaders:2,maxReadersPerOwner:3}])assert.throws(()=>createRequestBodyBudget(options),{code:'body_configuration'});
});

test('body budget reserves reader and retained-byte capacity per trusted owner without exposing identities',async()=>{
 const budget=createRequestBodyBudget({maxBufferedBytes:1536,maxBufferedBytesPerOwner:512,maxReaders:2,maxReadersPerOwner:1}),slow=Object.assign(new PassThrough(),{headers:{'content-type':'application/json','content-length':'100'}}),pending=budget.read(slow,{owner:'member:first'});const refused=request([Buffer.from('{}')]);await assert.rejects(budget.read(refused,{owner:'member:first'}),{code:'request_body_busy'});const peer=request([Buffer.from('{}')]);await budget.read(peer,{owner:'member:second'});assert.equal(budget.status().owners,2);budget.release(peer);slow.end('{}');await pending;budget.release(slow);empty(budget);assert.equal(budget.status().owners,0);
 const raw=Buffer.from(JSON.stringify({input:'x'.repeat(140)})),a=request([raw]),b=request([raw]),c=request([raw]);await budget.read(a,{owner:'member:first'});await assert.rejects(budget.read(b,{owner:'member:first'}),{code:'request_body_busy'});await budget.read(c,{owner:'member:second'});assert.ok(!JSON.stringify(budget.status()).includes('member:'));budget.release(a);budget.release(c);empty(budget);
});

test('R34 server-side cancellation ends an unfinished upload and returns its reader and byte reservations',async()=>{
 const budget=createRequestBodyBudget(),controller=new AbortController(),req=Object.assign(new PassThrough(),{headers:{'content-type':'application/json','content-length':'1000'}}),pending=budget.read(req,{signal:controller.signal});pending.catch(()=>{});req.write('{"input":"');await new Promise(setImmediate);assert.equal(budget.status().bufferedBytes,1000);controller.abort();await assert.rejects(pending,{status:499,code:'request_cancelled'});empty(budget);
});
