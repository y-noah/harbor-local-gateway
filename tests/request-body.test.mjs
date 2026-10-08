import test from 'node:test';
import assert from 'node:assert/strict';
import {Readable} from 'node:stream';
import {zstdCompressSync} from 'node:zlib';
import {randomBytes} from 'node:crypto';
import {readJsonBody,responseBodyBytes} from '../request-body.mjs';
function req(chunks,headers={}){const stream=Readable.from(chunks);stream.headers={'content-type':'application/json',...headers};return stream;}
const status=(value,code)=>e=>e.status===value&&(!code||e.code===code);
test('Responses uses the documented 128MiB contract; byte boundaries include non-ASCII UTF8',async()=>{
 assert.equal(responseBodyBytes,128*1024*1024);
 const raw=Buffer.from(JSON.stringify({input:'中文'}));assert.deepEqual(await readJsonBody(req([raw]),{maxBytes:raw.length}),{input:'中文'});
 await assert.rejects(readJsonBody(req([raw]),{maxBytes:raw.length-1}),status(413,'request_too_large'));
 const early=req([] ,{'content-length':'1025'});await assert.rejects(readJsonBody(early,{maxBytes:1024}),status(413));
});
test('zstd decoding preserves JSON content; encoded and expanded bounds are separate',async()=>{
 const value={input:randomBytes(2048).toString('hex')},raw=Buffer.from(JSON.stringify(value)),packed=zstdCompressSync(raw);
 assert.deepEqual(await readJsonBody(req([packed.subarray(0,10),packed.subarray(10)],{'content-encoding':'zstd'}),{maxBytes:raw.length,compressed:true}),value);
 await assert.rejects(readJsonBody(req([packed],{'content-encoding':'zstd'}),{maxBytes:raw.length-1,compressed:true}),status(413));
 await assert.rejects(readJsonBody(req([packed],{'content-encoding':'zstd'}),{maxBytes:packed.length-1,compressed:true}),status(413));
});
test('invalid, truncated, excessive-ratio and unsupported compressed inputs fail safely',async()=>{
 const packed=zstdCompressSync(Buffer.from(JSON.stringify({input:'x'.repeat(100000)})));
 await assert.rejects(readJsonBody(req([packed],{'content-encoding':'zstd'}),{maxBytes:200000,compressed:true}),status(413,'request_too_large'));
 for(const bytes of [Buffer.from('not-zstd'),packed.subarray(0,packed.length-2)])await assert.rejects(readJsonBody(req([bytes],{'content-encoding':'zstd'}),{compressed:true}),status(400,'invalid_compressed_body'));
 await assert.rejects(readJsonBody(req([packed],{'content-encoding':'gzip'}),{compressed:true}),status(415));
 await assert.rejects(readJsonBody(req([packed],{'content-encoding':'zstd'})),status(415));
 for(const body of ['null','[]','false','{'])await assert.rejects(readJsonBody(req([Buffer.from(body)])),status(400,'invalid_json'));
});
test('disconnect rejects the upload without parsing or retaining a partial request',async()=>{
 const source=req((async function*(){yield Buffer.from('{"input":"');throw Object.assign(Error('disconnected'),{code:'ECONNRESET'});})());
 await assert.rejects(readJsonBody(source),/disconnected/);
});
