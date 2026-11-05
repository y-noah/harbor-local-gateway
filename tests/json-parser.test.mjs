import test from 'node:test';
import assert from 'node:assert/strict';
import {Worker} from 'node:worker_threads';
import {Readable} from 'node:stream';
import {createJsonParser} from '../json-parser.mjs';
import {createRequestBodyBudget} from '../request-body-budget.mjs';
const wait=async predicate=>{const end=Date.now()+5000;while(!predicate()){assert.ok(Date.now()<end,'parser condition timeout');await new Promise(setImmediate);}};
const gate=()=>new Int32Array(new SharedArrayBuffer(4));
const unblock=state=>{Atomics.store(state,0,2);Atomics.notify(state,0);};
const fixture=(state,extra={})=>(url,options)=>new Worker(new URL('./json-parser-child.mjs',import.meta.url),{...options,workerData:{gate:state,...extra}});

test('JSON parser uses real workers, preserves Unicode and JSON values and isolates pooled bytes',async t=>{
 const pool=createJsonParser();t.after(()=>pool.close());const value={text:'中文 🍋',escaped:'a\nb',zero:-0,items:[null,true,2.5],nested:{__proto__:null,constructor:'literal'}},raw=Buffer.from(JSON.stringify(value)),before=Buffer.from(raw),parsed=await pool.parse(raw);assert.deepEqual(parsed,JSON.parse(before));assert.deepEqual(raw,before);assert.equal(pool.status().completed,1);assert.equal(pool.status().active,0);assert.equal(pool.status().workers,1);
 for(const invalid of ['{','null','[]','false'])await assert.rejects(pool.parse(Buffer.from(invalid)),{status:400,code:'invalid_json'});assert.equal((await pool.parse(Buffer.from('{"ok":true}'))).ok,true);
 const own=Buffer.allocUnsafeSlow(12);own.write('{"ok":true} ');const result=await pool.parse(own);assert.equal(result.ok,true);assert.equal(own.length,0);await pool.close();assert.equal(pool.status().workers,0);
});

test('JSON parser sends no environment to workers and applies an explicit isolate heap limit',async t=>{
 let configured;const pool=createJsonParser({heapMb:64},{workerFactory:(url,options)=>{configured=options;return fixture(undefined,{environment:true})(url,options);}});t.after(()=>pool.close());assert.deepEqual(await pool.parse(Buffer.from('{}')),{empty:true});assert.deepEqual(configured.env,{});assert.deepEqual(configured.execArgv,[]);assert.equal(configured.resourceLimits.maxOldGenerationSizeMb,64);
});

test('JSON parser queue is bounded, queued cancellation never dispatches and remaining jobs still run',async t=>{
 const state=gate(),pool=createJsonParser({maxWorkers:1,maxQueued:1},{workerFactory:fixture(state)});t.after(()=>pool.close());const first=pool.parse(Buffer.from('{"first":true}'));await wait(()=>Atomics.load(state,0)===1);const controller=new AbortController(),second=pool.parse(Buffer.from('{"second":true}'),{signal:controller.signal});second.catch(()=>{});assert.equal(pool.status().queued,1);await assert.rejects(pool.parse(Buffer.from('{}')),{status:503,code:'json_parser_busy'});controller.abort();await assert.rejects(second,{code:'request_cancelled'});assert.equal(pool.status().queued,0);unblock(state);assert.deepEqual(await first,{first:true});assert.equal(pool.status().completed,1);
});

test('JSON parser active cancellation waits for actual worker exit before replacing its slot',async t=>{
 const state=gate();let created=0,live=0,peak=0;const pool=createJsonParser({maxWorkers:1,maxQueued:1},{workerFactory:(url,options)=>{created++;const worker=created===1?fixture(state)(url,options):new Worker(url,options);live++;peak=Math.max(peak,live);worker.once('exit',()=>live--);return worker;}});t.after(()=>pool.close());const controller=new AbortController(),first=pool.parse(Buffer.from('{"first":true}'),{signal:controller.signal});first.catch(()=>{});await wait(()=>Atomics.load(state,0)===1);const second=pool.parse(Buffer.from('{"next":true}'));controller.abort();assert.equal(pool.status().retiring,1);assert.equal(created,1);await assert.rejects(first,{code:'request_cancelled'});assert.deepEqual(await second,{next:true});assert.equal(peak,1);await pool.close();assert.equal(live,0);
});

test('JSON parser worker exit rejects only its job without replay and the next request gets a fresh worker',async t=>{
 let created=0;const pool=createJsonParser({maxWorkers:1},{workerFactory:(url,options)=>++created===1?fixture(undefined,{exit:true})(url,options):new Worker(url,options)});t.after(()=>pool.close());await assert.rejects(pool.parse(Buffer.from('{"first":true}')),{status:503,code:'json_parser_failed'});assert.equal(created,1);assert.deepEqual(await pool.parse(Buffer.from('{"next":true}')),{next:true});assert.equal(created,2);
});

for(const stage of ['queue','active'])test('JSON parser '+stage+' deadline is bounded and preserves other admitted work',async t=>{
 const state=gate(),pool=createJsonParser({maxWorkers:1,waitMs:100,timeoutMs:stage==='active'?100:30000},{workerFactory:fixture(state)});t.after(()=>pool.close());t.mock.timers.enable({apis:['setTimeout']});const first=pool.parse(Buffer.from('{"first":true}'));first.catch(()=>{});let second;
 try{await wait(()=>Atomics.load(state,0)===1);if(stage==='queue'){second=pool.parse(Buffer.from('{}'));second.catch(()=>{});}t.mock.timers.tick(100);if(stage==='queue'){await assert.rejects(second,{code:'json_parser_queue_timeout'});assert.equal(pool.status().active,1);unblock(state);assert.deepEqual(await first,{first:true});}else{await assert.rejects(first,{code:'json_parser_timeout'});assert.equal(pool.status().workers,0);}}
 finally{t.mock.timers.reset();unblock(state);await first.catch(()=>{});await second?.catch(()=>{});}
});

test('JSON parser close rejects pending work, joins its workers and refuses future parsing',async t=>{
 const state=gate(),pool=createJsonParser({maxWorkers:1},{workerFactory:fixture(state)});t.after(()=>pool.close());const first=pool.parse(Buffer.from('{}'));first.catch(()=>{});await wait(()=>Atomics.load(state,0)===1);const second=pool.parse(Buffer.from('{}'));second.catch(()=>{});const closed=pool.close();await assert.rejects(first,{code:'json_parser_closed'});await assert.rejects(second,{code:'json_parser_closed'});await closed;assert.equal(pool.status().workers,0);assert.equal(pool.status().queued,0);await assert.rejects(pool.parse(Buffer.from('{}')),{code:'json_parser_closed'});await pool.close();
});

test('body budget retains exact original bytes across transferred parser input and releases on failures',async t=>{
 const pool=createJsonParser(),budget=createRequestBodyBudget({parse:pool.parse}),raw=Buffer.from(JSON.stringify({input:'x'.repeat(10000)})),req=Object.assign(Readable.from([raw]),{headers:{'content-type':'application/json'}});t.after(()=>pool.close());assert.equal((await budget.read(req)).input.length,10000);assert.equal(budget.status().bufferedBytes,10012);budget.release(req);assert.equal(budget.status().bufferedBytes,0);const bad=Object.assign(Readable.from([Buffer.from('{')]),{headers:{'content-type':'application/json'}});await assert.rejects(budget.read(bad),{code:'invalid_json'});assert.equal(budget.status().leases,0);
});

test('JSON parser configuration rejects invalid counts, deadlines and heap bounds',()=>{
 for(const options of [{maxWorkers:0},{maxWorkers:9},{maxQueued:-1},{maxQueued:129},{waitMs:99},{waitMs:60001},{timeoutMs:99},{timeoutMs:300001},{heapMb:63},{heapMb:2049},{rebuildMs:0},{rebuildMs:26}])assert.throws(()=>createJsonParser(options),{code:'json_parser_configuration'});
});

test('R33 JSON parsing retains deep trees and prototype-sensitive data without recursive cloning',async t=>{
 const pool=createJsonParser({maxWorkers:1,maxQueued:0});t.after(()=>pool.close());const depth=12000,raw=Buffer.from('{"deep":'+'['.repeat(depth)+'0'+']'.repeat(depth)+',"special":{"__proto__":{"polluted":"synthetic"},"constructor":{"prototype":{"literal":true}},"prototype":"value"}}'),result=await pool.parse(raw);let cursor=result.deep,count=0;while(Array.isArray(cursor)){assert.equal(cursor.length,1);cursor=cursor[0];count++;}assert.equal(count,depth);assert.equal(cursor,0);assert.equal(Object.getPrototypeOf(result.special),Object.prototype);assert.equal(Object.hasOwn(result.special,'__proto__'),true);assert.equal(result.special.__proto__.polluted,'synthetic');assert.equal({}.polluted,undefined);assert.equal(result.special.constructor.prototype.literal,true);assert.deepEqual(await pool.parse(Buffer.from('{"next":true}')),{next:true});const literals=await pool.parse(Buffer.from('{"zero":-0,"large":1e400,"text":"\\ud800"}'));assert.equal(Object.is(literals.zero,-0),true);assert.equal(literals.large,Infinity);assert.equal(literals.text.charCodeAt(0),0xd800);await pool.close();assert.equal(pool.status().workers,0);
});

test('R33 cancellation during iterative reconstruction releases the complete parser slot before replacement',async t=>{
 const {performance}=await import('node:perf_hooks'),pool=createJsonParser({maxWorkers:1,rebuildMs:1});t.after(()=>pool.close());let now=0;t.mock.method(performance,'now',()=>now+=2);const controller=new AbortController(),pending=pool.parse(Buffer.from('{"items":['+Array.from({length:3000},(_,n)=>n).join(',')+']}'),{signal:controller.signal});pending.catch(()=>{});
 try{await wait(()=>pool.status().rebuilding===1);controller.abort();await assert.rejects(pending,{code:'request_cancelled'});assert.equal(pool.status().workers,0);assert.equal(pool.status().rebuilding,0);t.mock.restoreAll();assert.deepEqual(await pool.parse(Buffer.from('{"next":true}')),{next:true});}finally{controller.abort();t.mock.restoreAll();await pending.catch(()=>{});}
});
