import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {Store} from '../store.mjs';
import {fetchModelReleaseDates,parseModelReleaseDates,modelReleaseMetadata,modelReleaseRegistry,mergeModelReleaseDates,modelReleaseSource,modelReleaseSetting} from '../model-release-dates.mjs';
import {pricingPublication} from '../pricing.mjs';
import {postgresPricingStatus} from '../postgres-observability.mjs';
const at=Date.UTC(2026,10,12),official=readFileSync(new URL('./fixtures/pricing-standard.md',import.meta.url),'utf8');
const announcement=(model='gpt-synthetic-future',day='Nov 10',text=`Released [Synthetic](https://developers.openai.com/api/docs/models/${model}) for synthetic testing.`)=>`# Changelog\n\n## November, 2026\n\n### ${day}\n\nFeature · Model: ${model} · API: v1/responses\n\n${text}\n`;
test('only explicit release subjects with exact official model links establish dates; the first announcement wins',()=>{
 const text=announcement()+announcement('gpt-synthetic-future','Nov 11','Added Ultrafast mode for [Synthetic](https://developers.openai.com/api/docs/models/gpt-synthetic-future).').replace('# Changelog\n\n','')+announcement('gpt-synthetic-future','Nov 9').replace('# Changelog\n\n','');
 const dates=parseModelReleaseDates(text,{at});assert.equal(dates['gpt-synthetic-future'].date,'2026-11-09');assert.equal(Object.keys(dates).length,1);
 for(const prose of ['Added Ultrafast mode for','Updated the API with','Knowledge cutoff:'])assert.throws(()=>parseModelReleaseDates(announcement('gpt-synthetic-future','Nov 10',`${prose} [Synthetic](https://developers.openai.com/api/docs/models/gpt-synthetic-future).`),{at}),/no verified/);
 assert.throws(()=>parseModelReleaseDates(announcement('gpt-one','Nov 10','Released [Other](https://developers.openai.com/api/docs/models/gpt-other) for testing.'),{at}),/no verified/);
 assert.throws(()=>parseModelReleaseDates(announcement('gpt-one','Nov 10','Released [Other](https://untrusted.invalid/api/docs/models/gpt-one) for testing.'),{at}),/no verified/);
 const multiple=announcement('gpt-one','Nov 10','Released [One](https://developers.openai.com/api/docs/models/gpt-one) and [Two](https://developers.openai.com/api/docs/models/gpt-two) for image generation.').replace('Model: gpt-one · API','Model: gpt-one · Model: gpt-two · API');assert.deepEqual(Object.keys(parseModelReleaseDates(multiple,{at})).sort(),['gpt-one','gpt-two']);
 for(const verb of ['Introducing','Introduced','Launched'])assert.equal(parseModelReleaseDates(announcement().replace('Released',verb),{at})['gpt-synthetic-future'].date,'2026-11-10');
});
test('invalid, future and mismatched dates, excessive input, and ambiguous headings fail closed',()=>{
 for(const text of [announcement('gpt-one','Nov 31'),announcement('gpt-one','Nov 13'),announcement().replace('November, 2026','Unknown, 2026'),announcement().replace('### Nov 10','### Oct 10'),announcement().replace('# Changelog','# Unrelated')])assert.throws(()=>parseModelReleaseDates(text,{at}));
 assert.throws(()=>parseModelReleaseDates('x'.repeat(3000001),{at}),/format invalid/);
 assert.equal(parseModelReleaseDates(announcement().replace('November, 2026','February, 2024').replace('Nov 10','Feb 29'),{at})['gpt-synthetic-future'].date,'2024-02-29');
 assert.throws(()=>parseModelReleaseDates(announcement().replace('November, 2026','February, 2025').replace('Nov 10','Feb 29'),{at}));
});
test('verified seed and merged registry remain independent of pricing books and never inherit aliases',()=>{
 assert.equal(modelReleaseMetadata('gpt-6.1-sol',null,at).release_date,'2026-09-29');assert.equal(modelReleaseMetadata('gpt-6-astra',null,at).release_date,'2026-09-03');assert.equal(modelReleaseMetadata('gpt-6.1-terra',null,at).release_date,'2026-10-26');
 assert.deepEqual(modelReleaseMetadata('gpt-6.1-sol-latest',null,at),{release_date:null,release_date_source:null});
 const prior={dates:{'gpt-stale':{date:'2026-10-01',source:'https://untrusted.invalid'}}};assert.equal(modelReleaseMetadata('gpt-stale',prior,at).release_date,null);
 const book=pricingPublication({'gpt-one':{input:'1',cached:'0.1',output:'2'}},{manualRates:{},aliases:[]});const merged=mergeModelReleaseDates(prior,{dates:parseModelReleaseDates(announcement(),{at}),sourceHash:'synthetic'},at);assert.equal(merged.dates['gpt-synthetic-future'].date,'2026-11-10');assert.deepEqual(book,pricingPublication(book.value.rates,{manualRates:{},aliases:[]}));assert.equal(Object.keys(modelReleaseRegistry(null,Date.UTC(2026,8,1))).length,0);
 const later=mergeModelReleaseDates(merged,{dates:parseModelReleaseDates(announcement('gpt-synthetic-future','Nov 11'),{at}),sourceHash:'later'},at);assert.equal(later.dates['gpt-synthetic-future'].date,'2026-11-10');
});
test('fetch is fixed to official Markdown, bounded, redirect refusing, and cancellation closes the body',async()=>{
 let options;const value=await fetchModelReleaseDates({at,fetchImpl:async(url,init)=>{assert.equal(url,modelReleaseSource);options=init;return new Response(announcement());}});assert.equal(options.redirect,'error');assert.equal(options.headers.Accept,'text/markdown');assert.equal(value.dates['gpt-synthetic-future'].date,'2026-11-10');
 for(const response of [new Response('no',{status:503}),new Response('<html>challenge</html>',{headers:{'Content-Type':'text/html'}}),new Response(announcement(),{headers:{'Content-Length':'3000001'}}),new Response('x'.repeat(3000001))])await assert.rejects(fetchModelReleaseDates({at,fetchImpl:async()=>response}));
 const redirect=new Response(announcement());Object.defineProperty(redirect,'url',{value:'https://untrusted.invalid/changelog'});await assert.rejects(fetchModelReleaseDates({at,fetchImpl:async()=>redirect}));
 const controller=new AbortController();let cancelled=false,started;const seen=new Promise(resolve=>started=resolve);const pending=fetchModelReleaseDates({at,signal:controller.signal,fetchImpl:async()=>new Response(new ReadableStream({start(){started();},cancel(){cancelled=true;}}))});await seen;controller.abort(Error('synthetic stop'));await assert.rejects(pending,/synthetic stop/);assert.equal(cancelled,true);
 const stopped=new AbortController();stopped.abort();await assert.rejects(fetchModelReleaseDates({signal:stopped.signal,fetchImpl:()=>assert.fail('must not fetch')}));
});
test('SQLite status is offline; background dates update/fail independently, preserve fees, and respect disabled/close',async t=>{
 let calls=0,releaseFails=false,pricingFails=false,revisionDay='Nov 10';const store=new Store(':memory:',{pricingOptions:{clock:()=>at,fetchImpl:async url=>{calls++;return url===modelReleaseSource?(releaseFails?new Response('failed',{status:503}):new Response(announcement('gpt-synthetic-future',revisionDay))):(pricingFails?new Response('failed',{status:503}):new Response(official));}}});t.after(()=>store.close());
 const initial=store.pricing.status();assert.equal(calls,0);assert.equal(initial.rates.find(row=>row.model==='gpt-6.1-sol').release_date,'2026-09-29');
 await store.pricing.refresh({force:true});const version=store.pricing.current(),saved=JSON.parse(store.get('SELECT value FROM gateway_settings WHERE name=?',modelReleaseSetting).value);assert.equal(saved.dates['gpt-synthetic-future'].date,'2026-11-10');assert.equal(calls,2);
 releaseFails=true;await store.pricing.refresh({force:true});assert.equal(store.pricing.current(),version);assert.equal(store.pricing.status().modelReleaseState.lastSuccess,at);assert.match(store.pricing.status().modelReleaseState.error,/保留已核实/);assert.equal(JSON.parse(store.get('SELECT value FROM gateway_settings WHERE name=?',modelReleaseSetting).value).dates['gpt-synthetic-future'].date,'2026-11-10');
 releaseFails=false;pricingFails=true;revisionDay='Nov 9';await store.pricing.refresh({force:true});assert.equal(store.pricing.current(),version);assert.equal(JSON.parse(store.get('SELECT value FROM gateway_settings WHERE name=?',modelReleaseSetting).value).dates['gpt-synthetic-future'].date,'2026-11-09');assert.match(store.pricing.status().state.error,/上次有效/);
 store.pricing.configure({expectedVersion:0,enabled:false});const prior=calls;await store.pricing.refresh();assert.equal(calls,prior);store.pricing.status();assert.equal(calls,prior);
});
test('SQLite close cancels and awaits both independent background fetches without writing dates',async()=>{
 let entered=0,started;const seen=new Promise(resolve=>started=resolve),signals=[];const store=new Store(':memory:',{pricingOptions:{clock:()=>at,fetchImpl:async(_url,{signal})=>{signals.push(signal);if(++entered===2)started();return new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>reject(signal.reason),{once:true}));}}});
 const pending=store.pricing.refresh({force:true});await seen;await store.pricing.close();await pending;assert.ok(signals.every(signal=>signal.aborted));assert.equal(store.get('SELECT value FROM gateway_settings WHERE name=?',modelReleaseSetting),undefined);await store.close();
});
test('PostgreSQL status exposes only exact locally stored dates without fetching',async()=>{
 const saved={dates:{'gpt-synthetic-future':{date:'2026-11-10',source:'https://developers.openai.com/api/docs/changelog'}},lastSuccess:at,error:null};const store={transaction:operation=>operation(),all:async sql=>sql.includes('gateway_settings')?[{name:'pricing.state',value:JSON.stringify({active:'synthetic'})},{name:modelReleaseSetting,value:JSON.stringify(saved)}]:[],pricingBook:async()=>({rates:{'gpt-synthetic-future':{input:'1',cached:'0.1',output:'2'},'gpt-synthetic-future-alias':{input:'1',cached:'0.1',output:'2'}}})};const value=await postgresPricingStatus(store,at);assert.equal(value.rates[0].release_date,'2026-11-10');assert.equal(value.rates[1].release_date,null);assert.equal(value.modelReleaseState.lastSuccess,at);
});
