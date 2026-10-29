import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Store,usageWindow} from '../store.mjs';
import {parseOfficialPrices,quoteReference,publicUsage,exactUsd,referenceMetric} from '../pricing.mjs';
import {requestsReader,requestObservation} from '../observability.mjs';
const official=readFileSync(new URL('./fixtures/pricing-standard.md',import.meta.url),'utf8');
const completed=(model,input,output=0,cached=0)=>({model,input,output,cached,started:Date.now(),ended:Date.now(),status:'ok',usage_unknown:0});
function fixture(t,options={}){
 const store=new Store(':memory:',{pricingOptions:{fetchImpl:async()=>new Response(official),...options}});t.after(()=>store.close());
 const m=store.createMember('Fixture',null,null),employee=store.authenticate(m.secret);
 const add=(model,input,output=0,cached=0,extra={})=>{const id=store.observe(employee,'synthetic','fixture',model);store.finish(id,{input,output,cached,...extra});return id;};
 return {store,add,employee};
}
test('official Standard parsing keeps distinct cached rates and never substitutes Batch pricing',()=>{
 const rates=parseOfficialPrices(official);assert.equal(Object.keys(rates).length,37);assert.deepEqual(rates['gpt-6.1-sol'],{input:'2.00',cached:'0.10',output:'10.00'});assert.equal(rates['gpt-6-astra'].input,'10.00');
 const book={rates,aliases:[]};assert.equal(publicUsage(quoteReference(completed('gpt-6.1-sol',1000000,10000,800000),book)).total_exact,'0.58');assert.equal(publicUsage(quoteReference(completed('gpt-6-astra',1000000,10000,800000),book)).total_exact,'3.3');
 const future='| gpt-future | $6.00 | $30.00 | $0.30 | $7.50 | $15.00 | $12.00 | $45.00 | $0.60 | $15.00 | $30.00 |\n';assert.equal(parseOfficialPrices(official.replace('| gpt-6-astra |',future+'| gpt-6-astra |'))['gpt-future'].input,'6.00');
 assert.throws(()=>parseOfficialPrices(official.replace('Short context cached input','changed cache schema')),/字段已变化/);
});
test('missing usage, unavailable cache price and unknown aliases are not priced as zero',()=>{
 for(const model of ['codex-auto-review','constructor','toString','new-unpriced-model'])assert.equal(quoteReference(completed(model,100)).state,'unpriced');
 assert.equal(quoteReference({...completed('gpt-6.1-sol',0),usage_unknown:1}).state,'unmetered');assert.equal(quoteReference(completed('gpt-5.5-pro',100,1,1)).state,'unpriced');
 assert.equal(quoteReference(completed('gpt-6.1-sol',0)).totalAtoms,0n);assert.equal(quoteReference(completed('gpt-6.1-sol',1,0,2)).state,'unmetered');
});
test('specialized Standard text models are discovered without ingesting Fast, training or mixed-modality prices',()=>{
 const text=official+'\nStandard\n### Grouped Pricing Table data\n| Category | Model | Input | Output | Cached input |\n| --- | --- | --- | --- | --- |\n| Codex | gpt-5.3-codex | $1.75 | $14.00 | $0.175 |\n\nFast\n### Grouped Pricing Table data\n| Category | Model | Input | Output | Cached input |\n| --- | --- | --- | --- | --- |\n| Codex | gpt-5.3-codex | $3.50 | $28.00 | $0.35 |\n\nStandard\n| Model | Training | Input | Output | Cached input |\n| --- | --- | --- | --- | --- |\n| gpt-5.3-codex | $1 | $99 | $99 | $99 |\n\n| Model | Modality | Input | Output | Cached input |\n| --- | --- | --- | --- | --- |\n| mixed-image | Image | $8 | $30 | $2 |\n';
 const parsed=parseOfficialPrices(text);assert.deepEqual(parsed['gpt-5.3-codex'],{input:'1.75',cached:'0.175',output:'14.00'});assert.ok(!Object.hasOwn(parsed,'mixed-image'));assert.equal(Object.keys(parsed).length,38);
});
test('price versions stay fixed across changes while unpriced data can be resolved by explicit dated mappings',t=>{
 const {store,add}=fixture(t);const prior=add('gpt-6.1-sol',1000000),unpriced=add('codex-auto-review',1000000),old=store.pricing.current();
 store.pricing.configure({expectedVersion:0,manualRates:{'gpt-6.1-sol':{input:'4',cached:'0.1',output:'10'}}});
 assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',prior)).total,2);assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',unpriced)).total,null);
 const newer=add('gpt-6.1-sol',1000000);assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',newer)).total,4);
 store.pricing.configure({expectedVersion:1,aliases:[{name:'codex-auto-review',target:'gpt-6.1-sol',from:0,to:null,source:'Synthetic explicit mapping evidence'}]});
 assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',unpriced)).total,4);assert.equal(store.pricing.backfill(),0);assert.equal(store.get('SELECT price_version FROM requests WHERE id=?',prior).price_version,old);
});
test('grouped request totals, key/account scopes and daily totals reconcile in reference USD with no raw statistics DTO',t=>{
 const {store,add,employee}=fixture(t);add('gpt-6.1-sol',1000000,10000,800000);add('gpt-6-astra',1000000,10000,800000);add('unknown-model',100,20);const snap=store.snapshot();
 assert.equal(snap.totals.standard.total_exact,'3.88');assert.equal(snap.totals.standard.unpriced,1);assert.equal(snap.totals.standard.state,'partial');assert.equal(snap.keys[0].standard.total_exact,'3.88');assert.equal(snap.members[0].standard.total_exact,'3.88');assert.equal(snap.daily[0].standard.total_exact,'3.88');
 assert.ok(!('tokens' in snap.totals));assert.ok(!('used' in snap.keys[0]));assert.ok(!('input' in snap.requests[0]));assert.ok(!('reserved' in snap.requests[0]));
 const result=requestsReader(store,{member:employee.id,model:'',status:'',page:1,pageSize:1});assert.equal(result.rows.length,1);assert.equal(result.summary.standard.total_exact,'3.88');assert.equal(result.byModel.reduce((n,r)=>n+(r.standard.total||0),0),3.88);assert.equal(result.byMember[0].standard.total_exact,'3.88');
 const filtered=requestsReader(store,{member:'',model:'gpt-6-astra',status:'',page:1,pageSize:25});assert.equal(filtered.summary.standard.total_exact,'3.3');assert.equal(filtered.summary.requests,1);
});
test('a response model is authoritative and a manual amount does not invent raw usage or add it twice',t=>{
 const {store,add}=fixture(t);const a=add('codex-auto-review',1000000,0,0,{actualModel:'gpt-6-astra'});assert.equal(requestObservation(store,a).standard.total,10);assert.equal(requestObservation(store,a).model,'gpt-6-astra');
 const manual=add('unknown',0,0,0,{usageKnown:false,status:'unknown'});store.run("UPDATE requests SET manual_cost_atoms='250000000000',status='reconciled' WHERE id=?",manual);
 const row=store.get('SELECT * FROM requests WHERE id=?',manual);assert.equal(row.input,0);assert.equal(row.usage_unknown,1);assert.equal(store.pricing.usage(row).adjustment,0.25);assert.equal(store.snapshot().totals.standard.total_exact,'10.25');
});
test('automatic source failure retains the prior version, successful refresh adds new models and respects manual overrides',async t=>{
 let text=official,failed=true,calls=0;const {store,add}=fixture(t,{fetchImpl:async()=>{calls++;return failed?new Response('<html>challenge</html>',{status:403}):new Response(text);}});const before=store.pricing.current();await store.pricing.refresh({force:true});assert.equal(store.pricing.current(),before);assert.match(store.pricing.status().state.error,/上次有效/);
 store.pricing.configure({expectedVersion:0,manualRates:{'gpt-6-astra':{input:'12',cached:'1',output:'50',source:'manual fixture'}}});const unknown=add('gpt-future',1000000);assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',unknown)).total,null);
 text=official.replace('| gpt-6-astra |','| gpt-future | $6.00 | $30.00 | $0.30 | $7.50 | $15.00 | $12.00 | $45.00 | $0.60 | $15.00 | $30.00 |\n| gpt-6-astra |');failed=false;await store.pricing.refresh({force:true});assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',unknown)).total,6);assert.equal(store.pricing.status().rates.find(r=>r.model==='gpt-6-astra').input,'12');assert.equal(calls,2);
 const stable=store.pricing.current();await store.pricing.refresh({force:true});assert.equal(store.pricing.current(),stable);
});
test('restart retains immutable prices and expired history does not reappear as zero reference consumption',t=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-pricing-'));t.after(()=>rmSync(root,{recursive:true,force:true}));let store=new Store(join(root,'db.sqlite'));const member=store.createMember('fixture',null,null),id=store.observe(store.authenticate(member.secret),'synthetic','fixture','gpt-6.1-sol');store.finish(id,{input:1000000});const rev=store.get('SELECT price_version FROM requests WHERE id=?',id).price_version;store.close();store=new Store(join(root,'db.sqlite'));t.after(()=>store.close());assert.equal(store.get('SELECT price_version FROM requests WHERE id=?',id).price_version,rev);assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',id)).total,2);
 const at=Date.now()+8*86400000;store.pruneUsage(at);assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',id),at).state,'expired');assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',id),at).total,null);
});
test('integer price arithmetic preserves tiny values and aggregates beyond JavaScript integer precision',()=>{
 const value=quoteReference(completed('gpt-6-luna',1,0,1));assert.equal(exactUsd(value.totalAtoms),'0.00000001');const large=quoteReference(completed('gpt-6-astra',1000000000000,10000000000,800000000000));assert.ok(large.totalAtoms>BigInt(Number.MAX_SAFE_INTEGER));assert.equal(exactUsd(large.totalAtoms*3000n),'9900000000');
});
test('fee publication and unresolved-history backfill roll back together when persistence fails',t=>{
 const {store,add}=fixture(t),id=add('unresolved-fixture',100),before=store.pricing.current(),versions=store.get('SELECT COUNT(*) n FROM pricing_versions').n,run=store.run;
 store.run=function(sql,...args){if(sql.startsWith('UPDATE requests SET price_version=')&&sql.includes('AND price_version=?'))throw Error('synthetic persistence failure');return run.call(this,sql,...args);};
 try{assert.throws(()=>store.pricing.configure({expectedVersion:0,manualRates:{'unresolved-fixture':{input:'2',cached:'0.1',output:'10'}}}),/synthetic persistence/);}finally{store.run=run;}
 assert.equal(store.pricing.current(),before);assert.equal(store.pricing.status().settings.version,0);assert.equal(store.get('SELECT COUNT(*) n FROM pricing_versions').n,versions);assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',id)).total,null);assert.ok(!store.pricing.status().rates.some(r=>r.model==='unresolved-fixture'));
});
test('a missing rate supplied while a request runs is applied on completion even with sync disabled',t=>{
 const {store,employee}=fixture(t),future=store.observe(employee,'synthetic','fixture','new-model'),known=store.observe(employee,'synthetic','fixture','gpt-6.1-sol'),old=store.pricing.current();
 store.run("UPDATE requests SET status='running' WHERE id IN (?,?)",future,known);
 store.pricing.configure({expectedVersion:0,enabled:false,manualRates:{'new-model':{input:'6',cached:'0.3',output:'30'},'gpt-6.1-sol':{input:'4',cached:'0.1',output:'10'}}});
 const current=store.pricing.current();assert.notEqual(current,old);assert.equal(store.get('SELECT price_version FROM requests WHERE id=?',future).price_version,old);
 store.finish(future,{input:1000000,output:10000,cached:800000});store.finish(known,{input:1000000});
 assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',future)).total_exact,'1.74');assert.equal(store.get('SELECT price_version FROM requests WHERE id=?',future).price_version,current);
 assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',known)).total_exact,'2');assert.equal(store.get('SELECT price_version FROM requests WHERE id=?',known).price_version,old);
});
test('legacy aggregate-token settlements remain unknown across upgrade and old-writer rollback until USD reconciliation',t=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-legacy-price-')),file=join(root,'db.sqlite');t.after(()=>rmSync(root,{recursive:true,force:true}));let store=new Store(file);
 const member=store.createMember('Fixture',null,null),id=store.observe(store.authenticate(member.secret),'synthetic','fixture','gpt-6.1-sol');store.finish(id,{usageKnown:false,status:'unknown'});
 // The old writer stores a whole manual token count as input; it is not a trustworthy input/output breakdown.
 store.run("UPDATE requests SET status='reconciled',usage_unknown=0,input=1000000,price_version=NULL WHERE id=?",id);store.close();store=new Store(file);
 assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',id)).state,'legacy_manual');assert.equal(store.snapshot().totals.standard.total,null);assert.equal(store.snapshot().totals.standard.unmetered,1);
 const rollback=store.observe(store.authenticate(member.secret),'synthetic','fixture','gpt-6.1-sol');store.finish(rollback,{usageKnown:false,status:'unknown'});store.close();
 store=new Store(file);store.run("UPDATE requests SET status='reconciled',usage_unknown=0,input=2000000 WHERE id=?",rollback);store.close();store=new Store(file);t.after(()=>store.close());
 assert.equal(store.pricing.usage(store.get('SELECT * FROM requests WHERE id=?',rollback)).state,'legacy_manual');assert.equal(store.snapshot().totals.standard.total,null);assert.equal(store.get('SELECT input FROM requests WHERE id=?',rollback).input,2000000);
});
