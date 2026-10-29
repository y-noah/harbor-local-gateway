import test from 'node:test';
import assert from 'node:assert/strict';
import {planBalance} from '../balancing.mjs';
import {quoteReference,referenceMetric} from '../pricing.mjs';
const hour=3600000,at=Date.parse('2026-10-29T02:00:10+08:00');
const account=id=>({id,kind:'codex',auth_mode:'subscription',enabled:1,state:'ready',removed_at:null,quota_json:JSON.stringify({plan:'same',checked:at,remaining:50,windows:[{minutes:10080,reset:at+24*hour,used:50}],limitReached:false})});
const session=(id,account_id)=>({id,account_id,member_id:'m',member_enabled:1,member_removed:null,protocol:'responses',enabled:1,nightly_enabled:1,route_version:0});
const req=(session_id,model,input,extra={})=>({session_id,model,input,output:0,cached:0,usage_unknown:0,status:'ok',started:at-2*hour,ended:at-hour,...extra});
const sessions=[session('astra','a'),session('small-1','a'),session('small-2','a'),session('other','b')];
const rows=[req('astra','gpt-6-astra',300),req('small-1','gpt-6.1-sol',1000),req('small-2','gpt-6.1-sol',1000),req('other','gpt-6.1-sol',1500)];
const plan=requests=>planBalance({accounts:[account('a'),account('b')],sessions,requests,at});
test('nightly prioritizes a costly Astra session even when its raw count is smaller than Sol sessions',()=>{
 const p=plan(rows);assert.equal(p.metric,referenceMetric);assert.equal(p.moves.length,3);assert.ok(p.moves.some(m=>m.session_id==='astra'));assert.ok(Math.abs(p.summaries[0].before.a-0.007)<1e-15);assert.ok(Math.abs(p.summaries[0].after.a-0.005)<1e-15);assert.ok(Math.abs(p.summaries[0].after.b-0.005)<1e-15);
});
test('nightly allocations and deviations are invariant under common workload scaling',()=>{
 const base=plan(rows),scaled=plan(rows.map(r=>({...r,input:r.input*10000})));assert.deepEqual(scaled.moves.map(m=>[m.session_id,m.from,m.to]),base.moves.map(m=>[m.session_id,m.from,m.to]));assert.ok(Math.abs(scaled.summaries[0].planning.afterDeviation-base.summaries[0].planning.afterDeviation)<1e-10);
});
test('unpriced completed workload protects its group instead of reusing raw count or guessing a fee',()=>{
 const p=plan(rows.map(r=>r.session_id==='astra'?{...r,model:'codex-auto-review'}:r));assert.equal(p.moves.length,0);assert.ok(p.skipped.some(x=>x.reason==='unknown_fixed_load'));
});
test('inflight estimation reprices same-model raw samples at the dispatch revision',()=>{
 const versions={old:{rates:{'gpt-6.1-sol':{input:'2',cached:'0.1',output:'10'}},aliases:[]},new:{rates:{'gpt-6.1-sol':{input:'4',cached:'0.1',output:'10'}},aliases:[]}};
 const requests=[req('astra','gpt-6.1-sol',1000,{price_version:'old'}),req('astra','gpt-6.1-sol',0,{price_version:'new',ended:null,status:'running'}),req('other','gpt-6.1-sol',1000,{price_version:'old'})];
 const p=planBalance({accounts:[account('a'),account('b')],sessions,requests,at,quoteUsage:(r,version=r.price_version)=>quoteReference(r,versions[version],version)});
 assert.ok(Math.abs(p.summaries[0].fixedLoad.a-0.006)<1e-15);assert.equal(p.moves.length,0);assert.ok(p.summaries[0].fixed.includes('astra'));
});
test('legacy manually merged token counts cannot become nightly work or comparable estimation samples',()=>{
 const p=plan(rows.map(r=>r.session_id==='astra'?{...r,status:'reconciled',model:'gpt-6.1-sol',input:1000000}:r));assert.equal(p.moves.length,0);assert.ok(p.skipped.some(x=>x.reason==='unknown_fixed_load'));
});
test('USD reconciliation counts completed work but never substitutes for raw samples of inflight requests',()=>{
 const work=[session('manual','a'),session('running','a'),session('sample','b')];
 const make=model=>[req('manual',model,1000000,{status:'reconciled',manual_cost_atoms:'3250000000000'}),req('running',model,0,{ended:null,status:'running'})];
 const unknown=planBalance({accounts:[account('a'),account('b')],sessions:work,requests:make('codex-auto-review'),at});
 assert.equal(unknown.moves.length,0);assert.ok(unknown.skipped.some(x=>x.reason==='unknown_fixed_load'));
 const rows=make('gpt-6.1-sol');rows.push(req('sample','gpt-6.1-sol',1000));
 const known=planBalance({accounts:[account('a'),account('b')],sessions:work,requests:rows,at});
 assert.equal(known.skipped.some(x=>x.reason==='unknown_fixed_load'),false);assert.equal(known.summaries[0].fixedLoad.a,0.002);assert.equal(known.summaries[0].before.a,3.252);assert.equal(rows[0].input,1000000);
});
