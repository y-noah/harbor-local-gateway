import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync,mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createGateway} from '../server.mjs';
import {createResponsesTransport} from '../responses.mjs';
const official=readFileSync(new URL('./fixtures/pricing-standard.md',import.meta.url),'utf8');
async function setup(t,options={}){
 const root=mkdtempSync(join(tmpdir(),'harbor-pricing-api-'));
 const app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'synthetic-pricing-admin',codexBinary:'unused',reserveTokens:100},rateReader:async()=>null,pricingFetch:async()=>new Response(official),...options});
 await new Promise(r=>app.server.listen(0,'127.0.0.1',r));t.after(async()=>{await app.close();rmSync(root,{recursive:true,force:true});});
 const base='http://127.0.0.1:'+app.server.address().port;
 const call=async(path,method='GET',body,secret='synthetic-pricing-admin')=>{const r=await fetch(base+path,{method,headers:{Authorization:'Bearer '+secret,'Content-Type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});let data;try{data=await r.json();}catch{}return {status:r.status,body:data};};
 return {...app,base,call};
}
const until=async predicate=>{const end=Date.now()+3000;while(!predicate()){assert.ok(Date.now()<end,'condition timed out');await new Promise(r=>setTimeout(r,5));}};
test('only administrators can read or alter rates and ordinary request fields cannot spoof pricing metadata',async t=>{
 const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'fixture',fetchImpl:async()=>new Response('data: '+JSON.stringify({type:'response.completed',response:{model:'gpt-6-astra',status:'completed',output:[],usage:{input_tokens:1000000,output_tokens:0}}})+'\n\n',{headers:{'Content-Type':'text/event-stream'}})});
 const a=await setup(t,{responsesTransport:transport}),m=a.store.createMember('Fixture',null,null);
 for(const [path,method,body] of [['/api/admin/pricing','GET'],['/api/admin/pricing','PATCH',{expectedVersion:0,enabled:false}],['/api/admin/pricing/refresh','POST',{}]])assert.equal((await a.call(path,method,body,m.secret)).status,401);
 a.store.run("INSERT INTO accounts(id,name,home,state) VALUES('fixture','Fixture','synthetic','ready')");
 const r=await a.call('/v1/responses','POST',{model:'gpt-6.1-sol',input:'synthetic',stream:false,price_version:'forged',billing_model:'gpt-6-luna',manual_cost_atoms:'0'},m.secret);assert.equal(r.status,200);
 const row=a.store.get('SELECT * FROM requests');assert.equal(row.billing_model,'gpt-6-astra');assert.notEqual(row.price_version,'forged');assert.equal(row.manual_cost_atoms,null);assert.equal(a.store.pricing.usage(row).total,10);
});
test('rate configuration rejects stale revisions and invalid prices while synchronization failures retain valid data',async t=>{
 const a=await setup(t,{pricingFetch:async()=>new Response('<html>challenge</html>',{status:403})}),before=(await a.call('/api/admin/pricing')).body;
 for(const value of [{expectedVersion:0,manualRates:{bad:{input:'-1',cached:null,output:'1'}}},{expectedVersion:0,manualRates:{bad:{input:2,cached:null,output:'1'}}},{expectedVersion:99,enabled:false},{expectedVersion:0,source:'http://arbitrary-destination.invalid'}])assert.equal((await a.call('/api/admin/pricing','PATCH',value)).status,400);
 const disabled=await a.call('/api/admin/pricing','PATCH',{expectedVersion:0,enabled:false});assert.equal(disabled.status,200);assert.equal(disabled.body.settings.enabled,false);assert.equal((await a.call('/api/admin/pricing','PATCH',{expectedVersion:0,enabled:true})).status,400);
 await a.store.pricing.refresh();assert.equal(a.store.pricing.current(),before.state.active);
 const force=await a.call('/api/admin/pricing/refresh','POST',{});assert.equal(force.status,200);assert.equal(force.body.state.active,before.state.active);assert.match(force.body.state.error,/上次有效/);
});
test('manual USD reconciliation is reflected consistently in details, employee usage and export without inventing raw counts',async t=>{
 const a=await setup(t),m=a.store.createMember('Fixture',null,null),id=a.store.observe(a.store.authenticate(m.secret),'synthetic','fixture','unknown');a.store.finish(id,{usageKnown:false,status:'unknown'});
 assert.equal((await a.call('/api/admin/requests/'+id+'/settle','POST',{tokens:30})).status,400);
 assert.equal((await a.call('/api/admin/requests/'+id+'/settle','POST',{standard_usd:'0.123456'})).status,200);
 assert.equal((await a.call('/api/admin/requests/'+id+'/settle','POST',{standard_usd:'0.123456'})).status,409);
 const row=a.store.get('SELECT * FROM requests');assert.equal(row.input,0);assert.equal(row.usage_unknown,1);assert.equal(row.reserved,0);
 const details=await a.call('/api/admin/requests/'+id);assert.equal(details.body.standard.total_exact,'0.123456');assert.equal(details.body.standard.model_source,'manual');
 const me=await a.call('/api/me','GET',undefined,m.secret);assert.equal(me.body.keyStandard.total_exact,'0.123456');assert.ok(!('keyUsed' in me.body));
 const exported=await fetch(a.base+'/api/admin/export',{headers:{Authorization:'Bearer synthetic-pricing-admin'}}),csv=await exported.text();assert.match(csv,/Standard合计参考USD/);assert.match(csv,/0\.123456/);assert.doesNotMatch(csv,/输入Token|输出Token/);
});
test('source refresh merges current administrator overrides rather than overwriting configuration saved during fetch',async t=>{
 let entered=false,release;const a=await setup(t,{pricingFetch:async()=>{entered=true;await new Promise(r=>release=r);return new Response(official);}});
 const pending=a.call('/api/admin/pricing/refresh','POST',{});await until(()=>entered);
 assert.equal((await a.call('/api/admin/pricing','PATCH',{expectedVersion:0,manualRates:{'gpt-6-astra':{input:'12',cached:'1',output:'50',source:'explicit fixture'}}})).status,200);
 release();const refreshed=await pending;assert.equal(refreshed.body.rates.find(r=>r.model==='gpt-6-astra').input,'12');
});
test('closing the gateway aborts an in-flight price source read without waiting for its network timeout',async t=>{
 let entered=false,aborted=false;const a=await setup(t,{pricingFetch:async(_url,{signal})=>{entered=true;return new Promise((_resolve,reject)=>signal.addEventListener('abort',()=>{aborted=true;reject(Error('cancelled'));},{once:true}));}});
 // This assertion measures source cancellation, not the HTTP client's idle keep-alive timeout.
 const pending=fetch(a.base+'/api/admin/pricing/refresh',{method:'POST',headers:{Authorization:'Bearer synthetic-pricing-admin','Content-Type':'application/json',Connection:'close'},body:'{}'}).then(r=>r.text());await until(()=>entered);let completed=false;
 const closing=a.close().then(()=>completed=true);await until(()=>aborted);await until(()=>completed);await closing;await pending;assert.equal(aborted,true);
});

test('archived requests stay intact and cannot be read or reconciled through admin endpoints',async t=>{
 const a=await setup(t),m=a.store.createMember('Fixture',null,null),id=a.store.observe(a.store.authenticate(m.secret),'synthetic','fixture','unknown');a.store.finish(id,{usageKnown:false,status:'unknown'});
 a.store.run('UPDATE requests SET ended=?,started=?,reserved=100 WHERE id=?',Date.now()-8*86400000,Date.now()-8*86400000,id);
 a.store.pruneUsage();const before=a.store.get('SELECT * FROM requests WHERE id=?',id);
 assert.equal((await a.call('/api/admin/requests/'+id+'/settle','POST',{standard_usd:'1.25'})).status,404);
 assert.deepEqual(a.store.get('SELECT * FROM requests WHERE id=?',id),before);assert.equal(before.usage_unknown,1);assert.equal(before.reserved,100);
 assert.equal((await a.call('/api/admin/requests/'+id)).status,404);assert.equal(a.store.snapshot().totals.standard.total,0);
});

test('legacy manual-token records can be reconciled once in USD without rewriting their compatibility counts',async t=>{
 const a=await setup(t),m=a.store.createMember('Fixture',null,null),id=a.store.observe(a.store.authenticate(m.secret),'synthetic','fixture','gpt-6.1-sol');a.store.finish(id,{usageKnown:false,status:'unknown'});
 a.store.run("UPDATE requests SET status='reconciled',input=1000000,usage_unknown=0 WHERE id=?",id);
 const before=(await a.call('/api/admin/requests/'+id)).body;assert.equal(before.standard.state,'legacy_manual');assert.equal(before.standard.total,null);assert.equal(before.can_reconcile,true);
 assert.equal((await a.call('/api/admin/requests/'+id+'/settle','POST',{standard_usd:'3.25'})).status,200);
 const after=(await a.call('/api/admin/requests/'+id)).body;assert.equal(after.standard.total_exact,'3.25');assert.equal(after.can_reconcile,false);assert.equal(a.store.get('SELECT input FROM requests WHERE id=?',id).input,1000000);
 assert.equal((await a.call('/api/admin/requests/'+id+'/settle','POST',{standard_usd:'3.25'})).status,409);
 assert.equal((await a.call('/api/me','GET',undefined,m.secret)).body.keyStandard.total_exact,'3.25');
});
