import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Store} from '../store.mjs';
import {historyReader,historyQuery,requestObservation,requestsQuery,requestsReader} from '../observability.mjs';
import {createBalancer} from '../balancing.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createGateway} from '../server.mjs';

function fixture(t){
  const dir=mkdtempSync(join(tmpdir(),'harbor-observability-')),file=join(dir,'gateway.sqlite'),store=new Store(file);
  const member=store.createMember('观测测试员工',null,null),at=Date.now();
  for(const [id,remaining,hours] of [['a',30,24],['b',90,160],['c',50,24]])store.run("INSERT INTO accounts(id,name,home,state,quota_json) VALUES(?,?,?,'ready',?)",id,'测试账号'+id,'unused',JSON.stringify({plan:'same',checked:at,remaining,limitReached:false,windows:[{used:100-remaining,minutes:10080,reset:at+hours*3600000}]}));
  store.run("INSERT INTO sessions(id,member_id,account_id,protocol,client_session_id,created,updated) VALUES('session-one',?,'c','responses','11111111-1111-4111-8111-111111111111',?,?)",member.memberId,at,at);
  t.after(()=>{try{store.close();}catch{}rmSync(dir,{recursive:true,force:true});});
  return {store,member,file,at,dir};
}
const query=values=>historyQuery(new URLSearchParams(values));
const until=async fn=>{const end=Date.now()+4000;while(!await fn()){assert.ok(Date.now()<end,'condition timeout');await new Promise(r=>setTimeout(r,5));}};

test('history query rejects ambiguous and unsafe pagination, filters and time boundaries',()=>{
  for(const value of ['page=0','page=1&page=2','page_size=101','page_size=1.5','start=NaN','start=100&end=100','kind=invalid','kind=nightly&action=session.route','account=a%27+OR+1%3D1','session=bad%20id','unknown=value'])assert.throws(()=>historyQuery(new URLSearchParams(value)),{status:400},value);
  assert.equal(query({page:'9999'}).page,9999);
});
test('audit history pages visible dates with deterministic ties and protects raw metadata',t=>{
  const {store,at}=fixture(t);store.run('DELETE FROM audit');
  for(let i=0;i<67;i++)store.run('INSERT INTO audit VALUES(?,?,?,?)','event-'+String(i).padStart(3,'0'),at-2*86400000,'session.route',JSON.stringify({sessionId:'session-one',from:'a',to:'b',reason:'nightly',prompt:'synthetic-private-marker',access_token:'synthetic-private-marker'}));
  store.audit('member.create','synthetic-private-marker');store.audit('unknown.event','synthetic-private-marker');
  const reader=historyReader(store),pages=[1,2,3].map(page=>reader.list(query({page:String(page),action:'session.route'})));
  assert.equal(pages[0].pagination.total,67);assert.equal(pages[2].rows.length,17);
  assert.equal(new Set(pages.flatMap(p=>p.rows.map(r=>r.id))).size,67);
  assert.equal(pages[0].rows[0].clientSession,'11111111-1111-4111-8111-111111111111');
  assert.ok(!JSON.stringify(reader.list(query({page_size:'100'}))).includes('synthetic-private-marker'));
  assert.equal(reader.list(query({action:'session.route',session:'11111111-1111-4111-8111-111111111111'})).pagination.total,67);
  assert.equal(reader.list(query({action:'session.route',account:'a'})).pagination.total,67);
  assert.equal(reader.list(query({action:'session.route',account:'c'})).pagination.total,0,'current binding must not rewrite old transfer participants');
  assert.equal(reader.list(query({start:String(at-86400000),action:'session.route'})).pagination.total,0);
});
test('weight audit shows before and after with safe basis, including deleted accounts',t=>{
  const {store}=fixture(t);store.run('DELETE FROM audit');store.run("UPDATE accounts SET removed_at=1 WHERE id='b'");
  store.audit('balancing.weights.auto',JSON.stringify({reason:'quota',previous:{a:50,b:50},weights:{a:0,b:100},basis:{at:123,accounts:[{id:'a',remaining:0,reset:500,hours:24,denominator:24,score:0,weight:0,secret:'synthetic-private-marker'}]},token:'synthetic-private-marker'}));
  const data=historyReader(store).list(query({account:'b'}));assert.equal(data.rows.length,1);
  assert.deepEqual(data.rows[0].detail.previous,{a:50,b:50});assert.deepEqual(data.rows[0].detail.weights,{a:0,b:100});
  assert.ok(!JSON.stringify(data).includes('synthetic-private-marker'));assert.ok(data.accounts.some(a=>a.id==='b'));
});
test('nightly history preserves planned versus applied migrations and filters both account and session',t=>{
  const {store,at}=fixture(t),summary={window:{start:at-100,end:at},moves:[{session_id:'session-one',from:'a',to:'b',weight:12,prompt:'synthetic-private-marker'}],applied:[],skipped:[{session_id:'session-one',reason:'nightly_plan_changed'}],summaries:[{accounts:['a','b'],before:{a:12,b:0},after:{a:0,b:12},target:{a:6,b:6},fixedLoad:{a:0,b:0},planning:{reason:'balanced',beforeDeviation:50,afterDeviation:50,tolerance:5,prioritySessions:['session-one'],heavySkipped:[]}}]};
  store.run("INSERT INTO balance_runs VALUES('2026-10-27',?,?,'completed',?)",at,at+1,JSON.stringify(summary));
  const reader=historyReader(store);assert.equal(reader.list(query({kind:'nightly',account:'b'})).pagination.total,1);
  assert.equal(reader.list(query({kind:'nightly',session:'11111111-1111-4111-8111-111111111111'})).pagination.total,1);
  const data=reader.nightly('2026-10-27');assert.equal(data.moves[0].applied,false);assert.equal(data.moves[0].client_session_id,'11111111-1111-4111-8111-111111111111');
  assert.equal(data.skipped[0].reason,'nightly_plan_changed');assert.ok(!JSON.stringify(data).includes('synthetic-private-marker'));
  assert.throws(()=>reader.nightly('2026-10-28'),{status:404});
});
test('routing explanation uses frozen calculation facts and never advances the actual allocation',t=>{
  const {store,at}=fixture(t);let clock=at;store.run("UPDATE accounts SET enabled=0 WHERE id='c'");
  store.run("INSERT INTO gateway_settings VALUES('routing_strategy',?)",JSON.stringify({weighted:true}));
  const b=createBalancer({store,preparing:new Map(),refreshQuota:async()=>{},clock:()=>clock});
  b.selectAccount();const before=store.get("SELECT value FROM gateway_settings WHERE name='routing_schedule'").value;
  const first=b.explain();assert.equal(first.basis.at,at);assert.equal(first.basis.accounts.find(a=>a.id==='a').remaining,30);
  clock+=3600000;store.run("UPDATE accounts SET quota_json=? WHERE id='a'",JSON.stringify({plan:'same',checked:clock,remaining:20,windows:[{used:80,minutes:10080,reset:clock+10*3600000}]}));
  const next=b.explain();assert.equal(next.basis.at,at);assert.equal(next.basis.accounts.find(a=>a.id==='a').remaining,30);
  assert.equal(store.get("SELECT value FROM gateway_settings WHERE name='routing_schedule'").value,before);
  assert.equal(b.selectAccount().id,next.nextAccount);
  const saved=JSON.parse(store.get("SELECT value FROM gateway_settings WHERE name='routing_auto'").value);delete saved.basis;
  store.run("UPDATE gateway_settings SET value=? WHERE name='routing_auto'",JSON.stringify(saved));assert.equal(b.explain().basis,null,'old weights must not gain invented historical evidence');
});

test('nightly client UUID filter includes all employee-scoped sessions while internal IDs remain exact',t=>{
  const {store,at}=fixture(t),other=store.createMember('另一个测试员工',null,null),client='11111111-1111-4111-8111-111111111111';
  store.run("INSERT INTO sessions(id,member_id,account_id,protocol,client_session_id,created,updated) VALUES('session-two',?,'b','responses',?,?,?)",other.memberId,client,at,at);
  for(const [day,sid] of [['2026-10-25','session-one'],['2026-10-26','session-two']])store.run("INSERT INTO balance_runs VALUES(?, ?,?,'completed',?)",day,at,at+1,JSON.stringify({moves:[{session_id:sid}],applied:[]}));
  const reader=historyReader(store);assert.equal(reader.list(query({kind:'nightly',session:client})).pagination.total,2);
  for(const sid of ['session-one','session-two'])assert.equal(reader.list(query({kind:'nightly',session:sid})).pagination.total,1);
});
test('account history includes manual restore and removed previous weights but not unrelated accounts',t=>{
  const {store}=fixture(t);store.run('DELETE FROM audit');
  store.audit('balancing.weights',JSON.stringify({mode:'auto',weights:{},previous:{mode:'manual',weights:{a:100},expires:1}}));
  const reader=historyReader(store);assert.equal(reader.list(query({account:'a'})).pagination.total,1);assert.equal(reader.list(query({account:'b'})).pagination.total,0);
  store.audit('balancing.weights.auto',JSON.stringify({weights:{a:100},previous:{b:100},reason:'recovery'}));assert.equal(reader.list(query({account:'b'})).pagination.total,1);
});
test('nightly DTO retains each skipped group and explains individual heavy-session exclusion',t=>{
  const {store,at}=fixture(t);
  store.run("INSERT INTO balance_runs VALUES('2026-10-27',?,?,'completed',?)",at,at+1,JSON.stringify({skipped:[{accounts:['a','b','synthetic-private-marker'],reason:'incomparable_quota'},{accounts:['c'],reason:'unknown_fixed_load'}],summaries:[{accounts:['a','b'],planning:{prioritySessions:['session-one'],heavySkipped:[{session_id:'session-one',reason:'cannot_fit',secret:'synthetic-private-marker'}]}}]}));
  const data=historyReader(store).nightly('2026-10-27');assert.deepEqual(data.skipped.map(x=>x.accounts),[['a','b'],['c']]);
  assert.equal(data.summaries[0].planning.heavySkipped[0].reason,'cannot_fit');assert.equal(data.summaries[0].planning.prioritySessions[0].client_session_id,'11111111-1111-4111-8111-111111111111');assert.ok(!JSON.stringify(data).includes('synthetic-private-marker'));
});
test('request settlement audit remains associated with its request account after session moves',t=>{
  const {store,member}=fixture(t),r=store.observe(store.authenticate(member.secret),'local','fixture','model');store.run("UPDATE requests SET account_id='a',session_id='session-one' WHERE id=?",r);store.finish(r,{});store.audit('request.settle',r);
  const reader=historyReader(store),data=reader.list(query({account:'a',action:'request.settle'}));assert.equal(data.rows.length,1);assert.equal(data.rows[0].detail.request,r);assert.equal(data.rows[0].clientSession,'11111111-1111-4111-8111-111111111111');assert.equal(reader.list(query({account:'c',action:'request.settle'})).pagination.total,0);
});
test('phase transitions persist bounded metadata and late callbacks cannot reopen a finished request',t=>{
  const {store,member}=fixture(t),r=store.observe(store.authenticate(member.secret),'127.0.0.1','fixture','model');
  store.run("UPDATE requests SET status='running' WHERE id=?",r);store.phase(r,'credentials');store.phase(r,'upstream');store.phase(r,'streaming');store.phase(r,'streaming');
  store.finish(r,{input:8,output:2,cached:3});const data=requestObservation(store,r);
  assert.deepEqual(data.timeline.map(p=>p.phase),['queued','credentials','upstream','streaming','settling','ok']);
  assert.ok(data.timeline.every(p=>p.duration>=0));assert.equal(store.get('SELECT input FROM requests WHERE id=?',r).input,8);assert.equal(store.get('SELECT cached FROM requests WHERE id=?',r).cached,3);assert.equal(data.standard.state,'unpriced');assert.ok(!('input' in data));
  const saved=store.get('SELECT phase_history FROM requests WHERE id=?',r).phase_history;store.phase(r,'upstream');assert.equal(store.get('SELECT phase_history FROM requests WHERE id=?',r).phase_history,saved);
});
test('restart closes running phase while preserving known timings and leaving old completed rows without fabricated stages',t=>{
  const {store,member,file}=fixture(t),person=store.authenticate(member.secret),running=store.observe(person,'local','fixture','model'),old=store.observe(person,'local','fixture','model');
  store.run("UPDATE requests SET status='running' WHERE id=?",running);store.phase(running,'upstream');store.finish(old,{});store.run("UPDATE requests SET phase=NULL,phase_at=NULL,phase_history='[]' WHERE id=?",old);store.close();
  const reopened=new Store(file);try{assert.equal(requestObservation(reopened,running).phase,'interrupted');assert.equal(requestObservation(reopened,running).status,'interrupted');assert.equal(requestObservation(reopened,old).timeline.length,0);}finally{reopened.close();}
});
test('new management endpoints require admin auth and reject malformed history without exposing employee Keys',async t=>{
  const dir=mkdtempSync(join(tmpdir(),'harbor-observe-api-')),app=createGateway({dataDir:join(dir,'data'),runtimeDir:join(dir,'runtime'),config:{adminToken:'test-only-admin'}});
  await new Promise(r=>app.server.listen(0,'127.0.0.1',r));t.after(async()=>{await app.close();rmSync(dir,{recursive:true,force:true});});
  const base='http://127.0.0.1:'+app.server.address().port,member=app.store.createMember('fixture',null,null);
  for(const path of ['/api/admin/requests','/api/admin/history','/api/admin/history/nightly/2026-10-27','/api/admin/balancing/explanation','/api/admin/requests/unknown'])assert.equal((await fetch(base+path,{headers:{Authorization:'Bearer '+member.secret}})).status,401);
  assert.equal((await fetch(base+'/api/admin/history?page_size=101',{headers:{Authorization:'Bearer test-only-admin'}})).status,400);
  const r=await fetch(base+'/api/admin/history',{headers:{Authorization:'Bearer test-only-admin'}});assert.equal(r.status,200);assert.ok(!(await r.text()).includes(member.secret));
});
test('real Responses transport exposes credentials, upstream wait and first received data without per-chunk transitions',async t=>{
  let releaseAuth,releaseHeaders,releaseTail;
  const authGate=new Promise(r=>releaseAuth=r),headerGate=new Promise(r=>releaseHeaders=r),tailGate=new Promise(r=>releaseTail=r),phases=[];
  const transport=createResponsesTransport({credentials:async()=>{await authGate;return {token:'test-only',accountId:'test-only'};},fetchImpl:async()=>{await headerGate;return new Response(new ReadableStream({async start(c){c.enqueue(Buffer.from('data: {"type":"response.created"}\n\n'));await tailGate;c.enqueue(Buffer.from('data: {"type":"response.completed","response":{"usage":{"input_tokens":8,"output_tokens":2},"output":[]}}\n\n'));c.close();}}),{headers:{'Content-Type':'text/event-stream'}});}});
  const pending=transport.run({binary:'unused',home:'unused',payload:{model:'fixture'},onPhase:p=>phases.push(p)});
  assert.deepEqual(phases,['credentials']);releaseAuth();await until(()=>phases.includes('upstream'));assert.ok(!phases.includes('streaming'));releaseHeaders();await until(()=>phases.includes('streaming'));releaseTail();assert.equal((await pending).input,8);assert.deepEqual(phases,['credentials','upstream','streaming']);
});


test('request query validates all filters and pagination without interpreting model names as SQL',()=>{
 for(const raw of ['page=0','page=2&page=3','page_size=101','page_size=2.5','status=invalid','member=bad%20id','model=%00','unknown=1'])assert.throws(()=>requestsQuery(new URLSearchParams(raw)),{status:400},raw);
 assert.equal(requestsQuery(new URLSearchParams({model:"x' OR 1=1 --"})).model,"x' OR 1=1 --");
});
test('request totals cover all pages, combine filters and keep users with identical names distinct',t=>{
 const {store,member,at}=fixture(t),other=store.createMember('观测测试员工',null,null),read=values=>requestsReader(store,requestsQuery(new URLSearchParams(values)),at);
 const window=read({}).window;
 for(let i=0;i<251;i++)store.run("INSERT INTO requests(id,member_id,started,ended,status,model,ip,input,output,cached,usage_unknown) VALUES(?,?,?,?,'ok',?,'fixture',10,2,3,?)",'request-'+String(i).padStart(3,'0'),i<201?member.memberId:other.memberId,at-1000,at,i%2?'gpt-6-sol':'gpt-6.1-sol',i===250?1:0);
 store.run("INSERT INTO requests(id,member_id,started,ended,status,model,ip,input) VALUES('expired',?, ?,?,'ok','old','fixture',999999)",member.memberId,window.start-1001,window.start-1);
 store.run("INSERT INTO requests(id,member_id,started,status,model,ip) VALUES('pending',?,?,'running','gpt-6.1-sol','fixture')",member.memberId,at);
 const all=read({});assert.equal(all.summary.requests,252);assert.equal(all.summary.standard.input_exact,'0.0036125');assert.equal(all.summary.standard.cached_exact,'0.0001125');assert.equal(all.summary.unmetered,1);assert.equal(all.summary.active,1);assert.equal(all.summary.average_ms,1000);assert.equal(all.byMember.length,2);assert.equal(all.members.length,2);assert.equal(all.rows.length,25);
 const pages=Array.from({length:3},(_,i)=>read({page:String(i+1),page_size:'100'}));assert.equal(new Set(pages.flatMap(x=>x.rows.map(r=>r.id))).size,252);
 const filtered=read({member:member.memberId,model:'gpt-6.1-sol',status:'ok'});assert.equal(filtered.summary.requests,101);assert.equal(filtered.summary.standard.output_exact,'0.00202');assert.equal(filtered.byMember.length,1);assert.equal(filtered.byModel.length,1);assert.equal(filtered.members.length,2);assert.equal(filtered.models.length,2);
 assert.equal(read({model:"x' OR 1=1 --"}).summary.requests,0);assert.equal(read({page:'999'}).pagination.page,11);
 assert.ok(!JSON.stringify(all).includes(member.secret));
});
