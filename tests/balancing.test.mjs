import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {EventEmitter} from 'node:events';
import {PassThrough} from 'node:stream';
import {Store} from '../store.mjs';
import {createGateway} from '../server.mjs';
import {createResponsesTransport,structuredFailure,refreshCredentials} from '../responses.mjs';
import {balanceSettings,nightlyWindow,planBalance,createBalancer,automaticWeights,nextWeightReset} from '../balancing.mjs';

const at=Date.parse('2026-10-14T02:00:15+08:00'),hour=3600000;
const account=(id,remaining=50)=>({id,kind:'codex',enabled:1,removed_at:null,state:'ready',route_block:null,route_epoch:0,quota_json:JSON.stringify({plan:'same-plan',checked:at,remaining,windows:[{used:100-remaining,minutes:300,reset:at+hour}],limitReached:false})});
const session=(id,account_id,extra={})=>({id,account_id,member_id:'member',member_enabled:1,member_removed:null,protocol:'responses',enabled:1,nightly_enabled:1,route_version:0,...extra});
const request=(session_id,weight,extra={})=>({session_id,started:at-2*hour,ended:at-hour,status:'ok',model:'fixture-model',input:weight,output:0,usage_unknown:0,...extra});
const plan=(accounts,sessions,requests,extra={})=>planBalance({accounts,sessions,requests,at,...extra});
const until=async fn=>{const deadline=Date.now()+3000;while(!await fn()){assert.ok(Date.now()<deadline,'condition timeout');await new Promise(r=>setTimeout(r,5));}};

test('nightly calendar is Beijing 02:00 with exact 16-hour window and one-minute trigger',()=>{
 const w=nightlyWindow(at);assert.equal(w.key,'2026-10-14');assert.equal(w.start,Date.parse('2026-10-13T10:00:00+08:00'));assert.equal(w.end,Date.parse('2026-10-14T02:00:00+08:00'));assert.ok(w.due);
 assert.equal(nightlyWindow(w.end-1).due,false);assert.equal(nightlyWindow(w.end+60000).due,false);
});
test('settings reject unsafe types and unexpected client options',()=>{
 assert.deepEqual(balanceSettings(),{failover:false,nightly:false,quietMinutes:30,weighted:false});
 for(const value of [null,[],2,{failover:'true'},{nightly:1},{quietMinutes:0},{quietMinutes:961},{quietMinutes:1.1},{account_id:'forged'}])assert.throws(()=>balanceSettings(value));
});
test('fixed long task consumes its account share while movable sessions move toward spare quota',()=>{
 const accounts=[account('a',40),account('b',60)],sessions=[session('long','a'),session('small','a'),session('heavy','a'),session('b-work','b')];
 const requests=[request('long',30),request('long',5,{status:'running',ended:null,started:at-20*hour}),request('small',5),request('heavy',30),request('b-work',30)];
 const p=plan(accounts,sessions,requests);
 assert.ok(p.summaries[0].fixed.includes('long'));assert.ok(p.summaries[0].before.a>65,'running work is estimated, not zero');
 assert.ok(p.moves.some(m=>m.session_id==='heavy'&&m.to==='b'));assert.ok(!p.moves.some(m=>m.session_id==='long'));
 assert.ok(p.summaries[0].after.a<p.summaries[0].before.a);
});
test('balanced allocations do not churn and indivisible oversized sessions are not moved without improvement',()=>{
 assert.equal(plan([account('a'),account('b')],[session('a1','a'),session('b1','b')],[request('a1',100),request('b1',100)]).moves.length,0);
 assert.equal(plan([account('a'),account('b')],[session('large','a')],[request('large',100)]).moves.length,0);
});
for(const weighted of [false,true])test('nightly accepts up to two percentage points without churn, weighted '+weighted,()=>{
 for(const tail of [1.5,2,2.0001]){
  const p=plan([account('a'),account('b')],[session('large','a'),session('tail','a'),session('other','b')],[request('large',50),request('tail',tail),request('other',50-tail)],{weighted});
  assert.equal(p.moves.length,tail<=2?0:1);
  if(tail>2)assert.equal(p.moves[0].session_id,'tail');
 }
});
test('nightly stops after reaching tolerance instead of migrating a tail for exact balance',()=>{
 const weights=[40,40,15,4,1],sessions=weights.map((_,i)=>session('work-'+i,'b'));
 const p=plan([account('a',60),account('b',40)],sessions,weights.map((w,i)=>request('work-'+i,w)),{weighted:true});
 assert.equal(p.moves.length,3);assert.equal(p.summaries[0].after.a,59);assert.equal(p.summaries[0].after.b,41);
 assert.equal(new Set(p.moves.map(m=>m.session_id)).size,3);assert.ok(p.moves.every(m=>m.from==='b'&&m.to==='a'));
});
test('one fitting session is preferred over a larger greedy transfer that misses tolerance',()=>{
 const sessions=[session('large','a'),session('small-1','a'),session('small-2','a'),session('other','b')];
 const p=plan([account('a'),account('b')],sessions,[request('large',30),request('small-1',20),request('small-2',20),request('other',30)]);
 assert.equal(p.moves.length,1);assert.equal(p.moves[0].weight,20);assert.deepEqual(p.summaries[0].after,{a:50,b:50});
});
test('nightly finds a smaller feasible combination when the largest session overshoots',()=>{
 const weights=[60,40,30,20,50],sessions=weights.map((_,i)=>session('work-'+i,i===4?'b':'a'));
 const p=plan([account('a'),account('b')],sessions,weights.map((w,i)=>request('work-'+i,w)),{weighted:true});
 assert.equal(p.moves.length,2);assert.deepEqual(p.moves.map(m=>m.weight).sort((a,b)=>a-b),[20,30]);
 assert.deepEqual(p.summaries[0].after,{a:100,b:100});
});
test('two-point tolerance is checked per account rather than averaged across the group',()=>{
 const sessions=[session('large','a'),session('small','a'),session('b-work','b'),session('c-work','c')];
 const p=plan([account('a'),account('b'),account('c')],sessions,[request('large',33),request('small',3),request('b-work',32),request('c-work',32)],{weighted:true});
 assert.equal(p.moves.length,1);assert.equal(p.moves[0].session_id,'small');
 const g=p.summaries[0];for(const a of ['a','b','c'])assert.ok(Math.abs(g.after[a]-g.target[a])<=2);
});
test('fewer-transfer combinations never allocate sessions to an exhausted zero-weight account',()=>{
 const weights=[60,40,30,20,50,1],sessions=weights.map((_,i)=>session('work-'+i,i<4?'a':i===4?'b':'empty'));
 const p=plan([account('a'),account('b'),account('empty',0)],sessions,weights.map((w,i)=>request('work-'+i,w)),{weighted:true});
 assert.equal(p.moves.length,2);assert.ok(p.moves.every(m=>m.to!=='empty'));
 const g=p.summaries[0],total=weights.reduce((sum,w)=>sum+w,0);for(const a of ['a','b','empty'])assert.ok(Math.abs(g.after[a]-g.target[a])/total<=0.02);
});
test('minimizing transfers keeps protected sessions fixed even when tolerance is unattainable',()=>{
 const p=plan([account('a'),account('b')],[session('protected','a',{nightly_enabled:0}),session('movable','a')],[request('protected',80),request('movable',20)],{weighted:true});
 assert.deepEqual(p.moves.map(m=>m.session_id),['movable']);assert.deepEqual(p.summaries[0].after,{a:80,b:20});
 assert.deepEqual(p.summaries[0].fixed,['protected']);assert.equal(p.summaries[0].fixedLoad.a,80);
});
test('tolerance and smaller-combination search stay isolated between comparable account groups',()=>{
 const c=account('c'),d=account('d');for(const a of [c,d]){const q=JSON.parse(a.quota_json);q.plan='other-plan';a.quota_json=JSON.stringify(q);}
 const sessions=[session('a-large','a'),session('a-small-1','a'),session('a-small-2','a'),session('b-work','b'),session('c-work','c'),session('d-work','d')];
 const p=plan([account('a'),account('b'),c,d],sessions,[request('a-large',30),request('a-small-1',20),request('a-small-2',20),request('b-work',30),request('c-work',52),request('d-work',48)],{weighted:true});
 assert.equal(p.summaries.length,2);assert.equal(p.moves.length,1);assert.equal(p.moves[0].from,'a');assert.equal(p.moves[0].to,'b');
 assert.deepEqual(p.summaries[1].after,{c:52,d:48});
});
test('quiet, manual exclusion, preparing and disabled sessions remain fixed and count as load',()=>{
 const sessions=[session('quiet','a'),session('manual','a',{nightly_enabled:0}),session('disabled','a',{enabled:0}),session('ready','a')];
 const rows=sessions.map(s=>request(s.id,20,s.id==='quiet'?{ended:at-1,started:nightlyWindow(at).end-1000}:{}));
 const p=plan([account('a',20),account('b',80)],sessions,rows);
 assert.equal(p.summaries[0].before.a,80);assert.deepEqual(p.moves.map(m=>m.session_id),['ready']);
 assert.equal(plan([account('a',20),account('b',80)],sessions,rows,{preparing:new Set(['member'])}).moves.length,0);
});
test('unknown ongoing load without comparable samples prevents fictional balancing',()=>{
 const p=plan([account('a',20),account('b',80)],[session('long','a'),session('ready','a')],[request('long',0,{status:'running',ended:null,model:'no-samples'}),request('ready',20)]);
 assert.equal(p.moves.length,0);assert.ok(p.skipped.some(s=>s.reason==='unknown_fixed_load'));
});
test('stale, reset, incomparable and blocked accounts never receive nightly transfers',()=>{
 for(const bad of [a=>({...a,route_block:'login_expired'}),a=>({...a,quota_json:'{}'}),a=>({...a,quota_json:JSON.stringify({...JSON.parse(a.quota_json),plan:'other-plan'})}),a=>({...a,quota_json:JSON.stringify({...JSON.parse(a.quota_json),checked:at-360000})}),a=>({...a,quota_json:JSON.stringify({...JSON.parse(a.quota_json),windows:[{used:0,minutes:300,reset:at}]})})]){
  assert.equal(plan([account('a',20),bad(account('b',80))],[session('one','a'),session('two','a')],[request('one',20),request('two',20)]).moves.length,0);
 }
});

async function gateway(t,{rateReader=async()=>{throw Error('unavailable');},clock}={}){
 const root=mkdtempSync(join(tmpdir(),'harbor-balancing-'));let mode='ok',release,arrivals=0;const calls=[];
 const transport=createResponsesTransport({version:async()=>'0.0.0',credentials:async(_binary,home)=>({token:'fixture-upstream',accountId:home}),fetchImpl:async(url,options)=>{
  calls.push({home:options.headers['ChatGPT-Account-ID'],body:JSON.parse(options.body)});arrivals++;
  if(mode==='hold')await new Promise(r=>{release=r;});
  if(mode==='quota')return Response.json({error:{type:'usage_limit_reached',message:'private should not escape'}},{status:429});
  if(mode==='auth')return Response.json({error:{type:'invalid_token'}},{status:401});
  if(mode==='rate')return Response.json({error:{type:'rate_limit_exceeded'}},{status:429});
  if(mode==='forbidden')return new Response('private forbidden',{status:403});
  if(mode==='network')throw Error('private timeout');
  if(mode==='top-quota'||mode==='top-rate')return new Response('data: '+JSON.stringify({type:'error',code:mode==='top-quota'?'usage_limit_reached':'rate_limit_exceeded',message:'synthetic private error'})+'\n\n',{headers:{'Content-Type':'text/event-stream'}});
  const output=mode==='partial'?[{type:'response.output_text.delta',delta:'fixture'},{type:'response.failed',response:{error:{type:'usage_limit_reached'}}}]:[{type:'response.completed',response:{status:'completed',output:[],usage:{input_tokens:5,output_tokens:5}}}];
  return new Response(output.map(x=>'data: '+JSON.stringify(x)+'\n\n').join(''),{headers:{'Content-Type':'text/event-stream'}});
 }});
 const app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-admin',codexBinary:'unused',reserveTokens:100},rateReader,responsesTransport:transport,...(clock?{clock}:{})});
 for(const a of ['a','b'])app.store.run("INSERT INTO accounts(id,name,home,state) VALUES(?,?,?,'ready')",a,a,a);
 app.balancer.saveSettings({failover:true});
 const member=app.store.createMember('Fixture',null,null);await new Promise(r=>app.server.listen(0,'127.0.0.1',r));const base='http://127.0.0.1:'+app.server.address().port;
 t.after(async()=>{release?.();await app.close();rmSync(root,{recursive:true,force:true});});
 const send=(thread='01234567-89ab-7cde-8fab-0123456789ab',extra={})=>fetch(base+'/v1/responses',{method:'POST',headers:{Authorization:'Bearer '+member.secret,'Content-Type':'application/json','thread-id':thread},body:JSON.stringify({model:'fixture-model',input:[{role:'user',content:'synthetic'}],stream:false,...extra})});
 return {...app,base,member,send,calls,mode:v=>{mode=v;},release:()=>release?.(),arrivals:()=>arrivals};
}
for(const cause of ['quota','auth','top-quota'])test('confirmed '+cause+' blocks whole account, next request migrates without replay or identity change',async t=>{
 const app=await gateway(t);await app.send();const original=app.store.get('SELECT * FROM sessions');app.mode(cause);
 assert.ok((await app.send()).status>=400);assert.equal(app.calls.length,2);assert.ok(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block);
 app.mode('ok');assert.equal((await app.send()).status,200);const moved=app.store.get('SELECT * FROM sessions');
 assert.equal(moved.id,original.id);assert.equal(moved.client_session_id,original.client_session_id);assert.equal(moved.account_id,'b');assert.equal(moved.route_version,1);assert.notEqual(moved.upstream_thread,original.id);
 assert.equal(app.calls.at(-1).home,'b');assert.equal(app.calls.at(-1).body.input[0].content,'synthetic');
 assert.equal((await app.send('another-client')).status,200);assert.equal(app.calls.at(-1).home,'b');
 assert.deepEqual(app.store.all('SELECT account_id FROM requests ORDER BY rowid').map(r=>r.account_id),['a','a','b','b']);
});
for(const cause of ['rate','forbidden','network','top-rate'])test(cause+' never triggers switching',async t=>{
 const app=await gateway(t);await app.send();app.mode(cause);assert.ok((await app.send()).status>=400);
 assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,null);app.mode('ok');await app.send();assert.equal(app.calls.at(-1).home,'a');
});
test('partial response failure is never replayed; next request may migrate',async t=>{
 const app=await gateway(t);app.mode('partial');const response=await app.send(undefined,{stream:true}),body=await response.text();assert.match(body,/fixture/);assert.equal(app.calls.length,1);
 assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,'quota_exhausted');app.mode('ok');await app.send();assert.equal(app.calls.at(-1).home,'b');
});
test('busy session waits for safe boundary; old success cannot clear newer block; no targets never loops',async t=>{
 const app=await gateway(t);await app.send();app.mode('hold');const pending=app.send();await until(()=>app.arrivals()===2);
 const a=app.store.get("SELECT * FROM accounts WHERE id='a'");app.balancer.block(a,'quota_exhausted');
 assert.equal((await app.send()).status,409);assert.equal(app.calls.length,2);
 app.mode('ok');app.release();assert.equal((await pending).status,200);assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,'quota_exhausted');
 app.balancer.recover(a,{remaining:90,limitReached:false,windows:[]});assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,'quota_exhausted');
 app.balancer.block(app.store.get("SELECT * FROM accounts WHERE id='b'"),'login_expired');assert.equal((await app.send()).status,503);assert.equal(app.calls.length,2);
});
test('admin-only controls, switch-off behavior and manual nightly exclusion',async t=>{
 const app=await gateway(t);await app.send();const s=app.store.get('SELECT * FROM sessions');
 const patch=(path,value,secret='fixture-admin')=>fetch(app.base+path,{method:'PATCH',headers:{Authorization:'Bearer '+secret,'Content-Type':'application/json'},body:JSON.stringify(value)});
 assert.equal((await patch('/api/admin/balancing',{nightly:true},app.member.secret)).status,401);
 assert.equal((await patch('/api/admin/balancing',{nightly:'true'})).status,400);
 assert.equal((await patch('/api/admin/balancing',null)).status,400);
 assert.equal((await patch('/api/admin/sessions/'+s.id,{account_id:'b',expected_route_version:0})).status,200);assert.equal(app.store.get('SELECT nightly_enabled FROM sessions').nightly_enabled,0);
 assert.equal((await patch('/api/admin/sessions/'+s.id,{nightly_enabled:true})).status,200);
 app.balancer.saveSettings({failover:false});app.mode('quota');await app.send();assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='b'").route_block,null);
});

test('scheduler persists one daily execution, versioned moves and restart idempotence',async()=>{
 const store=new Store(':memory:');let clock=at,refreshes=0;const m=store.createMember('Fixture',null,null);
 try{
  for(const a of [account('a',20),account('b',80)])store.run("INSERT INTO accounts(id,name,home,state,quota_json) VALUES(?,?,?,'ready',?)",a.id,a.id,a.id,a.quota_json);
  for(const i of [1,2,3,4]){const s='s'+i;store.run("INSERT INTO sessions(id,member_id,account_id,protocol,created,updated) VALUES(?,?,'a','responses',?,?)",s,m.memberId,at-2*hour,at-hour);store.run("INSERT INTO requests(id,member_id,account_id,session_id,started,ended,status,input,model,ip,agent) VALUES(?,?,'a',?,?,?,'ok',100,'fixture-model','127.0.0.1','fixture')",'r'+i,m.memberId,s,at-2*hour,at-hour);}
  const create=()=>createBalancer({store,preparing:new Set(),clock:()=>clock,refreshQuota:async a=>{refreshes++;return JSON.parse(a.quota_json);}});const b=create();b.saveSettings({nightly:true});
  await Promise.all([b.tick(),b.tick()]);assert.equal(refreshes,2);assert.ok(store.get("SELECT COUNT(*) n FROM sessions WHERE account_id='b'").n>0);assert.equal(store.get('SELECT status FROM balance_runs').status,'completed');
  const routes=store.all('SELECT id,account_id,route_version FROM sessions');await create().tick();assert.equal(refreshes,2);assert.deepEqual(store.all('SELECT id,account_id,route_version FROM sessions'),routes);
  clock+=24*hour+60000;await create().tick();assert.equal(refreshes,2,'missed window never catches up in daytime');
 }finally{store.close();}
});
test('preview computes the complete minimal plan and the nightly batch applies each session only once',async()=>{
 const store=new Store(':memory:');
 try{
  const member=store.createMember('Fixture',null,null),weights=[40,40,15,4,1];
  for(const a of [account('a',60),account('b',40)])store.run("INSERT INTO accounts(id,name,home,state,quota_json) VALUES(?,?,?,'ready',?)",a.id,a.id,a.id,a.quota_json);
  for(const [i,w] of weights.entries()){
   store.run("INSERT INTO sessions(id,member_id,account_id,protocol,created,updated) VALUES(?,?,'b','responses',?,?)",'s'+i,member.memberId,at-2*hour,at-hour);
   store.run("INSERT INTO requests(id,member_id,account_id,session_id,started,ended,status,input,model,ip) VALUES(?,?,'b',?,?,?,'ok',?,'fixture','fixture')",'r'+i,member.memberId,'s'+i,at-2*hour,at-hour,w);
  }
  const read=async a=>JSON.parse(a.quota_json),b=createBalancer({store,preparing:new Set(),clock:()=>at,refreshQuota:read,readQuota:read});
  b.saveSettings({nightly:true,weighted:true});const before=store.all('SELECT id,account_id,route_version FROM sessions ORDER BY id'),auditCount=store.get('SELECT COUNT(*) n FROM audit').n;
  const preview=await b.preview();assert.equal(preview.moves.length,3);
  assert.deepEqual(store.all('SELECT id,account_id,route_version FROM sessions ORDER BY id'),before);assert.equal(store.get('SELECT COUNT(*) n FROM audit').n,auditCount);
  await b.tick();const run=store.get('SELECT * FROM balance_runs'),saved=JSON.parse(run.summary);
  assert.equal(run.status,'completed');assert.deepEqual(saved.moves,preview.moves);assert.equal(saved.applied.length,3);
  assert.equal(store.get("SELECT COUNT(*) n FROM audit WHERE action='session.route'").n,3);
  assert.equal(store.get('SELECT MAX(route_version) n FROM sessions').n,1);
  await b.tick();assert.equal(store.get("SELECT COUNT(*) n FROM audit WHERE action='session.route'").n,3);
 }finally{store.close();}
});
test('structured classifier uses codes, never message substring or ambiguous errors',()=>{
 assert.equal(structuredFailure({error:{type:'usage_limit_reached'}}).routeFailure,'quota_exhausted');
 assert.equal(structuredFailure({code:'refresh_token_expired'}).routeFailure,'login_expired');
 for(const value of [{message:'usage_limit_reached'},null,{type:'rate_limit_exceeded'},{type:'server_error'},{code:'unknown',message:'refresh_token_expired'}])assert.equal(structuredFailure(value),null);
});

test('recovery requires new positive evidence and never moves sessions back',async t=>{
 const app=await gateway(t);await app.send();const a=app.store.get("SELECT * FROM accounts WHERE id='a'");app.balancer.block(a,'quota_exhausted');await app.send();
 const current=app.store.get("SELECT * FROM accounts WHERE id='a'");
 app.balancer.recover(current,{remaining:0,limitReached:true,windows:[]});assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,'quota_exhausted');
 app.balancer.recover(current,{remaining:80,limitReached:false,windows:[{reset:Date.now()-1}]});assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,'quota_exhausted');
 app.balancer.recover(current,{remaining:80,limitReached:false,windows:[{reset:Date.now()+60000}]});assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,null);
 assert.equal(app.store.get('SELECT account_id FROM sessions').account_id,'b');
 app.balancer.block(a,'login_expired');assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,null,'stale failure cannot override newer recovery');
});

test('nightly refresh failure skips cached quota and concurrent manual binding is protected',async()=>{
 const store=new Store(':memory:');const member=store.createMember('Fixture',null,null);
 try{
  for(const a of [account('a',20),account('b',80)])store.run("INSERT INTO accounts(id,name,home,state,quota_json) VALUES(?,?,?,'ready',?)",a.id,a.id,a.id,a.quota_json);
  const b=createBalancer({store,preparing:new Set(),clock:()=>at,refreshQuota:async()=>undefined});b.saveSettings({nightly:true});await b.tick();
  const result=JSON.parse(store.get('SELECT summary FROM balance_runs').summary);assert.equal(result.moves.length,0);assert.equal(result.skipped.filter(s=>s.reason==='quota_unavailable').length,2);
  store.run("INSERT INTO sessions(id,member_id,account_id,protocol,created,updated) VALUES('s',?,'a','responses',?,?)",member.memberId,at,at);
  const original=store.get("SELECT * FROM sessions WHERE id='s'");b.move(original,'b','manual');assert.throws(()=>b.move(original,'a','nightly'),e=>e.code==='session_route_conflict');
  b.saveSettings({nightly:false,failover:false});b.block(store.get("SELECT * FROM accounts WHERE id='b'"),'login_expired');assert.throws(()=>b.route(store.get("SELECT * FROM sessions WHERE id='s'")),e=>e.code==='session_account_blocked');
 }finally{store.close();}
});

test('activity includes requests that started earlier but finished inside the 16-hour window',()=>{
 const p=plan([account('a',20),account('b',80)],[session('long-finished','a'),session('recent','a')],[request('long-finished',100,{started:at-20*hour}),request('recent',100)]);
 assert.equal(p.summaries[0].before.a,200);assert.ok(p.moves.length>0);
});
test('credential refresh timeout is transient while confirmed null account is login loss',async t=>{
 const child=()=>{const c=new EventEmitter();c.stdin=new PassThrough();c.stdout=new PassThrough();c.stderr=new PassThrough();c.kill=()=>{};return c;};
 t.mock.timers.enable({apis:['setTimeout']});const hanging=child();
 const timeout=refreshCredentials('fixture','unused',{spawnProcess:()=>hanging});
 const checked=assert.rejects(timeout,e=>e.code==='upstream_auth_refresh'&&!e.routeFailure);t.mock.timers.tick(15000);await checked;
 const expired=child();const pending=refreshCredentials('fixture','unused',{spawnProcess:()=>expired});
 expired.stdout.write(JSON.stringify({id:1,result:{}})+'\n');expired.stdout.write(JSON.stringify({id:2,result:{account:null}})+'\n');
 await assert.rejects(pending,e=>e.routeFailure==='login_expired');
});

test('known zero-use rejection does not poison the whole account group',()=>{
 const accounts=[account('a',20),account('b',80)],sessions=[session('one','a'),session('two','a')],requests=[request('one',100),request('two',100)];
 const before=plan(accounts,sessions,requests),after=plan(accounts,[...sessions,session('invalid','a')],[...requests,request('invalid',0,{status:'failed'})]);
 assert.deepEqual(after.moves,before.moves);assert.ok(after.skipped.some(s=>s.session_id==='invalid'&&s.reason==='no_measured_work'));assert.ok(!after.skipped.some(s=>s.reason==='unknown_fixed_load'));
});
test('preview never persists refreshed quota, account blocks, recoveries, settings or audits',async t=>{
 let exhausted=true;
 const app=await gateway(t,{rateReader:async()=>({rateLimits:{planType:'same-plan',rateLimitReachedType:exhausted?'quota':null,primary:{usedPercent:exhausted?100:20,windowDurationMins:300,resetsAt:Math.floor(Date.now()/1000)+18000}}})});
 app.balancer.block(app.store.get("SELECT * FROM accounts WHERE id='b'"),'quota_exhausted');
 const snapshot=()=>JSON.stringify({accounts:app.store.all('SELECT * FROM accounts'),sessions:app.store.all('SELECT * FROM sessions'),settings:app.store.all('SELECT * FROM gateway_settings'),audit:app.store.all('SELECT * FROM audit'),runs:app.store.all('SELECT * FROM balance_runs')});
 for(const enabled of [true,false]){
  app.balancer.saveSettings({failover:enabled,nightly:enabled});
  for(const value of [true,false]){exhausted=value;const before=snapshot();const result=await fetch(app.base+'/api/admin/balancing/preview',{method:'POST',headers:{Authorization:'Bearer fixture-admin'}});assert.equal(result.status,200);await result.json();assert.equal(snapshot(),before);}
 }
});
test('daytime preview separates historical window, live tasks and recent-activity protection',()=>{
 const noon=Date.parse('2026-10-14T12:00:00+08:00');
 const accounts=[account('a',20),account('b',80)].map(a=>({...a,quota_json:JSON.stringify({...JSON.parse(a.quota_json),checked:noon,windows:[{used:a.id==='a'?80:20,minutes:300,reset:noon+hour}]})}));
 const sessions=[session('one','a'),session('two','a'),session('outside','b'),session('running','b')];
 const requests=[request('one',100),request('two',100),request('outside',1000,{started:noon-9*hour,ended:noon-8*hour})];
 const p=plan(accounts,sessions,requests,{at:noon});assert.equal(p.summaries[0].before.b,0);assert.equal(p.summaries[0].fixedLoad.b,0);
 const live=plan(accounts,sessions,[...requests,request('running',0,{started:noon-10000,ended:null,status:'running'}),request('one',500,{started:noon-600000,ended:noon-300000})],{at:noon});
 assert.ok(live.summaries[0].fixed.includes('running'));assert.ok(live.summaries[0].fixed.includes('one'));assert.equal(live.summaries[0].before.a,200);assert.equal(live.summaries[0].before.b,100);
});

for(const enabled of [true,false])test('pre-fault preview cannot supply recovery evidence to a new generation, switches '+enabled,async t=>{
 const pending=[],positive=()=>({rateLimits:{planType:'same-plan',primary:{usedPercent:20,windowDurationMins:300,resetsAt:Math.floor(Date.now()/1000)+18000}}});
 const app=await gateway(t,{rateReader:async(_binary,home)=>home==='a'?new Promise(resolve=>pending.push(resolve)):positive()});
 const headers={Authorization:'Bearer fixture-admin'};let preview,refresh;
 try{
  preview=fetch(app.base+'/api/admin/balancing/preview',{method:'POST',headers});await until(()=>pending.length===1);
  app.balancer.block(app.store.get("SELECT * FROM accounts WHERE id='a'"),'quota_exhausted');app.balancer.saveSettings({failover:enabled,nightly:enabled});
  refresh=fetch(app.base+'/api/admin/accounts/a/quota',{method:'POST',headers});await until(()=>pending.length===2);
  pending[0](positive());assert.equal((await preview).status,200);
  assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,'quota_exhausted');
  pending[1](positive());assert.equal((await refresh).status,200);
  assert.equal(app.store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,null,'only fresh post-fault evidence can recover');
 }finally{for(const resolve of pending)resolve(positive());await Promise.allSettled([preview,refresh].filter(Boolean));}
});

const timedAccount=(id,remaining,hours,checked=at)=>({...account(id,remaining),quota_json:JSON.stringify({plan:'same-plan',checked,remaining,windows:[{used:100-remaining,minutes:10080,reset:checked+hours*hour}],limitReached:false})});
test('24-hour budgets keep positive balances positive and do not amplify a nearly expired tail',()=>{
 for(const [remaining,hours,expected] of [[30,24,68.9655172414],[30,12,68.9655172414],[5,1,27.0270270270],[1,1,6.8965517241],[0,1,0]]){
  const result=automaticWeights([timedAccount('a',remaining,hours),timedAccount('b',90,160)],at);
  assert.equal(result.reason,null);assert.ok(Math.abs(result.weights.get('a')-expected)<1e-8);
  if(remaining>0){assert.ok(result.weights.get('a')>0&&result.weights.get('a')<100);assert.ok(result.weights.get('b')>0&&result.weights.get('b')<100);}
 }
 assert.ok(automaticWeights([timedAccount('a',0.000001,0.001),timedAccount('b',90,160)],at).weights.get('a')>0);
 const a=timedAccount('a',30,24),b=timedAccount('b',90,160);
 assert.equal(automaticWeights([a,{...b,quota_json:null}],at).reason,'quota_unavailable');
 assert.equal(automaticWeights([a,timedAccount('b',90,0)],at).reason,'quota_unavailable');
 const multi=JSON.parse(b.quota_json);multi.windows.push({...multi.windows[0],minutes:300});
 assert.equal(automaticWeights([a,{...b,quota_json:JSON.stringify(multi)}],at).reason,'incomparable_quota');
});
test('new sessions use atomic smooth weights and duplicate client sessions are not allocated again',async t=>{
 const checked=Date.now(),app=await gateway(t,{clock:()=>checked});
 for(const a of [timedAccount('a',30,24,checked),timedAccount('b',90,160,checked)])app.store.run('UPDATE accounts SET quota_json=? WHERE id=?',a.quota_json,a.id);
 app.balancer.saveSettings({weighted:true});
 const replies=await Promise.all(Array.from({length:100},(_,i)=>app.send('weighted-'+i)));assert.ok(replies.every(r=>r.status===200));
 const counts=app.store.all('SELECT account_id,COUNT(*) n FROM sessions GROUP BY account_id');assert.deepEqual(Object.fromEntries(counts.map(r=>[r.account_id,r.n])),{a:69,b:31});
 const before=app.store.get("SELECT value FROM gateway_settings WHERE name='routing_schedule'").value;
 await Promise.all(Array.from({length:8},()=>app.send('weighted-0')));
 assert.equal(app.store.get("SELECT value FROM gateway_settings WHERE name='routing_schedule'").value,before);
 assert.equal(app.store.get('SELECT COUNT(*) n FROM sessions').n,100);
 const admin=await fetch(app.base+'/api/admin/state',{headers:{Authorization:'Bearer fixture-admin'}});const state=await admin.json();assert.equal(state.balancing.weights.mode,'auto');
 assert.ok(Math.abs(state.balancing.weights.accounts[0].automatic-68.9655172414)<1e-8);
});
test('manual 100/0 keeps zero-weight fallback and official zero overrides the preference without replay',async t=>{
 const app=await gateway(t);const checked=Date.now();
 for(const a of [timedAccount('a',30,24,checked),timedAccount('b',90,160,checked)])app.store.run('UPDATE accounts SET quota_json=? WHERE id=?',a.quota_json,a.id);
 app.balancer.saveSettings({weighted:true});
 const patch=(body,key='fixture-admin')=>fetch(app.base+'/api/admin/balancing/weights',{method:'PATCH',headers:{Authorization:'Bearer '+key,'Content-Type':'application/json'},body:JSON.stringify(body)});
 assert.equal((await patch({mode:'manual',weights:{a:100,b:0},expected_version:0},app.member.secret)).status,401);
 for(const weights of [{a:101,b:-1},{a:50,b:49},{a:0,b:0},{forged:100},{a:'100'},{a:99.999,b:0.001}])assert.equal((await patch({mode:'manual',weights,expected_version:0})).status,400);
 assert.equal((await patch({mode:'manual',weights:{a:100,b:0},expected_version:0})).status,200);
 assert.equal((await patch({mode:'auto',expected_version:0})).status,409);
 await app.send();assert.equal(app.calls.at(-1).home,'a');
 app.store.run('UPDATE accounts SET quota_json=? WHERE id=?',timedAccount('a',0,24,Date.now()).quota_json,'a');
 assert.equal((await app.send()).status,200);assert.equal(app.calls.at(-1).home,'b');assert.equal(app.calls.length,2);
 assert.equal(app.balancer.status().weights.mode,'failover');assert.equal(app.balancer.status().weights.configuration.weights.a,100);
 assert.equal((await app.send('new-after-zero')).status,200);assert.equal(app.calls.at(-1).home,'b');
 assert.equal((await patch({mode:'auto',expected_version:1})).status,200);
});
test('manual settings expire at the next Beijing 02:00 even with nightly disabled, restart or missed trigger',async()=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-weight-expiry-'));let store,clock=Date.parse('2026-10-15T23:00:00+08:00');
 try{
  store=new Store(join(root,'db.sqlite'));for(const a of ['a','b'])store.run("INSERT INTO accounts(id,name,home,state) VALUES(?,?,?,'ready')",a,a,a);
  const create=()=>createBalancer({store,preparing:new Set(),clock:()=>clock,refreshQuota:async()=>null});
  let b=create();b.saveSettings({weighted:true});b.saveWeights({mode:'manual',weights:{a:100,b:0},expected_version:0});
  const expires=Date.parse('2026-10-16T02:00:00+08:00');assert.equal(b.status().weights.configuration.expires,expires);assert.equal(nextWeightReset(expires),expires+24*hour);
  store.db.close();store=new Store(join(root,'db.sqlite'));b=create();assert.equal(b.status().weights.mode,'manual');
  clock=expires+3*hour;assert.equal(b.status().weights.configuration.mode,'auto','deadline is enforced before maintenance');
  await b.tick();assert.equal(b.status().weights.configuration.mode,'auto');assert.equal(b.status().weights.configuration.version,2);assert.equal(store.get('SELECT COUNT(*) n FROM balance_runs').n,0);
  const before=store.get("SELECT value FROM gateway_settings WHERE name='routing_schedule'");
  assert.throws(()=>b.atomic(()=>{b.selectAccount();throw Error('rollback');}));assert.deepEqual(store.get("SELECT value FROM gateway_settings WHERE name='routing_schedule'"),before);
 }finally{store?.db.close();rmSync(root,{recursive:true,force:true});}
});
test('nightly uses the same hourly weights and accepts quota recovered by its own refresh',async()=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-weight-night-'));let store;
 try{
  store=new Store(join(root,'db.sqlite'));const m=store.createMember('Fixture',null,null);
  for(const a of [timedAccount('a',30,24),timedAccount('b',90,160)])store.run("INSERT INTO accounts(id,name,home,state,quota_json) VALUES(?,?,?,'ready',?)",a.id,a.id,a.id,a.quota_json);
  for(let i=0;i<10;i++){store.run("INSERT INTO sessions(id,member_id,account_id,protocol,created,updated) VALUES(?,?,'b','responses',?,?)",'night-'+i,m.memberId,at-2*hour,at-hour);store.run("INSERT INTO requests(id,member_id,session_id,account_id,started,ended,status,ip,model,input) VALUES(?,?,?,'b',?,?,'ok','fixture','fixture',10)",'r'+i,m.memberId,'night-'+i,at-2*hour,at-hour);}
  const b=createBalancer({store,preparing:new Set(),clock:()=>at,refreshQuota:async a=>{const q=JSON.parse(a.quota_json);b.recover(a,q);return q;}});
  b.saveSettings({weighted:true,nightly:true,failover:true});b.block(store.get("SELECT * FROM accounts WHERE id='a'"),'quota_exhausted');
  await b.tick();const result=JSON.parse(store.get('SELECT summary FROM balance_runs').summary);
  assert.equal(store.get("SELECT route_block FROM accounts WHERE id='a'").route_block,null);
  assert.ok(Math.abs(result.summaries[0].target.a-68.9655172414)<1e-8);assert.equal(store.get("SELECT COUNT(*) n FROM sessions WHERE account_id='a'").n,7);
 }finally{store?.db.close();rmSync(root,{recursive:true,force:true});}
});
test('weighted switch preserves the old balancing settings contract across disable and restart',()=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-weight-rollback-'));let store;
 try{
  store=new Store(join(root,'db.sqlite'));const create=()=>createBalancer({store,preparing:new Set(),clock:()=>at,refreshQuota:async()=>null});
  let b=create();b.saveSettings({weighted:true,failover:true,nightly:true});
  assert.deepEqual(JSON.parse(store.get("SELECT value FROM gateway_settings WHERE name='balancing'").value),{failover:true,nightly:true,quietMinutes:30});
  b.saveSettings({weighted:false});assert.deepEqual(JSON.parse(store.get("SELECT value FROM gateway_settings WHERE name='balancing'").value),{failover:true,nightly:true,quietMinutes:30});
  store.db.close();store=new Store(join(root,'db.sqlite'));b=create();assert.equal(b.settings().weighted,false);assert.equal(b.settings().nightly,true);
  store.run("UPDATE gateway_settings SET value=? WHERE name='balancing'",JSON.stringify({failover:true,nightly:true,quietMinutes:30,weighted:true}));b=create();assert.ok(!Object.hasOwn(JSON.parse(store.get("SELECT value FROM gateway_settings WHERE name='balancing'").value),'weighted'));assert.equal(b.settings().weighted,false,'existing independent setting wins over transitional data');
 }finally{store?.db.close();rmSync(root,{recursive:true,force:true});}
});
