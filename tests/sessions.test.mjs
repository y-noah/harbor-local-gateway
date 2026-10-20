import {test} from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtempSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createGateway} from '../server.mjs';
import {Store} from '../store.mjs';

async function setup(t){
 const root=mkdtempSync(join(tmpdir(),'harbor-session-controls-')),calls=[];let hold=null;
 const app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-admin',codexBinary:'fixture-cli',reserveTokens:100},checker:async()=>true,rateReader:async()=>({rateLimits:{primary:{usedPercent:5,windowDurationMins:300,resetsAt:Math.floor(Date.now()/1000)+18000}}}),responsesTransport:{models:async()=>[],run:async args=>{calls.push(args);if(hold)await hold;return {input:7,output:3,cached:0,usageKnown:true,response:{status:'completed',output:[]},terminal:{type:'response.completed',response:{status:'completed',output:[]}}};}}});
 for(const [id,home] of [['account-a','home-a'],['account-b','home-b']])app.store.run("INSERT INTO accounts(id,name,home,state) VALUES(?,?,?,'ready')",id,id,home);
 await new Promise(r=>app.server.listen(0,'127.0.0.1',r));const base='http://127.0.0.1:'+app.server.address().port;
 t.after(async()=>{await app.close();rmSync(root,{recursive:true,force:true});});
 const member=app.store.createMember('Fixture member',null,null),other=app.store.createMember('Other member',null,null);
 async function request(path,body,token='fixture-admin',method='PATCH',headers={}){const r=await fetch(base+path,{method,headers:{'Content-Type':'application/json',Authorization:'Bearer '+token,...headers},body:body===undefined?undefined:JSON.stringify(body)});return {status:r.status,body:await r.json()};}
 const send=(thread='thread-one',input=[{role:'user',content:'首次用户问题'}],person=member,extra={})=>request('/v1/responses',{model:'fixture-model',input,stream:false,...extra},person.secret,'POST',{'thread-id':thread});
 const session=()=>app.store.get('SELECT * FROM sessions WHERE member_id=? ORDER BY created LIMIT 1',member.memberId);
 return {...app,base,member,other,calls,request,send,session,hold:()=>{let release;hold=new Promise(r=>release=r);return ()=>{hold=null;release();};}};
}
const until=async(fn)=>{const end=Date.now()+3000;while(!await fn()){assert.ok(Date.now()<end,'condition timeout');await new Promise(r=>setTimeout(r,5));}};

test('legacy titles are erased on reopen without changing routing or usage',()=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-title-retirement-')),file=join(root,'gateway.sqlite');let store=new Store(file);
 try{
  const m=store.createMember('Fixture',null,null);
  store.run("INSERT INTO accounts(id,name,home,state) VALUES('account-a','Fixture','unused','ready')");
  store.run("INSERT INTO sessions(id,member_id,account_id,title,protocol,state,enabled,route_version,upstream_thread,created,updated) VALUES('legacy',?,'account-a','private synthetic text','responses','active',0,2,'isolated-thread',?,?)",m.memberId,Date.now(),Date.now());
  store.run('ALTER TABLE sessions DROP COLUMN client_session_id');
  const before=store.get("SELECT * FROM sessions WHERE id='legacy'");store.close();store=new Store(file);
  assert.deepEqual({...store.get("SELECT * FROM sessions WHERE id='legacy'")},{...before,title:null,client_session_id:null});
  assert.ok(!('title' in store.snapshot().sessions[0]));
 }finally{store.close();rmSync(root,{recursive:true,force:true});}
});

test('new requests do not capture content and admin title writes are rejected',async t=>{
 const app=await setup(t);await app.send('privacy-thread',[{role:'user',content:'synthetic confidential body'}]);const s=app.session();
 assert.equal(s.title,null);
 const path='/api/admin/sessions/'+s.id;
 assert.equal((await app.request(path,{title:'synthetic private title'})).status,400);
 assert.equal((await app.request(path,{title:'synthetic private title',enabled:false})).status,400);
 assert.equal(app.session().enabled,1);
 const state=await app.request('/api/admin/state',undefined,'fixture-admin','GET');assert.ok(!('title' in state.body.sessions[0]));
 assert.ok(!JSON.stringify(state.body).includes('synthetic confidential body'));
 assert.ok(!app.store.all('SELECT detail FROM audit').some(a=>a.detail.includes('synthetic')));
});

test('manual A-B-A routing preserves session, history, request attribution and other sessions',async t=>{
 const app=await setup(t);assert.equal((await app.send()).status,200);const s=app.session();assert.equal(s.title,null);assert.equal(s.account_id,'account-a');
 await app.send('other-thread',[{role:'user',content:'另一个会话'}]);const otherSession=app.store.get('SELECT * FROM sessions WHERE id<>?',s.id);
 const edit=b=>app.request('/api/admin/sessions/'+s.id,b);
 assert.equal((await edit({account_id:'account-b',expected_route_version:0})).status,200);
 const switched=app.session();assert.equal(switched.id,s.id);assert.notEqual(switched.upstream_thread,s.id);assert.equal(switched.route_version,1);
 const history=[{role:'user',content:'首次用户问题'},{type:'reasoning',encrypted_content:'opaque-fixture'},{type:'function_call',call_id:'c1',name:'read_file',arguments:'{}'},{type:'function_call_output',call_id:'c1',output:'synthetic result'},{role:'user',content:'继续'}];
 assert.equal((await app.send('thread-one',history)).status,200);assert.equal(app.calls.at(-1).home,'home-b');assert.deepEqual(app.calls.at(-1).payload.input,history);assert.equal(app.calls.at(-1).sessionId,switched.upstream_thread);assert.equal(app.calls.at(-1).payload.prompt_cache_key,switched.upstream_thread);
 assert.equal((await edit({account_id:'account-b',expected_route_version:1})).body.route_version,1);
 assert.equal((await edit({account_id:'account-a',expected_route_version:0})).status,409);
 assert.equal((await edit({account_id:'account-a',expected_route_version:1})).status,200);
 assert.equal((await app.send('thread-one',history)).status,200);assert.equal(app.calls.at(-1).home,'home-a');assert.notEqual(app.calls.at(-1).sessionId,switched.upstream_thread);
 assert.equal(app.store.get('SELECT * FROM sessions WHERE id=?',otherSession.id).account_id,otherSession.account_id);
 assert.deepEqual(app.store.all('SELECT account_id FROM requests WHERE session_id=? ORDER BY started,rowid',s.id).map(r=>r.account_id),['account-a','account-b','account-a']);
 assert.equal(app.store.get("SELECT budget_used FROM accounts WHERE id='account-a'").budget_used,20);
 assert.equal(app.store.get("SELECT budget_used FROM accounts WHERE id='account-b'").budget_used,20);
 assert.equal(app.store.get('SELECT title FROM sessions WHERE id=?',s.id).title,null);
});

test('session mutation authenticates admin and validates target and protocol',async t=>{
 const app=await setup(t);await app.send();const s=app.session(),path='/api/admin/sessions/'+s.id;
 assert.equal((await app.request(path,{title:'forged'},app.member.secret)).status,401);
 assert.equal((await app.request(path,{title:'forged'},app.other.secret)).status,401);
 assert.equal((await app.request(path,{account_id:'account-b'})).status,400);
 for(const values of ["enabled=0","removed_at=1","state='offline'","state='degraded'","kind='claude'"]){
  app.store.run("UPDATE accounts SET enabled=1,removed_at=NULL,state='ready',kind='codex' WHERE id='account-b'");app.store.run("UPDATE accounts SET "+values+" WHERE id='account-b'");
  assert.equal((await app.request(path,{account_id:'account-b',expected_route_version:0})).status,400);
 }
 assert.equal(app.session().account_id,s.account_id);
 app.store.run('UPDATE sessions SET title=NULL WHERE id=?',s.id);await app.send('thread-one',[{role:'user',content:'不要回填这个标题'}]);assert.equal(app.session().title,null);
 app.store.run("UPDATE sessions SET protocol='chat' WHERE id=?",s.id);
 assert.equal((await app.request(path,{account_id:'account-b',expected_route_version:0})).status,409);
 assert.equal((await app.request(path,{enabled:false})).status,200);
 const result=await app.request('/v1/chat/completions',{model:'codex',messages:[{role:'user',content:'继续'}],session_id:s.id},app.member.secret,'POST');assert.equal(result.status,403);assert.equal(result.body.error.type,'session_disabled');
});

test('disabled session blocks subsequent access and forged flags but lets current request finish and allows restore',async t=>{
 const app=await setup(t);await app.send();const s=app.session(),path='/api/admin/sessions/'+s.id;
 const release=app.hold();const pending=app.send();await until(()=>app.calls.length===2);
 assert.equal((await app.request(path,{account_id:'account-b',expected_route_version:0})).status,409);
 assert.equal((await app.request(path,{enabled:false})).status,200);
 assert.equal((await app.send('thread-one',[],app.member,{enabled:true})).status,403);
 assert.equal((await app.request(path,{account_id:'account-b',expected_route_version:0})).status,409);
 release();assert.equal((await pending).status,200);
 assert.equal((await app.send('different-thread')).status,200);
 assert.equal((await app.send('thread-one',undefined,app.other)).status,200);
 assert.equal((await app.request(path,{enabled:true})).status,200);assert.equal((await app.send()).status,200);
 assert.equal(app.session().id,s.id);assert.equal(app.session().account_id,s.account_id);
});

test('slow request preparation prevents routing until its binding has been resolved',async t=>{
 const app=await setup(t);await app.send();const s=app.session();
 const payload=JSON.stringify({model:'fixture-model',input:[{role:'user',content:'准备中'}],stream:false});
 let req;const pending=new Promise((resolve,reject)=>{req=http.request(app.base+'/v1/responses',{method:'POST',headers:{'Content-Type':'application/json','Content-Length':Buffer.byteLength(payload),Authorization:'Bearer '+app.member.secret,'thread-id':'thread-one'}},r=>{r.resume();r.on('end',()=>resolve(r.statusCode));});req.on('error',reject);req.write(payload.slice(0,12));});
 t.after(()=>req.destroy());
 await until(async()=>{const r=await app.request('/api/admin/state',undefined,'fixture-admin','GET');return r.body.preparing===1;});
 assert.equal((await app.request('/api/admin/sessions/'+s.id,{account_id:'account-b',expected_route_version:0})).status,409);
 req.end(payload.slice(12));assert.equal(await pending,200);
 assert.equal((await app.request('/api/admin/sessions/'+s.id,{account_id:'account-b',expected_route_version:0})).status,200);
});

test('client Session is displayed unchanged, isolated by employee and stable after routing',async t=>{
 const app=await setup(t),client='01234567-89ab-7cde-8fab-0123456789ab';
 assert.equal((await app.send(client)).status,200);const first=app.session();
 assert.equal(first.client_session_id,client);assert.notEqual(first.id,client);
 assert.equal(app.calls.at(-1).sessionId,first.id);
 assert.equal((await app.send(client,undefined,app.other)).status,200);
 const other=app.store.get('SELECT * FROM sessions WHERE member_id=?',app.other.memberId);
 assert.equal(other.client_session_id,client);assert.notEqual(other.id,first.id);
 assert.equal((await app.request('/api/admin/sessions/'+first.id,{account_id:'account-b',expected_route_version:0})).status,200);
 assert.equal((await app.send(client)).status,200);assert.equal(app.session().id,first.id);assert.equal(app.session().client_session_id,client);
 assert.equal(app.session().account_id,'account-b');assert.equal(app.store.get('SELECT account_id FROM sessions WHERE id=?',other.id).account_id,other.account_id);
 const state=await app.request('/api/admin/state',undefined,'fixture-admin','GET');
 assert.equal(state.body.sessions.find(x=>x.id===first.id).client_session_id,client);
 assert.equal((await app.request('/api/admin/sessions/'+first.id,{client_session_id:'forged'})).status,400);
 assert.equal((await app.request('/api/admin/sessions/'+first.id,{enabled:false})).status,200);
 assert.equal((await app.send(client)).status,403);
});
test('legacy hashed sessions backfill only the matching client UUID without replacing their binding',async t=>{
 const app=await setup(t),client='ABCDEF01-2345-6789-ABCD-0123456789AB';await app.send(client);
 const first=app.session();app.store.run('UPDATE sessions SET client_session_id=NULL WHERE id=?',first.id);
 assert.equal(app.store.snapshot().sessions.find(x=>x.id===first.id).client_session_id,null);
 await app.send(client);const next=app.session();
 assert.equal(next.client_session_id,client);assert.equal(next.id,first.id);assert.equal(next.account_id,first.account_id);assert.equal(next.route_version,first.route_version);
 assert.equal(app.store.get('SELECT COUNT(*) n FROM sessions WHERE member_id=?',app.member.memberId).n,1);
});
test('missing and non-UUID client identifiers never become display metadata',async t=>{
 const app=await setup(t);
 for(const client of ['synthetic-secret-not-a-session','<img src=x onerror=alert(1)>','01234567-89ab-7cde-8fab-0123456789ab-extra'])assert.equal((await app.send(client)).status,200);
 assert.equal((await app.request('/v1/responses',{model:'fixture-model',input:[],stream:false},app.member.secret,'POST')).status,200);
 assert.equal((await app.request('/v1/responses',{model:'fixture-model',input:[],stream:false,prompt_cache_key:'01234567-89ab-7cde-8fab-0123456789ab\n'},app.member.secret,'POST')).status,200);
 assert.equal(app.store.all('SELECT client_session_id FROM sessions').every(x=>x.client_session_id===null),true);
 const state=await app.request('/api/admin/state',undefined,'fixture-admin','GET');
 assert.ok(!JSON.stringify(state.body.sessions).includes('synthetic-secret'));assert.ok(!JSON.stringify(state.body.sessions).includes('onerror'));
});

test('admin session pages cover more than 100 records with stable ties, valid boundaries and unchanged details',async t=>{
 const app=await setup(t),at=Date.now(),start=Math.floor((at+8*3600000)/86400000)*86400000-8*3600000-6*86400000;
 for(let i=0;i<130;i++)app.store.run("INSERT INTO sessions(id,member_id,account_id,protocol,state,created,updated) VALUES(?,?,'account-a','responses','active',?,?)",'page-'+String(i).padStart(3,'0'),app.member.memberId,at-1000,at);
 for(const [sid,updated] of [['expired',start-1],['future',start+7*86400000]])app.store.run("INSERT INTO sessions(id,member_id,account_id,created,updated) VALUES(?,?,'account-a',?,?)",sid,app.member.memberId,at-1000,updated);
 const observed=app.store.observe(app.store.authenticate(app.member.secret),'synthetic','fixture','fixture');app.store.run("UPDATE requests SET session_id='page-000',started=?,ended=?,status='ok',input=5,output=2 WHERE id=?",at-500,at,observed);
 const read=query=>app.request('/api/admin/state'+query,undefined,'fixture-admin','GET');
 const first=(await read('')).body;assert.deepEqual(first.sessionsPagination,{page:1,pageSize:25,total:130,totalPages:6});assert.equal(first.sessions.length,25);
 const ids=[];for(let page=1;page<=6;page++){const result=await read('?session_page='+page);assert.equal(result.status,200);ids.push(...result.body.sessions.map(s=>s.id));}
 assert.deepEqual(ids,Array.from({length:130},(_,i)=>'page-'+String(129-i).padStart(3,'0')));assert.equal(new Set(ids).size,130);
 const tail=(await read('?session_page=999')).body;assert.equal(tail.sessionsPagination.page,6);assert.equal(tail.sessions.length,5);assert.equal(tail.sessions.at(-1).id,'page-000');assert.equal(tail.sessions.at(-1).request_count,1);assert.equal(tail.sessions.at(-1).account_id,'account-a');
 const large=(await read('?session_page=2&session_page_size=100')).body;assert.deepEqual(large.sessionsPagination,{page:2,pageSize:100,total:130,totalPages:2});assert.equal(large.sessions.length,30);
 assert.equal((await app.request('/api/admin/state?session_page=2',undefined,app.member.secret,'GET')).status,401);
 for(const query of ['?session_page=0','?session_page=-1','?session_page=1.5','?session_page=1e2','?session_page=abc','?session_page=01','?session_page=','?session_page=9007199254740992','?session_page_size=0','?session_page_size=101','?session_page_size=2.5','?session_page=1&session_page=2','?session_page_size=25&session_page_size=50'])assert.equal((await read(query)).status,400,query);
 app.store.run("UPDATE sessions SET updated=? WHERE id>='page-104' AND id<='page-129'",start-1);
 const reduced=(await read('?session_page=6')).body;assert.deepEqual(reduced.sessionsPagination,{page:5,pageSize:25,total:104,totalPages:5});assert.equal(reduced.sessions.length,4);
 app.store.run('UPDATE sessions SET updated=?',start-1);const empty=(await read('?session_page=999')).body;assert.deepEqual(empty.sessionsPagination,{page:1,pageSize:25,total:0,totalPages:1});assert.deepEqual(empty.sessions,[]);
});
