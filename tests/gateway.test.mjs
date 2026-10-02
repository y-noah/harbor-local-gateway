import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { resolve, join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import http from 'node:http';
import { createGateway,recoverCodexBinary } from '../server.mjs';
import { Store } from '../store.mjs';
import {accountCapacity,normalizeLimits} from '../rates.mjs';
import {usageDelta,argsFor,codexAuthArgs} from '../codex.mjs';
import {parseClaudeResult,claudeArgs,claudeEnv} from '../claude.mjs';
import {passwordHash,createAdminAuth} from '../auth.mjs';
const scratch=resolve(fileURLToPath(new URL('../../../work/',import.meta.url)),'tests');mkdirSync(scratch,{recursive:true});
function temp(){return mkdtempSync(join(scratch,'case-'));}
function cleanup(dir){const target=resolve(dir);if(!target.startsWith(scratch+'\\')&&!target.startsWith(scratch+'/'))throw Error('Unsafe cleanup');rmSync(target,{recursive:true,force:true});}
test('desktop CLI upgrade recovers a removed managed binary without replacing custom paths',()=>{
  const dir=temp(),folder=join(dir,'bin'),latest=join(folder,'new-version','codex.exe'),old=join(folder,'old-version','codex.exe');
  try{
    mkdirSync(dirname(latest),{recursive:true});writeFileSync(latest,'fixture');
    assert.equal(recoverCodexBinary(old,folder),latest);
    assert.equal(recoverCodexBinary(latest,folder),latest);
    const custom=join(dir,'custom','codex.exe');assert.equal(recoverCodexBinary(custom,folder),custom);
    assert.equal(recoverCodexBinary('codex',folder),'codex');
  }finally{cleanup(dir);}
});
test('Codex calls explicitly isolate the credential store for new and resumed sessions',()=>{
  for(const session of [undefined,'upstream-session']){
    const args=argsFor('isolated-workspace','codex',session);
    assert.ok(args.includes('cli_auth_credentials_store="file"'));
  }
  assert.deepEqual(codexAuthArgs(),['-c','cli_auth_credentials_store="file"']);
});
async function setup(t,executor,rateReader,extra={}){
  const dir=temp(),app=createGateway({dataDir:join(dir,'data'),runtimeDir:join(dir,'runtime'),config:{adminToken:'test-admin',codexBinary:'unused',reserveTokens:100},checker:async()=>true,rateReader:rateReader||(async()=>({rateLimits:{primary:{usedPercent:0,windowDurationMins:300,resetsAt:Math.floor(Date.now()/1000)+18000}}})),executor:executor||(async()=>({text:'你好',input:60,output:10,cached:20,usageKnown:true,threadId:'fake-thread'})),...extra});
  app.store.run("INSERT INTO accounts(id,name,home,state) VALUES('one','Test','unused','ready')");
  await new Promise(r=>app.server.listen(0,'127.0.0.1',r));const base='http://127.0.0.1:'+app.server.address().port;
  t.after(async()=>{await app.close();cleanup(dir);});
  const call=async(path,method='GET',body,secret='test-admin',extra={})=>{const r=await fetch(base+path,{method,headers:{Authorization:'Bearer '+secret,...(body!==undefined?{'Content-Type':'application/json'}:{}),...extra},...(body!==undefined?{body:JSON.stringify(body)}:{})});const b=await r.json();return {status:r.status,body:b};};
  const member=(quota=1000,expires=null)=>app.store.createMember('Test user',quota,expires);
  const chat=(secret,extra={})=>call('/v1/chat/completions','POST',{model:'codex',messages:[{role:'user',content:'hello'}],...extra},secret);
  return {...app,base,call,member,chat};
}
test('admin protection, host and origin guards',async t=>{const {call,base}=await setup(t);assert.equal((await call('/api/admin/state','GET',undefined,'wrong')).status,401);assert.equal((await call('/api/admin/state','GET',undefined,'test-admin',{Origin:'https://evil.test'})).status,403);const status=await new Promise((resolve,reject)=>{http.get(base+'/health',{headers:{Host:'evil.test'}},r=>{r.resume();resolve(r.statusCode);}).on('error',reject);});assert.equal(status,403);assert.equal((await call('/api/admin/state')).status,200);});
test('password-only login sets private cookie, guards admin and invalidates logout',async t=>{
  const app=await setup(t,undefined,undefined,{config:{adminToken:'test-admin',adminPasswordHash:passwordHash('Test!Password-Only-42'),codexBinary:'unused',reserveTokens:100}});
  const post=async(password,extra={})=>fetch(app.base+'/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json',...extra},body:JSON.stringify({password})});
  assert.equal((await post('bad')).status,401);assert.equal((await post('Test!Password-Only-42',{Origin:'https://evil.test'})).status,403);
  const login=await post('Test!Password-Only-42');assert.equal(login.status,200);const cookie=login.headers.get('set-cookie');assert.match(cookie,/HttpOnly/);assert.match(cookie,/SameSite=Strict/);assert.ok(!cookie.includes('Password'));
  const headers={Cookie:cookie.split(';')[0]};assert.equal((await fetch(app.base+'/api/admin/state',{headers})).status,200);
  assert.equal((await fetch(app.base+'/api/admin/state')).status,401);
  assert.deepEqual(await(await fetch(app.base+'/api/auth/session',{headers})).json(),{authenticated:true});
  await fetch(app.base+'/api/auth/logout',{method:'POST',headers});assert.equal((await fetch(app.base+'/api/admin/state',{headers})).status,401);
  for(let i=0;i<4;i++)assert.equal((await post('bad')).status,401);
  assert.equal((await post('bad')).status,401);assert.equal((await post('Test!Password-Only-42')).status,429);
});
test('password sessions expire and are not shared with another server instance',()=>{
  let at=0;const config={adminPasswordHash:passwordHash('test-password')};const auth=createAdminAuth(config,{clock:()=>at});let cookie;
  auth.login('test-password',{setHeader:(_,v)=>cookie=v});const req={headers:{cookie}};assert.equal(auth.authenticated(req),true);
  assert.equal(createAdminAuth(config).authenticated(req),false);at=12*3600000;assert.equal(auth.authenticated(req),false);
});

test('missing or empty admin token cannot become a predictable bearer credential',async t=>{
  for(const token of [undefined,null,'']){
    const app=await setup(t,undefined,undefined,{config:{adminToken:token,codexBinary:'unused',reserveTokens:100}});
    assert.equal((await app.call('/api/admin/state','GET',undefined,String(token))).status,401);
  }
});

test('CSV neutralizes formulas after leading whitespace and control characters',async t=>{
  const app=await setup(t);
  for(const name of ['=1+1','\t=1+1','\r\n+1+1','\u0000@SUM(1)',' ordinary']){
    const m=app.store.createMember(name,null,null);await app.chat(m.secret);
  }
  const r=await fetch(app.base+'/api/admin/export',{headers:{Authorization:'Bearer test-admin'}});
  const csv=await r.text();
  for(const name of ['=1+1','\t=1+1','\r\n+1+1','\u0000@SUM(1)'])assert.ok(csv.includes('"\''+name+'"'));
  assert.ok(csv.includes('" ordinary"'));
});

test('same employee is limited during quota lookup, including sibling Keys',async t=>{
  let release,started;const gate=new Promise(r=>release=r),entered=new Promise(r=>started=r);
  const app=await setup(t,undefined,async()=>{started();await gate;return rates(0);});
  const m=app.member(null),other=app.store.createKey(m.memberId);const first=app.chat(m.secret);
  await entered;
  try{
    const r=await fetch(app.base+'/v1/chat/completions',{method:'POST',headers:{Authorization:'Bearer '+other.secret,'Content-Type':'application/json'},body:JSON.stringify({messages:[{role:'user',content:'duplicate'}]}),signal:AbortSignal.timeout(1000)});
    assert.equal(r.status,429);assert.equal((await r.json()).error.type,'member_busy');
  }finally{release();await first;}
  assert.equal((await app.chat(m.secret)).status,200);
});

test('exhausted quota rejects before any upstream quota lookup and bad input releases admission',async t=>{
  let reads=0;const app=await setup(t,undefined,async()=>{reads++;return rates(0);});
  const m=app.member(0);assert.equal((await app.chat(m.secret)).status,429);assert.equal(reads,0);
  const k=app.member(null);app.store.run('UPDATE keys SET quota=0 WHERE id=?',k.keyId);
  assert.equal((await app.chat(k.secret)).status,429);assert.equal(reads,0);
  app.store.run('UPDATE keys SET quota=NULL WHERE id=?',k.keyId);
  assert.equal((await app.chat(k.secret,{messages:[]})).status,400);
  assert.equal((await app.chat(k.secret)).status,200);
});

test('preparation admission is globally bounded while quota lookup stalls',{timeout:15000},async t=>{
  let release;const gate=new Promise(r=>release=r);
  const app=await setup(t,undefined,async()=>{await gate;return rates(0);});
  let admissions=0;const allowance=app.store.allowance.bind(app.store);
  app.store.allowance=(...args)=>{admissions++;return allowance(...args);};
  const members=Array.from({length:17},()=>app.member(null)),calls=members.slice(0,16).map(m=>app.chat(m.secret));
  try{
    const deadline=Date.now()+5000;
    while(admissions<16){assert.ok(Date.now()<deadline,'requests reach preparation');await new Promise(r=>setTimeout(r,5));}
    const blocked=await app.chat(members[16].secret);assert.equal(blocked.status,429);assert.equal(admissions,16);
  }finally{release();await Promise.all(calls);}
  assert.equal((await app.chat(members[16].secret)).status,200);
  assert.equal(app.store.get("SELECT COUNT(*) n FROM requests WHERE status IN ('queued','running')").n,0);
});
test('per-Key IP statistics exclude sibling Keys and reject cross-member lookup',async t=>{
  const app=await setup(t);const m=app.member(null),k=app.store.createKey(m.memberId,null,null),other=app.member(null);
  await app.chat(m.secret);await app.chat(m.secret);await app.chat(k.secret);
  const r=await app.call('/api/admin/ips?member='+m.memberId+'&key='+k.keyId);assert.equal(r.body[0].requests,1);
  assert.equal((await app.call('/api/admin/ips?member='+other.memberId+'&key='+k.keyId)).status,404);
  assert.equal(app.store.snapshot().keys.find(x=>x.id===k.keyId).ips,1);
  assert.equal((await app.call('/api/me','GET',undefined,k.secret)).body.id,m.memberId);
});
test('capacity calibration accumulates tokens across unchanged rounded percentages',async t=>{
  let used=0;const app=await setup(t,async()=>{used+=1000;return {text:'ok',threadId:'thread',input:900,output:100,cached:0};},async()=>rates(Math.floor(used/2000)));
  const m=app.member(null);await app.chat(m.secret);await app.call('/api/admin/quotas/refresh','POST',{});await app.chat(m.secret);await app.call('/api/admin/quotas/refresh','POST',{});
  assert.equal(app.store.get('SELECT tokens_per_percent FROM accounts WHERE id=?','one').tokens_per_percent,2000);
});
test('relogin invalidates old sessions and clears identity-specific quota snapshots',async t=>{
  const app=await setup(t);const m=app.member(null);const first=await app.chat(m.secret);await app.call('/api/admin/quotas/refresh','POST',{});
  assert.equal((await app.call('/api/admin/accounts/one/login','POST',{})).status,200);
  const account=app.store.get('SELECT * FROM accounts WHERE id=?','one');assert.equal(account.quota_json,null);assert.equal(account.quota_calibration,null);
  assert.equal(app.store.get('SELECT state FROM sessions WHERE id=?',first.body.session_id).state,'uncertain');
  assert.equal((await app.chat(m.secret,{session_id:first.body.session_id})).status,409);
});
test('late login check cannot restore ready during another authentication attempt',async t=>{
  let releaseOld,releaseLogin;const app=await setup(t,undefined,undefined,{checker:async()=>new Promise(r=>{if(!releaseOld)releaseOld=r;else releaseLogin=r;})});
  const oldCheck=app.call('/api/admin/accounts/one/check','POST',{});while(!releaseOld)await new Promise(r=>setTimeout(r,5));
  await app.call('/api/admin/accounts/one/login','POST',{});while(!releaseLogin)await new Promise(r=>setTimeout(r,5));
  releaseOld(true);assert.equal((await oldCheck).status,409);assert.equal(app.store.get('SELECT state FROM accounts WHERE id=?','one').state,'login');
  assert.equal((await app.call('/api/admin/accounts/one/check','POST',{})).status,409);
  assert.equal((await app.chat(app.member(null).secret)).status,503);
  releaseLogin(true);await new Promise(r=>setTimeout(r,10));assert.equal(app.store.get('SELECT state FROM accounts WHERE id=?','one').state,'ready');
});
test('quota response started before relogin cannot overwrite the cleared snapshot',async t=>{
  let release,checking=false;const app=await setup(t,undefined,async()=>{checking=true;await new Promise(r=>release=r);return rates(0);});
  const refresh=app.call('/api/admin/accounts/one/quota','POST',{});while(!checking)await new Promise(r=>setTimeout(r,5));
  await app.call('/api/admin/accounts/one/login','POST',{});release();await refresh;
  assert.equal(app.store.get('SELECT quota_json FROM accounts WHERE id=?','one').quota_json,null);
});
test('usage counts actual tokens without double-counting cached input',async t=>{const {member,chat,store}=await setup(t);const m=member();const r=await chat(m.secret);assert.equal(r.status,200);assert.equal(r.body.usage.total_tokens,70);assert.equal(r.body.usage.prompt_tokens_details.cached_tokens,20);assert.equal(store.get('SELECT used FROM members WHERE id=?',m.memberId).used,70);assert.equal(store.get('SELECT reserved FROM requests').reserved,0);});
test('expiry, disabled keys, unknown keys rejected',async t=>{const {member,chat,store}=await setup(t);const expired=member(1000,Date.now()-1000);assert.equal((await chat(expired.secret)).status,403);const m=member();store.run('UPDATE keys SET enabled=0 WHERE id=?',m.keyId);assert.equal((await chat(m.secret)).status,403);assert.equal((await chat('unknown')).status,401);assert.equal(store.get('SELECT COUNT(*) n FROM requests').n,0);});
test('quota exhausted blocks future requests; final request settles actual usage',async t=>{const {member,chat,store}=await setup(t);const m=member(50);assert.equal((await chat(m.secret)).status,200);assert.equal((await chat(m.secret)).status,429);assert.equal(store.get('SELECT used FROM members').used,70);});
test('new key retains employee usage; revoked old key stays invalid',async t=>{const {member,chat,store,call}=await setup(t);const m=member(100);await chat(m.secret);const newer=await call('/api/admin/keys','POST',{memberId:m.memberId,expires:null});await call('/api/admin/keys/'+m.keyId,'PATCH',{enabled:false});assert.equal((await chat(m.secret)).status,403);await chat(newer.body.secret);assert.equal(store.get('SELECT used FROM members').used,140);assert.equal((await chat(newer.body.secret)).status,429);});
test('unlimited quota works; IP forwarding header is ignored',async t=>{const {member,call,store}=await setup(t);const m=member(null);const r=await call('/v1/chat/completions','POST',{messages:[{role:'user',content:'hello'}]},m.secret,{'X-Forwarded-For':'1.2.3.4'});assert.equal(r.status,200);assert.equal(store.get('SELECT ip FROM requests').ip,'127.0.0.1');});
test('same employee cannot spend concurrently',async t=>{let release;const {member,chat,store}=await setup(t,()=>new Promise(r=>{release=()=>r({text:'ok',input:20,output:2,cached:0});}));const m=member();const first=chat(m.secret);while(!release)await new Promise(r=>setTimeout(r,5));assert.equal((await chat(m.secret)).status,429);assert.equal(store.get('SELECT reserved FROM requests').reserved,100);release();assert.equal((await first).status,200);});
test('account serializes separate employees and chooses another idle account',async t=>{let active=0,max=0;const {member,chat,store}=await setup(t,async()=>{active++;max=Math.max(max,active);await new Promise(r=>setTimeout(r,40));active--;return {text:'ok',input:1,output:1,cached:0};});const a=member(),b=member();await Promise.all([chat(a.secret),chat(b.secret)]);assert.equal(max,1);store.run("INSERT INTO accounts(id,name,home,state) VALUES('two','Second','unused','ready')");await Promise.all([chat(a.secret),chat(b.secret)]);assert.equal(max,2);});
test('unknown upstream usage stays reserved until explicit reconciliation',async t=>{const {member,chat,store,call}=await setup(t,async()=>{throw Object.assign(Error('timeout'),{usageKnown:false});});const m=member(80);assert.equal((await chat(m.secret)).status,502);assert.equal((await chat(m.secret)).status,429);const row=store.get('SELECT * FROM requests');assert.equal(row.reserved,80);const settle=await call('/api/admin/requests/'+row.id+'/settle','POST',{tokens:30});assert.equal(settle.status,200);assert.equal(store.get('SELECT used FROM members').used,30);assert.equal(store.get('SELECT reserved FROM requests').reserved,0);assert.equal((await call('/api/admin/requests/'+row.id+'/settle','POST',{tokens:30})).status,409);});
test('known failures release reservation',async t=>{const {member,chat,store}=await setup(t,async()=>{throw Object.assign(Error('spawn failed'),{usageKnown:true});});const m=member();await chat(m.secret);assert.equal(store.get('SELECT reserved FROM requests').reserved,0);assert.equal(store.get('SELECT status FROM requests').status,'failed');});
test('restarting running requests preserves unknown reservation and marks interruption',()=>{const dir=temp(),db=join(dir,'s.sqlite');let s=new Store(db);const m=s.createMember('A',100,null);const request=s.reserve(s.authenticate(m.secret),'127.0.0.1','test',100,'codex');s.run("UPDATE requests SET status='running' WHERE id=?",request);s.close();s=new Store(db);assert.equal(s.get('SELECT status FROM requests').status,'interrupted');assert.equal(s.get('SELECT reserved FROM requests').reserved,100);assert.throws(()=>s.reserve(s.authenticate(m.secret),'ip','test',100,'codex'),/额度/);s.close();cleanup(dir);});
test('invalid protocol rejected rather than pretending support',async t=>{const {member,chat}=await setup(t);const m=member();for(const extra of [{tools:[]},{max_tokens:10},{model:'unknown-model'},{messages:[{role:'user',content:[]}]},{stream:'yes'}])assert.equal((await chat(m.secret,extra)).status,400);});
test('public responses never expose credential digests or home paths',async t=>{const {call,member}=await setup(t);const m=member();const r=await call('/api/admin/state');const text=JSON.stringify(r.body);assert.ok(!text.includes(m.secret));assert.ok(!text.includes('digest'));assert.ok(!text.includes('"home"'));});
test('invalid administrative updates are rejected',async t=>{const {call,member}=await setup(t);const m=member();assert.equal((await call('/api/admin/members/'+m.memberId,'PATCH',{quota:-1})).status,400);assert.equal((await call('/api/admin/keys/'+m.keyId,'PATCH',{expires:'bad'})).status,400);assert.equal((await call('/api/admin/members','POST',{name:'',quota:null,expires:null})).status,400);});
test('SSE-compatible final response and authenticated model list',async t=>{const {base,member,call}=await setup(t);const m=member();const r=await fetch(base+'/v1/chat/completions',{method:'POST',headers:{Authorization:'Bearer '+m.secret,'Content-Type':'application/json'},body:JSON.stringify({messages:[{role:'user',content:'Hi'}],stream:true})});assert.equal(r.status,200);assert.match(r.headers.get('content-type'),/event-stream/);assert.match(await r.text(),/data: \[DONE\]/);assert.equal((await call('/v1/models','GET',undefined,m.secret)).body.data[0].id,'codex');});
test('database backup and audit entries',async t=>{const {call,store}=await setup(t);const r=await call('/api/admin/backup','POST',{});assert.equal(r.status,200);assert.match(r.body.file,/^data\/backups\/gateway-/);assert.equal(store.get("SELECT COUNT(*) n FROM audit WHERE action='backup.create'").n,1);});
const rates=used=>({rateLimits:{primary:{usedPercent:used,windowDurationMins:300,resetsAt:Math.floor(Date.now()/1000)+18000}}});
test('new sessions select largest remaining quota rather than first or least-used account',async t=>{const {member,chat,store}=await setup(t,undefined,async(_,home)=>rates(home==='second'?10:80));store.run("INSERT INTO accounts(id,name,home,state,last_used) VALUES('two','Full account','second','ready',9999999999999)");const r=await chat(member().secret);assert.equal(r.status,200);assert.equal(store.get('SELECT account_id FROM sessions WHERE id=?',r.body.session_id).account_id,'two');});
test('session pins account and resumes exact upstream thread despite a fuller new account',async t=>{const seen=[];const {member,chat,store}=await setup(t,async args=>{seen.push({home:args.home,sessionId:args.sessionId,messages:args.messages});return {text:'reply',input:10,output:2,cached:0,threadId:args.sessionId||'upstream-pinned'};});const m=member();const first=await chat(m.secret);store.run("INSERT INTO accounts(id,name,home,state) VALUES('two','Other','second','ready')");const second=await chat(m.secret,{session_id:first.body.session_id});assert.equal(second.status,200);assert.equal(second.body.session_id,first.body.session_id);assert.equal(seen[1].home,'unused');assert.equal(seen[1].sessionId,'upstream-pinned');assert.equal(store.get('SELECT COUNT(*) n FROM sessions').n,1);});
test('exhausted bound account pauses existing session while new session can use another account',async t=>{const {member,chat,store}=await setup(t);const m=member();const first=await chat(m.secret);store.run('UPDATE accounts SET quota_json=? WHERE id=?',JSON.stringify(normalizeLimits(rates(100))),'one');store.run("INSERT INTO accounts(id,name,home,state) VALUES('two','Other','second','ready')");const paused=await chat(m.secret,{session_id:first.body.session_id});assert.equal(paused.status,429);assert.equal(paused.body.error.type,'session_quota_exhausted');const next=await chat(m.secret);assert.equal(next.status,200);assert.equal(store.get('SELECT account_id FROM sessions WHERE id=?',next.body.session_id).account_id,'two');});
test('session ownership follows employee and cannot cross to another employee',async t=>{const {member,chat}=await setup(t);const a=member(),b=member();const first=await chat(a.secret);assert.equal((await chat(b.secret,{session_id:first.body.session_id})).status,404);});
test('full client history is verified and only new message reaches resumed thread',async t=>{const seen=[];const {member,chat}=await setup(t,async args=>{seen.push(args.messages);return {text:'reply',input:10,output:2,cached:0,threadId:'history-thread'};});const m=member();const first=await chat(m.secret);const second=await chat(m.secret,{session_id:first.body.session_id,messages:[{role:'user',content:'hello'},{role:'assistant',content:'reply'},{role:'user',content:'next'}]});assert.equal(second.status,200);assert.deepEqual(seen[1],[{role:'user',content:'next'}]);const mismatch=await chat(m.secret,{session_id:first.body.session_id,messages:[{role:'user',content:'tampered'},{role:'user',content:'next'}]});assert.equal(mismatch.status,409);});
test('unknown quota is not treated as full; explicit local budget enables fallback',async t=>{const {member,chat,store}=await setup(t,undefined,async()=>{throw Error('unavailable');});const m=member();assert.equal((await chat(m.secret)).status,503);store.run('UPDATE accounts SET token_budget=100000 WHERE id=?','one');assert.equal((await chat(m.secret)).status,200);});
test('quota estimate includes pending token costs and tightest official window',()=>{const q=normalizeLimits({rateLimits:{primary:{usedPercent:20,resetsAt:9999999999},secondary:{usedPercent:80,resetsAt:9999999999}}});assert.equal(q.remaining,20);const a={quota_json:JSON.stringify(q),token_budget:null,budget_used:0,since_check_tokens:1000,tokens_per_percent:1000};assert.equal(accountCapacity(a,2000,3000).score,14000);assert.equal(accountCapacity(a,20000).eligible,false);q.windows[0].reset=Date.now()-1;assert.equal(accountCapacity({...a,quota_json:JSON.stringify(q)},1).source,'expired');});
test('different account capacities rank estimated tokens rather than percentages',async t=>{const app=await setup(t,undefined,async(_,home)=>rates(home==='large'?60:10));app.store.run('UPDATE accounts SET tokens_per_percent=1000 WHERE id=?','one');app.store.run("INSERT INTO accounts(id,name,home,state,tokens_per_percent) VALUES('large','Large plan','large','ready',10000)");const m=app.member(null);const r=await app.chat(m.secret);assert.equal(r.status,200);assert.equal(app.store.get('SELECT account_id FROM sessions WHERE id=?',r.body.session_id).account_id,'large');const manual={quota_json:null,token_budget:100000,budget_used:10000};assert.equal(accountCapacity(manual,10000).score,80000);assert.ok(accountCapacity({...manual,token_budget:1000000,budget_used:500000},10000).score>accountCapacity(manual,10000).score);});
test('unknown session outcome refuses silent continuation',async t=>{const {member,chat,store}=await setup(t,async()=>{throw Object.assign(Error('lost response'),{usageKnown:false,threadId:'uncertain-thread'});});const m=member(null);await chat(m.secret);const session=store.get('SELECT * FROM sessions');assert.equal(session.state,'uncertain');const r=await chat(m.secret,{session_id:session.id});assert.equal(r.status,409);assert.equal(r.body.error.type,'session_uncertain');});
test('resumed Codex cumulative usage is converted to per-request delta (real trace fixture)',()=>{const counts=usageDelta({input_tokens:18580,output_tokens:16,cached_input_tokens:16512},{input:9264,output:8,cached:7424});assert.equal(counts.input,9316);assert.equal(counts.output,8);assert.equal(counts.cached,9088);assert.equal(counts.input+counts.output,9324);assert.throws(()=>usageDelta({input_tokens:1,output_tokens:1},{input:100,output:1}),/核对/);});
test('Claude cumulative model usage counts cache and settles only the new turn',()=>{const value={type:'result',subtype:'success',is_error:false,result:'hello',session_id:'claude-session',modelUsage:{sonnet:{inputTokens:100,outputTokens:20,cacheReadInputTokens:300,cacheCreationInputTokens:50}}};const first=parseClaudeResult(value);assert.equal(first.input,450);assert.equal(first.output,20);const second=parseClaudeResult({...value,modelUsage:{sonnet:{inputTokens:150,outputTokens:30,cacheReadInputTokens:400,cacheCreationInputTokens:50}}},first.cumulativeUsage);assert.equal(second.input,150);assert.equal(second.output,10);assert.equal(second.cached,100);assert.throws(()=>parseClaudeResult({...value,modelUsage:{}}),/缺少会话用量/);});
test('Claude invocation disables tools without bare mode that drops subscription auth',()=>{const args=claudeArgs('fixed-session');assert.ok(args.includes('--safe-mode'));assert.ok(args.includes('--restricted'));assert.equal(args[args.indexOf('--tools')+1],'');assert.equal(args[args.indexOf('--disallowedTools')+1],'*');assert.equal(args[args.indexOf('--resume')+1],'fixed-session');assert.ok(!args.includes('--bare'));const env=claudeEnv('isolated-home');assert.equal(env.CLAUDE_CONFIG_DIR,'isolated-home');assert.equal(env.ANTHROPIC_API_KEY,undefined);assert.equal(env.CODEX_HOME,undefined);});
test('Claude sessions use only Claude accounts and remain sticky',async t=>{const seen=[];const app=await setup(t,undefined,undefined,{claudeExecutor:async a=>{seen.push(a);return {text:'Claude response',threadId:a.sessionId||'claude-thread',input:100,output:10,cached:0};}});app.store.run("INSERT INTO accounts(id,name,kind,home,state,token_budget) VALUES('claude-one','Claude','claude','claude-home','ready',100000)");const m=app.member(null);const first=await app.chat(m.secret,{model:'claude'});assert.equal(first.status,200);assert.equal(first.body.model,'claude');const second=await app.chat(m.secret,{model:'claude',session_id:first.body.session_id});assert.equal(second.status,200);assert.equal(seen[1].home,'claude-home');assert.equal(seen[1].sessionId,'claude-thread');assert.equal((await app.chat(m.secret,{model:'codex',session_id:first.body.session_id})).status,409);assert.equal(app.store.get('SELECT budget_used FROM accounts WHERE id=?','one').budget_used,0);});
test('independent Key quota limits one Key while sibling remains usable',async t=>{const {member,chat,call,store}=await setup(t);const m=member(null);await call('/api/admin/keys/'+m.keyId,'PATCH',{quota:70});assert.equal((await chat(m.secret)).status,200);assert.equal((await chat(m.secret)).body.error.type,'key_quota_exceeded');const k=await call('/api/admin/keys','POST',{memberId:m.memberId,quota:null,expires:null});assert.equal((await chat(k.body.secret)).status,200);assert.equal(store.get('SELECT used FROM keys WHERE id=?',m.keyId).used,70);assert.equal(store.get('SELECT used FROM members WHERE id=?',m.memberId).used,140);await call('/api/admin/keys/'+m.keyId,'PATCH',{quota:140});assert.equal((await chat(m.secret)).status,200);});
test('queued request is rejected if its Key quota is reduced before execution',async t=>{let release;let calls=0;const app=await setup(t,async()=>{calls++;if(calls===1)await new Promise(r=>release=r);return {text:'ok',input:10,output:1,cached:0,threadId:'thread'};});const a=app.member(),b=app.member();const first=app.chat(a.secret);while(!release)await new Promise(r=>setTimeout(r,5));const queued=app.chat(b.secret);while(!app.store.get("SELECT id FROM requests WHERE member_id=? AND status='queued'",b.memberId))await new Promise(r=>setTimeout(r,5));await app.call('/api/admin/keys/'+b.keyId,'PATCH',{quota:0});release();assert.equal((await first).status,200);assert.equal((await queued).status,429);assert.equal(calls,1);assert.equal(app.store.get('SELECT used FROM keys WHERE id=?',b.keyId).used,0);});
test('disconnect during quota lookup does not spend subscription usage',async t=>{let release,started=false,calls=0;const app=await setup(t,async()=>{calls++;return {text:'ok',input:1,output:1,cached:0};},async()=>{started=true;await new Promise(r=>release=r);return rates(0);});const m=app.member();const controller=new AbortController();const response=fetch(app.base+'/v1/chat/completions',{method:'POST',headers:{Authorization:'Bearer '+m.secret,'Content-Type':'application/json'},body:JSON.stringify({messages:[{role:'user',content:'cancel'}]}),signal:controller.signal}).catch(()=>null);while(!started)await new Promise(r=>setTimeout(r,5));controller.abort();await response;await new Promise(r=>setTimeout(r,20));release();await new Promise(r=>setTimeout(r,30));assert.equal(calls,0);assert.equal(app.store.get('SELECT COUNT(*) n FROM requests').n,0);});
test('three-account five-person workload preserves session and exact accounting',async t=>{const active=new Map(),usage=new Map(),threadAccounts=new Map();let max=0,total=0;const app=await setup(t,async args=>{const n=(active.get(args.home)||0)+1;active.set(args.home,n);assert.equal(n,1);max=Math.max(max,[...active.values()].reduce((a,b)=>a+b,0));await new Promise(r=>setTimeout(r,20));const thread=args.sessionId||('thread-'+threadAccounts.size);if(threadAccounts.has(thread))assert.equal(threadAccounts.get(thread),args.home);else threadAccounts.set(thread,args.home);usage.set(args.home,(usage.get(args.home)||0)+10);active.set(args.home,0);total+=10000;return {threadId:thread,text:'ok',input:9000,output:1000,cached:0};},async(_,home)=>rates(usage.get(home)||0));app.store.run("INSERT INTO accounts(id,name,home,state) VALUES('two','Second','second','ready'),('three','Third','third','ready')");const people=Array.from({length:5},()=>app.member(null));const first=await Promise.all(people.map(p=>app.chat(p.secret)));assert.ok(first.every(r=>r.status===200),JSON.stringify(first));const second=await Promise.all(people.map((p,i)=>app.chat(p.secret,{session_id:first[i].body.session_id})));assert.ok(second.every(r=>r.status===200));assert.equal(app.store.get('SELECT SUM(used) n FROM members').n,100000);assert.equal(app.store.get('SELECT SUM(used) n FROM keys').n,100000);assert.equal(app.store.get('SELECT SUM(budget_used) n FROM accounts').n,100000);assert.equal(app.store.get('SELECT COUNT(*) n FROM sessions').n,5);assert.equal(total,100000);assert.ok(max<=3);assert.equal(app.store.get('SELECT SUM(reserved) n FROM requests').n,0);});

test('restarting queued requests releases quota and preserves an existing active session',()=>{
 const dir=temp(),db=join(dir,'s.sqlite');let s=new Store(db);
 try{const m=s.createMember('A',100,null);s.run("INSERT INTO accounts(id,name,home,state) VALUES('one','Test','unused','ready')");s.run("INSERT INTO sessions(id,member_id,account_id,upstream_id,state,created,updated) VALUES('session',?,'one','upstream','active',1,1)",m.memberId);
 const request=s.reserve(s.authenticate(m.secret),'127.0.0.1','test',100,'codex');s.run("UPDATE requests SET session_id='session',account_id='one' WHERE id=?",request);s.close();s=new Store(db);
 assert.equal(s.get('SELECT status FROM requests').status,'cancelled');assert.equal(s.get('SELECT reserved FROM requests').reserved,0);assert.equal(s.get('SELECT state FROM sessions').state,'active');
 assert.doesNotThrow(()=>s.reserve(s.authenticate(m.secret),'ip','test',100,'codex'));
 }finally{s.close();cleanup(dir);}
});
