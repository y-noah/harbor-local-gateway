import test from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync,readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {createGateway} from '../server.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createBalancer,nextWeightReset} from '../balancing.mjs';

async function setup(t){
 const root=mkdtempSync(join(tmpdir(),'harbor-api-accounts-')),calls=[];let app,mode='ok',quotaReads=0,releaseModels;
 const transport=createResponsesTransport({apiKeyMode:true,credentials:async(_b,id)=>({token:app.store.upstreamKey(app.store.get('SELECT * FROM accounts WHERE id=?',id))}),fetchImpl:async(url,options)=>{
  calls.push({url,options});assert.ok(url.startsWith('https://api.openai.com/v1/'));assert.equal(options.headers['ChatGPT-Account-ID'],undefined);
  if(url.endsWith('/models')){if(mode==='hold-models')await new Promise((resolve,reject)=>{releaseModels=resolve;options.signal?.addEventListener('abort',()=>reject(options.signal.reason),{once:true});});return Response.json({data:[{id:'gpt-6-luna',object:'model'}]});}
  if(mode==='quota')return Response.json({error:{code:'insufficient_quota',message:'synthetic-upstream-private-key'}},{status:429});
  if(mode==='auth')return Response.json({error:{code:'invalid_api_key',message:'synthetic-upstream-private-key'}},{status:401});
  const response={id:'resp_api_fixture',object:'response',status:'completed',model:'gpt-6-luna',output:[{id:'msg_api_fixture',type:'message',role:'assistant',content:[{type:'output_text',text:'API fixture'}]}],usage:{input_tokens:10,output_tokens:2,input_tokens_details:{cached_tokens:3}}};
  return new Response([{type:'response.created',response:{id:response.id}},{type:'response.completed',response}].map(x=>'data: '+JSON.stringify(x)+'\n\n').join(''),{headers:{'Content-Type':'text/event-stream'}});
 }});
 app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-admin',reserveTokens:100,codexBinary:'unused'},apiResponsesTransport:transport,responsesTransport:{run:async()=>{throw Error('subscription transport must not run');},models:async()=>[{slug:'subscription-model'}]},rateReader:async()=>{quotaReads++;return null;}});
 await new Promise(r=>app.server.listen(0,'127.0.0.1',r));const base='http://127.0.0.1:'+app.server.address().port;
 const member=app.store.createMember('API verification fixture',null,null);
 t.after(async()=>{await app.close();rmSync(root,{recursive:true,force:true});});
 const call=async(path,method='GET',body,secret='fixture-admin',headers={})=>{const r=await fetch(base+path,{method,headers:{Authorization:'Bearer '+secret,...(body?{'Content-Type':'application/json'}:{}),...headers},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,body:await r.json()};};
 const add=async(weight=0)=>{const r=await call('/api/admin/accounts','POST',{name:'Official API fixture',auth_mode:'openai_api',api_key:'synthetic-upstream-private-key',api_weight:weight});assert.equal(r.status,201);return r.body.id;};
 const send=(sid,thread='11111111-1111-4111-8111-111111111111',person=member.secret)=>call('/v1/responses','POST',{model:'gpt-6-luna',input:'synthetic test',stream:false},person,{'thread-id':thread,'x-harbor-session':sid,'user-agent':'codex_cli_rs/fixture',originator:'codex_cli_rs'});
 return {...app,root,base,member,call,add,send,calls,mode:x=>mode=x,modelsHeld:()=>!!releaseModels,releaseModels:()=>releaseModels?.(),quotaReads:()=>quotaReads};
}

test('official Key is administrator-only, encrypted with account identity, never returned or used for subscription login',async t=>{
 const a=await setup(t);assert.equal((await a.call('/api/admin/accounts','POST',{name:'bad',auth_mode:'openai_api',api_key:'x'},a.member.secret)).status,401);
 const id=await a.add(),row=a.store.get('SELECT * FROM accounts WHERE id=?',id);assert.equal(row.auth_mode,'openai_api');assert.equal(row.api_weight,0);assert.equal(row.state,'offline');assert.notEqual(row.api_secret_cipher,'synthetic-upstream-private-key');assert.equal(a.store.upstreamKey(row),'synthetic-upstream-private-key');assert.equal(a.store.upstreamKey({...row,id:'another-account'}),null);
 assert.ok(!JSON.stringify(a.store.snapshot()).includes('synthetic-upstream-private-key'));assert.ok(!readFileSync(join(a.root,'data','gateway.sqlite')).includes(Buffer.from('synthetic-upstream-private-key')));
 assert.equal((await a.call('/api/admin/accounts/'+id+'/login','POST',{})).status,400);assert.equal((await a.call('/api/admin/accounts/'+id+'/check','POST',{})).status,200);await a.call('/api/admin/accounts/'+id+'/quota','POST',{});assert.equal(a.quotaReads(),0);
 assert.equal((await a.call('/api/admin/accounts/'+id,'PATCH',{api_key:'contains\ncontrol'})).status,400);
 const s=await a.call('/api/admin/state');assert.ok(!JSON.stringify(s.body).includes(row.api_secret_cipher));assert.ok(s.body.accounts[0].api_key_configured);
});
test('zero-weight official Key serves an explicit fixed session, binds one employee/client and records actual API usage',async t=>{
 const a=await setup(t),id=await a.add();await a.call('/api/admin/accounts/'+id+'/check','POST',{});
 assert.throws(()=>a.balancer.selectAccount(),{code:'no_account'});
 const fixed=await a.call('/api/admin/sessions','POST',{member_id:a.member.memberId,account_id:id});assert.equal(fixed.status,201);const sid=fixed.body.session_id;
 assert.equal((await a.send(sid)).status,200);const inference=a.calls.find(x=>x.url.endsWith('/responses'));assert.equal(inference.options.headers.Authorization,'Bearer synthetic-upstream-private-key');assert.equal(inference.options.headers.originator,'codex_cli_rs');assert.equal(inference.options.headers['User-Agent'],'codex_cli_rs/fixture');
 const r=a.store.get('SELECT * FROM requests');assert.equal(r.account_id,id);assert.equal(r.input,10);assert.equal(r.output,2);assert.equal(r.cached,3);assert.equal(r.usage_unknown,0);assert.equal(r.route_version,0);assert.equal(a.store.get('SELECT * FROM sessions WHERE id=?',sid).client_session_id,'11111111-1111-4111-8111-111111111111');
 const another=a.store.createMember('other',null,null);assert.equal((await a.send(sid,'11111111-1111-4111-8111-111111111111',another.secret)).status,404);assert.equal((await a.send(sid,'22222222-2222-4222-8222-222222222222')).status,409);assert.equal(a.calls.filter(x=>x.url.endsWith('/responses')).length,1);
 assert.equal((await a.send(sid)).status,200);assert.equal(a.store.get('SELECT account_id FROM sessions WHERE id=?',sid).account_id,id);
 assert.equal((await a.call('/api/admin/sessions/'+sid,'PATCH',{nightly_enabled:true})).status,400);
});
test('persistent manual API percentages combine with the subscription group without poisoning its quota calculation',async t=>{
 const a=await setup(t),id=await a.add(20);await a.call('/api/admin/accounts/'+id+'/check','POST',{});const time=Date.now();
 for(const [aid,remaining] of [['sub-a',60],['sub-b',40]])a.store.run("INSERT INTO accounts(id,name,home,state,quota_json) VALUES(?,?,?,'ready',?)",aid,aid,'unused',JSON.stringify({checked:time,plan:'same',remaining,windows:[{used:100-remaining,minutes:10080,reset:time+86400000}]}));
 a.balancer.saveSettings({weighted:true});const state=a.balancer.distribution();assert.equal(state.mode,'api_mixed');assert.equal(state.weights.get(id),20);assert.equal(Math.round(state.weights.get('sub-a')),48);assert.equal(Math.round(state.weights.get('sub-b')),32);assert.equal(state.reason,null);
 const counts=new Map();for(let i=0;i<100;i++){const selected=a.balancer.selectAccount().id;counts.set(selected,(counts.get(selected)||0)+1);}assert.equal(counts.get(id),20);assert.equal(counts.get('sub-a'),48);assert.equal(counts.get('sub-b'),32);
 const other=await a.add(80);assert.equal((await a.call('/api/admin/accounts/'+other,'PATCH',{api_weight:81})).status,400);assert.equal(a.store.get('SELECT api_weight FROM accounts WHERE id=?',id).api_weight,20);
 const b=createBalancer({store:a.store,preparing:new Set(),clock:()=>nextWeightReset(time)+1,refreshQuota:async()=>null});await b.tick();assert.equal(a.store.get('SELECT api_weight FROM accounts WHERE id=?',id).api_weight,20);assert.equal(a.store.get('SELECT enabled FROM accounts WHERE id=?',id).enabled,1);
});
test('official API failures are redacted, not replayed, and leave configured weight under human control',async t=>{
 const a=await setup(t),id=await a.add(10);await a.call('/api/admin/accounts/'+id+'/check','POST',{});const f=await a.call('/api/admin/sessions','POST',{member_id:a.member.memberId,account_id:id});a.mode('quota');
 const r=await a.send(f.body.session_id);assert.equal(r.status,429);assert.ok(!JSON.stringify(r.body).includes('synthetic-upstream-private-key'));assert.equal(a.calls.filter(x=>x.url.endsWith('/responses')).length,1);assert.equal(a.store.get('SELECT api_weight FROM accounts WHERE id=?',id).api_weight,10);assert.equal(a.store.get('SELECT route_block FROM accounts WHERE id=?',id).route_block,'quota_exhausted');
});

test('disabling a Key migrates its fixed session and removal erases the upstream ciphertext',async t=>{
 const a=await setup(t),id=await a.add();await a.call('/api/admin/accounts/'+id+'/check','POST',{});const f=await a.call('/api/admin/sessions','POST',{member_id:a.member.memberId,account_id:id});
 a.store.run("INSERT INTO accounts(id,name,home,state) VALUES('sub','Subscription fixture','unused','ready')");
 const stop=await a.call('/api/admin/accounts/'+id,'PATCH',{enabled:false});assert.equal(stop.status,200);assert.equal(stop.body.moved,1);const session=a.store.get('SELECT * FROM sessions WHERE id=?',f.body.session_id);assert.equal(session.account_id,'sub');assert.ok(session.nightly_expires>Date.now());
 assert.equal((await a.call('/api/admin/accounts/'+id,'DELETE')).status,200);const removed=a.store.get('SELECT * FROM accounts WHERE id=?',id);assert.equal(removed.api_secret_cipher,null);assert.equal(removed.api_secret_digest,null);assert.equal(a.store.upstreamKey(removed),null);
});

test('Key update and an older asynchronous authentication check cannot race to enable an unverified credential',async t=>{
 const a=await setup(t),id=await a.add();await a.call('/api/admin/accounts/'+id+'/check','POST',{});
 a.mode('hold-models');const checking=a.call('/api/admin/accounts/'+id+'/check','POST',{});const limit=Date.now()+3000;while(!a.modelsHeld()){assert.ok(Date.now()<limit);await new Promise(r=>setTimeout(r,5));}
 const update=await a.call('/api/admin/accounts/'+id,'PATCH',{api_key:'synthetic-replacement-private-key'});a.releaseModels();assert.equal((await checking).status,409);a.mode('ok');assert.equal(update.status,200);const row=a.store.get('SELECT * FROM accounts WHERE id=?',id);assert.equal(row.state,'offline');assert.equal(a.store.upstreamKey(row),'synthetic-replacement-private-key');assert.equal(row.api_weight,0);
 const events=a.store.all("SELECT action,detail FROM audit WHERE action LIKE 'account.api.%'");assert.ok(events.some(e=>e.action==='account.api.key'));assert.ok(!JSON.stringify(events).includes('synthetic-replacement-private-key'));
 assert.equal((await a.call('/api/admin/sessions','POST',{member_id:a.member.memberId,account_id:id})).status,400);
});
