import {test} from 'node:test';
import assert from 'node:assert/strict';
import {mkdtempSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createGateway} from '../server.mjs';
import {Store} from '../store.mjs';
import {createResponsesTransport,validateResponses} from '../responses.mjs';
const model='fixture-model';
const output={type:'function_call',id:'fc_fixture',call_id:'call_fixture',name:'read_file',arguments:'{"path":"fixture.txt"}',status:'completed'};
const complete=(items=[output])=>({type:'response.completed',response:{id:'resp_fixture',object:'response',status:'completed',model,output:items,usage:{input_tokens:80,output_tokens:10,input_tokens_details:{cached_tokens:20}}}});
const frame=e=>'event: '+e.type+'\ndata: '+JSON.stringify(e)+'\n\n';
const eventStream=events=>new Response(events.map(frame).join(''),{headers:{'Content-Type':'text/event-stream'}});
function fixtureTransport(responder){
 const calls=[];const transport=createResponsesTransport({version:async()=> 'test-version',credentials:async()=>({token:'fixture-upstream-only',accountId:'fixture-account'}),fetchImpl:async(url,options)=>{
  calls.push({url,options});if(url.includes('/models?'))return Response.json({models:[{slug:model}]});return responder?responder(url,options):eventStream([{type:'response.created',response:{id:'resp_fixture'}},{type:'response.output_item.done',item:output},complete()]);
 }});return {transport,calls};
}
async function setup(t,responder){
 const root=mkdtempSync(join(tmpdir(),'harbor-responses-test-'));const {transport,calls}=fixtureTransport(responder);
 const app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'fixture-admin',codexBinary:'fixture-cli',reserveTokens:100},checker:async()=>true,rateReader:async()=>({rateLimits:{primary:{usedPercent:0,windowDurationMins:300,resetsAt:Math.floor(Date.now()/1000)+18000}}}),responsesTransport:transport});
 app.store.run("INSERT INTO accounts(id,name,home,state) VALUES('account-one','Fixture','fixture-home','ready')");
 await new Promise(r=>app.server.listen(0,'127.0.0.1',r));const base='http://127.0.0.1:'+app.server.address().port;
 t.after(async()=>{await app.close();rmSync(root,{recursive:true,force:true});});
 const member=()=>app.store.createMember('Fixture member',100000,null);
 const send=(person,overrides={},headers={},options={})=>fetch(base+'/v1/responses',{method:'POST',headers:{'Content-Type':'application/json',Authorization:'Bearer '+person.secret,...headers},body:JSON.stringify({model,input:[{role:'user',content:'Read fixture'}],stream:false,...overrides}),...options});
 return {...app,base,calls,member,send};
}
const until=async(fn)=>{const deadline=Date.now()+3000;while(!fn()){if(Date.now()>deadline)throw Error('condition timed out');await new Promise(r=>setTimeout(r,5));}};
test('Responses preserves function calls, replayed results, metadata and scoped account affinity',async t=>{
 const app=await setup(t),m=app.member(),headers={'thread-id':'client-thread','Authorization':'Bearer '+m.secret,'ChatGPT-Account-ID':'forged','x-openai-internal-codex-responses-lite':'true'};
 const tools=[{type:'function',name:'read_file',parameters:{type:'object',properties:{path:{type:'string'}}}}];
 const first=await app.send(m,{tools},headers);assert.equal(first.status,200);assert.deepEqual((await first.json()).output,[output]);
 const input=[{role:'user',content:'Read fixture'},output,{type:'function_call_output',call_id:output.call_id,output:'fixture-content'},{type:'reasoning',id:'rs_fixture',encrypted_content:'fixture-encrypted'}];
 assert.equal((await app.send(m,{tools,input},headers)).status,200);
 const sent=app.calls.filter(c=>c.url.endsWith('/responses'));assert.equal(sent.length,2);assert.deepEqual(JSON.parse(sent[1].options.body).input,input);assert.deepEqual(JSON.parse(sent[0].options.body).tools,tools);
 assert.equal(sent[0].options.headers.Authorization,'Bearer fixture-upstream-only');assert.equal(sent[0].options.headers['ChatGPT-Account-ID'],'fixture-account');assert.equal(sent[0].options.headers['x-openai-internal-codex-responses-lite'],'true');assert.equal(sent[0].options.headers['thread-id'],sent[1].options.headers['thread-id']);assert.notEqual(sent[0].options.headers['thread-id'],'client-thread');
 assert.equal(app.store.get('SELECT COUNT(*) n FROM sessions').n,1);assert.equal(app.store.get('SELECT used FROM members').used,180);assert.equal(app.store.get('SELECT used FROM keys').used,180);assert.equal(app.store.get('SELECT budget_used FROM accounts').budget_used,180);assert.equal(app.store.get('SELECT SUM(reserved) n FROM requests').n,0);assert.equal(app.store.get('SELECT model FROM requests').model,model);
 const other=app.member();await app.send(other,{}, {...headers,Authorization:'Bearer '+other.secret});assert.equal(app.store.get('SELECT COUNT(*) n FROM sessions').n,2);
});
test('SSE is incremental and accounting finishes before completed is exposed',async t=>{
 let release;const gate=new Promise(r=>release=r);
 const app=await setup(t,()=>new Response(new ReadableStream({async start(c){c.enqueue(Buffer.from(frame({type:'response.created',response:{id:'r'}})));await gate;c.enqueue(Buffer.from(frame(complete())));c.close();}}),{headers:{'Content-Type':'text/event-stream'}}));
 const m=app.member();const r=await app.send(m,{stream:true});assert.equal(r.status,200);const reader=r.body.getReader();const first=await reader.read();assert.match(new TextDecoder().decode(first.value),/response.created/);assert.equal(app.store.get('SELECT status FROM requests').status,'running');release();let tail='';while(true){const x=await reader.read();if(x.done)break;tail+=new TextDecoder().decode(x.value);}assert.match(tail,/response.completed/);assert.equal(app.store.get('SELECT status FROM requests').status,'ok');
});
test('protocol validation rejects unsupported transport modes; model names are routed to the upstream',async t=>{
 const app=await setup(t),m=app.member();
 for(const value of [{previous_response_id:'other-response'},{store:true},{background:true},{stream:'yes'},{model:''}])assert.equal((await app.send(m,value)).status,400);
 assert.equal(app.calls.length,0);assert.equal((await app.send(m,{model:'new-upstream-model'},{'thread-id':'known-failure'})).status,200);assert.equal(app.calls.filter(c=>c.url.endsWith('/responses')).length,1);assert.equal(app.calls.filter(c=>c.url.includes('/models')).length,0);assert.equal(JSON.parse(app.calls[0].options.body).model,'new-upstream-model');assert.equal(app.store.get('SELECT reserved FROM requests').reserved,0);
 assert.equal((await app.send(m,{}, {'thread-id':'known-failure'})).status,200);
});
test('Models require employee credentials and include Codex catalog metadata',async t=>{
 const app=await setup(t),m=app.member();assert.equal((await fetch(app.base+'/v1/models')).status,401);
 const r=await fetch(app.base+'/v1/models',{headers:{Authorization:'Bearer '+m.secret}});const b=await r.json();assert.ok(b.data.some(x=>x.id===model));assert.deepEqual(b.models,[{slug:model}]);
});
test('extended client fields and identifiers are forwarded without arbitrary length or shape gates',async t=>{
 const app=await setup(t),m=app.member(),thread='client-thread-'.repeat(40),beta='feature-'.repeat(40);
 const payload={model:'future-model-'.repeat(20),input:'full client input',instructions:'client instructions',tools:[{type:'future_tool',opaque:{enabled:true}}],max_output_tokens:12345,reasoning:{effort:'high'},metadata:{client:'fixture'},future_option:{nested:['unchanged']}};
 assert.equal((await app.send(m,payload,{'thread-id':thread,'x-codex-beta-features':beta})).status,200);
 assert.equal(app.calls.length,1);const sent=app.calls[0];const body=JSON.parse(sent.options.body);
 for(const [name,value] of Object.entries(payload))assert.deepEqual(body[name],value);
 assert.equal(sent.options.headers['x-codex-beta-features'],beta);
 assert.equal((await app.send(m,payload,{'thread-id':thread})).status,200);
 assert.equal(app.store.get('SELECT COUNT(*) n FROM sessions').n,1);
});
test('invalid, revoked and expired keys are rejected while legacy quotas do not gate Responses',async t=>{
 const app=await setup(t),m=app.member();assert.equal((await app.send({secret:'invalid'})).status,401);
 app.store.run('UPDATE keys SET expires=1 WHERE id=?',m.keyId);assert.equal((await app.send(m)).status,403);
 app.store.run('UPDATE keys SET expires=NULL,enabled=0 WHERE id=?',m.keyId);assert.equal((await app.send(m)).status,403);
 assert.equal(app.calls.length,0);app.store.run('UPDATE keys SET enabled=1,quota=0 WHERE id=?',m.keyId);app.store.run('UPDATE members SET quota=0 WHERE id=?',m.memberId);app.store.run("UPDATE accounts SET token_budget=0 WHERE id='account-one'");assert.equal((await app.send(m)).status,200);
});
test('upstream errors are redacted and known rejection releases reservations',async t=>{
 const app=await setup(t,()=>new Response('fixture-secret must not reach the client',{status:429})),m=app.member();const r=await app.send(m);assert.equal(r.status,429);const text=await r.text();assert.ok(!text.includes('fixture-secret'));assert.match(text,/upstream_rate_limit/);assert.equal(app.store.get('SELECT status FROM requests').status,'failed');assert.equal(app.store.get('SELECT reserved FROM requests').reserved,0);
});
test('full-history continuation retains unknown usage without locking the client thread',async t=>{
 let calls=0;const app=await setup(t,()=>eventStream(++calls===1?[{type:'response.created',response:{id:'r'}}]:[complete()])),m=app.member();
 const r=await app.send(m,{stream:true},{'thread-id':'lost-stream'});assert.match(await r.text(),/upstream_incomplete/);
 const unknown=app.store.get("SELECT * FROM requests WHERE status='unknown'");assert.equal(unknown.reserved,0);
 const original=app.store.get('SELECT * FROM sessions');
 assert.equal((await app.send(m,{}, {'thread-id':'lost-stream'})).status,200);
 assert.equal(app.store.get('SELECT reserved FROM requests WHERE id=?',unknown.id).reserved,0);
 assert.equal(app.store.get('SELECT used FROM members').used,90);
 assert.equal(app.store.get('SELECT id FROM sessions').id,original.id);
 app.store.run("UPDATE accounts SET enabled=0 WHERE id='account-one'");
 assert.equal((await app.send(m,{}, {'thread-id':'lost-stream'})).status,503);
});
test('five same-Key conversations reach upstream concurrently; cancellation and busy guards are isolated',async t=>{
 const releases=[];let arrived=0;
 const app=await setup(t,async(url,options)=>new Promise((resolve,reject)=>{
  const n=Number(JSON.parse(options.body).input[0].content);arrived++;releases[n]=()=>resolve(eventStream([complete()]));options.signal.addEventListener('abort',()=>reject(options.signal.reason),{once:true});
 })),m=app.member();
 const accountId='00000000-0000-4000-8000-000000000001';app.store.run("UPDATE accounts SET id=? WHERE id='account-one'",accountId);
 const controllers=Array.from({length:5},()=>new AbortController());
 const pending=controllers.map((c,i)=>app.send(m,{input:[{role:'user',content:String(i)}]}, {'thread-id':'parallel-'+i},{signal:c.signal}));
 const cancelled=assert.rejects(pending[1],e=>e.name==='AbortError');
 try{
 await until(()=>arrived===5);assert.equal(app.store.get("SELECT COUNT(*) n FROM requests WHERE status='running'").n,5);
 assert.equal(app.store.get('SELECT SUM(reserved) n FROM requests').n,0);
 const admin=(path,method='GET')=>fetch(app.base+'/api/admin/'+path,{method,headers:{Authorization:'Bearer fixture-admin'}});
 assert.equal((await (await admin('state')).json()).busy,5);
 releases[0]();assert.equal((await pending[0]).status,200);assert.equal((await admin('keys/'+m.keyId,'DELETE')).status,409);
 controllers[1].abort();await cancelled;await until(()=>app.store.get("SELECT COUNT(*) n FROM requests WHERE status='running'").n===3);
 assert.equal((await (await admin('state')).json()).busy,3);
 assert.equal((await admin('accounts/'+accountId,'DELETE')).status,409);
 assert.equal((await admin('accounts/'+accountId+'/login','POST')).status,409);
 for(const i of [4,2,3])releases[i]();for(const i of [2,3,4])assert.equal((await pending[i]).status,200);
 assert.equal(app.store.get('SELECT used FROM keys WHERE id=?',m.keyId).used,360);
 assert.equal(app.store.get('SELECT unmetered FROM keys WHERE id=?',m.keyId).unmetered,1);
 assert.equal((await admin('keys/'+m.keyId,'DELETE')).status,200);
 }finally{for(const c of controllers)c.abort();for(const release of releases)release?.();await Promise.allSettled(pending);}
});
test('multiple employees and sibling Keys run together and isolate usage; same thread mapping is unique',async t=>{
 let arrived=0,release;const gate=new Promise(r=>release=r);
 const app=await setup(t,async()=>{arrived++;await gate;return eventStream([complete()]);}),a=app.member(),b=app.member(),sibling=app.store.createKey(a.memberId);
 const pending=[a,sibling,b].map(m=>app.send(m,{}, {'thread-id':'shared-client-id'}));await until(()=>arrived===3);
 assert.equal(app.store.get('SELECT COUNT(*) n FROM sessions').n,2);release();for(const p of pending)assert.equal((await p).status,200);
 assert.equal(app.store.get('SELECT used FROM members WHERE id=?',a.memberId).used,180);assert.equal(app.store.get('SELECT used FROM members WHERE id=?',b.memberId).used,90);
 for(const k of [a.keyId,sibling.keyId,b.keyId])assert.equal(app.store.get('SELECT used FROM keys WHERE id=?',k).used,90);
});
test('completed response without usage succeeds and records an unmetered call instead of blocking',async t=>{
 const app=await setup(t,()=>{const event=complete();delete event.response.usage;return eventStream([event]);}),m=app.member();
 for(let i=0;i<2;i++)assert.equal((await app.send(m,{}, {'thread-id':'no-usage'})).status,200);
 assert.equal(app.store.get('SELECT used FROM keys').used,0);assert.equal(app.store.get('SELECT unmetered FROM keys').unmetered,2);
 assert.equal(app.store.get('SELECT SUM(reserved) n FROM requests').n,0);
 const request=app.store.get('SELECT id FROM requests');assert.throws(()=>app.store.finish(request.id,{input:90}),/already settled/);assert.equal(app.store.get('SELECT used FROM keys').used,0);
});
test('shutdown cancels all parallel upstream calls and finishes their records',async t=>{
 let arrived=0;const app=await setup(t,(url,options)=>new Promise((resolve,reject)=>{arrived++;options.signal.addEventListener('abort',()=>reject(options.signal.reason),{once:true});})),m=app.member();
 const pending=Array.from({length:5},(_,i)=>app.send(m,{}, {'thread-id':'shutdown-'+i}));await until(()=>arrived===5);
 const stopped=app.close();for(const p of pending)assert.equal((await p).status,499);await stopped;
});
test('stream parser handles split UTF-8 and CRLF without losing tool arguments',async()=>{
 const text=frame({type:'response.output_text.delta',delta:'你好'}).replaceAll('\n','\r\n')+frame(complete([])).replaceAll('\n','\r\n');const bytes=Buffer.from(text);const events=[];
 const {transport}=fixtureTransport(()=>new Response(new ReadableStream({start(c){for(const b of bytes)c.enqueue(Uint8Array.of(b));c.close();}}),{headers:{'Content-Type':'text/event-stream'}}));
 const r=await transport.run({binary:'unused',home:'fixture',payload:validateResponses({model,input:[]}),sessionId:'fixture',onEvent:async e=>events.push(e)});assert.equal(events[0].delta,'你好');assert.equal(r.input,80);
});
test('valid SSE without Content-Type is accepted, but an explicit non-SSE body is rejected',async()=>{
 for(const contentType of [null,'application/json']){
  const {transport}=fixtureTransport(()=>new Response(Buffer.from(frame(complete([]))),{headers:contentType?{'Content-Type':contentType}:{}}));
  const pending=transport.run({binary:'unused',home:'fixture',payload:validateResponses({model,input:[]}),sessionId:'fixture'});
  if(contentType)await assert.rejects(pending,error=>error.code==='upstream_protocol');else assert.equal((await pending).input,80);
 }
});
test('long active streams are not cut off after two minutes; explicit client cancellation still releases them',async t=>{
 t.mock.timers.enable({apis:['setTimeout']});
 const {transport}=fixtureTransport(()=>eventStream([{type:'response.created',response:{id:'r'}}]));
 const controller=new AbortController();let started,observed;const ready=new Promise(resolve=>started=resolve);
 const pending=transport.run({binary:'unused',home:'fixture',payload:validateResponses({model,input:[]}),sessionId:'fixture',signal:controller.signal,onEvent:async(event,signal)=>{
  assert.ok(signal instanceof AbortSignal);observed=signal;
  await new Promise((resolve,reject)=>{signal.addEventListener('abort',()=>reject(signal.reason),{once:true});started();});
 }});
 const rejected=assert.rejects(pending,error=>error.code==='request_cancelled'&&error.usageKnown===false);
 await ready;t.mock.timers.tick(600000);assert.equal(observed.aborted,false);controller.abort();await rejected;
});

test('restart counts unfinished observation once without reserving or resetting cumulative usage',()=>{
 const root=mkdtempSync(join(tmpdir(),'harbor-meter-restart-')),file=join(root,'data.sqlite');let store=new Store(file);
 try{
  const member=store.createMember('Fixture',0,null),identity=store.authenticate(member.secret);store.run('UPDATE keys SET used=75 WHERE id=?',member.keyId);
  const requestId=store.observe(identity,'fixture','fixture',model);store.run("UPDATE requests SET status='running' WHERE id=?",requestId);store.close();store=new Store(file);
  assert.equal(store.get('SELECT used FROM keys').used,75);assert.equal(store.get('SELECT unmetered FROM keys').unmetered,1);assert.equal(store.get('SELECT reserved FROM requests').reserved,0);
  assert.equal(store.get('SELECT status FROM requests').status,'interrupted');store.close();store=new Store(file);assert.equal(store.get('SELECT unmetered FROM keys').unmetered,1);
 }finally{store.close();rmSync(root,{recursive:true,force:true});}
});

test('five SSE conversations all begin streaming before any conversation completes',async t=>{
 const releases=[];const app=await setup(t,(url,options)=>{
  const n=Number(JSON.parse(options.body).input[0].content);
  return new Response(new ReadableStream({start(c){
   c.enqueue(Buffer.from(frame({type:'response.created',response:{id:'stream-'+n}})));
   let done=false;releases[n]=()=>{if(done)return;done=true;c.enqueue(Buffer.from(frame({type:'response.output_text.delta',delta:'output-'+n})+frame(complete())));c.close();};
  }}),{headers:{'Content-Type':'text/event-stream'}});
 }),m=app.member();
 try{
  const pending=Array.from({length:5},(_,i)=>app.send(m,{stream:true,input:[{role:'user',content:String(i)}]},{'thread-id':'sse-'+i}));
  const responses=await Promise.all(pending);assert.ok(responses.every(r=>r.status===200));assert.equal(app.store.get("SELECT COUNT(*) n FROM requests WHERE status='running'").n,5);
  const readers=responses.map(r=>r.body.getReader());for(const reader of readers)assert.match(new TextDecoder().decode((await reader.read()).value),/response.created/);
  for(const n of [3,0,4,1,2])releases[n]();
  for(let i=0;i<readers.length;i++){let output='';for(;;){const r=await readers[i].read();if(r.done)break;output+=new TextDecoder().decode(r.value);}assert.ok(output.includes('output-'+i));assert.ok(output.includes('response.completed'));}
  assert.equal(app.store.get('SELECT used FROM keys').used,450);
 }finally{for(const release of releases)release?.();}
});

test('transient upstream failures and 429 do not label the subscription as disconnected',async t=>{
 for(const status of [429,500]){
  const app=await setup(t,()=>new Response('fixture',{status})),m=app.member();await app.send(m);
  assert.equal(app.store.get('SELECT state FROM accounts').state,'ready');assert.equal(app.store.get('SELECT last_error FROM accounts').last_error,null);
 }
 const app=await setup(t,()=>eventStream([{type:'response.created',response:{id:'fixture'}}])),m=app.member();await app.send(m);
 assert.equal(app.store.get('SELECT state FROM accounts').state,'ready');assert.equal(app.store.get('SELECT status FROM requests').status,'unknown');
});
test('authentication rejection alone marks login for checking and Responses thread is the actual forwarded identifier',async t=>{
 const app=await setup(t,()=>new Response('fixture',{status:401})),m=app.member();await app.send(m,{}, {'thread-id':'fixture-client-thread'});
 assert.equal(app.store.get('SELECT state FROM accounts').state,'degraded');
 const row=app.store.snapshot().sessions[0],sent=app.calls.find(x=>x.url.endsWith('/responses'));
 assert.equal(row.upstream_id,null);assert.equal(row.upstream_thread,sent.options.headers['thread-id']);assert.equal(row.upstream_thread,sent.options.headers['session-id']);assert.equal(row.request_count,1);
});

test('plain and zstd HTTP requests larger than the old 8MiB limit reach upstream with history intact',async t=>{
 const {randomBytes,createHash}=await import('node:crypto'),{zstdCompressSync}=await import('node:zlib');
 const app=await setup(t),m=app.member(),large=randomBytes(7*1024*1024).toString('base64'),digest=x=>createHash('sha256').update(x).digest('hex');
 const payload={model,input:[{role:'user',content:large},{type:'compaction',encrypted_content:'fixture-opaque-state'}],stream:false,context_management:[{type:'compaction',compact_threshold:100000}]};
 const raw=Buffer.from(JSON.stringify(payload));assert.ok(raw.length>8*1024*1024);
 for(const compressed of [false,true]){
  const response=await fetch(app.base+'/v1/responses',{method:'POST',headers:{Authorization:'Bearer '+m.secret,'Content-Type':'application/json',...(compressed?{'Content-Encoding':'zstd'}:{})},body:compressed?zstdCompressSync(raw):raw});
  assert.equal(response.status,200);await response.json();const sent=JSON.parse(app.calls.filter(c=>c.url.endsWith('/responses')).at(-1).options.body);
  assert.equal(digest(sent.input[0].content),digest(large));assert.deepEqual(sent.input[1],payload.input[1]);assert.deepEqual(sent.context_management,payload.context_management);
 }
 assert.equal(app.store.snapshot().keys[0].used,180);
});
test('bad compression is rejected before inference and unauthenticated requests never decode it',async t=>{
 const app=await setup(t),m=app.member();
 const send=key=>fetch(app.base+'/v1/responses',{method:'POST',headers:{Authorization:'Bearer '+key,'Content-Type':'application/json','Content-Encoding':'zstd'},body:'invalid-zstd'});
 assert.equal((await send('invalid')).status,401);const invalid=await send(m.secret);assert.equal(invalid.status,400);assert.equal((await invalid.json()).error.type,'invalid_compressed_body');assert.equal(app.calls.length,0);assert.equal(app.store.get('SELECT COUNT(*) n FROM requests').n,0);
});

test('large upstream SSE events are forwarded without the old 8MiB ceiling',async()=>{
 const text='x'.repeat(9*1024*1024),events=[];
 const {transport}=fixtureTransport(()=>eventStream([{type:'response.output_text.delta',delta:text},complete([])]));
 const result=await transport.run({binary:'unused',home:'fixture',payload:validateResponses({model,input:[]}),sessionId:'fixture',onEvent:async e=>events.push(e)});
 assert.equal(events[0].delta.length,text.length);assert.equal(result.input,80);
});
test('upstream 413 remains distinguishable from authentication and network failure',async t=>{
 const app=await setup(t,()=>new Response('fixture-private-error',{status:413})),m=app.member();const r=await app.send(m);
 assert.equal(r.status,413);const b=await r.json();assert.equal(b.error.type,'upstream_payload_too_large');assert.ok(!JSON.stringify(b).includes('fixture-private-error'));
});
