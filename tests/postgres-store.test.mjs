import {once as eventOnce} from 'node:events';
import {createPostgresChatState} from '../postgres-chat-state.mjs';
import {chatSettlementFacts,digestChatMessages} from '../chat-facts.mjs';
import {spawn} from 'node:child_process';
import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {mkdtempSync,rmSync,writeFileSync,readFileSync,mkdirSync,existsSync,readdirSync,statSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createPostgresGateway,startPostgresGateway} from '../postgres-gateway.mjs';
import {passwordHash} from '../auth.mjs';
import {PostgresStore} from '../postgres-store.mjs';
import {Store,hash,usageWindow} from '../store.mjs';
import {createResponsesTransport} from '../responses.mjs';
import pg from 'pg';
import {migratePostgres} from '../postgres-migrate.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {ConversationCapture} from '../conversation-capture.mjs';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {createPostgresResponsesHandler} from '../postgres-responses.mjs';
import {createPostgresSessionHandoffs} from '../postgres-session-handoffs.mjs';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';
import {registerRequestHandoffGroup} from '../request-handoff-group.mjs';
import {transferWindow} from '../transfer-schedule.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresMemberBatches} from '../postgres-member-batches.mjs';
import {createPostgresMemberQueries,memberQuery} from '../postgres-member-query.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';
import {createMemberLimits} from '../member-limits.mjs';
import {createPostgresRouter,lockPostgresRouting} from '../postgres-routing.mjs';
import {createPostgresJobs} from '../postgres-jobs.mjs';
import {createPostgresWorkerRuntime} from '../postgres-worker-runtime.mjs';
import {createPostgresRoutingMaintenance} from '../postgres-routing-maintenance.mjs';
import {createPostgresWorker} from '../postgres-worker.mjs';
import {createPostgresAccountQuotas} from '../postgres-account-quotas.mjs';
import {createBalancer,nextWeightReset,planBalance} from '../balancing.mjs';
import {quoteReference,referenceMetric} from '../pricing.mjs';
import {createPostgresPlanningInput} from '../postgres-planning-input.mjs';
import {createPostgresPlanningJobs} from '../postgres-planning-jobs.mjs';
import {createPostgresNightly} from '../postgres-nightly.mjs';
import {createPostgresNightlyRevisions} from '../postgres-nightly-revisions.mjs';
import {createRoutingPreparation} from '../routing-preparation.mjs';
import {createPostgresQueryQueue} from '../postgres-query-queue.mjs';
import {createPostgresStateQueries,stateQuery} from '../postgres-state-query.mjs';
import {createPostgresRequestQueries} from '../postgres-request-query.mjs';
import {createPostgresHistoryQueries} from '../postgres-history-query.mjs';
import {requestsQuery,requestsReader,historyQuery} from '../observability.mjs';
import http from 'node:http';
import https from 'node:https';
import {sign} from 'node:crypto';
import {certificateFixture,clientKey} from './device-fixture.mjs';
import {deviceEnrollmentMessage,deviceHardwareDigest} from '../device-proof.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET;
const integration={skip:!url&&!socket?'需隔离的PostgreSQL 18.6；CI test_postgres执行真实数据库验证':false};
function options(database){
  if(url){const value=new URL(url);if(database)value.pathname='/'+database;return {connectionString:value.toString(),allowPlaintextLocalNetwork:true};}
  return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_ci',database:database||'postgres'}};
}
async function fixture(t){
  const admin=new PostgresStore({...options(),poolSize:1}),name='harbor_test_'+randomUUID().replaceAll('-','');
  // Generated identifiers only; never accept an existing database name as a cleanup target.
  const end=Date.now()+30000;
  for(;;){try{await admin.get('SELECT 1 ready');break;}catch(e){if(!['ECONNREFUSED','57P03'].includes(e.code)||Date.now()>=end)throw e;await new Promise(r=>setTimeout(r,100));}}
  await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");
  const store=new PostgresStore({...options(name),poolSize:4});
  t.after(async()=>{await store.close();await admin.query('DROP DATABASE '+name);await admin.close();});
  return {store,name};
}
async function legacyMember(t,store){
  const root=mkdtempSync(join(tmpdir(),'harbor-pg-key-')),legacy=new Store(join(root,'legacy.sqlite'),{encryptionKey:randomBytes(32)});
  t.after(()=>{legacy.close();rmSync(root,{recursive:true,force:true});});
  const member=legacy.createMember('合成迁移员工',null,Date.now()+60000,['gpt-6.1-sol']);
  for(const [table,id] of [['members',member.memberId],['keys',member.keyId]]){
    const row=legacy.get('SELECT * FROM '+table+' WHERE id=?',id),columns=Object.keys(row);
    await store.run('INSERT INTO harbor.'+table+'('+columns.join(',')+') VALUES('+columns.map((_,i)=>'$'+(i+1)).join(',')+')',...columns.map(c=>row[c]));
  }
  return {member,legacy};
}

test('HTTP Responses uses the original Key, PostgreSQL ownership, shared model policy, metering and admin-only incremental recordings',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store);
  const account=randomUUID(),session=randomUUID(),at=Date.now(),root=mkdtempSync(join(tmpdir(),'harbor-pg-http-'));
  await store.run("INSERT INTO harbor.accounts(id,name,kind,home,state) VALUES($1,'合成账号','codex','fixture-home','ready')",account);
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,$3,'responses',$4,$4)",session,member.memberId,account,at);
  const recordings=new RecordingStore(store,{encryptionKey:randomBytes(32)}),buffer=await RecordingBuffer.create(recordings,{directory:join(root,'spool'),flushMs:10000});
  let dispatched=0;const transport=createResponsesTransport({version:async()=>'0.160.1',credentials:async()=>({token:'fixture-upstream',accountId:'fixture-account'}),fetchImpl:async()=>{
    dispatched++;return new Response([
      {type:'response.output_text.delta',item_id:'message',content_index:0,delta:'合成模型回复'},
      {type:'response.output_item.done',output_index:0,item:{id:'message',type:'message',content:[{type:'output_text',text:'合成模型回复'}]}},
      {type:'response.completed',response:{id:'fixture-response',model:'gpt-6.1-sol',output:[{id:'message',type:'message',content:[{type:'output_text',text:'合成模型回复'}]}],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:0}}}}
    ].map(event=>'data: '+JSON.stringify(event)+'\n\n').join(''),{headers:{'content-type':'text/event-stream','x-request-id':'req_synthetic_http'}});
  }});
  const limits=createPostgresMemberLimits(store),router=createPostgresRouter(store);
  const meteringRecovery=await MeteringRecovery.create(store,{encryptionKey:recordings.key,directory:join(root,'metering'),flushMs:10000});
  const handlerOptions={store,recordings,buffer,meteringRecovery,
    resolveRoute:router.resolve,onRouteFailure:router.block,checkLimits:(current,payload,options)=>limits.check(current.id,payload.model,{...options,tools:payload.tools}),
    transportForAccount:async()=>transport,adminAuthenticated:async req=>req.headers.authorization==='Bearer fixture-admin'};
  let handler=createPostgresResponsesHandler(handlerOptions);
  const alias=Object.create(meteringRecovery);alias.directory=buffer.directory;assert.throws(()=>createPostgresResponsesHandler({...handlerOptions,meteringRecovery:alias}),{code:'metering_configuration'});
  const server=http.createServer((req,res)=>{void handler.handle(req,res).then(handled=>{if(!handled){res.writeHead(404);res.end();}}).catch(e=>{if(res.headersSent){res.end('event: error\ndata: '+JSON.stringify({code:e.code})+'\n\n');return;}res.writeHead(e.status||500,{'Content-Type':'application/json'});res.end(JSON.stringify({error:{code:e.code||'internal_error'}}));});});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  t.after(async()=>{await new Promise(resolve=>server.close(resolve));await handler.closeQueries();await meteringRecovery.close();await buffer.close();rmSync(root,{recursive:true,force:true});});
  const send=(payload,secret=member.secret)=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json','x-harbor-session':session,'thread-id':'fixture-http-client'},body:JSON.stringify(payload)});
  let response=await send({model:'gpt-6.1-sol',input:'本轮合成提问 Bearer opaque-secret',stream:false,reasoning:{effort:'high'},service_tier:'auto'});assert.equal(response.status,200);assert.equal((await response.json()).id,'fixture-response');const request=response.headers.get('x-harbor-request');assert.ok(request);
  await buffer.flush();
  response=await fetch(origin+'/api/admin/requests/'+request+'/recording',{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,401);
  response=await fetch(origin+'/api/admin/requests/'+request+'/recording?limit=1',{headers:{authorization:'Bearer fixture-admin'}});assert.equal(response.status,200);const first=await response.json();assert.equal(first.state,'complete');assert.equal(first.hasMore,true);assert.ok(!JSON.stringify(first).includes('opaque-secret'));
  response=await fetch(origin+'/api/admin/requests/'+request+'/recording?after='+first.next,{headers:{authorization:'Bearer fixture-admin'}});const later=await response.json();assert.ok(later.events.some(e=>e.content.text?.includes('合成模型回复')));assert.ok(later.events.every(e=>e.sequence>first.next));
  const saved=await store.get('SELECT * FROM harbor.requests WHERE id=$1',request);assert.equal(saved.upstream_request_id,'req_synthetic_http');assert.equal(saved.effort,'high');assert.equal(saved.service_tier,'auto');assert.equal(saved.input,100);assert.equal(saved.output,20);assert.equal(saved.status,'ok');assert.equal(saved.account_id,account);assert.equal(saved.session_id,session);assert.equal((await store.referenceSummary('key_id',member.keyId)).known,1);
  response=await fetch(origin+'/api/admin/requests/'+request,{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,401);
  response=await fetch(origin+'/api/admin/requests/'+request,{headers:{authorization:'Bearer fixture-admin'}});const observation=await response.json();assert.equal(response.status,200);assert.equal(observation.id,request);assert.equal(observation.upstream_request_id,'req_synthetic_http');assert.equal(observation.effort,'high');assert.equal(observation.service_tier,'auto');assert.equal(observation.recording_state,'complete');assert.ok(observation.timeline.length);assert.equal(observation.standard.total_exact,(await store.referenceSummary('key_id',member.keyId)).total_exact);assert.equal(observation.input,undefined);assert.equal(observation.price_version,undefined);
  response=await fetch(origin+'/api/admin/state',{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,401);
  response=await fetch(origin+'/api/admin/state?session_page_size=1',{headers:{authorization:'Bearer fixture-admin'}});const overview=await response.json();assert.equal(response.status,200);assert.equal(overview.totals.requests,1);assert.equal(overview.members[0].id,member.memberId);assert.equal(overview.sessions.length,1);assert.equal(overview.totals.standard.total_exact,observation.standard.total_exact);
  response=await fetch(origin+'/api/admin/state?session_page=0',{headers:{authorization:'Bearer fixture-admin'}});assert.equal(response.status,400);
  response=await fetch(origin+'/api/admin/requests?view=list',{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,401);
  response=await fetch(origin+'/api/admin/requests?view=list',{headers:{authorization:'Bearer fixture-admin'}});const listing=await response.json();assert.equal(response.status,200);assert.equal(listing.rows[0].id,request);assert.equal(listing.rows[0].inserted_xid,undefined);assert.equal(listing.rows[0].q_total,undefined);
  assert.match(listing.rows[0].input_preview,/本轮合成提问/);assert.ok(['available','uncertain'].includes(listing.rows[0].input_preview_state));assert.ok(!JSON.stringify(listing).includes('opaque-secret'));assert.equal(listing.rows[0].preview_cipher,undefined);
  response=await fetch(origin+'/api/admin/requests?view=summary&snapshot='+listing.snapshot,{headers:{authorization:'Bearer fixture-admin'}});const statistics=await response.json();assert.equal(response.status,200);assert.equal(statistics.summary.standard.total_exact,observation.standard.total_exact);assert.equal(statistics.summary.requests,1);assert.equal(statistics.byModel[0].standard.total_exact,observation.standard.total_exact);assert.equal(statistics.watermarkKind,'postgres_visibility');
  response=await send({model:'gpt-6.1-sol',input:'流式合成提问',stream:true});assert.equal(response.status,200);const streamed=await response.text();assert.ok(streamed.includes('response.completed'));assert.ok(!streamed.includes('event: error'));await buffer.flush();
  const finish=store.finish.bind(store);store.finish=async()=>{throw Error('injected settlement outage');};
  response=await send({model:'gpt-6.1-sol',input:'结算故障后不重放模型',stream:false});assert.equal(response.status,200);assert.equal(response.headers.get('x-harbor-usage'),'pending');const recoveryId=response.headers.get('x-harbor-request');await response.json();
  assert.equal((await store.get('SELECT ended FROM harbor.requests WHERE id=$1',recoveryId)).ended,null);assert.ok(meteringRecovery.status().files>0);
  store.finish=finish;await meteringRecovery.flush();assert.deepEqual(await store.get('SELECT status,upstream_request_id FROM harbor.requests WHERE id=$1',recoveryId),{status:'ok',upstream_request_id:'req_synthetic_http'});assert.equal(dispatched,3);assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member.memberId)).used,360);
  const start=buffer.start.bind(buffer),append=buffer.append.bind(buffer);let orphanAppends=0;buffer.start=()=>false;buffer.append=(...args)=>{orphanAppends++;return append(...args);};
  response=await send({model:'gpt-6.1-sol',input:'初始化拒绝后不提交孤立正文',stream:false});assert.equal(response.status,200);const rejectedStartId=response.headers.get('x-harbor-request');await response.json();assert.equal(orphanAppends,0);buffer.start=start;buffer.append=append;await buffer.flush();
  const deadline=Date.now()+5000;while((await store.get('SELECT recording_state FROM harbor.requests WHERE id=$1',rejectedStartId)).recording_state!=='partial'){assert.ok(Date.now()<deadline,'missing queue-limit marker');await new Promise(resolve=>setTimeout(resolve,10));}
  let appendCalls=0;buffer.append=(...args)=>++appendCalls===2?false:append(...args);
  response=await send({model:'gpt-6.1-sol',input:'已接纳的输入前缀应保留',stream:false});assert.equal(response.status,200);const partialId=response.headers.get('x-harbor-request');await response.json();buffer.append=append;
  await buffer.flush();const partial=await recordings.read(partialId);assert.equal(partial.state,'partial');assert.ok(partial.events.some(event=>event.kind==='user'&&event.content.text.includes('输入前缀')));assert.equal(buffer.status().files,0);
  const finishRecording=buffer.finish.bind(buffer);buffer.finish=()=>false;
  response=await send({model:'gpt-6.1-sol',input:'R22完成标记拥塞后仍可恢复此前接受的正文',stream:false});assert.equal(response.status,200);const latePrefix=response.headers.get('x-harbor-request');await response.json();buffer.finish=finishRecording;
  const partialDeadline=Date.now()+5000;while((await store.get('SELECT recording_state FROM harbor.requests WHERE id=$1',latePrefix)).recording_state!=='partial'){assert.ok(Date.now()<partialDeadline);await new Promise(resolve=>setTimeout(resolve,10));}
  const emptyPrefix=await recordings.read(latePrefix);assert.equal(emptyPrefix.events.length,0);assert.equal(emptyPrefix.hasMore,false);
  await buffer.flush();const recoveredPrefix=await recordings.read(latePrefix,{after:emptyPrefix.next});assert.equal(recoveredPrefix.state,'partial');assert.ok(recoveredPrefix.events.some(event=>event.kind==='user'&&event.content.text.includes('R22')));assert.equal((await recordings.read(latePrefix,{after:recoveredPrefix.next})).events.length,0);
  // Synchronous recording failures must never change a successful upstream
  // response into a failed request or dispatch the model a second time.
  for(const [object,method] of [[buffer,'start'],[buffer,'append'],[buffer,'finish'],[ConversationCapture.prototype,'input'],[ConversationCapture.prototype,'event'],[ConversationCapture.prototype,'finish']]){
    const original=object[method],before=dispatched;object[method]=()=>{throw Error('synthetic capture failure');};
    try{response=await send({model:'gpt-6.1-sol',input:'采集失败但模型与计量应成功',stream:method==='event'});assert.equal(response.status,200);const body=await response.text();assert.ok(body.includes(method==='event'?'response.completed':'fixture-response'));assert.ok(!body.includes('event: error'));assert.equal(dispatched,before+1);}
    finally{object[method]=original;}
    await buffer.flush();
    const deadline=Date.now()+5000;
    for(;;){const saved=await store.get('SELECT status,recording_state,recording_reason FROM harbor.requests ORDER BY query_sequence DESC LIMIT 1');if(saved.recording_state==='partial'){assert.equal(saved.status,'ok');assert.equal(saved.recording_reason,'capture_failed');break;}assert.ok(Date.now()<deadline,'capture failure must be visible');await new Promise(resolve=>setTimeout(resolve,10));}
  }
  const acceptedCalls=dispatched;
  response=await send({model:'unapproved-model',input:'should be rejected'});assert.equal(response.status,403);assert.equal(dispatched,acceptedCalls);
  response=await send({model:'gpt-6.1-sol',input:'not authenticated'},'invalid');assert.equal(response.status,401);assert.equal(dispatched,acceptedCalls);
  const runTransport=transport.run;transport.run=async()=>{throw Object.assign(Error('synthetic confirmed credential loss'),{status:503,code:'upstream_authentication',routeFailure:'login_expired',usageKnown:true});};
  response=await send({model:'gpt-6.1-sol',input:'confirmed route failure'});assert.equal(response.status,503);assert.equal((await store.get('SELECT route_block FROM harbor.accounts WHERE id=$1',account)).route_block,'login_expired');
  response=await send({model:'gpt-6.1-sol',input:'blocked route is not retried'});assert.equal(response.status,503);assert.equal((await response.json()).error.code,'session_account_blocked');
  transport.run=runTransport;await store.run('UPDATE harbor.accounts SET route_block=NULL,route_epoch=route_epoch+1 WHERE id=$1',account);
  await limits.save(member.memberId,{enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'0'}],expected_version:0});
  response=await send({model:'gpt-6.1-sol',input:'do not bypass limits'});assert.equal(response.status,429);assert.equal(dispatched,acceptedCalls);
  await store.run('UPDATE harbor.members SET usage_limits=$1 WHERE id=$2',JSON.stringify({enabled:true,rules:[]}),member.memberId);response=await send({model:'gpt-6.1-sol',input:'invalid limits are not unlimited'});assert.equal(response.status,409);assert.equal(dispatched,acceptedCalls);
  await store.run('UPDATE harbor.members SET usage_limits=$1 WHERE id=$2',JSON.stringify({enabled:false,rules:[]}),member.memberId);
  await handler.closeQueries();handler=createPostgresResponsesHandler({...handlerOptions,recordingEnabled:false});
  const startRecording=buffer.start;buffer.start=()=>{throw Error('disabled capture must not run');};
  try{response=await send({model:'gpt-6.1-sol',input:'关闭采集仍完成模型与计量',stream:false});assert.equal(response.status,200);const id=response.headers.get('x-harbor-request');await response.json();assert.equal((await store.get('SELECT recording_state FROM harbor.requests WHERE id=$1',id)).recording_state,'disabled');}
  finally{buffer.start=startRecording;}
  handler.drain();response=await send({model:'gpt-6.1-sol',input:'draining'});assert.equal(response.status,503);assert.equal(dispatched,acceptedCalls+1);
});

test('R21 slow uploads protect routing until every same-member body completes or disconnects',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),root=mkdtempSync(join(tmpdir(),'harbor-pg-upload-'));
  for(const aid of ['a','b'])await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES($1,$1,$1,'ready')",aid);
  const put=(name,value)=>store.run('INSERT INTO harbor.gateway_settings VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',name,JSON.stringify(value));
  await put('routing_strategy',{weighted:true});await put('routing_weights',{mode:'manual',weights:{a:100,b:0},expires:nextWeightReset(Date.now()),version:1});
  const router=createPostgresRouter(store),limits=createPostgresMemberLimits(store),recordings=new RecordingStore(store,{encryptionKey:randomBytes(32)}),buffer=await RecordingBuffer.create(recordings,{directory:join(root,'spool')});let calls=0;
  const meteringRecovery=await MeteringRecovery.create(store,{encryptionKey:recordings.key,directory:join(root,'metering')});
  const handler=createPostgresResponsesHandler({store,recordings,buffer,meteringRecovery,resolveRoute:router.resolve,checkLimits:(m,p,o)=>limits.check(m.id,p.model,{...o,tools:p.tools}),adminAuthenticated:async()=>false,
    transportForAccount:async()=>({run:async()=>{calls++;return {status:'ok',input:1,output:0,cached:0,usageKnown:true,response:{id:'synthetic',model:'gpt-6.1-sol',output:[]}};}})});
  const server=http.createServer((req,res)=>{void handler.handle(req,res).then(found=>{if(!found){res.writeHead(404);res.end();}}).catch(e=>{if(res.destroyed)return;res.writeHead(e.status||500,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:e.code}}));});});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port,headers={authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'slow-client'},uploads=[];
  t.after(async()=>{for(const request of uploads)request.destroy();await new Promise(resolve=>server.close(resolve));await handler.closeQueries();await meteringRecovery.close();await buffer.close();rmSync(root,{recursive:true,force:true});});
  const send=()=>fetch(origin+'/v1/responses',{method:'POST',headers,body:JSON.stringify({model:'gpt-6.1-sol',input:'synthetic',stream:false})});
  const wait=async predicate=>{const deadline=Date.now()+5000;while(!await predicate()){assert.ok(Date.now()<deadline,'upload state timeout');await new Promise(resolve=>setTimeout(resolve,10));}};
  let response=await send();assert.equal(response.status,200);await response.json();const before=await store.get("SELECT * FROM harbor.sessions WHERE account_id='a'");
  await put('routing_weights',{mode:'manual',weights:{a:0,b:100},expires:nextWeightReset(Date.now()),version:2});
  for(let i=0;i<2;i++){const request=http.request(origin+'/v1/responses',{method:'POST',headers});request.on('error',()=>{});request.on('response',res=>res.resume());uploads.push(request);request.write('{"model":"gpt-6.1-sol","input":"');}
  await wait(()=>handler.status().preparing===2);
  response=await send();assert.equal(response.status,409);assert.equal((await response.json()).error.code,'session_busy');assert.equal(calls,1);
  // Finishing one body must not clear the other upload's reference count.
  // Body-read completion precedes async routing; wait for its actual rejection before releasing the sibling.
  const completedUpload=eventOnce(uploads[0],'response',{signal:AbortSignal.timeout(5000)});
  uploads[0].end('synthetic","stream":false}');const [uploadResult]=await completedUpload;assert.equal(uploadResult.statusCode,409);assert.equal(calls,1);await wait(()=>handler.status().preparing===1);
  response=await send();assert.equal(response.status,409);await response.json();assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',before.id)).route_version,0);
  uploads[1].destroy();await wait(()=>handler.status().preparing===0);
  response=await send();assert.equal(response.status,200);await response.json();const after=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',before.id);assert.equal(after.account_id,'b');assert.equal(after.route_version,1);assert.equal(calls,2);
});

test('request input previews are page bounded, encrypted, expiration aware and isolated from damaged ciphertext',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret),at=Date.now();
  const recording=new RecordingStore(store,{encryptionKey:randomBytes(32),clock:()=>at});
  const make=async(text,turnState='known')=>{const id=await store.observe(person,'fixture','fixture','gpt-6.1-sol');await recording.start(id,{turnState});await recording.append(id,[{sourceKey:'input',kind:text===null?'assistant':'user',provenance:text===null?'upstream':'client',content:{text:text??'只收到输出'}}]);await recording.finish(id);return id;};
  const available=await make('问题 Bearer fixture-private '+ '界'.repeat(300)),uncertain=await make('上下文中的输入','uncertain'),expired=await make('已过期的输入'),broken=await make('损坏的输入'),oversized=await make('异常大的密文'),empty=await make(null);
  await store.run('UPDATE harbor.recordings SET expires_at=$1 WHERE request_id=$2',at,expired);
  await store.run('UPDATE harbor.recordings SET preview_cipher=$1 WHERE request_id=$2',Buffer.alloc(80),broken);
  await store.run('UPDATE harbor.recordings SET preview_cipher=$1 WHERE request_id=$2',Buffer.alloc(4096),oversized);
  const previews=await recording.previews([available,uncertain,expired,broken,oversized,empty]);
  assert.equal(previews.get(available).input_preview_state,'available');assert.ok(previews.get(available).input_preview.length<=200);assert.ok(!previews.get(available).input_preview.includes('fixture-private'));
  assert.equal(previews.get(uncertain).input_preview_state,'uncertain');
  for(const [id,state] of [[expired,'expired'],[broken,'unreadable'],[oversized,'unreadable'],[empty,'unavailable']])assert.deepEqual(previews.get(id),{input_preview:null,input_preview_state:state});
  const {createPostgresRecordingJobs}=await import('../postgres-recording-jobs.mjs'),jobs=createPostgresJobs(store),backend=createPostgresRecordingJobs(store,{jobs,recordingKey:recording.key,clock:()=>at,storagePolicy:async()=>({captureAllowed:true,exportAllowed:true})});
  await jobs.enqueue('recording.purge',{id:'recording.purge:'+Math.floor(at/60000),payload:{}});const purge=await jobs.claim(['recording.purge']);assert.ok(purge);await jobs.complete(purge,await backend.handlers[purge.kind](purge));
  assert.equal(await store.get('SELECT request_id FROM harbor.recordings WHERE request_id=$1',expired),undefined);assert.deepEqual((await recording.previews([expired])).get(expired),{input_preview:null,input_preview_state:'expired'});
  const historical=await store.observe(person,'fixture','fixture','gpt-6.1-sol');assert.deepEqual((await recording.previews([historical])).get(historical),{input_preview:null,input_preview_state:'unavailable'});
  const ids=[],sql=[],originalAll=store.all.bind(store);store.all=async(query,...args)=>{sql.push(query);return originalAll(query,...args);};
  const reader=createPostgresRequestQueries(store,{clock:()=>at,readInputPreviews:page=>{ids.push(page);return recording.previews(page);}});t.after(()=>reader.close());
  const first=await reader.read(requestsQuery(new URLSearchParams({view:'list',page_size:'2'})));assert.deepEqual(ids,[first.rows.map(row=>row.id)]);assert.equal(sql.filter(query=>query.includes('preview_cipher')).length,1);assert.ok(sql.every(query=>!query.includes('conversation_events')));assert.ok(first.rows.every(row=>!('preview_cipher' in row)));
  await reader.read(requestsQuery(new URLSearchParams({view:'summary',snapshot:first.snapshot})));assert.equal(ids.length,1);
  const afterPurge=await reader.read(requestsQuery(new URLSearchParams({view:'list',page_size:'100'})));assert.equal(afterPurge.rows.find(row=>row.id===expired).input_preview_state,'expired');assert.equal(afterPurge.rows.find(row=>row.id===expired).input_preview,null);
  await assert.rejects(recording.previews(Array(101).fill(available)),{status:400});
});

test('recording pages bound fetched ciphertext bytes, advance past a large single event and retain separate metadata visibility',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret),at=Date.now();
  const request=await store.observe(person,'fixture','fixture','gpt-6.1-sol'),recording=new RecordingStore(store,{encryptionKey:randomBytes(32),maxPageBytes:256,maxEventBytes:1024,maxBytes:4096});
  await recording.start(request);await recording.append(request,[150,150,400].map((n,i)=>({sourceKey:'event-'+i,kind:'assistant',provenance:'upstream',content:{text:String(i).repeat(n)}})));await recording.finish(request);
  let after=0;for(let i=0;i<3;i++){const page=await recording.read(request,{after,limit:100});assert.equal(page.events.length,1);assert.equal(page.events[0].content.text,String(i).repeat([150,150,400][i]));assert.equal(page.hasMore,i<2);assert.ok(page.next>after);after=page.next;}
  // The pre-existing metadata detail keeps its seven-day scope; recordings use their own retention.
  await store.run('UPDATE harbor.requests SET started=$1,ended=$1 WHERE id=$2',at-8*86400000,request);
  const {postgresRequestObservation}=await import('../postgres-observability.mjs');await assert.rejects(postgresRequestObservation(store,request,at),{status:404});assert.equal((await recording.read(request)).events.length,1);
  assert.throws(()=>new RecordingStore(store,{encryptionKey:randomBytes(32),maxPageBytes:255}),{code:'recording_configuration'});
});

test('encrypted recording persists only filtered turn events, retries idempotently and pages incrementally',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret);
  const request=await store.observe(person,'fixture','fixture','gpt-6.1-sol'),events=[];
  const capture=new ConversationCapture({emit:event=>events.push(event)}),turnState=capture.input({instructions:'private-system',input:'本轮问题 Bearer opaque-secret'});
  for(const delta of ['回复前段','，后段'])capture.event({type:'response.output_text.delta',item_id:'msg',delta});
  capture.finish({output:[{id:'msg',type:'message',content:[{type:'output_text',text:'回复前段，后段'}]}]});
  const key=randomBytes(32),recording=new RecordingStore(store,{encryptionKey:key});
  assert.equal((await recording.start(request,{turnState})).started,true);assert.equal((await recording.start(request,{turnState})).started,false);
  assert.equal((await recording.append(request,events)).accepted,events.length);assert.equal((await recording.append(request,events)).accepted,0);
  let page=await recording.read(request,{limit:1});assert.equal(page.state,'pending');assert.equal(page.hasMore,true);assert.equal(page.turnState,'known');assert.ok(!JSON.stringify(page).includes('opaque-secret'));
  const next=await recording.read(request,{after:page.next});assert.ok(next.events.every(e=>e.sequence>page.next));assert.equal(next.hasMore,false);
  const raw=await store.all('SELECT content_cipher FROM harbor.conversation_events WHERE request_id=$1',request);assert.ok(raw.every(r=>!r.content_cipher.includes(Buffer.from('回复'))&&!r.content_cipher.includes(Buffer.from('opaque-secret'))));
  await recording.finish(request);page=await recording.read(request);assert.equal(page.state,'complete');
  assert.equal((await recording.append(request,[{sourceKey:'late',kind:'assistant',provenance:'upstream',content:{text:'late'}}])).accepted,0);
  await assert.rejects(new RecordingStore(store,{encryptionKey:randomBytes(32)}).read(request),{code:'recording_unreadable'});
  const stored=await store.get('SELECT sequence,content_cipher FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence LIMIT 1',request);
  await store.run('UPDATE harbor.conversation_events SET content_cipher=$1 WHERE request_id=$2 AND sequence=$3',Buffer.alloc(40),request,stored.sequence);await assert.rejects(recording.read(request),{code:'recording_unreadable'});
});

test('stream argument filtering stays invariant at every split after encrypted disk buffering and PostgreSQL replay',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret),request=await store.observe(person,'fixture','fixture','gpt-6.1-sol');
  const root=mkdtempSync(join(tmpdir(),'harbor-record-splits-')),recordings=new RecordingStore(store,{encryptionKey:randomBytes(32)}),buffer=await RecordingBuffer.create(recordings,{directory:join(root,'spool'),flushMs:10000}),events=[];
  t.after(async()=>{await buffer.close();rmSync(root,{recursive:true,force:true});});
  const capture=new ConversationCapture({maxFields:1024,emit:event=>events.push(event)}),expected=new Map(),expectedAssistant=new Map(),pem='-----BEGIN PRIVATE KEY-----fixture-material-----END PRIVATE KEY-----';
  const cases=[
    [{password:'opaque-secret',path:'visible.txt'},{password:'[已脱敏]',path:'visible.txt'}],
    [{authorization:123,path:'visible.txt'},{authorization:'[已脱敏]',path:'visible.txt'}],
    [{password:{pem},path:'visible.txt'},{password:'[已脱敏]',path:'visible.txt'}],
    [{cookie:[1,null,false],path:'visible.txt'},{cookie:'[已脱敏]',path:'visible.txt'}],
    [{note:'Authorization: opaque-fixture-secret\nvisible tail',path:'visible.txt'},{note:'Authorization: [已脱敏]\nvisible tail',path:'visible.txt'}]
  ];
  const header='Authorization: opaque-fixture-secret';
  for(let split=0;split<=header.length;split++)capture.input({input:[{type:'function_call_output',call_id:'source-'+split,output:JSON.stringify({content:[{type:'text',text:header.slice(0,split)},{type:'image',data:'SYNTHETIC_BINARY'},{type:'text',text:header.slice(split)}]})}]});
  capture.input({input:[{type:'function_call_output',call_id:'header-tail',output:JSON.stringify({content:[{type:'text',text:header+'\n'},{type:'text',text:'visible tail'},{type:'image',data:'SYNTHETIC_BINARY'}]})}]});
  const quoteHeader="Alice's configuration\nCookie: session='fixture-secret'; other=another-secret\nvisible tail";
  capture.input({input:[{type:'function_call_output',call_id:'quote-cookie',output:JSON.stringify({content:[{type:'text',text:quoteHeader}]})}]});
  capture.input({input:[{role:'user',content:[{type:'input_text',text:'hello'},{type:'input_image',image_url:'data:image/png;base64,PRIVATE'},{type:'input_text',text:'world'}]}]});
  let index=0;
  for(const [source,result] of cases){const raw=JSON.stringify(source);for(let split=0;split<=raw.length;split++,index++){
    const item={id:'tool-'+index,type:'function_call',name:'lookup',call_id:'call-'+index};expected.set('id:'+item.id+':arguments',result);
    capture.event({type:'response.output_item.added',output_index:index,item});
    for(const delta of [raw.slice(0,split),raw.slice(split)])capture.event({type:'response.function_call_arguments.delta',output_index:index,item_id:item.id,delta});
    capture.event({type:'response.output_item.done',output_index:index,item:{...item,arguments:raw}});
  }}
  for(const raw of [quoteHeader,'task_result_identifier','ask_user_for_confirmation'])for(let split=0;split<=raw.length;split++,index++){
    const itemId='plain-'+index;expectedAssistant.set('id:'+itemId+':0:text',raw===quoteHeader?"Alice's configuration\nCookie: [已脱敏]\nvisible tail":raw);
    for(const delta of [raw.slice(0,split),raw.slice(split)])capture.event({type:'response.output_text.delta',output_index:index,item_id:itemId,content_index:0,delta});
    capture.event({type:'response.output_item.done',output_index:index,item:{id:itemId,type:'message',content:[{type:'output_text',text:raw}]}});
  }
  assert.equal(capture.finish().truncated,false);assert.equal(buffer.start(request,'known'),true);
  for(let offset=0;offset<events.length;offset+=32)assert.equal(buffer.append(request,events.slice(offset,offset+32)),true);assert.equal(buffer.finish(request,{state:'complete'}),true);await buffer.flush();
  const fields=new Map(),assistantFields=new Map(),input=[];let after=0,toolResults=0;
  for(;;){const page=await recordings.read(request,{after,limit:100});for(const {kind,content:c} of page.events){
    if(kind==='tool_call')fields.set(c.field,c.mode==='replace'?c.text:(fields.get(c.field)||'')+c.text);
    else if(kind==='assistant')assistantFields.set(c.field,c.mode==='replace'?c.text:(assistantFields.get(c.field)||'')+c.text);
    else if(kind==='tool_result'){toolResults++;assert.ok(!JSON.stringify(c).includes('fixture-secret'));assert.ok(!JSON.stringify(c).includes('another-secret'));assert.ok(!JSON.stringify(c).includes('SYNTHETIC_BINARY'));const parsed=JSON.parse(c.output);if(c.callId==='quote-cookie')assert.ok(parsed.content[0].text.endsWith('visible tail'));else assert.ok(parsed.content.some(part=>part.content?.includes('省略')));if(c.callId==='header-tail')assert.equal(parsed.content[1].text,'visible tail');}
    else input.push({kind,...c});
  }after=page.next;if(!page.hasMore)break;}
  assert.equal(fields.size,expected.size);for(const [field,text] of fields){assert.deepEqual(JSON.parse(text),expected.get(field));assert.ok(!text.includes('opaque-secret'));assert.ok(!text.includes('fixture-material'));}
  assert.deepEqual(assistantFields,expectedAssistant);
  assert.equal(toolResults,header.length+3);assert.deepEqual(input.map(event=>event.kind),['user','attachment','user']);assert.deepEqual(input.map(event=>event.position),[0,1,2]);assert.equal(input[0].text,'hello');assert.equal(input[2].text,'world');
});

test('recording batch failure rolls back, capacity is visible and expiration cannot reveal content',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret);
  const request=await store.observe(person,'fixture','fixture','gpt-6.1-sol');let now=Date.now();const recording=new RecordingStore(store,{encryptionKey:randomBytes(32),maxBytes:512,maxEventBytes:256,retentionDays:1,clock:()=>now});
  await recording.start(request);
  const event={sourceKey:'one',kind:'user',provenance:'client',content:{text:'safe'}};
  const original=store.run.bind(store);let insertedRows=0;store.run=async(sql,...args)=>{
    const result=await original(sql,...args);
    if(sql.startsWith('INSERT INTO harbor.conversation_events')){
      insertedRows=Number((await store.get('SELECT COUNT(*) n FROM harbor.conversation_events WHERE request_id=$1',request)).n);
      throw Error('injected storage failure');
    }
    return result;
  };
  try{await assert.rejects(recording.append(request,[event,{...event,sourceKey:'two'}]),/injected storage failure/);}finally{store.run=original;}
  assert.equal(insertedRows,2);
  assert.equal((await recording.read(request)).events.length,0);assert.equal((await store.get('SELECT bytes FROM harbor.recordings WHERE request_id=$1',request)).bytes,0);
  const accepted=await Promise.all([recording.append(request,[event]),recording.append(request,[event])]);assert.equal(accepted.reduce((n,r)=>n+r.accepted,0),1);
  assert.equal((await recording.append(request,[{...event,sourceKey:'large',content:{text:'x'.repeat(300)}}])).state,'truncated');await recording.finish(request);assert.equal((await recording.read(request)).state,'truncated');
  now+=86400001;const expired=await recording.read(request);assert.equal(expired.state,'expired');assert.equal(expired.events.length,0);
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.conversation_events WHERE request_id=$1',request)).n,1);
  await assert.rejects(recording.read(request,{limit:101}),{status:400});
});

test('large user, tool and final-only assistant text are chunked below the event limit and read without loss',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret),request=await store.observe(person,'fixture','fixture','gpt-6.1-sol');
  const recordings=new RecordingStore(store,{encryptionKey:randomBytes(32)}),events=[],capture=new ConversationCapture({emit:event=>events.push(event)}),text={user:'user '.repeat(30000),tool_result:'tool '.repeat(30000),assistant:'answer '.repeat(30000)};
  const turnState=capture.input({input:[{role:'user',content:text.user},{type:'function_call_output',call_id:'call',output:text.tool_result}]});
  assert.equal(capture.finish({output:[{id:'message',type:'message',content:[{type:'output_text',text:text.assistant}]}]}).truncated,false);
  assert.ok(events.every(event=>Buffer.byteLength(JSON.stringify(event.content))<recordings.maxEventBytes));await recordings.start(request,{turnState});
  for(let offset=0;offset<events.length;offset+=32)assert.equal((await recordings.append(request,events.slice(offset,offset+32))).accepted,events.slice(offset,offset+32).length);await recordings.finish(request);
  const actual={user:'',tool_result:'',assistant:''};let after=0;
  for(;;){const page=await recordings.read(request,{after,limit:25});assert.equal(page.state,'complete');for(const event of page.events){const content=event.content;actual[event.kind]=(content.mode==='replace'?'':actual[event.kind])+(content.text??content.output);}after=page.next;if(!page.hasMore)break;}
  assert.deepEqual(actual,text);
});

test('PostgreSQL configuration rejects unsafe resources and verifies remote TLS by default',async()=>{
  for(const config of [{},{connection:{host:'/tmp/test'},poolSize:0},{connection:{host:'/tmp/test'},statementTimeoutMs:0},{connectionString:'https://not-postgres'},{connectionString:'postgresql://example.invalid/test?sslmode=disable'},{connection:{host:'example.invalid',ssl:{rejectUnauthorized:false}}}])assert.throws(()=>new PostgresStore(config),{status:503});
  const store=new PostgresStore({connection:{host:'example.invalid',database:'test'}});assert.equal(store.pool.options.ssl.rejectUnauthorized,true);await store.close();
});

test('R4/R6 final pg connection resolves environment host and preserves explicit CA and credentials',async()=>{
  const saved=Object.fromEntries(['PGHOST','PGSSLMODE'].map(k=>[k,process.env[k]]));
  try{
    process.env.PGHOST='fixture-remote.invalid';delete process.env.PGSSLMODE;
    let store=new PostgresStore({connectionString:'postgresql:///fixture_database'});
    let resolved=new pg.Client(store.pool.options).connectionParameters;
    assert.equal(resolved.host,'fixture-remote.invalid');assert.equal(resolved.ssl.rejectUnauthorized,true);await store.close();
    store=new PostgresStore({connection:{host:'/tmp/fixture-private',database:'fixture_database'}});resolved=new pg.Client(store.pool.options).connectionParameters;assert.equal(resolved.host,'/tmp/fixture-private');assert.equal(resolved.ssl,false);await store.close();
    store=new PostgresStore({connectionString:'postgresql://fixture-user:fixture-password@fixture-remote.invalid/fixture_database?sslmode=verify-full',connection:{ssl:{ca:'fixture-ca',cert:'fixture-cert',key:'fixture-private-key'}}});
    resolved=new pg.Client(store.pool.options).connectionParameters;
    assert.equal(resolved.ssl.ca,'fixture-ca');assert.equal(resolved.ssl.cert,'fixture-cert');assert.equal(resolved.ssl.key,'fixture-private-key');assert.equal(resolved.ssl.rejectUnauthorized,true);assert.equal(resolved.password,'fixture-password');assert.equal(store.pool.options.connectionString,undefined);await store.close();
    process.env.PGSSLMODE='disable';assert.throws(()=>new PostgresStore({connectionString:'postgresql:///fixture_database'}),{code:'database_configuration'});
  }finally{for(const [k,v] of Object.entries(saved))if(v===undefined)delete process.env[k];else process.env[k]=v;}
});

test('R6 TLS key survives all pg property-hiding and URL/file source combinations',async()=>{
  const root=mkdtempSync(join(tmpdir(),'harbor-pg-tls-'));
  try{
    const files={sslkey:join(root,'fixture.key'),sslcert:join(root,'fixture.crt'),sslrootcert:join(root,'fixture-ca.crt')};
    for(const [key,path] of Object.entries(files))writeFileSync(path,'fixture-'+key,{mode:0o600});
    const fileUrl=new URL('postgresql://fixture-remote.invalid/fixture_database');fileUrl.searchParams.set('sslmode','verify-full');for(const [key,path] of Object.entries(files))fileUrl.searchParams.set(key,path);
    for(const config of [
      {connection:{host:'fixture-remote.invalid',database:'fixture_database',ssl:{ca:'fixture-ca',cert:'fixture-cert',key:'fixture-key'}}},
      {connectionString:'postgresql://fixture-remote.invalid/fixture_database',connection:{ssl:{ca:'fixture-ca',cert:'fixture-cert',key:'fixture-key'}}},
      {connectionString:fileUrl.toString()}
    ]){
      const store=new PostgresStore(config);try{
        const actual=new pg.Client(store.pool.options).connectionParameters.ssl;
        assert.equal(actual.key,config.connection?'fixture-key':'fixture-sslkey');assert.equal(actual.cert,config.connection?'fixture-cert':'fixture-sslcert');assert.equal(actual.ca,config.connection?'fixture-ca':'fixture-sslrootcert');assert.equal(actual.rejectUnauthorized,true);
      }finally{await store.close();}
    }
  }finally{rmSync(root,{recursive:true,force:true});}
});

test('real PostgreSQL migrations serialize, are idempotent and reject edited historical SQL',integration,async t=>{
  const {store,name}=await fixture(t),second=new PostgresStore(options(name));
  try{
    const results=await Promise.all([store.migrate(),second.migrate()]);assert.equal(results.filter(r=>r.changed).length,1);assert.equal(await store.ready(),true);
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.schema_migrations')).n,22);
    assert.equal((await store.get("SELECT COUNT(*) n FROM information_schema.tables WHERE table_schema='harbor' AND table_name IN ('members','keys','accounts','sessions','requests','request_prices','recordings','conversation_events','background_jobs')")).n,9);
    assert.deepEqual(await store.get('SELECT enabled,version FROM harbor.extra_usage_policy WHERE singleton=1'),{enabled:false,version:0});
    assert.equal((await store.get("SELECT COUNT(*) n FROM information_schema.tables WHERE table_schema='harbor' AND table_name IN ('extra_usage_policy','account_extra_usage','account_extra_entitlements','account_extra_holds','account_extra_records')")).n,5);
    assert.equal((await store.get("SELECT COUNT(*) n FROM information_schema.tables WHERE table_schema='harbor' AND table_name IN ('extra_usage_previews','extra_usage_preview_items')")).n,2);
    await store.run("UPDATE harbor.schema_migrations SET checksum='changed' WHERE version=1");await assert.rejects(store.migrate(),{code:'database_migration_conflict'});
  }finally{await second.close();}
});

test('device and recording migrations upgrade populated version thirteen without changing credentials or encrypted source events',integration,async t=>{
  const {store}=await fixture(t);
  await store.query('CREATE SCHEMA harbor; CREATE TABLE harbor.schema_migrations(version INTEGER PRIMARY KEY,checksum TEXT NOT NULL,applied BIGINT NOT NULL)');
  for(const file of readdirSync(new URL('../migrations/',import.meta.url)).filter(file=>/^\d{3}-.*\.sql$/.test(file)&&Number(file.slice(0,3))<=13).sort()){
    const sql=readFileSync(new URL('../migrations/'+file,import.meta.url),'utf8');await store.query(sql);await store.run('INSERT INTO harbor.schema_migrations VALUES($1,$2,$3)',Number(file.slice(0,3)),createHash('sha256').update(sql).digest('hex'),Date.now());
  }
  const revision=await store.initializePricing(),{member}=await legacyMember(t,store),request=randomUUID(),recording=new RecordingStore(store,{encryptionKey:randomBytes(32)});
  await store.run("INSERT INTO harbor.requests(id,member_id,key_id,started,status,ip,agent,model,price_version,tracking_only,predicted,source_channel,recording_state) VALUES($1,$2,$3,$4,'queued','fixture','fixture','gpt-6.1-sol',$5,1,0,'responses','disabled')",request,member.memberId,member.keyId,Date.now(),revision);
  await recording.start(request,{turnState:'known'});await recording.append(request,[{sourceKey:'synthetic-upgrade',kind:'user',provenance:'client',content:{text:'合成升级文本'}}]);await recording.finish(request);
  const events=await store.all('SELECT * FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence',request),source=await store.get('SELECT * FROM harbor.recordings WHERE request_id=$1',request),key=await store.get('SELECT digest,secret_cipher,enabled FROM harbor.keys WHERE id=$1',member.keyId);
  assert.deepEqual(await store.migrate(),{version:22,changed:true});assert.equal(await store.verifySchema(),true);
  assert.deepEqual(await store.all('SELECT * FROM harbor.conversation_events WHERE request_id=$1 ORDER BY sequence',request),events);assert.deepEqual(await store.get('SELECT * FROM harbor.recordings WHERE request_id=$1',request),source);assert.deepEqual(await store.get('SELECT digest,secret_cipher,enabled FROM harbor.keys WHERE id=$1',member.keyId),key);
  assert.equal((await store.get('SELECT device_mode FROM harbor.members WHERE id=$1',member.memberId)).device_mode,'compatible');assert.equal((await store.get('SELECT device_auth_state FROM harbor.requests WHERE id=$1',request)).device_auth_state,'historical_unknown');assert.equal((await store.get('SELECT revision FROM harbor.recording_work WHERE request_id=$1',request)).revision,1);assert.equal((await store.authenticate(member.secret)).id,member.memberId);assert.deepEqual(await store.migrate(),{version:22,changed:false});
});

test('query and recovery migrations upgrade the original schema without rewriting request facts or historical SQL',integration,async t=>{
  const {store}=await fixture(t),sql=readFileSync(new URL('../migrations/001-postgres.sql',import.meta.url),'utf8');
  await store.query(sql);await store.query('CREATE TABLE harbor.schema_migrations(version INTEGER PRIMARY KEY,checksum TEXT NOT NULL,applied BIGINT NOT NULL)');
  await store.run('INSERT INTO harbor.schema_migrations VALUES(1,$1,$2)',createHash('sha256').update(sql).digest('hex'),Date.now());
  await store.initializePricing();const {member}=await legacyMember(t,store),request=randomUUID(),at=Date.now(),revision=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value).active;
  await store.run("INSERT INTO harbor.requests(id,member_id,key_id,started,ended,status,ip,model,input,output,cached,price_version) VALUES($1,$2,$3,$5,$6,'ok','fixture','gpt-6.1-sol',123,4,10,$4)",request,member.memberId,member.keyId,revision,at-1000,at);await store.projectPrice(await store.get('SELECT * FROM harbor.requests WHERE id=$1',request),at);
  const before=await store.get('SELECT id,status,input,output,cached,price_version FROM harbor.requests WHERE id=$1',request);
  assert.deepEqual(await store.migrate(),{version:22,changed:true});assert.deepEqual(await store.get('SELECT id,status,input,output,cached,price_version FROM harbor.requests WHERE id=$1',request),before);
  const reader=createPostgresRequestQueries(store);try{const result=await reader.read(requestsQuery(new URLSearchParams({view:'list'})));assert.equal(result.rows[0].id,request);assert.equal(result.rows[0].standard.state,'priced');}finally{await reader.close();}
  assert.deepEqual(await store.migrate(),{version:22,changed:false});
});

test('SQL shared limits match the original exact rolling-window, alias and unresolved-usage rules',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();
  const {member,legacy}=await legacyMember(t,store),at=Date.now(),sol='gpt-6.1-sol',astra='gpt-6-astra';
  const limits=createPostgresMemberLimits(store,{clock:()=>at}),original=createMemberLimits(legacy,{clock:()=>at});
  const rules=[{model:sol,hours:24,limit_usd:'2.00'},{model:sol,hours:168,limit_usd:'10.00'},{model:astra,hours:24,limit_usd:'20.00'}];
  const key2=legacy.createKey(member.memberId),other=legacy.createMember('合成其他员工',null,null);
  for(const [table,id] of [['keys',key2.keyId],['members',other.memberId],['keys',other.keyId]]){
    const row=legacy.get('SELECT * FROM '+table+' WHERE id=?',id),names=Object.keys(row);
    await store.run('INSERT INTO harbor.'+table+'('+names.join(',')+') VALUES('+names.map((_,i)=>'$'+(i+1)).join(',')+')',...names.map(k=>row[k]));
  }
  legacy.pricing.configure({expectedVersion:0,aliases:[{name:'fixture-sol-alias',target:sol,from:at-100000,to:null,source:'synthetic mapping'}]});
  const active=legacy.pricing.current(),pricing=legacy.get('SELECT * FROM pricing_versions WHERE id=?',active);
  await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3) ON CONFLICT DO NOTHING',active,pricing.created,pricing.book);
  await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({active}));
  await limits.save(member.memberId,{enabled:true,rules,expected_version:0});original.save(member.memberId,{enabled:true,rules,expected_version:0});
  const columns=new Set((await store.all("SELECT column_name FROM information_schema.columns WHERE table_schema='harbor' AND table_name='requests'")).map(r=>r.column_name));
  const sync=async id=>{
    const row=legacy.get('SELECT * FROM requests WHERE id=?',id),names=Object.keys(row).filter(k=>columns.has(k));
    await store.run('DELETE FROM harbor.requests WHERE id=$1',id);
    await store.run('INSERT INTO harbor.requests('+names.join(',')+') VALUES('+names.map((_,i)=>'$'+(i+1)).join(',')+')',...names.map(k=>row[k]));
    const version=legacy.get('SELECT * FROM pricing_versions WHERE id=?',row.price_version);
    if(version)await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3) ON CONFLICT DO NOTHING',version.id,version.created,version.book);
    await store.projectPrice(row,at);return id;
  };
  const seed=async(model,ended,changes={})=>{
    const id=legacy.observe(legacy.authenticate(member.secret),'synthetic','fixture',model);
    if(ended!==null)legacy.finish(id,{input:1000000,output:10,cached:500000,usageKnown:true});
    const update={started:at-1000,ended,...changes},names=Object.keys(update);
    legacy.run('UPDATE requests SET '+names.map(k=>k+'=?').join(',')+' WHERE id=?',...names.map(k=>update[k]),id);return sync(id);
  };
  const equal=async()=>assert.deepEqual(await limits.status(member.memberId,at),original.status(member.memberId,at));
  await equal();
  await seed(sol,at);await seed(sol,at-24*3600000);await seed(sol,at-168*3600000);await seed(sol,at+1);
  await seed(sol,null);await seed(astra,at);await equal();
  await seed('fixture-sol-alias',at,{key_id:key2.keyId});await equal();
  const beforeOther=await limits.status(member.memberId,at);
  await seed(sol,at,{member_id:other.memberId,key_id:other.keyId});
  assert.deepEqual(await limits.status(member.memberId,at),beforeOther);await equal();
  await assert.rejects(limits.save(member.memberId,{rules:[{model:'fixture-sol-alias',hours:24,limit_usd:'1'}],expected_version:1}),{status:400});
  const unknown=await seed(sol,at,{usage_unknown:1});await equal();
  legacy.run("UPDATE requests SET manual_cost_atoms='9999999999' WHERE id=?",unknown);await sync(unknown);await equal();
  const unassigned=await seed('unmapped-fixture',at,{manual_cost_atoms:'500000000000',status:'reconciled'});await equal();
  assert.ok((await limits.status(member.memberId,at)).usage.every(r=>r.unresolved===1));
  legacy.run("UPDATE requests SET manual_cost_atoms='0' WHERE id=?",unassigned);await sync(unassigned);await equal();
  await seed('unknown-zero',at,{input:0,output:0,cached:0,price_version:'missing-fixture'});await equal();
  await seed(sol,at,{price_version:'missing-fixture'});await equal();
  const outcome=async(fn)=>{try{await fn();return null;}catch(e){return e.code;}};
  for(const [model,tools] of [[sol,undefined],['unknown-model',undefined],[sol,[{type:'namespace',tools:[{type:'image_generation'}]}]],[astra,[{type:'function',model:'unknown-tool'}]]]){
    assert.equal(await outcome(()=>limits.check(member.memberId,model,{tools,at})),await outcome(()=>original.check(member.memberId,model,{tools})));
  }
  await limits.save(member.memberId,{enabled:false,expected_version:1});original.save(member.memberId,{enabled:false,expected_version:1});await equal();
  await limits.check(member.memberId,'unknown-model');
  await assert.rejects(limits.save(member.memberId,{enabled:true,expected_version:1}),{code:'member_limits_conflict'});
  const run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('injected audit outage');return run(sql,...args);};
  try{await assert.rejects(limits.save(member.memberId,{enabled:true,expected_version:2}),/audit outage/);}finally{store.run=run;}
  assert.equal((await limits.configuration(member.memberId)).version,2);assert.equal((await limits.configuration(member.memberId)).enabled,false);
});

test('SQL limits admit concurrent requests before settlement and refuse later calls without reserving future USD',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store);
  const at=Date.now(),limits=createPostgresMemberLimits(store,{clock:()=>at}),person=await store.authenticate(member.secret),sol='gpt-6.1-sol';
  await limits.save(member.memberId,{enabled:true,rules:[{model:sol,hours:24,limit_usd:'2.00'}],expected_version:0});
  const accepted=await Promise.all([1,2].map(()=>store.transaction(async()=>{
    await limits.check(member.memberId,sol,{at});return store.observe(person,'fixture','fixture',sol,{at:at-1});
  })));
  assert.equal((await limits.status(member.memberId,at)).usage[0].in_flight,2);
  const finished=await Promise.all(accepted.map(id=>store.finish(id,{input:1000000,usageKnown:true},at)));
  assert.ok(finished.every(r=>r.settled));const row=(await limits.status(member.memberId,at)).usage[0];assert.equal(row.used_usd,'4');assert.equal(row.in_flight,0);
  await assert.rejects(limits.check(member.memberId,sol,{at}),{status:429,code:'member_model_limit_exceeded'});
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests WHERE member_id=$1',member.memberId)).n,2);
  await limits.check(member.memberId,sol,{at:at+24*3600000});
});

test('PostgreSQL new-session routing shares the original multi-account weighted allocation and persistent credits',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member,legacy}=await legacyMember(t,store),person=await store.authenticate(member.secret),at=Date.now();
  for(const [aid,remaining,hours,api] of [['a',30,24,false],['b',90,160,false],['api',0,0,true]]){
    const quota=JSON.stringify({plan:'synthetic-plan',checked:at,remaining,windows:[{used:100-remaining,minutes:10080,reset:at+hours*3600000}],limitReached:false});
    legacy.run("INSERT INTO accounts(id,name,home,state,auth_mode,api_weight,quota_json) VALUES(?,?,?,'ready',?,?,?)",aid,aid,aid,api?'openai_api':'subscription',api?20:0,quota);
    const row=legacy.get('SELECT * FROM accounts WHERE id=?',aid),names=Object.keys(row);
    await store.run('INSERT INTO harbor.accounts('+names.join(',')+') VALUES('+names.map((_,i)=>'$'+(i+1)).join(',')+')',...names.map(k=>row[k]));
  }
  const original=createBalancer({store:legacy,preparing:new Set(),clock:()=>at,refreshQuota:async()=>null});original.saveSettings({weighted:true});
  for(const row of legacy.all('SELECT * FROM gateway_settings'))await store.run('INSERT INTO harbor.gateway_settings VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',row.name,row.value);
  const router=createPostgresRouter(store,{clock:()=>at}),route=client=>store.transaction(()=>router.resolve(person,{prompt_cache_key:client}));
  const assigned=[];
  for(let i=0;i<40;i++){const expected=original.selectAccount().id,result=await route('synthetic-'+i);assert.equal(result.account.id,expected);assigned.push(result);}
  for(const name of ['routing_schedule','routing_auto'])assert.deepEqual(JSON.parse((await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',name)).value),JSON.parse(legacy.get('SELECT value FROM gateway_settings WHERE name=?',name).value));
  const credits=(await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_schedule'")).value;
  const repeat=await route('synthetic-0');assert.equal(repeat.session.id,assigned[0].session.id);assert.equal((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_schedule'")).value,credits);
  const restarted=createPostgresRouter(store,{clock:()=>at});assert.equal((await store.transaction(()=>restarted.resolve(person,{prompt_cache_key:'after-restart'}))).account.id,original.selectAccount().id);
  await assert.rejects(router.resolve(person,{}),{code:'database_transaction_required'});
  await assert.rejects(store.transaction(()=>router.resolve(person,{prompt_cache_key:'other-client'},{'x-harbor-session':repeat.session.id})),{code:'session_route_conflict'});
  await store.run('UPDATE harbor.sessions SET enabled=0 WHERE id=$1',repeat.session.id);await assert.rejects(route('synthetic-0'),{code:'session_disabled'});
});

test('PostgreSQL routing serializes same-client creation, protects in-flight moves and honors manual positive exhausted-account weights without extra authorization',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret);let at=Date.now();
  await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);
  for(const aid of ['a','b'])await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) VALUES($1,$1,$1,'ready',$2)",aid,JSON.stringify({plan:'synthetic-plan',checked:at,remaining:50,windows:[{used:50,minutes:10080,reset:at+24*3600000}],limitReached:false}));
  const put=(name,value)=>store.run('INSERT INTO harbor.gateway_settings VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',name,JSON.stringify(value));
  await put('routing_strategy',{weighted:true});await put('routing_weights',{mode:'manual',weights:{a:100,b:0},expires:nextWeightReset(at),version:1});
  const router=createPostgresRouter(store,{clock:()=>at});
  const admit=client=>store.transaction(async()=>{const result=await router.resolve(person,{prompt_cache_key:client,model:'gpt-6.1-sol'});const request=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at});await store.run('UPDATE harbor.requests SET account_id=$1,session_id=$2,route_version=$3 WHERE id=$4',result.account.id,result.session.id,result.session.route_version,request);return {...result,request};});
  const concurrent=await Promise.all(Array.from({length:8},()=>admit('same-client')));assert.equal(new Set(concurrent.map(r=>r.session.id)).size,1);assert.ok(concurrent.every(r=>r.account.id==='a'));
  const stale=concurrent[0].account;assert.equal(await router.block(stale,'quota_exhausted'),true);assert.equal(await router.block(stale,'login_expired'),false);
  const permitted=await admit('manual-extra-quota');assert.equal(permitted.account.id,'a');await store.finish(permitted.request,{input:1,usageKnown:true},at+1);
  const reused=await admit('same-client');assert.equal(reused.account.id,'a');assert.equal(reused.session.route_version,0);await store.finish(reused.request,{input:1,usageKnown:true},at+1);
  assert.equal((await store.get('SELECT enabled FROM harbor.extra_usage_policy')).enabled,false);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.account_extra_holds')).n,0);
  await put('routing_weights',{mode:'manual',weights:{a:0,b:100},expires:nextWeightReset(at),version:2});
  await assert.rejects(admit('same-client'),{code:'session_busy'});
  await Promise.all(concurrent.map(r=>store.finish(r.request,{input:1,usageKnown:true},at+1)));
  const moved=await admit('same-client');assert.equal(moved.account.id,'b');assert.equal(moved.session.route_version,1);assert.notEqual(moved.session.upstream_thread,concurrent[0].session.upstream_thread);
  await store.finish(moved.request,{input:2,usageKnown:true},at+2);assert.equal((await store.get('SELECT state FROM harbor.sessions WHERE id=$1',moved.session.id)).state,'active');
  // Old in-flight settlements cannot overwrite a newer binding's lifecycle state.
  const late=await admit('late-client');await store.run("UPDATE harbor.sessions SET account_id='a',route_version=route_version+1,state='new' WHERE id=$1",late.session.id);await store.finish(late.request,{input:3,usageKnown:true},at+3);assert.equal((await store.get('SELECT state FROM harbor.sessions WHERE id=$1',late.session.id)).state,'new');
  at=nextWeightReset(at);const expired=await admit('after-02');assert.equal(expired.account.id,'b');
  const currentB=await store.get("SELECT * FROM harbor.accounts WHERE id='b'");assert.equal(await router.block(currentB,'login_expired'),true);
  await assert.rejects(admit('same-client'),{code:'session_account_blocked'});
  await put('balancing',{failover:true,nightly:false,quietMinutes:30});
  await put('routing_weights',{mode:'manual',weights:{a:100,b:0},expires:nextWeightReset(at),version:3});
  const recovered=await admit('same-client');assert.equal(recovered.account.id,'a');assert.equal(recovered.session.route_version,2);
  await store.run("UPDATE harbor.accounts SET enabled=0 WHERE id='a'");await assert.rejects(admit('disabled-account'),{code:'no_account'});
});

test('admission waits for the member before retaining a Key lock, allowing concurrent settlement to complete',integration,async t=>{
  const {store,name}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store);
  const writer=new PostgresStore({...options(name),poolSize:1});let admission;
  try{
    let ready;const started=new Promise(resolve=>{ready=resolve;});
    await writer.transaction(async()=>{
      await writer.get('SELECT id FROM harbor.members WHERE id=$1 FOR UPDATE',member.memberId);
      admission=store.transaction(async()=>{const {pid}=await store.get('SELECT pg_backend_pid() pid');ready(pid);return store.authenticate(member.secret,{lock:true});});
      // Observe an actual lock wait rather than assuming a sleep established it.
      const pid=await started,deadline=Date.now()+5000;
      while(!(await writer.get('SELECT cardinality(pg_blocking_pids($1)) n',pid)).n){assert.ok(Date.now()<deadline,'admission did not wait on member');await new Promise(resolve=>setTimeout(resolve,10));}
      await writer.run('UPDATE harbor.keys SET used=used+1 WHERE id=$1',member.keyId);
      await writer.run('UPDATE harbor.members SET used=used+1 WHERE id=$1',member.memberId);
    });
    assert.equal((await admission).id,member.memberId);
  }finally{if(admission)await admission.catch(()=>{});await writer.close();}
});

test('PostgreSQL request queries match original exact summaries and keep deletion-safe cursor pages across restart and expiry',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member,legacy}=await legacyMember(t,store);let at=Date.now();
  const query=values=>requestsQuery(new URLSearchParams(values)),reader=createPostgresRequestQueries(store,{clock:()=>at,snapshotTtlMs:60000});t.after(()=>reader.close());
  const seed=async(i,changes={})=>{
    const id=legacy.observe(legacy.authenticate(member.secret),'fixture','fixture',i%2?'gpt-6.1-sol':'gpt-6-astra');legacy.finish(id,{input:100+i,output:20,cached:50,usageKnown:true});
    const values={started:at-1000,ended:at,...changes},names=Object.keys(values);legacy.run('UPDATE requests SET '+names.map(n=>n+'=?').join(',')+' WHERE id=?',...names.map(n=>values[n]),id);
    const row=legacy.get('SELECT * FROM requests WHERE id=?',id),columns=Object.keys(row),version=legacy.get('SELECT * FROM pricing_versions WHERE id=?',row.price_version);
    if(version)await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3) ON CONFLICT DO NOTHING',version.id,version.created,version.book);
    await store.run('INSERT INTO harbor.requests('+columns.join(',')+') VALUES('+columns.map((_,i)=>'$'+(i+1)).join(',')+')',...columns.map(c=>row[c]));await store.projectPrice(row,at);return id;
  };
  for(let i=0;i<12;i++)await seed(i);
  await seed(13,{usage_unknown:1,status:'unknown'});await seed(14,{manual_cost_atoms:'12345678901234567890',status:'reconciled'});await seed(15,{model:'unmapped-fixture'});await seed(16,{ended:null,status:'running'});await seed(18,{price_version:'missing-fixture'});
  const old=requestsReader(legacy,query({view:'summary'}),at),summary=await reader.read(query({view:'summary'}));
  for(const key of ['summary','byModel','byMember','models','members'])assert.deepEqual(summary[key],old[key]);
  const filters=query({view:'all',member:member.memberId,model:'gpt-6.1-sol',status:'ok',page_size:'3'}),filtered=await reader.read(filters),originalFiltered=requestsReader(legacy,filters,at);
  assert.deepEqual(filtered.summary,originalFiltered.summary);assert.deepEqual(filtered.rows.map(r=>({id:r.id,standard:r.standard})),originalFiltered.rows.map(r=>({id:r.id,standard:r.standard})));
  const empty=await reader.read(query({view:'all',model:"missing-model' OR 1=1 --"}));assert.equal(empty.rows.length,0);assert.equal(empty.summary.requests,0);assert.equal(empty.summary.standard.total_exact,'0');
  const known=await store.get('SELECT * FROM harbor.requests WHERE input=100');await store.run('DELETE FROM harbor.request_prices WHERE request_id=$1',known.id);
  const missing=await reader.read(query({view:'summary'}));assert.equal(missing.summary.standard.unpriced,summary.summary.standard.unpriced+1);assert.equal(missing.summary.standard.known,summary.summary.standard.known-1);
  await store.projectPrice(known,at);assert.deepEqual((await reader.read(query({view:'summary'}))).summary,old.summary);
  const first=await reader.read(query({view:'list',page_size:'5'}));assert.equal(first.rows.length,5);assert.equal(first.summary,undefined);assert.ok(first.pagination.nextCursor);
  const malformed=Buffer.from(JSON.stringify({snapshot:first.snapshot,started:at-1000,id:'\0'})).toString('base64url');await assert.rejects(reader.read(query({view:'list',snapshot:first.snapshot,cursor:malformed})),{code:'invalid_request_snapshot'});
  const expected=legacy.all('SELECT id FROM requests ORDER BY started DESC,id DESC').map(r=>r.id),seen=first.rows.map(r=>r.id);
  for(const id of seen)await store.run('DELETE FROM harbor.requests WHERE id=$1',id);
  const fresh=await seed(21,{started:at-1});let page=first;
  const restarted=createPostgresRequestQueries(store,{clock:()=>at});
  try{
    while(page.pagination.nextCursor){page=await restarted.read(query({view:'list',page_size:'5',page:String(page.pagination.page+1),snapshot:first.snapshot,cursor:page.pagination.nextCursor}));seen.push(...page.rows.map(r=>r.id));}
    assert.deepEqual(seen,expected);assert.ok(!seen.includes(fresh));assert.equal(new Set(seen).size,seen.length);
    assert.equal((await restarted.read(query({view:'list',page_size:'5'}))).rows[0].id,fresh);
    await assert.rejects(restarted.read(query({view:'list',snapshot:first.snapshot,model:'gpt-6.1-sol'})),{code:'invalid_request_snapshot'});
    await assert.rejects(restarted.read(query({view:'list',snapshot:'------------------------------------'})),{code:'invalid_request_snapshot'});
    at+=60000;await assert.rejects(restarted.read(query({view:'list',snapshot:first.snapshot})),{code:'invalid_request_snapshot'});
  }finally{await restarted.close();}
  await reader.close();await assert.rejects(reader.read(query({})),{code:'query_closed'});
});

test('PostgreSQL pagination excludes an earlier-sequence insertion that commits after the first page',integration,async t=>{
  const {store,name}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret),at=Date.now();
  const writer=new PostgresStore({...options(name),poolSize:1}),reader=createPostgresRequestQueries(store,{clock:()=>at}),query=values=>requestsQuery(new URLSearchParams(values));let release,late;
  try{
    let started,lateId;const ready=new Promise(resolve=>{started=resolve;}),gate=new Promise(resolve=>{release=resolve;});
    late=writer.transaction(async()=>{lateId=await writer.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-3000});started();await gate;});
    await ready;const b=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-1000}),c=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-2000});
    const first=await reader.read(query({view:'list',page_size:'1'}));assert.equal(first.rows[0].id,b);release();await late;
    const second=await reader.read(query({view:'list',page_size:'1',page:'2',snapshot:first.snapshot,cursor:first.pagination.nextCursor}));assert.equal(second.rows[0].id,c);assert.equal(second.pagination.nextCursor,null);assert.equal(second.pagination.total,2);
    const latest=await reader.read(query({view:'list',page_size:'100'}));assert.equal(latest.rows.length,3);assert.ok(latest.rows.some(row=>row.id===lateId));
  }finally{release?.();if(late)await late.catch(()=>{});await reader.close();await writer.close();}
});

test('concurrent query cache expiry does not serialize or alter request facts',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=Date.parse('2026-11-02T04:00:00Z');
  const reader=createPostgresRequestQueries(store,{clock:()=>at,maxSnapshots:8,snapshotTtlMs:60000}),query=requestsQuery(new URLSearchParams({view:'list'}));
  try{
    for(let i=0;i<8;i++)await reader.read(query);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.request_query_snapshots')).n,8);
    at+=60000;const results=await Promise.all([reader.read(query),reader.read(query)]);assert.ok(results.every(r=>r.pagination.total===0));assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.request_query_snapshots')).n,2);
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
  }finally{await reader.close();}
});

test('real PostgreSQL preserves integer amounts and nested/concurrent transaction rollback boundaries',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();
  await store.transaction(async()=>{
    await store.run("INSERT INTO harbor.gateway_settings VALUES('outer','before')");
    await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.gateway_settings SET value='inner' WHERE name='outer'");await store.run("INSERT INTO harbor.gateway_settings VALUES('outer','duplicate')");}),{code:'23505'});
    assert.equal((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='outer'")).value,'before');
  });
  await assert.rejects(store.transaction(async()=>{await store.run("UPDATE harbor.gateway_settings SET value='rollback' WHERE name='outer'");throw Error('fixture rollback');}),/fixture rollback/);
  assert.equal((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='outer'")).value,'before');
  const exact='123456789012345678901234567890123456789';assert.equal((await store.get('SELECT $1::numeric+$1::numeric total',exact)).total,(BigInt(exact)*2n).toString());
  await assert.rejects(store.get("SELECT '9007199254740992'::bigint n"),{code:'database_value_invalid'});
  await Promise.all(['one','two'].map(name=>store.transaction(async()=>{await store.run('INSERT INTO harbor.gateway_settings VALUES($1,$2)',name,name);assert.equal((await store.get('SELECT value FROM harbor.gateway_settings WHERE name=$1',name)).value,name);})));
  assert.equal(store.status().waiting,0);
});

test('original SQLite employee Key authenticates unchanged in PostgreSQL and retains enabled/expiry/deletion boundaries',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();const {member}=await legacyMember(t,store);
  const authenticated=await store.authenticate(member.secret);assert.equal(authenticated.id,member.memberId);assert.equal(authenticated.key_id,member.keyId);assert.deepEqual(JSON.parse(authenticated.allowed_models),['gpt-6.1-sol']);assert.equal(authenticated.secret_cipher,undefined);
  assert.ok(!(await store.get('SELECT secret_cipher FROM harbor.keys WHERE id=$1',member.keyId)).secret_cipher.includes(member.secret));
  await assert.rejects(store.authenticate('wrong-fixture-secret'),{status:401});
  await store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',member.keyId);await assert.rejects(store.authenticate(member.secret),{code:'key_disabled'});
  await store.run('UPDATE harbor.keys SET enabled=1,expires=$1 WHERE id=$2',Date.now()-1,member.keyId);await assert.rejects(store.authenticate(member.secret),{code:'key_expired'});
  await store.run('UPDATE harbor.keys SET expires=NULL,removed_at=$1 WHERE id=$2',Date.now(),member.keyId);await assert.rejects(store.authenticate(member.secret),{status:401});
});

test('request admission locks the current Key/member policy until commit and later requests see revocation',integration,async t=>{
  const {store,name}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),writer=new PostgresStore({...options(name),poolSize:1});
  try{await assert.rejects(store.authenticate(member.secret,{lock:true}),{code:'database_transaction_required'});
  const pid=(await writer.get('SELECT pg_backend_pid() pid')).pid;let update;
  const request=await store.transaction(async()=>{
    const current=await store.authenticate(member.secret,{lock:true});await store.checkModel(current,'gpt-6.1-sol');
    update=writer.run('UPDATE harbor.keys SET allowed_models=$1,model_policy_version=model_policy_version+1 WHERE id=$2',JSON.stringify(['different-model']),member.keyId);update.catch(()=>{});
    const deadline=Date.now()+5000;
    while(!(await store.get("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock') waiting",pid)).waiting){assert.ok(Date.now()<deadline,'policy update did not wait for admission');await new Promise(resolve=>setTimeout(resolve,10));}
    return store.observe(current,'fixture','fixture','gpt-6.1-sol');
  });
  await update;const current=await store.authenticate(member.secret);await assert.rejects(store.checkModel(current,'gpt-6.1-sol'),{code:'model_not_allowed'});
  assert.equal((await store.get('SELECT model FROM harbor.requests WHERE id=$1',request)).model,'gpt-6.1-sol');assert.equal((await store.finish(request,{input:1,output:1,usageKnown:true})).settled,true);
  }finally{await writer.close();}
});

test('real Responses transport settles exactly once into PostgreSQL and stale projection repair cannot undo it',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret);
  const request=await store.observe(person,'fixture','fixture','gpt-6.1-sol'),stale=await store.get('SELECT * FROM harbor.requests WHERE id=$1',request);
  const pending=await store.referenceSummary('key_id',member.keyId);assert.equal(pending.pending,1);assert.equal(pending.total,null);
  const events=[];const transport=createResponsesTransport({version:async()=>'0.160.1',credentials:async()=>({token:'fixture-upstream',accountId:'fixture-account'}),fetchImpl:async()=>new Response([
    {type:'response.output_text.delta',item_id:'fixture-item',delta:'合成回复'},
    {type:'response.completed',response:{id:'fixture-response',model:'gpt-6.1-sol',output:[{type:'message',content:[{type:'output_text',text:'合成回复'}]}],usage:{input_tokens:1000000,output_tokens:10000,input_tokens_details:{cached_tokens:800000}}}}
  ].map(e=>'data: '+JSON.stringify(e)+'\n\n').join(''),{headers:{'Content-Type':'text/event-stream'}})});
  const result=await transport.run({binary:'fixture',home:'fixture',payload:{model:'gpt-6.1-sol'},onEvent:async e=>events.push(e)});assert.equal(events[0].delta,'合成回复');
  const settled=await Promise.all([store.finish(request,result),store.finish(request,result)]);assert.equal(settled.filter(r=>r.settled).length,1);
  const summary=await store.referenceSummary('key_id',member.keyId);assert.equal(summary.total_exact,'0.58');assert.equal(summary.cached_exact,'0.08');assert.equal(summary.requests,1);assert.equal(summary.known,1);
  assert.equal((await store.get('SELECT used FROM harbor.keys WHERE id=$1',member.keyId)).used,1010000);
  await store.projectPrice(stale);assert.equal((await store.referenceSummary('key_id',member.keyId)).total_exact,'0.58');
  const missing=await store.observe(person,'fixture','fixture','gpt-6.1-sol');await store.finish(missing,{usageKnown:false});const partial=await store.referenceSummary('member_id',member.memberId);assert.equal(partial.unmetered,1);assert.equal(partial.total_exact,'0.58');assert.equal(partial.state,'partial');
});

test('PostgreSQL keeps valid dispatch prices immutable and backfills only previously missing rates on completion',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret);
  const known=await store.observe(person,'fixture','fixture','gpt-6.1-sol'),future=await store.observe(person,'fixture','fixture','fixture-future-model');
  const old=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value).active;
  const book=await store.pricingBook(old);book.rates['gpt-6.1-sol']={input:'4',cached:'0.1',output:'10'};book.rates['fixture-future-model']={input:'6',cached:'0.3',output:'30'};
  await store.run("INSERT INTO harbor.pricing_versions VALUES('fixture-new-version',$1,$2)",Date.now(),JSON.stringify(book));await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({active:'fixture-new-version'}));
  assert.equal((await store.finish(known,{input:1000000})).standard.total_exact,'2');assert.equal((await store.finish(future,{input:1000000})).standard.total_exact,'6');
  assert.equal((await store.get('SELECT price_version FROM harbor.requests WHERE id=$1',known)).price_version,old);assert.equal((await store.get('SELECT price_version FROM harbor.requests WHERE id=$1',future)).price_version,'fixture-new-version');
  assert.equal(await store.initializePricing(),'fixture-new-version','startup must not overwrite an existing active book');
});

test('PostgreSQL price projection failure rolls back settlement and all usage counters',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret),request=await store.observe(person,'fixture','fixture','gpt-6.1-sol');
  await store.query(`CREATE FUNCTION harbor.reject_price_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture projection failure'; END $$;
    CREATE TRIGGER reject_price_fixture BEFORE UPDATE ON harbor.request_prices FOR EACH ROW WHEN (NEW.state='priced') EXECUTE FUNCTION harbor.reject_price_fixture()`);
  await assert.rejects(store.finish(request,{input:1000000}),/fixture projection failure/);
  assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',request)).status,'queued');assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member.memberId)).used,0);assert.equal((await store.get('SELECT used FROM harbor.keys WHERE id=$1',member.keyId)).used,0);
  await store.query('DROP TRIGGER reject_price_fixture ON harbor.request_prices');await store.finish(request,{input:1000000});assert.equal((await store.referenceSummary('member_id',member.memberId)).total_exact,'2');
});

test('R5 sibling savepoints and grandchildren serialize without rollback interference or reentrant deadlock',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();
  await store.transaction(async()=>{
    const result=await Promise.allSettled([
      store.transaction(async()=>{await store.run("INSERT INTO harbor.gateway_settings VALUES('rolled-back','fixture')");throw Error('fixture child failure');}),
      store.transaction(async()=>{await store.run("INSERT INTO harbor.gateway_settings VALUES('kept','fixture')");await store.transaction(async()=>store.run("INSERT INTO harbor.gateway_settings VALUES('grandchild','fixture')"));})
    ]);
    assert.equal(result[0].status,'rejected');assert.equal(result[1].status,'fulfilled');assert.equal(await store.get("SELECT value FROM harbor.gateway_settings WHERE name='rolled-back'"),undefined);
  });
  assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.gateway_settings WHERE name IN ('kept','grandchild')")).n,2);assert.equal(store.status().waiting,0);
});

test('R5 parallel settlements within a parent commit once each and partial child failure remains recoverable',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),person=await store.authenticate(member.secret);
  const one=await store.observe(person,'fixture','fixture','gpt-6.1-sol'),two=await store.observe(person,'fixture','fixture','gpt-6.1-sol');
  await store.query(`CREATE FUNCTION harbor.reject_one_price() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture child projection failure'; END $$;
    CREATE TRIGGER reject_one_price BEFORE UPDATE ON harbor.request_prices FOR EACH ROW WHEN (NEW.total_atoms=2000000000000) EXECUTE FUNCTION harbor.reject_one_price()`);
  const result=await store.transaction(async()=>Promise.allSettled([store.finish(one,{input:1000000}),store.finish(two,{input:2000000})]));assert.equal(result[0].status,'rejected');assert.equal(result[1].status,'fulfilled');
  assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',one)).status,'queued');assert.equal((await store.referenceSummary('key_id',member.keyId)).total_exact,'4');assert.equal((await store.get('SELECT used FROM harbor.keys WHERE id=$1',member.keyId)).used,2000000);
  await store.query('DROP TRIGGER reject_one_price ON harbor.request_prices');
  const settled=await store.transaction(async()=>Promise.all([store.finish(one,{input:1000000}),store.finish(two,{input:2000000})]));assert.equal(settled.filter(r=>r.settled).length,1);assert.equal((await store.referenceSummary('key_id',member.keyId)).total_exact,'6');assert.equal((await store.get('SELECT used FROM harbor.keys WHERE id=$1',member.keyId)).used,3000000);
});

test('R5 parent failure drains pending children before rollback and closed async scopes cannot reuse a released client',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();
  await assert.rejects(store.transaction(async()=>Promise.all([
    store.transaction(async()=>{await store.run("INSERT INTO harbor.gateway_settings VALUES('fail-parent','fixture')");throw Error('fixture parent failure');}),
    store.transaction(async()=>store.run("INSERT INTO harbor.gateway_settings VALUES('also-rolled-back','fixture')"))
  ])),/fixture parent failure/);
  assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.gateway_settings WHERE name IN ('fail-parent','also-rolled-back')")).n,0);
  let release,late;const gate=new Promise(r=>release=r);
  await store.transaction(async()=>{late=gate.then(()=>store.get('SELECT 1 n'));});release();await assert.rejects(late,{code:'database_transaction_closed'});assert.equal((await store.get('SELECT 1 n')).n,1);assert.equal(store.status().waiting,0);
  await assert.rejects(store.transaction(async()=>{try{await store.query('SELECT missing_fixture_column');}catch{}}),{code:'database_transaction_failed'});
});

test('explicit migration connection is not redirected by an unrelated environment URL',integration,async t=>{
  const {store}=await fixture(t),saved=process.env.HARBOR_DATABASE_URL,params=new pg.Client(store.pool.options).connectionParameters;
  const connection={host:params.host,port:params.port,user:params.user,password:params.password,database:params.database,ssl:params.ssl};
  try{
    process.env.HARBOR_DATABASE_URL='postgresql://fixture-remote.invalid/unrelated_fixture';
    assert.equal((await migratePostgres({connection,allowPlaintextLocalNetwork:true})).changed,true);assert.equal(await store.ready(),true);
  }finally{if(saved===undefined)delete process.env.HARBOR_DATABASE_URL;else process.env.HARBOR_DATABASE_URL=saved;}
});


test('PostgreSQL overview batches exact seven-day and twelve-hour usage, preserves Key recovery and pages only visible sessions',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member,legacy}=await legacyMember(t,store);
  const at=Date.now(),window=legacy.pruneUsage(at);
  const copy=async(table,id)=>{const row=legacy.get('SELECT * FROM '+table+' WHERE id=?',id),columns=Object.keys(row);await store.run('INSERT INTO harbor.'+table+'('+columns.join(',')+') VALUES('+columns.map((_,i)=>'$'+(i+1)).join(',')+')',...columns.map(c=>row[c]));};
  const other=legacy.createMember('已移除的合成员工',null,null);await copy('members',other.memberId);await copy('keys',other.keyId);
  for(const id of ['fixture-a','fixture-b']){legacy.run("INSERT INTO accounts(id,name,kind,home,state,api_secret_cipher,api_secret_digest) VALUES(?,?,'codex','private-home','ready','never-return-cipher','fixture-digest')",id,id);await copy('accounts',id);}
  for(let i=0;i<4;i++){legacy.run("INSERT INTO sessions(id,member_id,account_id,created,updated,protocol,nightly_enabled,nightly_expires,title,history_digest) VALUES(?,?,?, ?,?,'responses',0,?,'never-return-title','never-return-history')",'fixture-session-'+i,member.memberId,'fixture-a',at-10000,at-i,at-1);await copy('sessions','fixture-session-'+i);}
  const seed=async(i,changes={})=>{
    const m=i===40?legacy.authenticate(other.secret):legacy.authenticate(member.secret),id=legacy.observe(m,'synthetic-ip-'+(i%2),'synthetic-agent',i%2?'gpt-6.1-sol':'gpt-6-astra');
    legacy.finish(id,{input:100+i,output:20,cached:50,usageKnown:true});
    const values={started:at-1000,ended:at-1,account_id:'fixture-a',session_id:'fixture-session-'+i%4,...changes},names=Object.keys(values);
    legacy.run('UPDATE requests SET '+names.map(n=>n+'=?').join(',')+' WHERE id=?',...names.map(n=>values[n]),id);
    const row=legacy.get('SELECT * FROM requests WHERE id=?',id),version=legacy.get('SELECT * FROM pricing_versions WHERE id=?',row.price_version);
    if(version)await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3) ON CONFLICT DO NOTHING',version.id,version.created,version.book);
    await copy('requests',id);await store.projectPrice(row,at);return id;
  };
  for(let i=0;i<10;i++)await seed(i);
  await seed(11,{ended:at-12*3600000});await seed(12,{ended:at});await seed(13,{ended:at-12*3600000-1});
  await seed(14,{usage_unknown:1,status:'unknown',error:'Bearer synthetic-private-error',phase_history:'[{"phase":"Bearer synthetic-private-phase"}]'});await seed(15,{status:'running',ended:null});await seed(16,{model:'unmapped-fixture'});
  await seed(17,{manual_cost_atoms:'12345678901234567890',status:'reconciled'});await seed(18,{price_version:'missing-fixture'});
  await seed(19,{ended:window.start});await seed(20,{ended:window.start-1});await seed(21,{ended:window.end});await seed(40);
  legacy.run('UPDATE members SET removed_at=? WHERE id=?',at,other.memberId);await store.run('UPDATE harbor.members SET removed_at=$1 WHERE id=$2',at,other.memberId);
  legacy.run('UPDATE keys SET removed_at=? WHERE id=?',at,other.keyId);await store.run('UPDATE harbor.keys SET removed_at=$1 WHERE id=$2',at,other.keyId);
  await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)','fixture-audit',at-1,'account.update','Bearer never-return-audit');
  const reader=createPostgresStateQueries(store,{clock:()=>at,encryptionKey:legacy.encryptionKey});t.after(()=>reader.close());
  const value=await reader.read({sessionPageSize:2}),old=legacy.snapshot(at,{sessionPageSize:2});
  const compact=await reader.read({view:'overview'});
  assert.equal(compact.view,'overview');assert.deepEqual(compact.totals,{requests:value.totals.requests,success:value.totals.success});
  assert.deepEqual(compact.daily,value.daily.map(({day,requests})=>({day,requests})));
  assert.deepEqual(compact.accounts,value.accounts.map(({id,name,kind,auth_mode,enabled,state})=>({id,name,kind,auth_mode,enabled,state})));
  assert.equal(compact.memberCount,value.memberCount);assert.deepEqual(compact.sessions,[]);assert.deepEqual(compact.keys,[]);assert.deepEqual(compact.requests,[]);
  assert.equal(compact.totals.standard,undefined);
  for(const params of ['view=invalid','view=overview&view=full'])assert.throws(()=>stateQuery(new URLSearchParams(params)),{status:400});
  await assert.rejects(reader.read({view:'invalid'}),{status:400});

  for(const field of ['usageMetric','usageWindow','recentUsageWindow','sessionsPagination','totals','daily'])assert.deepEqual(value[field],old[field],field);
  assert.deepEqual(value.sessions,old.sessions);
  assert.deepEqual(value.members.map(({usage_type_id,usage_type_version,usage_type_code,usage_type_name,defaults_version,configuration_source,default_models_set,default_models,...legacy})=>{assert.equal(usage_type_id,null);assert.equal(usage_type_version,0);assert.equal(defaults_version,0);assert.equal(default_models_set,0);assert.equal(default_models,null);assert.deepEqual(configuration_source,{models:{kind:'legacy'},limits:{kind:'legacy'}});return legacy;}),old.members);
  assert.deepEqual(value.keys.map(({model_source,...legacy})=>{assert.deepEqual(model_source,{kind:'legacy'});return legacy;}),old.keys);
  for(const {package:budget,package_selection,...row} of value.accounts){assert.equal(budget.mode,'legacy');assert.equal(budget.reason,'package_policy_disabled');assert.equal(budget.eligible,false);assert.equal(budget.score,null);assert.equal(package_selection.mode,'unknown');assert.equal(package_selection.revision,0);assert.deepEqual(row,old.accounts.find(a=>a.id===row.id));}
  assert.equal(value.keys[0].full_key,member.secret);assert.equal(value.totals.requests,20);assert.equal(value.members.length,1);assert.equal(value.keys.length,1);
  const serialized=JSON.stringify(value);for(const hidden of ['never-return-title','never-return-history','private-home','never-return-cipher','never-return-audit','secret_cipher','inserted_xid','q_total','synthetic-private-error','synthetic-private-phase'])assert.ok(!serialized.includes(hidden),hidden);
  assert.equal(value.requests[0].input,undefined);assert.equal(value.keys[0].digest,undefined);assert.equal(value.accounts[0].api_secret_digest,undefined);assert.equal(value.sessions[0].nightly_enabled,1);
  const second=await reader.read({sessionPage:99,sessionPageSize:2});assert.equal(second.sessionsPagination.page,2);assert.deepEqual(second.sessions.map(s=>s.id),old.sessionsPagination.total===4?['fixture-session-2','fixture-session-3']:[]);
  assert.ok(second.sessions.every(s=>s.standard.requests>0));
  const typeA=(await store.get("SELECT id FROM harbor.member_types WHERE code='A'")).id,typeB=(await store.get("SELECT id FROM harbor.member_types WHERE code='B'")).id;
  await store.run('UPDATE harbor.members SET usage_type_id=$1,usage_type_version=1,name=$2 WHERE id=$3',typeA,'筛选员工',member.memberId);
  await store.run("UPDATE harbor.sessions SET enabled=0,account_id='fixture-b',client_session_id='Client-Needle' WHERE id='fixture-session-1'");
  const filtered=await reader.read({people:'none',sessionQ:' client-needle ',sessionType:typeA,sessionAccount:'fixture-b',sessionStatus:'disabled',sessionPage:99,sessionPageSize:1});
  assert.equal(filtered.sessionsPagination.total,1);assert.equal(filtered.sessionsPagination.page,1);assert.deepEqual(filtered.sessions.map(row=>row.id),['fixture-session-1']);assert.deepEqual(filtered.totals,value.totals);
  assert.equal((await reader.read({people:'none',sessionQ:'筛选员工',sessionStatus:'enabled'})).sessionsPagination.total,3);
  assert.equal((await reader.read({people:'none',sessionQ:member.memberId})).sessionsPagination.total,4);
  for(const filters of [{sessionQ:"' OR TRUE --"},{sessionQ:'%'},{sessionType:'unclassified'},{sessionAccount:'missing-account'}])assert.equal((await reader.read({people:'none',...filters})).sessionsPagination.total,0);
  await store.run('UPDATE harbor.members SET usage_type_id=$1 WHERE id=$2',typeB,member.memberId);
  assert.equal((await reader.read({people:'none',sessionType:typeA})).sessionsPagination.total,0);assert.equal((await reader.read({people:'none',sessionType:typeB})).sessionsPagination.total,4);
  await store.run('UPDATE harbor.members SET usage_type_id=NULL,usage_type_version=0 WHERE id=$1',member.memberId);
  assert.equal((await reader.read({people:'none',sessionType:'unclassified'})).sessionsPagination.total,4);
  for(const params of ['session_q=a&session_q=b','session_type=bad%27','session_account=bad%2Fid','session_status=active','session_q='+encodeURIComponent('x'.repeat(101)),'session_q=%00'])assert.throws(()=>stateQuery(new URLSearchParams(params)),{status:400});
  assert.deepEqual(stateQuery(new URLSearchParams('session_q=%20hello%20&session_status=enabled')),{sessionPage:1,sessionPageSize:25,sessionQ:'hello',sessionStatus:'enabled'});
  await assert.rejects(reader.read({sessionQ:null}),{status:400});
  await assert.rejects(reader.read({sessionPageSize:101}),{status:400});
  for(const params of ['session_page=0','session_page=1&session_page=2','session_page_size=101','session_page=9007199254740992'])assert.throws(()=>stateQuery(new URLSearchParams(params)),{status:400});
  assert.deepEqual(stateQuery(new URLSearchParams('session_page=2&session_page_size=10')),{sessionPage:2,sessionPageSize:10});
  await store.run("UPDATE harbor.keys SET allowed_models='[]' WHERE id=$1",member.keyId);await assert.rejects(reader.read(),{code:'model_policy_invalid'});
  await store.run('UPDATE harbor.keys SET allowed_models=$1 WHERE id=$2',JSON.stringify(['gpt-6.1-sol']),member.keyId);
  await store.run("UPDATE harbor.members SET usage_limits='{}' WHERE id=$1",member.memberId);await assert.rejects(reader.read(),{code:'member_limits_invalid'});
});

test('PostgreSQL session filters use authenticated HTTP and the current employee type',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const app=await open(),origin=await listenGateway(app),at=Date.now(),cookie=await gatewayLogin(origin,password);
 await store.run("UPDATE harbor.members SET name='筛选HTTP员工',usage_type_id='usage-a',usage_type_version=1 WHERE id=$1",member.memberId);
 for(let i=0;i<3;i++)await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,created,updated,protocol,enabled,client_session_id) VALUES($1,$2,'synthetic-runtime',$3,$3,'responses',$4,$5)",'filter-http-'+i,member.memberId,at-i,i===1?0:1,'filter-client-'+i);
 const path='/api/admin/state?people=none&session_q=filter-client-1&session_type=usage-a&session_account=synthetic-runtime&session_status=disabled&session_page=9&session_page_size=1';
 assert.equal((await fetch(origin+path)).status,401);
 const response=await fetch(origin+path,{headers:{cookie}});assert.equal(response.status,200);const data=await response.json();assert.equal(data.capabilities.sessionFilters,true);assert.equal(data.sessionsPagination.total,1);assert.equal(data.sessionsPagination.page,1);assert.deepEqual(data.sessions.map(row=>row.id),['filter-http-1']);assert.deepEqual(data.keys,[]);assert.ok(!JSON.stringify(data).includes(member.secret));
 await store.run("UPDATE harbor.members SET usage_type_id='usage-b' WHERE id=$1",member.memberId);assert.equal((await (await fetch(origin+path,{headers:{cookie}})).json()).sessionsPagination.total,0);
 assert.equal((await fetch(origin+'/api/admin/state?session_status=invalid',{headers:{cookie}})).status,400);
}));

test('PostgreSQL employee and Key management uses authenticated HTTP, atomic creation and shared limits without interrupting dispatched work',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();
  const root=mkdtempSync(join(tmpdir(),'harbor-pg-admin-')),key=randomBytes(32),recordings=new RecordingStore(store,{encryptionKey:randomBytes(32)});
  const buffer=await RecordingBuffer.create(recordings,{directory:join(root,'records')}),meteringRecovery=await MeteringRecovery.create(store,{encryptionKey:key,directory:join(root,'metering')});
  const router=createPostgresRouter(store),limits=createPostgresMemberLimits(store);let dispatched=0,gate;
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic','合成账号','synthetic','ready')");
  const handler=createPostgresResponsesHandler({store,recordings,buffer,meteringRecovery,keyEncryptionKey:key,resolveRoute:router.resolve,checkLimits:(m,p,o)=>limits.check(m.id,p.model,{...o,tools:p.tools}),adminAuthenticated:async req=>req.headers.authorization==='Bearer synthetic-admin',transportForAccount:async()=>({run:async()=>{dispatched++;if(gate)await gate;return {status:'ok',input:100,output:20,cached:10,usageKnown:true,actualModel:'gpt-6.1-sol',response:{id:'synthetic-response',model:'gpt-6.1-sol',output:[]}};}})});
  const server=http.createServer((req,res)=>{void handler.handle(req,res).then(handled=>{if(!handled){res.writeHead(404);res.end();}}).catch(e=>{res.writeHead(e.status||500,{'content-type':'application/json'});res.end(JSON.stringify({error:{code:e.code}}));});});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
  t.after(async()=>{await new Promise(resolve=>server.close(resolve));await handler.closeQueries();await meteringRecovery.close();await buffer.close();rmSync(root,{recursive:true,force:true});});
  const admin=(path,method='GET',body,secret='synthetic-admin')=>fetch(origin+'/api/admin/'+path,{method,headers:{authorization:'Bearer '+secret,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});
  const send=secret=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json','thread-id':'synthetic-management-client'},body:JSON.stringify({model:'gpt-6.1-sol',input:'管理链路合成输入',stream:false})});
  const wait=async predicate=>{const end=Date.now()+5000;while(!await predicate()){assert.ok(Date.now()<end,'management state timeout');await new Promise(resolve=>setTimeout(resolve,10));}};
  const payload={name:'HTTP合成员工',quota:null,expires:null,allowed_models:['gpt-6.1-sol']};
  await assert.rejects(createPostgresMemberAdmin(store).handle('/api/admin/members','POST',payload),{code:'key_configuration'});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.members')).n,0);
  const run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic audit failure');return run(sql,...args);};
  try{const failed=await admin('members','POST',payload);assert.equal(failed.status,500);}finally{store.run=run;}
  for(const table of ['members','keys','audit'])assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.'+table)).n,0);
  let response=await admin('members','POST',payload,'invalid');assert.equal(response.status,401);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.members')).n,0);
  response=await admin('members','POST',payload);assert.equal(response.status,201);const first=await response.json();assert.equal((await store.authenticate(first.secret)).id,first.memberId);
  response=await admin('keys','POST',{memberId:first.memberId,expires:null,allowed_models:['gpt-6.1-sol']});assert.equal(response.status,201);const second=await response.json();
  response=await admin('pricing','GET',undefined,first.secret);assert.equal(response.status,401);response=await admin('pricing');const pricing=await response.json();assert.equal(response.status,200);assert.ok(pricing.rates.some(row=>row.model==='gpt-6.1-sol'));assert.equal(pricing.unpricedModels.length,0);
  response=await admin('state');let state=await response.json();assert.equal(state.members.length,1);assert.equal(state.keys.length,2);assert.ok(state.keys.some(k=>k.full_key===first.secret));assert.ok(state.keys.some(k=>k.full_key===second.secret));assert.ok(!JSON.stringify(await store.all('SELECT detail FROM harbor.audit')).includes(first.secret));
  const updates=await Promise.all([admin('keys/'+first.keyId,'PATCH',{allowed_models:['gpt-6-astra'],expected_model_policy_version:0}),admin('keys/'+first.keyId,'PATCH',{allowed_models:null,expected_model_policy_version:0})]);assert.deepEqual(updates.map(r=>r.status).sort(),[200,409]);
  response=await admin('keys/'+first.keyId,'PATCH',{allowed_models:[],expected_model_policy_version:1});assert.equal(response.status,400);assert.equal((await store.get('SELECT model_policy_version FROM harbor.keys WHERE id=$1',first.keyId)).model_policy_version,1);
  response=await admin('keys/'+first.keyId,'PATCH',{allowed_models:['gpt-6.1-sol'],expected_model_policy_version:1});assert.equal(response.status,200);
  response=await admin('keys/'+second.keyId,'PATCH',{enabled:false});assert.equal(response.status,200);
  response=await admin('members/'+first.memberId,'PATCH',{enabled:false,name:'更名后的合成员工'});assert.equal(response.status,200);
  response=await admin('members/'+first.memberId,'PATCH',{enabled:true});assert.equal(response.status,200);await assert.rejects(store.authenticate(second.secret),{code:'key_disabled'});
  response=await admin('keys/'+second.keyId,'PATCH',{enabled:true,expires:Date.now()-1000});assert.equal(response.status,200);await assert.rejects(store.authenticate(second.secret),{code:'key_expired'});
  response=await admin('keys/'+second.keyId,'PATCH',{expires:null});assert.equal(response.status,200);
  let release;gate=new Promise(resolve=>{release=resolve;});const running=send(first.secret);await wait(()=>dispatched===1);
  response=await admin('members/'+first.memberId,'DELETE');assert.equal(response.status,409);response=await admin('keys/'+first.keyId,'DELETE');assert.equal(response.status,409);
  response=await admin('members/'+first.memberId,'PATCH',{enabled:false});assert.equal(response.status,200);response=await send(second.secret);assert.equal(response.status,403);assert.equal(dispatched,1);
  release();gate=null;response=await running;assert.equal(response.status,200);await response.json();await wait(()=>handler.status().active===0);
  response=await admin('members/'+first.memberId,'PATCH',{enabled:true});assert.equal(response.status,200);
  const queued=createPostgresQueryQueue({maxConcurrent:1,maxQueued:0});let unblock;const held=queued.run(()=>new Promise(resolve=>{unblock=resolve;}));
  await assert.rejects(createPostgresMemberAdmin(store,{queryQueue:queued}).handle('/api/admin/members/'+first.memberId+'/usage-limits','PATCH',{enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'0'}],expected_version:0}),{code:'query_queue_full'});assert.equal((await store.get('SELECT usage_limits_version FROM harbor.members WHERE id=$1',first.memberId)).usage_limits_version,0);unblock();await held;await queued.close();
  response=await admin('members/'+first.memberId+'/usage-limits','PATCH',{enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'0'}],expected_version:0});assert.equal(response.status,200);const budget=await response.json();assert.ok(budget.usage[0].exceeded);assert.equal(budget.usage[0].in_flight,0);
  for(const secret of [first.secret,second.secret]){response=await send(secret);assert.equal(response.status,429);}assert.equal(dispatched,1);
  response=await admin('keys/'+first.keyId,'DELETE');assert.equal(response.status,200);await assert.rejects(store.authenticate(first.secret),{code:'invalid_api_key'});assert.equal((await store.get('SELECT digest FROM harbor.keys WHERE id=$1',first.keyId)).digest,'removed:'+first.keyId);
  response=await admin('state');state=await response.json();const total=state.totals.standard.total_exact;assert.equal(state.totals.requests,1);assert.equal(state.members[0].standard.requests,1);assert.equal(state.keys.length,1);
  response=await admin('members/'+first.memberId,'DELETE');assert.equal(response.status,200);await assert.rejects(store.authenticate(second.secret),{code:'invalid_api_key'});
  response=await admin('state');state=await response.json();assert.equal(state.members.length,0);assert.equal(state.keys.length,0);assert.equal(state.totals.requests,1);assert.equal(state.totals.standard.total_exact,total);
  response=await admin('keys','POST',{memberId:first.memberId,expires:null});assert.equal(response.status,404);
});


const gatewayWait=async(predicate,timeout=5000)=>{const end=Date.now()+timeout;while(!await predicate()){assert.ok(Date.now()<end,'gateway condition timeout');await new Promise(resolve=>setTimeout(resolve,10));}};
const controlledResponses=(before=async()=>{})=>createResponsesTransport({version:async()=>'0.160.1',credentials:async()=>({token:'synthetic-runtime-upstream',accountId:'synthetic-runtime-account'}),fetchImpl:async(_url,{signal})=>{
  if(new URL(_url).pathname.endsWith('/models'))return new Response(JSON.stringify({models:[{slug:'gpt-6.1-sol'},{slug:'gpt-6-astra'}]}),{headers:{'content-type':'application/json'}});
  await before(signal);return new Response([
    {type:'response.output_text.delta',item_id:'synthetic-message',content_index:0,delta:'服务入口合成回复'},
    {type:'response.completed',response:{id:'synthetic-runtime-response',model:'gpt-6.1-sol',output:[{id:'synthetic-message',type:'message',content:[{type:'output_text',text:'服务入口合成回复'}]}],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:10}}}}
  ].map(event=>'data: '+JSON.stringify(event)+'\n\n').join(''),{headers:{'content-type':'text/event-stream'}});
}});
async function withGateway(t,run,{migrated=true,legacy=true}={}){
  const {store,name}=await fixture(t),root=mkdtempSync(join(tmpdir(),'harbor-pg-runtime-')),data=join(root,'data'),apps=[];
  mkdirSync(data,{mode:0o700});if(migrated){await store.migrate();await store.initializePricing();}
  let member;
  if(legacy){const old=await legacyMember(t,store);member=old.member;writeFileSync(join(data,'key-encryption.key'),old.legacy.encryptionKey,{mode:0o600});}
  if(migrated)await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-runtime','合成运行账号',$1,'ready')",join(root,'runtime','accounts','synthetic-runtime'));
  const password='synthetic-runtime-password',config={port:43127,codexBinary:'codex',adminPasswordHash:passwordHash(password)};
  // This fixture controls the storage-policy boundary for existing protocol
  // assertions. Real PGDATA identity, low/unknown disks and Worker recovery are
  // exercised separately by postgres-recording-storage/integration tests.
  const recordingProbe=async()=>[{total:1000000000000,free:900000000000}];recordingProbe.status=()=>({source:'controlled_test_measurement'});
  const open=async extra=>{const app=await createPostgresGateway({database:options(name),dataDir:data,runtimeDir:join(root,'runtime'),config,env:{},resources:{corePoolSize:1,queryPoolSize:1,recordingPoolSize:1},drainTimeoutMs:500,responsesTransport:controlledResponses(),rateReader:async()=>{throw Error('synthetic quota unavailable');},...extra,recording:{enabled:true,storage:{probe:recordingProbe},...extra?.recording}});apps.push(app);return app;};
  try{await run({store,name,root,data,member,password,config,open});}finally{for(const app of apps.reverse())await app.close();rmSync(root,{recursive:true,force:true});}
}
async function deviceGatewayRequest(origin,path,{method='POST',body,agent,headers={}}={}){return new Promise((resolve,reject)=>{
  const bytes=body===undefined?undefined:Buffer.from(JSON.stringify(body)),req=https.request(new URL(path,origin),{method,agent,headers:{...headers,...(bytes?{'content-type':'application/json','content-length':bytes.length}:{})}},res=>{const parts=[];res.on('data',part=>parts.push(part));res.on('end',()=>{const raw=Buffer.concat(parts).toString();let value;try{value=JSON.parse(raw);}catch{}resolve({status:res.statusCode,value,headers:res.headers,raw});});});req.setTimeout(10000,()=>req.destroy(Error('synthetic TLS timeout')));req.on('error',reject);req.end(bytes);
});}
async function enrolledGateway(t,context,extra={}){
  const ca=await certificateFixture(context.root),app=await context.open({...extra,devices:{issuer:ca.issuer,tls:{key:ca.key,cert:ca.cert,ca:ca.ca,port:0}}}),plain=await listenGateway(app),address=await app.deviceTls.listen(),origin='https://127.0.0.1:'+address.port,anon=new https.Agent({ca:ca.ca}),cookie=await gatewayLogin(plain,context.password),keys=await clientKey(context.root,'shared-device-client');let client;
  t.after(()=>{anon.destroy();client?.agent.destroy();ca.issuer.close();});
  const admin=async(path,method='GET',body)=>{const response=await fetch(plain+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:response.status,value:await response.json()};};
  const invitation=await admin('device-invitations','POST',{member_id:context.member.memberId});assert.equal(invitation.status,201);
  const challenge=await deviceGatewayRequest(origin,'/api/device/enroll/challenge',{agent:anon,body:{invitation:invitation.value.token}});assert.equal(challenge.status,200);
  const payload={invitation:invitation.value.token,challenge:challenge.value.challenge,nonce:challenge.value.nonce,scope:challenge.value.scope,csr:keys.csr,name:'共享入口合成设备',system:'合成Windows与WSL',fingerprint:deviceHardwareDigest(invitation.value.scope,'12345678-1234-1234-1234-123456789abc'),installation_fingerprint:null,fingerprint_source:'smbios_uuid'};payload.signature=sign('sha256',Buffer.from(deviceEnrollmentMessage(payload)),keys.key).toString('base64');
  const registration=await deviceGatewayRequest(origin,'/api/device/enroll/register',{agent:anon,body:payload});assert.equal(registration.status,201);
  assert.equal((await admin('devices/'+registration.value.device_id+'/approve','POST',{expected_version:0,credentials_only:false})).status,200);
  const delivered=await deviceGatewayRequest(origin,'/api/device/enroll/status',{agent:anon,body:{enrollment_token:registration.value.enrollment_token}});assert.equal(delivered.status,200);
  client={agent:new https.Agent({ca:ca.ca,key:keys.key,cert:delivered.value.certificate,keepAlive:true,maxSockets:4}),headers:{authorization:'Bearer '+context.member.secret,'x-harbor-hardware':payload.fingerprint}};
  return {app,plain,origin,anon,client,admin,deviceId:registration.value.device_id,send:(path,body,options={})=>deviceGatewayRequest(origin,path,{...client,body,...options})};
}

test('shared PostgreSQL gateway enforces trusted devices across every inference and self endpoint and snapshots accepted proof',integration,async t=>withGateway(t,async context=>{
  const {store,member}=context;await prepareHttpChat(store,member);
  const transport={...controlledResponses(),runJson:async({beforeDispatch,onDispatch})=>{await beforeDispatch();onDispatch();return {response:{data:[]},input:10,output:2,cached:0,usageKnown:true};}},env=await enrolledGateway(t,context,{responsesTransport:transport,executor:async()=>({text:'合成CLI输出',threadId:'synthetic-device-cli',input:10,output:2,cached:0,usageKnown:true}),platformTransport:{models:async()=>['synthetic-model'],run:async()=>platformResult}}),{app,plain,origin,anon,client,admin,deviceId,send}=env;
  const question={model:'gpt-6.1-sol',input:'共享设备合成问题',stream:false};let response=await send('/v1/responses',question);assert.equal(response.status,200);
  const first=await store.get('SELECT * FROM harbor.requests WHERE id=$1',response.headers['x-harbor-request']);assert.equal(first.device_id,deviceId);assert.equal(first.device_name,'共享入口合成设备');assert.equal(first.device_auth_state,'authenticated');assert.equal(first.device_generation,1);assert.ok((await store.get('SELECT verified_request_at FROM harbor.employee_devices WHERE id=$1',deviceId)).verified_request_at);
  assert.equal((await admin('members/'+member.memberId+'/device-policy','PATCH',{expected_version:0,mode:'enforced'})).status,200);
  for(const [path,method] of [['/v1/responses','POST'],['/v1/images/generations','POST'],['/v1/images/edits','POST'],['/v1/chat/completions','POST'],['/v1/platform/chat/completions','POST'],['/v1/platform/models','GET'],['/api/me','GET']]){
    const denied=await deviceGatewayRequest(origin,path,{agent:anon,method,headers:{authorization:'Bearer '+member.secret},...(method==='POST'?{body:{}}:{})});assert.equal(denied.status,403,path);assert.equal(denied.value.error.code,'device_required',path);
  }
  const forged=await fetch(plain+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','x-harbor-device-id':deviceId,'ssl-client-verify':'SUCCESS','x-forwarded-client-cert':'synthetic-forgery'},body:JSON.stringify(question)});assert.equal(forged.status,403);
  assert.equal((await send('/api/me',undefined,{method:'GET'})).status,200);assert.equal((await send('/v1/images/generations',{model:'gpt-6.1-sol',prompt:'合成图像',stream:false})).status,200);
  assert.equal((await send('/v1/chat/completions',{messages:[{role:'user',content:'合成文本任务'}]})).status,200);
  await preparePlatformAccount(plain,(await gatewayLogin(plain,context.password)));assert.equal((await send('/v1/platform/models',undefined,{method:'GET'})).status,200);assert.equal((await send('/v1/platform/chat/completions',{model:'deepseek/synthetic-model',messages:[{role:'user',content:'合成平台任务'}]})).status,200);
  const snapshots=await store.all("SELECT source_channel,device_id,device_auth_state,device_generation FROM harbor.requests WHERE status='ok'");assert.equal(snapshots.length,4);assert.ok(snapshots.every(row=>row.device_id===deviceId&&row.device_auth_state==='authenticated'&&row.device_generation===1));assert.deepEqual(new Set(snapshots.map(row=>row.source_channel)),new Set(['responses','images/generations','chat','platform']));
  const current=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);assert.equal((await admin('devices/'+deviceId,'PATCH',{expected_version:current.version,name:'修改后的合成名称'})).status,200);assert.equal((await store.get('SELECT device_name FROM harbor.requests WHERE id=$1',first.id)).device_name,first.device_name);
  assert.equal((await fetch(plain+'/api/admin/devices')).status,401);assert.equal((await admin('devices')).status,200);assert.equal(app.status().active,0);
}));

test('shared PostgreSQL device revocation rejects admitted Responses before dispatch and lets already dispatched responses finish',integration,async t=>withGateway(t,async context=>{
  let entered=0,releaseUpstream,upstreamGate;const env=await enrolledGateway(t,context,{responsesTransport:controlledResponses(async()=>{entered++;if(upstreamGate)await upstreamGate;})}),{store,member}=context,{app,send,admin,deviceId}=env,question={model:'gpt-6.1-sol',input:'合成撤销边界',stream:false};assert.equal((await send('/v1/responses',question)).status,200);
  await app.deviceAccess.policy(member.memberId,{expected_version:0,mode:'enforced'});const original=app.store.transaction.bind(app.store);let held=false,resume;
  app.store.transaction=async(operation,options)=>{const result=await original(operation,options);if(result?.requestId&&!held){held=true;await new Promise(resolve=>{resume=resolve;});}return result;};
  const pending=send('/v1/responses',question);try{await gatewayWait(()=>held);let row=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);assert.equal((await admin('devices/'+deviceId,'PATCH',{expected_version:row.version,enabled:false})).status,200);resume();assert.equal((await pending).status,403);assert.equal(entered,1);}finally{resume?.();app.store.transaction=original;await pending.catch(()=>{});}
  let row=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);assert.equal((await admin('devices/'+deviceId,'PATCH',{expected_version:row.version,enabled:true})).status,200);
  upstreamGate=new Promise(resolve=>{releaseUpstream=resolve;});const active=send('/v1/responses',question);try{await gatewayWait(()=>entered===2);row=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);assert.equal((await admin('devices/'+deviceId,'PATCH',{expected_version:row.version,enabled:false})).status,200);releaseUpstream();assert.equal((await active).status,200);assert.equal((await send('/v1/responses',question)).status,403);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE status='ok'")).n,2);}finally{releaseUpstream?.();await active.catch(()=>{});}
}));

for(const channel of ['chat','platform'])test('shared PostgreSQL device '+channel+' rechecks queued admission and preserves already dispatched completion',integration,async t=>withGateway(t,async context=>{
  const {store,member}=context;await prepareHttpChat(store,member);let calls=0,gate,release;
  const execute=async()=>{calls++;if(gate)await gate;return channel==='chat'?{text:'合成CLI设备任务',threadId:'synthetic-device-boundary',input:10,output:2,cached:0,usageKnown:true}:platformResult;};
  const env=await enrolledGateway(t,context,{executor:execute,platformTransport:{models:async()=>['synthetic-model'],run:execute}}),{app,send,admin,deviceId,plain}=env;
  assert.equal((await send('/v1/responses',{model:'gpt-6.1-sol',input:'合成设备验证'})).status,200);await app.deviceAccess.policy(member.memberId,{expected_version:0,mode:'enforced'});
  if(channel==='platform')await preparePlatformAccount(plain,await gatewayLogin(plain,context.password));
  const path=channel==='chat'?'/v1/chat/completions':'/v1/platform/chat/completions',body={messages:[{role:'user',content:'合成排队边界'}],...(channel==='platform'?{model:'deepseek/synthetic-model'}:{})},original=app.store.transaction.bind(app.store);let held=false,resume;
  app.store.transaction=async(op,options)=>{const result=await original(op,options);if(result?.requestId&&!held){held=true;await new Promise(resolve=>{resume=resolve;});}return result;};const pending=send(path,body);
  try{await gatewayWait(()=>held);let device=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);await app.deviceAccess.update(deviceId,{expected_version:device.version,enabled:false});resume();assert.equal((await pending).status,403);assert.equal(calls,0);}finally{resume?.();app.store.transaction=original;await pending.catch(()=>{});}
  let device=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);await app.deviceAccess.update(deviceId,{expected_version:device.version,enabled:true});
  gate=new Promise(resolve=>{release=resolve;});const active=send(path,body);try{await gatewayWait(()=>calls===1);device=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);await app.deviceAccess.update(deviceId,{expected_version:device.version,enabled:false});release();assert.equal((await active).status,200);assert.equal((await send(path,body)).status,403);assert.equal(calls,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE source_channel=$1 AND status='ok'",channel)).n,1);}finally{release?.();await active.catch(()=>{});}
}));

test('PostgreSQL device TLS configuration loads only controlled files and disabling transport cannot disable persisted enforcement',integration,async t=>withGateway(t,async context=>{
  const {root,open,store,member}=context,ca=await certificateFixture(root);t.after(()=>ca.issuer.close());
  for(const env of [{HARBOR_DEVICE_TLS_ENABLED:'yes'},{HARBOR_DEVICE_TLS_ENABLED:'1',HARBOR_DEVICE_TLS_PORT:'0'},{HARBOR_DEVICE_TLS_ENABLED:'1',HARBOR_DEVICE_TLS_PORT:'4443',HARBOR_DEVICE_TLS_KEY_FILE:'relative'}])await assert.rejects(open({env}),{code:'device_tls_configuration'});
  let app=await open({env:{HARBOR_DEVICE_TLS_ENABLED:'1',HARBOR_DEVICE_TLS_PORT:'4443',HARBOR_DEVICE_TLS_KEY_FILE:join(root,'server-key.pem'),HARBOR_DEVICE_TLS_CERT_FILE:join(root,'server.pem'),HARBOR_DEVICE_CA_CERT_FILE:join(root,'ca.pem'),HARBOR_DEVICE_CA_KEY_FILE:join(root,'ca-key.pem')}});assert.ok(app.deviceTls);assert.equal(app.deviceAccess.status().issuerConfigured,true);await app.close();
  await store.run("UPDATE harbor.members SET device_mode='enforced' WHERE id=$1",member.memberId);app=await open({env:{HARBOR_DEVICE_TLS_ENABLED:'0',HARBOR_DEVICE_TLS_KEY_FILE:'/must-not-read-disabled.pem'}});assert.equal(app.deviceTls,undefined);assert.equal(app.deviceAccess.status().issuerConfigured,false);const origin=await listenGateway(app),response=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:'not-json'});assert.equal(response.status,403);assert.equal((await response.json()).error.code,'device_required');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
}));

for(const path of ['/v1/responses','/v1/images/generations'])test('R48 R49 device revocation during real credentials preparation prevents '+path+' dispatch with known zero use',integration,async t=>withGateway(t,async context=>{
  const {store,member,root}=context;let gate,release,waiting=false,calls=0;const transport=createResponsesTransport({version:async()=>'0.160.1',credentials:async()=>{if(gate){waiting=true;await gate;}return {token:'synthetic-upstream-token',accountId:'synthetic-account'};},fetchImpl:async target=>{calls++;if(target.endsWith('/images/generations'))return new Response(JSON.stringify({data:[{b64_json:'c3lu'}],usage:{input_tokens:10,output_tokens:2,input_tokens_details:{cached_tokens:0}}}),{headers:{'content-type':'application/json'}});return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-r48',model:'gpt-6.1-sol',output:[],usage:{input_tokens:10,output_tokens:2,input_tokens_details:{cached_tokens:0}}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});}}),env=await enrolledGateway(t,context,{responsesTransport:transport}),{app,send,client,origin,deviceId,admin}=env;
  assert.equal((await send('/v1/responses',{model:'gpt-6.1-sol',input:'合成初次验证'})).status,200);await app.deviceAccess.policy(member.memberId,{expected_version:0,mode:'enforced'});assert.equal((await admin('members/'+member.memberId+'/usage-limits','PATCH',{expected_version:0,enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'20.00'}]})).status,200);
  const peerKey=await clientKey(root,'r49-normal-peer'),invitation=await app.deviceAccess.invite({member_id:member.memberId}),challenge=await app.deviceAccess.challenge({invitation:invitation.token}),payload={invitation:invitation.token,challenge:challenge.challenge,nonce:challenge.nonce,scope:challenge.scope,csr:peerKey.csr,name:'另一正常合成设备',system:'合成系统',fingerprint:client.headers['x-harbor-hardware'],installation_fingerprint:null,fingerprint_source:'smbios_uuid'};payload.signature=sign('sha256',Buffer.from(deviceEnrollmentMessage(payload)),peerKey.key).toString('base64');const registration=await app.deviceAccess.register(payload);await app.deviceAccess.approve(registration.device_id,{expected_version:0,credentials_only:false});const certificate=await app.deviceAccess.enrollmentStatus({enrollment_token:registration.enrollment_token}),ca=readFileSync(join(root,'ca.pem')),peer=new https.Agent({ca,key:peerKey.key,cert:certificate.certificate});t.after(()=>peer.destroy());
  const body={model:'gpt-6.1-sol',stream:false,prompt_cache_key:'synthetic-r48-'+path,...(path==='/v1/responses'?{input:'凭据等待合成任务'}:{prompt:'凭据等待合成图片'})};gate=new Promise(resolve=>{release=resolve;});const pending=send(path,body);
  try{await gatewayWait(()=>waiting);const device=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);await app.deviceAccess.update(deviceId,{expected_version:device.version,enabled:false});release();const denied=await pending;assert.equal(denied.status,403);assert.equal(calls,1);const rejected=await store.get('SELECT * FROM harbor.requests ORDER BY query_sequence DESC LIMIT 1');assert.equal(rejected.status,'rejected');assert.equal(rejected.usage_unknown,0);assert.equal(rejected.input,0);assert.equal(rejected.output,0);assert.equal((await store.get('SELECT unmetered FROM harbor.keys WHERE id=$1',member.keyId)).unmetered,0);assert.notEqual((await store.get('SELECT state FROM harbor.sessions WHERE id=$1',rejected.session_id)).state,'uncertain');
    gate=null;const continued=await deviceGatewayRequest(origin,path,{agent:peer,headers:{authorization:'Bearer '+member.secret,'x-harbor-hardware':payload.fingerprint},body});assert.equal(continued.status,200);assert.equal(calls,2);
  }finally{release?.();await pending.catch(()=>{});}
}));

test('PostgreSQL shared action routes freeze stable employee IDs and execute access and renewal through the independent Worker',integration,async t=>withGateway(t,async({store,name,member,password,open})=>{
  const legacyId='synthetic-action-person';await otherHttpChatKey(store,legacyId);const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),call=async(path,method='GET',body)=>{const response=await fetch(origin+'/api/admin/member-action-batches'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:response.status,value:await response.json()};},worker=await createPostgresWorker({database:options(name)});
  const execute=async id=>{assert.equal((await call('/'+id+'/execute','POST',{})).status,202);await gatewayWait(async()=>{await worker.runOnce();return (await store.get('SELECT status FROM harbor.member_action_batches WHERE id=$1',id)).status==='completed';});return (await call('/'+id)).value;};
  try{
    assert.equal((await fetch(origin+'/api/admin/member-action-batches')).status,401);const preview=await call('/preview','POST',{kind:'member_access',enabled:false,member_ids:[member.memberId,legacyId]});assert.equal(preview.status,201);await store.run('UPDATE harbor.members SET enabled=0 WHERE id=$1',legacyId);await store.run('UPDATE harbor.members SET enabled=1 WHERE id=$1',legacyId);
    const result=await execute(preview.value.id);assert.equal(result.counts.success,1);assert.equal(result.counts.skipped,1);assert.equal((await store.get('SELECT enabled FROM harbor.members WHERE id=$1',member.memberId)).enabled,0);assert.equal((await store.get('SELECT enabled FROM harbor.members WHERE id=$1',legacyId)).enabled,1);assert.equal((await store.get('SELECT enabled FROM harbor.keys WHERE id=$1',member.keyId)).enabled,1);
    const undo=await call('/'+preview.value.id+'/undo','POST',{});assert.equal(undo.status,201);await execute(undo.value.id);assert.equal((await store.get('SELECT enabled FROM harbor.members WHERE id=$1',member.memberId)).enabled,1);
    const expiry=Date.now()+7*86400000,renew=await call('/preview','POST',{kind:'key_renewal',expires:expiry,key_ids:[member.keyId,legacyId+'-key']});assert.equal(renew.status,201);await execute(renew.value.id);assert.equal((await store.get('SELECT expires FROM harbor.keys WHERE id=$1',member.keyId)).expires,expiry);assert.equal((await store.get('SELECT expires FROM harbor.keys WHERE id=$1',legacyId+'-key')).expires,null);assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member.memberId)).used,0);assert.equal((await call('')).value.items.length,3);const history=await (await fetch(origin+'/api/admin/history?action=member.actions.apply',{headers:{cookie}})).json();assert.ok(history.rows.some(row=>row.detail.operation==='member_access'));assert.ok(!JSON.stringify(history).includes(member.secret));
  }finally{await worker.close();}
}));

test('PostgreSQL revision history aggregates committed facts across a slot and pages each immutable plan without ordinal collisions',integration,async t=>withGateway(t,async({store,password,open})=>{
  const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),at=Date.now(),day=new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Shanghai',year:'numeric',month:'2-digit',day:'2-digit'}).format(at)+'-1300',jobs=createPostgresJobs(store),first=randomUUID(),second=randomUUID(),pending=randomUUID();
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-peer','合成修订目标','synthetic','ready')");
  for(const [id,count,moved] of [[first,3,2],[second,5,1]]){await jobs.enqueue('routing.nightly',{id,target:day,payload:{}});await store.run("UPDATE harbor.background_jobs SET status='completed',result='{}' WHERE id=$1",id);const header={counts:{groups:1,moves:count,skipped:0,fixed:0},metric:referenceMetric,window:{start:at-3600000,end:at},requestBoundary:true};await store.run('INSERT INTO harbor.routing_plans VALUES($1,$2,$3,$4::jsonb,1)',id,at-1000,at+60000,JSON.stringify(header));await store.run("INSERT INTO harbor.routing_plan_items VALUES($1,'groups',1,$2::jsonb,1)",id,JSON.stringify({accounts:['synthetic-runtime','synthetic-peer'],before:{'synthetic-runtime':3,'synthetic-peer':0},after:{'synthetic-runtime':1,'synthetic-peer':2},fixedLoad:{'synthetic-runtime':0,'synthetic-peer':0},target:{'synthetic-runtime':1.5,'synthetic-peer':1.5}}));for(let i=1;i<=count;i++)await store.run("INSERT INTO harbor.routing_plan_items VALUES($1,'moves',$2,$3::jsonb,1)",id,i,JSON.stringify({session_id:(id===first?'original-session-':'revised-session-')+i,client_session_id:'synthetic-client-'+i,from:'synthetic-runtime',to:'synthetic-peer',weight:1}));}
  await store.run("INSERT INTO harbor.balance_runs(day,started,ended,status,summary,plan_id) VALUES($1,$2,$3,'completed','{}',$4)",day,at-1000,at,second);
  for(const [id,moved] of [[first,2],[second,1]]){await store.run("INSERT INTO harbor.nightly_plan_applications(plan_id,slot,started,ended,status) VALUES($1,$2,$3,$4,'completed')",id,day,at-1000,at);await store.run("INSERT INTO harbor.nightly_plan_group_results VALUES($1,1,$2,'applied',NULL,$3)",id,at,moved);}
  await store.run("INSERT INTO harbor.nightly_group_results VALUES($1,1,$2,'applied',NULL,99)",day,at);await store.run("INSERT INTO harbor.nightly_plan_revisions VALUES($1,2,$2,$3,$4,'gateway_restart')",day,second,first,at-1000);await jobs.enqueue('routing.nightly',{id:pending,target:day,payload:{}});await store.run("UPDATE harbor.background_jobs SET status='failed',error_code='synthetic-sensitive-reason' WHERE id=$1",pending);await store.run("INSERT INTO harbor.nightly_plan_revisions VALUES($1,3,$2,$3,$4,'gateway_restart')",day,pending,second,at);
  const request=async path=>{const response=await fetch(origin+'/api/admin/history/nightly/'+day+path,{headers:{cookie}});return {status:response.status,value:await response.json()};};let latest=await request('');assert.equal(latest.status,200);assert.equal(latest.value.revisionAware,true);assert.equal(latest.value.results.applied,3);assert.equal(latest.value.counts.moves,5);assert.equal(latest.value.plannedTotal,8);assert.equal(latest.value.selectedResults.applied,1);assert.equal(latest.value.previousRevision,0);assert.equal(latest.value.nextRevision,null);assert.equal(latest.value.pendingRevision.status,'failed');assert.equal(latest.value.pendingRevision.reason,null);
  let page=await request('/items?section=moves&limit=1');assert.equal(page.value.items[0].result.moved,1);assert.match(page.value.items[0].session_id,/revised-session/);const original=await request('?revision=0');assert.equal(original.value.results.applied,3);assert.equal(original.value.selectedResults.applied,2);assert.equal(original.value.nextRevision,2);page=await request('/items?section=moves&revision=0&limit=1');assert.equal(page.value.items[0].result.moved,2);assert.match(page.value.items[0].session_id,/original-session/);
  let revisions=await request('/revisions?limit=1');assert.equal(revisions.value.items[0].revision,0);assert.equal(revisions.value.has_more,true);revisions=await request('/revisions?limit=1&after='+revisions.value.after);assert.equal(revisions.value.items[0].revision,2);assert.equal(revisions.value.has_more,false);for(const path of ['?revision=1','?revision=99'])assert.equal((await request(path)).status,404);for(const path of ['?revision=-1','?revision=0&revision=2','/revisions?revision=0'])assert.equal((await request(path)).status,400);const filtered=await (await fetch(origin+'/api/admin/history?kind=nightly&session=original-session-1',{headers:{cookie}})).json();assert.ok(filtered.rows.some(row=>row.day===day));
}));

for(const terminal of ['failed','cancelled'])test('R51 PostgreSQL a later revision '+terminal+' before producing a plan synchronizes batch state without changing committed facts',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let now=Date.now();const app=await open({clock:()=>now}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),slot=transferWindow(now).key,prior=randomUUID(),jobs=createPostgresJobs(store,{clock:()=>now}),revisions=createPostgresNightlyRevisions(store,{clock:()=>now});
  await jobs.enqueue('routing.nightly',{id:prior,target:slot,payload:{}});await store.run("UPDATE harbor.background_jobs SET status='completed' WHERE id=$1",prior);await store.run('INSERT INTO harbor.routing_plans VALUES($1,$2,$3,$4::jsonb,1)',prior,now-1000,now+60000,JSON.stringify({purpose:'nightly',requestBoundary:true,metric:referenceMetric,window:{start:now-3600000,end:now},counts:{groups:1,moves:1,skipped:0,fixed:0}}));await store.run("INSERT INTO harbor.balance_runs(day,started,ended,status,summary,plan_id) VALUES($1,$2,$3,'completed','{}',$4)",slot,now-1000,now-500,prior);await store.run("INSERT INTO harbor.nightly_plan_applications(plan_id,slot,started,ended,status) VALUES($1,$2,$3,$4,'completed')",prior,slot,now-1000,now-500);await store.run("INSERT INTO harbor.nightly_plan_group_results VALUES($1,1,$2,'applied',NULL,1)",prior,now-500);await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,route_version,created,updated) VALUES('synthetic-committed-r51',$1,'synthetic-runtime','responses',3,$2,$2)",member.memberId,now-500);
  const facts=await store.all('SELECT * FROM harbor.nightly_plan_group_results ORDER BY plan_id,group_ordinal'),binding=await store.get("SELECT * FROM harbor.sessions WHERE id='synthetic-committed-r51'"),next=await revisions.request({id:prior,target:slot},'gateway_restart');assert.equal((await store.get('SELECT status FROM harbor.balance_runs WHERE day=$1',slot)).status,'running');assert.equal(JSON.parse((await store.get('SELECT summary FROM harbor.balance_runs WHERE day=$1',slot)).summary).pending_revision_status,'pending');
  await jobs.enqueue('routing.nightly',{id:next,target:slot,payload:{boundary:transferWindow(now).end},maxAttempts:1});const job=await jobs.claim(['routing.nightly']);assert.equal(job.id,next);now+=100;if(terminal==='failed')assert.equal(await jobs.fail(job,{retry:false,errorCode:'planning_failed'}),'failed');else assert.equal(await jobs.cancel(next),true);
  await app.nightly.runOnce();let result=await store.get('SELECT status,ended,summary FROM harbor.balance_runs WHERE day=$1',slot);assert.equal(result.status,terminal);assert.equal(result.ended,now);assert.equal(JSON.parse(result.summary).applied,1);const header=await (await fetch(origin+'/api/admin/history/nightly/'+slot,{headers:{cookie}})).json(),list=await (await fetch(origin+'/api/admin/history?kind=nightly',{headers:{cookie}})).json();assert.equal(header.status,terminal);assert.equal(header.results.applied,1);assert.equal(header.pendingRevision.status,terminal);assert.equal(list.rows.find(row=>row.day===slot).status,terminal);assert.deepEqual(await store.all('SELECT * FROM harbor.nightly_plan_group_results ORDER BY plan_id,group_ordinal'),facts);assert.deepEqual(await store.get("SELECT * FROM harbor.sessions WHERE id='synthetic-committed-r51'"),binding);await app.nightly.runOnce();assert.deepEqual(await store.get('SELECT status,ended,summary FROM harbor.balance_runs WHERE day=$1',slot),result);
}));

test('shared PostgreSQL client catalogs preserve Key model filtering and serve a fixed credential-free MCP script',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),paths=['/v1/models','/v1/codex/models','/v1/codex/image-mcp.mjs'];for(const path of paths){assert.equal((await fetch(origin+path)).status,401);assert.equal((await fetch(origin+path,{headers:{cookie}})).status,401);}
 let response=await fetch(origin+'/v1/models',{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,200);let value=await response.json();assert.deepEqual(value.data.map(model=>model.id),['gpt-6.1-sol']);assert.equal(value.catalog_status.complete,true);response=await fetch(origin+'/v1/codex/models',{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,200);assert.deepEqual((await response.json()).models.map(model=>model.slug),['gpt-6.1-sol']);response=await fetch(origin+'/v1/codex/image-mcp.mjs',{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,200);const script=await response.text();assert.equal(script,readFileSync(new URL('../clients/image-mcp.mjs',import.meta.url),'utf8'));assert.ok(!script.includes(member.secret));
 await store.run('UPDATE harbor.keys SET allowed_models=NULL WHERE id=$1',member.keyId);response=await fetch(origin+'/v1/models',{headers:{authorization:'Bearer '+member.secret}});value=await response.json();assert.deepEqual(value.data.map(model=>model.id),['codex','claude','gpt-6.1-sol','gpt-6-astra']);assert.equal(app.status().clientCatalog.requests.active,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
}));
test('shared PostgreSQL client catalogs enforce persisted device policy on all three GET endpoints over actual TLS',integration,async t=>withGateway(t,async context=>{
 const {store,member}=context,env=await enrolledGateway(t,context),{app,send,origin,plain,anon,deviceId}=env;assert.equal((await send('/v1/responses',{model:'gpt-6.1-sol',input:'合成设备核验'})).status,200);await app.deviceAccess.policy(member.memberId,{mode:'enforced',expected_version:0});
 for(const path of ['/v1/models','/v1/codex/models','/v1/codex/image-mcp.mjs']){assert.equal((await send(path,undefined,{method:'GET'})).status,200);assert.equal((await deviceGatewayRequest(origin,path,{method:'GET',agent:anon,headers:{authorization:'Bearer '+member.secret,'x-harbor-device-id':deviceId}})).status,403);assert.equal((await fetch(plain+path,{headers:{authorization:'Bearer '+member.secret,'ssl-client-verify':'SUCCESS'}})).status,403);}
 const device=await store.get('SELECT version FROM harbor.employee_devices WHERE id=$1',deviceId);await app.deviceAccess.update(deviceId,{expected_version:device.version,enabled:false});assert.equal((await send('/v1/codex/models',undefined,{method:'GET'})).status,403);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
}));

test('R53 shared PostgreSQL catalog aborts on real ownership loss while ignored cancellation retains provider capacity',integration,async t=>withGateway(t,async({store,member,open})=>{
 let entered=false,providerSignal,release;const gate=new Promise(resolve=>{release=resolve;}),app=await open({responsesTransport:{...controlledResponses(),models:async({signal})=>{entered=true;providerSignal=signal;await gate;return [{slug:'gpt-6.1-sol'}];}}}),origin=await listenGateway(app),pending=fetch(origin+'/v1/models',{headers:{authorization:'Bearer '+member.secret}});pending.catch(()=>{});
 try{await gatewayWait(()=>entered);const owner=await store.get("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND granted");assert.ok(owner);await store.get('SELECT pg_terminate_backend($1) terminated',owner.pid);await gatewayWait(()=>!app.status().ownership);assert.equal(app.status().clientCatalog.stopping,true);assert.equal(providerSignal.aborted,true);const response=await pending;assert.equal(response.status,503);assert.equal((await response.json()).error.code,'service_stopping');assert.equal(app.status().clientCatalog.providers.active,1);assert.equal(app.status().clientCatalog.requests.active,1);let closed=false;const closing=app.close().then(()=>{closed=true;});await new Promise(resolve=>setImmediate(resolve));assert.equal(closed,false);release();await closing;assert.equal(app.status().clientCatalog.providers.active,0);assert.equal(app.status().clientCatalog.requests.active,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);}finally{release();await pending.catch(()=>{});}
}));

test('PostgreSQL configuration batches freeze targets, retain exceptions, skip concurrent changes and execute through the real Worker',integration,async t=>withGateway(t,async({store,name,password,open})=>{
  let entered=false,release;const held=new Promise(resolve=>{release=resolve;}),app=await open({responsesTransport:controlledResponses(async()=>{entered=true;await held;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const admin=async(path,method='GET',body)=>{const r=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,value:await r.json()};};
  assert.equal((await fetch(origin+'/api/admin/member-config-batches/preview',{method:'POST',headers:{'content-type':'application/json'},body:'{}'})).status,401);
  const old={allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'9.00'}]}},next={allowed_models:['gpt-6-astra'],usage_limits:{enabled:true,rules:[{model:'gpt-6-astra',hours:168,limit_usd:'2.00'}]}};
  assert.equal((await admin('member-types/usage-a','PATCH',{expected_version:0,defaults:old})).status,200);
  const create=async label=>(await admin('members','POST',{name:label,quota:null,expires:null,type_id:'usage-a',expected_type_version:1,expected_template_version:1})).value,first=await create('批次保留例外员工'),second=await create('批次冲突员工'),extra=(await admin('keys','POST',{memberId:first.memberId,expires:null})).value;
  await admin('keys/'+first.keyId,'PATCH',{allowed_models:['gpt-6.1-sol'],expected_model_policy_version:0});await admin('members/'+first.memberId+'/usage-limits','PATCH',{enabled:false,rules:[],expected_version:1});
  await store.run('UPDATE harbor.members SET used=123 WHERE id=$1',first.memberId);await admin('member-types/usage-a','PATCH',{expected_version:1,defaults:next});
  const preview=await admin('member-config-batches/preview','POST',{scope:{type_id:'usage-a'},type_id:'usage-a',expected_type_version:2,models:true,limits:true,classify:false,overwrite_exceptions:false});assert.equal(preview.status,201);assert.equal(preview.value.members,2);
  const detail=(await admin('member-config-batches/'+preview.value.id+'?limit=1')).value;assert.equal(detail.items.length,1);assert.equal(detail.has_more,true);assert.ok(!JSON.stringify(detail).includes(first.secret));const rest=(await admin('member-config-batches/'+preview.value.id+'?limit=1&after='+detail.after)).value;assert.equal(rest.items.length,1);assert.equal(rest.has_more,false);
  const pending=fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+extra.secret,'content-type':'application/json','thread-id':'batch-inflight'},body:JSON.stringify({model:'gpt-6.1-sol',input:'批次在途合成请求'})}).then(async r=>({status:r.status,body:await r.json()}));
  try{
    await gatewayWait(()=>entered);await admin('members/'+second.memberId+'/type','PATCH',{type_id:'usage-b',expected_type_version:0,expected_usage_type_version:1,apply_defaults:false});
    const later=(await admin('members','POST',{name:'预览后新增员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:2,expected_template_version:2})).value;
    assert.equal((await admin('member-config-batches/'+preview.value.id+'/execute','POST',{member_ids:[later.memberId]})).status,400);assert.equal((await admin('member-config-batches/'+preview.value.id+'/execute','POST',{})).status,202);
    const worker=await createPostgresWorker({database:options(name),runtime:{pollMs:100}});try{worker.start();await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.member_config_batches WHERE id=$1',preview.value.id)).status==='completed');}finally{await worker.close();}
    const result=(await admin('member-config-batches/'+preview.value.id)).value;assert.deepEqual(result.counts,{success:1,skipped:1});assert.equal(result.items.find(row=>row.member_id===second.memberId).reason,'configuration_changed');
    const applied=await store.get('SELECT * FROM harbor.members WHERE id=$1',first.memberId);assert.equal(applied.used,123);assert.equal(JSON.parse(applied.usage_limits).enabled,false);assert.equal(applied.configuration_source.limits.kind,'custom');assert.deepEqual(JSON.parse(applied.default_models),['gpt-6-astra']);
    assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',first.keyId)).allowed_models),['gpt-6.1-sol']);assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',extra.keyId)).allowed_models),['gpt-6-astra']);assert.equal(result.items.find(row=>row.member_id===first.memberId).exceptions.length,2);
    const newKey=(await admin('keys','POST',{memberId:first.memberId,expires:null})).value;assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',newKey.keyId)).allowed_models),['gpt-6-astra']);
    assert.equal((await store.get('SELECT defaults_version FROM harbor.members WHERE id=$1',later.memberId)).defaults_version,2);
    release();assert.equal((await pending).status,200);const denied=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+extra.secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',input:'后续请求应拒绝'})});assert.equal(denied.status,403);
    const undo=await admin('member-config-batches/'+preview.value.id+'/undo','POST',{});assert.equal(undo.status,201);await admin('member-config-batches/'+undo.value.id+'/execute','POST',{});
    const undoWorker=await createPostgresWorker({database:options(name),runtime:{pollMs:100}});try{undoWorker.start();await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.member_config_batches WHERE id=$1',undo.value.id)).status==='completed');}finally{await undoWorker.close();}
    assert.equal((await admin('member-config-batches/'+undo.value.id)).value.items[0].reason,'configuration_changed');assert.deepEqual(JSON.parse((await store.get('SELECT default_models FROM harbor.members WHERE id=$1',first.memberId)).default_models),['gpt-6-astra']);
  }finally{release();await pending;}
}));

test('PostgreSQL batch failures roll back the whole employee, retry only failed items and undo with increasing versions',integration,async t=>withGateway(t,async({store,open})=>{
  const app=await open(),admin=createPostgresMemberAdmin(app.store,{encryptionKey:Buffer.alloc(32,2)}),request=async(path,method,payload)=>(await admin.handle('/api/admin/'+path,method,payload)).value;
  const old={allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]}},next={allowed_models:['gpt-6-astra'],usage_limits:{enabled:true,rules:[{model:'gpt-6-astra',hours:24,limit_usd:'1.00'}]}};
  await request('member-types/usage-c','PATCH',{expected_version:0,defaults:old});const employee=await request('members','POST',{name:'事务批次合成员工',quota:null,expires:null,type_id:'usage-c',expected_type_version:1,expected_template_version:1});await request('member-types/usage-c','PATCH',{expected_version:1,defaults:next});
  const jobs=createPostgresJobs(store),batches=createPostgresMemberBatches(store,{jobs}),preview=await batches.preview({scope:{member_ids:[employee.memberId]},type_id:'usage-c',expected_type_version:2,models:true,limits:true,classify:false,overwrite_exceptions:false});
  const before=await store.get('SELECT * FROM harbor.members WHERE id=$1',employee.memberId);await batches.submit(preview.id);let job=await jobs.claim(['member.config']);const run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit')&&args[2]==='member.batch.apply')throw Error('synthetic batch audit failure');return run(sql,...args);};
  try{await jobs.complete(job,await batches.handlers['member.config'](job));}finally{store.run=run;}assert.deepEqual(await store.get('SELECT * FROM harbor.members WHERE id=$1',employee.memberId),before);assert.deepEqual((await batches.status(preview.id)).counts,{failed:1});
  const retry=await batches.derived(preview.id,'retry');assert.equal(retry.members,1);await batches.submit(retry.id);job=await jobs.claim(['member.config']);await jobs.complete(job,await batches.handlers['member.config'](job));assert.deepEqual((await batches.status(retry.id)).counts,{success:1});const applied=await store.get('SELECT * FROM harbor.members WHERE id=$1',employee.memberId);assert.equal(applied.configuration_source.models.template_version,2);
  const undo=await batches.derived(retry.id,'undo');await batches.submit(undo.id);job=await jobs.claim(['member.config']);await jobs.complete(job,await batches.handlers['member.config'](job));assert.deepEqual((await batches.status(undo.id)).counts,{success:1});const restored=await store.get('SELECT * FROM harbor.members WHERE id=$1',employee.memberId);assert.deepEqual(JSON.parse(restored.default_models),old.allowed_models);assert.equal(JSON.parse(restored.usage_limits).enabled,false);assert.ok(restored.defaults_version>applied.defaults_version);assert.ok(restored.usage_limits_version>applied.usage_limits_version);assert.equal(restored.used,before.used);
  const stale=await batches.preview({scope:{member_ids:[employee.memberId]},type_id:'usage-c',expected_type_version:2,models:true,limits:false,classify:false,overwrite_exceptions:true});await request('member-types/usage-c','PATCH',{expected_version:2,defaults:old});await batches.submit(stale.id);job=await jobs.claim(['member.config']);await jobs.complete(job,await batches.handlers['member.config'](job));assert.equal((await batches.status(stale.id)).items[0].reason,'template_changed');
  const cancelled=await batches.preview({scope:{member_ids:[employee.memberId]},type_id:'usage-c',expected_type_version:3,models:false,limits:false,classify:true,overwrite_exceptions:false});await batches.submit(cancelled.id);await batches.cancel(cancelled.id);job=await jobs.claim(['member.config']);await jobs.complete(job,await batches.handlers['member.config'](job));assert.deepEqual((await batches.status(cancelled.id)).counts,{cancelled:1});
}));

test('PostgreSQL configuration batches resume only pending members after interruption and explicit override does not re-enable Keys',integration,async t=>withGateway(t,async({store,open})=>{
  const app=await open(),admin=createPostgresMemberAdmin(app.store,{encryptionKey:Buffer.alloc(32,3)}),request=async(path,method,payload)=>(await admin.handle('/api/admin/'+path,method,payload)).value;
  const target={allowed_models:['gpt-6-astra'],usage_limits:{enabled:false,rules:[]}};await request('member-types/usage-e','PATCH',{expected_version:0,defaults:target});const employees=[];
  for(let index=0;index<3;index++)employees.push(await request('members','POST',{name:'恢复批次员工'+index,quota:null,expires:null,allowed_models:['gpt-6.1-sol']}));await request('keys/'+employees[0].keyId,'PATCH',{enabled:false});
  const jobs=createPostgresJobs(store),batches=createPostgresMemberBatches(store,{jobs}),preview=await batches.preview({scope:{member_ids:employees.map(e=>e.memberId)},type_id:'usage-e',expected_type_version:1,models:true,limits:true,classify:true,overwrite_exceptions:true});const initial=await batches.status(preview.id);assert.equal(initial.items[0].exceptions.length,3);assert.ok(initial.items.every(item=>item.exceptions.every(e=>e.action==='overwritten')));
  await batches.submit(preview.id);let job=await jobs.claim(['member.config']),controller=new AbortController();const run=store.run.bind(store);let first=true;store.run=async(sql,...args)=>{const value=await run(sql,...args);if(first&&sql.startsWith('UPDATE harbor.member_config_batch_items SET status=$1')){first=false;controller.abort();}return value;};
  try{await assert.rejects(batches.handlers['member.config'](job,{signal:controller.signal}),{code:'worker_stopping'});}finally{store.run=run;}assert.deepEqual((await batches.status(preview.id)).counts,{success:1,pending:2});await jobs.fail(job,{errorCode:'worker_interrupted',retryDelayMs:0});
  job=await jobs.claim(['member.config']);await jobs.complete(job,await createPostgresMemberBatches(store,{jobs}).handlers['member.config'](job));assert.deepEqual((await batches.status(preview.id)).counts,{success:3});assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='member.batch.apply' AND detail::jsonb->>'batch'=$1",preview.id)).n,3);assert.equal((await store.get('SELECT enabled FROM harbor.keys WHERE id=$1',employees[0].keyId)).enabled,0);assert.ok((await store.all('SELECT usage_type_id FROM harbor.members WHERE id=ANY($1::text[])',employees.map(e=>e.memberId))).every(m=>m.usage_type_id==='usage-e'));
  const config=await store.get('SELECT default_models,usage_limits,configuration_source FROM harbor.members WHERE id=$1',employees[0].memberId),beforeCount=(await store.get('SELECT COUNT(*) n FROM harbor.member_config_batches')).n;
  const small=createPostgresMemberBatches(store,{maxTargets:1});await assert.rejects(small.preview({scope:{type_id:'usage-e'},type_id:'usage-e',expected_type_version:1,models:true,limits:false,classify:false,overwrite_exceptions:true}),{code:'member_batch_capacity'});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.member_config_batches')).n,beforeCount);assert.deepEqual(await store.get('SELECT default_models,usage_limits,configuration_source FROM harbor.members WHERE id=$1',employees[0].memberId),config);
  const narrow=createPostgresMemberBatches(store,{maxPreviewBytes:1024});await assert.rejects(narrow.preview({scope:{type_id:'usage-e'},type_id:'usage-e',expected_type_version:1,models:true,limits:false,classify:false,overwrite_exceptions:true}),{code:'member_batch_capacity'});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.member_config_batches')).n,beforeCount);
  const listed=await batches.handle('/api/admin/member-config-batches','GET',{},new URLSearchParams({limit:'1'}));assert.equal(listed.value.items.length,1);assert.equal(listed.value.items[0].id,preview.id);await assert.rejects(batches.handle('/api/admin/member-config-batches','GET',{},new URLSearchParams('limit=1&limit=2')),{code:'member_batch_invalid'});
}));

test('PostgreSQL classification-only batch retains legacy limit values and exposes exhausted jobs for safe retry',integration,async t=>withGateway(t,async({store,member})=>{
  const legacy={enabled:true,rules:[{model:'synthetic-legacy-no-rate',hours:24,limit_usd:'1.00'}]};await store.run('UPDATE harbor.members SET usage_limits=$1 WHERE id=$2',JSON.stringify(legacy),member.memberId);
  const jobs=createPostgresJobs(store),batches=createPostgresMemberBatches(store,{jobs}),preview=await batches.preview({scope:{member_ids:[member.memberId]},type_id:'usage-f',expected_type_version:0,models:false,limits:false,classify:true,overwrite_exceptions:false});await batches.submit(preview.id);
  for(let attempt=0;attempt<5;attempt++){const job=await jobs.claim(['member.config']);assert.ok(job);await jobs.fail(job,{errorCode:'worker_interrupted',retryDelayMs:0});}
  assert.equal((await batches.status(preview.id)).execution_failed,true);const retry=await batches.derived(preview.id,'retry');assert.equal(retry.members,1);assert.equal((await batches.status(preview.id)).items[0].reason,'execution_interrupted');await batches.submit(retry.id);const job=await jobs.claim(['member.config']);await jobs.complete(job,await batches.handlers['member.config'](job));
  assert.deepEqual((await batches.status(retry.id)).counts,{success:1});const result=await store.get('SELECT usage_type_id,usage_limits,usage_limits_version,configuration_source FROM harbor.members WHERE id=$1',member.memberId);assert.equal(result.usage_type_id,'usage-f');assert.deepEqual(JSON.parse(result.usage_limits),legacy);assert.equal(result.usage_limits_version,0);assert.equal(result.configuration_source.limits.kind,'retained');
}));

test('R41 stale Key defaults are rejected and R42 acknowledgement loss after all effects recovers without an empty retry',integration,async t=>withGateway(t,async({store,open})=>{
  const app=await open(),admin=createPostgresMemberAdmin(app.store,{encryptionKey:Buffer.alloc(32,4)}),request=async(path,method,payload)=>(await admin.handle('/api/admin/'+path,method,payload)).value,employee=await request('members','POST',{name:'终态恢复合成员工',quota:null,expires:null,allowed_models:['gpt-6.1-sol']});await request('member-types/usage-d','PATCH',{expected_version:0,defaults:{allowed_models:['gpt-6-astra'],usage_limits:{enabled:false,rules:[]}}});
  const jobs=createPostgresJobs(store),batches=createPostgresMemberBatches(store,{jobs}),preview=await batches.preview({scope:{member_ids:[employee.memberId]},type_id:'usage-d',expected_type_version:1,models:true,limits:true,classify:true,overwrite_exceptions:true});await batches.submit(preview.id);let job;for(let attempt=0;attempt<5;attempt++){job=await jobs.claim(['member.config']);assert.ok(job);if(attempt<4)await jobs.fail(job,{errorCode:'worker_interrupted',retryDelayMs:0});}
  const commit=jobs.commitEffect;jobs.commitEffect=async()=>{throw Object.assign(Error('synthetic final acknowledgement loss'),{code:'worker_stopping'});};try{await assert.rejects(batches.handlers['member.config'](job),{code:'worker_stopping'});}finally{jobs.commitEffect=commit;}await jobs.fail(job,{errorCode:'worker_interrupted',retryDelayMs:0});assert.equal((await batches.status(preview.id)).execution_failed,true);assert.deepEqual((await batches.status(preview.id)).counts,{success:1});
  const keyCount=(await store.get('SELECT COUNT(*) n FROM harbor.keys WHERE member_id=$1',employee.memberId)).n;await assert.rejects(request('keys','POST',{memberId:employee.memberId,expires:null,expected_defaults_version:0,allowed_models:null}),{code:'employee_defaults_conflict'});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.keys WHERE member_id=$1',employee.memberId)).n,keyCount);
  const beforeCount=(await store.get('SELECT COUNT(*) n FROM harbor.member_config_batches')).n,retry=await batches.derived(preview.id,'retry');assert.equal(retry.id,preview.id);assert.equal(retry.no_retry_needed,true);assert.equal((await batches.status(preview.id)).status,'completed');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.member_config_batches')).n,beforeCount);
  const undo=await batches.derived(preview.id,'undo');await batches.submit(undo.id);job=await jobs.claim(['member.config']);await jobs.complete(job,await batches.handlers['member.config'](job));assert.deepEqual((await batches.status(undo.id)).counts,{success:1});assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='member.batch.apply' AND detail::jsonb->>'batch'=$1",preview.id)).n,1);
}));

test('R43 preserving an employee old default also preserves its inherited Keys until exception overwrite is explicit',integration,async t=>withGateway(t,async({store,open})=>{
  const app=await open(),admin=createPostgresMemberAdmin(app.store,{encryptionKey:Buffer.alloc(32,5)}),request=async(path,method,payload)=>(await admin.handle('/api/admin/'+path,method,payload)).value;
  await request('member-types/usage-a','PATCH',{expected_version:0,defaults:{allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]}}});await request('member-types/usage-b','PATCH',{expected_version:0,defaults:{allowed_models:['gpt-6-astra'],usage_limits:{enabled:false,rules:[]}}});const employee=await request('members','POST',{name:'保留旧默认合成员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:1,expected_template_version:1});await request('members/'+employee.memberId+'/type','PATCH',{type_id:'usage-b',expected_type_version:1,expected_usage_type_version:1,apply_defaults:false});
  const batches=createPostgresMemberBatches(store),input={scope:{member_ids:[employee.memberId]},type_id:'usage-b',expected_type_version:1,models:true,limits:false,classify:false,overwrite_exceptions:false},keep=await batches.preview(input),kept=(await batches.status(keep.id)).items[0];assert.deepEqual(kept.desired_config.default_models,['gpt-6.1-sol']);assert.deepEqual(kept.desired_config.keys[0].allowed_models,['gpt-6.1-sol']);assert.ok(kept.exceptions.some(e=>e.component==='key'&&e.action==='preserved'));
  const override=await batches.preview({...input,overwrite_exceptions:true}),overridden=(await batches.status(override.id)).items[0];assert.deepEqual(overridden.desired_config.default_models,['gpt-6-astra']);assert.deepEqual(overridden.desired_config.keys[0].allowed_models,['gpt-6-astra']);assert.ok(overridden.exceptions.some(e=>e.component==='key'&&e.action==='overwritten'));
}));

test('PostgreSQL member pages filter on the server and defer all Key credentials until one paged member detail',integration,async t=>withGateway(t,async({store,password,open})=>{
  const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),request=async(path,method='GET',body)=>{const r=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,value:await r.json()};};
  for(const path of ['members','keys','members/selection'])assert.equal((await fetch(origin+'/api/admin/'+path)).status,401);
  await request('member-types/usage-a','PATCH',{expected_version:0,defaults:{allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]}}});const employees=[];
  for(let index=0;index<60;index++){const typed=index%2===0,result=await request('members','POST',{name:'分页合成员工'+String(index).padStart(2,'0'),quota:null,expires:null,...(typed?{type_id:'usage-a',expected_type_version:1,expected_template_version:1}:{allowed_models:['gpt-6.1-sol']})});assert.equal(result.status,201);employees.push(result.value);if(index%3===0)await request('members/'+result.value.memberId,'PATCH',{enabled:false});}
  const literal=await request('members','POST',{name:'字面%员工',quota:null,expires:null,allowed_models:null});let value=(await request('members')).value;assert.equal(value.rows.length,25);assert.equal(value.pagination.total,62);assert.equal(value.pagination.totalPages,3);const ids=new Set(value.rows.map(r=>r.id));for(let page=2;page<=3;page++)for(const row of (await request('members?page='+page)).value.rows){assert.ok(!ids.has(row.id));ids.add(row.id);}assert.equal(ids.size,62);
  assert.equal((await request('members?q=%25')).value.rows[0].id,literal.value.memberId);assert.equal((await request('members?type_id=usage-a&status=enabled')).value.pagination.total,20);assert.equal((await request('members?type_id=unclassified')).value.pagination.total,32);
  for(const path of ['members?page=0','members?page=1&page=2','members?type_id=missing','members?q='+encodeURIComponent('x'.repeat(101))])assert.equal((await request(path)).status,400);
  const limitedState=createPostgresStateQueries(store,{maxPeople:1});try{await assert.rejects(limitedState.read(),{code:'state_people_capacity'});assert.equal((await limitedState.read({people:'none'})).memberCount,62);}finally{await limitedState.close();}
  const compact=(await request('state?people=none')).value;assert.equal(compact.peopleLoaded,false);assert.equal(compact.memberCount,62);assert.deepEqual(compact.members,[]);assert.deepEqual(compact.keys,[]);assert.ok(!JSON.stringify(compact).includes(employees[1].secret));
  const list=(await request('keys?page_size=100')).value;assert.equal(list.rows.length,62);assert.ok(list.rows.every(row=>row.credentials_deferred&&!Object.hasOwn(row,'full_key')&&!Object.hasOwn(row,'allowed_models')));assert.ok(!JSON.stringify(list).includes(employees[1].secret));assert.ok(!JSON.stringify(value).includes(employees[1].secret));
  const owner=employees[1];const call=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+owner.secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',input:'分页统计合成问题'})});assert.equal(call.status,200);await call.json();await gatewayWait(()=>app.status().active===0);const full=(await request('state')).value,ownerView=(await request('members/'+owner.memberId)).value;assert.deepEqual(ownerView.standard,full.members.find(row=>row.id===owner.memberId).standard);assert.deepEqual(ownerView.standard_12h,full.members.find(row=>row.id===owner.memberId).standard_12h);await request('members/'+owner.memberId+'/usage-limits','PATCH',{expected_version:0,enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'1.00'}]});const limited=(await request('members/'+owner.memberId)).value,status=(await request('members/'+owner.memberId+'/usage-limits')).value;assert.equal(limited.limit_usage[0].remaining_usd,status.usage[0].remaining_usd);assert.equal(limited.limit_usage[0].used_usd,status.usage[0].used_usd);
  for(let i=0;i<26;i++)await request('keys','POST',{memberId:owner.memberId,expires:null});const metadata=(await request('members/'+owner.memberId)).value;assert.equal(metadata.key_count,27);assert.ok(!JSON.stringify(metadata).includes(owner.secret));
  const first=(await request('members/'+owner.memberId+'/keys')).value,last=(await request('members/'+owner.memberId+'/keys?page=2')).value;assert.equal(first.rows.length,25);assert.equal(last.rows.length,2);assert.equal(first.pagination.total,27);assert.ok([...first.rows,...last.rows].some(row=>row.full_key===owner.secret));assert.ok([...first.rows,...last.rows].every(row=>row.member_id===owner.memberId));assert.ok(!JSON.stringify(first).includes(employees[2].secret));
  assert.equal((await request('keys?q='+encodeURIComponent(owner.secret.slice(0,11))+'&member_id='+owner.memberId)).value.pagination.total,1);
}));

test('PostgreSQL member metadata pages never scan usage and keep configuration available before statistics',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  await createPostgresMemberLimits(store).save(member.memberId,{expected_version:0,enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'20'}]});
  const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),all=app.queryStore.all.bind(app.queryStore),get=app.queryStore.get.bind(app.queryStore);let forbidden=0;
  const check=sql=>{if(/\bharbor\.(requests|request_prices)\b/.test(sql)){forbidden++;throw Error('metadata attempted a usage scan');}};
  const read=async path=>{const r=await fetch(origin+path,{headers:{cookie}});return {status:r.status,value:await r.json()};};
  app.queryStore.all=(sql,...args)=>{check(sql);return all(sql,...args);};app.queryStore.get=(sql,...args)=>{check(sql);return get(sql,...args);};
  let metadata;try{const result=await read('/api/admin/members?view=list');assert.equal(result.status,200);metadata=result.value;assert.equal(forbidden,0);assert.equal(metadata.usagePending,true);assert.equal(metadata.rows.length,1);const row=metadata.rows[0];assert.equal(row.id,member.memberId);assert.equal(row.usage_limits.enabled,true);assert.equal(row.key_count,1);assert.equal(row.registered_devices,0);for(const field of ['standard','standard_12h','limit_usage','last_seen','used_devices','unverified_requests'])assert.equal(Object.hasOwn(row,field),false);}
  finally{app.queryStore.all=all;app.queryStore.get=get;}
  const full=await read('/api/admin/members');assert.equal(full.status,200);assert.equal(Object.hasOwn(full.value,'usagePending'),false);assert.deepEqual(full.value.rows[0].usage_limits,metadata.rows[0].usage_limits);assert.equal(full.value.rows[0].limit_usage.length,1);assert.equal(full.value.rows[0].standard.currency,'USD');
  for(const path of ['/api/admin/members?view=unknown','/api/admin/members?view=list&view=list','/api/admin/keys?view=list','/api/admin/members/selection?view=list'])assert.equal((await read(path)).status,400);
}));

test('PostgreSQL all-filter selection fixes IDs and configuration versions before batch preview',integration,async t=>withGateway(t,async({store,password,open})=>{
  const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),request=async(path,method='GET',body)=>{const r=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,value:await r.json()};};
  await request('member-types/usage-b','PATCH',{expected_version:0,defaults:{allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]}}});const create=async name=>(await request('members','POST',{name,quota:null,expires:null,type_id:'usage-b',expected_type_version:1,expected_template_version:1})).value,first=await create('固定筛选一'),second=await create('固定筛选二'),selection=(await request('members/selection?q='+encodeURIComponent('固定筛选')+'&type_id=usage-b')).value;assert.equal(selection.total,2);
  await create('固定筛选新增');const input={scope:{member_ids:selection.members.map(row=>row.id),member_versions:selection.members},type_id:'usage-b',expected_type_version:1,models:true,limits:false,classify:false,overwrite_exceptions:false},preview=await request('member-config-batches/preview','POST',input);assert.equal(preview.status,201);assert.equal(preview.value.members,2);assert.ok(selection.members.every(row=>Number.isSafeInteger(row.access_version)));for(const access_version of [-1,'0'])assert.equal((await request('member-config-batches/preview','POST',{...input,scope:{...input.scope,member_versions:selection.members.map(row=>({...row,access_version}))}})).status,400);
  await request('members/'+first.memberId+'/type','PATCH',{type_id:'usage-f',expected_type_version:0,expected_usage_type_version:1,apply_defaults:false});const before=(await store.get('SELECT COUNT(*) n FROM harbor.member_config_batches')).n;assert.equal((await request('member-config-batches/preview','POST',input)).status,409);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.member_config_batches')).n,before);assert.equal((await request('member-config-batches/preview','POST',{...input,scope:{...input.scope,member_versions:[selection.members[0],selection.members[0]]}})).status,400);
  const removed=await request('members/'+second.memberId,'DELETE');assert.equal(removed.status,200);assert.equal((await request('members/'+second.memberId+'/keys')).status,404);
}));

test('R47 paged member and Key twelve-hour usage excludes the observation instant and future records exactly like state',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member,legacy}=await legacyMember(t,store),at=Date.parse('2026-11-03T12:00:00+08:00'),person=await store.authenticate(member.secret,{at});
  for(const offset of [-12*3600000-1,-12*3600000,-1,0,1]){const request=await store.observe(person,'synthetic','synthetic','gpt-6.1-sol',{at:at+offset-1000});await store.finish(request,{input:100,output:20,cached:10,usageKnown:true},at+offset);}
  const state=createPostgresStateQueries(store,{clock:()=>at,encryptionKey:legacy.encryptionKey});try{const old=await state.read(),queries=createPostgresMemberQueries(store,{clock:()=>at,encryptionKey:legacy.encryptionKey}),employee=await queries.detail(member.memberId),key=(await queries.keys(memberQuery(new URLSearchParams(),{keys:true}))).rows[0];assert.equal(employee.standard.requests,5);assert.equal(employee.standard_12h.requests,2);assert.deepEqual(employee.standard_12h,old.members[0].standard_12h);assert.deepEqual(key.standard_12h,old.keys[0].standard_12h);}finally{await state.close();}
});

test('PostgreSQL type template history is paged and restoring creates a new version without rewriting employees or old templates',integration,async t=>withGateway(t,async({store,password,open})=>{
  const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),request=async(path,method='GET',body)=>{const r=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:r.status,value:await r.json()};};
  assert.equal((await fetch(origin+'/api/admin/member-types/usage-a/templates')).status,401);const first={allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'2.00'}]}},second={allowed_models:['gpt-6-astra'],usage_limits:{enabled:false,rules:[]}};
  await request('member-types/usage-a','PATCH',{expected_version:0,defaults:first});const employee=(await request('members','POST',{name:'历史恢复合成员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:1,expected_template_version:1})).value,before=await store.get('SELECT * FROM harbor.members WHERE id=$1',employee.memberId),oldTemplate=await store.get("SELECT * FROM harbor.member_type_templates WHERE type_id='usage-a' AND version=1");await request('member-types/usage-a','PATCH',{expected_version:1,defaults:second});
  let history=await request('member-types/usage-a/templates?limit=1');assert.equal(history.status,200);assert.equal(history.value.items[0].version,2);assert.equal(history.value.has_more,true);const previous=await request('member-types/usage-a/templates?after='+history.value.after+'&limit=1');assert.equal(previous.value.items[0].version,1);assert.equal(previous.value.has_more,false);assert.deepEqual(previous.value.items[0].defaults,first);
  for(const path of ['member-types/usage-a/templates?limit=0','member-types/usage-a/templates?after=-1','member-types/usage-a/templates?limit=1&limit=2','member-types/usage-a/templates?secret=x'])assert.equal((await request(path)).status,400);
  for(const payload of [{expected_version:2,restore_version:0},{expected_version:2,restore_version:1,defaults:first},{expected_version:2,restore_version:1,name:'mixed'}])assert.equal((await request('member-types/usage-a','PATCH',payload)).status,400);
  assert.equal((await request('member-types/usage-a','PATCH',{expected_version:1,restore_version:1})).status,409);assert.equal((await request('member-types/usage-a','PATCH',{expected_version:2,restore_version:999})).status,404);
  const restored=await request('member-types/usage-a','PATCH',{expected_version:2,restore_version:1});assert.equal(restored.status,200);assert.equal(restored.value.default_version,3);assert.deepEqual(restored.value.defaults,first);assert.deepEqual(await store.get('SELECT * FROM harbor.members WHERE id=$1',employee.memberId),before);assert.deepEqual(await store.get("SELECT * FROM harbor.member_type_templates WHERE type_id='usage-a' AND version=1"),oldTemplate);assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',employee.keyId)).allowed_models),['gpt-6.1-sol']);
  const events=(await request('history?kind=audit&action=member.type.configure')).value;assert.ok(events.rows.some(row=>row.detail.restored_from===1&&row.detail.template===3));assert.ok(!JSON.stringify(events).includes(employee.secret));
  const run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic template audit failure');return run(sql,...args);};try{assert.equal((await request('member-types/usage-a','PATCH',{expected_version:3,restore_version:2})).status,500);}finally{app.store.run=run;}assert.equal((await store.get("SELECT default_version FROM harbor.member_types WHERE id='usage-a'")).default_version,3);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.member_type_templates WHERE type_id='usage-a'")).n,3);
  await store.run("UPDATE harbor.member_type_templates SET created=$1 WHERE type_id='usage-a' AND version=1",Date.now()-8*86400000);history=await request('member-types/usage-a/templates');assert.ok(history.value.items.every(item=>item.version!==1));assert.equal((await request('member-types/usage-a','PATCH',{expected_version:3,restore_version:1})).status,404);
}));

test('PostgreSQL six usage defaults are explicit, versioned, atomic and inherited without rewriting existing employees',integration,async t=>withGateway(t,async({store,password,open})=>{
  const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),admin=async(path,method='GET',body)=>{const result=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:result.status,value:await result.json()};};
  assert.equal((await fetch(origin+'/api/admin/member-types')).status,401);let catalog=(await admin('member-types')).value.types;assert.deepEqual(catalog.map(t=>t.code),['A','B','C','D','E','F']);assert.ok(catalog.every(t=>!t.configured&&t.defaults===null));assert.ok(catalog.every(t=>!/[高级中级初级]{2}开发/.test(t.name)));
  const count=(await store.get('SELECT COUNT(*) n FROM harbor.members')).n;
  assert.equal((await admin('members','POST',{name:'无默认合成员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:0,expected_template_version:null})).status,409);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.members')).n,count);
  for(const defaults of [{allowed_models:[],usage_limits:{enabled:false,rules:[]}},{allowed_models:['synthetic-no-model'],usage_limits:{enabled:false,rules:[]}},{allowed_models:null,usage_limits:{enabled:true,rules:[]}}])assert.equal((await admin('member-types/usage-a','PATCH',{expected_version:0,defaults})).status,400);
  const created=[];
  for(const [index,type] of catalog.entries()){
    const defaults={allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:true,rules:[{model:'gpt-6.1-sol',hours:index%2?168:24,limit_usd:String(index+1)}]}},configured=await admin('member-types/'+type.id,'PATCH',{expected_version:0,defaults});assert.equal(configured.status,200);assert.equal(configured.value.default_version,1);
    const result=await admin('members','POST',{name:'类型合成员工'+type.code,quota:null,expires:null,type_id:type.id,expected_type_version:1,expected_template_version:1,configuration_source:{models:{kind:'forged'}}});assert.equal(result.status,201);created.push(result.value);
    const owner=await store.get('SELECT * FROM harbor.members WHERE id=$1',result.value.memberId),key=await store.get('SELECT * FROM harbor.keys WHERE id=$1',result.value.keyId);assert.equal(owner.usage_type_id,type.id);assert.equal(owner.configuration_source.models.kind,'type');assert.equal(key.model_source.kind,'employee');assert.equal(key.model_source.template_version,1);assert.deepEqual(JSON.parse(key.allowed_models),['gpt-6.1-sol']);assert.equal(JSON.parse(owner.usage_limits).rules[0].limit_usd,(index+1)+'.00');assert.equal(owner.used,0);
    await store.checkModel(await store.authenticate(result.value.secret),'gpt-6.1-sol');await assert.rejects(store.checkModel(await store.authenticate(result.value.secret),'gpt-6-astra'),{code:'model_not_allowed'});
  }
  const first=created[0],savedBefore=await store.get('SELECT * FROM harbor.members WHERE id=$1',first.memberId);
  const newDefaults={allowed_models:['gpt-6-astra'],usage_limits:{enabled:false,rules:[]}};assert.equal((await admin('member-types/usage-a','PATCH',{expected_version:1,defaults:newDefaults})).status,200);assert.equal((await admin('member-types/usage-a','PATCH',{expected_version:1,defaults:newDefaults})).status,409);
  assert.deepEqual(await store.get('SELECT * FROM harbor.members WHERE id=$1',first.memberId),savedBefore);
  let added=await admin('keys','POST',{memberId:first.memberId,expires:null});assert.equal(added.status,201);assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',added.value.keyId)).allowed_models),['gpt-6.1-sol']);assert.equal((await store.get('SELECT model_source FROM harbor.keys WHERE id=$1',added.value.keyId)).model_source.template_version,1);
  assert.equal((await admin('members','POST',{name:'旧预览合成员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:1,expected_template_version:1})).status,409);
  const latest=await admin('members','POST',{name:'新默认合成员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:2,expected_template_version:2});assert.equal(latest.status,201);assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',latest.value.keyId)).allowed_models),['gpt-6-astra']);
  assert.equal((await admin('keys/'+first.keyId,'PATCH',{allowed_models:null,expected_model_policy_version:0})).status,200);assert.equal((await store.get('SELECT model_source FROM harbor.keys WHERE id=$1',first.keyId)).model_source.kind,'custom');added=await admin('keys','POST',{memberId:first.memberId,expires:null});assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',added.value.keyId)).allowed_models),['gpt-6.1-sol']);
  const run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic type audit failure');return run(sql,...args);};try{assert.equal((await admin('member-types/usage-b','PATCH',{expected_version:1,defaults:newDefaults})).status,500);}finally{app.store.run=run;}
  assert.equal((await store.get("SELECT version FROM harbor.member_types WHERE id='usage-b'")).version,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.member_type_templates WHERE type_id='usage-b'")).n,1);
  catalog=(await admin('member-types')).value.types;assert.equal(catalog[0].members,2);assert.equal(catalog[1].default_version,1);
}));

test('PostgreSQL usage type custom components, classification-only changes and request snapshots preserve permissions and historical ownership',integration,async t=>withGateway(t,async({store,password,open})=>{
  let calls=0;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),admin=async(path,method,body)=>{const result=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},body:JSON.stringify(body)});return {status:result.status,value:await result.json()};};
  const defaults={allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]}};assert.equal((await admin('member-types/usage-a','PATCH',{expected_version:0,defaults})).status,200);
  const custom=await admin('members','POST',{name:'自定义合成员工',quota:null,expires:null,type_id:'usage-f',expected_type_version:0,models_mode:'custom',allowed_models:['gpt-6.1-sol'],limits_mode:'custom',usage_limits:{enabled:false,rules:[]}});assert.equal(custom.status,201);assert.equal((await store.get('SELECT configuration_source FROM harbor.members WHERE id=$1',custom.value.memberId)).configuration_source.models.kind,'custom');
  const created=await admin('members','POST',{name:'历史类型合成员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:1,expected_template_version:1});assert.equal(created.status,201);
  const send=()=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+created.value.secret,'content-type':'application/json','thread-id':'synthetic-type-history','x-usage-type':'F'},body:JSON.stringify({model:'gpt-6.1-sol',input:'类型快照合成问题',metadata:{usage_type_id:'usage-f'}})});let result=await send();assert.equal(result.status,200);await result.json();await gatewayWait(()=>app.status().active===0);
  const before=await store.get('SELECT usage_limits,used,default_models FROM harbor.members WHERE id=$1',created.value.memberId),old=await store.get('SELECT usage_type_id,usage_type_code,usage_type_name,usage_type_version FROM harbor.requests WHERE member_id=$1',created.value.memberId);assert.equal(old.usage_type_id,'usage-a');assert.equal(old.usage_type_code,'A');assert.equal(old.usage_type_name,'复杂研发');
  assert.equal((await admin('members/'+created.value.memberId+'/type','PATCH',{type_id:'usage-f',expected_type_version:0,expected_usage_type_version:1,apply_defaults:false})).status,200);assert.deepEqual(await store.get('SELECT usage_limits,used,default_models FROM harbor.members WHERE id=$1',created.value.memberId),before);result=await send();assert.equal(result.status,200);await result.json();await gatewayWait(()=>app.status().active===0);
  const requests=await store.all('SELECT usage_type_id,usage_type_code,usage_type_name,usage_type_version FROM harbor.requests WHERE member_id=$1 ORDER BY query_sequence',created.value.memberId);assert.deepEqual(requests[0],old);assert.equal(requests[1].usage_type_code,'F');assert.equal(requests[1].usage_type_version,2);assert.equal(calls,2);
  assert.equal((await admin('member-types/usage-f','PATCH',{expected_version:0,enabled:false})).status,200);const key=await admin('keys','POST',{memberId:created.value.memberId,expires:null});assert.equal(key.status,201);assert.deepEqual(JSON.parse((await store.get('SELECT allowed_models FROM harbor.keys WHERE id=$1',key.value.keyId)).allowed_models),['gpt-6.1-sol']);
  assert.equal((await admin('members','POST',{name:'停用类型合成员工',quota:null,expires:null,type_id:'usage-f',expected_type_version:1,models_mode:'custom',allowed_models:null,limits_mode:'custom',usage_limits:{enabled:false,rules:[]}})).status,400);
}));

test('PostgreSQL typed employee creation rolls back defaults and audits when Key encryption fails and persisted defaults survive restart',integration,async t=>withGateway(t,async({store,open})=>{
  let app=await open(),admin=createPostgresMemberAdmin(app.store),typePath='/api/admin/member-types/usage-c',defaults={allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'3.00'}]}};await admin.handle(typePath,'PATCH',{expected_version:0,defaults});
  const before=await store.get('SELECT (SELECT COUNT(*) FROM harbor.members) members,(SELECT COUNT(*) FROM harbor.keys) keys,(SELECT COUNT(*) FROM harbor.audit) audit');await assert.rejects(admin.handle('/api/admin/members','POST',{name:'应回滚合成员工',quota:null,expires:null,type_id:'usage-c',expected_type_version:1,expected_template_version:1}),{code:'key_configuration'});assert.deepEqual(await store.get('SELECT (SELECT COUNT(*) FROM harbor.members) members,(SELECT COUNT(*) FROM harbor.keys) keys,(SELECT COUNT(*) FROM harbor.audit) audit'),before);
  await app.close();app=await open();admin=createPostgresMemberAdmin(app.store);const catalog=(await admin.handle('/api/admin/member-types','GET')).value.types,type=catalog.find(row=>row.id==='usage-c');assert.equal(type.default_version,1);assert.deepEqual(type.defaults,defaults);
}));

test('PostgreSQL changing usage type during a real in-flight request keeps its accepted snapshot and allows it to finish',integration,async t=>withGateway(t,async({store,password,open})=>{
  let entered=false,release,calls=0;const held=new Promise(resolve=>{release=resolve;}),app=await open({responsesTransport:controlledResponses(async signal=>{calls++;if(calls===1){entered=true;await held;assert.equal(signal.aborted,false);}})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),admin=async(path,body)=>fetch(origin+'/api/admin/'+path,{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(body)});
  const created=await (await admin('members',{name:'在途类型合成员工',quota:null,expires:null,type_id:'usage-a',expected_type_version:0,models_mode:'custom',allowed_models:['gpt-6.1-sol'],limits_mode:'custom',usage_limits:{enabled:false,rules:[]}})).json();const send=()=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+created.secret,'content-type':'application/json','thread-id':'inflight-type-test'},body:JSON.stringify({model:'gpt-6.1-sol',input:'在途类型合成问题'})}),pending=send();
  try{await gatewayWait(()=>entered);const changed=await fetch(origin+'/api/admin/members/'+created.memberId+'/type',{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({type_id:'usage-b',expected_type_version:0,expected_usage_type_version:1,apply_defaults:false})});assert.equal(changed.status,200);release();const first=await pending;assert.equal(first.status,200);await first.json();const next=await send();assert.equal(next.status,200);await next.json();assert.equal(calls,2);assert.deepEqual((await store.all('SELECT usage_type_code FROM harbor.requests WHERE member_id=$1 ORDER BY query_sequence',created.memberId)).map(row=>row.usage_type_code),['A','B']);}finally{release();await pending;}
}));

async function listenGateway(app){await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));return 'http://127.0.0.1:'+app.server.address().port;}
async function gatewayLogin(origin,password){const response=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password})});assert.equal(response.status,200);return response.headers.get('set-cookie').split(';')[0];}
const gatewaySend=(origin,secret)=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json','thread-id':'synthetic-runtime-client'},body:JSON.stringify({model:'gpt-6.1-sol',input:'服务入口合成问题',stream:false}),signal:AbortSignal.timeout(5000)});

for(const commit of [true,false])test('PostgreSQL Responses request-boundary handoff '+(commit?'commits':'rolls back')+' while other sessions keep working',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0,releaseOld,releaseCommit,applying=false;const oldGate=new Promise(resolve=>{releaseOld=resolve;}),commitGate=new Promise(resolve=>{releaseCommit=resolve;});
  const app=await open({resources:{corePoolSize:2,queryPoolSize:1,recordingPoolSize:1},responsesTransport:controlledResponses(async()=>{calls++;if(calls===2)await oldGate;})}),origin=await listenGateway(app);
  const first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);
  const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-handoff-target','合成交接目标','synthetic','ready')");
  const old=gatewaySend(origin,member.secret);let next,intent;
  try{
    await gatewayWait(()=>calls===2);intent=app.sessionHandoffs.register({memberId:member.memberId,sessionId:session.id,decisionId:'synthetic-handoff'});
    intent.activate(signal=>app.store.transaction(async()=>{
      assert.equal(signal.aborted,false);assert.equal((await app.store.get("SELECT COUNT(*) n FROM harbor.requests WHERE session_id=$1 AND status IN ('queued','running')",session.id)).n,0);
      await lockPostgresRouting(app.store);await app.store.run("UPDATE harbor.sessions SET account_id='synthetic-handoff-target',route_version=route_version+1 WHERE id=$1",session.id);applying=true;await commitGate;
      if(!commit)throw Error('synthetic handoff rollback');
    }));
    next=gatewaySend(origin,member.secret);await gatewayWait(()=>app.status().sessionHandoffs.waiting===1);assert.equal(calls,2);assert.equal(applying,false);
    const other=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'synthetic-other-session'},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成其他会话'}),signal:AbortSignal.timeout(5000)});assert.equal(other.status,200);await other.json();assert.equal(calls,3);
    // The old model is paused, but it owns no transaction or global route lock.
    assert.equal((await store.get('SELECT 1 n')).n,1);releaseOld();const finished=await old;assert.equal(finished.status,200);await finished.json();await gatewayWait(()=>applying);
    assert.equal(app.status().sessionHandoffs.waiting,1);assert.equal(calls,3);releaseCommit();assert.deepEqual(await intent.done,{state:commit?'completed':'failed'});
    const response=await next;assert.equal(response.status,200);await response.json();await gatewayWait(()=>app.status().active===0);assert.equal(calls,4);
    assert.deepEqual(await store.all('SELECT account_id,route_version,status FROM harbor.requests WHERE session_id=$1 ORDER BY query_sequence',session.id),[
      {account_id:'synthetic-runtime',route_version:0,status:'ok'},
      {account_id:'synthetic-runtime',route_version:0,status:'ok'},
      {account_id:commit?'synthetic-handoff-target':'synthetic-runtime',route_version:commit?1:0,status:'ok'}
    ]);
    assert.deepEqual(app.status().sessionHandoffs,{tickets:0,pending:0,waiting:0,stopped:false});assert.equal(app.status().execution.active,0);assert.equal(app.status().requestBodies.bufferedBytes,0);
  }finally{releaseOld();releaseCommit();intent?.cancel();await Promise.allSettled([old,next]);}
}));

for(const commit of [true,false])test('PostgreSQL Responses group boundary '+(commit?'commits':'rolls back')+' every binding after all actual old requests finish',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0,releaseA,releaseB,releaseCommit,applying=false;const heldA=new Promise(resolve=>{releaseA=resolve;}),heldB=new Promise(resolve=>{releaseB=resolve;}),heldCommit=new Promise(resolve=>{releaseCommit=resolve;});
  const app=await open({resources:{corePoolSize:2,queryPoolSize:1,recordingPoolSize:1},responsesTransport:controlledResponses(async()=>{const call=++calls;if(call===3)await heldA;if(call===4)await heldB;})}),origin=await listenGateway(app),send=thread=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':thread},body:JSON.stringify({model:'gpt-6.1-sol',input:'整组交接合成完整上下文'}),signal:AbortSignal.timeout(5000)});
  for(const thread of ['synthetic-group-a','synthetic-group-b']){const response=await send(thread);assert.equal(response.status,200);await response.json();}await gatewayWait(()=>app.status().active===0);
  const sessions=await store.all('SELECT * FROM harbor.sessions ORDER BY created,id'),ids=sessions.map(s=>s.id);assert.equal(ids.length,2);await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-group-target','合成整组目标','synthetic','ready')");
  const oldA=send('synthetic-group-a');let oldB,nextA,nextB,intent;
  try{
    await gatewayWait(()=>calls===3);oldB=send('synthetic-group-b');await gatewayWait(()=>calls===4);intent=registerRequestHandoffGroup(app.sessionHandoffs,sessions.map((s,i)=>({memberId:member.memberId,sessionId:s.id,decisionId:'synthetic-group-'+i})));
    intent.activate(signal=>app.store.transaction(async()=>{
      assert.equal(signal.aborted,false);assert.equal((await app.store.get("SELECT COUNT(*) n FROM harbor.requests WHERE session_id=ANY($1::text[]) AND status IN ('queued','running')",ids)).n,0);await lockPostgresRouting(app.store);await app.store.run("UPDATE harbor.sessions SET account_id='synthetic-group-target',route_version=route_version+1 WHERE id=ANY($1::text[])",ids);applying=true;await heldCommit;if(!commit)throw Error('synthetic group audit rollback');
    }));
    nextA=send('synthetic-group-a');nextB=send('synthetic-group-b');await gatewayWait(()=>app.status().sessionHandoffs.waiting===2);assert.equal(calls,4);assert.equal(applying,false);
    const other=await send('synthetic-group-unrelated');assert.equal(other.status,200);await other.json();assert.equal(calls,5);assert.equal((await store.get('SELECT 1 n')).n,1);
    releaseA();const first=await oldA;assert.equal(first.status,200);await first.json();await gatewayWait(()=>intent.status().ready===1);assert.equal(applying,false);assert.equal(app.status().sessionHandoffs.waiting,2);
    releaseB();const second=await oldB;assert.equal(second.status,200);await second.json();await gatewayWait(()=>applying);assert.equal(calls,5);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.sessions WHERE id=ANY($1::text[]) AND account_id='synthetic-runtime'",ids)).n,2);assert.equal(app.status().sessionHandoffs.waiting,2);
    releaseCommit();const result=await intent.done;assert.equal(result.state,commit?'completed':'failed');for(const pending of [nextA,nextB]){const response=await pending;assert.equal(response.status,200);await response.json();}await gatewayWait(()=>app.status().active===0);assert.equal(calls,7);
    for(const id of ids)assert.deepEqual(await store.all('SELECT account_id,route_version,status FROM harbor.requests WHERE session_id=$1 ORDER BY query_sequence',id),[{account_id:'synthetic-runtime',route_version:0,status:'ok'},{account_id:'synthetic-runtime',route_version:0,status:'ok'},{account_id:commit?'synthetic-group-target':'synthetic-runtime',route_version:commit?1:0,status:'ok'}]);
    assert.equal(app.status().sessionHandoffs.pending,0);assert.equal(app.status().sessionHandoffs.waiting,0);assert.equal(app.status().execution.active,0);assert.equal(app.status().requestBodies.bufferedBytes,0);
  }finally{releaseA();releaseB();releaseCommit();intent?.cancel();await Promise.allSettled([oldA,oldB,nextA,nextB]);}
}));

test('PostgreSQL gateway handoff capacity rejects invalid server settings and admits a configured complete group',integration,async t=>withGateway(t,async({open})=>{
  for(const maxPending of [0,1.5,200001])await assert.rejects(open({handoffs:{maxPending}}),{code:'session_handoff_configuration'});
  const app=await open({handoffs:{maxPending:2}}),members=Array.from({length:3},(_,i)=>({memberId:'capacity-owner',sessionId:'capacity-session-'+i,decisionId:'capacity-decision-'+i}));assert.throws(()=>registerRequestHandoffGroup(app.sessionHandoffs,members),{code:'session_handoff_busy'});assert.equal(app.status().sessionHandoffs.pending,0);
  const group=registerRequestHandoffGroup(app.sessionHandoffs,members.slice(0,2));group.activate(async()=> 'configured');assert.deepEqual(await group.done,{state:'completed',value:'configured'});assert.equal(app.status().sessionHandoffs.pending,0);
}));

test('PostgreSQL handoff preserves a slow upload identified only by body prompt_cache_key',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0,upload,intent,next;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),initial=await gatewaySend(origin,member.secret);assert.equal(initial.status,200);await initial.json();await gatewayWait(()=>app.status().active===0);
  const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-upload-target','合成上传交接目标','synthetic','ready')");
  const uploaded=new Promise((resolve,reject)=>{upload=http.request(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'}},res=>{res.resume();res.on('end',()=>resolve(res.statusCode));});upload.on('error',reject);});uploaded.catch(()=>{});
  try{
    upload.write('{"model":"gpt-6.1-sol","input":"合成慢上传');await gatewayWait(()=>app.status().preparing===1);
    intent=app.sessionHandoffs.register({memberId:member.memberId,sessionId:session.id,decisionId:'synthetic-body-only'});let applied=0;
    intent.activate(()=>app.store.transaction(async()=>{await lockPostgresRouting(app.store);await app.store.run("UPDATE harbor.sessions SET account_id='synthetic-upload-target',route_version=route_version+1 WHERE id=$1",session.id);applied++;}));
    next=gatewaySend(origin,member.secret);await gatewayWait(()=>app.status().sessionHandoffs.waiting===1);assert.equal(applied,0);assert.equal(calls,1);assert.equal(intent.status().oldRequests,1);
    upload.end('","prompt_cache_key":"synthetic-runtime-client"}');assert.equal(await uploaded,200);const response=await next;assert.equal(response.status,200);await response.json();assert.deepEqual(await intent.done,{state:'completed'});await gatewayWait(()=>app.status().active===0);
    assert.equal(calls,3);assert.equal(applied,1);assert.deepEqual(await store.all('SELECT account_id,route_version FROM harbor.requests WHERE session_id=$1 ORDER BY query_sequence',session.id),[{account_id:'synthetic-runtime',route_version:0},{account_id:'synthetic-runtime',route_version:0},{account_id:'synthetic-upload-target',route_version:1}]);
    assert.deepEqual(app.status().sessionHandoffs,{tickets:0,pending:0,waiting:0,stopped:false});
  }finally{upload.destroy();intent?.cancel();await Promise.allSettled([uploaded,next]);}
}));

const handoffTransport=before=>({...controlledResponses(before),models:async()=>[{slug:'gpt-6.1-sol'}]});
const handoffCall=(origin,cookie,sessionId,method='GET',body,handoffId)=>fetch(origin+'/api/admin/sessions/'+sessionId+'/handoffs'+(handoffId?'/'+handoffId:''),{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});

test('PostgreSQL durable manual handoff accepts a busy session, persists intent and switches only subsequent requests',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,release,checks=0;const wait=new Promise(resolve=>{release=resolve;}),transport=handoffTransport(async()=>{calls++;if(calls===2)await wait;}),app=await open({responsesTransport:{...transport,models:async()=>{checks++;assert.equal(app.store.context.getStore(),undefined);return [{slug:'gpt-6.1-sol'}];}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);
  const session=await store.get('SELECT * FROM harbor.sessions'),empty=await (await handoffCall(origin,cookie,session.id)).json();assert.equal(empty.status,'none');assert.equal(empty.id,null);assert.equal(empty.binding.account_id,'synthetic-runtime');assert.equal(empty.binding.route_version,0);await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-durable-target','合成目标','synthetic','ready')");const old=gatewaySend(origin,member.secret);let next;
  try{
    await gatewayWait(()=>calls===2);assert.equal((await handoffCall(origin,'',session.id)).status,401);const response=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-durable-target',expected_route_version:0});assert.equal(response.status,202);const accepted=await response.json(),saved=await store.get('SELECT * FROM harbor.session_handoffs WHERE id=$1',accepted.handoff_id);assert.equal(saved.status,'waiting');assert.equal(saved.pin_expires,nextWeightReset(saved.requested));assert.equal((await store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',session.id)).account_id,'synthetic-runtime');assert.equal((await (await handoffCall(origin,cookie,session.id)).json()).phase,'waiting');
    next=gatewaySend(origin,member.secret);await gatewayWait(()=>app.status().sessionHandoffs.waiting===1);assert.equal(calls,2);assert.equal(checks,0);release();assert.equal((await old).status,200);const following=await next;assert.equal(following.status,200);await following.json();await gatewayWait(()=>app.status().active===0&&app.status().handoffAdmin.pending===0);
    const final=await (await handoffCall(origin,cookie,session.id)).json();assert.equal(final.status,'completed');assert.equal(final.applied_route_version,1);assert.equal(final.source_account_id,'synthetic-runtime');assert.equal(final.target_account_id,'synthetic-durable-target');assert.equal(checks,1);assert.equal(calls,3);
    assert.deepEqual(await store.all('SELECT account_id,route_version,status FROM harbor.requests WHERE session_id=$1 ORDER BY query_sequence',session.id),[{account_id:'synthetic-runtime',route_version:0,status:'ok'},{account_id:'synthetic-runtime',route_version:0,status:'ok'},{account_id:'synthetic-durable-target',route_version:1,status:'ok'}]);assert.equal((await store.get('SELECT handoff_requires_replay FROM harbor.sessions WHERE id=$1',session.id)).handoff_requires_replay,1);
    const blocked=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'synthetic-runtime-client'},body:JSON.stringify({model:'gpt-6.1-sol',input:[{type:'reasoning',encrypted_content:'synthetic-opaque'}]})});assert.equal(blocked.status,409);assert.equal((await blocked.json()).error.code,'session_context_unsupported');assert.equal(calls,3);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,3);
    // A later routing writer can change the live binding without rewriting the
    // completed decision. One DTO must distinguish those two business facts.
    await store.transaction(async()=>{await lockPostgresRouting(store);await store.run("UPDATE harbor.sessions SET account_id='synthetic-runtime',route_version=2 WHERE id=$1",session.id);});const changed=await (await handoffCall(origin,cookie,session.id)).json();assert.equal(changed.target_account_id,'synthetic-durable-target');assert.equal(changed.applied_route_version,1);assert.equal(changed.binding.account_id,'synthetic-runtime');assert.equal(changed.binding.route_version,2);
    await store.run('UPDATE harbor.session_handoffs SET requested=$1 WHERE id=$2',usageWindow(Date.now()).start-1,accepted.handoff_id);const historical=await (await handoffCall(origin,cookie,session.id)).json();assert.equal(historical.status,'none');assert.equal(historical.binding.route_version,2);assert.equal(historical.target_account_id,undefined);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.session_handoffs')).n,1);
  }finally{release();await Promise.allSettled([old,next]);}
}));

for(const action of ['cancel','replace'])test('PostgreSQL durable handoff '+action+' keeps the original request boundary and does not replay the old model',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,release;const wait=new Promise(resolve=>{release=resolve;}),app=await open({responsesTransport:handoffTransport(async()=>{calls++;if(calls===2)await wait;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);
  const session=await store.get('SELECT * FROM harbor.sessions');for(const target of ['synthetic-durable-a','synthetic-durable-b'])await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES($1,$1,'synthetic','ready')",target);const old=gatewaySend(origin,member.secret);let next;
  try{
    await gatewayWait(()=>calls===2);const accepted=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-durable-a',expected_route_version:0});assert.equal(accepted.status,202);const original=await accepted.json();next=gatewaySend(origin,member.secret);await gatewayWait(()=>app.status().sessionHandoffs.waiting===1);
    if(action==='cancel'){
      const cancelled=await handoffCall(origin,cookie,session.id,'DELETE',null,original.handoff_id);assert.equal(cancelled.status,200);assert.equal((await cancelled.json()).status,'cancelled');assert.equal((await next).status,200);assert.equal(calls,3);assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',session.id)).route_version,0);release();
    }else{
      const replaced=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-durable-b',expected_route_version:0,replace_handoff_id:original.handoff_id});assert.equal(replaced.status,202);await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',original.handoff_id)).status==='superseded');assert.equal(app.status().sessionHandoffs.waiting,1);assert.equal(calls,2);release();assert.equal((await next).status,200);
    }
    assert.equal((await old).status,200);await gatewayWait(()=>app.status().active===0&&app.status().handoffAdmin.pending===0);const current=await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',session.id);assert.deepEqual(current,{account_id:action==='cancel'?'synthetic-runtime':'synthetic-durable-b',route_version:action==='cancel'?0:1});assert.equal(calls,3);assert.deepEqual(app.status().sessionHandoffs,{tickets:0,pending:0,waiting:0,stopped:false});
  }finally{release();await Promise.allSettled([old,next]);}
}));

test('PostgreSQL durable manual handoff resumes after gateway restart and never reapplies a completed decision',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,release;const wait=new Promise(resolve=>{release=resolve;}),app=await open({responsesTransport:handoffTransport(async()=>{calls++;if(calls===2)await wait;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),initial=await gatewaySend(origin,member.secret);assert.equal(initial.status,200);await initial.json();await gatewayWait(()=>app.status().active===0);const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-restart-target','合成恢复目标','synthetic','ready')");const old=gatewaySend(origin,member.secret);
  try{
    await gatewayWait(()=>calls===2);const response=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-restart-target',expected_route_version:0});assert.equal(response.status,202);const {handoff_id}=await response.json(),closed=app.close();release();assert.equal((await old).status,200);assert.equal((await closed).forced,false);assert.equal((await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',handoff_id)).status,'waiting');
    const second=await open({responsesTransport:handoffTransport()}),secondOrigin=await listenGateway(second);await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',handoff_id)).status==='completed');const next=await gatewaySend(secondOrigin,member.secret);assert.equal(next.status,200);await next.json();await second.close();
    const third=await open({responsesTransport:handoffTransport()});assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',session.id)).route_version,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.handoff.completed'")).n,1);await third.close();assert.equal(calls,2);
  }finally{release();await old.catch(()=>{});}
}));

for(const change of ['route','epoch','permission','model','expiry','context','audit'])test('PostgreSQL durable handoff rechecks '+change+' and retains the original binding on rejection',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,release,at=Date.now();const wait=new Promise(resolve=>{release=resolve;}),base=handoffTransport(async()=>{calls++;if(calls===2)await wait;}),app=await open({clock:()=>at,responsesTransport:{...base,models:async()=>[{slug:change==='model'?'synthetic-other-model':'gpt-6.1-sol'}],run:async options=>{const result=await base.run(options);if(change==='context'&&calls===2)result.response.output.push({type:'reasoning',encrypted_content:'synthetic-opaque'});return result;}}}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const cookie=await gatewayLogin(origin,password),first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-recheck-target','合成核验目标','synthetic','ready')");const old=gatewaySend(origin,member.secret),run=app.store.run.bind(app.store);
  try{
    await gatewayWait(()=>calls===2);const response=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-recheck-target',expected_route_version:0});assert.equal(response.status,202);const accepted=await response.json();
    if(change==='route')await store.transaction(async()=>{await lockPostgresRouting(store);await store.run('UPDATE harbor.sessions SET route_version=route_version+1 WHERE id=$1',session.id);});
    if(change==='epoch')await store.run("UPDATE harbor.accounts SET route_epoch=route_epoch+1 WHERE id='synthetic-recheck-target'");
    if(change==='permission')assert.equal((await fetch(origin+'/api/admin/keys/'+member.keyId,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({allowed_models:['gpt-6-astra'],expected_model_policy_version:0})})).status,200);
    if(change==='expiry')at=nextWeightReset(at);
    if(change==='audit')app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit')&&args[2]==='session.handoff.completed')throw Error('synthetic handoff audit failure');return run(sql,...args);};
    release();assert.equal((await old).status,200);await gatewayWait(()=>app.status().handoffAdmin.pending===0&&app.status().active===0);const row=await store.get('SELECT status,reason FROM harbor.session_handoffs WHERE id=$1',accepted.handoff_id);assert.equal(row.status,change==='audit'?'failed':'cancelled');assert.equal(row.reason,{route:'session_route_conflict',epoch:'session_account_changed',permission:'session_permissions_changed',model:'session_model_unsupported',expiry:'session_handoff_expired',context:'session_context_unsupported',audit:'session_handoff_failed'}[change]);assert.deepEqual(await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',session.id),{account_id:'synthetic-runtime',route_version:change==='route'?1:0});assert.equal(calls,2);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.handoff.completed'")).n,0);assert.equal(app.status().sessionHandoffs.pending,0);
  }finally{app.store.run=run;release();await old.catch(()=>{});}
}));

test('PostgreSQL durable handoff registration audit failure rolls back the decision and releases the barrier',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0;const app=await open({responsesTransport:handoffTransport(async()=>{calls++;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-registration-target','合成登记目标','synthetic','ready')");const run=app.store.run.bind(app.store);
  app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit')&&args[2]==='session.handoff.requested')throw Error('synthetic registration audit failure');return run(sql,...args);};
  try{const response=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-registration-target',expected_route_version:0});assert.equal(response.status,500);}finally{app.store.run=run;}
  await gatewayWait(()=>app.status().handoffAdmin.pending===0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.session_handoffs')).n,0);const next=await gatewaySend(origin,member.secret);assert.equal(next.status,200);await next.json();assert.equal(calls,2);assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',session.id)).route_version,0);await gatewayWait(()=>app.status().active===0);assert.equal(app.status().sessionHandoffs.pending,0);
}));

test('PostgreSQL handoff rejects an earlier parallel opaque result even when the latest request is replayable',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,release;const wait=new Promise(resolve=>{release=resolve;}),base=handoffTransport(async()=>{calls++;if(calls===2)await wait;}),app=await open({responsesTransport:{...base,run:async options=>{const ordinal=calls+1,result=await base.run(options);if(ordinal===2)result.response.output.push({type:'reasoning',encrypted_content:'synthetic-parallel-opaque'});return result;}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-parallel-target','合成并行目标','synthetic','ready')");const older=gatewaySend(origin,member.secret);
  try{
    await gatewayWait(()=>calls===2);const latest=await gatewaySend(origin,member.secret);assert.equal(latest.status,200);await latest.json();await gatewayWait(()=>app.status().active===1);const response=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-parallel-target',expected_route_version:0});assert.equal(response.status,202);const accepted=await response.json();release();assert.equal((await older).status,200);await gatewayWait(()=>app.status().active===0&&app.status().handoffAdmin.pending===0);assert.deepEqual(await store.get('SELECT status,reason FROM harbor.session_handoffs WHERE id=$1',accepted.handoff_id),{status:'cancelled',reason:'session_context_unsupported'});assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',session.id)).route_version,0);assert.equal(calls,3);
    const replay=await gatewaySend(origin,member.secret);assert.equal(replay.status,200);await replay.json();await gatewayWait(()=>app.status().active===0);const retry=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-parallel-target',expected_route_version:0});assert.equal(retry.status,202);const second=await retry.json();await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',second.handoff_id)).status==='completed');assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',session.id)).route_version,1);assert.equal(calls,4);
  }finally{release();await older.catch(()=>{});}
}));

test('PostgreSQL cancelling one durable handoff does not await another busy session',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,release,block=false;const wait=new Promise(resolve=>{release=resolve;}),app=await open({responsesTransport:handoffTransport(async()=>{calls++;if(block)await wait;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),send=client=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':client},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成独立会话'}),signal:AbortSignal.timeout(5000)});
  for(const client of ['synthetic-cancel-one','synthetic-cancel-two']){const response=await send(client);assert.equal(response.status,200);await response.json();}await gatewayWait(()=>app.status().active===0);const sessions=await store.all('SELECT id,client_key FROM harbor.sessions ORDER BY id');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-independent-target','合成独立目标','synthetic','ready')");block=true;const one=send('synthetic-cancel-one'),two=send('synthetic-cancel-two');
  try{
    await gatewayWait(()=>calls===4);const accepted=[];for(const session of sessions){const response=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-independent-target',expected_route_version:0});assert.equal(response.status,202);accepted.push(await response.json());}
    const response=await fetch(origin+'/api/admin/sessions/'+sessions[0].id+'/handoffs/'+accepted[0].handoff_id,{method:'DELETE',headers:{cookie},signal:AbortSignal.timeout(1000)});assert.equal(response.status,200);assert.equal((await response.json()).status,'cancelled');assert.equal((await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',accepted[1].handoff_id)).status,'waiting');assert.equal(app.status().sessionHandoffs.pending,1);
    release();assert.equal((await one).status,200);assert.equal((await two).status,200);await gatewayWait(()=>app.status().active===0&&app.status().handoffAdmin.pending===0);assert.equal((await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',accepted[1].handoff_id)).status,'completed');assert.equal(calls,4);
  }finally{release();await Promise.allSettled([one,two]);}
}));

test('PostgreSQL handoff metadata commits before the terminal response so the next sequential call retains capacity',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0,release,held=false,once=true,delivered=false;const wait=new Promise(resolve=>{release=resolve;}),app=await open({maxConcurrent:1,responsesTransport:handoffTransport(async()=>{calls++;})}),origin=await listenGateway(app),run=app.store.run.bind(app.store);
  app.store.run=async(sql,...args)=>{if(once&&sql.startsWith('UPDATE harbor.requests SET handoff_context=')){once=false;held=true;await wait;}return run(sql,...args);};const pending=gatewaySend(origin,member.secret).then(response=>{delivered=true;return response;});
  try{
    await gatewayWait(()=>held);assert.equal(delivered,false);assert.equal((await store.get('SELECT handoff_context FROM harbor.requests')).handoff_context,'unknown');assert.equal((await fetch(origin+'/health')).status,200);release();const first=await pending;assert.equal(first.status,200);await first.json();const second=await gatewaySend(origin,member.secret);assert.equal(second.status,200);await second.json();assert.equal(calls,2);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE handoff_context='replayable'")).n,2);await gatewayWait(()=>app.status().active===0);assert.equal(app.status().requestBodies.leases,0);
  }finally{app.store.run=run;release();await pending.catch(()=>{});}
}));

test('PostgreSQL failed capability metadata stays unknown without losing usage and only a fresh replay permits handoff',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,once=true;const app=await open({responsesTransport:handoffTransport(async()=>{calls++;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{if(once&&sql.startsWith('UPDATE harbor.requests SET handoff_context=')){once=false;throw Error('synthetic capability metadata failure');}return run(sql,...args);};
  try{
    const first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);assert.deepEqual(await store.get('SELECT status,usage_unknown,input,output,handoff_context FROM harbor.requests'),{status:'ok',usage_unknown:0,input:100,output:20,handoff_context:'unknown'});const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-metadata-target','合成元数据目标','synthetic','ready')");
    const refused=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-metadata-target',expected_route_version:0});assert.equal(refused.status,409);assert.equal((await refused.json()).error.code,'session_context_unsupported');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.session_handoffs')).n,0);
    const replay=await gatewaySend(origin,member.secret);assert.equal(replay.status,200);await replay.json();await gatewayWait(()=>app.status().active===0);const accepted=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-metadata-target',expected_route_version:0});assert.equal(accepted.status,202);const {handoff_id}=await accepted.json();await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',handoff_id)).status==='completed');assert.equal(calls,2);assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',session.id)).route_version,1);
  }finally{app.store.run=run;}
}));

for(const fault of ['target','audit'])test('R38 PostgreSQL rejected handoff replacement '+fault+' restores the accepted decision and original waiters',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0,release;const wait=new Promise(resolve=>{release=resolve;}),app=await open({responsesTransport:handoffTransport(async()=>{calls++;if(calls===2)await wait;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),initial=await gatewaySend(origin,member.secret);assert.equal(initial.status,200);await initial.json();await gatewayWait(()=>app.status().active===0);const session=await store.get('SELECT * FROM harbor.sessions');for(const target of ['synthetic-original-target','synthetic-rejected-target'])await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES($1,$1,'synthetic','ready')",target);const old=gatewaySend(origin,member.secret),run=app.store.run.bind(app.store);let next;
  try{
    await gatewayWait(()=>calls===2);const original=await handoffCall(origin,cookie,session.id,'POST',{account_id:'synthetic-original-target',expected_route_version:0});assert.equal(original.status,202);const accepted=await original.json();next=gatewaySend(origin,member.secret);await gatewayWait(()=>app.status().sessionHandoffs.waiting===1);
    if(fault==='audit')app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit')&&args[2]==='session.handoff.requested')throw Error('synthetic replacement audit failure');return run(sql,...args);};
    const response=await handoffCall(origin,cookie,session.id,'POST',{account_id:fault==='target'?'synthetic-missing-target':'synthetic-rejected-target',expected_route_version:0,replace_handoff_id:accepted.handoff_id});assert.equal(response.status,fault==='target'?400:500);app.store.run=run;
    assert.equal((await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',accepted.handoff_id)).status,'waiting');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.session_handoffs')).n,1);assert.equal((await (await handoffCall(origin,cookie,session.id)).json()).id,accepted.handoff_id);assert.equal(app.status().sessionHandoffs.waiting,1);assert.equal(calls,2);
    release();assert.equal((await old).status,200);const result=await next;assert.equal(result.status,200);await result.json();await gatewayWait(()=>app.status().active===0&&app.status().handoffAdmin.pending===0);assert.deepEqual(await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',session.id),{account_id:'synthetic-original-target',route_version:1});assert.equal((await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',accepted.handoff_id)).status,'completed');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.handoff.completed'")).n,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.handoff.superseded'")).n,0);assert.equal(calls,3);assert.deepEqual(app.status().sessionHandoffs,{tickets:0,pending:0,waiting:0,stopped:false});
  }finally{app.store.run=run;release();await Promise.allSettled([old,next]);}
}));

test('R39 PostgreSQL cancellation between registration callback and real COMMIT persists a terminal decision before returning error',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),gate=createSessionRequestHandoff(),sessionId=randomUUID(),at=Date.now();
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-commit-source','合成提交源','synthetic','ready'),('synthetic-commit-target','合成提交目标','synthetic','ready')");
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,'synthetic-commit-source','responses',$3,$3)",sessionId,member.memberId,at);
  let captured,releaseCommit,held=false,transactions=0,first=true;const commitGate=new Promise(resolve=>{releaseCommit=resolve;}),register=gate.register;gate.register=value=>(captured=register(value));const service=createPostgresSessionHandoffs(store,{gate,validateTarget:async()=>true}),transaction=store.transaction.bind(store);
  store.transaction=(operation,options)=>{transactions++;return transaction(async current=>{const value=await operation(current);if(first){first=false;held=true;await commitGate;}return value;},options);};
  const path='/api/admin/sessions/'+sessionId+'/handoffs',request=service.handle(path,'POST',{account_id:'synthetic-commit-target',expected_route_version:0});request.catch(()=>{});
  try{
    await gatewayWait(()=>held);assert.equal(transactions,1);captured.cancel();assert.deepEqual(await captured.done,{state:'cancelled'});
    // The watch callback was subscribed before this await. It must be paused
    // on registration visibility, before even starting a finalizer transaction.
    assert.equal(transactions,1);assert.equal(service.status().pending,1);assert.equal(gate.status().pending,0);releaseCommit();await assert.rejects(request,{code:'session_handoff_conflict'});assert.equal(service.status().pending,0);
    const firstRow=await store.get('SELECT id,status FROM harbor.session_handoffs');assert.equal(firstRow.status,'cancelled');assert.equal((await service.handle(path,'GET')).value.status,'cancelled');assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',sessionId)).route_version,0);
    store.transaction=transaction;const accepted=await service.handle(path,'POST',{account_id:'synthetic-commit-target',expected_route_version:0});assert.equal(accepted.status,202);await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',accepted.value.handoff_id)).status==='completed');await service.close();
    const recovered=createPostgresSessionHandoffs(store,{gate:createSessionRequestHandoff(),validateTarget:async()=>true});try{await recovered.recover();assert.equal(recovered.status().pending,0);assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',sessionId)).route_version,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.handoff.completed'")).n,1);}finally{await recovered.close();}
  }finally{releaseCommit();await request.catch(()=>{});store.transaction=transaction;await service.close();}
});

test('PostgreSQL a handoff aborted during model verification retains its real waiting-timeout reason',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),sessionId=randomUUID(),at=Date.now();
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-wait-source','合成等待源','synthetic','ready'),('synthetic-wait-target','合成等待目标','synthetic','ready')");await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,'synthetic-wait-source','responses',$3,$3)",sessionId,member.memberId,at);
  // Assert the cancellation outcome, not elapsed wall time. Keep the bounded
  // timer real here so PostgreSQL pool cleanup timers are never globally faked.
  const gate=createSessionRequestHandoff({waitMs:10,pendingMs:1000}),register=gate.register;let intent,started;const checking=new Promise(resolve=>{started=resolve;});gate.register=value=>(intent=register(value));const service=createPostgresSessionHandoffs(store,{gate,validateTarget:async(_account,_model,signal)=>new Promise((_resolve,reject)=>{signal.addEventListener('abort',()=>reject(Error('synthetic cancelled directory read')),{once:true});started();})});let ticket;
  try{
    const accepted=await service.handle('/api/admin/sessions/'+sessionId+'/handoffs','POST',{account_id:'synthetic-wait-target',expected_route_version:0});assert.equal(accepted.status,202);await checking;ticket=gate.arrive(member.memberId);const waiting=ticket.bind(sessionId);await assert.rejects(waiting,{code:'session_handoff_timeout'});assert.deepEqual(await intent.done,{state:'wait_timeout'});await gatewayWait(()=>service.status().pending===0);
    assert.deepEqual(await store.get('SELECT status,reason FROM harbor.session_handoffs WHERE id=$1',accepted.value.handoff_id),{status:'cancelled',reason:'wait_timeout'});assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',sessionId)).route_version,0);ticket.close();assert.equal(gate.status().waiting,0);
  }finally{ticket?.close();await service.close();}
});

for(const phase of ['checking','applying'])test('PostgreSQL handoff '+phase+' exposes the actual cancellation boundary before binding commits',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),sessionId=randomUUID(),at=Date.now(),gate=createSessionRequestHandoff();
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('phase-source','合成阶段源','synthetic','ready'),('phase-target','合成阶段目标','synthetic','ready')");await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,'phase-source','responses',$3,$3)",sessionId,member.memberId,at);
  let entered,release;const started=new Promise(resolve=>{entered=resolve;}),held=new Promise(resolve=>{release=resolve;}),run=store.run.bind(store),service=createPostgresSessionHandoffs(store,{gate,validateTarget:async(_account,_model,signal)=>{if(phase!=='checking')return true;return new Promise((_resolve,reject)=>{signal.addEventListener('abort',()=>reject(Error('synthetic directory cancellation')),{once:true});entered();});}});
  if(phase==='applying')store.run=async(sql,...args)=>{if(sql.startsWith('UPDATE harbor.sessions SET account_id=')){entered();await held;}return run(sql,...args);};
  try{
    const path='/api/admin/sessions/'+sessionId+'/handoffs',accepted=await service.handle(path,'POST',{account_id:'phase-target',expected_route_version:0});assert.equal(accepted.status,202);await started;const current=(await service.handle(path,'GET')).value;assert.equal(current.phase,phase);assert.equal(current.binding.account_id,'phase-source');assert.equal(current.binding.route_version,0);
    if(phase==='checking'){const cancelled=await service.handle(path+'/'+accepted.value.handoff_id,'DELETE');assert.equal(cancelled.value.status,'cancelled');assert.equal(cancelled.value.reason,'cancelled');assert.equal(cancelled.value.binding.account_id,'phase-source');assert.equal(gate.status().pending,0);}else{await assert.rejects(service.handle(path+'/'+accepted.value.handoff_id,'DELETE'),{status:409,code:'session_route_conflict'});release();await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',accepted.value.handoff_id)).status==='completed');assert.equal((await service.handle(path,'GET')).value.binding.account_id,'phase-target');}
  }finally{release();store.run=run;await service.close();}
});

test('PostgreSQL application boots real auth/static/request/admin paths and isolates saturated query and recording pools from inference',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app);
  assert.equal((await fetch(origin+'/health')).status,200);assert.equal((await fetch(origin+'/ready')).status,200);
  assert.equal((await fetch(origin+'/',{headers:{origin:'http://invalid.example'}})).status,403);
  let response=await fetch(origin+'/');assert.equal(response.status,200);assert.ok((await response.text()).includes('Harbor'));assert.equal(response.headers.get('cache-control'),'no-store');
  response=await fetch(origin+'/api/admin/state',{headers:{authorization:'Bearer '+member.secret}});assert.equal(response.status,401);
  const cookie=await gatewayLogin(origin,password),headers={cookie};response=await gatewaySend(origin,member.secret);assert.equal(response.status,200);const requestId=response.headers.get('x-harbor-request');assert.equal((await response.json()).id,'synthetic-runtime-response');
  await app.buffer.flush();response=await fetch(origin+'/api/admin/requests/'+requestId+'/recording',{headers});const recording=await response.json();assert.equal(response.status,200);assert.equal(recording.state,'complete');assert.ok(recording.events.some(e=>e.kind==='user'));assert.ok(recording.events.some(e=>e.kind==='assistant'));
  response=await fetch(origin+'/api/admin/state',{headers});const state=await response.json();assert.equal(state.keys.find(k=>k.id===member.keyId).full_key,member.secret);assert.equal(state.totals.requests,1);assert.equal(state.endpoint,origin+'/v1');
  response=await fetch(origin+'/api/admin/members',{method:'POST',headers:{...headers,'content-type':'application/json'},body:JSON.stringify({name:'服务入口新增合成员工',quota:null,expires:null,allowed_models:['gpt-6.1-sol']})});assert.equal(response.status,201);const added=await response.json();assert.equal((await store.authenticate(added.secret)).id,added.memberId);
  const queryConnection=await app.queryStore.pool.connect();let slow;
  try{slow=fetch(origin+'/api/admin/state',{headers});await gatewayWait(()=>app.queryStore.status().waiting>0);response=await gatewaySend(origin,member.secret);assert.equal(response.status,200);await response.json();assert.equal(calls,2);}
  finally{queryConnection.release();if(slow)assert.equal((await slow).status,200);}
  await app.buffer.flush();const recordingConnection=await app.recordingStore.pool.connect();let flushing;
  try{response=await gatewaySend(origin,member.secret);assert.equal(response.status,200);const lastId=response.headers.get('x-harbor-request');await response.json();flushing=app.buffer.flush();await gatewayWait(()=>app.recordingStore.status().waiting>0);response=await fetch(origin+'/api/admin/requests/'+lastId+'/recording',{headers});assert.equal(response.status,200);assert.equal((await response.json()).state,'pending');assert.equal(calls,3);}
  finally{recordingConnection.release();if(flushing)await flushing;}
  response=await fetch(origin+'/api/admin/status',{headers});const status=await response.json();assert.equal(response.status,200);assert.deepEqual(Object.keys(status.pools).sort(),['core','query','recording']);assert.ok(!JSON.stringify(status).includes('synthetic-runtime-password'));assert.equal(status.active,0);
  response=await fetch(origin+'/api/auth/logout',{method:'POST',headers});assert.equal(response.status,200);assert.equal((await fetch(origin+'/api/admin/state',{headers})).status,401);
}));

test('PostgreSQL application enforces one database owner across data directories and releases locks after close',integration,async t=>withGateway(t,async({root,open})=>{
  const first=await open();await assert.rejects(open(),{code:'EADDRINUSE'});await assert.rejects(open({dataDir:join(root,'other-data')}),{code:'gateway_already_running'});
  const result=await first.close();assert.equal(result.forced,false);assert.equal(result.ownershipLost,false);const next=await open();assert.equal(next.status().ownership,true);await next.close();
}));

test('PostgreSQL application refuses missing/edited/future schemas, old SQLite directories and lost encryption keys without replacing data',integration,async t=>withGateway(t,async({store,root,data,open})=>{
  await assert.rejects(open(),{code:'database_migration_required'});assert.equal((await store.get("SELECT to_regclass('harbor.members') relation")).relation,null);
  await store.migrate();await store.initializePricing();const initialPassword='synthetic-initial-config-recovery',initial={port:43128,codexBinary:'codex',adminPasswordHash:passwordHash(initialPassword)};writeFileSync(join(data,'settings.json.pending'),JSON.stringify(initial),{mode:0o600});await assert.rejects(open({config:undefined}),{code:'gateway_configuration'});assert.equal(existsSync(join(data,'管理密码.txt')),false);writeFileSync(join(data,'管理密码.txt'),'Harbor 管理密码（请勿分享）\r\n\r\n'+initialPassword+'\r\n',{mode:0o600});const app=await open({config:undefined});assert.equal(app.config.port,43128);assert.equal(existsSync(join(data,'settings.json.pending')),false);assert.ok(readFileSync(join(data,'管理密码.txt'),'utf8').includes(initialPassword));assert.equal(statSync(join(data,'settings.json')).mode&0o777,0o600);assert.equal(statSync(join(data,'管理密码.txt')).mode&0o777,0o600);await app.close();
  const rows=await store.all('SELECT * FROM harbor.schema_migrations ORDER BY version');await store.run("UPDATE harbor.schema_migrations SET checksum='edited' WHERE version=1");await assert.rejects(open(),{code:'database_migration_conflict'});await store.run('UPDATE harbor.schema_migrations SET checksum=$1 WHERE version=1',rows[0].checksum);
  const editedKnown=rows.at(-1).version+1;await store.run("INSERT INTO harbor.schema_migrations VALUES($1,'edited-known',1)",editedKnown);await assert.rejects(open(),{code:'database_migration_conflict'});await store.run('DELETE FROM harbor.schema_migrations WHERE version=$1',editedKnown);
  const supported=await store.migrate({recordingContextIndex:true}),future=supported.version+1;await store.run("INSERT INTO harbor.schema_migrations VALUES($1,'future',1)",future);await assert.rejects(open(),{code:'database_migration_required'});await store.run('DELETE FROM harbor.schema_migrations WHERE version=$1',future);
  writeFileSync(join(data,'gateway.sqlite'),'synthetic legacy marker',{mode:0o600});await assert.rejects(open(),{code:'database_migration_required'});assert.equal(readFileSync(join(data,'gateway.sqlite'),'utf8'),'synthetic legacy marker');rmSync(join(data,'gateway.sqlite'));
  const savedKey=readFileSync(join(data,'key-encryption.key'));rmSync(join(data,'key-encryption.key'));await assert.rejects(open(),{code:'encryption_material_missing'});assert.equal(existsSync(join(data,'key-encryption.key')),false);writeFileSync(join(data,'key-encryption.key'),savedKey,{mode:0o600});
  const final=await open();await final.close();assert.ok(existsSync(join(root,'data','settings.json')));
},{migrated:false,legacy:false}));

test('PostgreSQL startup restores persisted metering before marking abandoned work interrupted, without replaying models',integration,async t=>withGateway(t,async({store,data,member,open})=>{
  const person=await store.authenticate(member.secret),recovered=await store.observe(person,'synthetic','synthetic','gpt-6.1-sol'),abandoned=await store.observe(person,'synthetic','synthetic','gpt-6.1-sol'),recordKey=randomBytes(32),ended=Date.now()-1000;
  writeFileSync(join(data,'recording-encryption.key'),recordKey,{mode:0o600});const recordings=new RecordingStore(store,{encryptionKey:recordKey});await recordings.start(abandoned);await recordings.append(abandoned,[{sourceKey:'synthetic-prefix',kind:'user',provenance:'client',content:{text:'异常退出前已保存的合成问题'}}]);
  const spool=await MeteringRecovery.create({finish:async()=>{throw Error('synthetic shutdown database outage');}},{encryptionKey:readFileSync(join(data,'key-encryption.key')),directory:join(data,'metering-spool'),flushMs:10000});spool.settle(recovered,{input:12,output:3,cached:0,usageKnown:true},ended);await spool.flush();await spool.close();
  let calls=0;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})});assert.equal(calls,0);assert.equal(app.status().interruptedOnStartup,1);
  const restored=await store.get('SELECT * FROM harbor.requests WHERE id=$1',recovered);assert.equal(restored.status,'ok');assert.equal(restored.ended,ended);assert.equal(restored.input,12);
  const interrupted=await store.get('SELECT * FROM harbor.requests WHERE id=$1',abandoned);assert.equal(interrupted.status,'interrupted');assert.equal(interrupted.usage_unknown,1);assert.equal(interrupted.recording_state,'partial');assert.equal(interrupted.recording_reason,'process_interrupted');assert.equal((await recordings.read(abandoned)).events.length,1);
  assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member.memberId)).used,15);await app.close();const again=await open();assert.equal(again.status().interruptedOnStartup,0);assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member.memberId)).used,15);
}));

test('PostgreSQL startup fails closed on corrupt critical recovery and can retry after authenticated file repair',integration,async t=>withGateway(t,async({store,data,member,open})=>{
  const person=await store.authenticate(member.secret),request=await store.observe(person,'synthetic','synthetic','gpt-6.1-sol');
  const spool=await MeteringRecovery.create({finish:async()=>{throw Error('synthetic outage');}},{encryptionKey:readFileSync(join(data,'key-encryption.key')),directory:join(data,'metering-spool'),flushMs:10000});spool.settle(request,{input:2,output:1,usageKnown:true},Date.now()-100);await spool.flush();await spool.close();
  const file=join(data,'metering-spool',readdirSync(join(data,'metering-spool'))[0]),bytes=readFileSync(file);writeFileSync(file,Buffer.alloc(40),{mode:0o600});await assert.rejects(open(),{code:'metering_recovery_required'});assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',request)).status,'queued');assert.equal(existsSync(file),true);
  writeFileSync(file,bytes,{mode:0o600});const app=await open();assert.equal(app.status().interruptedOnStartup,0);assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',request)).status,'ok');
}));

test('PostgreSQL shutdown waits for accepted inference and records timeout interruption before releasing ownership',integration,async t=>withGateway(t,async({store,member,open})=>{
  let entered=false,release;const gate=new Promise(resolve=>{release=resolve;}),app=await open({drainTimeoutMs:2000,responsesTransport:controlledResponses(async()=>{entered=true;await gate;})}),origin=await listenGateway(app),request=gatewaySend(origin,member.secret);await gatewayWait(()=>entered);
  let closed=false;const closing=app.close().then(result=>{closed=true;return result;});assert.equal(app.status().stopping,true);await new Promise(resolve=>setImmediate(resolve));assert.equal(closed,false);release();const normal=await request;assert.equal(normal.status,200);await normal.json();const result=await closing;assert.equal(result.forced,false);assert.equal(result.metering.queued,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE status='ok'")).n,1);
  entered=false;const next=await open({drainTimeoutMs:100,responsesTransport:controlledResponses(signal=>new Promise((resolve,reject)=>{entered=true;if(signal.aborted)reject(signal.reason);else signal.addEventListener('abort',()=>reject(signal.reason),{once:true});}))}),nextOrigin=await listenGateway(next),pending=gatewaySend(nextOrigin,member.secret).catch(()=>null);await gatewayWait(()=>entered);const forced=await next.close();await pending;assert.equal(forced.forced,true);
  const interrupted=await store.get("SELECT * FROM harbor.requests WHERE status='interrupted'");assert.ok(interrupted);assert.equal(interrupted.usage_unknown,1);assert.equal(interrupted.recording_state,'partial');assert.equal(interrupted.recording_reason,'process_interrupted');
  const retry=await open();assert.equal(retry.status().interruptedOnStartup,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,2);
}));


test('PostgreSQL ownership loss stops admission and readiness while releasing resources for a later restart',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app);
  const owner=await store.get("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND granted");assert.ok(owner);
  await store.get('SELECT pg_terminate_backend($1) terminated',owner.pid);await gatewayWait(()=>!app.status().ownership);
  assert.equal((await fetch(origin+'/health')).status,200);const ready=await fetch(origin+'/ready');assert.equal(ready.status,503);assert.equal((await ready.json()).ready,false);
  const response=await gatewaySend(origin,member.secret);assert.equal(response.status,503);assert.equal((await response.json()).error.code,'gateway_ownership_lost');assert.equal(calls,0);
  await app.close();const next=await open();assert.equal(next.status().ownership,true);
}));

test('PostgreSQL listen failure cleans database ownership and workspace locks instead of leaving an initialized process',integration,async t=>withGateway(t,async({name,root,data,config,open})=>{
  const occupied=http.createServer();await new Promise(resolve=>occupied.listen(0,'127.0.0.1',resolve));
  try{await assert.rejects(startPostgresGateway({database:options(name),dataDir:data,runtimeDir:join(root,'runtime'),config,env:{HARBOR_PORT:String(occupied.address().port)}}),{code:'gateway_listen_failed'});}
  finally{await new Promise(resolve=>occupied.close(resolve));}
  const retry=await open();assert.equal(retry.status().ownership,true);
}));

test('PostgreSQL startup uses partial recovery indexes and leaves completed retained history unchanged',integration,async t=>withGateway(t,async({store,member,open})=>{
  const at=Date.now();await store.run("INSERT INTO harbor.requests(id,started,ended,status,ip) SELECT 'synthetic-retained-'||i,$1,$2,'ok','synthetic' FROM generate_series(1,10000) i",at-2000,at-1000);
  const request=await store.observe(await store.authenticate(member.secret),'synthetic','synthetic','gpt-6.1-sol');await store.run("UPDATE harbor.requests SET recording_state='pending' WHERE id=$1",request);await store.query('ANALYZE harbor.requests');
  const uses=(plan,index)=>plan['Index Name']===index||(plan.Plans||[]).some(child=>uses(child,index));
  for(const [sql,index] of [["SELECT id FROM harbor.requests WHERE ended IS NULL AND status IN ('queued','running') ORDER BY started,id LIMIT 100",'request_unfinished_recovery'],["SELECT id FROM harbor.requests WHERE recording_state='pending' ORDER BY id LIMIT 1000",'request_recording_pending']]){
    const row=await store.get('EXPLAIN (FORMAT JSON) '+sql);assert.ok(uses(row['QUERY PLAN'][0].Plan,index),index);
  }
  const app=await open();assert.equal(app.status().interruptedOnStartup,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE status='ok' AND ended=$1",at-1000)).n,10000);assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',request)).status,'interrupted');
}));

test('PostgreSQL startup recovery excludes requests inserted after its ownership snapshot',integration,async t=>withGateway(t,async({store,member,open})=>{
  const query=pg.Client.prototype.query;let injected=false,late;
  t.mock.method(pg.Client.prototype,'query',function(sql,...args){
    const result=query.call(this,sql,...args);
    if(sql!=='SELECT pg_current_snapshot()::text visibility'||injected)return result;
    injected=true;return result.then(async value=>{late=await store.observe(await store.authenticate(member.secret),'synthetic','synthetic','gpt-6.1-sol');await store.run("UPDATE harbor.requests SET recording_state='pending' WHERE id=$1",late);return value;});
  });
  const app=await open();assert.equal(app.status().interruptedOnStartup,0);assert.ok(late);const row=await store.get('SELECT status,ended,recording_state FROM harbor.requests WHERE id=$1',late);assert.deepEqual(row,{status:'queued',ended:null,recording_state:'pending'});
}));


test('R23 ownership loss during graceful drain interrupts accepted inference before the drain deadline',integration,async t=>withGateway(t,async({store,member,open})=>{
  let entered=false,aborted=false;const app=await open({drainTimeoutMs:5000,responsesTransport:controlledResponses(signal=>new Promise((resolve,reject)=>{entered=true;signal.addEventListener('abort',()=>{aborted=true;reject(signal.reason);},{once:true});}))}),origin=await listenGateway(app),pending=gatewaySend(origin,member.secret).catch(()=>null);
  await gatewayWait(()=>entered);const closing=app.close();assert.equal(app.status().stopping,true);
  const owner=await store.get("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND granted");assert.ok(owner);await store.get('SELECT pg_terminate_backend($1) terminated',owner.pid);
  await gatewayWait(()=>aborted&&!app.status().ownership,1000);const result=await closing;const response=await pending;assert.equal(response.status,503);assert.equal(result.forced,false);assert.equal(result.ownershipLost,true);
  const request=await store.get('SELECT status,usage_unknown,recording_state FROM harbor.requests');assert.deepEqual(request,{status:'interrupted',usage_unknown:1,recording_state:'partial'});
}));

test('PostgreSQL disabling matches original weighted migration across bounded batches and protects late settlement',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member,legacy}=await legacyMember(t,store),at=Date.now();
  const copy=async(table,key)=>{const row=legacy.get('SELECT * FROM '+table+' WHERE id=?',key),columns=Object.keys(row);await store.run('INSERT INTO harbor.'+table+'('+columns.join(',')+') VALUES('+columns.map((_,i)=>'$'+(i+1)).join(',')+')',...columns.map(c=>row[c]));};
  for(const aid of ['a','b','c']){legacy.run("INSERT INTO accounts(id,name,home,state,route_block) VALUES(?,?,?,'ready',?)",aid,aid,aid,null);await copy('accounts',aid);}
  const settings={routing_strategy:{weighted:true},routing_weights:{mode:'manual',weights:{a:0,b:75,c:25},expires:nextWeightReset(at),version:1}};
  for(const [key,value] of Object.entries(settings)){legacy.run('INSERT INTO gateway_settings VALUES(?,?)',key,JSON.stringify(value));await store.run('INSERT INTO harbor.gateway_settings VALUES($1,$2)',key,JSON.stringify(value));}
  for(let i=0;i<600;i++){const sid='synthetic-move-'+String(i).padStart(4,'0');legacy.run("INSERT INTO sessions(id,member_id,account_id,protocol,enabled,nightly_enabled,nightly_expires,created,updated,upstream_id,message_count,cum_input,history_digest) VALUES(?,?,'a',?,?,0,123,1,1,'old-context',4,99,'synthetic-old-history')",sid,member.memberId,i%10?'responses':'chat',i%7?1:0);await copy('sessions',sid);}
  const old=createBalancer({store:legacy,preparing:new Set(),refreshQuota:async()=>{},clock:()=>at}),router=createPostgresRouter(store,{clock:()=>at});
  const expected=old.disableAccount('a'),actual=await router.disableAccount('a');assert.deepEqual(actual,expected);assert.deepEqual(actual,{moved:600,resetTextSessions:60});
  const columns='id,account_id,state,route_version,enabled,nightly_enabled,nightly_expires,upstream_id,message_count,cum_input,cum_output,cum_cached,history_digest,updated';assert.deepEqual(await store.all('SELECT '+columns+' FROM harbor.sessions ORDER BY id'),legacy.all('SELECT '+columns+' FROM sessions ORDER BY id').map(r=>({...r})));
  assert.deepEqual(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_schedule'")).value),JSON.parse(legacy.get("SELECT value FROM gateway_settings WHERE name='routing_schedule'").value));
  const epoch=(await store.get("SELECT route_epoch FROM harbor.accounts WHERE id='a'")).route_epoch;assert.equal((await router.disableAccount('a')).moved,0);assert.equal((await store.get("SELECT route_epoch FROM harbor.accounts WHERE id='a'")).route_epoch,epoch);
  await store.run("UPDATE harbor.accounts SET enabled=1 WHERE id='a'");await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,state,created,updated) VALUES('synthetic-inflight',$1,'a','responses','uncertain',$2,$2)",member.memberId,at);
  const request=await store.observe(await store.authenticate(member.secret),'synthetic','synthetic','gpt-6.1-sol');await store.run("UPDATE harbor.requests SET account_id='a',session_id='synthetic-inflight',route_version=0 WHERE id=$1",request);
  assert.equal((await router.disableAccount('a')).moved,1);await store.finish(request,{input:100,output:20,usageKnown:true},at+1);const after=await store.get("SELECT * FROM harbor.sessions WHERE id='synthetic-inflight'");assert.notEqual(after.account_id,'a');assert.equal(after.route_version,1);assert.equal(after.state,'uncertain');assert.equal((await store.get('SELECT account_id FROM harbor.requests WHERE id=$1',request)).account_id,'a');
});

test('PostgreSQL disabling rolls back every batch, schedule and audit on missing targets or a later audit failure',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),at=Date.now(),router=createPostgresRouter(store,{clock:()=>at});
  for(const aid of ['a','b'])await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES($1,$1,$1,'ready')",aid);
  await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_strategy',$1)",JSON.stringify({weighted:true}));await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_weights',$1)",JSON.stringify({mode:'manual',weights:{a:100,b:0},expires:nextWeightReset(at),version:1}));
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) SELECT 'synthetic-rollback-'||lpad(i::text,4,'0'),$1,'a','responses',1,1 FROM generate_series(1,300) i",member.memberId);
  const snapshot=async()=>({accounts:await store.all('SELECT id,enabled,route_epoch FROM harbor.accounts ORDER BY id'),sessions:await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),settings:await store.all('SELECT * FROM harbor.gateway_settings ORDER BY name'),audit:(await store.get('SELECT COUNT(*) n FROM harbor.audit')).n});
  let before=await snapshot();await assert.rejects(router.disableAccount('a'),{code:'disable_no_target'});assert.deepEqual(await snapshot(),before);
  await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='routing_weights'",JSON.stringify({mode:'manual',weights:{a:0,b:100},expires:nextWeightReset(at),version:2}));before=await snapshot();const run=store.run.bind(store);let batches=0;
  store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit(id,at,action,detail) SELECT')&&++batches===2)throw Error('synthetic second batch audit failure');return run(sql,...args);};
  try{await assert.rejects(router.disableAccount('a'),/synthetic second batch audit failure/);}finally{store.run=run;}assert.equal(batches,2);assert.deepEqual(await snapshot(),before);
});

test('PostgreSQL non-Codex disabling keeps platform boundaries and original least-binding allocation',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();const {member}=await legacyMember(t,store),router=createPostgresRouter(store);
  for(const [aid,kind] of [['a','claude'],['b','claude'],['c','claude'],['other','codex']])await store.run("INSERT INTO harbor.accounts(id,name,home,state,kind) VALUES($1,$1,$1,'ready',$2)",aid,kind);
  for(let i=0;i<9;i++)await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,$3,'chat',1,1)",'synthetic-kind-'+i,member.memberId,i<4?'a':'b');
  assert.deepEqual(await router.disableAccount('a'),{moved:4,resetTextSessions:4});assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.sessions WHERE account_id='c'")).n,4);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.sessions WHERE account_id='other'")).n,0);
});

test('PostgreSQL account HTTP creates encrypted API accounts, serializes shares, checks defaults and migrates disabled bindings',integration,async t=>withGateway(t,async({store,data,root,member,password,open})=>{
  const originalFetch=globalThis.fetch;let upstreamCalls=0,lastSecret;
  t.mock.method(globalThis,'fetch',async(input,options)=>{
    const url=new URL(input);if(url.origin!=='https://api.openai.com')return originalFetch(input,options);
    lastSecret=options.headers.Authorization;
    if(url.pathname==='/v1/models')return new Response(JSON.stringify({data:[{id:'gpt-6.1-sol'}]}),{headers:{'content-type':'application/json'}});
    assert.equal(url.pathname,'/v1/responses');upstreamCalls++;return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-account-response',model:'gpt-6.1-sol',output:[{id:'synthetic-message',type:'message',content:[{type:'output_text',text:'合成账号回复'}]}],usage:{input_tokens:100,output_tokens:20}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});
  });
  const at=Date.now(),app=await open({clock:()=>at,checker:async()=>true}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const call=(path,method='GET',body,credential=cookie)=>fetch(origin+'/api/admin/'+path,{method,headers:{cookie:credential,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});
  const payload={name:'合成API账号',kind:'codex',auth_mode:'openai_api',api_key:'synthetic-upstream-one',api_weight:70};
  let response=await call('accounts','POST',payload,'invalid');assert.equal(response.status,401);
  const run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic account audit failure');return run(sql,...args);};
  try{response=await call('accounts','POST',payload);assert.equal(response.status,500);}finally{app.store.run=run;}
  assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.accounts WHERE auth_mode='openai_api'")).n,0);assert.equal(readdirSync(join(root,'runtime','accounts')).length,0);
  const attempts=await Promise.all([call('accounts','POST',payload),call('accounts','POST',{...payload,name:'另一合成API账号'})]);assert.deepEqual(attempts.map(r=>r.status).sort(),[201,400]);const added=await attempts.find(r=>r.status===201).json(),accountId=added.id;
  const row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',accountId);assert.equal(row.state,'offline');assert.ok(row.api_secret_cipher&&!row.api_secret_cipher.includes('synthetic-upstream-one'));assert.ok(row.api_secret_digest===hash('synthetic-upstream-one'));assert.equal(readdirSync(join(root,'runtime','accounts')).length,1);
  response=await call('accounts/'+accountId+'/check','POST',{});assert.equal(response.status,200);const checked=await response.json();assert.equal(checked.ready,true);assert.equal(checked.balance.available,false);assert.ok(lastSecret==='Bearer synthetic-upstream-one');
  response=await call('accounts/'+accountId,'PATCH',{api_key:'synthetic-upstream-two',api_weight:100});assert.equal(response.status,200);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',accountId)).state,'offline');
  response=await call('accounts/'+accountId+'/check','POST',{});assert.equal(response.status,200);assert.ok(lastSecret==='Bearer synthetic-upstream-two');
  response=await call('state');const state=await response.json(),visible=state.accounts.find(a=>a.id===accountId);assert.equal(visible.api_key_configured,true);assert.equal(visible.api_secret_cipher,undefined);assert.equal(visible.api_secret_digest,undefined);assert.ok(!JSON.stringify(state).includes('synthetic-upstream-two'));assert.ok(!JSON.stringify(await store.all('SELECT detail FROM harbor.audit')).includes('synthetic-upstream-two'));
  response=await call('accounts/synthetic-runtime','PATCH',{api_key:'not-allowed'});assert.equal(response.status,400);
  response=await gatewaySend(origin,member.secret);assert.equal(response.status,200);await response.json();assert.equal(upstreamCalls,1);const session=await store.get('SELECT * FROM harbor.sessions WHERE account_id=$1',accountId);assert.ok(session);await store.run('UPDATE harbor.sessions SET nightly_enabled=0,nightly_expires=NULL WHERE id=$1',session.id);
  response=await call('accounts/'+accountId,'PATCH',{api_weight:0});assert.equal(response.status,200);assert.deepEqual(await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',session.id),{account_id:accountId,route_version:session.route_version});
  response=await call('accounts/'+accountId,'PATCH',{enabled:false});assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,moved:1,resetTextSessions:0});const migrated=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',session.id);assert.equal(migrated.account_id,'synthetic-runtime');assert.equal(migrated.nightly_expires,nextWeightReset(at));assert.equal(migrated.route_version,session.route_version+1);
  response=await call('accounts/'+accountId,'DELETE');assert.equal(response.status,200);const removed=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',accountId);assert.ok(removed.removed_at);assert.equal(removed.api_secret_cipher,null);assert.equal(removed.api_secret_digest,null);assert.ok(existsSync(row.home));assert.equal((await store.get('SELECT account_id FROM harbor.requests')).account_id,accountId);
  response=await call('accounts/synthetic-runtime/check','POST',{});assert.equal(response.status,200);assert.equal((await response.json()).ready,true);
  const before=await store.get('SELECT * FROM harbor.accounts WHERE id=$1','synthetic-runtime');response=await call('accounts/synthetic-runtime','PATCH',{enabled:false});assert.equal(response.status,409);assert.deepEqual(await store.get('SELECT * FROM harbor.accounts WHERE id=$1','synthetic-runtime'),before);
}));

test('PostgreSQL account checks deduplicate, reject stale rotated results and drain bounded work without reviving removed accounts',integration,async t=>withGateway(t,async({store,password,open})=>{
  const originalFetch=globalThis.fetch;let release,arrivals=0,aborted=false;
  t.mock.method(globalThis,'fetch',async(input,options)=>{
    const url=new URL(input);if(url.origin!=='https://api.openai.com')return originalFetch(input,options);arrivals++;
    await new Promise((resolve,reject)=>{release=resolve;options.signal.addEventListener('abort',()=>{aborted=true;reject(options.signal.reason);},{once:true});});
    return new Response(JSON.stringify({data:[{id:'gpt-6.1-sol'}]}),{headers:{'content-type':'application/json'}});
  });
  const app=await open({accountChecks:{maxConcurrent:1,maxQueued:1,queueTimeoutMs:1000},drainTimeoutMs:5000}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const call=(path,method='POST',body={})=>fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(method==='DELETE'?{}:{body:JSON.stringify(body)})});
  const ids=[];for(let i=0;i<3;i++){const response=await call('accounts','POST',{name:'合成检查'+i,kind:'codex',auth_mode:'openai_api',api_key:'synthetic-check-'+i,api_weight:0});assert.equal(response.status,201);ids.push((await response.json()).id);}
  const first=call('accounts/'+ids[0]+'/check');await gatewayWait(()=>arrivals===1);const repeated=call('accounts/'+ids[0]+'/check'),second=call('accounts/'+ids[1]+'/check');await gatewayWait(()=>app.status().accountChecks.queued===1);
  const full=await call('accounts/'+ids[2]+'/check');assert.equal(full.status,503);assert.equal(arrivals,1);
  assert.equal((await call('accounts/'+ids[0],'PATCH',{api_key:'synthetic-rotated'})).status,200);assert.equal((await call('accounts/'+ids[1],'DELETE')).status,200);release();
  for(const pending of [first,repeated])assert.equal((await pending).status,409);assert.notEqual((await second).status,200);assert.equal(arrivals,1);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',ids[0])).state,'offline');assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',ids[1])).state,'removed');
  const last=call('accounts/'+ids[2]+'/check');await gatewayWait(()=>arrivals===2);const closing=app.close();assert.equal((await last).status,503);await closing;assert.equal(aborted,true);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',ids[2])).state,'offline');
}));


test('PostgreSQL account HTTP refuses removal in flight but disabling preserves the old response and next binding',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let entered=false,release,calls=0;const gate=new Promise(resolve=>{release=resolve;});
  const app=await open({responsesTransport:controlledResponses(async()=>{calls++;if(calls===1){entered=true;await gate;}})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const call=(method,body)=>fetch(origin+'/api/admin/accounts/synthetic-runtime',{method,headers:{cookie,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});
  const pending=gatewaySend(origin,member.secret);let original,migrated;
  try{
    await gatewayWait(()=>entered);original=await store.get('SELECT * FROM harbor.sessions');
    assert.equal(original.account_id,'synthetic-runtime');assert.equal((await call('DELETE')).status,409);
    await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-target','合成接收账号','synthetic-target','ready')");
    const response=await call('PATCH',{enabled:false});assert.equal(response.status,200);assert.deepEqual(await response.json(),{ok:true,moved:1,resetTextSessions:0});
    migrated=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',original.id);assert.equal(migrated.account_id,'synthetic-target');assert.equal(migrated.route_version,original.route_version+1);
    assert.equal((await store.get('SELECT status FROM harbor.requests')).status,'running');
  }finally{release();}
  const finished=await pending;assert.equal(finished.status,200);await finished.json();await gatewayWait(()=>app.status().active===0);
  assert.deepEqual(await store.get('SELECT account_id,status FROM harbor.requests'),{account_id:'synthetic-runtime',status:'ok'});
  assert.deepEqual(await store.get('SELECT account_id,route_version FROM harbor.sessions WHERE id=$1',original.id),{account_id:'synthetic-target',route_version:migrated.route_version});
  const next=await gatewaySend(origin,member.secret);assert.equal(next.status,200);await next.json();assert.equal(calls,2);
  assert.deepEqual(await store.all('SELECT account_id,status FROM harbor.requests ORDER BY started,id'),[{account_id:'synthetic-runtime',status:'ok'},{account_id:'synthetic-target',status:'ok'}]);
}));

function syntheticLoginFlow(){
  let authorize,deny,complete,cancelled=false;
  const challenge=new Promise((resolve,reject)=>{authorize=resolve;deny=reject;}),completed=new Promise(resolve=>{complete=resolve;});
  return {challenge,completed,authorize,deny,complete,cancel:()=>{cancelled=true;deny(Error('synthetic cancelled'));complete({success:false});},get cancelled(){return cancelled;}};
}
test('PostgreSQL official login serializes flows, rolls back failed starts and rejects stale cancellation and disable results',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  const flows=[];let entered=false,release;const gate=new Promise(resolve=>{release=resolve;});
  const app=await open({checker:async()=>true,loginStarter:()=>{const flow=syntheticLoginFlow();flows.push(flow);return flow;},responsesTransport:controlledResponses(async()=>{entered=true;await gate;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const call=(id,action,method='POST',body={})=>fetch(origin+'/api/admin/accounts/'+id+(action?'/'+action:''),{method,headers:{cookie,'content-type':'application/json'},...(method==='GET'||method==='DELETE'?{}:{body:JSON.stringify(body)})});
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-login-second','合成登录备用','synthetic-login-second','ready')");
  const run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic login audit failure');return run(sql,...args);};
  try{assert.equal((await call('synthetic-runtime','login')).status,500);}finally{app.store.run=run;}
  assert.equal(flows.length,0);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'ready');
  const challenge={type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'};
  const first=call('synthetic-runtime','login');await gatewayWait(()=>flows.length===1);const duplicate=call('synthetic-runtime','login');
  assert.equal((await call('synthetic-runtime','check')).status,409);assert.equal((await call('synthetic-login-second','login')).status,409);
  flows[0].authorize(challenge);for(const response of await Promise.all([first,duplicate])){assert.equal(response.status,200);assert.equal((await response.json()).userCode,'TEST-ONLY');}assert.equal(flows.length,1);
  const poll=await call('synthetic-runtime','login','GET');assert.equal(poll.status,200);assert.equal((await poll.json()).state,'login');
  assert.equal((await call('synthetic-runtime','login/cancel')).status,200);flows[0].complete({success:true});await gatewayWait(()=>app.status().accountChecks.login.tasks===0);assert.equal(flows[0].cancelled,true);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'offline');
  const retry=call('synthetic-runtime','login');await gatewayWait(()=>flows.length===2);flows[1].authorize(challenge);assert.equal((await retry).status,200);flows[1].complete({success:true});await gatewayWait(async()=> (await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state==='ready');
  await gatewayWait(()=>app.status().accountChecks.login.tasks===0);
  // Remove the alternate during dispatch selection so the active-account guard
  // is exercised on the account whose credentials would be overwritten.
  await store.run("UPDATE harbor.accounts SET enabled=0 WHERE id='synthetic-login-second'");const inference=gatewaySend(origin,member.secret);
  try{await gatewayWait(()=>entered);assert.equal((await call('synthetic-runtime','login')).status,409);}finally{release();}
  assert.equal((await inference).status,200);await gatewayWait(()=>app.status().active===0);await store.run("UPDATE harbor.accounts SET enabled=1 WHERE id='synthetic-login-second'");
  const pending=call('synthetic-runtime','login');await gatewayWait(()=>flows.length===3);flows[2].authorize(challenge);assert.equal((await pending).status,200);
  const disabled=await call('synthetic-runtime','','PATCH',{enabled:false});assert.equal(disabled.status,200);assert.equal((await disabled.json()).moved,1);flows[2].complete({success:true});await gatewayWait(()=>app.status().accountChecks.login.tasks===0);assert.equal(flows[2].cancelled,true);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'offline');assert.equal((await call('synthetic-runtime','login')).status,409);
  const removing=call('synthetic-login-second','login');await gatewayWait(()=>flows.length===4);flows[3].authorize(challenge);assert.equal((await removing).status,200);assert.equal((await call('synthetic-login-second','','DELETE')).status,200);flows[3].complete({success:true});await gatewayWait(()=>app.status().accountChecks.login.tasks===0);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-login-second'")).state,'removed');
  assert.ok(!JSON.stringify(await store.all('SELECT detail FROM harbor.audit')).includes('TEST-ONLY'));
}));

test('PostgreSQL login shutdown cancels waiting authorization and startup exposes interrupted login for retry',integration,async t=>withGateway(t,async({store,password,open})=>{
  let flow;const app=await open({loginStarter:()=>flow=syntheticLoginFlow()}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const request=fetch(origin+'/api/admin/accounts/synthetic-runtime/login',{method:'POST',headers:{cookie,'content-type':'application/json'},body:'{}'});await gatewayWait(()=>!!flow);
  await app.close();assert.equal(flow.cancelled,true);assert.equal((await request).status,409);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'login');
  const next=await open();assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'offline');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='account.login.failed'")).n,1);assert.equal(next.status().accountChecks.login.pending,0);
}));

test('PostgreSQL login failures remain retryable, hide diagnostics and retain remote Claude and API guards',integration,async t=>withGateway(t,async({store,password,open})=>{
  let flow,calls=0;const app=await open({env:{HARBOR_LISTEN_HOST:'0.0.0.0',HARBOR_PUBLIC_ORIGINS:'http://localhost:43127'},checker:async()=>false,loginStarter:()=>{calls++;return flow=syntheticLoginFlow();}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const call=(id,action,method='POST')=>fetch(origin+'/api/admin/accounts/'+id+'/'+action,{method,headers:{cookie,'content-type':'application/json'},...(method==='POST'?{body:'{}'}:{})});
  await store.run("INSERT INTO harbor.accounts(id,name,home,kind,state) VALUES('synthetic-claude','合成Claude','synthetic-claude','claude','offline')");await store.run("INSERT INTO harbor.accounts(id,name,home,auth_mode,state) VALUES('synthetic-api','合成API','synthetic-api','openai_api','offline')");
  assert.equal((await call('synthetic-claude','login')).status,400);for(const [action,method] of [['login','POST'],['login','GET'],['login/cancel','POST']])assert.equal((await call('synthetic-api',action,method)).status,400);assert.equal(calls,0);
  const failed=call('synthetic-runtime','login');await gatewayWait(()=>calls===1);flow.deny(Error('synthetic-private-credential'));flow.complete({success:false,message:'synthetic-private-credential'});const failure=await failed;assert.equal(failure.status,502);assert.ok(!(await failure.text()).includes('synthetic-private-credential'));await gatewayWait(()=>app.status().accountChecks.login.tasks===0);
  assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'offline');
  const retry=call('synthetic-runtime','login');await gatewayWait(()=>calls===2);flow.authorize({type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'});assert.equal((await retry).status,200);flow.complete({success:true});await gatewayWait(()=>app.status().accountChecks.login.tasks===0);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'offline');assert.ok(!JSON.stringify(await store.all('SELECT last_error FROM harbor.accounts')).includes('synthetic-private-credential'));
}));

test('PostgreSQL cancelling login during credential verification rejects its late success',integration,async t=>withGateway(t,async({store,password,open})=>{
  let flow,verifying=false,release,signal;const gate=new Promise(resolve=>{release=resolve;});
  const app=await open({loginStarter:()=>flow=syntheticLoginFlow(),checker:async(_binary,_home,options)=>{verifying=true;signal=options.signal;await gate;return true;}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const post=action=>fetch(origin+'/api/admin/accounts/synthetic-runtime/'+action,{method:'POST',headers:{cookie,'content-type':'application/json'},body:'{}'});
  const request=post('login');await gatewayWait(()=>!!flow);flow.authorize({type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'});assert.equal((await request).status,200);flow.complete({success:true});
  try{await gatewayWait(()=>verifying);assert.equal((await post('login/cancel')).status,200);assert.equal(signal.aborted,true);}finally{release();}
  await gatewayWait(()=>app.status().accountChecks.login.tasks===0);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'offline');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='account.login.success'")).n,0);
}));

const syntheticQuota=(used,at,{reset=at+24*3600000,plan='synthetic-plan',minutes=10080}={})=>({rateLimits:{planType:plan,primary:{usedPercent:used,windowDurationMins:minutes,resetsAt:reset/1000}}});
test('PostgreSQL quota refresh preserves original recovery, exhaustion, calibration and manual weight rules',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store);let at=Date.now(),used=50;
  const router=createPostgresRouter(store,{clock:()=>at}),queue=createPostgresQueryQueue(),quotas=createPostgresAccountQuotas(store,{router,queue,clock:()=>at,readQuota:async()=>syntheticQuota(used,at)});t.after(async()=>{quotas.stop();await queue.close();await quotas.close();});
  assert.throws(()=>createPostgresAccountQuotas(store,{router,queue,readQuota:async()=>null,parallelism:0}),{code:'quota_configuration'});
  for(const aid of ['a','b'])await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json,route_block) VALUES($1,$1,$1,'ready',$2,$3)",aid,JSON.stringify({plan:'synthetic-plan',checked:at,remaining:20,windows:[{used:80,minutes:10080,reset:at+24*3600000}],limitReached:false}),aid==='a'?'quota_exhausted':null);
  await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_strategy','{\"weighted\":true}')");await router.syncWeights();assert.equal(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_auto'")).value).weights.a,0);
  assert.deepEqual(await quotas.refresh(await quotas.load('a'),{force:true}),{refreshed:true});let account=await quotas.load('a');assert.equal(account.route_block,null);assert.equal(account.route_epoch,1);assert.equal(account.quota_attempted_at,at);assert.equal(JSON.parse(account.quota_json).remaining,50);
  let auto=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_auto'")).value);assert.ok(Math.abs(auto.weights.a-100*50/70)<1e-8);assert.equal(auto.trigger,'recovery');
  const manual={mode:'manual',weights:{a:100,b:0},expires:nextWeightReset(at),version:1};await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_weights',$1)",JSON.stringify(manual));
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES('synthetic-quota-bound',$1,'a','responses',$2,$2)",member.memberId,at);
  await store.run("UPDATE harbor.accounts SET budget_used=1000 WHERE id='a'");used=60;await quotas.refresh(await quotas.load('a'),{force:true});account=await quotas.load('a');assert.equal(account.tokens_per_percent,100);assert.equal(JSON.parse(account.quota_calibration).remaining,40);
  used=100;await quotas.refresh(account,{force:true});account=await quotas.load('a');assert.equal(account.route_block,'quota_exhausted');auto=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_auto'")).value);assert.equal(auto.weights.a,0);assert.deepEqual(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'")).value),manual);assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='synthetic-quota-bound'")).account_id,'a');
  await store.run("UPDATE harbor.accounts SET route_block='login_expired',route_epoch=route_epoch+1 WHERE id='a'");await quotas.refresh(await quotas.load('a'),{force:true});assert.equal((await quotas.load('a')).route_block,'quota_exhausted');
  used=20;await quotas.refresh(await quotas.load('a'),{force:true});assert.equal((await quotas.load('a')).route_block,null);
  const after=await quotas.load('a');assert.deepEqual(await quotas.refresh(after),{skipped:true});at+=30001;assert.deepEqual(await quotas.refresh(after),{refreshed:true});
});

test('PostgreSQL quota HTTP deduplicates reads and rejects stale disabled, removed and login results',integration,async t=>withGateway(t,async({store,password,open})=>{
  let release,flow,entered=0,at=Date.now();const app=await open({clock:()=>at,loginStarter:()=>flow=syntheticLoginFlow(),rateReader:async()=>{entered++;await new Promise(resolve=>{release=resolve;});return syntheticQuota(25,at);}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const call=(account,action='quota',method='POST',body={})=>fetch(origin+'/api/admin/accounts/'+account+(action?'/'+action:''),{method,headers:{cookie,'content-type':'application/json'},...(method==='DELETE'?{}:{body:JSON.stringify(body)})});
  const unauthorized=await fetch(origin+'/api/admin/quotas/refresh',{method:'POST',headers:{'content-type':'application/json'},body:'{}'});assert.equal(unauthorized.status,401);assert.equal(entered,0);
  const first=call('synthetic-runtime');await gatewayWait(()=>entered===1);const duplicate=call('synthetic-runtime');await gatewayWait(()=>app.status().accountChecks.quotas.pending===1);
  assert.equal((await call('synthetic-runtime','','PATCH',{enabled:false})).status,200);release();for(const response of await Promise.all([first,duplicate])){assert.equal(response.status,200);assert.equal((await response.json()).skipped,true);}assert.equal(entered,1);let row=await store.get("SELECT * FROM harbor.accounts WHERE id='synthetic-runtime'");assert.equal(row.enabled,0);assert.equal(row.quota_json,null);
  assert.equal((await call('synthetic-runtime','','PATCH',{enabled:true})).status,200);const removed=call('synthetic-runtime');await gatewayWait(()=>entered===2);assert.equal((await call('synthetic-runtime','','DELETE')).status,200);release();assert.equal((await removed).status,200);row=await store.get("SELECT * FROM harbor.accounts WHERE id='synthetic-runtime'");assert.equal(row.state,'removed');assert.equal(row.quota_json,null);
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-quota-login','合成刷新登录','synthetic-quota-login','ready')");const stale=call('synthetic-quota-login');await gatewayWait(()=>entered===3);const login=call('synthetic-quota-login','login');await gatewayWait(()=>!!flow);flow.authorize({type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'});assert.equal((await login).status,200);release();assert.equal((await stale).status,200);row=await store.get("SELECT * FROM harbor.accounts WHERE id='synthetic-quota-login'");assert.equal(row.state,'login');assert.equal(row.quota_json,null);assert.equal((await call('synthetic-quota-login','login/cancel')).status,200);
}));

test('PostgreSQL quota bulk refresh scans bounded pages, caps work and persists failed-attempt throttling',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=Date.now(),calls=0,active=0,peak=0,fail=false;
  const queue=createPostgresQueryQueue({maxConcurrent:2,maxQueued:2}),router=createPostgresRouter(store,{clock:()=>at}),quotas=createPostgresAccountQuotas(store,{router,queue,parallelism:2,clock:()=>at,readQuota:async()=>{calls++;active++;peak=Math.max(peak,active);try{await new Promise(resolve=>setImmediate(resolve));if(fail)throw Error('synthetic-private-upstream-error');return syntheticQuota(25,at);}finally{active--;}}});t.after(async()=>{quotas.stop();await queue.close();await quotas.close();});
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) SELECT 'synthetic-quota-'||lpad(i::text,4,'0'),'合成批量额度','synthetic','ready' FROM generate_series(1,260) i");
  const all=store.all.bind(store);let largest=0;store.all=async(sql,...args)=>{const rows=await all(sql,...args);if(sql.includes('id>$1'))largest=Math.max(largest,rows.length);return rows;};
  const scan=quotas.refreshAll({force:true}),same=quotas.refreshAll({force:true});assert.equal(scan,same);const result=await scan;assert.deepEqual(result,{ok:true,refreshed:260,failed:0,skipped:0});assert.equal(calls,260);assert.ok(peak<=2);assert.equal(largest,256);
  const before=await quotas.load('synthetic-quota-0001');at+=30001;fail=true;assert.deepEqual(await quotas.refresh(before),{failed:true});const failed=await quotas.load(before.id);assert.equal(failed.quota_json,before.quota_json);assert.equal(failed.quota_attempted_at,at);assert.ok(!failed.quota_error.includes('synthetic-private'));assert.deepEqual(await quotas.refresh(failed),{skipped:true});
  // Recreating the reader must not discard failure throttling.
  const reopened=createPostgresAccountQuotas(store,{router,queue,clock:()=>at,readQuota:async()=>{throw Error('must remain throttled');}});assert.deepEqual(await reopened.refresh(failed),{skipped:true});await reopened.close();
  at+=30001;fail=false;assert.deepEqual(await quotas.refresh(failed),{refreshed:true});assert.equal((await quotas.load(failed.id)).quota_error,null);
});

test('PostgreSQL quota shutdown aborts reads without writing stale snapshots or quota errors',integration,async t=>withGateway(t,async({store,password,open})=>{
  let entered=false,aborted=false;const app=await open({rateReader:async(_binary,_home,{signal})=>new Promise((resolve,reject)=>{entered=true;signal.addEventListener('abort',()=>{aborted=true;reject(Error('synthetic abort'));},{once:true});})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const request=fetch(origin+'/api/admin/accounts/synthetic-runtime/quota',{method:'POST',headers:{cookie,'content-type':'application/json'},body:'{}'});await gatewayWait(()=>entered);await app.close();const response=await request;assert.equal(response.status,503);assert.equal(aborted,true);const row=await store.get("SELECT quota_json,quota_error FROM harbor.accounts WHERE id='synthetic-runtime'");assert.deepEqual(row,{quota_json:null,quota_error:null});
}));

test('PostgreSQL routing settings and explanation match original weights without mutating read snapshots or credits',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();const {legacy}=await legacyMember(t,store);let at=Date.now();
  for(const [aid,mode] of [['a','subscription'],['b','subscription'],['k','openai_api']]){
    const quota={plan:'synthetic-plan',checked:at,remaining:aid==='a'?80:20,windows:[{used:aid==='a'?20:80,minutes:10080,reset:at+24*3600000}],limitReached:false};
    legacy.run("INSERT INTO accounts(id,name,home,state,auth_mode,api_weight,quota_json) VALUES(?,?,?,'ready',?,?,?)",aid,aid,aid,mode,mode==='openai_api'?10:0,JSON.stringify(quota));
    const row=legacy.get('SELECT * FROM accounts WHERE id=?',aid),columns=Object.keys(row);await store.run('INSERT INTO harbor.accounts('+columns.join(',')+') VALUES('+columns.map((_,i)=>'$'+(i+1)).join(',')+')',...columns.map(c=>row[c]));
  }
  const old=createBalancer({store:legacy,preparing:new Set(),refreshQuota:async()=>{},clock:()=>at}),router=createPostgresRouter(store,{clock:()=>at});
  assert.deepEqual(await router.saveSettings({weighted:true,failover:true,nightly:true,quietMinutes:45}),old.saveSettings({weighted:true,failover:true,nightly:true,quietMinutes:45}));
  const legacyExplanation=async()=>{const {strategy,coverage,preview,...value}=await router.readView({explanation:true});assert.deepEqual(strategy,{configured:true,effective:true,source:'hourly_percentage',package_policy:{enabled:false,smoothing_hours:24,version:0}});assert.equal(preview,false);assert.equal(coverage.reason,null);assert.equal(coverage.comparable_groups,0);assert.equal(coverage.legacy,2);assert.equal(coverage.unknown,0);return {...value,accounts:value.accounts.map(({extra,...account})=>{assert.deepEqual(extra,{allowed:true,mode:account.id==='k'?'api':'package',reason:account.id==='k'?'extra_not_applicable':null,estimated:false});return account;})};};
  await router.syncWeights();old.syncWeights();assert.deepEqual(await legacyExplanation(),old.explain());
  const manual={mode:'manual',weights:{a:33.33,b:66.67},expected_version:0};assert.deepEqual(await router.saveWeights(manual),old.saveWeights(manual));assert.deepEqual(await legacyExplanation(),old.explain());
  const before={settings:await store.all('SELECT * FROM harbor.gateway_settings ORDER BY name'),audit:await store.all('SELECT * FROM harbor.audit ORDER BY id')};
  await router.readView();await router.readView({explanation:true});assert.deepEqual({settings:await store.all('SELECT * FROM harbor.gateway_settings ORDER BY name'),audit:await store.all('SELECT * FROM harbor.audit ORDER BY id')},before);
  const run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic routing audit failure');return run(sql,...args);};
  try{await assert.rejects(router.saveWeights({mode:'auto',expected_version:1}),/synthetic routing audit failure/);await assert.rejects(router.saveSettings({weighted:false}),/synthetic routing audit failure/);}finally{store.run=run;}
  assert.deepEqual({settings:await store.all('SELECT * FROM harbor.gateway_settings ORDER BY name'),audit:await store.all('SELECT * FROM harbor.audit ORDER BY id')},before);
  const concurrent=await Promise.allSettled([router.saveWeights({...manual,expected_version:1}),router.saveWeights({mode:'auto',expected_version:1})]);assert.equal(concurrent.filter(r=>r.status==='fulfilled').length,1);assert.equal(concurrent.find(r=>r.status==='rejected').reason.status,409);
  for(const bad of [{...manual,weights:{a:90,b:9}},{...manual,weights:{k:100}},{...manual,weights:{unknown:100}},{...manual,weights:{a:100.001}},{mode:'auto',weights:{},expected_version:2}])await assert.rejects(router.saveWeights({...bad,expected_version:2}),{status:400});
  const current=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'")).value);if(current.mode!=='manual')await router.saveWeights({...manual,expected_version:current.version});
  at=nextWeightReset(at);const expired=await router.readView();assert.equal(expired.weights.configuration.mode,'auto');assert.equal(expired.weights.configuration.expires,null);assert.equal(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'")).value).mode,'manual');
});

for(const began of ['2026-11-03T01:00:00+08:00','2026-11-03T03:00:00+08:00'])test('PostgreSQL session HTTP keeps manual binding, temporary expiry, version guards and immutable historical ownership from '+began,integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let at=Date.parse(began);const app=await open({clock:()=>at}),origin=await listenGateway(app);let cookie=await gatewayLogin(origin,password);
  const call=(path,method='PATCH',body={},credential=cookie)=>fetch(origin+'/api/admin/'+path,{method,headers:{cookie:credential,'content-type':'application/json'},...(method==='GET'?{}:{body:JSON.stringify(body)})});
  await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode) VALUES('synthetic-fixed','合成固定API','synthetic-fixed','ready','openai_api')");
  assert.equal((await call('sessions','POST',{member_id:member.memberId,account_id:'synthetic-fixed'},'invalid')).status,401);
  for(const payload of [{member_id:'missing',account_id:'synthetic-fixed'},{member_id:member.memberId,account_id:'synthetic-runtime'}])assert.equal((await call('sessions','POST',payload)).status,400);
  const run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic session audit failure');return run(sql,...args);};
  try{assert.equal((await call('sessions','POST',{member_id:member.memberId,account_id:'synthetic-fixed'})).status,500);}finally{app.store.run=run;}
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions')).n,0);
  const created=await call('sessions','POST',{member_id:member.memberId,account_id:'synthetic-fixed'});assert.equal(created.status,201);const fixed=await created.json();assert.equal(fixed.header,'X-Harbor-Session');let session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',fixed.session_id);assert.equal(session.nightly_enabled,0);assert.equal(session.nightly_expires,null);
  assert.equal((await call('sessions/'+session.id,'PATCH',{nightly_enabled:true})).status,400);
  const moved=await call('sessions/'+session.id,'PATCH',{account_id:'synthetic-runtime',expected_route_version:0});assert.equal(moved.status,200);assert.equal((await moved.json()).route_version,1);session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',session.id);assert.equal(session.nightly_expires,nextWeightReset(at));assert.ok(session.upstream_thread);assert.equal(session.account_id,'synthetic-runtime');
  assert.equal((await call('sessions/'+session.id,'PATCH',{account_id:'synthetic-fixed',expected_route_version:0})).status,409);assert.equal((await call('sessions/'+session.id,'PATCH',{expected_route_version:1})).status,400);assert.equal((await call('sessions/'+session.id,'PATCH',{enabled:'false'})).status,400);
  assert.equal((await call('sessions/'+session.id,'PATCH',{enabled:false})).status,200);assert.equal((await call('sessions/'+session.id,'PATCH',{account_id:'synthetic-fixed',expected_route_version:1})).status,409);assert.equal((await call('sessions/'+session.id,'PATCH',{enabled:true})).status,200);
  assert.equal((await call('sessions/'+session.id,'PATCH',{account_id:'synthetic-fixed',expected_route_version:1})).status,200);session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',session.id);assert.equal(session.nightly_enabled,0);assert.equal(session.nightly_expires,null);assert.equal(session.route_version,2);
  assert.equal((await call('sessions/'+session.id,'PATCH',{account_id:'synthetic-runtime',expected_route_version:2,nightly_enabled:true})).status,200);session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',session.id);assert.equal(session.nightly_enabled,1);assert.equal(session.nightly_expires,null);
  assert.equal((await call('sessions/'+session.id,'PATCH',{nightly_enabled:false})).status,200);at=nextWeightReset(at);const previousLogin=await call('state','GET');assert.equal(previousLogin.status,at-Date.parse(began)>=12*3600000?401:200);cookie=await gatewayLogin(origin,password);const stateResponse=await call('state','GET');assert.equal(stateResponse.status,200);const state=await stateResponse.json();assert.equal(state.sessions.find(s=>s.id===session.id).nightly_enabled,1);assert.ok(state.balancing?.weights);
  const explanation=await call('balancing/explanation','GET');assert.equal(explanation.status,200);assert.ok(Array.isArray((await explanation.json()).accounts));
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES('synthetic-legacy-switch',$1,'synthetic-runtime','chat',$2,$2)",member.memberId,at);assert.equal((await call('sessions/synthetic-legacy-switch','PATCH',{account_id:'synthetic-fixed',expected_route_version:0})).status,409);
  const memberResponse=await call('members/'+member.memberId,'PATCH',{enabled:false});assert.equal(memberResponse.status,200);assert.equal((await call('sessions/'+session.id,'PATCH',{account_id:'synthetic-fixed',expected_route_version:3})).status,409);
}));

test('PostgreSQL manual session changes protect live requests and slow uploads, serialize versions and roll back audit failures',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let entered=false,release,calls=0;const gate=new Promise(resolve=>{release=resolve;});const app=await open({responsesTransport:controlledResponses(async()=>{calls++;if(calls===1){entered=true;await gate;}})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const call=(session,body)=>fetch(origin+'/api/admin/sessions/'+session,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(body)});
  for(const aid of ['synthetic-target','synthetic-third'])await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES($1,$1,$1,'ready')",aid);
  // Force the first request to the existing account, independently of ID order.
  await store.run("UPDATE harbor.accounts SET enabled=0 WHERE id IN ('synthetic-target','synthetic-third')");const inference=gatewaySend(origin,member.secret);let session;
  try{
    await gatewayWait(()=>entered);session=await store.get('SELECT * FROM harbor.sessions');await store.run("UPDATE harbor.accounts SET enabled=1 WHERE id IN ('synthetic-target','synthetic-third')");
    assert.equal((await call(session.id,{account_id:'synthetic-target',expected_route_version:0})).status,409);
    assert.equal((await call(session.id,{enabled:false})).status,200);assert.equal((await gatewaySend(origin,member.secret)).status,403);
  }finally{release();}
  const completed=await inference;assert.equal(completed.status,200);await completed.json();await gatewayWait(()=>app.status().active===0);assert.equal(calls,1);assert.deepEqual(await store.get('SELECT account_id,status FROM harbor.requests'),{account_id:'synthetic-runtime',status:'ok'});
  assert.equal((await call(session.id,{enabled:true})).status,200);assert.equal((await call(session.id,{account_id:'synthetic-target',expected_route_version:0})).status,200);
  const next=await gatewaySend(origin,member.secret);assert.equal(next.status,200);await next.json();await gatewayWait(()=>app.status().active===0);
  const upload=http.request(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'synthetic-runtime-client'}});upload.on('error',()=>{});upload.on('response',res=>res.resume());
  try{upload.write('{"model":"gpt-6.1-sol","input":"');await gatewayWait(()=>app.status().preparing===1);assert.equal((await call(session.id,{account_id:'synthetic-runtime',expected_route_version:1})).status,409);}finally{upload.destroy();}
  await gatewayWait(()=>app.status().preparing===0);assert.equal((await call(session.id,{account_id:'synthetic-runtime',expected_route_version:1})).status,200);
  const before=await store.get('SELECT * FROM harbor.sessions WHERE id=$1',session.id),audit=(await store.get('SELECT COUNT(*) n FROM harbor.audit')).n,run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic session update audit failure');return run(sql,...args);};
  try{assert.equal((await call(session.id,{enabled:false,nightly_enabled:false})).status,500);}finally{app.store.run=run;}
  assert.deepEqual(await store.get('SELECT * FROM harbor.sessions WHERE id=$1',session.id),before);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.audit')).n,audit);
  const concurrent=await Promise.all([call(session.id,{account_id:'synthetic-target',expected_route_version:2}),call(session.id,{account_id:'synthetic-third',expected_route_version:2})]);assert.deepEqual(concurrent.map(r=>r.status).sort(),[200,409]);assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',session.id)).route_version,3);
  assert.deepEqual(await store.all("SELECT account_id,status FROM harbor.requests WHERE status='ok' ORDER BY started,id"),[{account_id:'synthetic-runtime',status:'ok'},{account_id:'synthetic-target',status:'ok'}]);
}));

for(const lifecycle of ['drain','ownership'])for(const route of ['balancing','balancing/weights'])test('R24 '+route+' rolls back after '+lifecycle+' while waiting for the routing lock',integration,async t=>withGateway(t,async({store,password,open})=>{
  const app=await open({drainTimeoutMs:5000}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const owner=await store.get("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND granted");assert.ok(owner);
  const snapshot=async()=>({settings:await store.all('SELECT * FROM harbor.gateway_settings ORDER BY name'),audit:await store.all('SELECT * FROM harbor.audit ORDER BY id')});const before=await snapshot();
  let release,held=false,closing;const gate=new Promise(resolve=>{release=resolve;}),blocker=store.transaction(async()=>{await lockPostgresRouting(store);held=true;await gate;});
  await gatewayWait(()=>held);const request=fetch(origin+'/api/admin/'+route,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(route==='balancing'?{weighted:true}:{mode:'manual',weights:{'synthetic-runtime':100},expected_version:0})});
  try{
    await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND NOT granted")).n===1);
    if(lifecycle==='ownership'){await store.get('SELECT pg_terminate_backend($1) terminated',owner.pid);await gatewayWait(()=>!app.status().ownership);}
    else{closing=app.close();assert.equal(app.status().stopping,true);}
  }finally{release();await blocker;}
  const response=await request;assert.equal(response.status,503);assert.equal((await response.json()).error.code,'service_stopping');if(closing)await closing;else await app.close();assert.deepEqual(await snapshot(),before);
}));

for(const lifecycle of ['drain','ownership'])for(const route of ['balancing','balancing/weights'])test('R24 '+route+' rolls back after '+lifecycle+' before committing its audit',integration,async t=>withGateway(t,async({store,password,open})=>{
  const app=await open({drainTimeoutMs:5000}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
  const owner=await store.get("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND granted");assert.ok(owner);
  const snapshot=async()=>({settings:await store.all('SELECT * FROM harbor.gateway_settings ORDER BY name'),audit:await store.all('SELECT * FROM harbor.audit ORDER BY id')});const before=await snapshot();
  let release,held=false,closing;const gate=new Promise(resolve=>{release=resolve;}),run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{const result=await run(sql,...args);if(sql.startsWith('INSERT INTO harbor.audit')){held=true;await gate;}return result;};
  const request=fetch(origin+'/api/admin/'+route,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(route==='balancing'?{weighted:true}:{mode:'auto',expected_version:0})});
  try{await gatewayWait(()=>held);if(lifecycle==='ownership'){await store.get('SELECT pg_terminate_backend($1) terminated',owner.pid);await gatewayWait(()=>!app.status().ownership);}else{closing=app.close();assert.equal(app.status().stopping,true);}}
  finally{release();app.store.run=run;}
  const response=await request;assert.equal(response.status,503);assert.equal((await response.json()).error.code,'service_stopping');if(closing)await closing;else await app.close();assert.deepEqual(await snapshot(),before);
}));

test('PostgreSQL durable jobs bound admission, deduplicate identities and fence expired or cancelled owners',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=1000;const jobs=createPostgresJobs(store,{clock:()=>at,leaseMs:1000,maxPending:2});
  assert.throws(()=>createPostgresJobs(store,{leaseMs:0}),{code:'job_configuration'});await assert.rejects(jobs.enqueue('fixture.task',{payload:{text:'x'.repeat(65536)}}),{code:'job_payload_invalid'});
  assert.deepEqual(await jobs.enqueue('fixture.task',{id:'a',payload:{a:1,b:2},maxAttempts:2}),{id:'a',created:true});assert.deepEqual(await jobs.enqueue('fixture.task',{id:'a',payload:{b:2,a:1},maxAttempts:2}),{id:'a',created:false});
  await assert.rejects(jobs.enqueue('fixture.task',{id:'a',payload:{a:2},maxAttempts:2}),{code:'job_identity_conflict'});
  const concurrent=await Promise.allSettled(['b','c'].map(id=>jobs.enqueue('fixture.task',{id})));assert.equal(concurrent.filter(x=>x.status==='fulfilled').length,1);assert.equal(concurrent.find(x=>x.status==='rejected').reason.code,'job_queue_full');
  const first=await jobs.claim(['fixture.task']);assert.equal(first.id,'a');assert.equal(first.attempts,1);at=2001;assert.equal(await jobs.heartbeat(first),false);const next=await jobs.claim(['fixture.task']);assert.equal(next.id,'a');assert.equal(next.attempts,2);assert.notEqual(first.claim_token,next.claim_token);
  await assert.rejects(jobs.complete(first,{fake:true}),{code:'job_lease_lost'});await assert.rejects(jobs.owned(first,async()=>assert.fail('stale owner entered')),{code:'job_lease_lost'});
  const result=await jobs.commitEffect(next,async()=>{await store.run("INSERT INTO harbor.audit VALUES('synthetic-job-effect',$1,'fixture.effect','synthetic')",at);return {applied:1};});assert.deepEqual(result,{applied:1});assert.deepEqual(await jobs.commitEffect(next,async()=>assert.fail('effect repeated')),result);assert.equal(await jobs.complete(next,result),true);assert.equal(await jobs.complete(next,{fake:true}),false);
  const other=await jobs.claim(['fixture.task']);assert.ok(other);assert.equal(await jobs.cancel(other.id),true);assert.equal(await jobs.heartbeat(other),false);await assert.rejects(jobs.fail(other),{code:'job_lease_lost'});
  await jobs.enqueue('fixture.task',{id:'exhausted',maxAttempts:1});const last=await jobs.claim(['fixture.task']);assert.equal(last.id,'exhausted');at+=1001;assert.equal(await jobs.claim(['fixture.task']),null);assert.equal((await store.get("SELECT status FROM harbor.background_jobs WHERE id='exhausted'")).status,'failed');
  assert.ok(await createPostgresJobs(store).now()>0);
});

test('PostgreSQL job effect and result commit atomically and recover once across acknowledgement loss',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=1000;const jobs=createPostgresJobs(store,{clock:()=>at,leaseMs:1000});await jobs.enqueue('fixture.task',{id:'synthetic-atomic'});const first=await jobs.claim(['fixture.task']);
  await assert.rejects(jobs.commitEffect(first,async()=>{await store.run("INSERT INTO harbor.audit VALUES('synthetic-expired-effect',$1,'fixture.effect','synthetic')",at);at=2001;return {applied:1};}),{code:'job_lease_lost'});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.audit')).n,0);assert.equal((await store.get("SELECT result FROM harbor.background_jobs WHERE id='synthetic-atomic'")).result,null);
  const next=await jobs.claim(['fixture.task']);const result=await jobs.commitEffect(next,async()=>{await store.run("INSERT INTO harbor.audit VALUES('synthetic-final-effect',$1,'fixture.effect','synthetic')",at);return {applied:1};});at+=1001;
  const recovered=await jobs.claim(['fixture.task']);assert.notEqual(recovered.claim_token,next.claim_token);assert.deepEqual(await jobs.commitEffect(recovered,async()=>assert.fail('ack loss repeated effect')),result);await jobs.complete(recovered,result);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.audit')).n,1);
  await jobs.enqueue('fixture.task',{id:'synthetic-known-result',maxAttempts:1});const known=await jobs.claim(['fixture.task']);const saved=await jobs.commitEffect(known,async()=>({done:true}));assert.equal(await jobs.cancel(known.id),false);assert.equal(await jobs.fail(known,{errorCode:'worker_interrupted'}),'completed');assert.equal(await jobs.complete(known,saved),false);assert.equal((await store.get('SELECT error_code FROM harbor.background_jobs WHERE id=$1',known.id)).error_code,null);
  await jobs.enqueue('fixture.task',{id:'synthetic-exhausted-ack',maxAttempts:1});const once=await jobs.claim(['fixture.task']);await jobs.commitEffect(once,async()=>({done:true}));at+=1001;assert.equal(await jobs.claim(['fixture.task']),null);assert.equal((await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',once.id)).status,'completed');
});

test('PostgreSQL Worker caps parallel handlers, renews leases and releases interrupted jobs for retry',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=1000;const jobs=createPostgresJobs(store,{clock:()=>at,leaseMs:1000}),entered=[];
  assert.throws(()=>createPostgresWorkerRuntime({jobs:{},handlers:{'fixture.task':async()=>{}}}),{code:'worker_configuration'});
  for(const id of ['a','b','c'])await jobs.enqueue('fixture.task',{id});
  t.mock.timers.enable({apis:['setInterval']});
  const worker=createPostgresWorkerRuntime({jobs,concurrency:2,handlers:{'fixture.task':async(job,{signal})=>{entered.push(job.id);await new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(Error('synthetic interrupted')),{once:true}));}}});
  await worker.runOnce();await gatewayWait(()=>entered.length===2);assert.equal(worker.status().active,2);await worker.runOnce();assert.equal(entered.length,2);at=1500;t.mock.timers.tick(334);await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status='running' AND lease_until=2500")).n===2);
  await worker.close();assert.equal(worker.status().active,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status='pending'")).n,3);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE error_code='worker_interrupted'")).n,2);t.mock.timers.reset();
  at=3000;const retry=createPostgresWorkerRuntime({jobs,handlers:{'fixture.task':async job=>jobs.commitEffect(job,async()=>({done:true}))},concurrency:2});await retry.runOnce();await gatewayWait(()=>retry.status().active===0);await retry.runOnce();await gatewayWait(()=>retry.status().active===0);await retry.close();assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status='completed'")).n,3);
});

test('PostgreSQL routing maintenance restores expired settings in bounded resumable batches without migrating bindings',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();const {member}=await legacyMember(t,store);let at=Date.parse('2026-11-03T18:00:00Z');const jobs=createPostgresJobs(store,{clock:()=>at}),router=createPostgresRouter(store,{clock:()=>at}),maintenance=createPostgresRoutingMaintenance(store,{jobs,router});
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-maintenance','合成周期账号','synthetic','ready')");await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_weights',$1)",JSON.stringify({mode:'manual',weights:{'synthetic-maintenance':100},expires:at,version:7}));
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,nightly_enabled,nightly_expires,created,updated) SELECT 'synthetic-expiry-'||i,$1,'synthetic-maintenance','responses',0,$2,$2,$2 FROM generate_series(1,300) i",member.memberId,at);
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,nightly_enabled,created,updated) VALUES('synthetic-permanent',$1,'synthetic-maintenance','responses',0,$2,$2)",member.memberId,at);
  assert.equal((await maintenance.schedule()).created,true);assert.equal((await maintenance.schedule()).created,false);const job=await jobs.claim(['routing.expiry']);
  const run=store.run.bind(store);let batches=0;store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit(id,at,action,detail) SELECT')&&++batches===2)throw Error('synthetic second expiry audit failure');return run(sql,...args);};
  try{await assert.rejects(maintenance.handlers['routing.expiry'](job),/synthetic second expiry audit failure/);}finally{store.run=run;}
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions WHERE nightly_enabled=1')).n,256);assert.equal(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'")).value).version,7);
  const result=await maintenance.handlers['routing.expiry'](job);assert.deepEqual(result,{sessionsRestored:300,weightsExpired:true});assert.deepEqual(await maintenance.handlers['routing.expiry'](job),result);await jobs.complete(job,result);
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions WHERE nightly_enabled=1')).n,300);assert.equal((await store.get("SELECT nightly_enabled FROM harbor.sessions WHERE id='synthetic-permanent'")).nightly_enabled,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.sessions WHERE account_id<>'synthetic-maintenance' OR route_version<>0")).n,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.nightly.expired'")).n,300);assert.equal(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'")).value).version,8);
});

test('PostgreSQL independent Workers share one cycle, preserve API pins and close their own pools idempotently',integration,async t=>{
  const {store,name}=await fixture(t);await assert.rejects(createPostgresWorker({database:options(name)}),{code:'database_migration_required'});await store.migrate();const {member}=await legacyMember(t,store);let at=Date.parse('2026-11-03T18:00:00Z');
  for(const [aid,mode] of [['synthetic-sub','subscription'],['synthetic-api','openai_api']])await store.run("INSERT INTO harbor.accounts(id,name,home,auth_mode,state) VALUES($1,$1,'synthetic',$2,'ready')",aid,mode);
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,nightly_enabled,nightly_expires,created,updated) VALUES('synthetic-sub-pin',$1,'synthetic-sub','responses',0,$2,$2,$2),('synthetic-api-pin',$1,'synthetic-api','responses',0,NULL,$2,$2)",member.memberId,at);
  const workers=[];try{for(let i=0;i<2;i++)workers.push(await createPostgresWorker({database:options(name),clock:()=>at,runtime:{concurrency:1,pollMs:100}}));
  await Promise.all(workers.map(worker=>worker.runOnce()));await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status='completed'")).n===1);await gatewayWait(()=>workers.every(worker=>worker.status().active===0));
  assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.background_jobs')).n,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.nightly.expired'")).n,1);assert.equal((await store.get("SELECT nightly_enabled FROM harbor.sessions WHERE id='synthetic-api-pin'")).nightly_enabled,0);
  await Promise.all(workers.map(worker=>worker.runOnce()));assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.background_jobs')).n,1);at+=86400000;await workers[0].runOnce();await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status='completed'")).n===2);
  await workers[0].close();await workers[0].close();assert.equal(workers[0].status().database.connections,0);
  }finally{for(const worker of workers)await worker.close();}
});

test('PostgreSQL Worker CLI starts independently and shuts down on SIGTERM without credentials or payload output',{...integration,skip:!socket?'CLI子进程验收需隔离Unix socket；CI另测Worker工厂入口':integration.skip},async t=>{
  const {store,name}=await fixture(t);await store.migrate();const connection=new URL('postgresql://'+encodeURIComponent(process.env.HARBOR_TEST_PG_USER||'harbor_ci')+'@localhost:'+Number(process.env.HARBOR_TEST_PG_PORT||55461)+'/'+name);connection.searchParams.set('host',socket);
  const child=spawn(process.execPath,['postgres-worker.mjs'],{cwd:new URL('..',import.meta.url),env:{HARBOR_DATABASE_URL:connection.href,HARBOR_RECORDING_WORKER_ENABLED:'0'},stdio:['ignore','pipe','pipe']});let output='',errors='';child.stdout.on('data',bytes=>{output+=bytes;});child.stderr.on('data',bytes=>{errors+=bytes;});
  const ended=new Promise((resolve,reject)=>{child.once('error',reject);child.once('close',(code,signal)=>resolve({code,signal}));});t.after(()=>{if(child.exitCode===null)child.kill('SIGKILL');});
  let exitTimer;try{
  await gatewayWait(()=>output.includes('Harbor PostgreSQL worker ready'));await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status='completed'")).n===1);child.kill('SIGTERM');const result=await Promise.race([ended,new Promise((_,reject)=>{exitTimer=setTimeout(()=>reject(Error('Worker CLI did not exit')),5000);exitTimer.unref();})]);assert.deepEqual(result,{code:0,signal:null});assert.equal(errors,'');assert.equal(output.trim(),'Harbor PostgreSQL worker ready');
  }finally{clearTimeout(exitTimer);if(child.exitCode===null&&child.signalCode===null)child.kill('SIGKILL');await ended.catch(()=>{});}
});

test('PostgreSQL Worker heartbeat cannot revive a lease that expires while waiting for its row lock',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=1000;const jobs=createPostgresJobs(store,{clock:()=>at,leaseMs:1000});await jobs.enqueue('fixture.task',{id:'synthetic-heartbeat-wait'});const job=await jobs.claim(['fixture.task']);
  let release,held=false;const gate=new Promise(resolve=>{release=resolve;}),blocker=store.transaction(async()=>{await store.get('SELECT id FROM harbor.background_jobs WHERE id=$1 FOR UPDATE',job.id);held=true;await gate;});await gatewayWait(()=>held);at=1500;const renewed=jobs.heartbeat(job);
  try{await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock'")).n>=1);at=2100;}finally{release();await blocker;}
  assert.equal(await renewed,false);assert.equal((await store.get('SELECT lease_until FROM harbor.background_jobs WHERE id=$1',job.id)).lease_until,2000);
});

test('PostgreSQL Worker drains admitted jobs even when scheduling hits the queue limit',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();const jobs=createPostgresJobs(store,{maxPending:1});await jobs.enqueue('fixture.task',{id:'synthetic-full-queue'});let entered=0;
  const worker=createPostgresWorkerRuntime({jobs,concurrency:1,schedule:()=>jobs.enqueue('fixture.task',{id:'synthetic-next-job'}),handlers:{'fixture.task':async()=>{entered++;return {done:true};}}});
  try{await worker.runOnce();assert.equal(entered,1);await gatewayWait(()=>worker.status().active===0);assert.equal((await store.get("SELECT status FROM harbor.background_jobs WHERE id='synthetic-full-queue'")).status,'completed');await worker.runOnce();await gatewayWait(()=>worker.status().active===0);assert.equal(entered,2);}finally{await worker.close();}
});

test('R25 Worker tracks overlapping claim generations independently and closes every handler',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=1000,running=0,peak=0;const jobs=createPostgresJobs(store,{clock:()=>at,leaseMs:1000}),entered=[];await jobs.enqueue('fixture.task',{id:'synthetic-overlap'});t.mock.timers.enable({apis:['setInterval']});
  const worker=createPostgresWorkerRuntime({jobs,concurrency:2,handlers:{'fixture.task':async(job,{signal})=>{
    const entry={token:job.claim_token,signal,release:null};entered.push(entry);running++;peak=Math.max(peak,running);
    try{await new Promise((resolve,reject)=>{entry.release=resolve;signal.addEventListener('abort',()=>reject(Error('synthetic stopped')),{once:true});});return {done:true};}finally{running--;}
  }}});
  try{
    await worker.runOnce();assert.equal(entered.length,1);at=2001;await worker.runOnce();assert.equal(entered.length,2);assert.notEqual(entered[0].token,entered[1].token);assert.equal(worker.status().active,2);
    await jobs.enqueue('fixture.task',{id:'synthetic-other'});await worker.runOnce();assert.equal(entered.length,2);assert.equal(peak,2);
    entered[0].release();await gatewayWait(()=>worker.status().active===1&&running===1);assert.equal(entered[1].signal.aborted,false);
    await worker.close();assert.equal(entered[1].signal.aborted,true);assert.equal(running,0);assert.equal(worker.status().active,0);assert.equal(entered.length,2);
  }finally{for(const entry of entered)entry.release?.();await worker.close();await gatewayWait(()=>running===0);t.mock.timers.reset();}
});

async function planningFixture(t){
  const value=await fixture(t),{store}=value;await store.migrate();await store.initializePricing();
  // Original SQL/SQLite equivalence and conservative worker fixtures intentionally retain the pre-016 policy.
  // Live request-boundary behavior is covered by the real gateway and automatic handoff fixtures.
  await store.run("UPDATE harbor.gateway_settings SET value='{\"mode\":\"legacy\",\"version\":0}' WHERE name='request_handoff_policy'");
  const {member}=await legacyMember(t,store),at=Date.parse('2026-11-03T02:00:10+08:00');
  const books={old:{rates:{'gpt-6.1-sol':{input:'2',cached:'0.1',output:'10'},'gpt-6-astra':{input:'10',cached:'1',output:'50'}},aliases:[{name:'synthetic-alias',target:'gpt-6.1-sol',from:0,to:null}]},new:{rates:{'gpt-6.1-sol':{input:'4',cached:null,output:'20'},'gpt-6-astra':{input:'20',cached:'2',output:'100'}},aliases:[{name:'synthetic-alias',target:'gpt-6-astra',from:0,to:null}]}};
  for(const [revision,book] of Object.entries(books))await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3)',revision,at,JSON.stringify(book));
  for(const aid of ['a','b'])await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) VALUES($1,$1,'synthetic','ready',$2)",aid,JSON.stringify({plan:'synthetic-plan',checked:at,remaining:50,windows:[{minutes:10080,reset:at+24*3600000,used:50}],limitReached:false}));
  for(const [sid,aid] of [['heavy','a'],['small-1','a'],['small-2','a'],['other','b'],['running','a'],['manual','a']])await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,$3,'responses',$4,$4)",sid,member.memberId,aid,at-7200000);
  const row=(session,model,input,extra={})=>({session_id:session,model,input,output:0,cached:0,usage_unknown:0,status:'ok',started:at-7200000,ended:at-3600000,price_version:'old',...extra});
  const load=async rows=>{
    await store.query('TRUNCATE harbor.requests CASCADE');let index=0;
    for(const record of rows){const r={...record,id:'synthetic-planning-'+(++index),member_id:member.memberId,key_id:member.keyId,ip:'synthetic'},columns=Object.keys(r);await store.run('INSERT INTO harbor.requests('+columns.join(',')+') VALUES('+columns.map((_,i)=>'$'+(i+1)).join(',')+')',...columns.map(key=>r[key]));await store.projectPrice(await store.get('SELECT * FROM harbor.requests WHERE id=$1',r.id),at);}
  };
  const reference=async options=>planBalance({accounts:await store.all('SELECT * FROM harbor.accounts ORDER BY id'),sessions:await store.all('SELECT s.*,m.enabled member_enabled,m.removed_at member_removed FROM harbor.sessions s JOIN harbor.members m ON m.id=s.member_id ORDER BY s.created,s.id'),requests:await store.all('SELECT * FROM harbor.requests ORDER BY query_sequence'),at,quoteUsage:(r,revision=r.price_version)=>books[revision]?quoteReference(r,books[revision],revision):{state:'unpriced',model:r.billing_model||r.model,totalAtoms:null},...options});
  return {...value,at,member,books,row,load,reference};
}
function assertPlanningEqual(actual,expected){
  const normalize=value=>({...value,priceRevisions:[...value.priceRevisions].sort()});assert.deepEqual(normalize(actual),normalize(expected));
}
test('PostgreSQL planning aggregates preserve costly-session selection, revision medians, aliases and unknown protection',integration,async t=>{
  const {store,at,row,load,reference}=await planningFixture(t),reader=createPostgresPlanningInput(store,{clock:()=>at});
  const cases=[
    [row('heavy','gpt-6-astra',300),row('small-1','gpt-6.1-sol',1000),row('small-2','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1500)],
    [row('heavy','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',3000),row('running','gpt-6.1-sol',0,{ended:null,status:'running',price_version:'new'})],
    [row('heavy','synthetic-alias',1000),row('other','gpt-6.1-sol',3000),row('running','gpt-6.1-sol',0,{ended:null,status:'running',price_version:'new'})],
    [row('heavy','gpt-6.1-sol',1000,{cached:100}),row('other','gpt-6.1-sol',3000),row('running','gpt-6.1-sol',0,{ended:null,status:'running',price_version:'new'})],
    [row('manual','gpt-6.1-sol',999999,{status:'reconciled',manual_cost_atoms:'3250000000000'}),row('other','gpt-6.1-sol',1000),row('running','gpt-6.1-sol',0,{ended:null,status:'running'})],
    [row('heavy','synthetic-alias',1000),row('other','gpt-6-astra',300),row('running','synthetic-alias',0,{ended:null,status:'running',price_version:'new'})],
    [row('heavy','synthetic-request-alias',300,{billing_model:'gpt-6-astra'}),row('other','gpt-6.1-sol',1500),row('running','gpt-6-astra',0,{ended:null,status:'running',price_version:'new'})],
    [row('heavy','gpt-6.1-sol',1000,{cached:100,output:200}),row('other','gpt-6.1-sol',3000,{cached:500,output:20}),row('running','gpt-6.1-sol',0,{ended:null,status:'running'})],
    [row('heavy','gpt-6.1-sol',1000,{output:333}),row('other','gpt-6.1-sol',3000,{output:999}),row('running','gpt-6.1-sol',0,{ended:null,status:'running',price_version:'new'})],
    [row('heavy','gpt-6.1-sol',123,{status:'interrupted',usage_unknown:1}),row('other','gpt-6.1-sol',1000)],
    [row('manual','gpt-6.1-sol',999999,{status:'reconciled',manual_cost_atoms:'3250000000000'}),row('running','gpt-6.1-sol',0,{ended:null,status:'running'})],
    [row('heavy','synthetic-unknown-model',1000),row('other','gpt-6.1-sol',1000)],
    [row('heavy','gpt-6.1-sol',1000,{status:'reconciled'}),row('other','gpt-6.1-sol',1000)],
    [row('heavy','gpt-6.1-sol',0),row('other','gpt-6.1-sol',1000)],
    [row('heavy','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1000),row('running','gpt-6.1-sol',0,{ended:null,status:'running',started:at+1})]
  ];
  for(const rows of cases){await load(rows);for(const weighted of [false,true])assertPlanningEqual(await reader.plan({weighted}),await reference({weighted}));}
});

test('PostgreSQL planning protects pending work without samples and refuses partial output above resource limits',integration,async t=>{
  const {store,at,row,load,reference}=await planningFixture(t);await load([row('running','gpt-6.1-sol',0,{ended:null,status:'running'})]);const reader=createPostgresPlanningInput(store,{clock:()=>at});assertPlanningEqual(await reader.plan(),await reference());
  await load([row('heavy','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1000)]);
  for(const options of [{maxSessions:1},{maxAccounts:1}])await assert.rejects(createPostgresPlanningInput(store,{clock:()=>at,...options}).plan(),{code:'planning_capacity'});
  await load([row('heavy','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1000,{price_version:'new'})]);await assert.rejects(createPostgresPlanningInput(store,{clock:()=>at,maxVersions:1}).plan(),{code:'planning_capacity'});
  await store.run("DELETE FROM harbor.request_prices WHERE request_id='synthetic-planning-1'");const missing=await reader.plan();assert.ok(missing.skipped.some(item=>item.reason==='unknown_fixed_load'));assert.equal(missing.moves.length,0);
});

test('PostgreSQL planning reads session aggregates rather than full request history and uses indexed window candidates',integration,async t=>{
  const {store,at,member}=await planningFixture(t);await store.transaction(async()=>{
    await store.query('SET LOCAL statement_timeout=120000');
    await store.run(`INSERT INTO harbor.requests(id,member_id,key_id,session_id,started,ended,status,ip,model,input,price_version)
      SELECT 'synthetic-planning-volume-'||i,$1,$2,CASE WHEN i%2=0 THEN 'heavy' ELSE 'other' END,
        CASE WHEN i<=99000 THEN $3::bigint-7*86400000 ELSE $3::bigint-7200000 END,CASE WHEN i<=99000 THEN $3::bigint-7*86400000+1000 ELSE $3::bigint-3600000 END,
        'ok','synthetic','gpt-6.1-sol',100+i%100,'old' FROM generate_series(1,100000) i`,member.memberId,member.keyId,at);
    await store.run(`INSERT INTO harbor.request_prices(request_id,state,model,model_source,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at)
      SELECT id,'priced',model,'requested',price_version,input::numeric*2000000,0,0,0,input::numeric*2000000,$1 FROM harbor.requests WHERE started>$1::bigint-86400000`,at);
  });
  await store.run("INSERT INTO harbor.requests(id,member_id,key_id,session_id,started,status,ip,model,price_version) VALUES('synthetic-planning-live',$1,$2,'running',$3,'running','synthetic','gpt-6.1-sol','new')",member.memberId,member.keyId,at-7200000);await store.projectPrice(await store.get("SELECT * FROM harbor.requests WHERE id='synthetic-planning-live'"),at);await store.query('ANALYZE harbor.requests');await store.query('ANALYZE harbor.request_prices');
  const all=store.all.bind(store);let largest=0,workloadQuery;store.all=async(sql,...args)=>{const result=await all(sql,...args);largest=Math.max(largest,result.length);if(sql.startsWith('WITH source AS MATERIALIZED'))workloadQuery={sql,args};return result;};
  const begin=performance.now(),input=await createPostgresPlanningInput(store,{clock:()=>at}).read(),elapsed=performance.now()-begin;assert.equal(input.workloads.length,3);assert.equal(largest,3);assert.ok(input.workloads.every(row=>!Object.hasOwn(row,'input')&&!Object.hasOwn(row,'output')&&!Object.hasOwn(row,'cached')));
  assert.equal(input.workloads.find(row=>row.id==='heavy').measuredAtoms,149000000000n);assert.equal(input.workloads.find(row=>row.id==='other').measuredAtoms,150000000000n);assert.equal(input.workloads.find(row=>row.id==='running').estimatedAtoms,598000000);assert.equal(input.workloads.find(row=>row.id==='running').running,true);
  const explain=await store.get('EXPLAIN (FORMAT JSON) '+workloadQuery.sql,...workloadQuery.args),uses=(plan,index)=>plan['Index Name']===index||(plan.Plans||[]).some(child=>uses(child,index));assert.ok(uses(explain['QUERY PLAN'][0].Plan,'request_time'));assert.ok(uses(explain['QUERY PLAN'][0].Plan,'request_status_page'));
  await store.transaction(async()=>{await store.query('SET LOCAL statement_timeout=120000');await store.run('UPDATE harbor.requests SET started=$1::bigint-7200000,ended=$1::bigint-3600000 WHERE ended<$1::bigint-86400000',at);await store.run(`INSERT INTO harbor.request_prices(request_id,state,model,model_source,revision,input_atoms,output_atoms,cached_atoms,adjustment_atoms,total_atoms,measured_at)
    SELECT r.id,'priced',r.model,'requested',r.price_version,r.input::numeric*2000000,0,0,0,r.input::numeric*2000000,$1 FROM harbor.requests r WHERE r.ended IS NOT NULL AND NOT EXISTS(SELECT 1 FROM harbor.request_prices p WHERE p.request_id=r.id)`,at);});await store.query('ANALYZE harbor.requests');await store.query('ANALYZE harbor.request_prices');
  const fullBegin=performance.now(),full=await createPostgresPlanningInput(store,{clock:()=>at}).read(),fullElapsed=performance.now()-fullBegin;assert.equal(full.workloads.length,3);assert.equal(largest,3);assert.equal(full.workloads.find(row=>row.id==='heavy').measuredAtoms,14900000000000n);assert.equal(full.workloads.find(row=>row.id==='other').measuredAtoms,15000000000000n);assert.equal(full.workloads.find(row=>row.id==='running').estimatedAtoms,598000000);
  t.diagnostic(JSON.stringify({syntheticRequests:100001,workloads:input.workloads.length,largestReturnedRows:largest,sparseWindowMs:Math.round(elapsed),fullWindowMs:Math.round(fullElapsed)}));
});

test('PostgreSQL planning account and workload inputs share a repeatable snapshot during concurrent changes',integration,async t=>{
  const {store,name,at,row,load}=await planningFixture(t);await load([row('heavy','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1000)]);const writer=new PostgresStore(options(name)),all=store.all.bind(store);let changed=false;
  try{
    store.all=async(sql,...args)=>{const result=await all(sql,...args);if(!changed&&sql.includes('FROM harbor.accounts WHERE')){changed=true;await writer.transaction(async()=>{await writer.run("UPDATE harbor.accounts SET route_epoch=route_epoch+1 WHERE id='a'");await writer.run("UPDATE harbor.requests SET input=9999 WHERE id='synthetic-planning-1'");await writer.projectPrice(await writer.get("SELECT * FROM harbor.requests WHERE id='synthetic-planning-1'"),at);});}return result;};
    const reader=createPostgresPlanningInput(store,{clock:()=>at}),before=await reader.read();assert.equal(before.accounts.find(a=>a.id==='a').route_epoch,0);assert.equal(before.workloads.find(s=>s.id==='heavy').measuredAtoms,2000000000n);
    const after=await reader.read();assert.equal(after.accounts.find(a=>a.id==='a').route_epoch,1);assert.equal(after.workloads.find(s=>s.id==='heavy').measuredAtoms,19998000000n);
  }finally{store.all=all;await writer.close();}
});

test('PostgreSQL planning keeps multi-account groups, preparation and temporary protection compatible',integration,async t=>{
  const {store,at,member,row,load,reference}=await planningFixture(t);await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) SELECT 'c','c','synthetic','ready',quota_json FROM harbor.accounts WHERE id='a'");await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES('third',$1,'c','responses',$2,$2)",member.memberId,at-7200000);
  const reader=createPostgresPlanningInput(store,{clock:()=>at});await load([row('heavy','gpt-6-astra',600),row('small-1','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1500),row('third','gpt-6.1-sol',800)]);assertPlanningEqual(await reader.plan({weighted:true}),await reference({weighted:true}));
  await store.run("UPDATE harbor.sessions SET nightly_enabled=0,nightly_expires=$1 WHERE id='heavy'",at+3600000);await store.run("UPDATE harbor.sessions SET enabled=0 WHERE id='small-1'");assertPlanningEqual(await reader.plan(),await reference());const preparing=new Set([member.memberId]);assertPlanningEqual(await reader.plan({preparing}),await reference({preparing}));
  await store.run("INSERT INTO harbor.accounts(id,name,home,state,removed_at) SELECT 'synthetic-removed-'||i,'synthetic','synthetic','ready',$1 FROM generate_series(1,10) i",at);await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode) VALUES('synthetic-api-candidate','synthetic','synthetic','ready','openai_api')");assert.equal((await createPostgresPlanningInput(store,{clock:()=>at,maxAccounts:3}).read()).accounts.length,3);
});

test('PostgreSQL planning repricing respects each sample time within the target revision alias range',integration,async t=>{
  const {store,at,row,load,reference,books}=await planningFixture(t);books.new.aliases[0].from=at-3600000;await store.run("UPDATE harbor.pricing_versions SET book=$1 WHERE id='new'",JSON.stringify(books.new));await load([row('heavy','synthetic-alias',1000),row('other','gpt-6.1-sol',3000),row('running','gpt-6.1-sol',0,{ended:null,status:'running',price_version:'new'})]);
  const reader=createPostgresPlanningInput(store,{clock:()=>at}),input=await reader.read();assert.equal(input.workloads.find(s=>s.id==='running').estimatedAtoms,12000000000);assertPlanningEqual(await reader.plan(),await reference());
});

test('PostgreSQL preview Worker persists an immutable complete plan, pages it and reuses committed results',integration,async t=>{
  const {store,at,row,load,reference}=await planningFixture(t);await load([row('heavy','gpt-6-astra',600),row('small-1','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1500)]);
  await store.run("UPDATE harbor.sessions SET client_session_id=md5(id)::uuid::text");
  await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_strategy','{\"weighted\":true}')");const expected=await reference({weighted:true}),before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');
  let now=at;const jobs=createPostgresJobs(store,{clock:()=>now}),planning=createPostgresPlanningJobs(store,{jobs,clock:()=>now}),queued=await planning.enqueue();assert.equal((await planning.status(queued.id)).ready,false);await assert.rejects(planning.page(queued.id),{code:'planning_pending'});
  const job=await jobs.claim(['routing.preview']),result=await planning.handlers[job.kind](job);assert.deepEqual(result,{plan_id:queued.id});assert.ok(Buffer.byteLength(JSON.stringify(result))<65536);
  assert.deepEqual(await planning.handlers[job.kind](job),result);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,1);now+=30001;const next=await jobs.claim(['routing.preview']);assert.notEqual(next.claim_token,job.claim_token);const restarted=createPostgresPlanningJobs(store,{jobs,clock:()=>now});assert.deepEqual(await restarted.handlers[next.kind](next),result);await assert.rejects(planning.handlers[job.kind](job),{code:'job_lease_lost'});await jobs.complete(next,result);
  const view=await planning.status(queued.id);assert.equal(view.status,'completed');assert.equal(view.created,at);assert.equal(view.planned_at,at);assert.equal(view.plan.settings.weighted,true);assert.equal(view.plan.preparationChecked,false);assert.equal(view.plan.counts.moves,expected.moves.length);
  const collect=async section=>{let after=0;const rows=[];for(;;){const page=await planning.page(queued.id,{section,after,limit:1});assert.ok(page.items.length<=1);rows.push(...page.items.map(({position,...item})=>item));if(!page.has_more)return rows;assert.ok(page.after>after);after=page.after;}};
  const moves=await collect('moves');assert.ok(moves.length>0);const identities=new Map((await store.all('SELECT id,client_session_id FROM harbor.sessions')).map(row=>[row.id,row.client_session_id]));assert.ok(moves.every(row=>row.client_session_id===identities.get(row.session_id)&&row.protocol==='responses'));const strip=rows=>rows.map(({client_session_id,protocol,...row})=>row);assert.deepEqual(strip(moves),expected.moves);assert.deepEqual(strip(await collect('skipped')),expected.skipped);
  const groups=await collect('groups'),fixed=await collect('fixed');assert.deepEqual(groups.map(({group,...value})=>({...value,fixed:fixed.filter(item=>item.group===group).map(item=>item.session_id)})),expected.summaries);
  assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);
  for(const query of [{section:'accounts'},{section:'sessions'},{after:-1},{limit:101}])await assert.rejects(planning.page(queued.id,query),{code:'planning_query_invalid'});
  const expired=createPostgresPlanningJobs(store,{jobs,clock:()=>at+3600000});await assert.rejects(expired.status(queued.id),{code:'planning_expired'});await assert.rejects(expired.page(queued.id),{code:'planning_expired'});
});

test('PostgreSQL preview atomic storage rejects capacity, cancellation and lost leases without partial plans',integration,async t=>{
  const {store,at,row,load}=await planningFixture(t);await load([row('heavy','gpt-6-astra',600),row('other','gpt-6.1-sol',1500)]);let now=at;
  const jobs=createPostgresJobs(store,{clock:()=>now,leaseMs:1000}),small=createPostgresPlanningJobs(store,{jobs,clock:()=>now,maxPlanBytes:1024,maxItemBytes:512});let queued=await small.enqueue(),job=await jobs.claim(['routing.preview']);await assert.rejects(small.handlers[job.kind](job),{code:'planning_capacity'});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,0);await jobs.cancel(queued.id);
  const planning=createPostgresPlanningJobs(store,{jobs,clock:()=>now});queued=await planning.enqueue();job=await jobs.claim(['routing.preview']);const controller=new AbortController();controller.abort();await assert.rejects(planning.handlers[job.kind](job,{signal:controller.signal}),{code:'worker_stopping'});await jobs.cancel(queued.id);
  queued=await planning.enqueue();job=await jobs.claim(['routing.preview']);const run=store.run.bind(store);let injected=false;store.run=async(sql,...args)=>{const result=await run(sql,...args);if(sql.startsWith('INSERT INTO harbor.routing_plan_items')&&!injected){injected=true;now+=1001;}return result;};
  try{await assert.rejects(planning.handlers[job.kind](job),{code:'job_lease_lost'});}finally{store.run=run;}assert.equal(injected,true);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plan_items')).n,0);
  const replacement=await jobs.claim(['routing.preview']);assert.equal(replacement.id,queued.id);assert.notEqual(replacement.claim_token,job.claim_token);await planning.handlers[replacement.kind](replacement);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,1);
});

test('PostgreSQL preview keeps plans larger than job result limits complete and bounds each returned page',integration,async t=>{
  const {store,at,member,row,load}=await planningFixture(t);await load([row('heavy','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1000)]);
  await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) SELECT 'synthetic-many-'||i,$1,'a','responses',$2::bigint-7200000,$2::bigint-7200000 FROM generate_series(1,1200)i",member.memberId,at);
  await store.run("INSERT INTO harbor.requests(id,member_id,key_id,session_id,started,status,ip,model,price_version) SELECT 'synthetic-many-r-'||i,$1,$2,'synthetic-many-'||i,$3::bigint-7200000,'running','synthetic','gpt-6.1-sol','old' FROM generate_series(1,1200)i",member.memberId,member.keyId,at);
  await store.run("INSERT INTO harbor.request_prices(request_id,state,model,model_source,revision,measured_at) SELECT id,'pending',model,'requested',price_version,$1 FROM harbor.requests WHERE status='running'",at);
  const jobs=createPostgresJobs(store,{clock:()=>at}),planning=createPostgresPlanningJobs(store,{jobs,clock:()=>at,pageBytes:256}),queued=await planning.enqueue(),job=await jobs.claim(['routing.preview']),result=await planning.handlers[job.kind](job);await jobs.complete(job,result);
  const stored=await store.get('SELECT bytes FROM harbor.routing_plans WHERE id=$1',queued.id);assert.ok(stored.bytes>65536);assert.equal((await planning.status(queued.id)).plan.counts.sessions,1202);
  let after=0,count=0;for(;;){const page=await planning.page(queued.id,{section:'fixed',after,limit:100});assert.ok(page.items.length<100);count+=page.items.length;if(!page.has_more)break;assert.ok(page.after>after);after=page.after;}assert.equal(count,1200);
});

test('PostgreSQL gateway admin preview queue reaches an independent Worker without model calls or binding changes',integration,async t=>withGateway(t,async({store,name,password,member,open})=>{
  const at=Date.now(),app=await open({clock:()=>at}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),worker=await createPostgresWorker({database:options(name)});
  const base='/api/admin/balancing/plans',call=(path,method='GET',body,credential=cookie)=>fetch(origin+path,{method,headers:{cookie:credential,'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});
  try{
    assert.equal((await call(base,'POST',{},'')).status,401);const employee=await fetch(origin+base,{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:'{}'});assert.equal(employee.status,401);
    assert.equal((await call(base,'POST',{weighted:true})).status,400);let response=await call(base,'POST',{});assert.equal(response.status,202);const task=await response.json();assert.equal((await (await call(base+'/'+task.id)).json()).status,'pending');
    const before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');await worker.runOnce();await gatewayWait(async()=>{const row=await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',task.id);return row.status==='completed';});
    response=await call(base+'/'+task.id);assert.equal(response.status,200);assert.equal((await response.json()).ready,true);response=await call(base+'/'+task.id+'/items?section=moves&limit=1');assert.equal(response.status,200);assert.deepEqual((await response.json()).items,[]);
    for(const query of ['section=accounts','limit=0','after=1e2','foo=1'])assert.equal((await call(base+'/'+task.id+'/items?'+query)).status,400);
    assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
  }finally{await worker.close();}
}));

test('PostgreSQL preview captures configuration and workload together while cancellation during persistence rolls back all rows',integration,async t=>{
  const {store,name,at,row,load}=await planningFixture(t);await load([row('heavy','gpt-6-astra',600),row('other','gpt-6.1-sol',1500)]);await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_strategy','{\"weighted\":false}')");
  const jobs=createPostgresJobs(store,{clock:()=>at}),planning=createPostgresPlanningJobs(store,{jobs,clock:()=>at}),writer=new PostgresStore(options(name)),all=store.all.bind(store);let changed=false;
  try{
    store.all=async(sql,...args)=>{const rows=await all(sql,...args);if(!changed&&sql.includes("name IN ('balancing','routing_strategy','routing_weights')")){changed=true;await writer.transaction(async()=>{await writer.run("UPDATE harbor.gateway_settings SET value='{\"weighted\":true}' WHERE name='routing_strategy'");await writer.run("UPDATE harbor.accounts SET route_epoch=1 WHERE id='a'");});}return rows;};
    const task=await planning.enqueue(),job=await jobs.claim(['routing.preview']);await planning.handlers[job.kind](job);assert.equal((await planning.status(task.id)).plan.settings.weighted,false);const account=await store.get("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='accounts' AND payload->>'id'='a'",task.id);assert.equal(account.payload.route_epoch,0);await jobs.complete(job,{plan_id:task.id});
  }finally{store.all=all;await writer.close();}
  const queued=await planning.enqueue(),job=await jobs.claim(['routing.preview']),controller=new AbortController(),run=store.run.bind(store);let inserted=false;
  store.run=async(sql,...args)=>{const result=await run(sql,...args);if(sql.startsWith('INSERT INTO harbor.routing_plan_items')){inserted=true;controller.abort();}return result;};
  try{await assert.rejects(planning.handlers[job.kind](job,{signal:controller.signal}),{code:'worker_stopping'});}finally{store.run=run;}
  assert.equal(inserted,true);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans WHERE id=$1',queued.id)).n,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plan_items WHERE plan_id=$1',queued.id)).n,0);assert.equal((await store.get('SELECT result FROM harbor.background_jobs WHERE id=$1',queued.id)).result,null);
});

test('R26 Worker retains heartbeat capacity while two real PostgreSQL preview snapshots wait for table locks',integration,async t=>{
  const {store,name}=await fixture(t);await store.migrate();await store.initializePricing();const worker=await createPostgresWorker({database:{...options(name),connectionTimeoutMs:100},jobs:{leaseMs:1000}}),blocker=new PostgresStore(options(name));let release,blocked;
  try{
    const tasks=await Promise.all([worker.planning.enqueue(),worker.planning.enqueue()]);
    blocked=blocker.transaction(async()=>{await blocker.query('LOCK TABLE harbor.requests IN ACCESS EXCLUSIVE MODE');await new Promise(resolve=>{release=resolve;});});await gatewayWait(()=>!!release);
    await worker.runOnce();await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=$1 AND wait_event_type='Lock' AND query LIKE '%SELECT DISTINCT price_version%'",name)).n===2);
    await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE id=ANY($1::text[]) AND status='running' AND lease_until>claimed_at+1000",tasks.map(task=>task.id))).n===2,2000);
    assert.equal(worker.status().active,2);assert.equal(worker.status().failed,0);release();await blocked;blocked=null;
    await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE id=ANY($1::text[]) AND status='completed'",tasks.map(task=>task.id))).n===2);
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,2);assert.equal(worker.status().failed,0);
  }finally{release?.();await blocked;await worker.close();await blocker.close();}
});

test('R26 Worker rejects pool configurations without control capacity and releases every connection on close',integration,async t=>{
  const {store,name}=await fixture(t);await store.migrate();
  for(const [concurrency,poolSize] of [[1,1],[2,2],[2,3],[8,8],[0,2],[9,10],[2,65]])await assert.rejects(createPostgresWorker({database:options(name),poolSize,runtime:{concurrency}}),{code:'worker_configuration'});
  const worker=await createPostgresWorker({database:options(name),poolSize:2,runtime:{concurrency:1}});assert.equal(worker.store.pool.options.max,2);await worker.close();assert.deepEqual(worker.store.status(),{connections:0,idle:0,waiting:0,pool_errors:0});
});

test('R26 an expiry heartbeat waiting for its effect lock cannot starve a concurrent preview heartbeat',integration,async t=>{
  const {store,name}=await fixture(t);await store.migrate();await store.initializePricing();const worker=await createPostgresWorker({database:{...options(name),connectionTimeoutMs:100},jobs:{leaseMs:1000}}),blocker=new PostgresStore(options(name));let release,blocked;
  try{
    const task=await worker.planning.enqueue();blocked=blocker.transaction(async()=>{await lockPostgresRouting(blocker);await blocker.query('LOCK TABLE harbor.requests IN ACCESS EXCLUSIVE MODE');await new Promise(resolve=>{release=resolve;});});await gatewayWait(()=>!!release);
    await worker.runOnce();await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=$1 AND wait_event_type='Lock' AND (query LIKE '%SELECT DISTINCT price_version%' OR query LIKE '%harbor-routing-state%')",name)).n>=2);
    await gatewayWait(async()=>{const row=await store.get('SELECT lease_until,claimed_at FROM harbor.background_jobs WHERE id=$1',task.id);return row.lease_until>row.claimed_at+1600;},2000);
    release();await blocked;blocked=null;await gatewayWait(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE status='completed'")).n===2);assert.equal(worker.status().failed,0);
  }finally{release?.();await blocked;await worker.close();await blocker.close();}
});

test('PostgreSQL preparation stays protected between complete upload and committed request admission',integration,async t=>withGateway(t,async({store,member,open})=>{
  let releaseLock,entered=false,finishModel;const modelGate=new Promise(resolve=>{finishModel=resolve;}),app=await open({responsesTransport:controlledResponses(async()=>{entered=true;await modelGate;})}),origin=await listenGateway(app);
  const locked=store.transaction(async()=>{await store.query('SELECT id FROM harbor.members WHERE id=$1 FOR UPDATE',[member.memberId]);await new Promise(resolve=>{releaseLock=resolve;});});await gatewayWait(()=>!!releaseLock);const pending=gatewaySend(origin,member.secret);
  try{
    await gatewayWait(()=>app.status().routingPreparation.readers===1&&app.status().preparing===0);await gatewayWait(async()=> (await app.queryStore.get("SELECT COUNT(*) n FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%harbor.members%' AND query LIKE '%FOR SHARE%'")).n>0);assert.equal(app.preparation.reserve([member.memberId]),null);assert.equal((await app.queryStore.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
    releaseLock();await locked;await gatewayWait(()=>entered);assert.equal(app.status().routingPreparation.readers,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE status='running'")).n,1);
  }finally{releaseLock?.();await locked;finishModel();}
  assert.equal((await pending).status,200);await gatewayWait(()=>app.status().active===0);assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});
}));

for(const commit of [true,false])test('PostgreSQL a new request waits through binding transaction '+(commit?'commit':'rollback')+' without model replay',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),initial=await gatewaySend(origin,member.secret);assert.equal(initial.status,200);await initial.json();await gatewayWait(()=>app.status().active===0);
  const session=await store.get('SELECT * FROM harbor.sessions');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-migration-target','合成迁移目标','synthetic','ready')");
  const release=app.preparation.reserve([member.memberId]);assert.ok(release);let finishTransaction,transaction,pending;
  try{
    transaction=store.transaction(async()=>{await lockPostgresRouting(store);await store.run("UPDATE harbor.sessions SET account_id='synthetic-migration-target',route_version=route_version+1 WHERE id=$1",session.id);await new Promise(resolve=>{finishTransaction=resolve;});if(!commit)throw Error('synthetic migration rollback');});transaction.catch(()=>{});await gatewayWait(()=>!!finishTransaction);
    pending=gatewaySend(origin,member.secret);await gatewayWait(()=>app.status().routingPreparation.waiting===1);assert.equal(app.status().preparing,0);assert.equal(calls,1);assert.equal((await app.queryStore.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
    finishTransaction();if(commit)await transaction;else await assert.rejects(transaction,/synthetic migration rollback/);release();
    const response=await pending;assert.equal(response.status,200);await response.json();await gatewayWait(()=>app.status().active===0);assert.equal(calls,2);
    const rows=await store.all('SELECT account_id,route_version FROM harbor.requests ORDER BY query_sequence');assert.deepEqual(rows,[{account_id:'synthetic-runtime',route_version:0},{account_id:commit?'synthetic-migration-target':'synthetic-runtime',route_version:commit?1:0}]);
    assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});
  }finally{finishTransaction?.();await transaction?.catch(()=>{});release();if(pending)await pending.catch(()=>{});}
}));

test('PostgreSQL client cancellation and gateway shutdown clear requests waiting for a preparation reservation',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0;const app=await open({drainTimeoutMs:100,responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),release=app.preparation.reserve([member.memberId]),controller=new AbortController();
  try{
    const cancelled=fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成等待取消'}),signal:controller.signal});cancelled.catch(()=>{});await gatewayWait(()=>app.status().routingPreparation.waiting===1);controller.abort();await assert.rejects(cancelled,{name:'AbortError'});await gatewayWait(()=>app.status().active===0);assert.equal(app.status().routingPreparation.waiting,0);
    const waiting=gatewaySend(origin,member.secret).then(response=>({response}),error=>({error}));await gatewayWait(()=>app.status().routingPreparation.waiting===1);await app.close();const result=await waiting;if(result.response)assert.equal(result.response.status,503);else assert.equal(result.error.cause?.code,'UND_ERR_SOCKET');assert.equal(app.status().routingPreparation.waiting,0);assert.equal(app.status().routingPreparation.readers,0);assert.equal(calls,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
  }finally{release();}
  assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});
}));

test('PostgreSQL real manual rebind holds the preparation reservation until its audit and transaction commit',integration,async t=>withGateway(t,async({store,member,password,open})=>{
  let calls=0;const app=await open({resources:{corePoolSize:2,queryPoolSize:1,recordingPoolSize:1},responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),first=await gatewaySend(origin,member.secret);assert.equal(first.status,200);await first.json();await gatewayWait(()=>app.status().active===0);
  const session=await store.get('SELECT * FROM harbor.sessions'),cookie=await gatewayLogin(origin,password);await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-manual-target','合成人工目标','synthetic','ready')");
  const run=app.store.run.bind(app.store);let finishAudit,auditHeld=false,change,next;
  app.store.run=async(sql,...args)=>{const result=await run(sql,...args);if(sql.startsWith('INSERT INTO harbor.audit')&&args[2]==='session.route'){auditHeld=true;await new Promise(resolve=>{finishAudit=resolve;});}return result;};
  try{
    change=fetch(origin+'/api/admin/sessions/'+session.id,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({account_id:'synthetic-manual-target',expected_route_version:0})});await gatewayWait(()=>auditHeld);assert.equal(app.preparation.status().blockedMembers,1);
    next=gatewaySend(origin,member.secret);await gatewayWait(()=>app.preparation.status().waiting===1);assert.equal(calls,1);assert.equal((await app.queryStore.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
    finishAudit();assert.equal((await change).status,200);const response=await next;assert.equal(response.status,200);await response.json();await gatewayWait(()=>app.status().active===0);assert.equal(calls,2);assert.deepEqual(await store.all('SELECT account_id FROM harbor.requests ORDER BY query_sequence'),[{account_id:'synthetic-runtime'},{account_id:'synthetic-manual-target'}]);assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});
  }finally{finishAudit?.();app.store.run=run;if(change)await change;if(next)await next;}
}));

test('PostgreSQL invalid input, model denial and failed admission release preparation ownership before a later valid request',integration,async t=>withGateway(t,async({store,member,open})=>{
  let calls=0;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),headers={authorization:'Bearer '+member.secret,'content-type':'application/json'};
  for(const body of ['{broken',JSON.stringify({model:'synthetic-forbidden',input:'合成权限拒绝'})]){const response=await fetch(origin+'/v1/responses',{method:'POST',headers,body});assert.ok([400,403].includes(response.status));await gatewayWait(()=>app.status().active===0);assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});}
  const observe=app.store.observe.bind(app.store);app.store.observe=async(...args)=>{await observe(...args);throw Error('synthetic admission rollback');};
  try{assert.equal((await gatewaySend(origin,member.secret)).status,500);}finally{app.store.observe=observe;}
  await gatewayWait(()=>app.status().active===0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions')).n,0);assert.equal(calls,0);assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});
  const response=await gatewaySend(origin,member.secret);assert.equal(response.status,200);await response.json();await gatewayWait(()=>app.status().active===0);assert.equal(calls,1);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
}));

async function nightlyFixture(t){
  const value=await planningFixture(t),{store,at,row,load,name}=value;await load([row('heavy','gpt-6-astra',600),row('small-1','gpt-6.1-sol',1000),row('other','gpt-6.1-sol',1500)]);await store.run('UPDATE harbor.requests r SET account_id=s.account_id,route_version=s.route_version FROM harbor.sessions s WHERE s.id=r.session_id');
  await store.run("INSERT INTO harbor.gateway_settings VALUES('balancing',$1)",JSON.stringify({nightly:true,failover:false,quietMinutes:30}));
  const worker=await createPostgresWorker({database:options(name),clock:()=>at}),preparation=createRoutingPreparation(),applier=createPostgresNightly(store,{preparation,refreshQuotas:async()=>{},clock:()=>at});
  const plan=async()=>{await store.run('INSERT INTO harbor.nightly_cycle_ready VALUES($1,$2) ON CONFLICT(day) DO NOTHING','2026-11-03',at);await worker.runOnce();await gatewayWait(async()=>{await worker.runOnce();return (await store.get("SELECT status FROM harbor.background_jobs WHERE kind='routing.nightly'"))?.status==='completed';});return (await store.get("SELECT id FROM harbor.background_jobs WHERE kind='routing.nightly'")).id;};
  return {...value,worker,preparation,applier,plan};
}

test('PostgreSQL nightly Worker deduplicates a day, applies an atomic bidirectional group once and preserves request ownership',integration,async t=>{
  const {store,at,worker,preparation,applier,plan}=await nightlyFixture(t);
  try{
    const planned=await plan(),moves=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' ORDER BY ordinal",planned)).map(r=>r.payload);assert.equal(moves.length,2);assert.ok(moves.some(m=>m.from==='a')&&moves.some(m=>m.from==='b'));
    const history=await store.all('SELECT id,account_id,route_version FROM harbor.requests ORDER BY id');await applier.apply(planned);const sessions=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');for(const move of moves){const actual=sessions.find(s=>s.id===move.session_id);assert.equal(actual.account_id,move.to);assert.equal(actual.route_version,move.version+1);}
    assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.requests ORDER BY id'),history);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,2);assert.deepEqual(await store.get('SELECT status,moved FROM harbor.nightly_group_results'),{status:'applied',moved:2});assert.equal((await store.get('SELECT status FROM harbor.balance_runs')).status,'completed');
    await applier.apply(planned);await worker.runOnce();assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind='routing.nightly'")).n,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,2);assert.deepEqual(preparation.status(),{readers:0,waiting:0,blockedMembers:0});
    const preview=await worker.planning.enqueue();await worker.runOnce();await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',preview.id)).status==='completed');await assert.rejects(applier.apply(preview.id),{code:'nightly_plan_invalid'});
    await store.transaction(async()=>{await assert.rejects(applier.apply(planned),{code:'nightly_transaction_invalid'});});
  }finally{await applier.close();await worker.close();}
});

for(const condition of ['preparing','running','member-disabled','excluded','version','quota','settings'])test('PostgreSQL nightly skips the entire group when '+condition+' changes after planning',integration,async t=>{
  const {store,at,member,worker,preparation,applier,plan}=await nightlyFixture(t);let release;
  try{
    const planned=await plan();
    if(condition==='preparing')release=await preparation.enter(member.memberId);
    if(condition==='running')await store.run("INSERT INTO harbor.requests(id,member_id,key_id,session_id,started,status,ip) VALUES('synthetic-night-busy',$1,$2,'heavy',$3,'running','synthetic')",member.memberId,member.keyId,at);
    if(condition==='member-disabled')await store.run('UPDATE harbor.members SET enabled=0 WHERE id=$1',member.memberId);
    if(condition==='excluded')await store.run("UPDATE harbor.sessions SET nightly_enabled=0,nightly_expires=$1 WHERE id='heavy'",at+3600000);
    if(condition==='version')await store.run("UPDATE harbor.sessions SET route_version=route_version+1 WHERE id='heavy'");
    if(condition==='quota')await store.run("UPDATE harbor.accounts SET quota_json=jsonb_set(quota_json::jsonb,'{windows,0,used}','51')::text WHERE id='a'");
    if(condition==='settings')await store.run("UPDATE harbor.gateway_settings SET value='{\"nightly\":false}' WHERE name='balancing'");
    const before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');await applier.apply(planned);assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);assert.deepEqual(await store.get('SELECT status,moved FROM harbor.nightly_group_results'),{status:'skipped',moved:0});assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);assert.equal((await store.get('SELECT status FROM harbor.balance_runs')).status,'completed');
  }finally{release?.();await applier.close();await worker.close();}
});

test('PostgreSQL nightly audit failure rolls back both directions and a restarted applier resumes without duplicate migration',integration,async t=>{
  const {store,at,worker,preparation,applier,plan}=await nightlyFixture(t);let resumed;
  try{
    const planned=await plan(),before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit SELECT'))throw Error('synthetic nightly audit failure');return run(sql,...args);};
    try{await assert.rejects(applier.apply(planned),/synthetic nightly audit failure/);}finally{store.run=run;}
    assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,0);assert.deepEqual(preparation.status(),{readers:0,waiting:0,blockedMembers:0});
    await applier.close();resumed=createPostgresNightly(store,{preparation,refreshQuotas:async()=>{},clock:()=>at});await resumed.apply(planned);await resumed.apply(planned);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,2);
  }finally{await resumed?.close();await applier.close();await worker.close();}
});

for(const quotaFailed of [false,true])for(const time of ['02:00:10','10:00:10','13:00:10','16:00:10','20:00:10','12:59:00'])test('PostgreSQL gateway and independent Worker run the real nightly cycle after quota refresh, failure '+quotaFailed+' at '+time,integration,async t=>withGateway(t,async({store,name,member,password,open})=>{
  const at=Date.parse('2026-11-03T'+time+'+08:00'),boundary=transferWindow(at).end;let quotaCalls=0,modelCalls=0;
  await store.run('UPDATE harbor.keys SET expires=$1 WHERE id=$2',at+86400000,member.keyId);
  await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-peer','合成夜间目标','synthetic','ready')");await store.run("UPDATE harbor.accounts SET quota_json=$1,route_block=CASE WHEN id='synthetic-runtime' THEN 'quota_exhausted' END",JSON.stringify({plan:'synthetic-plan',checked:boundary-1000,remaining:0,limitReached:true,windows:[{minutes:10080,used:100,reset:boundary}]}));
  await store.run("INSERT INTO harbor.gateway_settings VALUES('balancing','{\"nightly\":true,\"failover\":false,\"quietMinutes\":30}'),('routing_strategy','{\"weighted\":true}')");
  const revision=(await store.get('SELECT id FROM harbor.pricing_versions ORDER BY created DESC,id LIMIT 1')).id;
  for(let i=0;i<10;i++){const sid='synthetic-night-session-'+i,rid='synthetic-night-request-'+i;await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) VALUES($1,$2,'synthetic-runtime','responses',$3,$3)",sid,member.memberId,at-7200000);await store.run("INSERT INTO harbor.requests(id,member_id,key_id,session_id,account_id,route_version,started,ended,status,ip,model,input,price_version) VALUES($1,$2,$3,$4,'synthetic-runtime',0,$5,$6,'ok','synthetic','gpt-6.1-sol',100,$7)",rid,member.memberId,member.keyId,sid,at-7200000,at-3600000,revision);await store.projectPrice(await store.get('SELECT * FROM harbor.requests WHERE id=$1',rid),at);await store.run("UPDATE harbor.requests SET handoff_context='replayable',handoff_input='replayable' WHERE id=$1",rid);}
  const app=await open({clock:()=>at,rateReader:async()=>{quotaCalls++;if(quotaFailed)throw Error('synthetic official quota unavailable');const recovered=syntheticQuota(50,at);recovered.ordinaryUsageAllowed=true;return recovered;},responsesTransport:controlledResponses(async()=>{modelCalls++;})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),worker=await createPostgresWorker({database:options(name),clock:()=>at,runtime:{pollMs:100}});
  try{
    worker.start();await gatewayWait(async()=> (await store.get('SELECT status FROM harbor.balance_runs'))?.status==='completed',10000);
    const run=await store.get('SELECT * FROM harbor.balance_runs'),summary=JSON.parse(run.summary);assert.equal(quotaCalls,2);assert.equal(modelCalls,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_cycle_ready')).n,1);assert.equal(run.plan_id,'routing.nightly:'+transferWindow(at).key);
    assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.sessions WHERE account_id='synthetic-peer'")).n,quotaFailed?0:5);assert.equal(summary.applied,quotaFailed?0:5);assert.equal(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='request_handoff_policy'")).value).mode,'request-boundary');const header=(await store.get('SELECT header FROM harbor.routing_plans WHERE id=$1',run.plan_id)).header;assert.equal(header.requestBoundary,true);if(quotaFailed)assert.ok(header.counts.skipped>=2);
    await app.nightly.runOnce();await worker.runOnce();assert.equal(quotaCalls,2);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.balance_runs')).n,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,quotaFailed?0:5);
    const response=await fetch(origin+'/api/admin/state',{headers:{cookie}});assert.equal(response.status,200);assert.equal((await response.json()).balancing.lastRun.status,'completed');assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});
  }finally{await worker.close();}
}));

test('PostgreSQL nightly resumes committed groups after acknowledgement loss without migrating them twice',integration,async t=>{
  const {store,at,worker,preparation,applier,plan}=await nightlyFixture(t);let resumed;
  try{
    const planned=await plan(),transaction=store.transaction.bind(store);let lost=false;
    store.transaction=async(operation,options)=>{const result=await transaction(operation,options);if(!lost&&(await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n){lost=true;throw Object.assign(Error('synthetic commit acknowledgement lost'),{code:'ECONNRESET'});}return result;};
    try{await assert.rejects(applier.apply(planned),{code:'ECONNRESET'});}finally{store.transaction=transaction;}
    assert.equal(lost,true);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,2);
    await applier.close();resumed=createPostgresNightly(store,{preparation,refreshQuotas:async()=>{},clock:()=>at});await resumed.apply(planned);assert.equal((await store.get('SELECT status FROM harbor.balance_runs')).status,'completed');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,2);
  }finally{await resumed?.close();await applier.close();await worker.close();}
});

test('PostgreSQL nightly resumes a later failed group without repeating an earlier committed group',integration,async t=>{
  const {store,at,worker,preparation,applier,plan}=await nightlyFixture(t);let resumed;
  try{
    for(const [id,source] of [['c','a'],['d','b']])await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) SELECT $1,$1,'synthetic','ready',jsonb_set(quota_json::jsonb,'{plan}','\"synthetic-second-plan\"')::text FROM harbor.accounts WHERE id=$2",id,source);
    await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated) SELECT 'second-'||id,member_id,CASE account_id WHEN 'a' THEN 'c' ELSE 'd' END,protocol,created,updated FROM harbor.sessions WHERE id IN ('heavy','small-1','other')");
    await store.run("INSERT INTO harbor.requests(id,member_id,key_id,session_id,account_id,route_version,started,ended,status,ip,model,input,output,cached,price_version) SELECT 'second-'||id,member_id,key_id,'second-'||session_id,CASE account_id WHEN 'a' THEN 'c' ELSE 'd' END,route_version,started,ended,status,ip,model,input,output,cached,price_version FROM harbor.requests");for(const row of await store.all("SELECT * FROM harbor.requests WHERE id LIKE 'second-%'"))await store.projectPrice(row,at);
    const planned=await plan(),run=store.run.bind(store);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='groups'",planned)).n,2);
    store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit SELECT')&&JSON.parse(args[0]).some(m=>m.from==='c'||m.from==='d'))throw Error('synthetic second group failure');return run(sql,...args);};
    try{await assert.rejects(applier.apply(planned),/synthetic second group failure/);}finally{store.run=run;}
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,1);const first=await store.all("SELECT id,account_id,route_version FROM harbor.sessions WHERE id NOT LIKE 'second-%' ORDER BY id");assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,2);
    await applier.close();resumed=createPostgresNightly(store,{preparation,refreshQuotas:async()=>{},clock:()=>at});await resumed.apply(planned);assert.deepEqual(await store.all("SELECT id,account_id,route_version FROM harbor.sessions WHERE id NOT LIKE 'second-%' ORDER BY id"),first);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,2);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,4);
  }finally{await resumed?.close();await applier.close();await worker.close();}
});

test('PostgreSQL nightly close waits for rollback and releases waiting readers only after the group transaction finishes',integration,async t=>{
  const {store,at,member,worker,preparation,applier,plan}=await nightlyFixture(t);let resumeAudit,reading,pending,closing;
  const run=store.run.bind(store);
  try{
    const planned=await plan(),before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');
    store.run=async(sql,...args)=>{const result=await run(sql,...args);if(sql.startsWith('INSERT INTO harbor.audit SELECT'))await new Promise(resolve=>{resumeAudit=resolve;});return result;};
    pending=applier.apply(planned);pending.catch(()=>{});await gatewayWait(()=>!!resumeAudit);reading=preparation.enter(member.memberId);assert.equal(preparation.status().waiting,1);let closed=false;closing=applier.close().then(()=>{closed=true;});assert.equal(closed,false);assert.equal(preparation.status().blockedMembers,1);
    resumeAudit();await assert.rejects(pending,{code:'service_stopping'});await closing;const releaseReader=await reading;releaseReader();assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);assert.deepEqual(preparation.status(),{readers:0,waiting:0,blockedMembers:0});
  }finally{resumeAudit?.();store.run=run;await pending?.catch(()=>{});if(reading)(await reading)();await closing;await applier.close();await worker.close();}
});

test('PostgreSQL nightly refuses expired plans without touching bindings and records the daily failure once',integration,async t=>{
  const {store,at,worker,preparation,applier,plan}=await nightlyFixture(t);let expired;
  try{const planned=await plan(),before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');expired=createPostgresNightly(store,{preparation,refreshQuotas:async()=>{},clock:()=>at+3600000});await expired.apply(planned);await expired.runOnce();assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);const run=await store.get('SELECT status,summary FROM harbor.balance_runs');assert.equal(run.status,'failed');assert.equal(JSON.parse(run.summary).reason,'planning_expired');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.balance_runs')).n,1);}finally{await expired?.close();await applier.close();await worker.close();}
});

test('PostgreSQL nightly scheduling recovers only the latest of five slots and respects the disabled switch',integration,async t=>{
  const {store,at}=await planningFixture(t);let now=at-11000;const jobs=createPostgresJobs(store,{clock:()=>now}),planning=createPostgresPlanningJobs(store,{jobs,clock:()=>now});assert.equal(await planning.schedule(),null);now=at;assert.equal(await planning.schedule(),null);
  await store.run("INSERT INTO harbor.gateway_settings VALUES('balancing','{\"nightly\":true}')");const first=await planning.schedule(),same=await planning.schedule();assert.equal(first.id,'routing.nightly:2026-11-03');assert.equal(first.created,true);assert.equal(same.created,false);
  now=at+60000;assert.deepEqual(await planning.schedule(),{id:first.id,created:false});
  now=Date.parse('2026-11-03T12:59:00+08:00');const recovered=await planning.schedule();assert.equal(recovered.id,'routing.nightly:2026-11-03-1000');
  now=Date.parse('2026-11-03T19:59:00+08:00');assert.equal((await planning.schedule()).id,'routing.nightly:2026-11-03-1600');
  const tasks=await store.all("SELECT target,dependency_id FROM harbor.background_jobs WHERE kind='routing.nightly' ORDER BY created");assert.deepEqual(tasks.map(t=>t.target),['2026-11-03','2026-11-03-1000','2026-11-03-1600']);assert.ok(tasks.every(t=>t.dependency_id==='routing.expiry:'+String(at-10000)));
  await store.run("UPDATE harbor.gateway_settings SET value='{\"nightly\":false}' WHERE name='balancing'");now=Date.parse('2026-11-03T20:00:00+08:00');assert.equal(await planning.schedule(),null);
});

for(const time of ['02:00:10','10:00:10','13:00:10','16:00:10','20:00:10','12:59:00'])test('PostgreSQL scheduled transfers actually apply and deduplicate the current slot at '+time,integration,async t=>{
  const {store,name,member,row,load}=await planningFixture(t),now=Date.parse('2026-11-03T'+time+'+08:00'),slot=time==='12:59:00'?'1000':time.slice(0,5).replace(':',''),key=slot==='0200'?'2026-11-03':'2026-11-03-'+slot;
  await load([row('heavy','gpt-6-astra',600,{started:now-7200000,ended:now-3600000}),row('small-1','gpt-6.1-sol',1000,{started:now-7200000,ended:now-3600000}),row('other','gpt-6.1-sol',1500,{started:now-7200000,ended:now-3600000})]);
  await store.run('UPDATE harbor.requests r SET account_id=s.account_id,route_version=s.route_version FROM harbor.sessions s WHERE s.id=r.session_id');
  await store.run("INSERT INTO harbor.gateway_settings VALUES('balancing','{\"nightly\":true}'),('routing_weights',$1)",JSON.stringify({mode:'auto',weights:{},version:2,expires:null}));
  const pinned=Date.parse('2026-11-04T02:00:00+08:00');await store.run("UPDATE harbor.sessions SET nightly_enabled=0,nightly_expires=$1 WHERE id='manual'",pinned);
  const preparation=createRoutingPreparation();let refreshes=0;
  const applier=createPostgresNightly(store,{preparation,clock:()=>now,refreshQuotas:async()=>{refreshes++;await store.run('UPDATE harbor.accounts SET quota_json=$1',JSON.stringify({plan:'synthetic-plan',checked:now,limitReached:false,windows:[{minutes:10080,used:50,reset:now+7*86400000}]}));}}),worker=await createPostgresWorker({database:options(name),clock:()=>now});
  try{
    await gatewayWait(async()=>{await worker.runOnce();await applier.runOnce();return (await store.get('SELECT status FROM harbor.balance_runs WHERE day=$1',key))?.status==='completed';},10000);
    const run=await store.get('SELECT * FROM harbor.balance_runs WHERE day=$1',key),plan=await store.get('SELECT header,expires FROM harbor.routing_plans WHERE id=$1',run.plan_id),moved=await store.all("SELECT id,account_id,route_version FROM harbor.sessions WHERE route_version>0 ORDER BY id");
    assert.equal(run.plan_id,'routing.nightly:'+key);assert.equal(plan.header.window.end,now);assert.equal(plan.header.window.boundary,Date.parse('2026-11-03T'+(time==='12:59:00'?'10:00:00':time.slice(0,5)+':00')+'+08:00'));assert.ok(plan.expires>now);assert.equal(moved.length,2);assert.equal(JSON.parse(run.summary).applied,2);assert.equal(refreshes,1);
    const history=await createPostgresHistoryQueries(store,{clock:()=>now}).nightly(key);assert.equal(history.results.applied,2);assert.equal(history.plannedAt,now);assert.equal(history.day,key);
    await worker.runOnce();await applier.runOnce();assert.deepEqual(await store.all("SELECT id,account_id,route_version FROM harbor.sessions WHERE route_version>0 ORDER BY id"),moved);assert.equal(refreshes,1);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.balance_runs')).n,1);
    assert.equal((await store.get("SELECT nightly_expires FROM harbor.sessions WHERE id='manual'")).nightly_expires,pinned);assert.equal(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'")).value).expires,null);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind='routing.expiry'")).n,1);assert.deepEqual(preparation.status(),{readers:0,waiting:0,blockedMembers:0});
  }finally{await applier.close();await worker.close();}
});

test('PostgreSQL transfer schedule saves with version protection, future-only effect and atomic audit',integration,async t=>{
  const {store,at}=await planningFixture(t);let now=at;const router=createPostgresRouter(store,{clock:()=>now}),jobs=createPostgresJobs(store,{clock:()=>now}),planning=createPostgresPlanningJobs(store,{jobs,clock:()=>now});
  const before=(await router.readView()).transfers;assert.deepEqual(before.times,['02:00','10:00','13:00','16:00','20:00']);assert.equal(before.version,0);
  const saved=await router.saveTransferSchedule({times:['16:00','02:00','16:00'],expected_version:0});assert.deepEqual(saved.times,['02:00','16:00']);assert.equal(saved.version,1);assert.equal(saved.effective,at);assert.equal((await router.readView()).transfers.next,Date.parse('2026-11-03T16:00:00+08:00'));
  await assert.rejects(router.saveTransferSchedule({times:['10:00'],expected_version:0}),{code:'transfer_schedule_conflict'});await assert.rejects(router.saveTransferSchedule({times:['24:00'],expected_version:1}),{code:'transfer_schedule_invalid'});
  await store.run("INSERT INTO harbor.gateway_settings VALUES('balancing','{\"nightly\":true}')");assert.equal(await planning.schedule(),null);now=Date.parse('2026-11-03T16:00:00+08:00');assert.equal((await planning.schedule()).id,'routing.nightly:2026-11-03-1600');
  const run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit'))throw Error('synthetic schedule audit failure');return run(sql,...args);};try{await assert.rejects(router.saveTransferSchedule({times:['20:00'],expected_version:1}),/synthetic schedule audit failure/);}finally{store.run=run;}
  assert.deepEqual(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='transfer_schedule'")).value),saved);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='balancing.transfer_schedule'")).n,1);
});

test('PostgreSQL transfer scheduler cancels disabled and superseded automatic jobs without cancelling manual previews',integration,async t=>{
  const {store}=await planningFixture(t);let now=Date.parse('2026-11-03T10:00:00+08:00');const jobs=createPostgresJobs(store,{clock:()=>now}),planning=createPostgresPlanningJobs(store,{jobs,clock:()=>now}),router=createPostgresRouter(store,{clock:()=>now});await router.saveSettings({nightly:true});
  const first=await planning.schedule(),preview=await planning.enqueue(),expiry=await jobs.claim(['routing.expiry']);await jobs.complete(expiry,{sessionsRestored:0});const running=await jobs.claim(['routing.nightly']);assert.equal(running.id,first.id);
  await router.saveSettings({nightly:false});assert.equal(await planning.schedule(),null);assert.equal((await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',first.id)).status,'cancelled');assert.equal(await jobs.heartbeat(running),false);assert.equal((await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',preview.id)).status,'pending');
  await router.saveSettings({nightly:true});now=Date.parse('2026-11-03T13:00:00+08:00');const second=await planning.schedule();assert.equal(second.id,'routing.nightly:2026-11-03-1300');now=Date.parse('2026-11-03T20:00:00+08:00');const latest=await planning.schedule();assert.equal(latest.id,'routing.nightly:2026-11-03-2000');assert.equal((await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',second.id)).status,'cancelled');assert.equal((await store.get('SELECT status FROM harbor.background_jobs WHERE id=$1',preview.id)).status,'pending');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,0);
});

test('PostgreSQL transfer schedule HTTP requires admin and exposes configured future slots without changing the 02:00 reset',integration,async t=>withGateway(t,async({store,password,open})=>{
  const now=Date.parse('2026-11-03T12:00:00+08:00'),app=await open({clock:()=>now,responsesTransport:controlledResponses(async()=>assert.fail('schedule settings must not call a model'))}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),path=origin+'/api/admin/balancing/schedule',data={times:['20:00','13:00','02:00'],expected_version:0};
  assert.equal((await fetch(path,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify(data)})).status,401);assert.equal(await store.get("SELECT value FROM harbor.gateway_settings WHERE name='transfer_schedule'"),undefined);
  const saved=await fetch(path,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(data)});assert.equal(saved.status,200);assert.deepEqual((await saved.json()).times,['02:00','13:00','20:00']);
  const state=await (await fetch(origin+'/api/admin/state',{headers:{cookie}})).json();assert.equal(state.balancing.transfers.next,Date.parse('2026-11-03T13:00:00+08:00'));assert.equal(state.balancing.transfers.timezone,'Asia/Shanghai');assert.equal(state.balancing.settings.nightly,false);
  assert.equal((await fetch(path,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify(data)})).status,409);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='balancing.transfer_schedule'")).n,1);
}));

for(const stop of [false,true])test('PostgreSQL nightly group gate '+(stop?'rolls back on close':'commits atomically')+' and holds all waiters until the real transaction ends',integration,async t=>{
  const {store,at,member,worker,applier:legacy,plan}=await nightlyFixture(t),gate=createSessionRequestHandoff(),old=gate.arrive(member.memberId);await old.bind('heavy');
  const applier=createPostgresNightly(store,{preparation:createRoutingPreparation(),sessionHandoffs:gate,refreshQuotas:async()=>{},clock:()=>at}),run=store.run.bind(store);let releaseAudit,entered=false,pending,closing;
  const held=new Promise(resolve=>{releaseAudit=resolve;}),before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');
  const nextA=gate.arrive(member.memberId),nextB=gate.arrive(member.memberId);let waitingA,waitingB,open=0;
  try{
    const planned=await plan();store.run=async(sql,...args)=>{const result=await run(sql,...args);if(sql.startsWith('INSERT INTO harbor.audit SELECT')){entered=true;await held;}return result;};
    pending=applier.apply(planned);pending.catch(()=>{});await gatewayWait(()=>applier.status().waitingGroups===1);assert.equal(entered,false);assert.equal((await store.get('SELECT 1 n')).n,1);
    // Tickets arriving after the group reservation are not part of its old cohort.
    nextA.close();nextB.close();const a=gate.arrive(member.memberId),b=gate.arrive(member.memberId);waitingA=a.bind('heavy').then(()=>{open++;a.close();});waitingB=b.bind('other').then(()=>{open++;b.close();});await gatewayWait(()=>gate.status().waiting===2);
    old.close();await gatewayWait(()=>entered);assert.equal(open,0);assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);
    if(stop){closing=applier.close();assert.equal(gate.status().pending,2);assert.equal(open,0);}releaseAudit();
    if(stop)await assert.rejects(pending,{code:'service_stopping'});else await pending;await closing;await Promise.all([waitingA,waitingB]);assert.equal(open,2);assert.equal(gate.status().pending,0);assert.equal(applier.status().waitingGroups,0);
    if(stop){assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,0);}else{assert.equal((await store.get('SELECT moved FROM harbor.nightly_group_results')).moved,2);assert.equal((await store.get('SELECT status FROM harbor.balance_runs')).status,'completed');}
  }finally{releaseAudit();old.close();nextA.close();nextB.close();await pending?.catch(()=>{});await Promise.allSettled([waitingA,waitingB]);store.run=run;await applier.close();await legacy.close();await worker.close();}
});

test('PostgreSQL nightly close during plan input reading cannot install a later group behind the shutdown snapshot',integration,async t=>{
  const {store,at,member,worker,applier:legacy,plan}=await nightlyFixture(t),gate=createSessionRequestHandoff(),old=gate.arrive(member.memberId);await old.bind('heavy');const planned=await plan(),applier=createPostgresNightly(store,{preparation:createRoutingPreparation(),sessionHandoffs:gate,refreshQuotas:async()=>{},clock:()=>at}),all=store.all.bind(store);let release,entered=false,pending,closing;const held=new Promise(resolve=>{release=resolve;});
  store.all=async(sql,...args)=>{if(sql.startsWith('SELECT payload FROM harbor.routing_plan_items')&&sql.includes("section='sessions'")){entered=true;await held;}return all(sql,...args);};
  try{pending=applier.apply(planned);pending.catch(()=>{});await gatewayWait(()=>entered);closing=applier.close();release();await assert.rejects(pending,{code:'service_stopping'});await closing;assert.equal(gate.status().pending,0);assert.equal(gate.status().tickets,1);assert.equal(applier.status().waitingGroups,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,0);}finally{release();store.all=all;old.close();await pending?.catch(()=>{});await closing;await applier.close();await legacy.close();await worker.close();}
});

test('PostgreSQL nightly waiting for the gateway refresh barrier can stop and resume the same daily task',integration,async t=>{
  const {store,at,name,worker,applier}=await nightlyFixture(t);let restarted;
  try{
    await worker.runOnce();await gatewayWait(async()=>{await worker.runOnce();return (await store.get("SELECT status FROM harbor.background_jobs WHERE kind='routing.nightly'"))?.status==='running';});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,0);await worker.close();assert.equal((await store.get("SELECT status FROM harbor.background_jobs WHERE kind='routing.nightly'")).status,'pending');
    await store.run('INSERT INTO harbor.nightly_cycle_ready VALUES($1,$2)','2026-11-03',at+1000);restarted=await createPostgresWorker({database:options(name),clock:()=>at+1000});await restarted.runOnce();await gatewayWait(async()=> (await store.get("SELECT status FROM harbor.background_jobs WHERE kind='routing.nightly'")).status==='completed');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind='routing.nightly'")).n,1);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,1);
  }finally{await restarted?.close();await applier.close();await worker.close();}
});

test('PostgreSQL nightly does not reuse an earlier successful snapshot when this cycle refresh fails',integration,async t=>{
  const {store,worker,applier,plan}=await nightlyFixture(t);
  try{await store.run("UPDATE harbor.accounts SET quota_error='合成刷新失败'");const planned=await plan(),header=(await store.get('SELECT header FROM harbor.routing_plans WHERE id=$1',planned)).header;assert.equal(header.counts.groups,0);assert.ok(header.counts.skipped>=2);await applier.apply(planned);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);}finally{await applier.close();await worker.close();}
});

test('PostgreSQL nightly application failures respect the durable task attempt budget',integration,async t=>{
  const {store,worker,applier,plan}=await nightlyFixture(t);
  try{const planned=await plan();await store.run('UPDATE harbor.background_jobs SET max_attempts=2 WHERE id=$1',planned);const run=store.run.bind(store);store.run=async(sql,...args)=>{if(sql.startsWith('INSERT INTO harbor.audit SELECT'))throw Error('synthetic persistent apply failure');return run(sql,...args);};try{for(let i=0;i<2;i++)await assert.rejects(applier.apply(planned),/synthetic persistent apply failure/);await applier.apply(planned);await applier.apply(planned);}finally{store.run=run;}const result=await store.get('SELECT status,attempts,summary FROM harbor.balance_runs');assert.equal(result.status,'failed');assert.equal(result.attempts,2);assert.equal(JSON.parse(result.summary).reason,'application_attempts_exhausted');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);}finally{await applier.close();await worker.close();}
});

for(const terminal of ['failed','cancelled'])test('PostgreSQL nightly backfills a prior-day '+terminal+' task record once without applying bindings',integration,async t=>{
  const {store,at}=await planningFixture(t),jobs=createPostgresJobs(store,{clock:()=>at}),preparation=createRoutingPreparation(),applier=createPostgresNightly(store,{preparation,refreshQuotas:async()=>assert.fail('terminal task refreshed quotas'),clock:()=>at+60000});
  try{const task=await jobs.enqueue('routing.nightly',{id:'routing.nightly:2026-11-02',target:'2026-11-02',payload:{boundary:at-86400000-10000}});if(terminal==='cancelled')await jobs.cancel(task.id);else{const job=await jobs.claim(['routing.nightly']);await jobs.fail(job,{retry:false});}const before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');await applier.runOnce();await applier.runOnce();assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.balance_runs')).n,1);assert.equal((await store.get('SELECT day,status FROM harbor.balance_runs')).day,'2026-11-02');assert.equal((await store.get('SELECT status FROM harbor.balance_runs')).status,terminal);assert.equal((await store.get('SELECT ended FROM harbor.balance_runs')).ended,at);assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);}finally{await applier.close();}
});

test('PostgreSQL nightly restart refreshes a stale cycle barrier before computing and applying the queued plan',integration,async t=>{
  const {store,name,at,worker,preparation,applier}=await nightlyFixture(t);let resumed,lateApplier,refreshes=0;
  try{
    await worker.planning.schedule();await store.run('INSERT INTO harbor.nightly_cycle_ready VALUES($1,$2)','2026-11-03',at);await worker.close();const later=at+600000;
    resumed=await createPostgresWorker({database:options(name),clock:()=>later});await resumed.runOnce();await gatewayWait(async()=>{await resumed.runOnce();return (await store.get("SELECT status FROM harbor.background_jobs WHERE kind='routing.nightly'")).status==='running';});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,0);
    lateApplier=createPostgresNightly(store,{preparation,clock:()=>later,refreshQuotas:async options=>{assert.equal(options.force,true);refreshes++;await store.run("UPDATE harbor.accounts SET quota_json=jsonb_set(quota_json::jsonb,'{checked}',to_jsonb($1::bigint))::text",later);}});
    await lateApplier.runOnce();await gatewayWait(async()=>{await lateApplier.runOnce();return (await store.get('SELECT status FROM harbor.balance_runs'))?.status==='completed';},5000);assert.equal(refreshes,1);assert.equal((await store.get('SELECT at FROM harbor.nightly_cycle_ready')).at,later);assert.equal(JSON.parse((await store.get('SELECT summary FROM harbor.balance_runs')).summary).applied,2);
  }finally{await lateApplier?.close();await resumed?.close();await applier.close();await worker.close();}
});

test('R27 single-slot Worker leaves nightly pending without attempts until the expiry retry succeeds',integration,async t=>{
  const {store,name,at,worker,applier}=await nightlyFixture(t);let single,now=at;await worker.close();
  try{await store.run("INSERT INTO harbor.gateway_settings VALUES('routing_strategy','{\"weighted\":true}')");single=await createPostgresWorker({database:options(name),clock:()=>now,runtime:{concurrency:1,pollMs:100}});const run=single.store.run.bind(single.store);let failed=false;single.store.run=async(sql,...args)=>{if(!failed&&sql.startsWith('INSERT INTO harbor.audit')){failed=true;throw Error('synthetic expiry transient');}return run(sql,...args);};await store.run('INSERT INTO harbor.nightly_cycle_ready VALUES($1,$2)','2026-11-03',at);
    await single.runOnce();await gatewayWait(async()=>{const row=await store.get("SELECT status,attempts FROM harbor.background_jobs WHERE kind='routing.expiry'");return row?.status==='pending'&&row.attempts===1;});await single.runOnce();const pending=await store.get("SELECT status,attempts FROM harbor.background_jobs WHERE kind='routing.nightly'");assert.deepEqual(pending,{status:'pending',attempts:0});
    now+=1000;await gatewayWait(async()=>{await single.runOnce();return (await store.get("SELECT COUNT(*) n FROM harbor.background_jobs WHERE kind IN ('routing.expiry','routing.nightly') AND status='completed'")).n===2;},3000);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,1);
  }finally{await single?.close();await applier.close();await worker.close();}
});

for(const stage of ['before-group','after-audit','after-checkpoint','before-summary','after-summary'])test('R28 execution expiry '+stage+' fails the day while retaining only committed group effects',integration,async t=>{
  const {store,at,worker,preparation,applier,plan}=await nightlyFixture(t);let expiring,now=at;const get=store.get.bind(store),run=store.run.bind(store);
  try{const planned=await plan(),before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');let crossed=false;
    store.get=async(sql,...args)=>{const result=await get(sql,...args);if(!crossed&&(stage==='before-group'&&sql.includes("section='groups' AND ordinal>")||stage==='before-summary'&&sql.startsWith('SELECT COUNT(*) groups'))){crossed=true;now=at+3600000;}return result;};
    store.run=async(sql,...args)=>{const result=await run(sql,...args);if(!crossed&&(stage==='after-audit'&&sql.startsWith('INSERT INTO harbor.audit SELECT')||stage==='after-checkpoint'&&sql.startsWith('INSERT INTO harbor.nightly_group_results')||stage==='after-summary'&&sql.startsWith('UPDATE harbor.balance_runs SET status=')&&sql.includes('summary=$3')&&(sql.includes("status='completed'")||args[Number(/^UPDATE harbor\.balance_runs SET status=\$(\d+)/.exec(sql)?.[1])-1]==='completed'))){crossed=true;now=at+3600000;}return result;};
    expiring=createPostgresNightly(store,{preparation,refreshQuotas:async()=>{},clock:()=>now});await expiring.apply(planned);assert.equal(crossed,true);const result=await get('SELECT status,summary FROM harbor.balance_runs');assert.equal(result.status,'failed');assert.equal(JSON.parse(result.summary).reason,'planning_expired');const audits=(await get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n;assert.equal(audits,['before-summary','after-summary'].includes(stage)?2:0);if(!['before-summary','after-summary'].includes(stage))assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);assert.equal((await get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,['before-summary','after-summary'].includes(stage)?1:0);await expiring.apply(planned);assert.equal((await get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,audits);
  }finally{store.get=get;store.run=run;await expiring?.close();await applier.close();await worker.close();}
});

test('R29 a plan completed while the gateway was absent is recorded interrupted on the next day without migration',integration,async t=>{
  const {store,at,worker,preparation,applier,plan}=await nightlyFixture(t);let restarted;
  try{const planned=await plan(),before=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.balance_runs')).n,0);restarted=createPostgresNightly(store,{preparation,refreshQuotas:async()=>assert.fail('old cycle refreshed'),clock:()=>at+86400000});await restarted.runOnce();await restarted.runOnce();const result=await store.get('SELECT day,status,plan_id FROM harbor.balance_runs');assert.deepEqual(result,{day:'2026-11-03',status:'interrupted',plan_id:planned});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.balance_runs')).n,1);assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),before);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);}finally{await restarted?.close();await applier.close();await worker.close();}
});

test('PostgreSQL job dependencies wait without attempts, preserve identity and propagate terminal failure',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();const jobs=createPostgresJobs(store,{clock:()=>1000});
  await assert.rejects(jobs.enqueue('fixture.child',{id:'self',dependencyId:'self'}),{code:'job_dependency_invalid'});
  await assert.rejects(jobs.enqueue('fixture.child',{id:'missing',dependencyId:'absent'}),{code:'job_dependency_invalid'});
  await jobs.enqueue('fixture.parent',{id:'parent'});await jobs.enqueue('fixture.child',{id:'child',dependencyId:'parent'});
  assert.equal(await jobs.claim(['fixture.child']),null);assert.equal((await store.get("SELECT attempts FROM harbor.background_jobs WHERE id='child'")).attempts,0);
  assert.equal((await jobs.enqueue('fixture.child',{id:'child',dependencyId:'parent'})).created,false);await assert.rejects(jobs.enqueue('fixture.child',{id:'child'}),{code:'job_identity_conflict'});
  const parent=await jobs.claim(['fixture.parent']);await jobs.complete(parent,{done:true});const child=await jobs.claim(['fixture.child']);assert.equal(child.dependency_id,'parent');await jobs.complete(child);
  for(const terminal of ['failed','cancelled']){const id='parent-'+terminal;await jobs.enqueue('fixture.parent',{id});await jobs.enqueue('fixture.child',{id:'child-'+terminal,dependencyId:id});if(terminal==='cancelled')await jobs.cancel(id);else await jobs.fail(await jobs.claim(['fixture.parent']),{retry:false});assert.equal(await jobs.claim(['fixture.child']),null);assert.deepEqual(await store.get('SELECT status,attempts,error_code FROM harbor.background_jobs WHERE id=$1','child-'+terminal),{status:'failed',attempts:0,error_code:'job_dependency_failed'});}
});

test('PostgreSQL 008 upgrade restores missing expiry prerequisite for queued nightly jobs',integration,async t=>{
  const {store}=await fixture(t);await store.query('CREATE SCHEMA harbor');await store.query('CREATE TABLE harbor.schema_migrations(version INTEGER PRIMARY KEY,checksum TEXT NOT NULL,applied BIGINT NOT NULL)');
  const files=['001-postgres.sql','002-request-queries.sql','003-startup-recovery.sql','004-account-bindings.sql','005-quota-refresh.sql','006-background-leases.sql','007-routing-plans.sql','008-nightly-execution.sql'];
  for(const [index,file] of files.entries()){const sql=readFileSync(new URL('../migrations/'+file,import.meta.url),'utf8');await store.query(sql);await store.run('INSERT INTO harbor.schema_migrations VALUES($1,$2,1)',index+1,createHash('sha256').update(sql).digest('hex'));}
  const boundary=1793642400000;await store.run("INSERT INTO harbor.background_jobs(id,kind,target,payload,available_at,created,updated) VALUES('routing.nightly:2026-11-03','routing.nightly','2026-11-03',$1,1,1,1)",JSON.stringify({boundary}));await store.migrate();
  const jobs=createPostgresJobs(store,{clock:()=>1000});assert.equal(await jobs.claim(['routing.nightly']),null);const parent=await jobs.claim(['routing.expiry']);assert.equal(parent.id,'routing.expiry:'+boundary);await jobs.complete(parent,{});const child=await jobs.claim(['routing.nightly']);assert.equal(child.dependency_id,parent.id);assert.equal(child.attempts,1);
});

test('PostgreSQL dependency completion preserves a committed effect when acknowledgement must be retried',integration,async t=>{
  const {store}=await fixture(t);await store.migrate();let at=1000;const jobs=createPostgresJobs(store,{clock:()=>at,leaseMs:1000});await jobs.enqueue('fixture.parent',{id:'parent'});await jobs.enqueue('fixture.child',{id:'child',dependencyId:'parent'});await jobs.complete(await jobs.claim(['fixture.parent']),{});const child=await jobs.claim(['fixture.child']);let effects=0;assert.deepEqual(await jobs.commitEffect(child,async()=>({effects:++effects})),{effects:1});
  // A dependency's later terminal observation cannot discard an already committed effect.
  await store.run("UPDATE harbor.background_jobs SET status='cancelled' WHERE id='parent'");at+=1001;const resumed=await jobs.claim(['fixture.child']);assert.ok(resumed);assert.deepEqual(await jobs.commitEffect(resumed,async()=>assert.fail('effect repeated')),{effects:1});await jobs.complete(resumed,{effects:1});assert.equal(effects,1);assert.equal((await store.get("SELECT status FROM harbor.background_jobs WHERE id='child'")).status,'completed');
});

test('PostgreSQL handoff history exposes known route identities and cancellation reasons without private decision input',integration,async t=>{
 const {store,at}=await planningFixture(t),reader=createPostgresHistoryQueries(store,{clock:()=>at});
 await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)','handoff-visible',at,'session.handoff.cancelled',JSON.stringify({session:'heavy',from:'a',to:'b',reason:'wait_timeout',handoff:'must-never-leave-handoff',payload:'must-never-leave-handoff'}));
 await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)','handoff-private',at-1,'session.handoff.cancelled',JSON.stringify({session:'heavy',from:'must-never-leave-handoff',to:'b',reason:'must-never-leave-handoff',api_key:'must-never-leave-handoff'}));
 const result=await reader.list(historyQuery(new URLSearchParams({kind:'audit',action:'session.handoff.cancelled'})));assert.equal(result.rows.length,2);assert.deepEqual(result.rows.find(r=>r.id==='handoff-visible').detail,{session:'heavy',from:'a',to:'b',reason:'wait_timeout'});assert.deepEqual(result.rows.find(r=>r.id==='handoff-private').detail,{session:'heavy',from:null,to:'b',reason:null});assert.doesNotMatch(JSON.stringify(result),/must-never-leave-handoff/);
});

test('PostgreSQL operation history pages sanitized metadata, combines filters and limits visibility to seven Beijing days',integration,async t=>{
 const {store,at,member}=await planningFixture(t),reader=createPostgresHistoryQueries(store,{clock:()=>at});
 await store.run("UPDATE harbor.sessions SET client_session_id='synthetic-client' WHERE id='heavy'");
 for(let n=0;n<55;n++)await store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)','synthetic-history-'+String(n).padStart(3,'0'),at-n,'session.route',JSON.stringify({session:'heavy',from:'a',to:'b',reason:'nightly',secret:'must-never-leave-history'}));
 await store.run("INSERT INTO harbor.audit VALUES('old',$1,'account.check','must-never-leave-history')",at-8*86400000);
 await store.run("INSERT INTO harbor.audit VALUES('broken',$1,'unrecognized.action','not-json-must-never-leave-history')",at);
 const query=params=>historyQuery(new URLSearchParams(params));const first=await reader.list(query({kind:'audit',action:'session.route',account:'a',session:'synthetic-client'}));assert.equal(first.pagination.total,55);assert.equal(first.rows.length,25);assert.equal(first.rows[0].detail.from,'a');assert.equal(first.rows[0].detail.to,'b');assert.equal(first.rows[0].clientSession,'synthetic-client');assert.doesNotMatch(JSON.stringify(first),/must-never-leave-history/);
 const second=await reader.list(query({kind:'audit',action:'session.route',page:'2'}));assert.equal(second.rows.length,25);assert.ok(!second.rows.some(row=>first.rows.some(previous=>row.id===previous.id)));
 assert.equal((await reader.list(query({account:'absent'}))).pagination.total,0);assert.equal((await reader.list(query({}))).pagination.total,56);assert.equal((await reader.list(query({start:String(at-20*86400000),end:String(at+1)}))).pagination.total,56);
 assert.deepEqual((await reader.list(query({action:'unrecognized.action'}))).rows[0].detail,{});assert.equal((await reader.list(query({action:'session.route',session:'other'}))).rows.length,0);
});

test('PostgreSQL nightly history reads persisted actual group outcomes after plan expiry with bounded public pages',integration,async t=>{
 const {store,at,worker,applier,plan}=await nightlyFixture(t);
 try{const planned=await plan();await applier.apply(planned);const reader=createPostgresHistoryQueries(store,{clock:()=>at+7200000,pageBytes:256});const header=await reader.nightly('2026-11-03');assert.equal(header.hasPlan,true);assert.equal(header.results.applied,2);assert.equal(header.counts.moves,2);assert.equal(header.paginated,true);
 const groups=await reader.page('2026-11-03',{section:'groups'});assert.equal(groups.items[0].result.status,'applied');const moves=await reader.page('2026-11-03',{section:'moves',limit:1});assert.equal(moves.items.length,1);assert.equal(moves.items[0].result.status,'applied');assert.equal(moves.has_more,true);const next=await reader.page('2026-11-03',{section:'moves',after:moves.after});assert.equal(next.items.length,1);assert.notEqual(next.items[0].session_id,moves.items[0].session_id);assert.equal(next.has_more,false);
 await assert.rejects(reader.page('2026-11-03',{section:'accounts'}),{status:400});assert.doesNotMatch(JSON.stringify(header),/quota_json|configuration|route_epoch/);
 const filtered=await reader.list(historyQuery(new URLSearchParams({kind:'nightly',account:'a',session:moves.items[0].session_id})));assert.equal(filtered.rows.length,1);
 const expired=createPostgresHistoryQueries(store,{clock:()=>at+7*86400000});await assert.rejects(expired.nightly('2026-11-03'),{status:404});await assert.rejects(expired.page('2026-11-03'),{status:404});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans WHERE id=$1',planned)).n,1);
 }finally{await applier.close();await worker.close();}
});

test('PostgreSQL history HTTP requires administrator authentication and validates pagination before reading private sections',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
 for(const path of ['/api/admin/history','/api/admin/history/nightly/2026-11-03','/api/admin/history/nightly/2026-11-03/items']){assert.equal((await fetch(origin+path)).status,401);assert.equal((await fetch(origin+path,{headers:{authorization:'Bearer '+member.secret}})).status,401);}
 const response=await fetch(origin+'/api/admin/history',{headers:{cookie}});assert.equal(response.status,200);assert.ok(Array.isArray((await response.json()).rows));
 for(const suffix of ['?kind=invalid','?page=0','?kind=audit&kind=nightly'])assert.equal((await fetch(origin+'/api/admin/history'+suffix,{headers:{cookie}})).status,400);
 assert.equal((await fetch(origin+'/api/admin/history/nightly/2026-11-03/items?section=accounts',{headers:{cookie}})).status,400);assert.equal((await fetch(origin+'/api/admin/history/nightly/2026-11-03?limit=2',{headers:{cookie}})).status,400);
}));

test('PostgreSQL historical nightly summaries remain filterable and paged without inventing execution results or old prices',integration,async t=>{
 const {store,at}=await planningFixture(t),reader=createPostgresHistoryQueries(store,{clock:()=>at});await store.run("UPDATE harbor.sessions SET client_session_id='legacy-client' WHERE id='heavy'");
 const summary={moves:[{session_id:'heavy',from:'a',to:'b',weight:999},{session_id:'other',from:'b',to:'a'}],applied:['heavy'],summaries:[{accounts:['a','b'],before:{a:999},planning:{reason:'balanced',secret:'must-never-leave-history'}}],skipped:[]};await store.run("INSERT INTO harbor.balance_runs VALUES('2026-11-03',$1,$1,'completed',$2,NULL,0)",at,JSON.stringify(summary));
 const header=await reader.nightly('2026-11-03');assert.equal(header.legacy,true);assert.equal(header.hasPlan,true);assert.equal(header.results.applied,1);assert.equal(header.counts.moves,2);assert.equal(header.metric,null);
 const moves=await reader.page('2026-11-03',{section:'moves',limit:1});assert.equal(moves.items[0].result.status,'applied');assert.equal(moves.items[0].weight,null);assert.equal(moves.items[0].client_session_id,'legacy-client');assert.equal(moves.has_more,true);const next=await reader.page('2026-11-03',{section:'moves',after:moves.after});assert.equal(next.items[0].result.status,'not_applied');
 const groups=await reader.page('2026-11-03',{section:'groups'});assert.deepEqual(groups.items[0].before,{});assert.equal(groups.items[0].result.status,'historical');assert.doesNotMatch(JSON.stringify(groups),/must-never-leave-history/);
 const result=await reader.list(historyQuery(new URLSearchParams({kind:'nightly',account:'a',session:'legacy-client'})));assert.equal(result.pagination.total,1);
});

test('PostgreSQL history marks skipped groups without claiming their planned moves were executed',integration,async t=>{
 const {store,at,member,worker,applier,plan}=await nightlyFixture(t);
 try{await plan();await store.run('UPDATE harbor.members SET enabled=0 WHERE id=$1',member.memberId);await applier.apply('routing.nightly:2026-11-03');const history=createPostgresHistoryQueries(store,{clock:()=>at});const header=await history.nightly('2026-11-03');assert.equal(header.results.applied,0);assert.equal(header.results.skipped,1);const moves=await history.page('2026-11-03',{section:'moves'});assert.equal(moves.items.length,2);assert.ok(moves.items.every(row=>row.result.status==='skipped'&&row.result.reason==='nightly_plan_changed'));}finally{await applier.close();await worker.close();}
});

test('PostgreSQL history refuses oversized audit details explicitly without transferring raw secrets',integration,async t=>{
 const {store,at}=await planningFixture(t),history=createPostgresHistoryQueries(store,{clock:()=>at});await store.run("INSERT INTO harbor.audit VALUES('oversized',$1,'unknown.action',$2)",at,'x'.repeat(2097153));await assert.rejects(history.list(historyQuery(new URLSearchParams())),{code:'history_capacity'});assert.equal((await store.get('SELECT octet_length(detail) n FROM harbor.audit')).n,2097153);
});

test('R30 nightly account and protected-session filters match independently within the same batch for new and old plans',integration,async t=>{
 const {store,at,worker,applier,plan}=await nightlyFixture(t);
 try{await store.run("UPDATE harbor.sessions SET nightly_enabled=0,nightly_expires=$1,client_session_id='synthetic-protected-client' WHERE id='heavy'",at+3600000);const planned=await plan();
 assert.ok(await store.get("SELECT ordinal FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='fixed' AND payload->>'session_id'='heavy'",planned));assert.ok(await store.get("SELECT ordinal FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='skipped' AND payload->>'session_id'='heavy'",planned));await applier.apply(planned);
 await store.run("INSERT INTO harbor.balance_runs(day,started,ended,status,summary) VALUES('2026-11-02',$1,$1,'completed',$2)",at-86400000,JSON.stringify({moves:[],skipped:[{session_id:'heavy',reason:'protected'}],summaries:[{accounts:['a','b']}]}));const history=createPostgresHistoryQueries(store,{clock:()=>at});
 for(const session of ['heavy','synthetic-protected-client']){const result=await history.list(historyQuery(new URLSearchParams({kind:'nightly',account:'a',session})));assert.equal(result.pagination.total,2);assert.deepEqual(result.rows.map(row=>row.day),['2026-11-03','2026-11-02']);}
 for(const [account,session] of [['absent','heavy'],['a','absent']])assert.equal((await history.list(historyQuery(new URLSearchParams({kind:'nightly',account,session})))).pagination.total,0);
 }finally{await applier.close();await worker.close();}
});

for(const path of ['/images/generations','/images/edits'])test('PostgreSQL JSON '+path+' preserves payload, ownership, exact metering and records only sanitized structural facts',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const calls=[],image='c3ludGhldGljLWltYWdlLWJpbmFyeQ==',transport=createResponsesTransport({credentials:async()=>({token:'synthetic-subscription-only',accountId:'synthetic-subscription'}),version:async()=>'test',fetchImpl:async(url,options)=>{calls.push({url,payload:JSON.parse(options.body)});return Response.json({model:'gpt-6.1-sol',data:[{b64_json:image,revised_prompt:'合成结果说明'}],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:10}}});}});
 const app=await open({responsesTransport:transport}),origin=await listenGateway(app),payload={model:'gpt-6.1-sol',prompt:'合成图片提示 Authorization: Bearer sk-synthetic-should-not-persist-1234567890',...(path.endsWith('edits')?{images:[{image_url:'data:image/png;base64,'+image}]}:{})};
 const response=await fetch(origin+'/v1'+path,{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'synthetic-json-client'},body:JSON.stringify(payload)});assert.equal(response.status,200);assert.equal((await response.json()).data[0].b64_json,image);assert.equal(calls.length,1);assert.deepEqual(calls[0].payload,payload);assert.ok(calls[0].url.endsWith(path));
 const request=await store.get('SELECT * FROM harbor.requests');assert.equal(request.source_channel,path.slice(1));assert.equal(request.status,'ok');assert.equal(request.input,100);assert.equal(request.output,20);assert.equal(request.cached,10);assert.equal(request.account_id,'synthetic-runtime');assert.ok(response.headers.get('x-harbor-session'));assert.equal((await store.get('SELECT account_id FROM harbor.sessions')).account_id,'synthetic-runtime');
 await app.buffer.flush();const cookie=await gatewayLogin(origin,password),recording=await fetch(origin+'/api/admin/requests/'+request.id+'/recording?limit=100',{headers:{cookie}});assert.equal(recording.status,200);const body=await recording.json();assert.equal(body.state,'complete');assert.ok(body.events.some(event=>event.kind==='boundary'&&event.provenance==='client'));assert.ok(body.events.some(event=>event.kind==='attachment'&&event.content.type==='generated_image'));assert.ok(!body.events.some(event=>['user','assistant'].includes(event.kind)));const saved=JSON.stringify(body);assert.doesNotMatch(saved,/sk-synthetic-should-not-persist|data:image|c3ludGhldGljLWltYWdlLWJpbmFyeQ/);assert.match(saved,/合成图片提示|合成结果说明/);assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});
}));

test('PostgreSQL JSON rejects invalid protocol, model denial and API-only accounts before dispatch or binding',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0;const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'test',fetchImpl:async()=>{calls++;return Response.json({data:[{b64_json:'eA=='}]});}}),app=await open({responsesTransport:transport}),origin=await listenGateway(app);
 const send=payload=>fetch(origin+'/v1/images/generations',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:JSON.stringify(payload)});
 assert.equal((await send({model:'gpt-6.1-sol',prompt:'synthetic',stream:true})).status,400);assert.equal((await send({model:'forbidden-model',prompt:'synthetic'})).status,403);assert.equal(calls,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions')).n,0);
 await store.run("UPDATE harbor.accounts SET auth_mode='openai_api',api_weight=100");assert.equal((await send({model:'gpt-6.1-sol',prompt:'synthetic'})).status,503);assert.equal(calls,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions')).n,0);
}));

test('PostgreSQL JSON refuses a fixed API conversation without silently rebinding it to a subscription',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0;const app=await open({responsesTransport:{runJson:async()=>{calls++;}}}),origin=await listenGateway(app);await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode) VALUES('synthetic-api','synthetic','synthetic','ready','openai_api')");await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,client_key,created,updated) VALUES('synthetic-fixed-api',$1,'synthetic-api','responses',$2,1,1)",member.memberId,hash('synthetic-fixed-client'));
 const response=await fetch(origin+'/v1/images/generations',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'synthetic-fixed-client','x-harbor-session':'synthetic-fixed-api'},body:JSON.stringify({model:'gpt-6.1-sol',prompt:'synthetic'})});assert.equal(response.status,400);assert.equal(calls,0);assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='synthetic-fixed-api'")).account_id,'synthetic-api');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
}));

test('PostgreSQL JSON shares employee model limits while recording can be disabled independently of accounting',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0;const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'test',fetchImpl:async()=>{calls++;return Response.json({data:[{b64_json:'eA=='}],usage:{input_tokens:1000000,output_tokens:0}});}});await store.run('UPDATE harbor.members SET usage_limits=$1 WHERE id=$2',JSON.stringify({enabled:true,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'0.01'}]}),member.memberId);
 const app=await open({responsesTransport:transport,recording:{enabled:false}}),origin=await listenGateway(app),send=()=>fetch(origin+'/v1/images/generations',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'synthetic-limit-client'},body:JSON.stringify({model:'gpt-6.1-sol',prompt:'synthetic'})});assert.equal((await send()).status,200);assert.equal((await send()).status,429);assert.equal(calls,1);const request=await store.get('SELECT * FROM harbor.requests');assert.equal(request.input,1000000);assert.equal(request.recording_state,'disabled');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.conversation_events')).n,0);
}));

for(const kind of ['missing-usage','invalid-output','oversized'])test('PostgreSQL JSON '+kind+' preserves known or unknown metering without retry',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0;const transport=createResponsesTransport({maxJsonResponseBytes:256,credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'test',fetchImpl:async()=>{calls++;return Response.json(kind==='oversized'?{data:[{b64_json:'x'.repeat(300)}]}:kind==='invalid-output'?{data:[],usage:{input_tokens:123,output_tokens:4}}:{data:[{b64_json:'eA=='}]});}}),app=await open({responsesTransport:transport}),origin=await listenGateway(app);
 const response=await fetch(origin+'/v1/images/generations',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',prompt:'synthetic'})});assert.equal(response.status,kind==='missing-usage'?200:502);assert.equal(calls,1);const request=await store.get('SELECT * FROM harbor.requests');assert.equal(request.usage_unknown,kind==='invalid-output'?0:1);assert.equal(request.input,kind==='invalid-output'?123:0);assert.equal(request.output,kind==='invalid-output'?4:0);await app.buffer.flush();assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
}));

test('PostgreSQL JSON in-flight work blocks manual rebinding and cancellation settles once without losing the saved input prefix',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 let calls=0;const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'test',fetchImpl:async(_url,{signal})=>{calls++;await new Promise((resolve,reject)=>{signal.addEventListener('abort',()=>reject(Error('synthetic image aborted')),{once:true});if(signal.aborted)reject(Error('synthetic image already aborted'));});}}),app=await open({responsesTransport:transport}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),controller=new AbortController();
 const pending=fetch(origin+'/v1/images/generations',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',prompt:'合成取消前的输入'}),signal:controller.signal});pending.catch(()=>{});
 try{await gatewayWait(()=>calls===1);const request=await store.get('SELECT * FROM harbor.requests');assert.equal(request.status,'running');await store.run("INSERT INTO harbor.accounts(id,name,home,state) VALUES('synthetic-json-target','synthetic','synthetic','ready')");const response=await fetch(origin+'/api/admin/sessions/'+request.session_id,{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({account_id:'synthetic-json-target',expected_route_version:0})});assert.equal(response.status,409);assert.equal((await response.json()).error.code,'session_busy');controller.abort();await assert.rejects(pending,{name:'AbortError'});await gatewayWait(()=>app.status().active===0);const ended=await store.get('SELECT * FROM harbor.requests');assert.equal(ended.status,'cancelled');assert.equal(ended.usage_unknown,1);assert.equal(ended.account_id,'synthetic-runtime');assert.equal(calls,1);await app.buffer.flush();const saved=await (await fetch(origin+'/api/admin/requests/'+ended.id+'/recording?limit=100',{headers:{cookie}})).json();assert.equal(saved.state,'partial');assert.ok(saved.events.some(event=>event.content.text?.includes('合成取消前的输入')));assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});}
 finally{controller.abort();await pending.catch(()=>{});}
}));

test('PostgreSQL JSON and Responses preserve the receiver of injected transport methods',integration,async t=>withGateway(t,async({member,open})=>{
 let calls=0;const transport={marker:'synthetic-transport-context',async run(){assert.equal(this.marker,'synthetic-transport-context');calls++;return {usageKnown:true,input:1,output:0,cached:0,response:{output:[]}};},async runJson(){assert.equal(this.marker,'synthetic-transport-context');calls++;return {usageKnown:true,input:1,output:0,cached:0,response:{data:[{b64_json:'eA=='}]}};}};const app=await open({responsesTransport:transport}),origin=await listenGateway(app);assert.equal((await gatewaySend(origin,member.secret)).status,200);const image=await fetch(origin+'/v1/images/generations',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',prompt:'synthetic'})});assert.equal(image.status,200);assert.equal(calls,2);
}));

async function legacyChatFixture(t){
 const {store,name}=await fixture(t);await store.migrate();await store.initializePricing();const {member}=await legacyMember(t,store),at=Date.parse('2026-11-03T10:00:00+08:00');await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);await store.run("INSERT INTO harbor.accounts(id,name,home,state,token_budget) VALUES('chat-account','synthetic','synthetic','ready',1000000)");const limits=createPostgresMemberLimits(store,{clock:()=>at}),chat=createPostgresChatState(store,{clock:()=>at,reserveTokens:100,checkLimits:(person,payload,options)=>limits.check(person.id,payload.model,options)}),message={role:'user',content:'合成首轮文本'};return {store,name,member,chat,message,at};
}
const chatResult={text:'合成首轮回复',threadId:'synthetic-cli-thread',input:10,output:5,cached:1,usageKnown:true,actualModel:'gpt-6.1-sol'};

test('PostgreSQL legacy Chat admission reserves atomically and preserves full-history deltas with cumulative cursor settlement',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t);const first=await chat.admit(member.secret,{model:'codex',messages:[message]});let request=await store.get('SELECT * FROM harbor.requests WHERE id=$1',first.requestId);assert.equal(request.source_channel,'chat');assert.equal(request.tracking_only,0);assert.equal(request.reserved,100);assert.equal(request.predicted,10000);await chat.dispatch(member.secret,first.requestId);await store.finish(first.requestId,{...chatResult,chat:chatSettlementFacts(first,chatResult)},at);
 let session=await store.get('SELECT * FROM harbor.sessions');assert.equal(session.protocol,'chat');assert.equal(session.upstream_id,chatResult.threadId);assert.equal(session.message_count,2);assert.equal(session.cum_input,10);assert.equal(session.history_digest,digestChatMessages('',[message,{role:'assistant',content:chatResult.text}]));assert.equal((await store.get('SELECT used FROM harbor.members')).used,15);assert.equal((await store.get('SELECT reserved FROM harbor.requests')).reserved,0);
 const next={role:'user',content:'合成第二轮文本'},second=await chat.admit(member.secret,{model:'codex',session_id:session.id,messages:[message,{role:'assistant',content:chatResult.text},next]});assert.deepEqual(second.delta,[next]);await chat.dispatch(member.secret,second.requestId);const result={...chatResult,text:'合成第二轮回复',input:7,output:3,cached:1,cumulativeUsage:{input:17,output:8,cached:2}};await store.finish(second.requestId,{...result,chat:chatSettlementFacts(second,result)},at+1);assert.equal((await store.get('SELECT message_count FROM harbor.sessions')).message_count,4);assert.equal((await store.get('SELECT cum_input FROM harbor.sessions')).cum_input,17);assert.equal((await store.get('SELECT used FROM harbor.members')).used,25);
 await assert.rejects(chat.admit(member.secret,{session_id:session.id,messages:[{role:'user',content:'不匹配的历史'},next]}),{code:'session_history_mismatch'});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,2);const repeat=await chat.admit(member.secret,{session_id:session.id,messages:[next]});assert.deepEqual(repeat.delta,[next]);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,3);
});

test('PostgreSQL legacy Chat serializes employee admission and retains unknown reservations across Keys',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t);await store.run('UPDATE harbor.members SET quota=100');const other='synthetic-second-chat-key';await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('chat-second-key',$1,$2,'synthetic',0)",member.memberId,hash(other));const results=await Promise.allSettled([chat.admit(member.secret,{messages:[message]}),chat.admit(other,{messages:[message]})]);assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(results.find(r=>r.status==='rejected').reason.code,'member_busy');const first=results.find(r=>r.status==='fulfilled').value;const admittedSecret=first.person.key_id===member.keyId?member.secret:other;await chat.dispatch(admittedSecret,first.requestId);const result={status:'cancelled',usageKnown:false};await store.finish(first.requestId,{...result,chat:chatSettlementFacts(first,result)},at);assert.equal((await store.get('SELECT reserved FROM harbor.requests')).reserved,100);assert.equal((await store.get('SELECT status FROM harbor.requests')).status,'unknown');assert.equal((await store.get('SELECT state FROM harbor.sessions')).state,'uncertain');await assert.rejects(chat.admit(other,{messages:[message]}),{code:'quota_exceeded'});assert.equal((await store.get('SELECT used FROM harbor.members')).used,0);
});

test('PostgreSQL legacy Chat rechecks queued permissions and cannot cross employee, protocol or provider ownership',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t),first=await chat.admit(member.secret,{messages:[message]});await store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',member.keyId);await assert.rejects(chat.dispatch(member.secret,first.requestId),{code:'key_disabled'});await store.run('UPDATE harbor.keys SET enabled=1 WHERE id=$1',member.keyId);await store.finish(first.requestId,{status:'rejected',usageKnown:true,chat:chatSettlementFacts(first,{status:'rejected',usageKnown:true},{dispatched:false})},at);assert.equal((await store.get('SELECT reserved FROM harbor.requests')).reserved,0);assert.equal((await store.get('SELECT state FROM harbor.sessions')).state,'new');
 await assert.rejects(chat.admit(member.secret,{model:'claude',session_id:first.session.id,messages:[message]}),{code:'session_provider_mismatch'});await store.run("UPDATE harbor.sessions SET protocol='responses' WHERE id=$1",first.session.id);await assert.rejects(chat.admit(member.secret,{session_id:first.session.id,messages:[message]}),{code:'session_protocol_mismatch'});await store.run("UPDATE harbor.sessions SET protocol='chat',enabled=0 WHERE id=$1",first.session.id);await assert.rejects(chat.admit(member.secret,{session_id:first.session.id,messages:[message]}),{code:'session_disabled'});
});

test('PostgreSQL legacy Chat cursor, billing and price rollback together and recovery never applies an old cursor to a moved binding',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t),first=await chat.admit(member.secret,{messages:[message]});await chat.dispatch(member.secret,first.requestId);const result={...chatResult,chat:chatSettlementFacts(first,chatResult)};await store.query(`CREATE FUNCTION harbor.reject_chat_projection_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic chat projection failure'; END $$; CREATE TRIGGER reject_chat_projection_fixture BEFORE UPDATE ON harbor.request_prices FOR EACH ROW WHEN (NEW.state='priced') EXECUTE FUNCTION harbor.reject_chat_projection_fixture()`);try{await assert.rejects(store.finish(first.requestId,result,at),/synthetic chat projection failure/);}finally{await store.query('DROP TRIGGER reject_chat_projection_fixture ON harbor.request_prices');}
 assert.equal((await store.get('SELECT used FROM harbor.members')).used,0);assert.equal((await store.get('SELECT status FROM harbor.requests')).status,'running');assert.equal((await store.get('SELECT upstream_id FROM harbor.sessions')).upstream_id,null);
 await store.run("INSERT INTO harbor.accounts(id,name,home,state,token_budget) VALUES('chat-peer','synthetic','synthetic','ready',1000000)");await createPostgresRouter(store,{clock:()=>at}).disableAccount('chat-account');await store.finish(first.requestId,result,at);await store.finish(first.requestId,result,at);const session=await store.get('SELECT * FROM harbor.sessions');assert.equal(session.account_id,'chat-peer');assert.equal(session.upstream_id,null);assert.equal(session.message_count,0);assert.equal(session.state,'new');assert.equal((await store.get('SELECT used FROM harbor.members')).used,15);assert.equal((await store.get("SELECT budget_used FROM harbor.accounts WHERE id='chat-account'")).budget_used,15);
});

test('PostgreSQL legacy Chat critical recovery replays only a typed cursor and billing facts without conversation text',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t),first=await chat.admit(member.secret,{messages:[message]});await chat.dispatch(member.secret,first.requestId);const root=mkdtempSync(join(tmpdir(),'harbor-chat-recovery-')),key=randomBytes(32);let recovery,restored;
 try{recovery=await MeteringRecovery.create({finish:async()=>{throw Error('synthetic chat DB outage');}},{encryptionKey:key,directory:join(root,'spool'),flushMs:10000});const result={...chatResult,text:'synthetic-must-not-be-in-critical-spool',messages:[message],secret:'synthetic-secret-field'};result.chat=chatSettlementFacts(first,result);assert.equal(recovery.settle(first.requestId,result,at),true);const persisted=await recovery.flush();assert.equal(persisted.queued,0);assert.equal(persisted.files,1);await recovery.close();for(const file of readdirSync(join(root,'spool')))assert.ok(!readFileSync(join(root,'spool',file)).includes(Buffer.from(result.text)));
 const recovered=[];restored=await MeteringRecovery.create({finish:async(id,value,time)=>{recovered.push(value);return store.finish(id,value,time);}},{encryptionKey:key,directory:join(root,'spool'),flushMs:10000});const replayed=await restored.flush();assert.equal(replayed.queued,0);assert.equal(replayed.files,0);assert.equal(recovered.length,1);assert.equal(recovered[0].text,undefined);assert.equal(recovered[0].messages,undefined);assert.equal(recovered[0].secret,undefined);assert.equal(recovered[0].chat.cursor.messageCount,2);assert.equal((await store.get('SELECT upstream_id FROM harbor.sessions')).upstream_id,chatResult.threadId);assert.equal((await store.get('SELECT used FROM harbor.members')).used,15);
 }finally{await restored?.close();await recovery?.close();rmSync(root,{recursive:true,force:true});}
});

test('PostgreSQL legacy Chat honors independent Key reservations and exact-model Keys cannot use CLI aliases',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t);await store.run('UPDATE harbor.keys SET quota=50 WHERE id=$1',member.keyId);const first=await chat.admit(member.secret,{messages:[message]});assert.equal(first.reserved,50);await chat.dispatch(member.secret,first.requestId);const failed={status:'failed',usageKnown:true,input:1,output:0,cached:0};await store.finish(first.requestId,{...failed,chat:chatSettlementFacts(first,failed)},at);assert.equal((await store.get('SELECT reserved FROM harbor.requests')).reserved,0);assert.equal((await store.get('SELECT used FROM harbor.keys')).used,1);assert.equal((await store.get('SELECT state FROM harbor.sessions')).state,'uncertain');
 await store.run('UPDATE harbor.keys SET quota=1 WHERE id=$1',member.keyId);await assert.rejects(chat.admit(member.secret,{messages:[message]}),{code:'key_quota_exceeded'});await store.run('UPDATE harbor.keys SET quota=NULL,allowed_models=$1 WHERE id=$2',JSON.stringify(['gpt-6.1-sol']),member.keyId);await assert.rejects(chat.admit(member.secret,{messages:[message]}),{status:403});assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);
});

test('PostgreSQL legacy Chat capacity includes queued predictions and never resolves another employee session',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t),other='synthetic-other-chat-key';await store.run("INSERT INTO harbor.members(id,name,created) VALUES('chat-other','synthetic',0)");await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('chat-other-key','chat-other',$1,'synthetic',0)",hash(other));await store.run('UPDATE harbor.accounts SET token_budget=12000');const first=await chat.admit(member.secret,{messages:[message]});await assert.rejects(chat.admit(other,{messages:[message]}),{code:'no_account_capacity'});await assert.rejects(chat.admit(other,{messages:[message],session_id:first.session.id}),{code:'session_not_found'});await chat.dispatch(member.secret,first.requestId);await store.finish(first.requestId,{...chatResult,chat:chatSettlementFacts(first,chatResult)},at);const second=await chat.admit(other,{messages:[message]});assert.equal(second.person.id,'chat-other');assert.notEqual(second.session.id,first.session.id);
});

test('PostgreSQL legacy Chat settlement rejects contradictory typed cursors while retaining measured billing',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t),first=await chat.admit(member.secret,{messages:[message]});await chat.dispatch(member.secret,first.requestId);const facts=chatSettlementFacts(first,chatResult);facts.cursor.cumulative.input=999;await store.finish(first.requestId,{...chatResult,chat:facts},at);const session=await store.get('SELECT * FROM harbor.sessions');assert.equal(session.state,'uncertain');assert.equal(session.message_count,0);assert.equal(session.cum_input,0);assert.equal((await store.get('SELECT input FROM harbor.requests')).input,10);assert.equal((await store.get('SELECT used FROM harbor.members')).used,15);
});

test('PostgreSQL legacy Chat keeps Claude subscription capacity and cumulative cursor semantics separate from Codex',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t);await store.run("INSERT INTO harbor.accounts(id,name,home,kind,state,token_budget) VALUES('chat-claude','synthetic','synthetic','claude','ready',1000000)");const first=await chat.admit(member.secret,{model:'claude',messages:[message]});assert.equal(first.account.id,'chat-claude');await chat.dispatch(member.secret,first.requestId);const result={...chatResult,actualModel:undefined,cumulativeUsage:{input:10,output:5,cached:1}};await store.finish(first.requestId,{...result,chat:chatSettlementFacts(first,result)},at);const session=await store.get('SELECT * FROM harbor.sessions');assert.equal(session.cum_input,10);assert.equal(session.cum_cached,1);assert.equal(session.upstream_id,result.threadId);assert.equal((await store.get('SELECT account_id FROM harbor.requests')).account_id,'chat-claude');
});

test('R31 already-admitted legacy Chat can dispatch while a later Responses call runs on another account',integration,async t=>{
 const {store,member,chat,message,at}=await legacyChatFixture(t),first=await chat.admit(member.secret,{messages:[message]});await store.run("INSERT INTO harbor.accounts(id,name,home,state,token_budget) VALUES('responses-peer','synthetic','synthetic','ready',1000000)");const person=await store.authenticate(member.secret),response=await store.observe(person,'synthetic','synthetic','gpt-6.1-sol',{at,channel:'responses'});await store.run("UPDATE harbor.requests SET account_id='responses-peer',status='running',business_status='running' WHERE id=$1",response);
 await chat.dispatch(member.secret,first.requestId);assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',first.requestId)).status,'running');assert.equal((await store.get('SELECT status FROM harbor.requests WHERE id=$1',response)).status,'running');assert.equal((await store.get('SELECT account_id FROM harbor.requests WHERE id=$1',first.requestId)).account_id,'chat-account');await store.finish(first.requestId,{...chatResult,chat:chatSettlementFacts(first,chatResult)},at);assert.equal((await store.get('SELECT used FROM harbor.members')).used,15);
});

const prepareHttpChat=async(store,member)=>{await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);await store.run("UPDATE harbor.accounts SET token_budget=1000000 WHERE id='synthetic-runtime'");};
const sendHttpChat=(origin,secret,body,options={})=>fetch(origin+'/v1/chat/completions',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json'},body:JSON.stringify(body),...options});

test('PostgreSQL Chat HTTP preserves deltas, SSE and cumulative cursor while storing only the current sanitized turn',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 await prepareHttpChat(store,member);const calls=[];const executor=async args=>{calls.push(args);const text='合成文本回复 '+calls.length;args.onText(text.slice(0,4));args.onText(text.slice(4));return {text,threadId:args.sessionId||'synthetic-http-chat-thread',input:10,output:5,cached:1,usageKnown:true,actualModel:'gpt-6.1-sol',cumulativeUsage:{input:args.previousUsage.input+10,output:args.previousUsage.output+5,cached:args.previousUsage.cached+1}};};const app=await open({executor}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),firstMessage={role:'user',content:'合成新问题 Authorization: Bearer sk-synthetic-do-not-save-1234567890'};
 const first=await sendHttpChat(origin,member.secret,{messages:[firstMessage]});assert.equal(first.status,200);const answer=await first.json();assert.equal(answer.choices[0].message.content,'合成文本回复 1');assert.equal(answer.usage.prompt_tokens,10);assert.equal(answer.harbor_standard_usage.currency,'USD');assert.ok(answer.session_id);assert.equal(calls[0].messages[0].content,firstMessage.content);
 const next={role:'user',content:'合成后续问题'},second=await sendHttpChat(origin,member.secret,{session_id:answer.session_id,stream:true,messages:[firstMessage,{role:'assistant',content:answer.choices[0].message.content},next]});assert.equal(second.status,200);assert.match(second.headers.get('content-type'),/event-stream/);assert.match(await second.text(),/\[DONE\]/);assert.deepEqual(calls[1].messages,[next]);assert.equal(calls[1].sessionId,'synthetic-http-chat-thread');assert.equal(calls[1].previousUsage.input,10);assert.equal((await store.get('SELECT message_count FROM harbor.sessions')).message_count,4);assert.equal((await store.get('SELECT used FROM harbor.members')).used,30);
 await app.buffer.flush();const requests=await store.all('SELECT id FROM harbor.requests ORDER BY query_sequence');const record=await (await fetch(origin+'/api/admin/requests/'+requests[1].id+'/recording?limit=100',{headers:{cookie}})).json();assert.equal(record.state,'complete');const saved=JSON.stringify(record);assert.match(saved,/合成后续问题/);assert.doesNotMatch(saved,/合成新问题/);assert.equal(record.events.filter(e=>e.kind==='assistant').map(e=>e.content.text||'').join(''),'合成文本回复 2');assert.deepEqual(app.status().chat,{requests:0,queued:0});
}));

for(const provider of ['codex','claude'])test('PostgreSQL Chat HTTP reaches the real '+provider+' parser through a controlled subprocess',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);if(provider==='claude')await store.run("INSERT INTO harbor.accounts(id,name,kind,home,state,token_budget) VALUES('synthetic-claude','synthetic','claude','synthetic','ready',1000000)");const {runCodex}=await import('../codex.mjs'),{runClaude}=await import('../claude.mjs');let children=0;
 const executor=args=>(provider==='codex'?runCodex:runClaude)({...args,spawnProcess:(_binary,_args,options)=>{children++;return spawn(process.execPath,[new URL('./provider-child.mjs',import.meta.url).pathname,provider,'ok'],options);}}),app=await open(provider==='codex'?{executor}:{claudeExecutor:executor}),origin=await listenGateway(app),response=await sendHttpChat(origin,member.secret,{model:provider,messages:[{role:'user',content:'synthetic'}]});assert.equal(response.status,200);const value=await response.json();assert.equal(value.choices[0].message.content,'中文测试 🍋 Unicode');assert.equal(value.usage.prompt_tokens,120);assert.equal(children,1);const session=await store.get('SELECT * FROM harbor.sessions');assert.equal(session.upstream_id,'fixed-thread');assert.equal(session.cum_input,120);assert.equal(session.cum_cached,50);await app.buffer.flush();
}));

async function otherHttpChatKey(store,label){const key='synthetic-chat-key-'+label;await store.run('INSERT INTO harbor.members(id,name,created) VALUES($1,$1,0)',label);await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,0)',label+'-key',label,hash(key),'synthetic');return key;}
function heldHttpChat(){const gates=[],calls=[];return {calls,gates,executor:async args=>{const index=calls.push(args);await new Promise((resolve,reject)=>{gates.push(resolve);args.signal.addEventListener('abort',()=>reject(Object.assign(Error('synthetic CLI cancelled'),{usageKnown:false})),{once:true});if(args.signal.aborted)reject(Object.assign(Error('synthetic CLI cancelled'),{usageKnown:false}));});return {text:'synthetic queued reply',threadId:args.sessionId||'synthetic-thread-'+index,input:10,output:5,cached:1,usageKnown:true,actualModel:'gpt-6.1-sol'};}};}

test('PostgreSQL Chat queue serializes one account and queued client cancellation releases reservations before another request',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);const secondKey=await otherHttpChatKey(store,'chat-second'),held=heldHttpChat(),app=await open({executor:held.executor}),origin=await listenGateway(app),first=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'first'}]});let second;
 try{await gatewayWait(()=>held.calls.length===1);const controller=new AbortController();second=sendHttpChat(origin,secondKey,{messages:[{role:'user',content:'second'}]},{signal:controller.signal});second.catch(()=>{});await gatewayWait(()=>app.status().chat.queued===1);assert.equal(held.calls.length,1);controller.abort();await assert.rejects(second,{name:'AbortError'});await gatewayWait(()=>app.status().chat.queued===0&&app.status().chat.requests===1);const cancelled=await store.get("SELECT * FROM harbor.requests WHERE member_id='chat-second'");assert.equal(cancelled.status,'cancelled');assert.equal(cancelled.reserved,0);assert.equal(cancelled.usage_unknown,0);held.gates[0]();assert.equal((await first).status,200);await gatewayWait(()=>app.status().chat.requests===0);
 }finally{for(const release of held.gates)release();await first.catch(()=>{});await second?.catch(()=>{});}
}));

for(const scenario of ['timeout','disabled-key'])test('PostgreSQL Chat queued '+scenario+' is rejected without starting another CLI',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);const secondKey=await otherHttpChatKey(store,'chat-waiter'),held=heldHttpChat(),app=await open({executor:held.executor,chat:{waitMs:scenario==='timeout'?100:120000}}),origin=await listenGateway(app),first=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'first'}]});let second;
 let mockedQueueClock=false;
 try{await gatewayWait(()=>held.calls.length===1);if(scenario==='timeout'){t.mock.timers.enable({apis:['setTimeout']});mockedQueueClock=true;}second=sendHttpChat(origin,secondKey,{messages:[{role:'user',content:'waiter'}]});
  if(scenario==='timeout'){
   // Keep the 100ms deadline pending until the queue state is observed, even on a busy CI host.
   const deadline=Date.now()+5000;while(app.status().chat.queued!==1){assert.ok(Date.now()<deadline,'chat did not enter queue');await new Promise(setImmediate);}
   t.mock.timers.tick(100);t.mock.timers.reset();mockedQueueClock=false;
  }else await gatewayWait(()=>app.status().chat.queued===1);if(scenario==='disabled-key'){await store.run("UPDATE harbor.keys SET enabled=0 WHERE id='chat-waiter-key'");held.gates[0]();assert.equal((await first).status,200);}const rejected=await second;assert.equal(rejected.status,scenario==='timeout'?503:403);assert.equal(held.calls.length,1);const row=await store.get("SELECT * FROM harbor.requests WHERE member_id='chat-waiter'");assert.equal(row.status,'rejected');assert.equal(row.reserved,0);assert.equal(row.usage_unknown,0);if(scenario==='timeout'){held.gates[0]();await first;}
 }finally{if(mockedQueueClock)t.mock.timers.reset();for(const release of held.gates)release();await first.catch(()=>{});await second?.catch(()=>{});}
}));

test('PostgreSQL Chat queued work follows a disabled account new binding while in-flight billing stays with its old account',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 await prepareHttpChat(store,member);const secondKey=await otherHttpChatKey(store,'chat-migrating'),held=heldHttpChat(),app=await open({executor:held.executor}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),first=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'first'}]});let second;
 try{await gatewayWait(()=>held.calls.length===1);second=sendHttpChat(origin,secondKey,{messages:[{role:'user',content:'queued message'}]});await gatewayWait(()=>app.status().chat.queued===1);await store.run("INSERT INTO harbor.accounts(id,name,home,state,token_budget) VALUES('synthetic-chat-peer','synthetic','synthetic-peer-home','ready',1000000)");const disabled=await fetch(origin+'/api/admin/accounts/synthetic-runtime',{method:'PATCH',headers:{cookie,'content-type':'application/json'},body:'{"enabled":false}'});assert.equal(disabled.status,200);await gatewayWait(()=>held.calls.length===2);assert.equal(held.calls[1].home,'synthetic-peer-home');assert.equal(held.calls[1].sessionId,null);assert.deepEqual(held.calls[1].previousUsage,{input:0,output:0,cached:0});held.gates[1]();const moved=await second;assert.equal(moved.status,200);assert.equal((await moved.json()).context_reset,true);held.gates[0]();assert.equal((await first).status,200);
 const rows=await store.all('SELECT member_id,account_id,route_version FROM harbor.requests ORDER BY query_sequence');assert.equal(rows[0].account_id,'synthetic-runtime');assert.equal(rows[1].account_id,'synthetic-chat-peer');assert.equal(rows[1].route_version,1);const old=await store.get('SELECT * FROM harbor.sessions WHERE member_id=$1',member.memberId);assert.equal(old.account_id,'synthetic-chat-peer');assert.equal(old.upstream_id,null);assert.equal(old.message_count,0);assert.equal((await store.get("SELECT budget_used FROM harbor.accounts WHERE id='synthetic-runtime'")).budget_used,15);assert.equal((await store.get("SELECT budget_used FROM harbor.accounts WHERE id='synthetic-chat-peer'")).budget_used,15);
 }finally{for(const release of held.gates)release();await first.catch(()=>{});await second?.catch(()=>{});}
}));

test('PostgreSQL Chat survives billing outage with a typed cursor spool and restart never repeats the CLI',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);let calls=0;const executor=async()=>{calls++;return {text:'synthetic completed reply',threadId:'synthetic-recovered-thread',input:10,output:5,cached:1,usageKnown:true,actualModel:'gpt-6.1-sol'};},app=await open({executor}),origin=await listenGateway(app);app.store.finish=async()=>{throw Error('synthetic billing unavailable');};const response=await sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'synthetic'}]});assert.equal(response.status,200);assert.equal(response.headers.get('x-harbor-usage'),'pending');assert.equal((await response.json()).harbor_standard_usage,null);assert.equal(app.meteringRecovery.status().files,1);await app.close();const restarted=await open({executor});assert.equal(calls,1);assert.equal((await store.get('SELECT status FROM harbor.requests')).status,'ok');assert.equal((await store.get('SELECT used FROM harbor.members')).used,15);assert.equal((await store.get('SELECT upstream_id FROM harbor.sessions')).upstream_id,'synthetic-recovered-thread');assert.equal(restarted.meteringRecovery.status().files,0);
}));

test('PostgreSQL Chat graceful drain cancels queued work but allows its accepted CLI to complete',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);const other=await otherHttpChatKey(store,'chat-drain'),held=heldHttpChat(),app=await open({executor:held.executor,drainTimeoutMs:2000}),origin=await listenGateway(app),first=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'first'}]});let second,closing;
 try{await gatewayWait(()=>held.calls.length===1);second=sendHttpChat(origin,other,{messages:[{role:'user',content:'queued'}]});await gatewayWait(()=>app.status().chat.queued===1);closing=app.close();assert.equal((await second).status,503);held.gates[0]();assert.equal((await first).status,200);assert.equal((await closing).forced,false);assert.equal(held.calls.length,1);assert.equal((await store.get("SELECT reserved FROM harbor.requests WHERE member_id='chat-drain'")).reserved,0);
 }finally{for(const release of held.gates)release();await first.catch(()=>{});await second?.catch(()=>{});await closing;}
}));

test('PostgreSQL Chat HTTP validates before dispatch and clears all preparation state after denied or oversized input',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);let calls=0;const executor=async()=>{calls++;return {text:'synthetic',threadId:'synthetic-valid-thread',input:1,output:0,cached:0,usageKnown:true};},app=await open({executor}),origin=await listenGateway(app);assert.equal((await fetch(origin+'/v1/chat/completions',{method:'POST',headers:{'content-type':'application/json'},body:'not-json'})).status,401);assert.equal((await sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'x'}],tools:[]})).status,400);assert.equal((await sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'x'.repeat(131072)}]})).status,413);await store.run('UPDATE harbor.keys SET allowed_models=$1 WHERE id=$2',JSON.stringify(['gpt-6.1-sol']),member.keyId);assert.equal((await sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'x'}]})).status,403);assert.equal(calls,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);await store.run('UPDATE harbor.keys SET allowed_models=NULL WHERE id=$1',member.keyId);assert.equal((await sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'valid'}],reserveTokens:0,timeoutMs:0})).status,200);assert.equal(calls,1);assert.deepEqual(app.preparation.status(),{readers:0,waiting:0,blockedMembers:0});assert.equal(app.status().preparing,0);
}));

test('PostgreSQL Chat request capacity cannot be bypassed by a body option and is released after completion',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);const other=await otherHttpChatKey(store,'chat-capacity'),held=heldHttpChat(),app=await open({executor:held.executor,chat:{maxRequests:1,maxQueued:1}}),origin=await listenGateway(app),first=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'first'}]});try{await gatewayWait(()=>held.calls.length===1);const denied=await sendHttpChat(origin,other,{messages:[{role:'user',content:'denied'}],maxRequests:99});assert.equal(denied.status,429);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);held.gates[0]();await first;await gatewayWait(()=>app.status().chat.requests===0);const next=sendHttpChat(origin,other,{messages:[{role:'user',content:'next'}]});await gatewayWait(()=>held.calls.length===2);held.gates[1]();assert.equal((await next).status,200);}finally{for(const release of held.gates)release();await first.catch(()=>{});}
}));

test('PostgreSQL Chat HTTP partial CLI failure preserves its saved reply prefix and unknown reservation',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 await prepareHttpChat(store,member);let calls=0;const app=await open({executor:async args=>{calls++;args.onText('合成未完成的回复');throw Object.assign(Error('synthetic upstream failure'),{usageKnown:false});}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),response=await sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'合成问题'}]});assert.equal(response.status,502);assert.equal(calls,1);const request=await store.get('SELECT * FROM harbor.requests');assert.equal(request.status,'unknown');assert.equal(request.reserved,32000);assert.equal(request.usage_unknown,1);await app.buffer.flush();const body=await (await fetch(origin+'/api/admin/requests/'+request.id+'/recording?limit=100',{headers:{cookie}})).json();assert.equal(body.state,'partial');assert.match(JSON.stringify(body),/合成未完成的回复/);const again=await sendHttpChat(origin,member.secret,{session_id:request.session_id,messages:[{role:'user',content:'retry'}]});assert.equal(again.status,409);assert.equal(calls,1);
}));

test('PostgreSQL Chat forced drain cancels its real CLI and retains the interrupted request for recovery without replay',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);let calls=0;const {runCodex}=await import('../codex.mjs'),app=await open({drainTimeoutMs:100,executor:args=>{calls++;return runCodex({...args,spawnProcess:(_binary,_args,options)=>spawn(process.execPath,[new URL('./provider-child.mjs',import.meta.url).pathname,'codex','hang'],options)});}}),origin=await listenGateway(app),pending=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'synthetic'}]}).then(response=>({response}),error=>({error}));await gatewayWait(()=>calls===1);const closed=await app.close();assert.equal(closed.forced,true);await pending;const row=await store.get('SELECT * FROM harbor.requests');assert.equal(row.status,'interrupted');assert.equal(row.usage_unknown,1);assert.equal(row.reserved,32000);await open();assert.equal(calls,1);assert.equal((await store.get('SELECT status FROM harbor.requests')).status,'interrupted');
}));

test('R31 PostgreSQL mixed HTTP requests do not cancel already queued Chat when later Responses runs elsewhere',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);const blockerKey=await otherHttpChatKey(store,'response-blocker'),responseGates=[];let responseCalls=0,chatCalls=0;const app=await open({responsesTransport:controlledResponses(async()=>{responseCalls++;await new Promise(resolve=>responseGates.push(resolve));}),executor:async()=>{chatCalls++;return {text:'synthetic mixed reply',threadId:'synthetic-mixed-thread',input:10,output:5,cached:1,usageKnown:true};}}),origin=await listenGateway(app),blocker=gatewaySend(origin,blockerKey);let chat,other;
 try{await gatewayWait(()=>responseCalls===1);chat=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'queued'}]});await gatewayWait(()=>app.status().chat.queued===1);await store.run("INSERT INTO harbor.accounts(id,name,home,state,token_budget) VALUES('synthetic-mixed-peer','synthetic','synthetic','ready',1000000)");other=gatewaySend(origin,member.secret);await gatewayWait(()=>responseCalls===2);responseGates[0]();assert.equal((await blocker).status,200);const result=await chat;assert.equal(result.status,200);assert.equal(chatCalls,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE source_channel='responses' AND status='running'")).n,1);responseGates[1]();assert.equal((await other).status,200);
 }finally{for(const release of responseGates)release();await blocker.catch(()=>{});await chat?.catch(()=>{});await other?.catch(()=>{});}
}));

test('PostgreSQL Chat queue timeout during dispatch rolls back before freeing its account or attempting the CLI',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);let calls=0,resume,held=false;const app=await open({executor:async()=>{calls++;},chat:{waitMs:100}}),origin=await listenGateway(app),run=app.store.run.bind(app.store);app.store.run=async(sql,...args)=>{const result=await run(sql,...args);if(sql.startsWith("UPDATE harbor.requests SET phase='cli'")){held=true;await new Promise(resolve=>{resume=resolve;});}return result;};let pending;
 try{t.mock.timers.enable({apis:['setTimeout']});pending=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'synthetic'}]});const end=Date.now()+3000;while(!held){assert.ok(Date.now()<end,'dispatch did not reach transaction hold');await new Promise(setImmediate);}assert.equal((await store.get('SELECT status FROM harbor.requests')).status,'queued');t.mock.timers.tick(100);assert.equal(calls,0);resume();const response=await pending;assert.equal(response.status,503);assert.equal((await response.json()).error.code,'chat_queue_timeout');const row=await store.get('SELECT * FROM harbor.requests');assert.equal(row.status,'rejected');assert.equal(row.reserved,0);assert.equal(row.usage_unknown,0);assert.equal(calls,0);assert.deepEqual(app.status().chat,{requests:0,queued:0});
 }finally{resume?.();t.mock.timers.reset();app.store.run=run;await pending?.catch(()=>{});}
}));

const platformAdminRequest=(origin,cookie,path,method='GET',payload)=>fetch(origin+'/api/admin/platform/'+path,{method,headers:{cookie,'content-type':'application/json'},...(payload===undefined?{}:{body:JSON.stringify(payload)})});
test('PostgreSQL platform administration creates independent encrypted credentials and checks each supported manual provider over real HTTP',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const calls=[],app=await open({platformTransport:{models:async(row,value)=>{calls.push({row,value});return ['synthetic-model'];}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);
 assert.equal((await platformAdminRequest(origin,'','providers')).status,401);const providers=await (await platformAdminRequest(origin,cookie,'providers')).json();assert.deepEqual(providers.providers.map(p=>p.id),['workbuddy','qwen','deepseek','qoder']);
 const ids=[];for(const provider of ['qwen','deepseek','qoder']){const secret='synthetic-platform-secret-'+provider;const added=await platformAdminRequest(origin,cookie,'accounts','POST',{provider,name:'合成'+provider,credential:secret});assert.equal(added.status,201);const {id}=await added.json();ids.push(id);const row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);assert.equal(row.auth_mode,'platform');assert.equal(row.state,'offline');assert.ok(row.api_secret_cipher);assert.ok(!row.api_secret_cipher.includes(secret));assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id+'/check','POST',{})).status,200);assert.equal(calls.at(-1).value.token,secret);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',id)).state,'ready');}
 assert.equal(new Set(ids).size,3);assert.equal((await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'unknown',name:'invalid',credential:'synthetic'})).status,400);const state=JSON.stringify(await (await fetch(origin+'/api/admin/state',{headers:{cookie}})).json());assert.doesNotMatch(state,/synthetic-platform-secret/);assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+ids[0],'PATCH',{credential:'synthetic-next-secret',expected_epoch:0})).status,200);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',ids[0])).state,'offline');assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+ids[0],'PATCH',{credential:'synthetic-stale',expected_epoch:0})).status,409);
}));

test('PostgreSQL platform check deduplicates and a credential change rejects stale model access results',integration,async t=>withGateway(t,async({store,password,open})=>{
 let release,calls=0;const modelGate=new Promise(resolve=>{release=resolve;}),app=await open({platformTransport:{models:async()=>{calls++;await modelGate;return ['old-model'];}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),added=await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'deepseek',name:'合成账号',credential:'synthetic-platform-key'}),{id}=await added.json();const first=platformAdminRequest(origin,cookie,'accounts/'+id+'/check','POST',{}),get=app.store.get.bind(app.store);let second,secondSnapshot=false;
 try{await gatewayWait(()=>calls===1);
  // Observe the second handler's old credential snapshot, rather than the
  // first check's already-true status. Its continuation joins before polling.
  app.store.get=async(sql,...args)=>{const row=await get(sql,...args);if(sql==="SELECT * FROM harbor.accounts WHERE id=$1 AND auth_mode='platform' AND removed_at IS NULL"&&args[0]===id)secondSnapshot=true;return row;};
  second=platformAdminRequest(origin,cookie,'accounts/'+id+'/check','POST',{});await gatewayWait(()=>secondSnapshot);app.store.get=get;assert.equal(app.status().accountChecks.platform.checks,1);assert.equal(calls,1);assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id,'PATCH',{credential:'synthetic-platform-new',expected_epoch:0})).status,200);release();assert.equal((await first).status,409);assert.equal((await second).status,409);assert.equal(calls,1);const row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);assert.equal(row.state,'offline');assert.equal(row.platform_models,'[]');assert.equal(row.last_error,null);
 }finally{app.store.get=get;release();await first.catch(()=>{});await second?.catch(()=>{});}
}));

test('PostgreSQL platform credential changes and account creation roll back when their audit fails',integration,async t=>withGateway(t,async({store,password,open})=>{
 const app=await open({platformTransport:{models:async()=>['synthetic']}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),added=await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'qwen',name:'合成',credential:'synthetic-original'}),{id}=await added.json(),before=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);
 await store.run("CREATE FUNCTION harbor.fail_platform_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action IN ('account.platform.credential','account.create') THEN RAISE EXCEPTION 'synthetic audit outage'; END IF; RETURN NEW; END $$");await store.run('CREATE TRIGGER fail_platform_audit BEFORE INSERT ON harbor.audit FOR EACH ROW EXECUTE FUNCTION harbor.fail_platform_audit()');assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id,'PATCH',{expected_epoch:0,credential:'synthetic-replacement'})).status,500);const after=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);assert.equal(after.api_secret_cipher,before.api_secret_cipher);assert.equal(after.route_epoch,0);assert.equal((await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'qoder',name:'合成失败',credential:'synthetic'})).status,500);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.accounts WHERE auth_mode='platform'")).n,1);
}));

test('PostgreSQL WorkBuddy login waits without holding the database and cancellation rejects late credentials',integration,async t=>withGateway(t,async({store,password,open})=>{
 let complete,started=0;const app=await open({platformTransport:{login:async({onChallenge})=>{started++;onChallenge({loginUrl:'https://example.invalid/synthetic'});return new Promise(resolve=>{complete=resolve;});},models:async()=>['synthetic']}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),added=await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'workbuddy',name:'合成WorkBuddy'}),{id}=await added.json();
 const response=await platformAdminRequest(origin,cookie,'accounts/'+id+'/login','POST',{});assert.equal(response.status,200);assert.match((await response.json()).loginUrl,/example.invalid/);assert.equal((await store.get('SELECT 1 ready')).ready,1);assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id+'/login','POST',{})).status,200);assert.equal(started,1);assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id+'/login/cancel','POST',{})).status,200);complete({accessToken:'synthetic-late-secret',expiresAt:Date.now()+3600000});await gatewayWait(()=>app.status().accountChecks.platform.tasks===0);const row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);assert.equal(row.state,'offline');assert.equal(row.api_secret_cipher,null);assert.equal(row.route_epoch,2);
}));

test('PostgreSQL WorkBuddy verifies login before ready and deduplicates refresh without nesting the single check slot',integration,async t=>withGateway(t,async({store,password,open})=>{
 let complete,refreshRelease,refreshCalls=0;const credentials=[],app=await open({accountChecks:{maxConcurrent:1},platformTransport:{login:async({onChallenge})=>{onChallenge({type:'browser',verificationUrl:'https://example.invalid/synthetic'});return new Promise(resolve=>{complete=resolve;});},models:async(_row,value)=>{credentials.push(value);return ['synthetic-model'];},refresh:async value=>{refreshCalls++;await new Promise(resolve=>{refreshRelease=resolve;});return {...value,accessToken:'synthetic-refreshed',expiresAt:Date.now()+3600000};}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),added=await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'workbuddy',name:'合成WorkBuddy'}),{id}=await added.json();assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id+'/login','POST',{})).status,200);complete({accessToken:'synthetic-expired',refreshToken:'synthetic-refresh',expiresAt:0});await gatewayWait(()=>app.status().accountChecks.platform.tasks===0);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',id)).state,'ready');
 let first,second;try{first=platformAdminRequest(origin,cookie,'accounts/'+id+'/check','POST',{});await gatewayWait(()=>refreshCalls===1);second=platformAdminRequest(origin,cookie,'accounts/'+id+'/check','POST',{});refreshRelease();assert.equal((await first).status,200);assert.equal((await second).status,200);assert.equal(refreshCalls,1);assert.equal(credentials.at(-1).accessToken,'synthetic-refreshed');assert.equal(app.status().accountChecks.platform.refreshes,0);}finally{refreshRelease?.();await first?.catch(()=>{});await second?.catch(()=>{});}
}));

test('PostgreSQL platform shutdown aborts an active model check without marking the stopped account ready',integration,async t=>withGateway(t,async({store,password,open})=>{
 let entered=false,aborted=false;const app=await open({platformTransport:{models:async(_row,_value,signal)=>{entered=true;await new Promise((resolve,reject)=>{signal.addEventListener('abort',()=>{aborted=true;reject(Error('synthetic check cancelled'));},{once:true});});return ['synthetic'];}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),added=await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'qwen',name:'合成',credential:'synthetic-key'}),{id}=await added.json(),pending=platformAdminRequest(origin,cookie,'accounts/'+id+'/check','POST',{});pending.catch(()=>{});await gatewayWait(()=>entered);await app.close();await pending.catch(()=>{});assert.equal(aborted,true);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',id)).state,'offline');assert.equal(app.status().accountChecks.platform.tasks,0);
}));

for(const change of ['disable','remove'])test('PostgreSQL WorkBuddy '+change+' cancels login and never applies its late success',integration,async t=>withGateway(t,async({store,password,open})=>{
 let complete;const app=await open({platformTransport:{login:async({onChallenge})=>{onChallenge({type:'browser',verificationUrl:'https://example.invalid/synthetic'});return new Promise(resolve=>{complete=resolve;});},models:async()=>['synthetic']}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),{id}=await (await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'workbuddy',name:'合成'})).json();assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id+'/login','POST',{})).status,200);const response=await fetch(origin+'/api/admin/accounts/'+id,{method:change==='disable'?'PATCH':'DELETE',headers:{cookie,'content-type':'application/json'},...(change==='disable'?{body:'{"enabled":false}'}:{})});assert.equal(response.status,200);complete({accessToken:'synthetic-late',expiresAt:Date.now()+3600000});await gatewayWait(()=>app.status().accountChecks.platform.tasks===0);const row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);assert.equal(row.enabled,0);assert.equal(row.api_secret_cipher,null);assert.equal(row.state,change==='disable'?'offline':'removed');
}));

test('PostgreSQL WorkBuddy failed model verification does not persist a successful authorization or mark ready',integration,async t=>withGateway(t,async({store,password,open})=>{
 let complete;const app=await open({platformTransport:{login:async({onChallenge})=>{onChallenge({type:'browser',verificationUrl:'https://example.invalid/synthetic'});return new Promise(resolve=>{complete=resolve;});},models:async()=>{throw Error('synthetic secret must not persist');}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),{id}=await (await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'workbuddy',name:'合成'})).json();assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id+'/login','POST',{})).status,200);complete({accessToken:'synthetic-token',expiresAt:Date.now()+3600000});await gatewayWait(()=>app.status().accountChecks.platform.tasks===0);const row=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',id);assert.equal(row.state,'offline');assert.equal(row.api_secret_cipher,null);assert.doesNotMatch(row.last_error,/secret/);
}));

test('PostgreSQL platform check queue saturation preserves an already checked account state',integration,async t=>withGateway(t,async({store,password,open})=>{
 let release,calls=0;const app=await open({accountChecks:{maxConcurrent:1,maxQueued:0},platformTransport:{models:async()=>{calls++;await new Promise(resolve=>{release=resolve;});return ['synthetic'];}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),create=async name=>(await (await platformAdminRequest(origin,cookie,'accounts','POST',{provider:'deepseek',name,credential:'synthetic-key-'+name})).json()).id,firstId=await create('first'),secondId=await create('second');await store.run("UPDATE harbor.accounts SET state='ready',platform_models='[\"synthetic\"]' WHERE id=$1",secondId);const first=platformAdminRequest(origin,cookie,'accounts/'+firstId+'/check','POST',{});
 try{await gatewayWait(()=>calls===1);const response=await platformAdminRequest(origin,cookie,'accounts/'+secondId+'/check','POST',{});assert.equal(response.status,503);assert.equal((await response.json()).error.code,'query_queue_full');const row=await store.get('SELECT state,last_error FROM harbor.accounts WHERE id=$1',secondId);assert.equal(row.state,'ready');assert.equal(row.last_error,null);release();assert.equal((await first).status,200);}finally{release?.();await first.catch(()=>{});}
}));

const platformSend=(origin,secret,body,options={})=>fetch(origin+'/v1/platform/chat/completions',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json'},body:JSON.stringify(body),...options});
async function preparePlatformAccount(origin,cookie,provider='deepseek'){const response=await platformAdminRequest(origin,cookie,'accounts','POST',{provider,name:'合成平台'+provider,credential:'synthetic-platform-credential'});assert.equal(response.status,201);const {id}=await response.json();if(provider!=='workbuddy')assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+id+'/check','POST',{})).status,200);return id;}
const platformResult={text:'合成平台回复',input:100,output:20,cached:30,usageKnown:true,actualModel:'deepseek/synthetic-model'};

test('PostgreSQL platform inference uses the original employee Key, model policy, binding and current-turn recording over real HTTP',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const calls=[],app=await open({platformTransport:{models:async()=>['synthetic-model','other-model'],run:async(row,credential,payload,_signal,{onText})=>{calls.push({row,credential,payload});onText(platformResult.text.slice(0,3));onText(platformResult.text.slice(3));return platformResult;}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),accountId=await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=$1,expires=NULL WHERE id=$2',JSON.stringify(['deepseek/synthetic-model']),member.keyId);
 let catalog=await fetch(origin+'/v1/platform/models',{headers:{authorization:'Bearer '+member.secret}});assert.equal(catalog.status,200);assert.deepEqual((await catalog.json()).data.map(m=>m.id),['deepseek/synthetic-model']);const old={role:'user',content:'合成旧问题'},message={role:'user',content:'合成本轮问题 Authorization: Bearer sk-synthetic-sensitive-1234567890'},payload={model:'deepseek/synthetic-model',messages:[old,{role:'assistant',content:'合成旧回复'},message]},response=await platformSend(origin,member.secret,payload);assert.equal(response.status,200);const first=await response.json();assert.equal(first.choices[0].message.content,platformResult.text);assert.equal(first.usage.prompt_tokens,100);assert.equal(calls[0].credential.token,'synthetic-platform-credential');assert.deepEqual(calls[0].payload.messages,payload.messages);assert.equal(calls[0].row.id,accountId);const request=response.headers.get('x-harbor-request');await app.buffer.flush();const row=await store.get('SELECT * FROM harbor.requests WHERE id=$1',request);assert.equal(row.source_channel,'platform');assert.equal(row.status,'ok');assert.equal(row.account_id,accountId);assert.equal(row.input,100);assert.equal((await store.get('SELECT state FROM harbor.sessions WHERE id=$1',first.session_id)).state,'active');
 const record=await (await fetch(origin+'/api/admin/requests/'+request+'/recording?limit=100',{headers:{cookie}})).json(),text=JSON.stringify(record);assert.equal(record.state,'complete');assert.match(text,/合成本轮问题/);assert.doesNotMatch(text,/合成旧问题|合成旧回复|sk-synthetic-sensitive/);assert.equal(record.events.filter(e=>e.kind==='assistant').map(e=>e.content.text||'').join(''),platformResult.text);const continued=await platformSend(origin,member.secret,{model:payload.model,session_id:first.session_id,stream:true,messages:[...payload.messages,{role:'assistant',content:first.choices[0].message.content},{role:'user',content:'合成后续'}]});assert.equal(continued.status,200);assert.match(await continued.text(),/\[DONE\]/);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions WHERE member_id=$1',member.memberId)).n,1);
 const otherKey=await otherHttpChatKey(store,'platform-other');assert.equal((await platformSend(origin,otherKey,{...payload,session_id:first.session_id})).status,404);assert.equal((await platformSend(origin,member.secret,{...payload,model:'deepseek/other-model'})).status,403);assert.equal(calls.length,2);assert.equal(app.status().active,0);await store.run('UPDATE harbor.keys SET allowed_models=NULL WHERE id=$1',member.keyId);assert.equal((await platformSend(origin,member.secret,{...payload,session_id:first.session_id,model:'qwen/synthetic-model'})).status,409);assert.equal((await platformSend(origin,member.secret,{...payload,session_id:first.session_id,model:'deepseek/unsupported-model'})).status,409);await store.run('UPDATE harbor.sessions SET enabled=0 WHERE id=$1',first.session_id);assert.equal((await platformSend(origin,member.secret,{...payload,session_id:first.session_id})).status,403);assert.equal(calls.length,2);
}));

for(const provider of ['qwen','deepseek'])test('PostgreSQL platform '+provider+' inference reaches the actual compatible parser with controlled upstream',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const {createPlatformTransport}=await import('../platforms.mjs');let calls=0;const app=await open({platformTransport:createPlatformTransport({fetchImpl:async(url,options)=>{if(new URL(url).pathname.endsWith('/models'))return new Response(JSON.stringify({data:[{id:'synthetic-model'}]}));calls++;assert.equal(options.headers.Authorization,'Bearer synthetic-platform-credential');return new Response(JSON.stringify({model:'synthetic-model',choices:[{message:{content:'合成真实解析回复'},finish_reason:'stop'}],usage:{prompt_tokens:100,completion_tokens:20,prompt_tokens_details:{cached_tokens:30}}}));}})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie,provider);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const response=await platformSend(origin,member.secret,{model:provider+'/synthetic-model',messages:[{role:'user',content:'合成测试'}]});assert.equal(response.status,200);assert.equal((await response.json()).choices[0].message.content,'合成真实解析回复');assert.equal(calls,1);const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(row.billing_model,provider+'/synthetic-model');assert.equal(row.cached,30);
}));

test('PostgreSQL platform dispatch rechecks a Key disabled after admission and never calls upstream',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 let calls=0;const app=await open({platformTransport:{models:async()=>['synthetic-model'],run:async()=>{calls++;return platformResult;}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const original=app.store.transaction.bind(app.store);let held=false,resume;app.store.transaction=async(fn,...args)=>{const result=await original(fn,...args);if(result?.requestId&&result.account?.auth_mode==='platform'&&!held){held=true;await new Promise(resolve=>{resume=resolve;});}return result;};const pending=platformSend(origin,member.secret,{model:'deepseek/synthetic-model',messages:[{role:'user',content:'合成'}]});
 try{await gatewayWait(()=>held);await store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',member.keyId);resume();const response=await pending;assert.equal(response.status,403);assert.equal(calls,0);const row=await store.get('SELECT status,usage_unknown FROM harbor.requests');assert.equal(row.status,'rejected');assert.equal(row.usage_unknown,0);
 }finally{resume?.();app.store.transaction=original;await pending.catch(()=>{});}
}));

test('PostgreSQL platform Qoder request runs a controlled real CLI and settles normalized usage',integration,async t=>withGateway(t,async({store,member,password,root,open})=>{
 const {createPlatformTransport}=await import('../platforms.mjs'),binary=join(root,'synthetic-qoder');writeFileSync(binary,"#!/usr/bin/env node\nif(process.argv.includes('--list-models'))console.log('MODEL\\nPerformance');else{process.stdin.resume();process.stdin.on('end',()=>console.log(JSON.stringify({result:'合成Qoder回复',usage:{input_tokens:100,output_tokens:20,cache_read_input_tokens:30}})));}\n",{mode:0o700});const app=await open({platformTransport:createPlatformTransport({qoderBinary:binary})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie,'qoder');await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const model='qoder/name-'+Buffer.from('Performance').toString('base64url'),response=await platformSend(origin,member.secret,{model,messages:[{role:'user',content:'合成'}]});assert.equal(response.status,200);assert.equal((await response.json()).choices[0].message.content,'合成Qoder回复');const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(row.input,100);assert.equal(row.cached,30);await app.buffer.flush();
}));

test('PostgreSQL WorkBuddy incremental parser saves the received reply prefix when the real response stream fails',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const {createPlatformTransport}=await import('../platforms.mjs');let complete,streamController;const parser=createPlatformTransport({fetchImpl:async()=>new Response(new ReadableStream({start(controller){streamController=controller;controller.enqueue(Buffer.from('data: '+JSON.stringify({choices:[{index:0,delta:{content:'合成中断前缀 🍋'}}]})+'\r\n\r\n'));}}))}),app=await open({platformTransport:{...parser,login:async({onChallenge})=>{onChallenge({type:'browser',verificationUrl:'https://copilot.tencent.com/synthetic'});return new Promise(resolve=>{complete=resolve;});},models:async()=>['synthetic-model']}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),accountId=await preparePlatformAccount(origin,cookie,'workbuddy');assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+accountId+'/login','POST',{})).status,200);complete({accessToken:'synthetic-access',expiresAt:Date.now()+3600000});await gatewayWait(()=>app.status().accountChecks.platform.tasks===0);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const pending=platformSend(origin,member.secret,{model:'workbuddy/synthetic-model',messages:[{role:'user',content:'合成问题'}]});
 try{await gatewayWait(()=>!!streamController);await gatewayWait(async()=>{await app.buffer.flush();const request=await store.get("SELECT id FROM harbor.requests WHERE source_channel='platform'");if(!request)return false;const value=await (await fetch(origin+'/api/admin/requests/'+request.id+'/recording?limit=100',{headers:{cookie}})).json();return JSON.stringify(value).includes('合成中断前缀');});streamController.error(Error('synthetic network loss'));const response=await pending;assert.equal(response.status,502);await app.buffer.flush();const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(row.status,'unknown');assert.equal(row.usage_unknown,1);const record=await (await fetch(origin+'/api/admin/requests/'+row.id+'/recording?limit=100',{headers:{cookie}})).json();assert.equal(record.state,'partial');assert.match(JSON.stringify(record),/合成中断前缀/);
 }finally{try{streamController?.error(Error('synthetic cleanup'));}catch{}await pending.catch(()=>{});}
}));

test('PostgreSQL platform metering outage persists recovery and never repeats model execution on restart',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 let calls=0;const transport={models:async()=>['synthetic-model'],run:async()=>{calls++;return platformResult;}},app=await open({platformTransport:transport}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);app.store.finish=async()=>{throw Error('synthetic database outage');};const response=await platformSend(origin,member.secret,{model:'deepseek/synthetic-model',messages:[{role:'user',content:'合成'}]});assert.equal(response.status,200);assert.equal(response.headers.get('x-harbor-usage'),'pending');assert.equal((await response.json()).harbor_standard_usage,null);assert.equal(app.meteringRecovery.status().files,1);await app.close();await open({platformTransport:transport});assert.equal(calls,1);const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(row.status,'ok');assert.equal(row.input,100);assert.equal((await store.get('SELECT used FROM harbor.members WHERE id=$1',member.memberId)).used,120);
}));

for(const recording of [true,false])test('PostgreSQL platform unknown usage is never silently converted to zero with recording '+recording,integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const app=await open({recording:{enabled:recording},platformTransport:{models:async()=>['synthetic-model'],run:async()=>({text:'合成未知计量回复'})}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const response=await platformSend(origin,member.secret,{model:'deepseek/synthetic-model',messages:[{role:'user',content:'合成'}]});assert.equal(response.status,200);assert.equal((await response.json()).usage,null);const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(row.usage_unknown,1);await app.buffer.flush();assert.equal((await store.get('SELECT recording_state FROM harbor.requests WHERE id=$1',row.id)).recording_state,recording?'complete':'disabled');
}));

test('PostgreSQL platform shared USD limits apply across employee Keys and reject only later requests',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 let calls=0;const app=await open({platformTransport:{models:async()=>['synthetic-model'],run:async()=>{calls++;return {...platformResult,input:1000000,output:200000,cached:300000};}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const book={rates:{'deepseek/synthetic-model':{input:'1',cached:'1',output:'1'}},aliases:[]},revision=hash(JSON.stringify(book));await store.run('INSERT INTO harbor.pricing_versions VALUES($1,$2,$3)',revision,Date.now(),JSON.stringify(book));await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='pricing.state'",JSON.stringify({active:revision}));const limits=createPostgresMemberLimits(store);await limits.save(member.memberId,{enabled:true,rules:[{model:'deepseek/synthetic-model',hours:24,limit_usd:'1.00'}],expected_version:0});const body={model:'deepseek/synthetic-model',messages:[{role:'user',content:'合成'}]},first=await platformSend(origin,member.secret,body);assert.equal(first.status,200);assert.equal((await first.json()).harbor_standard_usage.total_exact,'1.2');const key='synthetic-other-employee-key';await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,0)',randomUUID(),member.memberId,hash(key),'synthetic');const next=await platformSend(origin,key,body);assert.equal(next.status,429);assert.equal((await next.json()).error.code,'member_model_limit_exceeded');assert.equal(calls,1);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.requests WHERE source_channel='platform'")).n,1);
}));

test('PostgreSQL platform concurrency shares the Responses budget and cancellation releases every active counter',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 let calls=0;const app=await open({maxConcurrent:1,platformTransport:{models:async()=>['synthetic-model'],run:async(_a,_v,_p,signal)=>{calls++;return new Promise((resolve,reject)=>{signal.addEventListener('abort',()=>reject(Error('synthetic cancel')),{once:true});});}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const controller=new AbortController(),body={model:'deepseek/synthetic-model',messages:[{role:'user',content:'合成'}]},first=platformSend(origin,member.secret,body,{signal:controller.signal});first.catch(()=>{});
 try{await gatewayWait(()=>calls===1);assert.equal((await platformSend(origin,member.secret,body)).status,429);assert.equal((await gatewaySend(origin,member.secret)).status,429);controller.abort();await assert.rejects(first,{name:'AbortError'});await gatewayWait(()=>app.status().active===0);assert.equal(app.status().preparing,0);assert.equal((await store.get("SELECT usage_unknown FROM harbor.requests WHERE source_channel='platform'")).usage_unknown,1);}finally{controller.abort();await first.catch(()=>{});}
}));

test('PostgreSQL platform forced drain marks interrupted and startup never retries the model',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 let calls=0;const transport={models:async()=>['synthetic-model'],run:async(_a,_v,_p,signal)=>{calls++;return new Promise((resolve,reject)=>{signal.addEventListener('abort',()=>reject(Error('synthetic stopping')),{once:true});});}},app=await open({platformTransport:transport}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const pending=platformSend(origin,member.secret,{model:'deepseek/synthetic-model',messages:[{role:'user',content:'合成'}]});pending.catch(()=>{});await gatewayWait(()=>calls===1);const result=await app.close();assert.equal(result.forced,true);await pending.catch(()=>{});const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(row.status,'interrupted');assert.equal(row.usage_unknown,1);await open({platformTransport:transport});assert.equal(calls,1);
}));

test('PostgreSQL self query exposes only the authenticated employee and Key seven-day exact usage with projection integrity checks',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const app=await open(),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const firstPerson=await store.authenticate(member.secret),first=await store.observe(firstPerson,'synthetic','synthetic','gpt-6.1-sol');await store.finish(first,{input:100,output:20,cached:30,usageKnown:true});const secondSecret='synthetic-sibling-key',secondId=randomUUID();await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,0)',secondId,member.memberId,hash(secondSecret),'synthetic');const second=await store.observe(await store.authenticate(secondSecret),'synthetic','synthetic','gpt-6.1-sol');await store.finish(second,{input:200,output:10,cached:0,usageKnown:true});const other=await otherHttpChatKey(store,'self-other'),third=await store.observe(await store.authenticate(other),'synthetic','synthetic','gpt-6.1-sol');await store.finish(third,{input:99999,output:99999,cached:0,usageKnown:true});const archived=await store.observe(firstPerson,'synthetic','synthetic','gpt-6.1-sol');await store.finish(archived,{input:999999,output:999999,cached:0,usageKnown:true});const window=(await import('../store.mjs')).usageWindow(Date.now());await store.run('UPDATE harbor.requests SET started=$1,ended=$1 WHERE id=$2',window.start-1,archived);
 const read=()=>fetch(origin+'/api/me?member=self-other&key='+secondId,{headers:{authorization:'Bearer '+member.secret}});let response=await read();assert.equal(response.status,200);let value=await response.json();assert.equal(value.id,member.memberId);assert.equal(value.standard.requests,2);assert.equal(value.keyStandard.requests,1);assert.equal(value.standard.total_exact,(await store.referenceSummary('member_id',member.memberId)).total_exact);assert.equal(value.keyStandard.total_exact,(await store.referenceSummary('key_id',member.keyId)).total_exact);assert.deepEqual(value.allowed_models,['gpt-6.1-sol']);assert.doesNotMatch(JSON.stringify(value),/synthetic-sibling-key|self-other|secret_cipher|digest/);assert.equal((await fetch(origin+'/api/me',{headers:{cookie}})).status,401);
 const saved=await store.get('SELECT price_version FROM harbor.requests WHERE id=$1',first),book=await store.pricingBook(saved.price_version);await store.run("INSERT INTO harbor.pricing_versions VALUES('synthetic-wrong-revision',$1,$2)",Date.now(),JSON.stringify(book));await store.run("UPDATE harbor.request_prices SET revision='synthetic-wrong-revision' WHERE request_id=$1",first);response=await read();value=await response.json();assert.equal(value.keyStandard.known,0);assert.equal(value.keyStandard.unpriced,1);assert.equal(value.keyStandard.total_exact,null);assert.equal(value.standard.known,1);assert.equal(value.standard.state,'partial');await store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',member.keyId);assert.equal((await read()).status,403);
}));

for(const scope of ['self','catalog'])test('R32 PostgreSQL '+scope+' read retains its authentication snapshot during concurrent commits',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const app=await open({platformTransport:{models:async()=>['synthetic-model']}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),accountId=await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const authenticate=app.queryStore.authenticate.bind(app.queryStore);let held=false,resume,actual;app.queryStore.authenticate=async(...args)=>{const result=await authenticate(...args);if(!held){held=true;actual=(await app.queryStore.get('SHOW transaction_isolation')).transaction_isolation;await new Promise(resolve=>{resume=resolve;});}return result;};const path=scope==='self'?'/api/me':'/v1/platform/models',read=()=>fetch(origin+path,{headers:{authorization:'Bearer '+member.secret}}),pending=read();
 try{await gatewayWait(()=>held);if(scope==='self'){const request=await store.observe(await store.authenticate(member.secret),'synthetic','synthetic','gpt-6.1-sol');await store.finish(request,{input:100,output:20,cached:0,usageKnown:true});}else await store.run('UPDATE harbor.accounts SET platform_models=$1 WHERE id=$2',JSON.stringify(['synthetic-model','new-model']),accountId);resume();let response=await pending;assert.equal(response.status,200);const before=await response.json();if(scope==='self')assert.equal(before.standard.requests,0);else assert.deepEqual(before.data.map(m=>m.id),['deepseek/synthetic-model']);assert.equal(actual,'repeatable read');app.queryStore.authenticate=authenticate;response=await read();const after=await response.json();if(scope==='self')assert.equal(after.standard.requests,1);else assert.deepEqual(after.data.map(m=>m.id),['deepseek/synthetic-model','deepseek/new-model']);
 }finally{resume?.();app.queryStore.authenticate=authenticate;await pending.catch(()=>{});}
}));

test('PostgreSQL body byte budget is shared across Responses, images, Chat, platform, management and login until inference finishes',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 const payload={model:'gpt-6.1-sol',input:'s'.repeat(600),stream:false},size=Buffer.byteLength(JSON.stringify(payload));let release,calls=0,platformCalls=0,chatCalls=0;const app=await open({requestBodies:{maxBufferedBytes:size*3,maxReaders:8},responsesTransport:controlledResponses(async()=>{calls++;if(calls===1)await new Promise(resolve=>{release=resolve;});}),executor:async()=>{chatCalls++;return chatResult;},platformTransport:{models:async()=>['synthetic-model'],run:async()=>{platformCalls++;return platformResult;}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password);await preparePlatformAccount(origin,cookie);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const send=()=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:JSON.stringify(payload)}),first=send();
 try{await gatewayWait(()=>calls===1);assert.equal(app.status().requestBodies.bufferedBytes,size);assert.equal(app.status().requestBodies.readers,0);const request=(path,body,headers={authorization:'Bearer '+member.secret})=>fetch(origin+path,{method:'POST',headers:{...headers,'content-type':'application/json'},body:JSON.stringify(body)});for(const [path,body,headers] of [['/v1/responses',{...payload,maxBufferedBytes:9999999999}],['/v1/images/generations',{model:'gpt-image-2',prompt:'s'.repeat(600)}],['/v1/chat/completions',{messages:[{role:'user',content:'s'.repeat(600)}]}],['/v1/platform/chat/completions',{model:'deepseek/synthetic-model',messages:[{role:'user',content:'s'.repeat(600)}]}],['/api/admin/members',{name:'合成',notes:'s'.repeat(600)},{cookie}],['/api/auth/login',{password:'s'.repeat(600)},{}]]){const response=await request(path,body,headers);assert.equal(response.status,503,path);assert.equal((await response.json()).error.code,'request_body_busy');assert.equal(app.status().requestBodies.bufferedBytes,size,path);assert.equal(app.status().requestBodies.readers,0,path);}
 assert.equal(calls,1);assert.equal(chatCalls,0);assert.equal(platformCalls,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);release();assert.equal((await first).status,200);await gatewayWait(()=>app.status().requestBodies.bufferedBytes===0);assert.equal((await send()).status,200);assert.equal(calls,2);await gatewayWait(()=>app.status().requestBodies.leases===0);assert.ok(app.status().requestBodies.peakBufferedBytes<=size*3);
 }finally{release?.();await first.catch(()=>{});}
}));

test('PostgreSQL body reader capacity rejects a second slow upload and disconnect releases all preparation and byte reservations',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0;const app=await open({requestBodies:{maxBufferedBytes:2048,maxReaders:1},responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const slow=http.request(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','content-length':'1000'}});slow.on('error',()=>{});slow.write('{"input":"');
 try{await gatewayWait(()=>app.status().requestBodies.readers===1);assert.equal(app.status().requestBodies.bufferedBytes,1000);const second=await gatewaySend(origin,member.secret);assert.equal(second.status,503);assert.equal((await second.json()).error.code,'request_body_busy');const bad=await gatewaySend(origin,'synthetic-invalid-key');assert.equal(bad.status,401);assert.equal(calls,0);slow.destroy();await gatewayWait(()=>app.status().requestBodies.leases===0&&app.status().active===0&&app.status().preparing===0);assert.equal(app.status().requestBodies.bufferedBytes,0);assert.equal((await gatewaySend(origin,member.secret)).status,200);assert.equal(calls,1);
 }finally{slow.destroy();}
}));

test('PostgreSQL compressed body expansion obeys the shared byte budget and rejected parsing cannot leak capacity',integration,async t=>withGateway(t,async({store,member,open})=>{
 const {zstdCompressSync}=await import('node:zlib');let calls=0;const app=await open({requestBodies:{maxBufferedBytes:1024},responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const packed=zstdCompressSync(Buffer.from(JSON.stringify({model:'gpt-6.1-sol',input:'s'.repeat(600),stream:false}))),send=body=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','content-encoding':'zstd'},body});let response=await send(packed);assert.equal(response.status,503);assert.equal((await response.json()).error.code,'request_body_busy');assert.equal(app.status().requestBodies.bufferedBytes,0);assert.equal(app.status().requestBodies.leases,0);response=await send(Buffer.from('synthetic-invalid-zstd'));assert.equal(response.status,400);assert.equal((await response.json()).error.code,'invalid_compressed_body');assert.equal(app.status().requestBodies.leases,0);assert.equal(calls,0);response=await gatewaySend(origin,member.secret);assert.equal(response.status,200);assert.equal(calls,1);assert.equal(app.status().requestBodies.bufferedBytes,0);
}));

test('PostgreSQL body reader fairness is shared by an employee Keys and cannot be spoofed through request headers',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0;const app=await open({requestBodies:{maxBufferedBytes:4096,maxReaders:2,maxReadersPerOwner:1},responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),sibling='synthetic-body-sibling',other=await otherHttpChatKey(store,'body-fair-peer');await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,0)',randomUUID(),member.memberId,hash(sibling),'synthetic');const slow=http.request(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','content-length':'1000'}});slow.on('error',()=>{});slow.write('{"input":"');
 try{await gatewayWait(()=>app.status().requestBodies.readers===1);const rejected=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+sibling,'content-type':'application/json','x-harbor-member':'body-fair-peer'},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成',owner:'body-fair-peer'})});assert.equal(rejected.status,503);assert.equal((await rejected.json()).error.code,'request_body_busy');const accepted=await gatewaySend(origin,other);assert.equal(accepted.status,200);assert.equal(calls,1);assert.equal(app.status().requestBodies.readers,1);assert.equal(app.status().requestBodies.bufferedBytes,1000);slow.destroy();await gatewayWait(()=>app.status().requestBodies.leases===0&&app.status().active===0);assert.equal(app.status().requestBodies.owners,0);
 }finally{slow.destroy();}
}));

const parserGate=()=>new Int32Array(new SharedArrayBuffer(4));
const releaseParser=state=>{Atomics.store(state,0,2);Atomics.notify(state,0);};
async function parserWorker(state,{firstOnly=false}={}){const {Worker}=await import('node:worker_threads');let created=0;return (url,options)=>new Worker(!firstOnly||++created===1?new URL('./json-parser-child.mjs',import.meta.url):url,{...options,workerData:{gate:state}});}

test('PostgreSQL parsing queue is isolated from health and cancellation releases bytes before another model dispatch',integration,async t=>withGateway(t,async({store,member,open})=>{
 const state=parserGate();let calls=0;const app=await open({jsonParsing:{maxWorkers:1,maxQueued:1},jsonParserWorker:await parserWorker(state,{firstOnly:true}),responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const controller=new AbortController(),body=JSON.stringify({model:'gpt-6.1-sol',input:'合成解析测试',stream:false}),send=signal=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body,signal}),first=send(controller.signal);first.catch(()=>{});let second;
 try{await gatewayWait(()=>Atomics.load(state,0)===1);assert.equal((await fetch(origin+'/health')).status,200);assert.equal((await fetch(origin+'/ready')).status,200);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);second=send();await gatewayWait(()=>app.status().jsonParsing.queued===1);const refused=await send();assert.equal(refused.status,503);assert.equal((await refused.json()).error.code,'json_parser_busy');controller.abort();await assert.rejects(first,{name:'AbortError'});assert.equal((await second).status,200);await gatewayWait(()=>app.status().requestBodies.leases===0&&app.status().active===0);assert.equal(calls,1);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);assert.equal(app.status().jsonParsing.spawned,2);assert.equal(app.status().requestBodies.bufferedBytes,0);
 }finally{controller.abort();releaseParser(state);await first.catch(()=>{});await second?.catch(()=>{});}
}));

for(const mode of ['graceful','forced'])test('PostgreSQL '+mode+' shutdown joins parsing workers and releases their byte budget',integration,async t=>withGateway(t,async({store,member,open})=>{
 const state=parserGate();let calls=0;const app=await open({jsonParsing:{maxWorkers:1},jsonParserWorker:await parserWorker(state),responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const pending=gatewaySend(origin,member.secret);pending.catch(()=>{});
 try{await gatewayWait(()=>Atomics.load(state,0)===1);const closed=app.close();assert.equal(app.status().jsonParsing.stopped,false);if(mode==='graceful')releaseParser(state);const result=await closed;assert.equal(result.forced,mode==='forced');if(mode==='graceful')assert.equal((await pending).status,200);else await pending.catch(()=>{});assert.equal(calls,mode==='graceful'?1:0);assert.equal(app.status().jsonParsing.workers,0);assert.equal(app.status().requestBodies.leases,0);assert.equal(app.status().requestBodies.bufferedBytes,0);assert.deepEqual(app.status().sessionHandoffs,{tickets:0,pending:0,waiting:0,stopped:true});
 }finally{releaseParser(state);await pending.catch(()=>{});}
}));

test('PostgreSQL cancelled login parsing cannot authenticate and a new login uses a healthy replacement',integration,async t=>withGateway(t,async({password,open})=>{
 const state=parserGate(),app=await open({jsonParsing:{maxWorkers:1},jsonParserWorker:await parserWorker(state,{firstOnly:true})}),origin=await listenGateway(app),controller=new AbortController(),pending=fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password}),signal:controller.signal});pending.catch(()=>{});
 try{await gatewayWait(()=>Atomics.load(state,0)===1);controller.abort();await assert.rejects(pending,{name:'AbortError'});await gatewayWait(()=>app.status().jsonParsing.active===0&&app.status().requestBodies.leases===0);const response=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password})});assert.equal(response.status,200);assert.ok(response.headers.get('set-cookie'));assert.equal(app.status().requestBodies.bufferedBytes,0);
 }finally{controller.abort();releaseParser(state);await pending.catch(()=>{});}
}));

for(const compressed of [false,true])test('PostgreSQL real JSON parser preserves a large '+(compressed?'zstd':'plain')+' Responses body through its actual upstream transport',integration,async t=>withGateway(t,async({store,member,open})=>{
 const {zstdCompressSync}=await import('node:zlib'),text='合成开头🍋'+randomBytes(2*1024*1024).toString('base64')+'合成末尾',payload={model:'gpt-6.1-sol',input:[{role:'user',content:'合成历史'},{type:'function_call',name:'synthetic_tool',call_id:'synthetic-call',arguments:'{"path":"synthetic"}'},{type:'function_call_output',call_id:'synthetic-call',output:text},{role:'user',content:'合成后续'}],tools:[{type:'function',name:'synthetic_tool',parameters:{type:'object',properties:{path:{type:'string'}}}}],instructions:'合成约束',stream:false,synthetic_extension:{unchanged:true}},raw=Buffer.from(JSON.stringify(payload));let seen=0;const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'0.160.1',fetchImpl:async(_url,options)=>{seen++;const actual=JSON.parse(options.body);assert.deepEqual(actual.input,payload.input);assert.deepEqual(actual.tools,payload.tools);assert.equal(actual.instructions,payload.instructions);assert.deepEqual(actual.synthetic_extension,payload.synthetic_extension);return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-large-result',model:payload.model,output:[],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:0}}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});}}),app=await open({responsesTransport:transport,recording:{enabled:false}}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const response=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json',...(compressed?{'content-encoding':'zstd'}:{})},body:compressed?zstdCompressSync(raw):raw});assert.equal(response.status,200);assert.equal((await response.json()).id,'synthetic-large-result');assert.equal(seen,1);assert.equal(app.status().jsonParsing.completed,1);assert.equal(app.status().requestBodies.leases,0);assert.equal((await store.get('SELECT input FROM harbor.requests')).input,100);
}));

test('PostgreSQL cancelled administration parsing cannot commit a new employee and replacement parsing remains usable',integration,async t=>withGateway(t,async({store,password,open})=>{
 const {Worker}=await import('node:worker_threads'),state=parserGate();let created=0;const app=await open({jsonParsing:{maxWorkers:1},jsonParserWorker:(url,options)=>new Worker(++created===1?new URL('./json-parser-child.mjs',import.meta.url):url,{...options,workerData:{gate:state,gateName:'合成解析暂停'}})}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),before=(await store.get('SELECT COUNT(*) n FROM harbor.members')).n,controller=new AbortController(),pending=fetch(origin+'/api/admin/members',{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({name:'合成解析暂停',quota:null,expires:null}),signal:controller.signal});pending.catch(()=>{});
 try{await gatewayWait(()=>Atomics.load(state,0)===1);controller.abort();await assert.rejects(pending,{name:'AbortError'});await gatewayWait(()=>app.status().jsonParsing.active===0&&app.status().requestBodies.leases===0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.members')).n,before);const response=await fetch(origin+'/api/admin/members',{method:'POST',headers:{cookie,'content-type':'application/json'},body:JSON.stringify({name:'合成后续员工',quota:null,expires:null})});assert.equal(response.status,201);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.members')).n,before+1);assert.equal(app.status().requestBodies.bufferedBytes,0);
 }finally{controller.abort();releaseParser(state);await pending.catch(()=>{});}
}));

test('R33 PostgreSQL forwards a previously supported deep JSON extension once through the real Responses adapter',integration,async t=>withGateway(t,async({store,member,open})=>{
 const deep='['.repeat(4000)+'0'+']'.repeat(4000),raw='{"model":"gpt-6.1-sol","input":"synthetic","stream":false,"extension":'+deep+',"special":{"__proto__":{"literal":true},"constructor":"value"}}';let seen=0;const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),version:async()=>'0.160.1',fetchImpl:async(_url,options)=>{seen++;const forwarded=JSON.parse(options.body);assert.equal(JSON.stringify(forwarded.extension),deep);assert.equal(Object.hasOwn(forwarded.special,'__proto__'),true);assert.equal(forwarded.special.__proto__.literal,true);assert.equal({}.literal,undefined);return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-deep-response',model:'gpt-6.1-sol',output:[],usage:{input_tokens:100,output_tokens:20}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});}}),app=await open({responsesTransport:transport,recording:{enabled:false}}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const response=await fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:raw});assert.equal(response.status,200);assert.equal((await response.json()).id,'synthetic-deep-response');assert.equal(seen,1);assert.equal(app.status().requestBodies.leases,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,1);await app.close();assert.equal(app.status().jsonParsing.workers,0);
}));

test('R34 PostgreSQL ownership loss terminates a slow upload without waiting for client disconnect or dispatching a model',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0;const app=await open({responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app);await store.run('UPDATE harbor.keys SET expires=NULL WHERE id=$1',member.keyId);const slow=http.request(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','content-length':'1000'}});slow.on('error',()=>{});slow.write('{"input":"');
 try{await gatewayWait(()=>app.status().requestBodies.readers===1);const owner=await store.get("SELECT pid FROM pg_locks WHERE locktype='advisory' AND database=(SELECT oid FROM pg_database WHERE datname=current_database()) AND granted");assert.ok(owner);await store.get('SELECT pg_terminate_backend($1) terminated',owner.pid);await gatewayWait(()=>!app.status().ownership&&app.status().requestBodies.leases===0&&app.status().active===0);assert.equal(app.status().requestBodies.bufferedBytes,0);assert.equal(app.status().requestBodies.readers,0);assert.equal(calls,0);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.requests')).n,0);
 }finally{slow.destroy();}
}));

test('PostgreSQL execution account cap rolls back Responses admission and reopens after completion',integration,async t=>withGateway(t,async({store,member,open})=>{
 let release,calls=0;const gate=new Promise(resolve=>{release=resolve;}),app=await open({execution:{maxPerAccount:1},responsesTransport:controlledResponses(async()=>{calls++;if(calls===1)await gate;})}),origin=await listenGateway(app);
 const send=thread=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':thread},body:JSON.stringify({model:'gpt-6.1-sol',input:'synthetic',stream:false}),signal:AbortSignal.timeout(5000)}),first=send('held-capacity');
 try{await gatewayWait(()=>calls===1);assert.equal(app.status().execution.active,1);const before=await store.get('SELECT (SELECT count(*) FROM harbor.sessions)::int AS sessions,(SELECT count(*) FROM harbor.requests)::int AS requests');const rejected=await send('rejected-capacity');assert.equal(rejected.status,503);assert.match(await rejected.text(),/inference_capacity_busy/);assert.deepEqual(await store.get('SELECT (SELECT count(*) FROM harbor.sessions)::int AS sessions,(SELECT count(*) FROM harbor.requests)::int AS requests'),before);assert.equal(calls,1);release();assert.equal((await first).status,200);await gatewayWait(()=>app.status().execution.active===0);assert.equal((await send('rejected-capacity')).status,200);assert.equal(calls,2);await gatewayWait(()=>app.status().execution.active===0);assert.equal(app.status().execution.accounts,0);
 }finally{release();await first.catch(()=>{});}
}));

test('PostgreSQL execution global CLI cap queues another account and release wakes it exactly once',integration,async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);await store.run("INSERT INTO harbor.accounts(id,name,kind,home,state,token_budget) VALUES('synthetic-claude','synthetic','claude','synthetic','ready',1000000)");const secondKey=await otherHttpChatKey(store,'cli-capacity-second'),held=heldHttpChat(),app=await open({execution:{maxCli:1},executor:held.executor,claudeExecutor:held.executor}),origin=await listenGateway(app),first=sendHttpChat(origin,member.secret,{model:'codex',messages:[{role:'user',content:'first'}]});let second;
 try{await gatewayWait(()=>held.calls.length===1);second=sendHttpChat(origin,secondKey,{model:'claude',messages:[{role:'user',content:'second'}]});await gatewayWait(()=>app.status().chat.queued===1);assert.equal(app.status().execution.cliActive,1);assert.equal(held.calls.length,1);held.gates[0]();assert.equal((await first).status,200);await gatewayWait(()=>held.calls.length===2);assert.equal(app.status().execution.cliActive,1);held.gates[1]();assert.equal((await second).status,200);await gatewayWait(()=>app.status().execution.active===0);assert.equal(held.calls.length,2);assert.equal(app.status().execution.cliPeak,1);assert.equal(app.status().chat.queued,0);
 }finally{for(const release of held.gates)release();await first.catch(()=>{});await second?.catch(()=>{});}
}));

test('PostgreSQL execution permit survives failed admission until rollback and releases without metering',integration,async t=>withGateway(t,async({store,member,open})=>{
 let calls=0,resume,entered=false;const app=await open({execution:{maxPerAccount:1},responsesTransport:controlledResponses(async()=>{calls++;})}),origin=await listenGateway(app),observe=app.store.observe.bind(app.store);app.store.observe=async()=>{entered=true;await new Promise(resolve=>{resume=resolve;});throw Error('synthetic admission failure');};const first=gatewaySend(origin,member.secret);
 try{await gatewayWait(()=>entered);assert.equal(app.status().execution.active,1);resume();assert.equal((await first).status,500);await gatewayWait(()=>app.status().execution.active===0);assert.equal((await store.get('SELECT count(*)::int AS n FROM harbor.requests')).n,0);assert.equal((await store.get('SELECT count(*)::int AS n FROM harbor.sessions')).n,0);assert.equal(calls,0);app.store.observe=observe;assert.equal((await gatewaySend(origin,member.secret)).status,200);assert.equal(calls,1);await gatewayWait(()=>app.status().execution.active===0);
 }finally{resume?.();app.store.observe=observe;await first.catch(()=>{});}
}));

test('PostgreSQL execution Qoder and Chat share CLI capacity without changing account health or unknown usage',integration,async t=>withGateway(t,async({store,member,password,open})=>{
 await prepareHttpChat(store,member);const secondKey=await otherHttpChatKey(store,'qoder-cli-peer'),held=heldHttpChat();let release,qoderCalls=0;const app=await open({execution:{maxCli:1},executor:held.executor,platformTransport:{models:async()=>['synthetic-model'],run:async()=>{qoderCalls++;await new Promise(resolve=>{release=resolve;});return {...platformResult,actualModel:'qoder/synthetic-model'};}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),accountId=await preparePlatformAccount(origin,cookie,'qoder'),body={model:'qoder/synthetic-model',messages:[{role:'user',content:'synthetic'}]},first=platformSend(origin,member.secret,body);let chat;
 try{await gatewayWait(()=>qoderCalls===1);chat=sendHttpChat(origin,secondKey,{messages:[{role:'user',content:'synthetic'}]});await gatewayWait(()=>app.status().chat.queued===1);assert.equal(held.calls.length,0);release();assert.equal((await first).status,200);await gatewayWait(()=>held.calls.length===1);const rejected=await platformSend(origin,member.secret,body);assert.equal(rejected.status,503);assert.match(await rejected.text(),/inference_capacity_busy/);assert.equal(qoderCalls,1);const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform' ORDER BY query_sequence DESC LIMIT 1");assert.equal(row.status,'rejected');assert.equal(row.usage_unknown,0);assert.equal(row.input,0);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',accountId)).state,'ready');held.gates[0]();assert.equal((await chat).status,200);await gatewayWait(()=>app.status().execution.active===0);assert.equal(app.status().execution.cliPeak,1);
 }finally{release?.();for(const done of held.gates)done();await first.catch(()=>{});await chat?.catch(()=>{});}
}));

const cliAdminPost=(origin,cookie,path)=>fetch(origin+'/api/admin/'+path,{method:'POST',headers:{cookie,'content-type':'application/json'},body:'{}'});

test('PostgreSQL real CLI quota waits for close after RPC success and wakes waiting Chat without poisoning login',{...integration,timeout:15000},async t=>withGateway(t,async({store,member,password,open})=>{
 await prepareHttpChat(store,member);let children=0,quotaResponded=false,quotaSettled=false;const processes=[];const app=await open({rateReader:undefined,cliProcesses:{maxConcurrent:1,killGraceMs:5000},cliSpawner:(_binary,args,options)=>{children++;const quota=args[0]!=='exec',processChild=spawn(process.execPath,quota?[new URL('./cli-budget-child.mjs',import.meta.url).pathname,'quota-eof-held']:[new URL('./provider-child.mjs',import.meta.url).pathname,'codex','ok'],quota?{...options,stdio:['pipe','pipe','pipe','ipc']}:options);if(quota)processChild.stdin.once('finish',()=>{quotaResponded=true;});processes.push(processChild);return processChild;}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),call=(path)=>cliAdminPost(origin,cookie,path);
 const quotaCall=call('accounts/synthetic-runtime/quota').finally(()=>{quotaSettled=true;});await gatewayWait(()=>quotaResponded);assert.equal(quotaSettled,false);assert.equal(app.status().cliProcesses.inUse,1);assert.equal(children,1);const before=await store.get("SELECT state,route_epoch,quota_json FROM harbor.accounts WHERE id='synthetic-runtime'");const deniedLogin=await call('accounts/synthetic-runtime/login');assert.equal(deniedLogin.status,503);assert.match(await deniedLogin.text(),/cli_capacity_busy/);assert.deepEqual(await store.get("SELECT state,route_epoch,quota_json FROM harbor.accounts WHERE id='synthetic-runtime'"),before);
 const deniedCheck=await call('accounts/synthetic-runtime/check');assert.equal(deniedCheck.status,503);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'ready');const pending=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'synthetic'}]});await gatewayWait(()=>app.status().chat.requests===1&&app.status().accountChecks.quotas.scans===1);assert.equal(children,1);assert.equal(app.status().chat.queued,0);assert.equal((await store.get("SELECT COUNT(*)::int n FROM harbor.requests WHERE source_channel='chat'")).n,0);processes[0].send('release');const quota=await quotaCall;assert.equal(quota.status,200);assert.equal((await quota.json()).refreshed,true);assert.equal((await pending).status,200);assert.equal(children,2);await gatewayWait(()=>app.status().cliProcesses.inUse===0);assert.equal(app.status().cliProcesses.peak,1);const rows=await store.all("SELECT status FROM harbor.requests WHERE source_channel='chat'");assert.equal(rows.length,1);assert.equal(rows[0].status,'ok');

}));

test('PostgreSQL real CLI login waits for its process close before verification and releases reservations on audit rollback',{...integration,timeout:15000},async t=>withGateway(t,async({store,password,open})=>{
 let children=0;const app=await open({cliProcesses:{maxConcurrent:1,killGraceMs:30},cliSpawner:(_binary,args,options)=>{children++;return spawn(process.execPath,[new URL('./cli-budget-child.mjs',import.meta.url).pathname,args[0]==='login'?'check':'login-complete'],options);}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),call=()=>cliAdminPost(origin,cookie,'accounts/synthetic-runtime/login'),run=app.store.run.bind(app.store);let fail=true;
 app.store.run=async(sql,...args)=>{if(fail&&sql==='INSERT INTO harbor.audit VALUES($1,$2,$3,$4)'){fail=false;throw Error('synthetic login audit rollback');}return run(sql,...args);};const before=await store.get("SELECT state,route_epoch FROM harbor.accounts WHERE id='synthetic-runtime'");assert.equal((await call()).status,500);app.store.run=run;assert.deepEqual(await store.get("SELECT state,route_epoch FROM harbor.accounts WHERE id='synthetic-runtime'"),before);assert.equal(app.status().cliProcesses.inUse,0);assert.equal(children,0);
 assert.equal((await call()).status,200);await gatewayWait(()=>app.status().accountChecks.login.tasks===0);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'ready');assert.equal(children,2);assert.equal(app.status().cliProcesses.inUse,0);assert.equal(app.status().cliProcesses.peak,1);
}));

test('PostgreSQL real CLI Qoder directory shares budget and saturation never marks a connected account offline',{...integration,timeout:15000},async t=>withGateway(t,async({store,member,password,open})=>{
 let children=0,quotaResponded=false,quotaSettled=false;const processes=[];const app=await open({platformTransport:undefined,rateReader:undefined,cliProcesses:{maxConcurrent:1,killGraceMs:5000},cliSpawner:(_binary,args,options)=>{children++;const mode=args.includes('--list-models')?'qoder-models':args.includes('--print')?'qoder-run':'quota-eof-held';const processChild=spawn(process.execPath,[new URL('./cli-budget-child.mjs',import.meta.url).pathname,mode],mode==='quota-eof-held'?{...options,stdio:['pipe','pipe','pipe','ipc']}:options);if(mode==='quota-eof-held')processChild.stdin.once('finish',()=>{quotaResponded=true;});processes.push(processChild);return processChild;}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),accountId=await preparePlatformAccount(origin,cookie,'qoder');await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);await gatewayWait(()=>app.status().cliProcesses.inUse===0);assert.equal(children,1);const quotaCall=cliAdminPost(origin,cookie,'accounts/synthetic-runtime/quota').finally(()=>{quotaSettled=true;});await gatewayWait(()=>quotaResponded);assert.equal(quotaSettled,false);assert.equal(app.status().cliProcesses.inUse,1);const check=await platformAdminRequest(origin,cookie,'accounts/'+accountId+'/check','POST',{});assert.equal(check.status,503);assert.match(await check.text(),/cli_capacity_busy/);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',accountId)).state,'ready');const model='qoder/name-'+Buffer.from('Performance').toString('base64url'),body={model,messages:[{role:'user',content:'synthetic'}]},denied=await platformSend(origin,member.secret,body);assert.equal(denied.status,503);const rejected=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(rejected.status,'rejected');assert.equal(rejected.usage_unknown,0);assert.equal(children,2);processes[1].send('release');const quota=await quotaCall;assert.equal(quota.status,200);assert.equal((await quota.json()).refreshed,true);await gatewayWait(()=>app.status().cliProcesses.inUse===0);const response=await platformSend(origin,member.secret,body);assert.equal(response.status,200);assert.equal((await response.json()).choices[0].message.content,'synthetic Qoder result');assert.equal(children,3);await gatewayWait(()=>app.status().cliProcesses.inUse===0);
}));

test('PostgreSQL real CLI credential refresh is shared across parallel Responses and shutdown waits for its late process exit',{...integration,timeout:15000},async t=>withGateway(t,async({store,member,root,open})=>{
 const home=join(root,'runtime','accounts','synthetic-runtime');mkdirSync(home,{recursive:true});const token='synthetic.'+Buffer.from(JSON.stringify({exp:0})).toString('base64url')+'.signature';writeFileSync(join(home,'auth.json'),JSON.stringify({tokens:{access_token:token,account_id:'synthetic-account'}}),{mode:0o600});let children=0,calls=0;const processes=[],app=await open({responsesTransport:undefined,cliProcesses:{maxConcurrent:1,killGraceMs:40},cliSpawner:(_binary,_args,options)=>{children++;const processChild=spawn(process.execPath,[new URL('./cli-budget-child.mjs',import.meta.url).pathname,'refresh-held'],{...options,stdio:['pipe','pipe','pipe','ipc']});processes.push(processChild);return processChild;},responsesFetch:async()=>{calls++;return new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic',model:'gpt-6.1-sol',output:[],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:10}}}})+'\n\n',{headers:{'content-type':'text/event-stream'}});}}),origin=await listenGateway(app);
 const pending=Promise.all([gatewaySend(origin,member.secret),gatewaySend(origin,member.secret)]);await gatewayWait(async()=>children===1&&(await store.get("SELECT count(*)::int AS n FROM harbor.requests WHERE status='queued'")).n===2);assert.equal(calls,0);assert.equal((await store.get("SELECT count(*)::int AS n FROM harbor.requests WHERE status='running'")).n,0);processes[0].send('release');const replies=await pending;for(const reply of replies)assert.equal(reply.status,200);assert.equal(children,1);assert.equal(calls,2);assert.equal(app.status().cliProcesses.peak,1);await app.close();assert.equal(app.status().cliProcesses.inUse,0);assert.equal(app.status().cliProcesses.running,0);assert.notEqual(processes[0].exitCode===null&&processes[0].signalCode===null,true);assert.equal((await store.get('SELECT count(*)::int AS n FROM harbor.requests')).n,2);
}));

test('PostgreSQL real CLI forced drain kills an ignored-TERM model and retains unknown billing without restarting it',{...integration,timeout:15000},async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);let children=0,ready=false;const app=await open({drainTimeoutMs:100,cliProcesses:{killGraceMs:30},cliSpawner:(_binary,_args,options)=>{children++;const processChild=spawn(process.execPath,['-e',"process.on('SIGTERM',()=>{});process.stdout.write('ready');setInterval(()=>{},1000)"],options);processChild.stdout.once('data',()=>{ready=true;});return processChild;}}),origin=await listenGateway(app),pending=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'synthetic'}]});pending.catch(()=>{});await gatewayWait(()=>ready&&app.status().cliProcesses.running===1);const result=await app.close();assert.equal(result.forced,true);await pending.catch(()=>{});assert.equal(app.status().cliProcesses.inUse,0);assert.equal(app.status().execution.active,0);const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='chat'");assert.equal(row.status,'interrupted');assert.equal(row.usage_unknown,1);assert.ok(row.reserved>0);await open();assert.equal(children,1);
}));

test('PostgreSQL real CLI graceful drain completes the accepted process before closing its budget',{...integration,timeout:15000},async t=>withGateway(t,async({store,member,open})=>{
 await prepareHttpChat(store,member);let child,ready=false;const source="process.on('message',()=>{console.log(JSON.stringify({type:'thread.started',thread_id:'synthetic-drain'}));console.log(JSON.stringify({type:'item.completed',item:{type:'agent_message',text:'synthetic reply'}}));console.log(JSON.stringify({type:'turn.completed',usage:{input_tokens:10,output_tokens:5,cached_input_tokens:1}}));process.exit(0);});console.log(JSON.stringify({type:'thread.started',thread_id:'synthetic-drain'}));";const app=await open({drainTimeoutMs:5000,cliProcesses:{killGraceMs:30},cliSpawner:(_binary,_args,options)=>{child=spawn(process.execPath,['-e',source],{...options,stdio:['pipe','pipe','pipe','ipc']});child.stdout.once('data',()=>{ready=true;});return child;}}),origin=await listenGateway(app),pending=sendHttpChat(origin,member.secret,{messages:[{role:'user',content:'synthetic'}]});await gatewayWait(()=>ready);const closing=app.close();assert.equal(app.status().cliProcesses.running,1);child.send('complete');const response=await pending;assert.equal(response.status,200);const closed=await closing;assert.equal(closed.forced,false);assert.equal(app.status().cliProcesses.inUse,0);assert.equal(app.status().execution.active,0);assert.equal((await store.get("SELECT status FROM harbor.requests WHERE source_channel='chat'")).status,'ok');
}));

test('PostgreSQL shared output bytes isolate employee Keys, cancel overload without replay and recover capacity', {...integration,timeout:15000},async t=>withGateway(t,async({store,member,open})=>{
 const peer=await otherHttpChatKey(store,'output-peer'),secondKey='synthetic-output-key-two';await store.run('INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES($1,$2,$3,$4,0)','output-key-two',member.memberId,hash(secondKey),'synthetic');let hold,calls=0,cancelled=0;const terminal='data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-output',model:'gpt-6.1-sol',output:[],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:10}}}})+'\n\n',delta=size=>'data: '+JSON.stringify({type:'response.output_text.delta',item_id:'synthetic-output',content_index:0,delta:'x'.repeat(size)})+'\n\n';
 const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),fetchImpl:async(_url,options)=>{calls++;const payload=JSON.parse(options.body);if(payload.input==='hold')return new Response(new ReadableStream({start(controller){hold=controller;controller.enqueue(Buffer.from(delta(1850).slice(0,-2)));},cancel(){cancelled++;}}),{headers:{'content-type':'text/event-stream'}});let index=0;const chunks=[delta(2200),terminal];return new Response(new ReadableStream({pull(controller){if(index<chunks.length)controller.enqueue(Buffer.from(chunks[index++]));else controller.close();},cancel(){cancelled++;}}),{headers:{'content-type':'text/event-stream'}});}}),app=await open({responsesTransport:transport,outputBuffers:{maxBufferedBytes:64000,maxBytesPerOwner:30000}}),origin=await listenGateway(app),send=(secret,input)=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',input,stream:false}),signal:AbortSignal.timeout(5000)}),first=send(member.secret,'hold');
 try{await gatewayWait(()=>hold&&app.status().outputBuffers.bufferedBytes>1000);const rejected=await send(secondKey,'large');assert.equal(rejected.status,503);assert.match(await rejected.text(),/upstream_buffer_busy/);assert.equal(calls,2);assert.equal(cancelled,1);const failed=await store.get("SELECT * FROM harbor.requests ORDER BY query_sequence DESC LIMIT 1");assert.equal(failed.status,'failed');assert.equal(failed.usage_unknown,1);assert.equal((await store.get("SELECT state FROM harbor.accounts WHERE id='synthetic-runtime'")).state,'ready');const other=await send(peer,'large');assert.equal(other.status,200);await other.json();assert.equal(calls,3);hold.enqueue(Buffer.from('\n\n'+terminal));hold.close();assert.equal((await first).status,200);await gatewayWait(()=>app.status().outputBuffers.scopes===0);assert.equal(app.status().outputBuffers.bufferedBytes,0);assert.equal((await send(member.secret,'large')).status,200);assert.equal(calls,4);await gatewayWait(()=>app.status().outputBuffers.scopes===0);assert.equal(app.status().execution.active,0);
 }finally{try{hold?.enqueue(Buffer.from('\n\n'+terminal));hold?.close();}catch{}await first.catch(()=>{});}
}));

test('PostgreSQL output bytes remain reserved during real socket backpressure and cancellation releases the source', {...integration,timeout:15000},async t=>withGateway(t,async({store,member,open})=>{
 let socket,gate,rawClient,calls=0,cancelled=0;const delta={type:'response.output_text.delta',item_id:'synthetic-socket',content_index:0,delta:'x'.repeat(128*1024)},terminal={type:'response.completed',response:{id:'synthetic-socket',model:'gpt-6.1-sol',output:[],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:10}}}};
 const transport=createResponsesTransport({credentials:async()=>({token:'synthetic',accountId:'synthetic'}),fetchImpl:async()=>{calls++;let index=0;const chunks=[delta,terminal].map(value=>Buffer.from('data: '+JSON.stringify(value)+'\n\n'));return new Response(new ReadableStream({pull(controller){if(index<chunks.length)controller.enqueue(chunks[index++]);else controller.close();},cancel(){cancelled++;}}),{headers:{'content-type':'text/event-stream'}});}}),app=await open({responsesTransport:transport,outputBuffers:{maxBufferedBytes:4*1024*1024}}),origin=await listenGateway(app);app.server.prependListener('request',(req,res)=>{if(req.url==='/v1/responses'){socket=res.socket;const current={socket,uncork:socket.uncork,released:false};gate=current;socket.cork();socket.uncork=function(){if(current.released)return current.uncork.call(this);};}});const uncork=()=>{if(!gate)return;gate.released=true;gate.socket.uncork=gate.uncork;while(gate.socket.writableCorked)gate.uncork.call(gate.socket);},send=signal=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json'},body:JSON.stringify({model:'gpt-6.1-sol',input:'synthetic',stream:true}),signal});
 let pending=send(AbortSignal.timeout(5000));try{await gatewayWait(()=>socket?.writableLength>100000);assert.ok(app.status().outputBuffers.bufferedBytes>128*1024);assert.equal((await store.get('SELECT status FROM harbor.requests')).status,'running');assert.equal((await fetch(origin+'/health')).status,200);uncork();const response=await pending;assert.equal(response.status,200);assert.match(await response.text(),/response.completed/);await gatewayWait(()=>app.status().outputBuffers.scopes===0);assert.equal(app.status().outputBuffers.bufferedBytes,0);assert.equal(calls,1);
 const {createConnection}=await import('node:net');socket=undefined;rawClient=createConnection({host:'127.0.0.1',port:new URL(origin).port});rawClient.on('error',()=>{});await new Promise(resolve=>rawClient.once('connect',resolve));const rawBody=JSON.stringify({model:'gpt-6.1-sol',input:'synthetic',stream:true});rawClient.write('POST /v1/responses HTTP/1.1\r\nHost: '+new URL(origin).host+'\r\nAuthorization: Bearer '+member.secret+'\r\nContent-Type: application/json\r\nContent-Length: '+Buffer.byteLength(rawBody)+'\r\n\r\n'+rawBody);await gatewayWait(()=>socket?.writableLength>100000);rawClient.resetAndDestroy();await gatewayWait(()=>app.status().outputBuffers.scopes===0&&app.status().active===0);assert.equal(app.status().outputBuffers.bufferedBytes,0);assert.equal(calls,2);const interrupted=await store.get('SELECT * FROM harbor.requests ORDER BY query_sequence DESC LIMIT 1');assert.equal(interrupted.status,'cancelled');assert.equal(interrupted.usage_unknown,1);assert.ok(cancelled>=1);
 }finally{rawClient?.destroy();uncork();await pending.catch(()=>{});}
}));

test('PostgreSQL oversized real subscription status is a resource error and never clears a connected account', {...integration,timeout:15000},async t=>withGateway(t,async({store,password,open})=>{
 const app=await open({cliProcesses:{killGraceMs:20},cliSpawner:(_binary,_args,options)=>spawn(process.execPath,[new URL('./provider-child.mjs',import.meta.url).pathname,'codex','oversize'],options)}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),before=await store.get("SELECT state,route_epoch,quota_json FROM harbor.accounts WHERE id='synthetic-runtime'");const response=await cliAdminPost(origin,cookie,'accounts/synthetic-runtime/check');assert.equal(response.status,502);assert.match(await response.text(),/upstream_response_too_large/);assert.deepEqual(await store.get("SELECT state,route_epoch,quota_json FROM harbor.accounts WHERE id='synthetic-runtime'"),before);await gatewayWait(()=>app.status().cliProcesses.inUse===0);assert.equal(app.status().outputBuffers.scopes,0);
}));

for(const measured of [false,true])test('R35 PostgreSQL WorkBuddy output overload preserves '+(measured?'final known':'unknown')+' usage without replay', {...integration,timeout:15000},async t=>withGateway(t,async({store,member,password,open})=>{
 const {createPlatformTransport}=await import('../platforms.mjs');let holder,release,calls=0,cancelled=0;const gate=new Promise(resolve=>{release=resolve;}),parser=createPlatformTransport({fetchImpl:async()=>{calls++;let index=0;const event={model:'actual-model',choices:[{index:0,delta:{content:'ok'},...(measured?{finish_reason:'stop'}:{})}],...(measured?{usage:{prompt_tokens:100,completion_tokens:20,prompt_tokens_details:{cached_tokens:30}}}:{})};return new Response(new ReadableStream({pull(controller){if(index===1)holder.resize('pressure',4000);if(index===0){index++;controller.enqueue(Buffer.from('data: '+JSON.stringify(event)+'\n\n'));}else if(index===1){index++;controller.enqueue(Buffer.from('data: [DONE]\n\n'));}else controller.close();},cancel(){cancelled++;}},{highWaterMark:0}));}}),app=await open({outputBuffers:{maxBufferedBytes:4096},responsesTransport:{run:async({output})=>{holder=output;await gate;return {input:1,output:1,cached:0,usageKnown:true,response:{id:'synthetic-holder',output:[]}};}},platformTransport:{...parser,models:async()=>['synthetic-model'],login:async({onChallenge})=>{onChallenge({type:'browser',verificationUrl:'https://copilot.tencent.com/synthetic'});return {accessToken:'synthetic-access',refreshToken:'synthetic-refresh',expiresAt:Date.now()+3600000};}}}),origin=await listenGateway(app),cookie=await gatewayLogin(origin,password),accountId=await preparePlatformAccount(origin,cookie,'workbuddy');assert.equal((await platformAdminRequest(origin,cookie,'accounts/'+accountId+'/login','POST',{})).status,200);await gatewayWait(()=>app.status().accountChecks.platform.tasks===0);await store.run('UPDATE harbor.keys SET allowed_models=NULL,expires=NULL WHERE id=$1',member.keyId);const peer=await otherHttpChatKey(store,'r35-peer'),holding=gatewaySend(origin,peer);
 try{await gatewayWait(()=>!!holder);const response=await platformSend(origin,member.secret,{model:'workbuddy/synthetic-model',messages:[{role:'user',content:'synthetic'}]});assert.equal(response.status,503);assert.match(await response.text(),/upstream_buffer_busy/);const row=await store.get("SELECT * FROM harbor.requests WHERE source_channel='platform'");assert.equal(row.status,measured?'failed':'unknown');assert.equal(row.usage_unknown,measured?0:1);assert.equal(row.input,measured?100:0);assert.equal(row.output,measured?20:0);assert.equal(row.cached,measured?30:0);if(measured)assert.equal(row.billing_model,'workbuddy/actual-model');assert.equal(calls,1);assert.equal(cancelled,1);assert.equal((await store.get('SELECT state FROM harbor.accounts WHERE id=$1',accountId)).state,'ready');assert.equal(app.status().outputBuffers.bufferedBytes,4000);holder.resize('pressure',0);release();assert.equal((await holding).status,200);await gatewayWait(()=>app.status().outputBuffers.scopes===0&&app.status().active===0);assert.equal(app.status().outputBuffers.bufferedBytes,0);
 }finally{try{holder?.resize('pressure',0);}catch{}release();await holding.catch(()=>{});}
}));
