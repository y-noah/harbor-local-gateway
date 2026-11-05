import test from 'node:test';
import assert from 'node:assert/strict';
import {randomUUID,randomBytes} from 'node:crypto';
import {mkdtempSync,rmSync,readFileSync,mkdirSync,writeFileSync,readdirSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {PostgresStore} from '../postgres-store.mjs';
import {Store,hash} from '../store.mjs';
import {passwordHash} from '../auth.mjs';
import {createResponsesTransport} from '../responses.mjs';
import {createPostgresGateway} from '../postgres-gateway.mjs';
import {createPostgresWorker} from '../postgres-worker.mjs';
import {createPostgresNightly} from '../postgres-nightly.mjs';
import {transferWindow} from '../transfer-schedule.mjs';
import {createPostgresPlanningInput} from '../postgres-planning-input.mjs';
import {createSessionRequestHandoff} from '../session-request-handoff.mjs';

const url=process.env.HARBOR_TEST_DATABASE_URL,socket=process.env.HARBOR_TEST_PG_SOCKET,integration={skip:!url&&!socket?'需隔离PG18；CI test_postgres执行真实链路':false};
function databaseOptions(name='postgres'){
  if(url){const address=new URL(url);address.pathname='/'+name;return {connectionString:address.toString(),allowPlaintextLocalNetwork:true};}
  return {connection:{host:socket,port:Number(process.env.HARBOR_TEST_PG_PORT||55461),user:process.env.HARBOR_TEST_PG_USER||'harbor_scale',database:name}};
}
const until=async(predicate,timeout=10000)=>{const deadline=Date.now()+timeout;while(!await predicate()){assert.ok(Date.now()<deadline,'automatic handoff condition timeout');await new Promise(resolve=>setTimeout(resolve,10));}};
async function fixture(t,run){
  const admin=new PostgresStore(databaseOptions()),name='harbor_test_'+randomUUID().replaceAll('-',''),root=mkdtempSync(join(tmpdir(),'harbor-auto-group-'));let store,legacy,app,worker,nightly,releaseOld;const calls=[];
  try{
    await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");store=new PostgresStore(databaseOptions(name));await store.migrate();if(!(await store.get("SELECT to_regclass('harbor.automatic_handoff_groups') present")).present)await store.query(readFileSync(new URL('../migrations/016-automatic-handoffs.sql',import.meta.url),'utf8'));await store.initializePricing();
    const at=Date.parse('2026-11-03T10:00:01+08:00'),now={at};legacy=new Store(join(root,'legacy.sqlite'),{encryptionKey:randomBytes(32)});const member=legacy.createMember('自动交接合成员工',null,null,['gpt-6.1-sol']);
    for(const [table,key] of [['members',member.memberId],['keys',member.keyId]]){const row=legacy.get('SELECT * FROM '+table+' WHERE id=?',key),columns=Object.keys(row);await store.run('INSERT INTO harbor.'+table+'('+columns.join(',')+') VALUES('+columns.map((_,index)=>'$'+(index+1)).join(',')+')',...columns.map(column=>row[column]));}
    const quota=JSON.stringify({plan:'synthetic-auto-plan',checked:at,limitReached:false,windows:[{minutes:10080,used:50,reset:at+7*86400000}]});
    for(const account of ['a','b','c'])await store.run("INSERT INTO harbor.accounts(id,name,kind,auth_mode,home,state,quota_json) VALUES($1,$2,'codex','subscription',$3,'ready',$4)",account,'合成账号'+account,join(root,'accounts',account),quota);
    await store.run("UPDATE harbor.accounts SET auth_mode='openai_api',quota_json=NULL WHERE id='c'");
    for(const [index,input] of [6000,4000,4000,2000].entries()){
      const session='auto-session-'+index,account=index<2?'a':'b';await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated,client_key,upstream_thread) VALUES($1,$2,$3,'responses',$4,$4,$5,$1)",session,member.memberId,account,at-2*3600000,hash('auto-thread-'+index));
      const person=await store.authenticate(member.secret,{at}),request=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-3600000});await store.run("UPDATE harbor.requests SET session_id=$1,account_id=$2,route_version=0,handoff_context='replayable',handoff_input='replayable' WHERE id=$3",session,account,request);await store.finish(request,{input,output:0,cached:0},at-3600000+10);
    }
    await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('balancing',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",JSON.stringify({nightly:true,failover:false,quietMinutes:30}));
    mkdirSync(join(root,'data'),{mode:0o700});writeFileSync(join(root,'data','key-encryption.key'),legacy.encryptionKey,{mode:0o600});
    const old=new Promise(resolve=>{releaseOld=resolve;}),held={enabled:false},modelChecks={allowed:true,denied:new Set(),opaque:false,blocked:new Map()};
    const transport=createResponsesTransport({version:async()=>'0.160.1',credentials:async()=>({token:'synthetic-auto-upstream',accountId:'synthetic-auto-account'}),fetchImpl:async()=>new Response('data: '+JSON.stringify({type:'response.completed',response:{id:'synthetic-auto-response',model:'gpt-6.1-sol',output:[{type:'message',role:'assistant',content:[{type:'output_text',text:'合成响应'}]},...(modelChecks.opaque?[{type:'reasoning',encrypted_content:'synthetic-opaque'}]:[])],usage:{input_tokens:100,output_tokens:20,input_tokens_details:{cached_tokens:0}}}})+'\n\n',{headers:{'content-type':'text/event-stream'}})});
    const controlled={...transport,models:async({home,signal})=>{const key=home.split('/').at(-1),wait=modelChecks.blocked.get(key);if(wait)await Promise.race([wait,new Promise((resolve,reject)=>{if(signal?.aborted)reject(signal.reason);else signal?.addEventListener('abort',()=>reject(signal.reason),{once:true});})]);return modelChecks.allowed&&!modelChecks.denied.has(key)?[{slug:'gpt-6.1-sol'}]:[];},run:async options=>{calls.push({home:options.home,thread:options.payload.prompt_cache_key});if(held.enabled&&options.payload.prompt_cache_key==='auto-session-0')await old;return transport.run(options);}};
    app=await createPostgresGateway({database:databaseOptions(name),dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{port:43127,codexBinary:'codex',adminPasswordHash:passwordHash('synthetic-auto-password')},clock:()=>now.at,env:{},responsesTransport:controlled,apiResponsesTransport:controlled,rateReader:async()=>JSON.parse(quota),drainTimeoutMs:500});await app.nightly.close();
    assert.ok(app.handoffAdmin,'gateway exposes the same internal manual service for integration');const newNightly=()=>createPostgresNightly(app.store,{preparation:app.preparation,sessionHandoffs:app.sessionHandoffs,handoffAdmin:app.handoffAdmin,clock:()=>now.at,refreshQuotas:async()=>{}});nightly=newNightly();
    await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+app.server.address().port,login=await fetch(origin+'/api/auth/login',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({password:'synthetic-auto-password'})});assert.equal(login.status,200);const cookie=login.headers.get('set-cookie').split(';')[0];
    const adminCall=async(path,method='GET',body)=>{const response=await fetch(origin+'/api/admin/'+path,{method,headers:{cookie,'content-type':'application/json'},...(body?{body:JSON.stringify(body)}:{})});return {status:response.status,body:await response.json()};};
    const send=(index=0)=>fetch(origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+member.secret,'content-type':'application/json','thread-id':'auto-thread-'+index},body:JSON.stringify({model:'gpt-6.1-sol',input:'合成完整上下文',stream:false})}).then(async response=>({status:response.status,body:await response.json()}));
    worker=await createPostgresWorker({database:databaseOptions(name),clock:()=>now.at,runtime:{pollMs:100}});
    const plan=async()=>{const slot=transferWindow(now.at);await store.run('INSERT INTO harbor.nightly_cycle_ready VALUES($1,$2) ON CONFLICT(day) DO UPDATE SET at=EXCLUDED.at',slot.key,now.at);let selected;await until(async()=>{await worker.runOnce();const revision=await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE slot=$1 ORDER BY revision DESC LIMIT 1',slot.key);selected=revision?.job_id??'routing.nightly:'+slot.key;return (await store.get("SELECT status FROM harbor.background_jobs WHERE id=$1",selected))?.status==='completed';});return selected;};
    await run({store,app,worker,nightly,name,now,at,member,calls,held,modelChecks,releaseOld,send,adminCall,plan,newNightly,replaceNightly:value=>{nightly=value;}});
  }finally{releaseOld?.();await nightly?.close();await worker?.close();await app?.close();legacy?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.close();rmSync(root,{recursive:true,force:true});}
}

test('automatic plan considers an active real HTTP request and applies one durable atomic group before its next model call',integration,async t=>fixture(t,async({store,app,nightly,held,releaseOld,send,calls,plan})=>{
  held.enabled=true;const first=send();let application,next;try{
    await until(()=>calls.length===1);const planned=await plan(),header=await store.get('SELECT header FROM harbor.routing_plans WHERE id=$1',planned),moves=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' ORDER BY ordinal",planned)).map(row=>row.payload);assert.equal(header.header.requestBoundary,true);assert.ok(moves.some(move=>move.session_id==='auto-session-0'));
    application=nightly.apply(planned);await until(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.session_handoffs WHERE origin='automatic' AND status='waiting'")).n===moves.length);assert.equal(JSON.parse((await store.get('SELECT summary FROM harbor.balance_runs')).summary).waiting,moves.length);let returned=false;next=send().then(value=>{returned=true;return value;});await until(()=>app.sessionHandoffs.status().waiting===1);assert.equal(returned,false);assert.equal(calls.length,1);assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'a');
    releaseOld();assert.equal((await first).status,200);await application;assert.equal((await next).status,200);await until(()=>app.status().active===0);
    const actual=await store.all('SELECT id,account_id,route_version,handoff_requires_replay FROM harbor.sessions ORDER BY id');for(const move of moves){const row=actual.find(row=>row.id===move.session_id);assert.equal(row.account_id,move.to);assert.equal(row.route_version,move.version+1);assert.equal(row.handoff_requires_replay,1);}
    const requests=await store.all("SELECT account_id,route_version FROM harbor.requests WHERE session_id='auto-session-0' ORDER BY query_sequence DESC LIMIT 2");assert.deepEqual(requests,[{account_id:'b',route_version:1},{account_id:'a',route_version:0}]);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.session_handoffs WHERE origin='automatic' AND status='completed'")).n,moves.length);assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'completed');
    await nightly.apply(planned);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,moves.length);assert.equal(calls.length,2);
  }finally{releaseOld();await first;await application?.catch(()=>{});await next?.catch(()=>{});}
}));

test('a rejected manual HTTP registration restores a still-draining automatic group and its original waiters',integration,async t=>fixture(t,async({store,app,nightly,held,releaseOld,send,calls,plan,adminCall})=>{
  held.enabled=true;const first=send();let application,next;try{
    await until(()=>calls.length===1);const planned=await plan();application=nightly.apply(planned);await until(async()=>!!await store.get("SELECT id FROM harbor.session_handoffs WHERE session_id='auto-session-0' AND status='waiting'"));
    const original=await store.get("SELECT id FROM harbor.session_handoffs WHERE session_id='auto-session-0' AND status='waiting'");next=send();await until(()=>app.sessionHandoffs.status().waiting===1);
    const rejected=await adminCall('sessions/auto-session-0/handoffs','POST',{account_id:'missing-account',expected_route_version:0,replace_handoff_id:original.id});assert.equal(rejected.status,400);assert.equal((await store.get('SELECT status FROM harbor.session_handoffs WHERE id=$1',original.id)).status,'waiting');assert.equal(app.sessionHandoffs.status().waiting,1);
    releaseOld();assert.equal((await first).status,200);await application;assert.equal((await next).status,200);assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'b');assert.equal(calls.length,2);
  }finally{releaseOld();await first;await application?.catch(()=>{});await next?.catch(()=>{});}
}));

test('an accepted manual HTTP takeover failing model validation restores and completes the original automatic group',integration,async t=>fixture(t,async({store,app,nightly,held,releaseOld,send,calls,plan,adminCall,modelChecks})=>{
  held.enabled=true;const first=send();let application,next;try{
    await until(()=>calls.length===1);const planned=await plan();application=nightly.apply(planned);await until(async()=>!!await store.get("SELECT id FROM harbor.session_handoffs WHERE session_id='auto-session-0' AND status='waiting'"));
    modelChecks.denied.add('c');const accepted=await adminCall('sessions/auto-session-0/handoffs','POST',{account_id:'c',expected_route_version:0});assert.equal(accepted.status,202);next=send();await until(()=>app.sessionHandoffs.status().waiting===1);
    releaseOld();assert.equal((await first).status,200);await application;assert.equal((await next).status,200);const manual=await store.get("SELECT status,reason FROM harbor.session_handoffs WHERE origin='manual'");assert.equal(manual.status,'failed');assert.equal(manual.reason,'session_model_unsupported');assert.equal((await store.get("SELECT status FROM harbor.automatic_handoff_groups")).status,'completed');assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'b');assert.equal(calls.length,2);
  }finally{releaseOld();await first;await application?.catch(()=>{});await next?.catch(()=>{});}
}));

test('a successful manual HTTP takeover wins over the pending automatic group without moving its other sessions',integration,async t=>fixture(t,async({store,app,nightly,held,releaseOld,send,calls,plan,adminCall})=>{
  held.enabled=true;const first=send();let application,next;try{
    await until(()=>calls.length===1);const planned=await plan();application=nightly.apply(planned);await until(async()=>!!await store.get("SELECT id FROM harbor.session_handoffs WHERE session_id='auto-session-0' AND status='waiting'"));
    const accepted=await adminCall('sessions/auto-session-0/handoffs','POST',{account_id:'c',expected_route_version:0});assert.equal(accepted.status,202);next=send();await until(()=>app.sessionHandoffs.status().waiting===1);releaseOld();assert.equal((await first).status,200);assert.equal((await next).status,200);await application;
    assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'c');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.sessions WHERE id<>'auto-session-0' AND route_version<>0")).n,0);assert.equal((await store.get("SELECT status FROM harbor.automatic_handoff_groups")).status,'superseded');assert.equal((await store.get("SELECT status FROM harbor.session_handoffs WHERE origin='manual'")).status,'completed');assert.equal(calls.length,2);
  }finally{releaseOld();await first;await application?.catch(()=>{});await next?.catch(()=>{});}
}));

for(const boundary of ['opaque','quota','disabled'])test('automatic HTTP handoff preserves the original route when '+boundary+' changes while its old request runs',integration,async t=>fixture(t,async({store,app,nightly,held,releaseOld,send,calls,plan,modelChecks})=>{
  held.enabled=true;const first=send();let application,next;try{
    await until(()=>calls.length===1);const planned=await plan();application=nightly.apply(planned);await until(async()=>!!await store.get("SELECT id FROM harbor.session_handoffs WHERE origin='automatic' AND status='waiting'"));next=send();await until(()=>app.sessionHandoffs.status().waiting===1);
    if(boundary==='opaque')modelChecks.opaque=true;else if(boundary==='quota')await store.run("UPDATE harbor.accounts SET quota_json=jsonb_set(quota_json::jsonb,'{windows,0,used}','100'::jsonb)::text WHERE id='b'");else await store.run("UPDATE harbor.gateway_settings SET value='{\"nightly\":false}' WHERE name='balancing'");
    if(boundary==='disabled')await nightly.runOnce();releaseOld();const initial=await first;assert.equal(initial.status,200);if(boundary==='opaque')assert.ok(initial.body.output.some(item=>item.encrypted_content==='synthetic-opaque'));await application;assert.equal((await next).status,200);assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'a');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);assert.equal(calls.length,2);
  }finally{releaseOld();await first;await application?.catch(()=>{});await next?.catch(()=>{});}
}));


test('a gateway coordinator restart cancels old durable decisions and recomputes a distinct immutable current-slot plan',integration,async t=>fixture(t,async({store,nightly,newNightly,replaceNightly,held,releaseOld,send,calls,plan})=>{
  held.enabled=true;const first=send();let oldApply,application,next,recovered;
  try{
    await until(()=>calls.length===1);const original=await plan();oldApply=nightly.apply(original);oldApply.catch(()=>{});await until(async()=>!!await store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE status='waiting'"));
    await nightly.close();await oldApply.catch(()=>{});assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'waiting');
    recovered=newNightly();replaceNightly(recovered);await recovered.recover();assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'cancelled');
    const revision=await plan();assert.notEqual(revision,original);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,2);application=recovered.apply(revision);await until(async()=>!!await store.get("SELECT id FROM harbor.session_handoffs WHERE origin='automatic' AND status='waiting'"));next=send();await until(()=>calls.length===1&&recovered.status().automatic.pendingGroups===1);
    releaseOld();assert.equal((await first).status,200);await application;assert.equal((await next).status,200);assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'b');
    const summary=JSON.parse((await store.get('SELECT summary FROM harbor.balance_runs')).summary);assert.equal(summary.revisions.length,2);assert.equal(summary.revisions[0].status,'interrupted');assert.equal(summary.revisions[0].applied,0);assert.equal(summary.revisions[1].status,'completed');assert.equal(summary.applied,summary.revisions[1].applied);
    await recovered.apply(revision);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,summary.applied);
  }finally{releaseOld();await first;await oldApply?.catch(()=>{});await application?.catch(()=>{});await next?.catch(()=>{});}
}));

test('an unstarted expired plan is replaced within its current slot using fresh workload instead of replaying the saved plan',integration,async t=>fixture(t,async({store,nightly,now,member,plan})=>{
  const original=await plan(),oldMoves=await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves'",original);assert.ok(oldMoves.length>0);
  now.at+=3600001;await store.run("UPDATE harbor.accounts SET quota_json=jsonb_set(quota_json::jsonb,'{checked}',to_jsonb($1::bigint))::text",now.at);
  const identity=await store.authenticate(member.secret,{at:now.at}),extra=await store.observe(identity,'fixture','fixture','gpt-6.1-sol',{at:now.at-100});await store.run("UPDATE harbor.requests SET session_id='auto-session-2',account_id='b',route_version=0,handoff_context='replayable',handoff_input='replayable' WHERE id=$1",extra);await store.finish(extra,{input:24000,output:0,cached:0},now.at-50);
  await nightly.runOnce();const revision=await plan();assert.notEqual(revision,original);const newMoves=await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves'",revision);assert.ok(newMoves.some(({payload})=>payload.from==='b'&&payload.to==='a'),'fresh high load on b reverses the plan direction');await nightly.apply(revision);
  assert.deepEqual(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves'",original),oldMoves);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.routing_plans')).n,2);
}));

test('a partial slot restart preserves already committed groups and fixes their moved sessions in the fresh revision and preview',integration,async t=>fixture(t,async({store,app,nightly,newNightly,replaceNightly,held,releaseOld,send,calls,member,at,plan})=>{
  const quota=JSON.parse((await store.get("SELECT quota_json FROM harbor.accounts WHERE id='a'")).quota_json);quota.plan='synthetic-second-plan';
  for(const account of ['d','e'])await store.run("INSERT INTO harbor.accounts(id,name,kind,auth_mode,home,state,quota_json) SELECT $1,$1,'codex','subscription',home||$1,'ready',$2 FROM harbor.accounts WHERE id='a'",account,JSON.stringify(quota));
  for(const [index,input] of [6000,4000,4000,2000].entries()){
    const session='second-session-'+index,account=index<2?'d':'e';await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated,client_key,upstream_thread) VALUES($1,$2,$3,'responses',$4,$4,$1,$1)",session,member.memberId,account,at-7200000);
    const person=await store.authenticate(member.secret,{at}),request=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-3600000});await store.run("UPDATE harbor.requests SET session_id=$1,account_id=$2,route_version=0,handoff_context='replayable',handoff_input='replayable' WHERE id=$3",session,account,request);await store.finish(request,{input,output:0,cached:0},at-3599990);
  }
  held.enabled=true;const first=send();let oldApply,newApply,recovered;
  try{
    await until(()=>calls.length===1);const original=await plan();oldApply=nightly.apply(original);oldApply.catch(()=>{});await until(async()=> (await store.get("SELECT COUNT(*) n FROM harbor.automatic_handoff_groups WHERE status='completed'")).n===1&&(await store.get("SELECT COUNT(*) n FROM harbor.automatic_handoff_groups WHERE status='waiting'")).n===1);
    const committed=await store.all("SELECT session_id,applied_route_version FROM harbor.session_handoffs WHERE origin='automatic' AND status='completed'");assert.ok(committed.length>0);await nightly.close();await oldApply.catch(()=>{});recovered=newNightly();replaceNightly(recovered);await recovered.recover();const revision=await plan();
    const moves=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves'",revision)).map(row=>row.payload);for(const old of committed)assert.equal(moves.some(move=>move.session_id===old.session_id),false);
    const snapshots=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='sessions'",revision)).map(row=>row.payload);for(const old of committed)assert.equal(snapshots.find(row=>row.id===old.session_id).transferredInSlot,true);
    newApply=recovered.apply(revision);releaseOld();assert.equal((await first).status,200);await newApply;for(const old of committed)assert.equal((await store.get('SELECT route_version FROM harbor.sessions WHERE id=$1',old.session_id)).route_version,old.applied_route_version);
    const summary=JSON.parse((await store.get('SELECT summary FROM harbor.balance_runs')).summary);assert.equal(summary.applied,(await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n);assert.ok(summary.revisions[0].applied>0);assert.equal(summary.revisions.length,2);assert.equal(app.status().active,0);
  }finally{releaseOld();await first;await oldApply?.catch(()=>{});await newApply?.catch(()=>{});}
}));

test('an automatic completion audit failure rolls back every binding and leaves explicit durable failure without replaying the old request',integration,async t=>fixture(t,async({store,app,nightly,plan,calls})=>{
  const planned=await plan(),original=app.store.run.bind(app.store);let failed=false;
  app.store.run=async(sql,...params)=>{if(!failed&&sql==='INSERT INTO harbor.audit VALUES($1,$2,$3,$4)'&&params[2]==='session.handoff.completed'){failed=true;throw new Error('synthetic automatic audit failure');}return original(sql,...params);};
  await assert.rejects(nightly.apply(planned));app.store.run=original;assert.equal(failed,true);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.sessions WHERE route_version<>0')).n,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.nightly_plan_group_results")).n,0);assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'failed');assert.equal(calls.length,0);
}));


test('H2 planning fixes an earlier overlapping opaque result and recovers only after a fresh non-overlapping replayable context',integration,async t=>fixture(t,async({store,member,at})=>{
  const person=await store.authenticate(member.secret,{at}),old=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-2000}),latest=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-1500});
  for(const request of [old,latest]){await store.run("UPDATE harbor.requests SET session_id='auto-session-0',account_id='a',route_version=0,handoff_context='replayable',handoff_input='replayable' WHERE id=$1",request);await store.finish(request,{input:100,output:0,cached:0},at-1000);}
  await store.run("UPDATE harbor.requests SET handoff_context='opaque' WHERE id=$1",old);const reader=createPostgresPlanningInput(store,{clock:()=>at}),window={...transferWindow(at),start:at-16*3600000,end:at};
  assert.equal((await reader.read({window,requestBoundary:true})).workloads.find(row=>row.id==='auto-session-0').contextReplayable,false);
  const fresh=await store.observe(person,'fixture','fixture','gpt-6.1-sol',{at:at-500});await store.run("UPDATE harbor.requests SET session_id='auto-session-0',account_id='a',route_version=0,handoff_context='replayable',handoff_input='replayable' WHERE id=$1",fresh);await store.finish(fresh,{input:100,output:0,cached:0},at-100);
  assert.equal((await reader.read({window,requestBoundary:true})).workloads.find(row=>row.id==='auto-session-0').contextReplayable,true);
  // The older opaque row can fall outside the workload source window while
  // still overlapping a long latest request. Context checks are not windowed.
  await store.run('UPDATE harbor.requests SET started=$1,ended=$2 WHERE id=$3',at-19*3600000,at-17*3600000,old);await store.run('UPDATE harbor.requests SET started=$1 WHERE id=$2',at-18*3600000,fresh);
  assert.equal((await reader.read({window,requestBoundary:true})).workloads.find(row=>row.id==='auto-session-0').contextReplayable,false);
}));

test('planning shares the execution permission check without per-session key queries',integration,async t=>fixture(t,async({store,member,at})=>{
  const reader=createPostgresPlanningInput(store,{clock:()=>at}),window={...transferWindow(at),start:at-16*3600000,end:at};await store.run('UPDATE harbor.keys SET enabled=0 WHERE id=$1',member.keyId);
  const captured=await reader.read({window,requestBoundary:true});assert.ok(captured.workloads.every(row=>row.sourcePermissionAvailable===false));const preview=await reader.plan({window,requestBoundary:true});assert.equal(preview.moves.length,0);assert.ok(preview.skipped.some(row=>row.reason==='session_permissions_changed'));
}));

for(const complete of [false,true])test('H1 migration preserves legacy '+(complete?'completed':'partially committed')+' slot facts and fixes already migrated sessions',integration,async()=>{
  const admin=new PostgresStore(databaseOptions()),name='harbor_test_'+randomUUID().replaceAll('-','');let store,nightly;
  try{
    await admin.query('CREATE DATABASE '+name+" TEMPLATE template0 ENCODING 'UTF8' LC_COLLATE 'C.UTF-8' LC_CTYPE 'C.UTF-8'");store=new PostgresStore(databaseOptions(name));
    for(const file of readdirSync(new URL('../migrations/',import.meta.url)).filter(file=>/^\d{3}-.*\.sql$/.test(file)&&Number(file.slice(0,3))<=13).sort())await store.query(readFileSync(new URL('../migrations/'+file,import.meta.url),'utf8'));await store.initializePricing();
    const at=Date.parse('2026-11-03T10:00:01+08:00'),slot=transferWindow(at),planId='routing.nightly:'+slot.key,summary=JSON.stringify({planned:2,applied:complete?2:1});
    await store.run("INSERT INTO harbor.members(id,name,created) VALUES('legacy-member','合成员工',$1)",at);for(const account of ['a','b','c','d'])await store.run("INSERT INTO harbor.accounts(id,name,kind,auth_mode,home,state,quota_json) VALUES($1,$1,'codex','subscription',$1,'ready',$2)",account,JSON.stringify({plan:'synthetic-legacy',checked:at,limitReached:false,windows:[{minutes:10080,used:50,reset:at+7*86400000}]}));
    await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated,route_version,client_key,upstream_thread) VALUES('legacy-moved','legacy-member','b','responses',$1,$1,1,'synthetic-client','synthetic-thread')",at-1000);
    await store.run("INSERT INTO harbor.keys(id,member_id,digest,prefix,created) VALUES('legacy-key','legacy-member','synthetic-legacy-digest','synthetic-prefix',$1)",at);
    const request='legacy-request',pricing=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='pricing.state'")).value).active;await store.run("INSERT INTO harbor.requests(id,member_id,key_id,started,status,ip,model,price_version) VALUES($1,'legacy-member','legacy-key',$2,'queued','synthetic','gpt-6.1-sol',$3)",request,at-1000,pricing);await store.run("UPDATE harbor.requests SET session_id='legacy-moved',account_id='a',route_version=0,handoff_context='replayable',handoff_input='replayable' WHERE id=$1",request);await store.finish(request,{input:1000,output:0,cached:0},at-500);
    await store.run("INSERT INTO harbor.background_jobs(id,kind,target,payload,available_at,created,updated,status) VALUES($1,'routing.nightly',$2,$3::jsonb,$4,$4,$4,'completed')",planId,slot.key,JSON.stringify({boundary:slot.end}),at);
    await store.run('INSERT INTO harbor.routing_plans VALUES($1,$2,$3,$4::jsonb,1000)',planId,at,at+3600000,JSON.stringify({purpose:'nightly',window:slot,metric:'synthetic-reference-usd',counts:{moves:2,groups:2}}));
    for(const [section,ordinal,payload] of [['groups',1,{accounts:['a','b']}],['groups',2,{accounts:['c','d']}],['moves',1,{session_id:'legacy-moved',from:'a',to:'b',version:0}]])await store.run('INSERT INTO harbor.routing_plan_items VALUES($1,$2,$3,$4::jsonb,100)',planId,section,ordinal,JSON.stringify(payload));
    await store.run('INSERT INTO harbor.balance_runs(day,started,ended,status,summary,plan_id,attempts) VALUES($1,$2,$3,$4,$5,$6,1)',slot.key,at,complete?at:null,complete?'completed':'running',summary,planId);await store.run("INSERT INTO harbor.nightly_group_results VALUES($1,1,$2,'applied',NULL,1)",slot.key,at);if(complete)await store.run("INSERT INTO harbor.nightly_group_results VALUES($1,2,$2,'applied',NULL,1)",slot.key,at);
    // Keep the populated old schema as the migration source; current runtime
    // readers require the complete upgrade, including the later package tables.
    for(const file of readdirSync(new URL('../migrations/',import.meta.url)).filter(file=>/^\d{3}-.*\.sql$/.test(file)&&Number(file.slice(0,3))>13).sort())await store.query(readFileSync(new URL('../migrations/'+file,import.meta.url),'utf8'));await store.run("INSERT INTO harbor.gateway_settings(name,value) VALUES('balancing',$1) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value",JSON.stringify({nightly:true}));
    const gate=createSessionRequestHandoff(),contexts=Object.fromEntries(['inspect','sourceContext','targetAccount','validateTarget'].map(key=>[key,async()=>{throw new Error('completed legacy groups must never execute');}]));nightly=createPostgresNightly(store,{preparation:{reserve:()=>()=>{}},refreshQuotas:async()=>{},sessionHandoffs:gate,handoffAdmin:{contexts,attachAutomatic:()=>{}},clock:()=>at});await nightly.recover();
    const actual=await store.get('SELECT summary,status FROM harbor.balance_runs WHERE day=$1',slot.key);if(complete){assert.equal(actual.summary,summary);assert.equal(actual.status,'completed');assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_plan_revisions')).n,0);}else{assert.equal(JSON.parse(actual.summary).applied,1);assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_plan_revisions')).n,1);}
    assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_group_results')).n,complete?2:1);const input=await createPostgresPlanningInput(store,{clock:()=>at}).read({window:{...slot,start:at-16*3600000,end:at},requestBoundary:true});assert.equal(input.workloads.find(row=>row.id==='legacy-moved').transferredInSlot,true);assert.equal((await store.get("SELECT route_version FROM harbor.sessions WHERE id='legacy-moved'")).route_version,1);
  }finally{await nightly?.close();await store?.close();await admin.query('DROP DATABASE IF EXISTS '+name);await admin.close();}
});

test('the next slot cancels its predecessor without creating an obsolete revision and applies only the new decision',integration,async t=>fixture(t,async({store,nightly,now,held,releaseOld,send,calls,plan})=>{
  held.enabled=true;const first=send();let originalApply,currentApply;
  try{
    await until(()=>calls.length===1);const original=await plan(),oldSlot=transferWindow(now.at).key;originalApply=nightly.apply(original);originalApply.catch(()=>{});await until(async()=>!!await store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE status='waiting'"));now.at+=3*3600000;await store.run("UPDATE harbor.accounts SET quota_json=jsonb_set(quota_json::jsonb,'{checked}',to_jsonb($1::bigint))::text WHERE quota_json IS NOT NULL",now.at);
    await nightly.runOnce();await originalApply;assert.equal((await store.get('SELECT COUNT(*) n FROM harbor.nightly_plan_revisions WHERE slot=$1',oldSlot)).n,0);assert.equal((await store.get('SELECT status FROM harbor.balance_runs WHERE day=$1',oldSlot)).status,'interrupted');const current=await plan();assert.notEqual(current,original);currentApply=nightly.apply(current);await until(async()=>!!await store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE status='waiting'"));releaseOld();assert.equal((await first).status,200);await currentApply;assert.equal((await store.get("SELECT route_version FROM harbor.sessions WHERE id='auto-session-0'")).route_version,1);assert.equal((await store.get("SELECT account_id FROM harbor.requests WHERE session_id='auto-session-0' ORDER BY query_sequence DESC LIMIT 1")).account_id,'a');
  }finally{releaseOld();await first;await originalApply?.catch(()=>{});await currentApply?.catch(()=>{});}
}));

test('losing the automatic transaction acknowledgement preserves its committed checkpoint and never migrates or infers twice',integration,async t=>fixture(t,async({store,app,nightly,plan,calls})=>{
  const planned=await plan(),original=app.store.transaction.bind(app.store);let lost=false;
  app.store.transaction=async(callback,...options)=>{
    const outer=!app.store.context.getStore(),value=await original(callback,...options);
    if(outer&&!lost&&await store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE status='completed' LIMIT 1")){lost=true;throw new Error('synthetic committed acknowledgement loss');}
    return value;
  };
  await assert.rejects(nightly.apply(planned));app.store.transaction=original;assert.equal(lost,true);const committed=await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),audits=(await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n;assert.ok(audits>0);assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'completed');
  await nightly.apply(planned);assert.deepEqual(await store.all('SELECT id,account_id,route_version FROM harbor.sessions ORDER BY id'),committed);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,audits);assert.equal((await store.get('SELECT status FROM harbor.balance_runs')).status,'completed');assert.equal(calls.length,0);
}));

for(const summaryFails of [false,true])test(summaryFails?'H5 a failed orphan-summary write rolls back the terminal state and retries until the real history is accurate':'H3 terminal persistence failure exhausting the application budget is reaped so manual management is not permanently blocked',integration,async t=>fixture(t,async({store,app,nightly,plan,modelChecks,adminCall})=>{
  const planned=await plan(),original=app.store.run.bind(app.store);modelChecks.denied.add('b');app.store.run=async(sql,...params)=>{if(sql.startsWith('UPDATE harbor.automatic_handoff_groups SET status=$1'))throw new Error('synthetic finalizer unavailable');return original(sql,...params);};
  try{for(let attempt=0;attempt<5;attempt++)await assert.rejects(nightly.apply(planned));assert.equal((await nightly.apply(planned)).completed,false);}finally{app.store.run=original;}
  assert.equal(nightly.status().automatic.pendingGroups,0);assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.session_handoffs WHERE status='waiting'")).n>0,true);
  if(summaryFails){let failed=false;app.store.run=async(sql,...params)=>{if(!failed&&sql.startsWith('UPDATE harbor.balance_runs SET plan_id=')){failed=true;throw new Error('synthetic slot summary failure');}return original(sql,...params);};await nightly.runOnce();app.store.run=original;assert.equal(failed,true);assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'waiting');assert.ok((await store.get("SELECT COUNT(*) n FROM harbor.session_handoffs WHERE status='waiting'")).n>0);}
  await nightly.runOnce();assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.session_handoffs WHERE status='waiting'")).n,0);const batch=await store.get('SELECT status,summary FROM harbor.balance_runs');assert.equal(batch.status,'failed');assert.equal(JSON.parse(batch.summary).waiting,0);
  const manual=await adminCall('sessions/auto-session-0/handoffs','POST',{account_id:'c',expected_route_version:0});assert.equal(manual.status,202);await until(async()=> (await store.get("SELECT status FROM harbor.session_handoffs WHERE origin='manual'")).status==='completed');assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'c');
}));

test('disabling automatic balance leaves an already accepted manual takeover and its waiting request intact',integration,async t=>fixture(t,async({store,nightly,held,releaseOld,send,calls,plan,adminCall})=>{
  held.enabled=true;const first=send();let application,next;
  try{
    await until(()=>calls.length===1);application=nightly.apply(await plan());await until(async()=>!!await store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE status='waiting'"));const manual=await adminCall('sessions/auto-session-0/handoffs','POST',{account_id:'c',expected_route_version:0});assert.equal(manual.status,202);next=send();await until(async()=> (await adminCall('sessions/auto-session-0/handoffs')).body.phase==='waiting');await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='balancing'",JSON.stringify({nightly:false}));await nightly.runOnce();await application;assert.equal((await store.get("SELECT status FROM harbor.session_handoffs WHERE origin='manual'")).status,'waiting');releaseOld();assert.equal((await first).status,200);assert.equal((await next).status,200);assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'c');assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'cancelled');
  }finally{releaseOld();await first;await application?.catch(()=>{});await next?.catch(()=>{});}
}));

test('H4 cancelling automatic model verification reports cancellation and releases the original route without applying a binding',integration,async t=>fixture(t,async({store,nightly,held,releaseOld,send,calls,plan,adminCall,modelChecks})=>{
  held.enabled=true;modelChecks.blocked.set('b',new Promise(()=>{}));const first=send();let application;
  try{
    await until(()=>calls.length===1);application=nightly.apply(await plan());await until(async()=>!!await store.get("SELECT id FROM harbor.automatic_handoff_groups WHERE status='waiting'"));releaseOld();assert.equal((await first).status,200);await until(async()=> (await adminCall('sessions/auto-session-0/handoffs')).body.phase==='checking');const decision=(await adminCall('sessions/auto-session-0/handoffs')).body.id,cancelled=await adminCall('sessions/auto-session-0/handoffs/'+decision,'DELETE');assert.equal(cancelled.status,200);await application;assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'cancelled');assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'a');assert.equal((await store.get("SELECT COUNT(*) n FROM harbor.audit WHERE action='session.route'")).n,0);assert.equal((await send()).status,200);assert.equal(calls.length,2);
  }finally{releaseOld();await first;await application?.catch(()=>{});}
}));

test('H6 a restored automatic waiting decision remains visible and cancellable above the newer failed manual history',integration,async t=>fixture(t,async({store,nightly,held,releaseOld,send,calls,plan,adminCall,modelChecks})=>{
  held.enabled=true;modelChecks.blocked.set('b',new Promise(()=>{}));modelChecks.denied.add('c');const first=send();let application;
  try{
    await until(()=>calls.length===1);application=nightly.apply(await plan());await until(async()=>!!await store.get("SELECT id FROM harbor.session_handoffs WHERE session_id='auto-session-0' AND origin='automatic' AND status='waiting'"));const automatic=await store.get("SELECT id FROM harbor.session_handoffs WHERE session_id='auto-session-0' AND origin='automatic' AND status='waiting'");const manual=await adminCall('sessions/auto-session-0/handoffs','POST',{account_id:'c',expected_route_version:0});assert.equal(manual.status,202);releaseOld();assert.equal((await first).status,200);await until(async()=> (await store.get("SELECT status FROM harbor.session_handoffs WHERE origin='manual'")).status==='failed');
    const visible=await adminCall('sessions/auto-session-0/handoffs');assert.equal(visible.status,200);assert.equal(visible.body.id,automatic.id);assert.equal(visible.body.origin,'automatic');assert.equal(visible.body.status,'waiting');assert.equal(visible.body.binding.account_id,'a');const cancel=await adminCall('sessions/auto-session-0/handoffs/'+automatic.id,'DELETE');assert.equal(cancel.status,200);await application;assert.equal((await store.get('SELECT status FROM harbor.automatic_handoff_groups')).status,'cancelled');assert.equal((await store.get("SELECT account_id FROM harbor.sessions WHERE id='auto-session-0'")).account_id,'a');const historical=await adminCall('sessions/auto-session-0/handoffs');assert.equal(historical.body.origin,'manual');assert.equal(historical.body.status,'failed');assert.equal((await send()).status,200);assert.equal(calls.length,2);
  }finally{releaseOld();await first;await application?.catch(()=>{});}
}));
