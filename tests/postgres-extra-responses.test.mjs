import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import pricingSeed from '../pricing-data.mjs';
import {modelReleaseSource} from '../model-release-dates.mjs';
import {mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomUUID,randomBytes,createHash} from 'node:crypto';
import {PostgresStore} from '../postgres-store.mjs';
import {createPrivatePostgres} from '../tools/capacity/owned-runtime.mjs';
import {createPostgresMemberAdmin} from '../postgres-member-admin.mjs';
import {createPostgresMemberLimits} from '../postgres-member-limits.mjs';
import {createPostgresRouter} from '../postgres-routing.mjs';
import {createPostgresAccountExtraUsage} from '../postgres-account-extra-usage.mjs';
import {createPostgresExtraRuntime} from '../postgres-extra-runtime.mjs';
import {createPostgresResponsesHandler} from '../postgres-responses.mjs';
import {RecordingStore} from '../recording-store.mjs';
import {RecordingBuffer} from '../recording-buffer.mjs';
import {MeteringRecovery} from '../metering-recovery.mjs';
import {createResponsesTransport} from '../responses.mjs';
const binDirectory=process.env.HARBOR_CAPACITY_PG_BIN,libraryDirectory=process.env.HARBOR_CAPACITY_PG_LIB,integration={skip:!binDirectory?'需要本任务独占PG与真实HTTP，不访问上游收费服务':false,timeout:60000},model='gpt-6.1-sol',actor={session_ref:createHash('sha256').update('synthetic-http-admin').digest('hex')};
const deferred=()=>{let resolve;const promise=new Promise(yes=>{resolve=yes;});return {promise,resolve};};
const until=async predicate=>{const end=Date.now()+8000;while(!await predicate()){assert.ok(Date.now()<end,'synthetic condition timed out');await new Promise(resolve=>setTimeout(resolve,5));}};
const quota=(remaining=0)=>JSON.stringify({plan:'pro',checked:Date.now(),remaining,windows:[{used:100-remaining,minutes:10080,reset:Date.now()+3600000}]});
async function fixture(t,{second=false,initial=0,api=false,recording=false,admin=false,pricingOptions={}}={}){
 const lab=await createPrivatePostgres({binDirectory,libraryDirectory}),store=new PostgresStore({connection:lab.connection}),root=await mkdtemp(join(tmpdir(),'harbor-extra-http-')),key=randomBytes(32),accountId=randomUUID(),otherId=second?randomUUID():null,apiId=api?randomUUID():null,calls=[],gates=[];let upstream,server,handler,buffer,metering,memberAdmin;const state={credentialGate:null,sourceGate:null,disconnect:false};
 t.after(async()=>{for(const gate of gates)gate.resolve();handler?.abort();await until(()=>!handler||handler.status().active===0).catch(()=>{});server?.closeAllConnections();upstream?.closeAllConnections();if(server)await new Promise(resolve=>server.close(resolve));if(upstream)await new Promise(resolve=>upstream.close(resolve));await handler?.closeQueries();await metering?.close();await buffer?.close();await memberAdmin?.close();await store.close();await lab.close();await rm(root,{recursive:true,force:true});});
 await store.migrate();await store.initializePricing();memberAdmin=createPostgresMemberAdmin(store,{encryptionKey:key});const member=(await memberAdmin.handle('/api/admin/members','POST',{name:'合成HTTP额度员工',quota:null,expires:null,allowed_models:[model]})).value;await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) VALUES($1,'合成耗尽账号',$1,'ready',$2)",accountId,quota(initial));if(otherId)await store.run("INSERT INTO harbor.accounts(id,name,home,state,quota_json) VALUES($1,'合成包内账号',$1,'ready',$2)",otherId,quota(50));if(apiId)await store.run("INSERT INTO harbor.accounts(id,name,home,state,auth_mode,api_weight) VALUES($1,'合成API账号',$1,'ready','openai_api',20)",apiId);
 const service=createPostgresAccountExtraUsage(store,{readEntitlement:async owner=>({capability:{supported:true,platform:owner.kind,auth_mode:owner.auth_mode},entitlement:{state:'verified',source:'official',remaining_atoms:'1000',unit:'credits',models:[model],protocols:['responses','images'],verified_at:Date.now(),valid_until:Date.now()+60000,expires_at:null}})}),extraRuntime=createPostgresExtraRuntime(store,{service}),router=createPostgresRouter(store,{extraRuntime}),limits=createPostgresMemberLimits(store);
 upstream=http.createServer((req,res)=>{void(async()=>{for await(const _chunk of req){}calls.push({account_id:req.headers['chatgpt-account-id']||(req.headers.authorization==='Bearer synthetic-api'?apiId:'unknown'),path:req.url});if(state.sourceGate)await state.sourceGate.promise;if(state.disconnect){res.writeHead(200,{'Content-Type':'text/event-stream'});res.write('data: '+JSON.stringify({type:'response.output_text.delta',item_id:'message',output_index:0,delta:'合成片段'})+'\n\n');setImmediate(()=>res.destroy());return;}if(req.url.includes('images')){res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify({model,data:[{b64_json:'c3ludGhldGlj'}],usage:{input_tokens:1000,output_tokens:200,input_tokens_details:{cached_tokens:100}}}));return;}const response={id:'synthetic-response',status:'completed',model,output:[{id:'message',type:'message',content:[{type:'output_text',text:'合成完成'}]}],usage:{input_tokens:1000,output_tokens:200,input_tokens_details:{cached_tokens:100}}};response.output.push(...(state.extraOutput??[]));res.writeHead(200,{'Content-Type':'text/event-stream'});res.end((state.terminalOnly?'':'data: '+JSON.stringify({type:'response.output_text.delta',item_id:'message',output_index:0,delta:'合成完成'})+'\n\n')+'data: '+JSON.stringify({type:'response.completed',response})+'\n\n');})().catch(()=>res.destroy());});await new Promise(resolve=>upstream.listen(0,'127.0.0.1',resolve));const upstreamOrigin='http://127.0.0.1:'+upstream.address().port;
 const transport=apiMode=>createResponsesTransport({apiKeyMode:apiMode,credentials:async(_binary,home)=>{if(state.credentialGate)await state.credentialGate.promise;return {token:apiMode?'synthetic-api':'synthetic-subscription',accountId:home};},version:async()=>'0.160.1',fetchImpl:(url,options)=>fetch(upstreamOrigin+new URL(url).pathname,options)}),subscription=transport(false),apiTransport=transport(true);
 const recordings=new RecordingStore(store,{encryptionKey:key});buffer=await RecordingBuffer.create(recordings,{directory:join(root,'recording'),flushMs:10000});metering=await MeteringRecovery.create(store,{encryptionKey:key,directory:join(root,'metering'),flushMs:10000});handler=createPostgresResponsesHandler({store,recordings,buffer,meteringRecovery:metering,extraRuntime,resolveRoute:router.resolve,onRouteFailure:router.block,checkLimits:(current,payload,options)=>limits.check(current.id,payload.model,options),transportForAccount:async account=>account.auth_mode==='openai_api'?apiTransport:subscription,adminAuthenticated:async req=>admin&&req.headers.authorization==='Bearer synthetic-admin',pricingOptions,recordingEnabled:recording});
 server=http.createServer((req,res)=>{const write=res.write.bind(res);res.write=(...args)=>{if(state.failDownstreamWrite){state.failDownstreamWrite=false;res.emit('close');throw Object.assign(new Error('synthetic downstream disconnect'),{status:499,code:'request_cancelled'});}return write(...args);};void handler.handle(req,res).then(handled=>{if(!handled){res.writeHead(404);res.end();}}).catch(error=>{if(res.headersSent){res.end();return;}res.writeHead(error.status||500,{'Content-Type':'application/json'});res.end(JSON.stringify({error:{code:error.code||'internal_error'}}));});});await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));const origin='http://127.0.0.1:'+server.address().port;
 const send=async({thread=randomUUID(),fixed,signal,path='/v1/responses',secret=member.secret}={})=>{const res=await fetch(origin+path,{method:'POST',signal,headers:{authorization:'Bearer '+secret,'content-type':'application/json','thread-id':thread,...(fixed?{'x-harbor-session':fixed}:{})},body:JSON.stringify(path.includes('images')?{model,prompt:'合成图片请求'}:{model,input:'合成预算测试',stream:false})});const data=await res.json();return {status:res.status,data,thread};};
 const legacyEnable=async({limit='1000000000000',reserve='10000000000'}={})=>{const p=await service.readPolicy();if(!p.enabled)await service.savePolicy({enabled:true,expected_version:p.version},actor);const row=await service.readAccount(accountId);await service.saveAccount(accountId,{mode:'inherit',budget:{basis:'reference_estimate',unit:'reference_usd_atoms',limit_atoms:limit,request_reserve_atoms:reserve},expected_version:row.configuration.version,expected_identity_epoch:row.identity_epoch},actor);await service.refresh(accountId);};
 const manual=async()=>{await router.saveSettings({weighted:true});const current=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'"))?.value||'{"version":0}');await router.saveWeights({mode:'manual',weights:{[accountId]:100,...(otherId?{[otherId]:0}:{})},expected_version:current.version});};
 const fixed=async(account=accountId)=>{const session=randomUUID();await store.run("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated,nightly_enabled) VALUES($1,$2,$3,'responses',$4,$4,0)",session,member.memberId,account,Date.now());return session;};
 return {lab,store,metering,buffer,recordings,origin,service,router,handler,member,accountId,otherId,apiId,calls,state,send,legacyEnable,manual,fixed,gate:()=>{const gate=deferred();gates.push(gate);return gate;}};
}


const noHolds=async f=>assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.account_extra_holds')).n,0);
const queued=async f=>until(async()=>(await f.store.get("SELECT COUNT(*) n FROM harbor.requests WHERE status='queued'")).n===1);
const expire=async f=>{const current=JSON.parse((await f.store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights'")).value);current.expires=Date.now()-1;await f.store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='routing_weights'",JSON.stringify(current));};

test('耗尽账号没有手动正权重时拒绝新请求及固定绑定，旧许可预算不构成自动授权',integration,async t=>{const f=await fixture(t);for(const legacy of [false,true]){if(legacy)await f.legacyEnable();assert.equal((await f.send()).status,503);assert.equal((await f.send({fixed:await f.fixed()})).status,503);}assert.equal(f.calls.length,0);await noHolds(f);});
test('管理员手动正权重直接调用耗尽订阅，不需许可预算且仅使用原请求计量',integration,async t=>{const f=await fixture(t,{second:true});await f.manual();assert.equal((await f.service.readPolicy()).enabled,false);const result=await f.send();assert.equal(result.status,200);assert.equal(f.calls[0].account_id,f.accountId);const row=await f.store.get('SELECT r.status,r.input,r.output,r.cached,p.total_atoms::text amount FROM harbor.requests r JOIN harbor.request_prices p ON p.request_id=r.id');assert.equal(row.status,'ok');assert.equal(row.input,1000);assert.equal(row.output,200);assert.equal(row.cached,100);assert.ok(BigInt(row.amount)>0n);await noHolds(f);assert.equal((await f.service.readAccount(f.accountId)).usage.used_atoms,'0');});
test('凭据准备期间将权重改为0会撤销尚未派发的额外调用',integration,async t=>{const f=await fixture(t,{second:true});await f.manual();const gate=f.gate();f.state.credentialGate=gate;const pending=f.send();await queued(f);await f.router.saveWeights({mode:'manual',weights:{[f.accountId]:0,[f.otherId]:100},expected_version:1});gate.resolve();const result=await pending;assert.equal(result.status,503);assert.equal(result.data.error.code,'extra_manual_weight_zero');assert.equal(f.calls.length,0);await noHolds(f);});
test('凭据准备期间收紧员工模型权限会拒绝尚未派发的额外调用',integration,async t=>{const f=await fixture(t);await f.manual();const gate=f.gate();f.state.credentialGate=gate;const pending=f.send();await queued(f);await f.store.run("UPDATE harbor.keys SET allowed_models='[]' WHERE id=$1",f.member.keyId);gate.resolve();assert.equal((await pending).status,403);assert.equal(f.calls.length,0);await noHolds(f);});
test('凭据准备期间账号停用不会继续调用原账号',integration,async t=>{const f=await fixture(t);await f.manual();const gate=f.gate();f.state.credentialGate=gate;const pending=f.send();await queued(f);await f.store.run('UPDATE harbor.accounts SET enabled=0 WHERE id=$1',f.accountId);gate.resolve();assert.equal((await pending).status,503);assert.equal(f.calls.length,0);await noHolds(f);});
test('手动正权重在凭据准备期间包内耗尽后仍按原账号调用，不创建额外hold',integration,async t=>{const f=await fixture(t,{initial:50});await f.manual();const gate=f.gate();f.state.credentialGate=gate;const pending=f.send();await queued(f);await f.store.run('UPDATE harbor.accounts SET quota_json=$1 WHERE id=$2',quota(0),f.accountId);gate.resolve();assert.equal((await pending).status,200);assert.equal(f.calls[0].account_id,f.accountId);await noHolds(f);});
test('手动正权重支持真实并发请求，无旧一次授权预算限制且逐请求计量',integration,async t=>{const f=await fixture(t);await f.manual();const gate=f.gate();f.state.sourceGate=gate;const first=f.send(),second=f.send();await until(()=>f.calls.length===2);await noHolds(f);gate.resolve();const results=await Promise.all([first,second]);assert.deepEqual(results.map(result=>result.status),[200,200]);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.requests')).n,2);assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.request_prices WHERE total_atoms>0')).n,2);});
test('上游断流仅计为未知失败，不自动重放或产生额外hold',integration,async t=>{const f=await fixture(t,{second:true});await f.manual();f.state.disconnect=true;assert.equal((await f.send()).status,502);assert.equal(f.calls.length,1);assert.equal(f.calls[0].account_id,f.accountId);const row=await f.store.get('SELECT status,usage_unknown FROM harbor.requests');assert.notEqual(row.status,'ok');assert.equal(row.usage_unknown,1);await noHolds(f);});
test('已派发请求在权重调整后继续完成，后续新请求使用新权重',integration,async t=>{const f=await fixture(t,{second:true});await f.manual();const gate=f.gate();f.state.sourceGate=gate;const pending=f.send();await until(()=>f.calls.length===1);await f.router.saveWeights({mode:'manual',weights:{[f.accountId]:0,[f.otherId]:100},expected_version:1});gate.resolve();assert.equal((await pending).status,200);f.state.sourceGate=null;assert.equal((await f.send()).status,200);assert.deepEqual(f.calls.map(call=>call.account_id),[f.accountId,f.otherId]);await noHolds(f);});
test('官方API固定通道不受订阅额外额度策略影响，不创建订阅hold',integration,async t=>{const f=await fixture(t,{api:true});await f.manual();await f.store.run('UPDATE harbor.accounts SET api_weight=0 WHERE id=$1',f.apiId);assert.equal((await f.send({fixed:await f.fixed(f.apiId)})).status,200);assert.equal(f.calls[0].account_id,f.apiId);await noHolds(f);});
test('未知包内快照保持兼容，真实JSON图片调用按手动正权重执行并计量',integration,async t=>{const f=await fixture(t);await f.store.run('UPDATE harbor.accounts SET quota_json=NULL WHERE id=$1',f.accountId);assert.equal((await f.send({fixed:await f.fixed()})).status,200);await f.store.run('UPDATE harbor.accounts SET quota_json=$1 WHERE id=$2',quota(0),f.accountId);await f.manual();assert.equal((await f.send({path:'/v1/images/generations'})).status,200);assert.equal(f.calls.length,2);assert.ok(f.calls[1].path.includes('images'));assert.equal((await f.store.get('SELECT COUNT(*) n FROM harbor.request_prices WHERE total_atoms>0')).n,2);await noHolds(f);});
test('停用账号可转移到手动正权重的耗尽账号，未设正权重时保持原绑定拒绝停用',integration,async t=>{const f=await fixture(t,{second:true}),session=await f.fixed(f.otherId);await assert.rejects(f.router.disableAccount(f.otherId),error=>error.code==='disable_no_target');assert.equal((await f.store.get('SELECT enabled FROM harbor.accounts WHERE id=$1',f.otherId)).enabled,1);assert.equal((await f.store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',session)).account_id,f.otherId);await f.manual();await f.router.disableAccount(f.otherId);assert.equal((await f.store.get('SELECT enabled FROM harbor.accounts WHERE id=$1',f.otherId)).enabled,0);assert.equal((await f.store.get('SELECT account_id FROM harbor.sessions WHERE id=$1',session)).account_id,f.accountId);assert.equal((await f.send({fixed:session})).status,200);assert.equal(f.calls[0].account_id,f.accountId);await noHolds(f);});
test('自动包内请求在准备期间耗尽不会自动回退额外调用，下一请求选择可用包内账号',integration,async t=>{const f=await fixture(t,{initial:50,second:true});await f.store.run('UPDATE harbor.accounts SET enabled=0 WHERE id=$1',f.otherId);const gate=f.gate();f.state.credentialGate=gate;const pending=f.send();await queued(f);await f.store.run('UPDATE harbor.accounts SET quota_json=$1 WHERE id=$2',quota(0),f.accountId);await f.store.run('UPDATE harbor.accounts SET enabled=1 WHERE id=$1',f.otherId);gate.resolve();assert.equal((await pending).status,503);assert.equal(f.calls.length,0);f.state.credentialGate=null;assert.equal((await f.send()).status,200);assert.equal(f.calls[0].account_id,f.otherId);await noHolds(f);});
test('固定绑定不绕过手动正权重要求，准备期间耗尽后拒绝额外调用',integration,async t=>{const f=await fixture(t,{initial:50,second:true});const gate=f.gate();f.state.credentialGate=gate;const pending=f.send({fixed:await f.fixed()});await queued(f);await f.store.run('UPDATE harbor.accounts SET quota_json=$1 WHERE id=$2',quota(0),f.accountId);gate.resolve();assert.equal((await pending).status,503);assert.equal(f.calls.length,0);await noHolds(f);});
test('旧全局许可写锁不再阻塞手动正权重的真实派发',integration,async t=>{const f=await fixture(t);await f.manual();const blocker=new PostgresStore({connection:f.lab.connection}),entered=deferred(),release=deferred();t.after(()=>blocker.close());const blocked=blocker.transaction(async()=>{await blocker.get('SELECT version FROM harbor.extra_usage_policy WHERE singleton=1 FOR UPDATE');entered.resolve();await release.promise;});let pending;try{await entered.promise;pending=f.send();await until(()=>f.calls.length===1);assert.equal((await pending).status,200);await noHolds(f);}finally{release.resolve();await blocked;await pending;}});
test('手动正权重到期后拒绝新请求与固定耗尽绑定，凭据准备中到期也拒绝',integration,async t=>{const f=await fixture(t);await f.manual();const gate=f.gate();f.state.credentialGate=gate;const pending=f.send();await queued(f);await expire(f);gate.resolve();assert.equal((await pending).status,503);f.state.credentialGate=null;assert.equal((await f.send()).status,503);assert.equal((await f.send({fixed:await f.fixed()})).status,503);assert.equal(f.calls.length,0);await noHolds(f);});
test('官方硬消费限制仍阻止正权重包内及额外请求，不向上游派发',integration,async t=>{const f=await fixture(t,{initial:50});await f.manual();for(const flag of ['spend_control','workspace']){const snapshot=JSON.parse(quota(50));snapshot.officialDenial={ordinary:false,spend_control:false,workspace:false,[flag]:true};await f.store.run('UPDATE harbor.accounts SET quota_json=$1 WHERE id=$2',JSON.stringify(snapshot),f.accountId);assert.equal((await f.send()).status,503);assert.equal((await f.send({fixed:await f.fixed()})).status,503);}assert.equal(f.calls.length,0);await noHolds(f);});
test('官方普通额度拒绝且没有窗口时管理员正权重可直接尝试上游',integration,async t=>{const f=await fixture(t);await f.store.run('UPDATE harbor.accounts SET quota_json=$1 WHERE id=$2',JSON.stringify({checked:Date.now(),remaining:null,windows:[],ordinaryUsageAllowed:false,officialDenial:{ordinary:true,spend_control:false,workspace:false}}),f.accountId);assert.equal((await f.send()).status,503);await f.manual();assert.equal((await f.send()).status,200);assert.equal(f.calls.length,1);await noHolds(f);});


test('真实Responses在HTTP结束后持久化分段计时，重复遥测不重复计量',integration,async t=>{
 const f=await fixture(t);await f.manual();assert.equal((await f.send()).status,200);
 await until(async()=>{await f.metering.flush();const row=await f.store.get('SELECT phase_history FROM harbor.requests');return !!JSON.parse(row.phase_history).at(-1).stream_timing;});
 const before=await f.store.get('SELECT * FROM harbor.requests'),summary=JSON.parse(before.phase_history).at(-1).stream_timing;
 assert.equal(summary.version,1);assert.equal(summary.clientReceiptVerified,false);assert.equal(summary.outcome,'ok');
 assert.ok(summary.offsetsMs.responseEnd>=summary.offsetsMs.transportEnd);assert.ok(summary.counts.rawChunks>0);assert.ok(summary.counts.decodedEvents>0);
 const memberBefore=await f.store.get('SELECT used FROM harbor.members WHERE id=$1',before.member_id),priceBefore=await f.store.get('SELECT total_atoms::text amount FROM harbor.request_prices WHERE request_id=$1',before.id);
 assert.equal((await f.store.finish(before.id,{input:999999,output:999999,streamTiming:{...summary,outcome:'failed'}},Date.now())).settled,false);
 assert.deepEqual(await f.store.get('SELECT used FROM harbor.members WHERE id=$1',before.member_id),memberBefore);
 assert.deepEqual(await f.store.get('SELECT total_atoms::text amount FROM harbor.request_prices WHERE request_id=$1',before.id),priceBefore);
 assert.deepEqual(JSON.parse((await f.store.get('SELECT phase_history FROM harbor.requests')).phase_history).at(-1).stream_timing,summary);
});


test('业务成功与工具采集覆盖分开，未知可见项不能假报正文完整',integration,async t=>{
 const f=await fixture(t,{recording:true});await f.manual();
 f.state.extraOutput=[{id:'synthetic-tool',type:'custom_tool_call',name:'exec',call_id:'synthetic-call',input:'echo "synthetic"'}, {id:'synthetic-unknown',type:'future_visible_work',text:'synthetic'}];
 assert.equal((await f.send()).status,200);await f.buffer.flush();
 const row=await f.store.get('SELECT id,status,recording_state,recording_reason FROM harbor.requests');
 assert.equal(row.status,'ok');assert.equal(row.recording_state,'partial');
 const page=await f.recordings.read(row.id,{limit:100}),tools=page.events.filter(event=>event.kind==='tool_call');
 assert.equal(tools.length,1);assert.equal(tools[0].content.callId,'synthetic-call');assert.equal(tools[0].content.text,'echo "synthetic"');
 const boundary=page.events.find(event=>event.content.section==='coverage');assert.ok(boundary);assert.equal(boundary.content.complete,false);assert.ok(boundary.content.reasons.includes('unsupported_item'));
 assert.equal(boundary.content.seen.toolCalls,1);assert.equal(boundary.content.captured.toolCalls,1);
});


test('非流式仅终态正文配对最终JSON写入，不误报内容丢弃',integration,async t=>{
 const f=await fixture(t);await f.manual();f.state.terminalOnly=true;assert.equal((await f.send()).status,200);
 await until(async()=>{await f.metering.flush();const row=await f.store.get('SELECT phase_history FROM harbor.requests');return !!JSON.parse(row.phase_history).at(-1).stream_timing;});
 const summary=JSON.parse((await f.store.get('SELECT phase_history FROM harbor.requests')).phase_history).at(-1).stream_timing;
 assert.equal(summary.complete,true);assert.deepEqual(summary.missing,[]);assert.equal(summary.counts.contentChunks,1);assert.equal(summary.counts.failedWrites,0);
 assert.ok(summary.durationsMs.firstContentRelay>=0);assert.ok(summary.durationsMs.firstContentWrite>=0);assert.equal(summary.clientReceiptVerified,false);
});


test('真实管理入口恢复费率配置和刷新，拒绝未登录并经原请求体读取',integration,async t=>{
 const official='# Pricing\n### Standard pricing data\n| Model | Input | Cached input | Output |\n| --- | --- | --- | --- |\n| synthetic-new-model | $3 | $0.3 | $15 |\n### Batch pricing data\n';const releases='# Changelog\n\n## November, 2026\n\n### Nov 10\n\nFeature · Model: synthetic-new-model · API: v1/responses\n\nReleased [Synthetic](https://developers.openai.com/api/docs/models/synthetic-new-model) for synthetic testing.\n';const fetched={pricing:0,releases:0};
 const f=await fixture(t,{admin:true,pricingOptions:{fetchImpl:async url=>{if(url===pricingSeed.source){fetched.pricing++;return new Response(official);}assert.equal(url,modelReleaseSource,'只允许受控的官方价格与发布日期来源');fetched.releases++;return new Response(releases);}}});
 const call=async(path,method='GET',body,authenticated=true)=>{
  const response=await fetch(f.origin+path,{method,headers:{...(authenticated?{authorization:'Bearer synthetic-admin'}:{}),'content-type':'application/json'},...(body===undefined?{}:{body:JSON.stringify(body)})});
  return {status:response.status,value:await response.json()};
 };
 assert.equal((await call('/api/admin/pricing','GET',undefined,false)).status,401);
 const initial=await call('/api/admin/pricing');assert.equal(initial.status,200);
 const configured=await call('/api/admin/pricing','PATCH',{expectedVersion:initial.value.settings.version,refreshHours:12});
 assert.equal(configured.status,200);assert.equal(configured.value.settings.refreshHours,12);
 assert.equal((await call('/api/admin/pricing','PATCH',{expectedVersion:initial.value.settings.version,refreshHours:6})).status,400);
 const refreshed=await call('/api/admin/pricing/refresh','POST',{});assert.equal(refreshed.status,200);assert.deepEqual(fetched,{pricing:1,releases:1});assert.equal(refreshed.value.rates.find(rate=>rate.model==='synthetic-new-model')?.release_date,'2026-11-10');assert.equal(refreshed.value.modelReleaseState.error,null);assert.ok(refreshed.value.rates.some(rate=>rate.model==='synthetic-new-model'));
 assert.ok(!refreshed.value.rates.some(rate=>rate.model==='codex-auto-review'));
});

test('关闭流程先排空已接受的费率操作，再关闭共享查询队列',integration,async t=>{
 const f=await fixture(t,{admin:true}),gate=f.gate(),original=f.store.all.bind(f.store);let blocked=false;
 f.store.all=async(sql,...args)=>{if(sql.includes("GROUP BY COALESCE(r.billing_model,r.model,'未记录')")&&!blocked){blocked=true;await gate.promise;}return original(sql,...args);};
 const pending=fetch(f.origin+'/api/admin/pricing',{method:'PATCH',headers:{authorization:'Bearer synthetic-admin','content-type':'application/json'},body:JSON.stringify({expectedVersion:0,refreshHours:12})});
 await until(()=>blocked);const closing=f.handler.closeQueries();
 assert.equal(f.handler.status().queries.closed,false,'admitted pricing response must retain its query queue');
 gate.resolve();const response=await pending;assert.equal(response.status,200);assert.equal((await response.json()).settings.refreshHours,12);
 await closing;assert.equal(f.handler.status().queries.closed,true);
});


test('下游中断仍结算同一已收分片的真实终态用量，不重发模型',integration,async t=>{
 const f=await fixture(t);await f.manual();f.state.failDownstreamWrite=true;
 let disconnected=false;try{const response=await fetch(f.origin+'/v1/responses',{method:'POST',headers:{authorization:'Bearer '+f.member.secret,'content-type':'application/json'},body:JSON.stringify({model,input:'合成已收计量验证',stream:true})});await response.text();}catch(error){assert.equal(error.cause?.code,'UND_ERR_SOCKET');disconnected=true;}assert.equal(disconnected,true);
 await until(async()=>{const row=await f.store.get('SELECT status FROM harbor.requests');return !!row&&row.status!=='queued'&&row.status!=='running';});
 const row=await f.store.get('SELECT r.status,r.usage_unknown,r.input,r.output,r.cached,p.total_atoms::text amount FROM harbor.requests r JOIN harbor.request_prices p ON p.request_id=r.id');
 assert.equal(row.status,'cancelled');assert.equal(row.usage_unknown,0);assert.equal(row.input,1000);assert.equal(row.output,200);assert.equal(row.cached,100);assert.ok(BigInt(row.amount)>0n);assert.equal(f.calls.length,1);
});
