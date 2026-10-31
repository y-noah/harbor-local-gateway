import {test} from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtempSync,rmSync,readFileSync,writeFileSync,existsSync} from 'node:fs';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {createPlatformService} from '../platform-service.mjs';
import {createGateway} from '../server.mjs';
import {createPlatformTransport,platformConfiguration,platformModel,validatePlatformChat,qoderCatalog,qoderCounts} from '../platforms.mjs';

const payload={model:'deepseek/example-model',messages:[{role:'user',content:'合成问题'}]};
async function fixture(t,{transport,upstream,clock,rateReader}={}){
  const root=mkdtempSync(join(tmpdir(),'harbor-platform-test-')),calls=[];
  let upstreamServer,upstreamBase;
  if(upstream){upstreamServer=http.createServer(async(req,res)=>{let raw='';for await(const chunk of req)raw+=chunk;calls.push({path:req.url,headers:req.headers,body:raw?JSON.parse(raw):null});try{await upstream(req,res,calls.at(-1));}catch{res.writeHead(500);res.end('{}');}});await new Promise(resolve=>upstreamServer.listen(0,'127.0.0.1',resolve));upstreamBase='http://127.0.0.1:'+upstreamServer.address().port;}
  const platformTransport=transport||createPlatformTransport({fetchImpl:(url,options)=>fetch(upstreamBase+new URL(url).pathname+new URL(url).search,options),pollMs:1,clock});
  const app=createGateway({dataDir:join(root,'data'),runtimeDir:join(root,'runtime'),config:{adminToken:'test-admin',codexBinary:'unused'},responsesTransport:{models:async()=>[],run:async()=>({response:{id:'synthetic-response'},usageKnown:true,input:1,output:1,cached:0})},platformTransport,clock,rateReader:rateReader|| (async()=>null),pricingFetch:async()=>{throw Error('offline');}});
  await new Promise(resolve=>app.server.listen(0,'127.0.0.1',resolve));const base='http://127.0.0.1:'+app.server.address().port;
  t.after(async()=>{await app.close();if(upstreamServer)await new Promise(resolve=>upstreamServer.close(resolve));rmSync(root,{recursive:true,force:true});});
  async function call(path,method='GET',data,secret='test-admin'){
    const response=await fetch(base+path,{method,headers:{Authorization:'Bearer '+secret,...(data?{'Content-Type':'application/json'}:{})},...(data?{body:JSON.stringify(data)}:{})});return {status:response.status,body:await response.json()};
  }
  const create=async(provider='deepseek',extra={})=>{const created=await call('/api/admin/platform/accounts','POST',{name:'合成平台账号',provider,credential:'synthetic-upstream-key',...extra});assert.equal(created.status,201);return created.body.id;};
  const check=id=>call('/api/admin/platform/accounts/'+id+'/check','POST',{});
  const member=app.store.createMember('合成员工',null,null);
  const chat=(data={},key=member.secret)=>call('/v1/platform/chat/completions','POST',{...payload,...data},key);
  return {...app,base,root,calls,call,create,check,chat,member};
}
const send=(res,value)=>{res.writeHead(200,{'Content-Type':'application/json'});res.end(JSON.stringify(value));};
function compatible(req,res){if(req.url.endsWith('/models'))return send(res,{data:[{id:'example-model'},{id:'other-model'}]});send(res,{model:'example-model',choices:[{message:{content:'合成回复'},finish_reason:'stop'}],usage:{prompt_tokens:100,completion_tokens:20,prompt_tokens_details:{cached_tokens:30}}});}

test('platform configuration rejects arbitrary destinations and model aliases',()=>{
  for(const provider of ['constructor','__proto__','unknown'])assert.throws(()=>platformConfiguration(provider));
  assert.throws(()=>platformConfiguration('qwen','http://localhost'));
  assert.equal(platformConfiguration('qwen','dashscope').region,'dashscope');
  assert.deepEqual(platformModel('deepseek/model/name'),{provider:'deepseek',model:'model/name'});
  for(const model of ['codex','unknown/test','deepseek/--flag','qwen/'])assert.throws(()=>platformModel(model));
  for(const bad of [{...payload,tools:[]},{...payload,messages:[{role:'user',content:[]}]},{...payload,stream:'true'},{...payload,messages:[{role:'user',content:'x',tool_calls:[]}]},{...payload,temperature:-1}])assert.throws(()=>validatePlatformChat(bad));
});
test('platform creation is admin only, encrypted and separate from legacy pools',async t=>{
  const app=await fixture(t,{upstream:compatible});assert.equal((await app.call('/api/admin/platform/accounts','POST',{},app.member.secret)).status,401);
  const id=await app.create(),row=app.store.get('SELECT * FROM accounts WHERE id=?',id);assert.equal(row.auth_mode,'platform');assert.equal(row.state,'offline');assert.ok(!row.api_secret_cipher.includes('synthetic-upstream-key'));assert.equal(JSON.parse(app.store.upstreamKey(row)).token,'synthetic-upstream-key');
  assert.equal((await app.chat()).status,503);assert.equal((await app.check(id)).status,200);
  const state=await app.call('/api/admin/state');assert.ok(!JSON.stringify(state).includes('synthetic-upstream-key'));assert.deepEqual(app.balancer.distribution().eligible,[]);
  assert.equal((await app.call('/api/admin/accounts/'+id+'/check','POST',{})).status,400);
  const oldModels=await app.call('/v1/models','GET',undefined,app.member.secret);assert.ok(!JSON.stringify(oldModels).includes('deepseek/example-model'));
  const models=await app.call('/v1/platform/models','GET',undefined,app.member.secret);assert.equal(models.body.data[0].id,'deepseek/example-model');
});
test('DeepSeek HTTP dispatch records usage, binds account and scopes ownership',async t=>{
  const app=await fixture(t,{upstream:compatible}),id=await app.create();await app.check(id);
  const result=await app.chat();assert.equal(result.status,200);assert.equal(result.body.choices[0].message.content,'合成回复');assert.equal(result.body.usage.prompt_tokens,100);assert.equal(result.body.usage.prompt_tokens_details.cached_tokens,30);
  const request=app.store.get('SELECT * FROM requests');assert.equal(request.model,'deepseek/example-model');assert.equal(request.billing_model,'deepseek/example-model');assert.equal(request.status,'ok');assert.equal(request.usage_unknown,0);assert.equal(request.account_id,id);
  assert.ok(!JSON.stringify(app.store.all('SELECT * FROM requests')).includes('合成问题'));assert.ok(!readFileSync(join(app.root,'data','gateway.sqlite')).includes(Buffer.from('合成问题')));
  const other=app.store.createMember('其他合成员工',null,null);assert.equal((await app.chat({session_id:result.body.session_id},other.secret)).status,404);
  assert.equal((await app.chat({session_id:result.body.session_id,model:'qwen/example-model'})).status,409);
  assert.equal(app.calls.at(-1).headers.authorization,'Bearer synthetic-upstream-key');assert.equal(app.calls.at(-1).body.model,'example-model');
});
test('Qwen uses configured official endpoint and exact model authorization',async t=>{
  const app=await fixture(t,{upstream:compatible}),id=await app.create('qwen',{region:'china_plan'});assert.equal((await app.check(id)).status,200);assert.equal(app.calls[0].path,'/compatible-mode/v1/models');
  app.store.run('UPDATE keys SET allowed_models=? WHERE id=?',JSON.stringify(['qwen/other-model']),app.member.keyId);
  assert.equal((await app.chat({model:'qwen/example-model'})).status,403);const listed=await app.call('/v1/platform/models','GET',undefined,app.member.secret);assert.deepEqual(listed.body.data.map(m=>m.id),['qwen/other-model']);
  assert.equal((await app.chat({model:'qwen/other-model'})).status,200);
});
test('platform uses per-member shared limits; closed switch preserves legacy behavior',async t=>{
  const app=await fixture(t,{upstream:compatible}),id=await app.create();await app.check(id);
  app.store.pricing.configure({expectedVersion:app.store.pricing.status().settings.version,manualRates:{'deepseek/example-model':{input:'1',cached:'0.5',output:'2',source:'合成验收费率'}}});
  app.store.memberLimits.save(app.member.memberId,{expected_version:0,enabled:true,rules:[{model:'deepseek/example-model',hours:24,limit_usd:'0.00'}]});
  assert.equal((await app.chat()).status,429);const sibling=app.store.createKey(app.member.memberId);assert.equal((await app.chat({},sibling.secret)).status,429);assert.equal(app.calls.filter(c=>c.path.includes('chat/completions')).length,0);
  app.store.memberLimits.save(app.member.memberId,{expected_version:1,enabled:false});assert.equal((await app.chat()).status,200);
});
test('unknown or malformed upstream usage is never recorded as free usage',async t=>{
  const app=await fixture(t,{upstream:(req,res)=>req.url.endsWith('/models')?compatible(req,res):send(res,{model:'example-model',choices:[{message:{content:'done'},finish_reason:'stop'}],usage:{prompt_tokens:1,completion_tokens:2,prompt_tokens_details:{cached_tokens:5}}})}),id=await app.create();await app.check(id);const result=await app.chat();assert.equal(result.status,200);assert.equal(result.body.usage,null);assert.equal(app.store.get('SELECT usage_unknown FROM requests').usage_unknown,1);
});
test('upstream authentication messages cannot leak credentials or body',async t=>{
  const app=await fixture(t,{upstream:(req,res)=>{res.writeHead(401);res.end('synthetic-upstream-key private-upstream-text');}}),id=await app.create();const result=await app.check(id);assert.equal(result.status,401);assert.ok(!JSON.stringify(result).includes('synthetic-upstream-key'));assert.equal(app.store.get('SELECT state FROM accounts WHERE id=?',id).state,'offline');
});
test('credential replacement invalidates a late connection check',async t=>{
  let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);
  const app=await fixture(t,{transport:{models:async()=>{entered();await gate;return ['example-model'];}}}),id=await app.create();const checking=app.check(id);await started;
  const changed=await app.call('/api/admin/platform/accounts/'+id,'PATCH',{expected_epoch:0,credential:'synthetic-new-key'});assert.equal(changed.status,200);release();assert.equal((await checking).status,409);assert.equal(app.store.get('SELECT state FROM accounts WHERE id=?',id).state,'offline');
});
test('disable migrates platform bindings and in-flight completion cannot overwrite new route',async t=>{
  let release,entered;const gate=new Promise(resolve=>release=resolve),started=new Promise(resolve=>entered=resolve);
  const app=await fixture(t,{transport:{models:async()=>['example-model'],run:async()=>{entered();await gate;return {text:'done',input:1,output:1,cached:0,usageKnown:true};}}}),first=await app.create(),second=await app.create();await app.check(first);await app.check(second);
  const request=app.chat();await started;const original=app.store.get('SELECT * FROM sessions'),source=original.account_id,target=source===first?second:first;try{const disabled=await app.call('/api/admin/accounts/'+source,'PATCH',{enabled:false});assert.equal(disabled.status,200);assert.equal(disabled.body.moved,1);}finally{release();}assert.equal((await request).status,200);
  const latest=app.store.get('SELECT * FROM sessions WHERE id=?',original.id);assert.equal(latest.account_id,target);assert.equal(latest.route_version,original.route_version+1);assert.equal(app.store.get('SELECT account_id FROM requests').account_id,source);
});
test('WorkBuddy browser flow, polling, model catalog and streamed chat use real HTTP protocol',async t=>{
  let polls=0;
  const app=await fixture(t,{upstream:(req,res)=>{
    assert.equal(req.headers.accept,req.url==='/v2/chat/completions'?'text/event-stream':'application/json');
    if(req.url.includes('/auth/state'))return send(res,{code:0,data:{state:'synthetic-state',authUrl:'https://copilot.tencent.com/authorize?synthetic=1'}});
    if(req.url.includes('/auth/token?'))return send(res,++polls===1?{code:11217}:{code:0,data:{accessToken:'synthetic-access',refreshToken:'synthetic-refresh',expiresIn:3600}});
    if(req.url.includes('/login/account'))return send(res,{code:0,data:{uid:'synthetic-uid'}});
    if(req.url==='/v3/config')return send(res,{code:0,data:{models:[{id:'example-model'}]}});
    assert.equal(req.headers['x-user-id'],'synthetic-uid');res.writeHead(200,{'Content-Type':'text/event-stream'});res.end('data: '+JSON.stringify({model:'example-model',choices:[{index:0,delta:{content:'流式合成回复'},finish_reason:null}]})+'\n\ndata: '+JSON.stringify({choices:[{index:0,delta:{},finish_reason:'stop'}],usage:{prompt_tokens:10,completion_tokens:2}})+'\n\ndata: [DONE]\n\n');
  }}),id=await app.create('workbuddy');const login=await app.call('/api/admin/platform/accounts/'+id+'/login','POST',{});assert.equal(login.status,200);assert.equal(login.body.type,'browser');
  const deadline=Date.now()+3000;while(app.store.get('SELECT state FROM accounts WHERE id=?',id).state==='login'&&Date.now()<deadline)await new Promise(r=>setTimeout(r,5));assert.equal(app.store.get('SELECT state FROM accounts WHERE id=?',id).state,'ready');
  const result=await app.chat({model:'workbuddy/example-model'});assert.equal(result.status,200);assert.equal(result.body.choices[0].message.content,'流式合成回复');assert.equal(result.body.usage.total_tokens,12);
  const snapshot=JSON.stringify((await app.call('/api/admin/state')).body);assert.ok(!snapshot.includes('synthetic-access'));assert.ok(!snapshot.includes('synthetic-refresh'));assert.ok(!snapshot.includes('synthetic-state'));
});
test('R6 WorkBuddy refresh requests JSON while chat still requests SSE',async t=>{
  const app=await fixture(t,{upstream:(req,res)=>{
    if(req.url==='/v2/plugin/auth/token/refresh'){
      assert.equal(req.headers.accept,'application/json');assert.equal(req.headers['content-type'],'application/json');assert.equal(req.headers.authorization,undefined);assert.equal(req.headers['x-refresh-token'],'synthetic-refresh');assert.equal(req.headers['x-auth-refresh-source'],'plugin');return send(res,{code:0,data:{accessToken:'synthetic-renewed',refreshToken:'synthetic-new-refresh',expiresIn:3600}});
    }
    if(req.url==='/v3/config'){assert.equal(req.headers.accept,'application/json');assert.equal(req.headers.authorization,'Bearer synthetic-renewed');return send(res,{code:0,data:{models:[{id:'example-model'}]}});}
    throw Error('unexpected route');
  }}),id=await app.create('workbuddy');app.store.saveUpstreamKey(id,JSON.stringify({accessToken:'synthetic-old',refreshToken:'synthetic-refresh',expiresAt:0}));assert.equal((await app.check(id)).status,200);assert.equal(app.calls.length,2);
});
test('WorkBuddy canceled authorization cannot restore removed or disabled account',async t=>{
  let challenge,release;const entered=new Promise(r=>challenge=r),gate=new Promise(r=>release=r);
  const app=await fixture(t,{transport:{login:async({onChallenge})=>{onChallenge({type:'browser',verificationUrl:'https://copilot.tencent.com/authorize'});challenge();await gate;return {accessToken:'synthetic-late'};},models:async()=>['example-model']}}),id=await app.create('workbuddy');
  const login=app.call('/api/admin/platform/accounts/'+id+'/login','POST',{});await entered;await login;
  assert.equal((await app.call('/api/admin/platform/accounts/'+id+'/login/cancel','POST',{})).status,200);release();await new Promise(r=>setImmediate(r));assert.equal(app.store.get('SELECT state FROM accounts WHERE id=?',id).state,'offline');assert.equal(app.store.upstreamKey(app.store.get('SELECT * FROM accounts WHERE id=?',id)),null);
});
test('Qoder uses official model table and isolated, tool-disabled subprocess',async t=>{
  assert.throws(()=>qoderCatalog('Not logged in'));const listed=qoderCatalog('MODEL\nPerformance\nCustom model (custom-key)\n');assert.equal(Buffer.from(listed[1].slice(5),'base64url').toString(),'Custom model');
  const root=mkdtempSync(join(tmpdir(),'harbor-qoder-fixture-'));t.after(()=>rmSync(root,{recursive:true,force:true}));const binary=join(root,'qoder');
  writeFileSync(binary,`#!/usr/bin/env node\nconst a=process.argv.slice(2);if(!process.env.QODER_PERSONAL_ACCESS_TOKEN||process.env.QODER_EXPOSE_TOKEN_USAGE!=='1'||!a.includes('--strict-mcp-config')||!a.includes('--setting-sources')||!process.env.HOME.startsWith('/tmp/harbor-qoder-'))process.exit(4);if(a.includes('--list-models')){console.log('MODEL\\nPerformance');process.exit(0);}if(a[a.indexOf('--tools')+1]!==''||!a.includes('--no-session-persistence')||a[a.indexOf('--model')+1]!=='Performance')process.exit(5);process.stdin.resume();process.stdin.on('end',()=>console.log(JSON.stringify({result:'合成Qoder回复',usage:{input_tokens:12,output_tokens:3}})));\n`,{mode:0o700});
  const app=await fixture(t,{transport:createPlatformTransport({qoderBinary:binary})}),id=await app.create('qoder');assert.equal((await app.check(id)).status,200);const model=JSON.parse(app.store.get('SELECT platform_models FROM accounts WHERE id=?',id).platform_models)[0];const result=await app.chat({model:'qoder/'+model});assert.equal(result.status,200);assert.equal(result.body.usage.total_tokens,15);
});
test('R1 Qoder normalized total already includes cache reads and writes',()=>{
  assert.equal(qoderCounts({input_tokens:0,output_tokens:0,cache_read_input_tokens:0,cache_creation_input_tokens:0}).usageKnown,false);
  // Official 1.1.81 aU(): compatible prompt_tokens=100 with 30 cache hits.
  assert.deepEqual(qoderCounts({input_tokens:100,output_tokens:5,cache_read_input_tokens:30,cache_creation_input_tokens:0}),{input:100,output:5,cached:30,usageKnown:true});
  // Official eat() -> aU(): Anthropic plain=10, read=20, creation=30 -> total=60.
  assert.deepEqual(qoderCounts({input_tokens:60,output_tokens:5,cache_read_input_tokens:20,cache_creation_input_tokens:30}),{input:60,output:5,cached:20,usageKnown:true});
  assert.equal(qoderCounts({input_tokens:10,output_tokens:5,cache_read_input_tokens:20,cache_creation_input_tokens:30}).usageKnown,false);
  assert.equal(qoderCounts({input_tokens:10,output_tokens:5,cache_read_input_tokens:-1}).usageKnown,false);
});
test('R1 official normalized Qoder counters preserve exact USD and do not reject below the ceiling',async t=>{
  const root=mkdtempSync(join(tmpdir(),'harbor-qoder-inclusive-'));t.after(()=>rmSync(root,{recursive:true,force:true}));const binary=join(root,'qoder');
  writeFileSync(binary,`#!/usr/bin/env node\nif(process.argv.includes('--list-models')){console.log('MODEL\\nPerformance');process.exit(0);}process.stdin.resume();process.stdin.on('end',()=>console.log(JSON.stringify({result:'合成规范化计量',usage:{input_tokens:1000000,output_tokens:10,cache_read_input_tokens:300000,cache_creation_input_tokens:100000}})));\n`,{mode:0o700});
  const app=await fixture(t,{transport:createPlatformTransport({qoderBinary:binary})}),id=await app.create('qoder');await app.check(id);const model='qoder/'+JSON.parse(app.store.get('SELECT platform_models FROM accounts WHERE id=?',id).platform_models)[0];
  app.store.pricing.configure({expectedVersion:app.store.pricing.status().settings.version,manualRates:{[model]:{input:'1',cached:'0.5',output:'1',source:'合成验收费率'}}});app.store.memberLimits.save(app.member.memberId,{expected_version:0,enabled:true,rules:[{model,hours:24,limit_usd:'1.00'}]});
  const first=await app.chat({model});assert.equal(first.status,200);assert.equal(first.body.usage.prompt_tokens,1000000);assert.equal(first.body.harbor_standard_usage.total_exact,'0.85001');assert.equal(app.store.get('SELECT input FROM requests').input,1000000);
  assert.equal(app.store.memberLimits.status(app.member.memberId).usage[0].blocked,false);assert.equal((await app.chat({model})).status,200);assert.equal((await app.chat({model})).status,429);
});
test('R1 a Qoder result with masked zero usage cannot bypass a configured USD ceiling',async t=>{
  const root=mkdtempSync(join(tmpdir(),'harbor-qoder-masked-'));t.after(()=>rmSync(root,{recursive:true,force:true}));const binary=join(root,'qoder');
  writeFileSync(binary,`#!/usr/bin/env node\nif(process.argv.includes('--list-models')){console.log('MODEL\\nPerformance');process.exit(0);}process.stdin.resume();process.stdin.on('end',()=>console.log(JSON.stringify({result:'合成有输出但计量隐藏',usage:{input_tokens:0,output_tokens:0,cache_read_input_tokens:0,cache_creation_input_tokens:0}})));\n`,{mode:0o700});
  const app=await fixture(t,{transport:createPlatformTransport({qoderBinary:binary})}),id=await app.create('qoder');await app.check(id);const model='qoder/'+JSON.parse(app.store.get('SELECT platform_models FROM accounts WHERE id=?',id).platform_models)[0];
  app.store.pricing.configure({expectedVersion:app.store.pricing.status().settings.version,manualRates:{[model]:{input:'1',cached:'1',output:'1',source:'合成验收费率'}}});app.store.memberLimits.save(app.member.memberId,{expected_version:0,enabled:true,rules:[{model,hours:24,limit_usd:'1.00'}]});
  const result=await app.chat({model});assert.equal(result.status,200);assert.equal(result.body.usage,null);assert.equal(app.store.get('SELECT usage_unknown FROM requests').usage_unknown,1);assert.notEqual(result.body.harbor_standard_usage.state,'priced');
  const denied=await app.chat({model});assert.equal(denied.status,409);assert.equal(denied.body.error.type,'member_model_usage_unknown');
});
test('R3 unsupported Qoder arguments do not create unknown usage or block the next request',async t=>{
  let runs=0;const app=await fixture(t,{transport:{models:async()=>['example-model'],run:async()=>{runs++;return {text:'done',input:1,output:1,cached:0,usageKnown:true};}}}),id=await app.create('qoder');await app.check(id);
  app.store.pricing.configure({expectedVersion:app.store.pricing.status().settings.version,manualRates:{'qoder/example-model':{input:'1',cached:'1',output:'1',source:'合成验收费率'}}});
  app.store.memberLimits.save(app.member.memberId,{expected_version:0,enabled:true,rules:[{model:'qoder/example-model',hours:24,limit_usd:'1.00'}]});
  for(const args of [{temperature:1},{max_tokens:100}])assert.equal((await app.chat({model:'qoder/example-model',...args})).status,400);
  assert.equal(app.store.get('SELECT COUNT(*) n FROM requests').n,0);assert.equal((await app.chat({model:'qoder/example-model'})).status,200);assert.equal(runs,1);
});
test('R4 credential CAS is checked after body read and removed accounts cannot regain secrets',async t=>{
  const app=await fixture(t,{transport:{}}),id=await app.create(),url=new URL('http://synthetic/api/admin/platform/accounts/'+id);let first,second,third;
  const service=createPlatformService({store:app.store,runtime:app.runtime,body:req=>new Promise(resolve=>{if(req.label===1)first=resolve;else if(req.label===2)second=resolve;else third=resolve;}),json:(res,status)=>res.status=status,isStopping:()=>false});
  const a={},b={},c={},p1=service.admin({method:'PATCH',label:1},a,url),p2=service.admin({method:'PATCH',label:2},b,url);
  first({expected_epoch:0,credential:'synthetic-new'});await p1;second({expected_epoch:0,credential:'synthetic-stale'});await assert.rejects(p2,error=>error.status===409);
  assert.equal(a.status,200);assert.equal(JSON.parse(app.store.upstreamKey(app.store.get('SELECT * FROM accounts WHERE id=?',id))).token,'synthetic-new');
  const p3=service.admin({method:'PATCH',label:3},c,url);assert.equal((await app.call('/api/admin/accounts/'+id,'DELETE')).status,200);third({expected_epoch:1,credential:'synthetic-removed'});await assert.rejects(p3,error=>error.status===404);assert.equal(app.store.get('SELECT api_secret_cipher FROM accounts WHERE id=?',id).api_secret_cipher,null);
});
test('R5 platform accounts never invoke the legacy Codex quota reader',async t=>{
  let reads=0;const app=await fixture(t,{upstream:compatible,rateReader:async()=>{reads++;return null;}}),id=await app.create();await app.check(id);assert.equal((await app.call('/api/admin/quotas/refresh','POST',{})).status,200);assert.equal(reads,0);assert.equal(app.store.get('SELECT quota_error FROM accounts WHERE id=?',id).quota_error,null);
  app.store.run("INSERT INTO accounts(id,name,home,state) VALUES('legacy','合成订阅','unused','ready')");await app.call('/api/admin/quotas/refresh','POST',{});assert.equal(reads,1);
});
test('R2 cancellation and timeout kill a Qoder dispatcher and its inference descendant',{skip:process.platform==='win32'},async t=>{
  const root=mkdtempSync(join(tmpdir(),'harbor-qoder-tree-'));t.after(()=>rmSync(root,{recursive:true,force:true}));
  const binary=join(root,'dispatcher'),record=join(root,'record.json');
  writeFileSync(binary,`#!/usr/bin/env node\nconst {spawnSync}=require('node:child_process');spawnSync(process.execPath,['-e',${JSON.stringify("require('node:fs').writeFileSync("+JSON.stringify(record)+",JSON.stringify({pid:process.pid,home:process.env.HOME}));process.on('SIGTERM',()=>{});setInterval(()=>{},1000)")}],{stdio:'inherit'});`,{mode:0o700});
  async function exercise(timeout){
    const controller=new AbortController(),transport=createPlatformTransport({qoderBinary:binary,timeoutMs:timeout?1000:10000,killGraceMs:50}),started=Date.now();
    const pending=transport.run({kind:'qoder'},{token:'synthetic-pat'},{model:'qoder/name-'+Buffer.from('Performance').toString('base64url'),messages:payload.messages},controller.signal);const observed=pending.then(()=>({ok:true}),error=>({error}));
    const deadline=Date.now()+3000;let value;while(Date.now()<deadline){try{value=JSON.parse(readFileSync(record));break;}catch{await new Promise(r=>setTimeout(r,5));}}
    assert.ok(value,'inference descendant started');if(!timeout)controller.abort();const result=await observed;assert.ok(result.error);assert.ok(Date.now()-started<5000);
    const goneDeadline=Date.now()+2000;let alive=true;while(Date.now()<goneDeadline){try{const state=readFileSync('/proc/'+value.pid+'/stat','utf8').split(' ')[2];alive=state!=='Z';}catch{alive=false;}if(!alive)break;await new Promise(r=>setTimeout(r,5));}assert.equal(alive,false);assert.equal(existsSync(value.home),false);rmSync(record,{force:true});
  }
  await exercise(false);await exercise(true);
  const model='name-'+Buffer.from('Performance').toString('base64url'),app=await fixture(t,{transport:createPlatformTransport({qoderBinary:binary,killGraceMs:50})}),id=await app.create('qoder');
  app.store.run("UPDATE accounts SET state='ready',platform_models=? WHERE id=?",JSON.stringify([model]),id);const running=app.chat({model:'qoder/'+model});
  const deadline=Date.now()+3000;let value;while(Date.now()<deadline){try{value=JSON.parse(readFileSync(record));break;}catch{await new Promise(r=>setTimeout(r,5));}}assert.ok(value);await app.close();assert.equal((await running).status,499);assert.equal(existsSync(value.home),false);
});
test('WorkBuddy refresh is single-flight and revoked Key is rechecked before dispatch',async t=>{
  let release,entered,refreshes=0,runs=0;const gate=new Promise(r=>release=r),started=new Promise(r=>entered=r);
  const app=await fixture(t,{transport:{models:async()=>['example-model'],refresh:async value=>{refreshes++;entered();await gate;return {...value,accessToken:'synthetic-new',expiresAt:Date.now()+3600000};},run:async()=>{runs++;return {text:'done',input:1,output:1,cached:0,usageKnown:true};}}}),id=await app.create('workbuddy');
  app.store.saveUpstreamKey(id,JSON.stringify({accessToken:'synthetic-old',refreshToken:'synthetic-refresh',expiresAt:0}));app.store.run("UPDATE accounts SET state='ready',platform_models=? WHERE id=?",JSON.stringify(['example-model']),id);
  const first=app.chat({model:'workbuddy/example-model'});await started;const second=app.chat({model:'workbuddy/example-model'});
  const deadline=Date.now()+2000;while(app.store.get('SELECT COUNT(*) n FROM requests').n<2&&Date.now()<deadline)await new Promise(r=>setTimeout(r,5));
  try{assert.equal(app.store.get('SELECT COUNT(*) n FROM requests').n,2);app.store.run('UPDATE keys SET enabled=0 WHERE id=?',app.member.keyId);}finally{release();}
  const results=await Promise.all([first,second]);assert.deepEqual(results.map(r=>r.status),[403,403]);assert.equal(refreshes,1);assert.equal(runs,0);assert.ok(app.store.all('SELECT status FROM requests').every(r=>r.status==='rejected'));
});
