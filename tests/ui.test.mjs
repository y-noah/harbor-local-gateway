import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const snapshot=()=>({totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],daily:[],requests:[],busy:0,queue:0});
const response=(body,status=200)=>({ok:status<400,status,json:async()=>body});
async function ui(fetcher){
  const nodes=new Map(),handlers=new Map(),downloads=[],popups=[],timers=[],copied=[],intervals=[];
  const node=id=>{if(!nodes.has(id))nodes.set(id,{classList:{values:new Set(),add(v){this.values.add(v);},remove(v){this.values.delete(v);},toggle(){}},value:'',textContent:'',innerHTML:'',open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
  class BrowserURL extends URL{static createObjectURL(){return 'blob:test-only';}static revokeObjectURL(){}}
  const context=vm.createContext({URLSearchParams,navigator:{clipboard:{writeText:async value=>copied.push(value)}},document:{querySelector:node,querySelectorAll:()=>[],addEventListener(type,handler){handlers.set(type,handler);},createElement(){return {click(){downloads.push(this.download);}};},activeElement:{tagName:'BODY'}},URL:BrowserURL,window:{open(){const popup={closed:false,close(){this.closed=true;},location:{replace(url){popup.url=url;}}};popups.push(popup);return popup;}},sessionStorage:{removeItem(){}},location:{hash:''},setInterval(fn,delay){intervals.push({fn,delay});},setTimeout(fn,delay){const timer={fn,delay,active:true};timers.push(timer);return timer;},clearTimeout(timer){if(timer)timer.active=false;},fetch:async(path,options)=>path==='/api/auth/session'?response({authenticated:false}):fetcher(path,options),console});
  vm.runInContext(source,context);await new Promise(setImmediate);
  return {node,downloads,popups,timers,copied,intervals,click:(action,id='')=>handlers.get('click')({target:{closest:selector=>selector==='[data-action]'?{dataset:{action,id}}:null}}),run:code=>vm.runInContext(code,context)};
}

test('late admin refresh cannot reopen the console after logout',async()=>{
  let release;
  const page=await ui(path=>path==='/api/admin/state'?new Promise(resolve=>release=()=>resolve(response(snapshot()))):response({ok:true}));
  page.run('admin=true;state={};');
  const pending=page.run('refresh()');
  await page.node('#logout').onclick();
  assert.equal(page.node('#shell').classList.values.has('hidden'),true);
  release();await pending;
  assert.equal(page.node('#shell').classList.values.has('hidden'),true);
  assert.equal(page.run('state'),null);
});

test('employee list pairs every full Key with its expiry and copy action below the name',async()=>{
 const page=await ui(()=>response({ok:true}));
 page.run("admin=true;tab='members';state={..."+JSON.stringify(snapshot())+",members:[{id:'m',name:'员工<&>',enabled:1}],keys:[{id:'k1',member_id:'m',full_key:'synthetic-key-one',expires:1,enabled:1},{id:'k2',member_id:'m',full_key:'synthetic-key-two',expires:null,enabled:1}]};render();");
 const html=page.node('#content').innerHTML;assert.match(html,/员工&lt;&amp;&gt;/);assert.ok(html.indexOf('员工&lt;&amp;&gt;')<html.indexOf('synthetic-key-one'));
 assert.match(html,/synthetic-key-one/);assert.match(html,/synthetic-key-two/);assert.match(html,/已过期/);assert.match(html,/永久有效/);
 await page.click('copy-key','k2');assert.deepEqual(page.copied,['synthetic-key-two']);
});

test('late routing explanation cannot overwrite a dialog opened after closing it',async()=>{
 let release;const page=await ui(()=>new Promise(r=>release=()=>r(response({mode:'auto',accounts:[],configuration:{},basis:null}))));
 page.run('admin=true;');const pending=page.click('routing-explanation');await page.click('close');page.run("modal('<h2>另一个窗口</h2>')");release();await pending;
 assert.equal(page.node('#modal-content').innerHTML,'<h2>另一个窗口</h2>');
});

test('closing a request phase dialog prevents late polling results from restoring it',async()=>{
 let release;const page=await ui(()=>new Promise(r=>release=()=>r(response({id:'r',timeline:[]}))));
 page.run('admin=true;');const pending=page.click('request-detail','r');await page.click('close');release();await pending;
 assert.equal(page.node('#modal').open,false);assert.equal(page.node('#modal-content').innerHTML,'');assert.equal(page.run('requestDetailId'),'');
});

test('nightly history names skipped account groups and preserves heavy-session exclusion reasons',async()=>{
 const page=await ui(()=>response({ok:true}));
 const result={day:'2026-10-27',status:'completed',accounts:[{id:'a',name:'测试组A'},{id:'b',name:'测试组B'},{id:'c',name:'测试组C'}],moves:[],skipped:[{accounts:['a','b'],reason:'incomparable_quota'},{accounts:['c'],reason:'unknown_fixed_load'}],summaries:[{accounts:[],planning:{reason:'infeasible',prioritySessions:[{session_id:'heavy-one'}],heavySkipped:[{session_id:'heavy-two',reason:'cannot_fit'}]}}]};
 const html=page.run('nightlyHistory('+JSON.stringify(result)+')');assert.match(html,/测试组A \/ 测试组B/);assert.match(html,/测试组C/);assert.match(html,/额度窗口或套餐不可比较/);assert.match(html,/固定会话缺少明确模型费率或历史样本/);assert.match(html,/heavy-one/);assert.match(html,/heavy-two/);assert.match(html,/会话负载无法放入其他账号的容差范围/);
});

test('logout clears history cache and a pending history page cannot restore private data',async()=>{
 let release;const page=await ui(path=>path.startsWith('/api/admin/history')?new Promise(r=>release=()=>r(response({accounts:[],rows:[{id:'private-history'}],pagination:{page:1,pageSize:25,total:1,totalPages:1}}))):response({ok:true}));
 page.run('admin=true;state='+JSON.stringify({...snapshot(),keys:[]})+";tab='requests';");
 const pending=page.click('record-view','history');await page.node('#logout').onclick();release();await pending;
 assert.equal(page.run('historyData'),null);assert.equal(page.node('#content').innerHTML,'');assert.equal(page.run('recordView'),'usage');
});

test('expired admin session closes modal and allows password entry',async()=>{
  const page=await ui(()=>response({error:{message:'expired'}},401));
  page.run('admin=true;state={};');page.node('#modal').open=true;
  await assert.rejects(page.run("api('/api/admin/members','POST',{})"),/expired/);
  assert.equal(page.node('#modal').open,false);
  assert.equal(page.node('#login').classList.values.has('hidden'),false);
  assert.equal(page.node('#shell').classList.values.has('hidden'),true);
  assert.equal(page.run('admin'),false);
});

test('stale unauthorized response cannot revoke a newer login',async()=>{
  let release;
  const page=await ui(()=>new Promise(resolve=>release=()=>resolve(response({error:{message:'old expired session'}},401))));
  page.run('admin=true;state={};');
  const pending=page.run("api('/api/admin/state')");
  page.run('authEpoch++;admin=true;state={newLogin:true};showShell();');
  release();await assert.rejects(pending,/old expired session/);
  assert.equal(page.run('admin'),true);
  assert.equal(page.run('state.newLogin'),true);
  assert.equal(page.node('#shell').classList.values.has('hidden'),false);
});

test('late successful admin response cannot reveal a generated key after logout',async()=>{
  let release;
  const page=await ui(path=>path==='/api/admin/keys'?new Promise(resolve=>release=()=>resolve(response({secret:'test-only-generated-key'}))):response({ok:true}));
  page.run('admin=true;state={};');
  const pending=page.run("api('/api/admin/keys','POST',{}).then(r=>showSecret(r.secret))");
  await page.node('#logout').onclick();
  release();await assert.rejects(pending,/登录状态已变更/);
  assert.equal(page.node('#modal').open,false);
  assert.equal(page.node('#modal-content').innerHTML,'');
  assert.equal(page.node('#shell').classList.values.has('hidden'),true);
});

test('expired admin session removes employee key, conversation and private markup',async()=>{
  const page=await ui(()=>response({error:{message:'expired'}},401));
  page.run("admin=true;state={};testKey='test-only-key';chat=[{role:'user',content:'private prompt'}];chatSession='session';chatOwner='employee';draft='private draft';");
  page.node('#content').innerHTML='private stats';page.node('#modal-content').innerHTML='private key';
  await assert.rejects(page.run("api('/api/admin/state')"),/expired/);
  assert.equal(page.run('testKey+chatSession+chatOwner+draft'),'');
  assert.equal(page.run('chat.length'),0);
  assert.equal(page.node('#content').innerHTML,'');
  assert.equal(page.node('#modal-content').innerHTML,'');
  page.run("showSecret('late-key')");
  assert.equal(page.node('#modal').open,false);
});

test('pending chat completion cannot reopen a session-expired screen or restore its conversation',async()=>{
  let release;
  const page=await ui(path=>path==='/api/me'?response({id:'employee'}):path==='/v1/chat/completions'?new Promise(resolve=>release=()=>resolve(response({session_id:'session',choices:[{message:{content:'private reply'}}],usage:{total_tokens:2}}))):response({error:{message:'expired'}},401));
  page.run("admin=true;tab='playground';state={};");
  page.node('#test-key').value='test-only-key';page.node('#prompt').value='private prompt';
  const pending=page.run('sendChat({preventDefault(){}})');await new Promise(setImmediate);
  await assert.rejects(page.run("api('/api/admin/state')"),/expired/);
  release();await pending;
  assert.equal(page.run('chat.length'),0);
  assert.equal(page.run('chatSession+testKey+draft'),'');
  assert.equal(page.run('sending'),false);
  assert.equal(page.node('#shell').classList.values.has('hidden'),true);
});

test('quota lookup for a replaced key does not display the prior employee data',async()=>{
  let release;
  const page=await ui(()=>new Promise(resolve=>release=()=>resolve(response({name:'Previous employee',used:1,quota:null,keyUsed:1,keyQuota:null,reserved:0}))));
  page.run("tab='playground';");page.node('#test-key').value='old-test-key';
  const pending=page.run('checkKey()');
  page.node('#test-key').value='new-test-key';
  release();await pending;
  assert.equal(page.node('#my-quota').textContent,'');
});

test('closing the generated key dialog removes the plaintext key markup',async()=>{
  const page=await ui(()=>response({ok:true}));
  page.run("admin=true;modal('test-only-private-key');close();");
  assert.equal(page.node('#modal').open,false);
  assert.equal(page.node('#modal-content').innerHTML,'');
});

test('Escape dismisses the key dialog and clears its private content',async()=>{
  const page=await ui(()=>response({ok:true}));let prevented=false;
  page.run("admin=true;modal('test-only-private-key');");
  page.node('#modal').oncancel({preventDefault(){prevented=true;}});
  assert.equal(prevented,true);assert.equal(page.node('#modal').open,false);
  assert.equal(page.node('#modal-content').innerHTML,'');
});

test('CSV export does not download private statistics after logout',async()=>{
  let release;
  const page=await ui(path=>path==='/api/admin/export'?{ok:true,blob:()=>new Promise(resolve=>release=()=>resolve({}))}:response({ok:true}));
  page.run('admin=true;state={};');
  const pending=page.click('export');await new Promise(setImmediate);
  await page.node('#logout').onclick();
  release();await pending;
  assert.deepEqual(page.downloads,[]);
  assert.equal(page.node('#toast').textContent,'');
});

test('login shows a usable official link and code and opens the browser from the click',async()=>{
  const calls=[];const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/state'?{...snapshot(),accounts:[{id:'one',name:'Test',kind:'codex',state:'login',token_budget:null}]}:{type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY',expiresAt:Date.now()+600000});});
  page.run("admin=true;state={accounts:[{id:'one',name:'Test',kind:'codex'}]};tab='accounts';");
  await page.click('login-account','one');assert.equal(page.node('#modal').open,true);assert.equal(page.node('#device-code').textContent,'TEST-ONLY');assert.equal(page.node('#device-link').href,'https://auth.openai.com/codex/device');assert.equal(page.popups[0].url,'https://auth.openai.com/codex/device');assert.equal(page.popups[0].opener,null);
  assert.equal(calls[0][0],'/api/admin/accounts/one/login');assert.ok(page.timers.some(x=>x.delay===2000&&x.active));
  page.run('close()');assert.ok(!page.timers.some(x=>x.delay===2000&&x.active));assert.equal(page.node('#modal-content').innerHTML,'');
});
test('login failures close the blank popup and show the actual failure instead of a success toast',async()=>{
  const page=await ui(()=>response({error:{message:'设备授权不可用'}},502));page.run("admin=true;state={accounts:[{id:'one',name:'Test',kind:'codex'}]};");
  await page.click('login-account','one');assert.equal(page.popups[0].closed,true);assert.match(page.node('#modal-content').innerHTML,/设备授权不可用/);assert.ok(!page.node('#modal-content').innerHTML.includes('已启动'));
});
test('empty login responses show a usable HTTP error without exposing the JSON parser exception',async()=>{
  const page=await ui(()=>({ok:false,status:502,json:async()=>{throw SyntaxError('Unexpected end of JSON input');}}));
  page.run("admin=true;state={accounts:[{id:'one',name:'Test',kind:'codex'}]};");
  await page.click('login-account','one');
  assert.equal(page.popups[0].closed,true);
  assert.match(page.node('#modal-content').innerHTML,/HTTP 502/);
  assert.ok(!page.node('#modal-content').innerHTML.includes('Unexpected end'));
});
test('empty unauthorized responses still clear private data and return to password entry',async()=>{
  const page=await ui(()=>({ok:false,status:401,json:async()=>{throw SyntaxError('Unexpected end of JSON input');}}));
  page.run("admin=true;state={};testKey='private-test-key';modal('private-data');");
  await assert.rejects(page.run("api('/api/admin/state')"),/HTTP 401/);
  assert.equal(page.run('admin'),false);assert.equal(page.run('state'),null);
  assert.equal(page.run('testKey'),'');assert.equal(page.node('#modal-content').innerHTML,'');
});
test('remove requires confirmation and sends an authenticated DELETE without removing history locally',async()=>{
  const calls=[];const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/state'?snapshot():{ok:true});});
  page.run("admin=true;state={accounts:[{id:'one',name:'Test',kind:'codex'}]};");await page.click('remove-account','one');assert.equal(calls.length,0);assert.match(page.node('#modal-content').innerHTML,/历史用量/);
  await page.click('confirm-remove-account','one');assert.equal(calls[0][0],'/api/admin/accounts/one');assert.equal(calls[0][1].method,'DELETE');assert.equal(page.node('#modal').open,false);assert.match(page.node('#toast').textContent,/已移除/);
});
test('a login response arriving after logout cannot open the official page or reveal the code',async()=>{
  let release;const page=await ui(path=>path.endsWith('/login')?new Promise(resolve=>release=()=>resolve(response({type:'device',verificationUrl:'https://auth.openai.com/codex/device',userCode:'TEST-ONLY'}))):response({ok:true}));
  page.run("admin=true;state={accounts:[{id:'one',name:'Test',kind:'codex'}]};");const pending=page.click('login-account','one');await new Promise(setImmediate);await page.node('#logout').onclick();release();await pending;
  assert.equal(page.popups[0].closed,true);assert.equal(page.popups[0].url,undefined);assert.equal(page.node('#modal').open,false);assert.equal(page.node('#modal-content').innerHTML,'');
});
test('Key deletion requires confirmation, cancellation is safe and history remains visible',async()=>{
 const calls=[],data={...snapshot(),keys:[],members:[{id:'member',name:'Fixture',used:70,quota:null,reserved:0}]};
 const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/state'?data:path==='/api/admin/requests/request'?{id:'request',key_prefix:'fixture',key_removed_at:1,started:1,ended:2,timeline:[]}:{ok:true});});
 page.run("admin=true;state={keys:[{id:'key',member_id:'member',prefix:'fixture'}]};");
 await page.click('remove-key','key');assert.equal(calls.length,0);assert.match(page.node('#modal-content').innerHTML,/无法恢复/);await page.click('close');assert.equal(calls.length,0);
 await page.click('remove-key','key');await page.click('confirm-remove-key','key');assert.equal(calls[0][0],'/api/admin/keys/key');assert.equal(calls[0][1].method,'DELETE');assert.match(page.node('#toast').textContent,/已删除/);assert.ok(!page.node('#modal-content').innerHTML.includes('data-action="remove-key"'));
 page.run("state.requests=[{id:'request',key_id:'key',key_prefix:'fixture',key_removed_at:1,started:1,input:60,output:10,reserved:0}];");await page.click('request-detail','request');assert.match(page.node('#modal-content').innerHTML,/fixture…（已删除）/);
});
test('a late Key deletion response after logout cannot reopen employee details',async()=>{
 let release;const page=await ui(path=>path==='/api/admin/keys/key'?new Promise(r=>release=()=>r(response({ok:true}))):response(snapshot()));
 page.run("admin=true;state={keys:[{id:'key',member_id:'member'}]};");const pending=page.click('confirm-remove-key','key');await new Promise(setImmediate);await page.node('#logout').onclick();release();await pending;assert.equal(page.node('#modal').open,false);assert.equal(page.run('state'),null);
});

test('usage displays full Keys and reference USD without raw Token statistics',async()=>{
 const page=await ui(()=>response(snapshot()));page.run(`admin=true;state={members:[{id:'m',name:'Fixture',enabled:1}],keys:[{id:'k',member_id:'m',prefix:'hg_fixture',full_key:'hg_fixture_complete_only',standard:{total:0.000001,unmetered:0,unpriced:0},standard_12h:{total:0},enabled:1}]};`);
 const html=page.run('usageTable(usageRows())');assert.match(html,/hg_fixture_complete_only/);assert.match(html,/copy-key/);assert.match(html,/近7天参考消耗（USD）/);assert.match(html,/\$0\.000001/);assert.doesNotMatch(html,/亿 Token/);
 assert.equal(page.run('referenceMoney(3.3)'), '$3.30');assert.equal(page.run('referenceMoney(null)'), '—');assert.equal(page.run('referenceMoney(0.00000001)'), '<$0.000001');
 assert.match(page.run("keyDisplay({prefix:'old'})"),/下次成功调用后补全/);
});
test('stale official quotas remain visible with their age instead of disappearing',async()=>{
 const page=await ui(()=>response(snapshot()));
 const html=page.run(`quotaCard({quota_json:JSON.stringify({remaining:42,checked:Date.now()-600000,windows:[{used:58,minutes:300,reset:Date.now()-1000}]})})`);
 assert.match(html,/42/);assert.match(html,/上次官方额度/);assert.match(html,/待刷新/);assert.match(html,/当前余量待确认/);
});
test('session table exposes only routing metadata and hides normal binding status',async()=>{
 const page=await ui(()=>response(snapshot()));page.run(`admin=true;tab='sessions';state={sessions:[{id:'s',client_session_id:'01234567-89ab-7cde-8fab-0123456789ab',title:'<img src=x onerror=alert(1)>',enabled:1,protocol:'responses',upstream_thread:'forwarded-fixture',request_count:2,state:'active'}]};render();`);
 const html=page.node('#content').innerHTML;
 assert.match(html,/<th>Codex Session \/ 员工<\/th><th>绑定账号<\/th><th>近7天请求数<\/th><th>最近活动<\/th><th>操作<\/th>/);
 assert.match(html,/<td class="mono">01234567-89ab-7cde-8fab-0123456789ab<small>/);assert.ok(!html.includes('onerror'));assert.ok(!html.includes('会话名称'));assert.ok(!html.includes('forwarded-fixture'));assert.ok(!html.includes('已绑定'));assert.match(html,/管理会话/);
 await page.click('session-actions','s');assert.match(page.node('#modal-content').innerHTML,/<p class="mono muted">01234567-89ab-7cde-8fab-0123456789ab<\/p>/);assert.ok(!page.node('#modal-content').innerHTML.includes('onerror'));assert.ok(!page.node('#modal-content').innerHTML.includes('修改会话名称'));assert.match(page.node('#modal-content').innerHTML,/修改绑定账号/);assert.match(page.node('#modal-content').innerHTML,/禁止该会话访问/);
 page.run(`state.sessions[0].enabled=0;render();`);assert.doesNotMatch(page.node('#content').innerHTML,/已禁止访问/);await page.click('session-actions','s');assert.match(page.node('#modal-content').innerHTML,/已禁止访问/);
 await page.click('session-actions','s');assert.match(page.node('#modal-content').innerHTML,/恢复该会话访问/);assert.ok(!page.node('#modal-content').innerHTML.includes('修改绑定账号'));
});

test('missing client Session is explicit and never falls back to internal routing identifiers',async()=>{
 const page=await ui(()=>response(snapshot()));
 assert.equal(page.run("sessionDisplayId({id:'internal',upstream_thread:'upstream',protocol:'responses'})"),'未获取');
 assert.equal(page.run("sessionDisplayId({id:'internal',upstream_id:'upstream',protocol:'chat'})"),'旧 CLI 会话');
});

test('positive automatic weights are never displayed as zero or one hundred after rounding',async()=>{
 const page=await ui(()=>response(snapshot()));
 assert.equal(page.run('weightPercent(0.000001)'),'低于0.01%');assert.equal(page.run('weightPercent(99.999999)'),'高于99.99%');
 assert.equal(page.run('weightPercent(0)'),'0%');assert.equal(page.run('weightPercent(100)'),'100%');
});
test('temporary weight editor uses server version and offers explicit automatic restore',async()=>{
 const calls=[],data={...snapshot(),accounts:[{id:'a',name:'Account A',kind:'codex',enabled:1,state:'ready'},{id:'b',name:'Account B',kind:'codex',enabled:1,state:'ready'}],balancing:{settings:{weighted:true},weights:{mode:'manual',configuration:{mode:'manual',weights:{a:100,b:0},expires:1800000000000,version:7},accounts:[{id:'a',automatic:68.965,effective:100},{id:'b',automatic:31.035,effective:0}]}}};
 const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/state'?data:{ok:true});});
 page.run('admin=true;state='+JSON.stringify(data)+';');await page.click('routing-weights');
 const html=page.node('#modal-content').innerHTML;assert.match(html,/value="100"/);assert.match(html,/value="0"/);assert.match(html,/每天北京时间02:00恢复自动/);assert.match(html,/0权重不参与兜底/);assert.match(html,/订阅额度为0也可设置正权重/);
 await page.click('routing-auto');const saved=calls.find(([p])=>p==='/api/admin/balancing/weights');assert.deepEqual(JSON.parse(saved[1].body),{mode:'auto',expected_version:7});
});

test('recent usage renders both windows and exposes accessible estimate help',async()=>{
 const page=await ui(()=>response(snapshot()));
 page.run(`admin=true;state={...${JSON.stringify(snapshot())},keys:[{id:'key',member_id:'member',prefix:'hg_fixture',standard:{total:2,unmetered:12,unpriced:1},standard_12h:{total:0.3,unmetered:0,unpriced:0},enabled:1}],members:[{id:'member',name:'Fixture',enabled:1}],sessions:[]};tab='overview';render();`);
 assert.match(page.node('#content').innerHTML,/最近用量/);assert.match(page.node('#content').innerHTML,/aria-label="最近用量说明"/);
 assert.match(page.node('#content').innerHTML,/近12小时参考消耗（USD）/);assert.match(page.node('#content').innerHTML,/0\.30/);assert.doesNotMatch(page.node('#content').innerHTML,/近7天未计量请求/);
 await page.click('usage-help');assert.match(page.node('#modal-content').innerHTML,/缺计量/);assert.match(page.node('#modal-content').innerHTML,/不是实际扣款或订阅扣额/);
 page.run("tab='requests';render();");assert.match(page.node('#content').innerHTML,/最近用量/);
});
test('uncertain Responses warning appears only in session management, with temporary deadline',async()=>{
 const page=await ui(()=>response(snapshot()));
 page.run(`admin=true;state={...${JSON.stringify(snapshot())},sessions:[{id:'session',protocol:'responses',enabled:1,state:'uncertain',nightly_enabled:0,nightly_expires:Date.now()+1000,member_name:'Fixture',account_name:'Fixture account'}]};tab='sessions';render();`);
 assert.doesNotMatch(page.node('#content').innerHTML,/可继续 · 用量可能漏计/);await page.click('session-actions','session');
 assert.match(page.node('#modal-content').innerHTML,/可继续 · 用量可能漏计/);assert.match(page.node('#modal-content').innerHTML,/临时固定至/);assert.match(page.node('#modal-content').innerHTML,/02:00/);assert.match(page.node('#modal-content').innerHTML,/恢复自动调整/);
 page.run("state.sessions[0].enabled=0;render();");assert.doesNotMatch(page.node('#content').innerHTML,/已禁止访问/);await page.click('session-actions','session');assert.match(page.node('#modal-content').innerHTML,/已禁止访问/);
});

test('every session action cell contains only the management button, including disabled and legacy sessions',async()=>{
 const page=await ui(()=>response(snapshot())),sessions=[['active','responses',1],['uncertain','responses',1],['uncertain','chat',1],['active','responses',0],['new','responses',1]].map(([state,protocol,enabled],i)=>({id:'s'+i,state,protocol,enabled,member_name:'Fixture',account_name:'Fixture'}));
 page.run(`admin=true;state={...${JSON.stringify(snapshot())},sessions:${JSON.stringify(sessions)}};tab='sessions';render();`);
 const cells=[...page.node('#content').innerHTML.matchAll(/<td>((?:(?!<\/td>).)*)<\/td><\/tr>/g)].map(m=>m[1]);assert.equal(cells.length,5);
 for(const cell of cells)assert.match(cell,/^<button[^>]*data-action="session-actions"[^>]*>管理会话<\/button>$/);
 assert.doesNotMatch(page.node('#content').innerHTML,/最多100个/);
});
test('session pagination preserves pages on refresh and rejects late results from an earlier page',async()=>{
 const base={...snapshot(),sessions:[],sessionsPagination:{page:1,pageSize:25,total:130,totalPages:6}},pending=[],paths=[];
 const page=await ui(path=>{paths.push(path);return new Promise(resolve=>pending.push(resolve));});
 page.run(`admin=true;state=${JSON.stringify(base)};tab='sessions';render();`);
 const older=page.run('refresh()'),next=page.click('sessions-page','2');assert.match(paths.at(-1),/session_page=2/);assert.equal(page.run('sessionsLoading'),true);
 const size=paths.length;await page.click('sessions-page','3');assert.equal(paths.length,size,'paging is disabled during load');
 pending[1](response({...base,sessionsPagination:{...base.sessionsPagination,page:2}}));await next;pending[0](response(base));await older;
 assert.equal(page.run('state.sessionsPagination.page'),2);assert.equal(page.run('sessionsPage'),2);assert.equal(page.run('sessionsLoading'),false);
 const refreshed=page.run('refresh()');assert.match(paths.at(-1),/session_page=2/);pending[2](response({...base,sessionsPagination:{...base.sessionsPagination,page:2}}));await refreshed;
 const changed=page.node('#session-page-size').onchange({target:{value:'100'}});assert.match(paths.at(-1),/session_page=1&session_page_size=100/);pending[3](response({...base,sessionsPagination:{page:1,pageSize:100,total:130,totalPages:2}}));await changed;
 assert.equal(page.run('sessionsPageSize'),100);assert.equal(page.run('sessionsPage'),1);
});
test('failed page changes retain the last confirmed page and unlock pagination',async()=>{
 const page=await ui(()=>response({error:{message:'synthetic network failure'}},503));
 page.run(`admin=true;state={...${JSON.stringify(snapshot())},sessions:[],sessionsPagination:{page:2,pageSize:25,total:130,totalPages:6}};sessionsPage=2;tab='sessions';render();`);
 await page.click('sessions-page','3');assert.equal(page.run('sessionsPage'),2);assert.equal(page.run('state.sessionsPagination.page'),2);assert.equal(page.run('sessionsLoading'),false);assert.match(page.node('#toast').textContent,/synthetic network failure/);
});

test('pending pagination disables old row management and in-flight refresh preserves an open session dialog',async()=>{
 const pending=[],calls=[],s={id:'old-session',enabled:1,protocol:'responses',state:'active',member_name:'Fixture',account_name:'Fixture',nightly_enabled:1},base={...snapshot(),sessions:[s],sessionsPagination:{page:1,pageSize:25,total:50,totalPages:2}};
 const page=await ui((path,options)=>{calls.push([path,options]);if(path.startsWith('/api/admin/state'))return new Promise(resolve=>pending.push(resolve));return response({ok:true});});
 page.run(`admin=true;state=${JSON.stringify(base)};tab='sessions';render();`);
 const next=page.click('sessions-page','2');assert.match(page.node('#content').innerHTML,/data-action="session-actions" data-id="old-session" disabled/);await page.click('session-actions','old-session');assert.equal(page.node('#modal').open,false);
 pending[0](response({...base,sessions:[{...s,id:'new-session'}],sessionsPagination:{...base.sessionsPagination,page:2}}));await next;assert.equal(page.run('sessionsLoading'),false);
 await page.click('session-actions','new-session');const refresh=page.run('refresh()');pending[1](response({...base,sessions:[],sessionsPagination:{...base.sessionsPagination,page:1}}));await refresh;
 assert.equal(page.run('state.sessions[0].id'),'new-session');assert.equal(page.run('sessionsPage'),2);assert.match(page.node('#modal-content').innerHTML,/禁止该会话访问/);
 await page.click('toggle-session','new-session');assert.match(page.node('#modal-content').innerHTML,/禁止该会话访问？/);await page.click('close');
 const afterClose=page.run('refresh()');pending[2](response({...base,sessions:[],sessionsPagination:{...base.sessionsPagination,page:1}}));await afterClose;assert.equal(page.run('sessionsPage'),1);
});
test('balancing preview renders identifiers supplied by the preview even outside the current page',async()=>{
 const client='00000000-0000-4000-8000-000000000777';
 const page=await ui(()=>response({window:{end:Date.now()},summaries:[],skipped:[],moves:[{session_id:'not-on-page',from:'a',to:'b',protocol:'responses',client_session_id:client},{session_id:'missing',from:'a',to:'b',protocol:'responses',client_session_id:null}]}));
 page.run(`admin=true;state={...${JSON.stringify(snapshot())},sessions:[]};`);await page.click('balancing-preview');assert.match(page.node('#modal-content').innerHTML,new RegExp(client));assert.match(page.node('#modal-content').innerHTML,/未获取/);
});

test('request list combines filters with complete totals and discards stale or logged-out responses',async()=>{
 const result={summary:{requests:251,success:250,input:100,output:20,cached:5,unmetered:1,active:0,average_ms:1000},pagination:{page:1,pageSize:25,total:251,totalPages:11},rows:[],models:['alpha','beta'],members:[{id:'m1',name:'同名'},{id:'m2',name:'同名'}],byModel:[],byMember:[]};
 const pending=[];const page=await ui((path)=>path.startsWith('/api/admin/requests?')?new Promise(r=>pending.push({path,release:body=>r(response(body))})):response({ok:true}));
 page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';");
 const first=page.run('loadRequests()');assert.match(page.node('#content').innerHTML,/正在读取/);pending[0].release(result);await first;assert.match(page.node('#content').innerHTML,/251/);
 const older=page.run("requestsFilters.model='alpha';loadRequests()");const newer=page.run("requestsFilters.member='m1';loadRequests()");assert.match(pending[2].path,/member=m1&model=alpha/);pending[2].release({...result,summary:{...result.summary,requests:4}});await newer;pending[1].release(result);await older;assert.equal(page.run('requestsData.summary.requests'),4);
 const last=page.run('loadRequests()');await page.node('#logout').onclick();pending[3].release(result);await last;assert.equal(page.run('requestsData'),null);assert.equal(page.node('#content').innerHTML,'');
});
test('combined connection view preserves drafts and prevents employee access to admin instructions',async()=>{
 const page=await ui(()=>response({ok:true}));page.run('admin=true;state='+JSON.stringify({...snapshot(),endpoint:'http://localhost/v1'})+";tab='playground';draft='待发送';render();");
 assert.match(page.node('#content').innerHTML,/接入与测试/);await page.click('connection-view','guide');assert.match(page.node('#content').innerHTML,/Codex Windows/);assert.equal(page.run('draft'),'待发送');await page.click('connection-view','test');assert.match(page.node('#content').innerHTML,/待发送/);
 page.run('sending=true');await page.click('connection-view','guide');assert.equal(page.run('connectionView'),'test');page.run('sending=false;admin=false');await page.click('connection-view','guide');assert.equal(page.run('connectionView'),'test');assert.ok(!page.node('#content').innerHTML.includes('data-action="backup"'));
});
test('account disable asks for confirmation then shows full migration result',async()=>{
 const calls=[];const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/state'?{...snapshot(),accounts:[{id:'a',name:'A',kind:'codex',enabled:0}]}:{ok:true,moved:8,resetTextSessions:2});});
 page.run('admin=true;state='+JSON.stringify({...snapshot(),accounts:[{id:'a',name:'A',enabled:1}]})+";tab='accounts';");await page.click('toggle-account','a');assert.equal(calls.length,0);assert.match(page.node('#modal-content').innerHTML,/凌晨自动恢复/);await page.click('confirm-disable-account','a');assert.equal(JSON.parse(calls[0][1].body).enabled,false);assert.match(page.node('#toast').textContent,/迁移 8 个会话/);
});

test('a fixed API session created after its dialog closes cannot replace a newer dialog',async()=>{
 let release;const page=await ui(path=>path==='/api/admin/sessions'?new Promise(r=>release=()=>r(response({session_id:'synthetic-fixed-session',header:'X-Harbor-Session'}))):response({ok:true}));
 page.run('admin=true;state='+JSON.stringify({...snapshot(),members:[{id:'m',name:'Fixture',enabled:1}],accounts:[{id:'a',name:'API',auth_mode:'openai_api',enabled:1,state:'ready'}]})+";globalThis.FormData=class{get(){return 'm';}};");
 await page.click('create-fixed-session','a');const pending=page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});await page.click('close');page.run("modal('<h2>new dialog</h2>')");release();await pending;assert.equal(page.node('#modal-content').innerHTML,'<h2>new dialog</h2>');
});

test('automatic refresh is limited to live views and the unfiltered first request page',async()=>{
 const calls=[];const page=await ui(path=>{calls.push(path);return response(snapshot());});page.run('admin=true;state='+JSON.stringify(snapshot())+';');
 for(const tab of ['members','playground']){page.run('tab='+JSON.stringify(tab));await page.run('automaticRefresh()');}
 for(const record of ['usage','history']){page.run("tab='requests';recordView="+JSON.stringify(record));await page.run('automaticRefresh()');}
 page.run("tab='requests';recordView='requests';requestsFilters.page=2;");await page.run('automaticRefresh()');page.run("requestsFilters.page=1;requestsFilters.model='gpt-fixture'");await page.run('automaticRefresh()');assert.deepEqual(calls,[]);
 for(const tab of ['overview','accounts','sessions']){page.run('tab='+JSON.stringify(tab));await page.run('automaticRefresh()');}assert.equal(calls.length,3);assert.ok(calls.every(x=>x==='/api/admin/state'));
 page.run("tab='overview';document.hidden=true");await page.run('automaticRefresh()');page.run("document.hidden=false;document.activeElement.tagName='TEXTAREA'");await page.run('automaticRefresh()');page.run("document.activeElement.tagName='BODY'");page.node('#modal').open=true;await page.run('automaticRefresh()');assert.equal(calls.length,3);
});
test('a late automatic response cannot refresh a static page reached after the request began',async()=>{
 let release;const page=await ui(()=>new Promise(r=>release=()=>r(response({...snapshot(),busy:42}))));page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='overview';");const pending=page.run('automaticRefresh()');page.run("tab='members';render()");const before=page.node('#content').innerHTML;release();await pending;assert.equal(page.node('#content').innerHTML,before);assert.equal(page.run('state.busy'),0);assert.equal(page.run('autoRefreshPending'),false);
});
test('automatic latest-request refresh does not flash loading or replace unchanged rows; manual refresh remains available',async()=>{
 let release;const result={observedAt:1,summary:{requests:1,success:1,input:1,output:1,cached:0,unmetered:0,active:0,average_ms:1000},pagination:{page:1,pageSize:25,total:1,totalPages:1},rows:[],models:[],members:[],byModel:[],byMember:[]};
 const calls=[];const page=await ui(path=>{calls.push(path);return new Promise(r=>release=()=>r(response({...result,observedAt:2})));});page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';requestsData="+JSON.stringify(result)+";render();");const before=page.node('#content').innerHTML,pending=page.run('automaticRefresh()');assert.equal(page.node('#content').innerHTML,before);assert.ok(!page.node('#content').innerHTML.includes('正在读取'));release();await pending;assert.equal(page.node('#content').innerHTML,before);assert.ok(calls[0].startsWith('/api/admin/requests?'));assert.ok(!calls.includes('/api/admin/state'));
 page.run("recordView='history';historyData=null;render()");assert.equal(page.run('automaticRefreshScope()'),'');
});

test('request refresh restores online status after an outage without accepting late responses',async()=>{
 const data={summary:{active:0},pagination:{page:1,pageSize:25,total:0,totalPages:1},rows:[],models:[],members:[],byModel:[],byMember:[]};
 let failing=true,release;
 const page=await ui(()=>release===null?new Promise(r=>release=()=>r(response(data))):failing?response({error:{message:'synthetic outage'}},502):response(data));
 page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';requestsData="+JSON.stringify(data));
 await page.run('automaticRefresh()');assert.equal(page.node('#connection').textContent,'连接异常');
 failing=false;await page.run('automaticRefresh()');assert.equal(page.node('#connection').textContent,'● 服务在线');
 failing=true;await page.run('automaticRefresh()');failing=false;await page.run('loadRequests()');assert.equal(page.node('#connection').textContent,'● 服务在线');
 release=null;const pending=page.run('automaticRefresh()');page.run("tab='members';render()");page.node('#connection').textContent='new-page-status';release();await pending;
 assert.equal(page.node('#connection').textContent,'new-page-status');
});

test('account entry separates subscription and Key types and preserves independent repeated additions',async()=>{
 const created=[],page=await ui((path,options)=>{if(path==='/api/admin/accounts')created.push(JSON.parse(options.body));return response(path==='/api/admin/state'?snapshot():{id:'account-'+created.length});});
 page.run('admin=true;state='+JSON.stringify(snapshot())+';globalThis.FormData=class{get(k){return formValues[k]??null;}};');
 const submit=()=>page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});
 for(const name of ['API A','API B']){
  await page.click('create-api-account');assert.match(page.node('#modal-content').innerHTML,/接入类型/);assert.equal(page.node('#new-api-fields').disabled,false);assert.equal(page.node('#new-subscription-fields').disabled,true);assert.equal(page.node('#new-api-key').required,true);
  page.run('globalThis.formValues='+JSON.stringify({auth_mode:'openai_api',kind:'claude',name,api_key:'synthetic-'+name,api_weight:'0'}));await submit();
 }
 assert.equal(created.length,2);assert.notEqual(created[0].name,created[1].name);assert.ok(created.every(a=>a.auth_mode==='openai_api'&&a.kind==='codex'&&a.api_weight===0));
 await page.click('create-api-account');page.node('#new-account-auth').onchange({target:{value:'subscription'}});assert.equal(page.node('#new-api-fields').disabled,true);assert.equal(page.node('#new-subscription-fields').disabled,false);assert.equal(page.node('#new-api-key').required,false);
 page.run('formValues='+JSON.stringify({auth_mode:'subscription',kind:'claude',name:'Subscription C',api_key:'must-not-submit',api_weight:'80'}));await submit();assert.deepEqual(created[2],{name:'Subscription C',auth_mode:'subscription',kind:'claude'});
 await page.click('create-account');assert.equal(page.node('#new-api-fields').disabled,true);assert.equal(page.node('#new-subscription-fields').disabled,false);
});

const pricingState=()=>({seedCaptured:'2026-10-29',settings:{enabled:true,refreshHours:24,version:0,manualRates:{},aliases:[]},state:{active:'a'.repeat(64)},unpricedModels:[{model:'new-model',requests:2}],rates:[{model:'gpt-6.1-sol',input:'2',cached:'0.1',output:'10',manual:false}]});
test('late fee settings cannot replace a newer dialog and the settings view identifies missing prices without raw usage',async()=>{
 let release;const page=await ui(()=>new Promise(r=>release=()=>r(response(pricingState()))));page.run('admin=true;');const pending=page.click('pricing-settings');await page.click('close');page.run("modal('<h2>other</h2>')");release();await pending;assert.equal(page.node('#modal-content').innerHTML,'<h2>other</h2>');
 const html=page.run('pricingOverview('+JSON.stringify(pricingState())+')');assert.match(html,/new-model：2条/);assert.match(html,/缺价不按0/);assert.match(html,/Standard参考消耗USD/);assert.doesNotMatch(html,/亿 Token/);
});
test('fee alias removal identifies the exact mapping even after the server has reordered the list',async()=>{
 const a={name:'alias-a',target:'gpt-6.1-sol',from:1,to:null,source:'fixture A'},b={name:'alias-b',target:'gpt-6.1-sol',from:1,to:null,source:'fixture B'},calls=[];
 const state=pricingState();state.settings.aliases=[b,a];state.settings.version=1;
 const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/pricing'?state:snapshot());});page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='accounts';modal('<h2>pricing</h2>')");
 await page.click('pricing-alias-remove',JSON.stringify([a.name,a.target,a.from,a.to]));const sent=calls.find(([p,o])=>p==='/api/admin/pricing'&&o.method==='PATCH');assert.deepEqual(JSON.parse(sent[1].body),{expectedVersion:1,aliases:[b]});
});
test('every rendered reference amount comes from Standard data rather than legacy counters',async()=>{
 const page=await ui(()=>response(snapshot()));page.run('admin=true;state='+JSON.stringify({...snapshot(),members:[{id:'m',name:'Fixture',enabled:1}],keys:[{id:'k',member_id:'m',enabled:1,prefix:'synthetic',used:999999999999,used_12h:999999999999,standard:{total:null,unpriced:3},standard_12h:{total:0.58,unmetered:0,unpriced:0}}]})+";tab='requests';recordView='usage';render();");
 const html=page.node('#content').innerHTML;assert.match(html,/3条缺费率/);assert.match(html,/\$0\.58/);assert.doesNotMatch(html,/999999999999|亿 Token/);assert.match(html,/—/);
});
test('session management shows its own reference amount and coverage without borrowing account totals',async()=>{
 const page=await ui(()=>response({ok:true}));
 const sessions=[{id:'a',standard:{total:0.58}},{id:'b',standard:{total:3.3}},{id:'c',standard:{total:null,unpriced:1}}].map(s=>({...s,protocol:'responses',client_session_id:s.id,member_name:'Fixture',account_name:'Account',account_id:'account',enabled:1,nightly_enabled:1}));
 page.run('admin=true;state='+JSON.stringify({sessions,accounts:[{id:'account',standard:{total:3.88}}]})+';');
 page.run("sessionDetail('a')");assert.match(page.node('#modal-content').innerHTML,/\$0\.58/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/\$3\.88/);
 page.run("sessionDetail('b')");assert.match(page.node('#modal-content').innerHTML,/\$3\.30/);
 page.run("sessionDetail('c')");assert.match(page.node('#modal-content').innerHTML,/1条缺费率/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/\$0\.00/);
});

test('Key model selection preserves exact names, saved versions and ignores stale async dialogs',async()=>{
 const calls=[],state={...snapshot(),keys:[{id:'k',member_id:'m',allowed_models:['sol'],model_policy_version:7}]};
 const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/pricing'?pricingState():state);});
 page.run('admin=true;state='+JSON.stringify(state)+';globalThis.FormData=class{get(k){return formValues[k]??null;}};');
 await page.click('key-models','k');assert.match(page.node('#modal-content').innerHTML,/只允许指定模型/);assert.match(page.node('#modal-content').innerHTML,/gpt-6.1-sol/);
 page.node('[name="allowed_models"]').value='sol';page.node('#key-model-suggestion').onchange({target:{value:'gpt-6.1-sol'}});assert.equal(page.node('[name="allowed_models"]').value,'sol\ngpt-6.1-sol');assert.equal(page.node('[name="model_policy_mode"]').value,'limited');
 page.run('globalThis.formValues={model_policy_mode:"limited",allowed_models:"sol, astra"};');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});
 const sent=calls.find(([p,o])=>p==='/api/admin/keys/k'&&o.method==='PATCH');assert.deepEqual(JSON.parse(sent[1].body),{allowed_models:['sol','astra'],expected_model_policy_version:7});
 let release;const delayed=await ui(()=>new Promise(r=>release=()=>r(response(pricingState()))));delayed.run('admin=true;state='+JSON.stringify(state));const pending=delayed.click('key-models','k');await delayed.click('close');delayed.run("modal('<h2>new dialog</h2>')");release();await pending;assert.equal(delayed.node('#modal-content').innerHTML,'<h2>new dialog</h2>');
});
