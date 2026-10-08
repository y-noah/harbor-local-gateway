import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const snapshot=()=>({totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],daily:[],requests:[],busy:0,queue:0});
const response=(body,status=200)=>({ok:status<400,status,json:async()=>body});
async function ui(fetcher){
  const nodes=new Map(),handlers=new Map(),downloads=[],popups=[],timers=[];
  const node=id=>{if(!nodes.has(id))nodes.set(id,{classList:{values:new Set(),add(v){this.values.add(v);},remove(v){this.values.delete(v);},toggle(){}},value:'',textContent:'',innerHTML:'',open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
  class BrowserURL extends URL{static createObjectURL(){return 'blob:test-only';}static revokeObjectURL(){}}
  const context=vm.createContext({document:{querySelector:node,querySelectorAll:()=>[],addEventListener(type,handler){handlers.set(type,handler);},createElement(){return {click(){downloads.push(this.download);}};},activeElement:{tagName:'BODY'}},URL:BrowserURL,window:{open(){const popup={closed:false,close(){this.closed=true;},location:{replace(url){popup.url=url;}}};popups.push(popup);return popup;}},sessionStorage:{removeItem(){}},location:{hash:''},setInterval(){},setTimeout(fn,delay){const timer={fn,delay,active:true};timers.push(timer);return timer;},clearTimeout(timer){if(timer)timer.active=false;},fetch:async(path,options)=>path==='/api/auth/session'?response({authenticated:false}):fetcher(path,options),console});
  vm.runInContext(source,context);await new Promise(setImmediate);
  return {node,downloads,popups,timers,click:(action,id='')=>handlers.get('click')({target:{closest:selector=>selector==='[data-action]'?{dataset:{action,id}}:null}}),run:code=>vm.runInContext(code,context)};
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
 const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/state'?data:{ok:true});});
 page.run("admin=true;state={keys:[{id:'key',member_id:'member',prefix:'fixture'}]};");
 await page.click('remove-key','key');assert.equal(calls.length,0);assert.match(page.node('#modal-content').innerHTML,/无法恢复/);await page.click('close');assert.equal(calls.length,0);
 await page.click('remove-key','key');await page.click('confirm-remove-key','key');assert.equal(calls[0][0],'/api/admin/keys/key');assert.equal(calls[0][1].method,'DELETE');assert.match(page.node('#toast').textContent,/已删除/);assert.ok(!page.node('#modal-content').innerHTML.includes('data-action="remove-key"'));
 page.run("state.requests=[{id:'request',key_id:'key',key_prefix:'fixture',key_removed_at:1,started:1,input:60,output:10,reserved:0}];");await page.click('request-detail','request');assert.match(page.node('#modal-content').innerHTML,/fixture…（已删除）/);
});
test('a late Key deletion response after logout cannot reopen employee details',async()=>{
 let release;const page=await ui(path=>path==='/api/admin/keys/key'?new Promise(r=>release=()=>r(response({ok:true}))):response(snapshot()));
 page.run("admin=true;state={keys:[{id:'key',member_id:'member'}]};");const pending=page.click('confirm-remove-key','key');await new Promise(setImmediate);await page.node('#logout').onclick();release();await pending;assert.equal(page.node('#modal').open,false);assert.equal(page.run('state'),null);
});

test('usage displays full Keys and seven-day billion units with exactly two decimals',async()=>{
 const page=await ui(()=>response(snapshot()));page.run(`admin=true;state={members:[{id:'m',name:'Fixture',enabled:1}],keys:[{id:'k',member_id:'m',prefix:'hg_fixture',full_key:'hg_fixture_complete_only',used:1,enabled:1,unmetered:0}]};`);
 const html=page.run('usageTable(usageRows())');assert.match(html,/hg_fixture_complete_only/);assert.match(html,/copy-key/);assert.match(html,/近7天 Token（亿）/);assert.match(html,/0\.00/);
 assert.equal(page.run('tokenBillions(123456789)'), '1.23');assert.equal(page.run('tokenBillions(100000000)'), '1.00');
 assert.match(page.run("keyDisplay({prefix:'old'})"),/下次成功调用后补全/);
});
test('stale official quotas remain visible with their age instead of disappearing',async()=>{
 const page=await ui(()=>response(snapshot()));
 const html=page.run(`quotaCard({quota_json:JSON.stringify({remaining:42,checked:Date.now()-600000,windows:[{used:58,minutes:300,reset:Date.now()-1000}]})})`);
 assert.match(html,/42/);assert.match(html,/上次官方额度/);assert.match(html,/待刷新/);assert.match(html,/当前余量待确认/);
});
test('Responses session presentation uses forwarded thread identity instead of an uncreated CLI Session',async()=>{
 const page=await ui(()=>response(snapshot()));page.run(`admin=true;tab='sessions';state={sessions:[{id:'s',protocol:'responses',upstream_thread:'forwarded-fixture',request_count:2,state:'active'}]};render();`);
 assert.match(page.node('#content').innerHTML,/forwarded-fixture/);assert.match(page.node('#content').innerHTML,/Responses 转发线程/);assert.ok(!page.node('#content').innerHTML.includes('尚未创建'));
});
