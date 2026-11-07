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
  const context=vm.createContext({AbortController,URLSearchParams,navigator:{clipboard:{writeText:async value=>copied.push(value)}},document:{querySelector:node,querySelectorAll:()=>[],addEventListener(type,handler){handlers.set(type,handler);},createElement(){return {click(){downloads.push(this.download);}};},activeElement:{tagName:'BODY'}},URL:BrowserURL,window:{open(){const popup={closed:false,close(){this.closed=true;},location:{replace(url){popup.url=url;}}};popups.push(popup);return popup;}},sessionStorage:{removeItem(){}},location:{hash:''},setInterval(fn,delay){intervals.push({fn,delay});},setTimeout(fn,delay){const timer={fn,delay,active:true};timers.push(timer);return timer;},clearTimeout(timer){if(timer)timer.active=false;},fetch:async(path,options)=>path==='/api/auth/session'?response({authenticated:false}):fetcher(path,options),console});
  vm.runInContext(source,context);await new Promise(setImmediate);
  return {node,downloads,popups,timers,copied,intervals,click:(action,id='')=>handlers.get('click')({target:{closest:selector=>selector==='[data-action]'?{dataset:{action,id}}:null}}),run:code=>vm.runInContext(code,context)};
}

const handoffSnapshot=()=>({...snapshot(),capabilities:{sessionHandoffs:true},keys:[],sessions:[{id:'session',client_session_id:'synthetic-client',protocol:'responses',member_name:'合成员工',account_id:'a',account_name:'合成账号A',route_version:0,enabled:1,nightly_enabled:1,state:'active',standard:{total:1.25}}],accounts:[{id:'a',name:'合成账号A',kind:'codex',auth_mode:'subscription',enabled:1,state:'ready'},{id:'b',name:'合成账号B',kind:'codex',auth_mode:'subscription',enabled:1,state:'ready'},{id:'c',name:'合成账号C',kind:'codex',auth_mode:'openai_api',enabled:1,state:'ready'}]});
const handoffRecord=(patch={})=>({id:'handoff-one',session_id:'session',status:'waiting',phase:'waiting',origin:'manual',source_account_id:'a',source_account_name:'合成账号A',target_account_id:'b',target_account_name:'合成账号B',requested:1,expires:1000000,reason:null,binding:{account_id:'a',account_name:'合成账号A',route_version:0,protocol:'responses',enabled:1,auth_mode:'subscription',nightly_enabled:1,nightly_expires:null},...patch});
const prepareHandoff=page=>page.run('admin=true;state='+JSON.stringify(handoffSnapshot())+';tab="sessions";render();');
const handoffPoll=async page=>{page.intervals.find(timer=>timer.delay===4000).fn();await new Promise(setImmediate);};

const batchRecord=(patch={})=>({id:'synthetic-batch',created:Date.now(),expires:Date.now()+3600000,status:'preview',direction:'apply',counts:{pending:1},items:[{ordinal:1,member_id:'synthetic-member',member_name:'合成批次员工',before_config:{usage_type_id:'usage-a',default_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]},keys:[]},desired_config:{usage_type_id:'usage-b',default_models:['gpt-6-astra'],usage_limits:{enabled:false,rules:[]},keys:[]},exceptions:[],status:'pending'}],after:1,has_more:false,...patch});
test('configuration batch late reads cannot replace another dialog or restart polling after close',async()=>{
  let release,reads=0;const page=await ui(()=>{reads++;return new Promise(resolve=>{release=()=>resolve(response(batchRecord({status:'queued'})));});});page.run('admin=true;state='+JSON.stringify(snapshot()));const pending=page.click('member-batch-open','synthetic-batch');await new Promise(setImmediate);await page.click('close');page.run('modal("another dialog")');release();await pending;await handoffPoll(page);assert.equal(reads,1);assert.equal(page.node('#modal-content').innerHTML,'another dialog');assert.equal(page.run('memberBatchView'),null);
});
test('configuration batch polling runs only for its visible queued result and retries submit only the fixed batch',async()=>{
  let reads=0,writes=[];const page=await ui((path,options)=>{if(options.method==='POST'){writes.push({path,body:JSON.parse(options.body)});return response({id:'synthetic-batch',status:'queued'});}reads++;return response(batchRecord({status:reads===1?'preview':'queued'}));});page.run('admin=true;state='+JSON.stringify(snapshot()));await page.click('member-batch-open','synthetic-batch');await handoffPoll(page);assert.equal(reads,1);await page.click('member-batch-execute');assert.deepEqual(writes,[{path:'/api/admin/member-config-batches/synthetic-batch/execute',body:{}}]);const afterSubmit=reads;await handoffPoll(page);assert.equal(reads,afterSubmit+1);await page.click('close');await handoffPoll(page);assert.equal(reads,afterSubmit+1);
});
test('configuration batch refresh errors preserve old preview and disable executing until a fresh read succeeds',async()=>{
  let reads=0,writes=0;const page=await ui((_path,options)=>{if(options.method==='POST'){writes++;return response({});}return ++reads===1?response(batchRecord()):response({error:{message:'synthetic query failed'}},503);});page.run('admin=true;state='+JSON.stringify(snapshot()));await page.click('member-batch-open','synthetic-batch');await page.click('member-batch-refresh');assert.match(page.node('#member-batch-content').innerHTML,/上次快照/);assert.match(page.node('#member-batch-content').innerHTML,/data-action="member-batch-execute"[^>]+disabled/);await page.click('member-batch-execute');assert.equal(writes,0);
});

test('R41 completed batch refreshes employee defaults and new Key rechecks them without sending unlimited',async()=>{
  let reads=0,stateReads=0,sent;const old={...snapshot(),capabilities:{memberTypes:true},members:[{id:'member',name:'旧员工',default_models_set:0,defaults_version:0}],keys:[]},updated={...old,members:[{...old.members[0],usage_type_id:'usage-a',usage_type_code:'A',usage_type_name:'复杂研发',default_models_set:1,defaults_version:3,default_models:['gpt-6-astra']}]};
  const page=await ui((path,options)=>{if(path.startsWith('/api/admin/state')){stateReads++;return response(stateReads===1?old:updated);}if(path==='/api/admin/keys'){sent=JSON.parse(options.body);return response({secret:'synthetic-new-key'});}if(options.method==='POST')return response({id:'synthetic-batch',status:'queued'});return response(batchRecord({status:++reads===1?'preview':reads===2?'queued':'completed'}));});page.run('admin=true;state='+JSON.stringify(old)+';tab="members";');await page.click('member-batch-open','synthetic-batch');await page.click('member-batch-execute');await handoffPoll(page);assert.equal(page.run('state.members[0].default_models_set'),1);await page.click('close');await page.click('new-key','member');assert.ok(stateReads>=3);assert.match(page.node('#modal-content').innerHTML,/沿用员工已生效默认/);page.run('globalThis.FormData=class{get(key){return {days:"0",key_model_source:"inherit"}[key];}}');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(Object.hasOwn(sent,'allowed_models'),false);assert.equal(sent.expected_defaults_version,3);
});

test('R44 failed terminal employee refresh remains unsynced and manual result refresh retries authoritative state',async()=>{
  let stateReads=0;const old={...snapshot(),capabilities:{memberTypes:true},members:[{id:'member',name:'旧缓存员工',default_models_set:0,defaults_version:0}],keys:[]},updated={...old,members:[{...old.members[0],default_models_set:1,defaults_version:3,default_models:['gpt-6-astra']}]};const page=await ui(path=>path.startsWith('/api/admin/state')?(++stateReads===1?response({error:{message:'temporary unavailable'}},503):response(updated)):response(batchRecord({status:'completed',counts:{success:1}})));page.run('admin=true;state='+JSON.stringify(old)+';tab="members";');await page.click('member-batch-open','synthetic-batch');assert.notEqual(page.run('memberBatchView.synced'),true);assert.match(page.node('#member-batch-content').innerHTML,/未同步/);assert.equal(page.run('state.members[0].defaults_version'),0);await page.click('member-batch-refresh');assert.equal(stateReads,2);assert.equal(page.run('memberBatchView.synced'),true);assert.equal(page.run('memberBatchView.error'),'');assert.equal(page.run('state.members[0].defaults_version'),3);
});
test('R44 new Key does not use stale employee defaults when its authoritative pre-read fails',async()=>{
  const page=await ui(()=>response({error:{message:'temporary unavailable'}},503)),old={...snapshot(),capabilities:{memberTypes:true},members:[{id:'member',name:'旧缓存员工',default_models_set:0,defaults_version:0}],keys:[]};page.run('admin=true;state='+JSON.stringify(old)+';tab="members";modal("original employee detail")');await page.click('new-key','member');assert.equal(page.node('#modal-content').innerHTML,'original employee detail');assert.match(page.node('#toast').textContent,/temporary unavailable/);
});

const pagedSnapshot=()=>({...snapshot(),capabilities:{memberTypes:true,pagedMembers:true},memberCount:2,keys:[],sessions:[]});
const memberPage=(id='member-one',patch={})=>({rows:[{id,name:'合成分页员工',enabled:1,created:1,usage_type_id:'usage-a',usage_type_code:'A',usage_type_name:'复杂研发',usage_type_version:1,default_models:null,default_models_set:1,defaults_version:2,configuration_source:{models:{kind:'type',type_id:'usage-a',template_version:1},limits:{kind:'type',type_id:'usage-a',template_version:1}},usage_limits:{enabled:false,rules:[],version:1},limit_usage:[],key_count:1,key_exceptions:0}],types:[{id:'usage-a',code:'A',name:'复杂研发',description:'复杂研发任务',enabled:1}],pagination:{page:1,pageSize:25,total:2,totalPages:2},...patch});
test('paged employee late list cannot overwrite a different tab and selection is by stable ID across pages',async()=>{
  let release;const page=await ui(path=>path.includes('page=2')?new Promise(resolve=>{release=()=>resolve(response(memberPage('member-two',{pagination:{page:2,pageSize:25,total:2,totalPages:2}})));}):response(memberPage()));page.run('admin=true;state='+JSON.stringify(pagedSnapshot())+';tab="members";');await page.run('loadMembers()');await page.click('members-toggle-select','member-one');assert.equal(page.run('memberSelection.size'),1);const pending=page.click('members-page','2');await new Promise(setImmediate);page.run('setTab("accounts")');release();await pending;assert.equal(page.run('tab'),'accounts');assert.equal(page.run('membersData.rows[0].id'),'member-one');assert.equal(page.run('memberSelection.has("member-one")'),true);
});
test('paged employee search clears selections and all-filter selection cannot apply after filter changes',async()=>{
  let release;const page=await ui(path=>path.includes('/selection')?new Promise(resolve=>{release=()=>resolve(response({members:[{id:'member-one',usage_type_version:1,defaults_version:2}]}));}):response(memberPage()));page.run('admin=true;state='+JSON.stringify(pagedSnapshot())+';tab="members";');await page.run('loadMembers()');await page.click('members-toggle-select','member-one');const pending=page.click('members-select-all');await new Promise(setImmediate);page.node('#members-search').oninput({target:{value:'changed'}});assert.equal(page.run('memberSelection.size'),0);release();await pending;assert.equal(page.run('memberSelection.size'),0);assert.equal(page.run('memberFilters.page'),1);
});
test('paged member detail late credentials cannot replace another dialog',async()=>{
  let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response({member:memberPage().rows[0],rows:[],pagination:{page:1,pageSize:25,total:0,totalPages:1}}));}));page.run('admin=true;state='+JSON.stringify(pagedSnapshot())+';tab="members";');const pending=page.click('member-detail','member-one');await new Promise(setImmediate);await page.click('close');page.run('modal("another dialog")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another dialog');
});
test('总览只读紧凑状态，进入使用数据后才分页读取Key用量',async()=>{
  const paths=[],state={...pagedSnapshot(),accounts:[],daily:[],requests:[],busy:0,queue:0,totals:{requests:0,success:0}};const page=await ui(path=>{paths.push(path);return response(path.startsWith('/api/admin/state')?state:{rows:[],pagination:{page:1,pageSize:25,total:0,totalPages:1}});});page.run('admin=true;state='+JSON.stringify(state)+';tab="overview";');assert.equal(await page.run('refresh()'),true);assert.ok(paths.includes('/api/admin/state?people=none'));assert.ok(!paths.some(path=>path.startsWith('/api/admin/keys?')));assert.deepEqual(page.run('state.keys'),[]);page.run('setTab("usage")');await new Promise(setImmediate);assert.equal(page.run('tab'),'sessions');assert.ok(!paths.some(path=>path.startsWith('/api/admin/keys?')));await page.click('record-view','usage');await new Promise(setImmediate);assert.ok(paths.some(path=>path.startsWith('/api/admin/keys?')));assert.equal(page.run('tab'),'requests');
});

const deferredKeyUsage=name=>({id:'synthetic-key-'+name,member_id:'member-'+name,member_name:name,member_enabled:1,prefix:'hg_test_'+name,enabled:1,expires:null,credentials_deferred:true});
test('R45 changing a paged usage filter immediately removes the previous range while its response is delayed',async()=>{
  let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response({rows:[deferredKeyUsage('Bob')],pagination:{page:1,pageSize:25,total:1,totalPages:1}}));}));page.run('admin=true;state='+JSON.stringify(pagedSnapshot())+';tab="requests";recordView="usage";keyUsageScope="requests:";keyUsageData='+JSON.stringify({rows:[deferredKeyUsage('Alice')],pagination:{page:1,pageSize:25,total:1,totalPages:1}})+';render();');assert.match(page.node('#content').innerHTML,/Alice/);page.node('#request-filter').onchange({target:{value:'bob'}});await new Promise(setImmediate);assert.doesNotMatch(page.node('#content').innerHTML,/Alice/);assert.match(page.node('#content').innerHTML,/正在读取/);release();await new Promise(setImmediate);assert.match(page.node('#content').innerHTML,/Bob/);
});
test('R46 paged key usage displays the server page for case-insensitive and trimmed searches',async()=>{
  const page=await ui(()=>response({}));page.run('admin=true;state='+JSON.stringify(pagedSnapshot())+';tab="requests";recordView="usage";requestFilter=" alice ";keyUsageScope="requests: alice ";keyUsageData='+JSON.stringify({rows:[deferredKeyUsage('Alice')],pagination:{page:1,pageSize:25,total:1,totalPages:1}})+';render();');assert.match(page.node('#content').innerHTML,/Alice/);assert.match(page.node('#content').innerHTML,/共 1 个 Key/);
});

const typeHistory=()=>({type:{id:'usage-a',code:'A',name:'复杂研发',version:2,default_version:2},items:[{version:1,created:1,defaults:{allowed_models:['gpt-6.1-sol'],usage_limits:{enabled:false,rules:[]}}}],after:1,has_more:false});
test('type template history late reads cannot replace another dialog',async()=>{
  let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response(typeHistory()));}));page.run('admin=true;state='+JSON.stringify(snapshot()));const pending=page.click('type-history','usage-a');await new Promise(setImmediate);await page.click('close');page.run('modal("another form")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another form');
});
test('template restore submits only the frozen version and keeps conflict preview for rereading',async()=>{
  let sent;const page=await ui((path,options)=>{if(options.method==='PATCH'){sent=JSON.parse(options.body);return response({error:{message:'使用类型配置已变化'}},409);}return response(typeHistory());});page.run('admin=true;state='+JSON.stringify(snapshot()));await page.click('type-history','usage-a');await page.click('type-history-restore','1');page.run('globalThis.FormData=class{}');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.deepEqual(sent,{expected_version:2,restore_version:1});assert.match(page.node('#form-error').textContent,/已变化/);assert.match(page.node('#modal-content').innerHTML,/重新读取历史并核对/);
});

test('type restore late conflict cannot write an error into another dialog',async()=>{
  let release;const page=await ui((path,options)=>options.method==='PATCH'?new Promise(resolve=>{release=()=>resolve(response({error:{message:'stale version'}},409));}):response(typeHistory()));page.run('admin=true;state='+JSON.stringify(snapshot()));await page.click('type-history','usage-a');await page.click('type-history-restore','1');page.run('globalThis.FormData=class{}');const pending=page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});await new Promise(setImmediate);await page.click('close');page.run('modal("another form")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another form');assert.equal(page.node('#form-error').textContent,'');
});

const typeCatalog=()=>({types:[{id:'usage-a',code:'A',name:'复杂研发',description:'适合复杂研发任务',enabled:1,version:0,default_version:null,configured:false,defaults:null,members:0}]});
test('usage type late reads cannot replace another dialog after closing',async()=>{
  let release;const page=await ui(path=>path==='/api/admin/member-types'?new Promise(resolve=>{release=()=>resolve(response(typeCatalog()));}):response({rates:[{model:'gpt-6.1-sol'}]}));page.run('admin=true;state='+JSON.stringify(snapshot()));const pending=page.click('member-types');await new Promise(setImmediate);await page.click('close');page.run('modal("another form")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another form');assert.equal(page.run('typeController'),null);
});
test('unconfigured type editor requires explicit models and limits and ignores stale save errors',async()=>{
  let release;const page=await ui((path,options)=>options.method==='PATCH'?new Promise(resolve=>{release=()=>resolve(response({error:{message:'stale type version'}},409));}):response(path==='/api/admin/member-types'?typeCatalog():{rates:[{model:'gpt-6.1-sol'}]}));page.run('admin=true;state='+JSON.stringify(snapshot()));await page.click('edit-member-type','usage-a');assert.equal(page.node('[name="model_policy_mode"]').value,'');assert.equal(page.node('[name="type_limits_enabled"]').value,'');page.run('globalThis.FormData=class{get(key){return {model_policy_mode:"limited",allowed_models:"gpt-6.1-sol",type_limits_enabled:"false"}[key];}getAll(){return [];}}');const pending=page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});await new Promise(setImmediate);await page.click('close');page.run('modal("another form")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another form');assert.equal(page.node('#form-error').textContent??'','');
});
test('new Key using an employee applied default omits a permission override instead of sending unlimited',async()=>{
  let sent;const state={...snapshot(),members:[{id:'member',name:'合成员工',default_models_set:1,default_models:['gpt-6.1-sol'],defaults_version:2}],keys:[]};const page=await ui((path,options)=>{if(options.method==='POST'){sent=JSON.parse(options.body);return response({secret:'synthetic-key'});}return response(state);});page.run('admin=true;state='+JSON.stringify(state));await page.click('new-key','member');assert.match(page.node('#modal-content').innerHTML,/沿用员工已生效默认/);page.run('globalThis.FormData=class{get(key){return {key_model_source:"inherit",days:"0"}[key];}}');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(sent.memberId,'member');assert.equal(Object.hasOwn(sent,'allowed_models'),false);
});

test('scheduled transfer settings show actual times and retain explicit 02:00 expiry with legacy compatibility',async()=>{
  for(const enabled of [false,true]){
    const page=await ui(()=>response({})),state={...snapshot(),balancing:{settings:{nightly:enabled,quietMinutes:30},transfers:{times:['02:00','10:00','13:00','16:00','20:00'],version:2,next:Date.parse('2026-11-03T13:00:00+08:00')}}};page.run('admin=true;state='+JSON.stringify(state));await page.click('balancing-settings');assert.match(page.node('#modal-content').innerHTML,/02:00、10:00、13:00、16:00、20:00/);assert.match(page.node('#modal-content').innerHTML,/临时权重与固定仍在02:00到期/);assert.match(page.node('#modal-content').innerHTML,/data-action="transfer-schedule"/);await page.click('transfer-schedule');assert.match(page.node('#modal-content').innerHTML,/Asia\/Shanghai/);assert.match(page.node('#modal-content').innerHTML,enabled?/自动调度已开启/:/保存时点不会开启调度/);
  }
  const legacy=await ui(()=>response({}));legacy.run('admin=true;state='+JSON.stringify({...snapshot(),balancing:{settings:{nightly:false,quietMinutes:30}}}));await legacy.click('balancing-settings');assert.doesNotMatch(legacy.node('#modal-content').innerHTML,/data-action="transfer-schedule"/);assert.match(legacy.node('#modal-content').innerHTML,/北京时间每日 02:00 调整/);
});

test('scheduled transfer editor saves normalized input with version and ignores completion after another dialog opens',async()=>{
  let release,sent;const page=await ui((path,options)=>{sent={path,body:JSON.parse(options.body)};return new Promise(resolve=>{release=()=>resolve(response({version:3}));});});page.run('admin=true;state='+JSON.stringify({...snapshot(),balancing:{settings:{nightly:false},transfers:{times:['02:00','10:00'],version:2,next:1}}}));await page.click('transfer-schedule');page.run('globalThis.FormData=class{get(){return " 02:00， 13:00,20:00 ";}}');const pending=page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});await new Promise(setImmediate);assert.equal(sent.path,'/api/admin/balancing/schedule');assert.deepEqual(sent.body,{times:['02:00','13:00','20:00'],expected_version:2});await page.click('close');page.run('modal("another editing form")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another editing form');assert.equal(page.node('#modal').open,true);
});

test('manual handoff polls only its visible pending dialog and displays actual binding separately from historical completion',async()=>{
  let reads=0;const paths=[];const page=await ui((path,options)=>{paths.push([path,options.method]);return response(++reads<3?handoffRecord():handoffRecord({status:'completed',phase:'completed',finished:2,applied_route_version:1,binding:{...handoffRecord().binding,account_id:'c',account_name:'合成账号C',route_version:2,auth_mode:'openai_api',nightly_enabled:0}}));});prepareHandoff(page);
  await page.click('session-actions','session');assert.match(page.node('#session-handoff-content').innerHTML,/等待当前请求完成/);assert.match(page.node('#session-handoff-content').innerHTML,/禁止该会话访问/);assert.match(page.node('#session-handoff-content').innerHTML,/临时固定至02:00/);assert.match(page.node('#session-handoff-content').innerHTML,/\$1\.25/);const original=page.node('#session-handoff-content').innerHTML;
  page.run('document.hidden=true');await handoffPoll(page);assert.equal(reads,1);page.run('document.hidden=false');await handoffPoll(page);assert.equal(reads,2);assert.equal(page.node('#session-handoff-content').innerHTML,original);
  await handoffPoll(page);assert.equal(reads,3);assert.match(page.node('#session-handoff-content').innerHTML,/当前实际绑定：<strong>合成账号C/);assert.match(page.node('#session-handoff-content').innerHTML,/合成账号A → 合成账号B/);assert.match(page.node('#session-handoff-content').innerHTML,/已切换/);assert.doesNotMatch(page.node('#session-handoff-content').innerHTML,/data-action="nightly-session"/);await handoffPoll(page);assert.equal(reads,3);
  await page.click('close');assert.equal(page.run('handoffView'),null);assert.match(page.node('#content').innerHTML,/合成账号C/);assert.equal(paths.length,3);assert.ok(paths.every(([path])=>path==='/api/admin/sessions/session/handoffs'));
});

test('manual handoff supports an empty history and retains old CLI and SQLite controls',async()=>{
  let reads=0;const page=await ui(()=>{reads++;return response(handoffRecord({id:null,status:'none',phase:'none'}));});prepareHandoff(page);await page.click('session-actions','session');assert.match(page.node('#session-handoff-content').innerHTML,/近7天暂无转移记录/);await handoffPoll(page);assert.equal(reads,1);await page.click('handoff-edit','session');assert.match(page.node('#session-handoff-content').innerHTML,/登记转移/);assert.doesNotMatch(page.node('#session-handoff-content').innerHTML,/<option value="a"/);assert.match(page.node('#session-handoff-content').innerHTML,/<option value="b" selected/);await handoffPoll(page);assert.equal(reads,1);await page.click('close');
  page.run('state.sessions[0].protocol="chat"');await page.click('session-actions','session');assert.match(page.node('#modal-content').innerHTML,/旧 CLI Session/);assert.match(page.node('#modal-content').innerHTML,/禁止该会话访问/);assert.equal(reads,1);await page.click('close');page.run('state.sessions[0].protocol="responses";state.capabilities={}');await page.click('session-actions','session');assert.match(page.node('#modal-content').innerHTML,/修改绑定账号/);assert.equal(reads,1);
});

test('manual handoff late reads cannot modify a closed, replaced, logged-out or natively closed dialog',async()=>{
  for(const exit of ['close','replace','logout','native','tab']){
    let release,signal;const page=await ui((_path,options)=>(signal=options.signal,new Promise(resolve=>{release=()=>resolve(response(handoffRecord()));})));prepareHandoff(page);const pending=page.click('session-actions','session');await new Promise(setImmediate);
    if(exit==='close')await page.click('close');else if(exit==='replace')page.run('modal("replacement dialog")');else if(exit==='logout')page.run('admin=false;authEpoch++;showLogin()');else if(exit==='native'){page.node('#modal').open=false;page.node('#modal').onclose();}else page.run('setTab("overview")');assert.equal(signal.aborted,true);release();await pending;assert.equal(page.run('handoffView'),null);assert.equal(page.node('#modal-content').innerHTML,exit==='replace'?'replacement dialog':'');
  }
});

test('manual handoff submits only the confirmed target/version, preserves edits on error and never declares success from acceptance',async()=>{
  const calls=[];let fail=true;const page=await ui((path,options)=>{calls.push([path,options]);if(options.method==='POST')return fail?response({error:{message:'绑定已变化，请重新核对'}},409):response({handoff_id:'handoff-two',status:'waiting'},202);return response(handoffRecord());});prepareHandoff(page);page.run('globalThis.FormData=class{get(){return "c";}}');await page.click('session-actions','session');await page.click('handoff-edit');const form=page.node('#session-handoff-form'),submit={disabled:false},selection={disabled:false},event={preventDefault(){},target:{querySelector:s=>s==='[type=submit]'?submit:selection}};
  await form.onsubmit(event);assert.equal(page.node('#session-handoff-error').textContent,'绑定已变化，请重新核对');assert.equal(page.run('handoffView.mode'),'edit');assert.equal(selection.disabled,false);await handoffPoll(page);assert.equal(calls.length,2);assert.deepEqual(JSON.parse(calls[1][1].body),{account_id:'c',expected_route_version:0,replace_handoff_id:'handoff-one'});
  fail=false;await form.onsubmit(event);assert.equal(page.run('handoffView.mode'),'status');assert.match(page.node('#session-handoff-content').innerHTML,/转移已登记/);assert.match(page.node('#session-handoff-content').innerHTML,/等待当前请求完成/);assert.match(page.node('#session-handoff-content').innerHTML,/当前实际绑定：<strong>合成账号A/);assert.doesNotMatch(page.node('#session-handoff-content').innerHTML,/<strong>已切换/);assert.equal(page.run('state.sessions[0].account_id'),'a');assert.equal(calls.filter(([,options])=>options.method==='POST').length,2);
});

test('manual handoff cancellation reads back actual state, stops after failure and retry recovers without broad refresh',async()=>{
  let reads=0,cancels=0,broken=false;const page=await ui((_path,options)=>{if(options.method==='DELETE'){cancels++;return response(handoffRecord({status:'cancelled',phase:'cancelled',reason:'cancelled'}));}reads++;return broken?response({error:{message:'合成查询失败'}},503):response(handoffRecord(cancels?{status:'cancelled',phase:'cancelled',reason:'cancelled'}:{}));});prepareHandoff(page);await page.click('session-actions','session');broken=true;await handoffPoll(page);assert.match(page.node('#session-handoff-content').innerHTML,/上次读取的快照/);assert.match(page.node('#session-handoff-content').innerHTML,/data-action="handoff-cancel"[^>]+disabled/);await handoffPoll(page);assert.equal(reads,2);await page.click('handoff-cancel');assert.equal(cancels,0);broken=false;await page.click('handoff-refresh');await page.click('handoff-cancel');assert.equal(cancels,1);assert.match(page.node('#session-handoff-content').innerHTML,/已取消待转移/);assert.match(page.node('#session-handoff-content').innerHTML,/当前实际绑定：<strong>合成账号A/);await handoffPoll(page);assert.equal(reads,4);
});

test('manual handoff stale writes cannot close or paint another form and invalid server state cannot enable actions',async()=>{
  let release;const page=await ui((_path,options)=>options.method==='POST'?new Promise(resolve=>{release=()=>resolve(response({handoff_id:'late',status:'waiting'},202));}):response(handoffRecord()));prepareHandoff(page);page.run('globalThis.FormData=class{get(){return "b";}}');await page.click('route-session','session');const pending=page.node('#session-handoff-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});await new Promise(setImmediate);await page.click('close');page.run('modal("another form")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another form');assert.equal(page.run('handoffView'),null);
  const invalid=await ui(()=>response(handoffRecord({session_id:'other',status:'untrusted'})));prepareHandoff(invalid);await invalid.click('session-actions','session');assert.match(invalid.node('#session-handoff-content').innerHTML,/绑定无效/);assert.doesNotMatch(invalid.node('#session-handoff-content').innerHTML,/data-action="handoff-edit"/);
});

test('manual handoff permits cancellation during target verification and disables it once the binding transaction starts',async()=>{
  for(const phase of ['checking','applying']){
    let cancelled=0;const page=await ui((_path,options)=>{if(options.method==='DELETE'){cancelled++;return response(handoffRecord({status:'cancelled',phase:'cancelled'}));}return response(handoffRecord(cancelled?{status:'cancelled',phase:'cancelled'}:{phase}));});prepareHandoff(page);await page.click('session-actions','session');assert.match(page.node('#session-handoff-content').innerHTML,phase==='checking'?/正在核验目标账号/:/正在切换/);assert.match(page.node('#session-handoff-content').innerHTML,/data-action="handoff-edit"[^>]+disabled/);if(phase==='applying')assert.match(page.node('#session-handoff-content').innerHTML,/data-action="handoff-cancel"[^>]+disabled/);else assert.doesNotMatch(page.node('#session-handoff-content').innerHTML,/data-action="handoff-cancel"[^>]+disabled/);await page.click('handoff-cancel');assert.equal(cancelled,phase==='checking'?1:0);
  }
});

test('recording details merge fields across pages, escape text and stop polling completed or backlogged content',async()=>{
  let reads=0;
  const event=(sequence,kind,content)=>({sequence,kind,provenance:kind==='user'?'client':'upstream',created:1,content});
  const page=await ui(path=>{
    if(path==='/api/admin/requests/r')return response({id:'r',ended:1,timeline:[],recording_state:'pending'});
    reads++;
    return response(reads===1?{state:'pending',turnState:'uncertain',events:[event(1,'user',{field:'u',mode:'append',text:'<img src=x onerror=alert(1)>'}),event(2,'assistant',{field:'a',mode:'append',text:'初始片段'})],next:2,hasMore:true}:
      {state:'complete',events:[event(3,'assistant',{field:'a',mode:'replace',text:'完整回复'}),event(4,'tool_call',{field:'tool',mode:'append',name:'read_file',callId:'fixture-call',text:'{"path":"fixture.txt"}'})],next:4,hasMore:false});
  });
  page.run('admin=true;');await page.click('request-detail','r');let html=page.node('#request-recording').innerHTML;
  assert.match(html,/&lt;img/);assert.doesNotMatch(html,/<img/);assert.match(html,/本轮关联不完整/);
  page.intervals.find(x=>x.delay===4000).fn();await new Promise(setImmediate);assert.equal(reads,1,'backlog requires explicit load-more');
  await page.click('recording-more');html=page.node('#request-recording').innerHTML;assert.match(html,/完整回复/);assert.doesNotMatch(html,/初始片段/);assert.match(html,/read_file/);assert.equal(page.run('recordingDetail.entries.length'),3);
  page.intervals.find(x=>x.delay===4000).fn();await new Promise(setImmediate);assert.equal(reads,2,'completed recordings do not poll');
  await page.click('close');assert.equal(page.run('recordingDetail'),null);
});

test('late recording reads cannot restore a closed dialog and recorded expiry clears displayed content',async()=>{
  let release,signal;
  const page=await ui((path,options)=>path==='/api/admin/requests/r'?response({id:'r',ended:1,timeline:[],recording_state:'complete'}):new Promise(resolve=>{signal=options.signal;release=()=>resolve(response({state:'complete',events:[{sequence:1,kind:'assistant',provenance:'upstream',content:{text:'late synthetic body'}}],next:1,hasMore:false}));}));
  page.run('admin=true;');const pending=page.click('request-detail','r');await new Promise(setImmediate);await page.click('close');assert.equal(signal.aborted,true);release();await pending;assert.equal(page.run('recordingDetail'),null);assert.equal(page.node('#modal-content').innerHTML,'');
  const expiry=await ui(path=>response(path==='/api/admin/requests/r'?{id:'r',ended:1,timeline:[],recording_state:'complete'}:{state:'complete',expiresAt:1500,events:[{sequence:1,kind:'assistant',provenance:'upstream',content:{text:'synthetic retained body'}}],next:1,hasMore:false}));
  expiry.run('admin=true;Date.now=()=>1000;');await expiry.click('request-detail','r');assert.match(expiry.node('#request-recording').innerHTML,/synthetic retained body/);
  expiry.run('Date.now=()=>2000;');expiry.intervals.find(x=>x.delay===4000).fn();assert.doesNotMatch(expiry.node('#request-recording').innerHTML,/synthetic retained body/);assert.match(expiry.node('#request-recording').innerHTML,/正文已过保留期/);assert.equal(expiry.run('recordingDetail.fields.size'),0);
});

test('recording retries validate the complete page before appending and historical details never query the recording endpoint',async()=>{
  let reads=0;const page=await ui(path=>path==='/api/admin/requests/old'?response({id:'old',ended:1,timeline:[]}):path==='/api/admin/requests/r'?response({id:'r',ended:1,timeline:[],recording_state:'partial'}):(reads++,response({state:'partial',events:[{sequence:1,kind:'user',provenance:'client',content:{text:'must not partially append'}},{sequence:1,kind:'assistant',provenance:'upstream',content:{text:'duplicate'}}],next:1,hasMore:false})));
  page.run('admin=true;');await page.click('request-detail','old');assert.equal(reads,0);assert.match(page.node('#request-recording').innerHTML,/历史记录未保存正文/);
  await page.click('request-detail','r');assert.equal(page.run('recordingDetail.entries.length'),0);assert.match(page.node('#request-recording').innerHTML,/正文事件顺序或结构无效/);await page.click('recording-retry');assert.equal(page.run('recordingDetail.entries.length'),0);assert.equal(reads,2);
});

test('slow initial request metadata is not replaced by a premature polling render',async()=>{
  let release,reads=0;const page=await ui(path=>{assert.equal(path,'/api/admin/requests/r');reads++;return new Promise(resolve=>{release=()=>resolve(response({id:'r',ended:1,timeline:[]}));});});
  page.run('admin=true;');const pending=page.click('request-detail','r');page.intervals.find(x=>x.delay===4000).fn();await new Promise(setImmediate);assert.equal(reads,1);release();await pending;assert.match(page.node('#modal-content').innerHTML,/request-phase-detail/);assert.equal(page.run('requestDetailLoading'),false);
});

test('request detail polling can initialize the view after the first metadata read fails',async()=>{
  let reads=0;const page=await ui(()=>++reads===1?response({error:{message:'synthetic outage'}},503):response({id:'r',ended:1,timeline:[]}));
  page.run('admin=true;');await page.click('request-detail','r');assert.equal(page.run('requestDetailLoading'),false);page.intervals.find(x=>x.delay===4000).fn();await new Promise(setImmediate);assert.match(page.node('#modal-content').innerHTML,/request-recording/);assert.equal(page.run('recordingDetail.state'),'historical');
});

test('R22 partial recordings can manually load a recovered prefix without polling or duplicate append',async()=>{
  let reads=0;const page=await ui(path=>path==='/api/admin/requests/r'?response({id:'r',ended:1,timeline:[],recording_state:'partial'}):response(++reads===2?{state:'partial',reason:'queue_limit',events:[{sequence:1,kind:'user',provenance:'client',content:{field:'u',mode:'append',text:'recovered synthetic prefix'}}],next:1,hasMore:false}:{state:'partial',reason:'queue_limit',events:[],next:reads===1?0:1,hasMore:false}));
  page.run('admin=true;');await page.click('request-detail','r');assert.match(page.node('#request-recording').innerHTML,/刷新已保存正文/);
  page.intervals.find(x=>x.delay===4000).fn();await new Promise(setImmediate);assert.equal(reads,1,'partial does not automatically poll');
  await page.click('recording-refresh');assert.match(page.node('#request-recording').innerHTML,/recovered synthetic prefix/);assert.match(page.node('#request-recording').innerHTML,/部分缺失/);
  await page.click('recording-refresh');assert.equal(page.run('recordingDetail.entries.length'),1);assert.equal(page.run('recordingDetail.entries[0].text'),'recovered synthetic prefix');
});

test('PostgreSQL read snapshot differences do not claim that request data necessarily changed',async()=>{
  const page=await ui(()=>response({ok:true}));page.run("requestsData={rows:[],summary:{requests:0,success:0,active:0,average_ms:0,standard:{}},pagination:{page:1,pageSize:25,total:0,totalPages:1,nextCursor:null},statisticsAt:1000,watermark:'list',statisticsWatermark:'summary',statisticsWatermarkKind:'postgres_visibility'};");
  let html=page.run('requestsSection()');assert.match(html,/列表与汇总来自不同读取快照/);assert.doesNotMatch(html,/数据已有变化/);
  page.run('requestsData.statisticsWatermarkKind=undefined;');html=page.run('requestsSection()');assert.match(html,/列表加载后数据已有变化/);
});

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
 const pending=page.run('tab="history";loadHistory()');await page.node('#logout').onclick();release();await pending;
 assert.equal(page.run('historyData'),null);assert.equal(page.node('#content').innerHTML,'');assert.equal(page.run('recordView'),'usage');
});
test('系统操作历史归入系统管理，离开页面使迟到历史失效且不归入使用数据',async()=>{
 let release;const paths=[],page=await ui(path=>{paths.push(path);return new Promise(resolve=>release=()=>resolve(response({accounts:[],rows:[],pagination:{page:1,total:0,totalPages:1}})));});
 page.run('admin=true;state='+JSON.stringify({...snapshot(),keys:[]})+';tab="history";');
 const pending=page.run('loadHistory()');assert.equal(page.run('historyLoading'),true);assert.match(page.node('#content').innerHTML,/系统操作历史/);assert.match(page.node('#content').innerHTML,/aria-busy="true"/);assert.ok(!page.run('recordTabs()').includes('data-id="history"'));
 page.run('setTab("accounts")');release();await pending;
 assert.equal(page.run('historyData'),null);assert.equal(page.run('historyLoading'),false);assert.equal(paths.length,1);assert.ok(paths[0].startsWith('/api/admin/history?'));
});
test('侧栏管理设置集中入口并按实际环境提供备份，员工不见管理操作',async()=>{
 const paths=[],page=await ui(path=>{paths.push(path);return response({});});
 page.run('admin=true;state='+JSON.stringify({...snapshot(),mode:'PostgreSQL · 开发验证',capabilities:{recordingHistory:true}}));
 const pg=page.run('managementSettings()');assert.match(pg,/统计口径/);assert.match(pg,/模型费率/);assert.match(pg,/正文保留期/);assert.match(pg,/备份由运维命令管理/);assert.ok(!pg.includes('data-action="backup"'));
 await page.click('backup');assert.deepEqual(paths,[]);
 page.run('state.mode="本机 · 订阅测试"');assert.match(page.run('managementSettings()'),/data-action="backup"/);
 page.run('admin=false');assert.equal(page.run('managementSettings()'),'');
});
test('手动刷新显示当前区域反馈，切页和失败会解除，后台刷新不罩页面',async()=>{
 let release;const page=await ui(()=>new Promise(resolve=>release=()=>resolve(response({...snapshot(),busy:9}))));page.run('admin=true;state='+JSON.stringify(snapshot())+';tab="accounts";render();');
 const pending=page.run('refresh()');assert.equal(page.run('pageRefreshing()'),true);assert.equal(page.node('#refresh').disabled,true);assert.equal(page.node('#page-refresh-note').hidden,false);
 page.run('setTab("members")');assert.equal(page.run('pageRefreshing()'),false);assert.equal(page.node('#refresh').disabled,false);release();await pending;assert.equal(page.run('state.busy'),0);assert.equal(page.run('foregroundRefreshVersion'),0);
 const failed=await ui(()=>response({error:{message:'合成刷新失败'}},503));failed.run('admin=true;state='+JSON.stringify(snapshot())+';tab="accounts";render();');await failed.run('refresh()');assert.equal(failed.run('pageRefreshing()'),false);assert.equal(failed.node('#refresh').disabled,false);assert.match(failed.node('#toast').textContent,/合成刷新失败/);
 let finish;const background=await ui(()=>new Promise(resolve=>finish=()=>resolve(response(snapshot()))));background.run('admin=true;state='+JSON.stringify(snapshot())+';tab="accounts";render();');const updating=background.run('automaticRefresh()');assert.equal(background.run('pageRefreshing()'),false);assert.equal(background.node('#refresh').disabled,false);finish();await updating;
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
 const html=page.run('usageTable(usageRows())');assert.match(html,/hg_fixture_complete_only/);assert.match(html,/copy-key/);assert.match(html,/近7天参考消耗（USD）/);assert.match(html,/\$0\.00</);assert.doesNotMatch(html,/亿 Token/);
 assert.equal(page.run('referenceMoney(3.3)'), '$3.30');assert.equal(page.run('referenceMoney(null)'), '—');assert.equal(page.run('referenceMoney(0.00000001)'), '$0.00');assert.equal(page.run('referenceMoney(1.005)'), '$1.01');assert.equal(page.run('referenceMoney(1234.567)'), '$1,234.57');
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

test('参考消耗明细只在使用数据展示两个窗口，总览仅保留汇总',async()=>{
 const page=await ui(()=>response(snapshot()));
 page.run(`admin=true;state={...${JSON.stringify(snapshot())},keys:[{id:'key',member_id:'member',prefix:'hg_fixture',standard:{total:2,unmetered:12,unpriced:1},standard_12h:{total:0.3,unmetered:0,unpriced:0},enabled:1}],members:[{id:'member',name:'Fixture',enabled:1}],sessions:[]};tab='overview';render();`);
 assert.doesNotMatch(page.node('#content').innerHTML,/近12小时参考消耗（USD）|hg_fixture|最近用量/);page.run("tab='requests';recordView='usage';render();");
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
 assert.match(page.node('#content').innerHTML,/<h1>系统管理<\/h1>/);await page.click('connection-view','guide');assert.match(page.node('#content').innerHTML,/Codex Windows/);assert.equal(page.run('draft'),'待发送');await page.click('connection-view','test');assert.match(page.node('#content').innerHTML,/待发送/);
 page.run('sending=true');await page.click('connection-view','guide');assert.equal(page.run('connectionView'),'test');page.run('sending=false;admin=false');await page.click('connection-view','guide');assert.equal(page.run('connectionView'),'test');assert.ok(!page.node('#content').innerHTML.includes('data-action="backup"'));
});
test('侧栏按使用数据与系统管理归类，保留内部页面的真实刷新范围',async()=>{
 const html=readFileSync(new URL('../public/index.html',import.meta.url),'utf8'),navigation=html.match(/<nav>([\s\S]*?)<\/nav>/)[1];
 assert.match(navigation,/data-tab="usage"[^>]*>[\s\S]*?使用数据/);assert.match(navigation,/data-tab="system"[^>]*>[\s\S]*?系统管理/);
 assert.doesNotMatch(navigation,/data-tab="(?:sessions|requests|history|playground)"/);
 const page=await ui(()=>response({...snapshot(),sessions:[],endpoint:'http://localhost/v1'}));page.run('admin=true;state='+JSON.stringify({...snapshot(),sessions:[],endpoint:'http://localhost/v1'})+';tab="overview";setTab("system")');
 assert.equal(page.run('tab'),'playground');assert.equal(page.run('connectionView'),'guide');assert.equal(page.run('navigationTab()'),'system');assert.equal(page.run('automaticRefreshScope()'),'');
 await page.click('connection-view','test');page.run('setTab("system")');assert.equal(page.run('connectionView'),'test','组内导航保留手动选择');
 page.run('setTab("usage")');assert.equal(page.run('tab'),'sessions');assert.equal(page.run('navigationTab()'),'usage');assert.equal(page.run('automaticRefreshScope()'),'state:sessions');
 await page.click('record-view','usage');assert.equal(page.run('tab'),'requests');page.run('setTab("usage")');assert.equal(page.run('tab'),'requests','组内导航保留手动子页');
 page.run('setTab("overview");setTab("history")');assert.equal(page.run('tab'),'history','显式历史入口保持');page.run('setTab("overview");setTab("requests")');assert.equal(page.run('tab'),'requests','显式Key用量入口保持');page.run('setTab("overview");setTab("guide")');assert.equal(page.run('connectionView'),'guide','显式接入说明入口保持');
});
test('系统管理组内切换保留请求草稿，迟到历史不覆盖接入页，发送中不清理当前页面',async()=>{
 let release;const page=await ui(path=>path.startsWith('/api/admin/history?')?new Promise(resolve=>release=()=>resolve(response({rows:[],accounts:[],pagination:{page:1,total:0,totalPages:1}}))):response({}));
 page.run('admin=true;state='+JSON.stringify({...snapshot(),endpoint:'http://localhost/v1'})+';tab="playground";draft="保留草稿";testKey="hg_synthetic_preview";chatSession="synthetic-session";render();');
 await page.click('connection-view','history');assert.equal(page.run('tab'),'history');assert.equal(page.run('historyLoading'),true);assert.equal(page.run('navigationTab()'),'system');
 await page.click('connection-view','guide');release();await new Promise(setImmediate);
 assert.equal(page.run('tab'),'playground');assert.equal(page.run('connectionView'),'guide');assert.equal(page.run('historyData'),null);assert.equal(page.run('draft'),'保留草稿');assert.equal(page.run('chatSession'),'synthetic-session');assert.equal(page.run('testKey'),'hg_synthetic_preview');
 assert.match(page.node('#content').innerHTML,/Codex Windows/);await page.click('connection-view','test');assert.match(page.node('#content').innerHTML,/保留草稿/);
 page.run('modal("保留弹窗");sending=true;historyController=new AbortController();setTab("usage")');assert.equal(page.run('tab'),'playground');assert.equal(page.node('#modal').open,true);assert.equal(page.run('historyController.signal.aborted'),false);
 await page.click('connection-view','history');assert.equal(page.run('tab'),'playground');
});
test('使用数据组内切换会话与正文不默认读取正文，迟到请求不能覆盖会话列表',async()=>{
 let release;const paths=[],state={...snapshot(),sessions:[],capabilities:{recordingHistory:true}},page=await ui(path=>{paths.push(path);return new Promise(resolve=>release=()=>resolve(response({rows:[],models:[],members:[],pagination:{page:1,total:0,totalPages:1}})));});
 page.run('admin=true;state='+JSON.stringify(state)+';tab="sessions";render();');await page.click('record-view','recordings');assert.equal(page.run('tab'),'requests');assert.equal(page.run('recordView'),'recordings');assert.deepEqual(paths,[]);
 const pending=page.click('record-view','requests');await new Promise(setImmediate);assert.equal(paths.length,1);assert.ok(paths[0].startsWith('/api/admin/requests?'));
 await page.click('record-view','sessions');release();await pending;assert.equal(page.run('tab'),'sessions');assert.equal(page.run('requestsData'),null);assert.match(page.node('#content').innerHTML,/近7天活动会话/);assert.ok(!paths.some(path=>path.includes('/recordings/')));
});
test('离开使用数据后迟到Key用量不进入总览，总览也不会主动读Key明细',async()=>{
 let release;const paths=[],page=await ui(path=>{paths.push(path);return new Promise(resolve=>release=()=>resolve(response({rows:[deferredKeyUsage('late')],pagination:{page:1,total:1,totalPages:1}})));});
 page.run('admin=true;state='+JSON.stringify({...pagedSnapshot(),sessions:[]})+';tab="requests";recordView="usage";keyUsageScope="requests:";');const pending=page.run('loadKeyUsage()');page.run('setTab("overview")');release();await pending;
 assert.equal(page.run('keyUsageData'),null);assert.doesNotMatch(page.node('#content').innerHTML,/hg_test_late|key-usage-results|近12小时参考消耗/);assert.equal(await page.run('loadKeyUsage()'),false);assert.equal(paths.length,1);
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
 page.run("tab='history'");await page.run('automaticRefresh()');page.run("tab='requests';recordView='usage'");await page.run('automaticRefresh()');
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
 page.run("tab='history';historyData=null;render()");assert.equal(page.run('automaticRefreshScope()'),'');
});

test('request refresh restores online status after an outage without accepting late responses',async()=>{
 const data={summary:{active:0},pagination:{page:1,pageSize:25,total:0,totalPages:1},rows:[],models:[],members:[],byModel:[],byMember:[]};
 let failing=true,release;
 const page=await ui(()=>release===null?new Promise(r=>release=()=>r(response(data))):failing?response({error:{message:'synthetic outage'}},502):response(data));
 page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';requestsData="+JSON.stringify(data));
 await page.run('automaticRefresh()');assert.equal(page.node('#connection').textContent,'连接异常');
 failing=false;await page.run('automaticRefresh()');assert.equal(page.node('#connection').textContent,'服务在线');
 failing=true;await page.run('automaticRefresh()');failing=false;await page.run('loadRequests()');assert.equal(page.node('#connection').textContent,'服务在线');
 release=null;const pending=page.run('automaticRefresh()');page.run("tab='members';render()");page.node('#connection').textContent='new-page-status';release();await pending;
 assert.equal(page.node('#connection').textContent,'new-page-status');
});

test('account entry separates subscription and Key types and preserves independent repeated additions',async()=>{
 const created=[],page=await ui((path,options)=>{if(path==='/api/admin/accounts')created.push(JSON.parse(options.body));return response(path==='/api/admin/state'?snapshot():{id:'account-'+created.length});});
 page.run('admin=true;state='+JSON.stringify(snapshot())+';globalThis.FormData=class{get(k){return formValues[k]??null;}};');
 page.run("tab='accounts';render();");assert.equal((page.node('#content').innerHTML.match(/＋ 添加账户/g)||[]).length,2);assert.doesNotMatch(page.node('#content').innerHTML,/create-api-account|添加一个独立账号|添加订阅账号|添加API Key/);
 const submit=()=>page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});
 for(const name of ['API A','API B']){
  await page.click('create-account');page.node('#new-account-auth').onchange({target:{value:'openai_api'}});assert.match(page.node('#modal-content').innerHTML,/接入类型/);assert.equal(page.node('#new-api-fields').disabled,false);assert.equal(page.node('#new-subscription-fields').disabled,true);assert.equal(page.node('#new-api-key').required,true);
  page.run('globalThis.formValues='+JSON.stringify({auth_mode:'openai_api',kind:'claude',name,api_key:'synthetic-'+name,api_weight:'0'}));await submit();
 }
 assert.equal(created.length,2);assert.notEqual(created[0].name,created[1].name);assert.ok(created.every(a=>a.auth_mode==='openai_api'&&a.kind==='codex'&&a.api_weight===0));
 await page.click('create-account');page.node('#new-account-auth').onchange({target:{value:'subscription'}});assert.equal(page.node('#new-api-fields').disabled,true);assert.equal(page.node('#new-subscription-fields').disabled,false);assert.equal(page.node('#new-api-key').required,false);
 page.run('formValues='+JSON.stringify({auth_mode:'subscription',kind:'claude',name:'Subscription C',api_key:'must-not-submit',api_weight:'80'}));await submit();assert.deepEqual(created[2],{name:'Subscription C',auth_mode:'subscription',kind:'claude'});
 await page.click('create-account');assert.equal(page.node('#new-api-fields').disabled,true);assert.equal(page.node('#new-subscription-fields').disabled,false);
});

const pricingState=()=>({seedCaptured:'2026-10-29',settings:{enabled:true,refreshHours:24,version:0,manualRates:{},aliases:[]},state:{active:'a'.repeat(64)},unpricedModels:[{model:'new-model',requests:2}],rates:[{model:'gpt-6.1-sol',input:'2',cached:'0.1',output:'10',manual:false}]});
test('late fee settings cannot replace a newer dialog and the settings view identifies missing prices without raw usage',async()=>{
 let release;const page=await ui(()=>new Promise(r=>release=()=>r(response(pricingState()))));page.run('admin=true;');const pending=page.click('pricing-settings');await page.click('close');page.run("modal('<h2>other</h2>')");release();await pending;assert.equal(page.node('#modal-content').innerHTML,'<h2>other</h2>');
 const html=page.run('pricingOverview('+JSON.stringify(pricingState())+')');assert.match(html,/new-model：2条/);assert.match(html,/缺价不按0/);assert.match(html,/USD \/ 100万Token/);assert.doesNotMatch(html,/亿 Token/);assert.match(html,/\$2\.00/);assert.match(html,/\$0\.10/);
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

test('member model limits use an employee-scoped endpoint, retain disabled rules and guard stale dialogs',async()=>{
 const limits={enabled:false,version:2,rules:[{model:'gpt-6.1-sol',hours:24,limit_usd:'20.00'}],usage:[]},calls=[];
 const page=await ui((path,options)=>{calls.push([path,options]);return response(path==='/api/admin/pricing'?pricingState():path.endsWith('usage-limits')?limits:snapshot());});
 page.run('admin=true;globalThis.FormData=class{get(k){return values[k]??null;}getAll(k){return values[k]||[];}};');await page.click('member-limits','member-id');assert.match(page.node('#modal-content').innerHTML,/滚动24小时/);assert.match(page.node('#modal-content').innerHTML,/20.00/);
 page.node('#member-limit-enabled').onchange({target:{value:'on'}});assert.equal(page.run('memberLimitEditor.enabled'),true);
 page.run('globalThis.values={limits_enabled:"on",limit_model:["gpt-6.1-sol"],limit_hours:["168"],limit_usd:["30.00"]};');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});
 const saved=calls.find(([p,o])=>p==='/api/admin/members/member-id/usage-limits'&&o.method==='PATCH');assert.deepEqual(JSON.parse(saved[1].body),{expected_version:2,enabled:true,rules:[{model:'gpt-6.1-sol',hours:168,limit_usd:'30.00'}]});
 let release;const delayed=await ui(path=>path==='/api/admin/pricing'?response(pricingState()):new Promise(r=>release=()=>r(response(limits))));delayed.run('admin=true;');const pending=delayed.click('member-limits','member-id');await delayed.click('close');delayed.run("modal('<h2>other</h2>')");release();await pending;assert.equal(delayed.node('#modal-content').innerHTML,'<h2>other</h2>');
});
test('R2 member limit loading shows retry/close for network and HTTP errors and ignores late failures',async()=>{
 for(const network of [true,false]){
  const page=await ui(()=>{if(network)throw Error('synthetic offline');return response({error:{message:'synthetic unavailable'}},503);});page.run('admin=true;');await page.click('member-limits','member-id');const html=page.node('#modal-content').innerHTML;assert.match(html,/synthetic/);assert.match(html,/重试/);assert.match(html,/关闭/);assert.doesNotMatch(html,/正在读取/);
 }
 let reject;const page=await ui(path=>path==='/api/admin/pricing'?response(pricingState()):new Promise((_,r)=>reject=r));page.run('admin=true;');const pending=page.click('member-limits','member-id');await page.click('close');page.run("modal('<h2>new dialog</h2>')");reject(Error('late failure'));await pending;assert.equal(page.node('#modal-content').innerHTML,'<h2>new dialog</h2>');
});


test('request metadata renders before statistics and a stale summary cannot overwrite a new filter',async()=>{
 const pending=[];const page=await ui((path,options)=>new Promise(resolve=>pending.push({path,signal:options.signal,release:body=>resolve(response(body))})));
 page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';");
 const list={snapshot:'stable',watermark:1,observedAt:1,pagination:{page:1,pageSize:25,total:3,totalPages:1},rows:[]};
 const first=page.run('loadRequests()');pending[0].release(list);await first;
 assert.match(pending[0].path,/view=list/);assert.match(pending[1].path,/view=summary/);assert.match(page.node('#content').innerHTML,/汇总独立加载中/);assert.match(page.node('#content').innerHTML,/共 3 条/);
 const second=page.run("requestsFilters.model='changed';loadRequests()");assert.equal(pending[1].signal.aborted,true);
 pending[1].release({watermark:1,observedAt:1,summary:{requests:999,success:999,active:0,average_ms:1},byModel:[],byMember:[],members:[],models:[]});
 pending[2].release({...list,watermark:2,pagination:{...list.pagination,total:1}});await second;await new Promise(setImmediate);assert.ok(!page.node('#content').innerHTML.includes('999'));
 pending[3].release({watermark:3,observedAt:2,summary:{requests:1,success:1,active:0,average_ms:1,standard:{total:2}},byModel:[],byMember:[],members:[],models:['changed']});await new Promise(setImmediate);
 assert.match(page.node('#content').innerHTML,/统计时间/);assert.match(page.node('#content').innerHTML,/同一统计水位/);assert.equal(page.run('requestsData.summary.requests'),1);
});

test('R1 cursor navigation uses nextCursor after the matching count shrinks',async()=>{
 const calls=[],result={summary:{requests:75,active:0,average_ms:1},pagination:{page:4,pageSize:25,total:75,totalPages:3,nextCursor:null,cursorMode:true},rows:[],models:[],members:[],byModel:[],byMember:[]};
 const page=await ui(path=>{calls.push(path);return response(result);});page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';requestsFilters.page=3;requestsPageSnapshot='bound';requestsCursors=new Map([[4,'next']]);requestsData="+JSON.stringify({...result,pagination:{...result.pagination,page:3,nextCursor:'next'}})+";render();");
 assert.match(page.node('#content').innerHTML,/data-action="requests-page" data-id="4">/);await page.click('requests-page','4');assert.match(calls[0],/page=4/);assert.match(calls[0],/cursor=next/);assert.equal(page.run('requestsFilters.page'),4);
});

test('R2 explicit refresh discards a deep or invalid snapshot and recovers on the first page',async()=>{
 const calls=[];let fail=false;const result={summary:{requests:3,active:0,average_ms:1},pagination:{page:1,pageSize:25,total:3,totalPages:1,nextCursor:null},rows:[],models:[],members:[],byModel:[],byMember:[]};
 const page=await ui(path=>{calls.push(path);return fail?response({error:{message:'snapshot expired'}},400):response(result);});page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';requestsFilters.page=2;requestsPageSnapshot='expired';requestsCursors=new Map([[2,'old']]);");
 fail=true;await page.run("loadRequests('',{preservePage:true})");assert.match(calls[0],/snapshot=expired/);assert.equal(page.run('requestsData'),null);
 fail=false;await page.click('requests-reload');assert.match(calls[1],/page=1&/);assert.ok(!calls[1].includes('snapshot='));assert.ok(!calls[1].includes('cursor='));assert.equal(page.run('requestsData.pagination.page'),1);
});

test('R3 delayed statistics defer success and failure rendering while editing or hidden',async()=>{
 for(const protect of ['edit','hidden'])for(const fail of [false,true]){
  const pending=[],page=await ui(path=>new Promise(resolve=>pending.push({path,release:(body,status=200)=>resolve(response(body,status))})));
  page.run('admin=true;state='+JSON.stringify(snapshot())+";tab='requests';recordView='requests';");const load=page.run("loadRequests('requests:latest')");
  pending[0].release({snapshot:'stable',watermark:1,pagination:{page:1,pageSize:25,total:3,totalPages:1},rows:[]});await load;
  const before=page.node('#content').innerHTML;page.run(protect==='edit'?"document.activeElement={tagName:'SELECT'}":"document.hidden=true");
  pending[1].release(fail?{error:{message:'暂不可用'}}:{watermark:1,observedAt:2,summary:{requests:3,success:3,active:0,average_ms:1,standard:{total:2}},byModel:[],byMember:[],members:[],models:[]},fail?503:200);await new Promise(setImmediate);
  assert.equal(page.node('#content').innerHTML,before);assert.equal(page.run('requestsRenderPending'),true);
  page.run("document.activeElement={tagName:'BODY'};document.hidden=false;flushRequestStatistics()");assert.equal(page.run('requestsRenderPending'),false);assert.match(page.node('#content').innerHTML,fail?/暂不可用/:/统计时间/);
 }
});

const planView=(extra={})=>({id:'plan-one',status:'completed',ready:true,planned_at:Date.now(),expires:Date.now()+60000,plan:{window:{end:Date.now()},counts:{groups:1,moves:2,skipped:1,fixed:1},preparationChecked:false},...extra});
const planGroup={position:1,accounts:['a','b'],before:{a:2,b:0},after:{a:1,b:1},target:{a:1,b:1},fixedLoad:{a:0,b:0}};
function preparePlanning(page){page.run(`admin=true;state={...${JSON.stringify(snapshot())},capabilities:{asyncPlanning:true},sessions:[],accounts:[{id:'a',name:'账号A'},{id:'b',name:'<img src=x>'}]};`);}
test('async preview pages one section at a time, preserves navigation and shows off-page client identifiers safely',async()=>{
  const calls=[];const page=await ui((path,options)=>{calls.push([path,options]);if(path==='/api/admin/balancing/plans')return response({id:'plan-one'});if(path.endsWith('plan-one'))return response(planView());const q=new URL('http://local'+path).searchParams,section=q.get('section'),after=Number(q.get('after'));return response({section,items:section==='groups'?[planGroup]:section==='moves'?[{position:after+1,session_id:'internal',client_session_id:after?'client-two':'<script>client-one</script>',from:'a',to:'b',weight:1}]:section==='fixed'?[{position:1,session_id:'fixed-id',client_session_id:null,group:0}]:[{position:1,reason:'busy',session_id:'busy-id'}],after:after+1,has_more:section==='moves'&&!after});});preparePlanning(page);
  await page.click('balancing-preview');assert.match(page.node('#modal-content').innerHTML,/后台预览尚未核验/);assert.match(page.node('#modal-content').innerHTML,/&lt;img src=x&gt;/);assert.equal(calls.filter(([p])=>p.includes('/items')).length,1);
  await page.click('planning-section','moves');assert.match(page.node('#modal-content').innerHTML,/&lt;script&gt;client-one&lt;\/script&gt;/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/<script>/);
  await page.click('planning-page','next');assert.match(page.node('#modal-content').innerHTML,/client-two/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/client-one/);assert.equal(page.run('planningPreview.back.length'),1);
  await page.click('planning-page','previous');assert.match(page.node('#modal-content').innerHTML,/client-one/);assert.equal(page.run('planningPreview.back.length'),0);
  await page.click('planning-section','fixed');assert.match(page.node('#modal-content').innerHTML,/平台会话 fixed-id/);await page.click('planning-section','skipped');assert.match(page.node('#modal-content').innerHTML,/会话有请求执行中/);
  assert.equal(calls.filter(([p,o])=>p==='/api/admin/balancing/plans'&&o.method==='POST').length,1);assert.ok(calls.every(([p])=>!p.includes('/balancing/preview')));
});

test('async preview polls only unfinished visible dialogs and stops after completion or close',async()=>{
  let reads=0;const page=await ui(path=>path.endsWith('/plans')?response({id:'plan-one'}):path.endsWith('plan-one')?response(++reads===1?planView({ready:false,status:'pending',plan:undefined}):planView()):response({section:'groups',items:[],after:0,has_more:false}));preparePlanning(page);await page.click('balancing-preview');assert.equal(reads,1);
  page.run('document.hidden=true');let timer=page.timers.find(t=>t.active&&t.delay===2000);await timer.fn();assert.equal(reads,1);
  page.run('document.hidden=false');timer=page.timers.filter(t=>t.active&&t.delay===2000).at(-1);await timer.fn();await new Promise(setImmediate);assert.equal(reads,2);assert.equal(page.timers.filter(t=>t.active&&t.delay===2000).length,0);
  await page.click('close');assert.equal(page.run('planningPreview'),null);assert.equal(page.timers.filter(t=>t.active&&t.delay>=2000).length,0);
});

test('async preview ignores delayed responses after close, replacement, logout, native close and tab switch',async()=>{
  for(const exit of ['close','replace','logout','native','tab']){
    let release,signal;const page=await ui((path,options)=>path.endsWith('/plans')?(signal=options.signal,new Promise(resolve=>{release=resolve;})):response({}));preparePlanning(page);const pending=page.click('balancing-preview');await new Promise(setImmediate);assert.equal(page.node('#modal').open,true);
    if(exit==='close')await page.click('close');else if(exit==='replace')page.run('modal("replacement dialog")');else if(exit==='logout')page.run('admin=false;authEpoch++;showLogin()');else if(exit==='native'){page.node('#modal').open=false;page.node('#modal').onclose();}else page.run('setTab("overview")');
    assert.equal(signal.aborted,true);release(response({id:'plan-one'}));await pending;assert.equal(page.run('planningPreview'),null);assert.equal(page.node('#modal').open,exit==='replace');assert.equal(page.node('#modal-content').innerHTML,exit==='replace'?'replacement dialog':'');
  }
});

test('async preview retries reads without creating another task and rejects nonadvancing pages',async()=>{
  let posts=0,statusReads=0,pages=0;const page=await ui(path=>path.endsWith('/plans')?(posts++,response({id:'plan-one'})):path.endsWith('plan-one')?(++statusReads===1?response({error:{message:'暂时无法查询'}},503):response(planView())):++pages===1?response({section:'groups',items:[planGroup],after:0,has_more:true}):response({section:'groups',items:[planGroup],after:1,has_more:false}));preparePlanning(page);await page.click('balancing-preview');assert.match(page.node('#modal-content').innerHTML,/暂时无法查询/);assert.equal(page.timers.filter(t=>t.active&&t.delay===2000).length,0);
  await page.click('planning-retry');assert.match(page.node('#modal-content').innerHTML,/规划分页/);assert.equal(page.run('planningPreview.items.length'),0);await page.click('planning-retry');assert.equal(page.run('planningPreview.items.length'),1);assert.equal(posts,1);
});

test('async preview clears expired results and can explicitly resubmit after a failed submission',async()=>{
  let posts=0;const page=await ui(path=>path.endsWith('/plans')?(++posts===1?response({error:{message:'提交失败'}},503):response({id:'plan-one'})):path.endsWith('plan-one')?response(planView()):response({section:'groups',items:[planGroup],after:1,has_more:false}));preparePlanning(page);await page.click('balancing-preview');assert.match(page.node('#modal-content').innerHTML,/重新生成/);assert.equal(page.run('planningPreview.id'),undefined);
  await page.click('planning-new');assert.equal(page.run('planningPreview.items.length'),1);const expiry=page.timers.find(t=>t.active&&t.delay>10000);expiry.fn();assert.match(page.node('#modal-content').innerHTML,/计划已过期/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/账号A/);assert.equal(page.run('planningPreview.items.length'),0);assert.equal(page.run('planningPreview.controller.signal.aborted'),true);
});

test('async preview displays terminal failures without polling and discards delayed status or page results',async()=>{
  for(const status of ['failed','cancelled']){const page=await ui(path=>path.endsWith('/plans')?response({id:'plan-one'}):response(planView({status,ready:false,plan:undefined})));preparePlanning(page);await page.click('balancing-preview');assert.match(page.node('#modal-content').innerHTML,/重新生成/);assert.equal(page.timers.filter(t=>t.active&&t.delay===2000).length,0);}
  for(const phase of ['status','page']){let release,signal;const page=await ui((path,options)=>{if(path.endsWith('/plans'))return response({id:'plan-one'});if(phase==='page'&&path.endsWith('plan-one'))return response(planView());signal=options.signal;return new Promise(resolve=>{release=resolve;});});preparePlanning(page);const pending=page.click('balancing-preview');await new Promise(setImmediate);assert.ok(release);page.run('modal("new dialog")');assert.equal(signal.aborted,true);release(response(phase==='status'?planView():{section:'groups',items:[planGroup],after:1,has_more:false}));await pending;assert.equal(page.node('#modal-content').innerHTML,'new dialog');assert.equal(page.run('planningPreview'),null);}
});

test('async preview handles server expiration as a terminal state rather than retrying the stale page',async()=>{
  let reads=0;const page=await ui(path=>path.endsWith('/plans')?response({id:'plan-one'}):path.endsWith('plan-one')?response(planView()):++reads===1?response({section:'groups',items:[planGroup],after:1,has_more:true}):response({error:{message:'规划已过期'}},410));preparePlanning(page);await page.click('balancing-preview');assert.equal(page.run('planningPreview.items.length'),1);await page.click('planning-page','next');assert.match(page.node('#modal-content').innerHTML,/计划已过期/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/账号A/);assert.equal(page.run('planningPreview.expired'),true);assert.equal(page.timers.filter(t=>t.active&&t.delay===2000).length,0);
});

test('PostgreSQL nightly history distinguishes actual results, pages without preview submissions and clears on close',async()=>{
 const paths=[],until=Date.now()+86400000,header={day:'2026-11-03',status:'failed',started:1,ended:2,paginated:true,hasPlan:true,visibleUntil:until,plannedAt:1,window:{start:0,end:1},counts:{groups:1,moves:2,skipped:0,fixed:0},results:{applied:1,skipped:0},accounts:[{id:'a',name:'合成账号A'},{id:'b',name:'合成账号B'}]},group={position:1,accounts:['a','b'],before:{a:2,b:0},after:{a:1,b:1},target:{a:1,b:1},fixedLoad:{a:0,b:0},result:{status:'applied',moved:1}};
 const page=await ui(path=>{paths.push(path);if(!path.includes('/items?'))return response(header);const query=new URL('http://localhost'+path).searchParams,section=query.get('section'),after=Number(query.get('after'));return response(section==='groups'?{section,items:[group],after:1,has_more:false}:{section,items:[{position:after+1,session_id:'session-'+after,client_session_id:'client-'+after,from:'a',to:'b',weight:1,result:{status:after?'skipped':'applied'}}],after:after+1,has_more:after===0});});
 page.run('admin=true;state={capabilities:{pagedHistory:true}};');await page.click('nightly-history','2026-11-03');assert.match(page.node('#modal-content').innerHTML,/确认已迁移 1 个/);assert.match(page.node('#modal-content').innerHTML,/执行结果：已应用/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/仅预览，不修改绑定/);
 await page.click('planning-section','moves');assert.match(page.node('#modal-content').innerHTML,/client-0/);assert.match(page.node('#modal-content').innerHTML,/确认已迁移/);await page.click('planning-page','next');assert.match(page.node('#modal-content').innerHTML,/client-1/);assert.match(page.node('#modal-content').innerHTML,/整组跳过/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/client-0/);assert.ok(paths.every(path=>path.startsWith('/api/admin/history/nightly/')));
 await page.click('close');assert.equal(page.run('planningPreview'),null);assert.equal(page.node('#modal-content').innerHTML,'');
});

test('PostgreSQL nightly history retries a failed header via history and ignores a late response after close',async()=>{
 let calls=0,release;const paths=[];const page=await ui(path=>{paths.push(path);if(++calls===1)return response({error:{message:'合成读取失败'}},503);return new Promise(resolve=>{release=()=>resolve(response({day:'2026-11-03',paginated:true,hasPlan:false,visibleUntil:Date.now()+86400000,counts:{groups:0,moves:0,skipped:0,fixed:0},results:{applied:0,skipped:0},accounts:[]}));});});
 page.run('admin=true;state={capabilities:{pagedHistory:true}};');await page.click('nightly-history','2026-11-03');assert.match(page.node('#modal-content').innerHTML,/合成读取失败/);const pending=page.click('planning-retry');await new Promise(setImmediate);await page.click('close');release();await pending;assert.equal(page.run('planningPreview'),null);assert.equal(page.node('#modal-content').innerHTML,'');assert.ok(paths.every(path=>path.startsWith('/api/admin/history/nightly/')));
});

test('PostgreSQL nightly records without a saved plan do not pretend that planned or applied counts are zero',async()=>{
 const page=await ui(()=>response({day:'2026-11-03',status:'failed',paginated:true,hasPlan:false,visibleUntil:Date.now()+86400000,counts:{groups:0,moves:0,skipped:0,fixed:0},results:{applied:null,skipped:null},accounts:[]}));page.run('admin=true;state={capabilities:{pagedHistory:true}};');await page.click('nightly-history','2026-11-03');const html=page.node('#modal-content').innerHTML;assert.match(html,/无法确认计划数量和逐会话结果/);assert.doesNotMatch(html,/计划迁移 0|确认已迁移 0|data-action="planning-section"/);
});

const revisionHistoryHeader=revision=>({day:'2026-11-03',status:'failed',started:1,ended:2,paginated:true,hasPlan:true,visibleUntil:Date.now()+86400000,plannedAt:1,window:{start:0,end:1},counts:{groups:1,moves:revision===0?3:5,skipped:0,fixed:0},results:{applied:3,skipped:0},selectedResults:{applied:revision===0?2:1,skipped:0},accounts:[],revisionAware:true,revision,latestRevision:2,revisionCount:2,plannedTotal:8,previousRevision:revision===0?null:0,nextRevision:revision===0?2:null,waiting:0,waitingGroups:0,pendingRevision:{revision:3,status:'failed',reason:null}});
test('revision history separates slot actual totals from repeated plan counts and uses actual adjacent revisions',async()=>{
 const paths=[],page=await ui(path=>{paths.push(path);const query=new URL('http://localhost'+path).searchParams,revision=Number(query.get('revision')??2);return response(path.includes('/items?')?{section:'groups',items:[],after:0,has_more:false}:revisionHistoryHeader(revision));});page.run('admin=true;state={capabilities:{pagedHistory:true}};');await page.click('nightly-history','2026-11-03');assert.match(page.node('#modal-content').innerHTML,/累计实际迁移 3 个/);assert.match(page.node('#modal-content').innerHTML,/不能当作独立会话数/);assert.match(page.node('#modal-content').innerHTML,/当前修订 v2：计划 5 个，实际 1 个/);assert.match(page.node('#modal-content').innerHTML,/修订 v3：计划失败/);
 await page.click('history-revision-prev');assert.match(page.node('#modal-content').innerHTML,/当前修订 v0：计划 3 个，实际 2 个/);assert.ok(paths.some(path=>path.endsWith('?revision=0')));assert.ok(paths.some(path=>path.includes('/items?')&&path.endsWith('&revision=0')));await page.click('history-revision-next');assert.match(page.node('#modal-content').innerHTML,/当前修订 v2/);assert.ok(!paths.some(path=>path.endsWith('?revision=1')));
});
test('revision history ignores a previous-plan response after the dialog is replaced',async()=>{
 let release;const page=await ui(path=>{const query=new URL('http://localhost'+path).searchParams;if(query.get('revision')==='0'&&!path.includes('/items?'))return new Promise(resolve=>{release=()=>resolve(response(revisionHistoryHeader(0)));});return response(path.includes('/items?')?{section:'groups',items:[],after:0,has_more:false}:revisionHistoryHeader(2));});page.run('admin=true;state={capabilities:{pagedHistory:true}};');await page.click('nightly-history','2026-11-03');const pending=page.click('history-revision-prev');await new Promise(setImmediate);await page.click('close');page.run('modal("another form")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'another form');
});

test('request-boundary preview explains active candidates without promising a static quiet-period exclusion',async()=>{
 const header=planView();header.plan.requestBoundary=true;const page=await ui(path=>path.endsWith('/plans')?response({id:'plan-one'}):path.includes('/items?')?response({section:'groups',items:[],after:0,has_more:false}):response(header));preparePlanning(page);await page.click('balancing-preview');assert.match(page.node('#modal-content').innerHTML,/定时分配预览/);assert.match(page.node('#modal-content').innerHTML,/等待旧请求完成/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/静默期和当前配置/);
});

const deviceDirectory=(patch={})=>({rows:[{id:'11111111-1111-1111-1111-111111111111',member_id:'member-one',name:'合成工作站',system:'Windows / WSL',state:'pending',version:3,hardware_state:'unavailable',allowed_key_ids:null,credential_generation:0,request_count:0}],summary:{registered_devices:29,used_devices:7,unverified_requests:3},member:{id:'member-one',device_mode:'compatible',device_policy_version:2},pagination:{page:1,pageSize:25,total:29,totalPages:2},...patch});
const deviceSnapshot=()=>({...pagedSnapshot(),capabilities:{...pagedSnapshot().capabilities,deviceAccess:true,memberActions:true}});
test('设备页面按服务端全量summary显示且迟到响应不能污染其他弹窗',async()=>{
 let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response(deviceDirectory()));}));page.run('admin=true;state='+JSON.stringify(deviceSnapshot())+';tab="members";');const pending=page.click('member-devices','member-one');await new Promise(setImmediate);await page.click('close');page.run('modal("另一个弹窗")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'另一个弹窗');assert.equal(page.run('deviceView'),null);
});
test('设备只在可见待审批目录轮询，编辑关闭切页隐藏后不再自动读取',async()=>{
 let reads=0;const page=await ui(()=>{reads++;return response(deviceDirectory());});page.run('admin=true;state='+JSON.stringify(deviceSnapshot())+';tab="members";');await page.click('member-devices','member-one');assert.match(page.node('#devices-content').innerHTML,/登记 29 · 近7天使用 7 · 未认证请求 3/);await handoffPoll(page);assert.equal(reads,2);page.run('document.hidden=true');await handoffPoll(page);assert.equal(reads,2);page.run('document.hidden=false');await page.click('device-rename','11111111-1111-1111-1111-111111111111');await handoffPoll(page);assert.equal(reads,2);assert.match(page.node('#modal-content').innerHTML,/修改设备名称/);await page.click('close');await handoffPoll(page);assert.equal(reads,2);await page.click('member-devices','member-one');page.run('setTab("accounts")');assert.equal(page.run('deviceView'),null);
});
test('设备启停使用读取版本，409后不隐瞒冲突并读取新目录',async()=>{
 const writes=[];let reads=0;const row={...deviceDirectory().rows[0],state:'approved',hardware_state:'reported',version:7};const page=await ui((path,options)=>{if(options.method==='PATCH'){writes.push(JSON.parse(options.body));return response({error:{message:'设备已变化'}},409);}reads++;return response(deviceDirectory({rows:[row]}));});page.run('admin=true;state='+JSON.stringify(deviceSnapshot()));await page.click('member-devices','member-one');await page.click('device-toggle',row.id);assert.deepEqual(writes,[{expected_version:7,enabled:false}]);assert.equal(reads,2);assert.match(page.node('#toast').textContent,/设备已变化/);
});
const accessBatchRecord=(patch={})=>({id:'access-one',status:'preview',expires:Date.now()+3600000,created:Date.now(),direction:'apply',operation:{kind:'member_access',enabled:false},counts:{pending:1},items:[{ordinal:1,member_id:'member-one',member_name:'合成访问员工',selected_keys:[],before_state:{member:{enabled:1},keys:[]},desired_state:{member:{enabled:0},keys:[]},status:'pending',notes:[]}],after:1,has_more:false,...patch});
test('访问批次只提交固定批次ID，202后继续显示排队且关闭不再轮询',async()=>{
 let reads=0;const writes=[];const page=await ui((path,options)=>{if(options.method==='POST'){writes.push({path,body:JSON.parse(options.body)});return response({status:'queued',id:'access-one'});}reads++;return response(accessBatchRecord({status:reads===1?'preview':'queued'}));});page.run('admin=true;state='+JSON.stringify(deviceSnapshot()));await page.click('access-batch-open','access-one');await handoffPoll(page);assert.equal(reads,1);await page.click('access-batch-execute');assert.deepEqual(writes,[{path:'/api/admin/member-action-batches/access-one/execute',body:{}}]);assert.equal(page.run('accessBatchView.record.status'),'queued');await handoffPoll(page);assert.equal(reads,3);await page.click('close');await handoffPoll(page);assert.equal(reads,3);
});
test('访问批次刷新失败禁用执行，迟到结果不能污染新弹窗',async()=>{
 let reads=0,writes=0;const page=await ui((_path,options)=>{if(options.method==='POST'){writes++;return response({});}return ++reads===1?response(accessBatchRecord()):response({error:{message:'合成查询失败'}},503);});page.run('admin=true;state='+JSON.stringify(deviceSnapshot()));await page.click('access-batch-open','access-one');await page.click('access-batch-refresh');await page.click('access-batch-execute');assert.equal(writes,0);assert.match(page.node('#access-batch-content').innerHTML,/上次快照/);let release;const other=await ui(()=>new Promise(resolve=>{release=()=>resolve(response(accessBatchRecord()));}));other.run('admin=true;state='+JSON.stringify(deviceSnapshot()));const pending=other.click('access-batch-open','access-one');await new Promise(setImmediate);await other.click('close');other.run('modal("其他内容")');release();await pending;assert.equal(other.node('#modal-content').innerHTML,'其他内容');
});
test('请求设备列与详情只读当时快照，不将来源IP推断为设备',async()=>{
 const page=await ui(()=>response({}));page.run('admin=true;state='+JSON.stringify(deviceSnapshot()));const html=page.run('requestTable([{id:"request",started:1,ended:2,ip:"sensitive-network",device_name:"当时设备",device_system:"合成系统",device_auth_state:"authenticated",device_id:"11111111-1111-1111-1111-111111111111",standard:{}}])');assert.match(html,/当时设备/);assert.match(html,/设备已认证/);assert.ok(!html.includes('sensitive-network'));const unknown=page.run('phaseDetail({id:"r",ip:"sensitive-network",timeline:[]})');assert.match(unknown,/历史未记录/);assert.ok(!unknown.includes('sensitive-network'));
});
test('Key设备范围409保留跨页选择，读取新版本前禁止重发，翻页失败显示原因',async()=>{
 const id='11111111-1111-1111-1111-111111111111',old='22222222-2222-2222-2222-222222222222';let keyReads=0,deviceReads=0;const writes=[];
 const page=await ui((path,options)=>{if(options.method==='PATCH'){writes.push(JSON.parse(options.body));return response({error:{message:'Key设备策略已变化'}},409);}if(path.includes('/keys?'))return response({rows:[{id:'key-one',device_ids:[old],device_policy_version:++keyReads===1?2:5}],pagination:{page:1,totalPages:1}});if(++deviceReads===3)return response({error:{message:'设备分页暂不可用'}},503);return response(deviceDirectory({rows:[{...deviceDirectory().rows[0],state:'approved'}]}));});
 page.run('admin=true;state='+JSON.stringify(deviceSnapshot()));await page.run('openDevices("member-one","key-one")');await page.click('device-key-policy');assert.match(page.node('#modal-content').innerHTML,/移除 22222222/);await page.click('device-scope-remove',old);await page.click('device-scope-select',id);await page.click('device-scope-save');assert.equal(writes.length,1);assert.equal(writes[0].expected_version,2);assert.deepEqual(writes[0].device_ids,[id]);assert.match(page.node('#modal-content').innerHTML,/Key设备策略已变化/);await page.click('device-scope-save');assert.equal(writes.length,1);await page.click('device-scope-reload-policy');assert.match(page.node('#modal-content').innerHTML,/你的选择已保留/);await page.click('device-scope-save');assert.equal(writes[1].expected_version,5);assert.deepEqual(writes[1].device_ids,[id]);await page.click('device-scope-page','2');assert.match(page.node('#modal-content').innerHTML,/设备分页暂不可用/);
});
test('Key设备范围关闭后迟到保存失败不写入新弹窗',async()=>{
 let release;const page=await ui((path,options)=>options.method==='PATCH'?new Promise(resolve=>{release=()=>resolve(response({error:{message:'过时冲突'}},409));}):path.includes('/keys?')?response({rows:[{id:'key-one',device_ids:null,device_policy_version:2}],pagination:{totalPages:1}}):response(deviceDirectory({rows:[]})));page.run('admin=true;state='+JSON.stringify(deviceSnapshot()));await page.run('openDevices("member-one","key-one")');await page.click('device-key-policy');const pending=page.click('device-scope-save');await new Promise(setImmediate);await page.click('close');page.run('modal("新的内容")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'新的内容');
});
test('员工新访问版本不泄漏给原批量配置预览的严格版本契约',async()=>{
 const page=await ui(()=>response({types:[]}));page.run('admin=true;state='+JSON.stringify(deviceSnapshot())+';memberSelection=new Map([["member-one",{id:"member-one",access_version:9,usage_type_version:3,defaults_version:4}]]);globalThis.capturedScope=null;configureMemberBatch=async scope=>{capturedScope=scope;};');await page.click('members-configure-selected');assert.deepEqual(JSON.parse(page.run('JSON.stringify(capturedScope.member_versions)')),[{id:'member-one',usage_type_version:3,defaults_version:4}]);
});
test('设备改名保存后保留原目录页与筛选，编辑不被轮询覆盖',async()=>{
 const queries=[];const id='11111111-1111-1111-1111-111111111111';let patches=0;const page=await ui((path,options)=>{if(options.method==='PATCH'){patches++;return response({});}queries.push(path);return response(deviceDirectory({pagination:{page:2,pageSize:25,total:27,totalPages:2}}));});page.run('admin=true;state='+JSON.stringify(deviceSnapshot()));await page.run('openDevices("member-one","",{page:2,state:"pending"})');await page.click('device-rename',id);page.run('globalThis.FormData=class{get(){return "保留页设备";}}');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(patches,1);assert.ok(queries.every(path=>path.includes('page=2')&&path.includes('state=pending')));assert.equal(page.run('deviceView.page'),2);
});

const typeStatisticsFixture=(patch={})=>({summary:{requests:3,standard:{total:6.15},outcomes:{success:1,failed:1,queued:0,running:0,cancelled:0,rejected:1,interrupted:0,unknown:0},failureRate:0.5},rows:[{id:'type:usage-a',type:'usage-a',name:'复杂研发',code:'A',standard:{total:6.15},outcomes:{success:1,failed:1,queued:0,running:0,cancelled:0,rejected:1,interrupted:0,unknown:0},failureRate:0.5,requestFilters:{type:'usage-a',member:'',model:'',outcome:'failed',from:1000,to:2000,member_missing:false,model_missing:false}}],pagination:{page:1,pageSize:25,total:1,totalPages:1},observedAt:1500,...patch});
const typeSnapshot=()=>({...snapshot(),keys:[],sessions:[],capabilities:{typeStatistics:true}});
test('类型统计显示精确参考USD与业务失败率，未知和未分类分开，不把人工核对当结果',async()=>{
 const page=await ui(path=>response(path.includes('member-types')?{types:[{id:'usage-a',code:'A',name:'复杂研发'}]}:typeStatisticsFixture()));page.run('admin=true;state='+JSON.stringify(typeSnapshot())+';tab="requests";recordView="types";');await page.run('loadTypeStatistics()');const html=page.node('#content').innerHTML;assert.match(html,/\$6\.15/);assert.match(html,/50\.00%/);assert.match(html,/历史类型未知/);assert.match(html,/未分类（新请求）/);assert.match(html,/业务结果独立于人工费用核对/);assert.ok(!html.includes('原始Token'));assert.ok(!html.includes('已核对</option>'));
});
test('类型统计迟到响应不能污染不同页签，筛选变化取消旧读取',async()=>{
 let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response(typeStatisticsFixture()));}));page.run('admin=true;state='+JSON.stringify(typeSnapshot())+';tab="requests";recordView="types";');const pending=page.run('loadTypeStatistics()');await new Promise(setImmediate);page.run('setTab("accounts")');release();await pending;assert.equal(page.run('typeStatisticsData'),null);assert.equal(page.run('tab'),'accounts');assert.ok(!page.node('#content').innerHTML.includes('50.00%'));
});
test('类型分组下钻使用服务器完整requestFilters并重置分页快照，缺失身份标记不丢失',async()=>{
 const paths=[];const page=await ui(path=>{paths.push(path);return response({rows:[],pagination:{page:1,pageSize:25,total:0,totalPages:1},summary:{requests:0,success:0,standard:{},average_ms:0},models:[],members:[],byModel:[],byMember:[]});});const data=typeStatisticsFixture();data.rows[0].requestFilters={...data.rows[0].requestFilters,model_missing:true};page.run('admin=true;state='+JSON.stringify(typeSnapshot())+';tab="requests";recordView="types";typeStatisticsData='+JSON.stringify(data)+';requestsPageSnapshot="old";requestsCursors=new Map([[1,"old-cursor"]]);');await page.click('type-statistics-drill','0');const url=new URL(paths[0],'http://localhost');assert.equal(url.searchParams.get('type'),'usage-a');assert.equal(url.searchParams.get('outcome'),'failed');assert.equal(url.searchParams.get('from'),'1000');assert.equal(url.searchParams.get('to'),'2000');assert.equal(url.searchParams.get('model_missing'),'true');assert.equal(url.searchParams.has('snapshot'),false);assert.equal(url.searchParams.has('cursor'),false);assert.equal(url.searchParams.has('status'),false);assert.equal(page.run('recordView'),'requests');assert.match(page.node('#content').innerHTML,/清除固定筛选/);
});
test('总览选择类型后不再把全公司用量和日图展示为筛选结果，健康人数明确全公司',async()=>{
 const page=await ui(()=>response(typeStatisticsFixture()));page.run('admin=true;state='+JSON.stringify(typeSnapshot())+';tab="overview";overviewType="usage-a";typeStatisticsData='+JSON.stringify(typeStatisticsFixture())+';render();');const html=page.node('#content').innerHTML;assert.match(html,/可用账号（全公司）/);assert.match(html,/员工（全公司）/);assert.match(html,/类型请求概览/);assert.match(html,/请求／成功／失败/);assert.doesNotMatch(html,/参考消耗|USD|\$6\.15|<table|data-action="type-statistics-drill"|data-action="type-statistics-page"/);assert.ok(!html.includes('最近 7 天请求'));assert.ok(!html.includes('近12小时参考消耗'));
});
test('总览与使用类型页的业务结果筛选独立保存，互不改变统计范围',async()=>{
 const paths=[];const page=await ui(path=>{paths.push(path);return response(typeStatisticsFixture());});page.run('admin=true;state='+JSON.stringify(typeSnapshot())+';tab="requests";recordView="types";typeStatisticsFilters.status="failed";');await page.run('loadTypeStatistics()');page.run('tab="overview";overviewType="usage-a";');await page.run('loadTypeStatistics()');assert.equal(new URL(paths[0],'http://localhost').searchParams.get('status'),'failed');assert.equal(new URL(paths[1],'http://localhost').searchParams.has('status'),false);assert.equal(page.run('typeStatisticsFilters.status'),'failed');
});
test('费用人工核对后的请求仍显示原业务失败，核对状态只单独说明',async()=>{
 const page=await ui(()=>response({}));page.run('admin=true;state='+JSON.stringify(typeSnapshot()));const html=page.run('requestTable([{id:"r",started:1,ended:2,status:"reconciled",business_status:"failed",standard:{}}])');assert.match(html,/>失败<\/span>/);assert.match(html,/费用已人工核对/);assert.ok(!html.includes('>已核对</span>'));
});

const recordingAdminSnapshot=()=>({...typeSnapshot(),capabilities:{...typeSnapshot().capabilities,recordingHistory:true}});
const recordingSearchPage=(patch={})=>({items:[{id:'recording-one',started:Date.now()-1000,model:'gpt-6.1-sol',usage_type_version:1,usage_type_code:'A',usage_type_name:'复杂研发',kind:'user',snippet:'合成中文问题 <img src=x onerror=alert(1)>',session_id:'session-one',expires_at:Date.now()+3600000}],next:null,has_more:false,index_backlog:0,complete:true,range:{from:Date.now()-86400000,to:Date.now()},...patch});
const recordingDocumentPage=(patch={})=>({state:'complete',turn_state:'known',source_sequence:2,indexed_at:100,index_pending:false,expires_at:Date.now()+3600000,items:[{ordinal:1,kind:'user',text:'<script>不得执行()</script>',metadata:{},continuation:false}],next:1,has_more:false,...patch});
const exportTask=(patch={})=>({id:'recording.export:11111111-1111-1111-1111-111111111111',status:'queued',job_status:'queued',rows:0,bytes:0,chunks:0,created:Date.now(),expires:Date.now()+3600000,include_body:false,format:'csv',limits:{rows:1000,bytes:1048576},download_ready:false,...patch});
const prepareRecordingAdmin=page=>page.run('admin=true;state='+JSON.stringify(recordingAdminSnapshot())+';tab="requests";recordView="recordings";');
test('正文管理默认只显示条件，进入和返回不读取正文状态目录或旧导出任务',async()=>{
 const calls=[],page=await ui(path=>{calls.push(path);return response(path.includes('/status')?{index_pending:0,index_failed:0,storage:{status:'healthy'}}:path.includes('/exports')?{items:[],next_cursor:null}:recordingSearchPage());});
 prepareRecordingAdmin(page);page.run('recordView="usage";recordingAdminFilters.q="保留查询草稿";recordingAdminTab="exports";recordingAdminData='+JSON.stringify(recordingSearchPage())+';recordingAdminStatus={index_pending:7};recordingExportList={items:[],next_cursor:null};');
 await page.click('record-view','recordings');await new Promise(setImmediate);
 assert.deepEqual(calls,[]);assert.equal(page.run('recordingAdminData'),null);assert.equal(page.run('recordingAdminStatus'),null);assert.equal(page.run('recordingAdminQuery'),null);assert.equal(page.run('recordingExportList'),null);assert.equal(page.run('recordingAdminTab'),'query');assert.equal(page.run('recordingAdminFilters.q'),'保留查询草稿');
 assert.ok(!page.node('#content').innerHTML.includes('合成中文问题'));assert.ok(!page.node('#content').innerHTML.includes('后台状态待读取'));
 for(const timer of page.intervals)timer.fn();await new Promise(setImmediate);assert.deepEqual(calls,[]);
 await page.click('recording-admin-status');assert.deepEqual(calls,['/api/admin/recordings/status']);
 await page.run('loadRecordingAdminQuery(true)');assert.ok(calls[1].startsWith('/api/admin/recordings/search?'));
 await page.click('recording-admin-tab','exports');assert.ok(calls[2].startsWith('/api/admin/recordings/exports?'));
 page.run('setTab("accounts");setTab("requests")');await new Promise(setImmediate);assert.equal(calls.length,3);assert.equal(page.run('recordingAdminData'),null);assert.equal(page.run('recordingAdminStatus'),null);assert.equal(page.run('recordingExportList'),null);assert.equal(page.run('recordingAdminTab'),'query');
});
test('会话正文入口只带入条件，查询仍由管理员主动发起',async()=>{
 const paths=[],page=await ui(path=>{paths.push(path);return response(recordingSearchPage());});prepareRecordingAdmin(page);
 await page.run('openRecordingSession("session-explicit")');assert.deepEqual(paths,[]);assert.equal(page.run('recordingAdminFilters.session'),'session-explicit');assert.equal(page.run('recordingAdminData'),null);
 await page.run('loadRecordingAdminQuery(true)');assert.equal(paths.length,1);assert.ok(paths[0].startsWith('/api/admin/recordings/session?'));assert.equal(new URL(paths[0],'http://localhost').searchParams.get('session'),'session-explicit');
});
test('正文查询固定时间和游标，字面中文代码正确编码且片段始终转义',async()=>{
 const paths=[];const page=await ui(path=>{paths.push(path);return response(recordingSearchPage({has_more:paths.length===1,next:paths.length===1?'opaque-next':null}));});prepareRecordingAdmin(page);page.run('recordingAdminFilters.q="中文 createInvoice()";');await page.run('loadRecordingAdminQuery(true)');assert.match(page.node('#content').innerHTML,/&lt;img src=x onerror=alert\(1\)&gt;/);assert.ok(!page.node('#content').innerHTML.includes('<img src=x'));await page.click('recording-query-page','next');const first=new URL(paths[0],'http://localhost'),next=new URL(paths[1],'http://localhost');assert.equal(first.searchParams.get('q'),'中文 createInvoice()');assert.equal(first.searchParams.get('from'),next.searchParams.get('from'));assert.equal(next.searchParams.get('cursor'),'opaque-next');
});
test('修改正文筛选会使在途查询失效，切页后迟到响应不显示正文',async()=>{
 let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response(recordingSearchPage()));}));prepareRecordingAdmin(page);page.run('recordingAdminFilters.q="原问题";');const pending=page.run('loadRecordingAdminQuery(true)');await new Promise(setImmediate);page.run('recordingAdminFilters.q="新问题";invalidateRecordingQuery();');release();await pending;assert.equal(page.run('recordingAdminData'),null);assert.match(page.node('#recording-query-results').innerHTML,/筛选已修改/);
});
test('正文详情按需加载并转义，整理版本变化拒绝混页，过期立即清空已有正文',async()=>{
 let reads=0;const page=await ui(()=>response(++reads===1?recordingDocumentPage({has_more:true}):reads===2?recordingDocumentPage({source_sequence:3,indexed_at:200,next:2}):recordingDocumentPage({state:'expired',items:[],has_more:false,next:1,source_sequence:undefined,indexed_at:undefined})));prepareRecordingAdmin(page);await page.run('showRecordingDocument({id:"recording-one",started:1})');assert.match(page.node('#recording-document-content').innerHTML,/&lt;script&gt;/);await page.click('recording-document-more');assert.match(page.node('#recording-document-content').innerHTML,/避免混合不同版本/);assert.equal(page.run('recordingDocumentView.items.length'),1);await page.click('recording-document-reload');assert.equal(page.run('recordingDocumentView.items.length'),0);assert.equal(page.run('recordingDocumentView.state'),'expired');
});
test('正文详情关闭后迟到内容不会进入新的弹窗',async()=>{
 let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response(recordingDocumentPage()));}));prepareRecordingAdmin(page);const pending=page.run('showRecordingDocument({id:"recording-one"})');await new Promise(setImmediate);await page.click('close');page.run('modal("新弹窗内容")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'新弹窗内容');assert.equal(page.run('recordingDocumentView'),null);
});
test('后台导出默认无正文CSV，关键词必须明确排除，提交202后仍显示排队',async()=>{
 const writes=[];const page=await ui((path,options)=>{if(options.method==='POST'){writes.push(JSON.parse(options.body));return response(exportTask());}return response(exportTask());});prepareRecordingAdmin(page);page.run('recordingAdminFilters.q="需要单独确认的关键词";');await page.click('recording-export-create');assert.match(page.node('#modal-content').innerHTML,/不会用于导出/);page.run('globalThis.FormData=class{get(key){return {export_kind:"metadata",exclude_q_confirm:"on"}[key]||null;}}');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(writes.length,1);assert.equal(writes[0].include_body,false);assert.equal(writes[0].format,'csv');assert.equal(Object.hasOwn(writes[0].filters,'q'),false);assert.match(page.node('#recording-export-task').innerHTML,/排队中/);assert.match(page.node('#recording-export-task').innerHTML,/data-action="recording-export-download"[^>]+disabled/);
});
test('正文导出必须显式选择JSONL，结构化筛选不随页面后续编辑变化',async()=>{
 let body;const page=await ui((path,options)=>options.method==='POST'?(body=JSON.parse(options.body),response(exportTask({include_body:true,format:'jsonl'}))):response(exportTask({include_body:true,format:'jsonl'})));prepareRecordingAdmin(page);page.run('recordingAdminFilters.type="usage-a";');await page.click('recording-export-create');page.run('recordingAdminFilters.type="usage-b";globalThis.FormData=class{get(key){return key==="export_kind"?"body":null;}}');await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(body.include_body,true);assert.equal(body.format,'jsonl');assert.equal(body.filters.type,'usage-a');
});
test('导出任务只在可见处理中轮询，后台失败和关闭后停止',async()=>{
 let reads=0;const page=await ui(()=>response(exportTask({job_status:++reads>=3?'failed':'queued'})));prepareRecordingAdmin(page);await page.run('openRecordingTask('+JSON.stringify(exportTask().id)+')');await handoffPoll(page);assert.equal(reads,2);page.run('document.hidden=true');await handoffPoll(page);assert.equal(reads,2);page.run('document.hidden=false');await handoffPoll(page);assert.equal(reads,3);assert.match(page.node('#recording-export-task').innerHTML,/失败/);await handoffPoll(page);assert.equal(reads,3);await page.click('close');await handoffPoll(page);assert.equal(reads,3);
});
test('导出任务列表使用服务端不透明游标分页，切换页面后迟到列表不污染',async()=>{
 const paths=[];const page=await ui(path=>{paths.push(path);return response({items:[exportTask()],next_cursor:paths.length===1?'server-opaque':null});});prepareRecordingAdmin(page);await page.click('recording-admin-tab','exports');await page.click('recording-export-list-page','next');assert.equal(new URL(paths[1],'http://localhost').searchParams.get('before'),'server-opaque');assert.match(page.node('#content').innerHTML,/刷新网页后仍可查看/);let release;const other=await ui(()=>new Promise(resolve=>{release=()=>resolve(response({items:[exportTask()],next_cursor:null}));}));prepareRecordingAdmin(other);const pending=other.click('recording-admin-tab','exports');await new Promise(setImmediate);other.run('setTab("accounts")');release();await pending;assert.equal(other.run('recordingExportList'),null);
});
test('正文保留期必须先预览，确认只提交固定plan版本，409后禁止重复确认',async()=>{
 const plan={id:'22222222-2222-2222-2222-222222222222',days:7,previous_days:30,expected_version:4,expires:Date.now()+300000,affected_requests:3,affected_bytes:1234},writes=[];const page=await ui((_path,options)=>{writes.push(JSON.parse(options.body));return response({error:{message:'保留期配置已变化'}},409);});prepareRecordingAdmin(page);page.run('showRecordingRetentionPreview('+JSON.stringify(plan)+')');assert.equal(writes.length,0);assert.match(page.node('#recording-retention-content').innerHTML,/3 条/);await page.click('recording-retention-confirm');assert.deepEqual(writes,[{id:plan.id,expected_version:4,confirm:true}]);assert.match(page.node('#recording-retention-content').innerHTML,/保留期配置已变化/);await page.click('recording-retention-confirm');assert.equal(writes.length,1);assert.match(page.node('#recording-retention-content').innerHTML,/重新生成影响预览/);
});
test('附件只有真实类型与完整字节才交给浏览器，下载错误不会生成伪文件',async()=>{
 const task=exportTask({status:'complete',job_status:'completed',bytes:12,download_ready:true});let corrupt=true;const page=await ui(path=>path.endsWith('/download')?{ok:true,status:200,headers:{get(name){return name==='content-type'?'text/csv; charset=utf-8':'attachment; filename="test.csv"';}},blob:async()=>({size:corrupt?3:12})}:response(task));prepareRecordingAdmin(page);await page.run('openRecordingTask('+JSON.stringify(task.id)+')');await page.click('recording-export-download');assert.equal(page.downloads.length,0);assert.match(page.node('#recording-export-task').innerHTML,/附件字节不完整/);corrupt=false;await page.click('recording-export-task-refresh');await page.click('recording-export-download');assert.deepEqual(page.downloads,['Harbor-请求元数据.csv']);
});
test('可见搜索片段在到期后本地移除正文，不等待全局刷新或重新查询',async()=>{
 const page=await ui(()=>response({}));prepareRecordingAdmin(page);page.run('recordingAdminData='+JSON.stringify(recordingSearchPage({items:[{id:'old',snippet:'到期应清除的敏感正文',expires_at:Date.now()-1}]}))+';recordingAdminQuery={q:"正文"};expireRecordingSearchResults();');assert.equal(page.run('recordingAdminData.items[0].snippet'),'正文已过期');assert.ok(!page.node('#recording-query-results').innerHTML.includes('敏感正文'));
});
test('关闭下载弹窗后迟到的附件不会触发浏览器下载',async()=>{
 const task=exportTask({status:'complete',job_status:'completed',bytes:12,download_ready:true});let release;const page=await ui(path=>path.endsWith('/download')?{ok:true,status:200,headers:{get(name){return name==='content-type'?'text/csv':'attachment; filename="test.csv"';}},blob:()=>new Promise(resolve=>{release=()=>resolve({size:12});})}:response(task));prepareRecordingAdmin(page);await page.run('openRecordingTask('+JSON.stringify(task.id)+')');const pending=page.click('recording-export-download');await new Promise(setImmediate);await page.click('close');page.run('modal("其他弹窗")');release();await pending;assert.equal(page.downloads.length,0);assert.equal(page.node('#modal-content').innerHTML,'其他弹窗');
});
test('请求页导出员工筛选不会错误显示正文查询中另一个员工名称',async()=>{
 const page=await ui(()=>response({}));prepareRecordingAdmin(page);page.run('recordingAdminFilters.member="member-a";recordingAdminFilters.memberLabel="另一个员工";requestsFilters.member="member-b";');await page.click('requests-export-structured');assert.match(page.node('#modal-content').innerHTML,/member-b/);assert.ok(!page.node('#modal-content').innerHTML.includes('另一个员工'));
});
test('在途正文查询被编辑取消后立即恢复提交按钮，旧返回不阻挡新查询',async()=>{
 let release,reads=0;const page=await ui(()=>++reads===1?new Promise(resolve=>{release=()=>resolve(response(recordingSearchPage()));}):response(recordingSearchPage()));prepareRecordingAdmin(page);page.run('recordingAdminFilters.q="旧问题";');const pending=page.run('loadRecordingAdminQuery(true)');await new Promise(setImmediate);const submit={disabled:true};page.node('#recording-query-form').querySelector=()=>submit;page.run('recordingAdminFilters.q="新问题";invalidateRecordingQuery();');assert.equal(submit.disabled,false);release();await pending;assert.equal(page.run('recordingAdminData'),null);await page.run('loadRecordingAdminQuery(true)');assert.equal(reads,2);assert.equal(page.run('recordingAdminQuery.q'),'新问题');
});
test('正文多页展开完毕后轮询只读当前游标，保留已有内容且版本变化仍拒绝混合',async()=>{
 const paths=[];const page=await ui(path=>{paths.push(path);return response(paths.length===1?recordingDocumentPage({items:[{ordinal:1,kind:'user',text:'第一页'}],next:1,has_more:true,state:'pending',index_pending:true}):paths.length===2?recordingDocumentPage({items:[{ordinal:2,kind:'assistant',text:'第二页'}],next:2,state:'pending',index_pending:true}):paths.length===3?recordingDocumentPage({items:[],next:2,state:'pending',index_pending:true}):recordingDocumentPage({items:[{ordinal:3,kind:'tool_result',text:'新整理版本不能混入'}],next:3,source_sequence:9,indexed_at:200,state:'pending',index_pending:true}));});prepareRecordingAdmin(page);await page.run('showRecordingDocument({id:"recording-one"})');await page.click('recording-document-more');await handoffPoll(page);assert.match(paths[2],/after=2&/);assert.equal(page.run('recordingDocumentView.items.length'),2);assert.match(page.node('#recording-document-content').innerHTML,/第二页/);await handoffPoll(page);assert.match(paths[3],/after=2&/);assert.equal(page.run('recordingDocumentView.items.length'),2);assert.match(page.node('#recording-document-content').innerHTML,/避免混合不同版本/);assert.ok(!page.node('#recording-document-content').innerHTML.includes('新整理版本不能混入'));
});
test('保留期预览到期后明确提示重新预览，后台耗尽失败不继续伪装处理中',async()=>{
 let reads=0;const page=await ui(()=>{reads++;return response({});});prepareRecordingAdmin(page);page.run('showRecordingRetentionPreview({id:"22222222-2222-2222-2222-222222222222",days:7,previous_days:30,expected_version:1,affected_requests:2,affected_bytes:10,expires:Date.now()-1})');await page.click('recording-retention-confirm');assert.equal(reads,0);assert.match(page.node('#recording-retention-content').innerHTML,/预览已过期/);page.run('recordingRetentionView.data={status:"queued",job_status:"failed",error_code:"synthetic_job_exhausted",days:7,counts:{pending:2}};recordingRetentionView.error="";renderRecordingRetention(recordingRetentionView);');assert.match(page.node('#recording-retention-content').innerHTML,/失败/);await handoffPoll(page);assert.equal(reads,0);
});
test('正文结果保留员工归属，可按需查看，缺姓名不按正文推断身份',async()=>{
 const page=await ui(()=>response({}));prepareRecordingAdmin(page);page.run('recordingAdminData='+JSON.stringify(recordingSearchPage({items:[{id:'r',member_id:'owner-one',snippet:'某个用户名字只是正文',usage_type_version:1}]}))+';recordingAdminQuery={q:"正文"};render();');const html=page.node('#content').innerHTML;assert.match(html,/员工标识 …wner-one/);assert.match(html,/data-action="member-detail" data-id="owner-one"/);assert.match(html,/查看员工/);assert.equal(page.run('recordingOwnerLabel({member_id:"owner-one"})'),'员工标识 …wner-one');
});

const packageDefinitionFixture=(patch={})=>({name:'合成容量套餐',platform:'codex',auth_modes:['subscription'],protocols:['responses'],models:['*'],comparison:{group:'synthetic-capacity',unit:'relative',rule_id:'synthetic-rule',rule_version:1,units:{relative:1}},official:{ids:['synthetic-plan'],granularity:'exact'},source:{kind:'administrator',reference:'合成容量依据',confirmed:true},effective_at:0,windows:[{id:'short',minutes:300,capacity:100,unit:'relative'}],...patch});
const packageCatalogFixture=(patch={})=>({items:[{id:'pkg-one',latest_version:1,definition:packageDefinitionFixture(),accounts:2}],policy:{enabled:false,smoothing_hours:24,version:2},...patch});
const packageBudgetFixture=(patch={})=>({id:'account-a',mode:'capacity',eligible:true,score:2,estimate:true,reason:null,recognition:{plan:'synthetic-plan',status:'match',capacity_confirmed:true},windows:[{id:'short',minutes:300,remaining:50,unit:'relative',hours:5,denominator:24,score:2,source:'package_estimate',reset:Date.now()+3600000}],...patch});
const packageSelectionFixture=(patch={})=>({mode:'selected',revision:3,recognition_version:4,observed_plan:'synthetic-plan',template_id:'pkg-one',template_version:1,definition:packageDefinitionFixture(),capacity_override:null,...patch});
const packageSnapshot=()=>({...recordingAdminSnapshot(),capabilities:{accountPackages:true},accounts:[{id:'account-a',name:'合成小容量账户',kind:'codex',auth_mode:'subscription',enabled:1,state:'ready',package_selection:packageSelectionFixture(),package:packageBudgetFixture(),quota_json:JSON.stringify({checked:Date.now(),plan:'synthetic-plan',windows:[{minutes:300,used:50,reset:Date.now()+3600000}]})}],balancing:{strategy:{configured:false,effective:true,source:'package_capacity',package_policy:{enabled:true,smoothing_hours:24,version:2}},settings:{weighted:false,nightly:false,failover:false,quietMinutes:30},weights:{mode:'capacity',configuration:{mode:'auto',weights:{},expires:null,version:1},accounts:[{id:'account-a',automatic:20,effective:20,configured:null,package:packageBudgetFixture()}]}}});
const packageFormValues=(patch={})=>({template_id:'pkg-one',name:'合成容量套餐',platform:'codex',auth_modes:['subscription'],protocols:'responses',models:'*',comparison_group:'synthetic-capacity',comparison_unit:'relative',rule_id:'synthetic-rule',rule_version:'1',unit_name:['relative'],unit_factor:['1'],window_id:['short'],window_minutes:['300'],window_capacity_mode:['estimate'],window_capacity:['100'],window_unit:['relative'],official_ids:'synthetic-plan',official_granularity:'exact',source_kind:'administrator',source_reference:'合成容量依据',source_confirmed:'on',effective_at:'2026-11-03T00:00:00',...patch});
function useSyntheticForm(page,values){page.run('globalThis.FormData=class{constructor(){}get(key){const values='+JSON.stringify(values)+';const value=values[key];return Array.isArray(value)?value[0]??null:value??null;}getAll(key){const values='+JSON.stringify(values)+';const value=values[key];return value===undefined?[]:Array.isArray(value)?value:[value];}}');}
const packagePreviewFixture=(patch={})=>({id:'package-preview-one',next_version:2,expires:Date.now()+3600000,changes:[],automatic_reference:{before:[],after:[]},affected_accounts:2,...patch});
test('套餐账号池区分官方比例、人工容量估算和自动/人工/有效权重，不拿费用当容量',async()=>{
 const page=await ui(()=>response({}));page.run('admin=true;state='+JSON.stringify(packageSnapshot())+';tab="accounts";packageCatalog='+JSON.stringify(packageCatalogFixture())+';render();');const html=page.node('#content').innerHTML;assert.match(html,/官方窗口快照/);assert.match(html,/剩余 50%/);assert.match(html,/调度估算/);assert.match(html,/自动参考 20%/);assert.match(html,/最终生效 20%/);assert.match(html,/套餐策略已开启/);assert.match(html,/预算已启用/);assert.match(html,/套餐容量预算/);
});
test('账号池套餐筛选命名空间不会把名为api的真实模板当成按量API',async()=>{
 const page=await ui(()=>response({})),state=packageSnapshot();state.accounts[0].package_selection.template_id='api';state.accounts.push({id:'api-key',name:'按量测试',kind:'codex',auth_mode:'openai_api',enabled:1,state:'ready',api_weight:10});page.run('admin=true;state='+JSON.stringify(state)+';accountPoolFilters.package="template:api";');assert.equal(page.run('packageAccountRows().length'),1);assert.equal(page.run('packageAccountRows()[0].id'),'account-a');page.run('accountPoolFilters.package="kind:api"');assert.equal(page.run('packageAccountRows()[0].id'),'api-key');
});
test('套餐模板普通表单生成多窗口与换算规则，版本和固定目标保持明确，个体覆盖默认不覆盖',async()=>{
 let body;const page=await ui((path,options)=>options.method==='POST'?(body=JSON.parse(options.body),response(packagePreviewFixture())):response(packageCatalogFixture()));page.run('admin=true;state='+JSON.stringify(packageSnapshot())+';packageCatalog='+JSON.stringify(packageCatalogFixture()));await page.click('package-template-edit','pkg-one');useSyntheticForm(page,packageFormValues({unit_name:['relative','tokens'],unit_factor:['1','0.5'],window_id:['short','week'],window_minutes:['300','10080'],window_capacity_mode:['estimate','official'],window_capacity:['100',''],window_unit:['relative','tokens']}));await page.node('#package-editor-form').onsubmit({preventDefault(){},target:{}});assert.equal(body.expected_version,1);assert.deepEqual(body.account_ids,['account-a']);assert.deepEqual(body.overwrite_overrides,[]);assert.equal(body.definition.windows.length,2);assert.equal(body.definition.windows[1].capacity,null);assert.equal(body.definition.comparison.units.tokens,0.5);assert.match(page.node('#package-preview-content').innerHTML,/固定 0 个账号/);
});
test('重复来源单位不能被静默覆盖，错误保留表单输入而不是提交',async()=>{
 let writes=0;const page=await ui(()=>{writes++;return response({});});page.run('admin=true;state='+JSON.stringify(packageSnapshot())+';packageCatalog='+JSON.stringify(packageCatalogFixture()));await page.click('package-template-edit','pkg-one');useSyntheticForm(page,packageFormValues({unit_name:['relative','relative'],unit_factor:['1','4']}));await page.node('#package-editor-form').onsubmit({preventDefault(){},target:{}});assert.equal(writes,0);assert.match(page.node('#package-editor-error').textContent,/来源单位不能重名/);
});
test('套餐预览确认仅提交固定ID和空对象，关闭后的迟到应用不污染新弹窗',async()=>{
 let release,writes=[];const page=await ui((path,options)=>{writes.push({path,body:JSON.parse(options.body)});return new Promise(resolve=>{release=()=>resolve(response({status:'applied',version:2,applied:[],retained:[]}));});});page.run('admin=true;state='+JSON.stringify(packageSnapshot())+';showPackagePreview('+JSON.stringify(packagePreviewFixture())+',{id:"pkg-one",version:1,draft:'+JSON.stringify(packageDefinitionFixture())+',targets:[],overwrites:[]})');const pending=page.click('package-preview-apply');await new Promise(setImmediate);await page.click('close');page.run('modal("其他弹窗")');release();await pending;assert.deepEqual(writes,[{path:'/api/admin/account-packages/previews/package-preview-one/apply',body:{}}]);assert.equal(page.node('#modal-content').innerHTML,'其他弹窗');
});
test('套餐覆盖预览列缺口后才允许明确开启，使用真实policy版本，409禁止重复提交',async()=>{
 const writes=[],coverage={strategy:{package_policy:{enabled:true,version:2}},coverage:{unknown:1,legacy:1,comparable_groups:2,reason:'incomparable_package_budget'},accounts:[{id:'account-a',name:'合成账户',automatic:null,effective:null,package:packageBudgetFixture({reason:'official_plan_conflict',recognition:{status:'conflict',capacity_confirmed:false}})}]};const page=await ui((path,options)=>{if(options.method==='PATCH'){writes.push(JSON.parse(options.body));return response({error:{message:'套餐策略版本已变化'}},409);}return response(path.endsWith('/coverage')?coverage:packageCatalogFixture());});page.run('admin=true;state='+JSON.stringify(packageSnapshot()));await page.click('package-policy');assert.match(page.node('#package-policy-content').innerHTML,/未知套餐 1/);assert.match(page.node('#package-policy-content').innerHTML,/可比较组 2/);assert.match(page.node('#package-policy-content').innerHTML,/官方冲突 1/);useSyntheticForm(page,{confirm_coverage:'on'});await page.node('#package-policy-form').onsubmit({preventDefault(){},target:{}});assert.deepEqual(writes,[{enabled:true,expected_version:2}]);await page.node('#package-policy-form').onsubmit({preventDefault(){},target:{}});assert.equal(writes.length,1);assert.match(page.node('#package-policy-content').innerHTML,/套餐策略版本已变化/);
});
test('账号人工容量确认绑定当前识别代次和官方标识，不按family猜档位',async()=>{
 let sent;const detail={account_id:'account-a',selection:packageSelectionFixture(),budget:packageBudgetFixture({recognition:{plan:'synthetic-plan',status:'partial',capacity_confirmed:false}}),policy:{enabled:true,version:2}};const page=await ui((path,options)=>options.method==='PATCH'?(sent=JSON.parse(options.body),response({error:{message:'官方识别已变化'}},409)):response(path.endsWith('/package')?detail:packageCatalogFixture()));page.run('admin=true;state='+JSON.stringify(packageSnapshot()));await page.click('package-account','account-a');assert.match(page.node('#package-account-content').innerHTML,/不能证明具体额度档位/);useSyntheticForm(page,{package_choice:'pkg-one@1',confirm_official:'on',reason:'已人工核对真实档位与容量'});await page.node('#package-account-form').onsubmit({preventDefault(){},target:{}});assert.equal(sent.expected_revision,3);assert.equal(sent.expected_recognition_version,4);assert.equal(sent.expected_official_plan,'synthetic-plan');assert.equal(sent.confirm_official,true);assert.equal(sent.capacity_override,null);assert.match(page.node('#package-account-content').innerHTML,/官方识别已变化/);assert.match(page.node('#package-account-content').innerHTML,/type="submit" disabled/);
});
test('套餐开启时负载设置不能把disabled的旧weighted误保存成false',async()=>{
 let sent;const state=packageSnapshot();state.balancing.settings.weighted=true;const page=await ui((path,options)=>options.method==='PATCH'?(sent=JSON.parse(options.body),response({})):response(state));page.run('admin=true;state='+JSON.stringify(state));await page.click('balancing-settings');assert.match(page.node('#modal-content').innerHTML,/select name="weighted" disabled/);assert.match(page.node('#modal-content').innerHTML,/原24小时预算开关只读/);useSyntheticForm(page,{failover:'false',nightly:'false',quietMinutes:'30'});await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(Object.hasOwn(sent,'weighted'),false);
});
test('添加按量API时不发送残留的订阅套餐字段，未知订阅则显式保存null套餐',async()=>{
 const writes=[];const snapshot=packageSnapshot(),page=await ui((path,options)=>{if(options.method==='POST'){writes.push(JSON.parse(options.body));return response({id:'new-account'});}return response(path.includes('account-packages')?packageCatalogFixture():snapshot);});page.run('admin=true;state='+JSON.stringify(snapshot)+';tab="accounts";');await page.click('create-account');useSyntheticForm(page,{auth_mode:'openai_api',name:'合成API账户',api_key:'sk-proj-synthetic-test-only',api_weight:'12',package_choice:'pkg-one@1'});await page.node('#package-create-account-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(writes[0].auth_mode,'openai_api');assert.equal(writes[0].api_weight,12);assert.equal(Object.hasOwn(writes[0],'package'),false);await page.click('close');await page.click('create-account');useSyntheticForm(page,{auth_mode:'subscription',kind:'codex',name:'未知合成订阅',package_choice:''});await page.node('#package-create-account-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.deepEqual(writes[1].package,{template_id:null});assert.match(page.node('#modal-content').innerHTML,/明确未知/);
});
test('套餐创建目录迟到和保存后的刷新迟到都不能替换用户新打开的弹窗',async()=>{
 let release;const page=await ui(()=>new Promise(resolve=>{release=()=>resolve(response(packageCatalogFixture()));}));page.run('admin=true;state='+JSON.stringify(packageSnapshot())+';tab="accounts";');const pending=page.click('create-account');await new Promise(setImmediate);await page.click('close');page.run('modal("另一个对话框")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'另一个对话框');
 let releaseState;const other=await ui((path,options)=>options.method==='POST'?response({id:'new-account'}):path.includes('account-packages')?response(packageCatalogFixture()):new Promise(resolve=>{releaseState=()=>resolve(response(packageSnapshot()));}));other.run('admin=true;state='+JSON.stringify(packageSnapshot())+';tab="accounts";');await other.click('create-account');useSyntheticForm(other,{auth_mode:'subscription',kind:'codex',name:'合成订阅',package_choice:''});const submit=other.node('#package-create-account-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});await new Promise(setImmediate);other.run('modal("刷新期间打开的其他弹窗")');releaseState();await submit;assert.equal(other.node('#modal-content').innerHTML,'刷新期间打开的其他弹窗');
});
test('套餐模板409后显式读新版本保留容量草稿，再预览才用新版本',async()=>{
 const writes=[];let reads=0;const page=await ui((path,options)=>{if(options.method==='POST'){writes.push(JSON.parse(options.body));return writes.length===1?response({error:{message:'模板版本已变化'}},409):response(packagePreviewFixture());}reads++;return response(packageCatalogFixture({items:[{id:'pkg-one',latest_version:4,definition:packageDefinitionFixture({name:'服务器已更新模板'}),accounts:1}]}));});page.run('admin=true;state='+JSON.stringify(packageSnapshot())+';packageCatalog='+JSON.stringify(packageCatalogFixture()));await page.click('package-template-edit','pkg-one');useSyntheticForm(page,packageFormValues({window_capacity:['150']}));await page.node('#package-editor-form').onsubmit({preventDefault(){},target:{}});assert.match(page.node('#package-editor-content').innerHTML,/模板版本已变化/);await page.click('package-editor-read-version');assert.equal(page.run('packageEditorView.version'),4);assert.equal(page.run('packageEditorView.draft.windows[0].capacity'),150);assert.match(page.node('#package-editor-content').innerHTML,/服务器已更新模板/);await page.node('#package-editor-form').onsubmit({preventDefault(){},target:{}});assert.equal(writes[1].expected_version,4);assert.equal(writes[1].definition.windows[0].capacity,150);assert.equal(reads,1);
});
test('官方绝对额度与百分比分别展示，零额度与过期快照不被隐藏或冒充容量估算',async()=>{
 const page=await ui(()=>response({})),snapshot=packageSnapshot();snapshot.accounts[0].quota_json=JSON.stringify({checked:Date.now()-600000,plan:'synthetic-plan',windows:[{minutes:300,reset:Date.now()-1,absolute:{source:'official',remaining:0,unit:'official-unit'}}]});page.run('admin=true;state='+JSON.stringify(snapshot)+';tab="accounts";render();');let html=page.node('#content').innerHTML;assert.match(html,/官方绝对剩余量 0 official-unit/);assert.match(html,/比例未知/);assert.match(html,/上次快照，待刷新/);snapshot.accounts[0].quota_json=JSON.stringify({checked:Date.now(),windows:[{minutes:300,used:40,reset:Date.now()+3600000,absolute:{source:'official',remaining:123,unit:'official-unit'}}]});page.run('state='+JSON.stringify(snapshot)+';render();');html=page.node('#content').innerHTML;assert.match(html,/剩余 60%/);assert.match(html,/官方绝对剩余量 123 official-unit/);assert.match(html,/调度估算/);
});
test('临时人工比例有效的定时批次明确显示跳过，保持02时到期，不包装为平衡成功或失败重试',async()=>{
 const page=await ui(()=>response({}));page.run('admin=true;state='+JSON.stringify(packageSnapshot())+';modal("预览");const view={epoch:authEpoch,dialog:dialogVersion,controller:new AbortController(),history:false,section:"skipped",items:[{reason:"manual_weights_active"}],plan:{skipReason:"manual_weights_active",counts:{moves:0,skipped:1,groups:0,fixed:0},window:{end:Date.now()},requestBoundary:true},names:new Map(),back:[],hasMore:false,expires:Date.now()+3600000,plannedAt:Date.now()};planningPreview=view;renderPlanningPreview(view);');const html=page.node('#modal-content').innerHTML;assert.match(html,/本时点明确跳过自动迁移/);assert.match(html,/人工比例保持到下次北京时间02:00/);assert.ok(!html.includes('data-action="planning-new"'));assert.equal(page.run('nightlyRunStatus({status:"interrupted",reason:"manual_weights_active"})'),'按人工权重跳过自动迁移');
});
test('只读weighted以打开表单时状态固定，迟到刷新关闭套餐后提交仍不写weighted',async()=>{
 let sent;const original=packageSnapshot();original.balancing.settings.weighted=true;const refreshed=packageSnapshot();refreshed.balancing.strategy.package_policy.enabled=false;refreshed.balancing.settings.weighted=false;const page=await ui((path,options)=>options.method==='PATCH'?(sent=JSON.parse(options.body),response({})):response(refreshed));page.run('admin=true;state='+JSON.stringify(original));await page.click('balancing-settings');assert.match(page.node('#modal-content').innerHTML,/select name="weighted" disabled/);page.run('state='+JSON.stringify(refreshed));useSyntheticForm(page,{failover:'true',nightly:'false',quietMinutes:'30'});await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});assert.equal(Object.hasOwn(sent,'weighted'),false);assert.equal(sent.failover,true);
});
test('关闭套餐且原预算开关关闭时标原兼容策略，不把无百分比的旧调度误报成缺失额度',async()=>{
 const snapshot=packageSnapshot();snapshot.balancing.strategy={source:'legacy',effective:false,configured:false,package_policy:{enabled:false,version:3}};const page=await ui(()=>response({}));page.run('admin=true;state='+JSON.stringify(snapshot)+';tab="accounts";render();');assert.match(page.node('#content').innerHTML,/最终生效 原兼容策略（不使用百分比）/);assert.ok(!page.node('#content').innerHTML.includes('旧 weighted'));
});
test('R57 同一套餐弹窗切换平台与接入方式立即更新目标，保留完整草稿和显式选择',async()=>{
 let sent;const snapshot=packageSnapshot();snapshot.accounts.push({id:'qwen-one',name:'合成Qwen账号',kind:'qwen',auth_mode:'platform',enabled:1,state:'ready'});const page=await ui((path,options)=>options.method==='POST'?(sent=JSON.parse(options.body),response(packagePreviewFixture())):response(packageCatalogFixture()));page.run('admin=true;state='+JSON.stringify(snapshot)+';packageCatalog='+JSON.stringify(packageCatalogFixture()));await page.click('package-template-edit','pkg-one');
 const target=()=>page.node('#package-editor-content').innerHTML.match(/<input[^>]+data-action="package-target-toggle"[^>]+data-id="qwen-one"[^>]*>/)?.[0];assert.match(target(),/disabled/);
 useSyntheticForm(page,packageFormValues({platform:'qwen',auth_modes:['platform'],name:'未保存的完整草稿',window_capacity:['175'],source_reference:'切换前已填写的依据'}));await page.node('#package-editor-form [name="platform"]').onchange();assert.ok(!target().includes('disabled'));assert.equal(page.run('packageEditorView.draft.name'),'未保存的完整草稿');assert.equal(page.run('packageEditorView.draft.windows[0].capacity'),175);assert.equal(page.run('packageEditorView.draft.source.reference'),'切换前已填写的依据');assert.equal(page.run('packageEditorView.targets.has("account-a")'),true,'不兼容的原目标须由用户明确取消');
 await page.click('package-target-toggle','account-a');await page.click('package-target-toggle','qwen-one');
 useSyntheticForm(page,packageFormValues({platform:'qwen',auth_modes:['subscription'],name:'未保存的完整草稿',window_capacity:['175']}));await page.node('#package-editor-form [name="auth_modes"]').onchange();assert.ok(!target().includes('disabled'),'已选不兼容目标仍可取消');assert.match(page.node('#package-editor-content').innerHTML,/与当前新范围不匹配，请取消选择/);assert.equal(page.run('packageEditorView.targets.has("qwen-one")'),true);
 useSyntheticForm(page,packageFormValues({platform:'qwen',auth_modes:['platform'],name:'未保存的完整草稿',window_capacity:['175']}));await page.node('#package-editor-form [name="auth_modes"]').onchange();await page.node('#package-editor-form').onsubmit({preventDefault(){},target:{}});assert.deepEqual(sent.account_ids,['qwen-one']);assert.equal(sent.definition.platform,'qwen');assert.deepEqual(sent.definition.auth_modes,['platform']);assert.equal(sent.definition.windows[0].capacity,175);
});

const managedAccounts=()=>[{id:'sub',name:'合成订阅账号',kind:'codex',auth_mode:'subscription',enabled:1,state:'ready'},{id:'api-one',name:'合成API Key一',kind:'codex',auth_mode:'openai_api',enabled:1,state:'ready',api_weight:10},{id:'api-two',name:'合成API Key二',kind:'codex',auth_mode:'openai_api',enabled:0,state:'ready',api_weight:0},{id:'platform',name:'合成平台账号',kind:'deepseek',auth_mode:'platform',enabled:1,state:'ready'}];
for(const packages of [false,true])for(const extra of [false,true])test(`账号列表仅保留管理入口，套餐${packages}与权益${extra}组合不丢操作`,async()=>{
 const page=await ui(()=>response({})),data={...packageSnapshot(),accounts:managedAccounts(),capabilities:{accountPackages:packages,extraUsage:extra}};page.run('admin=true;state='+JSON.stringify(data)+';tab="accounts";render();');const list=page.node('#content').innerHTML;assert.equal((list.match(/data-action="manage-account"/g)||[]).length,4);for(const action of ['check-account','api-account-settings','create-fixed-session','platform-check','toggle-account','remove-account','package-account','extra-account'])assert.ok(!list.includes(`data-action="${action}"`),action+' leaked to list');
 for(const account of data.accounts){await page.click('manage-account',account.id);const html=page.node('#modal-content').innerHTML;assert.ok(html.includes(account.name));assert.match(html,/连接与认证/);assert.match(html,/账号状态/);assert.ok(html.includes('data-action="toggle-account"'));assert.ok(html.includes('data-action="remove-account"'));assert.equal(html.includes('data-action="extra-account"'),extra);assert.equal(html.includes('data-action="package-account"'),packages&&account.auth_mode!=='openai_api');assert.equal(html.includes('data-action="api-account-settings"'),account.auth_mode==='openai_api');assert.equal(html.includes('data-action="create-fixed-session"'),account.auth_mode==='openai_api');if(account.id==='api-two')assert.match(html,/data-action="create-fixed-session"[^>]*disabled/);if(account.auth_mode==='platform'){assert.match(html,/data-action="platform-check"/);assert.match(html,/data-action="platform-credential"/);}assert.equal(page.run('accountManagementId'),account.id);await page.click('close');assert.equal(page.run('accountManagementId'),'');}
});
test('账号管理复用原停用与移除确认，不因打开管理而写入',async()=>{
 const writes=[],page=await ui((path,options)=>{writes.push({path,method:options.method});return response({});});page.run('admin=true;state='+JSON.stringify({...snapshot(),accounts:managedAccounts()})+';');await page.click('manage-account','api-one');assert.equal(writes.length,0);await page.click('toggle-account','api-one');assert.match(page.node('#modal-content').innerHTML,/立即迁移全部绑定会话/);assert.match(page.node('#modal-content').innerHTML,/data-action="confirm-disable-account"/);assert.equal(writes.length,0);await page.click('manage-account','api-one');await page.click('remove-account','api-one');assert.match(page.node('#modal-content').innerHTML,/历史用量、会话记录和独立目录保留/);assert.match(page.node('#modal-content').innerHTML,/data-action="confirm-remove-account"/);assert.equal(writes.length,0);
});
test('检查Key后只更新仍在查看的账号管理，迟到检查不覆盖其他弹窗',async()=>{
 let release;const data={...snapshot(),accounts:managedAccounts()},page=await ui((path,options)=>path.endsWith('/check')?new Promise(resolve=>{release=()=>resolve(response({ready:true}));}):response(data));page.run('admin=true;state='+JSON.stringify(data)+';');await page.click('manage-account','api-one');const checking=page.click('check-account','api-one');await new Promise(setImmediate);await page.click('manage-account','api-two');release();await checking;assert.match(page.node('#modal-content').innerHTML,/合成API Key二/);assert.equal(page.run('accountManagementId'),'api-two');
});

// Foreground cancellation and automatic paged reads exercise separate lifecycles.
test('会话翻页读取中离开并返回会释放遮罩，旧响应不覆盖新查询',async()=>{
 let release;const page=await ui(()=>new Promise(resolve=>release=()=>resolve(response({...snapshot(),sessions:[],busy:9}))));page.run('admin=true;state='+JSON.stringify({...snapshot(),sessions:[]})+';tab="sessions";sessionsLoading=true;sessionsPage=2;render();');const pending=page.run('refresh()');page.run('setTab("accounts");setTab("sessions")');assert.equal(page.run('sessionsLoading'),false);assert.equal(page.run('pageRefreshing()'),false);assert.equal(page.run('sessionsPage'),1);release();await pending;assert.equal(page.run('state.busy'),0);assert.equal(page.run('sessionsLoading'),false);
 const next=page.run('refresh()');assert.equal(page.run('pageRefreshing()'),true);release();await next;assert.equal(page.run('state.busy'),9);assert.equal(page.run('pageRefreshing()'),false);
});
test('使用数据Key后台读取不启用前景遮罩，手动读取仍反馈',async()=>{
 let release;const value=pagedSnapshot(),result={rows:[],pagination:{page:1,pageSize:25,total:0,totalPages:1}};const page=await ui(path=>path.startsWith('/api/admin/keys?')?new Promise(resolve=>release=()=>resolve(response(result))):response(value));page.run('admin=true;state='+JSON.stringify(value)+';tab="requests";recordView="usage";keyUsageScope="requests:";render();');const pending=page.run('loadKeyUsage(true)');await new Promise(setImmediate);assert.equal(page.run('keyUsageLoading'),false);assert.equal(page.run('pageRefreshing()'),false);assert.ok(!page.node('#content').innerHTML.includes('aria-busy="true"'));release();await pending;
 const manual=page.run('loadKeyUsage()');assert.equal(page.run('keyUsageLoading'),true);release();await manual;assert.equal(page.run('keyUsageLoading'),false);
});

test('正文类型筛选按需加载目录，同一在途去重并原位更新且失败可重试',async()=>{
 const paths=[];let release,fail=true;const page=await ui(path=>{paths.push(path);return new Promise(resolve=>release=()=>resolve(fail?response({error:{message:'目录暂时失败'}},503):response({types:[{id:'type-a',code:'A',name:'研发'}]})));});prepareRecordingAdmin(page);const select={value:'historical_unknown',innerHTML:''};page.node('#recording-query-form').querySelector=()=>select;page.run('bindRecordingAdmin()');const form=page.node('#recording-query-form'),event={target:{name:'type',tagName:'SELECT'}};assert.deepEqual(paths,[]);select.onfocus(event);form.onpointerdown(event);assert.deepEqual(paths,['/api/admin/member-types']);release();await new Promise(setImmediate);assert.equal(page.run('typeCatalogController'),null);fail=false;form.onpointerdown(event);select.onfocus(event);assert.equal(paths.length,2);release();await new Promise(setImmediate);assert.match(select.innerHTML,/A · 研发/);assert.match(select.innerHTML,/value="historical_unknown" selected/);assert.equal(select.value,'historical_unknown');assert.equal(page.run('recordingAdminData'),null);assert.equal(page.run('recordingAdminStatus'),null);assert.ok(paths.every(path=>path==='/api/admin/member-types'));
});
test('概览已在途类型目录进入正文后只更新筛选，不自动读取正文或覆盖草稿',async()=>{
 let release;const paths=[],page=await ui(path=>{paths.push(path);return new Promise(resolve=>release=()=>resolve(response({types:[{id:'type-a',code:'A',name:'研发'}]})));});prepareRecordingAdmin(page);page.run('tab="overview"');const pending=page.run('loadTypeCatalog()');page.run('tab="requests";recordView="recordings";recordingAdminFilters.q="保留草稿";');const select={value:'type-a',innerHTML:''};page.node('#recording-query-form').querySelector=()=>select;page.run('bindRecordingAdmin()');select.onfocus({target:{name:'type',tagName:'SELECT'}});assert.equal(paths.length,1);release();await pending;assert.match(select.innerHTML,/A · 研发/);assert.equal(page.run('recordingAdminFilters.q'),'保留草稿');assert.deepEqual(paths,['/api/admin/member-types']);
});

test('运行总览只显示请求趋势和运行状态，金额只在使用数据展示',async()=>{
 const data={...snapshot(),usageWindow:{start:Date.UTC(2026,10,1)},daily:[{day:'2026-11-01',requests:7,standard:{total:123.45}}]};
 const page=await ui(()=>response(data));page.run('admin=true;state='+JSON.stringify(data)+';tab="overview";render();');
 const html=page.node('#content').innerHTML;assert.match(html,/最近 7 天请求/);assert.match(html,/处理中 \/ 排队/);assert.match(html,/<span class="chart-label">7<\/span>/);assert.doesNotMatch(html,/参考消耗|Standard|USD|\$123\.45/);
});


test('总览宽屏布局仅作用于管理员总览，切页与员工入口恢复默认宽度',async()=>{
 const page=await ui(()=>response(snapshot())),classes=new Map();page.node('.page').classList.toggle=(name,value)=>classes.set(name,value);page.run('admin=true;state='+JSON.stringify(snapshot())+';tab="overview";render()');assert.equal(classes.get('overview-page'),true);page.run('tab="accounts";render()');assert.equal(classes.get('overview-page'),false);page.run('admin=false;tab="playground";render()');assert.equal(classes.get('overview-page'),false);
});
test('页头省去空说明与英文眉题，接入测试保留能力限制',async()=>{
 const page=await ui(()=>response(snapshot()));const title=page.run('heading("INTERNAL LABEL","标题","")');assert.equal(title,'<div class="page-heading"><div><h1>标题</h1></div></div>');page.run('admin=true;state='+JSON.stringify(snapshot())+';tab="playground";render()');const html=page.node('#content').innerHTML;assert.doesNotMatch(html,/用员工 Key 验证鉴权、订阅调用与额度统计|使用员工 Key 从 Windows、WSL 或其他机器连接网关/);assert.match(html,/不执行代码或访问工作文件/);
});
