import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const snapshot=()=>({totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],daily:[],requests:[],busy:0,queue:0});
const response=(body,status=200)=>({ok:status<400,status,json:async()=>body});
async function ui(fetcher){
  const nodes=new Map(),handlers=new Map(),downloads=[],popups=[],timers=[],copied=[],intervals=[];
  const node=id=>{if(!nodes.has(id))nodes.set(id,{dataset:{},classList:{values:new Set(),add(v){this.values.add(v);},remove(v){this.values.delete(v);},toggle(){}},value:'',textContent:'',innerHTML:'',open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
  class BrowserURL extends URL{static createObjectURL(){return 'blob:test-only';}static revokeObjectURL(){}}
  const context=vm.createContext({Blob,AbortController,URLSearchParams,navigator:{clipboard:{writeText:async value=>copied.push(value)}},document:{querySelector:node,querySelectorAll:()=>[],addEventListener(type,handler){handlers.set(type,handler);},createElement(){return {click(){downloads.push(this.download);}};},activeElement:{tagName:'BODY'}},URL:BrowserURL,window:{open(){const popup={closed:false,close(){this.closed=true;},location:{replace(url){popup.url=url;}}};popups.push(popup);return popup;}},sessionStorage:{removeItem(){}},location:{hash:''},setInterval(fn,delay){intervals.push({fn,delay});},setTimeout(fn,delay){const timer={fn,delay,active:true};timers.push(timer);return timer;},clearTimeout(timer){if(timer)timer.active=false;},fetch:async(path,options)=>path==='/api/auth/session'?response({authenticated:false}):fetcher(path,options),console});
  vm.runInContext(source,context);await new Promise(setImmediate);
  return {node,downloads,popups,timers,copied,intervals,click:(action,id='')=>handlers.get('click')({target:{closest:selector=>selector==='[data-action]'?{dataset:{action,id}}:null}}),run:code=>vm.runInContext(code,context)};
}

const key={id:'key-one',member_id:'member-one',prefix:'synthetic',enabled:1,allowed_models:['gpt-6.1-sol'],model_policy_version:3,consumption_policy:'subscription_only',consumption_policy_version:4};
const fixture=(capability=true)=>({...snapshot(),capabilities:{consumptionPolicy:capability},members:[{id:'member-one',name:'合成员工',enabled:1}],keys:[key]});
function form(page,values){page.run('globalThis.FormData=class{get(key){return '+JSON.stringify(values)+'[key];}}');return page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {disabled:false};}}});}
function prepare(page,capability=true){page.run('admin=true;state='+JSON.stringify(fixture(capability))+';tab="members";');}
const policy=patch=>({...key,key_id:key.id,...patch});
for(const capability of [false,undefined,'true',1])test('旧后端能力 '+String(capability)+' 不显示或发送来源',async()=>{
 const writes=[],reads=[];const page=await ui((path,options)=>{reads.push(path);if(options.method==='PATCH'){writes.push(JSON.parse(options.body));return response({});}if(path==='/api/admin/pricing')return response({rates:[]});return response(fixture(capability));});prepare(page,capability);if(capability===undefined)page.run('delete state.capabilities.consumptionPolicy');await page.click('key-models',key.id);assert.doesNotMatch(page.node('#modal-content').innerHTML,/name="consumption_policy"/);await form(page,{model_policy_mode:'limited',allowed_models:'gpt-6.1-sol'});assert.deepEqual(writes,[{allowed_models:['gpt-6.1-sol'],expected_model_policy_version:3}]);assert.equal(reads.some(path=>path.endsWith('/policy')),false);
});
test('编辑Key权威读取四种来源并提交两个版本',async()=>{
 const writes=[];const page=await ui((path,options)=>{if(options.method==='PATCH'){writes.push(JSON.parse(options.body));return response({});}return response(path.endsWith('/policy')?policy({consumption_policy:'api_first',consumption_policy_version:8,model_policy_version:6}):path==='/api/admin/pricing'?{rates:[]}:fixture());});prepare(page);await page.click('key-models',key.id);const html=page.node('#modal-content').innerHTML;for(const value of ['inherit','api_only','subscription_only','api_first'])assert.match(html,new RegExp('value="'+value+'"'));assert.match(html,/value="api_first" selected/);await form(page,{model_policy_mode:'all',consumption_policy:'api_only'});assert.deepEqual(writes,[{allowed_models:null,expected_model_policy_version:6,consumption_policy:'api_only',expected_consumption_policy_version:8}]);
});
for(const patch of [{consumption_policy:'unknown'},{consumption_policy_version:null},{consumption_policy_version:-1},{key_id:'another'},{member_id:'another'},{model_policy_version:null}])test('未知Key权限不得默填或保存 '+JSON.stringify(patch),async()=>{
 let writes=0;const page=await ui((path,options)=>{if(options.method==='PATCH')writes++;return response(path.endsWith('/policy')?policy(patch):{rates:[]});});prepare(page);await page.click('key-models',key.id);assert.match(page.node('#modal-content').innerHTML,/未取得完整权限/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/name="consumption_policy"/);assert.equal(writes,0);
});
test('冲突重新权威读取并等待用户再次确认，不自动重试',async()=>{
 let reads=0;const writes=[];const page=await ui((path,options)=>{if(options.method==='PATCH'){writes.push(JSON.parse(options.body));return writes.length===1?response({error:{message:'changed'}},409):response({});}return response(path.endsWith('/policy')?policy({consumption_policy:++reads===1?'subscription_only':'api_first',consumption_policy_version:reads+4}):path==='/api/admin/pricing'?{rates:[]}:fixture());});prepare(page);await page.click('key-models',key.id);await form(page,{model_policy_mode:'all',consumption_policy:'api_only'});assert.equal(writes.length,1);assert.equal(reads,2);assert.match(page.node('#modal-content').innerHTML,/重新读取/);assert.match(page.node('#modal-content').innerHTML,/value="api_first" selected/);await form(page,{model_policy_mode:'all',consumption_policy:'subscription_only'});assert.equal(writes.length,2);assert.equal(writes[1].expected_consumption_policy_version,6);
});
test('迟到权限读取不能覆盖已关闭的弹窗',async()=>{
 let release;const page=await ui(path=>path.endsWith('/policy')?new Promise(resolve=>release=()=>resolve(response(policy()))):response({rates:[]}));prepare(page);const pending=page.click('key-models',key.id);await new Promise(setImmediate);await page.click('close');page.run('modal("other")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'other');
});
for(const capability of [true,false])test('创建首Key按能力发送来源且不自报版本 '+capability,async()=>{
 let sent;const page=await ui((path,options)=>{if(path==='/api/admin/members'){sent=JSON.parse(options.body);return response({secret:'synthetic',keyId:'new'});}return response(fixture(capability));});prepare(page,capability);await page.click('create-member');assert.equal(page.node('#modal-content').innerHTML.includes('name="consumption_policy"'),capability);await form(page,{name:'测试',days:'0',model_policy_mode:'all',consumption_policy:'api_first'});assert.equal(sent.consumption_policy,capability?'api_first':undefined);assert.equal(Object.hasOwn(sent,'consumption_policy_version'),false);assert.equal(Object.hasOwn(sent,'expected_consumption_policy_version'),false);
});
for(const capability of [true,false])test('新增Key按能力发送来源且保留模型权限 '+capability,async()=>{
 let sent;const page=await ui((path,options)=>{if(path==='/api/admin/keys'){sent=JSON.parse(options.body);return response({secret:'synthetic',keyId:'new'});}return response(fixture(capability));});prepare(page,capability);await page.click('new-key','member-one');assert.equal(page.node('#modal-content').innerHTML.includes('name="consumption_policy"'),capability);await form(page,{days:'0',model_policy_mode:'limited',allowed_models:'gpt-6.1-sol',consumption_policy:'api_only'});assert.deepEqual(sent.allowed_models,['gpt-6.1-sol']);assert.equal(sent.consumption_policy,capability?'api_only':undefined);assert.equal(Object.hasOwn(sent,'consumption_policy_version'),false);assert.equal(Object.hasOwn(sent,'expected_consumption_policy_version'),false);
});

test('类型创建首Key包含来源，不发送客户端版本',async()=>{
 let sent;const page=await ui((path,options)=>{if(path==='/api/admin/members'){sent=JSON.parse(options.body);return response({secret:'synthetic',keyId:'new'});}return response(fixture(true));});prepare(page);page.run('showTypedMemberForm([{id:"type-one",name:"合成类型",enabled:true,version:2,default_version:3}],[])');assert.match(page.node('#modal-content').innerHTML,/name="consumption_policy"/);await form(page,{name:'测试',type_id:'type-one',days:'0',models_mode:'template',limits_mode:'template',consumption_policy:'subscription_only'});assert.equal(sent.consumption_policy,'subscription_only');assert.equal(sent.expected_template_version,3);assert.equal(Object.hasOwn(sent,'consumption_policy_version'),false);
});
test('已打开表单能力消失时阻止权限保存',async()=>{
 let writes=0;const page=await ui((path,options)=>{if(options.method==='PATCH')writes++;return response(path.endsWith('/policy')?policy():{rates:[]});});prepare(page);await page.click('key-models',key.id);page.run('state.capabilities.consumptionPolicy=false');await form(page,{model_policy_mode:'all',consumption_policy:'api_only'});assert.equal(writes,0);assert.match(page.node('#form-error').textContent,/能力已变化/);
});

for(const allowed of [undefined,'all',{},[null],['']])test('未知模型权限不默填无限制 '+JSON.stringify(allowed),async()=>{
 let writes=0;const page=await ui((path,options)=>{if(options.method==='PATCH')writes++;return response(path.endsWith('/policy')?policy({allowed_models:allowed}):{rates:[]});});prepare(page);await page.click('key-models',key.id);assert.match(page.node('#modal-content').innerHTML,/未取得完整权限/);assert.equal(writes,0);
});
for(const entry of ['member','typed','key'])test('创建途中能力变化阻止提交 '+entry,async()=>{
 let writes=0;const page=await ui((_path,options)=>{if(options.method==='POST')writes++;return response(fixture());});prepare(page);if(entry==='typed')page.run('showTypedMemberForm([{id:"type-one",enabled:true,version:2,default_version:3}],[])');else await page.click(entry==='member'?'create-member':'new-key','member-one');page.run('state.capabilities.consumptionPolicy=false');await form(page,{name:'测试',type_id:'type-one',days:'0',models_mode:'template',limits_mode:'template',model_policy_mode:'all',consumption_policy:'subscription_only'});assert.equal(writes,0);assert.match(page.node('#form-error').textContent,/能力已变化/);
});
test('首次读取失败显示重读入口',async()=>{
 const page=await ui(path=>path.endsWith('/policy')?response({error:{message:'synthetic unavailable'}},503):response({rates:[]}));prepare(page);await page.click('key-models',key.id);assert.match(page.node('#modal-content').innerHTML,/无法读取Key权限/);assert.match(page.node('#modal-content').innerHTML,/重新读取/);
});
test('冲突后重读失败可恢复，不遗留加载中',async()=>{
 let reads=0,writes=0;const page=await ui((path,options)=>{if(options.method==='PATCH'){writes++;return response({error:{message:'changed'}},409);}return path.endsWith('/policy')?(++reads===1?response(policy()):response({error:{message:'unavailable'}},503)):response({rates:[]});});prepare(page);await page.click('key-models',key.id);await form(page,{model_policy_mode:'all',consumption_policy:'api_only'});assert.equal(writes,1);assert.match(page.node('#modal-content').innerHTML,/无法读取Key权限/);
});
test('关闭后的迟到读取失败不覆盖其他弹窗',async()=>{
 let release;const page=await ui(path=>path.endsWith('/policy')?new Promise(resolve=>release=()=>resolve(response({error:{message:'unavailable'}},503))):response({rates:[]}));prepare(page);const pending=page.click('key-models',key.id);await new Promise(setImmediate);await page.click('close');page.run('modal("other")');release();await pending;assert.equal(page.node('#modal-content').innerHTML,'other');
});
