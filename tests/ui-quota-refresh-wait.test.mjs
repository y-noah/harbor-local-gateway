import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const snapshot=()=>({totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],daily:[],requests:[],busy:0,queue:0});
const response=(body,status=200)=>({ok:status<400,status,json:async()=>body});
async function ui(fetcher,{restore=false}={}){
  const nodes=new Map(),handlers=new Map(),downloads=[],popups=[],timers=[],copied=[],intervals=[];
  const node=id=>{if(!nodes.has(id))nodes.set(id,{dataset:{},classList:{values:new Set(),add(v){this.values.add(v);},remove(v){this.values.delete(v);},toggle(){}},value:'',textContent:'',innerHTML:'',insertAdjacentHTML(_position,html){this.innerHTML=html+this.innerHTML;},open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
  class BrowserURL extends URL{static createObjectURL(){return 'blob:test-only';}static revokeObjectURL(){}}
  const context=vm.createContext({Blob,AbortController,URLSearchParams,navigator:{clipboard:{writeText:async value=>copied.push(value)}},document:{querySelector:node,querySelectorAll:()=>[],addEventListener(type,handler){handlers.set(type,handler);},createElement(){return {click(){downloads.push(this.download);}};},activeElement:{tagName:'BODY'}},URL:BrowserURL,window:{open(){const popup={closed:false,close(){this.closed=true;},location:{replace(url){popup.url=url;}}};popups.push(popup);return popup;}},sessionStorage:{removeItem(){}},location:{hash:''},setInterval(fn,delay){intervals.push({fn,delay});},setTimeout(fn,delay){const timer={fn,delay,active:true};timers.push(timer);return timer;},clearTimeout(timer){if(timer)timer.active=false;},fetch:async(path,options)=>path==='/api/auth/session'&&!restore?response({authenticated:false}):fetcher(path,options),console});
  vm.runInContext(source,context);await new Promise(setImmediate);
  return {node,downloads,popups,timers,copied,intervals,click:(action,id='',dataset={})=>handlers.get('click')({target:{closest:selector=>selector==='[data-action]'?{dataset:{action,id,...dataset}}:null}}),run:code=>vm.runInContext(code,context)};
}

const prepare=page=>page.run('admin=true;state='+JSON.stringify(snapshot())+';tab="accounts";render();');
const tick=()=>new Promise(setImmediate);
const expire=page=>page.timers.find(timer=>timer.active&&timer.delay===30000).fn();
test('官方刷新等待到期显示未确认提示并GET重读，不重复POST且解除UI等待',async()=>{
 const calls=[];const page=await ui((path,options)=>{calls.push([path,options.method]);return options.method==='POST'?new Promise(()=>{}):response(snapshot());});prepare(page);
 const pending=page.node('#refresh').onclick();await tick();expire(page);await pending;
 assert.equal(page.run('manualRefreshPending'),false);assert.equal(page.node('#refresh').disabled,false);assert.match(page.node('#page-read-feedback').innerHTML,/结果尚未确认.*后台可能仍在刷新/);assert.equal(calls.filter(([p])=>p==='/api/admin/quotas/refresh').length,1);assert.ok(calls.some(([p])=>p.startsWith('/api/admin/state')));
 await page.node('#refresh').onclick();assert.equal(calls.filter(([p])=>p==='/api/admin/quotas/refresh').length,1);
 await page.click('retry-workspace');assert.equal(calls.filter(([p])=>p==='/api/admin/quotas/refresh').length,1);
});
test('原请求迟到完成不改UI；人工消费已确认结果后才允许下一次POST',async()=>{
 let release;const calls=[];const page=await ui((path,options)=>{calls.push([path,options.method]);return options.method==='POST'&&calls.filter(([,m])=>m==='POST').length===1?new Promise(resolve=>release=()=>resolve(response({}))):response(snapshot());});prepare(page);
 const pending=page.node('#refresh').onclick();await tick();expire(page);await pending;const before=page.node('#page-read-feedback').innerHTML;release();await tick();assert.equal(page.node('#page-read-feedback').innerHTML,before);
 await page.node('#refresh').onclick();assert.equal(calls.filter(([,m])=>m==='POST').length,1);assert.doesNotMatch(page.node('#page-read-feedback').innerHTML,/结果尚未确认/);
 await page.node('#refresh').onclick();assert.equal(calls.filter(([,m])=>m==='POST').length,2);
});
test('正常刷新成功与立即重复点击仍只发送一次POST',async()=>{
 let release;let posts=0;const page=await ui((path,options)=>options.method==='POST'?(posts++,new Promise(resolve=>release=()=>resolve(response({})))):response(snapshot()));prepare(page);
 const pending=page.node('#refresh').onclick();await tick();await page.node('#refresh').onclick();assert.equal(posts,1);release();await pending;assert.equal(page.node('#refresh').disabled,false);assert.equal(page.run('quotaRefreshAttempt'),null);
});
test('官方刷新明确失败也先GET重读并保留可恢复错误，不自动重发POST',async()=>{
 const calls=[];const page=await ui((path,options)=>{calls.push([path,options.method]);return options.method==='POST'?response({error:{message:'合成刷新失败'}},503):response(snapshot());});prepare(page);await page.node('#refresh').onclick();assert.match(page.node('#page-read-feedback').innerHTML,/合成刷新失败/);assert.equal(calls.filter(([,m])=>m==='POST').length,1);assert.ok(calls.some(([p])=>p.startsWith('/api/admin/state')));assert.equal(page.run('manualRefreshPending'),false);
});
test('旧登录迟到结果与finally不能清除新登录刷新状态',async()=>{
 const releases=[];const page=await ui((path,options)=>options.method==='POST'?new Promise(resolve=>releases.push(()=>resolve(response({})))):response(snapshot()));prepare(page);
 const old=page.node('#refresh').onclick();await tick();page.run('authEpoch++;showLogin();');prepare(page);const newer=page.node('#refresh').onclick();await tick();assert.equal(releases.length,2);releases[0]();await old;assert.equal(page.run('manualRefreshPending'),true);assert.equal(page.node('#refresh').disabled,true);releases[1]();await newer;assert.equal(page.run('manualRefreshPending'),false);
});
test('旧登录等待到期不向新会话写提示或触发旧GET',async()=>{
 const calls=[];const page=await ui((path,options)=>{calls.push([path,options.method]);return options.method==='POST'?new Promise(()=>{}):response(snapshot());});prepare(page);const old=page.node('#refresh').onclick();await tick();const timer=page.timers.find(t=>t.active&&t.delay===30000);page.run('authEpoch++;showLogin();');prepare(page);timer.fn();await old;assert.equal(page.run('manualRefreshPending'),false);assert.ok(!calls.some(([path])=>path.startsWith('/api/admin/state')));assert.doesNotMatch(page.node('#page-read-feedback').innerHTML,/结果尚未确认/);
});
test('其他POST仍无通用超时，GET保留原30秒超时契约',async()=>{
 const page=await ui((_path,options)=>new Promise((_resolve,reject)=>options.signal?.addEventListener('abort',()=>reject(Object.assign(Error('aborted'),{name:'AbortError'})))));page.run('admin=true;state={};');const before=page.timers.length;page.run("void api('/api/admin/example','POST',{})");await tick();assert.equal(page.timers.length,before);const pending=page.run("api('/api/admin/example')");const check=assert.rejects(pending,/读取超时/);expire(page);await check;assert.equal(page.run('admin'),true);
});
