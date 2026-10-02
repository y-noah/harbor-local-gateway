import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const snapshot=()=>({totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],daily:[],requests:[],busy:0,queue:0});
const response=(body,status=200)=>({ok:status<400,status,json:async()=>body});
async function ui(fetcher){
  const nodes=new Map();
  const node=id=>{if(!nodes.has(id))nodes.set(id,{classList:{values:new Set(),add(v){this.values.add(v);},remove(v){this.values.delete(v);},toggle(){}},value:'',textContent:'',innerHTML:'',open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
  const context=vm.createContext({document:{querySelector:node,querySelectorAll:()=>[],addEventListener(){},activeElement:{tagName:'BODY'}},sessionStorage:{removeItem(){}},location:{hash:''},setInterval(){},setTimeout(){},clearTimeout(){},fetch:async(path,options)=>path==='/api/auth/session'?response({authenticated:false}):fetcher(path,options),console});
  vm.runInContext(source,context);await new Promise(setImmediate);
  return {node,run:code=>vm.runInContext(code,context)};
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
