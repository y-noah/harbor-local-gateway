import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const snapshot=()=>({totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],daily:[],requests:[],busy:0,queue:0});
const response=(body,status=200)=>({ok:status<400,status,json:async()=>body});
async function ui(fetcher){
  const nodes=new Map(),handlers=new Map(),downloads=[];
  const node=id=>{if(!nodes.has(id))nodes.set(id,{classList:{values:new Set(),add(v){this.values.add(v);},remove(v){this.values.delete(v);},toggle(){}},value:'',textContent:'',innerHTML:'',open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
  const context=vm.createContext({document:{querySelector:node,querySelectorAll:()=>[],addEventListener(type,handler){handlers.set(type,handler);},createElement(){return {click(){downloads.push(this.download);}};},activeElement:{tagName:'BODY'}},URL:{createObjectURL:()=> 'blob:test-only',revokeObjectURL(){}},sessionStorage:{removeItem(){}},location:{hash:''},setInterval(){},setTimeout(){},clearTimeout(){},fetch:async(path,options)=>path==='/api/auth/session'?response({authenticated:false}):fetcher(path,options),console});
  vm.runInContext(source,context);await new Promise(setImmediate);
  return {node,downloads,click:action=>handlers.get('click')({target:{closest:selector=>selector==='[data-action]'?{dataset:{action}}:null}}),run:code=>vm.runInContext(code,context)};
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
