import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const response=body=>({ok:true,status:200,json:async()=>body});
async function ui(fetcher=()=>response({rows:[],next:null})){
 const nodes=new Map(),storage=new Map(),calls=[],events=new Map();let reloads=0;
 const node=id=>{if(!nodes.has(id))nodes.set(id,{dataset:{},value:'',innerHTML:'',textContent:'',classList:{add(){},remove(){},toggle(){}},open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
 const context=vm.createContext({window:{},AbortController,URLSearchParams,URL,console,FormData:class{constructor(){}get(){return '合成名称';}},document:{querySelector:node,querySelectorAll:()=>[],addEventListener:(name,fn)=>events.set(name,fn),activeElement:{tagName:'BODY'}},navigator:{clipboard:{writeText:async()=>{}}},sessionStorage:{getItem:key=>storage.get(key),setItem:(key,value)=>storage.set(key,value),removeItem:key=>storage.delete(key)},location:{hash:'',reload(){reloads++;}},setInterval(){},setTimeout(){},clearTimeout(){},fetch:async(path,options)=>{if(path==='/api/auth/session')return response({authenticated:false});calls.push({path,options});return fetcher(path,options);}});
 vm.runInContext(source,context);await new Promise(setImmediate);return {run:code=>vm.runInContext(code,context),node,calls,storage,reloads:()=>reloads};
}
const root="admin=true;applyManagementIdentity({role:'super_admin',view:'global',department_id:null});";
test('legacy uses no view header, department admin ignores stored selector and has no global controls',async()=>{
 const page=await ui();await page.run("api('/api/admin/state')");assert.equal(page.calls[0].options.headers['X-Harbor-Department'],undefined);
 page.storage.set('harbor.department-view',JSON.stringify({id:'other-department',name:'其他部门'}));page.run("admin=true;applyManagementIdentity({role:'department_admin',department_id:'department-a',department_name:'合成部门A'});showShell()");await page.run("api('/api/admin/state')");assert.equal(page.calls.at(-1).options.headers['X-Harbor-Department'],'department-a');assert.match(page.node('#department-context').innerHTML,/合成部门A/);assert.doesNotMatch(page.node('#department-context').innerHTML,/department-picker/);assert.doesNotMatch(page.run('managementSettings()'),/pricing-settings|data-action="backup"/);
});
test('view switch aborts old reads, keeps original write header, clears old data and rejects follow-up calls',async()=>{
 let release;const page=await ui((path,options)=>path.endsWith('/wait')?new Promise(resolve=>release=()=>resolve(response({ok:true}))):response({ok:true}));page.run(root);page.run("departmentView='department-a';departmentViewName='A';state={private:'old'}");const pending=page.run("api('/api/admin/wait').catch(error=>error.message)");await new Promise(setImmediate);await page.run("api('/api/admin/members','POST',{name:'synthetic'})");page.run("switchDepartment('department-b','合成部门B')");assert.equal(page.calls[0].options.signal.aborted,true);assert.equal(page.calls[1].options.headers['X-Harbor-Department'],'department-a');assert.equal(page.run('departmentView'),'department-a');assert.equal(page.run('state'),null);assert.equal(page.reloads(),1);assert.deepEqual(JSON.parse(page.storage.get('harbor.department-view')),{id:'department-b'});await assert.rejects(page.run("api('/api/admin/members','POST',{})"),/正在切换部门/);release();assert.match(await pending,/登录状态已变更/);
});
test('form created in old scope cannot submit after view transition',async()=>{
 const page=await ui();page.run(root+"globalThis.submitted=0;formModal('新建','', '',async()=>{submitted++})");const submit=page.node('#dialog-form').onsubmit;page.run("switchDepartment('department-b','B')");await submit({preventDefault(){},target:{querySelector(){return {};}}});assert.equal(page.run('submitted'),0);
});
test('department and management key lists follow cursor pagination and revoke the selected version',async()=>{
 const page=await ui((path,options)=>{if(options.method==='POST')return response({});if(path.includes('management-keys'))return response({rows:[{id:'key-one',label:'合成Key',prefix:'hdm_sample',enabled:true,version:3,created:2}],next:{created:2,id:'key-one'}});return response({rows:[{id:path.includes('after_id')?'department-b':'department-a',name:'合成部门',enabled:true,version:2}],next:path.includes('after_id')?null:{created:1,id:'department-a'}});});page.run(root);await page.run('openDepartments()');await page.run("departmentAction('department-more','')");assert.match(page.calls.at(-1).path,/after_id=department-a/);assert.equal(page.run('departmentDialog.rows.length'),2);await page.run("openDepartmentKeys('department-b')");await page.run("departmentAction('department-keys-more','')");assert.match(page.calls.at(-1).path,/department-b\/management-keys.*after_id=key-one/);await page.run("departmentAction('department-key-revoke','key-one')");const submit=page.node('#dialog-form').onsubmit;await submit({preventDefault(){},target:{querySelector(){return {};}}});const write=page.calls.find(call=>call.options.method==='POST');assert.equal(write.path,'/api/admin/departments/department-b/management-keys/key-one/revoke');assert.deepEqual(JSON.parse(write.options.body),{expected_version:3});
});

test('restored super view ignores cached names and reads the current name from its exact server department',async()=>{
 const page=await ui(path=>response({id:'department-a',name:'服务端新名称'}));page.storage.set('harbor.department-view',JSON.stringify({id:'department-a',name:'错误的缓存部门名称'}));page.run(root);assert.equal(page.run('departmentViewName'),'');await page.run('verifyManagementView();');assert.equal(page.calls.at(-1).path,'/api/admin/departments/department-a');assert.equal(page.calls.at(-1).options.headers['X-Harbor-Department'],'department-a');assert.equal(page.run('departmentViewName'),'服务端新名称');
});
test('GET remains cancellable after headers arrive and before its JSON body completes',async()=>{
 let finish;const page=await ui(()=>({ok:true,status:200,json:()=>new Promise(resolve=>finish=resolve)}));page.run(root+"globalThis.reader=new AbortController()");const pending=page.run("api('/api/admin/wait','GET',undefined,'',reader.signal)");await new Promise(setImmediate);assert.equal(page.run('adminReads.size'),1);page.run('reader.abort()');assert.equal(page.calls[0].options.signal.aborted,true);finish({ok:true});await pending;assert.equal(page.run('adminReads.size'),0);
});

test('overview labels accurately distinguish a department from the global company view',async()=>{
 const page=await ui();page.run(root+"state={totals:{requests:0,success:0},accounts:[],members:[],daily:[],busy:0,queue:0,capabilities:{typeStatistics:true}};departmentView='department-a';departmentViewName='A';render()");assert.match(page.node('#content').innerHTML,/可用账号（当前部门）/);assert.match(page.node('#content').innerHTML,/员工（当前部门）/);assert.match(page.node('#content').innerHTML,/当前部门全部类型/);assert.doesNotMatch(page.node('#content').innerHTML,/全公司/);page.run("departmentView='global';render()");assert.match(page.node('#content').innerHTML,/全公司全部类型/);
});
