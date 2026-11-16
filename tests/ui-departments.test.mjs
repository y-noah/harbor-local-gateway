import test from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';
const source=readFileSync(new URL('../public/app.js',import.meta.url),'utf8');
const response=body=>({ok:true,status:200,json:async()=>body});
async function ui(fetcher=()=>response({rows:[],next:null}),{session=null}={}){
 const nodes=new Map(),storage=new Map(),calls=[],events=new Map();let reloads=0;
 const node=id=>{if(!nodes.has(id))nodes.set(id,{dataset:{},value:'',innerHTML:'',textContent:'',classList:{add(){},remove(){},toggle(){}},open:false,close(){this.open=false;},showModal(){this.open=true;}});return nodes.get(id);};
 const context=vm.createContext({window:{},AbortController,URLSearchParams,URL,console,FormData:class{constructor(){}get(){return '合成名称';}},document:{querySelector:node,querySelectorAll:()=>[],addEventListener:(name,fn)=>events.set(name,fn),activeElement:{tagName:'BODY'}},navigator:{clipboard:{writeText:async()=>{}}},sessionStorage:{getItem:key=>storage.get(key),setItem:(key,value)=>storage.set(key,value),removeItem:key=>storage.delete(key)},location:{hash:'',reload(){reloads++;}},setInterval(){},setTimeout(){},clearTimeout(){},fetch:async(path,options)=>{if(path==='/api/auth/session')return typeof session==='function'?session():response(session||{authenticated:false});calls.push({path,options});return fetcher(path,options);}});
 vm.runInContext(source,context);await new Promise(setImmediate);return {run:code=>vm.runInContext(code,context),node,calls,storage,reloads:()=>reloads,click:(action,id='')=>events.get('click')({target:{closest:selector=>selector==='[data-action]'?{dataset:{action,id}}:null}})};
}
const root="admin=true;applyManagementIdentity({role:'super_admin',view:'global',department_id:null});";
test('legacy uses no view header, department admin ignores stored selector and has no global controls',async()=>{
 const page=await ui();await page.run("api('/api/admin/state')");assert.equal(page.calls[0].options.headers['X-Harbor-Department'],undefined);
 page.storage.set('harbor.department-view',JSON.stringify({id:'other-department',name:'其他部门'}));page.run("admin=true;applyManagementIdentity({role:'department_admin',department_id:'department-a',department_name:'合成部门A'});showShell()");await page.run("api('/api/admin/state')");assert.equal(page.calls.at(-1).options.headers['X-Harbor-Department'],'department-a');assert.match(page.node('#department-context').innerHTML,/合成部门A/);assert.doesNotMatch(page.node('#department-context').innerHTML,/department-picker/);assert.doesNotMatch(page.run('managementSettings()'),/pricing-settings|data-action="backup"/);
});
test('超管业务固定服务端所属团队，旧选择与切换动作不能改变读取或写入范围',async()=>{
 const page=await ui(()=>response({ok:true}));page.storage.set('harbor.department-view',JSON.stringify({id:'department-b'}));page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',department_name:'团队A'});state={private:'same-team'}");
 await page.run("api('/api/admin/state')");page.run("switchDepartment('department-b','团队B')");await page.run("api('/api/admin/members','POST',{})");
 assert.ok(page.calls.every(call=>call.options.headers['X-Harbor-Department']==='department-a'));assert.equal(page.run('departmentView'),'department-a');assert.equal(page.run('state.private'),'same-team');assert.equal(page.reloads(),0);assert.equal(page.storage.has('harbor.department-view'),false);
});
test('旧身份创建的表单在身份纪元改变后不能提交',async()=>{
 const page=await ui();page.run(root+"globalThis.submitted=0;formModal('新建','', '',async()=>{submitted++})");const submit=page.node('#dialog-form').onsubmit;page.run('authEpoch++');await submit({preventDefault(){},target:{querySelector(){return {};}}});assert.equal(page.run('submitted'),0);
});
test('department and management key lists follow cursor pagination and revoke the selected version',async()=>{
 const page=await ui((path,options)=>{if(options.method==='POST')return response({});if(path.includes('management-keys'))return response({rows:[{id:'key-one',label:'合成Key',prefix:'hdm_sample',enabled:true,version:3,created:2}],next:{created:2,id:'key-one'}});return response({rows:[{id:path.includes('after_id')?'department-b':'department-a',name:'合成部门',enabled:true,version:2}],next:path.includes('after_id')?null:{created:1,id:'department-a'}});});page.run(root);await page.run('openDepartments()');await page.run("departmentAction('department-more','')");assert.match(page.calls.at(-1).path,/after_id=department-a/);assert.equal(page.run('departmentDialog.rows.length'),2);await page.run("openDepartmentKeys('department-b')");await page.run("departmentAction('department-keys-more','')");assert.match(page.calls.at(-1).path,/department-b\/management-keys.*after_id=key-one/);await page.run("departmentAction('department-key-revoke','key-one')");const submit=page.node('#dialog-form').onsubmit;await submit({preventDefault(){},target:{querySelector(){return {};}}});const write=page.calls.find(call=>call.options.method==='POST');assert.equal(write.path,'/api/admin/departments/department-b/management-keys/key-one/revoke');assert.deepEqual(JSON.parse(write.options.body),{expected_version:3});
});

test('超管不恢复缓存团队与名称，只采用服务端身份且无需额外部门查询',async()=>{
 const page=await ui();page.storage.set('harbor.department-view',JSON.stringify({id:'department-b',name:'缓存伪造名称'}));page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',department_name:'服务端团队A'})");await page.run('verifyManagementView()');assert.equal(page.run('departmentView'),'department-a');assert.equal(page.run('departmentViewName'),'服务端团队A');assert.equal(page.calls.length,0);assert.equal(page.storage.has('harbor.department-view'),false);
});
test('GET remains cancellable after headers arrive and before its JSON body completes',async()=>{
 let finish;const page=await ui(()=>({ok:true,status:200,json:()=>new Promise(resolve=>finish=resolve)}));page.run(root+"globalThis.reader=new AbortController()");const pending=page.run("api('/api/admin/wait','GET',undefined,'',reader.signal)");await new Promise(setImmediate);assert.equal(page.run('adminReads.size'),1);page.run('reader.abort()');assert.equal(page.calls[0].options.signal.aborted,true);finish({ok:true});await pending;assert.equal(page.run('adminReads.size'),0);
});

test('overview labels accurately distinguish a department from the global company view',async()=>{
 const page=await ui();page.run(root+"state={totals:{requests:0,success:0},accounts:[],members:[],daily:[],busy:0,queue:0,capabilities:{typeStatistics:true}};departmentView='department-a';departmentViewName='A';render()");assert.match(page.node('#content').innerHTML,/可用账号（当前部门）/);assert.match(page.node('#content').innerHTML,/员工（当前部门）/);assert.match(page.node('#content').innerHTML,/当前部门全部类型/);assert.doesNotMatch(page.node('#content').innerHTML,/全公司/);page.run("departmentView='global';render()");assert.match(page.node('#content').innerHTML,/全公司全部类型/);
});
test('侧栏只保留范围，部门管理分类仅超管可见且整块内容位于系统管理',async()=>{
 const page=await ui();page.run(root+"showShell()");assert.match(page.node('#department-context').innerHTML,/范围：全局视图/);assert.doesNotMatch(page.node('#department-context').innerHTML,/超级管理员|department-picker|业务操作/);
 assert.match(page.run('connectionTabs()'),/部门管理/);assert.match(page.run('departmentManagementContext()'),/超级管理员|department-picker/);
 page.run("applyManagementIdentity({role:'department_admin',department_id:'department-a',department_name:'合成团队A'})");assert.doesNotMatch(page.run('connectionTabs()'),/部门管理/);assert.equal(page.run('departmentManagementContext()'),'');
});
test('全局新建和员工批量配置在输入前选择团队，不提交预览或业务写请求',async()=>{
 for(const action of ['create-member','members-configure-selected','create-account','routing-weights','device-policy','package-policy','extra-global']){
  const page=await ui(()=>response({rows:[{id:'department-a',name:'合成A',enabled:true}],next:null}));page.run(root);
  await page.click(action,'synthetic');assert.match(page.node('#modal-content').innerHTML,/选择业务所属部门/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/<form|department-key-create|department-create/);assert.ok(page.calls.every(call=>call.options.method==='GET'));assert.equal(page.calls[0].path,'/api/admin/departments?limit=20');
 }
});
test('全局使用类型管理先选团队，不读取或呈现跨团队同码目录',async()=>{
 const page=await ui(()=>response({rows:[{id:'department-a',name:'合成A',enabled:true},{id:'department-b',name:'合成B',enabled:true}],next:null}));page.run(root);
 await page.click('member-types');assert.match(page.node('#modal-content').innerHTML,/选择业务所属部门/);assert.equal(page.calls.some(call=>call.path==='/api/admin/member-types'),false);
 await page.run("openUsageTypes('edit','type-a')");assert.equal(page.calls.some(call=>call.path==='/api/admin/member-types'),false);assert.doesNotMatch(page.node('#modal-content').innerHTML,/type-editor|保存默认/);
});
test('全局月费按单账号服务端归属定位，不猜默认部门，不读取月费或提交global写入',async()=>{
 const page=await ui(path=>response(path==='/api/admin/accounts/account-a'?{id:'account-a',department_id:'department-b'}:{id:'department-b',name:'服务端团队B',enabled:true}));page.run(root);
 await page.run("openSubscriptionPrice('account-a')");assert.deepEqual(page.calls.map(c=>c.path),['/api/admin/accounts/account-a','/api/admin/departments/department-b']);assert.match(page.node('#modal-content').innerHTML,/服务端团队B/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/subscription-price-form|department-default/);
 await page.run("departmentAction('department-select','department-b')");assert.equal(page.storage.has('harbor.department-view'),false);assert.equal(page.reloads(),0);assert.equal(page.run('departmentView'),'global');
});
test('全局归属缺失或迟到不打开业务表单，未选部门写在fetch前拒绝，全局系统操作保留',async()=>{
 const page=await ui(()=>response({id:'account-a'}));page.run(root);await page.run("openSubscriptionPrice('account-a')");assert.equal(page.calls.length,1);assert.match(page.node('#modal-content').innerHTML,/归属信息不完整/);
 await assert.rejects(page.run("api('/api/admin/member-config-batches/preview','POST',{})"),/先选择业务所属部门/);assert.equal(page.calls.length,1);
 await page.run("api('/api/admin/pricing','PATCH',{})");assert.equal(page.calls.at(-1).options.headers['X-Harbor-Department'],'global');
 page.run("departmentView='department-b'");await page.run("api('/api/admin/accounts/account-a/subscription','PATCH',{monthly_usd:200,expected_version:0})");assert.equal(page.calls.at(-1).options.headers['X-Harbor-Department'],'department-b');
});
test('员工分页与兼容列表在所有范围均不展示所属团队列或团队名称',async()=>{
 const page=await ui();page.run(root+"state={capabilities:{},keys:[]};globalThis.rows=[{id:'member-a',name:'合成员工',department_id:'department-a',department_name:'服务端团队A',enabled:1,key_count:0}];membersData={rows,types:[],pagination:{page:1,pageSize:20,total:1,totalPages:1}}");
 for(const scope of ['global','department-a']){
  page.run('departmentView='+JSON.stringify(scope));
  for(const markup of [page.run('memberTable(rows)'),page.run('pagedMembersSection()')]){assert.doesNotMatch(markup,/所属团队|服务端团队A|归属待核对/);assert.match(markup,/合成员工/);assert.equal((markup.match(/<th>/g)||[]).length,(markup.match(/<td(?:[ >])/g)||[]).length);}
 }
 assert.equal(page.calls.length,0);
});
test('全局同码类型筛选显示服务端团队，不按code合并不同类型，团队视图标签保持简洁',async()=>{
 const page=await ui();page.run(root+"typeCatalog=[{id:'type-a',code:'A',name:'研发',department_name:'真实团队甲'},{id:'type-b',code:'A',name:'研发',department_name:'真实团队乙'}]");
 const options=page.run("typeOptions('type-b')");assert.match(options,/value="type-a"[^>]*>A · 研发 · 真实团队甲/);assert.match(options,/value="type-b" selected>A · 研发 · 真实团队乙/);
 page.run("departmentView='department-a'");assert.doesNotMatch(page.run('typeOptions()'),/真实团队/);
});
test('剩余额度区分确定金额、计价缺失、首次加载与旧值更新，不把未知变为满额',async()=>{
 const page=await ui();page.run("globalThis.limitRow={usage_limits:{enabled:true},limit_usage:[{model:'gpt-6.1-sol',hours:24,limit_usd:'100',remaining_usd:'73.21',unresolved:0}]}");
 assert.match(page.run('memberLimitSummary(limitRow,{})'),/剩余 \$73\.21/);
 assert.match(page.run('memberLimitSummary(limitRow,{usagePending:true})'),/\$73\.21.*更新中/);
 page.run('limitRow.limit_usage[0].unresolved=2;limitRow.limit_usage[0].remaining_usd="100"');const unknown=page.run('memberLimitSummary(limitRow,{})');assert.match(unknown,/2条请求未完成计价，剩余额度暂不可算/);assert.doesNotMatch(unknown,/剩余 \$100/);
 page.run('limitRow.limit_usage=[]');assert.match(page.run('memberLimitSummary(limitRow,{usagePending:true})'),/剩余额度加载中/);assert.match(page.run('memberLimitSummary(limitRow,{usageError:true})'),/剩余额度获取失败/);
});
test('团队分配设置主动读取该团队权重，缺失时可重试而非要求无效的全局刷新',async()=>{
 const page=await ui(()=>response({error:{message:'合成权重读取失败'}},503));page.run(root+"departmentView='department-a';departmentViewName='团队A';state={accounts:[],capabilities:{}};");
 await page.run('openSubscriptionWeights()');assert.equal(page.calls.at(-1).path,'/api/admin/state?view=accounts&people=none');assert.equal(page.calls.at(-1).options.headers['X-Harbor-Department'],'department-a');assert.match(page.node('#modal-content').innerHTML,/重新读取当前团队权重/);assert.doesNotMatch(page.node('#modal-content').innerHTML,/请先刷新账号状态|账号池全局/);
});
test('全局账号按服务端所属团队权重展示，不跨团队归一化且兼容策略不伪造百分比',async()=>{
 const page=await ui();page.run(root+"state={capabilities:{accountPackages:true},accounts:[]};globalThis.account={id:'a',kind:'codex',routing_weight:{effective:75,department_id:'department-a',department_name:'团队甲',strategy:{source:'weighted'}}}");
 assert.equal(page.run('accountWeightSummary(account)'),'75.00%');
 page.run("account.routing_weight.strategy={source:'legacy'}");assert.match(page.run('accountWeightSummary(account)'),/不使用百分比/);
 page.run("account.routing_weight.strategy={source:'legacy_compatibility',effective:false}");assert.match(page.run('accountWeightSummary(account)'),/不使用百分比/);
 page.run("account.auth_mode='platform'");assert.equal(page.run('accountWeightSummary(account)'),'独立通道');
});
test('有效管理Key按原部门和Key回读，仅显示在当前弹窗且历史Key明确无法恢复',async()=>{
 let stored=true;const page=await ui(path=>response(path.endsWith('/reveal')?stored?{available:true,secret:'synthetic-secret'}:{available:false,reason:'not_stored'}:{rows:[{id:'key-a',enabled:true}],next:null}));page.run(root);
 await page.run("openDepartmentKeys('department-a')");assert.match(page.node('#modal-content').innerHTML,/department-key-reveal/);await page.run("departmentAction('department-key-reveal','key-a')");assert.equal(page.node('#department-secret').textContent,'synthetic-secret');assert.equal(page.calls.at(-1).path,'/api/admin/departments/department-a/management-keys/key-a/reveal');
 stored=false;await page.run("openDepartmentKeys('department-a')");await page.run("departmentAction('department-key-reveal','key-a')");assert.match(page.node('#modal-content').innerHTML,/历史 Key 未保存可恢复的原文/);
});

test('超管团队管理独立使用global范围，业务读写始终留在服务端本部门',async()=>{
 const page=await ui();page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',department_name:'团队A'})");
 await page.run("api('/api/admin/departments?limit=20')");await page.run("api('/api/admin/departments/department-b/management-keys/key-b/reveal','POST',{})");await page.run("api('/api/admin/state')");await page.run("api('/api/admin/members','POST',{})");assert.deepEqual(page.calls.map(call=>call.options.headers['X-Harbor-Department']),['global','global','department-a','department-a']);
});
test('默认团队停用仍保持超管会话与团队恢复入口，不循环读取业务数据',async()=>{
 const page=await ui();page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',department_name:'团队A',business_available:false});state=null;render()");
 assert.match(page.node('#content').innerHTML,/当前团队已停用/);assert.match(page.node('#content').innerHTML,/department-picker/);await page.run('refresh()');assert.equal(page.calls.length,0);assert.equal(page.run('admin'),true);
 await page.click('department-picker');assert.equal(page.calls.at(-1).options.headers['X-Harbor-Department'],'global');assert.match(page.node('#modal-content').innerHTML,/部门管理/);assert.equal(page.run('admin'),true);
});

test('停用团队导航与直接业务读取均在fetch前拒绝，团队恢复接口仍可用',async()=>{
 const page=await ui();page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',business_available:false});state=null");
 for(const next of ['overview','members','accounts','usage','system','history']){page.run('setTab('+JSON.stringify(next)+')');assert.match(page.node('#content').innerHTML,/当前团队已停用/);}
 for(const path of ['/api/admin/state','/api/admin/history','/api/admin/members'])await assert.rejects(page.run('api('+JSON.stringify(path)+')'),/当前团队已停用/);
 assert.equal(page.calls.length,0);await page.run("api('/api/admin/departments')");assert.equal(page.calls.length,1);assert.equal(page.calls[0].options.headers['X-Harbor-Department'],'global');assert.equal(page.run('admin'),true);
});
test('启用本团队后先重新读取真实业务state，关闭团队管理可恢复主页面',async()=>{
 const identity={role:'super_admin',department_id:'department-a',department_name:'团队A',business_available:true};
 const snapshot={totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],daily:[],busy:0,queue:0,capabilities:{}};
 const page=await ui((path,options)=>response(path.startsWith('/api/admin/state')?snapshot:path.startsWith('/api/admin/departments')&&options.method==='GET'?{rows:[{id:'department-a',name:'团队A',enabled:false,version:2}],next:null}:{}),{session:{authenticated:true,identity}});
 page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',department_name:'团队A',business_available:false});state=null");const initialCalls=page.calls.length;await page.run('openDepartments()');await page.run("departmentAction('department-edit','department-a')");page.run("globalThis.FormData=class{get(key){return key==='enabled'?'true':'团队A'}}");await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {};}}});
 assert.equal(page.run('state.totals.requests'),0);assert.equal(page.run('managementIdentity.business_available'),true);const reads=page.calls.slice(initialCalls).filter(call=>call.path.startsWith('/api/admin/state'));assert.equal(reads.length,1);assert.equal(reads[0].options.headers['X-Harbor-Department'],'department-a');page.run('close();render()');assert.doesNotMatch(page.node('#content').innerHTML,/当前团队已停用|工作空间读取失败/);assert.match(page.node('#content').innerHTML,/可用账号/);
});

const recoverySnapshot=tab=>({totals:{tokens:0,requests:0,success:0,reserved:0},accounts:[],members:[],keys:[],daily:[],busy:0,queue:0,capabilities:{pagedMembers:tab==='members',sessionStateView:true},view:tab==='sessions'?'sessions':'overview',sessions:[],sessionsPagination:{page:1,pageSize:20,total:0,totalPages:1},endpoint:'http://synthetic.invalid'});
for(const delayed of ['PATCH','session'])for(const aftermath of ['close','new-dialog','new-login'])for(const retainedTab of ['overview','sessions','members'])test(`本团队恢复生命周期：延迟${delayed}／${aftermath}／保留${retainedTab}`,async()=>{
 let release,started,activeSession=false;const entered=new Promise(resolve=>started=resolve),identity={role:'super_admin',department_id:'department-a',department_name:'团队A',business_available:true},snapshot=recoverySnapshot(retainedTab);
 const members={rows:[],types:[],pagination:{page:1,pageSize:20,total:0,totalPages:1}};
 const pause=body=>new Promise(resolve=>{release=()=>resolve(response(body));started();});
 const page=await ui((path,options)=>path.startsWith('/api/admin/state')?response(snapshot):path.startsWith('/api/admin/members')?response(members):options.method==='PATCH'&&delayed==='PATCH'?pause({}):response(options.method==='GET'?{rows:[{id:'department-a',name:'团队A',enabled:false,version:2}],next:null}:{}),{session:()=>!activeSession?response({authenticated:false}):delayed==='session'?pause({authenticated:true,identity}):response({authenticated:true,identity})});
 activeSession=true;page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',department_name:'团队A',business_available:false});state=null;tab="+JSON.stringify(retainedTab));await page.run('openDepartments()');await page.run("departmentAction('department-edit','department-a')");page.run("globalThis.FormData=class{get(key){return key==='enabled'?'true':'团队A'}}");const before=page.calls.length,pending=page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {};}}});await entered;
 page.run('close()');if(aftermath==='new-dialog')page.run('modal("后来弹窗")');if(aftermath==='new-login')page.run("authEpoch++;applyManagementIdentity({role:'super_admin',department_id:'department-b',business_available:true});state={marker:'new-login'}");release();await pending;await new Promise(setImmediate);
 if(aftermath==='new-login'){assert.equal(page.run('managementIdentity.department_id'),'department-b');assert.equal(page.run('state.marker'),'new-login');assert.equal(page.calls.slice(before).filter(call=>call.path.startsWith('/api/admin/state')).length,0);}
 else{assert.equal(page.run('managementIdentity.business_available'),true);assert.equal(page.run('state.view'),snapshot.view);assert.equal(page.run('pageDataReady()'),true);if(retainedTab==='members')assert.ok(page.calls.slice(before).some(call=>call.path.startsWith('/api/admin/members')));if(aftermath==='new-dialog')assert.equal(page.node('#modal-content').innerHTML,'后来弹窗');else assert.equal(page.node('#modal').open,false);}
});
test('会话页恢复首请求显式sessions视图并保留非空筛选，控件与筛选结果一致',async()=>{
 let enabled=false;const identity={role:'super_admin',department_id:'department-a',department_name:'团队A',business_available:true};const paths=[];
 const filtered={...recoverySnapshot('sessions'),capabilities:{sessionStateView:true,sessionFilters:true},sessions:[{id:'matched-session',protocol:'responses',client_session_id:'matched-filter-client',member_name:'筛选员工',account_name:'筛选账号',enabled:1,state:'active',updated:Date.now()}]};
 const page=await ui((path,options)=>{if(path.startsWith('/api/admin/state')){paths.push(path);const params=new URL(path,'http://test').searchParams;return response(params.get('view')==='sessions'&&params.get('session_q')==='精确查询'&&params.get('session_status')==='enabled'?filtered:{...recoverySnapshot('overview'),sessions:[{id:'unfiltered-session',client_session_id:'unfiltered-client'}]});}if(path.startsWith('/api/admin/member-types'))return response({types:[]});if(options.method==='PATCH'){enabled=true;return response({});}return response({rows:[{id:'department-a',name:'团队A',enabled:false,version:2}],next:null});},{session:()=>response(enabled?{authenticated:true,identity}:{authenticated:false})});
 page.run("admin=true;applyManagementIdentity({role:'super_admin',department_id:'department-a',business_available:false});state=null;tab='sessions';sessionsFilters={q:'精确查询',type:'',account:'',status:'enabled'}");await page.run('openDepartments()');await page.run("departmentAction('department-edit','department-a')");page.run("globalThis.FormData=class{get(key){return key==='enabled'?'true':'团队A'}}");await page.node('#dialog-form').onsubmit({preventDefault(){},target:{querySelector(){return {};}}});page.run('close();render()');
 assert.equal(paths.length,1);const params=new URL(paths[0],'http://test').searchParams;assert.equal(params.get('view'),'sessions');assert.equal(params.get('session_q'),'精确查询');assert.equal(params.get('session_status'),'enabled');assert.equal(page.run('sessionsConfirmedFilters.q'),'精确查询');assert.equal(page.run('sessionsConfirmedFilters.status'),'enabled');assert.match(page.node('#content').innerHTML,/value="精确查询"/);assert.match(page.node('#content').innerHTML,/matched-filter-client/);assert.doesNotMatch(page.node('#content').innerHTML,/unfiltered-client/);assert.equal(page.run('pageDataReady()'),true);
});
