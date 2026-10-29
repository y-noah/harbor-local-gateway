const $=s=>document.querySelector(s), esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=n=>Number(n||0).toLocaleString('zh-CN'),date=t=>t?new Date(t).toLocaleString('zh-CN',{hour12:false}):'—';
const referenceMoney=n=>n===null||n===undefined?'—':Number(n)>0&&Number(n)<0.000001?'<$0.000001':'$'+Number(n).toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:6});
const referenceTotal=u=>referenceMoney(u?.total);
const referenceCoverage=u=>!u?'计价数据待更新':[u.unmetered?fmt(u.unmetered)+'条未计量':'',u.unpriced?fmt(u.unpriced)+'条缺费率':'',u.expired?fmt(u.expired)+'条历史不可恢复':''].filter(Boolean).join(' · ')||'已知用量按Standard标准价计算';
const referenceState=s=>({priced:'已计价',manual:'人工核对',legacy_manual:'旧版人工补记缺少分类，待美元核对',unpriced:'模型费率待确认',unmetered:'原始用量缺失',pending:'执行中，尚未结算',expired:'历史已过保留期'})[s]||'部分用量待确认';
const beijingDate=t=>t?new Date(t).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'—';
let admin=false,authEpoch=0,state=null,tab='overview',testKey='',chat=[],chatSession='',chatModel='codex',draft='',chatOwner='',sending=false,requestFilter='';
let requestsData=null,requestsLoading=false,requestsVersion=0,connectionView='test',autoRefreshPending=false;
let requestsFilters={member:'',model:'',status:'',page:1,pageSize:25};
let sessionsPage=1,sessionsPageSize=25,sessionsLoading=false,refreshVersion=0;
let recordView='usage',historyData=null,historyLoading=false,historyVersion=0,dialogVersion=0,requestDetailId='',requestDetailVersion=0;
let historyFilters={kind:'audit',page:1,action:'',account:'',session:'',start:'',end:''};
const phaseLabels={queued:'等待执行',credentials:'准备凭据',upstream:'连接上游 / 等待响应',streaming:'接收上游数据',cli:'官方 CLI 执行中',settling:'统计收尾',ok:'完成',failed:'失败',unknown:'结束 · 用量待核对',interrupted:'重启中断',cancelled:'已取消',rejected:'已拒绝'};
const actionLabels={'account.api.check':'检查官方Key','account.api.weight':'调整API人工权重','account.api.key':'更新官方Key','session.route':'会话换绑','session.nightly':'会话自动调整设置','session.nightly.expired':'会话临时固定到期','session.nightly.deadline':'历史固定期限补全','session.enable':'恢复会话访问','session.disable':'禁止会话访问','balancing.settings':'负载均衡设置','balancing.weights':'手动调整权重','balancing.weights.auto':'系统更新权重','balancing.weights.expired':'手动权重到期','account.route_block':'账号路由受限','account.route_recover':'账号路由恢复','account.create':'添加账号','account.remove':'移除账号','account.update':'修改账号','account.login':'开始账号登录','account.login.success':'账号登录成功','account.login.failed':'账号登录失败','account.login.cancel':'取消账号登录','key.create':'创建 Key','key.remove':'删除 Key','key.update':'修改 Key','member.create':'创建员工','member.update':'修改员工','member.remove':'移除员工','backup.create':'创建数据库备份','request.settle':'核对请求用量','pricing.configure':'调整模型费率'};
const reasonLabels={manual_binding_only:'仅人工绑定，不分配新会话',account_disabled:'账号停用',manual:'手动操作',initialize:'首次初始化',nightly:'夜间调整',quota:'额度耗尽或恢复',recovery:'账号恢复',weight_zero:'原账号权重为0',quota_exhausted:'确认订阅额度耗尽',login_expired:'确认登录失效',disabled:'账号已停用',not_ready:'账号尚未就绪',zero_weight:'生效权重为0',eligible:'可参与新会话分配',quota_unavailable:'额度快照缺失、过期或已越过重置点',incomparable_quota:'额度窗口或套餐不可比较',unknown_fixed_load:'固定会话缺少明确模型费率或历史样本，无法比较负载',unknown_workload:'缺少明确模型费率或可用的历史用量样本',busy:'会话有请求执行中',protected:'受静默期、临时固定或其他保护条件限制',no_measured_work:'没有可计量的历史负载',session_disabled:'会话不可迁移',session_busy:'会话仍有请求执行中',nightly_plan_changed:'执行前条件发生变化，本组未迁移',planning_failed:'计划计算失败',cannot_fit:'会话负载无法放入其他账号的容差范围',infeasible:'不存在满足约束的完整迁移方案',search_limited:'计算预算不足，未确认可行方案'};
const labels={overview:'运行总览',members:'员工与密钥',accounts:'上游账号池',sessions:'会话绑定',requests:'使用记录',playground:'接入与测试'};
sessionStorage.removeItem('harbor-admin');if(location.hash)history.replaceState(null,'',location.pathname);
let toastTimer;function toast(message){$('#toast').textContent=message;$('#toast').classList.remove('hidden');clearTimeout(toastTimer);toastTimer=setTimeout(()=>$('#toast').classList.add('hidden'),5000);}
async function api(path,method='GET',data,credential=''){
  const epoch=authEpoch;
  const r=await fetch(path,{method,headers:{...(credential?{Authorization:'Bearer '+credential}:{}),...(data!==undefined?{'Content-Type':'application/json'}:{})},...(data!==undefined?{body:JSON.stringify(data)}:{})});
  if(r.status===401&&path.startsWith('/api/admin/')&&epoch===authEpoch){admin=false;authEpoch++;state=null;showLogin();}
  let b;try{b=await r.json();}catch{throw Error(r.ok?'服务返回了空或无效响应，请刷新页面后重试':'请求失败（HTTP '+r.status+'），请稍后重试');}
  if(!r.ok)throw Error(b?.error?.message||'请求失败（HTTP '+r.status+'）');
  if(!b||typeof b!=='object')throw Error('服务返回了无效响应，请刷新页面后重试');
  if(path.startsWith('/api/admin/')&&epoch!==authEpoch)throw Error('登录状态已变更，请重新操作');
  return b;
}
function badge(status){const map={ok:['完成',''],running:['处理中','warn'],queued:['排队中','warn'],failed:['失败','bad'],unknown:['待核对','warn'],interrupted:['待核对','warn'],cancelled:['已取消','off'],rejected:['已拒绝','off'],reconciled:['已核对',''],ready:['已连接',''],offline:['未登录','off'],degraded:['需检查登录','warn'],login:['登录中','warn']};const [text,css]=map[status]||[status,'off'];return `<span class="badge ${css}">${esc(text)}</span>`;}
function heading(eyebrow,title,sub,action=''){return `<div class="page-heading"><div><div class="eyebrow">${eyebrow}</div><h1>${title}</h1><p class="subtitle">${sub}</p></div>${action}</div>`;}
function button(action,label,css='secondary',id='',disabled=false){return `<button type="button" class="${css}" data-action="${action}" data-id="${esc(id)}"${disabled?' disabled':''}>${label}</button>`;}
function empty(text){return `<div class="empty"><span class="empty-symbol">◌</span>${text}</div>`;}
function panel(title,inside,action=''){return `<section class="panel"><div class="panel-heading"><h2>${title}</h2>${action}</div>${inside}</section>`;}
function fixedSessionCommand(identifier){return `codex -c 'model_providers.harbor.http_headers.X-Harbor-Session="${identifier}"'`;}
function apiAccount(a){return a?.auth_mode==='openai_api';}
function quotaCard(a){
  if(apiAccount(a))return `<div class="quota"><span class="hint">官方API余额</span><div class="quota-number">—</div><small>普通Key未提供公开余额查询；请在官方账单后台核对。平台用量不等于赠额抵扣。</small><small>Key ${a.api_key_configured?'已配置 · 指纹 '+esc(a.api_key_fingerprint):'未配置'}</small></div>`;
  let q=null;try{q=JSON.parse(a.quota_json);}catch{}
  if(q?.windows?.length){
    const stale=q.checked<=Date.now()-300000||q.windows.some(w=>w.reset&&w.reset<=Date.now());
    return `<div class="quota"><span class="hint">${stale?'上次官方额度 · 待刷新':'官方窗口最低剩余比例'}</span><div class="quota-number">${q.remaining.toFixed(0)}<small>%</small></div><div class="quota-track"><span data-width="${q.remaining}"></span></div>${q.windows.map(w=>`<div class="quota-row">${w.minutes?fmt(w.minutes/60)+' 小时窗口':'额度窗口'}<strong>剩余 ${(100-w.used).toFixed(0)}%</strong></div><small>重置：${date(w.reset)}</small>`).join('')}<small>更新：${date(q.checked)} · ${esc(q.plan||'订阅')}</small>${stale?'<small>当前余量待确认，后台自动刷新</small>':''}</div>`;
  }
  return `<div class="quota"><span class="hint">${a.kind==='claude'?'暂无官方额度查询':'正在读取官方额度'}</span><div class="quota-number">—</div><small>官方余量仅供参考，不阻止 Codex 转发</small></div>`;
}
function weightPercent(value){
  if(value===null||value===undefined)return '待确认';
  if(value===0||value===100)return value+'%';
  const rounded=Number(value.toFixed(2));return rounded===0?'低于0.01%':rounded===100?'高于99.99%':rounded+'%';
}
function planningHint(p){
  if(!p)return '';
  const reason=({within_tolerance:p.strategy==='heavy-bias-first-v2'?'已在容差内且无需纠正偏差方向':'已在容差内，按当时规则无需迁移',balanced:'已找到达标计划',direction_unavailable:'已在容差内，但没有可改善偏差方向的完整方案',infeasible:'受固定负载或会话大小限制，无法达标；仅保留可改善偏差的迁移',search_limited:'计算预算不足以确认重会话方案，本组不迁移',priority_unresolved:'无法确认完整重会话方案，本组不迁移'})[p.reason]||'计划结果待确认';
  return `<p class="hint">整组偏差 ${esc(Number(p.beforeDeviation).toFixed(2))} → ${esc(Number(p.afterDeviation).toFixed(2))} 个百分点（容差 ${esc(p.tolerance)}）。${esc(reason)}。优先迁移重会话 ${fmt(p.prioritySessions?.length)} 个${p.heavySkipped?.length?'，无法纳入或待确认 '+fmt(p.heavySkipped.length)+' 个':''}。${p.reason==='balanced'&&!p.optimizationComplete?'已满足重会话优先，计算预算已用完，'+(p.directionComplete?'偏差方向已达到保护条件允许的最优，尚未证明迁移数量最少。':'尚未证明偏差方向最优或迁移数量最少。'):''}</p>`;
}
function routingCard(a){
  if(apiAccount(a))return `<div class="quota"><div class="quota-row">人工新会话占比<strong>${esc(weightPercent(a.api_weight||0))}</strong></div><small>开关、权重长期由人工控制。权重0仅不分配新会话；人工固定绑定仍使用此Key，停用会迁出全部绑定。</small></div>`;
  if(a.kind!=='codex'||!state.balancing?.weights)return '';
  const w=state.balancing.weights,row=w.accounts.find(x=>x.id===a.id);
  if(!row)return '';
  const mode=({api_mixed:'API人工占比＋订阅组原规则',auto:w.reason?'自动 · 等待额度确认，沿用已知权重':'自动',manual:'临时手动',fallback:'额度待确认 · 按原顺序',legacy:'新权重未开启'})[w.mode];
  return `<div class="quota"><div class="quota-row">系统权重<strong>${esc(weightPercent(row.automatic))}</strong></div><div class="quota-row">生效权重<strong>${esc(weightPercent(row.effective))}</strong></div><small>${esc(mode)}${w.configuration.mode==='manual'?' · 手动到期：'+beijingDate(w.configuration.expires):''}</small></div>`;
}
function automaticRefreshScope(){
  if(!admin||document.hidden||$('#modal').open)return '';
  const focus=document.activeElement;if(['INPUT','SELECT','TEXTAREA'].includes(focus?.tagName)||focus?.isContentEditable)return '';
  if(['overview','accounts','sessions'].includes(tab))return 'state:'+tab;
  if(tab==='requests'&&recordView==='requests'&&requestsFilters.page===1&&!requestsFilters.member&&!requestsFilters.model&&!requestsFilters.status)return 'requests:latest';
  return '';
}
function stateSnapshot(value){const {recentUsageWindow,...data}=value||{};return JSON.stringify(data);}
function requestsSnapshot(value){const {observedAt,...data}=value||{};return JSON.stringify(data);}
async function automaticRefresh(){
  const scope=automaticRefreshScope();if(!scope||autoRefreshPending||sessionsLoading||requestsLoading)return;
  autoRefreshPending=true;try{if(scope==='requests:latest')await loadRequests(scope);else await refresh(true,scope);}finally{autoRefreshPending=false;}
}
async function refresh(renderPage=true,automaticScope=''){
  if(!admin)return;const epoch=authEpoch,version=++refreshVersion;
  const query=sessionsPage===1&&sessionsPageSize===25?'':`?session_page=${sessionsPage}&session_page_size=${sessionsPageSize}`;
  try{
    const next=await api('/api/admin/state'+query);
    if(!admin||epoch!==authEpoch||version!==refreshVersion||automaticScope&&automaticRefreshScope()!==automaticScope)return;
    if(tab==='sessions'&&$('#modal').open)return;
    const changed=stateSnapshot(next)!==stateSnapshot(state);state=next;sessionsPage=next.sessionsPagination?.page??sessionsPage;sessionsPageSize=next.sessionsPagination?.pageSize??sessionsPageSize;sessionsLoading=false;
    $('#connection').textContent='● 服务在线';if(renderPage&&(!automaticScope||changed))render();
    if(tab==='requests'&&recordView==='requests'&&!requestsLoading&&!$('#modal').open)await loadRequests();
  }catch(e){
    if(!admin||epoch!==authEpoch||version!==refreshVersion||automaticScope&&automaticRefreshScope()!==automaticScope)return;
    sessionsLoading=false;sessionsPage=state?.sessionsPagination?.page??1;sessionsPageSize=state?.sessionsPagination?.pageSize??25;$('#connection').textContent='连接异常';
    if(automaticScope)return;if(!state)showLogin();else if(renderPage)render();toast(e.message);
  }
}
function showLogin(){requestsVersion++;requestsData=null;requestsLoading=false;requestsFilters={member:'',model:'',status:'',page:1,pageSize:25};connectionView='test';historyVersion++;historyData=null;historyLoading=false;recordView='usage';historyFilters={kind:'audit',page:1,action:'',account:'',session:'',start:'',end:''};if($('#modal').open)close();sessionsPage=1;sessionsPageSize=25;sessionsLoading=false;testKey='';chat=[];chatSession='';chatOwner='';draft='';$('#modal-content').innerHTML='';$('#content').innerHTML='';$('#login').classList.remove('hidden');$('#shell').classList.add('hidden');}
function showShell(){$('#login').classList.add('hidden');$('#shell').classList.remove('hidden');}
function setTab(next){if(sending)return toast('请等待当前请求完成后再切换页面');if(next==='guide'){next='playground';if(admin)connectionView='guide';}if(!admin&&next!=='playground'){toast('请先进入管理后台');return;}if(!admin)connectionView='test';requestsVersion++;requestsLoading=false;tab=next;render();if(tab==='requests'&&recordView==='requests')void loadRequests();}
function memberTable(rows){return rows.length?`<div class="table-wrap"><table><thead><tr><th>员工 / Key及有效期</th><th>近7天来源 IP</th><th>最近使用</th><th>状态</th><th>管理</th></tr></thead><tbody>${rows.map(m=>`<tr><td class="member-identity"><div class="member-cell"><span class="avatar">${esc(m.name.slice(0,1))}</span><strong>${esc(m.name)}</strong></div><div class="member-keys">${(state.keys||[]).filter(k=>k.member_id===m.id).map(k=>`<div class="member-key"><div class="mono">${keyDisplay(k)}</div><small>${k.expires?'到期：'+beijingDate(k.expires)+'（北京时间）':'永久有效'} · ${!k.enabled?'已停用':!m.enabled?'员工已停用':k.expires&&k.expires<=Date.now()?'已过期':'可用'}</small></div>`).join('')||'<small>暂无 Key</small>'}</div></td><td>${button('ips',fmt(m.ips)+' 个 IP','table-button',m.id)}</td><td>${date(m.last_seen)}</td><td>${m.enabled?'<span class="badge">启用</span>':'<span class="badge off">已停用</span>'}</td><td>${button('member-detail','管理','table-button',m.id)}</td></tr>`).join('')}</tbody></table></div>`:empty('还没有员工。创建第一个 Key，开始使用。');}
function requestTable(rows){return rows.length?`<div class="table-wrap"><table><thead><tr><th>时间 / 员工</th><th>账号 / 模型</th><th>Standard参考消耗（USD）</th><th>来源 IP</th><th>总耗时 / 当前阶段</th><th>状态 / 详情</th></tr></thead><tbody>${rows.map(r=>`<tr><td>${date(r.started)}<small>${esc(r.member_name)}</small></td><td>${esc(r.account_name||'—')}<small>${esc(r.model)}</small></td><td>${referenceTotal(r.standard)}<small>输入 ${referenceMoney(r.standard?.input)} / 输出 ${referenceMoney(r.standard?.output)}</small><small>${r.standard?.adjustment?'人工核对 '+referenceMoney(r.standard.adjustment):'其中缓存 '+referenceMoney(r.standard?.cached)} · ${esc(referenceState(r.standard?.state))}</small></td><td class="mono">${esc(r.ip)}</td><td>${((Math.max(0,(r.ended||Date.now())-r.started))/1000).toFixed(1)}s<small>${esc(phaseLabels[r.phase]||'旧记录未记录阶段')}${!r.ended&&r.phase_at?' · '+(Math.max(0,Date.now()-r.phase_at)/1000).toFixed(1)+'s':''}</small></td><td>${button('request-detail',badge(r.status),'table-button',r.id)}</td></tr>`).join('')}</tbody></table></div>`:empty('暂无请求记录。测试一次请求后，数据会显示在这里。');}
function usageRows(){return (state.keys||[]).map(k=>({...k,member_name:state.members.find(m=>m.id===k.member_id)?.name||'已移除员工',member_enabled:state.members.find(m=>m.id===k.member_id)?.enabled}));}
function keyDisplay(k){return k.full_key?`<span class="key-value">${esc(k.full_key)}</span>${button('copy-key','复制','table-button',k.id)}`:`<span>${esc(k.prefix)}…</span><small>旧 Key 未存全文，下次成功调用后补全</small>`;}
async function copyKey(value){
  if(navigator.clipboard?.writeText){await navigator.clipboard.writeText(value);return;}
  const input=document.createElement('textarea');input.value=value;input.setAttribute('readonly','');input.className='clipboard-fallback';document.body.append(input);input.select();
  try{if(!document.execCommand('copy'))throw Error('复制失败，请手动选择完整 Key 复制');}finally{input.remove();}
}
function recentUsageTitle(){return 'Standard参考消耗 <button type="button" class="usage-help table-button" data-action="usage-help" aria-label="最近用量说明" title="查看统计口径与漏计说明">?</button>';}
function pricingOverview(r){
  return `<h2>模型费率与统计口径</h2><p>统计统一为Standard参考消耗USD，按普通输入、缓存读取和输出分别计算，不是实际扣款或订阅扣额。下表为每100万Token的标准美元价（计价单位）；统计页面只展示参考USD。</p><p class="hint">计价基准为官方Standard短上下文价；速度、长上下文、缓存写入附加费和工具/存储费不计入此参考值。</p><p>自动同步：${r.settings.enabled?'已开启':'已关闭'} · 每${fmt(r.settings.refreshHours)}小时检查<br>最近成功：${date(r.state.lastSuccess)} · 初始官方快照：${esc(r.seedCaptured)}<br>版本：<span class="mono">${esc(r.state.active.slice(0,12))}</span></p>${r.state.error?`<div class="notice warn">${esc(r.state.error)}</div>`:''}<div class="actions">${button('pricing-refresh','检查官方费率','secondary')}${button('pricing-config','自动同步设置','secondary')}${button('pricing-edit','添加手动费率','secondary')}${button('pricing-alias','添加模型映射','secondary')}</div>${r.unpricedModels.length?`<h3>待计价模型</h3><div class="notice">${r.unpricedModels.map(x=>`${esc(x.model)}：${fmt(x.requests)}条`).join('<br>')}<br>缺价不按0统计；未知别名不自动猜模型。</div>`:''}<div class="table-wrap"><table><thead><tr><th>模型</th><th>普通输入</th><th>缓存读取</th><th>输出</th><th>来源 / 操作</th></tr></thead><tbody>${r.rates.map(x=>`<tr><td>${esc(x.model)}</td><td>${esc(x.input)}</td><td>${x.cached===null?'不支持':esc(x.cached)}</td><td>${esc(x.output)}</td><td>${x.manual?'手动覆盖':'官方快照'} ${button('pricing-edit','编辑','table-button',x.model)}${x.manual?button('pricing-clear','恢复官方','table-button',x.model):''}</td></tr>`).join('')}</tbody></table></div>${r.settings.aliases.length?`<h3>明确模型映射</h3>${r.settings.aliases.map((a,i)=>`<div class="mini-row"><span>${esc(a.name)} → ${esc(a.target)}<small>${date(a.from)} 至 ${a.to?date(a.to):'长期'} · ${esc(a.source)}</small></span>${button('pricing-alias-remove','移除','table-button',JSON.stringify([a.name,a.target,a.from,a.to]))}</div>`).join('')}`:''}<p class="hint">自动更新不覆盖手动费率；清除手动覆盖后采用上次有效官方值。已计价记录保留版本，未计价记录可在费率补齐后回算。映射须有依据，历史范围不可随意推断。</p><div class="dialog-actions">${button('close','关闭','primary')}</div>`;
}
async function pricingAction(action,identifier){
  if(!admin)return;
  if(action==='pricing-settings')modal('<h2>费率与口径</h2><p>正在读取…</p>',true);
  const version=dialogVersion,epoch=authEpoch,r=await api('/api/admin/pricing');
  const current=()=>admin&&epoch===authEpoch&&version===dialogVersion&&$('#modal').open;
  if(!current())return;
  if(action==='pricing-settings'){$('#modal-content').innerHTML=pricingOverview(r);return;}
  if(action==='pricing-refresh'){const next=await api('/api/admin/pricing/refresh','POST',{});if(current()){await refresh();if(current())$('#modal-content').innerHTML=pricingOverview(next);}return;}
  const save=async payload=>{const v=dialogVersion,e=authEpoch;await api('/api/admin/pricing','PATCH',{expectedVersion:r.settings.version,...payload});if(admin&&e===authEpoch&&v===dialogVersion&&$('#modal').open){close();await refresh();toast('费率配置已保存，已计价记录保持原版本');}};
  if(action==='pricing-config')return formModal('自动费率同步','后台直接读取官方定价页，失败沿用上次有效版本；关闭只停止更新。',`<label>自动同步<select name="enabled"><option value="true" ${r.settings.enabled?'selected':''}>开启</option><option value="false" ${!r.settings.enabled?'selected':''}>关闭</option></select></label><label>间隔（小时）<input name="hours" type="number" min="1" max="168" value="${r.settings.refreshHours}" required></label>`,f=>save({enabled:f.get('enabled')==='true',refreshHours:Number(f.get('hours'))}));
  if(action==='pricing-edit'){
    const rate=r.rates.find(x=>x.model===identifier);
    return formModal('手动模型费率','每100万原始计量的Standard参考美元价。手动数据会长期覆盖自动值，只有人工清除才恢复官方。',`<label>明确模型ID<input name="model" value="${esc(identifier||'')}" maxlength="200" required></label><label>普通输入<input name="input" type="number" min="0" step="0.000001" value="${esc(rate?.input||'')}" required></label><label>缓存读取<input name="cached" type="number" min="0" step="0.000001" value="${esc(rate?.cached??'')}" placeholder="留空表示不支持，不能理解为免费"></label><label>输出<input name="output" type="number" min="0" step="0.000001" value="${esc(rate?.output||'')}" required></label><label>来源说明<input name="source" maxlength="500" value="${esc(rate?.source||'')}" required></label>`,f=>save({manualRates:{[f.get('model')]:{input:f.get('input'),cached:f.get('cached')||null,output:f.get('output'),source:f.get('source')}}}));
  }
  if(action==='pricing-clear')return modal(`<h2>恢复官方费率？</h2><p>${esc(identifier)} 的手动覆盖将被清除，已有计价记录保持原版本。</p><div class="dialog-actions">${button('close','取消','secondary')}${button('pricing-clear-confirm','确认恢复','primary',identifier)}</div>`);
  if(action==='pricing-clear-confirm')return save({manualRates:{[identifier]:null}});
  if(action==='pricing-alias')return formModal('明确模型映射','只依据可信资料确认映射，不因名称相似猜模型。开始时间留空仅影响保存后的请求；历史回算须明确时间及依据。',`<label>入口别名<input name="name" maxlength="200" required></label><label>明确模型ID<select name="target">${r.rates.map(x=>`<option value="${esc(x.model)}">${esc(x.model)}</option>`).join('')}</select></label><label>开始时间（浏览器本地时区，可留空）<input name="from" type="datetime-local"></label><label>结束时间（可留空）<input name="to" type="datetime-local"></label><label>映射依据<input name="source" maxlength="500" required></label>`,f=>save({aliases:[...r.settings.aliases,{name:f.get('name'),target:f.get('target'),...(f.get('from')?{from:new Date(f.get('from')).getTime()}:{}),to:f.get('to')?new Date(f.get('to')).getTime():null,source:f.get('source')}]}));
  if(action==='pricing-alias-remove'){
    const index=r.settings.aliases.findIndex(a=>JSON.stringify([a.name,a.target,a.from,a.to])===identifier);if(index<0)return toast('映射已变化，请重新打开费率页面');
    return save({aliases:r.settings.aliases.filter((_,i)=>i!==index)});
  }
}
function usageTable(rows){return rows.length?`<div class="table-wrap"><table><thead><tr><th>员工</th><th>Key</th><th>近7天参考消耗（USD）</th><th>近12小时参考消耗（USD）</th><th>状态</th></tr></thead><tbody>${rows.map(k=>`<tr><td>${esc(k.member_name)}</td><td class="mono key-cell">${keyDisplay(k)}</td><td>${referenceTotal(k.standard)}<small>${esc(referenceCoverage(k.standard))}</small></td><td>${referenceTotal(k.standard_12h)}<small>${esc(referenceCoverage(k.standard_12h))}</small></td><td>${!k.enabled||!k.member_enabled?'已停用':k.expires&&k.expires<=Date.now()?'已过期':'可用'}</td></tr>`).join('')}</tbody></table></div>`:empty('暂无 Key 用量。');}
function recordTabs(){return `<div class="actions record-tabs" aria-label="使用记录分类">${[['usage','Key 用量'],['requests','请求明细'],['history','操作历史']].map(([id,label])=>button('record-view',label,recordView===id?'primary':'secondary',id)).join('')}</div>`;}
async function loadRequests(automaticScope=''){
  if(!admin||tab!=='requests'||recordView!=='requests')return;
  const epoch=authEpoch,version=++requestsVersion,filters={...requestsFilters};if(!automaticScope){requestsLoading=true;render();}
  const query=new URLSearchParams({page:String(filters.page),page_size:String(filters.pageSize)});
  for(const field of ['member','model','status'])if(filters[field])query.set(field,filters[field]);
  try{
    const data=await api('/api/admin/requests?'+query);
    if(!admin||epoch!==authEpoch||version!==requestsVersion||tab!=='requests'||recordView!=='requests')return;
    if(automaticScope&&automaticRefreshScope()!==automaticScope)return;
    $('#connection').textContent='● 服务在线';
    const changed=requestsSnapshot(data)!==requestsSnapshot(requestsData);requestsData=data;requestsFilters.page=data.pagination.page;requestsLoading=false;if(!automaticScope||changed||data.summary?.active>0)render();
  }catch(e){if(admin&&epoch===authEpoch&&version===requestsVersion){if(automaticScope){if(automaticRefreshScope()===automaticScope)$('#connection').textContent='连接异常';return;}requestsLoading=false;requestsData=null;render();toast(e.message);}}
}
function requestsSection(){
  const data=requestsData,f=requestsFilters;
  const select=(field,label,rows)=>`<label>${label}<select id="requests-${field}" aria-label="${label}"><option value="">全部</option>${rows.map(([value,text])=>`<option value="${esc(value)}" ${f[field]===value?'selected':''}>${esc(text)}</option>`).join('')}</select></label>`;
  const filters=`<div class="history-filters request-filters">${select('member','用户',(data?.members||[]).map(x=>[x.id,x.name+(x.removed_at?'（已移除）':'')]))}${select('model','模型',(data?.models||[]).map(x=>[x,x]))}${select('status','状态',['running','queued','ok','failed','unknown','interrupted','cancelled','rejected','reconciled'].map(x=>[x,phaseLabels[x]||({running:'执行中',reconciled:'已核对'})[x]||x]))}</div>`;
  if(requestsLoading)return panel('近7天请求明细',`<div class="panel-body">${filters}</div>${empty('正在读取筛选结果…')}`);
  if(!data)return panel('近7天请求明细',`<div class="panel-body">${filters}</div>${empty('暂无请求数据。')}`,button('requests-reload','重新读取','text-button'));
  const t=data.summary,p=data.pagination;
  const summary=`<div class="stats request-stats">${[['请求 / 成功',fmt(t.requests)+' / '+fmt(t.success),'处理中 '+fmt(t.active)],['Standard参考消耗（USD）',referenceTotal(t.standard),referenceCoverage(t.standard)],['未计量 / 缺费率',fmt(t.standard?.unmetered)+' / '+fmt(t.standard?.unpriced),'不将缺失用量或价格当作0'],['完成平均耗时',(t.average_ms/1000).toFixed(1)+'s','仅统计已结束请求']].map(([label,value,hint])=>`<div class="stat"><div class="stat-label">${label}</div><div class="stat-value">${value}</div><small>${hint}</small></div>`).join('')}</div>`;
  const grouped=(title,rows,name)=>panel(title,rows.length?`<div class="table-wrap"><table><thead><tr><th>${name==='model'?'模型':'用户'}</th><th>请求 / 成功</th><th>参考合计（USD）</th><th>输入 / 输出（USD）</th><th>未计量 / 缺费率</th></tr></thead><tbody>${rows.map(x=>`<tr><td>${esc(x[name]||'未记录')}</td><td>${fmt(x.requests)} / ${fmt(x.success)}</td><td>${referenceTotal(x.standard)}<small>人工核对 ${referenceMoney(x.standard?.adjustment)}</small></td><td>${referenceMoney(x.standard?.input)} / ${referenceMoney(x.standard?.output)}<small>其中缓存 ${referenceMoney(x.standard?.cached)}</small></td><td>${fmt(x.standard?.unmetered)} / ${fmt(x.standard?.unpriced)}</td></tr>`).join('')}</tbody></table></div>`:empty('当前筛选无数据。'));
  const pager=`<div class="session-pager"><span>第 ${p.page} / ${p.totalPages} 页 · 共 ${fmt(p.total)} 条</span><label>每页<select id="requests-page-size">${[25,50,100].map(n=>`<option value="${n}" ${p.pageSize===n?'selected':''}>${n}</option>`).join('')}</select>条</label><div class="actions">${button('requests-page','上一页','secondary',p.page-1,p.page<=1)}${button('requests-page','下一页','secondary',p.page+1,p.page>=p.totalPages)}</div></div>`;
  return panel('近7天请求明细',`<div class="panel-body">${filters}<p class="hint">北京时间今天及前6天，按完成时间归属；进行中按开始时间。下方统计覆盖全部筛选结果，分页仅影响明细。</p></div>`+summary+requestTable(data.rows)+pager,button('requests-reload','刷新请求','text-button'))+`<div class="two-col">${grouped('按模型统计',data.byModel,'model')}${grouped('按用户统计',data.byMember,'member_name')}</div>`;
}
function bindRequestFilters(){
  if(tab!=='requests'||recordView!=='requests')return;
  for(const field of ['member','model','status'])$(`#requests-${field}`).onchange=e=>{requestsFilters[field]=e.target.value;requestsFilters.page=1;void loadRequests();};
  if($('#requests-page-size'))$('#requests-page-size').onchange=e=>{requestsFilters.pageSize=Number(e.target.value);requestsFilters.page=1;void loadRequests();};
}
function connectionTabs(){return `<div class="actions record-tabs" aria-label="接入与测试分类">${[['guide','接入说明'],['test','请求测试']].map(([value,label])=>button('connection-view',label,connectionView===value?'primary':'secondary',value,sending)).join('')}</div>`;}
function historyName(id){return historyData?.accounts?.find(a=>a.id===id)?.name||state?.accounts?.find(a=>a.id===id)?.name||id||'—';}
function weightsText(weights){return weights?Object.entries(weights).map(([id,n])=>historyName(id)+' '+weightPercent(n)).join('；')||'自动模式':'此前值未记录';}
function historyText(r){
  const d=r.detail;
  if(r.action==='account.api.weight')return `${historyName(d.account)} · ${weightPercent(d.from)} → ${weightPercent(d.to)}（人工持久设置）`;
  if(r.action==='session.route')return `${historyName(d.from)} → ${historyName(d.to)} · ${reasonLabels[d.reason]||'历史原因未记录'}`;
  if(r.action==='balancing.weights.auto')return `${d.previous?weightsText(d.previous):'此前值未记录'} → ${weightsText(d.weights)} · ${reasonLabels[d.reason]||'历史触发原因未记录'}${d.retained?' · 数据待确认，沿用已知权重':''}`;
  if(r.action==='balancing.weights')return `${d.previous?.mode==='manual'?weightsText(d.previous.weights):d.previous?'自动模式':'此前值未记录'} → ${d.mode==='manual'?weightsText(d.weights):'自动模式'}${d.expires?' · 到期 '+beijingDate(d.expires):''}`;
  if(r.action==='account.route_block'||r.action==='account.route_recover')return `${historyName(d.account)} · ${reasonLabels[d.reason]||(r.action==='account.route_recover'?'确认额度或登录恢复':'历史原因未记录')}`;
  if(r.action==='balancing.settings')return `夜间调整：${d.nightly?'开启':'关闭'} · 加权分配：${d.weighted===null?'历史未记录':d.weighted?'开启':'关闭'} · 静默期 ${d.quietMinutes??'—'} 分钟`;
  if(r.action.startsWith('session.'))return `${d.enabled===true?'参与自动调整':d.enabled===false?'临时固定':'会话状态变更'}${d.expires?' · 到期 '+beijingDate(d.expires):''}`;
  if(d.request)return `${d.member||'已移除员工'} · ${historyName(d.account)} · 请求 ${d.request}`;
  if(d.account)return historyName(d.account);
  if(d.prefix)return `${d.member||'已移除员工'} · ${d.prefix}…`;
  if(d.member)return d.member;
  if(d.expires)return '到期 '+beijingDate(d.expires);
  return '已记录操作；旧记录可能没有结构化详情';
}
function historySection(){
  const f=historyFilters,data=historyData,options=data?.accounts||state.accounts,p=data?.pagination;
  const input=(name,label,type='text')=>`<label>${label}<input name="${name}" type="${type}" value="${esc(f[name])}" ${historyLoading?'disabled':''} ${type==='text'?'maxlength="100"':''}></label>`;
  const filters=`<form id="history-filter-form" class="history-filters"><label>记录类型<select name="kind" ${historyLoading?'disabled':''}><option value="audit" ${f.kind==='audit'?'selected':''}>操作事件</option><option value="nightly" ${f.kind==='nightly'?'selected':''}>夜间批次</option></select></label><label>事件<select name="action" ${historyLoading||f.kind==='nightly'?'disabled':''}><option value="">全部事件</option>${(data?.actions||Object.keys(actionLabels)).map(a=>`<option value="${esc(a)}" ${a===f.action?'selected':''}>${esc(actionLabels[a]||a)}</option>`).join('')}</select></label><label>涉及账号<select name="account" ${historyLoading?'disabled':''}><option value="">全部账号</option>${options.map(a=>`<option value="${esc(a.id)}" ${a.id===f.account?'selected':''}>${esc(a.name)}</option>`).join('')}</select></label>${input('session','会话 UUID / 内部标识')}${input('start','开始时间（北京时间）','datetime-local')}${input('end','结束时间（不含，北京时间）','datetime-local')}<button class="primary" ${historyLoading?'disabled':''}>查询</button>${button('history-reset','清空筛选','secondary')}</form>`;
  const table=data?.rows.length?`<div class="table-wrap"><table><thead><tr><th>时间 / 批次</th><th>操作 / 状态</th><th>说明</th><th>详情</th></tr></thead><tbody>${data.rows.map(r=>f.kind==='nightly'?`<tr><td>${esc(r.day)}<small>${date(r.started)}</small></td><td>${esc(({completed:'已完成',failed:'失败',interrupted:'重启中断',running:'执行中'})[r.status]||r.status)}</td><td>查看当时的计划、实际迁移和跳过原因</td><td>${button('nightly-history','查看批次','table-button',r.day)}</td></tr>`:`<tr><td>${date(r.at)}</td><td>${esc(actionLabels[r.action]||r.action)}</td><td class="history-description">${esc(historyText(r))}${r.clientSession||r.session?`<small class="mono">${esc(r.clientSession||r.session)}</small>`:''}</td><td>${button('history-detail','查看详情','table-button',r.id)}</td></tr>`).join('')}</tbody></table></div>`:empty(historyLoading?'正在查询历史…':'没有符合条件的记录');
  const pager=p?`<div class="session-pager"><span class="hint">共 ${fmt(p.total)} 条 · 第 ${p.page} / ${p.totalPages} 页${historyLoading?' · 正在读取…':''}</span><div class="actions">${button('history-page','首页','secondary','1',historyLoading||p.page<=1)}${button('history-page','上一页','secondary',p.page-1,historyLoading||p.page<=1)}${button('history-page','下一页','secondary',p.page+1,historyLoading||p.page>=p.totalPages)}${button('history-page','末页','secondary',p.totalPages,historyLoading||p.page>=p.totalPages)}</div></div>`:'';
  return panel('操作历史',`<div class="panel-body"><p class="hint">查询数据库中保留的全部历史，不限近7天。历史缺失字段不会补造；时间筛选按开始含、结束不含。</p>${filters}</div>${table}${pager}`,button('history-reload','刷新历史','text-button','',historyLoading));
}
async function loadHistory(){
  const version=++historyVersion,epoch=authEpoch;historyLoading=true;if(tab==='requests')render();
  const query=new URLSearchParams({kind:historyFilters.kind,page:String(historyFilters.page)});
  for(const name of ['action','account','session'])if(historyFilters[name]&&(name!=='action'||historyFilters.kind==='audit'))query.set(name,historyFilters[name]);
  for(const name of ['start','end'])if(historyFilters[name]){const time=Date.parse(historyFilters[name]+'+08:00');if(!Number.isFinite(time)){historyLoading=false;render();throw Error('请填写有效的北京时间');}query.set(name,String(time));}
  try{const result=await api('/api/admin/history?'+query);if(version!==historyVersion||epoch!==authEpoch||!admin||recordView!=='history')return;historyData=result;historyFilters.page=result.pagination.page;}
  finally{if(version===historyVersion&&epoch===authEpoch){historyLoading=false;if(admin&&tab==='requests'&&recordView==='history')render();}}
}
function bindHistoryForm(){
  const form=$('#history-filter-form');if(!form||recordView!=='history'||tab!=='requests')return;
  form.onchange=()=>{form.elements.action.disabled=form.elements.kind.value==='nightly';};
  form.onsubmit=async e=>{e.preventDefault();const values=new FormData(form);historyFilters={...historyFilters,page:1,...Object.fromEntries(['kind','action','account','session','start','end'].map(k=>[k,String(values.get(k)||'').trim()]))};historyData=null;try{await loadHistory();}catch(err){toast(err.message);}};
}
function phaseDetail(r){return `<h2>请求详情</h2><p class="mono muted">${esc(r.id)}</p><div class="details-line">员工：${esc(r.member_name)}<br>账号：${esc(r.account_name||'—')}<br>模型：${esc(r.model||'—')}<br>时间：${date(r.started)}<br>Key：${esc(r.key_prefix||'—')}…${r.key_removed_at?'（已删除）':''}<br>来源：${esc(r.ip)}<br>客户端：${esc(r.agent)}<br>Standard参考消耗（USD）：${referenceTotal(r.standard)}<br>输入 / 输出：${referenceMoney(r.standard?.input)} / ${referenceMoney(r.standard?.output)}<br>其中缓存：${referenceMoney(r.standard?.cached)}<br>人工核对：${referenceMoney(r.standard?.adjustment)}<br>计价状态：${esc(referenceState(r.standard?.state))}<br>模型依据：${esc(({upstream:'上游明确返回',requested:'按明确请求模型参考',configured_alias:'管理员明确映射',manual:'人工核对参考消耗'})[r.standard?.model_source]||'尚未确认')}<br>费率版本：${esc(r.standard?.revision?.slice(0,12)||'未计价')}${r.has_legacy_reservation?'<br>旧协议预占尚未换算，不并入参考消耗':''}</div><h3>请求阶段</h3>${r.timeline?.length?`<div class="table-wrap"><table><thead><tr><th>阶段</th><th>开始时间</th><th>耗时</th></tr></thead><tbody>${r.timeline.map(p=>`<tr><td>${esc(phaseLabels[p.phase]||p.phase)}</td><td>${date(p.at)}</td><td>${(p.duration/1000).toFixed(2)}s${!r.ended&&p===r.timeline.at(-1)?' · 进行中':''}</td></tr>`).join('')}</tbody></table></div>`:'<p class="hint">此记录没有完整阶段信息，旧请求不会补造历史。</p>'}<p class="hint">连接与等待响应为合并观察阶段；收到数据不代表模型已输出正文。CLI内部阶段不可见。不记录对话正文，不因阶段计时中断长任务。${!r.ended?'执行中每4秒自动刷新。':''}</p>${r.error?`<div class="notice warn">${esc(r.error)}</div>`:''}<div class="dialog-actions">${button('close','关闭','secondary')}${r.can_reconcile?button('settle','核对用量','primary',r.id):''}</div>`;}
async function requestDetail(identifier,initial=true){
  if(initial){modal('<h2>请求详情</h2><p>正在读取…</p>',true);requestDetailId=identifier;}
  const version=dialogVersion,poll=++requestDetailVersion,epoch=authEpoch,r=await api('/api/admin/requests/'+identifier);
  if(!admin||epoch!==authEpoch||version!==dialogVersion||poll!==requestDetailVersion||requestDetailId!==identifier||!$('#modal').open)return;
  if(r.id!==identifier||!Array.isArray(r.timeline))throw Error('请求详情响应无效，请刷新后重试');
  $('#modal-content').innerHTML=phaseDetail(r);if(r.ended)requestDetailId='';
}
function routingExplanation(r){
  const basis=r.basis,mode=({api_mixed:'API人工占比＋订阅组原规则',auto:'系统自动权重',manual:'临时手动权重',legacy:'原顺序分配',fallback:'额度待确认，按原顺序'})[r.mode]||r.mode;
  return `<h2>调度依据</h2><p>当前方式：${esc(mode)}。下一条新会话预计分配给：${esc(r.accounts.find(a=>a.id===r.nextAccount)?.name||'暂无可用账号')}。</p><p class="hint">这是当前快照的试算，不推进分配顺序；并发请求、配置和额度状态变化后结果可能不同。已有正权重会话保持绑定。</p>${r.reason?`<div class="notice warn">${esc(reasonLabels[r.reason]||'等待额度确认')}；系统沿用已有权重或原顺序，不按页面读取时间重算比例。</div>`:''}<div class="table-wrap"><table><thead><tr><th>账号</th><th>系统权重</th><th>生效权重</th><th>参与情况</th></tr></thead><tbody>${r.accounts.map(a=>`<tr><td>${esc(a.name)}</td><td>${a.manualApi?'人工设定，不自动重置':esc(weightPercent(a.automatic))}</td><td>${esc(weightPercent(a.effective))}</td><td>${esc(reasonLabels[a.reason]||a.reason)}${a.extraQuota?'<small>手动正权重允许使用额外额度</small>':''}</td></tr>`).join('')}</tbody></table></div><h3>系统权重计算依据</h3>${basis?`<p class="hint">快照时间：${beijingDate(basis.at)}（北京时间）；预算值＝剩余百分比 ÷ max（距重置小时数，24），再归一化。${r.mode==='manual'?'当前生效权重来自手动设置，不由下表决定。':''}</p><div class="table-wrap"><table><thead><tr><th>账号</th><th>当时剩余</th><th>当时距重置</th><th>除数</th><th>预算值</th><th>重置时间</th></tr></thead><tbody>${basis.accounts.map(a=>`<tr><td>${esc(r.accounts.find(x=>x.id===a.id)?.name||a.id)}</td><td>${a.remaining===null?'未确认':Number(a.remaining).toFixed(2)+'%'}</td><td>${a.hours===null?'—':Number(a.hours).toFixed(2)+'h'}</td><td>${a.denominator===null?'—':Number(a.denominator).toFixed(2)+'h'}</td><td>${a.score===null?'未计算':Number(a.score).toFixed(4)}</td><td>${beijingDate(a.reset)}</td></tr>`).join('')}</tbody></table></div>`:'<p class="hint">历史权重没有保存计算快照，下次系统正常重算后可查看；不会为展示依据强制重算。</p>'}${r.configuration.expires?`<p>临时手动到期：${beijingDate(r.configuration.expires)}（北京时间）。</p>`:''}<p class="hint">自动比例在每日02:00、确认耗尽或恢复等既有触发时更新。夜间仍按重会话优先、偏差方向、最少迁移的顺序计算，并保留在途保护。</p><div class="dialog-actions">${button('routing-history','查看调整历史','secondary')}${button('close','关闭','primary')}</div>`;
}
function heavyHistory(planning){
  if(!planning)return '';
  const session=m=>m.client_session_id||m.session_id||'旧记录未保留会话标识';
  return `${planning.prioritySessions?.length?`<p class="hint">已纳入优先迁移的重会话：${planning.prioritySessions.map(m=>esc(session(m))).join('、')}</p>`:''}${planning.heavySkipped?.length?`<div class="notice">${planning.heavySkipped.map(m=>`${esc(session(m))}：${esc(reasonLabels[m.reason]||'历史原因未记录')}`).join('<br>')}</div>`:''}`;
}
function nightlyHistory(r){
  const name=id=>r.accounts.find(a=>a.id===id)?.name||id||'—',session=m=>m.client_session_id||m.session_id||'未记录';
  return `<h2>夜间批次 · ${esc(r.day)}</h2>${r.metric?'<p class="hint">负载统一为Standard参考消耗USD。</p>':'<div class="notice">此批次为旧统计口径，缺少换算依据，隐藏不可比较的历史用量；迁移记录保留。</div>'}<p>开始：${date(r.started)} · 结束：${date(r.ended)} · ${esc(({completed:'已完成',failed:'失败',interrupted:'重启中断',running:'执行中'})[r.status]||r.status)}</p><p>计划迁移 ${r.moves.length} 个，确认已迁移 ${r.moves.filter(m=>m.applied===true).length} 个，跳过 ${r.skipped.length} 项。</p><p class="hint">以下是执行当时保存的计划，表格中的“计划后”不等于全部实际执行。每个会话的执行结果见迁移清单。统计窗口：${date(r.window?.start)} 至 ${date(r.window?.end)}。</p>${r.reason?`<div class="notice warn">${esc(reasonLabels[r.reason]||'批次未完成')}</div>`:''}${r.summaries.map(g=>`${planningHint(g.planning)}${heavyHistory(g.planning)}<div class="table-wrap"><table><thead><tr><th>账号</th><th>计划前负载</th><th>固定负载</th><th>目标负载</th><th>计划后负载</th></tr></thead><tbody>${g.accounts.map(id=>`<tr><td>${esc(name(id))}</td><td>${referenceMoney(g.before[id])}</td><td>${referenceMoney(g.fixedLoad[id])}</td><td>${referenceMoney(g.target[id])}</td><td>${referenceMoney(g.after[id])}</td></tr>`).join('')}</tbody></table></div>`).join('')}<h3>迁移清单</h3>${r.moves.length?`<div class="table-wrap"><table><thead><tr><th>会话 / 员工</th><th>原账号 → 目标账号</th><th>历史负载</th><th>执行结果</th></tr></thead><tbody>${r.moves.map(m=>`<tr><td class="mono">${esc(session(m))}<small>${esc(m.member)}</small></td><td>${esc(name(m.from))} → ${esc(name(m.to))}</td><td>${referenceMoney(m.weight)}</td><td>${m.applied===null?'旧记录未保存结果':m.applied?'已迁移':'未执行'}</td></tr>`).join('')}</tbody></table></div>`:empty('本批次没有计划迁移')}<h3>跳过原因</h3>${r.skipped.length?r.skipped.map(m=>`<div class="mini-row"><span class="mono">${esc(m.session_id?session(m):m.accounts?.length?m.accounts.map(name).join(' / '):m.account?name(m.account):'账号组（旧记录未记录成员）')}</span><span>${esc(reasonLabels[m.reason]||'历史原因未记录')}</span></div>`).join(''):empty('没有跳过项')}<p class="hint">新批次负载为当时Standard参考USD及在途估计，不代表实际扣款或官方订阅额度。</p><div class="dialog-actions">${button('close','关闭','primary')}</div>`;
}
function sessionPager(){
  const p=state.sessionsPagination||{page:sessionsPage,pageSize:sessionsPageSize,total:state.sessions.length,totalPages:1};
  const control=(label,page,disabled)=>`<button type="button" class="secondary" data-action="sessions-page" data-id="${page}" ${disabled||sessionsLoading?'disabled':''}>${label}</button>`;
  return `<div class="session-pager" aria-label="会话分页"><span class="hint">共 ${fmt(p.total)} 个会话 · 第 ${fmt(p.page)} / ${fmt(p.totalPages)} 页${sessionsLoading?' · 正在读取…':''}</span><div class="actions"><label>每页<select id="session-page-size" aria-label="每页会话数" ${sessionsLoading?'disabled':''}>${[25,50,100].map(n=>`<option value="${n}" ${n===p.pageSize?'selected':''}>${n} 条</option>`).join('')}</select></label>${control('首页',1,p.page<=1)}${control('上一页',p.page-1,p.page<=1)}${control('下一页',p.page+1,p.page>=p.totalPages)}${control('末页',p.totalPages,p.page>=p.totalPages)}</div></div>`;
}
function render(){
  if(!admin&&tab!=='playground'){showLogin();return;}
  showShell();document.querySelectorAll('[data-tab]').forEach(el=>el.classList.toggle('active',el.dataset.tab===tab));$('#crumb').textContent=labels[tab];
  const showGuide=tab==='playground'&&admin&&connectionView==='guide';
  if(!state&&(tab!=='playground'||showGuide)){$('#content').innerHTML=empty('正在读取工作空间…');return;}
  let html='';
  if(tab==='overview'){
    const total=state.totals,active=state.accounts.filter(a=>a.enabled&&a.state==='ready').length;
    const stats=[['近7天参考消耗（USD）',referenceTotal(total.standard),referenceCoverage(total.standard)],['近7天请求总数',fmt(total.requests),`成功 ${fmt(total.success)} 次`],['可用账号',`${active} <span class="muted">/ ${state.accounts.length}</span>`,'Codex 多会话并行转发'],['员工',fmt(state.members.length),`处理中 ${state.busy} · 排队 ${state.queue}`]];
    const days=Array.from({length:7},(_,i)=>{const d=new Date((state.usageWindow?.start??Date.now()-6*86400000)+i*86400000),day=d.toLocaleDateString('sv-SE',{timeZone:'Asia/Shanghai'});const drow=state.daily.find(x=>x.day===day);return {day,cost:drow?drow.standard?.total??null:0};});
    html=heading('WORKSPACE OVERVIEW','一切用量，清晰可见。','管理订阅连接与员工密钥；7天用量按北京时间自然日统计，12小时用量为滚动窗口。',button('create-member','＋ 创建员工 Key','primary'))+`<div class="stats">${stats.map(([label,value,hint])=>`<div class="stat"><div class="stat-label">${label}</div><div class="stat-value">${value}</div><small>${hint}</small></div>`).join('')}</div>`;
    html+=`<div class="two-col">${panel('最近 7 天用量',`<div class="panel-body"><div class="chart">${days.map(d=>`<div class="bar-col"><span class="chart-label">${referenceMoney(d.cost)}</span><div class="bar" data-height="${d.cost===null?0:d.cost/Math.max(0.000001,...days.map(x=>x.cost||0))*100}"></div><small>${d.day.slice(5).replace('-','/')}</small></div>`).join('')}</div></div>`,'<span class="hint">北京时间 · Standard参考USD</span>')}${panel('上游连接',`<div class="panel-body">${state.accounts.length?state.accounts.map(a=>`<div class="mini-row"><div><strong>${esc(a.name)}</strong><small class="muted"> · ${esc(apiAccount(a)?'OpenAI API Key':a.kind==='claude'?'Claude订阅':'Codex订阅')}</small></div>${a.enabled?badge(a.state):badge('offline')}</div>`).join(''):empty('尚未添加账号')}<p class="hint">平台近7天用量来自上游响应；订阅剩余量请以官方页面为准。</p></div>`,button('go-accounts','管理 →','text-button'))}</div>`;
    html+=panel(recentUsageTitle(),usageTable(usageRows()),button('go-requests','查看全部 →','text-button'));
  }
  if(tab==='members')html=heading('PEOPLE & ACCESS','员工与密钥','管理 Key 的有效期、启停和删除；近7天用量见使用记录。',button('create-member','＋ 创建员工 Key','primary'))+panel('员工列表',memberTable(state.members),`<span class="hint">${state.members.length} 位员工</span>`);
  if(tab==='accounts')html=heading('UPSTREAM POOL','上游账号池','Codex 新会话按生效权重分配，已有正权重绑定保持；不同会话并行。',`<div class="actions">${button('pricing-settings','费率与口径','secondary')}${button('routing-explanation','调度依据','secondary')}${button('routing-weights','临时设置权重','secondary')}${button('balancing-settings','负载均衡','secondary')}${button('refresh-quotas','↻ 刷新额度','secondary')}${button('create-account','＋ 添加订阅账号','secondary')}${button('create-api-account','＋ 添加API Key','primary')}</div>`)+`<div class="notice">Codex 直接并行转发，不按本地预测 Token 阻止请求。自动权重在每日02:00、额度耗尽或恢复时更新，也可临时手动设置；订阅账号0权重不接收后续请求。API Key由人工控制，权重0不分配新会话，人工固定会话仍可使用；停用会迁出绑定。Claude 仍为旧文本兼容入口，尚待真实验收。</div><div class="cards">${state.accounts.map(a=>`<article class="account-card"><div class="account-top"><span class="account-logo">${apiAccount(a)?'K':'C'}</span>${a.enabled?badge(a.state):'<span class="badge off">已停用</span>'}</div><h2>${esc(a.name)}</h2><small>${esc(apiAccount(a)?'OpenAI 官方API Key':a.kind==='claude'?'Claude 订阅 · 待真实验证':'Codex 订阅')}</small>${quotaCard(a)}${routingCard(a)}${a.route_block?`<div class="error-line">${apiAccount(a)?a.route_block==='quota_exhausted'?'官方API额度不足，请核对账单后手动检查恢复':'官方Key认证失效，请检查或更换Key':a.route_block==='quota_exhausted'?'订阅额度耗尽 · 自动权重为0，可临时设置正权重使用额外额度':'登录失效 · 暂停接收请求'}</div>`:''}<p>最近调用：${date(a.last_used)}<br>近7天参考USD：${referenceTotal(a.standard)}<small> · ${esc(referenceCoverage(a.standard))}</small></p>${a.last_error||a.quota_error?`<div class="error-line">${esc(a.last_error||a.quota_error)}</div>`:''}<div class="actions">${button('check-account',apiAccount(a)?'检查Key':'检查登录','secondary',a.id)}${apiAccount(a)?button('api-account-settings','Key与人工权重','text-button',a.id)+button('create-fixed-session','创建固定会话','text-button',a.id,!a.enabled||a.state!=='ready'||!!a.route_block):button('login-account',a.state==='offline'?'登录账号':'重新登录（重建会话）','text-button',a.id)}${button('toggle-account',a.enabled?'停用':'启用','text-button',a.id)}${button('remove-account','移除账号','text-button danger',a.id)}</div></article>`).join('')}<div class="account-card add">${button('create-account','＋ 添加一个独立账号','big-add')}</div></div>`;
  if(tab==='sessions')html=heading('SESSION AFFINITY','会话绑定','新会话自动分配账号，已有会话保持绑定；需要时可手动修改。')+`<div class="notice">手动切换从下一次请求生效；正在处理的会话须等待完成。禁止访问仅阻止该会话的新请求，不中断已经开始的请求。</div>`+panel('近7天活动会话',(state.sessions.length?`<div class="table-wrap"><table><thead><tr><th>Codex Session / 员工</th><th>绑定账号</th><th>近7天请求数</th><th>最近活动</th><th>操作</th></tr></thead><tbody>${state.sessions.map(s=>`<tr><td class="mono">${esc(sessionDisplayId(s))}<small>${esc(s.member_name)}</small></td><td>${esc(s.account_name)}</td><td>${fmt(s.request_count)}</td><td>${date(s.updated)}</td><td>${button('session-actions','管理会话','table-button',s.id,sessionsLoading)}</td></tr>`).join('')}</tbody></table></div>`:empty('发送第一条请求后自动创建会话。'))+sessionPager());
  if(tab==='requests'){
    const rows=usageRows().filter(k=>!requestFilter||k.member_name.includes(requestFilter)||(k.full_key||k.prefix).includes(requestFilter));
    html=heading('CUMULATIVE USAGE','使用记录','查看用量、请求阶段与操作历史；不记录对话正文。')+recordTabs();
    if(recordView==='usage')html+=`<div class="toolbar"><input id="request-filter" placeholder="搜索员工或 Key" value="${esc(requestFilter)}" aria-label="搜索员工或 Key"></div>`+panel(recentUsageTitle(),usageTable(rows));
    if(recordView==='requests')html+=requestsSection();
    if(recordView==='history')html+=historySection();
  }
  if(tab==='playground'&&!showGuide){
    html=heading('CONNECTION & TEST','接入与测试','用员工 Key 验证鉴权、订阅调用与额度统计。')+(admin?connectionTabs():'')+`<div class="chat-layout"><section class="panel"><div class="chat-controls"><h2>连接设置</h2><label>员工 API Key<input id="test-key" type="password" autocomplete="off" placeholder="hg_…" value="${esc(testKey)}" ${sending?'disabled':''}></label><button class="secondary" id="check-key">查看近7天用量</button><div id="my-quota" class="details-line"></div><label>模型<select id="chat-model" ${sending?'disabled':''}><option value="codex" ${chatModel==='codex'?'selected':''}>Codex</option><option value="claude" ${chatModel==='claude'?'selected':''}>Claude</option></select></label><p class="hint">同一对话持续使用同一个上游 session。新会话才重新分配账号。</p><div class="notice">纯文本模式<br>不执行代码或访问工作文件。</div>${button('clear-chat','＋ 新建会话','text-button')}<p class="hint mono">${chatSession?esc(chatSession):'尚未创建会话'}</p><p class="hint">页面刷新会清空显示。上游上下文保存在本机独立 Codex 会话目录，可通过 session_id 继续。</p></div></section><section class="panel"><div class="panel-heading"><h2>对话测试</h2><span class="hint">${chatSession?'会话已绑定':'新会话'}</span></div><div class="chat-log" id="chat-log">${renderChat()}</div><form class="composer" id="chat-form"><textarea id="prompt" placeholder="例如：用一句话解释什么是 API 网关" aria-label="消息" required ${sending?'disabled':''}>${esc(draft)}</textarea><div class="composer-bottom"><span class="hint">${sending?'正在等待回复，最长约 2 分钟…':'每次请求会消耗真实订阅额度'}</span><button class="primary" ${sending?'disabled':''}>${sending?'处理中…':'发送请求 ↗'}</button></div></form></section></div>`;
  }
  if(showGuide)html=heading('CONNECTION & TEST','接入与测试','使用员工 Key 从 Windows、WSL 或其他机器连接网关。')+connectionTabs()+`<div class="guide-grid">${panel('纯文本聊天配置',`<div class="panel-body"><label>Base URL</label><div class="code">${esc(state.endpoint)}</div><label>API Key</label><div class="code">员工 Key（hg_ 开头）</div><label>模型</label><div class="code">codex</div><p class="hint">此处是网页聊天使用的旧文本接口。Codex Windows / WSL 使用下方 Responses 配置，支持增量流和客户端工具调用。</p></div>`)}${panel('运行与数据',`<div class="panel-body"><ul class="guide-list"><li>客户端使用本页 Base URL 连接；订阅登录保存在网关服务器。</li><li>每账号并发 1，同员工并发 1，最多排队 5 个请求。</li><li>到期、停用或额度耗尽的 Key 会被拒绝。</li><li>来源 IP 使用实际连接地址，不信任客户端伪造的转发头。</li><li>异常用量未知时保留预占额度，管理员核对后释放。</li><li>SQLite 保存统计；官方凭证在独立 runtime 目录中。</li></ul>${button('backup','创建数据库备份','secondary')}</div>`)}</div>`+panel('请求示例',`<div class="panel-body"><pre class="code">POST /v1/chat/completions
Authorization: Bearer hg_你的员工Key
Content-Type: application/json

{
  "model": "codex",
  "messages": [{"role": "user", "content": "你好"}],
  "stream": false
}</pre><p class="hint">后台统一显示Standard参考消耗USD：按模型普通输入、缓存读取、输出分别计价，缓存不重复计算。它不是实际扣款或官方剩余额度；客户端协议usage维持官方原始格式。</p></div>`);
  if(showGuide)html+=panel('保持同一个会话',`<div class="panel-body"><p class="hint">第一条请求返回 session_id。后续请求带上该值，只发送本轮新消息；不提供 session_id 就会创建新会话、重新选择账号。</p><pre class="code">{
  "model": "codex",
  "session_id": "上一条响应中的会话ID",
  "messages": [{"role": "user", "content": "继续上一个问题"}]
}</pre><p class="hint">也可使用 X-Harbor-Session 请求头。完整历史会校验后去重；不同员工无法访问同一个会话。账号不足时暂停，不自动切换。上游会话正文保存在独立 Codex 目录，平台统计库只保存映射与摘要。</p></div>`);
  if(showGuide)html+=panel('Codex Windows / WSL 接入',`<div class="panel-body"><p>Windows：%USERPROFILE%&#92;.codex&#92;config.toml；WSL：~/.codex/config.toml。合并以下配置，顶层键放在第一个表头之前，不覆盖已有配置。</p><pre class="code">model = "gpt-6.1-sol"
model_provider = "harbor"
web_search = "disabled"

[model_providers.harbor]
name = "Harbor"
base_url = ${esc(JSON.stringify(state.endpoint))}
wire_api = "responses"
env_key = "HARBOR_API_KEY"
requires_openai_auth = false
supports_websockets = false

[features]
memories = false

[memories]
generate_memories = false
use_memories = false</pre><p>将员工 Key 放入客户端进程的 HARBOR_API_KEY 环境变量，然后完全退出并重启客户端。Windows 桌面应用必须能读取该变量；WSL需单独设置。不要把Key写进TOML或工程。</p><p>默认使用 gpt-6.1-sol，可在 Codex 中切换；模型名使用 GET /v1/models 返回的实际名称，推理强度按客户端选择转发。为让模型选择器显示最新模型，请带员工Key从 /v1/codex/models 下载目录，设置顶层 model_catalog_json 为文件的绝对路径并重启客户端；模型更新后重新下载。</p><p>网页版聊天和记忆不接入。文件读写和命令在客户端执行，平台只转发订阅请求。使用 HTTP 完整历史续聊；同一个 Key、同一个订阅账号均支持多会话并行；只累计已获取用量，不预占、不按本地额度拦截。中断后可在原任务继续。</p></div>`);
  if(showGuide)html+=panel('image2 图片生成与编辑（测试接入）',imageGuide(state.endpoint));
  $('#content').innerHTML=html;document.querySelectorAll('[data-height]').forEach(el=>el.style.height=el.dataset.height+'px');document.querySelectorAll('[data-width]').forEach(el=>el.style.width=el.dataset.width+'%');
  bindHistoryForm();bindRequestFilters();
  if(tab==='playground'&&!showGuide){$('#chat-form').onsubmit=sendChat;$('#check-key').onclick=checkKey;$('#test-key').oninput=e=>{testKey=e.target.value;$('#my-quota').textContent='';};$('#prompt').oninput=e=>draft=e.target.value;$('#chat-model').onchange=e=>{if(sending)return;chatModel=e.target.value;chat=[];chatSession='';chatOwner='';render();};$('#chat-log').scrollTop=$('#chat-log').scrollHeight;}
  if(tab==='sessions'){$('#session-page-size').onchange=async e=>{if(sessionsLoading)return;sessionsPageSize=Number(e.target.value);sessionsPage=1;sessionsLoading=true;render();await refresh();};}
  if(tab==='requests'&&recordView==='usage'){$('#request-filter').onchange=e=>{requestFilter=e.target.value;render();};}
}
function imageGuide(endpoint){
  const download=JSON.stringify(endpoint.replace(/\/$/,'')+'/codex/image-mcp.mjs');
  const windows=`Invoke-WebRequest -UseBasicParsing ${download} -Headers @{Authorization="Bearer $env:HARBOR_API_KEY"} -OutFile "$env:USERPROFILE\\.codex\\harbor-image-mcp.mjs"`;
  const linux=`curl --fail --silent --show-error -H "Authorization: Bearer $HARBOR_API_KEY" ${download} -o "$HOME/.codex/harbor-image-mcp.mjs"`;
  const config=`[mcp_servers.harbor_image]
command = "node"
args = ['C:\\Users\\YOUR_USER\\.codex\\harbor-image-mcp.mjs', 'C:\\Work\\YOUR_WORKSPACE']
env_vars = ["HARBOR_API_KEY"]
tool_timeout_sec = 600

[mcp_servers.harbor_image.env]
HARBOR_BASE_URL = ${JSON.stringify(endpoint)}`;
  return `<div class="panel-body"><p>原员工 Key 仍可使用，无需 OPENAI_API_KEY。每台客户端需单独安装图片 MCP；仅升级网关或新开聊天不会自动添加工具。</p><ol class="guide-list"><li>安装 Node.js 24，确认 node --version；在客户端进程中加载现有 HARBOR_API_KEY。</li><li>下载脚本到实际 CODEX_HOME（默认用户目录下的 .codex）；以下默认路径的父目录须已存在。</li></ol><label>Windows PowerShell</label><pre class="code">${esc(windows)}</pre><label>WSL / Linux</label><pre class="code">${esc(linux)}</pre><p>将下面配置合并到用户 config.toml，已有同名表时更新，不重复添加。把两个路径换成脚本和允许读写图片的工作区绝对路径；WSL 使用 Linux 路径。桌面应用找不到 node 时，填写 Node 24 可执行文件的绝对路径。</p><pre class="code">${esc(config)}</pre><p>完全退出并重启 Codex，再新建会话，确认 harbor_image 已连接、image_generate 工具可用。可以先运行 codex mcp list 检查注册；注册成功仍需实际调用验证。</p><p>生成：“用 Harbor 图片工具生成蓝色圆形，保存到 generated.png。”编辑：“把 generated.png 的圆形改成红色，保存到 edited.png。”输出路径必须在工作区内且文件尚不存在；检查图片可打开，并核对网关调用记录。</p><p>若仍提示“没有 image2 工具”或“缺少 OPENAI_API_KEY”，先检查是否加载了 harbor_image、Node 路径、工作区和环境变量；不要因此申请独立 API Key。超时或取消可能已消耗订阅额度，不自动重试。</p><p class="hint">这是自建 MCP 适配，功能实测不等于官方认可订阅转供。Linux 真实生图与改图已验收，Windows 原生图片流程尚未验收。远程使用应配置 HTTPS。移除上述 MCP 配置并重启即可停用；网页版聊天和记忆不接入。</p></div>`;
}
function renderChat(){return chat.length?chat.map(m=>`<div class="message ${m.role==='user'?'user':''}"><span class="role">${m.role==='user'?'YOU':chatModel.toUpperCase()}</span>${esc(m.content)}</div>`).join(''):empty('从一个简单问题开始。');}
let loginPoll,loginAccount='',loginView=0;
function modal(html,wide=false){if(!admin)return;if($('#modal').open)close();dialogVersion++;$('#modal').classList.toggle('wide',wide);$('#modal-content').innerHTML=html;$('#modal').showModal();}
function close(){clearTimeout(loginPoll);loginAccount='';loginView++;dialogVersion++;requestDetailId='';$('#modal').close();$('#modal-content').innerHTML='';}
function validDeviceLogin(value){try{const u=new URL(value.verificationUrl);return value.type==='device'&&u.origin==='https://auth.openai.com'&&u.pathname==='/codex/device'&&!u.search&&!u.hash&&!u.username&&!u.password&&/^[A-Z0-9-]{4,32}$/.test(value.userCode);}catch{return false;}}
function loginDialog(account,value){
  modal(`<h2>${esc(account.name)} · 官方登录</h2><p class="hint">在官方页面登录你的 ChatGPT 账号，再输入下面的一次性验证码。仅授权你刚刚发起的登录；验证码请勿分享。</p><div class="secret" id="device-code"></div><p class="hint">若新窗口未打开，可点击下方链接。设备码登录需在 ChatGPT 安全设置或工作区权限中启用。</p><div class="actions"><a class="primary" id="device-link" target="_blank" rel="noopener noreferrer">打开官方授权页面</a></div><p class="hint" id="login-status">等待官方授权，最多 10 分钟…</p><div class="dialog-actions">${button('cancel-login','取消登录','secondary',account.id)}${button('close','稍后继续','secondary')}</div>`);
  $('#device-code').textContent=value.userCode;$('#device-link').href=value.verificationUrl;
  loginAccount=account.id;const view=loginView,epoch=authEpoch;
  async function poll(){
    try{
      const current=await api('/api/admin/accounts/'+account.id+'/login');
      if(!admin||epoch!==authEpoch||view!==loginView||loginAccount!==account.id)return;
      if(current.state!=='login'){close();await refresh();return toast(current.state==='ready'?'账号已连接':current.message||'官方登录未完成，请重试');}
      if(current.expiresAt&&current.expiresAt<=Date.now()){$('#device-code').textContent='验证码已过期';$('#device-link').removeAttribute('href');$('#login-status').textContent='请取消后重新登录';return;}
      loginPoll=setTimeout(poll,2000);
    }catch(e){if(admin&&epoch===authEpoch&&view===loginView){$('#device-code').textContent='';$('#device-link').removeAttribute('href');$('#login-status').textContent=e.message;}}
  }
  loginPoll=setTimeout(poll,2000);
}
async function loginAccountFlow(identifier){
  const account=state.accounts.find(x=>x.id===identifier),epoch=authEpoch;let popup;
  if(account.kind==='codex')try{popup=window.open('about:blank','_blank');if(popup)popup.opener=null;}catch{}
  modal('<h2>正在获取官方授权…</h2><p class="hint">请稍候。获取失败会显示原因，可以关闭后重试。</p>');
  const view=loginView;
  try{
    const result=await api('/api/admin/accounts/'+identifier+'/login','POST',{});
    if(!admin||epoch!==authEpoch||view!==loginView){popup?.close();return;}
    if(result.type==='browser'){popup?.close();close();await refresh();return toast('请在运行 Harbor 的本机浏览器完成官方登录');}
    if(!validDeviceLogin(result))throw Error('官方授权信息无效，请重新登录');
    loginDialog(account,result);if(popup&&!popup.closed)popup.location.replace(result.verificationUrl);
    await refresh();
  }catch(e){popup?.close();if(admin&&epoch===authEpoch&&view===loginView){modal(`<h2>官方登录未能启动</h2><div class="inline-error">${esc(e.message)}</div><div class="dialog-actions">${button('cancel-login','取消登录','secondary',identifier)}${button('close','关闭','secondary')}</div>`);}throw e;}
}
$('#modal').oncancel=e=>{e.preventDefault();close();};
function formModal(title,sub,fields,onSubmit){modal(`<h2>${title}</h2><p class="hint">${sub}</p><form id="dialog-form">${fields}<div id="form-error" class="inline-error"></div><div class="dialog-actions"><button class="secondary" type="button" data-action="close">取消</button><button class="primary" type="submit">保存</button></div></form>`);$('#dialog-form').onsubmit=async e=>{e.preventDefault();const btn=e.target.querySelector('[type=submit]');btn.disabled=true;try{await onSubmit(new FormData(e.target));}catch(err){const output=$('#form-error');if(output)output.textContent=err.message;else toast(err.message);}finally{btn.disabled=false;}};}
const expirationFields=`<label>有效期<select name="days"><option value="30">30 天</option><option value="7">7 天</option><option value="90">90 天</option><option value="0">永久</option></select></label>`;
const expiresFrom=f=>Number(f.get('days'))?Date.now()+Number(f.get('days'))*86400000:null;

function showSecret(secret){if(!admin)return;close();modal(`<h2>Key 已生成</h2><p class="hint">Key 已加密保存，可在运行总览或使用记录中完整查看和复制。</p><div class="secret" id="new-secret"></div><div class="dialog-actions">${button('close','完成','secondary')}${button('use-secret','去请求测试','secondary')}${button('copy-secret','复制 Key','primary')}</div>`);$('#new-secret').textContent=secret;}
function sessionDisplayId(s){return s.protocol==='responses'?(s.client_session_id||'未获取'):'旧 CLI 会话';}
function sessionStatus(s){return !s.enabled?'已禁止访问':s.state==='active'?'已绑定':s.state==='uncertain'?(s.protocol==='responses'?'可继续 · 用量可能漏计':'需新建会话'):'待首次请求';}
function sessionDetail(identifier){
  const s=state.sessions.find(x=>x.id===identifier);if(!s)return toast('会话不在当前列表，请刷新后重试');
  const fixedApi=apiAccount((state.accounts||[]).find(a=>a.id===s.account_id));
  modal(`<h2>管理会话</h2><p class="mono muted">${esc(sessionDisplayId(s))}</p><div class="details-line">员工：${esc(s.member_name)}<br>当前状态：${sessionStatus(s)}<br>绑定账号：${esc(s.account_name)}<br>近7天Standard参考消耗（USD）：${referenceTotal(s.standard)}<br>${esc(referenceCoverage(s.standard))}<br>自动调整：${fixedApi?'API人工固定，不参与夜间调整':s.nightly_enabled?'已参与':'临时固定至 '+beijingDate(s.nightly_expires)+'（北京时间02:00）'}</div><div class="actions">${s.protocol==='responses'&&s.enabled?button('route-session','修改绑定账号','secondary',s.id):''}${s.protocol==='responses'&&!fixedApi?button('nightly-session',s.nightly_enabled?'临时固定至02:00':'恢复自动调整','secondary',s.id):''}${button('toggle-session',s.enabled?'禁止该会话访问':'恢复该会话访问',s.enabled?'text-button danger':'secondary',s.id)}</div>${s.protocol!=='responses'?'<p class="hint">旧 CLI Session 的上下文保存在原账号目录，暂不支持修改绑定账号。</p>':''}<div class="dialog-actions">${button('close','关闭','primary')}</div>`);
}
function memberDetail(memberId){const m=state.members.find(x=>x.id===memberId),keys=state.keys.filter(k=>k.member_id===memberId);modal(`<h2>${esc(m.name)}</h2><div class="actions">${button('edit-member','修改名称','secondary',m.id)}${button('new-key','新增 Key','secondary',m.id)}${button('toggle-member',m.enabled?'停用员工':'启用员工','text-button',m.id)}</div>${keys.map(k=>`<div class="mini-row"><div class="mono">${esc(k.prefix)}…<small>${k.expires?'到期 '+date(k.expires):'永久有效'} · ${!k.enabled?'已停用':!m.enabled?'员工已停用':k.expires&&k.expires<=Date.now()?'已过期':'可用'}</small></div><div>${button('key-ips',fmt(k.ips)+' 个 IP','table-button',k.id)}${button('renew-key','续期','table-button',k.id)}${button('toggle-key',k.enabled?'停用':'启用','table-button',k.id)}${button('remove-key','删除','table-button',k.id)}</div></div>`).join('')}<p class="hint">近7天用量见使用记录。换 Key 不影响其他 Key 的统计。</p><div class="dialog-actions">${button('close','完成','primary')}</div>`);}
async function checkKey(){testKey=$('#test-key').value.trim();const key=testKey,epoch=authEpoch;try{const m=await api('/api/me','GET',undefined,key);if(epoch!==authEpoch||tab!=='playground'||$('#test-key')?.value.trim()!==key)return;$('#my-quota').textContent=`${m.name}：此 Key 近7天 ${referenceTotal(m.keyStandard)} Standard参考USD · ${referenceCoverage(m.keyStandard)}`;}catch(e){if(epoch===authEpoch)toast(e.message);}}
async function sendChat(e){
  e.preventDefault();testKey=$('#test-key').value.trim();const content=$('#prompt').value.trim();
  if(!testKey)return toast('请填写员工 Key');if(!content||sending)return;
  const epoch=authEpoch;draft=content;sending=true;render();let appended=false;
  try{
    const member=await api('/api/me','GET',undefined,testKey);
    if(epoch!==authEpoch)return;
    if(chatOwner&&chatOwner!==member.id){chat=[];chatSession='';toast('已切换员工，自动新建会话');}
    chatOwner=member.id;chat.push({role:'user',content});appended=true;draft='';if(tab==='playground')render();
    const r=await api('/v1/chat/completions','POST',{model:chatModel,messages:chatSession?[chat.at(-1)]:chat,stream:false,...(chatSession?{session_id:chatSession}:{})},testKey);
    if(epoch!==authEpoch)return;
    if(r.context_reset)chat=[{role:'user',content}];chatSession=r.session_id;chat.push({role:'assistant',content:r.choices[0].message.content});toast((r.context_reset?'测试上下文已重置 · ':'完成 · ')+referenceTotal(r.harbor_standard_usage)+' Standard参考USD · '+referenceState(r.harbor_standard_usage?.state));
  }catch(err){if(epoch===authEpoch){if(appended)chat.pop();draft=content;toast(err.message+'；输入已保留');}}
  finally{sending=false;if(admin&&epoch===authEpoch)await refresh(false);if(tab==='playground'&&epoch===authEpoch)render();}
}

document.addEventListener('click',async e=>{
  const nav=e.target.closest('[data-tab]');if(nav)return setTab(nav.dataset.tab);
  const el=e.target.closest('[data-action]');if(!el)return;const action=el.dataset.action,identifier=el.dataset.id;
  try{
    if(action.startsWith('pricing-'))return await pricingAction(action,identifier);
    if(action==='connection-view'){if(sending)return toast('请等待当前请求完成');if(!admin||!['guide','test'].includes(identifier))return;connectionView=identifier;render();return;}
    if(action==='requests-reload')return await loadRequests();
    if(action==='requests-page'){if(requestsLoading||!requestsData)return;const page=Number(identifier);if(!Number.isInteger(page)||page<1||page>requestsData.pagination.totalPages)return;requestsFilters.page=page;return await loadRequests();}
    if(action==='record-view'){if(!['usage','requests','history'].includes(identifier))return;recordView=identifier;historyVersion++;historyLoading=false;requestsVersion++;requestsLoading=false;render();if(identifier==='history')return await loadHistory();if(identifier==='requests')return await loadRequests();return;}
    if(action==='history-reload')return await loadHistory();
    if(action==='history-reset'){historyFilters={kind:'audit',page:1,action:'',account:'',session:'',start:'',end:''};historyData=null;return await loadHistory();}
    if(action==='history-page'){if(historyLoading)return;historyFilters.page=Number(identifier);return await loadHistory();}
    if(action==='routing-history'){close();recordView='history';tab='requests';historyFilters={...historyFilters,kind:'audit',action:'balancing.weights.auto',page:1};historyData=null;render();return await loadHistory();}
    if(action==='history-detail'){const r=historyData?.rows.find(x=>x.id===identifier);if(!r)return toast('历史记录已更新，请重新选择');return modal(`<h2>${esc(actionLabels[r.action]||r.action)}</h2><p>${date(r.at)}</p><div class="notice history-description">${esc(historyText(r))}</div>${r.clientSession||r.session?`<p class="mono">会话：${esc(r.clientSession||r.session)}</p>`:''}${r.detail.basis?`<p class="hint">计算依据时间：${beijingDate(r.detail.basis.at)}（北京时间）</p><div class="table-wrap"><table><thead><tr><th>账号</th><th>当时剩余</th><th>距重置</th><th>预算值</th><th>计算权重</th></tr></thead><tbody>${r.detail.basis.accounts.map(a=>`<tr><td>${esc(historyName(a.id))}</td><td>${a.remaining??'—'}%</td><td>${a.hours===null?'—':a.hours.toFixed(2)+'h'}</td><td>${a.score===null?'—':a.score.toFixed(4)}</td><td>${esc(weightPercent(a.weight))}</td></tr>`).join('')}</tbody></table></div>`:''}<div class="dialog-actions">${button('close','关闭','primary')}</div>`,true);}
    if(action==='routing-explanation'){modal('<h2>调度依据</h2><p>正在读取…</p>',true);const version=dialogVersion,epoch=authEpoch,r=await api('/api/admin/balancing/explanation');if(admin&&epoch===authEpoch&&version===dialogVersion&&$('#modal').open)$('#modal-content').innerHTML=routingExplanation(r);return;}
    if(action==='nightly-history'){modal('<h2>夜间批次</h2><p>正在读取…</p>',true);const version=dialogVersion,epoch=authEpoch,r=await api('/api/admin/history/nightly/'+identifier);if(admin&&epoch===authEpoch&&version===dialogVersion&&$('#modal').open)$('#modal-content').innerHTML=nightlyHistory(r);return;}
    if(action==='usage-help')return modal(`<h2>Standard参考消耗说明</h2><p>按各模型官方Standard短上下文参考价，分别计算非缓存输入、缓存读取和输出，再以USD汇总；缓存已包含在输入中，不重复计算。人工核对单列。它是统一参考工作量，不是实际扣款或订阅扣额，不含Fast、长上下文等附加费及工具/存储费用。</p><p>近7天为北京时间今天及前6天；近12小时滚动统计。按完成时间归属，进行中暂按开始时间。缺模型费率或缺计量会分别提示，不当作0；已过保留期无法恢复。费率版本固定，后台同步不改写已计价请求，未经过平台的调用不包含在内。</p><div class="dialog-actions">${button('close','关闭','primary')}</div>`);
    if(action==='close')return close();if(action.startsWith('go-'))return setTab(action.slice(3));
    if(action==='create-member')return formModal('创建员工 Key','Key 支持 Codex 多会话并行，仅统计最近7天用量。',`<label>员工名称<input name="name" placeholder="例如：张同事" maxlength="60" required></label>${expirationFields}`,async f=>{const epoch=authEpoch;const r=await api('/api/admin/members','POST',{name:f.get('name'),quota:null,expires:expiresFrom(f)});await refresh();if(epoch===authEpoch)showSecret(r.secret);});
    if(action==='member-detail')return memberDetail(identifier);
    if(action==='edit-member'){const m=state.members.find(x=>x.id===identifier);close();return formModal('编辑员工','修改员工名称。',`<label>名称<input name="name" value="${esc(m.name)}" required maxlength="60"></label>`,async f=>{await api('/api/admin/members/'+identifier,'PATCH',{name:f.get('name')});close();await refresh();toast('已保存');});}
    if(action==='new-key'){close();return formModal('新增 Key','为此员工增加一个独立 Key。',expirationFields,async f=>{const epoch=authEpoch;const r=await api('/api/admin/keys','POST',{memberId:identifier,expires:expiresFrom(f),quota:null});await refresh();if(epoch===authEpoch)showSecret(r.secret);});}
    if(action==='toggle-member'){const m=state.members.find(x=>x.id===identifier);await api('/api/admin/members/'+identifier,'PATCH',{enabled:!m.enabled});close();await refresh();return toast('员工状态已更新');}
    if(action==='toggle-key'){const k=state.keys.find(x=>x.id===identifier);await api('/api/admin/keys/'+identifier,'PATCH',{enabled:!k.enabled});await refresh();close();return memberDetail(k.member_id);}
    if(action==='remove-key'){const k=state.keys.find(x=>x.id===identifier);return modal(`<h2>删除 Key？</h2><p class="mono">${esc(k.prefix)}…</p><p class="hint">删除后此 Key 永久失效，无法恢复或重新启用；历史调用与用量记录保留。该员工的请求须先完成。继续使用时需要新增 Key。</p><div class="dialog-actions">${button('close','取消','secondary')}${button('confirm-remove-key','确认删除','primary',identifier)}</div>`);}
    if(action==='confirm-remove-key'){const epoch=authEpoch,k=state.keys.find(x=>x.id===identifier);el.disabled=true;await api('/api/admin/keys/'+identifier,'DELETE');close();await refresh();if(epoch!==authEpoch||!admin)return;toast('Key 已删除，历史用量保留');return memberDetail(k.member_id);}
    if(action==='renew-key'){close();return formModal('Key 有效期','更改有效期不会重置近7天用量。',`<label>有效期<select name="days"><option value="keep">保持当前到期时间</option><option value="7">从现在起 7 天</option><option value="30">从现在起 30 天</option><option value="90">从现在起 90 天</option><option value="0">永久</option></select></label>`,async f=>{await api('/api/admin/keys/'+identifier,'PATCH',f.get('days')==='keep'?{}:{expires:expiresFrom(f)});close();await refresh();toast('Key 已更新');});}
    if(action==='copy-key'){const key=state.keys.find(k=>k.id===identifier);if(key?.full_key){await copyKey(key.full_key);toast('已复制 Key');}return;}
    if(action==='copy-secret'){await copyKey($('#new-secret').textContent);return toast('已复制 Key');}
    if(action==='use-secret'){testKey=$('#new-secret').textContent;close();connectionView='test';setTab('playground');return;}
    if(action==='ips'||action==='key-ips'){const key=action==='key-ips'?state.keys.find(k=>k.id===identifier):null;const rows=await api('/api/admin/ips?member='+(key?key.member_id:identifier)+(key?'&key='+key.id:''));close();return modal(`<h2>${key?'此 Key 的':'员工全部 Key 的'}来源 IP</h2><p class="hint">IP 数量不等于设备数。本机测试通常显示 127.0.0.1。</p>${rows.length?rows.map(r=>`<div class="mini-row"><div><strong class="mono">${esc(r.ip)}</strong><small>首次 ${date(r.first_seen)}<br>最近 ${date(r.last_seen)}<br>${esc(r.agent)}</small></div><span>${r.requests} 次</span></div>`).join(''):empty('暂无记录')}<div class="dialog-actions">${button('close','关闭','primary')}</div>`);}
    if(action==='routing-weights'){
      const w=state.balancing?.weights;
      if(!w)return toast('请先刷新账号状态');
      const accounts=state.accounts.filter(a=>a.kind==='codex'&&!apiAccount(a));
      if(!accounts.length)return toast('请先添加Codex账号');
      const cfg=w.configuration;
      const seeds=accounts.map(a=>({a,value:cfg.mode==='manual'?cfg.weights[a.id]||0:w.accounts.find(x=>x.id===a.id)?.automatic}));
      if(cfg.mode!=='manual'){
        const sum=seeds.reduce((n,x)=>n+(x.value||0),0);let left=10000;
        for(let i=0;i<seeds.length;i++){const bp=i===seeds.length-1?left:Math.min(left,Math.round((sum?(seeds[i].value||0)/sum:1/seeds.length)*10000));seeds[i].value=bp/100;left-=bp;}
      }
      return formModal('临时分配权重','按填写比例执行，总和须为100%。订阅额度为0也可设置正权重，使用官方自动扣除的额外额度；重复额度查询不会覆盖手动比例。0权重不参与兜底。每天北京时间02:00恢复自动。',`${!state.balancing.settings.weighted?'<p class="hint">新权重尚未开启；请先在负载均衡中开启，临时设置才会生效。</p>':''}${seeds.map(x=>`<label>${esc(x.a.name)}（系统 ${esc(weightPercent(w.accounts.find(r=>r.id===x.a.id)?.automatic))}）<input name="weight_${esc(x.a.id)}" type="number" min="0" max="100" step="0.01" value="${x.value}" required></label>`).join('')}<p class="hint">${cfg.mode==='manual'?'当前手动设置到期：'+date(cfg.expires):'保存后有效至下一次北京时间02:00，最长24小时。'} 正权重绑定保持；设为0的账号，其会话在下次空闲请求时按权重换绑。夜间预览使用自动权重；额外额度不足时返回官方错误，不自动重放。</p>${button('routing-auto','恢复自动','secondary')}`,async f=>{const weights=Object.fromEntries(accounts.map(a=>[a.id,Number(f.get('weight_'+a.id))]));await api('/api/admin/balancing/weights','PATCH',{mode:'manual',weights,expected_version:cfg.version});close();await refresh();toast('临时权重已保存');});
    }
    if(action==='routing-auto'){
      const cfg=state.balancing.weights.configuration;await api('/api/admin/balancing/weights','PATCH',{mode:'auto',expected_version:cfg.version});close();await refresh();return toast('已恢复自动权重');
    }
    if(action==='balancing-settings'){
      const b=state.balancing?.settings||{failover:false,nightly:false,quietMinutes:30},last=state.balancing?.lastRun;
      return formModal('负载均衡','自动权重在每日02:00及额度耗尽、恢复时更新，普通余量刷新不改比例。手动设置保持至02:00；额度切换只选正权重账号，不重放失败请求。',`<label>按24小时额度预算分配<select name="weighted"><option value="false" ${!b.weighted?'selected':''}>关闭（原策略）</option><option value="true" ${b.weighted?'selected':''}>开启</option></select></label><label>登录失效自动切换<select name="failover"><option value="false" ${!b.failover?'selected':''}>关闭</option><option value="true" ${b.failover?'selected':''}>开启</option></select></label><label>北京时间每日 02:00 调整<select name="nightly"><option value="false" ${!b.nightly?'selected':''}>关闭</option><option value="true" ${b.nightly?'selected':''}>开启</option></select></label><label>静默期（分钟）<input name="quietMinutes" type="number" min="1" max="960" value="${b.quietMinutes}" required></label><p class="hint">统计过去16小时活跃会话，整组负载偏差允许5个百分点；优先迁移过载账号的重会话，再优先让额度预算更高的账号承担偏差，最后尽量减少迁移数量。容差内偏差落在低预算侧时也会尝试纠正，可能需要增加迁移；同预算账号不区分方向。关闭夜间调整仍在02:00恢复自动权重。手动换绑和临时排除均只保持至下次北京时间02:00，届时恢复自动参与；在途保护不变。</p><p class="hint">最近执行：${last?esc(last.day)+' · '+esc(({completed:'已完成',failed:'失败',interrupted:'已中断',running:'执行中'})[last.status]||last.status):'尚未执行'}</p>${button('balancing-preview','预览分配（不修改绑定）','secondary')}`,async f=>{await api('/api/admin/balancing','PATCH',{weighted:f.get('weighted')==='true',failover:f.get('failover')==='true',nightly:f.get('nightly')==='true',quietMinutes:Number(f.get('quietMinutes'))});close();await refresh();toast('负载均衡设置已保存');});
    }
    if(action==='balancing-preview'){
      el.disabled=true;const epoch=authEpoch,result=await api('/api/admin/balancing/preview','POST',{});if(epoch!==authEpoch||!admin)return;
      const accountName=id=>state.accounts.find(a=>a.id===id)?.name||id;
      return modal(`<h2>夜间分配预览</h2><p class="hint">按当前额度和自动权重估计，临时手动权重不参与夜间计划。仅预览，不会修改绑定。窗口结束：${date(result.window.end)}。预计迁移 ${result.moves.length} 个会话，跳过 ${result.skipped.length} 项。</p>${result.summaries.map(g=>`${planningHint(g.planning)}<div class="table-wrap"><table><thead><tr><th>账号</th><th>当前负载</th><th>固定负载</th><th>分配后</th></tr></thead><tbody>${g.accounts.map(a=>`<tr><td>${esc(accountName(a))}</td><td>${referenceMoney(g.before[a])}</td><td>${referenceMoney(g.fixedLoad[a])}</td><td>${referenceMoney(g.after[a])}</td></tr>`).join('')}</tbody></table></div>`).join('')}<p class="hint">负载统一为Standard参考USD，包含历史参考消耗与同模型在途估计，不代表实际扣款或官方额度。</p>${result.moves.length?result.moves.map(m=>`<div class="mini-row"><span>${esc(sessionDisplayId(m))}</span><span>${esc(accountName(m.from))} → ${esc(accountName(m.to))}</span></div>`).join(''):empty('当前没有需要迁移的会话')}<p class="hint">忙碌、静默期内、手动排除、额度不可比或样本不足均可能跳过。长任务已计入原账号负载。</p><div class="dialog-actions">${button('close','关闭','primary')}</div>`);
    }
    if(action==='nightly-session'){const s=state.sessions.find(x=>x.id===identifier);await api('/api/admin/sessions/'+identifier,'PATCH',{nightly_enabled:!s.nightly_enabled});close();await refresh();return toast('夜间调整设置已更新');}
    if(action==='sessions-page'){if(sessionsLoading)return;sessionsPage=Number(identifier);sessionsLoading=true;render();return refresh();}
    if(action==='session-actions'){if(sessionsLoading)return;return sessionDetail(identifier);}
    if(action==='route-session'){
      const s=state.sessions.find(x=>x.id===identifier),accounts=state.accounts.filter(a=>a.kind==='codex'&&a.enabled&&a.state==='ready'&&!a.route_block);
      if(!accounts.some(a=>a.id!==s.account_id))return toast('暂无其他已连接的 Codex 账号');
      return formModal('修改绑定账号','从下一次请求起使用选定账号。订阅账号固定至下次02:00；官方API Key绑定由人工控制。不会重发失败请求；客户端须携带完整历史。',`<p class="hint">当前绑定：${esc(s.account_name)}</p><label>目标账号<select name="account_id">${accounts.map(a=>`<option value="${esc(a.id)}" ${a.id===s.account_id?'selected':''}>${esc(a.name)}</option>`).join('')}</select></label>`,async f=>{await api('/api/admin/sessions/'+identifier,'PATCH',{account_id:f.get('account_id'),expected_route_version:s.route_version});close();await refresh();toast('绑定已更新，下次请求生效');});
    }
    if(action==='toggle-session'){
      const s=state.sessions.find(x=>x.id===identifier);
      return modal(`<h2>${s.enabled?'禁止该会话访问？':'恢复该会话访问？'}</h2><p class="hint">${s.enabled?'该会话后续请求将被拒绝，已经开始的请求允许完成。其他会话与员工 Key 不受影响。':'恢复后继续使用当前绑定账号，不自动切换账号。'}</p><div class="dialog-actions">${button('close','取消','secondary')}${button(s.enabled?'disable-session':'enable-session',s.enabled?'确认禁止':'确认恢复','primary',identifier)}</div>`);
    }
    if(action==='disable-session'||action==='enable-session'){await api('/api/admin/sessions/'+identifier,'PATCH',{enabled:action==='enable-session'});close();await refresh();return toast(action==='enable-session'?'会话访问已恢复':'该会话已禁止访问');}
    if(action==='create-account'||action==='create-api-account'){
      const keyEntry=action==='create-api-account';
      formModal('添加上游账号','每次添加一个独立账号或Key，可重复添加多条，分别管理启停和权重。',`<label>接入类型<select name="auth_mode" id="new-account-auth" required><option value="subscription" ${!keyEntry?'selected':''}>订阅账号</option><option value="openai_api" ${keyEntry?'selected':''}>API Key</option></select></label><label>账号名称<input name="name" required maxlength="60"></label><fieldset id="new-subscription-fields"><label>订阅服务<select name="kind"><option value="codex">Codex订阅</option><option value="claude">Claude订阅（待真实验证）</option></select></label></fieldset><fieldset id="new-api-fields"><p class="hint">当前支持OpenAI官方API Key。</p><label>官方API Key<input id="new-api-key" name="api_key" type="password" autocomplete="new-password" maxlength="4096"></label><label>人工新会话占比（%）<input name="api_weight" type="number" min="0" max="100" step="0.01" value="0" required></label><p class="hint">0只用于人工固定会话，不加入其他人的新会话分配。占比与启停不在凌晨自动修改，余额需在官方后台核对。</p></fieldset>`,async f=>{
        const mode=f.get('auth_mode');await api('/api/admin/accounts','POST',{name:f.get('name'),auth_mode:mode,kind:mode==='openai_api'?'codex':f.get('kind'),...(mode==='openai_api'?{api_key:f.get('api_key'),api_weight:Number(f.get('api_weight'))}:{})});close();await refresh();toast(mode==='openai_api'?'已加密保存，请检查Key后创建固定会话':'账号已创建，请完成官方登录');
      });const showType=mode=>{const apiMode=mode==='openai_api';$('#new-api-fields').classList.toggle('hidden',!apiMode);$('#new-api-fields').disabled=!apiMode;$('#new-subscription-fields').classList.toggle('hidden',apiMode);$('#new-subscription-fields').disabled=apiMode;$('#new-api-key').required=apiMode;};$('#new-account-auth').onchange=e=>showType(e.target.value);showType(keyEntry?'openai_api':'subscription');return;
    }
    if(action==='api-account-settings'){
      const a=state.accounts.find(x=>x.id===identifier);if(!apiAccount(a))return;
      return formModal('官方Key与人工权重','Key留空保持现值；更换后须重新检查。配置长期保留，不受每日02:00影响。',`<label>更换官方Key<input name="api_key" type="password" autocomplete="new-password" maxlength="4096" placeholder="留空保持现有Key"></label><label>人工新会话占比（%）<input name="api_weight" type="number" min="0" max="100" step="0.01" value="${a.api_weight||0}" required></label><p class="hint">全部API账号占比之和最多100%；其余由订阅组按原规则分配。权重0仍允许使用人工固定会话，停用才会迁出全部绑定。Key明文不会回显。</p>`,async f=>{const key=f.get('api_key').trim();await api('/api/admin/accounts/'+identifier,'PATCH',{api_weight:Number(f.get('api_weight')),...(key?{api_key:key}:{})});close();await refresh();toast('人工配置已保存');});
    }
    if(action==='create-fixed-session'){
      const a=state.accounts.find(x=>x.id===identifier),members=state.members.filter(m=>m.enabled);if(!apiAccount(a)||!members.length)return toast('请先创建可用员工');
      return formModal('创建API固定会话','使用所选员工现有Key，新开一个客户端对话，专用于此上游。',`<p>绑定账号：${esc(a.name)}</p><label>所属员工<select name="member_id">${members.map(m=>`<option value="${esc(m.id)}">${esc(m.name)}</option>`).join('')}</select></label>`,async f=>{const version=dialogVersion,epoch=authEpoch,r=await api('/api/admin/sessions','POST',{member_id:f.get('member_id'),account_id:identifier});if(!admin||epoch!==authEpoch||version!==dialogVersion||!$('#modal').open)return;close();modal(`<h2>API固定会话已创建</h2><p>在已配置Harbor（provider名harbor）与所属员工Key的PowerShell或WSL终端执行下面命令，启动一个专用CLI对话。参数仅对这个进程生效，不修改全局配置或其他已开的会话。</p><pre class="code">${esc(fixedSessionCommand(r.session_id))}</pre><p class="hint">模型须选择该官方Key可用的实际模型。此进程只使用一条客户端对话，不要复用于其他新对话；结束后退出即可。固定标识不替代员工Key，官方Key始终只保存在网关。余额抵扣需与官方账单核对。</p><div class="dialog-actions">${button('close','完成','primary')}</div>`,true);});
    }
    if(action==='refresh-quotas'){el.disabled=true;await api('/api/admin/quotas/refresh','POST',{});await refresh();return toast(state.accounts.some(a=>a.enabled&&a.quota_error)?'部分账号额度未能刷新，已保留上次结果':'官方额度已更新');}

    if(action==='toggle-account'){const a=state.accounts.find(x=>x.id===identifier);if(!a)return;if(a.enabled)return modal(`<h2>停用 ${esc(a.name)}？</h2><p>立即迁移全部绑定会话，正在执行的请求按原账号完成，后续请求使用新绑定。网页测试聊天清空旧上下文，Codex客户端继续使用自己的历史。</p><p class="hint">停用不会在凌晨自动恢复，须手动启用。仅临时设置权重为0才会在北京时间02:00恢复自动；无可用接收账号时整笔操作拒绝。</p><div class="dialog-actions">${button('close','取消','secondary')}${button('confirm-disable-account','确认停用并迁移','primary',identifier)}</div>`);await api('/api/admin/accounts/'+identifier,'PATCH',{enabled:true});await refresh();return;}
    if(action==='confirm-disable-account'){el.disabled=true;try{const r=await api('/api/admin/accounts/'+identifier,'PATCH',{enabled:false});close();await refresh();toast(`账号已停用，迁移 ${r.moved} 个会话${r.resetTextSessions?'，其中 '+r.resetTextSessions+' 个测试会话已清空旧上下文':''}`);}finally{el.disabled=false;}return;}
    if(action==='login-account'){el.disabled=true;return await loginAccountFlow(identifier);}
    if(action==='cancel-login'){el.disabled=true;await api('/api/admin/accounts/'+identifier+'/login/cancel','POST',{});close();await refresh();return toast('登录已取消，可重新登录');}
    if(action==='check-account'){el.disabled=true;const r=await api('/api/admin/accounts/'+identifier+'/check','POST',{});await refresh();return toast(r.ready?(apiAccount(state.accounts.find(a=>a.id===identifier))?'官方Key认证检查通过；余额仍请在官方核对':'已检测到订阅登录'):'未检测到订阅登录');}
    if(action==='remove-account'){const account=state.accounts.find(x=>x.id===identifier);return modal(`<h2>移除 ${esc(account.name)}？</h2><p class="hint">移除后不再使用此账号，正在登录的流程会取消，绑定会话需新建。历史用量、会话记录和独立目录保留；不会删除其他账号。正在处理或排队的请求须先完成。</p><div class="dialog-actions">${button('close','取消','secondary')}${button('confirm-remove-account','确认移除','primary',identifier)}</div>`);}
    if(action==='confirm-remove-account'){el.disabled=true;await api('/api/admin/accounts/'+identifier,'DELETE');close();await refresh();return toast('账号已移除，历史用量保留');}
    if(action==='request-detail')return await requestDetail(identifier);
    if(action==='settle'){close();return formModal('核对异常请求','填写整笔请求已核对的Standard参考消耗USD，替代原不完整统计，不伪造原始计量。填0可释放旧预占；此操作会记入审计。','<label>Standard参考消耗（USD）<input name="standard_usd" type="number" min="0" step="0.000001" required></label>',async f=>{await api('/api/admin/requests/'+identifier+'/settle','POST',{standard_usd:f.get('standard_usd')});close();await refresh();toast('已补记用量并释放预占');});}
    if(action==='clear-chat'){if(sending)return toast('请等待当前请求完成');chat=[];chatSession='';render();return;}
    if(action==='backup'){const r=await api('/api/admin/backup','POST',{});return toast('备份已保存：'+r.file);}
    if(action==='export'){const epoch=authEpoch,r=await fetch('/api/admin/export');if(!r.ok)throw Error('导出失败');const blob=await r.blob();if(!admin||epoch!==authEpoch)return;const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download='Harbor-使用记录.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);return;}
  }catch(err){toast(err.message);}finally{el.disabled=false;}
});
$('#login-form').onsubmit=async e=>{e.preventDefault();const submit=e.submitter;submit.disabled=true;try{await api('/api/auth/login','POST',{password:$('#admin-input').value});authEpoch++;admin=true;$('#admin-input').value='';await refresh();}catch(err){toast(err.message);}finally{submit.disabled=false;}};
$('#employee-entry').onclick=()=>{connectionView='test';tab='playground';render();};$('#refresh').onclick=()=>refresh();$('#logout').onclick=async()=>{if(sending)return toast('请等待当前请求完成后退出');try{await api('/api/auth/logout','POST',{});authEpoch++;admin=false;state=null;testKey='';chat=[];chatSession='';chatOwner='';draft='';showLogin();}catch(err){toast(err.message);}};
setInterval(()=>{void automaticRefresh();},12000);
setInterval(()=>{if(admin&&requestDetailId&&$('#modal').open&&!document.hidden)requestDetail(requestDetailId,false).catch(e=>toast(e.message));},4000);
const initialAuthEpoch=authEpoch;api('/api/auth/session').then(r=>{if(initialAuthEpoch!==authEpoch)return;admin=r.authenticated;if(admin)refresh();else showLogin();}).catch(()=>{if(initialAuthEpoch===authEpoch)showLogin();});
