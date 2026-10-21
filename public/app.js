const $=s=>document.querySelector(s), esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const fmt=n=>Number(n||0).toLocaleString('zh-CN'),date=t=>t?new Date(t).toLocaleString('zh-CN',{hour12:false}):'—';
const tokenBillions=n=>(Number(n||0)/1e8).toLocaleString('zh-CN',{minimumFractionDigits:2,maximumFractionDigits:2});
const beijingDate=t=>t?new Date(t).toLocaleString('zh-CN',{timeZone:'Asia/Shanghai',hour12:false}):'—';
let admin=false,authEpoch=0,state=null,tab='overview',testKey='',chat=[],chatSession='',chatModel='codex',draft='',chatOwner='',sending=false,requestFilter='',statusFilter='';
let sessionsPage=1,sessionsPageSize=25,sessionsLoading=false,refreshVersion=0;
const labels={overview:'运行总览',members:'员工与密钥',accounts:'订阅账号池',sessions:'会话绑定',requests:'使用记录',playground:'请求测试',guide:'接入说明'};
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
function quotaCard(a){
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
  const reason=({within_tolerance:'已在容差内，无需迁移',balanced:'已找到达标计划',infeasible:'受固定负载或会话大小限制，无法达标；仅保留可改善偏差的迁移',search_limited:'计算预算不足以确认重会话方案，本组不迁移',priority_unresolved:'无法确认完整重会话方案，本组不迁移'})[p.reason]||'计划结果待确认';
  return `<p class="hint">整组偏差 ${esc(Number(p.beforeDeviation).toFixed(2))} → ${esc(Number(p.afterDeviation).toFixed(2))} 个百分点（容差 ${esc(p.tolerance)}）。${esc(reason)}。优先迁移重会话 ${fmt(p.prioritySessions?.length)} 个${p.heavySkipped?.length?'，无法纳入或待确认 '+fmt(p.heavySkipped.length)+' 个':''}。${p.reason==='balanced'&&!p.optimizationComplete?'已满足重会话优先，计算预算已用完，尚未证明迁移数量最少。':''}</p>`;
}
function routingCard(a){
  if(a.kind!=='codex'||!state.balancing?.weights)return '';
  const w=state.balancing.weights,row=w.accounts.find(x=>x.id===a.id);
  if(!row)return '';
  const mode=({auto:w.reason?'自动 · 等待额度确认，沿用已知权重':'自动',manual:'临时手动',fallback:'额度待确认 · 按原顺序',legacy:'新权重未开启'})[w.mode];
  return `<div class="quota"><div class="quota-row">系统权重<strong>${esc(weightPercent(row.automatic))}</strong></div><div class="quota-row">生效权重<strong>${esc(weightPercent(row.effective))}</strong></div><small>${esc(mode)}${w.configuration.mode==='manual'?' · 手动到期：'+beijingDate(w.configuration.expires):''}</small></div>`;
}
async function refresh(renderPage=true){
  if(!admin)return;const epoch=authEpoch,version=++refreshVersion;
  const query=sessionsPage===1&&sessionsPageSize===25?'':`?session_page=${sessionsPage}&session_page_size=${sessionsPageSize}`;
  try{const next=await api('/api/admin/state'+query);if(!admin||epoch!==authEpoch||version!==refreshVersion)return;if(tab==='sessions'&&$('#modal').open)return;state=next;sessionsPage=next.sessionsPagination?.page??sessionsPage;sessionsPageSize=next.sessionsPagination?.pageSize??sessionsPageSize;sessionsLoading=false;$('#connection').textContent='● 服务在线';if(renderPage)render();}
  catch(e){if(!admin||epoch!==authEpoch||version!==refreshVersion)return;sessionsLoading=false;sessionsPage=state?.sessionsPagination?.page??1;sessionsPageSize=state?.sessionsPagination?.pageSize??25;$('#connection').textContent='连接异常';if(!state){showLogin();}else if(renderPage)render();toast(e.message);}
}
function showLogin(){if($('#modal').open)close();sessionsPage=1;sessionsPageSize=25;sessionsLoading=false;testKey='';chat=[];chatSession='';chatOwner='';draft='';$('#modal-content').innerHTML='';$('#content').innerHTML='';$('#login').classList.remove('hidden');$('#shell').classList.add('hidden');}
function showShell(){$('#login').classList.add('hidden');$('#shell').classList.remove('hidden');}
function setTab(next){if(sending)return toast('请等待当前请求完成后再切换页面');if(!admin&&next!=='playground'){toast('请先进入管理后台');return;}tab=next;render();}
function memberTable(rows){return rows.length?`<div class="table-wrap"><table><thead><tr><th>员工</th><th>近7天来源 IP</th><th>最近使用</th><th>状态</th><th>管理</th></tr></thead><tbody>${rows.map(m=>`<tr><td><div class="member-cell"><span class="avatar">${esc(m.name.slice(0,1))}</span><strong>${esc(m.name)}</strong></div></td><td>${button('ips',fmt(m.ips)+' 个 IP','table-button',m.id)}</td><td>${date(m.last_seen)}</td><td>${m.enabled?'<span class="badge">启用</span>':'<span class="badge off">已停用</span>'}</td><td>${button('member-detail','管理','table-button',m.id)}</td></tr>`).join('')}</tbody></table></div>`:empty('还没有员工。创建第一个 Key，开始使用。');}
function requestTable(rows){return rows.length?`<div class="table-wrap"><table><thead><tr><th>时间 / 员工</th><th>账号</th><th>输入 / 输出（亿 Token）</th><th>近7天来源 IP</th><th>耗时</th><th>状态</th></tr></thead><tbody>${rows.map(r=>`<tr><td>${date(r.started)}<small>${esc(r.member_name)}</small></td><td>${esc(r.account_name||'—')}</td><td>${tokenBillions(r.input)} / ${tokenBillions(r.output)}<small>其中缓存 ${tokenBillions(r.cached)}</small></td><td class="mono">${esc(r.ip)}</td><td>${r.ended?((r.ended-r.started)/1000).toFixed(1)+'s':'—'}</td><td>${button('request-detail',badge(r.status),'table-button',r.id)}</td></tr>`).join('')}</tbody></table></div>`:empty('暂无请求记录。测试一次请求后，数据会显示在这里。');}
function usageRows(){return (state.keys||[]).map(k=>({...k,member_name:state.members.find(m=>m.id===k.member_id)?.name||'已移除员工',member_enabled:state.members.find(m=>m.id===k.member_id)?.enabled}));}
function keyDisplay(k){return k.full_key?`<span class="key-value">${esc(k.full_key)}</span>${button('copy-key','复制','table-button',k.id)}`:`<span>${esc(k.prefix)}…</span><small>旧 Key 未存全文，下次成功调用后补全</small>`;}
async function copyKey(value){
  if(navigator.clipboard?.writeText){await navigator.clipboard.writeText(value);return;}
  const input=document.createElement('textarea');input.value=value;input.setAttribute('readonly','');input.className='clipboard-fallback';document.body.append(input);input.select();
  try{if(!document.execCommand('copy'))throw Error('复制失败，请手动选择完整 Key 复制');}finally{input.remove();}
}
function recentUsageTitle(){return '最近用量 <button type="button" class="usage-help table-button" data-action="usage-help" aria-label="最近用量说明" title="查看统计口径与漏计说明">?</button>';}
function usageTable(rows){return rows.length?`<div class="table-wrap"><table><thead><tr><th>员工</th><th>Key</th><th>近7天 Token（亿）</th><th>近12小时 Token（亿）</th><th>状态</th></tr></thead><tbody>${rows.map(k=>`<tr><td>${esc(k.member_name)}</td><td class="mono key-cell">${keyDisplay(k)}</td><td>${tokenBillions(k.used)}</td><td>${tokenBillions(k.used_12h)}</td><td>${!k.enabled||!k.member_enabled?'已停用':k.expires&&k.expires<=Date.now()?'已过期':'可用'}</td></tr>`).join('')}</tbody></table></div>`:empty('暂无 Key 用量。');}
function sessionPager(){
  const p=state.sessionsPagination||{page:sessionsPage,pageSize:sessionsPageSize,total:state.sessions.length,totalPages:1};
  const control=(label,page,disabled)=>`<button type="button" class="secondary" data-action="sessions-page" data-id="${page}" ${disabled||sessionsLoading?'disabled':''}>${label}</button>`;
  return `<div class="session-pager" aria-label="会话分页"><span class="hint">共 ${fmt(p.total)} 个会话 · 第 ${fmt(p.page)} / ${fmt(p.totalPages)} 页${sessionsLoading?' · 正在读取…':''}</span><div class="actions"><label>每页<select id="session-page-size" aria-label="每页会话数" ${sessionsLoading?'disabled':''}>${[25,50,100].map(n=>`<option value="${n}" ${n===p.pageSize?'selected':''}>${n} 条</option>`).join('')}</select></label>${control('首页',1,p.page<=1)}${control('上一页',p.page-1,p.page<=1)}${control('下一页',p.page+1,p.page>=p.totalPages)}${control('末页',p.totalPages,p.page>=p.totalPages)}</div></div>`;
}
function render(){
  if(!admin&&tab!=='playground'){showLogin();return;}
  showShell();document.querySelectorAll('[data-tab]').forEach(el=>el.classList.toggle('active',el.dataset.tab===tab));$('#crumb').textContent=labels[tab];
  if(!state&&tab!=='playground'){$('#content').innerHTML=empty('正在读取工作空间…');return;}
  let html='';
  if(tab==='overview'){
    const total=state.totals,active=state.accounts.filter(a=>a.enabled&&a.state==='ready').length;
    const stats=[['近7天 Token（亿）',tokenBillions(total.tokens),'输入与输出相加，缓存不重复计入'],['近7天请求总数',fmt(total.requests),`成功 ${fmt(total.success)} 次`],['可用账号',`${active} <span class="muted">/ ${state.accounts.length}</span>`,'Codex 多会话并行转发'],['员工',fmt(state.members.length),`处理中 ${state.busy} · 排队 ${state.queue}`]];
    const days=Array.from({length:7},(_,i)=>{const d=new Date((state.usageWindow?.start??Date.now()-6*86400000)+i*86400000),day=d.toLocaleDateString('sv-SE',{timeZone:'Asia/Shanghai'});return {day,tokens:state.daily.find(x=>x.day===day)?.tokens||0};});
    html=heading('WORKSPACE OVERVIEW','一切用量，清晰可见。','管理订阅连接与员工密钥；7天用量按北京时间自然日统计，12小时用量为滚动窗口。',button('create-member','＋ 创建员工 Key','primary'))+`<div class="stats">${stats.map(([label,value,hint])=>`<div class="stat"><div class="stat-label">${label}</div><div class="stat-value">${value}</div><small>${hint}</small></div>`).join('')}</div>`;
    html+=`<div class="two-col">${panel('最近 7 天用量',`<div class="panel-body"><div class="chart">${days.map(d=>`<div class="bar-col"><span class="chart-label">${tokenBillions(d.tokens)}</span><div class="bar" data-height="${Math.max(3,d.tokens/Math.max(1,...days.map(x=>x.tokens))*100)}"></div><small>${d.day.slice(5).replace('-','/')}</small></div>`).join('')}</div></div>`,'<span class="hint">北京时间 · 亿 Token</span>')}${panel('订阅连接',`<div class="panel-body">${state.accounts.length?state.accounts.map(a=>`<div class="mini-row"><div><strong>${esc(a.name)}</strong><small class="muted"> · ${esc(a.kind==='claude'?'Claude':'Codex')}</small></div>${a.enabled?badge(a.state):badge('offline')}</div>`).join(''):empty('尚未添加账号')}<p class="hint">平台近7天用量来自上游响应；订阅剩余量请以官方页面为准。</p></div>`,button('go-accounts','管理 →','text-button'))}</div>`;
    html+=panel(recentUsageTitle(),usageTable(usageRows()),button('go-requests','查看全部 →','text-button'));
  }
  if(tab==='members')html=heading('PEOPLE & ACCESS','员工与密钥','管理 Key 的有效期、启停和删除；近7天用量见使用记录。',button('create-member','＋ 创建员工 Key','primary'))+panel('员工列表',memberTable(state.members),`<span class="hint">${state.members.length} 位员工</span>`);
  if(tab==='accounts')html=heading('SUBSCRIPTION POOL','订阅账号池','Codex 新会话按生效权重分配，已有正权重绑定保持；不同会话并行。',`<div class="actions">${button('routing-weights','临时设置权重','secondary')}${button('balancing-settings','负载均衡','secondary')}${button('refresh-quotas','↻ 刷新额度','secondary')}${button('create-account','＋ 添加账号','primary')}</div>`)+`<div class="notice">Codex 直接并行转发，不按本地预测 Token 阻止请求。自动权重在每日02:00、额度耗尽或恢复时更新，也可临时手动设置；0权重不接收后续请求，全部为0时暂停转发。Claude 仍为旧文本兼容入口，尚待真实验收。</div><div class="cards">${state.accounts.map(a=>`<article class="account-card"><div class="account-top"><span class="account-logo">C</span>${a.enabled?badge(a.state):'<span class="badge off">已停用</span>'}</div><h2>${esc(a.name)}</h2><small>${esc(a.kind==='claude'?'Claude 订阅 · 待真实验证':'Codex 订阅')}</small>${quotaCard(a)}${routingCard(a)}${a.route_block?`<div class="error-line">${a.route_block==='quota_exhausted'?'订阅额度耗尽 · 自动权重为0，可临时设置正权重使用额外额度':'登录失效 · 暂停接收请求'}</div>`:''}<p>最近调用：${date(a.last_used)}<br>近7天消耗：${tokenBillions(a.budget_used)} 亿 Token</p>${a.last_error||a.quota_error?`<div class="error-line">${esc(a.last_error||a.quota_error)}</div>`:''}<div class="actions">${button('check-account','检查登录','secondary',a.id)}${button('login-account',a.state==='offline'?'登录账号':'重新登录（重建会话）','text-button',a.id)}${button('toggle-account',a.enabled?'停用':'启用','text-button',a.id)}${button('remove-account','移除账号','text-button danger',a.id)}</div></article>`).join('')}<div class="account-card add">${button('create-account','＋ 添加一个独立账号','big-add')}</div></div>`;
  if(tab==='sessions')html=heading('SESSION AFFINITY','会话绑定','新会话自动分配账号，已有会话保持绑定；需要时可手动修改。')+`<div class="notice">手动切换从下一次请求生效；正在处理的会话须等待完成。禁止访问仅阻止该会话的新请求，不中断已经开始的请求。</div>`+panel('近7天活动会话',(state.sessions.length?`<div class="table-wrap"><table><thead><tr><th>Codex Session / 员工</th><th>绑定账号</th><th>近7天请求数</th><th>最近活动</th><th>操作</th></tr></thead><tbody>${state.sessions.map(s=>`<tr><td class="mono">${esc(sessionDisplayId(s))}<small>${esc(s.member_name)}</small></td><td>${esc(s.account_name)}</td><td>${fmt(s.request_count)}</td><td>${date(s.updated)}</td><td>${button('session-actions','管理会话','table-button',s.id,sessionsLoading)}</td></tr>`).join('')}</tbody></table></div>`:empty('发送第一条请求后自动创建会话。'))+sessionPager());
  if(tab==='requests'){
    const rows=usageRows().filter(k=>!requestFilter||k.member_name.includes(requestFilter)||(k.full_key||k.prefix).includes(requestFilter));
    html=heading('CUMULATIVE USAGE','使用记录','同时展示近7个北京时间自然日与滚动近12小时用量；输入加输出，缓存不重复计算。')+`<div class="toolbar"><input id="request-filter" placeholder="搜索员工或 Key" value="${esc(requestFilter)}" aria-label="搜索员工或 Key"></div>`+panel(recentUsageTitle(),usageTable(rows));
  }
  if(tab==='playground'){
    html=heading('PLAYGROUND','发送第一条请求','用员工 Key 验证鉴权、订阅调用与额度统计。')+`<div class="chat-layout"><section class="panel"><div class="chat-controls"><h2>连接设置</h2><label>员工 API Key<input id="test-key" type="password" autocomplete="off" placeholder="hg_…" value="${esc(testKey)}" ${sending?'disabled':''}></label><button class="secondary" id="check-key">查看近7天用量</button><div id="my-quota" class="details-line"></div><label>模型<select id="chat-model" ${sending?'disabled':''}><option value="codex" ${chatModel==='codex'?'selected':''}>Codex</option><option value="claude" ${chatModel==='claude'?'selected':''}>Claude</option></select></label><p class="hint">同一对话持续使用同一个上游 session。新会话才重新分配账号。</p><div class="notice">纯文本模式<br>不执行代码或访问工作文件。</div>${button('clear-chat','＋ 新建会话','text-button')}<p class="hint mono">${chatSession?esc(chatSession):'尚未创建会话'}</p><p class="hint">页面刷新会清空显示。上游上下文保存在本机独立 Codex 会话目录，可通过 session_id 继续。</p></div></section><section class="panel"><div class="panel-heading"><h2>对话测试</h2><span class="hint">${chatSession?'会话已绑定':'新会话'}</span></div><div class="chat-log" id="chat-log">${renderChat()}</div><form class="composer" id="chat-form"><textarea id="prompt" placeholder="例如：用一句话解释什么是 API 网关" aria-label="消息" required ${sending?'disabled':''}>${esc(draft)}</textarea><div class="composer-bottom"><span class="hint">${sending?'正在等待回复，最长约 2 分钟…':'每次请求会消耗真实订阅额度'}</span><button class="primary" ${sending?'disabled':''}>${sending?'处理中…':'发送请求 ↗'}</button></div></form></section></div>`;
  }
  if(tab==='guide')html=heading('CONNECTION GUIDE','简单接入，随时掌握。','使用员工 Key 从 Windows、WSL 或其他机器连接网关。')+`<div class="guide-grid">${panel('纯文本聊天配置',`<div class="panel-body"><label>Base URL</label><div class="code">${esc(state.endpoint)}</div><label>API Key</label><div class="code">员工 Key（hg_ 开头）</div><label>模型</label><div class="code">codex</div><p class="hint">此处是网页聊天使用的旧文本接口。Codex Windows / WSL 使用下方 Responses 配置，支持增量流和客户端工具调用。</p></div>`)}${panel('运行与数据',`<div class="panel-body"><ul class="guide-list"><li>客户端使用本页 Base URL 连接；订阅登录保存在网关服务器。</li><li>每账号并发 1，同员工并发 1，最多排队 5 个请求。</li><li>到期、停用或额度耗尽的 Key 会被拒绝。</li><li>来源 IP 使用实际连接地址，不信任客户端伪造的转发头。</li><li>异常用量未知时保留预占额度，管理员核对后释放。</li><li>SQLite 保存统计；官方凭证在独立 runtime 目录中。</li></ul>${button('backup','创建数据库备份','secondary')}</div>`)}</div>`+panel('请求示例',`<div class="panel-body"><pre class="code">POST /v1/chat/completions
Authorization: Bearer hg_你的员工Key
Content-Type: application/json

{
  "model": "codex",
  "messages": [{"role": "user", "content": "你好"}],
  "stream": false
}</pre><p class="hint">Token 总量 = 输入 + 输出；缓存 Token 已包含在输入中。不支持 max_tokens 硬上限，也不把本地 Token 配额换算成订阅剩余额度。</p></div>`);
  if(tab==='guide')html+=panel('保持同一个会话',`<div class="panel-body"><p class="hint">第一条请求返回 session_id。后续请求带上该值，只发送本轮新消息；不提供 session_id 就会创建新会话、重新选择账号。</p><pre class="code">{
  "model": "codex",
  "session_id": "上一条响应中的会话ID",
  "messages": [{"role": "user", "content": "继续上一个问题"}]
}</pre><p class="hint">也可使用 X-Harbor-Session 请求头。完整历史会校验后去重；不同员工无法访问同一个会话。账号不足时暂停，不自动切换。上游会话正文保存在独立 Codex 目录，平台统计库只保存映射与摘要。</p></div>`);
  if(tab==='guide')html+=panel('Codex Windows / WSL 接入',`<div class="panel-body"><p>Windows：%USERPROFILE%&#92;.codex&#92;config.toml；WSL：~/.codex/config.toml。合并以下配置，顶层键放在第一个表头之前，不覆盖已有配置。</p><pre class="code">model = "gpt-6.1-sol"
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
  if(tab==='guide')html+=panel('image2 图片生成与编辑（测试接入）',imageGuide(state.endpoint));
  $('#content').innerHTML=html;document.querySelectorAll('[data-height]').forEach(el=>el.style.height=el.dataset.height+'px');document.querySelectorAll('[data-width]').forEach(el=>el.style.width=el.dataset.width+'%');
  if(tab==='playground'){$('#chat-form').onsubmit=sendChat;$('#check-key').onclick=checkKey;$('#test-key').oninput=e=>{testKey=e.target.value;$('#my-quota').textContent='';};$('#prompt').oninput=e=>draft=e.target.value;$('#chat-model').onchange=e=>{if(sending)return;chatModel=e.target.value;chat=[];chatSession='';chatOwner='';render();};$('#chat-log').scrollTop=$('#chat-log').scrollHeight;}
  if(tab==='sessions'){$('#session-page-size').onchange=async e=>{if(sessionsLoading)return;sessionsPageSize=Number(e.target.value);sessionsPage=1;sessionsLoading=true;render();await refresh();};}
  if(tab==='requests'){$('#request-filter').onchange=e=>{requestFilter=e.target.value;render();};}
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
function modal(html){if(!admin)return;if($('#modal').open)close();$('#modal-content').innerHTML=html;$('#modal').showModal();}
function close(){clearTimeout(loginPoll);loginAccount='';loginView++;$('#modal').close();$('#modal-content').innerHTML='';}
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
function quotaValue(f){const v=f.get('quota');return v===''?null:Number(v);}
function showSecret(secret){if(!admin)return;close();modal(`<h2>Key 已生成</h2><p class="hint">Key 已加密保存，可在运行总览或使用记录中完整查看和复制。</p><div class="secret" id="new-secret"></div><div class="dialog-actions">${button('close','完成','secondary')}${button('use-secret','去请求测试','secondary')}${button('copy-secret','复制 Key','primary')}</div>`);$('#new-secret').textContent=secret;}
function sessionDisplayId(s){return s.protocol==='responses'?(s.client_session_id||'未获取'):'旧 CLI 会话';}
function sessionStatus(s){return !s.enabled?'已禁止访问':s.state==='active'?'已绑定':s.state==='uncertain'?(s.protocol==='responses'?'可继续 · 用量可能漏计':'需新建会话'):'待首次请求';}
function sessionDetail(identifier){
  const s=state.sessions.find(x=>x.id===identifier);if(!s)return toast('会话不在当前列表，请刷新后重试');
  modal(`<h2>管理会话</h2><p class="mono muted">${esc(sessionDisplayId(s))}</p><div class="details-line">员工：${esc(s.member_name)}<br>当前状态：${sessionStatus(s)}<br>绑定账号：${esc(s.account_name)}<br>自动调整：${s.nightly_enabled?'已参与':'临时固定至 '+beijingDate(s.nightly_expires)+'（北京时间02:00）'}</div><div class="actions">${s.protocol==='responses'&&s.enabled?button('route-session','修改绑定账号','secondary',s.id):''}${s.protocol==='responses'?button('nightly-session',s.nightly_enabled?'临时固定至02:00':'恢复自动调整','secondary',s.id):''}${button('toggle-session',s.enabled?'禁止该会话访问':'恢复该会话访问',s.enabled?'text-button danger':'secondary',s.id)}</div>${s.protocol!=='responses'?'<p class="hint">旧 CLI Session 的上下文保存在原账号目录，暂不支持修改绑定账号。</p>':''}<div class="dialog-actions">${button('close','关闭','primary')}</div>`);
}
function memberDetail(memberId){const m=state.members.find(x=>x.id===memberId),keys=state.keys.filter(k=>k.member_id===memberId);modal(`<h2>${esc(m.name)}</h2><div class="actions">${button('edit-member','修改名称','secondary',m.id)}${button('new-key','新增 Key','secondary',m.id)}${button('toggle-member',m.enabled?'停用员工':'启用员工','text-button',m.id)}</div>${keys.map(k=>`<div class="mini-row"><div class="mono">${esc(k.prefix)}…<small>${k.expires?'到期 '+date(k.expires):'永久有效'} · ${!k.enabled?'已停用':!m.enabled?'员工已停用':k.expires&&k.expires<=Date.now()?'已过期':'可用'}</small></div><div>${button('key-ips',fmt(k.ips)+' 个 IP','table-button',k.id)}${button('renew-key','续期','table-button',k.id)}${button('toggle-key',k.enabled?'停用':'启用','table-button',k.id)}${button('remove-key','删除','table-button',k.id)}</div></div>`).join('')}<p class="hint">近7天用量见使用记录。换 Key 不影响其他 Key 的统计。</p><div class="dialog-actions">${button('close','完成','primary')}</div>`);}
async function checkKey(){testKey=$('#test-key').value.trim();const key=testKey,epoch=authEpoch;try{const m=await api('/api/me','GET',undefined,key);if(epoch!==authEpoch||tab!=='playground'||$('#test-key')?.value.trim()!==key)return;$('#my-quota').textContent=`${m.name}：此 Key 近7天 ${tokenBillions(m.keyUsed)} 亿 Token`;}catch(e){if(epoch===authEpoch)toast(e.message);}}
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
    chatSession=r.session_id;chat.push({role:'assistant',content:r.choices[0].message.content});toast('完成 · '+tokenBillions(r.usage.total_tokens)+' 亿 Token');
  }catch(err){if(epoch===authEpoch){if(appended)chat.pop();draft=content;toast(err.message+'；输入已保留');}}
  finally{sending=false;if(admin&&epoch===authEpoch)await refresh(false);if(tab==='playground'&&epoch===authEpoch)render();}
}

document.addEventListener('click',async e=>{
  const nav=e.target.closest('[data-tab]');if(nav)return setTab(nav.dataset.tab);
  const el=e.target.closest('[data-action]');if(!el)return;const action=el.dataset.action,identifier=el.dataset.id;
  try{
    if(action==='usage-help')return modal(`<h2>最近用量说明</h2><p>近7天按北京时间今天及前6天统计；近12小时为服务端当前时刻向前滚动12小时，按请求完成时间统计。输入与输出相加，缓存不重复计入。</p><p>上游未返回用量或请求中断时可能漏计，当前数据仅供预估，不等于官方订阅额度消耗，也不包含未经过平台的调用。</p><div class="dialog-actions">${button('close','关闭','primary')}</div>`);
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
    if(action==='use-secret'){testKey=$('#new-secret').textContent;close();setTab('playground');return;}
    if(action==='ips'||action==='key-ips'){const key=action==='key-ips'?state.keys.find(k=>k.id===identifier):null;const rows=await api('/api/admin/ips?member='+(key?key.member_id:identifier)+(key?'&key='+key.id:''));close();return modal(`<h2>${key?'此 Key 的':'员工全部 Key 的'}来源 IP</h2><p class="hint">IP 数量不等于设备数。本机测试通常显示 127.0.0.1。</p>${rows.length?rows.map(r=>`<div class="mini-row"><div><strong class="mono">${esc(r.ip)}</strong><small>首次 ${date(r.first_seen)}<br>最近 ${date(r.last_seen)}<br>${esc(r.agent)}</small></div><span>${r.requests} 次</span></div>`).join(''):empty('暂无记录')}<div class="dialog-actions">${button('close','关闭','primary')}</div>`);}
    if(action==='routing-weights'){
      const w=state.balancing?.weights;
      if(!w)return toast('请先刷新账号状态');
      const accounts=state.accounts.filter(a=>a.kind==='codex');
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
      return formModal('负载均衡','自动权重在每日02:00及额度耗尽、恢复时更新，普通余量刷新不改比例。手动设置保持至02:00；额度切换只选正权重账号，不重放失败请求。',`<label>按24小时额度预算分配<select name="weighted"><option value="false" ${!b.weighted?'selected':''}>关闭（原策略）</option><option value="true" ${b.weighted?'selected':''}>开启</option></select></label><label>登录失效自动切换<select name="failover"><option value="false" ${!b.failover?'selected':''}>关闭</option><option value="true" ${b.failover?'selected':''}>开启</option></select></label><label>北京时间每日 02:00 调整<select name="nightly"><option value="false" ${!b.nightly?'selected':''}>关闭</option><option value="true" ${b.nightly?'selected':''}>开启</option></select></label><label>静默期（分钟）<input name="quietMinutes" type="number" min="1" max="960" value="${b.quietMinutes}" required></label><p class="hint">统计过去16小时活跃会话，整组负载偏差允许5个百分点；超出时优先迁移过载账号的重会话，再尽量减少迁移数量。同等方案优先让额度预算更高的账号承担偏差。关闭夜间调整仍在02:00恢复自动权重。手动换绑和临时排除均只保持至下次北京时间02:00，届时恢复自动参与；在途保护不变。</p><p class="hint">最近执行：${last?esc(last.day)+' · '+esc(({completed:'已完成',failed:'失败',interrupted:'已中断',running:'执行中'})[last.status]||last.status):'尚未执行'}</p>${button('balancing-preview','预览分配（不修改绑定）','secondary')}`,async f=>{await api('/api/admin/balancing','PATCH',{weighted:f.get('weighted')==='true',failover:f.get('failover')==='true',nightly:f.get('nightly')==='true',quietMinutes:Number(f.get('quietMinutes'))});close();await refresh();toast('负载均衡设置已保存');});
    }
    if(action==='balancing-preview'){
      el.disabled=true;const epoch=authEpoch,result=await api('/api/admin/balancing/preview','POST',{});if(epoch!==authEpoch||!admin)return;
      const accountName=id=>state.accounts.find(a=>a.id===id)?.name||id;
      return modal(`<h2>夜间分配预览</h2><p class="hint">按当前额度和自动权重估计，临时手动权重不参与夜间计划。仅预览，不会修改绑定。窗口结束：${date(result.window.end)}。预计迁移 ${result.moves.length} 个会话，跳过 ${result.skipped.length} 项。</p>${result.summaries.map(g=>`${planningHint(g.planning)}<div class="table-wrap"><table><thead><tr><th>账号</th><th>当前负载</th><th>固定负载</th><th>分配后</th></tr></thead><tbody>${g.accounts.map(a=>`<tr><td>${esc(accountName(a))}</td><td>${fmt(g.before[a])}</td><td>${fmt(g.fixedLoad[a])}</td><td>${fmt(g.after[a])}</td></tr>`).join('')}</tbody></table></div>`).join('')}<p class="hint">负载为历史用量及在途估计，仅用于相对分配，不代表官方额度。</p>${result.moves.length?result.moves.map(m=>`<div class="mini-row"><span>${esc(sessionDisplayId(m))}</span><span>${esc(accountName(m.from))} → ${esc(accountName(m.to))}</span></div>`).join(''):empty('当前没有需要迁移的会话')}<p class="hint">忙碌、静默期内、手动排除、额度不可比或样本不足均可能跳过。长任务已计入原账号负载。</p><div class="dialog-actions">${button('close','关闭','primary')}</div>`);
    }
    if(action==='nightly-session'){const s=state.sessions.find(x=>x.id===identifier);await api('/api/admin/sessions/'+identifier,'PATCH',{nightly_enabled:!s.nightly_enabled});close();await refresh();return toast('夜间调整设置已更新');}
    if(action==='sessions-page'){if(sessionsLoading)return;sessionsPage=Number(identifier);sessionsLoading=true;render();return refresh();}
    if(action==='session-actions'){if(sessionsLoading)return;return sessionDetail(identifier);}
    if(action==='route-session'){
      const s=state.sessions.find(x=>x.id===identifier),accounts=state.accounts.filter(a=>a.kind==='codex'&&a.enabled&&a.state==='ready'&&!a.route_block);
      if(!accounts.some(a=>a.id!==s.account_id))return toast('暂无其他已连接的 Codex 账号');
      return formModal('修改绑定账号','从下一次请求起使用选定账号，临时固定至下次北京时间02:00；届时自动规则接管。不会重发失败请求；客户端须携带完整历史。',`<p class="hint">当前绑定：${esc(s.account_name)}</p><label>目标账号<select name="account_id">${accounts.map(a=>`<option value="${esc(a.id)}" ${a.id===s.account_id?'selected':''}>${esc(a.name)}</option>`).join('')}</select></label>`,async f=>{await api('/api/admin/sessions/'+identifier,'PATCH',{account_id:f.get('account_id'),expected_route_version:s.route_version});close();await refresh();toast('绑定已更新，下次请求生效');});
    }
    if(action==='toggle-session'){
      const s=state.sessions.find(x=>x.id===identifier);
      return modal(`<h2>${s.enabled?'禁止该会话访问？':'恢复该会话访问？'}</h2><p class="hint">${s.enabled?'该会话后续请求将被拒绝，已经开始的请求允许完成。其他会话与员工 Key 不受影响。':'恢复后继续使用当前绑定账号，不自动切换账号。'}</p><div class="dialog-actions">${button('close','取消','secondary')}${button(s.enabled?'disable-session':'enable-session',s.enabled?'确认禁止':'确认恢复','primary',identifier)}</div>`);
    }
    if(action==='disable-session'||action==='enable-session'){await api('/api/admin/sessions/'+identifier,'PATCH',{enabled:action==='enable-session'});close();await refresh();return toast(action==='enable-session'?'会话访问已恢复':'该会话已禁止访问');}
    if(action==='create-account')return formModal('添加订阅账号','创建独立登录目录后，可使用官方登录。重新登录会结束该账号已有会话绑定，后续须新建会话。','<label>账号类型<select name="kind"><option value="codex">Codex 订阅</option><option value="claude">Claude 订阅（待真实验证）</option></select></label><label>账号名称<input name="name" required maxlength="60" placeholder="例如：测试账号 02"></label>',async f=>{await api('/api/admin/accounts','POST',{name:f.get('name'),kind:f.get('kind')});close();await refresh();toast('账号已创建，请完成官方登录');});
    if(action==='refresh-quotas'){el.disabled=true;await api('/api/admin/quotas/refresh','POST',{});await refresh();return toast(state.accounts.some(a=>a.enabled&&a.quota_error)?'部分账号额度未能刷新，已保留上次结果':'官方额度已更新');}
    if(action==='budget-account'){const a=state.accounts.find(x=>x.id===identifier);return formModal('本地 Token 预算','这是手动估算预算，不是官方订阅额度。已用 '+fmt(a.budget_used)+' Token；预算留空表示不设置本地上限。',`<label>总预算（Token）<input name="quota" type="number" min="0" step="1" value="${a.token_budget??''}" placeholder="留空"></label>${a.kind!=='claude'?`<label>每 1% 额度的预计 Token 容量<input name="capacity" type="number" min="100" max="10000000" step="1" value="${Math.round(a.tokens_per_percent||10000)}"></label><small>仅用于预测排序；会随官方额度变化校准，不是官方余额。</small>`:''}`,async f=>{await api('/api/admin/accounts/'+identifier,'PATCH',{token_budget:quotaValue(f),...(a.kind!=='claude'?{tokens_per_percent:Number(f.get('capacity'))}:{})});close();await refresh();toast('预算已保存');});}
    if(action==='toggle-account'){const a=state.accounts.find(x=>x.id===identifier);await api('/api/admin/accounts/'+identifier,'PATCH',{enabled:!a.enabled});await refresh();return;}
    if(action==='login-account'){el.disabled=true;return await loginAccountFlow(identifier);}
    if(action==='cancel-login'){el.disabled=true;await api('/api/admin/accounts/'+identifier+'/login/cancel','POST',{});close();await refresh();return toast('登录已取消，可重新登录');}
    if(action==='check-account'){el.disabled=true;const r=await api('/api/admin/accounts/'+identifier+'/check','POST',{});await refresh();return toast(r.ready?'已检测到订阅登录':'未检测到订阅登录');}
    if(action==='remove-account'){const account=state.accounts.find(x=>x.id===identifier);return modal(`<h2>移除 ${esc(account.name)}？</h2><p class="hint">移除后不再使用此账号，正在登录的流程会取消，绑定会话需新建。历史用量、会话记录和独立目录保留；不会删除其他账号。正在处理或排队的请求须先完成。</p><div class="dialog-actions">${button('close','取消','secondary')}${button('confirm-remove-account','确认移除','primary',identifier)}</div>`);}
    if(action==='confirm-remove-account'){el.disabled=true;await api('/api/admin/accounts/'+identifier,'DELETE');close();await refresh();return toast('账号已移除，历史用量保留');}
    if(action==='request-detail'){const r=state.requests.find(x=>x.id===identifier);return modal(`<h2>请求详情</h2><p class="mono muted">${esc(r.id)}</p><div class="details-line">员工：${esc(r.member_name)}<br>时间：${date(r.started)}<br>Key：${esc(r.key_prefix||state.keys.find(k=>k.id===r.key_id)?.prefix||'—')}…${r.key_removed_at?'（已删除）':''}<br>来源：${esc(r.ip)}<br>客户端：${esc(r.agent)}<br>输入 / 输出：${tokenBillions(r.input)} / ${tokenBillions(r.output)}<br>预占：${tokenBillions(r.reserved)}</div>${r.error?`<div class="notice warn">${esc(r.error)}</div>`:''}<div class="dialog-actions">${button('close','关闭','secondary')}${['unknown','interrupted'].includes(r.status)?button('settle','核对用量','primary',r.id):''}</div>`);}
    if(action==='settle'){close();return formModal('核对异常请求','核实上游实际用量后填写需要补记的 Token。填 0 只释放预占；此操作会记入审计记录。','<label>补记 Token<input name="tokens" type="number" min="0" step="1" required></label>',async f=>{await api('/api/admin/requests/'+identifier+'/settle','POST',{tokens:Number(f.get('tokens'))});close();await refresh();toast('已补记用量并释放预占');});}
    if(action==='clear-chat'){if(sending)return toast('请等待当前请求完成');chat=[];chatSession='';render();return;}
    if(action==='backup'){const r=await api('/api/admin/backup','POST',{});return toast('备份已保存：'+r.file);}
    if(action==='export'){const epoch=authEpoch,r=await fetch('/api/admin/export');if(!r.ok)throw Error('导出失败');const blob=await r.blob();if(!admin||epoch!==authEpoch)return;const url=URL.createObjectURL(blob),a=document.createElement('a');a.href=url;a.download='Harbor-使用记录.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);return;}
  }catch(err){toast(err.message);}finally{el.disabled=false;}
});
$('#login-form').onsubmit=async e=>{e.preventDefault();const submit=e.submitter;submit.disabled=true;try{await api('/api/auth/login','POST',{password:$('#admin-input').value});authEpoch++;admin=true;$('#admin-input').value='';await refresh();}catch(err){toast(err.message);}finally{submit.disabled=false;}};
$('#employee-entry').onclick=()=>{tab='playground';render();};$('#refresh').onclick=()=>refresh();$('#logout').onclick=async()=>{if(sending)return toast('请等待当前请求完成后退出');try{await api('/api/auth/logout','POST',{});authEpoch++;admin=false;state=null;testKey='';chat=[];chatSession='';chatOwner='';draft='';showLogin();}catch(err){toast(err.message);}};
setInterval(()=>{if(admin&&!sessionsLoading&&tab!=='playground'&&!$('#modal').open&&!document.hidden&&document.activeElement.tagName!=='INPUT')refresh();},12000);
const initialAuthEpoch=authEpoch;api('/api/auth/session').then(r=>{if(initialAuthEpoch!==authEpoch)return;admin=r.authenticated;if(admin)refresh();else showLogin();}).catch(()=>{if(initialAuthEpoch===authEpoch)showLogin();});
