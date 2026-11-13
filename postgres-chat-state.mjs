import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {AppError,id,usageWindow} from './store.mjs';
import {accountCapacity} from './rates.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {createPostgresExtraRuntime} from './postgres-extra-runtime.mjs';
import {accountPackageBudget} from './account-package-budget.mjs';
import {createPostgresRouter,lockPostgresRouting} from './postgres-routing.mjs';
import {digestChatMessages,validateLegacyChat} from './chat-facts.mjs';

const fail=(status,message,code)=>{throw new AppError(status,message,code);};
// Admission and dispatch are short DB transactions. Quota/credential reads and
// CLI execution must happen outside them; queued requests keep their reserves.
export function createPostgresChatState(store,{checkLimits,clock=Date.now,reserveTokens=32000,extraRuntime=createPostgresExtraRuntime(store,{clock})}={}){
  if(typeof checkLimits!=='function'||typeof clock!=='function'||!Number.isSafeInteger(reserveTokens)||reserveTokens<1)fail(503,'文本会话准入依赖无效','chat_configuration');
  const access=createRoutingDepartments(store,{clock}),packages=createPostgresAccountPackages(store,{clock}),router=createPostgresRouter(store,{clock,extraRuntime});
  async function checkPackageScope(account,scope){if(account.kind!=='codex'||!(await packages.policy(store,{scope:scope??undefined})).enabled)return;const [decorated]=await packages.decorate([account]),budget=accountPackageBudget(decorated,{at:clock(),enabled:true,protocol:'chat',model:'codex'});if(budget.reason==='login_expired')fail(503,'账号登录已失效，请重新登录','session_account_unavailable');if(budget.reason==='package_scope_mismatch')fail(409,'套餐不适用于旧CLI文本通道；此通道不能推断实际模型，请使用明确支持chat和codex粗入口的配置','package_scope_mismatch');}
  async function checkManualWeight(account,scope){if(account.kind!=='codex')return;const view=await router.extraDispatchPreference({accountId:account.id,model:'codex',protocol:'chat',scope});if(view.manual&&!view.manualPositive)fail(429,'账号当前人工分配比例为0，请调整绑定或权重','session_weight_zero');}
  async function allowance(person,exclude='',{checkBusy=true}={}){
    const key=await store.get('SELECT quota,used FROM harbor.keys WHERE id=$1',person.key_id),pending=await store.get("SELECT COALESCE(SUM(reserved),0)::text member,COALESCE(SUM(reserved) FILTER(WHERE key_id=$2),0)::text key FROM harbor.requests WHERE member_id=$1 AND id<>$3 AND status IN ('queued','running','unknown','interrupted')",person.id,person.key_id,exclude);
    if(checkBusy&&await store.get("SELECT id FROM harbor.requests WHERE member_id=$1 AND id<>$2 AND status IN ('queued','running') LIMIT 1",person.id,exclude))fail(429,'该员工已有请求在处理，请稍后再试','member_busy');
    const remaining=(quota,used,held,code)=>{if(quota===null)return BigInt(reserveTokens);const value=BigInt(quota)-BigInt(used)-BigInt(held);if(value<=0n)fail(429,'额度已用完或被待核对请求占用',code);return value;};
    const values=[BigInt(reserveTokens),remaining(person.quota,person.used,pending.member,'quota_exceeded'),remaining(key.quota,key.used,pending.key,'key_quota_exceeded')];return Number(values.reduce((a,b)=>a<b?a:b));
  }
  async function estimate(messages,scope){
    const row=await store.get("SELECT AVG(CASE WHEN ended<$1 THEN 0 ELSE input+output END)::float8 n FROM (SELECT input,output,ended FROM harbor.requests WHERE status='ok'"+(scope?' AND department_id=$2':'')+" ORDER BY started DESC,query_sequence DESC LIMIT 10) recent",usageWindow(clock()).start,...(scope?[scope.department_id]:[]));
    return Math.ceil(Math.max(10000,row.n||10000,JSON.stringify(messages).length/2+9000));
  }
  async function capacities(accounts,predicted,exclude='',scope){
    const pending=new Map((await store.all("SELECT account_id,COALESCE(SUM(predicted),0)::text n FROM harbor.requests WHERE account_id=ANY($1::text[]) AND id<>$2 AND status IN ('queued','running','unknown','interrupted') GROUP BY account_id",accounts.map(a=>a.id),exclude)).map(r=>[r.account_id,Number(r.n)]));
    const packagePolicy=await packages.policy(store,{scope:scope??undefined}),usePackages=accounts.some(account=>account.kind==='codex')&&packagePolicy.enabled,decorated=usePackages?new Map((await packages.decorate(accounts.filter(account=>account.kind==='codex'))).map(account=>[account.id,account])):new Map(),classifications=await extraRuntime.classifyAccounts(accounts,{model:accounts[0]?.kind,protocol:'chat',scope});
    return accounts.map(account=>{let capacity=accountCapacity(account,predicted,pending.get(account.id)||0,clock());const configured=decorated.get(account.id),budget=configured?accountPackageBudget(configured,{at:clock(),enabled:true,protocol:'chat',model:'codex'}):null,classification=classifications.get(account.id),extra=classification.mode==='manual_extra';
      // Local token reserves remain independent of official absolute/extra units.
      if(classification.mode==='blocked'||classification.mode==='extra'&&!extra)capacity={eligible:false,score:-1,source:'official_exhausted',remaining:0};
      else if(extra||budget?.eligible&&budget.windows.some(window=>window.source==='official_absolute')){const local=accountCapacity({...account,quota_json:null},predicted,pending.get(account.id)||0,clock());capacity=account.token_budget!==null&&!local.eligible?local:{eligible:true,score:local.source==='local'?local.score:extra?local.score:budget.score,source:extra?'authorized_extra':'official_absolute',remaining:null};}
      return {account,capacity,extra,allocation:null};});
  }
  async function admit(secret,body,{headers={},ip='unknown',agent='unknown',recordingEnabled=true,authorizeDevice,scope:expectedScope}={}){
    const payload=validateLegacyChat(body),sessionId=payload.session_id||headers['x-harbor-session'];
    if(sessionId&&(typeof sessionId!=='string'||sessionId.length>80))fail(400,'session_id 格式错误','invalid_session');
    return store.transaction(async()=>{
      const at=clock(),person=await store.authenticate(secret,{at,lock:true,exclusive:true}),scope=await access.resolve(expectedScope===undefined?store.departmentScope(person):expectedScope,{member:person,write:true,employeeOnly:true}),device=await authorizeDevice?.(person);await store.checkModel(person,payload.model);const reserved=await allowance(person);await checkLimits(person,payload,{at});
      await lockPostgresRouting(store);const predicted=await estimate(payload.messages,scope);let session=sessionId?await store.get('SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 FOR UPDATE',sessionId,person.id):null;
      if(session)access.same(scope,session);
      if(sessionId&&!session)fail(404,'会话不存在或不属于该员工','session_not_found');
      if(session?.protocol!=='chat'&&session)fail(409,'会话协议不匹配','session_protocol_mismatch');
      if(session?.state==='uncertain')fail(409,'此会话的上次调用状态不确定，请创建新会话','session_uncertain');
      if(session&&!session.enabled)fail(403,'该会话已被禁止访问','session_disabled');
      const accounts=session?await store.all('SELECT * FROM harbor.accounts WHERE id=$1 FOR SHARE',session.account_id):await store.all("SELECT * FROM harbor.accounts WHERE kind=$1 AND auth_mode='subscription' AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded')"+(scope?' AND department_id=$2':'')+" ORDER BY id FOR SHARE",payload.model,...(scope?[scope.department_id]:[]));
      for(const account of accounts)access.same(scope,account);
      if(session&&accounts[0]?.kind!==payload.model)fail(409,'会话已绑定另一种模型，请创建新会话','session_provider_mismatch');
      if(session&&(!accounts[0]?.enabled||accounts[0].removed_at!==null||accounts[0].auth_mode!=='subscription'||!['ready','degraded'].includes(accounts[0].state)))fail(503,'会话绑定账号不可用；会话不会自动换号','session_account_unavailable');
      let candidates=(await capacities(accounts,predicted,'',scope)).filter(row=>row.capacity.eligible);if(!session&&payload.model!=='codex'){const normal=candidates.filter(row=>!row.extra);if(normal.length)candidates=normal;else if(candidates.length>1&&candidates.some(row=>row.allocation?.basis!=='reference_estimate'||row.allocation?.unit!=='reference_usd_atoms'))fail(503,'多个账号的额外额度单位不可比较，请明确配置分配方式','extra_budget_incomparable');}
      const ranked=candidates.sort((a,b)=>(b.capacity.score-a.capacity.score)||a.account.last_used-b.account.last_used||a.account.id.localeCompare(b.account.id));if(!ranked.length)fail(session?429:503,'没有预计额度足够的账号；请刷新官方额度或设置本地预算',session?'session_quota_exhausted':'no_account_capacity');let account;if(!session&&payload.model==='codex')account=await router.selectAccount({at,protocol:'chat',model:'codex',accountIds:new Set(ranked.map(row=>row.account.id)),legacyOrder:ranked.map(row=>row.account.id),scope});else account=ranked[0]?.account;
      if(!account)fail(session?429:503,'没有预计额度足够的账号；请刷新官方额度或设置本地预算',session?'session_quota_exhausted':'no_account_capacity');
      await checkManualWeight(account,scope);await checkPackageScope(account,scope);
      let delta=payload.messages;
      if(session?.message_count&&payload.messages.length>1){if(payload.messages.length<=session.message_count||digestChatMessages('',payload.messages.slice(0,session.message_count))!==session.history_digest)fail(409,'完整历史与会话不匹配；请只发送本轮新消息，或创建新会话','session_history_mismatch');delta=payload.messages.slice(session.message_count);}
      if(!session)session=await store.get("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,created,updated"+(scope?',department_id':'')+") VALUES($1,$2,$3,'chat',$4,$4"+(scope?',$5':'')+") RETURNING *",id(),person.id,account.id,at,...(scope?[scope.department_id]:[]));
      const requestId=await store.observe(person,ip,agent,payload.model,{at,channel:'chat',device,scope:scope??undefined});await store.run('UPDATE harbor.requests SET account_id=$1,session_id=$2,route_version=$3,tracking_only=0,reserved=$4,predicted=$5,recording_state=$6 WHERE id=$7',account.id,session.id,session.route_version,reserved,predicted,recordingEnabled?'pending':'disabled',requestId);
      const extraAdmission=await extraRuntime.admit({requestId,account,model:payload.model,protocol:'chat',scope:scope??undefined});return {requestId,person,account,session,delta,payload,predicted,reserved,extraAdmission,scope};
    });
  }
  async function dispatch(secret,requestId,{followDisabled=false,deferAuthorization=false,acquireAccount=()=>true,isCancelled=()=>false,authorizeDevice,scope:expectedScope}={}){
    if(typeof followDisabled!=='boolean'||typeof acquireAccount!=='function'||typeof isCancelled!=='function')fail(503,'文本派发配置无效','chat_configuration');
    const ready=await store.transaction(async()=>{
      // Same request→member→Key order as settlement, including queued cancel.
      const request=await store.get('SELECT * FROM harbor.requests WHERE id=$1 FOR UPDATE',requestId);if(!request||request.source_channel!=='chat'||request.status!=='queued'||request.ended!==null)fail(409,'文本请求已结束或已派发','chat_request_conflict');
      const person=await store.authenticate(secret,{at:clock(),lock:true,exclusive:true}),scope=await access.resolve(expectedScope===undefined?store.departmentScope(person):expectedScope,{member:person,write:true,employeeOnly:true});access.same(scope,request);if(person.id!==request.member_id||person.key_id!==request.key_id)fail(404,'文本请求不属于此Key','chat_request_not_found');await authorizeDevice?.(person);await store.checkModel(person,request.model);await allowance(person,requestId,{checkBusy:false});await checkLimits(person,{model:request.model},{at:clock(),requestId});
      await lockPostgresRouting(store);let account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 FOR SHARE',request.account_id),session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1 FOR SHARE',request.session_id);
      if(session)access.same(scope,session);if(account)access.same(scope,account);
      if(!session||session.member_id!==person.id||session.protocol!=='chat'||!session.enabled)fail(403,'该会话已被禁止访问','session_disabled');
      if(session.account_id!==request.account_id||session.route_version!==request.route_version){
        if(!followDisabled||!account||account.enabled||account.removed_at!==null||session.state!=='new'||session.upstream_id!==null||session.message_count!==0||session.cum_input!==0||session.cum_output!==0||session.cum_cached!==0||session.route_version<=request.route_version)fail(409,'会话绑定或状态已变化，请重新开始','session_route_conflict');
        account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 FOR SHARE',session.account_id);if(account)access.same(scope,account);request.account_id=session.account_id;request.route_version=session.route_version;
        // Return the candidate only; the final authorization commits this
        // binding together with any old/new extra reservation transfer.
      }
      if(session.state==='uncertain')fail(409,'会话状态不确定，请重新开始','session_uncertain');
      if(!account?.enabled||account.removed_at!==null||account.kind!==request.model||account.auth_mode!=='subscription'||!['ready','degraded'].includes(account.state))fail(503,'绑定账号不可用','session_account_unavailable');
      await checkManualWeight(account,scope);await checkPackageScope(account,scope);
      if(!(await capacities([account],request.predicted,requestId,scope))[0].capacity.eligible)fail(429,'绑定账号预计额度不足；会话不会换号','session_quota_exhausted');
      if(isCancelled())fail(503,'文本请求已取消或服务停止','chat_cancelled');
      if(acquireAccount(account)!==true)fail(409,'账号仍在执行其他请求','chat_account_busy');
      await store.run("UPDATE harbor.requests SET phase='cli',phase_at=$1,phase_history=$2 WHERE id=$3",clock(),JSON.stringify([{phase:'queued',at:request.started},{phase:'cli',at:clock()}]),requestId);await store.run('UPDATE harbor.accounts SET last_used=$1 WHERE id=$2',clock(),account.id);if(isCancelled())fail(503,'文本请求已取消或服务停止','chat_cancelled');return {request,account,session,person,scope,contextReset:session.state==='new'&&session.route_version>0};
    });
    if(!deferAuthorization){const edge=dispatchBoundary(secret,{requestId,...ready},{isCancelled,authorizeDevice});await edge.run(async()=>ready);return {...ready,extraBoundary:edge};}return ready;
  }
  function dispatchBoundary(secret,admission,{isCancelled=()=>false,authorizeDevice}={}){
    const scope=admission.scope??undefined;
    const notCancelled=()=>{if(isCancelled())fail(503,'文本请求已取消或服务停止','chat_cancelled');};
    return extraRuntime.boundary({requestId:admission.requestId,protocol:'chat',scope,admittedExtra:admission.extraAdmission?.extraReserved===true,packagePreference:async request=>{if(admission.account.kind==='codex'){const preference=await router.extraDispatchPreference({accountId:request.account_id,model:request.model,protocol:'chat',scope});if(!preference.packageAlternatives.length)return preference;const rows=await store.all('SELECT * FROM harbor.accounts WHERE id=ANY($1::text[])'+(scope?' AND department_id=$2':'')+' ORDER BY id',preference.packageAlternatives,...(scope?[scope.department_id]:[])),eligible=await capacities(rows,request.predicted,request.id,scope);return {...preference,packageAlternatives:eligible.filter(row=>row.capacity.eligible&&!row.extra).map(row=>row.account.id)};}const rows=await store.all("SELECT * FROM harbor.accounts WHERE kind=$1 AND auth_mode='subscription' AND removed_at IS NULL"+(scope?' AND department_id=$2':'')+" ORDER BY id",request.model,...(scope?[scope.department_id]:[])),current=rows.find(row=>row.id===request.account_id),classified=current?await extraRuntime.classify(current,{model:request.model,protocol:'chat',scope}):null,eligible=await capacities(rows.filter(row=>row.enabled&&['ready','degraded'].includes(row.state)&&(!row.route_block||row.route_block==='quota_exhausted')),request.predicted,request.id,scope);return {mode:classified?.mode,manualPositive:false,accountIds:rows.map(row=>row.id),packageAlternatives:eligible.filter(row=>row.account.id!==request.account_id&&row.capacity.eligible&&!row.extra).map(row=>row.account.id)};},
      prepare:async request=>{notCancelled();const person=await store.authenticate(secret,{at:clock(),lock:true,exclusive:true});await access.resolve(scope,{member:person,write:true,employeeOnly:true});access.same(scope??null,request);if(person.id!==request.member_id||person.key_id!==request.key_id)fail(404,'文本请求不属于此Key','chat_request_not_found');await authorizeDevice?.(person);await store.checkModel(person,request.model);await allowance(person,request.id,{checkBusy:false});await checkLimits(person,{model:request.model},{at:clock(),requestId:request.id});await lockPostgresRouting(store);if(request.account_id!==admission.account.id||request.route_version!==admission.session.route_version){const source=await store.get('SELECT enabled,removed_at FROM harbor.accounts WHERE id=$1',request.account_id),session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1 FOR SHARE',request.session_id);if(!source||source.enabled||source.removed_at!==null||!session||session.member_id!==request.member_id||session.protocol!=='chat'||session.account_id!==admission.account.id||session.route_version!==admission.session.route_version||session.state!=='new'||session.upstream_id!==null||session.message_count!==0||session.cum_input!==0||session.cum_output!==0||session.cum_cached!==0||session.route_version<=request.route_version)fail(409,'排队会话绑定已变化，请重新开始','session_route_conflict');await store.run('UPDATE harbor.requests SET account_id=$1,route_version=$2 WHERE id=$3',session.account_id,session.route_version,request.id);}return notCancelled;},
      validate:async(account,request)=>{const session=await store.get('SELECT * FROM harbor.sessions WHERE id=$1 FOR SHARE',request.session_id),full=await store.get('SELECT * FROM harbor.accounts WHERE id=$1',account.id);access.same(scope??null,full);if(session)access.same(scope??null,session);if(account.id!==admission.account.id||account.route_epoch!==admission.account.route_epoch||!session||session.member_id!==request.member_id||session.protocol!=='chat'||!session.enabled||session.state==='uncertain'||session.account_id!==account.id||session.route_version!==admission.session.route_version||request.route_version!==session.route_version)fail(409,'文本账号或会话绑定已变化，请重试','session_route_conflict');await checkManualWeight(account,scope);await checkPackageScope(account,scope);if(!(await capacities([full],request.predicted,request.id,scope))[0].capacity.eligible)fail(429,'绑定账号预计额度不足；会话不会换号','session_quota_exhausted');notCancelled();}
    });
  }
  return {admit,dispatch,dispatchBoundary};
}
