import {codexQuotaDenials} from './codex-entitlements.mjs';
import {AppError,hash,id,nextDailyReset,usageWindow} from './store.mjs';
import {transferSchedule,transferWindow} from './transfer-schedule.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {createPostgresExtraRuntime,classifyExtraPackage,noteExtraDispatchRoutingLock,readExtraDispatchSettings,registerExtraDispatchPreferenceReader} from './postgres-extra-runtime.mjs';
import {consumptionSource,consumptionCandidateGroups,normalizeConsumptionPolicy} from './consumption-policy.mjs';
import {automaticWeights,balanceSettings,routingAutoState,routingDistribution,routingAllocation} from './balancing.mjs';

const failure=(status,message,code)=>{throw new AppError(status,message,code);};
// The pre-admission handoff lookup and transactional router must use exactly
// the same client identity precedence and trusted employee ownership checks.
export async function findPostgresResponseSession(store,member,payload,headers={},{lock=false}={}){
  const clientId=headers['thread-id']||headers['session-id']||headers.session_id||headers['x-codex-thread-id']||payload.prompt_cache_key;
  if(clientId!==undefined&&(typeof clientId!=='string'||!clientId.length))failure(400,'客户端会话标识无效','invalid_session');
  const clientKey=clientId?hash(clientId):null,clientSessionId=typeof clientId==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(clientId)?clientId:null;
  const fixed=headers['x-harbor-session'];
  if(fixed!==undefined&&(typeof fixed!=='string'||!/^[a-zA-Z0-9-]{1,80}$/.test(fixed)))failure(400,'固定会话标识无效','invalid_session');
  const suffix=lock?' FOR UPDATE':'';
  const session=fixed?await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND protocol='responses'"+suffix,fixed,member.id):clientKey?await store.get("SELECT * FROM harbor.sessions WHERE member_id=$1 AND protocol='responses' AND client_key=$2"+suffix,member.id,clientKey):null;
  if(fixed&&!session)failure(404,'固定会话不存在或不属于该员工','session_not_found');
  if(fixed&&(!clientKey||session.client_key&&session.client_key!==clientKey))failure(409,'固定绑定已用于另一条客户端对话，请创建新的固定会话','session_route_conflict');
  return {session,clientKey,clientSessionId,fixed};
}
// Every routing writer uses this transaction-scoped lock. It covers allocation
// credits and binding changes, never body upload or the upstream model call.
export async function lockPostgresRouting(store){
  if(!store.context.getStore())failure(503,'路由必须在请求受理事务内执行','database_transaction_required');
  await store.query("SELECT pg_advisory_xact_lock(hashtextextended('harbor-routing-state',0))");
  noteExtraDispatchRoutingLock(store);
}
const consumptionScopes=new WeakMap();
// Minted only by the authenticated consumption adapter, never from payload or
// headers. Copy the candidate set so later callers cannot broaden the scope.
export function createPostgresConsumptionRoutingScope({accountIds,source,rebind=false,expectedSession=null,excludeRequestId=null,resumeSession=false}={}){
  if(!['api','subscription'].includes(source)||!(accountIds instanceof Set)||[...accountIds].some(value=>typeof value!=='string'||!value.length)||typeof rebind!=='boolean'||typeof resumeSession!=='boolean'||resumeSession&&!expectedSession||excludeRequestId!==null&&typeof excludeRequestId!=='string'||expectedSession!==null&&(!expectedSession.id||!expectedSession.account_id||!Number.isSafeInteger(expectedSession.route_version)))failure(503,'来源路由约束无效','consumption_scope_invalid');
  const token=Object.freeze({});consumptionScopes.set(token,{accountIds:new Set(accountIds),source,rebind,resumeSession,expectedSession:expectedSession&&{id:expectedSession.id,account_id:expectedSession.account_id,route_version:expectedSession.route_version},excludeRequestId});return token;
}
export function createPostgresRouter(store,{clock=Date.now,preparing=new Set(),isStopping=()=>false,extraRuntime=createPostgresExtraRuntime(store,{clock})}={}){
  const packages=createPostgresAccountPackages(store,{clock});
  const write=(name,value)=>store.run('INSERT INTO harbor.gateway_settings(name,value) VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET value=EXCLUDED.value',name,JSON.stringify(value));
  const audit=(action,detail,at)=>store.run('INSERT INTO harbor.audit VALUES($1,$2,$3,$4)',id(),at,action,JSON.stringify(detail));
  async function state(at,{protocol='responses',model,force=false,persist=true,persistAuto=true,accountIds,packagePolicyOverride,scheduleName='routing_schedule'}={}){
    const saved=await readExtraDispatchSettings(store,async()=>new Map((await store.all("SELECT name,value FROM harbor.gateway_settings WHERE name IN ('balancing','routing_strategy','routing_weights','routing_auto','routing_schedule','routing_schedule.consumption_api','routing_schedule.consumption_subscription','account_package_policy')"+(persist?' FOR SHARE':''))).map(row=>[row.name,JSON.parse(row.value)])));
    const legacy=saved.get('balancing')||{},settings=balanceSettings({...legacy,weighted:(saved.get('routing_strategy')||{}).weighted??legacy.weighted??false});
    let accounts=await packages.routingAccounts({protocol,persist});if(accountIds)accounts=accounts.filter(account=>accountIds.has(account.id));
    const packagePolicy=packagePolicyOverride??saved.get('account_package_policy')??{enabled:false,smoothing_hours:24,version:0},options={packagePolicy,protocol,model};
    let system;
    if(settings.weighted||packagePolicy.enabled||force){
      const {save,audit:detail,...value}=routingAutoState(accounts,at,saved.get('routing_auto')||null,force,options);system=value;
      if(save&&persist&&persistAuto){await write('routing_auto',save);await audit('balancing.weights.auto',detail,at);}
    }else system=automaticWeights(accounts.filter(a=>a.auth_mode!=='openai_api'),at,options);
    const configured=saved.get('routing_weights')||{mode:'auto',weights:{},expires:null,version:0},manualActive=configured.mode==='manual'&&configured.expires>at,effectiveWeighted=settings.weighted||packagePolicy.enabled&&(!system.legacyCompatibility||manualActive);
    const baseDistribution=routingDistribution(accounts,{at,weighted:effectiveWeighted,system,configured}),extraView=await extraRuntime.routingView(accounts,{model,protocol,packagePolicy,distribution:baseDistribution}),distribution=extraView.distribution;
    const select=async()=>{
      const result=routingAllocation(distribution,saved.get(scheduleName)||{});
      if(!result.selected){if(system.strict)failure(503,'没有可比较且额度允许的套餐预算，请完善套餐依据或设置临时人工比例','package_routing_unavailable');failure(503,'没有生效权重大于0的可用账号，请调整权重或等待额度恢复','no_account');}
      if(result.schedule&&persist)await write(scheduleName,result.schedule);
      return result.selected;
    };
    return {settings,packagePolicy,effectiveWeighted,accounts,distribution,extraView,select,schedule:saved.get(scheduleName)||{}};
  }
  async function resolve(member,payload,headers={},{preparing:requestPreparing,protocol='responses',consumptionScope}={}){
    const scope=consumptionScope===undefined?null:consumptionScopes.get(consumptionScope);if(consumptionScope!==undefined&&!scope)failure(503,'来源路由约束未经认证','consumption_scope_invalid');
    if(!['responses','images'].includes(protocol))failure(503,'推理通道配置无效','routing_configuration');
    await lockPostgresRouting(store);const at=clock();
    const identity=await findPostgresResponseSession(store,member,payload,headers,{lock:true}),{clientKey,clientSessionId,fixed}=identity;let {session}=identity;
    if(!session&&scope?.resumeSession&&!fixed)session=await store.get("SELECT * FROM harbor.sessions WHERE id=$1 AND member_id=$2 AND protocol='responses' FOR UPDATE",scope.expectedSession.id,member.id);
    if(session&&!session.enabled)failure(403,'该会话已被禁止访问','session_disabled');
    if(protocol==='images'&&session&&(await store.get('SELECT auth_mode FROM harbor.accounts WHERE id=$1',session.account_id))?.auth_mode==='openai_api')failure(400,'官方Key接入暂仅支持Responses会话','unsupported_api_route');
    if(scope?.expectedSession&&(!session||['id','account_id','route_version'].some(field=>session[field]!==scope.expectedSession[field])))failure(409,'会话绑定已变化','session_route_conflict');
    const routing=await state(at,{protocol,model:payload.model,...(scope?{accountIds:scope.accountIds,persistAuto:false,scheduleName:'routing_schedule.consumption_'+scope.source}:{})});let account=session?routing.accounts.find(a=>a.id===session.account_id):null;
    if(scope&&session&&!account){
      if(fixed||!scope.rebind)failure(409,'固定会话或上下文不允许改变消耗来源','consumption_source_conflict');
      if(preparing.has(member.id)||requestPreparing?.has(member.id)||await store.get("SELECT id FROM harbor.requests WHERE session_id=$1 AND status IN ('queued','running') AND ($2::text IS NULL OR id<>$2) LIMIT 1",session.id,scope.excludeRequestId))failure(409,'会话仍有请求处理中，暂不能切换','session_busy');
      const target=await routing.select(),from=session.account_id;
      const changed=await store.get('UPDATE harbor.sessions SET account_id=$1,upstream_thread=$2,route_version=route_version+1,handoff_requires_replay=1 WHERE id=$3 AND route_version=$4 RETURNING *',target.id,id(),session.id,session.route_version);
      if(!changed)failure(409,'会话绑定已变化','session_route_conflict');session=changed;account=target;
      await audit('session.route',{session:session.id,from,to:target.id,reason:'consumption_source'},at);
    }
    if(session){
      if(!account||!account.enabled||!['ready','degraded'].includes(account.state)&&!account.route_block)failure(503,'会话绑定账号不可用，请新建客户端任务','session_account_unavailable');
      const api=account.auth_mode==='openai_api',blocked=account.route_block&&(api||account.route_block!=='quota_exhausted');
      if(blocked&&!routing.settings.failover)failure(503,'绑定账号已阻断，自动切换未开启','session_account_blocked');
      const pinnedApi=api&&['ready','degraded'].includes(account.state)&&!account.route_block;
      const ownExtra=routing.extraView.decisions.get(account.id)?.decision,authorizedExtra=ownExtra?.allowed===true&&ownExtra.mode==='manual_extra',extraDenied=['extra','blocked'].includes(routing.extraView.classifications.get(account.id)?.mode)&&routing.extraView.classifications.get(account.id)?.reason!=='package_scope_mismatch'&&!authorizedExtra;
      const positive=authorizedExtra&&routing.distribution.subscriptionMode!=='manual'||(routing.distribution.mode==='fallback'?routing.distribution.eligible.some(a=>a.id===account.id):routing.distribution.weights.get(account.id)>0);
      const packageScope=routing.packagePolicy.enabled&&routing.distribution.system.packages?.items.find(item=>item.id===account.id);if(packageScope?.reason==='package_scope_mismatch')failure(409,'当前会话账号的套餐不适用于此模型或协议，请选择兼容模型或新建会话','package_scope_mismatch');
      const uncertain=routing.packagePolicy.enabled&&routing.distribution.subscriptionMode!=='manual'&&routing.distribution.system.packages?.items.find(item=>item.id===account.id),insufficient=uncertain&&!uncertain.exhausted&&uncertain.reason!=='quota_exhausted'&&(!uncertain.eligible||routing.distribution.system.reason);
      const exhausted=routing.packagePolicy.enabled&&packageScope?.exhausted&&!authorizedExtra;
      if(!pinnedApi&&(blocked||exhausted||extraDenied||routing.effectiveWeighted&&!positive&&!insufficient)){
        if(preparing.has(member.id)||requestPreparing?.has(member.id)||await store.get("SELECT id FROM harbor.requests WHERE session_id=$1 AND status IN ('queued','running') LIMIT 1",session.id))failure(409,'会话仍有请求处理中，暂不能切换','session_busy');
        let destination=routing;
        if(routing.packagePolicy.enabled&&!api&&routing.distribution.subscriptionMode!=='manual'){
          const items=new Map(routing.distribution.system.packages.items.map(item=>[item.id,item])),source=items.get(account.id),legacy=account.package_selection?.mode==='legacy',legacyGroup=routing.distribution.system.snapshots.get(account.id)?.group;
          const compatible=new Set(routing.accounts.filter(candidate=>candidate.auth_mode==='openai_api'||routing.extraView.decisions.get(candidate.id)?.decision.mode==='extra'&&routing.extraView.decisions.get(candidate.id)?.decision.allowed||source?.comparison_key&&items.get(candidate.id)?.comparison_key===source.comparison_key||legacy&&legacyGroup&&candidate.package_selection?.mode==='legacy'&&routing.distribution.system.snapshots.get(candidate.id)?.group===legacyGroup).map(candidate=>candidate.id));
          destination=await state(at,{protocol,model:payload.model,accountIds:scope?new Set([...compatible].filter(accountId=>scope.accountIds.has(accountId))):compatible,persistAuto:false,...(scope?{scheduleName:'routing_schedule.consumption_'+scope.source}:{})});
        }
        const target=await destination.select();
        if(target.id!==account.id){
          const expires=api&&target.auth_mode!=='openai_api'&&!session.nightly_enabled&&session.nightly_expires===null?nextDailyReset(at):session.nightly_expires;
          const changed=await store.get('UPDATE harbor.sessions SET account_id=$1,upstream_thread=$2,route_version=route_version+1,nightly_expires=$3 WHERE id=$4 AND route_version=$5 RETURNING *',target.id,id(),expires,session.id,session.route_version);
          if(!changed)failure(409,'会话绑定已变化','session_route_conflict');
          await audit('session.route',{session:session.id,from:account.id,to:target.id,reason:blocked?account.route_block:'weight_zero'},at);session=changed;account=target;
        }
      }
      if(fixed&&!session.client_key){
        const duplicate=await store.get("SELECT id FROM harbor.sessions WHERE member_id=$1 AND protocol='responses' AND client_key=$2 AND id<>$3",member.id,clientKey,session.id);
        if(duplicate)failure(409,'客户端对话已有另一条绑定，请使用新对话','session_route_conflict');
        session=await store.get('UPDATE harbor.sessions SET client_key=$1,client_session_id=$2 WHERE id=$3 RETURNING *',clientKey,clientSessionId,session.id);
      }else if(clientSessionId&&!session.client_session_id)session=await store.get('UPDATE harbor.sessions SET client_session_id=$1 WHERE id=$2 RETURNING *',clientSessionId,session.id);
    }else{
      account=await routing.select();
      session=await store.get("INSERT INTO harbor.sessions(id,member_id,account_id,protocol,client_key,client_session_id,created,updated) VALUES($1,$2,$3,'responses',$4,$5,$6,$6) RETURNING *",id(),member.id,account.id,clientKey,clientSessionId,at);
    }
    return {session,account};
  }
  async function disableAccount(accountId){
    return store.transaction(async()=>{
      await lockPostgresRouting(store);const account=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL FOR UPDATE',accountId);
      if(!account)failure(404,'账号不存在或已移除','account_not_found');
      const at=clock(),exists=await store.get('SELECT id FROM harbor.sessions WHERE account_id=$1 LIMIT 1',accountId);
      if(account.enabled||exists)await store.run('UPDATE harbor.accounts SET enabled=0,route_epoch=route_epoch+1 WHERE id=$1',accountId);
      let moved=0,resetTextSessions=0,schedule=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_schedule'"))?.value||'{}'),scheduleChanged=false;
      const states=new Map();let alternatives;
      if(account.kind!=='codex'){
        alternatives=await store.all("SELECT id,auth_mode,last_used FROM harbor.accounts WHERE kind=$1 AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') ORDER BY id FOR SHARE",account.kind);
        const counts=new Map((await store.all('SELECT account_id,COUNT(*) bindings FROM harbor.sessions WHERE account_id=ANY($1::text[]) GROUP BY account_id',alternatives.map(a=>a.id))).map(row=>[row.account_id,row.bindings]));
        for(const target of alternatives)target.bindings=counts.get(target.id)||0;
      }
      for(;;){
        const sessions=await store.all('SELECT id,protocol,route_version,nightly_enabled,nightly_expires FROM harbor.sessions WHERE account_id=$1 ORDER BY id LIMIT 256 FOR UPDATE',accountId);if(!sessions.length)break;
        const changes=[];
        for(const session of sessions){
          let target;
          if(account.kind==='codex'){
            const protocol=session.protocol==='responses'?'responses':'chat';if(!states.has(protocol))states.set(protocol,await state(at,{protocol}));
            const result=routingAllocation(states.get(protocol).distribution,schedule);target=result.selected;
            if(result.schedule){schedule=result.schedule;scheduleChanged=true;}
          }else{
            target=[...alternatives].sort((a,b)=>a.bindings-b.bindings||a.last_used-b.last_used||a.id.localeCompare(b.id))[0];if(target)target.bindings++;
          }
          if(!target)failure(409,'没有可接收会话的同类型正权重账号，未停用或迁移任何会话','disable_no_target');
          const expires=account.auth_mode==='openai_api'&&target.auth_mode!=='openai_api'&&!session.nightly_enabled&&session.nightly_expires===null?nextDailyReset(at):session.nightly_expires;
          changes.push({id:session.id,target:target.id,thread:id(),expires,version:session.route_version,audit:id()});
          if(session.protocol!=='responses')resetTextSessions++;
        }
        const records=JSON.stringify(changes),updated=await store.run(`UPDATE harbor.sessions s SET account_id=p.target,upstream_thread=p.thread,route_version=s.route_version+1,nightly_expires=p.expires,updated=$2,
          upstream_id=CASE WHEN s.protocol='responses' THEN s.upstream_id ELSE NULL END,state=CASE WHEN s.protocol='responses' THEN s.state ELSE 'new' END,
          message_count=CASE WHEN s.protocol='responses' THEN s.message_count ELSE 0 END,history_digest=CASE WHEN s.protocol='responses' THEN s.history_digest ELSE '' END,
          cum_input=CASE WHEN s.protocol='responses' THEN s.cum_input ELSE 0 END,cum_output=CASE WHEN s.protocol='responses' THEN s.cum_output ELSE 0 END,cum_cached=CASE WHEN s.protocol='responses' THEN s.cum_cached ELSE 0 END
          FROM jsonb_to_recordset($1::jsonb) AS p(id text,target text,thread text,expires bigint,version bigint) WHERE s.id=p.id AND s.account_id=$3 AND s.route_version=p.version`,records,at,accountId);
        if(updated.changes!==changes.length)failure(409,'会话绑定已变化，停用操作已回滚','session_route_conflict');
        await store.run(`INSERT INTO harbor.audit(id,at,action,detail) SELECT p.audit,$2,'session.route',jsonb_build_object('sessionId',p.id,'from',$3::text,'to',p.target,'reason','account_disabled','version',p.version+1)::text
          FROM jsonb_to_recordset($1::jsonb) AS p(id text,target text,audit text,version bigint)`,records,at,accountId);moved+=changes.length;
      }
      if(scheduleChanged)await write('routing_schedule',schedule);
      return {moved,resetTextSessions};
    });
  }
  async function block(account,reason,{quotaRefresh=false}={}){
    if(!['quota_exhausted','login_expired'].includes(reason))return false;
    return store.transaction(async()=>{
      await lockPostgresRouting(store);const current=await store.get('SELECT * FROM harbor.accounts WHERE id=$1 FOR UPDATE',account.id);
      if(!current||current.route_epoch!==account.route_epoch||quotaRefresh&&current.route_block===reason||reason==='quota_exhausted'&&current.route_block==='login_expired')return false;
      const at=clock();await store.run('UPDATE harbor.accounts SET route_block=$1,route_epoch=route_epoch+1,route_blocked_at=$2 WHERE id=$3',reason,at,account.id);
      await audit('account.route_block',{account:account.id,reason},at);await state(at);return true;
    });
  }
  const managementTransaction=operation=>store.transaction(async()=>{
    await lockPostgresRouting(store);
    const running=()=>{if(isStopping())throw new AppError(503,'调度管理正在关闭','service_stopping');};
    running();const result=await operation();running();return result;
  });
  async function saveSettings(value){
    balanceSettings(value);return managementTransaction(async()=>{
      const {settings}=await state(clock(),{persist:false}),next=balanceSettings({...settings,...value}),{weighted,...legacy}=next;
      await write('balancing',legacy);await write('routing_strategy',{weighted});await audit('balancing.settings',next,clock());return next;
    });
  }
  async function saveTransferSchedule(value){
    if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!['times','expected_version'].includes(k))||!Number.isSafeInteger(value.expected_version)||!Array.isArray(value.times))failure(400,'定时转移设置无效','transfer_schedule_invalid');
    const desired=transferSchedule({times:value.times});
    return managementTransaction(async()=>{
      const previous=transferSchedule(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='transfer_schedule' FOR UPDATE"))?.value||'{}'));
      if(previous.version!==value.expected_version)failure(409,'定时转移配置已变化，请刷新后重试','transfer_schedule_conflict');
      const next=transferSchedule({...desired,version:previous.version+1,effective:clock()});
      await write('transfer_schedule',next);await audit('balancing.transfer_schedule',{...next,previous},clock());return next;
    });
  }
  async function saveWeights(value){
    if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!['mode','weights','expected_version'].includes(k))||!['auto','manual'].includes(value.mode)||!Number.isSafeInteger(value.expected_version))failure(400,'权重设置无效');
    return managementTransaction(async()=>{
      const current=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights' FOR UPDATE"))?.value||'{"mode":"auto","weights":{},"expires":null,"version":0}');
      if(value.expected_version!==current.version)failure(409,'权重已变化，请刷新后重试');let weights={};
      if(value.mode==='manual'){
        weights=value.weights;const ids=new Set((await store.all("SELECT id FROM harbor.accounts WHERE kind='codex' AND auth_mode='subscription' AND removed_at IS NULL ORDER BY id FOR SHARE")).map(a=>a.id));
        if(!weights||typeof weights!=='object'||Array.isArray(weights)||!Object.keys(weights).length||Object.entries(weights).some(([key,v])=>!ids.has(key)||!Number.isFinite(v)||v<0||v>100||Math.abs(v*100-Math.round(v*100))>1e-8)||Math.abs(Object.values(weights).reduce((n,v)=>n+v,0)-100)>1e-8)failure(400,'请填写有效账号权重，最多两位小数且总和必须为100%');
      }else if(value.weights!==undefined)failure(400,'自动模式无需填写权重');
      const next={mode:value.mode,weights,expires:value.mode==='manual'?nextDailyReset(clock()):null,version:current.version+1};
      await write('routing_weights',next);if(value.mode==='auto')await state(clock(),{force:'manual'});await audit('balancing.weights',{...next,previous:current},clock());return next;
    });
  }
  async function readView({explanation=false,packagePreview=false,at=clock()}={}){
    return store.transaction(async()=>{
      const {settings,packagePolicy,effectiveWeighted,accounts,distribution,schedule,extraView}=await state(at,{persist:false,...(packagePreview?{packagePolicyOverride:{...await packages.policy(),enabled:true}}:{})});
      const strategy={configured:settings.weighted,effective:effectiveWeighted,source:packagePolicy.enabled?distribution.system.legacyCompatibility?'legacy_compatibility':'package_capacity':settings.weighted?'hourly_percentage':'legacy',package_policy:packagePolicy};
      if(explanation){const available=a=>a.enabled&&!a.removed_at&&a.kind==='codex'&&['ready','degraded'].includes(a.state)&&(!a.route_block||a.route_block==='quota_exhausted'),extraScopeRequired=false,next=routingAllocation(distribution,schedule).selected;
        return {at,...(extraScopeRequired?{extraScopeRequired:true}:{}),preview:packagePreview,strategy,coverage:{unknown:distribution.system.packages?.items.filter(item=>item.mode==='unknown').length??0,legacy:distribution.system.packages?.items.filter(item=>item.mode==='legacy').length??0,comparable_groups:distribution.system.packages?.groups.length??0,reason:distribution.system.reason},mode:distribution.mode,reason:distribution.reason,configuration:distribution.configuration,basis:distribution.system.basis??null,updated:distribution.system.updated??null,trigger:distribution.system.trigger??null,nextAccount:next?.id??null,
          accounts:accounts.map(a=>({...distribution.accounts.find(x=>x.id===a.id),name:a.name,reason:!a.enabled?'disabled':a.auth_mode==='openai_api'&&!a.route_block&&!(a.api_weight>0)?'manual_binding_only':a.route_block==='login_expired'?'login_expired':!available(a)?'not_ready':!distribution.eligible.some(x=>x.id===a.id)?distribution.system.packages?.items.find(item=>item.id===a.id)?.reason||distribution.system.reason||'quota_exhausted':!['legacy','fallback'].includes(distribution.mode)&&!(distribution.weights.get(a.id)>0)?'zero_weight':'eligible',extraQuota:distribution.accounts.find(item=>item.id===a.id)?.extra?.allowed===true&&distribution.accounts.find(item=>item.id===a.id)?.extra?.mode==='manual_extra'}))};
      }
      const transfers=transferSchedule(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='transfer_schedule'"))?.value||'{}'));
      const window=usageWindow(at),lastRun=await store.get('SELECT day,started,ended,status FROM harbor.balance_runs WHERE started>=$1 AND started<$2 ORDER BY started DESC,day DESC LIMIT 1',window.start,window.end);
      return {settings,strategy,transfers:{...transfers,next:transferWindow(at,transfers).next},weights:{mode:distribution.mode,reason:distribution.reason,configuration:distribution.configuration,accounts:distribution.accounts},lastRun:lastRun||null};
    },{level:'REPEATABLE READ',readOnly:true});
  }
  async function syncWeights(force=false,at=clock()){return store.transaction(async()=>{await lockPostgresRouting(store);return state(at,{force});});}
  async function recover(account,snapshot){
    if(!account.route_block||!snapshot?.windows?.length||snapshot.windows.some(w=>!w.reset||w.reset<=clock()))return false;
    const denial=codexQuotaDenials(snapshot);if(denial.ordinary||denial.spend_control||denial.workspace)return false;const classification=classifyExtraPackage({...account,route_block:null,quota_json:snapshot},{at:clock()});if(!['package','extra'].includes(classification.mode))return false;const exhausted=classification.mode==='extra';if(exhausted&&account.route_block!=='login_expired')return false;
    return store.transaction(async()=>{
      await lockPostgresRouting(store);const current=await store.get('SELECT route_epoch,route_block FROM harbor.accounts WHERE id=$1 AND removed_at IS NULL FOR UPDATE',account.id);
      if(!current||current.route_epoch!==account.route_epoch||!current.route_block)return false;
      const at=clock();await store.run('UPDATE harbor.accounts SET route_block=$1,route_epoch=route_epoch+1,route_blocked_at=$2 WHERE id=$3',exhausted?'quota_exhausted':null,exhausted?at:null,account.id);
      await audit('account.route_recover',account.id,at);await state(at,{force:'recovery'});return true;
    });
  }
  async function catalogCandidates({at=clock(),policy='inherit',model,protocol='responses'}={}){
    normalizeConsumptionPolicy(policy,{defaultValue:null});if(!['responses','images'].includes(protocol))failure(503,'推理通道配置无效','routing_configuration');
    const read=async()=>{
      const whole=await state(at,{protocol,model,persist:false,persistAuto:false}),groups=consumptionCandidateGroups(policy,whole.accounts),candidates=[],sources={};
      for(const group of groups){
        const accountIds=new Set(group.candidates.map(account=>account.id)),view=group.source===null?whole:await state(at,{protocol,model,persist:false,persistAuto:false,accountIds});
        for(const account of view.distribution.eligible){if(!(view.distribution.weights.get(account.id)>0))continue;candidates.push({account,source:consumptionSource(account),eligible:true,support:'unknown',reason:model?'model_support_unverified':'model_not_selected'});}
      }
      for(const source of ['api','subscription']){
        const accounts=whole.accounts.filter(account=>consumptionSource(account)===source),configured=accounts.filter(account=>account.enabled&&['ready','degraded'].includes(account.state)&&(source!=='api'||account.api_weight>0));
        sources[source]={accountIds:accounts.map(account=>account.id),configuredIds:configured.map(account=>account.id),quotaExhaustedIds:configured.filter(account=>account.route_block==='quota_exhausted').map(account=>account.id)};
      }
      return {candidates,sources,policy,model:model??null,protocol};
    };
    return store.context.getStore()?read():store.transaction(read,{level:'REPEATABLE READ',readOnly:true});
  }
  async function selectAccount({at=clock(),protocol='responses',model,accountIds,legacyOrder}={}){await lockPostgresRouting(store);const scoped=await state(at,{protocol,model,accountIds,persistAuto:false});if(scoped.distribution.system.legacyCompatibility&&scoped.distribution.subscriptionMode!=='manual'&&Array.isArray(legacyOrder)){const pool=new Map(scoped.distribution.eligible.map(account=>[account.id,account]));for(const accountId of legacyOrder)if(pool.has(accountId))return pool.get(accountId);}return scoped.select();}
  async function extraDispatchPreference({accountId,model,protocol='responses'}={}){
    const scoped=await state(clock(),{model,protocol,persist:false,persistAuto:false}),manual=scoped.distribution.subscriptionMode==='manual',classification=scoped.extraView.classifications.get(accountId);
    return {mode:classification?.mode,manual,manualPositive:manual&&scoped.distribution.configuration.weights[accountId]>0,accountIds:scoped.accounts.filter(account=>account.auth_mode!=='openai_api').map(account=>account.id),packageAlternatives:scoped.distribution.eligible.filter(account=>account.id!==accountId&&account.auth_mode!=='openai_api'&&scoped.distribution.weights.get(account.id)>0&&['package','package_compatibility'].includes(scoped.extraView.classifications.get(account.id)?.mode)).map(account=>account.id)};
  }
  const dispatchPreference=registerExtraDispatchPreferenceReader(request=>extraDispatchPreference({accountId:request.account_id,model:request.model,protocol:request.source_channel==='responses'?'responses':'images'}));
  return {dispatchPreference,extraDispatchPreference,selectAccount,catalogCandidates,resolve,block,disableAccount,recover,syncWeights,saveSettings,saveWeights,saveTransferSchedule,readView};
}
