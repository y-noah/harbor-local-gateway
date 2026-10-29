import {AppError,id,nextDailyReset,participatesNightly} from './store.mjs';
import {planNightly} from './nightly-plan.mjs';
import {quoteReference,referenceMetric} from './pricing.mjs';

const minute=60000,day=24*60*minute;
export const balanceDefaults=Object.freeze({failover:false,nightly:false,quietMinutes:30,weighted:false});
export function balanceSettings(value={}){
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!Object.hasOwn(balanceDefaults,k)))throw new AppError(400,'负载均衡设置无效');
  const result={...balanceDefaults,...value};
  if(typeof result.failover!=='boolean'||typeof result.nightly!=='boolean'||typeof result.weighted!=='boolean'||!Number.isInteger(result.quietMinutes)||result.quietMinutes<1||result.quietMinutes>960)throw new AppError(400,'静默期须为 1–960 分钟，开关须为布尔值');
  return result;
}
export function nightlyWindow(at){
  const local=at+8*60*minute,startOfDay=Math.floor(local/day)*day-8*60*minute,end=startOfDay+2*60*minute;
  return {key:new Date(local).toISOString().slice(0,10),start:end-16*60*minute,end,due:at>=end&&at<end+minute};
}
export function quotaSnapshot(account,at){
  if(!account)return null;
  let q;try{q=JSON.parse(account.quota_json);}catch{return null;}
  if(!q||!Number.isFinite(q.checked)||q.checked>at||at-q.checked>5*minute||!q.plan||!Array.isArray(q.windows)||!q.windows.length)return null;
  if(q.windows.some(w=>!Number.isFinite(w.used)||w.used<0||w.used>100||!Number.isFinite(w.minutes)||w.minutes<=0||!Number.isFinite(w.reset)||w.reset<=at))return null;
  return {...q,remaining:q.limitReached||account.route_block==='quota_exhausted'?0:Math.min(...q.windows.map(w=>100-w.used)),group:JSON.stringify([q.plan,q.windows.map(w=>w.minutes).sort((a,b)=>a-b)])};
}
const isApi=a=>a?.auth_mode==='openai_api';
const available=a=>a.enabled&&!a.removed_at&&a.kind==='codex'&&['ready','degraded'].includes(a.state)&&(!a.route_block||a.route_block==='quota_exhausted');
export function automaticWeights(accounts,at){
  const snapshots=new Map(accounts.map(a=>[a.id,quotaSnapshot(a,at)]));
  const eligible=accounts.filter(a=>!isApi(a)&&available(a)&&a.route_block!=='quota_exhausted'&&!(snapshots.get(a.id)?.limitReached||snapshots.get(a.id)?.remaining===0));
  let reason=null;
  if(eligible.some(a=>!snapshots.get(a.id)))reason='quota_unavailable';
  else if(new Set(eligible.map(a=>snapshots.get(a.id).group)).size>1||eligible.some(a=>snapshots.get(a.id).windows.length!==1))reason='incomparable_quota';
  const scores=new Map(eligible.map(a=>{const q=snapshots.get(a.id);return [a.id,reason?0:q.remaining/Math.max((q.windows[0].reset-at)/3600000,24)];}));
  const total=[...scores.values()].reduce((n,v)=>n+v,0);
  const weights=new Map(accounts.map(a=>[a.id,eligible.includes(a)?reason?null:scores.get(a.id)/total*100:0]));
  const basis={at,formula:'remaining / max(hours_to_reset, 24)',accounts:accounts.map(a=>{
    const q=snapshots.get(a.id),hours=q?.windows.length===1?(q.windows[0].reset-at)/3600000:null;
    return {id:a.id,remaining:q?.remaining??null,checked:q?.checked??null,reset:hours!==null?q.windows[0].reset:null,hours,denominator:hours!==null?Math.max(hours,24):null,score:reason?null:scores.get(a.id)??0,weight:weights.get(a.id)};
  })};
  return {eligible,snapshots,reason,weights,basis};
}
export const nextWeightReset=nextDailyReset;
const median=values=>{if(!values.length)return null;const sorted=[...values].sort((a,b)=>a-b),n=sorted.length;return n%2?sorted[(n-1)/2]:(sorted[n/2-1]+sorted[n/2])/2;};
// Workload is a relative estimate, never a quota admission check.
export function planBalance({accounts,sessions,requests,at,quietMinutes=30,preparing=new Set(),weighted=false,quoteUsage=row=>quoteReference(row)}){
  const window=nightlyWindow(at),eligible=accounts.filter(a=>available(a)&&!isApi(a));
  const snapshots=new Map(eligible.map(a=>[a.id,quotaSnapshot(a,at)]));
  const active=new Map(),bySession=new Map(),latest=new Map();
  for(const r of requests){
    if(!r.session_id)continue;
    latest.set(r.session_id,Math.max(latest.get(r.session_id)||0,r.started,r.ended||r.started));
    const running=['queued','running'].includes(r.status);
    if((r.started<window.end&&(r.ended??at)>=window.start)||running){
      if(!bySession.has(r.session_id))bySession.set(r.session_id,[]);
      bySession.get(r.session_id).push(r);
    }
  }
  const samples=new Map();
  for(const rows of bySession.values())for(const r of rows)if(r.ended&&!r.usage_unknown){
    const value=quoteUsage(r);if(value.state!=='priced'||value.totalAtoms===null||value.totalAtoms===0n)continue;
    const key=value.model||r.billing_model||r.model||'';if(!samples.has(key))samples.set(key,[]);samples.get(key).push(r);
  }
  const uncertainAccounts=new Set(),skipped=[];
  for(const s of sessions){
    const rows=bySession.get(s.id);if(!rows)continue;
    let measuredAtoms=0n,estimatedAtoms=0,unknown=false;
    for(const r of rows){
      const value=quoteUsage(r);
      if(r.ended&&value.totalAtoms!==null){measuredAtoms+=value.totalAtoms;continue;}
      if(value.state==='unpriced'||value.state==='legacy_manual'){unknown=true;continue;}
      const key=value.model||r.billing_model||r.model||'';
      const comparable=(samples.get(key)||[]).map(sample=>quoteUsage(sample,r.price_version)).filter(q=>q.totalAtoms!==null).map(q=>Number(q.totalAtoms));
      const estimate=median(comparable);
      if(estimate===null)unknown=true;else estimatedAtoms+=estimate;
    }
    const weight=(Number(measuredAtoms)+estimatedAtoms)/1e12;
    if(weight===0&&!unknown){skipped.push({session_id:s.id,reason:'no_measured_work'});continue;}
    const running=rows.some(r=>['queued','running'].includes(r.status));
    const last=latest.get(s.id);
    const fixed=running||rows.some(r=>r.started>=window.end)||preparing.has(s.member_id)||at-last<quietMinutes*minute||!participatesNightly(s,at)||!s.enabled||!s.member_enabled||s.member_removed!==null||s.protocol!=='responses'||unknown;
    if(unknown)uncertainAccounts.add(s.account_id);
    active.set(s.id,{...s,weight,fixed,unknown});
    if(fixed)skipped.push({session_id:s.id,reason:unknown?'unknown_workload':running?'busy':'protected'});
  }
  const moves=[],groups=new Map();
  for(const a of eligible){const q=snapshots.get(a.id);if(!q){skipped.push({account_id:a.id,reason:'quota_unavailable'});continue;}if(!groups.has(q.group))groups.set(q.group,[]);groups.get(q.group).push(a);}
  const summaries=[];
  for(const pool of groups.values()){
    if(pool.length<2){skipped.push({reason:'incomparable_quota',accounts:pool.map(a=>a.id)});continue;}
    const ids=new Set(pool.map(a=>a.id)),work=[...active.values()].filter(s=>ids.has(s.account_id));
    // With no sample for an in-flight task, no defensible comparative plan exists.
    if(pool.some(a=>uncertainAccounts.has(a.id))){skipped.push({reason:'unknown_fixed_load',accounts:pool.map(a=>a.id)});continue;}
    const calculated=weighted?automaticWeights(pool,at):null;
    if(calculated?.reason){skipped.push({reason:calculated.reason,accounts:pool.map(a=>a.id)});continue;}
    const total=work.reduce((n,s)=>n+s.weight,0),remaining=pool.reduce((n,a)=>n+snapshots.get(a.id).remaining,0);
    if(!total||!remaining)continue;
    const shares=new Map(pool.map(a=>[a.id,weighted?calculated.weights.get(a.id)/100:snapshots.get(a.id).remaining/remaining]));
    const loads=new Map(pool.map(a=>[a.id,0])),target=new Map(pool.map(a=>[a.id,total*shares.get(a.id)]));
    for(const s of work)loads.set(s.account_id,loads.get(s.account_id)+s.weight);
    const before=Object.fromEntries(loads);
    const movable=work.filter(s=>!s.fixed).sort((a,b)=>b.weight-a.weight||a.id.localeCompare(b.id));
    const {moves:selected,planning}=planNightly({pool,movable,before,target,total});
    for(const a of pool)loads.set(a.id,before[a.id]);
    for(const m of selected){loads.set(m.from,loads.get(m.from)-m.weight);loads.set(m.to,loads.get(m.to)+m.weight);}
    for(const m of selected)moves.push(m);
    summaries.push({planning,accounts:pool.map(a=>a.id),weights:Object.fromEntries(shares),before,after:Object.fromEntries(loads),target:Object.fromEntries(target),fixed:work.filter(s=>s.fixed).map(s=>s.id),fixedLoad:Object.fromEntries(pool.map(a=>[a.id,work.filter(s=>s.fixed&&s.account_id===a.id).reduce((sum,s)=>sum+s.weight,0)]))});
  }
  return {metric:referenceMetric,priceRevisions:[...new Set(requests.map(r=>r.price_version).filter(Boolean))],window,moves,skipped,summaries};
}

export function createBalancer({store,preparing,refreshQuota,readQuota=async()=>null,clock=Date.now,stopping=()=>false}){
  let running=null;
  const settings=()=>balanceSettings({...readSetting('balancing',{}),weighted:readSetting('routing_strategy',{weighted:false}).weighted});
  const saveSettings=value=>{balanceSettings(value);return atomic(()=>{const next=balanceSettings({...settings(),...value}),{weighted,...legacy}=next;writeSetting('balancing',legacy);writeSetting('routing_strategy',{weighted});store.audit('balancing.settings',JSON.stringify(next));return next;});};
  const readSetting=(name,fallback)=>JSON.parse(store.get('SELECT value FROM gateway_settings WHERE name=?',name)?.value||JSON.stringify(fallback));
  const writeSetting=(name,value)=>store.run('INSERT INTO gateway_settings(name,value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value',name,JSON.stringify(value));
  const manual=()=>readSetting('routing_weights',{mode:'auto',weights:{},expires:null,version:0});
  const atomic=fn=>{if(store.db.isTransaction)return fn();store.db.exec('BEGIN IMMEDIATE');try{const result=fn();store.db.exec('COMMIT');return result;}catch(e){store.db.exec('ROLLBACK');throw e;}};
  function syncWeights(accounts=store.all("SELECT * FROM accounts WHERE kind='codex' AND removed_at IS NULL"),at=clock(),force=false){
    accounts=accounts.filter(a=>!isApi(a));
    const previous=readSetting('routing_auto',null),cycle=nextWeightReset(at)-day;
    const snapshots=new Map(accounts.map(a=>[a.id,quotaSnapshot(a,at)]));
    const states=Object.fromEntries(accounts.map(a=>[a.id,a.route_block==='quota_exhausted'?0:snapshots.get(a.id)?snapshots.get(a.id).remaining>0?1:0:previous?.states[a.id]??null]));
    const changed=!previous||previous.cycle!==cycle||accounts.some(a=>states[a.id]!==null&&states[a.id]!==previous.states[a.id]);
    const calculated=force||changed||previous?.pending?automaticWeights(accounts.map(a=>states[a.id]===0?{...a,route_block:'quota_exhausted'}:a),at):null;
    let saved=previous;
    if(force||changed||previous?.pending&&!calculated.reason){
      // A failed refresh cannot erase the last known allocation or resurrect an exhausted account.
      const retain=calculated.reason&&previous&&Object.values(previous.weights).every(Number.isFinite)&&Object.values(previous.weights).some(weight=>weight>0);
      const weights=retain?Object.fromEntries(accounts.map(a=>[a.id,states[a.id]===0?0:previous.weights[a.id]??0])):Object.fromEntries(calculated.weights);
      const trigger=force||(!previous?'initialize':previous.cycle!==cycle?'nightly':'quota');
      saved={cycle,states,weights,reason:calculated.reason,pending:!!calculated.reason,basis:retain?previous.basis??null:calculated.basis,updated:at,trigger};
      atomic(()=>{writeSetting('routing_auto',saved);store.audit('balancing.weights.auto',JSON.stringify({cycle,reason:trigger,weights,previous:previous?.weights??null,basis:saved.basis,retained:!!retain}));});
    }
    return {eligible:accounts.filter(a=>available(a)&&states[a.id]!==0),snapshots,reason:saved.reason,basis:saved.basis??null,updated:saved.updated??null,trigger:saved.trigger??null,weights:new Map(accounts.map(a=>[a.id,!available(a)||states[a.id]===0?0:Object.hasOwn(saved.weights,a.id)?saved.weights[a.id]:0]))};
  }
  const previousSettings=readSetting('balancing',{});
  // Older manual routes had no deadline. Recover only human actions, not automatic moves.
  const legacy=store.all("SELECT s.id,s.created FROM sessions s JOIN accounts a ON a.id=s.account_id WHERE s.nightly_enabled=0 AND s.nightly_expires IS NULL AND a.auth_mode='subscription'");
  if(legacy.length)atomic(()=>{
    const actions=new Map();
    for(const row of store.all("SELECT at,action,detail FROM audit WHERE action IN ('session.route','session.nightly') ORDER BY at")){
      let detail;try{detail=JSON.parse(row.detail);}catch{continue;}
      if(row.action==='session.route'&&!detail.reason||row.action==='session.nightly'&&detail.enabled===false){const sid=detail.session||detail.sessionId;actions.set(sid,Math.max(actions.get(sid)||0,row.at));}
    }
    for(const s of legacy){const expires=nextDailyReset(actions.get(s.id)??s.created);store.run('UPDATE sessions SET nightly_expires=? WHERE id=?',expires,s.id);store.audit('session.nightly.deadline',JSON.stringify({session:s.id,expires}));}
  });
  if(Object.hasOwn(previousSettings,'weighted'))atomic(()=>{const {weighted,...legacy}=previousSettings;if(!store.get("SELECT name FROM gateway_settings WHERE name='routing_strategy'"))writeSetting('routing_strategy',{weighted});writeSetting('balancing',legacy);});
  function saveWeights(value){
    if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!['mode','weights','expected_version'].includes(k))||!['auto','manual'].includes(value.mode)||!Number.isSafeInteger(value.expected_version))throw new AppError(400,'权重设置无效');
    return atomic(()=>{
      const current=manual();if(value.expected_version!==current.version)throw new AppError(409,'权重已变化，请刷新后重试');
      let weights={};
      if(value.mode==='manual'){
        weights=value.weights;
        const ids=new Set(store.all("SELECT id FROM accounts WHERE kind='codex' AND auth_mode='subscription' AND removed_at IS NULL").map(a=>a.id));
        if(!weights||typeof weights!=='object'||Array.isArray(weights)||!Object.keys(weights).length||Object.entries(weights).some(([key,v])=>!ids.has(key)||!Number.isFinite(v)||v<0||v>100||Math.abs(v*100-Math.round(v*100))>1e-8)||Math.abs(Object.values(weights).reduce((n,v)=>n+v,0)-100)>1e-8)throw new AppError(400,'请填写有效账号权重，最多两位小数且总和必须为100%');
      }else if(value.weights!==undefined)throw new AppError(400,'自动模式无需填写权重');
      const next={mode:value.mode,weights,expires:value.mode==='manual'?nextWeightReset(clock()):null,version:current.version+1};
      writeSetting('routing_weights',next);if(value.mode==='auto')syncWeights(undefined,clock(),'manual');store.audit('balancing.weights',JSON.stringify({...next,previous:current}));return next;
    });
  }
  function distribution(accounts=store.all("SELECT * FROM accounts WHERE kind='codex' AND removed_at IS NULL")){
    const subscriptions=accounts.filter(a=>!isApi(a)),at=clock(),weighted=settings().weighted;
    const system=weighted?syncWeights(subscriptions,at):automaticWeights(subscriptions,at),configured=manual(),active=configured.mode==='manual'&&configured.expires>at;
    const subscriptionMode=!weighted?'legacy':active?'manual':system.eligible.some(a=>system.weights.get(a.id)===null)?'fallback':'auto';
    const subEligible=subscriptionMode==='legacy'||subscriptionMode==='manual'?subscriptions.filter(available):system.eligible;
    const desired=a=>['legacy','fallback'].includes(subscriptionMode)?1:subscriptionMode==='manual'?configured.weights[a.id]||0:system.weights.get(a.id)||0;
    const sum=subEligible.reduce((n,a)=>n+desired(a),0),apis=accounts.filter(a=>isApi(a)&&available(a)&&!a.route_block&&a.api_weight>0),apiShare=apis.reduce((n,a)=>n+a.api_weight,0);
    const weights=new Map(accounts.map(a=>[a.id,apis.includes(a)?a.api_weight:subEligible.includes(a)&&sum>0?Math.max(0,100-apiShare)*desired(a)/sum:0]));
    const eligible=[...subEligible,...apis],total=eligible.reduce((n,a)=>n+weights.get(a.id),0);
    if(total>0)for(const a of eligible)weights.set(a.id,weights.get(a.id)/total*100);
    const mode=apis.length?'api_mixed':subscriptionMode;
    return {mode,subscriptionMode,reason:system.reason,eligible,weights,system,configuration:{...configured,...(!active?{mode:'auto',expires:null}:{})},accounts:accounts.map(a=>({id:a.id,automatic:isApi(a)?null:system.weights.get(a.id),effective:isApi(a)?weights.get(a.id):['legacy','fallback'].includes(mode)?null:weights.get(a.id),configured:isApi(a)?a.api_weight:active?configured.weights[a.id]??0:null,manualApi:isApi(a)}))};
  }
  function allocation(state){
    const pool=state.eligible;
    if(['legacy','fallback'].includes(state.mode))return {selected:[...pool].sort((a,b)=>a.last_used-b.last_used||a.id.localeCompare(b.id))[0]};
    const signature=JSON.stringify([state.mode,state.mode==='manual'?state.configuration.version:0,pool.map(a=>a.id).sort()]),previous=readSetting('routing_schedule',{});
    const credits=Object.fromEntries(pool.map(a=>[a.id,previous.signature===signature?previous.credits?.[a.id]||0:0]));
    const mean=Object.values(credits).reduce((n,v)=>n+v,0)/pool.length;
    for(const a of pool)credits[a.id]=credits[a.id]-mean+state.weights.get(a.id)/100;
    const candidates=pool.filter(a=>state.weights.get(a.id)>0).sort((a,b)=>credits[b.id]-credits[a.id]||a.id.localeCompare(b.id));
    const selected=candidates[0];
    if(selected)credits[selected.id]-=1;
    return {selected,schedule:{signature,credits}};
  }
  function selectAccount({protocol='responses'}={}){
    const state=distribution(protocol==='responses'?undefined:store.all("SELECT * FROM accounts WHERE kind='codex' AND auth_mode='subscription' AND removed_at IS NULL"));
    if(!state.eligible.length)throw new AppError(503,'所有账号均不可用，请等待额度恢复或重新登录','no_account');
    const result=allocation(state);
    if(!result.selected)throw new AppError(503,'没有生效权重大于0的可用账号，请调整权重或等待额度恢复','no_account');
    if(result.schedule)writeSetting('routing_schedule',result.schedule);
    return result.selected;
  }
  function disableAccount(accountId){
    return atomic(()=>{
      const account=store.get('SELECT * FROM accounts WHERE id=? AND removed_at IS NULL',accountId);
      if(!account)throw new AppError(404,'账号不存在或已移除');
      const sessions=store.all('SELECT * FROM sessions WHERE account_id=? ORDER BY id',accountId);
      if(account.enabled||sessions.length)store.run('UPDATE accounts SET enabled=0,route_epoch=route_epoch+1 WHERE id=?',accountId);
      const moved=[];
      for(const session of sessions){
        let target;
        try{target=account.kind==='codex'?selectAccount({protocol:session.protocol}):store.get("SELECT a.* FROM accounts a WHERE a.kind=? AND a.enabled=1 AND a.removed_at IS NULL AND a.state IN ('ready','degraded') ORDER BY (SELECT COUNT(*) FROM sessions s WHERE s.account_id=a.id),a.last_used,a.id LIMIT 1",account.kind);}
        catch(e){if(e.code!=='no_account')throw e;throw new AppError(409,'没有可接收会话的同类型正权重账号，未停用或迁移任何会话','disable_no_target');}
        if(!target)throw new AppError(409,'没有可接收会话的同类型账号，未停用或迁移任何会话','disable_no_target');
        const legacy=session.protocol!=='responses',expires=isApi(account)&&!isApi(target)&&!session.nightly_enabled&&session.nightly_expires===null?nextWeightReset(clock()):session.nightly_expires;
        store.run(`UPDATE sessions SET account_id=?,upstream_thread=?,route_version=route_version+1,nightly_expires=?,updated=?${legacy?",upstream_id=NULL,state='new',message_count=0,history_digest='',cum_input=0,cum_output=0,cum_cached=0":''} WHERE id=?`,target.id,id(),expires,clock(),session.id);
        store.audit('session.route',JSON.stringify({sessionId:session.id,from:accountId,to:target.id,reason:'account_disabled',version:session.route_version+1}));
        moved.push(session.id);
      }
      return {moved:moved.length,resetTextSessions:sessions.filter(s=>s.protocol!=='responses').length};
    });
  }
  function explain(){
    const accounts=store.all("SELECT * FROM accounts WHERE kind='codex' AND removed_at IS NULL"),state=distribution(accounts),next=allocation(state).selected;
    return {at:clock(),mode:state.mode,reason:state.reason,configuration:state.configuration,basis:state.system.basis??null,updated:state.system.updated??null,trigger:state.system.trigger??null,nextAccount:next?.id??null,
      accounts:accounts.map(a=>({...state.accounts.find(x=>x.id===a.id),name:a.name,reason:!a.enabled?'disabled':isApi(a)&&!a.route_block&&!(a.api_weight>0)?'manual_binding_only':a.route_block==='login_expired'?'login_expired':!available(a)?'not_ready':!state.eligible.some(x=>x.id===a.id)?'quota_exhausted':!['legacy','fallback'].includes(state.mode)&&!(state.weights.get(a.id)>0)?'zero_weight':'eligible',extraQuota:a.route_block==='quota_exhausted'&&state.mode==='manual'&&state.weights.get(a.id)>0}))};
  }
  const busy=s=>preparing.has(s.member_id)||!!store.get("SELECT id FROM requests WHERE session_id=? AND status IN ('queued','running')",s.id);
  function move(session,target,reason){
    const current=store.get('SELECT s.*,m.enabled member_enabled,m.removed_at member_removed FROM sessions s JOIN members m ON m.id=s.member_id WHERE s.id=?',session.id);
    if(!current||!current.enabled||!current.member_enabled||current.member_removed!==null||current.protocol!=='responses')throw new AppError(409,'会话当前不可迁移','session_disabled');
    if(current.route_version!==session.route_version)throw new AppError(409,'会话绑定已变化','session_route_conflict');
    if(busy(current))throw new AppError(409,'会话仍有请求处理中，暂不能切换','session_busy');
    const account=store.get("SELECT * FROM accounts WHERE id=? AND enabled=1 AND removed_at IS NULL AND kind='codex' AND state IN ('ready','degraded') AND (route_block IS NULL OR route_block='quota_exhausted')",target);
    if(!account)throw new AppError(503,'没有可用的目标账号','no_account');
    if(current.account_id===target)return current;
    return atomic(()=>{
      const source=store.get('SELECT auth_mode FROM accounts WHERE id=?',current.account_id),expires=isApi(source)&&!isApi(account)&&!current.nightly_enabled&&current.nightly_expires===null?nextWeightReset(clock()):current.nightly_expires;
      store.run('UPDATE sessions SET account_id=?,upstream_thread=?,route_version=route_version+1,nightly_expires=? WHERE id=?',target,id(),expires,current.id);
      store.audit('session.route',JSON.stringify({session:current.id,from:current.account_id,to:target,reason}));return store.get('SELECT * FROM sessions WHERE id=?',current.id);
    });
  }
  function block(account,reason,{quotaRefresh=false}={}){
    if(!['quota_exhausted','login_expired'].includes(reason))return;
    return atomic(()=>{
      const current=store.get('SELECT * FROM accounts WHERE id=?',account.id);
      if(!current||current.route_epoch!==account.route_epoch)return;
      if(quotaRefresh&&current.route_block===reason||reason==='quota_exhausted'&&current.route_block==='login_expired')return;
      store.run('UPDATE accounts SET route_block=?,route_epoch=route_epoch+1,route_blocked_at=? WHERE id=?',reason,clock(),account.id);
      store.audit('account.route_block',JSON.stringify({account:account.id,reason}));
      if(settings().weighted)syncWeights();
    });
  }
  function recover(account,snapshot){
    if(!account.route_block||!snapshot||!snapshot.windows?.length||snapshot.windows.some(w=>!w.reset||w.reset<=clock()))return;
    const exhausted=snapshot.limitReached||snapshot.remaining<=0;if(exhausted&&account.route_block!=='login_expired')return;
    return atomic(()=>{
      const result=store.run('UPDATE accounts SET route_block=?,route_epoch=route_epoch+1,route_blocked_at=? WHERE id=? AND route_epoch=? AND route_block IS NOT NULL',exhausted?'quota_exhausted':null,exhausted?clock():null,account.id,account.route_epoch);
      if(result.changes){store.audit('account.route_recover',account.id);syncWeights(undefined,clock(),'recovery');}
      return result.changes>0;
    });
  }
  function route(session,{protocol='responses'}={}){
    const account=store.get('SELECT * FROM accounts WHERE id=?',session.account_id);
    if(protocol!=='responses'&&isApi(account))throw new AppError(400,'官方Key接入暂仅支持Responses会话','unsupported_api_route');
    if(isApi(account)&&account.enabled&&!account.removed_at&&['ready','degraded'].includes(account.state)&&!account.route_block)return session;
    const blocked=account?.route_block&&(isApi(account)||account.route_block!=='quota_exhausted');
    if(blocked&&!settings().failover)throw new AppError(503,'绑定账号已阻断，自动切换未开启','session_account_blocked');
    if(!blocked){
      if(!settings().weighted)return session;
      const state=distribution(protocol==='responses'?undefined:store.all("SELECT * FROM accounts WHERE kind='codex' AND auth_mode='subscription' AND removed_at IS NULL"));if(state.mode==='fallback'?state.eligible.some(a=>a.id===account.id):state.weights.get(account.id)>0)return session;
    }
    return atomic(()=>move(session,selectAccount({protocol}).id,blocked?account.route_block:'weight_zero'));
  }
  async function plan(at,preview=false){
    const accounts=store.all("SELECT * FROM accounts WHERE kind='codex' AND auth_mode='subscription' AND enabled=1 AND removed_at IS NULL");
    const refreshed=await Promise.all(accounts.map(a=>Promise.resolve().then(()=>preview?readQuota(a):refreshQuota(a,true)).catch(()=>null)));
    const freshById=new Map(accounts.map((a,index)=>{const current=store.get('SELECT quota_json,route_epoch FROM accounts WHERE id=?',a.id);return [a.id,{snapshot:refreshed[index],epoch:!preview&&refreshed[index]&&current.quota_json===JSON.stringify(refreshed[index])?current.route_epoch:a.route_epoch}];}));
    if(stopping())throw new AppError(503,'服务正在停止');
    const planningAccounts=store.all('SELECT * FROM accounts').map(a=>({...a,quota_json:freshById.get(a.id)?.epoch===a.route_epoch&&freshById.get(a.id)?.snapshot?JSON.stringify(freshById.get(a.id).snapshot):null}));
    if(!preview&&settings().weighted)syncWeights(planningAccounts,clock(),'nightly');
    return planBalance({accounts:planningAccounts,sessions:store.all('SELECT s.*,m.enabled member_enabled,m.removed_at member_removed FROM sessions s JOIN members m ON m.id=s.member_id'),requests:store.all("SELECT * FROM requests WHERE COALESCE(ended,started)>=? OR status IN ('queued','running')",nightlyWindow(at).start),at:clock(),quietMinutes:settings().quietMinutes,weighted:settings().weighted,preparing,quoteUsage:(r,revision)=>store.pricing.quoteAt(r,revision)});
  }
  async function tick(){
    const at=clock(),window=nightlyWindow(at);
    if(stopping())return;
    atomic(()=>{
      for(const s of store.all('SELECT id,nightly_expires FROM sessions WHERE nightly_enabled=0 AND nightly_expires<=?',at)){
        store.run('UPDATE sessions SET nightly_enabled=1,nightly_expires=NULL WHERE id=?',s.id);
        store.audit('session.nightly.expired',JSON.stringify({session:s.id,expires:s.nightly_expires}));
      }
    });
    const configured=manual();if(configured.mode==='manual'&&configured.expires<=at){writeSetting('routing_weights',{mode:'auto',weights:{},expires:null,version:configured.version+1});store.audit('balancing.weights.expired',String(configured.expires));}
    if(settings().weighted&&(!settings().nightly||!window.due))syncWeights();
    if(stopping()||running||!settings().nightly||!window.due)return;
    const claim=store.run("INSERT OR IGNORE INTO balance_runs(day,started,status) VALUES(?,?,'running')",window.key,at);
    if(!claim.changes)return;
    running=(async()=>{
      try{
        const result=await plan(at),applied=[];
        for(const group of result.summaries){
          if(stopping()||!settings().nightly)break;
          const ids=new Set(group.accounts),items=result.moves.filter(item=>ids.has(item.from));
          if(!items.length)continue;
          // Smaller combinations can require both directions; never publish only half a group.
          try{
            const committed=atomic(()=>{
              const moved=[];
              for(const item of items){
                if(stopping()||!settings().nightly)throw new AppError(409,'夜间计划条件已变化','nightly_plan_changed');
                const session=store.get('SELECT * FROM sessions WHERE id=?',item.session_id),target=store.get('SELECT * FROM accounts WHERE id=?',item.to),source=store.get('SELECT * FROM accounts WHERE id=?',item.from);
                const latest=store.get('SELECT MAX(MAX(started,COALESCE(ended,started))) last FROM requests WHERE session_id=?',item.session_id)?.last,targetQuota=quotaSnapshot(target,clock());
                if(!session||!participatesNightly(session,clock())||session.route_version!==item.version||session.account_id!==item.from||latest>clock()-settings().quietMinutes*minute||!targetQuota||targetQuota.limitReached||targetQuota.remaining<=0||!quotaSnapshot(source,clock())||source.route_block&&source.route_block!=='quota_exhausted')throw new AppError(409,'夜间计划条件已变化','nightly_plan_changed');
                move(session,item.to,'nightly');moved.push(item.session_id);
              }
              const committedAt=clock();if(group.accounts.some(account=>!quotaSnapshot(store.get('SELECT * FROM accounts WHERE id=?',account),committedAt)))throw new AppError(409,'夜间计划额度已变化','nightly_plan_changed');
              return moved;
            });
            for(const session of committed)applied.push(session);
          }catch(e){if(!(e instanceof AppError))throw e;for(const item of items)result.skipped.push({session_id:item.session_id,reason:e.code});}
        }
        store.run("UPDATE balance_runs SET ended=?,status='completed',summary=? WHERE day=?",clock(),JSON.stringify({...result,applied}),window.key);
      }catch{store.run("UPDATE balance_runs SET ended=?,status='failed',summary=? WHERE day=?",clock(),JSON.stringify({reason:'planning_failed'}),window.key);}
    })().finally(()=>{running=null;});
    return running;
  }
  return {settings,saveSettings,saveWeights,selectAccount,disableAccount,explain,atomic,distribution,syncWeights,move,block,recover,route,tick,preview:()=>plan(clock(),true),pending:()=>running,status:()=>{const state=distribution();return {settings:settings(),weights:{mode:state.mode,reason:state.reason,configuration:state.configuration,accounts:state.accounts},lastRun:store.get('SELECT * FROM balance_runs ORDER BY day DESC LIMIT 1')||null};}};
}
