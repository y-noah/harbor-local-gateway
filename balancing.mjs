import {AppError,id} from './store.mjs';

const minute=60000,day=24*60*minute,nightlyTolerance=0.02,nightlySearchBudget=100000;
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
  return {...q,remaining:Math.min(...q.windows.map(w=>100-w.used)),group:JSON.stringify([q.plan,q.windows.map(w=>w.minutes).sort((a,b)=>a-b)])};
}
const available=a=>a.enabled&&!a.removed_at&&a.kind==='codex'&&['ready','degraded'].includes(a.state)&&!a.route_block;
export function automaticWeights(accounts,at){
  const snapshots=new Map(accounts.map(a=>[a.id,quotaSnapshot(a,at)]));
  const eligible=accounts.filter(a=>available(a)&&!(snapshots.get(a.id)?.limitReached||snapshots.get(a.id)?.remaining===0));
  let reason=null;
  if(eligible.some(a=>!snapshots.get(a.id)))reason='quota_unavailable';
  else if(new Set(eligible.map(a=>snapshots.get(a.id).group)).size>1||eligible.some(a=>snapshots.get(a.id).windows.length!==1))reason='incomparable_quota';
  const scores=new Map(eligible.map(a=>{const q=snapshots.get(a.id);return [a.id,reason?0:q.remaining/Math.max((q.windows[0].reset-at)/3600000,24)];}));
  const total=[...scores.values()].reduce((n,v)=>n+v,0);
  return {eligible,snapshots,reason,weights:new Map(accounts.map(a=>[a.id,eligible.includes(a)?reason?null:scores.get(a.id)/total*100:0]))};
}
export function nextWeightReset(at){const end=nightlyWindow(at).end;return end>at?end:end+day;}
const median=values=>{if(!values.length)return null;const sorted=[...values].sort((a,b)=>a-b),n=sorted.length;return n%2?sorted[(n-1)/2]:(sorted[n/2-1]+sorted[n/2])/2;};
const withinTolerance=(loads,target,total)=>[...target].every(([account,weight])=>Math.abs(loads.get(account)-weight)<=total*(nightlyTolerance+Number.EPSILON));

// Prefer fewer transfers without an unbounded combinatorial search on the request thread.
function minimizeTransfers(pool,movable,before,target,total,proposed){
  const loads=new Map(Object.entries(before)),slack=total*(nightlyTolerance+Number.EPSILON);
  if(withinTolerance(loads,target,total))return [];
  const movableTotal=movable.reduce((sum,s)=>sum+s.weight,0);
  if(pool.some(a=>{
    const fixed=before[a.id]-movable.filter(s=>s.account_id===a.id).reduce((sum,s)=>sum+s.weight,0);
    return fixed>target.get(a.id)+slack||fixed+movableTotal<target.get(a.id)-slack;
  }))return proposed;
  const proposedLoads=new Map(loads);
  for(const m of proposed){proposedLoads.set(m.from,proposedLoads.get(m.from)-m.weight);proposedLoads.set(m.to,proposedLoads.get(m.to)+m.weight);}
  const limit=withinTolerance(proposedLoads,target,total)?proposed.length-1:movable.length;
  let budget=nightlySearchBudget;
  const selected=[],prefix=[0];for(const s of movable)prefix.push(prefix.at(-1)+s.weight);
  function search(count){
    const stack=[{start:0,left:count}];
    const leave=()=>{const frame=stack.pop();if(frame.restore){const r=frame.restore;loads.set(r.from,r.fromLoad);loads.set(r.to,r.toLoad);selected.pop();}};
    try{
      while(stack.length&&budget>0){
        const frame=stack.at(-1);
        if(frame.index===undefined){
          budget--;
          if(!frame.left){if(withinTolerance(loads,target,total))return [...selected];leave();continue;}
          if(movable.length-frame.start<frame.left){leave();continue;}
          const capacity=prefix[frame.start+frame.left]-prefix[frame.start];
          if(pool.some(a=>Math.abs(loads.get(a.id)-target.get(a.id))>capacity+slack)){leave();continue;}
          frame.index=frame.start;frame.destination=0;
        }
        if(frame.index>movable.length-frame.left){leave();continue;}
        const index=frame.index,s=movable[index],a=pool[frame.destination++];
        if(frame.destination===pool.length){frame.index++;frame.destination=0;}
        if(a.id===s.account_id||target.get(a.id)<=0)continue;
        const fromLoad=loads.get(s.account_id),toLoad=loads.get(a.id);
        loads.set(s.account_id,fromLoad-s.weight);loads.set(a.id,toLoad+s.weight);
        selected.push({session_id:s.id,from:s.account_id,to:a.id,version:s.route_version,weight:s.weight});
        stack.push({start:index+1,left:frame.left-1,restore:{from:s.account_id,to:a.id,fromLoad,toLoad}});
      }
      return null;
    }finally{while(stack.length)leave();}
  }
  for(let count=1;count<=limit&&budget>0;count++){const found=search(count);if(found)return found;}
  return proposed;
}

// Workload is a relative estimate, never a quota admission check.
export function planBalance({accounts,sessions,requests,at,quietMinutes=30,preparing=new Set(),weighted=false}){
  const window=nightlyWindow(at),eligible=accounts.filter(a=>a.enabled&&!a.removed_at&&a.kind==='codex'&&['ready','degraded'].includes(a.state)&&!a.route_block);
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
  for(const rows of bySession.values())for(const r of rows)if(r.ended&&!r.usage_unknown&&r.input+r.output>0){
    const key=r.model||'';if(!samples.has(key))samples.set(key,[]);samples.get(key).push(r.input+r.output);
  }
  const uncertainAccounts=new Set(),skipped=[];
  for(const s of sessions){
    const rows=bySession.get(s.id);if(!rows)continue;
    let weight=0,unknown=false;
    for(const r of rows){
      if(r.ended&&!r.usage_unknown){weight+=r.input+r.output;continue;}
      const estimate=median(samples.get(r.model||'')||[]);
      if(estimate===null)unknown=true;else weight+=estimate;
    }
    if(weight===0&&!unknown){skipped.push({session_id:s.id,reason:'no_measured_work'});continue;}
    const running=rows.some(r=>['queued','running'].includes(r.status));
    const last=latest.get(s.id);
    const fixed=running||rows.some(r=>r.started>=window.end)||preparing.has(s.member_id)||at-last<quietMinutes*minute||!s.nightly_enabled||!s.enabled||!s.member_enabled||s.member_removed!==null||s.protocol!=='responses'||unknown;
    if(unknown)uncertainAccounts.add(s.account_id);
    active.set(s.id,{...s,weight,fixed,unknown});
    if(fixed)skipped.push({session_id:s.id,reason:unknown?'unknown_workload':running?'busy':'protected'});
  }
  const moves=[],groups=new Map();
  for(const a of eligible){const q=snapshots.get(a.id);if(!q||q.limitReached){skipped.push({account_id:a.id,reason:'quota_unavailable'});continue;}if(!groups.has(q.group))groups.set(q.group,[]);groups.get(q.group).push(a);}
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
    const before=Object.fromEntries(loads),groupMoves=[];
    const movable=work.filter(s=>!s.fixed).sort((a,b)=>b.weight-a.weight||a.id.localeCompare(b.id));
    for(const s of movable){
      if(withinTolerance(loads,target,total))break;
      const origin=s.account_id,from=loads.get(origin)-target.get(origin);
      let best=null,benefit=0;
      for(const a of pool){
        if(a.id===origin)continue;
        const to=loads.get(a.id)-target.get(a.id);
        const improvement=Math.abs(from)+Math.abs(to)-Math.abs(from-s.weight)-Math.abs(to+s.weight);
        if(improvement-benefit>Number.EPSILON*total){benefit=improvement;best=a.id;}
      }
      if(best){loads.set(origin,loads.get(origin)-s.weight);loads.set(best,loads.get(best)+s.weight);groupMoves.push({session_id:s.id,from:origin,to:best,version:s.route_version,weight:s.weight});}
    }
    // Cancel any transfer that is unnecessary for tolerance, or improves an infeasible plan.
    const deviation=()=>pool.reduce((sum,a)=>sum+Math.abs(loads.get(a.id)-target.get(a.id)),0);
    for(let i=groupMoves.length-1;i>=0;i--){const m=groupMoves[i],balanced=withinTolerance(loads,target,total),beforeDeviation=deviation();loads.set(m.from,loads.get(m.from)+m.weight);loads.set(m.to,loads.get(m.to)-m.weight);if(balanced?withinTolerance(loads,target,total):deviation()<=beforeDeviation+Number.EPSILON*total)groupMoves.splice(i,1);else{loads.set(m.from,loads.get(m.from)-m.weight);loads.set(m.to,loads.get(m.to)+m.weight);}}
    const selected=minimizeTransfers(pool,movable,before,target,total,groupMoves);
    for(const a of pool)loads.set(a.id,before[a.id]);
    for(const m of selected){loads.set(m.from,loads.get(m.from)-m.weight);loads.set(m.to,loads.get(m.to)+m.weight);}
    for(const m of selected)moves.push(m);
    summaries.push({accounts:pool.map(a=>a.id),weights:Object.fromEntries(shares),before,after:Object.fromEntries(loads),target:Object.fromEntries(target),fixed:work.filter(s=>s.fixed).map(s=>s.id),fixedLoad:Object.fromEntries(pool.map(a=>[a.id,work.filter(s=>s.fixed&&s.account_id===a.id).reduce((sum,s)=>sum+s.weight,0)]))});
  }
  return {window,moves,skipped,summaries};
}

export function createBalancer({store,preparing,refreshQuota,readQuota=async()=>null,clock=Date.now,stopping=()=>false}){
  let running=null;
  const settings=()=>balanceSettings({...readSetting('balancing',{}),weighted:readSetting('routing_strategy',{weighted:false}).weighted});
  const saveSettings=value=>{balanceSettings(value);return atomic(()=>{const next=balanceSettings({...settings(),...value}),{weighted,...legacy}=next;writeSetting('balancing',legacy);writeSetting('routing_strategy',{weighted});store.audit('balancing.settings',JSON.stringify(next));return next;});};
  const readSetting=(name,fallback)=>JSON.parse(store.get('SELECT value FROM gateway_settings WHERE name=?',name)?.value||JSON.stringify(fallback));
  const writeSetting=(name,value)=>store.run('INSERT INTO gateway_settings(name,value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=excluded.value',name,JSON.stringify(value));
  const manual=()=>readSetting('routing_weights',{mode:'auto',weights:{},expires:null,version:0});
  const atomic=fn=>{if(store.db.isTransaction)return fn();store.db.exec('BEGIN IMMEDIATE');try{const result=fn();store.db.exec('COMMIT');return result;}catch(e){store.db.exec('ROLLBACK');throw e;}};
  const previousSettings=readSetting('balancing',{});
  if(Object.hasOwn(previousSettings,'weighted'))atomic(()=>{const {weighted,...legacy}=previousSettings;if(!store.get("SELECT name FROM gateway_settings WHERE name='routing_strategy'"))writeSetting('routing_strategy',{weighted});writeSetting('balancing',legacy);});
  function saveWeights(value){
    if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!['mode','weights','expected_version'].includes(k))||!['auto','manual'].includes(value.mode)||!Number.isSafeInteger(value.expected_version))throw new AppError(400,'权重设置无效');
    return atomic(()=>{
      const current=manual();if(value.expected_version!==current.version)throw new AppError(409,'权重已变化，请刷新后重试');
      let weights={};
      if(value.mode==='manual'){
        weights=value.weights;
        const ids=new Set(store.all("SELECT id FROM accounts WHERE kind='codex' AND removed_at IS NULL").map(a=>a.id));
        if(!weights||typeof weights!=='object'||Array.isArray(weights)||!Object.keys(weights).length||Object.entries(weights).some(([key,v])=>!ids.has(key)||!Number.isFinite(v)||v<0||v>100||Math.abs(v*100-Math.round(v*100))>1e-8)||Math.abs(Object.values(weights).reduce((n,v)=>n+v,0)-100)>1e-8)throw new AppError(400,'请填写有效账号权重，最多两位小数且总和必须为100%');
      }else if(value.weights!==undefined)throw new AppError(400,'自动模式无需填写权重');
      const next={mode:value.mode,weights,expires:value.mode==='manual'?nextWeightReset(clock()):null,version:current.version+1};
      writeSetting('routing_weights',next);store.audit('balancing.weights',JSON.stringify(next));return next;
    });
  }
  function distribution(accounts=store.all("SELECT * FROM accounts WHERE kind='codex' AND removed_at IS NULL")){
    const at=clock(),system=automaticWeights(accounts,at),configured=manual(),active=configured.mode==='manual'&&configured.expires>at;
    let mode=settings().weighted?(system.reason?'fallback':'auto'):'legacy',weights=new Map(system.weights);
    if(settings().weighted&&active){
      const sum=system.eligible.reduce((n,a)=>n+(configured.weights[a.id]||0),0);
      if(sum>0){mode='manual';weights=new Map(accounts.map(a=>[a.id,system.eligible.includes(a)?(configured.weights[a.id]||0)/sum*100:0]));}
      else mode=system.reason?'fallback':'failover';
    }
    return {mode,reason:system.reason,eligible:mode==='legacy'?accounts.filter(available):system.eligible,weights,system,configuration:{...configured,...(!active?{mode:'auto',expires:null}:{})},accounts:accounts.map(a=>({id:a.id,automatic:system.weights.get(a.id),effective:['legacy','fallback'].includes(mode)?null:weights.get(a.id),configured:active?configured.weights[a.id]??0:null}))};
  }
  function selectAccount(){
    const state=distribution(),pool=state.eligible;
    if(!pool.length)throw new AppError(503,'所有账号均不可用，请等待额度恢复或重新登录','no_account');
    if(['legacy','fallback'].includes(state.mode))return [...pool].sort((a,b)=>a.last_used-b.last_used||a.id.localeCompare(b.id))[0];
    const signature=JSON.stringify([state.mode,state.mode==='manual'?state.configuration.version:0,pool.map(a=>a.id).sort()]),previous=readSetting('routing_schedule',{});
    const credits=Object.fromEntries(pool.map(a=>[a.id,previous.signature===signature?previous.credits?.[a.id]||0:0]));
    const mean=Object.values(credits).reduce((n,v)=>n+v,0)/pool.length;
    for(const a of pool)credits[a.id]=credits[a.id]-mean+state.weights.get(a.id)/100;
    const candidates=pool.filter(a=>state.weights.get(a.id)>0).sort((a,b)=>credits[b.id]-credits[a.id]||a.id.localeCompare(b.id));
    const selected=candidates[0];credits[selected.id]-=1;writeSetting('routing_schedule',{signature,credits});return selected;
  }
  const busy=s=>preparing.has(s.member_id)||!!store.get("SELECT id FROM requests WHERE session_id=? AND status IN ('queued','running')",s.id);
  function move(session,target,reason){
    const current=store.get('SELECT s.*,m.enabled member_enabled,m.removed_at member_removed FROM sessions s JOIN members m ON m.id=s.member_id WHERE s.id=?',session.id);
    if(!current||!current.enabled||!current.member_enabled||current.member_removed!==null||current.protocol!=='responses')throw new AppError(409,'会话当前不可迁移','session_disabled');
    if(current.route_version!==session.route_version)throw new AppError(409,'会话绑定已变化','session_route_conflict');
    if(busy(current))throw new AppError(409,'会话仍有请求处理中，暂不能切换','session_busy');
    const account=store.get("SELECT * FROM accounts WHERE id=? AND enabled=1 AND removed_at IS NULL AND kind='codex' AND state IN ('ready','degraded') AND route_block IS NULL",target);
    if(!account)throw new AppError(503,'没有可用的目标账号','no_account');
    if(current.account_id===target)return current;
    return atomic(()=>{
      store.run('UPDATE sessions SET account_id=?,upstream_thread=?,route_version=route_version+1 WHERE id=?',target,id(),current.id);
      store.audit('session.route',JSON.stringify({session:current.id,from:current.account_id,to:target,reason}));return store.get('SELECT * FROM sessions WHERE id=?',current.id);
    });
  }
  function block(account,reason){
    if(!['quota_exhausted','login_expired'].includes(reason))return;
    const current=store.get('SELECT * FROM accounts WHERE id=?',account.id);
    if(!current||current.route_epoch!==account.route_epoch)return;
    store.run('UPDATE accounts SET route_block=?,route_epoch=route_epoch+1,route_blocked_at=? WHERE id=?',reason,clock(),account.id);
    store.audit('account.route_block',JSON.stringify({account:account.id,reason}));
  }
  function recover(account,snapshot){
    if(!account.route_block||!snapshot||snapshot.limitReached||snapshot.remaining<=0||snapshot.windows.some(w=>w.reset&&w.reset<=clock()))return;
    const result=store.run('UPDATE accounts SET route_block=NULL,route_epoch=route_epoch+1,route_blocked_at=NULL WHERE id=? AND route_epoch=? AND route_block IS NOT NULL',account.id,account.route_epoch);
    if(result.changes)store.audit('account.route_recover',account.id);
  }
  function route(session){
    let account=store.get('SELECT * FROM accounts WHERE id=?',session.account_id);
    const snapshot=quotaSnapshot(account,clock());
    if(!account?.route_block&&settings().weighted&&settings().failover&&snapshot&&(snapshot.limitReached||snapshot.remaining===0)){block(account,'quota_exhausted');account=store.get('SELECT * FROM accounts WHERE id=?',session.account_id);}
    if(!account?.route_block)return session;
    if(!settings().failover)throw new AppError(503,'绑定账号已阻断，自动切换未开启','session_account_blocked');
    return atomic(()=>move(session,selectAccount().id,account.route_block));
  }
  async function plan(at,preview=false){
    const accounts=store.all("SELECT * FROM accounts WHERE kind='codex' AND enabled=1 AND removed_at IS NULL");
    const refreshed=await Promise.all(accounts.map(a=>Promise.resolve().then(()=>preview?readQuota(a):refreshQuota(a,true)).catch(()=>null)));
    const freshById=new Map(accounts.map((a,index)=>{const current=store.get('SELECT quota_json,route_epoch FROM accounts WHERE id=?',a.id);return [a.id,{snapshot:refreshed[index],epoch:!preview&&refreshed[index]&&current.quota_json===JSON.stringify(refreshed[index])?current.route_epoch:a.route_epoch}];}));
    if(stopping())throw new AppError(503,'服务正在停止');
    return planBalance({accounts:store.all('SELECT * FROM accounts').map(a=>({...a,quota_json:freshById.get(a.id)?.epoch===a.route_epoch&&freshById.get(a.id)?.snapshot?JSON.stringify(freshById.get(a.id).snapshot):null})),sessions:store.all('SELECT s.*,m.enabled member_enabled,m.removed_at member_removed FROM sessions s JOIN members m ON m.id=s.member_id'),requests:store.all("SELECT * FROM requests WHERE COALESCE(ended,started)>=? OR status IN ('queued','running')",nightlyWindow(at).start),at:clock(),quietMinutes:settings().quietMinutes,weighted:settings().weighted,preparing});
  }
  async function tick(){
    const at=clock(),window=nightlyWindow(at);
    const configured=manual();if(configured.mode==='manual'&&configured.expires<=at){writeSetting('routing_weights',{mode:'auto',weights:{},expires:null,version:configured.version+1});store.audit('balancing.weights.expired',String(configured.expires));}
    if(stopping()||running||!settings().nightly||!window.due)return;
    const claim=store.run("INSERT OR IGNORE INTO balance_runs(day,started,status) VALUES(?,?,'running')",window.key,at);
    if(!claim.changes)return;
    running=(async()=>{
      try{
        const result=await plan(at),applied=[];
        for(const item of result.moves){
          if(stopping()||!settings().nightly)break;
          const session=store.get('SELECT * FROM sessions WHERE id=?',item.session_id),target=store.get('SELECT * FROM accounts WHERE id=?',item.to),source=store.get('SELECT * FROM accounts WHERE id=?',item.from);
          const latest=store.get('SELECT MAX(MAX(started,COALESCE(ended,started))) last FROM requests WHERE session_id=?',item.session_id)?.last;
          if(!session?.nightly_enabled||session.route_version!==item.version||session.account_id!==item.from||latest>clock()-settings().quietMinutes*minute||!quotaSnapshot(target,clock())||!quotaSnapshot(source,clock())||source.route_block)continue;
          try{move(session,item.to,'nightly');applied.push(item.session_id);}catch(e){if(!(e instanceof AppError))throw e;result.skipped.push({session_id:item.session_id,reason:e.code});}
        }
        store.run("UPDATE balance_runs SET ended=?,status='completed',summary=? WHERE day=?",clock(),JSON.stringify({...result,applied}),window.key);
      }catch{store.run("UPDATE balance_runs SET ended=?,status='failed',summary=? WHERE day=?",clock(),JSON.stringify({reason:'planning_failed'}),window.key);}
    })().finally(()=>{running=null;});
    return running;
  }
  return {settings,saveSettings,saveWeights,selectAccount,atomic,distribution,move,block,recover,route,tick,preview:()=>plan(clock(),true),pending:()=>running,status:()=>{const state=distribution();return {settings:settings(),weights:{mode:state.mode,reason:state.reason,configuration:state.configuration,accounts:state.accounts},lastRun:store.get('SELECT * FROM balance_runs ORDER BY day DESC LIMIT 1')||null};}};
}
