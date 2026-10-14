import {AppError,id} from './store.mjs';

const minute=60000,day=24*60*minute;
export const balanceDefaults=Object.freeze({failover:false,nightly:false,quietMinutes:30});
export function balanceSettings(value={}){
  if(!value||typeof value!=='object'||Array.isArray(value)||Object.keys(value).some(k=>!Object.hasOwn(balanceDefaults,k)))throw new AppError(400,'负载均衡设置无效');
  const result={...balanceDefaults,...value};
  if(typeof result.failover!=='boolean'||typeof result.nightly!=='boolean'||!Number.isInteger(result.quietMinutes)||result.quietMinutes<1||result.quietMinutes>960)throw new AppError(400,'静默期须为 1–960 分钟，开关须为布尔值');
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
const median=values=>{if(!values.length)return null;const sorted=[...values].sort((a,b)=>a-b),n=sorted.length;return n%2?sorted[(n-1)/2]:(sorted[n/2-1]+sorted[n/2])/2;};

// Workload is a relative estimate, never a quota admission check.
export function planBalance({accounts,sessions,requests,at,quietMinutes=30,preparing=new Set()}){
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
    const total=work.reduce((n,s)=>n+s.weight,0),remaining=pool.reduce((n,a)=>n+snapshots.get(a.id).remaining,0);
    if(!total||!remaining)continue;
    const loads=new Map(pool.map(a=>[a.id,0])),target=new Map(pool.map(a=>[a.id,total*snapshots.get(a.id).remaining/remaining]));
    for(const s of work)loads.set(s.account_id,loads.get(s.account_id)+s.weight);
    const before=Object.fromEntries(loads);
    const movable=work.filter(s=>!s.fixed).sort((a,b)=>b.weight-a.weight||a.id.localeCompare(b.id));
    for(const s of movable){
      const origin=s.account_id,from=loads.get(origin)-target.get(origin);
      let best=null,benefit=0;
      for(const a of pool){
        if(a.id===origin)continue;
        const to=loads.get(a.id)-target.get(a.id);
        const improvement=Math.abs(from)+Math.abs(to)-Math.abs(from-s.weight)-Math.abs(to+s.weight);
        if(improvement-benefit>Number.EPSILON*total){benefit=improvement;best=a.id;}
      }
      if(best){loads.set(origin,loads.get(origin)-s.weight);loads.set(best,loads.get(best)+s.weight);moves.push({session_id:s.id,from:origin,to:best,version:s.route_version,weight:s.weight});}
    }
    summaries.push({accounts:pool.map(a=>a.id),before,after:Object.fromEntries(loads),target:Object.fromEntries(target),fixed:work.filter(s=>s.fixed).map(s=>s.id),fixedLoad:Object.fromEntries(pool.map(a=>[a.id,work.filter(s=>s.fixed&&s.account_id===a.id).reduce((sum,s)=>sum+s.weight,0)]))});
  }
  return {window,moves,skipped,summaries};
}

export function createBalancer({store,preparing,refreshQuota,readQuota=async()=>null,clock=Date.now,stopping=()=>false}){
  let running=null;
  const settings=()=>balanceSettings(JSON.parse(store.get("SELECT value FROM gateway_settings WHERE name='balancing'")?.value||'{}'));
  const saveSettings=value=>{balanceSettings(value);const next=balanceSettings({...settings(),...value});store.run("INSERT INTO gateway_settings(name,value) VALUES('balancing',?) ON CONFLICT(name) DO UPDATE SET value=excluded.value",JSON.stringify(next));store.audit('balancing.settings',JSON.stringify(next));return next;};
  const busy=s=>preparing.has(s.member_id)||!!store.get("SELECT id FROM requests WHERE session_id=? AND status IN ('queued','running')",s.id);
  function move(session,target,reason){
    const current=store.get('SELECT s.*,m.enabled member_enabled,m.removed_at member_removed FROM sessions s JOIN members m ON m.id=s.member_id WHERE s.id=?',session.id);
    if(!current||!current.enabled||!current.member_enabled||current.member_removed!==null||current.protocol!=='responses')throw new AppError(409,'会话当前不可迁移','session_disabled');
    if(current.route_version!==session.route_version)throw new AppError(409,'会话绑定已变化','session_route_conflict');
    if(busy(current))throw new AppError(409,'会话仍有请求处理中，暂不能切换','session_busy');
    const account=store.get("SELECT * FROM accounts WHERE id=? AND enabled=1 AND removed_at IS NULL AND kind='codex' AND state IN ('ready','degraded') AND route_block IS NULL",target);
    if(!account)throw new AppError(503,'没有可用的目标账号','no_account');
    if(current.account_id===target)return current;
    store.db.exec('BEGIN IMMEDIATE');
    try{
      store.run('UPDATE sessions SET account_id=?,upstream_thread=?,route_version=route_version+1 WHERE id=?',target,id(),current.id);
      store.audit('session.route',JSON.stringify({session:current.id,from:current.account_id,to:target,reason}));store.db.exec('COMMIT');
    }catch(e){store.db.exec('ROLLBACK');throw e;}
    return store.get('SELECT * FROM sessions WHERE id=?',current.id);
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
    const account=store.get('SELECT * FROM accounts WHERE id=?',session.account_id);
    if(!account?.route_block)return session;
    if(!settings().failover)throw new AppError(503,'绑定账号已阻断，自动切换未开启','session_account_blocked');
    const next=store.get("SELECT * FROM accounts WHERE kind='codex' AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') AND route_block IS NULL ORDER BY last_used,id LIMIT 1");
    if(!next)throw new AppError(503,'所有账号均不可用，请等待额度恢复或重新登录','no_account');
    return move(session,next.id,account.route_block);
  }
  async function plan(at,preview=false){
    const accounts=store.all("SELECT * FROM accounts WHERE kind='codex' AND enabled=1 AND removed_at IS NULL");
    const refreshed=await Promise.all(accounts.map(a=>Promise.resolve().then(()=>preview?readQuota(a):refreshQuota(a,true)).catch(()=>null)));
    const freshById=new Map(accounts.map((a,index)=>[a.id,{snapshot:refreshed[index],epoch:a.route_epoch}]));
    if(stopping())throw new AppError(503,'服务正在停止');
    return planBalance({accounts:store.all('SELECT * FROM accounts').map(a=>({...a,quota_json:freshById.get(a.id)?.epoch===a.route_epoch&&freshById.get(a.id)?.snapshot?JSON.stringify(freshById.get(a.id).snapshot):null})),sessions:store.all('SELECT s.*,m.enabled member_enabled,m.removed_at member_removed FROM sessions s JOIN members m ON m.id=s.member_id'),requests:store.all("SELECT * FROM requests WHERE COALESCE(ended,started)>=? OR status IN ('queued','running')",nightlyWindow(at).start),at:clock(),quietMinutes:settings().quietMinutes,preparing});
  }
  async function tick(){
    const at=clock(),window=nightlyWindow(at);
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
  return {settings,saveSettings,move,block,recover,route,tick,preview:()=>plan(clock(),true),pending:()=>running,status:()=>({settings:settings(),lastRun:store.get('SELECT * FROM balance_runs ORDER BY day DESC LIMIT 1')||null})};
}
