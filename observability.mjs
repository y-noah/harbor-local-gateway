import {AppError,requestPhases} from './store.mjs';

const parse=value=>{try{return JSON.parse(value);}catch{return null;}};
const number=value=>Number.isFinite(value)?value:null;
const reasonCodes=new Set(['manual','nightly','quota','initialize','recovery','weight_zero','quota_exhausted','login_expired','unknown_workload','busy','protected','no_measured_work','quota_unavailable','incomparable_quota','unknown_fixed_load','session_disabled','session_busy','nightly_plan_changed','planning_failed','balanced','within_tolerance','direction_unavailable','infeasible','search_limited','priority_unresolved']);
const reason=value=>reasonCodes.has(value)?value:null;

// Only structured routing metadata crosses the history boundary. Never return raw audit text.
export function historyQuery(params){
  const allowed=new Set(['kind','page','page_size','action','start','end','account','session']);
  for(const key of params.keys())if(!allowed.has(key)||params.getAll(key).length!==1)throw new AppError(400,'历史查询参数无效');
  const integer=(key,fallback,max)=>{
    const value=params.get(key);if(value===null||value==='')return fallback;
    if(!/^\d+$/.test(value)||!Number.isSafeInteger(Number(value))||Number(value)>max||Number(value)<(key==='page'||key==='page_size'?1:0))throw new AppError(400,'历史查询范围无效');
    return Number(value);
  };
  const result={kind:params.get('kind')||'audit',page:integer('page',1,1e7),pageSize:integer('page_size',25,100),start:integer('start',null,9e15),end:integer('end',null,9e15),action:params.get('action')||'',account:params.get('account')||'',session:params.get('session')||''};
  if(!['audit','nightly'].includes(result.kind)||result.start!==null&&result.end!==null&&result.start>=result.end||result.action&&!/^[a-z.]{1,80}$/.test(result.action)||[result.account,result.session].some(x=>x&&!/^[a-zA-Z0-9-]{1,100}$/.test(x))||result.kind==='nightly'&&result.action)throw new AppError(400,'历史筛选条件无效');
  return result;
}

export function historyReader(store){
  const accounts=()=>store.all('SELECT id,name FROM accounts');
  function details(row,known){
    const value=parse(row.detail),d=value&&typeof value==='object'&&!Array.isArray(value)?value:{};
    const weights=value=>value&&typeof value==='object'&&!Array.isArray(value)?Object.fromEntries(Object.entries(value).filter(([id,w])=>known.has(id)&&Number.isFinite(w)&&w>=0&&w<=100)):null;
    const config=value=>value&&typeof value==='object'?{mode:['auto','manual'].includes(value.mode)?value.mode:null,weights:weights(value.weights),expires:number(value.expires)}:null;
    if(row.action==='session.route')return {session:row.session_id||null,from:known.has(d.from)?d.from:null,to:known.has(d.to)?d.to:null,reason:reason(d.reason)||(!d.reason?'manual':null)};
    if(row.action==='balancing.weights.auto')return {reason:reason(d.reason),weights:weights(d.weights),previous:weights(d.previous),retained:d.retained===true,basis:d.basis?{at:number(d.basis.at),accounts:(Array.isArray(d.basis.accounts)?d.basis.accounts:[]).filter(a=>known.has(a.id)).map(a=>({id:a.id,remaining:number(a.remaining),reset:number(a.reset),hours:number(a.hours),denominator:number(a.denominator),score:number(a.score),weight:number(a.weight)}))}:null};
    if(row.action==='balancing.weights')return {...config(d),previous:config(d.previous)};
    if(row.action==='balancing.weights.expired')return {expires:number(Number(row.detail))};
    if(row.action==='account.route_block')return {account:known.has(d.account)?d.account:null,reason:reason(d.reason)};
    if(row.action.startsWith('account.'))return {account:known.has(row.detail)?row.detail:null};
    if(row.action.startsWith('session.'))return {session:row.session_id||null,enabled:typeof d.enabled==='boolean'?d.enabled:null,expires:number(d.expires)};
    if(row.action==='balancing.settings')return {weighted:typeof d.weighted==='boolean'?d.weighted:null,nightly:typeof d.nightly==='boolean'?d.nightly:null,failover:typeof d.failover==='boolean'?d.failover:null,quietMinutes:number(d.quietMinutes)};
    if(row.action.startsWith('key.')){const k=store.get('SELECT prefix,member_id FROM keys WHERE id=?',row.detail);return k?{prefix:k.prefix,member:store.get('SELECT name FROM members WHERE id=?',k.member_id)?.name??null}:{};}
    if(row.action.startsWith('member.')&&row.action!=='member.create')return {member:store.get('SELECT name FROM members WHERE id=?',row.detail)?.name??null};
    return {};
  }
  function list(query){
    const {kind,start,end,action,account,session,pageSize}=query,clauses=[],args=[],catalog=accounts(),known=new Set(catalog.map(a=>a.id));
    const isNight=kind==='nightly',time=isNight?'a.started':'a.at',table=isNight?'balance_runs':'audit';
    if(start!==null){clauses.push(time+'>=?');args.push(start);}
    if(end!==null){clauses.push(time+'<?');args.push(end);}
    if(action){clauses.push('a.action=?');args.push(action);}
    let join='';
    if(isNight){
      if(account){clauses.push("EXISTS(SELECT 1 FROM json_tree(CASE WHEN json_valid(a.summary) THEN a.summary ELSE '{}' END) WHERE value=?)");args.push(account);}
      if(session){const sid=store.get('SELECT id FROM sessions WHERE id=? OR client_session_id=?',session,session)?.id||session;clauses.push("EXISTS(SELECT 1 FROM json_tree(CASE WHEN json_valid(a.summary) THEN a.summary ELSE '{}' END) WHERE value=?)");args.push(sid);}
    }else{
      const detail="CASE WHEN json_valid(a.detail) THEN a.detail ELSE '{}' END";
      join=` LEFT JOIN sessions s ON s.id=COALESCE(json_extract(${detail},'$.session'),json_extract(${detail},'$.sessionId'),CASE WHEN a.action IN ('session.enable','session.disable') THEN a.detail END)`;
      if(session){clauses.push('(s.id=? OR s.client_session_id=?)');args.push(session,session);}
      if(account){clauses.push(`(json_extract(${detail},'$.from')=? OR json_extract(${detail},'$.to')=? OR json_extract(${detail},'$.account')=? OR (a.action<>'session.route' AND s.account_id=?) OR (a.action LIKE 'account.%' AND a.detail=?) OR EXISTS(SELECT 1 FROM json_each(${detail},'$.weights') WHERE key=?))`);args.push(account,account,account,account,account,account);}
    }
    const where=clauses.length?' WHERE '+clauses.join(' AND '):'',from=` FROM ${table} a${join}${where}`;
    const total=store.get('SELECT COUNT(*) total'+from,...args).total,totalPages=Math.max(1,Math.ceil(total/pageSize)),page=Math.min(query.page,totalPages);
    const columns=isNight?'a.day,a.started,a.ended,a.status':"a.id,a.at,a.action,a.detail,s.id session_id,s.client_session_id";
    const rows=store.all(`SELECT ${columns}${from} ORDER BY ${time} DESC,${isNight?'a.day':'a.id'} DESC LIMIT ? OFFSET ?`,...args,pageSize,(page-1)*pageSize);
    return {kind,accounts:catalog,actions:store.all('SELECT DISTINCT action FROM audit ORDER BY action').map(r=>r.action),pagination:{page,pageSize,total,totalPages},rows:isNight?rows:rows.map(r=>({id:r.id,at:r.at,action:r.action,session:r.session_id,clientSession:r.client_session_id,detail:details(r,known)}))};
  }
  function nightly(day){
    if(!/^\d{4}-\d{2}-\d{2}$/.test(day))throw new AppError(400,'夜间批次无效');
    const row=store.get('SELECT * FROM balance_runs WHERE day=?',day);if(!row)throw new AppError(404,'夜间批次不存在');
    const source=parse(row.summary),raw=source&&typeof source==='object'?source:{},known=new Set(accounts().map(a=>a.id));
    const nums=value=>Object.fromEntries(Object.entries(value||{}).filter(([id,n])=>known.has(id)&&Number.isFinite(n)));
    const sessionInfo=id=>{const s=store.get('SELECT id,client_session_id,member_id FROM sessions WHERE id=?',id);return {session_id:s?.id??null,client_session_id:s?.client_session_id??null,member:s?store.get('SELECT name FROM members WHERE id=?',s.member_id)?.name??null:null};};
    return {day:row.day,status:row.status,started:row.started,ended:row.ended,accounts:accounts(),window:raw.window?{start:number(raw.window.start),end:number(raw.window.end)}:null,reason:reason(raw.reason),
      moves:(Array.isArray(raw.moves)?raw.moves:[]).map(m=>({...sessionInfo(m.session_id),from:known.has(m.from)?m.from:null,to:known.has(m.to)?m.to:null,weight:number(m.weight),applied:Array.isArray(raw.applied)?raw.applied.includes(m.session_id):null})),
      skipped:(Array.isArray(raw.skipped)?raw.skipped:[]).map(m=>({...(m.session_id?sessionInfo(m.session_id):{}),account:known.has(m.account_id)?m.account_id:null,reason:reason(m.reason)})),
      summaries:(Array.isArray(raw.summaries)?raw.summaries:[]).map(g=>({accounts:(g.accounts||[]).filter(id=>known.has(id)),weights:nums(g.weights),before:nums(g.before),after:nums(g.after),target:nums(g.target),fixedLoad:nums(g.fixedLoad),planning:g.planning?{reason:reason(g.planning.reason),strategy:g.planning.strategy==='heavy-bias-first-v2'?'heavy-bias-first-v2':null,beforeDeviation:number(g.planning.beforeDeviation),afterDeviation:number(g.planning.afterDeviation),tolerance:number(g.planning.tolerance),optimizationComplete:g.planning.optimizationComplete===true,directionComplete:g.planning.directionComplete===true,prioritySessions:Array.isArray(g.planning.prioritySessions)?g.planning.prioritySessions.map(()=>null):[],heavySkipped:Array.isArray(g.planning.heavySkipped)?g.planning.heavySkipped.map(()=>null):[]}:null}))};
  }
  return {list,nightly};
}

export function requestObservation(store,id,at=Date.now()){
  const r=store.get('SELECT r.id,r.started,r.ended,r.status,r.phase,r.phase_at,r.phase_history,r.account_id,r.session_id,r.model,r.input,r.output,r.cached,r.reserved,r.error,r.ip,r.agent,r.key_id,m.name member_name,a.name account_name,k.prefix key_prefix,k.removed_at key_removed_at FROM requests r LEFT JOIN members m ON m.id=r.member_id LEFT JOIN accounts a ON a.id=r.account_id LEFT JOIN keys k ON k.id=r.key_id WHERE r.id=?',id);
  if(!r)throw new AppError(404,'请求记录不存在');
  const raw=parse(r.phase_history),timeline=(Array.isArray(raw)?raw:[]).filter(p=>p&&requestPhases.has(p.phase)&&Number.isFinite(p.at));
  return {...r,phase_history:undefined,observedAt:at,timeline:timeline.map((p,i)=>({...p,duration:Math.max(0,(timeline[i+1]?.at??r.ended??at)-p.at)}))};
}
