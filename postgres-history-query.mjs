import {AppError,usageWindow} from './store.mjs';
import {referenceMetric} from './pricing.mjs';
import {sanitizeRecord} from './record-redaction.mjs';

const json=source=>{try{const value=typeof source==='string'?JSON.parse(source):source;return value&&typeof value==='object'&&!Array.isArray(value)?value:{};}catch{return {};}};
const number=value=>Number.isFinite(value)?value:null;
const identifier=value=>typeof value==='string'&&/^[a-zA-Z0-9-]{1,100}$/.test(value)?value:null;
const codes=new Set(['manual','nightly','quota','initialize','recovery','weight_zero','account_disabled','quota_exhausted','login_expired','unknown_workload','busy','protected','no_measured_work','quota_unavailable','incomparable_quota','unknown_fixed_load','session_disabled','session_busy','nightly_plan_changed','planning_failed','balanced','within_tolerance','direction_unavailable','infeasible','search_limited','priority_unresolved','cannot_fit','planning_expired','planning_cancelled','application_attempts_exhausted','application_not_started']);
const reason=value=>codes.has(value)?value:null;
for(const code of ['cancelled','wait_timeout','wait_capacity','expired','replaced','superseded','session_handoff_expired','session_route_conflict','session_route_unsupported','session_context_unsupported','invalid_route_account','session_account_changed','session_permissions_changed','session_model_unsupported','service_stopping','session_handoff_failed'])codes.add(code);
for(const code of ['manual_handoff_pending','already_transferred_in_slot','session_permissions_changed','session_context_unsupported','application_attempts_exhausted','nightly_plan_expired','gateway_restart','planning_expired','manual_weights_active','package','package_scope_mismatch','package_budget_unavailable','incomparable_package_budget','package_unknown','official_plan_conflict','capacity_basis_unconfirmed','window_conversion_unavailable','quota_expired','quota_window_missing','package_not_effective','window_capacity_unavailable','absolute_unit_unavailable'])codes.add(code);
const reasonSql=value=>`CASE WHEN ${value}=ANY(ARRAY[${[...codes].map(code=>"'"+code+"'").join(',')}]::text[]) THEN ${value} END`;
const amounts=value=>Object.fromEntries(Object.entries(object(value)).filter(([id,n])=>identifier(id)&&Number.isFinite(n)));
const object=value=>value&&typeof value==='object'&&!Array.isArray(value)?value:{};
const legacyJson="CASE WHEN pg_input_is_valid(summary,'jsonb') THEN summary::jsonb ELSE '{}'::jsonb END";
const legacyArray=name=>`CASE WHEN jsonb_typeof(d->'${name}')='array' THEN d->'${name}' ELSE '[]'::jsonb END`;
const safeJson=alias=>`CASE WHEN pg_input_is_valid(${alias}.detail,'jsonb') THEN ${alias}.detail::jsonb ELSE '{}'::jsonb END`;

// These reads use the same bounded management pool as request statistics. Raw
// audit text and private plan input sections never cross the DTO boundary.
export function createPostgresHistoryQueries(store,{clock=Date.now,pageBytes=131072}={}){
  if(typeof clock!=='function'||!Number.isInteger(pageBytes)||pageBytes<256||pageBytes>2097152)throw new AppError(503,'历史查询资源配置无效','history_configuration');
  const snapshot=operation=>store.transaction(operation,{level:'REPEATABLE READ',readOnly:true});
  const catalog=()=>store.all('SELECT id,name FROM harbor.accounts ORDER BY id');
  function detail(row,known){
    const d=json(row.detail),account=value=>known.has(value)?value:null;
    const weights=value=>value&&typeof value==='object'&&!Array.isArray(value)?Object.fromEntries(Object.entries(value).filter(([id,n])=>known.has(id)&&Number.isFinite(n)&&n>=0&&n<=100)):null;
    const config=value=>{const v=object(value);return {mode:['auto','manual'].includes(v.mode)?v.mode:null,weights:weights(v.weights),expires:number(v.expires)};};
    if(row.action==='session.route')return {session:row.session_id??null,from:account(d.from),to:account(d.to),reason:reason(d.reason)||(!d.reason?'manual':null)};
    if(['session.handoff.requested','session.handoff.completed','session.handoff.cancelled','session.handoff.failed','session.handoff.superseded'].includes(row.action))return {session:row.session_id??null,from:account(d.from),to:account(d.to),reason:reason(d.reason)};
    if(row.action==='balancing.weights.auto')return {reason:reason(d.reason),weights:weights(d.weights),previous:weights(d.previous),retained:d.retained===true,basis:d.basis?{at:number(d.basis.at),accounts:(Array.isArray(d.basis.accounts)?d.basis.accounts:[]).filter(a=>a&&known.has(a.id)).map(a=>({id:a.id,...Object.fromEntries(['remaining','reset','hours','denominator','score','weight'].map(k=>[k,number(a[k])]))}))}:null};
    if(row.action==='balancing.weights')return {...config(d),previous:config(d.previous)};
    if(row.action==='balancing.weights.expired')return {expires:number(Number(row.detail))};
    if(row.action==='balancing.transfer_schedule'){const safe=v=>({times:Array.isArray(v?.times)?v.times.filter(t=>typeof t==='string'&&/^([01]\d|2[0-3]):[0-5]\d$/.test(t)):[],timezone:v?.timezone==='Asia/Shanghai'?v.timezone:null,version:number(v?.version),effective:number(v?.effective)});return {...safe(d),previous:safe(d.previous)};}
    if(row.action==='balancing.settings')return {weighted:typeof d.weighted==='boolean'?d.weighted:null,nightly:typeof d.nightly==='boolean'?d.nightly:null,failover:typeof d.failover==='boolean'?d.failover:null,quietMinutes:number(d.quietMinutes)};
    if(row.action==='account.api.weight')return {account:account(d.account),from:number(d.from),to:number(d.to)};
    if(['account.route_block','account.route_recover'].includes(row.action))return {account:account(d.account),reason:reason(d.reason)};
    if(row.action.startsWith('account.'))return {account:account(row.detail)};
    if(row.action.startsWith('session.'))return {session:row.session_id??null,enabled:typeof d.enabled==='boolean'?d.enabled:null,expires:number(d.expires)};
    if(row.action.startsWith('key.'))return {prefix:row.key_prefix??null,member:row.key_member_name??null,...(row.action==='key.models'?{allowed_models:d.allowed_models===null?null:(Array.isArray(d.allowed_models)?d.allowed_models:[]).filter(v=>typeof v==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(v))}:{})};
    if(row.action==='request.settle')return {request:row.request_id??null,account:account(row.request_account),member:row.request_member_name??null,...(d.metric===referenceMetric?{metric:referenceMetric,standard_usd:typeof d.standard_usd==='string'&&/^\d{1,12}\.\d{2}$/.test(d.standard_usd)?d.standard_usd:null}:{})};
    if(row.action==='pricing.configure')return {version:number(d.version),revision:typeof d.revision==='string'&&/^[a-f0-9]{64}$/.test(d.revision)?d.revision:null};
    if(row.action==='member.type.create')return {type:identifier(d.type),code:identifier(d.code)};
    if(row.action==='member.type.configure')return {type:identifier(d.type),code:identifier(d.code),...(typeof d.name_changed==='boolean'?{name_changed:d.name_changed}:{}),...(typeof d.description_changed==='boolean'?{description_changed:d.description_changed}:{}),...(typeof d.enabled_before==='boolean'&&typeof d.enabled_after==='boolean'?{enabled_before:d.enabled_before,enabled_after:d.enabled_after}:{}),version:number(d.version),template:number(d.template),previous:number(d.previous),...(number(d.restored_from)!==null?{restored_from:number(d.restored_from)}:{})};
    if(row.action==='member.type.change')return {member:row.member_name??null,from:identifier(d.from),to:identifier(d.to),version:number(d.version),apply_defaults:false};
    if(row.action==='member.actions.apply')return {member:row.member_name??null,batch:identifier(d.batch),operation:['member_access','key_renewal'].includes(d.operation)?d.operation:null,direction:['apply','undo'].includes(d.direction)?d.direction:null,actor:d.actor==='admin'?'admin':null};
    if(row.action==='member.batch.apply')return {member:row.member_name??null,batch:identifier(d.batch),direction:['apply','undo'].includes(d.direction)?d.direction:null,actor:d.actor==='admin'?'admin':null};
    if(row.action==='member.limits')return {member:row.member_name??null,enabled:d.after?.enabled===true,rules:(Array.isArray(d.after?.rules)?d.after.rules:[]).filter(r=>r&&typeof r.model==='string'&&/^[a-zA-Z0-9][a-zA-Z0-9._:/-]{0,199}$/.test(r.model)&&[24,168].includes(r.hours)&&typeof r.limit_usd==='string'&&/^\d{1,12}\.\d{2}$/.test(r.limit_usd)).map(({model,hours,limit_usd})=>({model,hours,limit_usd})),version:number(d.version)};
    if(row.action.startsWith('member.')&&row.action!=='member.create')return {member:row.member_name??null};
    return {};
  }
  async function list(query){
    return snapshot(async()=>{
      const at=clock(),window=usageWindow(at),accounts=await catalog(),known=new Set(accounts.map(a=>a.id)),nightly=query.kind==='nightly',args=[Math.max(query.start??window.start,window.start),Math.min(query.end??window.end,window.end)];
      const parameter=value=>{args.push(value);return '$'+args.length;},time=nightly?'a.started':'a.at',conditions=[time+'>=$1',time+'<$2'];let from;
      if(nightly){
        from=`FROM harbor.balance_runs a CROSS JOIN LATERAL (SELECT CASE WHEN pg_input_is_valid(a.summary,'jsonb') THEN a.summary::jsonb ELSE '{}'::jsonb END d) source`;
        if(query.account||query.session){
          const parts=[];
          if(query.account){const p=parameter(query.account);parts.push(`(i.payload->>'from'=${p} OR i.payload->>'to'=${p} OR i.payload->>'account_id'=${p} OR i.payload->'accounts' ? ${p})`);}
          if(query.session){const p=parameter(query.session);parts.push(`(i.payload->>'session_id'=${p} OR i.payload->>'client_session_id'=${p})`);}
          const legacyParts=[];if(query.account){const p=parameter(query.account);legacyParts.push(`jsonb_path_exists(source.d,'$.** ? (@ == $needle)',jsonb_build_object('needle',${p}::text))`);}if(query.session){const p=parameter(query.session);legacyParts.push(`(jsonb_path_exists(source.d,'$.** ? (@ == $needle)',jsonb_build_object('needle',${p}::text)) OR EXISTS(SELECT 1 FROM harbor.sessions s WHERE s.client_session_id=${p} AND jsonb_path_exists(source.d,'$.** ? (@ == $needle)',jsonb_build_object('needle',s.id))))`);}
          const matches=parts.map(predicate=>`EXISTS(SELECT 1 FROM harbor.routing_plan_items i WHERE (i.plan_id=a.plan_id OR EXISTS(SELECT 1 FROM harbor.nightly_plan_applications ap WHERE ap.slot=a.day AND ap.plan_id=i.plan_id)) AND i.section IN ('moves','skipped','groups','fixed') AND ${predicate})`);
          conditions.push(`((${matches.join(' AND ')}) OR (a.plan_id IS NULL AND ${legacyParts.join(' AND ')}))`);
        }
      }else{
        const d=safeJson('a');from=`FROM harbor.audit a CROSS JOIN LATERAL (SELECT ${d} d) data LEFT JOIN harbor.requests r ON a.action='request.settle' AND r.id=COALESCE(data.d->>'request',a.detail) LEFT JOIN harbor.sessions s ON s.id=COALESCE(data.d->>'session',data.d->>'sessionId',CASE WHEN a.action IN ('session.enable','session.disable') THEN a.detail END,r.session_id)`;
        if(query.action)conditions.push('a.action='+parameter(query.action));
        if(query.session){const p=parameter(query.session);conditions.push(`(s.id=${p} OR s.client_session_id=${p})`);}
        if(query.account){const p=parameter(query.account);conditions.push(`(data.d->>'from'=${p} OR data.d->>'to'=${p} OR data.d->>'account'=${p} OR (a.action NOT IN ('session.route','request.settle') AND s.account_id=${p}) OR r.account_id=${p} OR (a.action LIKE 'account.%' AND a.detail=${p}) OR data.d->'weights' ? ${p} OR data.d->'previous' ? ${p} OR data.d->'previous'->'weights' ? ${p})`);}
      }
      const where=' WHERE '+conditions.join(' AND '),total=(await store.get('SELECT COUNT(*) n '+from+where,...args)).n,totalPages=Math.max(1,Math.ceil(total/query.pageSize)),page=Math.min(query.page,totalPages);
      const columns=nightly?`a.day,a.started,a.ended,a.status,${reasonSql("source.d->>'reason'")} reason`:`a.id,a.at,a.action,CASE WHEN octet_length(a.detail)<=2097152 THEN a.detail END detail,octet_length(a.detail)>2097152 detail_too_large,s.id session_id,s.client_session_id,r.id request_id,r.account_id request_account,rm.name request_member_name,k.prefix key_prefix,km.name key_member_name,m.name member_name`;
      const extra=nightly?'':" LEFT JOIN harbor.members rm ON rm.id=r.member_id LEFT JOIN harbor.keys k ON a.action LIKE 'key.%' AND k.id=CASE WHEN a.action='key.models' THEN data.d->>'key' ELSE a.detail END LEFT JOIN harbor.members km ON km.id=k.member_id LEFT JOIN harbor.members m ON a.action LIKE 'member.%' AND m.id=CASE WHEN a.action IN ('member.limits','member.type.change','member.batch.apply','member.actions.apply') THEN data.d->>'member' ELSE a.detail END";
      const rows=await store.all(`SELECT ${columns} ${from}${extra}${where} ORDER BY ${time} DESC,${nightly?'a.day':'a.id'} DESC LIMIT $${args.length+1} OFFSET $${args.length+2}`,...args,query.pageSize,(page-1)*query.pageSize);
      if(rows.some(row=>row.detail_too_large))throw new AppError(503,'历史单项超过读取上限，请通过数据库核对','history_capacity');
      const actions=(await store.all('SELECT DISTINCT action FROM harbor.audit WHERE at>=$1 AND at<$2 ORDER BY action',window.start,window.end)).map(r=>r.action);
      return {kind:query.kind,usageWindow:window,accounts,actions,pagination:{page,pageSize:query.pageSize,total,totalPages},rows:nightly?rows:rows.map(row=>({id:row.id,at:row.at,action:row.action,session:row.session_id,clientSession:row.client_session_id,detail:sanitizeRecord(detail(row,known),{onTruncate:()=>{throw new AppError(503,'历史详情超过展示上限，请通过数据库核对','history_capacity');}})}))};
    });
  }
  async function batch(day){
    if(!/^\d{4}-\d{2}-\d{2}(?:-\d{4})?$/.test(day))throw new AppError(400,'夜间批次无效');
    const window=usageWindow(clock()),row=await store.get(`SELECT day,started,ended,status,jsonb_build_object('reason',${reasonSql("d->>'reason'")},'metric',d->'metric','window',d->'window') summary,plan_id FROM harbor.balance_runs CROSS JOIN LATERAL (SELECT ${legacyJson} d) source WHERE day=$1 AND started>=$2 AND started<$3`,day,window.start,window.end);if(!row)throw new AppError(404,'夜间批次不存在或超出近7天可见范围');return row;
  }
  async function selectedRevision(row,revision){
    if(revision!==undefined&&(!Number.isSafeInteger(revision)||revision<0))throw new AppError(400,'批次修订无效');
    const selected=await store.get(`SELECT a.plan_id,a.status,a.started,a.ended,COALESCE(v.revision,0)::int revision FROM harbor.nightly_plan_applications a LEFT JOIN harbor.nightly_plan_revisions v ON v.job_id=a.plan_id WHERE a.slot=$1 AND ($2::int IS NULL OR COALESCE(v.revision,0)=$2) ORDER BY COALESCE(v.revision,0) DESC,a.plan_id DESC LIMIT 1`,row.day,revision??null);
    if(!selected&&revision!==undefined&&(revision!==0||await store.get('SELECT plan_id FROM harbor.nightly_plan_applications WHERE slot=$1 LIMIT 1',row.day)))throw new AppError(404,'批次修订不存在');return selected;
  }
  const groupTotals=(table,column,key)=>store.get(`SELECT COUNT(*) groups,COUNT(*) FILTER(WHERE status='skipped') skipped,COALESCE(SUM(moved),0)::int applied FROM ${table} WHERE ${column}=$1`,key);
  async function revisionPage(day,{after=0,limit=25}={}){
    if(!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>100)throw new AppError(400,'修订分页无效');
    return snapshot(async()=>{await batch(day);const rows=await store.all(`SELECT COALESCE(v.revision,0)::int+1 position,a.status,a.started,a.ended,p.created planned_at,(p.header->'counts'->>'moves')::int planned,
      COALESCE(t.applied,0)::int applied,COALESCE(t.skipped,0)::int skipped,
      (SELECT COUNT(*)::int FROM harbor.session_handoffs h JOIN harbor.automatic_handoff_groups g ON g.id=h.automatic_group_id WHERE g.plan_id=a.plan_id AND h.status='waiting') waiting
      FROM harbor.nightly_plan_applications a JOIN harbor.routing_plans p ON p.id=a.plan_id LEFT JOIN harbor.nightly_plan_revisions v ON v.job_id=a.plan_id
      LEFT JOIN LATERAL(SELECT SUM(moved) applied,COUNT(*) FILTER(WHERE status='skipped') skipped FROM harbor.nightly_plan_group_results WHERE plan_id=a.plan_id) t ON true
      WHERE a.slot=$1 AND COALESCE(v.revision,0)+1>$2 ORDER BY COALESCE(v.revision,0),a.plan_id LIMIT $3`,day,after,limit+1);
      const items=[];let bytes=0;for(const row of rows.slice(0,limit)){const value={...row,revision:row.position-1},size=Buffer.byteLength(JSON.stringify(value));if(size>pageBytes)throw new AppError(503,'修订单项超过读取上限','history_capacity');if(items.length&&bytes+size>pageBytes)break;bytes+=size;items.push(value);}return {items,after:items.at(-1)?.position??after,has_more:rows.length>items.length};});
  }
  async function nightly(day,{revision}={}){return snapshot(async()=>{
    const row=await batch(day),summary=json(row.summary),selected=await selectedRevision(row,revision),planId=selected?.plan_id??row.plan_id,plan=planId?await store.get('SELECT created,header FROM harbor.routing_plans WHERE id=$1',planId):null;
    const legacy=plan?null:await store.get(`SELECT jsonb_typeof(d->'moves')='array' OR jsonb_typeof(d->'summaries')='array' available,jsonb_array_length(${legacyArray('moves')}) moves,jsonb_array_length(${legacyArray('skipped')}) skipped,jsonb_array_length(${legacyArray('summaries')}) groups,0 fixed,CASE WHEN jsonb_typeof(d->'applied')='array' THEN (SELECT COUNT(*)::int FROM jsonb_array_elements(${legacyArray('moves')}) m WHERE d->'applied' ? (m->>'session_id')) END applied FROM harbor.balance_runs CROSS JOIN LATERAL (SELECT ${legacyJson} d) source WHERE day=$1`,day);
    const counts=plan?.header.counts??{moves:legacy.moves,skipped:legacy.skipped,groups:legacy.groups,fixed:0},totals=selected?await store.get("SELECT COUNT(*) groups,COUNT(*) FILTER(WHERE g.status='skipped') skipped,COALESCE(SUM(g.moved),0)::int applied FROM harbor.nightly_plan_group_results g JOIN harbor.nightly_plan_applications a ON a.plan_id=g.plan_id WHERE a.slot=$1",day):await groupTotals('harbor.nightly_group_results','day',day);
    let modern={};if(selected){const facts=await store.get(`SELECT COUNT(*)::int revisions,MAX(COALESCE(v.revision,0))::int latest_revision,COALESCE(SUM((p.header->'counts'->>'moves')::int),0)::int planned_total,MAX(COALESCE(v.revision,0)) FILTER(WHERE COALESCE(v.revision,0)<$2)::int previous_revision,MIN(COALESCE(v.revision,0)) FILTER(WHERE COALESCE(v.revision,0)>$2)::int next_revision FROM harbor.nightly_plan_applications a JOIN harbor.routing_plans p ON p.id=a.plan_id LEFT JOIN harbor.nightly_plan_revisions v ON v.job_id=a.plan_id WHERE a.slot=$1`,day,selected.revision),waiting=await store.get("SELECT COUNT(*)::int sessions,COUNT(DISTINCT g.id)::int groups FROM harbor.automatic_handoff_groups g JOIN harbor.session_handoffs h ON h.automatic_group_id=g.id AND h.status='waiting' WHERE g.slot=$1 AND g.status='waiting'",day),pending=await store.get(`SELECT v.revision,COALESCE(j.status,'pending') status,j.error_code FROM harbor.nightly_plan_revisions v LEFT JOIN harbor.background_jobs j ON j.id=v.job_id WHERE v.slot=$1 AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_applications a WHERE a.plan_id=v.job_id) ORDER BY v.revision DESC LIMIT 1`,day);
      modern={revisionAware:true,revision:selected.revision,latestRevision:facts.latest_revision,revisionCount:facts.revisions,previousRevision:facts.previous_revision,nextRevision:facts.next_revision,plannedTotal:facts.planned_total,selectedStatus:selected.status,selectedResults:await groupTotals('harbor.nightly_plan_group_results','plan_id',planId),waiting:waiting.sessions,waitingGroups:waiting.groups,pendingRevision:pending?{revision:pending.revision,status:pending.status,reason:reason(pending.error_code)}:null};}
    return {day:row.day,status:row.status,started:row.started,ended:row.ended,metric:(plan?.header.metric??summary.metric)===referenceMetric?referenceMetric:null,window:plan?.header.window??(summary.window?{start:number(summary.window.start),end:number(summary.window.end)}:null),plannedAt:plan?.created??null,reason:reason(summary.reason),accounts:await catalog(),counts,results:plan?totals:{groups:null,skipped:null,applied:legacy.applied??null},...modern,paginated:true,visibleUntil:usageWindow(row.started).end+6*86400000,hasPlan:!!plan||legacy?.available===true,legacy:!plan};
  });}
  async function legacyPage(row,section,after,limit){
    if(section==='fixed')return {section,items:[],after,has_more:false};
    const key=section==='groups'?'summaries':section;
    const source=`WITH source AS (SELECT ${legacyJson} d FROM harbor.balance_runs WHERE day=$1), items AS (SELECT e.ordinality::int ordinal,e.value payload,octet_length(e.value::text) bytes FROM source CROSS JOIN LATERAL jsonb_array_elements(${legacyArray(key)}) WITH ORDINALITY e(value,ordinality))`;
    const sizes=await store.all(source+' SELECT ordinal,bytes FROM items WHERE ordinal>$2 ORDER BY ordinal LIMIT $3',row.day,after,limit+1);let bytes=0;const selected=[];for(const item of sizes.slice(0,limit)){if(selected.length&&bytes+item.bytes>pageBytes)break;selected.push(item.ordinal);bytes+=item.bytes;}
    if(sizes[0]?.bytes>2097152)throw new AppError(503,'旧批次单项超过读取上限，请通过数据库核对','history_capacity');
    const items=selected.length?await store.all(source+` SELECT i.ordinal,i.payload,s.client_session_id,CASE WHEN jsonb_typeof(source.d->'applied')='array' THEN source.d->'applied' ? (i.payload->>'session_id') END applied FROM items i CROSS JOIN source LEFT JOIN harbor.sessions s ON s.id=i.payload->>'session_id' WHERE i.ordinal=ANY($2::int[]) ORDER BY i.ordinal`,row.day,selected):[];
    const comparable=json(row.summary).metric===referenceMetric;
    return {section,items:items.map(i=>{const p=object(i.payload);if(section==='groups')return {position:i.ordinal,accounts:Array.isArray(p.accounts)?p.accounts.filter(identifier):[],before:comparable?amounts(p.before):{},after:comparable?amounts(p.after):{},target:comparable?amounts(p.target):{},fixedLoad:comparable?amounts(p.fixedLoad):{},planning:p.planning?{reason:reason(p.planning.reason),strategy:p.planning.strategy==='heavy-bias-first-v2'?'heavy-bias-first-v2':null,beforeDeviation:number(p.planning.beforeDeviation),afterDeviation:number(p.planning.afterDeviation),tolerance:number(p.planning.tolerance),optimizationComplete:p.planning.optimizationComplete===true,directionComplete:p.planning.directionComplete===true,prioritySessions:(Array.isArray(p.planning.prioritySessions)?p.planning.prioritySessions:[]).filter(identifier),heavySkipped:(Array.isArray(p.planning.heavySkipped)?p.planning.heavySkipped:[]).filter(m=>m&&identifier(m.session_id)).map(m=>({session_id:m.session_id,reason:reason(m.reason)}))}:null,result:{status:'historical',reason:null,moved:null}};return {position:i.ordinal,session_id:identifier(p.session_id),client_session_id:i.client_session_id??null,from:identifier(p.from),to:identifier(p.to),account_id:identifier(p.account_id),accounts:Array.isArray(p.accounts)?p.accounts.filter(identifier):[],weight:comparable?number(p.weight):null,reason:reason(p.reason),result:{status:i.applied===true?'applied':i.applied===false?'not_applied':'historical',reason:null,moved:null}};}),after:selected.at(-1)??after,has_more:sizes.length>selected.length};
  }
  async function page(day,{section='moves',after=0,limit=25,revision}={}){
    if(!['groups','moves','skipped','fixed'].includes(section)||!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>100)throw new AppError(400,'夜间历史分页无效');
    return snapshot(async()=>{
      const source=await batch(day),selectedRevisionRow=await selectedRevision(source,revision),row={...source,plan_id:selectedRevisionRow?.plan_id??source.plan_id};if(!row.plan_id)return legacyPage(row,section,after,limit);
      const sizes=await store.all('SELECT ordinal,bytes FROM harbor.routing_plan_items WHERE plan_id=$1 AND section=$2 AND ordinal>$3 ORDER BY ordinal LIMIT $4',row.plan_id,section,after,limit+1);let bytes=0;const selected=[];
      for(const item of sizes.slice(0,limit)){if(selected.length&&bytes+item.bytes>pageBytes)break;selected.push(item.ordinal);bytes+=item.bytes;}
      const items=selected.length?await store.all(`SELECT i.ordinal,i.payload,g.status result_status,g.reason result_reason,g.moved result_moved FROM harbor.routing_plan_items i LEFT JOIN LATERAL (SELECT p.ordinal FROM harbor.routing_plan_items p WHERE p.plan_id=i.plan_id AND p.section='groups' AND (CASE WHEN i.section='groups' THEN p.ordinal=i.ordinal WHEN i.section='fixed' THEN p.ordinal=(i.payload->>'group')::int+1 ELSE p.payload->'accounts' ? (i.payload->>'from') END) LIMIT 1) group_id ON true LEFT JOIN ${selectedRevisionRow?'harbor.nightly_plan_group_results':'harbor.nightly_group_results'} g ON g.${selectedRevisionRow?'plan_id':'day'}=$4 AND g.group_ordinal=group_id.ordinal WHERE i.plan_id=$1 AND i.section=$2 AND i.ordinal=ANY($3::integer[]) ORDER BY i.ordinal`,row.plan_id,section,selected,selectedRevisionRow?row.plan_id:day):[];
      return {section,items:items.map(i=>({position:i.ordinal,...i.payload,result:{status:i.result_status??'pending',reason:reason(i.result_reason),moved:i.result_moved??null}})),after:selected.at(-1)??after,has_more:sizes.length>selected.length};
    });
  }
  return {list,nightly,page,revisions:revisionPage};
}
