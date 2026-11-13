import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {codexQuotaDenials} from './codex-entitlements.mjs';
import {annotateExtraPlanning} from './postgres-extra-planning.mjs';
import {AppError,storedModelAllowlist,checkAllowedModel} from './store.mjs';
import {nightlyWindow,planBalanceWorkloads} from './balancing.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {validPrice,joins} from './postgres-reference-query.mjs';

// Only per-session totals and bounded account/version metadata leave SQL. Raw
// samples are repriced in SQL at each estimator's dispatch revision, not at the
// current rate, and manual reconciliations never become raw usage samples.
const workloadSql=scope=>`WITH source AS MATERIALIZED (
  SELECT r.session_id,r.route_version,r.query_sequence,r.handoff_context,r.handoff_input,r.started,r.ended,r.status,r.price_version,r.billing_model,r.model,r.input,r.output,r.cached,r.usage_unknown,
    CASE WHEN ${validPrice} THEN p.state ELSE 'unpriced' END price_state,
    CASE WHEN ${validPrice} THEN p.total_atoms END total,
    COALESCE(NULLIF(CASE WHEN ${validPrice} THEN p.model END,''),NULLIF(r.billing_model,''),r.model,'') model_key,
    COALESCE(NULLIF(r.billing_model,''),r.model) chosen
  FROM harbor.requests r${joins} WHERE ${scope?'r.department_id=$5 AND ':''}(r.usage_at>=$1 OR r.status IN ('queued','running'))
), included AS MATERIALIZED (
  SELECT *,ended IS NOT NULL AND ended<>0 AND total IS NOT NULL measured
  FROM source WHERE session_id IS NOT NULL AND ((started<$2 AND COALESCE(ended,$3)>=$1) OR status IN ('queued','running'))
), samples AS MATERIALIZED (
  SELECT * FROM included WHERE ended IS NOT NULL AND ended<>0 AND usage_unknown=0 AND price_state='priced' AND total>0
), pairs AS MATERIALIZED (
  SELECT DISTINCT model_key,price_version FROM included WHERE NOT measured AND price_state NOT IN ('unpriced','legacy_manual')
), books AS MATERIALIZED (
  SELECT id,book::jsonb book FROM harbor.pricing_versions WHERE id IN (SELECT price_version FROM pairs)
), resolved AS (
  SELECT t.model_key,t.price_version,s.input,s.output,s.cached,
    b.book->'rates'->(CASE WHEN b.book->'rates' ? s.chosen THEN s.chosen ELSE a.target END) rate
  FROM pairs t JOIN samples s ON s.model_key=t.model_key JOIN books b ON b.id=t.price_version
  LEFT JOIN LATERAL (
    SELECT alias->>'target' target FROM jsonb_array_elements(b.book->'aliases') WITH ORDINALITY AS entry(alias,position)
    WHERE alias->>'name'=s.chosen AND s.started>=(alias->>'from')::bigint AND (alias->>'to' IS NULL OR s.started<(alias->>'to')::bigint)
    ORDER BY position LIMIT 1
  ) a ON TRUE
), repriced AS (
  SELECT model_key,price_version,CASE WHEN rate IS NOT NULL AND input BETWEEN 0 AND 9007199254740991
    AND output BETWEEN 0 AND 9007199254740991 AND cached BETWEEN 0 AND input AND (cached=0 OR rate->>'cached' IS NOT NULL)
    THEN ((input-cached)::numeric*(rate->>'input')::numeric+cached::numeric*COALESCE((rate->>'cached')::numeric,0)+output::numeric*(rate->>'output')::numeric)*1000000 END cost
  FROM resolved
), ranked AS (
  SELECT model_key,price_version,cost::double precision cost,ROW_NUMBER() OVER(PARTITION BY model_key,price_version ORDER BY cost::double precision) n,
    COUNT(*) OVER(PARTITION BY model_key,price_version) count FROM repriced WHERE cost IS NOT NULL
), medians AS (
  SELECT model_key,price_version,(MIN(cost)+MAX(cost))/2 median FROM ranked WHERE n IN ((count+1)/2,(count+2)/2) GROUP BY model_key,price_version
), measurements AS (
  SELECT r.session_id,COALESCE(SUM(r.total) FILTER(WHERE r.measured),0)::text measured_atoms,
    COALESCE(SUM(m.median) FILTER(WHERE NOT r.measured AND r.price_state NOT IN ('unpriced','legacy_manual')),0) estimated_atoms,
    BOOL_OR(NOT r.measured AND (r.price_state IN ('unpriced','legacy_manual') OR m.median IS NULL)) unknown,
    BOOL_OR(r.status IN ('queued','running')) running,BOOL_OR(r.started>=$2) after_boundary
  FROM included r LEFT JOIN medians m ON m.model_key=r.model_key AND m.price_version=r.price_version GROUP BY r.session_id
), latest AS (
  SELECT session_id,MAX(GREATEST(started,COALESCE(NULLIF(ended,0),started))) last FROM source WHERE session_id IS NOT NULL GROUP BY session_id
)
SELECT s.id,s.member_id,s.account_id,s.client_session_id,s.protocol,s.enabled,s.nightly_enabled,s.nightly_expires,s.route_version,s.created,s.updated,
  owner.enabled member_enabled,owner.removed_at member_removed,c.query_sequence context_sequence,c.model context_model,c.source_channel context_channel,
  context_key.id context_key_id,context_key.enabled context_key_enabled,context_key.expires context_key_expires,context_key.removed_at context_key_removed,context_key.allowed_models context_allowed_models,w.measured_atoms,w.estimated_atoms,w.unknown,w.running,w.after_boundary,l.last,
  (c.handoff_context='replayable' OR (c.status IN ('queued','running') AND c.handoff_input='replayable'))
  AND NOT EXISTS(SELECT 1 FROM harbor.requests previous WHERE previous.session_id=s.id AND previous.route_version=s.route_version
    AND previous.query_sequence<c.query_sequence AND (previous.ended IS NULL OR previous.ended>=c.started)
    AND previous.handoff_context<>'replayable' AND NOT(previous.status IN ('queued','running') AND previous.handoff_input='replayable')) context_replayable
FROM measurements w JOIN harbor.sessions s ON s.id=w.session_id JOIN harbor.members owner ON owner.id=s.member_id JOIN latest l ON l.session_id=s.id
LEFT JOIN LATERAL(SELECT query_sequence,started,handoff_context,handoff_input,status,key_id,model,source_channel FROM harbor.requests WHERE session_id=s.id AND route_version=s.route_version ORDER BY query_sequence DESC LIMIT 1) c ON true
LEFT JOIN harbor.keys context_key ON context_key.id=c.key_id AND context_key.member_id=s.member_id
ORDER BY s.created,s.id LIMIT $4`;

export function createPostgresPlanningInput(store,{clock=Date.now,maxSessions=20000,maxAccounts=1000,maxVersions=1000}={}){
  if(!store||typeof clock!=='function'||!Number.isInteger(maxSessions)||maxSessions<1||maxSessions>100000||!Number.isInteger(maxAccounts)||maxAccounts<1||maxAccounts>100000||!Number.isInteger(maxVersions)||maxVersions<1||maxVersions>10000)throw new AppError(503,'夜间规划资源配置无效','planning_configuration');
  const packages=createPostgresAccountPackages(store,{clock,maxAccounts}),departments=createRoutingDepartments(store,{clock});
  const capacity=()=>{throw new AppError(503,'夜间规划数据超出配置容量，未生成部分计划','planning_capacity');};
  async function read({at=clock(),window=nightlyWindow(at),requestBoundary=false,quotaBoundary,scope}={}){
    if(quotaBoundary!==undefined&&(!Number.isSafeInteger(quotaBoundary)||quotaBoundary<0))throw new AppError(400,'规划额度边界无效','planning_configuration');
    if(!Number.isSafeInteger(at)||at<0||!window||!Number.isSafeInteger(window.start)||!Number.isSafeInteger(window.end)||window.start>=window.end)throw new AppError(400,'规划时间无效');
    return store.transaction(async()=>{
      scope=await departments.resolve(scope);
      let accounts=await store.all(`SELECT ${scope?'department_id,':''}id,name,kind,auth_mode,enabled,removed_at,state,last_used,quota_json,quota_error,route_block,route_epoch FROM harbor.accounts WHERE kind='codex' AND auth_mode<>'openai_api' AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') AND (route_block IS NULL OR route_block='quota_exhausted') ${scope?'AND department_id=$2':''} ORDER BY id LIMIT $1`,maxAccounts+1,...(scope?[scope.department_id]:[]));if(accounts.length>maxAccounts)capacity();accounts=await packages.decorate(accounts);if(quotaBoundary!==undefined)accounts=accounts.map(account=>{let quota;try{quota=JSON.parse(account.quota_json);}catch{}return quota?.checked>=quotaBoundary&&!account.quota_error?account:{...account,quota_json:null,planningQuotaDenials:codexQuotaDenials(quota)};});
      const versions=await store.all("SELECT DISTINCT price_version FROM harbor.requests WHERE (usage_at>=$1 OR status IN ('queued','running')) AND price_version IS NOT NULL"+(scope?" AND department_id=$3":"")+" ORDER BY price_version LIMIT $2",window.start,maxVersions+1,...(scope?[scope.department_id]:[]));if(versions.length>maxVersions)capacity();
      const rows=await store.all(workloadSql(scope),window.start,window.end,at,maxSessions+1,...(scope?[scope.department_id]:[]));if(rows.length>maxSessions)capacity();
      const fixed=requestBoundary?await store.all("WITH facts AS (SELECT h.session_id,h.origin='manual' AND h.status='waiting' manual,h.origin='automatic' AND h.status='completed' AND g.slot=$1 transferred FROM harbor.session_handoffs h LEFT JOIN harbor.automatic_handoff_groups g ON g.id=h.automatic_group_id WHERE h.session_id=ANY($2::text[]) AND (h.origin='manual' AND h.status='waiting' OR h.origin='automatic' AND h.status='completed' AND g.slot=$1) UNION ALL SELECT m.payload->>'session_id',false,true FROM harbor.nightly_plan_applications a JOIN harbor.nightly_plan_group_results result ON result.plan_id=a.plan_id AND result.status='applied' AND result.moved>0 JOIN harbor.routing_plan_items group_item ON group_item.plan_id=a.plan_id AND group_item.section='groups' AND group_item.ordinal=result.group_ordinal JOIN harbor.routing_plan_items m ON m.plan_id=a.plan_id AND m.section='moves' AND group_item.payload->'accounts' ? (m.payload->>'from') WHERE a.slot=$1 AND m.payload->>'session_id'=ANY($2::text[])) SELECT session_id,BOOL_OR(manual) manual,BOOL_OR(transferred) transferred FROM facts GROUP BY session_id",window.key,rows.map(row=>row.id)):[],fixedById=new Map(fixed.map(row=>[row.session_id,row]));
      const workloads=rows.map(({measured_atoms,estimated_atoms,after_boundary,context_replayable,context_sequence,context_model,context_channel,context_key_id,context_key_enabled,context_key_expires,context_key_removed,context_allowed_models,...row})=>{
        let sourcePermissionAvailable=true;
        if(context_sequence!==null){sourcePermissionAvailable=!!context_key_id&&!!context_key_enabled&&context_key_removed===null&&(context_key_expires===null||context_key_expires>at);if(sourcePermissionAvailable)try{checkAllowedModel(storedModelAllowlist(context_allowed_models),context_model);}catch{sourcePermissionAvailable=false;}}
        return {...row,contextModel:context_model,contextProtocol:context_channel==='responses'||context_channel==='legacy'&&context_replayable===true?'responses':['images/generations','images/edits'].includes(context_channel)?'images':null,measuredAtoms:BigInt(measured_atoms),estimatedAtoms:estimated_atoms,afterBoundary:after_boundary,contextReplayable:context_replayable!==false,sourcePermissionAvailable,...(fixedById.get(row.id)?{pendingManual:fixedById.get(row.id).manual,transferredInSlot:fixedById.get(row.id).transferred}:{})};
      });
      const packagePolicy=scope?(await departments.setting(scope,'account_package_policy')??{enabled:false,smoothing_hours:24,version:0}):await packages.policy();accounts=await annotateExtraPlanning(store,{accounts,workloads,at,packagePolicy,requestBoundary});return {accounts,workloads,at,window,packagePolicy,priceRevisions:versions.map(row=>row.price_version)};
    },{level:'REPEATABLE READ',readOnly:true});
  }
  return {read,plan:async(options={})=>planBalanceWorkloads({...await read(options),quietMinutes:options.quietMinutes,preparing:options.preparing,weighted:options.weighted,requestBoundary:options.requestBoundary})};
}
