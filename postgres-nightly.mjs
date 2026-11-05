import {annotateExtraPlanning,planningDispatchIdentity} from './postgres-extra-planning.mjs';
import {classifyExtraPackage,createPostgresExtraRuntime} from './postgres-extra-runtime.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {accountPackageBudget} from './account-package-budget.mjs';
import {AppError,id,participatesNightly,usageWindow} from './store.mjs';
import {quotaSnapshot,quotaFreshnessMs,planningAccountGroup} from './balancing.mjs';
import {transferSchedule,transferWindow} from './transfer-schedule.mjs';
import {registerRequestHandoffGroup} from './request-handoff-group.mjs';
import {lockPostgresRouting,createPostgresRouter} from './postgres-routing.mjs';
import {createPostgresAutomaticHandoffs} from './postgres-auto-handoff-groups.mjs';
import {createPostgresNightlyRevisions} from './postgres-nightly-revisions.mjs';

const changed=()=>{throw new AppError(409,'夜间整组条件已变化，未迁移本组','nightly_plan_changed');};
const sourceBudget=q=>q.eligible||q.reason==='quota_exhausted'&&!!q.comparison_key;
const packageBasis=q=>JSON.stringify([q.identity,q.windows.map(w=>[w.id,w.minutes,w.remaining,w.unit,w.reset,w.source])]);
const basis=q=>JSON.stringify([q.plan,q.limitReached,q.windows.map(w=>[w.minutes,w.reset,w.used])]);

// Only the active gateway may apply a plan. The worker has no access to its
// preparation handoff and can never use this module to bypass live uploads.
export function createPostgresNightly(store,{preparation,refreshQuotas,clock=Date.now,isStopping=()=>false,sessionHandoffs,handoffAdmin}={}){
  if(sessionHandoffs&&typeof sessionHandoffs.register!=='function')throw new AppError(503,'整组请求交接依赖未配置','nightly_configuration');
  if(!preparation||typeof preparation.reserve!=='function'||typeof refreshQuotas!=='function'||typeof clock!=='function'||typeof isStopping!=='function')throw new AppError(503,'夜间执行依赖未配置','nightly_configuration');
  let pumping,stopping=false,lastError=null;const executions=new Map(),groups=new Set(),router=createPostgresRouter(store,{clock,isStopping});
  const extraRuntime=createPostgresExtraRuntime(store,{clock});
  async function automaticTarget(accountId,{lock=false,model}={}){const account=await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND kind='codex' AND auth_mode='subscription' AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') AND (route_block IS NULL OR route_block='quota_exhausted')"+(lock?' FOR SHARE':''),accountId);if(!account)changed();const classification=await extraRuntime.classify(account,{model,protocol:'responses'});if(['package','package_compatibility'].includes(classification.mode))return account;if(classification.mode!=='extra'||!model||!(await extraRuntime.service.decision(accountId,{model,protocol:'responses',packageAvailable:false})).allowed)changed();return account;}
  const revisions=createPostgresNightlyRevisions(store,{clock});
  const automatic=handoffAdmin?createPostgresAutomaticHandoffs(store,{gate:sessionHandoffs,contexts:handoffAdmin.contexts,targetAccount:automaticTarget,clock,isStopping:()=>stopping||isStopping(),onSettled:slot=>store.transaction(()=>revisions.summary(slot))}):null;
  if(automatic)handoffAdmin.attachAutomatic(automatic);
  const modern=plan=>!!automatic&&!!plan.header.requestBoundary;
  const runSource=plan=>modern(plan)?'harbor.nightly_plan_applications WHERE plan_id=$1':'harbor.balance_runs WHERE day=$1';
  const runKey=plan=>modern(plan)?plan.id:plan.target;
  const resultSource=plan=>modern(plan)?'harbor.nightly_plan_group_results WHERE plan_id=$1':'harbor.nightly_group_results WHERE day=$1';
  async function lockedRun(plan){if(modern(plan))await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1 FOR UPDATE',plan.target);return store.get('SELECT plan_id,status,attempts FROM '+runSource(plan)+' FOR UPDATE',runKey(plan));}
  const totalsFor=plan=>store.get("SELECT COUNT(*) groups,COUNT(*) FILTER(WHERE status='skipped') skipped,COALESCE(SUM(moved),0)::int applied FROM "+resultSource(plan),runKey(plan));
  const alive=()=>{if(stopping||isStopping())throw new AppError(503,'夜间执行正在停止','service_stopping');};
  const withinWindow=plan=>{alive();if(plan.expires<=clock()||transferWindow(clock(),plan.header.configuration?.transfer_schedule).key!==plan.target)throw new AppError(410,'夜间执行窗口已结束','nightly_plan_expired');};
  const configuration=async()=>Object.fromEntries((await store.all("SELECT name,value FROM harbor.gateway_settings WHERE name IN ('balancing','routing_strategy','routing_weights','transfer_schedule','request_handoff_policy','account_package_policy') ORDER BY name")).map(row=>[row.name,JSON.parse(row.value)]));
  const scheduleConfig=async()=>transferSchedule(JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='transfer_schedule'"))?.value||'{}'));
  async function checkpoint(plan,ordinal,status,reason,moved){
    withinWindow(plan);await store.run('INSERT INTO '+(modern(plan)?'harbor.nightly_plan_group_results':'harbor.nightly_group_results')+' VALUES($1,$2,$3,$4,$5,$6)',runKey(plan),ordinal,clock(),status,reason,moved);if(modern(plan))await revisions.summary(plan.target);withinWindow(plan);
  }
  async function group(plan,entry,registerOnly=false){
    const day=plan.target,summary=entry.payload,accounts=summary.accounts;
    if(!Array.isArray(accounts)||!accounts.length)changed();
    const moves=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='moves' AND payload->>'from'=ANY($2::text[]) ORDER BY ordinal",plan.id,accounts)).map(row=>row.payload);
    const sessions=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='sessions' AND payload->>'id'=ANY($2::text[])",plan.id,moves.map(m=>m.session_id))).map(row=>row.payload),expected=new Map(sessions.map(s=>[s.id,s]));
    const owners=[...new Set(sessions.map(s=>s.member_id))].sort();let release;
    const skipped=async error=>{
      if(!(error instanceof AppError)||error.status!==409)throw error;
      await store.transaction(async()=>{alive();const run=await lockedRun(plan);if(run?.plan_id!==plan.id||run.status!=='running')throw error;if(!await store.get('SELECT group_ordinal FROM '+resultSource(plan)+' AND group_ordinal=$2',runKey(plan),entry.ordinal))await checkpoint(plan,entry.ordinal,'skipped',error.code,0);});
    };
    try{
      const execute=async(signal,beforeBindings)=>{
      const ensure=()=>{alive();if(signal?.aborted)throw signal.reason;};ensure();
      if(moves.length&&!sessionHandoffs){release=preparation.reserve(owners);if(!release)throw new AppError(409,'员工仍在准备请求，本组不迁移','session_busy');}
      return store.transaction(async()=>{
        ensure();const run=await lockedRun(plan);if(run?.plan_id!==plan.id||run.status!=='running')changed();
        if(await store.get('SELECT group_ordinal FROM '+resultSource(plan)+' AND group_ordinal=$2',runKey(plan),entry.ordinal))return;
        withinWindow(plan);
        if(!moves.length){await checkpoint(plan,entry.ordinal,'applied',null,0);return;}
        if(!plan.header.settings?.nightly)changed();
        const members=await store.all('SELECT id,enabled,removed_at FROM harbor.members WHERE id=ANY($1::text[]) ORDER BY id FOR SHARE',owners);ensure();if(members.length!==owners.length||members.some(m=>!m.enabled||m.removed_at!==null)||sessions.length!==moves.length)changed();
        await lockPostgresRouting(store);ensure();
        const config=await configuration();if(!(await store.get('SELECT $1::jsonb=$2::jsonb equal',JSON.stringify(config),JSON.stringify(plan.header.configuration))).equal)changed();
        let currentAccounts=await createPostgresAccountPackages(store,{clock}).decorate(await store.all('SELECT id,kind,auth_mode,enabled,removed_at,state,route_block,route_epoch,quota_json FROM harbor.accounts WHERE id=ANY($1::text[]) ORDER BY id FOR SHARE',accounts));
        const oldAccounts=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='accounts' AND payload->>'id'=ANY($2::text[])",plan.id,accounts)).map(row=>row.payload),old=new Map(oldAccounts.map(a=>[a.id,a])),dispatchAware=oldAccounts.every(account=>account.dispatch_planning),scopeFacts=[],seenScopes=new Set(),packagePolicy=plan.header.configuration.account_package_policy??{enabled:false};if(dispatchAware){for(const account of oldAccounts)for(const model of account.dispatch_planning.models??[]){const group=planningAccountGroup(account,{at:plan.created,packagePolicy}).key,key=JSON.stringify([group,model]);if(seenScopes.has(key))continue;seenScopes.add(key);scopeFacts.push({account_id:account.id,protocol:'responses',contextProtocol:'responses',contextModel:model});}currentAccounts=await annotateExtraPlanning(store,{accounts:currentAccounts,workloads:scopeFacts,at:clock(),packagePolicy,requestBoundary:!!plan.header.requestBoundary});for(const account of currentAccounts)if(planningDispatchIdentity(account)!==planningDispatchIdentity(old.get(account.id)??{}))changed();}const current=new Map(currentAccounts.map(a=>[a.id,a]));
        if(currentAccounts.length!==accounts.length||oldAccounts.length!==accounts.length)changed();
        for(const account of currentAccounts){const previous=old.get(account.id),capacity=plan.header.configuration.account_package_policy?.enabled&&account.package_selection?.mode!=='legacy',nowBudget=capacity?accountPackageBudget(account,{at:clock(),enabled:true,protocol:'responses'}):null,oldBudget=capacity?accountPackageBudget(previous,{at:plan.created,enabled:true,protocol:'responses'}):null,nowQuota=capacity&&sourceBudget(nowBudget)?nowBudget:quotaSnapshot(account,clock()),oldQuota=capacity&&sourceBudget(oldBudget)?oldBudget:quotaSnapshot(previous,plan.created);if(plan.header.configuration.account_package_policy?.enabled&&accountPackageBudget(account,{at:clock(),enabled:true,protocol:'responses'}).identity!==accountPackageBudget(previous,{at:plan.created,enabled:true,protocol:'responses'}).identity)changed();if(!previous||previous.route_epoch!==account.route_epoch||!account.enabled||account.removed_at!==null||account.kind!=='codex'||account.auth_mode==='openai_api'||!['ready','degraded'].includes(account.state)||account.route_block&&account.route_block!=='quota_exhausted'||!nowQuota||!oldQuota||(capacity?(!sourceBudget(nowBudget)||!sourceBudget(oldBudget)||packageBasis(nowBudget)!==packageBasis(oldBudget)):basis(nowQuota)!==basis(oldQuota)))changed();}
        const currentSessions=await store.all('SELECT * FROM harbor.sessions WHERE id=ANY($1::text[]) ORDER BY id FOR UPDATE',moves.map(m=>m.session_id)),byId=new Map(currentSessions.map(s=>[s.id,s]));ensure();
        const busy=plan.header.requestBoundary?await store.get("SELECT id FROM harbor.requests WHERE session_id=ANY($1::text[]) AND status IN ('queued','running') LIMIT 1",moves.map(m=>m.session_id)):await store.get("SELECT id FROM harbor.requests WHERE session_id=ANY($1::text[]) AND (status IN ('queued','running') OR started>$2 OR ended>$2) LIMIT 1",moves.map(m=>m.session_id),clock()-plan.header.settings.quietMinutes*60000);if(busy)throw new AppError(409,'会话仍在执行或静默期未结束，本组不迁移','session_busy');
        const latestModels=dispatchAware||plan.header.configuration.account_package_policy?.enabled?new Map((await store.all('SELECT DISTINCT ON(r.session_id) r.session_id,r.model,r.source_channel,r.handoff_context FROM harbor.requests r JOIN harbor.sessions s ON s.id=r.session_id AND s.route_version=r.route_version WHERE r.session_id=ANY($1::text[]) ORDER BY r.session_id,r.query_sequence DESC',moves.map(move=>move.session_id))).map(row=>[row.session_id,row])):new Map();
        for(const move of moves){const latest=latestModels.get(move.session_id),latestModel=latest?.model,latestProtocol=latest?.source_channel==='responses'||latest?.source_channel==='legacy'&&latest?.handoff_context==='replayable'?'responses':['images/generations','images/edits'].includes(latest?.source_channel)?'images':undefined,source=current.get(move.from);if(plan.header.configuration.account_package_policy?.enabled&&source?.package_selection?.mode!=='legacy'&&!sourceBudget(accountPackageBudget(source,{at:clock(),enabled:true,protocol:latestProtocol,model:latestModel})))changed();const session=byId.get(move.session_id),original=expected.get(move.session_id),target=current.get(move.to),targetClass=target&&classifyExtraPackage(target,{at:clock(),protocol:latestProtocol,model:latestModel,packagePolicy}),extraTarget=dispatchAware&&latestProtocol==='responses'&&summary.dispatchTier==='extra'&&target?.dispatch_planning.mode==='extra'&&target.dispatch_planning.allowed&&target.dispatch_planning.models.includes(latestModel),normalTarget=targetClass&&['package','package_compatibility'].includes(targetClass.mode)&&(!dispatchAware||['package','package_compatibility'].includes(target.dispatch_planning.mode));if(!session||!original||!target||!session.enabled||session.member_id!==original.member_id||session.protocol!=='responses'||session.account_id!==move.from||session.route_version!==move.version||!participatesNightly(session,clock())||!(extraTarget||normalTarget&&(plan.header.configuration.account_package_policy?.enabled&&target.package_selection?.mode!=='legacy'?accountPackageBudget(target,{at:clock(),enabled:true,protocol:latestProtocol,model:latestModel}).eligible:quotaSnapshot(target,clock())?.remaining>0)))changed();}
        if(beforeBindings)await beforeBindings();
        for(let start=0;start<moves.length;start+=256){
          ensure();const batch=moves.slice(start,start+256).map(move=>({...move,thread:id(),audit:id(),detail:JSON.stringify({session:move.session_id,from:move.from,to:move.to,reason:'nightly',plan:plan.id,version:move.version+1})})),body=JSON.stringify(batch);
          const updated=await store.run(`UPDATE harbor.sessions s SET account_id=p.to,upstream_thread=p.thread,route_version=s.route_version+1,handoff_requires_replay=CASE WHEN $2 THEN 1 ELSE s.handoff_requires_replay END FROM jsonb_to_recordset($1::jsonb) AS p(session_id text,"from" text,"to" text,version bigint,thread text) WHERE s.id=p.session_id AND s.account_id=p.from AND s.route_version=p.version`,body,!!plan.header.requestBoundary);if(updated.changes!==batch.length)changed();
          await store.run("INSERT INTO harbor.audit SELECT p.audit,$2,'session.route',p.detail FROM jsonb_to_recordset($1::jsonb) AS p(audit text,detail text)",body,clock());
        }
        withinWindow(plan);if(dispatchAware){const finalAccounts=await annotateExtraPlanning(store,{accounts:currentAccounts,workloads:scopeFacts,at:clock(),packagePolicy,requestBoundary:!!plan.header.requestBoundary});if(finalAccounts.some(account=>planningDispatchIdentity(account)!==planningDispatchIdentity(old.get(account.id)??{})))changed();}if(currentAccounts.some(account=>plan.header.configuration.account_package_policy?.enabled&&account.package_selection?.mode!=='legacy'?!sourceBudget(accountPackageBudget(account,{at:clock(),enabled:true,protocol:'responses'})):!quotaSnapshot(account,clock())))changed();await checkpoint(plan,entry.ordinal,'applied',null,moves.length);ensure();
      });
      };
      if(!moves.length||!sessionHandoffs)return await execute();
      if(plan.header.requestBoundary&&automatic){const registered=await automatic.start({plan,entry,moves,sessions},execute),done=registered.done.catch(skipped);done.catch(()=>{});return registerOnly?{...registered,done}:await done;}
      alive();const intent=registerRequestHandoffGroup(sessionHandoffs,moves.map(move=>({memberId:expected.get(move.session_id)?.member_id,sessionId:move.session_id,decisionId:plan.id+':'+entry.ordinal+':'+move.session_id})));groups.add(intent);
      try{
        intent.activate(execute);const result=await intent.done;
        if(result.state!=='completed')throw result.state==='failed'&&result.error?result.error:new AppError(409,'整组请求交接未完成',result.state);
        return result.value;
      }finally{groups.delete(intent);}

    }catch(error){
      await skipped(error);
    }finally{release?.();}
  }
  async function performApply(planId){
    alive();const plan=await store.get("SELECT p.*,j.target,j.payload,j.max_attempts FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id WHERE p.id=$1 AND j.kind='routing.nightly' AND j.status='completed'",planId);
    if(plan?.header.requestBoundary&&!automatic)throw new AppError(503,'请求间自动交接服务未接入，不能应用此计划','nightly_configuration');
    const currentWindow=transferWindow(clock(),await scheduleConfig());
    if(!plan||!currentWindow.due||plan.header.purpose!=='nightly'||plan.target!==currentWindow.key||plan.target!==plan.header.window?.key||!(plan.id==='routing.nightly:'+plan.target||modern(plan)&&!!await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE job_id=$1 AND slot=$2',plan.id,plan.target))||plan.payload.boundary!==(plan.header.window.boundary??plan.header.window.end)||clock()<plan.payload.boundary)throw new AppError(409,'此计划不能用于夜间执行','nightly_plan_invalid');
    const run=await store.transaction(async()=>{
      alive();if(modern(plan))await revisions.ensure(plan);else await store.run("INSERT INTO harbor.balance_runs(day,started,status,summary,plan_id) VALUES($1,$2,'running',$3,$4) ON CONFLICT(day) DO NOTHING",plan.target,clock(),JSON.stringify({metric:plan.header.metric,window:plan.header.window,plan_id:plan.id}),plan.id);
      const current=await lockedRun(plan);if(current.plan_id!==plan.id)throw new AppError(409,'此日期已有其他计划','nightly_plan_conflict');
      if(current.status==='running'){
        if(current.attempts>=plan.max_attempts){await store.run("UPDATE "+runSource(plan).split(' WHERE ')[0]+" SET status='failed',ended=$2,summary=$3 WHERE "+(modern(plan)?'plan_id':'day')+"=$1",runKey(plan),clock(),JSON.stringify({plan_id:plan.id,reason:'application_attempts_exhausted'}));current.status='failed';if(modern(plan))await revisions.summary(plan.target);}
        else await store.run('UPDATE '+runSource(plan).replace(' WHERE ',' SET attempts=attempts+1 WHERE '),runKey(plan));
      }
      alive();return current;
    });if(run.status!=='running')return {completed:run.status==='completed'};
    try{
    withinWindow(plan);
    let after=0;const pending=[];
    for(;;){alive();const entry=await store.get("SELECT ordinal,payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='groups' AND ordinal>$2 ORDER BY ordinal LIMIT 1",plan.id,after);if(!entry)break;const registered=await group(plan,entry,!!automatic&&!!plan.header.requestBoundary);if(registered?.done)pending.push(registered.done);after=entry.ordinal;}
    await Promise.all(pending);
    return await store.transaction(async()=>{withinWindow(plan);const current=await lockedRun(plan);if(current.plan_id!==plan.id)changed();const totals=await totalsFor(plan);if(totals.groups!==plan.header.counts.groups)throw new AppError(503,'夜间计划分组不完整','nightly_plan_invalid');
      withinWindow(plan);const manual=plan.header.skipReason==='manual_weights_active'||!!await store.get('SELECT group_ordinal FROM '+resultSource(plan)+" AND reason='manual_weights_active' LIMIT 1",runKey(plan));const result={plan_id:plan.id,metric:plan.header.metric,window:plan.header.window,planned:plan.header.counts.moves,skippedItems:plan.header.counts.skipped,...totals,...(manual?{reason:'manual_weights_active',status:'skipped'}:{})};await store.run('UPDATE '+runSource(plan).split(' WHERE ')[0]+" SET status=$4,ended=$2,summary=$3 WHERE "+(modern(plan)?'plan_id':'day')+'=$1',runKey(plan),clock(),JSON.stringify(result),manual?'interrupted':'completed');if(modern(plan))await revisions.summary(plan.target);withinWindow(plan);return result;});
    }catch(error){
      if(error?.code!=='nightly_plan_expired')throw error;
      return store.transaction(async()=>{alive();if(modern(plan)){await lockedRun(plan);await store.run("UPDATE harbor.nightly_plan_applications SET status='interrupted',ended=$1,summary=$2 WHERE plan_id=$3 AND status='running'",clock(),JSON.stringify({plan_id:plan.id,reason:'planning_expired'}),plan.id);const window=transferWindow(clock(),await scheduleConfig());if(window.due&&window.key===plan.target&&(await configuration()).balancing?.nightly)await revisions.request(plan,'planning_expired');else await revisions.summary(plan.target);}else await store.run("UPDATE harbor.balance_runs SET status='failed',ended=$1,summary=$2 WHERE day=$3 AND plan_id=$4 AND status='running'",clock(),JSON.stringify({plan_id:plan.id,reason:'planning_expired'}),plan.target,plan.id);alive();return {completed:false};});
    }
  }
  function apply(planId){if(store.context.getStore())return Promise.reject(new AppError(503,'夜间执行不能嵌套在其他事务内','nightly_transaction_invalid'));if(executions.has(planId))return executions.get(planId);const operation=performApply(planId).finally(()=>executions.delete(planId));executions.set(planId,operation);return operation;}
  function runOnce(){
    if(stopping||isStopping())return Promise.resolve();if(pumping)return pumping;
    pumping=(async()=>{
      if(automatic)await automatic.reconcile();
      if(automatic?.takeRecovery())await recover({restart:false});
      alive();const window=transferWindow(clock(),await scheduleConfig()),day=window.key;if(automatic)await revisions.reconcile(day);if(!window.due)return;
      await store.transaction(async()=>{alive();await store.run("UPDATE harbor.balance_runs SET status='interrupted',ended=$1 WHERE status='running' AND plan_id IS NOT NULL AND started<$2",clock(),window.end);alive();});
      await store.transaction(async()=>{alive();await store.run("INSERT INTO harbor.balance_runs(day,started,ended,status,summary,plan_id) SELECT j.target,j.created,$1,'interrupted',jsonb_build_object('reason','application_not_started','plan_id',p.id,'planned_at',p.created)::text,p.id FROM harbor.background_jobs j JOIN harbor.routing_plans p ON p.id=j.id WHERE j.kind='routing.nightly' AND j.status='completed' AND (j.payload->>'boundary')::bigint<$2 AND j.created>=$3 AND j.id='routing.nightly:'||j.target AND p.header->>'purpose'='nightly' AND NOT EXISTS(SELECT 1 FROM harbor.balance_runs r WHERE r.day=j.target) ORDER BY j.created,j.id LIMIT 1 ON CONFLICT(day) DO NOTHING",clock(),window.end,usageWindow(clock()).start);alive();});
      const failed=await store.get("SELECT j.id,j.target,j.created,j.updated,j.status FROM harbor.background_jobs j WHERE j.kind='routing.nightly' AND (j.payload->>'boundary')::bigint<=$1 AND j.created>=$2 AND j.status IN ('failed','cancelled') AND j.id='routing.nightly:'||j.target AND NOT EXISTS(SELECT 1 FROM harbor.balance_runs r WHERE r.day=j.target) ORDER BY j.created,j.id LIMIT 1",clock(),usageWindow(clock()).start);
      if(failed)await store.transaction(async()=>{alive();await store.run("INSERT INTO harbor.balance_runs(day,started,ended,status,summary) VALUES($1,$2,$3,$4,$5) ON CONFLICT(day) DO NOTHING",failed.target,failed.created,failed.updated,failed.status==='cancelled'?'cancelled':'failed',JSON.stringify({reason:failed.status==='cancelled'?'planning_cancelled':'planning_failed',job_id:failed.id}));alive();});
      const waiting=await store.get("SELECT id FROM harbor.background_jobs WHERE kind='routing.nightly' AND target=$1 AND status IN ('pending','running') LIMIT 1",day);
      if(waiting&&!await store.get('SELECT day FROM harbor.nightly_cycle_ready WHERE day=$1 AND at>=$2',day,clock()-quotaFreshnessMs)){
        await refreshQuotas({force:true});alive();if(transferWindow(clock(),await scheduleConfig()).key!==day)changed();await store.transaction(async()=>{await lockPostgresRouting(store);alive();await router.syncWeights('nightly',clock());await store.run('INSERT INTO harbor.nightly_cycle_ready VALUES($1,$2) ON CONFLICT(day) DO UPDATE SET at=EXCLUDED.at',day,clock());alive();});
      }
      const mode=(await configuration()).request_handoff_policy?.mode==='request-boundary'&&!!automatic;
      const plan=await store.get(mode?"SELECT p.*,j.target FROM harbor.background_jobs j JOIN harbor.routing_plans p ON p.id=j.id WHERE j.kind='routing.nightly' AND j.status='completed' AND j.target=$1 AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_applications a WHERE a.plan_id=p.id AND a.status<>'running') AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_revisions r WHERE r.replaces_plan_id=p.id) ORDER BY j.created DESC,j.id DESC LIMIT 1":"SELECT j.id FROM harbor.background_jobs j JOIN harbor.routing_plans p ON p.id=j.id WHERE j.kind='routing.nightly' AND j.status='completed' AND j.target=$1 AND NOT EXISTS(SELECT 1 FROM harbor.balance_runs r WHERE r.plan_id=p.id AND r.status IN ('completed','failed','interrupted')) ORDER BY j.created,j.id LIMIT 1",day);
      if(plan){if(mode&&plan.expires<=clock()){await store.transaction(async()=>{await revisions.ensure(plan);await revisions.request(plan,'planning_expired');});}else if(mode)void apply(plan.id).catch(()=>{lastError='nightly_apply_failed';});else await apply(plan.id);}lastError=null;
    })().catch(()=>{lastError='nightly_apply_failed';}).finally(()=>{pumping=null;});return pumping;
  }
  async function recover({restart=true}={}){
    if(!automatic||(await configuration()).request_handoff_policy?.mode!=='request-boundary')return;const rows=await store.all("SELECT g.*,p.created planned_at,p.header,p.expires plan_expires,j.target,j.payload,j.max_attempts FROM harbor.automatic_handoff_groups g JOIN harbor.routing_plans p ON p.id=g.plan_id JOIN harbor.background_jobs j ON j.id=p.id WHERE g.status='waiting' ORDER BY g.requested,g.id LIMIT 20001");if(rows.length>20000)throw new AppError(503,'自动交接恢复超过容量','session_handoff_busy');
    const plans=new Set();for(const row of rows){alive();if(automatic.has(row.id))continue;const window=transferWindow(clock(),await scheduleConfig()),settings=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='balancing'"))?.value||'{}');
      if(restart){await automatic.discard(row);continue;}
      if(row.expires<=clock()||row.plan_expires<=clock()||row.slot!==window.key||!window.due||!settings.nightly){await automatic.discard(row);continue;}
      if(await store.get("SELECT id FROM harbor.session_handoffs WHERE origin='manual' AND status='waiting' AND replaces_handoff_id IN (SELECT id FROM harbor.session_handoffs WHERE automatic_group_id=$1) LIMIT 1",row.id))continue;
      if(await store.get("SELECT id FROM harbor.session_handoffs WHERE automatic_group_id=$1 AND status<>'waiting' LIMIT 1",row.id)){await automatic.discard(row);continue;}
      const plan={id:row.plan_id,created:row.planned_at,expires:row.plan_expires,header:row.header,target:row.target,payload:row.payload,max_attempts:row.max_attempts},entry=await store.get("SELECT ordinal,payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='groups' AND ordinal=$2",row.plan_id,row.group_ordinal);
      if(!entry){await automatic.discard(row);continue;}await group(plan,entry,true);plans.add(plan.id);
    }
    if(restart){const window=transferWindow(clock(),await scheduleConfig()),config=await configuration();if(window.due&&config.balancing?.nightly&&config.request_handoff_policy?.mode==='request-boundary'){const candidates=await store.all("SELECT p.*,j.target FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id LEFT JOIN harbor.nightly_plan_applications a ON a.plan_id=p.id WHERE j.kind='routing.nightly' AND j.status='completed' AND j.target=$1 AND COALESCE(a.status,'running')='running' AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_revisions r WHERE r.replaces_plan_id=p.id) ORDER BY p.created,p.id",window.key);for(const candidate of candidates){await store.transaction(async()=>{await revisions.ensure(candidate);await revisions.request(candidate,'gateway_restart');});}}}
    for(const planId of plans)void apply(planId).catch(()=>{lastError='nightly_apply_failed';});
  }
  return {apply,runOnce,recover,close:async()=>{stopping=true;await automatic?.close();for(const intent of groups)intent.cancel();await pumping;await Promise.allSettled([...executions.values()]);},status:()=>({stopping,running:!!pumping||executions.size>0,waitingGroups:groups.size+(automatic?.status().pendingGroups||0),lastError,automatic:automatic?.status()??null})};
}
