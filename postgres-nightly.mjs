import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {verifyDepartmentIsolationAvailability,assertDepartmentIsolationReadiness,isPostgresDepartmentActorService} from './postgres-departments.mjs';
import {requireDepartmentScope} from './department-access.mjs';
import {departmentPlanningJobId} from './postgres-planning-jobs.mjs';
import {annotateExtraPlanning,planningDispatchIdentity} from './postgres-extra-planning.mjs';
import {classifyExtraPackage,createPostgresExtraRuntime} from './postgres-extra-runtime.mjs';
import {createPostgresAccountPackages} from './postgres-account-packages.mjs';
import {accountPackageBudget} from './account-package-budget.mjs';
import {AppError,id,participatesNightly,usageWindow} from './store.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';
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
export function createPostgresNightly(store,{preparation,refreshQuotas,clock=Date.now,isStopping=()=>false,sessionHandoffs,handoffAdmin,departmentActors,departmentScopes}={}){
  if(sessionHandoffs&&typeof sessionHandoffs.register!=='function')throw new AppError(503,'整组请求交接依赖未配置','nightly_configuration');
  if(!preparation||typeof preparation.reserve!=='function'||typeof refreshQuotas!=='function'||typeof clock!=='function'||typeof isStopping!=='function')throw new AppError(503,'夜间执行依赖未配置','nightly_configuration');
  let initialization;async function installed(){const first=initialization===undefined,pending=initialization??=verifyDepartmentIsolationAvailability(store);let value;try{value=await pending;}catch(error){if(initialization===pending)initialization=undefined;throw error;}if(!first)await assertDepartmentIsolationReadiness(store,value);return value;}
  const access=createRoutingDepartments(store,{clock}),planScopes=new WeakMap();
  const scopeOf=plan=>planScopes.get(plan)??null,scopeOptions=plan=>({scope:scopeOf(plan)??undefined});
  const own=(scope,index,alias='')=>scope?' AND '+(alias?alias+'.':'')+'department_id=$'+index:'',args=scope=>scope?[scope.department_id]:[];
  async function scopes(){if(!await installed())return [undefined];if(typeof departmentScopes!=='function')throw new AppError(503,'夜间部门调度入口未配置','nightly_configuration');const values=await departmentScopes({includeDisabled:true});if(!Array.isArray(values))throw new AppError(503,'夜间部门列表无效','nightly_configuration');const seen=new Set();for(const value of values){const actor=requireDepartmentScope(value,{allowSystem:true,roles:['system']});if(actor.purpose!=='routing.nightly'||seen.has(actor.department_id))throw new AppError(403,'夜间部门身份无效','department_forbidden');seen.add(actor.department_id);}return values;}
  async function authorize(plan,{lock=false}={}){
    if(!await installed()){await access.resolve(undefined);planScopes.set(plan,null);return null;}
    if(!isPostgresDepartmentActorService(departmentActors,store))throw new AppError(503,'夜间计划身份校验未配置','nightly_configuration');
    const scope=await departmentActors.authorizeNightlyPlanActor(plan.id,{lock});access.same(scope,plan);planScopes.set(plan,scope);return scope;
  }
  let pumping,stopping=false,lastError=null;const executions=new Map(),groups=new Set(),groupScopes=new WeakMap(),router=createPostgresRouter(store,{clock,isStopping});
  const extraRuntime=createPostgresExtraRuntime(store,{clock});
  async function automaticTarget(accountId,{lock=false,model,scope}={}){scope=await access.resolve(scope,{lock});const account=await store.get("SELECT * FROM harbor.accounts WHERE id=$1 AND kind='codex' AND auth_mode='subscription' AND enabled=1 AND removed_at IS NULL AND state IN ('ready','degraded') AND (route_block IS NULL OR route_block='quota_exhausted')"+(lock?' FOR SHARE':''),accountId);if(!account)changed();access.same(scope,account);const classification=await extraRuntime.classify(account,{model,protocol:'responses',scope:scope??undefined});if(['package','package_compatibility'].includes(classification.mode))return account;if(classification.mode!=='extra'||!model||!(await extraRuntime.service.decision(accountId,{model,protocol:'responses',packageAvailable:false})).allowed)changed();return account;}
  const revisions=createPostgresNightlyRevisions(store,{clock,departmentActors});
  const automatic=handoffAdmin?createPostgresAutomaticHandoffs(store,{gate:sessionHandoffs,contexts:handoffAdmin.contexts,targetAccount:automaticTarget,clock,isStopping:()=>stopping||isStopping(),onSettled:(slot,options)=>store.transaction(()=>revisions.summary(slot,options))}):null;
  if(automatic)handoffAdmin.attachAutomatic(automatic);
  const modern=plan=>!!automatic&&!!plan.header.requestBoundary;
  const runSource=plan=>modern(plan)?'harbor.nightly_plan_applications WHERE plan_id=$1':(scopeOf(plan)?'harbor.balance_runs WHERE plan_id=$1':'harbor.balance_runs WHERE day=$1');
  const runKey=plan=>modern(plan)||scopeOf(plan)?plan.id:plan.target;
  const resultSource=plan=>modern(plan)?'harbor.nightly_plan_group_results WHERE plan_id=$1':(scopeOf(plan)?'harbor.nightly_group_results WHERE (department_id,day)=(SELECT department_id,target FROM harbor.background_jobs WHERE id=$1)':'harbor.nightly_group_results WHERE day=$1');
  async function lockedRun(plan){await authorize(plan,{lock:true});if(modern(plan))await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1'+own(scopeOf(plan),2)+' FOR UPDATE',plan.target,...args(scopeOf(plan)));return store.get('SELECT plan_id,status,attempts FROM '+runSource(plan)+' FOR UPDATE',runKey(plan));}
  const totalsFor=plan=>store.get("SELECT COUNT(*) groups,COUNT(*) FILTER(WHERE status='skipped') skipped,COALESCE(SUM(moved),0)::int applied FROM "+resultSource(plan),runKey(plan));
  const alive=()=>{if(stopping||isStopping())throw new AppError(503,'夜间执行正在停止','service_stopping');};
  const withinWindow=plan=>{alive();if(plan.expires<=clock()||transferWindow(clock(),plan.header.configuration?.transfer_schedule).key!==plan.target)throw new AppError(410,'夜间执行窗口已结束','nightly_plan_expired');};
  const configuration=async scope=>Object.fromEntries(await access.settings(scope,['balancing','routing_strategy','routing_weights','transfer_schedule','request_handoff_policy','account_package_policy']));
  const scheduleConfig=async scope=>transferSchedule(await access.setting(scope,'transfer_schedule')??{});
  async function checkpoint(plan,ordinal,status,reason,moved){
    const scope=scopeOf(plan);withinWindow(plan);await authorize(plan,{lock:true});
    await store.run('INSERT INTO '+(modern(plan)?'harbor.nightly_plan_group_results(plan_id':'harbor.nightly_group_results(day')+',group_ordinal,at,status,reason,moved'+(scope?',department_id':'')+') VALUES($1,$2,$3,$4,$5,$6'+(scope?',$7':'')+')',modern(plan)?plan.id:plan.target,ordinal,clock(),status,reason,moved,...args(scope));if(modern(plan))await revisions.summary(plan.target,scopeOptions(plan));withinWindow(plan);
  }
  async function group(plan,entry,registerOnly=false){
    const scope=await authorize(plan),day=plan.target,summary=entry.payload,accounts=summary.accounts;
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
        const members=await store.all('SELECT '+(scope?'department_id,':'')+'id,enabled,removed_at FROM harbor.members WHERE id=ANY($1::text[]) ORDER BY id FOR SHARE',owners);ensure();for(const member of members)access.same(scope,member);if(members.length!==owners.length||members.some(m=>!m.enabled||m.removed_at!==null)||sessions.length!==moves.length)changed();
        await lockPostgresRouting(store);ensure();
        const config=await configuration(scope);if(!(await store.get('SELECT $1::jsonb=$2::jsonb equal',JSON.stringify(config),JSON.stringify(plan.header.configuration))).equal)changed();
        let currentAccounts=await createPostgresAccountPackages(store,{clock}).decorate(await store.all('SELECT '+(scope?'department_id,':'')+'id,kind,auth_mode,enabled,removed_at,state,route_block,route_epoch,quota_json FROM harbor.accounts WHERE id=ANY($1::text[]) ORDER BY id FOR SHARE',accounts));
        const oldAccounts=(await store.all("SELECT payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='accounts' AND payload->>'id'=ANY($2::text[])",plan.id,accounts)).map(row=>row.payload),old=new Map(oldAccounts.map(a=>[a.id,a])),dispatchAware=oldAccounts.every(account=>account.dispatch_planning),scopeFacts=[],seenScopes=new Set(),packagePolicy=plan.header.configuration.account_package_policy??{enabled:false};if(dispatchAware){for(const account of oldAccounts)for(const model of account.dispatch_planning.models??[]){const group=planningAccountGroup(account,{at:plan.created,packagePolicy}).key,key=JSON.stringify([group,model]);if(seenScopes.has(key))continue;seenScopes.add(key);scopeFacts.push({account_id:account.id,protocol:'responses',contextProtocol:'responses',contextModel:model});}currentAccounts=await annotateExtraPlanning(store,{accounts:currentAccounts,workloads:scopeFacts,at:clock(),packagePolicy,requestBoundary:!!plan.header.requestBoundary});for(const account of currentAccounts)if(planningDispatchIdentity(account)!==planningDispatchIdentity(old.get(account.id)??{}))changed();}const current=new Map(currentAccounts.map(a=>[a.id,a]));
        if(currentAccounts.length!==accounts.length||oldAccounts.length!==accounts.length)changed();
        for(const account of currentAccounts){const previous=old.get(account.id),capacity=plan.header.configuration.account_package_policy?.enabled&&account.package_selection?.mode!=='legacy',nowBudget=capacity?accountPackageBudget(account,{at:clock(),enabled:true,protocol:'responses'}):null,oldBudget=capacity?accountPackageBudget(previous,{at:plan.created,enabled:true,protocol:'responses'}):null,nowQuota=capacity&&sourceBudget(nowBudget)?nowBudget:quotaSnapshot(account,clock()),oldQuota=capacity&&sourceBudget(oldBudget)?oldBudget:quotaSnapshot(previous,plan.created);if(plan.header.configuration.account_package_policy?.enabled&&accountPackageBudget(account,{at:clock(),enabled:true,protocol:'responses'}).identity!==accountPackageBudget(previous,{at:plan.created,enabled:true,protocol:'responses'}).identity)changed();if(!previous||previous.route_epoch!==account.route_epoch||!account.enabled||account.removed_at!==null||account.kind!=='codex'||account.auth_mode==='openai_api'||!['ready','degraded'].includes(account.state)||account.route_block&&account.route_block!=='quota_exhausted'||!nowQuota||!oldQuota||(capacity?(!sourceBudget(nowBudget)||!sourceBudget(oldBudget)||packageBasis(nowBudget)!==packageBasis(oldBudget)):basis(nowQuota)!==basis(oldQuota)))changed();}
        const currentSessions=await store.all('SELECT * FROM harbor.sessions WHERE id=ANY($1::text[]) ORDER BY id FOR UPDATE',moves.map(m=>m.session_id)),byId=new Map(currentSessions.map(s=>[s.id,s]));ensure();
        const busy=plan.header.requestBoundary?await store.get("SELECT id FROM harbor.requests WHERE session_id=ANY($1::text[]) AND status IN ('queued','running') LIMIT 1",moves.map(m=>m.session_id)):await store.get("SELECT id FROM harbor.requests WHERE session_id=ANY($1::text[]) AND (status IN ('queued','running') OR started>$2 OR ended>$2) LIMIT 1",moves.map(m=>m.session_id),clock()-plan.header.settings.quietMinutes*60000);if(busy)throw new AppError(409,'会话仍在执行或静默期未结束，本组不迁移','session_busy');
        const latestModels=dispatchAware||plan.header.configuration.account_package_policy?.enabled?new Map((await store.all('SELECT DISTINCT ON(r.session_id) r.session_id,r.model,r.source_channel,r.handoff_context FROM harbor.requests r JOIN harbor.sessions s ON s.id=r.session_id AND s.route_version=r.route_version WHERE r.session_id=ANY($1::text[]) ORDER BY r.session_id,r.query_sequence DESC',moves.map(move=>move.session_id))).map(row=>[row.session_id,row])):new Map();
        for(const session of currentSessions)access.same(scope,session);for(const account of currentAccounts)access.same(scope,account);
        for(const move of moves){const latest=latestModels.get(move.session_id),latestModel=latest?.model,latestProtocol=latest?.source_channel==='responses'||latest?.source_channel==='legacy'&&latest?.handoff_context==='replayable'?'responses':['images/generations','images/edits'].includes(latest?.source_channel)?'images':undefined,source=current.get(move.from);if(plan.header.configuration.account_package_policy?.enabled&&source?.package_selection?.mode!=='legacy'&&!sourceBudget(accountPackageBudget(source,{at:clock(),enabled:true,protocol:latestProtocol,model:latestModel})))changed();const session=byId.get(move.session_id),original=expected.get(move.session_id),target=current.get(move.to),targetClass=target&&classifyExtraPackage(target,{at:clock(),protocol:latestProtocol,model:latestModel,packagePolicy}),extraTarget=dispatchAware&&latestProtocol==='responses'&&summary.dispatchTier==='extra'&&target?.dispatch_planning.mode==='extra'&&target.dispatch_planning.allowed&&target.dispatch_planning.models.includes(latestModel),normalTarget=targetClass&&['package','package_compatibility'].includes(targetClass.mode)&&(!dispatchAware||['package','package_compatibility'].includes(target.dispatch_planning.mode));if(!session||!original||!target||!session.enabled||session.member_id!==original.member_id||session.protocol!=='responses'||session.account_id!==move.from||session.route_version!==move.version||!participatesNightly(session,clock())||!(extraTarget||normalTarget&&(plan.header.configuration.account_package_policy?.enabled&&target.package_selection?.mode!=='legacy'?accountPackageBudget(target,{at:clock(),enabled:true,protocol:latestProtocol,model:latestModel}).eligible:quotaSnapshot(target,clock())?.remaining>0)))changed();}
        await authorize(plan,{lock:true});if(beforeBindings)await beforeBindings();
        for(let start=0;start<moves.length;start+=256){
          ensure();const batch=moves.slice(start,start+256).map(move=>({...move,thread:id(),audit:id(),detail:JSON.stringify({session:move.session_id,from:move.from,to:move.to,reason:'nightly',plan:plan.id,version:move.version+1})})),body=JSON.stringify(batch);
          const updated=await store.run(`UPDATE harbor.sessions s SET account_id=p.to,upstream_thread=p.thread,route_version=s.route_version+1,handoff_requires_replay=CASE WHEN $2 THEN 1 ELSE s.handoff_requires_replay END FROM jsonb_to_recordset($1::jsonb) AS p(session_id text,"from" text,"to" text,version bigint,thread text) WHERE s.id=p.session_id AND s.account_id=p.from AND s.route_version=p.version`,body,!!plan.header.requestBoundary);if(updated.changes!==batch.length)changed();
          await createPostgresAudit(store,{clock}).writeMany(scope,batch.map(event=>({...event,action:'session.route',subject:{kind:'session',id:event.session_id}})),{at:clock()});
        }
        withinWindow(plan);if(dispatchAware){const finalAccounts=await annotateExtraPlanning(store,{accounts:currentAccounts,workloads:scopeFacts,at:clock(),packagePolicy,requestBoundary:!!plan.header.requestBoundary});if(finalAccounts.some(account=>planningDispatchIdentity(account)!==planningDispatchIdentity(old.get(account.id)??{})))changed();}if(currentAccounts.some(account=>plan.header.configuration.account_package_policy?.enabled&&account.package_selection?.mode!=='legacy'?!sourceBudget(accountPackageBudget(account,{at:clock(),enabled:true,protocol:'responses'})):!quotaSnapshot(account,clock())))changed();await checkpoint(plan,entry.ordinal,'applied',null,moves.length);ensure();
      });
      };
      if(!moves.length||!sessionHandoffs)return await execute();
      if(plan.header.requestBoundary&&automatic){const registered=await automatic.start({plan,entry,moves,sessions},execute,{scope:scope??undefined}),done=registered.done.catch(skipped);done.catch(()=>{});return registerOnly?{...registered,done}:await done;}
      alive();const intent=registerRequestHandoffGroup(sessionHandoffs,moves.map(move=>({memberId:expected.get(move.session_id)?.member_id,sessionId:move.session_id,decisionId:plan.id+':'+entry.ordinal+':'+move.session_id})));groups.add(intent);groupScopes.set(intent,scope);
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
    if(!plan)throw new AppError(409,'此计划不能用于夜间执行','nightly_plan_invalid');const scope=await authorize(plan);
    const currentWindow=transferWindow(clock(),await scheduleConfig(scope));
    if(!plan||!currentWindow.due||plan.header.purpose!=='nightly'||plan.target!==currentWindow.key||plan.target!==plan.header.window?.key||!(plan.id===departmentPlanningJobId('routing.nightly',plan.target,scope)||modern(plan)&&!!await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE job_id=$1 AND slot=$2',plan.id,plan.target))||plan.payload.boundary!==(plan.header.window.boundary??plan.header.window.end)||clock()<plan.payload.boundary)throw new AppError(409,'此计划不能用于夜间执行','nightly_plan_invalid');
    const run=await store.transaction(async()=>{
      alive();if(modern(plan))await revisions.ensure(plan,scopeOptions(plan));else await store.run("INSERT INTO harbor.balance_runs(day,started,status,summary,plan_id"+(scope?',department_id':'')+") VALUES($1,$2,'running',$3,$4"+(scope?',$5':'')+") ON CONFLICT("+(scope?'department_id,':'')+"day) DO NOTHING",plan.target,clock(),JSON.stringify({metric:plan.header.metric,window:plan.header.window,plan_id:plan.id}),plan.id,...args(scope));
      const current=await lockedRun(plan);if(current.plan_id!==plan.id)throw new AppError(409,'此日期已有其他计划','nightly_plan_conflict');
      if(current.status==='running'){
        if(current.attempts>=plan.max_attempts){await store.run("UPDATE "+runSource(plan).split(' WHERE ')[0]+" SET status='failed',ended=$2,summary=$3 WHERE "+(modern(plan)||scopeOf(plan)?'plan_id':'day')+"=$1",runKey(plan),clock(),JSON.stringify({plan_id:plan.id,reason:'application_attempts_exhausted'}));current.status='failed';if(modern(plan))await revisions.summary(plan.target,scopeOptions(plan));}
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
      withinWindow(plan);const manual=plan.header.skipReason==='manual_weights_active'||!!await store.get('SELECT group_ordinal FROM '+resultSource(plan)+" AND reason='manual_weights_active' LIMIT 1",runKey(plan));const result={plan_id:plan.id,metric:plan.header.metric,window:plan.header.window,planned:plan.header.counts.moves,skippedItems:plan.header.counts.skipped,...totals,...(manual?{reason:'manual_weights_active',status:'skipped'}:{})};await store.run('UPDATE '+runSource(plan).split(' WHERE ')[0]+" SET status=$4,ended=$2,summary=$3 WHERE "+(modern(plan)||scopeOf(plan)?'plan_id':'day')+'=$1',runKey(plan),clock(),JSON.stringify(result),manual?'interrupted':'completed');if(modern(plan))await revisions.summary(plan.target,scopeOptions(plan));withinWindow(plan);return result;});
    }catch(error){
      if(error?.code!=='nightly_plan_expired'){if(scope&&error?.status===403)await finalizeDisabled(scope);throw error;}
      return store.transaction(async()=>{alive();if(modern(plan)){await lockedRun(plan);await store.run("UPDATE harbor.nightly_plan_applications SET status='interrupted',ended=$1,summary=$2 WHERE plan_id=$3 AND status='running'",clock(),JSON.stringify({plan_id:plan.id,reason:'planning_expired'}),plan.id);const window=transferWindow(clock(),await scheduleConfig(scope));if(window.due&&window.key===plan.target&&(await configuration(scope)).balancing?.nightly)await revisions.request(plan,'planning_expired',scopeOptions(plan));else await revisions.summary(plan.target,scopeOptions(plan));}else await store.run("UPDATE harbor.balance_runs SET status='failed',ended=$1,summary=$2 WHERE day=$3 AND plan_id=$4 AND status='running'",clock(),JSON.stringify({plan_id:plan.id,reason:'planning_expired'}),plan.target,plan.id);alive();return {completed:false};});
    }
  }
  function apply(planId){if(store.context.getStore())return Promise.reject(new AppError(503,'夜间执行不能嵌套在其他事务内','nightly_transaction_invalid'));if(executions.has(planId))return executions.get(planId);const operation=performApply(planId).finally(()=>executions.delete(planId));executions.set(planId,operation);return operation;}
  async function finalizeDisabled(scope){
    if(!scope)return false;const trusted=requireDepartmentScope(scope,{allowSystem:true,roles:['system']});if(trusted.purpose!=='routing.nightly')throw new AppError(403,'夜间收尾身份用途不符','department_forbidden');
    const disabled=await store.transaction(async()=>{
      const row=await store.get('SELECT enabled FROM harbor.departments WHERE id=$1 FOR SHARE',trusted.department_id);if(!row||row.enabled)return false;
      const validJob="j.kind='routing.nightly' AND j.status='completed' AND j.actor_kind='system' AND j.actor_id='system:routing.nightly' AND j.actor_version=0 AND j.scope_kind='department' AND j.scope_identity='department:'||p.department_id",noWaiting="NOT EXISTS(SELECT 1 FROM harbor.automatic_handoff_groups g WHERE g.plan_id=p.id AND g.status='waiting')";
      await store.run("UPDATE harbor.nightly_plan_applications a SET status='interrupted',ended=$1,summary=jsonb_build_object('reason','department_disabled','plan_id',a.plan_id)::text FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id AND j.department_id=p.department_id WHERE a.plan_id=p.id AND a.department_id=p.department_id AND a.department_id=$2 AND a.status='running' AND COALESCE((p.header->>'requestBoundary')::boolean,false) AND "+noWaiting+" AND "+validJob,clock(),trusted.department_id);
      await store.run("UPDATE harbor.balance_runs b SET status=CASE WHEN COALESCE((p.header->>'requestBoundary')::boolean,false) THEN a.status ELSE 'interrupted' END,ended=COALESCE(a.ended,$1),summary=COALESCE(a.summary,jsonb_build_object('reason','department_disabled','plan_id',b.plan_id)::text) FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id AND j.department_id=p.department_id LEFT JOIN harbor.nightly_plan_applications a ON a.plan_id=p.id AND a.department_id=p.department_id WHERE b.plan_id=p.id AND b.department_id=p.department_id AND b.department_id=$2 AND b.status='running' AND (NOT COALESCE((p.header->>'requestBoundary')::boolean,false) OR a.status IN ('completed','failed','interrupted') AND "+noWaiting+") AND "+validJob,clock(),trusted.department_id);return true;
    });
    if(disabled)for(const intent of groups)if(groupScopes.get(intent)?.department_id===trusted.department_id)intent.cancel();return disabled;
  }
  async function runDepartment(scope){
    if(automatic)await automatic.reconcile({scope});
    try{scope=await access.resolve(scope);}catch(error){if(error.code==='department_forbidden'&&await finalizeDisabled(scope))return;throw error;}
    alive();const window=transferWindow(clock(),await scheduleConfig(scope)),day=window.key;if(automatic)await revisions.reconcile(day,{scope:scope??undefined});if(!window.due)return;
    await store.transaction(async()=>{await access.resolve(scope,{write:true});alive();await store.run("UPDATE harbor.balance_runs SET status='interrupted',ended=$1 WHERE status='running' AND plan_id IS NOT NULL AND started<$2"+own(scope,3),clock(),window.end,...args(scope));alive();});
    const baseJob=scope?"j.id='routing.nightly:'||j.department_id||':'||j.target":"j.id='routing.nightly:'||j.target",sameRun=scope?' AND r.department_id=j.department_id':'';
    await store.transaction(async()=>{await access.resolve(scope,{write:true});alive();await store.run("INSERT INTO harbor.balance_runs(day,started,ended,status,summary,plan_id"+(scope?',department_id':'')+") SELECT j.target,j.created,$1,'interrupted',jsonb_build_object('reason','application_not_started','plan_id',p.id,'planned_at',p.created)::text,p.id"+(scope?',j.department_id':'')+" FROM harbor.background_jobs j JOIN harbor.routing_plans p ON p.id=j.id WHERE j.kind='routing.nightly' AND j.status='completed' AND (j.payload->>'boundary')::bigint<$2 AND j.created>=$3 AND "+baseJob+" AND p.header->>'purpose'='nightly' AND NOT EXISTS(SELECT 1 FROM harbor.balance_runs r WHERE r.day=j.target"+sameRun+")"+own(scope,4,'j')+" ORDER BY j.created,j.id LIMIT 1 ON CONFLICT("+(scope?'department_id,':'')+"day) DO NOTHING",clock(),window.end,usageWindow(clock()).start,...args(scope));alive();});
    const failed=await store.get("SELECT j.id,j.target,j.created,j.updated,j.status FROM harbor.background_jobs j WHERE j.kind='routing.nightly' AND (j.payload->>'boundary')::bigint<=$1 AND j.created>=$2 AND j.status IN ('failed','cancelled') AND "+baseJob+" AND NOT EXISTS(SELECT 1 FROM harbor.balance_runs r WHERE r.day=j.target"+sameRun+")"+own(scope,3,'j')+" ORDER BY j.created,j.id LIMIT 1",clock(),usageWindow(clock()).start,...args(scope));
    if(failed)await store.transaction(async()=>{await access.resolve(scope,{write:true});alive();await store.run("INSERT INTO harbor.balance_runs(day,started,ended,status,summary"+(scope?',department_id':'')+") VALUES($1,$2,$3,$4,$5"+(scope?',$6':'')+") ON CONFLICT("+(scope?'department_id,':'')+"day) DO NOTHING",failed.target,failed.created,failed.updated,failed.status==='cancelled'?'cancelled':'failed',JSON.stringify({reason:failed.status==='cancelled'?'planning_cancelled':'planning_failed',job_id:failed.id}),...args(scope));alive();});
    const waiting=await store.get("SELECT id FROM harbor.background_jobs WHERE kind='routing.nightly' AND target=$1 AND status IN ('pending','running')"+own(scope,2)+" LIMIT 1",day,...args(scope));
    if(waiting&&!await store.get('SELECT day FROM harbor.nightly_cycle_ready WHERE day=$1 AND at>=$2'+own(scope,3),day,clock()-quotaFreshnessMs,...args(scope))){
      await refreshQuotas({force:true,...(scope?{scope}:{})});alive();await access.resolve(scope);if(transferWindow(clock(),await scheduleConfig(scope)).key!==day)changed();
      await store.transaction(async()=>{await access.resolve(scope,{write:true});await lockPostgresRouting(store);alive();await router.syncWeights('nightly',clock(),{scope:scope??undefined});await store.run('INSERT INTO harbor.nightly_cycle_ready(day,at'+(scope?',department_id':'')+') VALUES($1,$2'+(scope?',$3':'')+') ON CONFLICT('+(scope?'department_id,':'')+'day) DO UPDATE SET at=EXCLUDED.at',day,clock(),...args(scope));alive();});
    }
    const mode=(await configuration(scope)).request_handoff_policy?.mode==='request-boundary'&&!!automatic;
    const plan=await store.get(mode?"SELECT p.*,j.target FROM harbor.background_jobs j JOIN harbor.routing_plans p ON p.id=j.id WHERE j.kind='routing.nightly' AND j.status='completed' AND j.target=$1"+own(scope,2,'j')+" AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_applications a WHERE a.plan_id=p.id AND a.status<>'running') AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_revisions r WHERE r.replaces_plan_id=p.id) ORDER BY j.created DESC,j.id DESC LIMIT 1":"SELECT j.id FROM harbor.background_jobs j JOIN harbor.routing_plans p ON p.id=j.id WHERE j.kind='routing.nightly' AND j.status='completed' AND j.target=$1"+own(scope,2,'j')+" AND NOT EXISTS(SELECT 1 FROM harbor.balance_runs r WHERE r.plan_id=p.id AND r.status IN ('completed','failed','interrupted')) ORDER BY j.created,j.id LIMIT 1",day,...args(scope));
    if(plan){if(mode&&plan.expires<=clock()){await authorize(plan);await store.transaction(async()=>{await revisions.ensure(plan,scopeOptions(plan));await revisions.request(plan,'planning_expired',scopeOptions(plan));});}else if(mode||scope)void apply(plan.id).catch(()=>{lastError='nightly_apply_failed';});else await apply(plan.id);}
  }
  function runOnce(){
    if(stopping||isStopping())return Promise.resolve();if(pumping)return pumping;
    pumping=(async()=>{
      if(automatic?.takeRecovery())await recover({restart:false});
      let failed=false;for(const scope of await scopes()){try{await runDepartment(scope);}catch{failed=true;}}
      lastError=failed?'nightly_apply_failed':null;
    })().catch(()=>{lastError='nightly_apply_failed';}).finally(()=>{pumping=null;});return pumping;
  }
  async function recoverDepartment(scope,{restart}){
    if(!automatic)return;
    let disabled=false;try{scope=await access.resolve(scope);}catch(error){if(error.code!=='department_forbidden')throw error;disabled=true;}
    if(!disabled&&(await configuration(scope)).request_handoff_policy?.mode!=='request-boundary'){await automatic.reconcile({scope:scope??undefined});return;}
    const rows=await store.all("SELECT g.*,p.created planned_at,p.header,p.expires plan_expires,j.target,j.payload,j.max_attempts FROM harbor.automatic_handoff_groups g JOIN harbor.routing_plans p ON p.id=g.plan_id JOIN harbor.background_jobs j ON j.id=p.id WHERE g.status='waiting'"+own(scope,1,'g')+" ORDER BY g.requested,g.id LIMIT 20001",...args(scope));if(rows.length>20000)throw new AppError(503,'自动交接恢复超过容量','session_handoff_busy');
    const plans=new Set();for(const row of rows){alive();if(automatic.has(row.id))continue;
      if(disabled||restart){await automatic.discard(row,{scope:scope??undefined});continue;}
      const window=transferWindow(clock(),await scheduleConfig(scope)),settings=await access.setting(scope,'balancing')??{};
      if(row.expires<=clock()||row.plan_expires<=clock()||row.slot!==window.key||!window.due||!settings.nightly){await automatic.discard(row,{scope:scope??undefined});continue;}
      if(await store.get("SELECT id FROM harbor.session_handoffs WHERE origin='manual' AND status='waiting' AND replaces_handoff_id IN (SELECT id FROM harbor.session_handoffs WHERE automatic_group_id=$1) LIMIT 1",row.id))continue;
      if(await store.get("SELECT id FROM harbor.session_handoffs WHERE automatic_group_id=$1 AND status<>'waiting' LIMIT 1",row.id)){await automatic.discard(row,{scope:scope??undefined});continue;}
      const plan={id:row.plan_id,department_id:row.department_id,created:row.planned_at,expires:row.plan_expires,header:row.header,target:row.target,payload:row.payload,max_attempts:row.max_attempts},entry=await store.get("SELECT ordinal,payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section='groups' AND ordinal=$2",row.plan_id,row.group_ordinal);
      if(!entry){await automatic.discard(row,{scope:scope??undefined});continue;}await group(plan,entry,true);plans.add(plan.id);
    }
    if(disabled)return;
    if(restart){const window=transferWindow(clock(),await scheduleConfig(scope)),config=await configuration(scope);if(window.due&&config.balancing?.nightly&&config.request_handoff_policy?.mode==='request-boundary'){const candidates=await store.all("SELECT p.*,j.target FROM harbor.routing_plans p JOIN harbor.background_jobs j ON j.id=p.id LEFT JOIN harbor.nightly_plan_applications a ON a.plan_id=p.id WHERE j.kind='routing.nightly' AND j.status='completed' AND j.target=$1"+own(scope,2,'j')+" AND COALESCE(a.status,'running')='running' AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_revisions r WHERE r.replaces_plan_id=p.id) ORDER BY p.created,p.id",window.key,...args(scope));for(const candidate of candidates){await authorize(candidate);await store.transaction(async()=>{await revisions.ensure(candidate,scopeOptions(candidate));await revisions.request(candidate,'gateway_restart',scopeOptions(candidate));});}}}
    for(const planId of plans)void apply(planId).catch(()=>{lastError='nightly_apply_failed';});
  }
  async function recover({restart=true}={}){for(const scope of await scopes()){if(scope)await finalizeDisabled(scope);await recoverDepartment(scope,{restart});}}
  return {apply,runOnce,recover,close:async()=>{stopping=true;await automatic?.close();for(const intent of groups)intent.cancel();await pumping;await Promise.allSettled([...executions.values()]);},status:()=>({stopping,running:!!pumping||executions.size>0,waitingGroups:groups.size+(automatic?.status().pendingGroups||0),lastError,automatic:automatic?.status()??null})};
}
