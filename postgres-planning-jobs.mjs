import {scheduledRecoveryId,originalScheduledId,sameScheduledIdentity,priorDailyExpiryBoundary} from './scheduled-task-identity.mjs';
import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {setTimeout as delay} from 'node:timers/promises';
import {AppError,usageWindow} from './store.mjs';
import {balanceSettings,planBalanceWorkloads,nightlyWindow,quotaFreshnessMs} from './balancing.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';
import {transferSchedule,transferWindow} from './transfer-schedule.mjs';
import {createPostgresPlanningInput} from './postgres-planning-input.mjs';

export const departmentPlanningJobId=(kind,target,scope)=>kind+':'+(scope?scope.department_id+':':'')+target;
const configurationNames=['balancing','routing_strategy','routing_weights','transfer_schedule','request_handoff_policy','account_package_policy'];
const publicSections=new Set(['moves','skipped','groups','fixed']);
const stopped=signal=>{if(signal?.aborted)throw new AppError(503,'规划任务已停止','worker_stopping');};
const capacity=()=>{throw new AppError(503,'完整规划超出存储容量，未保存部分计划','planning_capacity');};

// Plans are immutable, separate from the small lease/result record. This job
// computes immutable plans; applying bindings belongs to the active gateway.
export function createPostgresPlanningJobs(store,{jobs,clock=Date.now,input={},ttlMs=3600000,maxPlanBytes=16777216,maxItemBytes=262144,pageBytes=131072}={}){
  if(!jobs||typeof clock!=='function'||!Number.isInteger(ttlMs)||ttlMs<60000||ttlMs>86400000||!Number.isInteger(maxPlanBytes)||maxPlanBytes<1024||maxPlanBytes>67108864||!Number.isInteger(maxItemBytes)||maxItemBytes<256||maxItemBytes>2097152||maxItemBytes>maxPlanBytes||!Number.isInteger(pageBytes)||pageBytes<256||pageBytes>2097152)throw new AppError(503,'规划存储配置无效','planning_configuration');
  const departments=createRoutingDepartments(store,{clock});
  const readConfiguration=async scope=>Object.fromEntries(await departments.settings(scope,configurationNames));
  const kind='routing.preview',reader=createPostgresPlanningInput(store,{...input,clock});
  const enqueue=async({scope}={})=>{scope=await departments.resolve(scope,{management:true});return jobs.enqueue(kind,scope?{scope}:{});};
  const scheduleConfig=async scope=>transferSchedule(await departments.setting(scope,'transfer_schedule')??{});
  async function schedule({scope,expiryScope}={}){
    scope=await departments.resolve(scope);if(scope&&(scope.role!=='system'||scope.purpose!=='routing.nightly'))throw new AppError(403,'夜间计划需专用系统身份','department_forbidden');
    const at=await jobs.now(),window=transferWindow(at,await scheduleConfig(scope));
    const enabled=balanceSettings(await departments.setting(scope,'balancing')??{}).nightly;
    const obsolete=await store.all("SELECT id FROM harbor.background_jobs WHERE kind='routing.nightly' AND status IN ('pending','running') AND result IS NULL AND ($1 OR target<>$2)"+(scope?' AND department_id=$3':'')+" ORDER BY created,id LIMIT 256",!enabled||!window.due,window.key,...(scope?[scope.department_id]:[]));
    for(const row of obsolete)await jobs.cancel(row.id);
    if(!enabled||!window.due)return null;
    if(scope){expiryScope=await departments.resolve(expiryScope);if(expiryScope.department_id!==scope.department_id||expiryScope.role!=='system'||expiryScope.purpose!=='routing.expiry')throw new AppError(403,'周期恢复身份不匹配','department_forbidden');}
    const expiry=window.end,dependencyId=departmentPlanningJobId('routing.expiry',expiry,scope);await jobs.enqueue('routing.expiry',{id:dependencyId,payload:{boundary:expiry},...(scope?{scope:expiryScope}:{})});
    const policy=await departments.setting(scope,'request_handoff_policy')??{};
    const revision=policy.mode==='request-boundary'?await store.get('SELECT revision,job_id FROM harbor.nightly_plan_revisions WHERE slot=$1'+(scope?' AND department_id=$2':'')+' ORDER BY revision DESC LIMIT 1',window.key,...(scope?[scope.department_id]:[])):null;
    const id=revision?.job_id??departmentPlanningJobId('routing.nightly',window.key,scope),payload={boundary:window.end,...(revision?{revision:revision.revision}:{})};
    return store.transaction(async()=>{
      const existing=await store.get('SELECT * FROM harbor.background_jobs WHERE id=$1'+(scope?' AND department_id=$2':'')+' FOR UPDATE',id,...(scope?[scope.department_id]:[]));
      const oldDependency=departmentPlanningJobId('routing.expiry',priorDailyExpiryBoundary(window.end),scope);
      if(existing&&existing.dependency_id!==dependencyId&&existing.dependency_id===oldDependency&&existing.kind==='routing.nightly'&&existing.target===window.key&&existing.payload.boundary===payload.boundary&&existing.payload.revision===payload.revision&&Object.keys(existing.payload).length===Object.keys(payload).length){
        // Scheduling inspects a locked pending/cancelled predecessor; only execution owns a lease.
        const actor=await jobs.authorizePersistedActor(existing,{requireLease:false});
        if(scope&&(!actor||actor.department_id!==scope.department_id||actor.role!=='system'||actor.purpose!=='routing.nightly'))throw new AppError(403,'原定时任务身份不匹配','department_forbidden');
        // Never replace a job that already committed a plan or another result.
        if(existing.result!==null||await store.get('SELECT id FROM harbor.routing_plans WHERE id=$1',id))return {id,created:false};
        await jobs.cancel(id);
        return jobs.enqueue('routing.nightly',{id:scheduledRecoveryId(id),target:window.key,payload,dependencyId,...(scope?{scope}:{})});
      }
      return jobs.enqueue('routing.nightly',{id,target:window.key,payload,dependencyId,...(scope?{scope}:{})});
    });
  }
  async function execute(job,{signal}={}){
    const scope=await jobs.authorizePersistedActor(job);
    const nightly=job.kind==='routing.nightly',boundary=job.payload?.boundary,window=Number.isSafeInteger(boundary)?transferWindow(boundary,await scheduleConfig(scope)):null,expiry=Number.isSafeInteger(boundary)?boundary:null;
    const revision=nightly&&job.payload?.revision!==undefined?await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE slot=$1 AND revision=$2'+(scope?' AND department_id=$3':''),job.target,Number.isSafeInteger(job.payload.revision)?job.payload.revision:-1,...(scope?[scope.department_id]:[])):null;
    if(nightly?(!window||!window.due||window.end!==boundary||boundary<0||(revision?!sameScheduledIdentity(revision.job_id,job.id):!sameScheduledIdentity(departmentPlanningJobId('routing.nightly',window.key,scope),job.id))||job.target!==window.key||job.dependency_id!==departmentPlanningJobId('routing.expiry',expiry,scope)||Object.keys(job.payload).length!==(revision?2:1)):job.kind!==kind||Object.keys(job.payload||{}).length)throw new AppError(400,'规划任务参数无效','job_payload_invalid');
    if(nightly&&originalScheduledId(job.id)!==job.id){const predecessor=await store.get('SELECT dependency_id,result FROM harbor.background_jobs WHERE id=$1'+(scope?' AND department_id=$2':''),originalScheduledId(job.id),...(scope?[scope.department_id]:[]));if(!predecessor||predecessor.result!==null||predecessor.dependency_id!==departmentPlanningJobId('routing.expiry',priorDailyExpiryBoundary(boundary),scope)||predecessor.dependency_id===job.dependency_id||await store.get('SELECT id FROM harbor.routing_plans WHERE id=$1',originalScheduledId(job.id)))throw new AppError(409,'原定时任务不能替代','job_identity_conflict');}
    stopped(signal);
    const saved=await jobs.owned(job,()=>store.get('SELECT result FROM harbor.background_jobs WHERE id=$1',job.id));
    if(saved.result!==null)return saved.result;
    if(nightly)for(;;){
      stopped(signal);const now=await jobs.now();if(now<boundary||now>=window.until||transferWindow(now,await scheduleConfig(scope)).key!==job.target)throw new AppError(409,'夜间规划窗口已失效','nightly_plan_expired');
      const dependency=await store.get('SELECT status,result FROM harbor.background_jobs WHERE id=$1',departmentPlanningJobId('routing.expiry',expiry,scope));
      if(dependency?.status==='completed'&&dependency.result!==null&&await store.get('SELECT day FROM harbor.nightly_cycle_ready WHERE day=$1 AND at>=$2'+(scope?' AND department_id=$3':''),job.target,now-quotaFreshnessMs,...(scope?[scope.department_id]:[])))break;
      if(dependency&&['failed','cancelled'].includes(dependency.status))throw new AppError(409,'周期恢复未完成，未生成夜间计划','nightly_dependency_failed');
      await delay(1000,undefined,{signal});
    }
    const at=await jobs.now();
    const captured=await store.transaction(async()=>{
      await jobs.authorizePersistedActor(job);
      const configuration=await readConfiguration(scope);
      const legacy=configuration.balancing||{},settings=balanceSettings({...legacy,weighted:configuration.routing_strategy?.weighted??legacy.weighted??false});
      const requestBoundary=configuration.request_handoff_policy?.mode==='request-boundary',currentWindow=nightly||requestBoundary?transferWindow(at,configuration.transfer_schedule):nightlyWindow(at);if(nightly&&(!settings.nightly||!currentWindow.due||currentWindow.key!==job.target||currentWindow.end!==boundary))throw new AppError(409,'定时转移配置已变化','nightly_plan_expired');
      const workloadWindow=nightly||requestBoundary?{...currentWindow,boundary:currentWindow.end,start:at-16*3600000,end:at}:currentWindow;
      const captured=await reader.read({scope,at,window:workloadWindow,requestBoundary,...(nightly?{quotaBoundary:boundary}:{})});
      return {configuration,settings,requestBoundary,...captured};
    },{level:'REPEATABLE READ',readOnly:true});
    stopped(signal);
    const plan=planBalanceWorkloads({...captured,quietMinutes:captured.settings.quietMinutes,weighted:captured.settings.weighted,configuredWeights:captured.configuration.routing_weights});
    const identities=new Map(captured.workloads.map(row=>[row.id,{client_session_id:row.client_session_id??null,protocol:row.protocol}])),identify=row=>({...row,...(identities.get(row.session_id)||{})});
    const sections={moves:plan.moves.map(identify),skipped:plan.skipped.map(identify),groups:[],fixed:[],accounts:captured.accounts,sessions:captured.workloads.map(({measuredAtoms,estimatedAtoms,...row})=>({...row,measured_atoms:String(measuredAtoms),estimated_atoms:estimatedAtoms}))};
    for(const [group,summary] of plan.summaries.entries()){
      const {fixed,...value}=summary;sections.groups.push({...value,group});for(const session_id of fixed)sections.fixed.push(identify({group,session_id}));
    }
    const counts=Object.fromEntries(Object.entries(sections).map(([section,rows])=>[section,rows.length]));
    const header={skipReason:plan.skipped.some(item=>item.reason==='manual_weights_active')?'manual_weights_active':null,purpose:nightly?'nightly':'preview',metric:plan.metric,window:plan.window,priceRevisions:plan.priceRevisions,configuration:captured.configuration,settings:captured.settings,requestBoundary:captured.requestBoundary,counts,preparationChecked:false};
    const encodedHeader=JSON.stringify(header);let bytes=Buffer.byteLength(encodedHeader);if(bytes>maxItemBytes)capacity();
    const items=[];
    for(const [section,rows] of Object.entries(sections))for(const [index,row] of rows.entries()){
      const payload=JSON.stringify(row),size=Buffer.byteLength(payload);bytes+=size;
      if(size>maxItemBytes||bytes>maxPlanBytes)capacity();items.push({section,ordinal:index+1,payload,bytes:size});
    }
    stopped(signal);
    return jobs.commitEffect(job,async()=>{
      stopped(signal);await jobs.authorizePersistedActor(job);await departments.resolve(scope,{write:true});
      if(nightly){
        await lockPostgresRouting(store);stopped(signal);
        const current=await readConfiguration(scope);
        const replacement=captured.requestBoundary?await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE slot=$1'+(scope?' AND department_id=$2':'')+' ORDER BY revision DESC LIMIT 1',job.target,...(scope?[scope.department_id]:[])):null;
        if(replacement&&!sameScheduledIdentity(replacement.job_id,job.id))throw new AppError(409,'计划已被恢复修订替代','nightly_plan_expired');
        if(!(await store.get('SELECT $1::jsonb=$2::jsonb equal',JSON.stringify(current),JSON.stringify(captured.configuration))).equal||!transferWindow(await jobs.now(),current.transfer_schedule).due||transferWindow(await jobs.now(),current.transfer_schedule).key!==job.target)throw new AppError(409,'定时转移配置已变化，未保存旧计划','nightly_plan_expired');
      }
      await store.run('INSERT INTO harbor.routing_plans(id,created,expires,header,bytes'+(scope?',department_id':'')+') VALUES($1,$2,$3,$4::jsonb,$5'+(scope?',$6':'')+')',job.id,at,nightly?Math.min(at+ttlMs,window.until):at+ttlMs,encodedHeader,bytes,...(scope?[scope.department_id]:[]));
      // Bound each statement as well as total plan size; no giant JSON array is
      // passed as a single SQL parameter and no partial plan commits.
      for(let start=0;start<items.length;){
        stopped(signal);const batch=[];let batchBytes=0;
        while(start<items.length&&batch.length<128){const item=items[start];if(batch.length&&batchBytes+item.bytes>524288)break;batch.push(item);batchBytes+=item.bytes;start++;}
        await store.run(`INSERT INTO harbor.routing_plan_items(plan_id,section,ordinal,payload,bytes)
          SELECT $1,p.section,p.ordinal,p.payload::jsonb,p.bytes FROM jsonb_to_recordset($2::jsonb) AS p(section text,ordinal integer,payload text,bytes integer)`,job.id,JSON.stringify(batch));
      }
      stopped(signal);return {plan_id:job.id};
    });
  }
  async function status(id,{scope}={}){
    scope=await departments.resolve(scope,{management:true});
    const at=clock(),row=await store.get("SELECT j.id,j.status,j.error_code,j.result,j.created,j.updated,p.created planned_at,p.expires,p.header FROM harbor.background_jobs j LEFT JOIN harbor.routing_plans p ON p.id=j.id WHERE j.id=$1 AND j.kind=$2 AND j.created>=$3 AND j.created<$4"+(scope?" AND j.department_id=$5":""),id,kind,usageWindow(at).start,usageWindow(at).end,...(scope?[scope.department_id]:[]));
    if(!row)throw new AppError(404,'规划任务不存在或超出查询范围','planning_not_found');
    const {result,header,...view}=row;
    if(header&&row.expires<=at)throw new AppError(410,'规划已过期，请重新生成','planning_expired');
    return {...view,...(header?{plan:header}:{}),ready:!!header};
  }
  async function page(id,{section='moves',after=0,limit=20,scope}={}){
    if(!publicSections.has(section)||!Number.isSafeInteger(after)||after<0||!Number.isInteger(limit)||limit<1||limit>100)throw new AppError(400,'规划分页参数无效','planning_query_invalid');
    return store.transaction(async()=>{
      const view=await status(id,{scope});if(!view.ready)throw new AppError(409,'规划尚未生成','planning_pending');
      const rows=await store.all('SELECT ordinal,bytes FROM harbor.routing_plan_items WHERE plan_id=$1 AND section=$2 AND ordinal>$3 ORDER BY ordinal LIMIT $4',id,section,after,limit+1);
      let size=0;const selected=[];
      for(const row of rows.slice(0,limit)){if(selected.length&&size+row.bytes>pageBytes)break;selected.push(row.ordinal);size+=row.bytes;}
      const items=selected.length?await store.all('SELECT ordinal,payload FROM harbor.routing_plan_items WHERE plan_id=$1 AND section=$2 AND ordinal=ANY($3::integer[]) ORDER BY ordinal',id,section,selected):[];
      return {section,items:items.map(row=>({position:row.ordinal,...row.payload})),after:selected.at(-1)??after,has_more:rows.length>selected.length};
    },{level:'REPEATABLE READ',readOnly:true});
  }
  return {enqueue,schedule,status,page,handlers:{[kind]:execute,'routing.nightly':execute}};
}
