// Frozen upgrade producer from 5bbcbdce18e94dfee5a6ea37e90e1b5c769a05a5 postgres-nightly-revisions.mjs.
// Only import paths and the removed legacy time helper are adapted; do not modernize this fixture.
import {AppError} from '../../store.mjs';
import {createRoutingDepartments} from '../../postgres-routing-departments.mjs';
import {createPostgresJobs} from '../../postgres-jobs.mjs';
import {departmentPlanningJobId} from '../../postgres-planning-jobs.mjs';
import {requireDepartmentScope} from '../../department-access.mjs';

// Only the active gateway requests a revision. Workers keep their existing
// immutable job/result ownership; retries never replace a committed plan.
export function createPostgresNightlyRevisions(store,{clock=Date.now,departmentActors}={}){
  const departments=createRoutingDepartments(store,{clock}),jobs=createPostgresJobs(store,{clock,departmentActors,scopedKinds:['routing.nightly']});
  const args=scope=>scope?[scope.department_id]:[];
  const own=(scope,alias,index)=>scope?' AND '+(alias?alias+'.':'')+'department_id=$'+index:'';
  async function authorize(scope){const trusted=await departments.resolve(scope,{write:true,lock:true});if(trusted&&(trusted.role!=='system'||trusted.purpose!=='routing.nightly'))throw new AppError(403,'夜间修订缺少原计划系统身份','department_forbidden');return trusted;}
  async function summary(slot,{scope,finalize=false,groupId}={}){return store.transaction(async()=>{
    if(typeof finalize!=='boolean')throw new AppError(400,'夜间汇总收尾参数无效','nightly_plan_invalid');
    if(finalize&&scope!==undefined){
      scope=requireDepartmentScope(scope,{allowSystem:true,roles:['system']});
      if(scope.purpose!=='routing.nightly'||typeof groupId!=='string')throw new AppError(403,'夜间收尾缺少真实终态分组','department_forbidden');
      const terminal=await store.get("SELECT g.plan_id,d.enabled FROM harbor.automatic_handoff_groups g JOIN harbor.departments d ON d.id=g.department_id WHERE g.id=$1 AND g.department_id=$2 AND g.slot=$3 AND g.status<>'waiting' AND g.finished IS NOT NULL FOR SHARE OF d,g",groupId,scope.department_id,slot);
      if(!terminal)throw new AppError(403,'夜间收尾缺少真实终态分组','department_forbidden');
      if(!terminal.enabled)await store.run("UPDATE harbor.nightly_plan_applications a SET status='interrupted',ended=$1,summary=$2 WHERE a.plan_id=$3 AND a.department_id=$4 AND a.status='running' AND NOT EXISTS(SELECT 1 FROM harbor.automatic_handoff_groups g WHERE g.plan_id=a.plan_id AND g.status='waiting')",clock(),JSON.stringify({reason:'department_disabled',plan_id:terminal.plan_id}),terminal.plan_id,scope.department_id);
    }else scope=await authorize(scope);
    const revisions=await store.all(`SELECT a.plan_id,a.status,a.started,a.ended,COALESCE(r.revision,0)::int revision,
      (p.header->'counts'->>'moves')::int planned,p.header->'metric' metric,p.header->'window' plan_window,COALESCE(p.header->>'skipReason',CASE WHEN a.summary IS NOT NULL THEN a.summary::jsonb->>'reason' END) skip_reason,
      (SELECT COUNT(*)::int FROM harbor.session_handoffs h JOIN harbor.automatic_handoff_groups g ON g.id=h.automatic_group_id WHERE g.plan_id=a.plan_id AND h.status='waiting') waiting,
      (SELECT COUNT(*)::int FROM harbor.automatic_handoff_groups g WHERE g.plan_id=a.plan_id AND g.status IN ('cancelled','failed','superseded')) cancelled_groups,
      (SELECT COUNT(*)::int FROM harbor.automatic_handoff_groups g WHERE g.plan_id=a.plan_id AND g.status='waiting') waiting_groups,
      COALESCE(t.applied,0)::int applied,COALESCE(t.skipped,0)::int skipped
      FROM harbor.nightly_plan_applications a JOIN harbor.routing_plans p ON p.id=a.plan_id
      LEFT JOIN harbor.nightly_plan_revisions r ON r.job_id=a.plan_id
      LEFT JOIN LATERAL(SELECT SUM(moved) applied,COUNT(*) FILTER(WHERE status='skipped') skipped FROM harbor.nightly_plan_group_results WHERE plan_id=a.plan_id) t ON true
      WHERE a.slot=$1${own(scope,'a',2)} ORDER BY COALESCE(r.revision,0),a.plan_id`,slot,...args(scope));
    if(!revisions.length)return;
    const latest=revisions.at(-1),pending=await store.get("SELECT r.job_id,COALESCE(j.status,'pending') status,j.updated FROM harbor.nightly_plan_revisions r LEFT JOIN harbor.background_jobs j ON j.id=r.job_id WHERE r.slot=$1 AND r.revision>$2"+own(scope,'r',3)+" ORDER BY r.revision DESC LIMIT 1",slot,latest.revision,...args(scope)),terminal=pending&&['failed','cancelled'].includes(pending.status);
    const value={plan_id:latest.plan_id,revision_count:revisions.length,planned:revisions.reduce((n,row)=>n+row.planned,0),applied:revisions.reduce((n,row)=>n+row.applied,0),skipped:revisions.reduce((n,row)=>n+row.skipped,0),waiting:revisions.reduce((n,row)=>n+row.waiting,0),waitingGroups:revisions.reduce((n,row)=>n+row.waiting_groups,0),metric:latest.metric,window:latest.plan_window,revisions,...(latest.skip_reason?{reason:latest.skip_reason}:{}),...(pending?{pending_revision:pending.job_id,pending_revision_status:pending.status}:{}),...(terminal?{reason:pending.status==='failed'?'planning_failed':'planning_cancelled'}:{})};
    await store.run('UPDATE harbor.balance_runs SET plan_id=$1,status=$2,ended=$3,summary=$4 WHERE day=$5'+own(scope,'',6),latest.plan_id,pending?(terminal?pending.status:'running'):latest.status,pending?(terminal?pending.updated:null):latest.ended,JSON.stringify(value),slot,...args(scope));
    return value;
  });}
  async function request(plan,reason,{scope}={}){
    return store.transaction(async()=>{
      scope=await authorize(scope);
      if(scope&&!await store.get('SELECT id FROM harbor.routing_plans WHERE id=$1 AND department_id=$2',plan.id,scope.department_id))throw new AppError(404,'夜间计划不存在或无权访问','department_object_not_found');
      // The same slot row serializes apply finalization and revision requests.
      await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1'+own(scope,'',2)+' FOR UPDATE',plan.target,...args(scope));
      const existing=await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE replaces_plan_id=$1'+own(scope,'',2),plan.id,...args(scope));if(existing)return existing.job_id;
      const latest=await store.get('SELECT revision,job_id FROM harbor.nightly_plan_revisions WHERE slot=$1'+own(scope,'',2)+' ORDER BY revision DESC LIMIT 1',plan.target,...args(scope));
      if(latest&&latest.job_id!==plan.id)return latest.job_id;
      const revision=(latest?.revision??0)+1,jobId=departmentPlanningJobId('routing.nightly',plan.target,scope)+':r'+revision;
      if(scope){
        const original=await store.get("SELECT dependency_id,payload FROM harbor.background_jobs WHERE id=$1 AND department_id=$2 AND kind='routing.nightly' AND status='completed'",plan.id,scope.department_id);
        if(!original)throw new AppError(409,'原夜间任务未完成','nightly_plan_invalid');
        await jobs.enqueue('routing.nightly',{id:jobId,target:plan.target,payload:{boundary:original.payload.boundary,revision},dependencyId:original.dependency_id,scope});
      }
      await store.run('INSERT INTO harbor.nightly_plan_revisions(slot,revision,job_id,replaces_plan_id,requested,reason'+(scope?',department_id':'')+') VALUES($1,$2,$3,$4,$5,$6'+(scope?',$7':'')+')',plan.target,revision,jobId,plan.id,clock(),reason,...args(scope));
      await store.run("UPDATE harbor.nightly_plan_applications SET status='interrupted',ended=$1 WHERE plan_id=$2 AND status='running'",clock(),plan.id);
      await summary(plan.target,{scope:scope??undefined});return jobId;
    });
  }
  async function ensure(plan,{scope}={}){return store.transaction(async()=>{
    scope=await authorize(scope);
    if(scope&&!await store.get('SELECT id FROM harbor.routing_plans WHERE id=$1 AND department_id=$2',plan.id,scope.department_id))throw new AppError(404,'夜间计划不存在或无权访问','department_object_not_found');
    await store.run("INSERT INTO harbor.balance_runs(day,started,status,summary,plan_id"+(scope?',department_id':'')+") VALUES($1,$2,'running',$3,$4"+(scope?',$5':'')+") ON CONFLICT("+(scope?'department_id,day':'day')+") DO NOTHING",plan.target,clock(),JSON.stringify({plan_id:plan.id}),plan.id,...args(scope));
    await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1'+own(scope,'',2)+' FOR UPDATE',plan.target,...args(scope));
    const revision=await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE slot=$1'+own(scope,'',2)+' ORDER BY revision DESC LIMIT 1',plan.target,...args(scope));
    if(revision&&revision.job_id!==plan.id)throw new AppError(409,'此计划已被当前时段的新修订替代','nightly_plan_changed');
    await store.run("INSERT INTO harbor.nightly_plan_applications(plan_id,slot,started,status"+(scope?',department_id':'')+") VALUES($1,$2,$3,'running'"+(scope?',$4':'')+") ON CONFLICT(plan_id) DO NOTHING",plan.id,plan.target,clock(),...args(scope));
  });}
  async function reconcile(slot,{scope}={}){return store.transaction(async()=>{
    scope=await authorize(scope);
    // Serialize with applications before projecting a later planner terminal.
    // A completed plan without an application is still waiting for the gateway.
    const row=await store.get(`SELECT b.day FROM harbor.balance_runs b JOIN harbor.nightly_plan_revisions r ON r.slot=b.day${scope?' AND r.department_id=b.department_id':''} JOIN harbor.background_jobs j ON j.id=r.job_id
      WHERE b.day=$1${own(scope,'b',2)} AND j.status IN ('failed','cancelled') AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_revisions newer WHERE newer.slot=r.slot${scope?' AND newer.department_id=r.department_id':''} AND newer.revision>r.revision)
      AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_applications a WHERE a.plan_id=r.job_id)
      AND (b.status IS DISTINCT FROM j.status OR b.ended IS DISTINCT FROM j.updated) FOR UPDATE OF b`,slot,...args(scope));
    if(row)await summary(slot,{scope:scope??undefined});
  });}
  return {summary,request,ensure,reconcile};
}
