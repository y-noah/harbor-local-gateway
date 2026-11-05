import {AppError} from './store.mjs';

// Only the active gateway requests a revision. Workers keep their existing
// immutable job/result ownership; retries never replace a committed plan.
export function createPostgresNightlyRevisions(store,{clock=Date.now}={}){
  async function summary(slot){
    const revisions=await store.all(`SELECT a.plan_id,a.status,a.started,a.ended,COALESCE(r.revision,0)::int revision,
      (p.header->'counts'->>'moves')::int planned,p.header->'metric' metric,p.header->'window' plan_window,COALESCE(p.header->>'skipReason',CASE WHEN a.summary IS NOT NULL THEN a.summary::jsonb->>'reason' END) skip_reason,
      (SELECT COUNT(*)::int FROM harbor.session_handoffs h JOIN harbor.automatic_handoff_groups g ON g.id=h.automatic_group_id WHERE g.plan_id=a.plan_id AND h.status='waiting') waiting,
      (SELECT COUNT(*)::int FROM harbor.automatic_handoff_groups g WHERE g.plan_id=a.plan_id AND g.status IN ('cancelled','failed','superseded')) cancelled_groups,
      (SELECT COUNT(*)::int FROM harbor.automatic_handoff_groups g WHERE g.plan_id=a.plan_id AND g.status='waiting') waiting_groups,
      COALESCE(t.applied,0)::int applied,COALESCE(t.skipped,0)::int skipped
      FROM harbor.nightly_plan_applications a JOIN harbor.routing_plans p ON p.id=a.plan_id
      LEFT JOIN harbor.nightly_plan_revisions r ON r.job_id=a.plan_id
      LEFT JOIN LATERAL(SELECT SUM(moved) applied,COUNT(*) FILTER(WHERE status='skipped') skipped FROM harbor.nightly_plan_group_results WHERE plan_id=a.plan_id) t ON true
      WHERE a.slot=$1 ORDER BY COALESCE(r.revision,0),a.plan_id`,slot);
    if(!revisions.length)return;
    const latest=revisions.at(-1),pending=await store.get("SELECT r.job_id,COALESCE(j.status,'pending') status,j.updated FROM harbor.nightly_plan_revisions r LEFT JOIN harbor.background_jobs j ON j.id=r.job_id WHERE r.slot=$1 AND r.revision>$2 ORDER BY r.revision DESC LIMIT 1",slot,latest.revision),terminal=pending&&['failed','cancelled'].includes(pending.status);
    const value={plan_id:latest.plan_id,revision_count:revisions.length,planned:revisions.reduce((n,row)=>n+row.planned,0),applied:revisions.reduce((n,row)=>n+row.applied,0),skipped:revisions.reduce((n,row)=>n+row.skipped,0),waiting:revisions.reduce((n,row)=>n+row.waiting,0),waitingGroups:revisions.reduce((n,row)=>n+row.waiting_groups,0),metric:latest.metric,window:latest.plan_window,revisions,...(latest.skip_reason?{reason:latest.skip_reason}:{}),...(pending?{pending_revision:pending.job_id,pending_revision_status:pending.status}:{}),...(terminal?{reason:pending.status==='failed'?'planning_failed':'planning_cancelled'}:{})};
    await store.run('UPDATE harbor.balance_runs SET plan_id=$1,status=$2,ended=$3,summary=$4 WHERE day=$5',latest.plan_id,pending?(terminal?pending.status:'running'):latest.status,pending?(terminal?pending.updated:null):latest.ended,JSON.stringify(value),slot);
    return value;
  }
  async function request(plan,reason){
    return store.transaction(async()=>{
      // The same slot row serializes apply finalization and revision requests.
      await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1 FOR UPDATE',plan.target);
      const existing=await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE replaces_plan_id=$1',plan.id);if(existing)return existing.job_id;
      const latest=await store.get('SELECT revision,job_id FROM harbor.nightly_plan_revisions WHERE slot=$1 ORDER BY revision DESC LIMIT 1',plan.target);
      if(latest&&latest.job_id!==plan.id)return latest.job_id;
      const revision=(latest?.revision??0)+1,jobId='routing.nightly:'+plan.target+':r'+revision;
      await store.run('INSERT INTO harbor.nightly_plan_revisions VALUES($1,$2,$3,$4,$5,$6)',plan.target,revision,jobId,plan.id,clock(),reason);
      await store.run("UPDATE harbor.nightly_plan_applications SET status='interrupted',ended=$1 WHERE plan_id=$2 AND status='running'",clock(),plan.id);
      await summary(plan.target);return jobId;
    });
  }
  async function ensure(plan){
    await store.run("INSERT INTO harbor.balance_runs(day,started,status,summary,plan_id) VALUES($1,$2,'running',$3,$4) ON CONFLICT(day) DO NOTHING",plan.target,clock(),JSON.stringify({plan_id:plan.id}),plan.id);
    await store.get('SELECT day FROM harbor.balance_runs WHERE day=$1 FOR UPDATE',plan.target);
    const revision=await store.get('SELECT job_id FROM harbor.nightly_plan_revisions WHERE slot=$1 ORDER BY revision DESC LIMIT 1',plan.target);
    if(revision&&revision.job_id!==plan.id)throw new AppError(409,'此计划已被当前时段的新修订替代','nightly_plan_changed');
    await store.run("INSERT INTO harbor.nightly_plan_applications(plan_id,slot,started,status) VALUES($1,$2,$3,'running') ON CONFLICT(plan_id) DO NOTHING",plan.id,plan.target,clock());
  }
  async function reconcile(slot){return store.transaction(async()=>{
    // Serialize with applications before projecting a later planner terminal.
    // A completed plan without an application is still waiting for the gateway.
    const row=await store.get(`SELECT b.day FROM harbor.balance_runs b JOIN harbor.nightly_plan_revisions r ON r.slot=b.day JOIN harbor.background_jobs j ON j.id=r.job_id
      WHERE b.day=$1 AND j.status IN ('failed','cancelled') AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_revisions newer WHERE newer.slot=r.slot AND newer.revision>r.revision)
      AND NOT EXISTS(SELECT 1 FROM harbor.nightly_plan_applications a WHERE a.plan_id=r.job_id)
      AND (b.status IS DISTINCT FROM j.status OR b.ended IS DISTINCT FROM j.updated) FOR UPDATE OF b`,slot);
    if(row)await summary(slot);
  });}
  return {summary,request,ensure,reconcile};
}
