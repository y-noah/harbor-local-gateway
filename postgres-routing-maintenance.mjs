import {createRoutingDepartments} from './postgres-routing-departments.mjs';
import {departmentPlanningJobId} from './postgres-planning-jobs.mjs';
import {AppError,id,nextDailyReset} from './store.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';
import {createPostgresAudit} from './postgres-audit.mjs';

// A separate process restores expired temporary settings. It never calls an
// upstream model or migrates a binding; the nightly planner is a separate job.
export function createPostgresRoutingMaintenance(store,{jobs,router}={}){
  const kind='routing.expiry',departments=createRoutingDepartments(store);
  async function schedule({scope}={}){scope=await departments.resolve(scope);const at=await jobs.now(),boundary=nextDailyReset(at)-86400000;return jobs.enqueue(kind,{id:departmentPlanningJobId(kind,boundary,scope),payload:{boundary},maxAttempts:5,...(scope?{scope}:{})});}
  async function execute(job,{signal}={}){
    const scope=await jobs.authorizePersistedActor(job);
    if(job.kind!==kind||(!Number.isSafeInteger(job.payload?.boundary)||job.payload.boundary<0||nextDailyReset(job.payload.boundary)-86400000!==job.payload.boundary))throw new AppError(400,'周期维护任务无效','job_payload_invalid');
    for(;;){
      const step=await jobs.owned(job,async()=>{
        await jobs.authorizePersistedActor(job);await departments.resolve(scope,{write:true});
        const saved=await store.get('SELECT progress,result FROM harbor.background_jobs WHERE id=$1',job.id);
        if(saved.result!==null)return {done:true,result:saved.result};
        const progress={sessionsRestored:saved.progress.sessionsRestored??0};
        if(!Number.isSafeInteger(progress.sessionsRestored)||progress.sessionsRestored<0)throw new AppError(503,'周期维护进度无效','job_progress_invalid');
        await lockPostgresRouting(store);const at=await jobs.now();if(signal?.aborted)throw new AppError(503,'周期维护已停止','worker_stopping');
        if(job.payload.boundary>at)throw new AppError(409,'周期维护尚未到期','job_not_due');
        const rows=await store.all('SELECT id,nightly_expires FROM harbor.sessions WHERE nightly_enabled=0 AND nightly_expires<=$1'+(scope?' AND department_id=$2':'')+' ORDER BY nightly_expires,id LIMIT 256 FOR UPDATE',at,...(scope?[scope.department_id]:[]));
        if(rows.length){
          await store.run('UPDATE harbor.sessions SET nightly_enabled=1,nightly_expires=NULL WHERE id=ANY($1::text[])',rows.map(row=>row.id));
          for(const row of rows)await createPostgresAudit(store).write(scope,{action:'session.nightly.expired',at,detail:{session:row.id,expires:row.nightly_expires},subject:{kind:'session',id:row.id}});
          progress.sessionsRestored+=rows.length;
          await store.run('UPDATE harbor.background_jobs SET progress=$1::jsonb,updated=$2 WHERE id=$3',JSON.stringify(progress),at,job.id);
          if(signal?.aborted)throw new AppError(503,'周期维护已停止','worker_stopping');return {done:false};
        }
        const current=await departments.setting(scope,'routing_weights',{lock:true})??{mode:'auto',weights:{},expires:null,version:0};let weightsExpired=false;
        if(current.mode==='manual'&&current.expires<=at){
          await departments.write(scope,'routing_weights',{mode:'auto',weights:{},expires:null,version:current.version+1});
          await createPostgresAudit(store).write(scope,{action:'balancing.weights.expired',at,detail:String(current.expires)});weightsExpired=true;
        }
        await router.syncWeights(false,at,{scope});const result={...progress,weightsExpired};
        await store.run('UPDATE harbor.background_jobs SET result=$1::jsonb,updated=$2 WHERE id=$3',JSON.stringify(result),at,job.id);
        if(signal?.aborted)throw new AppError(503,'周期维护已停止','worker_stopping');return {done:true,result};
      });
      if(step.done)return step.result;
      if(!await jobs.heartbeat(job))throw new AppError(409,'周期维护租约已失效','job_lease_lost');
    }
  }
  return {schedule,handlers:{[kind]:execute}};
}
