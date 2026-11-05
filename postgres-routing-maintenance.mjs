import {AppError,id,nextDailyReset} from './store.mjs';
import {lockPostgresRouting} from './postgres-routing.mjs';

// A separate process restores expired temporary settings. It never calls an
// upstream model or migrates a binding; the nightly planner is a separate job.
export function createPostgresRoutingMaintenance(store,{jobs,router}={}){
  const kind='routing.expiry';
  async function schedule(){const at=await jobs.now(),boundary=nextDailyReset(at)-86400000;return jobs.enqueue(kind,{id:kind+':'+boundary,payload:{boundary},maxAttempts:5});}
  async function execute(job,{signal}={}){
    if(job.kind!==kind||(!Number.isSafeInteger(job.payload?.boundary)||job.payload.boundary<0||nextDailyReset(job.payload.boundary)-86400000!==job.payload.boundary))throw new AppError(400,'周期维护任务无效','job_payload_invalid');
    for(;;){
      const step=await jobs.owned(job,async()=>{
        const saved=await store.get('SELECT progress,result FROM harbor.background_jobs WHERE id=$1',job.id);
        if(saved.result!==null)return {done:true,result:saved.result};
        const progress={sessionsRestored:saved.progress.sessionsRestored??0};
        if(!Number.isSafeInteger(progress.sessionsRestored)||progress.sessionsRestored<0)throw new AppError(503,'周期维护进度无效','job_progress_invalid');
        await lockPostgresRouting(store);const at=await jobs.now();if(signal?.aborted)throw new AppError(503,'周期维护已停止','worker_stopping');
        if(job.payload.boundary>at)throw new AppError(409,'周期维护尚未到期','job_not_due');
        const rows=await store.all('SELECT id,nightly_expires FROM harbor.sessions WHERE nightly_enabled=0 AND nightly_expires<=$1 ORDER BY nightly_expires,id LIMIT 256 FOR UPDATE',at);
        if(rows.length){
          await store.run('UPDATE harbor.sessions SET nightly_enabled=1,nightly_expires=NULL WHERE id=ANY($1::text[])',rows.map(row=>row.id));
          await store.run("INSERT INTO harbor.audit(id,at,action,detail) SELECT p.audit,$2,'session.nightly.expired',jsonb_build_object('session',p.id,'expires',p.expires)::text FROM jsonb_to_recordset($1::jsonb) AS p(id text,expires bigint,audit text)",JSON.stringify(rows.map(row=>({id:row.id,expires:row.nightly_expires,audit:id()}))),at);
          progress.sessionsRestored+=rows.length;
          await store.run('UPDATE harbor.background_jobs SET progress=$1::jsonb,updated=$2 WHERE id=$3',JSON.stringify(progress),at,job.id);
          if(signal?.aborted)throw new AppError(503,'周期维护已停止','worker_stopping');return {done:false};
        }
        const current=JSON.parse((await store.get("SELECT value FROM harbor.gateway_settings WHERE name='routing_weights' FOR UPDATE"))?.value||'{"mode":"auto","weights":{},"expires":null,"version":0}');let weightsExpired=false;
        if(current.mode==='manual'&&current.expires<=at){
          await store.run("UPDATE harbor.gateway_settings SET value=$1 WHERE name='routing_weights'",JSON.stringify({mode:'auto',weights:{},expires:null,version:current.version+1}));
          await store.run("INSERT INTO harbor.audit VALUES($1,$2,'balancing.weights.expired',$3)",id(),at,String(current.expires));weightsExpired=true;
        }
        await router.syncWeights(false,at);const result={...progress,weightsExpired};
        await store.run('UPDATE harbor.background_jobs SET result=$1::jsonb,updated=$2 WHERE id=$3',JSON.stringify(result),at,job.id);
        if(signal?.aborted)throw new AppError(503,'周期维护已停止','worker_stopping');return {done:true,result};
      });
      if(step.done)return step.result;
      if(!await jobs.heartbeat(job))throw new AppError(409,'周期维护租约已失效','job_lease_lost');
    }
  }
  return {schedule,handlers:{[kind]:execute}};
}
